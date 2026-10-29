# Copyright (c) Microsoft. All rights reserved.

"""Public HTTP routes for the checkpoint-only native Functions host.

The ordinary ``AgentFunctionApp`` is not part of a current-only cold directory, so its
original public addresses would otherwise disappear. This module registers the addresses
that belong to the trusted workflow through public ``df.DFApp`` decorators and durable
client bindings. It imports neither the ordinary host, the mutable legacy agent-state
reader, the original invocation helper, nor any migration compiler or converter.

Covered addresses (under the host ``routePrefix``):

* ``POST workflow/{name}/run`` for the top-level workflow.
* ``GET workflow/{name}/status/{instanceId}`` for the top-level workflow.
* ``POST workflow/{name}/respond/{instanceId}/{requestId}`` for the top-level workflow.
* ``POST agents/{workflow-scoped executor id}/run`` for every hosted workflow agent.
* ``GET health`` listing those agents.

A migration-only deployment rejects ``workflow/{name}/run`` before touching the backend.
Only a new-workflows-only deployment may start a new root. The agent ``run`` route signals
the workflow-scoped agent entity. The entity handler, not this route, owns what that
entity accepts. Instance and execution controls remain backend SDK operations.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import unicodedata
import uuid
from collections.abc import Callable, Mapping
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any, cast

import azure.durable_functions as df
import azure.functions as func
from agent_framework import AgentResponse, SupportsAgentRun, Workflow
from agent_framework_durabletask import (
    DEFAULT_MAX_POLL_RETRIES,
    DEFAULT_POLL_INTERVAL_SECONDS,
    LEGACY_THREAD_ID_FIELD,
    MIMETYPE_APPLICATION_JSON,
    MIMETYPE_TEXT_PLAIN,
    REQUEST_RESPONSE_FORMAT_JSON,
    REQUEST_RESPONSE_FORMAT_TEXT,
    SESSION_ID_FIELD,
    SESSION_ID_HEADER,
    WAIT_FOR_RESPONSE_FIELD,
    WAIT_FOR_RESPONSE_HEADER,
    AgentSessionId,
    ApiResponseFields,
    RunRequest,
    deserialize_workflow_output,
    plan_workflow_registration,
    read_agent_state,
    serialize_agent_response,
    validate_workflow_start_input,
    wrap_workflow_input,
)
from agent_framework_durabletask._checkpoints._reader import SemanticAgentStateReader
from agent_framework_durabletask._workflows.naming import (
    SUBWORKFLOW_REQUEST_SEPARATOR,
    iter_subworkflow_instances,
    split_subworkflow_request_id,
    workflow_orchestrator_name,
    workflow_scoped_executor_id,
)
from agent_framework_durabletask._workflows.registration import collect_hosted_workflows
from agent_framework_durabletask._workflows.serialization import strip_pickle_markers, strip_subworkflow_markers
from durabletask.client import OrchestrationState, OrchestrationStatus
from durabletask.entities import EntityInstanceId

from ._errors import IncomingRequestError
from ._routes import build_workflow_respond_url, build_workflow_status_url, split_request_url

logger = logging.getLogger("agent_framework.azurefunctions")

_DEFAULT_WORKFLOW_WAIT_TIMEOUT_SECONDS = 10
_MAX_WORKFLOW_WAIT_TIMEOUT_SECONDS = 200
_MAX_WORKFLOW_RUN_ID_LENGTH = 100
_RUN_ID_QUERY_PARAMETER = "runId"
_WORKFLOW_WAIT_FOR_RESPONSE_QUERY_PARAMETER = "waitForResponse"
_WORKFLOW_WAIT_TIMEOUT_SECONDS_QUERY_PARAMETER = "timeoutSeconds"

# Camel-case aliases are accepted and emitted in addition to the published snake_case names.
_AGENT_SESSION_ID_FIELD = "sessionId"
_AGENT_WAIT_FOR_RESPONSE_FIELD = "waitForResponse"
_AGENT_CORRELATION_ID_FIELD = "correlationId"
_LEGACY_AGENT_CORRELATION_ID_FIELD = "correlation_id"

_DEPRECATION_HEADERS = {
    "Deprecation": "@1789430400",
    "Link": (
        "<https://github.com/microsoft/agent-framework-durable-extension/blob/main/"
        'docs/features/durable-agents/http-api-camelcase-migration.md>; rel="deprecation"'
    ),
    "Warning": (
        '299 - "Deprecated agent HTTP field names are supported temporarily; use sessionId and waitForResponse."'
    ),
}

_SEMANTIC_CONTRACT = "agent-semantic-checkpoint/1"
_ADMISSION_PROFILES = ("migration-only", "new-workflows-only")
MIGRATION_ONLY_START_ERROR = (
    "This migration-only deployment does not accept new workflow starts. "
    "Resume an operator-authorized migrated instance instead."
)

_RUNTIME_STATUS_WIRE_NAMES: Mapping[OrchestrationStatus, str] = {
    OrchestrationStatus.RUNNING: "Running",
    OrchestrationStatus.COMPLETED: "Completed",
    OrchestrationStatus.FAILED: "Failed",
    OrchestrationStatus.TERMINATED: "Terminated",
    OrchestrationStatus.CONTINUED_AS_NEW: "ContinuedAsNew",
    OrchestrationStatus.PENDING: "Pending",
    OrchestrationStatus.SUSPENDED: "Suspended",
}
_TERMINAL_RUNTIME_STATUSES = frozenset({
    OrchestrationStatus.COMPLETED,
    OrchestrationStatus.FAILED,
    OrchestrationStatus.TERMINATED,
})


def _runtime_status_name(status: OrchestrationStatus | None) -> str | None:
    if status is None:
        return None
    return _RUNTIME_STATUS_WIRE_NAMES.get(status, status.name)


def _read_json_field(serialized: str | None) -> Any:
    """Read framework JSON without the Functions converter's object constructor."""
    if serialized is None or serialized == "":
        return None
    try:
        return json.loads(serialized)
    except json.JSONDecodeError:
        return serialized


def _workflow_error(status: OrchestrationState, decoded_output: Any) -> Any:
    if status.failure_details is not None:
        return status.failure_details.message
    return decoded_output


def _utc_isoformat(value: datetime | None) -> str | None:
    if value is None:
        return None
    return (value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)).isoformat()


def _json_default(obj: Any) -> Any:
    if isinstance(obj, AgentResponse):
        return serialize_agent_response(cast("AgentResponse[Any]", obj))
    to_dict = getattr(obj, "to_dict", None)
    if callable(to_dict):
        try:
            return to_dict()
        except Exception:
            logger.debug("to_dict() failed while encoding %s for HTTP output", type(obj).__name__)
    model_dump = getattr(obj, "model_dump", None)
    if callable(model_dump):
        try:
            return model_dump(mode="json")
        except Exception:
            logger.debug("model_dump() failed while encoding %s for HTTP output", type(obj).__name__)
    if is_dataclass(obj) and not isinstance(obj, type):
        return asdict(obj)
    return str(obj)


def _try_coerce_to_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "on"}:
            return True
        if normalized in {"false", "0", "no", "n", "off"}:
            return False
    return None


def _error_response(message: str, status_code: int = 400) -> func.HttpResponse:
    return func.HttpResponse(
        json.dumps({"error": message}),
        status_code=status_code,
        mimetype=MIMETYPE_APPLICATION_JSON,
    )


def _normalized_headers(req: func.HttpRequest) -> dict[str, str]:
    return {key.lower(): value for key, value in cast(Mapping[str, str], req.headers).items()}


def _validate_run_id(run_id: str | None) -> str | None:
    if run_id is None:
        return None
    if (
        not 1 <= len(run_id) <= _MAX_WORKFLOW_RUN_ID_LENGTH
        or run_id.startswith("@")
        or any(character in run_id for character in "/\\#?")
        or any(unicodedata.category(character) == "Cc" for character in run_id)
    ):
        raise ValueError(
            f"'{_RUN_ID_QUERY_PARAMETER}' must be between 1 and {_MAX_WORKFLOW_RUN_ID_LENGTH} characters, "
            "must not start with '@', and must not contain '/', '\\', '#', '?', or control characters."
        )
    return run_id


def _workflow_wait_timeout_seconds(req: func.HttpRequest) -> int:
    value = (req.params or {}).get(_WORKFLOW_WAIT_TIMEOUT_SECONDS_QUERY_PARAMETER)
    if value is None:
        return _DEFAULT_WORKFLOW_WAIT_TIMEOUT_SECONDS
    message = (
        f"'{_WORKFLOW_WAIT_TIMEOUT_SECONDS_QUERY_PARAMETER}' must be an integer between "
        f"1 and {_MAX_WORKFLOW_WAIT_TIMEOUT_SECONDS}."
    )
    try:
        seconds = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(message) from exc
    if seconds <= 0 or seconds > _MAX_WORKFLOW_WAIT_TIMEOUT_SECONDS:
        raise ValueError(message)
    return seconds


def _workflow_wait_for_response(req: func.HttpRequest) -> bool:
    parsed_header = _try_coerce_to_bool(_normalized_headers(req).get(WAIT_FOR_RESPONSE_HEADER))
    if parsed_header is not None:
        return parsed_header
    query_value = (req.params or {}).get(_WORKFLOW_WAIT_FOR_RESPONSE_QUERY_PARAMETER)
    if query_value is None:
        return False
    parsed_query = _try_coerce_to_bool(query_value)
    if parsed_query is None:
        raise ValueError(f"'{_WORKFLOW_WAIT_FOR_RESPONSE_QUERY_PARAMETER}' must be a boolean value.")
    return parsed_query


def _is_terminal(status: Any) -> bool:
    runtime_status = getattr(status, "runtime_status", None)
    return isinstance(runtime_status, OrchestrationStatus) and runtime_status in _TERMINAL_RUNTIME_STATUSES


def _is_owned_orchestration(status: Any, workflow_name: str) -> bool:
    """Scope an instance ID to this workflow's orchestrator, as the original endpoints did."""
    name = getattr(status, "name", None)
    return isinstance(name, str) and name.casefold() == workflow_orchestrator_name(workflow_name).casefold()


async def _gather_pending_hitl_requests(
    client: df.DurableFunctionsClient,
    custom_status: dict[str, Any],
    *,
    prefix: str = "",
) -> list[tuple[str, dict[str, Any]]]:
    gathered: list[tuple[str, dict[str, Any]]] = []
    pending = custom_status.get("pending_requests")
    if isinstance(pending, dict):
        for req_id, req_data in cast("dict[str, Any]", pending).items():
            if isinstance(req_data, dict):
                typed = cast("dict[str, Any]", req_data)
                gathered.append((f"{prefix}{typed.get('request_id', req_id)}", typed))
    subworkflows = custom_status.get("subworkflows")
    if isinstance(subworkflows, dict):
        separator = SUBWORKFLOW_REQUEST_SEPARATOR
        for executor_id, child_ids in cast("dict[str, Any]", subworkflows).items():
            for ordinal, child_instance_id in iter_subworkflow_instances(child_ids):
                child_status = await client.get_orchestration_state(child_instance_id)
                if child_status is None or _is_terminal(child_status):
                    continue
                child_custom = _read_json_field(child_status.serialized_custom_status)
                if isinstance(child_custom, dict):
                    gathered.extend(
                        await _gather_pending_hitl_requests(
                            client,
                            cast("dict[str, Any]", child_custom),
                            prefix=f"{prefix}{executor_id}{separator}{ordinal}{separator}",
                        )
                    )
    return gathered


async def _resolve_hitl_target(
    client: df.DurableFunctionsClient, instance_id: str, request_id: str
) -> tuple[str, str] | None:
    status = await client.get_orchestration_state(instance_id)
    if status is None or _is_terminal(status):
        return None
    hop = split_subworkflow_request_id(request_id)
    if hop is None:
        return instance_id, request_id
    custom_status = _read_json_field(status.serialized_custom_status)
    if not isinstance(custom_status, dict):
        return None
    executor_id, ordinal, remainder = hop
    subworkflows = cast("dict[str, Any]", custom_status).get("subworkflows")
    if not isinstance(subworkflows, dict):
        return None
    children = cast("dict[str, Any]", subworkflows).get(executor_id)
    child_instance_id = dict(iter_subworkflow_instances(children)).get(ordinal)
    if child_instance_id is None:
        return None
    return await _resolve_hitl_target(client, child_instance_id, remainder)


def _function_name(agent_name: str) -> str:
    sanitized = re.sub(r"[^0-9a-zA-Z_]", "_", agent_name or "agent").strip("_") or "agent"
    if sanitized[0].isdigit():
        sanitized = f"agent_{sanitized}"
    return f"http-{sanitized}"


def _workflow_route_function_name(workflow: Workflow, suffix: str) -> str:
    name = f"{workflow_orchestrator_name(workflow.name)}-{suffix}"
    plan = plan_workflow_registration(workflow)
    if any(executor.id.casefold() == suffix for executor in (*plan.agent_executors, *plan.activity_executors)):
        return f"http-{name}"
    return name


def _agent_entity_id(session_id: AgentSessionId) -> EntityInstanceId:
    try:
        EntityInstanceId.validate_key(session_id.key)
    except ValueError as exc:
        raise IncomingRequestError(f"Invalid session ID: {exc}") from exc
    return EntityInstanceId(entity=session_id.entity_name, key=session_id.key)


class _AgentRunRoute:
    """Release agent run semantics over the published semantic polling view."""

    def __init__(self, *, max_poll_retries: int, poll_interval_seconds: float) -> None:
        self.max_poll_retries = max_poll_retries
        self.poll_interval_seconds = poll_interval_seconds

    async def handle(
        self, req: func.HttpRequest, client: df.DurableFunctionsClient, agent_name: str
    ) -> func.HttpResponse:
        response_format = REQUEST_RESPONSE_FORMAT_JSON
        session_id: str | None = None
        legacy = self._uses_legacy_names(req)
        try:
            body, message, response_format = self._parse_request(req)
            legacy = self._uses_legacy_names(req, body)
            session_id = self._resolve_session_id(req, body)
            wait_for_response = self._should_wait(req, body)
            if not message:
                return self._response({"error": "Message is required"}, 400, response_format, session_id, legacy)

            agent_session_id = AgentSessionId(name=agent_name, key=session_id)
            correlation_id = uuid.uuid4().hex
            entity_id = _agent_entity_id(agent_session_id)
            await client.signal_entity(
                entity_id, "run", self._request_data(body, message, correlation_id, response_format)
            )
            if wait_for_response:
                result = await self._wait_for_response(client, entity_id, correlation_id, message, session_id)
                status_code = 200 if result.get("status") == "success" else 500
                return self._response(result, status_code, response_format, session_id, legacy)
            accepted = self._payload(
                response="Agent request accepted",
                message=message,
                session_id=session_id,
                status="accepted",
                correlation_id=correlation_id,
            )
            return self._response(accepted, 202, response_format, session_id, legacy)
        except IncomingRequestError as exc:
            return self._response({"error": str(exc)}, exc.status_code, response_format, session_id, legacy)
        except ValueError:
            return self._response({"error": "Invalid JSON"}, 400, response_format, session_id, legacy)
        except Exception as exc:
            logger.error("[Checkpoint HTTP] Agent run failed: %s", exc, exc_info=True)
            return self._response({"error": str(exc)}, 500, response_format, session_id, legacy)

    @staticmethod
    def _parse_request(req: func.HttpRequest) -> tuple[dict[str, Any], str, str]:
        headers = _normalized_headers(req)
        content_type = headers.get("content-type", "").split(";")[0].strip().lower()
        is_json_body = content_type == MIMETYPE_APPLICATION_JSON or content_type.endswith("+json")
        prefers_json = any(
            value.split(";")[0].strip().lower() == MIMETYPE_APPLICATION_JSON
            for value in headers.get("accept", "").split(",")
        )
        response_format = REQUEST_RESPONSE_FORMAT_JSON if is_json_body or prefers_json else REQUEST_RESPONSE_FORMAT_TEXT
        if is_json_body:
            body = req.get_json()
            if not isinstance(body, dict):
                raise IncomingRequestError("Invalid JSON payload. Expected an object.")
            typed = cast(dict[str, Any], body)
            value = typed.get("message", "")
            return typed, value if isinstance(value, str) else str(value), response_format
        raw = req.get_body()
        return {}, (raw.decode("utf-8", errors="replace") if raw else "").strip(), response_format

    @staticmethod
    def _resolve_session_id(req: func.HttpRequest, body: dict[str, Any]) -> str:
        candidates: dict[str, str] = {}
        for source_name, source in (("request body", body), ("query string", req.params or {})):
            for field in (_AGENT_SESSION_ID_FIELD, SESSION_ID_FIELD, LEGACY_THREAD_ID_FIELD):
                value = source.get(field)
                if value is not None and str(value).strip():
                    candidates[f"{field} in the {source_name}"] = str(value)
        distinct = set(candidates.values())
        if len(distinct) > 1:
            raise IncomingRequestError(
                "Conflicting session identifiers supplied: "
                + ", ".join(f"{name}={value!r}" for name, value in sorted(candidates.items()))
            )
        return distinct.pop() if distinct else uuid.uuid4().hex

    @staticmethod
    def _should_wait(req: func.HttpRequest, body: dict[str, Any]) -> bool:
        params = req.params or {}
        query = _try_coerce_to_bool(params.get(_AGENT_WAIT_FOR_RESPONSE_FIELD))
        legacy_query = _try_coerce_to_bool(params.get(WAIT_FOR_RESPONSE_FIELD))
        if query is not None and legacy_query is not None and query != legacy_query:
            raise IncomingRequestError(
                f"{_AGENT_WAIT_FOR_RESPONSE_FIELD} and {WAIT_FOR_RESPONSE_FIELD} specified in the query string "
                "must match."
            )
        in_body = _try_coerce_to_bool(body.get(_AGENT_WAIT_FOR_RESPONSE_FIELD))
        legacy_body = _try_coerce_to_bool(body.get(WAIT_FOR_RESPONSE_FIELD))
        if in_body is not None and legacy_body is not None and in_body != legacy_body:
            raise IncomingRequestError(
                f"{_AGENT_WAIT_FOR_RESPONSE_FIELD} and {WAIT_FOR_RESPONSE_FIELD} specified in the request body "
                "must match."
            )
        header = _try_coerce_to_bool(_normalized_headers(req).get(WAIT_FOR_RESPONSE_HEADER))
        for value in (header, query, legacy_query, in_body, legacy_body):
            if value is not None:
                return value
        return True

    @staticmethod
    def _request_data(body: dict[str, Any], message: str, correlation_id: str, response_format: str) -> dict[str, Any]:
        enable_tool_calls = body.get("enable_tool_calls")
        return RunRequest(
            message=message,
            role=body.get("role"),
            request_response_format=response_format,
            response_format=body.get("response_format"),
            enable_tool_calls=True if enable_tool_calls is None else bool(_try_coerce_to_bool(enable_tool_calls)),
            correlation_id=correlation_id,
            created_at=datetime.now(timezone.utc),
        ).to_dict()

    @staticmethod
    def _payload(
        *,
        response: str | None,
        message: str,
        session_id: str,
        status: str,
        correlation_id: str,
        extra_fields: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "response": response,
            "message": message,
            _AGENT_SESSION_ID_FIELD: session_id,
            SESSION_ID_FIELD: session_id,
            "status": status,
            _AGENT_CORRELATION_ID_FIELD: correlation_id,
            _LEGACY_AGENT_CORRELATION_ID_FIELD: correlation_id,
        }
        if extra_fields:
            payload.update(extra_fields)
            for legacy_name, canonical_name in (
                (ApiResponseFields.MESSAGE_COUNT, "messageCount"),
                ("error_code", "errorCode"),
            ):
                if legacy_name in extra_fields:
                    payload[canonical_name] = extra_fields[legacy_name]
        return payload

    async def _wait_for_response(
        self,
        client: df.DurableFunctionsClient,
        entity_id: EntityInstanceId,
        correlation_id: str,
        message: str,
        session_id: str,
    ) -> dict[str, Any]:
        for _ in range(self.max_poll_retries):
            await asyncio.sleep(self.poll_interval_seconds)
            result = await self._poll(client, entity_id, correlation_id, message, session_id)
            if result is not None:
                return result
        return self._payload(
            response="Agent is still processing or timed out...",
            message=message,
            session_id=session_id,
            status="timeout",
            correlation_id=correlation_id,
        )

    async def _poll(
        self,
        client: df.DurableFunctionsClient,
        entity_id: EntityInstanceId,
        correlation_id: str,
        message: str,
        session_id: str,
    ) -> dict[str, Any] | None:
        def failure(error_code: str, diagnostic: str) -> dict[str, Any]:
            # Stored values can appear in exception text, so only the phase is public.
            logger.warning("[Checkpoint HTTP] %s", diagnostic)
            return self._payload(
                response=None,
                message=message,
                session_id=session_id,
                status="error",
                correlation_id=correlation_id,
                extra_fields={"error": diagnostic, "error_code": error_code},
            )

        try:
            entity = await client.get_entity(entity_id)
        except Exception:
            logger.warning("[Checkpoint HTTP] Entity state transport read failed", exc_info=True)
            return None
        if entity is None:
            return None
        try:
            raw = entity.get_state()
            peeked = json.loads(raw) if isinstance(raw, str) else raw
            # Only a semantic checkpoint is readable here. Any other shape is malformed in
            # this deployment and must not reach the mutable legacy reader.
            if not isinstance(peeked, dict) or cast("dict[str, Any]", peeked).get("contract") != _SEMANTIC_CONTRACT:
                raise ValueError("Unsupported stored agent state.")
            state = read_agent_state(raw)
            if not isinstance(state, SemanticAgentStateReader):
                raise ValueError("Unsupported stored agent state.")
        except Exception:
            return failure("state_read_error", "Failed to read the stored agent response.")
        try:
            agent_response = state.try_get_agent_response(correlation_id)
        except Exception:
            return failure("response_projection_error", "Failed to project the stored agent response.")
        if not agent_response:
            return None
        try:
            return self._payload(
                response=agent_response.text,
                message=message,
                session_id=session_id,
                status="success",
                correlation_id=correlation_id,
                extra_fields={ApiResponseFields.MESSAGE_COUNT: state.message_count},
            )
        except Exception:
            return failure("response_processing_error", "Failed to process the agent response.")

    @staticmethod
    def _uses_legacy_names(req: func.HttpRequest, body: dict[str, Any] | None = None) -> bool:
        """Report the deprecated snake_case session and wait aliases, as the ordinary host does."""
        params = req.params or {}
        values = [params.get(SESSION_ID_FIELD), params.get(LEGACY_THREAD_ID_FIELD), params.get(WAIT_FOR_RESPONSE_FIELD)]
        if body:
            values += [body.get(SESSION_ID_FIELD), body.get(LEGACY_THREAD_ID_FIELD), body.get(WAIT_FOR_RESPONSE_FIELD)]
        return any(value is not None and str(value).strip() for value in values)

    def _response(
        self,
        payload: dict[str, Any],
        status_code: int,
        response_format: str,
        session_id: str | None,
        legacy: bool = False,
    ) -> func.HttpResponse:
        headers: dict[str, str] = {}
        if response_format == REQUEST_RESPONSE_FORMAT_TEXT and session_id is not None:
            headers[SESSION_ID_HEADER] = session_id
        if legacy:
            headers.update(_DEPRECATION_HEADERS)
        if response_format == REQUEST_RESPONSE_FORMAT_TEXT:
            return func.HttpResponse(
                self._as_text(payload), status_code=status_code, mimetype=MIMETYPE_TEXT_PLAIN, headers=headers or None
            )
        return func.HttpResponse(
            json.dumps(payload),
            status_code=status_code,
            mimetype=MIMETYPE_APPLICATION_JSON,
            headers=headers or None,
        )

    @staticmethod
    def _as_text(payload: dict[str, Any]) -> str:
        response = payload.get("response")
        if payload.get("status") == "success" and isinstance(response, str):
            return response
        for key in ("response", "error", "message"):
            value = payload.get(key)
            if isinstance(value, str) and value:
                return value
        return json.dumps(payload)


def register_checkpoint_routes(
    app: df.DFApp,
    workflow: Workflow,
    *,
    admission_profile: str = "migration-only",
    enable_health_check: bool = True,
    max_poll_retries: int = DEFAULT_MAX_POLL_RETRIES,
    poll_interval_seconds: float = DEFAULT_POLL_INTERVAL_SECONDS,
) -> None:
    """Register the trusted workflow's original public HTTP addresses on a native ``DFApp``.

    ``workflow`` and ``admission_profile`` must be the values used to register the current
    handlers. The routes carry no authority. ``migration-only`` rejects new workflow starts
    with HTTP 403 and never schedules one. Raises ``ValueError`` for an unsupported profile
    or a repeated function name. Names already on the app are rejected by the host's validation.
    """
    if admission_profile not in _ADMISSION_PROFILES:
        raise ValueError("Unsupported deployment admission profile.")
    # DFApp.get_functions() validates once and cannot be called twice, so it is left to the host.
    taken: set[str] = set()

    def reserve(name: str) -> str:
        if name in taken:
            raise ValueError(f"Function name already registered: {name}")
        taken.add(name)
        return name

    agents: dict[str, SupportsAgentRun] = {}
    for graph in collect_hosted_workflows(workflow):
        for executor in plan_workflow_registration(graph).agent_executors:
            agents.setdefault(workflow_scoped_executor_id(graph.name, executor.id), executor.agent)
    runner = _AgentRunRoute(
        max_poll_retries=max(1, int(max_poll_retries)),
        poll_interval_seconds=poll_interval_seconds if poll_interval_seconds > 0 else DEFAULT_POLL_INTERVAL_SECONDS,
    )

    for agent_name in agents:
        _register_agent_route(app, runner, agent_name, reserve(_function_name(agent_name)))
    _register_workflow_routes(app, workflow, admission_profile, reserve)

    if enable_health_check:

        @app.function_name(reserve("health_check"))
        @app.route(route="health", methods=["GET"])
        def health_check(req: func.HttpRequest) -> func.HttpResponse:
            info = [
                {
                    "name": name,
                    "type": type(agent).__name__,
                    "http_endpoint_enabled": True,
                    "mcp_tool_enabled": False,
                }
                for name, agent in agents.items()
            ]
            return func.HttpResponse(
                json.dumps({"status": "healthy", "agents": info, "agent_count": len(info)}),
                status_code=200,
                mimetype=MIMETYPE_APPLICATION_JSON,
            )

        _ = health_check


def _register_agent_route(app: df.DFApp, runner: _AgentRunRoute, agent_name: str, function_name: str) -> None:
    @app.function_name(function_name)
    @app.route(route=f"agents/{agent_name}/run", methods=["POST"])
    @app.durable_client_input(client_name="client")
    async def http_start(req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
        return await runner.handle(req, client, agent_name)

    _ = http_start


def _register_workflow_routes(
    app: df.DFApp, workflow: Workflow, admission_profile: str, reserve: Callable[[str], str]
) -> None:
    workflow_name = workflow.name
    orchestrator_name = workflow_orchestrator_name(workflow_name)

    @app.function_name(reserve(_workflow_route_function_name(workflow, "start")))
    @app.route(route=f"workflow/{workflow_name}/run", methods=["POST"])
    @app.durable_client_input(client_name="client")
    async def start_workflow(req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
        if admission_profile != "new-workflows-only":
            return _error_response(MIGRATION_ONLY_START_ERROR, status_code=403)
        try:
            wait_for_response = _workflow_wait_for_response(req)
            wait_timeout = _workflow_wait_timeout_seconds(req) if wait_for_response else 0
            requested_instance_id = _validate_run_id((req.params or {}).get(_RUN_ID_QUERY_PARAMETER))
        except ValueError as exc:
            return _error_response(str(exc), status_code=400)
        try:
            client_input: Any = req.get_json()
        except ValueError:
            raw_body = req.get_body()
            if not raw_body:
                return _error_response("Request body is required")
            client_input = raw_body.decode("utf-8")
        try:
            validate_workflow_start_input(client_input)
            client_input = strip_pickle_markers(strip_subworkflow_markers(client_input))
            workflow_input = wrap_workflow_input(client_input)
        except ValueError as exc:
            return _error_response(str(exc), status_code=400)

        instance_id = await client.schedule_new_orchestration(
            orchestrator_name, input=workflow_input, instance_id=requested_instance_id
        )
        if wait_for_response:
            try:
                completed = await client.wait_for_orchestration_completion(instance_id, timeout=wait_timeout)
            except TimeoutError:
                completed = None
            if completed is not None and completed.runtime_status in _TERMINAL_RUNTIME_STATUSES:
                return _terminal_response(completed, instance_id)
        base_url, prefix = split_request_url(req.url)
        return func.HttpResponse(
            json.dumps({
                "instanceId": instance_id,
                "statusQueryGetUri": build_workflow_status_url(base_url, workflow_name, instance_id, prefix=prefix),
                "respondUri": build_workflow_respond_url(
                    base_url, workflow_name, instance_id, "{requestId}", prefix=prefix
                ),
                "message": "Workflow started",
            }),
            status_code=202,
            mimetype=MIMETYPE_APPLICATION_JSON,
        )

    @app.function_name(reserve(_workflow_route_function_name(workflow, "status")))
    @app.route(route=f"workflow/{workflow_name}/status/{{instanceId}}", methods=["GET"])
    @app.durable_client_input(client_name="client")
    async def get_workflow_status(req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
        instance_id = req.route_params.get("instanceId")
        if not instance_id:
            return _error_response("Instance ID is required", status_code=400)
        status = await client.get_orchestration_state(instance_id)
        if status is None or not _is_owned_orchestration(status, workflow_name):
            return _error_response("Instance not found", status_code=404)

        raw_output = _read_json_field(status.serialized_output)
        decoded_output = deserialize_workflow_output(raw_output) if raw_output is not None else None
        custom_status = _read_json_field(status.serialized_custom_status)
        response: dict[str, Any] = {
            "instanceId": status.instance_id,
            "runtimeStatus": _runtime_status_name(status.runtime_status),
            "customStatus": custom_status,
            "output": decoded_output,
            "error": (
                _workflow_error(status, decoded_output) if status.runtime_status == OrchestrationStatus.FAILED else None
            ),
            "createdTime": _utc_isoformat(status.created_at),
            "lastUpdatedTime": _utc_isoformat(status.last_updated_at),
        }
        if isinstance(custom_status, dict) and not _is_terminal(status):
            gathered = await _gather_pending_hitl_requests(client, cast("dict[str, Any]", custom_status))
            if gathered:
                base_url, prefix = split_request_url(req.url)
                response["pendingHumanInputRequests"] = [
                    {
                        "requestId": qualified_id,
                        "sourceExecutor": data.get("source_executor_id"),
                        "requestData": data.get("data"),
                        "requestType": data.get("request_type"),
                        "responseType": data.get("response_type"),
                        "respondUrl": build_workflow_respond_url(
                            base_url, workflow_name, instance_id, qualified_id, prefix=prefix
                        ),
                    }
                    for qualified_id, data in gathered
                ]
        return func.HttpResponse(
            json.dumps(response, default=_json_default), status_code=200, mimetype=MIMETYPE_APPLICATION_JSON
        )

    @app.function_name(reserve(_workflow_route_function_name(workflow, "respond")))
    @app.route(route=f"workflow/{workflow_name}/respond/{{instanceId}}/{{requestId}}", methods=["POST"])
    @app.durable_client_input(client_name="client")
    async def send_hitl_response(req: func.HttpRequest, client: df.DurableFunctionsClient) -> func.HttpResponse:
        instance_id = req.route_params.get("instanceId")
        request_id = req.route_params.get("requestId")
        if not instance_id or not request_id:
            return _error_response("Instance ID and Request ID are required.")
        status = await client.get_orchestration_state(instance_id)
        if not _is_owned_orchestration(status, workflow_name):
            return _error_response("Instance not found", status_code=404)
        if _is_terminal(status):
            return _error_response("Workflow instance has a terminal runtime status", status_code=409)
        try:
            response_data = req.get_json()
        except ValueError:
            return _error_response("Request body must be valid JSON.")
        safe_response = strip_pickle_markers(response_data)
        if safe_response is None and response_data is not None:
            return _error_response("HITL response contained disallowed pickle/type markers.")
        resolved = await _resolve_hitl_target(client, instance_id, request_id)
        if resolved is None:
            return _error_response("Pending request not found", status_code=404)
        target_instance_id, bare_request_id = resolved
        await client.raise_orchestration_event(target_instance_id, bare_request_id, data=safe_response)
        return func.HttpResponse(
            json.dumps({
                "message": "Response delivered successfully",
                "instanceId": instance_id,
                "requestId": request_id,
            }),
            status_code=200,
            mimetype=MIMETYPE_APPLICATION_JSON,
        )

    _ = (start_workflow, get_workflow_status, send_hitl_response)


def _terminal_response(status: OrchestrationState, instance_id: str) -> func.HttpResponse:
    runtime_status = status.runtime_status
    if runtime_status not in {OrchestrationStatus.COMPLETED, OrchestrationStatus.FAILED}:
        return _error_response(
            f"Workflow orchestration '{instance_id}' ended with unexpected status "
            f"'{_runtime_status_name(runtime_status)}'.",
            status_code=500,
        )
    raw_output = _read_json_field(status.serialized_output)
    decoded_output = deserialize_workflow_output(raw_output) if raw_output is not None else None
    response: dict[str, Any] = {
        "instanceId": status.instance_id or instance_id,
        "runtimeStatus": _runtime_status_name(runtime_status),
        "output": decoded_output if runtime_status == OrchestrationStatus.COMPLETED else None,
    }
    if runtime_status == OrchestrationStatus.FAILED:
        response["error"] = _workflow_error(status, decoded_output)
    return func.HttpResponse(
        json.dumps(response, default=_json_default), status_code=200, mimetype=MIMETYPE_APPLICATION_JSON
    )
