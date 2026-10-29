# Copyright (c) Microsoft. All rights reserved.

"""Component tests for the checkpoint-only public routes.

These call the registered native HTTP functions with a stub durable client. They prove the
request and response contract only, not behavior against a live backend.
"""

from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import azure.durable_functions as df
import azure.functions as func
import pytest
from agent_framework import (
    AgentExecutor,
    AgentResponse,
    BaseAgent,
    Message,
    SupportsAgentRun,
    Workflow,
    WorkflowBuilder,
)
from agent_framework_durabletask import serialize_agent_response
from agent_framework_durabletask._checkpoints._agent import empty_checkpoint
from durabletask.client import OrchestrationStatus

from agent_framework_azurefunctions._checkpoint_routes import MIGRATION_ONLY_START_ERROR, register_checkpoint_routes

_BASE = "http://localhost:7071/api"
_SEMANTIC_RESPONSE = serialize_agent_response(AgentResponse(messages=[Message(role="assistant", contents=["hello"])]))


class _Agent(BaseAgent):
    async def run(self, messages: Any = None, *, stream: bool = False, session: Any = None, **kwargs: Any) -> Any:
        return AgentResponse(messages=[Message(role="assistant", contents=["unused"])])


def _workflow() -> Workflow:
    node = AgentExecutor(cast(SupportsAgentRun, _Agent(name="agent")), id="agent")
    return WorkflowBuilder(name="routes", start_executor=node, output_from=[node]).build()


def _functions(profile: str = "migration-only", **kwargs: Any) -> dict[str, Any]:
    app = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)
    register_checkpoint_routes(
        app, _workflow(), admission_profile=profile, poll_interval_seconds=0.001, max_poll_retries=3, **kwargs
    )
    functions = {}
    for function in app.get_functions():
        name = function.get_function_name()
        if name is None:
            raise AssertionError("Registered Azure Functions must have names.")
        functions[name] = function
    return functions


def _route(function: Any) -> str:
    bindings = json.loads(function.get_function_json())["bindings"]
    return next(binding["route"] for binding in bindings if binding["type"] == "httpTrigger")


async def _call(
    functions: Mapping[str, Any],
    name: str,
    client: Any,
    *,
    method: str = "POST",
    url: str = f"{_BASE}/x",
    body: Any = None,
    raw: bytes | None = None,
    params: dict[str, str] | None = None,
    route_params: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
) -> func.HttpResponse:
    payload = raw if raw is not None else (json.dumps(body).encode() if body is not None else b"")
    request = func.HttpRequest(
        method=method,
        url=url,
        body=payload,
        params=params or {},
        route_params=route_params or {},
        headers=headers or ({"Content-Type": "application/json"} if body is not None else {}),
    )
    user_function = functions[name].get_user_function()
    handler = getattr(user_function, "client_function", user_function)
    if name == "health_check":
        return handler(request)
    return await handler(req=request, client=client)


def _state(name: str, *, status: OrchestrationStatus = OrchestrationStatus.RUNNING, **fields: Any) -> MagicMock:
    state = MagicMock()
    state.name = name
    state.instance_id = "i1"
    state.runtime_status = status
    state.serialized_output = fields.get("output")
    state.serialized_custom_status = fields.get("custom")
    state.failure_details = fields.get("failure")
    state.created_at = datetime(2026, 1, 2, tzinfo=timezone.utc)
    state.last_updated_at = datetime(2026, 1, 3, tzinfo=timezone.utc)
    return state


def _body(response: func.HttpResponse) -> dict[str, Any]:
    return json.loads(response.get_body())


def _semantic(*observations: tuple[str, dict[str, Any]]) -> str:
    raw = empty_checkpoint("scope")
    raw["observations"] = [{"correlation": key, "response": value} for key, value in observations]
    raw["publishedEntryCount"] = 2 * len(observations)
    return json.dumps(raw)


def test_published_addresses_and_names() -> None:
    functions = _functions()
    assert {name: _route(function) for name, function in functions.items() if not name.startswith("BuiltIn__")} == {
        "http-routes_agent": "agents/routes-agent/run",
        "dafx-routes-start": "workflow/routes/run",
        "dafx-routes-status": "workflow/routes/status/{instanceId}",
        "dafx-routes-respond": "workflow/routes/respond/{instanceId}/{requestId}",
        "health_check": "health",
    }


def test_duplicate_name_and_unknown_profile_fail_closed() -> None:
    app = df.DFApp()
    register_checkpoint_routes(app, _workflow())
    register_checkpoint_routes(app, _workflow())
    with pytest.raises(ValueError, match="unique"):
        app.get_functions()
    with pytest.raises(ValueError, match="Unsupported"):
        register_checkpoint_routes(df.DFApp(), _workflow(), admission_profile="anything")


@pytest.mark.asyncio
async def test_migration_only_never_starts_a_root() -> None:
    client = MagicMock()
    client.schedule_new_orchestration = AsyncMock()
    response = await _call(_functions(), "dafx-routes-start", client, body={"x": 1})
    assert response.status_code == 403
    assert _body(response) == {"error": MIGRATION_ONLY_START_ERROR}
    client.schedule_new_orchestration.assert_not_called()


@pytest.mark.asyncio
async def test_new_workflows_only_start_contract() -> None:
    functions = _functions("new-workflows-only")
    client = MagicMock()
    client.schedule_new_orchestration = AsyncMock(return_value="run-1")
    response = await _call(
        functions,
        "dafx-routes-start",
        client,
        url=f"{_BASE}/workflow/routes/run",
        raw=b"hello",
        params={"runId": "run-1"},
        headers={},
    )
    assert response.status_code == 202
    assert _body(response) == {
        "instanceId": "run-1",
        "statusQueryGetUri": f"{_BASE}/workflow/routes/status/run-1",
        "respondUri": f"{_BASE}/workflow/routes/respond/run-1/{{requestId}}",
        "message": "Workflow started",
    }
    args = client.schedule_new_orchestration.call_args
    assert args.args == ("dafx-routes",)
    assert args.kwargs["instance_id"] == "run-1"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("params", "raw"),
    [
        ({"runId": "@bad"}, b"x"),
        ({"runId": "a/b"}, b"x"),
        ({"runId": "a" * 101}, b"x"),
        ({"waitForResponse": "maybe"}, b"x"),
        ({"waitForResponse": "true", "timeoutSeconds": "0"}, b"x"),
        ({"waitForResponse": "true", "timeoutSeconds": "201"}, b"x"),
        ({}, b""),
    ],
)
async def test_new_workflows_only_start_rejects_malformed_input(params: dict[str, str], raw: bytes) -> None:
    client = MagicMock()
    client.schedule_new_orchestration = AsyncMock()
    response = await _call(_functions("new-workflows-only"), "dafx-routes-start", client, raw=raw, params=params)
    assert response.status_code == 400
    client.schedule_new_orchestration.assert_not_called()


@pytest.mark.asyncio
async def test_status_scopes_ownership_and_projects_native_fields() -> None:
    functions = _functions()
    client = MagicMock()
    client.get_orchestration_state = AsyncMock(return_value=_state("dafx-other"))
    missing = await _call(functions, "dafx-routes-status", client, route_params={"instanceId": "i1"})
    assert (missing.status_code, _body(missing)) == (404, {"error": "Instance not found"})

    failure = MagicMock()
    failure.message = "boom"
    client.get_orchestration_state = AsyncMock(
        return_value=_state("DAFX-ROUTES", status=OrchestrationStatus.FAILED, failure=failure)
    )
    response = await _call(functions, "dafx-routes-status", client, route_params={"instanceId": "i1"})
    body = _body(response)
    assert response.status_code == 200
    assert body["runtimeStatus"] == "Failed"
    assert body["error"] == "boom"
    assert body["createdTime"] == "2026-01-02T00:00:00+00:00"


@pytest.mark.asyncio
async def test_status_publishes_pending_human_input_with_child_qualification() -> None:
    functions = _functions()
    parent = _state(
        "dafx-routes",
        custom=json.dumps({"subworkflows": {"child": {"0": "child-1"}}}),
    )
    child = _state(
        "dafx-child",
        custom=json.dumps({"pending_requests": {"r1": {"request_id": "r1", "source_executor_id": "e"}}}),
    )
    client = MagicMock()
    client.get_orchestration_state = AsyncMock(side_effect=lambda instance_id: parent if instance_id == "p" else child)
    response = await _call(
        functions,
        "dafx-routes-status",
        client,
        method="GET",
        url=f"{_BASE}/workflow/routes/status/p",
        route_params={"instanceId": "p"},
    )
    (pending,) = _body(response)["pendingHumanInputRequests"]
    assert pending["requestId"] == "child~0~r1"
    assert pending["respondUrl"] == f"{_BASE}/workflow/routes/respond/p/child~0~r1"


@pytest.mark.asyncio
async def test_respond_contract() -> None:
    functions = _functions()
    client = MagicMock()
    client.raise_orchestration_event = AsyncMock()
    route = {"instanceId": "i1", "requestId": "r1"}

    client.get_orchestration_state = AsyncMock(return_value=None)
    assert (await _call(functions, "dafx-routes-respond", client, body={}, route_params=route)).status_code == 404

    client.get_orchestration_state = AsyncMock(return_value=_state("dafx-routes", status=OrchestrationStatus.COMPLETED))
    assert (await _call(functions, "dafx-routes-respond", client, body={}, route_params=route)).status_code == 409

    client.get_orchestration_state = AsyncMock(return_value=_state("dafx-routes"))
    bad = await _call(functions, "dafx-routes-respond", client, raw=b"{", route_params=route)
    assert bad.status_code == 400
    ok = await _call(functions, "dafx-routes-respond", client, body={"approved": True}, route_params=route)
    assert ok.status_code == 200
    assert _body(ok) == {"message": "Response delivered successfully", "instanceId": "i1", "requestId": "r1"}
    client.raise_orchestration_event.assert_awaited_once_with("i1", "r1", data={"approved": True})


@pytest.mark.asyncio
async def test_respond_routes_qualified_child_request() -> None:
    functions = _functions()
    parent = _state("dafx-routes", custom=json.dumps({"subworkflows": {"child": {"0": "child-1"}}}))
    client = MagicMock()
    client.get_orchestration_state = AsyncMock(
        side_effect=lambda instance_id: parent if instance_id == "p" else _state("dafx-child")
    )
    client.raise_orchestration_event = AsyncMock()
    response = await _call(
        functions, "dafx-routes-respond", client, body=1, route_params={"instanceId": "p", "requestId": "child~0~r1"}
    )
    assert response.status_code == 200
    client.raise_orchestration_event.assert_awaited_once_with("child-1", "r1", data=1)


def _agent_client(raw_state: str | None) -> MagicMock:
    client = MagicMock()
    client.signal_entity = AsyncMock()
    entity = MagicMock()
    entity.get_state.return_value = raw_state
    client.get_entity = AsyncMock(return_value=None if raw_state is None else entity)
    return client


@pytest.mark.asyncio
async def test_agent_run_validates_input_and_accepts_without_waiting() -> None:
    functions = _functions()
    client = _agent_client(None)
    empty = await _call(functions, "http-routes_agent", client, body={"message": ""})
    assert empty.status_code == 400
    conflict = await _call(
        functions, "http-routes_agent", client, body={"message": "m", "sessionId": "a"}, params={"session_id": "b"}
    )
    assert conflict.status_code == 400
    assert "Conflicting session identifiers" in _body(conflict)["error"]
    invalid = await _call(
        functions, "http-routes_agent", client, raw=b"{", headers={"Content-Type": "application/json"}
    )
    assert invalid.status_code == 400
    client.signal_entity.assert_not_called()

    accepted = await _call(
        functions,
        "http-routes_agent",
        client,
        body={"message": "m", "sessionId": "s1"},
        params={"wait_for_response": "false"},
    )
    body = _body(accepted)
    assert accepted.status_code == 202
    assert body["status"] == "accepted"
    assert body["session_id"] == body["sessionId"] == "s1"
    assert body["correlation_id"] == body["correlationId"]
    entity_id, operation, request = client.signal_entity.call_args.args
    assert (str(entity_id), operation) == ("@dafx-routes-agent@s1", "run")
    assert request["correlationId"] == body["correlation_id"] or request["correlation_id"] == body["correlation_id"]


@pytest.mark.asyncio
async def test_agent_run_returns_first_matching_published_observation() -> None:
    functions = _functions()
    client = _agent_client(None)

    async def signal(entity_id: Any, operation: str, request: dict[str, Any]) -> None:
        correlation = request.get("correlationId") or request.get("correlation_id")
        if not isinstance(correlation, str):
            raise AssertionError("Accepted agent request requires a correlation ID.")
        second = serialize_agent_response(AgentResponse(messages=[Message(role="assistant", contents=["second"])]))
        entity = MagicMock()
        entity.get_state.return_value = _semantic((correlation, _SEMANTIC_RESPONSE), (correlation, second))
        client.get_entity = AsyncMock(return_value=entity)

    client.signal_entity = AsyncMock(side_effect=signal)
    response = await _call(functions, "http-routes_agent", client, body={"message": "m", "sessionId": "s1"})
    body = _body(response)
    assert response.status_code == 200
    assert (body["status"], body["response"], body["message_count"], body["messageCount"]) == (
        "success",
        "hello",
        4,
        4,
    )


@pytest.mark.asyncio
async def test_agent_run_times_out_without_matching_observation() -> None:
    functions = _functions()
    client = _agent_client(_semantic(("other", _SEMANTIC_RESPONSE)))
    response = await _call(functions, "http-routes_agent", client, body={"message": "m"})
    assert response.status_code == 500
    assert _body(response)["status"] == "timeout"


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["{}", "not json", json.dumps({"contract": "other"})])
async def test_agent_run_never_reads_non_semantic_state(state: str) -> None:
    functions = _functions()
    client = _agent_client(state)
    response = await _call(functions, "http-routes_agent", client, body={"message": "m"})
    body = _body(response)
    assert response.status_code == 500
    assert (body["status"], body["error_code"], body["errorCode"]) == ("error", "state_read_error", "state_read_error")


@pytest.mark.asyncio
async def test_agent_run_text_response_and_session_header() -> None:
    functions = _functions()
    client = _agent_client(None)
    response = await _call(
        functions,
        "http-routes_agent",
        client,
        raw=b"hi",
        params={"sessionId": "s2", "waitForResponse": "false"},
        headers={},
    )
    assert response.status_code == 202
    assert response.headers["x-ms-session-id"] == "s2"
    assert response.get_body() == b"Agent request accepted"


@pytest.mark.asyncio
async def test_health_lists_only_trusted_workflow_agents() -> None:
    response = await _call(_functions(), "health_check", MagicMock(), method="GET")
    health = json.loads(response.get_body())
    assert health["agent_count"] == 1
    assert health["agents"][0]["name"] == "routes-agent"
    assert health["agents"][0]["mcp_tool_enabled"] is False


def test_routes_import_without_ordinary_host_or_migration_compiler() -> None:
    package = Path(__file__).resolve().parents[1]
    script = (
        "import sys\n"
        "blocked = ('agent_framework_azurefunctions._app', 'agent_framework_durabletask.checkpoint_migration')\n"
        "for name in blocked:\n"
        "    sys.modules[name] = None\n"
        "import agent_framework_azurefunctions._checkpoint_routes\n"
        "assert all(sys.modules[name] is None for name in blocked)\n"
    )
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, cwd=package, check=False)
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
@pytest.mark.parametrize("as_text", [False, True])
@pytest.mark.parametrize(
    ("params", "body", "deprecated"),
    [
        ({"sessionId": "s", "waitForResponse": "false"}, {"message": "m"}, False),
        ({"session_id": "s", "waitForResponse": "false"}, {"message": "m"}, True),
        ({"thread_id": "s", "waitForResponse": "false"}, {"message": "m"}, True),
        ({"wait_for_response": "false"}, {"message": "m"}, True),
        ({"waitForResponse": "false"}, {"message": "m", "session_id": "s"}, True),
        ({"waitForResponse": "false"}, {"message": "m", "thread_id": "s"}, True),
        ({"waitForResponse": "false"}, {"message": "m", "wait_for_response": False}, True),
        ({"waitForResponse": "false"}, {"message": "m", "sessionId": "s"}, False),
    ],
)
async def test_legacy_agent_aliases_carry_deprecation_headers(
    params: dict[str, str], body: dict[str, Any], deprecated: bool, as_text: bool
) -> None:
    functions = _functions()
    client = _agent_client(None)
    if as_text:
        response = await _call(functions, "http-routes_agent", client, raw=b"m", params=params, headers={})
    else:
        response = await _call(functions, "http-routes_agent", client, body=body, params=params)
    headers = {key.lower(): value for key, value in response.headers.items()}
    assert response.status_code == 202
    names = {"deprecation", "link", "warning"}
    if as_text:
        deprecated = bool({"session_id", "thread_id", "wait_for_response"} & params.keys())
        assert headers["x-ms-session-id"]
    if not deprecated:
        assert not names & headers.keys()
        return
    assert headers["deprecation"] == "@1789430400"
    assert headers["link"].endswith('http-api-camelcase-migration.md>; rel="deprecation"')
    assert headers["warning"] == (
        '299 - "Deprecated agent HTTP field names are supported temporarily; use sessionId and waitForResponse."'
    )
