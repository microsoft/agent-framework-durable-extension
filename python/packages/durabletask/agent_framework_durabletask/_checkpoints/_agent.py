# Copyright (c) Microsoft. All rights reserved.

"""New semantic entity checkpoint with genuine current-core execution.

The one-time bridge converter projects readable observations without creating
historical completion/ingestion receipts. Target execution uses current v2 state
with explicit transcript and stream-failure policies, not an old schema reader
or imported published invocation implementation.
"""

import hashlib
import json
from copy import copy, deepcopy
from datetime import datetime, timezone
from typing import Any, Protocol, TypeGuard, cast

from agent_framework import Agent, AgentMiddleware, AgentResponse, Content, Message, SupportsAgentRun
from durabletask.entities import EntityContext

from agent_framework_durabletask._async_bridge import run_agent_coroutine
from agent_framework_durabletask._entities import (
    AgentEntity,
    DurableTaskEntityStateProvider,
    filter_already_stored_messages,
)
from agent_framework_durabletask._json_payload import JsonPayload
from agent_framework_durabletask._models import RunRequest
from agent_framework_durabletask._response_utils import (
    is_terminal_agent_response,
    serialize_agent_response,
    serialize_input_message,
)
from agent_framework_durabletask._shared_agent_state import DurableAgentState, DurableAgentStateRequest

from ._types import JsonObject, OperationAttempt

CONTRACT = "agent-semantic-checkpoint/1"
RETRY_POLICY = "published-native-operation-attempts/2"
PUBLIC_POLICY = "published-python-invocation-public-view/2"
REPLAY_POLICY = "published-cold-content-replay/2"


class _SupportsContextProviders(Protocol):
    context_providers: Any


def _is_core_agent(agent: SupportsAgentRun) -> TypeGuard[Agent[Any]]:
    return isinstance(agent, Agent)


def _published_message(message: Any, *, include_reasoning: bool) -> Message | None:
    contents: list[Content] = []
    for item in message.contents:
        if item.type in ("reasoning", "text_reasoning"):
            if include_reasoning:
                contents.append(Content.from_text_reasoning(text=item.text))
        elif item.type == "function_call":
            arguments = item.arguments
            if isinstance(arguments, str):
                try:
                    arguments = json.loads(arguments)
                except json.JSONDecodeError:
                    arguments = {}
            contents.append(
                Content.from_function_call(
                    call_id=item.call_id,
                    name=item.name,
                    arguments=json.dumps(arguments if arguments is not None else {}),
                )
            )
        elif item.type == "function_result":
            contents.append(Content.from_function_result(call_id=item.call_id, result=deepcopy(item.result)))
        elif item.type == "text":
            contents.append(Content.from_text(item.text))
        elif item.type == "error":
            contents.append(
                Content.from_error(
                    message=item.message,
                    error_code=item.error_code,
                    error_details=item.error_details,
                )
            )
        elif item.type in ("data", "uri"):
            contents.append(Content.from_uri(uri=item.uri, media_type=item.media_type))
        elif item.type == "hosted_file":
            contents.append(Content.from_hosted_file(file_id=item.file_id))
        elif item.type == "hosted_vector_store":
            contents.append(Content.from_hosted_vector_store(vector_store_id=item.vector_store_id))
        elif item.type == "usage":
            usage_details = item.usage_details
            usage: Any = {} if usage_details is None else deepcopy(usage_details)
            for key in ("input_token_count", "output_token_count", "total_token_count"):
                usage.setdefault(key, None)
            contents.append(Content.from_usage(usage_details=usage))
        else:
            contents.append(Content.from_dict(deepcopy(item.to_dict())))
    if not contents:
        return None
    return Message(
        message.role,
        contents,
        author_name=message.author_name,
        additional_properties=deepcopy(message.additional_properties),
    )


def published_replay_message(message: Any) -> Message | None:
    """Project current Core content through the published persisted replay policy."""
    return _published_message(message, include_reasoning=False)


def _published_poll_message(message: Any) -> Message | None:
    return _published_message(message, include_reasoning=True)


class PublishedCheckpointReplay(AgentMiddleware):
    def __init__(self, messages: list[JsonObject]) -> None:
        self.messages = messages

    async def process(self, context: Any, call_next: Any) -> None:
        from agent_framework_durabletask._response_utils import load_agent_response

        context.messages = load_agent_response({"messages": deepcopy(self.messages)}).messages
        await call_next()


class PublishedCheckpointReplayAgent:
    """Apply the published transcript at the SupportsAgentRun message boundary."""

    __slots__ = ("_agent", "_messages", "description", "id", "name")

    def __init__(self, agent: SupportsAgentRun, messages: list[JsonObject]) -> None:
        from agent_framework_durabletask._response_utils import load_agent_response

        self._agent = agent
        self._messages = load_agent_response({"messages": deepcopy(messages)}).messages
        self.id = agent.id
        self.name = agent.name
        self.description = agent.description

    @property
    def context_providers(self) -> Any:
        return getattr(self._agent, "context_providers", None)

    @context_providers.setter
    def context_providers(self, providers: Any) -> None:
        cast(_SupportsContextProviders, self._agent).context_providers = providers

    def __copy__(self) -> "PublishedCheckpointReplayAgent":
        clone = object.__new__(PublishedCheckpointReplayAgent)
        object.__setattr__(clone, "_agent", copy(self._agent))
        object.__setattr__(clone, "_messages", deepcopy(self._messages))
        object.__setattr__(clone, "description", self.description)
        object.__setattr__(clone, "id", self.id)
        object.__setattr__(clone, "name", self.name)
        return clone

    def __getattr__(self, name: str) -> Any:
        try:
            agent = object.__getattribute__(self, "_agent")
        except AttributeError:
            raise AttributeError(name) from None
        return getattr(agent, name)

    @property
    def _stream_capability_agent(self) -> SupportsAgentRun:
        return self._agent

    def create_session(self, *, session_id: str | None = None) -> Any:
        return self._agent.create_session(session_id=session_id)

    def get_session(self, service_session_id: Any, *, session_id: str | None = None) -> Any:
        return self._agent.get_session(service_session_id, session_id=session_id)

    def run(self, messages: Any = None, **kwargs: Any) -> Any:
        run: Any = self._agent.run
        return run(messages=deepcopy(self._messages), **kwargs)


def published_error_view(response: Any, correlation: str) -> AgentResponse[Any]:
    """Preserve the old public error envelope without changing current receipts."""
    properties: dict[str, Any] = {} if response.additional_properties is None else response.additional_properties
    if properties.get("durable_status") != "error" or properties.get("correlation_id") != correlation:
        return response
    errors = [content for message in response.messages for content in message.contents if content.type == "error"]
    if len(errors) != 1:
        raise ValueError("Unexpected current generated error shape.")
    error = errors[0]
    return AgentResponse(
        messages=[
            Message(
                "assistant",
                [
                    Content.from_error(message=error.message, error_code=error.error_code),
                ],
            )
        ],
        created_at=response.created_at,
    )


def published_poll_view(response: Any, *, generated_failure: bool = False) -> AgentResponse[Any]:
    """The published poll reader reconstructs stored fields, not the native return."""
    usage_details = response.usage_details
    usage: Any = {} if usage_details is None else deepcopy(usage_details)
    if not generated_failure and response.usage_details is not None:
        for key in ("input_token_count", "output_token_count", "total_token_count"):
            usage.setdefault(key, None)
    return AgentResponse(
        messages=[
            _published_poll_message(message)
            or Message(
                message.role,
                [],
                author_name=message.author_name,
                additional_properties=deepcopy(message.additional_properties),
            )
            for message in response.messages
        ],
        created_at=response.created_at or datetime.now(timezone.utc).isoformat(),
        usage_details=usage,
    )


def digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def empty_checkpoint(entity_scope: str) -> JsonObject:
    from agent_framework_durabletask._shared_agent_state import DurableAgentState

    return {
        "contract": CONTRACT,
        "historyInput": "full-transcript/1",
        "publicInvocationPolicy": PUBLIC_POLICY,
        "publicReplayPolicy": REPLAY_POLICY,
        "publishedReplayMessages": [],
        "publishedEntryCount": 0,
        "retryPolicy": RETRY_POLICY,
        "retryAttempts": {},
        "nativeOperations": {},
        "invocationFailurePolicy": "current-v1",
        "sourceWriter": "checkpoint-native/1",
        "sourceSessionObservation": None,
        "sourceOptionalUsage": {"profile": "published-optional-usage/1", "entries": []},
        "sourceDigest": digest({"origin": "checkpoint-native/1", "entity": entity_scope}),
        "runtime": DurableAgentState().to_dict(),
        "observations": [],
    }


def select_operation_attempt(
    checkpoint: JsonObject,
    request: RunRequest,
    native_operation_id: str | None,
    *,
    entity_scope: str,
    request_digest: str | None = None,
) -> tuple[JsonObject, RunRequest, OperationAttempt]:
    """Separate public polling identity from an authoritative native operation."""
    if not isinstance(native_operation_id, str) or not native_operation_id:
        raise ValueError("Checkpoint execution requires authoritative EntityContext.operation_id.")
    if not isinstance(entity_scope, str) or not entity_scope:
        raise ValueError("Checkpoint execution requires its actual native entity address.")
    candidate = deepcopy(checkpoint)
    if candidate.setdefault("nativeOperationEntity", entity_scope) != entity_scope:
        raise ValueError("Native operation state belongs to a different entity address.")
    policy = candidate.setdefault("retryPolicy", RETRY_POLICY)
    if policy != RETRY_POLICY:
        raise ValueError("Unsupported checkpoint correlation retry policy.")
    operations: dict[str, OperationAttempt] = candidate.setdefault("nativeOperations", {})
    attempts: list[OperationAttempt] = candidate.setdefault("retryAttempts", {}).setdefault(request.correlation_id, [])
    receipts = candidate["runtime"]["data"].get("completionReceipts", {})
    fingerprint = request_digest if request_digest is not None else digest(request.to_dict())
    selected: OperationAttempt | None = operations.get(native_operation_id)
    if selected is not None:
        if selected["originalCorrelation"] != request.correlation_id or selected["requestDigest"] != fingerprint:
            raise ValueError("A native operation identity cannot be reused for a different request.")
    else:
        runtime_correlation = request.correlation_id
        if attempts or runtime_correlation in receipts:
            runtime_correlation = "operation-" + digest({
                "correlation": request.correlation_id,
                "nativeOperationId": native_operation_id,
                "sourceDigest": candidate["sourceDigest"],
                "entity": entity_scope,
            })
            if runtime_correlation in receipts:
                raise ValueError("Native operation runtime correlation already belongs to a committed result.")
        selected = {
            "nativeOperationId": native_operation_id,
            "originalCorrelation": request.correlation_id,
            "requestDigest": fingerprint,
            "runtimeCorrelation": runtime_correlation,
            "attempt": len(attempts),
        }
        attempts.append(selected)
        operations[native_operation_id] = selected
    invocation = deepcopy(request)
    invocation.correlation_id = selected["runtimeCorrelation"]
    if invocation.context_messages is not None:
        originals = invocation.context_message_ids or [
            message.get("message_id") or str(index) for index, message in enumerate(invocation.context_messages)
        ]
        invocation.context_message_ids = [
            "retry-input-"
            + digest({
                "nativeOperationId": native_operation_id,
                "entity": entity_scope,
                "occurrence": identity,
            })
            for identity in originals
        ]
        selected["inputOccurrences"] = [
            {"original": original, "runtime": runtime}
            for original, runtime in zip(originals, invocation.context_message_ids)
        ]
    return candidate, invocation, selected


class CheckpointEntityStateProvider(DurableTaskEntityStateProvider):
    """Typed semantic state boundary for the separate migration-only converter."""

    _checkpoint: JsonObject | None

    def _allowed_entity_operations(self) -> tuple[str, ...]:
        return ("run", "reset", "delete")

    def _initialize_entity_context(self, context: EntityContext) -> None:
        if context.operation not in self._allowed_entity_operations():
            raise ValueError(f"Checkpoint entity operation '{context.operation}' is not supported.")
        super()._initialize_entity_context(context)

    def checkpoint(self) -> JsonObject:
        raise NotImplementedError


def create_entity(agent: SupportsAgentRun, *, entity_id: str) -> type[CheckpointEntityStateProvider]:
    class CheckpointEntity(CheckpointEntityStateProvider):
        def __init__(self):
            super().__init__()
            self._checkpoint = None
            self._agent_entity = None

        def checkpoint(self) -> JsonObject:
            if self._checkpoint is None:
                raw = super()._get_state_dict()
                if raw.get("contract") != CONTRACT:
                    if not raw:
                        raw = empty_checkpoint(str(self.entity_context.entity_id))
                    else:
                        raise ValueError("Entity requires an authorized semantic checkpoint conversion.")
                    super()._set_state_dict(raw)
                if raw.get("historyInput") != "full-transcript/1":
                    raise ValueError("Checkpoint history input policy does not match this deployment.")
                self._checkpoint = deepcopy(raw)
            return self._checkpoint

        def _get_state_dict(self) -> dict[str, Any]:
            return deepcopy(self.checkpoint()["runtime"])

        def _set_state_dict(self, state: dict[str, Any]) -> None:
            candidate = deepcopy(self.checkpoint())
            candidate["runtime"] = deepcopy(state)
            super()._set_state_dict(candidate)
            self._checkpoint = candidate

        def run(self, request: JsonPayload) -> JsonObject:
            payload: Any = json.loads(request) if isinstance(request, str) else request
            parsed = RunRequest.from_dict(payload)
            parsed.validate_context()
            checkpoint = self.checkpoint()
            self.ensure_v2_writable()
            native_operation_id = getattr(self.entity_context, "operation_id", None)
            checkpoint, invocation, selected = select_operation_attempt(
                checkpoint,
                parsed,
                native_operation_id,
                entity_scope=str(self.entity_context.entity_id),
                request_digest=digest(payload),
            )
            runtime_state = DurableAgentState.from_dict(checkpoint["runtime"])
            for observation in checkpoint["observations"]:
                if observation.get("nativeOperationId") == native_operation_id:
                    committed_response = runtime_state.try_get_agent_response(selected["runtimeCorrelation"])
                    if committed_response is None:
                        raise ValueError("A cached native operation observation has no runtime completion receipt.")
                    if committed_response.additional_properties.get("durable_status") != "already_completed":
                        return deepcopy(observation.get("nativeResponse", observation["response"]))
                    break
            completed_before_run = (
                selected["runtimeCorrelation"] in checkpoint["runtime"]["data"].get("completionReceipts", {})
            )
            if checkpoint.get("publicInvocationPolicy") != PUBLIC_POLICY:
                raise ValueError("Unsupported public invocation compatibility policy.")
            if checkpoint.get("publicReplayPolicy") != REPLAY_POLICY:
                raise ValueError("Checkpoint is missing its explicit published public replay contract.")
            replay_messages = deepcopy(checkpoint["publishedReplayMessages"])
            if invocation.context_messages is None:
                replay_messages.append(Message(parsed.role, [Content.from_text(parsed.message)]).to_dict())
            else:
                replay_request = DurableAgentStateRequest.from_run_request(invocation)
                replay_inputs = filter_already_stored_messages(
                    replay_request.messages,
                    ingested_messages=deepcopy(self.state.data.ingested_messages),
                    occurrence_ids=invocation.context_message_ids,
                )
                replay_messages.extend(
                    serialize_input_message(replay_input.to_chat_message()) for replay_input in replay_inputs
                )
            # Published False sets a missing tools option, but keeps configured tools.
            if not invocation.enable_tool_calls:
                invocation.enable_tool_calls = True
                invocation.options.setdefault("tools", None)
            super()._set_state_dict(checkpoint)
            self._checkpoint = checkpoint
            from agent_framework_durabletask._entities import invocation_failure_policy_from_checkpoint

            invocation_agent: SupportsAgentRun
            if _is_core_agent(agent):
                core_invocation = copy(agent)
                core_invocation.middleware = [
                    PublishedCheckpointReplay(replay_messages),
                    *(core_invocation.middleware or []),
                ]
                invocation_agent = core_invocation
            else:
                invocation_agent = PublishedCheckpointReplayAgent(agent, replay_messages)
            self._agent_entity = AgentEntity(
                invocation_agent,
                state_provider=self,
                retention="keep_all",
                max_state_bytes=None,
                history_input="transcript",
                invocation_failure_policy=invocation_failure_policy_from_checkpoint(checkpoint),
            )
            core_response = run_agent_coroutine(self._agent_entity.run(invocation))
            if (
                completed_before_run
                or core_response.additional_properties.get("durable_status") == "already_completed"
            ):
                response = serialize_agent_response(
                    published_error_view(core_response, invocation.correlation_id)
                )
                properties = response.get("additional_properties", {})
                if properties.get("correlation_id") == invocation.correlation_id:
                    properties["correlation_id"] = parsed.correlation_id
                return response
            public_response = published_error_view(core_response, invocation.correlation_id)
            response = serialize_agent_response(public_response)
            # Public correlation stays original; genuine receipt identity is recorded below.
            properties = response.get("additional_properties", {})
            if properties.get("correlation_id") == invocation.correlation_id:
                properties["correlation_id"] = parsed.correlation_id
            checkpoint = deepcopy(self.checkpoint())
            checkpoint["publishedReplayMessages"] = replay_messages + [
                replay.to_dict()
                for message in public_response.messages
                if (replay := published_replay_message(message)) is not None
            ]
            checkpoint["publishedEntryCount"] += 2
            checkpoint["observations"].append({
                "correlation": parsed.correlation_id,
                "response": serialize_agent_response(
                    published_poll_view(
                        public_response,
                        generated_failure=public_response is not core_response,
                    )
                ),
                "nativeResponse": response,
                "runtimeCorrelation": selected["runtimeCorrelation"],
                "attempt": selected["attempt"],
                "nativeOperationId": native_operation_id,
                "evidence": "target-returned-response-with-published-poll-view",
                "completionTime": None,
                "outcome": "failed" if is_terminal_agent_response(core_response) else "succeeded",
            })
            super()._set_state_dict(checkpoint)
            self._checkpoint = checkpoint
            return response

        def reset(self) -> None:
            checkpoint = self.checkpoint()
            self.ensure_v2_writable()
            if self._agent_entity is None:
                from agent_framework_durabletask._entities import invocation_failure_policy_from_checkpoint

                self._agent_entity = AgentEntity(
                    agent,
                    state_provider=self,
                    retention="keep_all",
                    max_state_bytes=None,
                    history_input="transcript",
                    invocation_failure_policy=invocation_failure_policy_from_checkpoint(checkpoint),
                )
            self._agent_entity.reset()
            candidate = deepcopy(self.checkpoint())
            candidate["publishedReplayMessages"] = []
            candidate["publishedEntryCount"] = 0
            candidate["observations"] = []
            super()._set_state_dict(candidate)
            self._checkpoint = candidate
            self._state_cache = None
            self._agent_entity = None

        def delete(self, input: Any = None) -> None:
            super().delete(input)
            self._checkpoint = None
            self._state_cache = None
            self._agent_entity = None

    CheckpointEntity.__name__ = "dafx-" + entity_id
    CheckpointEntity.__qualname__ = CheckpointEntity.__name__
    return CheckpointEntity
