# Copyright (c) Microsoft. All rights reserved.

"""Native entity-boundary regression for checkpoint replay into custom agents."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import Mock

import pytest
from _history_pipeline_test_support import ToolChatClient
from agent_framework import (
    Agent,
    AgentContext,
    AgentMiddleware,
    AgentResponse,
    AgentResponseUpdate,
    Content,
    HistoryProvider,
    Message,
    ResponseStream,
    SupportsAgentRun,
)
from durabletask.entities import EntityInstanceId
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.internal.entity_state_shim import StateShim
from durabletask.task import EntityContext
from durabletask.worker import TaskHubGrpcWorker

from agent_framework_durabletask import _delivery_state
from agent_framework_durabletask._checkpoints._agent import (
    PublishedCheckpointReplayAgent,
    create_entity,
    empty_checkpoint,
)
from agent_framework_durabletask._entities import InvocationFailurePolicy, invocation_failure_policy_from_checkpoint
from agent_framework_durabletask._history_provider import prepare_history_owner
from agent_framework_durabletask._shared_agent_state import DurableAgentState

_ENTITY_ADDRESS = "@dafx-checkpoint-custom-agent@session-1"


class _EntityContextWithOperationId(EntityContext):
    operation_id: str


class _SlottedCustomAgent:
    __slots__ = ("id", "name", "description", "calls", "context_providers")

    id: str
    name: str | None
    description: str | None
    calls: list[tuple[bool, list[Message]]]
    context_providers: list[HistoryProvider]

    def __init__(self) -> None:
        self.id = "slotted-custom"
        self.name = "slotted-custom"
        self.description = "Custom provider without Agent middleware support"
        self.calls: list[tuple[bool, list[Message]]] = []
        self.context_providers = []

    def run(self, messages: Any = None, *, stream: bool = False, **kwargs: Any) -> Any:
        transcript = list(messages or [])
        self.calls.append((stream, transcript))

        if stream:
            if any(content.text == "second prompt" for message in transcript for content in message.contents):
                raise TypeError("stream is not supported")

            async def updates() -> AsyncIterator[AgentResponseUpdate]:
                if False:
                    yield AgentResponseUpdate()

            return ResponseStream(
                updates(),
                finalizer=lambda _: AgentResponse(messages=[Message("assistant", ["first answer"])]),
            )

        async def response() -> AgentResponse[Any]:
            return AgentResponse(messages=[Message("assistant", ["second answer"])])

        return response()

    def create_session(self, *, session_id: str | None = None) -> Any:
        raise NotImplementedError

    def get_session(self, service_session_id: Any, *, session_id: str | None = None) -> Any:
        raise NotImplementedError


class _ExternalHistoryProvider(HistoryProvider):
    def __init__(self) -> None:
        super().__init__("checkpoint-external")

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        return []

    async def save_messages(
        self,
        session_id: str | None,
        messages: Sequence[Message],
        **kwargs: Any,
    ) -> None:
        return None


def _run_native_operation(
    worker: TaskHubGrpcWorker,
    *,
    request_id: str,
    message: str,
    state: str | None,
) -> Any:
    request = pb.EntityBatchRequest(
        instanceId=_ENTITY_ADDRESS,
        entityState=helpers.get_string_value(state),
        operations=[
            pb.OperationRequest(
                operation="run",
                requestId=request_id,
                input=helpers.get_string_value(
                    json.dumps({"message": message, "correlationId": "same-public-correlation"})
                ),
            )
        ],
    )
    return worker._execute_entity_batch(request, Mock(), "checkpoint-custom-test")


def _texts(messages: Sequence[Message]) -> list[list[str | None]]:
    return [[content.text for content in message.contents] for message in messages]


def _direct_entity(
    entity_type: type,
    *,
    state: dict[str, Any] | None,
    operation: str,
    operation_id: str | None = None,
) -> tuple[Any, EntityContext]:
    worker = TaskHubGrpcWorker(channel=Mock(), emit_trace_spans=False)
    encoded = None if state is None else json.dumps(state)
    context = _EntityContextWithOperationId(
        "checkpoint-custom-test",
        operation,
        StateShim(encoded, worker._data_converter, is_serialized=encoded is not None),
        EntityInstanceId("dafx-checkpoint-custom-agent", "session-1"),
        worker._data_converter,
    )
    if operation_id is not None:
        context.operation_id = operation_id
    entity = entity_type()
    entity._initialize_entity_context(context)
    return entity, context


def _entity_state(context: EntityContext) -> dict[str, Any]:
    return context.get_state(dict, default={})


def test_native_entity_replay_preserves_slotted_agent_transcript_and_request_identity() -> None:
    if not hasattr(EntityContext, "operation_id"):
        pytest.skip("Requires the proposed SDK entity operation identity API.")
    agent = _SlottedCustomAgent()
    assert isinstance(agent, SupportsAgentRun)
    entity_type = create_entity(agent, entity_id="checkpoint-custom-agent")
    worker = TaskHubGrpcWorker(channel=Mock(), emit_trace_spans=False)
    worker._registry.add_entity(entity_type)

    first = _run_native_operation(
        worker,
        request_id="native-first",
        message="first prompt",
        state=None,
    )
    assert first.results[0].HasField("success"), first
    assert _texts(agent.calls[0][1]) == [["first prompt"]]

    second = _run_native_operation(
        worker,
        request_id="native-second",
        message="second prompt",
        state=first.entityState.value,
    )
    assert second.results[0].HasField("success"), second
    expected_transcript = [["first prompt"], ["first answer"], ["second prompt"]]
    assert _texts(agent.calls[1][1]) == expected_transcript
    assert _texts(agent.calls[2][1]) == expected_transcript
    assert [stream for stream, _ in agent.calls] == [True, True, False]

    checkpoint = json.loads(second.entityState.value)
    assert set(checkpoint["nativeOperations"]) == {"native-first", "native-second"}
    assert [row["nativeOperationId"] for row in checkpoint["observations"]] == [
        "native-first",
        "native-second",
    ]
    assert checkpoint["observations"][0]["correlation"] == checkpoint["observations"][1]["correlation"]

    redelivery = _run_native_operation(
        worker,
        request_id="native-first",
        message="first prompt",
        state=second.entityState.value,
    )
    assert redelivery.results[0].HasField("success"), redelivery
    assert redelivery.results[0].success.result.value == first.results[0].success.result.value
    assert len(agent.calls) == 3


@pytest.mark.parametrize("operation", ["set_state", "import_checkpoint"])
def test_cold_checkpoint_entity_rejects_untrusted_native_operations(operation: str) -> None:
    agent = _SlottedCustomAgent()
    worker = TaskHubGrpcWorker(channel=Mock(), emit_trace_spans=False)
    worker._registry.add_entity(create_entity(agent, entity_id="checkpoint-custom-agent"))
    request = pb.EntityBatchRequest(
        instanceId=_ENTITY_ADDRESS,
        operations=[
            pb.OperationRequest(
                operation=operation,
                requestId="unauthorized-operation",
                input=helpers.get_string_value(json.dumps({"contract": "agent-semantic-checkpoint/1"})),
            )
        ],
    )

    result = worker._execute_entity_batch(request, Mock(), "checkpoint-custom-test")

    assert result.results[0].HasField("failure"), result
    assert agent.calls == []


class _AppendCustomerMessage(AgentMiddleware):
    def __init__(self) -> None:
        self.seen: list[str | None] = []

    async def process(self, context: AgentContext, call_next: Callable[[], Awaitable[None]]) -> None:
        self.seen = [content.text for message in context.messages for content in message.contents]
        context.messages = [*context.messages, Message("system", ["customer middleware addition"])]
        await call_next()


def test_imported_transcript_runs_before_customer_middleware() -> None:
    client = ToolChatClient(tool_calls=False)
    middleware = _AppendCustomerMessage()
    agent = Agent(client=client, middleware=[middleware], name="checkpoint-middleware")
    checkpoint = empty_checkpoint(_ENTITY_ADDRESS)
    checkpoint["publishedReplayMessages"] = [Message("user", ["imported transcript"]).to_dict()]
    entity_type = create_entity(agent, entity_id="checkpoint-custom-agent")
    entity, _ = _direct_entity(entity_type, state=checkpoint, operation="run", operation_id="native-middleware")

    result = entity.run({"message": "new prompt", "correlationId": "middleware-correlation"})

    assert result["messages"]
    assert middleware.seen == ["imported transcript", "new prompt"]
    assert _texts(client.received_messages[0]) == [
        ["imported transcript"],
        ["new prompt"],
        ["customer middleware addition"],
    ]


def test_custom_checkpoint_replay_copies_agent_before_replacing_history_provider() -> None:
    agent = _SlottedCustomAgent()
    provider = _ExternalHistoryProvider()
    agent.context_providers = [provider]
    replay = PublishedCheckpointReplayAgent(agent, [])

    prepared = prepare_history_owner(replay, service_owns_history=True)

    assert isinstance(prepared, PublishedCheckpointReplayAgent)
    assert prepared is not replay
    replacement = prepared.context_providers[0]
    assert getattr(replacement, "__wrapped__", None) is provider
    assert agent.context_providers == [provider]
    asyncio.run(prepared.run(messages=[]))
    assert agent.calls == [(False, [])]


def test_checkpoint_replays_explicit_context_messages_and_occurrences() -> None:
    agent = _SlottedCustomAgent()
    entity_type = create_entity(agent, entity_id="checkpoint-custom-agent")
    entity, context = _direct_entity(
        entity_type,
        state=empty_checkpoint(_ENTITY_ADDRESS),
        operation="run",
        operation_id="native-context",
    )
    context_messages = [
        Message("system", ["system context"], message_id="system-context"),
        Message(
            "tool",
            [Content.from_function_result(call_id="lookup", result={"ok": True})],
            message_id="tool-result",
        ),
        Message(
            "user",
            [Content.from_uri("https://example.test/context.png", media_type="image/png")],
            message_id="image-context",
        ),
        Message("user", ["same occurrence payload"], message_id="shared-message-id"),
        Message("user", ["same occurrence payload"], message_id="shared-message-id"),
        Message("user", ["same occurrence payload"], message_id="shared-message-id"),
    ]
    context_message_ids = ["system", "tool", "image", "occurrence-1", "occurrence-2", "occurrence-2"]
    expected = [message.to_dict() for message in context_messages[:5]]

    entity.run({
        "message": "logging preview must not be replayed",
        "correlationId": "context-only",
        "contextMessages": [message.to_dict() for message in context_messages],
        "contextMessageIds": context_message_ids,
    })

    assert agent.calls
    assert all([message.to_dict() for message in transcript] == expected for _, transcript in agent.calls)
    checkpoint = _entity_state(context)
    assert checkpoint["publishedReplayMessages"][: len(expected)] == expected
    assert "logging preview must not be replayed" not in _texts(agent.calls[-1][1])[0]


def test_checkpoint_empty_context_does_not_replay_logging_preview() -> None:
    agent = _SlottedCustomAgent()
    entity, _ = _direct_entity(
        create_entity(agent, entity_id="checkpoint-custom-agent"),
        state=empty_checkpoint(_ENTITY_ADDRESS),
        operation="run",
        operation_id="native-empty-context",
    )

    entity.run({
        "message": "preview only",
        "correlationId": "empty-context",
        "contextMessages": [],
        "contextMessageIds": [],
    })

    assert agent.calls
    assert all(transcript == [] for _, transcript in agent.calls)


def test_core_checkpoint_replay_uses_explicit_context_not_preview() -> None:
    client = ToolChatClient(tool_calls=False)
    middleware = _AppendCustomerMessage()
    agent = Agent(client=client, middleware=[middleware], name="checkpoint-context")
    context_message = Message("system", ["explicit context"])
    entity, _ = _direct_entity(
        create_entity(agent, entity_id="checkpoint-custom-agent"),
        state=empty_checkpoint(_ENTITY_ADDRESS),
        operation="run",
        operation_id="native-core-context",
    )

    entity.run({
        "message": "preview only",
        "correlationId": "core-context",
        "contextMessages": [context_message.to_dict()],
        "contextMessageIds": ["context-occurrence"],
    })

    assert middleware.seen == ["explicit context"]
    assert _texts(client.received_messages[0]) == [["explicit context"], ["customer middleware addition"]]


class _ReasoningAgent(_SlottedCustomAgent):
    def run(self, messages: Any = None, *, stream: bool = False, **kwargs: Any) -> Any:
        transcript = list(messages or [])
        self.calls.append((stream, transcript))
        if stream:
            raise TypeError("stream is not supported")

        async def response() -> AgentResponse[Any]:
            return AgentResponse(
                messages=[
                    Message(
                        "assistant",
                        [
                            Content.from_text_reasoning(text="private rationale"),
                            Content.from_text("visible answer"),
                        ],
                    )
                ]
            )

        return response()


class _EffectBeforeStreamFailureAgent(_SlottedCustomAgent):
    __slots__ = ("effects",)

    def __init__(self) -> None:
        super().__init__()
        self.effects = 0

    def run(self, messages: Any = None, *, stream: bool = False, **kwargs: Any) -> Any:
        transcript = list(messages or [])
        self.calls.append((stream, transcript))
        self.effects += 1
        if stream:
            raise RuntimeError("stream failed after the effect")

        async def response() -> AgentResponse[Any]:
            return AgentResponse(messages=[Message("assistant", ["unexpected retry"])])

        return response()


class _NoStreamSignatureAgent:
    def __init__(self) -> None:
        self.id = "no-stream-signature"
        self.name = self.id
        self.description = self.id
        self.calls = 0

    def run(self, messages: Any = None, *, options: Any = None) -> AgentResponse[Any]:
        self.calls += 1
        return AgentResponse(messages=[Message("assistant", ["no-stream fallback"])])

    def create_session(self, *, session_id: str | None = None) -> Any:
        raise NotImplementedError

    def get_session(self, service_session_id: Any, *, session_id: str | None = None) -> Any:
        raise NotImplementedError


class _StreamRefusalCallable:
    def __init__(self, calls: list[bool]) -> None:
        self.calls = calls

    def __call__(self, messages: Any = None, *, stream: bool = False, options: Any = None) -> AgentResponse[Any]:
        self.calls.append(stream)
        if stream:
            raise TypeError("stream is not supported")
        return AgentResponse(messages=[Message("assistant", ["no-stream fallback"])])


class _CallableRunAgent:
    __slots__ = ("calls", "description", "id", "name", "run")

    def __init__(self) -> None:
        self.id = "callable-run"
        self.name = self.id
        self.description = self.id
        self.calls: list[bool] = []
        self.run = _StreamRefusalCallable(self.calls)

    def create_session(self, *, session_id: str | None = None) -> Any:
        raise NotImplementedError

    def get_session(self, service_session_id: Any, *, session_id: str | None = None) -> Any:
        raise NotImplementedError


def test_checkpoint_stream_failure_does_not_repeat_possible_effect() -> None:
    agent = _EffectBeforeStreamFailureAgent()
    checkpoint = empty_checkpoint(_ENTITY_ADDRESS)
    assert invocation_failure_policy_from_checkpoint(checkpoint) is InvocationFailurePolicy.CURRENT_V1
    with pytest.raises(ValueError, match="Unsupported semantic invocation failure policy"):
        invocation_failure_policy_from_checkpoint({
            **checkpoint,
            "invocationFailurePolicy": "published-stream-fallback-v1",
        })

    entity_type = create_entity(agent, entity_id="checkpoint-custom-agent")
    entity, _ = _direct_entity(
        entity_type,
        state=checkpoint,
        operation="run",
        operation_id="native-stream-failure",
    )
    entity.run({"message": "effectful prompt", "correlationId": "stream-failure"})

    assert agent.effects == 1
    assert [stream for stream, _ in agent.calls] == [True]


def test_checkpoint_wrapper_preserves_immediate_no_stream_fallback() -> None:
    no_stream_agent = _NoStreamSignatureAgent()
    no_stream_entity, _ = _direct_entity(
        create_entity(cast(SupportsAgentRun, no_stream_agent), entity_id="no-stream-signature"),
        state=empty_checkpoint(_ENTITY_ADDRESS),
        operation="run",
        operation_id="native-no-stream-signature",
    )
    no_stream_result = no_stream_entity.run({"message": "prompt", "correlationId": "no-stream-signature"})
    assert no_stream_result["messages"][0]["contents"][0]["text"] == "no-stream fallback"
    assert no_stream_agent.calls == 1

    callable_agent = _CallableRunAgent()
    callable_entity, _ = _direct_entity(
        create_entity(cast(SupportsAgentRun, callable_agent), entity_id="callable-run"),
        state=empty_checkpoint(_ENTITY_ADDRESS),
        operation="run",
        operation_id="native-callable-run",
    )
    callable_result = callable_entity.run({"message": "prompt", "correlationId": "callable-run"})
    assert callable_result["messages"][0]["contents"][0]["text"] == "no-stream fallback"
    assert callable_agent.calls == [True, False]


def test_reasoning_stays_observable_but_is_not_replayed() -> None:
    agent = _ReasoningAgent()
    entity_type = create_entity(agent, entity_id="checkpoint-custom-agent")
    first, first_context = _direct_entity(
        entity_type,
        state=None,
        operation="run",
        operation_id="native-reasoning-first",
    )

    first.run({"message": "first prompt", "correlationId": "reasoning-first"})
    after_first = _entity_state(first_context)

    replay_text = [
        content["text"]
        for message in after_first["publishedReplayMessages"]
        for content in message["contents"]
        if content["type"] == "text"
    ]
    observed_types = [
        content["type"]
        for message in after_first["observations"][0]["response"]["messages"]
        for content in message["contents"]
    ]
    assert replay_text == ["first prompt", "visible answer"]
    assert observed_types == ["text_reasoning", "text"]

    second, _ = _direct_entity(
        entity_type,
        state=after_first,
        operation="run",
        operation_id="native-reasoning-second",
    )
    second.run({"message": "second prompt", "correlationId": "reasoning-second"})
    assert _texts(agent.calls[-1][1]) == [["first prompt"], ["visible answer"], ["second prompt"]]


def test_reset_clears_public_state_but_preserves_native_completion_floor() -> None:
    agent = _SlottedCustomAgent()
    entity_type = create_entity(agent, entity_id="checkpoint-custom-agent")
    first, first_context = _direct_entity(
        entity_type,
        state=None,
        operation="run",
        operation_id="native-reset",
    )
    request = {"message": "first prompt", "correlationId": "same-public-correlation"}
    first.run(request)
    before_reset = _entity_state(first_context)

    reset, reset_context = _direct_entity(entity_type, state=before_reset, operation="reset")
    reset.reset()
    after_reset = _entity_state(reset_context)

    assert after_reset["publishedReplayMessages"] == []
    assert after_reset["observations"] == []
    assert set(after_reset["nativeOperations"]) == {"native-reset"}
    receipt = after_reset["runtime"]["data"]["completionReceipts"]["same-public-correlation"]
    assert receipt["resultState"] == "available"

    redelivery, redelivery_context = _direct_entity(
        entity_type,
        state=after_reset,
        operation="run",
        operation_id="native-reset",
    )
    response = redelivery.run(request)
    after_redelivery = _entity_state(redelivery_context)

    assert response["messages"][0]["contents"][0]["text"] == "first answer"
    assert len(agent.calls) == 1
    assert after_redelivery["publishedReplayMessages"] == []
    assert after_redelivery["observations"] == []


def test_reset_redelivery_preserves_published_error_view() -> None:
    agent = _EffectBeforeStreamFailureAgent()
    entity_type = create_entity(agent, entity_id="checkpoint-custom-agent")
    first, first_context = _direct_entity(
        entity_type,
        state=None,
        operation="run",
        operation_id="native-failed-reset",
    )
    request = {"message": "effectful prompt", "correlationId": "failed-reset"}
    first_response = first.run(request)
    before_reset = _entity_state(first_context)

    reset, reset_context = _direct_entity(entity_type, state=before_reset, operation="reset")
    reset.reset()
    after_reset = _entity_state(reset_context)

    redelivery, _ = _direct_entity(
        entity_type,
        state=after_reset,
        operation="run",
        operation_id="native-failed-reset",
    )
    redelivered_response = redelivery.run(request)

    assert redelivered_response == first_response
    assert first_response["messages"][0]["contents"] == [
        {
            "type": "error",
            "message": "stream failed after the effect",
            "error_code": "RuntimeError",
            "additional_properties": {},
        }
    ]
    assert agent.effects == 1


@pytest.mark.parametrize("mark_unavailable", [False, True], ids=["deadline-passed", "receipt-unavailable"])
def test_native_redelivery_after_expiry_uses_runtime_receipt_not_cached_response(
    mark_unavailable: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent = _SlottedCustomAgent()
    entity_type = create_entity(agent, entity_id="checkpoint-custom-agent")
    first, first_context = _direct_entity(
        entity_type,
        state=None,
        operation="run",
        operation_id="native-expired",
    )
    request = {"message": "completed prompt", "correlationId": "expired-correlation"}
    first_response = first.run(request)
    expired_checkpoint = _entity_state(first_context)
    runtime_state = DurableAgentState.from_dict(expired_checkpoint["runtime"])
    receipt = runtime_state.data.completed_correlations["expired-correlation"]
    if mark_unavailable:
        runtime_state.expire_responses(now=datetime.now(timezone.utc) + timedelta(days=3))
    else:
        result_expiry = datetime.fromisoformat(receipt["resultExpiresAt"])
        future = result_expiry + timedelta(seconds=1)

        class _FutureDateTime(datetime):
            @classmethod
            def now(cls, tz: Any = None) -> _FutureDateTime:
                target = future.astimezone(tz) if tz is not None else future.replace(tzinfo=None)
                return cls(
                    target.year,
                    target.month,
                    target.day,
                    target.hour,
                    target.minute,
                    target.second,
                    target.microsecond,
                    tzinfo=target.tzinfo,
                )

        monkeypatch.setattr(_delivery_state, "datetime", _FutureDateTime)
    receipt = runtime_state.data.completed_correlations["expired-correlation"]
    expired_checkpoint["runtime"] = runtime_state.to_dict()
    assert receipt["resultState"] == ("unavailable" if mark_unavailable else "available")

    redelivery, _ = _direct_entity(
        entity_type,
        state=expired_checkpoint,
        operation="run",
        operation_id="native-expired",
    )
    response = redelivery.run(request)

    assert response != first_response
    assert response["additional_properties"]["durable_status"] == "already_completed"
    assert response["messages"][0]["contents"][0]["error_code"] == "response_expired"
    assert len(agent.calls) == 1
