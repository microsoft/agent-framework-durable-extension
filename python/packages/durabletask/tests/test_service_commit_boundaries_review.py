# Copyright (c) Microsoft. All rights reserved.

"""Real Core service completion, session progress, and canonical delivery boundaries."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Mapping, Sequence
from copy import deepcopy
from traceback import extract_stack
from typing import Any, Literal, cast

import pytest
from _execution_test_support import JsonStateProvider, NonStreamingAgent, RecordingChatClient, ToolChatClient
from agent_framework import (
    AgentResponse,
    AgentSession,
    ChatMiddlewareLayer,
    ChatResponse,
    ContextProvider,
    FunctionInvocationLayer,
    HistoryProvider,
    Message,
    SessionContext,
    SlidingWindowStrategy,
    tool,
)
from agent_framework._sessions import MessageInjectionMiddleware as CoreMessageInjectionMiddleware
from durabletask.task import CompletableTask
from pydantic import BaseModel

from agent_framework_durabletask import AgentEntity, DurableAgentState, DurableHistoryProvider, RunRequest
from agent_framework_durabletask import _entities as durable_entities
from agent_framework_durabletask._executors import DurableAgentTask
from agent_framework_durabletask._history_provider import current_durable_history_binding
from agent_framework_durabletask._response_utils import ensure_response_format, serialize_agent_response


class Answer(BaseModel):
    answer: int


class _MissingParent(RuntimeError):
    code = "previous_response_not_found"


class _SessionProbe(ContextProvider):
    def __init__(self) -> None:
        super().__init__("probe")
        self.session: AgentSession | None = None
        self.entries: list[dict[str, Any]] = []

    async def before_run(self, *, session: AgentSession, context: SessionContext, **kwargs: Any) -> None:
        self.session = session
        self.entries.append({
            "session": deepcopy(session.to_dict()),
            "inputs": deepcopy([message.to_dict() for message in context.input_messages]),
        })


class _Audit(HistoryProvider):
    def __init__(self, events: list[str], failure: Literal["audit", "missing"] | None) -> None:
        super().__init__("audit", load_messages=False)
        self.events = events
        self.failure = failure
        self.hooks: list[dict[str, Any]] = []
        self.saves: list[list[Message]] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        raise AssertionError("A store-only audit must not load model history")

    async def after_run(self, *, session: AgentSession, context: SessionContext, **kwargs: Any) -> None:
        binding = current_durable_history_binding()
        self.hooks.append({
            "per_service_call": any(frame.name == "_persist_service_call_response" for frame in extract_stack()),
            "session": deepcopy(session.to_dict()),
            "inputs": deepcopy([message.to_dict() for message in context.input_messages]),
            "accepted": set(binding.accepted_inputs) if binding is not None else None,
        })
        await super().after_run(session=session, context=context, **kwargs)

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.saves.append(deepcopy(list(messages)))
        self.events.append("audit")
        if self.failure == "audit":
            raise OSError("audit sink unavailable after leaf completion")
        if self.failure == "missing":
            raise _MissingParent("missing parent reported after leaf completion")


class _ServiceClient(FunctionInvocationLayer, ChatMiddlewareLayer, RecordingChatClient):
    """Reuse the offline leaf, recording observations outside clone-local scalar state."""

    def __init__(
        self,
        probe: _SessionProbe,
        events: list[str],
        *,
        compact: bool = False,
        rejections: int = 0,
        service_id: str = "S1",
    ) -> None:
        super().__init__(conversation_id=service_id)
        self.probe = probe
        self.events = events
        self.rejections = rejections
        self.compaction_strategy = SlidingWindowStrategy(keep_last_groups=1) if compact else None
        self.calls: list[dict[str, Any]] = []
        self.instances: list[_ServiceClient] = []
        self.local_calls = 0

    def _inner_get_response(
        self, *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
    ) -> Awaitable[ChatResponse]:
        assert not stream
        assert self.probe.session is not None
        self.local_calls += 1
        self.instances.append(self)
        call: dict[str, Any] = {
            "ids": [message.message_id for message in messages],
            "session": deepcopy(self.probe.session.to_dict()),
            "completed": False,
        }
        self.calls.append(call)
        self.events.append("leaf")
        if len(self.calls) <= self.rejections:
            self.received_messages.append(deepcopy(list(messages)))
            self.received_options.append(deepcopy(dict(options)))
            raise _MissingParent("provider refused the supplied parent")
        result = cast(
            Awaitable[ChatResponse],
            super()._inner_get_response(messages=messages, stream=stream, options=options, **kwargs),
        )

        async def complete() -> ChatResponse:
            response = await result
            call["completed"] = True
            call["returned_id"] = response.conversation_id
            self.events.append("completed")
            return response

        return complete()


def _provider(*, service_id: Any = "S0", injection: CoreMessageInjectionMiddleware | None = None) -> JsonStateProvider:
    session = AgentSession(session_id="@review@thread", service_session_id=deepcopy(service_id))
    session.state = {"untouched": {"marker": [False, 0]}, "probe": {}, "audit": {}, "history": {}}
    if injection is not None:
        injection.enqueue_messages(session, [Message("user", ["queued input"], message_id="Q")])
    state = DurableAgentState()
    state.data.session = session.to_dict()
    return JsonStateProvider(state.to_dict(), session_id="thread", entity_name="review")


def _build(
    provider: JsonStateProvider,
    *,
    failure: Literal["audit", "missing"] | None = None,
    compact: bool = False,
    rejections: int = 0,
    injection: CoreMessageInjectionMiddleware | None = None,
    service_id: str = "S1",
) -> tuple[AgentEntity, _ServiceClient, _SessionProbe, _Audit]:
    events: list[str] = []
    probe = _SessionProbe()
    audit = _Audit(events, failure)
    client = _ServiceClient(probe, events, compact=compact, rejections=rejections, service_id=service_id)
    agent = NonStreamingAgent(
        client=client,
        name="review",
        default_options={"store": True},
        context_providers=[probe, audit, DurableHistoryProvider("history")],
        middleware=[injection] if injection is not None else [],
        require_per_service_call_history_persistence=True,
    )
    return AgentEntity(agent, state_provider=provider), client, probe, audit


def _request(correlation: str, *ids: str) -> dict[str, Any]:
    return {
        "message": "contextMessages are authoritative",
        "correlationId": correlation,
        "enable_tool_calls": False,
        "options": {"store": True},
        "contextMessages": [Message("user", [key], message_id=key).to_dict() for key in ids],
        "contextMessageIds": [f"occ-{key}" for key in ids],
    }


def _committed(provider: JsonStateProvider) -> DurableAgentState:
    return DurableAgentState.from_dict(deepcopy(provider.raw))


def _session(provider: JsonStateProvider) -> AgentSession:
    payload = _committed(provider).data.session
    assert isinstance(payload, dict)
    return AgentSession.from_dict(deepcopy(payload))


def _assert_failed(provider: JsonStateProvider, response: AgentResponse, error_code: str, detail: str) -> None:
    assert response.additional_properties["durable_status"] == "error"
    assert detail in response.text
    codes = [
        content.error_code for message in response.messages for content in message.contents if content.type == "error"
    ]
    assert codes == [error_code]
    assert provider.successful_writes == 1
    data = provider.raw["data"]
    assert data["completionReceipts"]["first"]["outcome"] == "failed"
    assert data["terminalResults"]["first"]["outcome"] == "failed"


@pytest.fixture(autouse=True)
def _zero_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    # A regressed retry must finish quickly without replacing the real retry loop.
    monkeypatch.setattr(durable_entities, "_REJECTED_ID_BACKOFF_SECONDS", 0)


@pytest.mark.parametrize("compact", [False, True], ids=["no-compaction", "compacted"])
@pytest.mark.parametrize("fail_audit", [False, True], ids=["audit-success", "audit-failure"])
async def test_service_receipts_and_cold_continuation_follow_completed_leaf_only(
    compact: bool, fail_audit: bool
) -> None:
    provider = _provider()
    entity, client, probe, audit = _build(provider, compact=compact, failure="audit" if fail_audit else None)
    original_client = dict(vars(client))
    configuration = deepcopy(client.function_invocation_configuration)
    middleware = tuple(client.chat_middleware)
    request = _request("first", "A", "B")
    original_request = deepcopy(request)

    response = await entity.run(request)

    expected_ids = ["B"] if compact else ["A", "B"]
    expected_occurrences = {"occ-B"} if compact else {"occ-A", "occ-B"}
    assert len(client.calls) == len(probe.entries) == len(audit.hooks) == len(audit.saves) == 1
    assert client.calls[0]["ids"] == expected_ids
    assert [message.text for message in client.received_messages[0]] == expected_ids
    assert client.calls[0]["completed"] is True
    assert client.calls[0]["returned_id"] == "S1"
    assert client.received_options[0]["conversation_id"] == "S0"
    assert client.received_options[0]["store"] is True
    assert client.calls[0]["session"]["service_session_id"] == "S0"
    assert client.events == ["leaf", "completed", "audit"]
    assert audit.hooks[0]["per_service_call"] is True
    # Inspect the actual session inside Core's audit hook, before Core's outer
    # agent response handler can publish the new service conversation ID.
    assert audit.hooks[0]["session"]["service_session_id"] == "S1"
    assert {occurrence for occurrence, _ in audit.hooks[0]["accepted"]} == expected_occurrences
    assert [message["message_id"] for message in probe.entries[0]["inputs"]] == ["A", "B"]

    committed = _committed(provider)
    receipts = committed.data.ingested_messages
    assert set(receipts) == expected_occurrences
    assert all(
        isinstance(values, list) and len(values) == 1 and isinstance(values[0], str) for values in receipts.values()
    )
    assert committed.data.conversation_history == []
    assert _session(provider).service_session_id == "S1"
    assert _session(provider).state["untouched"] == {"marker": [False, 0]}
    if fail_audit:
        _assert_failed(provider, response, "OSError", "audit sink unavailable after leaf completion")
    else:
        assert response.additional_properties.get("durable_status") != "error"
        assert response.text == "reply-1"
        assert provider.successful_writes == 1
        assert committed.data.completed_correlations["first"]["outcome"] == "succeeded"

    # The provider runs on the dynamic clone, not on the shared client. Mutable
    # observation lists are intentional, configuration and scalar counters are not.
    assert vars(client) == original_client
    assert client.local_calls == 0
    assert client.instances[0] is not client
    assert client.instances[0].local_calls == 1
    assert client.function_invocation_configuration == configuration
    assert tuple(client.chat_middleware) == middleware
    assert client._cached_chat_middleware_pipeline is None
    assert "_inner_get_response" not in vars(client)
    assert request == original_request

    first_commit = deepcopy(provider.raw)
    for resend_a in (False, True):
        # Each branch starts from the SAME committed JSON. No warmed session,
        # provider, agent, client, or compaction strategy crosses the boundary.
        cold = JsonStateProvider(deepcopy(first_commit), session_id="thread", entity_name="review")
        next_entity, next_client, next_probe, _ = _build(cold, service_id="S2")
        assert next_client.compaction_strategy is None
        next_ids = ("A", "B", "C") if resend_a else ("B", "C")
        next_request = _request("next", *next_ids)
        next_response = await next_entity.run(next_request)

        expected_next = ["A", "C"] if compact and resend_a else ["C"]
        assert next_response.additional_properties.get("durable_status") != "error"
        assert len(next_client.calls) == len(next_probe.entries) == 1
        assert next_probe.entries[0]["session"]["service_session_id"] == "S1"
        assert next_client.received_options[0]["conversation_id"] == "S1"
        assert [message.message_id for message in next_client.received_messages[0]] == expected_next
        assert [message.text for message in next_client.received_messages[0]] == expected_next
        assert set(_committed(cold).data.ingested_messages) == expected_occurrences | {
            f"occ-{key}" for key in expected_next
        }
        assert _session(cold).service_session_id == "S2"
        assert cold.successful_writes == 1
    assert provider.raw == first_commit


async def test_real_message_injection_queue_progress_blocks_whole_run_retry() -> None:
    injection = CoreMessageInjectionMiddleware()
    provider = _provider(injection=injection)
    assert [message.message_id for message in injection.get_pending_messages(_session(provider))] == ["Q"]
    entity, client, probe, audit = _build(provider, injection=injection, rejections=1)

    response = await entity.run(_request("first", "B"))

    _assert_failed(provider, response, "_MissingParent", "provider refused the supplied parent")
    assert len(client.calls) == len(probe.entries) == 1
    assert [message.message_id for message in client.received_messages[0]] == ["B", "Q"]
    assert [message.text for message in client.received_messages[0]] == ["B", "queued input"]
    assert client.received_options[0]["conversation_id"] == "S0"
    assert client.calls[0]["completed"] is False
    assert client.events == ["leaf"]
    assert audit.hooks == audit.saves == []
    # Core actually drained the persisted queue. There must be no second variant
    # of the same turn that silently succeeds with only B after losing Q.
    at_leaf = AgentSession.from_dict(deepcopy(client.calls[0]["session"]))
    assert injection.get_pending_messages(at_leaf) == []
    assert _committed(provider).data.ingested_messages == {}
    assert _session(provider).service_session_id == "S0"


async def test_read_only_session_control_retries_identical_parent_and_inputs_only_three_times() -> None:
    provider = _provider()
    original_session = _session(provider).to_dict()
    # A fifth attempt would succeed, so both an early stop and an excess retry
    # violate the independently specified initial call plus three-retry budget.
    entity, client, probe, audit = _build(provider, rejections=4, service_id="S0")

    response = await entity.run(_request("first", "B"))

    _assert_failed(provider, response, "_MissingParent", "provider refused the supplied parent")
    assert len(client.calls) == len(probe.entries) == 4
    assert [entry["session"] for entry in probe.entries] == [original_session] * 4
    assert [call["session"] for call in client.calls] == [original_session] * 4
    assert [call["completed"] for call in client.calls] == [False] * 4
    assert [options["conversation_id"] for options in client.received_options] == ["S0"] * 4
    expected = [Message("user", ["B"], message_id="B").to_dict()]
    assert [[message.to_dict() for message in batch] for batch in client.received_messages] == [expected] * 4
    assert client.events == ["leaf"] * 4
    assert audit.hooks == audit.saves == []
    assert _committed(provider).data.ingested_messages == {}
    assert _session(provider).service_session_id == "S0"


class _MutatingAgent:
    """A custom session-capable agent, since Core's generic chat path rejects mapping IDs."""

    name = "custom"

    def __init__(self, mutation: Literal["continuation", "input"]) -> None:
        self.context_providers: list[Any] = []
        self.default_options = {"store": True}
        self.mutation = mutation
        self.calls: list[dict[str, Any]] = []

    def create_session(self, session_id: str) -> AgentSession:
        return AgentSession(session_id=session_id)

    async def run(self, *, session: AgentSession, messages: list[Message], options: dict[str, Any]) -> AgentResponse:
        self.calls.append({
            "session": deepcopy(session.to_dict()),
            "inputs": deepcopy([message.to_dict() for message in messages]),
        })
        if self.mutation == "continuation":
            cursor = cast(dict[str, Any], session.service_session_id)
            cursor["cursor"] += 1
        else:
            messages[0].contents[0].text = "changed in place"
        raise _MissingParent("rejected after in-place progress")


@pytest.mark.parametrize("mutation", ["continuation", "input"])
async def test_in_place_continuation_or_input_progress_blocks_retry(mutation: Literal["continuation", "input"]) -> None:
    provider = _provider(service_id={"cursor": 0} if mutation == "continuation" else "S0")
    agent = _MutatingAgent(mutation)
    entity = AgentEntity(cast(Any, agent), state_provider=provider)
    request = _request("first", "B")
    before = deepcopy(request)

    response = await entity.run(request)

    _assert_failed(provider, response, "_MissingParent", "rejected after in-place progress")
    assert len(agent.calls) == 1
    assert agent.calls[0]["inputs"][0]["contents"][0]["text"] == "B"
    assert agent.calls[0]["session"]["service_session_id"] == ({"cursor": 0} if mutation == "continuation" else "S0")
    assert _session(provider).service_session_id == ({"cursor": 1} if mutation == "continuation" else "S0")
    assert _session(provider).state["untouched"] == {"marker": [False, 0]}
    assert _committed(provider).data.ingested_messages == {}
    assert request == before


async def test_completed_leaf_with_unchanged_parent_id_blocks_later_missing_parent_retry() -> None:
    provider = _provider()
    initial_session = _session(provider).to_dict()
    entity, client, probe, audit = _build(provider, failure="missing", service_id="S0")

    response = await entity.run(_request("first", "B"))

    _assert_failed(provider, response, "_MissingParent", "missing parent reported after leaf completion")
    assert len(client.calls) == len(probe.entries) == len(audit.hooks) == 1
    assert client.calls[0]["completed"] is True
    assert client.calls[0]["returned_id"] == client.received_options[0]["conversation_id"] == "S0"
    assert client.events == ["leaf", "completed", "audit"]
    assert audit.hooks[0]["per_service_call"] is True
    # No stream, tool, ID, session, or input advancement explains this refusal
    # to retry. Completing a service call is independently sufficient progress.
    assert audit.hooks[0]["session"] == probe.entries[0]["session"] == initial_session
    assert audit.hooks[0]["inputs"] == probe.entries[0]["inputs"]
    assert set(_committed(provider).data.ingested_messages) == {"occ-B"}
    assert _session(provider).service_session_id == "S0"


async def test_first_typed_approval_from_entity_is_canonical_and_delivers_through_task_without_tool_execution() -> None:
    effects: list[str] = []

    @tool(name="lookup", approval_mode="always_require")
    def lookup(key: str) -> str:
        effects.append(key)
        return f"value:{key}"

    client = ToolChatClient()
    provider = JsonStateProvider()
    agent = NonStreamingAgent(client=client, name="approval", tools=[lookup], default_options={"store": True})
    entity = AgentEntity(agent, state_provider=provider)
    request = RunRequest(
        message="Use lookup", correlation_id="approval", response_format=Answer, options={"store": True}
    )

    first = await entity.run(request)

    assert effects == []
    assert len(client.received_messages) == 1
    assert client.received_options[0]["response_format"] is Answer
    assert first.additional_properties.get("durable_status") != "error"
    assert len(first.user_input_requests) == 1
    approval = first.user_input_requests[0]
    assert approval.type == "function_approval_request"
    assert approval.id == "call-1" and approval.approved is None
    assert approval.function_call is not None
    assert approval.function_call.call_id == "call-1"
    assert approval.function_call.name == "lookup"
    arguments = approval.function_call.arguments
    assert (json.loads(arguments) if isinstance(arguments, str) else arguments) == {"key": "durable"}
    wire = json.loads(json.dumps(serialize_agent_response(first), allow_nan=False))
    assert wire["_durable_approval_policy"] == {"profile": "agent-framework-python.shared-approval", "version": 1}
    assert "value" not in wire
    ensure_response_format(Answer, "approval", first)
    assert first.value is None
    assert provider.successful_writes == 1
    assert provider.raw["data"]["completionReceipts"]["approval"]["outcome"] == "succeeded"
    assert "value" not in provider.raw["data"]["terminalResults"]["approval"]["response"]

    child: CompletableTask[Any] = CompletableTask()
    task = DurableAgentTask(child, Answer, "approval")
    assert not task.is_complete
    child.complete(wire)
    assert task.is_complete and not task.is_failed
    delivered = task.get_result()
    assert delivered.value is None
    assert len(delivered.user_input_requests) == 1
    assert delivered.user_input_requests[0].to_dict() == approval.to_dict()

    cold_provider = JsonStateProvider(deepcopy(provider.raw))
    cold_client = ToolChatClient()
    cold_entity = AgentEntity(
        NonStreamingAgent(client=cold_client, name="approval", tools=[lookup], default_options={"store": True}),
        state_provider=cold_provider,
    )
    duplicate = await cold_entity.run(request)
    assert serialize_agent_response(duplicate) == wire
    assert cold_client.received_messages == []
    assert cold_provider.successful_writes == 0
    assert effects == []
