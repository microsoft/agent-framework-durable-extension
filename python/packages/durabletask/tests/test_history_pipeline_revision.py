# Copyright (c) Microsoft. All rights reserved.

"""Core-pipeline cadence, detached transcript appends and compaction reconciliation."""

import json
from collections.abc import AsyncIterable, Awaitable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

import pytest
from agent_framework import (
    Agent,
    AgentResponse,
    AgentSession,
    BaseChatClient,
    ChatMiddlewareLayer,
    ChatResponse,
    ChatResponseUpdate,
    CompactionProvider,
    Content,
    ContextProvider,
    FunctionInvocationLayer,
    HistoryProvider,
    InMemoryHistoryProvider,
    Message,
    ResponseStream,
    SessionContext,
    SummarizationStrategy,
    annotate_message_groups,
    tool,
)
from test_durable_history_provider import RecordingChatClient, _InMemoryStateProvider

from agent_framework_durabletask import AgentEntity, DurableAgentState, DurableHistoryProvider
from agent_framework_durabletask._durable_agent_state import (
    DurableAgentStateCompaction,
    DurableAgentStateEntry,
    DurableAgentStateEntryJsonType,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
    DurableAgentStateUnknownEntry,
    DurableAgentStateUsage,
)
from agent_framework_durabletask._history_provider import (
    POSITIONS_KEY,
    WORKING_BUFFER_KEY,
    DurableHistoryBinding,
    bind_durable_history,
    current_durable_history_binding,
    ensure_durable_history,
    prune_messages,
    unbind_durable_history,
)
from agent_framework_durabletask._message_identity import message_identity

OLD = datetime(2026, 1, 1, tzinfo=timezone.utc)
PROMPT = "Use lookup for durable."


class ToolChatClient(FunctionInvocationLayer, ChatMiddlewareLayer, BaseChatClient):
    """Exercise real core middleware and function invocation, not just the client protocol."""

    def __init__(
        self,
        *,
        tool_calls: bool = True,
        response_message_id: str | None = None,
        fail: bool = False,
        fail_on_call: int | None = None,
        events: list[str] | None = None,
    ) -> None:
        super().__init__(middleware=[])
        self.tool_calls = tool_calls
        self.response_message_id = response_message_id
        self.fail = fail
        self.fail_on_call = fail_on_call
        self.events = events if events is not None else []
        self.received_messages: list[list[Message]] = []
        self.received_options: list[dict[str, Any]] = []

    def _inner_get_response(
        self,
        *,
        messages: Sequence[Message],
        stream: bool,
        options: Mapping[str, Any],
        **kwargs: Any,
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        self.received_messages.append(deepcopy(list(messages)))
        self.received_options.append(dict(options))
        call = len(self.received_messages)
        self.events.append(f"model-{call}")
        if self.fail or call == self.fail_on_call:
            raise RuntimeError("model failed before history persistence")
        calls_tool = self.tool_calls and call == 1
        contents = (
            [Content.from_function_call(call_id="call-1", name="lookup", arguments='{"key":"durable"}')]
            if calls_tool
            else [Content.from_text(f"answer-{call}")]
        )
        response = ChatResponse(
            messages=[
                Message(
                    "assistant",
                    contents,
                    message_id=self.response_message_id,
                    additional_properties={"model_metadata": {"tags": ["original"]}},
                )
            ],
            response_id=f"response-{call}",
            conversation_id="service-thread" if options.get("store") else None,
            finish_reason="tool_calls" if calls_tool else "stop",
        )
        if stream:

            async def updates() -> AsyncIterable[ChatResponseUpdate]:
                for message in response.messages:
                    yield ChatResponseUpdate(
                        role="assistant",
                        contents=message.contents,
                        message_id=message.message_id,
                        additional_properties=deepcopy(message.additional_properties),
                        response_id=response.response_id,
                        conversation_id=response.conversation_id,
                        finish_reason=response.finish_reason,
                    )

            return ResponseStream(updates(), finalizer=ChatResponse.from_updates)

        async def get() -> ChatResponse:
            return response

        return get()


class NonStreamingAgent(Agent):
    """Negotiate non-streaming before any core model or tool execution."""

    def run(self, *args: Any, **kwargs: Any) -> Any:
        if kwargs.get("stream"):
            raise TypeError("stream is not supported")
        return super().run(*args, **kwargs)


class CountingHistory(DurableHistoryProvider):
    def __init__(self, events: list[str]) -> None:
        super().__init__(prune_excluded=False)
        self.events = events
        self.before_calls = 0
        self.after_calls = 0

    async def before_run(self, **kwargs: Any) -> None:
        self.before_calls += 1
        self.events.append("history-before")
        await super().before_run(**kwargs)

    async def after_run(self, **kwargs: Any) -> None:
        self.after_calls += 1
        self.events.append("history-after")
        await super().after_run(**kwargs)


class CaptureHistory(HistoryProvider):
    def __init__(self, *, load_messages: bool = False) -> None:
        super().__init__("audit", load_messages=load_messages)
        self.saved: list[list[Message]] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        return [deepcopy(message) for batch in self.saved for message in batch]

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.saved.append(deepcopy(list(messages)))


class AddContext(ContextProvider):
    async def before_run(self, *, context: SessionContext, **kwargs: Any) -> None:
        context.extend_messages(self, [Message("user", [f"context-{self.source_id}"])])


class SummarizeSeed:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.calls = 0

    async def __call__(self, messages: list[Message]) -> bool:
        self.calls += 1
        self.events.append("compaction")
        seed = next(message for message in messages if message.message_id == "seed-user")
        seed.additional_properties.update({"_excluded": True, "after_hook": {"tags": ["kept"]}})
        if any(message.message_id == "seed-summary" for message in messages):
            return False
        messages.insert(
            messages.index(seed) + 1,
            Message(
                "assistant",
                ["seed summary"],
                message_id="seed-summary",
                additional_properties={"_summary_of_message_ids": ["seed-user"]},
            ),
        )
        return True


@contextmanager
def bound(
    provider: _InMemoryStateProvider,
    correlation_id: str | None = "current",
    *,
    service_owns_history: bool = False,
) -> Iterator[DurableHistoryBinding]:
    binding = DurableHistoryBinding(provider, correlation_id, service_owns_history)
    token = bind_durable_history(binding)
    try:
        yield binding
    finally:
        unbind_durable_history(token)


def stored(message_id: str | None, text: str, role: str = "user") -> DurableAgentStateMessage:
    return DurableAgentStateMessage.from_chat_message(Message(role, [text], message_id=message_id))


def seed(provider: _InMemoryStateProvider) -> None:
    provider.state.data.conversation_history.extend([
        DurableAgentStateRequest("seed", OLD, [stored("seed-user", "seed question")]),
        DurableAgentStateResponse("seed", OLD, [stored("seed-assistant", "seed answer", "assistant")]),
    ])


def transcript(provider: _InMemoryStateProvider) -> list[DurableAgentStateMessage]:
    return [message for entry in provider.state.data.conversation_history for message in entry.messages]


def ids(provider: _InMemoryStateProvider) -> list[str | None]:
    return [message.message_id for message in transcript(provider)]


def assert_current_positions(provider: _InMemoryStateProvider, state: dict[str, Any]) -> None:
    history = provider.state.data.conversation_history
    positions = state[POSITIONS_KEY]
    for message in state[WORKING_BUFFER_KEY]:
        entry, index = positions[message.message_id]
        assert any(candidate is entry for candidate in history)
        assert entry.messages[index].message_id == message.message_id
    assert len(positions) == len({message.message_id for message in transcript(provider)})


def assert_tool_follow_up(client: ToolChatClient) -> None:
    assert len(client.received_messages) == 2
    second = client.received_messages[1]
    assert [message.text for message in second].count(PROMPT) == 1
    calls = [content for message in second for content in message.contents if content.type == "function_call"]
    results = [content for message in second for content in message.contents if content.type == "function_result"]
    assert len(calls) == len(results) == 1
    assert calls[0].call_id == results[0].call_id == "call-1"
    assert results[0].result == "value:durable"
    assert not client.received_options[1].get("conversation_id"), "the core sentinel must not reach the model"


@tool(name="lookup", approval_mode="never_require")
def lookup(key: str) -> str:
    return f"value:{key}"


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
@pytest.mark.parametrize("stream", [False, True], ids=["non-streaming", "streaming"])
async def test_real_core_tool_loop_and_final_flush(per_call: bool, stream: bool) -> None:
    events: list[str] = []
    history = CountingHistory(events)
    strategy = SummarizeSeed(events)
    provider = _InMemoryStateProvider()
    seed(provider)
    client = ToolChatClient(events=events)
    agent = Agent(
        client=client,
        name="tool-agent",
        tools=[lookup],
        context_providers=[history, CompactionProvider(after_strategy=strategy, history_source_id=history.source_id)],
        require_per_service_call_history_persistence=per_call,
    )
    session = agent.create_session(session_id="pipeline-session")
    with bound(provider) as binding:
        if stream:
            response = await agent.run(PROMPT, session=session, stream=True).get_final_response()
        else:
            response = await agent.run(PROMPT, session=session)

        assert_tool_follow_up(client)
        assert response.text == "answer-2"
        assert history.before_calls == history.after_calls == (2 if per_call else 1)
        assert binding.pending_inputs == []
        assert strategy.calls == 1
        assert events[-1] == ("compaction" if per_call else "history-after")
        if per_call:
            assert "seed-summary" not in ids(provider), "run-end compaction still needs the entity's final flush"
        original_response = deepcopy(response.to_dict())
        state = session.state[history.source_id]
        history.flush(state)
        assert current_durable_history_binding() is binding
        assert [message.text for message in transcript(provider)] == [
            "seed question",
            "seed summary",
            "seed answer",
            PROMPT,
            "",
            "",
            "answer-2",
        ]
        assert all(ids(provider)) and len(ids(provider)) == len(set(ids(provider)))
        assert (transcript(provider)[0].extension_data or {})["after_hook"] == {"tags": ["kept"]}
        current = [entry for entry in provider.state.data.conversation_history if entry.correlation_id == "current"]
        assert len(current) == (4 if per_call else 2)
        assert [entry.json_type for entry in current] == (
            [DurableAgentStateEntryJsonType.REQUEST, DurableAgentStateEntryJsonType.RESPONSE] * (2 if per_call else 1)
        )
        assert_current_positions(provider, state)
        snapshot = deepcopy(provider.state.to_dict())
        ordinal = binding.append_ordinal
        history.finalize_failed_run(state)
        history.flush(state)
        history.flush(state)
        assert provider.state.to_dict() == snapshot
        assert binding.append_ordinal == ordinal
        assert strategy.calls == 1 and len(client.received_messages) == 2
        assert response.to_dict() == original_response
        assert provider.writes == 0


@pytest.mark.parametrize("per_call", [False, True])
async def test_entity_flushes_after_all_core_providers_and_cold_reload(per_call: bool) -> None:
    events: list[str] = []
    history = CountingHistory(events)
    strategy = SummarizeSeed(events)
    bindings: list[DurableHistoryBinding] = []

    class LastAfterProvider(ContextProvider):
        async def after_run(self, *, session: AgentSession, **kwargs: Any) -> None:
            binding = current_durable_history_binding()
            assert binding is not None
            bindings.append(binding)
            session.state[history.source_id][WORKING_BUFFER_KEY][-1].additional_properties["last_after"] = True

    provider = _InMemoryStateProvider()
    seed(provider)
    client = ToolChatClient(events=events)
    agent = Agent(
        client=client,
        tools=[lookup],
        context_providers=[
            LastAfterProvider("last-after"),
            history,
            CompactionProvider(after_strategy=strategy, history_source_id=history.source_id),
        ],
        require_per_service_call_history_persistence=per_call,
    )
    entity = AgentEntity(agent, state_provider=provider)
    response = await entity.run({"message": PROMPT, "correlationId": "tool-turn"})

    assert_tool_follow_up(client)
    assert history.after_calls == (2 if per_call else 1) and strategy.calls == 1
    assert len(bindings) == 1 and bindings[0].correlation_id == "tool-turn"
    assert provider.writes == 1
    assert len(transcript(provider)) == 7
    assert ids(provider).count("seed-summary") == 1
    assert (transcript(provider)[-1].extension_data or {})["last_after"] is True
    assert not response.messages[-1].additional_properties.get("last_after")
    assert history.source_id not in provider._get_state_dict()["data"]["session"]["state"]
    original = deepcopy(response.to_dict())
    mailbox = deepcopy(provider.state.data.response_mailbox)
    receipts = deepcopy(provider.state.data.completed_correlations)

    cold_provider = _InMemoryStateProvider(raw=provider._get_state_dict())
    cold_client = ToolChatClient(tool_calls=False)
    cold_history = DurableHistoryProvider(prune_excluded=False)
    cold = AgentEntity(
        Agent(
            client=cold_client,
            context_providers=[cold_history],
            require_per_service_call_history_persistence=per_call,
        ),
        state_provider=cold_provider,
    )
    repeated = await cold.run({"message": "must not execute", "correlationId": "tool-turn"})
    assert repeated.to_dict() == original
    assert cold_client.received_messages == [] and cold_provider.writes == 0
    assert cold_provider.state.data.response_mailbox == mailbox
    assert cold_provider.state.data.completed_correlations == receipts
    await cold.run({"message": "continue", "correlationId": "next"})
    assert [message.text for message in cold_client.received_messages[0]] == [
        "seed summary",
        "seed answer",
        PROMPT,
        "",
        "",
        "answer-2",
        "continue",
    ]
    assert cold_provider.writes == 1
    assert len(ids(cold_provider)) == len(set(ids(cold_provider)))


@pytest.mark.parametrize("provider_kind", ["in-memory", "explicit-durable"])
@pytest.mark.parametrize("per_call", [False, True])
@pytest.mark.parametrize("store_inputs", [False, True])
@pytest.mark.parametrize("store_outputs", [False, True])
@pytest.mark.parametrize("store_context_messages", [False, True])
@pytest.mark.parametrize("store_context_from", [None, set(), {"selected"}])
async def test_all_store_flags_survive_substitution_and_control_real_core_hooks(
    provider_kind: str,
    per_call: bool,
    store_inputs: bool,
    store_outputs: bool,
    store_context_messages: bool,
    store_context_from: set[str] | None,
) -> None:
    factory = InMemoryHistoryProvider if provider_kind == "in-memory" else DurableHistoryProvider
    original = factory(
        "custom-history",
        store_inputs=store_inputs,
        store_outputs=store_outputs,
        store_context_messages=store_context_messages,
        store_context_from=store_context_from,
        skip_excluded=False,
    )
    client = ToolChatClient(tool_calls=False)
    agent = Agent(
        client=client,
        context_providers=[original, AddContext("selected"), AddContext("other")],
        require_per_service_call_history_persistence=per_call,
    )
    prepared: Any = ensure_durable_history(agent, prune_excluded=True)
    history = prepared.context_providers[0]
    assert isinstance(history, DurableHistoryProvider)
    assert history is not original and agent.context_providers[0] is original
    assert history.source_id == "custom-history"
    assert history.skip_excluded is False and history.prune_excluded is True
    assert history.store_inputs is store_inputs and history.store_outputs is store_outputs
    assert history.store_context_messages is store_context_messages
    assert history.store_context_from == store_context_from
    if store_context_from is not None:
        assert history.store_context_from is not original.store_context_from
    if isinstance(original, DurableHistoryProvider):
        assert original.prune_excluded is None
    provider = _InMemoryStateProvider()
    session = prepared.create_session()
    with bound(provider):
        await prepared.run("input", session=session)
        history.flush(session.state[history.source_id])
    expected_context = [
        f"context-{source}"
        for source in ("selected", "other")
        if store_context_messages and (store_context_from is None or source in store_context_from)
    ]
    expected_inputs = [*expected_context, *(["input"] if store_inputs else [])]
    expected_outputs = ["answer-1"] if store_outputs else []
    assert [message.text for message in transcript(provider)] == expected_inputs + expected_outputs
    entries = provider.state.data.conversation_history
    assert [entry.json_type for entry in entries] == (
        ([DurableAgentStateEntryJsonType.REQUEST] if expected_inputs else [])
        + ([DurableAgentStateEntryJsonType.RESPONSE] if expected_outputs else [])
    )
    assert [message.text for message in client.received_messages[0]] == ["context-selected", "context-other", "input"]
    assert provider.writes == 0


@pytest.mark.parametrize("prune_excluded", [False, True])
def test_explicit_pruning_and_store_only_sinks_are_not_reconfigured(prune_excluded: bool) -> None:
    history = DurableHistoryProvider(
        store_inputs=False,
        store_outputs=False,
        store_context_messages=True,
        store_context_from={"selected"},
        prune_excluded=prune_excluded,
    )
    sink = InMemoryHistoryProvider("sink", load_messages=False, store_inputs=False)
    client: Any = RecordingChatClient()
    agent = Agent(client=client, context_providers=[history, sink])
    assert ensure_durable_history(agent, prune_excluded=not prune_excluded) is agent
    assert agent.context_providers == [history, sink]
    external = CaptureHistory(load_messages=True)
    external_client: Any = RecordingChatClient()
    external_agent = Agent(client=external_client, context_providers=[external, sink])
    assert ensure_durable_history(external_agent) is external_agent
    with pytest.raises(ValueError, match="primary"):
        conflicting_client: Any = RecordingChatClient()
        ensure_durable_history(Agent(client=conflicting_client, context_providers=[history, external, sink]))


@pytest.mark.parametrize("per_call", [False, True])
async def test_service_ownership_suppresses_only_durable_history(per_call: bool) -> None:
    provider = _InMemoryStateProvider()
    seed(provider)
    before = deepcopy(provider.state.to_dict())
    history = DurableHistoryProvider(prune_excluded=True)
    sink = CaptureHistory()
    client = ToolChatClient(tool_calls=False)
    agent = Agent(
        client=client,
        default_options={"store": True},
        context_providers=[history, sink],
        require_per_service_call_history_persistence=per_call,
    )
    session = agent.create_session()
    with bound(provider, service_owns_history=True) as binding:
        await agent.run("service input", session=session)
        assert await history.get_messages(session.session_id) == []
        await history.save_messages(session.session_id, [Message("user", ["do not store"])])
        history.flush({WORKING_BUFFER_KEY: [Message("assistant", ["do not insert"])]})
        assert binding.append_ordinal == 0
    assert provider.state.to_dict() == before and provider.writes == 0
    assert [message.text for message in client.received_messages[0]] == ["service input"]
    assert [[message.text for message in batch] for batch in sink.saved] == [["service input", "answer-1"]]


@pytest.mark.parametrize("per_call", [False, True])
@pytest.mark.parametrize("stream", [False, True], ids=["non-streaming", "streaming"])
async def test_failed_core_call_does_not_persist_inputs_before_the_history_hook(per_call: bool, stream: bool) -> None:
    provider = _InMemoryStateProvider()
    seed(provider)
    before = deepcopy(provider.state.to_dict())
    history = CountingHistory([])
    agent = Agent(
        client=ToolChatClient(fail=True),
        context_providers=[history],
        require_per_service_call_history_persistence=per_call,
    )
    session = agent.create_session()
    with bound(provider) as binding:
        with pytest.raises(RuntimeError, match="model failed"):
            if stream:
                await agent.run("failed input", session=session, stream=True).get_final_response()
            else:
                await agent.run("failed input", session=session)
        history.finalize_failed_run(session.state[history.source_id])
        history.flush(session.state[history.source_id])
        assert binding.pending_inputs == []
    assert history.after_calls == 0
    assert provider.state.to_dict() == before
    assert provider.writes == 0


@pytest.mark.parametrize("stream", [False, True], ids=["non-streaming", "streaming"])
async def test_failed_second_service_call_commits_actual_tool_result_and_cold_replays_it(stream: bool) -> None:
    tool_calls: list[str] = []

    @tool(name="lookup", approval_mode="never_require")
    def counted_lookup(key: str) -> str:
        tool_calls.append(key)
        return f"value:{key}"

    history = CountingHistory([])
    client = ToolChatClient(fail_on_call=2)
    agent_type = Agent if stream else NonStreamingAgent
    agent = agent_type(
        client=client,
        tools=[counted_lookup],
        context_providers=[history],
        require_per_service_call_history_persistence=True,
    )
    provider = _InMemoryStateProvider()
    session = agent.create_session()
    foreign_state = {"approval": {"pending": ["keep"]}}
    session.state["foreign-provider"] = deepcopy(foreign_state)
    provider.state.data.session = session.to_dict()
    entity = AgentEntity(agent, state_provider=provider)
    message = Message(
        "user", [PROMPT], message_id="projected-input", additional_properties={"trace": {"tags": ["original"]}}
    )
    original_input = deepcopy(message.to_dict())
    request = {"message": PROMPT, "correlationId": "failed-tool", "contextMessages": [deepcopy(original_input)]}

    failed = await entity.run(request)

    assert_tool_follow_up(client)
    assert history.before_calls == 2 and history.after_calls == 1
    assert tool_calls == ["durable"]
    assert failed.additional_properties["durable_status"] == "error"
    assert "model failed" in failed.text and provider.writes == 1
    assert message.to_dict() == original_input and request["contextMessages"] == [original_input]
    assert [entry.json_type for entry in provider.state.data.conversation_history] == [
        DurableAgentStateEntryJsonType.REQUEST,
        DurableAgentStateEntryJsonType.RESPONSE,
        DurableAgentStateEntryJsonType.REQUEST,
    ]
    assert all(entry.correlation_id == "failed-tool" for entry in provider.state.data.conversation_history)
    assert [message.text for message in transcript(provider)] == [PROMPT, "", ""]
    actual_result = next(
        content
        for message in client.received_messages[1]
        for content in message.contents
        if content.type == "function_result"
    )
    saved_result = transcript(provider)[-1].to_chat_message()
    assert len(saved_result.contents) == 1
    assert saved_result.contents[0].call_id == actual_result.call_id == "call-1"
    assert saved_result.contents[0].result == actual_result.result == "value:durable"

    raw = provider._get_state_dict()
    data = raw["data"]
    assert data["responseMailbox"]["failed-tool"]["response"] == failed.to_dict()
    assert "failed-tool" in data["completedCorrelations"]
    assert data["ingestedMessages"] == {"projected-input": [message_identity(message)]}
    assert history.source_id not in data["session"]["state"]
    assert data["session"]["state"]["foreign-provider"] == foreign_state
    cold_provider = _InMemoryStateProvider(raw=raw)
    cold_client = ToolChatClient(tool_calls=False)
    cold = AgentEntity(
        agent_type(
            client=cold_client,
            tools=[counted_lookup],
            context_providers=[DurableHistoryProvider(prune_excluded=False)],
            require_per_service_call_history_persistence=True,
        ),
        state_provider=cold_provider,
    )
    repeated = await cold.run(request)
    assert repeated.to_dict() == failed.to_dict()
    assert cold_client.received_messages == [] and cold_provider.writes == 0
    await cold.run({"message": "continue", "correlationId": "next"})
    replayed = cold_client.received_messages[0]
    assert [message.text for message in replayed] == [PROMPT, "", "", "continue"]
    assert [message.role for message in replayed] == ["user", "assistant", "tool", "user"]
    calls = [content for message in replayed for content in message.contents if content.type == "function_call"]
    results = [content for message in replayed for content in message.contents if content.type == "function_result"]
    assert len(calls) == len(results) == 1
    assert calls[0].call_id == results[0].call_id == actual_result.call_id
    assert results[0].result == actual_result.result
    assert tool_calls == ["durable"] and cold_provider.writes == 1
    assert cold_provider.state.data.response_mailbox["failed-tool"] == data["responseMailbox"]["failed-tool"]
    assert (
        cold_provider.state.data.completed_correlations["failed-tool"] == (data["completedCorrelations"]["failed-tool"])
    )


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
@pytest.mark.parametrize("store_inputs", [False, True])
@pytest.mark.parametrize("store_outputs", [False, True])
@pytest.mark.parametrize("stream", [False, True], ids=["non-streaming", "streaming"])
async def test_failure_finalization_respects_real_core_cadence_and_store_flags(
    per_call: bool, store_inputs: bool, store_outputs: bool, stream: bool
) -> None:
    tool_calls: list[str] = []

    @tool(name="lookup", approval_mode="never_require")
    def counted_lookup(key: str) -> str:
        tool_calls.append(key)
        return f"value:{key}"

    provider = _InMemoryStateProvider()
    history = DurableHistoryProvider(store_inputs=store_inputs, store_outputs=store_outputs, prune_excluded=False)
    client = ToolChatClient(fail_on_call=2)
    agent = Agent(
        client=client,
        tools=[counted_lookup],
        context_providers=[history],
        require_per_service_call_history_persistence=per_call,
    )
    session = agent.create_session()
    with bound(provider) as binding:
        with pytest.raises(RuntimeError, match="model failed"):
            if stream:
                await agent.run(PROMPT, session=session, stream=True).get_final_response()
            else:
                await agent.run(PROMPT, session=session)
        state = session.state[history.source_id]
        before = deepcopy(provider.state.to_dict())
        assert bool(binding.pending_inputs) is (per_call and store_inputs)
        expected = ([PROMPT] if per_call and store_inputs else []) + ([""] if per_call and store_outputs else [])
        assert [message.text for message in transcript(provider)] == expected
        history.finalize_failed_run(state)
        history.flush(state)
        assert binding.pending_inputs == []
        expected_result = per_call and store_inputs and store_outputs
        assert [message.text for message in transcript(provider)] == expected + ([""] if expected_result else [])
        results = [
            content
            for message in transcript(provider)
            for content in message.to_chat_message().contents
            if content.type == "function_result"
        ]
        assert len(results) == int(expected_result)
        if expected_result:
            assert results[0].result == "value:durable"
            assert_tool_follow_up(client)
        else:
            assert provider.state.to_dict() == before
        snapshot = deepcopy(provider.state.to_dict())
        ordinal = binding.append_ordinal
        history.finalize_failed_run(state)
        history.flush(state)
        assert provider.state.to_dict() == snapshot and binding.append_ordinal == ordinal
    assert len(client.received_messages) == 2 and tool_calls == ["durable"]
    assert provider.writes == 0


@pytest.mark.parametrize("message_id", [None, "shared"], ids=["anonymous", "reused-id"])
async def test_failed_inputs_preserve_matched_groups_metadata_and_original_ingestion_hash(
    message_id: str | None,
) -> None:
    provider = _InMemoryStateProvider()
    history = DurableHistoryProvider(prune_excluded=False)
    agent = Agent(client=ToolChatClient(), require_per_service_call_history_persistence=True)
    session = agent.create_session()
    state: dict[str, Any] = {}
    session.state[history.source_id] = state
    old_call = Message(
        "assistant",
        [Content.from_function_call("historical", "lookup", arguments={})],
        message_id="old-call",
    )
    provider.state.data.conversation_history.append(
        DurableAgentStateResponse("previous", OLD, [DurableAgentStateMessage.from_chat_message(old_call)])
    )
    with bound(provider) as binding:
        await history.before_run(
            agent=agent,
            session=session,
            context=SessionContext(input_messages=[Message("user", ["discard this snapshot"])]),
            state=state,
        )
        assert binding.pending_inputs
        context = SessionContext(input_messages=[Message("user", [PROMPT], message_id="shared")])
        context._response = AgentResponse(
            messages=[
                Message(
                    "assistant",
                    [
                        Content.from_function_call(call_id, "lookup", arguments={})
                        for call_id in ("call-1", "call-2", "completed")
                    ],
                )
            ]
        )
        await history.after_run(agent=agent, session=session, context=context, state=state)
        assert binding.pending_inputs == [], "a standalone after_run must also clear the pending snapshot"
        await history.save_messages(
            session.session_id,
            [Message("tool", [Content.from_function_result("completed", result="already stored")])],
            state=state,
        )
        result = Message(
            "tool",
            [
                Content("function_result", call_id=call_id, result={"values": [call_id]})
                for call_id in ("call-1", "call-2")
            ],
            message_id=message_id,
            additional_properties={"trace": {"tags": ["original"]}},
        )
        original = deepcopy(result.to_dict())
        inputs = [
            Message("user", ["unrelated fresh request"]),
            Message("tool", [Content.from_function_result("historical", result="wrong correlation")]),
            Message("tool", [Content.from_function_result("unknown", result="no stored call")]),
            Message("tool", [Content.from_function_result("completed", result="duplicate result")]),
            Message("tool", []),
            Message("tool", [Content("function_result", result="no call id")]),
            Message("user", [Content.from_function_result("call-1", result="wrong role")]),
            Message(
                "tool", [Content.from_function_result("call-1", result="mixed input"), Content.from_text("fresh input")]
            ),
            Message(
                "tool",
                [
                    Content.from_function_result("call-1", result="mixed ids"),
                    Content.from_function_result("unknown", result="no"),
                ],
            ),
            Message("tool", [Content.from_function_result("call-1", result="duplicate id")] * 2),
            result,
            deepcopy(result),
        ]
        before = deepcopy(provider.state.to_dict())
        await history.before_run(
            agent=agent,
            session=session,
            context=SessionContext(
                input_messages=[Message("tool", [Content.from_function_result("call-1", result="superseded snapshot")])]
            ),
            state=state,
        )
        await history.before_run(
            agent=agent, session=session, context=SessionContext(input_messages=inputs), state=state
        )
        assert provider.state.to_dict() == before, "before_run must capture, not append"
        assert binding.pending_inputs[-2] is not result
        assert binding.pending_inputs[-2].additional_properties["trace"] is not result.additional_properties["trace"]
        assert result.to_dict() == original
        result.contents[0].result["values"].append("caller mutation")
        result.additional_properties["trace"]["tags"].append("caller mutation")
        history.finalize_failed_run(state)
        assert binding.pending_inputs == []
        assert set(state) == {WORKING_BUFFER_KEY, POSITIONS_KEY}
        assert len(transcript(provider)) == 5
        saved = transcript(provider)[-1]
        assert saved.ingestion_identity == message_identity(Message.from_dict(original))
        assert saved.message_id and saved.message_id != message_id
        assert result.message_id == message_id
        assert [content.to_dict()["result"] for content in saved.contents] == [
            {"values": ["call-1"]},
            {"values": ["call-2"]},
        ]
        assert saved.extension_data == {"trace": {"tags": ["original"]}}
        working = state[WORKING_BUFFER_KEY][-1]
        working.additional_properties["trace"]["tags"].append("compaction")
        working.contents[0].result["values"].append("working mutation")
        assert saved.extension_data == {"trace": {"tags": ["original"]}}
        history.flush(state)
        assert saved.extension_data == {"trace": {"tags": ["original", "compaction"]}}
        assert saved.contents[0].to_dict()["result"] == {"values": ["call-1"]}
        assert_current_positions(provider, state)
        snapshot = deepcopy(provider.state.to_dict())
        ordinal = binding.append_ordinal
        history.finalize_failed_run(state)
        history.flush(state)
        assert provider.state.to_dict() == snapshot and binding.append_ordinal == ordinal
    assert provider.writes == 0


@pytest.mark.parametrize("disabled_by", ["service", "store-inputs", "no-correlation"])
async def test_finalizing_pending_inputs_rechecks_binding_and_input_storage(disabled_by: str) -> None:
    provider = _InMemoryStateProvider()
    history = DurableHistoryProvider(prune_excluded=False)
    agent = Agent(client=ToolChatClient(), require_per_service_call_history_persistence=True)
    session = agent.create_session()
    state: dict[str, Any] = {}
    with bound(provider) as binding:
        context = SessionContext(input_messages=[])
        context._response = AgentResponse(
            messages=[Message("assistant", [Content.from_function_call("call-1", "lookup", arguments={})])]
        )
        await history.after_run(agent=agent, session=session, context=context, state=state)
        await history.before_run(
            agent=agent,
            session=session,
            context=SessionContext(
                input_messages=[Message("tool", [Content.from_function_result("call-1", result="actual result")])]
            ),
            state=state,
        )
        assert binding.pending_inputs
        before = deepcopy(provider.state.to_dict())
        ordinal = binding.append_ordinal
        if disabled_by == "service":
            binding.service_owns_history = True
        elif disabled_by == "store-inputs":
            history.store_inputs = False
        else:
            binding.correlation_id = None
        history.finalize_failed_run(state)
        assert binding.pending_inputs == []
        assert provider.state.to_dict() == before and binding.append_ordinal == ordinal
    history.finalize_failed_run(state)
    assert provider.state.to_dict() == before and provider.writes == 0


async def test_failed_load_does_not_store_a_raw_historical_tool_continuation() -> None:
    class FailingLoadHistory(DurableHistoryProvider):
        async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
            raise OSError("history load failed")

    provider = _InMemoryStateProvider()
    old_call = Message(
        "assistant", [Content.from_function_call("call-1", "lookup", arguments={})], message_id="old-call"
    )
    provider.state.data.conversation_history.append(
        DurableAgentStateResponse("previous", OLD, [DurableAgentStateMessage.from_chat_message(old_call)])
    )
    before = deepcopy(provider.state.to_dict())
    history = FailingLoadHistory(prune_excluded=False)
    client = ToolChatClient()
    agent = Agent(client=client, context_providers=[history], require_per_service_call_history_persistence=True)
    session = agent.create_session()
    messages = [
        Message("tool", [Content.from_function_result("call-1", result="caller result")]),
        Message("user", ["new input"]),
    ]
    with bound(provider) as binding:
        with pytest.raises(OSError, match="history load failed"):
            await agent.run(messages, session=session)
        assert binding.pending_inputs
        history.finalize_failed_run(session.state[history.source_id])
        history.flush(session.state[history.source_id])
        assert binding.pending_inputs == []
    assert provider.state.to_dict() == before
    assert client.received_messages == [] and provider.writes == 0


@pytest.mark.parametrize("with_state", [False, True])
async def test_generic_save_appends_anonymous_messages_with_stable_write_time_ids(with_state: bool) -> None:
    provider = _InMemoryStateProvider()
    history = DurableHistoryProvider()
    state: dict[str, Any] | None = {} if with_state else None
    messages = [Message("user", ["repeat"]), Message("user", ["repeat"])]
    with bound(provider, "append") as binding:
        await history.save_messages("session", messages, state=state)
        await history.save_messages("session", messages[:1], state=state)
        await history.save_messages("session", [], state=state)
        assert binding.append_ordinal == 2
        assert ids(provider) == [
            "durable_request_append_0_0",
            "durable_request_append_0_1",
            "durable_request_append_1_0",
        ]
        assert [message.message_id for message in messages] == [None, None]
        assert [message.text for message in transcript(provider)] == ["repeat"] * 3
        if state is not None:
            assert_current_positions(provider, state)
    assert provider.writes == 0
    cold = _InMemoryStateProvider(raw=json.loads(provider.state.to_json()))
    with bound(cold, "append"):
        loaded = await history.get_messages("session")
    assert [message.message_id for message in loaded] == ids(provider)
    assert [message.text for message in loaded] == ["repeat"] * 3


async def test_reused_ids_get_internal_revisions_without_changing_external_ids() -> None:
    provider = _InMemoryStateProvider()
    history = DurableHistoryProvider(prune_excluded=False)
    sink = CaptureHistory()
    agent = Agent(
        client=ToolChatClient(tool_calls=False, response_message_id="shared"),
        context_providers=[history, sink],
    )
    session = agent.create_session()
    inputs = [
        Message("user", ["version one"], message_id="shared"),
        Message("user", ["version two"], message_id="shared"),
    ]
    responses: list[AgentResponse] = []
    for index, message in enumerate(inputs):
        with bound(provider, f"revision-{index}"):
            responses.append(await agent.run(message, session=session))
            history.flush(session.state[history.source_id])
    assert [message.message_id for message in inputs] == ["shared", "shared"]
    assert [response.messages[0].message_id for response in responses] == ["shared", "shared"]
    assert [[message.message_id for message in batch] for batch in sink.saved] == [["shared", "shared"]] * 2
    assert len(ids(provider)) == len(set(ids(provider))) == 4
    assert ids(provider)[0] == "shared"
    assert all(message_id and message_id.startswith("durable_revision_") for message_id in ids(provider)[1:])
    assert [message.text for message in transcript(provider)] == ["version one", "answer-1", "version two", "answer-2"]
    before = deepcopy(provider.state.to_dict())
    inputs[0].contents[0].text = "caller changed input"
    responses[-1].messages[0].additional_properties["model_metadata"]["tags"].append("caller changed output")
    assert provider.state.to_dict() == before
    cold = _InMemoryStateProvider(raw=json.loads(provider.state.to_json()))
    state: dict[str, Any] = {}
    with bound(cold):
        loaded = await history.get_messages("session", state=state)
        loaded[2].additional_properties["revision_marker"] = True
        history.flush(state)
        marked = [message.text for message in transcript(cold) if (message.extension_data or {}).get("revision_marker")]
        assert marked == ["version two"]
        assert_current_positions(cold, state)
    assert [message.message_id for message in loaded] == ids(provider)
    assert [message.text for message in loaded] == ["version one", "answer-1", "version two", "answer-2"]


async def test_generated_ids_reserve_supplied_ids_in_the_same_batch() -> None:
    provider = _InMemoryStateProvider()
    history = DurableHistoryProvider()
    reserved_id = "durable_request_current_0_0"
    messages = [Message("user", ["anonymous"]), Message("user", ["supplied"], message_id=reserved_id)]
    with bound(provider):
        await history.save_messages("session", messages)
    assert ids(provider) == [f"{reserved_id}_1", reserved_id]
    assert [message.message_id for message in messages] == [None, reserved_id]


async def test_append_and_flush_never_alias_tool_payloads_or_response_annotations() -> None:
    provider = _InMemoryStateProvider()
    history = DurableHistoryProvider(prune_excluded=False)
    session = AgentSession()
    result = Message(
        "tool",
        [Content("function_result", call_id="call-1", result={"values": [1]})],
        additional_properties={"trace": {"tags": ["original"]}},
    )
    response = AgentResponse(messages=[result], additional_properties={"receipt": {"tags": ["original"]}})
    before = deepcopy(response.to_dict())
    context = SessionContext(input_messages=[Message("user", ["input"])])
    context._response = response
    state: dict[str, Any] = {}
    with bound(provider):
        await history.after_run(agent=None, session=session, context=context, state=state)
        assert ids(provider) == ["durable_request_current_0_0", "durable_response_current_1_0"]
        assert context.input_messages[0].message_id is None and response.messages[0].message_id is None
        working = state[WORKING_BUFFER_KEY][-1]
        working.additional_properties["trace"]["tags"].append("compaction")
        working.contents[0].result["values"].append(2)
        assert transcript(provider)[-1].contents[0].to_dict()["result"] == {"values": [1]}
        assert (transcript(provider)[-1].extension_data or {})["trace"] == {"tags": ["original"]}
        history.flush(state)
        assert (transcript(provider)[-1].extension_data or {})["trace"] == {"tags": ["original", "compaction"]}
        assert transcript(provider)[-1].contents[0].to_dict()["result"] == {"values": [1]}
        assert response.to_dict() == before
        working.additional_properties["trace"]["tags"].append("not flushed")
        assert (transcript(provider)[-1].extension_data or {})["trace"] == {"tags": ["original", "compaction"]}
    assert provider.writes == 0


async def test_repeated_core_summaries_survive_pruning_id_reuse_and_cold_reload() -> None:
    provider = _InMemoryStateProvider()
    seed(provider)
    history = DurableHistoryProvider(prune_excluded=True)
    summary_client = ToolChatClient(tool_calls=False)
    compaction = CompactionProvider(
        after_strategy=SummarizationStrategy(
            client=summary_client, target_count=2, threshold=0, max_summary_input_tokens=None
        ),
        history_source_id=history.source_id,
    )
    session = AgentSession()
    state: dict[str, Any] = {}
    session.state[history.source_id] = state
    generated_ids: list[str | None] = []
    for turn in range(3):
        with bound(provider, f"turn-{turn}") as binding:
            await history.get_messages(session.session_id, state=state)
            await history.save_messages(
                session.session_id,
                [
                    Message("user", [f"question-{turn}"], message_id=f"user-{turn}"),
                    Message("assistant", [f"response-{turn}"], message_id=f"assistant-{turn}"),
                ],
                state=state,
            )
            await compaction.after_run(agent=None, session=session, context=None, state={})
            buffer = state[WORKING_BUFFER_KEY]
            summary = buffer[0]
            assert summary.text == f"answer-{turn + 1}"
            generated_id = summary.message_id
            generated_ids.append(generated_id)
            original_links = deepcopy(summary.additional_properties["_group"])
            originals = [
                message for message in buffer[1:] if message.message_id in original_links["_summary_of_message_ids"]
            ]
            # Core may re-include the older summary while grouping duplicate IDs. Preserve its
            # actual inclusion decisions, without hiding the new summary's text or identity.
            expected_texts = [message.text for message in buffer if not message.additional_properties.get("_excluded")]
            assert originals
            if turn == 2:
                older_summary = next(message for message in originals if message.message_id == generated_id)
                assert older_summary.text == "answer-2"
                assert sum(message.message_id == generated_id for message in buffer) == 2

            history.flush(state)
            if turn == 2:
                assert summary.message_id != generated_id
                assert summary.message_id.startswith("durable_revision_compaction_turn-2_")
            assert [message.text for message in transcript(provider)] == expected_texts
            assert len(ids(provider)) == len(set(ids(provider))) == len(expected_texts)
            assert (
                summary.additional_properties["_group"]["_summary_of_message_ids"]
                == (original_links["_summary_of_message_ids"])
            )
            assert (
                summary.additional_properties["_group"]["_summary_of_group_ids"]
                == (original_links["_summary_of_group_ids"])
            )
            assert summary.additional_properties["_group"]["id"] == f"group_{summary.message_id}"
            assert all(
                message.additional_properties["_group"]["_summarized_by_summary_id"] == summary.message_id
                for message in originals
            )
            assert all(
                (message.extension_data or {})["_group"]["_summarized_by_summary_id"] == summary.message_id
                for message in transcript(provider)
                if message.message_id in original_links["_summary_of_message_ids"]
            )
            assert_current_positions(provider, state)
            snapshot = deepcopy(provider.state.to_dict())
            ordinal = binding.append_ordinal
            history.flush(state)
            assert provider.state.to_dict() == snapshot and binding.append_ordinal == ordinal
    assert generated_ids == ["summary_4", "summary_5", "summary_5"]
    assert len(summary_client.received_messages) == 3 and provider.writes == 0

    cold = _InMemoryStateProvider(raw=json.loads(provider.state.to_json()))
    cold_history = DurableHistoryProvider(prune_excluded=True)
    cold_state: dict[str, Any] = {}
    with bound(cold, "cold"):
        loaded = await cold_history.get_messages(session.session_id, state=cold_state)
        assert loaded[0].text == "answer-3"
        assert [message.text for message in loaded] == expected_texts
        assert [message.message_id for message in loaded] == ids(provider)
        assert loaded[0].additional_properties == transcript(provider)[0].extension_data
        cold_history.flush(cold_state)
        assert cold.state.to_dict() == provider.state.to_dict()
        assert_current_positions(cold, cold_state)
    assert cold.writes == 0


@pytest.mark.parametrize("nested_links", [False, True], ids=["top-level", "core-group"])
@pytest.mark.parametrize("remove_old_summary", [False, True], ids=["old-in-buffer", "old-removed"])
async def test_reused_summary_ids_keep_older_links_and_original_contents(
    nested_links: bool, remove_old_summary: bool
) -> None:
    provider = _InMemoryStateProvider()
    seed(provider)
    provider.state.data.conversation_history.append(
        DurableAgentStateRequest("current", OLD, [stored("current", "current")])
    )
    history = DurableHistoryProvider(prune_excluded=False)
    session = AgentSession()
    state: dict[str, Any] = {}
    session.state[history.source_id] = state
    calls = 0

    def links(message: Message) -> dict[str, Any]:
        if nested_links:
            return message.additional_properties.setdefault("_group", {})
        return message.additional_properties

    async def summarize(messages: list[Message]) -> bool:
        nonlocal calls
        source_id = "seed-user" if calls == 0 else "seed-assistant"
        source = next(message for message in messages if message.message_id == source_id)
        calls += 1
        summary_id = "repeated-summary"
        source.additional_properties["_excluded"] = True
        links(source)["_summarized_by_summary_id"] = summary_id
        if calls == 2:
            # Ordinary source edits must not turn annotation reconciliation into content replacement.
            source.contents[0].text = "working-only text"
            if remove_old_summary:
                messages[:] = [message for message in messages if message.message_id != summary_id]
        summary = Message(
            "assistant",
            [Content.from_text(f"summary version {calls}", additional_properties={"trace": {"call": calls}})],
            message_id=summary_id,
        )
        links(summary)["_summary_of_message_ids"] = [source_id]
        insertion_index = messages.index(source) + 1
        messages.insert(insertion_index, summary)
        annotate_message_groups(messages, from_index=insertion_index)
        return True

    compaction = CompactionProvider(after_strategy=summarize, history_source_id=history.source_id)
    with bound(provider) as binding:
        await history.get_messages(session.session_id, state=state)
        await compaction.after_run(agent=None, session=session, context=None, state={})
        history.flush(state)
        await compaction.after_run(agent=None, session=session, context=None, state={})
        new_summary = next(message for message in state[WORKING_BUFFER_KEY] if message.text == "summary version 2")
        assert new_summary.message_id == "repeated-summary"
        history.flush(state)
        assert new_summary.message_id != "repeated-summary"
        # Removing a summary from the logical buffer does not erase its body or lineage under keep_all.
        assert [message.text for message in transcript(provider)] == [
            "seed question",
            "summary version 1",
            "seed answer",
            "summary version 2",
            "current",
        ]
        assert len(ids(provider)) == len(set(ids(provider))) == 5
        stored_messages = {message.message_id: message.to_chat_message() for message in transcript(provider)}
        assert links(stored_messages["seed-user"])["_summarized_by_summary_id"] == "repeated-summary"
        assert links(stored_messages["seed-assistant"])["_summarized_by_summary_id"] == new_summary.message_id
        assert links(stored_messages["repeated-summary"])["_summary_of_message_ids"] == ["seed-user"]
        assert links(stored_messages[new_summary.message_id])["_summary_of_message_ids"] == ["seed-assistant"]
        assert_current_positions(provider, state)
        snapshot = deepcopy(provider.state.to_dict())
        ordinal = binding.append_ordinal
        history.flush(state)
        assert provider.state.to_dict() == snapshot and binding.append_ordinal == ordinal
    assert calls == 2 and provider.writes == 0

    cold = _InMemoryStateProvider(raw=json.loads(provider.state.to_json()))
    cold_history = DurableHistoryProvider(prune_excluded=False)
    cold_state: dict[str, Any] = {}
    with bound(cold):
        loaded = await cold_history.get_messages(session.session_id, state=cold_state)
        assert [message.text for message in loaded] == [
            *([] if remove_old_summary else ["summary version 1"]),
            "summary version 2",
            "current",
        ]
        assert [message.message_id for message in loaded] == [
            *([] if remove_old_summary else ["repeated-summary"]),
            new_summary.message_id,
            "current",
        ]
        cold_history.flush(cold_state)
        assert cold.state.to_dict() == provider.state.to_dict()
        assert_current_positions(cold, cold_state)
    assert cold.writes == 0


@pytest.mark.parametrize("entry_type", [DurableAgentStateRequest, DurableAgentStateResponse])
@pytest.mark.parametrize("message_count", [2, 4])
async def test_multiple_mid_entry_summaries_keep_exact_order_metadata_and_receipts(
    entry_type: type[DurableAgentStateRequest] | type[DurableAgentStateResponse], message_count: int
) -> None:
    provider = _InMemoryStateProvider()
    source_ids = [f"item-{index}" for index in range(message_count)]
    owner = entry_type("old", OLD, [stored(message_id, message_id) for message_id in source_ids])
    owner.extension_data = {"envelope": {"tags": ["original"]}}
    owner.unknown_fields = {"futureField": {"keep": [1]}}
    if isinstance(owner, DurableAgentStateRequest):
        owner.orchestration_id = "workflow"
        owner.response_schema = {"properties": {"value": {"type": "string"}}}
    else:
        owner.usage = DurableAgentStateUsage(input_token_count=7)
    unknown = DurableAgentStateUnknownEntry({"$type": "futureKind", "future": {"opaque": [1, 2]}})
    current = DurableAgentStateRequest("current", OLD, [stored("current", "current")])
    provider.state.data.conversation_history.extend([unknown, owner, current])
    provider.state.record_response(
        "old",
        AgentResponse(messages=[Message("assistant", ["original answer"])]),
        delivery_window_seconds=3600,
    )
    mailbox = deepcopy(provider.state.data.response_mailbox)
    receipts = deepcopy(provider.state.data.completed_correlations)
    history = DurableHistoryProvider(prune_excluded=False)
    state: dict[str, Any] = {}
    with bound(provider):
        loaded = await history.get_messages("session", state=state)
        buffer: list[Message] = []
        expected: list[str] = []
        for index, message in enumerate(loaded[:-1]):
            buffer.append(message)
            expected.append(source_ids[index])
            if index + 1 < message_count:
                summary_id = f"summary-{index}"
                buffer.append(Message("assistant", [summary_id], message_id=summary_id))
                expected.append(summary_id)
        buffer[-1].additional_properties["target"] = "last original"
        buffer.append(loaded[-1])
        expected.append("current")
        state[WORKING_BUFFER_KEY] = buffer
        history.flush(state)
        assert ids(provider) == expected
        assert (transcript(provider)[-2].extension_data or {})["target"] == "last original"
        assert_current_positions(provider, state)
        envelopes: list[Any] = [
            entry
            for entry in provider.state.data.conversation_history
            if isinstance(entry, entry_type) and entry.correlation_id == "old"
        ]
        assert len(envelopes) == message_count
        for entry in envelopes:
            assert entry.correlation_id == "old" and entry.created_at == OLD
            assert entry.extension_data == owner.extension_data and entry.unknown_fields == owner.unknown_fields
            assert all(
                message.message_id and not message.message_id.startswith("summary-") for message in entry.messages
            )
        assert envelopes[0].extension_data is not envelopes[-1].extension_data
        assert envelopes[0].unknown_fields is not envelopes[-1].unknown_fields
        if isinstance(owner, DurableAgentStateRequest):
            assert all(entry.response_schema == owner.response_schema for entry in envelopes)
            assert all(entry.orchestration_id == "workflow" for entry in envelopes)
            assert envelopes[0].response_schema is not envelopes[-1].response_schema
        else:
            assert owner.usage is not None
            expected_usage = owner.usage.to_dict()
            assert all(entry.usage.to_dict() == expected_usage for entry in envelopes)
            assert envelopes[0].usage is not envelopes[-1].usage
        snapshot = deepcopy(provider.state.to_dict())
        history.flush(state)
        assert provider.state.to_dict() == snapshot
    assert provider.state.data.response_mailbox == mailbox
    assert provider.state.data.completed_correlations == receipts

    cold = _InMemoryStateProvider(raw=json.loads(provider.state.to_json()))
    history = DurableHistoryProvider(prune_excluded=True)
    with bound(cold):
        loaded = await history.get_messages("session", state=state)
        assert [message.message_id for message in loaded] == expected
        next(message for message in loaded if message.message_id == "item-1").additional_properties["_excluded"] = True
        state[WORKING_BUFFER_KEY].insert(1, Message("assistant", ["later"], message_id="later-summary"))
        loaded[0].additional_properties["target"] = "first original"
        history.flush(state)
        assert ids(cold) == [
            expected[0],
            "later-summary",
            *[message_id for message_id in expected[1:] if message_id != "item-1"],
        ]
        assert (transcript(cold)[0].extension_data or {})["target"] == "first original"
        assert (cold.state.data.truncation or {})["evictedMessageCount"] == 1
        assert_current_positions(cold, state)
        snapshot = deepcopy(cold.state.to_dict())
        history.flush(state)
        assert cold.state.to_dict() == snapshot
        assert cold.state.data.conversation_history[0].to_dict() == unknown.to_dict()
        assert cold.state.data.response_mailbox == mailbox
        assert cold.state.data.completed_correlations == receipts


@pytest.mark.parametrize("remove_entry", [False, True])
async def test_flush_rebuilds_positions_after_detached_pruning_without_resurrection(remove_entry: bool) -> None:
    provider = _InMemoryStateProvider()
    owner = DurableAgentStateRequest("old", OLD, [stored("a", "a"), stored("b", "b")])
    other = DurableAgentStateResponse("older", OLD, [stored("c", "c", "assistant")])
    provider.state.data.conversation_history.extend([owner, other])
    history = DurableHistoryProvider(prune_excluded=False)
    state: dict[str, Any] = {}
    with bound(provider):
        await history.get_messages("session", state=state)
        replacement = deepcopy(provider.state.data.conversation_history)
        if remove_entry:
            replacement.pop(0)
        else:
            replacement[0].messages.pop(0)
        provider.state.data.conversation_history = replacement
        state[WORKING_BUFFER_KEY][-1].additional_properties["current_owner"] = True
        history.flush(state)
        assert ids(provider) == (["c"] if remove_entry else ["b", "c"])
        assert (transcript(provider)[-1].extension_data or {})["current_owner"] is True
        assert not (other.messages[0].extension_data or {}).get("current_owner")
        assert_current_positions(provider, state)
        snapshot = deepcopy(provider.state.to_dict())
        history.flush(state)
        assert provider.state.to_dict() == snapshot


@pytest.mark.parametrize("kind", list(DurableAgentStateEntryJsonType))
def test_prune_only_drops_changed_bare_known_envelopes(kind: DurableAgentStateEntryJsonType) -> None:
    bare = DurableAgentStateEntry(kind, "bare", OLD, [stored("bare", "remove")])
    metadata = DurableAgentStateEntry(kind, "metadata", OLD, [stored("metadata", "remove")], extension_data={})
    metadata.unknown_fields = {"future": {"keep": True}}
    empty = DurableAgentStateRequest("already-empty", OLD, [])
    opaque = DurableAgentStateUnknownEntry({"$type": "futureKind", "payload": {"keep": [1]}})
    unknown = DurableAgentStateEntry("futureKind", "future", OLD, [stored("future", "opaque")])
    history = [opaque, empty, bare, metadata, unknown]
    unknown_before = deepcopy(unknown.to_dict())
    metadata_before = deepcopy(metadata.to_dict())
    message = bare.messages[0]
    prune_messages(
        history,
        [(bare, message), (bare, message), (metadata, metadata.messages[0]), (unknown, unknown.messages[0])],
    )
    assert history == [opaque, empty, metadata, unknown]
    metadata_before["messages"] = []
    assert metadata.to_dict() == metadata_before
    assert unknown.to_dict() == unknown_before
    snapshot = [deepcopy(entry.to_dict()) for entry in history]
    prune_messages(history, [(bare, message)])
    assert [entry.to_dict() for entry in history] == snapshot


@pytest.mark.parametrize("correlation_id", [None, "current"])
async def test_eager_pruning_protects_system_and_current_exchange_with_exact_count(correlation_id: str | None) -> None:
    provider = _InMemoryStateProvider()
    old = DurableAgentStateRequest("old", OLD, [stored("system", "keep instructions", "system"), stored("old", "drop")])
    metadata = DurableAgentStateResponse(
        "old",
        OLD,
        [stored("old-answer", "drop", "assistant")],
        extension_data={"usage-note": {"keep": [1]}},
    )
    current = DurableAgentStateRequest("current", OLD, [stored("current-input", "keep")])
    answer = DurableAgentStateResponse("current", OLD, [stored("current-answer", "keep", "assistant")])
    opaque = DurableAgentStateUnknownEntry({"$type": "futureKind", "payload": {"keep": [1]}})
    empty = DurableAgentStateRequest("already-empty", OLD, [])
    provider.state.data.conversation_history.extend([opaque, empty, old, metadata, current, answer])
    provider.state.record_response(
        "old",
        AgentResponse(messages=[Message("assistant", ["mailbox original"])]),
        delivery_window_seconds=3600,
    )
    mailbox = deepcopy(provider.state.data.response_mailbox)
    receipts = deepcopy(provider.state.data.completed_correlations)
    provider.state.data.truncation = {"evictedMessageCount": 7, "firstEvictedAt": OLD.isoformat(), "future": [1]}
    history = DurableHistoryProvider(prune_excluded=True)
    state: dict[str, Any] = {}
    with bound(provider, correlation_id) as binding:
        await history.get_messages("session", state=state)
        old_message = old.messages[-1]
        for message in state[WORKING_BUFFER_KEY]:
            message.additional_properties["_excluded"] = True
        history.flush(state)
        assert ids(provider) == ["system", "current-input", "current-answer"]
        assert metadata.messages == [] and metadata in provider.state.data.conversation_history
        assert empty in provider.state.data.conversation_history and opaque in provider.state.data.conversation_history
        assert (provider.state.data.truncation or {})["evictedMessageCount"] == 9
        assert (provider.state.data.truncation or {})["firstEvictedAt"] == OLD.isoformat()
        assert (provider.state.data.truncation or {})["future"] == [1]
        snapshot = deepcopy(provider.state.to_dict())
        history._prune(binding, [(old, old_message), (old, old_message)])
        history.flush(state)
        assert provider.state.to_dict() == snapshot
        assert_current_positions(provider, state)
    assert provider.state.data.response_mailbox == mailbox
    assert provider.state.data.completed_correlations == receipts
    assert provider.writes == 0


@pytest.mark.parametrize("protected_by", ["system", "current", "newest"])
async def test_eager_pruning_protects_atomic_groups_intersecting_the_floor(protected_by: str) -> None:
    provider = _InMemoryStateProvider()
    call = DurableAgentStateMessage.from_chat_message(
        Message(
            "assistant",
            [Content.from_function_call(call_id="lookup", name="lookup", arguments="{}")],
            message_id="call",
        )
    )
    result = DurableAgentStateMessage.from_chat_message(
        Message("tool", [Content.from_function_result(call_id="lookup", result="keep")], message_id="result")
    )
    policy = stored("policy", "keep instructions", "system")
    if protected_by == "system":
        policy.extension_data = {"_group": {"id": "saved-policy"}}
        call.extension_data = {"_group": {"id": "saved-policy"}}
    result_owner = "active" if protected_by == "current" else "newest" if protected_by == "newest" else "old"
    provider.state.data.conversation_history.extend([
        DurableAgentStateRequest("policy", OLD, [policy]),
        DurableAgentStateResponse("old", OLD, [call]),
        DurableAgentStateRequest("gap", OLD, [stored("gap", "drop")]),
        DurableAgentStateResponse(result_owner, OLD, [result]),
        DurableAgentStateRequest("newest", OLD, [stored("newest-user", "keep")]),
        DurableAgentStateResponse("newest", OLD, [stored("newest-answer", "keep", "assistant")]),
    ])
    history = DurableHistoryProvider(skip_excluded=False, prune_excluded=True)
    state: dict[str, Any] = {}
    with bound(provider, "active" if protected_by == "current" else None):
        await history.get_messages("session", state=state)
        for message in state[WORKING_BUFFER_KEY]:
            message.additional_properties["_excluded"] = True
        history.flush(state)
        assert ids(provider) == ["policy", "call", "result", "newest-user", "newest-answer"]
        assert (provider.state.data.truncation or {})["evictedMessageCount"] == 1
        if protected_by == "system":
            assert (call.extension_data or {})["_group"] == {"id": "saved-policy"}
            assert (policy.extension_data or {})["_group"] == {"id": "saved-policy"}
        assert_current_positions(provider, state)
        snapshot = deepcopy(provider.state.to_dict())
        history.flush(state)
        assert provider.state.to_dict() == snapshot
    assert provider.writes == 0

    cold = _InMemoryStateProvider(raw=json.loads(provider.state.to_json()))
    with bound(cold, "active" if protected_by == "current" else None):
        cold_state: dict[str, Any] = {}
        loaded = await history.get_messages("session", state=cold_state)
        assert [message.message_id for message in loaded] == ids(provider)
        history.flush(cold_state)
        assert cold.state.to_dict() == provider.state.to_dict()
        assert_current_positions(cold, cold_state)
    assert cold.writes == 0


async def test_legacy_repeated_and_missing_ids_survive_multiple_cold_loads() -> None:
    provider = _InMemoryStateProvider()
    provider.state.data.conversation_history.extend([
        DurableAgentStateRequest("same", OLD, [stored(None, "first")]),
        DurableAgentStateRequest("same", OLD, [stored(None, "second")]),
        DurableAgentStateResponse("same", OLD, [stored("reused", "version one", "assistant")]),
        DurableAgentStateResponse("same", OLD, [stored("reused", "version two", "assistant")]),
    ])
    raw = json.loads(provider.state.to_json())
    history = DurableHistoryProvider()
    snapshots: list[dict[str, Any]] = []
    for _ in range(2):
        cold = _InMemoryStateProvider(raw=raw)
        with bound(cold, "same"):
            loaded = await history.get_messages("session")
        assert [message.text for message in loaded] == ["first", "second", "version one", "version two"]
        assert all(ids(cold)) and len(ids(cold)) == len(set(ids(cold))) == 4
        snapshots.append(cold.state.to_dict())
    assert snapshots[0] == snapshots[1]
    assert DurableAgentState.from_json(json.dumps(snapshots[0])).to_dict() == snapshots[0]


async def test_anonymous_summary_can_be_inserted_into_an_empty_history_once() -> None:
    provider = _InMemoryStateProvider()
    history = DurableHistoryProvider(prune_excluded=False)
    state: dict[str, Any] = {WORKING_BUFFER_KEY: [Message("assistant", ["summary"])], POSITIONS_KEY: {}}
    with bound(provider):
        history.flush(state)
        assert len(provider.state.data.conversation_history) == 1
        assert isinstance(provider.state.data.conversation_history[0], DurableAgentStateCompaction)
        assert ids(provider) == ["durable_compaction_current_0_0"]
        history.flush(state)
        assert len(provider.state.data.conversation_history) == 1
        assert_current_positions(provider, state)
    assert provider.writes == 0


async def test_newest_exchange_is_protected_before_current_inputs_are_appended() -> None:
    provider = _InMemoryStateProvider()
    seed(provider)
    history = DurableHistoryProvider(prune_excluded=True)
    state: dict[str, Any] = {}
    with bound(provider, "not-yet-appended"):
        await history.get_messages("session", state=state)
        for message in state[WORKING_BUFFER_KEY]:
            message.additional_properties["_excluded"] = True
        history.flush(state)
    assert ids(provider) == ["seed-user", "seed-assistant"]
    assert provider.state.data.truncation is None


async def test_direct_save_initializes_the_complete_working_buffer() -> None:
    provider = _InMemoryStateProvider()
    seed(provider)
    history = DurableHistoryProvider()
    state: dict[str, Any] = {}
    with bound(provider):
        await history.save_messages("session", [Message("user", ["next"])], state=state)
    assert [message.text for message in state[WORKING_BUFFER_KEY]] == ["seed question", "seed answer", "next"]
    assert_current_positions(provider, state)


@pytest.mark.parametrize("provider_type", [InMemoryHistoryProvider, DurableHistoryProvider])
@pytest.mark.parametrize("store_inputs", [False, True])
@pytest.mark.parametrize("store_outputs", [False, True])
async def test_entity_does_not_bypass_provider_store_choices(
    provider_type: type[InMemoryHistoryProvider] | type[DurableHistoryProvider],
    store_inputs: bool,
    store_outputs: bool,
) -> None:
    original = provider_type(
        store_inputs=store_inputs,
        store_outputs=store_outputs,
        store_context_messages=True,
        store_context_from={"selected"},
    )
    provider = _InMemoryStateProvider()
    client: Any = RecordingChatClient()
    entity = AgentEntity(
        Agent(client=client, context_providers=[original, AddContext("selected"), AddContext("other")]),
        state_provider=provider,
    )
    response = await entity.run({"message": "input", "correlationId": "choices"})
    assert [message.text for message in transcript(provider)] == [
        "context-selected",
        *(["input"] if store_inputs else []),
        *(["reply-1"] if store_outputs else []),
    ]
    assert provider.state.data.response_mailbox["choices"]["response"] == response.to_dict()
    assert len(client.received_messages) == provider.writes == 1


async def test_entity_failure_before_history_hook_is_mailbox_only() -> None:
    provider = _InMemoryStateProvider()
    entity = AgentEntity(
        Agent(client=ToolChatClient(fail=True), context_providers=[DurableHistoryProvider()]),
        state_provider=provider,
    )
    response = await entity.run({"message": "not saved", "correlationId": "failed"})
    assert provider.state.data.conversation_history == []
    assert provider.state.data.response_mailbox["failed"]["response"] == response.to_dict()
    assert any(content.type == "error" for message in response.messages for content in message.contents)
    assert provider.writes == 1
