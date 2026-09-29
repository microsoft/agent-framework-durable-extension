# Copyright (c) Microsoft. All rights reserved.

"""Private pipeline tests for the shared durable history provider."""

from __future__ import annotations

from collections.abc import Sequence
from copy import deepcopy
from datetime import datetime
from typing import Any

import pytest
from _history_pipeline_test_support import (
    OLD,
    AddContext,
    CountingHistory,
    ToolChatClient,
    _bound,
    _CanonicalStateProvider,
    _request,
    _stored,
    lookup,
)
from agent_framework import (
    Agent,
    AgentResponse,
    CompactionProvider,
    Content,
    Message,
    SessionContext,
    SummarizationStrategy,
    annotate_message_groups,
)

from agent_framework_durabletask._history_provider import (
    POSITIONS_KEY,
    WORKING_BUFFER_KEY,
    DurableHistoryProvider,
    current_durable_history_binding,
    ensure_durable_history,
)
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._shared_agent_state import (
    DurableAgentStateEntryJsonType,
    DurableAgentStateFunctionResultContent,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
    DurableAgentStateUsage,
)

PROMPT = "Use lookup for durable."


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


def _response(correlation_id: str, *messages: DurableAgentStateMessage) -> DurableAgentStateResponse:
    return DurableAgentStateResponse(correlation_id, OLD, list(messages))


def _seed(provider: _CanonicalStateProvider) -> None:
    provider.state.data.conversation_history.extend([
        _request("seed", _stored("seed question", message_id="seed-user")),
        _response("seed", _stored("seed answer", role="assistant", message_id="seed-assistant")),
    ])


def _transcript(provider: _CanonicalStateProvider) -> list[DurableAgentStateMessage]:
    return [message for entry in provider.state.data.conversation_history for message in entry.messages]


def _ids(provider: _CanonicalStateProvider) -> list[str | None]:
    return [message.message_id for message in _transcript(provider)]


def _history_texts(provider: _CanonicalStateProvider) -> list[str]:
    return [message.text for message in _transcript(provider)]


def _make_compaction_session(history: DurableHistoryProvider, state: dict[str, Any]) -> Any:
    session = Agent(client=ToolChatClient(tool_calls=False)).create_session(session_id="compaction-session")
    session.state[history.source_id] = state
    return session


def _tool_result_count(provider: _CanonicalStateProvider) -> int:
    return sum(
        1
        for message in _transcript(provider)
        for content in message.contents
        if isinstance(content, DurableAgentStateFunctionResultContent)
    )


def _assert_current_positions(provider: _CanonicalStateProvider, state: dict[str, Any]) -> None:
    history = provider.state.data.conversation_history
    positions = state[POSITIONS_KEY]
    for message in state[WORKING_BUFFER_KEY]:
        history_id = getattr(message, "_durable_history_id", None) or message.message_id
        entry, index = positions[history_id]
        assert any(candidate is entry for candidate in history)
        assert entry.messages[index].message_id == history_id
    assert len(positions) == len({message.message_id for message in _transcript(provider)})


def _assert_tool_follow_up(client: ToolChatClient) -> None:
    assert len(client.received_messages) == 2
    second = client.received_messages[1]
    assert [message.text for message in second].count(PROMPT) == 1
    calls = [content for message in second for content in message.contents if content.type == "function_call"]
    results = [content for message in second for content in message.contents if content.type == "function_result"]
    assert len(calls) == len(results) == 1
    assert calls[0].call_id == results[0].call_id == "call-1"
    assert results[0].result == "value:durable"
    assert not client.received_options[1].get("conversation_id")


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
@pytest.mark.parametrize("stream", [False, True], ids=["non-streaming", "streaming"])
async def test_real_core_tool_loop_and_final_flush(per_call: bool, stream: bool) -> None:
    events: list[str] = []
    history = CountingHistory(events)
    strategy = SummarizeSeed(events)
    provider = _CanonicalStateProvider()
    _seed(provider)
    client = ToolChatClient(events=events)
    agent = Agent(
        client=client,
        name="tool-agent",
        tools=[lookup],
        context_providers=[history, CompactionProvider(after_strategy=strategy, history_source_id=history.source_id)],
        require_per_service_call_history_persistence=per_call,
    )
    session = agent.create_session(session_id="pipeline-session")
    with _bound(provider) as binding:
        if stream:
            response = await agent.run(PROMPT, session=session, stream=True).get_final_response()
        else:
            response = await agent.run(PROMPT, session=session)

        _assert_tool_follow_up(client)
        assert response.text == "answer-2"
        assert history.before_calls == history.after_calls == (2 if per_call else 1)
        assert binding.pending_inputs == []
        assert strategy.calls == 1
        assert events[-1] == ("compaction" if per_call else "history-after")
        state = session.state[history.source_id]
        history.flush(state)
        assert current_durable_history_binding() is binding
        assert _history_texts(provider) == [
            "seed question",
            "seed summary",
            "seed answer",
            PROMPT,
            "",
            "",
            "answer-2",
        ]
        assert all(_ids(provider)) and len(_ids(provider)) == len(set(_ids(provider)))
        assert (_transcript(provider)[0].extension_data or {})["after_hook"] == {"tags": ["kept"]}
        current = [entry for entry in provider.state.data.conversation_history if entry.correlation_id == "current"]
        assert len(current) == (4 if per_call else 2)
        assert [entry.json_type for entry in current] == (
            [DurableAgentStateEntryJsonType.REQUEST, DurableAgentStateEntryJsonType.RESPONSE] * (2 if per_call else 1)
        )
        _assert_current_positions(provider, state)
        snapshot = deepcopy(provider.state.to_dict())
        ordinal = binding.append_ordinal
        history.finalize_failed_run(state)
        history.flush(state)
        history.flush(state)
        assert provider.state.to_dict() == snapshot
        assert binding.append_ordinal == ordinal
        assert strategy.calls == 1 and len(client.received_messages) == 2
        assert provider.persist_count == 0


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
    from agent_framework import InMemoryHistoryProvider

    original = (
        InMemoryHistoryProvider(
            "custom-history",
            store_inputs=store_inputs,
            store_outputs=store_outputs,
            store_context_messages=store_context_messages,
            store_context_from=store_context_from,
            skip_excluded=False,
        )
        if provider_kind == "in-memory"
        else DurableHistoryProvider(
            "custom-history",
            store_inputs=store_inputs,
            store_outputs=store_outputs,
            store_context_messages=store_context_messages,
            store_context_from=store_context_from,
            skip_excluded=False,
        )
    )
    client = ToolChatClient(tool_calls=False)
    agent = Agent(
        client=client,
        context_providers=[original, AddContext("selected"), AddContext("other")],
        require_per_service_call_history_persistence=per_call,
    )
    prepared = ensure_durable_history(agent)
    assert isinstance(prepared, Agent)
    history = prepared.context_providers[0]
    assert isinstance(history, DurableHistoryProvider)
    if provider_kind == "in-memory":
        assert history is not original and agent.context_providers[0] is original
    else:
        assert history is original
    assert history.source_id == "custom-history"
    assert history.skip_excluded is False
    assert history.store_inputs is store_inputs and history.store_outputs is store_outputs
    assert history.store_context_messages is store_context_messages
    assert history.store_context_from == store_context_from
    if store_context_from is not None:
        assert history.store_context_from is not original.store_context_from or history is original
    provider = _CanonicalStateProvider()
    session = prepared.create_session()
    with _bound(provider):
        await prepared.run("input", session=session)
        history.flush(session.state[history.source_id])
    expected_context = [
        f"context-{source}"
        for source in ("selected", "other")
        if store_context_messages and (store_context_from is None or source in store_context_from)
    ]
    expected_inputs = [*expected_context, *(["input"] if store_inputs else [])]
    expected_outputs = ["answer-1"] if store_outputs else []
    assert _history_texts(provider) == expected_inputs + expected_outputs
    entries = provider.state.data.conversation_history
    assert [entry.json_type for entry in entries] == (
        ([DurableAgentStateEntryJsonType.REQUEST] if expected_inputs else [])
        + ([DurableAgentStateEntryJsonType.RESPONSE] if expected_outputs else [])
    )
    assert [message.text for message in client.received_messages[0]] == ["context-selected", "context-other", "input"]
    assert provider.persist_count == 0


@pytest.mark.parametrize("per_call", [False, True])
async def test_service_ownership_suppresses_only_durable_history(per_call: bool) -> None:
    from agent_framework import HistoryProvider

    class CaptureHistory(HistoryProvider):
        def __init__(self) -> None:
            super().__init__("audit", load_messages=False)
            self.saved: list[list[Message]] = []

        async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
            return []

        async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
            self.saved.append(deepcopy(list(messages)))

    provider = _CanonicalStateProvider()
    _seed(provider)
    before = deepcopy(provider.state.to_dict())
    history = DurableHistoryProvider(skip_excluded=True)
    sink = CaptureHistory()
    client = ToolChatClient(tool_calls=False)
    agent = Agent(
        client=client,
        default_options={"store": True},
        context_providers=[history, sink],
        require_per_service_call_history_persistence=per_call,
    )
    session = agent.create_session()
    with _bound(provider, service_owns_history=True) as binding:
        await agent.run("service input", session=session)
        assert await history.get_messages(session.session_id) == []
        await history.save_messages(session.session_id, [Message("user", ["do not store"])])
        history.flush({WORKING_BUFFER_KEY: [Message("assistant", ["do not insert"])]})
        assert binding.append_ordinal == 0
    assert provider.state.to_dict() == before and provider.persist_count == 0
    assert [message.text for message in client.received_messages[0]] == ["service input"]
    assert [[message.text for message in batch] for batch in sink.saved] == [["service input", "answer-1"]]


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
@pytest.mark.parametrize("store_inputs", [False, True])
@pytest.mark.parametrize("store_outputs", [False, True])
@pytest.mark.parametrize("stream", [False, True], ids=["non-streaming", "streaming"])
async def test_failure_finalization_respects_real_core_cadence_and_store_flags(
    per_call: bool, store_inputs: bool, store_outputs: bool, stream: bool
) -> None:
    provider = _CanonicalStateProvider()
    history = DurableHistoryProvider(store_inputs=store_inputs, store_outputs=store_outputs, skip_excluded=False)
    client = ToolChatClient(fail_on_call=2)
    agent = Agent(
        client=client,
        tools=[lookup],
        context_providers=[history],
        require_per_service_call_history_persistence=per_call,
    )
    session = agent.create_session()
    with _bound(provider) as binding:
        with pytest.raises(RuntimeError, match="model failed"):
            if stream:
                await agent.run(PROMPT, session=session, stream=True).get_final_response()
            else:
                await agent.run(PROMPT, session=session)
        state = session.state[history.source_id]
        before = deepcopy(provider.state.to_dict())
        assert bool(binding.pending_inputs) is (per_call and store_inputs)
        expected = ([PROMPT] if per_call and store_inputs else []) + ([""] if per_call and store_outputs else [])
        assert _history_texts(provider) == expected
        history.finalize_failed_run(state)
        history.flush(state)
        assert binding.pending_inputs == []
        expected_result = per_call and store_inputs and store_outputs
        assert _history_texts(provider) == expected + ([""] if expected_result else [])
        results = [
            content
            for message in _transcript(provider)
            for content in message.to_chat_message().contents
            if content.type == "function_result"
        ]
        assert len(results) == int(expected_result)
        if expected_result:
            assert results[0].result == "value:durable"
            _assert_tool_follow_up(client)
        else:
            assert provider.state.to_dict() == before
        snapshot = deepcopy(provider.state.to_dict())
        ordinal = binding.append_ordinal
        history.finalize_failed_run(state)
        history.flush(state)
        assert provider.state.to_dict() == snapshot and binding.append_ordinal == ordinal
    assert len(client.received_messages) == 2
    assert provider.persist_count == 0


@pytest.mark.parametrize("with_state", [False, True])
async def test_generic_save_appends_anonymous_messages_with_stable_write_time_ids(with_state: bool) -> None:
    provider = _CanonicalStateProvider()
    history = DurableHistoryProvider()
    state: dict[str, Any] | None = {} if with_state else None
    messages = [Message("user", ["repeat"]), Message("user", ["repeat"])]
    with _bound(provider, "append") as binding:
        await history.save_messages("session", messages, state=state)
        await history.save_messages("session", messages[:1], state=state)
        await history.save_messages("session", [], state=state)
        assert binding.append_ordinal == 2
        assert _ids(provider) == [
            "durable_request_append_0_0",
            "durable_request_append_0_1",
            "durable_request_append_1_0",
        ]
        assert [message.message_id for message in messages] == [None, None]
        assert _history_texts(provider) == ["repeat"] * 3
        rows = [message.to_dict() for message in _transcript(provider)]
        assert all("messageId" not in row for row in rows)
        assert [row["pythonHistoryId"] for row in rows] == _ids(provider)
        assert len(set(_ids(provider))) == 3
        if state is not None:
            assert [message.message_id for message in state[WORKING_BUFFER_KEY]] == [None] * 3
            assert [getattr(message, "_durable_history_id", None) for message in state[WORKING_BUFFER_KEY]] == _ids(
                provider
            )
            _assert_current_positions(provider, state)
    assert provider.persist_count == 0
    cold = provider.clone()
    with _bound(cold, "append"):
        loaded = await history.get_messages("session")
    assert [message.message_id for message in loaded] == [None] * 3
    assert [getattr(message, "_durable_history_id", None) for message in loaded] == _ids(provider)
    assert _ids(cold) == _ids(provider)
    assert [message.text for message in loaded] == ["repeat"] * 3


async def test_reused_ids_get_internal_revisions_without_changing_external_ids() -> None:
    provider = _CanonicalStateProvider()
    history = DurableHistoryProvider(skip_excluded=False)
    client = ToolChatClient(tool_calls=False, response_message_id="shared")
    agent = Agent(client=client, context_providers=[history])
    session = agent.create_session()
    inputs = [
        Message("user", ["version one"], message_id="shared"),
        Message("user", ["version two"], message_id="shared"),
    ]
    responses: list[AgentResponse] = []
    for index, message in enumerate(inputs):
        with _bound(provider, f"revision-{index}"):
            responses.append(await agent.run(message, session=session))
            history.flush(session.state[history.source_id])
    assert [message.message_id for message in inputs] == ["shared", "shared"]
    assert [response.messages[0].message_id for response in responses] == ["shared", "shared"]
    assert len(_ids(provider)) == len(set(_ids(provider))) == 4
    assert _ids(provider)[0] == "shared"
    assert all(message_id and message_id.startswith("durable_revision_") for message_id in _ids(provider)[1:])
    assert _history_texts(provider) == ["version one", "answer-1", "version two", "answer-2"]
    rows = [message.to_dict() for message in _transcript(provider)]
    assert [row["messageId"] for row in rows] == ["shared"] * 4
    assert "pythonHistoryId" not in rows[0] and "pythonHistoryIdentity" not in rows[0]
    assert [row["pythonHistoryId"] for row in rows[1:]] == _ids(provider)[1:]
    assert all(
        row["pythonHistoryIdentity"] == {"profile": "agent-framework-python.history-identity", "version": 1}
        and type(row["pythonHistoryIdentity"]["version"]) is int
        for row in rows[1:]
    )
    before = deepcopy(provider.state.to_dict())
    inputs[0].contents[0].text = "caller changed input"
    responses[-1].messages[0].additional_properties["model_metadata"]["tags"].append("caller changed output")
    assert provider.state.to_dict() == before
    cold = provider.clone()
    state: dict[str, Any] = {}
    with _bound(cold):
        loaded = await history.get_messages("session", state=state)
        loaded[2].additional_properties["revision_marker"] = True
        history.flush(state)
        marked = [
            message.text for message in _transcript(cold) if (message.extension_data or {}).get("revision_marker")
        ]
        assert marked == ["version two"]
        _assert_current_positions(cold, state)
    assert [message.message_id for message in loaded] == ["shared"] * 4
    assert [getattr(message, "_durable_history_id", None) for message in loaded] == _ids(provider)
    assert [message.text for message in loaded] == ["version one", "answer-1", "version two", "answer-2"]


async def test_generated_ids_reserve_supplied_ids_in_the_same_batch() -> None:
    provider = _CanonicalStateProvider()
    history = DurableHistoryProvider()
    reserved_id = "durable_request_current_0_0"
    messages = [Message("user", ["anonymous"]), Message("user", ["supplied"], message_id=reserved_id)]
    with _bound(provider):
        await history.save_messages("session", messages)
    assert _ids(provider) == [f"{reserved_id}_1", reserved_id]
    assert [message.message_id for message in messages] == [None, reserved_id]


async def test_append_and_flush_never_alias_tool_payloads_or_response_annotations() -> None:
    provider = _CanonicalStateProvider()
    history = DurableHistoryProvider(skip_excluded=False)
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
    with _bound(provider):
        await history.after_run(agent=None, session=None, context=context, state=state)
        assert _ids(provider) == ["durable_request_current_0_0", "durable_response_current_1_0"]
        assert context.input_messages[0].message_id is None and response.messages[0].message_id is None
        working = state[WORKING_BUFFER_KEY][-1]
        working.additional_properties["trace"]["tags"].append("compaction")
        working.contents[0].result["values"].append(2)
        assert _transcript(provider)[-1].to_chat_message().contents[0].result == {"values": [1]}
        assert (_transcript(provider)[-1].extension_data or {})["trace"] == {"tags": ["original"]}
        history.flush(state)
        assert (_transcript(provider)[-1].extension_data or {})["trace"] == {"tags": ["original", "compaction"]}
        assert _transcript(provider)[-1].to_chat_message().contents[0].result == {"values": [1]}
        assert response.to_dict() == before
        working.additional_properties["trace"]["tags"].append("not flushed")
        assert (_transcript(provider)[-1].extension_data or {})["trace"] == {"tags": ["original", "compaction"]}
    assert provider.persist_count == 0


async def test_repeated_core_summaries_preserve_excluded_storage_and_cold_reload() -> None:
    provider = _CanonicalStateProvider()
    _seed(provider)
    history = DurableHistoryProvider(skip_excluded=True)
    summary_client = ToolChatClient(tool_calls=False)
    compaction = CompactionProvider(
        after_strategy=SummarizationStrategy(
            client=summary_client, target_count=2, threshold=0, max_summary_input_tokens=None
        ),
        history_source_id=history.source_id,
    )
    state: dict[str, Any] = {}
    session = _make_compaction_session(history, state)
    generated_ids: list[str | None] = []
    expected_texts: list[str] = []
    expected_full_storage_texts: list[str] = []
    for turn in range(3):
        with _bound(provider, f"turn-{turn}") as binding:
            await history.get_messages("session", state=state)
            await history.save_messages(
                "session",
                [
                    Message("user", [f"question-{turn}"], message_id=f"user-{turn}"),
                    Message("assistant", [f"response-{turn}"], message_id=f"assistant-{turn}"),
                ],
                state=state,
            )
            await compaction.after_run(agent=None, session=session, context=None, state={})
            buffer = state[WORKING_BUFFER_KEY]
            summary = next(message for message in buffer if message.text == f"answer-{turn + 1}")
            assert summary.text == f"answer-{turn + 1}"
            generated_id = summary.message_id
            generated_ids.append(generated_id)
            original_links = deepcopy(summary.additional_properties["_group"])
            originals = [
                message for message in buffer[1:] if message.message_id in original_links["_summary_of_message_ids"]
            ]
            expected_texts = [message.text for message in buffer if not message.additional_properties.get("_excluded")]
            expected_full_storage_texts = [message.text for message in buffer]
            assert originals
            history.flush(state)
            assert [message.text for message in _transcript(provider)] == expected_full_storage_texts
            assert len(_ids(provider)) == len(set(_ids(provider))) == len(expected_full_storage_texts)
            included = [m.text for m in _transcript(provider) if not (m.extension_data or {}).get("_excluded")]
            assert included == expected_texts
            assert {"seed question", "seed answer"} <= set(expected_full_storage_texts)
            assert (
                summary.additional_properties["_group"]["_summary_of_message_ids"]
                == original_links["_summary_of_message_ids"]
            )
            assert (
                summary.additional_properties["_group"]["_summary_of_group_ids"]
                == original_links["_summary_of_group_ids"]
            )
            assert summary.additional_properties["_group"]["id"] == f"group_{summary.message_id}"
            assert all(
                message.additional_properties["_group"]["_summarized_by_summary_id"] == summary.message_id
                for message in originals
            )
            assert all(
                (message.extension_data or {})["_group"]["_summarized_by_summary_id"] == summary.message_id
                for message in _transcript(provider)
                if message.message_id in original_links["_summary_of_message_ids"]
            )
            _assert_current_positions(provider, state)
            snapshot = deepcopy(provider.state.to_dict())
            ordinal = binding.append_ordinal
            history.flush(state)
            assert provider.state.to_dict() == snapshot and binding.append_ordinal == ordinal
    assert len(generated_ids) == 3 and all(generated_ids)
    assert len(summary_client.received_messages) == 3 and provider.persist_count == 0

    cold = provider.clone()
    cold_history = DurableHistoryProvider(skip_excluded=False)
    cold_state: dict[str, Any] = {}
    with _bound(cold, "cold"):
        loaded = await cold_history.get_messages("session", state=cold_state)
        assert next(message for message in loaded if message.text == "answer-3") in loaded
        assert [message.text for message in loaded] == expected_full_storage_texts
        assert [message.message_id for message in loaded] == _ids(provider)
        assert loaded[0].additional_properties == _transcript(provider)[0].extension_data
        replay = [message for message in loaded if not message.additional_properties.get("_excluded")]
        assert [message.text for message in replay] == expected_texts
        cold_history.flush(cold_state)
        assert cold.state.to_dict() == provider.state.to_dict()
        _assert_current_positions(cold, cold_state)
    assert cold.persist_count == 0


@pytest.mark.parametrize("nested_links", [False, True], ids=["top-level", "core-group"])
@pytest.mark.parametrize("remove_old_summary", [False, True], ids=["old-in-buffer", "old-removed"])
async def test_reused_summary_ids_keep_older_links_and_original_contents(
    nested_links: bool, remove_old_summary: bool
) -> None:
    provider = _CanonicalStateProvider()
    _seed(provider)
    provider.state.data.conversation_history.append(_request("current", _stored("current", message_id="current")))
    history = DurableHistoryProvider(skip_excluded=False)
    state: dict[str, Any] = {}
    session = _make_compaction_session(history, state)
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
    with _bound(provider) as binding:
        await history.get_messages("session", state=state)
        await compaction.after_run(agent=None, session=session, context=None, state={})
        history.flush(state)
        await compaction.after_run(agent=None, session=session, context=None, state={})
        new_summary = next(
            message
            for message in state[WORKING_BUFFER_KEY]
            if message.text == "summary version 2" and message.additional_properties.get("_group")
        )
        assert new_summary.message_id == "repeated-summary"
        history.flush(state)
        assert new_summary.message_id != "repeated-summary"
        assert _history_texts(provider) == [
            "seed question",
            "summary version 1",
            "seed answer",
            "summary version 2",
            "current",
        ]
        assert len(_ids(provider)) == len(set(_ids(provider))) == 5
        stored_messages = {message.message_id: message.to_chat_message() for message in _transcript(provider)}
        assert links(stored_messages["seed-user"])["_summarized_by_summary_id"] == "repeated-summary"
        assert links(stored_messages["seed-assistant"])["_summarized_by_summary_id"] == new_summary.message_id
        assert links(stored_messages["repeated-summary"])["_summary_of_message_ids"] == ["seed-user"]
        assert links(stored_messages[new_summary.message_id])["_summary_of_message_ids"] == ["seed-assistant"]
        _assert_current_positions(provider, state)
        snapshot = deepcopy(provider.state.to_dict())
        ordinal = binding.append_ordinal
        history.flush(state)
        assert provider.state.to_dict() == snapshot and binding.append_ordinal == ordinal
    assert calls == 2 and provider.persist_count == 0

    cold = provider.clone()
    cold_history = DurableHistoryProvider(skip_excluded=False)
    cold_state: dict[str, Any] = {}
    with _bound(cold):
        loaded = await cold_history.get_messages("session", state=cold_state)
        assert [message.text for message in loaded] == [
            "seed question",
            "summary version 1",
            "seed answer",
            "summary version 2",
            "current",
        ]
        replay = [message for message in loaded if not message.additional_properties.get("_excluded")]
        assert [message.text for message in replay] == [
            *([] if remove_old_summary else ["summary version 1"]),
            "summary version 2",
            "current",
        ]
        assert [message.message_id for message in replay] == [
            *([] if remove_old_summary else ["repeated-summary"]),
            new_summary.message_id,
            "current",
        ]
        cold_history.flush(cold_state)
        assert cold.state.to_dict() == provider.state.to_dict()
        _assert_current_positions(cold, cold_state)
    assert cold.persist_count == 0


@pytest.mark.parametrize("entry_type", [DurableAgentStateRequest, DurableAgentStateResponse])
@pytest.mark.parametrize("message_count", [2, 4])
@pytest.mark.parametrize("created_at", [OLD, None], ids=["timestamp", "no-timestamp"])
async def test_multiple_mid_entry_summaries_keep_exact_order_metadata_and_receipts(
    entry_type: type[DurableAgentStateRequest] | type[DurableAgentStateResponse],
    message_count: int,
    created_at: datetime | None,
) -> None:
    provider = _CanonicalStateProvider()
    source_ids = [f"item-{index}" for index in range(message_count)]
    owner = entry_type("old", created_at, [_stored(message_id, message_id=message_id) for message_id in source_ids])
    owner.extension_data = {"envelope": {"tags": ["original"]}}
    owner.unknown_fields = {"futureField": {"keep": [1]}}
    if isinstance(owner, DurableAgentStateRequest):
        owner.orchestration_id = "workflow"
        owner.response_schema = {"properties": {"value": {"type": "string"}}}
    else:
        owner.usage = DurableAgentStateUsage(input_token_count=7)
    barrier = DurableAgentStateRequest("barrier", OLD, [])
    barrier.unknown_fields = {"future": {"opaque": [1, 2]}}
    barrier_before = deepcopy(barrier.to_dict())
    current = DurableAgentStateRequest("current", OLD, [_stored("current", message_id="current")])
    provider.state.data.conversation_history.extend([barrier, owner, current])
    mailbox = deepcopy(provider.state.data.response_mailbox)
    receipts = deepcopy(provider.state.data.completed_correlations)
    history = DurableHistoryProvider(skip_excluded=False)
    state: dict[str, Any] = {}
    with _bound(provider):
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
        assert _ids(provider) == expected
        assert (_transcript(provider)[-2].extension_data or {})["target"] == "last original"
        _assert_current_positions(provider, state)
        envelopes = [
            entry
            for entry in provider.state.data.conversation_history
            if isinstance(entry, entry_type) and entry.correlation_id == "old"
        ]
        assert len(envelopes) == message_count
        for entry in envelopes:
            assert entry.correlation_id == "old" and entry.created_at == created_at
            if created_at is None:
                assert "createdAt" not in entry.to_dict()
            assert entry.extension_data == owner.extension_data and entry.unknown_fields == owner.unknown_fields
            assert all(
                message.message_id and not message.message_id.startswith("summary-") for message in entry.messages
            )
        assert envelopes[0].extension_data is not envelopes[-1].extension_data
        assert envelopes[0].unknown_fields is not envelopes[-1].unknown_fields
        if isinstance(owner, DurableAgentStateRequest):
            request_entries = [entry for entry in envelopes if isinstance(entry, DurableAgentStateRequest)]
            assert len(request_entries) == len(envelopes)
            assert all(entry.response_schema == owner.response_schema for entry in request_entries)
            assert all(entry.orchestration_id == "workflow" for entry in request_entries)
            assert request_entries[0].response_schema is not request_entries[-1].response_schema
        else:
            response_entries = [entry for entry in envelopes if isinstance(entry, DurableAgentStateResponse)]
            assert len(response_entries) == len(envelopes)
            assert owner.usage is not None
            expected_usage = owner.usage.to_dict()
            assert all(
                entry.usage is not None and entry.usage.to_dict() == expected_usage for entry in response_entries
            )
            assert response_entries[0].usage is not response_entries[-1].usage
        snapshot = deepcopy(provider.state.to_dict())
        history.flush(state)
        assert provider.state.to_dict() == snapshot
        assert barrier.to_dict() == barrier_before
    assert provider.state.data.response_mailbox == mailbox
    assert provider.state.data.completed_correlations == receipts

    cold = provider.clone()
    cold_history = DurableHistoryProvider(skip_excluded=True)
    cold_state: dict[str, Any] = {}
    with _bound(cold):
        loaded = await cold_history.get_messages("session", state=cold_state)
        assert [message.message_id for message in loaded] == expected
        next(message for message in loaded if message.message_id == "item-1").additional_properties["_excluded"] = True
        cold_state[WORKING_BUFFER_KEY].insert(1, Message("assistant", ["later"], message_id="later-summary"))
        loaded[0].additional_properties["target"] = "first original"
        cold_history.flush(cold_state)
        assert _ids(cold) == [expected[0], "later-summary", *expected[1:]]
        assert (_transcript(cold)[0].extension_data or {})["target"] == "first original"
        excluded = next(message for message in _transcript(cold) if message.message_id == "item-1")
        assert (excluded.extension_data or {})["_excluded"] is True
        _assert_current_positions(cold, cold_state)
        snapshot = deepcopy(cold.state.to_dict())
        cold_history.flush(cold_state)
        assert cold.state.to_dict() == snapshot
    assert cold.persist_count == 0


async def test_summary_metadata_round_trips_through_storage_projection() -> None:
    provider = _CanonicalStateProvider()
    _seed(provider)
    history = DurableHistoryProvider(skip_excluded=False)
    state: dict[str, Any] = {}
    with _bound(provider):
        loaded = await history.get_messages("session", state=state)
        summary = Message(
            "assistant",
            ["seed summary"],
            message_id="seed-summary",
            additional_properties={
                "_group": {
                    "id": "group_seed-summary",
                    "_summary_of_message_ids": ["seed-user"],
                    "_summary_of_group_ids": ["group_seed-user"],
                }
            },
        )
        loaded.insert(1, summary)
        loaded[0].additional_properties.setdefault("_group", {})["_summarized_by_summary_id"] = "seed-summary"
        state[WORKING_BUFFER_KEY] = loaded
        history.flush(state)
        stored_summary = next(message for message in _transcript(provider) if message.message_id == "seed-summary")
        stored_source = next(message for message in _transcript(provider) if message.message_id == "seed-user")
        assert (stored_summary.extension_data or {})["_group"]["_summary_of_message_ids"] == ["seed-user"]
        assert (stored_summary.extension_data or {})["_group"]["_summary_of_group_ids"] == ["group_seed-user"]
        assert (stored_source.extension_data or {})["_group"]["_summarized_by_summary_id"] == "seed-summary"
        _assert_current_positions(provider, state)
    cold = provider.clone()
    cold_state: dict[str, Any] = {}
    with _bound(cold):
        reloaded = await history.get_messages("session", state=cold_state)
        restored_summary = next(message for message in reloaded if message.message_id == "seed-summary")
        restored_source = next(message for message in reloaded if message.message_id == "seed-user")
        assert restored_summary.additional_properties["_group"]["_summary_of_message_ids"] == ["seed-user"]
        assert restored_source.additional_properties["_group"]["_summarized_by_summary_id"] == "seed-summary"
        history.flush(cold_state)
        assert cold.state.to_dict() == provider.state.to_dict()


async def test_failed_inputs_preserve_original_function_result_payloads_and_ingestion_identity() -> None:
    provider = _CanonicalStateProvider()
    history = DurableHistoryProvider(skip_excluded=False)
    agent = Agent(client=ToolChatClient(), require_per_service_call_history_persistence=True)
    session = agent.create_session()
    state: dict[str, Any] = {}
    session.state[history.source_id] = state
    with _bound(provider) as binding:
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
            additional_properties={"trace": {"tags": ["original"]}},
        )
        original = deepcopy(result.to_dict())
        inputs = [
            Message("user", ["unrelated fresh request"]),
            Message("tool", [Content.from_function_result("unknown", result="no stored call")]),
            result,
            deepcopy(result),
        ]
        before = deepcopy(provider.state.to_dict())
        await history.before_run(
            agent=agent, session=session, context=SessionContext(input_messages=inputs), state=state
        )
        assert provider.state.to_dict() == before
        assert binding.pending_inputs[-2] is not result
        assert binding.pending_inputs[-2].additional_properties["trace"] is not result.additional_properties["trace"]
        assert result.to_dict() == original
        result.contents[0].result["values"].append("caller mutation")
        result.additional_properties["trace"]["tags"].append("caller mutation")
        history.finalize_failed_run(state)
        assert binding.pending_inputs == []
        saved = _transcript(provider)[-1]
        assert saved.ingestion_identity == message_identity(Message.from_dict(original))
        assert [content.result for content in saved.to_chat_message().contents] == [
            {"values": ["call-1"]},
            {"values": ["call-2"]},
        ]
        assert saved.extension_data == {"trace": {"tags": ["original"]}}
        working = state[WORKING_BUFFER_KEY][-1]
        working.additional_properties["trace"]["tags"].append("compaction")
        working.contents[0].result["values"].append("working mutation")
        history.flush(state)
        assert saved.extension_data == {"trace": {"tags": ["original", "compaction"]}}
        assert saved.to_chat_message().contents[0].result == {"values": ["call-1"]}
        _assert_current_positions(provider, state)


@pytest.mark.parametrize("remove_entry", [False, True])
async def test_flush_tracks_replaced_owner_without_resurrecting_stale_buffer(remove_entry: bool) -> None:
    provider = _CanonicalStateProvider()
    owner = _request("old", _stored("a", message_id="a"), _stored("b", message_id="b"))
    other = _response("older", _stored("c", role="assistant", message_id="c"))
    provider.state.data.conversation_history.extend([owner, other])
    history = DurableHistoryProvider()
    state: dict[str, Any] = {}
    with _bound(provider):
        await history.get_messages("session", state=state)
        replacement = deepcopy(provider.state.data.conversation_history)
        if remove_entry:
            replacement.pop(0)
        else:
            replacement[0].messages.pop(0)
        provider.state.data.conversation_history = replacement
        state[WORKING_BUFFER_KEY][-1].additional_properties["current_owner"] = True
        history.flush(state)
        assert _ids(provider) == (["c"] if remove_entry else ["b", "c"])
        assert (_transcript(provider)[-1].extension_data or {})["current_owner"] is True
        assert not (other.messages[0].extension_data or {}).get("current_owner")
        _assert_current_positions(provider, state)
        snapshot = deepcopy(provider.state.to_dict())
        history.flush(state)
        assert provider.state.to_dict() == snapshot
