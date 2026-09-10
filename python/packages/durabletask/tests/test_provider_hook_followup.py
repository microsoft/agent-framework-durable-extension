# Copyright (c) Microsoft. All rights reserved.

"""Core hook ordering, custom history preservation and list-reducing compaction."""

import json
from copy import deepcopy
from itertools import combinations
from typing import Any

import pytest
from agent_framework import Agent, AgentSession, CompactionProvider, Content, InMemoryHistoryProvider, Message
from test_durable_history_provider import _InMemoryStateProvider
from test_history_pipeline_revision import OLD, ToolChatClient, bound, ids, seed, stored, transcript

from agent_framework_durabletask import AgentEntity, DurableHistoryProvider
from agent_framework_durabletask._durable_agent_state import (
    DurableAgentStateCompaction,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
    DurableAgentStateUnknownEntry,
)
from agent_framework_durabletask._history_provider import (
    WORKING_BUFFER_KEY,
    ensure_durable_history,
    prepare_history_owner,
    prune_messages,
)


class RecordingStrategy:
    def __init__(self) -> None:
        self.seen: list[list[tuple[str, str]]] = []

    async def __call__(self, messages: list[Message]) -> bool:
        self.seen.append([(message.role, message.text) for message in messages])
        return False


class CustomMemory(InMemoryHistoryProvider):
    after_run_once_per_turn = True

    def __init__(self, source_id: str = "in_memory") -> None:
        super().__init__(source_id)
        self.events: list[tuple[str, int]] = []
        self.resource = object()

    async def before_run(self, *, state: dict[str, Any], **kwargs: Any) -> None:
        self.events.append(("before", state.get("hook_runs", 0)))
        await super().before_run(state=state, **kwargs)

    async def after_run(self, *, state: dict[str, Any], **kwargs: Any) -> None:
        await super().after_run(state=state, **kwargs)
        state["hook_runs"] = state.get("hook_runs", 0) + 1
        state["custom"] = {"safe": [None, False, 3, {"nested": "kept"}]}
        self.events.append(("after", state["hook_runs"]))


class CustomDurable(DurableHistoryProvider):
    after_run_once_per_turn = True

    def __init__(self) -> None:
        super().__init__("custom-durable", store_context_from={"selected"})
        self.resource = object()
        self.events: list[str] = []

    async def after_run(self, **kwargs: Any) -> None:
        self.events.append("after")
        await super().after_run(**kwargs)


@pytest.mark.parametrize("per_call", [False, True])
async def test_implicit_history_matches_core_first_turn_after_strategy(per_call: bool) -> None:
    core_strategy = RecordingStrategy()
    core_compaction = CompactionProvider(after_strategy=core_strategy)
    core = Agent(
        client=ToolChatClient(tool_calls=False),
        name="assistant",
        context_providers=[core_compaction],
        require_per_service_call_history_persistence=per_call,
    )
    core_session = core.create_session()
    await core.run("first", session=core_session)
    assert core.context_providers[0] is core_compaction
    assert type(core.context_providers[-1]) is InMemoryHistoryProvider

    strategy = RecordingStrategy()
    compaction = CompactionProvider(after_strategy=strategy)
    original = Agent(
        client=ToolChatClient(tool_calls=False),
        name="assistant",
        context_providers=[compaction],
        require_per_service_call_history_persistence=per_call,
    )
    entity = AgentEntity(original, state_provider=_InMemoryStateProvider())
    await entity.run({"message": "first", "correlationId": "first"})
    prepared: Any = entity.agent
    assert prepared.context_providers[0] is compaction
    history = prepared.context_providers[-1]
    assert isinstance(history, DurableHistoryProvider)
    assert history.source_id == core.context_providers[-1].source_id == compaction.history_source_id == "in_memory"
    assert original.context_providers == [compaction]
    assert strategy.seen == core_strategy.seen == [[("user", "first"), ("assistant", "answer-1")]]


@pytest.mark.parametrize("history_first", [False, True])
async def test_explicit_history_registration_order_is_not_rewritten(history_first: bool) -> None:
    core_strategy = RecordingStrategy()
    core_history = InMemoryHistoryProvider("chosen")
    core_compaction = CompactionProvider(after_strategy=core_strategy, history_source_id="chosen")
    core_providers = [core_history, core_compaction] if history_first else [core_compaction, core_history]
    core = Agent(client=ToolChatClient(tool_calls=False), context_providers=core_providers)
    await core.run("first", session=core.create_session())

    strategy = RecordingStrategy()
    history = InMemoryHistoryProvider("chosen")
    compaction = CompactionProvider(after_strategy=strategy, history_source_id="chosen")
    providers = [history, compaction] if history_first else [compaction, history]
    original = Agent(client=ToolChatClient(tool_calls=False), context_providers=providers)
    entity = AgentEntity(original, state_provider=_InMemoryStateProvider())
    await entity.run({"message": "first", "correlationId": "first"})
    prepared: Any = entity.agent
    assert [provider.source_id for provider in prepared.context_providers] == [p.source_id for p in providers]
    assert original.context_providers == providers
    assert strategy.seen == core_strategy.seen
    assert bool(strategy.seen) is not history_first


@pytest.mark.parametrize("per_call", [False, True])
@pytest.mark.parametrize("source_id", ["in_memory", "custom-history"])
async def test_custom_memory_hooks_and_transcript_survive_entity_cold_reload(per_call: bool, source_id: str) -> None:
    core_history = CustomMemory(source_id)
    core = Agent(
        client=ToolChatClient(tool_calls=False),
        name="assistant",
        context_providers=[core_history],
        require_per_service_call_history_persistence=per_call,
    )
    session = core.create_session()
    history = CustomMemory(source_id)
    client = ToolChatClient(tool_calls=False)
    provider = _InMemoryStateProvider()
    for turn in (1, 2):
        original = Agent(
            client=client,
            name="assistant",
            context_providers=[history],
            require_per_service_call_history_persistence=per_call,
        )
        assert ensure_durable_history(original, prune_excluded=True) is original
        entity = AgentEntity(original, state_provider=provider, retention="follow_compaction")
        prepared: Any = entity.agent
        assert prepared.context_providers == [history]
        assert prepared.require_per_service_call_history_persistence is per_call
        await core.run(f"turn-{turn}", session=session, stream=True).get_final_response()
        await entity.run({"message": f"turn-{turn}", "correlationId": f"turn-{turn}"})

        raw = provider._get_state_dict()
        restored = AgentSession.from_dict(raw["data"]["session"])
        assert restored.to_dict()["state"][source_id] == session.to_dict()["state"][source_id]
        assert restored.state[source_id]["hook_runs"] == turn
        assert all(isinstance(message, Message) for message in restored.state[source_id]["messages"])
        assert provider.state.data.conversation_history == []
        assert (
            history.events
            == core_history.events
            == [event for index in range(1, turn + 1) for event in (("before", index - 1), ("after", index))]
        )
        session = AgentSession.from_dict(json.loads(json.dumps(session.to_dict())))
        provider = _InMemoryStateProvider(raw=raw)
    assert [message.text for message in client.received_messages[-1]] == ["turn-1", "answer-1", "turn-2"]
    assert client.received_messages[-1][0].additional_properties["_attribution"]["source_type"] == "CustomMemory"


@pytest.mark.parametrize("factory", [InMemoryHistoryProvider, CustomMemory, CustomDurable])
@pytest.mark.parametrize("once_per_turn", [False, True])
def test_substitution_preserves_once_per_turn_metadata(factory: Any, once_per_turn: bool) -> None:
    original = factory()
    original.after_run_once_per_turn = once_per_turn
    agent = Agent(client=ToolChatClient(tool_calls=False), context_providers=[original])
    prepared: Any = ensure_durable_history(agent, prune_excluded=True)
    replacement = prepared.context_providers[0]
    assert replacement.after_run_once_per_turn is once_per_turn
    assert agent.context_providers == [original]
    if type(original) is InMemoryHistoryProvider:
        assert type(replacement) is DurableHistoryProvider
    elif isinstance(original, CustomMemory):
        assert prepared is agent and replacement is original
    else:
        assert type(replacement) is CustomDurable and replacement is not original
        assert replacement.resource is original.resource and replacement.events is original.events
        assert replacement.store_context_from == original.store_context_from
        assert replacement.store_context_from is not original.store_context_from
        assert replacement.prune_excluded is True and original.prune_excluded is None


@pytest.mark.parametrize("factory", [InMemoryHistoryProvider, CustomMemory, CustomDurable])
@pytest.mark.parametrize("once_per_turn", [False, True])
async def test_preserved_metadata_controls_real_core_loop_iteration_hooks(factory: Any, once_per_turn: bool) -> None:
    supports_once_per_turn = hasattr(InMemoryHistoryProvider(), "after_run_once_per_turn")
    original = factory()
    original.after_run_once_per_turn = once_per_turn
    prepared: Any = ensure_durable_history(Agent(client=ToolChatClient(tool_calls=False), context_providers=[original]))
    provider = _InMemoryStateProvider()
    session = prepared.create_session()
    history = prepared.context_providers[0]
    with bound(provider):
        await prepared.run("iteration", session=session, options={"_agent_loop_iteration": "turn"})
        if isinstance(history, DurableHistoryProvider):
            history.flush(session.state[history.source_id])
            saved = [m.to_chat_message().text for m in transcript(provider)]
        else:
            saved = [m.text for m in session.state[history.source_id].get("messages", [])]
        deferred = once_per_turn and supports_once_per_turn
        assert saved == ([] if deferred else ["iteration", "answer-1"])
        if isinstance(history, CustomDurable):
            assert history.events == ([] if deferred else ["after"])
        if isinstance(history, CustomMemory):
            assert history.events == ([("before", 0)] if deferred else [("before", 0), ("after", 1)])


@pytest.mark.parametrize("per_call", [False, True])
async def test_custom_memory_uses_existing_inactive_primary_service_adapter(per_call: bool) -> None:
    history = CustomMemory()
    sink = InMemoryHistoryProvider("audit", load_messages=False)
    client = ToolChatClient(tool_calls=False)
    agent = Agent(
        client=client,
        context_providers=[history, sink],
        require_per_service_call_history_persistence=per_call,
    )
    assert ensure_durable_history(agent) is agent
    session = agent.create_session()
    service_id = None
    for turn, service_owned in enumerate((True, False, True), start=1):
        # Ownership intentionally isolates branches, rather than mirroring Core's service-call saves.
        session.service_session_id = service_id if service_owned else None
        prepared: Any = prepare_history_owner(agent, service_owned)
        if service_owned:
            wrapper = prepared.context_providers[0]
            assert wrapper.__wrapped__ is history and wrapper.resource is history.resource
            assert wrapper.after_run_once_per_turn is True
            local: Any = prepare_history_owner(prepared, False)
            assert local.context_providers[0] is history
        else:
            assert prepared is agent
        assert prepared.context_providers[1] is sink
        await prepared.run(f"turn-{turn}", session=session, options={"store": service_owned})
        if service_owned:
            service_id = session.service_session_id
        session = AgentSession.from_dict(json.loads(json.dumps(session.to_dict())))
    assert history.events == [("before", 0), ("after", 1)]
    assert session.state[history.source_id]["hook_runs"] == 1
    assert [m.text for m in session.state[history.source_id]["messages"]] == ["turn-2", "answer-2"]
    assert len(session.state["audit"]["messages"]) == 6
    assert [m.text for m in client.received_messages[2]] == ["turn-3"]
    assert agent.context_providers == [history, sink]


class SliceOldExchange:
    def __init__(self) -> None:
        self.current: list[tuple[str, str]] = []

    async def __call__(self, messages: list[Message]) -> bool:
        self.current = [(m.role, m.text) for m in messages[-2:]]
        start = next(index for index, message in enumerate(messages) if message.message_id == "seed-user")
        assert messages[start + 1].message_id == "seed-assistant"
        del messages[start : start + 2]
        return True


@pytest.mark.parametrize("retention", ["keep_all", "follow_compaction"])
async def test_after_strategy_list_removal_survives_cold_next_turn(retention: Any) -> None:
    provider = _InMemoryStateProvider()
    seed(provider)
    originals = transcript(provider)
    for message in originals:
        message.extension_data = {
            "future": {"safe": [None, False, 3, {"nested": "kept"}]},
            "_group": {"_summarized_by_summary_id": "kept-summary"},
        }
    summary = Message(
        "assistant",
        ["old summary"],
        message_id="kept-summary",
        additional_properties={"_group": {"_summary_of_message_ids": ["seed-user", "seed-assistant"]}},
    )
    unknown = DurableAgentStateUnknownEntry({"$type": "futureKind", "payload": {"keep": [None, False, {"x": 1}]}})
    provider.state.data.conversation_history.insert(0, unknown)
    provider.state.data.conversation_history.insert(
        1, DurableAgentStateRequest("system", OLD, [stored("system", "instructions", "system")])
    )
    provider.state.data.conversation_history.append(
        DurableAgentStateCompaction(OLD, [DurableAgentStateMessage.from_chat_message(summary)])
    )
    unknown_before = deepcopy(unknown.to_dict())
    source_metadata = deepcopy(originals[0].extension_data)
    assert source_metadata is not None
    core_strategy = SliceOldExchange()
    core_client = ToolChatClient(tool_calls=False)
    core = Agent(client=core_client, context_providers=[CompactionProvider(after_strategy=core_strategy)])
    session = core.create_session()
    session.state["in_memory"] = {"messages": [deepcopy(m).to_chat_message() for m in transcript(provider)]}
    strategy = SliceOldExchange()
    entity = AgentEntity(
        Agent(client=ToolChatClient(tool_calls=False), context_providers=[CompactionProvider(after_strategy=strategy)]),
        state_provider=provider,
        retention=retention,
    )
    await core.run("current", session=session)
    response = await entity.run({"message": "current", "correlationId": "current"})
    assert response.text == "answer-1"
    assert strategy.current == core_strategy.current == [("user", "current"), ("assistant", "answer-1")]
    saved = {message.message_id: message for message in transcript(provider)}
    old_ids = {"seed-user", "seed-assistant"}
    if retention == "keep_all":
        assert old_ids <= saved.keys()
        assert all(saved[message_id].extension_data == {**source_metadata, "_excluded": True} for message_id in old_ids)
        assert provider.state.data.truncation is None
    else:
        assert not old_ids & saved.keys()
        assert (provider.state.data.truncation or {})["evictedMessageCount"] == 2
    assert "system" in saved
    assert [
        m.to_chat_message().text
        for entry in provider.state.data.conversation_history
        if entry.correlation_id == "current"
        for m in entry.messages
    ] == ["current", "answer-1"]
    assert (saved["kept-summary"].extension_data or {})["_group"]["_summary_of_message_ids"] == [
        "seed-user",
        "seed-assistant",
    ]
    assert unknown.to_dict() == unknown_before
    delivered = provider.state.try_get_agent_response("current")
    assert delivered is not None and delivered.to_dict() == response.to_dict()
    cold_provider = _InMemoryStateProvider(raw=provider._get_state_dict())
    cold_client = ToolChatClient(tool_calls=False)
    cold = AgentEntity(Agent(client=cold_client), state_provider=cold_provider, retention=retention)
    core.context_providers = [core.context_providers[-1]]
    await core.run("next", session=AgentSession.from_dict(json.loads(json.dumps(session.to_dict()))))
    await cold.run({"message": "next", "correlationId": "next"})
    core_input = [m.text for m in core_client.received_messages[-1]]
    assert (
        [m.text for m in cold_client.received_messages[0]]
        == core_input
        == [
            "instructions",
            "old summary",
            "current",
            "answer-1",
            "next",
        ]
    )
    assert cold_provider.state.data.conversation_history[0].to_dict() == unknown_before
    delivered = cold_provider.state.try_get_agent_response("current")
    assert delivered is not None and delivered.to_dict() == response.to_dict()


@pytest.mark.parametrize(
    "removed", [set(group) for size in range(3) for group in combinations(("call", "result"), size)]
)
async def test_list_removal_prunes_only_complete_atomic_groups_and_keeps_floor(removed: set[str]) -> None:
    provider = _InMemoryStateProvider()
    call = Message(
        "assistant", [Content.from_function_call(call_id="t", name="lookup", arguments="{}")], message_id="call"
    )
    result = Message("tool", [Content.from_function_result(call_id="t", result="value")], message_id="result")
    provider.state.data.conversation_history.extend([
        DurableAgentStateRequest("system", OLD, [stored("system", "instructions", "system")]),
        DurableAgentStateResponse("old", OLD, [DurableAgentStateMessage.from_chat_message(call)]),
        DurableAgentStateRequest("old", OLD, [DurableAgentStateMessage.from_chat_message(result)]),
        DurableAgentStateRequest("current", OLD, [stored("current-input", "current")]),
        DurableAgentStateResponse("current", OLD, [stored("current-answer", "answer", "assistant")]),
    ])
    history = DurableHistoryProvider(prune_excluded=True)
    state: dict[str, Any] = {}
    with bound(provider):
        await history.get_messages("session", state=state)
        # Even removing protected messages from the buffer cannot authorize their physical deletion.
        missing = removed | {"system", "current-input", "current-answer"}
        state[WORKING_BUFFER_KEY][:] = [m for m in state[WORKING_BUFFER_KEY] if m.message_id not in missing]
        history.flush(state)
        assert {"system", "current-input", "current-answer"} <= set(ids(provider))
        assert {"call", "result"} & set(ids(provider)) == (set() if len(removed) == 2 else {"call", "result"})
        assert (provider.state.data.truncation or {}).get("evictedMessageCount", 0) == (2 if len(removed) == 2 else 0)
        snapshot = deepcopy(provider.state.to_dict())
        history.flush(state)
        assert provider.state.to_dict() == snapshot


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("prune", [False, True])
async def test_removed_summary_revision_stays_excluded_without_rewriting_backlinks(nested: bool, prune: bool) -> None:
    def links(message: Message) -> dict[str, Any]:
        return message.additional_properties.setdefault("_group", {}) if nested else message.additional_properties

    provider = _InMemoryStateProvider()
    seed(provider)
    provider.state.data.conversation_history.append(
        DurableAgentStateRequest("current", OLD, [stored("current", "current")])
    )
    history = DurableHistoryProvider(prune_excluded=prune)
    state: dict[str, Any] = {}
    with bound(provider) as binding:
        await history.get_messages("session", state=state)
        buffer = state[WORKING_BUFFER_KEY]
        links(buffer[0])["_summarized_by_summary_id"] = "summary"
        first = Message(
            "assistant", ["first summary"], message_id="summary", additional_properties={"future": {"keep": [1]}}
        )
        links(first)["_summary_of_message_ids"] = ["seed-user"]
        buffer.insert(1, first)
        history.flush(state)
        buffer.remove(first)
        source = next(m for m in buffer if m.message_id == "seed-assistant")
        links(source)["_summarized_by_summary_id"] = "summary"
        second = Message("assistant", ["second summary"], message_id="summary")
        links(second)["_summary_of_message_ids"] = ["seed-assistant"]
        buffer.insert(buffer.index(source) + 1, second)
        history.flush(state)
        assert second.message_id != "summary"
        saved = {m.message_id: deepcopy(m).to_chat_message() for m in transcript(provider)}
        assert links(saved["seed-user"])["_summarized_by_summary_id"] == "summary"
        assert links(saved["seed-assistant"])["_summarized_by_summary_id"] == second.message_id
        assert links(saved[second.message_id])["_summary_of_message_ids"] == ["seed-assistant"]
        if prune:
            assert "summary" not in saved
        else:
            assert saved["summary"].additional_properties["_excluded"] is True
            assert saved["summary"].additional_properties["future"] == {"keep": [1]}
            assert links(saved["summary"])["_summary_of_message_ids"] == ["seed-user"]
        snapshot = deepcopy(provider.state.to_dict())
        ordinal = binding.append_ordinal
        history.flush(state)
        assert provider.state.to_dict() == snapshot and binding.append_ordinal == ordinal
    cold = _InMemoryStateProvider(raw=json.loads(provider.state.to_json()))
    with bound(cold):
        loaded = await history.get_messages("session", state={})
        assert "first summary" not in [m.text for m in loaded]
        assert [m.text for m in loaded].count("second summary") == 1


async def test_stale_summary_removed_from_storage_is_not_reinserted() -> None:
    provider = _InMemoryStateProvider()
    summary = DurableAgentStateCompaction(OLD, [stored("summary", "old summary", "assistant")])
    provider.state.data.conversation_history.extend([
        summary,
        DurableAgentStateRequest("current", OLD, [stored("current", "current")]),
    ])
    history = DurableHistoryProvider(prune_excluded=False)
    state: dict[str, Any] = {}
    with bound(provider) as binding:
        await history.get_messages("session", state=state)
        prune_messages(provider.state.data.conversation_history, [(summary, summary.messages[0])])
        history.flush(state)
        assert ids(provider) == ["current"] and binding.append_ordinal == 0
        assert [m.message_id for m in state[WORKING_BUFFER_KEY]] == ["current"]


@pytest.mark.parametrize("prune", [False, True])
async def test_unloaded_empty_payload_is_not_mistaken_for_a_strategy_removal(prune: bool) -> None:
    provider = _InMemoryStateProvider()
    empty = DurableAgentStateMessage("user", [], message_id="empty", extension_data={"future": {"keep": [1]}})
    provider.state.data.conversation_history.extend([
        DurableAgentStateRequest("old", OLD, [empty]),
        DurableAgentStateRequest("current", OLD, [stored("current", "current")]),
    ])
    history = DurableHistoryProvider(prune_excluded=prune)
    state: dict[str, Any] = {}
    with bound(provider):
        loaded = await history.get_messages("session", state=state)
        assert [m.message_id for m in loaded] == ["current"]
        snapshot = deepcopy(provider.state.to_dict())
        history.flush(state)
        assert provider.state.to_dict() == snapshot
