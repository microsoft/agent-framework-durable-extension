# Copyright (c) Microsoft. All rights reserved.

"""Inserted occurrences, explicit history sources and empty cursor snapshots."""

from copy import deepcopy
from typing import Any

import pytest
from agent_framework import Agent, CompactionProvider, InMemoryHistoryProvider, Message
from test_shared_history_provider import _bound, _CanonicalStateProvider, _PassiveChatClient, _request, _stored

from agent_framework_durabletask._history_provider import (
    WORKING_BUFFER_KEY,
    DurableHistoryProvider,
    ensure_durable_history,
)
from agent_framework_durabletask._shared_agent_state import DurableAgentState


@pytest.mark.parametrize("remove_original", [False, True])
@pytest.mark.parametrize("remove_from_storage", [False, True])
async def test_new_colliding_message_is_distinct_from_loaded_occurrence(
    remove_original: bool, remove_from_storage: bool
) -> None:
    provider = _CanonicalStateProvider([_request("seed", _stored("original", message_id="same"))])
    history = DurableHistoryProvider()
    working: dict[str, Any] = {}
    with _bound(provider):
        await history.get_messages("session", state=working)
        if remove_from_storage:
            provider.state.data.conversation_history.clear()
        if remove_original:
            working[WORKING_BUFFER_KEY].clear()
        inserted = Message("assistant", ["inserted"], message_id="same")
        working[WORKING_BUFFER_KEY].append(inserted)
        history.flush(working)
        rows = [m for entry in provider.state.data.conversation_history for m in entry.messages]
        expected = (
            ["inserted"]
            if remove_from_storage
            else (["inserted", "original"] if remove_original else ["original", "inserted"])
        )
        assert [m.text for m in rows] == expected
        assert len({m.message_id for m in rows}) == len(rows)
        assert inserted.message_id == "same"
        assert next(m for m in rows if m.text == "inserted").public_message_id == "same"
        before = provider.state.to_dict()
        history.flush(working)
        assert provider.state.to_dict() == before
        cold = DurableAgentState.from_json(provider.state.to_json())
        assert cold.to_dict() == before


async def test_no_primary_uses_the_configured_compaction_source() -> None:
    observed: list[list[str]] = []

    async def strategy(messages: list[Message]) -> bool:
        observed.append([m.text for m in messages])
        return False

    compaction = CompactionProvider(after_strategy=strategy, history_source_id="custom-history")
    agent = Agent(client=_PassiveChatClient(), context_providers=[compaction])
    prepared = ensure_durable_history(agent)
    assert isinstance(prepared, Agent)
    history = next(p for p in prepared.context_providers if isinstance(p, DurableHistoryProvider))
    assert history.source_id == "custom-history"
    provider = _CanonicalStateProvider([_request("seed", _stored("prior", message_id="prior"))])
    session = prepared.create_session()
    with _bound(provider):
        await prepared.run("input", session=session)
        history.flush(session.state[history.source_id])
    assert observed == [["prior", "input", "answer-1"]]
    assert agent.context_providers == [compaction]


def test_custom_history_source_does_not_conflict_with_default_named_audit_sink() -> None:
    compaction = CompactionProvider(history_source_id="custom-history")
    audit = InMemoryHistoryProvider("in_memory", load_messages=False)
    agent = Agent(client=_PassiveChatClient(), context_providers=[compaction, audit])
    prepared = ensure_durable_history(agent)
    assert isinstance(prepared, Agent)
    assert audit in prepared.context_providers
    assert any(
        isinstance(p, DurableHistoryProvider) and p.source_id == "custom-history" for p in prepared.context_providers
    )


def test_ambiguous_injected_history_sources_reject_without_reconfiguring_the_agent() -> None:
    first = CompactionProvider(source_id="first", history_source_id="one")
    second = CompactionProvider(source_id="second", history_source_id="two")
    agent = Agent(client=_PassiveChatClient(), context_providers=[first, second])
    original = agent.context_providers
    with pytest.raises(ValueError, match="ambiguous.*history"):
        ensure_durable_history(agent)
    assert agent.context_providers is original


@pytest.mark.parametrize("initial", [None, {}, {"producer": 4}])
def test_explicit_empty_ingestion_positions_are_not_an_omission(initial: Any) -> None:
    raw: dict[str, Any] = {
        "schemaVersion": "2.0.0",
        "data": {"conversationHistory": [], "terminalResults": {}, "completionReceipts": {}},
    }
    if initial is not None:
        raw["data"]["ingestedPositions"] = deepcopy(initial)
    state = DurableAgentState.from_dict(raw)
    state.data.ingested_positions = {}
    encoded = state.to_dict()
    assert encoded["data"]["ingestedPositions"] == {}
    assert DurableAgentState.from_dict(encoded).to_dict() == encoded
    state.data.ingested_positions = None
    assert "ingestedPositions" not in state.to_dict()["data"]
