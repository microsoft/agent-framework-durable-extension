# Copyright (c) Microsoft. All rights reserved.

"""Review tests for skipped-entry aliasing and compaction insertion order."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest
from agent_framework import SUMMARY_OF_MESSAGE_IDS_KEY, Message
from test_shared_history_provider import OLD, _bound, _CanonicalStateProvider, _request, _stored

from agent_framework_durabletask._history_provider import POSITIONS_KEY, WORKING_BUFFER_KEY, DurableHistoryProvider
from agent_framework_durabletask._shared_agent_state import (
    DurableAgentState,
    DurableAgentStateCompaction,
    DurableAgentStateEntry,
    DurableAgentStateErrorResponse,
    DurableAgentStateMessage,
    DurableAgentStateTextReasoningContent,
    DurableAgentStateUnknownEntry,
)


def _unknown_entry(*messages: DurableAgentStateMessage) -> DurableAgentStateUnknownEntry:
    entry = DurableAgentStateUnknownEntry({
        "$type": "unknown",
        "correlationId": "seed",
        "messages": [message.to_dict() for message in messages],
    })
    entry.messages = list(messages)
    return entry


def _error_entry(*messages: DurableAgentStateMessage) -> DurableAgentStateErrorResponse:
    return DurableAgentStateErrorResponse("seed", OLD, list(messages))


def _reasoning_only_entry(text: str, *, message_id: str | None = None) -> DurableAgentStateEntry:
    return _request(
        "seed",
        DurableAgentStateMessage(
            "assistant",
            [DurableAgentStateTextReasoningContent(text)],
            message_id=message_id,
        ),
    )


def _summary(text: str, *, source_ids: list[str]) -> Message:
    return Message("assistant", [text], additional_properties={SUMMARY_OF_MESSAGE_IDS_KEY: source_ids})


def _history_markers(provider: _CanonicalStateProvider) -> list[str]:
    markers: list[str] = []
    for entry in provider.state.data.conversation_history:
        if isinstance(entry, DurableAgentStateCompaction):
            markers.append(entry.messages[0].text)
            continue
        if not entry.messages:
            markers.append(str(entry.json_type))
            continue
        first_content = entry.messages[0].contents[0]
        markers.append(getattr(first_content, "text", str(entry.json_type)))
    return markers


@pytest.mark.parametrize(
    "skipped_entry",
    [
        pytest.param(_error_entry, id="error-response"),
        pytest.param(_unknown_entry, id="unknown-entry"),
    ],
)
async def test_aliased_replayable_and_skipped_occurrences_reject_before_any_identity_repair(
    skipped_entry: Any,
) -> None:
    shared = DurableAgentStateMessage.from_chat_message(Message("assistant", ["shared object"]))
    provider = _CanonicalStateProvider([_request("seed", shared), skipped_entry(shared)])
    if skipped_entry is _unknown_entry:
        # Exercise the private model's opaque legacy-entry surface, not v2
        # admission of an unsupported entry discriminator or replay of it.
        provider.state.schema_version = "1.2.0"
    before = provider.state.to_dict()
    history = DurableHistoryProvider()
    state: dict[str, Any] = {}

    with _bound(provider), pytest.raises(ValueError, match="distinct stored message"):
        await history.get_messages("session", state=state)

    assert state == {}
    assert provider.state.to_dict() == before
    assert (
        provider.state.data.conversation_history[0].messages[0]
        is provider.state.data.conversation_history[1].messages[0]
    )


async def test_aliased_nonreplayable_occurrences_reject_without_mutating_storage() -> None:
    shared = DurableAgentStateMessage.from_chat_message(Message("assistant", ["hidden shared object"]))
    provider = _CanonicalStateProvider([_error_entry(shared), _unknown_entry(shared)])
    provider.state.schema_version = "1.2.0"
    before = provider.state.to_dict()
    history = DurableHistoryProvider()

    with _bound(provider), pytest.raises(ValueError, match="distinct stored message"):
        await history.get_messages("session", state={})
    assert provider.state.to_dict() == before
    assert (
        provider.state.data.conversation_history[0].messages[0]
        is provider.state.data.conversation_history[1].messages[0]
    )


async def test_distinct_duplicate_public_ids_still_allow_identity_repair_and_idempotent_flush() -> None:
    provider = _CanonicalStateProvider([
        _request(
            "seed",
            _stored("first", message_id="duplicate"),
            _stored("second", message_id="duplicate"),
        )
    ])
    history = DurableHistoryProvider()
    state: dict[str, Any] = {}

    with _bound(provider):
        loaded = await history.get_messages("session", state=state)
        assert [message.text for message in loaded] == ["first", "second"]
        assert [message.message_id for message in loaded] == ["duplicate", "duplicate"]
        positions = state[POSITIONS_KEY]
        assert isinstance(positions, dict)
        assert len(positions) == 2
        assert len(set(positions)) == 2
        before = deepcopy(provider.state.to_dict())
        history.flush(state)
        history.flush(state)

    assert provider.state.to_dict() == before
    assert DurableAgentState.from_json(provider.state.to_json()).to_dict() == before


@pytest.mark.parametrize(
    ("skipped_prefix", "expected_prefix"),
    [
        pytest.param(
            _error_entry(_stored("error-prefix", role="assistant", message_id="error-prefix")),
            "error-prefix",
            id="error",
        ),
        pytest.param(
            _reasoning_only_entry("reasoning-prefix", message_id="reasoning-prefix"),
            "reasoning-prefix",
            id="reasoning-only",
        ),
        pytest.param(
            _unknown_entry(_stored("unknown-prefix", role="assistant", message_id="unknown-prefix")),
            "unknown-prefix",
            id="unknown",
        ),
    ],
)
async def test_new_summary_prepended_in_buffer_stays_after_skipped_prefix_before_loaded_request(
    skipped_prefix: DurableAgentStateEntry,
    expected_prefix: str,
) -> None:
    provider = _CanonicalStateProvider([
        skipped_prefix,
        _request("seed", _stored("request", message_id="request")),
    ])
    history = DurableHistoryProvider()
    state: dict[str, Any] = {}

    with _bound(provider):
        loaded = await history.get_messages("session", state=state)
        assert [message.text for message in loaded] == ["request"]
        cast_buffer = state[WORKING_BUFFER_KEY]
        assert isinstance(cast_buffer, list)
        cast_buffer.insert(0, _summary("summary", source_ids=["request"]))
        history.flush(state)

    assert _history_markers(provider) == [expected_prefix, "summary", "request"]


async def test_new_summary_without_loaded_anchor_appends_after_existing_skipped_history() -> None:
    provider = _CanonicalStateProvider([
        _unknown_entry(_stored("old-unknown", role="assistant", message_id="old-unknown"))
    ])
    history = DurableHistoryProvider()
    state: dict[str, Any] = {}

    with _bound(provider):
        loaded = await history.get_messages("session", state=state)
        assert loaded == []
        cast_buffer = state[WORKING_BUFFER_KEY]
        assert isinstance(cast_buffer, list)
        cast_buffer.append(_summary("summary", source_ids=["old-unknown"]))
        history.flush(state)

    assert _history_markers(provider) == ["old-unknown", "summary"]


async def test_new_summary_in_empty_history_inserts_at_index_zero() -> None:
    provider = _CanonicalStateProvider()
    history = DurableHistoryProvider()
    state: dict[str, Any] = {}

    with _bound(provider):
        loaded = await history.get_messages("session", state=state)
        assert loaded == []
        cast_buffer = state[WORKING_BUFFER_KEY]
        assert isinstance(cast_buffer, list)
        cast_buffer.append(_summary("summary", source_ids=[]))
        history.flush(state)

    assert _history_markers(provider) == ["summary"]


async def test_new_summary_inserted_between_loaded_messages_keeps_loaded_predecessor_anchor() -> None:
    provider = _CanonicalStateProvider([
        _request("seed", _stored("first", message_id="first"), _stored("second", message_id="second"))
    ])
    history = DurableHistoryProvider()
    state: dict[str, Any] = {}

    with _bound(provider):
        loaded = await history.get_messages("session", state=state)
        assert [message.text for message in loaded] == ["first", "second"]
        cast_buffer = state[WORKING_BUFFER_KEY]
        assert isinstance(cast_buffer, list)
        cast_buffer.insert(1, _summary("summary", source_ids=["second"]))
        history.flush(state)

    assert _history_markers(provider) == ["first", "summary", "second"]
