# Copyright (c) Microsoft. All rights reserved.

"""Core projection failures must not commit a partial history identity repair."""

import json
from copy import deepcopy
from typing import Any

import pytest
from _shared_history_test_support import _bound, _CanonicalStateProvider
from agent_framework import SUMMARY_OF_MESSAGE_IDS_KEY, Message

from agent_framework_durabletask._history_provider import (
    POSITIONS_KEY,
    WORKING_BUFFER_KEY,
    DurableHistoryProvider,
)
from agent_framework_durabletask._shared_agent_state import DurableAgentState
from agent_framework_durabletask._shared_state_validation import validate_shared_state


def _raw_state(public_id: str | None, *, prefix_type: str, fields: Any) -> dict[str, Any]:
    messages: list[dict[str, Any]] = [
        {
            "role": "assistant",
            "messageId": "duplicate",
            "contents": [{"$type": prefix_type, "text": "existing identity"}],
        },
        {
            "role": "assistant",
            "contents": [{"$type": prefix_type, "text": "valid earlier message needing repair"}],
            "futureMessage": {"values": [None, False, 0, 0.0]},
        },
        {
            "role": "assistant",
            "contents": [
                {
                    "$type": "text",
                    "text": "later message",
                    "pythonCoreFields": {
                        "profile": "agent-framework-python.core-fields",
                        "version": 1,
                        "fields": deepcopy(fields),
                        "futureProfile": [None, False, 0, 0.0],
                    },
                    # Neither sibling is constrained to a string/object for text content.
                    "callId": {"opaque": [False, 0]},
                    "extensionData": [None, False, 0, 0.0],
                }
            ],
        },
    ]
    if public_id is not None:
        for message in messages[1:]:
            message["messageId"] = public_id
    return {
        "schemaVersion": "2.0.0",
        "futureRoot": {"values": [None, False, 0, 0.0]},
        "data": {
            "conversationHistory": [
                {
                    "$type": "request",
                    "correlationId": "seed",
                    "messages": messages,
                    "futureEntry": {"values": [None, False, 0, 0.0]},
                }
            ],
            "terminalResults": {},
            "completionReceipts": {},
            "session": {"session_id": "session", "state": {"other": {"keep": True}}},
            "futureData": {"values": [None, False, 0, 0.0]},
        },
    }


def _snapshot(value: Any) -> str:
    # Dict equality alone would conflate unknown-field values such as False and 0.
    return json.dumps(value, sort_keys=True, allow_nan=False)


@pytest.mark.parametrize("operation", ["get_messages", "flush"])
@pytest.mark.parametrize("public_id", [None, "duplicate"], ids=["missing-ids", "duplicate-ids"])
async def test_later_core_projection_failure_preserves_entire_state_and_history_references(
    operation: str, public_id: str | None
) -> None:
    # Flush searches for the first exposed message. Valid reasoning-only prefixes
    # make that real projection path reach the malformed later message as well.
    prefix_type = "reasoning" if operation == "flush" else "text"
    raw = _raw_state(public_id, prefix_type=prefix_type, fields=None)
    raw_before = _snapshot(raw)
    validate_shared_state(raw)
    owner = _CanonicalStateProvider()
    owner.state = DurableAgentState.from_dict(raw)
    canonical = owner.state
    data = canonical.data
    history = data.conversation_history
    entry = history[0]
    messages = entry.messages
    originals = tuple(messages)
    content_lists = tuple(message.contents for message in originals)
    original_contents = tuple(tuple(contents) for contents in content_lists)
    stored_ids = tuple(message.message_id for message in originals)
    public_ids = tuple(message.public_message_id for message in originals)
    captured_raw = canonical.to_dict()
    before = _snapshot(captured_raw)
    assert before == raw_before
    assert stored_ids == ("duplicate", public_id, public_id)
    assert public_ids == stored_ids
    assert all(message.to_chat_message().contents for message in originals[:-1])

    caller_state = {"keep": [None, False, 0, 0.0]}
    working: dict[str, Any] = {"caller": caller_state}
    buffer: list[Message] = []
    positions: dict[str, Any] = {}
    if operation == "flush":
        # Construct a pending compaction buffer directly, not by attempting a
        # get_messages call that would already fail and repair canonical IDs.
        # Reasoning-only prefixes have no exposed positions in this buffer.
        buffer.append(
            Message(
                "assistant",
                ["pending summary"],
                message_id="summary",
                additional_properties={SUMMARY_OF_MESSAGE_IDS_KEY: []},
            )
        )
        working[WORKING_BUFFER_KEY] = buffer
        working[POSITIONS_KEY] = positions
    working_keys = set(working)
    original_buffer = tuple(buffer)
    buffer_before = _snapshot([message.to_dict() for message in buffer])
    caller_before = _snapshot(caller_state)
    provider = DurableHistoryProvider()

    with _bound(owner), pytest.raises(ValueError, match="Python core-fields profile requires a fields object"):
        if operation == "get_messages":
            await provider.get_messages("session", state=working)
        else:
            provider.flush(working)

    assert tuple(message.message_id for message in originals) == stored_ids
    assert tuple(message.public_message_id for message in originals) == public_ids
    assert _snapshot(canonical.to_dict()) == before
    assert _snapshot(captured_raw) == before
    assert _snapshot(raw) == raw_before
    assert owner.state is canonical
    assert canonical.data is data
    assert data.conversation_history is history
    assert len(history) == 1 and history[0] is entry
    assert entry.messages is messages
    assert len(messages) == len(originals)
    for stored, original, contents, items in zip(messages, originals, content_lists, original_contents, strict=True):
        assert stored is original
        assert stored.contents is contents
        assert len(contents) == len(items)
        assert all(content is item for content, item in zip(contents, items, strict=True))
    assert owner.persist_count == 0
    assert set(working) == working_keys
    assert working["caller"] is caller_state
    assert _snapshot(caller_state) == caller_before
    if operation == "flush":
        assert working[WORKING_BUFFER_KEY] is buffer
        assert working[POSITIONS_KEY] is positions
        assert positions == {}
        assert len(buffer) == len(original_buffer)
        assert all(message is original for message, original in zip(buffer, original_buffer, strict=True))
        assert _snapshot([message.to_dict() for message in buffer]) == buffer_before


@pytest.mark.parametrize("public_id", [None, "duplicate"], ids=["missing-ids", "duplicate-ids"])
async def test_valid_core_profile_allows_identity_repair_and_idempotent_flush(public_id: str | None) -> None:
    raw = _raw_state(public_id, prefix_type="text", fields={})
    raw_before = _snapshot(raw)
    validate_shared_state(raw)
    owner = _CanonicalStateProvider()
    owner.state = DurableAgentState.from_dict(raw)
    history = owner.state.data.conversation_history
    entry = history[0]
    originals = tuple(entry.messages)
    provider = DurableHistoryProvider()
    working: dict[str, Any] = {}

    with _bound(owner):
        loaded = await provider.get_messages("session", state=working)
        assert [message.text for message in loaded] == [
            "existing identity",
            "valid earlier message needing repair",
            "later message",
        ]
        assert [message.message_id for message in loaded] == ["duplicate", public_id, public_id]
        stored_ids = [message.message_id for message in originals]
        assert all(isinstance(message_id, str) and message_id for message_id in stored_ids)
        assert len(set(stored_ids)) == 3
        assert stored_ids[0] == "duplicate"
        assert all(message_id != public_id for message_id in stored_ids[1:])
        assert set(working[POSITIONS_KEY]) == set(stored_ids)
        for index, stored_id in enumerate(stored_ids):
            position = working[POSITIONS_KEY][stored_id]
            assert position[0] is entry and position[1] == index

        repaired_raw = owner.state.to_dict()
        expected = deepcopy(raw)
        for message, stored_id in zip(expected["data"]["conversationHistory"][0]["messages"][1:], stored_ids[1:]):
            message["pythonHistoryId"] = stored_id
            message["pythonHistoryIdentity"] = {"profile": "agent-framework-python.history-identity", "version": 1}
        assert _snapshot(repaired_raw) == _snapshot(expected)
        repaired_before = _snapshot(repaired_raw)
        for _ in range(2):
            provider.flush(working)
            assert _snapshot(owner.state.to_dict()) == repaired_before

    assert _snapshot(raw) == raw_before
    assert _snapshot(repaired_raw) == repaired_before
    assert owner.state.data.conversation_history is history
    assert len(history) == 1 and history[0] is entry
    assert len(entry.messages) == len(originals)
    assert all(message is original for message, original in zip(entry.messages, originals, strict=True))
    assert owner.persist_count == 0
