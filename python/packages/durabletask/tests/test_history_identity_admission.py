# Copyright (c) Microsoft. All rights reserved.

"""Foreign identity admission and timestamp preservation at the Core boundary."""

import inspect
import json
from copy import deepcopy
from typing import Any

import pytest
from _shared_history_test_support import _bound, _CanonicalStateProvider
from agent_framework import Message

from agent_framework_durabletask._history_provider import DurableHistoryProvider
from agent_framework_durabletask._shared_agent_state import DurableAgentState, DurableAgentStateMessage


def _root(messages: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schemaVersion": "2.0.0",
        "data": {
            "conversationHistory": [{"$type": "request", "messages": messages}],
            "terminalResults": {},
            "completionReceipts": {},
        },
    }


@pytest.mark.parametrize("identity", [None, {}, {"profile": "foreign", "version": 1}])
@pytest.mark.parametrize("field", ["pythonHistoryId", "pythonHistoryIdentity"])
@pytest.mark.parametrize("public_id", [None, "", "duplicate"])
async def test_opaque_identity_repair_rejects_before_mutating_any_message(
    identity: Any, field: str, public_id: str | None
) -> None:
    messages: list[dict[str, Any]] = [
        {"role": "user", "contents": [{"$type": "text", "text": "anonymous repair first"}]},
        {"role": "user", "messageId": "duplicate", "contents": []},
        {"role": "user", field: deepcopy(identity), "contents": [{"$type": "text", "text": "foreign"}]},
    ]
    if public_id is not None:
        messages[-1]["messageId"] = public_id
    raw = _root(messages)
    before = json.dumps(raw, sort_keys=True)
    provider = _CanonicalStateProvider()
    provider.state = DurableAgentState.from_dict(raw)
    originals = tuple(provider.state.data.conversation_history[0].messages)
    working: dict[str, Any] = {}
    with _bound(provider), pytest.raises(ValueError, match="opaque.*history identity"):
        await DurableHistoryProvider().get_messages("session", state=working)
    assert working == {}
    assert json.dumps(provider.state.to_dict(), sort_keys=True) == before
    assert tuple(provider.state.data.conversation_history[0].messages) == originals
    assert provider.persist_count == 0


def test_direct_history_identity_assignment_preserves_foreign_fields_on_rejection() -> None:
    raw = {"role": "user", "pythonHistoryIdentity": {"profile": "other", "version": 1}}
    message = DurableAgentStateMessage.from_dict(raw)
    with pytest.raises(ValueError, match="opaque.*history identity"):
        message.set_history_id("private")
    assert message.to_dict() == raw
    assert message.message_id is None and message.public_message_id is None


async def test_unique_foreign_identity_and_high_precision_timestamp_survive_replay_and_flush() -> None:
    raw = _root([
        {
            "role": "user",
            "messageId": "unique",
            "createdAt": "2026-09-17T09:00:00.123456789+01:00",
            "pythonHistoryId": {"opaque": [None, False, 0]},
            "pythonHistoryIdentity": {"profile": "other", "version": 1},
            "contents": [{"$type": "text", "text": "message"}],
        }
    ])
    provider = _CanonicalStateProvider()
    provider.state = DurableAgentState.from_dict(raw)
    working: dict[str, Any] = {}
    history = DurableHistoryProvider()
    with _bound(provider):
        loaded = await history.get_messages("session", state=working)
        assert loaded[0].message_id == "unique"
        assert loaded[0].text == "message"
        # Core 1.13 and 1.16 have no public Message timestamp constructor field.
        assert "created_at" not in inspect.signature(Message.__init__).parameters
        history.flush(working)
        history.flush(working)
    assert provider.state.to_dict() == raw
    assert DurableAgentState.from_json(provider.state.to_json()).to_dict() == raw


@pytest.mark.parametrize("public_id", [None, "repeated"])
async def test_aliased_stored_occurrences_reject_before_identity_repair(public_id: str | None) -> None:
    raw_message: dict[str, Any] = {"role": "user", "contents": [{"$type": "text", "text": "same object"}]}
    if public_id is not None:
        raw_message["messageId"] = public_id
    provider = _CanonicalStateProvider()
    provider.state = DurableAgentState.from_dict(_root([raw_message]))
    messages = provider.state.data.conversation_history[0].messages
    messages.append(messages[0])
    before = provider.state.to_dict()
    with _bound(provider), pytest.raises(ValueError, match="distinct stored message"):
        await DurableHistoryProvider().get_messages("session", state={})
    assert provider.state.to_dict() == before
    assert messages[0] is messages[1]
