# Copyright (c) Microsoft. All rights reserved.

"""No-op delivery and unchanged message metadata retain their original representation."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from _shared_history_test_support import _bound, _CanonicalStateProvider
from agent_framework import AgentResponse, Message

from agent_framework_durabletask._history_provider import DurableHistoryProvider
from agent_framework_durabletask._shared_agent_state import DurableAgentState


@pytest.mark.parametrize("metadata", [None, {}, {"trace": [False, 0]}])
@pytest.mark.parametrize("clear", [False, True])
async def test_flush_preserves_empty_metadata_presence_and_applies_real_clear(metadata: Any, clear: bool) -> None:
    message: dict[str, Any] = {"role": "user", "messageId": "id", "contents": [{"$type": "text", "text": "value"}]}
    if metadata is not None:
        message["extensionData"] = deepcopy(metadata)
    raw: dict[str, Any] = {
        "schemaVersion": "2.0.0",
        "data": {
            "conversationHistory": [{"$type": "request", "messages": [message]}],
            "terminalResults": {},
            "completionReceipts": {},
        },
    }
    provider = _CanonicalStateProvider()
    provider.state = DurableAgentState.from_dict(raw)
    history = DurableHistoryProvider()
    working: dict[str, Any] = {}
    with _bound(provider):
        loaded = await history.get_messages("session", state=working)
        if clear:
            loaded[0].additional_properties.clear()
        history.flush(working)
        history.flush(working)
    expected = deepcopy(raw)
    if clear and metadata:
        expected["data"]["conversationHistory"][0]["messages"][0].pop("extensionData")
    assert provider.state.to_dict() == expected


@pytest.mark.parametrize("operation", ["duplicate", "expiry"])
def test_noop_delivery_keeps_map_and_record_identity(operation: str) -> None:
    now = datetime(2026, 9, 18, tzinfo=timezone.utc)
    state = DurableAgentState()
    state.record_response(
        "c", AgentResponse(messages=[Message("assistant", ["answer"])]), delivery_window_seconds=60, now=now
    )
    results = state.data.response_mailbox
    receipts = state.data.completed_correlations
    result = results["c"]
    receipt = receipts["c"]
    before = state.to_dict()
    if operation == "duplicate":
        state.record_response("c", AgentResponse(messages=[]), delivery_window_seconds=-1, now=now)
    else:
        state.expire_responses(now=now + timedelta(seconds=1))
    assert state.to_dict() == before
    assert state.data.response_mailbox is results
    assert state.data.completed_correlations is receipts
    assert results["c"] is result and receipts["c"] is receipt
