# Copyright (c) Microsoft. All rights reserved.

"""Retention observations across uncertain host writes and warm retries."""

import json
from datetime import timedelta
from typing import Any

import pytest
import test_retention_telemetry as retention_metrics
from _execution_test_support import JsonStateProvider, RecordingChatClient
from agent_framework import EXCLUDED_KEY, Agent, AgentResponse, Message
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, Metric, Sum

from agent_framework_durabletask import (
    AgentEntity,
    DurableAgentState,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
)

# Reuse the isolated SDK reader and instrument-cache cleanup, not a global provider.
retention_reader = retention_metrics.reader
CURRENT = "retention-acknowledgement"


def _json(value: Any) -> Any:
    return json.loads(json.dumps(value, allow_nan=False))


def _initial_state() -> DurableAgentState:
    state = DurableAgentState()
    old = retention_metrics.NOW - timedelta(days=1)
    for index in range(3):
        correlation = f"old-{index}"
        for role, kind in (("user", DurableAgentStateRequest), ("assistant", DurableAgentStateResponse)):
            message = Message(
                role,
                [f"old {role} {index} " * 40],
                message_id=f"{correlation}-{role}",
                additional_properties={EXCLUDED_KEY: True} if index < 2 else {},
            )
            state.data.conversation_history.append(
                kind(correlation, old, [DurableAgentStateMessage.from_chat_message(message)])
            )
        response = AgentResponse(messages=[message])
        state.record_response(correlation, response, now=old, delivery_window_seconds=60)
    # Retain legitimate completion receipts without an unrelated expiry write on retry.
    state.expire_responses(now=retention_metrics.NOW)
    return state


class _FailFirstWrite(JsonStateProvider):
    def __init__(self, raw: dict[str, Any], *, committed: bool) -> None:
        super().__init__(raw)
        self.committed = committed
        self.attempts: list[dict[str, Any]] = []

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        first = not self.attempts
        payload = _json(state)
        self.attempts.append(payload)
        self.fail_before_write = first and not self.committed
        try:
            super()._set_state_dict(payload)
        finally:
            self.fail_before_write = False
        if first:
            raise OSError("storage acknowledgement lost after write")


def _history_ids(raw: dict[str, Any]) -> set[str]:
    return {
        message["messageId"]
        for entry in raw["data"]["conversationHistory"]
        for message in entry["messages"]
        if "messageId" in message
    }


def _assert_staged_turn(raw: dict[str, Any], before: dict[str, Any], excluded: set[str], *, reply: str) -> None:
    assert _json(raw) == raw
    data = raw["data"]
    assert _history_ids(raw) & _history_ids(before) == _history_ids(before) - excluded
    assert data["truncation"]["evictedMessageCount"] == len(excluded)
    assert [
        content["text"]
        for entry in data["conversationHistory"]
        if entry.get("correlationId") == CURRENT
        for message in entry["messages"]
        for content in message["contents"]
        if content["$type"] == "text"
    ] == ["new question", reply]
    assert set(data["terminalResults"]) == {CURRENT}
    result = data["terminalResults"][CURRENT]
    assert result["correlationId"] == CURRENT
    assert result["outcome"] == "succeeded"
    assert [
        content["text"]
        for message in result["response"]["messages"]
        for content in message["contents"]
        if content["$type"] == "text"
    ] == [reply]
    assert data["completionReceipts"] == {
        **before["data"]["completionReceipts"],
        CURRENT: {
            "correlationId": CURRENT,
            "outcome": "succeeded",
            "completedAt": result["completedAt"],
            "resultExpiresAt": result["resultExpiresAt"],
            "resultState": "available",
        },
    }


def _assert_staged_metrics(metrics: dict[str, Metric], *, removed: int, operations: int) -> None:
    attrs = retention_metrics._attributes("eager")
    retention_metrics._counter(metrics["removed_messages"], attrs, removed)
    retention_metrics._counter(metrics["removed_entries"], attrs, removed)
    operation_data = metrics["operations"].data
    assert isinstance(operation_data, Sum)
    assert (
        sum(point.value for point in operation_data.data_points if (point.attributes or {}).get("deletion_staged"))
        == operations
    )
    for name, metric in metrics.items():
        for point in metric.data.data_points:
            assert point.attributes is not None
            assert point.attributes["commit_status"] in {"not_attempted", "unknown"}
            if name in {"removed_messages", "removed_entries", "reclaimed_bytes"}:
                assert point.attributes["outcome"] == "staged"
                assert point.attributes["commit_status"] == "not_attempted"


@pytest.mark.parametrize("committed", [True, False], ids=["lost-acknowledgement", "prewrite-failure"])
async def test_retention_unknown_acknowledgement_refreshes_before_warm_retry(
    retention_reader: InMemoryMetricReader, committed: bool
) -> None:
    before = _json(_initial_state().to_dict())
    excluded = {
        message["messageId"]
        for entry in before["data"]["conversationHistory"]
        for message in entry["messages"]
        if message.get("extensionData", {}).get(EXCLUDED_KEY)
    }
    assert len(excluded) == 4
    assert before["data"]["terminalResults"] == {}
    assert set(before["data"]["completionReceipts"]) == {"old-0", "old-1", "old-2"}
    assert all(receipt["resultState"] == "unavailable" for receipt in before["data"]["completionReceipts"].values())
    provider = _FailFirstWrite(before, committed=committed)
    client = RecordingChatClient(response_message_id="new-answer")
    entity = AgentEntity(
        Agent(client=client, name="retention-acknowledgement"),
        state_provider=provider,
        retention="follow_compaction",
        max_state_bytes=None,
    )
    original = entity.state
    request = {"message": "new question", "correlationId": CURRENT}
    error = "acknowledgement lost" if committed else "injected commit failure"

    with pytest.raises(OSError, match=error):
        await entity.run(request)

    assert len(client.received_messages) == 1
    assert excluded.isdisjoint(message.message_id for message in client.received_messages[0])
    assert provider.attempted_writes == 1
    assert provider.successful_writes == int(committed)
    assert entity.state is original
    assert _json(entity.state.to_dict()) == before
    assert entity.state.try_get_agent_response(CURRENT) is None
    attempted = provider.attempts[0]
    _assert_staged_turn(attempted, before, excluded, reply="reply-1")
    assert _json(provider.raw) == (attempted if committed else before)
    if committed:
        _assert_staged_turn(provider.raw, before, excluded, reply="reply-1")

    failed = {"outcome": "failed", "commit_status": "unknown", "deletion_staged": True}
    metrics = retention_metrics._metrics(retention_reader)
    _assert_staged_metrics(metrics, removed=len(excluded), operations=1)
    retention_metrics._counter(metrics["operations"], failed, 1)
    retention_metrics._counter(metrics["write_attempts"], {**failed, "stage": "set_state"}, 1)

    # Same entity and provider must refresh the uncertain cache before duplicate detection.
    response = await entity.run(request)

    calls = 1 if committed else 2
    assert response.text == f"reply-{calls}"
    assert len(client.received_messages) == calls
    assert provider.attempted_writes == calls
    assert provider.successful_writes == 1
    assert entity.state is not original
    assert _json(entity.state.to_dict()) == _json(provider.raw)
    _assert_staged_turn(provider.raw, before, excluded, reply=f"reply-{calls}")
    if committed:
        # The refreshed completion prevents another model call, history edit or host write.
        assert provider.raw == attempted
    else:
        assert provider.raw == provider.attempts[1]
        assert provider.raw != attempted

    metrics = retention_metrics._metrics(retention_reader)
    _assert_staged_metrics(metrics, removed=len(excluded) * calls, operations=calls)
    retention_metrics._counter(metrics["operations"], failed, 1)
    retention_metrics._counter(metrics["write_attempts"], {**failed, "stage": "set_state"}, 1)
    if not committed:
        returned = {"outcome": "returned", "commit_status": "unknown", "deletion_staged": True}
        retention_metrics._counter(metrics["operations"], returned, 1)
        retention_metrics._counter(metrics["write_attempts"], {**returned, "stage": "set_state"}, 1)
