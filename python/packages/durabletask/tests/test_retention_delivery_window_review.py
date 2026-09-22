# Copyright (c) Microsoft. All rights reserved.

"""Delivery-window contracts across transcript retention and cold duplicate runs.

Receipt-backed v2 delivery is independent of transcript retention. Unreceipted v2
history does not promise delivery, while legacy state still delivers its transcript.
"""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any, Literal, TypeAlias

import pytest
import test_retention as retention_donor
from _execution_test_support import JsonStateProvider, RecordingChatClient
from agent_framework import EXCLUDED_KEY, Agent, AgentResponse

from agent_framework_durabletask import (
    AgentEntity,
    DurableAgentState,
    DurableAgentStateTextContent,
    _delivery_state,
    _entities,
    _history_provider,
    _models,
    _retention,
    _shared_agent_state,
    _shared_state_validation,
    _state_reader,
    read_agent_state,
)
from agent_framework_durabletask._history_provider import (
    DurableHistoryBinding,
    DurableHistoryProvider,
    bind_durable_history,
    unbind_durable_history,
)

BASE = datetime(2026, 9, 22, 12, tzinfo=timezone.utc)
Mechanism: TypeAlias = Literal["pressure", "eager"]


class _Clock(datetime):
    instant = BASE

    @classmethod
    def now(cls, tz: tzinfo | None = None) -> _Clock:
        return cls.fromtimestamp(cls.instant.timestamp(), tz)


@pytest.fixture(autouse=True)
def frozen_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    _Clock.instant = BASE
    # Patch the imported modules, including validation of clock-produced values.
    # Deployment isolation remains owned by the repository's root fixture.
    monkeypatch.setattr(_delivery_state, "datetime", _Clock)
    monkeypatch.setattr(_entities, "datetime", _Clock)
    monkeypatch.setattr(_history_provider, "datetime", _Clock)
    monkeypatch.setattr(_models, "datetime", _Clock)
    monkeypatch.setattr(_retention, "datetime", _Clock)
    monkeypatch.setattr(_shared_agent_state, "datetime", _Clock)
    monkeypatch.setattr(_shared_state_validation, "datetime", _Clock)
    monkeypatch.setattr(_state_reader, "datetime", _Clock)
    monkeypatch.setattr(retention_donor, "datetime", _Clock)
    assert _retention.DELIVERY_WINDOW_SECONDS == 60


def _entity_for(
    provider: JsonStateProvider,
    window: int,
    mechanism: Mechanism,
    *,
    client: RecordingChatClient | None = None,
    budget: int | None = None,
) -> tuple[AgentEntity, RecordingChatClient, DurableHistoryProvider]:
    client = client if client is not None else RecordingChatClient()
    history = DurableHistoryProvider(prune_excluded=mechanism == "eager")
    entity = AgentEntity(
        Agent(client=client, name="window-audit", context_providers=[history]),
        state_provider=provider,
        response_delivery_window_seconds=window,
        retention="follow_compaction" if mechanism == "eager" else "keep_all",
        max_state_bytes=budget,
    )
    return entity, client, history


def _history_ids(raw: dict[str, Any], correlation: str | None = None) -> set[str]:
    # Public messageId can be absent while the canonical occurrence has a
    # pythonHistoryId. Use the real shared-state projection, not a wire-key guess.
    state = DurableAgentState.from_dict(raw)
    ids: set[str] = set()
    for entry in state.data.conversation_history:
        if correlation is None or entry.correlation_id == correlation:
            for message in entry.messages:
                assert message.message_id is not None
                ids.add(message.message_id)
    return ids


def _assert_response(response: AgentResponse | None, expected: dict[str, Any], *, expired: bool) -> AgentResponse:
    assert response is not None
    if expired:
        assert response.additional_properties == {
            "durable_status": "already_completed",
            "correlation_id": "old",
            "durable_outcome": "succeeded",
        }
        error_codes = [content.error_code for message in response.messages for content in message.contents]
        assert error_codes == ["response_expired"]
    else:
        assert response.text == "reply-1"
        assert response.to_dict() == expected
    return response


def _poll(raw: dict[str, Any], expected: dict[str, Any], *, expired: bool) -> None:
    before = deepcopy(raw)
    reader = read_agent_state(raw)
    response = _assert_response(reader.try_get_agent_response("old"), expected, expired=expired)
    # The consumer owns its projection, not either retained copy.
    response.messages.clear()
    _assert_response(reader.try_get_agent_response("old"), expected, expired=expired)
    assert reader.to_dict() == raw == before


async def _duplicate(
    raw: dict[str, Any], window: int, mechanism: Mechanism, expected: dict[str, Any], *, expired: bool
) -> None:
    provider = JsonStateProvider(raw)
    entity, client, _ = _entity_for(provider, window, mechanism, budget=10_000)
    before = deepcopy(provider.raw)
    response = await entity.run({"correlationId": "old", "message": "must not execute this new input"})
    _assert_response(response, expected, expired=expired)
    assert client.received_messages == []
    assert provider.attempted_writes == provider.successful_writes == 0
    assert provider.raw == entity.state.to_dict() == before


# Boundary-focused pairs, not the full window/age Cartesian product.
@pytest.mark.asyncio
@pytest.mark.parametrize("mechanism", ["pressure", "eager"])
@pytest.mark.parametrize(
    ("window", "age"),
    [(5, 1), (5, 4), (5, 5), (5, 30), (300, 1), (300, 5), (300, 30), (300, 61), (300, 299), (300, 300)],
)
async def test_real_turn_retention_and_cold_duplicate_delivery(mechanism: Mechanism, window: int, age: int) -> None:
    provider = JsonStateProvider()
    entity, client, _ = _entity_for(provider, window, mechanism)
    response = await entity.run({"correlationId": "old", "message": "original question"})
    assert response.text == "reply-1"
    assert len(client.received_messages) == provider.successful_writes == 1
    expected = deepcopy(response.to_dict())
    original = deepcopy(provider.raw["data"])
    result, receipt = original["terminalResults"]["old"], original["completionReceipts"]["old"]
    assert receipt["completedAt"] == BASE.isoformat()
    assert receipt["resultExpiresAt"] == (BASE + timedelta(seconds=window)).isoformat()
    assert result["response"]["messages"][0]["contents"][0]["text"] == "reply-1"
    # Put byte pressure ONLY on transcript copies. Never manufacture delivery records.
    for entry in entity.state.data.conversation_history:
        assert entry.correlation_id == "old"
        for message in entry.messages:
            message.extension_data = {**(message.extension_data or {}), "auditPadding": "x" * 16_000}
            if message.role == "assistant":
                content = message.contents[0]
                assert isinstance(content, DurableAgentStateTextContent)
                content.text = "transcript-only-copy"
            if mechanism == "eager":
                message.extension_data[EXCLUDED_KEY] = True
    entity.persist_state()
    before = deepcopy(provider.raw)
    transcript_texts = [
        content["text"]
        for entry in before["data"]["conversationHistory"]
        for message in entry["messages"]
        if message["role"] == "assistant"
        for content in message["contents"]
    ]
    assert transcript_texts == ["transcript-only-copy"]
    assert before["data"]["terminalResults"] == original["terminalResults"]
    assert before["data"]["completionReceipts"] == original["completionReceipts"]
    old_ids = _history_ids(before, "old")
    assert len(old_ids) == 2
    assert len(json.dumps(before)) > 10_000

    _Clock.instant = BASE + timedelta(seconds=age)
    expired = age >= window
    _poll(before, expected, expired=expired)
    await _duplicate(before, window, mechanism, expected, expired=expired)
    # Cold storage reload, then a genuine different public turn. run itself calls
    # _enforce_retention, and eager pruning goes through the real load/flush pipeline.
    cold = JsonStateProvider(before)
    entity, _, history = _entity_for(
        cold,
        window,
        mechanism,
        client=client,
        budget=10_000 if mechanism == "pressure" else None,
    )
    assert history.prune_excluded is (mechanism == "eager")
    new_response = await entity.run({"correlationId": "new", "message": "new question"})
    assert new_response.text == "reply-2"
    assert len(client.received_messages) == 2
    assert cold.attempted_writes == cold.successful_writes == 1
    after = deepcopy(cold.raw)
    assert old_ids.isdisjoint(_history_ids(after)), "The targeted OLD transcript must actually be deleted."
    assert len(_history_ids(after, "new")) == 2, "Do not satisfy deletion assertions by pruning the newest turn."
    assert after["data"]["truncation"]["evictedMessageCount"] == len(old_ids)
    assert set(after["data"]["completionReceipts"]) == {"old", "new"}
    if expired:
        assert set(after["data"]["terminalResults"]) == {"new"}
        assert after["data"]["completionReceipts"]["old"] == {
            **receipt,
            "resultState": "unavailable",
            "resultUnavailableAt": _Clock.now(timezone.utc).isoformat(),
        }
    else:
        assert set(after["data"]["terminalResults"]) == {"old", "new"}
        assert after["data"]["terminalResults"]["old"] == result
        assert after["data"]["completionReceipts"]["old"] == receipt
    _poll(after, expected, expired=expired)
    await _duplicate(after, window, mechanism, expected, expired=expired)


@pytest.mark.asyncio
@pytest.mark.parametrize("mechanism", ["pressure", "eager"])
@pytest.mark.parametrize("age", [30, 61])
async def test_unreceipted_v2_transcript_never_promises_delivery(mechanism: Mechanism, age: int) -> None:
    # The donor intentionally builds 1.2 transcript-delivered state. Copy only its
    # history into fresh v2 state, without inventing terminal results or receipts.
    # This characterizes unreceipted history, not an authorized migration.
    donor = retention_donor._state(12, chars=1000)
    assert donor.schema_version == "1.2.0"
    state = DurableAgentState()
    state.data.conversation_history = deepcopy(donor.data.conversation_history)
    for entry in state.data.conversation_history[:2]:
        entry.created_at = BASE
        if mechanism == "eager":
            for message in entry.messages:
                message.extension_data = {EXCLUDED_KEY: True}
    _Clock.instant = BASE + timedelta(seconds=age)
    provider = JsonStateProvider(state.to_dict())
    entity, client, history = _entity_for(provider, 300, mechanism, budget=12_000 if mechanism == "pressure" else None)
    before = deepcopy(provider.raw)
    assert _history_ids(before, "c0") == {"u0", "a0"}
    assert entity.state.try_get_agent_response("c0") is None
    reader = read_agent_state(before)
    assert reader.try_get_agent_response("c0") is None
    assert reader.to_dict() == before == provider.raw
    if mechanism == "eager":
        token = bind_durable_history(DurableHistoryBinding(provider, correlation_id="c11"))
        try:
            working: dict[str, Any] = {}
            await history.get_messages(provider.session_id, state=working)
            history.flush(working)
        finally:
            unbind_durable_history(token)
    await entity._enforce_retention()
    entity.persist_state()
    after = deepcopy(provider.raw)
    removed_target = mechanism == "eager" or age == 61
    assert _history_ids(after, "c0") == (set() if removed_target else {"u0", "a0"})
    assert _history_ids(after, "c11") == {"u11", "a11"}
    assert len(_history_ids(after)) < len(_history_ids(before)), "Exercise real deletion even when c0 is protected."
    assert after["data"]["terminalResults"] == before["data"]["terminalResults"] == {}
    assert after["data"]["completionReceipts"] == before["data"]["completionReceipts"] == {}
    assert entity.state.try_get_agent_response("c0") is None
    reader = read_agent_state(after)
    assert reader.try_get_agent_response("c0") is None
    assert reader.to_dict() == after == provider.raw
    assert client.received_messages == []


@pytest.mark.asyncio
@pytest.mark.parametrize("age", [30, 61])
async def test_legacy_default_helper_has_distinct_transcript_delivery_contract(age: int) -> None:
    # Keep the donor's legacy schema here so transcript lookup remains the control.
    state = retention_donor._state(12, chars=1000)
    assert state.schema_version == "1.2.0"
    for entry in state.data.conversation_history[:2]:
        entry.created_at = BASE
    _Clock.instant = BASE + timedelta(seconds=age)
    response = state.try_get_agent_response("c0")
    assert response is not None
    assert response.text == "a" * 1000
    removed = await _retention.enforce_budget(state, max_state_bytes=12_000)
    assert removed > 0
    assert _history_ids(state.to_dict(), "c11") == {"u11", "a11"}
    if age == 30:
        assert _history_ids(state.to_dict(), "c0") == {"u0", "a0"}
        retained = state.try_get_agent_response("c0")
        assert retained is not None
        assert retained.text == "a" * 1000
    else:
        assert _history_ids(state.to_dict(), "c0") == set()
        assert state.try_get_agent_response("c0") is None
