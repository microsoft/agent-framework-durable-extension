# Copyright (c) Microsoft. All rights reserved.

"""Admit canonical shared state and reject malformed known fields before execution."""

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from agent_framework import Agent, AgentResponse, Message
from test_durable_history_provider import RecordingChatClient
from test_revision_contract import JsonStateProvider

from agent_framework_durabletask import AgentEntity, DurableAgentState
from agent_framework_durabletask._response_utils import invocation_outcome, serialize_agent_response

FIXTURES = Path(__file__).resolve().parents[4] / "schemas" / "fixtures"
SHARED_FIXTURE_PATHS = sorted(FIXTURES.glob("shared-durable-agent-state-2.0*.json"))


def _shared_state(*, expired: bool = False) -> dict[str, Any]:
    receipt: dict[str, Any] = {
        "correlationId": "completed",
        "outcome": "succeeded",
        "completedAt": "2026-01-01T00:00:00Z",
        "resultState": "unavailable" if expired else "available",
    }
    results = {
        "completed": {
            "correlationId": "completed",
            "outcome": "succeeded",
            "completedAt": "2026-01-01T00:00:00Z",
            "response": {"messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "answer"}]}]},
        }
    }
    if expired:
        receipt["resultUnavailableAt"] = "2026-01-01T00:01:00Z"
        results = {}
    return {
        "schemaVersion": "2.0.0",
        "data": {
            "conversationHistory": [],
            "terminalResults": results,
            "completionReceipts": {"completed": receipt},
            "historyBinding": {"version": 1, "ownerKind": "durableState", "providerKey": "example.history"},
        },
    }


@pytest.mark.parametrize("expired", [False, True], ids=["available", "expired"])
@pytest.mark.parametrize("json_boundary", [False, True], ids=["dict", "json"])
def test_canonical_layout_is_admitted_without_rewriting_raw_results(expired: bool, json_boundary: bool) -> None:
    payload = _shared_state(expired=expired)
    before = deepcopy(payload)
    state = DurableAgentState.from_json(json.dumps(payload)) if json_boundary else DurableAgentState.from_dict(payload)
    assert state.data.response_mailbox == payload["data"]["terminalResults"]
    assert state.data.completed_correlations == payload["data"]["completionReceipts"]
    response = state.try_get_agent_response("completed")
    assert response is not None
    if expired:
        assert response.additional_properties["durable_status"] == "already_completed"
        assert response.additional_properties["durable_outcome"] == "succeeded"
    else:
        assert response.text == "answer"
        assert serialize_agent_response(response)["type"] == "agent_response"
    assert state.to_dict() == before
    assert payload == before


@pytest.mark.parametrize("field", ["terminalResults", "completionReceipts"])
@pytest.mark.parametrize("value", [None, [], False, 0, ""], ids=["null", "array", "boolean", "zero", "empty-string"])
def test_known_delivery_containers_reject_invalid_types(field: str, value: Any) -> None:
    raw = _shared_state()
    raw["data"][field] = value
    before = deepcopy(raw)
    with pytest.raises(ValueError, match=field):
        DurableAgentState.from_dict(raw)
    assert raw == before


@pytest.mark.parametrize("operation", ["run", "reset", "expire_responses"])
@pytest.mark.parametrize("expired", [False, True])
async def test_entity_accepts_shared_completions_without_reexecuting(operation: str, expired: bool) -> None:
    provider = JsonStateProvider(_shared_state(expired=expired))
    before = deepcopy(provider.raw)
    client: Any = RecordingChatClient()
    entity = AgentEntity(Agent(client=client), state_provider=provider)
    if operation == "run":
        response = await entity.run({"message": "do not repeat", "correlationId": "completed"})
        if expired:
            assert response.additional_properties["durable_outcome"] == "succeeded"
        else:
            assert response.text == "answer"
        assert provider.writes == 0 and provider.raw == before
    else:
        getattr(entity, operation)()
    assert entity.state.data.response_mailbox == before["data"]["terminalResults"]
    assert entity.state.data.completed_correlations == before["data"]["completionReceipts"]
    assert client.received_messages == []


@pytest.mark.parametrize("field", ["response_mailbox", "completed_correlations"])
@pytest.mark.parametrize("operation", ["read", "serialize", "write", "run"])
async def test_cached_state_cannot_bypass_known_field_validation(field: str, operation: str) -> None:
    provider = JsonStateProvider(_shared_state())
    state = provider.state
    getattr(state.data, field)["completed"]["outcome"] = "unknown"
    before = deepcopy((state.data.response_mailbox, state.data.completed_correlations))
    committed = deepcopy(provider.raw)
    client: Any = RecordingChatClient()
    entity = AgentEntity(Agent(client=client), state_provider=provider)
    with pytest.raises(ValueError, match="outcome"):
        if operation == "read":
            state.try_get_agent_response("completed")
        elif operation == "serialize":
            state.to_dict()
        elif operation == "write":
            state.prepare_for_write(delivery_window_seconds=60)
        else:
            await entity.run({"message": "do not repeat", "correlationId": "completed"})
    assert (state.data.response_mailbox, state.data.completed_correlations) == before
    assert provider.raw == committed
    assert provider.writes == 0 and client.received_messages == []


def test_native_empty_delivery_and_unrelated_nested_metadata_remain_supported() -> None:
    state = DurableAgentState()
    raw = state.to_dict()
    raw["data"]["futureMetadata"] = {"terminalResults": {}, "completionReceipts": {"opaque": False}}
    raw["data"]["session"] = {"session_id": "test", "state": {"application": {"terminalResults": [1]}}}
    raw["application"] = {"completionReceipts": None}
    restored = DurableAgentState.from_dict(raw)
    assert restored.to_dict() == raw
    assert restored.try_get_agent_response("absent") is None
    restored.record_response(
        "native", AgentResponse(messages=[Message("assistant", ["native answer"])]), delivery_window_seconds=3600
    )
    cold = DurableAgentState.from_json(restored.to_json())
    response = cold.try_get_agent_response("native")
    assert response is not None and response.text == "native answer"


def test_published_shared_fixture_corpus_is_present() -> None:
    assert SHARED_FIXTURE_PATHS


@pytest.mark.parametrize("path", SHARED_FIXTURE_PATHS, ids=lambda path: path.stem)
def test_published_shared_fixtures_round_trip_without_normalizing_unknown_siblings(path: Path) -> None:
    raw = json.loads(path.read_text(encoding="utf-8"))
    before = deepcopy(raw)
    state = DurableAgentState.from_json(json.dumps(raw))
    assert state.to_dict() == before
    assert state.data.response_mailbox == raw["data"]["terminalResults"]
    assert state.data.completed_correlations == raw["data"]["completionReceipts"]
    for correlation, receipt in raw["data"]["completionReceipts"].items():
        response = state.try_get_agent_response(correlation)
        assert response is not None
        if receipt["resultState"] == "unavailable":
            assert response.additional_properties["durable_status"] == "already_completed"
            assert response.additional_properties["durable_outcome"] == receipt["outcome"]
        elif "resultExpiresAt" not in receipt:
            assert invocation_outcome(response) == receipt["outcome"]
    assert state.to_dict() == before and raw == before


@pytest.mark.parametrize(
    "invalid",
    [
        {"messages": None},
        {"messages": [{"role": "future-role"}]},
        {"messages": [{"role": "assistant", "contents": [{"$type": "text", "text": False}]}]},
        {"messages": [{"role": "assistant", "contents": [{"$type": "futureContent"}]}]},
        {"messages": [], "usage": {"inputTokenCount": True}},
    ],
)
async def test_invalid_known_response_fields_reject_before_model_or_writes(invalid: dict[str, Any]) -> None:
    raw = _shared_state()
    raw["data"]["terminalResults"]["completed"]["response"] = invalid
    before = deepcopy(raw)
    provider = JsonStateProvider(raw)
    client: Any = RecordingChatClient()
    entity = AgentEntity(Agent(client=client), state_provider=provider)
    with pytest.raises(ValueError):
        await entity.run({"message": "do not repeat", "correlationId": "completed"})
    assert provider.raw == before and provider.writes == 0
    assert client.received_messages == [] and raw == before
