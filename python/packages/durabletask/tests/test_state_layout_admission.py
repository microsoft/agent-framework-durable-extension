# Copyright (c) Microsoft. All rights reserved.

"""Reject known incompatible delivery layouts instead of reopening completed work."""

import json
from copy import deepcopy
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import Agent, AgentResponse, Message
from test_durable_history_provider import RecordingChatClient
from test_revision_contract import JsonStateProvider

from agent_framework_durabletask import AgentEntity, DurableAgentState


def _foreign_state(*, expired: bool = False) -> dict[str, Any]:
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
def test_incompatible_layout_is_rejected_even_with_the_same_schema_version(expired: bool, json_boundary: bool) -> None:
    payload = _foreign_state(expired=expired)
    before = deepcopy(payload)
    with pytest.raises(ValueError, match="incompatible.*delivery|delivery.*incompatible"):
        if json_boundary:
            DurableAgentState.from_json(json.dumps(payload))
        else:
            DurableAgentState.from_dict(payload)
    assert payload == before


@pytest.mark.parametrize("field", ["terminalResults", "completionReceipts"])
@pytest.mark.parametrize("value", [{}, None, []], ids=["empty", "null", "malformed"])
@pytest.mark.parametrize("mixed", [False, True])
def test_reserved_alternate_containers_never_hide_as_optional_metadata(field: str, value: Any, mixed: bool) -> None:
    state = DurableAgentState()
    if mixed:
        state.record_response("native", AgentResponse(messages=[]), delivery_window_seconds=3600)
    raw = state.to_dict()
    raw["data"][field] = value
    before = deepcopy(raw)
    with pytest.raises(ValueError, match="incompatible.*delivery|delivery.*incompatible"):
        DurableAgentState.from_dict(raw)
    assert raw == before


@pytest.mark.parametrize("operation", ["run", "reset", "expire_responses"])
@pytest.mark.parametrize("expired", [False, True])
async def test_entity_refuses_incompatible_state_before_model_calls_or_writes(operation: str, expired: bool) -> None:
    provider = JsonStateProvider(_foreign_state(expired=expired))
    before = deepcopy(provider.raw)
    client: Any = RecordingChatClient()
    entity = AgentEntity(Agent(client=client), state_provider=provider)
    with pytest.raises(ValueError, match="incompatible.*delivery|delivery.*incompatible"):
        if operation == "run":
            await entity.run({"message": "do not repeat", "correlationId": "completed"})
        else:
            getattr(entity, operation)()
    assert client.received_messages == []
    assert provider.writes == 0 and provider.raw == before


@pytest.mark.parametrize("field", ["terminalResults", "completionReceipts"])
@pytest.mark.parametrize("operation", ["read", "serialize", "write", "run"])
async def test_cached_state_cannot_bypass_delivery_layout_admission(field: str, operation: str) -> None:
    provider = JsonStateProvider()
    provider.state.data.unknown_fields[field] = {"completed": {"outcome": "succeeded"}}
    state = provider.state
    before = deepcopy(state.data.unknown_fields)
    client: Any = RecordingChatClient()
    entity = AgentEntity(Agent(client=client), state_provider=provider)
    with pytest.raises(ValueError, match="incompatible.*delivery|delivery.*incompatible"):
        if operation == "read":
            state.try_get_agent_response("completed")
        elif operation == "serialize":
            state.to_dict()
        elif operation == "write":
            state.prepare_for_write(delivery_window_seconds=60)
        else:
            await entity.run({"message": "do not repeat", "correlationId": "completed"})
    assert state.data.unknown_fields == before
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


def test_alternate_completion_is_rejected_before_any_response_deserialization(monkeypatch: pytest.MonkeyPatch) -> None:
    loader = Mock(side_effect=AssertionError("Unsupported state must not be interpreted as a response"))
    monkeypatch.setattr("agent_framework_durabletask._durable_agent_state.load_agent_response", loader)
    with pytest.raises(ValueError, match="incompatible.*delivery|delivery.*incompatible"):
        DurableAgentState.from_dict(_foreign_state())
    loader.assert_not_called()
