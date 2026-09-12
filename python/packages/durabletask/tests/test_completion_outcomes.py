# Copyright (c) Microsoft. All rights reserved.

"""Outcome receipts across expiry, old-state handling and trusted migration boundaries."""

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from agent_framework import Agent, AgentResponse, Content, Message
from test_durable_history_provider import RecordingChatClient
from test_revision_contract import JsonStateProvider
from typing_extensions import Self

from agent_framework_durabletask import AgentEntity, DurableAgentState, migrate_legacy_state, state_snapshot_digest
from agent_framework_durabletask import _durable_agent_state as state_module
from agent_framework_durabletask._response_utils import serialize_agent_response

NOW = datetime(2026, 9, 11, 12, tzinfo=timezone.utc)
WINDOW = 60
CORRELATION = "outcome-correlation"


class Clock(datetime):
    current = NOW

    @classmethod
    def now(cls, tz: Any = None) -> Self:
        return cls.fromtimestamp(cls.current.timestamp(), tz or timezone.utc)


@pytest.fixture(autouse=True)
def clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Clock, "current", NOW)
    monkeypatch.setattr(state_module, "datetime", Clock)


def _response(kind: str) -> AgentResponse[Any]:
    if kind == "error-status":
        return AgentResponse(messages=[], additional_properties={"durable_status": "error"})
    if kind == "error-content":
        return AgentResponse(messages=[Message("assistant", [Content.from_error(message="provider failed")])])
    if kind == "recovered-tool":
        return AgentResponse(
            messages=[
                Message("tool", [Content.from_error(message="recoverable lookup failure")]),
                Message("assistant", ["recovered answer"]),
            ]
        )
    if kind == "approval":
        return AgentResponse(
            messages=[
                Message(
                    "assistant",
                    [
                        Content.from_function_approval_request(
                            "approval-1", Content.from_function_call("call-1", "work")
                        )
                    ],
                )
            ]
        )
    if kind == "empty":
        return AgentResponse(messages=[])
    if kind == "structured-false":
        return AgentResponse(messages=[], value=False)
    return AgentResponse(messages=[Message("assistant", ["original answer"])], response_id="original-response")


def _state(kind: str = "success") -> DurableAgentState:
    state = DurableAgentState()
    state.record_response(CORRELATION, _response(kind), delivery_window_seconds=WINDOW, now=NOW)
    return state


def _cold(state: DurableAgentState) -> DurableAgentState:
    return DurableAgentState.from_json(state.to_json())


@pytest.mark.parametrize(
    "kind", ["success", "empty", "structured-false", "recovered-tool", "approval", "error-status", "error-content"]
)
@pytest.mark.parametrize("cleanup", [False, True])
@pytest.mark.parametrize("cold", [False, True])
def test_new_receipt_retains_invocation_outcome_without_payload_after_expiry(
    kind: str, cleanup: bool, cold: bool
) -> None:
    response = _response(kind)
    state = _state(kind)
    expected = "failed" if kind.startswith("error-") else "succeeded"
    receipt = {"completedAt": NOW.isoformat(), "outcome": expected}
    assert state.data.completed_correlations[CORRELATION] == receipt
    Clock.current = NOW + timedelta(seconds=WINDOW, microseconds=-1)
    delivered = state.try_get_agent_response(CORRELATION)
    assert delivered is not None
    assert serialize_agent_response(delivered) == serialize_agent_response(response)

    Clock.current = NOW + timedelta(seconds=WINDOW)
    if cleanup:
        state.expire_responses(now=Clock.current)
    if cold:
        state = _cold(state)
    before = state.to_json()
    expired = state.try_get_agent_response(CORRELATION)
    assert expired is not None
    assert expired.additional_properties == {
        "durable_status": "already_completed",
        "correlation_id": CORRELATION,
        "durable_outcome": expected,
    }
    assert expired.messages[0].contents[0].error_code == "response_expired"
    assert expired.response_id is None and expired.value is None and expired.continuation_token is None
    assert state.to_json() == before
    assert state.data.completed_correlations[CORRELATION] == receipt
    assert bool(state.data.response_mailbox) is not cleanup
    state.record_response(
        CORRELATION, _response("error-status" if expected == "succeeded" else "success"), delivery_window_seconds=999
    )
    assert state.to_json() == before


@pytest.mark.parametrize("kind", ["success", "error-content"])
@pytest.mark.parametrize("legacy", [False, True])
def test_old_receipt_uses_only_independent_result_evidence_before_cleanup(kind: str, legacy: bool) -> None:
    state = _state(kind)
    receipt = state.data.completed_correlations[CORRELATION]
    receipt.pop("outcome", None)
    receipt["future"] = {"keep": [1]}
    if legacy:
        receipt["legacy"] = True
    state = _cold(state)
    original_receipt = deepcopy(state.data.completed_correlations[CORRELATION])
    Clock.current = NOW + timedelta(seconds=WINDOW)
    expected = "failed" if kind == "error-content" else "unknown" if legacy else "succeeded"
    before = state.to_json()
    response = state.try_get_agent_response(CORRELATION)
    assert response is not None and response.additional_properties["durable_outcome"] == expected
    assert state.to_json() == before, "lookup cannot rewrite persisted evidence"
    state.expire_responses(now=Clock.current)
    state = _cold(state)
    assert state.data.response_mailbox == {}
    assert state.data.completed_correlations[CORRELATION] == {
        **original_receipt,
        **({"outcome": expected} if expected != "unknown" else {}),
    }
    expired = state.try_get_agent_response(CORRELATION)
    assert expired is not None and expired.additional_properties["durable_outcome"] == expected


@pytest.mark.parametrize("cold", [False, True])
async def test_old_unknown_receipt_still_suppresses_execution_and_survives_reset(cold: bool) -> None:
    state = DurableAgentState()
    state.data.completed_correlations[CORRELATION] = {"completedAt": NOW.isoformat(), "future": {"keep": True}}
    original = deepcopy(state.to_dict())
    client: Any = RecordingChatClient()
    provider = JsonStateProvider(_cold(state).to_dict() if cold else state.to_dict())
    entity = AgentEntity(Agent(client=client), state_provider=provider)
    response = await entity.run({"message": "never rerun", "correlationId": CORRELATION})
    assert response.additional_properties["durable_outcome"] == "unknown"
    assert response.additional_properties["durable_status"] == "already_completed"
    assert client.received_messages == [] and provider.writes == 0 and provider.raw == original
    entity.reset()
    assert _cold(entity.state).data.completed_correlations == state.data.completed_correlations
    assert entity.state.data.response_mailbox == {}


@pytest.mark.parametrize("invalid", [None, "", "success", "FAILED", "unknown", 1, False, [], {}])
def test_invalid_present_outcome_is_rejected_at_read_and_warm_boundaries(invalid: Any) -> None:
    state = _state()
    state.data.response_mailbox.clear()
    state.data.completed_correlations[CORRELATION]["outcome"] = invalid
    raw = {"schemaVersion": "2.0.0", "data": {"completedCorrelations": deepcopy(state.data.completed_correlations)}}
    with pytest.raises(ValueError, match="outcome"):
        DurableAgentState.from_dict(raw)
    with pytest.raises(ValueError, match="outcome"):
        DurableAgentState.from_json(json.dumps(raw))
    with pytest.raises(ValueError, match="outcome"):
        state.to_dict()
    with pytest.raises(ValueError, match="outcome"):
        state.prepare_for_write(delivery_window_seconds=WINDOW)
    with pytest.raises(ValueError, match="outcome"):
        state.try_get_agent_response(CORRELATION)


def _migrate(source: dict[str, Any], **kwargs: Any) -> DurableAgentState:
    return migrate_legacy_state(
        source,
        source_digest=state_snapshot_digest(source),
        source_session_id="original-session",
        migration_id="outcome-migration",
        ownership_transfer_id="authorized-transfer",
        delivery_window_seconds=WINDOW,
        now=NOW + timedelta(days=1),
        **kwargs,
    )


@pytest.mark.parametrize("mailbox", [False, True])
def test_known_outcome_import_requires_authoritative_evidence_and_preserves_completion_time(mailbox: bool) -> None:
    source = _state("error-content").to_dict()
    source["schemaVersion"] = "1.1.0"
    source["data"]["completedCorrelations"][CORRELATION].pop("outcome", None)
    if not mailbox:
        source["data"].pop("responseMailbox")
    before = deepcopy(source)
    if mailbox:
        result = _cold(_migrate(source, require_known_outcomes=True))
        assert result.data.completed_correlations[CORRELATION] == {"completedAt": NOW.isoformat(), "outcome": "failed"}
        assert result.data.response_mailbox == before["data"]["responseMailbox"]
    else:
        with pytest.raises(ValueError, match="outcome.*evidence"):
            _migrate(source, require_known_outcomes=True)
        compatible = _cold(_migrate(source))
        assert compatible.data.completed_correlations == source["data"]["completedCorrelations"]
        assert compatible.data.response_mailbox == {}
        response = compatible.try_get_agent_response(CORRELATION)
        assert response is not None and response.additional_properties["durable_outcome"] == "unknown"
    assert source == before


def test_existing_mailbox_backfill_uses_original_completion_timestamp_not_migration_time() -> None:
    source = _state().to_dict()
    source["schemaVersion"] = "1.1.0"
    source["data"].pop("completedCorrelations")
    result = _cold(_migrate(source, require_known_outcomes=True))
    assert result.data.completed_correlations[CORRELATION] == {
        "completedAt": NOW.isoformat(),
        "outcome": "succeeded",
        "legacy": True,
    }
    assert result.data.response_mailbox == source["data"]["responseMailbox"]


def test_partial_legacy_transcript_is_not_proof_of_success() -> None:
    source: dict[str, Any] = {
        "schemaVersion": "1.1.0",
        "data": {
            "conversationHistory": [
                {
                    "$type": "response",
                    "correlationId": CORRELATION,
                    "createdAt": NOW.isoformat(),
                    "messages": [],
                }
            ]
        },
    }
    with pytest.raises(ValueError, match="outcome.*evidence"):
        _migrate(source, require_known_outcomes=True)
    compatible = _cold(_migrate(source))
    assert "outcome" not in compatible.data.completed_correlations[CORRELATION]
    compatible.expire_responses(now=NOW + timedelta(days=2))
    assert compatible.try_get_agent_response(CORRELATION).additional_properties["durable_outcome"] == "unknown"  # type: ignore[union-attr]


def test_strict_entity_import_rejects_unknown_without_any_write_or_model_call() -> None:
    source = DurableAgentState("1.1.0")
    source.data.completed_correlations[CORRELATION] = {"completedAt": NOW.isoformat()}
    provider = JsonStateProvider()
    client: Any = RecordingChatClient()
    entity = AgentEntity(Agent(client=client), state_provider=provider)
    request = {
        "source": source.to_dict(),
        "sourceDigest": state_snapshot_digest(source.to_dict()),
        "sourceSessionId": "original-session",
        "destinationSessionId": provider.core_session_id,
        "migrationId": "strict-import",
        "ownershipTransferId": "authorized-transfer",
        "requireKnownOutcomes": True,
    }
    with pytest.raises(ValueError, match="outcome.*evidence"):
        entity.migrate(request)
    assert provider.raw == {} and provider.writes == 0 and client.received_messages == []


@pytest.mark.parametrize("status", ["accepted", "already_completed"])
def test_new_completion_requires_outcome_not_an_acceptance_or_unavailable_status(status: str) -> None:
    state = DurableAgentState()
    response = AgentResponse(messages=[], additional_properties={"durable_status": status})
    before = state.to_dict()
    with pytest.raises(ValueError, match="completion.*outcome"):
        state.record_response(CORRELATION, response, delivery_window_seconds=WINDOW)
    assert state.to_dict() == before


@pytest.mark.parametrize("formatted", [False, True])
async def test_inner_acceptance_is_not_committed_as_success(formatted: bool) -> None:
    class AcceptanceAgent:
        name = "acceptance"
        calls = 0

        async def run(self, *, stream: bool = False, **kwargs: Any) -> AgentResponse:
            if stream:
                raise TypeError("stream is not supported")
            self.calls += 1
            return AgentResponse(
                messages=[Message("assistant", ["Request accepted"])] if formatted else [],
                response_format={"type": "object"} if formatted else None,
                additional_properties={"durable_status": "accepted"},
            )

    agent: Any = AcceptanceAgent()
    provider = JsonStateProvider()
    entity = AgentEntity(agent, state_provider=provider)
    with pytest.raises(ValueError, match="completion.*outcome"):
        await entity.run({"message": "work", "correlationId": CORRELATION})
    assert agent.calls == 1 and provider.raw == {} and provider.writes == 0
    assert entity.state.try_get_agent_response(CORRELATION) is None
