# Copyright (c) Microsoft. All rights reserved.

"""Outcome receipts across expiry, old-state handling and trusted migration boundaries."""

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, cast

import pytest
from _prototype_response_expectations import assert_shared_transport, expected_shared_transport
from agent_framework import Agent, AgentResponse, Content, Message
from clock_helpers import ClockDateTime
from test_durable_history_provider import RecordingChatClient
from test_revision_contract import JsonStateProvider
from typing_extensions import Self

from agent_framework_durabletask import AgentEntity, DurableAgentState, migrate_legacy_state, state_snapshot_digest
from agent_framework_durabletask import _delivery_state as delivery_module
from agent_framework_durabletask import _durable_agent_state as state_module
from agent_framework_durabletask import _shared_state_validation as validation_module
from agent_framework_durabletask._response_utils import invocation_outcome, serialize_agent_response

NOW = datetime(2026, 9, 11, 12, tzinfo=timezone.utc)
ORIGINAL_COMPLETED_AT = "2026-09-11T12:00:01.123456789Z"
WINDOW = 60
CORRELATION = "outcome-correlation"


class Clock(ClockDateTime):
    current = NOW

    @classmethod
    def now(cls, tz: Any = None) -> Self:
        return cls.fromtimestamp(cls.current.timestamp(), tz or timezone.utc)


@pytest.fixture(autouse=True)
def clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(Clock, "current", NOW)
    for module in (state_module, delivery_module, validation_module):
        monkeypatch.setattr(module, "datetime", Clock)


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
    receipt = {
        "correlationId": CORRELATION,
        "completedAt": NOW.isoformat(),
        "outcome": expected,
        "resultExpiresAt": (NOW + timedelta(seconds=WINDOW)).isoformat(),
        "resultState": "available",
    }
    assert state.data.completed_correlations[CORRELATION] == receipt
    before_delivery = state.to_json()
    expected_delivery = expected_shared_transport(response)
    if kind == "error-content":
        expected_delivery["messages"][0]["contents"][0]["error_code"] = "agent_error"
    elif kind == "error-status":
        expected_delivery["messages"].append(
            Message(
                "system",
                [Content.from_error(message="The agent invocation failed.", error_code="agent_error")],
            ).to_dict()
        )
    if expected == "failed":
        expected_delivery["additional_properties"] = {
            **response.additional_properties,
            "durable_status": "error",
            "correlation_id": CORRELATION,
        }
        assert state.data.response_mailbox[CORRELATION]["error"] == {
            "code": "agent_error",
            "message": "provider failed" if kind == "error-content" else "The agent invocation failed.",
        }
    Clock.current = NOW + timedelta(seconds=WINDOW, microseconds=-1)
    delivered = state.try_get_agent_response(CORRELATION)
    assert delivered is not None
    assert invocation_outcome(delivered) == expected
    # Only the consumer gains transport policy and missing authoritative failure fields.
    assert_shared_transport(delivered, expected_delivery)
    assert state.to_json() == before_delivery
    assert "_durable_value_policy" not in serialize_agent_response(response)

    Clock.current = NOW + timedelta(seconds=WINDOW)
    if cleanup:
        state.expire_responses(now=Clock.current)
        receipt.update(resultState="unavailable", resultUnavailableAt=Clock.current.isoformat())
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


@pytest.mark.parametrize("messages", [[], [{"role": "assistant", "contents": [{"$type": "text", "text": "partial"}]}]])
@pytest.mark.parametrize("cold", [False, True])
def test_authoritative_failed_result_delivers_failure_even_without_error_content(
    messages: list[Any], cold: bool
) -> None:
    raw = _state("error-content").to_dict()
    result = raw["data"]["terminalResults"][CORRELATION]
    result["response"] = {"messages": messages}
    result["error"] = {"code": "ProviderFailed", "message": "authoritative failure", "details": {"retry": False}}
    before = deepcopy(raw)
    state = DurableAgentState.from_dict(raw)
    if cold:
        state = _cold(state)
    expected_messages = [Message("assistant", ["partial"])] if messages else []
    expected_messages.append(
        Message(
            "system",
            [
                Content.from_error(
                    message="authoritative failure",
                    error_code="ProviderFailed",
                    error_details=cast(Any, {"retry": False}),
                )
            ],
        )
    )
    expected_delivery = expected_shared_transport(
        AgentResponse(
            messages=expected_messages,
            additional_properties={"durable_status": "error", "correlation_id": CORRELATION},
        )
    )
    delivered = state.try_get_agent_response(CORRELATION)
    assert delivered is not None and invocation_outcome(delivered) == "failed"
    assert_shared_transport(delivered, expected_delivery)
    assert delivered.additional_properties["durable_status"] == "error"
    errors = [content for message in delivered.messages for content in message.contents if content.type == "error"]
    assert len(errors) == 1
    assert errors[0].message == "authoritative failure" and errors[0].error_code == "ProviderFailed"
    assert errors[0].error_details == {"retry": False}
    if messages:
        assert delivered.messages[0].text == "partial"
    assert state.to_dict() == before and raw == before


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize("status", ["accepted", "already_completed"])
def test_available_projection_corrects_reserved_metadata_without_rewriting_original_profiles(
    outcome: str, status: str
) -> None:
    original = AgentResponse[Any](
        messages=[
            Message(
                "assistant",
                [
                    Content.from_error(
                        message="original failure",
                        error_code="provider_error",
                        error_details=cast(Any, {"keep": [None, False]}),
                    )
                ]
                if outcome == "failed"
                else [Content.from_text("original success")],
                message_id="original-message",
            )
        ],
        response_id="original-response",
        value={"answer": 0},
        additional_properties={"provider": {"keep": [None, False, 0]}},
    )
    state = DurableAgentState()
    state.record_response(CORRELATION, original, delivery_window_seconds=WINDOW, now=NOW)
    raw = state.to_dict()
    wire = raw["data"]["terminalResults"][CORRELATION]["response"]
    wire["extensionData"].update(
        durable_status=status,
        correlation_id="provider-correlation",
        durable_outcome="succeeded" if outcome == "failed" else "failed",
    )
    wire["futureResponse"] = {"keep": [None, False, 0]}
    wire["pythonCoreFields"] = {
        "profile": "agent-framework-python.core-fields",
        "version": 1,
        "fields": {"future_response": {"keep": [None, False, 0]}},
    }
    before = deepcopy(raw)
    restored = DurableAgentState.from_json(json.dumps(raw))
    expected = expected_shared_transport(original)
    expected["additional_properties"].update(
        correlation_id=CORRELATION if outcome == "failed" else "provider-correlation",
        durable_outcome=outcome,
    )
    if outcome == "failed":
        expected["additional_properties"]["durable_status"] = "error"
    delivered = restored.try_get_agent_response(CORRELATION)
    assert delivered is not None
    assert_shared_transport(delivered, expected)
    assert invocation_outcome(delivered) == outcome
    assert restored.to_dict() == before and raw == before


@pytest.mark.parametrize("cold", [False, True])
@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
async def test_unavailable_known_receipt_suppresses_execution_and_survives_reset(cold: bool, outcome: str) -> None:
    state = DurableAgentState()
    state.data.completed_correlations[CORRELATION] = {
        "correlationId": CORRELATION,
        "completedAt": NOW.isoformat(),
        "outcome": outcome,
        "resultState": "unavailable",
        "resultUnavailableAt": NOW.isoformat(),
        "future": {"keep": True},
    }
    original = deepcopy(state.to_dict())
    client: Any = RecordingChatClient()
    provider = JsonStateProvider(_cold(state).to_dict() if cold else state.to_dict())
    entity = AgentEntity(Agent(client=client), state_provider=provider)
    response = await entity.run({"message": "never rerun", "correlationId": CORRELATION})
    assert response.additional_properties["durable_outcome"] == outcome
    assert response.additional_properties["durable_status"] == "already_completed"
    assert client.received_messages == [] and provider.writes == 0 and provider.raw == original
    entity.reset()
    assert _cold(entity.state).data.completed_correlations == state.data.completed_correlations
    assert entity.state.data.response_mailbox == {}


@pytest.mark.parametrize("invalid", [None, "", "success", "FAILED", "unknown", 1, False, [], {}])
def test_invalid_present_outcome_is_rejected_at_read_and_warm_boundaries(invalid: Any) -> None:
    state = _state()
    raw = state.to_dict()
    state.data.completed_correlations[CORRELATION]["outcome"] = invalid
    raw["data"]["completionReceipts"][CORRELATION]["outcome"] = invalid
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


@pytest.mark.parametrize("boundary", ["serialize", "prepare", "lookup", "cleanup", "backfill", "record"])
def test_missing_outcome_is_not_repaired_at_warm_boundaries(boundary: str) -> None:
    state = _state()
    del state.data.completed_correlations[CORRELATION]["outcome"]
    before = deepcopy((state.data.response_mailbox, state.data.completed_correlations))
    with pytest.raises(ValueError, match="outcome"):
        if boundary == "serialize":
            state.to_dict()
        elif boundary == "prepare":
            state.prepare_for_write(delivery_window_seconds=WINDOW)
        elif boundary == "lookup":
            state.try_get_agent_response(CORRELATION)
        elif boundary == "cleanup":
            state.expire_responses(now=NOW + timedelta(days=1))
        elif boundary == "backfill":
            state._backfill_completion_outcomes()
        else:
            state.record_response("new", _response("success"), delivery_window_seconds=WINDOW)
    assert (state.data.response_mailbox, state.data.completed_correlations) == before


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


def _legacy_source(kind: str = "response", *, messages: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "schemaVersion": "1.1.0",
        "data": {
            "conversationHistory": [
                {
                    "$type": kind,
                    "correlationId": CORRELATION,
                    "createdAt": NOW.isoformat(),
                    "messages": [] if messages is None else messages,
                }
            ],
        },
    }


def _original_failure(*, messages: list[dict[str, Any]]) -> dict[str, Any]:
    # Fixture ground truth from the original invocation, not inferred from its retained transcript.
    return {
        "correlationId": CORRELATION,
        "outcome": "failed",
        "completedAt": ORIGINAL_COMPLETED_AT,
        "response": {"createdAt": NOW.isoformat(), "messages": deepcopy(messages)},
        "error": {"code": "provider_error", "message": "Original provider invocation failed."},
    }


def _completions(source: dict[str, Any], *results: dict[str, Any]) -> dict[str, Any]:
    return {
        "sourceDigest": state_snapshot_digest(source),
        "evidenceId": "outcome-completion-journal",
        "complete": True,
        "results": deepcopy(list(results)),
    }


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("kind", ["errorResponse", "response"])
def test_legacy_failure_import_preserves_original_completion_and_new_delivery_grace(strict: bool, kind: str) -> None:
    source = _legacy_source(
        kind,
        messages=[{"role": "assistant", "contents": [{"$type": "error", "message": "provider failed"}]}],
    )
    before = deepcopy(source)
    original = _original_failure(
        messages=[
            {
                "role": "assistant",
                "contents": [{"$type": "error", "message": "Original provider invocation failed."}],
            }
        ]
    )
    evidence = _completions(source, original)
    before_evidence = deepcopy(evidence)
    result = _cold(_migrate(source, require_known_outcomes=strict, completion_evidence=evidence))
    expires_at = (NOW + timedelta(days=1, seconds=WINDOW)).isoformat()
    assert result.data.completed_correlations[CORRELATION] == {
        "correlationId": CORRELATION,
        "completedAt": ORIGINAL_COMPLETED_AT,
        "outcome": "failed",
        "resultExpiresAt": expires_at,
        "resultState": "available",
    }
    terminal = result.data.response_mailbox[CORRELATION]
    assert terminal == {**original, "resultExpiresAt": expires_at}
    assert terminal["response"]["createdAt"] == NOW.isoformat()
    assert result.to_dict()["data"]["conversationHistory"] == before["data"]["conversationHistory"]
    assert source == before and evidence == before_evidence


@pytest.mark.parametrize("strict", [False, True])
def test_contentless_legacy_error_response_import_uses_original_failed_result(strict: bool) -> None:
    source = _legacy_source("errorResponse")
    original = _original_failure(messages=[])
    evidence = _completions(source, original)
    before_source, before_evidence = deepcopy(source), deepcopy(evidence)
    result = _cold(_migrate(source, require_known_outcomes=strict, completion_evidence=evidence))
    assert result.data.completed_correlations[CORRELATION]["outcome"] == "failed"
    assert result.data.completed_correlations[CORRELATION]["completedAt"] == ORIGINAL_COMPLETED_AT
    assert result.data.response_mailbox[CORRELATION] == {
        **original,
        "resultExpiresAt": (NOW + timedelta(days=1, seconds=WINDOW)).isoformat(),
    }
    response = result.try_get_agent_response(CORRELATION)
    assert response is not None and invocation_outcome(response) == "failed"
    assert source == before_source and evidence == before_evidence


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("text", [None, "", "plausible success"])
def test_partial_legacy_transcript_is_not_proof_of_success(strict: bool, text: str | None) -> None:
    messages: list[dict[str, Any]] = (
        [] if text is None else [{"role": "assistant", "contents": [{"$type": "text", "text": text}]}]
    )
    source = _legacy_source(messages=messages)
    before = deepcopy(source)
    with pytest.raises(ValueError, match="outcome.*evidence"):
        _migrate(source, require_known_outcomes=strict)
    assert source == before


@pytest.mark.parametrize("strict", [False, True])
def test_entity_import_rejects_ambiguous_legacy_without_any_write_or_model_call(strict: bool) -> None:
    source = _legacy_source()
    provider = JsonStateProvider()
    client: Any = RecordingChatClient()
    entity = AgentEntity(Agent(client=client), state_provider=provider)
    request = {
        "source": source,
        "sourceDigest": state_snapshot_digest(source),
        "sourceSessionId": "original-session",
        "destinationSessionId": provider.core_session_id,
        "migrationId": "strict-import",
        "ownershipTransferId": "authorized-transfer",
        "requireKnownOutcomes": strict,
    }
    with pytest.raises(ValueError, match="outcome.*evidence"):
        entity.migrate(request)
    assert provider.raw == {} and provider.writes == 0 and client.received_messages == []


@pytest.mark.parametrize("status", ["accepted", "already_completed"])
@pytest.mark.parametrize("outcome", [None, "succeeded", "failed"])
def test_new_completion_requires_outcome_not_an_acceptance_or_unavailable_status(
    status: str, outcome: str | None
) -> None:
    state = DurableAgentState()
    properties = {"durable_status": status}
    if outcome is not None:
        properties["durable_outcome"] = outcome
    response = AgentResponse(messages=[], additional_properties=properties)
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
