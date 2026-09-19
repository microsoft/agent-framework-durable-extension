# Copyright (c) Microsoft. All rights reserved.

"""Conflicting completion evidence must fail before delivery, cleanup or import."""

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, get_args, get_origin, get_type_hints

import pytest
from _prototype_response_expectations import assert_shared_transport, expected_shared_transport
from agent_framework import Agent, AgentResponse, Content, Message
from jsonschema import Draft202012Validator, FormatChecker
from test_durable_history_provider import RecordingChatClient
from test_revision_contract import JsonStateProvider

from agent_framework_durabletask import AgentEntity, DurableAgentState, migrate_legacy_state, state_snapshot_digest
from agent_framework_durabletask._response_utils import invocation_outcome

CORRELATION = "completion-under-review"
OLD = datetime(2024, 1, 1, tzinfo=timezone.utc)
CONFLICT = "agree|conflicts"


def _response(outcome: str) -> AgentResponse:
    if outcome == "failed":
        return AgentResponse(messages=[Message("assistant", [Content.from_error(message="original failure")])])
    return AgentResponse(messages=[Message("assistant", ["original success"])])


def _snapshot(outcome: str = "succeeded", *, expired: bool = False) -> dict[str, Any]:
    state = DurableAgentState()
    # An earlier due result catches partial cleanup before a later conflict.
    state.record_response("earlier", _response("succeeded"), delivery_window_seconds=60, now=OLD)
    state.record_response(
        CORRELATION,
        _response(outcome),
        delivery_window_seconds=3600,
        now=OLD if expired else datetime.now(timezone.utc),
    )
    state.data.completed_correlations[CORRELATION]["opaque"] = {"keep": [0, False, None]}
    return state.to_dict()


def _conflict(raw: dict[str, Any]) -> None:
    receipt = raw["data"]["completionReceipts"][CORRELATION]
    receipt["outcome"] = "failed" if receipt["outcome"] == "succeeded" else "succeeded"


def _evidence(state: DurableAgentState) -> tuple[dict[str, Any], dict[str, Any]]:
    return deepcopy(state.data.response_mailbox), deepcopy(state.data.completed_correlations)


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize("expired", [False, True])
def test_cold_read_rejects_opposite_recorded_outcomes(outcome: str, expired: bool) -> None:
    raw = _snapshot(outcome, expired=expired)
    _conflict(raw)
    before = deepcopy(raw)
    with pytest.raises(ValueError, match=CONFLICT):
        DurableAgentState.from_dict(raw)
    with pytest.raises(ValueError, match=CONFLICT):
        DurableAgentState.from_json(json.dumps(raw))
    assert raw == before


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize(
    "boundary", ["serialize", "prepare", "lookup", "other-lookup", "expire", "backfill", "duplicate", "record"]
)
def test_warm_boundary_rejects_conflict_without_mutating_any_delivery_record(outcome: str, boundary: str) -> None:
    raw = _snapshot(outcome, expired=True)
    state = DurableAgentState.from_dict(raw)
    _conflict(raw)
    state.data.completed_correlations = deepcopy(raw["data"]["completionReceipts"])
    before = _evidence(state)
    with pytest.raises(ValueError, match=CONFLICT):
        if boundary == "serialize":
            state.to_dict()
        elif boundary == "prepare":
            state.prepare_for_write(delivery_window_seconds=60)
        elif boundary in ("lookup", "other-lookup"):
            state.try_get_agent_response(CORRELATION if boundary == "lookup" else "unrelated")
        elif boundary == "expire":
            state.expire_responses(now=datetime.now(timezone.utc))
        elif boundary == "backfill":
            state._backfill_completion_outcomes()
        else:
            state.record_response(
                CORRELATION if boundary == "duplicate" else "new",
                _response(outcome),
                delivery_window_seconds=60,
            )
    assert _evidence(state) == before


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize("operation", ["run", "reset", "expire"])
async def test_entity_warm_conflict_prevents_execution_and_state_writes(outcome: str, operation: str) -> None:
    raw = _snapshot(outcome, expired=True)
    provider = JsonStateProvider(raw)
    client: Any = RecordingChatClient()
    entity = AgentEntity(Agent(client=client), state_provider=provider)
    original = entity.state
    corrupt = deepcopy(raw)
    _conflict(corrupt)
    original.data.completed_correlations = corrupt["data"]["completionReceipts"]
    before = _evidence(original)
    with pytest.raises(ValueError, match=CONFLICT):
        if operation == "run":
            await entity.run({"message": "must not run", "correlationId": "new"})
        elif operation == "reset":
            entity.reset()
        else:
            entity.expire_responses()
    assert entity.state is original and _evidence(original) == before
    assert provider.raw == raw and provider.writes == 0 and client.received_messages == []


def _migration_request(source: dict[str, Any], *, strict: bool) -> dict[str, Any]:
    return {
        "source": source,
        "sourceDigest": state_snapshot_digest(source),
        "sourceSessionId": "source-session",
        "destinationSessionId": "revision-session",
        "migrationId": "consistency-import",
        "ownershipTransferId": "authorized-transfer",
        "requireKnownOutcomes": strict,
    }


def _legacy_source(*, failed: bool) -> dict[str, Any]:
    return {
        "schemaVersion": "1.1.0",
        "data": {
            "conversationHistory": [
                {
                    "$type": "errorResponse" if failed else "response",
                    "correlationId": CORRELATION,
                    "createdAt": OLD.isoformat(),
                    "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "retained portion"}]}],
                }
            ],
        },
    }


@pytest.mark.parametrize("strict", [False, True])
def test_both_import_modes_reject_ambiguous_legacy_completion_without_commit(strict: bool) -> None:
    source = _legacy_source(failed=False)
    before = deepcopy(source)
    with pytest.raises(ValueError, match="outcome.*evidence"):
        migrate_legacy_state(
            source,
            source_digest=state_snapshot_digest(source),
            source_session_id="source-session",
            migration_id="consistency-import",
            ownership_transfer_id="authorized-transfer",
            delivery_window_seconds=60,
            require_known_outcomes=strict,
        )
    provider = JsonStateProvider()
    client: Any = RecordingChatClient()
    entity = AgentEntity(Agent(client=client), state_provider=provider)
    with pytest.raises(ValueError, match="outcome.*evidence"):
        entity.migrate(_migration_request(source, strict=strict))
    assert source == before
    assert provider.raw == {} and provider.writes == 0 and client.received_messages == []


def test_idempotent_migration_retry_validates_warm_destination() -> None:
    source = _legacy_source(failed=True)
    request = _migration_request(source, strict=True)
    original_result = {
        "correlationId": CORRELATION,
        "outcome": "failed",
        "completedAt": "2024-01-02T03:04:05.123456789Z",
        "response": {
            "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "original partial result"}]}],
        },
        "error": {"code": "provider_error", "message": "Original invocation failed."},
    }
    request["completionEvidence"] = {
        "sourceDigest": state_snapshot_digest(source),
        "evidenceId": "consistency-completion-journal",
        "complete": True,
        "results": [deepcopy(original_result)],
    }
    before_request = deepcopy(request)
    provider = JsonStateProvider()
    client: Any = RecordingChatClient()
    entity = AgentEntity(Agent(client=client), state_provider=provider)
    expected = entity.migrate(request)
    committed = deepcopy(provider.raw)
    result = committed["data"]["terminalResults"][CORRELATION]
    assert result == {**original_result, "resultExpiresAt": result["resultExpiresAt"]}
    assert committed["data"]["completionReceipts"][CORRELATION]["completedAt"] == original_result["completedAt"]
    assert entity.migrate(request) == expected and provider.writes == 1
    assert provider.raw == committed and request == before_request
    corrupt = deepcopy(committed)
    _conflict(corrupt)
    original = entity.state
    original.data.completed_correlations = corrupt["data"]["completionReceipts"]
    warm_before = _evidence(original)
    with pytest.raises(ValueError, match=CONFLICT):
        entity.migrate(request)
    assert entity.state is original and _evidence(original) == warm_before
    assert provider.raw == committed and provider.writes == 1
    assert client.received_messages == []
    assert request == before_request


@pytest.mark.parametrize("field", ["completedAt", "resultExpiresAt"])
def test_mismatched_timestamp_instants_are_rejected(field: str) -> None:
    raw = _snapshot(expired=True)
    raw["data"]["completionReceipts"][CORRELATION][field] = "2024-01-01T00:00:01Z"
    before = deepcopy(raw)
    with pytest.raises(ValueError, match="agree"):
        DurableAgentState.from_json(json.dumps(raw))
    assert raw == before


@pytest.mark.parametrize("container", ["terminalResults", "completionReceipts"])
def test_optional_expiry_presence_must_agree(container: str) -> None:
    raw = _snapshot()
    del raw["data"][container][CORRELATION]["resultExpiresAt"]
    with pytest.raises(ValueError, match="agree"):
        DurableAgentState.from_dict(raw)


@pytest.mark.parametrize("expiry", [False, True])
def test_equal_timestamp_instants_preserve_original_offsets_and_precision(expiry: bool) -> None:
    raw = _snapshot(expired=True)
    result = raw["data"]["terminalResults"][CORRELATION]
    receipt = raw["data"]["completionReceipts"][CORRELATION]
    result["completedAt"] = "2024-01-01T00:00:00.123456789Z"
    receipt["completedAt"] = "2024-01-01T05:30:00.1234567890+05:30"
    if expiry:
        result["resultExpiresAt"] = "2024-01-01T01:00:00.123456789Z"
        receipt["resultExpiresAt"] = "2024-01-01T06:30:00.1234567890+05:30"
    else:
        del result["resultExpiresAt"]
        del receipt["resultExpiresAt"]
    assert DurableAgentState.from_json(json.dumps(raw)).to_dict() == raw


@pytest.mark.parametrize("field", ["completedAt", "resultExpiresAt"])
def test_distinct_submicrosecond_instants_do_not_compare_equal(field: str) -> None:
    raw = _snapshot(expired=True)
    hour = "00" if field == "completedAt" else "01"
    raw["data"]["terminalResults"][CORRELATION][field] = f"2024-01-01T{hour}:00:00.123456781Z"
    raw["data"]["completionReceipts"][CORRELATION][field] = f"2024-01-01T{hour}:00:00.123456782Z"
    with pytest.raises(ValueError, match="agree"):
        DurableAgentState.from_dict(raw)


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
def test_duplicate_record_does_not_reclassify_the_replacement_argument(outcome: str) -> None:
    state = DurableAgentState.from_dict(_snapshot(outcome))
    original = state.to_json()
    replacement = AgentResponse(messages=[Message("assistant", ["not JSON"])], response_format={"type": "object"})
    state.record_response(CORRELATION, replacement, delivery_window_seconds=999)
    assert state.to_json() == original
    assert replacement._value_parsed is False
    delivered = state.try_get_agent_response(CORRELATION)
    assert delivered is not None and invocation_outcome(delivered) == outcome
    expected = expected_shared_transport(_response(outcome))
    if outcome == "failed":
        expected["messages"][0]["contents"][0]["error_code"] = "agent_error"
        expected["additional_properties"] = {"durable_status": "error", "correlation_id": CORRELATION}
        assert state.data.response_mailbox[CORRELATION]["error"] == {
            "code": "agent_error",
            "message": "original failure",
        }
    assert_shared_transport(delivered, expected)
    assert state.to_json() == original


@pytest.mark.parametrize("kind", ["tool-error", "approval", "status-error"])
def test_consistency_uses_invocation_semantics_not_any_error_or_pending_action(kind: str) -> None:
    if kind == "tool-error":
        response = AgentResponse(
            messages=[
                Message("tool", [Content.from_error(message="recovered error")]),
                Message("assistant", ["answer"]),
            ]
        )
    elif kind == "approval":
        response = AgentResponse(
            messages=[
                Message(
                    "assistant",
                    [Content.from_function_approval_request("approval", Content.from_function_call("call", "tool"))],
                )
            ]
        )
    else:
        response = AgentResponse(messages=[], additional_properties={"durable_status": "error"})
    state = DurableAgentState()
    state.record_response(CORRELATION, response, delivery_window_seconds=60)
    raw = state.to_dict()
    assert DurableAgentState.from_json(json.dumps(raw)).to_dict() == raw
    _conflict(raw)
    with pytest.raises(ValueError, match=CONFLICT):
        DurableAgentState.from_dict(raw)


@pytest.mark.parametrize("kind", ["content", "status"])
def test_matching_success_maps_do_not_hide_affirmative_response_failure(kind: str) -> None:
    raw = _snapshot()
    raw["data"]["terminalResults"][CORRELATION]["response"] = (
        {"messages": [{"role": "assistant", "contents": [{"$type": "error", "message": "provider failed"}]}]}
        if kind == "content"
        else {"messages": [], "extensionData": {"durable_status": "error"}}
    )
    before = deepcopy(raw)
    with pytest.raises(ValueError, match="conflicts"):
        DurableAgentState.from_json(json.dumps(raw))
    assert raw == before


@pytest.fixture(scope="module")
def shared_schema() -> dict[str, Any]:
    path = Path(__file__).resolve().parents[4] / "schemas" / "durable-agent-entity-state.json"
    return json.loads(path.read_text(encoding="utf-8"))


def test_shared_schema_requires_exact_runtime_outcomes(shared_schema: dict[str, Any]) -> None:
    definition = shared_schema["$defs"]["completionReceipt"]
    outcome = definition["properties"]["outcome"]
    runtime_outcomes = {
        value
        for branch in get_args(get_type_hints(invocation_outcome)["return"])
        if get_origin(branch) is Literal
        for value in get_args(branch)
    }
    assert outcome["type"] == "string" and set(outcome["enum"]) == runtime_outcomes
    assert "outcome" in definition["required"]


@pytest.mark.parametrize("value", [None, "unknown", "", "success", "FAILED", False, 0, [], {}])
def test_shared_schema_rejects_invalid_present_outcomes(shared_schema: dict[str, Any], value: Any) -> None:
    raw = _snapshot()
    raw["data"]["completionReceipts"][CORRELATION]["outcome"] = value
    assert not Draft202012Validator(shared_schema, format_checker=FormatChecker()).is_valid(raw)
    with pytest.raises(ValueError, match="outcome"):
        DurableAgentState.from_dict(raw)


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
def test_shared_schema_and_runtime_preserve_unavailable_known_receipts(
    shared_schema: dict[str, Any], outcome: str
) -> None:
    state = DurableAgentState.from_dict(_snapshot(outcome, expired=True))
    state.expire_responses(now=datetime.now(timezone.utc))
    raw = state.to_dict()
    assert raw["data"]["terminalResults"] == {}
    assert raw["data"]["completionReceipts"][CORRELATION]["outcome"] == outcome
    Draft202012Validator(shared_schema, format_checker=FormatChecker()).validate(raw)
    assert DurableAgentState.from_json(json.dumps(raw)).to_dict() == raw


def test_missing_outcome_is_rejected_not_backfilled_from_a_retained_result(shared_schema: dict[str, Any]) -> None:
    raw = _snapshot()
    del raw["data"]["completionReceipts"][CORRELATION]["outcome"]
    assert not Draft202012Validator(shared_schema, format_checker=FormatChecker()).is_valid(raw)
    with pytest.raises(ValueError, match="outcome"):
        DurableAgentState.from_dict(raw)


@pytest.mark.parametrize(
    "change",
    [
        "result-identity",
        "receipt-identity",
        "missing-receipt",
        "missing-result",
        "unavailable-with-result",
        "unavailable-missing-time",
        "unavailable-before-expiry",
        "available-with-unavailable-time",
        "success-with-error",
        "failure-without-error",
        "failure-with-malformed-error",
    ],
)
@pytest.mark.parametrize("warm", [False, True])
def test_malformed_shared_evidence_rejects_before_any_cleanup(change: str, warm: bool) -> None:
    raw = _snapshot(expired=True)
    state = DurableAgentState.from_dict(raw)
    results, receipts = raw["data"]["terminalResults"], raw["data"]["completionReceipts"]
    result, receipt = results[CORRELATION], receipts[CORRELATION]
    if change == "result-identity":
        result["correlationId"] = CORRELATION.upper()
    elif change == "receipt-identity":
        receipt["correlationId"] = CORRELATION.upper()
    elif change == "missing-receipt":
        del receipts[CORRELATION]
    elif change == "missing-result":
        del results[CORRELATION]
    elif change.startswith("unavailable-"):
        receipt["resultState"] = "unavailable"
        if change != "unavailable-with-result":
            del results[CORRELATION]
        if change != "unavailable-missing-time":
            receipt["resultUnavailableAt"] = (
                "2024-01-01T00:00:01Z" if change == "unavailable-before-expiry" else "2024-01-01T01:00:01Z"
            )
    elif change == "available-with-unavailable-time":
        receipt["resultUnavailableAt"] = "2024-01-01T01:00:01Z"
    elif change == "success-with-error":
        result["error"] = {"code": "ProviderFailed", "message": "failure"}
    else:
        result["outcome"] = receipt["outcome"] = "failed"
        if change == "failure-with-malformed-error":
            result["error"] = {"code": False, "message": "failure"}
    before = deepcopy(raw)
    if warm:
        state.data.response_mailbox = deepcopy(results)
        state.data.completed_correlations = deepcopy(receipts)
        evidence = _evidence(state)
        with pytest.raises(ValueError):
            state.expire_responses(now=datetime.now(timezone.utc))
        assert _evidence(state) == evidence
    else:
        with pytest.raises(ValueError):
            DurableAgentState.from_json(json.dumps(raw))
    assert raw == before
