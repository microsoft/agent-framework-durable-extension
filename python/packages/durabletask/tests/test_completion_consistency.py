# Copyright (c) Microsoft. All rights reserved.

"""Conflicting completion evidence must fail before delivery, cleanup or import."""

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, get_args, get_origin, get_type_hints

import pytest
from agent_framework import Agent, AgentResponse, Content, Message
from jsonschema import Draft202012Validator, FormatChecker
from test_durable_history_provider import RecordingChatClient
from test_revision_contract import JsonStateProvider

from agent_framework_durabletask import AgentEntity, DurableAgentState, migrate_legacy_state, state_snapshot_digest
from agent_framework_durabletask._response_utils import invocation_outcome, serialize_agent_response

CORRELATION = "completion-under-review"
OLD = datetime(2024, 1, 1, tzinfo=timezone.utc)
CONFLICT = "outcome.*conflicts"


def _response(outcome: str) -> AgentResponse:
    if outcome == "failed":
        return AgentResponse(messages=[Message("assistant", [Content.from_error(message="original failure")])])
    return AgentResponse(messages=[Message("assistant", ["original success"])])


def _snapshot(outcome: str = "succeeded", *, expired: bool = False, legacy: bool = False) -> dict[str, Any]:
    state = DurableAgentState()
    # An earlier, eligible record catches partial expiry/backfill before a later conflict.
    state.record_response("earlier", _response("succeeded"), delivery_window_seconds=60, now=OLD)
    state.data.completed_correlations["earlier"].pop("outcome")
    state.record_response(
        CORRELATION,
        _response(outcome),
        delivery_window_seconds=3600,
        now=OLD if expired else datetime.now(timezone.utc),
    )
    state.data.completed_correlations[CORRELATION]["opaque"] = {"keep": [0, False, None]}
    if legacy:
        state.data.completed_correlations[CORRELATION]["legacy"] = True
    return state.to_dict()


def _conflict(raw: dict[str, Any]) -> None:
    receipt = raw["data"]["completedCorrelations"][CORRELATION]
    receipt["outcome"] = "failed" if receipt["outcome"] == "succeeded" else "succeeded"


def _evidence(state: DurableAgentState) -> tuple[dict[str, Any], dict[str, Any]]:
    return deepcopy(state.data.response_mailbox), deepcopy(state.data.completed_correlations)


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize("version", ["1.1.0", "2.0.0", "2.7.3"])
@pytest.mark.parametrize("expired", [False, True])
def test_cold_read_rejects_opposite_recorded_outcomes(outcome: str, version: str, expired: bool) -> None:
    raw = _snapshot(outcome, expired=expired)
    raw["schemaVersion"] = version
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
    state.data.completed_correlations = deepcopy(raw["data"]["completedCorrelations"])
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
    original.data.completed_correlations = corrupt["data"]["completedCorrelations"]
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


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("expired", [False, True])
def test_both_import_modes_reject_conflicting_source_without_commit(outcome: str, strict: bool, expired: bool) -> None:
    source = _snapshot(outcome, expired=expired)
    source["schemaVersion"] = "1.1.0"
    _conflict(source)
    before = deepcopy(source)
    with pytest.raises(ValueError, match=CONFLICT):
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
    with pytest.raises(ValueError, match=CONFLICT):
        entity.migrate(_migration_request(source, strict=strict))
    assert source == before
    assert provider.raw == {} and provider.writes == 0 and client.received_messages == []


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
def test_idempotent_migration_retry_validates_warm_destination(outcome: str) -> None:
    source = _snapshot(outcome)
    source["schemaVersion"] = "1.1.0"
    request = _migration_request(source, strict=True)
    provider = JsonStateProvider()
    client: Any = RecordingChatClient()
    entity = AgentEntity(Agent(client=client), state_provider=provider)
    expected = entity.migrate(request)
    assert entity.migrate(request) == expected and provider.writes == 1
    committed = deepcopy(provider.raw)
    corrupt = deepcopy(committed)
    _conflict(corrupt)
    original = entity.state
    original.data.completed_correlations = corrupt["data"]["completedCorrelations"]
    warm_before = _evidence(original)
    with pytest.raises(ValueError, match=CONFLICT):
        entity.migrate(request)
    assert entity.state is original and _evidence(original) == warm_before
    assert provider.raw == committed and provider.writes == 1
    assert client.received_messages == []


@pytest.mark.parametrize("receipt_outcome", [None, "succeeded", "failed"])
def test_ambiguous_legacy_text_never_overrides_or_invents_outcome(receipt_outcome: str | None) -> None:
    raw = _snapshot(legacy=True)
    receipt = raw["data"]["completedCorrelations"][CORRELATION]
    if receipt_outcome is None:
        receipt.pop("outcome")
    else:
        receipt["outcome"] = receipt_outcome
    # Historic migration timestamps may differ from an independently retained mailbox.
    receipt["completedAt"] = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    state = DurableAgentState.from_json(json.dumps(raw))
    assert state.to_dict() == raw
    expected_receipt = deepcopy(receipt)
    state.prepare_for_write(delivery_window_seconds=60)
    state.expire_responses(now=datetime.max.replace(tzinfo=timezone.utc))
    restored = DurableAgentState.from_json(state.to_json())
    expired = restored.try_get_agent_response(CORRELATION)
    assert expired is not None
    assert expired.additional_properties["durable_outcome"] == (receipt_outcome or "unknown")
    assert restored.data.completed_correlations[CORRELATION] == expected_receipt


def test_legacy_marker_does_not_hide_affirmative_failure_evidence() -> None:
    raw = _snapshot("failed", legacy=True)
    _conflict(raw)
    with pytest.raises(ValueError, match=CONFLICT):
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
    assert delivered is not None and serialize_agent_response(delivered) == serialize_agent_response(_response(outcome))


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


@pytest.fixture(scope="module")
def private_schema() -> dict[str, Any]:
    path = Path(__file__).resolve().parents[4] / "schemas" / "durable-agent-entity-state.json"
    return json.loads(path.read_text(encoding="utf-8"))


def test_private_schema_declares_the_runtime_outcomes_as_optional(private_schema: dict[str, Any]) -> None:
    definition = private_schema["$defs"]["completedCorrelation"]
    outcome = definition["properties"]["outcome"]
    runtime_outcomes = {
        value
        for branch in get_args(get_type_hints(invocation_outcome)["return"])
        if get_origin(branch) is Literal
        for value in get_args(branch)
    }
    assert outcome["type"] == "string" and set(outcome["enum"]) == runtime_outcomes
    assert "outcome" not in definition["required"]


@pytest.mark.parametrize("value", [None, "unknown", "", "success", "FAILED", False, 0, [], {}])
def test_private_schema_rejects_invalid_present_outcomes(private_schema: dict[str, Any], value: Any) -> None:
    raw = _snapshot()
    raw["data"].pop("responseMailbox")
    raw["data"]["completedCorrelations"][CORRELATION]["outcome"] = value
    assert not Draft202012Validator(private_schema, format_checker=FormatChecker()).is_valid(raw)
    with pytest.raises(ValueError, match="outcome"):
        DurableAgentState.from_dict(raw)


@pytest.mark.parametrize("outcome", [None, "succeeded", "failed"])
def test_private_schema_and_runtime_preserve_outcomeless_or_known_receipts(
    private_schema: dict[str, Any], outcome: str | None
) -> None:
    raw = _snapshot()
    raw["data"].pop("responseMailbox")
    receipt = raw["data"]["completedCorrelations"][CORRELATION]
    if outcome is None:
        receipt.pop("outcome")
    else:
        receipt["outcome"] = outcome
    Draft202012Validator(private_schema, format_checker=FormatChecker()).validate(raw)
    assert DurableAgentState.from_json(json.dumps(raw)).to_dict() == raw
