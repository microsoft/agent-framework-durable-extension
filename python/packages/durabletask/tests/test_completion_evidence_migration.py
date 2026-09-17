# Copyright (c) Microsoft. All rights reserved.

# ruff: noqa: D, INP001, S101
"""Original completion journals, not transcript projections, authorize legacy adoption."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from agent_framework import Message

from agent_framework_durabletask._durable_agent_state import DurableAgentState, DurableAgentStateEntryJsonType
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._retention import StateCapacityError
from agent_framework_durabletask._shared_state_validation import validate_shared_state
from agent_framework_durabletask._state_migration import migrate_legacy_state, state_snapshot_digest
from agent_framework_durabletask._workflows.naming import workflow_message_id

NOW = datetime(2026, 9, 16, 12, tzinfo=timezone.utc)
OLD = "2024-01-02T03:04:05.123456789+05:30"
TRANSCRIPT_TIME = "2023-01-01T00:00:00Z"
WINDOW = 60
SESSION = "dafx-agent:original-session"


def _source(*history: dict[str, Any]) -> dict[str, Any]:
    return {"schemaVersion": "1.1.0", "data": {"conversationHistory": deepcopy(list(history))}}


def _entry(kind: str = "response", correlation: str = "done") -> dict[str, Any]:
    return {
        "$type": kind,
        **({"correlationId": correlation} if kind != "compaction" else {}),
        "createdAt": TRANSCRIPT_TIME,
        "messages": [{"role": "user" if kind == "request" else "assistant", "contents": []}],
    }


def _result(correlation: str = "done", outcome: str = "succeeded") -> dict[str, Any]:
    return {
        "correlationId": correlation,
        "outcome": outcome,
        "completedAt": OLD,
        "response": {"messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "original"}]}]},
        **({"error": {"code": "provider_error", "message": "The invocation failed."}} if outcome == "failed" else {}),
    }


def _journal(source: dict[str, Any], *results: dict[str, Any]) -> dict[str, Any]:
    return {
        "sourceDigest": state_snapshot_digest(source),
        "evidenceId": "completion-journal-1",
        "complete": True,
        "results": deepcopy(list(results)),
    }


def _delivery(source: dict[str, Any], *messages: Message) -> dict[str, Any]:
    return {
        "sourceDigest": state_snapshot_digest(source),
        "evidenceId": "delivery-journal-1",
        "complete": True,
        "messages": [message.to_dict() for message in messages],
    }


def _migrate(source: dict[str, Any], **overrides: Any) -> DurableAgentState:
    options: dict[str, Any] = {
        "source_digest": state_snapshot_digest(source),
        "source_session_id": SESSION,
        "migration_id": "migration-1",
        "ownership_transfer_id": "transfer-1",
        "delivery_window_seconds": WINDOW,
        "now": NOW,
    }
    options.update(overrides)
    return migrate_legacy_state(source, **options)


def _cold(state: DurableAgentState) -> DurableAgentState:
    raw = state.to_dict()
    validate_shared_state(raw)
    restored = DurableAgentState.from_json(json.dumps(raw, allow_nan=False))
    assert restored.to_dict() == raw
    return restored


def _assert_rejected(source: dict[str, Any], evidence: Any, **options: Any) -> None:
    # Assign only after staging succeeds, as the parent's atomic commit must do.
    destination = DurableAgentState()
    destination.unknown_fields["keep"] = {"whole": [None, False, 0]}
    target_before = destination.to_dict()
    before = deepcopy(source)
    evidence_before = deepcopy(evidence)
    options_before = deepcopy(options)
    with pytest.raises((ValueError, StateCapacityError)):
        destination = _migrate(source, completion_evidence=evidence, **options)
    assert source == before
    assert evidence == evidence_before
    assert options == options_before
    assert destination.to_dict() == target_before


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize("strict", [False, True])
def test_original_completion_time_and_shared_result_survive_new_grace(
    version: str, outcome: str, strict: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _source(_entry("errorResponse" if outcome == "failed" else "response"))
    source["schemaVersion"] = version
    original = _result(outcome=outcome)
    evidence = _journal(source, original)
    before, evidence_before = deepcopy(source), deepcopy(evidence)

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("Migration must not record a new completion or derive a result through Core.")

    monkeypatch.setattr(DurableAgentState, "record_response", forbidden)
    monkeypatch.setattr("agent_framework_durabletask._state_migration.load_agent_response", forbidden)
    migrated = _cold(_migrate(source, completion_evidence=evidence, require_known_outcomes=strict))
    expiry = (NOW + timedelta(seconds=WINDOW)).isoformat()
    assert migrated.data.response_mailbox == {"done": {**original, "resultExpiresAt": expiry}}
    assert migrated.data.completed_correlations == {
        "done": {
            "correlationId": "done",
            "outcome": outcome,
            "completedAt": OLD,
            "resultExpiresAt": expiry,
            "resultState": "available",
        }
    }
    assert migrated.to_dict()["data"]["conversationHistory"] == before["data"]["conversationHistory"]
    assert migrated.data.unknown_fields["migration"] == {
        "id": "migration-1",
        "sourceDigest": state_snapshot_digest(before),
        "sourceSessionId": SESSION,
        "ownershipTransferId": "transfer-1",
        "createdAt": NOW.isoformat(),
        "completionEvidenceId": "completion-journal-1",
    }
    migrated.expire_responses(now=NOW + timedelta(seconds=WINDOW))
    expired = _cold(migrated)
    assert expired.data.response_mailbox == {}
    assert expired.data.completed_correlations["done"]["completedAt"] == OLD
    assert expired.data.completed_correlations["done"]["outcome"] == outcome
    assert expired.data.completed_correlations["done"]["resultState"] == "unavailable"
    assert source == before and evidence == evidence_before


@pytest.mark.parametrize("kind", list(DurableAgentStateEntryJsonType))
@pytest.mark.parametrize("strict", [False, True])
def test_every_nonempty_history_kind_requires_completion_evidence(kind: str, strict: bool) -> None:
    source = _source(_entry(kind))
    _assert_rejected(source, None, require_known_outcomes=strict, delivery_evidence=_delivery(source))


@pytest.mark.parametrize("loss", ["truncation", "ingestion"])
def test_used_source_without_history_still_requires_completion_journal(loss: str) -> None:
    source = _source()
    delivery = None
    if loss == "truncation":
        source["data"]["truncation"] = {"evictedMessageCount": 2, "firstEvictedAt": OLD, "lastEvictedAt": OLD}
    else:
        source["data"]["ingestedPositions"] = {"upstream": 0}
        delivery = _delivery(source, Message("user", ["accepted"], message_id=workflow_message_id("upstream", 0)))
    _assert_rejected(source, None, delivery_evidence=delivery)


@pytest.mark.parametrize("omit_history", [False, True])
def test_truly_fresh_legacy_source_needs_no_completion_journal(omit_history: bool) -> None:
    source = _source()
    if omit_history:
        del source["data"]["conversationHistory"]
    before = deepcopy(source)
    staged = _cold(_migrate(source))
    assert staged.data.response_mailbox == staged.data.completed_correlations == {}
    assert "completionEvidenceId" not in staged.data.unknown_fields["migration"]
    assert staged.to_dict()["data"]["conversationHistory"] == []
    assert source == before


def test_empty_complete_journal_can_assert_used_request_only_source_had_no_completions() -> None:
    source = _source(_entry("request", "pending"))
    evidence = _journal(source)
    migrated = _cold(_migrate(source, completion_evidence=evidence))
    assert migrated.data.response_mailbox == migrated.data.completed_correlations == {}
    assert migrated.data.unknown_fields["migration"]["completionEvidenceId"] == evidence["evidenceId"]
    assert migrated.to_dict()["data"]["conversationHistory"] == source["data"]["conversationHistory"]


@pytest.mark.parametrize("kind", ["response", "errorResponse"])
def test_empty_journal_cannot_discard_a_retained_completion(kind: str) -> None:
    source = _source(_entry(kind))
    _assert_rejected(source, _journal(source))


@pytest.mark.parametrize("loss", ["truncation", "compaction", "both"])
def test_complete_journal_carries_pruned_originals_independently_of_retained_history(loss: str) -> None:
    source = _source(_entry())
    if loss in ("compaction", "both"):
        source["data"]["conversationHistory"].append(_entry("compaction"))
    if loss in ("truncation", "both"):
        source["data"]["truncation"] = {"evictedMessageCount": 4, "firstEvictedAt": OLD, "lastEvictedAt": OLD}
    originals = [_result(), _result("pruned-success"), _result("pruned-failure", "failed")]
    evidence = _journal(source, *originals)
    migrated = _cold(_migrate(source, completion_evidence=evidence))
    assert set(migrated.data.completed_correlations) == {item["correlationId"] for item in originals}
    for original in originals:
        assert migrated.data.response_mailbox[original["correlationId"]] == {
            **original,
            "resultExpiresAt": (NOW + timedelta(seconds=WINDOW)).isoformat(),
        }
    for key, value in source["data"].items():
        assert migrated.to_dict()["data"][key] == value


@pytest.mark.parametrize("with_created_at", [False, True])
def test_unknown_json_and_optional_presence_survive_at_original_locations(with_created_at: bool) -> None:
    source = _source(_entry("errorResponse"))
    source["futureRoot"] = {"opaque": [False, 0, None, "雪😀"]}
    source["data"]["expirationTimeUtc"] = None
    source["data"]["session"] = {"session_id": SESSION, "state": {"provider": {"original": [1]}}}
    source["data"]["conversationHistory"][0].pop("createdAt")
    original = _result(outcome="failed")
    original["futureResult"] = {"opaque": [False, 0, None]}
    original["error"].update(details=None, futureError=[False, None])
    response = original["response"]
    response.update({
        "value": None,
        "futureResponse": {"opaque": [False, 0, None]},
        "extensionData": {},
        "usage": {"inputTokenCount": 0, "extensionData": {}, "futureUsage": None},
        "continuationToken": "AAEC",
        "foreignContinuationProfile": {"opaque": [1]},
    })
    response["messages"] = [
        {"role": "assistant", "futureMessage": None},
        {"role": "assistant", "contents": [], "extensionData": {}, "authorName": "", "messageId": ""},
        {
            "role": "assistant",
            "contents": [
                {"$type": "unknown", "content": None, "futureContent": [False, 0]},
                {"$type": "functionResult", "callId": "call", "result": None, "extensionData": None},
                {"$type": "functionResult", "callId": "absent-result"},
            ],
        },
    ]
    if with_created_at:
        response["createdAt"] = TRANSCRIPT_TIME
    evidence = _journal(source, original)
    before, evidence_before = deepcopy(source), deepcopy(evidence)
    staged = _migrate(source, completion_evidence=evidence)
    cold = _cold(staged)
    expected = {**original, "resultExpiresAt": (NOW + timedelta(seconds=WINDOW)).isoformat()}
    assert cold.data.response_mailbox["done"] == expected
    for key, value in before["data"].items():
        assert cold.to_dict()["data"][key] == value
    assert cold.to_dict()["futureRoot"] == before["futureRoot"]
    assert source == before and evidence == evidence_before
    staged.data.response_mailbox["done"]["futureResult"]["opaque"].append("staged edit")
    evidence["results"][0]["response"]["futureResponse"]["opaque"].append("caller edit")
    assert cold.data.response_mailbox["done"] == expected
    assert staged.data.response_mailbox["done"]["response"] == original["response"]


@pytest.mark.parametrize("kind", ["errorResponse", "response"])
@pytest.mark.parametrize("reverse", [False, True])
def test_every_duplicate_transcript_correlation_checks_affirmative_failure(kind: str, reverse: bool) -> None:
    failed = _entry(kind)
    if kind == "response":
        failed["messages"][0]["contents"] = [{"$type": "error", "message": "failed", "errorCode": "provider"}]
    entries = [_entry(), failed]
    source = _source(*(list(reversed(entries)) if reverse else entries))
    _assert_rejected(source, _journal(source, _result()))
    original = _result(outcome="failed")
    migrated = _cold(_migrate(source, completion_evidence=_journal(source, original)))
    assert migrated.data.response_mailbox["done"]["outcome"] == "failed"
    assert migrated.to_dict()["data"]["conversationHistory"] == source["data"]["conversationHistory"]


@pytest.mark.parametrize("role", ["user", "assistant", "system", "tool"])
@pytest.mark.parametrize("code", ["provider_error", "response_expired"])
def test_only_non_tool_invocation_error_contradicts_journal_success(role: str, code: str) -> None:
    entry = _entry()
    entry["messages"] = [{"role": role, "contents": [{"$type": "error", "errorCode": code}]}]
    source = _source(entry)
    evidence = _journal(source, _result())
    if role != "tool" and code != "response_expired":
        _assert_rejected(source, evidence)
    else:
        assert (
            _cold(_migrate(source, completion_evidence=evidence)).data.response_mailbox["done"]["outcome"]
            == "succeeded"
        )


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize("ack", ["accepted", "already_completed", "expired-error"])
def test_availability_acknowledgements_are_not_original_results_even_with_known_outcome(outcome: str, ack: str) -> None:
    source = _source()
    original = _result(outcome=outcome)
    original["response"]["extensionData"] = {"durable_outcome": outcome}
    if ack == "expired-error":
        original["response"]["messages"] = [
            {
                "role": "system",
                "contents": [
                    {
                        "$type": "error",
                        "errorCode": "response_expired",
                        "message": "Expired.",
                    }
                ],
            }
        ]
    else:
        original["response"]["extensionData"]["durable_status"] = ack
    _assert_rejected(source, _journal(source, original))


@pytest.mark.parametrize("failure", ["status", "content"])
def test_original_response_failure_evidence_cannot_be_disguised_as_success(failure: str) -> None:
    source = _source()
    original = _result()
    if failure == "status":
        original["response"]["extensionData"] = {"durable_status": "error"}
    else:
        original["response"]["messages"][0]["contents"] = [{"$type": "error", "message": "failed"}]
    _assert_rejected(source, _journal(source, original))


@pytest.mark.parametrize("field", ["sourceDigest", "evidenceId", "complete", "results"])
def test_every_completion_envelope_field_is_required(field: str) -> None:
    source = _source()
    evidence = _journal(source, _result())
    del evidence[field]
    _assert_rejected(source, evidence)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("sourceDigest", "0" * 64),
        ("evidenceId", " \t"),
        ("evidenceId", None),
        ("complete", False),
        ("complete", 1),
        ("complete", "true"),
        ("results", {}),
        ("results", None),
        ("results", [None]),
        ("extra", []),
    ],
)
def test_completion_envelope_is_exact_complete_and_source_bound(field: str, value: Any) -> None:
    source = _source()
    evidence = _journal(source, _result())
    evidence[field] = value
    _assert_rejected(source, evidence)


@pytest.mark.parametrize("evidence", [[], False, "journal"])
def test_nonobject_completion_evidence_rejects(evidence: Any) -> None:
    _assert_rejected(_source(), evidence)


@pytest.mark.parametrize("field", ["correlationId", "outcome", "completedAt", "response"])
def test_each_required_terminal_result_field_must_be_originally_present(field: str) -> None:
    source = _source()
    original = _result()
    del original[field]
    _assert_rejected(source, _journal(source, original))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("correlationId", ""),
        ("correlationId", "bad\nidentity"),
        ("correlationId", "x" * 257),
        ("outcome", "unknown"),
        ("outcome", True),
        ("completedAt", None),
        ("completedAt", TRANSCRIPT_TIME[:-1]),
        ("completedAt", "2024-02-30T00:00:00Z"),
        ("resultExpiresAt", None),
        ("resultExpiresAt", "2027-01-01T00:00:00Z"),
        ("response", {}),
        ("response", {"messages": None}),
        ("response", {"messages": [{"role": "future"}]}),
        ("response", {"messages": [{"role": "assistant", "contents": [{"type": "text", "text": "Core"}]}]}),
        ("response", {"messages": [], "usage": None}),
        ("response", {"messages": [], "extensionData": {"": "bad-key"}}),
        ("response", {"messages": [], "continuationToken": "not-base64"}),
        ("error", None),
    ],
)
def test_malformed_terminal_results_are_not_repaired_or_disguised(field: str, value: Any) -> None:
    source = _source()
    original = _result()
    original[field] = value
    _assert_rejected(source, _journal(source, original))


@pytest.mark.parametrize(
    "error", [None, {}, {"code": "code"}, {"code": "", "message": "failed"}, {"code": "c", "message": " "}]
)
def test_failed_results_require_full_shared_error(error: Any) -> None:
    source = _source()
    original = _result(outcome="failed")
    if error is None:
        del original["error"]
    else:
        original["error"] = error
    _assert_rejected(source, _journal(source, original))


@pytest.mark.parametrize("second_outcome", ["succeeded", "failed"])
def test_duplicate_journal_correlations_reject_even_identical_results(second_outcome: str) -> None:
    source = _source()
    _assert_rejected(source, _journal(source, _result(), _result(outcome=second_outcome)))


@pytest.mark.parametrize("correlation", [None, "missing-original", " "])
def test_every_retained_response_requires_a_matching_nonblank_journal_identity(correlation: Any) -> None:
    entry = _entry()
    if correlation is None:
        entry.pop("correlationId")
    else:
        entry["correlationId"] = correlation
    source = _source(entry)
    _assert_rejected(source, _journal(source, _result()))


@pytest.mark.parametrize(
    "completed",
    [
        "2026-09-16T12:01:00.000000001Z",
        "2026-09-16T13:01:00.000000001+01:00",
        "2027-01-01T00:00:00Z",
    ],
)
def test_grace_cannot_precede_completion_even_by_submicrosecond(completed: str) -> None:
    source = _source()
    original = _result()
    original["completedAt"] = completed
    _assert_rejected(source, _journal(source, original))


def test_grace_can_equal_completion_without_normalizing_original_offset_or_precision() -> None:
    source = _source()
    original = _result()
    original["completedAt"] = "2026-09-16T13:01:00.000000000+01:00"
    migrated = _cold(_migrate(source, completion_evidence=_journal(source, original)))
    assert migrated.data.response_mailbox["done"]["completedAt"] == original["completedAt"]


def test_snapshot_digest_binds_raw_missing_fields_and_unknown_json_before_defaults() -> None:
    source: dict[str, Any] = {"schemaVersion": "1.1.0", "data": {}, "future": ["雪", False, 0, None]}
    evidence = _journal(source, _result())
    raw_digest = hashlib.sha256(
        json.dumps(source, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    ).hexdigest()
    assert evidence["sourceDigest"] == raw_digest
    migrated = _cold(_migrate(source, completion_evidence=evidence))
    assert migrated.data.unknown_fields["migration"]["sourceDigest"] == raw_digest
    defaulted = deepcopy(source)
    defaulted["data"]["conversationHistory"] = []
    _assert_rejected(defaulted, evidence)
    changed_unknown = deepcopy(source)
    changed_unknown["future"][1] = 0
    _assert_rejected(changed_unknown, evidence)
    _assert_rejected(source, evidence, source_digest="0" * 64)


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), (1, 2), {1: "bad-key"}, {1, 2}])
def test_non_json_unknown_completion_fields_reject_without_normalization(bad: Any) -> None:
    source = _source()
    evidence = _journal(source, _result())
    evidence["results"][0]["response"]["unknown"] = bad
    # NaN cannot be compared by value. The original nested object must stay attached.
    before = deepcopy(source)
    original_results = evidence["results"]
    with pytest.raises(ValueError, match="strict JSON"):
        _migrate(source, completion_evidence=evidence)
    assert source == before and evidence["results"] is original_results
    assert evidence["results"][0]["response"]["unknown"] is bad


def test_cyclic_completion_evidence_is_rejected_without_mutating_its_cycle() -> None:
    source = _source()
    evidence = _journal(source, _result())
    evidence["results"][0]["cycle"] = evidence
    with pytest.raises(ValueError, match="strict JSON"):
        _migrate(source, completion_evidence=evidence)
    assert evidence["results"][0]["cycle"] is evidence


@pytest.mark.parametrize("entry", [{"messages": []}, {"$type": "future", "messages": []}, {"jsonType": "response"}])
def test_unsupported_legacy_entries_cannot_become_writable_v2_even_with_complete_journal(entry: dict[str, Any]) -> None:
    source = _source(entry)
    _assert_rejected(source, _journal(source, _result()))


def test_delivery_journal_stays_separate_and_unversioned_receipts_stay_opaque() -> None:
    first = Message("user", ["first"], message_id=workflow_message_id("upstream", 1))
    third = Message("user", ["third"], message_id=workflow_message_id("upstream", 3))
    revised = Message("user", ["third revised"], message_id=third.message_id)
    source = _source(_entry("request", "pending"))
    source["data"].update({
        "ingestedPositions": {"upstream": 3},
        "ingestedMessages": {"not-runtime-receipts": ["opaque", None, False]},
    })
    delivery = _delivery(source, revised, first, third)
    evidence = _journal(source)
    before, delivery_before, evidence_before = deepcopy(source), deepcopy(delivery), deepcopy(evidence)
    migrated = _cold(_migrate(source, delivery_evidence=delivery, completion_evidence=evidence))
    assert migrated.data.ingested_messages == {
        first.message_id: [message_identity(first)],
        third.message_id: [message_identity(revised), message_identity(third)],
    }
    assert workflow_message_id("upstream", 2) not in migrated.data.ingested_messages
    assert migrated.to_dict()["data"]["ingestedMessages"] == before["data"]["ingestedMessages"]
    assert migrated.data.unknown_fields["migration"]["evidenceId"] == delivery["evidenceId"]
    assert migrated.data.unknown_fields["migration"]["completionEvidenceId"] == evidence["evidenceId"]
    assert source == before and delivery == delivery_before and evidence == evidence_before
    _assert_rejected(source, evidence)  # Completion authority cannot replace accepted-input evidence.
    bad_delivery = deepcopy(delivery)
    bad_delivery["messages"].append(deepcopy(bad_delivery["messages"][0]))
    _assert_rejected(source, evidence, delivery_evidence=bad_delivery)


@pytest.mark.parametrize("opaque", [None, [], False, {"legacy": ["not-a-fingerprint"]}])
def test_unversioned_ingested_messages_are_not_completion_or_delivery_evidence(opaque: Any) -> None:
    source = _source(_entry("request", "pending"))
    source["data"]["ingestedMessages"] = opaque
    evidence = _journal(source)
    migrated = _cold(_migrate(source, completion_evidence=evidence))
    assert migrated.data.ingested_messages == {}
    assert migrated.to_dict()["data"]["ingestedMessages"] == opaque
    _assert_rejected(source, None)


def test_oversize_rejection_protects_whole_source_evidence_and_existing_destination() -> None:
    source = _source(_entry())
    source["future"] = "雪😀" * 20
    evidence = _journal(source, _result())
    payload = _migrate(source, completion_evidence=evidence).to_dict()
    size = len(json.dumps(payload, allow_nan=False))
    assert _migrate(source, completion_evidence=evidence, max_state_bytes=size).to_dict() == payload
    _assert_rejected(source, evidence, max_state_bytes=size - 1)


@pytest.mark.parametrize("invalid", [None, 0, 1, "false"])
def test_deprecated_known_outcomes_flag_still_requires_boolean(invalid: Any) -> None:
    source = _source()
    _assert_rejected(source, _journal(source), require_known_outcomes=invalid)
