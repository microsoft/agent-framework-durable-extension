# Copyright (c) Microsoft. All rights reserved.

"""Legacy failure evidence and ambiguity rejection for the shared completion target."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from agent_framework_durabletask import DurableAgentState
from agent_framework_durabletask._shared_response import load_terminal_response
from agent_framework_durabletask._state_migration import migrate_legacy_state, state_snapshot_digest

CORRELATION = "legacy-completion"
CREATED = "2024-01-01T00:00:00+00:00"
ORIGINAL_COMPLETED_AT = "2024-01-02T03:04:05.123456789Z"
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
WINDOW = 60
OPAQUE = {"keep": [0, False, None, {"outcome": "application-value"}]}


def _entry(
    kind: str = "errorResponse",
    contents: list[dict[str, Any]] | None = None,
    *,
    role: str = "assistant",
) -> dict[str, Any]:
    return {
        "$type": kind,
        "correlationId": CORRELATION,
        "createdAt": CREATED,
        "messages": [] if contents is None else [{"role": role, "contents": contents}],
    }


def _source(entry: dict[str, Any] | None) -> dict[str, Any]:
    data: dict[str, Any] = {
        "conversationHistory": [] if entry is None else [entry],
        "future": deepcopy(OPAQUE),
        "session": {"session_id": "original-session", "state": deepcopy(OPAQUE)},
    }
    return {"schemaVersion": "1.1.0", "data": data, "future": deepcopy(OPAQUE)}


def _original_result(*, outcome: str = "failed") -> dict[str, Any]:
    # Ground truth supplied by the synthetic operator, not inferred from _entry.
    return {
        "correlationId": CORRELATION,
        "outcome": outcome,
        "completedAt": ORIGINAL_COMPLETED_AT,
        "response": {
            "createdAt": CREATED,
            "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "original answer"}]}],
        },
        **(
            {"error": {"code": "provider_error", "message": "Original invocation failed."}}
            if outcome == "failed"
            else {}
        ),
    }


def _journal(source: dict[str, Any], *results: dict[str, Any]) -> dict[str, Any]:
    return {
        "sourceDigest": state_snapshot_digest(source),
        "evidenceId": "original-completions",
        "complete": True,
        "results": deepcopy(list(results)),
    }


def _migrate(
    source: dict[str, Any], *, strict: bool, now: datetime = NOW, completion_evidence: dict[str, Any] | None = None
) -> DurableAgentState:
    options: dict[str, Any] = {"completion_evidence": completion_evidence} if completion_evidence is not None else {}
    return migrate_legacy_state(
        source,
        source_digest=state_snapshot_digest(source),
        source_session_id="original-session",
        migration_id="legacy-failure-migration",
        ownership_transfer_id="authorized-transfer",
        delivery_window_seconds=WINDOW,
        require_known_outcomes=strict,
        **options,
        now=now,
    )


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize(
    "entry",
    [
        pytest.param(_entry(contents=[{"$type": "text", "text": "Failed operation"}]), id="typed-text"),
        pytest.param(_entry(), id="typed-no-messages"),
        pytest.param(_entry(contents=[{"$type": "text", "text": ""}]), id="typed-empty-text"),
        pytest.param(_entry("response", [{"$type": "error", "message": "provider failed"}]), id="error-content"),
    ],
)
def test_authoritative_failure_migrates_and_expiry_never_reopens_delivery(
    entry: dict[str, Any], version: str, strict: bool
) -> None:
    """Failure evidence must agree with the journal, which owns the original result and time."""
    source = _source(deepcopy(entry))
    source["schemaVersion"] = version
    before = deepcopy(source)
    digest = state_snapshot_digest(source)
    original = _original_result()
    evidence = _journal(source, original)
    evidence_before = deepcopy(evidence)
    migrated = DurableAgentState.from_json(_migrate(source, strict=strict, completion_evidence=evidence).to_json())
    mailbox = migrated.data.response_mailbox[CORRELATION]
    receipt = deepcopy(migrated.data.completed_correlations[CORRELATION])
    assert receipt["correlationId"] == mailbox["correlationId"] == CORRELATION
    assert receipt["outcome"] == mailbox["outcome"] == "failed"
    assert receipt["completedAt"] == mailbox["completedAt"] == ORIGINAL_COMPLETED_AT
    assert receipt["resultExpiresAt"] == mailbox["resultExpiresAt"] == (NOW + timedelta(seconds=WINDOW)).isoformat()
    assert mailbox == {**original, "resultExpiresAt": receipt["resultExpiresAt"]}
    assert receipt["resultState"] == "available"
    assert "resultUnavailableAt" not in receipt
    assert mailbox["error"]["code"] and mailbox["error"]["message"]
    assert mailbox["response"]["createdAt"] == CREATED
    response = load_terminal_response(mailbox["response"])
    assert response.text == "original answer"
    assert migrated.to_dict()["data"]["conversationHistory"] == before["data"]["conversationHistory"]
    assert migrated.data.session == before["data"]["session"]

    expired_at = NOW + timedelta(days=1)
    migrated.expire_responses(now=expired_at)
    migrated = DurableAgentState.from_json(migrated.to_json())
    assert migrated.data.response_mailbox == {}
    assert migrated.data.completed_correlations[CORRELATION] == {
        **receipt,
        "resultState": "unavailable",
        "resultUnavailableAt": expired_at.isoformat(),
    }
    expired = migrated.try_get_agent_response(CORRELATION)
    assert expired is not None
    assert expired.additional_properties["durable_status"] == "already_completed"
    assert expired.additional_properties["durable_outcome"] == "failed"
    after_expiry = migrated.to_dict()
    migrated.record_response(CORRELATION, response, delivery_window_seconds=WINDOW, now=expired_at, legacy=True)
    migrated.expire_responses(now=expired_at + timedelta(days=7))
    assert migrated.to_dict() == after_expiry
    assert migrated.unknown_fields["future"] == OPAQUE
    assert migrated.data.unknown_fields["future"] == OPAQUE
    assert source == before and evidence == evidence_before
    assert state_snapshot_digest(source) == digest


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize(
    "entry",
    [
        pytest.param(_entry("response"), id="empty"),
        pytest.param(_entry("response", [{"$type": "text", "text": "positive answer"}]), id="positive-text"),
        pytest.param(_entry("response", [{"$type": "text", "text": ""}]), id="partial-empty-text"),
        pytest.param(
            _entry("response", [{"$type": "error", "message": "tool failed"}], role="tool"),
            id="tool-error-only",
        ),
        pytest.param(
            _entry("response", [{"$type": "error", "errorCode": "response_expired", "message": "expired"}]),
            id="delivery-expired-not-invocation-failure",
        ),
    ],
)
def test_partial_legacy_evidence_rejects_even_when_known_outcomes_flag_is_false(
    entry: dict[str, Any], strict: bool
) -> None:
    """Neither absent errors nor delivery-status errors establish an invocation outcome."""
    source = _source(deepcopy(entry))
    before = deepcopy(source)
    with pytest.raises(ValueError, match="outcome.*evidence"):
        _migrate(source, strict=strict)
    assert source == before


@pytest.mark.parametrize("strict", [False, True])
def test_session_only_source_requires_explicit_complete_empty_journal(strict: bool) -> None:
    source = _source(None)
    before = deepcopy(source)
    # Nonempty provider/session state may outlive all retained history.
    with pytest.raises(ValueError, match="authoritative completion evidence"):
        _migrate(source, strict=strict)
    evidence = _journal(source)
    migrated = DurableAgentState.from_json(_migrate(source, strict=strict, completion_evidence=evidence).to_json())
    assert migrated.data.completed_correlations == {}
    assert migrated.data.response_mailbox == {}
    assert migrated.try_get_agent_response(CORRELATION) is None
    assert migrated.data.session == before["data"]["session"]
    assert source == before


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("ambiguous_first", [False, True])
@pytest.mark.parametrize("same_correlation", [False, True])
def test_affirmative_failure_cannot_hide_another_ambiguous_response(
    strict: bool, ambiguous_first: bool, same_correlation: bool
) -> None:
    failure = _entry()
    partial = _entry("response", [{"$type": "text", "text": "partial answer"}])
    if not same_correlation:
        partial["correlationId"] = "other-request"
    source = _source(None)
    source["data"]["conversationHistory"] = [partial, failure] if ambiguous_first else [failure, partial]
    before = deepcopy(source)
    with pytest.raises(ValueError, match="outcome.*evidence"):
        _migrate(source, strict=strict)
    assert source == before


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("kind", ["response", "errorResponse"])
def test_affirmative_failure_without_completion_authority_is_not_auto_imported(strict: bool, kind: str) -> None:
    source = _source(_entry(kind, [{"$type": "error", "message": "provider failed"}]))
    before = deepcopy(source)
    with pytest.raises(ValueError, match="authoritative completion evidence"):
        _migrate(source, strict=strict)
    assert source == before


@pytest.mark.parametrize("strict", [False, True])
def test_error_response_cannot_be_relabelled_succeeded_by_journal(strict: bool) -> None:
    source = _source(_entry())
    evidence = _journal(source, _original_result(outcome="succeeded"))
    before, evidence_before = deepcopy(source), deepcopy(evidence)
    with pytest.raises(ValueError, match="outcome conflicts with retained response failure evidence"):
        _migrate(source, strict=strict, completion_evidence=evidence)
    assert source == before and evidence == evidence_before


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("correlation", [None, "", " "])
def test_failure_without_a_request_identity_rejects_without_mutating_source(
    strict: bool, correlation: str | None
) -> None:
    entry = _entry()
    if correlation is None:
        del entry["correlationId"]
    else:
        entry["correlationId"] = correlation
    source = _source(entry)
    evidence = _journal(source, _original_result())
    before, evidence_before = deepcopy(source), deepcopy(evidence)
    with pytest.raises(ValueError, match="correlationId"):
        _migrate(source, strict=strict, completion_evidence=evidence)
    assert source == before and evidence == evidence_before


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("contents", [[], [{"$type": "text", "text": "partial"}]])
def test_partial_response_can_import_authoritative_original_success(
    strict: bool, contents: list[dict[str, Any]]
) -> None:
    entry = _entry("response", contents)
    del entry["createdAt"]
    source = _source(entry)
    original = _original_result(outcome="succeeded")
    evidence = _journal(source, original)
    before = deepcopy(source)
    migrated = DurableAgentState.from_json(_migrate(source, strict=strict, completion_evidence=evidence).to_json())
    assert migrated.data.response_mailbox[CORRELATION] == {
        **original,
        "resultExpiresAt": (NOW + timedelta(seconds=WINDOW)).isoformat(),
    }
    assert migrated.data.completed_correlations[CORRELATION]["completedAt"] == ORIGINAL_COMPLETED_AT
    assert migrated.to_dict()["data"]["conversationHistory"] == [entry]
    assert source == before
