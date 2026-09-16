# Copyright (c) Microsoft. All rights reserved.

"""Migration-only failure enrichment without restoring expired legacy delivery."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from agent_framework_durabletask import DurableAgentState
from agent_framework_durabletask._state_migration import migrate_legacy_state, state_snapshot_digest

CORRELATION = "legacy-completion"
CREATED = "2024-01-01T00:00:00+00:00"
COMPLETED = "2024-01-02T03:04:05.123400+05:30"
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


def _receipt() -> dict[str, Any]:
    return {"completedAt": COMPLETED, "future": deepcopy(OPAQUE)}


def _source(entry: dict[str, Any] | None, *, receipt: dict[str, Any] | None) -> dict[str, Any]:
    data: dict[str, Any] = {
        "conversationHistory": [] if entry is None else [entry],
        "future": deepcopy(OPAQUE),
    }
    if receipt is not None:
        data["completedCorrelations"] = {CORRELATION: receipt}
    return {"schemaVersion": "1.1.0", "data": data, "future": deepcopy(OPAQUE)}


def _migrate(source: dict[str, Any], *, strict: bool, now: datetime = NOW) -> DurableAgentState:
    return migrate_legacy_state(
        source,
        source_digest=state_snapshot_digest(source),
        source_session_id="original-session",
        migration_id="legacy-outcome-enrichment",
        ownership_transfer_id="authorized-transfer",
        delivery_window_seconds=WINDOW,
        require_known_outcomes=strict,
        now=now,
    )


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("existing_receipt", [False, True])
@pytest.mark.parametrize("legacy", [None, False, True])
@pytest.mark.parametrize(
    "entry",
    [
        pytest.param(_entry(contents=[{"$type": "text", "text": "Failed operation"}]), id="typed-text"),
        pytest.param(_entry(), id="typed-no-messages"),
        pytest.param(_entry(contents=[{"$type": "text", "text": ""}]), id="typed-empty-text"),
        pytest.param(_entry("response", [{"$type": "error", "message": "provider failed"}]), id="error-content"),
    ],
)
def test_failure_enrichment_preserves_receipt_and_never_reopens_delivery(
    entry: dict[str, Any], existing_receipt: bool, strict: bool, legacy: bool | None
) -> None:
    """Affirmative failures work in both modes, with receipt-absent backfill as a control."""
    receipt = _receipt()
    if legacy is not None:
        receipt["legacy"] = legacy
    source = _source(deepcopy(entry), receipt=receipt if existing_receipt else None)
    before = deepcopy(source)
    digest = state_snapshot_digest(source)
    for now in (NOW, NOW + timedelta(days=7)):
        migrated = _migrate(source, strict=strict, now=now)
        migrated = DurableAgentState.from_json(migrated.to_json())
        if existing_receipt:
            assert migrated.data.completed_correlations[CORRELATION] == {**receipt, "outcome": "failed"}
            assert migrated.data.response_mailbox == {}
            expired = migrated.try_get_agent_response(CORRELATION)
            assert expired is not None
            assert expired.additional_properties["durable_status"] == "already_completed"
            assert expired.additional_properties["durable_outcome"] == "failed"
        else:
            assert migrated.data.completed_correlations[CORRELATION] == {
                "completedAt": now.isoformat(),
                "outcome": "failed",
                "legacy": True,
            }
            mailbox = migrated.data.response_mailbox[CORRELATION]
            assert mailbox["createdAt"] == now.isoformat()
            assert mailbox["expiresAt"] == (now + timedelta(seconds=WINDOW)).isoformat()
        assert migrated.unknown_fields["future"] == OPAQUE
        assert migrated.data.unknown_fields["future"] == OPAQUE
        assert source == before
        assert state_snapshot_digest(source) == digest


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("existing_receipt", [False, True])
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
def test_partial_legacy_evidence_stays_unknown_or_strictly_rejects(
    entry: dict[str, Any], existing_receipt: bool, strict: bool
) -> None:
    """Neither absent errors nor delivery-status errors establish an invocation outcome."""
    receipt = {**_receipt(), "legacy": True}
    source = _source(deepcopy(entry), receipt=receipt if existing_receipt else None)
    before = deepcopy(source)
    if strict:
        with pytest.raises(ValueError, match="outcome.*evidence"):
            _migrate(source, strict=True)
    else:
        migrated = _migrate(source, strict=False)
        assert "outcome" not in migrated.data.completed_correlations[CORRELATION]
        if existing_receipt:
            assert migrated.data.completed_correlations[CORRELATION] == receipt
            assert migrated.data.response_mailbox == {}
        else:
            assert migrated.data.completed_correlations[CORRELATION] == {
                "completedAt": NOW.isoformat(),
                "legacy": True,
            }
            mailbox = migrated.data.response_mailbox[CORRELATION]
            assert mailbox["createdAt"] == NOW.isoformat()
            assert mailbox["expiresAt"] == (NOW + timedelta(seconds=WINDOW)).isoformat()
        migrated.expire_responses(now=NOW + timedelta(days=1))
        migrated = DurableAgentState.from_json(migrated.to_json())
        expired = migrated.try_get_agent_response(CORRELATION)
        assert expired is not None and expired.additional_properties["durable_outcome"] == "unknown"
    assert source == before


@pytest.mark.parametrize("strict", [False, True])
def test_receipt_without_retained_response_is_not_enriched(strict: bool) -> None:
    """Missing transcript evidence is not proof of either success or failure."""
    source = _source(None, receipt=_receipt())
    before = deepcopy(source)
    if strict:
        with pytest.raises(ValueError, match="outcome.*evidence"):
            _migrate(source, strict=True)
    else:
        migrated = _migrate(source, strict=False)
        assert migrated.data.completed_correlations == source["data"]["completedCorrelations"]
        assert migrated.data.response_mailbox == {}
    assert source == before


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize("legacy", [False, True])
def test_known_receipt_is_immutable_despite_retained_transcript_error(strict: bool, outcome: str, legacy: bool) -> None:
    """A possibly altered transcript is not an authoritative pair for a known receipt."""
    receipt = {**_receipt(), "outcome": outcome, "legacy": legacy}
    source = _source(_entry(), receipt=receipt)
    before = deepcopy(source)
    migrated = _migrate(source, strict=strict)
    assert migrated.data.completed_correlations[CORRELATION] == receipt
    assert migrated.data.response_mailbox == {}
    assert source == before


def _mailbox(*, failed: bool, expired: bool) -> dict[str, Any]:
    return {
        "createdAt": CREATED,
        "expiresAt": "2024-01-01T00:01:00Z" if expired else "9999-01-01T00:00:00Z",
        "response": {
            "type": "agent_response",
            "messages": [],
            "additional_properties": {"durable_status": "error"} if failed else {},
        },
        "future": deepcopy(OPAQUE),
    }


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("expired", [False, True])
@pytest.mark.parametrize("failed", [False, True])
@pytest.mark.parametrize("legacy", [False, True])
def test_existing_mailbox_controls_unknown_receipt_even_with_transcript_failure(
    strict: bool, expired: bool, failed: bool, legacy: bool
) -> None:
    """Transcript enrichment cannot replace independent or ambiguous legacy mailbox evidence."""
    receipt = {**_receipt(), "legacy": legacy}
    source = _source(_entry(), receipt=receipt)
    source["data"]["responseMailbox"] = {CORRELATION: _mailbox(failed=failed, expired=expired)}
    before = deepcopy(source)
    expected = "failed" if failed else None if legacy else "succeeded"
    if strict and expected is None:
        with pytest.raises(ValueError, match="outcome.*evidence"):
            _migrate(source, strict=True)
    else:
        migrated = _migrate(source, strict=strict)
        assert migrated.data.completed_correlations[CORRELATION] == {
            **receipt,
            **({"outcome": expected} if expected is not None else {}),
        }
        assert migrated.data.response_mailbox == before["data"]["responseMailbox"]
    assert source == before


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("expired", [False, True])
@pytest.mark.parametrize("failed", [False, True])
def test_authoritative_mailbox_conflict_rejects_without_mutating_source(
    strict: bool, expired: bool, failed: bool
) -> None:
    """Validate all receipts before enriching an earlier eligible transcript failure."""
    source = _source(_entry(), receipt=_receipt())
    source["data"]["completedCorrelations"]["conflicting"] = {
        **_receipt(),
        "outcome": "succeeded" if failed else "failed",
        "legacy": failed,
    }
    source["data"]["responseMailbox"] = {"conflicting": _mailbox(failed=failed, expired=expired)}
    before = deepcopy(source)
    with pytest.raises(ValueError, match="outcome.*conflicts"):
        _migrate(source, strict=strict)
    assert source == before


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("outcome", [None, "", "unknown", "success", "FAILED", True, 0, [], {}])
def test_invalid_present_outcome_is_not_treated_as_unknown(strict: bool, outcome: Any) -> None:
    """Only an absent outcome is eligible, invalid present fields must fail closed."""
    source = _source(_entry(), receipt={**_receipt(), "outcome": outcome})
    before = deepcopy(source)
    with pytest.raises(ValueError, match="outcome"):
        _migrate(source, strict=strict)
    assert source == before


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize(
    "invalid_receipt",
    [
        pytest.param(None, id="null-receipt"),
        pytest.param([], id="array-receipt"),
        pytest.param({}, id="missing-time"),
        pytest.param({"completedAt": "2024-01-01T00:00:00"}, id="no-time-offset"),
        pytest.param({**_receipt(), "legacy": "true"}, id="nonboolean-legacy"),
    ],
)
def test_malformed_receipt_rejects_without_mutating_source(strict: bool, invalid_receipt: Any) -> None:
    """Enrichment must not repair malformed completion records into valid evidence."""
    source = _source(_entry(), receipt=_receipt())
    source["data"]["completedCorrelations"][CORRELATION] = deepcopy(invalid_receipt)
    before = deepcopy(source)
    with pytest.raises(ValueError, match="completedCorrelations"):
        _migrate(source, strict=strict)
    assert source == before
