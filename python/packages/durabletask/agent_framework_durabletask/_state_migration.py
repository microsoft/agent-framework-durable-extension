# Copyright (c) Microsoft. All rights reserved.

"""Explicit, detached legacy-to-v2 state migration, with no storage or provider access."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from ._constants import DurableStateFields
from ._durable_agent_state import (
    DurableAgentState,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
    _validate_json,  # pyright: ignore[reportPrivateUsage]
)
from ._message_identity import message_identity
from ._response_utils import load_agent_response
from ._retention import StateCapacityError
from ._workflows.naming import parse_workflow_message_id

__all__ = ["migrate_legacy_state", "state_snapshot_digest"]

_SHA256 = re.compile(r"[0-9a-f]{64}")
_EVIDENCE_FIELDS = {"sourceDigest", "evidenceId", "complete", "messages"}
_JOURNAL_REQUIRED = (
    "Legacy ingestedPositions require recorded delivery evidence: a complete authoritative accepted-message "
    "journal from the quiesced legacy deployment, including evicted messages. If that journal is unavailable, "
    "keep the old session on the old engine rather than guessing delivery receipts."
)


def _canonical_json(value: Any) -> str:
    try:
        _validate_json(value)
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError, RecursionError) as exc:
        raise ValueError("Migration inputs must be strict JSON with string keys and finite numbers.") from exc


def state_snapshot_digest(source: dict[str, Any]) -> str:
    """Return the SHA-256 of the complete strict-JSON source snapshot encoded as UTF-8.

    Object keys are sorted, separators are compact, Unicode is not ASCII-escaped,
    and non-finite numbers, non-string keys and non-JSON values are rejected.
    Array order and all unknown fields participate in the digest.

    Args:
        source: The unmodified exported legacy state, not a parsed or upgraded state.

    Returns:
        A lowercase hexadecimal SHA-256 digest.
    """
    if not isinstance(source, dict):
        raise ValueError("source must be a JSON object.")
    return hashlib.sha256(_canonical_json(source).encode("utf-8")).hexdigest()


def _nonblank(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonblank string.")
    return value


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer, not a boolean.")
    return value


def _workflow_position(identity: str) -> tuple[str, int] | None:
    parsed = parse_workflow_message_id(identity)
    if identity.startswith("wf_") and (parsed is None or identity != identity.strip() or not parsed[0].strip()):
        raise ValueError("Malformed workflow message ID in recorded delivery evidence or legacy receipts.")
    return parsed


def _legacy_positions(data: dict[str, Any]) -> dict[str, int]:
    raw = data.get("ingestedPositions", {})
    if not isinstance(raw, dict):
        raise ValueError("Legacy ingestedPositions must be an object of nonnegative integer producer positions.")
    positions: dict[str, int] = {}
    for producer, position in cast(dict[str, Any], raw).items():
        _nonblank(producer, "ingestedPositions producer")
        if isinstance(position, bool) or not isinstance(position, int) or position < 0:
            raise ValueError("Every ingestedPositions position must be a nonnegative integer, not a boolean.")
        positions[producer] = position
    return positions


def _validate_receipts(receipts: dict[str, list[str] | None]) -> None:
    for identity, fingerprints in receipts.items():
        _nonblank(identity, "ingestedMessages ID")
        workflow = _workflow_position(identity)
        if fingerprints is None:
            if workflow is not None:
                raise ValueError("Workflow identity-only markers are not exact recorded delivery evidence.")
            continue
        if not fingerprints or any(_SHA256.fullmatch(value) is None for value in fingerprints):
            raise ValueError("ingestedMessages requires nonempty lists of lowercase SHA-256 fingerprints.")
        if len(set(fingerprints)) != len(fingerprints):
            raise ValueError("ingestedMessages must not contain duplicate fingerprints.")


def _journal_receipts(
    evidence: dict[str, Any], *, source_digest: str, positions: dict[str, int]
) -> tuple[str, dict[str, list[str]]]:
    if not isinstance(evidence, dict) or evidence.keys() != _EVIDENCE_FIELDS:
        raise ValueError("Recorded delivery evidence requires exactly sourceDigest, evidenceId, complete and messages.")
    # Detach before constructing any core object. The loader must never touch the caller's journal.
    journal: dict[str, Any] = json.loads(_canonical_json(evidence))
    if journal["sourceDigest"] != source_digest:
        raise ValueError("Recorded delivery evidence sourceDigest does not match the source snapshot.")
    evidence_id = _nonblank(journal["evidenceId"], "Recorded delivery evidence evidenceId")
    if journal["complete"] is not True:
        raise ValueError("Recorded delivery evidence requires the explicit complete=True operator assertion.")
    messages = journal["messages"]
    if not isinstance(messages, list):
        raise ValueError("Recorded delivery evidence messages must be a list of complete canonical message objects.")

    receipts: dict[str, list[str]] = {}
    maxima: dict[str, int] = {}
    for raw in cast(list[Any], messages):
        if not isinstance(raw, dict):
            raise ValueError("Recorded delivery evidence messages must contain canonical message objects.")
        raw = cast(dict[str, Any], raw)
        identity = _nonblank(raw.get("message_id"), "Recorded delivery evidence message_id")
        workflow = _workflow_position(identity)
        _nonblank(raw.get("role"), "Recorded delivery evidence message role")
        if not isinstance(raw.get("contents"), list):
            raise ValueError("Recorded delivery evidence message contents must be a canonical array.")
        try:
            message = load_agent_response({"messages": [raw]}).messages[0]
            # Do not hash a projection which silently lost unknown fields or changed their types.
            if _canonical_json(message.to_dict()) != _canonical_json(raw):
                raise ValueError("The message does not round-trip as a complete canonical input.")
            fingerprint = message_identity(message)
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError("Recorded delivery evidence requires lossless complete canonical message inputs.") from exc
        revisions = receipts.setdefault(identity, [])
        if fingerprint in revisions:
            raise ValueError("Recorded delivery evidence contains a duplicate message ID/fingerprint pair.")
        revisions.append(fingerprint)
        if workflow is not None:
            producer, position = workflow
            maxima[producer] = max(maxima.get(producer, position), position)

    # This is only a consistency check. Sparse positions are valid; a maximum is never
    # proof of a complete prefix, nor proof that the operator's journal is complete.
    if maxima != positions:
        raise ValueError(
            "Recorded delivery evidence workflow producers and maximum positions must match ingestedPositions."
        )
    return evidence_id, receipts


def _retained_custom_request_ids(state: DurableAgentState) -> Iterator[str]:
    """Yield legacy lookup identities, never fingerprints of possibly pruned content."""
    for entry in state.data.conversation_history:
        if isinstance(entry, DurableAgentStateRequest):
            for message in entry.messages:
                if message.message_id is not None:
                    if isinstance(message.message_id, str) and not message.message_id.strip():
                        continue
                    identity = _nonblank(message.message_id, "Legacy request message ID")
                    if _workflow_position(identity) is None:
                        yield identity


def _apply_journal(state: DurableAgentState, journal: dict[str, list[str]]) -> None:
    receipts = state.data.ingested_messages
    for identity, existing in receipts.items():
        recorded = journal.get(identity)
        if recorded is None or (existing is not None and not set(existing).issubset(recorded)):
            raise ValueError("Recorded delivery evidence is inconsistent with existing ingestedMessages receipts.")
    for identity in _retained_custom_request_ids(state):
        if identity not in journal:
            raise ValueError("Recorded delivery evidence must include every retained legacy custom request message ID.")
    for identity, recorded in journal.items():
        existing = receipts.get(identity)
        if existing is None:
            receipts[identity] = list(recorded)
        else:
            existing.extend(fingerprint for fingerprint in recorded if fingerprint not in existing)


def _preserve_session(state: DurableAgentState, source_session_id: str) -> None:
    session = state.data.session
    if session is None:
        state.data.session = {"session_id": source_session_id, "state": {}}
        return
    if not isinstance(session, dict):
        raise ValueError("Legacy session must be an AgentSession-like object or null.")
    existing_id = session.get("session_id")
    if existing_id is not None and not isinstance(existing_id, str):
        raise ValueError("Legacy session.session_id must be a string or null.")
    if isinstance(existing_id, str) and existing_id.strip() and existing_id != source_session_id:
        raise ValueError("Legacy session.session_id must match source_session_id to preserve external-store identity.")
    session["session_id"] = source_session_id
    session.setdefault("state", {})


def migrate_legacy_state(
    source: dict[str, Any],
    *,
    source_digest: str,
    source_session_id: str,
    migration_id: str,
    ownership_transfer_id: str,
    delivery_window_seconds: int,
    max_state_bytes: int | None = None,
    delivery_evidence: dict[str, Any] | None = None,
    require_known_outcomes: bool = False,
    now: datetime | None = None,
) -> DurableAgentState:
    """Stage a detached legacy migration for an explicit entity migrate operation.

    The parent must enforce an EMPTY, separate destination on an isolated v2 hub,
    quiesce the legacy owner, authorize ownership transfer, and atomically commit
    once with idempotency keyed by the migration request. This function performs
    no model, tool or provider calls, backend writes, or provider-transcript import.
    It does not authorize the supplied IDs or prove source ownership.

    A nonempty scalar ingestedPositions map requires privileged operator-provided
    recorded delivery evidence, not cryptographically proven history. The operator
    must obtain the COMPLETE authoritative accepted-message journal from a quiesced
    legacy deployment, including retained and evicted inputs and every accepted
    revision of a message ID. Migration cannot independently verify the evidence's
    authority or completeness. If that journal is unavailable, keep the old session
    on the old engine rather than guessing. Equal maxima check consistency only;
    no contiguous positions or prefixes are required or inferred.

    Only recorded responses receive completion/mailbox backfill. Surviving legacy
    responses may be partial, not immutable originals. Existing delivery records
    are preserved, never reopened. A fresh grace timestamp is captured once per
    call, not once per migration ID: the parent owns retry idempotency and must not
    repeatedly migrate the same source to refresh grace. Fixed now gives fixed
    backfill timestamps. Existing state parsing owns legacy transcript conversion.

    Args:
        source: Raw exported version-1 state. The caller's object remains untouched.
        source_digest: Lowercase SHA-256 returned by state_snapshot_digest(source).
        source_session_id: Original logical session identity, including its existing namespace.
        migration_id: Nonblank parent-managed idempotency identifier.
        ownership_transfer_id: Nonblank parent-authorized ownership transfer identifier.
        delivery_window_seconds: Positive bounded grace period for legacy response backfill.
        max_state_bytes: Optional positive resolved budget, measured with default ASCII JSON.
            All migrated data and metadata are protected; oversize states fail without pruning.
        delivery_evidence: Exactly sourceDigest, nonblank evidenceId, complete=True and
            messages, a list of complete canonical Message.to_dict() inputs. Unsupported
            or lossy canonical inputs and duplicate ID/fingerprint pairs are rejected.
        require_known_outcomes: Reject imports with unknown invocation outcomes instead
            of using legacy-compatible receipts. Neither mode discards completion evidence.
        now: Offset-aware timestamp for this staging call, defaulting to UTC now.

    Returns:
        A detached version-2 DurableAgentState ready for parent validation and commit.

    Raises:
        ValueError: Invalid input, version, digest, evidence, identity, or timestamp.
        StateCapacityError: The complete staged result exceeds max_state_bytes.
    """
    _nonblank(source_session_id, "source_session_id")
    _nonblank(migration_id, "migration_id")
    _nonblank(ownership_transfer_id, "ownership_transfer_id")
    _positive_int(delivery_window_seconds, "delivery_window_seconds")
    if not isinstance(require_known_outcomes, bool):
        raise ValueError("require_known_outcomes must be a boolean.")
    if max_state_bytes is not None:
        _positive_int(max_state_bytes, "max_state_bytes")
    if not isinstance(source_digest, str) or _SHA256.fullmatch(source_digest) is None:
        raise ValueError("source_digest must be a lowercase SHA-256 snapshot digest.")
    if state_snapshot_digest(source) != source_digest:
        raise ValueError("source_digest does not match the canonical source snapshot.")
    snapshot: dict[str, Any] = json.loads(_canonical_json(source))
    version = snapshot.get("schemaVersion")
    if not isinstance(version, str) or re.fullmatch(r"1\.[0-9]+\.[0-9]+", version) is None:
        raise ValueError("Explicit migration accepts only legacy version-1 state, never a v2 source.")
    raw_data = snapshot.get("data")
    if not isinstance(raw_data, dict):
        raise ValueError("Legacy state data must be an object.")
    positions = _legacy_positions(cast(dict[str, Any], raw_data))
    if positions and delivery_evidence is None:
        raise ValueError(_JOURNAL_REQUIRED)
    timestamp = now if now is not None else datetime.now(timezone.utc)
    if not isinstance(timestamp, datetime) or timestamp.utcoffset() is None:
        raise ValueError("now must be an offset-aware datetime.")
    timestamp = timestamp.astimezone(timezone.utc)
    try:
        _ = timestamp + timedelta(seconds=delivery_window_seconds)
    except OverflowError as exc:
        raise ValueError("delivery_window_seconds exceeds the representable bounded grace period.") from exc

    state = DurableAgentState.from_dict(snapshot)
    if "migration" in state.data.unknown_fields:
        raise ValueError("Legacy state already contains reserved migration metadata; refusing to overwrite it.")
    _validate_receipts(state.data.ingested_messages)
    _preserve_session(state, source_session_id)
    evidence_id: str | None = None
    if delivery_evidence is not None:
        evidence_id, journal = _journal_receipts(delivery_evidence, source_digest=source_digest, positions=positions)
        _apply_journal(state, journal)
    else:
        for identity in _retained_custom_request_ids(state):
            state.data.ingested_messages.setdefault(identity, None)

    # A mailbox is itself a recorded response. Do not replace it from a transcript
    # or refresh its expiry, even if its matching completion receipt was absent.
    for correlation_id, mailbox in state.data.response_mailbox.items():
        # An original mailbox establishes its completion time, unlike a legacy
        # transcript's created_at. Backfill before marking new receipts as legacy.
        state.data.completed_correlations.setdefault(
            correlation_id, {DurableStateFields.COMPLETED_AT: mailbox[DurableStateFields.CREATED_AT]}
        )
    state._backfill_completion_outcomes(require_known=False)  # pyright: ignore[reportPrivateUsage]
    for correlation_id in state.data.response_mailbox:
        if correlation_id not in cast(dict[str, Any], raw_data).get(DurableStateFields.COMPLETED_CORRELATIONS, {}):
            state.data.completed_correlations[correlation_id]["legacy"] = True
    for entry in state.data.conversation_history:
        if isinstance(entry, DurableAgentStateResponse) and entry.correlation_id is not None:
            correlation_id = _nonblank(entry.correlation_id, "Legacy response correlation ID")
            if correlation_id not in state.data.completed_correlations:
                state.record_response(
                    correlation_id,
                    entry.to_run_response(entry),
                    delivery_window_seconds=delivery_window_seconds,
                    now=timestamp,
                    legacy=True,
                )

    state._backfill_completion_outcomes(require_known=require_known_outcomes)  # pyright: ignore[reportPrivateUsage]
    state.schema_version = DurableAgentState.SCHEMA_VERSION
    state.data.unknown_fields["migration"] = {
        "id": migration_id,
        "sourceDigest": source_digest,
        "sourceSessionId": source_session_id,
        "ownershipTransferId": ownership_transfer_id,
        "createdAt": timestamp.isoformat(),
        **({"evidenceId": evidence_id} if evidence_id is not None else {}),
    }
    size = len(json.dumps(state.to_dict(), allow_nan=False))
    if max_state_bytes is not None and size > max_state_bytes:
        raise StateCapacityError(
            size_bytes=size, max_state_bytes=max_state_bytes, floor_bytes=size, target_bytes=max_state_bytes
        )
    return state
