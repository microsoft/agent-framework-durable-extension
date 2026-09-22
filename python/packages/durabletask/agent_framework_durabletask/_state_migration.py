# Copyright (c) Microsoft. All rights reserved.

"""Explicit, detached legacy-to-v2 state migration, with no storage or provider access."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from ._message_identity import message_identity
from ._response_utils import load_agent_response
from ._shared_agent_state import (
    DurableAgentState,
    DurableAgentStateRequest,
    _validate_json,  # pyright: ignore[reportPrivateUsage]
)
from ._shared_state_validation import validate_identifier, validate_shared_data
from ._state_capacity import StateCapacityError

__all__ = ["migrate_legacy_state", "state_snapshot_digest"]

_SHA256 = re.compile(r"[0-9a-f]{64}")
_EVIDENCE_FIELDS = {"sourceDigest", "evidenceId", "complete", "messages"}
_COMPLETION_EVIDENCE_FIELDS = {"sourceDigest", "evidenceId", "complete", "results"}
_JOURNAL_REQUIRED = (
    "Legacy ingestedPositions require recorded delivery evidence: a complete authoritative accepted-message "
    "journal from the quiesced legacy deployment, including evicted messages. If that journal is unavailable, "
    "keep the old session on the old engine rather than guessing delivery receipts."
)
_COMPLETION_REQUIRED = (
    "Legacy completion outcomes require authoritative completion evidence. Retained responses may be partial, "
    "and an accepted-message delivery journal does not establish invocation outcomes or recover lost completions. "
    "Supply a complete completion journal from the quiesced legacy deployment, including original results and "
    "authoritative completion timestamps for pruned responses. Otherwise keep the session on the old engine or "
    "explicitly start a new session generation. require_known_outcomes=False cannot waive the shared target contract."
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


def _legacy_positions(data: dict[str, Any]) -> dict[str, int]:
    raw = data.get("ingestedPositions", {})
    if not isinstance(raw, dict):
        raise ValueError("Legacy ingestedPositions must be an object of nonnegative integer producer positions.")
    positions: dict[str, int] = {}
    for producer, position in cast(dict[str, Any], raw).items():
        _nonblank(producer, "ingestedPositions producer")
        # The published JSON Schema integer type admits integral floating-point
        # representations. Normalize only the detached comparison value.
        if isinstance(position, float) and position.is_integer():
            position = int(position)
        if isinstance(position, bool) or not isinstance(position, int) or position < 0:
            raise ValueError("Every ingestedPositions position must be a nonnegative integer, not a boolean.")
        positions[producer] = position
    return positions


def _validate_receipts(receipts: dict[str, list[str] | None]) -> None:
    for identity, fingerprints in receipts.items():
        _nonblank(identity, "ingestedMessages ID")
        if fingerprints is None:
            continue
        if not fingerprints or any(_SHA256.fullmatch(value) is None for value in fingerprints):
            raise ValueError("ingestedMessages requires nonempty lists of lowercase SHA-256 fingerprints.")
        if len(set(fingerprints)) != len(fingerprints):
            raise ValueError("ingestedMessages must not contain duplicate fingerprints.")


def _journal_receipts(
    evidence: dict[str, Any], *, source_digest: str, positions: dict[str, int]
) -> tuple[str, dict[str, list[str]]]:
    if not isinstance(evidence, dict) or evidence.keys() not in (
        _EVIDENCE_FIELDS,
        _EVIDENCE_FIELDS | {"messagePositions"},
    ):
        raise ValueError(
            "Recorded delivery evidence requires exactly sourceDigest, evidenceId, complete and messages, "
            "with only messagePositions optional."
        )
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
    messages = cast(list[Any], messages)
    if "messagePositions" not in journal:
        if positions:
            raise ValueError(
                "Recorded delivery evidence requires explicit messagePositions for legacy ingestedPositions. "
                "Public message IDs do not establish cursor provenance."
            )
        attribution: list[Any] = [None] * len(messages)
    else:
        raw_attribution = journal["messagePositions"]
        if not isinstance(raw_attribution, list):
            raise ValueError("Recorded delivery evidence messagePositions must contain one entry per message.")
        attribution = cast(list[Any], raw_attribution)
        if len(attribution) != len(messages):
            raise ValueError("Recorded delivery evidence messagePositions must contain one entry per message.")

    receipts: dict[str, list[str]] = {}
    maxima: dict[str, int] = {}
    for raw, position_record in zip(messages, attribution, strict=True):
        # Attribution comes from the authoritative accepted-input journal, not
        # from public ID spelling, request orchestration IDs or retained bodies.
        if position_record is not None:
            if not isinstance(position_record, dict) or position_record.keys() != {"producer", "position"}:
                raise ValueError(
                    "Recorded delivery evidence messagePositions entries must be null or exactly producer and position."
                )
            position_record = cast(dict[str, Any], position_record)
            producer = _nonblank(position_record["producer"], "Recorded delivery evidence messagePositions producer")
            position = position_record["position"]
            if isinstance(position, bool) or not isinstance(position, int) or position < 0:
                raise ValueError(
                    "Recorded delivery evidence messagePositions position must be a nonnegative integer, not a boolean."
                )
            maxima[producer] = max(maxima.get(producer, position), position)
        if not isinstance(raw, dict):
            raise ValueError("Recorded delivery evidence messages must contain canonical message objects.")
        raw = cast(dict[str, Any], raw)
        identity = _nonblank(raw.get("message_id"), "Recorded delivery evidence message_id")
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

    # This is only a consistency check. Sparse positions are valid; a maximum is never
    # proof of a complete prefix, nor proof that the operator's journal is complete.
    if maxima != positions:
        raise ValueError(
            "Recorded delivery evidence workflow producers and maximum positions must match ingestedPositions."
        )
    return evidence_id, receipts


def _retained_request_ids(state: DurableAgentState) -> Iterator[str]:
    """Yield opaque public lookup IDs, not reconciliation IDs or inferred provenance."""
    for entry in state.data.conversation_history:
        if isinstance(entry, DurableAgentStateRequest):
            for message in entry.messages:
                identity = message.public_message_id
                if identity is not None:
                    if isinstance(identity, str) and not identity.strip():
                        continue
                    yield _nonblank(identity, "Legacy request message ID")


def _apply_journal(state: DurableAgentState, journal: dict[str, list[str]]) -> None:
    receipts = state.data.ingested_messages
    for identity, existing in receipts.items():
        recorded = journal.get(identity)
        if recorded is None or (existing is not None and not set(existing).issubset(recorded)):
            raise ValueError("Recorded delivery evidence is inconsistent with existing ingestedMessages receipts.")
    for identity in _retained_request_ids(state):
        if identity not in journal:
            raise ValueError("Recorded delivery evidence must include every retained legacy request message ID.")
    for identity, recorded in journal.items():
        existing = receipts.get(identity)
        if existing is None:
            receipts[identity] = list(recorded)
        else:
            existing.extend(fingerprint for fingerprint in recorded if fingerprint not in existing)


def _response_evidence(response: dict[str, Any]) -> tuple[bool, bool]:
    """Read failure and availability evidence from validated shared JSON without Core projection.

    Only response extensionData and non-tool error contents have receipt meaning.
    Unknown siblings, profiles and nested business values remain inert. Missing
    failure evidence never establishes success in a possibly partial transcript.
    """
    status = response.get("extensionData", {}).get("durable_status")
    failed = status == "error"
    acknowledgement = status in ("accepted", "already_completed")
    for message in response.get("messages", []):
        if message["role"] == "tool":
            continue
        for content in message.get("contents", []):
            if content["$type"] != "error":
                continue
            if content.get("errorCode") == "response_expired":
                acknowledgement = True
            else:
                failed = True
    return failed, acknowledgement


def _completion_journal(
    evidence: dict[str, Any], *, source_digest: str, expires_at: str
) -> tuple[str, dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Detach original shared terminal results and build matching new-grace receipts.

    Completeness and authoritative original completedAt are operator assertions,
    not facts derivable from the retained transcript or the conversion clock.
    """
    if not isinstance(evidence, dict) or evidence.keys() != _COMPLETION_EVIDENCE_FIELDS:
        raise ValueError("Completion evidence requires exactly sourceDigest, evidenceId, complete and results.")
    journal: dict[str, Any] = json.loads(_canonical_json(evidence))
    if journal["sourceDigest"] != source_digest:
        raise ValueError("Completion evidence sourceDigest does not match the source snapshot.")
    evidence_id = _nonblank(journal["evidenceId"], "Completion evidence evidenceId")
    if journal["complete"] is not True:
        raise ValueError("Completion evidence requires the explicit complete=True operator assertion.")
    if not isinstance(journal["results"], list):
        raise ValueError("Completion evidence results must be a list of original shared terminal results.")

    results: dict[str, dict[str, Any]] = {}
    receipts: dict[str, dict[str, Any]] = {}
    for value in cast(list[Any], journal["results"]):
        if not isinstance(value, dict):
            raise ValueError("Completion evidence results must contain canonical shared terminal result objects.")
        result = cast(dict[str, Any], value)
        correlation_id = result.get("correlationId")
        validate_identifier(correlation_id, "Completion evidence correlationId")
        correlation_id = cast(str, correlation_id)
        if correlation_id in results:
            raise ValueError("Completion evidence contains a duplicate correlationId.")
        if "resultExpiresAt" in result:
            raise ValueError("Completion evidence must not contain resultExpiresAt, including null.")
        # Do not call record_response: that API records a *new* completion at now.
        # All original fields, including absent optionals and opaque siblings,
        # survive at their exact locations. Only the new grace deadline is added.
        results[correlation_id] = {**result, "resultExpiresAt": expires_at}
        receipts[correlation_id] = {
            "correlationId": correlation_id,
            "outcome": result.get("outcome"),
            "completedAt": result.get("completedAt"),
            "resultExpiresAt": expires_at,
            "resultState": "available",
        }
    validate_shared_data({
        "conversationHistory": [],
        "terminalResults": results,
        "completionReceipts": receipts,
    })
    for result in results.values():
        failed, acknowledgement = _response_evidence(result["response"])
        if acknowledgement:
            raise ValueError(
                "Completion evidence requires original terminal results, not availability acknowledgements."
            )
        if failed and result["outcome"] != "failed":
            raise ValueError("Completion evidence outcome conflicts with its response failure evidence.")
    return evidence_id, results, receipts


def _validate_retained_completions(history: list[Any], results: dict[str, dict[str, Any]]) -> None:
    """Require journal coverage and check every retained response, including duplicates."""
    for entry in history:
        if entry["$type"] not in ("response", "errorResponse"):
            continue
        correlation_id = entry.get("correlationId")
        validate_identifier(correlation_id, "Legacy response correlationId")
        result = results.get(correlation_id)
        if result is None:
            raise ValueError("Completion evidence must include every retained response/errorResponse correlationId.")
        failed, _ = _response_evidence(entry)
        if (entry["$type"] == "errorResponse" or failed) and result["outcome"] != "failed":
            raise ValueError("Completion evidence outcome conflicts with retained response failure evidence.")


def _preserve_session(state: DurableAgentState, source_session_id: str) -> None:
    session = state.data.session
    if session is None:
        state.data.session = {"session_id": source_session_id, "state": {}}
        return
    if not isinstance(session, dict):
        raise ValueError("Legacy session must be an AgentSession-like object when present.")
    existing_id = session.get("session_id")
    if existing_id is not None and not isinstance(existing_id, str):
        raise ValueError("Legacy session.session_id must be a string or null.")
    if isinstance(existing_id, str) and existing_id.strip() and existing_id != source_session_id:
        raise ValueError("Legacy session.session_id must match source_session_id to preserve external-store identity.")
    if not isinstance(session.get("state", {}), dict):
        raise ValueError("Legacy session.state must be an object when present.")
    service_session_id = session.get("service_session_id")
    # Core's ServiceSessionId is Mapping[str, Any], not a provider-specific schema.
    # The source snapshot already enforces strict JSON, including string keys.
    if service_session_id is not None and not isinstance(service_session_id, (str, dict)):
        raise ValueError("Legacy session.service_session_id must be a string, object or null.")
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
    completion_evidence: dict[str, Any] | None = None,
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
    no contiguous positions or prefixes are required or inferred. Cursor attribution
    is supplied separately in messagePositions, never inferred from public ID spelling.
    Every retained nonblank public request ID must be covered by a supplied journal.
    Without a journal, retained IDs preserve identity-only compatibility markers,
    not exact acceptance fingerprints or evidence for evicted inputs.

    Any retained history, truncation, nonempty session or ingestedPositions, or
    nonempty supplied accepted-message journal requires a
    COMPLETE authoritative completion journal from the quiesced legacy deployment,
    including original terminal results no longer in retained history. An empty
    results list is an operator assertion of no completions, valid only without
    retained responses. Only a fresh source without history, session, truncation or ingestion
    can omit this journal. Authority and completeness cannot be proven here.
    Every retained response must be covered, and affirmative failure evidence must
    agree with its journal outcome. Partial non-error content never proves success.
    Delivery evidence proves accepted inputs only, independently of this journal.
    No private prototype completion maps or unversioned ingestedMessages are read.

    Any legacy data.pythonIngestion field is rejected before parsing, including
    null, empty, foreign and recognized profiles. It must not become active v2
    bookkeeping. Session state must be an object when present, defaulting to {}
    only when absent. service_session_id may be absent, null, any string or a JSON
    object, matching Core's str | Mapping[str, Any] | None contract. Objects have
    no required keys or string-only value restriction. Empty strings and objects,
    session values and unknown siblings remain JSON, without Core deserialization.
    Provider continuation compatibility is separate from session restoration.

    Original journal completedAt strings are immutable. Neither transcript createdAt
    nor the migration clock supplies a completion time. New matching result/receipt
    grace deadlines use now + delivery_window_seconds and cannot precede completedAt.
    Canonical result JSON is retained without conversion through Core responses.
    The parent owns retry idempotency and must not refresh grace by re-migrating.

    Args:
        source: Raw exported shared 1.0.0, 1.1.0 or 1.2.0 state, without v2 maps.
            The caller's object remains untouched. No version-2 source is migrated.
        source_digest: Lowercase SHA-256 returned by state_snapshot_digest(source).
        source_session_id: Original logical session identity, including its existing namespace.
        migration_id: Nonblank parent-managed idempotency identifier.
        ownership_transfer_id: Nonblank parent-authorized ownership transfer identifier.
        delivery_window_seconds: Positive bounded grace period for imported original results.
        max_state_bytes: Optional positive resolved budget, measured with default ASCII JSON.
            All migrated data and metadata are protected; oversize states fail without pruning.
        delivery_evidence: Required sourceDigest, nonblank evidenceId, complete=True and
            messages, a list of complete canonical Message.to_dict() inputs. The only
            optional key, messagePositions, is required for nonempty ingestedPositions.
            It is aligned one-for-one with messages, each entry null for an unpositioned
            input or exactly producer (nonblank string) and position (nonnegative integer,
            not boolean) from authoritative acceptance records. Omission means no inputs
            are positioned. Unsupported or lossy canonical inputs and duplicate
            ID/fingerprint pairs are rejected. No ID-to-position naming rule is imposed.
        completion_evidence: Exactly sourceDigest, nonblank evidenceId, complete=True and
            results, a list of canonical shared terminalResult objects with correlationId,
            known outcome, authoritative original completedAt, response.messages and error
            for failures. resultExpiresAt is forbidden. Unknown JSON fields are preserved.
            The digest binds the raw source before defaults. Acknowledgements, duplicate
            correlations, malformed results and contradictory failure evidence are rejected.
        require_known_outcomes: Deprecated compatibility argument that must be a boolean.
            False cannot waive the shared target's requirement for known outcomes.
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
    if not isinstance(version, str) or version not in ("1.0.0", "1.1.0", "1.2.0"):
        raise ValueError("Explicit migration accepts only legacy shared 1.0.0, 1.1.0 or 1.2.0, never a v2 source.")
    raw_data = snapshot.get("data")
    if not isinstance(raw_data, dict):
        raise ValueError("Legacy state data must be an object.")
    raw_data = cast(dict[str, Any], raw_data)
    if "pythonIngestion" in raw_data:
        raise ValueError(
            "Legacy data contains reserved pythonIngestion metadata; explicit migration does not accept it."
        )
    # History is optional in the legacy schema but required by the target. Add
    # only an absent array on the detached snapshot, never replace a present value.
    raw_data.setdefault("conversationHistory", [])
    # The parent loader validates the published shared source before projection.
    # Its raw shadows preserve unknown fields and optional-field representations.
    state = DurableAgentState.from_dict(snapshot)
    positions = _legacy_positions(raw_data)
    if positions and delivery_evidence is None:
        raise ValueError(_JOURNAL_REQUIRED)
    timestamp = now if now is not None else datetime.now(timezone.utc)
    if not isinstance(timestamp, datetime) or timestamp.utcoffset() is None:
        raise ValueError("now must be an offset-aware datetime.")
    timestamp = timestamp.astimezone(timezone.utc)
    try:
        expires_at = (timestamp + timedelta(seconds=delivery_window_seconds)).isoformat()
    except OverflowError as exc:
        raise ValueError("delivery_window_seconds exceeds the representable bounded grace period.") from exc

    if "migration" in state.data.unknown_fields:
        raise ValueError("Legacy state already contains reserved migration metadata; refusing to overwrite it.")
    if completion_evidence is None and (
        raw_data["conversationHistory"] or "truncation" in raw_data or positions or raw_data.get("session")
    ):
        raise ValueError(_COMPLETION_REQUIRED)
    completion_evidence_id: str | None = None
    results: dict[str, dict[str, Any]] = {}
    receipts: dict[str, dict[str, Any]] = {}
    if completion_evidence is not None:
        completion_evidence_id, results, receipts = _completion_journal(
            completion_evidence, source_digest=source_digest, expires_at=expires_at
        )
    # Validate the target wire shape before reading response evidence. Legacy
    # unknown/missing entry discriminators must not be disguised as writable v2.
    validate_shared_data({
        **raw_data,
        "terminalResults": results,
        "completionReceipts": receipts,
    })
    _validate_retained_completions(raw_data["conversationHistory"], results)
    _validate_receipts(state.data.ingested_messages)
    _preserve_session(state, source_session_id)
    evidence_id: str | None = None
    if delivery_evidence is not None:
        evidence_id, journal = _journal_receipts(delivery_evidence, source_digest=source_digest, positions=positions)
        if journal and completion_evidence is None:
            raise ValueError(_COMPLETION_REQUIRED)
        _apply_journal(state, journal)
    else:
        for identity in _retained_request_ids(state):
            state.data.ingested_messages.setdefault(identity, None)

    # Switch the detached root before emitting or validating target maps. The
    # data shadow must merge those new fields rather than revalidate as legacy.
    state.schema_version = DurableAgentState.SCHEMA_VERSION
    state.data.response_mailbox = results
    state.data.completed_correlations = receipts
    state.data.unknown_fields["migration"] = {
        "id": migration_id,
        "sourceDigest": source_digest,
        "sourceSessionId": source_session_id,
        "ownershipTransferId": ownership_transfer_id,
        "createdAt": timestamp.isoformat(),
        **({"evidenceId": evidence_id} if evidence_id is not None else {}),
        **({"completionEvidenceId": completion_evidence_id} if completion_evidence_id is not None else {}),
    }
    size = len(json.dumps(state.to_dict(), allow_nan=False))
    if max_state_bytes is not None and size > max_state_bytes:
        raise StateCapacityError(
            size_bytes=size, max_state_bytes=max_state_bytes, floor_bytes=size, target_bytes=max_state_bytes
        )
    return state
