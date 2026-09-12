# Copyright (c) Microsoft. All rights reserved.

"""Detached explicit migration contracts. No hosts, providers or backends are needed."""

import hashlib
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from agent_framework import AgentResponse, Content, Message
from typing_extensions import Self

from agent_framework_durabletask import migrate_legacy_state, state_snapshot_digest
from agent_framework_durabletask._durable_agent_state import DurableAgentState, DurableAgentStateEntryJsonType
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._retention import StateCapacityError
from agent_framework_durabletask._workflows.naming import workflow_message_id

NOW = datetime(2026, 9, 9, 12, tzinfo=timezone.utc)
OLD = datetime(2024, 1, 1, tzinfo=timezone.utc)
SESSION_ID = "dafx-agent:original-session"
WINDOW = 60


def _entry(kind: str, correlation: str, *, message_id: str = "custom-id") -> dict[str, Any]:
    return {
        "$type": kind,
        "correlationId": correlation,
        "createdAt": OLD.isoformat(),
        "messages": [
            {
                "role": "user" if kind == "request" else "assistant",
                "messageId": message_id,
                "contents": [{"$type": "text", "text": "retained portion"}],
            }
        ],
    }


def _source() -> dict[str, Any]:
    return {
        "schemaVersion": "1.1.0",
        "data": {
            "conversationHistory": [
                _entry("request", "done"),
                _entry("response", "done", message_id="answer-id"),
            ]
        },
    }


def _migrate(source: dict[str, Any], **overrides: Any) -> DurableAgentState:
    options: dict[str, Any] = {
        "source_digest": state_snapshot_digest(source),
        "source_session_id": SESSION_ID,
        "migration_id": "migration-1",
        "ownership_transfer_id": "transfer-1",
        "delivery_window_seconds": WINDOW,
        "now": NOW,
    }
    options.update(overrides)
    return migrate_legacy_state(source, **options)


def _message(position: int, *, producer: str = "upstream", text: str = "accepted") -> Message:
    return Message(
        "user",
        [Content.from_text(text, additional_properties={"nested": {"labels": ["original"]}})],
        message_id=workflow_message_id(producer, position),
        author_name="author",
        additional_properties={"nested": {"labels": ["message"]}},
    )


def _evidence(source: dict[str, Any], messages: list[Message]) -> dict[str, Any]:
    return {
        "sourceDigest": state_snapshot_digest(source),
        "evidenceId": "operator-journal-1",
        "complete": True,
        "messages": [message.to_dict() for message in messages],
    }


def _cold(state: DurableAgentState) -> DurableAgentState:
    return DurableAgentState.from_json(json.dumps(state.to_dict(), allow_nan=False))


def _existing_delivery() -> dict[str, Any]:
    state = DurableAgentState()
    state.record_response(
        "done",
        AgentResponse(messages=[Message("assistant", ["original mailbox"])]),
        delivery_window_seconds=WINDOW,
        now=OLD,
    )
    return state.to_dict()["data"]


def test_source_digest_uses_complete_strict_canonical_utf8_json() -> None:
    source: dict[str, Any] = {
        "schemaVersion": "1.1.0",
        "data": {"conversationHistory": [], "future": ["é", "雪", 0, False]},
    }
    expected = hashlib.sha256(
        json.dumps(source, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    ).hexdigest()
    reordered: dict[str, Any] = {
        "data": {"future": source["data"]["future"], "conversationHistory": []},
        "schemaVersion": "1.1.0",
    }
    assert state_snapshot_digest(source) == state_snapshot_digest(reordered) == expected
    assert expected != hashlib.sha256(json.dumps(source, sort_keys=True).encode()).hexdigest()
    reordered["data"]["future"] = list(reversed(source["data"]["future"]))
    assert state_snapshot_digest(reordered) != expected


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), float("-inf"), (1, 2), {1: "key"}, {1, 2}])
def test_digest_rejects_non_json_and_nonfinite_nested_values(invalid: Any) -> None:
    source = _source()
    source["data"]["future"] = {"nested": invalid}
    with pytest.raises(ValueError, match="strict JSON"):
        state_snapshot_digest(source)


def test_digest_rejects_cycles_and_nonobject_source() -> None:
    source: dict[str, Any] = {}
    source["cycle"] = source
    with pytest.raises(ValueError, match="strict JSON"):
        state_snapshot_digest(source)
    with pytest.raises(ValueError, match="JSON object"):
        state_snapshot_digest([])  # type: ignore[arg-type]


@pytest.mark.parametrize("kind", list(DurableAgentStateEntryJsonType))
def test_only_recorded_response_kinds_backfill_completion(kind: str) -> None:
    source = _source()
    source["data"]["conversationHistory"] = [_entry(kind, "done")]
    result = _cold(_migrate(source))
    is_response = kind in (DurableAgentStateEntryJsonType.RESPONSE, DurableAgentStateEntryJsonType.ERROR_RESPONSE)
    assert ("done" in result.data.completed_correlations) is is_response
    assert ("done" in result.data.response_mailbox) is is_response
    assert result.to_dict()["data"]["conversationHistory"] == source["data"]["conversationHistory"]
    if is_response:
        assert result.data.completed_correlations["done"] == {
            "completedAt": NOW.isoformat(),
            "legacy": True,
            **({"outcome": "failed"} if kind == DurableAgentStateEntryJsonType.ERROR_RESPONSE else {}),
        }
        mailbox = result.data.response_mailbox["done"]
        assert mailbox["createdAt"] == NOW.isoformat()
        assert mailbox["expiresAt"] == (NOW + timedelta(seconds=WINDOW)).isoformat()
        assert mailbox["response"]["messages"][0]["contents"][0]["text"] == "retained portion"
        assert mailbox["response"]["created_at"] == OLD.isoformat()


def test_partial_and_contentless_recorded_responses_are_not_claimed_as_originals() -> None:
    source = _source()
    history = source["data"]["conversationHistory"]
    history[1]["usage"] = {"inputTokenCount": 3, "futureUsage": {"keep": [1]}}
    history.append(_entry("response", "empty"))
    history[-1]["messages"] = []
    history.append(_entry("request", "old-pruned-without-response"))
    history[-1]["messages"][0]["contents"] = []
    history.append(_entry("request", "unfinished"))
    source["data"]["truncation"] = {"evictedMessageCount": 20, "future": [1]}
    result = _cold(_migrate(source))

    assert set(result.data.completed_correlations) == {"done", "empty"}
    assert set(result.data.response_mailbox) == {"done", "empty"}
    assert result.data.response_mailbox["empty"]["response"]["messages"] == []
    assert result.data.response_mailbox["done"]["response"]["usage_details"] == {"input_token_count": 3}
    assert all(record["legacy"] is True for record in result.data.completed_correlations.values())
    assert result.try_get_agent_response("old-pruned-without-response") is None
    assert result.try_get_agent_response("unfinished") is None
    assert result.to_dict()["data"]["conversationHistory"] == history


@pytest.mark.parametrize("keep_mailbox", [False, True])
def test_existing_completion_and_mailbox_are_not_overwritten_or_reopened(keep_mailbox: bool) -> None:
    source = _source()
    delivery = _existing_delivery()
    delivery["completedCorrelations"]["done"]["future"] = {"keep": [1]}
    source["data"]["completedCorrelations"] = delivery["completedCorrelations"]
    if keep_mailbox:
        delivery["responseMailbox"]["done"]["response"]["futureResponse"] = {"keep": [2]}
        source["data"]["responseMailbox"] = delivery["responseMailbox"]
    result = _cold(_migrate(source))
    assert result.data.completed_correlations == delivery["completedCorrelations"]
    assert result.data.response_mailbox == (delivery["responseMailbox"] if keep_mailbox else {})


def test_existing_mailbox_without_receipt_is_preserved_with_completion_backfill() -> None:
    source = _source()
    source["data"]["responseMailbox"] = _existing_delivery()["responseMailbox"]
    result = _cold(_migrate(source))
    assert result.data.response_mailbox == source["data"]["responseMailbox"]
    assert result.data.completed_correlations["done"] == {
        "completedAt": OLD.isoformat(),
        "legacy": True,
        "outcome": "succeeded",
    }


def test_sparse_journal_preserves_exact_revisions_not_an_inferred_prefix_after_cold_reload() -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"upstream": 3}
    # Neither the original accepted input nor the missing position can be recovered
    # from this compacted/pruned transcript. It must not contribute fingerprints.
    source["data"]["conversationHistory"][0]["messages"] = [
        {"role": "user", "messageId": workflow_message_id("upstream", 3), "contents": []}
    ]
    first, third, revision = _message(1), _message(3), _message(3, text="accepted revision")
    assert first.message_id is not None
    assert third.message_id is not None
    source["data"]["ingestedMessages"] = {third.message_id: [message_identity(third)]}
    evidence = _evidence(source, [revision, first, third])
    before_source, before_evidence = deepcopy(source), deepcopy(evidence)
    result = _cold(_migrate(source, delivery_evidence=evidence))

    assert result.data.ingested_messages == {
        first.message_id: [message_identity(first)],
        third.message_id: [message_identity(third), message_identity(revision)],
    }
    assert workflow_message_id("upstream", 0) not in result.data.ingested_messages
    assert workflow_message_id("upstream", 2) not in result.data.ingested_messages
    fingerprints = result.data.ingested_messages[third.message_id]
    assert fingerprints is not None
    assert message_identity(_message(3, text="new revision")) not in fingerprints
    assert result.data.ingested_positions == {"upstream": 3}
    assert result.data.unknown_fields["migration"]["evidenceId"] == "operator-journal-1"
    assert source == before_source and evidence == before_evidence
    evidence["messages"][0]["contents"][0]["additional_properties"]["nested"]["labels"].append("caller edit")
    assert result.data.ingested_messages[third.message_id] == [message_identity(third), message_identity(revision)]


def test_multiple_producers_allow_sparse_zero_based_and_out_of_order_journal() -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"first_with_underscores": 9, "other": 0}
    # Supply the accepted custom input explicitly, not a fingerprint inferred from its retained portion.
    custom = Message("user", ["complete original accepted input"], message_id="custom-id")
    messages = [_message(9, producer="first_with_underscores"), _message(0, producer="other"), custom]
    result = _cold(_migrate(source, delivery_evidence=_evidence(source, messages)))
    assert result.data.ingested_messages == {message.message_id: [message_identity(message)] for message in messages}


@pytest.mark.parametrize("position", [0, 3])
def test_scalar_positions_without_complete_journal_fail_even_with_retained_messages(position: int) -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"upstream": position}
    source["data"]["conversationHistory"][0]["messages"][0]["messageId"] = workflow_message_id("upstream", position)
    source["data"]["ingestedMessages"] = {workflow_message_id("upstream", position): ["a" * 64]}
    before = deepcopy(source)
    with pytest.raises(ValueError, match="recorded delivery evidence.*old engine"):
        _migrate(source)
    assert source == before


def test_no_scalar_no_journal_uses_only_custom_id_markers_and_preserves_exact_receipts() -> None:
    source = _source()
    messages = source["data"]["conversationHistory"][0]["messages"]
    messages.extend([
        {"role": "user", "messageId": "cleared-custom", "contents": []},
        {"role": "user", "messageId": workflow_message_id("upstream", 3), "contents": []},
        {"role": "user", "messageId": "already-exact", "contents": []},
    ])
    source["data"]["ingestedMessages"] = {"already-exact": ["b" * 64, "a" * 64], "old-marker": None}
    result = _cold(_migrate(source))
    assert result.data.ingested_messages == {
        "already-exact": ["b" * 64, "a" * 64],
        "old-marker": None,
        "custom-id": None,
        "cleared-custom": None,
    }
    assert "answer-id" not in result.data.ingested_messages


@pytest.mark.parametrize("identity", [None, "", " "])
def test_anonymous_legacy_request_ids_are_preserved_without_receipts(identity: Any) -> None:
    source = _source()
    message = source["data"]["conversationHistory"][0]["messages"][0]
    if identity is None:
        message.pop("messageId")
    else:
        message["messageId"] = identity
    result = _cold(_migrate(source))
    assert result.data.ingested_messages == {}
    assert result.to_dict()["data"]["conversationHistory"][0] == source["data"]["conversationHistory"][0]


def test_complete_custom_journal_replaces_identity_marker_with_content_sensitive_revisions() -> None:
    source = _source()
    source["data"]["ingestedMessages"] = {"custom-id": None}
    old = Message("user", ["original"], message_id="custom-id")
    revised = Message("user", ["revision"], message_id="custom-id")
    result = _cold(_migrate(source, delivery_evidence=_evidence(source, [old, revised])))
    assert result.data.ingested_messages == {"custom-id": [message_identity(old), message_identity(revised)]}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("sourceDigest", "a" * 64),
        ("evidenceId", " "),
        ("evidenceId", None),
        ("complete", False),
        ("complete", 1),
        ("complete", "true"),
        ("messages", {}),
        ("extra", []),
    ],
)
def test_evidence_binding_envelope_completeness_and_extra_fields_are_validated(field: str, value: Any) -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"upstream": 3}
    evidence = _evidence(source, [_message(3)])
    evidence[field] = value
    before = deepcopy(evidence)
    with pytest.raises(ValueError, match="[Rr]ecorded delivery evidence"):
        _migrate(source, delivery_evidence=evidence)
    assert evidence == before


@pytest.mark.parametrize("field", ["sourceDigest", "evidenceId", "complete", "messages"])
def test_all_evidence_fields_are_required(field: str) -> None:
    source = _source()
    evidence = _evidence(source, [])
    del evidence[field]
    with pytest.raises(ValueError, match="requires exactly"):
        _migrate(source, delivery_evidence=evidence)


@pytest.mark.parametrize(
    "messages",
    [[], [_message(1)], [_message(4)], [_message(3, producer="other")], [_message(3), _message(0, producer="extra")]],
)
def test_evidence_workflow_producer_set_and_maxima_must_match(messages: list[Message]) -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"upstream": 3}
    with pytest.raises(ValueError, match="producers and maximum positions"):
        _migrate(source, delivery_evidence=_evidence(source, messages))


@pytest.mark.parametrize(
    "positions",
    [None, [], False, {"upstream": True}, {"upstream": -1}, {"upstream": 1.5}, {"upstream": "3"}, {"": 0}, {" ": 0}],
)
def test_all_legacy_cursor_entries_require_named_producers_and_nonbool_nonnegative_ints(positions: Any) -> None:
    source = _source()
    source["data"]["ingestedPositions"] = positions
    with pytest.raises(ValueError, match="ingestedPositions"):
        _migrate(source, delivery_evidence=_evidence(source, []))


@pytest.mark.parametrize(
    "identity", [None, "", "  ", True, 7, "wf_upstream_-1", "wf_upstream_true", "wf__3", "wf_upstream_3\n"]
)
def test_evidence_rejects_missing_blank_nonstring_and_malformed_workflow_ids(identity: Any) -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"upstream": 3}
    evidence = _evidence(source, [_message(3)])
    evidence["messages"][0]["message_id"] = identity
    with pytest.raises(ValueError):
        _migrate(source, delivery_evidence=evidence)


def test_exact_duplicate_evidence_is_rejected_but_revisions_are_not() -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"upstream": 3}
    message = _message(3)
    with pytest.raises(ValueError, match="duplicate message ID/fingerprint"):
        _migrate(source, delivery_evidence=_evidence(source, [message, message]))


@pytest.mark.parametrize("change", ["unknown-field", "raw-representation", "wrong-contents", "bad-content", "bad-role"])
def test_journal_never_hashes_a_lossy_or_malformed_message_projection(change: str) -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"upstream": 3}
    evidence = _evidence(source, [_message(3)])
    message = evidence["messages"][0]
    if change == "unknown-field":
        message["future_unrecognized_message_field"] = {"must-not-disappear": [1]}
    elif change == "raw-representation":
        message["raw_representation"] = {"must-not-disappear": [1]}
    elif change == "wrong-contents":
        message["contents"] = {}
    elif change == "bad-content":
        message["contents"] = [{"type": ""}]
    else:
        message["role"] = ""
    with pytest.raises(ValueError):
        _migrate(source, delivery_evidence=evidence)


@pytest.mark.parametrize("mutation", ["author", "role", "text", "message-metadata", "content-metadata"])
def test_journal_fingerprints_cover_complete_canonical_inputs(mutation: str) -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"upstream": 3}
    original = _message(3)
    changed = deepcopy(original)
    if mutation == "author":
        changed.author_name = "different"
    elif mutation == "role":
        changed.role = "assistant"
    elif mutation == "text":
        changed.contents[0].text = "different"
    elif mutation == "message-metadata":
        changed.additional_properties["nested"]["labels"].append("different")
    else:
        changed.contents[0].additional_properties["nested"]["labels"].append("different")
    custom = Message("user", ["complete original accepted input"], message_id="custom-id")
    result = _cold(_migrate(source, delivery_evidence=_evidence(source, [original, changed, custom])))
    assert message_identity(original) != message_identity(changed)
    assert original.message_id is not None
    assert result.data.ingested_messages == {
        original.message_id: [message_identity(original), message_identity(changed)],
        "custom-id": [message_identity(custom)],
    }


@pytest.mark.parametrize(
    "receipts",
    [
        {"custom": []},
        {"custom": ["short"]},
        {"custom": ["A" * 64]},
        {"custom": ["g" * 64]},
        {"custom": [True]},
        {"custom": ["a" * 64, "a" * 64]},
        {"custom": "a" * 64},
        {" ": ["a" * 64]},
        {"wf_upstream_3": None},
    ],
)
def test_existing_receipts_reject_invalid_fingerprint_shapes_and_workflow_markers(receipts: Any) -> None:
    source = _source()
    source["data"]["ingestedMessages"] = receipts
    with pytest.raises(ValueError):
        _migrate(source)


@pytest.mark.parametrize("receipt", [{"other-custom": None}, {"wf_upstream_3": ["a" * 64]}])
def test_complete_journal_must_include_existing_identity_and_exact_receipts(receipt: Any) -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"upstream": 3}
    source["data"]["ingestedMessages"] = receipt
    with pytest.raises(ValueError, match="inconsistent with existing"):
        _migrate(source, delivery_evidence=_evidence(source, [_message(3)]))


def test_raw_unknown_nested_fields_order_session_and_source_are_preserved() -> None:
    source = _source()
    source["futureRoot"] = {"nested": [None, {"keep": "雪"}]}
    data = source["data"]
    data["futureData"] = {"nested": [1]}
    data["session"] = {
        "session_id": SESSION_ID,
        "service_session_id": "original-provider-thread",
        "state": {"external-store": {"provider-key": "original", "nested": [2]}},
        "futureSession": [3],
    }
    data["extensionData"] = {"keep": [4]}
    history = data["conversationHistory"]
    history[0]["futureEntry"] = {"nested": [5]}
    history[0]["messages"][0]["futureMessage"] = {"nested": [6]}
    history[0]["messages"][0]["contents"][0]["futureContent"] = {"nested": [7]}
    history[1]["usage"] = {"inputTokenCount": 0, "futureUsage": {"nested": [8]}}
    history.append({"$type": "futureEntryKind", "opaque": [None], "messages": {"unknown": [9]}})
    history[0]["messages"][0]["contents"].append({"$type": "futureContentKind", "payload": None})
    before = deepcopy(source)
    migrated = _migrate(source)
    result = _cold(migrated)
    serialized = result.to_dict()
    assert serialized["futureRoot"] == source["futureRoot"]
    for key in ("futureData", "session", "extensionData", "conversationHistory"):
        assert serialized["data"][key] == data[key]
    assert serialized["schemaVersion"] == "2.0.0"
    assert source == before

    assert migrated.data.session is not None
    migrated.data.session["state"]["external-store"]["nested"].append("result edit")
    migrated.data.conversation_history[0].messages[0].unknown_fields["futureMessage"]["nested"].append("result edit")
    source["futureRoot"]["nested"].append("source edit")
    source["data"]["session"]["state"]["external-store"]["nested"].append("source edit")
    assert result.to_dict() == serialized
    assert before["data"]["session"]["state"]["external-store"]["nested"] == [2]
    assert migrated.unknown_fields["futureRoot"] == before["futureRoot"]


@pytest.mark.parametrize(
    "session",
    [
        None,
        {},
        {"state": {"keep": [1]}},
        {"session_id": "", "state": {"keep": [1]}},
        {"session_id": " ", "state": {"keep": [1]}},
    ],
)
def test_missing_logical_session_identity_is_filled_without_random_or_provider_state_reset(session: Any) -> None:
    source = _source()
    source["data"]["session"] = session
    result = _cold(_migrate(source))
    expected = deepcopy(session) if session is not None else {}
    expected["session_id"] = SESSION_ID
    expected.setdefault("state", {})
    assert result.data.session == expected


@pytest.mark.parametrize("session", [{"session_id": "destination-id"}, {"session_id": True}, []])
def test_conflicting_or_malformed_session_identity_fails(session: Any) -> None:
    source = _source()
    source["data"]["session"] = session
    with pytest.raises(ValueError, match="session"):
        _migrate(source)


def test_metadata_exact_contract_fixed_now_repeatability_and_parent_owned_idempotency() -> None:
    source = _source()
    before = deepcopy(source)
    first, second = _migrate(source), _migrate(source)
    assert first.to_dict() == second.to_dict()
    assert first is not second
    assert first.data.unknown_fields["migration"] == {
        "id": "migration-1",
        "sourceDigest": state_snapshot_digest(source),
        "sourceSessionId": SESSION_ID,
        "ownershipTransferId": "transfer-1",
        "createdAt": NOW.isoformat(),
    }
    assert first.data.session == {"session_id": SESSION_ID, "state": {}}
    later = _migrate(source, now=NOW + timedelta(days=1))
    assert later.data.response_mailbox["done"]["expiresAt"] != first.data.response_mailbox["done"]["expiresAt"]
    with pytest.raises(ValueError, match="never a v2 source"):
        _migrate(first.to_dict())
    assert source == before


def test_one_utc_clock_capture_for_all_backfills(monkeypatch: pytest.MonkeyPatch) -> None:
    from agent_framework_durabletask import _state_migration as migration_module

    calls: list[Any] = []

    class Clock(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> Self:
            calls.append(tz)
            return cls(2026, 9, 9, 12, tzinfo=timezone.utc)

    source = _source()
    source["data"]["conversationHistory"].append(_entry("response", "another"))
    monkeypatch.setattr(migration_module, "datetime", Clock)
    result = _migrate(source, now=None)
    assert calls == [timezone.utc]
    assert {record["createdAt"] for record in result.data.response_mailbox.values()} == {NOW.isoformat()}
    assert {record["completedAt"] for record in result.data.completed_correlations.values()} == {NOW.isoformat()}


def test_rfc3339_z_existing_delivery_reloads_without_python311_fromisoformat(monkeypatch: pytest.MonkeyPatch) -> None:
    from agent_framework_durabletask import _durable_agent_state as state_module

    class Python310Datetime(datetime):
        @classmethod
        def fromisoformat(cls, value: str) -> Self:
            assert not value.endswith(("Z", "z"))
            return super().fromisoformat(value)

    source = _source()
    delivery = _existing_delivery()
    delivery["responseMailbox"]["done"].update(createdAt="2024-01-01T00:00:00Z", expiresAt="2024-01-01T00:01:00z")
    delivery["completedCorrelations"]["done"]["completedAt"] = "2024-01-01T00:00:00Z"
    source["data"].update(
        responseMailbox=delivery["responseMailbox"], completedCorrelations=delivery["completedCorrelations"]
    )
    monkeypatch.setattr(state_module, "datetime", Python310Datetime)
    result = _cold(_migrate(source))
    assert result.data.response_mailbox == delivery["responseMailbox"]
    assert result.data.completed_correlations == delivery["completedCorrelations"]


@pytest.mark.parametrize("version", ["2.0.0", "2.3.0", "3.0.0", "1", None, True])
def test_migration_is_legacy_only(version: Any) -> None:
    source = _source()
    source["schemaVersion"] = version
    with pytest.raises(ValueError, match="only legacy"):
        _migrate(source)


@pytest.mark.parametrize("digest", ["a" * 64, "A" * 64, "short", None])
def test_source_digest_must_match_the_unmodified_snapshot(digest: Any) -> None:
    with pytest.raises(ValueError, match="source_digest"):
        _migrate(_source(), source_digest=digest)


@pytest.mark.parametrize("field", ["source_session_id", "migration_id", "ownership_transfer_id"])
@pytest.mark.parametrize("value", [None, "", " ", True, 1])
def test_parent_identifiers_must_be_nonblank_strings(field: str, value: Any) -> None:
    with pytest.raises(ValueError, match=field):
        _migrate(_source(), **{field: value})


@pytest.mark.parametrize("field", ["delivery_window_seconds", "max_state_bytes"])
@pytest.mark.parametrize("value", [True, False, 0, -1, 1.5, "60"])
def test_grace_and_resolved_budget_must_be_positive_nonbool_integers(field: str, value: Any) -> None:
    with pytest.raises(ValueError, match=field):
        _migrate(_source(), **{field: value})


@pytest.mark.parametrize("now", [datetime(2026, 9, 9), "2026-09-09T00:00:00Z", False])
def test_now_requires_an_aware_datetime(now: Any) -> None:
    with pytest.raises(ValueError, match="offset-aware"):
        _migrate(_source(), now=now)


def test_grace_overflow_is_rejected_even_without_a_recorded_response() -> None:
    source: dict[str, Any] = {"schemaVersion": "1.0.0", "data": {"conversationHistory": []}}
    with pytest.raises(ValueError, match="bounded grace"):
        _migrate(source, delivery_window_seconds=10**100)


def test_reserved_migration_metadata_is_not_silently_overwritten() -> None:
    source = _source()
    source["data"]["migration"] = {"unknown-owner": [1]}
    before = deepcopy(source)
    with pytest.raises(ValueError, match="reserved migration metadata"):
        _migrate(source)
    assert source == before


def test_budget_includes_metadata_receipts_mailbox_session_and_ascii_escaped_unknowns_without_pruning() -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"upstream": 3}
    source["futureRoot"] = {"unicode": "雪😀" * 20}
    custom = Message("user", ["complete original accepted input"], message_id="custom-id")
    messages = [_message(1), _message(3), custom]
    evidence = _evidence(source, messages)
    before_source, before_evidence = deepcopy(source), deepcopy(evidence)
    result = _migrate(source, delivery_evidence=evidence)
    assert result.data.ingested_messages == {message.message_id: [message_identity(message)] for message in messages}
    size = len(json.dumps(result.to_dict(), allow_nan=False))
    assert size > len(json.dumps(result.to_dict(), ensure_ascii=False, allow_nan=False).encode("utf-8"))
    assert _migrate(source, delivery_evidence=evidence, max_state_bytes=size).to_dict() == result.to_dict()
    without_metadata = result.to_dict()
    del without_metadata["data"]["migration"]
    for budget in (size - 1, len(json.dumps(without_metadata, allow_nan=False))):
        with pytest.raises(StateCapacityError) as error:
            _migrate(source, delivery_evidence=evidence, max_state_bytes=budget)
        assert error.value.size_bytes == error.value.floor_bytes == size
        assert error.value.max_state_bytes == error.value.target_bytes == budget
    assert source == before_source and evidence == before_evidence
    assert result.to_dict()["data"]["conversationHistory"] == source["data"]["conversationHistory"]
    assert "truncation" not in result.to_dict()["data"]
