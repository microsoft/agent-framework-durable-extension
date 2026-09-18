# Copyright (c) Microsoft. All rights reserved.

"""Published legacy inputs migrate only when the shared target can retain known completions."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from agent_framework import Content, Message
from jsonschema import Draft202012Validator, FormatChecker

from agent_framework_durabletask import migrate_legacy_state, state_snapshot_digest
from agent_framework_durabletask._entities import AgentEntity
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._shared_agent_state import DurableAgentState, DurableAgentStateMessage
from agent_framework_durabletask._shared_state_validation import validate_shared_state
from agent_framework_durabletask._state_capacity import StateCapacityError
from agent_framework_durabletask._workflows.naming import workflow_message_id

SCHEMA = json.loads(
    (Path(__file__).resolve().parents[4] / "schemas" / "durable-agent-entity-state.json").read_text(encoding="utf-8")
)
LEGACY_VERSIONS = tuple(
    version for version in SCHEMA["properties"]["schemaVersion"]["enum"] if version.startswith("1.")
)
NOW = datetime(2026, 9, 16, 12, tzinfo=timezone.utc)
OLD = "2024-01-02T03:04:05.123456789+05:30"
ORIGINAL_COMPLETED_AT = "2024-01-03T04:05:06.987654321+05:30"
SESSION = "dafx-agent:original-session"
WINDOW = 60


def _source(*entries: dict[str, Any], version: str = "1.1.0") -> dict[str, Any]:
    return {"schemaVersion": version, "data": {"conversationHistory": deepcopy(list(entries))}}


def _entry(kind: str, *, correlation: str = "done", contents: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "$type": kind,
        "correlationId": correlation,
        "createdAt": OLD,
        "messages": [] if contents is None else [{"role": "assistant", "contents": contents}],
    }


def _original_result(correlation: str = "done", *, outcome: str = "failed") -> dict[str, Any]:
    # Synthetic operator ground truth, independent of retained transcript content and its clock.
    return {
        "correlationId": correlation,
        "outcome": outcome,
        "completedAt": ORIGINAL_COMPLETED_AT,
        "response": {
            "createdAt": OLD,
            "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "original result"}]}],
        },
        **(
            {"error": {"code": "provider_error", "message": "Original invocation failed."}}
            if outcome == "failed"
            else {}
        ),
    }


def _completions(source: dict[str, Any], *results: dict[str, Any]) -> dict[str, Any]:
    return {
        "sourceDigest": state_snapshot_digest(source),
        "evidenceId": "completion-journal-1",
        "complete": True,
        "results": deepcopy(list(results)),
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


def _validate(source: dict[str, Any]) -> None:
    validate_shared_state(source)
    Draft202012Validator(SCHEMA, format_checker=FormatChecker()).validate(source)


def _cold(state: DurableAgentState) -> DurableAgentState:
    payload = state.to_dict()
    _validate(payload)
    restored = DurableAgentState.from_json(json.dumps(payload, allow_nan=False))
    assert restored.to_dict() == payload
    return restored


def _message(position: int, *, text: str = "accepted", producer: str = "upstream") -> Message:
    return Message(
        "user",
        [Content.from_text(text, additional_properties={"nested": [False, None, 3]})],
        message_id=workflow_message_id(producer, position),
        author_name="author",
        additional_properties={"source": {"labels": ["original"]}},
    )


def _journal(
    source: dict[str, Any],
    messages: list[Message],
    *,
    message_positions: list[dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    return {
        "sourceDigest": state_snapshot_digest(source),
        "evidenceId": "accepted-journal-1",
        "complete": True,
        "messages": [message.to_dict() for message in messages],
        **({"messagePositions": deepcopy(message_positions)} if message_positions is not None else {}),
    }


@pytest.mark.parametrize("version", LEGACY_VERSIONS)
@pytest.mark.parametrize("omit_history", [False, True])
def test_empty_published_legacy_state_migrates_without_inventing_completions(version: str, omit_history: bool) -> None:
    source = _source(version=version)
    if omit_history:
        del source["data"]["conversationHistory"]
    before = deepcopy(source)
    _validate(source)
    result = _cold(_migrate(source))
    assert result.schema_version == "2.0.0"
    assert result.data.response_mailbox == result.data.completed_correlations == {}
    assert result.to_dict()["data"]["conversationHistory"] == []
    assert result.data.session == {"session_id": SESSION, "state": {}}
    assert result.try_get_agent_response("not-completed") is None
    assert source == before


@pytest.mark.parametrize("version", LEGACY_VERSIONS)
@pytest.mark.parametrize("kind", ["response", "errorResponse"])
def test_simple_legacy_reads_remain_detached_without_backfilling(version: str, kind: str) -> None:
    source = _source(_entry(kind, contents=[{"$type": "text", "text": "retained text"}]), version=version)
    before = deepcopy(source)
    _validate(source)
    state = DurableAgentState.from_dict(source)
    response = state.try_get_agent_response("done")
    assert response is not None and response.text == "retained text"
    assert (response.additional_properties.get("durable_status") == "error") is (kind == "errorResponse")
    assert state.schema_version == version
    assert state.data.response_mailbox == state.data.completed_correlations == {}
    assert state.to_dict() == before
    assert source == before


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize(
    "entry",
    [
        pytest.param(_entry("errorResponse"), id="typed-empty-failure"),
        pytest.param(_entry("errorResponse", contents=[{"$type": "text", "text": ""}]), id="typed-pruned-failure"),
        pytest.param(
            _entry("errorResponse", contents=[{"$type": "error", "errorCode": "response_expired"}]),
            id="typed-failure-with-delivery-status",
        ),
        pytest.param(
            _entry("response", contents=[{"$type": "error", "message": "provider failed", "errorCode": "provider"}]),
            id="affirmative-invocation-error",
        ),
    ],
)
def test_known_failures_use_shared_records_and_controlled_grace(entry: dict[str, Any], strict: bool) -> None:
    source = _source(entry)
    before = deepcopy(source)
    _validate(source)
    original = _original_result()
    evidence = _completions(source, original)
    evidence_before = deepcopy(evidence)
    migrated = _migrate(source, completion_evidence=evidence, require_known_outcomes=strict)
    result = _cold(migrated)
    common = {
        "correlationId": "done",
        "outcome": "failed",
        "completedAt": ORIGINAL_COMPLETED_AT,
        "resultExpiresAt": (NOW + timedelta(seconds=WINDOW)).isoformat(),
    }
    assert result.data.completed_correlations == {"done": {**common, "resultState": "available"}}
    terminal = result.data.response_mailbox["done"]
    assert {key: terminal[key] for key in common} == common
    assert terminal == {**original, "resultExpiresAt": common["resultExpiresAt"]}
    assert terminal["response"]["createdAt"] == OLD
    assert terminal["error"]["code"] and terminal["error"]["message"]
    assert result.to_dict()["data"]["conversationHistory"] == before["data"]["conversationHistory"]
    assert result.to_dict()["data"]["terminalResults"] == result.data.response_mailbox
    assert result.to_dict()["data"]["completionReceipts"] == result.data.completed_correlations
    assert _migrate(source, completion_evidence=evidence, require_known_outcomes=strict).to_dict() == migrated.to_dict()
    result.expire_responses(now=NOW + timedelta(seconds=WINDOW))
    result = _cold(result)
    assert result.data.response_mailbox == {}
    assert result.data.completed_correlations["done"] == {
        **common,
        "resultState": "unavailable",
        "resultUnavailableAt": (NOW + timedelta(seconds=WINDOW)).isoformat(),
    }
    completed = result.try_get_agent_response("done")
    assert completed is not None
    assert completed.additional_properties["durable_status"] == "already_completed"
    assert completed.additional_properties["durable_outcome"] == "failed"
    assert source == before and evidence == evidence_before


def test_migration_imports_originals_without_recording_new_completions(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Migration must import original terminal results, not record new completions.")

    monkeypatch.setattr(DurableAgentState, "record_response", forbidden)
    source = _source(_entry("errorResponse"), _entry("errorResponse", correlation="other"))
    originals = [_original_result(), _original_result("other")]
    result = _cold(_migrate(source, completion_evidence=_completions(source, *originals)))
    assert result.schema_version == "2.0.0"
    assert result.data.response_mailbox == {
        original["correlationId"]: {**original, "resultExpiresAt": (NOW + timedelta(seconds=WINDOW)).isoformat()}
        for original in originals
    }


@pytest.mark.parametrize("strict", [None, False, True])
@pytest.mark.parametrize(
    "entry",
    [
        pytest.param(_entry("response"), id="contentless"),
        pytest.param(_entry("response", contents=[{"$type": "text", "text": "success"}]), id="success-text"),
        pytest.param(_entry("response", contents=[{"$type": "text", "text": "failed"}]), id="failure-text"),
        pytest.param(_entry("response", contents=[{"$type": "text", "text": ""}]), id="partial-text"),
        pytest.param(
            _entry("response", contents=[{"$type": "error", "errorCode": "response_expired"}]),
            id="delivery-expired",
        ),
        pytest.param(
            {
                **_entry("response"),
                "messages": [{"role": "tool", "contents": [{"$type": "error", "message": "tool failed"}]}],
            },
            id="tool-error",
        ),
    ],
)
def test_ambiguous_outcomes_reject_even_with_complete_input_journal(entry: dict[str, Any], strict: bool | None) -> None:
    source = _source(entry)
    source["data"]["ingestedPositions"] = {"upstream": 3}
    evidence = _journal(
        source,
        [_message(1), _message(3)],
        message_positions=[{"producer": "upstream", "position": 1}, {"producer": "upstream", "position": 3}],
    )
    before, evidence_before = deepcopy(source), deepcopy(evidence)
    options = {} if strict is None else {"require_known_outcomes": strict}
    with pytest.raises(ValueError, match="authoritative completion evidence.*new session generation"):
        _migrate(source, delivery_evidence=evidence, **options)
    assert source == before and evidence == evidence_before


@pytest.mark.parametrize("unknown_first", [False, True])
def test_a_duplicate_correlation_cannot_hide_an_ambiguous_response(unknown_first: bool) -> None:
    entries = [_entry("errorResponse"), _entry("response")]
    source = _source(*(list(reversed(entries)) if unknown_first else entries))
    before = deepcopy(source)
    with pytest.raises(ValueError, match="authoritative completion evidence"):
        _migrate(source)
    assert source == before


def test_missing_response_identity_is_not_silently_discarded() -> None:
    entry = _entry("errorResponse")
    del entry["correlationId"]
    source = _source(entry)
    _validate(source)
    before = deepcopy(source)
    with pytest.raises(ValueError, match="correlationId"):
        _migrate(source, completion_evidence=_completions(source, _original_result()))
    assert source == before


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("loss", ["truncation", "compaction"])
def test_input_journal_cannot_replace_potentially_lost_completions(loss: str, strict: bool) -> None:
    source = _source()
    if loss == "truncation":
        source["data"]["truncation"] = {"evictedMessageCount": 1, "firstEvictedAt": OLD, "lastEvictedAt": OLD}
    else:
        source["data"]["conversationHistory"] = [{"$type": "compaction", "createdAt": OLD, "messages": []}]
    source["data"]["ingestedPositions"] = {"upstream": 3}
    evidence = _journal(source, [_message(3)], message_positions=[{"producer": "upstream", "position": 3}])
    before = deepcopy(source)
    _validate(source)
    with pytest.raises(ValueError, match="authoritative completion evidence"):
        _migrate(source, delivery_evidence=evidence, require_known_outcomes=strict)
    assert source == before


@pytest.mark.parametrize("version", ["1.0.1", "1.3.0", "1.99.99", "2.0.0", "2.1.0", "3.0.0", None, True])
def test_no_unpublished_legacy_or_version_two_source_is_migrated(version: Any) -> None:
    source = _source()
    source["schemaVersion"] = version
    before = deepcopy(source)
    with pytest.raises(ValueError, match="only legacy shared.*never a v2 source"):
        _migrate(source)
    assert source == before


@pytest.mark.parametrize("version", LEGACY_VERSIONS)
@pytest.mark.parametrize("field", ["terminalResults", "completionReceipts", "historyBinding"])
def test_shared_legacy_forbids_target_only_maps_and_bindings(version: str, field: str) -> None:
    source = _source(version=version)
    source["data"][field] = {}
    before = deepcopy(source)
    with pytest.raises(ValueError, match="Legacy state must not contain"):
        _migrate(source)
    assert source == before


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("kind", [None, "futureEntry", "", True])
def test_completion_authority_cannot_admit_unknown_or_missing_entry_discriminators(kind: Any, strict: bool) -> None:
    entry = _entry("request")
    if kind is None:
        del entry["$type"]
    else:
        entry["$type"] = kind
    source = _source(entry)
    before = deepcopy(source)
    # Empty authoritative results isolate structural validation from missing authority.
    with pytest.raises(ValueError, match="entry|unsupported|discriminator"):
        _migrate(source, completion_evidence=_completions(source), require_known_outcomes=strict)
    assert source == before


@pytest.mark.parametrize("data", [{"conversationHistory": None}, {"session": None}, {"expirationTimeUtc": "bad"}])
def test_malformed_shared_source_is_not_repaired_during_conversion(data: dict[str, Any]) -> None:
    source = {"schemaVersion": "1.1.0", "data": deepcopy(data)}
    before = deepcopy(source)
    with pytest.raises(ValueError):
        _migrate(source)
    assert source == before


@pytest.mark.parametrize("expiration", [None, "2027-02-03T04:05:06.123456789Z"])
def test_migration_preserves_raw_fields_session_ttl_and_detachment(expiration: Any) -> None:
    request = {
        "$type": "request",
        "correlationId": "pending",
        "createdAt": OLD,
        "messages": [
            {
                "role": "user",
                "messageId": "custom",
                "createdAt": OLD,
                "extensionData": {},
                "futureMessage": [None, False],
                "contents": [{"$type": "text", "text": "", "extensionData": None, "futureContent": [2**65]}],
            }
        ],
        "extensionData": {},
        "futureEntry": {"opaque": [None]},
    }
    response = _entry("errorResponse")
    response["usage"] = {"inputTokenCount": 0, "extensionData": {}, "futureUsage": {"x": [False, 0]}}
    source = _source(request, response)
    source.update({"extensionData": {}, "futureRoot": {"unicode": "雪😀", "nested": [None]}})
    source["data"].update({
        "session": {
            "session_id": SESSION,
            "service_session_id": "provider-thread",
            "state": {"external-store": {"key": "original", "nested": [1]}},
            "futureSession": [False, None],
        },
        "expirationTimeUtc": expiration,
        "ingestedPositions": {},
        "extensionData": {},
        "futureData": {"nested": [False, 0, None]},
    })
    before = deepcopy(source)
    _validate(source)
    staged = _migrate(source, completion_evidence=_completions(source, _original_result()))
    result = _cold(staged)
    payload = result.to_dict()
    for key, value in before["data"].items():
        assert payload["data"][key] == value
    assert payload["futureRoot"] == before["futureRoot"]
    assert payload["extensionData"] == {}
    assert result.data.ingested_messages == {"custom": None}
    assert result.try_get_agent_response("pending") is None
    assert source == before
    assert staged.data.session is not None
    staged.data.session["state"]["external-store"]["nested"].append("staged edit")
    source["futureRoot"]["nested"].append("source edit")
    assert result.to_dict() == payload
    assert staged.unknown_fields["futureRoot"] == before["futureRoot"]


def test_full_sparse_input_journal_preserves_evicted_revisions_and_delta_behavior() -> None:
    first, third, revised = _message(1), _message(3), _message(3, text="accepted revision")
    assert first.message_id is not None and third.message_id is not None
    source = _source({
        "$type": "request",
        "correlationId": "pending",
        "messages": [{"role": "user", "messageId": third.message_id, "contents": []}],
    })
    source["data"]["ingestedPositions"] = {"upstream": 3}
    source["data"]["ingestedMessages"] = {third.message_id: [message_identity(third)]}
    evidence = _journal(
        source,
        [revised, first, third],
        message_positions=[
            {"producer": "upstream", "position": 3},
            {"producer": "upstream", "position": 1},
            {"producer": "upstream", "position": 3},
        ],
    )
    before, evidence_before = deepcopy(source), deepcopy(evidence)
    result = _cold(_migrate(source, delivery_evidence=evidence, completion_evidence=_completions(source)))
    assert result.data.ingested_messages == {
        first.message_id: [message_identity(first)],
        third.message_id: [message_identity(revised), message_identity(third)],
    }
    assert result.to_dict()["data"]["ingestedMessages"] == before["data"]["ingestedMessages"]
    assert result.data.completed_correlations == result.data.response_mailbox == {}
    assert result.data.ingested_positions == {"upstream": 3}
    assert result.data.unknown_fields["migration"]["evidenceId"] == "accepted-journal-1"
    gap, changed = _message(2), _message(3, text="new revision")
    messages = [
        DurableAgentStateMessage.from_chat_message(message) for message in (first, third, revised, gap, changed)
    ]
    # Drive the receiver's real filtering without constructing a worker or invoking an agent.
    receiver: Any = SimpleNamespace(state=result)
    kept = AgentEntity._drop_already_stored(receiver, messages)
    assert kept == messages[-2:]
    assert AgentEntity._drop_already_stored(receiver, messages) == []
    assert workflow_message_id("upstream", 0) not in result.data.ingested_messages
    assert gap.message_id is not None and changed.message_id is not None
    assert result.data.ingested_messages[gap.message_id] == [message_identity(gap)]
    assert result.data.ingested_messages[changed.message_id] == [
        message_identity(revised),
        message_identity(third),
        message_identity(changed),
    ]
    assert source == before and evidence == evidence_before


def test_custom_journal_establishes_revisions_without_consuming_opaque_legacy_markers() -> None:
    source = _source({"$type": "request", "messages": [{"role": "user", "messageId": "custom", "contents": []}]})
    source["data"]["ingestedMessages"] = {"custom": None}
    first = Message("user", ["original accepted input"], message_id="custom")
    revised = Message("user", ["accepted revision"], message_id="custom")
    result = _cold(
        _migrate(source, delivery_evidence=_journal(source, [first, revised]), completion_evidence=_completions(source))
    )
    assert result.data.ingested_messages == {"custom": [message_identity(first), message_identity(revised)]}
    assert result.to_dict()["data"]["ingestedMessages"] == {"custom": None}


@pytest.mark.parametrize("fault", ["missing", "digest", "incomplete", "maxima", "duplicate", "lossy", "custom-id"])
def test_input_journal_still_requires_complete_lossless_bound_evidence(fault: str) -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"upstream": 3}
    if fault == "custom-id":
        source["data"]["conversationHistory"] = [
            {
                "$type": "request",
                "messages": [{"role": "user", "messageId": "other", "contents": []}],
            }
        ]
    evidence = _journal(source, [_message(3)], message_positions=[{"producer": "upstream", "position": 3}])
    if fault == "digest":
        evidence["sourceDigest"] = "a" * 64
    elif fault == "incomplete":
        evidence["complete"] = False
    elif fault == "maxima":
        evidence["messages"] = [_message(2).to_dict()]
        evidence["messagePositions"] = [{"producer": "upstream", "position": 2}]
    elif fault == "duplicate":
        evidence["messages"] *= 2
        evidence["messagePositions"] *= 2
    elif fault == "lossy":
        evidence["messages"][0]["future_unrecognized_field"] = {"must": "survive"}
    before, evidence_before = deepcopy(source), deepcopy(evidence)
    with pytest.raises(ValueError):
        _migrate(
            source,
            delivery_evidence=None if fault == "missing" else evidence,
            completion_evidence=_completions(source),
        )
    assert source == before and evidence == evidence_before


@pytest.mark.parametrize("option", ["source_digest", "migration_id", "ownership_transfer_id", "source_session_id"])
def test_migration_identity_and_digest_validation_remain_required(option: str) -> None:
    with pytest.raises(ValueError, match=option):
        _migrate(_source(), **{option: ""})


def test_source_binding_metadata_and_budget_include_every_preserved_byte() -> None:
    source = _source(_entry("errorResponse"))
    source["future"] = "雪😀" * 20
    before = deepcopy(source)
    evidence = _completions(source, _original_result())
    result = _migrate(source, completion_evidence=evidence)
    assert result.data.unknown_fields["migration"] == {
        "id": "migration-1",
        "sourceDigest": state_snapshot_digest(before),
        "sourceSessionId": SESSION,
        "ownershipTransferId": "transfer-1",
        "createdAt": NOW.isoformat(),
        "completionEvidenceId": "completion-journal-1",
    }
    size = len(json.dumps(result.to_dict(), allow_nan=False))
    assert _migrate(source, completion_evidence=evidence, max_state_bytes=size).to_dict() == result.to_dict()
    with pytest.raises(StateCapacityError) as error:
        _migrate(source, completion_evidence=evidence, max_state_bytes=size - 1)
    assert error.value.size_bytes == error.value.floor_bytes == size
    assert error.value.max_state_bytes == error.value.target_bytes == size - 1
    with pytest.raises(ValueError, match="source_digest does not match"):
        _migrate(source, source_digest="a" * 64)
    assert source == before


@pytest.mark.parametrize("value", [None, 0, 1, "false"])
def test_compatibility_flag_still_requires_a_boolean(value: Any) -> None:
    with pytest.raises(ValueError, match="require_known_outcomes must be a boolean"):
        _migrate(_source(), require_known_outcomes=value)


def test_absent_historical_timestamp_stays_absent_while_grace_is_recorded() -> None:
    entry = _entry("errorResponse")
    del entry["createdAt"]
    source = _source(entry)
    before = deepcopy(source)
    original = _original_result()
    del original["response"]["createdAt"]
    result = _cold(_migrate(source, completion_evidence=_completions(source, original)))
    assert result.to_dict()["data"]["conversationHistory"] == [entry]
    assert "createdAt" not in result.data.response_mailbox["done"]["response"]
    assert result.data.completed_correlations["done"]["completedAt"] == ORIGINAL_COMPLETED_AT
    assert source == before
