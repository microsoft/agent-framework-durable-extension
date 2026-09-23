# Copyright (c) Microsoft. All rights reserved.

"""Detached explicit migration contracts. No hosts, providers or backends are needed."""

import hashlib
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest
from agent_framework import AgentResponse, AgentSession, Content, Message
from typing_extensions import Self

from agent_framework_durabletask import migrate_legacy_state, state_snapshot_digest
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._shared_agent_state import DurableAgentState, DurableAgentStateEntryJsonType
from agent_framework_durabletask._shared_response import load_terminal_response
from agent_framework_durabletask._state_capacity import StateCapacityError
from agent_framework_durabletask._workflows.naming import workflow_message_id

NOW = datetime(2026, 9, 9, 12, tzinfo=timezone.utc)
OLD = datetime(2024, 1, 1, tzinfo=timezone.utc)
ORIGINAL_COMPLETED_AT = "2024-01-02T03:04:05.123456789Z"
SESSION_ID = "dafx-agent:original-session"
WINDOW = 60


def _entry(kind: str, correlation: str, *, message_id: str = "custom-id") -> dict[str, Any]:
    return {
        "$type": kind,
        **({"correlationId": correlation} if kind != "compaction" else {}),
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
                _entry("errorResponse", "done", message_id="answer-id"),
            ]
        },
    }


def _original_result(correlation: str = "done", *, outcome: str = "failed") -> dict[str, Any]:
    # Explicit synthetic ground truth. Never reconstruct completion time from retained entries.
    return {
        "correlationId": correlation,
        "outcome": outcome,
        "completedAt": ORIGINAL_COMPLETED_AT,
        "response": {
            "createdAt": OLD.isoformat(),
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


def _evidence(
    source: dict[str, Any],
    messages: list[Message],
    *,
    message_positions: list[dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    return {
        "sourceDigest": state_snapshot_digest(source),
        "evidenceId": "operator-journal-1",
        "complete": True,
        "messages": [message.to_dict() for message in messages],
        **({"messagePositions": deepcopy(message_positions)} if message_positions is not None else {}),
    }


def _cold(state: DurableAgentState) -> DurableAgentState:
    raw = state.to_dict()
    if state.data.ingested_messages:
        assert raw["data"]["pythonIngestion"] == {
            "profile": "agent-framework-python.ingestion",
            "version": 1,
            "messages": state.data.ingested_messages,
        }
    restored = DurableAgentState.from_json(json.dumps(raw, allow_nan=False))
    assert restored.to_dict() == raw
    return restored


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
@pytest.mark.parametrize("strict", [False, True])
def test_every_used_history_kind_needs_an_explicit_completion_journal(kind: str, strict: bool) -> None:
    source = _source()
    source["data"]["conversationHistory"] = [_entry(kind, "done")]
    before = deepcopy(source)
    with pytest.raises(ValueError, match="authoritative completion evidence"):
        _migrate(source, require_known_outcomes=strict)
    assert source == before
    originals: list[dict[str, Any]] = []
    if kind in (DurableAgentStateEntryJsonType.RESPONSE, DurableAgentStateEntryJsonType.ERROR_RESPONSE):
        outcome = "failed" if kind == DurableAgentStateEntryJsonType.ERROR_RESPONSE else "succeeded"
        originals.append(_original_result(outcome=outcome))
    result = _cold(
        _migrate(source, require_known_outcomes=strict, completion_evidence=_completions(source, *originals))
    )
    assert set(result.data.completed_correlations) == {item["correlationId"] for item in originals}
    assert set(result.data.response_mailbox) == {item["correlationId"] for item in originals}
    assert result.to_dict()["data"]["conversationHistory"] == source["data"]["conversationHistory"]
    if originals:
        receipt = result.data.completed_correlations["done"]
        mailbox = result.data.response_mailbox["done"]
        assert receipt["correlationId"] == mailbox["correlationId"] == "done"
        assert receipt["outcome"] == mailbox["outcome"] == originals[0]["outcome"]
        assert receipt["resultState"] == "available"
        assert receipt["completedAt"] == mailbox["completedAt"] == ORIGINAL_COMPLETED_AT
        assert receipt["resultExpiresAt"] == mailbox["resultExpiresAt"] == (NOW + timedelta(seconds=WINDOW)).isoformat()
        assert mailbox == {**originals[0], "resultExpiresAt": receipt["resultExpiresAt"]}
    assert source == before


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("contents", [[], [{"$type": "text", "text": "apparently successful partial answer"}]])
def test_partial_and_contentless_recorded_responses_cannot_establish_success(
    strict: bool, contents: list[dict[str, Any]]
) -> None:
    source = _source()
    history = source["data"]["conversationHistory"]
    history[1]["$type"] = "response"
    history[1]["messages"][0]["contents"] = contents
    before = deepcopy(source)
    with pytest.raises(ValueError, match="outcome.*evidence"):
        _migrate(source, require_known_outcomes=strict)
    original = _original_result(outcome="succeeded")
    result = _cold(_migrate(source, require_known_outcomes=strict, completion_evidence=_completions(source, original)))
    assert result.data.response_mailbox["done"] == {
        **original,
        "resultExpiresAt": (NOW + timedelta(seconds=WINDOW)).isoformat(),
    }
    assert result.data.completed_correlations["done"]["completedAt"] == ORIGINAL_COMPLETED_AT
    assert result.to_dict()["data"]["conversationHistory"] == history
    assert source == before


def test_failure_usage_contentless_failure_and_unfinished_requests_remain_distinct() -> None:
    source = _source()
    history = source["data"]["conversationHistory"]
    history[1]["usage"] = {"inputTokenCount": 3, "futureUsage": {"keep": [1]}}
    history.append(_entry("errorResponse", "empty"))
    history[-1]["messages"] = []
    history.append(_entry("request", "accepted-without-response"))
    history[-1]["messages"][0]["contents"] = []
    history.append(_entry("request", "unfinished"))
    before = deepcopy(source)
    original, empty = _original_result(), _original_result("empty")
    original["response"]["usage"] = {"inputTokenCount": 3, "futureUsage": {"keep": [1]}}
    empty["response"]["messages"] = []
    result = _cold(_migrate(source, completion_evidence=_completions(source, original, empty)))

    assert set(result.data.completed_correlations) == {"done", "empty"}
    assert set(result.data.response_mailbox) == {"done", "empty"}
    assert result.data.response_mailbox["empty"]["response"]["messages"] == []
    response = load_terminal_response(result.data.response_mailbox["done"]["response"])
    assert response.usage_details == {"input_token_count": 3}
    assert all(record["outcome"] == "failed" for record in result.data.completed_correlations.values())
    assert result.try_get_agent_response("accepted-without-response") is None
    assert result.try_get_agent_response("unfinished") is None
    assert result.to_dict()["data"]["conversationHistory"] == history
    assert source == before


@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("loss", ["truncation", "compaction"])
@pytest.mark.parametrize("with_delivery_journal", [False, True])
def test_history_loss_requires_completion_journal_independently_of_complete_input_journal(
    strict: bool, loss: str, with_delivery_journal: bool
) -> None:
    source = _source()
    if loss == "truncation":
        source["data"]["truncation"] = {
            "evictedMessageCount": 20,
            "firstEvictedAt": OLD.isoformat(),
            "lastEvictedAt": OLD.isoformat(),
            "future": [1],
        }
    else:
        source["data"]["conversationHistory"].append(_entry("compaction", "unused"))
    journal = _evidence(source, [Message("user", ["complete accepted input"], message_id="custom-id")])
    before_source, before_journal = deepcopy(source), deepcopy(journal)
    with pytest.raises(ValueError, match="outcome.*evidence"):
        _migrate(source, require_known_outcomes=strict, delivery_evidence=journal if with_delivery_journal else None)
    originals = [_original_result(), _original_result("pruned", outcome="succeeded")]
    completions = _completions(source, *originals)
    result = _cold(
        _migrate(
            source,
            require_known_outcomes=strict,
            delivery_evidence=journal if with_delivery_journal else None,
            completion_evidence=completions,
        )
    )
    assert result.data.response_mailbox == {
        original["correlationId"]: {**original, "resultExpiresAt": (NOW + timedelta(seconds=WINDOW)).isoformat()}
        for original in originals
    }
    assert result.to_dict()["data"]["conversationHistory"] == before_source["data"]["conversationHistory"]
    if loss == "truncation":
        assert result.to_dict()["data"]["truncation"] == before_source["data"]["truncation"]
    assert source == before_source and journal == before_journal


def test_sparse_journal_preserves_exact_revisions_not_an_inferred_prefix_after_cold_reload() -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"upstream": 3}
    # The contentless request cannot establish the original accepted input or gaps.
    # Only the complete journal contributes exact fingerprints.
    source["data"]["conversationHistory"][0]["messages"] = [
        {"role": "user", "messageId": workflow_message_id("upstream", 3), "contents": []}
    ]
    first, third, revision = _message(1), _message(3), _message(3, text="accepted revision")
    assert first.message_id is not None
    assert third.message_id is not None
    source["data"]["ingestedMessages"] = {"opaque": {"keep": [False, None]}}
    evidence = _evidence(
        source,
        [revision, first, third],
        message_positions=[
            {"producer": "upstream", "position": 3},
            {"producer": "upstream", "position": 1},
            {"producer": "upstream", "position": 3},
        ],
    )
    before_source, before_evidence = deepcopy(source), deepcopy(evidence)
    result = _cold(
        _migrate(source, delivery_evidence=evidence, completion_evidence=_completions(source, _original_result()))
    )

    assert result.data.ingested_messages == {
        first.message_id: [message_identity(first)],
        third.message_id: [message_identity(revision), message_identity(third)],
    }
    assert workflow_message_id("upstream", 0) not in result.data.ingested_messages
    assert workflow_message_id("upstream", 2) not in result.data.ingested_messages
    fingerprints = result.data.ingested_messages[third.message_id]
    assert fingerprints is not None
    assert message_identity(_message(3, text="new revision")) not in fingerprints
    assert result.data.ingested_positions == {"upstream": 3}
    assert result.to_dict()["data"]["ingestedMessages"] == before_source["data"]["ingestedMessages"]
    assert result.data.unknown_fields["migration"]["evidenceId"] == "operator-journal-1"
    assert source == before_source and evidence == before_evidence
    evidence["messages"][0]["contents"][0]["additional_properties"]["nested"]["labels"].append("caller edit")
    assert result.data.ingested_messages[third.message_id] == [message_identity(revision), message_identity(third)]


def test_multiple_producers_allow_sparse_zero_based_and_out_of_order_journal() -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"first_with_underscores": 9, "other": 0}
    # Supply the accepted custom input explicitly, not a fingerprint inferred from its retained portion.
    custom = Message("user", ["complete original accepted input"], message_id="custom-id")
    messages = [_message(9, producer="first_with_underscores"), _message(0, producer="other"), custom]
    result = _cold(
        _migrate(
            source,
            delivery_evidence=_evidence(
                source,
                messages,
                message_positions=[
                    {"producer": "first_with_underscores", "position": 9},
                    {"producer": "other", "position": 0},
                    None,
                ],
            ),
            completion_evidence=_completions(source, _original_result()),
        )
    )
    assert result.data.ingested_messages == {message.message_id: [message_identity(message)] for message in messages}


@pytest.mark.parametrize("position", [0, 3])
def test_scalar_positions_without_complete_journal_fail_even_with_retained_messages(position: int) -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"upstream": position}
    source["data"]["conversationHistory"][0]["messages"][0]["messageId"] = workflow_message_id("upstream", position)
    source["data"]["ingestedMessages"] = {workflow_message_id("upstream", position): ["a" * 64]}
    before = deepcopy(source)
    with pytest.raises(ValueError, match="recorded delivery evidence.*old engine"):
        _migrate(source, completion_evidence=_completions(source, _original_result()))
    assert source == before


def test_no_scalar_or_delivery_journal_uses_public_id_markers_without_interpreting_opaque_legacy_data() -> None:
    source = _source()
    messages = source["data"]["conversationHistory"][0]["messages"]
    messages.extend([
        {"role": "user", "messageId": "cleared-custom", "contents": []},
        {"role": "user", "messageId": workflow_message_id("upstream", 3), "contents": []},
        {"role": "user", "messageId": "already-exact", "contents": []},
    ])
    source["data"]["ingestedMessages"] = {"already-exact": ["b" * 64, "a" * 64], "old-marker": None}
    result = _cold(_migrate(source, completion_evidence=_completions(source, _original_result())))
    assert result.data.ingested_messages == {
        "already-exact": None,
        "custom-id": None,
        "cleared-custom": None,
        workflow_message_id("upstream", 3): None,
    }
    assert result.to_dict()["data"]["ingestedMessages"] == source["data"]["ingestedMessages"]
    assert "old-marker" not in result.data.ingested_messages
    assert "answer-id" not in result.data.ingested_messages


@pytest.mark.parametrize("identity", [None, "", " "])
def test_anonymous_legacy_request_ids_are_preserved_without_receipts(identity: Any) -> None:
    source = _source()
    message = source["data"]["conversationHistory"][0]["messages"][0]
    if identity is None:
        message.pop("messageId")
    else:
        message["messageId"] = identity
    result = _cold(_migrate(source, completion_evidence=_completions(source, _original_result())))
    assert result.data.ingested_messages == {}
    assert result.to_dict()["data"]["conversationHistory"][0] == source["data"]["conversationHistory"][0]


def test_complete_custom_journal_establishes_content_sensitive_revisions_without_interpreting_opaque_data() -> None:
    source = _source()
    source["data"]["ingestedMessages"] = {"custom-id": None}
    old = Message("user", ["original"], message_id="custom-id")
    revised = Message("user", ["revision"], message_id="custom-id")
    result = _cold(
        _migrate(
            source,
            delivery_evidence=_evidence(source, [old, revised]),
            completion_evidence=_completions(source, _original_result()),
        )
    )
    assert result.data.ingested_messages == {"custom-id": [message_identity(old), message_identity(revised)]}
    assert result.to_dict()["data"]["ingestedMessages"] == {"custom-id": None}


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
    source["data"]["conversationHistory"][0]["messages"][0]["messageId"] = workflow_message_id("upstream", 3)
    evidence = _evidence(source, [_message(3)], message_positions=[{"producer": "upstream", "position": 3}])
    evidence[field] = value
    before = deepcopy(evidence)
    with pytest.raises(ValueError, match="[Rr]ecorded delivery evidence"):
        _migrate(source, delivery_evidence=evidence, completion_evidence=_completions(source, _original_result()))
    assert evidence == before


@pytest.mark.parametrize("field", ["sourceDigest", "evidenceId", "complete", "messages"])
def test_all_evidence_fields_are_required(field: str) -> None:
    source = _source()
    evidence = _evidence(source, [])
    del evidence[field]
    with pytest.raises(ValueError, match="requires.*sourceDigest.*evidenceId.*complete.*messages"):
        _migrate(source, delivery_evidence=evidence, completion_evidence=_completions(source, _original_result()))


@pytest.mark.parametrize(
    ("messages", "message_positions"),
    [
        ([], []),
        ([_message(1)], [{"producer": "upstream", "position": 1}]),
        ([_message(4)], [{"producer": "upstream", "position": 4}]),
        ([_message(3, producer="other")], [{"producer": "other", "position": 3}]),
        (
            [_message(3), _message(0, producer="extra")],
            [{"producer": "upstream", "position": 3}, {"producer": "extra", "position": 0}],
        ),
    ],
)
def test_evidence_explicit_producer_set_and_maxima_must_match(
    messages: list[Message], message_positions: list[dict[str, Any] | None]
) -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"upstream": 3}
    custom = Message("user", ["complete original accepted input"], message_id="custom-id")
    with pytest.raises(ValueError, match="producers and maximum positions"):
        _migrate(
            source,
            delivery_evidence=_evidence(source, [*messages, custom], message_positions=[*message_positions, None]),
            completion_evidence=_completions(source, _original_result()),
        )


@pytest.mark.parametrize(
    "positions",
    [None, [], False, {"upstream": True}, {"upstream": -1}, {"upstream": 1.5}, {"upstream": "3"}, {"": 0}, {" ": 0}],
)
def test_all_legacy_cursor_entries_require_named_producers_and_nonbool_nonnegative_ints(positions: Any) -> None:
    source = _source()
    source["data"]["ingestedPositions"] = positions
    with pytest.raises(ValueError, match="ingested[Pp]osition|ingested position"):
        _migrate(
            source,
            delivery_evidence=_evidence(source, [], message_positions=[]),
            completion_evidence=_completions(source, _original_result()),
        )


@pytest.mark.parametrize("identity", [None, "", "  ", True, 7])
def test_evidence_rejects_missing_blank_and_nonstring_public_ids(identity: Any) -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"upstream": 3}
    source["data"]["conversationHistory"][0]["messages"][0]["messageId"] = workflow_message_id("upstream", 3)
    evidence = _evidence(source, [_message(3)], message_positions=[{"producer": "upstream", "position": 3}])
    evidence["messages"][0]["message_id"] = identity
    with pytest.raises(ValueError, match="message_id.*nonblank string"):
        _migrate(source, delivery_evidence=evidence, completion_evidence=_completions(source, _original_result()))


@pytest.mark.parametrize(
    "identity",
    ["wf_upstream_-1", "wf_upstream_true", "wf__3", "wf_upstream_3\n", "wf_other_99", "custom-id"],
)
def test_nonblank_public_ids_are_opaque_to_explicit_position_attribution(identity: str) -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"upstream": 3}
    source["data"]["conversationHistory"][0]["messages"] = [{"role": "user", "messageId": identity, "contents": []}]
    original = Message("user", ["complete original accepted input"], message_id=identity)
    evidence = _evidence(source, [original], message_positions=[{"producer": "upstream", "position": 3}])
    before_source, before_evidence = deepcopy(source), deepcopy(evidence)
    result = _cold(
        _migrate(source, delivery_evidence=evidence, completion_evidence=_completions(source, _original_result()))
    )
    assert result.data.ingested_positions == {"upstream": 3}
    assert result.data.ingested_messages == {identity: [message_identity(original)]}
    assert result.to_dict()["data"]["conversationHistory"] == before_source["data"]["conversationHistory"]
    assert source == before_source and evidence == before_evidence


def test_exact_duplicate_evidence_is_rejected_but_revisions_are_not() -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"upstream": 3}
    source["data"]["conversationHistory"][0]["messages"][0]["messageId"] = workflow_message_id("upstream", 3)
    message = _message(3)
    with pytest.raises(ValueError, match="duplicate message ID/fingerprint"):
        _migrate(
            source,
            delivery_evidence=_evidence(
                source,
                [message, message],
                message_positions=[{"producer": "upstream", "position": 3}, {"producer": "upstream", "position": 3}],
            ),
            completion_evidence=_completions(source, _original_result()),
        )


@pytest.mark.parametrize("change", ["unknown-field", "raw-representation", "wrong-contents", "bad-content", "bad-role"])
def test_journal_never_hashes_a_lossy_or_malformed_message_projection(change: str) -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"upstream": 3}
    source["data"]["conversationHistory"][0]["messages"][0]["messageId"] = workflow_message_id("upstream", 3)
    evidence = _evidence(source, [_message(3)], message_positions=[{"producer": "upstream", "position": 3}])
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
        _migrate(source, delivery_evidence=evidence, completion_evidence=_completions(source, _original_result()))


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
    result = _cold(
        _migrate(
            source,
            delivery_evidence=_evidence(
                source,
                [original, changed, custom],
                message_positions=[
                    {"producer": "upstream", "position": 3},
                    {"producer": "upstream", "position": 3},
                    None,
                ],
            ),
            completion_evidence=_completions(source, _original_result()),
        )
    )
    assert message_identity(original) != message_identity(changed)
    assert original.message_id is not None
    assert result.data.ingested_messages == {
        original.message_id: [message_identity(original), message_identity(changed)],
        "custom-id": [message_identity(custom)],
    }


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize(
    "opaque",
    [
        None,
        [],
        False,
        0,
        "arbitrary legacy value",
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
def test_unversioned_legacy_ingested_messages_are_arbitrary_opaque_json(version: str, opaque: Any) -> None:
    source = _source()
    source["schemaVersion"] = version
    source["data"]["ingestedMessages"] = deepcopy(opaque)
    before = deepcopy(source)
    assert DurableAgentState.from_dict(source).data.ingested_messages == {}
    result = _cold(_migrate(source, completion_evidence=_completions(source, _original_result())))
    assert result.to_dict()["data"]["ingestedMessages"] == opaque
    assert result.data.ingested_messages == {"custom-id": None}
    assert source == before


@pytest.mark.parametrize("identity", ["custom-id", "wf_retained_7", "wf__3"])
def test_complete_journal_must_include_every_retained_public_request_identity(identity: str) -> None:
    source = _source()
    source["data"]["ingestedPositions"] = {"upstream": 3}
    source["data"]["conversationHistory"][0]["messages"][0]["messageId"] = identity
    before = deepcopy(source)
    with pytest.raises(ValueError, match="every retained legacy request message ID"):
        _migrate(
            source,
            delivery_evidence=_evidence(
                source, [_message(3)], message_positions=[{"producer": "upstream", "position": 3}]
            ),
            completion_evidence=_completions(source, _original_result()),
        )
    assert source == before


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
    history.append({"$type": "request", "opaque": [None], "futureEntry": {"unknown": [9]}, "messages": []})
    history[0]["messages"][0]["contents"].append({
        "$type": "unknown",
        "content": {"$type": "futureContentKind", "payload": None},
    })
    before = deepcopy(source)
    migrated = _migrate(source, completion_evidence=_completions(source, _original_result()))
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
        {"session_id": None, "state": {"keep": [1]}},
        {"session_id": "", "state": {"keep": [1]}},
        {"session_id": " ", "state": {"keep": [1]}},
    ],
)
def test_missing_logical_session_identity_is_filled_without_random_or_provider_state_reset(session: Any) -> None:
    source = _source()
    if session is not None:
        source["data"]["session"] = session
    result = _cold(_migrate(source, completion_evidence=_completions(source, _original_result())))
    expected = deepcopy(session) if session is not None else {}
    expected["session_id"] = SESSION_ID
    expected.setdefault("state", {})
    assert result.data.session == expected


@pytest.mark.parametrize("session", [{"session_id": "destination-id"}, {"session_id": True}, [], None])
def test_conflicting_or_malformed_session_identity_fails(session: Any) -> None:
    source = _source()
    source["data"]["session"] = session
    with pytest.raises(ValueError, match="session"):
        _migrate(source, completion_evidence=_completions(source, _original_result()))


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("state", []),
        ("state", None),
        ("state", 7),
        ("state", False),
        ("state", "opaque"),
        ("service_session_id", []),
        ("service_session_id", ["provider-id"]),
        ("service_session_id", 7),
        ("service_session_id", 1.5),
        ("service_session_id", False),
        ("service_session_id", True),
    ],
)
def test_migration_rejects_invalid_session_fields_without_mutating_inputs(
    version: str, field: str, value: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _source()
    source["schemaVersion"] = version
    source["data"]["session"] = {field: deepcopy(value), "futureSession": {"keep": [False, None]}}
    evidence = _completions(source, _original_result())
    before_source, before_evidence = deepcopy(source), deepcopy(evidence)

    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Migration must validate session JSON without Core deserialization or provider registration.")

    monkeypatch.setattr(AgentSession, "from_dict", forbidden)
    monkeypatch.setattr("agent_framework_durabletask._entities._register_loaded_state_types", forbidden)
    with pytest.raises(ValueError, match=rf"Legacy session\.{field} must be"):
        _migrate(source, completion_evidence=evidence)
    assert source == before_source
    assert evidence == before_evidence


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize(
    "service_fields",
    [
        {},
        {"service_session_id": None},
        {"service_session_id": " provider-id "},
        {"service_session_id": ""},
        {"service_session_id": " \t\n"},
        {"service_session_id": {}},
        {"service_session_id": {"conversation": "provider-id"}},
        {"service_session_id": {"conversation_id": "c", "response_id": "r", "future_id": "opaque"}},
        {
            "service_session_id": {
                "conversation_id": "c",
                "response_id": "r",
                "metadata": {"type": "message", "values": [None, False, 0, 1.5, {}, [], "雪"]},
            }
        },
        {"service_session_id": {"conversation_id": None, "response_id": 7, "future_id": False}},
    ],
)
@pytest.mark.parametrize(
    "state_fields",
    [
        {},
        {"state": {}},
        {"state": {"typed": {"type": "message", "opaque": [None, False, 0]}, "nested": [1, {}, None]}},
    ],
)
def test_migration_preserves_session_json_without_core_deserialization(
    version: str, service_fields: dict[str, Any], state_fields: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _source()
    source["schemaVersion"] = version
    source["data"]["session"] = {
        "session_id": SESSION_ID,
        "futureSession": {"pythonIngestion": {"messages": ["opaque"]}},
        **deepcopy(service_fields),
        **deepcopy(state_fields),
    }
    evidence = _completions(source, _original_result())
    before_source, before_evidence = deepcopy(source), deepcopy(evidence)

    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Migration must not deserialize typed session values or register provider types.")

    monkeypatch.setattr(AgentSession, "from_dict", forbidden)
    monkeypatch.setattr("agent_framework_durabletask._entities._register_loaded_state_types", forbidden)
    staged = _migrate(source, completion_evidence=evidence)
    assert staged.data.session is not source["data"]["session"]
    result = _cold(staged)
    expected = deepcopy(source["data"]["session"])
    expected.setdefault("state", {})
    assert result.data.session == expected
    assert json.dumps(result.data.session, sort_keys=True) == json.dumps(expected, sort_keys=True)
    assert result.data.session is not None
    assert result.data.session["session_id"] == SESSION_ID
    assert staged.data.session is not None
    if isinstance(staged.data.session.get("service_session_id"), dict):
        staged.data.session["service_session_id"]["migration_result_edit"] = [1]
        assert result.data.session == expected
    assert source == before_source
    assert evidence == before_evidence


def test_metadata_exact_contract_fixed_now_repeatability_and_parent_owned_idempotency() -> None:
    source = _source()
    before = deepcopy(source)
    evidence = _completions(source, _original_result())
    first = _migrate(source, completion_evidence=evidence)
    second = _migrate(source, completion_evidence=evidence)
    assert first.to_dict() == second.to_dict()
    assert first is not second
    assert first.data.unknown_fields["migration"] == {
        "id": "migration-1",
        "sourceDigest": state_snapshot_digest(source),
        "sourceSessionId": SESSION_ID,
        "ownershipTransferId": "transfer-1",
        "createdAt": NOW.isoformat(),
        "completionEvidenceId": "completion-journal-1",
    }
    assert first.data.session == {"session_id": SESSION_ID, "state": {}}
    later = _migrate(source, now=NOW + timedelta(days=1), completion_evidence=evidence)
    assert later.data.unknown_fields["migration"]["createdAt"] != first.data.unknown_fields["migration"]["createdAt"]
    assert later.data.completed_correlations["done"]["completedAt"] == ORIGINAL_COMPLETED_AT
    with pytest.raises(ValueError, match="never a v2 source"):
        _migrate(first.to_dict())
    assert source == before


def test_one_utc_clock_capture_for_migration_metadata_and_bounded_delivery(monkeypatch: pytest.MonkeyPatch) -> None:
    from agent_framework_durabletask import _state_migration as migration_module

    calls: list[Any] = []

    class Clock(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> Self:
            calls.append(tz)
            return cls(2026, 9, 9, 12, tzinfo=timezone.utc)

    source = _source()
    source["data"]["conversationHistory"].append(_entry("errorResponse", "another"))
    monkeypatch.setattr(migration_module, "datetime", Clock)
    result = _migrate(
        source, now=None, completion_evidence=_completions(source, _original_result(), _original_result("another"))
    )
    assert calls == [timezone.utc]
    assert result.data.unknown_fields["migration"]["createdAt"] == NOW.isoformat()
    assert {record["resultExpiresAt"] for record in result.data.response_mailbox.values()} == {
        (NOW + timedelta(seconds=WINDOW)).isoformat()
    }
    for correlation, mailbox in result.data.response_mailbox.items():
        assert (
            mailbox["completedAt"]
            == result.data.completed_correlations[correlation]["completedAt"]
            == (ORIGINAL_COMPLETED_AT)
        )


def test_rfc3339_z_existing_delivery_uses_shared_portable_parser(monkeypatch: pytest.MonkeyPatch) -> None:
    from agent_framework_durabletask import _shared_state_validation as validation_module

    constructed: list[tuple[Any, ...]] = []

    class Python310Datetime(datetime):
        def __new__(cls, *args: Any, **kwargs: Any) -> Self:
            constructed.append(args)
            return super().__new__(cls, *args, **kwargs)

        @classmethod
        def fromisoformat(cls, value: str) -> Self:
            assert not value.endswith(("Z", "z"))
            return super().fromisoformat(value)

    delivery = _existing_delivery()
    for field in ("terminalResults", "completionReceipts"):
        delivery[field]["done"].update(completedAt="2024-01-01T00:00:00Z", resultExpiresAt="2024-01-01T00:01:00z")
    source = {"schemaVersion": "2.0.0", "data": delivery}
    before = deepcopy(source)
    monkeypatch.setattr(validation_module, "datetime", Python310Datetime)
    result = _cold(DurableAgentState.from_dict(source))
    assert result.to_dict() == before
    assert (2024, 1, 1, 0, 0, 0) in constructed
    constructed.clear()
    # The transition validator also requires an instance of its patched clock type.
    result.expire_responses(now=Python310Datetime(2026, 9, 9, 12, tzinfo=timezone.utc))
    assert (2024, 1, 1, 0, 1, 0) in constructed
    assert result.data.response_mailbox == {}
    assert result.data.completed_correlations["done"] == {
        **delivery["completionReceipts"]["done"],
        "resultState": "unavailable",
        "resultUnavailableAt": NOW.isoformat(),
    }
    assert source == before


@pytest.mark.parametrize(
    "version", ["1.0", "1.3.0", "1.2.1", "1.2.0-preview", "2.0.0", "2.3.0", "3.0.0", "1", None, True]
)
def test_migration_is_legacy_only(version: Any) -> None:
    source = _source()
    source["schemaVersion"] = version
    with pytest.raises(ValueError, match="only legacy"):
        _migrate(source)


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize("field", ["terminalResults", "completionReceipts", "historyBinding"])
@pytest.mark.parametrize("value", [{}, None])
def test_legacy_source_rejects_shared_v2_fields_even_when_empty(version: str, field: str, value: Any) -> None:
    source = _source()
    source["schemaVersion"] = version
    source["data"][field] = deepcopy(value)
    before = deepcopy(source)
    with pytest.raises(ValueError, match="Legacy state must not contain"):
        _migrate(source)
    assert source == before


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize(
    "marker",
    [
        None,
        {},
        {"profile": "foreign", "version": 1, "messages": {"accepted": ["a" * 64]}},
        {"profile": "agent-framework-python.ingestion", "version": 1, "messages": {"accepted": ["a" * 64]}},
    ],
)
@pytest.mark.parametrize("retained_request", [False, True])
def test_migration_rejects_reserved_python_ingestion_before_legacy_parse(
    version: str, marker: Any, retained_request: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _source()
    source["schemaVersion"] = version
    source["data"]["pythonIngestion"] = deepcopy(marker)
    if not retained_request:
        # No ingestion markers will be generated. A recognized profile must not
        # pass through as opaque JSON and become active only on the v2 cold load.
        source["data"].pop("conversationHistory")
    completions = _completions(source, *([_original_result()] if retained_request else []))
    delivery = _evidence(source, [Message("user", ["accepted"], message_id="custom-id")] if retained_request else [])
    before = deepcopy((source, completions, delivery))

    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Reserved legacy fields must be rejected before DurableAgentState.from_dict.")

    monkeypatch.setattr(DurableAgentState, "from_dict", forbidden)
    with pytest.raises(ValueError, match="Legacy data contains reserved pythonIngestion metadata"):
        _migrate(source, completion_evidence=completions, delivery_evidence=delivery)
    assert (source, completions, delivery) == before


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
    evidence = _evidence(
        source,
        messages,
        message_positions=[{"producer": "upstream", "position": 1}, {"producer": "upstream", "position": 3}, None],
    )
    before_source, before_evidence = deepcopy(source), deepcopy(evidence)
    completions = _completions(source, _original_result())
    result = _migrate(source, delivery_evidence=evidence, completion_evidence=completions)
    assert result.data.ingested_messages == {message.message_id: [message_identity(message)] for message in messages}
    size = len(json.dumps(result.to_dict(), allow_nan=False))
    assert size > len(json.dumps(result.to_dict(), ensure_ascii=False, allow_nan=False).encode("utf-8"))
    assert (
        _migrate(source, delivery_evidence=evidence, completion_evidence=completions, max_state_bytes=size).to_dict()
        == result.to_dict()
    )
    without_metadata = result.to_dict()
    del without_metadata["data"]["migration"]
    for budget in (size - 1, len(json.dumps(without_metadata, allow_nan=False))):
        with pytest.raises(StateCapacityError) as error:
            _migrate(source, delivery_evidence=evidence, completion_evidence=completions, max_state_bytes=budget)
        assert error.value.size_bytes == error.value.floor_bytes == size
        assert error.value.max_state_bytes == error.value.target_bytes == budget
    assert source == before_source and evidence == before_evidence
    assert result.to_dict()["data"]["conversationHistory"] == source["data"]["conversationHistory"]
    assert "truncation" not in result.to_dict()["data"]


def test_source_is_canonically_encoded_once_for_hash_and_detachment(monkeypatch: pytest.MonkeyPatch) -> None:
    from agent_framework_durabletask import _state_migration as migration_module

    shared = {"values": [False, 0, 0.0, None, "雪😀"]}
    source: dict[str, Any] = {"schemaVersion": "1.1.0", "data": {"future": shared}, "future": shared}
    canonical = json.dumps(source, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    encode = migration_module._canonical_json
    calls: list[object] = []

    def observed(value: Any) -> str:
        if value is source:
            calls.append(value)
        return encode(value)

    monkeypatch.setattr(migration_module, "_canonical_json", observed)
    result = migrate_legacy_state(
        source,
        source_digest=digest,
        source_session_id=SESSION_ID,
        migration_id="migration-1",
        ownership_transfer_id="transfer-1",
        delivery_window_seconds=WINDOW,
        now=NOW,
    )
    assert len(calls) == 1
    assert result.data.unknown_fields["migration"]["sourceDigest"] == digest
    assert result.to_dict()["data"]["conversationHistory"] == []
    assert "conversationHistory" not in source["data"]
    expected = json.loads(canonical)["future"]
    shared["values"].append("caller edit")
    assert result.to_dict()["future"] == result.to_dict()["data"]["future"] == expected
    caller_before = deepcopy(source)
    result.unknown_fields["future"]["values"].append("result edit")
    assert result.to_dict()["data"]["future"] == expected
    assert source == caller_before


@pytest.mark.parametrize("budget", [None, 1_000_000])
def test_unbounded_migration_skips_only_outer_size_encoding(
    budget: int | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    from agent_framework_durabletask import _state_migration as migration_module

    source: dict[str, Any] = {"schemaVersion": "1.1.0", "data": {}}
    encoded_targets: list[dict[str, Any]] = []
    validated_targets: list[DurableAgentState] = []
    to_dict = DurableAgentState.to_dict

    def observe_dump(value: Any, **kwargs: Any) -> str:
        if isinstance(value, dict) and value.get("schemaVersion") == "2.0.0":
            encoded_targets.append(value)
        return json.dumps(value, **kwargs)

    def observe_validation(state: DurableAgentState) -> dict[str, Any]:
        if state.schema_version == "2.0.0":
            validated_targets.append(state)
        return to_dict(state)

    # Replace only the migration module's JSON reference, not process-wide json.dumps.
    monkeypatch.setattr(migration_module, "json", SimpleNamespace(dumps=observe_dump, loads=json.loads))
    monkeypatch.setattr(DurableAgentState, "to_dict", observe_validation)
    result = _migrate(source, max_state_bytes=budget)
    assert validated_targets == [result]
    assert len(encoded_targets) == (0 if budget is None else 1)
    assert to_dict(result)["data"]["session"] == {"session_id": SESSION_ID, "state": {}}


@pytest.mark.parametrize("count", [32, 64])
def test_journal_revision_membership_is_linear_and_preserves_encounter_order(
    count: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    from agent_framework_durabletask import _state_migration as migration_module

    source = _source()
    source["data"]["ingestedMessages"] = {"custom-id": ["opaque legacy value"]}
    messages = [
        Message(
            "user",
            [f"revision-{revision}"],
            message_id=identity,
            additional_properties={"values": [False, 0, 0.0, None, "雪"]},
        )
        for revision in reversed(range(count))
        for identity in ("custom-id", "another-id")
    ]
    evidence = _evidence(source, messages)
    completion = _completions(source, _original_result())
    before = deepcopy((source, evidence, completion))
    expected: dict[str, list[str]] = {}
    for raw in evidence["messages"]:
        encoded = json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        expected.setdefault(raw["message_id"], []).append(hashlib.sha256(encoded.encode("utf-8")).hexdigest())
    comparisons = 0

    class Fingerprint(str):
        __hash__ = str.__hash__

        def __eq__(self, other: object) -> bool:
            nonlocal comparisons
            comparisons += 1
            return str.__eq__(self, other)

    def counted_fingerprint(message: Message) -> str:
        return Fingerprint(message_identity(message))

    monkeypatch.setattr(migration_module, "message_identity", counted_fingerprint)
    result = _migrate(source, delivery_evidence=evidence, completion_evidence=completion)
    # Count membership work, not wall time. List membership takes count*(count-1)
    # comparisons across these two IDs before any final snapshot validation.
    assert comparisons <= 4 * len(messages)
    assert _cold(result).data.ingested_messages == expected
    assert result.to_dict()["data"]["ingestedMessages"] == before[0]["data"]["ingestedMessages"]
    assert (source, evidence, completion) == before

    evidence["messages"].append(deepcopy(evidence["messages"][0]))
    duplicate_before = deepcopy(evidence)
    with pytest.raises(ValueError, match="duplicate message ID/fingerprint"):
        _migrate(source, delivery_evidence=evidence, completion_evidence=completion)
    assert evidence == duplicate_before and source == before[0] and completion == before[2]
