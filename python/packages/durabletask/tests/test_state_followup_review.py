# Copyright (c) Microsoft. All rights reserved.

"""Focused state and detached migration regressions, without entity ownership changes."""

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import jsonschema
import pytest
from agent_framework import AgentResponse, Message

from agent_framework_durabletask import _delivery_state as delivery_module
from agent_framework_durabletask import _durable_agent_state as state_module
from agent_framework_durabletask import migrate_legacy_state, state_snapshot_digest
from agent_framework_durabletask._durable_agent_state import (
    DurableAgentState,
    DurableAgentStateContent,
    DurableAgentStateTextContent,
)
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._response_utils import is_terminal_agent_response
from agent_framework_durabletask._shared_response import load_terminal_response, serialize_terminal_response
from agent_framework_durabletask._workflows.naming import workflow_message_id

NOW = datetime(2026, 9, 9, 12, tzinfo=timezone.utc)
OLD = datetime(2024, 1, 1, tzinfo=timezone.utc)
ORIGINAL_COMPLETED_AT = "2024-01-02T03:04:05.123456789Z"
SESSION_ID = "dafx-agent:original-session"


@pytest.fixture(scope="module")
def schema() -> dict[str, Any]:
    path = Path(__file__).resolve().parents[4] / "schemas" / "durable-agent-entity-state.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _validate(payload: dict[str, Any], schema: dict[str, Any]) -> None:
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(payload)


@pytest.mark.parametrize(
    "fixture_path",
    sorted((Path(__file__).resolve().parents[4] / "schemas" / "fixtures").glob("shared-durable-agent-state-2.0*.json")),
    ids=lambda path: path.stem,
)
def test_published_shared_fixtures_are_admitted_without_changing_original_json(
    fixture_path: Path, schema: dict[str, Any]
) -> None:
    raw = json.loads(fixture_path.read_text(encoding="utf-8"))
    before = deepcopy(raw)
    _validate(raw, schema)
    state = DurableAgentState.from_json(json.dumps(raw))
    state.prepare_for_write(delivery_window_seconds=60)
    assert state.to_dict() == before
    assert json.dumps(state.to_dict(), sort_keys=True) == json.dumps(before, sort_keys=True)
    for result in state.data.response_mailbox.values():
        projection = load_terminal_response(result["response"])
        assert serialize_terminal_response(projection) == result["response"]
        projection.messages.clear()
    detached = state.to_dict()
    detached["data"]["terminalResults"].clear()
    assert state.to_dict() == before and raw == before


@pytest.mark.parametrize(
    "mismatch",
    [
        "result-key",
        "receipt-key",
        "outcome",
        "completedAt",
        "resultExpiresAt",
        "missing-receipt",
        "missing-result",
        "unavailable-with-result",
    ],
)
def test_schema_valid_cross_map_corruption_is_rejected_without_mutating_input(
    mismatch: str, schema: dict[str, Any]
) -> None:
    path = Path(__file__).resolve().parents[4] / "schemas" / "fixtures" / "shared-durable-agent-state-2.0.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert DurableAgentState.from_dict(raw).to_dict() == raw
    results = raw["data"]["terminalResults"]
    receipts = raw["data"]["completionReceipts"]
    result, receipt = results["corr-2"], receipts["corr-2"]
    if mismatch == "result-key":
        result["correlationId"] = "other"
    elif mismatch == "receipt-key":
        receipt["correlationId"] = "other"
    elif mismatch == "outcome":
        receipt["outcome"] = "failed"
    elif mismatch == "completedAt":
        receipt["completedAt"] = "2026-09-10T05:00:04+00:00"
    elif mismatch == "resultExpiresAt":
        receipt["resultExpiresAt"] = "2026-09-11T05:00:04+00:00"
    elif mismatch == "missing-receipt":
        del receipts["corr-2"]
    elif mismatch == "missing-result":
        del results["corr-2"]
    else:
        receipt.update(resultState="unavailable", resultUnavailableAt="2026-09-11T05:00:04+00:00")
    _validate(raw, schema)  # JSON Schema does not express these cross-map invariants.
    before = deepcopy(raw)
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(raw)
    with pytest.raises(ValueError):
        DurableAgentState.from_json(json.dumps(raw))
    assert raw == before


def _source(*, version: str = "1.1.0", contents: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "schemaVersion": version,
        "data": {
            **({"terminalResults": {}, "completionReceipts": {}} if version == "2.0.0" else {}),
            "conversationHistory": [
                {
                    "$type": "request",
                    "correlationId": "turn",
                    "createdAt": OLD.isoformat(),
                    "messages": [
                        {
                            "role": "user",
                            "messageId": "custom-id",
                            "contents": contents if contents is not None else [{"$type": "text", "text": "retained"}],
                        }
                    ],
                }
            ],
        },
    }


def _migrate(
    source: dict[str, Any], *, completion_evidence: dict[str, Any], evidence: dict[str, Any] | None = None
) -> DurableAgentState:
    return migrate_legacy_state(
        source,
        source_digest=state_snapshot_digest(source),
        source_session_id=SESSION_ID,
        migration_id="followup-migration",
        ownership_transfer_id="authorized-transfer",
        delivery_window_seconds=60,
        delivery_evidence=evidence,
        completion_evidence=completion_evidence,
        now=NOW,
    )


def _completions(source: dict[str, Any], *results: dict[str, Any]) -> dict[str, Any]:
    return {
        "sourceDigest": state_snapshot_digest(source),
        "evidenceId": "followup-completion-journal",
        "complete": True,
        "results": deepcopy(list(results)),
    }


def _evidence(
    source: dict[str, Any],
    messages: list[Message],
    *,
    message_positions: list[dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    return {
        "sourceDigest": state_snapshot_digest(source),
        "evidenceId": "complete-journal",
        "complete": True,
        "messages": [message.to_dict() for message in messages],
        **({"messagePositions": deepcopy(message_positions)} if message_positions is not None else {}),
    }


@pytest.mark.parametrize(
    "invalid",
    [
        pytest.param({1: "numeric", "1": "string"}, id="colliding-keys"),
        pytest.param({False: "boolean-key"}, id="boolean-key"),
        pytest.param({None: "null-key"}, id="null-key"),
        pytest.param((1, "tuple"), id="tuple"),
        pytest.param(object(), id="object"),
        pytest.param(float("nan"), id="nan"),
        pytest.param(float("inf"), id="infinity"),
        pytest.param(float("-inf"), id="negative-infinity"),
    ],
)
@pytest.mark.parametrize("location", ["root", "session", "content"])
def test_ordinary_state_rejects_non_json_before_encoding(invalid: Any, location: str) -> None:
    raw = _source(version="2.0.0")
    target = raw
    if location == "session":
        target = {"session_id": SESSION_ID, "state": {}}
        raw["data"]["session"] = target
    elif location == "content":
        target = raw["data"]["conversationHistory"][0]["messages"][0]["contents"][0]
    target["future"] = {"nested": [invalid]}

    with pytest.raises(ValueError, match="JSON"):
        DurableAgentState.from_dict(raw)

    # The write boundary uses the same validation, not only migration's digest.
    state = DurableAgentState()
    state.unknown_fields["future"] = {"nested": [invalid]}
    with pytest.raises(ValueError, match="JSON"):
        state.to_dict()
    with pytest.raises(ValueError, match="JSON"):
        state_snapshot_digest(raw)


def test_strict_json_snapshot_preserves_valid_values_and_detaches_them() -> None:
    raw = _source(version="2.0.0")
    raw["future"] = {"1": [None, False, 0, 0.0, "", [], {}, "雪"]}
    before = deepcopy(raw)
    state = DurableAgentState.from_dict(raw)
    assert state.to_dict() == before
    assert json.dumps(state.to_dict(), sort_keys=True) == json.dumps(before, sort_keys=True)
    raw["future"]["1"].append("caller edit")
    detached = state.to_dict()
    detached["future"]["1"].append("consumer edit")
    assert state.to_dict() == before


def test_ordinary_state_rejects_cycles_as_invalid_json() -> None:
    raw = _source(version="2.0.0")
    raw["cycle"] = raw
    with pytest.raises(ValueError, match="JSON"):
        DurableAgentState.from_dict(raw)


@pytest.mark.parametrize("invalid", [{1: "numeric", "1": "string"}, (1, 2), float("nan")])
def test_mailbox_snapshot_rejects_non_json_without_staging_a_completion(
    invalid: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    response = AgentResponse(messages=[])
    payload = {"type": "agent_response", "messages": [], "future": {"nested": invalid}}
    monkeypatch.setattr(delivery_module, "serialize_agent_response", lambda _: payload)
    state = DurableAgentState()
    before = state.to_dict()
    with pytest.raises(ValueError, match="JSON"):
        state.record_response("done", response, delivery_window_seconds=60, now=NOW)
    assert state.to_dict() == before


def test_subtype_null_exceptions_match_the_shared_schema(schema: dict[str, Any]) -> None:
    definitions = schema["$defs"]

    def resolve(branch: dict[str, Any]) -> dict[str, Any]:
        while "$ref" in branch:
            branch = definitions[branch["$ref"].split("/")[-1]]
        return branch

    known = {
        resolve(branch)["properties"]["$type"]["const"]: resolve(branch)
        for branch in definitions["v2ChatContentItem"]["oneOf"]
    }
    subclasses = {cls.type: cls for cls in DurableAgentStateContent.__subclasses__() if cls.type}
    assert subclasses.keys() == known.keys()
    nullable_fields = {kind: cls._NULLABLE_FIELDS for kind, cls in subclasses.items() if cls._NULLABLE_FIELDS}
    assert nullable_fields == {
        kind: {
            field
            for field, field_schema in definition["properties"].items()
            if jsonschema.Draft202012Validator(resolve(field_schema)).is_valid(None)
        }
        for kind, definition in known.items()
        if any(
            jsonschema.Draft202012Validator(resolve(field)).is_valid(None)
            for field in definition["properties"].values()
        )
    }
    for kind, fields in nullable_fields.items():
        for field in fields:
            jsonschema.Draft202012Validator(known[kind]["properties"][field]).validate(None)
    assert "content" in known["unknown"]["required"]
    assert "text" in known["text"]["required"]


@pytest.mark.parametrize("opaque", [None, False, 0, 0.0, "", [], {}])
def test_unknown_falsey_payloads_preserve_required_content_and_opaque_extensions(
    opaque: Any, schema: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = _source(
        version="2.0.0",
        contents=[
            {
                "$type": "unknown",
                "content": opaque,
                "future": {"null": None, "flag": False, "count": 0},
                "extensionData": {"coreContent": {"type": "future.module.Content", "items": {"opaque": None}}},
            }
        ],
    )
    _validate(raw, schema)
    state = DurableAgentState.from_json(json.dumps(raw))
    persisted = json.loads(state.to_json())
    _validate(persisted, schema)
    assert persisted == raw
    stored = state.data.conversation_history[0].messages[0].contents[0]
    loader = Mock(side_effect=AssertionError("Opaque content must not interpret extensionData.coreContent"))
    monkeypatch.setattr(state_module, "load_agent_response", loader)
    for restored in (stored.to_ai_content(), stored.to_core_content()):
        assert restored.type == "unknown"
        assert restored.additional_properties == {"content": opaque}
        assert type(restored.additional_properties["content"]) is type(opaque)
    loader.assert_not_called()
    assert state.to_dict() == raw


@pytest.mark.parametrize("result", [None, False, 0, "", [], {}])
def test_function_result_nullable_payload_survives_schema_and_cold_roundtrip(
    result: Any, schema: dict[str, Any]
) -> None:
    raw = _source(version="2.0.0", contents=[{"$type": "functionResult", "callId": "call", "result": result}])
    _validate(raw, schema)
    state = DurableAgentState.from_json(json.dumps(raw))
    persisted = json.loads(state.to_json())
    _validate(persisted, schema)
    assert persisted == raw
    assert "result" in persisted["data"]["conversationHistory"][0]["messages"][0]["contents"][0]


@pytest.mark.parametrize(
    "invalid_content",
    [
        {"$type": "reasoning", "text": None},
        {"$type": "uri", "uri": "https://example.test", "mediaType": None},
        {"$type": "data", "uri": "data:text/plain,", "mediaType": None},
        {"$type": "functionCall", "callId": "call", "name": "f", "arguments": None},
    ],
)
def test_present_nonnullable_fields_reject_null_but_absence_and_falsey_metadata_survive(
    invalid_content: dict[str, Any],
    schema: dict[str, Any],
) -> None:
    contents: list[dict[str, Any]] = [
        invalid_content,
        {"$type": "text", "text": "", "extensionData": {"flag": False, "count": 0}},
        {"$type": "usage", "usage": {"inputTokenCount": 0}},
    ]
    raw = _source(version="2.0.0", contents=contents)
    before = deepcopy(raw)
    with pytest.raises(jsonschema.ValidationError):
        _validate(raw, schema)
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(raw)
    assert raw == before
    # Omission is valid. Loading a present null must not silently produce this shape.
    expected = [{key: value for key, value in item.items() if value is not None} for item in contents]
    state = DurableAgentState.from_dict(_source(version="2.0.0", contents=expected))
    persisted = state.to_dict()
    _validate(persisted, schema)
    actual = persisted["data"]["conversationHistory"][0]["messages"][0]["contents"]
    assert actual == expected


def test_required_text_is_not_silently_omitted_on_write() -> None:
    with pytest.raises(ValueError, match="requires a text string"):
        DurableAgentStateTextContent(text=None).to_persisted_dict()


def test_explicit_unknown_wrapper_preserves_future_json_without_interpreting_extensions(schema: dict[str, Any]) -> None:
    content = {
        "$type": "futureContent",
        "content": None,
        "payload": [False, 0, {}],
        "extensionData": {"coreContent": {"type": "text", "text": "not authoritative"}},
    }
    raw = _source(version="2.0.0", contents=[{"$type": "unknown", "content": content}])
    state = DurableAgentState.from_json(json.dumps(raw))
    _validate(state.to_dict(), schema)
    restored = state.data.conversation_history[0].messages[0].contents[0].to_core_content()
    assert restored.type == "unknown"
    assert restored.additional_properties == {"content": content}
    restored.additional_properties["content"]["payload"].append("consumer edit")
    assert state.to_dict() == raw


@pytest.mark.parametrize("version", ["2.0.1", "2.1.0", "2.999.0"])
def test_future_revision_is_rejected_without_changing_any_control_state(version: str, schema: dict[str, Any]) -> None:
    raw = _source(version="2.0.0")
    raw["futureRoot"] = {"opaque": [None, False, 0]}
    raw["data"].update(
        futureData={"opaque": [None]},
        session={"session_id": SESSION_ID, "state": {"provider": {"thread": "original", "value": None}}},
        extensionData={"opaque": [None]},
        ingestedPositions={"producer": 3},
        ingestedMessages={"opaqueLegacy": [None, False, {"custom": 0}]},
        pythonIngestion={
            "profile": "agent-framework-python.ingestion",
            "version": 1,
            "messages": {"custom-id": None, "exact": ["a" * 64]},
        },
        truncation={"evictedMessageCount": 1, "firstEvictedAt": OLD.isoformat(), "lastEvictedAt": NOW.isoformat()},
    )
    completion = {"correlationId": "done", "outcome": "succeeded", "completedAt": NOW.isoformat()}
    raw["data"]["terminalResults"]["done"] = {
        **completion,
        "future": None,
        "response": {"messages": [], "future": {"opaque": None}},
    }
    raw["data"]["completionReceipts"]["done"] = {**completion, "resultState": "available", "future": None}
    raw["data"]["conversationHistory"].append({"$type": "request", "messages": [], "futureEntry": {"opaque": None}})
    _validate(raw, schema)
    current = DurableAgentState.from_json(json.dumps(raw))
    current.prepare_for_write(delivery_window_seconds=60)
    assert current.to_dict() == raw
    raw["schemaVersion"] = version
    before = deepcopy(raw)
    with pytest.raises(jsonschema.ValidationError):
        _validate(raw, schema)
    with pytest.raises(ValueError, match="Unsupported.*schemaVersion"):
        DurableAgentState.from_json(json.dumps(raw))
    assert raw == before


def test_exact_current_revision_is_writable_without_mutation() -> None:
    state = DurableAgentState.from_dict(_source(version=DurableAgentState.SCHEMA_VERSION))
    before = state.to_dict()
    state.prepare_for_write(delivery_window_seconds=60)
    assert state.to_dict() == before


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
def test_legacy_write_rejection_message_remains_unchanged(version: str) -> None:
    state = DurableAgentState.from_dict(_source(version=version))
    before = state.to_dict()
    with pytest.raises(ValueError) as error:
        state.prepare_for_write(delivery_window_seconds=60)
    assert str(error.value) == (
        "Legacy state is read-only in this runtime. Keep it on its original deployment or use explicit "
        "migration into a separate isolated-v2 entity. Legacy ingestedPositions require recorded delivery evidence."
    )
    assert state.to_dict() == before


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize("field", ["terminalResults", "completionReceipts", "historyBinding"])
def test_historical_versions_reject_v2_maps_even_when_empty(version: str, field: str, schema: dict[str, Any]) -> None:
    raw = _source(version=version)
    _validate(raw, schema)
    assert DurableAgentState.from_dict(raw).to_dict() == raw
    raw["data"][field] = {}
    before = deepcopy(raw)
    with pytest.raises(jsonschema.ValidationError):
        _validate(raw, schema)
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(raw)
    with pytest.raises(ValueError):
        DurableAgentState.from_json(json.dumps(raw))
    assert raw == before


@pytest.mark.parametrize("version", ["1.0.1", "1.999.0", "2", "2.", "2.1.0-preview", "2.1.0\n", "2.\u0661.0", "3.0.0"])
def test_version_admission_requires_a_complete_supported_ascii_version(version: str) -> None:
    with pytest.raises(ValueError, match="Unsupported.*schemaVersion"):
        DurableAgentState.from_dict(_source(version=version))


@pytest.mark.parametrize("kind", ["errorResponse", "response"])
@pytest.mark.parametrize("strict", [False, True])
def test_text_only_migration_preserves_proven_failure_but_never_fabricates_success(kind: str, strict: bool) -> None:
    source = _source()
    source["data"]["conversationHistory"] = [
        {
            "$type": kind,
            "correlationId": "done",
            "createdAt": OLD.isoformat(),
            "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "legacy text only"}]}],
        }
    ]
    before = deepcopy(source)
    legacy_response = DurableAgentState.from_dict(source).try_get_agent_response("done")
    assert legacy_response is not None
    assert legacy_response.text == "legacy text only"
    assert is_terminal_agent_response(legacy_response) is (kind == "errorResponse")
    kwargs: dict[str, Any] = {
        "source_digest": state_snapshot_digest(source),
        "source_session_id": SESSION_ID,
        "migration_id": "followup-migration",
        "ownership_transfer_id": "authorized-transfer",
        "delivery_window_seconds": 60,
        "now": NOW,
        "require_known_outcomes": strict,
    }
    with pytest.raises(ValueError, match="completion.*evidence|known.*outcome"):
        migrate_legacy_state(source, **kwargs)
    assert source == before
    if kind == "response":
        return
    original_result = {
        "correlationId": "done",
        "outcome": "failed",
        "completedAt": ORIGINAL_COMPLETED_AT,
        "response": {
            "createdAt": OLD.isoformat(),
            "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "original failure text"}]}],
            "extensionData": {"durable_status": "error", "durable_outcome": "failed"},
        },
        "error": {"code": "provider_error", "message": "Original invocation failed."},
    }
    completions = _completions(source, original_result)
    before_completions = deepcopy(completions)
    state = DurableAgentState.from_json(
        migrate_legacy_state(source, completion_evidence=completions, **kwargs).to_json()
    )
    payload = state.data.response_mailbox["done"]["response"]
    delivered = load_terminal_response(payload)
    assert delivered.text == "original failure text"
    for response in (legacy_response, delivered):
        assert all(content.type == "text" for message in response.messages for content in message.contents)
        # HTTP polling branches on this predicate, even when there is no error Content.
        assert is_terminal_agent_response(response)
    assert legacy_response.additional_properties == {"durable_status": "error"}
    assert delivered.additional_properties == {"durable_status": "error", "durable_outcome": "failed"}
    assert state.data.completed_correlations["done"] == {
        "correlationId": "done",
        "completedAt": ORIGINAL_COMPLETED_AT,
        "outcome": "failed",
        "resultExpiresAt": "2026-09-09T12:01:00+00:00",
        "resultState": "available",
    }
    assert state.data.response_mailbox["done"] == {
        **original_result,
        "resultExpiresAt": "2026-09-09T12:01:00+00:00",
    }
    assert state.to_dict()["data"]["conversationHistory"] == source["data"]["conversationHistory"]
    assert source == before and completions == before_completions


@pytest.mark.parametrize("retained_contents", [[], [{"$type": "text", "text": "pruned portion"}]])
@pytest.mark.parametrize("journal_kind", ["empty", "other-custom", "workflow"])
@pytest.mark.parametrize("identity", ["custom-id", "wf_retained_7", "wf__3"])
def test_complete_journal_cannot_omit_retained_public_request_identity(
    retained_contents: list[dict[str, Any]], journal_kind: str, identity: str
) -> None:
    source = _source(contents=retained_contents)
    source["data"]["conversationHistory"][0]["messages"][0]["messageId"] = identity
    messages: list[Message] = []
    message_positions: list[dict[str, Any] | None] | None = None
    if journal_kind == "other-custom":
        messages = [Message("user", ["accepted"], message_id="other-custom")]
    elif journal_kind == "workflow":
        source["data"]["ingestedPositions"] = {"upstream": 3}
        messages = [Message("user", ["accepted"], message_id=workflow_message_id("upstream", 3))]
        message_positions = [{"producer": "upstream", "position": 3}]
    evidence = _evidence(source, messages, message_positions=message_positions)
    # This controlled request-only source has no completions, independently of its delivery journal.
    completions = _completions(source)
    before_source, before_evidence = deepcopy(source), deepcopy(evidence)
    before_completions = deepcopy(completions)
    with pytest.raises(ValueError, match="must include every retained legacy request message ID"):
        _migrate(source, evidence=evidence, completion_evidence=completions)
    assert source == before_source
    assert evidence == before_evidence
    assert completions == before_completions


@pytest.mark.parametrize("retained_contents", [[], [{"$type": "text", "text": "pruned portion"}]])
@pytest.mark.parametrize("identity", ["custom-id", "wf_upstream_3", "wf__3"])
def test_public_id_journal_compares_identity_not_the_pruned_body(
    retained_contents: list[dict[str, Any]], identity: str
) -> None:
    source = _source(contents=retained_contents)
    source["data"]["conversationHistory"][0]["messages"][0]["messageId"] = identity
    original = Message("user", ["complete original accepted input"], message_id=identity)
    evidence = _evidence(source, [original])
    completions = _completions(source)
    before_source, before_evidence = deepcopy(source), deepcopy(evidence)
    before_completions = deepcopy(completions)
    state = DurableAgentState.from_json(_migrate(source, evidence=evidence, completion_evidence=completions).to_json())
    assert state.data.ingested_messages == {identity: [message_identity(original)]}
    assert state.data.ingested_positions is None
    assert "ingestedPositions" not in state.to_dict()["data"]
    assert state.data.completed_correlations == state.data.response_mailbox == {}
    assert state.to_dict()["data"]["conversationHistory"] == source["data"]["conversationHistory"]
    assert source == before_source
    assert evidence == before_evidence
    assert completions == before_completions


@pytest.mark.parametrize(
    "opaque",
    [
        None,
        False,
        0,
        "",
        [],
        {"existing-exact": ["a" * 64], "existing-marker": None},
        {"arbitrary": [False, 0, None, {"items": "not receipts"}]},
    ],
)
def test_no_delivery_journal_keeps_public_identity_markers_without_fabricating_exact_receipts(opaque: Any) -> None:
    source = _source(contents=[])
    source["data"]["conversationHistory"][0]["messages"].append({
        "role": "user",
        "messageId": workflow_message_id("upstream", 3),
        "contents": [],
    })
    source["data"]["ingestedMessages"] = deepcopy(opaque)
    before = deepcopy(source)
    completions = _completions(source)
    before_completions = deepcopy(completions)
    state = DurableAgentState.from_json(_migrate(source, completion_evidence=completions).to_json())
    assert state.data.ingested_messages == {
        "custom-id": None,
        workflow_message_id("upstream", 3): None,
    }
    assert state.to_dict()["data"]["ingestedMessages"] == before["data"]["ingestedMessages"]
    assert json.dumps(state.to_dict()["data"]["ingestedMessages"], sort_keys=True) == json.dumps(opaque, sort_keys=True)
    assert state.to_dict()["data"]["pythonIngestion"] == {
        "profile": "agent-framework-python.ingestion",
        "version": 1,
        "messages": {"custom-id": None, workflow_message_id("upstream", 3): None},
    }
    assert state.data.completed_correlations == {}
    assert state.data.response_mailbox == {}
    assert source == before and completions == before_completions


def test_empty_complete_delivery_journal_rejects_a_retained_workflow_shaped_public_id() -> None:
    source = _source(contents=[])
    history = source["data"]["conversationHistory"]
    history[0]["messages"][0]["messageId"] = workflow_message_id("upstream", 3)
    history.append({"$type": "request", "messages": [], "futureEntry": {"messageId": "opaque-not-a-request"}})
    evidence, completions = _evidence(source, []), _completions(source)
    before_source, before_evidence, before_completions = deepcopy(source), deepcopy(evidence), deepcopy(completions)
    with pytest.raises(ValueError, match="every retained legacy request message ID"):
        _migrate(source, evidence=evidence, completion_evidence=completions)
    assert source == before_source and evidence == before_evidence and completions == before_completions


def test_empty_complete_delivery_journal_is_valid_without_retained_public_request_ids() -> None:
    source = _source(contents=[])
    history = source["data"]["conversationHistory"]
    history[0]["messages"] = []
    history.append({"$type": "request", "messages": [], "futureEntry": {"messageId": "opaque-not-a-request"}})
    evidence, completions = _evidence(source, []), _completions(source)
    before_source, before_evidence, before_completions = deepcopy(source), deepcopy(evidence), deepcopy(completions)
    state = DurableAgentState.from_json(_migrate(source, evidence=evidence, completion_evidence=completions).to_json())
    assert state.data.ingested_messages == {}
    assert state.data.completed_correlations == state.data.response_mailbox == {}
    assert state.to_dict()["data"]["conversationHistory"] == history
    assert source == before_source and evidence == before_evidence and completions == before_completions
