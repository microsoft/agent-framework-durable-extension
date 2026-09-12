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

from agent_framework_durabletask import _durable_agent_state as state_module
from agent_framework_durabletask import migrate_legacy_state, state_snapshot_digest
from agent_framework_durabletask._durable_agent_state import (
    DurableAgentState,
    DurableAgentStateContent,
    DurableAgentStateTextContent,
)
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._response_utils import is_terminal_agent_response, load_agent_response
from agent_framework_durabletask._workflows.naming import workflow_message_id

NOW = datetime(2026, 9, 9, 12, tzinfo=timezone.utc)
OLD = datetime(2024, 1, 1, tzinfo=timezone.utc)
SESSION_ID = "dafx-agent:original-session"


@pytest.fixture(scope="module")
def schema() -> dict[str, Any]:
    path = Path(__file__).resolve().parents[4] / "schemas" / "durable-agent-entity-state.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _validate(payload: dict[str, Any], schema: dict[str, Any]) -> None:
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(payload)


def _source(*, version: str = "1.1.0", contents: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "schemaVersion": version,
        "data": {
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
            ]
        },
    }


def _migrate(source: dict[str, Any], *, evidence: dict[str, Any] | None = None) -> DurableAgentState:
    return migrate_legacy_state(
        source,
        source_digest=state_snapshot_digest(source),
        source_session_id=SESSION_ID,
        migration_id="followup-migration",
        ownership_transfer_id="authorized-transfer",
        delivery_window_seconds=60,
        delivery_evidence=evidence,
        now=NOW,
    )


def _evidence(source: dict[str, Any], messages: list[Message]) -> dict[str, Any]:
    return {
        "sourceDigest": state_snapshot_digest(source),
        "evidenceId": "complete-journal",
        "complete": True,
        "messages": [message.to_dict() for message in messages],
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

    with pytest.raises(ValueError, match="strict JSON"):
        DurableAgentState.from_dict(raw)

    # The write boundary uses the same validation, not only migration's digest.
    state = DurableAgentState()
    state.unknown_fields["future"] = {"nested": [invalid]}
    with pytest.raises(ValueError, match="strict JSON"):
        state.to_dict()
    with pytest.raises(ValueError, match="strict JSON"):
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
    with pytest.raises(ValueError, match="strict JSON"):
        DurableAgentState.from_dict(raw)


@pytest.mark.parametrize("invalid", [{1: "numeric", "1": "string"}, (1, 2), float("nan")])
def test_mailbox_snapshot_rejects_non_json_without_staging_a_completion(
    invalid: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    response = AgentResponse(messages=[])
    payload = {"type": "agent_response", "messages": [], "future": {"nested": invalid}}
    monkeypatch.setattr(state_module, "serialize_agent_response", lambda _: payload)
    state = DurableAgentState()
    before = state.to_dict()
    with pytest.raises(ValueError, match="strict JSON"):
        state.record_response("done", response, delivery_window_seconds=60, now=NOW)
    assert state.to_dict() == before


def test_subtype_null_exceptions_match_the_shared_schema(schema: dict[str, Any]) -> None:
    definitions = schema["$defs"]
    known = {
        definitions[branch["$ref"].split("/")[-1]]["properties"]["$type"]["const"]: definitions[
            branch["$ref"].split("/")[-1]
        ]
        for branch in definitions["chatContentItem"]["oneOf"]
        if "$ref" in branch
    }
    subclasses = {cls.type: cls for cls in DurableAgentStateContent.__subclasses__() if cls.type}
    assert subclasses.keys() == known.keys()
    nullable_fields = {kind: cls._NULLABLE_FIELDS for kind, cls in subclasses.items() if cls._NULLABLE_FIELDS}
    assert nullable_fields == {"unknown": {"content"}, "functionResult": {"result"}}
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


def test_optional_nonnullable_fields_are_omitted_without_dropping_empty_text_or_zero_flags(
    schema: dict[str, Any],
) -> None:
    contents: list[dict[str, Any]] = [
        {"$type": "reasoning", "text": None},
        {"$type": "uri", "uri": "https://example.test", "mediaType": None},
        {"$type": "data", "uri": "data:text/plain,", "mediaType": None},
        {"$type": "functionCall", "callId": "call", "name": "f", "arguments": None},
        {"$type": "text", "text": "", "extensionData": {"flag": False, "count": 0}},
        {"$type": "usage", "usage": {"inputTokenCount": 0}},
    ]
    state = DurableAgentState.from_dict(_source(version="2.0.0", contents=contents))
    persisted = state.to_dict()
    _validate(persisted, schema)
    actual = persisted["data"]["conversationHistory"][0]["messages"][0]["contents"]
    expected = [{key: value for key, value in item.items() if value is not None} for item in contents]
    assert actual == expected


def test_required_text_is_not_silently_omitted_on_write() -> None:
    with pytest.raises(ValueError, match="requires a text string"):
        DurableAgentStateTextContent(text=None).to_persisted_dict()


def test_future_raw_content_preserves_nulls_and_never_interprets_extensions(schema: dict[str, Any]) -> None:
    content = {
        "$type": "futureContent",
        "content": None,
        "payload": [False, 0, {}],
        "extensionData": {"coreContent": {"type": "text", "text": "not authoritative"}},
    }
    raw = _source(version="2.0.0", contents=[content])
    state = DurableAgentState.from_json(json.dumps(raw))
    _validate(state.to_dict(), schema)
    restored = state.data.conversation_history[0].messages[0].contents[0].to_core_content()
    assert restored.type == "unknown"
    assert restored.additional_properties == {"content": content}
    restored.additional_properties["content"]["payload"].append("consumer edit")
    assert state.to_dict() == raw


@pytest.mark.parametrize("version", ["2.0.1", "2.1.0", "2.999.0"])
def test_future_revision_reads_but_cannot_prepare_a_write_preserving_all_control_state(
    version: str, schema: dict[str, Any]
) -> None:
    raw = _source(version=version)
    raw["futureRoot"] = {"opaque": [None, False, 0]}
    raw["data"].update(
        futureData={"opaque": [None]},
        session={"session_id": SESSION_ID, "state": {"provider": {"thread": "original", "value": None}}},
        extensionData={"opaque": [None]},
        ingestedPositions={"producer": 3},
        ingestedMessages={"custom-id": None, "exact": ["a" * 64]},
        completedCorrelations={"done": {"completedAt": NOW.isoformat(), "future": None}},
        responseMailbox={
            "done": {
                "createdAt": NOW.isoformat(),
                "expiresAt": "2099-01-01T00:00:00+00:00",
                "future": None,
                "response": {"type": "agent_response", "messages": [], "future": {"opaque": None}},
            }
        },
        truncation={"evictedMessageCount": 1, "firstEvictedAt": OLD.isoformat(), "lastEvictedAt": NOW.isoformat()},
    )
    raw["data"]["conversationHistory"].append({"$type": "futureEntry", "messages": {"opaque": None}})
    _validate(raw, schema)
    before = deepcopy(raw)
    state = DurableAgentState.from_json(json.dumps(raw))
    assert state.try_get_agent_response("done") is not None
    with pytest.raises(ValueError, match="Only 2.0.0 is writable"):
        state.prepare_for_write(delivery_window_seconds=60)
    assert state.to_dict() == raw == before


def test_exact_current_revision_is_writable_without_mutation() -> None:
    state = DurableAgentState.from_dict(_source(version=DurableAgentState.SCHEMA_VERSION))
    before = state.to_dict()
    state.prepare_for_write(delivery_window_seconds=60)
    assert state.to_dict() == before


@pytest.mark.parametrize("version", ["1.0.0", "1.999.0"])
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


@pytest.mark.parametrize("version", ["2", "2.", "2.1.0-preview", "2.1.0\n", "2.\u0661.0", "3.0.0"])
def test_version_admission_requires_a_complete_supported_ascii_version(version: str) -> None:
    with pytest.raises(ValueError, match="Unsupported.*schemaVersion"):
        DurableAgentState.from_dict(_source(version=version))


@pytest.mark.parametrize("kind", ["errorResponse", "response"])
def test_migrated_text_only_failure_retains_http_terminal_classification(kind: str) -> None:
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
    state = DurableAgentState.from_json(_migrate(source).to_json())
    payload = state.data.response_mailbox["done"]["response"]
    delivered = load_agent_response(payload)
    for response in (legacy_response, delivered):
        assert response.text == "legacy text only"
        assert all(content.type == "text" for message in response.messages for content in message.contents)
        # HTTP polling branches on this predicate, even when there is no error Content.
        assert is_terminal_agent_response(response) is (kind == "errorResponse")
        assert response.additional_properties == ({"durable_status": "error"} if kind == "errorResponse" else {})
    assert state.data.completed_correlations["done"] == {
        "completedAt": NOW.isoformat(),
        "legacy": True,
        **({"outcome": "failed"} if kind == "errorResponse" else {}),
    }
    assert state.to_dict()["data"]["conversationHistory"] == source["data"]["conversationHistory"]
    assert source == before


@pytest.mark.parametrize("retained_contents", [[], [{"$type": "text", "text": "pruned portion"}]])
@pytest.mark.parametrize("journal_kind", ["empty", "other-custom", "workflow"])
def test_complete_journal_cannot_omit_retained_custom_request_identity(
    retained_contents: list[dict[str, Any]], journal_kind: str
) -> None:
    source = _source(contents=retained_contents)
    messages: list[Message] = []
    if journal_kind == "other-custom":
        messages = [Message("user", ["accepted"], message_id="other-custom")]
    elif journal_kind == "workflow":
        source["data"]["ingestedPositions"] = {"upstream": 3}
        messages = [Message("user", ["accepted"], message_id=workflow_message_id("upstream", 3))]
    evidence = _evidence(source, messages)
    before_source, before_evidence = deepcopy(source), deepcopy(evidence)
    with pytest.raises(ValueError, match="must include every retained legacy custom request message ID"):
        _migrate(source, evidence=evidence)
    assert source == before_source
    assert evidence == before_evidence


@pytest.mark.parametrize("retained_contents", [[], [{"$type": "text", "text": "pruned portion"}]])
def test_custom_journal_compares_identity_not_the_pruned_body(retained_contents: list[dict[str, Any]]) -> None:
    source = _source(contents=retained_contents)
    original = Message("user", ["complete original accepted input"], message_id="custom-id")
    evidence = _evidence(source, [original])
    before_source, before_evidence = deepcopy(source), deepcopy(evidence)
    state = DurableAgentState.from_json(_migrate(source, evidence=evidence).to_json())
    assert state.data.ingested_messages == {"custom-id": [message_identity(original)]}
    assert state.to_dict()["data"]["conversationHistory"] == source["data"]["conversationHistory"]
    assert source == before_source
    assert evidence == before_evidence


def test_no_journal_keeps_custom_identity_markers_without_fabricating_workflow_receipts() -> None:
    source = _source(contents=[])
    source["data"]["conversationHistory"][0]["messages"].append({
        "role": "user",
        "messageId": workflow_message_id("upstream", 3),
        "contents": [],
    })
    source["data"]["ingestedMessages"] = {"existing-exact": ["a" * 64], "existing-marker": None}
    before = deepcopy(source)
    state = DurableAgentState.from_json(_migrate(source).to_json())
    assert state.data.ingested_messages == {
        "custom-id": None,
        "existing-exact": ["a" * 64],
        "existing-marker": None,
    }
    assert state.data.completed_correlations == {}
    assert state.data.response_mailbox == {}
    assert source == before


def test_empty_complete_journal_is_valid_when_no_retained_custom_requests_contradict_it() -> None:
    source = _source(contents=[])
    history = source["data"]["conversationHistory"]
    history[0]["messages"][0]["messageId"] = workflow_message_id("upstream", 3)
    history.append({"$type": "futureEntry", "messages": {"messageId": "opaque-not-a-request"}})
    state = DurableAgentState.from_json(_migrate(source, evidence=_evidence(source, [])).to_json())
    assert state.data.ingested_messages == {}
    assert state.to_dict()["data"]["conversationHistory"] == history
