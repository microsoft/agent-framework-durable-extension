# Copyright (c) Microsoft. All rights reserved.

"""Transcript fidelity, independent of the parent Root/Data delivery rewrite."""

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast, get_args, get_type_hints

import pytest
from agent_framework import Content, Message
from jsonschema import Draft202012Validator, FormatChecker

from agent_framework_durabletask._durable_agent_state import (
    DurableAgentStateContent,
    DurableAgentStateEntryJsonType,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
    DurableAgentStateTextContent,
    DurableAgentStateUriContent,
    DurableAgentStateUsage,
    _parse_history_entries,
)
from agent_framework_durabletask._history_provider import DurableHistoryBinding, DurableHistoryProvider
from agent_framework_durabletask._shared_state_validation import validate_shared_state

SCHEMAS = Path(__file__).resolve().parents[4] / "schemas"
SCHEMA = json.loads((SCHEMAS / "durable-agent-entity-state.json").read_text(encoding="utf-8"))
HISTORY_PROFILE = {"profile": "agent-framework-python.history-identity", "version": 1}
CONTENT_CASES: list[tuple[str, dict[str, Any]]] = [
    ("data", {"uri": "data:application/octet-stream;base64,AQID"}),
    ("error", {"details": None}),
    ("functionCall", {"callId": "c", "name": "f", "arguments": ' { "partial": '}),
    ("functionResult", {"callId": "c"}),
    ("hostedFile", {"fileId": "f"}),
    ("hostedVectorStore", {"vectorStoreId": "v"}),
    ("usage", {"usage": {"inputTokenCount": 2**65, "extensionData": {"opaque": [None]}, "future": False}}),
    ("text", {"text": ""}),
    ("reasoning", {}),
    ("uri", {"uri": "urn:test"}),
    ("unknown", {"content": None}),
]


def _validate(history: list[dict[str, Any]]) -> None:
    state = {
        "schemaVersion": "2.0.0",
        "data": {"conversationHistory": history, "terminalResults": {}, "completionReceipts": {}},
    }
    Draft202012Validator(SCHEMA, format_checker=FormatChecker()).validate(state)
    validate_shared_state(state)


def _entry(raw: dict[str, Any]) -> Any:
    _validate([raw])
    return _parse_history_entries({"conversationHistory": [raw]})[0]


def test_cases_cover_all_canonical_content_discriminators() -> None:
    def kind(ref: dict[str, Any]) -> str:
        definition = SCHEMA["$defs"][ref["$ref"].rsplit("/", 1)[-1]]
        return kind(definition) if "$ref" in definition else definition["properties"]["$type"]["const"]

    assert {name for name, _ in CONTENT_CASES} == {kind(ref) for ref in SCHEMA["$defs"]["v2ChatContentItem"]["oneOf"]}


@pytest.mark.parametrize("kind", list(DurableAgentStateEntryJsonType))
@pytest.mark.parametrize("fields", [{}, {"messages": []}, {"messages": [{"role": "assistant"}]}])
def test_missing_fields_stay_missing_without_fabricated_time(kind: str, fields: dict[str, Any]) -> None:
    raw = {"$type": kind, **deepcopy(fields), "future": {"presentNull": None}, "extensionData": {}}
    entry = _entry(raw)
    assert entry.created_at is None
    assert entry.to_dict() == raw
    assert _entry(entry.to_dict()).to_dict() == raw


@pytest.mark.parametrize(("kind", "fields"), CONTENT_CASES)
@pytest.mark.parametrize("extension", [None, False, 0, "opaque", [], {"future": None}])
def test_known_content_fields_and_arbitrary_extension_siblings(
    kind: str, fields: dict[str, Any], extension: Any
) -> None:
    raw = {
        "$type": "response",
        "createdAt": "2026-09-16T01:00:00.123456789+01:00",
        "future": [False, None],
        "usage": {"inputTokenCount": 0, "future": {"x": None}, "extensionData": {"provider": [1.25]}},
        "messages": [
            {
                "role": "assistant",
                "messageId": "",
                "authorName": "",
                "createdAt": "2026-09-16t00:00:00.123456789z",
                "extensionData": {},
                "future": {"x": None},
                "contents": [{"$type": kind, **deepcopy(fields), "extensionData": extension, "future": [None, 0]}],
            }
        ],
    }
    before = deepcopy(raw)
    entry = _entry(raw)
    assert entry.to_dict() == before
    # A non-object extensionData is opaque, not a coreContent profile.
    entry.messages[0].to_chat_message()
    assert entry.to_dict() == before and raw == before
    _validate([entry.to_dict()])


@pytest.mark.parametrize("fixture", sorted(SCHEMAS.glob("fixtures/shared-durable-agent-state-2.0*.json")))
def test_shared_fixture_transcripts_without_root_dependency(fixture: Path) -> None:
    source = json.loads(fixture.read_text(encoding="utf-8"))
    history = source["data"]["conversationHistory"]
    assert [entry.to_dict() for entry in _parse_history_entries(source["data"])] == history
    for result in source["data"]["terminalResults"].values():
        for raw in result["response"]["messages"]:
            assert DurableAgentStateMessage.from_dict(raw).to_dict() == raw


@pytest.mark.parametrize("factory", [DurableAgentStateRequest.from_dict, DurableAgentStateResponse.from_dict])
def test_direct_entry_factory_owns_detached_raw_shadow(factory: Any) -> None:
    kind = "request" if factory.__self__ is DurableAgentStateRequest else "response"
    raw: dict[str, Any] = {
        "$type": kind,
        "messages": [{"role": "user", "contents": [{"$type": "text", "text": "before"}]}],
    }
    entry = factory(raw)
    raw["messages"][0]["contents"][0]["text"] = "source mutation"
    assert entry.messages[0].text == "before"
    output = entry.to_dict()
    output["messages"][0]["contents"][0]["text"] = "output mutation"
    assert entry.to_dict()["messages"][0]["contents"][0]["text"] == "before"


def test_mutations_override_shadow_without_normalizing_unrelated_fields() -> None:
    raw: dict[str, Any] = {
        "$type": "request",
        "createdAt": "2026-09-16T00:00:00.123456789Z",
        "responseSchema": {"properties": {}},
        "future": True,
        "messages": [
            {
                "role": "user",
                "createdAt": "2026-09-16T00:00:00.123456789Z",
                "contents": [
                    {"$type": "text", "text": "before", "future": [None]},
                    {"$type": "functionResult", "callId": "c", "result": False},
                    {"$type": "error", "details": {"nested": [1]}},
                ],
            }
        ],
    }
    entry = _entry(raw)
    entry.messages[0].contents[0].text = "after"
    entry.messages[0].contents[1].result = 0
    entry.messages[0].contents[2].details["nested"].append(2)
    entry.response_schema["properties"]["n"] = {"type": "number"}
    entry.unknown_fields["future"] = 1
    expected = deepcopy(raw)
    expected["messages"][0]["contents"][0]["text"] = "after"
    expected["messages"][0]["contents"][1]["result"] = 0
    expected["messages"][0]["contents"][2]["details"]["nested"] = [1, 2]
    expected["responseSchema"]["properties"]["n"] = {"type": "number"}
    expected["future"] = 1
    encoded = entry.to_dict()
    assert encoded == expected and type(encoded["future"]) is int
    assert type(encoded["messages"][0]["contents"][1]["result"]) is int
    _validate([encoded])
    entry.created_at = datetime(2026, 9, 17, tzinfo=timezone.utc)
    assert entry.to_dict()["createdAt"] == "2026-09-17T00:00:00+00:00"
    assert raw["responseSchema"] == {"properties": {}}


@pytest.mark.parametrize(
    "payload", [None, False, 0, "", [], {}, {"type": "text", "text": "inert", "$runtimeType": "X"}]
)
def test_unknown_wrapper_is_opaque_without_recognized_python_profile(payload: Any) -> None:
    raw = {"role": "assistant", "contents": [{"$type": "unknown", "content": payload}]}
    message = DurableAgentStateMessage.from_dict(raw)
    restored = message.to_chat_message().contents[0]
    assert restored.type == "unknown"
    assert restored.additional_properties["content"] == payload
    assert message.to_dict() == raw


@pytest.mark.parametrize("foreign", [None, False, 1, "old-public-id", {}, []])
def test_old_identity_sibling_is_not_interpreted(foreign: Any) -> None:
    raw = {"role": "user", "messageId": "public", "originalMessageId": foreign}
    message = DurableAgentStateMessage.from_dict(raw)
    assert message.public_message_id == message.message_id == "public"
    assert message.to_chat_message().message_id == "public"
    assert message.to_dict() == raw


def test_public_wire_id_and_profiled_internal_reconciliation_id() -> None:
    stored = DurableAgentStateMessage.from_chat_message(Message("user", ["text"], message_id="public"))
    stored.original_message_id = "public"
    stored.message_id = "private-occurrence"
    wire = stored.to_dict()
    assert wire["messageId"] == "public" and "originalMessageId" not in wire
    assert wire["pythonHistoryId"] == "private-occurrence"
    assert wire["pythonHistoryIdentity"] == HISTORY_PROFILE
    _validate([{"$type": "request", "messages": [wire]}])
    loaded = DurableAgentStateMessage.from_dict(wire)
    assert loaded.message_id == "private-occurrence" and loaded.public_message_id == "public"
    assert loaded.to_chat_message().message_id == "public" and loaded.to_dict() == wire


@pytest.mark.parametrize(
    "metadata",
    [
        {},
        {"pythonHistoryId": "inert"},
        {"pythonHistoryId": None, "pythonHistoryIdentity": {"profile": "foreign", "version": 1}},
        {"pythonHistoryId": "inert", "pythonHistoryIdentity": {**HISTORY_PROFILE, "version": True}},
        {"pythonHistoryId": "inert", "pythonHistoryIdentity": {**HISTORY_PROFILE, "version": 2}},
    ],
)
def test_foreign_identity_profiles_are_not_validated_or_activated(metadata: dict[str, Any]) -> None:
    raw = {"role": "user", "messageId": "public", **deepcopy(metadata)}
    message = DurableAgentStateMessage.from_dict(raw)
    assert message.message_id == message.public_message_id == "public"
    assert message.to_dict() == raw


@pytest.mark.parametrize("history_id", [None, False, 0, "", " \t", [], {}])
def test_recognized_identity_profile_rejects_malformed_internal_id(history_id: Any) -> None:
    raw = {
        "role": "user",
        "messageId": "public",
        "pythonHistoryId": deepcopy(history_id),
        "pythonHistoryIdentity": deepcopy(HISTORY_PROFILE),
    }
    before = deepcopy(raw)
    with pytest.raises(ValueError, match="pythonHistoryId"):
        DurableAgentStateMessage.from_dict(raw)
    assert raw == before


def test_recognized_identity_profile_requires_internal_id() -> None:
    raw = {"role": "user", "messageId": "public", "pythonHistoryIdentity": deepcopy(HISTORY_PROFILE)}
    before = deepcopy(raw)
    with pytest.raises(ValueError, match="pythonHistoryId"):
        DurableAgentStateMessage.from_dict(raw)
    assert raw == before


def test_recognized_identity_profile_keeps_additive_unknown_fields_on_roundtrip() -> None:
    raw = {
        "role": "user",
        "messageId": "public",
        "pythonHistoryId": "private-occurrence",
        "pythonHistoryIdentity": {**HISTORY_PROFILE, "future": {"opaque": [None, False, {}]}},
    }
    before = deepcopy(raw)
    message = DurableAgentStateMessage.from_dict(raw)
    assert message.message_id == "private-occurrence"
    assert message.public_message_id == message.to_chat_message().message_id == "public"
    wire = message.to_dict()
    assert wire == before and raw == before
    _validate([{"$type": "request", "messages": [wire]}])
    assert DurableAgentStateMessage.from_dict(wire).to_dict() == before
    wire["pythonHistoryIdentity"]["future"]["opaque"].append("consumer-only")
    assert message.to_dict() == raw == before


@pytest.mark.parametrize("kind", get_args(get_type_hints(Content.__init__)["type"]))
def test_new_core_content_is_canonical_typed_or_explicit_unknown(kind: str) -> None:
    fields: dict[str, dict[str, Any]] = {
        "data": {"uri": "data:application/octet-stream;base64,AQID"},
        "function_call": {"call_id": "c", "name": "f"},
        "function_result": {"call_id": "c"},
        "hosted_file": {"file_id": "f"},
        "hosted_vector_store": {"vector_store_id": "v"},
        "usage": {"usage_details": {}},
        "text": {"text": ""},
        "uri": {"uri": "urn:test"},
    }
    content = Content(cast(Any, kind), **fields.get(kind, {}))
    stored = DurableAgentStateContent.from_ai_content(content)
    wire = stored.to_persisted_dict()
    _validate([{"$type": "request", "messages": [{"role": "user", "contents": [wire]}]}])
    restored = DurableAgentStateMessage.from_dict({"role": "user", "contents": [wire]}).to_chat_message()
    assert restored.contents[0].type == kind


@pytest.mark.parametrize("text", [None, 1, False, {}, []])
def test_new_text_must_be_a_string(text: Any) -> None:
    with pytest.raises(ValueError, match="text string"):
        DurableAgentStateTextContent(text).to_persisted_dict()


def test_low_level_legacy_unknown_entry_remains_opaque() -> None:
    raw = {"$type": "future-entry", "messages": "not interpreted", "future": None}
    entry = _parse_history_entries({"conversationHistory": [raw]})[0]
    assert entry.messages == [] and entry.to_dict() == raw


@pytest.mark.parametrize(
    ("kind", "field", "required"),
    [
        ("error", "details", {}),
        ("functionResult", "result", {"callId": "c"}),
    ],
)
@pytest.mark.parametrize("present", [False, True])
def test_nullable_presence_and_current_value_mutation(kind: str, field: str, required: dict, present: bool) -> None:
    raw = {"$type": kind, **required, **({field: None} if present else {})}
    message = DurableAgentStateMessage.from_dict({"role": "tool", "contents": [raw]})
    stored = message.contents[0]
    assert stored.to_persisted_dict() == raw
    setattr(stored, field, {"nested": []})
    assert stored.to_persisted_dict() == {**raw, field: {"nested": []}}
    _validate([{"$type": "response", "messages": [message.to_dict()]}])


def test_typed_field_and_unknown_sibling_removal_overrides_raw() -> None:
    raw: dict[str, Any] = {
        "role": "user",
        "authorName": "writer",
        "future": [None],
        "contents": [
            {"$type": "uri", "uri": "urn:test", "mediaType": "image/png", "future": False},
        ],
    }
    message = DurableAgentStateMessage.from_dict(raw)
    message.author_name = None
    message.unknown_fields.clear()
    content = message.contents[0]
    assert isinstance(content, DurableAgentStateUriContent) and content.unknown_fields is not None
    content.media_type = None
    content.unknown_fields.clear()
    assert message.to_dict() == {"role": "user", "contents": [{"$type": "uri", "uri": "urn:test"}]}
    message.contents.clear()
    assert message.to_dict() == {"role": "user", "contents": []}


def test_duplicate_public_ids_keep_unique_reconciliation_across_reload() -> None:
    history = [
        _entry({
            "$type": "request",
            "correlationId": "turn",
            "messages": [
                {"role": "user", "messageId": "same", "contents": [{"$type": "text", "text": "equal"}]},
                {"role": "user", "messageId": "same", "contents": [{"$type": "text", "text": "equal"}]},
            ],
        })
    ]
    provider = DurableHistoryProvider()
    state_provider = SimpleNamespace(
        state=SimpleNamespace(schema_version="2.0.0", data=SimpleNamespace(conversation_history=history))
    )
    binding = DurableHistoryBinding(cast(Any, state_provider))
    positions = provider._positions(binding)
    assert len(positions) == 2
    wire = [entry.to_dict() for entry in history]
    assert [message["messageId"] for message in wire[0]["messages"]] == ["same", "same"]
    assert wire[0]["messages"][1]["pythonHistoryId"] != "same"
    cold = _parse_history_entries({"conversationHistory": wire})
    binding.state_provider.state.data.conversation_history = cold
    assert list(provider._positions(binding)) == list(positions)
    assert [message.to_chat_message().message_id for message in cold[0].messages] == ["same", "same"]
    assert [entry.to_dict() for entry in cold] == wire


@pytest.mark.parametrize("field", ["input_token_count", "output_token_count", "total_token_count"])
@pytest.mark.parametrize("value", [None, False, 0.0, 1.5, "provider-value", [], {"value": None}])
def test_noninteger_provider_usage_remains_explicit_detached_metadata(field: str, value: Any) -> None:
    source = {field: deepcopy(value), "provider": {"labels": [False, None]}}
    before = deepcopy(source)
    usage = DurableAgentStateUsage.from_usage(source)
    assert usage is not None
    wire = usage.to_dict()
    assert wire == {"extensionData": before}
    assert usage.to_usage_details() == before
    content = DurableAgentStateContent.from_ai_content(Content.from_usage(cast(Any, source)))
    message = {"role": "assistant", "contents": [content.to_persisted_dict()]}
    _validate([{"$type": "response", "messages": [message]}])
    projected = DurableAgentStateMessage.from_dict(message).to_chat_message().contents[0]
    assert json.dumps(projected.usage_details, sort_keys=True) == json.dumps(before, sort_keys=True)
    source["provider"]["labels"].append("source mutation")
    assert usage.to_dict() == wire


@pytest.mark.parametrize("field", ["input", "output", "total"])
def test_typed_shared_usage_counts_override_metadata_only_in_projection(field: str) -> None:
    raw = {f"{field}TokenCount": 7, "extensionData": {f"{field}_token_count": 99, "provider": None}}
    usage = DurableAgentStateUsage.from_dict(raw)
    assert usage.to_usage_details() == {f"{field}_token_count": 7, "provider": None}
    assert usage.to_dict() == raw
    message = {"role": "assistant", "contents": [{"$type": "usage", "usage": raw}]}
    projected = DurableAgentStateMessage.from_dict(message).to_chat_message().contents[0]
    assert projected.usage_details == {f"{field}_token_count": 7, "provider": None}


def test_response_projection_keeps_absent_and_precise_timestamp() -> None:
    for fields in ({}, {"createdAt": "2026-09-16T00:00:00.123456789Z"}):
        entry = DurableAgentStateResponse.from_dict({"$type": "response", **fields})
        assert DurableAgentStateResponse.to_run_response(entry).created_at == fields.get("createdAt")
