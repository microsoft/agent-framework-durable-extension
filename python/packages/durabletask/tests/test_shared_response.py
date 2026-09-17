# Copyright (c) Microsoft. All rights reserved.

"""Focused shared response codec contract tests, independent of entity-state integration."""

import base64
import importlib
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast, get_args, get_type_hints
from unittest.mock import Mock

import pytest
from agent_framework import AgentResponse, Content, Message
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import BaseModel, Field

from agent_framework_durabletask import _shared_response as codec
from agent_framework_durabletask._response_utils import (
    _constructor_fields,
    load_agent_response,
    preserve_input_envelope,
    serialize_agent_response,
)
from agent_framework_durabletask._shared_response import (
    load_terminal_response,
    serialize_terminal_response,
    terminal_error,
)

SCHEMAS = Path(__file__).resolve().parents[4] / "schemas"
SCHEMA = json.loads((SCHEMAS / "durable-agent-entity-state.json").read_text(encoding="utf-8"))
PROFILE = {"profile": "agent-framework-python.continuation", "version": 1, "format": "json"}
CONTENT_PROFILE = {"profile": "agent-framework-python.content", "version": 1}
CONTENT_CASES: list[tuple[str, dict[str, Any]]] = [
    ("data", {"uri": "data:image/png;base64,AQID", "mediaType": "image/png"}),
    ("error", {"message": "failure", "errorCode": "test", "details": {"future": [None, False]}}),
    ("functionCall", {"callId": "call", "name": "tool", "arguments": ' { "unfinished": '}),
    ("functionResult", {"callId": "call", "result": None}),
    ("hostedFile", {"fileId": "file"}),
    ("hostedVectorStore", {"vectorStoreId": "vector"}),
    ("usage", {"usage": {"inputTokenCount": 2**64, "extensionData": {"provider": [None, 1.25]}}}),
    ("text", {"text": ""}),
    ("reasoning", {}),
    ("uri", {"uri": "https://example.test/no-inferred-media-type"}),
    ("unknown", {"content": {"$type": "functionCall", "$runtimeType": "untrusted.Type", "extra": None}}),
]


def _response_with(content: dict[str, Any]) -> dict[str, Any]:
    return {"messages": [{"role": "assistant", "contents": [content]}]}


def _core_with(content: dict[str, Any]) -> dict[str, Any]:
    return {"type": "agent_response", "messages": [{"role": "assistant", "contents": [content]}]}


def _validate(response: dict[str, Any]) -> None:
    envelope = {
        "schemaVersion": "2.0.0",
        "data": {
            "conversationHistory": [],
            "terminalResults": {
                "test": {
                    "correlationId": "test",
                    "outcome": "succeeded",
                    "completedAt": "2026-09-16T00:00:00Z",
                    "response": response,
                }
            },
            "completionReceipts": {
                "test": {
                    "correlationId": "test",
                    "outcome": "succeeded",
                    "completedAt": "2026-09-16T00:00:00Z",
                    "resultState": "available",
                }
            },
        },
    }
    Draft202012Validator(SCHEMA, format_checker=FormatChecker()).validate(envelope)


def _roundtrip(payload: dict[str, Any]) -> AgentResponse[Any]:
    before = deepcopy(payload)
    response = load_terminal_response(payload)
    encoded = serialize_terminal_response(response)
    _validate(encoded)
    assert json.dumps(encoded, sort_keys=True, allow_nan=False) == json.dumps(before, sort_keys=True, allow_nan=False)
    assert json.dumps(payload, sort_keys=True, allow_nan=False) == json.dumps(before, sort_keys=True, allow_nan=False)
    return response


def _assert_instance_matches_core_snapshot(response: AgentResponse[Any]) -> dict[str, Any]:
    # Whole-body, type-sensitive equality is the oracle, not selected projected fields.
    # A lazy format is tested separately because only the parent resolves it.
    before = _observed_state(response)
    canonical = serialize_agent_response(response)
    expected = serialize_terminal_response(canonical)
    actual = serialize_terminal_response(response)
    assert json.dumps(actual, sort_keys=True, allow_nan=False) == json.dumps(expected, sort_keys=True, allow_nan=False)
    assert _observed_state(response) == before
    _roundtrip(actual)
    return actual


def _observed_state(value: Any) -> Any:
    # Message has identity equality. Compare attributes recursively, not copied objects.
    if isinstance(value, (AgentResponse, Message, Content)):
        return type(value), _observed_state(vars(value))
    if isinstance(value, dict):
        return {name: _observed_state(item) for name, item in value.items()}
    if isinstance(value, list):
        return [_observed_state(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_observed_state(item) for item in value)
    return deepcopy(value)


def test_content_case_list_covers_every_shared_discriminator() -> None:
    def discriminator(reference: dict[str, Any]) -> str:
        definition = SCHEMA["$defs"][reference["$ref"].rsplit("/", 1)[-1]]
        return discriminator(definition) if "$ref" in definition else definition["properties"]["$type"]["const"]

    assert {kind for kind, _ in CONTENT_CASES} == {
        discriminator(reference) for reference in SCHEMA["$defs"]["v2ChatContentItem"]["oneOf"]
    }


@pytest.mark.parametrize(("kind", "fields"), CONTENT_CASES)
def test_every_shared_content_kind_roundtrips_unknowns_at_original_locations(kind: str, fields: dict[str, Any]) -> None:
    payload = _response_with({"$type": kind, **fields, "future": {"nested": [False, None, 2**65]}})
    payload["messages"][0].update({
        "authorName": "",
        "messageId": "",
        "createdAt": "2026-09-16T00:00:00.123456789Z",
        "extensionData": {"future": "explicit-message"},
        "future": {"nested": 1},
    })
    payload.update({
        "extensionData": {"future": "explicit-response"},
        "future": [None],
        "usage": {"inputTokenCount": 2**65, "extensionData": {"input_token_count": "collision"}, "future": 1.25},
    })
    _roundtrip(payload)


@pytest.mark.parametrize("fixture", sorted(SCHEMAS.glob("fixtures/shared-durable-agent-state-2.0*.json")))
def test_canonical_interoperability_fixtures(fixture: Path) -> None:
    data = json.loads(fixture.read_text(encoding="utf-8"))
    for result in data["data"]["terminalResults"].values():
        _roundtrip(result["response"])


@pytest.mark.parametrize("value", [None, False, 0, -0.0, "", [], {}, {"nested": [2**65, 1.25, None]}])
def test_explicit_values_do_not_come_from_message_text(value: Any) -> None:
    payload = {**_response_with({"$type": "text", "text": "42"}), "value": value}
    response = _roundtrip(payload)
    assert response.value == value
    assert type(response.value) is type(value)
    core = {**_core_with({"type": "text", "text": "42"}), "value": value}
    produced = serialize_terminal_response(core)
    _validate(produced)
    assert "value" in produced and type(produced["value"]) is type(value)


def test_absent_value_does_not_trigger_lazy_text_parsing() -> None:
    response = AgentResponse(messages=[Message("assistant", ["42"])], response_format={"type": "integer"})
    before = dict(vars(response))
    assert "value" not in serialize_terminal_response(response)
    assert vars(response) == before
    assert "value" not in serialize_terminal_response(_roundtrip({"messages": []}))


def test_core_typed_fields_extras_and_collisions_are_separate() -> None:
    source = {
        **_core_with({
            "type": "function_call",
            "call_id": "c",
            "name": "tool",
            "arguments": "  { unfinished ",
            "annotations": [{"future": None}],
            "callId": "inert-collision",
            "additional_properties": {"coreContent": "provider", "arguments": "not-the-arguments"},
        }),
        "response_id": "real",
        "responseId": "extra",
        "pythonCoreFields": {"provider": True},
        "additional_properties": {"responseId": "metadata", "pythonCoreFields": "metadata"},
    }
    source["messages"][0].update({"author_name": "writer", "authorName": "extra", "message_id": "message"})
    before = deepcopy(source)
    shared = serialize_terminal_response(source)
    _validate(shared)
    assert shared["responseId"] == "real"
    assert shared["extensionData"] == source["additional_properties"]
    assert shared["pythonCoreFields"]["fields"]["responseId"] == "extra"
    assert shared["pythonCoreFields"]["fields"]["pythonCoreFields"] == {"provider": True}
    loaded = _roundtrip(shared)
    content = loaded.messages[0].contents[0]
    assert content.call_id == "c"
    assert content.arguments == "  { unfinished "
    assert content.annotations == [{"future": None}]
    assert content.additional_properties == source["messages"][0]["contents"][0]["additional_properties"]
    assert source == before


def test_core_response_instance_preserves_canonical_fields_without_inventing_null_result() -> None:
    response = AgentResponse[Any](
        messages=[Message("developer", [Content("function_result", call_id="call", result=None)])],
        created_at="2026-09-16T00:00:00+00:00",
        response_id="response",
        agent_id="agent",
        finish_reason="stop",
        usage_details={"input_token_count": 0, "output_token_count": 2**64},
        value=False,
    )
    shared = _assert_instance_matches_core_snapshot(response)
    _validate(shared)
    assert shared["createdAt"] == "2026-09-16T00:00:00+00:00"
    canonical_content = serialize_agent_response(response)["messages"][0]["contents"][0]
    assert ("result" in shared["messages"][0]["contents"][0]) == ("result" in canonical_content)
    assert shared["usage"] == {"inputTokenCount": 0, "outputTokenCount": 2**64}
    assert shared["value"] is False
    _roundtrip(shared)


def test_core_arbitrary_usage_metadata_is_not_coerced_or_dropped() -> None:
    usage = {"input_token_count": None, "output_token_count": 2, "provider": {"n": 2**64}, "fraction": 0.25}
    shared = serialize_terminal_response({"type": "agent_response", "messages": [], "usage_details": usage})
    _validate(shared)
    expected_metadata = {key: value for key, value in usage.items() if key != "output_token_count"}
    assert shared["usage"]["extensionData"] == expected_metadata
    assert load_terminal_response(shared).usage_details == usage


@pytest.mark.parametrize("kind", get_args(get_type_hints(Content.__init__)["type"]))
def test_every_public_core_kind_is_typed_or_explicitly_wrapped(kind: str) -> None:
    minimal: dict[str, dict[str, Any]] = {
        "data": {"uri": "data:application/octet-stream;base64,AQID"},
        "function_call": {"call_id": "c", "name": "f"},
        "function_result": {"call_id": "c"},
        "hosted_file": {"file_id": "f"},
        "hosted_vector_store": {"vector_store_id": "v"},
        "usage": {"usage_details": {}},
        "text": {"text": ""},
        "uri": {"uri": "urn:test"},
    }
    source = {"type": kind, **minimal.get(kind, {}), "future": {"$runtimeType": "untrusted.Type", "null": None}}
    shared = serialize_terminal_response(_core_with(source))
    _validate(shared)
    content = shared["messages"][0]["contents"][0]
    if content["$type"] == "unknown":
        assert content["content"] == source
        assert content["pythonContentEncoding"] == CONTENT_PROFILE
    else:
        assert content["pythonCoreFields"]["fields"]["future"] == source["future"]
    assert _roundtrip(shared).messages[0].contents[0].type == kind


@pytest.mark.parametrize("opaque", [None, False, 0, "", [], {"type": "text", "$runtimeType": "untrusted.Type"}])
def test_unknown_payload_does_not_select_a_runtime_type(opaque: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    forbidden = Mock(side_effect=AssertionError("No dynamic imports"))
    monkeypatch.setattr(importlib, "import_module", forbidden)
    response = _roundtrip(_response_with({"$type": "unknown", "content": opaque}))
    content = response.messages[0].contents[0]
    assert type(response) is AgentResponse and type(content) is Content
    assert content.type == "unknown" and content.additional_properties["content"] == opaque
    forbidden.assert_not_called()


def test_python_json_continuation_has_an_identified_versioned_profile() -> None:
    token = {"provider": "test", "resume": {"$runtimeType": "inert", "offset": 0, "unicode": "é"}}
    shared = serialize_terminal_response({"type": "agent_response", "messages": [], "continuation_token": token})
    _validate(shared)
    assert shared["pythonContinuationEncoding"] == PROFILE
    assert json.loads(base64.b64decode(shared["continuationToken"])) == token
    loaded = load_terminal_response(shared, require_resumable_continuation=True)
    assert loaded.continuation_token == token
    assert serialize_terminal_response(loaded) == shared


@pytest.mark.parametrize("opaque", [b"\x00\xff\x01", b'{"looks":"like-json"}', b""])
@pytest.mark.parametrize(
    "profile",
    [
        None,
        {},
        {"version": 1, "format": "json"},
        {**PROFILE, "version": True},
        {**PROFILE, "version": 2},
        {**PROFILE, "format": "other"},
        {**PROFILE, "profile": "foreign"},
    ],
)
def test_foreign_tokens_are_preserved_but_never_parsed_or_treated_as_resumable(opaque: bytes, profile: Any) -> None:
    payload = {"messages": [], "continuationToken": base64.b64encode(opaque).decode("ascii")}
    if profile is not None:
        payload["pythonContinuationEncoding"] = profile
    loaded = _roundtrip(payload)
    assert loaded.continuation_token is None
    assert vars(loaded)["_opaque_shared_continuation_token"] == opaque
    with pytest.raises(ValueError, match="Cannot resume"):
        load_terminal_response(payload, require_resumable_continuation=True)


@pytest.mark.parametrize("token", [b"not JSON", b"\xff", b"null", b"[]", b'{"n":NaN}', b'{"a":1,"a":2}'])
def test_recognized_profile_rejects_invalid_json_dictionaries(token: bytes) -> None:
    payload = {
        "messages": [],
        "continuationToken": base64.b64encode(token).decode("ascii"),
        "pythonContinuationEncoding": PROFILE,
    }
    codec.validate_terminal_response(payload)
    with pytest.raises(ValueError, match="recognized Python continuation"):
        load_terminal_response(payload)


@pytest.mark.parametrize("token", [b"bytes", "text", [1], object(), None, {"bad": object()}])
def test_non_dictionary_core_tokens_fail_instead_of_becoming_fake_resumable_tokens(token: Any) -> None:
    with pytest.raises(ValueError):
        serialize_terminal_response({"type": "agent_response", "messages": [], "continuation_token": token})


@pytest.mark.parametrize(
    "patch",
    [
        {"messages": None},
        {"messages": {}},
        {"value": float("nan")},
        {"responseId": ""},
        {"agentId": "\x00"},
        {"finishReason": "x" * 257},
        {"extensionData": {"": 1}},
        {"extensionData": []},
        {"createdAt": "2026-99-99T00:00:00Z"},
        {"usage": {"inputTokenCount": None}},
        {"usage": {"totalTokenCount": True}},
        {"usage": {"outputTokenCount": 1.5}},
        {"usage": {"extensionData": None}},
        {"continuationToken": "!!!!"},
        {"continuationToken": "AQID\n"},
        {"continuationToken": "A" * 16388},
    ],
)
def test_malformed_known_response_fields_are_rejected(patch: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        load_terminal_response({"messages": [], **patch})


@pytest.mark.parametrize(
    "content",
    [
        {"$type": "future"},
        {"$type": "unknown"},
        {"$type": "text", "text": None},
        {"$type": "data"},
        {"$type": "uri", "uri": "urn:test", "mediaType": None},
        {"$type": "functionCall", "callId": "c", "name": "f", "arguments": None},
        {"$type": "functionCall", "callId": 1, "name": "f"},
        {"$type": "usage", "usage": []},
    ],
)
def test_malformed_known_content_is_not_hidden_in_unknown_wrappers(content: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        load_terminal_response(_response_with(content))


@pytest.mark.parametrize(
    "content",
    [
        {"type": "text", "text": None},
        {"type": "function_call", "call_id": "c", "name": "f", "arguments": []},
        {"type": "uri", "uri": 1},
        {"type": "hosted_file"},
    ],
)
def test_malformed_known_core_fields_cannot_escape_through_metadata(content: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        serialize_terminal_response(_core_with(content))


@pytest.mark.parametrize("value", [object(), (1, 2), {1: "key"}, float("inf"), {"nested": object()}])
def test_non_json_content_extras_fail_even_when_core_would_skip_them(value: Any) -> None:
    content = Content.from_text("text", additional_properties={"x": value})
    response = AgentResponse(messages=[Message("assistant", [content])])
    with pytest.raises(ValueError):
        serialize_terminal_response(response)


@pytest.mark.parametrize("location", ["response", "message", "content", "nested_content"])
@pytest.mark.parametrize("value", [object(), (1, 2), {1: "key"}, float("nan"), {"nested": object()}])
def test_runtime_envelope_audit_rejects_non_json_extras_before_core_can_skip_them(location: str, value: Any) -> None:
    leaf = Content.from_text("text")
    content = Content("function_result", call_id="call", items=[leaf])
    message = Message("assistant", [content])
    response = AgentResponse(messages=[message])
    target = {"response": response, "message": message, "content": content, "nested_content": leaf}[location]
    vars(target)["future"] = value
    with pytest.raises(ValueError, match="JSON"):
        serialize_terminal_response(response)


def test_cyclic_json_is_rejected_without_stringification() -> None:
    cyclic: dict[str, Any] = {}
    cyclic["self"] = cyclic
    with pytest.raises(ValueError):
        serialize_terminal_response({"type": "agent_response", "messages": [], "value": cyclic})


def test_unchanged_shadows_are_detached_and_changed_projections_fail_closed() -> None:
    payload: dict[str, Any] = {"messages": [], "value": {"n": False}, "future": [None]}
    loaded: AgentResponse[Any] = load_terminal_response(payload)
    output = serialize_terminal_response(loaded)
    output["future"].append(2)
    payload["future"].append(3)
    assert serialize_terminal_response(loaded)["future"] == [None]
    value = loaded.value
    assert isinstance(value, dict)
    value["n"] = 0
    with pytest.raises(ValueError, match="modified shared response"):
        serialize_terminal_response(loaded)


def test_core_field_profile_cannot_override_known_response_or_content_fields() -> None:
    profile = {"profile": "agent-framework-python.core-fields", "version": 1, "fields": {"response_id": "spoof"}}
    with pytest.raises(ValueError, match="cannot override"):
        load_terminal_response({"messages": [], "pythonCoreFields": profile})
    profile["fields"] = {"text": "spoof"}
    with pytest.raises(ValueError, match="cannot override"):
        load_terminal_response(_response_with({"$type": "text", "text": "real", "pythonCoreFields": profile}))


@pytest.mark.parametrize("location", ["response", "message", "content"])
@pytest.mark.parametrize(
    "profile",
    [
        None,
        False,
        [],
        "future encoding",
        {},
        {"version": 1, "fields": None},
        {"profile": "foreign", "version": 1, "fields": {"role": "spoof", "text": "spoof"}},
        {"profile": "agent-framework-python.core-fields", "version": 2, "fields": None},
        {"profile": "agent-framework-python.core-fields", "version": True, "fields": None},
        {"profile": "agent-framework-python.core-fields", "version": 1.0, "fields": None},
    ],
)
def test_foreign_core_profiles_remain_inert_and_roundtrip_at_their_original_level(location: str, profile: Any) -> None:
    payload = _response_with({"$type": "text", "text": "real"})
    target = payload if location == "response" else payload["messages"][0]
    if location == "content":
        target = target["contents"][0]
    target["pythonCoreFields"] = profile
    codec.validate_terminal_response(payload)
    response = _roundtrip(payload)
    assert response.messages[0].role == "assistant"
    assert response.messages[0].contents[0].text == "real"


@pytest.mark.parametrize("location", ["response", "message", "content"])
@pytest.mark.parametrize("fields", [None, [], False, "invalid", {"type": "spoof"}, {"raw_representation": {}}])
def test_recognized_malformed_core_profiles_fail_projection_not_wire_validation(location: str, fields: Any) -> None:
    payload = _response_with({"$type": "text", "text": "real"})
    target = payload if location == "response" else payload["messages"][0]
    if location == "content":
        target = target["contents"][0]
    target["pythonCoreFields"] = {"profile": "agent-framework-python.core-fields", "version": 1, "fields": fields}
    before = deepcopy(payload)
    codec.validate_terminal_response(payload)
    with pytest.raises(ValueError):
        load_terminal_response(payload)
    assert payload == before


def test_recognized_annotations_and_unprojected_message_time_keep_the_exact_shared_body() -> None:
    annotations = [{"type": "citation", "title": "", "start_index": 0, "future": [None, False]}]
    payload = _response_with({
        "$type": "text",
        "text": "partial answer",
        "pythonCoreFields": {
            "profile": "agent-framework-python.core-fields",
            "version": 1,
            "fields": {"annotations": annotations, "future": None, "fields": {"text": False}},
        },
        "future": {"type": "future", "value": -0.0},
    })
    payload["messages"][0].update({
        "createdAt": "2026-09-16T00:00:00.123456789+03:00",
        "future": False,
        "extensionData": {"createdAt": "metadata only"},
        "pythonCoreFields": {
            "profile": "agent-framework-python.core-fields",
            "version": 1,
            "fields": {"fields": {"createdAt": None}},
        },
    })
    payload["extensionData"] = {"durable_outcome": "failed"}
    response = _roundtrip(payload)
    assert response.messages[0].contents[0].annotations == annotations
    assert response.additional_properties["durable_outcome"] == "failed"
    # Editing a projected annotation is not an unchanged shadow re-encode.
    projected_annotations = response.messages[0].contents[0].annotations
    assert projected_annotations is not None
    projected_annotations[0]["title"] = "modified"
    with pytest.raises(ValueError, match="modified shared response"):
        serialize_terminal_response(response)


def test_shared_dictionary_is_not_mistaken_for_a_core_snapshot() -> None:
    with pytest.raises(ValueError, match="canonical Core"):
        serialize_terminal_response({"messages": []})
    response = load_agent_response({"type": "agent_response", "messages": [], "value": None})
    assert serialize_terminal_response(response)["value"] is None


@pytest.mark.parametrize(
    ("code", "message"),
    [(None, None), ("\x00  ", "\x00  "), ("x" * 300, "y" * 17000), ("provider\nerror", "bounded\nmessage")],
)
def test_terminal_error_is_bounded_text_only(code: Any, message: Any) -> None:
    response = AgentResponse(
        messages=[
            Message("tool", [Content.from_error(error_code="ignored", message="tool")]),
            Message(
                "assistant", [Content.from_error(error_code=code, message=message, error_details="private traceback")]
            ),
        ]
    )
    error = terminal_error(response)
    validator = Draft202012Validator({"$defs": SCHEMA["$defs"], "$ref": "#/$defs/terminalError"})
    validator.validate(error)
    assert set(error) == {"code", "message"}
    assert error["code"] != "ignored"
    assert "private traceback" not in json.dumps(error)


def test_terminal_error_without_error_content_has_a_valid_generic_fallback() -> None:
    assert terminal_error(AgentResponse()) == {"code": "agent_error", "message": "The agent invocation failed."}


@pytest.mark.parametrize("role", SCHEMA["$defs"]["v2ChatMessage"]["properties"]["role"]["enum"])
@pytest.mark.parametrize("fields", [{}, {"contents": [], "extensionData": {}}])
def test_all_shared_roles_preserve_absent_and_empty_message_fields(role: str, fields: dict[str, Any]) -> None:
    _roundtrip({"messages": [{"role": role, **fields}]})


@pytest.mark.parametrize("role", [None, "future", 1, {}])
def test_invalid_roles_are_rejected_on_both_boundaries(role: Any) -> None:
    with pytest.raises(ValueError):
        load_terminal_response({"messages": [{"role": role}]})
    with pytest.raises(ValueError):
        serialize_terminal_response({"type": "agent_response", "messages": [{"role": role}]})


@pytest.mark.parametrize("value", [None, False, 0, "", [], {}, {"type": "text", "not_content": [None, 2**64]}])
def test_function_results_remain_opaque_present_values(value: Any) -> None:
    source = _core_with({"type": "function_result", "call_id": "c", "result": value})
    shared = serialize_terminal_response(source)
    result = shared["messages"][0]["contents"][0]
    assert "result" in result and type(result["result"]) is type(value)
    assert _roundtrip(shared).messages[0].contents[0].result == value


@pytest.mark.parametrize("extension", [None, [], False, 0, "metadata", {"future": None}])
def test_content_extension_data_is_not_assumed_to_be_a_runtime_metadata_object(extension: Any) -> None:
    _roundtrip(_response_with({"$type": "text", "text": "", "extensionData": extension}))


def test_unattached_public_extras_follow_canonical_core_presence() -> None:
    content = Content.from_text("text")
    vars(content)["future"] = None
    message = Message("assistant", [content])
    vars(message)["future"] = None
    response = AgentResponse(messages=[message])
    vars(response)["future"] = None
    _assert_instance_matches_core_snapshot(response)


@pytest.mark.parametrize("value", [None, False, 0, -0.0, "", [], {}, {"future": [None, False]}])
def test_instance_function_result_presence_matches_core_snapshot(value: Any) -> None:
    content = Content("function_result", call_id="call", result=value)
    response = AgentResponse(messages=[Message("assistant", [content])])
    _assert_instance_matches_core_snapshot(response)


@pytest.mark.parametrize("value", [None, False, 0, -0.0, "", [], {}, {"future": [None, False]}])
def test_attached_core_input_preserves_explicit_presence_and_inert_extras(value: Any) -> None:
    raw: dict[str, Any] = {
        "type": "chat_message",
        "role": "assistant",
        "author_name": None,
        "created_at": "2026-09-16T00:00:00.123456789Z",
        "future": value,
        "contents": [
            {
                "type": "function_result",
                "call_id": "call",
                "result": value,
                "annotations": [],
                "future": value,
                "fields": {"text": "inert"},
                "pythonCoreFields": {"provider": value},
                "items": [{"type": "text", "text": "", "future": value}],
            }
        ],
    }
    response = load_agent_response({"type": "agent_response", "messages": [raw]})
    preserve_input_envelope(response.messages[0], raw)
    # The input envelope has its own presence contract. Shared authorName cannot be null.
    raw["author_name"] = "changed caller copy"
    response.messages[0].author_name = "writer"
    wire = serialize_terminal_response(response)
    message = wire["messages"][0]
    content = message["contents"][0]
    assert message["authorName"] == "writer"
    assert message["createdAt"] == "2026-09-16T00:00:00.123456789Z"
    assert message["pythonCoreFields"]["fields"]["future"] == value
    assert "result" in content and type(content["result"]) is type(value)
    extras = content["pythonCoreFields"]["fields"]
    assert "annotations" in extras and extras["annotations"] == []
    assert "future" in extras and type(extras["future"]) is type(value)
    assert extras["fields"] == {"text": "inert"}
    assert extras["pythonCoreFields"] == {"provider": value}
    assert extras["items"][0]["future"] == value
    _roundtrip(wire)


def test_unknown_core_content_with_an_inner_runtime_name_is_not_reinterpreted() -> None:
    opaque = {"$runtimeType": "untrusted.Type", "$type": "functionCall", "type": "text", "text": "not replay"}
    source = {"type": "unknown", "additional_properties": {"content": opaque}}
    shared = serialize_terminal_response(_core_with(source))
    loaded = _roundtrip(shared)
    content = loaded.messages[0].contents[0]
    assert content.type == "unknown"
    assert content.additional_properties["content"] == opaque
    assert content.text is None and content.function_call is None


def test_foreign_continuation_never_calls_the_json_decoder(monkeypatch: pytest.MonkeyPatch) -> None:
    forbidden = Mock(side_effect=AssertionError("Foreign token must not be JSON decoded"))
    monkeypatch.setattr(json, "loads", forbidden)
    raw = b'{"provider":"foreign"}'
    response = load_terminal_response({"messages": [], "continuationToken": base64.b64encode(raw).decode("ascii")})
    assert response.continuation_token is None
    assert vars(response)["_opaque_shared_continuation_token"] == raw
    forbidden.assert_not_called()


# Pin the fixtures against the installed Core literal, not against the codec's table.
# Each category supplies its functional fields, including falsy values and nested edges.
CALL = {"type": "function_call", "call_id": "call", "name": "tool", "arguments": {"count": 0}}
DATA = {"type": "data", "uri": "data:image/png;base64,AQID", "media_type": "image/png"}
SHELL_OUTPUT = {"type": "shell_command_output", "stdout": "", "stderr": "stderr", "exit_code": 0, "timed_out": False}
CORE_CATEGORIES: dict[str, dict[str, dict[str, Any]]] = {
    "text": {"text": {"text": ""}, "text_reasoning": {"text": "reason", "protected_data": "protected"}},
    "media": {"data": DATA, "uri": {"uri": "urn:asset", "media_type": "image/png"}},
    "diagnostics": {
        "error": {"message": "failure", "error_code": "test", "error_details": "details"},
        "usage": {"usage_details": {"input_token_count": 0, "total_token_count": 2**64, "provider": None}},
    },
    "function": {
        "function_call": {**CALL, "informational_only": True},
        "function_result": {"call_id": "call", "result": None, "exception": "failed", "items": [DATA]},
    },
    "hosted": {"hosted_file": {"file_id": "file"}, "hosted_vector_store": {"vector_store_id": "vector"}},
    "code": {
        "code_interpreter_tool_call": {"call_id": "code", "inputs": [{"type": "text", "text": "1+1"}, DATA]},
        "code_interpreter_tool_result": {"call_id": "code", "outputs": [DATA, {"type": "text", "text": "2"}]},
    },
    "image": {
        "image_generation_tool_call": {"call_id": "image", "inputs": [DATA]},
        "image_generation_tool_result": {"call_id": "image", "image_id": "image", "outputs": [DATA]},
    },
    "mcp": {
        "mcp_server_tool_call": {"call_id": "mcp", "tool_name": "remote", "server_name": "server", "arguments": CALL},
        "mcp_server_tool_result": {"call_id": "mcp", "output": {"type": "text", "text": "business"}},
    },
    "search": {
        "search_tool_call": {"call_id": "search", "arguments": "partial {"},
        "search_tool_result": {"call_id": "search", "output": [False, None, 0]},
    },
    "shell": {
        "shell_tool_call": {"call_id": "shell", "commands": ["echo test"], "timeout_ms": 0, "max_output_length": 0},
        "shell_tool_result": {"call_id": "shell", "status": "completed", "outputs": [SHELL_OUTPUT]},
        "shell_command_output": SHELL_OUTPUT,
    },
    "approval": {
        "function_approval_request": {"id": "approval", "function_call": CALL, "user_input_request": True},
        "function_approval_response": {"id": "approval", "function_call": CALL, "approved": False},
        "oauth_consent_request": {
            "id": "consent",
            "consent_link": "https://example.test/consent",
            "user_input_request": True,
        },
    },
}


@pytest.mark.parametrize("with_message", [False, True])
@pytest.mark.parametrize("with_content", [False, True])
def test_default_instances_have_exactly_the_canonical_core_wire_body(with_message: bool, with_content: bool) -> None:
    response = AgentResponse(
        messages=[Message("assistant", [Content("text", text="text")] if with_content else [])] if with_message else []
    )
    _assert_instance_matches_core_snapshot(response)


@pytest.mark.parametrize("annotations", [None, [], (), [{"type": "citation", "title": "", "start_index": 0}]])
def test_content_annotation_presence_follows_the_core_serializer(annotations: Any) -> None:
    _assert_instance_matches_core_snapshot(
        AgentResponse(messages=[Message("assistant", [Content.from_text("text", annotations=annotations)])])
    )


@pytest.mark.parametrize(
    ("kind", "fields"),
    [
        pytest.param(kind, fields, id=f"{category}-{kind}")
        for category, cases in CORE_CATEGORIES.items()
        for kind, fields in cases.items()
    ],
)
@pytest.mark.parametrize("with_provider_extras", [False, True])
def test_every_core_kind_instance_matches_canonical_snapshot(
    kind: str, fields: dict[str, Any], with_provider_extras: bool
) -> None:
    source = _core_with({**fields, "type": kind})
    if with_provider_extras:
        source["messages"][0]["contents"][0].update({
            "annotations": [{"type": "citation", "title": "", "future": [None, False]}],
            "additional_properties": {"fields": {}, "pythonCoreFields": None, "provider": {"value": False}},
        })
        source["messages"][0]["additional_properties"] = {"messageId": "provider", "fields": {"empty": []}}
        source["additional_properties"] = {"responseId": "provider", "fields": {"value": 0}}
    _assert_instance_matches_core_snapshot(load_agent_response(source))


@pytest.mark.parametrize("edge", ["function_call", "items", "inputs", "code_outputs", "shell_outputs"])
def test_nested_runtime_content_matches_canonical_snapshot(edge: str) -> None:
    leaf = Content.from_text("", annotations=[], additional_properties={"provider": {"false": False, "null": None}})
    if edge == "function_call":
        content = Content("function_approval_request", id="approval", function_call=leaf, user_input_request=True)
    elif edge in ("code_outputs", "shell_outputs"):
        kind = "code_interpreter_tool_result" if edge == "code_outputs" else "shell_tool_result"
        content = Content(cast(Any, kind), call_id="call", outputs=[leaf])
    elif edge == "items":
        content = Content("function_result", call_id="call", items=[leaf])
    else:
        content = Content("function_result", call_id="call", inputs=[leaf])
    _assert_instance_matches_core_snapshot(AgentResponse(messages=[Message("assistant", [content])]))


@pytest.mark.parametrize("status", ["error", "already_completed", "accepted"])
def test_terminal_statuses_do_not_change_the_core_mapping_or_classify_partial_text(status: str) -> None:
    response = AgentResponse(
        messages=[Message("assistant", [Content.from_text("partial answer")])],
        additional_properties={"durable_status": status, "durable_outcome": "failed"},
    )
    shared = _assert_instance_matches_core_snapshot(response)
    assert shared["messages"][0]["contents"][0]["text"] == "partial answer"
    assert shared["extensionData"] == {"durable_status": status, "durable_outcome": "failed"}


def test_datetime_instance_uses_core_serialization_not_an_audit_override() -> None:
    response = AgentResponse(created_at=cast(Any, datetime(2026, 9, 16, tzinfo=timezone.utc)))
    _assert_instance_matches_core_snapshot(response)


def test_instance_envelope_extras_and_extension_collisions_match_the_core_snapshot() -> None:
    content = Content.from_function_call(
        "call",
        "tool",
        arguments={"fields": {"pythonCoreFields": None}},
        additional_properties={"callId": "metadata", "fields": False, "pythonCoreFields": {}},
    )
    message = Message("assistant", [content], message_id="message", additional_properties={"fields": None})
    vars(message).update({"fields": {"message_id": "extra"}, "messageId": False, "pythonCoreFields": {"fields": []}})
    response = AgentResponse(
        messages=[message],
        response_id="response",
        additional_properties={"responseId": "metadata", "fields": [], "pythonCoreFields": {"value": False}},
    )
    vars(response).update({"fields": {"response_id": "extra"}, "responseId": 0, "pythonCoreFields": {"fields": None}})
    wire = _assert_instance_matches_core_snapshot(response)
    assert wire["responseId"] == "response"
    assert wire["extensionData"] == response.additional_properties
    assert wire["pythonCoreFields"]["fields"] == {
        "fields": {"response_id": "extra"},
        "responseId": 0,
        "pythonCoreFields": {"fields": None},
    }
    assert wire["messages"][0]["messageId"] == "message"
    assert wire["messages"][0]["pythonCoreFields"]["fields"]["messageId"] is False
    assert wire["messages"][0]["contents"][0]["extensionData"] == content.additional_properties


def test_value_by_name_model_uses_the_same_canonical_value_and_profile() -> None:
    class Output(BaseModel):
        count: int = Field(default=0, serialization_alias="serializedCount")

    model = Output(count=7)
    response = AgentResponse(value=model)
    wire = _assert_instance_matches_core_snapshot(response)
    assert wire["value"] == {"count": 7}
    assert wire["pythonCoreFields"]["fields"]["_durable_value_by_name"] is True
    loaded = _roundtrip(wire)
    assert vars(loaded)["_durable_value_by_name"] is True
    assert response._value is model


@pytest.mark.parametrize("value", [None, False, 0, "", [], {}, {"count": 0}])
def test_loaded_value_by_name_presence_matches_canonical_snapshot(value: Any) -> None:
    response = load_agent_response({
        "type": "agent_response",
        "messages": [],
        "value": value,
        "_durable_value_by_name": True,
    })
    wire = _assert_instance_matches_core_snapshot(response)
    assert "value" in wire and type(wire["value"]) is type(value)
    assert wire["pythonCoreFields"]["fields"]["_durable_value_by_name"] is True


@pytest.mark.parametrize("by_name", [False, True])
def test_value_by_name_marker_does_not_collide_with_extension_data(by_name: bool) -> None:
    response = load_agent_response({
        "type": "agent_response",
        "messages": [],
        "value": {"count": 0},
        "_durable_value_by_name": by_name,
        "additional_properties": {
            "_durable_value_by_name": not by_name,
            "fields": {"_durable_value_by_name": not by_name},
            "pythonCoreFields": {"fields": {"value": "metadata only"}},
        },
    })
    wire = _assert_instance_matches_core_snapshot(response)
    assert wire["extensionData"] == response.additional_properties
    loaded = _roundtrip(wire)
    assert getattr(loaded, "_durable_value_by_name", False) is by_name
    assert loaded.value == {"count": 0}


def test_instance_continuation_and_falsey_provider_usage_match_canonical_snapshot() -> None:
    response = AgentResponse(
        messages=[Message("assistant", [Content.from_text("")])],
        continuation_token=cast(Any, {"fields": [], "pythonCoreFields": None, "offset": 0, "flag": False}),
        usage_details=cast(Any, {"input_token_count": 0, "output_token_count": None, "provider": {"n": False}}),
    )
    wire = _assert_instance_matches_core_snapshot(response)
    assert wire["pythonContinuationEncoding"] == PROFILE
    assert _roundtrip(wire).continuation_token == response.continuation_token


def test_instance_never_calls_a_lazy_subclass_value_getter_or_serializer() -> None:
    class LazyResponse(AgentResponse[Any]):
        @property
        def value(self) -> Any:
            raise AssertionError("The shared codec must not evaluate the source value getter")

        def to_dict(self, **kwargs: Any) -> dict[str, Any]:
            raise AssertionError("The shared codec must not use the subclass serializer")

    response = LazyResponse(messages=[Message("assistant", ["not JSON"])], response_format={"type": "integer"})
    before = _observed_state(response)
    shared = serialize_terminal_response(response)
    assert "value" not in shared
    assert _observed_state(response) == before
    _roundtrip(shared)


def test_core_categories_cover_every_public_kind_and_constructor_field() -> None:
    assert {kind for category in CORE_CATEGORIES.values() for kind in category} == set(
        get_args(get_type_hints(Content.__init__)["type"])
    )
    covered = {key for category in CORE_CATEGORIES.values() for fields in category.values() for key in fields}
    assert covered | {"type", "annotations", "additional_properties", "raw_representation", "name"} == set(
        _constructor_fields(Content)
    )


def _assert_core_projection(actual: Content, expected: Content) -> None:
    assert type(actual) is Content
    for name in _constructor_fields(Content):
        value, original = getattr(actual, name), getattr(expected, name)
        if isinstance(original, Content):
            _assert_core_projection(value, original)
        elif isinstance(original, list) and any(isinstance(item, Content) for item in original):
            assert len(value) == len(original)
            for item, expected_item in zip(value, original):
                _assert_core_projection(item, expected_item)
        else:
            assert type(value) is type(original), name
            assert value == original, name


@pytest.mark.parametrize("as_instance", [False, True], ids=["snapshot", "instance"])
@pytest.mark.parametrize(
    ("kind", "fields"),
    [
        pytest.param(kind, fields, id=f"{category}-{kind}")
        for category, cases in CORE_CATEGORIES.items()
        for kind, fields in cases.items()
    ],
)
def test_core_categories_keep_full_typed_content(kind: str, fields: dict[str, Any], as_instance: bool) -> None:
    content = {
        **fields,
        "type": kind,
        "annotations": [{"type": "citation", "url": "https://example.test", "title": ""}],
        "additional_properties": {"": None, "provider": {"type": "text", "text": "inert"}},
    }
    source = _core_with(content)
    expected = load_agent_response(source)
    shared = serialize_terminal_response(expected if as_instance else source)
    actual = _roundtrip(shared)
    _assert_core_projection(actual.messages[0].contents[0], expected.messages[0].contents[0])


@pytest.mark.parametrize("approved", [False, True])
def test_approval_roundtrip_remains_actionable_for_hitl(approved: bool) -> None:
    request = Content.from_function_approval_request(
        "approval",
        Content.from_function_call("call", "tool", arguments={"n": 0}),
    )
    pending = AgentResponse(messages=[Message("assistant", [request])])
    loaded = _roundtrip(serialize_terminal_response(serialize_agent_response(pending)))
    assert len(loaded.user_input_requests) == 1
    response = loaded.user_input_requests[0].to_function_approval_response(approved)
    answered = _roundtrip(serialize_terminal_response(AgentResponse(messages=[Message("user", [response])])))
    result = answered.messages[0].contents[0]
    assert result.type == "function_approval_response" and result.approved is approved
    assert isinstance(result.function_call, Content)
    assert result.id == "approval" and result.function_call.call_id == "call"
    assert result.function_call.name == "tool" and result.function_call.arguments == {"n": 0}
    assert not answered.user_input_requests


@pytest.mark.parametrize("edge", ["function_call", "items", "inputs"])
def test_known_shared_core_field_profile_loads_nested_canonical_content(edge: str) -> None:
    nested = {"type": "function_approval_request", "id": "approval", "user_input_request": True, "function_call": CALL}
    value = nested if edge == "function_call" else [nested]
    shared = _response_with({
        "$type": "functionResult",
        "callId": "result",
        "result": {"type": "text", "text": "opaque"},
        "pythonCoreFields": {
            "profile": "agent-framework-python.core-fields",
            "version": 1,
            "fields": {edge: value},
        },
    })
    actual = _roundtrip(shared).messages[0].contents[0]
    content = getattr(actual, edge)
    if edge != "function_call":
        content = content[0]
    assert type(content) is Content and content.type == "function_approval_request"
    assert type(content.function_call) is Content and content.function_call.name == "tool"
    assert actual.result == {"type": "text", "text": "opaque"}


def test_core_profile_usage_on_non_usage_content_stays_canonical() -> None:
    source = {"type": "text", "text": "", "usage_details": {"input_token_count": 0, "provider": None}}
    actual = _roundtrip(serialize_terminal_response(_core_with(source)))
    assert actual.messages[0].contents[0].usage_details == source["usage_details"]


@pytest.mark.parametrize(
    "profile",
    [
        None,
        {},
        {"version": 1},
        {**CONTENT_PROFILE, "version": True},
        {**CONTENT_PROFILE, "version": 1.0},
        {**CONTENT_PROFILE, "version": 2},
        {**CONTENT_PROFILE, "profile": "foreign"},
        "profile",
        [],
    ],
)
def test_unknown_content_profiles_stay_inert(profile: Any) -> None:
    source = {"type": "function_approval_request", "function_call": CALL, "user_input_request": True}
    wrapper = {"$type": "unknown", "content": source, "future": {"control": None}}
    if profile is not None:
        wrapper["pythonContentEncoding"] = profile
    loaded = _roundtrip(_response_with(wrapper))
    assert not loaded.user_input_requests
    assert loaded.messages[0].contents[0].type == "unknown"
    assert loaded.messages[0].contents[0].additional_properties["content"] == source


@pytest.mark.parametrize("source", [None, [], False, "text", {}, {"type": None}, {"type": ""}, {"type": 1}])
def test_recognized_content_profile_requires_a_core_content_object(source: Any) -> None:
    payload = _response_with({
        "$type": "unknown",
        "content": source,
        "pythonContentEncoding": CONTENT_PROFILE,
    })
    codec.validate_terminal_response(payload)
    with pytest.raises(ValueError):
        load_terminal_response(payload)


def test_recognized_content_uses_base_constructors_and_does_not_activate_business_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    business = {"$type": "unknown", "pythonContentEncoding": CONTENT_PROFILE, "content": CALL, "type": "text"}
    source = {
        "type": "image_generation_tool_result",
        "$runtimeType": "untrusted.Type",
        "future": {"raw": None},
        "outputs": [business],
        "output": business,
        "result": business,
        "arguments": business,
        "annotations": [business],
        "additional_properties": {"business": business},
    }
    forbidden = Mock(side_effect=AssertionError("No dynamic imports or polymorphic deserialization"))
    monkeypatch.setattr(importlib, "import_module", forbidden)
    for cls in (Content, Message, AgentResponse):
        monkeypatch.setattr(cls, "from_dict", forbidden)
    wire = _response_with({
        "$type": "unknown",
        "content": source,
        "pythonContentEncoding": CONTENT_PROFILE,
        "future": [None],
    })
    loaded = _roundtrip(wire)
    content = loaded.messages[0].contents[0]
    assert type(loaded) is AgentResponse and type(content) is Content
    assert content.type == "image_generation_tool_result"
    for name in ("outputs", "output", "result", "arguments", "annotations", "additional_properties"):
        assert getattr(content, name) == source[name]
    assert not hasattr(content, "future") and not hasattr(content, "$runtimeType")
    forbidden.assert_not_called()


@pytest.mark.parametrize("key", ["", " ", "bad\nkey", "\x7f", "\x9f", "x" * 257])
@pytest.mark.parametrize("as_instance", [False, True])
def test_core_response_metadata_keys_outside_shared_contract_are_explicitly_rejected(
    key: str,
    as_instance: bool,
) -> None:
    source = {"type": "agent_response", "messages": [], "additional_properties": {key: 1}}
    with pytest.raises(ValueError, match="identifiers"):
        serialize_terminal_response(load_agent_response(source) if as_instance else source)


def test_valid_response_metadata_keys_and_unrestricted_business_keys_roundtrip() -> None:
    business = {"": 1, "\n": None, "x" * 257: False}
    source = {"type": "agent_response", "messages": [], "additional_properties": {"x" * 256: business}}
    assert _roundtrip(serialize_terminal_response(source)).additional_properties == source["additional_properties"]


def test_parent_snapshot_resolves_lazy_value_before_shared_conversion() -> None:
    response = AgentResponse(messages=[Message("assistant", ["42"])], response_format={"type": "integer"})
    snapshot = serialize_agent_response(response)
    assert snapshot["value"] == 42
    assert _roundtrip(serialize_terminal_response(snapshot)).value == 42
    assert not response._value_parsed


def test_public_validator_is_observational_and_does_not_load_runtime_profiles(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = _response_with({"$type": "unknown", "content": None, "pythonContentEncoding": CONTENT_PROFILE})
    before = deepcopy(payload)
    forbidden = Mock(side_effect=AssertionError("Validation must not construct runtime objects"))
    monkeypatch.setattr(codec, "load_agent_response", forbidden)
    codec.validate_terminal_response(payload)
    assert payload == before
    forbidden.assert_not_called()


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {"messages": []},
        {"messages": [], "value": False},
        {"messages": None},
        {"messages": [], "extensionData": {"": 1}},
    ],
)
def test_public_validator_matches_shared_schema(payload: Any) -> None:
    validator = Draft202012Validator({"$defs": SCHEMA["$defs"], "$ref": "#/$defs/terminalResponse"})
    if validator.is_valid(payload):
        codec.validate_terminal_response(payload)
    else:
        with pytest.raises(ValueError):
            codec.validate_terminal_response(payload)
