# Copyright (c) Microsoft. All rights reserved.

"""Cold-delivery fidelity tests against public core constructors, without host mocks."""

import builtins
import importlib
import json
from copy import deepcopy
from datetime import date
from inspect import Parameter, signature
from typing import Any, cast, get_args, get_type_hints
from unittest.mock import Mock

import pytest
from agent_framework import AgentResponse, Content, ContinuationToken, Message
from pydantic import BaseModel, ConfigDict, Field, Json, RootModel, ValidationError

from agent_framework_durabletask._response_utils import (
    ensure_response_format,
    is_terminal_agent_response,
    load_agent_response,
    serialize_agent_response,
)

CORRELATION_ID = "fidelity-review"


class AliasCount(BaseModel):
    count: int = Field(alias="aliasCount")


class JsonValue(BaseModel):
    document: Json[list[int]]


class NullValue(RootModel[None]):
    pass


def _wire(response: AgentResponse[Any]) -> dict[str, Any]:
    return json.loads(json.dumps(serialize_agent_response(response), allow_nan=False))


def _response(value: Any) -> AgentResponse[Any]:
    return AgentResponse(messages=[Message("assistant", ["Not the structured result"])], value=value)


@pytest.mark.parametrize(
    "value",
    [AliasCount(aliasCount=7), JsonValue(document="[1,2]"), NullValue(root=None)],
    ids=["validation-alias", "json-round-trip", "explicit-null"],
)
def test_structured_models_survive_cold_delivery_without_using_text(value: BaseModel) -> None:
    payload = _wire(_response(value))
    assert payload["value"] == value.model_dump(mode="json", by_alias=True, round_trip=True)
    loaded = load_agent_response(payload)

    ensure_response_format(type(value), CORRELATION_ID, loaded)

    assert type(loaded.value) is type(value)
    assert loaded.value == value
    assert loaded.text == "Not the structured result"


def test_nested_aliases_and_json_fields_round_trip_together() -> None:
    class NestedValue(BaseModel):
        child: AliasCount = Field(alias="nestedChild")
        document: Json[list[int]] = Field(alias="nestedDocument")

    value = NestedValue(nestedChild={"aliasCount": 4}, nestedDocument="[2,3]")
    payload = _wire(_response(value))
    assert payload["value"] == {"nestedChild": {"aliasCount": 4}, "nestedDocument": "[2,3]"}
    loaded = load_agent_response(payload)

    ensure_response_format(NestedValue, CORRELATION_ID, loaded)

    assert loaded.value == value


@pytest.mark.parametrize("defaulted", [False, True])
def test_distinct_serialization_aliases_use_checked_field_name_input(defaulted: bool) -> None:
    default: Any = 0 if defaulted else ...

    class SeparateAliases(BaseModel):
        count: int = Field(
            default=default,
            validation_alias="inputCount",
            serialization_alias="outputCount",
        )

    class NestedValue(BaseModel):
        child: SeparateAliases = Field(alias="nestedChild")
        values: list[SeparateAliases]

    value = NestedValue(nestedChild={"inputCount": 7}, values=[SeparateAliases(inputCount=8)])
    payload = _wire(_response(value))
    assert payload["value"] == {"child": {"count": 7}, "values": [{"count": 8}]}
    assert payload["_durable_value_by_name"] is True
    loaded = load_agent_response(payload)
    # An untyped delivery can cross another JSON boundary before a caller requests its model.
    loaded = load_agent_response(_wire(loaded))

    ensure_response_format(NestedValue, CORRELATION_ID, loaded)

    assert loaded.value == value
    assert loaded.additional_properties == {}


def test_strict_json_types_are_validated_as_json_not_python_values() -> None:
    class StrictValue(BaseModel):
        model_config = ConfigDict(strict=True)
        day: date
        coordinates: tuple[int, int]

    value = StrictValue(day=date(2026, 9, 9), coordinates=(1, 2))
    loaded = load_agent_response(_wire(_response(value)))

    ensure_response_format(StrictValue, CORRELATION_ID, loaded)

    assert loaded.value == value


@pytest.mark.parametrize("value", [None, False, 0, "", [], {}, {"type": "text", "custom": [1]}])
def test_retained_value_presence_is_not_truthiness(value: Any) -> None:
    payload = {"type": "agent_response", "messages": [Message("assistant", ["42"]).to_dict()], "value": value}
    loaded = load_agent_response(payload)
    assert "value" in _wire(loaded)
    assert _wire(loaded)["value"] == value

    ensure_response_format(RootModel[Any], CORRELATION_ID, loaded)

    assert isinstance(loaded.value, RootModel)
    assert loaded.value.root == value
    assert type(loaded.value.root) is type(value)


def test_explicit_null_is_not_replaced_by_valid_conflicting_text() -> None:
    loaded = load_agent_response({"messages": [Message("assistant", ['{"aliasCount":7}']).to_dict()], "value": None})

    with pytest.raises(ValidationError):
        ensure_response_format(AliasCount, CORRELATION_ID, loaded)


def test_absent_value_uses_requested_format_instead_of_original_lazy_format() -> None:
    response = AgentResponse(
        messages=[Message("assistant", ['{"aliasCount":7}'])],
        response_format=JsonValue,
    )

    ensure_response_format(AliasCount, CORRELATION_ID, response)

    assert response.value == AliasCount(aliasCount=7)
    assert "value" not in _wire(AgentResponse())


def test_matching_model_value_is_not_replaced() -> None:
    value = AliasCount(aliasCount=7)
    response = _response(value)

    ensure_response_format(AliasCount, CORRELATION_ID, response)

    assert response.value is value


def test_serializing_a_lazy_value_does_not_mutate_the_original_response() -> None:
    response = AgentResponse(messages=[Message("assistant", ['{"aliasCount":7}'])], response_format=AliasCount)
    before = dict(vars(response))
    before_fields = deepcopy(response.to_dict())

    payload = _wire(response)

    assert payload["value"] == {"aliasCount": 7}
    assert vars(response) == before
    assert response.to_dict() == before_fields
    response.messages[0].contents[0].text = '{"aliasCount":9}'
    assert response.value == AliasCount(aliasCount=9)


def test_lazy_schema_null_is_present_without_changing_the_original_cache() -> None:
    response = AgentResponse(messages=[Message("assistant", ["null"])], response_format={"type": "null"})
    before = dict(vars(response))

    payload = _wire(response)

    assert "value" in payload and payload["value"] is None
    assert vars(response) == before
    loaded = load_agent_response(payload)
    ensure_response_format(NullValue, CORRELATION_ID, loaded)
    assert loaded.value == NullValue(root=None)


def test_different_model_types_use_alias_json_when_validating_a_retained_model() -> None:
    class OtherCount(BaseModel):
        count: int = Field(alias="aliasCount")

    response = _response(AliasCount(aliasCount=7))

    ensure_response_format(OtherCount, CORRELATION_ID, response)

    assert type(response.value) is OtherCount
    assert response.value == OtherCount(aliasCount=7)


def test_subclass_snapshot_has_canonical_base_fields_and_retains_raw_extras() -> None:
    class CustomResponse(AgentResponse[Any]):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.custom_payload = {"type": "text", "application_field": [1]}

        def to_dict(self, **kwargs: Any) -> dict[str, Any]:
            return {"type": "custom_response", "response_id": "not-the-public-id"}

    response = CustomResponse(
        messages=[Message("assistant", ["answer"])],
        response_id="public-id",
        agent_id="public-agent",
        value=AliasCount(aliasCount=7),
        additional_properties={"type": "provider", "opaque": {"answer": 42}},
    )
    payload = _wire(response)

    assert payload["type"] == "agent_response"
    assert payload["response_id"] == "public-id"
    assert payload["agent_id"] == "public-agent"
    assert payload["custom_payload"] == response.custom_payload
    assert response.to_dict()["type"] == "custom_response"
    loaded = load_agent_response(payload)
    assert type(loaded) is AgentResponse
    assert not hasattr(loaded, "custom_payload")
    assert loaded.response_id == "public-id"
    assert loaded.additional_properties == response.additional_properties
    assert loaded.value == {"aliasCount": 7}


def test_ordinary_response_payload_is_canonical_and_accepted_by_core_loader() -> None:
    response = AgentResponse(
        messages=[Message("assistant", ["answer"])],
        response_id="public-id",
        additional_properties={"future": {"opaque": [1]}},
    )
    payload = _wire(response)

    assert payload["type"] == "agent_response"
    assert AgentResponse.from_dict(deepcopy(payload)).to_dict() == response.to_dict()
    assert load_agent_response(payload).to_dict() == response.to_dict()


def test_unknown_envelope_fields_are_ignored_without_changing_raw_snapshot() -> None:
    payload = {
        "type": "custom_response",
        "future_response": {"type": "future_type", "keep": [1]},
        "response_format": {"type": "object", "required": ["not_in_value"]},
        "messages": [
            {
                "type": "custom_message",
                "role": "assistant",
                "future_message": [2],
                "contents": [{"type": "text", "text": "answer", "future_content": [3]}],
            }
        ],
        "value": {"type": "agent_response", "arbitrary": {"type": "text", "future": [4]}},
        "additional_properties": {"type": "content", "keep": [5]},
    }
    before = deepcopy(payload)

    loaded = load_agent_response(payload)

    assert loaded.text == "answer"
    assert loaded.value == before["value"]
    assert not hasattr(loaded, "future_response")
    assert not hasattr(loaded.messages[0], "future_message")
    assert not hasattr(loaded.messages[0].contents[0], "future_content")
    loaded.value["arbitrary"]["future"].append(9)
    loaded.additional_properties["keep"].append(9)
    loaded.messages[0].contents[0].text = "changed"
    assert payload == before


def test_stored_type_names_never_select_or_import_python_classes(monkeypatch: pytest.MonkeyPatch) -> None:
    forbidden_import = Mock(side_effect=AssertionError("Stored type names must not trigger imports"))
    original_import = builtins.__import__

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name.startswith("untrusted"):
            return forbidden_import(name)
        return original_import(name, *args, **kwargs)

    payload = {
        "type": "untrusted.provider.CustomResponse",
        "response_format": {"type": "untrusted.provider.CustomModel"},
        "future_response": {"class": "untrusted.provider.Future"},
        "messages": [
            {
                "type": "untrusted.provider.CustomMessage",
                "role": "assistant",
                "contents": [
                    {
                        "type": "untrusted.provider.FutureContent",
                        "future_content": [1],
                        "additional_properties": {"opaque": [2]},
                    }
                ],
            }
        ],
    }
    before = deepcopy(payload)
    with monkeypatch.context() as patch:
        patch.setattr(builtins, "__import__", guarded_import)
        patch.setattr(importlib, "import_module", forbidden_import)
        loaded = load_agent_response(payload)

    forbidden_import.assert_not_called()
    assert type(loaded) is AgentResponse
    assert type(loaded.messages[0]) is Message
    content = loaded.messages[0].contents[0]
    assert type(content) is Content and content.type == "untrusted.provider.FutureContent"
    assert content.additional_properties == {"opaque": [2]}
    assert not hasattr(content, "future_content")
    assert payload == before


@pytest.mark.parametrize("content_type", get_args(get_type_hints(Content.__init__)["type"]))
def test_all_public_content_kinds_tolerate_unknown_optional_envelope_fields(content_type: Any) -> None:
    original = Content(content_type, additional_properties={"type": "opaque", "unknown": [1]})
    data = original.to_dict()
    data["future_content"] = {"custom": True}
    loaded = load_agent_response({"messages": [{"role": "assistant", "contents": [data]}]})

    assert type(loaded.messages[0].contents[0]) is Content
    assert loaded.messages[0].contents[0].to_dict() == original.to_dict()
    assert data["future_content"] == {"custom": True}


@pytest.mark.parametrize(
    ("kind", "field", "sequence"),
    [
        ("function_result", "items", True),
        ("search_tool_result", "items", True),
        ("code_interpreter_tool_call", "inputs", True),
        ("code_interpreter_tool_result", "outputs", True),
        ("shell_tool_result", "outputs", True),
        ("function_approval_request", "function_call", False),
        ("function_approval_response", "function_call", False),
    ],
)
def test_nested_framework_content_is_reconstructed_at_known_edges(kind: str, field: str, sequence: bool) -> None:
    inner = {"type": "text", "text": "result", "future_inner": {"keep": 1}}
    middle = {"type": "function_result", "items": [inner], "future_middle": [2]}
    content = {"type": kind, field: [middle] if sequence else middle, "future_outer": [3]}
    raw = {"messages": [{"role": "tool", "contents": [content]}]}
    before = deepcopy(raw)

    loaded = load_agent_response(raw)

    nested = getattr(loaded.messages[0].contents[0], field)
    nested = nested[0] if sequence else nested
    assert type(nested) is Content
    assert nested.items is not None
    assert type(nested.items[0]) is Content
    assert nested.items[0].text == "result"
    assert raw == before


@pytest.mark.parametrize("field", ["arguments", "result", "output", "outputs", "additional_properties"])
def test_application_payloads_with_framework_type_names_are_not_reconstructed(field: str) -> None:
    application = {"type": "text", "contents": [{"type": "error", "custom": [1]}], "not_a_core_field": [2]}
    value: Any = [application] if field == "outputs" else application
    raw = {"messages": [{"role": "assistant", "contents": [{"type": "image_generation_tool_result", field: value}]}]}

    loaded = load_agent_response(raw)

    assert getattr(loaded.messages[0].contents[0], field) == value
    assert not is_terminal_agent_response(loaded)


def test_rich_response_metadata_and_value_round_trip_independently() -> None:
    application = {"type": "text", "provider_field": {"type": "error", "unknown": [1]}}
    citation: Any = {
        "type": "citation",
        "title": "Source",
        "url": "https://example.test/source",
        "annotated_regions": [{"type": "text_span", "start_index": 0, "end_index": 6, "future": [1]}],
        "additional_properties": application,
        "future_annotation": [2],
    }
    response = AgentResponse(
        messages=[
            Message(
                "assistant",
                [
                    Content.from_text("answer", annotations=[citation], additional_properties=deepcopy(application)),
                    Content.from_text_reasoning(id="reason", text="summary", protected_data="opaque"),
                    Content.from_function_call("call", "lookup", arguments=deepcopy(application)),
                ],
                author_name="writer",
                message_id="message",
                additional_properties=deepcopy(application),
                raw_representation=object(),
            ),
            Message(
                "tool",
                [
                    Content.from_function_result(
                        "call", result=[Content.from_text("tool result"), Content.from_data(b"data", "image/png")]
                    )
                ],
            ),
        ],
        response_id="response",
        agent_id="agent",
        created_at="2026-09-09T00:00:00Z",
        finish_reason="stop",
        usage_details={"input_token_count": 3, "output_token_count": 2, "cache_read_input_token_count": 1},
        continuation_token=cast(ContinuationToken, deepcopy(application)),
        additional_properties=deepcopy(application),
        raw_representation=object(),
        value=AliasCount(aliasCount=7),
    )
    expected = response.to_dict()
    payload = _wire(response)
    loaded = load_agent_response(payload)

    assert loaded.to_dict() == expected
    assert loaded.messages[0].contents[0].annotations == [citation]
    assert loaded.messages[1].contents[0].items == response.messages[1].contents[0].items
    ensure_response_format(AliasCount, CORRELATION_ID, loaded)
    assert loaded.value == AliasCount(aliasCount=7)
    assert loaded.to_dict() == expected
    payload["additional_properties"]["provider_field"]["unknown"].append(9)
    assert response.additional_properties == application
    assert loaded.additional_properties == application
    assert "raw_representation" not in payload
    assert "raw_representation" not in payload["messages"][0]


def test_canonical_response_projection_tracks_public_constructor_fields() -> None:
    response = AgentResponse(response_id="response", agent_id="agent", value=AliasCount(aliasCount=7))
    payload = _wire(response)
    loaded = load_agent_response(payload)
    # Derive the category from core's public signature rather than a copied response-field list.
    for name, parameter in signature(AgentResponse).parameters.items():
        if parameter.kind not in (Parameter.POSITIONAL_OR_KEYWORD, Parameter.KEYWORD_ONLY):
            continue
        if name in ("value", "response_format", "raw_representation"):
            continue
        assert getattr(loaded, name) == getattr(response, name), name


@pytest.mark.parametrize("status", ["error", "already_completed"])
def test_explicit_terminal_status_skips_typed_parsing_even_without_error_content(status: str) -> None:
    response = AgentResponse(
        messages=[Message("assistant", ["not JSON"])],
        response_format=AliasCount,
        additional_properties={"durable_status": status},
    )
    assert is_terminal_agent_response(response)
    loaded = load_agent_response(_wire(response))

    ensure_response_format(AliasCount, CORRELATION_ID, loaded)

    assert loaded.value is None
    assert loaded.additional_properties == response.additional_properties


def test_accepted_acknowledgement_skips_validation_but_is_not_a_terminal_failure() -> None:
    response = AgentResponse(
        messages=[Message("assistant", ["Request accepted"])],
        response_format=AliasCount,
        additional_properties={"durable_status": "accepted"},
    )
    loaded = load_agent_response(_wire(response))

    ensure_response_format(AliasCount, CORRELATION_ID, loaded)

    assert not is_terminal_agent_response(loaded)
    assert loaded.value is None


@pytest.mark.parametrize("role", ["assistant", "system", "user", "developer", "tool"])
def test_direct_legacy_errors_are_terminal_only_outside_tool_messages(role: str) -> None:
    response: AgentResponse[Any] = AgentResponse(
        messages=[Message(role, [Content.from_error(message="failure")])],
        value={"aliasCount": 7},
    )
    loaded = load_agent_response(_wire(response))

    assert is_terminal_agent_response(loaded) is (role != "tool")
    ensure_response_format(AliasCount, CORRELATION_ID, loaded)
    if role == "tool":
        assert loaded.value == AliasCount(aliasCount=7)
    else:
        assert loaded.value == {"aliasCount": 7}


@pytest.mark.parametrize("valid", [False, True])
@pytest.mark.parametrize("nested", [False, True])
def test_recoverable_tool_errors_do_not_bypass_success_validation(valid: bool, nested: bool) -> None:
    error = Content.from_error(message="retryable lookup failure")
    content = Content.from_function_result("call", result=[error]) if nested else error
    response = AgentResponse(
        messages=[
            Message("tool", [content]),
            Message("assistant", ['{"aliasCount":7}' if valid else "invalid structured result"]),
        ]
    )
    loaded = load_agent_response(_wire(response))

    assert not is_terminal_agent_response(loaded)
    if valid:
        ensure_response_format(AliasCount, CORRELATION_ID, loaded)
        assert loaded.value == AliasCount(aliasCount=7)
    else:
        with pytest.raises(ValueError):
            ensure_response_format(AliasCount, CORRELATION_ID, loaded)


@pytest.mark.parametrize("version", [None, True, 0, 2, "1", {}, []])
def test_unknown_or_malformed_codec_versions_fail_without_mutating_input(version: Any) -> None:
    payload = {"type": "agent_response", "_durable_response_version": version, "future": [1]}
    before = deepcopy(payload)

    with pytest.raises(ValueError, match="Unsupported durable response version"):
        load_agent_response(payload)

    assert payload == before


def test_optional_supported_delivery_version_is_still_readable() -> None:
    response = AgentResponse(messages=[Message("assistant", ["marked snapshot"])])
    payload = _wire(response)
    payload["_durable_response_version"] = 1
    before = deepcopy(payload)

    assert load_agent_response(payload).to_dict() == response.to_dict()
    assert payload == before


@pytest.mark.parametrize("payload", [[], "response", 1])
def test_loader_rejects_unsupported_input_types(payload: Any) -> None:
    with pytest.raises(TypeError, match="Unsupported type"):
        load_agent_response(payload)


def test_loader_preserves_existing_instances_and_rejects_absent_input() -> None:
    response = AgentResponse()
    assert load_agent_response(response) is response
    with pytest.raises(ValueError, match="cannot be None"):
        load_agent_response(None)


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"invalid": "format"},
        {"value": 42},
        {"response_id": "not-an-envelope"},
        {"type": "text", "text": "not a response"},
        {"type": "custom_response"},
        {"messages": None},
        {"type": "custom_response", "messages": None},
    ],
)
def test_loader_rejects_nonresponse_mappings_without_mutating_input(payload: dict[str, Any]) -> None:
    before = deepcopy(payload)

    with pytest.raises(ValueError, match="requires a response type or messages"):
        load_agent_response(payload)

    assert payload == before


@pytest.mark.parametrize("response_type", [None, "", False, 1, [], {}])
def test_loader_rejects_corrupt_response_types_even_with_valid_messages(response_type: Any) -> None:
    with pytest.raises(ValueError, match="type must be a non-empty string"):
        load_agent_response({"type": response_type, "messages": []})


@pytest.mark.parametrize("response_type", [None, "agent_response", "custom_response"])
@pytest.mark.parametrize("empty", [False, True])
def test_loader_accepts_response_like_messages_with_or_without_type(response_type: str | None, empty: bool) -> None:
    response = AgentResponse(messages=[] if empty else [Message("assistant", ["internal helper"])])
    payload = response.to_dict()
    payload.pop("type", None)
    if response_type is not None:
        payload["type"] = response_type

    loaded = load_agent_response(payload)

    assert type(loaded) is AgentResponse
    assert loaded.to_dict() == response.to_dict()


def test_loader_accepts_canonical_empty_response_without_messages() -> None:
    assert load_agent_response({"type": "agent_response"}).to_dict() == AgentResponse().to_dict()


@pytest.mark.parametrize(
    "messages", [{}, {"role": "assistant"}, "", "not messages", b"", 0, False, ["not a message"], [{"contents": []}]]
)
def test_loader_rejects_malformed_message_envelopes(messages: Any) -> None:
    with pytest.raises(TypeError):
        load_agent_response({"messages": messages})


@pytest.mark.parametrize("content", [{"text": "missing type"}, {"type": None}, {"type": ""}, {"type": 1}])
def test_loader_rejects_corrupt_content_type_instead_of_guessing_a_framework_shape(content: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="requires 'type'"):
        load_agent_response({"messages": [{"role": "assistant", "contents": [content]}]})
