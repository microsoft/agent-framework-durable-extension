# Copyright (c) Microsoft. All rights reserved.

"""Non-converting preflight and bounded serializer-cost regressions, with literal oracles."""

import json
from collections.abc import Callable, Mapping
from typing import Any

import pytest
from agent_framework import AgentResponse, Content, Message
from agent_framework._serialization import SerializationMixin
from pydantic import BaseModel, model_serializer, model_validator

from agent_framework_durabletask import _response_utils as core
from agent_framework_durabletask import _shared_response as shared


class SerializingExtra(SerializationMixin):
    def __init__(self) -> None:
        self.calls = 0
        self.copies = 0
        self.cache: list[str] = []

    def to_dict(self, **kwargs: Any) -> dict[str, Any]:
        self.calls += 1
        self.cache.append("serialized")
        return {"keep": [None, False, 0]}

    @classmethod
    def from_dict(cls, value: Mapping[str, Any], /, **kwargs: Any) -> Any:
        raise AssertionError("No runtime reconstruction")

    def __deepcopy__(self, memo: dict[int, Any]) -> Any:
        self.copies += 1
        result = SerializingExtra()
        memo[id(self)] = result
        return result


def _with_extra(location: str, extra: Any) -> tuple[AgentResponse[Any], tuple[Any, ...]]:
    leaf = Content.from_text("leaf")
    content = Content("function_result", call_id="call", items=[leaf])
    message = Message("assistant", [content])
    response = AgentResponse(messages=[message])
    if location == "response-extra":
        vars(response)["provider_extra"] = extra
        path: tuple[Any, ...] = ("provider_extra",)
    elif location == "response-metadata":
        response.additional_properties["provider_extra"] = extra
        path = ("additional_properties", "provider_extra")
    elif location == "message-extra":
        vars(message)["provider_extra"] = extra
        path = ("messages", 0, "provider_extra")
    elif location == "content-result":
        content.result = extra
        path = ("messages", 0, "contents", 0, "result")
    else:
        assert location == "nested-metadata"
        leaf.additional_properties["provider_extra"] = extra
        path = ("messages", 0, "contents", 0, "items", 0, "additional_properties", "provider_extra")
    return response, path


@pytest.mark.parametrize(
    "location", ["response-extra", "response-metadata", "message-extra", "content-result", "nested-metadata"]
)
def test_preflight_and_strict_codec_reject_core_serializable_extras_before_hooks(location: str) -> None:
    # A separate native control proves these are executable Core serializers, not
    # unsupported fixtures that would fail before reaching the production boundary.
    native_extra = SerializingExtra()
    native, path = _with_extra(location, native_extra)
    payload: Any = AgentResponse.to_dict(native, exclude={"raw_representation", "response_format"})
    for key in path:
        payload = payload[key]
    assert payload == {"keep": [None, False, 0]}
    assert native_extra.calls > 0 and native_extra.cache == ["serialized"] * native_extra.calls
    assert native_extra.copies == 0

    for operation in (shared._audit_response, shared.serialize_terminal_response):
        extra = SerializingExtra()
        response, _ = _with_extra(location, extra)
        with pytest.raises(ValueError, match="JSON"):
            operation(response)
        assert extra.calls == 0 and extra.copies == 0 and extra.cache == []
        assert response._value is None and not response._value_parsed


@pytest.mark.parametrize("text", ["42", "not JSON"])
def test_preflight_leaves_raw_objects_and_lazy_value_resolution_to_their_owners(text: str) -> None:
    raw = SerializingExtra()
    content = Content.from_text(text, raw_representation=raw)
    message = Message("assistant", [content], raw_representation=raw)
    response = AgentResponse(messages=[message], response_format={"type": "integer"}, raw_representation=raw)
    shared._audit_response(response)
    assert not response._value_parsed and response._value is None
    assert "value" not in shared.serialize_terminal_response(response)
    if text == "42":
        assert core.serialize_agent_response(response)["value"] == 42
    else:
        with pytest.raises(ValueError, match="not valid JSON"):
            core.serialize_agent_response(response)
    assert not response._value_parsed and response._value is None
    assert raw.calls == 0 and raw.copies == 0 and raw.cache == []
    assert all(item.raw_representation is raw for item in (response, message, content))


def test_preflight_does_not_dump_or_validate_the_explicit_model_value() -> None:
    dumps: list[str] = []
    validations: list[Any] = []

    class Answer(BaseModel):
        answer: int

        @model_validator(mode="before")
        @classmethod
        def record_validation(cls, value: Any) -> Any:
            validations.append(value)
            return value

        @model_serializer(mode="wrap")
        def record_dump(self, handler: Any) -> Any:
            dumps.append("dump")
            return handler(self)

    value = Answer(answer=7)
    validations.clear()
    response: Any = AgentResponse(value=value)
    shared._audit_response(response)
    assert dumps == [] and validations == [] and response._value is value
    assert shared.serialize_terminal_response(response) == {"messages": [], "extensionData": {}, "value": {"answer": 7}}
    assert dumps and validations == [{"answer": 7}] and response._value is value


@pytest.mark.parametrize("location", ["response", "message"])
def test_inline_serializable_extras_keep_their_own_base_named_fields(location: str) -> None:
    class Extra(SerializationMixin):
        def __init__(self) -> None:
            self.response_id = "extra-response"
            self.message_id = "extra-message"
            self.additional_properties = {"keep": [False, 0, None]}

    extra = Extra()
    response = AgentResponse(messages=[Message("assistant", [])])
    target = response if location == "response" else response.messages[0]
    vars(target)["extra"] = extra
    payload = core.serialize_agent_response(response)
    parent = payload if location == "response" else payload["messages"][0]
    assert parent["extra"] == {
        "type": "extra",
        "response_id": "extra-response",
        "message_id": "extra-message",
        "additional_properties": {"keep": [False, 0, None]},
    }
    parent["extra"]["additional_properties"]["keep"].append("detached")
    assert extra.additional_properties == {"keep": [False, 0, None]}


def _observed(value: Any) -> Any:
    if isinstance(value, (AgentResponse, Message, Content)):
        return type(value), id(value), _observed(vars(value))
    if isinstance(value, dict):
        return id(value), tuple((key, _observed(item)) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return type(value), id(value), tuple(_observed(item) for item in value)
    return type(value), value


def _cost_input(size: int, nested: bool) -> tuple[AgentResponse[Any], dict[str, Any]]:
    if nested:
        node = Content.from_text("leaf")
        literal: dict[str, Any] = {"type": "text", "text": "leaf", "additional_properties": {}}
        for index in range(size - 1):
            node = Content("function_result", call_id=f"call-{index}", items=[node])
            literal = {
                "type": "function_result",
                "call_id": f"call-{index}",
                "items": [literal],
                "additional_properties": {},
            }
        contents = [node]
        expected_contents = [
            {
                "$type": "functionResult",
                "callId": f"call-{size - 2}",
                "extensionData": {},
                "pythonCoreFields": {
                    "profile": "agent-framework-python.core-fields",
                    "version": 1,
                    "fields": {"items": literal["items"]},
                },
            }
        ]
    else:
        contents = [Content.from_text(f"leaf-{index}") for index in range(size)]
        expected_contents = [{"$type": "text", "text": f"leaf-{index}", "extensionData": {}} for index in range(size)]
    return AgentResponse(messages=[Message("assistant", contents)]), {
        "messages": [{"role": "assistant", "contents": expected_contents, "extensionData": {}}],
        "extensionData": {},
    }


def test_terminal_snapshot_serializes_each_content_once(
    monkeypatch: pytest.MonkeyPatch, record_property: Callable[[str, Any], None]
) -> None:
    original = core.serialize_input_content
    visits = 0

    def counted(content: Content) -> dict[str, Any]:
        nonlocal visits
        visits += 1
        return original(content)

    # Retain both the imported root call and the real recursive serializer calls.
    monkeypatch.setattr(core, "serialize_input_content", counted)
    monkeypatch.setattr(shared, "serialize_input_content", counted)
    counts: dict[str, dict[int, int]] = {"nested": {}, "flat": {}}
    for shape, sizes in (("nested", (8, 16, 32)), ("flat", (32, 64))):
        for size in sizes:
            response, expected = _cost_input(size, shape == "nested")
            before = _observed(response)
            visits = 0
            wire = shared.serialize_terminal_response(response)
            counts[shape][size] = visits
            assert json.dumps(wire, sort_keys=True, allow_nan=False) == json.dumps(
                expected, sort_keys=True, allow_nan=False
            )
            assert _observed(response) == before
            wire["messages"][0]["contents"].clear()
            assert _observed(response) == before
    record_property("codec_serializer_visits", json.dumps(counts))
    assert counts == {"nested": {8: 8, 16: 16, 32: 32}, "flat": {32: 32, 64: 64}}
