# Copyright (c) Microsoft. All rights reserved.

"""Producer-to-staging fidelity, using real codecs and independent wire expectations."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

import pytest
from agent_framework import AgentResponse, Content, Message
from pydantic import BaseModel, Field, Json, RootModel

from agent_framework_durabletask._delivery_state import lookup_response, stage_response
from agent_framework_durabletask._response_utils import (
    ensure_response_format,
    load_agent_response,
    preserve_input_envelope,
)
from agent_framework_durabletask._shared_response import load_terminal_response

NOW = datetime(2026, 9, 19, 12, tzinfo=timezone.utc)


class Answer(BaseModel):
    answer: int


class SymmetricAnswer(BaseModel):
    answer: int = Field(alias="inputAnswer")


class SeparateAliases(BaseModel):
    answer: int = Field(default=0, validation_alias="inputAnswer", serialization_alias="outputAnswer")


class JsonAnswer(BaseModel):
    document: Json[list[int]]


class NullAnswer(RootModel[None]):
    pass


class ProviderResponse(AgentResponse[Any]):
    def to_dict(self, **kwargs: Any) -> dict[str, Any]:
        raise AssertionError("Staging must use the canonical base-response serializer")


def _state() -> dict[str, Any]:
    return {
        "schemaVersion": "2.0.0",
        "future": {"keep": [None, False, 0]},
        "data": {
            "conversationHistory": [],
            "session": {"state": {"keep": [None, False, 0]}},
            "terminalResults": {
                "existing": {
                    "correlationId": "existing",
                    "outcome": "succeeded",
                    "completedAt": "2026-09-19T10:00:00Z",
                    "response": {"messages": [], "value": False},
                }
            },
            "completionReceipts": {
                "existing": {
                    "correlationId": "existing",
                    "outcome": "succeeded",
                    "completedAt": "2026-09-19T10:00:00Z",
                    "resultState": "available",
                }
            },
        },
    }


def _observed(value: Any) -> Any:
    # Inspect attributes without invoking a serializer/getter that could hide loss
    # or populate the lazy-value cache. Retain object and container identities too.
    if isinstance(value, (AgentResponse, Message, Content, BaseModel)):
        return type(value), id(value), _observed(vars(value))
    if isinstance(value, dict):
        return id(value), {key: _observed(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value), id(value), [_observed(item) for item in value]
    return type(value), value


def _wire(candidate: dict[str, Any]) -> dict[str, Any]:
    return candidate["data"]["terminalResults"]["new"]["response"]


def test_staging_preserves_attached_core_nulls_and_future_fields_without_mutation() -> None:
    raw = {
        "type": "chat_message",
        "role": "assistant",
        "future": None,
        "contents": [{"type": "function_result", "call_id": "call", "result": None, "future": None}],
    }
    response = load_agent_response({"type": "agent_response", "messages": [raw]})
    preserve_input_envelope(response.messages[0], raw)
    state = _state()
    inputs = (state, response, raw)
    before = _observed(inputs)

    candidate = stage_response(state, "new", response, delivery_window_seconds=60, now=NOW)

    # Literal presence assertions distinguish explicit null from omitted Core defaults.
    expected_response = {
        "messages": [
            {
                "role": "assistant",
                "contents": [
                    {
                        "$type": "functionResult",
                        "callId": "call",
                        "result": None,
                        "extensionData": {},
                        "pythonCoreFields": {
                            "profile": "agent-framework-python.core-fields",
                            "version": 1,
                            "fields": {"future": None},
                        },
                    }
                ],
                "extensionData": {},
                "pythonCoreFields": {
                    "profile": "agent-framework-python.core-fields",
                    "version": 1,
                    "fields": {"future": None},
                },
            }
        ],
        "extensionData": {},
    }
    expected = deepcopy(state)
    common = {
        "correlationId": "new",
        "outcome": "succeeded",
        "completedAt": "2026-09-19T12:00:00+00:00",
        "resultExpiresAt": "2026-09-19T12:01:00+00:00",
    }
    expected["data"]["terminalResults"]["new"] = {**common, "response": expected_response}
    expected["data"]["completionReceipts"]["new"] = {**common, "resultState": "available"}
    assert json.dumps(candidate, sort_keys=True, allow_nan=False) == json.dumps(
        expected, sort_keys=True, allow_nan=False
    )
    assert _observed(inputs) == before

    _wire(candidate)["messages"][0]["contents"][0]["pythonCoreFields"]["fields"]["future"] = ["edited"]
    candidate["data"]["session"]["state"]["keep"].append("edited")
    candidate["data"]["terminalResults"]["existing"]["response"]["value"] = True
    assert _observed(inputs) == before


def test_unattached_null_content_keeps_canonical_core_omission() -> None:
    content = Content("function_result", call_id="call", result=None)
    vars(content)["future"] = None
    response = AgentResponse(messages=[Message("assistant", [content])])
    state = _state()
    inputs = (state, response)
    before = _observed(inputs)

    candidate = stage_response(state, "new", response, delivery_window_seconds=60, now=NOW)

    assert _wire(candidate)["messages"][0]["contents"] == [
        {"$type": "functionResult", "callId": "call", "extensionData": {}}
    ]
    assert _observed(inputs) == before


@pytest.mark.parametrize(
    "location",
    [
        "response-extra",
        "message-extra",
        "content-extra",
        "nested-content-extra",
        "response-metadata",
        "message-metadata",
        "content-metadata",
    ],
)
def test_non_json_producer_fields_fail_without_publishing_a_candidate_or_mutating_inputs(location: str) -> None:
    leaf = Content.from_text("leaf")
    content = Content("function_result", call_id="call", items=[leaf])
    message = Message("assistant", [content, Content.from_text('{"answer":7}')])
    response = AgentResponse(messages=[message], response_format=Answer)
    target: Any = {
        "response": response,
        "message": message,
        "content": content,
        "nested-content": leaf,
    }[location.rsplit("-", 1)[0]]
    invalid = object()
    if location.endswith("-metadata"):
        target.additional_properties["future_non_json"] = invalid
    else:
        vars(target)["future_non_json"] = invalid
    state = _state()
    inputs = (state, response)
    before = _observed(inputs)
    candidates: list[dict[str, Any]] = []

    with pytest.raises(ValueError, match="JSON"):
        candidates.append(stage_response(state, "new", response, delivery_window_seconds=60, now=NOW))

    assert candidates == []
    assert "new" not in state["data"]["terminalResults"]
    assert "new" not in state["data"]["completionReceipts"]
    assert _observed(inputs) == before
    assert response._value is None and response._value_parsed is False


@pytest.mark.parametrize(
    ("response_format", "text", "expected", "by_name"),
    [
        (Answer, '{"answer":7}', {"answer": 7}, False),
        (SymmetricAnswer, '{"inputAnswer":7}', {"inputAnswer": 7}, False),
        (SeparateAliases, '{"inputAnswer":7}', {"answer": 7}, True),
        ({"type": "object"}, '{"answer":7}', {"answer": 7}, False),
    ],
)
def test_lazy_structured_values_keep_canonical_aliases_without_caching_on_the_input(
    response_format: Any, text: str, expected: Any, by_name: bool
) -> None:
    response = AgentResponse(messages=[Message("assistant", [text])], response_format=response_format)
    state = _state()
    inputs = (state, response)
    before = _observed(inputs)

    candidate = stage_response(state, "new", response, delivery_window_seconds=60, now=NOW)

    wire = _wire(candidate)
    assert wire["value"] == expected
    assert wire.get("pythonCoreFields") == (
        {
            "profile": "agent-framework-python.core-fields",
            "version": 1,
            "fields": {"_durable_value_by_name": True},
        }
        if by_name
        else None
    )
    assert _observed(inputs) == before
    assert response._value is None and response._value_parsed is False
    loaded = lookup_response(candidate, "new", now=NOW)
    assert loaded is not None
    if isinstance(response_format, type):
        ensure_response_format(response_format, "new", loaded)
        assert isinstance(loaded.value, response_format)
        assert isinstance(loaded.value, (Answer, SymmetricAnswer, SeparateAliases))
        assert loaded.value.answer == 7
    else:
        assert loaded.value == {"answer": 7}


@pytest.mark.parametrize("response_type", [AgentResponse, ProviderResponse])
@pytest.mark.parametrize(
    ("value", "expected", "by_name"),
    [
        (SymmetricAnswer(inputAnswer=7), {"inputAnswer": 7}, False),
        (SeparateAliases(inputAnswer=7), {"answer": 7}, True),
        (JsonAnswer(document="[1,2]"), {"document": "[1,2]"}, False),
        (NullAnswer(root=None), None, False),
    ],
)
def test_typed_values_keep_base_serialization_and_roundtrip_policy(
    response_type: type[AgentResponse[Any]], value: BaseModel, expected: Any, by_name: bool
) -> None:
    response = response_type(messages=[Message("assistant", ["not structured JSON"])], value=value)
    state = _state()
    inputs = (state, response)
    before = _observed(inputs)

    candidate = stage_response(state, "new", response, delivery_window_seconds=60, now=NOW)

    wire = _wire(candidate)
    assert "value" in wire and wire["value"] == expected
    assert wire.get("pythonCoreFields", {}).get("fields", {}).get("_durable_value_by_name", False) is by_name
    loaded = lookup_response(candidate, "new", now=NOW)
    assert loaded is not None
    ensure_response_format(type(value), "new", loaded)
    assert type(loaded.value) is type(value) and loaded.value == value
    assert _observed(inputs) == before
    assert response._value is value


def _opaque_response() -> dict[str, Any]:
    return {
        "messages": [
            {
                "role": "assistant",
                "createdAt": "2026-09-19T11:00:00.123456789Z",
                "future": None,
                "contents": [
                    {"$type": "text", "text": "original", "future": None},
                    {"$type": "unknown", "content": {"type": "foreign", "data": [None, False, 0]}},
                ],
            }
        ],
        "value": {"answer": 7},
        "pythonCoreFields": {
            "profile": "agent-framework-python.core-fields",
            "version": 1,
            "fields": {"_durable_value_by_name": True},
        },
        "continuationToken": "AQID",
        "pythonContinuationEncoding": {"profile": "foreign", "version": 99},
        "extensionData": {"provider": [None, False, 0]},
        "future": {"keep": [None, False, 0, 0.0]},
    }


@pytest.mark.parametrize("typed", [False, True])
def test_loaded_opaque_shared_response_keeps_its_original_shadow_and_value_encoding(typed: bool) -> None:
    raw = _opaque_response()
    response = load_terminal_response(raw)
    if typed:
        ensure_response_format(SeparateAliases, "new", response)
    state = _state()
    inputs = (state, response, raw)
    before = _observed(inputs)

    candidate = stage_response(state, "new", response, delivery_window_seconds=60, now=NOW)

    assert json.dumps(_wire(candidate), sort_keys=True, allow_nan=False) == json.dumps(
        raw, sort_keys=True, allow_nan=False
    )
    assert response.continuation_token is None
    assert _observed(inputs) == before
    _wire(candidate)["future"]["keep"].append("edited")
    assert _observed(inputs) == before


@pytest.mark.parametrize("mutation", ["text", "value", "metadata"])
def test_modified_shared_projection_still_fails_without_publishing_a_candidate(mutation: str) -> None:
    raw = _opaque_response()
    raw_before = deepcopy(raw)
    response = load_terminal_response(raw)
    if mutation == "text":
        response.messages[0].contents[0].text = "changed"
    elif mutation == "value":
        assert isinstance(response.value, dict)
        response.value["answer"] = 8
    else:
        response.additional_properties["provider"] = ["changed"]
    state = _state()
    inputs = (state, response, raw)
    before = _observed(inputs)
    candidates: list[dict[str, Any]] = []

    with pytest.raises(ValueError, match="modified shared response|cannot preserve the shared structured value"):
        candidates.append(stage_response(state, "new", response, delivery_window_seconds=60, now=NOW))

    assert candidates == []
    assert "new" not in state["data"]["terminalResults"]
    assert "new" not in state["data"]["completionReceipts"]
    assert _observed(inputs) == before
    assert raw == raw_before
