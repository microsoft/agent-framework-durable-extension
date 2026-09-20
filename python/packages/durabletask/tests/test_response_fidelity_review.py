# Copyright (c) Microsoft. All rights reserved.

"""Source-audit regressions at the shared projection and real SDK delivery boundaries."""

import json
from copy import deepcopy
from typing import Any

import pytest
from agent_framework import AgentResponse, Content, Message
from durabletask.task import CompletableTask
from pydantic import BaseModel, Field, RootModel, field_validator

from agent_framework_durabletask import (
    ensure_response_format,
    load_agent_response,
    read_agent_state,
    serialize_agent_response,
)
from agent_framework_durabletask._executors import DurableAgentTask
from agent_framework_durabletask._shared_response import load_terminal_response, serialize_terminal_response

CORRELATION = "response-fidelity"
COLLISIONS = [
    pytest.param(False, 0, id="false-default-zero-value"),
    pytest.param(0, False, id="zero-default-false-value"),
    pytest.param(0, 0.0, id="integer-default-float-value"),
    pytest.param(0.0, 0, id="float-default-integer-value"),
    pytest.param({"nested": [False]}, {"nested": [0]}, id="nested-false-zero"),
]


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, allow_nan=False)


@pytest.mark.parametrize(("default", "value"), COLLISIONS)
def test_serialization_only_alias_cannot_hide_a_json_type_change(default: Any, value: Any) -> None:
    class AnswerAny(BaseModel):
        answer: Any = Field(default=default, serialization_alias="outputAnswer")

    original = AnswerAny(answer=deepcopy(value))
    raw = json.loads(_json(serialize_agent_response(AgentResponse[Any](value=original))))
    assert raw["_durable_value_by_name"] is True
    assert _json(raw["value"]) == _json({"answer": value})
    loaded = load_agent_response(raw)
    ensure_response_format(AnswerAny, CORRELATION, loaded)
    assert isinstance(loaded.value, AnswerAny)
    assert type(loaded.value.answer) is type(value)
    assert _json(loaded.value.answer) == _json(value)


@pytest.mark.parametrize(("default", "value"), COLLISIONS)
def test_field_name_fallback_also_rejects_json_type_changes(default: Any, value: Any) -> None:
    class ChangingAnswer(BaseModel):
        # Required input forces the alias branch to raise before the by-name probe.
        answer: Any = Field(serialization_alias="outputAnswer")

        @field_validator("answer", mode="before")
        @classmethod
        def normalize(cls, incoming: Any) -> Any:
            return deepcopy(default)

    # A retained/model-constructed value need not already match a validation pass.
    original = ChangingAnswer.model_construct(answer=deepcopy(value))
    with pytest.raises(ValueError, match="cannot round-trip"):
        serialize_agent_response(AgentResponse[Any](value=original))
    assert _json(original.answer) == _json(value)


@pytest.mark.parametrize("value", [False, 0, 0.0, None, {"nested": [False, 0, 0.0]}])
def test_symmetric_alias_still_uses_alias_json_when_lossless(value: Any) -> None:
    class AnswerAny(BaseModel):
        answer: Any = Field(alias="outputAnswer")

    raw = serialize_agent_response(AgentResponse[Any](value=AnswerAny(outputAnswer=value)))
    assert "_durable_value_by_name" not in raw
    assert _json(raw["value"]) == _json({"outputAnswer": value})
    loaded = load_agent_response(json.loads(_json(raw)))
    ensure_response_format(AnswerAny, CORRELATION, loaded)
    assert isinstance(loaded.value, AnswerAny)
    assert _json(loaded.value.answer) == _json(value)


@pytest.mark.parametrize("by_name", [False, True])
@pytest.mark.parametrize("value", [False, 0, 0.0, None])
def test_typed_shared_symmetric_model_reencodes_the_original_field_policy(by_name: bool, value: Any) -> None:
    class AnswerAny(BaseModel):
        answer: Any = Field(alias="outputAnswer")

    core: dict[str, Any] = {
        "type": "agent_response",
        "messages": [],
        "value": {"answer" if by_name else "outputAnswer": value},
    }
    if by_name:
        core["_durable_value_by_name"] = True
    wire = json.loads(_json(serialize_terminal_response(core)))
    wire["future"] = {"inert": [False, 0, None]}
    before = _json(wire)
    response = load_terminal_response(wire)
    for _ in range(2):
        ensure_response_format(AnswerAny, CORRELATION, response)
        assert isinstance(response.value, AnswerAny)
        assert _json(response.value.answer) == _json(value)
        encoded = serialize_terminal_response(response)
        assert _json(encoded) == before
        assert _json(serialize_agent_response(response)["value"]) == _json(core["value"])
        response = load_terminal_response(json.loads(_json(encoded)))
    assert _json(wire) == before


@pytest.mark.parametrize("mutation", ["false-to-zero", "remove-value"])
def test_typed_by_name_shared_reencoding_still_rejects_value_or_presence_changes(mutation: str) -> None:
    class AnswerAny(BaseModel):
        answer: Any = Field(alias="outputAnswer")

    wire = serialize_terminal_response({
        "type": "agent_response",
        "messages": [],
        "value": {"answer": False},
        "_durable_value_by_name": True,
    })
    before = _json(wire)
    response = load_terminal_response(wire)
    ensure_response_format(AnswerAny, CORRELATION, response)
    assert isinstance(response.value, AnswerAny)
    if mutation == "false-to-zero":
        response.value.answer = 0
    else:
        response._value = None
        response._value_parsed = False
    with pytest.raises(ValueError, match="modified shared response projection"):
        serialize_terminal_response(response)
    with pytest.raises(ValueError, match="cannot preserve"):
        serialize_agent_response(response)
    assert _json(wire) == before


@pytest.mark.parametrize("present", [False, True])
def test_shared_typed_root_null_reencoding_does_not_change_value_presence(present: bool) -> None:
    wire: dict[str, Any] = {"messages": []}
    if present:
        wire["value"] = None
    before = _json(wire)
    response = load_terminal_response(wire)
    if present:
        ensure_response_format(RootModel[None], CORRELATION, response)
        assert isinstance(response.value, RootModel) and response.value.root is None
    else:
        with pytest.raises(ValueError, match="no structured value"):
            ensure_response_format(RootModel[None], CORRELATION, response)
    assert _json(serialize_terminal_response(response)) == before
    assert ("value" in serialize_agent_response(response)) is present


def _wire(present: bool) -> dict[str, Any]:
    result: dict[str, Any] = {"$type": "functionResult", "callId": "call", "future_content": [False]}
    error: dict[str, Any] = {"$type": "error", "message": "tool diagnostic", "future_content": [0]}
    if present:
        result["result"] = None
        error["details"] = None
    return {
        "messages": [{"role": "tool", "contents": [result, error], "future_message": [None]}],
        "future_response": {"value": None},
    }


def _cold_lookup(wire: dict[str, Any]) -> AgentResponse[Any]:
    common = {"correlationId": CORRELATION, "outcome": "succeeded", "completedAt": "2026-09-19T00:00:00Z"}
    state = {
        "schemaVersion": "2.0.0",
        "data": {
            "conversationHistory": [],
            "terminalResults": {CORRELATION: {**common, "response": wire}},
            "completionReceipts": {CORRELATION: {**common, "resultState": "available"}},
        },
    }
    reader = read_agent_state(_json(state))
    response = reader.try_get_agent_response(CORRELATION)
    assert response is not None
    assert _json(reader.to_dict()) == _json(state)
    return response


def _deliver(raw: dict[str, Any], consumer: str, precompleted: bool) -> AgentResponse[Any]:
    if consumer == "durabletask":
        child: CompletableTask[Any] = CompletableTask()
        if precompleted:
            child.complete(raw)
        task = DurableAgentTask(child, None, CORRELATION)
        if not precompleted:
            assert not task.is_complete
            child.complete(raw)
        assert task.is_complete and not task.is_failed
        assert child.get_result() is raw
        return task.get_result()

    from agent_framework_azurefunctions._orchestration import AgentTask
    from azure.durable_functions.models.actions.NoOpAction import NoOpAction
    from azure.durable_functions.models.Task import AtomicTask, TaskState

    af_child = AtomicTask(17, NoOpAction())
    if precompleted:
        af_child.set_value(is_error=False, value=raw)
    af_task = AgentTask(af_child, None, CORRELATION)
    if not precompleted:
        assert af_task.state is TaskState.RUNNING
        af_child.set_value(is_error=False, value=raw)
    assert af_task.state is TaskState.SUCCEEDED
    assert af_child.result is raw
    assert isinstance(af_task.result, AgentResponse)
    return af_task.result


def _assert_presence(raw: dict[str, Any], present: bool) -> None:
    message = raw["messages"][0]
    result, error = message["contents"]
    assert result["type"] == "function_result" and error["type"] == "error"
    assert ("result" in result) is present
    assert ("error_details" in error) is present
    if present:
        assert result["result"] is None and error["error_details"] is None
    assert "value" not in raw and "future_response" not in raw
    assert "future_message" not in message
    assert all("future_content" not in content and "$type" not in content for content in message["contents"])


@pytest.mark.parametrize("present", [False, True], ids=["absent", "explicit-null"])
@pytest.mark.parametrize("cold", [False, True], ids=["codec", "cold-state-lookup"])
@pytest.mark.parametrize("consumer", ["durabletask", "azurefunctions"])
@pytest.mark.parametrize("precompleted", [False, True], ids=["delayed", "precompleted"])
def test_shared_content_presence_survives_actual_json_task_returns(
    present: bool, cold: bool, consumer: str, precompleted: bool
) -> None:
    wire = _wire(present)
    before = _json(wire)
    response = _cold_lookup(wire) if cold else load_terminal_response(wire)
    assert _json(serialize_terminal_response(response)) == before
    for _ in range(2):
        raw = json.loads(_json(serialize_agent_response(response)))
        _assert_presence(raw, present)
        raw_before = _json(raw)
        response = _deliver(raw, consumer, precompleted)
        _assert_presence(json.loads(_json(serialize_agent_response(response))), present)
        assert _json(raw) == raw_before
    assert _json(wire) == before
    _assert_presence(serialize_agent_response(_cold_lookup(wire)), present)


@pytest.mark.parametrize("original", [None, {"old": False}])
@pytest.mark.parametrize("replacement", [None, {"new": 0}])
def test_current_content_fields_override_the_retained_envelope(original: Any, replacement: Any) -> None:
    wire = _wire(True)
    wire["messages"][0]["contents"][0]["result"] = deepcopy(original)
    before = _json(wire)
    response = load_terminal_response(wire)
    response.messages[0].contents[0].result = deepcopy(replacement)
    raw = serialize_agent_response(response)["messages"][0]["contents"][0]
    if replacement is None and original is not None:
        assert "result" not in raw
    else:
        assert "result" in raw and _json(raw["result"]) == _json(replacement)
    assert _json(wire) == before


@pytest.mark.parametrize("edge", ["function_call", "items", "inputs", "code-outputs", "shell-outputs"])
def test_profiled_nested_core_presence_survives_json_without_promoting_unknown_fields(edge: str) -> None:
    leaf = {"type": "function_result", "call_id": "nested", "result": None, "future_nested": [False]}
    outer: dict[str, Any] = {"type": "future_content", "future_outer": [0]}
    if edge == "function_call":
        outer[edge] = leaf
    elif edge in ("code-outputs", "shell-outputs"):
        outer["type"] = "code_interpreter_tool_result" if edge == "code-outputs" else "shell_tool_result"
        outer["outputs"] = [leaf]
    else:
        outer[edge] = [leaf]
    wire = {
        "messages": [
            {
                "role": "tool",
                "contents": [
                    {
                        "$type": "unknown",
                        "content": outer,
                        "pythonContentEncoding": {"profile": "agent-framework-python.content", "version": 1},
                    }
                ],
            }
        ],
    }
    before = _json(wire)
    response = load_terminal_response(wire)
    assert _json(serialize_terminal_response(response)) == before
    for _ in range(2):
        raw = json.loads(_json(serialize_agent_response(response)))
        content = raw["messages"][0]["contents"][0]
        nested = (
            content["function_call"]
            if edge == "function_call"
            else content["outputs" if edge.endswith("outputs") else edge][0]
        )
        assert nested["result"] is None
        assert "future_outer" not in content and "future_nested" not in nested
        response = load_agent_response(raw)
    assert _json(wire) == before


def test_null_presence_tracks_content_occurrences_after_reordering_and_replacement() -> None:
    wire = {
        "messages": [
            {
                "role": "tool",
                "contents": [
                    {"$type": "functionResult", "callId": "present", "result": None},
                    {"$type": "functionResult", "callId": "absent"},
                ],
            }
        ],
    }
    before = _json(wire)
    response = load_terminal_response(wire)
    response.messages[0].contents.reverse()
    raw = serialize_agent_response(response)["messages"][0]["contents"]
    assert raw[0]["call_id"] == "absent" and "result" not in raw[0]
    assert raw[1]["call_id"] == "present" and raw[1]["result"] is None
    response.messages[0].contents[1] = Content("function_result", call_id="present", result=None)
    raw = serialize_agent_response(response)["messages"][0]["contents"]
    assert all("result" not in content for content in raw)
    assert _json(wire) == before


def test_cleared_message_fields_do_not_restore_original_values_or_constructor_defaults() -> None:
    wire = _wire(True)
    wire["messages"][0].update(authorName="original", extensionData={"provider": False})
    before = _json(wire)
    response = load_terminal_response(wire)
    response.messages[0].author_name = None
    # Simulate an ordinary public-field edit even though its annotation excludes None.
    vars(response.messages[0])["additional_properties"] = None
    raw = serialize_agent_response(response)["messages"][0]
    assert "author_name" not in raw and "additional_properties" not in raw
    assert _json(wire) == before


def test_bare_core_null_does_not_invent_explicit_presence() -> None:
    response = AgentResponse(
        messages=[
            Message(
                "tool",
                [
                    Content("function_result", call_id="call", result=None),
                    Content("error", message="tool diagnostic", error_details=None),
                ],
            )
        ]
    )
    _assert_presence(serialize_agent_response(response), False)


def test_current_base_fields_win_over_subclass_serializers() -> None:
    class MisleadingContent(Content):
        def to_dict(self, **kwargs: Any) -> dict[str, Any]:
            return {"type": "text", "text": "not the current result", "result": "stale"}

    class MisleadingMessage(Message):
        DEFAULT_EXCLUDE = {"contents", "author_name", "raw_representation"}

    response = AgentResponse(
        messages=[
            MisleadingMessage(
                "tool", [MisleadingContent("function_result", call_id="call", result=0)], author_name="current"
            )
        ]
    )
    raw = serialize_agent_response(response)["messages"][0]
    assert raw["type"] == "message" and raw["author_name"] == "current"
    assert raw["contents"] == [{"type": "function_result", "call_id": "call", "result": 0, "additional_properties": {}}]
