# Copyright (c) Microsoft. All rights reserved.

"""Container validation must not redefine the legacy Core constructor contract."""

import json
from copy import deepcopy
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import AgentResponse, Content, Message
from durabletask.task import CompletableTask

from agent_framework_durabletask._executors import DurableAgentTask
from agent_framework_durabletask._response_utils import (
    load_agent_response,
    preserve_input_envelope,
    serialize_agent_response,
    serialize_input_message,
)

PRIVATE = "private-contents-review-sentinel"
CONTAINER_ERROR = "Message contents must be a sequence of content items"
INVALID_CONTAINERS = [
    pytest.param({}, id="empty-object"),
    pytest.param({"type": "text", "text": PRIVATE}, id="content-object"),
    pytest.param({PRIVATE: "value"}, id="private-object-key"),
    pytest.param(0, id="zero"),
    pytest.param(17, id="integer"),
    pytest.param(False, id="false"),
    pytest.param(1.5, id="float"),
    pytest.param(b"", id="empty-bytes"),
    pytest.param(PRIVATE.encode(), id="bytes"),
    pytest.param(bytearray(PRIVATE.encode()), id="bytearray"),
    pytest.param(set(), id="empty-set"),
    pytest.param({PRIVATE}, id="set"),
]


@pytest.mark.parametrize("contents", INVALID_CONTAINERS)
def test_invalid_contents_containers_fail_without_changing_input(contents: Any) -> None:
    raw = {"messages": [{"role": "assistant", "contents": contents}]}
    before = deepcopy(raw)
    with pytest.raises(TypeError) as raised:
        load_agent_response(raw)
    assert type(raised.value) is TypeError
    assert str(raised.value) == CONTAINER_ERROR
    assert PRIVATE not in str(raised.value)
    assert raw == before


def test_a_content_object_is_not_a_sequence_of_its_field_names() -> None:
    # The old loop fed these keys to Core as two valid text items, yielding
    # "type text" instead of rejecting the malformed serialized container.
    raw = json.loads('{"messages":[{"role":"assistant","contents":{"type":"text","text":"hello"}}]}')
    with pytest.raises(TypeError, match="^Message contents must be a sequence of content items$"):
        load_agent_response(raw)


@pytest.mark.parametrize(
    ("fields", "texts"),
    [
        pytest.param({}, [], id="omitted"),
        pytest.param({"contents": None}, [], id="null"),
        pytest.param({"contents": []}, [], id="empty-list"),
        pytest.param({"contents": ()}, [], id="empty-tuple"),
        pytest.param({"contents": ""}, [], id="empty-string"),
        pytest.param({"contents": "hello"}, list("hello"), id="legacy-scalar-string"),
        pytest.param({"contents": ["hello"]}, ["hello"], id="single-text"),
        pytest.param({"contents": ("hello", "world")}, ["hello", "world"], id="tuple"),
        pytest.param({"contents": [None, "hello", None]}, ["hello"], id="null-items"),
        pytest.param({"contents": [{"type": "text", "text": "hello"}]}, ["hello"], id="content-mapping"),
        pytest.param({"contents": [Content.from_text("hello")]}, ["hello"], id="content-instance"),
    ],
)
def test_constructor_inputs_and_real_core_json_remain_compatible(fields: dict[str, Any], texts: list[str]) -> None:
    core = Message("user", **deepcopy(fields))
    assert [content.text for content in core.contents] == texts
    assert core.text == " ".join(texts)
    source = {"role": "user", **deepcopy(fields)}
    response = load_agent_response({"messages": [source]})
    assert response.messages[0].to_dict() == core.to_dict()
    wire = json.loads(AgentResponse(messages=[core]).to_json())
    assert isinstance(wire["messages"][0]["contents"], list)
    assert load_agent_response(wire).messages[0].to_dict() == core.to_dict()
    # Literal constructor strings are not changed into singleton text items.
    assert serialize_agent_response(response)["messages"][0]["contents"] == wire["messages"][0]["contents"]


def test_existing_response_and_message_instances_bypass_mapping_container_validation() -> None:
    message = Message("user", "hello")
    original = AgentResponse(messages=[message])
    assert load_agent_response(original) is original
    loaded = load_agent_response({"messages": message})
    assert loaded.messages[0].to_dict() == message.to_dict()
    assert loaded.messages[0].text == "h e l l o"


@pytest.mark.parametrize("contents", INVALID_CONTAINERS)
@pytest.mark.parametrize("already_attached", [False, True])
def test_preservation_validates_before_replacing_any_envelope(contents: Any, already_attached: bool) -> None:
    message = Message("assistant", [Content.from_text("first"), Content.from_text("second")])
    if already_attached:
        preserve_input_envelope(
            message,
            {
                "role": "assistant",
                "future": "original",
                "contents": [
                    {"type": "text", "text": "first", "future": "first"},
                    {"type": "text", "text": "second", "future": "second"},
                ],
            },
        )
    objects = [message, *message.contents]
    before = [dict(vars(value)) for value in objects]
    raw = {"role": "assistant", "contents": contents, "future": PRIVATE}
    raw_before = deepcopy(raw)
    with pytest.raises(TypeError) as raised:
        preserve_input_envelope(message, raw)
    assert type(raised.value) is TypeError
    assert str(raised.value) == CONTAINER_ERROR
    assert raw == raw_before
    for value, previous in zip(objects, before):
        assert vars(value) == previous
        for key in ("_durable_original_core_message", "_durable_original_core_content"):
            if key in previous:
                assert getattr(value, key) is previous[key]


@pytest.mark.parametrize("fields", [{}, {"contents": None}, {"contents": []}, {"contents": ()}, {"contents": ""}])
def test_empty_preservation_inputs_keep_inert_message_fields(fields: dict[str, Any]) -> None:
    raw: dict[str, Any] = {"role": "user", "author_name": None, "future": {"contents": PRIVATE}, **fields}
    message = Message("user", **deepcopy(fields))
    preserve_input_envelope(message, raw)
    assert serialize_input_message(message) == {
        "type": "message",
        "role": "user",
        "author_name": None,
        "future": {"contents": PRIVATE},
        "additional_properties": {},
        "contents": [],
    }
    raw["future"]["contents"] = "changed"
    assert serialize_input_message(message)["future"] == {"contents": PRIVATE}


@pytest.mark.parametrize("contents", ["hello", ["hello"], ("hello",)])
def test_preserving_legacy_text_inputs_does_not_reinterpret_strings(contents: Any) -> None:
    message = Message("user", contents)
    before = message.to_dict()
    raw = {"role": "user", "contents": contents, "future": None}
    preserve_input_envelope(message, raw)
    assert message.to_dict() == before
    assert serialize_input_message(message) == {**before, "future": None}


@pytest.mark.parametrize("as_tuple", [False, True])
@pytest.mark.parametrize("raw_count", [1, 2, 3])
def test_preservation_does_not_invent_a_length_equality_requirement(as_tuple: bool, raw_count: int) -> None:
    # This helper has always attached only paired items. Shape validation does
    # not authorize rejecting edited messages or adding a new cardinality rule.
    message = Message("assistant", ["one", "two"])
    contents = [{"type": "text", "text": "old", "future": index} for index in range(raw_count)]
    raw = {"role": "assistant", "contents": tuple(contents) if as_tuple else contents}
    preserve_input_envelope(message, raw)
    current = serialize_input_message(message)["contents"]
    assert [item["text"] for item in current] == ["one", "two"]
    assert [item.get("future") for item in current] == ([0, None] if raw_count == 1 else [0, 1])


@pytest.mark.parametrize(
    ("kind", "edge"),
    [
        ("function_approval_request", "function_call"),
        ("function_result", "items"),
        ("code_interpreter_tool_call", "inputs"),
        ("code_interpreter_tool_result", "outputs"),
        ("shell_tool_result", "outputs"),
    ],
)
def test_nested_content_edges_keep_presence_and_opaque_metadata(
    kind: str, edge: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    business = {"type": "untrusted.Business", "contents": {PRIVATE: False}, "items": 7}
    nested = {"type": "function_call", "call_id": "call", "name": "tool", "arguments": business, "future": None}
    outer = {"type": kind, edge: nested if edge == "function_call" else [nested], "future": business}
    raw = {"role": "assistant", "contents": [outer], "additional_properties": business}
    before = deepcopy(raw)
    forbidden = Mock(side_effect=AssertionError("No polymorphic deserialization"))
    for cls in (AgentResponse, Message, Content):
        monkeypatch.setattr(cls, "from_dict", forbidden)
    message = load_agent_response({"messages": [raw]}).messages[0]
    preserve_input_envelope(message, raw)
    snapshot = serialize_input_message(message)
    encoded = snapshot["contents"][0]
    child = encoded[edge] if edge == "function_call" else encoded[edge][0]
    assert child["future"] is None
    assert child["arguments"] == business
    assert encoded["future"] == business
    assert snapshot["additional_properties"] == business
    assert raw == before
    forbidden.assert_not_called()


@pytest.mark.parametrize("field", ["result", "arguments", "output", "outputs", "annotations", "additional_properties"])
def test_non_content_edges_are_not_subject_to_container_validation(field: str) -> None:
    business = {"type": "not.Content", "contents": {"secret": PRIVATE}, "inputs": 3}
    value = [business] if field in ("outputs", "annotations") else business
    raw = {"role": "assistant", "contents": [{"type": "image_generation_tool_result", field: value}]}
    message = load_agent_response({"messages": [raw]}).messages[0]
    preserve_input_envelope(message, raw)
    assert getattr(message.contents[0], field) == value
    assert serialize_input_message(message)["contents"][0][field] == value


@pytest.mark.parametrize("edge", ["top", "function_call", "items", "inputs", "code-outputs", "shell-outputs"])
def test_malformed_nested_content_keeps_its_private_value_error(edge: str, caplog: pytest.LogCaptureFixture) -> None:
    invalid = {"type": {"private": PRIVATE}, "text": PRIVATE}
    if edge == "top":
        content: dict[str, Any] = invalid
    elif edge == "function_call":
        content = {"type": "function_approval_request", "function_call": invalid}
    elif edge in ("items", "inputs"):
        content = {"type": "function_result", edge: [invalid]}
    else:
        content = {
            "type": "code_interpreter_tool_result" if edge == "code-outputs" else "shell_tool_result",
            "outputs": [invalid],
        }
    raw = {"messages": [{"role": "assistant", "contents": [content]}]}
    before = deepcopy(raw)
    with pytest.raises(ValueError) as raised:
        load_agent_response(raw)
    assert type(raised.value) is ValueError
    assert str(raised.value) == "Content mapping requires 'type' to be a non-empty string"
    assert PRIVATE not in caplog.text
    assert raw == before


@pytest.mark.parametrize("consumer", ["durabletask", "azurefunctions"])
@pytest.mark.parametrize("precompleted", [False, True])
def test_real_sdk_tasks_fail_malformed_delivery_without_logging_payload(
    consumer: str, precompleted: bool, caplog: pytest.LogCaptureFixture
) -> None:
    raw = json.loads(json.dumps({"messages": [{"role": "assistant", "contents": {PRIVATE: "hello"}}]}))
    before = deepcopy(raw)
    if consumer == "durabletask":
        child: CompletableTask[Any] = CompletableTask()
        if precompleted:
            child.complete(raw)
        task = DurableAgentTask(child, None, "contents-review")
        if not precompleted:
            child.complete(raw)
        assert task.is_complete and task.is_failed
        assert child.get_result() is raw
        failure = task.get_exception()
        assert failure.details.error_type == "TypeError"
        diagnostic = failure.details.message
    else:
        from agent_framework_azurefunctions._orchestration import AgentTask
        from azure.durable_functions.models.actions.NoOpAction import NoOpAction
        from azure.durable_functions.models.Task import AtomicTask, TaskState

        af_child = AtomicTask(7, NoOpAction())
        if precompleted:
            af_child.set_value(is_error=False, value=raw)
        af_task = AgentTask(af_child, None, "contents-review")
        if not precompleted:
            af_child.set_value(is_error=False, value=raw)
        assert af_task.state is TaskState.FAILED
        assert af_child.result is raw
        assert type(af_task.result) is TypeError
        diagnostic = str(af_task.result)
    assert diagnostic == CONTAINER_ERROR
    assert PRIVATE not in caplog.text
    assert raw == before
