# Copyright (c) Microsoft. All rights reserved.

"""Shared approval classification survives delivery without weakening legacy value handling."""

import importlib
import json
from copy import deepcopy
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import AgentResponse, Content, Message
from durabletask.task import CompletableTask, TaskFailedError
from pydantic import BaseModel, RootModel

from agent_framework_durabletask import ensure_response_format, load_agent_response, serialize_agent_response
from agent_framework_durabletask._executors import DurableAgentTask
from agent_framework_durabletask._shared_response import load_terminal_response, serialize_terminal_response

KEY = "_durable_approval_policy"
POLICY = {"profile": "agent-framework-python.shared-approval", "version": 1}
VALUE_KEY = "_durable_value_policy"
VALUE_POLICY = {"profile": "agent-framework-python.shared-value", "version": 1}
CONSUMERS = [
    ("direct", False),
    ("durabletask", False),
    ("durabletask", True),
    ("azurefunctions", False),
    ("azurefunctions", True),
]
MALFORMED: tuple[Any, ...] = (
    None,
    False,
    [],
    "shared",
    {},
    {"profile": "foreign", "version": 1},
    {"profile": POLICY["profile"]},
    {**POLICY, "extra": None},
    *({**POLICY, "version": value} for value in (None, False, True, 1.0, "1", 2)),
)


class Answer(BaseModel):
    answer: int


def _pending() -> AgentResponse[Any]:
    request = Content.from_function_approval_request(
        "approval",
        Content.from_function_call("call", "never_invoked", arguments={"type": "business", "n": False}),
    )
    return AgentResponse(messages=[Message("assistant", [request])], response_format=Answer)


def _json(value: Any) -> Any:
    return json.loads(json.dumps(value, allow_nan=False))


def _shared() -> dict[str, Any]:
    # Real producer -> shared wire -> shared reader, not a hand-attached private flag.
    wire = _json(serialize_terminal_response(serialize_agent_response(_pending())))
    wire["future"] = {"opaque": [None, False, 0]}
    return wire


def _consume(
    raw: dict[str, Any],
    consumer: str,
    precompleted: bool,
    response_format: type[BaseModel] = Answer,
) -> AgentResponse[Any]:
    if consumer == "direct":
        response = load_agent_response(raw)
        ensure_response_format(response_format, "approval-transport", response)
        return response
    if consumer == "durabletask":
        child: CompletableTask[Any] = CompletableTask()
        if precompleted:
            child.complete(raw)
        task = DurableAgentTask(child, response_format, "approval-transport")
        if not precompleted:
            assert not task.is_complete
            child.complete(raw)
        assert task.is_complete and child.get_result() is raw
        if task.is_failed:
            failure = task.get_exception()
            assert isinstance(failure, TaskFailedError)
            assert failure.details.error_type in ("ValueError", "ValidationError")
            # SDK tasks expose a TaskFailedError, while direct and AF consumers
            # retain the original ValueError. Compare the real diagnostic below.
            raise ValueError(failure.details.message) from failure
        return task.get_result()

    # Exercise the real AF consumer too. This test requires both workspace packages.
    from agent_framework_azurefunctions._orchestration import AgentTask
    from azure.durable_functions.models.actions.NoOpAction import NoOpAction
    from azure.durable_functions.models.Task import AtomicTask, TaskState

    af_child = AtomicTask(7, NoOpAction())
    if precompleted:
        af_child.set_value(is_error=False, value=raw)
    af_task = AgentTask(af_child, response_format, "approval-transport")
    if not precompleted:
        assert af_task.state is TaskState.RUNNING
        af_child.set_value(is_error=False, value=raw)
    assert af_child.result is raw
    if af_task.state is TaskState.FAILED:
        raise af_task.result
    assert af_task.state is TaskState.SUCCEEDED
    assert isinstance(af_task.result, AgentResponse)
    return af_task.result


@pytest.mark.parametrize(("consumer", "precompleted"), CONSUMERS)
def test_shared_approval_direct_and_json_task_delivery_agree(consumer: str, precompleted: bool) -> None:
    wire = _shared()
    before = deepcopy(wire)
    direct = load_terminal_response(wire)
    ensure_response_format(Answer, "approval-transport", direct)
    assert direct.value is None and len(direct.user_input_requests) == 1
    assert serialize_terminal_response(direct) == before
    raw = _json(serialize_agent_response(direct))
    assert raw[KEY] == POLICY and raw[VALUE_KEY] == VALUE_POLICY
    assert "_durable_response_version" not in raw and "value" not in raw
    assert "future" not in raw and "_original_shared_response" not in raw
    for _ in range(2):
        original = deepcopy(raw)
        response = _consume(raw, consumer, precompleted)
        assert response.value is None and len(response.user_input_requests) == 1
        assert not hasattr(response, "_original_shared_response")
        request = response.user_input_requests[0]
        assert type(request) is Content and request.id == "approval" and request.approved is None
        assert type(request.function_call) is Content and request.function_call.call_id == "call"
        assert request.function_call.name == "never_invoked"
        assert request.function_call.arguments == {"type": "business", "n": False}
        raw = _json(serialize_agent_response(response))
        assert raw == original
    assert wire == before


@pytest.mark.parametrize("policy", MALFORMED)
@pytest.mark.parametrize(("consumer", "precompleted"), CONSUMERS)
def test_malformed_approval_policy_fails_without_mutating_input(policy: Any, consumer: str, precompleted: bool) -> None:
    raw = _json(serialize_agent_response(_pending()))
    raw[KEY] = policy
    before = deepcopy(raw)
    with pytest.raises(ValueError, match="shared-approval policy"):
        _consume(raw, consumer, precompleted)
    assert raw == before


@pytest.mark.parametrize(("consumer", "precompleted"), CONSUMERS)
@pytest.mark.parametrize(
    "shape",
    ["empty", "text", "flagged-text", "opaque", "nested-business", "answered", "unflagged", "numeric-flag", "no-call"],
)
def test_policy_requires_actual_pending_function_approval_content(
    shape: str, consumer: str, precompleted: bool
) -> None:
    raw = _json(serialize_agent_response(_pending()))
    approval = raw["messages"][0]["contents"][0]
    if shape == "empty":
        raw["messages"] = []
    elif shape in ("text", "flagged-text"):
        raw["messages"][0]["contents"] = [
            {"type": "text", "text": "not JSON", "user_input_request": shape == "flagged-text"}
        ]
    elif shape in ("opaque", "nested-business"):
        raw["messages"][0]["contents"] = [
            {"type": "unknown", "additional_properties": {"content": approval}}
            if shape == "opaque"
            else {"type": "function_result", "call_id": "call", "result": approval}
        ]
    elif shape == "answered":
        approval["type"] = "function_approval_response"
        approval["approved"] = True
    elif shape in ("unflagged", "numeric-flag"):
        approval["user_input_request"] = False if shape == "unflagged" else 1
    else:
        approval.pop("function_call")
    raw[KEY] = dict(POLICY)
    before = deepcopy(raw)
    with pytest.raises(ValueError, match="requires pending function approval Content"):
        _consume(raw, consumer, precompleted)
    assert raw == before


@pytest.mark.parametrize("value", [None, False, 0, {}, {"answer": 1}, {"answer": "invalid"}])
@pytest.mark.parametrize(("consumer", "precompleted"), CONSUMERS)
def test_explicit_approval_policy_cannot_hide_any_completed_value(
    value: Any, consumer: str, precompleted: bool
) -> None:
    raw = _json(serialize_agent_response(_pending()))
    raw.update({KEY: dict(POLICY), "value": value})
    before = deepcopy(raw)
    with pytest.raises(ValueError, match="without a structured value"):
        _consume(raw, consumer, precompleted)
    assert raw == before


@pytest.mark.parametrize("marked", [False, True])
@pytest.mark.parametrize("present", [False, True])
@pytest.mark.parametrize(("consumer", "precompleted"), CONSUMERS)
def test_initial_core_approval_and_value_policy_only_remain_legacy(
    marked: bool, present: bool, consumer: str, precompleted: bool
) -> None:
    # A first-time host response is not activated merely by using the new serializer.
    pending = _pending()
    raw = _json(serialize_agent_response(pending))
    assert KEY not in raw
    if marked:
        raw[VALUE_KEY] = dict(VALUE_POLICY)
    if present:
        raw["value"] = {"answer": "invalid"}
    before = deepcopy(raw)
    with pytest.raises(ValueError):
        _consume(raw, consumer, precompleted)
    assert KEY not in serialize_agent_response(load_agent_response(raw))
    assert raw == before


@pytest.mark.parametrize("location", ["response", "message", "content", "arguments", "value"])
@pytest.mark.parametrize(("consumer", "precompleted"), CONSUMERS)
def test_approval_marker_in_business_data_cannot_bypass_legacy_format(
    location: str, consumer: str, precompleted: bool
) -> None:
    raw = _json(serialize_agent_response(_pending()))
    message = raw["messages"][0]
    content = message["contents"][0]
    targets = {
        "response": raw.setdefault("additional_properties", {}),
        "message": message.setdefault("additional_properties", {}),
        "content": content.setdefault("additional_properties", {}),
        "arguments": content["function_call"]["arguments"],
        "value": {},
    }
    targets[location][KEY] = dict(POLICY)
    if location == "value":
        raw["value"] = targets[location]
    before = deepcopy(raw)
    with pytest.raises(ValueError):
        _consume(raw, consumer, precompleted)
    assert KEY not in serialize_agent_response(load_agent_response(raw))
    assert raw == before


@pytest.mark.parametrize("value", [1, "1"])
@pytest.mark.parametrize(("consumer", "precompleted"), CONSUMERS)
def test_legacy_approval_with_valid_value_still_uses_requested_format(
    value: Any, consumer: str, precompleted: bool
) -> None:
    raw = _json(serialize_agent_response(_pending()))
    raw["value"] = {"answer": value}
    response = _consume(raw, consumer, precompleted)
    assert isinstance(response.value, Answer) and response.value.answer == 1
    assert KEY not in serialize_agent_response(response)


@pytest.mark.parametrize("shared", [False, True])
@pytest.mark.parametrize(("consumer", "precompleted"), CONSUMERS)
def test_ordinary_completed_value_never_gets_approval_marker(shared: bool, consumer: str, precompleted: bool) -> None:
    response = AgentResponse(messages=[Message("assistant", ['{"answer":1}'])], response_format=Answer)
    raw = serialize_agent_response(response)
    if shared:
        raw = serialize_agent_response(load_terminal_response(serialize_terminal_response(raw)))
    raw = _json(raw)
    assert KEY not in raw
    delivered = _consume(raw, consumer, precompleted)
    assert isinstance(delivered.value, Answer) and delivered.value.answer == 1


@pytest.mark.parametrize("policy", MALFORMED)
def test_malformed_policy_in_shared_core_profile_fails_only_when_projected(policy: Any) -> None:
    raw = _json(serialize_agent_response(_pending()))
    raw[KEY] = policy
    shared = serialize_terminal_response(raw)
    before = deepcopy(shared)
    with pytest.raises(ValueError, match="shared-approval policy"):
        load_terminal_response(shared)
    assert shared == before


@pytest.mark.parametrize(
    "marker", ["_original_shared_response", "_durable_shared_approval", "_durable_response_version"]
)
def test_raw_private_flags_and_legacy_version_do_not_establish_shared_approval(marker: str) -> None:
    raw = _json(serialize_agent_response(_pending()))
    raw[marker] = {"messages": []} if marker == "_original_shared_response" else 1
    with pytest.raises(ValueError):
        _consume(raw, "direct", False)
    assert KEY not in serialize_agent_response(load_agent_response(raw))


@pytest.mark.parametrize(("consumer", "precompleted"), CONSUMERS)
@pytest.mark.parametrize(
    ("value", "response_format", "valid"),
    [
        ({"answer": 1}, Answer, True),
        ({"answer": "1"}, Answer, False),
        ({"answer": 1, "extra": False}, Answer, False),
        (None, RootModel[None], True),
        (False, RootModel[bool], True),
        (False, RootModel[int], False),
        (0, RootModel[float], False),
    ],
)
def test_shared_values_with_approval_content_keep_presence_and_json_type_fidelity(
    value: Any, response_format: type[BaseModel], valid: bool, consumer: str, precompleted: bool
) -> None:
    wire = _shared()
    wire["value"] = deepcopy(value)
    before = deepcopy(wire)
    direct = load_terminal_response(wire)
    raw = _json(serialize_agent_response(direct))
    assert KEY not in raw and raw[VALUE_KEY] == VALUE_POLICY
    if valid:
        ensure_response_format(response_format, "approval-transport", direct)
        delivered = _consume(raw, consumer, precompleted, response_format)
        assert isinstance(delivered.value, response_format)
        result = serialize_agent_response(delivered)
        assert json.dumps(result["value"]) == json.dumps(value)
        assert KEY not in result
    else:
        with pytest.raises(ValueError, match="cannot preserve"):
            ensure_response_format(response_format, "approval-transport", direct)
        with pytest.raises(ValueError, match="cannot preserve"):
            _consume(raw, consumer, precompleted, response_format)
    assert wire == before


@pytest.mark.parametrize("transported", [False, True])
@pytest.mark.parametrize("mutation", ["remove", "answer", "add-value"])
def test_approval_classification_is_rechecked_after_consumer_mutation(transported: bool, mutation: str) -> None:
    response: AgentResponse[Any] = load_terminal_response(_shared())
    if transported:
        response = load_agent_response(_json(serialize_agent_response(response)))
    if mutation == "remove":
        response.messages = []
    elif mutation == "answer":
        response.messages[0].contents[0].approved = True
    else:
        response._value = {"answer": 1}
        response._value_parsed = True
    with pytest.raises(ValueError, match="no structured value"):
        ensure_response_format(Answer, "approval-transport", response)
    if mutation == "add-value":
        with pytest.raises(ValueError, match="cannot preserve"):
            serialize_agent_response(response)
    else:
        assert KEY not in serialize_agent_response(response)


@pytest.mark.parametrize(("consumer", "precompleted"), CONSUMERS)
def test_explicit_profile_is_untrusted_classification_not_tool_authorization(
    consumer: str, precompleted: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    # No private provenance is needed from an untrusted transport. The exact profile
    # and real content shape suffice, but cannot import a type or invoke/approve a tool.
    raw = _json(serialize_agent_response(_pending()))
    raw[KEY] = dict(POLICY)
    call = raw["messages"][0]["contents"][0]["function_call"]
    call["arguments"].update({"$runtimeType": "untrusted.Tool", "__pickled__": "inert"})
    forbidden = Mock(side_effect=AssertionError("No executable deserialization or tool approval"))
    # Load optional consumer dependencies before blocking dynamic imports.
    _consume(deepcopy(raw), consumer, precompleted)
    monkeypatch.setattr(importlib, "import_module", forbidden)
    for cls in (AgentResponse, Message, Content):
        monkeypatch.setattr(cls, "from_dict", forbidden)
    monkeypatch.setattr(Content, "to_function_approval_response", forbidden)
    loaded = _consume(raw, consumer, precompleted)
    assert loaded.user_input_requests[0].approved is None
    snapshot = serialize_agent_response(loaded)
    assert snapshot[KEY] == POLICY and snapshot[VALUE_KEY] == VALUE_POLICY
    assert snapshot["messages"][0]["contents"][0]["function_call"] == call
    forbidden.assert_not_called()


@pytest.mark.parametrize("as_instance", [False, True])
def test_explicit_shared_conversion_preserves_approval_semantics(as_instance: bool) -> None:
    raw = serialize_agent_response(load_terminal_response(_shared()))
    source = load_agent_response(raw) if as_instance else raw
    shared = _json(serialize_terminal_response(source))
    response = load_terminal_response(shared)
    ensure_response_format(Answer, "approval-transport", response)
    assert response.value is None and len(response.user_input_requests) == 1
    assert serialize_terminal_response(response) == shared
    assert serialize_agent_response(response)[KEY] == POLICY


@pytest.mark.parametrize(("consumer", "precompleted"), CONSUMERS)
@pytest.mark.parametrize("response_format", [{"type": "object"}, None])
def test_approval_policy_rejects_an_embedded_response_format_before_lazy_parsing(
    consumer: str, precompleted: bool, response_format: Any
) -> None:
    raw = _json(serialize_agent_response(_pending()))
    raw.update({KEY: dict(POLICY), "response_format": response_format})
    raw["messages"][0]["contents"].append({"type": "text", "text": '{"answer":1}'})
    before = deepcopy(raw)
    with pytest.raises(ValueError, match="shared-approval policy.*response_format"):
        _consume(raw, consumer, precompleted)
    assert raw == before


def test_plain_legacy_approval_retains_its_json_response_format_constructor_contract() -> None:
    raw = _json(serialize_agent_response(_pending()))
    raw["response_format"] = {"type": "object"}
    raw["messages"][0]["contents"].append({"type": "text", "text": '{"answer":1}'})
    before = deepcopy(raw)
    loaded = load_agent_response(raw)
    assert loaded.value == {"answer": 1}
    assert len(loaded.user_input_requests) == 1
    assert KEY not in serialize_agent_response(loaded)
    assert raw == before


@pytest.mark.parametrize(("consumer", "precompleted"), CONSUMERS)
def test_approval_policy_without_embedded_format_keeps_json_text_a_non_result(
    consumer: str, precompleted: bool
) -> None:
    raw = _json(serialize_agent_response(_pending()))
    raw[KEY] = dict(POLICY)
    raw["messages"][0]["contents"].append({"type": "text", "text": '{"answer":1}'})
    before = deepcopy(raw)
    loaded = _consume(raw, consumer, precompleted)
    for _ in range(2):
        assert loaded.value is None and len(loaded.user_input_requests) == 1
        snapshot = serialize_agent_response(loaded)
        assert "value" not in snapshot and snapshot[KEY] == POLICY
    assert raw == before
