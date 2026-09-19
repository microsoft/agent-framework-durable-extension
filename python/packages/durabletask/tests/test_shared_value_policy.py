# Copyright (c) Microsoft. All rights reserved.

"""Shared-value transport policy is explicit, lossless and restriction-only."""

import json
from copy import deepcopy
from typing import Any

import pytest
from agent_framework import AgentResponse, Content, Message
from durabletask.task import CompletableTask
from pydantic import BaseModel, Field

from agent_framework_durabletask import ensure_response_format, load_agent_response, serialize_agent_response
from agent_framework_durabletask._executors import DurableAgentTask
from agent_framework_durabletask._shared_response import load_terminal_response, serialize_terminal_response

KEY = "_durable_value_policy"
POLICY = {"profile": "agent-framework-python.shared-value", "version": 1}
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


@pytest.mark.parametrize("policy", MALFORMED)
def test_invalid_policy_fails_without_mutation(policy: Any) -> None:
    raw = {"type": "agent_response", KEY: policy, "value": {"answer": 1}}
    before = deepcopy(raw)
    with pytest.raises(ValueError, match="structured-value policy"):
        load_agent_response(raw)
    assert raw == before


@pytest.mark.parametrize("marked", [False, True])
@pytest.mark.parametrize("present", [False, True])
@pytest.mark.parametrize("precompleted", [False, True])
def test_value_marker_cannot_bypass_a_legacy_approval_format(marked: bool, present: bool, precompleted: bool) -> None:
    content = Content.from_function_approval_request("approval", Content.from_function_call("call", "tool"))
    raw = AgentResponse(messages=[Message("assistant", [content])]).to_dict()
    if marked:
        raw[KEY] = dict(POLICY)
    if present:
        raw["value"] = {"answer": "not an integer"}
    before = deepcopy(raw)
    with pytest.raises(ValueError):
        ensure_response_format(Answer, "correlation", load_agent_response(raw))
    child: CompletableTask[Any] = CompletableTask()
    if precompleted:
        child.complete(raw)
    task = DurableAgentTask(child, Answer, "correlation")
    if not precompleted:
        child.complete(raw)
    assert task.is_complete and task.is_failed
    assert raw == before


@pytest.mark.parametrize("location", ["value", "metadata"])
def test_marker_lookalikes_in_business_data_do_not_enable_policy(location: str) -> None:
    raw: dict[str, Any] = {"type": "agent_response", "messages": [{"role": "assistant", "contents": ['{"answer":1}']}]}
    if location == "value":
        raw["value"] = {KEY: {"version": "not-a-policy"}}
    else:
        raw["additional_properties"] = {KEY: {"version": "not-a-policy"}}
    response = load_agent_response(raw)
    assert KEY not in serialize_agent_response(response)
    assert getattr(response, "_durable_shared_value_snapshot", None) is None


@pytest.mark.parametrize("as_instance", [False, True])
def test_valid_policy_survives_explicit_shared_codec_conversion(as_instance: bool) -> None:
    raw = {"type": "agent_response", "messages": [], "value": {"answer": 1}, KEY: dict(POLICY)}
    source: Any = load_agent_response(raw) if as_instance else raw
    shared = serialize_terminal_response(source)
    loaded = load_terminal_response(shared)
    ensure_response_format(Answer, "correlation", loaded)
    assert loaded.value == Answer(answer=1)
    inline = serialize_agent_response(loaded)
    assert inline[KEY] == POLICY and inline["value"] == raw["value"]


@pytest.mark.parametrize("policy", MALFORMED)
def test_invalid_policy_in_shared_profile_fails_only_targeted_projection(policy: Any) -> None:
    raw = {"type": "agent_response", "messages": [], "value": {"answer": 1}, KEY: policy}
    shared = serialize_terminal_response(raw)
    before = deepcopy(shared)
    with pytest.raises(ValueError, match="structured-value policy"):
        load_terminal_response(shared)
    assert shared == before


@pytest.mark.parametrize("alias_mode", ["none", "asymmetric", "symmetric"])
def test_policy_survives_serialization_after_successful_typed_conversion(alias_mode: str) -> None:
    class Aliased(BaseModel):
        answer: int = Field(validation_alias="inputAnswer", serialization_alias="outputAnswer")

    class Symmetric(BaseModel):
        answer: int = Field(alias="aliasedAnswer")

    by_name = alias_mode != "none"
    models: dict[str, Any] = {"none": Answer, "asymmetric": Aliased, "symmetric": Symmetric}
    model = models[alias_mode]
    raw: dict[str, Any] = {"type": "agent_response", "messages": [], "value": {"answer": 1}, KEY: dict(POLICY)}
    if by_name:
        raw["_durable_value_by_name"] = True
    response = load_agent_response(raw)
    ensure_response_format(model, "correlation", response)
    for _ in range(2):
        inline = json.loads(json.dumps(serialize_agent_response(response)))
        assert inline[KEY] == POLICY and inline["value"] == {"answer": 1}
        response = load_agent_response(inline)
        ensure_response_format(model, "correlation", response)
        assert isinstance(response.value, model) and response.value.answer == 1


@pytest.mark.parametrize("mutation", ["change", "add", "remove", "false-to-zero"])
def test_serialization_cannot_change_shared_value_or_presence(mutation: str) -> None:
    raw: dict[str, Any] = {"type": "agent_response", "messages": [], KEY: dict(POLICY)}
    if mutation != "add":
        raw["value"] = False if mutation == "false-to-zero" else {"answer": 1}
    before = deepcopy(raw)
    response: AgentResponse[Any] = load_agent_response(raw)
    if mutation == "remove":
        response._value = None
        response._value_parsed = False
    else:
        response._value = 0 if mutation == "false-to-zero" else {"answer": 2}
        response._value_parsed = True
    with pytest.raises(ValueError, match="cannot preserve"):
        serialize_agent_response(response)
    assert raw == before
