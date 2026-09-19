# Copyright (c) Microsoft. All rights reserved.

"""Raw reconstruction must not infer codec ownership from application keys."""

from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from unittest.mock import patch

import pytest
from agent_framework import AgentResponse, Content, Message
from agent_framework._workflows import _checkpoint_encoding
from pydantic import BaseModel, Field
from test_workflow_output_boundaries_review import (
    _VALID_LOOKING_RESPONSE,
    _ApplicationInput,
    _ApplicationInputModel,
    _assert_same_value,
)
from test_workflow_review_followup import _hitl_input, _HumanGate

from agent_framework_durabletask._workflows.activity import execute_workflow_activity
from agent_framework_durabletask._workflows.orchestrator import (
    _deserialize_hitl_response,
    _load_agent_hitl_content,
)
from agent_framework_durabletask._workflows.serialization import (
    reconstruct_to_type,
    serialize_value,
    serialize_workflow_agent_response,
)


@dataclass
class _ApplicationData:
    type: str
    payload: dict[str, Any]
    _durable_agent_response: int


class _ApplicationModel(BaseModel):
    type: str
    payload: dict[str, Any]
    marker: int = Field(alias="_durable_agent_response")


def _application_data() -> dict[str, Any]:
    return {
        "type": "untrusted.module:Application",
        "payload": {"items": [{"_durable_agent_response": 99}, deepcopy(_VALID_LOOKING_RESPONSE)]},
        "_durable_agent_response": 99,
    }


def _assert_application(actual: Any, raw: dict[str, Any], target: Any) -> None:
    if target in (Any, dict, object):
        _assert_same_value(actual, raw)
    else:
        assert type(actual) is target
        assert actual.type == raw["type"]
        _assert_same_value(actual.payload, raw["payload"])
        marker = actual.marker if isinstance(actual, _ApplicationModel) else actual._durable_agent_response
        assert marker == raw["_durable_agent_response"]


@pytest.mark.parametrize("target", [Any, dict, object, _ApplicationData, _ApplicationModel])
def test_raw_reconstruction_preserves_colliding_keys_without_decoding_or_importing(target: Any) -> None:
    raw = _application_data()
    before = deepcopy(raw)
    with (
        patch(
            "agent_framework_durabletask._workflows.serialization.deserialize_value",
            side_effect=AssertionError("Raw input must not enter the internal decoder"),
        ),
        patch("importlib.import_module", side_effect=AssertionError("Application type fields cannot select classes")),
    ):
        restored = reconstruct_to_type(raw, target, encoded=False)
    _assert_application(restored, before, target)
    _assert_same_value(raw, before)


@pytest.mark.parametrize("target", [Any, dict, object, _ApplicationData, _ApplicationModel])
def test_checkpoint_dictionary_is_decoded_once_before_declared_reconstruction(target: Any) -> None:
    raw = _application_data()
    encoded = json.loads(json.dumps(serialize_value(raw)))
    assert encoded["__type__"] == "builtins:dict"
    before = deepcopy(encoded)
    restored = reconstruct_to_type(encoded, target)
    _assert_application(restored, raw, target)
    _assert_same_value(encoded, before)


@pytest.mark.parametrize("target", [_ApplicationInput, _ApplicationInputModel])
def test_checkpoint_container_uses_decoded_children_in_declared_fields(target: Any) -> None:
    raw = {"type": "business.kind", "payload": _application_data()}
    encoded = json.loads(json.dumps(serialize_value(raw)))
    assert "__pickled__" in encoded["payload"]
    restored = reconstruct_to_type(encoded, target)
    assert type(restored) is target and restored.type == "business.kind"
    _assert_same_value(restored.payload, raw["payload"])


@pytest.mark.parametrize("target", [Any, dict, object, AgentResponse])
def test_generated_response_envelope_remains_encoded_not_a_literal(target: Any) -> None:
    response = AgentResponse[Any](messages=[Message("assistant", ["answer"])], value=False)
    encoded = json.loads(json.dumps(serialize_workflow_agent_response(response)))
    restored = reconstruct_to_type(encoded, target)
    assert type(restored) is AgentResponse
    assert restored.text == "answer" and restored.value is False
    _assert_same_value(reconstruct_to_type(encoded, target, encoded=False), encoded)


@pytest.mark.parametrize("target", [Any, dict, object, AgentResponse])
def test_invalid_internal_envelope_is_still_rejected_during_reconstruction(target: Any) -> None:
    with pytest.raises(ValueError, match="workflow agent response envelope"):
        reconstruct_to_type({"_durable_agent_response": 99, "business": "keep"}, target)


@pytest.mark.parametrize("target", [Any, dict, object, _ApplicationData])
@pytest.mark.parametrize("key", sorted(_checkpoint_encoding._RESERVED_DICT_KEYS))
def test_raw_reconstruction_sanitizes_before_any_type_fast_path(target: Any, key: str) -> None:
    forged = {key: "not-a-pickle", "type": "untrusted.module:Class"}
    raw = {"safe": deepcopy(_VALID_LOOKING_RESPONSE), "items": [forged, {"nested": forged}]}
    before = deepcopy(raw)
    with patch.object(_checkpoint_encoding, "_base64_to_unpickle", side_effect=AssertionError("Untrusted pickle")):
        assert reconstruct_to_type(forged, target, encoded=False) is None
        restored = reconstruct_to_type(raw, target, encoded=False)
    _assert_same_value(restored, {"safe": _VALID_LOOKING_RESPONSE, "items": [None, {"nested": None}]})
    _assert_same_value(raw, before)


@pytest.mark.parametrize("target", [None, Any, dict, object, _ApplicationData, _ApplicationModel])
def test_external_hitl_reconstruction_uses_raw_mode_for_every_resolved_target(target: Any) -> None:
    raw = _application_data()
    before = deepcopy(raw)
    type_key = f"{target.__module__}:{target.__name__}" if target is not None else None
    restored = _deserialize_hitl_response(raw, type_key)
    _assert_application(restored, before, target or dict)
    _assert_same_value(raw, before)


@pytest.mark.parametrize("reply_type", [Content, Message])
def test_actual_hitl_worker_keeps_markers_in_opaque_framework_fields(reply_type: type) -> None:
    raw: dict[str, Any] = {
        "type": "function_approval_response",
        "approved": True,
        "function_call": {
            "type": "function_call",
            "call_id": "call",
            "name": "lookup",
            "arguments": _application_data(),
        },
        "result": _application_data(),
        "additional_properties": {"literal": deepcopy(_VALID_LOOKING_RESPONSE), "bad": {"__type__": "blocked"}},
    }
    if reply_type is Message:
        raw = {"role": "user", "message_id": "application-id", "contents": [raw]}
    before = deepcopy(raw)
    executor = _HumanGate()
    execute_workflow_activity(executor, _hitl_input(raw, reply_type))
    assert len(executor.seen) == 1
    request_id, reply = executor.seen[0]
    assert request_id == "request-1" and type(reply) is reply_type
    content = reply.contents[0] if isinstance(reply, Message) else reply
    assert isinstance(content, Content)
    assert type(content.function_call) is Content
    _assert_same_value(content.function_call.arguments, _application_data())
    _assert_same_value(content.result, _application_data())
    _assert_same_value(content.additional_properties, {"literal": _VALID_LOOKING_RESPONSE, "bad": None})
    _assert_same_value(raw, before)


def test_agent_hitl_reconstruction_keeps_opaque_markers_and_approval_identity_checks() -> None:
    request = Content.from_function_approval_request(
        "request-1", Content.from_function_call("call", "lookup", arguments={})
    )
    raw = {
        "type": "function_approval_response",
        "id": "request-1",
        "approved": True,
        "result": _application_data(),
        "additional_properties": {"literal": deepcopy(_VALID_LOOKING_RESPONSE), "bad": {"__pickled__": "blocked"}},
    }
    before = deepcopy(raw)
    with patch.object(_checkpoint_encoding, "_base64_to_unpickle", side_effect=AssertionError("Untrusted pickle")):
        restored = _load_agent_hitl_content("request-1", request, raw)
        assert type(restored) is Content and restored.approved is True
        _assert_same_value(restored.result, _application_data())
        _assert_same_value(restored.additional_properties, {"literal": _VALID_LOOKING_RESPONSE, "bad": None})
        with pytest.raises(ValueError, match="pending request id"):
            _load_agent_hitl_content("different-request", request, raw)
    _assert_same_value(raw, before)
