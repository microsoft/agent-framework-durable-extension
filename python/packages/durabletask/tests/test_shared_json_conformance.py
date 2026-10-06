# Copyright (c) Microsoft. All rights reserved.

"""Declared JSON boundaries, independent of SDK projection and writer activation."""

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

from agent_framework_durabletask import DurableAgentState, LegacyDurableAgentState, read_agent_state
from agent_framework_durabletask._durable_agent_state import DurableAgentStateUriContent as LegacyUriContent
from agent_framework_durabletask._shared_agent_state import DurableAgentStateMessage
from agent_framework_durabletask._shared_state_validation import validate_shared_state

SCHEMAS = Path(__file__).resolve().parents[4] / "schemas"
SCHEMA = json.loads((SCHEMAS / "durable-agent-entity-state.json").read_text(encoding="utf-8"))
REGISTRY = Registry().with_resource(SCHEMA["$id"], Resource.from_contents(SCHEMA))
CORPUS = [
    pytest.param(group["schema"], case, id=f"{path.name}/{group['description']}/{case['description']}")
    for path in sorted((SCHEMAS / "tests").glob("*-cases.json"))
    for group in json.loads(path.read_text(encoding="utf-8"))
    for case in group["tests"]
]
CONTENT_CASES = [
    pytest.param(case["data"], id=f"{group['description']}/{case['description']}")
    for group in json.loads((SCHEMAS / "tests" / "content-metadata-cases.json").read_text(encoding="utf-8"))
    for case in group["tests"]
    if case["valid"]
]
VERSIONS = ("1.0.0", "1.1.0", "1.2.0", "2.0.0")


def _encoded(value: Any) -> str:
    return json.dumps(value, sort_keys=True, allow_nan=False)


def _state(version: str, message: dict[str, Any]) -> dict[str, Any]:
    data: dict[str, Any] = {
        "conversationHistory": [
            {
                "$type": "request",
                "createdAt": "2026-09-16T00:00:00Z",
                "correlationId": "synthetic-request",
                "messages": [deepcopy(message)],
            }
        ],
    }
    if version == "2.0.0":
        data.update(terminalResults={}, completionReceipts={})
    return {"schemaVersion": version, "data": data}


@pytest.mark.parametrize(("schema", "case"), CORPUS)
def test_local_json_schema_corpus(schema: dict[str, Any], case: dict[str, Any]) -> None:
    Draft202012Validator.check_schema(SCHEMA)
    validator = Draft202012Validator(schema, registry=REGISTRY, format_checker=FormatChecker())
    assert validator.is_valid(case["data"]) is case["valid"]


@pytest.mark.parametrize("content", CONTENT_CASES)
def test_content_metadata_survives_real_core_projection_and_cold_shared_state(content: dict[str, Any]) -> None:
    message = {
        "role": "assistant",
        "messageId": "synthetic-message",
        "extensionData": {"messageMetadata": [None, False]},
        "futureMessage": {"inert": 2**80},
        "contents": [deepcopy(content), {"$type": "text", "text": "second"}],
    }
    raw = _state("2.0.0", message)
    before = _encoded(raw)
    validate_shared_state(raw)
    Draft202012Validator(SCHEMA).validate(raw)
    stored = DurableAgentStateMessage.from_dict(message)
    runtime = stored.to_chat_message()
    assert runtime.message_id == "synthetic-message"
    assert runtime.role == "assistant"
    assert len(runtime.contents) == 2 and runtime.contents[1].text == "second"
    metadata = content.get("extensionData")
    if content["$type"] == "unknown":
        # The unprofiled Core wrapper projects its payload, not opaque wire metadata.
        assert runtime.contents[0].additional_properties == {"content": content["content"]}
    elif isinstance(metadata, dict):
        assert _encoded(runtime.contents[0].additional_properties) == _encoded(metadata)
    else:
        assert not runtime.contents[0].additional_properties
    assert "futureSibling" not in (runtime.contents[0].additional_properties or {})
    assert _encoded(stored.to_dict()) == _encoded(message)
    mutable = DurableAgentState.from_dict(raw)
    assert _encoded(mutable.to_dict()) == before
    cold = read_agent_state(mutable.to_json())
    assert _encoded(cold.to_dict()) == before
    assert _encoded(json.loads(cold.to_json())) == before
    assert _encoded(raw) == before


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("role", ["developer", "", "producer-defined-role"])
def test_versioned_roles_remain_literal_not_mapped(version: str, role: str) -> None:
    raw = _state(version, {"role": role, "messageId": "synthetic-message", "contents": []})
    before = _encoded(raw)
    valid = version != "2.0.0" or role == "developer"
    assert Draft202012Validator(SCHEMA).is_valid(raw) is valid
    if valid:
        validate_shared_state(raw)
        loaded = read_agent_state(raw)
        message = loaded.to_dict()["data"]["conversationHistory"][0]["messages"][0]
        assert message["role"] == role
        if isinstance(loaded, LegacyDurableAgentState):
            runtime = loaded.data.conversation_history[0].messages[0].to_chat_message()
            assert runtime.role == role
            assert "messageId" not in message and runtime.message_id is None
        else:
            assert message["messageId"] == "synthetic-message"
    else:
        with pytest.raises(ValueError, match="role"):
            validate_shared_state(raw)
        with pytest.raises(ValueError, match="role"):
            read_agent_state(raw)
    assert _encoded(raw) == before


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("arguments", ["", ' { "partial": ', {"value": [None, False, 2**63 - 1]}])
def test_versioned_arguments_preserve_wire_form_and_characterize_legacy_projection(
    version: str, arguments: Any
) -> None:
    raw = _state(
        version,
        {
            "role": "assistant",
            "messageId": "synthetic-message",
            "contents": [{"$type": "functionCall", "callId": "synthetic-call", "name": "f", "arguments": arguments}],
        },
    )
    before = _encoded(raw)
    validate_shared_state(raw)
    Draft202012Validator(SCHEMA).validate(raw)
    loaded = read_agent_state(raw)
    message = loaded.to_dict()["data"]["conversationHistory"][0]["messages"][0]
    assert _encoded(message["contents"][0]["arguments"]) == _encoded(arguments)
    if isinstance(loaded, LegacyDurableAgentState):
        runtime = loaded.data.conversation_history[0].messages[0].to_chat_message().contents[0]
        # The historical SDK projection re-encodes arguments; it is not lossless.
        assert runtime.arguments == json.dumps(arguments)
    else:
        runtime = DurableAgentStateMessage.from_dict(message).to_chat_message().contents[0]
        assert _encoded(runtime.arguments) == _encoded(arguments)
    assert runtime.call_id == "synthetic-call" and runtime.name == "f"
    assert _encoded(raw) == before


@pytest.mark.parametrize("version", VERSIONS)
def test_uri_without_media_type_is_valid_without_inventing_wire_metadata(version: str) -> None:
    raw = _state(
        version,
        {
            "role": "assistant",
            "messageId": "synthetic-message",
            "contents": [{"$type": "uri", "uri": "urn:synthetic"}],
        },
    )
    before = _encoded(raw)
    validate_shared_state(raw)
    Draft202012Validator(SCHEMA).validate(raw)
    loaded = read_agent_state(raw)
    if isinstance(loaded, LegacyDurableAgentState):
        content = loaded.data.conversation_history[0].messages[0].contents[0]
        assert isinstance(content, LegacyUriContent)
        assert content.uri == "urn:synthetic"
        assert content.to_ai_content().uri == "urn:synthetic"
        assert content.to_dict()["mediaType"] == ""
    else:
        message = loaded.to_dict()["data"]["conversationHistory"][0]["messages"][0]
        assert "mediaType" not in message["contents"][0]
        assert DurableAgentStateMessage.from_dict(message).to_chat_message().contents[0].media_type is None
    assert _encoded(raw) == before


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize(
    "message",
    [
        {"contents": []},
        {"role": None},
        {"role": False},
        {"role": 0},
        {"role": []},
        {"role": {}},
        {"role": "assistant", "contents": [{"$type": "functionCall", "callId": "c", "name": "f", "arguments": []}]},
        {"role": "assistant", "contents": [{"$type": "functionCall", "callId": "c", "name": "f", "arguments": None}]},
        {"role": "assistant", "contents": [{"$type": "uri", "uri": False}]},
        {"role": "assistant", "contents": [{"$type": "uri", "uri": "urn:synthetic", "mediaType": None}]},
    ],
)
def test_legacy_alignment_does_not_admit_invalid_known_fields(version: str, message: dict[str, Any]) -> None:
    raw = _state(version, message)
    before = _encoded(raw)
    assert not Draft202012Validator(SCHEMA).is_valid(raw)
    with pytest.raises(ValueError):
        validate_shared_state(raw)
    assert _encoded(raw) == before


@pytest.mark.parametrize("version", VERSIONS)
@pytest.mark.parametrize("location", ["root", "data", "entry", "message", "usage", "response"])
@pytest.mark.parametrize("metadata", [None, [], "", 0, False])
def test_noncontent_metadata_remains_object_only(version: str, location: str, metadata: Any) -> None:
    raw = _state(version, {"role": "assistant", "contents": []})
    entry = raw["data"]["conversationHistory"][0]
    targets = {"root": raw, "data": raw["data"], "entry": entry, "message": entry["messages"][0]}
    if location == "usage":
        entry["$type"] = "response"
        targets["usage"] = entry["usage"] = {}
    elif location == "response":
        if version != "2.0.0":
            # Legacy responses are entries, not v2 terminal-response objects.
            entry["$type"] = "response"
            targets["response"] = entry
        else:
            common = {
                "correlationId": "synthetic-request",
                "outcome": "succeeded",
                "completedAt": "2026-09-16T00:00:00Z",
            }
            targets["response"] = {"messages": []}
            raw["data"]["terminalResults"]["synthetic-request"] = {**common, "response": targets["response"]}
            raw["data"]["completionReceipts"]["synthetic-request"] = {**common, "resultState": "available"}
    targets[location]["extensionData"] = metadata
    before = _encoded(raw)
    assert not Draft202012Validator(SCHEMA).is_valid(raw)
    with pytest.raises(ValueError):
        validate_shared_state(raw)
    assert _encoded(raw) == before
