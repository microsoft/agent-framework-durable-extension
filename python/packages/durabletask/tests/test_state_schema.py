# Copyright (c) Microsoft. All rights reserved.

"""Validate real versioned state against the shared transcript and delivery contract."""

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import jsonschema
import pytest
from agent_framework import AgentResponse, Message

from agent_framework_durabletask import (
    DurableAgentState,
    DurableAgentStateCompaction,
    DurableAgentStateErrorResponse,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
)
from agent_framework_durabletask._durable_agent_state import DurableAgentStateEntryJsonType
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._response_utils import serialize_agent_response
from agent_framework_durabletask._shared_response import load_terminal_response, serialize_terminal_response

SCHEMA_PATH = Path(__file__).resolve().parents[4] / "schemas" / "durable-agent-entity-state.json"


@pytest.fixture(scope="module")
def schema() -> dict[str, Any]:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def _validate(payload: dict[str, Any], schema: dict[str, Any]) -> None:
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(payload)


def _populated_state() -> DurableAgentState:
    """Build real version 2 transcript, delivery, ingestion and opaque session state."""
    now = datetime.now(tz=timezone.utc)
    request_message = Message(role="user", contents=["hello"], message_id="wf_input_0")
    request = DurableAgentStateRequest(
        correlation_id="c0",
        created_at=now,
        messages=[DurableAgentStateMessage.from_chat_message(request_message)],
    )
    core_response = AgentResponse(
        messages=[Message(role="assistant", contents=["hi"], author_name="writer", message_id="wf_writer_1")],
        response_id="response-0",
        agent_id="writer",
        created_at=now.isoformat(),
        finish_reason="stop",
        usage_details={"input_token_count": 2, "output_token_count": 1, "total_token_count": 3},
        additional_properties={"provider": {"metadata": [1, 2]}},
    )
    response = DurableAgentStateResponse.from_run_response("c0", core_response)
    # Annotations are what carry compaction state across a round-trip.
    response.messages[0].extension_data = {"_excluded": True, "_excluded_reason": "sliding_window"}

    state = DurableAgentState()
    state.data.conversation_history.extend([request, response])
    state.data.session = {"type": "session", "session_id": "@dafx-writer@run-1", "state": {"compaction": {}}}
    state.data.ingested_messages = {"wf_input_0": [message_identity(request_message)], "legacy-known-id": None}
    state.record_response("c0", core_response, delivery_window_seconds=60, now=now)
    return state


def test_the_schema_itself_is_valid(schema: dict[str, Any]) -> None:
    jsonschema.Draft202012Validator.check_schema(schema)


def test_empty_state_validates(schema: dict[str, Any]) -> None:
    _validate(DurableAgentState().to_dict(), schema)


def test_populated_state_validates(schema: dict[str, Any]) -> None:
    _validate(_populated_state().to_dict(), schema)


def test_message_identity_and_annotations_are_declared(schema: dict[str, Any]) -> None:
    """Both are load-bearing for compaction, so an implementer must be told to round-trip them."""
    properties = schema["$defs"]["chatMessage"]["properties"]

    assert "messageId" in properties
    assert "extensionData" in properties


def test_shared_delivery_fields_are_declared(schema: dict[str, Any]) -> None:
    properties = schema["$defs"]["data"]["properties"]
    assert {"terminalResults", "completionReceipts", "ingestedPositions"} <= properties.keys()
    assert "ingestedMessages" not in properties


def test_session_is_left_opaque(schema: dict[str, Any]) -> None:
    """The two runtimes serialize sessions differently, so the shared schema must not fix a shape.

    .NET produces ``conversationId`` plus ``stateBag``. Python produces ``session_id``,
    ``service_session_id`` and ``state``. Declaring either one would make the other invalid.
    """
    session = schema["$defs"]["data"]["properties"]["session"]

    assert "properties" not in session, "the schema pins one runtime's session shape"

    dotnet_shaped = {
        "schemaVersion": DurableAgentState.SCHEMA_VERSION,
        "data": {
            "conversationHistory": [],
            "terminalResults": {},
            "completionReceipts": {},
            "session": {"conversationId": "abc", "stateBag": {}},
        },
    }
    _validate(dotnet_shaped, schema)


def test_state_survives_a_round_trip_through_the_schema(schema: dict[str, Any]) -> None:
    """Serialize, validate, restore, and confirm the compaction-critical fields came back."""
    payload = _populated_state().to_dict()
    _validate(payload, schema)

    restored = DurableAgentState.from_dict(payload)
    stored = restored.data.conversation_history[1].messages[0]

    assert stored.message_id == "wf_writer_1"
    assert (stored.extension_data or {}).get("_excluded") is True
    assert payload["data"]["pythonIngestion"]["profile"] == "agent-framework-python.ingestion"
    assert type(payload["data"]["pythonIngestion"]["version"]) is int
    assert payload["data"]["pythonIngestion"]["version"] == 1
    assert "ingestedMessages" not in payload["data"]
    assert restored.data.ingested_messages == payload["data"]["pythonIngestion"]["messages"]
    delivered = restored.try_get_agent_response("c0")
    assert isinstance(delivered, AgentResponse)
    wire = payload["data"]["terminalResults"]["c0"]["response"]
    assert serialize_terminal_response(delivered) == wire
    assert serialize_agent_response(delivered) == serialize_agent_response(load_terminal_response(wire))


def _entry_of_each_kind() -> DurableAgentState:
    """State containing each known entry kind, including compaction without a correlation."""
    now = datetime.now(tz=timezone.utc)
    state = _populated_state()
    state.data.conversation_history.append(
        DurableAgentStateErrorResponse(
            correlation_id="c1",
            created_at=now,
            messages=[
                DurableAgentStateMessage.from_chat_message(
                    Message(role="assistant", contents=["it broke"], message_id="err0")
                )
            ],
        )
    )
    state.data.conversation_history.append(
        DurableAgentStateCompaction(
            created_at=now,
            messages=[
                DurableAgentStateMessage.from_chat_message(
                    Message(role="assistant", contents=["summary"], message_id="sum0")
                )
            ],
        )
    )
    return state


def test_every_entry_kind_validates(schema: dict[str, Any]) -> None:
    payload = _entry_of_each_kind().to_dict()

    _validate(payload, schema)

    kinds = {entry["$type"] for entry in payload["data"]["conversationHistory"]}
    assert kinds == {kind.value for kind in DurableAgentStateEntryJsonType}


def test_an_entry_without_a_correlation_omits_the_field(schema: dict[str, Any]) -> None:
    """A compaction entry answers no request, so it has no correlation to record.

    Written as an absent field rather than an explicit null. `null` would type the field as
    something other than a string wherever a reader looks at it, which the schema rejects and
    which a stricter cross-language reader would too.
    """
    payload = _entry_of_each_kind().to_dict()

    compaction = next(e for e in payload["data"]["conversationHistory"] if e["$type"] == "compaction")

    assert "correlationId" not in compaction
    _validate(payload, schema)


def test_the_discriminator_is_required(schema: dict[str, Any]) -> None:
    payload = _populated_state().to_dict()
    del payload["data"]["conversationHistory"][0]["$type"]
    with pytest.raises(jsonschema.ValidationError):
        _validate(payload, schema)


def test_unknown_sibling_properties_validate_and_round_trip(schema: dict[str, Any]) -> None:
    payload = _populated_state().to_dict()
    payload["futureRoot"] = {"nested": [1, {"opaque": True}]}
    payload["data"]["futureData"] = {"nested": [None, "keep"]}
    payload["data"]["conversationHistory"][0]["futureEntry"] = {"nested": [2, 3]}
    payload["data"]["terminalResults"]["c0"]["futureDelivery"] = {"nested": [4, 5]}
    payload["data"]["completionReceipts"]["c0"]["futureReceipt"] = {"nested": [6, 7]}
    message = payload["data"]["terminalResults"]["c0"]["response"]["messages"][0]
    message["futureMessage"] = {"keep": None}
    message["contents"][0]["futureContent"] = [False, 0]
    _validate(payload, schema)
    restored = DurableAgentState.from_json(json.dumps(payload))
    assert restored.to_dict() == payload


def test_entry_union_contains_exactly_the_known_discriminators(schema: dict[str, Any]) -> None:
    kinds = {kind.value for kind in DurableAgentStateEntryJsonType}
    entries = schema["allOf"][0]["then"]["properties"]["data"]["properties"]["conversationHistory"]["items"]["oneOf"]
    typed_kinds = {schema["$defs"][entry["$ref"].split("/")[-1]]["properties"]["$type"]["const"] for entry in entries}
    assert typed_kinds == kinds


@pytest.mark.parametrize("kind", list(DurableAgentStateEntryJsonType))
@pytest.mark.parametrize(
    "invalid_fields",
    [
        {"messages": "not-an-array"},
        {"messages": [{"contents": []}]},
        {"correlationId": 17},
        {"createdAt": False},
    ],
)
def test_known_entries_cannot_bypass_their_contract_as_opaque_entries(
    schema: dict[str, Any], kind: DurableAgentStateEntryJsonType, invalid_fields: dict[str, Any]
) -> None:
    payload = DurableAgentState().to_dict()
    payload["data"]["conversationHistory"] = [{"$type": kind.value, **invalid_fields}]
    with pytest.raises(jsonschema.ValidationError):
        _validate(payload, schema)


@pytest.mark.parametrize("kind", [None, False, 0, "", [], {}, "futureKind"])
def test_unsupported_entry_discriminators_are_rejected(schema: dict[str, Any], kind: Any) -> None:
    payload = DurableAgentState().to_dict()
    payload["data"]["conversationHistory"] = [{"$type": kind}]
    with pytest.raises(jsonschema.ValidationError):
        _validate(payload, schema)
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(payload)


def test_exact_schema_versions_include_the_version_two_writer(schema: dict[str, Any]) -> None:
    assert schema["properties"]["schemaVersion"]["enum"] == ["1.0.0", "1.1.0", "1.2.0", "2.0.0"]
    assert DurableAgentState.SCHEMA_VERSION == "2.0.0"


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0", "2.0.0"])
def test_reader_versions_remain_valid_without_implicit_upgrade(schema: dict[str, Any], version: str) -> None:
    payload: dict[str, Any] = {"schemaVersion": version, "data": {"conversationHistory": []}}
    if version == "2.0.0":
        payload["data"].update(terminalResults={}, completionReceipts={})
    _validate(payload, schema)
    state = DurableAgentState.from_json(json.dumps(payload))
    assert state.to_dict() == payload
    if version != "2.0.0":
        with pytest.raises(ValueError, match="read-only"):
            state.prepare_for_write(delivery_window_seconds=60)
        assert state.to_dict() == payload


@pytest.mark.parametrize(
    "version", [None, False, 2, "", "0.1.0", "1.3.0", "2.7.3", "3.0.0", "2.0", "2.0.0-preview", "2.0.0\n"]
)
def test_schema_rejects_unsupported_or_malformed_versions(schema: dict[str, Any], version: Any) -> None:
    payload = DurableAgentState().to_dict()
    payload["schemaVersion"] = version
    with pytest.raises(jsonschema.ValidationError):
        _validate(payload, schema)


@pytest.mark.parametrize("field", ["schemaVersion", "data"])
def test_root_fields_are_required(schema: dict[str, Any], field: str) -> None:
    payload = DurableAgentState().to_dict()
    del payload[field]
    with pytest.raises(jsonschema.ValidationError):
        _validate(payload, schema)


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
def test_legacy_scalar_positions_remain_readable(schema: dict[str, Any], version: str) -> None:
    payload = {"schemaVersion": version, "data": {"conversationHistory": [], "ingestedPositions": {"executor": 3}}}
    _validate(payload, schema)
    assert DurableAgentState.from_json(json.dumps(payload)).to_dict() == payload


@pytest.mark.parametrize("field", ["terminalResults", "completionReceipts"])
@pytest.mark.parametrize("value", [None, False, 0, "", [], "not-an-object"])
def test_delivery_containers_are_typed(schema: dict[str, Any], field: str, value: Any) -> None:
    payload = DurableAgentState().to_dict()
    payload["data"][field] = value
    with pytest.raises(jsonschema.ValidationError):
        _validate(payload, schema)


@pytest.mark.parametrize(
    ("record_name", "required_field"),
    [
        ("terminalResults", "response"),
        ("terminalResults", "correlationId"),
        ("terminalResults", "outcome"),
        ("terminalResults", "completedAt"),
        ("completionReceipts", "correlationId"),
        ("completionReceipts", "outcome"),
        ("completionReceipts", "completedAt"),
        ("completionReceipts", "resultState"),
    ],
)
def test_delivery_record_fields_are_required(schema: dict[str, Any], record_name: str, required_field: str) -> None:
    payload = _populated_state().to_dict()
    del payload["data"][record_name]["c0"][required_field]
    with pytest.raises(jsonschema.ValidationError):
        _validate(payload, schema)


@pytest.mark.parametrize(
    ("record_name", "field"),
    [
        ("terminalResults", "completedAt"),
        ("terminalResults", "resultExpiresAt"),
        ("completionReceipts", "completedAt"),
        ("completionReceipts", "resultExpiresAt"),
    ],
)
@pytest.mark.parametrize("value", [None, False, 0, "not-a-timestamp"])
def test_delivery_timestamps_are_validated(schema: dict[str, Any], record_name: str, field: str, value: Any) -> None:
    payload = _populated_state().to_dict()
    payload["data"][record_name]["c0"][field] = value
    structural = jsonschema.Draft202012Validator(schema)
    if isinstance(value, str):
        # date-time is an annotation. FormatChecker also needs the optional RFC3339 checker.
        # Runtime readers must reject malformed strings independently of that extra.
        structural.validate(payload)
    else:
        with pytest.raises(jsonschema.ValidationError):
            structural.validate(payload)
        with pytest.raises(jsonschema.ValidationError):
            _validate(payload, schema)
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(payload)
    with pytest.raises(ValueError):
        DurableAgentState.from_json(json.dumps(payload))


@pytest.mark.parametrize("field", ["conversationHistory", "terminalResults", "completionReceipts"])
def test_version_two_requires_all_shared_data_containers(schema: dict[str, Any], field: str) -> None:
    payload = _populated_state().to_dict()
    del payload["data"][field]
    with pytest.raises(jsonschema.ValidationError):
        _validate(payload, schema)


@pytest.mark.parametrize(
    "response",
    [None, [], "{}", {}, {"messages": [{"role": "assistant", "contents": [{"type": "text", "text": "Core"}]}]}],
)
def test_terminal_result_requires_shared_response_json(schema: dict[str, Any], response: Any) -> None:
    payload = _populated_state().to_dict()
    payload["data"]["terminalResults"]["c0"]["response"] = response
    with pytest.raises(jsonschema.ValidationError):
        _validate(payload, schema)


@pytest.mark.parametrize("outcome", [None, "unknown", "", "success", "FAILED", False, 0, [], {}])
@pytest.mark.parametrize("container", ["terminalResults", "completionReceipts"])
def test_known_outcome_is_required(schema: dict[str, Any], outcome: Any, container: str) -> None:
    payload = _populated_state().to_dict()
    payload["data"][container]["c0"]["outcome"] = outcome
    with pytest.raises(jsonschema.ValidationError):
        _validate(payload, schema)


@pytest.mark.parametrize("opaque", [None, False, 0, "owner-value", [], {"id": "not-a-fingerprint-list"}])
def test_unprofiled_ingestion_is_opaque_not_exact_delivery_evidence(schema: dict[str, Any], opaque: Any) -> None:
    payload = DurableAgentState().to_dict()
    payload["data"]["ingestedMessages"] = opaque
    _validate(payload, schema)
    state = DurableAgentState.from_json(json.dumps(payload))
    assert state.data.ingested_messages == {}
    assert state.to_dict() == payload


@pytest.mark.parametrize("location", ["transcript", "terminal"])
def test_unknown_wire_discriminators_require_an_explicit_unknown_wrapper(schema: dict[str, Any], location: str) -> None:
    payload = _populated_state().to_dict()
    messages = (
        payload["data"]["conversationHistory"][0]["messages"]
        if location == "transcript"
        else payload["data"]["terminalResults"]["c0"]["response"]["messages"]
    )
    messages[0]["contents"][0] = {"$type": "futureContent", "payload": None}
    with pytest.raises(jsonschema.ValidationError):
        _validate(payload, schema)
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(payload)
    messages[0]["contents"][0] = {"$type": "unknown", "content": {"$type": "futureContent", "payload": None}}
    _validate(payload, schema)
    assert DurableAgentState.from_dict(payload).to_dict() == payload


def test_opaque_session_preserves_owner_message_shaped_data(schema: dict[str, Any]) -> None:
    session = {"owner": "external", "state": {"provider": {"messages": [{"custom": "keep"}], "cursor": [1, 2]}}}
    payload = DurableAgentState().to_dict()
    payload["data"]["session"] = session
    original = deepcopy(payload)
    _validate(payload, schema)
    assert DurableAgentState.from_json(json.dumps(payload)).to_dict() == original
