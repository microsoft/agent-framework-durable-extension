# Copyright (c) Microsoft. All rights reserved.

"""Public Core input, canonical history and cold replay share one input contract."""

import hashlib
import json
from collections.abc import Awaitable, Mapping, Sequence
from copy import deepcopy
from typing import Any, ClassVar

import pytest
from agent_framework import Agent, AgentResponse, BaseChatClient, ChatMiddlewareLayer, ChatResponse, Content, Message
from test_private_history_pipeline import _bound, _CanonicalStateProvider

from agent_framework_durabletask import load_agent_response
from agent_framework_durabletask._history_provider import DurableHistoryProvider
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._response_utils import (
    preserve_input_envelope,
    serialize_input_content,
    serialize_input_message,
)
from agent_framework_durabletask._shared_agent_state import DurableAgentState, DurableAgentStateMessage
from agent_framework_durabletask._shared_response import serialize_terminal_response


def _json(value: Any) -> str:
    # Exact JSON distinguishes bool/int/float and positive/negative zero.
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _message(content: dict[str, Any]) -> Message:
    raw = {"role": "tool", "message_id": "same", "contents": [deepcopy(content)]}
    message = load_agent_response({"messages": [raw]}).messages[0]
    preserve_input_envelope(message, raw)
    return message


REVISIONS = [
    pytest.param({}, {"result": None}, id="omitted-null"),
    pytest.param({"result": False}, {"result": 0}, id="bool-int"),
    pytest.param({"result": 0}, {"result": 0.0}, id="int-float"),
    pytest.param({"result": 0.0}, {"result": -0.0}, id="signed-zero"),
    pytest.param({"result": {"answer": False}}, {"result": {"answer": 0}}, id="nested-bool-int"),
]


@pytest.mark.parametrize(("first", "second"), REVISIONS)
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("nested", [False, True])
def test_public_input_revisions_have_exact_contract_fingerprints(
    first: dict[str, Any], second: dict[str, Any], reverse: bool, nested: bool
) -> None:
    revisions = [second, first] if reverse else [first, second]
    fingerprints: list[str] = []
    for revision in revisions:
        content: dict[str, Any] = {"type": "function_result", "call_id": "call", **deepcopy(revision)}
        expected_content = {**deepcopy(content), "additional_properties": {}}
        if nested:
            content = {"type": "function_result", "call_id": "outer", "items": [content]}
            expected_content = {
                "type": "function_result",
                "call_id": "outer",
                "items": [expected_content],
                "additional_properties": {},
            }
        message = _message(content)
        expected = {
            "type": "message",
            "role": "tool",
            "message_id": "same",
            "contents": [expected_content],
            "additional_properties": {},
        }
        before = _json(content)
        fingerprint = message_identity(message)
        # The oracle is literal JSON, not a second call to the serializer being tested.
        assert fingerprint == hashlib.sha256(_json(expected).encode("utf-8")).hexdigest()
        assert _json(serialize_input_message(message)) == _json(expected)
        stored = DurableAgentStateMessage.from_core_dict({"role": "tool", "message_id": "same", "contents": [content]})
        assert stored.ingestion_identity == fingerprint
        assert message_identity(deepcopy(message)) == fingerprint
        assert _json(content) == before
        fingerprints.append(fingerprint)
    assert fingerprints[0] != fingerprints[1]


class _HiddenContent(Content):
    def to_dict(self, **kwargs: Any) -> dict[str, Any]:
        return Content.to_dict(self, exclude={"result", "arguments", "items", "inputs", "outputs", "function_call"})


class _HiddenMessage(Message):
    DEFAULT_EXCLUDE: ClassVar[set[str]] = Message.DEFAULT_EXCLUDE | {
        "role",
        "author_name",
        "message_id",
        "additional_properties",
    }

    def to_dict(self, **kwargs: Any) -> dict[str, Any]:
        return Message.to_dict(self, exclude={"contents"})


class _OutputClient(ChatMiddlewareLayer, BaseChatClient):
    STORES_BY_DEFAULT = False

    def __init__(self, output: Message) -> None:
        super().__init__(middleware=[])
        self.output = output
        self.received_messages: list[list[Message]] = []

    def _inner_get_response(
        self, *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
    ) -> Awaitable[ChatResponse]:
        assert not stream
        self.received_messages.append(deepcopy(list(messages)))

        async def get() -> ChatResponse:
            return ChatResponse(messages=[self.output], finish_reason="stop")

        return get()


def _cold(provider: _CanonicalStateProvider) -> _CanonicalStateProvider:
    cold = _CanonicalStateProvider()
    cold.state = DurableAgentState.from_json(provider.state.to_json())
    return cold


@pytest.mark.parametrize(("first", "second"), REVISIONS)
@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("nested", [False, True])
async def test_public_agent_inputs_keep_revisions_through_json_cold_replay(
    first: dict[str, Any], second: dict[str, Any], reverse: bool, nested: bool
) -> None:
    owner = _CanonicalStateProvider()
    expected: list[dict[str, Any]] = []
    for index, revision in enumerate([second, first] if reverse else [first, second]):
        content: dict[str, Any] = {"type": "function_result", "call_id": "call", **deepcopy(revision)}
        if nested:
            content = {"type": "function_result", "call_id": "outer", "items": [content]}
        history = DurableHistoryProvider()
        client = _OutputClient(Message("assistant", ["ack"]))
        agent = Agent(client=client, context_providers=[history])
        session = agent.create_session(session_id="same-session")
        with _bound(owner, f"revision-{index}"):
            await agent.run([_message(content)], session=session)
            history.flush(session.state[history.source_id])
        expected.append(revision)
        owner = _cold(owner)

    client = _OutputClient(Message("assistant", ["ack"]))
    history = DurableHistoryProvider()
    agent = Agent(client=client, context_providers=[history])
    with _bound(owner, "replay"):
        await agent.run("next", session=agent.create_session(session_id="same-session"))
    received = [message for message in client.received_messages[0] if message.message_id == "same"]
    assert len(received) == 2
    for message, revision in zip(received, expected, strict=True):
        replayed_content = message.contents[0]
        if nested:
            assert replayed_content.items is not None
            replayed_content = replayed_content.items[0]
        payload = serialize_input_content(replayed_content)
        assert ("result" in payload) is ("result" in revision)
        if "result" in revision:
            assert _json(payload["result"]) == _json(revision["result"])


@pytest.mark.parametrize("value", [None, False, 0, 0.0, -0.0, {"answer": False}])
@pytest.mark.parametrize("edge", ["result", "arguments", "items", "inputs", "outputs", "function_call"])
@pytest.mark.parametrize("per_call", [False, True])
async def test_hidden_public_fields_match_mailbox_transcript_and_cold_model_input(
    value: Any, edge: str, per_call: bool
) -> None:
    result = _HiddenContent("function_result", call_id="call", result=deepcopy(value), raw_representation=object())
    field = "result"
    if edge == "result":
        content = result
    elif edge == "arguments":
        field = "arguments"
        content = _HiddenContent("function_call", call_id="call", name="lookup", arguments={"answer": value})
    elif edge == "function_call":
        field = "arguments"
        content = _HiddenContent(
            "function_approval_response",
            approved=False,
            id="approval",
            function_call=_HiddenContent("function_call", call_id="call", name="lookup", arguments={"answer": value}),
        )
    elif edge == "outputs":
        content = _HiddenContent("code_interpreter_tool_result", call_id="call", outputs=[result])
    elif edge == "inputs":
        content = _HiddenContent("code_interpreter_tool_call", call_id="call", inputs=[result])
    else:
        content = _HiddenContent("function_result", call_id="outer", items=[result])
    expected = {"answer": value} if field == "arguments" else value
    output = _HiddenMessage(
        "tool",
        [content],
        message_id="output",
        author_name="tool-author",
        additional_properties={"public": False},
        raw_representation=object(),
    )
    owner = _CanonicalStateProvider()
    history = DurableHistoryProvider()
    client = _OutputClient(output)
    agent = Agent(client=client, context_providers=[history], require_per_service_call_history_persistence=per_call)
    session = agent.create_session(session_id="same-session")
    with _bound(owner, "first"):
        response = await agent.run("question", session=session)
        history.flush(session.state[history.source_id])
    owner.state.record_response("first", response, delivery_window_seconds=3600)
    owner = _cold(owner)
    delivered = owner.state.try_get_agent_response("first")
    assert delivered is not None
    stored = next(
        message
        for entry in owner.state.data.conversation_history
        for message in entry.messages
        if message.public_message_id == "output"
    )
    cold_client = _OutputClient(Message("assistant", ["next answer"]))
    cold_history = DurableHistoryProvider()
    cold_agent = Agent(client=cold_client, context_providers=[cold_history])
    with _bound(owner, "next"):
        await cold_agent.run("next", session=cold_agent.create_session(session_id="same-session"))
    replayed = next(message for message in cold_client.received_messages[0] if message.message_id == "output")
    for message in (delivered.messages[0], stored.to_chat_message(), replayed):
        assert message.role == "tool"
        assert message.author_name == "tool-author"
        assert message.additional_properties["public"] is False
        restored = message.contents[0]
        if edge in ("items", "inputs", "outputs"):
            restored = getattr(restored, edge)[0]
        elif edge == "function_call":
            assert restored.function_call is not None
            restored = restored.function_call
        assert _json(getattr(restored, field)) == _json(expected)
    assert "raw_representation" not in _json(owner.state.to_dict())
    if edge == "result":
        assert _json(stored.to_dict()["contents"][0]["result"]) == _json(expected)
    assert _json(result.result) == _json(value)


@pytest.mark.parametrize("entry_point", ["literal", "attached"])
def test_input_extras_remain_inert_and_raw_sdk_fields_are_not_persisted(entry_point: str) -> None:
    raw: dict[str, Any] = {
        "role": "tool",
        "message_id": "same",
        "futureMessage": {"type": "unregistered", "payload": [False, 0, 0.0]},
        "raw_representation": {"sdk": "message"},
        "contents": [
            {
                "type": "function_result",
                "call_id": "outer",
                "result": None,
                "raw_representation": {"sdk": "content"},
                "futureContent": {"type": "unregistered", "payload": [None, False]},
                "items": [
                    {
                        "type": "function_result",
                        "call_id": "inner",
                        "result": {"answer": False},
                        "raw_representation": {"sdk": "nested"},
                        "futureNested": {"value": 0.0},
                    }
                ],
            }
        ],
    }
    before = _json(raw)
    message = load_agent_response({"messages": [raw]}).messages[0]
    preserve_input_envelope(message, raw)
    stored = (
        DurableAgentStateMessage.from_core_dict(raw)
        if entry_point == "literal"
        else DurableAgentStateMessage.from_chat_message(message)
    )
    wire = stored.to_dict()
    assert wire["futureMessage"] == raw["futureMessage"]
    fields = wire["contents"][0]["pythonCoreFields"]["fields"]
    assert fields["futureContent"] == raw["contents"][0]["futureContent"]
    assert fields["items"][0]["futureNested"] == {"value": 0.0}
    assert "raw_representation" not in _json(wire)
    assert "raw_representation" not in _json(serialize_input_message(message))
    cold = DurableAgentStateMessage.from_dict(json.loads(_json(wire)))
    replayed = cold.to_chat_message()
    assert replayed.contents[0].result is None
    assert replayed.contents[0].items[0].result == {"answer": False}
    assert not hasattr(replayed, "futureMessage")
    assert _json(cold.to_dict()) == _json(wire)
    assert _json(raw) == before


@pytest.mark.parametrize(("first", "second"), REVISIONS[1:])
@pytest.mark.parametrize("reverse", [False, True])
def test_live_subclass_edits_override_attached_values_without_changing_input(
    first: dict[str, Any], second: dict[str, Any], reverse: bool
) -> None:
    original, changed = (second, first) if reverse else (first, second)
    raw: dict[str, Any] = {
        "role": "tool",
        "message_id": "same",
        "contents": [
            {
                "type": "function_result",
                "call_id": "call",
                **deepcopy(original),
                "future": {"keep": None},
            }
        ],
    }
    message = _HiddenMessage("tool", [_HiddenContent("function_result", call_id="call", **deepcopy(original))])
    message.message_id = "same"
    preserve_input_envelope(message, raw)
    initial = message_identity(message)
    message.contents[0].result = deepcopy(changed["result"])

    expected = {
        "type": "message",
        "role": "tool",
        "message_id": "same",
        "additional_properties": {},
        "contents": [
            {
                "type": "function_result",
                "call_id": "call",
                **deepcopy(changed),
                "future": {"keep": None},
                "additional_properties": {},
            }
        ],
    }
    assert message_identity(message) == hashlib.sha256(_json(expected).encode("utf-8")).hexdigest()
    assert message_identity(message) != initial
    stored = DurableAgentStateMessage.from_chat_message(message)
    assert _json(stored.to_dict()["contents"][0]["result"]) == _json(changed["result"])
    assert _json(raw["contents"][0]["result"]) == _json(original["result"])


# Literal schema fields are the oracle, not the projection helper being tested.
OPTIONAL_FIELDS: list[tuple[str, str, str, str, dict[str, Any], dict[str, Any], list[Any]]] = [
    (
        "function_call",
        "functionCall",
        "arguments",
        "arguments",
        {"call_id": "call", "name": "lookup"},
        {"callId": "call", "name": "lookup"},
        [{}, "", ' { "partial": ', {"answer": False}],
    ),
    ("error", "error", "message", "message", {}, {}, ["", "diagnostic"]),
    ("error", "error", "error_code", "errorCode", {}, {}, ["", "tool_failed"]),
    ("error", "error", "error_details", "details", {}, {}, [None, False, 0, {}]),
    (
        "function_result",
        "functionResult",
        "result",
        "result",
        {"call_id": "call"},
        {"callId": "call"},
        [None, False, 0, {}],
    ),
    ("data", "data", "media_type", "mediaType", {"uri": "urn:data"}, {"uri": "urn:data"}, ["", "image/png"]),
    ("uri", "uri", "media_type", "mediaType", {"uri": "urn:asset"}, {"uri": "urn:asset"}, ["", "image/png"]),
    ("text_reasoning", "reasoning", "text", "text", {}, {}, ["", "reasoning"]),
]
OPTIONAL_CASES = [
    pytest.param(
        {"type": core_kind, **core_required, **({core_field: value} if present else {})},
        {"$type": wire_kind, **wire_required, **({wire_field: value} if present else {})},
        id=f"{core_kind}-{core_field}-{'present-' + str(index) if present else 'omitted'}",
    )
    for core_kind, wire_kind, core_field, wire_field, core_required, wire_required, values in OPTIONAL_FIELDS
    for index, (present, value) in enumerate([(False, None), *((True, value) for value in values)])
]


@pytest.mark.parametrize(("core_content", "wire_content"), OPTIONAL_CASES)
@pytest.mark.parametrize("origin", ["shared", "shared-profile", "core-input"])
def test_cold_optional_content_presence_survives_forward_shared_delivery_and_identity(
    core_content: dict[str, Any], wire_content: dict[str, Any], origin: str
) -> None:
    expected_content = {**deepcopy(core_content), "additional_properties": {}}
    expected_message = {
        "type": "message",
        "role": "tool",
        "message_id": "same",
        "contents": [expected_content],
        "additional_properties": {},
    }
    fingerprint = hashlib.sha256(_json(expected_message).encode("utf-8")).hexdigest()
    if origin == "core-input":
        producer = _message(core_content)
        assert message_identity(producer) == fingerprint
        stored = DurableAgentStateMessage.from_chat_message(producer)
        assert stored.ingestion_identity == fingerprint
        raw_message = stored.to_dict()
    else:
        content = deepcopy(wire_content)
        if origin == "shared-profile":
            content["pythonCoreFields"] = {
                "profile": "agent-framework-python.core-fields",
                "version": 1,
                "fields": {},
            }
        raw_message = {"role": "tool", "messageId": "same", "contents": [content]}
    raw = {
        "schemaVersion": "2.0.0",
        "data": {
            "conversationHistory": [{"$type": "request", "messages": [raw_message]}],
            "terminalResults": {},
            "completionReceipts": {},
        },
    }
    before = _json(raw)
    state = DurableAgentState.from_json(before)
    for _ in range(2):
        stored = state.data.conversation_history[0].messages[0]
        projected = stored.to_chat_message()
        # Forward the real projected Content, not a literal stand-in for it.
        forwarded = serialize_terminal_response(AgentResponse(messages=[projected]))
        assert _json(forwarded["messages"][0]["contents"][0]) == _json({**wire_content, "extensionData": {}})
        assert _json(serialize_input_message(projected)) == _json(expected_message)
        assert message_identity(projected) == fingerprint
        assert _json(state.to_dict()) == before
        state = DurableAgentState.from_json(state.to_json())
    assert _json(raw) == before


@pytest.mark.parametrize(
    ("core_kind", "wire_kind", "core_field", "wire_field", "core_required", "wire_required", "values"),
    OPTIONAL_FIELDS,
)
def test_plain_core_optional_defaults_keep_existing_writer_policy_through_cold_delivery(
    core_kind: Any,
    wire_kind: str,
    core_field: str,
    wire_field: str,
    core_required: dict[str, Any],
    wire_required: dict[str, Any],
    values: list[Any],
) -> None:
    producer = Message("tool", [Content(core_kind, **core_required)], message_id="same")
    assert not hasattr(producer.contents[0], "_durable_original_core_content")
    stored = DurableAgentStateMessage.from_chat_message(producer)
    wire = stored.to_dict()
    nullable = core_field in ("result", "error_details")
    assert (wire_field in wire["contents"][0]) is nullable
    if nullable:
        assert wire["contents"][0][wire_field] is None
    projected = DurableAgentStateMessage.from_dict(json.loads(_json(wire))).to_chat_message()
    forwarded = serialize_terminal_response(AgentResponse(messages=[projected]))
    defaults = {"details": None} if wire_kind == "error" else {"result": None} if wire_kind == "functionResult" else {}
    expected = {"$type": wire_kind, **wire_required, **defaults, "extensionData": {}}
    assert _json(forwarded["messages"][0]["contents"][0]) == _json(expected)
    # Bare Core lacks an absence bit. Only its established canonical nullable
    # history defaults intentionally differ from the original Core fingerprint.
    assert (message_identity(projected) == message_identity(producer)) is (not defaults)
    assert _json(stored.to_dict()) == _json(wire)


@pytest.mark.parametrize(
    ("core_kind", "wire_kind", "core_field", "wire_field", "core_required", "wire_required", "values"),
    OPTIONAL_FIELDS,
)
def test_profile_cannot_supply_an_omitted_mapped_content_field(
    core_kind: str,
    wire_kind: str,
    core_field: str,
    wire_field: str,
    core_required: dict[str, Any],
    wire_required: dict[str, Any],
    values: list[Any],
) -> None:
    raw = {
        "role": "tool",
        "contents": [
            {
                "$type": wire_kind,
                **wire_required,
                "pythonCoreFields": {
                    "profile": "agent-framework-python.core-fields",
                    "version": 1,
                    "fields": {core_field: deepcopy(values[0])},
                },
            }
        ],
    }
    stored = DurableAgentStateMessage.from_dict(raw)
    with pytest.raises(ValueError, match="cannot replace known content fields"):
        stored.to_chat_message()
    assert _json(stored.to_dict()) == _json(raw)


@pytest.mark.parametrize(
    ("core_kind", "wire_kind", "core_field", "wire_field", "core_required", "wire_required", "values"),
    OPTIONAL_FIELDS,
)
@pytest.mark.parametrize("profiled", [False, True])
def test_current_optional_field_edits_control_cold_projection_without_new_null_policy(
    core_kind: str,
    wire_kind: str,
    core_field: str,
    wire_field: str,
    core_required: dict[str, Any],
    wire_required: dict[str, Any],
    values: list[Any],
    profiled: bool,
) -> None:
    content = {"$type": wire_kind, **wire_required, wire_field: deepcopy(values[-1])}
    if profiled:
        content["pythonCoreFields"] = {
            "profile": "agent-framework-python.core-fields",
            "version": 1,
            "fields": {},
        }
    raw = {"role": "tool", "messageId": "same", "contents": [content]}
    before = _json(raw)
    stored = DurableAgentStateMessage.from_dict(json.loads(before))
    original = message_identity(stored.to_chat_message())
    # These are edits to the typed history view, not a new absence/null policy.
    attribute = "details" if wire_field == "details" else core_field
    setattr(stored.contents[0], attribute, None)
    expected_content = {"$type": wire_kind, **wire_required, "extensionData": {}}
    if core_field in ("result", "error_details"):
        expected_content[wire_field] = None
    for _ in range(2):
        projected = stored.to_chat_message()
        forwarded = serialize_terminal_response(AgentResponse(messages=[projected]))
        assert _json(forwarded["messages"][0]["contents"][0]) == _json(expected_content)
        assert message_identity(projected) != original
        stored = DurableAgentStateMessage.from_dict(json.loads(_json(stored.to_dict())))
    assert _json(raw) == before


@pytest.mark.parametrize(
    ("wire_kind", "wire_field", "required"),
    [
        ("functionCall", "arguments", {"callId": "call", "name": "lookup"}),
        ("error", "message", {}),
        ("error", "errorCode", {}),
        ("data", "mediaType", {"uri": "urn:data"}),
        ("uri", "mediaType", {"uri": "urn:asset"}),
        ("reasoning", "text", {}),
    ],
)
def test_presence_projection_does_not_relax_shared_nonnullable_fields(
    wire_kind: str, wire_field: str, required: dict[str, Any]
) -> None:
    raw = {
        "schemaVersion": "2.0.0",
        "data": {
            "conversationHistory": [
                {
                    "$type": "request",
                    "messages": [{"role": "tool", "contents": [{"$type": wire_kind, **required, wire_field: None}]}],
                }
            ],
            "terminalResults": {},
            "completionReceipts": {},
        },
    }
    before = _json(raw)
    with pytest.raises(ValueError, match="must be"):
        DurableAgentState.from_json(before)
    assert _json(raw) == before
