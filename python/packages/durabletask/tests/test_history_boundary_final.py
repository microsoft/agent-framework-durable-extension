# Copyright (c) Microsoft. All rights reserved.

"""Mailbox read compatibility and occurrence-local input envelopes through real core runs."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from agent_framework import Agent, AgentResponse, Content, ContextProvider, Message, SessionContext
from test_durable_history_provider import _ingestion_messages
from test_history_admission_followup import _RESERVED_EXTRAS, _OutputExtraClient
from test_history_identity_acceptance import _assert_no_private_fields, _core_fields, _history_id, _rows, _wire
from test_history_pipeline_revision import NonStreamingAgent, ToolChatClient
from test_revision_contract import JsonStateProvider

from agent_framework_durabletask import AgentEntity, DurableAgentState, DurableHistoryProvider
from agent_framework_durabletask._durable_agent_state import DurableAgentStateMessage
from agent_framework_durabletask._response_utils import load_agent_response
from agent_framework_durabletask._shared_response import load_terminal_response, serialize_terminal_response


def _assert_filtered_delivery(response: AgentResponse[Any], expected: dict[str, Any]) -> None:
    assert type(response) is AgentResponse
    assert response.to_dict() == expected
    assert len(response.messages) == 1
    message = response.messages[0]
    assert type(message) is Message
    assert message.message_id == "real-output-id"
    assert not hasattr(message, "originalMessageId")
    assert not hasattr(message, "original_message_id")
    assert not hasattr(message, "future_message")
    _assert_no_private_fields(response.to_dict())


async def _assert_cold_mailbox_and_duplicate(
    raw: dict[str, Any], request: dict[str, Any], client: ToolChatClient, agent: Agent
) -> None:
    before = deepcopy(raw)
    payload = raw["data"]["terminalResults"][request["correlationId"]]["response"]
    expected = _wire(load_terminal_response(payload).to_dict())
    calls = len(client.received_messages)

    # Read the committed JSON, not the writer's already-cached DurableAgentState.
    restored = DurableAgentState.from_dict(_wire(raw))
    assert restored.to_dict() == before
    delivered = restored.try_get_agent_response(request["correlationId"])
    assert delivered is not None
    _assert_filtered_delivery(delivered, expected)
    assert delivered.messages[0].additional_properties == payload["messages"][0]["extensionData"]
    delivered.messages[0].contents[0].text = "consumer-only mutation"
    delivered.messages[0].additional_properties["model_metadata"]["tags"].append("consumer")
    assert restored.to_dict() == before and raw == before
    again = restored.try_get_agent_response(request["correlationId"])
    assert again is not None
    _assert_filtered_delivery(again, expected)

    cold = JsonStateProvider(_wire(raw))
    duplicate = await AgentEntity(agent, state_provider=cold).run(deepcopy(request))
    _assert_filtered_delivery(duplicate, expected)
    assert len(client.received_messages) == calls, "a retained completion must not call the model again"
    assert cold.writes == 0 and cold.raw == before and cold.state.to_dict() == before
    duplicate.messages[0].additional_properties["model_metadata"]["tags"].append("duplicate-consumer")
    assert cold.state.to_dict() == before and raw == before


@pytest.mark.parametrize("value", [{}, "opaque-not-the-public-id"], ids=["object", "string"])
@pytest.mark.parametrize("ownership", ["service-store-true", "local-store-outputs-false"])
@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
async def test_response_alias_extra_is_inert_in_mailbox_when_local_outputs_are_disabled(
    value: Any, ownership: str, per_call: bool
) -> None:
    service_owned = ownership == "service-store-true"
    client = _OutputExtraClient(deepcopy(value))
    history = DurableHistoryProvider(store_outputs=service_owned, prune_excluded=False)
    agent = NonStreamingAgent(
        client=client,
        context_providers=[history],
        require_per_service_call_history_persistence=per_call,
    )
    provider = JsonStateProvider()
    request = {
        "message": "answer once",
        "correlationId": "mailbox-extra",
        "options": {"store": service_owned},
    }
    before_request = deepcopy(request)

    response = await AgentEntity(agent, state_provider=provider).run(request)

    assert response.text == "answer-1" and response.additional_properties.get("durable_status") != "error"
    assert len(client.received_messages) == 1 and client.output is not None
    assert client.received_options[0]["store"] is service_owned
    original_output = _wire(client.output.to_dict())
    assert original_output["message_id"] == "real-output-id"
    assert original_output["originalMessageId"] == value
    assert response.messages[0].to_dict() == original_output
    assert provider.writes == 1 and request == before_request
    rows = _rows(provider)
    assert [row["role"] for row in rows] == ([] if service_owned else ["user"])
    assert all(entry["$type"] == "request" for entry in provider.raw["data"]["conversationHistory"])
    assert provider.raw["data"]["completionReceipts"]["mailbox-extra"]["outcome"] == "succeeded"
    mailbox = provider.raw["data"]["terminalResults"]["mailbox-extra"]["response"]
    expected_message = serialize_terminal_response({"type": "agent_response", "messages": [original_output]})
    assert mailbox["messages"] == expected_message["messages"]
    saved_message = mailbox["messages"][0]
    assert saved_message["messageId"] == "real-output-id" and "message_id" not in saved_message
    assert saved_message["contents"][0]["$type"] == "text"
    assert _core_fields(saved_message)["originalMessageId"] == value
    assert saved_message["extensionData"] == {"model_metadata": {"tags": ["original"]}}
    _assert_no_private_fields(provider.raw)

    await _assert_cold_mailbox_and_duplicate(provider.raw, request, client, agent)

    assert client.output.to_dict() == original_output and request == before_request


@pytest.mark.parametrize(("field", "value"), _RESERVED_EXTRAS)
async def test_preexisting_mailbox_alias_is_not_revalidated_as_new_context(field: str, value: Any) -> None:
    # Assemble a preexisting mailbox independently of record_response and context admission.
    message = Message(
        "assistant",
        ["stored answer"],
        message_id="real-output-id",
        author_name="real-author",
        additional_properties={
            "model_metadata": {"tags": ["original"]},
            field: deepcopy(value),
        },
    ).to_dict()
    message[field] = deepcopy(value)
    message["future_message"] = {"type": "not.a.Python.Type", "opaque": [None, False, {}]}
    now = datetime.now(timezone.utc)
    completion = {
        "correlationId": "preexisting",
        "completedAt": now.isoformat(),
        "resultExpiresAt": (now + timedelta(hours=1)).isoformat(),
        "outcome": "succeeded",
    }
    payload = serialize_terminal_response({"type": "agent_response", "messages": [message]})
    payload["futureResponse"] = {"opaque": [None, False, {}]}
    payload["messages"][0]["futureSharedMessage"] = {"opaque": [False, None]}
    raw: dict[str, Any] = {
        "schemaVersion": DurableAgentState.SCHEMA_VERSION,
        "data": {
            "conversationHistory": [],
            "terminalResults": {
                "preexisting": {
                    **completion,
                    "response": payload,
                }
            },
            "completionReceipts": {"preexisting": {**completion, "resultState": "available"}},
        },
    }
    before = deepcopy(raw)
    client = ToolChatClient(tool_calls=False, fail=True)
    agent = NonStreamingAgent(client=client)
    request = {"message": "must not execute", "correlationId": "preexisting"}
    projected = load_terminal_response(payload)
    assert projected.messages[0].author_name == "real-author"
    assert not hasattr(projected.messages[0], field)
    assert projected.messages[0].additional_properties[field] == value

    await _assert_cold_mailbox_and_duplicate(raw, request, client, agent)

    assert raw == before and client.received_messages == []


@pytest.mark.parametrize(("field", "value"), _RESERVED_EXTRAS)
def test_new_context_still_rejects_top_level_reserved_alias_control(field: str, value: Any) -> None:
    raw = Message("user", ["new input"], message_id="real-input-id").to_dict()
    raw[field] = deepcopy(value)
    before = deepcopy(raw)

    with pytest.raises(ValueError, match=field):
        DurableAgentStateMessage.from_core_dict(raw)

    assert raw == before


@pytest.mark.parametrize("value", [{}, "opaque-not-the-public-id"], ids=["object", "string"])
async def test_local_output_storage_still_rejects_reserved_alias_control(value: Any) -> None:
    client = _OutputExtraClient(deepcopy(value))
    provider = JsonStateProvider()
    agent = NonStreamingAgent(client=client, context_providers=[DurableHistoryProvider(prune_excluded=False)])
    request = {"message": "answer once", "correlationId": "local-reject", "options": {"store": False}}

    response = await AgentEntity(agent, state_provider=provider).run(request)

    assert len(client.received_messages) == 1 and client.output is not None
    assert response.additional_properties.get("durable_status") == "error"
    assert "originalMessageId" in response.text
    assert provider.writes == 1
    assert provider.raw["data"]["completionReceipts"]["local-reject"]["outcome"] == "failed"
    assert all(row.get("messageId") != "real-output-id" and "originalMessageId" not in row for row in _rows(provider))
    before = deepcopy(provider.raw)
    cold = JsonStateProvider(_wire(before))
    assert cold.state.to_dict() == before
    duplicate = await AgentEntity(agent, state_provider=cold).run(request)
    assert duplicate.to_dict() == response.to_dict()
    assert len(client.received_messages) == 1 and cold.writes == 0 and cold.raw == before
    assert client.output.to_dict()["originalMessageId"] == value


def test_reserved_aliases_in_known_additional_metadata_are_not_envelope_fields() -> None:
    metadata = {
        "originalMessageId": {"not": "identity"},
        "messageId": "not-the-public-id",
        "authorName": [False, None],
        "createdAt": "business timestamp",
        "extensionData": {"nested": [0, "", {}]},
        "pythonHistoryId": "business value, not an occurrence",
        "pythonHistoryIdentity": {"profile": "agent-framework-python.history-identity", "version": 1},
    }
    for role in ("user", "assistant"):
        message = Message(role, ["known metadata"], message_id="real-id", additional_properties=deepcopy(metadata))
        raw = _wire(message.to_dict())
        before = deepcopy(raw)
        for stored in (
            DurableAgentStateMessage.from_core_dict(raw),
            DurableAgentStateMessage.from_chat_message(message),
        ):
            assert stored.to_dict()["extensionData"] == metadata
            assert "originalMessageId" not in stored.to_dict()
            assert stored.public_message_id == "real-id"
            assert stored.to_chat_message().to_dict() == before
        assert raw == before and message.to_dict() == before


class _InputProbe(ContextProvider):
    def __init__(self) -> None:
        super().__init__("boundary-input-probe")
        self.inputs: list[list[Message]] = []

    async def before_run(self, *, context: SessionContext, **kwargs: Any) -> None:
        self.inputs.append(list(context.input_messages))


def _occurrence_raw(label: str, text: str, public_id: str | None) -> dict[str, Any]:
    raw = _wire(
        Message(
            "user",
            [Content.from_text(text, additional_properties={"known_content": ["unchanged"]})],
            message_id=public_id,
            author_name="same-author",
            additional_properties={"known_message": ["unchanged"]},
        ).to_dict()
    )
    raw["future_message"] = label
    raw["contents"][0]["future_content"] = {"owner": label, "nested": [None, False, {"items": [0, ""]}]}
    return raw


def _known_message(raw: dict[str, Any]) -> Message:
    return load_agent_response({"messages": [deepcopy(raw)]}).messages[0]


def _assert_inert_input(message: Message) -> None:
    assert not hasattr(message, "future_message")
    assert "future_message" not in message.to_dict()
    for content in message.contents:
        assert not hasattr(content, "future_content")
        assert "future_content" not in content.to_dict()
    _assert_no_private_fields(message.to_dict())


def _assert_occurrence_rows(provider: JsonStateProvider, originals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [row for row in _rows(provider) if row["role"] == "user"]
    assert len(rows) == len(originals)
    assert len({_history_id(row) for row in rows}) == len(rows)
    assert [row.get("future_message") for row in rows] == [raw["future_message"] for raw in originals], (
        "equal public messages must retain each occurrence's own raw envelope"
    )
    for row, raw in zip(rows, originals, strict=True):
        saved_content = _core_fields(row["contents"][0])
        assert saved_content["future_content"] == raw["contents"][0]["future_content"]
        expected = DurableAgentStateMessage.from_core_dict(deepcopy(raw)).to_dict()
        actual = deepcopy(row)
        for key in ("messageId", "pythonHistoryId", "pythonHistoryIdentity"):
            expected.pop(key, None)
            actual.pop(key, None)
        assert actual == expected
        assert row.get("messageId") == raw.get("message_id")
        if raw.get("message_id") is None:
            assert "messageId" not in row
            assert "pythonHistoryId" in row
        assert "originalMessageId" not in row
    _assert_no_private_fields(provider.raw)
    assert DurableAgentState.from_dict(_wire(provider.raw)).to_dict() == provider.raw
    return rows


@pytest.mark.parametrize("public_id", [None, "shared-public-id"], ids=["anonymous", "repeated-public-id"])
@pytest.mark.parametrize("same_body", [True, False], ids=["identical-known-fields", "different-body-control"])
@pytest.mark.parametrize("skip_first", [False, True], ids=["both-new", "first-already-accepted"])
@pytest.mark.parametrize("stream", [False, True], ids=["non-streaming", "streaming"])
@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
async def test_context_raw_extras_follow_occurrences_not_filtered_fingerprints_or_positions(
    public_id: str | None, same_body: bool, skip_first: bool, stream: bool, per_call: bool
) -> None:
    originals = [
        _occurrence_raw("first", "same body", public_id),
        _occurrence_raw("second", "same body" if same_body else "different body", public_id),
    ]
    known = [_known_message(raw).to_dict() for raw in originals]
    assert (known[0] == known[1]) is same_body
    assert originals[0] != originals[1]
    occurrences = ["occ-first", "occ-second"]
    request: dict[str, Any] = {
        "message": "logging only, never input",
        "correlationId": "occurrence-pair",
        "contextMessages": deepcopy(originals),
        "contextMessageIds": list(occurrences),
        "options": {"store": False},
    }
    before_request = deepcopy(request)
    seed = DurableAgentState().to_dict()
    if skip_first:
        # An expired transcript can leave an acceptance receipt. The remaining input is
        # request position 1, not position 0 in the post-admission projection.
        admitted = DurableAgentStateMessage.from_core_dict(deepcopy(originals[0]))
        assert admitted.ingestion_identity is not None
        seed["data"]["pythonIngestion"] = {
            "profile": "agent-framework-python.ingestion",
            "version": 1,
            "messages": {occurrences[0]: [admitted.ingestion_identity]},
        }
    before_seed = deepcopy(seed)
    provider = JsonStateProvider(seed)
    history = DurableHistoryProvider(prune_excluded=False)
    probe = _InputProbe()
    client = ToolChatClient(tool_calls=False)
    agent_type = Agent if stream else NonStreamingAgent
    agent = agent_type(
        client=client,
        context_providers=[history, probe],
        require_per_service_call_history_persistence=per_call,
    )
    accepted_raw = originals[1:] if skip_first else originals

    response = await AgentEntity(agent, state_provider=provider).run(request)

    assert response.text == "answer-1" and response.additional_properties.get("durable_status") != "error"
    assert len(client.received_messages) == len(probe.inputs) == 1
    assert provider.writes == 1 and request == before_request and seed == before_seed
    for batch in (client.received_messages[0], probe.inputs[0]):
        assert [message.to_dict() for message in batch] == [_known_message(raw).to_dict() for raw in accepted_raw]
        for message in batch:
            _assert_inert_input(message)
    entries = provider.raw["data"]["conversationHistory"]
    requests = [entry for entry in entries if entry["$type"] == "request"]
    assert len(requests) == 1 and len(requests[0]["messages"]) == len(accepted_raw)
    saved_rows = deepcopy(_assert_occurrence_rows(provider, accepted_raw))
    assert set(_ingestion_messages(provider.raw)) == set(occurrences)
    committed = deepcopy(provider.raw)

    # Consumer-owned input mutations must affect neither a sibling occurrence nor storage.
    inputs = probe.inputs[0]
    before_inputs = [DurableAgentStateMessage.from_chat_message(message).to_dict() for message in inputs]
    inputs[0].additional_properties["known_message"].append("first-only")
    inputs[0].contents[0].additional_properties["known_content"].append("first-only")
    inputs[0].contents[0].text = "first-only mutation"
    first_envelope = getattr(inputs[0], "_durable_original_core_message", None)
    first_content = getattr(inputs[0].contents[0], "_durable_original_core_content", None)
    assert isinstance(first_envelope, dict) and isinstance(first_content, dict)
    first_envelope["future_message"] = "first-only envelope mutation"
    first_content["future_content"]["nested"].append("first-only content mutation")
    mutated = DurableAgentStateMessage.from_chat_message(inputs[0]).to_dict()
    assert mutated["future_message"] == "first-only envelope mutation"
    assert _core_fields(mutated["contents"][0])["future_content"]["nested"][-1] == ("first-only content mutation")
    if len(inputs) == 2:
        assert inputs[0] is not inputs[1] and inputs[0].contents[0] is not inputs[1].contents[0]
        assert DurableAgentStateMessage.from_chat_message(inputs[1]).to_dict() == before_inputs[1]
    assert request == before_request and provider.raw == committed and provider.state.to_dict() == committed
    request["contextMessages"][0]["contents"][0]["future_content"]["nested"].append("caller-only")
    assert request["contextMessages"][1] == before_request["contextMessages"][1]
    assert provider.raw == committed and originals == before_request["contextMessages"]

    duplicate_provider = JsonStateProvider(_wire(committed))
    duplicate = await AgentEntity(agent, state_provider=duplicate_provider).run(deepcopy(before_request))
    assert duplicate.to_dict() == response.to_dict()
    assert len(client.received_messages) == 1 and duplicate_provider.writes == 0
    assert duplicate_provider.raw == committed

    # The equal-body case also makes the new cold input collide with old filtered
    # fingerprints. Keep the different-body control distinct on the cold turn too.
    third = _occurrence_raw("third", "same body" if same_body else "third distinct body", public_id)
    followup = {
        **deepcopy(before_request),
        "correlationId": "cold-next",
        "contextMessages": [*deepcopy(originals), third],
        "contextMessageIds": [*occurrences, "occ-third"],
    }
    before_followup = deepcopy(followup)
    cold = JsonStateProvider(_wire(committed))
    cold_probe = _InputProbe()
    cold_client = ToolChatClient(tool_calls=False)
    cold_agent = agent_type(
        client=cold_client,
        context_providers=[DurableHistoryProvider(prune_excluded=False), cold_probe],
        require_per_service_call_history_persistence=per_call,
    )

    next_response = await AgentEntity(cold_agent, state_provider=cold).run(followup)

    assert next_response.text == "answer-1" and next_response.additional_properties.get("durable_status") != "error"
    assert len(cold_client.received_messages) == len(cold_probe.inputs) == 1
    assert [message.to_dict() for message in cold_probe.inputs[0]] == [_known_message(third).to_dict()]
    model_users = [message for message in cold_client.received_messages[0] if message.role == "user"]
    assert [message.text for message in model_users] == [raw["contents"][0]["text"] for raw in [*accepted_raw, third]]
    assert [message.message_id for message in model_users] == [
        *[row.get("messageId") for row in saved_rows],
        public_id,
    ]
    for index, message in enumerate(model_users):
        _assert_inert_input(message)
        expected_properties: dict[str, Any] = {"known_message": ["unchanged"]}
        if index < len(accepted_raw):
            expected_properties["_attribution"] = {
                "source_id": "durable_history",
                "source_type": "DurableHistoryProvider",
            }
        assert message.additional_properties == expected_properties
        assert message.contents[0].additional_properties == {"known_content": ["unchanged"]}
    final_rows = _assert_occurrence_rows(cold, [*accepted_raw, third])
    assert final_rows[:-1] == saved_rows, "a cold replay must not rewrite an older occurrence's raw extras"
    assert set(_ingestion_messages(cold.raw)) == {*occurrences, "occ-third"}
    assert cold.writes == 1 and followup == before_followup and provider.raw == committed
