# Copyright (c) Microsoft. All rights reserved.

"""State-only delivery regressions using core responses and real JSON deserialization."""

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, cast

import pytest
from agent_framework import AgentResponse, Annotation, Content, ContinuationToken, Message
from pydantic import BaseModel

from agent_framework_durabletask import migrate_legacy_state, state_snapshot_digest
from agent_framework_durabletask._durable_agent_state import (
    DurableAgentState,
    DurableAgentStateEntryJsonType,
    DurableAgentStateResponse,
    DurableAgentStateTextContent,
    DurableAgentStateUnknownEntry,
)
from agent_framework_durabletask._history_provider import replayable_entries
from agent_framework_durabletask._message_identity import message_identity

DELIVERY_WINDOW_SECONDS = 60
HISTORICAL_TIME = datetime(2024, 1, 1, tzinfo=timezone.utc)
CORRELATION_ID = "correlation-1"
SOURCE_SESSION_ID = "@dafx-delivery@legacy-source"


def _migrate_legacy_payload(payload: dict[str, Any]) -> DurableAgentState:
    return migrate_legacy_state(
        payload,
        source_digest=state_snapshot_digest(payload),
        source_session_id=SOURCE_SESSION_ID,
        migration_id="delivery-migration-1",
        ownership_transfer_id="delivery-transfer-1",
        delivery_window_seconds=DELIVERY_WINDOW_SECONDS,
    )


def _response(*, value: Any = None) -> AgentResponse[Any]:
    """Use public core 1.16 constructor arguments, not attributes invented by a mock."""
    annotations: list[Annotation] = [
        {
            "type": "citation",
            "title": "Source",
            "url": "https://example.test/source",
            "annotated_regions": [{"type": "text_span", "start_index": 0, "end_index": 6}],
            "additional_properties": {"pages": [2, 3]},
        }
    ]
    return AgentResponse(
        messages=[
            Message(
                "assistant",
                [
                    Content.from_text(
                        "answer",
                        annotations=annotations,
                        additional_properties={"nested": {"labels": ["content"]}},
                        raw_representation=object(),
                    ),
                    Content.from_function_call("call-1", "lookup", arguments={"ids": [1, 2]}, informational_only=True),
                    Content.from_text_reasoning(
                        id="reasoning-1",
                        text="reasoning summary",
                        protected_data="opaque-protected-payload",
                        additional_properties={"provider": {"sequence": [1]}},
                    ),
                ],
                author_name="planner",
                message_id="message-1",
                additional_properties={"nested": {"labels": ["message"]}},
                raw_representation=object(),
            ),
            Message(
                "tool",
                [
                    Content.from_function_result(
                        "call-1",
                        result=[
                            Content.from_text("tool result"),
                            Content.from_data(b"data", "application/octet-stream"),
                        ],
                        additional_properties={"provider": {"sequence": [1]}},
                    )
                ],
                author_name="lookup",
                message_id="message-2",
            ),
        ],
        response_id="response-1",
        agent_id="agent-1",
        created_at=HISTORICAL_TIME.isoformat(),
        finish_reason="stop",
        usage_details={
            "input_token_count": 12,
            "output_token_count": 8,
            "total_token_count": 20,
            "cache_creation_input_token_count": 2,
            "cache_read_input_token_count": 3,
            "reasoning_output_token_count": 4,
        },
        value=value,
        continuation_token=cast(ContinuationToken, {"cursor": {"pages": [1, 2]}}),
        additional_properties={"nested": {"labels": ["response"]}},
        raw_representation=object(),
    )


def _record(state: DurableAgentState, response: AgentResponse[Any], *, now: datetime | None = None) -> None:
    state.record_response(CORRELATION_ID, response, delivery_window_seconds=DELIVERY_WINDOW_SECONDS, now=now)


def _legacy_payload(version: str) -> dict[str, Any]:
    return {
        "schemaVersion": version,
        "data": {
            "conversationHistory": [
                {
                    "$type": "request",
                    "correlationId": CORRELATION_ID,
                    "createdAt": HISTORICAL_TIME.isoformat(),
                    "messages": [{"role": "user", "contents": [], "messageId": "legacy-known-id"}],
                },
                {
                    "$type": "response",
                    "correlationId": CORRELATION_ID,
                    "createdAt": HISTORICAL_TIME.isoformat(),
                    "messages": [
                        {
                            "role": "assistant",
                            "contents": [{"$type": "text", "text": "surviving legacy transcript"}],
                            "messageId": "legacy-response",
                            "authorName": "legacy-agent",
                        }
                    ],
                    "usage": {"inputTokenCount": 3, "outputTokenCount": 2, "totalTokenCount": 5},
                },
            ]
        },
    }


def _assert_expired(response: AgentResponse[Any] | None) -> None:
    assert isinstance(response, AgentResponse)
    assert response.additional_properties["durable_status"] == "already_completed"
    assert response.additional_properties["correlation_id"] == CORRELATION_ID
    assert len(response.messages) == 1
    assert response.messages[0].role == "system"
    assert len(response.messages[0].contents) == 1
    content = response.messages[0].contents[0]
    assert content.type == "error"
    assert content.error_code == "response_expired"
    assert content.error_details is None
    assert response.response_id is None
    assert response.agent_id is None
    assert response.continuation_token is None
    assert response.value is None


def test_record_response_snapshots_core_metadata_and_reloads_real_response() -> None:
    response = _response()
    expected = json.loads(json.dumps(response.to_dict(), allow_nan=False))
    now = datetime.now(timezone.utc)
    state = DurableAgentState()

    _record(state, response, now=now)

    payload = json.loads(state.to_json())
    assert payload["schemaVersion"] == "2.0.0"
    assert payload["data"]["conversationHistory"] == []
    assert payload["data"]["responseMailbox"][CORRELATION_ID] == {
        "response": expected,
        "createdAt": now.isoformat(),
        "expiresAt": (now + timedelta(seconds=DELIVERY_WINDOW_SECONDS)).isoformat(),
    }
    assert payload["data"]["completedCorrelations"][CORRELATION_ID] == {
        "completedAt": now.isoformat(),
        "outcome": "succeeded",
    }
    assert expected["type"] == "agent_response"
    assert expected["response_id"] == "response-1"
    assert expected["agent_id"] == "agent-1"
    assert expected["created_at"] == HISTORICAL_TIME.isoformat()
    assert expected["finish_reason"] == "stop"
    assert expected["usage_details"]["cache_read_input_token_count"] == 3
    assert expected["continuation_token"] == {"cursor": {"pages": [1, 2]}}
    assert expected["additional_properties"] == {"nested": {"labels": ["response"]}}
    assert "raw_representation" not in expected

    # Exercise core's own reader as well as the durable state's reader.
    direct = AgentResponse.from_dict(deepcopy(payload["data"]["responseMailbox"][CORRELATION_ID]["response"]))
    restored = DurableAgentState.from_json(json.dumps(payload))
    delivered = restored.try_get_agent_response(CORRELATION_ID)
    assert isinstance(delivered, AgentResponse)
    assert direct.to_dict() == delivered.to_dict() == expected
    assert all(isinstance(message, Message) for message in delivered.messages)
    assert all(isinstance(content, Content) for message in delivered.messages for content in message.contents)
    assert delivered.messages[0].author_name == "planner"
    assert delivered.messages[0].message_id == "message-1"
    assert delivered.messages[0].contents[0].annotations == response.messages[0].contents[0].annotations
    assert delivered.messages[0].contents[1].call_id == "call-1"
    assert delivered.messages[0].contents[1].informational_only is True
    assert delivered.messages[0].contents[2].id == "reasoning-1"
    assert delivered.messages[0].contents[2].protected_data == "opaque-protected-payload"
    assert delivered.messages[1].contents[0].items == response.messages[1].contents[0].items
    assert restored.try_get_agent_response("unknown-correlation") is None


@pytest.mark.parametrize("value", [{"items": [{"answer": 42}]}, {}, [], 0, False, "structured result"])
def test_record_response_preserves_structured_value_not_just_core_to_dict(value: Any) -> None:
    """Core 1.16 keeps value in private state, so to_dict equality alone cannot prove delivery fidelity."""
    response = _response(value=deepcopy(value))
    state = DurableAgentState()
    _record(state, response)

    snapshot = json.loads(state.to_json())["data"]["responseMailbox"][CORRELATION_ID]["response"]
    assert "value" in snapshot, "record_response lost the public structured result"
    assert snapshot["value"] == value
    assert type(snapshot["value"]) is type(value)
    direct = AgentResponse.from_dict(snapshot)
    assert direct.value == value
    restored = DurableAgentState.from_json(state.to_json())
    delivered = restored.try_get_agent_response(CORRELATION_ID)
    assert isinstance(delivered, AgentResponse)
    assert delivered.value == value


def test_structured_model_value_is_stored_as_inline_json() -> None:
    class Result(BaseModel):
        answer: int
        citations: list[str]

    value = Result(answer=42, citations=["source-1"])
    expected = value.model_dump(mode="json")
    state = DurableAgentState()
    _record(state, _response(value=value))
    value.citations.append("caller edit")

    snapshot = json.loads(state.to_json())["data"]["responseMailbox"][CORRELATION_ID]["response"]
    assert snapshot["value"] == expected
    assert AgentResponse.from_dict(snapshot).value == expected
    delivered = DurableAgentState.from_json(state.to_json()).try_get_agent_response(CORRELATION_ID)
    assert isinstance(delivered, AgentResponse)
    assert delivered.value == expected


def test_lazy_structured_value_is_captured_before_caller_text_changes() -> None:
    response = AgentResponse(
        messages=[Message("assistant", ['{"answer":42}'])],
        response_format={"type": "object", "properties": {"answer": {"type": "integer"}}},
    )
    state = DurableAgentState()
    # Do not access response.value first: recording must capture the public lazy value itself.
    _record(state, response)
    response.messages[0].contents[0].text = '{"answer":0}'

    snapshot = json.loads(state.to_json())["data"]["responseMailbox"][CORRELATION_ID]["response"]
    assert snapshot["value"] == {"answer": 42}
    assert AgentResponse.from_dict(snapshot).value == {"answer": 42}
    delivered = DurableAgentState.from_json(state.to_json()).try_get_agent_response(CORRELATION_ID)
    assert isinstance(delivered, AgentResponse)
    assert delivered.value == {"answer": 42}


def test_mutating_caller_response_and_transcript_cannot_change_mailbox() -> None:
    response = _response()
    expected = json.loads(json.dumps(response.to_dict(), allow_nan=False))
    state = DurableAgentState()
    transcript = DurableAgentStateResponse.from_run_response(CORRELATION_ID, response)
    state.data.conversation_history.append(transcript)
    _record(state, response)

    response.response_id = "changed"
    response.agent_id = "changed"
    response.created_at = datetime.now(timezone.utc).isoformat()
    response.finish_reason = "length"
    response.additional_properties["nested"]["labels"].append("changed")
    assert response.usage_details is not None
    response.usage_details["input_token_count"] = 999
    assert response.continuation_token is not None
    cast(dict[str, Any], response.continuation_token)["cursor"]["pages"].append(999)
    response.messages[0].author_name = "changed"
    response.messages[0].message_id = "changed"
    response.messages[0].additional_properties["nested"]["labels"].append("changed")
    response.messages[0].contents[0].text = "changed"
    response.messages[0].contents[0].additional_properties["nested"]["labels"].append("changed")
    assert response.messages[0].contents[0].annotations is not None
    response.messages[0].contents[0].annotations[0]["additional_properties"]["pages"].append(999)
    cast(dict[str, Any], response.messages[0].contents[1].arguments)["ids"].append(999)
    response.messages[0].contents[2].id = "changed"
    response.messages[0].contents[2].protected_data = "changed"
    assert response.messages[1].contents[0].items is not None
    response.messages[1].contents[0].items[0].text = "changed tool result"
    response.messages.clear()
    transcript.messages[0].contents = [DurableAgentStateTextContent("compacted, not the original answer")]
    transcript.messages[0].extension_data = {"_excluded": True}
    transcript.messages.clear()
    state.data.conversation_history.clear()

    restored = DurableAgentState.from_json(state.to_json())
    assert restored.data.response_mailbox[CORRELATION_ID]["response"] == expected
    delivered = restored.try_get_agent_response(CORRELATION_ID)
    assert isinstance(delivered, AgentResponse)
    assert delivered.to_dict() == expected


def test_structured_value_is_detached_from_the_caller() -> None:
    value = {"nested": {"items": [1, 2]}}
    expected = deepcopy(value)
    response = _response(value=value)
    state = DurableAgentState()
    _record(state, response)
    value["nested"]["items"].append(3)
    assert response.value is not None
    response.value["nested"]["items"].append(4)

    restored = DurableAgentState.from_json(state.to_json())
    delivered = restored.try_get_agent_response(CORRELATION_ID)
    assert isinstance(delivered, AgentResponse)
    assert delivered.value == expected


def test_poll_results_and_serialized_delivery_records_are_detached() -> None:
    state = DurableAgentState()
    _record(state, _response())
    state.data.ingested_messages = {"message-1": ["a" * 64], "legacy-known-id": None}
    state = DurableAgentState.from_json(state.to_json())
    expected = json.loads(state.to_json())

    delivered = state.try_get_agent_response(CORRELATION_ID)
    assert isinstance(delivered, AgentResponse)
    delivered.additional_properties["nested"]["labels"].append("caller edit")
    delivered.messages[0].contents[0].additional_properties["nested"]["labels"].append("caller edit")
    delivered.messages.clear()
    exported = state.to_dict()
    exported["data"]["responseMailbox"][CORRELATION_ID]["response"]["messages"].clear()
    exported["data"]["completedCorrelations"][CORRELATION_ID]["completedAt"] = "changed"
    exported["data"]["ingestedMessages"]["message-1"].clear()

    assert state.to_dict() == expected
    second = state.try_get_agent_response(CORRELATION_ID)
    assert isinstance(second, AgentResponse)
    assert second.to_dict() == expected["data"]["responseMailbox"][CORRELATION_ID]["response"]


@pytest.mark.parametrize("cleanup", [False, True], ids=["before-cleanup", "after-cleanup"])
@pytest.mark.parametrize("original_error", [False, True], ids=["success", "error"])
def test_expiry_returns_completed_status_never_the_surviving_transcript(cleanup: bool, original_error: bool) -> None:
    state = DurableAgentState()
    response = _response()
    if original_error:
        response.messages = [
            Message(
                "system",
                [
                    Content.from_error(
                        message="original provider failure",
                        error_code="previous_response_not_found",
                        error_details="original provider details",
                    )
                ],
            )
        ]
    state.data.conversation_history.append(DurableAgentStateResponse.from_run_response(CORRELATION_ID, response))
    now = datetime.now(timezone.utc)
    _record(state, response, now=now - timedelta(seconds=DELIVERY_WINDOW_SECONDS + 1))
    receipt = deepcopy(state.data.completed_correlations[CORRELATION_ID])
    transcript = deepcopy(state.to_dict()["data"]["conversationHistory"])
    if cleanup:
        state.expire_responses(now=now)

    restored = DurableAgentState.from_json(state.to_json())
    assert bool(restored.data.response_mailbox) is not cleanup
    before_poll = restored.to_json()
    _assert_expired(restored.try_get_agent_response(CORRELATION_ID))
    assert restored.to_json() == before_poll
    assert restored.data.completed_correlations[CORRELATION_ID] == receipt
    assert restored.to_dict()["data"]["conversationHistory"] == transcript
    assert restored.try_get_agent_response("never-completed") is None


def test_expiry_boundary_removes_only_due_payloads_not_receipts() -> None:
    state = DurableAgentState()
    _record(state, _response(), now=HISTORICAL_TIME)
    state.record_response(
        "later",
        _response(),
        delivery_window_seconds=DELIVERY_WINDOW_SECONDS,
        now=HISTORICAL_TIME + timedelta(seconds=30),
    )
    receipts = deepcopy(state.data.completed_correlations)
    boundary = HISTORICAL_TIME + timedelta(seconds=DELIVERY_WINDOW_SECONDS)
    state.expire_responses(now=boundary - timedelta(microseconds=1))
    assert set(state.data.response_mailbox) == {CORRELATION_ID, "later"}
    state.expire_responses(now=boundary)
    assert set(state.data.response_mailbox) == {"later"}
    assert state.data.completed_correlations == receipts
    state.expire_responses(now=boundary + timedelta(seconds=30))
    assert state.data.response_mailbox == {}
    assert DurableAgentState.from_json(state.to_json()).data.completed_correlations == receipts


@pytest.mark.parametrize("expired", [False, True])
def test_duplicate_record_does_not_replace_or_reopen_a_completed_response(expired: bool) -> None:
    state = DurableAgentState()
    now = datetime.now(timezone.utc)
    _record(state, _response(), now=now)
    if expired:
        state.expire_responses(now=now + timedelta(seconds=DELIVERY_WINDOW_SECONDS))
    state = DurableAgentState.from_json(state.to_json())
    before = state.to_json()
    replacement = AgentResponse(messages=[Message("assistant", ["must not replace the original"])])
    _record(state, replacement, now=now + timedelta(days=1))
    assert state.to_json() == before
    if expired:
        _assert_expired(state.try_get_agent_response(CORRELATION_ID))


def test_version_two_does_not_poll_transcript_without_delivery_evidence() -> None:
    state = DurableAgentState.from_dict(_legacy_payload("2.0.0"))
    assert state.try_get_agent_response(CORRELATION_ID) is None


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0"])
@pytest.mark.parametrize("kind", ["response", "errorResponse"])
def test_legacy_reader_round_trip_and_polling_do_not_upgrade_state(version: str, kind: str) -> None:
    payload = _legacy_payload(version)
    payload["data"]["conversationHistory"][1]["$type"] = kind
    state = DurableAgentState.from_dict(deepcopy(payload))
    restored = DurableAgentState.from_json(state.to_json())
    assert restored.to_dict() == payload
    delivered = restored.try_get_agent_response(CORRELATION_ID)
    assert isinstance(delivered, AgentResponse)
    assert delivered.text == "surviving legacy transcript"
    assert delivered.messages[0].author_name == "legacy-agent"
    assert delivered.usage_details == {"input_token_count": 3, "output_token_count": 2, "total_token_count": 5}
    assert restored.try_get_agent_response("never-completed") is None
    assert restored.to_dict() == payload


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0"])
def test_legacy_conversion_records_a_fresh_grace_window_not_a_historical_original(version: str) -> None:
    payload = _legacy_payload(version)
    original = deepcopy(payload)
    legacy_response = DurableAgentState.from_dict(payload).try_get_agent_response(CORRELATION_ID)
    assert isinstance(legacy_response, AgentResponse)
    before = datetime.now(timezone.utc)
    state = _migrate_legacy_payload(payload)
    after = datetime.now(timezone.utc)

    restored = DurableAgentState.from_json(state.to_json())
    assert restored.schema_version == "2.0.0"
    mailbox = restored.data.response_mailbox[CORRELATION_ID]
    created_at = datetime.fromisoformat(mailbox["createdAt"])
    assert before <= created_at <= after
    assert created_at != HISTORICAL_TIME
    assert datetime.fromisoformat(mailbox["expiresAt"]) - created_at == timedelta(seconds=DELIVERY_WINDOW_SECONDS)
    assert restored.data.completed_correlations[CORRELATION_ID] == {"completedAt": mailbox["createdAt"], "legacy": True}
    assert restored.data.ingested_messages == {"legacy-known-id": None}
    assert restored.data.unknown_fields["migration"] == {
        "id": "delivery-migration-1",
        "sourceDigest": state_snapshot_digest(original),
        "sourceSessionId": SOURCE_SESSION_ID,
        "ownershipTransferId": "delivery-transfer-1",
        "createdAt": mailbox["createdAt"],
    }
    assert restored.data.session == {"session_id": SOURCE_SESSION_ID, "state": {}}
    assert payload == original
    delivered = restored.try_get_agent_response(CORRELATION_ID)
    assert isinstance(delivered, AgentResponse)
    assert delivered.to_dict() == legacy_response.to_dict()
    assert delivered.created_at == HISTORICAL_TIME.isoformat()

    first_conversion = restored.to_json()
    restored.prepare_for_write(delivery_window_seconds=DELIVERY_WINDOW_SECONDS * 2)
    assert restored.to_json() == first_conversion
    restored.expire_responses(now=datetime.fromisoformat(mailbox["expiresAt"]))
    expired = DurableAgentState.from_json(restored.to_json())
    after_expiry = expired.to_json()
    expired.prepare_for_write(delivery_window_seconds=DELIVERY_WINDOW_SECONDS * 2)
    assert expired.to_json() == after_expiry
    _assert_expired(expired.try_get_agent_response(CORRELATION_ID))


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0"])
@pytest.mark.parametrize("position", [0, 3])
def test_scalar_legacy_ingestion_cannot_be_migrated_without_evidence(version: str, position: int) -> None:
    payload = _legacy_payload(version)
    payload["futureRoot"] = {"opaque": [1]}
    payload["data"]["ingestedPositions"] = {"source": position}
    original = deepcopy(payload)
    state = DurableAgentState.from_dict(payload)
    before = state.to_json()
    for _ in range(2):
        with pytest.raises(ValueError, match="ingestedPositions.*recorded delivery evidence"):
            state.prepare_for_write(delivery_window_seconds=DELIVERY_WINDOW_SECONDS)
        with pytest.raises(ValueError, match="ingestedPositions.*recorded delivery evidence"):
            _migrate_legacy_payload(payload)
        assert state.to_json() == before
        assert payload == original
        assert state.data.response_mailbox == {}
        assert state.data.completed_correlations == {}
        assert state.data.ingested_messages == {}
    # Refusing the writer upgrade must not prevent legacy read-only polling.
    delivered = DurableAgentState.from_json(state.to_json()).try_get_agent_response(CORRELATION_ID)
    assert isinstance(delivered, AgentResponse)
    assert delivered.text == "surviving legacy transcript"


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "2.0.0", "2.7.3"])
def test_unknown_root_data_and_entry_properties_survive_reload_and_explicit_migration(version: str) -> None:
    payload = _legacy_payload(version)
    payload["futureRoot"] = {"nested": [1, {"keep": True}]}
    payload["data"]["futureData"] = {"nested": [2, {"keep": None}]}
    payload["data"]["session"] = {
        "session_id": SOURCE_SESSION_ID,
        "owner": "custom-provider",
        "state": {"external": {"messages": [{"custom": "owned data"}], "cursor": [3, 4]}},
    }
    known_entry = deepcopy(payload["data"]["conversationHistory"][1])
    history: list[dict[str, Any]] = []
    for kind in DurableAgentStateEntryJsonType:
        entry = deepcopy(known_entry)
        entry["$type"] = kind.value
        entry["correlationId"] = kind.value
        entry["messages"][0]["contents"][0]["text"] = kind.value
        entry["futureEntry"] = {"nested": [kind.value, {"keep": False}]}
        entry["extensionData"] = {"existing": {"keep": True}}
        if kind not in (DurableAgentStateEntryJsonType.RESPONSE, DurableAgentStateEntryJsonType.ERROR_RESPONSE):
            entry.pop("usage")
        if kind == DurableAgentStateEntryJsonType.COMPACTION:
            entry.pop("correlationId")
        history.append(entry)
    opaque = {
        "$type": "future-owner-entry",
        "correlationId": "opaque",
        "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "do not replay"}]}],
        "futureEntry": {"nested": [None, {"keep": "opaque"}]},
    }
    history.insert(1, opaque)
    payload["data"]["conversationHistory"] = history
    state = DurableAgentState.from_dict(deepcopy(payload))
    state = DurableAgentState.from_json(state.to_json())
    assert state.to_dict() == payload
    assert isinstance(state.data.conversation_history[1], DurableAgentStateUnknownEntry)
    replayed = [entry.messages[index].text for entry, index in replayable_entries(state.data.conversation_history)]
    assert replayed == ["request", "response", "compaction"]
    assert state.try_get_agent_response("opaque") is None

    if version.startswith("1."):
        state = _migrate_legacy_payload(payload)
    elif version == "2.0.0":
        state.prepare_for_write(delivery_window_seconds=DELIVERY_WINDOW_SECONDS)
    else:
        # Future revisions remain readable without permitting a write or downgrading the source.
        with pytest.raises(ValueError, match="Only 2.0.0 is writable"):
            state.prepare_for_write(delivery_window_seconds=DELIVERY_WINDOW_SECONDS)
        assert state.to_dict() == payload
    upgraded = DurableAgentState.from_json(state.to_json()).to_dict()
    assert upgraded["schemaVersion"] == ("2.0.0" if version.startswith("1.") else version)
    assert upgraded["futureRoot"] == payload["futureRoot"]
    for key in ("futureData", "session", "conversationHistory"):
        assert upgraded["data"][key] == payload["data"][key]


def test_ingestion_hash_lists_and_legacy_known_id_markers_survive_json_reload() -> None:
    first = Message("user", ["first"], message_id="same-id")
    changed = Message("user", ["changed"], message_id="same-id")
    hashes = [message_identity(first), message_identity(changed)]
    assert hashes[0] != hashes[1]
    payload = {
        "schemaVersion": "2.0.0",
        "data": {"conversationHistory": [], "ingestedMessages": {"same-id": hashes, "legacy-known-id": None}},
    }
    state = DurableAgentState.from_dict(payload)
    assert DurableAgentState.from_json(state.to_json()).to_dict() == payload


@pytest.mark.parametrize("version", [None, False, 2, "", "0.1.0", "3.0.0", "2.0", "2.0.0-preview", "2.0.0\n"])
def test_unsupported_or_malformed_version_fails_without_resetting_input(version: Any) -> None:
    payload = _legacy_payload("1.1.0")
    payload["schemaVersion"] = version
    original = deepcopy(payload)
    with pytest.raises(ValueError, match="schemaVersion"):
        DurableAgentState.from_dict(payload)
    with pytest.raises(ValueError, match="schemaVersion"):
        DurableAgentState.from_json(json.dumps(payload))
    assert payload == original


def test_missing_version_fails_without_resetting_existing_history() -> None:
    payload = _legacy_payload("1.1.0")
    del payload["schemaVersion"]
    original = deepcopy(payload)
    with pytest.raises(ValueError, match="missing schemaVersion"):
        DurableAgentState.from_dict(payload)
    with pytest.raises(ValueError, match="missing schemaVersion"):
        DurableAgentState.from_json(json.dumps(payload))
    assert payload == original


@pytest.mark.parametrize("data", [None, False, 0, "", [], [1]])
def test_non_object_data_is_not_silently_reset(data: Any) -> None:
    payload = {"schemaVersion": "2.0.0", "data": data}
    with pytest.raises(ValueError, match="data"):
        DurableAgentState.from_dict(payload)


@pytest.mark.parametrize("field", ["responseMailbox", "completedCorrelations", "ingestedMessages"])
@pytest.mark.parametrize("value", [None, False, 0, "", [], "not-an-object", [1]])
def test_malformed_delivery_containers_fail_on_initial_read_including_falsy_values(field: str, value: Any) -> None:
    """An explicitly malformed field must not be normalized to an empty receipt store."""
    payload = {"schemaVersion": "2.0.0", "data": {"conversationHistory": [], field: value}}
    original = deepcopy(payload)
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(payload)
    assert payload == original


@pytest.mark.parametrize("field", ["responseMailbox", "completedCorrelations"])
@pytest.mark.parametrize("value", [None, False, 0, "", [], "not-an-entry"])
def test_delivery_record_must_be_an_object_at_initial_read(field: str, value: Any) -> None:
    payload = {"schemaVersion": "2.0.0", "data": {field: {CORRELATION_ID: value}}}
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(payload)


@pytest.mark.parametrize(
    ("record_name", "required_field"),
    [
        ("responseMailbox", "response"),
        ("responseMailbox", "createdAt"),
        ("responseMailbox", "expiresAt"),
        ("completedCorrelations", "completedAt"),
    ],
)
def test_required_delivery_record_fields_are_checked_before_polling(record_name: str, required_field: str) -> None:
    state = DurableAgentState()
    _record(state, _response())
    payload = json.loads(state.to_json())
    del payload["data"][record_name][CORRELATION_ID][required_field]
    original = deepcopy(payload)
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(payload)
    assert payload == original


@pytest.mark.parametrize(
    ("record_name", "field"),
    [("responseMailbox", "createdAt"), ("responseMailbox", "expiresAt"), ("completedCorrelations", "completedAt")],
)
@pytest.mark.parametrize("value", [None, False, 0, [], "", "not-a-timestamp"])
def test_invalid_delivery_timestamps_fail_at_initial_read(record_name: str, field: str, value: Any) -> None:
    state = DurableAgentState()
    _record(state, _response())
    payload = json.loads(state.to_json())
    payload["data"][record_name][CORRELATION_ID][field] = value
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(payload)


@pytest.mark.parametrize("response", [None, [], "{}", {}, {"type": "other", "messages": []}])
def test_invalid_inline_response_fails_at_initial_read(response: Any) -> None:
    state = DurableAgentState()
    _record(state, _response())
    payload = json.loads(state.to_json())
    payload["data"]["responseMailbox"][CORRELATION_ID]["response"] = response
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(payload)


@pytest.mark.parametrize("legacy", [None, 0, 1, "true", [], {}])
def test_legacy_receipt_marker_must_be_boolean_at_initial_read(legacy: Any) -> None:
    payload = {
        "schemaVersion": "2.0.0",
        "data": {
            "completedCorrelations": {CORRELATION_ID: {"completedAt": HISTORICAL_TIME.isoformat(), "legacy": legacy}}
        },
    }
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(payload)


@pytest.mark.parametrize("fingerprints", [False, 0, "a" * 64, {}, [None], [1], ["a" * 64, False]])
def test_ingestion_record_rejects_anything_but_hash_lists_or_legacy_null(fingerprints: Any) -> None:
    payload = {"schemaVersion": "2.0.0", "data": {"ingestedMessages": {"message-id": fingerprints}}}
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(payload)
