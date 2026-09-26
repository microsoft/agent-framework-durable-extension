# Copyright (c) Microsoft. All rights reserved.

"""Focused tests for the extracted shared-history state and identity helpers."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import pytest
from agent_framework import AgentResponse, Content, Message

from agent_framework_durabletask._delivery_state import stage_expiry, stage_response
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._shared_agent_state import (
    DurableAgentState,
    DurableAgentStateData,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
    _parse_history_entries,
)

HISTORY_PROFILE = {"profile": "agent-framework-python.history-identity", "version": 1}
INGESTION_PROFILE = {"profile": "agent-framework-python.ingestion", "version": 1}
NOW = datetime(2026, 9, 17, 12, 0, 0, tzinfo=timezone.utc)
SCHEMA_PATH = Path(__file__).resolve().parents[4] / "schemas" / "durable-agent-entity-state.json"


def _json(value: Any) -> str:
    # Python equality conflates False, 0, 0.0 and -0.0.
    return json.dumps(value, ensure_ascii=True, sort_keys=True, allow_nan=False)


def _request(raw: dict[str, Any]) -> DurableAgentStateRequest:
    return DurableAgentStateRequest.from_dict(raw)


def _response(raw: dict[str, Any]) -> DurableAgentStateResponse:
    return DurableAgentStateResponse.from_dict(raw)


def _schema_fixture() -> dict[str, Any]:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def _canonical_v2_state() -> dict[str, Any]:
    schema = _schema_fixture()
    data_properties = schema["$defs"]["data"]["properties"]
    assert set(("conversationHistory", "session", "ingestedPositions")) <= set(data_properties)
    return {
        "schemaVersion": "2.0.0",
        "unknownRoot": {"deep": {"aliases": [None, False, 0, -0.0, "e\u0301😀", 2**70, {"leaf": [[], {}, "keep"]}]}},
        "data": {
            "conversationHistory": [
                {
                    "$type": "request",
                    "correlationId": "seed",
                    "createdAt": "2026-09-17T00:00:00Z",
                    "messages": [
                        {
                            "role": "user",
                            "messageId": "public-id",
                            "pythonHistoryId": "private-id",
                            "pythonHistoryIdentity": deepcopy(HISTORY_PROFILE),
                            "unknownMessage": {"persist": [False, 0]},
                            "contents": [
                                {"$type": "text", "text": "question"},
                                {
                                    "$type": "data",
                                    "uri": "https://example.invalid/corpus.png",
                                    "mediaType": "image/png",
                                    "unknownMedia": {"corpus": True},
                                },
                                {
                                    "$type": "unknown",
                                    "content": {"deep": [None, False, 0, -0.0, "", [], {}, {"n": 2**70}]},
                                },
                            ],
                        }
                    ],
                    "orchestrationId": "orch-seed",
                    "responseType": "json",
                    "responseSchema": {
                        "type": "object",
                        "properties": {"answer": {"type": "string"}},
                    },
                    "unknownRequest": {"preserve": "request"},
                },
                {
                    "$type": "response",
                    "correlationId": "seed",
                    "createdAt": "2026-09-17T00:00:01Z",
                    "messages": [
                        {
                            "role": "assistant",
                            "messageId": "answer-public",
                            "contents": [{"$type": "text", "text": "answer"}],
                        }
                    ],
                    "usage": {
                        "inputTokenCount": 2**70,
                        "extensionData": {"provider": None},
                        "unknownUsage": False,
                    },
                    "unknownResponse": {"preserve": [False, 0]},
                },
            ],
            "session": {
                "session_id": "session-1",
                "service_session_id": "svc-1",
                "state": {"opaque": [False, 0, None, {"deep": ["keep"]}]},
            },
            "ingestedPositions": {"executor": 3},
            "truncation": {
                "evictedMessageCount": 2,
                "firstEvictedAt": "2026-09-17T08:00:00Z",
                "lastEvictedAt": "2026-09-17T09:00:00Z",
                "unknownTruncation": [False, 0],
            },
            "terminalResults": {},
            "completionReceipts": {},
            "pythonIngestion": {
                **deepcopy(INGESTION_PROFILE),
                "messages": {"session-1": ["a" * 64, "b" * 64]},
            },
            "unknownData": {"shadow": [False, 0, {"deep": [None, "keep"]}]},
        },
    }


def _delivery_state_root(*, deadline: datetime, expired: bool = False) -> dict[str, Any]:
    raw = _canonical_v2_state()
    outcome = "failed" if expired else "succeeded"
    payload = {
        "messages": [
            {
                "role": "assistant",
                "messageId": "result-public",
                "unknownMessage": {"keep": [False, 0]},
                "contents": [
                    {"$type": "text", "text": "canonical result", "unknownContent": {"keep": None}},
                    {"$type": "unknown", "content": {"deep": [False, 0, {"n": 2**70}]}, "flag": False},
                ],
            }
        ],
        "value": {"nested": [None, False, 0, -0.0, "", [], {}]},
        "responseId": "response-c",
        "createdAt": "2026-09-17T09:59:59.123456789Z",
        "extensionData": {"provider": {"flag": False}},
        "unknownResponse": {"keep": [False, 0]},
    }
    raw["data"]["terminalResults"] = {
        "ready": {
            "correlationId": "ready",
            "outcome": outcome,
            "completedAt": "2026-09-17T10:00:00Z",
            "resultExpiresAt": deadline.isoformat(),
            "unknownResult": {"keep": [False, 0, 0.0]},
            "response": payload,
            "error": {"code": "provider_failure", "message": "Provider failed."} if expired else None,
        }
    }
    if not expired:
        raw["data"]["terminalResults"]["ready"].pop("error")
    raw["data"]["completionReceipts"] = {
        "ready": {
            "correlationId": "ready",
            "outcome": outcome,
            "completedAt": "2026-09-17T10:00:00Z",
            "resultExpiresAt": deadline.isoformat(),
            "resultState": "available",
            "unknownReceipt": {"keep": [False, 0, 0.0]},
        }
    }
    return raw


def _legacy_state(version: str) -> dict[str, Any]:
    return {
        "schemaVersion": version,
        "data": {
            "conversationHistory": [
                {
                    "$type": "request",
                    "correlationId": "legacy",
                    "createdAt": "2026-09-16T10:00:00Z",
                    "messages": [{"role": "user", "contents": [{"$type": "text", "text": "before"}]}],
                },
                {
                    "$type": "response",
                    "correlationId": "legacy",
                    "createdAt": "2026-09-16T10:01:00Z",
                    "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "legacy result"}]}],
                },
            ]
        },
    }


def _agent_response(
    text: str,
    *,
    status: str | None = None,
    error_code: str | None = None,
) -> AgentResponse:
    if error_code is None:
        contents: list[Any] = [text]
    else:
        contents = [Content.from_error(message=text, error_code=error_code)]
    return AgentResponse(
        messages=[Message("assistant", contents, message_id="output-public")],
        created_at="2026-09-17T12:00:00Z",
        additional_properties={"durable_status": status} if status is not None else {"provider": {"flag": False}},
    )


class _DoNotInspect:
    def __getattribute__(self, name: str) -> Any:
        raise AssertionError(f"Duplicate inspected {name}")

    def __bool__(self) -> bool:
        raise AssertionError("Duplicate inspected truthiness")

    def __repr__(self) -> str:
        raise AssertionError("Duplicate formatted an unused argument")


def test_message_identity_uses_canonical_full_message_json() -> None:
    original = Message(
        "assistant",
        [{"type": "function_call", "call_id": "call", "name": "lookup", "arguments": {"b": 2, "a": 1}}],
        message_id="custom-id",
        author_name="author",
        additional_properties={"nested": {"z": "world", "a": 1}},
        raw_representation=object(),
    )
    reordered = Message(
        "assistant",
        [{"arguments": {"a": 1, "b": 2}, "name": "lookup", "call_id": "call", "type": "function_call"}],
        message_id="custom-id",
        author_name="author",
        additional_properties={"nested": {"a": 1, "z": "world"}},
    )
    canonical = json.dumps(
        original.to_dict(),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    expected = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    assert message_identity(original) == expected == message_identity(reordered)
    assert message_identity(Message.from_dict(json.loads(original.to_json()))) == expected
    assert original.message_id == "custom-id"


def test_public_and_internal_ids_roundtrip_without_changing_public_message_id() -> None:
    stored = DurableAgentStateMessage.from_chat_message(Message("user", ["text"], message_id="public-id"))
    stored.set_history_id("private-occurrence")

    wire = stored.to_dict()
    assert wire["messageId"] == "public-id"
    assert wire["pythonHistoryId"] == "private-occurrence"
    assert wire["pythonHistoryIdentity"] == HISTORY_PROFILE

    loaded = DurableAgentStateMessage.from_dict(wire)
    assert loaded.message_id == "private-occurrence"
    assert loaded.public_message_id == "public-id"
    assert loaded.to_chat_message().message_id == "public-id"
    assert loaded.to_dict() == wire


@pytest.mark.parametrize("history_id", [None, False, 0, "", " \t", [], {}])
def test_recognized_identity_profile_rejects_malformed_internal_ids(history_id: Any) -> None:
    raw = {
        "role": "user",
        "messageId": "public-id",
        "pythonHistoryId": deepcopy(history_id),
        "pythonHistoryIdentity": deepcopy(HISTORY_PROFILE),
    }
    before = deepcopy(raw)

    with pytest.raises(ValueError, match="pythonHistoryId"):
        DurableAgentStateMessage.from_dict(raw)

    assert raw == before


@pytest.mark.parametrize("factory", [_request, _response], ids=["request", "response"])
def test_direct_entry_factories_detach_source_and_exported_snapshots(factory: Callable[[dict[str, Any]], Any]) -> None:
    kind = "request" if factory is _request else "response"
    raw: dict[str, Any] = {
        "$type": kind,
        "createdAt": "2026-09-17T00:00:00Z",
        "messages": [{"role": "user", "contents": [{"$type": "text", "text": "before"}]}],
    }

    entry = factory(raw)
    raw["messages"][0]["contents"][0]["text"] = "source mutation"
    assert entry.messages[0].text == "before"

    encoded = entry.to_dict()
    encoded["messages"][0]["contents"][0]["text"] = "output mutation"
    assert entry.to_dict()["messages"][0]["contents"][0]["text"] == "before"


def test_parse_history_entries_roundtrips_and_preserves_internal_ids_across_json_cold_reload() -> None:
    history = [
        {
            "$type": "request",
            "correlationId": "corr-1",
            "createdAt": "2026-09-17T00:00:00Z",
            "messages": [
                {
                    "role": "user",
                    "messageId": "public-id",
                    "pythonHistoryId": "private-occurrence",
                    "pythonHistoryIdentity": deepcopy(HISTORY_PROFILE),
                    "contents": [{"$type": "text", "text": "question"}],
                }
            ],
        },
        {
            "$type": "response",
            "correlationId": "corr-1",
            "createdAt": "2026-09-17T00:00:01Z",
            "messages": [
                {
                    "role": "assistant",
                    "messageId": "answer-id",
                    "contents": [{"$type": "text", "text": "answer"}],
                }
            ],
        },
    ]

    entries = _parse_history_entries({"conversationHistory": deepcopy(history)})
    assert [entry.to_dict() for entry in entries] == history

    reloaded = _parse_history_entries({"conversationHistory": json.loads(json.dumps(history, allow_nan=False))})
    first_message = reloaded[0].messages[0]
    assert first_message.message_id == "private-occurrence"
    assert first_message.public_message_id == "public-id"
    assert [entry.to_dict() for entry in reloaded] == history


def test_private_state_roundtrips_canonical_fixture_with_media_session_and_ingestion() -> None:
    raw = _canonical_v2_state()
    before = deepcopy(raw)

    state = DurableAgentState.from_dict(raw)

    assert raw == before
    assert state.schema_version == DurableAgentState.SCHEMA_VERSION == "2.0.0"
    assert state.to_dict() == before
    assert state.data.session == before["data"]["session"]
    assert state.data.ingested_messages == before["data"]["pythonIngestion"]["messages"]

    reloaded = DurableAgentState.from_json(json.dumps(before, allow_nan=False))
    assert reloaded.to_dict() == before
    assert reloaded.data.conversation_history[0].messages[0].public_message_id == "public-id"
    assert reloaded.data.conversation_history[0].messages[0].message_id == "private-id"


def test_state_snapshots_do_not_alias_unknown_root_session_or_delivery_maps() -> None:
    raw = _canonical_v2_state()
    state = DurableAgentState.from_dict(raw)

    raw["unknownRoot"]["deep"]["aliases"][4] = "source mutation"
    raw["data"]["session"]["state"]["opaque"][3]["deep"][0] = "source mutation"

    encoded = state.to_dict()
    encoded["unknownRoot"]["deep"]["aliases"][4] = "output mutation"
    encoded["data"]["session"]["state"]["opaque"][3]["deep"][0] = "output mutation"
    encoded["data"]["terminalResults"]["new"] = {"correlationId": "new"}

    fresh = state.to_dict()
    assert fresh["unknownRoot"]["deep"]["aliases"][4] == "e\u0301😀"
    assert fresh["data"]["session"]["state"]["opaque"][3]["deep"][0] == "keep"
    assert fresh["data"]["terminalResults"] == {}


@pytest.mark.parametrize("mutate_source", [True, False], ids=["source", "export"])
def test_direct_data_factory_detaches_source_and_exported_snapshots(mutate_source: bool) -> None:
    raw: dict[str, Any] = _delivery_state_root(deadline=NOW + timedelta(minutes=5))["data"]
    raw["extensionData"] = {"provider": {"values": [None, False, 0, 0.0, -0.0, "", [], {}, {"deep": ["keep"]}]}}
    raw["session"]["unknownSession"] = {"values": [False, 0, 0.0]}
    raw["ingestedPositions"] = {"executor": 3.0, "zero": 0, "floatZero": -0.0}
    raw["truncation"]["evictedMessageCount"] = 2.0
    raw["pythonIngestion"]["unknownIngestion"] = {"values": [False, 0, 0.0]}
    before = deepcopy(raw)

    # Do not enter through DurableAgentState.from_dict, which already owns its input.
    data = DurableAgentStateData.from_dict(raw)
    encoded = data.to_dict()
    assert _json(encoded) == _json(before)

    target = raw if mutate_source else encoded
    target["session"]["state"]["opaque"][3]["deep"].append("mutation")
    target["session"]["unknownSession"]["values"][0] = 0
    target["extensionData"]["provider"]["values"][1] = 0
    target["extensionData"]["provider"]["values"][-1]["deep"].append("mutation")
    target["ingestedPositions"]["executor"] = 3
    target["ingestedPositions"]["floatZero"] = 0.0
    target["truncation"]["evictedMessageCount"] = 2
    target["truncation"]["unknownTruncation"][0] = 0
    target["unknownData"]["shadow"][0] = 0
    target["unknownData"]["shadow"][2]["deep"].append("mutation")
    target["pythonIngestion"]["messages"]["session-1"].append("c" * 64)
    target["pythonIngestion"]["unknownIngestion"]["values"][0] = 0
    target["terminalResults"]["ready"]["unknownResult"]["keep"][0] = 0
    target["terminalResults"]["ready"]["response"]["value"]["nested"][1] = 0
    target["completionReceipts"]["ready"]["unknownReceipt"]["keep"][0] = 0
    target["conversationHistory"][0]["messages"][0]["contents"][0]["text"] = "mutation"

    assert _json(data.session) == _json(before["session"])
    assert _json(data.extension_data) == _json(before["extensionData"])
    assert _json(data.ingested_positions) == _json(before["ingestedPositions"])
    assert _json(data.truncation) == _json(before["truncation"])
    assert _json(data.unknown_fields["unknownData"]) == _json(before["unknownData"])
    assert _json(data.ingested_messages) == _json(before["pythonIngestion"]["messages"])
    assert _json(data.response_mailbox) == _json(before["terminalResults"])
    assert _json(data.completed_correlations) == _json(before["completionReceipts"])
    fresh = data.to_dict()
    assert _json(fresh) == _json(before)
    assert [type(value) for value in fresh["extensionData"]["provider"]["values"][:5]] == [
        type(None),
        bool,
        int,
        float,
        float,
    ]
    assert _json(encoded if mutate_source else raw) == _json(before)


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
def test_legacy_state_reads_but_prepare_for_write_rejects_canonical_private_writer(version: str) -> None:
    raw = _legacy_state(version)
    state = DurableAgentState.from_dict(raw)

    response = state.try_get_agent_response("legacy", now=NOW)
    assert response is not None
    assert response.text == "legacy result"

    with pytest.raises(ValueError, match="Legacy state is read-only"):
        state.prepare_for_write(delivery_window_seconds=60)


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize("operation", ["record", "expiry"])
def test_legacy_delivery_operations_reject_without_writes_or_schema_upgrade(version: str, operation: str) -> None:
    raw = _legacy_state(version)
    before = _json(raw)
    state = DurableAgentState.from_dict(raw)
    data = state.data
    history = data.conversation_history
    identities = [(entry, entry.messages, tuple(entry.messages)) for entry in history]
    mailbox = data.response_mailbox
    receipts = data.completed_correlations

    if operation == "record":
        with pytest.raises(ValueError, match="writable shared schema version"):
            state.record_response("new", _agent_response("not persisted"), delivery_window_seconds=60, now=NOW)
    else:
        with pytest.raises(ValueError, match="requires schemaVersion 2.0.0"):
            state.expire_responses(now=NOW)

    assert state.schema_version == version
    assert state.data is data
    assert data.response_mailbox is mailbox
    assert data.completed_correlations is receipts
    assert mailbox == {}
    assert receipts == {}
    assert data.conversation_history is history
    for actual, (entry, message_list, messages) in zip(history, identities, strict=True):
        assert actual is entry
        assert actual.messages is message_list
        assert all(current is original for current, original in zip(actual.messages, messages, strict=True))
    assert _json(state.to_dict()) == before
    assert _json(raw) == before


def test_prepare_for_write_rejects_opaque_python_ingestion_shadow_on_current_schema() -> None:
    state = DurableAgentState.from_dict(_canonical_v2_state())
    state.data.unknown_fields["pythonIngestion"] = {"profile": "foreign", "version": 9}

    with pytest.raises(ValueError, match="opaque pythonIngestion"):
        state.prepare_for_write(delivery_window_seconds=60)


def test_record_response_matches_stage_response_and_preserves_history_object_identity() -> None:
    state = DurableAgentState.from_dict(_canonical_v2_state())
    history = state.data.conversation_history
    first_message = history[0].messages[0]
    response = _agent_response("new answer")
    before = state.to_dict()

    expected = stage_response(before, "new", response, delivery_window_seconds=60, now=NOW)
    state.record_response("new", response, delivery_window_seconds=60, now=NOW)

    assert state.data.conversation_history is history
    assert state.data.conversation_history[0].messages[0] is first_message
    assert state.to_dict() == expected
    delivered = state.try_get_agent_response("new", now=NOW)
    assert delivered is not None
    assert delivered.messages[0].message_id == "output-public"


def test_record_response_rejects_invalid_unknown_root_without_touching_existing_payload() -> None:
    state = DurableAgentState.from_dict(_canonical_v2_state())
    before_mailbox = deepcopy(state.data.response_mailbox)
    before_receipts = deepcopy(state.data.completed_correlations)
    state.unknown_fields["unknownRoot"] = object()

    with pytest.raises(ValueError, match="strict JSON"):
        state.record_response("new", _agent_response("new answer"), delivery_window_seconds=60, now=NOW)

    assert state.data.response_mailbox == before_mailbox
    assert state.data.completed_correlations == before_receipts


@pytest.mark.parametrize(
    "invalid",
    [
        pytest.param(object(), id="runtime-object"),
        pytest.param((False, 0), id="tuple"),
        pytest.param({1: "non-string key"}, id="non-string-key"),
        pytest.param(float("nan"), id="non-finite-number"),
    ],
)
def test_record_response_validates_opaque_root_before_duplicate_return_without_changing_maps(invalid: Any) -> None:
    state = DurableAgentState.from_dict(_delivery_state_root(deadline=NOW + timedelta(minutes=5)))
    data = state.data
    mailbox = data.response_mailbox
    receipts = data.completed_correlations
    before = _json(data.to_dict())
    state.unknown_fields["unknownRoot"]["deep"]["invalid"] = invalid
    poison: Any = _DoNotInspect()

    with pytest.raises(ValueError, match="strict JSON"):
        state.record_response("ready", poison, delivery_window_seconds=poison, now=poison)

    assert state.data is data
    assert data.response_mailbox is mailbox
    assert data.completed_correlations is receipts
    assert _json(data.to_dict()) == before
    assert state.unknown_fields["unknownRoot"]["deep"]["invalid"] is invalid


@pytest.mark.parametrize("kind", ["available", "logically-expired", "unavailable"])
@pytest.mark.parametrize("failed", [False, True], ids=["succeeded", "failed"])
def test_record_response_duplicate_ignores_replacement_and_preserves_raw_maps_and_history_identity(
    kind: str, failed: bool
) -> None:
    deadline = NOW + timedelta(minutes=5) if kind == "available" else NOW - timedelta(seconds=1)
    raw = _delivery_state_root(deadline=deadline, expired=failed)
    if kind == "unavailable":
        del raw["data"]["terminalResults"]["ready"]
        raw["data"]["completionReceipts"]["ready"].update(
            resultState="unavailable", resultUnavailableAt=NOW.isoformat()
        )
    before = _json(raw)
    state = DurableAgentState.from_dict(raw)
    data = state.data
    history = data.conversation_history
    identities = [(entry, entry.messages, tuple(entry.messages)) for entry in history]
    poison: Any = _DoNotInspect()

    state.record_response("ready", poison, delivery_window_seconds=poison, now=poison)

    assert state.data is data
    assert data.conversation_history is history
    for actual, (entry, message_list, messages) in zip(history, identities, strict=True):
        assert actual is entry
        assert actual.messages is message_list
        assert all(current is original for current, original in zip(actual.messages, messages, strict=True))
    assert _json(data.response_mailbox) == _json(raw["data"]["terminalResults"])
    assert _json(data.completed_correlations) == _json(raw["data"]["completionReceipts"])
    assert _json(state.to_dict()) == before
    assert _json(raw) == before


@pytest.mark.parametrize("status", ["accepted", "already_completed"])
def test_record_response_rejects_acknowledgement_outcomes_without_mutation(status: str) -> None:
    state = DurableAgentState.from_dict(_canonical_v2_state())
    before = state.to_dict()

    with pytest.raises(ValueError, match="known invocation outcome"):
        state.record_response("new", _agent_response("pending", status=status), delivery_window_seconds=60, now=NOW)

    assert state.to_dict() == before


def test_record_response_rejects_response_expired_error_without_inventing_completion() -> None:
    state = DurableAgentState.from_dict(_canonical_v2_state())
    before = state.to_dict()
    expired = _agent_response("expired", error_code="response_expired")

    with pytest.raises(ValueError, match="known invocation outcome"):
        state.record_response("new", expired, delivery_window_seconds=60, now=NOW)

    assert state.to_dict() == before


def test_expire_responses_is_noop_before_deadline_and_preserves_unknown_metadata() -> None:
    raw = _delivery_state_root(deadline=NOW + timedelta(minutes=5))
    state = DurableAgentState.from_dict(raw)
    history = state.data.conversation_history
    first_message = history[0].messages[0]

    state.expire_responses(now=NOW)

    assert state.data.conversation_history is history
    assert state.data.conversation_history[0].messages[0] is first_message
    assert state.to_dict() == raw


def test_expire_responses_matches_stage_expiry_and_preserves_history_object_identity() -> None:
    raw = _delivery_state_root(deadline=NOW - timedelta(seconds=1))
    state = DurableAgentState.from_dict(raw)
    history = state.data.conversation_history
    first_message = history[0].messages[0]
    expected, _ = stage_expiry(raw, now=NOW)

    state.expire_responses(now=NOW)

    assert state.data.conversation_history is history
    assert state.data.conversation_history[0].messages[0] is first_message
    assert state.to_dict() == expected


def test_try_get_agent_response_prefers_canonical_mailbox_result_without_rewriting_state() -> None:
    raw = _delivery_state_root(deadline=NOW + timedelta(minutes=5))
    raw["data"]["conversationHistory"].append({
        "$type": "response",
        "correlationId": "ready",
        "createdAt": "2026-09-17T11:00:00Z",
        "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "stale transcript"}]}],
    })
    state = DurableAgentState.from_dict(raw)

    response = state.try_get_agent_response("ready", now=NOW)

    assert response is not None
    assert response.text == "canonical result"
    assert response.messages[0].message_id == "result-public"
    assert state.to_dict() == raw
