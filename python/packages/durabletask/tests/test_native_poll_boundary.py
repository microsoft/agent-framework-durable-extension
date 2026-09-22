# Copyright (c) Microsoft. All rights reserved.

"""Native client/metadata polling boundaries with only the service RPCs stubbed."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from unittest.mock import Mock, call

import grpc
import pytest
from agent_framework import AgentResponse, Content
from durabletask.client import TaskHubGrpcClient as NativeTaskHubGrpcClient
from durabletask.entities import EntityInstanceId
from durabletask.entities.entity_metadata import EntityMetadata
from durabletask.internal import orchestrator_service_pb2 as pb
from pydantic import BaseModel

from agent_framework_durabletask import AgentSessionId, DurableAgentSession, DurableAIAgentClient, read_agent_state
from agent_framework_durabletask._executors import ClientAgentExecutor

CORRELATION = "poll-boundary-request"
ENTITY = EntityInstanceId("dafx-assistant", "poll-boundary-session")
COMPLETED = "2026-09-17T11:00:00+00:00"
DEADLINE = "2026-09-17T12:00:00+00:00"
NOW = datetime(2026, 9, 17, 12, tzinfo=timezone.utc)
SENTINEL = "PRIVATE-STORED-PAYLOAD"
NativePoll = tuple[NativeTaskHubGrpcClient, Mock, Mock]


class Answer(BaseModel):
    answer: int


def _shared(correlation_id: str = CORRELATION, outcome: str = "succeeded") -> dict[str, Any]:
    common = {"correlationId": correlation_id, "outcome": outcome, "completedAt": COMPLETED}
    result: dict[str, Any] = {
        **common,
        "response": {
            "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "canonical answer"}]}],
            "value": {"answer": 7},
        },
    }
    if outcome == "failed":
        result["error"] = {"code": "provider_failure", "message": "Authoritative failure"}
    return {
        "schemaVersion": "2.0.0",
        "future": {"opaque": [SENTINEL, None, False, 0]},
        "data": {
            "conversationHistory": [],
            "terminalResults": {correlation_id: result},
            "completionReceipts": {correlation_id: {**common, "resultState": "available"}},
        },
    }


def _payload(raw: dict[str, Any], correlation_id: str = CORRELATION) -> dict[str, Any]:
    return raw["data"]["terminalResults"][correlation_id]["response"]


def _json(raw: Any) -> str:
    return json.dumps(raw, sort_keys=True, allow_nan=False)


def _wire(state: str | None, *, exists: bool = True) -> pb.GetEntityResponse:
    response = pb.GetEntityResponse(exists=exists)
    response.entity.instanceId = str(ENTITY)
    response.entity.lastModifiedTime.FromDatetime(NOW)
    if state is not None:
        response.entity.serializedState.value = state
    return response


def _errors(response: AgentResponse) -> list[Content]:
    return [item for message in response.messages for item in message.contents if item.type == "error"]


def _run(client: NativeTaskHubGrpcClient, *, typed: bool = True) -> AgentResponse:
    agent = DurableAIAgentClient(client, max_poll_retries=3, poll_interval_seconds=0.25).get_agent("assistant")
    session = DurableAgentSession.from_session_id(AgentSessionId("assistant", "poll-boundary-session"))
    options: dict[str, Any] = {"temperature": 0, "metadata": {"caller": [False, 0]}}
    if typed:
        options["response_format"] = Answer
    before = deepcopy(options)
    try:
        return agent.run("question", session=session, options=options)
    finally:
        assert options == before


def _assert_calls(rpc: Mock, sleep: Mock, count: int) -> None:
    assert sleep.call_args_list == [call(0.25)] * count
    expected_read = call(pb.GetEntityRequest(instanceId=str(ENTITY), includeState=True))
    assert rpc.GetEntity.call_args_list == [expected_read] * count
    rpc.SignalEntity.assert_called_once()
    signal = rpc.SignalEntity.call_args.args[0]
    assert type(signal) is pb.SignalEntityRequest
    assert signal.instanceId == str(ENTITY)
    assert signal.name == "run"
    request = json.loads(signal.input.value)
    assert request["correlationId"] == CORRELATION
    assert request["message"] == "question"
    assert request["options"] == {"temperature": 0, "metadata": {"caller": [False, 0]}}


@pytest.fixture
def native_poll(monkeypatch: pytest.MonkeyPatch) -> Iterator[NativePoll]:
    rpc = Mock(spec=["GetEntity", "SignalEntity"])
    rpc.SignalEntity.return_value = pb.SignalEntityResponse()
    sleep = Mock()
    monkeypatch.setattr("agent_framework_durabletask._executors.time.sleep", sleep)
    monkeypatch.setattr(ClientAgentExecutor, "generate_unique_id", lambda self: CORRELATION)
    clock = Mock()
    clock.now.return_value = NOW
    monkeypatch.setattr("agent_framework_durabletask._state_reader.datetime", clock)
    # A channel alone makes no RPC. Replacing the service stub keeps the native
    # request construction, get_entity, EntityMetadata and get_state paths intact.
    with grpc.insecure_channel("localhost:1") as channel, NativeTaskHubGrpcClient(channel=channel) as client:
        monkeypatch.setattr(client, "_stub", rpc)
        yield client, rpc, sleep


def _invalid_state(case: str) -> Any:
    raw = _shared()
    if case == "malformed-json":
        return "{" + SENTINEL
    if case == "null":
        return None
    if case == "false":
        return False
    if case == "zero":
        return 0
    if case == "empty-array":
        return []
    if case == "future-version":
        raw["schemaVersion"] = "3.0.0"
    elif case == "missing-collections":
        del raw["data"]["terminalResults"]
    elif case == "receipt-mismatch":
        raw["data"]["completionReceipts"][CORRELATION]["correlationId"] = "other"
    elif case == "legacy-discriminator":
        raw = {
            "schemaVersion": "1.1.0",
            "data": {"conversationHistory": [{"$type": SENTINEL, "createdAt": COMPLETED, "messages": []}]},
        }
    elif case == "legacy-projection":
        raw = {
            "schemaVersion": "1.1.0",
            "data": {
                "conversationHistory": [
                    {
                        "$type": "response",
                        "correlationId": CORRELATION,
                        "createdAt": COMPLETED,
                        "messages": [{"role": "assistant", "contents": [{"$type": "unknown", "content": None}]}],
                    }
                ]
            },
        }
    else:
        node = _payload(raw)
        if case in ("message-profile", "content-profile"):
            node = node["messages"][0]
        if case == "content-profile":
            node = node["contents"][0]
        node["pythonCoreFields"] = {
            "profile": "agent-framework-python.core-fields",
            "version": 1,
            "fields": None,
        }
    return raw


@pytest.mark.parametrize("typed", [False, True])
@pytest.mark.parametrize(
    "case",
    [
        "malformed-json",
        "null",
        "false",
        "zero",
        "empty-array",
        "future-version",
        "missing-collections",
        "receipt-mismatch",
        "legacy-discriminator",
        "response-profile",
        "message-profile",
        "content-profile",
        "legacy-projection",
    ],
)
def test_native_stored_error_stops_before_a_later_valid_response(
    case: str, typed: bool, native_poll: NativePoll, caplog: pytest.LogCaptureFixture
) -> None:
    client, rpc, sleep = native_poll
    raw = _invalid_state(case)
    before = _json(raw)
    wire_state = raw if case == "malformed-json" else before
    first, second = _wire(wire_state), _wire(_json(_shared()))
    wire_before = (first.SerializeToString(), second.SerializeToString())
    rpc.GetEntity.return_value = first

    # Establish the actual SDK boundary, not a mock returning already-decoded state.
    metadata = client.get_entity(ENTITY, include_state=True)
    assert type(metadata) is EntityMetadata
    assert metadata.id == ENTITY and metadata.includes_state
    assert metadata.get_state() == wire_state
    if case.endswith("-profile") or case == "legacy-projection":
        reader = read_agent_state(metadata.get_state())
        expected = Exception if case == "legacy-projection" else ValueError
        with pytest.raises(expected) as projection_error:
            reader.try_get_agent_response(CORRELATION)
        assert type(projection_error.value) is expected
    else:
        with pytest.raises(ValueError) as decode_error:
            read_agent_state(metadata.get_state())
        if case == "legacy-discriminator":
            assert SENTINEL in str(decode_error.value)  # This real exception must never be rendered by the poller.
    rpc.GetEntity.reset_mock()
    rpc.GetEntity.side_effect = [first, second]

    with caplog.at_level(logging.WARNING, logger="agent_framework.durabletask"):
        response = _run(client, typed=typed)

    assert [item.error_code for item in _errors(response)] == ["state_read_error"]
    assert _errors(response)[0].message == "Failed to read the stored agent response."
    assert _errors(response)[0].error_details is None
    assert response.additional_properties == {"durable_status": "error", "correlation_id": CORRELATION}
    assert len(response.messages) == 1 and response.messages[0].role == "system"
    assert response.value is None
    assert response.created_at is not None
    _assert_calls(rpc, sleep, 1)
    warnings = [record for record in caplog.records if record.name == "agent_framework.durabletask"]
    assert len(warnings) == 1
    assert warnings[0].getMessage() == "[ClientAgentExecutor] Failed to decode or project stored agent state"
    assert warnings[0].exc_info is None and warnings[0].stack_info is None
    assert SENTINEL not in caplog.text
    assert SENTINEL not in _json(response.to_dict())
    assert metadata.get_state() == wire_state
    assert _json(raw) == before
    assert (first.SerializeToString(), second.SerializeToString()) == wire_before


@pytest.mark.parametrize("failure", ["rpc", "metadata"])
@pytest.mark.parametrize("recovers", [False, True])
def test_native_sdk_read_failures_remain_retryable(failure: str, recovers: bool, native_poll: NativePoll) -> None:
    client, rpc, sleep = native_poll
    malformed = _wire(_json(_shared()))
    malformed.entity.instanceId = "invalid-entity-id"
    before = malformed.SerializeToString()
    # The ValueError originates inside native get_entity, before get_state or
    # read_agent_state. An outer generic ValueError handler would misclassify it.
    rpc.GetEntity.return_value = malformed
    with pytest.raises(ValueError, match="Invalid entity instance ID"):
        client.get_entity(ENTITY, include_state=True)
    rpc.GetEntity.reset_mock()
    bad: Any = grpc.RpcError("Transient read failure") if failure == "rpc" else malformed
    rpc.GetEntity.side_effect = [bad, _wire(_json(_shared()))] if recovers else [bad] * 3

    response = _run(client)

    if recovers:
        assert response.value == Answer(answer=7)
        assert response.text == "canonical answer"
        assert _errors(response) == []
    else:
        assert [item.error_code for item in _errors(response)] == ["response_timeout"]
    _assert_calls(rpc, sleep, 2 if recovers else 3)
    assert malformed.SerializeToString() == before


@pytest.mark.parametrize("pending", ["entity", "state", "empty-state", "empty-object", "correlation"])
@pytest.mark.parametrize("completes", [False, True])
def test_native_absence_stays_pending(pending: str, completes: bool, native_poll: NativePoll) -> None:
    client, rpc, sleep = native_poll
    if pending == "correlation":
        raw = _shared("other")
        # Even an unprojectable profile for another request is not our failure.
        _payload(raw, "other")["pythonCoreFields"] = {
            "profile": "agent-framework-python.core-fields",
            "version": 1,
            "fields": None,
        }
        first = _wire(_json(raw))
    else:
        state = {"entity": None, "state": None, "empty-state": "", "empty-object": "{}"}[pending]
        first = _wire(state, exists=pending != "entity")
    before = first.SerializeToString()
    rpc.GetEntity.side_effect = [first, _wire(_json(_shared()))] if completes else [first] * 3

    response = _run(client)

    if completes:
        assert response.value == Answer(answer=7)
        assert _errors(response) == []
    else:
        assert [item.error_code for item in _errors(response)] == ["response_timeout"]
    _assert_calls(rpc, sleep, 2 if completes else 3)
    assert first.SerializeToString() == before


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize("delivery", ["available", "expired", "unavailable"])
def test_native_authoritative_outcomes_remain_terminal(outcome: str, delivery: str, native_poll: NativePoll) -> None:
    client, rpc, sleep = native_poll
    raw = _shared(outcome=outcome)
    if outcome == "failed":
        _payload(raw)["value"] = {"answer": "not-an-integer"}
    if delivery == "expired":
        for collection in ("terminalResults", "completionReceipts"):
            raw["data"][collection][CORRELATION]["resultExpiresAt"] = DEADLINE
        # Expired delivery must not project a retained, incompatible profile.
        _payload(raw)["pythonCoreFields"] = {
            "profile": "agent-framework-python.core-fields",
            "version": 1,
            "fields": None,
        }
    elif delivery == "unavailable":
        raw["data"]["terminalResults"] = {}
        raw["data"]["completionReceipts"][CORRELATION].update(resultState="unavailable", resultUnavailableAt=DEADLINE)
    before = _json(raw)
    first = _wire(before)
    wire_before = first.SerializeToString()
    rpc.GetEntity.side_effect = [first, _wire(_json(_shared()))]

    response = _run(client)

    if delivery != "available":
        assert [item.error_code for item in _errors(response)] == ["response_expired"]
        assert response.additional_properties == {
            "durable_status": "already_completed",
            "correlation_id": CORRELATION,
            "durable_outcome": outcome,
        }
        assert response.value is None
    elif outcome == "failed":
        assert [item.error_code for item in _errors(response)] == ["provider_failure"]
        assert _errors(response)[0].message == "Authoritative failure"
        assert response.additional_properties["durable_status"] == "error"
        assert response.value == {"answer": "not-an-integer"}
    else:
        assert _errors(response) == []
        assert response.value == Answer(answer=7)
    _assert_calls(rpc, sleep, 1)
    assert _json(raw) == before
    assert first.SerializeToString() == wire_before


@pytest.mark.parametrize("value_case", ["missing", "invalid", "lossy"])
def test_native_post_read_typed_errors_do_not_become_state_errors_or_timeouts(
    value_case: str, native_poll: NativePoll
) -> None:
    client, rpc, sleep = native_poll
    raw = _shared()
    if value_case == "missing":
        del _payload(raw)["value"]
    else:
        _payload(raw)["value"] = {"answer": "not-an-integer" if value_case == "invalid" else True}
    before = _json(raw)
    assert read_agent_state(before).try_get_agent_response(CORRELATION) is not None
    first = _wire(before)
    wire_before = first.SerializeToString()
    rpc.GetEntity.side_effect = [first, _wire(_json(_shared()))]

    response = _run(client)

    assert [item.error_code for item in _errors(response)] == ["response_processing_error"]
    _assert_calls(rpc, sleep, 1)
    assert _json(raw) == before
    assert first.SerializeToString() == wire_before
