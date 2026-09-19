# Copyright (c) Microsoft. All rights reserved.

"""Caller-facing DT regressions using literal state, mock RPCs and real SDK tasks."""

from __future__ import annotations

import json
import logging
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from unittest.mock import Mock, call

import pytest
from agent_framework import AgentResponse, Content, Message
from durabletask.client import TaskHubGrpcClient
from durabletask.entities import EntityInstanceId
from durabletask.task import CompletableTask, OrchestrationContext, TaskFailedError
from pydantic import BaseModel, Field, RootModel

from agent_framework_durabletask import (
    DEFAULT_POLL_INTERVAL_SECONDS,
    AgentSessionId,
    DurableAgentSession,
    DurableAgentState,
    DurableAIAgent,
    DurableAIAgentClient,
    DurableAIAgentOrchestrationContext,
    LegacyDurableAgentState,
    SharedAgentStateReader,
    read_agent_state,
    serialize_agent_response,
)
from agent_framework_durabletask._executors import ClientAgentExecutor, DurableAgentTask

CORRELATION = "Request-01"
COMPLETED = "2026-09-17T11:00:00+00:00"
DEADLINE = "2026-09-17T12:00:00.123456Z"
NOW = datetime(2026, 9, 17, 12, 0, 0, 123456, tzinfo=timezone.utc)
ENTITY = EntityInstanceId("dafx-assistant", "session-01")
SESSION = DurableAgentSession.from_session_id(AgentSessionId("assistant", "session-01"))
OPAQUE: dict[str, Any] = {"type": "business-data", "nested": [None, False, 0, "", [], {}]}


class CountAnswer(BaseModel):
    count: int = Field(alias="aliasCount")


class NullAnswer(RootModel[None]):
    pass


VALUE_CASES = [
    pytest.param(CountAnswer, {"aliasCount": 7}, id="aliased-model"),
    pytest.param(NullAnswer, None, id="null-root"),
    pytest.param(RootModel[bool], False, id="false"),
    pytest.param(RootModel[int], 0, id="zero"),
    pytest.param(RootModel[str], "", id="empty-string"),
    pytest.param(RootModel[list[int]], [], id="empty-list"),
    pytest.param(RootModel[dict[str, int]], {}, id="empty-object"),
]


def _json(value: Any) -> str:
    # JSON comparison also distinguishes false from zero and absent from null.
    return json.dumps(value, sort_keys=True, allow_nan=False)


def _shared(outcome: str = "succeeded", delivery: str = "available") -> dict[str, Any]:
    common: dict[str, Any] = {
        "correlationId": CORRELATION,
        "outcome": outcome,
        "completedAt": COMPLETED,
    }
    if delivery == "expired":
        common["resultExpiresAt"] = DEADLINE
    receipt = {**common, "resultState": "unavailable" if delivery == "unavailable" else "available"}
    results = {}
    if delivery == "unavailable":
        receipt["resultUnavailableAt"] = DEADLINE
    else:
        result = {
            **common,
            "response": {
                "messages": [
                    {
                        "role": "assistant",
                        "messageId": "answer-01",
                        "authorName": "provider",
                        "contents": [{"$type": "text", "text": "canonical answer"}],
                        "extensionData": {"message-marker": [False]},
                    }
                ],
                "responseId": "response-01",
                "agentId": "agent-01",
                "createdAt": COMPLETED,
                "finishReason": "stop",
                "usage": {"inputTokenCount": 3, "outputTokenCount": 0, "totalTokenCount": 3},
                "extensionData": {"provider": deepcopy(OPAQUE)},
            },
        }
        if outcome == "failed":
            result["error"] = {"code": "provider_failure", "message": "Original failure", "details": deepcopy(OPAQUE)}
        results[CORRELATION] = result
    return {
        "schemaVersion": "2.0.0",
        "future": deepcopy(OPAQUE),
        "data": {
            "conversationHistory": [
                {
                    "$type": "response",
                    "correlationId": CORRELATION,
                    "createdAt": COMPLETED,
                    "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "stale transcript"}]}],
                }
            ],
            "terminalResults": results,
            "completionReceipts": {CORRELATION: receipt},
            "session": {"state": deepcopy(OPAQUE)},
            "historyBinding": deepcopy(OPAQUE),
        },
    }


def _payload(raw: dict[str, Any]) -> dict[str, Any]:
    return raw["data"]["terminalResults"][CORRELATION]["response"]


def _metadata(raw: Any, *, as_json: bool = True) -> Mock:
    metadata = Mock(spec=["get_state"])
    metadata.get_state.return_value = _json(raw) if as_json else raw
    return metadata


def _errors(response: AgentResponse) -> list[Content]:
    return [content for message in response.messages for content in message.contents if content.type == "error"]


def _assert_metadata(response: AgentResponse) -> None:
    assert response.response_id == "response-01"
    assert response.agent_id == "agent-01"
    assert response.created_at == COMPLETED
    assert response.finish_reason == "stop"
    assert response.usage_details == {"input_token_count": 3, "output_token_count": 0, "total_token_count": 3}
    assert response.additional_properties["provider"] == OPAQUE
    assert response.messages[0].message_id == "answer-01"
    assert response.messages[0].author_name == "provider"
    assert response.messages[0].additional_properties == {"message-marker": [False]}


def _assert_expired(response: AgentResponse, outcome: str) -> None:
    assert type(response) is AgentResponse
    assert response.additional_properties == {
        "durable_status": "already_completed",
        "correlation_id": CORRELATION,
        "durable_outcome": outcome,
    }
    assert response.value is None
    assert len(response.messages) == 1
    assert response.messages[0].role == "system"
    assert [content.error_code for content in _errors(response)] == ["response_expired"]
    assert "stale transcript" not in response.text
    assert "canonical answer" not in response.text


@pytest.fixture
def rpc() -> Mock:
    client = Mock(spec=TaskHubGrpcClient)
    client.get_entity.return_value = None
    return client


@pytest.fixture
def sleep(monkeypatch: pytest.MonkeyPatch) -> Mock:
    sleeper = Mock()
    monkeypatch.setattr("agent_framework_durabletask._executors.time.sleep", sleeper)
    clock = Mock()
    clock.now.return_value = NOW
    monkeypatch.setattr("agent_framework_durabletask._state_reader.datetime", clock)
    return sleeper


@pytest.fixture
def client_agent(rpc: Mock, sleep: Mock, monkeypatch: pytest.MonkeyPatch) -> DurableAIAgent[AgentResponse]:
    monkeypatch.setattr(ClientAgentExecutor, "generate_unique_id", lambda self: CORRELATION)
    return DurableAIAgentClient(rpc, max_poll_retries=3, poll_interval_seconds=0.25).get_agent("assistant")


def _assert_polled(rpc: Mock, sleep: Mock, count: int, *, signals: int = 1) -> None:
    assert rpc.get_entity.call_args_list == [call(ENTITY, include_state=True)] * count
    assert sleep.call_args_list == [call(0.25)] * count
    assert rpc.signal_entity.call_count == signals
    for invocation in rpc.signal_entity.call_args_list:
        entity, operation, request = invocation.args
        assert entity == ENTITY
        assert operation == "run"
        assert request["correlationId"] == CORRELATION
        assert request["message"] == "question"


def test_public_state_dispatch_keeps_readers_separate_from_the_v2_writer() -> None:
    raw = _shared()
    reader = read_agent_state(_json(raw))
    assert isinstance(reader, SharedAgentStateReader)
    assert _json(reader.to_dict()) == _json(raw)
    assert reader.message_count == 1
    assert not hasattr(reader, "record_response")
    assert DurableAgentState().schema_version == "2.0.0"
    assert LegacyDurableAgentState().schema_version == "1.1.0"
    assert type(read_agent_state("{}")) is LegacyDurableAgentState
    assert not isinstance(reader, DurableAgentState)


@pytest.mark.parametrize("as_json", [False, True])
@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
def test_client_returns_canonical_result_and_metadata(
    outcome: str, as_json: bool, client_agent: DurableAIAgent[AgentResponse], rpc: Mock, sleep: Mock
) -> None:
    raw = _shared(outcome)
    before = _json(raw)
    metadata = _metadata(raw, as_json=as_json)
    rpc.get_entity.return_value = metadata
    options = {"response_format": CountAnswer} if outcome == "failed" else None

    response = client_agent.run("question", session=SESSION, options=options)

    assert type(response) is AgentResponse
    assert response.text == "canonical answer"
    _assert_metadata(response)
    if outcome == "failed":
        assert response.additional_properties["durable_status"] == "error"
        assert response.additional_properties["correlation_id"] == CORRELATION
        assert [error.error_code for error in _errors(response)] == ["provider_failure"]
        assert _errors(response)[0].message == "Original failure"
        assert _errors(response)[0].error_details == OPAQUE
        assert response.value is None
    else:
        assert _errors(response) == []
    _assert_polled(rpc, sleep, 1)
    metadata.get_state.assert_called_once_with()
    assert _json(raw) == before


@pytest.mark.parametrize("as_json", [False, True])
@pytest.mark.parametrize("delivery", ["unavailable", "expired"])
@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
def test_client_completed_receipts_are_terminal_even_with_a_requested_model(
    outcome: str, delivery: str, as_json: bool, client_agent: DurableAIAgent[AgentResponse], rpc: Mock, sleep: Mock
) -> None:
    raw = _shared(outcome, delivery)
    before = _json(raw)
    rpc.get_entity.return_value = _metadata(raw, as_json=as_json)

    response = client_agent.run("question", session=SESSION, options={"response_format": CountAnswer})

    _assert_expired(response, outcome)
    _assert_polled(rpc, sleep, 1)
    assert _json(raw) == before


@pytest.mark.parametrize(
    ("deadline", "expired"),
    [
        ("2026-09-17T12:00:00.123455999999999999Z", True),
        ("2026-09-17T17:30:00.123456+05:30", True),
        ("2026-09-17T12:00:00.123456000000000001Z", False),
    ],
)
def test_client_observes_exact_expiry_without_pruning_the_snapshot(
    deadline: str, expired: bool, client_agent: DurableAIAgent[AgentResponse], rpc: Mock, sleep: Mock
) -> None:
    raw = _shared(delivery="expired")
    for collection in ("terminalResults", "completionReceipts"):
        raw["data"][collection][CORRELATION]["resultExpiresAt"] = deadline
    before = _json(raw)
    rpc.get_entity.return_value = _metadata(raw, as_json=False)

    response = client_agent.run("question", session=SESSION)

    if expired:
        _assert_expired(response, "succeeded")
    else:
        assert response.text == "canonical answer"
        assert _errors(response) == []
    _assert_polled(rpc, sleep, 1)
    assert _json(raw) == before


@pytest.mark.parametrize("response_format,value", VALUE_CASES)
def test_client_retained_typed_value_wins_over_conflicting_text(
    response_format: type[BaseModel], value: Any, client_agent: DurableAIAgent[AgentResponse], rpc: Mock, sleep: Mock
) -> None:
    raw = _shared()
    _payload(raw)["value"] = deepcopy(value)
    _payload(raw)["messages"][0]["contents"][0]["text"] = "42"
    before = _json(raw)
    rpc.get_entity.return_value = _metadata(raw)

    response = client_agent.run("question", session=SESSION, options={"response_format": response_format})

    assert isinstance(response.value, BaseModel)
    assert type(response.value) is response_format
    assert _json(response.value.model_dump(mode="json", by_alias=True, round_trip=True)) == _json(value)
    assert response.text == "42"
    assert _errors(response) == []
    _assert_metadata(response)
    _assert_polled(rpc, sleep, 1)
    assert rpc.signal_entity.call_args.args[2]["response_format"]["qualname"] == response_format.__qualname__
    assert _json(raw) == before


@pytest.mark.parametrize("text", ['{"aliasCount":7}', "42", "null", ""])
@pytest.mark.parametrize("response_format", [CountAnswer, RootModel[Any]])
def test_shared_absent_value_is_not_inferred_from_text_for_a_typed_caller(
    text: str,
    response_format: type[BaseModel],
    client_agent: DurableAIAgent[AgentResponse],
    rpc: Mock,
    sleep: Mock,
) -> None:
    raw = _shared()
    _payload(raw)["messages"][0]["contents"][0]["text"] = text
    before = _json(raw)
    rpc.get_entity.return_value = _metadata(raw)
    response = client_agent.run("question", session=SESSION, options={"response_format": response_format})
    assert [error.error_code for error in _errors(response)] == ["response_processing_error"]
    assert "no structured value" in (_errors(response)[0].message or "")
    assert response.value is None
    _assert_polled(rpc, sleep, 1)
    assert _json(raw) == before and "value" not in _payload(raw)


@pytest.mark.parametrize("value", [{"aliasCount": "7"}, {"aliasCount": True}, {"aliasCount": 7, "extra": None}])
def test_shared_typed_projection_cannot_change_original_json_values(
    value: dict[str, Any], client_agent: DurableAIAgent[AgentResponse], rpc: Mock, sleep: Mock
) -> None:
    raw = _shared()
    _payload(raw)["value"] = deepcopy(value)
    before = _json(raw)
    rpc.get_entity.return_value = _metadata(raw)
    response = client_agent.run("question", session=SESSION, options={"response_format": CountAnswer})
    assert [error.error_code for error in _errors(response)] == ["response_processing_error"]
    assert "preserve" in (_errors(response)[0].message or "")
    _assert_polled(rpc, sleep, 1)
    assert _json(raw) == before


@pytest.mark.parametrize("value", [None, {"aliasCount": "not-an-integer"}])
def test_client_invalid_retained_value_is_a_processing_error_not_text_fallback(
    value: Any, client_agent: DurableAIAgent[AgentResponse], rpc: Mock, sleep: Mock
) -> None:
    raw = _shared()
    _payload(raw)["value"] = value
    _payload(raw)["messages"][0]["contents"][0]["text"] = '{"aliasCount":7}'
    before = _json(raw)
    rpc.get_entity.return_value = _metadata(raw)

    response = client_agent.run("question", session=SESSION, options={"response_format": CountAnswer})

    assert [error.error_code for error in _errors(response)] == ["response_processing_error"]
    assert response.value is None
    _assert_polled(rpc, sleep, 1)
    assert _json(raw) == before


@pytest.mark.parametrize(
    "messages",
    [
        [],
        [{"role": "assistant", "contents": []}],
        [{"role": "assistant", "contents": [{"$type": "text", "text": ""}]}],
    ],
    ids=["no-messages", "no-contents", "empty-text"],
)
def test_client_blank_success_does_not_poll_again_or_fall_back_to_history(
    messages: list[dict[str, Any]], client_agent: DurableAIAgent[AgentResponse], rpc: Mock, sleep: Mock
) -> None:
    raw = _shared()
    _payload(raw)["messages"] = messages
    rpc.get_entity.return_value = _metadata(raw)

    response = client_agent.run("question", session=SESSION)

    assert response.text == ""
    assert len(response.messages) == len(messages)
    assert response.response_id == "response-01"
    assert _errors(response) == []
    _assert_polled(rpc, sleep, 1)


@pytest.mark.parametrize("hint", ["accepted", "already_completed"])
@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
def test_client_provider_hints_cannot_reclassify_an_available_result(
    outcome: str, hint: str, client_agent: DurableAIAgent[AgentResponse], rpc: Mock, sleep: Mock
) -> None:
    raw = _shared(outcome)
    _payload(raw)["value"] = {"aliasCount": 7}
    _payload(raw)["extensionData"].update(
        durable_status=hint, durable_outcome="failed" if outcome == "succeeded" else "succeeded"
    )
    before = _json(raw)
    rpc.get_entity.return_value = _metadata(raw, as_json=False)

    response = client_agent.run("question", session=SESSION, options={"response_format": CountAnswer})

    assert response.additional_properties.get("durable_status") not in ("accepted", "already_completed")
    assert response.additional_properties["durable_outcome"] == outcome
    if outcome == "succeeded":
        assert response.value == CountAnswer(aliasCount=7)
        assert _errors(response) == []
    else:
        assert type(response.value) is dict  # Failed deliveries do not parse the retained value either.
        assert response.value == {"aliasCount": 7}
        assert [error.error_code for error in _errors(response)] == ["provider_failure"]
    _assert_metadata(response)
    _assert_polled(rpc, sleep, 1)
    assert _json(raw) == before


@pytest.mark.parametrize("other", [CORRELATION.lower(), f" {CORRELATION} ", f"{CORRELATION}0"])
def test_client_waits_for_the_exact_correlation_then_stops(
    other: str, client_agent: DurableAIAgent[AgentResponse], rpc: Mock, sleep: Mock
) -> None:
    pending = _shared()
    for collection in ("terminalResults", "completionReceipts"):
        record = pending["data"][collection].pop(CORRELATION)
        record["correlationId"] = other
        pending["data"][collection][other] = record
    before = _json(pending)
    first, second = _metadata(pending), _metadata(_shared())
    rpc.get_entity.side_effect = [first, second]
    events = Mock()
    events.attach_mock(rpc.signal_entity, "signal")
    events.attach_mock(sleep, "sleep")
    events.attach_mock(rpc.get_entity, "poll")

    response = client_agent.run("question", session=SESSION)

    assert response.text == "canonical answer"
    _assert_polled(rpc, sleep, 2)
    assert [event[0] for event in events.mock_calls] == ["signal", "sleep", "poll", "sleep", "poll"]
    first.get_state.assert_called_once_with()
    second.get_state.assert_called_once_with()
    assert _json(pending) == before


def test_client_v2_transcript_alone_is_not_a_result(
    client_agent: DurableAIAgent[AgentResponse], rpc: Mock, sleep: Mock
) -> None:
    raw = _shared()
    raw["data"]["terminalResults"] = {}
    raw["data"]["completionReceipts"] = {}
    before = _json(raw)
    rpc.get_entity.return_value = _metadata(raw)

    response = client_agent.run("question", session=SESSION, options={"response_format": CountAnswer})

    assert [error.error_code for error in _errors(response)] == ["response_timeout"]
    assert _errors(response)[0].message == "Timeout waiting for agent response after 3 attempts"
    assert "stale transcript" not in response.text
    _assert_polled(rpc, sleep, 3)
    assert _json(raw) == before


@pytest.mark.parametrize("as_json", [False, True])
def test_client_cold_reads_are_detached_from_consumer_mutations(
    as_json: bool, client_agent: DurableAIAgent[AgentResponse], rpc: Mock, sleep: Mock
) -> None:
    raw = _shared()
    _payload(raw)["value"] = deepcopy(OPAQUE)
    before = _json(raw)
    metadata = _metadata(raw, as_json=as_json)
    rpc.get_entity.return_value = metadata
    first = client_agent.run("question", session=SESSION)
    assert isinstance(first.value, dict)
    first.value["nested"].append("consumer value edit")
    first.additional_properties["provider"]["nested"].append("consumer metadata edit")
    first.messages[0].contents[0].text = "consumer text edit"
    first.messages[0].additional_properties["message-marker"].append(True)

    cold_agent = DurableAIAgentClient(rpc, max_poll_retries=3, poll_interval_seconds=0.25).get_agent("assistant")
    second = cold_agent.run("question", session=SESSION)

    assert second is not first
    assert second.text == "canonical answer"
    assert _json(second.value) == _json(OPAQUE)
    _assert_metadata(second)
    _assert_polled(rpc, sleep, 2, signals=2)
    assert metadata.get_state.call_count == 2
    assert _json(raw) == before
    cold_reader = read_agent_state(_json(raw))
    assert isinstance(cold_reader, SharedAgentStateReader)
    assert _json(cold_reader.to_dict()) == before


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize("valid", [False, True])
def test_legacy_client_keeps_transcript_text_parsing_and_format_errors(
    version: str, valid: bool, client_agent: DurableAIAgent[AgentResponse], rpc: Mock, sleep: Mock
) -> None:
    text = '{"aliasCount":7}' if valid else '{"wrong":"field"}'
    raw = {
        "schemaVersion": version,
        "data": {
            "conversationHistory": [
                {
                    "$type": "response",
                    "correlationId": CORRELATION,
                    "createdAt": COMPLETED,
                    "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": text}]}],
                    "usage": {"inputTokenCount": None, "outputTokenCount": 1, "totalTokenCount": None},
                }
            ]
        },
    }
    before = _json(raw)
    rpc.get_entity.return_value = _metadata(raw)

    response = client_agent.run("question", session=SESSION, options={"response_format": CountAnswer})

    if valid:
        assert response.value == CountAnswer(aliasCount=7)
        assert response.text == text
        assert response.created_at == COMPLETED
        assert response.usage_details is not None
        assert response.usage_details["output_token_count"] == 1
        assert _errors(response) == []
    else:
        assert [error.error_code for error in _errors(response)] == ["response_processing_error"]
    _assert_polled(rpc, sleep, 1)
    assert _json(raw) == before
    # Only existing projected fields are asserted, not lossless legacy reserialization.


@pytest.mark.parametrize("missing", ["entity", "state", "empty-string", "empty-object"])
def test_client_missing_state_keeps_bounded_timeout_behavior(
    missing: str, client_agent: DurableAIAgent[AgentResponse], rpc: Mock, sleep: Mock
) -> None:
    if missing != "entity":
        raw = {"state": None, "empty-string": "", "empty-object": "{}"}[missing]
        rpc.get_entity.return_value = _metadata(raw, as_json=False)

    response = client_agent.run("question", session=SESSION, options={"response_format": CountAnswer})

    assert [error.error_code for error in _errors(response)] == ["response_timeout"]
    _assert_polled(rpc, sleep, 3)


@pytest.mark.parametrize("retries,interval", [(0, 0.0), (-2, -0.5), (2, 0.75)])
def test_public_client_polling_controls_still_reach_the_executor(
    retries: int, interval: float, rpc: Mock, sleep: Mock
) -> None:
    client = DurableAIAgentClient(rpc, max_poll_retries=retries, poll_interval_seconds=interval)

    response = client.get_agent("assistant").run("question", session=SESSION)

    attempts = max(1, retries)
    delay = interval if interval > 0 else DEFAULT_POLL_INTERVAL_SECONDS
    assert rpc.signal_entity.call_count == 1
    assert rpc.get_entity.call_args_list == [call(ENTITY, include_state=True)] * attempts
    assert sleep.call_args_list == [call(delay)] * attempts
    assert [error.error_code for error in _errors(response)] == ["response_timeout"]


@pytest.mark.parametrize("invalid", ["malformed-json", "future-version", "missing-collections", "receipt-mismatch"])
def test_invalid_state_currently_logs_and_uses_the_existing_timeout_contract(
    invalid: str, client_agent: DurableAIAgent[AgentResponse], rpc: Mock, sleep: Mock, caplog: pytest.LogCaptureFixture
) -> None:
    raw: Any = _shared()
    if invalid == "malformed-json":
        raw = "{not-json"
    elif invalid == "future-version":
        raw["schemaVersion"] = "3.0.0"
    elif invalid == "missing-collections":
        del raw["data"]["terminalResults"]
    else:
        raw["data"]["completionReceipts"][CORRELATION]["correlationId"] = "other"
    before = _json(raw)
    with pytest.raises(ValueError):
        read_agent_state(raw)
    rpc.get_entity.return_value = _metadata(raw, as_json=False)

    # Characterize the existing catch-and-retry boundary, not a new state-error API.
    with caplog.at_level(logging.WARNING, logger="agent_framework.durabletask"):
        response = client_agent.run("question", session=SESSION)

    assert [error.error_code for error in _errors(response)] == ["response_timeout"]
    assert sum("Error reading entity state:" in record.getMessage() for record in caplog.records) == 3
    _assert_polled(rpc, sleep, 3)
    assert _json(raw) == before


def test_client_fire_and_forget_signals_and_returns_acceptance_without_reading(
    client_agent: DurableAIAgent[AgentResponse], rpc: Mock, sleep: Mock
) -> None:
    rpc.get_entity.side_effect = AssertionError("Fire-and-forget must not read state")
    options = {"wait_for_response": False, "response_format": CountAnswer, "temperature": 0}
    before = dict(options)

    response = client_agent.run("question", session=SESSION, options=options)

    assert len(response.messages) == 1
    assert response.messages[0].role == "system"
    assert "accepted" in response.text
    assert "background" in response.text
    assert CORRELATION in response.text
    assert response.value is None
    assert _errors(response) == []
    _assert_polled(rpc, sleep, 0)
    request = rpc.signal_entity.call_args.args[2]
    assert request["wait_for_response"] is False
    assert request["options"] == {"temperature": 0}
    assert options == before


def _orchestration_agent(child: CompletableTask[Any]) -> tuple[DurableAIAgent[DurableAgentTask], Mock]:
    context = Mock(spec=OrchestrationContext)
    context.instance_id = "orchestration-01"
    context.new_uuid.return_value = CORRELATION
    context.call_entity.return_value = child
    return DurableAIAgentOrchestrationContext(context).get_agent("assistant"), context


@pytest.mark.parametrize("completed_before_wrap", [False, True], ids=["delayed", "already-complete"])
@pytest.mark.parametrize("response_format,value", VALUE_CASES)
def test_real_task_delivers_inline_core_values_through_the_orchestration_shim(
    completed_before_wrap: bool, response_format: type[BaseModel], value: Any
) -> None:
    model = response_format.model_validate(value)
    original = AgentResponse(
        messages=[Message("assistant", ["42"])],
        value=model,
        response_id="inline-01",
        additional_properties={"provider": deepcopy(OPAQUE)},
    )
    raw = json.loads(_json(serialize_agent_response(original)))
    before = _json(raw)
    assert raw["type"] == "agent_response"
    assert raw["messages"][0]["contents"][0]["type"] == "text"
    assert "schemaVersion" not in raw
    assert "_durable_response_version" not in raw
    child: CompletableTask[Any] = CompletableTask()
    if completed_before_wrap:
        child.complete(raw)
    agent, context = _orchestration_agent(child)

    task = agent.run("question", session=SESSION, options={"response_format": response_format})

    assert isinstance(task, DurableAgentTask)
    assert task.get_tasks() == [child]
    if not completed_before_wrap:
        assert not task.is_complete
        with pytest.raises(ValueError, match="not completed"):
            task.get_result()
        child.complete(raw)  # The real SDK invokes the parent callback.
    assert task.is_complete and not task.is_failed
    response = task.get_result()
    assert isinstance(response.value, BaseModel)
    assert type(response.value) is response_format
    assert _json(response.value.model_dump(mode="json", by_alias=True, round_trip=True)) == _json(value)
    assert response.text == "42"
    assert response.response_id == "inline-01"
    assert response.additional_properties == {"provider": OPAQUE}
    task.on_child_completed(child)
    assert task.get_result() is response
    assert context.call_entity.call_count == 1
    entity, operation, request = context.call_entity.call_args.args
    assert (entity, operation) == (ENTITY, "run")
    assert request["correlationId"] == CORRELATION
    assert request["orchestrationId"] == "orchestration-01"
    assert request["message"] == "question"
    context.signal_entity.assert_not_called()
    response.additional_properties["provider"]["nested"].append("consumer edit")
    assert _json(raw) == before


@pytest.mark.parametrize("completed_before_wrap", [False, True])
@pytest.mark.parametrize("delivery", ["available", "unavailable", "expired"])
@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
def test_real_task_accepts_projected_shared_results_as_inline_core_responses(
    completed_before_wrap: bool, delivery: str, outcome: str
) -> None:
    raw = _shared(outcome, delivery)
    if delivery == "available":
        _payload(raw)["value"] = {"aliasCount": 7}
    before = _json(raw)
    reader = read_agent_state(_json(raw))
    assert isinstance(reader, SharedAgentStateReader)
    projected = reader.try_get_agent_response(CORRELATION, now=NOW)
    assert projected is not None
    inline = json.loads(_json(serialize_agent_response(projected)))
    inline_before = _json(inline)
    child: CompletableTask[Any] = CompletableTask()
    if completed_before_wrap:
        child.complete(inline)

    task = DurableAgentTask(child, CountAnswer, CORRELATION)
    if not completed_before_wrap:
        assert not task.is_complete
        child.complete(inline)

    assert task.is_complete and not task.is_failed
    response = task.get_result()
    if delivery != "available":
        _assert_expired(response, outcome)
    elif outcome == "failed":
        assert response.additional_properties["durable_status"] == "error"
        assert [error.error_code for error in _errors(response)] == ["provider_failure"]
        assert response.value == {"aliasCount": 7}
        assert type(response.value) is dict
    else:
        assert response.value == CountAnswer(aliasCount=7)
        assert response.text == "canonical answer"
    task.on_child_completed(child)
    assert task.get_result() is response
    assert _json(inline) == inline_before
    assert _json(reader.to_dict()) == before
    assert _json(raw) == before


@pytest.mark.parametrize("completed_before_wrap", [False, True])
def test_real_task_propagates_the_child_failure_without_response_conversion(
    completed_before_wrap: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    child: CompletableTask[Any] = CompletableTask()
    propagated: list[Any] = []
    real_fail = DurableAgentTask.fail

    def observe_fail(self: DurableAgentTask, message: str, details: Any) -> None:
        propagated.append(details)
        real_fail(self, message, details)

    monkeypatch.setattr(DurableAgentTask, "fail", observe_fail)
    decoder = Mock(side_effect=AssertionError("A failed RPC must not be decoded as a response"))
    monkeypatch.setattr("agent_framework_durabletask._executors.load_agent_response", decoder)
    if completed_before_wrap:
        child.fail("entity call failed", ValueError("original provider failure"))

    task = DurableAgentTask(child, CountAnswer, CORRELATION)
    if not completed_before_wrap:
        assert not task.is_complete
        child.fail("entity call failed", ValueError("original provider failure"))

    original = child.get_exception()
    assert len(propagated) == 1 and propagated[0] is original
    assert task.is_complete and task.is_failed
    failure = task.get_exception()
    # The SDK wraps failures. Identity is preserved at the forwarding boundary,
    # not by promising that the parent's wrapper is the child's exception.
    assert failure.details.error_type == "TaskFailedError"
    assert failure.details.message == "entity call failed"
    with pytest.raises(TaskFailedError) as raised:
        task.get_result()
    assert raised.value is failure
    task.on_child_completed(child)
    assert task.get_exception() is failure
    assert child.get_exception() is original
    assert len(propagated) == 1
    decoder.assert_not_called()


@pytest.mark.parametrize("completed_before_wrap", [False, True])
def test_real_task_does_not_reinterpret_shared_content_as_inline_core_content(completed_before_wrap: bool) -> None:
    shared_response = _payload(_shared())
    before = _json(shared_response)
    child: CompletableTask[Any] = CompletableTask()
    if completed_before_wrap:
        child.complete(shared_response)

    task = DurableAgentTask(child, None, CORRELATION)
    if not completed_before_wrap:
        child.complete(shared_response)

    assert task.is_failed
    failure = task.get_exception()
    assert failure.details.error_type == "ValueError"
    assert "Content mapping requires 'type'" in failure.details.message
    with pytest.raises(TaskFailedError) as raised:
        task.get_result()
    assert raised.value is failure
    assert _json(shared_response) == before


def test_orchestration_fire_and_forget_still_returns_a_completed_acceptance_task() -> None:
    unused_child: CompletableTask[Any] = CompletableTask()
    agent, context = _orchestration_agent(unused_child)

    task = agent.run("question", session=SESSION, options={"wait_for_response": False})

    assert isinstance(task, DurableAgentTask)
    assert task.is_complete and not task.is_failed
    response = task.get_result()
    assert response.messages[0].role == "system"
    assert "accepted" in response.text and CORRELATION in response.text
    context.call_entity.assert_not_called()
    context.signal_entity.assert_called_once()
    entity, operation, request = context.signal_entity.call_args.args
    assert (entity, operation) == (ENTITY, "run")
    assert request["wait_for_response"] is False
    assert request["orchestrationId"] == "orchestration-01"
    assert not unused_child.is_complete
