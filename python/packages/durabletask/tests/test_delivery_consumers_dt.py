# Copyright (c) Microsoft. All rights reserved.

"""Delivery consumers using real core responses, JSON reloads, and durable tasks."""

import json
from copy import deepcopy
from datetime import date, datetime, timezone
from typing import Any, cast
from unittest.mock import Mock

import pytest
from agent_framework import AgentResponse, Content, ContinuationToken, Message
from durabletask.client import TaskHubGrpcClient
from durabletask.task import CompletableTask
from pydantic import BaseModel, ConfigDict, RootModel

from agent_framework_durabletask import (
    DurableAgentState,
    DurableAgentStateErrorResponse,
    DurableAgentStateResponse,
    RunRequest,
    ensure_response_format,
    load_agent_response,
    serialize_agent_response,
)
from agent_framework_durabletask._executors import ClientAgentExecutor, DurableAgentTask

CORRELATION_ID = "consumer-correlation"
HISTORICAL_TIME = datetime(2024, 1, 1, tzinfo=timezone.utc)


class Answer(BaseModel):
    answer: int


def _response(*, value: Any = None, text: str = "Readable answer") -> AgentResponse[Any]:
    return AgentResponse(
        messages=[
            Message(
                "tool",
                [Content.from_function_result("call-1", result=[Content.from_text("lookup result")])],
                author_name="lookup",
                message_id="tool-message",
            ),
            Message(
                "assistant",
                [
                    Content.from_text(
                        text,
                        annotations=[{"type": "citation", "title": "Source", "url": "https://example.test/source"}],
                        additional_properties={"provider": {"labels": ["content"]}},
                        raw_representation=object(),
                    )
                ],
                author_name="writer",
                message_id="answer-message",
                additional_properties={"provider": {"labels": ["message"]}},
                raw_representation=object(),
            ),
        ],
        response_id="response-1",
        agent_id="agent-1",
        created_at=HISTORICAL_TIME.isoformat(),
        finish_reason="stop",
        usage_details={"input_token_count": 3, "output_token_count": 2, "total_token_count": 5},
        continuation_token=cast(ContinuationToken, {"cursor": {"pages": [1, 2]}}),
        additional_properties={"provider": {"labels": ["response"]}},
        raw_representation=object(),
        value=value,
    )


def _mailbox_state(response: AgentResponse[Any], *, expired: bool = False, cleanup: bool = False) -> str:
    state = DurableAgentState()
    state.data.conversation_history.append(DurableAgentStateResponse.from_run_response(CORRELATION_ID, response))
    state.record_response(
        CORRELATION_ID,
        response,
        delivery_window_seconds=3600,
        now=HISTORICAL_TIME if expired else None,
    )
    if not expired:
        state.data.conversation_history.clear()
    if cleanup:
        state.expire_responses()
    return state.to_json()


def _client(state_json: str | None) -> tuple[ClientAgentExecutor, Mock]:
    client = Mock(spec=TaskHubGrpcClient)
    if state_json is None:
        client.get_entity.return_value = None
    else:
        client.get_entity.return_value.get_state.return_value = state_json
    return ClientAgentExecutor(client, max_poll_retries=3, poll_interval_seconds=0.01), client


def _task(
    payload: dict[str, Any], response_format: type[BaseModel] | None, *, precompleted: bool = False
) -> DurableAgentTask:
    child: CompletableTask[Any] = CompletableTask()
    if precompleted:
        child.complete(payload)
    task = DurableAgentTask(child, response_format, CORRELATION_ID)
    if not precompleted:
        assert not task.is_complete
        child.complete(payload)
    return task


def _assert_expired(response: AgentResponse[Any]) -> None:
    assert response.additional_properties == {
        "durable_status": "already_completed",
        "correlation_id": CORRELATION_ID,
    }
    content = response.messages[0].contents[0]
    assert content.type == "error"
    assert content.error_code == "response_expired"
    assert content.message == "This request completed, but its response delivery window has expired."
    assert response.value is None


@pytest.fixture
def sleep(monkeypatch: pytest.MonkeyPatch) -> Mock:
    mocked = Mock()
    monkeypatch.setattr("agent_framework_durabletask._executors.time.sleep", mocked)
    return mocked


@pytest.mark.parametrize("value", [None, 0, False, "", [], {}, {"items": [{"answer": 42}]}])
def test_public_serializer_and_loader_preserve_values_and_core_metadata(value: Any) -> None:
    response = _response(value=deepcopy(value))
    expected = response.to_dict()
    if value is not None:
        expected["value"] = deepcopy(value)

    snapshot = json.loads(json.dumps(serialize_agent_response(response), allow_nan=False))

    assert snapshot == expected
    assert ("value" in snapshot) is (value is not None)
    loaded = load_agent_response(snapshot)
    assert isinstance(loaded, AgentResponse)
    assert loaded.value == value
    assert type(loaded.value) is type(value)
    assert loaded.to_dict() == response.to_dict()
    assert all(isinstance(message, Message) for message in loaded.messages)
    assert all(isinstance(content, Content) for message in loaded.messages for content in message.contents)
    assert loaded.messages[1].author_name == "writer"
    assert loaded.messages[1].message_id == "answer-message"
    assert loaded.messages[1].contents[0].annotations == response.messages[1].contents[0].annotations
    assert loaded.messages[0].contents[0].items == response.messages[0].contents[0].items
    assert "raw_representation" not in snapshot
    assert "raw_representation" not in snapshot["messages"][1]
    assert "raw_representation" not in snapshot["messages"][1]["contents"][0]
    assert load_agent_response(loaded) is loaded


def test_public_serializer_uses_json_mode_for_pydantic_values() -> None:
    class DatedAnswer(BaseModel):
        answer: int
        day: date

    response = _response(value=DatedAnswer(answer=42, day=date(2026, 9, 8)))
    snapshot = json.loads(json.dumps(serialize_agent_response(response), allow_nan=False))

    assert snapshot["value"] == {"answer": 42, "day": "2026-09-08"}
    assert load_agent_response(snapshot).value == snapshot["value"]


@pytest.mark.parametrize("response_format", [Answer, Answer.model_json_schema()])
def test_public_serializer_captures_a_lazy_structured_value(response_format: Any) -> None:
    response = AgentResponse(messages=[Message("assistant", ['{"answer":42}'])], response_format=response_format)

    snapshot = json.loads(json.dumps(serialize_agent_response(response)))

    assert snapshot["value"] == {"answer": 42}
    assert load_agent_response(snapshot).value == {"answer": 42}


@pytest.mark.parametrize("response_format", [None, Answer])
def test_client_reads_full_mailbox_response_after_cold_reload_and_transcript_pruning(
    response_format: type[BaseModel] | None, sleep: Mock
) -> None:
    response = _response(value={"answer": 42})
    state_json = _mailbox_state(response)
    assert json.loads(state_json)["data"]["conversationHistory"] == []
    executor, client = _client(state_json)

    result = executor.run_durable_agent(
        "consumer", RunRequest(message="question", correlation_id=CORRELATION_ID, response_format=response_format)
    )

    assert result.to_dict() == response.to_dict()
    if response_format is None:
        assert result.value == {"answer": 42}
    else:
        assert isinstance(result.value, Answer)
        assert result.value.answer == 42
    client.signal_entity.assert_called_once()
    entity_id = client.signal_entity.call_args.args[0]
    client.get_entity.assert_called_once_with(entity_id, include_state=True)
    sleep.assert_called_once_with(0.01)


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0"])
@pytest.mark.parametrize("failed", [False, True])
def test_client_retains_legacy_lookup_and_does_not_reparse_legacy_errors(
    version: str, failed: bool, sleep: Mock
) -> None:
    response = _response(text='{"answer":42}')
    if failed:
        response.messages[1].contents.append(Content.from_error(message="Provider failed", error_code="RuntimeError"))
    state = DurableAgentState(schema_version=version)
    entry_type = DurableAgentStateErrorResponse if failed else DurableAgentStateResponse
    state.data.conversation_history.append(entry_type.from_run_response(CORRELATION_ID, response))
    state_json = state.to_json()
    executor, client = _client(state_json)

    result = executor.run_durable_agent(
        "consumer", RunRequest(message="question", correlation_id=CORRELATION_ID, response_format=Answer)
    )

    assert result.text == response.text
    assert result.messages[1].author_name == "writer"
    assert result.messages[1].message_id == "answer-message"
    assert result.usage_details == response.usage_details
    if failed:
        assert result.messages[1].contents[-1].error_code == "RuntimeError"
        assert result.value is None
    else:
        assert isinstance(result.value, Answer)
        assert result.value.answer == 42
    client.get_entity.assert_called_once()
    sleep.assert_called_once_with(0.01)
    assert json.loads(state_json)["schemaVersion"] == version


@pytest.mark.parametrize("response_format", [None, Answer])
@pytest.mark.parametrize("cleanup", [False, True])
def test_expired_client_delivery_is_terminal_on_the_first_read(
    response_format: type[BaseModel] | None, cleanup: bool, sleep: Mock
) -> None:
    executor, client = _client(_mailbox_state(_response(value={"answer": 42}), expired=True, cleanup=cleanup))

    result = executor.run_durable_agent(
        "consumer", RunRequest(message="question", correlation_id=CORRELATION_ID, response_format=response_format)
    )

    _assert_expired(result)
    client.signal_entity.assert_called_once()
    client.get_entity.assert_called_once()
    sleep.assert_called_once_with(0.01)


@pytest.mark.parametrize("response_format", [None, Answer])
@pytest.mark.parametrize("precompleted", [False, True])
def test_task_reconstructs_snapshot_value_and_metadata(
    response_format: type[BaseModel] | None, precompleted: bool
) -> None:
    response = _response(value={"answer": 42})
    payload = json.loads(_mailbox_state(response))["data"]["responseMailbox"][CORRELATION_ID]["response"]

    task = _task(payload, response_format, precompleted=precompleted)

    assert task.is_complete and not task.is_failed
    result = task.get_result()
    assert isinstance(result, AgentResponse)
    assert result.to_dict() == response.to_dict()
    assert result.messages[1].author_name == "writer"
    if response_format is None:
        assert result.value == {"answer": 42}
    else:
        assert isinstance(result.value, Answer)
        assert result.value.answer == 42


@pytest.mark.parametrize("precompleted", [False, True])
@pytest.mark.parametrize("cleanup", [False, True])
def test_task_returns_expired_status_instead_of_failing_schema_validation(precompleted: bool, cleanup: bool) -> None:
    state = DurableAgentState.from_json(_mailbox_state(_response(), expired=True, cleanup=cleanup))
    expired = state.try_get_agent_response(CORRELATION_ID)
    assert isinstance(expired, AgentResponse)

    task = _task(json.loads(json.dumps(serialize_agent_response(expired))), Answer, precompleted=precompleted)

    assert task.is_complete and not task.is_failed
    _assert_expired(task.get_result())


@pytest.mark.parametrize("terminal_kind", ["error", "already_completed"])
@pytest.mark.parametrize("text", ["not JSON", '{"answer":0}'])
def test_terminal_response_formats_skip_all_messages_and_status_only_results(terminal_kind: str, text: str) -> None:
    response = _response(text=text)
    if terminal_kind == "error":
        response.messages[1].contents.append(Content.from_error(message="Provider failed", error_code="RuntimeError"))
    else:
        response.additional_properties["durable_status"] = "already_completed"
    snapshot = json.loads(json.dumps(serialize_agent_response(response)))

    direct = load_agent_response(deepcopy(snapshot))
    ensure_response_format(Answer, CORRELATION_ID, direct)
    executor, _ = _client(None)
    polled = executor._handle_agent_response(load_agent_response(deepcopy(snapshot)), Answer, CORRELATION_ID)
    task = _task(deepcopy(snapshot), Answer)

    assert task.is_complete and not task.is_failed
    for result in (direct, polled, task.get_result()):
        assert result.to_dict() == response.to_dict()
        assert result.value is None


def test_response_format_validates_the_saved_value_not_conflicting_text() -> None:
    response = _response(value={"answer": 42}, text='{"answer":0}')

    ensure_response_format(Answer, CORRELATION_ID, response)

    assert isinstance(response.value, Answer)
    assert response.value.answer == 42
    assert response.messages[1].text == '{"answer":0}'


def test_response_format_does_not_replace_an_invalid_saved_value_with_valid_text() -> None:
    response = _response(value={"wrong": 42}, text='{"answer":0}')

    with pytest.raises(ValueError):
        ensure_response_format(Answer, CORRELATION_ID, response)


def test_response_format_keeps_a_matching_pydantic_value() -> None:
    value = Answer(answer=42)
    response = _response(value=value)

    ensure_response_format(Answer, CORRELATION_ID, response)

    assert response.value is value


def test_response_format_uses_json_validation_for_saved_strict_models() -> None:
    class StrictAnswer(BaseModel):
        model_config = ConfigDict(strict=True)
        day: date
        coordinates: tuple[int, int]

    value = StrictAnswer(day=date(2026, 9, 8), coordinates=(1, 2))
    payload = json.loads(json.dumps(serialize_agent_response(_response(value=value))))
    response = load_agent_response(payload)

    ensure_response_format(StrictAnswer, CORRELATION_ID, response)

    assert isinstance(response.value, StrictAnswer)
    assert response.value == value


@pytest.mark.parametrize("value", [0, False, "", [], {}])
def test_response_format_preserves_falsey_saved_values(value: Any) -> None:
    class SavedValue(RootModel[Any]):
        pass

    response = _response(value=deepcopy(value))

    ensure_response_format(SavedValue, CORRELATION_ID, response)

    assert isinstance(response.value, SavedValue)
    assert response.value.root == value
    assert type(response.value.root) is type(value)


def test_response_format_override_still_controls_unparsed_responses() -> None:
    class OtherAnswer(BaseModel):
        missing: str

    response = AgentResponse(messages=[Message("assistant", ['{"answer":42}'])], response_format=OtherAnswer)

    ensure_response_format(Answer, CORRELATION_ID, response)

    assert isinstance(response.value, Answer)
    assert response.value.answer == 42


def test_successful_invalid_schema_still_fails_validation() -> None:
    response = _response(text='{"wrong":42}')
    executor, _ = _client(None)

    with pytest.raises(ValueError):
        ensure_response_format(Answer, CORRELATION_ID, response)
    polled = executor._handle_agent_response(load_agent_response(response.to_dict()), Answer, CORRELATION_ID)
    assert polled.messages[0].contents[0].error_code == "response_processing_error"
    task = _task(response.to_dict(), Answer)
    assert task.is_complete and task.is_failed


def test_missing_response_keeps_the_bounded_timeout_behavior(sleep: Mock) -> None:
    executor, client = _client(None)

    result = executor.run_durable_agent(
        "consumer", RunRequest(message="question", correlation_id=CORRELATION_ID, response_format=Answer)
    )

    assert result.messages[0].contents[0].error_code == "response_timeout"
    assert client.get_entity.call_count == executor.max_poll_retries
    assert sleep.call_count == executor.max_poll_retries
