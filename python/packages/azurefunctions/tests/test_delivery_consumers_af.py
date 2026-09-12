# Copyright (c) Microsoft. All rights reserved.

"""Azure Functions delivery through real state readers, HTTP handlers, and tasks."""

import json
from collections.abc import Awaitable, Callable
from copy import deepcopy
from datetime import date, datetime, timezone
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock, patch

import azure.durable_functions as df
import azure.functions as func
import pytest
from agent_framework import AgentResponse, Content, ContinuationToken, Message
from agent_framework_durabletask import (
    MIMETYPE_APPLICATION_JSON,
    MIMETYPE_TEXT_PLAIN,
    SESSION_ID_HEADER,
    WAIT_FOR_RESPONSE_HEADER,
    DurableAgentState,
    DurableAgentStateErrorResponse,
    DurableAgentStateResponse,
    RunRequest,
    serialize_agent_response,
)
from azure.durable_functions.models.actions.NoOpAction import NoOpAction
from azure.durable_functions.models.Task import AtomicTask, TaskState
from pydantic import BaseModel

from agent_framework_azurefunctions import AgentFunctionApp
from agent_framework_azurefunctions._entities import create_agent_entity
from agent_framework_azurefunctions._orchestration import AgentTask

CORRELATION_ID = "consumer-correlation"
SESSION_ID = "consumer-session"
AGENT_NAME = "consumer"
HISTORICAL_TIME = datetime(2024, 1, 1, tzinfo=timezone.utc)
EXPIRED_MESSAGE = "This request completed, but its response delivery window has expired."
HttpHandler = Callable[[func.HttpRequest, Any], Awaitable[func.HttpResponse]]


class Answer(BaseModel):
    answer: int


def _response(*, value: Any = None, text: str = "Readable answer") -> AgentResponse[Any]:
    return AgentResponse(
        messages=[
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
            )
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


def _runtime_error(*, include_text: bool = True) -> AgentResponse[Any]:
    response = _response()
    contents = [
        Content.from_error(
            message="Model endpoint unavailable",
            error_code="ProviderUnavailable",
            error_details="provider details",
            additional_properties={"retryable": False},
        )
    ]
    if include_text:
        contents.append(Content.from_text("ProviderUnavailable: Model endpoint unavailable"))
    response.messages.append(Message("system", contents, author_name="runtime", message_id="error-message"))
    return response


def _mailbox_state(
    response: AgentResponse[Any], *, expired: bool = False, cleanup: bool = False, legacy: bool = False
) -> dict[str, Any]:
    state = DurableAgentState()
    state.data.conversation_history.append(DurableAgentStateResponse.from_run_response(CORRELATION_ID, response))
    state.record_response(
        CORRELATION_ID,
        response,
        delivery_window_seconds=3600,
        now=HISTORICAL_TIME if expired else None,
        legacy=legacy,
    )
    if not expired:
        state.data.conversation_history.clear()
    if cleanup:
        state.expire_responses()
    return json.loads(state.to_json())


def _legacy_state(response: AgentResponse[Any], version: str, *, failed: bool = False) -> dict[str, Any]:
    state = DurableAgentState(schema_version=version)
    entry_type = DurableAgentStateErrorResponse if failed else DurableAgentStateResponse
    state.data.conversation_history.append(entry_type.from_run_response(CORRELATION_ID, response))
    return json.loads(state.to_json())


def _client(payload: dict[str, Any] | None) -> Mock:
    client = Mock(spec=df.DurableOrchestrationClient)
    client.signal_entity = AsyncMock()
    client.read_entity_state = AsyncMock(
        return_value=SimpleNamespace(entity_exists=payload is not None, entity_state=deepcopy(payload))
    )
    return client


def _request(*, plain_text: bool = False, wait: bool = True) -> func.HttpRequest:
    content_type = MIMETYPE_TEXT_PLAIN if plain_text else MIMETYPE_APPLICATION_JSON
    body = b"question" if plain_text else json.dumps({"message": "question", "session_id": SESSION_ID}).encode()
    return func.HttpRequest(
        method="POST",
        url=f"https://example.test/api/agents/{AGENT_NAME}/run",
        headers={"Content-Type": content_type, "Accept": content_type, WAIT_FOR_RESPONSE_HEADER: str(wait).lower()},
        params={"session_id": SESSION_ID},
        body=body,
    )


def _entity_context(payload: dict[str, Any] | None, operation: str = "run") -> Mock:
    context = Mock(spec=df.DurableEntityContext)
    context.operation_name = operation
    context.entity_name = f"dafx-{AGENT_NAME}"
    context.entity_key = SESSION_ID
    context.get_state.return_value = deepcopy(payload)
    context.get_input.return_value = RunRequest(message="question", correlation_id=CORRELATION_ID).to_dict()
    return context


def _task(payload: dict[str, Any], response_format: type[BaseModel] | None, *, precompleted: bool = False) -> AgentTask:
    child = AtomicTask(1, NoOpAction())
    if precompleted:
        child.set_value(is_error=False, value=payload)
    task = AgentTask(child, response_format, CORRELATION_ID)
    if not precompleted:
        assert not task.is_completed
        child.set_value(is_error=False, value=payload)
    return task


@pytest.fixture
def sleep(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    mocked = AsyncMock()
    monkeypatch.setattr("agent_framework_azurefunctions._app.asyncio.sleep", mocked)
    return mocked


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch) -> AgentFunctionApp:
    result = AgentFunctionApp(
        enable_health_check=False, enable_http_endpoints=False, max_poll_retries=3, poll_interval_seconds=0.01
    )
    monkeypatch.setattr(result, "_generate_unique_id", Mock(return_value=CORRELATION_ID))
    return result


@pytest.fixture
def http_handler(app: AgentFunctionApp, monkeypatch: pytest.MonkeyPatch) -> HttpHandler:
    handlers: list[HttpHandler] = []

    def identity(*args: Any, **kwargs: Any) -> Callable[[HttpHandler], HttpHandler]:
        return lambda handler: handler

    def route(*args: Any, **kwargs: Any) -> Callable[[HttpHandler], HttpHandler]:
        def capture(handler: HttpHandler) -> HttpHandler:
            handlers.append(handler)
            return handler

        return capture

    monkeypatch.setattr(app, "function_name", identity)
    monkeypatch.setattr(app, "route", route)
    monkeypatch.setattr(app, "durable_client_input", identity)
    app._setup_http_run_route(AGENT_NAME)
    return handlers[0]


@pytest.mark.parametrize("value", [None, 0, False, "", [], {}, {"answer": 42}])
async def test_http_success_keeps_text_and_adds_full_mailbox_snapshot(
    value: Any, http_handler: HttpHandler, sleep: AsyncMock
) -> None:
    original = _response(value=deepcopy(value))
    state = _mailbox_state(original)
    assert state["data"]["conversationHistory"] == []
    client = _client(state)

    response = await http_handler(_request(), client)

    assert response.status_code == 200
    assert response.mimetype == MIMETYPE_APPLICATION_JSON
    payload = json.loads(response.get_body())
    assert payload == {
        "response": original.text,
        "message": "question",
        "session_id": SESSION_ID,
        "status": "success",
        "correlation_id": CORRELATION_ID,
        "message_count": 0,
        "agent_response": state["data"]["responseMailbox"][CORRELATION_ID]["response"],
    }
    delivered = AgentResponse.from_dict(payload["agent_response"])
    assert delivered.to_dict() == original.to_dict()
    assert delivered.value == value
    assert type(delivered.value) is type(value)
    assert delivered.messages[0].author_name == "writer"
    assert delivered.messages[0].message_id == "answer-message"
    client.signal_entity.assert_awaited_once()
    entity_id = client.signal_entity.call_args.args[0]
    assert entity_id.name == f"dafx-{AGENT_NAME}"
    assert entity_id.key == SESSION_ID
    client.read_entity_state.assert_awaited_once_with(entity_id)
    sleep.assert_awaited_once_with(0.01)


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0"])
async def test_http_keeps_legacy_transcript_lookup(version: str, http_handler: HttpHandler, sleep: AsyncMock) -> None:
    original = _response()
    state = _legacy_state(original, version)
    client = _client(state)

    response = await http_handler(_request(), client)

    assert response.status_code == 200
    payload = json.loads(response.get_body())
    assert payload["status"] == "success"
    assert payload["response"] == original.text
    assert payload["message_count"] == 1
    delivered = AgentResponse.from_dict(payload["agent_response"])
    assert delivered.messages[0].author_name == "writer"
    assert delivered.messages[0].message_id == "answer-message"
    assert delivered.usage_details == original.usage_details
    assert state["schemaVersion"] == version
    client.read_entity_state.assert_awaited_once()
    sleep.assert_awaited_once_with(0.01)


@pytest.mark.parametrize("cleanup", [False, True])
@pytest.mark.parametrize("plain_text", [False, True])
@pytest.mark.parametrize("failed", [False, True])
async def test_http_expired_delivery_returns_410_without_waiting_for_more_polls(
    cleanup: bool, plain_text: bool, failed: bool, http_handler: HttpHandler, sleep: AsyncMock
) -> None:
    original = _runtime_error() if failed else _response(value={"answer": 42})
    client = _client(_mailbox_state(original, expired=True, cleanup=cleanup))

    response = await http_handler(_request(plain_text=plain_text), client)

    assert response.status_code == 410
    if plain_text:
        assert response.mimetype == MIMETYPE_TEXT_PLAIN
        assert response.get_body().decode() == EXPIRED_MESSAGE
        assert response.headers[SESSION_ID_HEADER] == SESSION_ID
        assert response.headers["x-ms-durable-outcome"] == ("failed" if failed else "succeeded")
    else:
        payload = json.loads(response.get_body())
        assert payload["status"] == "already_completed"
        assert payload["error_code"] == "response_expired"
        assert payload["error"] == EXPIRED_MESSAGE
        assert payload["response"] is None
        assert payload["message_count"] == 1
        assert payload["outcome"] == ("failed" if failed else "succeeded")
        assert payload["agent_response"]["additional_properties"] == {
            "durable_status": "already_completed",
            "correlation_id": CORRELATION_ID,
            "durable_outcome": "failed" if failed else "succeeded",
        }
        error = payload["agent_response"]["messages"][0]["contents"][0]
        assert error["error_code"] == "response_expired"
        assert error["message"] == EXPIRED_MESSAGE
    client.read_entity_state.assert_awaited_once()
    sleep.assert_awaited_once_with(0.01)


@pytest.mark.parametrize("expiry_marker", ["error_code", "durable_status"])
async def test_http_accepts_either_terminal_expiry_marker(
    expiry_marker: str, http_handler: HttpHandler, sleep: AsyncMock
) -> None:
    original = AgentResponse(messages=[])
    if expiry_marker == "error_code":
        original.messages = [
            Message("system", [Content.from_error(message=EXPIRED_MESSAGE, error_code="response_expired")])
        ]
    else:
        original.additional_properties["durable_status"] = "already_completed"
    # An older result can itself be unavailable. Revised writers require a known
    # invocation outcome, but readers must keep suppressing that old completion.
    client = _client(_mailbox_state(original, legacy=True))

    response = await http_handler(_request(), client)

    assert response.status_code == 410
    payload = json.loads(response.get_body())
    assert payload["status"] == "already_completed"
    assert payload["error_code"] == "response_expired"
    assert payload["error"] == EXPIRED_MESSAGE
    assert payload["agent_response"] == original.to_dict()
    client.read_entity_state.assert_awaited_once()
    sleep.assert_awaited_once_with(0.01)


async def test_http_error_without_code_or_message_is_still_a_failure(
    http_handler: HttpHandler, sleep: AsyncMock
) -> None:
    original = AgentResponse(messages=[Message("system", [Content.from_error()])])
    client = _client(_mailbox_state(original))

    response = await http_handler(_request(), client)

    assert response.status_code == 500
    payload = json.loads(response.get_body())
    assert payload["status"] == "error"
    assert payload["error_code"] is None
    assert payload["error"] == "Agent execution failed."
    assert payload["agent_response"] == original.to_dict()
    client.read_entity_state.assert_awaited_once()
    sleep.assert_awaited_once_with(0.01)


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "2.0.0"])
@pytest.mark.parametrize("include_text", [False, True])
async def test_http_runtime_error_is_500_not_success(
    version: str, include_text: bool, http_handler: HttpHandler, sleep: AsyncMock
) -> None:
    original = _runtime_error(include_text=include_text)
    state = _mailbox_state(original) if version == "2.0.0" else _legacy_state(original, version, failed=True)
    client = _client(state)

    response = await http_handler(_request(), client)

    assert response.status_code == 500
    payload = json.loads(response.get_body())
    assert payload["status"] == "error"
    assert payload["error_code"] == "ProviderUnavailable"
    assert payload["error"] == "Model endpoint unavailable"
    assert payload["response"] is None
    assert payload["message"] == "question"
    assert payload["session_id"] == SESSION_ID
    assert payload["correlation_id"] == CORRELATION_ID
    delivered = AgentResponse.from_dict(payload["agent_response"])
    assert delivered.messages[1].contents[0].error_details == "provider details"
    if version == "2.0.0":
        assert delivered.to_dict() == original.to_dict()
        assert payload["message_count"] == 0
    client.read_entity_state.assert_awaited_once()
    sleep.assert_awaited_once_with(0.01)


async def test_plain_text_runtime_error_returns_the_error_not_partial_success(
    http_handler: HttpHandler, sleep: AsyncMock
) -> None:
    client = _client(_mailbox_state(_runtime_error()))

    response = await http_handler(_request(plain_text=True), client)

    assert response.status_code == 500
    assert response.get_body().decode() == "Model endpoint unavailable"
    client.read_entity_state.assert_awaited_once()
    sleep.assert_awaited_once_with(0.01)


async def test_http_missing_response_keeps_the_existing_timeout(
    http_handler: HttpHandler, app: AgentFunctionApp, sleep: AsyncMock
) -> None:
    client = _client(None)

    response = await http_handler(_request(), client)

    assert response.status_code == 500
    assert json.loads(response.get_body()) == {
        "response": "Agent is still processing or timed out...",
        "message": "question",
        "session_id": SESSION_ID,
        "status": "timeout",
        "correlation_id": CORRELATION_ID,
    }
    client.signal_entity.assert_awaited_once()
    assert client.read_entity_state.await_count == app.max_poll_retries
    assert sleep.await_count == app.max_poll_retries


async def test_http_signal_without_waiting_still_returns_202(http_handler: HttpHandler, sleep: AsyncMock) -> None:
    client = _client(_mailbox_state(_response(), expired=True))

    response = await http_handler(_request(wait=False), client)

    assert response.status_code == 202
    assert json.loads(response.get_body()) == {
        "response": "Agent request accepted",
        "message": "question",
        "session_id": SESSION_ID,
        "status": "accepted",
        "correlation_id": CORRELATION_ID,
    }
    client.signal_entity.assert_awaited_once()
    client.read_entity_state.assert_not_awaited()
    sleep.assert_not_awaited()


@pytest.mark.parametrize("expired", [False, True])
async def test_mcp_raises_for_expired_and_failed_delivery(
    expired: bool, app: AgentFunctionApp, sleep: AsyncMock
) -> None:
    client = _client(_mailbox_state(_runtime_error(), expired=expired, cleanup=expired))
    expected = EXPIRED_MESSAGE if expired else "Model endpoint unavailable"

    with pytest.raises(RuntimeError, match=expected) as error:
        await app._handle_mcp_tool_invocation(
            AGENT_NAME, json.dumps({"arguments": {"query": "question", "sessionId": SESSION_ID}}), client
        )
    if expired:
        assert "Invocation outcome: failed." in str(error.value)

    client.signal_entity.assert_awaited_once()
    client.read_entity_state.assert_awaited_once()
    sleep.assert_awaited_once_with(0.01)


def test_success_helper_keeps_its_existing_signature_and_payload(app: AgentFunctionApp) -> None:
    state = DurableAgentState()

    assert app._build_success_result("answer", "question", SESSION_ID, CORRELATION_ID, state) == {
        "response": "answer",
        "message": "question",
        "session_id": SESSION_ID,
        "status": "success",
        "correlation_id": CORRELATION_ID,
        "message_count": 0,
    }


@pytest.mark.parametrize("value", [None, 0, False, "", [], {}, {"answer": 42}])
@pytest.mark.parametrize("operation", ["run", "run_agent"])
def test_entity_factory_delivers_cold_mailbox_without_rerunning_agent(value: Any, operation: str) -> None:
    original = _response(value=deepcopy(value))
    state = _mailbox_state(original)
    agent = Mock(context_providers=None)
    agent.run = AsyncMock()
    context = _entity_context(state, operation)

    create_agent_entity(agent)(context)

    context.set_result.assert_called_once()
    payload = json.loads(json.dumps(context.set_result.call_args.args[0], allow_nan=False))
    assert payload == state["data"]["responseMailbox"][CORRELATION_ID]["response"]
    delivered = AgentResponse.from_dict(payload)
    assert delivered.value == value
    assert type(delivered.value) is type(value)
    agent.run.assert_not_called()
    context.set_state.assert_not_called()


def test_entity_factory_serializes_live_pydantic_value_in_json_mode() -> None:
    class DatedAnswer(BaseModel):
        answer: int
        day: date

    original = _response(value=DatedAnswer(answer=42, day=date(2026, 9, 8)))
    context = _entity_context(None)
    with patch("agent_framework_azurefunctions._entities.AgentEntity") as entity:
        entity.return_value.run = AsyncMock(return_value=original)
        create_agent_entity(Mock(context_providers=None))(context)
        entity.return_value.run.assert_awaited_once_with(context.get_input.return_value)

    context.set_result.assert_called_once()
    payload = json.loads(json.dumps(context.set_result.call_args.args[0], allow_nan=False))
    assert payload == {**original.to_dict(), "value": {"answer": 42, "day": "2026-09-08"}}


@pytest.mark.parametrize("cleanup", [False, True])
def test_entity_factory_and_task_keep_expired_delivery_terminal(cleanup: bool) -> None:
    agent = Mock(context_providers=None)
    agent.run = AsyncMock()
    state = _mailbox_state(_response(), expired=True, cleanup=cleanup)
    before = deepcopy(state)
    context = _entity_context(state)

    create_agent_entity(agent)(context)

    context.set_result.assert_called_once()
    payload = json.loads(json.dumps(context.set_result.call_args.args[0]))
    task = _task(payload, Answer)
    assert task.state == TaskState.SUCCEEDED
    assert isinstance(task.result, AgentResponse)
    assert task.result.additional_properties == {
        "durable_status": "already_completed",
        "correlation_id": CORRELATION_ID,
        "durable_outcome": "succeeded",
    }
    assert task.result.messages[0].contents[0].error_code == "response_expired"
    assert task.result.messages[0].contents[0].message == EXPIRED_MESSAGE
    assert task.result.value is None
    agent.run.assert_not_called()
    if cleanup:
        context.set_state.assert_not_called()
    else:
        context.set_state.assert_called_once()
        persisted = context.set_state.call_args.args[0]
        expected = deepcopy(state)
        del expected["data"]["responseMailbox"]
        assert persisted == expected
        assert persisted["data"]["completedCorrelations"] == state["data"]["completedCorrelations"]
        assert CORRELATION_ID in persisted["data"]["completedCorrelations"]
    assert state == before


@pytest.mark.parametrize("response_format", [None, Answer])
@pytest.mark.parametrize("precompleted", [False, True])
def test_functions_task_keeps_snapshot_metadata_and_structured_value(
    response_format: type[BaseModel] | None, precompleted: bool
) -> None:
    original = _response(value={"answer": 42})
    payload = _mailbox_state(original)["data"]["responseMailbox"][CORRELATION_ID]["response"]

    task = _task(payload, response_format, precompleted=precompleted)

    assert task.state == TaskState.SUCCEEDED
    assert isinstance(task.result, AgentResponse)
    assert task.result.to_dict() == original.to_dict()
    if response_format is None:
        assert task.result.value == {"answer": 42}
    else:
        assert isinstance(task.result.value, Answer)
        assert task.result.value.answer == 42


@pytest.mark.parametrize("terminal_kind", ["error", "already_completed"])
@pytest.mark.parametrize("text", ["not JSON", '{"answer":0}'])
@pytest.mark.parametrize("precompleted", [False, True])
def test_functions_task_does_not_parse_error_or_status_only_responses(
    terminal_kind: str, text: str, precompleted: bool
) -> None:
    original = _response(text=text)
    if terminal_kind == "error":
        original.messages.append(Message("system", [Content.from_error(message="Failure", error_code="RuntimeError")]))
    else:
        original.additional_properties["durable_status"] = "already_completed"

    payload = json.loads(json.dumps(serialize_agent_response(original)))
    task = _task(payload, Answer, precompleted=precompleted)

    assert task.state == TaskState.SUCCEEDED
    assert isinstance(task.result, AgentResponse)
    assert task.result.to_dict() == original.to_dict()
    assert task.result.value is None


def test_functions_task_still_rejects_invalid_success_schema() -> None:
    task = _task(_response(text='{"wrong":42}').to_dict(), Answer)

    assert task.state == TaskState.FAILED
    assert isinstance(task.result, ValueError)
