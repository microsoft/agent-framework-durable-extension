# Copyright (c) Microsoft. All rights reserved.

"""Read-only shared snapshots through registered Functions HTTP, MCP and proxy paths."""

import json
from collections.abc import Awaitable, Callable
from copy import deepcopy
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import azure.durable_functions as df
import azure.functions as func
import pytest
from agent_framework import AgentResponse
from agent_framework_durabletask import (
    MIMETYPE_APPLICATION_JSON,
    MIMETYPE_TEXT_PLAIN,
    SESSION_ID_HEADER,
    WAIT_FOR_RESPONSE_HEADER,
    DurableAgentState,
    SharedAgentStateReader,
    load_agent_response,
)
from azure.durable_functions.models.actions.NoOpAction import NoOpAction
from azure.durable_functions.models.Task import AtomicTask, TaskState
from pydantic import BaseModel, RootModel

from agent_framework_azurefunctions import AgentFunctionApp

AGENT_NAME = "shared-consumer"
CORRELATION_ID = "shared-correlation"
SESSION_ID = "shared-session"
COMPLETED_AT = "2024-01-01T00:00:00Z"
EXPIRED_AT = "2024-01-01T00:01:00Z"
EXPIRED_MESSAGE = "This request completed, but its response delivery window has expired."
ERROR_MESSAGE = "Response failed schema validation."
HttpHandler = Callable[[func.HttpRequest, Any], Awaitable[func.HttpResponse]]
McpHandler = Callable[[str, Any], Awaitable[str]]


class Answer(BaseModel):
    answer: int


class NullAnswer(RootModel[None]):
    pass


def _wire_response(text: str = "Readable answer") -> dict[str, Any]:
    return {
        "messages": [
            {
                "role": "assistant",
                "authorName": "writer",
                "messageId": "answer-message",
                "contents": [{"$type": "text", "text": text}],
                "extensionData": {"provider": {"labels": ["message"]}},
            }
        ],
        "responseId": "response-1",
        "agentId": "agent-1",
        "createdAt": COMPLETED_AT,
        "finishReason": "stop",
        "usage": {"inputTokenCount": 3, "outputTokenCount": 2, "totalTokenCount": 5},
        "extensionData": {"provider": {"labels": ["response"]}},
    }


def _shared_state(
    *,
    outcome: str = "succeeded",
    availability: str = "available",
    response: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Literal canonical JSON only, independent of mutable producers and history fixtures."""
    completion: dict[str, Any] = {
        "correlationId": CORRELATION_ID,
        "outcome": outcome,
        "completedAt": COMPLETED_AT,
    }
    if availability == "expired":
        completion["resultExpiresAt"] = EXPIRED_AT
    receipt = {**completion, "resultState": "available"}
    result = {**completion, "response": deepcopy(response) if response is not None else _wire_response()}
    if outcome == "failed":
        result["error"] = {"code": "schema_error", "message": ERROR_MESSAGE, "details": {"retryable": False}}
    results = {CORRELATION_ID: result}
    if availability == "unavailable":
        receipt.update(resultState="unavailable", resultUnavailableAt=EXPIRED_AT)
        results = {}
    return {
        "schemaVersion": "2.0.0",
        "futureRoot": {"keep": ["雪", None]},
        "data": {
            "conversationHistory": [],
            "terminalResults": results,
            "completionReceipts": {CORRELATION_ID: receipt},
            "session": {"state": {"application": {"keep": [False, 0]}}},
            "historyBinding": {"profile": "foreign-history", "version": 42},
        },
    }


def _history_entry(*, error: bool = False) -> dict[str, Any]:
    content = (
        {"$type": "error", "errorCode": "ProviderUnavailable", "message": "Legacy error"}
        if error
        else {"$type": "text", "text": "Legacy transcript answer"}
    )
    return {
        "$type": "response",
        "correlationId": CORRELATION_ID,
        "createdAt": COMPLETED_AT,
        "messages": [{"role": "assistant", "contents": [content]}],
    }


def _client(raw: Any, *, exists: bool = True) -> Mock:
    client = Mock(spec=df.DurableOrchestrationClient)
    client.signal_entity = AsyncMock()
    client.read_entity_state = AsyncMock(return_value=SimpleNamespace(entity_exists=exists, entity_state=deepcopy(raw)))
    return client


def _request(*, plain_text: bool = False, wait: bool = True) -> func.HttpRequest:
    content_type = MIMETYPE_TEXT_PLAIN if plain_text else MIMETYPE_APPLICATION_JSON
    return func.HttpRequest(
        method="POST",
        url=f"https://example.test/api/agents/{AGENT_NAME}/run",
        headers={"Content-Type": content_type, "Accept": content_type, WAIT_FOR_RESPONSE_HEADER: str(wait).lower()},
        params={"session_id": SESSION_ID},
        body=b"question" if plain_text else json.dumps({"message": "question", "session_id": SESSION_ID}).encode(),
    )


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch) -> AgentFunctionApp:
    result = AgentFunctionApp(
        enable_health_check=False, enable_http_endpoints=False, max_poll_retries=3, poll_interval_seconds=0.01
    )
    monkeypatch.setattr(result, "_generate_unique_id", Mock(return_value=CORRELATION_ID))
    return result


@pytest.fixture
def sleep(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    result = AsyncMock()
    monkeypatch.setattr("agent_framework_azurefunctions._app.asyncio.sleep", result)
    return result


@pytest.fixture
def handlers(app: AgentFunctionApp, monkeypatch: pytest.MonkeyPatch) -> tuple[HttpHandler, McpHandler]:
    captured: dict[str, Any] = {}

    def identity(*args: Any, **kwargs: Any) -> Any:
        return lambda handler: handler

    def route(*args: Any, **kwargs: Any) -> Any:
        assert kwargs == {"route": f"agents/{AGENT_NAME}/run", "methods": ["POST"]}

        def capture(handler: HttpHandler) -> HttpHandler:
            captured["http"] = handler
            return handler

        return capture

    def mcp(*args: Any, **kwargs: Any) -> Any:
        assert kwargs["tool_name"] == AGENT_NAME
        assert [item["propertyName"] for item in json.loads(kwargs["tool_properties"])] == ["query", "sessionId"]

        def capture(handler: McpHandler) -> McpHandler:
            captured["mcp"] = handler
            return handler

        return capture

    monkeypatch.setattr(app, "function_name", identity)
    monkeypatch.setattr(app, "durable_client_input", identity)
    monkeypatch.setattr(app, "entity_trigger", identity)
    monkeypatch.setattr(app, "route", route)
    monkeypatch.setattr(app, "mcp_tool_trigger", mcp)
    agent = Mock()
    agent.name = AGENT_NAME
    agent.description = "Shared snapshot consumer"
    app.add_agent(agent, enable_http_endpoint=True, enable_mcp_tool_trigger=True)
    assert app.workflows == {}
    assert set(captured) == {"http", "mcp"}
    return captured["http"], captured["mcp"]


def _assert_one_delivery(client: Mock, sleep: AsyncMock, original: Any) -> None:
    client.signal_entity.assert_awaited_once()
    entity_id, operation, request = client.signal_entity.call_args.args
    assert entity_id.name == f"dafx-{AGENT_NAME}" and entity_id.key == SESSION_ID
    assert operation == "run" and request["correlationId"] == CORRELATION_ID
    assert request["message"] == "question"
    client.read_entity_state.assert_awaited_once_with(entity_id)
    assert client.read_entity_state.return_value.entity_state == original
    sleep.assert_awaited_once_with(0.01)


@pytest.mark.parametrize("value", [None, 0, False, "", [], {}, {"answer": 42}])
async def test_http_shared_success_delivers_snapshot_and_falsey_values(
    value: Any, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    stored = _wire_response()
    stored["value"] = deepcopy(value)
    stored["futureResponse"] = {"opaque": [0, False, None]}
    raw = _shared_state(response=stored)
    client = _client(raw)

    response = await handlers[0](_request(), client)

    assert response.status_code == 200 and response.mimetype == MIMETYPE_APPLICATION_JSON
    payload = json.loads(response.get_body())
    assert set(payload) == {
        "response",
        "message",
        "session_id",
        "status",
        "correlation_id",
        "message_count",
        "agent_response",
    }
    assert payload["response"] == "Readable answer" and payload["status"] == "success"
    assert payload["message"] == "question" and payload["session_id"] == SESSION_ID
    assert payload["correlation_id"] == CORRELATION_ID and payload["message_count"] == 0
    snapshot = payload["agent_response"]
    assert "value" in snapshot and snapshot["value"] == value
    assert type(snapshot["value"]) is type(value)
    assert "futureResponse" not in snapshot
    delivered = load_agent_response(snapshot)
    assert delivered.response_id == "response-1" and delivered.agent_id == "agent-1"
    assert delivered.finish_reason == "stop"
    assert delivered.usage_details == {"input_token_count": 3, "output_token_count": 2, "total_token_count": 5}
    assert delivered.messages[0].author_name == "writer" and delivered.messages[0].message_id == "answer-message"
    assert delivered.additional_properties == {"provider": {"labels": ["response"]}}
    _assert_one_delivery(client, sleep, raw)
    snapshot["additional_properties"]["provider"]["labels"].append("consumer edit")
    assert client.read_entity_state.return_value.entity_state == raw


@pytest.mark.parametrize("value", [None, False, 0, "", [], {}])
async def test_plain_text_shared_empty_success_never_echoes_input(
    value: Any, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    stored = _wire_response("")
    stored["value"] = value
    raw = _shared_state(response=stored)
    client = _client(raw)

    response = await handlers[0](_request(plain_text=True), client)

    assert response.status_code == 200 and response.get_body() == b""
    assert response.headers[SESSION_ID_HEADER] == SESSION_ID
    assert "x-ms-durable-outcome" not in response.headers
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize("availability", ["expired", "unavailable"])
@pytest.mark.parametrize("plain_text", [False, True])
async def test_http_unavailable_preserves_receipt_outcome_and_stops_polling(
    outcome: str,
    availability: str,
    plain_text: bool,
    handlers: tuple[HttpHandler, McpHandler],
    sleep: AsyncMock,
) -> None:
    raw = _shared_state(outcome=outcome, availability=availability)
    raw["data"]["conversationHistory"] = [_history_entry()]
    client = _client(raw)

    response = await handlers[0](_request(plain_text=plain_text), client)

    assert response.status_code == 410
    if plain_text:
        assert response.get_body().decode() == EXPIRED_MESSAGE
        assert response.headers[SESSION_ID_HEADER] == SESSION_ID
        assert response.headers["x-ms-durable-outcome"] == outcome
    else:
        payload = json.loads(response.get_body())
        assert payload["status"] == "completed_unavailable" and payload["response"] is None
        assert payload["error_code"] == "response_expired" and payload["error"] == EXPIRED_MESSAGE
        assert payload["outcome"] == outcome and payload["message_count"] == 1
        assert payload["agent_response"]["additional_properties"] == {
            "durable_status": "already_completed",
            "durable_outcome": outcome,
            "correlation_id": CORRELATION_ID,
        }
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize("hint", [None, "accepted", "already_completed"])
@pytest.mark.parametrize("plain_text", [False, True])
async def test_live_failed_receipt_overrides_provider_delivery_hints(
    hint: str | None, plain_text: bool, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    stored = _wire_response('{"answer":"invalid"}')
    stored["value"] = {"answer": "invalid"}
    if hint is not None:
        stored["extensionData"]["durable_status"] = hint
    stored["messages"].insert(0, {"role": "tool", "contents": [{"$type": "error", "message": "Tool-only error"}]})
    stored["messages"].append({
        "role": "system",
        "contents": [{"$type": "error", "errorCode": "response_expired", "message": ERROR_MESSAGE}],
    })
    raw = _shared_state(outcome="failed", response=stored)
    client = _client(raw)

    response = await handlers[0](_request(plain_text=plain_text), client)

    assert response.status_code == 500
    assert "x-ms-durable-outcome" not in response.headers
    if plain_text:
        assert response.get_body().decode() == ERROR_MESSAGE
    else:
        payload = json.loads(response.get_body())
        assert payload["status"] == "error" and payload["response"] is None
        assert payload["error"] == ERROR_MESSAGE and payload["error_code"] == "schema_error"
        assert "outcome" not in payload
        assert payload["agent_response"]["value"] == {"answer": "invalid"}
        assert payload["agent_response"]["additional_properties"]["durable_status"] == "error"
    _assert_one_delivery(client, sleep, raw)


async def test_failed_partial_without_error_content_delivers_record_diagnostic(
    handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    raw = _shared_state(outcome="failed", response=_wire_response("Incomplete answer"))
    client = _client(raw)

    response = await handlers[0](_request(), client)

    assert response.status_code == 500
    payload = json.loads(response.get_body())
    assert payload["error"] == ERROR_MESSAGE and payload["error_code"] == "schema_error"
    assert payload["agent_response"]["messages"][-1]["contents"][0]["error_details"] == {"retryable": False}
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize("hint", ["accepted", "already_completed"])
async def test_success_receipt_ignores_provider_delivery_hints_and_tool_errors(
    hint: str, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    stored = _wire_response()
    stored["extensionData"]["durable_status"] = hint
    stored["messages"].append({"role": "tool", "contents": [{"$type": "error", "errorCode": "response_expired"}]})
    raw = _shared_state(response=stored)
    client = _client(raw)

    response = await handlers[0](_request(), client)

    assert response.status_code == 200
    payload = json.loads(response.get_body())
    assert payload["status"] == "success" and payload["response"] == "Readable answer"
    assert "durable_status" not in payload["agent_response"]["additional_properties"]
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize("conflict", ["status", "content"])
async def test_conflicting_success_evidence_is_an_explicit_read_error(
    conflict: str, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    stored = _wire_response()
    if conflict == "status":
        stored["extensionData"]["durable_status"] = "error"
    else:
        stored["messages"].append({"role": "system", "contents": [{"$type": "error", "errorCode": "response_expired"}]})
    raw = _shared_state(response=stored)
    client = _client(raw)

    response = await handlers[0](_request(), client)

    assert response.status_code == 500
    payload = json.loads(response.get_body())
    assert payload["status"] == "error" and payload["error_code"] == "state_read_error"
    assert payload["error"] == "Failed to read the stored agent response."
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize(
    "raw",
    [
        None,
        [],
        False,
        0,
        "",
        "not JSON",
        {"data": {}},
        {"schemaVersion": "2.0.1", "data": {}},
        {"schemaVersion": "1.1.0", "data": None},
        {"schemaVersion": "2.0.0", "data": {"conversationHistory": []}},
    ],
)
async def test_existing_malformed_state_is_not_normalized_or_polled_until_timeout(
    raw: Any, app: AgentFunctionApp, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    direct_client = _client(raw)
    with pytest.raises(ValueError):
        await app._read_cached_state(direct_client, df.EntityId(f"dafx-{AGENT_NAME}", SESSION_ID))
    client = _client(raw)

    response = await handlers[0](_request(), client)

    assert response.status_code == 500
    payload = json.loads(response.get_body())
    assert payload["status"] == "error" and payload["error_code"] == "state_read_error"
    assert payload["response"] is None
    _assert_one_delivery(client, sleep, raw)


async def test_transport_read_failure_retains_bounded_retries(
    handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    client = _client({})
    client.read_entity_state.side_effect = OSError("Storage read failed")

    response = await handlers[0](_request(), client)

    assert response.status_code == 500
    payload = json.loads(response.get_body())
    assert payload["status"] == "timeout"
    assert client.read_entity_state.await_count == 3
    assert sleep.await_count == 3


@pytest.mark.parametrize("kind", ["missing", "empty-legacy", "shared-transcript-only"])
async def test_only_absent_completion_keeps_polling(
    kind: str, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    raw = _shared_state() if kind == "shared-transcript-only" else {}
    if kind == "shared-transcript-only":
        raw["data"]["completionReceipts"] = {}
        raw["data"]["terminalResults"] = {}
        raw["data"]["conversationHistory"] = [_history_entry()]
    client = _client(raw, exists=kind != "missing")

    response = await handlers[0](_request(), client)

    assert response.status_code == 500 and json.loads(response.get_body())["status"] == "timeout"
    assert client.read_entity_state.await_count == 3 and sleep.await_count == 3
    client.signal_entity.assert_awaited_once()
    assert client.read_entity_state.return_value.entity_state == raw


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize("error", [False, True])
async def test_legacy_delivery_keeps_exact_base_builder_shape(
    version: str, error: bool, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    raw = {"schemaVersion": version, "data": {"conversationHistory": [_history_entry(error=error)]}}
    original = DurableAgentState.from_dict(raw).try_get_agent_response(CORRELATION_ID)
    assert original is not None
    client = _client(raw)

    response = await handlers[0](_request(), client)

    assert response.status_code == 200
    assert json.loads(response.get_body()) == {
        "response": original.text,
        "message": "question",
        "session_id": SESSION_ID,
        "status": "success",
        "correlation_id": CORRELATION_ID,
        "message_count": 1,
    }
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize("encoded", [False, True])
async def test_read_cached_state_uses_shared_view_without_changing_writer_default(
    encoded: bool, app: AgentFunctionApp
) -> None:
    raw = _shared_state()
    client = _client(json.dumps(raw) if encoded else raw)

    state = await app._read_cached_state(client, df.EntityId(f"dafx-{AGENT_NAME}", SESSION_ID))

    assert isinstance(state, SharedAgentStateReader) and not isinstance(state, DurableAgentState)
    assert state.to_dict() == raw
    legacy = DurableAgentState()
    assert legacy.schema_version == "1.1.0"
    assert "terminalResults" not in legacy.to_dict()["data"]
    assert "completionReceipts" not in legacy.to_dict()["data"]


@pytest.mark.parametrize("plain_text", [False, True])
async def test_fire_and_forget_http_remains_202_without_state_read(
    plain_text: bool, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    client = _client(None)

    response = await handlers[0](_request(plain_text=plain_text, wait=False), client)

    assert response.status_code == 202
    if plain_text:
        assert response.get_body() == b"Agent request accepted"
    else:
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


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
async def test_registered_agent_mcp_reports_unavailable_with_retained_outcome(
    outcome: str, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    raw = _shared_state(outcome=outcome, availability="unavailable")
    client = _client(raw)

    with pytest.raises(RuntimeError) as failure:
        await handlers[1](json.dumps({"arguments": {"query": "question", "sessionId": SESSION_ID}}), client)

    assert "unavailable" in str(failure.value) and f"outcome: {outcome}" in str(failure.value)
    assert EXPIRED_MESSAGE in str(failure.value)
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize("failed", [False, True])
async def test_registered_agent_mcp_empty_success_and_failure_are_distinct(
    failed: bool, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    raw = _shared_state(outcome="failed" if failed else "succeeded", response=_wire_response(""))
    client = _client(raw)
    context = json.dumps({"arguments": {"query": "question", "sessionId": SESSION_ID}})

    if failed:
        with pytest.raises(RuntimeError, match=ERROR_MESSAGE):
            await handlers[1](context, client)
    else:
        assert await handlers[1](context, client) == ""
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize("precompleted", [False, True])
@pytest.mark.parametrize("value, response_format", [({"answer": 42}, Answer), (None, NullAnswer)])
async def test_http_snapshot_reaches_public_proxy_typed_task_without_reparsing_text(
    precompleted: bool,
    value: Any,
    response_format: type[BaseModel],
    app: AgentFunctionApp,
    handlers: tuple[HttpHandler, McpHandler],
    sleep: AsyncMock,
) -> None:
    stored = _wire_response("Not JSON, the retained value is authoritative")
    stored["value"] = deepcopy(value)
    raw = _shared_state(response=stored)
    client = _client(raw)
    http_response = await handlers[0](_request(), client)
    assert http_response.status_code == 200
    payload = json.loads(http_response.get_body())["agent_response"]
    before = deepcopy(payload)
    child = AtomicTask(7, NoOpAction())
    if precompleted:
        child.set_value(is_error=False, value=payload)
    context = Mock(spec=df.DurableOrchestrationContext)
    context.instance_id = "shared-orchestration"
    context.new_uuid.side_effect = [SESSION_ID, CORRELATION_ID]
    context.call_entity.return_value = child
    proxy = app.get_agent(context, AGENT_NAME)

    task = proxy.run("question", session=proxy.create_session(), options={"response_format": response_format})

    context.call_entity.assert_called_once()
    entity_id, operation, request = context.call_entity.call_args.args
    assert entity_id.name == f"dafx-{AGENT_NAME}" and entity_id.key == SESSION_ID
    assert operation == "run" and request["correlationId"] == CORRELATION_ID
    assert request["orchestrationId"] == "shared-orchestration"
    context.signal_entity.assert_not_called()
    if not precompleted:
        assert task.state is TaskState.RUNNING
        child.set_value(is_error=False, value=payload)
    assert task.state is TaskState.SUCCEEDED and isinstance(task.result, AgentResponse)
    assert isinstance(task.result.value, response_format)
    assert task.result.value.model_dump(mode="json") == value
    assert child.result is payload and payload == before
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize("precompleted", [False, True])
@pytest.mark.parametrize("shape", ["absent", "coerced", "dropped-field", "valid", "null"])
async def test_http_shared_value_policy_survives_loading_and_typed_task_delivery(
    precompleted: bool,
    shape: str,
    handlers: tuple[HttpHandler, McpHandler],
    sleep: AsyncMock,
) -> None:
    from agent_framework_durabletask import ensure_response_format, serialize_agent_response

    from agent_framework_azurefunctions._orchestration import AgentTask

    stored = _wire_response('{"answer":42}')
    values = {
        "coerced": {"answer": "42"},
        "dropped-field": {"answer": 42, "future": False},
        "valid": {"answer": 42},
        "null": None,
    }
    if shape != "absent":
        stored["value"] = deepcopy(values[shape])
    raw = _shared_state(response=stored)
    client = _client(raw)
    http = await handlers[0](_request(), client)
    assert http.status_code == 200
    snapshot = json.loads(http.get_body())["agent_response"]
    original_snapshot = deepcopy(snapshot)
    response_format = NullAnswer if shape == "null" else Answer
    valid = shape in ("valid", "null")
    for _ in range(2):
        direct = load_agent_response(deepcopy(snapshot))
        if valid:
            ensure_response_format(response_format, CORRELATION_ID, direct)
            assert isinstance(direct.value, response_format)
            assert direct.value.model_dump(mode="json") == values[shape]
        else:
            with pytest.raises(ValueError, match="no structured value|cannot preserve"):
                ensure_response_format(response_format, CORRELATION_ID, direct)
        child = AtomicTask(7, NoOpAction())
        if precompleted:
            child.set_value(is_error=False, value=deepcopy(snapshot))
        task = AgentTask(child, response_format, CORRELATION_ID)
        if not precompleted:
            child.set_value(is_error=False, value=deepcopy(snapshot))
        assert task.state is (TaskState.SUCCEEDED if valid else TaskState.FAILED)
        # A second consumer serialization must not remove the policy either.
        snapshot = json.loads(json.dumps(serialize_agent_response(load_agent_response(snapshot))))
        assert ("value" in snapshot) is (shape != "absent")
        if shape != "absent":
            assert snapshot["value"] == values[shape]
    assert client.read_entity_state.return_value.entity_state == raw
    assert json.loads(http.get_body())["agent_response"] == original_snapshot
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize("status", ["error", "timeout", "accepted"])
def test_empty_non_success_text_still_uses_diagnostic(status: str, app: AgentFunctionApp) -> None:
    assert app._convert_payload_to_text({"status": status, "response": "", "error": "Diagnostic"}) == "Diagnostic"
