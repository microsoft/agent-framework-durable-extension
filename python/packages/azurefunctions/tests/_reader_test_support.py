# Copyright (c) Microsoft. All rights reserved.

"""Shared snapshots and registered Functions handlers for reader delivery tests."""

import json
from collections.abc import Awaitable, Callable
from copy import deepcopy
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import azure.durable_functions as df
import azure.functions as func
import pytest
from agent_framework_durabletask import (
    MIMETYPE_APPLICATION_JSON,
    MIMETYPE_TEXT_PLAIN,
    WAIT_FOR_RESPONSE_HEADER,
)
from pydantic import BaseModel

from agent_framework_azurefunctions import AgentFunctionApp

AGENT_NAME = "shared-consumer"
CORRELATION_ID = "shared-correlation"
SESSION_ID = "shared-session"
COMPLETED_AT = "2024-01-01T00:00:00Z"
EXPIRED_AT = "2024-01-01T00:01:00Z"
ERROR_MESSAGE = "Response failed schema validation."
HttpHandler = Callable[[func.HttpRequest, Any], Awaitable[func.HttpResponse]]
McpHandler = Callable[[str, Any], Awaitable[str]]


class Answer(BaseModel):
    answer: int


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
