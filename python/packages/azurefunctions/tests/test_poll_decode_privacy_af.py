# Copyright (c) Microsoft. All rights reserved.

"""Stored-state decode diagnostics through actual indexed HTTP and MCP handlers."""

import json
import logging
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock, call

import azure.durable_functions as df
import azure.functions as func
import pytest
from agent_framework_durabletask import read_agent_state

from agent_framework_azurefunctions import AgentFunctionApp
from agent_framework_azurefunctions import _app as af_app

AGENT = "poll_reader"
CORRELATION = "poll-correlation"
SESSION = "poll-session"
SENTINEL = "PRIVATE-STORED-PAYLOAD"
READ_ERROR = "Failed to read the stored agent response."
PROVIDER_ERROR = "Approved provider diagnostic."


def _state(*, failed: bool = False) -> dict[str, Any]:
    common = {
        "correlationId": CORRELATION,
        "outcome": "failed" if failed else "succeeded",
        "completedAt": "2026-09-23T11:00:00Z",
    }
    result: dict[str, Any] = {
        **common,
        "response": {"messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "Recovered"}]}]},
    }
    if failed:
        result["error"] = {"code": "provider_failure", "message": PROVIDER_ERROR}
    return {
        "schemaVersion": "2.0.0",
        "data": {
            "conversationHistory": [],
            "terminalResults": {CORRELATION: result},
            "completionReceipts": {CORRELATION: {**common, "resultState": "available"}},
        },
    }


@pytest.fixture
def registered(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    app = AgentFunctionApp(
        enable_health_check=False, enable_http_endpoints=False, max_poll_retries=3, poll_interval_seconds=0.01
    )
    agent = Mock(spec=["name", "description", "run"])
    agent.name = AGENT
    agent.description = "Stored-response reader"
    agent.run = AsyncMock(side_effect=AssertionError("Polling must not invoke the provider"))
    monkeypatch.setattr(app, "_generate_unique_id", lambda: CORRELATION)
    sleeper = AsyncMock()
    monkeypatch.setattr(af_app.asyncio, "sleep", sleeper)
    app.add_agent(agent, enable_http_endpoint=True, enable_mcp_tool_trigger=True)
    functions = {item.get_function_name(): item for item in cast(Any, app).get_functions()}
    assert set(functions) == {f"dafx-{AGENT}", f"http-{AGENT}", f"mcptool-{AGENT}"}
    assert functions[f"http-{AGENT}"].get_trigger().get_dict_repr()["route"] == f"agents/{AGENT}/run"
    assert functions[f"mcptool-{AGENT}"].get_trigger().get_binding_name() == "mcpToolTrigger"
    # Only bypass SDK rich-client construction to inject storage, not registration.
    http = getattr(functions[f"http-{AGENT}"].get_user_function(), "client_function", None)
    mcp = getattr(functions[f"mcptool-{AGENT}"].get_user_function(), "client_function", None)
    assert callable(http) and callable(mcp)
    return SimpleNamespace(http=http, mcp=mcp, sleep=sleeper, agent=agent)


def _storage(*states: Any) -> Mock:
    client = Mock(spec=df.DurableOrchestrationClient)
    client.signal_entity = AsyncMock()
    client.read_entity_state = AsyncMock(
        side_effect=[
            state if isinstance(state, Exception) else SimpleNamespace(entity_exists=True, entity_state=state)
            for state in states
        ]
    )
    return client


async def _invoke(registered: SimpleNamespace, client: Mock, surface: str) -> tuple[int, str, dict[str, Any]]:
    if surface == "mcp":
        text = await registered.mcp(
            context=json.dumps({"arguments": {"query": "question", "sessionId": SESSION}}), client=client
        )
        return 200, text, {}
    content_type = "text/plain" if surface == "text" else "application/json"
    request = func.HttpRequest(
        method="POST",
        url=f"https://example.test/api/agents/{AGENT}/run",
        headers={"Content-Type": content_type, "Accept": content_type},
        params={"session_id": SESSION},
        body=b"question" if surface == "text" else json.dumps({"message": "question", "session_id": SESSION}).encode(),
    )
    response = await registered.http(req=request, client=client)
    body = response.get_body().decode()
    return response.status_code, body, json.loads(body) if surface == "json" else {}


def _assert_polls(registered: SimpleNamespace, client: Mock, count: int) -> None:
    client.signal_entity.assert_awaited_once()
    entity, operation, request = client.signal_entity.call_args.args
    assert entity.name == f"dafx-{AGENT}" and entity.key == SESSION
    assert operation == "run" and request["correlationId"] == CORRELATION and request["message"] == "question"
    assert client.read_entity_state.await_args_list == [call(entity)] * count
    assert registered.sleep.await_args_list == [call(0.01)] * count
    registered.agent.run.assert_not_called()


@pytest.mark.parametrize("surface", ["json", "text", "mcp"])
@pytest.mark.parametrize("encoded", [False, True], ids=["object-state", "json-state"])
async def test_decode_failure_is_constant_private_and_terminal(
    surface: str, encoded: bool, registered: SimpleNamespace, caplog: pytest.LogCaptureFixture
) -> None:
    bad = {
        "schemaVersion": "1.1.0",
        "data": {"conversationHistory": [{"$type": SENTINEL, "createdAt": "2026-09-23T11:00:00Z", "messages": []}]},
    }
    stored: Any = json.dumps(bad) if encoded else bad
    before = json.dumps(stored, sort_keys=True)
    with pytest.raises(ValueError, match=SENTINEL):
        read_agent_state(stored)
    good = _state()
    good_before = json.dumps(good, sort_keys=True)
    recovered = read_agent_state(good).try_get_agent_response(CORRELATION)
    assert recovered is not None and recovered.text == "Recovered"
    client = _storage(stored, good)
    with caplog.at_level(logging.DEBUG, logger="agent_framework.azurefunctions"):
        if surface == "mcp":
            with pytest.raises(RuntimeError) as error:
                await _invoke(registered, client, surface)
            public = str(error.value)
            assert public == f"Agent execution failed: {READ_ERROR}"
        else:
            status, public, payload = await _invoke(registered, client, surface)
            assert status == 500
            if surface == "json":
                assert payload["status"] == "error" and payload["error_code"] == "state_read_error"
                assert payload["response"] is None and payload["correlation_id"] == CORRELATION
                assert payload["error"] == READ_ERROR
            else:
                assert public == READ_ERROR
    _assert_polls(registered, client, 1)
    assert json.dumps(stored, sort_keys=True) == before and json.dumps(good, sort_keys=True) == good_before
    assert SENTINEL not in public + caplog.text
    warnings = [
        record
        for record in caplog.records
        if record.name == "agent_framework.azurefunctions" and record.levelno == logging.WARNING
    ]
    assert len(warnings) == 1
    assert warnings[0].getMessage() == f"[HTTP Trigger] {READ_ERROR}"
    assert warnings[0].exc_info is None and warnings[0].stack_info is None


@pytest.mark.parametrize("surface", ["json", "text", "mcp"])
@pytest.mark.parametrize("failure_type", [OSError, ValueError])
async def test_transport_read_failure_still_retries(
    surface: str, failure_type: type[Exception], registered: SimpleNamespace
) -> None:
    client = _storage(failure_type("Transient storage failure"), _state())
    status, body, payload = await _invoke(registered, client, surface)
    assert status == 200
    if surface == "json":
        assert payload["status"] == "success" and payload["response"] == "Recovered"
    else:
        assert body == "Recovered"
    _assert_polls(registered, client, 2)


@pytest.mark.parametrize("surface", ["json", "text", "mcp"])
async def test_provider_failure_diagnostic_is_not_replaced(surface: str, registered: SimpleNamespace) -> None:
    client = _storage(_state(failed=True))
    if surface == "mcp":
        with pytest.raises(RuntimeError) as error:
            await _invoke(registered, client, surface)
        assert str(error.value) == f"Agent execution failed: {PROVIDER_ERROR}"
    else:
        status, body, payload = await _invoke(registered, client, surface)
        assert status == 500
        if surface == "json":
            assert payload["error_code"] == "provider_failure" and payload["error"] == PROVIDER_ERROR
        else:
            assert body == PROVIDER_ERROR
    _assert_polls(registered, client, 1)
