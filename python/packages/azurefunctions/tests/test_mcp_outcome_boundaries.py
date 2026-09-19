# Copyright (c) Microsoft. All rights reserved.

"""MCP outcomes through real polling, JSON state, and Core execution fixtures."""

import asyncio
import json
import logging
from copy import deepcopy
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import azure.durable_functions as df
import pytest
from agent_framework_durabletask import AgentEntity, DurableAgentState
from test_delivery_consumers_af import (
    AGENT_NAME,
    CORRELATION_ID,
    EXPIRED_MESSAGE,
    SESSION_ID,
    _client,
    _mailbox_state,
    _response,
    _runtime_error,
)
from test_delivery_consumers_af import response_expectations as response_expectations
from test_failure_boundary_consumers import _JsonAFBackend
from test_failure_boundary_consumers import boundaries as boundaries

from agent_framework_azurefunctions import AgentFunctionApp
from agent_framework_azurefunctions import _app as app_module

CANONICAL_SESSION_ID = f"@{AGENT_NAME}@{SESSION_ID}"
TIMEOUT_MESSAGE = (
    "Agent response timed out. Invocation outcome unresolved. "
    f"Correlation ID: {CORRELATION_ID}. Session ID: {CANONICAL_SESSION_ID}. "
    "Execution may still be in progress."
)


@pytest.fixture
def sleep(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    mocked = AsyncMock()
    # Keep real asyncio scheduling available for the concurrent Core execution.
    monkeypatch.setattr(app_module, "asyncio", SimpleNamespace(sleep=mocked))
    return mocked


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch) -> AgentFunctionApp:
    result = AgentFunctionApp(
        enable_health_check=False,
        enable_http_endpoints=False,
        max_poll_retries=1,
        poll_interval_seconds=0.01,
    )
    monkeypatch.setattr(result, "_generate_unique_id", Mock(return_value=CORRELATION_ID))
    return result


def _context() -> str:
    return json.dumps({"arguments": {"query": "question", "sessionId": SESSION_ID}})


def _assert_run_and_reads_only(client: Mock, reads: int) -> df.EntityId:
    client.signal_entity.assert_awaited_once()
    entity_id, operation, request = client.signal_entity.call_args.args
    assert entity_id.name == f"dafx-{AGENT_NAME}"
    assert entity_id.key == SESSION_ID
    assert operation == "run"
    assert request["correlationId"] == CORRELATION_ID
    assert request["message"] == "question"
    assert client.read_entity_state.await_count == reads
    assert all(call.args == (entity_id,) for call in client.read_entity_state.await_args_list)
    # No cancellation, retry signal, or other backend operation is permitted.
    assert [call[0] for call in client.mock_calls] == ["signal_entity", *["read_entity_state"] * reads]
    return entity_id


def _assert_unresolved_timeout(caplog: pytest.LogCaptureFixture) -> None:
    records = [record for record in caplog.records if record.name == app_module.logger.name]
    assert any(
        record.levelno == logging.WARNING and record.getMessage() == f"[MCP Tool] {TIMEOUT_MESSAGE}"
        for record in records
    )
    assert all(record.levelno < logging.ERROR for record in records)
    assert "execution failed" not in caplog.text
    assert "responded successfully" not in caplog.text
    assert "Unknown error" not in caplog.text


@pytest.mark.parametrize("max_poll_retries", [1, 3])
@pytest.mark.parametrize("state_exists", [False, True])
async def test_mcp_poll_exhaustion_is_identifiable_and_unresolved(
    max_poll_retries: int,
    state_exists: bool,
    app: AgentFunctionApp,
    sleep: AsyncMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger=app_module.logger.name)
    app.max_poll_retries = max_poll_retries
    payload = DurableAgentState().to_dict() if state_exists else None
    client = _client(payload)
    before = deepcopy(client.read_entity_state.return_value.entity_state)

    with pytest.raises(RuntimeError) as failure:
        await app._handle_mcp_tool_invocation(AGENT_NAME, _context(), client)

    assert type(failure.value) is RuntimeError
    assert str(failure.value) == TIMEOUT_MESSAGE
    _assert_unresolved_timeout(caplog)
    _assert_run_and_reads_only(client, max_poll_retries)
    assert sleep.await_count == max_poll_retries
    assert all(call.args == (0.01,) for call in sleep.await_args_list)
    assert client.read_entity_state.return_value.entity_state == before
    if before is not None:
        assert before["data"]["completionReceipts"] == {}
        assert before["data"]["terminalResults"] == {}


@pytest.mark.parametrize("expired", [False, True])
@pytest.mark.parametrize("failed", [False, True])
async def test_mcp_next_poll_delivers_committed_outcome(
    expired: bool,
    failed: bool,
    app: AgentFunctionApp,
    sleep: AsyncMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger=app_module.logger.name)
    app.max_poll_retries = 2
    original = _runtime_error() if failed else _response()
    payload = _mailbox_state(original, expired=expired, cleanup=expired)
    client = _client(None)
    client.read_entity_state.side_effect = [
        client.read_entity_state.return_value,
        _client(payload).read_entity_state.return_value,
    ]
    before = deepcopy(payload)

    if expired or failed:
        expected = EXPIRED_MESSAGE if expired else "Model endpoint unavailable"
        if expired:
            expected += f" Invocation outcome: {'failed' if failed else 'succeeded'}."
        with pytest.raises(RuntimeError) as failure:
            await app._handle_mcp_tool_invocation(AGENT_NAME, _context(), client)
        assert type(failure.value) is RuntimeError
        assert str(failure.value) == f"Agent execution failed: {expected}"
        assert "responded successfully" not in caplog.text
    else:
        assert await app._handle_mcp_tool_invocation(AGENT_NAME, _context(), client) == original.text
        assert "responded successfully" in caplog.text
        assert "execution failed" not in caplog.text

    assert "timed out" not in caplog.text
    assert "outcome unresolved" not in caplog.text
    _assert_run_and_reads_only(client, 2)
    assert sleep.await_count == 2
    assert payload == before


@pytest.mark.parametrize("interruption", ["timeout", "cancel"])
async def test_mcp_wait_ending_does_not_cancel_core_execution_and_later_poll_completes(
    interruption: str,
    boundaries: Any,
    app: AgentFunctionApp,
    sleep: AsyncMock,
    caplog: pytest.LogCaptureFixture,
    response_expectations: Any,
) -> None:
    caplog.set_level(logging.DEBUG, logger=app_module.logger.name)
    barrier = boundaries.PhaseBarrier()
    barrier.phase = "model"
    model = boundaries.BarrierClient(barrier)
    agent = boundaries.NonStreamingAgent(client=model, name=AGENT_NAME)
    provider = boundaries.JsonStateProvider()
    entity = AgentEntity(agent, state_provider=provider)
    backend = _JsonAFBackend(provider)
    backend.pause = interruption == "cancel"
    client = _client(None)
    client.read_entity_state.side_effect = backend.read_entity_state
    execution: asyncio.Task[Any] | None = None

    async def signal(entity_id: df.EntityId, operation: str, request: dict[str, Any]) -> None:
        nonlocal execution
        assert operation == "run"
        execution = asyncio.create_task(entity.run(request))
        await boundaries.await_boundary(execution, barrier.entered)

    client.signal_entity.side_effect = signal
    invocation = asyncio.create_task(app._handle_mcp_tool_invocation(AGENT_NAME, _context(), client))
    try:
        if interruption == "cancel":
            await boundaries.await_boundary(invocation, backend.entered)
            invocation.cancel()
            with pytest.raises(asyncio.CancelledError):
                await invocation
            assert invocation.cancelled()
            assert "timed out" not in caplog.text
            assert "Error invoking agent" not in caplog.text
        else:
            with pytest.raises(RuntimeError) as failure:
                await invocation
            assert str(failure.value) == TIMEOUT_MESSAGE
            _assert_unresolved_timeout(caplog)

        assert "execution failed" not in caplog.text
        assert "responded successfully" not in caplog.text
        assert execution is not None and not execution.done() and not execution.cancelled()
        assert provider.raw == {} and provider.writes == 0
        assert entity.state.try_get_agent_response(CORRELATION_ID) is None
        assert backend.observed == [{}]
        assert model.effects == ["model-side-effect"]
        entity_id = _assert_run_and_reads_only(client, 1)
        sleep.assert_awaited_once_with(0.01)

        barrier.release.set()
        response = await execution
        assert response.text == "boundary answer"
        assert provider.writes == 1
        committed = deepcopy(provider.raw)
        receipt = committed["data"]["completionReceipts"][CORRELATION_ID]
        assert receipt["outcome"] == "succeeded"
        assert receipt["correlationId"] == CORRELATION_ID
        assert receipt["resultState"] == "available"
        backend.pause = False

        # Resume observation of the same correlation, not a new MCP invocation.
        delivered = await app._get_response_from_entity(
            client=client,
            entity_instance_id=entity_id,
            correlation_id=CORRELATION_ID,
            message="question",
            session_id=CANONICAL_SESSION_ID,
        )

        assert delivered["status"] == "success"
        assert delivered["response"] == response.text
        assert delivered["agent_response"] == response_expectations.expected_shared_transport(response)
        assert delivered["correlation_id"] == CORRELATION_ID
        assert delivered["session_id"] == CANONICAL_SESSION_ID
        assert "error" not in delivered
        assert backend.observed == [{}, committed]
        assert provider.raw == committed and provider.writes == 1
        assert model.effects == ["model-side-effect"]
        _assert_run_and_reads_only(client, 2)
        assert sleep.await_count == 2
    finally:
        tasks = [invocation, *([execution] if execution is not None else [])]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
