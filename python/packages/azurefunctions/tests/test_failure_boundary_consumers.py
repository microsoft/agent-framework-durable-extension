# Copyright (c) Microsoft. All rights reserved.

"""AF polling at deterministic execution boundaries, without a live Functions host."""

import asyncio
import importlib
import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import azure.durable_functions as df
import pytest
from agent_framework_durabletask import AgentEntity, DurableAgentState

from agent_framework_azurefunctions import AgentFunctionApp
from agent_framework_azurefunctions import _app as app_module


@pytest.fixture
def boundaries(monkeypatch: pytest.MonkeyPatch) -> Any:
    # Allow this file to run alone, without requiring DT test collection first.
    # The directory is derived from this worktree, never from another checkout.
    tests = Path(__file__).resolve().parents[2] / "durabletask" / "tests"
    monkeypatch.syspath_prepend(str(tests))
    module = importlib.import_module("test_cancellation_boundaries")
    assert module.__file__ is not None
    assert Path(module.__file__).resolve().parent == tests
    return module


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch) -> AgentFunctionApp:
    async def immediate_poll_interval(interval: float) -> None:
        assert interval == 0.01

    # Replace only this module's scheduling seam, not asyncio.sleep process-wide.
    monkeypatch.setattr(app_module, "asyncio", SimpleNamespace(sleep=immediate_poll_interval))
    return AgentFunctionApp(
        enable_health_check=False,
        enable_http_endpoints=False,
        max_poll_retries=3,
        poll_interval_seconds=0.01,
    )


class _JsonAFBackend:
    """Each backend read returns a fresh real JSON snapshot, never a synthetic response."""

    def __init__(self, provider: Any) -> None:
        self.provider = provider
        self.reads = 0
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.pause = False
        self.observed: list[dict[str, Any]] = []

    async def read_entity_state(self, entity_id: df.EntityId) -> Any:
        self.reads += 1
        raw = json.loads(json.dumps(self.provider.raw))
        self.observed.append(raw)
        if self.pause:
            self.entered.set()
            await self.release.wait()
        return SimpleNamespace(entity_exists=True, entity_state=raw)


async def _poll(app: AgentFunctionApp, backend: Any, correlation: str) -> dict[str, Any]:
    return await app._get_response_from_entity(
        client=backend,
        entity_instance_id=df.EntityId("dafx-boundary", "revision-session"),
        correlation_id=correlation,
        message="boundary request",
        session_id="revision-session",
    )


@pytest.mark.parametrize("phase", ["load", "store"])
async def test_external_failure_and_rejected_error_write_timeout_until_a_real_commit(
    phase: str, boundaries: Any, app: AgentFunctionApp
) -> None:
    entity, provider, external, client = boundaries.failure_boundary(phase)
    before = deepcopy(provider.raw)
    request = boundaries.projected_request("provider-failed")
    with pytest.raises(OSError, match="entity storage write rejected"):
        await entity.run(request)
    boundaries.assert_staged_not_committed(provider, before, phase)
    assert entity.state.to_dict() == before
    assert external.loads == 1 and len(external.saved) == int(phase == "store")
    backend = _JsonAFBackend(provider)

    result = await _poll(app, backend, "provider-failed")

    assert result["status"] == "timeout"
    assert result["correlation_id"] == "provider-failed"
    assert "agent_response" not in result
    assert backend.reads == 3 and backend.observed == [before] * 3
    assert provider.writes == 0 and len(provider.attempts) == 1
    assert external.loads == 1 and len(client.effects) == int(phase == "store")

    provider.reject = False
    failed = await entity.run(request)
    assert provider.writes == 1
    assert failed.additional_properties["durable_status"] == "error"
    calls = (external.loads, len(external.saved), len(client.effects))
    delivered = await _poll(app, backend, "provider-failed")
    assert delivered["status"] == "error" and delivered["error_code"] == "OSError"
    assert delivered["agent_response"] == json.loads(json.dumps(failed.to_dict()))
    assert backend.reads == 4
    external.phase = None
    cold_provider = boundaries.JsonStateProvider(provider.raw)
    cold = AgentEntity(entity.agent, state_provider=cold_provider)
    assert (await cold.run(request)).to_dict() == failed.to_dict()
    assert (external.loads, len(external.saved), len(client.effects)) == calls
    assert cold_provider.writes == 0


async def test_cancelling_caller_poll_does_not_cancel_concurrent_entity_or_allow_duplicate_run(
    boundaries: Any, app: AgentFunctionApp
) -> None:
    barrier = boundaries.PhaseBarrier()
    barrier.phase = "model"
    client = boundaries.BarrierClient(barrier)
    agent = boundaries.NonStreamingAgent(client=client, name="boundary")
    provider = boundaries.JsonStateProvider()
    entity = AgentEntity(agent, state_provider=provider)
    request = boundaries.projected_request("caller-cancelled")
    backend = _JsonAFBackend(provider)
    backend.pause = True
    execution = asyncio.create_task(entity.run(request))
    polling: asyncio.Task[dict[str, Any]] | None = None
    try:
        await boundaries.await_boundary(execution, barrier.entered)
        polling = asyncio.create_task(_poll(app, backend, "caller-cancelled"))
        await boundaries.await_boundary(polling, backend.entered)
        assert not execution.done() and not polling.done()
        assert backend.observed == [{}] and provider.writes == 0
        assert len(client.effects) == 1

        polling.cancel()
        with pytest.raises(asyncio.CancelledError):
            await polling
        assert polling.cancelled()
        assert not execution.done() and not execution.cancelled()
        assert provider.writes == 0 and provider.raw == {}

        barrier.release.set()
        response = await execution
        assert response.text == "boundary answer" and provider.writes == 1
        assert len(client.effects) == 1
        raw = json.loads(json.dumps(provider.raw))
        assert DurableAgentState.from_json(json.dumps(raw)).try_get_agent_response("caller-cancelled") is not None
        backend.pause = False
        delivered = await _poll(app, backend, "caller-cancelled")
        assert delivered["status"] == "success"
        assert delivered["agent_response"] == response.to_dict()
        assert backend.reads == 2

        cold_provider = boundaries.JsonStateProvider(raw)
        cold_agent = boundaries.NonStreamingAgent(client=client, name="boundary")
        cold = AgentEntity(cold_agent, state_provider=cold_provider)
        assert (await cold.run(request)).to_dict() == response.to_dict()
        assert len(client.effects) == 1 and cold_provider.writes == 0
    finally:
        tasks = [execution, *([polling] if polling is not None else [])]
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
