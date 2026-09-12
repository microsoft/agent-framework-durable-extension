# Copyright (c) Microsoft. All rights reserved.

"""Deterministic interruption and JSON commit boundaries, not live worker shutdown tests."""

import asyncio
import json
from collections.abc import Awaitable, Sequence
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import AgentResponse, AgentSession, ChatResponse, HistoryProvider, Message
from test_durable_history_provider import RecordingChatClient
from test_history_pipeline_revision import NonStreamingAgent
from test_revision_contract import JsonStateProvider

from agent_framework_durabletask import AgentEntity, DurableAgentState, DurableHistoryProvider, RunRequest
from agent_framework_durabletask import _entities as entities
from agent_framework_durabletask._executors import ClientAgentExecutor
from agent_framework_durabletask._history_provider import current_durable_history_binding


class SimulatedWorkerStop(BaseException):
    """An explicit process-boundary sentinel, not a claim about SDK shutdown plumbing."""


class PhaseBarrier:
    def __init__(self) -> None:
        self.phase: str | None = None
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.stop: SimulatedWorkerStop | None = None
        self.observed_binding: Any = None

    async def wait(self, phase: str) -> None:
        if phase != self.phase:
            return
        self.observed_binding = current_durable_history_binding()
        self.entered.set()
        await self.release.wait()
        if self.stop is not None:
            raise self.stop


async def await_boundary(task: asyncio.Task[Any], entered: asyncio.Event) -> None:
    """Fail promptly if execution ends before its barrier, without clock-based waits."""
    waiter = asyncio.create_task(entered.wait())
    try:
        await asyncio.wait({task, waiter}, return_when=asyncio.FIRST_COMPLETED)
        if not entered.is_set():
            await task
            pytest.fail("execution finished without reaching the required boundary")
    finally:
        if not waiter.done():
            waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


class BarrierClient(RecordingChatClient):
    def __init__(self, barrier: PhaseBarrier) -> None:
        super().__init__()
        self.barrier = barrier
        self.effects: list[str] = []

    def get_response(self, messages: Any, *, stream: bool = False, **kwargs: Any) -> Awaitable[ChatResponse]:
        if stream:
            raise TypeError("stream is not supported")
        self.received_messages.append(deepcopy(list(messages)))

        async def get() -> ChatResponse:
            # The externally visible attempt precedes the cancellable model await.
            self.effects.append("model-side-effect")
            await self.barrier.wait("model")
            return ChatResponse(messages=[Message("assistant", ["boundary answer"], message_id="boundary-answer")])

        return get()


def _agent(client: BarrierClient, **kwargs: Any) -> NonStreamingAgent:
    chat_client: Any = client
    return NonStreamingAgent(client=chat_client, **kwargs)


class _BoundaryHistory(DurableHistoryProvider):
    def __init__(self, barrier: PhaseBarrier) -> None:
        super().__init__(prune_excluded=False)
        self.barrier = barrier
        self.effects: list[str] = []
        self.sessions: list[AgentSession] = []
        self.agents: list[Any] = []

    async def before_run(self, *, agent: Any, session: AgentSession, state: dict[str, Any], **kwargs: Any) -> None:
        self.agents.append(agent)
        self.sessions.append(session)
        await super().before_run(agent=agent, session=session, state=state, **kwargs)
        state["provider_control"] = {"visits": state.get("provider_control", {}).get("visits", 0) + 1}
        self.effects.append("external-before-effect")
        await self.barrier.wait("before_run")

    async def after_run(self, **kwargs: Any) -> None:
        await super().after_run(**kwargs)
        self.effects.append("external-after-effect")
        await self.barrier.wait("after_run")


def projected_request(correlation: str = "interrupted") -> dict[str, Any]:
    message = Message(
        "user",
        ["projected boundary input"],
        message_id=f"{correlation}-input",
        additional_properties={"json": [1, False]},
    )
    return {"message": message.text, "correlationId": correlation, "contextMessages": [message.to_dict()]}


def _initial_state() -> dict[str, Any]:
    state = DurableAgentState()
    session = AgentSession(session_id="revision-session", service_session_id="saved-service-id")
    session.state = {"foreign": {"pending_approval": {"id": "keep", "approved": False}}}
    state.data.session = session.to_dict()
    state.data.ingested_messages = {"previous-input": ["previous-fingerprint"]}
    state.data.ingested_positions = {"source": 4}
    state.data.extension_data = {"control": {"keep": [1]}}
    state.record_response(
        "expired",
        AgentResponse(messages=[Message("assistant", ["old delivery payload"])]),
        now=datetime(2020, 1, 1, tzinfo=timezone.utc),
        delivery_window_seconds=60,
    )
    state.record_response(
        "previous",
        AgentResponse(messages=[Message("assistant", ["retained delivery payload"])]),
        delivery_window_seconds=3600,
    )
    return json.loads(state.to_json())


@pytest.mark.parametrize("phase", ["before_run", "model", "after_run", "budget"])
@pytest.mark.parametrize("interruption", ["task-cancel", "base-exception-stop"])
async def test_interruption_rolls_back_every_local_slice_and_retry_can_repeat_external_effects(
    phase: str, interruption: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    barrier = PhaseBarrier()
    barrier.phase = phase
    history = _BoundaryHistory(barrier)
    client = BarrierClient(barrier)
    agent: Any = _agent(
        client=client,
        name="boundary",
        context_providers=[history],
        default_options={"store": False, "conversation_id": "inactive-default-id"},
    )
    provider = JsonStateProvider(_initial_state())
    entity = AgentEntity(agent, state_provider=provider, max_state_bytes=1_000_000)
    original_state = entity.state
    original_agent = entity.agent
    original_defaults = deepcopy(agent.default_options)
    before = json.loads(json.dumps(provider.raw))
    request = projected_request()
    real_budget = entities.enforce_budget

    async def budget(state: DurableAgentState, **kwargs: Any) -> int:
        removed = await real_budget(state, **kwargs)
        await barrier.wait("budget")
        return removed

    monkeypatch.setattr(entities, "enforce_budget", budget)
    task_final_bindings: list[Any] = []

    async def execute() -> AgentResponse:
        try:
            return await entity.run(request)
        finally:
            # Inspect the cancelled task's own context, not merely its parent's ContextVar.
            task_final_bindings.append(current_durable_history_binding())

    task = asyncio.create_task(execute())
    try:
        await await_boundary(task, barrier.entered)
        assert not task.done()
        assert provider.raw == before and provider.writes == 0
        staged = entity.state.to_dict()["data"]
        assert "expired" not in staged["responseMailbox"], "TTL cleanup must actually have been staged"
        assert "interrupted-input" in staged["ingestedMessages"]
        if phase == "budget":
            assert "interrupted" in staged["completedCorrelations"]
            assert "interrupted" in staged["responseMailbox"]
            assert staged["session"] != before["data"]["session"]
            assert barrier.observed_binding is None
        else:
            assert barrier.observed_binding is not None
        if phase == "after_run":
            assert any(entry.get("correlationId") == "interrupted" for entry in staged["conversationHistory"])
        if interruption == "task-cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert task.cancelled()
        else:
            barrier.stop = SimulatedWorkerStop("explicit worker-stop boundary")
            barrier.release.set()
            with pytest.raises(SimulatedWorkerStop) as error:
                await task
            assert error.value is barrier.stop
            assert not task.cancelled()
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert task_final_bindings == [None]
    assert current_durable_history_binding() is None
    assert history.agents[-1] is not original_agent, "exercise restoration of a real operation-local Agent clone"
    assert history.sessions[-1].service_session_id == "saved-service-id"
    assert entity.agent is original_agent and agent.default_options == original_defaults
    assert entity.state is original_state and entity.state.to_dict() == before
    assert provider.raw == before and provider.writes == 0
    assert entity.state.try_get_agent_response("interrupted") is None
    assert "interrupted" not in provider.raw["data"]["completedCorrelations"]
    assert "expired" in provider.raw["data"]["responseMailbox"]
    assert history.effects.count("external-before-effect") == 1
    assert len(client.effects) == int(phase != "before_run")

    barrier.phase = None
    barrier.stop = None
    response = await entity.run(request)
    assert response.text == "boundary answer"
    assert history.effects.count("external-before-effect") == 2
    assert len(client.effects) == 1 + int(phase != "before_run")
    assert provider.writes == 1
    assert "expired" not in provider.raw["data"]["responseMailbox"]
    assert (
        provider.raw["data"]["completedCorrelations"]["expired"] == before["data"]["completedCorrelations"]["expired"]
    )
    assert provider.raw["data"]["responseMailbox"]["previous"] == before["data"]["responseMailbox"]["previous"]
    assert provider.raw["data"]["session"]["state"]["foreign"] == before["data"]["session"]["state"]["foreign"]
    attempts = (len(client.effects), len(history.effects))
    cold_provider = JsonStateProvider(provider.raw)
    cold = AgentEntity(agent, state_provider=cold_provider)
    assert (await cold.run(request)).to_dict() == response.to_dict()
    assert (len(client.effects), len(history.effects)) == attempts and cold_provider.writes == 0


class _LostAcknowledgementStorage(JsonStateProvider):
    def _set_state_dict(self, state: dict[str, Any]) -> None:
        super()._set_state_dict(state)
        # The JSON write has definitely happened, but the operation cannot know that.
        raise OSError("storage acknowledgement lost after write")


async def test_unknown_commit_requires_fresh_json_read_and_suppresses_duplicate_execution() -> None:
    barrier = PhaseBarrier()
    client = BarrierClient(barrier)
    agent: Any = _agent(client=client, name="unknown-commit")
    provider = _LostAcknowledgementStorage(_initial_state())
    entity = AgentEntity(agent, state_provider=provider)
    original = entity.state
    before = deepcopy(provider.raw)
    request = projected_request("unknown-commit")

    with pytest.raises(OSError, match="acknowledgement lost"):
        await entity.run(request)

    assert provider.writes == 1 and len(client.effects) == 1
    assert entity.state is original and entity.state.to_dict() == before
    assert entity.state.try_get_agent_response("unknown-commit") is None
    assert current_durable_history_binding() is None
    raw = json.loads(json.dumps(provider.raw))
    assert raw != before
    committed = DurableAgentState.from_json(json.dumps(raw)).try_get_agent_response("unknown-commit")
    assert committed is not None and committed.text == "boundary answer"
    assert "unknown-commit" in raw["data"]["completedCorrelations"]
    assert raw["data"]["responseMailbox"]["unknown-commit"]["response"] == committed.to_dict()
    cold_provider = JsonStateProvider(raw)
    cold_agent: Any = _agent(client=client, name="unknown-commit")
    cold = AgentEntity(cold_agent, state_provider=cold_provider)
    duplicate = await cold.run(request)
    assert duplicate.to_dict() == committed.to_dict()
    assert len(client.effects) == 1 and cold_provider.writes == 0
    assert cold_provider.raw == raw


class FailingExternalHistory(HistoryProvider):
    def __init__(self, phase: str) -> None:
        super().__init__("external-boundary")
        self.phase: str | None = phase
        self.loads = 0
        self.saved: list[list[Message]] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        self.loads += 1
        if self.phase == "load":
            raise OSError("external load boundary failed")
        return deepcopy([message for batch in self.saved for message in batch])

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        # An external append is not undone by an entity storage failure.
        self.saved.append(deepcopy(list(messages)))
        if self.phase == "store":
            raise OSError("external store boundary failed")


class RejectedWriteStorage(JsonStateProvider):
    def __init__(self) -> None:
        super().__init__(_initial_state())
        self.attempts: list[dict[str, Any]] = []
        self.reject = True

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        self.attempts.append(json.loads(json.dumps(state)))
        if self.reject:
            raise OSError("entity storage write rejected")
        super()._set_state_dict(state)


def failure_boundary(phase: str) -> tuple[AgentEntity, RejectedWriteStorage, FailingExternalHistory, BarrierClient]:
    external = FailingExternalHistory(phase)
    client = BarrierClient(PhaseBarrier())
    agent: Any = _agent(client=client, name="failure-boundary", context_providers=[external])
    provider = RejectedWriteStorage()
    return AgentEntity(agent, state_provider=provider), provider, external, client


def assert_staged_not_committed(provider: RejectedWriteStorage, before: dict[str, Any], phase: str) -> None:
    assert len(provider.attempts) == 1 and provider.writes == 0
    assert provider.raw == before
    attempted = DurableAgentState.from_json(json.dumps(provider.attempts[0]))
    failed = attempted.try_get_agent_response("provider-failed")
    assert failed is not None
    assert failed.additional_properties["durable_status"] == "error"
    assert f"external {phase} boundary failed" in failed.text
    assert any(content.error_code == "OSError" for message in failed.messages for content in message.contents)
    assert "provider-failed" in attempted.data.completed_correlations
    assert DurableAgentState.from_json(json.dumps(provider.raw)).try_get_agent_response("provider-failed") is None


class _JsonDTBackend:
    """Signal acceptance and state reads only, with no fabricated response or completion."""

    def __init__(self, provider: JsonStateProvider) -> None:
        self.provider = provider
        self.signals: list[Any] = []
        self.reads = 0

    def signal_entity(self, *args: Any) -> None:
        self.signals.append(args)

    def get_entity(self, entity_id: Any, *, include_state: bool) -> Any:
        assert include_state
        self.reads += 1
        return SimpleNamespace(get_state=lambda: json.dumps(self.provider.raw))


@pytest.mark.parametrize("phase", ["load", "store"])
async def test_external_failure_then_rejected_error_commit_is_invisible_to_real_dt_poller(
    phase: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    entity, provider, external, client = failure_boundary(phase)
    before = deepcopy(provider.raw)
    request = projected_request("provider-failed")
    with pytest.raises(OSError, match="entity storage write rejected"):
        await entity.run(request)
    assert_staged_not_committed(provider, before, phase)
    assert entity.state.to_dict() == before and current_durable_history_binding() is None
    assert external.loads == 1
    assert len(external.saved) == len(client.effects) == int(phase == "store")
    backend: Any = _JsonDTBackend(provider)
    sleep = Mock()
    monkeypatch.setattr("agent_framework_durabletask._executors.time.sleep", sleep)
    executor = ClientAgentExecutor(backend, max_poll_retries=3, poll_interval_seconds=0.01)
    response = executor.run_durable_agent("failure-boundary", RunRequest.from_dict(request))
    assert [content.error_code for message in response.messages for content in message.contents] == ["response_timeout"]
    assert backend.reads == 3 and len(backend.signals) == 1 and sleep.call_count == 3
    assert provider.raw == before and len(provider.attempts) == 1
    assert external.loads == 1, "polling is a reader, not a direct entity execution"

    # Permit a real error commit while the provider outage remains. That changes visibility.
    provider.reject = False
    failed = await entity.run(request)
    assert failed.additional_properties["durable_status"] == "error" and provider.writes == 1
    calls = (external.loads, len(external.saved), len(client.effects))
    delivered = executor.run_durable_agent("failure-boundary", RunRequest.from_dict(request))
    assert delivered.to_dict() == failed.to_dict() and backend.reads == 4
    external.phase = None
    cold = AgentEntity(entity.agent, state_provider=JsonStateProvider(provider.raw))
    assert (await cold.run(request)).to_dict() == failed.to_dict()
    assert (external.loads, len(external.saved), len(client.effects)) == calls
