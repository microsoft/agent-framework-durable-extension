# Copyright (c) Microsoft. All rights reserved.

"""Runtime transaction boundaries for durable entity execution."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Mapping, Sequence
from copy import deepcopy
from typing import Any

import pytest
from _execution_test_support import (
    CountingHistory,
    JsonStateProvider,
    LostAcknowledgementJsonStateProvider,
    NonStreamingAgent,
    RecordingChatClient,
    ToolChatClient,
)
from agent_framework import (
    Agent,
    AgentResponse,
    AgentResponseUpdate,
    AgentSession,
    ChatResponse,
    ContextProvider,
    Message,
    SessionContext,
)

from agent_framework_durabletask import AgentEntity, DurableAgentState, DurableHistoryProvider
from agent_framework_durabletask._callbacks import AgentCallbackContext
from agent_framework_durabletask._history_provider import current_durable_history_binding
from agent_framework_durabletask._shared_response import serialize_terminal_response


def _json(value: Any) -> Any:
    return json.loads(json.dumps(value, allow_nan=False))


def _committed(provider: JsonStateProvider) -> dict[str, Any]:
    return _json(provider.raw)


def _request(correlation_id: str, message: str) -> dict[str, Any]:
    return {"message": message, "correlationId": correlation_id}


def _error_codes(response: AgentResponse) -> list[str]:
    return [
        content.error_code
        for message in response.messages
        for content in message.contents
        if content.type == "error" and content.error_code is not None
    ]


class _ControlProvider(ContextProvider):
    def __init__(self) -> None:
        super().__init__("control")
        self.loaded: list[dict[str, Any]] = []
        self.responses: list[AgentResponse] = []
        self.sessions: list[AgentSession] = []
        self.agents: list[Any] = []

    async def before_run(
        self,
        *,
        agent: Any,
        session: AgentSession,
        context: SessionContext,
        state: dict[str, Any],
    ) -> None:
        del context
        self.loaded.append(deepcopy(state))
        self.sessions.append(session)
        self.agents.append(agent)
        state["before_runs"] = state.get("before_runs", 0) + 1

    async def after_run(self, *, context: SessionContext, state: dict[str, Any], **kwargs: Any) -> None:
        del kwargs
        assert isinstance(context.response, AgentResponse)
        self.responses.append(context.response)
        state["after_runs"] = state.get("after_runs", 0) + 1


class _RecordingCallback:
    def __init__(self) -> None:
        self.updates: list[AgentResponseUpdate] = []
        self.responses: list[AgentResponse] = []

    async def on_streaming_response_update(self, update: AgentResponseUpdate, context: AgentCallbackContext) -> None:
        del context
        self.updates.append(deepcopy(update))

    async def on_agent_response(self, response: AgentResponse, context: AgentCallbackContext) -> None:
        del context
        self.responses.append(response)


class SimulatedWorkerStop(BaseException):
    pass


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


async def _await_boundary(task: asyncio.Task[Any], entered: asyncio.Event) -> None:
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

    def _inner_get_response(
        self,
        *,
        messages: Sequence[Message],
        stream: bool,
        options: Mapping[str, Any],
        **kwargs: Any,
    ) -> Awaitable[ChatResponse]:
        del kwargs
        if stream:
            raise TypeError("stream is not supported")
        self.received_messages.append(deepcopy(list(messages)))
        self.received_options.append(dict(options))

        async def get() -> ChatResponse:
            self.effects.append("model-side-effect")
            await self.barrier.wait("model")
            return ChatResponse(messages=[Message("assistant", ["boundary answer"], message_id="boundary-answer")])

        return get()


class BoundaryHistory(DurableHistoryProvider):
    def __init__(self, barrier: PhaseBarrier) -> None:
        super().__init__(skip_excluded=False)
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


class _FinalFlushFailureHistory(DurableHistoryProvider):
    def __init__(self) -> None:
        super().__init__(skip_excluded=False)
        self.fail_final_flush = False
        self.after_run_finished = False
        self.failed_snapshot: dict[str, Any] | None = None
        self.failures = 0

    async def after_run(self, **kwargs: Any) -> None:
        self.after_run_finished = False
        await super().after_run(**kwargs)
        self.after_run_finished = True

    def flush(self, state: dict[str, Any]) -> None:
        if self.fail_final_flush and self.after_run_finished:
            binding = current_durable_history_binding()
            assert binding is not None
            self.failed_snapshot = deepcopy(binding.state_provider.state.to_dict())
            self.failures += 1
            raise OSError("final durable history flush failed")
        super().flush(state)


class _PersistInsideHook(CountingHistory):
    def __init__(self, provider: JsonStateProvider, *, per_call: bool) -> None:
        super().__init__([])
        self.provider = provider
        self.per_call = per_call

    async def after_run(self, **kwargs: Any) -> None:
        await super().after_run(**kwargs)
        self.provider.persist_state()


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param({}, id="empty"),
        pytest.param({"data": {"foreign": {"values": [0, False, None, "text"]}}}, id="opaque-foreign"),
    ],
)
def test_json_state_provider_roundtrip_is_source_independent_and_counts_writes(raw: dict[str, Any]) -> None:
    provider = JsonStateProvider(raw, session_id="s1", entity_name="alpha")
    peer = JsonStateProvider(raw, session_id="s1", entity_name="beta")

    snapshot = provider._get_state_dict()
    snapshot.setdefault("changed", {})["nested"] = True
    assert provider.raw == _json(raw)
    assert provider.core_session_id == "@alpha@s1"
    assert peer.core_session_id == "@beta@s1"

    payload: dict[str, Any] = {"schemaVersion": "2.0.0", "data": {"session": {"state": {"flag": True}}}}
    provider._set_state_dict(payload)
    payload["data"]["session"]["state"]["flag"] = False
    assert provider.raw["data"]["session"]["state"]["flag"] is True
    assert provider.attempted_writes == 1
    assert provider.successful_writes == 1


def test_json_state_provider_fail_before_write_counts_attempt_and_preserves_raw() -> None:
    initial = {"schemaVersion": "2.0.0", "data": {"session": {"state": {"keep": True}}}}
    provider = JsonStateProvider(initial)
    provider.fail_before_write = True

    with pytest.raises(OSError, match="injected commit failure"):
        provider._set_state_dict({"schemaVersion": "2.0.0", "data": {"session": {"state": {"keep": False}}}})

    assert provider.raw == initial
    assert provider.attempted_writes == 1
    assert provider.successful_writes == 0


def test_state_setter_copies_candidate_and_does_not_alias_caller_state() -> None:
    provider = JsonStateProvider()
    candidate = DurableAgentState()
    candidate.record_response(
        "alias",
        AgentResponse(messages=[Message("assistant", ["first"])]),
        delivery_window_seconds=60,
    )

    provider.state = candidate
    candidate.record_response(
        "later",
        AgentResponse(messages=[Message("assistant", ["second"])]),
        delivery_window_seconds=60,
    )

    committed = DurableAgentState.from_dict(_committed(provider))
    assert committed.try_get_agent_response("alias") is not None
    assert committed.try_get_agent_response("later") is None
    assert provider.attempted_writes == 1
    assert provider.successful_writes == 1


async def test_committed_completion_is_immutable_against_response_mutation_and_cold_duplicate() -> None:
    provider = JsonStateProvider()
    client = RecordingChatClient(response_message_id="answer-1")
    entity = AgentEntity(Agent(client=client, name="mutable"), state_provider=provider)

    response = await entity.run(_request("immutable", "original"))
    original = response.to_dict()
    response.messages[0].additional_properties["tampered"] = True
    response.messages.append(Message("assistant", ["extra"]))

    assert _committed(provider)["data"]["terminalResults"]["immutable"]["response"] == serialize_terminal_response(
        AgentResponse.from_dict(original)
    )
    cold = AgentEntity(Agent(client=client, name="mutable"), state_provider=JsonStateProvider(_committed(provider)))
    duplicate = await cold.run(_request("immutable", "original"))
    assert duplicate.to_dict() == original
    assert len(client.received_messages) == 1


def test_nan_session_property_setter_rolls_back_raw_and_cache() -> None:
    provider = JsonStateProvider()
    candidate = DurableAgentState()
    candidate.data.session = {"nan": float("nan")}

    with pytest.raises(ValueError, match="strict JSON|finite numbers"):
        provider.state = candidate

    assert provider.raw == {}
    assert provider.state.data.session is None
    assert provider.attempted_writes == 0
    assert provider.successful_writes == 0


def test_nan_session_persist_state_rolls_back_raw_and_cache() -> None:
    provider = JsonStateProvider()
    provider.state.data.session = {"nan": float("nan")}

    with pytest.raises(ValueError, match="strict JSON|finite numbers"):
        provider.persist_state()

    assert provider.raw == {}
    assert provider.state.data.session is None
    assert provider.attempted_writes == 0
    assert provider.successful_writes == 0


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
async def test_provider_hook_cannot_commit_independently_of_enclosing_agent_operation(per_call: bool) -> None:
    provider = JsonStateProvider()
    history = _PersistInsideHook(provider, per_call=per_call)
    agent = Agent(
        client=ToolChatClient(tool_calls=False),
        name="hooked",
        context_providers=[history],
        require_per_service_call_history_persistence=per_call,
    )
    entity = AgentEntity(agent, state_provider=provider)

    response = await entity.run(_request("hook-rejected", "hello"))

    assert response.additional_properties["durable_status"] == "error"
    assert _error_codes(response) == ["ValueError"]
    assert "Provider hooks cannot commit independently" in response.text
    assert provider.attempted_writes == 1
    assert provider.successful_writes == 1
    assert _committed(provider)["data"]["terminalResults"]["hook-rejected"]["outcome"] == "failed"


async def test_lost_acknowledgement_requires_fresh_backend_read_and_suppresses_duplicate_execution() -> None:
    client = RecordingChatClient(response_message_id="answer-1")
    provider = LostAcknowledgementJsonStateProvider()
    entity = AgentEntity(Agent(client=client, name="unknown-commit"), state_provider=provider)

    with pytest.raises(OSError, match="acknowledgement lost"):
        await entity.run(_request("unknown-commit", "question"))

    assert provider.attempted_writes == 1
    assert provider.successful_writes == 1
    assert entity.state.try_get_agent_response("unknown-commit") is None
    committed = DurableAgentState.from_dict(_committed(provider)).try_get_agent_response("unknown-commit")
    assert committed is not None
    assert committed.text == "reply-1"

    cold = AgentEntity(
        Agent(client=client, name="unknown-commit"), state_provider=JsonStateProvider(_committed(provider))
    )
    duplicate = await cold.run(_request("unknown-commit", "question"))
    assert duplicate.to_dict() == committed.to_dict()
    assert len(client.received_messages) == 1


async def test_same_provider_rereads_backend_after_uncertain_write_before_retry() -> None:
    client = RecordingChatClient()
    provider = LostAcknowledgementJsonStateProvider()
    entity = AgentEntity(Agent(client=client, name="unknown-commit"), state_provider=provider)
    with pytest.raises(OSError, match="acknowledgement lost"):
        await entity.run(_request("same-provider", "question"))
    duplicate = await entity.run(_request("same-provider", "question"))
    assert duplicate.text == "reply-1"
    assert len(client.received_messages) == 1
    assert provider.attempted_writes == provider.successful_writes == 1


async def test_uncertain_write_refresh_failure_blocks_execution_until_a_valid_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = RecordingChatClient()
    provider = LostAcknowledgementJsonStateProvider()
    entity = AgentEntity(Agent(client=client, name="unknown-commit"), state_provider=provider)
    with pytest.raises(OSError, match="acknowledgement lost"):
        await entity.run(_request("uncertain-refresh", "question"))
    original_read = provider._get_state_dict

    def fail_read() -> dict[str, Any]:
        raise OSError("refresh unavailable")

    monkeypatch.setattr(provider, "_get_state_dict", fail_read)
    with pytest.raises(OSError, match="refresh unavailable"):
        await entity.run(_request("uncertain-refresh", "question"))
    assert len(client.received_messages) == 1
    monkeypatch.setattr(provider, "_get_state_dict", original_read)
    response = await entity.run(_request("uncertain-refresh", "question"))
    assert response.text == "reply-1"
    assert len(client.received_messages) == 1
    assert provider.attempted_writes == 1


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
async def test_final_flush_failure_rolls_back_local_state_and_unbinds(per_call: bool) -> None:
    history = _FinalFlushFailureHistory()
    control = _ControlProvider()
    client = ToolChatClient(tool_calls=False)
    agent = Agent(
        client=client,
        name="flush",
        default_options={"conversation_id": "stale", "store": False},
        context_providers=[history, control],
        require_per_service_call_history_persistence=per_call,
    )
    initial = DurableAgentState()
    session = AgentSession(session_id="runtime-session", service_session_id="saved-service-id")
    session.state = {"control": {"baseline": True}}
    initial.data.session = session.to_dict()
    provider = JsonStateProvider(_json(initial.to_dict()))
    entity = AgentEntity(agent, state_provider=provider)

    await entity.run(_request("previous", "previous input"))
    before = _committed(provider)
    original_state = entity.state
    original_agent = entity.agent
    history.fail_final_flush = True

    with pytest.raises(OSError, match="final durable history flush failed"):
        await entity.run(_request("flush-failed", "uncommitted input"))

    assert history.failures == 1
    failed_snapshot = history.failed_snapshot
    assert failed_snapshot is not None
    assert any(entry.get("correlationId") == "flush-failed" for entry in failed_snapshot["data"]["conversationHistory"])
    assert current_durable_history_binding() is None
    assert entity.agent is original_agent
    assert entity.state is original_state
    assert entity.state.to_dict() == DurableAgentState.from_dict(before).to_dict()
    assert _committed(provider) == before
    assert provider.successful_writes == 1
    assert entity.state.try_get_agent_response("flush-failed") is None

    history.fail_final_flush = False
    response = await entity.run(_request("flush-failed", "uncommitted input"))
    assert response.text == "answer-3"
    assert provider.successful_writes == 2
    assert current_durable_history_binding() is None
    assert control.sessions[-1].service_session_id == "saved-service-id"


@pytest.mark.parametrize("phase", ["before_run", "model", "after_run"])
@pytest.mark.parametrize("interruption", ["task-cancel", "base-exception-stop"])
async def test_cancellation_boundaries_roll_back_state_and_cleanup_binding(phase: str, interruption: str) -> None:
    barrier = PhaseBarrier()
    barrier.phase = phase
    history = BoundaryHistory(barrier)
    client = BarrierClient(barrier)
    agent = NonStreamingAgent(
        client=client,
        name="boundary",
        context_providers=[history],
        default_options={"store": False, "conversation_id": "inactive-default-id"},
    )
    initial = DurableAgentState()
    session = AgentSession(session_id="runtime-session", service_session_id="saved-service-id")
    session.state = {"foreign": {"pending_approval": {"id": "keep", "approved": False}}}
    initial.data.session = session.to_dict()
    provider = JsonStateProvider(_json(initial.to_dict()))
    entity = AgentEntity(agent, state_provider=provider, response_delivery_window_seconds=60)
    before = _committed(provider)
    original_state = entity.state
    original_agent = entity.agent
    task_final_bindings: list[Any] = []

    async def execute() -> AgentResponse:
        try:
            return await entity.run(_request("interrupted", "projected boundary input"))
        finally:
            task_final_bindings.append(current_durable_history_binding())

    task = asyncio.create_task(execute())
    try:
        await _await_boundary(task, barrier.entered)
        assert provider.raw == before
        assert provider.successful_writes == 0
        assert barrier.observed_binding is not None
        if interruption == "task-cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            barrier.stop = SimulatedWorkerStop("explicit worker-stop boundary")
            barrier.release.set()
            with pytest.raises(SimulatedWorkerStop) as error:
                await task
            assert error.value is barrier.stop
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    assert task_final_bindings == [None]
    assert current_durable_history_binding() is None
    assert entity.agent is original_agent
    assert entity.state is original_state
    assert entity.state.to_dict() == DurableAgentState.from_dict(before).to_dict()
    assert provider.raw == before
    assert provider.successful_writes == 0
    assert entity.state.try_get_agent_response("interrupted") is None

    barrier.phase = None
    barrier.stop = None
    barrier.release.set()
    response = await entity.run(_request("interrupted", "projected boundary input"))
    assert response.text == "boundary answer"
    assert provider.successful_writes == 1
    cold = AgentEntity(agent, state_provider=JsonStateProvider(_committed(provider)))
    assert (await cold.run(_request("interrupted", "projected boundary input"))).to_dict() == response.to_dict()


@pytest.mark.parametrize(
    ("model_fails", "expected_status", "expected_calls"),
    [
        pytest.param(False, "reply-2", 2, id="successful-turn-reexecutes-after-rejected-commit"),
        pytest.param(True, "error", 2, id="failed-turn-reexecutes-after-rejected-error-commit"),
    ],
)
async def test_commit_rejection_reverts_cache_and_requires_a_new_execution(
    model_fails: bool, expected_status: str, expected_calls: int
) -> None:
    provider = JsonStateProvider()
    provider.fail_before_write = True
    client = RecordingChatClient(fail=model_fails)
    entity = AgentEntity(Agent(client=client, name="rejected"), state_provider=provider)

    with pytest.raises(OSError, match="injected commit failure"):
        await entity.run(_request("rejected", "question"))

    assert provider.raw == {}
    assert provider.attempted_writes == 1
    assert provider.successful_writes == 0
    assert entity.state.try_get_agent_response("rejected") is None
    assert len(client.received_messages) == 1

    provider.fail_before_write = False
    response = await entity.run(_request("rejected", "question"))
    if model_fails:
        assert response.additional_properties["durable_status"] == expected_status
        assert _error_codes(response) == ["RuntimeError"]
    else:
        assert response.text == expected_status
    assert len(client.received_messages) == expected_calls
    assert provider.attempted_writes == 2
    assert provider.successful_writes == 1
    duplicate = await entity.run(_request("rejected", "question"))
    assert duplicate.to_dict() == response.to_dict()
    assert len(client.received_messages) == expected_calls


async def test_non_streaming_fallback_commits_once_without_replaying_the_model() -> None:
    callback = _RecordingCallback()
    client = RecordingChatClient(response_message_id="fallback-1")
    agent = NonStreamingAgent(client=client, name="fallback")
    provider = JsonStateProvider()
    entity = AgentEntity(agent, callback=callback, state_provider=provider)

    response = await entity.run(_request("fallback", "hello"))

    assert response.text == "reply-1"
    assert len(client.received_messages) == 1
    assert callback.updates == []
    assert len(callback.responses) == 1
    assert callback.responses[0].to_dict() == response.to_dict()
    assert provider.successful_writes == 1
    committed = _committed(provider)["data"]["terminalResults"]["fallback"]["response"]
    assert committed == serialize_terminal_response(response)
