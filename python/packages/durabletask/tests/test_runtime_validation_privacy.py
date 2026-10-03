# Copyright (c) Microsoft. All rights reserved.

"""Validation diagnostics across serialization, real SDK failures and durable delivery."""

from __future__ import annotations

import asyncio
import json
import logging
from copy import deepcopy
from typing import Any, cast
from unittest.mock import Mock

import httpx
import pytest
from _execution_test_support import (
    JsonStateProvider,
    LostAcknowledgementJsonStateProvider,
    NonStreamingAgent,
    RecordingChatClient,
    ToolChatClient,
)
from _validation_test_support import (
    PRIVATE,
    SAFE,
    Answer,
    GroupedValidationAgent,
    NormalizingAnswer,
    StructuredAgent,
    assert_private_failure,
    assert_private_logs,
    grouped_error,
    sdk_response,
    validation_error,
)
from agent_framework import Agent, AgentResponse, AgentSession, tool
from agent_framework.exceptions import ChatClientException
from agent_framework.openai import OpenAIChatClient
from agent_framework_azurefunctions._orchestration import AgentTask
from azure.durable_functions.models.actions.NoOpAction import NoOpAction
from azure.durable_functions.models.Task import AtomicTask
from durabletask.entities import EntityContext, EntityInstanceId
from durabletask.internal.entity_state_shim import StateShim
from durabletask.serialization import JsonDataConverter
from durabletask.task import CompletableTask
from openai import AsyncOpenAI
from pydantic import ValidationError

from agent_framework_durabletask import AgentEntity, DurableAIAgentWorker, RunRequest
from agent_framework_durabletask._entities import _validation_diagnostic
from agent_framework_durabletask._executors import DurableAgentTask
from agent_framework_durabletask._history_provider import current_durable_history_binding
from agent_framework_durabletask._response_utils import serialize_agent_response


@pytest.mark.parametrize("reader", ["durabletask", "functions"])
@pytest.mark.parametrize("precompleted", [False, True])
async def test_post_execution_revalidation_commits_safe_failure_and_cold_duplicate(
    reader: str, precompleted: bool, caplog: pytest.LogCaptureFixture
) -> None:
    agent = StructuredAgent()
    provider = JsonStateProvider()
    request = RunRequest(message="public question", correlation_id="private")
    caplog.set_level(logging.WARNING)

    # The producer returns a valid model. The existing lossless codec must still
    # reject its non-idempotent revalidation rather than storing altered data.
    model = NormalizingAnswer(answer=f"prefix:{PRIVATE}")
    assert model.answer == PRIVATE
    with pytest.raises(ValidationError):
        serialize_agent_response(AgentResponse(value=model))
    response = await AgentEntity(cast(Any, agent), state_provider=provider).run(request)

    wire = assert_private_failure(response)
    assert agent.calls == provider.successful_writes == 1
    data = provider.raw["data"]
    assert data["terminalResults"]["private"]["error"] == {"code": "ValidationError", "message": SAFE}
    assert data["completionReceipts"]["private"]["outcome"] == "failed"
    assert PRIVATE not in json.dumps(provider.raw)
    assert_private_logs(caplog)

    task: Any
    if reader == "durabletask":
        child: Any = CompletableTask()
        if precompleted:
            child.complete(wire)
        task = DurableAgentTask(child, NormalizingAnswer, "private")
        if not precompleted:
            child.complete(wire)
        delivered = task.get_result()
    else:
        child = AtomicTask(7, NoOpAction())
        if precompleted:
            child.set_value(is_error=False, value=wire)
        task = AgentTask(child, NormalizingAnswer, "private")
        if not precompleted:
            child.set_value(is_error=False, value=wire)
        delivered = task.result
    assert serialize_agent_response(delivered) == wire

    cold = JsonStateProvider(provider.raw)
    duplicate = await AgentEntity(cast(Any, agent), state_provider=cold).run(request)
    assert serialize_agent_response(duplicate) == wire
    assert agent.calls == 1 and cold.attempted_writes == 0


@pytest.mark.parametrize("grouped", [False, True], ids=["serialization", "group"])
def test_registered_durabletask_host_commits_safe_failure_and_suppresses_cold_duplicate(
    grouped: bool, caplog: pytest.LogCaptureFixture
) -> None:
    agent = GroupedValidationAgent() if grouped else StructuredAgent()
    native = Mock()
    worker = DurableAIAgentWorker(native, deployment_mode="isolated_v2")
    worker.add_agent(cast(Any, agent))
    factory = native.add_entity.call_args.args[0]

    def activate(raw: str | None) -> tuple[Any, StateShim]:
        converter = JsonDataConverter()
        shim = StateShim(raw, converter, is_serialized=True)
        context = EntityContext("orchestration", "operation", shim, EntityInstanceId("dafx-privacy", "s1"), converter)
        hosted = factory()
        hosted._initialize_entity_context(context)
        return hosted, shim

    caplog.set_level(logging.WARNING)
    hosted, shim = activate(None)
    request = {"message": "public question", "correlationId": "private"}
    wire = hosted.run(request)
    child: CompletableTask[Any] = CompletableTask()
    task = DurableAgentTask(child, NormalizingAnswer, "private")
    child.complete(wire)
    assert assert_private_failure(task.get_result(), code="ExceptionGroup" if grouped else "ValidationError") == wire
    raw = shim.encode_state()
    assert isinstance(raw, str) and PRIVATE not in raw
    assert json.loads(raw)["data"]["completionReceipts"]["private"]["outcome"] == "failed"
    cold, cold_shim = activate(raw)
    assert cold.run(request) == wire and agent.calls == 1
    cold_raw = cold_shim.encode_state()
    assert isinstance(cold_raw, str)
    assert json.loads(cold_raw) == json.loads(raw)
    assert_private_logs(caplog)


class FinalValueAgent(NonStreamingAgent):
    def run(self, *args: Any, **kwargs: Any) -> Any:
        result = super().run(*args, **kwargs)

        async def complete() -> AgentResponse[Any]:
            response = await result
            return AgentResponse(messages=response.messages, value=NormalizingAnswer(answer=f"prefix:{PRIVATE}"))

        return complete()


@pytest.mark.parametrize("store", [False, True], ids=["local-history", "service-history"])
@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
async def test_serialization_failure_preserves_tool_progress_prior_history_and_session(
    store: bool, per_call: bool
) -> None:
    effects: list[str] = []

    @tool(name="lookup", approval_mode="never_require")
    def lookup(key: str) -> str:
        effects.append(key)
        return f"value:{key}"

    provider = JsonStateProvider()
    await AgentEntity(Agent(client=RecordingChatClient(), name="privacy"), state_provider=provider).run({
        "message": "prior turn",
        "correlationId": "prior",
    })
    before = deepcopy(provider.raw)
    client = ToolChatClient()
    agent = FinalValueAgent(
        client=client,
        name="privacy",
        tools=[lookup],
        default_options={"store": store},
        require_per_service_call_history_persistence=per_call,
    )
    request = RunRequest(message="use lookup", correlation_id="private", options={"store": store})
    response = await AgentEntity(agent, state_provider=provider).run(request)

    assert_private_failure(response)
    assert effects == ["durable"] and len(client.received_messages) == 2
    data = provider.raw["data"]
    assert (
        data["conversationHistory"][: len(before["data"]["conversationHistory"])]
        == before["data"]["conversationHistory"]
    )
    assert data["terminalResults"]["prior"] == before["data"]["terminalResults"]["prior"]
    session = AgentSession.from_dict(data["session"])
    assert session.session_id == "@runtime@runtime-session"
    if store:
        assert session.service_session_id == "service-thread"
    else:
        results = [
            c
            for entry in data["conversationHistory"]
            for m in entry["messages"]
            for c in m["contents"]
            if c["$type"] == "functionResult"
        ]
        assert any(c["callId"] == "call-1" and c["result"] == "value:durable" for c in results)
    assert current_durable_history_binding() is None
    cold = JsonStateProvider(provider.raw)
    duplicate = await AgentEntity(agent, state_provider=cold).run(request)
    assert serialize_agent_response(duplicate) == serialize_agent_response(response)
    assert effects == ["durable"] and len(client.received_messages) == 2 and cold.attempted_writes == 0


@pytest.mark.parametrize("kind", ["plain", "wrapped", "group"])
async def test_direct_validation_failure_commits_sanitized_result_only(
    kind: str, caplog: pytest.LogCaptureFixture
) -> None:
    error: BaseException = validation_error(wrapped=kind == "wrapped")
    if kind == "group":
        error = grouped_error(RuntimeError("ordinary sibling"), grouped_error(error))

    class FailingAgent(StructuredAgent):
        async def run(self, **kwargs: Any) -> AgentResponse[Any]:
            self.calls += 1
            raise error

    agent = FailingAgent()
    provider = JsonStateProvider()
    caplog.set_level(logging.WARNING)
    request = RunRequest(message="public question", correlation_id="private")
    response = await AgentEntity(cast(Any, agent), state_provider=provider).run(request)
    code = {"plain": "ValidationError", "wrapped": "ChatClientException", "group": "ExceptionGroup"}[kind]
    wire = assert_private_failure(response, code=code)
    assert agent.calls == provider.successful_writes == 1
    assert PRIVATE not in json.dumps(provider.raw)
    assert provider.raw["data"]["terminalResults"]["private"]["error"] == {"code": code, "message": SAFE}
    cold = JsonStateProvider(provider.raw)
    duplicate = await AgentEntity(cast(Any, agent), state_provider=cold).run(request)
    assert serialize_agent_response(duplicate) == wire
    assert agent.calls == 1 and cold.attempted_writes == 0
    assert_private_logs(caplog)


async def test_unserializable_session_does_not_commit_a_failure_receipt_without_its_progress() -> None:
    class UnsafeSessionAgent(StructuredAgent):
        context_providers: list[Any] = []

        def create_session(self, *, session_id: str) -> AgentSession:
            return AgentSession(session_id=session_id)

        async def run(self, **kwargs: Any) -> AgentResponse[Any]:
            session = cast(AgentSession, kwargs.pop("session"))
            session.state["uncommittable"] = {"not_finite": float("nan")}
            return await super().run(**kwargs)

    provider = JsonStateProvider()
    entity = AgentEntity(cast(Any, UnsafeSessionAgent()), state_provider=provider)
    original = entity.state
    with pytest.raises(ValueError, match="not JSON-compatible|finite float"):
        await entity.run(RunRequest(message="public question", correlation_id="private"))
    assert entity.state is original and provider.raw == {} and provider.attempted_writes == 0
    assert entity.state.try_get_agent_response("private") is None
    assert current_durable_history_binding() is None


@pytest.mark.parametrize("lost_ack", [False, True], ids=["rejected-write", "lost-acknowledgement"])
async def test_safe_failure_commit_retains_existing_rollback_and_uncertain_ack_rules(lost_ack: bool) -> None:
    provider = LostAcknowledgementJsonStateProvider() if lost_ack else JsonStateProvider()
    provider.fail_before_write = not lost_ack
    agent = StructuredAgent()
    entity = AgentEntity(cast(Any, agent), state_provider=provider)
    original = entity.state
    request = RunRequest(message="public question", correlation_id="private")

    with pytest.raises(OSError, match="acknowledgement lost|injected commit failure"):
        await entity.run(request)

    assert entity.state is original
    assert entity.state.try_get_agent_response("private") is None
    assert agent.calls == provider.attempted_writes == 1
    assert provider.successful_writes == int(lost_ack)
    if not lost_ack:
        assert provider.raw == {}
        provider.fail_before_write = False
    response = await entity.run(request)
    assert_private_failure(response)
    assert agent.calls == (1 if lost_ack else 2)
    duplicate = await AgentEntity(cast(Any, agent), state_provider=JsonStateProvider(provider.raw)).run(request)
    assert serialize_agent_response(duplicate) == serialize_agent_response(response)
    assert agent.calls == (1 if lost_ack else 2)


async def test_real_openai_validation_cause_is_sanitized_before_durable_delivery(
    caplog: pytest.LogCaptureFixture,
) -> None:
    calls: list[dict[str, Any]] = []

    async def handle(request: httpx.Request) -> httpx.Response:
        calls.append(json.loads(request.content))
        return httpx.Response(200, json=sdk_response(json.dumps({"answer": PRIVATE})))

    caplog.set_level(logging.WARNING)
    async with (
        httpx.AsyncClient(transport=httpx.MockTransport(handle)) as transport,
        AsyncOpenAI(
            api_key="offline-placeholder",
            base_url="https://offline.invalid/v1",
            http_client=transport,
            max_retries=0,
        ) as sdk,
    ):
        client = OpenAIChatClient(async_client=sdk, model="offline-model")
        agent = NonStreamingAgent(client=client, name="privacy")
        # Prove that this SDK path really wraps Pydantic, not just that an
        # artificial exception with the same class name can be sanitized.
        with pytest.raises(ChatClientException) as captured:
            await agent.run("public question", options={"response_format": Answer})
        assert isinstance(captured.value.__cause__, ValidationError)
        assert PRIVATE in str(captured.value)
        calls.clear()
        provider = JsonStateProvider()
        request = RunRequest(message="public question", correlation_id="private", response_format=Answer)
        response = await AgentEntity(agent, state_provider=provider).run(request)
        wire = assert_private_failure(response, code="ChatClientException")
        cold = JsonStateProvider(provider.raw)
        duplicate = await AgentEntity(agent, state_provider=cold).run(request)
        assert serialize_agent_response(duplicate) == wire
        assert len(calls) == provider.successful_writes == 1 and cold.attempted_writes == 0
        assert PRIVATE not in json.dumps(provider.raw["data"]["terminalResults"])
    assert_private_logs(caplog)


class WrappedValidationCallback:
    def __init__(self, *, grouped: bool = False) -> None:
        self.calls: list[str] = []
        self.grouped = grouped

    async def on_streaming_response_update(self, update: Any, context: Any) -> None:
        self.calls.append("stream")
        error = validation_error(wrapped=not self.grouped)
        raise grouped_error(error) if self.grouped else error

    async def on_agent_response(self, response: Any, context: Any) -> None:
        self.calls.append("final")
        error = validation_error(wrapped=not self.grouped)
        raise grouped_error(error) if self.grouped else error


@pytest.mark.parametrize("grouped", [False, True], ids=["wrapped", "group"])
async def test_wrapped_validation_callback_warnings_do_not_leak_or_fail_the_turn(
    grouped: bool, caplog: pytest.LogCaptureFixture
) -> None:
    # Skip before dispatch on Python 3.10, rather than raising pytest's skip
    # exception from inside a callback.
    if grouped:
        grouped_error(validation_error(wrapped=False))
    callback = WrappedValidationCallback(grouped=grouped)
    provider = JsonStateProvider()
    caplog.set_level(logging.WARNING)
    response = await AgentEntity(
        Agent(client=RecordingChatClient(), name="privacy"), callback=callback, state_provider=provider
    ).run({"message": "public", "correlationId": "callback"})
    assert response.text == "reply-1"
    assert callback.calls == ["stream", "final"] and provider.successful_writes == 1
    assert_private_logs(caplog)


def test_validation_cause_walk_handles_cycles_and_unrelated_errors_without_rendering() -> None:
    outer = RuntimeError("ordinary detail")
    middle = RuntimeError("ordinary middle")
    outer.__cause__ = middle
    middle.__cause__ = outer
    assert _validation_diagnostic(outer) is None
    middle.__context__ = validation_error(wrapped=False)
    assert _validation_diagnostic(outer) == SAFE


def test_validation_group_walk_handles_cycles_and_ignores_non_group_children() -> None:
    validation = validation_error(wrapped=False)
    ordinary = RuntimeError("ordinary detail")
    ordinary.exceptions = (validation,)  # type: ignore[attr-defined]
    assert _validation_diagnostic(ordinary) is None
    group = grouped_error(ordinary)
    ordinary.__cause__ = group
    assert _validation_diagnostic(group) is None
    ordinary.__context__ = validation
    assert _validation_diagnostic(group) == SAFE


async def test_generic_exception_group_keeps_details_and_traceback(caplog: pytest.LogCaptureFixture) -> None:
    error = grouped_error(OSError("ordinary child detail"))

    class FailingAgent(StructuredAgent):
        async def run(self, **kwargs: Any) -> AgentResponse[Any]:
            self.calls += 1
            raise error

    provider = JsonStateProvider()
    caplog.set_level(logging.WARNING)
    response = await AgentEntity(cast(Any, FailingAgent()), state_provider=provider).run(
        RunRequest(message="public question", correlation_id="ordinary")
    )
    assert _validation_diagnostic(error) is None
    assert response.additional_properties["durable_status"] == "error"
    assert response.text == f"ExceptionGroup: {error}"
    assert provider.successful_writes == 1
    record = next(record for record in caplog.records if record.name == "agent_framework.durabletask")
    assert record.exc_info is not None and record.exc_info[1] is error
    assert "ordinary child detail" in caplog.text


@pytest.mark.parametrize("phase", ["execution", "stream", "final"])
async def test_cancellation_group_propagates_without_failure_receipt_or_callback_swallowing(
    phase: str, caplog: pytest.LogCaptureFixture
) -> None:
    error = grouped_error(asyncio.CancelledError("cancelled"), validation_error(wrapped=False))
    assert not isinstance(error, Exception)

    class CancellingAgent(StructuredAgent):
        async def run(self, **kwargs: Any) -> AgentResponse[Any]:
            self.calls += 1
            raise error

    class CancellingCallback:
        async def on_streaming_response_update(self, update: Any, context: Any) -> None:
            if phase == "stream":
                raise error

        async def on_agent_response(self, response: Any, context: Any) -> None:
            raise error

    agent = CancellingAgent() if phase == "execution" else Agent(client=RecordingChatClient(), name="privacy")
    provider = JsonStateProvider()
    entity = AgentEntity(
        cast(Any, agent),
        callback=None if phase == "execution" else CancellingCallback(),
        state_provider=provider,
    )
    original = entity.state
    caplog.set_level(logging.WARNING)
    with pytest.raises(BaseException) as captured:
        await entity.run(RunRequest(message="public question", correlation_id="cancelled"))
    assert captured.value is error
    assert entity.state is original and provider.raw == {} and provider.attempted_writes == 0
    assert current_durable_history_binding() is None
    assert PRIVATE not in caplog.text
