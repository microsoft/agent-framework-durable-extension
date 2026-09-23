# Copyright (c) Microsoft. All rights reserved.

"""Preserve async streaming without restarting application work after an awaited failure."""

import asyncio
from collections.abc import AsyncIterable, Awaitable, Callable
from copy import deepcopy
from typing import Any, cast

import pytest
from _execution_test_support import JsonStateProvider, NonStreamingAgent, RecordingChatClient
from agent_framework import (
    Agent,
    AgentContext,
    AgentMiddleware,
    AgentResponse,
    AgentResponseUpdate,
    Content,
    Message,
    ResponseStream,
)

from agent_framework_durabletask import AgentCallbackContext, AgentEntity
from agent_framework_durabletask._invocation_safety import InvocationProgress


def _no_keyword_helper() -> None:
    raise AssertionError("The helper body must not run after an argument-binding error")


class _FinalizeThenBadKeyword(AgentMiddleware):
    def __init__(self) -> None:
        self.modes: list[bool] = []
        self.finalized: list[str] = []
        self.before: list[Any] = []
        self.after: list[Any] = []
        self.errors: list[TypeError] = []

    async def process(self, context: AgentContext, call_next: Callable[[], Awaitable[None]]) -> None:
        self.modes.append(context.stream)
        assert context.session is not None
        self.before.append(deepcopy(context.session.service_session_id))
        await call_next()
        if context.stream:
            assert isinstance(context.result, ResponseStream)
            response = await context.result.get_final_response()
            assert isinstance(response, AgentResponse)
            self.finalized.append(response.text)
            self.after.append(deepcopy(context.session.service_session_id))
            helper: Any = _no_keyword_helper
            try:
                helper(stream=True)
            except TypeError as error:
                self.errors.append(error)
                raise


class _RecordingCallback:
    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []
        self.updates: list[AgentResponseUpdate] = []
        self.responses: list[AgentResponse] = []
        self.contexts: list[AgentCallbackContext] = []

    async def on_streaming_response_update(self, update: AgentResponseUpdate, context: AgentCallbackContext) -> None:
        self.events.append(("update", update.text))
        self.updates.append(update)
        self.contexts.append(context)

    async def on_agent_response(self, response: AgentResponse, context: AgentCallbackContext) -> None:
        self.events.append(("final", response.text))
        self.responses.append(response)
        self.contexts.append(context)


async def _run_and_redeliver(
    agent: Any, *, callback: _RecordingCallback | None = None
) -> tuple[AgentResponse, dict[str, Any]]:
    request = {"message": "hello", "correlationId": "fallback"}
    provider = JsonStateProvider()
    response = await AgentEntity(agent, callback=callback, state_provider=provider).run(request)
    assert (provider.attempted_writes, provider.successful_writes) == (1, 1)
    committed = deepcopy(provider.raw)
    cold = JsonStateProvider(committed)
    duplicate = await AgentEntity(agent, callback=callback, state_provider=cold).run(request)
    assert duplicate.to_dict() == response.to_dict()
    assert (cold.attempted_writes, cold.successful_writes) == (0, 0)
    assert cold.raw == committed
    return response, committed


def _assert_failed(response: AgentResponse, committed: dict[str, Any]) -> None:
    assert response.additional_properties["durable_status"] == "error"
    assert [c.error_code for m in response.messages for c in m.contents if c.type == "error"] == ["TypeError"]
    assert committed["data"]["completionReceipts"]["fallback"]["outcome"] == "failed"
    assert committed["data"]["terminalResults"]["fallback"]["outcome"] == "failed"


@pytest.mark.parametrize("store", [False, True], ids=["local-history", "service-history"])
async def test_eager_core_middleware_failure_after_finalization_does_not_restart_model(store: bool) -> None:
    client = RecordingChatClient(conversation_id="S1")
    middleware = _FinalizeThenBadKeyword()
    agent = Agent(client=client, name="eager", middleware=[middleware], default_options={"store": store})
    response, committed = await _run_and_redeliver(agent)
    assert middleware.finalized == ["reply-1"] and len(middleware.errors) == 1
    assert "unexpected keyword argument 'stream'" in str(middleware.errors[0])
    assert middleware.before == [None] and middleware.after == (["S1"] if store else [None])
    assert len(client.received_messages) == 1 and middleware.modes == [True]
    _assert_failed(response, committed)


class _DelegatingNonStreamingAgent(NonStreamingAgent):
    def run(self, *args: Any, **kwargs: Any) -> Any:
        # The refusal originates in the inherited run(), before either body
        # starts a model. Existing result-wrapping agents use this pattern.
        return super().run(*args, **kwargs)


class _LegacyAsyncNonStreamingAgent(NonStreamingAgent):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.modes: list[bool] = []

    # Intentional async override for the legacy-refusal fixture.
    async def run(self, *args: Any, **kwargs: Any) -> Any:  # type: ignore[override]
        self.modes.append(bool(kwargs.get("stream")))
        if kwargs.get("stream"):
            raise TypeError("streaming not supported")
        return await super().run(*args, **kwargs)


@pytest.mark.parametrize("agent_type", [NonStreamingAgent, _DelegatingNonStreamingAgent])
async def test_synchronous_refusal_in_direct_or_inherited_run_completes_once(agent_type: Any) -> None:
    client = RecordingChatClient()
    agent = agent_type(client=client, name="nonstream")
    callback = _RecordingCallback()
    response, committed = await _run_and_redeliver(agent, callback=callback)
    assert response.text == "reply-1" and len(client.received_messages) == 1
    assert committed["data"]["completionReceipts"]["fallback"]["outcome"] == "succeeded"
    assert callback.events == [("final", "reply-1")]
    assert callback.updates == [] and len(callback.responses) == 1


class _SyncRefusalRun:
    def __init__(self, detail: str, *, from_helper: bool = False) -> None:
        self.error = TypeError(detail)
        self.from_helper = from_helper
        self.modes: list[bool] = []
        self.helper_calls = 0
        self.responses: list[AgentResponse] = []

    def __call__(self, *, stream: bool = False, **kwargs: Any) -> AgentResponse:
        self.modes.append(stream)
        if stream:
            if self.from_helper:
                self._reject()
            raise self.error
        response = AgentResponse(
            messages=[Message("assistant", ["callable response"])],
            response_id="callable-response",
        )
        self.responses.append(response)
        return response

    def _reject(self) -> None:
        self.helper_calls += 1
        raise self.error


class _SyncCallableAgent:
    name = "sync-callable"

    def __init__(self, runner: _SyncRefusalRun, *, bound_call: bool = False) -> None:
        self.run = runner.__call__ if bound_call else runner


@pytest.mark.parametrize("bound_call", [False, True], ids=["callable-object", "bound-call-method"])
@pytest.mark.parametrize("detail", ["stream is not supported", "streaming not supported"])
async def test_synchronous_callable_refusal_returns_direct_response_once(bound_call: bool, detail: str) -> None:
    runner = _SyncRefusalRun(detail)
    agent = _SyncCallableAgent(runner, bound_call=bound_call)
    callback = _RecordingCallback()
    response, committed = await _run_and_redeliver(agent, callback=callback)
    assert runner.modes == [True, False] and runner.helper_calls == 0
    assert len(runner.responses) == 1 and isinstance(runner.responses[0], AgentResponse)
    assert response.text == "callable response" and response.response_id == "callable-response"
    assert committed["data"]["completionReceipts"]["fallback"]["outcome"] == "succeeded"
    assert committed["data"]["terminalResults"]["fallback"]["outcome"] == "succeeded"
    assert callback.events == [("final", "callable response")]
    assert callback.updates == [] and len(callback.responses) == 1
    assert callback.responses[0] is not runner.responses[0]
    assert callback.responses[0].response_id == "callable-response"


@pytest.mark.parametrize("bound_call", [False, True], ids=["callable-object", "bound-call-method"])
@pytest.mark.parametrize("detail", ["stream is not supported", "streaming not supported"])
async def test_synchronous_callable_helper_refusal_does_not_retry(bound_call: bool, detail: str) -> None:
    runner = _SyncRefusalRun(detail, from_helper=True)
    agent = _SyncCallableAgent(runner, bound_call=bound_call)
    callback = _RecordingCallback()
    response, committed = await _run_and_redeliver(agent, callback=callback)
    assert runner.modes == [True] and runner.helper_calls == 1
    assert runner.responses == []
    assert callback.events == [] and callback.updates == [] and callback.responses == []
    assert response.text == f"TypeError: {detail}"
    _assert_failed(response, committed)


async def test_legacy_async_refusal_is_terminal_even_before_model_progress() -> None:
    client = RecordingChatClient()
    agent = _LegacyAsyncNonStreamingAgent(client=client, name="legacy-async")
    response, committed = await _run_and_redeliver(agent)
    # This intentional fail-closed boundary replaces the old async-refusal
    # fallback. No observed progress does not prove that an async body is safe.
    assert agent.modes == [True] and client.received_messages == []
    assert response.text == "TypeError: streaming not supported"
    _assert_failed(response, committed)


def _async_agent(body: Callable[..., Awaitable[Any]], *, callable_run: bool) -> Any:
    class AsyncMethodAgent:
        name = "async-agent"

        async def run(self, *, stream: bool = False, **kwargs: Any) -> Any:
            return await body(stream=stream, **kwargs)

    class AsyncRun:
        async def __call__(self, *, stream: bool = False, **kwargs: Any) -> Any:
            return await body(stream=stream, **kwargs)

    class AsyncCallableAgent:
        name = "async-agent"

        def __init__(self) -> None:
            self.run = AsyncRun()

    return AsyncCallableAgent() if callable_run else AsyncMethodAgent()


@pytest.mark.parametrize("callable_run", [False, True], ids=["async-method", "async-callable"])
@pytest.mark.parametrize("result_kind", ["stream", "response"])
async def test_async_run_preserves_stream_selection_and_exact_callbacks(callable_run: bool, result_kind: str) -> None:
    modes: list[bool] = []
    chunks = [
        AgentResponseUpdate(
            role="assistant",
            contents=[Content.from_text(text)],
            message_id="async-message",
            response_id="async-response",
        )
        for text in ("async ", "response")
    ]
    direct_response = AgentResponse(
        messages=[Message("assistant", ["async response"], message_id="async-message")],
        response_id="async-response",
    )

    async def updates() -> AsyncIterable[AgentResponseUpdate]:
        for chunk in chunks:
            yield chunk

    async def run_body(*, stream: bool = False, **kwargs: Any) -> Any:
        modes.append(stream)
        await asyncio.sleep(0)
        if stream and result_kind == "stream":
            return ResponseStream(updates(), finalizer=AgentResponse.from_updates)
        # A demotion to non-streaming still returns a successful response, but
        # must fail the explicit mode and callback assertions below.
        return direct_response

    callback = _RecordingCallback()
    response, committed = await _run_and_redeliver(_async_agent(run_body, callable_run=callable_run), callback=callback)
    assert modes == [True]
    assert response.text == "async response" and response.response_id == "async-response"
    assert committed["data"]["completionReceipts"]["fallback"]["outcome"] == "succeeded"
    assert committed["data"]["terminalResults"]["fallback"]["outcome"] == "succeeded"
    expected_updates = chunks if result_kind == "stream" else []
    assert [update.to_dict() for update in callback.updates] == [update.to_dict() for update in expected_updates]
    assert all(actual is not original for actual, original in zip(callback.updates, expected_updates, strict=True))
    assert callback.events == (
        [("update", "async "), ("update", "response"), ("final", "async response")]
        if result_kind == "stream"
        else [("final", "async response")]
    )
    assert len(callback.responses) == 1
    assert callback.responses[0].text == "async response"
    assert callback.responses[0].response_id == "async-response"
    assert callback.responses[0] is not direct_response
    assert callback.contexts == [
        AgentCallbackContext(
            agent_name="async-agent", correlation_id="fallback", session_id="runtime-session", request_message="hello"
        )
    ] * (len(expected_updates) + 1)


@pytest.mark.parametrize("callable_run", [False, True], ids=["async-method", "async-callable"])
@pytest.mark.parametrize("phase", ["no-effects", "after-await"])
@pytest.mark.parametrize(
    "detail",
    ["stream is not supported", "streaming not supported", "unexpected keyword argument 'stream'", "application error"],
)
async def test_async_body_type_errors_never_authorize_a_nonstream_retry(
    callable_run: bool, phase: str, detail: str
) -> None:
    modes: list[bool] = []
    effects: list[str] = []
    error = TypeError(detail)

    async def run_body(*, stream: bool = False, **kwargs: Any) -> AgentResponse:
        modes.append(stream)
        if phase == "after-await":
            await asyncio.sleep(0)
            effects.append("application effect")
        if stream:
            raise error
        return AgentResponse(messages=[Message("assistant", ["must not restart"])])

    callback = _RecordingCallback()
    response, committed = await _run_and_redeliver(_async_agent(run_body, callable_run=callable_run), callback=callback)
    assert modes == [True]
    assert effects == (["application effect"] if phase == "after-await" else [])
    assert callback.events == [] and callback.updates == [] and callback.responses == []
    assert response.text == f"TypeError: {detail}"
    _assert_failed(response, committed)


@pytest.mark.parametrize("callable_run", [False, True], ids=["run-method", "callable-object"])
@pytest.mark.parametrize("field", ["stream_started", "function_started", "service_completed"])
async def test_observed_progress_blocks_even_an_explicit_synchronous_refusal(field: str, callable_run: bool) -> None:
    progress = InvocationProgress()
    setattr(progress, field, True)
    client = RecordingChatClient()
    runner = _SyncRefusalRun("stream is not supported")
    agent: Any = _SyncCallableAgent(runner) if callable_run else NonStreamingAgent(client=client, name="progress")
    entity = AgentEntity(agent, state_provider=JsonStateProvider())
    with pytest.raises(TypeError, match="stream is not supported"):
        await entity._invoke_agent({}, "fallback", "session", "hello", progress)
    assert client.received_messages == []
    if callable_run:
        assert runner.modes == [True] and runner.responses == []


class _NoStreamKeywordAgent:
    name = "binding-only"

    def __init__(self) -> None:
        self.calls = 0

    def run(self, *, messages: Any, options: Any) -> AgentResponse:
        self.calls += 1
        return AgentResponse(messages=[Message("assistant", ["bound once"])])


class _AsyncNoStreamKeywordAgent:
    name = "binding-only"

    def __init__(self) -> None:
        self.calls = 0

    async def run(self, *, messages: Any, options: Any) -> AgentResponse:
        self.calls += 1
        await asyncio.sleep(0)
        return AgentResponse(messages=[Message("assistant", ["bound once"])])


class _AsyncNoStreamRun:
    def __init__(self) -> None:
        self.calls = 0

    async def __call__(self, *, messages: Any, options: Any) -> AgentResponse:
        self.calls += 1
        await asyncio.sleep(0)
        return AgentResponse(messages=[Message("assistant", ["bound once"])])


class _AsyncNoStreamCallableAgent:
    name = "binding-only"

    def __init__(self) -> None:
        self.run = _AsyncNoStreamRun()

    @property
    def calls(self) -> int:
        return self.run.calls


@pytest.mark.parametrize("agent_type", [_NoStreamKeywordAgent, _AsyncNoStreamKeywordAgent, _AsyncNoStreamCallableAgent])
async def test_actual_run_argument_binding_failure_still_falls_back_before_body(agent_type: Any) -> None:
    # These signatures declare no stream keyword and no **kwargs. Their Python
    # argument-binding failures happen before any synchronous or async body runs.
    agent = agent_type()
    callback = _RecordingCallback()
    response, committed = await _run_and_redeliver(agent, callback=callback)
    assert response.text == "bound once" and agent.calls == 1
    assert committed["data"]["completionReceipts"]["fallback"]["outcome"] == "succeeded"
    assert callback.events == [("final", "bound once")]
    assert callback.updates == [] and len(callback.responses) == 1


class _FactoryAgent:
    name = "factory"

    def __init__(self, phase: str, error: BaseException) -> None:
        self.phase = phase
        self.error = error
        self.modes: list[bool] = []
        self.effects: list[str] = []

    def run(self, *, stream: bool = False, **kwargs: Any) -> Any:
        self.modes.append(stream)
        if stream and self.phase == "sync-helper":
            self.effects.append("sync-body")
            helper: Any = _no_keyword_helper
            helper(stream=True)
        if stream and self.phase == "sync-nested-refusal":
            self.effects.append("sync-body")
            self._reject()

        async def setup() -> AgentResponse:
            self.effects.append("setup")
            if stream:
                raise self.error
            return AgentResponse(messages=[Message("assistant", ["must not restart"])])

        async def updates() -> AsyncIterable[AgentResponseUpdate]:
            self.effects.append("stream")
            yield AgentResponseUpdate(contents=[Content.from_text("partial")])
            raise self.error

        if stream and self.phase == "stream":
            return ResponseStream(updates(), finalizer=AgentResponse.from_updates)
        return setup()

    def _reject(self) -> None:
        raise self.error


@pytest.mark.parametrize("phase", ["setup", "stream", "sync-helper", "sync-nested-refusal"])
@pytest.mark.parametrize("detail", ["stream is not supported", "unexpected keyword argument 'stream'"])
async def test_application_type_errors_are_terminal_not_capability_probes(phase: str, detail: str) -> None:
    agent = _FactoryAgent(phase, TypeError(detail))
    callback = _RecordingCallback()
    response, committed = await _run_and_redeliver(agent, callback=callback)
    assert agent.modes == [True]
    assert agent.effects == ["sync-body" if phase.startswith("sync-") else phase]
    assert callback.events == ([("update", "partial")] if phase == "stream" else [])
    assert callback.responses == []
    if phase == "sync-helper":
        assert "_no_keyword_helper() got an unexpected keyword argument 'stream'" in response.text
    else:
        assert response.text == f"TypeError: {detail}"
    _assert_failed(response, committed)


@pytest.mark.parametrize("call_shape", ["factory", "async-method", "async-callable"])
@pytest.mark.parametrize("error_type", [TypeError, ValueError, RuntimeError])
async def test_awaited_setup_preserves_original_exception_identity(
    call_shape: str, error_type: type[Exception]
) -> None:
    error = error_type("stream is not supported")
    factory = _FactoryAgent("setup", error)
    agent = (
        factory if call_shape == "factory" else _async_agent(factory.run, callable_run=call_shape == "async-callable")
    )
    entity = AgentEntity(cast(Any, agent), state_provider=JsonStateProvider())
    with pytest.raises(error_type) as captured:
        await entity._invoke_agent({}, "fallback", "session", "hello")
    assert captured.value is error and factory.modes == [True]


@pytest.mark.parametrize("call_shape", ["factory", "async-method", "async-callable"])
async def test_cancelled_setup_does_not_fallback_or_commit(call_shape: str) -> None:
    factory = _FactoryAgent("setup", asyncio.CancelledError())
    agent = (
        factory if call_shape == "factory" else _async_agent(factory.run, callable_run=call_shape == "async-callable")
    )
    provider = JsonStateProvider()
    with pytest.raises(asyncio.CancelledError):
        await AgentEntity(cast(Any, agent), state_provider=provider).run({
            "message": "hello",
            "correlationId": "fallback",
        })
    assert factory.modes == [True] and factory.effects == ["setup"]
    assert provider.raw == {} and (provider.attempted_writes, provider.successful_writes) == (0, 0)


@pytest.mark.parametrize("awaited", [False, True])
async def test_synchronous_factory_can_still_return_a_direct_or_awaited_response(awaited: bool) -> None:
    modes: list[bool] = []

    class DirectAgent:
        name = "direct"

        def run(self, *, stream: bool = False, **kwargs: Any) -> Any:
            modes.append(stream)
            response = AgentResponse(messages=[Message("assistant", ["direct response"])])

            async def complete() -> AgentResponse:
                return response

            return complete() if awaited else response

    response, committed = await _run_and_redeliver(DirectAgent())
    assert response.text == "direct response" and modes == [True]
    assert committed["data"]["completionReceipts"]["fallback"]["outcome"] == "succeeded"
