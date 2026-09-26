# Copyright (c) Microsoft. All rights reserved.

"""Real SDK request equality before retry dispatch, independently observed over HTTP."""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from copy import deepcopy
from typing import Any, cast

import httpx
import pytest
from _entity_retry_test_support import _entity, _RefusingClient
from _execution_test_support import JsonStateProvider, NonStreamingAgent, RecordingChatClient
from _invocation_progress_test_support import _ScriptedNonStreamingClient
from _service_commit_test_support import _assert_failed, _committed, _MissingParent, _provider, _request
from _validation_test_support import sdk_response
from agent_framework import AgentSession, ChatContext, ChatMiddleware, ChatResponse, FunctionTool, Message
from agent_framework.openai import OpenAIChatClient
from openai import AsyncOpenAI
from pydantic import BaseModel, create_model

from agent_framework_durabletask import AgentEntity, DurableAgentState, RunRequest
from agent_framework_durabletask import _entities as entities
from agent_framework_durabletask._invocation_safety import DurableServiceClient

PRIVATE = "SYNTHETIC_PRIVATE_RETRY_OPTIONS_3291"
RETRY_STOP = "Whole-agent retry stopped: the prepared service request changed or could not be compared."


class VaryOptions(ChatMiddleware):
    def __init__(self, *, varying: bool) -> None:
        self.calls = 0
        self.varying = varying
        self.temperatures: list[float] = []

    async def process(self, context: ChatContext, call_next: Callable[[], Awaitable[None]]) -> None:
        self.calls += 1
        assert context.options is not None
        options = cast("dict[str, Any]", context.options)
        if self.varying:
            options["temperature"] = self.calls / 10
        self.temperatures.append(options["temperature"])
        await call_next()


@pytest.mark.parametrize("mutation", ["unchanged", "defaults", "backoff-defaults", "middleware"])
async def test_real_sdk_retry_does_not_dispatch_different_effective_options(
    mutation: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict[str, Any]] = []
    middleware = VaryOptions(varying=mutation == "middleware")
    agent: Any = None
    sleeps = 0

    async def handle(request: httpx.Request) -> httpx.Response:
        calls.append(json.loads(request.content))
        if len(calls) == 1:
            if mutation == "defaults":
                agent.default_options["temperature"] = 0.9
            return httpx.Response(
                400,
                json={
                    "error": {
                        "message": "parent missing",
                        "type": "invalid_request_error",
                        "code": "previous_response_not_found",
                        "param": "previous_response_id",
                    }
                },
            )
        return httpx.Response(200, json=sdk_response())

    async def backoff(delay: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if mutation == "backoff-defaults":
            agent.default_options["temperature"] = 0.9

    monkeypatch.setattr(entities.asyncio, "sleep", backoff)
    async with (
        httpx.AsyncClient(transport=httpx.MockTransport(handle)) as transport,
        AsyncOpenAI(
            api_key="offline-placeholder",
            base_url="https://offline.invalid/v1",
            http_client=transport,
            max_retries=0,
        ) as sdk,
    ):
        client = OpenAIChatClient(async_client=sdk, model="offline-model", middleware=[middleware])
        configuration = deepcopy(client.function_invocation_configuration)
        original_middleware = tuple(client.chat_middleware)
        agent = NonStreamingAgent(client=client, default_options={"store": True, "temperature": 0.1})
        seed = DurableAgentState()
        seed.data.session = AgentSession(
            session_id="@runtime@runtime-session", service_session_id="resp_parent"
        ).to_dict()
        provider = JsonStateProvider(seed.to_dict())
        request = RunRequest(
            message="public question",
            correlation_id="retry",
            options={"store": True},
            context_messages=[Message("user", ["public question"], message_id="source").to_dict()],
            context_message_ids=["occ-source"],
        )
        response = await AgentEntity(agent, state_provider=provider).run(request)

        assert calls and calls[0]["temperature"] == 0.1
        assert all(call["previous_response_id"] == "resp_parent" for call in calls)
        assert all(call["input"] == calls[0]["input"] for call in calls)
        assert agent.client is client
        assert client.function_invocation_configuration == configuration
        assert tuple(client.chat_middleware) == original_middleware
        assert provider.successful_writes == 1
        count = len(calls)
        cold = JsonStateProvider(provider.raw)
        await AgentEntity(agent, state_provider=cold).run(request)
        assert len(calls) == count and cold.attempted_writes == 0

        if mutation in {"defaults", "backoff-defaults"}:
            assert count == 1
            assert response.additional_properties["durable_status"] == "error"
            assert sleeps == int(mutation == "backoff-defaults")
            assert agent.default_options["temperature"] == 0.9  # Never undo caller changes.
        elif mutation == "unchanged":
            assert count == 2 and calls[0] == calls[1] and response.text == "ok"
            assert set(DurableAgentState.from_dict(provider.raw).data.ingested_messages) == {"occ-source"}
        else:
            # Middleware actually produced 0.2. Do not restore its mutation or
            # merely report an error after a changed request already escaped.
            assert middleware.temperatures == [0.1, 0.2]
            assert middleware.calls == 2 and sleeps == 1 and count == 1
            assert response.additional_properties["durable_status"] == "error"
            errors = [c for m in response.messages for c in m.contents if c.type == "error"]
            assert [(c.error_code, c.message) for c in errors] == [("_RetryRequestChanged", RETRY_STOP)]
            committed = DurableAgentState.from_dict(provider.raw)
            assert committed.data.ingested_messages == {}
            assert committed.data.session is not None
            assert committed.data.session["service_session_id"] == "resp_parent"
            assert committed.data.completed_correlations["retry"]["outcome"] == "failed"


@pytest.mark.parametrize(
    "variant",
    [
        "same-schema",
        "schema-identity",
        "same-tool",
        "tool-config",
        "tool-choice",
        "messages",
        "compaction",
        "metadata",
        "opaque-first",
        "opaque-retry",
        "tokenizer-retry",
    ],
)
async def test_prepared_retry_boundary_with_real_sdk_and_independent_wire_control(
    variant: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # Distinct classes deliberately have identical schemas/names. A serializer or
    # type-name comparison cannot substitute for the actual response_format class.
    first_schema = create_model("Answer", answer=(int, ...))
    second_schema = create_model("Answer", answer=(int, ...))
    assert first_schema is not second_schema
    assert first_schema.model_json_schema() == second_schema.model_json_schema()
    tool_config: dict[str, Any] = {"type": "file_search", "vector_store_ids": ["vs_original"], "max_num_results": 1}
    opaque = FunctionTool(name="declaration", description=PRIVATE, input_model={"type": "object", "properties": {}})
    sleeps: list[float] = []

    async def backoff(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(entities.asyncio, "sleep", backoff)
    caplog.set_level(logging.WARNING)

    class Tokenizer:
        def count_tokens(self, text: str) -> int:
            return len(text)

    class PreparedOptions(ChatMiddleware):
        def __init__(self) -> None:
            self.calls = 0
            self.compactions = 0

        async def process(self, context: ChatContext, call_next: Callable[[], Awaitable[None]]) -> None:
            self.calls += 1
            assert context.options is not None
            options = cast("dict[str, Any]", context.options)
            if variant in {"same-schema", "schema-identity"}:
                options["response_format"] = (
                    second_schema if variant == "schema-identity" and self.calls > 1 else first_schema
                )
            elif variant in {"same-tool", "tool-config", "tool-choice"}:
                # The same mutable object is deliberately reused. A shallow
                # snapshot would compare the already-mutated first request equal.
                if variant == "tool-config" and self.calls > 1:
                    tool_config["max_num_results"] = 2
                options["tools"] = [tool_config]
                options["tool_choice"] = "none" if variant == "tool-choice" and self.calls > 1 else "auto"
            elif variant == "messages":
                # Replace, don't mutate outer run/session input (which has its own guard).
                context.messages = [Message("user", [f"{PRIVATE}-{self.calls}"], message_id="effective")]
            elif variant == "metadata":
                options["metadata"] = {"nested": {"private": PRIVATE, "attempt": self.calls}}
            elif variant == "opaque-first" or (variant == "opaque-retry" and self.calls > 1):
                options["tools"] = [opaque]
            elif variant == "tokenizer-retry" and self.calls > 1:
                context.kwargs["tokenizer"] = Tokenizer()
            elif variant == "compaction":

                async def compact(messages: list[Message]) -> bool:
                    self.compactions += 1
                    messages[:] = [Message("user", [f"{PRIVATE}-{self.compactions}"], message_id="effective")]
                    return True

                context.kwargs["compaction_strategy"] = compact
            await call_next()

    # The unwrapped real Core/SDK pipeline is an independent oracle for each
    # option's actual HTTP effect. The guarded run must stop BEFORE that second
    # request, not infer correctness from its own snapshot implementation.
    async def execute(*, guarded: bool) -> tuple[list[dict[str, Any]], PreparedOptions, Any, JsonStateProvider | None]:
        bodies: list[dict[str, Any]] = []
        middleware = PreparedOptions()
        tool_config["max_num_results"] = 1

        async def handle(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v1/responses"
            bodies.append(json.loads(request.content))
            if len(bodies) == 1:
                return httpx.Response(
                    400,
                    json={
                        "error": {
                            "message": "parent missing",
                            "type": "invalid_request_error",
                            "code": "previous_response_not_found",
                            "param": "previous_response_id",
                        }
                    },
                )
            return httpx.Response(200, json=sdk_response('{"answer":42}'))

        async with (
            httpx.AsyncClient(transport=httpx.MockTransport(handle)) as transport,
            AsyncOpenAI(
                api_key="offline-placeholder",
                base_url="https://offline.invalid/v1",
                http_client=transport,
                max_retries=0,
            ) as sdk,
        ):
            client = OpenAIChatClient(async_client=sdk, model="offline-model", middleware=[middleware])
            if not guarded:
                with pytest.raises(Exception) as refused:
                    await client.get_response(
                        [Message("user", ["question"], message_id="source")],
                        options={"store": True, "conversation_id": "resp_parent", "tool_choice": "auto"},
                    )
                assert "previous_response_not_found" in str(refused.value)
                await client.get_response(
                    [Message("user", ["question"], message_id="source")],
                    options={"store": True, "conversation_id": "resp_parent", "tool_choice": "auto"},
                )
                assert len(bodies) == 2
                return bodies, middleware, None, None

            agent = NonStreamingAgent(client=client, default_options={"store": True})
            configuration = deepcopy(client.function_invocation_configuration)
            seed = DurableAgentState()
            seed.data.session = AgentSession(
                session_id="@runtime@runtime-session", service_session_id="resp_parent"
            ).to_dict()
            provider = JsonStateProvider(seed.to_dict())
            request = RunRequest(
                message="question",
                correlation_id="prepared",
                options={"store": True},
                context_messages=[Message("user", ["question"], message_id="source").to_dict()],
                context_message_ids=["occ-source"],
            )
            response = await AgentEntity(agent, state_provider=provider).run(request)
            assert agent.client is client and client.client is sdk
            assert client.chat_middleware == [middleware]
            assert client.function_invocation_configuration == configuration
            cold = JsonStateProvider(provider.raw)
            count = len(bodies)
            duplicate = await AgentEntity(agent, state_provider=cold).run(request)
            assert duplicate.text == response.text
            assert len(bodies) == count and cold.attempted_writes == 0
            return bodies, middleware, response, provider

    control, _, _, _ = await execute(guarded=False)
    if variant in {"schema-identity", "same-schema", "same-tool", "opaque-first", "tokenizer-retry"}:
        assert control[0] == control[1]
    else:
        assert control[0] != control[1]
    if variant == "tool-config":
        assert [body["tools"][0]["max_num_results"] for body in control] == [1, 2]
    elif variant == "tool-choice":
        assert [body["tool_choice"] for body in control] == ["auto", "none"]
    elif variant in {"messages", "compaction"}:
        assert [body["input"][0]["content"][0]["text"] for body in control] == [f"{PRIVATE}-1", f"{PRIVATE}-2"]
    elif variant == "metadata":
        assert [body["metadata"]["nested"]["attempt"] for body in control] == [1, 2]
    caplog.clear()
    bodies, middleware, response, provider = await execute(guarded=True)
    assert provider is not None and provider.successful_writes == 1
    assert bodies[0] == control[0]
    if variant in {"same-schema", "same-tool"}:
        assert len(bodies) == middleware.calls == 2
        assert bodies[1] == bodies[0]
        assert response.additional_properties.get("durable_status") != "error"
        assert response.text == '{"answer":42}'
        assert set(DurableAgentState.from_dict(provider.raw).data.ingested_messages) == {"occ-source"}
    else:
        assert len(bodies) == 1
        assert middleware.calls == (1 if variant == "opaque-first" else 2)
        assert len(sleeps) == (0 if variant == "opaque-first" else 1)
        assert response.additional_properties["durable_status"] == "error"
        if variant != "opaque-first":
            errors = [c for m in response.messages for c in m.contents if c.type == "error"]
            assert [(c.error_code, c.message) for c in errors] == [("_RetryRequestChanged", RETRY_STOP)]
        committed = DurableAgentState.from_dict(provider.raw)
        assert committed.data.ingested_messages == {}
        assert committed.data.session is not None
        assert committed.data.session["service_session_id"] == "resp_parent"
        assert committed.data.completed_correlations["prepared"]["outcome"] == "failed"
        assert PRIVATE not in json.dumps(provider.raw)
        assert PRIVATE not in caplog.text
    if variant == "compaction":
        assert middleware.compactions == 2


@pytest.mark.parametrize("changed", [False, True])
async def test_retry_expectation_reset_preserves_only_comparable_evidence(changed: bool) -> None:
    accepted: list[Any] = []
    completed: list[Any] = []
    client = _RefusingClient()
    observer = DurableServiceClient(client, accepted.append, completed.append)
    with pytest.raises(_MissingParent):
        await observer.get_response([Message("user", ["original"])], options={"store": True})
    assert observer._expect_retry()
    observer.reset_observation()
    assert not observer.observed_request
    if changed:
        with pytest.raises(RuntimeError, match="prepared service request changed"):
            await observer.get_response([Message("user", ["changed"])], options={"store": True})
        assert len(client.calls) == 1 and accepted == completed == []
        assert not observer.observed_request
    else:
        assert (await observer.get_response([Message("user", ["original"])], options={"store": True})).text == "ok"
        assert len(client.calls) == 2 and len(accepted) == len(completed) == 1
        # A completed matched retry releases the first-leaf requirement. Normal
        # follow-up leaves can change, but entity progress blocks a whole-run retry.
        assert (await observer.get_response([Message("user", ["follow-up"])], options={"store": True})).text == "ok"
        assert len(client.calls) == 3 and len(accepted) == len(completed) == 2

    # The guard is local to the invocation, not installed on the caller's client.
    fresh = DurableServiceClient(RecordingChatClient(), accepted.append, completed.append)
    assert (await fresh.get_response([Message("user", ["changed"])])).text == "reply-1"


def test_retry_snapshot_captures_nested_defaults_and_default_schema_identity() -> None:
    class Answer(BaseModel):
        answer: int

    class OtherAnswer(BaseModel):
        answer: int

    defaults: dict[str, Any] = {"response_format": Answer, "metadata": {"values": [1]}}
    session = AgentSession()
    kwargs: dict[str, Any] = {"messages": [], "options": {}}
    before = entities._retry_snapshot(session, kwargs, set(), default_options=defaults)
    assert before is not None
    assert before == entities._retry_snapshot(session, kwargs, set(), default_options=defaults)
    defaults["metadata"]["values"].append(2)
    assert before != entities._retry_snapshot(session, kwargs, set(), default_options=defaults)
    defaults["metadata"]["values"] = [1]
    defaults["response_format"] = OtherAnswer
    assert before != entities._retry_snapshot(session, kwargs, set(), default_options=defaults)


@pytest.mark.parametrize("refusal_count", [2, 3], ids=["latest-refusal", "older-refusal"])
@pytest.mark.parametrize("link", ["cause", "context", "same-object"])
async def test_stale_retry_refusal_cannot_authorize_another_actual_dispatch(
    refusal_count: int, link: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Three retries normally permit four dispatches. Give the older-refusal
    # variant one extra slot so exhaustion cannot mask an erroneous fifth call.
    monkeypatch.setattr(entities, "_REJECTED_ID_RETRIES", refusal_count + 1)
    monkeypatch.setattr(entities, "_REJECTED_ID_BACKOFF_SECONDS", 0)
    refusals = [_MissingParent(f"fresh refusal {index}") for index in range(refusal_count)]
    stale = refusals[1]
    unrelated = RuntimeError("unrelated failure after fresh refusals")
    invoked: list[int] = []

    def dispatch() -> ChatResponse:
        index = len(invoked)
        invoked.append(index)
        if index < refusal_count:
            raise refusals[index]
        if index == refusal_count:
            if link == "same-object":
                raise stale
            if link == "cause":
                raise unrelated from stale
            try:
                raise stale
            except _MissingParent:
                raise unrelated  # noqa: B904 - deliberately reuse a prior implicit context
        return ChatResponse(messages=[Message("assistant", ["unexpected extra dispatch"])], conversation_id="S1")

    client = _ScriptedNonStreamingClient([dispatch] * (refusal_count + 2))
    provider = _provider()
    request = _request("first", "B")
    response = await _entity(provider, client).run(request)

    assert len(invoked) == len(client.received_messages) == refusal_count + 1
    expected = stale if link == "same-object" else unrelated
    _assert_failed(provider, response, type(expected).__name__, str(expected))
    assert _committed(provider).data.ingested_messages == {}
    assert all(options["conversation_id"] == "S0" for options in client.received_options)
    cold = JsonStateProvider(provider.raw, session_id="thread", entity_name="review")
    duplicate = await _entity(cold, client).run(request)
    assert duplicate.text == response.text
    assert len(invoked) == refusal_count + 1 and cold.attempted_writes == 0


@pytest.mark.parametrize("link", ["cause", "context"])
async def test_fresh_identical_structured_refusals_retry_despite_old_exception_links(
    link: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(entities, "_REJECTED_ID_RETRIES", 3)
    monkeypatch.setattr(entities, "_REJECTED_ID_BACKOFF_SECONDS", 0)
    refusals = [_MissingParent("same service refusal") for _ in range(3)]
    assert len({id(error) for error in refusals}) == 3
    invoked: list[int] = []

    def dispatch() -> ChatResponse:
        index = len(invoked)
        invoked.append(index)
        if index == 0:
            raise refusals[0]
        if index < 3:
            if link == "cause":
                raise refusals[index] from refusals[0]
            try:
                raise refusals[0]
            except _MissingParent:
                raise refusals[index]  # noqa: B904 - fresh top-level code overrides an old implicit context
        return ChatResponse(messages=[Message("assistant", ["fourth dispatch accepted"])], conversation_id="S1")

    client = _ScriptedNonStreamingClient([dispatch] * 4)
    provider = _provider()
    response = await _entity(provider, client).run(_request("first", "B"))

    assert len(invoked) == len(client.received_messages) == 4
    assert response.text == "fourth dispatch accepted"
    assert response.additional_properties.get("durable_status") != "error"
    assert provider.successful_writes == 1
    assert set(_committed(provider).data.ingested_messages) == {"occ-B"}
    assert all(options == client.received_options[0] for options in client.received_options)
