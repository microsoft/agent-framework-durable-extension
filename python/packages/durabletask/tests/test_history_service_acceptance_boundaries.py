# Copyright (c) Microsoft. All rights reserved.

"""Service receipts must observe the leaf, not synthetic responses or successful audit saves."""

from __future__ import annotations

from collections.abc import AsyncIterable, Awaitable, Callable, Sequence
from copy import deepcopy
from typing import Any

import pytest
from agent_framework import (
    Agent,
    AgentResponse,
    AgentSession,
    ChatContext,
    ChatMiddleware,
    ChatResponse,
    ChatResponseUpdate,
    Content,
    ContextProvider,
    HistoryProvider,
    Message,
    ResponseStream,
    SessionContext,
)
from test_durable_history_provider import _ingestion_messages
from test_history_identity_acceptance import _assert_no_private_fields, _projection, _seed_history, _wire
from test_history_pipeline_revision import NonStreamingAgent, ToolChatClient
from test_revision_contract import JsonStateProvider

from agent_framework_durabletask import AgentEntity, DurableHistoryProvider
from agent_framework_durabletask._history_provider import current_durable_history_binding
from agent_framework_durabletask._message_identity import message_identity


class _PassChat(ChatMiddleware):
    def __init__(self) -> None:
        self.calls: list[ChatContext] = []

    async def process(self, context: ChatContext, call_next: Callable[[], Awaitable[None]]) -> None:
        self.calls.append(context)
        await call_next()


class _SyntheticChatMiddleware(ChatMiddleware):
    def __init__(self) -> None:
        self.enabled = True
        self.calls: list[ChatContext] = []

    async def process(self, context: ChatContext, call_next: Callable[[], Awaitable[None]]) -> None:
        self.calls.append(context)
        if not self.enabled:
            await call_next()
            return
        if context.stream:

            async def updates() -> AsyncIterable[ChatResponseUpdate]:
                yield ChatResponseUpdate(
                    role="assistant",
                    contents=[Content.from_text("synthetic answer")],
                    response_id="synthetic-response",
                    conversation_id="synthetic-thread",
                    finish_reason="stop",
                )

            context.result = ResponseStream(updates(), finalizer=ChatResponse.from_updates)
        else:
            context.result = ChatResponse(
                messages=[Message("assistant", ["synthetic answer"])],
                response_id="synthetic-response",
                conversation_id="synthetic-thread",
                finish_reason="stop",
            )
        # Deliberately do not call_next. Neither response shape came from the service.


class _MiddlewareProvider(ContextProvider):
    def __init__(self, middleware: ChatMiddleware) -> None:
        super().__init__("middleware-provider")
        self.middleware = middleware
        self.before_contexts: list[SessionContext] = []
        self.after_contexts: list[SessionContext] = []

    async def before_run(self, *, context: SessionContext, **kwargs: Any) -> None:
        self.before_contexts.append(context)
        # This API takes a source_id string, unlike extend_messages' provider-object form.
        context.extend_middleware(self.source_id, [self.middleware])

    async def after_run(self, *, context: SessionContext, **kwargs: Any) -> None:
        self.after_contexts.append(context)


class _RunProbe(ContextProvider):
    def __init__(self, *, fail_after: bool = False) -> None:
        super().__init__("service-boundary-probe")
        self.fail_after = fail_after
        self.contexts: list[SessionContext] = []
        self.inputs: list[list[Message]] = []
        self.responses: list[AgentResponse[Any]] = []
        self.accepted: list[set[tuple[str, str]]] = []

    async def before_run(self, *, context: SessionContext, **kwargs: Any) -> None:
        self.contexts.append(context)
        self.inputs.append(deepcopy(context.input_messages))

    async def after_run(self, *, context: SessionContext, **kwargs: Any) -> None:
        assert isinstance(context.response, AgentResponse)
        self.responses.append(context.response)
        binding = current_durable_history_binding()
        self.accepted.append(set(binding.accepted_inputs) if binding is not None else set())
        if self.fail_after:
            raise RuntimeError("probe failed after response")


class _StoreOnlyHistory(HistoryProvider):
    def __init__(self, client: ToolChatClient, *, fail_save: bool) -> None:
        super().__init__("store-only-audit", load_messages=False)
        self.client = client
        self.fail_save = fail_save
        self.after_contexts: list[SessionContext] = []
        self.leaf_counts: list[int] = []
        self.attempted: list[list[Message]] = []
        self.saved: list[list[Message]] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        pytest.fail("a store-only audit must not load history")

    async def after_run(self, *, context: SessionContext, **kwargs: Any) -> None:
        self.after_contexts.append(context)
        self.leaf_counts.append(len(self.client.received_messages))
        assert isinstance(context.response, AgentResponse)
        assert context.response.text == f"answer-{self.leaf_counts[-1]}"
        await super().after_run(context=context, **kwargs)

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        assert session_id is not None
        assert len(self.client.received_messages) == self.leaf_counts[-1] > 0
        self.attempted.append(deepcopy(list(messages)))
        if self.fail_save:
            raise OSError("audit save failed after leaf completion")
        self.saved.append(deepcopy(list(messages)))


class _Registration:
    """Keep identity checks separate from core's expected per-run cache updates."""

    def __init__(self, agent: Agent, client: ToolChatClient) -> None:
        self.agent = agent
        self.client = client
        self.providers = agent.context_providers
        self.provider_items = tuple(agent.context_providers)
        self.middleware = agent.middleware
        self.middleware_items = tuple(agent.middleware or [])
        self.defaults = agent.default_options
        self.default_values = deepcopy(agent.default_options)
        self.chat_middleware = client.chat_middleware
        self.chat_items = tuple(client.chat_middleware)
        self.configuration = client.function_invocation_configuration
        self.configuration_values = deepcopy(client.function_invocation_configuration)
        self.get_response = client.get_response

    def assert_unchanged(self, entity: AgentEntity) -> None:
        assert entity.agent is self.agent
        assert self.agent.client is self.client
        assert self.agent.context_providers is self.providers
        assert tuple(self.providers) == self.provider_items
        assert self.agent.middleware is self.middleware
        assert tuple(self.middleware or []) == self.middleware_items
        assert self.agent.default_options is self.defaults and self.defaults == self.default_values
        assert self.client.chat_middleware is self.chat_middleware
        assert tuple(self.chat_middleware) == self.chat_items
        assert self.client.function_invocation_configuration is self.configuration
        assert self.configuration == self.configuration_values
        assert self.client.get_response == self.get_response
        assert current_durable_history_binding() is None


def _messages_json(messages: Sequence[Message]) -> list[dict[str, Any]]:
    return _wire([message.to_dict() for message in messages])


async def _assert_cold_next_turn(raw: dict[str, Any], request: dict[str, Any], *, accepted: bool, stream: bool) -> None:
    cold = JsonStateProvider(_wire(raw))
    probe = _RunProbe()
    client = ToolChatClient(tool_calls=False)
    ordinary = _PassChat()
    history = DurableHistoryProvider(prune_excluded=False)
    agent = (Agent if stream else NonStreamingAgent)(
        client=client, middleware=[ordinary], context_providers=[history, probe]
    )
    entity = AgentEntity(agent, state_provider=cold)
    registration = _Registration(agent, client)
    repeated = [Message.from_dict(item) for item in request["contextMessages"]]
    new_input = Message("user", ["genuinely new caller turn"], message_id="new-caller-input")
    followup = {
        **_projection("cold-next", [*repeated, new_input], [*request["contextMessageIds"], "new-caller-occ"]),
        "options": {"store": True},
    }
    before = deepcopy(followup)

    response = await entity.run(followup)

    assert response.text == "answer-1" and response.additional_properties.get("durable_status") != "error"
    assert len(client.received_messages) == len(probe.inputs) == len(probe.responses) == len(ordinary.calls) == 1
    expected = [new_input] if accepted else [*repeated, new_input]
    assert _messages_json(probe.inputs[0]) == _messages_json(expected), (
        "cold admission must suppress only inputs actually received by the service"
    )
    assert _messages_json(client.received_messages[0]) == _messages_json(expected), (
        "the real cold leaf must receive the unaccepted repeats and the new caller input"
    )
    assert ordinary.calls[0].client is client and ordinary.calls[0].stream is stream
    assert client.received_options[0]["store"] is True
    assert cold.writes == 1 and followup == before
    assert cold.raw["data"]["conversationHistory"] == raw["data"]["conversationHistory"]
    registration.assert_unchanged(entity)
    _assert_no_private_fields(cold.raw)


async def _assert_direct_caller_turn(agent: Agent, client: ToolChatClient, *, stream: bool) -> None:
    """Reuse the caller's registration outside a durable operation, without a stale observer."""
    assert current_durable_history_binding() is None
    calls = len(client.received_messages)
    message = Message("user", ["ordinary later core call"], message_id="direct-caller-input")
    session = AgentSession()
    if stream:
        response_stream = agent.run([message], session=session, stream=True, options={"store": True})
        response = await response_stream.get_final_response()
    else:
        response = await agent.run([message], session=session, options={"store": True})
    assert response.text == f"answer-{calls + 1}"
    assert len(client.received_messages) == calls + 1
    assert _messages_json(client.received_messages[-1]) == _messages_json([message])
    assert current_durable_history_binding() is None


@pytest.mark.parametrize("mode", ["provider-synthetic", "agent-synthetic-control", "pass-through-control"])
@pytest.mark.parametrize("stream", [False, True], ids=["non-streaming", "streaming"])
async def test_synthetic_service_response_does_not_accept_inputs_after_later_provider_failure(
    mode: str, stream: bool
) -> None:
    client = ToolChatClient(tool_calls=False)
    ordinary, provider_pass = _PassChat(), _PassChat()
    synthetic = _SyntheticChatMiddleware()
    injected = _MiddlewareProvider(synthetic if mode == "provider-synthetic" else provider_pass)
    probe = _RunProbe(fail_after=True)
    history = DurableHistoryProvider(store_inputs=False, store_outputs=False, prune_excluded=False)
    caller_middleware = [ordinary, synthetic] if mode == "agent-synthetic-control" else [ordinary]
    agent = (Agent if stream else NonStreamingAgent)(
        client=client, middleware=caller_middleware, context_providers=[probe, history, injected]
    )
    raw = _seed_history()
    raw["data"]["session"]["service_session_id"] = "prior-service"
    provider = JsonStateProvider(raw)
    entity = AgentEntity(agent, state_provider=provider)
    registration = _Registration(agent, client)
    message = Message("user", ["repeated delivery"], message_id="shared")
    request = {
        **_projection("synthetic-failed", [message, deepcopy(message)], ["occ-0", "occ-1"]),
        "options": {"store": True},
    }
    before = deepcopy(request)

    response = await entity.run(request)

    real_leaf = mode == "pass-through-control"
    assert response.additional_properties["durable_status"] == "error"
    assert "probe failed after response" in response.text
    assert len(client.received_messages) == int(real_leaf)
    assert len(injected.before_contexts) == len(injected.after_contexts) == len(probe.responses) == 1
    assert probe.responses[0].text == ("answer-1" if real_leaf else "synthetic answer")
    assert len(ordinary.calls) == 1 and ordinary.calls[0].client is client
    assert ordinary.calls[0].stream is stream
    assert len(synthetic.calls) == int(not real_leaf)
    assert len(provider_pass.calls) == int(real_leaf)
    assert _messages_json(probe.inputs[0]) == before["contextMessages"]
    if real_leaf:
        assert _messages_json(client.received_messages[0]) == before["contextMessages"]
        assert client.received_options[0]["store"] is True
    assert request == before
    assert caller_middleware == ([ordinary, synthetic] if mode == "agent-synthetic-control" else [ordinary])
    assert provider.writes == 1
    assert provider.raw["data"]["conversationHistory"] == raw["data"]["conversationHistory"]
    assert provider.raw["data"]["completionReceipts"]["synthetic-failed"]["outcome"] == "failed"
    registration.assert_unchanged(entity)
    _assert_no_private_fields(provider.raw)

    # Assert actual cold delivery before the bookkeeping check, so the repro shows lost input.
    await _assert_cold_next_turn(provider.raw, request, accepted=real_leaf, stream=stream)
    receipts = {occurrence: [message_identity(message)] for occurrence in request["contextMessageIds"]}
    assert _ingestion_messages(provider.raw) == {
        **_ingestion_messages(raw),
        **(receipts if real_leaf else {}),
    }
    assert probe.accepted == [{(key, message_identity(message)) for key in receipts} if real_leaf else set()]

    # Removing the injected behavior must not leave an observer or duplicate ordinary middleware behind.
    synthetic.enabled = False
    probe.fail_after = False
    durable_snapshot = deepcopy(provider.raw)
    await _assert_direct_caller_turn(agent, client, stream=stream)
    assert len(ordinary.calls) == 2 and all(context.client is client for context in ordinary.calls)
    assert len(injected.before_contexts) == len(injected.after_contexts) == len(probe.responses) == 2
    assert len(synthetic.calls) == (0 if real_leaf else 2)
    assert len(provider_pass.calls) == (0 if mode == "provider-synthetic" else 1 + int(real_leaf))
    assert provider.raw == durable_snapshot and probe.accepted[-1] == set()
    registration.assert_unchanged(entity)


@pytest.mark.parametrize("fail_save", [True, False], ids=["save-fails", "save-succeeds-control"])
@pytest.mark.parametrize("stream", [False, True], ids=["non-streaming", "streaming"])
async def test_completed_service_call_acceptance_survives_real_per_call_history_save_failure(
    fail_save: bool, stream: bool
) -> None:
    client = ToolChatClient(tool_calls=False)
    ordinary, provider_pass = _PassChat(), _PassChat()
    injected = _MiddlewareProvider(provider_pass)
    sink = _StoreOnlyHistory(client, fail_save=fail_save)
    probe = _RunProbe()
    history = DurableHistoryProvider(store_inputs=False, store_outputs=False, prune_excluded=False)
    caller_middleware = [ordinary]
    agent = (Agent if stream else NonStreamingAgent)(
        client=client,
        middleware=caller_middleware,
        context_providers=[probe, history, injected, sink],
        require_per_service_call_history_persistence=True,
    )
    raw = _seed_history()
    raw["data"]["session"]["service_session_id"] = "prior-service"
    provider = JsonStateProvider(raw)
    entity = AgentEntity(agent, state_provider=provider)
    registration = _Registration(agent, client)
    message = Message("user", ["already received by service"], message_id="shared")
    request = {
        **_projection("per-call", [message, deepcopy(message)], ["occ-0", "occ-1"]),
        "options": {"store": True},
    }
    before = deepcopy(request)

    response = await entity.run(request)

    assert len(client.received_messages) == 1
    assert _messages_json(client.received_messages[0]) == before["contextMessages"]
    assert client.received_options[0]["store"] is True
    assert sink.load_messages is False and sink.client is client
    assert sink.leaf_counts == [1] and len(sink.after_contexts) == len(sink.attempted) == 1
    # Core creates a distinct per-service-call context. This is not a manually invoked or run-end save.
    assert sink.after_contexts[0] is not probe.contexts[0]
    assert _messages_json(sink.after_contexts[0].input_messages) == before["contextMessages"]
    assert [item.role for item in sink.attempted[0]] == ["user", "user", "assistant"]
    assert [item.text for item in sink.attempted[0]] == [message.text, message.text, "answer-1"]
    assert len(sink.saved) == int(not fail_save)
    assert len(injected.before_contexts) == len(ordinary.calls) == len(provider_pass.calls) == 1
    assert ordinary.calls[0].client is provider_pass.calls[0].client is client
    assert ordinary.calls[0].stream is provider_pass.calls[0].stream is stream
    assert len(injected.after_contexts) == len(probe.responses) == int(not fail_save)
    if fail_save:
        assert response.additional_properties["durable_status"] == "error"
        assert "audit save failed after leaf completion" in response.text
    else:
        assert response.text == "answer-1" and response.additional_properties.get("durable_status") != "error"
    assert provider.writes == 1 and request == before and caller_middleware == [ordinary]
    assert provider.raw["data"]["conversationHistory"] == raw["data"]["conversationHistory"]
    outcome = "failed" if fail_save else "succeeded"
    assert provider.raw["data"]["completionReceipts"]["per-call"]["outcome"] == outcome
    registration.assert_unchanged(entity)
    _assert_no_private_fields(provider.raw)

    # The failed core hook may prevent continuation advancement. Acceptance must still survive.
    await _assert_cold_next_turn(provider.raw, request, accepted=True, stream=stream)
    assert _ingestion_messages(provider.raw) == {
        **_ingestion_messages(raw),
        **{occurrence: [message_identity(message)] for occurrence in request["contextMessageIds"]},
    }

    sink.fail_save = False
    durable_snapshot = deepcopy(provider.raw)
    await _assert_direct_caller_turn(agent, client, stream=stream)
    assert sink.leaf_counts == [1, 2] and len(sink.attempted) == 2
    assert len(sink.saved) == 2 - int(fail_save)
    assert [item.text for item in sink.saved[-1]] == ["ordinary later core call", "answer-2"]
    assert len(ordinary.calls) == len(provider_pass.calls) == len(injected.before_contexts) == 2
    assert all(context.client is client for context in [*ordinary.calls, *provider_pass.calls])
    assert len(injected.after_contexts) == len(probe.responses) == 2 - int(fail_save)
    assert provider.raw == durable_snapshot and probe.accepted[-1] == set()
    registration.assert_unchanged(entity)
