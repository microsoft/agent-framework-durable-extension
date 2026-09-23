# Copyright (c) Microsoft. All rights reserved.

"""Focused tests for run-local invocation safety helpers."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from copy import deepcopy
from typing import Any, cast

import pytest
from _invocation_test_support import ToolChatClient, lookup
from agent_framework import (
    Agent,
    ChatMiddleware,
    ChatResponse,
    ChatResponseUpdate,
    Content,
    ContextProvider,
    Message,
    ResponseStream,
    SessionContext,
)

from agent_framework_durabletask._invocation_safety import DurableServiceClient, DurableToolGuard, InvocationProgress


class _ObservedAgent(Agent):
    def __init__(self, *, client: Any, streaming: bool = True, **kwargs: Any) -> None:
        super().__init__(client=client, **kwargs)
        self.streaming = streaming
        self.run_modes: list[bool] = []
        self.run_client_kwargs: list[dict[str, object]] = []

    def run(self, *args: Any, **kwargs: Any) -> Any:
        if kwargs.get("stream") and not self.streaming:
            raise TypeError("stream is not supported")
        self.run_modes.append(bool(kwargs.get("stream")))
        self.run_client_kwargs.append(dict(kwargs.get("client_kwargs") or {}))
        return super().run(*args, **kwargs)


class _DelegatingClient:
    """A non-invocation wrapper whose configuration belongs to its inner client."""

    def __init__(self, inner: ToolChatClient) -> None:
        self.inner = inner
        self.forwarded: list[dict[str, object]] = []
        self.inner_configurations: list[dict[str, object]] = []

    def __getattr__(self, name: str) -> object:
        if name in {"function_invocation_configuration", "additional_properties"}:
            return getattr(self.inner, name)
        raise AttributeError(name)

    def get_response(
        self, messages: Sequence[Message], *, stream: bool = False, **kwargs: Any
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        self.forwarded.append(dict(kwargs.get("client_kwargs") or {}))
        self.inner_configurations.append(dict(self.inner.function_invocation_configuration))
        return cast(Callable[..., Any], self.inner.get_response)(messages=messages, stream=stream, **kwargs)


class ProviderTools(ContextProvider):
    async def before_run(self, *, context: SessionContext, **kwargs: Any) -> None:
        context.tools.append(lookup)


@pytest.mark.parametrize("stream", [False, True], ids=["non-streaming", "streaming"])
@pytest.mark.parametrize("enabled", [False, True], ids=["disabled", "enabled"])
async def test_durable_tool_guard_reaches_delegated_core_invocation_without_mutating_configuration(
    stream: bool, enabled: bool
) -> None:
    inner = ToolChatClient()
    wrapper = _DelegatingClient(inner)
    configuration = inner.function_invocation_configuration
    original_configuration = deepcopy(configuration)
    progress = InvocationProgress()
    guard = DurableToolGuard(progress, enabled=enabled)
    agent = _ObservedAgent(client=wrapper, streaming=stream, context_providers=[ProviderTools("provider-tools")])

    result = agent.run(
        "use lookup",
        session=agent.create_session(session_id="guard-session"),
        stream=stream,
        client_kwargs={"middleware": [guard]},
    )
    response = await result.get_final_response() if stream else await result

    assert response.text == "answer-2"
    assert len(inner.received_messages) == 2
    assert agent.run_modes == [stream]
    assert len(wrapper.forwarded) == 1
    assert guard in cast("list[Any]", wrapper.forwarded[0]["middleware"])
    assert progress.function_started is enabled
    results = [
        content
        for message in inner.received_messages[1]
        for content in message.contents
        if content.type == "function_result"
    ]
    assert len(results) == 1
    assert results[0].call_id == "call-1"
    assert results[0].result == ("value:durable" if enabled else "Tool execution is disabled for this invocation.")
    assert inner.function_invocation_configuration is configuration
    assert configuration == original_configuration
    assert wrapper.function_invocation_configuration is configuration
    assert wrapper.inner_configurations == [original_configuration]


@pytest.mark.parametrize("stream", [False, True], ids=["non-streaming", "streaming"])
async def test_service_acceptance_observes_only_completed_provider_results(stream: bool) -> None:
    accepted: list[list[Message]] = []
    inner = ToolChatClient()
    client = DurableServiceClient(
        inner,
        lambda messages: accepted.append(deepcopy(list(messages))),
    )
    agent = Agent(client=client, context_providers=[ProviderTools("provider-tools")])

    session = agent.create_session(session_id="accept")
    if stream:
        response = await agent.run("question", session=session, stream=True).get_final_response()
    else:
        response = await agent.run("question", session=session)

    assert response.text == "answer-2"
    assert len(accepted) == len(inner.received_messages) == 2
    assert [[m.to_dict() for m in batch] for batch in accepted] == [
        [m.to_dict() for m in batch] for batch in inner.received_messages
    ]
    assert [message.text for message in accepted[0]] == ["question"]
    assert any(content.type == "function_result" for message in accepted[1] for content in message.contents)


async def test_service_acceptance_does_not_observe_a_failed_first_service_call() -> None:
    accepted: list[list[str]] = []
    client = DurableServiceClient(
        ToolChatClient(fail_on_call=1),
        lambda messages: accepted.append([message.text for message in messages]),
    )
    agent = Agent(client=client)

    with pytest.raises(RuntimeError, match="service parent is not visible"):
        await agent.run(
            "question",
            session=agent.create_session(session_id="accept-failure"),
            stream=True,
        ).get_final_response()

    assert accepted == []


async def test_service_acceptance_retains_completed_leaf_before_later_service_failure() -> None:
    accepted: list[list[str]] = []
    client = DurableServiceClient(
        ToolChatClient(fail_on_call=2),
        lambda messages: accepted.append([message.text for message in messages]),
    )
    agent = Agent(client=client, context_providers=[ProviderTools("provider-tools")])

    with pytest.raises(RuntimeError, match="service parent is not visible"):
        await agent.run(
            "question",
            session=agent.create_session(session_id="accept-partial"),
            stream=True,
        ).get_final_response()

    assert accepted == [["question"]]


async def test_service_acceptance_captures_detached_inputs_before_postawait_mutation() -> None:
    accepted: list[list[dict[str, Any]]] = []
    observed_after_await: list[list[dict[str, Any]]] = []

    inner = ToolChatClient()
    client = DurableServiceClient(
        inner,
        lambda messages: accepted.append([message.to_dict() for message in messages]),
    )

    class MutatingProvider(ContextProvider):
        async def before_run(self, *, context: SessionContext, **kwargs: Any) -> None:
            context.extend_messages(
                self,
                [
                    Message(
                        "user",
                        [Content.from_text("context")],
                        additional_properties={"nested": {"items": [{"kind": "original"}]}, "raw": {"x": 1}},
                    )
                ],
            )

    class PostAwaitMutator(ChatMiddleware):
        async def process(
            self,
            context: Any,
            call_next: Callable[[], Awaitable[None]],
        ) -> None:
            await call_next()
            observed_after_await.append([message.to_dict() for message in context.messages])
            context.messages[0].contents[0].text = "context-mutated"
            context.messages[0].additional_properties["nested"]["items"][0]["kind"] = "mutated"
            context.messages[1].contents[0].text = "question-mutated"
            context.messages[1].additional_properties["native"] = {"changed": True}

    agent = _ObservedAgent(
        client=client,
        streaming=False,
        context_providers=[MutatingProvider("mutating-provider")],
        middleware=[PostAwaitMutator()],
    )

    response = await agent.run("question", session=agent.create_session(session_id="accept-detached"))

    assert response.messages[0].contents[0].type == "function_call"
    assert len(accepted) == len(inner.received_messages) == 1
    assert len(observed_after_await) == 1
    assert [message["contents"][0]["text"] for message in observed_after_await[0]] == ["context", "question"]
    assert [message["contents"][0]["text"] for message in accepted[0]] == ["context", "question"]
    assert accepted[0][1]["additional_properties"] == {}
    assert accepted[0][0]["additional_properties"] == {
        "nested": {"items": [{"kind": "original"}]},
        "raw": {"x": 1},
        "_attribution": {"source_id": "mutating-provider", "source_type": "MutatingProvider"},
    }
