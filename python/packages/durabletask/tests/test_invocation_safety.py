# Copyright (c) Microsoft. All rights reserved.

"""Focused tests for run-local invocation safety helpers."""

from __future__ import annotations

from collections.abc import AsyncIterable, Awaitable, Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any, cast

import pytest
from agent_framework import (
    Agent,
    BaseChatClient,
    ChatMiddlewareLayer,
    ChatResponse,
    ChatResponseUpdate,
    Content,
    ContextProvider,
    FunctionInvocationLayer,
    Message,
    ResponseStream,
    SessionContext,
    tool,
)

from agent_framework_durabletask._invocation_safety import DurableServiceClient, DurableToolGuard, InvocationProgress


class ToolChatClient(FunctionInvocationLayer, ChatMiddlewareLayer, BaseChatClient):
    """Exercise real core middleware and tool invocation through a delegated wrapper."""

    def __init__(self, *, fail_on_call: int | None = None) -> None:
        super().__init__(middleware=[])
        self.fail_on_call = fail_on_call
        self.received_messages: list[list[Message]] = []
        self.received_options: list[dict[str, Any]] = []

    def _inner_get_response(
        self,
        *,
        messages: Sequence[Message],
        stream: bool,
        options: Mapping[str, Any],
        **kwargs: Any,
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        self.received_messages.append(deepcopy(list(messages)))
        self.received_options.append(dict(options))
        call = len(self.received_messages)
        if call == self.fail_on_call:
            raise RuntimeError("service parent is not visible")
        calls_tool = call == 1
        contents = (
            [Content.from_function_call(call_id="call-1", name="lookup", arguments='{"key":"durable"}')]
            if calls_tool
            else [Content.from_text(f"answer-{call}")]
        )
        response = ChatResponse(
            messages=[Message("assistant", contents, additional_properties={"model_metadata": {"tags": ["keep"]}})],
            response_id=f"response-{call}",
            finish_reason="tool_calls" if calls_tool else "stop",
        )
        if stream:

            async def updates() -> AsyncIterable[ChatResponseUpdate]:
                yield ChatResponseUpdate(
                    role="assistant",
                    contents=response.messages[0].contents,
                    additional_properties=deepcopy(response.messages[0].additional_properties),
                    response_id=response.response_id,
                    finish_reason=response.finish_reason,
                )

            return ResponseStream(updates(), finalizer=ChatResponse.from_updates)

        async def get() -> ChatResponse:
            return response

        return get()


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


@tool(name="lookup", approval_mode="never_require")
def lookup(key: str) -> str:
    return f"value:{key}"


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
