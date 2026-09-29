# Copyright (c) Microsoft. All rights reserved.

"""Shared full-state and real Core pipeline doubles for history tests."""

from __future__ import annotations

from collections.abc import AsyncIterable, Awaitable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from agent_framework import (
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

from agent_framework_durabletask._history_provider import (
    DurableHistoryBinding,
    DurableHistoryProvider,
    bind_durable_history,
    unbind_durable_history,
)
from agent_framework_durabletask._shared_agent_state import (
    DurableAgentState,
    DurableAgentStateEntry,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
)

OLD = datetime(2026, 1, 1, tzinfo=timezone.utc)


class _CanonicalStateProvider:
    """State-provider double backed by canonical durable state."""

    def __init__(self, history: list[DurableAgentStateEntry] | None = None) -> None:
        self.persist_count = 0
        self.state = DurableAgentState()
        self.state.data.conversation_history = list(history or [])

    def persist(self) -> None:
        self.persist_count += 1

    def clone(self) -> _CanonicalStateProvider:
        clone = _CanonicalStateProvider()
        clone.state = DurableAgentState.from_dict(self.state.to_dict())
        return clone

    def _history_rows(self) -> list[dict[str, Any]]:
        return [entry.to_dict() for entry in self.state.data.conversation_history]


class ToolChatClient(FunctionInvocationLayer, ChatMiddlewareLayer, BaseChatClient):
    """Exercise real core middleware and function invocation."""

    STORES_BY_DEFAULT = False

    def __init__(
        self,
        *,
        tool_calls: bool = True,
        response_message_id: str | None = None,
        fail: bool = False,
        fail_on_call: int | None = None,
        events: list[str] | None = None,
    ) -> None:
        super().__init__(middleware=[])
        self.tool_calls = tool_calls
        self.response_message_id = response_message_id
        self.fail = fail
        self.fail_on_call = fail_on_call
        self.events = events if events is not None else []
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
        self.events.append(f"model-{call}")
        if self.fail or call == self.fail_on_call:
            raise RuntimeError("model failed before history persistence")
        calls_tool = self.tool_calls and call == 1
        contents = (
            [Content.from_function_call(call_id="call-1", name="lookup", arguments='{"key":"durable"}')]
            if calls_tool
            else [Content.from_text(f"answer-{call}")]
        )
        response = ChatResponse(
            messages=[
                Message(
                    "assistant",
                    contents,
                    message_id=self.response_message_id,
                    additional_properties={"model_metadata": {"tags": ["original"]}},
                )
            ],
            response_id=f"response-{call}",
            conversation_id="service-thread" if options.get("store") else None,
            finish_reason="tool_calls" if calls_tool else "stop",
        )
        if stream:

            async def updates() -> AsyncIterable[ChatResponseUpdate]:
                for message in response.messages:
                    yield ChatResponseUpdate(
                        role="assistant",
                        contents=message.contents,
                        message_id=message.message_id,
                        additional_properties=deepcopy(message.additional_properties),
                        response_id=response.response_id,
                        conversation_id=response.conversation_id,
                        finish_reason=response.finish_reason,
                    )

            return ResponseStream(updates(), finalizer=ChatResponse.from_updates)

        async def get() -> ChatResponse:
            return response

        return get()


class CountingHistory(DurableHistoryProvider):
    def __init__(self, events: list[str], **kwargs: Any) -> None:
        super().__init__(skip_excluded=False, **kwargs)
        self.events = events
        self.before_calls = 0
        self.after_calls = 0

    async def before_run(self, **kwargs: Any) -> None:
        self.before_calls += 1
        self.events.append("history-before")
        await super().before_run(**kwargs)

    async def after_run(self, **kwargs: Any) -> None:
        self.after_calls += 1
        self.events.append("history-after")
        await super().after_run(**kwargs)


class AddContext(ContextProvider):
    async def before_run(self, *, context: SessionContext, **kwargs: Any) -> None:
        context.extend_messages(self, [Message("user", [f"context-{self.source_id}"])])


@contextmanager
def _bound(
    provider: _CanonicalStateProvider,
    correlation_id: str | None = "current",
    *,
    service_owns_history: bool = False,
) -> Iterator[DurableHistoryBinding]:
    binding = DurableHistoryBinding(provider, correlation_id, service_owns_history)
    token = bind_durable_history(binding)
    try:
        yield binding
    finally:
        unbind_durable_history(token)


def _stored(
    text: str,
    *,
    role: str = "user",
    message_id: str | None = None,
    excluded: bool = False,
) -> DurableAgentStateMessage:
    stored = DurableAgentStateMessage.from_chat_message(Message(role, [text], message_id=message_id))
    if excluded:
        stored.extension_data = {"_excluded": True}
    return stored


def _request(correlation_id: str, *messages: DurableAgentStateMessage) -> DurableAgentStateRequest:
    return DurableAgentStateRequest(correlation_id, OLD, list(messages))


@tool(name="lookup", approval_mode="never_require")
def lookup(key: str) -> str:
    return f"value:{key}"
