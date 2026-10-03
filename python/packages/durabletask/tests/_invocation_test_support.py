# Copyright (c) Microsoft. All rights reserved.

"""Shared clients and tools for run-local invocation safety tests."""

from __future__ import annotations

from collections.abc import AsyncIterable, Awaitable, Mapping, Sequence
from copy import deepcopy
from typing import Any

from agent_framework import (
    BaseChatClient,
    ChatMiddlewareLayer,
    ChatResponse,
    ChatResponseUpdate,
    Content,
    FunctionInvocationLayer,
    Message,
    ResponseStream,
    tool,
)


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


@tool(name="lookup", approval_mode="never_require")
def lookup(key: str) -> str:
    return f"value:{key}"
