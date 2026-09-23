# Copyright (c) Microsoft. All rights reserved.

"""Shared client and entity construction for bounded retry tests."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any

from _execution_test_support import JsonStateProvider, NonStreamingAgent
from _service_commit_test_support import _MissingParent, _SessionProbe
from agent_framework import BaseChatClient, ChatResponse, Message

from agent_framework_durabletask import AgentEntity


class _RefusingClient(BaseChatClient):
    STORES_BY_DEFAULT = True

    def __init__(self, action: Callable[[Sequence[Message], Mapping[str, Any]], None] | None = None) -> None:
        super().__init__()
        self.action = action
        self.calls: list[dict[str, Any]] = []

    def _inner_get_response(
        self, *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
    ) -> Awaitable[ChatResponse]:
        assert not stream
        self.calls.append({"messages": deepcopy(list(messages)), "options": dict(options)})

        async def respond() -> ChatResponse:
            if len(self.calls) == 1:
                if self.action is not None:
                    self.action(messages, options)
                raise _MissingParent("first observed refusal")
            text = '{"answer": 42}' if options.get("response_format") is not None else "ok"
            return ChatResponse(messages=[Message("assistant", [text])], conversation_id="S1")

        return respond()


def _entity(
    provider: JsonStateProvider, client: Any, *, probe: _SessionProbe | None = None, middleware: Any = None
) -> AgentEntity:
    return AgentEntity(
        NonStreamingAgent(
            client=client,
            name="review",
            default_options={"store": True},
            context_providers=[probe] if probe is not None else [],
            middleware=middleware,
        ),
        state_provider=provider,
    )
