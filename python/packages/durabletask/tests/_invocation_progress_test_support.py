# Copyright (c) Microsoft. All rights reserved.

"""Scripted non-streaming client for runtime invocation progress tests."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any

from agent_framework import BaseChatClient, ChatResponse, Message


class _ScriptedNonStreamingClient(BaseChatClient):
    STORES_BY_DEFAULT = True

    def __init__(self, outcomes: Sequence[Callable[[], ChatResponse] | ChatResponse]) -> None:
        super().__init__()
        self._outcomes = list(outcomes)
        self.received_messages: list[list[Message]] = []
        self.received_options: list[dict[str, Any]] = []

    def _inner_get_response(
        self,
        *,
        messages: Sequence[Message],
        stream: bool,
        options: Mapping[str, Any],
        **kwargs: Any,
    ) -> Awaitable[ChatResponse]:
        del kwargs
        assert stream is False
        self.received_messages.append(deepcopy(list(messages)))
        self.received_options.append(dict(options))
        if not self._outcomes:
            raise AssertionError("missing scripted non-streaming outcome")
        outcome = self._outcomes.pop(0)

        async def get() -> ChatResponse:
            if callable(outcome):
                return outcome()
            return outcome

        return get()
