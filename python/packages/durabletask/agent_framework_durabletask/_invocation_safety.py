# Copyright (c) Microsoft. All rights reserved.

"""Run-local safeguards at core's function invocation boundary."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, cast

from agent_framework import (
    ChatContext,
    ChatMiddleware,
    ChatResponse,
    FunctionInvocationContext,
    FunctionMiddleware,
    Message,
    ResponseStream,
)


@dataclass
class InvocationProgress:
    """Track observable progress that makes restarting a whole agent run unsafe."""

    stream_started: bool = False
    function_started: bool = False


class DurableToolGuard(FunctionMiddleware):
    """Prevent callable execution even when a wrapper delegates to an inner core loop.

    This uses core's public per-run middleware contract. Arbitrary custom agents or
    clients that execute tools outside that contract remain responsible for their own
    side effects. No portable wrapper can sandbox their implementation.
    """

    def __init__(self, progress: InvocationProgress, *, enabled: bool) -> None:
        self.progress = progress
        self.enabled = enabled

    async def process(self, context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        if not self.enabled:
            context.result = "Tool execution is disabled for this invocation."
            return
        self.progress.function_started = True
        await call_next()


class DurableServiceAcceptance(ChatMiddleware):
    """Observe a completed service response without treating dispatch as acceptance."""

    def __init__(self, accept: Callable[[Sequence[Message]], None]) -> None:
        self._accept = accept

    async def process(self, context: ChatContext, call_next: Callable[[], Awaitable[None]]) -> None:
        inputs = deepcopy(list(context.messages))
        await call_next()

        def completed(response: ChatResponse) -> ChatResponse:
            # A completed provider response affirms receipt of these inputs, even
            # when the invocation outcome is failed. Interrupted streams do not.
            self._accept(inputs)
            return response

        if isinstance(context.result, ResponseStream):
            context.result.with_result_hook(completed)
        elif isinstance(context.result, ChatResponse):
            completed(context.result)


class DurableServiceClient:
    """Borrow a client and place acceptance observation after the complete middleware list."""

    def __init__(self, client: Any, accept: Callable[[Sequence[Message]], None]) -> None:
        self.__wrapped__ = client
        self._observer = DurableServiceAcceptance(accept)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.__wrapped__, name)

    def get_response(self, messages: Sequence[Message], **kwargs: Any) -> Any:
        # Agent and provider middleware, including per-call persistence, are now
        # assembled. An earlier short circuit never reaches the observer, while
        # a leaf completion is observed before a later persistence hook can fail.
        client_kwargs = dict(kwargs.get("client_kwargs") or {})
        existing = client_kwargs.get("middleware")
        if isinstance(existing, (list, tuple)):
            middleware = list(cast("Sequence[Any]", existing))
        else:
            middleware = [existing] if existing else []
        client_kwargs["middleware"] = [*middleware, self._observer]
        return self.__wrapped__.get_response(messages=messages, **{**kwargs, "client_kwargs": client_kwargs})
