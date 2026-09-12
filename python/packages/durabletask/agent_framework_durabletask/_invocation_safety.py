# Copyright (c) Microsoft. All rights reserved.

"""Run-local safeguards at core's function invocation boundary."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from agent_framework import FunctionInvocationContext, FunctionMiddleware


@dataclass
class InvocationProgress:
    """Track observable progress that makes restarting a whole agent run unsafe."""

    stream_started: bool = False
    function_started: bool = False


class DurableToolGuard(FunctionMiddleware):
    """Prevent callable execution even when a wrapper delegates to an inner core loop.

    This uses core's public per-run middleware contract. Arbitrary custom agents or
    clients that execute tools outside that contract remain responsible for their own
    side effects; no portable wrapper can sandbox their implementation.
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
