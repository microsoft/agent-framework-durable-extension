# Copyright (c) Microsoft. All rights reserved.

"""Callback interfaces for Durable Agent executions.

This module enables callers of AgentFunctionApp to supply streaming and final-response callbacks that are
invoked during durable entity execution.
"""

import warnings
from dataclasses import dataclass
from typing import Protocol

from agent_framework import AgentResponse, AgentResponseUpdate


@dataclass(frozen=True)
class AgentCallbackContext:
    """Context supplied to callback invocations.

    Note:
        The ``thread_id`` field was renamed to ``session_id``. Reading ``context.thread_id`` still
        works (with a ``DeprecationWarning``) and positional construction is unchanged, but
        constructing this class with the ``thread_id=`` keyword is no longer supported. The agent
        framework is the only producer of this type; callbacks are consumers.
    """

    agent_name: str
    correlation_id: str
    session_id: str | None = None
    request_message: str | None = None

    @property
    def thread_id(self) -> str | None:
        """Deprecated alias for :attr:`session_id`."""
        warnings.warn(
            "AgentCallbackContext.thread_id is deprecated; use session_id instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.session_id


class AgentResponseCallbackProtocol(Protocol):
    """Protocol describing the callbacks invoked during agent execution."""

    async def on_streaming_response_update(
        self,
        update: AgentResponseUpdate,
        context: AgentCallbackContext,
    ) -> None:
        """Handle a streaming response update emitted by the agent."""

    async def on_agent_response(
        self,
        response: AgentResponse,
        context: AgentCallbackContext,
    ) -> None:
        """Handle the final agent response."""
