# Copyright (c) Microsoft. All rights reserved.

"""Durable Agent Shim for Durable Task Framework.

This module provides the DurableAIAgent shim that implements SupportsAgentRun
and provides a consistent interface for both Client and Orchestration contexts.
The actual execution is delegated to the context-specific providers.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, Literal, TypeVar

from agent_framework import AgentSession, ServiceSessionId, SupportsAgentRun, normalize_messages
from agent_framework._types import AgentRunInputs

from ._executors import DurableAgentExecutor
from ._models import AgentSessionId, DurableAgentSession

# TypeVar for the task type returned by executors
# Covariant because TaskT only appears in return positions (output)
TaskT = TypeVar("TaskT", covariant=True)


def build_agent_task(
    executor: DurableAgentExecutor[Any],
    executor_id: str,
    message: str,
    orchestration_instance_id: str,
    context_messages: list[dict[str, Any]] | None = None,
) -> Any:
    """Create the yieldable task that runs a workflow's agent node.

    Shared by every host adapter: the only host-specific part of dispatching an agent is
    which :class:`DurableAgentExecutor` drives it, so the surrounding session/agent wiring
    lives here rather than being repeated per host.

    Args:
        executor: The host's executor, which knows how to reach the agent entity.
        executor_id: The workflow-scoped agent identity to dispatch to.
        message: The text message for this turn.
        orchestration_instance_id: Used as the entity session key, keeping conversation
            state isolated per workflow run.
        context_messages: Optional upstream conversation delivered as prior context.

    Returns:
        A yieldable task whose result is an ``AgentResponse``.
    """
    session_id = AgentSessionId(name=executor_id, key=orchestration_instance_id)
    session = DurableAgentSession(durable_session_id=session_id)
    agent = DurableAIAgent(executor, executor_id)
    return agent.run(message, session=session, context_messages=context_messages)


class DurableAgentProvider(ABC, Generic[TaskT]):
    """Abstract provider for constructing durable agent proxies.

    Implemented by context-specific wrappers (client/orchestration) to return a
    `DurableAIAgent` shim backed by their respective `DurableAgentExecutor`
    implementation, ensuring a consistent `get_agent` entry point regardless of
    execution context.
    """

    @abstractmethod
    def get_agent(self, agent_name: str) -> DurableAIAgent[TaskT]:
        """Retrieve a DurableAIAgent shim for the specified agent.

        Args:
            agent_name: Name of the agent to retrieve

        Returns:
            DurableAIAgent instance that can be used to run the agent

        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement get_agent()")


class DurableAIAgent(SupportsAgentRun, Generic[TaskT]):
    """A durable agent proxy that delegates execution to the provider.

    This class implements SupportsAgentRun but with one critical difference:
    - SupportsAgentRun.run() returns a Coroutine (async, must await)
    - DurableAIAgent.run() returns TaskT (sync Task object - must yield
        or the AgentResponse directly in the case of TaskHubGrpcClient)

    This represents fundamentally different execution models but maintains the same
    interface contract for all other properties and methods.

    The underlying provider determines how execution occurs (entity calls, HTTP requests, etc.)
    and what type of Task object is returned.

    Type Parameters:
        TaskT: The task type returned by this agent (e.g., AgentResponse, DurableAgentTask, AgentTask)
    """

    id: str
    name: str
    display_name: str
    description: str | None

    def __init__(self, executor: DurableAgentExecutor[TaskT], name: str, *, agent_id: str | None = None):
        """Initialize the shim with a provider and agent name.

        Args:
            executor: The execution provider (Client or OrchestrationContext)
            name: The name of the agent to execute
            agent_id: Optional unique identifier for the agent (defaults to name)
        """
        self._executor = executor
        self.name = name  # pyright: ignore[reportIncompatibleVariableOverride]
        self.id = agent_id if agent_id is not None else name
        self.display_name = name
        self.description = f"Durable agent proxy for {name}"

    def run(  # type: ignore[override]
        self,
        messages: AgentRunInputs | None = None,
        *,
        stream: Literal[False] = False,
        session: AgentSession | None = None,
        options: dict[str, Any] | None = None,
        context_messages: list[dict[str, Any]] | None = None,
    ) -> TaskT:
        """Execute the agent via the injected provider.

        Args:
            messages: The message(s) to send to the agent
            stream: Whether to use streaming for the response (must be False)
                DurableAgents do not support streaming mode.
            session: Optional agent session for conversation context
            options: Optional options dictionary. Supported keys include
                ``response_format``, ``enable_tool_calls``, and ``wait_for_response``.
                Additional keys are forwarded to the agent execution.
            context_messages: Optional upstream conversation (serialized ``Message`` dicts)
                delivered to the agent as prior context. Workflows use this to give a
                downstream agent the conversation produced by upstream nodes.

        Note:
            This method overrides SupportsAgentRun.run() with a different return type:
            - SupportsAgentRun.run() returns Coroutine[Any, Any, AgentResponse] (async)
            - DurableAIAgent.run() returns TaskT (Task object for yielding)

            This is intentional to support orchestration contexts that use yield patterns
            instead of async/await patterns.

        Returns:
            TaskT: The task type specific to the executor

        Raises:
            ValueError: If wait_for_response=False is used in an unsupported context
        """
        if stream is not False:
            raise ValueError("DurableAIAgent does not support streaming mode (stream must be False)")
        message_str = self._normalize_messages(messages)

        # Only forward context messages when a workflow supplied them, so executors that do
        # not implement the parameter keep working unchanged.
        extra: dict[str, Any] = {"context_messages": context_messages} if context_messages else {}
        run_request = self._executor.get_run_request(
            message=message_str,
            options=options,
            **extra,
        )

        return self._executor.run_durable_agent(
            agent_name=self.name,
            run_request=run_request,
            session=session,
        )

    def create_session(self, *, session_id: str | None = None) -> DurableAgentSession:
        """Create a new agent session via the provider."""
        return self._executor.get_new_session(self.name)

    def get_session(self, service_session_id: str | ServiceSessionId, *, session_id: str | None = None) -> AgentSession:
        """Retrieve an existing session via the provider."""
        if not isinstance(service_session_id, str):
            raise ValueError("DurableAIAgent requires service_session_id to be a string")
        return self._executor.get_new_session(self.name, service_session_id=service_session_id, session_id=session_id)

    def _normalize_messages(self, messages: AgentRunInputs | None) -> str:
        """Convert supported message inputs to a single string.

        Args:
            messages: The messages to normalize

        Returns:
            A single string representation of the messages

        Raises:
            ValueError: If normalized messages contain non-text content only.
        """
        normalized_messages = normalize_messages(messages)
        if not normalized_messages:
            return ""

        message_texts: list[str] = []
        for message in normalized_messages:
            if not message.text:
                raise ValueError("DurableAIAgent only supports text message inputs.")
            message_texts.append(message.text)

        return "\n".join(message_texts)
