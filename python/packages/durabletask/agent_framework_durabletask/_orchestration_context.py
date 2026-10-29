# Copyright (c) Microsoft. All rights reserved.

"""Orchestration context wrapper for Durable Task Agent Framework.

This module provides the DurableAIAgentOrchestrationContext class for use inside
orchestration functions to interact with durable agents.
"""

from __future__ import annotations

import logging

from durabletask.task import OrchestrationContext

from ._executors import AcceptanceResponsePolicy, DurableAgentTask, OrchestrationAgentExecutor
from ._shim import DurableAgentProvider, DurableAIAgent

logger = logging.getLogger("agent_framework.durabletask")


class DurableAIAgentOrchestrationContext(DurableAgentProvider[DurableAgentTask]):
    """Orchestration context wrapper for interacting with durable agents internally.

    This class wraps a durabletask OrchestrationContext and provides a convenient
    interface for retrieving and executing durable agents from within orchestration
    functions.

    Example:
        ```python
        from durabletask import Orchestration
        from agent_framework_durabletask import DurableAIAgentOrchestrationContext


        def my_orchestration(context: OrchestrationContext):
            # Wrap the context
            agent_context = DurableAIAgentOrchestrationContext(context)

            # Get an agent reference
            agent = agent_context.get_agent("assistant")

            # Run the agent (returns a Task to be yielded)
            result = yield agent.run("Hello, how are you?")

            return result.text
        ```
    """

    def __init__(
        self, context: OrchestrationContext, *,
        acceptance_response_policy: AcceptanceResponsePolicy = "current",
    ):
        """Initialize the orchestration context wrapper.

        Args:
            context: The durabletask orchestration context to wrap
            acceptance_response_policy: ``published-v1`` preserves the original
                asynchronous-start envelope. ``current`` retains current metadata.
        """
        self._context = context
        self._executor = OrchestrationAgentExecutor(
            self._context, acceptance_response_policy=acceptance_response_policy,
        )
        logger.debug("[DurableAIAgentOrchestrationContext] Initialized")

    def get_agent(self, agent_name: str) -> DurableAIAgent[DurableAgentTask]:
        """Retrieve a DurableAIAgent shim for the specified agent.

        This method returns a proxy object that can be used to execute the agent
        within an orchestration. The agent's run() method will return a Task that
        must be yielded.

        Args:
            agent_name: Name of the agent to retrieve (without the dafx- prefix)

        Returns:
            DurableAIAgent instance that can be used to run the agent

        Note:
            Validation is deferred to execution time. The entity must be registered
            on a worker with the name f"dafx-{agent_name}".
        """
        logger.debug("[DurableAIAgentOrchestrationContext] Creating agent proxy for: %s", agent_name)
        return DurableAIAgent(self._executor, agent_name)
