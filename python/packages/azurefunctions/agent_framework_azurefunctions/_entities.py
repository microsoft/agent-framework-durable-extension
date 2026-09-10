# Copyright (c) Microsoft. All rights reserved.

"""Durable Entity for Agent Execution.

This module defines a durable entity that manages agent state and execution.
Using entities instead of orchestrations provides better state management and
allows for long-running agent conversations.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, cast

import azure.durable_functions as df
from agent_framework import SupportsAgentRun
from agent_framework_durabletask import (
    DEFAULT_MAX_STATE_BYTES,
    DEFAULT_RETENTION,
    DELIVERY_WINDOW_SECONDS,
    HIGH_WATERMARK,
    LOW_WATERMARK,
    AgentEntity,
    AgentEntityStateProviderMixin,
    AgentResponseCallbackProtocol,
    RetentionMode,
    StateBudget,
    resolve_state_budget,
    run_agent_coroutine,
    serialize_agent_response,
    validate_agent_configuration,
    validate_response_delivery_window,
    validate_retention,
    validate_runtime_deployment,
)

logger = logging.getLogger("agent_framework.azurefunctions")


class AzureFunctionEntityStateProvider(AgentEntityStateProviderMixin):
    """Azure Functions Durable Entity state provider for AgentEntity.

    This class utilizes the Durable Entity context from `azure-functions-durable` package
    to get and set the state of the agent entity.
    """

    def __init__(self, context: df.DurableEntityContext) -> None:
        self._context = context

    def _get_state_dict(self) -> dict[str, Any]:
        raw_state = self._context.get_state(lambda: {})
        if raw_state is None:
            return {}
        if not isinstance(raw_state, dict):
            raise ValueError("Existing durable entity state must be a dictionary; refusing to replace malformed state.")
        return cast(dict[str, Any], raw_state)

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        self._context.set_state(state)

    def _get_session_id_from_entity(self) -> str:
        return str(self._context.entity_key)

    def _get_entity_name_from_entity(self) -> str:
        return str(self._context.entity_name)


def create_agent_entity(
    agent: SupportsAgentRun,
    callback: AgentResponseCallbackProtocol | None = None,
    *,
    deployment_mode: str | None = None,
    retention: RetentionMode = DEFAULT_RETENTION,
    max_state_bytes: StateBudget = DEFAULT_MAX_STATE_BYTES,
    high_watermark: float = HIGH_WATERMARK,
    low_watermark: float = LOW_WATERMARK,
    response_delivery_window_seconds: int = DELIVERY_WINDOW_SECONDS,
) -> Callable[[df.DurableEntityContext], None]:
    """Factory function to create an agent entity class.

    Args:
        agent: The Microsoft Agent Framework agent instance (must implement SupportsAgentRun)
        callback: Optional callback invoked during streaming and final responses

    Keyword Args:
        deployment_mode: Exactly ``isolated_v2`` to acknowledge an isolated schema 2
            deployment with upgraded clients. None reads ``DURABLE_AGENTS_DEPLOYMENT_MODE``.
            Old workflow histories stay on the old engine. This acknowledgement is not runtime
            proof of isolation and cannot detect peer workers.
        retention: Eager pruning policy, independent of pressure eviction.
        max_state_bytes: Positive integer pressure budget, or None to disable it. Functions cannot
            resolve ``backend_limit`` because the storage backend is configured outside Python.
        high_watermark: Budget fraction at which pressure eviction starts.
        low_watermark: Target budget fraction after pressure eviction.
        response_delivery_window_seconds: Positive integer response delivery window in seconds.

    Returns:
        Entity function configured with the agent

    Raises:
        ValueError: Deployment mode, retention settings, or the agent's history providers are invalid.
    """
    validate_runtime_deployment(deployment_mode)
    validate_retention(retention, high_watermark, low_watermark)
    resolved_budget = resolve_state_budget(max_state_bytes)
    validate_response_delivery_window(response_delivery_window_seconds)
    validate_agent_configuration(agent, retention=retention)

    async def _entity_coroutine(context: df.DurableEntityContext) -> None:
        """Async handler that executes the entity operations."""
        try:
            logger.debug("[entity_function] Entity triggered")
            logger.debug("[entity_function] Operation: %s", context.operation_name)

            state_provider = AzureFunctionEntityStateProvider(context)
            entity = AgentEntity(
                agent,
                callback,
                state_provider=state_provider,
                retention=retention,
                max_state_bytes=resolved_budget,
                high_watermark=high_watermark,
                low_watermark=low_watermark,
                response_delivery_window_seconds=response_delivery_window_seconds,
            )

            operation = context.operation_name

            if operation == "run" or operation == "run_agent":
                input_data: Any = context.get_input()

                request: str | dict[str, Any]
                if isinstance(input_data, dict) and "message" in input_data:
                    request = cast(dict[str, Any], input_data)
                else:
                    # Fall back to treating input as message string
                    request = "" if input_data is None else str(cast(object, input_data))

                result = await entity.run(request)
                context.set_result(serialize_agent_response(result))

            elif operation == "reset":
                entity.reset()
                context.set_result({"status": "reset"})

            elif operation == "expire_responses":
                context.set_result({"expired": entity.expire_responses()})

            elif operation == "migrate":
                context.set_result(entity.migrate(context.get_input()))

            else:
                logger.error("[entity_function] Unknown operation: %s", operation)
                context.set_result({"error": f"Unknown operation: {operation}"})

            logger.info("[entity_function] Operation %s completed successfully", operation)

        except Exception as exc:
            logger.exception("[entity_function] Error executing entity operation %s", exc)
            context.set_result({"error": str(exc), "status": "error"})

    def entity_function(context: df.DurableEntityContext) -> None:
        """Synchronous wrapper invoked by the Durable Functions runtime.

        All agent coroutines run on a single process-wide persistent event loop
        (see ``run_agent_coroutine``). This keeps async resources created by
        shared agent clients/credentials bound to a live loop across every
        invocation, preventing cross-loop hangs when the host dispatches
        successive entity operations onto different worker threads.
        """
        try:
            run_agent_coroutine(_entity_coroutine(context))
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("[entity_function] Unexpected error executing entity: %s", exc, exc_info=True)
            context.set_result({"error": str(exc), "status": "error"})

    return entity_function
