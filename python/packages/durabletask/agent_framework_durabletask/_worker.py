# Copyright (c) Microsoft. All rights reserved.

"""Worker wrapper for Durable Task Agent Framework.

This module provides the DurableAIAgentWorker class that wraps a durabletask worker
and enables registration of agents as durable entities, and optionally workflows
as durable orchestrations with automatically generated activity functions.
"""

from __future__ import annotations

import logging
from typing import Any

from agent_framework import SupportsAgentRun, Workflow
from agent_framework._telemetry import mark_feature_used
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker
from durabletask.task import ActivityContext, OrchestrationContext
from durabletask.worker import TaskHubGrpcWorker

from ._async_bridge import run_agent_coroutine
from ._callbacks import AgentResponseCallbackProtocol
from ._configuration import (
    INHERIT,
    AgentRegistrationSettings,
    RegistrationIdentity,
    StateBudgetOverride,
    resolve_state_budget_override,
    validate_agent_configuration,
    validate_response_delivery_window,
    validate_runtime_deployment,
)
from ._entities import AgentEntity, DurableTaskEntityStateProvider
from ._feature_usage import FeatureIndex
from ._response_utils import serialize_agent_response
from ._retention import (
    DEFAULT_MAX_STATE_BYTES,
    DEFAULT_RETENTION,
    DELIVERY_WINDOW_SECONDS,
    DTS_MAX_STATE_BYTES,
    HIGH_WATERMARK,
    LOW_WATERMARK,
    RetentionMode,
    StateBudget,
    resolve_state_budget,
    validate_retention,
)
from ._workflows.activity import execute_workflow_activity
from ._workflows.dt_context import DurableTaskWorkflowContext
from ._workflows.naming import (
    validate_executor_id,
    validate_workflow_name,
    workflow_executor_activity_name,
    workflow_orchestrator_name,
    workflow_scoped_executor_id,
)
from ._workflows.orchestrator import run_workflow_orchestrator
from ._workflows.protocol import unwrap_workflow_input
from ._workflows.registration import collect_hosted_workflows, plan_workflow_registration

logger = logging.getLogger("agent_framework.durabletask")


class DurableAIAgentWorker:
    """Wrapper for a durabletask worker that hosts agents and workflows.

    This class wraps an existing TaskHubGrpcWorker instance and is the single
    host-side registration surface for a worker process. It supports two
    complementary kinds of work:

    - **Agents** via :meth:`add_agent`, which registers each agent as a durable entity.
    - **Workflows** via :meth:`configure_workflow`, which registers a MAF
      ``Workflow`` (its agent executors as entities, its non-agent executors as
      activities, and the workflow orchestrator).

    A single worker process commonly hosts both, so registration is intentionally
    aggregated on one object rather than split per kind. (On the *client* side the
    surfaces are split into :class:`DurableAIAgentClient` and ``DurableWorkflowClient``,
    because a caller invokes one or the other.)

    Set ``deployment_mode="isolated_v2"`` or ``DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2``
    to acknowledge an isolated schema 2 task hub/deployment with upgraded clients.
    Old workflow histories must remain on the old engine. This acknowledgement is
    not runtime proof of isolation and cannot detect peer workers.

    Example:
        ```python
        from durabletask.worker import TaskHubGrpcWorker
        from agent_framework import Agent
        from agent_framework.openai import OpenAIChatCompletionClient
        from agent_framework_durabletask import DurableAIAgentWorker

        # Create the underlying worker
        worker = TaskHubGrpcWorker(host_address="localhost:4001")

        # Acknowledge that this is an isolated schema 2 deployment
        agent_worker = DurableAIAgentWorker(worker, deployment_mode="isolated_v2")

        # Register agents (or call configure_workflow(workflow) to host a workflow)
        client = OpenAIChatCompletionClient()
        my_agent = Agent(client=client, name="assistant")
        agent_worker.add_agent(my_agent)

        # Start the worker
        worker.start()
        ```
    """

    def __init__(
        self,
        worker: TaskHubGrpcWorker,
        callback: AgentResponseCallbackProtocol | None = None,
        *,
        deployment_mode: str | None = None,
        retention: RetentionMode = DEFAULT_RETENTION,
        max_state_bytes: StateBudget = DEFAULT_MAX_STATE_BYTES,
        high_watermark: float = HIGH_WATERMARK,
        low_watermark: float = LOW_WATERMARK,
        response_delivery_window_seconds: int = DELIVERY_WINDOW_SECONDS,
    ):
        """Initialize the worker wrapper.

        Args:
            worker: The durabletask worker instance to wrap
            callback: Optional callback for agent response notifications
            deployment_mode: Exactly ``isolated_v2`` to acknowledge an isolated schema 2
                deployment with upgraded clients. None reads ``DURABLE_AGENTS_DEPLOYMENT_MODE``.
                Old workflow histories stay on the old engine. This is not runtime proof of isolation.
            retention: Eager pruning policy. ``keep_all`` does not prune compaction exclusions;
                ``follow_compaction`` does. Pressure eviction is controlled separately by the budget.
            max_state_bytes: Optional serialized-state budget. None disables pressure eviction;
                ``backend_limit`` requires a DurableTaskSchedulerWorker. An explicit positive
                integer works with any backend.
            high_watermark: Budget fraction at which pressure eviction starts.
            low_watermark: Target budget fraction after pressure eviction.
            response_delivery_window_seconds: Positive integer response delivery window in seconds.
        """
        validate_runtime_deployment(deployment_mode)
        validate_retention(retention, high_watermark, low_watermark)
        self._backend_limit = DTS_MAX_STATE_BYTES if isinstance(worker, DurableTaskSchedulerWorker) else None
        resolved_max_state_bytes = resolve_state_budget(max_state_bytes, backend_limit=self._backend_limit)
        validate_response_delivery_window(response_delivery_window_seconds)

        self._worker = worker
        self._callback = callback
        self._retention: RetentionMode = retention
        self._max_state_bytes = resolved_max_state_bytes
        self._high_watermark = high_watermark
        self._low_watermark = low_watermark
        self._response_delivery_window_seconds = response_delivery_window_seconds
        self._registered_agents: dict[str, SupportsAgentRun] = {}
        self._workflows: dict[str, Workflow] = {}
        # Every workflow whose orchestration has been registered (top-level plus nested
        # sub-workflows), keyed by case-folded name -> the registered instance, so a
        # sub-workflow shared across the tree is registered once while two different
        # workflows whose names collide (including case-only differences) are rejected.
        self._registered_orchestrations: dict[str, Workflow] = {}
        self._registration_identities: dict[tuple[str, str], RegistrationIdentity] = {}
        self._registration_failed = False
        logger.debug("[DurableAIAgentWorker] Initialized with worker type: %s", type(worker).__name__)

    def add_agent(
        self,
        agent: SupportsAgentRun,
        callback: AgentResponseCallbackProtocol | None = None,
        *,
        entity_id: str | None = None,
        retention: RetentionMode | None = None,
        max_state_bytes: StateBudgetOverride = INHERIT,
        high_watermark: float | None = None,
        low_watermark: float | None = None,
        response_delivery_window_seconds: int | None = None,
    ) -> None:
        """Register an agent with the worker.

        This method creates a durable entity class for the agent and registers
        it with the underlying durabletask worker. The entity will be accessible
        by the name "dafx-{entity_id or agent_name}".

        Args:
            agent: The agent to register (must have a name)
            callback: Optional callback for this specific agent (overrides worker-level callback)
            entity_id: Optional identity to register the entity under instead of
                ``agent.name``. Workflow hosting passes the executor's ``id`` so the
                entity matches the identity the orchestrator dispatches to.
            retention: Per-agent retention override. When None, the worker-level setting is used.
            max_state_bytes: Per-agent budget. INHERIT uses the worker default; None disables it.
            high_watermark: Pressure trigger override, or None to inherit the worker default.
            low_watermark: Pressure target override, or None to inherit the worker default.
            response_delivery_window_seconds: Delivery window override, or None to inherit.

        Raises:
            ValueError: If the name, retention settings, or history-provider composition is invalid,
                or the agent is already registered.
        """
        self._ensure_registration_usable()
        registration_name = entity_id or agent.name
        if not isinstance(registration_name, str) or not registration_name:
            raise ValueError("Agent must have a name to be registered")

        if registration_name in self._registered_agents:
            raise ValueError(f"Agent '{registration_name}' is already registered")

        effective_retention = self._retention if retention is None else retention
        effective_budget = resolve_state_budget_override(
            max_state_bytes, self._max_state_bytes, backend_limit=self._backend_limit
        )
        effective_high = self._high_watermark if high_watermark is None else high_watermark
        effective_low = self._low_watermark if low_watermark is None else low_watermark
        effective_window = (
            self._response_delivery_window_seconds
            if response_delivery_window_seconds is None
            else response_delivery_window_seconds
        )
        validate_retention(effective_retention, effective_high, effective_low)
        validate_response_delivery_window(effective_window)
        validate_agent_configuration(agent, retention=effective_retention)
        effective_callback = self._callback if callback is None else callback
        settings = AgentRegistrationSettings(
            effective_retention, effective_budget, effective_high, effective_low, effective_window, effective_callback
        )
        identities = dict(self._registration_identities)
        RegistrationIdentity(agent, agent, "entity", settings, f"agent '{registration_name}'").reserve(
            identities, f"dafx-{registration_name}", namespace="entity-name"
        )

        logger.info(
            "[DurableAIAgentWorker] Registering agent: %s as entity: dafx-%s", registration_name, registration_name
        )

        # Create a configured entity class using the factory
        entity_class = self.__create_agent_entity(
            agent,
            effective_callback,
            entity_id=registration_name,
            retention=effective_retention,
            max_state_bytes=effective_budget,
            high_watermark=effective_high,
            low_watermark=effective_low,
            response_delivery_window_seconds=effective_window,
        )

        # Register the entity class with the worker
        # The worker.add_entity method takes a class
        try:
            entity_registered: str = self._worker.add_entity(entity_class)
        except Exception:
            # A backend can fail after mutating its registry, with no public rollback API.
            self._registration_failed = True
            raise
        self._registered_agents[registration_name] = agent
        self._registration_identities = identities

        logger.debug(
            "[DurableAIAgentWorker] Successfully registered entity class %s for agent: %s",
            entity_registered,
            registration_name,
        )

    def _ensure_registration_usable(self) -> None:
        if self._registration_failed:
            raise RuntimeError(
                "Backend registration failed; this host may be partially registered. "
                "Create a new host with a new underlying worker before registering or starting."
            )

    def start(self) -> None:
        """Start the worker to begin processing tasks.

        Note:
            This method delegates to the underlying worker's start method.
            The worker will block until stopped.
        """
        self._ensure_registration_usable()
        logger.info("[DurableAIAgentWorker] Starting worker with %d registered agents", len(self._registered_agents))
        mark_feature_used(FeatureIndex.DURABLETASK)
        self._worker.start()

    def stop(self) -> None:
        """Stop the worker gracefully.

        Note:
            This method delegates to the underlying worker's stop method.
        """
        logger.info("[DurableAIAgentWorker] Stopping worker")
        self._worker.stop()

    @property
    def registered_agent_names(self) -> list[str]:
        """Get the names of all registered agents.

        Returns:
            List of agent names (without the dafx- prefix)
        """
        return list(self._registered_agents.keys())

    @property
    def registered_workflow_names(self) -> list[str]:
        """Get the names of all workflows configured on this worker.

        Returns:
            List of workflow names (the identities used to derive each workflow's
            ``dafx-{name}`` orchestration).
        """
        return list(self._workflows.keys())

    # -----------------------------------------------------------------
    # Workflow support
    # -----------------------------------------------------------------

    def configure_workflow(
        self,
        workflow: Workflow,
        callback: AgentResponseCallbackProtocol | None = None,
        *,
        retention: RetentionMode | None = None,
        max_state_bytes: StateBudgetOverride = INHERIT,
        high_watermark: float | None = None,
        low_watermark: float | None = None,
        response_delivery_window_seconds: int | None = None,
    ) -> None:
        """Register a :class:`Workflow` for automatic orchestration.

        This extracts agents from the workflow and registers them as durable
        entities, registers non-agent executors as activities, and creates an
        orchestrator function that drives the workflow graph.

        Multiple workflows can be hosted on one worker: call this method once per
        workflow. Each workflow is keyed by its :attr:`Workflow.name`, and its
        durable primitives are scoped by that name (orchestration
        ``dafx-{name}``; activities/entities ``dafx-{name}-{executorId}``). Ambiguous
        derived names are rejected rather than renamed, preserving deployment compatibility.

        Sub-workflows nest: if the workflow contains
        :class:`~agent_framework.WorkflowExecutor` nodes, each inner workflow's
        orchestration/agents/activities are registered too (deduped by name) so the
        parent can drive them as durable child orchestrations.

        Args:
            workflow: The MAF :class:`Workflow` to register. Must have an explicit,
                stable :attr:`Workflow.name` (an auto-generated
                ``WorkflowBuilder-<uuid>`` name is rejected because it is not stable
                across restarts and would break durable resume). Every nested
                sub-workflow must likewise be named.
            callback: Optional callback for agent response notifications.
            retention: Retention for this workflow's agent nodes. When None, the worker-level
                setting is used. Worth setting separately, since a workflow node's entity lives
                for one orchestration while a standalone agent's can live indefinitely.
            max_state_bytes: Budget for newly registered agent nodes in this workflow and its nested
                workflows. INHERIT uses the worker default; None disables pressure eviction.
            high_watermark: Pressure trigger override, or None to inherit the worker default.
            low_watermark: Pressure target override, or None to inherit the worker default.
            response_delivery_window_seconds: Delivery window override, or None to inherit.

        Raises:
            ValueError: If the workflow (or a nested sub-workflow) name is missing,
                invalid, or auto-generated, a derived name has a different owner,
                a shared workflow has different settings, or history preparation fails.
        """
        self._ensure_registration_usable()
        workflow_name = workflow.name
        validate_workflow_name(workflow_name)

        effective_retention = self._retention if retention is None else retention
        effective_budget = resolve_state_budget_override(
            max_state_bytes, self._max_state_bytes, backend_limit=self._backend_limit
        )
        effective_high = self._high_watermark if high_watermark is None else high_watermark
        effective_low = self._low_watermark if low_watermark is None else low_watermark
        effective_window = (
            self._response_delivery_window_seconds
            if response_delivery_window_seconds is None
            else response_delivery_window_seconds
        )
        validate_retention(effective_retention, effective_high, effective_low)
        validate_response_delivery_window(effective_window)
        settings = AgentRegistrationSettings(
            effective_retention,
            effective_budget,
            effective_high,
            effective_low,
            effective_window,
            self._callback if callback is None else callback,
        )

        # Reserve the actual derived identities for the entire composition before any SDK calls.
        hosted_workflows = list(collect_hosted_workflows(workflow))
        identities = dict(self._registration_identities)
        for hosted in hosted_workflows:
            validate_workflow_name(hosted.name)
            for executor_id in hosted.executors:
                validate_executor_id(executor_id)
            label = f"workflow '{hosted.name}'"
            RegistrationIdentity(hosted, hosted, "orchestration", settings, label).reserve(
                identities, workflow_orchestrator_name(hosted.name), namespace="orchestrator-name"
            )
            plan = plan_workflow_registration(hosted)
            for agent_executor in plan.agent_executors:
                validate_executor_id(agent_executor.id)
                validate_agent_configuration(agent_executor.agent, retention=effective_retention)
                RegistrationIdentity(
                    hosted, agent_executor.agent, "entity", settings, f"{label} executor '{agent_executor.id}'"
                ).reserve(
                    identities,
                    f"dafx-{workflow_scoped_executor_id(hosted.name, agent_executor.id)}",
                    namespace="entity-name",
                )
            for executor in plan.activity_executors:
                validate_executor_id(executor.id)
                RegistrationIdentity(
                    hosted, executor, "activity", settings, f"{label} executor '{executor.id}'"
                ).reserve(
                    identities, workflow_executor_activity_name(hosted.name, executor.id), namespace="activity-name"
                )

        previous_agents = dict(self._registered_agents)
        previous_identities = self._registration_identities
        try:
            for hosted in hosted_workflows:
                if hosted.name.casefold() in self._registered_orchestrations:
                    continue
                self._register_single_workflow(
                    hosted,
                    callback,
                    effective_retention,
                    max_state_bytes=effective_budget,
                    high_watermark=effective_high,
                    low_watermark=effective_low,
                    response_delivery_window_seconds=effective_window,
                )
        except Exception:
            self._registration_failed = True
            self._registered_agents = previous_agents
            self._registration_identities = previous_identities
            raise
        self._registration_identities = identities
        self._registered_orchestrations.update({hosted.name.casefold(): hosted for hosted in hosted_workflows})
        self._workflows[workflow_name] = workflow

    def _register_single_workflow(
        self,
        workflow: Workflow,
        callback: AgentResponseCallbackProtocol | None,
        retention: RetentionMode | None = None,
        *,
        max_state_bytes: StateBudgetOverride = INHERIT,
        high_watermark: float | None = None,
        low_watermark: float | None = None,
        response_delivery_window_seconds: int | None = None,
    ) -> None:
        """Register one workflow's durable primitives (no recursion into sub-workflows).

        The "what to register" decision (agent -> entity, non-agent -> activity,
        sub-workflow -> child orchestration) is shared with the Azure Functions host
        via ``plan_workflow_registration``.
        """
        validate_workflow_name(workflow.name)
        plan = plan_workflow_registration(workflow)

        # Register agent executors under the names validated by composition preflight. The
        # entity is keyed by the scoped identity (the same identity the orchestrator
        # dispatches to); the entity *key* at run time is the orchestration instance
        # id, which keeps conversation state isolated per run.
        for agent_executor in plan.agent_executors:
            scoped_id = workflow_scoped_executor_id(workflow.name, agent_executor.id)
            self.add_agent(
                agent_executor.agent,
                callback=callback,
                entity_id=scoped_id,
                retention=retention,
                max_state_bytes=max_state_bytes,
                high_watermark=high_watermark,
                low_watermark=low_watermark,
                response_delivery_window_seconds=response_delivery_window_seconds,
            )

        # Register non-agent executors as durable activities, scoped by workflow name.
        # WorkflowExecutor nodes are intentionally not registered as activities: their
        # inner workflows are registered separately (above, via collect_hosted_workflows)
        # and driven as child orchestrations.
        for executor in plan.activity_executors:
            self._register_executor_activity(workflow, executor)

        # Register this workflow's orchestrator under its per-workflow name.
        self._register_workflow_orchestrator(workflow)

        logger.info(
            "[DurableAIAgentWorker] Workflow '%s' configured with %d executors "
            "(%d agents, %d activities, %d sub-workflows)",
            workflow.name,
            len(workflow.executors),
            len(plan.agent_executors),
            len(plan.activity_executors),
            len(plan.subworkflow_executors),
        )

    def _register_executor_activity(self, workflow: Workflow, executor: Any) -> None:
        """Register a non-agent executor as a durabletask activity (workflow-scoped)."""
        captured_executor = executor
        captured_workflow = workflow
        activity_name = workflow_executor_activity_name(workflow.name, executor.id)

        def executor_activity(ctx: ActivityContext, input_data: str) -> str:
            return execute_workflow_activity(captured_executor, input_data, captured_workflow)

        # Give the function the expected name for registration
        executor_activity.__name__ = activity_name
        executor_activity.__qualname__ = activity_name

        self._worker.add_activity(executor_activity)
        logger.debug("[DurableAIAgentWorker] Registered activity: %s", activity_name)

    def _register_workflow_orchestrator(self, workflow: Workflow) -> None:
        """Register a workflow's orchestrator function under its per-workflow name."""
        captured_workflow = workflow
        orchestrator_name = workflow_orchestrator_name(workflow.name)

        def workflow_orchestrator(context: OrchestrationContext, input_data: Any) -> Any:
            # Never replay the changed engine against a legacy recorded start.
            initial_message = unwrap_workflow_input(input_data)
            shared_state: dict[str, Any] = {}

            dt_ctx = DurableTaskWorkflowContext(context)
            outputs = yield from run_workflow_orchestrator(dt_ctx, captured_workflow, initial_message, shared_state)
            return outputs  # noqa: B901

        workflow_orchestrator.__name__ = orchestrator_name
        workflow_orchestrator.__qualname__ = orchestrator_name

        self._worker.add_orchestrator(workflow_orchestrator)
        logger.debug("[DurableAIAgentWorker] Registered workflow orchestrator: %s", orchestrator_name)

    def __create_agent_entity(
        self,
        agent: SupportsAgentRun,
        callback: AgentResponseCallbackProtocol | None = None,
        *,
        entity_id: str | None = None,
        retention: RetentionMode = DEFAULT_RETENTION,
        max_state_bytes: int | None = None,
        high_watermark: float = HIGH_WATERMARK,
        low_watermark: float = LOW_WATERMARK,
        response_delivery_window_seconds: int = DELIVERY_WINDOW_SECONDS,
    ) -> type[DurableTaskEntityStateProvider]:
        """Factory function to create a DurableEntity class configured with an agent.

        This factory creates a new class that combines the entity state provider
        with the agent execution logic. Each agent gets its own entity class.

        Args:
            agent: The agent instance to wrap
            callback: Optional callback for agent responses
            entity_id: Optional identity to register the entity under instead of
                ``agent.name`` (used by workflow hosting to key entities by
                executor id).
            retention: How much of the conversation durable state may discard.
            max_state_bytes: Resolved pressure budget, or None to disable pressure eviction.
            high_watermark: Budget fraction at which pressure eviction starts.
            low_watermark: Target budget fraction after pressure eviction.
            response_delivery_window_seconds: Response delivery window in seconds.

        Returns:
            A new DurableEntity subclass configured for this agent
        """
        agent_name = entity_id or agent.name or type(agent).__name__
        entity_name = f"dafx-{agent_name}"

        class ConfiguredAgentEntity(DurableTaskEntityStateProvider):
            """Durable entity configured with a specific agent instance."""

            def __init__(self) -> None:
                super().__init__()
                # Create the AgentEntity with this state provider
                self._agent_entity = AgentEntity(
                    agent=agent,
                    callback=callback,
                    state_provider=self,
                    retention=retention,
                    max_state_bytes=max_state_bytes,
                    high_watermark=high_watermark,
                    low_watermark=low_watermark,
                    response_delivery_window_seconds=response_delivery_window_seconds,
                )
                logger.debug(
                    "[ConfiguredAgentEntity] Initialized entity for agent: %s (entity name: %s)",
                    agent_name,
                    entity_name,
                )

            def run(self, request: Any) -> Any:
                """Handle run requests from clients or orchestrations.

                Args:
                    request: RunRequest as dict or string

                Returns:
                    AgentResponse as dict
                """
                logger.debug("[ConfiguredAgentEntity.run] Executing agent: %s", agent_name)
                # Run on the shared persistent loop so async resources created by
                # shared agent clients/credentials stay bound to a live loop across
                # successive entity invocations (avoids cross-loop hangs).
                response = run_agent_coroutine(self._agent_entity.run(request))
                return serialize_agent_response(response)

            def reset(self) -> None:
                """Delegate reset to the configured AgentEntity."""
                logger.debug("[ConfiguredAgentEntity.reset] Resetting agent: %s", agent_name)
                self._agent_entity.reset()

            def expire_responses(self) -> int:
                """Remove expired payloads when signaled by application-owned maintenance."""
                return self._agent_entity.expire_responses()

            def migrate(self, request: dict[str, Any]) -> dict[str, str]:
                """Import an authorized legacy export into an empty destination entity."""
                return self._agent_entity.migrate(request)

        # Set the entity name to match the prefixed agent name
        # This is used by durabletask to register the entity
        ConfiguredAgentEntity.__name__ = entity_name
        ConfiguredAgentEntity.__qualname__ = entity_name

        return ConfiguredAgentEntity
