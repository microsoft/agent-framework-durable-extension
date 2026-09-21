# Copyright (c) Microsoft. All rights reserved.

"""Azure Functions adapter for WorkflowOrchestrationContext.

Wraps ``azure.durable_functions.DurableOrchestrationContext`` to satisfy the
:class:`~agent_framework_durabletask.WorkflowOrchestrationContext` protocol.
"""

from __future__ import annotations

import logging
from collections import deque
from collections.abc import Callable
from datetime import datetime
from typing import Any, cast

from agent_framework_durabletask import WorkflowOrchestrationContext, build_agent_task
from azure.durable_functions import DurableOrchestrationContext
from azure.durable_functions.models.Task import TaskState

logger = logging.getLogger(__name__)


class _DeferredEventCallbacks(dict[int | str, Any]):
    """Consume SDK-buffered events once, in arrival order for each event name.

    azure-functions-durable 1.6.0 stores a callback in ``deferred_tasks`` when
    an event arrives before a wait is registered. Its ``_add_to_open_tasks``
    reads and invokes that callback without removing it, and a second early
    event overwrites the first. There is no public buffer-consumption API.

    Install this per-context mapping before external events are processed. Preserve
    callbacks on assignment, then remove exactly one on the SDK's lookup for
    a declared adapter event wait. Creating a wait does not register it, so
    consuming at wait creation would be premature. No SDK methods or task
    scheduling are replaced. Other task IDs retain normal dictionary reads.
    This private SDK contract is covered by real SDK history tests, not a
    promise of compatibility with a different future buffer representation.
    """

    def __init__(self, callbacks: dict[int | str, Any]) -> None:
        super().__init__()
        self.event_names: set[str] = set()
        self._queues: dict[str, deque[Callable[[], Any]]] = {}
        for key, callback in callbacks.items():
            self[key] = callback

    def __setitem__(self, key: int | str, callback: Any) -> None:
        super().__setitem__(key, callback)
        if isinstance(key, str):
            if callable(callback):
                self._queues.setdefault(key, deque()).append(callback)
            else:
                self._queues.pop(key, None)

    def __getitem__(self, key: int | str) -> Any:
        if isinstance(key, str) and key in self.event_names:
            callbacks = self._queues.get(key)
            if callbacks:
                callback = callbacks.popleft()
                if not callbacks:
                    del self._queues[key]
                    super().__delitem__(key)
                return callback
        return super().__getitem__(key)


class AzureFunctionsWorkflowContext:
    """Adapter that maps ``DurableOrchestrationContext`` to ``WorkflowOrchestrationContext``."""

    def __init__(self, context: DurableOrchestrationContext) -> None:
        self._context = context
        # Only adapt the known plain-dict/callback layout. Leave absent or
        # different SDK buffer implementations alone, including test doubles.
        # Reusing an adapter on the same context must not reset queued events.
        deferred: Any = getattr(context, "deferred_tasks", None)
        if type(deferred) is dict:
            callbacks = cast(dict[int | str, Any], deferred)
            if all(callable(callback) for callback in callbacks.values()):
                orchestration_context: Any = context
                orchestration_context.deferred_tasks = _DeferredEventCallbacks(callbacks)

    # -- Properties -----------------------------------------------------------

    @property
    def instance_id(self) -> str:
        # Typed local (not cast): mypy sees the untyped context as Any, while
        # pyright sees a concrete str - the annotation satisfies both.
        instance_id: str = self._context.instance_id
        return instance_id

    @property
    def is_replaying(self) -> bool:
        is_replaying: bool = self._context.is_replaying
        return is_replaying

    @property
    def supports_event_streaming(self) -> bool:
        # The Azure Functions host has no workflow event-streaming endpoint, and its
        # Durable Functions custom status is capped at 16 KB by the WebJobs extension.
        # Publishing the accumulating event log would overflow that cap and fail the
        # orchestrator, so events are omitted; state, pending HITL requests, and the
        # final output remain available via the workflow status endpoint.
        return False

    @property
    def current_utc_datetime(self) -> datetime:
        current: datetime = self._context.current_utc_datetime
        return current

    # -- Agent / Activity dispatch --------------------------------------------

    def prepare_agent_task(
        self,
        executor_id: str,
        message: str,
        orchestration_instance_id: str,
        context_messages: list[dict[str, Any]] | None = None,
        context_message_ids: list[str] | None = None,
    ) -> Any:
        from ._orchestration import AzureFunctionsAgentExecutor

        return build_agent_task(
            AzureFunctionsAgentExecutor(self._context),
            executor_id,
            message,
            orchestration_instance_id,
            context_messages,
            context_message_ids,
        )

    def prepare_activity_task(self, activity_name: str, input_json: str) -> Any:
        orchestration_context: Any = self._context
        return orchestration_context.call_activity(activity_name, input_json)

    def call_sub_orchestrator(self, name: str, input: Any, instance_id: str | None = None) -> Any:
        orchestration_context: Any = self._context
        return orchestration_context.call_sub_orchestrator(name, input_=input, instance_id=instance_id)

    # -- Composite tasks ------------------------------------------------------

    def task_all(self, tasks: list[Any]) -> Any:
        return self._context.task_all(tasks)

    def task_any(self, tasks: list[Any]) -> Any:
        return self._context.task_any(tasks)

    # -- External events / timers ---------------------------------------------

    def wait_for_external_event(self, name: str) -> Any:
        task = self._context.wait_for_external_event(name)
        deferred = getattr(self._context, "deferred_tasks", None)
        if isinstance(deferred, _DeferredEventCallbacks):
            deferred.event_names.add(name)
        return task

    def create_timer(self, fire_at: datetime) -> Any:
        return self._context.create_timer(fire_at)

    # -- Status / utility -----------------------------------------------------

    def set_custom_status(self, status: Any) -> None:
        self._context.set_custom_status(status)

    def new_uuid(self) -> str:
        new_uuid: str = self._context.new_uuid()
        return new_uuid

    def cancel_task(self, task: Any) -> None:
        cancel_fn = getattr(task, "cancel", None)
        if callable(cancel_fn):
            cancel_fn()

    def get_task_result(self, task: Any) -> Any:
        # task_any succeeds with the winning task even when that task failed.
        # Match yielding the task itself (and the standalone SDK's get_result).
        if getattr(task, "state", None) is TaskState.FAILED:
            raise task.result
        return getattr(task, "result", None)


# Ensure the adapter satisfies the protocol. Validated statically by the type checker,
# so a signature change on the protocol is caught here rather than at a distant call site.
_protocol_check: type[WorkflowOrchestrationContext] = AzureFunctionsWorkflowContext
