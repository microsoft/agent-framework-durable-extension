# Copyright (c) Microsoft. All rights reserved.

"""Azure Functions adapter for WorkflowOrchestrationContext.

Wraps ``azure.durable_functions.DurableOrchestrationContext`` to satisfy the
:class:`~agent_framework_durabletask.WorkflowOrchestrationContext` protocol.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict, deque
from collections.abc import Callable
from datetime import datetime
from typing import Any, Literal, cast

from agent_framework_durabletask import WorkflowOrchestrationContext, build_agent_task
from azure.durable_functions import DurableOrchestrationContext
from azure.durable_functions.models.entities.ResponseMessage import ResponseMessage
from azure.durable_functions.models.history.HistoryEventType import HistoryEventType
from azure.durable_functions.models.Task import AtomicTask, TaskState

logger = logging.getLogger(__name__)


class _OpaqueEventInput(str):
    """Mark an already-projected history field within this SDK context only."""

    original: str
    entity: bool

    def __new__(cls, original: str, *, entity: bool = False) -> _OpaqueEventInput:
        wire = json.dumps([original])
        if entity:
            # Both SDK decoding stages see only strings and a fixed dictionary.
            # Keep the entire original envelope opaque, including unknown fields.
            wire = json.dumps({"result": wire})
        value = super().__new__(cls, wire)
        value.original = original
        value.entity = entity
        return value


class _JsonEventTask(AtomicTask):
    """Restore plain reply JSON before the native task propagates completion."""

    def __init__(self, task: Any) -> None:
        # wait_for_external_event creates an unscheduled AtomicTask. Reuse its
        # exact identity/action, leaving SDK registration, FIFO and task_any intact.
        super().__init__(task.id, task.action_repr)

    def set_value(self, is_error: bool, value: Any) -> None:
        """Decode the projected payload without invoking SDK object hooks."""
        if not is_error and isinstance(value, list):
            # Every structured Input for this wait was projected to [raw_json].
            # Strings were not projected, even strings that happen to contain JSON.
            # No object_hook, imports or user-selected constructors are allowed.
            value = json.loads(cast(list[str], value)[0])
        super().set_value(is_error, value)


class _JsonEntityTask(AtomicTask):
    """Restore a workflow entity's JSON envelope after both native decode stages."""

    def __init__(self, task: Any) -> None:
        super().__init__(task.id, task.action_repr)

    def set_value(self, is_error: bool, value: Any) -> None:
        """Apply native envelope/error semantics without class-directed decoding."""
        if not is_error:
            envelope = ResponseMessage.from_dict(json.loads(cast(list[str], value)[0]))
            value = json.loads(envelope.result)
            if envelope.is_exception:
                is_error = True
                value = Exception(value)
        super().set_value(is_error, value)


class _EventInputBucket:
    """Keep history references and the extent projected for one selected task kind."""

    def __init__(self) -> None:
        self.events: list[Any] = []
        self.mode: Literal["native", "event", "entity"] | None = None
        self.processed = 0

    def project(self, mode: Literal["native", "event", "entity"]) -> None:
        """Visit only new occurrences unless the selected decoder has changed."""
        if self.mode != mode:
            self.mode = mode
            self.processed = 0
        while self.processed < len(self.events):
            event = self.events[self.processed]
            raw: Any = event.Input
            if isinstance(raw, _OpaqueEventInput):
                if mode != "native" and raw.entity == (mode == "entity"):
                    self.processed += 1
                    continue
                raw = raw.original
            if mode == "native":
                event.Input = raw
            elif isinstance(raw, str):
                if mode == "entity":
                    event.Input = _OpaqueEventInput(raw, entity=True)
                else:
                    try:
                        value = json.loads(raw)
                    except ValueError:
                        # A valid object prefix followed by invalid JSON can
                        # invoke the SDK hook before its parser raises too.
                        event.Input = _OpaqueEventInput(raw)
                    else:
                        event.Input = _OpaqueEventInput(raw) if isinstance(value, (dict, list)) else raw
            self.processed += 1


class _EventInputIndex:
    """Index this context's episode once, without copying or comparing payloads.

    The SDK supplies a fixed list of history objects for each execute call. Keep
    references to those objects so deferred callbacks see the same projection.
    Also accept appended history in fixtures. Replacement or an observed shrink
    rebuilds the index conservatively, including any existing opaque markers.
    Arbitrary in-place edits at the same length (or shrink/regrow between calls)
    are not supported. Only this adapter mutates Input during SDK execution.

    Stable modes cost one history visit and one projection per occurrence, plus
    constant work per wait/pop. A decoder change revisits only its name's bucket.
    Absent queried names never allocate buckets or retain original payload copies.
    """

    def __init__(self) -> None:
        self.histories: list[Any] | None = None
        self.indexed = 0
        self.buckets: dict[str, _EventInputBucket] = {}

    def update(self, histories: list[Any]) -> None:
        """Index the entire available episode, not just SDK-consumed history."""
        if histories is not self.histories or len(histories) < self.indexed:
            self.histories = histories
            self.indexed = 0
            self.buckets.clear()
        while self.indexed < len(histories):
            event = histories[self.indexed]
            if event.event_type == HistoryEventType.EVENT_RAISED:
                name: Any = event.Name
                if isinstance(name, str):
                    bucket = self.buckets.get(name)
                    if bucket is None:
                        bucket = self.buckets[name] = _EventInputBucket()
                    bucket.events.append(event)
            self.indexed += 1

    def project(self, name: str, mode: Literal["native", "event", "entity"]) -> None:
        """Project matching occurrences without remembering absent names."""
        bucket = self.buckets.get(name)
        if bucket is not None:
            bucket.project(mode)


def _event_input_index(context: DurableOrchestrationContext) -> _EventInputIndex | None:
    histories: Any = getattr(context, "histories", None)
    if not isinstance(histories, list):
        # Non-SDK context doubles have no history decoder to protect. Invalidate
        # a prior index if such a fixture temporarily replaces its history.
        if isinstance(getattr(context, "_workflow_event_inputs", None), _EventInputIndex):
            orchestration_context: Any = context
            orchestration_context._workflow_event_inputs = None
        return None
    index: Any = getattr(context, "_workflow_event_inputs", None)
    if not isinstance(index, _EventInputIndex):
        index = _EventInputIndex()
        orchestration_context = context
        orchestration_context._workflow_event_inputs = index
    result: _EventInputIndex = index
    result.update(cast(list[Any], histories))
    return result


def _protect_event_inputs(context: DurableOrchestrationContext, name: str, *, entity: bool = False) -> bool:
    """Shield framework reply waits and entity results from SDK custom objects.

    The SDK keeps EventRaised.Input raw until it finds the matching task,
    including in deferred callbacks. Rewrite those same history objects before
    SDK decoding, not just HTTP submissions. An array holding opaque JSON text
    cannot invoke the SDK object_hook and distinguishes objects/arrays from real
    string replies without adding a reserved field to application data. The
    private str subtype makes repeated adapter/wait creation idempotent.

    For entity calls, project the whole original envelope into a synthetic
    result string containing that same array. The native CallEntityAction path
    still decodes its two JSON layers, then _JsonEntityTask restores the original
    envelope and result with plain JSON. No application keys are reserved or
    discarded. EventSent correlation records and scheduled actions are untouched.
    """
    index = _event_input_index(context)
    if index is None:
        return False
    index.project(name, "entity" if entity else "event")
    return True


class _WorkflowOpenTasks(defaultdict[int | str, Any]):
    """Shield a reply at the SDK's task lookup, before either entity decoder.

    The native executor pops the target before decoding EventRaised.Input. Its
    EventSent handling also pops the numeric task ID before remapping it to the
    service-generated correlation. Preserve both operations and native task/list
    identities. Only framework JSON tasks opt in, so unrelated native tasks keep
    their decoder. This relies on the SDK's pop-before-decode private contract.
    """

    def __init__(self, context: DurableOrchestrationContext, tasks: defaultdict[int | str, Any]) -> None:
        super().__init__(list, tasks)
        self._context = context

    def pop(self, key: int | str, /, *args: Any) -> Any:
        """Preserve native lookup while projecting only the selected JSON task."""
        value: Any = super().pop(key, *args)
        # Native duplicate-name wait lists are consumed from their last entry.
        task: Any = cast(list[Any], value)[-1] if isinstance(value, list) and value else cast(Any, value)
        if isinstance(key, str) and isinstance(task, (_JsonEventTask, _JsonEntityTask)):
            _protect_event_inputs(self._context, key, entity=isinstance(task, _JsonEntityTask))
        elif isinstance(key, str):
            # A name may later belong to an unrelated native task. Undo only
            # our replay-local projection, preserving its original SDK semantics.
            index = _event_input_index(self._context)
            if index is not None:
                index.project(key, "native")
        return cast(Any, value)


class _JsonEntityContext:
    """Delegate agent scheduling unchanged, opting only workflow calls into JSON."""

    def __init__(self, context: DurableOrchestrationContext) -> None:
        self._context = context

    def __getattr__(self, name: str) -> Any:
        return getattr(self._context, name)

    def call_entity(self, entity_id: Any, operation: str, input_: Any = None) -> Any:
        """Keep the SDK action but restore its result as JSON before Core loading."""
        context: Any = self._context
        if not isinstance(getattr(context, "histories", None), list):
            return context.call_entity(entity_id, operation, input_)
        if not isinstance(context.open_tasks, _WorkflowOpenTasks):
            # Do not silently re-enable arbitrary construction on an unknown SDK.
            raise RuntimeError("Unsupported Durable Functions workflow entity task registry")
        return _JsonEntityTask(context.call_entity(entity_id, operation, input_))


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
        tasks: Any = getattr(context, "open_tasks", None)
        if type(tasks) is defaultdict and tasks.default_factory is list:
            orchestration_context: Any = context
            orchestration_context.open_tasks = _WorkflowOpenTasks(context, cast(defaultdict[int | str, Any], tasks))
        # Only adapt the known plain-dict/callback layout. Leave absent or
        # different SDK buffer implementations alone, including test doubles.
        # Reusing an adapter on the same context must not reset queued events.
        deferred: Any = getattr(context, "deferred_tasks", None)
        if type(deferred) is dict:
            callbacks = cast(dict[int | str, Any], deferred)
            if all(callable(callback) for callback in callbacks.values()):
                orchestration_context = context
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
            AzureFunctionsAgentExecutor(cast(Any, _JsonEntityContext(self._context))),
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
        protected = _protect_event_inputs(self._context, name)
        task = self._context.wait_for_external_event(name)
        if protected:
            task = _JsonEventTask(task)
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
