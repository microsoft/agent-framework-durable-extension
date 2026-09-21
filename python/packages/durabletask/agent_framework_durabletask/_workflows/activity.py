# Copyright (c) Microsoft. All rights reserved.

"""Host-agnostic execution of non-agent workflow executors as durable activities.

When a MAF :class:`Workflow` runs as a durable orchestration, each non-agent
executor is dispatched as a durable *activity*. The activity body is identical
regardless of host (Azure Functions or a standalone durabletask worker): it
deserializes the activity input, runs the executor (or a human-in-the-loop
response handler), captures shared-state writes, and serializes the executor's
outputs, sent messages, shared-state changes, and any pending HITL requests back
to the orchestrator.

This module provides that shared body as :func:`execute_workflow_activity` so
both host adapters call one implementation instead of duplicating it.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, cast

from agent_framework import Executor, Workflow, WorkflowEvent
from agent_framework._workflows._runner_context import YieldOutputEventType
from agent_framework._workflows._state import State

from .orchestrator import SOURCE_ORCHESTRATOR, execute_hitl_response_handler
from .runner_context import CapturingRunnerContext
from .serialization import (
    deserialize_value,
    serialize_response_type,
    serialize_value,
    serialize_workflow_event,
    validate_workflow_json,
)


class _ActivityState(State):
    """Journal explicit operations without changing Core's buffered state semantics.

    Equal-value assignments are writes, not inherited snapshot values. Keep
    pending intent separate so discard() cancels it, while commit(), import_state()
    and clear() retain their effects. Leave value ownership to the installed
    Core State: newer versions deep-copy get/export results, older ones expose
    nested references. Only mutations visible in committed state participate in
    the detached encoded snapshot comparison below.
    """

    def __init__(self, initial_state: dict[str, Any]) -> None:
        """Load an inherited snapshot without marking its keys as writes."""
        super().__init__()
        # Loading the activity snapshot is not an executor write.
        super().import_state(initial_state)
        self.written_keys: set[str] = set()
        self._pending_keys: set[str] = set()

    def set(self, key: str, value: Any) -> None:
        """Stage a write even when its value equals the inherited value."""
        # Delegate ownership/copying to Core and journal only a successful set.
        super().set(key, value)
        self._pending_keys.add(key)

    def delete(self, key: str) -> None:
        """Record only successful deletions, including newly staged keys."""
        super().delete(key)
        self._pending_keys.add(key)

    def commit(self) -> None:
        """Retain intent across commits within one activity invocation."""
        super().commit()
        self.written_keys.update(self._pending_keys)
        self._pending_keys.clear()

    def discard(self) -> None:
        """Cancel pending intent along with Core's pending values."""
        super().discard()
        self._pending_keys.clear()

    def clear(self) -> None:
        """Immediately clear known keys without deleting unseen sibling writes."""
        keys = set(self.export_state()) | self._pending_keys
        super().clear()
        self.written_keys.update(keys)
        self._pending_keys.clear()

    def import_state(self, state: dict[str, Any]) -> None:
        """Track explicit imports, which Core applies immediately."""
        super().import_state(state)
        self.written_keys.update(state)


def execute_workflow_activity(executor: Executor, input_json: str, workflow: Workflow | None = None) -> str:
    """Execute a single non-agent workflow executor and return its serialized result.

    This is the host-agnostic activity body shared by the Azure Functions and
    standalone durabletask workflow hosts.

    Args:
        executor: The non-agent executor instance to run.
        input_json: JSON-encoded activity input with keys ``message``,
            ``shared_state_snapshot``, ``source_executor_ids``, and the optional
            boolean ``is_hitl_response`` dispatch flag (defaults to false).
        workflow: The owning workflow, used to classify the executor's
            ``yield_output`` payloads as final ``output`` vs ``intermediate``.
            When omitted, all yielded outputs are treated as final outputs.

    Returns:
        A JSON string with keys ``sent_messages``, ``outputs``, ``events``,
        ``shared_state_updates``, ``shared_state_deletes``, and
        ``pending_request_info_events``.

    Raises:
        ValueError: If the input does not decode to a JSON object, or a HITL
            message payload is not a JSON object.
    """
    data_obj = json.loads(input_json)
    if not isinstance(data_obj, dict):
        raise ValueError("Activity input must decode to a JSON object")
    validate_workflow_json(data_obj)
    data = cast(dict[str, Any], data_obj)

    message_data = data.get("message")
    # The orchestrator may pass null for these when shared state / sources are
    # omitted, so coerce None to the appropriate empty default.
    shared_state_snapshot: dict[str, Any] = data.get("shared_state_snapshot") or {}
    source_executor_ids = cast(list[str], data.get("source_executor_ids") or [SOURCE_ORCHESTRATOR])
    # Orchestration metadata the orchestrator attaches when dispatching this activity
    # (instance_id, workflow_name). Absent for in-process / legacy inputs, so default
    # to None and surface it on the runner context for executors that want it.
    host_context = cast("dict[str, Any] | None", data.get("host_context"))

    # Reconstruct the message - deserialize_value restores the original typed
    # objects from the encoded data (with type markers).
    message = deserialize_value(message_data)

    # Only the orchestration dispatch envelope can mark a reply. Executor IDs
    # and application payload fields are not control metadata.
    is_hitl_response = data.get("is_hitl_response") is True

    def classify_yielded_output(executor_id: str) -> YieldOutputEventType | None:
        # Mirror the core runner's classification so intermediate executors'
        # yields are not surfaced as final workflow outputs.
        if workflow is None:
            return "output"
        if workflow.is_terminal_executor(executor_id):
            return "output"
        if workflow.is_intermediate_executor(executor_id):
            return "intermediate"
        return None

    async def _run() -> dict[str, Any]:
        runner_context = CapturingRunnerContext()
        runner_context.set_yield_output_classifier(classify_yielded_output)
        runner_context.set_host_metadata(host_context)

        # Deserialize shared state values to reconstruct dataclasses / Pydantic models.
        deserialized_state: dict[str, Any] = {str(k): deserialize_value(v) for k, v in shared_state_snapshot.items()}
        # The encoded input remains detached from the reconstructed state, even
        # for in-place mutations. Compare encoded values, not Python equality
        # (which conflates False/0 and 1/1.0, including inside containers).
        shared_state = _ActivityState(deserialized_state)

        hitl_message: dict[str, Any] | None = None
        if is_hitl_response:
            if not isinstance(message, dict):
                raise ValueError("HITL message payload must be a JSON object")
            hitl_message = cast(dict[str, Any], message)
            admission = await execute_hitl_response_handler(
                executor=executor,
                hitl_message=hitl_message,
                shared_state=shared_state,
                runner_context=runner_context,
            )
            if admission == "invalidreply":
                # This result is a framework control record, not user data or an
                # exception string. No handler or shared-state commit has run.
                return {
                    "hitl_admission": {"request_id": hitl_message.get("request_id"), "status": admission},
                    "sent_messages": [],
                    "outputs": [],
                    "events": [],
                    "shared_state_updates": {},
                    "shared_state_deletes": [],
                    "pending_request_info_events": [],
                }
        else:
            await executor.execute(
                message=message,
                source_executor_ids=source_executor_ids,
                state=shared_state,
                runner_context=runner_context,
            )

        # Commit explicit operations, then include unjournaled in-place changes.
        shared_state.commit()
        current_state = shared_state.export_state()
        original_keys: set[str] = set(shared_state_snapshot.keys())
        current_keys: set[str] = set(current_state.keys())

        # A successful set-then-delete also deletes a sibling's earlier write,
        # even if this key was absent from this activity's original snapshot.
        deletes = (original_keys | shared_state.written_keys) - current_keys

        # Serialize first so custom state still uses the checkpoint codec rather
        # than requiring the in-memory object itself to be JSON serializable.
        # Sorted JSON ignores dictionary insertion order but preserves wire types.
        serialized_updates: dict[str, Any] = {}
        for key in sorted(current_keys):
            encoded = serialize_value(current_state[key])
            if (
                key in shared_state.written_keys
                or key not in original_keys
                or json.dumps(encoded, sort_keys=True, allow_nan=False)
                != json.dumps(shared_state_snapshot[key], sort_keys=True, allow_nan=False)
            ):
                serialized_updates[key] = encoded

        sent_messages = await runner_context.drain_messages()
        events = await runner_context.drain_events()

        # Serialize the executor's workflow events so the orchestrator can republish
        # them to the streaming custom status. Output payloads are also extracted
        # separately for message routing and the final workflow result.
        outputs: list[Any] = []
        serialized_events: list[dict[str, Any]] = []
        for event in events:
            if not isinstance(event, WorkflowEvent):
                continue
            serialized_events.append(serialize_workflow_event(event))
            if event.type == "output":
                outputs.append(serialize_value(event.data))

        # Serialize pending HITL request info events for the orchestrator.
        pending_request_info_events = await runner_context.get_pending_request_info_events()
        serialized_pending_requests: list[dict[str, Any]] = []
        for _request_id, event in pending_request_info_events.items():
            serialized_pending_requests.append({
                "request_id": event.request_id,
                "source_executor_id": event.source_executor_id,
                "data": serialize_value(event.data),
                "request_type": f"{type(event.data).__module__}:{type(event.data).__name__}",
                "response_type": serialize_response_type(event.response_type),
            })

        # Serialize sent messages for JSON compatibility.
        serialized_sent_messages: list[dict[str, Any]] = []
        for _source_id, msg_list in sent_messages.items():
            for msg in msg_list:
                serialized_sent_messages.append({
                    "message": serialize_value(msg.data),
                    "target_id": msg.target_id,
                    "source_id": msg.source_id,
                })

        result: dict[str, Any] = {
            "sent_messages": serialized_sent_messages,
            "outputs": outputs,
            "events": serialized_events,
            "shared_state_updates": serialized_updates,
            "shared_state_deletes": sorted(deletes),
            "pending_request_info_events": serialized_pending_requests,
        }
        if hitl_message is not None and "request_id" in hitl_message:
            result["hitl_admission"] = {"request_id": hitl_message["request_id"], "status": "accepted"}
        return result

    result = asyncio.run(_run())
    return json.dumps(result, allow_nan=False)
