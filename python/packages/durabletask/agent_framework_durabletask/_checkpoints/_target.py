# Copyright (c) Microsoft. All rights reserved.

"""Experimental checkpoint-only scheduler using current execution primitives.

Migration owns admission and quiescence. This module never imports the published
engine, serializer, invocation helper or reader. Checkpoint values are trusted
objects supplied after authenticated admission, not arbitrary client JSON.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from collections.abc import Callable, Generator
from contextlib import suppress
from typing import Any

from agent_framework import (
    AgentExecutor,
    AgentExecutorRequest,
    AgentExecutorResponse,
    AgentResponse,
    Message,
    Workflow,
    WorkflowConvergenceException,
    WorkflowExecutor,
)
from agent_framework._workflows._edge import FanInEdgeGroup

from agent_framework_durabletask._response_utils import load_agent_response
from agent_framework_durabletask._workflows import orchestrator as current
from agent_framework_durabletask._workflows.context import WorkflowOrchestrationContext
from agent_framework_durabletask._workflows.serialization import (
    SUBWORKFLOW_RESULT_KEY,
    deserialize_value,
    serialize_value,
    strip_pickle_markers,
)

from ._addresses import published_subworkflow_instance_id
from ._types import is_list, is_list_or_tuple, is_string_keyed_dict

CONTRACT = "semantic-join-checkpoint/3"
logger = logging.getLogger(__name__)


def fan_bindings(workflow: Workflow) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    groups = [group for group in workflow.edge_groups if isinstance(group, FanInEdgeGroup)]
    return {
        f"fan/{index}": {
            "edges": [[edge.source_id, edge.target_id] for edge in group.edges],
            "sources": list(group.source_executor_ids),
            "targets": list(group.target_executor_ids),
        }
        for index, group in enumerate(groups)
    }, {f"fan/{index}": group.id for index, group in enumerate(groups)}


def route_join_result(
    result: current.ExecutorResult,
    workflow: Workflow,
    pending: dict[str, list[tuple[Any, Any]]],
    fan: dict[str, Any],
) -> None:
    """Versioned join routing. Fan-in flushes once after the whole result batch."""
    messages = [(result.output_message, None)] if result.output_message else []
    for item in (result.activity_result or {}).get("sent_messages", []):
        if item.get("message") is not None:
            messages.append((deserialize_value(item["message"]), item.get("target_id")))
    for message, target in messages:
        if target:
            pending.setdefault(target, []).append((message, [result.executor_id]))
            continue
        for group in workflow.edge_groups:
            if isinstance(group, FanInEdgeGroup) and result.executor_id in group.source_executor_ids:
                fan[group.id][result.executor_id].append((message, result.executor_id))
        for target in current.route_message_through_edge_groups(
            workflow.edge_groups,
            result.executor_id,
            message,
        ):
            pending.setdefault(target, []).append((message, [result.executor_id]))


def flush_join_fan(
    workflow: Workflow,
    fan: dict[str, Any],
    pending: dict[str, list[tuple[Any, Any]]],
) -> None:
    for group in workflow.edge_groups:
        if not isinstance(group, FanInEdgeGroup):
            continue
        rows = fan[group.id]
        if not all(rows.get(source) for source in group.source_executor_ids):
            continue
        values = [message for source in group.source_executor_ids for message, _ in rows[source]]
        first = rows[group.source_executor_ids[0]][0][1]
        pending.setdefault(group.target_executor_ids[0], []).append((values, [first]))
        fan[group.id] = defaultdict(list)


def published_frontier_agent_result(
    raw: Any,
    executor_id: str,
    previous_message: Any,
) -> current.ExecutorResult:
    """Versioned result projection for an already accepted published native call."""
    response = load_agent_response(raw) if raw is not None else None
    structured: dict[str, Any] | None = None
    if response is not None and response.value is not None:
        dump = getattr(response.value, "model_dump", None)
        value = dump() if callable(dump) else response.value
        if is_string_keyed_dict(value):
            structured = value
    text = response.text if response is not None else None
    assistant = Message(role="assistant", contents=[json.dumps(structured) if structured else text or ""])
    conversation: list[Message] = []
    if isinstance(previous_message, AgentExecutorResponse) and previous_message.full_conversation:
        conversation.extend(previous_message.full_conversation)
    elif isinstance(previous_message, str):
        conversation.append(Message(role="user", contents=[previous_message]))
    conversation.append(assistant)
    output = AgentExecutorResponse(
        executor_id=executor_id,
        agent_response=AgentResponse(messages=[assistant]),
        full_conversation=conversation,
    )
    return current.ExecutorResult(
        executor_id=executor_id,
        output_message=output,
        activity_result=None,
        task_type=current.TaskType.AGENT,
    )


def published_agent_input(message: Any) -> str:
    """The checkpoint profile carries last-text inputs, not Core context handoffs."""
    if isinstance(message, AgentExecutorResponse) and message.agent_response:
        response = message.agent_response
        return response.text or (response.messages[-1].text or "" if response.messages else "")
    if isinstance(message, AgentExecutorRequest) and message.messages:
        return message.messages[-1].text or ""
    if isinstance(message, str):
        return message
    if is_string_keyed_dict(message):
        logger.warning("Published checkpoint agent received a dict message. Keys: %s", list(message))
    return ""


def prepare_published_agent(
    ctx: WorkflowOrchestrationContext,
    workflow_name: str,
    executor: str,
    message: Any,
) -> Any:
    return ctx.prepare_agent_task(
        f"{workflow_name}-{executor}",
        published_agent_input(message),
        ctx.instance_id,
    )


def prepare_semantic_batch(
    ctx: WorkflowOrchestrationContext,
    workflow: Workflow,
    pending: dict[str, list[tuple[Any, Any]]],
    shared: dict[str, Any] | None,
    counter: list[int],
    address: dict[str, str],
    ledger: Any,
) -> tuple[list[Any], list[current.TaskMetadata], list[tuple[str, Any, Any]]]:
    ordinary = {
        executor: rows
        for executor, rows in pending.items()
        if not isinstance(workflow.executors[executor], AgentExecutor)
    }
    tasks, metadata, remaining = current.prepare_all_tasks(
        ctx,
        workflow,
        ordinary,
        shared,
        counter,
        address,
        ledger,
        child_instance_id_factory=published_subworkflow_instance_id,
    )
    if remaining:
        raise ValueError("Non-agent preparation unexpectedly created sequential agent work.")
    for executor, rows in pending.items():
        if not isinstance(workflow.executors[executor], AgentExecutor) or not rows:
            continue
        message, origin = rows[0]
        tasks.append(prepare_published_agent(ctx, workflow.name, executor, message))
        metadata.append(current.TaskMetadata(executor, message, origin, current.TaskType.AGENT))
        remaining.extend((executor, message, origin) for message, origin in rows[1:])
    return tasks, metadata, remaining


def project_checkpoint(source: dict[str, Any], workflow: Workflow) -> dict[str, Any]:
    """Convert a trusted safe-cut value into current semantic structures."""
    if source["phase"] not in ("prepare", "hitl", "children"):
        raise ValueError("Only a drained operation frontier can be projected.")
    requests: dict[str, dict[str, Any]] = {}
    for key, request in source["pending_hitl_requests"].items():
        requests[key] = {
            "contract": "semantic-hitl-request/1",
            "request_id": request.request_id,
            "source_executor_id": request.source_executor_id,
            "request_data": request.request_data,
            "request_type": request.request_type,
            "response_type": request.response_type,
        }
    pending = {
        executor: [(message, [origin] if isinstance(origin, str) else origin) for message, origin in rows]
        for executor, rows in source["pending_messages"].items()
    }
    waiting = list(requests)
    for request in source.get("admitted_hitl_responses", []):
        key = request.request_id
        matches: list[tuple[list[tuple[Any, Any]], int, dict[str, Any], Any]] = []
        for executor, rows in pending.items():
            for index, (message, origin) in enumerate(rows):
                if (
                    executor == request.source_executor_id
                    and is_string_keyed_dict(message)
                    and message.get("request_id") == key
                    and origin == [f"{current.SOURCE_HITL_RESPONSE}_{key}"]
                ):
                    matches.append((rows, index, message, origin))
        if len(matches) != 1:
            raise ValueError("Recorded admitted reply does not match exactly one queued message.")
        rows, index, message, origin = matches[0]
        requests[key] = {
            "contract": "semantic-hitl-request/1",
            "request_id": key,
            "source_executor_id": request.source_executor_id,
            "request_data": request.request_data,
            "request_type": request.request_type,
            "response_type": request.response_type,
        }
    bindings, ids = fan_bindings(workflow)
    if set(source["fan_in_pending"]) != set(ids.values()):
        raise ValueError("Captured fan-in state does not match the trusted original definition.")
    return {
        "contract": CONTRACT,
        "routingPolicy": "published-batch-join/1",
        "phase": source["phase"],
        "eventStreaming": source["event_streaming"],
        "pending": pending,
        "outputs": list(source["workflow_outputs"]),
        "iteration": source["iteration"],
        "child_counter": source["subworkflow_counter"][0],
        "is_child": source["is_subworkflow"],
        "address": dict(source["workflow_address"]),
        "events": list(source["live_events"]),
        "shared_state": source["shared_state"],
        "requests": requests,
        "waiting": waiting,
        "admittedReplyKeys": [request.request_id for request in source.get("admitted_hitl_responses", [])],
        "fanBindings": bindings,
        "fan": {key: source["fan_in_pending"][value] for key, value in ids.items()},
        "children": source.get("children", []),
        "notify_parent": source.get("notify_parent"),
    }


def run(
    ctx: WorkflowOrchestrationContext,
    workflow: Workflow,
    checkpoint: dict[str, Any],
    *,
    verify_child: Callable[..., Any] | None = None,
    child_result_task: Callable[..., Any] | None = None,
    native_child_tasks: dict[str, Any] | None = None,
    native_batch_tasks: list[Any] | None = None,
    automatic_emission: dict[str, str] | None = None,
) -> Generator[Any, Any, list[Any] | dict[str, Any]]:
    """Continue semantic state after its caller authenticates the runtime binding.

    A same-execution mechanical shell may pass its verified original native
    child tasks. That path uses backend result routing, not signed outboxes.
    """
    if checkpoint.get("contract") != CONTRACT:
        raise ValueError("Unsupported semantic checkpoint contract.")
    if checkpoint.get("routingPolicy") != "published-batch-join/1":
        raise ValueError("Unsupported checkpoint routing policy.")
    if not isinstance(checkpoint.get("eventStreaming"), bool):
        raise ValueError("Checkpoint requires an explicit source event publication policy.")
    if automatic_emission is not None and (
        set(automatic_emission) != {"definition_sha256"}
        or not isinstance(automatic_emission["definition_sha256"], str)
        or len(automatic_emission["definition_sha256"]) != 64
        or any(character not in "0123456789abcdef" for character in automatic_emission["definition_sha256"])
        or checkpoint.get("is_child") is not False
        or checkpoint.get("children")
    ):
        raise ValueError("Automatic native emission requires a root semantic frontier and definition digest.")
    pending: dict[str, list[tuple[Any, Any]]] = {name: list(rows) for name, rows in checkpoint["pending"].items()}
    requests: dict[str, current.PendingHITLRequest] = {}
    for key, record in checkpoint["requests"].items():
        if isinstance(record, current.PendingHITLRequest):
            requests[key] = record
            continue
        if (
            not is_string_keyed_dict(record)
            or set(record)
            != {
                "contract",
                "request_id",
                "source_executor_id",
                "request_data",
                "request_type",
                "response_type",
            }
            or record["contract"] != "semantic-hitl-request/1"
            or record["request_id"] != key
        ):
            raise ValueError("Unsupported semantic HITL request record.")
        requests[key] = current.PendingHITLRequest(
            **{name: value for name, value in record.items() if name != "contract"},
            task_type=current.TaskType.ACTIVITY,
        )
    admitted = checkpoint.get("admittedReplyKeys", [])
    if (
        not is_list(admitted)
        or not all(isinstance(key, str) for key in admitted)
        or len(set(admitted)) != len(admitted)
    ):
        raise ValueError("Invalid semantic admitted-reply keys.")
    for key in admitted:
        if key not in requests:
            raise ValueError("Admitted reply has no authenticated request.")
        request = requests[key]
        matches: list[tuple[list[tuple[Any, Any]], int, dict[str, Any], Any]] = []
        for executor, rows in pending.items():
            for index, (message, origin) in enumerate(rows):
                if (
                    executor == request.source_executor_id
                    and is_string_keyed_dict(message)
                    and message.get("request_id") == key
                    and origin == [f"{current.SOURCE_HITL_RESPONSE}_{key}"]
                ):
                    matches.append((rows, index, message, origin))
        if len(matches) != 1:
            raise ValueError("Admitted reply does not match exactly one authenticated queued message.")
        rows, index, message, origin = matches[0]
        rows[index] = (message, current.HITLMessageSources(origin))
    outputs = list(checkpoint["outputs"])
    shared = None if checkpoint["shared_state"] is None else dict(checkpoint["shared_state"])
    bindings, ids = fan_bindings(workflow)
    if bindings != checkpoint["fanBindings"] or set(ids) != set(checkpoint["fan"]):
        raise ValueError("Target fan-in definition does not match the authorized semantic checkpoint.")
    fan: dict[str, Any] = {ids[name]: defaultdict(list, rows) for name, rows in checkpoint["fan"].items()}
    ledger = current.WorkflowDeliveryLedger(instance_id=ctx.instance_id)
    counter = [checkpoint["child_counter"]]
    iteration = checkpoint["iteration"]
    phase = checkpoint["phase"]
    waiting = list(checkpoint["waiting"])
    publish_events = checkpoint["eventStreaming"] and ctx.supports_event_streaming
    events: list[dict[str, Any]] = list(checkpoint["events"]) if publish_events else []
    children: list[dict[str, Any]] = list(checkpoint.get("children", []))

    def child_executor(identifier: str) -> WorkflowExecutor:
        executor = workflow.executors[identifier]
        if not isinstance(executor, WorkflowExecutor):
            raise ValueError("A semantic child result must name a workflow executor.")
        return executor

    def emit(event_type: str, executor: str) -> None:
        if publish_events:
            events.append({"type": event_type, "executor_id": executor, "iteration": iteration})

    def append_events(result: current.ExecutorResult) -> None:
        if publish_events and result.activity_result:
            captured = result.activity_result.get("events")
            if is_list(captured):
                for event in captured:
                    if not is_string_keyed_dict(event):
                        raise ValueError("An activity event must be an object.")
                    events.append({**event, "iteration": iteration})

    def status(subworkflows: dict[str, Any] | None = None) -> None:
        is_waiting = bool(requests) and not pending and not subworkflows
        value: dict[str, Any] = {"state": "waiting_for_human_input" if is_waiting else "running"}
        if publish_events:
            value["events"] = list(events)
        if is_waiting:
            value["pending_requests"] = {
                key: {
                    "request_id": key,
                    "source_executor_id": request.source_executor_id,
                    "data": request.request_data,
                    "request_type": request.request_type,
                    "response_type": request.response_type,
                }
                for key, request in requests.items()
            }
        if subworkflows:
            value["subworkflows"] = subworkflows
        ctx.set_custom_status(value)

    def emission_snapshot() -> dict[str, Any]:
        return {
            "contract": CONTRACT,
            "routingPolicy": "published-batch-join/1",
            "phase": phase,
            "eventStreaming": checkpoint["eventStreaming"],
            "pending": {executor: list(rows) for executor, rows in pending.items()},
            "outputs": list(outputs),
            "iteration": iteration,
            "child_counter": counter[0],
            "is_child": False,
            "address": dict(checkpoint["address"]),
            "events": list(events),
            "shared_state": None if shared is None else dict(shared),
            "requests": {
                key: {
                    "contract": "semantic-hitl-request/1",
                    **{
                        name: getattr(request, name)
                        for name in (
                            "request_id",
                            "source_executor_id",
                            "request_data",
                            "request_type",
                            "response_type",
                        )
                    },
                }
                for key, request in requests.items()
            },
            "waiting": list(waiting),
            "admittedReplyKeys": list(checkpoint["admittedReplyKeys"]),
            "fanBindings": checkpoint["fanBindings"],
            "fan": {
                key: {source: list(values) for source, values in fan[group_id].items()}
                for key, group_id in ids.items()
            },
            "children": [],
            "notify_parent": None,
        }

    if phase == "native_batch":
        frontier = checkpoint.get("nativeBatch")
        if (
            not is_string_keyed_dict(frontier)
            or set(frontier)
            not in ({"operations", "metadata", "remaining"}, {"operations", "metadata", "remaining", "staged"})
            or children
            or native_child_tasks is not None
            or verify_child is not None
            or child_result_task is not None
            or not is_list(native_batch_tasks)
            or not native_batch_tasks
            or len(native_batch_tasks) != len(frontier["metadata"])
            or len(frontier["metadata"]) != len(frontier["operations"])
        ):
            raise ValueError("Native batch requires its authenticated complete original task frontier.")
        for record, operation in zip(frontier["metadata"], frontier["operations"], strict=True):
            if (
                not is_string_keyed_dict(record)
                or not is_string_keyed_dict(operation)
                or set(record) != {"kind", "executor", "message", "source"}
                or record["kind"] != operation["kind"]
                or record["executor"] not in workflow.executors
            ):
                raise ValueError("Native result metadata does not match its original operation.")
            executor = workflow.executors[record["executor"]]
            if record["kind"] == "entity":
                if (
                    not isinstance(executor, AgentExecutor)
                    or operation["operation"] != "run"
                    or operation["entity"] != f"@dafx-{workflow.name}-{executor.id}@{ctx.instance_id}"
                ):
                    raise ValueError("Original native agent address does not match its authorized graph node.")
            elif record["kind"] == "child":
                if (
                    not isinstance(executor, WorkflowExecutor)
                    or operation["name"] != f"dafx-{executor.workflow.name}"
                    or not operation["instance"].startswith(f"{ctx.instance_id}::{executor.id}::")
                ):
                    raise ValueError("Original native child does not match its authorized graph node.")
            elif record["kind"] == "activity":
                if (
                    isinstance(executor, (AgentExecutor, WorkflowExecutor))
                    or operation["name"] != f"dafx-{workflow.name}-{executor.id}"
                ):
                    raise ValueError("Original native activity does not match its authorized graph node.")
            else:
                raise ValueError("Unsupported original native batch result kind.")
        for row in frontier["remaining"]:
            if (
                not is_list_or_tuple(row)
                or len(row) != 3
                or row[0] not in workflow.executors
                or not isinstance(workflow.executors[row[0]], AgentExecutor)
            ):
                raise ValueError("Remaining sequential work must name an authorized original agent node.")
        raw_results = yield ctx.task_all(native_batch_tasks)
        from ._native_prefix import validate_native_batch_results

        validate_native_batch_results(frontier["operations"], raw_results)
        results: list[current.ExecutorResult] = [
            current.ExecutorResult(
                executor_id=record["executor"],
                output_message=record["output_message"],
                activity_result=record["activity_result"],
                task_type=current.TaskType(record["kind"]),
            )
            for record in frontier.get("staged", [])
        ]
        for raw, record in zip(raw_results, frontier["metadata"], strict=True):
            executor = record["executor"]
            if record["kind"] == "activity":
                result = current.process_activity_result(raw, executor, shared, outputs)
                append_events(result)
            elif record["kind"] == "entity":
                result = published_frontier_agent_result(raw, executor, record["message"])
                emit("executor_completed", executor)
            elif record["kind"] == "child":
                result = current.process_subworkflow_result(raw, child_executor(executor), outputs)
                append_events(result)
                emit("executor_completed", executor)
            else:
                raise ValueError("Unsupported original native batch result kind.")
            results.append(result)
        for executor, message, _origin in frontier["remaining"]:
            response = yield prepare_published_agent(ctx, workflow.name, executor, message)
            results.append(published_frontier_agent_result(response, executor, message))
            emit("executor_completed", executor)
        pending = {}
        for result in results:
            current.collect_hitl_requests(result, requests)
            route_join_result(result, workflow, pending, fan)
        flush_join_fan(workflow, fan, pending)
        status()
        phase = "hitl" if not pending and requests else "prepare"
        if phase == "hitl":
            waiting = list(requests)
        else:
            iteration += 1
    elif native_batch_tasks is not None:
        raise ValueError("Native batch handles require their explicit semantic frontier.")

    if children:
        pending = {}
        addresses: dict[str, dict[str, str]] = {}
        child_values: list[Any] | dict[int, Any]
        for child in children:
            slots = addresses.setdefault(child["executor"], {})
            slots[str(len(slots))] = child["instance"]
        status(addresses)
        if native_child_tasks is not None:
            if verify_child is not None or child_result_task is not None:
                raise ValueError("Native child routing cannot be combined with an operator result transport.")
            if set(native_child_tasks) != {child["instance"] for child in children}:
                raise ValueError("Native child handles do not match the authenticated checkpoint.")
            child_values = yield ctx.task_all([native_child_tasks[child["instance"]] for child in children])
        elif verify_child is None or child_result_task is None:
            raise ValueError("Child handoff requires an authenticated result verifier.")
        else:
            tasks = [child_result_task(child) for child in children]
            remaining = list(range(len(children)))
            child_values = {}
            while remaining:
                completed = yield ctx.task_any([tasks[index] for index in remaining])
                index = next((index for index in remaining if tasks[index] is completed), None)
                if index is None:
                    raise ValueError("Child result race returned a task outside its native batch.")
                child = children[index]
                raw = completed.get_result()
                raw = verify_child(raw, child)
                if raw["child"] != child["instance"]:
                    raise ValueError("Child completion does not match checkpoint.")
                from ._child_outcome import release

                child_value = release(
                    raw,
                    source_task_id=child["source_parent"]["task"],
                )
                child_values[index] = child_value
                remaining.remove(index)
        for index, child in enumerate(children):
            result = current.process_subworkflow_result(
                child_values[index],
                child_executor(child["executor"]),
                outputs,
            )
            append_events(result)
            emit("executor_completed", child["executor"])
            route_join_result(result, workflow, pending, fan)
        flush_join_fan(workflow, fan, pending)
        iteration += 1
        phase = "prepare"

    while pending or requests:
        if phase == "hitl" or (not pending and requests):
            status()
            # Legacy response handlers run only after every captured approval.
            # Current activity admission owns validation and typed reconstruction.
            for key in waiting:
                request = requests[key]
                while True:
                    task = ctx.wait_for_external_event(key)
                    raw = task.get_result() if task.is_complete else (yield task)
                    if isinstance(raw, str):
                        with suppress(json.JSONDecodeError):
                            raw = json.loads(raw)
                    try:
                        current.validate_hitl_response_json(raw)
                        sanitized = strip_pickle_markers(raw)
                        if sanitized is None and raw is not None:
                            raise ValueError("Disallowed checkpoint markers in external reply.")
                    except (TypeError, ValueError):
                        logger.warning("Rejected malformed reply for %s; awaiting replacement.", key)
                        continue
                    break
                current.route_hitl_response(request, sanitized, pending)
            phase = "prepare"
            waiting = []
            iteration += 1
            continue
        if iteration >= workflow.max_iterations:
            raise WorkflowConvergenceException(f"Workflow did not converge after {workflow.max_iterations} iterations.")
        tasks, metadata, remaining = prepare_semantic_batch(
            ctx,
            workflow,
            pending,
            shared,
            counter,
            checkpoint["address"],
            ledger,
        )
        for meta in metadata:
            if meta.task_type in (current.TaskType.AGENT, current.TaskType.SUBWORKFLOW):
                emit("executor_invoked", meta.executor_id)
        for executor, _, _ in remaining:
            emit("executor_invoked", executor)
        results = []
        if tasks:
            addresses = current.index_subworkflows(metadata)
            if addresses:
                status(addresses)
            raw_results = yield ctx.task_all(tasks)
            for raw, meta in zip(raw_results, metadata, strict=True):
                if meta.task_type == current.TaskType.ACTIVITY:
                    result = current.process_activity_result(raw, meta.executor_id, shared, outputs)
                    append_events(result)
                elif meta.task_type == current.TaskType.SUBWORKFLOW:
                    result = current.process_subworkflow_result(
                        raw,
                        child_executor(meta.executor_id),
                        outputs,
                    )
                    append_events(result)
                    emit("executor_completed", meta.executor_id)
                else:
                    result = published_frontier_agent_result(raw, meta.executor_id, meta.message)
                    emit("executor_completed", meta.executor_id)
                results.append(result)
        for executor, message, _origin in remaining:
            response = yield prepare_published_agent(ctx, workflow.name, executor, message)
            results.append(published_frontier_agent_result(response, executor, message))
            emit("executor_completed", executor)
        pending = {}
        for result in results:
            current.collect_hitl_requests(result, requests)
            route_join_result(result, workflow, pending, fan)
        flush_join_fan(workflow, fan, pending)
        status()
        if not pending and requests:
            phase = "hitl"
            waiting = list(requests)
        else:
            iteration += 1
        if automatic_emission is not None and (pending or requests):
            from ._format import encode_checkpoint_document

            payload = encode_checkpoint_document(
                {
                    "contract": "checkpoint-only-start/1",
                    "state": {
                        "contract": "native-automatic-checkpoint/1",
                        "definition_sha256": automatic_emission["definition_sha256"],
                        "workflow_name": workflow.name,
                        "semantic": serialize_value(emission_snapshot()),
                    },
                }
            )
            create_checkpoint = getattr(ctx, "create_dispatch_checkpoint", None)
            if not callable(create_checkpoint):
                raise RuntimeError("Automatic checkpoint emission requires the native replay-fence SDK proposal.")
            yield create_checkpoint(json.loads(payload))
            return outputs
    if checkpoint["is_child"]:
        return {SUBWORKFLOW_RESULT_KEY: True, "outputs": outputs, "events": events}
    return outputs  # noqa: B901
