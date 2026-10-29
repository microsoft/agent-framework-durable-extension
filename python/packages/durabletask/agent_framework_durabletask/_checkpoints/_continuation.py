# Copyright (c) Microsoft. All rights reserved.

"""Current-only continuation for newly authorized sequential workflow work."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Generator
from copy import deepcopy
from typing import Any

from agent_framework import AgentExecutor, Workflow

from agent_framework_durabletask._workflows import orchestrator as current
from agent_framework_durabletask._workflows.context import WorkflowOrchestrationContext

from ._target import (
    CONTRACT,
    fan_bindings,
    flush_join_fan,
    prepare_published_agent,
    published_frontier_agent_result,
    route_join_result,
    run,
)
from ._types import is_list, is_string_keyed_dict


def continue_checkpoint(
    ctx: WorkflowOrchestrationContext,
    workflow: Workflow,
    checkpoint: dict[str, Any],
    *,
    automatic_emission: dict[str, str] | None = None,
    **native_handles: Any,
) -> Generator[Any, Any, list[Any] | dict[str, Any]]:
    """Continue sequential future work without replaying already applied results.

    The authenticated mechanical prefix must have completed first. Only the
    remaining agent calls are new work. Routing waits until the entire original
    superstep's results exist. Ordinary phases delegate to the semantic target.
    A v5 mixed frontier supplies the whole original batch of real native task
    handles, including its now-dispatched suffix, through native_batch_tasks.
    """
    if checkpoint.get("phase") == "native_mixed_batch":
        state = deepcopy(checkpoint)
        state["phase"] = "native_batch"
        state["nativeBatch"].pop("accepted_count")
        return (yield from run(
            ctx,
            workflow,
            state,
            automatic_emission=automatic_emission,
            **native_handles,
        ))
    if checkpoint.get("phase") != "sequential_prepare":
        return (yield from run(
            ctx,
            workflow,
            checkpoint,
            automatic_emission=automatic_emission,
            **native_handles,
        ))
    if native_handles:
        raise ValueError("Unaccepted sequential work cannot have accepted native handles.")
    state = deepcopy(checkpoint)
    if state.get("contract") != CONTRACT or state.get("routingPolicy") != "published-batch-join/1":
        raise ValueError("Unsupported sequential semantic checkpoint contract or routing policy.")
    if not isinstance(state.get("eventStreaming"), bool):
        raise ValueError("Sequential checkpoint requires an explicit source event publication policy.")
    if state.get("admittedReplyKeys", []):
        raise ValueError("Sequential source frontier cannot contain partially admitted HITL replies.")
    plan: dict[str, Any] = state.pop("sequentialPrepare")
    if (
        set(plan) != {"remaining", "staged"}
        or not is_list(plan["remaining"])
        or not plan["remaining"]
        or state.get("children") != []
    ):
        raise ValueError("Invalid original sequential future-work plan.")
    for executor, _message, _origin in plan["remaining"]:
        if executor not in workflow.executors or not isinstance(workflow.executors[executor], AgentExecutor):
            raise ValueError("Sequential future work must name an original agent node.")
    bindings, ids = fan_bindings(workflow)
    if bindings != state["fanBindings"] or set(ids) != set(state["fan"]):
        raise ValueError("Original fan-in definition changed.")
    requests: dict[str, current.PendingHITLRequest] = {}
    for key, record in state["requests"].items():
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
            raise ValueError("Unsupported sequential semantic HITL request record.")
        requests[key] = current.PendingHITLRequest(
            **{name: value for name, value in record.items() if name != "contract"},
            task_type=current.TaskType.ACTIVITY,
        )
    results = [
        current.ExecutorResult(
            executor_id=record["executor"],
            output_message=record["output_message"],
            activity_result=record["activity_result"],
            task_type=current.TaskType(record["kind"]),
        )
        for record in plan["staged"]
    ]
    for executor, message, _origin in plan["remaining"]:
        response = yield prepare_published_agent(ctx, workflow.name, executor, message)
        results.append(published_frontier_agent_result(response, executor, message))
        if state["eventStreaming"]:
            state["events"].append({
                "type": "executor_completed",
                "executor_id": executor,
                "iteration": state["iteration"],
            })
    fan = {ids[key]: defaultdict(list, state["fan"][key]) for key in ids}
    pending: dict[str, list[tuple[Any, Any]]] = {}
    for result in results:
        current.collect_hitl_requests(result, requests)
        route_join_result(result, workflow, pending, fan)
    flush_join_fan(workflow, fan, pending)
    state["pending"], state["fan"] = pending, {key: fan[value] for key, value in ids.items()}
    state["requests"] = {
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
    }
    state["phase"] = "hitl" if not pending and requests else "prepare"
    state["waiting"] = list(requests) if state["phase"] == "hitl" else []
    if state["phase"] != "hitl":
        state["iteration"] += 1
    return (  # noqa: B901
        yield from run(
            ctx,
            workflow,
            state,
            automatic_emission=automatic_emission,
        )
    )
