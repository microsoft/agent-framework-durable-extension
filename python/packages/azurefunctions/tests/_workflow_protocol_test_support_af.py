# Copyright (c) Microsoft. All rights reserved.

"""Azure Functions registration and SDK task helpers for workflow boundary tests."""

from __future__ import annotations

import json
from collections.abc import Callable, Generator
from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock, Mock

import azure.durable_functions as df
import azure.functions as func
from agent_framework import Executor, Workflow
from agent_framework_durabletask._workflows.serialization import SUBWORKFLOW_RESULT_KEY
from azure.durable_functions.models.actions.NoOpAction import NoOpAction
from azure.durable_functions.models.ReplaySchema import ReplaySchema
from azure.durable_functions.models.Task import AtomicTask, WhenAllTask

from agent_framework_azurefunctions import AgentFunctionApp

_VERSION = "_durable_workflow_version"
_CONTROL = {"input": "application control", "items": [0, False, None, "世界"]}
_FORGED_ADDRESS = {
    "root_instance_id": "other-run",
    "root_workflow_name": "other-workflow",
    "request_path_prefix": "forged~9~",
}
_UNTRUSTED = {"__pickled__": "not-trusted-checkpoint-data", "__type__": "builtins:str"}


@dataclass
class _TypedInput:
    input: str
    control: dict[str, Any]


def _node(name: str = "start", input_type: type | None = None) -> Any:
    node = Mock(spec=Executor)
    node.id = name
    node.input_types = [] if input_type is None else [input_type]
    return node


def _workflow(name: str = "protocol", nodes: list[Any] | None = None, edges: list[Any] | None = None) -> Any:
    nodes = [_node()] if nodes is None else nodes
    workflow = Mock(spec=Workflow)
    workflow.name = name
    workflow.start_executor_id = nodes[0].id
    workflow.executors = {node.id: node for node in nodes}
    workflow.edge_groups = [] if edges is None else edges
    workflow.max_iterations = 10
    return workflow


def _register(workflow: Any) -> tuple[dict[str, Callable[..., Any]], Callable[..., Any]]:
    app = AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")
    functions = {function.get_function_name(): function for function in app.get_functions()}
    orchestrators: dict[str, Callable[..., Any]] = {}
    starters: list[Callable[..., Any]] = []
    for name, function in functions.items():
        assert name is not None
        trigger = function.get_trigger()
        assert trigger is not None
        binding = trigger.get_dict_repr()
        user_function: Any = function.get_user_function()
        assert user_function is not None
        if binding["type"] == "orchestrationTrigger":
            # SDK metadata exposes the real registered generator, without replacing decorators.
            orchestrators[name] = user_function.orchestrator_function
        elif binding["type"] == "httpTrigger" and binding["route"] == f"workflow/{workflow.name}/run":
            starters.append(user_function.client_function)
    assert len(starters) == 1
    return orchestrators, starters[0]


async def _start(starter: Callable[..., Any], payload: Any, name: str = "protocol") -> dict[str, Any]:
    request = func.HttpRequest(
        method="POST",
        url=f"https://example.test/api/workflow/{name}/run",
        headers={"Content-Type": "application/json"},
        params={"runId": "root-run"},
        body=json.dumps(payload, allow_nan=False).encode("utf-8"),
    )
    client = AsyncMock(spec=df.DurableOrchestrationClient)
    client.start_new.return_value = "root-run"
    response = await starter(request, client)
    assert response.status_code == 202
    client.start_new.assert_awaited_once()
    invocation = client.start_new.await_args
    assert invocation is not None
    assert invocation.args == (f"dafx-{name}",)
    assert invocation.kwargs["instance_id"] == "root-run"
    return json.loads(json.dumps(invocation.kwargs["client_input"], allow_nan=False))


def _complete(value: Any) -> AtomicTask:
    task = AtomicTask(0, NoOpAction())
    task.set_value(is_error=False, value=value)
    return task


def _drain(generator: Generator[Any, Any, Any], value: Any = None) -> Any:
    while True:
        try:
            task = generator.send(value)
        except StopIteration as completed:
            return completed.value
        assert task.is_completed, "Use explicit event completion for a paused generator"
        value = task.result


def _host(
    wire: Any,
    calls: list[dict[str, Any]],
    result: Callable[[str, dict[str, Any]], dict[str, Any]] | None = None,
    *,
    functions: dict[str, Callable[..., Any]] | None = None,
    instance_id: str = "root-run",
    parent_instance_id: str | None = None,
    replay: bool = False,
) -> Mock:
    host = Mock(spec=df.DurableOrchestrationContext)
    host._input = json.dumps(wire)
    host.get_input.side_effect = AssertionError("Generated workflow starts must not use SDK custom decoding")
    host.instance_id = instance_id
    host.parent_instance_id = parent_instance_id
    host.is_replaying = replay

    def activity(name: str, input: str) -> AtomicTask:
        payload = json.loads(input)
        calls.append({"kind": "activity", "instance": instance_id, "name": name, "input": deepcopy(payload)})
        response = {"outputs": ["done"]} if result is None else result(name, payload)
        return _complete(json.dumps(response))

    def child(name: str, *, input_: Any, instance_id: str) -> AtomicTask:
        assert functions is not None
        child_wire = json.loads(json.dumps(input_))
        calls.append({"kind": "child", "instance": instance_id, "name": name, "input": deepcopy(child_wire)})
        context = _host(
            child_wire,
            calls,
            result,
            functions=functions,
            instance_id=instance_id,
            parent_instance_id=host.instance_id,
            replay=replay,
        )
        child_result = _drain(functions[name](context))
        assert child_result[SUBWORKFLOW_RESULT_KEY] is True
        return _complete(child_result)

    host.call_activity.side_effect = activity
    host.call_sub_orchestrator.side_effect = child
    host.task_all.side_effect = lambda tasks: WhenAllTask(tasks, ReplaySchema.V1)
    host.wait_for_external_event.side_effect = lambda name: AtomicTask(name, NoOpAction())
    host.statuses = []
    host.set_custom_status.side_effect = lambda status: host.statuses.append(deepcopy(status))
    return host
