# Copyright (c) Microsoft. All rights reserved.

"""Durable Task registration and SDK task helpers for workflow boundary tests."""

from __future__ import annotations

import json
from collections.abc import Callable, Generator
from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from unittest.mock import Mock

from _workflow_test_support import create_registration_worker
from agent_framework import Executor, Workflow
from durabletask.task import CompletableTask, OrchestrationContext

from agent_framework_durabletask import DurableAIAgentWorker, DurableWorkflowClient
from agent_framework_durabletask._json_payload import JsonPayload
from agent_framework_durabletask._workflows.serialization import SUBWORKFLOW_RESULT_KEY

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


def _register(workflow: Any) -> dict[str, Callable[..., Any]]:
    # Use configure_workflow, not a reimplementation of its generated closure.
    native = create_registration_worker()
    DurableAIAgentWorker(native, deployment_mode="isolated_v2").configure_workflow(workflow)
    native.add_entity.assert_not_called()
    return {call.args[0].__name__: call.args[0] for call in native.add_orchestrator.call_args_list}


def _start(payload: Any, name: str = "protocol") -> dict[str, Any]:
    client = Mock()
    client.schedule_new_orchestration.return_value = "root-run"
    assert (
        DurableWorkflowClient(client, workflow_name=name).start_workflow(payload, instance_id="root-run") == "root-run"
    )
    client.schedule_new_orchestration.assert_called_once()
    call = client.schedule_new_orchestration.call_args
    assert call is not None
    assert call.args == (f"dafx-{name}",)
    assert call.kwargs["instance_id"] == "root-run"
    return json.loads(json.dumps(call.kwargs["input"], allow_nan=False))


def _complete(value: Any) -> CompletableTask[Any]:
    task: CompletableTask[Any] = CompletableTask()
    task.complete(value)
    return task


def _drain(generator: Generator[Any, Any, Any], value: Any = None) -> Any:
    while True:
        try:
            task = generator.send(value)
        except StopIteration as completed:
            return completed.value
        assert task.is_complete, "Use explicit event completion for a paused generator"
        value = task.get_result()


def _host(
    calls: list[dict[str, Any]],
    result: Callable[[str, dict[str, Any]], dict[str, Any]] | None = None,
    *,
    functions: dict[str, Callable[..., Any]] | None = None,
    instance_id: str = "root-run",
    parent_instance_id: str | None = None,
    replay: bool = False,
) -> Mock:
    host = Mock(spec=OrchestrationContext)
    host.instance_id = instance_id
    host.parent_instance_id = parent_instance_id
    host.is_replaying = replay

    def activity(name: str, *, input: str) -> CompletableTask[Any]:
        payload = json.loads(input)
        calls.append({"kind": "activity", "instance": instance_id, "name": name, "input": deepcopy(payload)})
        response = {"outputs": ["done"]} if result is None else result(name, payload)
        return _complete(json.dumps(response))

    def child(name: str, *, input: Any, instance_id: str, return_type: Any) -> CompletableTask[Any]:
        assert return_type is JsonPayload
        assert functions is not None
        wire = json.loads(json.dumps(input))
        calls.append({"kind": "child", "instance": instance_id, "name": name, "input": deepcopy(wire)})
        context = _host(
            calls,
            result,
            functions=functions,
            instance_id=instance_id,
            parent_instance_id=host.instance_id,
            replay=replay,
        )
        child_result = _drain(functions[name](context, wire))
        assert child_result[SUBWORKFLOW_RESULT_KEY] is True
        return _complete(child_result)

    def wait(name: str, *, data_type: Any) -> CompletableTask[Any]:
        assert data_type is JsonPayload
        return CompletableTask()

    host.call_activity.side_effect = activity
    host.call_sub_orchestrator.side_effect = child
    host.wait_for_external_event.side_effect = wait
    # Copy when published, rather than observing later mutation of the same dict/list.
    host.statuses = []
    host.set_custom_status.side_effect = lambda status: host.statuses.append(deepcopy(status))
    return host
