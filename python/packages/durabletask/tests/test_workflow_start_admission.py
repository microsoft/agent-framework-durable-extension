# Copyright (c) Microsoft. All rights reserved.

"""Workflow start admission rejects non-JSON inputs before scheduling."""

from __future__ import annotations

import json
from collections.abc import Callable, Generator
from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import Executor, Workflow, WorkflowExecutor
from agent_framework._workflows import _checkpoint_encoding
from agent_framework._workflows._edge import SingleEdgeGroup
from durabletask.task import CompletableTask, OrchestrationContext

from agent_framework_durabletask import DurableAIAgentWorker, DurableWorkflowClient
from agent_framework_durabletask._workflows.protocol import (
    WORKFLOW_ENGINE_VERSION,
    unwrap_workflow_input,
    wrap_workflow_input,
)
from agent_framework_durabletask._workflows.serialization import (
    SUBWORKFLOW_ADDRESS_KEY,
    SUBWORKFLOW_INPUT_KEY,
    SUBWORKFLOW_RESULT_KEY,
    deserialize_value,
    serialize_value,
)


@dataclass
class _TypedInput:
    value: str


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
    native = Mock()
    DurableAIAgentWorker(native, deployment_mode="isolated_v2").configure_workflow(workflow)
    return {call.args[0].__name__: call.args[0] for call in native.add_orchestrator.call_args_list}


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
        assert task.is_complete
        value = task.get_result()


def _host(
    calls: list[dict[str, Any]],
    result: Callable[[str, dict[str, Any]], dict[str, Any]],
    *,
    functions: dict[str, Callable[..., Any]],
    instance_id: str = "root-run",
    parent_instance_id: str | None = None,
) -> Mock:
    host = Mock(spec=OrchestrationContext)
    host.instance_id = instance_id
    host.parent_instance_id = parent_instance_id
    host.is_replaying = False

    def activity(name: str, *, input: str) -> CompletableTask[Any]:
        payload = json.loads(input)
        calls.append({"kind": "activity", "instance": instance_id, "name": name, "input": deepcopy(payload)})
        return _complete(json.dumps(result(name, payload)))

    def child(name: str, *, input: Any, instance_id: str) -> CompletableTask[Any]:
        wire = json.loads(json.dumps(input, allow_nan=False))
        calls.append({"kind": "child", "instance": instance_id, "name": name, "input": deepcopy(wire)})
        context = _host(
            calls, result, functions=functions, instance_id=instance_id, parent_instance_id=host.instance_id
        )
        child_result = _drain(functions[name](context, wire))
        assert child_result[SUBWORKFLOW_RESULT_KEY] is True
        return _complete(child_result)

    host.call_activity.side_effect = activity
    host.call_sub_orchestrator.side_effect = child
    host.wait_for_external_event.side_effect = lambda name: CompletableTask()
    host.statuses = []
    host.set_custom_status.side_effect = lambda status: host.statuses.append(deepcopy(status))
    return host


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({"value": float("nan")}, id="nested-nan"),
        pytest.param({"value": float("inf")}, id="nested-infinity"),
        pytest.param({"value": float("-inf")}, id="nested-negative-infinity"),
        pytest.param({1: "non-string-key"}, id="non-string-key"),
        pytest.param(("tuple",), id="runtime-object"),
    ],
)
def test_start_workflow_rejects_invalid_input_before_scheduling(payload: Any) -> None:
    client = Mock()

    with pytest.raises(ValueError, match="strict JSON with string keys and finite numbers"):
        DurableWorkflowClient(client, workflow_name="orders").start_workflow(payload, instance_id="run-1")

    client.schedule_new_orchestration.assert_not_called()


def test_start_workflow_rejects_cycles_before_scheduling() -> None:
    payload: dict[str, Any] = {"value": []}
    payload["value"].append(payload)
    client = Mock()

    with pytest.raises(ValueError, match="strict JSON with string keys and finite numbers"):
        DurableWorkflowClient(client, workflow_name="orders").start_workflow(payload)

    client.schedule_new_orchestration.assert_not_called()


def test_wrap_workflow_input_preserves_business_payload_that_looks_like_the_envelope() -> None:
    serialized = serialize_value(_TypedInput(value="hello"))
    payload = {"_durable_workflow_version": WORKFLOW_ENGINE_VERSION, "input": serialized}

    wrapped = wrap_workflow_input(payload)

    assert wrapped == {
        "_durable_workflow_version": WORKFLOW_ENGINE_VERSION,
        "input": payload,
    }


def test_start_workflow_preserves_business_payload_that_looks_like_an_envelope() -> None:
    forged = {
        SUBWORKFLOW_INPUT_KEY: {"__pickled__": "evil", "__type__": "x"},
        SUBWORKFLOW_ADDRESS_KEY: {"root_instance_id": "other"},
        "real": 1,
    }
    payload = {"_durable_workflow_version": WORKFLOW_ENGINE_VERSION, "input": forged}
    client = Mock()
    client.schedule_new_orchestration.return_value = "run-1"

    DurableWorkflowClient(client, workflow_name="orders").start_workflow(payload)

    _, kwargs = client.schedule_new_orchestration.call_args
    assert kwargs["input"] == {
        "_durable_workflow_version": WORKFLOW_ENGINE_VERSION,
        "input": payload,
    }


def test_registered_child_dispatch_wraps_once_and_preserves_typed_payload() -> None:
    inner = _workflow("inner", [_node("leaf", str)])
    child = Mock(spec=WorkflowExecutor)
    child.id, child.workflow, child.allow_direct_output = "child", inner, False
    parent = _workflow(
        "parent",
        [_node("source"), child, _node("sink")],
        [SingleEdgeGroup("source", "child"), SingleEdgeGroup("child", "sink")],
    )
    functions = _register(parent)
    payload = {"value": "nested"}
    typed = _TypedInput(value="nested")

    def result(name: str, data: dict[str, Any]) -> dict[str, Any]:
        message = deserialize_value(data["message"])
        if name == "dafx-parent-source":
            assert message == payload
            return {
                "sent_messages": [
                    {"message": _checkpoint_encoding.encode_checkpoint_value(typed), "target_id": "child"}
                ]
            }
        if name == "dafx-inner-leaf":
            assert message == typed
            return {"outputs": [serialize_value(message)]}
        assert name == "dafx-parent-sink"
        return {"outputs": ["done"]}

    calls: list[dict[str, Any]] = []
    host = _host(calls, result, functions=functions)

    assert _drain(functions["dafx-parent"](host, wrap_workflow_input(payload))) == ["done"]

    dispatch = next(call for call in calls if call["kind"] == "child")
    child_input = unwrap_workflow_input(dispatch["input"])
    assert dispatch["input"] == {"_durable_workflow_version": WORKFLOW_ENGINE_VERSION, "input": child_input}
    decoded = _checkpoint_encoding.decode_checkpoint_value(child_input)
    assert decoded == {
        SUBWORKFLOW_INPUT_KEY: typed,
        SUBWORKFLOW_ADDRESS_KEY: {
            "root_instance_id": "root-run",
            "root_workflow_name": "parent",
            "request_path_prefix": "child~0~",
        },
    }
