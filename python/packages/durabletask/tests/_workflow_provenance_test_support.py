# Copyright (c) Microsoft. All rights reserved.

"""Workflow provenance graphs, SDK parent histories and benign pickle probes."""

import json
import logging
from collections.abc import Callable
from typing import Any

from agent_framework import Executor, Workflow, WorkflowBuilder, WorkflowContext, WorkflowExecutor, handler
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.worker import TaskHubGrpcWorker, _ActivityExecutor, _OrchestrationExecutor

from agent_framework_durabletask import DurableAIAgentWorker
from agent_framework_durabletask._workflows.serialization import (
    SUBWORKFLOW_ADDRESS_KEY,
    SUBWORKFLOW_INPUT_KEY,
    serialize_value,
)

_LOGGER = logging.getLogger(__name__)
_PICKLE_CALLS: list[str] = []


class _PickleProbe:
    def __init__(self, value: str) -> None:
        self.value = value

    def __reduce__(self) -> tuple[Callable[..., Any], tuple[str]]:
        return _restore_probe, (self.value,)


def _restore_probe(value: str) -> _PickleProbe:
    _PICKLE_CALLS.append(value)
    return _PickleProbe(value)


class _Echo(Executor):
    def __init__(self) -> None:
        super().__init__(id="echo")
        self.seen: list[Any] = []

    @handler(input=object, workflow_output=object)
    async def handle(self, message: Any, ctx: WorkflowContext) -> None:
        self.seen.append(message)
        await ctx.yield_output(message)


class _Produce(Executor):
    @handler(input=dict, output=_PickleProbe)
    async def handle(self, message: dict[str, Any], ctx: WorkflowContext[_PickleProbe]) -> None:
        await ctx.send_message(_PickleProbe(message["value"]))


def _leaf() -> tuple[Workflow, _Echo]:
    echo = _Echo()
    return WorkflowBuilder(name="provenance-leaf", start_executor=echo, output_from=[echo]).build(), echo


def _tree() -> tuple[Workflow, _Echo]:
    leaf, echo = _leaf()
    grand = WorkflowExecutor(leaf, id="grand hop", allow_direct_output=True)
    middle = WorkflowBuilder(name="provenance-middle", start_executor=grand, output_from=[grand]).build()
    # These IDs are allowed by existing durable naming rules. A guard must not
    # introduce an alphanumeric-only policy or split executor IDs on '::'.
    child = WorkflowExecutor(middle, id="sub:: 世界", allow_direct_output=True)
    seed = _Produce(id="seed")
    root = (
        WorkflowBuilder(name="provenance-root", start_executor=seed, output_from=[child]).add_edge(seed, child).build()
    )
    return root, echo


def _internal(value: Any) -> dict[str, Any]:
    return {
        SUBWORKFLOW_INPUT_KEY: value,
        SUBWORKFLOW_ADDRESS_KEY: {
            "root_instance_id": "root",
            "root_workflow_name": "provenance-root",
            "request_path_prefix": "child~0~",
        },
    }


def _invalid_child(case: str) -> dict[str, Any]:
    value = _internal(serialize_value(_PickleProbe("rejected")))
    address = value[SUBWORKFLOW_ADDRESS_KEY]
    if case == "input-only":
        del value[SUBWORKFLOW_ADDRESS_KEY]
    elif case == "address-only":
        del value[SUBWORKFLOW_INPUT_KEY]
    elif case == "address-null":
        value[SUBWORKFLOW_ADDRESS_KEY] = None
    elif case == "root-mismatch":
        address["root_instance_id"] = "unrelated"
    elif case == "prefix-mismatch":
        address["request_path_prefix"] = "other~0~"
    elif case == "prefix-empty":
        address["request_path_prefix"] = ""
    elif case == "prefix-incomplete":
        address["request_path_prefix"] = "child~0"
    elif case == "ordinal-negative":
        address["request_path_prefix"] = "child~-1~"
    elif case == "ordinal-noncanonical":
        address["request_path_prefix"] = "child~00~"
    elif case == "workflow-empty":
        address["root_workflow_name"] = " "
    else:
        raise AssertionError(case)
    return value


_INVALID_CHILD_CASES = [
    "input-only",
    "address-only",
    "address-null",
    "root-mismatch",
    "prefix-mismatch",
    "prefix-empty",
    "prefix-incomplete",
    "ordinal-negative",
    "ordinal-noncanonical",
    "workflow-empty",
]


class _DTStarts:
    def __init__(self, workflow: Workflow) -> None:
        self.worker: Any = TaskHubGrpcWorker(host_address="localhost:1")
        DurableAIAgentWorker(self.worker, deployment_mode="isolated_v2").configure_workflow(workflow)
        self.histories: dict[str, list[Any]] = {}
        self.names: dict[str, str] = {}

    def replay(self, instance: str, *events: Any) -> Any:
        new = [helpers.new_orchestrator_started_event(), *events]
        result = _OrchestrationExecutor(self.worker._registry, _LOGGER, self.worker._data_converter).execute(
            instance, self.histories[instance], new
        )
        self.histories[instance].extend(new)
        return result

    def start(self, name: str, instance: str, wire: Any, parent: str | None = None, task_id: int = 1) -> Any:
        self.histories[instance] = []
        self.names[instance] = name
        started = helpers.new_execution_started_event(name, instance, json.dumps(wire))
        if parent is not None:
            started.executionStarted.parentInstance.CopyFrom(
                pb.ParentInstanceInfo(
                    taskScheduledId=task_id,
                    name=helpers.get_string_value(self.names.get(parent, "native-parent")),
                    orchestrationInstance=pb.OrchestrationInstance(instanceId=parent),
                )
            )
        return self.replay(instance, started)

    def complete_activity(self, instance: str, action: Any) -> Any:
        assert action.HasField("scheduleTask")
        task = action.scheduleTask
        self.histories[instance].append(helpers.new_task_scheduled_event(action.id, task.name, task.input.value))
        result = _ActivityExecutor(self.worker._registry, _LOGGER, self.worker._data_converter).execute(
            instance, task.name, action.id, task.input.value
        )
        return self.replay(instance, helpers.new_task_completed_event(action.id, result))

    def child(self, parent: str, action: Any) -> tuple[str, Any]:
        assert action.HasField("createSubOrchestration")
        task = action.createSubOrchestration
        self.histories[parent].append(
            helpers.new_sub_orchestration_created_event(action.id, task.name, task.instanceId, task.input.value)
        )
        return task.instanceId, self.start(task.name, task.instanceId, json.loads(task.input.value), parent, action.id)


def _only_action(result: Any, kind: str) -> Any:
    assert len(result.actions) == 1
    action = result.actions[0]
    assert action.HasField(kind)
    return action
