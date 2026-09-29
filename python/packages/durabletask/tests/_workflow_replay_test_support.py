# Copyright (c) Microsoft. All rights reserved.

"""Registered producers and service-shaped histories for cold SDK replay tests."""

import json
import logging
from collections import defaultdict
from collections.abc import Callable
from copy import deepcopy
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import Workflow
from durabletask.client import TaskHubGrpcClient
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.worker import TaskHubGrpcWorker, _ActivityExecutor, _OrchestrationExecutor

from agent_framework_durabletask import DurableAIAgentWorker, DurableWorkflowClient

_LOGGER = logging.getLogger(__name__)


def _worker(workflow: Workflow) -> Any:
    native = TaskHubGrpcWorker(host_address="localhost:1")
    DurableAIAgentWorker(native, deployment_mode="isolated_v2").configure_workflow(workflow)
    return native


def _replay(native: Any, instance: str, old: list[Any], new: list[Any] | None = None) -> Any:
    # A fresh SDK executor/context on every episode, not a hand-driven generator
    # whose is_replaying flag remains constant through the whole run.
    return _OrchestrationExecutor(native._registry, _LOGGER, native._data_converter).execute(
        instance, old, [helpers.new_orchestrator_started_event()] if new is None else new
    )


def _af_replay(history: list[Any], workflow: Workflow, *, instance: str = "audit-siblings") -> dict[str, Any]:
    af = pytest.importorskip("agent_framework_azurefunctions")
    from azure.durable_functions import DurableOrchestrationContext
    from azure.durable_functions.models.ReplaySchema import ReplaySchema
    from azure.durable_functions.models.TaskOrchestrationExecutor import TaskOrchestrationExecutor
    from google.protobuf.json_format import MessageToDict

    started = next(event.executionStarted for event in history if event.HasField("executionStarted"))
    assert started.orchestrationInstance.instanceId == instance
    # Preserve only service-shaped parent metadata already in the source event.
    # Missing metadata stays missing, even when the input claims to be a child.
    parent_instance_id = None
    if started.HasField("parentInstance") and started.parentInstance.HasField("orchestrationInstance"):
        parent_instance_id = started.parentInstance.orchestrationInstance.instanceId

    kinds = {
        "orchestratorStarted": 12,
        "executionStarted": 0,
        "taskScheduled": 4,
        "taskCompleted": 5,
        "taskFailed": 6,
        "eventRaised": 15,
        "subOrchestrationInstanceCreated": 7,
        "subOrchestrationInstanceCompleted": 8,
        "subOrchestrationInstanceFailed": 9,
    }
    rows: list[dict[str, Any]] = []
    for original in history:
        row = MessageToDict(original)
        kind = next(key for key in kinds if key in row)
        event: dict[str, Any] = {
            "EventType": kinds[kind],
            "EventId": row["eventId"] - 1 if row["eventId"] >= 1 else -1,
            "IsPlayed": True,
            "Timestamp": row.get("timestamp", "2026-09-20T23:00:00Z"),
            "Version": None,
        }
        for key, value in row[kind].items():
            if key == "taskScheduledId":
                event["TaskScheduledId"] = value - 1
            elif key in ("name", "input", "result"):
                event[key.capitalize()] = value
            elif key == "instanceId":
                event["InstanceId"] = value
            elif key == "failureDetails":
                event["Reason"] = value["errorMessage"]
                event["Details"] = value["errorType"]
        rows.append(event)
    rows.append({"EventType": 12, "EventId": -1, "IsPlayed": False, "Timestamp": "2026-09-20T23:00:00Z"})
    app = af.AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")
    function = next(
        item.get_user_function().orchestrator_function
        for item in app.get_functions()
        if item.get_function_name() == started.name
    )
    context = DurableOrchestrationContext(
        rows,
        instanceId=instance,
        isReplaying=True,
        parentInstanceId=parent_instance_id,
        input=started.input.value if started.HasField("input") else None,
        upperSchemaVersion=ReplaySchema.V3.value,
    )
    # Verify the SDK-owned raw field without replacing it or invoking custom decoding.
    assert vars(context)["_input"] == (started.input.value if started.HasField("input") else None)
    return json.loads(TaskOrchestrationExecutor().execute(context, context.histories, function))


class _Episodes:
    def __init__(self, workflow: Workflow) -> None:
        self.worker = _worker(workflow)
        self.histories: dict[str, list[Any]] = {}
        self.actions: dict[str, dict[int, Any]] = defaultdict(dict)
        self.statuses: dict[str, dict[str, Any]] = {}
        self.names: dict[str, str] = {}
        self.parents: dict[str, tuple[str, int]] = {}
        self.completions: dict[str, Any] = {}
        self.queued: list[tuple[str, Any]] = []
        self.executed: list[tuple[str, int, dict[str, Any]]] = []
        native = Mock(spec=TaskHubGrpcClient)
        native.schedule_new_orchestration.side_effect = self.start
        native.get_orchestration_state.side_effect = self.state
        native.raise_orchestration_event.side_effect = self.signal
        self.native = native
        self.client = DurableWorkflowClient(native, workflow_name=workflow.name)

    def start(self, name: str, *, input: Any, instance_id: str, **kwargs: Any) -> str:
        assert instance_id not in self.histories
        self.histories[instance_id] = []
        self.names[instance_id] = name
        started = helpers.new_execution_started_event(name, instance_id, json.dumps(input))
        if instance_id in self.parents:
            # Only createSubOrchestration actions populate this dispatch map.
            # Model service metadata absent from the SDK's test event helper,
            # never infer parentage from input markers or an instance ID shape.
            parent, task_id = self.parents[instance_id]
            started.executionStarted.parentInstance.CopyFrom(
                pb.ParentInstanceInfo(
                    taskScheduledId=task_id,
                    name=helpers.get_string_value(self.names[parent]),
                    orchestrationInstance=pb.OrchestrationInstance(instanceId=parent),
                )
            )
        self.episode(instance_id, started)
        return instance_id

    def episode(self, instance: str, *events: Any) -> Any:
        new = [helpers.new_orchestrator_started_event(), *events]
        result = _replay(self.worker, instance, self.histories[instance], new)
        self.histories[instance].extend(new)
        if result.encoded_custom_status is not None:
            self.statuses[instance] = json.loads(result.encoded_custom_status)
        for action in result.actions:
            if action.HasField("scheduleTask"):
                assert action.id not in self.actions[instance]
                self.actions[instance][action.id] = action
                scheduled = action.scheduleTask
                self.histories[instance].append(
                    helpers.new_task_scheduled_event(action.id, scheduled.name, scheduled.input.value)
                )
            elif action.HasField("createSubOrchestration"):
                child = action.createSubOrchestration
                self.histories[instance].append(
                    helpers.new_sub_orchestration_created_event(
                        action.id, child.name, child.instanceId, child.input.value
                    )
                )
                self.parents[child.instanceId] = (instance, action.id)
                self.start(child.name, input=json.loads(child.input.value), instance_id=child.instanceId)
            else:
                assert action.HasField("completeOrchestration")
                completion = action.completeOrchestration
                self.completions[instance] = completion
                if instance in self.parents:
                    parent, task_id = self.parents[instance]
                    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
                    self.queued.append((
                        parent,
                        helpers.new_sub_orchestration_completed_event(task_id, completion.result.value),
                    ))
        return result

    def state(self, instance: str) -> Any:
        if instance not in self.histories:
            return None
        return SimpleNamespace(
            name=self.names[instance], serialized_custom_status=json.dumps(self.statuses.get(instance, {}))
        )

    def signal(self, instance: str, *, event_name: str, data: Any) -> None:
        self.queued.append((instance, helpers.new_event_raised_event(event_name, json.dumps(data))))

    def flush(self) -> None:
        for _ in range(64):
            if not self.queued:
                return
            instance, event = self.queued.pop(0)
            self.episode(instance, event)
        pytest.fail("Exceeded bounded event delivery")

    def complete(self, instance: str, task_id: int, *, before_ack: Callable[[], None] | None = None) -> None:
        action = self.actions[instance].pop(task_id)
        task = action.scheduleTask
        payload = json.loads(json.loads(task.input.value))
        self.executed.append((instance, task_id, payload))
        result = _ActivityExecutor(self.worker._registry, _LOGGER, self.worker._data_converter).execute(
            instance, task.name, task_id, task.input.value
        )
        if before_ack is not None:
            before_ack()
        self.episode(instance, helpers.new_task_completed_event(task_id, result))
        self.flush()

    def complete_named(self, instance: str, executor: str, *, before_ack: Callable[[], None] | None = None) -> None:
        matching = [
            key for key, value in self.actions[instance].items() if value.scheduleTask.name.endswith(f"-{executor}")
        ]
        assert len(matching) == 1, (instance, executor, matching)
        self.complete(instance, matching[0], before_ack=before_ack)

    def pending(self) -> set[str]:
        return {request["request_id"] for request in self.client.get_pending_hitl_requests("root")}

    def reply(self, request: str, value: Any) -> None:
        self.client.send_hitl_response("root", request, value)
        self.flush()

    def cold(self, instance: str) -> None:
        before = deepcopy(self.executed)
        result = _replay(self.worker, instance, self.histories[instance])
        assert list(result.actions) == []
        assert json.loads(result.encoded_custom_status) == self.statuses[instance]
        assert self.executed == before


def _atomic_actions(groups: list[Any]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for item in groups:
        if isinstance(item, list):
            actions.extend(_atomic_actions(item))
        elif "compoundActions" in item:
            actions.extend(_atomic_actions(item["compoundActions"]))
        else:
            actions.append(item)
    return actions
