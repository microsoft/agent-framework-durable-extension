# Copyright (c) Microsoft. All rights reserved.

"""Registered Functions start and child-history helpers for SDK replay tests."""

import json
from copy import deepcopy
from typing import Any

import azure.durable_functions as df
from agent_framework import Workflow
from agent_framework_durabletask import wrap_workflow_input
from azure.durable_functions.models.ReplaySchema import ReplaySchema

from agent_framework_azurefunctions import AgentFunctionApp


def _event(kind: int, event_id: int = -1, **fields: Any) -> dict[str, Any]:
    return {
        "EventType": kind,
        "EventId": event_id,
        "IsPlayed": True,
        "Timestamp": "2026-09-22T00:00:00Z",
        **fields,
    }


def _actions(groups: list[Any]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for item in groups:
        if isinstance(item, list):
            actions.extend(_actions(item))
        elif "compoundActions" in item:
            actions.extend(_actions(item["compoundActions"]))
        else:
            actions.append(item)
    return actions


def _last_action(state: dict[str, Any], kind: int) -> dict[str, Any]:
    assert not state["isDone"] and not state.get("error")
    actions = [a for a in _actions(state["actions"]) if a["actionType"] == kind]
    assert actions
    return actions[-1]


class _AFStarts:
    def __init__(self, workflow: Workflow) -> None:
        app = AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")

        @app.function_name("native-input")
        @app.orchestration_trigger(context_name="context")
        def native(context: df.DurableOrchestrationContext) -> Any:
            # A co-registered application orchestrator retains the native SDK
            # decoder. Generated workflow hardening must not change it globally.
            return context.get_input()

        @app.function_name("native-parent")
        @app.orchestration_trigger(context_name="context")
        def native_parent(context: df.DurableOrchestrationContext) -> Any:
            result = yield context.call_sub_orchestrator(
                "dafx-provenance-leaf",
                input_=wrap_workflow_input(context.get_input()),
                instance_id="native-chosen-child",
            )
            return result  # noqa: B901

        self.functions: dict[str, Any] = {}
        for function in app.get_functions():
            name = function.get_function_name()
            assert name is not None
            self.functions[name] = function.get_user_function()
        self.starts: dict[str, dict[str, Any]] = {}

    def replay(self, instance: str) -> dict[str, Any]:
        record = self.starts[instance]
        before = deepcopy(record)
        result = json.loads(self.functions[record["history"][1]["Name"]](json.dumps(record)))
        assert record == before
        return result

    def start(self, name: str, instance: str, wire: Any, parent: str | None = None) -> dict[str, Any]:
        raw = json.dumps(wire)
        self.starts[instance] = {
            "history": [_event(12), _event(0, Name=name, Version="", Input=raw)],
            "instanceId": instance,
            "isReplaying": True,
            "parentInstanceId": parent,
            "input": raw,
            "upperSchemaVersion": ReplaySchema.V3.value,
        }
        return self.replay(instance)

    def complete_activity(self, instance: str, action: dict[str, Any], task_id: int = 0) -> dict[str, Any]:
        result = self.functions[action["functionName"]](json.loads(action["input"]))
        self.starts[instance]["history"].extend([
            _event(4, task_id, Name=action["functionName"], Input=action["input"]),
            _event(5, TaskScheduledId=task_id, Result=json.dumps(result)),
        ])
        return self.replay(instance)

    def child(self, parent: str, action: dict[str, Any], task_id: int) -> tuple[str, dict[str, Any]]:
        assert action["actionType"] == 2
        instance = action["instanceId"]
        self.starts[parent]["history"].append(
            _event(7, task_id, Name=action["functionName"], InstanceId=instance, Input=action["input"])
        )
        return instance, self.start(action["functionName"], instance, json.loads(action["input"]), parent)

    def complete_child(self, parent: str, task_id: int, output: Any) -> dict[str, Any]:
        self.starts[parent]["history"].append(_event(8, TaskScheduledId=task_id, Result=json.dumps(output)))
        return self.replay(parent)
