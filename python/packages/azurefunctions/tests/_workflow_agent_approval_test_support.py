# Copyright (c) Microsoft. All rights reserved.

"""Registered agent approvals and native SDK history helpers for backlog tests."""

import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import Any
from unittest.mock import patch

from _execution_test_support import NonStreamingAgent
from _workflow_event_test_support_af import _event
from _workflow_replay_test_support import _LOGGER, _atomic_actions, _replay, _worker
from agent_framework import (
    AgentExecutor,
    BaseChatClient,
    ChatMiddlewareLayer,
    ChatResponse,
    Content,
    FunctionInvocationLayer,
    Message,
    Workflow,
    WorkflowBuilder,
    tool,
)
from agent_framework_durabletask import wrap_workflow_input
from agent_framework_durabletask._workflows.dt_context import DurableTaskWorkflowContext
from azure.durable_functions import DurableOrchestrationContext
from azure.durable_functions.models.ReplaySchema import ReplaySchema
from azure.durable_functions.models.TaskOrchestrationExecutor import TaskOrchestrationExecutor
from durabletask.entities import EntityContext, EntityInstanceId
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.internal.entity_state_shim import StateShim
from durabletask.serialization import JsonDataConverter
from durabletask.worker import _ActivityExecutor

from agent_framework_azurefunctions import AgentFunctionApp
from agent_framework_azurefunctions._workflow_af_context import AzureFunctionsWorkflowContext

CHECKPOINT = "dafx__hitl-agent-backlog"
ENTITY = "dafx-agent-backlog-agent"


class _ApprovalClient(FunctionInvocationLayer, ChatMiddlewareLayer, BaseChatClient):
    STORES_BY_DEFAULT = False

    def __init__(self, *, sibling_first: bool = False) -> None:
        super().__init__(middleware=[])
        self.calls: list[list[Message]] = []
        self.keys = ("b", "a") if sibling_first else ("a", "b")

    def _inner_get_response(
        self, *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
    ) -> Any:
        assert not stream
        self.calls.append(deepcopy(list(messages)))
        first = len(self.calls) == 1
        contents = (
            [Content.from_function_call(key, "lookup", arguments={"key": key}) for key in self.keys]
            if first
            else [Content.from_text("done")]
        )

        async def get() -> ChatResponse:
            return ChatResponse(
                messages=[Message("assistant", contents)],
                response_id=f"model-{len(self.calls)}",
                finish_reason="tool_calls" if first else "stop",
            )

        return get()


def _workflow(*, sibling_first: bool = False) -> tuple[Workflow, _ApprovalClient, list[str]]:
    client = _ApprovalClient(sibling_first=sibling_first)
    effects: list[str] = []

    @tool(name="lookup", approval_mode="always_require")
    def lookup(key: str) -> str:
        """Look up a key only after explicit human approval."""
        effects.append(key)
        return f"value:{key}"

    agent = NonStreamingAgent(client=client, name="agent", tools=[lookup])
    node = AgentExecutor(agent, id="agent")
    workflow = WorkflowBuilder(name="agent-backlog", start_executor=node, output_from=[node]).build()
    workflow.max_iterations = 2  # Initial entity call and one fully approved call only.
    return workflow, client, effects


def _functions(workflow: Workflow) -> dict[str, Any]:
    app = AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")
    functions: dict[str, Any] = {}
    for function in app.get_functions():
        name = function.get_function_name()
        assert name is not None
        functions[name] = function.get_user_function()
    return functions


class _History:
    def __init__(self, functions_host: bool, workflow: Workflow) -> None:
        self.functions_host = functions_host
        self.functions = _functions(workflow) if functions_host else {}
        self.native: Any = None if functions_host else _worker(workflow)
        self.entity_state: str | None = None
        self.entity_inputs: list[dict[str, Any]] = []
        self.checkpoints = 0
        self.waits: dict[str, list[Any]] = {}
        start = json.dumps(wrap_workflow_input("go"))
        self.rows: list[Any] = (
            [_event(12), _event(0, Name="dafx-agent-backlog", Input=start)]
            if functions_host
            else [
                helpers.new_orchestrator_started_event(),
                helpers.new_execution_started_event("dafx-agent-backlog", "root", start),
            ]
        )

    def replay(self) -> dict[str, Any]:
        adapter: Any = AzureFunctionsWorkflowContext if self.functions_host else DurableTaskWorkflowContext
        original = adapter.wait_for_external_event
        self.waits = {}

        def observe(context: Any, name: str) -> Any:
            waiting = original(context, name)
            self.waits.setdefault(name, []).append(waiting)
            return waiting

        # Pass-through observation of real waits, not replacement tasks.
        with patch.object(adapter, "wait_for_external_event", observe):
            if self.functions_host:
                context = DurableOrchestrationContext(
                    self.rows,
                    instanceId="root",
                    isReplaying=True,
                    parentInstanceId=None,
                    input=json.dumps(wrap_workflow_input("go")),
                    upperSchemaVersion=ReplaySchema.V3.value,
                )
                result = json.loads(
                    TaskOrchestrationExecutor().execute(
                        context, context.histories, self.functions["dafx-agent-backlog"].orchestrator_function
                    )
                )
                result["scheduled"] = [
                    action for action in _atomic_actions(result["actions"]) if action["actionType"] in (0, 7)
                ]
                return result
            native_result = _replay(self.native, "root", self.rows)
        actions = list(native_result.actions)
        result = {
            "isDone": False,
            "customStatus": json.loads(native_result.encoded_custom_status or "{}"),
            "actions": actions,
        }
        if len(actions) == 1 and actions[0].HasField("completeOrchestration"):
            completion = actions[0].completeOrchestration
            assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
            result.update(isDone=True, output=json.loads(completion.result.value))
        return result

    def event(self, name: str, value: Any, event_id: int) -> None:
        wire = json.dumps(value)
        if self.functions_host:
            self.rows.append(_event(15, event_id, Name=name, Input=wire))
        else:
            event = helpers.new_event_raised_event(name, wire)
            event.eventId = event_id
            self.rows.append(event)

    def complete_entity(self, state: dict[str, Any]) -> dict[str, Any]:
        if self.functions_host:
            action = state["scheduled"][-1]
            task_id = len(state["scheduled"]) - 1
            assert action["actionType"] == 7 and action["instanceId"] == f"@{ENTITY}@root"
            assert action["operation"] == "run"
            self.entity_inputs.append(json.loads(action["input"]))
            batch_input = {
                "self": {"name": ENTITY, "key": "root"},
                "exists": self.entity_state is not None,
                "state": self.entity_state,
                "batch": [{"name": "run", "input": json.dumps(action["input"])}],
            }
            batch = json.loads(self.functions[ENTITY](json.dumps(batch_input)))
            assert len(batch["results"]) == 1 and batch["results"][0]["isError"] is False
            self.entity_state = batch["entityState"]
            wire_result = batch["results"][0]["result"]
            request_id = f"entity-call-{task_id}"
            self.rows.extend([
                _event(14, task_id, Name="op", Input=json.dumps({"id": request_id})),
                _event(15, Name=request_id, Input=json.dumps({"result": wire_result})),
            ])
            return json.loads(wire_result)

        assert len(state["actions"]) == 1
        action = state["actions"][0]
        assert action.HasField("sendEntityMessage")
        call = action.sendEntityMessage.entityOperationCalled
        entity_id = EntityInstanceId.parse(call.targetInstanceId.value)
        assert entity_id.entity == ENTITY and entity_id.key == "root" and call.operation == "run"
        request = json.loads(call.input.value)
        self.entity_inputs.append(deepcopy(request))
        converter = JsonDataConverter()
        shim = StateShim(self.entity_state, converter, is_serialized=True)
        factory = self.native._registry.get_entity(entity_id.entity)
        entity = factory()
        entity._initialize_entity_context(EntityContext("root", "run", shim, entity_id, converter))
        result = entity.run(request)
        self.entity_state = shim.encode_state()
        self.rows.extend([
            pb.HistoryEvent(eventId=action.id, entityOperationCalled=call),
            pb.HistoryEvent(
                eventId=-1,
                entityOperationCompleted=pb.EntityOperationCompletedEvent(
                    requestId=call.requestId, output={"value": json.dumps(result)}
                ),
            ),
        ])
        return result

    def checkpoint_input(self, state: dict[str, Any], index: int) -> str:
        if self.functions_host:
            assert len(state["scheduled"]) == index + 1
            action = state["scheduled"][-1]
            assert action["actionType"] == 0 and action["functionName"] == CHECKPOINT
            return str(action["input"])
        assert len(state["actions"]) == 1
        action = state["actions"][0]
        assert action.id == index + 1 and action.HasField("scheduleTask")
        assert action.scheduleTask.name == CHECKPOINT
        return str(action.scheduleTask.input.value)

    def complete_checkpoint(self, index: int, wire: str) -> None:
        if self.functions_host:
            result = self.functions[CHECKPOINT](json.loads(wire))
            assert result == "null"
            self.rows.extend([
                _event(4, index, Name=CHECKPOINT, Input=wire),
                _event(5, TaskScheduledId=index, Result=json.dumps(result)),
            ])
        else:
            result = _ActivityExecutor(self.native._registry, _LOGGER, self.native._data_converter).execute(
                "root", CHECKPOINT, index + 1, wire
            )
            assert result is not None and json.loads(result) == "null"
            self.rows.extend([
                helpers.new_task_scheduled_event(index + 1, CHECKPOINT, wire),
                helpers.new_task_completed_event(index + 1, result),
            ])
        self.checkpoints += 1
