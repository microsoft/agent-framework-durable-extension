# Copyright (c) Microsoft. All rights reserved.

"""Registered agent approvals and native SDK history helpers for backlog tests."""

import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import Any
from unittest.mock import patch

from _af_worker_test_support import _af_worker
from _execution_test_support import NonStreamingAgent
from _workflow_replay_test_support import _LOGGER, _replay, _worker
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
from durabletask.entities import EntityContext, EntityInstanceId
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.internal.entity_state_shim import StateShim
from durabletask.serialization import JsonDataConverter
from durabletask.worker import _ActivityExecutor

from agent_framework_azurefunctions import AgentFunctionApp

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
        # Both hosts run the shared engine on durabletask executors. The Functions
        # variant replays the app's registered functions with the app's converter.
        self.native: Any = (
            _af_worker(AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2"))
            if functions_host
            else _worker(workflow)
        )
        self.entity_state: str | None = None
        self.entity_inputs: list[dict[str, Any]] = []
        self.checkpoints = 0
        self.waits: dict[str, list[Any]] = {}
        start = json.dumps(wrap_workflow_input("go"))
        self.rows: list[Any] = [
            helpers.new_orchestrator_started_event(),
            helpers.new_execution_started_event("dafx-agent-backlog", "root", start),
        ]

    def replay(self) -> dict[str, Any]:
        original = DurableTaskWorkflowContext.wait_for_external_event
        self.waits = {}

        def observe(context: Any, name: str) -> Any:
            waiting = original(context, name)
            self.waits.setdefault(name, []).append(waiting)
            return waiting

        # Pass-through observation of real waits, not replacement tasks.
        with patch.object(DurableTaskWorkflowContext, "wait_for_external_event", observe):
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
        event = helpers.new_event_raised_event(name, json.dumps(value))
        event.eventId = event_id
        self.rows.append(event)

    def complete_entity(self, state: dict[str, Any]) -> dict[str, Any]:
        assert len(state["actions"]) == 1
        action = state["actions"][0]
        assert action.HasField("sendEntityMessage")
        call = action.sendEntityMessage.entityOperationCalled
        entity_id = EntityInstanceId.parse(call.targetInstanceId.value)
        assert entity_id.entity == ENTITY and entity_id.key == "root" and call.operation == "run"
        request = json.loads(call.input.value)
        self.entity_inputs.append(deepcopy(request))
        # The Functions entity decodes with its own worker's converter.
        converter: Any = self.native._data_converter if self.functions_host else JsonDataConverter()
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
        assert len(state["actions"]) == 1
        action = state["actions"][0]
        assert action.id == index + 1 and action.HasField("scheduleTask")
        assert action.scheduleTask.name == CHECKPOINT
        return str(action.scheduleTask.input.value)

    def complete_checkpoint(self, index: int, wire: str) -> None:
        result = _ActivityExecutor(self.native._registry, _LOGGER, self.native._data_converter).execute(
            "root", CHECKPOINT, index + 1, wire
        )
        assert result is not None and json.loads(result) == "null"
        self.rows.extend([
            helpers.new_task_scheduled_event(index + 1, CHECKPOINT, wire),
            helpers.new_task_completed_event(index + 1, result),
        ])
        self.checkpoints += 1
