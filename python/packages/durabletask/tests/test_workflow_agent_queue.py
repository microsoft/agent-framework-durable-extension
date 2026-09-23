# Copyright (c) Microsoft. All rights reserved.

"""FIFO agent messages drain once while a real SDK child task remains pending."""

import json
import logging
from copy import deepcopy
from typing import Any

import pytest
from _execution_test_support import NonStreamingAgent, RecordingChatClient
from agent_framework import AgentExecutor, Executor, WorkflowBuilder, WorkflowContext, WorkflowExecutor, handler
from durabletask.entities import EntityContext, EntityInstanceId
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.internal.entity_state_shim import StateShim
from durabletask.serialization import JsonDataConverter
from durabletask.worker import _ActivityExecutor
from test_workflow_recorded_replay_review import _replay, _worker
from typing_extensions import Never

from agent_framework_durabletask import wrap_workflow_input
from agent_framework_durabletask._workflows.naming import subworkflow_instance_id


@pytest.mark.parametrize("mixed", [False, True], ids=["local-wave", "held-child-wave"])
def test_registered_agent_queue_is_fifo_without_repeating_consumed_messages(mixed: bool) -> None:
    messages = [f"message-{index}" for index in range(6)]

    class Seed(Executor):
        @handler(input=str, output=str)
        async def handle(self, message: str, ctx: WorkflowContext[str]) -> None:
            for text in messages:
                await ctx.send_message(text, target_id="agent")
            if mixed:
                await ctx.send_message("hold", target_id="sub")

    class Child(Executor):
        @handler(input=str, workflow_output=str)
        async def handle(self, message: str, ctx: WorkflowContext[Never, str]) -> None:
            raise AssertionError("The child must remain pending while the parent drains its agent queue")

    client = RecordingChatClient()
    node = AgentExecutor(NonStreamingAgent(client=client, name="agent"), id="agent")
    seed = Seed(id="seed")
    builder = WorkflowBuilder(name="agent-queue", start_executor=seed, output_from=[node]).add_edge(seed, node)
    if mixed:
        child = Child(id="child")
        inner = WorkflowBuilder(name="queue-child", start_executor=child, output_from=[child]).build()
        builder.add_edge(seed, WorkflowExecutor(inner, id="sub"))
    native = _worker(builder.build())
    history: list[Any] = []

    def advance(*events: Any) -> Any:
        incoming = [helpers.new_orchestrator_started_event(), *events]
        result = _replay(native, "root", history, incoming)
        history.extend(incoming)
        for action in result.actions:
            if action.HasField("scheduleTask"):
                task = action.scheduleTask
                history.append(helpers.new_task_scheduled_event(action.id, task.name, task.input.value))
            elif action.HasField("createSubOrchestration"):
                child_task = action.createSubOrchestration
                history.append(
                    helpers.new_sub_orchestration_created_event(
                        action.id, child_task.name, child_task.instanceId, child_task.input.value
                    )
                )
            elif action.HasField("sendEntityMessage"):
                history.append(
                    pb.HistoryEvent(
                        eventId=action.id, entityOperationCalled=action.sendEntityMessage.entityOperationCalled
                    )
                )
            else:
                assert action.HasField("completeOrchestration")
        return result

    result = advance(
        helpers.new_execution_started_event("dafx-agent-queue", "root", json.dumps(wrap_workflow_input("go")))
    )
    assert len(result.actions) == 1 and result.actions[0].HasField("scheduleTask")
    seed_action = result.actions[0]
    task = seed_action.scheduleTask
    wire = _ActivityExecutor(native._registry, logging.getLogger(__name__), native._data_converter).execute(
        "root", task.name, seed_action.id, task.input.value
    )
    result = advance(helpers.new_task_completed_event(seed_action.id, wire))
    entity_state: str | None = None
    converter = JsonDataConverter()
    occurrences: list[str] = []
    for text in messages:
        actions = [action for action in result.actions if action.HasField("sendEntityMessage")]
        assert len(actions) == 1, "Exactly one message per agent may be in flight"
        call = actions[0].sendEntityMessage.entityOperationCalled
        entity_id = EntityInstanceId.parse(call.targetInstanceId.value)
        assert entity_id.entity == "dafx-agent-queue-agent" and entity_id.key == "root" and call.operation == "run"
        request = json.loads(call.input.value)
        assert request["message"] == text and len(request["contextMessages"]) == 1
        occurrences.extend(request["contextMessageIds"])
        shim = StateShim(entity_state, converter, is_serialized=True)
        entity = native._registry.get_entity(entity_id.entity)()
        entity._initialize_entity_context(EntityContext("root", "run", shim, entity_id, converter))
        response = entity.run(request)
        assert response.get("additional_properties", {}).get("durable_status") != "error"
        entity_state = shim.encode_state()
        result = advance(
            pb.HistoryEvent(
                eventId=-1,
                entityOperationCompleted=pb.EntityOperationCompletedEvent(
                    requestId=call.requestId, output={"value": json.dumps(response)}
                ),
            )
        )
    assert len(occurrences) == len(set(occurrences)) == len(messages)
    assert [batch[-1].text for batch in client.received_messages] == messages
    if mixed:
        assert list(result.actions) == []
        status = json.loads(result.encoded_custom_status)
        assert status["subworkflows"] == {"sub": {"0": subworkflow_instance_id("root", "sub", 0)}}
        child_dispatches = [event for event in history if event.HasField("subOrchestrationInstanceCreated")]
        assert len(child_dispatches) == 1
        assert not any(event.HasField("subOrchestrationInstanceCompleted") for event in history)
    else:
        assert len(result.actions) == 1 and result.actions[0].HasField("completeOrchestration")
        assert result.actions[0].completeOrchestration.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    before = deepcopy(client.received_messages)
    for _ in range(2):
        cold = _replay(native, "root", history)
        assert list(cold.actions) == list(result.actions)
        assert cold.encoded_custom_status == result.encoded_custom_status
        assert [[message.to_dict() for message in batch] for batch in client.received_messages] == [
            [message.to_dict() for message in batch] for batch in before
        ]
