# Copyright (c) Microsoft. All rights reserved.

"""Native SDK histories with registered entity/activity producers, not live hosts.

Only the external service's event delivery is constructed. Both orchestrators,
both entity registrations, Core's tool approvals, and checkpoint activities run
unchanged. No SDK resumption, recursion limit, task result or queue is replaced.
"""

import json
import pickle
from collections.abc import Mapping, Sequence
from copy import deepcopy
from typing import Any
from unittest.mock import Mock, patch

import pytest
from _execution_test_support import NonStreamingAgent
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
    WorkflowExecutor,
    tool,
)
from agent_framework_durabletask import DurableAIAgentWorker, load_agent_response, wrap_workflow_input
from agent_framework_durabletask._workflows import orchestrator as engine
from agent_framework_durabletask._workflows.dt_context import DurableTaskWorkflowContext
from agent_framework_durabletask._workflows.serialization import deserialize_workflow_output
from azure.durable_functions import DurableOrchestrationContext
from azure.durable_functions.models.ReplaySchema import ReplaySchema
from azure.durable_functions.models.TaskOrchestrationExecutor import TaskOrchestrationExecutor
from durabletask.entities import EntityContext, EntityInstanceId
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.internal.entity_state_shim import StateShim
from durabletask.serialization import JsonDataConverter
from durabletask.worker import _ActivityExecutor
from test_workflow_buffered_events_review_af import _event
from test_workflow_mixed_hitl_review import _atomic_actions
from test_workflow_recorded_replay_review import _LOGGER, _replay, _worker

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


@pytest.mark.parametrize("functions_host", [False, True], ids=["dt", "af"])
@pytest.mark.parametrize("sibling_first", [False, True], ids=["retained-sibling", "aggregated-sibling"])
@pytest.mark.parametrize(("kind", "count"), [("null", 1100), ("wrong-id", 3), ("malformed-envelope", 3)])
def test_native_agent_rejection_backlog_keeps_approvals_and_replays_without_execution(
    functions_host: bool, sibling_first: bool, kind: str, count: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    workflow, client, effects = _workflow(sibling_first=sibling_first)
    history = _History(functions_host, workflow)
    first = history.complete_entity(history.replay())
    approvals: dict[str, Content] = {}
    for request in load_agent_response(first).user_input_requests:
        assert request.function_call is not None and isinstance(request.function_call.call_id, str)
        approvals[request.function_call.call_id] = request
    assert set(approvals) == {"a", "b"}
    a, b = approvals["a"], approvals["b"]
    assert isinstance(a.id, str) and isinstance(b.id, str) and a.id != b.id
    answer_a = a.to_function_approval_response(True).to_dict()
    answer_b = b.to_function_approval_response(True).to_dict()
    invalid = {
        "null": None,
        "wrong-id": {**answer_a, "id": "not-the-pending-approval"},
        "malformed-envelope": {"__type__": ["unloaded:Untrusted"], "private": "PRIVATE_REPLY"},
    }[kind]
    assert len(client.calls) == len(history.entity_inputs) == 1 and effects == []
    initial_entity_state = history.entity_state

    # All replies arrive before the initial entity's completion, so the real
    # SDK must consume distinct, equal-valued buffered occurrences. Keep the
    # service event identity separate from the (identical) application payload.
    completion = history.rows.pop()
    if sibling_first:
        # Both names are buffered. Put b first in the real producer's request
        # order too: SDK task_any chooses the first already-ready wait, not the
        # globally earliest arrival across different event names.
        history.event(b.id, answer_b, 9000)
    backlog_start = len(history.rows)
    for index in range(count):
        history.event(a.id, invalid, 10000 + index)
    history.event(a.id, answer_a, 10000 + count)
    backlog = history.rows[backlog_start : backlog_start + count]
    if functions_host:
        assert len({row["EventId"] for row in backlog}) == count
        assert len({row["Input"] for row in backlog}) == 1
    else:
        assert len({row.eventId for row in backlog}) == count
        assert len({row.eventRaised.input.value for row in backlog}) == 1
    history.rows.append(completion)

    no_decode = Mock(side_effect=AssertionError("Agent rejection must not decode arbitrary response classes"))
    with monkeypatch.context() as guard:
        guard.setattr(pickle, "loads", no_decode)
        guard.setattr(engine, "_deserialize_hitl_response", no_decode)
        before = history.replay()
        wire = history.checkpoint_input(before, 1)
        assert json.loads(json.loads(wire)) == {"request_id": a.id, "status": "invalidreply"}
        assert "PRIVATE_REPLY" not in wire and "__type__" not in wire
        pending = deepcopy(before["customStatus"]["pending_requests"])
        assert set(pending) == ({a.id} if sibling_first else {a.id, b.id})
        assert not before["isDone"]
        assert len(history.waits[a.id]) == len(history.waits[b.id]) == 1

        history.complete_checkpoint(1, wire)
        after = history.replay()
        assert history.checkpoint_input(after, 2) == wire
        assert after["customStatus"] == before["customStatus"]
        assert len(history.waits[a.id]) == 2 and len(history.waits[b.id]) == 1
        # Build linear histories with real activity results instead of running
        # 1100 quadratic cold episodes. The final SDK pass checks the schedule.
        for index in range(2, count + 1):
            history.complete_checkpoint(index, wire)
        ready = history.replay()
        assert not ready["isDone"] and history.checkpoints == count
        assert len(history.waits[a.id]) == count + 1 and len(history.waits[b.id]) == 1
        assert history.entity_state == initial_entity_state
        assert len(client.calls) == len(history.entity_inputs) == 1 and effects == []
        if not sibling_first:
            assert ready["customStatus"]["pending_requests"] == {b.id: pending[b.id]}
            if functions_host:
                assert len(ready["scheduled"]) == count + 1
            else:
                assert ready["actions"] == []
            history.event(b.id, answer_b, 20000)
            ready = history.replay()
        no_decode.assert_not_called()

    # Only once every matching approval is accumulated does a second registered
    # entity run execute the real Core tools and make the next model request.
    second = history.complete_entity(ready)
    assert load_agent_response(second).text == "done"
    assert len(history.entity_inputs) == len(client.calls) == 2
    assert sorted(effects) == ["a", "b"] and len(effects) == 2
    delivered = history.entity_inputs[1]["contextMessages"]
    assert len(delivered) == 1 and delivered[0]["role"] == "user"
    expected = [answer_b, answer_a] if sibling_first else [answer_a, answer_b]
    assert delivered[0]["contents"] == expected
    committed = history.entity_state
    with monkeypatch.context() as guard:
        guard.setattr(pickle, "loads", no_decode)
        guard.setattr(engine, "_deserialize_hitl_response", no_decode)
        for _ in range(2):
            cold = history.replay()
            assert cold["isDone"] and not cold["customStatus"].get("pending_requests")
            assert deserialize_workflow_output(cold["output"])[0].text == "done"
            assert len(history.waits[a.id]) == count + 1 and len(history.waits[b.id]) == 1
            if functions_host:
                assert [action["actionType"] for action in cold["scheduled"]] == [7, *([0] * count), 7]
                assert all(action["input"] == wire for action in cold["scheduled"][1:-1])
            else:
                scheduled = [event.taskScheduled for event in history.rows if event.HasField("taskScheduled")]
                assert [task.name for task in scheduled] == [CHECKPOINT] * count
                assert all(task.input.value == wire for task in scheduled)
                assert sum(event.HasField("taskCompleted") for event in history.rows) == count
                invoked = [e for e in cold["customStatus"]["events"] if e["type"] == "executor_invoked"]
                assert [event["iteration"] for event in invoked] == [0, 1]
            assert history.entity_state == committed and history.checkpoints == count
            assert len(history.entity_inputs) == len(client.calls) == 2 and sorted(effects) == ["a", "b"]
        no_decode.assert_not_called()


@pytest.mark.parametrize("functions_host", [False, True], ids=["dt", "af"])
def test_valid_agent_approval_batch_keeps_entity_only_schedule(functions_host: bool) -> None:
    workflow, client, effects = _workflow()
    history = _History(functions_host, workflow)
    first = history.complete_entity(history.replay())
    requests = load_agent_response(first).user_input_requests
    assert len(requests) == 2
    pending = deepcopy(history.replay()["customStatus"]["pending_requests"])
    for index, request in enumerate(reversed(requests)):
        assert isinstance(request.id, str)
        history.event(request.id, request.to_function_approval_response(True).to_dict(), 30000 + index)
        ready = history.replay()
        if index == 0:
            assert set(ready["customStatus"]["pending_requests"]) == set(pending) - {request.id}
            assert len(history.entity_inputs) == len(client.calls) == 1 and effects == []
    second = history.complete_entity(ready)
    assert load_agent_response(second).text == "done"
    final = history.replay()
    assert final["isDone"] and history.checkpoints == 0
    assert len(history.entity_inputs) == len(client.calls) == 2 and sorted(effects) == ["a", "b"]
    if functions_host:
        assert [action["actionType"] for action in final["scheduled"]] == [7, 7]
    else:
        assert not any(event.HasField("taskScheduled") for event in history.rows)


@pytest.mark.parametrize("functions_host", [False, True], ids=["dt", "af"])
def test_checkpoint_registration_is_per_agent_workflow_and_deduplicates_nested_reuse(functions_host: bool) -> None:
    inner, _, _ = _workflow()
    another = AgentExecutor(NonStreamingAgent(client=_ApprovalClient(), name="other"), id="other")
    inner.executors[another.id] = another
    left, right = WorkflowExecutor(inner, id="left"), WorkflowExecutor(inner, id="right")
    outer = WorkflowBuilder(name="outer", start_executor=left, output_from=[left]).build()
    # Registration visits all registered nodes, including disconnected nodes.
    outer.executors[right.id] = right
    if functions_host:
        functions = _functions(outer)
        assert [name for name in functions if name.startswith("dafx__hitl-")] == [CHECKPOINT]
        assert ENTITY in functions and "dafx-agent-backlog-other" in functions
    else:
        native = Mock()
        worker = DurableAIAgentWorker(native, deployment_mode="isolated_v2")
        worker.configure_workflow(outer)
        worker.configure_workflow(inner)
        names = [call.args[0].__name__ for call in native.add_activity.call_args_list]
        assert names == [CHECKPOINT]
        assert [call.args[0].__name__ for call in native.add_entity.call_args_list] == [
            ENTITY,
            "dafx-agent-backlog-other",
        ]


@pytest.mark.parametrize("functions_host", [False, True], ids=["dt", "af"])
@pytest.mark.parametrize("instruction", [None, {}, {"request_id": "a", "status": "accepted"}])
def test_registered_checkpoint_rejects_untrusted_instruction_shapes(functions_host: bool, instruction: Any) -> None:
    workflow, client, effects = _workflow()
    if functions_host:
        function = _functions(workflow)[CHECKPOINT]
    else:
        native = Mock()
        DurableAIAgentWorker(native, deployment_mode="isolated_v2").configure_workflow(workflow)
        activities = {call.args[0].__name__: call.args[0] for call in native.add_activity.call_args_list}

        def function(value: str) -> str:
            return str(activities[CHECKPOINT](None, value))

    with pytest.raises(ValueError, match="internal HITL checkpoint instruction"):
        function(json.dumps(instruction))
    assert client.calls == effects == []
