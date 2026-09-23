# Copyright (c) Microsoft. All rights reserved.

"""Historical SDK records and fresh constructed replay controls.

Fixtures are the JSON values recorded by third-holistic-audit/probe.py using
durabletask 1.7.2's in-memory backend and Core 1.16.0. Sibling pause/partial
snapshots are prefixes of the completed recording. Functions tests translate
these DT records into the AF SDK's event schema, not a Functions-host capture.
The historical sibling recording predates admission outcomes and rejection
activities. It is kept unchanged to characterize the user-accepted break in
old in-flight protocol-2 runs. Fresh starts are required despite the same marker.
New histories below are constructed from real registered producer results,
not relabeled historical captures. No network worker is started.

The historical child also lacks ExecutionStarted.parentInstance and has an old
concatenated instance ID. Positive replay controls explicitly transform copies
of both histories using the matching recorded parent dispatch, adding service
parent metadata and replacing the child ID in the start and parent-created
events. These are service-shaped controls, not original captures or a claim
that old runs can be migrated. No parent is inferred from child input markers
or an instance ID shape.

Mixed local/child assertions are unconditional. They require a reconstructed
snapshot with local requests and only unfinished child ordinals. Live backend
status retention/replacement remains a separate integration check.

The generic response annotation control below checks checkpointed activity
rejection and remains owned by the independent validation change.
"""

import asyncio
import json
import logging
from copy import deepcopy
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import (
    Executor,
    Workflow,
    WorkflowBuilder,
    WorkflowContext,
    WorkflowExecutor,
    handler,
    response_handler,
)
from durabletask.client import TaskHubGrpcClient
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.worker import TaskHubGrpcWorker, _ActivityExecutor, _OrchestrationExecutor
from google.protobuf.json_format import ParseDict
from test_workflow_hitl_lifecycle_audit import _Transport, _typed_workflow
from typing_extensions import Never

from agent_framework_durabletask import DurableAIAgentWorker, DurableWorkflowClient, wrap_workflow_input
from agent_framework_durabletask._workflows.naming import subworkflow_instance_id

_FIXTURES = Path(__file__).parent / "fixtures" / "workflow_replay"
_LOGGER = logging.getLogger(__name__)


class _Seed(Executor):
    @handler(input=str, output=str)
    async def handle(self, message: str, ctx: WorkflowContext[str]) -> None:
        await ctx.send_message(message)


class _Gate(Executor):
    def __init__(self, id: str) -> None:
        super().__init__(id=id)
        self.seen: list[Any] = []

    @handler(input=str)
    async def handle(self, message: str, ctx: WorkflowContext) -> None:
        await ctx.request_info("decision", response_type=int, request_id=self.id)

    @response_handler(request=str, response=object, workflow_output=dict)
    async def answer(self, original_request: str, response: Any, ctx: WorkflowContext[Never, dict]) -> None:
        self.seen.append(response)
        await ctx.yield_output({self.id: response})


def _graph(*, mixed: bool = False) -> tuple[Workflow, tuple[_Gate, _Gate]]:
    seed, a, b = _Seed(id="seed"), _Gate("a"), _Gate("b")
    right: Executor = b
    if mixed:
        inner = WorkflowBuilder(name="audit-inner", start_executor=b, output_from=[b]).build()
        right = WorkflowExecutor(inner, id="sub", propagate_request=True)
    workflow = (
        WorkflowBuilder(name="audit-mixed" if mixed else "audit-siblings", start_executor=seed, output_from=[a, right])
        .add_fan_out_edges(seed, [a, right])
        .build()
    )
    return workflow, (a, b)


def _history(name: str) -> list[Any]:
    rows = json.loads((_FIXTURES / f"{name}.json").read_text(encoding="utf-8"))
    return [ParseDict(row, pb.HistoryEvent()) for row in rows]


def _service_shaped_histories(parent: list[Any], child: list[Any]) -> tuple[list[Any], list[Any]]:
    """Transform legacy copies only after proving their original dispatch matches."""
    parent_starts = [event.executionStarted for event in parent if event.HasField("executionStarted")]
    child_starts = [event.executionStarted for event in child if event.HasField("executionStarted")]
    assert len(parent_starts) == len(child_starts) == 1
    parent_start, child_start = parent_starts[0], child_starts[0]
    parent_id = parent_start.orchestrationInstance.instanceId
    old_child_id = child_start.orchestrationInstance.instanceId
    assert parent_id and not child_start.HasField("parentInstance")
    dispatches = [
        event
        for event in parent
        if event.HasField("subOrchestrationInstanceCreated")
        and event.subOrchestrationInstanceCreated.instanceId == old_child_id
    ]
    assert len(dispatches) == 1
    dispatch = dispatches[0]
    assert dispatch.subOrchestrationInstanceCreated.name == child_start.name
    assert dispatch.subOrchestrationInstanceCreated.input == child_start.input
    # Keep the capture's legacy identity explicit. The graph above supplies the
    # executor and ordinal, not a parsed child ID or claimed input parentage.
    assert parent_id == "audit-mixed" and old_child_id == "audit-mixed::sub::0"
    child_id = subworkflow_instance_id(parent_id, "sub", 0)
    transformed_parent, transformed_child = deepcopy(parent), deepcopy(child)
    for event in transformed_parent:
        if event.HasField("subOrchestrationInstanceCreated"):
            created = event.subOrchestrationInstanceCreated
            assert created.instanceId == old_child_id
            created.instanceId = child_id
    started = next(event.executionStarted for event in transformed_child if event.HasField("executionStarted"))
    started.orchestrationInstance.instanceId = child_id
    started.parentInstance.CopyFrom(
        pb.ParentInstanceInfo(
            taskScheduledId=dispatch.eventId,
            name=helpers.get_string_value(parent_start.name),
            orchestrationInstance=parent_start.orchestrationInstance,
        )
    )
    assert not child_start.HasField("parentInstance")
    assert child_start.orchestrationInstance.instanceId == old_child_id
    assert dispatch.subOrchestrationInstanceCreated.instanceId == old_child_id
    return transformed_parent, transformed_child


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


@pytest.mark.parametrize("name", ["siblings-completed", "mixed-parent-paused", "mixed-child-paused"])
def test_historical_activity_results_characterize_admission_metadata_change(name: str) -> None:
    workflow, gates = _graph(mixed=name.startswith("mixed"))
    native = _worker(workflow)
    history = _history(name)
    instance = next(
        event.executionStarted.orchestrationInstance.instanceId
        for event in history
        if event.HasField("executionStarted")
    )
    scheduled = {event.eventId: event.taskScheduled for event in history if event.HasField("taskScheduled")}
    executor = _ActivityExecutor(native._registry, _LOGGER, native._data_converter)
    for event in history:
        if not event.HasField("taskCompleted"):
            continue
        task_id = event.taskCompleted.taskScheduledId
        task = scheduled[task_id]
        actual = executor.execute(instance, task.name, task_id, task.input.value)
        assert actual is not None
        recorded = json.loads(json.loads(event.taskCompleted.result.value))
        payload = json.loads(json.loads(task.input.value))
        assert "hitl_admission" not in recorded
        expected = recorded
        if payload["is_hitl_response"]:
            # Characterize the exact delta, without rewriting the old capture.
            expected = {
                **recorded,
                "hitl_admission": {"request_id": payload["message"]["request_id"], "status": "accepted"},
            }
        assert json.loads(json.loads(actual)) == expected
    assert [gate.seen for gate in gates] == ([[11], [22]] if name == "siblings-completed" else [[], []])


@pytest.mark.parametrize("count", [11, 13, 18, 23], ids=["paused", "invalid", "partial", "completed"])
def test_sdk_characterizes_historical_sibling_schedule_incompatibility(count: int) -> None:
    workflow, gates = _graph()
    native = _worker(workflow)
    history = _history("siblings-completed")
    assert len(history) == 23
    result = _replay(native, "audit-siblings", history[:count])
    if count == 11:
        assert list(result.actions) == []
        assert set(json.loads(result.encoded_custom_status)["pending_requests"]) == {"a", "b"}
    elif count == 13:
        # This prefix has no conflicting schedule yet. The new rejection
        # activity is already a divergence from the historical empty action list.
        assert len(result.actions) == 1
        action = result.actions[0]
        assert action.id == 4 and action.HasField("scheduleTask")
        assert action.scheduleTask.name == "dafx-audit-siblings-a"
        assert json.loads(json.loads(action.scheduleTask.input.value))["message"] == {
            "request_id": "a",
            "original_request": "decision",
            "response": "invalid",
            "response_type": "builtins:int",
        }
        assert set(json.loads(result.encoded_custom_status)["pending_requests"]) == {"a", "b"}
    else:
        assert len(result.actions) == 1 and result.actions[0].HasField("completeOrchestration")
        completed = result.actions[0].completeOrchestration
        assert completed.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
        assert completed.failureDetails.errorType == "NonDeterminismError"
        assert "dafx-audit-siblings-a" in completed.failureDetails.errorMessage
        assert "dafx-audit-siblings-b" in completed.failureDetails.errorMessage
        assert not completed.HasField("result")
    assert [gate.seen for gate in gates] == [[], []]


@pytest.mark.parametrize("host", ["dt", "af"])
@pytest.mark.parametrize("malformed", [False, True], ids=["typed-rejection", "malformed-envelope"])
def test_constructed_sibling_histories_replay_exact_registered_results(host: str, malformed: bool) -> None:
    # Local import avoids the mixed transport's dependency on this module.
    from test_workflow_mixed_hitl_review import _atomic_actions, _Episodes

    workflow, gates = _graph()
    transport = _Episodes(workflow)
    transport.client.start_workflow("go", instance_id="root")
    for executor in ("seed", "a", "b"):
        transport.complete_named("root", executor)
    snapshots: list[tuple[list[Any], dict[str, Any]]] = []

    def capture() -> None:
        snapshots.append((deepcopy(transport.histories["root"]), deepcopy(transport.statuses["root"])))

    assert transport.pending() == {"a", "b"}
    pending = deepcopy(transport.statuses["root"]["pending_requests"])
    capture()
    if malformed:
        transport.signal("root", event_name="a", data={"__type__": "builtins:int"})
        transport.flush()
    else:
        transport.reply("a", "invalid")
    assert len(transport.actions["root"]) == 1 and [gate.seen for gate in gates] == [[], []]
    assert transport.statuses["root"]["pending_requests"] == pending
    rejected_payload = json.loads(json.loads(next(iter(transport.actions["root"].values())).scheduleTask.input.value))
    expected_message: dict[str, Any] = {
        "request_id": "a",
        "original_request": "decision",
        "response": None if malformed else "invalid",
        "response_type": "builtins:int",
    }
    if malformed:
        expected_message["validation_error"] = True
    assert rejected_payload["message"] == expected_message
    transport.complete_named("root", "a")
    rejected = json.loads(json.loads(transport.histories["root"][-1].taskCompleted.result.value))
    assert rejected == {
        "hitl_admission": {"request_id": "a", "status": "invalidreply"},
        "sent_messages": [],
        "outputs": [],
        "events": [],
        "shared_state_updates": {},
        "shared_state_deletes": [],
        "pending_request_info_events": [],
    }
    assert transport.actions["root"] == {} and [gate.seen for gate in gates] == [[], []]
    assert transport.statuses["root"]["pending_requests"] == pending
    capture()
    transport.reply("b", 22)
    transport.complete_named("root", "b")
    assert transport.pending() == {"a"} and [gate.seen for gate in gates] == [[], [22]]
    assert transport.statuses["root"]["pending_requests"] == {"a": pending["a"]}
    capture()
    transport.reply("a", 11)
    transport.complete_named("root", "a")
    completion = transport.completions["root"]
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.loads(completion.result.value) == [{"b": 22}, {"a": 11}]
    assert [gate.seen for gate in gates] == [[11], [22]]
    assert not transport.statuses["root"].get("pending_requests")
    capture()
    assert [len(history) for history, _ in snapshots] == [11, 16, 21, 26]
    history = snapshots[-1][0]
    scheduled = {event.eventId: event.taskScheduled for event in history if event.HasField("taskScheduled")}
    completed_events = [event.taskCompleted for event in history if event.HasField("taskCompleted")]
    assert len(transport.executed) == len(scheduled) == len(completed_events) == 6
    assert [task.name for task in scheduled.values()] == [
        f"dafx-audit-siblings-{executor}" for executor in ("seed", "a", "b", "a", "b", "a")
    ]
    assert all(json.loads(json.loads(task.input.value))["shared_state_snapshot"] == {} for task in scheduled.values())
    results = [json.loads(json.loads(event.result.value)) for event in completed_events]
    assert [result["outputs"] for result in results] == [[], [], [], [], [{"b": 22}], [{"a": 11}]]
    assert [result["hitl_admission"] for result in results if "hitl_admission" in result] == [
        {"request_id": "a", "status": "invalidreply"},
        {"request_id": "b", "status": "accepted"},
        {"request_id": "a", "status": "accepted"},
    ]

    # Exact producer/result equality for NEW histories. No admission field is
    # injected into a recorded result to make a replay appear compatible.
    producer_workflow, producer_gates = _graph()
    producer = _worker(producer_workflow)
    activities = _ActivityExecutor(producer._registry, _LOGGER, producer._data_converter)
    for event in completed_events:
        task = scheduled[event.taskScheduledId]
        actual = activities.execute("root", task.name, event.taskScheduledId, task.input.value)
        assert actual is not None
        assert json.loads(json.loads(actual)) == json.loads(json.loads(event.result.value))
    assert [gate.seen for gate in producer_gates] == [[11], [22]]

    replay_workflow, replay_gates = _graph()
    replay_worker = _worker(replay_workflow)
    for index, (history, status) in enumerate(snapshots):
        done = index == 3
        if host == "dt":
            replay = _replay(replay_worker, "root", history)
            assert json.loads(replay.encoded_custom_status) == status
            if done:
                assert len(replay.actions) == 1 and replay.actions[0].HasField("completeOrchestration")
                terminal = replay.actions[0].completeOrchestration
                assert terminal.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
                assert json.loads(terminal.result.value) == [{"b": 22}, {"a": 11}]
            else:
                assert list(replay.actions) == []
        else:
            translated = _af_replay(history, replay_workflow, instance="root")
            assert translated["isDone"] is done
            assert translated.get("output") == ([{"b": 22}, {"a": 11}] if done else None)
            assert translated["customStatus"] == {key: value for key, value in status.items() if key != "events"}
            actions = _atomic_actions(translated["actions"])
            assert [
                (action["functionName"], json.loads(json.loads(action["input"])))
                for action in actions
                if action["actionType"] == 0
            ] == [
                (event.taskScheduled.name, json.loads(json.loads(event.taskScheduled.input.value)))
                for event in history
                if event.HasField("taskScheduled")
            ]
            expected_waits = ["a", "b", *(["a"] if index else [])]
            assert [action["externalEventName"] for action in actions if action["actionType"] == 6] == expected_waits
        assert [gate.seen for gate in replay_gates] == [[], []]
        assert [gate.seen for gate in gates] == [[11], [22]]


def test_service_shaped_replay_rebuilds_local_and_active_child_status_snapshot() -> None:
    workflow, _ = _graph(mixed=True)
    native = _worker(workflow)
    parent, child = _service_shaped_histories(_history("mixed-parent-paused"), _history("mixed-child-paused"))
    child_id = subworkflow_instance_id("audit-mixed", "sub", 0)
    assert len(parent) == 9 and len(child) == 5
    # The local gate has completed, the child has not. Discovery must precede
    # the child join, including a cold replay with no new completion event.
    assert parent[-1].taskCompleted.taskScheduledId == 2
    assert not any(event.HasField("subOrchestrationInstanceCompleted") for event in parent)
    before = _replay(native, "audit-mixed", parent[:3], parent[3:7])
    visible = json.loads(before.encoded_custom_status)
    assert visible["subworkflows"] == {"sub": {"0": child_id}}
    after = _replay(native, "audit-mixed", parent[:7], parent[7:])
    assert list(after.actions) == []
    child_state = _replay(native, child_id, child[:3], child[3:])
    assert "b" in json.loads(child_state.encoded_custom_status)["pending_requests"]
    assert after.encoded_custom_status is not None
    status = json.loads(after.encoded_custom_status)
    assert status["subworkflows"] == {"sub": {"0": child_id}}
    assert status["state"] == "waiting_for_human_input" and set(status["pending_requests"]) == {"a"}
    cold = _replay(native, "audit-mixed", parent)
    assert list(cold.actions) == []
    assert json.loads(cold.encoded_custom_status) == status
    assert len([event for event in status["events"] if event.get("request_id") == "a"]) == 1


def test_public_discovery_includes_local_request_before_child_completion() -> None:
    async def core_requests() -> list[str]:
        core, _ = _graph(mixed=True)
        return [event.request_id for event in (await core.run("go")).get_request_info_events()]

    assert set(asyncio.run(core_requests())) == {"a", "b"}
    workflow, _ = _graph(mixed=True)
    transport = _Transport()
    try:
        run = transport.start(workflow)
        client = Mock(spec=TaskHubGrpcClient)
        client.get_orchestration_state.side_effect = transport.state
        public = DurableWorkflowClient(client)
        pending = [request["request_id"] for request in public.get_pending_hitl_requests("root-run")]
        assert not run.done and len(run.calls) == 4
        assert set(pending) == {"a", "sub~0~b"}
    finally:
        transport.close()


def test_service_shaped_all_completed_transition_retires_child_address() -> None:
    workflow, gates = _graph(mixed=True)
    native = _worker(workflow)
    parent, child = _service_shaped_histories(_history("mixed-parent-paused"), _history("mixed-child-paused"))
    child_id = subworkflow_instance_id("audit-mixed", "sub", 0)
    reply = ParseDict({"eventId": -1, "eventRaised": {"name": "b", "input": "22"}}, pb.HistoryEvent())
    new = [helpers.new_orchestrator_started_event(), reply]
    resumed = _replay(native, child_id, child, new)
    assert len(resumed.actions) == 1 and resumed.actions[0].HasField("scheduleTask")
    action = resumed.actions[0]
    result = _ActivityExecutor(native._registry, _LOGGER, native._data_converter).execute(
        child_id, action.scheduleTask.name, action.id, action.scheduleTask.input.value
    )
    assert result is not None
    completed_child = _replay(
        native,
        child_id,
        [
            *child,
            *new,
            ParseDict({"eventId": action.id, "taskScheduled": {"name": action.scheduleTask.name}}, pb.HistoryEvent()),
        ],
        [
            helpers.new_orchestrator_started_event(),
            helpers.new_task_completed_event(action.id, result),
        ],
    )
    assert len(completed_child.actions) == 1
    completion = completed_child.actions[0].completeOrchestration
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.loads(completion.result.value)["outputs"] == [{"b": 22}]
    parent_result = _replay(
        native,
        "audit-mixed",
        parent,
        [
            helpers.new_orchestrator_started_event(),
            ParseDict(
                {
                    "eventId": -1,
                    "subOrchestrationInstanceCompleted": {"taskScheduledId": 3, "result": completion.result.value},
                },
                pb.HistoryEvent(),
            ),
        ],
    )
    assert list(parent_result.actions) == []
    status = json.loads(parent_result.encoded_custom_status)
    assert status["state"] == "waiting_for_human_input"
    assert set(status["pending_requests"]) == {"a"}
    assert "subworkflows" not in status
    assert [gate.seen for gate in gates] == [[], [22]]


def test_generic_response_rejection_is_checkpointed_by_registered_activity() -> None:
    async def core_trial() -> list[Any]:
        core, seen = _typed_workflow(list[int])
        start = await core.run("go")
        assert start.get_request_info_events()[0].response_type == list[int]
        with pytest.raises((TypeError, ValueError), match="Response type mismatch"):
            await core.run(responses={"approval": ["bad"]})
        return seen

    assert asyncio.run(core_trial()) == []
    workflow, seen = _typed_workflow(list[int])
    native = _worker(workflow)
    start = ParseDict(
        {
            "eventId": -1,
            "executionStarted": {
                "name": f"dafx-{workflow.name}",
                "input": json.dumps(wrap_workflow_input("go")),
                "orchestrationInstance": {"instanceId": "generic-run"},
            },
        },
        pb.HistoryEvent(),
    )
    history = [helpers.new_orchestrator_started_event(), start]
    scheduled = _replay(native, "generic-run", [], history).actions
    assert len(scheduled) == 1 and scheduled[0].HasField("scheduleTask")
    action = scheduled[0]
    result = _ActivityExecutor(native._registry, _LOGGER, native._data_converter).execute(
        "generic-run", action.scheduleTask.name, action.id, action.scheduleTask.input.value
    )
    assert result is not None
    request = json.loads(json.loads(result))["pending_request_info_events"][0]
    history.extend([
        ParseDict({"eventId": action.id, "taskScheduled": {"name": action.scheduleTask.name}}, pb.HistoryEvent()),
        helpers.new_orchestrator_started_event(),
        ParseDict(
            {"eventId": -1, "taskCompleted": {"taskScheduledId": action.id, "result": result}}, pb.HistoryEvent()
        ),
    ])
    resumed = _replay(
        native,
        "generic-run",
        history,
        [
            helpers.new_orchestrator_started_event(),
            ParseDict({"eventId": -1, "eventRaised": {"name": "approval", "input": '["bad"]'}}, pb.HistoryEvent()),
        ],
    )
    assert request["response_type"] == {"_durable_response_type": 1, "kind": "list", "args": ["builtins:int"]}
    assert len(resumed.actions) == 1 and resumed.actions[0].HasField("scheduleTask")
    reply = resumed.actions[0]
    rejected = _ActivityExecutor(native._registry, _LOGGER, native._data_converter).execute(
        "generic-run", reply.scheduleTask.name, reply.id, reply.scheduleTask.input.value
    )
    assert rejected is not None and seen == []
    assert json.loads(json.loads(rejected)) == {
        "hitl_admission": {"request_id": "approval", "status": "invalidreply"},
        "sent_messages": [],
        "outputs": [],
        "events": [],
        "shared_state_updates": {},
        "shared_state_deletes": [],
        "pending_request_info_events": [],
    }


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


@pytest.mark.parametrize("count", [11, 13, 18, 23], ids=["paused", "invalid", "partial", "completed"])
def test_functions_sdk_exposes_historical_sibling_schedule_divergence(count: int) -> None:
    from test_workflow_mixed_hitl_review import _atomic_actions

    workflow, gates = _graph()
    history = deepcopy(_history("siblings-completed")[:count])
    result = _af_replay(history, workflow)
    assert not result["isDone"] and result.get("output") is None
    scheduled_names = [
        action["functionName"] for action in _atomic_actions(result["actions"]) if action["actionType"] == 0
    ]
    recorded_names = [event.taskScheduled.name for event in history if event.HasField("taskScheduled")]
    if count == 11:
        assert scheduled_names == recorded_names
        assert set(result["customStatus"]["pending_requests"]) == {"a", "b"}
    else:
        # The AF SDK does not validate historical activity names like DT does.
        # Its divergent action graph is not successful old-history replay.
        assert scheduled_names != recorded_names
        assert scheduled_names[:4] == [
            "dafx-audit-siblings-seed",
            "dafx-audit-siblings-a",
            "dafx-audit-siblings-b",
            "dafx-audit-siblings-a",
        ]
        if count >= 18:
            assert recorded_names[3] == "dafx-audit-siblings-b"
    assert [gate.seen for gate in gates] == [[], []]


def test_functions_sdk_reconstructs_service_shaped_mixed_parent_snapshot() -> None:
    workflow, gates = _graph(mixed=True)
    history, _ = _service_shaped_histories(_history("mixed-parent-paused"), _history("mixed-child-paused"))
    result = _af_replay(history, workflow, instance="audit-mixed")
    assert not result["isDone"]
    status = result["customStatus"]
    assert status["state"] == "waiting_for_human_input"
    assert set(status["pending_requests"]) == {"a"}
    assert status["subworkflows"] == {"sub": {"0": subworkflow_instance_id("audit-mixed", "sub", 0)}}
    assert "events" not in status
    assert _af_replay(history, workflow, instance="audit-mixed")["customStatus"] == status
    assert [gate.seen for gate in gates] == [[], []]
