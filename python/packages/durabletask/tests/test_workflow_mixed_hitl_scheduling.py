# Copyright (c) Microsoft. All rights reserved.

"""Mixed HITL through public clients, registered activities and cold SDK episodes.

The transport supplies only service history events and schedules returned actions.
It does not implement workflow routing or admission. Activities are executed by
the SDK registry and every orchestration episode uses a fresh SDK executor. This
is an action-graph test, not evidence of persistence on a live service.
"""

import asyncio
import json
from collections import defaultdict
from collections.abc import Callable
from copy import deepcopy
from types import SimpleNamespace
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
from durabletask.worker import _ActivityExecutor
from test_workflow_sdk_history_replay import _LOGGER, _af_replay, _replay, _worker
from typing_extensions import Never

from agent_framework_durabletask import DurableWorkflowClient
from agent_framework_durabletask._workflows.naming import subworkflow_instance_id


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


def _downstream(*, nested: bool, child_first: bool, twice: bool = True) -> tuple[Workflow, dict[str, Any]]:
    controls: dict[str, Any] = {"seen": [], "client": None, "cascade": False}

    class Seed(Executor):
        @handler(input=str, output=str)
        async def handle(self, message: str, ctx: WorkflowContext[str]) -> None:
            await ctx.send_message(message)
            if twice:
                await ctx.send_message(message, target_id="sub")

    class Parent(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            ctx.set_state("conflict", "parent")
            await ctx.request_info("parent", response_type=int, request_id="a")

        @response_handler(request=str, response=int, output=int)
        async def answer(self, original_request: str, response: int, ctx: WorkflowContext[int]) -> None:
            assert ctx.get_state("conflict") == "writer"
            controls["seen"].append(("parent", response))
            ctx.set_state("conflict", "reply")
            # The producer only sends a normal workflow message. Signaling the
            # child here would mock away the downstream routing dependency.
            await ctx.send_message(response)

    class Writer(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            ctx.set_state("conflict", "writer")

    class Bridge(Executor):
        @handler(input=int, output=int)
        async def handle(self, message: int, ctx: WorkflowContext[int]) -> None:
            assert ctx.get_state("conflict") == "reply"
            controls["seen"].append(("bridge", message))
            ctx.set_state("conflict", "bridge")
            await ctx.send_message(message + 1)

    class Signaler(Executor):
        @handler(input=int, workflow_output=dict)
        async def handle(self, message: int, ctx: WorkflowContext[Never, dict]) -> None:
            assert ctx.get_state("conflict") == "bridge"
            controls["seen"].append(("signaler", message))
            if controls["client"] is not None:
                client: DurableWorkflowClient = controls["client"]
                requests = client.get_pending_hitl_requests("root")
                paths = [request["request_id"] for request in requests]
                assert len(paths) == 2 and all(path.endswith("~b") for path in paths)
                for ordinal, path in enumerate(paths):
                    if controls["cascade"] and ordinal == 0:
                        continue
                    client.send_hitl_response("root", path, message + ordinal)
            await ctx.yield_output({"signal": message})

    class Child(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            await ctx.request_info("child", response_type=int, request_id="b")

        @response_handler(request=str, response=int, workflow_output=dict)
        async def answer(self, original_request: str, response: int, ctx: WorkflowContext[Never, dict]) -> None:
            controls["seen"].append(("child", response))
            await ctx.yield_output({"child": response})

    class Collector(Executor):
        # A nested WorkflowExecutor advertises Any for direct workflow outputs.
        # Accept that declared boundary without weakening the actual value oracle.
        @handler(input=Any, workflow_output=dict)
        async def handle(self, message: Any, ctx: WorkflowContext[Never, dict]) -> None:
            assert isinstance(message, dict) and set(message) == {"child"}
            assert type(message["child"]) is int
            assert ctx.get_state("conflict") == "bridge"
            if controls["cascade"] and message == {"child": 12}:
                client: DurableWorkflowClient = controls["client"]
                requests = client.get_pending_hitl_requests("root")
                assert [request["request_id"] for request in requests] == ["sub~0~b"]
                client.send_hitl_response("root", "sub~0~b", 11)
            await ctx.yield_output(message)

    child = Child(id="child")
    inner = WorkflowBuilder(name="causal-leaf", start_executor=child, output_from=[child]).build()
    if nested:
        leaf = WorkflowExecutor(inner, id="leaf", propagate_request=True, allow_direct_output=True)
        inner = WorkflowBuilder(name="causal-middle", start_executor=leaf, output_from=[leaf]).build()
    sub = WorkflowExecutor(inner, id="sub", propagate_request=True)
    seed, parent, writer = Seed(id="seed"), Parent(id="parent"), Writer(id="writer")
    bridge, signaler, collector = Bridge(id="bridge"), Signaler(id="signaler"), Collector(id="collector")
    targets = [sub, parent, writer] if child_first else [parent, writer, sub]
    workflow = (
        WorkflowBuilder(name="causal-root", start_executor=seed, output_from=[signaler, collector])
        .add_fan_out_edges(seed, targets)
        .add_edge(parent, bridge)
        .add_edge(bridge, signaler)
        .add_edge(sub, collector)
        .build()
    )
    return workflow, controls


def _cold_both(transport: _Episodes, workflow: Workflow) -> dict[str, Any]:
    transport.cold("root")
    before = deepcopy(transport.executed)
    result = _af_replay(transport.histories["root"], workflow, instance="root")
    assert not result["isDone"]
    expected = {key: value for key, value in transport.statuses["root"].items() if key != "events"}
    assert result["customStatus"] == expected
    assert transport.executed == before
    return result


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("child_first", [False, True])
@pytest.mark.parametrize("early", [False, True])
@pytest.mark.parametrize("reverse_children", [False, True])
def test_real_downstream_signaler_advances_two_waves_before_child_join_and_cold_replays(
    nested: bool, child_first: bool, early: bool, reverse_children: bool
) -> None:
    workflow, controls = _downstream(nested=nested, child_first=child_first)
    transport = _Episodes(workflow)
    controls["client"] = transport.client
    transport.client.start_workflow("go", instance_id="root")
    root_start = next(
        event.executionStarted for event in transport.histories["root"] if event.HasField("executionStarted")
    )
    assert not root_start.HasField("parentInstance")
    if early:
        # Real SDK event buffering, before the activity even publishes request a.
        transport.signal("root", event_name="a", data=10)
        transport.flush()
    transport.complete_named("root", "seed")
    children = [subworkflow_instance_id("root", "sub", ordinal) for ordinal in range(2)]
    owners = [subworkflow_instance_id(child, "leaf", 0) if nested else child for child in children]
    expected_parents = {child: "root" for child in children}
    if nested:
        expected_parents.update(zip(owners, children, strict=True))
    for child, parent in expected_parents.items():
        started = next(
            event.executionStarted for event in transport.histories[child] if event.HasField("executionStarted")
        )
        assert started.parentInstance.orchestrationInstance.instanceId == parent
    for owner in owners:
        transport.complete_named(owner, "child")
        transport.cold(owner)
        child_replay = _af_replay(transport.histories[owner], workflow, instance=owner)
        assert not child_replay["isDone"]
        assert child_replay["customStatus"] == {
            key: value for key, value in transport.statuses[owner].items() if key != "events"
        }
    local_order = ["parent", "writer"] if early else ["writer", "parent"]
    transport.complete_named("root", local_order[0])
    assert controls["seen"] == []
    _cold_both(transport, workflow)
    transport.complete_named("root", local_order[1])
    if not early:
        transport.reply("a", 10)
    transport.complete_named("root", "parent")
    assert controls["seen"] == [("parent", 10)]
    # This assertion fails with the old reply_results barrier. The handler has
    # returned, no child has completed, yet a real bridge activity must exist.
    assert [action.scheduleTask.name for action in transport.actions["root"].values()] == ["dafx-causal-root-bridge"]
    assert not transport.completions
    _cold_both(transport, workflow)
    transport.complete_named("root", "bridge")
    replay = _cold_both(transport, workflow)
    actions = _atomic_actions(replay["actions"])
    assert [action["externalEventName"] for action in actions if action["actionType"] == 6] == ["a"]
    assert len([action for action in actions if action["actionType"] == 2]) == 2
    assert [action["functionName"] for action in actions if action["actionType"] == 0][-3:] == [
        "dafx-causal-root-parent",
        "dafx-causal-root-bridge",
        "dafx-causal-root-signaler",
    ]
    transport.complete_named("root", "signaler")
    assert controls["seen"] == [("parent", 10), ("bridge", 10), ("signaler", 11)]
    assert not transport.completions and not transport.actions["root"]
    # Admission is checkpointed by each child response activity. A sent event
    # alone does not retire the child's still-unvalidated pending request.
    suffix = "leaf~0~b" if nested else "b"
    assert transport.pending() == {f"sub~0~{suffix}", f"sub~1~{suffix}"}
    _cold_both(transport, workflow)
    first, second = (1, 0) if reverse_children else (0, 1)
    transport.complete_named(owners[first], "child")
    assert transport.statuses["root"]["subworkflows"] == {"sub": {str(second): children[second]}}
    transport.complete_named("root", "collector")
    _cold_both(transport, workflow)
    transport.complete_named(owners[second], "child")
    # A ready child's output may drive work needed by another pending child.
    # Preserve send order within each result, not the original invocation order
    # of independently paused children. Replay preserves this history order.
    transport.complete_named("root", "collector")
    completion = transport.completions["root"]
    expected = [{"signal": 11}, {"child": 11 + first}, {"child": 11 + second}]
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.loads(completion.result.value) == expected
    assert "subworkflows" not in transport.statuses["root"]
    final = _af_replay(transport.histories["root"], workflow, instance="root")
    assert final["isDone"] and final["output"] == expected
    assert len(transport.executed) == 12
    parent_payloads = [payload for instance, _, payload in transport.executed if instance == "root"]
    assert [payload["shared_state_snapshot"].get("conflict") for payload in parent_payloads[3:6]] == [
        "writer",
        "reply",
        "bridge",
    ]


def test_ready_later_child_routes_to_collector_that_answers_earlier_child() -> None:
    workflow, controls = _downstream(nested=False, child_first=True)
    transport = _Episodes(workflow)
    controls.update(client=transport.client, cascade=True)
    transport.client.start_workflow("go", instance_id="root")
    transport.complete_named("root", "seed")
    children = [subworkflow_instance_id("root", "sub", ordinal) for ordinal in range(2)]
    for instance in children:
        transport.complete_named(instance, "child")
    transport.complete_named("root", "parent")
    transport.complete_named("root", "writer")
    transport.reply("a", 10)
    for executor in ("parent", "bridge", "signaler"):
        transport.complete_named("root", executor)
    transport.complete_named(children[1], "child")
    assert transport.pending() == {"sub~0~b"}
    assert transport.statuses["root"]["subworkflows"] == {"sub": {"0": children[0]}}
    # Original-invocation ordered buffering would deadlock right here.
    _cold_both(transport, workflow)
    transport.complete_named("root", "collector")
    transport.complete_named(children[0], "child")
    transport.complete_named("root", "collector")
    expected = [{"signal": 11}, {"child": 12}, {"child": 11}]
    assert json.loads(transport.completions["root"].result.value) == expected
    assert _af_replay(transport.histories["root"], workflow, instance="root")["output"] == expected


def test_waiting_for_child_at_wave_budget_does_not_spend_another_iteration() -> None:
    workflow, _ = _mixed()
    workflow.max_iterations = 7
    transport = _Episodes(workflow)
    transport.client.start_workflow("go", instance_id="root")
    for executor in ("seed", "parent", "writer", "sink"):
        transport.complete_named("root", executor)
    transport.complete_named(subworkflow_instance_id("root", "sub", 0), "child")
    # Rejections execute only admission, not handlers or state changes.
    for _ in range(3):
        transport.reply("a", "invalid")
        transport.complete_named("root", "parent")
    transport.reply("a", 10)
    transport.complete_named("root", "parent")
    transport.complete_named("root", "sink")
    transport.reply("c", 12)
    transport.complete_named("root", "parent")
    transport.complete_named("root", "sink")
    _cold_both(transport, workflow)
    transport.reply("sub~0~b", 11)
    transport.complete_named(subworkflow_instance_id("root", "sub", 0), "child")
    assert transport.completions["root"].orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED


@pytest.mark.parametrize("early", [False, True])
@pytest.mark.parametrize("typed_rejection", [False, True], ids=["malformed-envelope", "activity-rejection"])
def test_rejected_event_is_consumed_once_before_corrected_reply_on_cold_sdks(
    early: bool, typed_rejection: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    workflow, controls = _mixed(sibling_request=True)
    transport = _Episodes(workflow)
    transport.client.start_workflow("go", instance_id="root")
    invalid: Any = "invalid" if typed_rejection else {"__pickled__": "not-a-checkpoint", "__type__": "builtins:str"}

    def deliver_invalid() -> None:
        if typed_rejection:
            transport.reply("a", invalid)
        else:
            # Exercise the worker's raw event boundary. The public client
            # correctly rejects this malformed envelope before delivery.
            transport.signal("root", event_name="a", data=invalid)
            transport.flush()

    if early:
        deliver_invalid()
    transport.complete_named("root", "seed")
    transport.complete_named(subworkflow_instance_id("root", "sub", 0), "child")
    for executor in ("parent", "writer", "sink"):
        transport.complete_named("root", executor)
    if not early:
        deliver_invalid()
    pending = deepcopy(transport.statuses["root"]["pending_requests"])
    assert len(transport.actions["root"]) == 1 and controls["seen"] == []
    assert transport.pending() == {"a", "d", "sub~0~b"}
    # Both typed rejection and a discarded malformed envelope need an activity ack.
    transport.complete_named("root", "parent")
    assert transport.actions["root"] == {} and controls["seen"] == []
    assert controls["snapshots"] == []
    assert len([item for item in transport.executed if item[0] == "root"]) == 5
    assert transport.pending() == {"a", "d", "sub~0~b"}
    assert transport.statuses["root"]["pending_requests"] == pending
    transport.cold("root")
    rejected_history = deepcopy(transport.histories["root"])
    rejected_status = deepcopy(transport.statuses["root"])
    recorded = [
        json.loads(json.loads(event.taskCompleted.result.value))
        for event in rejected_history
        if event.HasField("taskCompleted")
    ]
    assert [result for result in recorded if "hitl_admission" in result] == [
        {
            "hitl_admission": {"request_id": "a", "status": "invalidreply"},
            "sent_messages": [],
            "outputs": [],
            "events": [],
            "shared_state_updates": {},
            "shared_state_deletes": [],
            "pending_request_info_events": [],
        }
    ]

    transport.reply("a", 10)
    transport.complete_named("root", "parent")
    transport.complete_named("root", "sink")
    assert controls["seen"] == [("a", 10)]
    assert controls["snapshots"] == ["writer"]
    assert len([item for item in transport.executed if item[0] == "root"]) == 7
    assert transport.pending() == {"c", "d", "sub~0~b"}
    transport.cold("root")
    assert [
        payload["message"]["response"]
        for instance, _, payload in transport.executed
        if instance == "root" and payload["is_hitl_response"]
    ] == (["invalid", 10] if typed_rejection else [None, 10])
    assert [
        payload["message"].get("validation_error", False)
        for instance, _, payload in transport.executed
        if instance == "root" and payload["is_hitl_response"]
    ] == [not typed_rejection, False]
    assert [
        json.loads(json.loads(event.taskCompleted.result.value))["hitl_admission"]
        for event in transport.histories["root"]
        if event.HasField("taskCompleted")
        and "hitl_admission" in json.loads(json.loads(event.taskCompleted.result.value))
    ] == [{"request_id": "a", "status": "invalidreply"}, {"request_id": "a", "status": "accepted"}]

    pytest.importorskip("agent_framework_azurefunctions")
    from agent_framework_azurefunctions._workflow_af_context import AzureFunctionsWorkflowContext

    original_wait = AzureFunctionsWorkflowContext.wait_for_external_event
    registrations: list[str] = []

    def observe_wait(context: AzureFunctionsWorkflowContext, name: str) -> Any:
        registrations.append(name)
        # Bound malformed-event replay too: repeating a consumed deferred event
        # would otherwise keep re-registering synchronously inside the AF SDK.
        assert registrations.count(name) <= (2 if name == "a" else 1), registrations
        return original_wait(context, name)

    monkeypatch.setattr(AzureFunctionsWorkflowContext, "wait_for_external_event", observe_wait)
    for history, status, corrected in (
        (rejected_history, rejected_status, False),
        (transport.histories["root"], transport.statuses["root"], True),
    ):
        registrations.clear()
        replay = _af_replay(history, workflow, instance="root")
        assert not replay["isDone"]
        assert replay["customStatus"] == {key: value for key, value in status.items() if key != "events"}
        assert registrations == ["a", "d", "a", *(["c"] if corrected else [])]
        actions = _atomic_actions(replay["actions"])
        # Compare the full activity schedule, not just pending status. Reusing
        # the buffered invalid reply can schedule an extra validation activity
        # while leaving the visible request/status snapshot unchanged.
        assert [
            (action["functionName"], json.loads(json.loads(action["input"])))
            for action in actions
            if action["actionType"] == 0
        ] == [
            (event.taskScheduled.name, json.loads(json.loads(event.taskScheduled.input.value)))
            for event in history
            if event.HasField("taskScheduled")
        ]
        assert controls["seen"] == [("a", 10)]


@pytest.mark.parametrize("nested", [False, True])
def test_core_routes_parent_reply_through_bridge_and_signaler_while_child_waits(nested: bool) -> None:
    async def trial() -> None:
        workflow, controls = _downstream(nested=nested, child_first=False, twice=False)
        start = await workflow.run("go")
        assert {event.request_id for event in start.get_request_info_events()} == {"a", "b"}
        resumed = await workflow.run(responses={"a": 10})
        assert controls["seen"] == [("parent", 10), ("bridge", 10), ("signaler", 11)]
        assert resumed.get_outputs() == [{"signal": 11}]
        assert set(await workflow._runner_context.get_pending_request_info_events()) == {"b"}
        completed = await workflow.run(responses={"b": 11})
        assert completed.get_outputs() == [{"child": 11}]

    asyncio.run(trial())


@pytest.mark.parametrize("failure", ["child", "bridge", "terminated"])
def test_downstream_failure_or_termination_does_not_dispatch_signaler(failure: str) -> None:
    workflow, controls = _downstream(nested=False, child_first=False)
    transport = _Episodes(workflow)
    controls["client"] = transport.client
    transport.client.start_workflow("go", instance_id="root")
    transport.complete_named("root", "seed")
    transport.complete_named("root", "parent")
    transport.complete_named("root", "writer")
    transport.reply("a", 10)
    transport.complete_named("root", "parent")
    if failure == "child":
        _, task_id = transport.parents[subworkflow_instance_id("root", "sub", 0)]
        event = helpers.new_sub_orchestration_failed_event(task_id, RuntimeError("downstream failure"))
    elif failure == "bridge":
        event = helpers.new_task_failed_event(next(iter(transport.actions["root"])), RuntimeError("downstream failure"))
    else:
        event = helpers.new_terminated_event(encoded_output='"cancelled"')
    result = transport.episode("root", event)
    assert len(result.actions) == 1 and result.actions[0].HasField("completeOrchestration")
    completion = transport.completions["root"]
    expected = pb.ORCHESTRATION_STATUS_TERMINATED if failure == "terminated" else pb.ORCHESTRATION_STATUS_FAILED
    assert completion.orchestrationStatus == expected
    assert controls["seen"] == [("parent", 10)]
    cold = _replay(transport.worker, "root", transport.histories["root"])
    assert cold.actions[-1].completeOrchestration.orchestrationStatus == expected
    if failure != "terminated":
        assert transport.statuses["root"]["pending_requests"] == {}
        assert "subworkflows" not in transport.statuses["root"]
        with pytest.raises(Exception, match="downstream failure") as raised:
            _af_replay(transport.histories["root"], workflow, instance="root")
        state = json.loads(str(raised.value).split("$OutOfProcData$:", 1)[1])
        assert state["customStatus"] == {"state": "failed", "pending_requests": {}}


def _mixed(
    *, nested: bool = False, child_first: bool = False, twice: bool = False, sibling_request: bool = False
) -> tuple[Workflow, dict[str, Any]]:
    controls: dict[str, Any] = {"seen": [], "snapshots": [], "send": None, "joined_routing": True}

    class Seed(Executor):
        @handler(input=str, output=str)
        async def handle(self, message: str, ctx: WorkflowContext[str]) -> None:
            await ctx.send_message(message)
            if twice:
                await ctx.send_message(message, target_id="sub")

    class Parent(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            ctx.set_state("conflict", "parent")
            await ctx.request_info("parent", response_type=int, request_id="a")
            if sibling_request:
                await ctx.request_info("sibling", response_type=int, request_id="d")

        @response_handler(request=str, response=int, output=dict, workflow_output=dict)
        async def answer(self, original_request: str, response: int, ctx: WorkflowContext[dict, dict]) -> None:
            controls["seen"].append((ctx.request_id, response))
            controls["snapshots"].append(ctx.get_state("conflict"))
            if ctx.request_id == "a":
                assert ctx.get_state("conflict") == "writer"
                ctx.set_state("conflict", "reply")
                if controls["send"] is not None:
                    controls["send"](response + 1)
                await ctx.request_info("follow-up", response_type=int, request_id="c")
            else:
                assert ctx.request_id in ("c", "d") and ctx.get_state("conflict") == "reply"
            await ctx.yield_output({ctx.request_id: response})
            await ctx.send_message({ctx.request_id: response})

    class Writer(Executor):
        @handler(input=str, output=dict, workflow_output=dict)
        async def handle(self, message: str, ctx: WorkflowContext[dict, dict]) -> None:
            ctx.set_state("conflict", "writer")
            await ctx.yield_output({"writer": 1})
            await ctx.send_message({"writer": 1})

    class Child(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            await ctx.request_info("child", response_type=int, request_id="b")

        @response_handler(request=str, response=int, workflow_output=dict)
        async def answer(self, original_request: str, response: int, ctx: WorkflowContext[Never, dict]) -> None:
            controls["seen"].append(("b", response))
            await ctx.yield_output({"b": response})

    class Sink(Executor):
        @handler(input=dict, workflow_output=dict)
        async def handle(self, message: dict, ctx: WorkflowContext[Never, dict]) -> None:
            if controls["joined_routing"]:
                assert ctx.get_state("conflict") == ("writer" if "writer" in message else "reply")
            await ctx.yield_output({"sink": message})

    child = Child(id="child")
    inner = WorkflowBuilder(name="mixed-leaf", start_executor=child, output_from=[child]).build()
    if nested:
        leaf = WorkflowExecutor(inner, id="leaf", propagate_request=True, allow_direct_output=True)
        inner = WorkflowBuilder(name="mixed-middle", start_executor=leaf, output_from=[leaf]).build()
    sub = WorkflowExecutor(inner, id="sub", propagate_request=True, allow_direct_output=True)
    seed, parent, writer, sink = Seed(id="seed"), Parent(id="parent"), Writer(id="writer"), Sink(id="sink")
    targets = [sub, parent, writer] if child_first else [parent, writer, sub]
    workflow = (
        WorkflowBuilder(name="mixed-root", start_executor=seed, output_from=[parent, writer, sub, sink])
        .add_fan_out_edges(seed, targets)
        .add_edge(parent, sink)
        .add_edge(writer, sink)
        .build()
    )
    return workflow, controls


@pytest.mark.parametrize("child_first", [False, True])
@pytest.mark.parametrize("writer_first", [False, True])
@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("child_before_ack", [False, True])
def test_direct_signal_control_preserves_wave_state_and_outputs(
    child_first: bool, writer_first: bool, nested: bool, child_before_ack: bool
) -> None:
    workflow, controls = _mixed(nested=nested, child_first=child_first, twice=True)
    transport = _Episodes(workflow)
    assert transport.client.start_workflow("go", instance_id="root") == "root"
    transport.complete_named("root", "seed")
    child_ids = [subworkflow_instance_id("root", "sub", ordinal) for ordinal in range(2)]
    owners = [subworkflow_instance_id(child, "leaf", 0) if nested else child for child in child_ids]
    paths = [f"sub~{ordinal}~" + ("leaf~0~" if nested else "") + "b" for ordinal in range(2)]
    for owner in owners:
        transport.complete_named(owner, "child")
    order = ["writer", "parent"] if writer_first else ["parent", "writer"]
    transport.complete_named("root", order[0])
    if not writer_first:
        assert transport.pending() == {"a", *paths}
        # The response is accepted while the sibling activity is still pending.
        # Its handler must not see a partial/shared-state merge.
        transport.reply("a", 10)
        assert controls["seen"] == []
        transport.cold("root")
    transport.complete_named("root", order[1])
    # Ordinary sibling work runs before admitting a reply, without joining
    # children. Its state snapshot contains the entire local wave's writes.
    transport.complete_named("root", "sink")
    if writer_first:
        assert transport.pending() == {"a", *paths}
        transport.reply("a", "invalid")
        transport.complete_named("root", "parent")
        assert controls["seen"] == [] and transport.pending() == {"a", *paths}
        transport.reply("a", 10)

    controls["send"] = lambda value: transport.client.send_hitl_response("root", paths[0], value)

    def finish_child_before_handler_ack() -> None:
        transport.flush()
        transport.complete_named(owners[0], "child")
        assert transport.statuses["root"]["subworkflows"] == {"sub": {"1": child_ids[1]}}
        transport.cold("root")

    transport.complete_named("root", "parent", before_ack=finish_child_before_handler_ack if child_before_ack else None)
    transport.complete_named("root", "sink")
    first_seen = [("a", 10), ("b", 11)] if child_before_ack else [("a", 10)]
    assert controls["seen"] == first_seen
    assert transport.pending() == ({"c", paths[1]} if child_before_ack else {"c", *paths})
    transport.cold("root")
    if not child_before_ack:
        transport.cold(owners[0])
    assert set(transport.statuses["root"]["subworkflows"]["sub"]) == ({"1"} if child_before_ack else {"0", "1"})
    # Respond to c while one child response handler is scheduled and another
    # child remains unanswered. Existing waits survive both handler iterations.
    transport.reply("c", 12)
    transport.complete_named("root", "parent")
    transport.complete_named("root", "sink")
    assert controls["seen"] == [*first_seen, ("c", 12)]
    if not child_before_ack:
        transport.complete_named(owners[0], "child")
    assert transport.pending() == {paths[1]}
    assert transport.statuses["root"]["subworkflows"] == {"sub": {"1": child_ids[1]}}
    transport.cold("root")
    transport.native.raise_orchestration_event.reset_mock()
    with pytest.raises(ValueError, match="No active sub-workflow"):
        transport.client.send_hitl_response("root", paths[0], 99)
    transport.native.raise_orchestration_event.assert_not_called()
    transport.reply(paths[1], 20)
    transport.complete_named(owners[1], "child")
    assert transport.actions["root"] == {}
    completion = transport.completions["root"]
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    # Order is dispatch order within a ready wave. An unfinished child does
    # not delay a later wave's outputs. Counts and values remain unchanged.
    assert json.loads(completion.result.value) == [
        {"writer": 1},
        {"sink": {"writer": 1}},
        *([{"b": 11}] if child_before_ack else []),
        {"a": 10},
        {"sink": {"a": 10}},
        {"c": 12},
        {"sink": {"c": 12}},
        *([] if child_before_ack else [{"b": 11}]),
        {"b": 20},
    ]
    assert controls["snapshots"] == ["writer", "reply"]
    assert transport.pending() == set()
    response_calls = [
        payload
        for instance, _, payload in transport.executed
        if instance == "root" and payload["is_hitl_response"] and payload["message"]["response"] != "invalid"
    ]
    assert [payload["message"]["request_id"] for payload in response_calls] == ["a", "c"]
    assert [payload["shared_state_snapshot"]["conflict"] for payload in response_calls] == ["writer", "reply"]


@pytest.mark.parametrize("failure", ["child", "activity", "terminated"])
def test_mixed_sdk_failure_does_not_schedule_or_run_a_pending_reply(failure: str) -> None:
    workflow, controls = _mixed()
    transport = _Episodes(workflow)
    transport.client.start_workflow("go", instance_id="root")
    transport.complete_named("root", "seed")
    transport.complete_named("root", "parent")
    transport.complete_named("root", "writer")
    transport.complete_named("root", "sink")
    transport.complete_named(subworkflow_instance_id("root", "sub", 0), "child")
    assert transport.pending() == {"a", "sub~0~b"}
    if failure == "child":
        _, task_id = transport.parents[subworkflow_instance_id("root", "sub", 0)]
        event = helpers.new_sub_orchestration_failed_event(task_id, RuntimeError("child failed"))
        expected = pb.ORCHESTRATION_STATUS_FAILED
    elif failure == "activity":
        transport.reply("a", 10)
        task_id = next(iter(transport.actions["root"]))
        event = helpers.new_task_failed_event(task_id, RuntimeError("response failed"))
        expected = pb.ORCHESTRATION_STATUS_FAILED
    else:
        event = helpers.new_terminated_event(encoded_output='"cancelled"')
        expected = pb.ORCHESTRATION_STATUS_TERMINATED
    # Termination is host-owned. Do not invent a service delivery of new work
    # after the terminal event in the same episode.
    result = transport.episode("root", event)
    assert len(result.actions) == 1 and result.actions[0].HasField("completeOrchestration")
    assert transport.completions["root"].orchestrationStatus == expected
    assert controls["seen"] == []
    if failure != "terminated":
        assert transport.statuses["root"]["state"] == "failed"
        assert transport.statuses["root"]["pending_requests"] == {}
        assert "subworkflows" not in transport.statuses["root"]


def test_functions_replay_services_parent_reply_while_child_is_pending() -> None:
    workflow, controls = _mixed()
    transport = _Episodes(workflow)
    transport.client.start_workflow("go", instance_id="root")
    transport.complete_named("root", "seed")
    transport.complete_named("root", "parent")
    transport.complete_named("root", "writer")
    transport.complete_named("root", "sink")
    transport.complete_named(subworkflow_instance_id("root", "sub", 0), "child")
    transport.reply("a", 10)
    controls["send"] = lambda value: transport.client.send_hitl_response("root", "sub~0~b", value)
    transport.complete_named("root", "parent")
    transport.complete_named("root", "sink")
    expected = deepcopy(controls["seen"])
    result = _af_replay(transport.histories["root"], workflow, instance="root")
    assert not result["isDone"]
    assert set(result["customStatus"]["pending_requests"]) == {"c"}
    assert result["customStatus"]["subworkflows"] == {"sub": {"0": subworkflow_instance_id("root", "sub", 0)}}
    assert "events" not in result["customStatus"]
    assert controls["seen"] == expected == [("a", 10)]

    actions = _atomic_actions(result["actions"])
    assert [action["externalEventName"] for action in actions if action["actionType"] == 6] == ["a", "c"]
    assert len([action for action in actions if action["actionType"] == 2]) == 1
    assert [action["functionName"] for action in actions if action["actionType"] == 0] == [
        "dafx-mixed-root-seed",
        "dafx-mixed-root-parent",
        "dafx-mixed-root-writer",
        "dafx-mixed-root-sink",
        "dafx-mixed-root-parent",
        "dafx-mixed-root-sink",
    ]


def test_ready_sibling_wait_survives_parent_handler_and_child_completion() -> None:
    workflow, controls = _mixed(sibling_request=True)
    transport = _Episodes(workflow)
    transport.client.start_workflow("go", instance_id="root")
    transport.complete_named("root", "seed")
    transport.complete_named("root", "parent")
    transport.complete_named("root", "writer")
    transport.complete_named("root", "sink")
    transport.complete_named(subworkflow_instance_id("root", "sub", 0), "child")
    assert transport.pending() == {"a", "d", "sub~0~b"}
    transport.reply("a", 10)
    scheduled = set(transport.actions["root"])
    # d's retained SDK wait completes while a's activity is still running.
    transport.reply("d", 13)
    transport.cold("root")
    assert set(transport.actions["root"]) == scheduled and not controls["seen"]
    transport.complete_named("root", "parent")
    transport.complete_named("root", "sink")
    assert controls["seen"] == [("a", 10)]
    assert transport.pending() == {"c", "d", "sub~0~b"}
    transport.cold("root")
    transport.reply("sub~0~b", 11)
    transport.complete_named(subworkflow_instance_id("root", "sub", 0), "child")
    assert transport.pending() == {"c", "d"}
    assert "subworkflows" not in transport.statuses["root"]
    transport.complete_named("root", "parent")
    assert controls["seen"] == [("a", 10), ("b", 11), ("d", 13)]
    for task_id in list(transport.actions["root"]):
        transport.complete("root", task_id)
    transport.cold("root")
    transport.reply("c", 12)
    transport.complete_named("root", "parent")
    transport.complete_named("root", "sink")
    assert transport.completions["root"].orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert controls["seen"] == [("a", 10), ("b", 11), ("d", 13), ("c", 12)]


def test_functions_wait_any_failed_winner_is_raised_not_returned_as_output() -> None:
    pytest.importorskip("agent_framework_azurefunctions")
    from agent_framework_azurefunctions._workflow_af_context import AzureFunctionsWorkflowContext
    from azure.durable_functions.models.actions.NoOpAction import NoOpAction
    from azure.durable_functions.models.ReplaySchema import ReplaySchema
    from azure.durable_functions.models.Task import AtomicTask, WhenAnyTask

    waiting, failed = AtomicTask("waiting", NoOpAction()), AtomicTask("failed", NoOpAction())
    any_task = WhenAnyTask([waiting, failed], ReplaySchema.V3)
    error = RuntimeError("child failed")
    failed.set_value(is_error=True, value=error)
    adapter = AzureFunctionsWorkflowContext(Mock())
    winner = adapter.get_task_result(any_task)
    assert winner is failed and not waiting.is_completed
    with pytest.raises(RuntimeError, match="child failed") as raised:
        adapter.get_task_result(winner)
    assert raised.value is error


@pytest.mark.parametrize("failure", ["child", "response"])
def test_functions_sdk_mixed_failure_ends_the_shared_orchestrator(failure: str) -> None:
    workflow, controls = _mixed()
    transport = _Episodes(workflow)
    transport.client.start_workflow("go", instance_id="root")
    transport.complete_named("root", "seed")
    transport.complete_named("root", "parent")
    transport.complete_named("root", "writer")
    transport.complete_named("root", "sink")
    if failure == "child":
        _, task_id = transport.parents[subworkflow_instance_id("root", "sub", 0)]
        failed = helpers.new_sub_orchestration_failed_event(task_id, RuntimeError("mixed failure"))
    else:
        transport.reply("a", 10)
        task_id = next(iter(transport.actions["root"]))
        failed = helpers.new_task_failed_event(task_id, RuntimeError("mixed failure"))
    history = [*transport.histories["root"], helpers.new_orchestrator_started_event(), failed]
    with pytest.raises(Exception, match="mixed failure") as raised:
        _af_replay(history, workflow, instance="root")
    state = json.loads(str(raised.value).split("$OutOfProcData$:", 1)[1])
    assert state["customStatus"] == {"state": "failed", "pending_requests": {}}
    assert controls["seen"] == []


@pytest.mark.parametrize("nested", [False, True])
def test_core_services_parent_and_child_requests_without_serializing_their_replies(nested: bool) -> None:
    async def trial() -> None:
        workflow, controls = _mixed(nested=nested)
        # Core shares an in-process pending state buffer. Durable activities
        # instead read a dispatched wave snapshot, tested separately above.
        controls["joined_routing"] = False
        started = await workflow.run("go")
        assert {event.request_id for event in started.get_request_info_events()} == {"a", "b"}
        parent = await workflow.run(responses={"a": 10})
        assert controls["seen"] == [("a", 10)]
        assert {"a": 10} in parent.get_outputs()
        pending = await workflow._runner_context.get_pending_request_info_events()
        assert set(pending) == {"b", "c"}
        await workflow.run(responses={"b": 11})
        assert controls["seen"] == [("a", 10), ("b", 11)]
        await workflow.run(responses={"c": 12})
        assert controls["seen"] == [("a", 10), ("b", 11), ("c", 12)]

    asyncio.run(trial())
