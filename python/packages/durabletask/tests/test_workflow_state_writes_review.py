# Copyright (c) Microsoft. All rights reserved.

"""Explicit state intent through Core contexts, registered activities and SDK replay.

These tests use fresh instances. They do not require replay compatibility with
activity results produced before explicit same-value writes were recorded.
"""

import asyncio
import json
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

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
from agent_framework._workflows._state import State
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.worker import _ActivityExecutor
from test_workflow_mixed_hitl_review import _Episodes
from test_workflow_recorded_replay_review import _LOGGER, _af_replay, _replay, _worker
from typing_extensions import Never

from agent_framework_durabletask._workflows.naming import subworkflow_instance_id
from agent_framework_durabletask._workflows.runner_context import CapturingRunnerContext
from agent_framework_durabletask._workflows.serialization import deserialize_value, serialize_value


def _operations(ctx: WorkflowContext, steps: list[tuple[str, Any]]) -> None:
    for operation, value in steps:
        if operation == "set":
            ctx.set_state("conflict", deepcopy(value))
        elif operation == "delete":
            ctx.state.delete("conflict")
        elif operation == "missing-delete":
            with pytest.raises(KeyError):
                ctx.state.delete("missing")
        elif operation == "commit":
            ctx.state.commit()
        elif operation == "discard":
            ctx.state.discard()
        elif operation == "clear":
            ctx.state.clear()
        elif operation == "import":
            ctx.state.import_state(deepcopy(value))
        elif operation == "read":
            assert ctx.get_state("conflict") == value
        elif operation == "absent":
            assert not ctx.state.has("conflict")
            assert ctx.get_state("conflict", "absent") == "absent"
        else:
            raise AssertionError(operation)


class _ScriptedWriter(Executor):
    def __init__(self, steps: list[tuple[str, Any]]) -> None:
        super().__init__(id="writer")
        self.steps = steps

    @handler(input=str)
    async def handle(self, message: str, ctx: WorkflowContext) -> None:
        _operations(ctx, self.steps)


def _registered_result(executor: Executor, snapshot: dict[str, Any]) -> dict[str, Any]:
    workflow = WorkflowBuilder(name="state-intent", start_executor=executor).build()
    native = _worker(workflow)
    payload = json.dumps({
        "message": "go",
        "source_executor_ids": ["seed"],
        "shared_state_snapshot": serialize_value(snapshot),
    })
    raw = _ActivityExecutor(native._registry, _LOGGER, native._data_converter).execute(
        "state-intent-instance", f"dafx-state-intent-{executor.id}", 1, json.dumps(payload)
    )
    assert raw is not None
    return json.loads(json.loads(raw))


def _core_committed_state(executor: Executor, snapshot: dict[str, Any]) -> dict[str, Any]:
    # A real, unmodified Core State and Executor.execute are the paired control.
    # No activity journal, durable delta extraction or version-detection probe.
    state = State()
    state.import_state(deepcopy(snapshot))
    asyncio.run(executor.execute("go", ["seed"], state, CapturingRunnerContext()))
    state.commit()
    return serialize_value(state.export_state())


@pytest.mark.parametrize(
    ("initial", "steps", "updates", "deletes", "final"),
    [
        pytest.param(
            {"conflict": "writer"},
            [("read", "writer"), ("commit", None)],
            {},
            [],
            {"conflict": "writer"},
            id="inherited-read-is-not-write",
        ),
        pytest.param(
            {"conflict": "writer"},
            [("set", "writer")],
            {"conflict": "writer"},
            [],
            {"conflict": "writer"},
            id="explicit-same-value",
        ),
        pytest.param(
            {"conflict": "writer"},
            [("set", "parent"), ("set", "writer")],
            {"conflict": "writer"},
            [],
            {"conflict": "writer"},
            id="write-back-to-original",
        ),
        pytest.param(
            {"conflict": "writer"},
            [("set", "parent"), ("delete", None), ("absent", None)],
            {},
            ["conflict"],
            {},
            id="set-delete-inherited",
        ),
        pytest.param(
            {},
            [("set", "parent"), ("delete", None), ("absent", None)],
            {},
            ["conflict"],
            {},
            id="set-delete-new-key",
        ),
        pytest.param(
            {"conflict": "writer"},
            [("delete", None), ("set", "writer")],
            {"conflict": "writer"},
            [],
            {"conflict": "writer"},
            id="delete-set-original",
        ),
        pytest.param(
            {},
            [("set", "parent"), ("delete", None), ("set", "writer")],
            {"conflict": "writer"},
            [],
            {"conflict": "writer"},
            id="new-key-set-delete-set",
        ),
        pytest.param(
            {"conflict": "writer"},
            [("set", "writer"), ("discard", None)],
            {},
            [],
            {"conflict": "writer"},
            id="discard-equal-write",
        ),
        pytest.param(
            {"conflict": "writer"},
            [("delete", None), ("discard", None), ("read", "writer")],
            {},
            [],
            {"conflict": "writer"},
            id="discard-delete",
        ),
        pytest.param(
            {},
            [("set", "parent"), ("delete", None), ("discard", None)],
            {},
            [],
            {},
            id="discard-new-key-set-delete",
        ),
        pytest.param(
            {"conflict": "writer"},
            [("set", "writer"), ("commit", None), ("discard", None)],
            {"conflict": "writer"},
            [],
            {"conflict": "writer"},
            id="discard-does-not-undo-commit",
        ),
        pytest.param(
            {"conflict": "writer"},
            [("delete", None), ("commit", None), ("set", "writer"), ("discard", None)],
            {},
            ["conflict"],
            {},
            id="discard-replacement-after-committed-delete",
        ),
        pytest.param(
            {"conflict": "writer"},
            [("missing-delete", None)],
            {},
            [],
            {"conflict": "writer"},
            id="failed-delete-is-not-intent",
        ),
        pytest.param(
            {"conflict": "writer"},
            [("import", {"conflict": "writer"}), ("discard", None)],
            {"conflict": "writer"},
            [],
            {"conflict": "writer"},
            id="import-is-immediate-write",
        ),
        pytest.param(
            {"conflict": "writer"},
            [("set", "pending"), ("import", {"conflict": "imported"}), ("read", "pending"), ("discard", None)],
            {"conflict": "imported"},
            [],
            {"conflict": "imported"},
            id="import-does-not-overwrite-pending",
        ),
        pytest.param(
            {"conflict": "writer", "keep": "value"},
            [("clear", None), ("discard", None)],
            {},
            ["conflict", "keep"],
            {},
            id="clear-is-immediate",
        ),
        pytest.param(
            {"conflict": "writer", "keep": "value"},
            [("clear", None), ("set", "writer")],
            {"conflict": "writer"},
            ["keep"],
            {"conflict": "writer"},
            id="clear-set-original",
        ),
        pytest.param(
            {},
            [("set", "pending"), ("clear", None)],
            {},
            ["conflict"],
            {},
            id="clear-new-pending-key",
        ),
    ],
)
def test_public_state_operations_retain_intent_and_core_buffer_semantics(
    initial: dict[str, Any],
    steps: list[tuple[str, Any]],
    updates: dict[str, Any],
    deletes: list[str],
    final: dict[str, Any],
) -> None:
    # Core itself is the value/buffering oracle. The literal update/delete
    # expectations separately assert intent, which export_state cannot expose.
    core_state = State()
    core_state.import_state(deepcopy(initial))
    asyncio.run(_ScriptedWriter(steps).execute("go", ["seed"], core_state, CapturingRunnerContext()))
    core_state.commit()
    assert core_state.export_state() == final

    original = deepcopy(initial)
    result = _registered_result(_ScriptedWriter(steps), initial)
    assert deserialize_value(result["shared_state_updates"]) == updates
    assert result["shared_state_deletes"] == deletes
    assert initial == original
    merged = {**initial, **deserialize_value(result["shared_state_updates"])}
    for key in result["shared_state_deletes"]:
        merged.pop(key, None)
    assert merged == final


@dataclass
class _Box:
    value: Any


@pytest.mark.parametrize("value", [None, False, 0, 1.0, {"items": [False, 0, 1.0]}, _Box({"items": [1]})])
def test_equal_assignment_is_not_suppressed_for_json_or_checkpoint_values(value: Any) -> None:
    before = {"conflict": deepcopy(value), "untouched": {"items": [False, 0, 1.0]}}
    original = json.dumps(serialize_value(before), sort_keys=True)
    result = _registered_result(_ScriptedWriter([("set", value)]), before)
    assert set(result["shared_state_updates"]) == {"conflict"}
    actual = deserialize_value(result["shared_state_updates"]["conflict"])
    assert type(actual) is type(value) and actual == value
    assert json.dumps(result["shared_state_updates"]["conflict"], sort_keys=True) == json.dumps(
        serialize_value(value), sort_keys=True
    )
    assert result["shared_state_deletes"] == []
    assert json.dumps(serialize_value(before), sort_keys=True) == original


@pytest.mark.parametrize("access", ["get", "export"])
@pytest.mark.parametrize("write_back", [False, True], ids=["in-place-only", "explicit-set"])
@pytest.mark.parametrize("restore", [False, True])
def test_in_place_mutation_and_root_copy_match_native_core(access: str, write_back: bool, restore: bool) -> None:
    observations: list[str] = []

    class Mutate(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            exported = ctx.state.export_state()
            exported["root_only"] = "not shared"
            exported.pop("keep")
            assert not ctx.state.has("root_only") and ctx.state.has("keep")
            value = exported["nested"] if access == "export" else ctx.get_state("nested")
            value["items"][:] = [0, 1.0]
            if restore:
                value["items"][:] = [False, 1]
            if write_back:
                ctx.set_state("nested", value)
                ctx.state.commit()
            # Core 1.13 exposes nested references, whereas 1.16 copies values.
            # Discard must preserve exactly what the native control observes.
            ctx.state.discard()
            observations.append(json.dumps(ctx.get_state("nested"), sort_keys=True))

    initial = {"nested": {"items": [False, 1]}, "keep": "value"}
    original = json.dumps(initial, sort_keys=True)
    core = _core_committed_state(Mutate(id="mutate"), initial)
    assert set(core) == {"nested", "keep"} and core["keep"] == "value"
    core_nested = json.dumps(core["nested"], sort_keys=True)
    if restore:
        assert core_nested == '{"items": [false, 1]}'
    elif write_back:
        assert core_nested == '{"items": [0, 1.0]}'
    else:
        assert core_nested in ('{"items": [false, 1]}', '{"items": [0, 1.0]}')
    result = _registered_result(Mutate(id="mutate"), initial)
    # Explicit assignment is intent even when restored to the input value.
    expected = {"nested": core["nested"]} if write_back or core_nested != '{"items": [false, 1]}' else {}
    assert json.dumps(result["shared_state_updates"], sort_keys=True) == json.dumps(expected, sort_keys=True)
    assert result["shared_state_deletes"] == []
    assert observations == [core_nested, core_nested]
    assert json.dumps(initial, sort_keys=True) == original


@pytest.mark.parametrize("operation", ["set", "import"])
def test_value_journal_preserves_native_core_copying_without_mutating_caller_input(operation: str) -> None:
    class PassValue(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            supplied: dict[str, Any] = {"nested": {"items": [False, 1]}}
            if operation == "set":
                ctx.set_state("nested", supplied["nested"])
            else:
                ctx.state.import_state(supplied)
            # Recording an operation must not itself mutate its input.
            assert json.dumps(supplied) == '{"nested": {"items": [false, 1]}}'
            supplied["nested"]["items"][:] = [0, 1.0]
            ctx.state.commit()
            exported = ctx.state.export_state()
            exported["root_only"] = "not shared"
            exported.pop("nested")
            assert not ctx.state.has("root_only") and ctx.state.has("nested")

    initial = {"nested": {"items": [False, 1]}, "keep": "value"}
    core = _core_committed_state(PassValue(id="pass-value"), initial)
    result = _registered_result(PassValue(id="pass-value"), initial)
    assert json.dumps(result["shared_state_updates"], sort_keys=True) == json.dumps(
        {"nested": core["nested"]}, sort_keys=True
    )
    assert result["shared_state_deletes"] == []
    assert json.dumps(initial, sort_keys=True) == '{"keep": "value", "nested": {"items": [false, 1]}}'


def _parallel_workflow(mode: str, *, mixed: bool) -> tuple[Workflow, list[dict[str, Any]]]:
    seen: list[dict[str, Any]] = []
    absent = mode == "absent-set-delete"

    class Seed(Executor):
        @handler(input=str, output=str)
        async def handle(self, message: str, ctx: WorkflowContext[str]) -> None:
            if not absent:
                ctx.set_state("conflict", "writer")
            ctx.set_state("nested", {"items": [False, 1]})
            ctx.set_state("keep", "value")
            await ctx.send_message("go")

    class A(Executor):
        @handler(input=str, output=str)
        async def handle(self, message: str, ctx: WorkflowContext[str]) -> None:
            assert ctx.get_state("conflict") == (None if absent else "writer")
            ctx.set_state("conflict", "parent")
            ctx.get_state("nested")["items"][:] = [0, 1.0]
            await ctx.send_message("a")

    class B(Executor):
        @handler(input=str, output=str)
        async def handle(self, message: str, ctx: WorkflowContext[str]) -> None:
            assert ctx.get_state("conflict") == (None if absent else "writer")
            # Check exact wire types, since False == 0 and 1 == 1.0 in Python.
            assert json.dumps(ctx.get_state("nested")) == '{"items": [false, 1]}'
            if mode == "same":
                ctx.set_state("conflict", "writer")
            elif mode in ("set-delete", "absent-set-delete"):
                ctx.set_state("conflict", "temporary")
                ctx.state.delete("conflict")
            elif mode == "delete-set":
                ctx.state.delete("conflict")
                ctx.set_state("conflict", "writer")
            elif mode == "discard":
                ctx.set_state("conflict", "writer")
                ctx.state.discard()
            else:
                assert mode == "inherit"
            await ctx.send_message("b")

    class Bridge(Executor):
        @handler(input=str, output=dict)
        async def handle(self, message: str, ctx: WorkflowContext[dict]) -> None:
            row = {
                "source": message,
                "present": ctx.state.has("conflict"),
                "value": ctx.get_state("conflict"),
                "nested": ctx.get_state("nested"),
                "keep": ctx.get_state("keep"),
            }
            seen.append(deepcopy(row))
            await ctx.send_message(row)

    class Sink(Executor):
        @handler(input=dict, workflow_output=dict)
        async def handle(self, message: dict, ctx: WorkflowContext[Never, dict]) -> None:
            assert ctx.get_state("conflict") == message["value"]
            await ctx.yield_output(message)

    seed, a, b = Seed(id="seed"), A(id="a"), B(id="b")
    bridge, sink = Bridge(id="bridge"), Sink(id="sink")
    targets: list[Executor] = [a, b]
    if mixed:

        class Child(Executor):
            @handler(input=str, workflow_output=str)
            async def handle(self, message: str, ctx: WorkflowContext[Never, str]) -> None:
                await ctx.yield_output("child")

        child = Child(id="child")
        inner = WorkflowBuilder(name="state-child", start_executor=child, output_from=[child]).build()
        targets.append(WorkflowExecutor(inner, id="sub"))
    workflow = (
        WorkflowBuilder(name="state-pipeline", start_executor=seed, output_from=[sink])
        .add_fan_out_edges(seed, targets)
        .add_edge(a, bridge)
        .add_edge(b, bridge)
        .add_edge(bridge, sink)
        .build()
    )
    return workflow, seen


def _complete_all_named(transport: _Episodes, executor: str) -> None:
    task_ids = [
        key for key, action in transport.actions["root"].items() if action.scheduleTask.name.endswith(f"-{executor}")
    ]
    assert len(task_ids) == 2
    for task_id in task_ids:
        transport.complete("root", task_id)


@pytest.mark.parametrize("mixed", [False, True], ids=["local-wave", "held-child-wave"])
@pytest.mark.parametrize("reverse", [False, True], ids=["a-completes-first", "b-completes-first"])
@pytest.mark.parametrize("mode", ["same", "inherit", "discard", "set-delete", "delete-set", "absent-set-delete"])
def test_sdk_dispatch_order_merge_reaches_message_pipeline_independent_of_completion_order(
    mixed: bool, reverse: bool, mode: str
) -> None:
    workflow, seen = _parallel_workflow(mode, mixed=mixed)
    transport = _Episodes(workflow)
    transport.client.start_workflow("go", instance_id="root")
    transport.complete_named("root", "seed")
    original_snapshots = {
        action.scheduleTask.name: json.loads(json.loads(action.scheduleTask.input.value))["shared_state_snapshot"]
        for action in transport.actions["root"].values()
    }
    assert list(original_snapshots) == ["dafx-state-pipeline-a", "dafx-state-pipeline-b"]
    assert json.dumps(original_snapshots["dafx-state-pipeline-a"], sort_keys=True) == json.dumps(
        original_snapshots["dafx-state-pipeline-b"], sort_keys=True
    )
    assert json.dumps(original_snapshots["dafx-state-pipeline-a"]["nested"]) == '{"items": [false, 1]}'
    # Pair A with the same handler on an unmodified native Core State. This is
    # an isolated activity snapshot control, not Core shared-pending fan-out.
    core_workflow, _ = _parallel_workflow(mode, mixed=mixed)
    core_a = _core_committed_state(
        core_workflow.executors["a"], deserialize_value(original_snapshots["dafx-state-pipeline-a"])
    )
    assert core_a["conflict"] == "parent" and core_a["keep"] == "value"
    core_nested = json.dumps(core_a["nested"], sort_keys=True)
    assert core_nested in ('{"items": [false, 1]}', '{"items": [0, 1.0]}')
    for index, executor in enumerate(["b", "a"] if reverse else ["a", "b"]):
        transport.complete_named("root", executor)
        if index == 0:
            assert seen == []
            assert not any(
                action.scheduleTask.name.endswith("-bridge") for action in transport.actions["root"].values()
            )
            transport.cold("root")

    scheduled = {
        event.eventId: event.taskScheduled.name
        for event in transport.histories["root"]
        if event.HasField("taskScheduled")
    }
    results = {
        scheduled[event.taskCompleted.taskScheduledId]: json.loads(json.loads(event.taskCompleted.result.value))
        for event in transport.histories["root"]
        if event.HasField("taskCompleted")
    }
    a_updates: dict[str, Any] = {"conflict": "parent"}
    if core_nested != '{"items": [false, 1]}':
        a_updates["nested"] = core_a["nested"]
    assert json.dumps(results["dafx-state-pipeline-a"]["shared_state_updates"], sort_keys=True) == json.dumps(
        a_updates, sort_keys=True
    )
    assert results["dafx-state-pipeline-a"]["shared_state_deletes"] == []
    b_result = results["dafx-state-pipeline-b"]
    deleting = mode in ("set-delete", "absent-set-delete")
    expected_updates = {"conflict": "writer"} if mode in ("same", "delete-set") else {}
    assert b_result["shared_state_updates"] == expected_updates
    assert b_result["shared_state_deletes"] == (["conflict"] if deleting else [])
    expected_value = None if deleting else ("parent" if mode in ("inherit", "discard") else "writer")
    expected = [
        {
            "source": source,
            "present": not deleting,
            "value": expected_value,
            "nested": core_a["nested"],
            "keep": "value",
        }
        for source in ("a", "b")
    ]
    transport.cold("root")
    _complete_all_named(transport, "bridge")
    assert json.dumps(seen, sort_keys=True) == json.dumps(expected, sort_keys=True)
    _complete_all_named(transport, "sink")
    if mixed:
        assert "root" not in transport.completions
        transport.cold("root")
        transport.complete_named(subworkflow_instance_id("root", "sub", 0), "child")
    completion = transport.completions["root"]
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.dumps(json.loads(completion.result.value), sort_keys=True) == json.dumps(expected, sort_keys=True)
    # Later dispatches carry A's native-Core-visible state, never B's inherited
    # root. No replay episode executes a producer or reconstructs a write diff.
    for _, _, payload in transport.executed:
        if payload["message"] in ("a", "b"):
            snapshot = payload["shared_state_snapshot"]
            assert json.dumps(snapshot["nested"], sort_keys=True) == core_nested
            assert snapshot.get("conflict") == expected_value
    before_replay = deepcopy(seen)
    cold = _replay(transport.worker, "root", transport.histories["root"])
    assert len(cold.actions) == 1 and cold.actions[0].HasField("completeOrchestration")
    assert json.dumps(json.loads(cold.actions[0].completeOrchestration.result.value), sort_keys=True) == json.dumps(
        expected, sort_keys=True
    )
    assert json.dumps(seen, sort_keys=True) == json.dumps(before_replay, sort_keys=True)
    # The AF SDK consumes these checkpointed results too. This is translated
    # SDK history, not a live Functions-host test or old-history compatibility.
    af = _af_replay(transport.histories["root"], workflow, instance="root")
    assert af["isDone"]
    assert json.dumps(af["output"], sort_keys=True) == json.dumps(expected, sort_keys=True)
    assert json.dumps(seen, sort_keys=True) == json.dumps(before_replay, sort_keys=True)


def test_hitl_admission_keeps_rejected_state_empty_and_journals_the_accepted_handler() -> None:
    seen: list[list[int]] = []

    class Gate(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            ctx.set_state("conflict", "writer")
            await ctx.request_info("decision", response_type=list[int], request_id="approval")

        @response_handler(request=str, response=list[int], output=str)
        async def answer(self, original_request: str, response: list[int], ctx: WorkflowContext[str]) -> None:
            seen.append(response)
            ctx.set_state("conflict", "writer")
            await ctx.send_message("observe")

    class Sink(Executor):
        @handler(input=str, workflow_output=str)
        async def handle(self, message: str, ctx: WorkflowContext[Never, str]) -> None:
            await ctx.yield_output(ctx.get_state("conflict"))

    gate, sink = Gate(id="gate"), Sink(id="sink")
    workflow = WorkflowBuilder(name="state-hitl", start_executor=gate, output_from=[sink]).add_edge(gate, sink).build()
    transport = _Episodes(workflow)
    transport.client.start_workflow("go", instance_id="root")
    transport.complete_named("root", "gate")
    transport.reply("approval", ["invalid"])
    transport.complete_named("root", "gate")
    rejected = json.loads(json.loads(transport.histories["root"][-1].taskCompleted.result.value))
    assert rejected["hitl_admission"] == {"request_id": "approval", "status": "invalidreply"}
    assert rejected["shared_state_updates"] == {} and rejected["shared_state_deletes"] == []
    assert rejected["sent_messages"] == [] and seen == []
    assert transport.pending() == {"approval"}
    transport.cold("root")
    transport.reply("approval", [7])
    transport.complete_named("root", "gate")
    completed = [event for event in transport.histories["root"] if event.HasField("taskCompleted")]
    accepted = json.loads(json.loads(completed[-1].taskCompleted.result.value))
    assert accepted["hitl_admission"] == {"request_id": "approval", "status": "accepted"}
    assert accepted["shared_state_updates"] == {"conflict": "writer"}
    assert accepted["shared_state_deletes"] == [] and seen == [[7]]
    transport.cold("root")
    transport.complete_named("root", "sink")
    assert json.loads(transport.completions["root"].result.value) == ["writer"]
    assert _af_replay(transport.histories["root"], workflow, instance="root")["output"] == ["writer"]
    assert seen == [[7]]
