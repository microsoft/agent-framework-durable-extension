# Copyright (c) Microsoft. All rights reserved.

"""Consumer-local state baselines through real registered activities and SDK merges."""

import inspect
import json
import os
import subprocess
import sys
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from _workflow_replay_test_support import _Episodes, _replay
from _workflow_state_process_support import SET_MEMBERS, process_identity, state_workflow
from _workflow_state_test_support import _core_committed_state, _registered_result
from agent_framework import Executor, WorkflowContext, handler
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb

from agent_framework_durabletask._workflows.naming import subworkflow_instance_id
from agent_framework_durabletask._workflows.serialization import deserialize_value


@pytest.fixture
def activity_process() -> Callable[..., dict[str, Any]]:
    # Export pytest's effective import paths, including a runner-selected Core
    # override. Children execute only the support module, not pytest collection.
    package = Path(__file__).resolve().parents[1]
    support = Path(__file__).with_name("_workflow_state_process_support.py").resolve()
    identity = process_identity()
    assert Path(identity["durable"]) == package / "agent_framework_durabletask/__init__.py"
    assert Path(identity["activity"]) == package / "agent_framework_durabletask/_workflows/activity.py"
    for symbol, name in (
        (state_workflow, "_workflow_state_process_support.py"),
        (_Episodes, "_workflow_replay_test_support.py"),
        (_registered_result, "_workflow_state_test_support.py"),
    ):
        assert Path(inspect.getfile(symbol)).resolve() == support.parent / name
    paths = [str(package), str(support.parent), *(str(Path(path).resolve()) for path in sys.path if path)]
    environment = {**os.environ, "PYTHONPATH": os.pathsep.join(paths), "PYTHONDONTWRITEBYTECODE": "1"}

    def execute(episodes: _Episodes, executor: str, *, seed: int, mixed: bool) -> dict[str, Any]:
        matches = [
            (task_id, action.scheduleTask)
            for task_id, action in episodes.actions["root"].items()
            if action.scheduleTask.name == f"dafx-state-process-{executor}"
        ]
        assert len(matches) == 1
        task_id, task = matches[0]
        result = subprocess.run(
            [sys.executable, str(support)],
            input=json.dumps({"mixed": mixed, "name": task.name, "task_id": task_id, "input": task.input.value}),
            cwd=package,
            env={**environment, "PYTHONHASHSEED": str(seed)},
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=20,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        child = json.loads(result.stdout)
        assert child["identity"] == identity
        assert child["pid"] != os.getpid() and child["hash_seed"] == str(seed)
        episodes.actions["root"].pop(task_id)
        episodes.executed.append(("root", task_id, json.loads(json.loads(task.input.value))))
        # Replay the exact SDK-converter result, without manufacturing a delta.
        episodes.episode("root", helpers.new_task_completed_event(task_id, child["wire"]))
        episodes.flush()
        return child

    return execute


@pytest.mark.parametrize("mixed", [False, True], ids=["local-wave", "held-child-wave"])
@pytest.mark.parametrize("reverse", [False, True], ids=["writer-first", "reader-first"])
def test_read_only_set_in_another_process_does_not_overwrite_sibling(
    mixed: bool, reverse: bool, activity_process: Callable[..., dict[str, Any]]
) -> None:
    workflow, _ = state_workflow(mixed=mixed)
    episodes = _Episodes(workflow)
    episodes.client.start_workflow("go", instance_id="root")
    produced = activity_process(episodes, "seed", seed=1, mixed=mixed)
    producer_value = json.loads(json.loads(produced["wire"]))["shared_state_updates"]["x"]
    decoded = deserialize_value(producer_value)
    assert type(decoded) is set and decoded == set(SET_MEMBERS)
    snapshots = [
        json.loads(json.loads(action.scheduleTask.input.value))["shared_state_snapshot"]
        for action in episodes.actions["root"].values()
    ]
    assert len(snapshots) == 2 and snapshots[0] == snapshots[1]
    assert snapshots[0]["x"] == producer_value
    consumers: dict[str, dict[str, Any]] = {}
    for executor in ["reader", "writer"] if reverse else ["writer", "reader"]:
        consumers[executor] = activity_process(episodes, executor, seed=2, mixed=mixed)
    read = consumers["reader"]["observed"]
    assert read["members"] == list(SET_MEMBERS)
    assert producer_value["__pickled__"] != read["encoding"]["__pickled__"], (
        "Fixture requires equal sets with different producer/consumer pickle bytes"
    )
    writer = json.loads(json.loads(consumers["writer"]["wire"]))
    reader = json.loads(json.loads(consumers["reader"]["wire"]))
    assert writer["shared_state_updates"] == {"x": "sibling-write"}
    assert reader["shared_state_updates"] == {} and reader["shared_state_deletes"] == []
    episodes.complete_named("root", "sink")
    if mixed:
        assert "root" not in episodes.completions
        episodes.complete_named(subworkflow_instance_id("root", "sub", 0), "child")
    completed = episodes.completions["root"]
    assert completed.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    output = json.loads(completed.result.value)
    assert output == [{"kind": "str", "value": "sibling-write", "keep": [False, 0]}]
    assert type(output[0]["keep"][0]) is bool and type(output[0]["keep"][1]) is int
    before = deepcopy(episodes.executed)
    for _ in range(2):
        cold = _replay(episodes.worker, "root", episodes.histories["root"])
        assert len(cold.actions) == 1 and cold.actions[0].HasField("completeOrchestration")
        assert cold.actions[0].completeOrchestration == completed
        assert episodes.executed == before


@dataclass(eq=False)
class _NoEqualityBox:
    items: list[Any]

    def __eq__(self, other: object) -> bool:
        raise AssertionError("State delta extraction must not compare reconstructed Python objects")


@pytest.mark.parametrize("boxed", [False, True], ids=["json", "checkpoint-object"])
@pytest.mark.parametrize("write_back", [False, True], ids=["implicit-mutation", "explicit-write"])
@pytest.mark.parametrize(
    ("initial", "replacement", "restore"),
    [(False, 0, False), (0, False, False), (1, 1.0, False), (1.0, 1, False), (False, 0, True)],
    ids=["false-to-zero", "zero-to-false", "int-to-float", "float-to-int", "restore-original"],
)
def test_nested_type_changes_and_explicit_intent_follow_core_without_native_equality(
    boxed: bool, write_back: bool, initial: Any, replacement: Any, restore: bool
) -> None:
    class Mutate(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            value = ctx.get_state("nested")
            items = value.items if boxed else value["items"]
            items[0] = replacement
            if restore:
                items[0] = initial
            if write_back:
                ctx.set_state("nested", value)

    snapshot: dict[str, Any] = {
        "nested": _NoEqualityBox([initial]) if boxed else {"items": [initial]},
        "keep": [False, 0],
    }
    # Native State decides whether get() exposes or copies a nested value.
    # The delta contract separately requires explicit writes, even equal ones.
    native = deserialize_value(_core_committed_state(Mutate(id="mutate"), snapshot))
    native_value = native["nested"].items[0] if boxed else native["nested"]["items"][0]
    if write_back or restore:
        expected_value = initial if restore else replacement
        assert type(native_value) is type(expected_value) and native_value == expected_value
    else:
        assert (type(native_value), native_value) in ((type(initial), initial), (type(replacement), replacement))
    changed = type(native_value) is not type(initial)
    result = _registered_result(Mutate(id="mutate"), snapshot)
    assert set(result["shared_state_updates"]) == ({"nested"} if write_back or changed else set())
    if result["shared_state_updates"]:
        actual = deserialize_value(result["shared_state_updates"]["nested"])
        actual_value = actual.items[0] if boxed else actual["items"][0]
        assert type(actual_value) is type(native_value) and actual_value == native_value
    assert result["shared_state_deletes"] == []
    original = snapshot["nested"].items[0] if boxed else snapshot["nested"]["items"][0]
    assert type(original) is type(initial) and original == initial
