# Copyright (c) Microsoft. All rights reserved.

"""Tuple descriptors and registered JSON reply admission against the public Core runner."""

import asyncio
import json
from copy import deepcopy
from typing import Any, Tuple, get_args, get_origin

import pytest
from durabletask.internal import orchestrator_service_pb2 as pb
from test_workflow_generic_hitl_review import _core_trial, _generic_workflow
from test_workflow_mixed_hitl_review import _Episodes
from test_workflow_recorded_replay_review import _replay

from agent_framework_durabletask._workflows.serialization import deserialize_response_type, deserialize_value


def _activity_result(episodes: _Episodes) -> dict[str, Any]:
    completed = [event.taskCompleted for event in episodes.histories["root"] if event.HasField("taskCompleted")]
    return json.loads(json.loads(completed[-1].result.value))


def _pending(annotation: Any) -> tuple[_Episodes, list[Any]]:
    workflow, seen = _generic_workflow(annotation)
    episodes = _Episodes(workflow)
    episodes.client.start_workflow("go", instance_id="root")
    episodes.complete_named("root", "gate")
    assert episodes.pending() == {"approval"} and seen == []
    return episodes, seen


def _assert_tuple(value: Any, expected: tuple[Any, ...]) -> None:
    assert type(value) is tuple and len(value) == len(expected)
    assert [(type(item), item) for item in value] == [(type(item), item) for item in expected]


@pytest.mark.parametrize("annotation", [tuple, Tuple, Tuple[()], tuple[()]], ids=["class", "typing", "empty", "pep585"])
def test_registered_tuple_request_preserves_alias_origin_even_without_arguments(annotation: Any) -> None:
    episodes, _ = _pending(annotation)
    result = _activity_result(episodes)
    expected: Any = (
        "builtins:tuple" if annotation is tuple else {"_durable_response_type": 1, "kind": "tuple", "args": []}
    )
    descriptor = result["pending_request_info_events"][0]["response_type"]
    event_descriptor = next(event["response_type"] for event in result["events"] if event["type"] == "request_info")
    assert descriptor == expected and event_descriptor == expected
    decoded = deserialize_response_type(descriptor)
    if annotation is tuple:
        assert decoded is tuple
    else:
        assert get_origin(annotation) is get_origin(decoded) is tuple
        assert get_args(annotation) == get_args(decoded) == ()
    # Core currently admits nonempty native tuples even for its empty aliases.
    # Preserve Core's behavior rather than imposing a new length restriction.
    for native in ((), (0, False)):
        accepted, original_seen = asyncio.run(_core_trial(annotation, native))
        decoded_accepted, decoded_seen = asyncio.run(_core_trial(decoded, native))
        assert accepted and decoded_accepted
        assert len(original_seen) == len(decoded_seen) == 1
        _assert_tuple(original_seen[0], native)
        _assert_tuple(decoded_seen[0], native)


@pytest.mark.parametrize(
    ("annotation", "wire", "typed"),
    [
        pytest.param(Tuple[()], [], (), id="empty-typing"),
        pytest.param(tuple[()], [], (), id="empty-pep585"),
        pytest.param(Tuple, [], (), id="unparameterized-typing"),
        pytest.param(tuple, [], (), id="concrete-tuple"),
        pytest.param(Tuple[()], [0], (), id="empty-typing-nonempty-json"),
        pytest.param(tuple[()], [0], (), id="empty-pep585-nonempty-json"),
        pytest.param(Tuple, [0], (), id="unparameterized-nonempty-json"),
        pytest.param(Tuple[()], None, (), id="empty-null"),
        pytest.param(tuple[int, bool], [0, False], (0, False), id="fixed-exact-types"),
        pytest.param(tuple[int, bool], [0, 0], (0, False), id="fixed-invalid-bool"),
        pytest.param(tuple[int, bool], [], (0, False), id="fixed-invalid-length"),
        pytest.param(tuple[int, ...], [0, 1], (0, 1), id="variadic"),
        pytest.param(tuple[int, ...], [], (), id="variadic-empty"),
        pytest.param(tuple[int, ...], ["bad"], (0,), id="variadic-invalid-member"),
    ],
)
def test_registered_tuple_json_admission_matches_installed_core(
    annotation: Any, wire: Any, typed: tuple[Any, ...]
) -> None:
    # Public Core admission is the oracle, not durable's serializer/coercer.
    # Core 1.13 rejects JSON tuple reconstruction that Core 1.16 supports.
    accepted, core_seen = asyncio.run(_core_trial(annotation, deepcopy(wire)))
    native_accepted, native_seen = asyncio.run(_core_trial(annotation, typed))
    assert native_accepted and len(native_seen) == 1
    _assert_tuple(native_seen[0], typed)
    if annotation is tuple:
        assert not accepted
    episodes, seen = _pending(annotation)
    pending = deepcopy(episodes.statuses["root"]["pending_requests"])
    episodes.reply("approval", deepcopy(wire))
    episodes.complete_named("root", "gate")
    result = _activity_result(episodes)
    assert result["hitl_admission"] == {"request_id": "approval", "status": "accepted" if accepted else "invalidreply"}
    if accepted:
        assert len(core_seen) == len(seen) == 1
        _assert_tuple(core_seen[0], typed)
        _assert_tuple(seen[0], typed)
        completed = episodes.completions["root"]
        assert completed.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
        output = deserialize_value(json.loads(completed.result.value))
        assert len(output) == 1 and set(output[0]) == {"value"}
        _assert_tuple(output[0]["value"], typed)
        assert episodes.pending() == set()
        for _ in range(2):
            cold = _replay(episodes.worker, "root", episodes.histories["root"])
            assert len(cold.actions) == 1 and cold.actions[0].HasField("completeOrchestration")
            assert cold.actions[0].completeOrchestration == completed
            assert len(seen) == 1
    else:
        assert core_seen == seen == [] and "root" not in episodes.completions
        assert episodes.statuses["root"]["pending_requests"] == pending
        assert (result["shared_state_updates"], result["shared_state_deletes"], result["outputs"]) == ({}, [], [])
        episodes.cold("root")
