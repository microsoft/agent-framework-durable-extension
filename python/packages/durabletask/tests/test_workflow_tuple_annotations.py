# Copyright (c) Microsoft. All rights reserved.

"""Tuple descriptors and registered JSON reply admission against the public Core runner."""

import asyncio
import builtins
import importlib
import json
import pickle
from copy import deepcopy
from types import GenericAlias
from typing import Any, Tuple, get_args, get_origin
from unittest.mock import Mock

import pytest
from durabletask.internal import orchestrator_service_pb2 as pb
from test_workflow_generic_hitl_review import _core_trial, _generic_workflow
from test_workflow_mixed_hitl_review import _Episodes
from test_workflow_recorded_replay_review import _replay

from agent_framework_durabletask._workflows.serialization import (
    deserialize_response_type,
    deserialize_value,
    serialize_response_type,
)

_EMPTY_ARGUMENT_ALIAS = GenericAlias(tuple, ((),))
_EMPTY_ARGUMENT_DESCRIPTOR = {
    "_durable_response_type": 1,
    "kind": "tuple",
    "args": [{"kind": "empty_tuple"}],
}


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


def _assert_native_admission_matches_core(annotation: Any, decoded: Any, native: tuple[Any, ...]) -> None:
    accepted, original_seen = asyncio.run(_core_trial(annotation, native))
    decoded_accepted, decoded_seen = asyncio.run(_core_trial(decoded, native))
    assert decoded_accepted is accepted
    if accepted:
        assert len(original_seen) == len(decoded_seen) == 1
        _assert_tuple(original_seen[0], native)
        _assert_tuple(decoded_seen[0], native)
    else:
        assert original_seen == decoded_seen == []


@pytest.mark.parametrize(
    "annotation",
    [tuple, Tuple, Tuple[()], tuple[()], _EMPTY_ARGUMENT_ALIAS],
    ids=["class", "typing", "empty", "pep585", "explicit-empty-argument"],
)
def test_registered_tuple_request_preserves_alias_origin_and_arguments(annotation: Any) -> None:
    episodes, _ = _pending(annotation)
    result = _activity_result(episodes)
    expected: Any = "builtins:tuple"
    if annotation is not tuple:
        expected = {
            "_durable_response_type": 1,
            "kind": "tuple",
            "args": [{"kind": "empty_tuple"}] if get_args(annotation) == ((),) else [],
        }
    descriptor = result["pending_request_info_events"][0]["response_type"]
    event_descriptor = next(event["response_type"] for event in result["events"] if event["type"] == "request_info")
    assert descriptor == expected and event_descriptor == expected
    decoded = deserialize_response_type(descriptor)
    if annotation is tuple:
        assert decoded is tuple
    else:
        assert get_origin(annotation) is get_origin(decoded) is tuple
        assert get_args(annotation) == get_args(decoded)
    # Python 3.10 exposes ((),) for typing.Tuple[()], unlike the zero-argument
    # aliases. Preserve the installed Core's rejection as well as its admission.
    for native in ((), (0,), (0, False)):
        _assert_native_admission_matches_core(annotation, decoded, native)


def test_empty_tuple_argument_descriptor_preserves_shape_without_hooks(monkeypatch: pytest.MonkeyPatch) -> None:
    forbidden = Mock(side_effect=AssertionError("Tuple descriptors must not import, evaluate or unpickle"))
    monkeypatch.setattr(importlib, "import_module", forbidden)
    monkeypatch.setattr(builtins, "eval", forbidden)
    monkeypatch.setattr(pickle, "loads", forbidden)
    # A descriptor written on Python 3.10 must not become the zero-argument
    # typing.Tuple[()] alias when a Python 3.13 reader reconstructs it.
    descriptor = json.loads(json.dumps(_EMPTY_ARGUMENT_DESCRIPTOR))
    decoded = deserialize_response_type(descriptor)
    assert get_origin(decoded) is tuple and get_args(decoded) == ((),)
    assert serialize_response_type(decoded) == descriptor
    forbidden.assert_not_called()


@pytest.mark.parametrize("kind", ["list", "set", "dict", "tuple", "union"])
def test_nested_empty_tuple_argument_keeps_its_own_tuple_scope(kind: str) -> None:
    args: list[Any] = [deepcopy(_EMPTY_ARGUMENT_DESCRIPTOR)]
    if kind == "dict":
        args.insert(0, "builtins:str")
    elif kind in ("tuple", "union"):
        args.append("builtins:int")
    descriptor = {"_durable_response_type": 1, "kind": kind, "args": args}
    decoded = deserialize_response_type(json.loads(json.dumps(descriptor)))
    nested = get_args(decoded)[1 if kind == "dict" else 0]
    assert get_origin(nested) is tuple and get_args(nested) == ((),)
    assert serialize_response_type(decoded) == descriptor


@pytest.mark.parametrize(
    "descriptor",
    [
        {"kind": "empty_tuple"},
        {"_durable_response_type": 1, "kind": "empty_tuple", "args": []},
        {"_durable_response_type": 1, "kind": "tuple", "args": [{"kind": "empty_tuple", "extra": None}]},
        {"_durable_response_type": 1, "kind": "tuple", "args": [{"kind": "empty_tuple"}, "builtins:int"]},
        {"_durable_response_type": 1, "kind": "tuple", "args": ["builtins:int", {"kind": "empty_tuple"}]},
        {"_durable_response_type": 1, "kind": "tuple", "args": [{"kind": "empty_tuple"}, {"kind": "ellipsis"}]},
        {"_durable_response_type": 1, "kind": "tuple", "args": [{"kind": "empty_tuple"}, {"kind": "empty_tuple"}]},
        {"_durable_response_type": 1, "kind": "tuple", "args": [[]]},
        {"_durable_response_type": 1, "kind": "list", "args": [{"kind": "empty_tuple"}]},
        {"_durable_response_type": 1, "kind": "set", "args": [{"kind": "empty_tuple"}]},
        {"_durable_response_type": 1, "kind": "dict", "args": ["builtins:str", {"kind": "empty_tuple"}]},
        {"_durable_response_type": 1, "kind": "union", "args": [{"kind": "empty_tuple"}, "builtins:int"]},
    ],
)
def test_empty_tuple_argument_marker_is_not_a_general_annotation(descriptor: Any) -> None:
    with pytest.raises(ValueError, match="HITL"):
        deserialize_response_type(descriptor)


@pytest.mark.parametrize(
    "annotation",
    [
        (),
        GenericAlias(list, ((),)),
        GenericAlias(set, ((),)),
        GenericAlias(dict, (str, ())),
        GenericAlias(tuple, ((), int)),
        GenericAlias(tuple, (int, ())),
        GenericAlias(tuple, ((), Ellipsis)),
        GenericAlias(tuple, ((), ())),
    ],
)
def test_empty_tuple_value_is_rejected_outside_a_sole_tuple_argument(annotation: Any) -> None:
    with pytest.raises(ValueError, match="Unsupported HITL response annotation"):
        serialize_response_type(annotation)


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
        pytest.param(_EMPTY_ARGUMENT_ALIAS, [], (), id="explicit-empty-argument"),
        pytest.param(tuple[Tuple[()], int], [[], 7], ((), 7), id="nested-empty-typing"),
        pytest.param(tuple[tuple[()], bool], [[], False], ((), False), id="nested-empty-pep585"),
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
    if annotation is tuple:
        assert not accepted
    episodes, seen = _pending(annotation)
    descriptor = _activity_result(episodes)["pending_request_info_events"][0]["response_type"]
    _assert_native_admission_matches_core(annotation, deserialize_response_type(descriptor), typed)
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
