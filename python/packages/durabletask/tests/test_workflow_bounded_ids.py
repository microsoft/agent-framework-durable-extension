# Copyright (c) Microsoft. All rights reserved.

"""Physical child naming is bounded and separate from semantic routing."""

import json
import os
import re
import subprocess
import sys
from collections.abc import Iterator
from typing import Any
from unittest.mock import Mock, patch

import grpc
import pytest
from _workflow_provenance_test_support import _DTStarts, _leaf, _only_action
from agent_framework import WorkflowBuilder, WorkflowExecutor
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.client import TaskHubGrpcClient
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb

from agent_framework_durabletask import DurableWorkflowClient, wrap_workflow_input
from agent_framework_durabletask._workflows.naming import subworkflow_instance_id, validate_dts_instance_id
from agent_framework_durabletask._workflows.protocol import validate_workflow_start_provenance

# Produced independently with Node crypto and Buffer.writeBigUInt64BE, not by
# invoking the Python helper. Freeze framing, field order, UTF-8 and domain bytes.
VECTORS = [
    ("root", "child", 0, "dafxsw_v1_530d1d0c46964f652725aaddecffc73327b1c7d5fd72c7d8fb9ca520a76b1ca1"),
    ("a::b", "c", 0, "dafxsw_v1_e5f4227feaab43d029d6f60e12f1c61dd4b35aa91e5cbeabaf42c7043230784d"),
    ("a", "b::c", 0, "dafxsw_v1_5bd174427b79fa51e5b3c10f4a6a690bc3ee9e5e428c006480b025a195ca974a"),
    ("ROOT", "review", 12, "dafxsw_v1_8d81eadbd0c44a5f607c6255211f4d4e58fad5bd3bba767d90c3861661b90292"),
    ("root", "世界", 7, "dafxsw_v1_6ecef511d82bb0acb909562db296ad85106a3e09c8a26d462326f404b72bec3e"),
]


class _SchedulerSubclass(DurableTaskSchedulerClient):
    pass


class _GenericClientSubclass(TaskHubGrpcClient):
    pass


@pytest.fixture
def inert_channel() -> Iterator[Mock]:
    # Inject the SDK's public channel argument, never create a socket/channel.
    # Stub construction binds RPC callables, but no test may invoke one.
    channel = Mock(spec=grpc.Channel)
    rpc = Mock(side_effect=AssertionError("Unexpected RPC in an offline client test"))
    channel.unary_unary.return_value = rpc
    channel.unary_stream.return_value = rpc
    yield channel
    rpc.assert_not_called()


@pytest.fixture(params=[DurableTaskSchedulerClient, _SchedulerSubclass], ids=["scheduler", "scheduler-subclass"])
def scheduler_client(request: pytest.FixtureRequest, inert_channel: Mock) -> Iterator[DurableTaskSchedulerClient]:
    client_type: type[DurableTaskSchedulerClient] = request.param
    native = client_type(
        host_address="unused.invalid:1", taskhub="unit-tests", token_credential=None, channel=inert_channel
    )
    assert type(native) is client_type
    with native:
        yield native


@pytest.fixture(params=[TaskHubGrpcClient, _GenericClientSubclass], ids=["generic", "generic-subclass"])
def generic_client(request: pytest.FixtureRequest, inert_channel: Mock) -> Iterator[TaskHubGrpcClient]:
    client_type: type[TaskHubGrpcClient] = request.param
    native = client_type(host_address="unused.invalid:1", channel=inert_channel)
    assert type(native) is client_type
    with native:
        yield native


@pytest.mark.parametrize(("parent", "executor", "ordinal", "expected"), VECTORS)
def test_literal_cross_language_vectors(parent: str, executor: str, ordinal: int, expected: str) -> None:
    assert subworkflow_instance_id(parent, executor, ordinal) == expected
    assert len(expected) == 74 and re.fullmatch(r"dafxsw_v1_[0-9a-f]{64}", expected)
    validate_dts_instance_id(expected)


def test_fresh_process_hash_seed_does_not_change_replay_identity() -> None:
    code = (
        "from agent_framework_durabletask._workflows.naming import subworkflow_instance_id;"
        "print(subworkflow_instance_id('root', 'child', 0))"
    )
    for seed in ("0", "8675309"):
        result = subprocess.run(
            [sys.executable, "-c", code],
            env={**os.environ, "PYTHONHASHSEED": seed},
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
        assert result.stdout.strip() == VECTORS[0][3]


def test_component_boundaries_case_and_unicode_are_not_normalized() -> None:
    tuples = [
        ("a::b", "c", 0),
        ("a", "b::c", 0),
        ("root", "child", 0),
        ("Root", "child", 0),
        ("root", "Child", 0),
        ("root", "child", 1),
        ("root", "é", 0),
        ("root", "e\u0301", 0),
    ]
    assert len({subworkflow_instance_id(*parts) for parts in tuples}) == len(tuples)


def test_long_names_and_deep_children_do_not_grow_physical_ids() -> None:
    parent = "r" * 100
    executor = "世界::" * 32
    assert len(executor) == 128
    seen: set[str] = set()
    for ordinal in range(200):
        child = subworkflow_instance_id(parent, executor, ordinal)
        assert child not in seen and len(child) == 74
        validate_dts_instance_id(child)
        seen.add(child)
        parent = child


def test_registered_child_dispatch_bounds_the_107_character_concat_case() -> None:
    root, executor = "r" * 32, "e" * 70
    legacy_id = f"{root}::{executor}::0"
    assert len(legacy_id) == 107
    validate_dts_instance_id(root)
    with pytest.raises(ValueError, match="DTS instance ID"):
        validate_dts_instance_id(legacy_id)

    leaf, echo = _leaf()
    child = WorkflowExecutor(leaf, id=executor, allow_direct_output=True)
    workflow = WorkflowBuilder(name="bounded-root", start_executor=child, output_from=[child]).build()
    # This fixture registers real SDK functions and executes constructed history.
    # It never starts the worker or connects to a service.
    host = _DTStarts(workflow)
    value = {"business": True}
    started = host.start("dafx-bounded-root", root, wrap_workflow_input(value))
    action = _only_action(started, "createSubOrchestration")
    dispatch = action.createSubOrchestration
    assert dispatch.name == "dafx-provenance-leaf"
    assert len(dispatch.instanceId) == 74 and re.fullmatch(r"dafxsw_v1_[0-9a-f]{64}", dispatch.instanceId)
    assert dispatch.instanceId == subworkflow_instance_id(root, executor, 0)
    validate_dts_instance_id(dispatch.instanceId)

    child_id, child_started = host.child(root, action)
    leaf_action = _only_action(child_started, "scheduleTask")
    assert leaf_action.scheduleTask.name == "dafx-provenance-leaf-echo"
    payload = json.loads(json.loads(leaf_action.scheduleTask.input.value))
    assert payload["message"] == value
    assert payload["host_context"] == {
        "instance_id": root,
        "workflow_name": "bounded-root",
        "request_path_prefix": f"{executor}~0~",
    }
    completed = host.complete_activity(child_id, leaf_action)
    terminal = _only_action(completed, "completeOrchestration").completeOrchestration
    assert terminal.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert echo.seen == [value]
    parent = host.replay(root, helpers.new_sub_orchestration_completed_event(action.id, terminal.result.value))
    result = _only_action(parent, "completeOrchestration").completeOrchestration
    assert result.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.loads(result.result.value) == [value]
    cold = _only_action(host.replay(root), "completeOrchestration").completeOrchestration
    assert cold.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert cold.result == result.result and echo.seen == [value]


@pytest.mark.parametrize("parent", ["", None, 42, b"root", "\ud800"])
def test_invalid_parent_rejected(parent: Any) -> None:
    with pytest.raises(ValueError):
        subworkflow_instance_id(parent, "child", 0)


@pytest.mark.parametrize("executor", ["", "bad~id", "x" * 129, None, 42, "\ud800"])
def test_invalid_executor_rejected(executor: Any) -> None:
    with pytest.raises(ValueError):
        subworkflow_instance_id("root", executor, 0)


@pytest.mark.parametrize("ordinal", [-1, True, False, 0.0, "0", None])
def test_invalid_ordinal_rejected(ordinal: Any) -> None:
    with pytest.raises(ValueError):
        subworkflow_instance_id("root", "child", ordinal)


def test_provenance_reconstructs_every_physical_hop_without_changing_semantic_path() -> None:
    root = "r" * 100
    first = subworkflow_instance_id(root, "child::世界", 5)
    second = subworkflow_instance_id(first, "grand hop", 9)
    value = {
        "__subworkflow_input__": {"business": True},
        "__subworkflow_address__": {
            "root_instance_id": root,
            "root_workflow_name": "root",
            "request_path_prefix": "child::世界~5~grand hop~9~",
        },
    }
    before = json.dumps(value)
    validate_workflow_start_provenance(value, instance_id=second, parent_instance_id=first)
    for parent, instance in ((root, second), (None, second), (first, "other")):
        with pytest.raises(ValueError):
            validate_workflow_start_provenance(value, instance_id=instance, parent_instance_id=parent)
    assert json.dumps(value) == before


@pytest.mark.parametrize("instance", [None, "r", "r" * 100, "Case sensitive", "a::b", "~._-", " root "])
def test_scheduler_roots_are_preserved_verbatim(
    instance: str | None, scheduler_client: DurableTaskSchedulerClient
) -> None:
    with patch.object(
        scheduler_client, "schedule_new_orchestration", autospec=True, return_value=instance or "generated"
    ) as schedule:
        client = DurableWorkflowClient(scheduler_client, workflow_name="flow")
        assert client.start_workflow({"input": "data"}, instance_id=instance) == (instance or "generated")
        schedule.assert_called_once_with(
            "dafx-flow", input={"_durable_workflow_version": 2, "input": {"input": "data"}}, instance_id=instance
        )


@pytest.mark.parametrize("instance", ["", " ", " " * 100, "r" * 101, "@reserved", "非ASCII", "a\n", "a\x7f", 1, False])
def test_scheduler_invalid_roots_rejected_before_scheduling(
    instance: Any, scheduler_client: DurableTaskSchedulerClient
) -> None:
    with patch.object(scheduler_client, "schedule_new_orchestration", autospec=True) as schedule:
        with pytest.raises(ValueError, match="DTS instance ID"):
            DurableWorkflowClient(scheduler_client, workflow_name="flow").start_workflow(instance_id=instance)
        schedule.assert_not_called()


def test_scheduler_root_guard_precedes_input_validation(scheduler_client: DurableTaskSchedulerClient) -> None:
    client = DurableWorkflowClient(scheduler_client, workflow_name="flow")
    payload = {"not_json": object()}
    with patch.object(scheduler_client, "schedule_new_orchestration", autospec=True) as schedule:
        with pytest.raises(ValueError, match="DTS instance ID"):
            client.start_workflow(payload, instance_id="@reserved")
        # The same input must reach the real JSON validator with a valid root.
        with pytest.raises(ValueError, match="strict JSON with string keys and finite numbers"):
            client.start_workflow(payload, instance_id="valid-root")
        schedule.assert_not_called()


@pytest.mark.parametrize("instance", ["非ASCII", "r" * 101])
def test_generic_client_keeps_unknown_backend_root_contract(instance: str, generic_client: TaskHubGrpcClient) -> None:
    with patch.object(generic_client, "schedule_new_orchestration", autospec=True, return_value=instance) as schedule:
        assert (
            DurableWorkflowClient(generic_client, workflow_name="flow").start_workflow(instance_id=instance) == instance
        )
        schedule.assert_called_once_with(
            "dafx-flow", input={"_durable_workflow_version": 2, "input": None}, instance_id=instance
        )
