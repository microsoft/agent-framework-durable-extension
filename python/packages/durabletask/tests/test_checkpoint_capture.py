# Copyright (c) Microsoft. All rights reserved.

"""Exact original source and recorded-history integration, with labeled models.

Set CHECKPOINT_CAPTURE_FIXTURES to the preserved migration artifact root. Tests
never download fixtures. That root must contain published/{six pinned modules},
suspended_prefix_fixture.py, suspended_prefix_child_fixture.py, the recorded
suspended_prefix_case_f752b5f27e88 and suspended_prefix_child_case_7b5e7cdfde80
directories, and suspended_prefix_child_ordering_case_1945eceda151.
The agent tests additionally require suspended_prefix_agent_fixture.py,
stateful-customer.registration.json and the two checkpoint-release-candidate
recording directories. Native agent producer checks require the exact original
release's _constants.py and _models.py, explicitly selected below.

Run against the native SDK proposal implementing classic replay fencing and
get_acquired_history(). No capture evidence is injected as runtime acquisition.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import inspect
import json
import logging
import os
import subprocess
import sys
import zipfile
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any, Callable, cast
from unittest.mock import Mock, patch

import pytest
from agent_framework import AgentExecutor, WorkflowBuilder, WorkflowExecutor
from durabletask import task, worker
from durabletask.entities import EntityInstanceId
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.serialization import DEFAULT_DATA_CONVERTER, JsonDataConverter
from google.protobuf.json_format import ParseDict  # type: ignore[import-untyped]

from agent_framework_durabletask._checkpoints._capture_prefix import (
    replay_capture_frontier,
    replay_checkpoint_prefix,
)
from agent_framework_durabletask._checkpoints._continuation import continue_checkpoint
from agent_framework_durabletask._checkpoints._native_prefix import replay_native_prefix
from agent_framework_durabletask._json_payload import JsonPayload, install_json_payload_converter
from agent_framework_durabletask._workflows.dt_context import DurableTaskWorkflowContext
from agent_framework_durabletask.checkpoint_migration import (
    PINS,
    Captured,
    PublishedSource,
    TrustedDefinition,
    TrustedModule,
    UnsupportedContract,
    capture,
    compile_source,
    load_definition,
    source_inventory,
)


def test_public_checkpoint_encoding_preserves_input_and_rejects_terminal_restart():
    checkpoint = Captured(
        boundary="waiting",
        control="running",
        state={"outputs": ["already committed"], "pending": []},
        prefix={"contract": "mechanical-native-prefix/4", "source_transport_profile": "native-sdk-v1"},
        evidence={},
        lowered_source="",
    )
    encoded = json.loads(checkpoint.checkpoint_bytes(original_input={"message": "original"}))
    assert set(encoded) == {"format", "formatVersion", "joinContract", "contract", "state", "prefix", "input"}
    assert encoded["format"] == "agent_framework_durabletask.logical_checkpoint"
    assert encoded["formatVersion"] == 1
    assert encoded["joinContract"] == "published-batch-join/1"
    assert encoded["contract"] == "checkpoint-only-prefix/1"
    assert encoded["input"] == {"message": "original"}
    assert encoded["state"] == {"outputs": ["already committed"], "pending": []}
    assert encoded["prefix"] == checkpoint.prefix
    terminal = Captured("completed", "completed", None, None, {}, "")
    with pytest.raises(ValueError, match="no same-execution continuation"):
        terminal.checkpoint_bytes(original_input=None)


@pytest.fixture(scope="module")
def assets():
    root = os.environ.get("CHECKPOINT_CAPTURE_FIXTURES")
    if root is None:
        pytest.skip("Set CHECKPOINT_CAPTURE_FIXTURES to preserved exact source and recording fixtures.")
    folder = Path(cast(str, root)).resolve()
    files = {name: folder / "published" / f"{name}.py" for name in PINS}
    missing = [str(path) for path in files.values() if not path.is_file()]
    if missing:
        pytest.fail(f"Explicit source fixtures are missing: {missing}")
    paths = [str(folder), str(folder / "registration-candidate")]
    sys.path[:0] = paths
    try:
        yield SimpleNamespace(
            root=folder,
            source=PublishedSource.load(files),
            clean=folder / "suspended_prefix_case_f752b5f27e88",
            child=folder / "suspended_prefix_child_case_7b5e7cdfde80",
            ordering=folder / "suspended_prefix_child_ordering_case_1945eceda151",
            agent=folder / "checkpoint-release-candidate" / "suspended-agent-live-500a5d7166e0",
        )
    finally:
        for path in paths:
            sys.path.remove(path)


def rows(folder, filename="capture-history.json"):
    path = folder / filename
    if not path.is_file():
        pytest.fail(f"Explicit recorded fixture missing: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def definition(module, factory="workflow"):
    loaded = importlib.import_module(module)
    module_path = loaded.__file__
    if module_path is None:
        pytest.fail(f"Trusted source module has no file path: {module}")
    path = Path(module_path)
    return TrustedDefinition(
        getattr(loaded, factory)(),
        (TrustedModule(loaded, path, hashlib.sha256(path.read_bytes()).hexdigest()),),
    )


def compile_case(assets, trusted, history, profile="classic-functions-v1", streaming=False, policy="first"):
    return capture(
        assets.source,
        trusted,
        history,
        event_streaming=streaming,
        shared_state={},
        source_transport_profile=profile,
    )


def runtime_prefix(prefix):
    """Return the source prefix unchanged under the SDK-first contract."""
    return deepcopy(prefix)


def _camel(key):
    first, *rest = key.split("_")
    return first + "".join(part.title() for part in rest)


def _convert(value, failure=False):
    if isinstance(value, list):
        return [_convert(item) for item in value]
    if not isinstance(value, dict):
        return value
    return {
        ("errorMessage" if failure and key == "message" else "spanID" if key == "span_id" else _camel(key)): (
            item if key == "properties" else _convert(item, key in ("failure_details", "inner_failure"))
        )
        for key, item in value.items()
        if item is not None
    }


def native_events(history):
    result = []
    for row in history:
        kind = row["type"][:-5]
        body = _convert({key: value for key, value in row.items() if key not in ("type", "event_id", "timestamp")})
        result.append(
            ParseDict(
                {
                    "eventId": row["event_id"],
                    "timestamp": row["timestamp"],
                    kind[0].lower() + kind[1:]: body,
                },
                pb.HistoryEvent(),
            )
        )
    return result


def consume(captured, trusted, history, profile="classic-functions-v1"):
    """Use real SDK history acquisition and public calls, without a service."""
    native = worker.TaskHubGrpcWorker(emit_trace_spans=False)
    install_json_payload_converter(native)
    start = next(row for row in history if row["type"] == "ExecutionStartedEvent")
    instance, execution = (start["orchestration_instance"][key] for key in ("instance_id", "execution_id"))

    def root(context, ignored):
        replay_prefix = captured.prefix
        handles = {}
        if captured.state["phase"] in ("native_batch", "native_mixed_batch"):
            frontier = yield from replay_capture_frontier(
                context,
                replay_prefix,
                captured.state,
                instance_id=instance,
                execution_id=execution,
            )
            handles["native_batch_tasks"] = frontier["batch"]
        else:
            yield from replay_checkpoint_prefix(
                context,
                replay_prefix,
                captured.state,
                instance_id=instance,
                execution_id=execution,
            )
        return (  # noqa: B901
            yield from continue_checkpoint(
                DurableTaskWorkflowContext(
                    context,
                    request_time_policy="orchestration-clock",
                ),
                trusted.workflow,
                captured.state,
                **handles,
            )
        )

    root.__name__ = start["name"]
    profiles = getattr(task, "ReplayProfile", None)
    if profiles is None:
        pytest.skip("Requires the proposed SDK replay profile API.")
    cast(Callable[..., Any], native.add_orchestrator)(
        root,
        replay_profile=(
            profiles.CLASSIC_FUNCTIONS_V1
            if profile == "classic-functions-v1"
            else profiles.DEFAULT
        ),
    )
    executor = worker._OrchestrationExecutor(native._registry, logging.getLogger("capture"), native._data_converter)
    events = native_events(history)
    dispatch_start = max(
        (index for index, event in enumerate(events) if event.HasField("orchestratorStarted")),
        default=-1,
    )
    if dispatch_start < 0 or not any(event.HasField("executionStarted") for event in events):
        raise AssertionError("Captured replay history must include a start and current dispatch boundary.")
    execute = cast(Callable[..., Any], executor.execute)
    return execute(
        instance,
        events[:dispatch_start],
        events[dispatch_start:],
        execution_id=execution,
    )


@pytest.mark.parametrize("prefix_kind", ["capture", "native"])
def test_external_wait_prefix_keeps_marker_payload_as_plain_json(prefix_kind):
    profiles = getattr(task, "ReplayProfile", None)
    if profiles is None:
        pytest.skip("Requires the proposed SDK replay profile API.")

    worker_instance, value, wire = _marker_aware_worker()
    assert isinstance(worker_instance._data_converter.deserialize(wire), SimpleNamespace)
    assert worker_instance._data_converter.deserialize(wire, JsonPayload) == value

    if prefix_kind == "capture":
        prefix = {
            "contract": "mechanical-native-prefix/4",
            "source_transport_profile": "classic-functions-v1",
            "operation_batches": [[{
                "kind": "external_wait",
                "wait_id": "approval-1",
                "event_name": "approval",
                "source_result": wire,
            }]],
            "children": [],
        }
    else:
        prefix = {
            "contract": "mechanical-native-prefix/2",
            "operation_batches": [[{
                "kind": "external_wait",
                "wait_id": "approval-1",
                "event_name": "approval",
                "source_result": wire,
            }]],
            "children": [],
        }
    checkpoint = {"phase": "prepare", "children": []}
    instance_id = f"external-wait-{prefix_kind}"
    execution_id = f"execution-{prefix_kind}"

    def resume(context, _: Any):
        if prefix_kind == "capture":
            return (
                yield from replay_checkpoint_prefix(
                    context, prefix, checkpoint, instance_id=instance_id, execution_id=execution_id
                )
            )
        return (  # noqa: B901
            yield from replay_native_prefix(
                context, prefix, checkpoint, instance_id=instance_id, execution_id=execution_id
            )
        )

    resume.__name__ = f"external_wait_{prefix_kind}"
    cast(Callable[..., Any], worker_instance.add_orchestrator)(
        resume,
        replay_profile=profiles.CLASSIC_FUNCTIONS_V1,
    )
    executor = worker._OrchestrationExecutor(
        worker_instance._registry,
        logging.getLogger("checkpoint-external-wait"),
        worker_instance._data_converter,
    )
    execute = cast(Callable[..., Any], executor.execute)
    start = helpers.new_execution_started_event(resume.__name__, instance_id, "null")
    start.executionStarted.orchestrationInstance.executionId.value = execution_id
    first = [helpers.new_orchestrator_started_event(), start]
    waiting = execute(instance_id, [], first, execution_id=execution_id)
    assert waiting.actions == []

    result = execute(
        instance_id,
        first,
        [helpers.new_orchestrator_started_event(), helpers.new_event_raised_event("approval", wire)],
        execution_id=execution_id,
    )
    completion = next(
        action.completeOrchestration for action in result.actions if action.HasField("completeOrchestration")
    )
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED, completion


def _marker_aware_worker():
    value = {"$type": "customer-marker", "value": "preserve-json"}
    wire = json.dumps(value)

    class MarkerAwareConverter(JsonDataConverter):
        def deserialize(self, data, target_type=None):
            if data and target_type is None:
                decoded = json.loads(data)
                if isinstance(decoded, dict) and decoded.get("$type") == "customer-marker":
                    return SimpleNamespace(value=decoded["value"])
            return super().deserialize(data, target_type)

    worker_instance = worker.TaskHubGrpcWorker(
        channel=Mock(),
        data_converter=MarkerAwareConverter(),
        emit_trace_spans=False,
    )
    install_json_payload_converter(worker_instance)
    return worker_instance, value, wire


@pytest.mark.parametrize("prefix_kind", ["capture", "native"])
def test_completed_activity_prefix_keeps_marker_result_as_plain_json(prefix_kind):
    profiles = getattr(task, "ReplayProfile", None)
    if profiles is None:
        pytest.skip("Requires the proposed SDK replay profile API.")

    worker_instance, value, wire = _marker_aware_worker()
    instance_id = f"marker-result-{prefix_kind}"
    execution_id = f"execution-{prefix_kind}"
    argument = "source-input"
    operation = {
        "kind": "activity",
        "task_id": 0,
        "name": "marker-activity",
        "python_input": argument,
        "source_input": json.dumps([json.dumps(argument)]),
        "source_result": wire,
        "runtime_input_policy": "classic-host-omits-scheduled-input/1",
    }
    if prefix_kind == "capture":
        prefix = {
            "contract": "mechanical-native-prefix/4",
            "source_transport_profile": "classic-functions-v1",
            "operation_batches": [[operation]],
            "children": [],
        }
        checkpoint = {"phase": "prepare", "children": []}
    else:
        prefix = {
            "contract": "mechanical-native-prefix/2",
            "operation_batches": [[operation]],
            "children": [],
        }
        checkpoint = {"children": []}

    def resume(context, _: Any):
        if prefix_kind == "capture":
            yield from replay_checkpoint_prefix(
                context, prefix, checkpoint, instance_id=instance_id, execution_id=execution_id
            )
        else:
            yield from replay_native_prefix(
                context, prefix, checkpoint, instance_id=instance_id, execution_id=execution_id
            )
        return None  # noqa: B901

    resume.__name__ = f"marker_activity_{prefix_kind}"
    cast(Callable[..., Any], worker_instance.add_orchestrator)(
        resume,
        replay_profile=profiles.CLASSIC_FUNCTIONS_V1,
    )
    executor = worker._OrchestrationExecutor(
        worker_instance._registry,
        logging.getLogger("checkpoint-marker-result"),
        worker_instance._data_converter,
    )
    execute = cast(Callable[..., Any], executor.execute)
    start = helpers.new_execution_started_event(resume.__name__, instance_id, "null")
    start.executionStarted.orchestrationInstance.executionId.value = execution_id
    completed_result = execute(
        instance_id,
        [],
        [
            helpers.new_orchestrator_started_event(),
            start,
            helpers.new_task_scheduled_event(0, "marker-activity", operation["source_input"]),
            helpers.new_task_completed_event(0, wire),
        ],
        execution_id=execution_id,
    )
    completion = next(
        action.completeOrchestration
        for action in completed_result.actions
        if action.HasField("completeOrchestration")
    )
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED, completion
    assert value == json.loads(wire)


@pytest.mark.parametrize("kind", ["activity", "child", "entity"])
@pytest.mark.parametrize("profile", ["classic-functions-v1", "native-sdk-v1"])
def test_capture_prefix_dispatch_decodes_marker_results_as_plain_json(kind, profile):
    from agent_framework_durabletask._checkpoints._capture_prefix import CLASSIC, NATIVE, _dispatch

    worker_instance, value, wire = _marker_aware_worker()
    correlation_id = "00000000-0000-0000-0000-000000000001"
    request_id = "00000000-0000-0000-0000-000000000002"
    entity = str(EntityInstanceId(entity="dafx-marker", key="root"))

    class ResultContext:
        instance_id = "root"
        execution_id = "execution"

        def __init__(self):
            self.calls = []

        def new_uuid(self):
            return correlation_id

        def _result(self, kind, return_type):
            self.calls.append((kind, return_type))
            return worker_instance._data_converter.deserialize(wire, return_type)

        def call_activity(self, name, *, input, return_type=None):
            return self._result("activity", return_type)

        def call_sub_orchestrator(self, name, *, input, instance_id, return_type=None):
            return self._result("child", return_type)

        def call_entity(self, entity_id, operation, *, input, request_id=None, return_type=None):
            return self._result("entity", return_type)

    item = {"kind": kind}
    if kind == "activity":
        item.update(name="marker-activity")
    elif kind == "child":
        item.update(name="marker-child", instance="root::child")
    else:
        item.update(
            entity=entity,
            operation="run",
            correlation_id=correlation_id,
            native_request_id=request_id,
        )
    context = ResultContext()
    result = _dispatch(context, item, "source-input", CLASSIC if profile == CLASSIC else NATIVE)
    assert result == value
    assert context.calls == [(kind, JsonPayload)]


@pytest.mark.parametrize("kind", ["activity", "child", "entity"])
@pytest.mark.parametrize("acquired", [False, True], ids=["new", "acquired"])
@pytest.mark.parametrize("profile", ["classic-functions-v1", "native-sdk-v1"])
def test_future_prefix_dispatch_decodes_marker_results_as_plain_json(kind, acquired, profile):
    from agent_framework_durabletask._checkpoints._capture_prefix import CLASSIC, NATIVE, _dispatch_future

    worker_instance, value, wire = _marker_aware_worker()
    correlation_id = "00000000-0000-0000-0000-000000000001"
    request_id = "00000000-0000-0000-0000-000000000002"
    entity = str(EntityInstanceId(entity="dafx-marker", key="root"))

    class ResultContext:
        instance_id = "root"
        execution_id = "execution"
        current_utc_datetime = datetime(2025, 1, 1, tzinfo=timezone.utc)

        def __init__(self):
            self.calls = []

        def new_uuid(self):
            return correlation_id

        def _result(self, kind, return_type):
            self.calls.append((kind, return_type))
            return worker_instance._data_converter.deserialize(wire, return_type)

        def call_activity(self, name, *, input, return_type=None):
            return self._result("activity", return_type)

        def call_sub_orchestrator(self, name, *, input, instance_id, return_type=None):
            return self._result("child", return_type)

        def call_entity(self, entity_id, operation, *, input, request_id=None, return_type=None):
            return self._result("entity", return_type)

    future = {
        "authorization": "source-future-operation/1",
        "kind": kind,
        "task_id": 4,
    }
    known = {}
    if kind == "activity":
        future.update(name="marker-activity", python_input="source-input")
    elif kind == "child":
        future.update(name="marker-child", instance="root::child", python_input={"value": "source-input"})
        if acquired:
            known[4] = {"input": json.dumps({"value": "acquired-input"})}
    else:
        future.update(entity=entity, operation="run", message="source-message")
        if acquired:
            known[4] = {
                "input": json.dumps({"correlationId": correlation_id, "message": "source-message"}),
                "request_id": request_id,
            }

    context = ResultContext()
    result = _dispatch_future(context, future, known, CLASSIC if profile == CLASSIC else NATIVE)
    assert result == value
    assert context.calls == [(kind, JsonPayload)]


def test_native_prefix_dispatches_marker_results_as_plain_json():
    from agent_framework_durabletask._checkpoints._native_prefix import replay_native_prefix

    worker_instance, value, wire = _marker_aware_worker()
    correlation_id = "00000000-0000-0000-0000-000000000001"
    request_id = "00000000-0000-0000-0000-000000000002"
    entity = str(EntityInstanceId(entity="dafx-marker", key="root"))
    child_instance = "root::child"
    request = {"orchestrationId": "root", "correlationId": correlation_id}
    activity = {
        "kind": "activity",
        "task_id": 0,
        "name": "marker-activity",
        "python_input": "source-input",
        "source_input": json.dumps([json.dumps("source-input")]),
        "source_result": wire,
        "runtime_input_policy": "classic-host-omits-scheduled-input/1",
    }
    entity_operation = {
        "kind": "entity",
        "task_id": 1,
        "entity": entity,
        "operation": "run",
        "python_input": request,
        "source_input": json.dumps(request),
        "source_result": wire,
        "native_request_id": request_id,
        "correlation_id": correlation_id,
    }
    child = {
        "task_id": 2,
        "name": "marker-child",
        "instance": child_instance,
        "input": json.dumps({"value": "source-input"}),
    }
    child_authority = {
        "instance": child_instance,
        "source_parent": {"instance": "root", "execution": "execution", "task": 2},
    }
    prefix = {
        "contract": "mechanical-native-prefix/2",
        "operation_batches": [[activity, entity_operation]],
        "children": [child],
    }
    checkpoint = {"children": [child_authority]}

    class ResultContext:
        instance_id = "root"
        execution_id = "execution"

        def __init__(self):
            self.calls = []

        def new_uuid(self):
            return correlation_id

        def validate_replay_activity(self, *args, **kwargs):
            pass

        def validate_replay_entity(self, *args, **kwargs):
            pass

        def validate_replay_child(self, *args, **kwargs):
            pass

        def _result(self, kind, return_type):
            self.calls.append((kind, return_type))
            return worker_instance._data_converter.deserialize(wire, return_type)

        def call_activity(self, name, *, input, return_type=None):
            return self._result("activity", return_type)

        def call_sub_orchestrator(self, name, *, input, instance_id, return_type=None):
            return self._result("child", return_type)

        def call_entity(self, entity_id, operation, *, input, request_id=None, return_type=None):
            return self._result("entity", return_type)

    context = ResultContext()
    joined = object()
    with patch("durabletask.task.when_all", return_value=joined):
        replay = replay_native_prefix(
            context, prefix, checkpoint, instance_id="root", execution_id="execution"
        )
        assert next(replay) is joined
        with pytest.raises(StopIteration) as completed:
            replay.send([value, value])
        result = completed.value.value
    assert context.calls == [
        ("activity", JsonPayload),
        ("entity", JsonPayload),
        ("child", JsonPayload),
    ]
    assert result == {child_instance: value}


def test_future_child_dispatch_wraps_new_input_and_preserves_acquired_inputs():
    from agent_framework_durabletask._checkpoints._capture_prefix import (
        CLASSIC,
        _dispatch_future,
        _validate_future_acquisition,
    )
    from agent_framework_durabletask._workflows.protocol import wrap_workflow_input
    from agent_framework_durabletask._workflows.serialization import SUBWORKFLOW_ADDRESS_KEY, SUBWORKFLOW_INPUT_KEY

    child_input = {
        SUBWORKFLOW_INPUT_KEY: json.dumps("child payload"),
        SUBWORKFLOW_ADDRESS_KEY: {
            "root_instance_id": "root",
            "root_workflow_name": "root-workflow",
            "request_path_prefix": "child~0~",
        },
    }
    future = {
        "authorization": "source-future-operation/1",
        "kind": "child",
        "task_id": 0,
        "name": "dafx-child-workflow",
        "instance": "root::child::1",
        "python_input": child_input,
    }

    def event(event_type, body):
        return type(event_type, (), {"to_dict": lambda self: body})()

    class RecordingContext:
        instance_id = "root"
        execution_id = "execution"

        def __init__(self, acquired):
            self.acquired = acquired
            self.call = None

        def get_acquired_history(self):
            return self.acquired

        def call_sub_orchestrator(self, name, *, input, instance_id, return_type=None):
            self.call = {"name": name, "input": input, "instance_id": instance_id}
            return self.call

    started = event(
        "ExecutionStartedEvent",
        {"orchestration_instance": {"instance_id": "root", "execution_id": "execution"}},
    )

    def acquired_input(input_value):
        return event(
            "SubOrchestrationInstanceCreatedEvent",
            {
                "event_id": 0,
                "name": future["name"],
                "instance_id": future["instance"],
                "input": input_value,
            },
        )

    expected_current_input = wrap_workflow_input(child_input)
    current_context = RecordingContext(
        [started, acquired_input(json.dumps(expected_current_input))]
    )
    known_current = _validate_future_acquisition(current_context, [], [future], CLASSIC)
    new_context = RecordingContext([])
    _dispatch_future(new_context, future, {}, CLASSIC)
    assert new_context.call == {
        "name": future["name"],
        "input": expected_current_input,
        "instance_id": future["instance"],
    }
    _dispatch_future(current_context, future, known_current, CLASSIC)
    assert current_context.call is not None
    assert current_context.call["input"] == expected_current_input

    legacy_context = RecordingContext([started, acquired_input(json.dumps(json.dumps(child_input)))])
    known_legacy = _validate_future_acquisition(legacy_context, [], [future], CLASSIC)
    _dispatch_future(legacy_context, future, known_legacy, CLASSIC)
    assert legacy_context.call is not None
    assert legacy_context.call["input"] == json.dumps(child_input)


def _execute_with_proposed_identity(
    executor: Any, instance: str, history: list[Any], *, execution_id: str
) -> Any:
    execute = cast(Callable[..., Any], executor.execute)
    if "execution_id" not in inspect.signature(execute).parameters:
        pytest.skip("Requires the proposed SDK orchestration execution identity API.")
    return execute(instance, [], history, execution_id=execution_id)


def _modeled_tail(history):
    """Remove current-only fields in a copy, not from preserved original evidence."""
    result = deepcopy(history)
    for row in result:
        if row["type"] == "TaskCompletedEvent":
            value = json.loads(json.loads(row["result"]))
            value.pop("hitl_admission", None)
            row["result"] = json.dumps(json.dumps(value))
        if row["type"] == "TaskScheduledEvent":
            argument = json.loads(json.loads(json.loads(row["input"])[0]))
            argument.pop("is_hitl_response", None)
            row["input"] = json.dumps([json.dumps(json.dumps(argument))])
    return result


def test_all_source_yields_lower_without_generator_or_frame_inspection(assets):
    reducer, generated, sites = compile_source(assets.source)
    assert [line for line, _ in sites] == [1144, 1169, 1222]
    assert not inspect.isgeneratorfunction(reducer)
    assert "yield " not in generated
    assert "gi_frame" not in generated
    inventory = source_inventory(assets.source)
    declaration = json.loads(
        (Path(inspect.getfile(capture)).parent / "checkpoint_migration_inventory.json").read_text(encoding="utf-8")
    )
    assert {item["line"] for item in declaration["source_scheduling_sites"]} == {
        line for line, _ in inventory["scheduling_calls"] + inventory["yield_sites"]
    }
    assert {line for line, _ in inventory["scheduling_calls"] + inventory["yield_sites"]} == {
        271,
        308,
        333,
        1144,
        1222,
        1169,
    }
    with patch.object(
        assets.source.modules["orchestrator"],
        "run_workflow_orchestrator",
        side_effect=AssertionError("Never replay old generator"),
    ):
        captured = compile_case(assets, definition("suspended_prefix_fixture"), rows(assets.clean))
    assert captured.boundary == "hitl"
    assert captured.control == "suspended"
    assert captured.state["outputs"] == ["prior"]


def test_runtime_delta_imports_without_migration_or_original_source():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import agent_framework_durabletask._checkpoints._capture_prefix; "
                "import agent_framework_durabletask._checkpoints._continuation; "
                "assert 'agent_framework_durabletask.checkpoint_migration' not in sys.modules; "
                "assert not any(name.startswith('_migration_capture_published') for name in sys.modules); "
                "assert not any(name in sys.modules for name in "
                "('release_capture','checkpoint_native_prefix_candidate','checkpoint_continuation_candidate'))"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_recorded_hitl_suspended_and_resumed_consumer(assets):
    trusted = definition("suspended_prefix_fixture")
    captured = compile_case(assets, trusted, rows(assets.clean))
    assert not consume(captured, trusted, rows(assets.clean)).actions
    final = rows(assets.clean, "final-history.json")
    cut = next(i for i, row in enumerate(final) if row["type"] == "TaskScheduledEvent" and row["event_id"] == 1)
    native = consume(captured, trusted, final[:cut])
    assert [a.id for a in native.actions if a.HasField("scheduleTask")] == [1, 2]


def test_real_pre_dispatch_authorizes_only_future_work(assets):
    trusted = definition("suspended_prefix_fixture")
    history = rows(assets.clean)
    cut = next(i for i, row in enumerate(history) if row["type"] == "TaskScheduledEvent")
    captured = compile_case(assets, trusted, history[:cut])
    assert captured.boundary == "prepare"
    assert captured.prefix["operation_batches"] == []
    assert "nativeBatch" not in captured.state
    native = consume(captured, trusted, history[:cut])
    assert [a.id for a in native.actions if a.HasField("scheduleTask")] == [0]


def test_recorded_child_batch_keeps_every_original_handle(assets):
    trusted = definition("suspended_prefix_child_fixture")
    history = rows(assets.child, "source-parent-history.json")
    captured = compile_case(assets, trusted, history)
    assert captured.state["outputs"] == ["parent-prior"]
    operations = captured.prefix["pending_batch"]
    assert [item["task_id"] for item in operations] == [1, 2, 3]
    assert "source_result" in operations[0]
    assert "source_result" not in operations[1]
    assert not consume(captured, trusted, history).actions


def test_recorded_completed_children_then_future_prepare(assets):
    trusted = definition("suspended_prefix_child_fixture")
    history = rows(assets.ordering, "parent-final-history.json")
    cut = next(i for i, row in enumerate(history) if row["type"] == "TaskScheduledEvent" and row["event_id"] == 4)
    captured = compile_case(assets, trusted, history[:cut])
    assert captured.boundary == "prepare"
    assert captured.prefix["contract"] == "mechanical-native-prefix/4"
    assert [row["kind"] for row in captured.prefix["operation_batches"][1]] == ["child"] * 3
    native = consume(captured, trusted, history[:cut])
    assert [a.id for a in native.actions if a.HasField("scheduleTask")] == [4, 5, 6, 7, 8]
    assert not any(a.HasField("createSubOrchestration") for a in native.actions)


def test_completed_children_then_modeled_hitl(assets):
    trusted = definition("suspended_prefix_child_fixture")
    history = _modeled_tail([
        row for row in rows(assets.ordering, "parent-final-history.json") if row["type"] != "ExecutionCompletedEvent"
    ])
    completion = next(row for row in history if row["type"] == "TaskCompletedEvent" and row["task_scheduled_id"] == 4)
    value = json.loads(json.loads(completion["result"]))
    value["pending_request_info_events"] = [
        {
            "request_id": "modeled-after-child",
            "source_executor_id": "finish",
            "data": "approval",
            "request_type": "builtins:str",
            "response_type": "builtins:int",
        }
    ]
    completion["result"] = json.dumps(json.dumps(value))
    captured = compile_case(assets, trusted, history)
    assert captured.boundary == "hitl"
    assert captured.state["waiting"] == ["modeled-after-child"]
    assert not consume(captured, trusted, history).actions


def test_recorded_source_return_before_native_commit_preserves_original_envelope(assets):
    trusted = definition("suspended_prefix_child_fixture", "child_workflow")
    history = rows(assets.child, "source-child-0-history.json")
    cut = next(i for i, row in enumerate(history) if row["type"] == "ExecutionCompletedEvent")
    expected = json.loads(history[cut]["result"])
    captured = compile_case(assets, trusted, history[:cut])
    assert captured.boundary == "awaiting_terminal_commitment"
    assert captured.state["is_child"]
    assert captured.evidence["source_return_value"] == expected
    assert captured.evidence["terminal"] is None
    native = consume(captured, trusted, history[:cut])
    assert len(native.actions) == 1
    assert native.actions[0].completeOrchestration.orchestrationStatus == 1
    assert json.loads(native.actions[0].completeOrchestration.result.value) == expected


def test_later_failed_child_waits_for_siblings_then_preserves_first_failure(assets):
    trusted = definition("suspended_prefix_child_fixture")
    suspended = rows(assets.child, "parent-suspended-after-child-outcome-history.json")
    captured = compile_case(assets, trusted, suspended)
    failures = [row for _, row in captured.evidence["buffered"] if "FailedEvent" in row["type"]]
    assert failures[0]["task_scheduled_id"] == 3
    assert failures[0]["failure_details"] is None
    assert "source_result" not in captured.prefix["pending_batch"][2]
    assert not consume(captured, trusted, suspended).actions
    resumed = rows(assets.child, "failure-parent-history.json")
    captured = compile_case(assets, trusted, resumed)
    assert captured.evidence["failure_order"][0]["event"]["task_scheduled_id"] == 3
    native = consume(captured, trusted, resumed)
    assert native.actions == []
    settled = deepcopy(resumed)
    settled.append({"type": "OrchestratorStartedEvent", "event_id": -1, "timestamp": settled[-1]["timestamp"]})
    settled.append({
        "type": "SubOrchestrationInstanceFailedEvent",
        "event_id": -1,
        "timestamp": settled[-1]["timestamp"],
        "task_scheduled_id": 2,
        "failure_details": None,
    })
    native = consume(captured, trusted, settled)
    assert len(native.actions) == 1
    assert native.actions[0].completeOrchestration.orchestrationStatus == 3
    assert "task #3 failed" in native.actions[0].completeOrchestration.failureDetails.errorMessage


def test_partial_rejected_buffered_and_duplicate_replies_remain_distinct(assets):
    trusted = definition("suspended_prefix_fixture")
    history = rows(assets.clean)
    first, second = compile_case(assets, trusted, history).state["waiting"]
    final = rows(assets.clean, "final-history.json")
    event = deepcopy(next(row for row in final if row["type"] == "EventRaisedEvent"))
    resume = deepcopy(next(row for row in final if row["type"] == "ExecutionResumedEvent"))
    event.update(name=first, input="11")
    buffered = compile_case(assets, trusted, [*history, event])
    assert buffered.evidence["consumed_external_events"] == []
    duplicate = dict(event, input="99")
    modeled = [*history, event, duplicate, resume]
    captured = compile_case(assets, trusted, modeled)
    assert captured.state["waiting"] == [second]
    assert captured.state["admittedReplyKeys"] == [first]
    assert len(captured.evidence["consumed_external_events"]) == 1
    assert captured.evidence["unconsumed_external_events"][first][0][1]["input"] == "99"
    assert not consume(captured, trusted, modeled).actions
    rejected = dict(event, input=json.dumps({"__pickled__": "not-deserialized"}))
    captured = compile_case(assets, trusted, [*history, rejected, resume])
    assert first in captured.state["waiting"]
    assert captured.state["admittedReplyKeys"] == []
    assert len(captured.evidence["consumed_external_events"]) == 1
    assert not consume(captured, trusted, [*history, rejected, resume]).actions


@pytest.mark.parametrize("status,boundary", [(1, "completed"), (3, "failed"), (5, "terminated")])
def test_terminal_is_disposition_not_a_restart_state(assets, status, boundary):
    history = rows(assets.clean)
    terminal = {
        "type": "ExecutionCompletedEvent",
        "event_id": 1,
        "timestamp": history[-1]["timestamp"],
        "orchestration_status": status,
        "result": None,
        "failure_details": {"error_type": "OriginalError", "message": "unchanged", "stack_trace": "native"},
    }
    captured = compile_case(assets, definition("suspended_prefix_fixture"), [*history, terminal])
    assert captured.boundary == boundary
    assert captured.state is None and captured.prefix is None
    assert captured.evidence["terminal"] == terminal


@pytest.mark.parametrize("status", [2, 4])
def test_can_and_canceled_have_no_published_producer(assets, status):
    history = rows(assets.clean)
    terminal = {"type": "ExecutionCompletedEvent", "event_id": 1, "orchestration_status": status}
    with pytest.raises(UnsupportedContract, match="unsupported-terminal-status"):
        compile_case(assets, definition("suspended_prefix_fixture"), [*history, terminal])


def test_fail_closed_for_undeclared_source_future_work_and_lost_arguments(assets):
    trusted = definition("suspended_prefix_fixture")
    history = rows(assets.clean)
    with pytest.raises(UnsupportedContract, match="original-definition-required"):
        compile_case(assets, TrustedDefinition(trusted.workflow, ()), history)
    with pytest.raises(UnsupportedContract, match="unrepresented-native-event"):
        compile_case(assets, trusted, [*history, {"type": "TimerCreatedEvent", "event_id": 1}])
    changed = deepcopy(history)
    next(row for row in changed if row["type"] == "TaskScheduledEvent")["input"] = None
    with pytest.raises(UnsupportedContract, match="missing-original-activity-argument"):
        compile_case(assets, trusted, changed)
    history = rows(assets.child, "source-parent-history.json")
    cut = next(
        i
        for i, row in enumerate(history)
        if row["type"] == "SubOrchestrationInstanceCreatedEvent" and row["event_id"] == 2
    )
    mixed = compile_case(assets, definition("suspended_prefix_child_fixture"), history[:cut])
    assert mixed.boundary == "native_mixed_batch"
    with pytest.raises(UnsupportedContract, match="exact-release-modules-required"):
        PublishedSource.load({})
    with pytest.raises(UnsupportedContract, match="published-source-changed"):
        PublishedSource.load({name: Path(__file__) for name in PINS})
    with pytest.raises(UnsupportedContract, match="trusted-module-changed"):
        TrustedDefinition(
            trusted.workflow,
            (TrustedModule(trusted.modules[0].module, trusted.modules[0].path, "0" * 64),),
        ).verify()


def test_recorded_mixed_activity_entity_retains_completed_sibling_and_pending_entity(assets):
    folder = assets.root / "checkpoint-release-candidate" / "partial-batch-live-30bab6349021"
    identity = json.loads((folder / "identity.json").read_text())
    manifest = json.loads((assets.root / "registration-candidate" / "partial_batch.registration.json").read_text())
    module_files = {
        name: (
            assets.root / "registration-candidate" / manifest["sources"][name]["path"],
            manifest["sources"][name]["sha256"],
        )
        for name in ("partial_batch_provider", "partial_batch_customer")
    }
    with patch.dict(os.environ, PARTIAL_BATCH_WORKFLOW=identity["workflow"]):
        trusted = load_definition(manifest["entrypoint"], module_files)
        history = rows(folder)
        captured = compile_case(assets, trusted, history)
    assert captured.state["outputs"] == ["begin:one"]
    assert [item["kind"] for item in captured.prefix["pending_batch"]] == ["activity", "entity"]
    assert "source_result" in captured.prefix["pending_batch"][0]
    assert "source_result" not in captured.prefix["pending_batch"][1]
    assert not consume(captured, trusted, history).actions


def _modeled_mixed_activity_agent(assets, profile, completed):
    folder = assets.root / "checkpoint-release-candidate" / "partial-batch-live-30bab6349021"
    identity = json.loads((folder / "identity.json").read_text())
    manifest = json.loads((assets.root / "registration-candidate" / "partial_batch.registration.json").read_text())
    module_files = {
        name: (
            assets.root / "registration-candidate" / manifest["sources"][name]["path"],
            manifest["sources"][name]["sha256"],
        )
        for name in ("partial_batch_provider", "partial_batch_customer")
    }
    with patch.dict(os.environ, PARTIAL_BATCH_WORKFLOW=identity["workflow"]):
        trusted = load_definition(manifest["entrypoint"], module_files)
    original = rows(folder)
    cut = next(i for i, row in enumerate(original) if row["type"] == "EntityOperationCalledEvent")
    history = deepcopy(original[:cut])
    # Model two source messages. Exact source preparation creates two activity
    # calls then the first agent, with its second message deferred sequentially.
    begin = next(row for row in history if row["type"] == "TaskCompletedEvent" and row["task_scheduled_id"] == 0)
    value = json.loads(json.loads(begin["result"]))
    value["sent_messages"].append(deepcopy(value["sent_messages"][0]))
    begin["result"] = json.dumps(json.dumps(value))
    if completed:
        history.append(
            deepcopy(
                next(row for row in original if row["type"] == "TaskCompletedEvent" and row["task_scheduled_id"] == 1)
            )
        )
    if profile == "native-sdk-v1":
        for row in history:
            if row["type"] == "ExecutionStartedEvent":
                row["input"] = json.loads(row["input"])
            elif row["type"] == "TaskScheduledEvent":
                row["input"] = json.dumps(json.loads(json.loads(row["input"])[0]))
                row["event_id"] += 1
            elif row["type"] == "TaskCompletedEvent":
                row["task_scheduled_id"] += 1
    return trusted, history


@pytest.mark.parametrize("profile", ["classic-functions-v1", "native-sdk-v1"])
@pytest.mark.parametrize("completed", [False, True])
def test_modeled_future_activity_and_agent_allocate_only_actual_runtime_ids(assets, profile, completed):
    trusted, history = _modeled_mixed_activity_agent(assets, profile, completed)
    captured = compile_case(assets, trusted, history, profile=profile)
    assert captured.boundary == "native_mixed_batch"
    future = captured.prefix["future_batch"]
    assert [item["kind"] for item in future] == ["activity", "entity"]
    for item in future:
        assert not ({"source_input", "source_result", "native_request_id", "correlation_id"} & set(item))
    native = consume(captured, trusted, history, profile=profile)
    origin = 0 if profile == "classic-functions-v1" else 1
    assert [a.id for a in native.actions] == [origin + 2, origin + 3]
    activity, entity = native.actions
    assert activity.HasField("scheduleTask")
    assert entity.HasField("sendEntityMessage")
    activity_input = json.loads(activity.scheduleTask.input.value)
    if profile == "classic-functions-v1":
        activity_input = json.loads(activity_input[0])
    assert activity_input == future[0]["python_input"]
    call = entity.sendEntityMessage.entityOperationCalled
    request = json.loads(call.input.value)
    assert request["message"] == "one"
    assert request["orchestrationId"] == captured.evidence["instance"]
    assert request["correlationId"] and call.requestId
    assert request["correlationId"] != call.requestId
    accepted = [
        *history,
        {
            "type": "TaskScheduledEvent",
            "event_id": activity.id,
            "timestamp": history[-1]["timestamp"],
            "name": activity.scheduleTask.name,
            "input": activity.scheduleTask.input.value,
        },
        {
            "type": "EntityOperationCalledEvent",
            "event_id": entity.id,
            "timestamp": history[-1]["timestamp"],
            "target_instance_id": call.targetInstanceId.value,
            "request_id": call.requestId,
            "operation": call.operation,
            "input": call.input.value,
        },
    ]
    # Replay of now-accepted future calls retains handles and their UUID order.
    replayed = consume(captured, trusted, accepted, profile=profile)
    assert not replayed.actions, [a.completeOrchestration.failureDetails.errorMessage for a in replayed.actions]


def test_mixed_frontier_rejects_known_accepted_effect_mismatch_without_rescheduling(assets):
    trusted, history = _modeled_mixed_activity_agent(assets, "classic-functions-v1", False)
    captured = compile_case(assets, trusted, history)
    native = consume(captured, trusted, history)
    activity = native.actions[0]
    changed = [
        *history,
        {
            "type": "TaskScheduledEvent",
            "event_id": activity.id,
            "timestamp": history[-1]["timestamp"],
            "name": activity.scheduleTask.name,
            "input": json.dumps([json.dumps("already accepted different effect")]),
        },
    ]
    rejected = consume(captured, trusted, changed)
    assert len(rejected.actions) == 1
    terminal = rejected.actions[0].completeOrchestration
    assert terminal.orchestrationStatus == 3
    assert "Refusing rescheduling" in terminal.failureDetails.errorMessage
    identity = {"instance_id": captured.evidence["instance"], "execution_id": captured.evidence["execution"]}
    with pytest.raises(ValueError, match="get_acquired_history"):
        replay_capture_frontier(
            SimpleNamespace(**identity),
            runtime_prefix(captured.prefix),
            captured.state,
            **identity,
        )
    forged_prefix, forged_state = deepcopy(captured.prefix), deepcopy(captured.state)
    forged_prefix["future_batch"][-1]["native_request_id"] = "00000000-0000-0000-0000-000000000001"
    forged_state["nativeBatch"]["operations"] = forged_prefix["pending_batch"] + forged_prefix["future_batch"]
    with pytest.raises(ValueError, match="cannot contain historical"):
        replay_capture_frontier(SimpleNamespace(**identity), runtime_prefix(forged_prefix), forged_state, **identity)


def test_compiler_rejects_missing_operation_amidst_later_accepted_batch(assets):
    original = rows(assets.child, "source-parent-history.json")
    changed = [
        row for row in original if not (row["type"] == "SubOrchestrationInstanceCreatedEvent" and row["event_id"] == 2)
    ]
    with pytest.raises(UnsupportedContract, match="noncontiguous-original-actions"):
        compile_case(assets, definition("suspended_prefix_child_fixture"), changed)


def test_modeled_future_agent_preserves_allocator_position_after_accepted_agent(assets):
    original, _ = _modeled_mixed_activity_agent(assets, "classic-functions-v1", False)
    assistant = original.workflow.executors["assistant"]
    second = AgentExecutor(assistant.agent, id="second")
    graph = (
        WorkflowBuilder(name=original.workflow.name, start_executor=original.workflow.executors["begin"])
        .add_fan_out_edges(original.workflow.executors["begin"], [assistant, second])
        .build()
    )
    trusted = TrustedDefinition(graph, original.modules)
    folder = assets.root / "checkpoint-release-candidate" / "partial-batch-live-30bab6349021"
    recorded = rows(folder)
    accepted = deepcopy(next(row for row in recorded if row["type"] == "EntityOperationCalledEvent"))
    accepted["event_id"] = 1
    cut = next(i for i, row in enumerate(recorded) if row["type"] == "TaskScheduledEvent" and row["event_id"] == 1)
    modeled = [*deepcopy(recorded[:cut]), accepted]
    captured = compile_case(assets, trusted, modeled)
    assert captured.boundary == "native_mixed_batch"
    assert captured.prefix["pending_batch"][0]["native_request_id"] == accepted["request_id"]
    future = captured.prefix["future_batch"][0]
    assert future["task_id"] == 2 and future["entity"].startswith("@dafx-" + graph.name + "-second@")
    native = consume(captured, trusted, modeled)
    assert len(native.actions) == 1
    action = native.actions[0]
    assert action.id == 2 and action.HasField("sendEntityMessage")
    call = action.sendEntityMessage.entityOperationCalled
    old_request = json.loads(accepted["input"])
    if isinstance(old_request, str):
        old_request = json.loads(old_request)
    future_request = json.loads(call.input.value)
    assert future_request["correlationId"] != old_request["correlationId"]
    assert call.requestId != accepted["request_id"]
    modeled.append({
        "type": "EntityOperationCalledEvent",
        "event_id": action.id,
        "timestamp": accepted["timestamp"],
        "request_id": call.requestId,
        "target_instance_id": call.targetInstanceId.value,
        "operation": call.operation,
        "input": call.input.value,
    })
    assert not consume(captured, trusted, modeled).actions


def test_mixed_batch_first_failure_is_actual_arrival_not_task_list_order(assets):
    trusted = definition("suspended_prefix_child_fixture")
    recorded = rows(assets.child, "source-parent-history.json")
    cut = next(
        i
        for i, row in enumerate(recorded)
        if row["type"] == "SubOrchestrationInstanceCreatedEvent" and row["event_id"] == 2
    )
    modeled = deepcopy(recorded[:cut])
    captured = compile_case(assets, trusted, modeled)
    native = consume(captured, trusted, modeled)
    for action in native.actions:
        call = action.createSubOrchestration
        modeled.append({
            "type": "SubOrchestrationInstanceCreatedEvent",
            "event_id": action.id,
            "timestamp": modeled[-1]["timestamp"],
            "name": call.name,
            "instance_id": call.instanceId,
            "input": call.input.value,
        })
    for task_id in (3, 1):
        modeled.append({
            "type": "SubOrchestrationInstanceFailedEvent",
            "event_id": -1,
            "timestamp": modeled[-1]["timestamp"],
            "task_scheduled_id": task_id,
            "failure_details": None,
        })
    result = consume(captured, trusted, modeled)
    assert result.actions == []
    settled = deepcopy(modeled)
    settled.append({"type": "OrchestratorStartedEvent", "event_id": -1, "timestamp": settled[-1]["timestamp"]})
    settled.append({
        "type": "SubOrchestrationInstanceFailedEvent",
        "event_id": -1,
        "timestamp": settled[-1]["timestamp"],
        "task_scheduled_id": 2,
        "failure_details": None,
    })
    result = consume(captured, trusted, settled)
    assert len(result.actions) == 1
    terminal = result.actions[0].completeOrchestration
    assert terminal.orchestrationStatus == 3
    assert "task #3 failed" in terminal.failureDetails.errorMessage


@pytest.mark.parametrize("completed", [False, True])
def test_modeled_partial_child_batch_creates_only_future_suffix_and_rejoins(assets, completed):
    trusted = definition("suspended_prefix_child_fixture")
    original = rows(assets.child, "source-parent-history.json")
    cut = next(
        index
        for index, row in enumerate(original)
        if row["type"] == "SubOrchestrationInstanceCreatedEvent" and row["event_id"] == 2
    )
    modeled_prefix = deepcopy(original[:cut])
    assert [row["event_id"] for row in modeled_prefix if row["type"] == "SubOrchestrationInstanceCreatedEvent"] == [1]
    if completed:
        modeled_prefix.append(
            deepcopy(
                next(
                    row
                    for row in original
                    if row["type"] == "SubOrchestrationInstanceCompletedEvent" and row["task_scheduled_id"] == 1
                )
            )
        )
    captured = compile_case(assets, trusted, modeled_prefix)
    assert captured.boundary == "native_mixed_batch"
    assert captured.prefix["contract"] == "mechanical-native-prefix/5"
    assert [item["task_id"] for item in captured.prefix["pending_batch"]] == [1]
    assert ("source_result" in captured.prefix["pending_batch"][0]) == completed
    assert [item["task_id"] for item in captured.prefix["future_batch"]] == [2, 3]
    native = consume(captured, trusted, modeled_prefix)
    calls = [a for a in native.actions if a.HasField("createSubOrchestration")]
    assert [a.id for a in calls] == [2, 3]
    assert [a.createSubOrchestration.instanceId for a in calls] == [
        item["instance"] for item in captured.prefix["future_batch"]
    ]
    from agent_framework_durabletask._workflows.protocol import wrap_workflow_input

    assert [
        json.loads(call.createSubOrchestration.input.value)
        for call in calls
    ] == [wrap_workflow_input(item["python_input"]) for item in captured.prefix["future_batch"]]
    # Acquisition can also reveal the suffix already accepted by the original
    # classic producer. Its actual converter arguments must remain untouched.
    assert not consume(captured, trusted, original).actions
    # Model the actual future native producer actions as subsequently accepted.
    resumed = deepcopy(modeled_prefix)
    for action in calls:
        resumed.append({
            "type": "SubOrchestrationInstanceCreatedEvent",
            "event_id": action.id,
            "timestamp": modeled_prefix[-1]["timestamp"],
            "name": action.createSubOrchestration.name,
            "instance_id": action.createSubOrchestration.instanceId,
            "input": action.createSubOrchestration.input.value,
        })
    assert not consume(captured, trusted, resumed).actions
    if not completed:
        resumed.append(
            deepcopy(
                next(
                    row
                    for row in original
                    if row["type"] == "SubOrchestrationInstanceCompletedEvent" and row["task_scheduled_id"] == 1
                )
            )
        )
    original_outcome = next(
        row
        for row in resumed
        if row["type"] == "SubOrchestrationInstanceCompletedEvent" and row["task_scheduled_id"] == 1
    )
    resumed.extend(dict(deepcopy(original_outcome), task_scheduled_id=task_id) for task_id in (2, 3))
    finished = consume(captured, trusted, resumed)
    assert [a.id for a in finished.actions if a.HasField("scheduleTask")] == [4, 5, 6]
    # Genuine complete recording is the compatibility control. Truncation does
    # not establish an authoritative stable backend state in the negative case.
    complete = compile_case(assets, trusted, original)
    assert complete.boundary == "native_batch"
    assert [item["task_id"] for item in complete.prefix["pending_batch"]] == [1, 2, 3]
    assert original == rows(assets.child, "source-parent-history.json")


def test_original_agent_between_steps_and_unaccepted_sequential_tail(assets):
    with patch.dict(os.environ, SUSPENDED_PREFIX_CASE=str(assets.agent), SUSPENDED_PREFIX_MODE="target"):
        trusted = definition("suspended_prefix_agent_fixture")
        history = rows(assets.agent)
        cut = next(i for i, row in enumerate(history) if row["type"] == "TaskScheduledEvent" and row["event_id"] == 2)
        history = history[:cut]
        captured = compile_case(assets, trusted, history)
        assert captured.boundary == "prepare"
        assert captured.state["outputs"] == ["prior"]
        native = consume(captured, trusted, history)
        assert [a.id for a in native.actions if a.HasField("scheduleTask")] == [2], native.actions
        # Independent model of two sequential source messages, one already done.
        modeled = deepcopy(history)
        begin = next(row for row in modeled if row["type"] == "TaskCompletedEvent" and row["task_scheduled_id"] == 0)
        value = json.loads(json.loads(begin["result"]))
        value["sent_messages"].append(deepcopy(value["sent_messages"][0]))
        begin["result"] = json.dumps(json.dumps(value))
        captured = compile_case(assets, trusted, modeled)
        assert captured.boundary == "sequential_prepare"
        assert len(captured.state["sequentialPrepare"]["staged"]) == 1
        assert len(captured.state["sequentialPrepare"]["remaining"]) == 1
        malformed = deepcopy(captured.state)
        malformed["routingPolicy"] = "unknown"
        # No context method can be called before this semantic contract rejection.
        with pytest.raises(ValueError, match="contract or routing policy"):
            next(continue_checkpoint(SimpleNamespace(), trusted.workflow, malformed))
        native = consume(captured, trusted, modeled)
        calls = [a for a in native.actions if a.HasField("sendEntityMessage")]
        assert [a.id for a in calls] == [2]
        call = calls[0].sendEntityMessage.entityOperationCalled
        template = next(row for row in modeled if row["type"] == "EntityOperationCalledEvent")
        accepted = dict(
            template,
            event_id=2,
            request_id=call.requestId,
            target_instance_id=call.targetInstanceId.value,
            operation=call.operation,
            input=call.input.value,
        )
        completion = dict(
            next(row for row in modeled if row["type"] == "EntityOperationCompletedEvent"),
            request_id=call.requestId,
        )
        resumed = consume(captured, trusted, [*modeled, accepted, completion])
        assert [a.id for a in resumed.actions if a.HasField("scheduleTask")] == [3, 4]
        assert not any(a.HasField("sendEntityMessage") for a in resumed.actions)


def produce_native(assets, kind):
    """Exact source preparation helpers plus real public SDK direct transport."""
    original = definition("suspended_prefix_fixture")
    if kind == "child":
        original = definition("suspended_prefix_child_fixture", "child_workflow")
        node = WorkflowExecutor(workflow=original.workflow, id="child")
        graph = WorkflowBuilder(name="native-child-first", start_executor=node).build()
        executor, message = "child", "earlier"
    elif kind == "entity":
        with patch.dict(os.environ, SUSPENDED_PREFIX_CASE=str(assets.agent), SUSPENDED_PREFIX_MODE="target"):
            original = definition("suspended_prefix_agent_fixture")
        graph = WorkflowBuilder(
            name="native-agent-first",
            start_executor=original.workflow.executors["assistant"],
        ).build()
        executor, message = "assistant", "one"
    else:
        graph, executor, message = original.workflow, "begin", "input"
    trusted = TrustedDefinition(graph, original.modules)
    instance, execution = "native-capture-" + kind, "native-execution"
    modules = {}
    if kind == "entity":
        # These exact explicitly selected original models are test producer inputs,
        # not additional compiler inputs or cold dependencies.
        package_name = "_capture_original_request_model"
        package = ModuleType(package_name)
        package.__path__ = []
        sys.modules[package_name] = package
        original_root = assets.root / "release" / "python" / "packages" / "durabletask" / "agent_framework_durabletask"
        for name in ("_constants", "_models"):
            path = original_root / f"{name}.py"
            if not path.is_file():
                pytest.fail(f"Exact original test producer model missing: {path}")
            spec = importlib.util.spec_from_file_location(f"{package_name}.{name}", path)
            if spec is None or spec.loader is None:
                pytest.fail(f"Unable to load exact original test producer model: {path}")
            module = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = module
            spec.loader.exec_module(module)
            modules[name] = module

    def root(context, ignored):
        def prepare_agent(scoped, value, orchestration):
            request = modules["_models"].RunRequest(message=value, correlation_id=context.new_uuid())
            request.orchestration_id = orchestration
            return context.call_entity(
                EntityInstanceId(entity="dafx-" + scoped, key=orchestration),
                "run",
                input=request.to_dict(),
            )

        adapter = SimpleNamespace(
            instance_id=context.instance_id,
            prepare_agent_task=prepare_agent,
            prepare_activity_task=lambda name, argument: context.call_activity(name, input=argument),
            call_sub_orchestrator=lambda name, input, instance_id=None: context.call_sub_orchestrator(
                name, input=input, instance_id=instance_id
            ),
        )
        tasks, _, _ = assets.source.modules["orchestrator"]._prepare_all_tasks(
            adapter,
            graph,
            {executor: [(message, "__workflow_start__")]},
            {},
            [0],
            {"root_instance_id": instance, "root_workflow_name": graph.name, "request_path_prefix": ""},
        )
        return (yield task.when_all(tasks))  # noqa: B901

    root.__name__ = "dafx-" + graph.name
    registry = worker._Registry()
    registry.add_orchestrator(root)
    start = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(root.__name__, instance, encoded_input=json.dumps(message)),
    ]
    native_executor = worker._OrchestrationExecutor(
        registry,
        logging.getLogger("capture-native-producer"),
        DEFAULT_DATA_CONVERTER,
    )
    produced = _execute_with_proposed_identity(native_executor, instance, start, execution_id=execution)
    action = produced.actions[0]
    template = rows(assets.clean)
    started = deepcopy(next(row for row in template if row["type"] == "ExecutionStartedEvent"))
    started.update(
        name=root.__name__,
        input=json.dumps(message),
        orchestration_instance={"instance_id": instance, "execution_id": execution},
    )
    clock = start[0].timestamp.ToDatetime().replace(tzinfo=timezone.utc).isoformat()
    first = dict(template[0], timestamp=clock)
    scheduled = {"event_id": action.id, "timestamp": started["timestamp"]}
    if kind == "activity":
        scheduled.update(
            type="TaskScheduledEvent", name=action.scheduleTask.name, input=action.scheduleTask.input.value
        )
    elif kind == "child":
        call = action.createSubOrchestration
        scheduled.update(
            type="SubOrchestrationInstanceCreatedEvent",
            name=call.name,
            instance_id=call.instanceId,
            input=call.input.value,
        )
    else:
        call = action.sendEntityMessage.entityOperationCalled
        scheduled.update(
            type="EntityOperationCalledEvent",
            request_id=call.requestId,
            operation=call.operation,
            target_instance_id=call.targetInstanceId.value,
            input=call.input.value,
        )
    return trusted, [first, started, scheduled]


@pytest.mark.parametrize("kind", ["activity", "child", "entity"])
def test_direct_native_producers_and_host_acquired_consumers(assets, kind):
    trusted, history = produce_native(assets, kind)
    captured = compile_case(assets, trusted, history, profile="native-sdk-v1", streaming=True)
    assert captured.boundary == "native_batch"
    assert captured.prefix["pending_batch"][0]["task_id"] == 1
    native = consume(captured, trusted, history, profile="native-sdk-v1")
    assert not native.actions
    with pytest.raises(UnsupportedContract, match="noncontiguous-original-actions"):
        compile_case(assets, trusted, history)


def test_native_history_boundary_fails_closed_without_host_getter(assets):
    trusted, history = produce_native(assets, "activity")
    captured = compile_case(assets, trusted, history, profile="native-sdk-v1")
    identity = history[1]["orchestration_instance"]
    context = SimpleNamespace(instance_id=identity["instance_id"], execution_id=identity["execution_id"])
    with pytest.raises(ValueError, match="get_acquired_history"):
        replay_capture_frontier(context, runtime_prefix(captured.prefix), captured.state, **identity)
    # A signed history keyword is not an alternate authority.
    with pytest.raises(TypeError, match="acquired_history"):
        cast(Callable[..., Any], replay_capture_frontier)(
            context,
            runtime_prefix(captured.prefix),
            captured.state,
            **identity,
            acquired_history=history,
        )


def test_native_signed_input_cannot_replace_actual_host_acquisition(assets):
    trusted, history = produce_native(assets, "activity")
    captured = compile_case(assets, trusted, history, profile="native-sdk-v1")
    changed_prefix = deepcopy(captured.prefix)
    operation = changed_prefix["pending_batch"][0]
    operation["python_input"] = "a different authorized-looking argument"
    operation["source_input"] = json.dumps(operation["python_input"])
    changed_state = deepcopy(captured.state)
    changed_state["nativeBatch"]["operations"] = changed_prefix["pending_batch"]
    changed = type(captured)(
        captured.boundary,
        captured.control,
        changed_state,
        changed_prefix,
        captured.evidence,
        captured.lowered_source,
    )
    native = consume(changed, trusted, history, profile="native-sdk-v1")
    assert len(native.actions) == 1
    terminal = native.actions[0].completeOrchestration
    assert terminal.orchestrationStatus == 3
    assert "identity or input changed in acquired history" in terminal.failureDetails.errorMessage


def test_native_pre_dispatch_authorizes_only_future_activity(assets):
    trusted, history = produce_native(assets, "activity")
    captured = compile_case(assets, trusted, history[:-1], profile="native-sdk-v1")
    assert captured.boundary == "prepare"
    assert captured.prefix["operation_batches"] == []
    native = consume(captured, trusted, history[:-1], profile="native-sdk-v1")
    calls = [action for action in native.actions if action.HasField("scheduleTask")]
    assert len(calls) == 1
    assert calls[0].id == 1


def test_native_repeated_suspend_keeps_default_recursive_producer_semantics(assets):
    trusted, history = produce_native(assets, "activity")
    completed = deepcopy(next(row for row in rows(assets.clean) if row["type"] == "TaskCompletedEvent"))
    completed["task_scheduled_id"] = 1
    history.append(completed)
    captured = compile_case(assets, trusted, history, profile="native-sdk-v1")
    first, second = captured.state["waiting"]
    timestamp = history[-1]["timestamp"]
    suspend = {"type": "ExecutionSuspendedEvent", "event_id": -1, "timestamp": timestamp, "input": "pause"}
    resume = {"type": "ExecutionResumedEvent", "event_id": -1, "timestamp": timestamp, "input": "resume"}
    event = {"type": "EventRaisedEvent", "event_id": -1, "timestamp": timestamp, "name": first, "input": "11"}
    modeled = [*history, suspend, event, deepcopy(suspend), resume]
    captured = compile_case(assets, trusted, modeled, profile="native-sdk-v1")
    assert captured.control == "suspended"
    assert captured.state["waiting"] == [second]
    assert captured.state["admittedReplyKeys"] == [first]
    assert not consume(captured, trusted, modeled, profile="native-sdk-v1").actions


def test_modeled_native_completed_activity_and_original_wait(assets):
    trusted, history = produce_native(assets, "activity")
    completed = deepcopy(next(row for row in rows(assets.clean) if row["type"] == "TaskCompletedEvent"))
    completed["task_scheduled_id"] = 1
    modeled = [*history, completed]
    captured = compile_case(assets, trusted, modeled, profile="native-sdk-v1", streaming=True)
    assert captured.boundary == "hitl"
    assert len(captured.state["waiting"]) == 2
    assert captured.state["eventStreaming"]
    assert not consume(captured, trusted, modeled, profile="native-sdk-v1").actions


def test_corrected_classic_repeated_suspend_commits_original_source_return(assets):
    trusted = definition("suspended_prefix_fixture")
    history = rows(assets.clean, "final-history.json")
    cut = next(i for i, row in enumerate(history) if row["type"] == "ExecutionCompletedEvent")
    modeled = _modeled_tail(history[:cut])
    captured = compile_case(assets, trusted, modeled)
    assert captured.boundary == "awaiting_terminal_commitment"
    assert captured.control == "running"
    native = consume(captured, trusted, modeled)
    # The historical SDK counterexample remains in the immutable artifacts.
    # The corrected classic profile must now commit the original source return.
    completed = [a.completeOrchestration for a in native.actions if a.HasField("completeOrchestration")]
    assert len(completed) == 1
    assert completed[0].orchestrationStatus == 1
    assert json.loads(completed[0].result.value) == ["prior", "first:11", "second:22"]


def test_source_convergence_failure_before_native_commit_is_not_a_success(assets):
    original = definition("suspended_prefix_fixture")
    graph = WorkflowBuilder(
        name=original.workflow.name,
        start_executor=original.workflow.executors["begin"],
        max_iterations=1,
    ).build()
    trusted = TrustedDefinition(graph, original.modules)
    final = rows(assets.clean, "final-history.json")
    cut = next(i for i, row in enumerate(final) if row["type"] == "TaskScheduledEvent" and row["event_id"] == 1)
    # Same source definition with an explicit modeled iteration limit. The
    # original accepted activity and real replies remain unchanged.
    history = final[:cut]
    captured = compile_case(assets, trusted, history)
    assert captured.boundary == "awaiting_failure_commitment"
    assert captured.evidence["terminal"] is None
    assert captured.evidence["source_failure"]["message"] == "Workflow did not converge after 1 iterations."
    assert captured.state["outputs"] == ["prior"]
    native = consume(captured, trusted, history)
    assert len(native.actions) == 1
    terminal = native.actions[0].completeOrchestration
    assert terminal.orchestrationStatus == 3
    assert terminal.failureDetails.errorType.endswith("WorkflowConvergenceException")
    assert terminal.failureDetails.errorMessage == captured.evidence["source_failure"]["message"]


def _native_child_history(assets):
    """Explicit native-transport model of recorded original source child calls."""
    history = deepcopy(rows(assets.child, "source-parent-history.json"))
    history = [row for row in history if row["type"] not in ("ExecutionSuspendedEvent", "ExecutionResumedEvent")]
    for row in history:
        if row["type"] == "ExecutionStartedEvent":
            row["input"] = json.loads(row["input"])
        elif row["type"] == "TaskScheduledEvent":
            row["input"] = json.dumps(json.loads(json.loads(row["input"])[0]))
            row["event_id"] += 1
        elif row["type"] == "SubOrchestrationInstanceCreatedEvent":
            row["input"] = json.loads(row["input"])
            row["event_id"] += 1
        elif row["type"] in ("TaskCompletedEvent", "SubOrchestrationInstanceCompletedEvent"):
            row["task_scheduled_id"] += 1
    failure = deepcopy(
        next(
            row
            for row in rows(assets.child, "failure-parent-history.json")
            if row["type"] == "SubOrchestrationInstanceFailedEvent"
        )
    )
    failure["task_scheduled_id"] += 1
    history.append(failure)
    return history


@pytest.mark.parametrize("version", ["1.5.0", "1.7.2", "1.12.0"])
def test_actual_official_native_task_sources_failure_release_and_success_order(assets, version):
    path = assets.root / "capture-runtime-policy" / ("durabletask-" + version) / "durabletask" / "task.py"
    digest = {
        "1.5.0": "33de47ab40116078f74a63af487ee8b672887a83adc1010cf1cc37dc52a483b6",
        "1.7.2": "3d5b6c8fe4b83360450f174d2ca0ce0e07bb16785c1b02cb38acc51c31f81c42",
        "1.12.0": "af37e7842a515f52a05a27106b5ea864fb6affc5c451b6ebbe11dac1d13cd9ee",
    }[version]
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
    name = "_capture_native_tasks_" + version.replace(".", "_")
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        pytest.fail(f"Unable to load official task implementation: {path}")
    original = importlib.util.module_from_spec(spec)
    sys.modules[name] = original
    try:
        exec(compile(path.read_bytes(), str(path), "exec"), vars(original))
        left, right = original.CompletableTask(), original.CompletableTask()
        joined = original.when_all([left, right])
        right.fail("right failed first", ValueError("original task implementation"))
        assert joined.is_complete is (version != "1.12.0")
        assert joined.is_failed is (version != "1.12.0")
        assert not left.is_complete  # No policy cancels unfinished siblings.
        if version == "1.12.0":
            left.complete("left")
            assert joined.is_complete and joined.is_failed
            assert "right failed first" in str(joined.get_exception())
        first, second = original.CompletableTask(), original.CompletableTask()
        success = original.when_all([first, second])
        second.complete("second")
        assert not success.is_complete
        first.complete("first")
        assert success.get_result() == ["first", "second"]
    finally:
        sys.modules.pop(name, None)


@pytest.mark.parametrize(
    ("directory", "wheel", "digest", "module"),
    [
        (
            "source-runtime-wheels",
            "durabletask-1.5.0-py3-none-any.whl",
            "58371340fe37714bf662d9c9d05791e038203e6b2e7888e7a4b9927af0999711",
            "durabletask/task.py",
        ),
        (
            "source-runtime-wheels",
            "durabletask-1.7.2-py3-none-any.whl",
            "e4397649d9b63f75d5ddcfe65197aad646b2187ad7eff79cdbdaa67cb7f1782a",
            "durabletask/task.py",
        ),
        (
            "release-source-wheels",
            "durabletask-1.12.0-py3-none-any.whl",
            "994d7dc554cadf3be36d1996b0499c2a451abb896fac28aa3e159b182c81c22f",
            "durabletask/task.py",
        ),
        (
            "source-runtime-wheels",
            "azure_functions_durable-1.3.1-py3-none-any.whl",
            "44dab37949b5a3a9300ef914c605df8e355ad246cd4502f7f39663150564dcfa",
            "azure/durable_functions/models/Task.py",
        ),
    ],
)
def test_original_task_controls_use_exact_official_wheel_bytes(assets, directory, wheel, digest, module):
    wheel_path = assets.root / directory / wheel
    assert hashlib.sha256(wheel_path.read_bytes()).hexdigest() == digest
    extracted = assets.root / "capture-runtime-policy" / wheel.removesuffix("-py3-none-any.whl")
    with zipfile.ZipFile(wheel_path) as archive:
        assert archive.read(module) == extracted.joinpath(*module.split("/")).read_bytes()


@pytest.mark.parametrize("version", ["1.3.1", "1.6.0"])
def test_actual_official_classic_tasks_fail_fast_without_loading_functions_host(assets, version):
    root = assets.root / "capture-runtime-policy" / ("azure_functions_durable-" + version)
    digest = {
        "1.3.1": "7b9e3cff07c991863f502a49930a9554ceb9b723fa8dc868c7f60db1ff12b253",
        "1.6.0": "587a0b3bcac20272512df80bd20c945adea4a681602034fa351f0dc506886d1c",
    }[version]
    path = root / "azure" / "durable_functions" / "models" / "Task.py"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
    # Namespace package shells avoid unrelated host/client __init__ imports.
    # All task/action implementation bytes are the unmodified official wheel.
    script = """
import sys, types
from pathlib import Path
root = Path(sys.argv[1])
for name in ('azure.durable_functions','azure.durable_functions.models','azure.durable_functions.models.actions'):
    package=types.ModuleType(name)
    package.__path__=[str(root.joinpath(*name.split('.')))]
    sys.modules[name]=package
from azure.durable_functions.models.Task import AtomicTask, WhenAllTask, TaskState
from azure.durable_functions.models.actions.NoOpAction import NoOpAction
from azure.durable_functions.models.ReplaySchema import ReplaySchema
a,b=AtomicTask('a',NoOpAction()),AtomicTask('b',NoOpAction())
joined=WhenAllTask([a,b],ReplaySchema.V2)
b.set_value(is_error=True,value=ValueError('first arrival'))
assert joined.is_completed and joined.state is TaskState.FAILED
assert not a.is_completed
a,b=AtomicTask('a',NoOpAction()),AtomicTask('b',NoOpAction())
joined=WhenAllTask([a,b],ReplaySchema.V2)
b.set_value(is_error=False,value='second')
assert not joined.is_completed
a.set_value(is_error=False,value='first')
assert joined.result == ['first','second']
"""
    result = subprocess.run([sys.executable, "-c", script, str(root)], capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
