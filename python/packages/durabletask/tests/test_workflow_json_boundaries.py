# Copyright (c) Microsoft. All rights reserved.

"""Finite JSON at workflow transport boundaries, without inspecting opaque checkpoints."""

from __future__ import annotations

import json
import math
from copy import deepcopy
from dataclasses import dataclass
from datetime import date
from typing import Any
from unittest.mock import Mock, patch, sentinel

import pytest
from agent_framework import AgentExecutorResponse, AgentResponse, Executor, Message, Workflow, WorkflowContext, handler
from agent_framework._workflows import _checkpoint_encoding
from pydantic import BaseModel, PrivateAttr
from test_subworkflow_orchestration import _subworkflow_executor
from test_workflow_review_followup import _agent, _host

from agent_framework_durabletask._workflows import activity, orchestrator
from agent_framework_durabletask._workflows.activity import execute_workflow_activity
from agent_framework_durabletask._workflows.orchestrator import (
    SOURCE_ORCHESTRATOR,
    ExecutorResult,
    TaskMetadata,
    TaskType,
    _coerce_initial_input,
    _prepare_activity_task,
    _prepare_all_tasks,
    _prepare_subworkflow_task,
    _process_activity_result,
    _process_subworkflow_result,
    _WorkflowDeliveryLedger,
    run_workflow_orchestrator,
)
from agent_framework_durabletask._workflows.protocol import unwrap_workflow_input
from agent_framework_durabletask._workflows.serialization import (
    SUBWORKFLOW_INPUT_KEY,
    SUBWORKFLOW_RESULT_KEY,
    deserialize_value,
    serialize_value,
)

_NONFINITE = [
    pytest.param(float("nan"), id="nan"),
    pytest.param(float("inf"), id="positive-infinity"),
    pytest.param(float("-inf"), id="negative-infinity"),
]
_NUMBER_TOKENS = ["NaN", "Infinity", "-Infinity", "1e309"]
_NUMBER_SLOT = "replace-this-with-a-json-number"
_ADDRESS = {"root_instance_id": "wire-run", "root_workflow_name": "wire", "request_path_prefix": ""}


def _nested(value: Any) -> dict[str, Any]:
    return {"items": [{"value": value}]}


def _number_json(payload: dict[str, Any], token: str) -> str:
    """Keep overflow as numeric JSON source, not a Python infinity spelling."""
    raw = json.dumps(payload, allow_nan=False)
    quoted_slot = json.dumps(_NUMBER_SLOT)
    assert raw.count(quoted_slot) == 1
    return raw.replace(quoted_slot, token)


def _snapshot() -> dict[str, Any]:
    return {"keep": False, "replace": "old", "remove": 0}


def _input(message: Any = None) -> dict[str, Any]:
    return {
        "message": serialize_value({"value": message}),
        "shared_state_snapshot": _snapshot(),
        "source_executor_ids": [SOURCE_ORCHESTRATOR],
        "host_context": {"instance_id": "wire-run", "workflow_name": "wire"},
    }


def _result() -> dict[str, Any]:
    return {
        "shared_state_updates": {"replace": "new", "added": {"ok": True}},
        "shared_state_deletes": ["remove"],
        "outputs": ["valid-output"],
        "events": [{"type": "intermediate", "executor_id": "producer", "data": "valid-event"}],
        "sent_messages": [{"source_id": "producer", "target_id": "sink", "message": "valid-message"}],
        "pending_request_info_events": [],
    }


def _ledger() -> _WorkflowDeliveryLedger:
    return _WorkflowDeliveryLedger(
        instance_id="wire-run", sent={"prior": {("occurrence", "fingerprint")}}, handoffs={"prior": 2}, completions=1
    )


def _upstream() -> AgentExecutorResponse:
    latest = Message("assistant", ["forwarded"], message_id="application-id")
    return AgentExecutorResponse("source", AgentResponse(messages=[latest]), [latest])


class _Producer(Executor):
    """Use the real handler, state and event capture paths of an activity."""

    def __init__(self, output: Any = "done", update: Any = "updated") -> None:
        super().__init__(id="producer")
        self.output = output
        self.update = update
        self.seen: list[dict[str, Any]] = []
        self.snapshots: list[dict[str, Any]] = []
        self.finished = False

    @handler
    async def produce(self, message: dict[str, Any], ctx: WorkflowContext[Any, Any]) -> None:
        self.seen.append(message)
        self.snapshots.append(ctx.state.export_state())
        ctx.set_state("replace", self.update)
        ctx.state.delete("remove")
        await ctx.send_message("routed", target_id="sink")
        await ctx.yield_output(self.output)
        self.finished = True


@pytest.mark.parametrize("bad", _NONFINITE)
@pytest.mark.parametrize("agent_start", [False, True])
def test_raw_start_rejects_nonfinite_values_before_reconstruction_or_stringification(
    bad: float, agent_start: bool
) -> None:
    workflow = Mock(spec=Workflow)
    workflow.start_executor_id = "target"
    workflow.executors = {"target": _agent() if agent_start else _Producer()}
    with (
        patch.object(orchestrator, "reconstruct_to_type", side_effect=AssertionError("Must validate first")),
        pytest.raises(ValueError),
    ):
        _coerce_initial_input(workflow, _nested(bad))


@pytest.mark.parametrize("bad", _NONFINITE)
@pytest.mark.parametrize("location", ["message", "shared_state_snapshot"])
def test_activity_dispatch_rejects_before_host_calls_or_ledger_commit(bad: float, location: str) -> None:
    host, ledger = _host(), _ledger()
    before = deepcopy(ledger)
    source = _upstream()
    snapshot = _snapshot()
    message: Any = source
    if location == "message":
        message = _nested(bad)
    else:
        snapshot["nested"] = _nested(bad)
    snapshot_before = json.dumps(snapshot, sort_keys=True)

    with pytest.raises(ValueError):
        _prepare_activity_task(host, "producer", message, "source", snapshot, "wire", _ADDRESS, ledger)

    assert host.mock_calls == []
    assert ledger == before
    assert json.dumps(snapshot, sort_keys=True) == snapshot_before
    assert not hasattr(source.agent_response, "_durable_workflow_forwarding")


@pytest.mark.parametrize("token", _NUMBER_TOKENS)
@pytest.mark.parametrize("location", ["message", "shared_state_snapshot", "host_context"])
def test_activity_input_rejects_entire_parsed_tree_before_decode_or_handler(
    token: str, location: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = _input()
    payload[location]["nested"] = _nested(_NUMBER_SLOT)
    raw = _number_json(payload, token)
    producer = _Producer()
    decoder = Mock(wraps=activity.deserialize_value)
    monkeypatch.setattr(activity, "deserialize_value", decoder)

    with pytest.raises(ValueError):
        execute_workflow_activity(producer, raw)

    decoder.assert_not_called()
    assert producer.seen == producer.snapshots == []
    assert not producer.finished


@pytest.mark.parametrize("bad", _NONFINITE)
@pytest.mark.parametrize("location", ["output", "shared_state_update"])
def test_real_activity_rejects_nonfinite_result_after_handler_without_returning_json(bad: float, location: str) -> None:
    producer = _Producer(output=_nested(bad)) if location == "output" else _Producer(update=_nested(bad))
    returned: list[str] = []

    with pytest.raises(ValueError):
        returned.append(execute_workflow_activity(producer, json.dumps(_input(), allow_nan=False)))

    assert returned == []
    assert producer.seen == [{"value": None}]
    assert producer.snapshots == [_snapshot()]
    # The handler ran once and finished. Rejection does not undo its external effects.
    assert producer.finished


@pytest.mark.parametrize("token", _NUMBER_TOKENS)
@pytest.mark.parametrize("location", ["shared_state_updates", "outputs", "events", "sent_messages"])
def test_activity_result_rejects_all_mutations_when_any_encoded_field_is_nonfinite(token: str, location: str) -> None:
    payload = _result()
    bad = _nested(_NUMBER_SLOT)
    if location == "shared_state_updates":
        payload[location]["bad"] = bad
    elif location == "outputs":
        payload[location].append(bad)
    elif location == "events":
        payload[location].append({"type": "intermediate", "executor_id": "producer", "data": bad})
    else:
        payload[location].append({"source_id": "producer", "target_id": "sink", "message": bad})
    state, outputs = _snapshot(), ["existing-output"]
    before = deepcopy((state, outputs))

    with pytest.raises(ValueError):
        _process_activity_result(_number_json(payload, token), "producer", state, outputs)

    assert (state, outputs) == before


@pytest.mark.parametrize("token", _NUMBER_TOKENS)
def test_activity_result_rejects_nonfinite_output_even_without_shared_state(token: str) -> None:
    payload = _result()
    payload["outputs"].append(_nested(_NUMBER_SLOT))
    outputs: list[Any] = ["existing-output"]

    with pytest.raises(ValueError):
        _process_activity_result(_number_json(payload, token), "producer", None, outputs)

    assert outputs == ["existing-output"]


def test_finite_activity_result_applies_updates_deletes_and_outputs_together() -> None:
    payload = _result()
    state, outputs = _snapshot(), ["existing-output"]

    result = _process_activity_result(json.dumps(payload, allow_nan=False), "producer", state, outputs)

    assert state == {"keep": False, "replace": "new", "added": {"ok": True}}
    assert outputs == ["existing-output", "valid-output"]
    assert result.activity_result == payload


@pytest.mark.parametrize("bad", _NONFINITE)
def test_child_dispatch_rejects_nonfinite_encoded_input_before_host_or_ledger_commit(bad: float) -> None:
    host, ledger = _host(), _ledger()
    before = deepcopy(ledger)
    child = _subworkflow_executor("child", "inner")

    with pytest.raises(ValueError):
        _prepare_subworkflow_task(host, child, _nested(bad), "wire-run::child::0", _ADDRESS, ledger)

    assert host.mock_calls == []
    assert ledger == before


@pytest.mark.parametrize("bad", _NONFINITE)
@pytest.mark.parametrize("location", ["outputs", "events"])
@pytest.mark.parametrize("direct", [False, True], ids=["routed", "direct"])
def test_child_result_rejects_before_parent_outputs_or_forwarded_events(
    bad: float, location: str, direct: bool
) -> None:
    child = _subworkflow_executor("child", "inner", allow_direct_output=direct)
    payload: dict[str, Any] = {
        SUBWORKFLOW_RESULT_KEY: True,
        "outputs": ["valid-output"],
        "events": [{"type": "intermediate", "executor_id": "inner", "data": "valid-event"}],
    }
    if location == "outputs":
        payload["outputs"].append(_nested(bad))
    else:
        payload["events"].append({"type": "intermediate", "executor_id": "inner", "data": _nested(bad)})
    before = json.dumps(payload, sort_keys=True)
    outputs: list[Any] = ["existing-output"]
    returned: list[ExecutorResult] = []

    with pytest.raises(ValueError):
        returned.append(_process_subworkflow_result(payload, child, outputs))

    assert returned == []
    assert outputs == ["existing-output"]
    assert json.dumps(payload, sort_keys=True) == before


def _batch(order: tuple[str, ...]) -> tuple[Any, dict[str, list[tuple[Any, str]]]]:
    nodes = {"child": _subworkflow_executor("child", "inner"), "producer": _Producer(), "target": _agent()}
    workflow = Mock(spec=Workflow)
    workflow.name = "wire"
    workflow.executors = nodes
    pending = {name: [(_upstream() if name == "child" else "start", "source")] for name in order}
    return workflow, pending


@pytest.mark.parametrize("bad", _NONFINITE)
@pytest.mark.parametrize(
    "order",
    [
        pytest.param(("child", "producer"), id="child-first"),
        pytest.param(("producer", "child"), id="activity-first"),
        pytest.param(("child",), id="child-only"),
        pytest.param(("target",), id="agent-only"),
    ],
)
def test_batch_snapshot_preflight_precedes_every_task_kind(bad: float, order: tuple[str, ...]) -> None:
    host, ledger = _host(), _ledger()
    workflow, pending = _batch(order)
    snapshot = {**_snapshot(), "nested": _nested(bad)}
    snapshot_before = json.dumps(snapshot, sort_keys=True)
    ledger_before = deepcopy(ledger)
    counter = [7]

    with pytest.raises(ValueError):
        _prepare_all_tasks(host, workflow, pending, snapshot, counter, _ADDRESS, ledger)

    assert host.mock_calls == []
    assert ledger == ledger_before
    assert counter == [7]
    assert json.dumps(snapshot, sort_keys=True) == snapshot_before


def _publisher(host: Any, events: list[dict[str, Any]]) -> Any:
    """Isolate the nested publisher from producer and result-boundary validation."""
    workflow = Mock(spec=Workflow)
    workflow.name = "wire"
    workflow.start_executor_id = "target"
    workflow.executors = {"target": _agent()}
    workflow.edge_groups = []
    workflow.max_iterations = 1
    metadata = TaskMetadata("target", "start", SOURCE_ORCHESTRATOR, TaskType.AGENT)
    completed = ExecutorResult("target", None, {"events": events}, TaskType.AGENT)
    host.task_all.side_effect = lambda tasks: tasks
    with (
        patch.object(orchestrator, "_prepare_all_tasks", return_value=([sentinel.task], [metadata], [])),
        patch.object(orchestrator, "_process_agent_response", return_value=completed),
    ):
        generator = run_workflow_orchestrator(host, workflow, "start")
        try:
            assert next(generator) == [sentinel.task]
            with pytest.raises(StopIteration) as finished:
                generator.send([sentinel.result])
            return finished.value.value
        finally:
            generator.close()


@pytest.mark.parametrize("bad", _NONFINITE)
def test_live_publisher_rejects_encoded_event_payload_before_set_custom_status(bad: float) -> None:
    host = _host()
    host.is_replaying = False
    host.supports_event_streaming = True

    with pytest.raises(ValueError):
        _publisher(host, [{"type": "intermediate", "executor_id": "target", "data": _nested(bad)}])

    host.set_custom_status.assert_not_called()
    host.prepare_activity_task.assert_not_called()
    host.call_sub_orchestrator.assert_not_called()


@pytest.mark.parametrize("replay", [False, True], ids=["compact-host", "replay"])
def test_publisher_only_validates_payload_that_will_be_published(replay: bool) -> None:
    host = _host()
    host.is_replaying = replay
    host.supports_event_streaming = replay

    assert _publisher(host, [{"type": "intermediate", "data": _nested(float("nan"))}]) == []

    if replay:
        host.set_custom_status.assert_not_called()
    else:
        host.set_custom_status.assert_called_once_with({"state": "running"})


@dataclass
class _CheckpointData:
    label: str
    _private_nan: float


class _CheckpointModel(BaseModel):
    label: str
    _private_nan: float = PrivateAttr(default_factory=lambda: float("nan"))


class _CheckpointObject:
    def __init__(self) -> None:
        self.label = "opaque"
        self._private_nan = float("nan")


def _valid_value(kind: str) -> Any:
    if kind == "primitives":
        return {
            "items": [False, 0, "", None, [], {}, "世界\n\t\u0000", 2**200, 1.25, -0.0, 1.7976931348623157e308],
            "enabled": True,
            "numeric_strings": _NUMBER_TOKENS,
        }
    if kind == "checkpoints":
        return {
            "dataclass": _CheckpointData("opaque", float("nan")),
            "model": _CheckpointModel(label="opaque"),
            "object": _CheckpointObject(),
            "message": Message("assistant", ["世界"], message_id="application-id"),
            "native": [date(2026, 9, 17), b"\x00\xff", (False, 0, None), {1, 2}, complex(1, 2)],
        }
    if kind == "literals":
        return [
            {"__pickled__": "literal, not base64", "nested": {"__type__": "business"}},
            {"__type__": "business", "value": False},
            {"_durable_agent_response": 1, "response": {"type": "agent_response", "messages": [], "value": 0}},
        ]
    raise AssertionError(f"Unknown control category: {kind}")


def _assert_preserved(actual: Any, expected: Any) -> None:
    assert type(actual) is type(expected)
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for key, value in expected.items():
            _assert_preserved(actual[key], value)
    elif isinstance(expected, (list, tuple)):
        assert len(actual) == len(expected)
        for item, value in zip(actual, expected, strict=True):
            _assert_preserved(item, value)
    elif isinstance(expected, (_CheckpointData, _CheckpointModel, _CheckpointObject)):
        assert actual.label == expected.label
        assert math.isnan(actual._private_nan)
    elif isinstance(expected, Message):
        assert actual.to_dict() == expected.to_dict()
    else:
        assert actual == expected
        if isinstance(expected, float):
            assert math.copysign(1, actual) == math.copysign(1, expected)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(False, id="false"),
        pytest.param(0, id="zero"),
        pytest.param("", id="empty-string"),
        pytest.param(None, id="none"),
        pytest.param([], id="empty-list"),
        pytest.param({}, id="empty-dict"),
        *[pytest.param(_valid_value(kind), id=kind) for kind in ("primitives", "checkpoints", "literals")],
    ],
)
def test_valid_wire_values_survive_activity_child_batch_and_status_boundaries(value: Any) -> None:
    encoded = serialize_value(value)
    json.dumps(encoded, allow_nan=False)
    host, ledger = _host(), _ledger()
    snapshot = {**_snapshot(), "initial": encoded}
    child = _subworkflow_executor("child", "inner", allow_direct_output=True)
    workflow, pending = _batch(("child", "producer"))
    counter = [0]
    no_decode = Mock(side_effect=AssertionError("Transport validation must not unpickle checkpoint payloads"))

    with patch.object(_checkpoint_encoding, "_base64_to_unpickle", no_decode):
        input_json = _prepare_activity_task(
            host, "producer", {"value": value}, "source", snapshot, "wire", _ADDRESS, ledger
        )
        child_input = _prepare_subworkflow_task(host, child, value, "wire-run::child::0", _ADDRESS, ledger)
        tasks, metadata, remaining = _prepare_all_tasks(host, workflow, pending, snapshot, counter, _ADDRESS, ledger)

    assert len(tasks) == 2 and remaining == [] and counter == [1]
    assert [item.task_type for item in metadata] == [TaskType.SUBWORKFLOW, TaskType.ACTIVITY]
    assert json.loads(input_json)["shared_state_snapshot"] == snapshot
    assert json.loads(host.prepare_activity_task.call_args.args[1])["shared_state_snapshot"] == snapshot
    child_wire = json.loads(json.dumps(child_input, allow_nan=False))
    _assert_preserved(deserialize_value(unwrap_workflow_input(child_wire)[SUBWORKFLOW_INPUT_KEY]), value)

    producer = _Producer(output=value, update=value)
    result_json = execute_workflow_activity(producer, input_json)
    result_wire = json.loads(result_json)
    json.dumps(result_wire, allow_nan=False)
    assert producer.finished and len(producer.seen) == 1
    _assert_preserved(producer.seen[0]["value"], value)
    _assert_preserved(producer.snapshots[0]["initial"], value)

    state, outputs = deepcopy(snapshot), ["existing-output"]
    child_outputs: list[Any] = ["existing-child-output"]
    event = {"type": "intermediate", "executor_id": "inner", "data": encoded}
    child_result = {SUBWORKFLOW_RESULT_KEY: True, "outputs": result_wire["outputs"], "events": [event]}
    host.is_replaying = False
    host.supports_event_streaming = True
    with patch.object(_checkpoint_encoding, "_base64_to_unpickle", no_decode):
        result = _process_activity_result(result_json, "producer", state, outputs)
        child_processed = _process_subworkflow_result(child_result, child, child_outputs)
        assert _publisher(host, [event]) == []

    no_decode.assert_not_called()
    assert result.activity_result == result_wire
    assert result_wire["shared_state_deletes"] == ["remove"]
    assert set(state) == {"keep", "replace", "initial"} and state["keep"] is False
    _assert_preserved(deserialize_value(state["replace"]), value)
    _assert_preserved(deserialize_value(outputs), ["existing-output", value])
    _assert_preserved(deserialize_value(child_outputs), ["existing-child-output", value])
    assert child_processed.activity_result is not None
    assert child_processed.activity_result["events"] == [{**event, "executor_id": "child"}]
    host.set_custom_status.assert_called_once()
    status = host.set_custom_status.call_args.args[0]
    status = json.loads(json.dumps(status, allow_nan=False))
    emitted = [item for item in status["events"] if item["type"] == "intermediate"]
    assert len(emitted) == 1
    _assert_preserved(deserialize_value(emitted[0]["data"]), value)
