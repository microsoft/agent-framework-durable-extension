# Copyright (c) Microsoft. All rights reserved.

"""Dictionary-key regression tests for workflow mapping serialization paths."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pytest
from agent_framework import Executor, WorkflowContext, handler
from pydantic import BaseModel

from agent_framework_durabletask import execute_workflow_activity
from agent_framework_durabletask._workflows.orchestrator import SOURCE_ORCHESTRATOR
from agent_framework_durabletask._workflows.serialization import (
    deserialize_value,
    deserialize_workflow_event,
    serialize_value,
)


@dataclass
class PersistedCheckpoint:
    value: int


class PersistedModel(BaseModel):
    value: int


@dataclass
class PersistedEnvelope:
    model: PersistedModel


@dataclass
class PersistedMapping:
    value: dict[Any, Any]


class Label(str):
    pass


class _MappingExecutor(Executor):
    def __init__(self, payload: Any, path: str) -> None:
        super().__init__(id="mapping-review")
        self.payload = payload
        self.path = path
        self.completed = False

    @handler
    async def handle(self, message: str, ctx: WorkflowContext[Any, Any]) -> None:
        assert message == "payload"
        assert ctx.source_executor_ids == [SOURCE_ORCHESTRATOR]
        if self.path == "outputs":
            await ctx.yield_output(self.payload)
        elif self.path == "sent_messages":
            await ctx.send_message(self.payload, target_id="sink")
        elif self.path == "shared_state":
            ctx.set_state("Shared.mapping", self.payload)
        else:
            raise AssertionError(f"Unexpected activity path: {self.path}")
        self.completed = True


def _run_activity(executor: _MappingExecutor) -> dict[str, Any]:
    input_json = json.dumps({
        "message": "payload",
        "shared_state_snapshot": {},
        "source_executor_ids": [SOURCE_ORCHESTRATOR],
    })
    result = json.loads(execute_workflow_activity(executor, input_json))
    assert executor.completed
    return result


def _assert_activity_result(result: dict[str, Any], path: str, payload: Any) -> None:
    assert deserialize_value(result["outputs"]) == ([payload] if path == "outputs" else [])
    assert deserialize_value(result["sent_messages"]) == (
        [{"message": payload, "target_id": "sink", "source_id": "mapping-review"}] if path == "sent_messages" else []
    )
    assert deserialize_value(result["shared_state_updates"]) == (
        {"Shared.mapping": payload} if path == "shared_state" else {}
    )
    assert result["shared_state_deletes"] == []
    assert result["pending_request_info_events"] == []

    events = [deserialize_workflow_event(event) for event in result["events"]]
    expected_events: list[tuple[str, Any]] = [("executor_invoked", "payload")]
    if path == "outputs":
        expected_events.append(("output", payload))
    expected_events.append(("executor_completed", None if path == "shared_state" else [payload]))
    assert [(event.type, event.data) for event in events] == expected_events
    assert all(event.executor_id == "mapping-review" for event in events)


@pytest.mark.parametrize(
    "bad_key",
    [
        pytest.param(1, id="int"),
        pytest.param(True, id="bool"),
        pytest.param(None, id="none"),
        pytest.param(1.5, id="float"),
        pytest.param(("a",), id="tuple"),
        pytest.param(b"a", id="bytes"),
    ],
)
@pytest.mark.parametrize("nesting", ["root", "dict", "list"])
@pytest.mark.parametrize("path", ["outputs", "sent_messages", "shared_state"])
def test_execute_workflow_activity_rejects_non_string_mapping_keys_before_serializing_results(
    bad_key: Any, nesting: str, path: str
) -> None:
    # Both entries would collide if the serializer coerced the key with str().
    bad_mapping = {bad_key: "non-string key", str(bad_key): "string key"}
    payload: dict[Any, Any] = bad_mapping
    if nesting == "dict":
        payload = {"outer": bad_mapping}
    elif nesting == "list":
        payload = {"outer": [bad_mapping]}
    executor = _MappingExecutor(payload, path)

    with pytest.raises(ValueError, match="Workflow transport dictionaries must use string keys"):
        _run_activity(executor)

    # The handler must finish. Rejection belongs to result serialization, not a broken fixture.
    assert executor.completed


@pytest.mark.parametrize("path", ["outputs", "sent_messages", "shared_state"])
def test_valid_string_keys_preserve_exact_mapping_data(path: str) -> None:
    payload = {
        Label("1"): "text",
        "": "empty key",
        "世界": "unicode key",
        "nested": {"true": True, "none": None, "items": [{"0": 0}, {}]},
    }
    result = _run_activity(_MappingExecutor(payload, path))

    _assert_activity_result(result, path, payload)


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(PersistedCheckpoint(value=7), id="dataclass"),
        pytest.param(PersistedModel(value=3), id="pydantic"),
        pytest.param(PersistedEnvelope(model=PersistedModel(value=3)), id="nested-pydantic"),
        pytest.param(PersistedMapping(value={1: "numeric", "1": "text"}), id="opaque-typed-mapping"),
    ],
)
@pytest.mark.parametrize("path", ["outputs", "sent_messages", "shared_state"])
def test_activity_preserves_native_typed_checkpoint_objects(payload: Any, path: str) -> None:
    result = _run_activity(_MappingExecutor(payload, path))

    _assert_activity_result(result, path, payload)


def test_typed_checkpoint_objects_still_roundtrip() -> None:
    payload = {
        "checkpoint": PersistedCheckpoint(value=7),
        "envelope": PersistedEnvelope(model=PersistedModel(value=3)),
        "opaque": PersistedMapping(value={1: "numeric", "1": "text"}),
    }
    encoded = json.loads(json.dumps(serialize_value(payload), allow_nan=False))
    decoded = deserialize_value(encoded)

    assert decoded == payload
    assert isinstance(decoded["checkpoint"], PersistedCheckpoint)
    assert isinstance(decoded["envelope"], PersistedEnvelope)
    assert isinstance(decoded["envelope"].model, PersistedModel)
    assert isinstance(decoded["opaque"], PersistedMapping)
    assert decoded["opaque"].value == {1: "numeric", "1": "text"}
