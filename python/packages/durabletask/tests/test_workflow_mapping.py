# Copyright (c) Microsoft. All rights reserved.

"""Dictionary-key regression tests for workflow mapping serialization paths."""

from __future__ import annotations

import json
from collections.abc import Generator
from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from unittest.mock import Mock

import pytest
from _execution_test_support import RecordingChatClient
from agent_framework import (
    Agent,
    AgentExecutor,
    AgentResponse,
    Executor,
    Message,
    WorkflowBuilder,
    WorkflowContext,
    handler,
)
from durabletask.task import CompletableTask, OrchestrationContext
from pydantic import BaseModel

from agent_framework_durabletask import DurableAIAgentWorker, execute_workflow_activity
from agent_framework_durabletask._workflows.orchestrator import SOURCE_ORCHESTRATOR
from agent_framework_durabletask._workflows.protocol import wrap_workflow_input
from agent_framework_durabletask._workflows.serialization import (
    deserialize_value,
    deserialize_workflow_event,
    serialize_value,
    serialize_workflow_agent_response,
    validate_workflow_json,
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
    encoded = serialize_value(payload)
    validate_workflow_json(encoded)
    decoded = deserialize_value(json.loads(json.dumps(encoded, allow_nan=False)))

    assert decoded == payload
    assert isinstance(decoded["checkpoint"], PersistedCheckpoint)
    assert isinstance(decoded["envelope"], PersistedEnvelope)
    assert isinstance(decoded["envelope"].model, PersistedModel)
    assert isinstance(decoded["opaque"], PersistedMapping)
    assert decoded["opaque"].value == {1: "numeric", "1": "text"}


def _generated_response(payload: Any, field: str) -> AgentResponse[Any]:
    fields = {"value": payload} if field == "value" else {"additional_properties": {"provider": payload}}
    return AgentResponse(messages=[Message("assistant", ["answer"])], **fields)


def _registered_agent_result(
    response: AgentResponse[Any],
) -> tuple[Generator[Any, Any, Any], list[AgentResponse[Any]], Mock]:
    agent = Agent(client=RecordingChatClient(), name="generated")
    executor = AgentExecutor(agent=agent, id="generated")
    workflow = WorkflowBuilder(name="mapping-review", start_executor=executor, output_from=[executor]).build()
    native = Mock()
    DurableAIAgentWorker(native, deployment_mode="isolated_v2").configure_workflow(workflow)
    native.add_orchestrator.assert_called_once()
    orchestrator = native.add_orchestrator.call_args.args[0]
    assert orchestrator.__name__ == "dafx-mapping-review"

    host = Mock(spec=OrchestrationContext)
    host.instance_id = "mapping-run"
    host.is_replaying = False
    host.new_uuid.return_value = "mapping-correlation"
    entity_task: CompletableTask[Any] = CompletableTask()
    host.call_entity.return_value = entity_task
    generator = orchestrator(host, wrap_workflow_input("payload"))
    batch = next(generator)
    host.call_entity.assert_called_once()
    host.call_activity.assert_not_called()
    assert not batch.is_complete
    # Complete the real SDK task without normalizing the response through JSON first.
    entity_task.complete(response)
    results = batch.get_result()
    assert results == [response]
    assert results[0] is response
    return generator, results, host


@pytest.mark.parametrize("bad_key", [1, True, None, 1.5, ("a",), b"a"])
@pytest.mark.parametrize("nesting", ["root", "dict", "list"])
@pytest.mark.parametrize("field", ["value", "additional_properties"])
def test_generated_response_rejects_non_string_mapping_keys(bad_key: Any, nesting: str, field: str) -> None:
    payload: dict[Any, Any] = {bad_key: "non-string key", str(bad_key): "string key"}
    if nesting == "dict":
        payload = {"outer": payload}
    elif nesting == "list":
        payload = {"outer": [payload]}
    original = deepcopy(payload)
    response = _generated_response(payload, field)

    with pytest.raises(ValueError, match="JSON object keys must be strings"):
        serialize_workflow_agent_response(response)

    assert payload == original
    assert (response.value if field == "value" else response.additional_properties["provider"]) == original


@pytest.mark.parametrize("field", ["value", "additional_properties"])
def test_registered_generated_agent_rejects_nested_non_string_keys_before_publishing(field: str) -> None:
    response = _generated_response({"outer": [{1: "numeric", "1": "text"}]}, field)
    generator, results, host = _registered_agent_result(response)

    with pytest.raises(ValueError, match="JSON object keys must be strings"):
        generator.send(results)

    host.set_custom_status.assert_not_called()


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        pytest.param(
            {Label("1"): "text", "世界": [{"": None, "true": True, "zero": 0}]},
            {"1": "text", "世界": [{"": None, "true": True, "zero": 0}]},
            id="string-mapping",
        ),
        pytest.param(PersistedModel(value=3), {"value": 3}, id="pydantic-value"),
    ],
)
@pytest.mark.parametrize("registered", [False, True], ids=["direct", "registered-workflow"])
def test_generated_response_preserves_valid_json_and_serialized_model_values(
    payload: Any, expected: Any, registered: bool
) -> None:
    response = AgentResponse(
        messages=[Message("assistant", ["answer"])],
        value=payload,
        additional_properties={"provider": {"nested": [{"1": "text", "unknown": None}]}},
    )
    if registered:
        generator, results, host = _registered_agent_result(response)
        with pytest.raises(StopIteration) as completed:
            generator.send(results)
        outputs = completed.value.value
        assert len(outputs) == 1
        encoded = outputs[0]
        status = host.set_custom_status.call_args.args[0]
        output_events = [event for event in status["events"] if event["type"] == "output"]
        assert len(output_events) == 1
        assert output_events[0]["data"] == encoded
    else:
        encoded = serialize_workflow_agent_response(response)

    assert encoded["response"]["value"] == expected
    restored = deserialize_value(json.loads(json.dumps(encoded, allow_nan=False)))
    assert type(restored) is AgentResponse
    assert restored.value == expected
    assert restored.additional_properties == response.additional_properties
    assert response.value == payload


@pytest.mark.parametrize("payload", [(1, 2), b"bytes", {1, 2}, object(), PersistedCheckpoint(value=7)])
def test_workflow_json_rejects_nested_runtime_objects_before_encoder_coercion(payload: Any) -> None:
    with pytest.raises(ValueError, match="JSON values, not runtime objects"):
        validate_workflow_json({"nested": [payload]})


@pytest.mark.parametrize("number", [float("nan"), float("inf"), float("-inf")])
def test_generated_response_still_rejects_nonfinite_values(number: float) -> None:
    with pytest.raises(ValueError, match="finite"):
        serialize_workflow_agent_response(_generated_response({"nested": [number]}, "value"))


@pytest.mark.parametrize("container", ["dict", "list"])
def test_workflow_json_rejects_cycles(container: str) -> None:
    payload: Any = {} if container == "dict" else []
    if container == "dict":
        payload["self"] = payload
    else:
        payload.append(payload)

    with pytest.raises(ValueError, match="cycles"):
        validate_workflow_json(payload)


def test_workflow_json_accepts_shared_subtrees_and_opaque_checkpoint_envelopes_without_decoding() -> None:
    shared = {"unknown": [None, False, 1, 1.5, {"future": "value"}]}
    payload = {
        "first": shared,
        "second": shared,
        "checkpoint": {"__type__": "unavailable.module:Unknown", "__pickled__": "opaque-not-decoded"},
    }
    original = deepcopy(payload)

    validate_workflow_json(payload)

    assert payload == original
    assert payload["first"] is payload["second"]
