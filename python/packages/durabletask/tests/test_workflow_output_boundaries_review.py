# Copyright (c) Microsoft. All rights reserved.

"""Parent output selection and portable generated responses at public boundaries."""

from __future__ import annotations

import asyncio
import json
from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

import pytest
from agent_framework import (
    AgentExecutorResponse,
    AgentResponse,
    Workflow,
    WorkflowBuilder,
    WorkflowEvent,
    WorkflowExecutor,
)
from agent_framework._workflows import _checkpoint_encoding
from durabletask.client import TaskHubGrpcClient
from pydantic import BaseModel, Field
from test_workflow_agent_contract_review import _Adapter, _agent, _InspectChild, _response, _wire

from agent_framework_durabletask import DurableWorkflowClient, deserialize_workflow_output, serialize_agent_response
from agent_framework_durabletask._response_utils import load_agent_response
from agent_framework_durabletask._workflows.activity import execute_workflow_activity
from agent_framework_durabletask._workflows.orchestrator import _FORWARDING_PROVENANCE, run_workflow_orchestrator
from agent_framework_durabletask._workflows.protocol import unwrap_workflow_input
from agent_framework_durabletask._workflows.serialization import (
    deserialize_value,
    deserialize_workflow_event,
    serialize_value,
    serialize_workflow_agent_response,
)


def _finish_raw(host: _Adapter, generator: Any, yielded: Any, value: Any) -> Any:
    with pytest.raises(StopIteration) as completed:
        generator.send(host.complete(yielded, value))
    return json.loads(json.dumps(completed.value.value, allow_nan=False))


def _nested(direct: bool, designation: str) -> tuple[Workflow, Workflow, _InspectChild]:
    progress = _agent("progress", [_response("progress")])
    answer = _agent("answer", [_response("answer")])
    inner = (
        WorkflowBuilder(
            name="inner", start_executor=progress, output_from=[answer], intermediate_output_from=[progress]
        )
        .add_edge(progress, answer)
        .build()
    )
    child = WorkflowExecutor(inner, id="child", allow_direct_output=direct)
    sink = _InspectChild()
    options: dict[str, Any] = {}
    if designation != "omitted":
        options = {
            "output_from": [child, sink] if designation == "output" else [sink],
            "intermediate_output_from": [child] if designation == "intermediate" else [],
        }
    outer = WorkflowBuilder(name="outer", start_executor=child, **options).add_edge(child, sink).build()
    return outer, inner, sink


@pytest.mark.parametrize("adapter", ["dt", "af"])
@pytest.mark.parametrize("direct", [False, True])
@pytest.mark.parametrize("designation", ["hidden", "intermediate", "output", "omitted"])
async def test_child_outputs_follow_parent_yield_policy_and_core_events(
    adapter: str, direct: bool, designation: str
) -> None:
    core, _, _ = _nested(direct, designation)
    expected = await core.run("question")
    outer, inner, sink = _nested(direct, designation)
    host = _Adapter(adapter)
    generator = run_workflow_orchestrator(host.context, outer, "question")
    yielded = next(generator)
    kind, _, _, kwargs = host.pending[0]
    assert kind == "child"
    child_input = unwrap_workflow_input(kwargs["input"] if adapter == "dt" else kwargs["input_"])
    child_host = _Adapter(adapter)
    child_host.native.instance_id = kwargs["instance_id"]
    child_generator = run_workflow_orchestrator(child_host.context, inner, child_input)
    child_yielded = next(child_generator)
    child_yielded = child_generator.send(child_host.complete(child_yielded, _wire(_response("progress"))))
    child_result = _finish_raw(child_host, child_generator, child_yielded, _wire(_response("answer")))
    assert child_result["outputs"][0]["_durable_agent_response"] == 1

    if direct:
        raw = _finish_raw(host, generator, yielded, child_result)
        host.native.call_activity.assert_not_called()
    else:
        yielded = generator.send(host.complete(yielded, child_result))
        activity_input = host.activity_input()
        assert type(deserialize_value(json.loads(activity_input)["message"])) is AgentResponse
        activity_result = await asyncio.to_thread(execute_workflow_activity, sink, activity_input, outer)
        raw = _finish_raw(host, generator, yielded, activity_result)

    def snapshot(value: Any) -> Any:
        return serialize_agent_response(value) if isinstance(value, AgentResponse) else value

    assert [snapshot(value) for value in deserialize_workflow_output(raw)] == [
        snapshot(value) for value in expected.get_outputs()
    ]
    if adapter == "af":
        assert all("events" not in status for status in [*host.statuses, *child_host.statuses])
        return

    events = [deserialize_workflow_event(event) for event in host.statuses[-1]["events"]]
    actual_yields = [event for event in events if event.type in ("output", "intermediate")]
    expected_yields = [event for event in expected if event.type in ("output", "intermediate")]
    assert all(isinstance(event, WorkflowEvent) for event in actual_yields)
    assert [(e.type, e.executor_id, snapshot(e.data)) for e in actual_yields] == [
        (e.type, e.executor_id, snapshot(e.data)) for e in expected_yields
    ]
    child_events = [event for event in events if event.executor_id == "child"]
    assert child_events[0].type == "executor_invoked"
    assert child_events[-1].type == "executor_completed"
    # Core forwards inner intermediate events even when the child node's own
    # direct yields are hidden, and outputs precede that forwarded progress.
    assert child_events[-2].type == "intermediate" and child_events[-2].data.text == "progress"


@pytest.mark.parametrize("adapter", ["dt", "af"])
@pytest.mark.parametrize("designation", ["output", "intermediate", "hidden"])
def test_worker_local_model_is_typed_in_conditions_but_never_pickled_for_generated_yields(
    adapter: str, designation: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    class WorkerAnswer(BaseModel):
        answer: int = Field(validation_alias="inputAnswer", serialization_alias="outputAnswer")
        day: date

    a = _agent("A", response_format=WorkerAnswer)
    b = _agent("B")
    observed: list[AgentExecutorResponse] = []

    def condition(value: AgentExecutorResponse) -> bool:
        assert type(value) is AgentExecutorResponse
        assert isinstance(value.agent_response.value, WorkerAnswer)
        assert value.agent_response.value.day == date(2026, 9, 9)
        observed.append(value)
        return True

    workflow = (
        WorkflowBuilder(
            name="portable",
            start_executor=a,
            output_from=[a, b] if designation == "output" else [b],
            intermediate_output_from=[a] if designation == "intermediate" else [],
        )
        .add_edge(a, b, condition=condition)
        .build()
    )
    response = _response("not JSON", value=WorkerAnswer(inputAnswer=42, day=date(2026, 9, 9)))
    setattr(response, _FORWARDING_PROVENANCE, ("private", response.messages))
    external = serialize_workflow_agent_response(response)
    assert _FORWARDING_PROVENANCE not in json.dumps(external)
    assert getattr(response, _FORWARDING_PROVENANCE) == ("private", response.messages)
    host = _Adapter(adapter)
    generator = run_workflow_orchestrator(host.context, workflow, "question")
    yielded = next(generator)
    no_pickle = Mock(side_effect=AssertionError("Generated responses must not require worker classes"))
    monkeypatch.setattr(_checkpoint_encoding, "_pickle_to_base64", no_pickle)
    monkeypatch.setattr(_checkpoint_encoding, "_base64_to_unpickle", no_pickle)
    yielded = generator.send(host.complete(yielded, _wire(response)))
    assert len(observed) == 1
    raw = _finish_raw(host, generator, yielded, _wire(_response("last", value=False)))
    assert "__pickled__" not in json.dumps([raw, host.statuses])
    assert _FORWARDING_PROVENANCE not in json.dumps([raw, host.statuses])
    assert "WorkerAnswer" not in json.dumps([raw, host.statuses])
    with patch("importlib.import_module", side_effect=AssertionError("Client must not import stored response types")):
        output = deserialize_workflow_output(raw)
        events = [deserialize_workflow_event(event) for event in host.statuses[-1].get("events", [])]
    assert all(type(value) is AgentResponse for value in output)
    assert output[-1].value is False
    if designation == "output":
        assert output[0].value == {"answer": 42, "day": "2026-09-09"}
        assert raw[0]["response"]["_durable_value_by_name"] is True
    if adapter == "dt" and designation != "hidden":
        emitted = next(event for event in events if event.executor_id == "A" and event.type == designation)
        assert type(emitted.data) is AgentResponse
        assert emitted.data.value == {"answer": 42, "day": "2026-09-09"}
    no_pickle.assert_not_called()


@pytest.mark.parametrize("value", [False, None, {"wireAlias": 0, "nullable": None}])
async def test_public_client_returns_response_values_and_streamed_events_without_type_resolution(value: Any) -> None:
    response = load_agent_response({"type": "agent_response", "messages": [], "value": value})
    host = _Adapter("dt")
    workflow = WorkflowBuilder(name="portable", start_executor=_agent("A")).build()
    generator = run_workflow_orchestrator(host.context, workflow, "question")
    raw = _finish_raw(host, generator, next(generator), _wire(response))
    state = SimpleNamespace(
        name="dafx-portable",
        runtime_status=SimpleNamespace(name="COMPLETED"),
        serialized_output=json.dumps(raw),
        serialized_custom_status=json.dumps(host.statuses[-1]),
    )
    native = Mock(spec=TaskHubGrpcClient)
    native.wait_for_orchestration_completion.return_value = state
    native.get_orchestration_state.return_value = state
    client = DurableWorkflowClient(native, workflow_name="portable")
    with (
        patch("importlib.import_module", side_effect=AssertionError("No response type imports")),
        patch.object(_checkpoint_encoding, "_base64_to_unpickle", side_effect=AssertionError("No response pickle")),
    ):
        output = client.await_workflow_output("contract-run")
        events = [event async for event in client.stream_workflow("contract-run")]
    assert len(output) == 1 and type(output[0]) is AgentResponse
    emitted = [event.data for event in events if event.type == "output"]
    assert len(emitted) == 1 and type(emitted[0]) is AgentResponse
    for restored in [output[0], emitted[0]]:
        assert restored.value == value and type(restored.value) is type(value)
        assert "value" in serialize_agent_response(restored)


def test_known_envelopes_recurse_only_through_codec_containers_not_response_application_data() -> None:
    application = {
        "type": "worker.only:Model",
        "__pickled__": "application data, not a pickle",
        "__type__": "application:type",
        "nested": {"_durable_agent_response": 99, "response": {"type": "business"}},
    }
    response = load_agent_response({
        "type": "agent_response",
        "messages": [],
        "value": application,
        "additional_properties": application,
    })
    envelope = serialize_workflow_agent_response(response)
    plain_response_dict = {"type": "agent_response", "messages": [], "value": False}
    # Plain lists/dicts produced by the core encoder can carry a known envelope.
    container = serialize_value({"outputs": [serialize_workflow_agent_response(_response(value=False))]})
    container["outputs"].extend([envelope, plain_response_dict])
    with (
        patch("importlib.import_module", side_effect=AssertionError("Application types are not imported")),
        patch.object(_checkpoint_encoding, "_base64_to_unpickle", side_effect=AssertionError("Data is not pickle")),
    ):
        restored = deserialize_workflow_output(json.loads(json.dumps(container)))
    first, second, plain = restored["outputs"]
    assert type(first) is AgentResponse and first.value is False
    assert type(second) is AgentResponse and second.value == application
    assert second.additional_properties == application
    assert type(plain) is dict and plain == plain_response_dict


@pytest.mark.parametrize(
    "envelope",
    [
        *[{"_durable_agent_response": version, "response": {}} for version in [None, False, True, 0, 2, 1.0, "1"]],
        {"_durable_agent_response": 1},
        {"_durable_agent_response": 1, "response": None},
        {"_durable_agent_response": 1, "response": []},
        {"_durable_agent_response": 1, "response": {}, "extra": False},
        {"_durable_agent_response": 1, "response": {}, "__pickled__": "bad", "__type__": "worker:Type"},
    ],
)
def test_invalid_known_envelope_rejected_before_core_decoder(envelope: Any) -> None:
    with (
        patch(
            "agent_framework_durabletask._workflows.serialization.decode_checkpoint_value",
            side_effect=AssertionError("Malformed response envelope must not reach the generic decoder"),
        ),
        pytest.raises(ValueError, match="workflow agent response envelope"),
    ):
        deserialize_workflow_output([{"nested": envelope}])


@pytest.mark.parametrize("payload", [{}, {"type": ""}, {"type": "agent_response", "messages": False}])
def test_known_envelope_still_validates_base_response_fields(payload: Any) -> None:
    with pytest.raises((ValueError, TypeError)):
        deserialize_value({"_durable_agent_response": 1, "response": payload})


def test_stored_response_type_and_format_are_not_client_constructor_instructions() -> None:
    envelope = {
        "_durable_agent_response": 1,
        "response": {
            "type": "worker.only:Response",
            "response_format": "worker.only:Model",
            "messages": [],
            "value": {"type": "business.kind", "flag": False},
        },
    }
    with patch("importlib.import_module", side_effect=AssertionError("Stored type names are not imported")):
        restored = deserialize_workflow_output(envelope)
    assert type(restored) is AgentResponse and restored.value == {"type": "business.kind", "flag": False}


def test_existing_internal_pickle_contract_and_escaped_application_dictionary_are_unchanged() -> None:
    from test_workflow_agent_contract_review import Answer

    response = _response(value=Answer(inputAnswer=42))
    internal = AgentExecutorResponse("A", response, full_conversation=response.messages)
    encoded = serialize_value(internal)
    assert "__pickled__" in encoded
    restored = deserialize_value(encoded)
    assert type(restored) is AgentExecutorResponse and isinstance(restored.agent_response.value, Answer)
    assert "__pickled__" in serialize_value(response)
    application = {"__pickled__": "literal", "__type__": "business", "_durable_agent_response": 99}
    assert deserialize_value(serialize_value(application)) == application
