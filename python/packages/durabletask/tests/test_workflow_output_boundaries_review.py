# Copyright (c) Microsoft. All rights reserved.

"""Parent output selection and portable generated responses at public boundaries."""

from __future__ import annotations

import asyncio
import builtins
import json
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from datetime import date
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock, patch

import pytest
from agent_framework import (
    AgentExecutorResponse,
    AgentResponse,
    Executor,
    Message,
    Workflow,
    WorkflowBuilder,
    WorkflowContext,
    WorkflowEvent,
    WorkflowExecutor,
    handler,
)
from agent_framework._workflows import _checkpoint_encoding
from agent_framework._workflows._edge import FanInEdgeGroup, SingleEdgeGroup
from durabletask.client import TaskHubGrpcClient
from pydantic import BaseModel, Field
from test_workflow_agent_contract_review import _Adapter, _agent, _InspectChild, _response, _wire

from agent_framework_durabletask import DurableWorkflowClient, deserialize_workflow_output, serialize_agent_response
from agent_framework_durabletask._response_utils import load_agent_response
from agent_framework_durabletask._workflows.activity import execute_workflow_activity
from agent_framework_durabletask._workflows.orchestrator import (
    _FORWARDING_PROVENANCE,
    ExecutorResult,
    TaskType,
    _check_fan_in_ready,
    _route_result_messages,
    run_workflow_orchestrator,
)
from agent_framework_durabletask._workflows.protocol import unwrap_workflow_input
from agent_framework_durabletask._workflows.serialization import (
    SUBWORKFLOW_RESULT_KEY,
    deserialize_value,
    deserialize_workflow_event,
    serialize_value,
    serialize_workflow_agent_response,
    serialize_workflow_event,
    strip_pickle_markers,
)

_INVALID_RESPONSE_VERSIONS: list[Any] = [None, False, True, 0, 2, 99, 1.0, "1", "", [], {}]
_INVALID_RESPONSE_ENVELOPES = [
    *[{"_durable_agent_response": version, "response": {}} for version in _INVALID_RESPONSE_VERSIONS],
    {"_durable_agent_response": 1},
    {"_durable_agent_response": 1, "response": None},
    {"_durable_agent_response": 1, "response": []},
    {"_durable_agent_response": 1, "response": {}, "extra": False},
    {"_durable_agent_response": 1, "response": {}, "__pickled__": "bad", "__type__": "worker:Type"},
]
_VALID_LOOKING_RESPONSE = {
    "_durable_agent_response": 1,
    "response": {"type": "agent_response", "messages": [], "value": False},
}
_LITERAL_RESPONSE_DICTS = [
    *_INVALID_RESPONSE_ENVELOPES,
    _VALID_LOOKING_RESPONSE,
    {"_durable_agent_response": 99, "business": "keep"},
    {"_durable_agent_response": 1, "response": {"type": "agent_response", "messages": False}},
    {"business": "keep"},
]


def _assert_same_value(actual: Any, expected: Any) -> None:
    """Check types too, since equality alone conflates False and zero."""
    assert type(actual) is type(expected)
    if isinstance(expected, dict):
        assert actual.keys() == expected.keys()
        for key in expected:
            _assert_same_value(actual[key], expected[key])
    elif isinstance(expected, (list, tuple)):
        assert len(actual) == len(expected)
        for item, original in zip(actual, expected, strict=True):
            _assert_same_value(item, original)
    elif isinstance(expected, Message):
        assert actual.to_dict() == expected.to_dict()
    else:
        assert actual == expected


@pytest.mark.parametrize("literal", _LITERAL_RESPONSE_DICTS)
@pytest.mark.parametrize("depth", [0, 1, 5])
def test_literal_response_dictionaries_round_trip_at_every_codec_boundary(literal: Any, depth: int) -> None:
    original = deepcopy(literal)
    value = deepcopy(literal)
    for _ in range(depth):
        value = {"items": [value], "controls": [None, False, 0, "", [], {}]}
    before = deepcopy(value)
    # Repeated hops must not turn a restored literal back into a control envelope.
    for _ in range(2):
        encoded = json.loads(json.dumps(serialize_value(value), allow_nan=False))
        _assert_same_value(value, before)
        stored = deepcopy(encoded)
        value = deserialize_value(encoded)
        _assert_same_value(value, before)
        _assert_same_value(encoded, stored)
        _assert_same_value(deserialize_workflow_output([encoded]), [before])
        event = serialize_workflow_event(WorkflowEvent("output", data=value, executor_id="source"))
        restored_event = deserialize_workflow_event(json.loads(json.dumps(event, allow_nan=False)))
        _assert_same_value(restored_event.data, before)
    _assert_same_value(literal, original)


@dataclass
class _TypedLiteral:
    payload: dict[str, Any]


@pytest.mark.parametrize("key", sorted(_checkpoint_encoding._RESERVED_DICT_KEYS | {"_durable_agent_response"}))
def test_literal_escaping_uses_core_envelopes_without_decoding_nested_control_data(key: str) -> None:
    literal = {
        key: "literal",
        "nested": [_VALID_LOOKING_RESPONSE, *_INVALID_RESPONSE_ENVELOPES],
        "message": Message("assistant", ["typed"]),
        "model": _TypedLiteral({"_durable_agent_response": 99}),
    }
    before = deepcopy(literal)
    encoded = json.loads(json.dumps(serialize_value(literal)))
    assert set(encoded) == {"__pickled__", "__type__"}
    assert encoded["__type__"] == "builtins:dict"
    assert encoded == _checkpoint_encoding._encode_pickle(literal)
    # Core and the durable decoder restore one opaque literal.
    _assert_same_value(_checkpoint_encoding.decode_checkpoint_value(encoded), before)
    _assert_same_value(deserialize_value(encoded), before)
    _assert_same_value(literal, before)


def test_mixed_typed_values_literals_and_generated_envelopes_keep_distinct_contracts() -> None:
    literal = deepcopy(_VALID_LOOKING_RESPONSE)
    typed = _TypedLiteral({"items": [_VALID_LOOKING_RESPONSE, {"_durable_agent_response": 99}]})
    message = Message("assistant", ["typed"], additional_properties={"literal": literal})
    values = {"literal": literal, "typed": typed, "message": message, "tuple": (literal, False, None)}
    encoded = serialize_value(values)
    generated = serialize_workflow_agent_response(_response("generated", value=False))
    stored = json.loads(json.dumps({"activity": encoded, "generated": [generated]}, allow_nan=False))
    restored = deserialize_workflow_output(stored)
    _assert_same_value(restored["activity"], values)
    assert type(restored["generated"][0]) is AgentResponse
    assert restored["generated"][0].text == "generated" and restored["generated"][0].value is False
    # Identical JSON shapes are literals when explicitly passed to the value encoder.
    _assert_same_value(deserialize_value(serialize_value(generated)), generated)


@pytest.mark.parametrize("key", [1, False, None, (1, 2)])
@pytest.mark.parametrize("nested", [False, True])
def test_nonstring_dictionary_keys_are_rejected_instead_of_normalized(key: Any, nested: bool) -> None:
    value = {key: [False, 0, None, ""], "typed": _TypedLiteral({"business": "keep"}), "tuple": (1, False)}
    if nested:
        value = {"items": [value]}
    before = deepcopy(value)
    with pytest.raises(ValueError, match="string keys"):
        serialize_value(value)
    _assert_same_value(value, before)


@pytest.mark.parametrize(
    "value",
    [
        None,
        False,
        0,
        -0.0,
        "",
        [],
        {},
        {"1": [False, 0, None, ""], "typed": _TypedLiteral({"business": "keep"}), "tuple": (1, False)},
    ],
)
def test_noncolliding_valid_values_keep_core_encoding_and_typed_checkpoints(value: Any) -> None:
    before = deepcopy(value)
    encoded = serialize_value(value)
    assert encoded == _checkpoint_encoding.encode_checkpoint_value(value)
    _assert_same_value(deserialize_value(json.loads(json.dumps(encoded))), before)
    _assert_same_value(value, before)


@pytest.mark.parametrize("key", sorted(_checkpoint_encoding._RESERVED_DICT_KEYS))
def test_literal_escape_does_not_weaken_the_untrusted_pickle_boundary(key: str) -> None:
    literal = {"_durable_agent_response": 99, "business": "keep"}
    escaped = json.loads(json.dumps(serialize_value(literal)))
    malicious = {key: "not a pickle", "payload": literal}
    raw = {"safe": literal, "items": [malicious, escaped, {"nested": malicious}], "flag": False}
    before = deepcopy(raw)
    with patch.object(_checkpoint_encoding, "_base64_to_unpickle", side_effect=AssertionError("Untrusted pickle")):
        cleaned = strip_pickle_markers(raw)
        _assert_same_value(cleaned, {"safe": literal, "items": [None, None, {"nested": None}], "flag": False})
        _assert_same_value(deserialize_value(cleaned["items"]), [None, None, {"nested": None}])
    _assert_same_value(raw, before)


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


@pytest.mark.parametrize("value", [False, None, {"wireAlias": 0, "nullable": None}, *_LITERAL_RESPONSE_DICTS])
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
    # The orchestrator assembles codec containers from already-encoded items.
    # serialize_value(envelope) would instead encode a literal application dict.
    container = {"outputs": [serialize_workflow_agent_response(_response(value=False))]}
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


@pytest.mark.parametrize("envelope", _INVALID_RESPONSE_ENVELOPES)
def test_invalid_known_envelope_rejected_before_core_decoder(envelope: Any) -> None:
    with (
        patch(
            "agent_framework_durabletask._workflows.serialization.decode_checkpoint_value",
            side_effect=AssertionError("Malformed response envelope must not reach the generic decoder"),
        ),
        pytest.raises(ValueError, match="workflow agent response envelope"),
    ):
        deserialize_workflow_output([{"nested": envelope}])


@pytest.mark.parametrize(
    ("payload", "error", "message"),
    [
        ({"type": ""}, ValueError, "Response type"),
        ({"type": "agent_response", "messages": False}, TypeError, "sequence of messages"),
    ],
)
def test_known_envelope_still_validates_base_response_fields(
    payload: Any, error: type[Exception], message: str
) -> None:
    before = deepcopy(payload)
    with pytest.raises(error, match=message):
        deserialize_value({"_durable_agent_response": 1, "response": payload})
    assert payload == before


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"type": "agent_response"},
        {"messages": None},
        {"type": "agent_response", "messages": None},
        {"messages": []},
        {"type": "agent_response", "messages": []},
    ],
)
def test_known_envelope_accepts_sparse_and_null_messages_as_empty_response(payload: Any) -> None:
    envelope = {"_durable_agent_response": 1, "response": payload}
    before = deepcopy(envelope)
    restored = deserialize_value(envelope)
    output = deserialize_workflow_output([envelope])
    assert len(output) == 1
    for response in (restored, output[0]):
        assert type(response) is AgentResponse
        assert response.messages == []
        assert response.text == ""
        assert response.value is None
    assert envelope == before


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
    before = deepcopy(envelope)
    forbidden_import = Mock(side_effect=AssertionError("Stored type names are not imported"))
    forbidden_constructor = Mock(side_effect=AssertionError("A rejected response must not construct its format"))
    original_import = builtins.__import__

    def guarded_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name.startswith("worker"):
            return forbidden_import(name)
        return original_import(name, *args, **kwargs)

    with (
        patch("importlib.import_module", forbidden_import),
        patch.object(builtins, "__import__", guarded_import),
    ):
        with (
            patch.object(AgentResponse, "__init__", forbidden_constructor),
            pytest.raises(ValueError, match="Response type"),
        ):
            deserialize_workflow_output(envelope)
        forbidden_constructor.assert_not_called()
        canonical = deepcopy(envelope)
        cast(dict[str, Any], canonical["response"])["type"] = "agent_response"
        before_canonical = deepcopy(canonical)
        restored = deserialize_workflow_output(canonical)
    forbidden_import.assert_not_called()
    assert type(restored) is AgentResponse and restored.value == {"type": "business.kind", "flag": False}
    assert "response_format" not in serialize_agent_response(restored)
    assert envelope == before
    assert canonical == before_canonical


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


class _SendValue(Executor):
    def __init__(self, value: Any) -> None:
        super().__init__(id="source")
        self.value = value

    @handler
    async def take(self, message: str, ctx: WorkflowContext[Any, Any]) -> None:
        await ctx.send_message(self.value)


class _FixedOutput(Executor):
    def __init__(self, value: Any) -> None:
        super().__init__(id="child-source")
        self.value = value

    @handler
    async def take(self, message: str, ctx: WorkflowContext[Any, Any]) -> None:
        await ctx.yield_output(self.value)


class _OutputSink(Executor):
    def __init__(self) -> None:
        super().__init__(id="sink")
        self.received: list[Any] = []

    @handler
    async def take(self, value: Any, ctx: WorkflowContext[Any, Any]) -> None:
        self.received.append(value)
        await ctx.yield_output(value)


class _PassThroughDictionary(Executor):
    def __init__(self) -> None:
        super().__init__(id="source")

    @handler
    async def take(self, value: dict[str, Any], ctx: WorkflowContext[Any, Any]) -> None:
        await ctx.send_message(value)


class _PassThroughAny(Executor):
    def __init__(self) -> None:
        super().__init__(id="source")

    @handler
    async def take(self, value: Any, ctx: WorkflowContext[Any, Any]) -> None:
        await ctx.send_message(value)


@dataclass
class _ApplicationInput:
    type: str
    payload: dict[str, Any]


class _ApplicationInputModel(BaseModel):
    type: str
    payload: dict[str, Any]


class _PassThroughDataclass(Executor):
    def __init__(self) -> None:
        super().__init__(id="source")

    @handler
    async def take(self, value: _ApplicationInput, ctx: WorkflowContext[Any, Any]) -> None:
        assert type(value) is _ApplicationInput
        await ctx.send_message(value)


class _PassThroughModel(Executor):
    def __init__(self) -> None:
        super().__init__(id="source")

    @handler
    async def take(self, value: _ApplicationInputModel, ctx: WorkflowContext[Any, Any]) -> None:
        assert type(value) is _ApplicationInputModel
        await ctx.send_message(value)


@pytest.mark.parametrize("adapter", ["dt", "af"])
@pytest.mark.parametrize("input_kind", ["any", "dict", "dataclass", "model"])
@pytest.mark.parametrize(
    "value",
    [
        {"_durable_agent_response": 99, "business": "keep"},
        {"items": [{"_durable_agent_response": 99, "business": "keep"}]},
        _VALID_LOOKING_RESPONSE,
        {"items": [_VALID_LOOKING_RESPONSE]},
        {"_durable_agent_response": 1, "response": {"type": "agent_response", "messages": False}},
    ],
)
async def test_plain_dictionary_input_survives_real_activity_routing(adapter: str, input_kind: str, value: Any) -> None:
    raw: dict[str, Any]
    if input_kind == "dataclass":
        source: Executor = _PassThroughDataclass()
        raw = {"type": "untrusted.module:Class", "payload": deepcopy(value)}
        core_input: Any = _ApplicationInput(**raw)
    elif input_kind == "model":
        source = _PassThroughModel()
        raw = {"type": "untrusted.module:Class", "payload": deepcopy(value)}
        core_input = _ApplicationInputModel(**raw)
    else:
        source = _PassThroughAny() if input_kind == "any" else _PassThroughDictionary()
        raw = deepcopy(value)
        core_input = deepcopy(raw)
    sink = _OutputSink()
    workflow = WorkflowBuilder(name="literal-input", start_executor=source, output_from=[sink]).add_edge(source, sink)
    built = workflow.build()
    before, raw_before = deepcopy(core_input), deepcopy(raw)
    expected = await built.run(core_input)
    _assert_same_value(expected.get_outputs(), [before])
    sink.received.clear()
    host = _Adapter(adapter)
    generator = run_workflow_orchestrator(host.context, built, json.loads(json.dumps(raw)))
    with patch("importlib.import_module", side_effect=AssertionError("Input type fields are application data")):
        yielded = next(generator)
    _assert_same_value(deserialize_value(json.loads(host.activity_input())["message"]), before)
    first = await asyncio.to_thread(execute_workflow_activity, source, host.activity_input(), built)
    yielded = generator.send(host.complete(yielded, first))
    _assert_same_value(deserialize_value(json.loads(host.activity_input())["message"]), before)
    last = await asyncio.to_thread(execute_workflow_activity, sink, host.activity_input(), built)
    _assert_same_value(deserialize_workflow_output(_finish_raw(host, generator, yielded, last)), [before])
    _assert_same_value(sink.received, [before])
    _assert_same_value(core_input, before)
    _assert_same_value(raw, raw_before)


@pytest.mark.parametrize("adapter", ["dt", "af"])
@pytest.mark.parametrize("origin", ["activity", "child"])
@pytest.mark.parametrize(
    "value",
    [
        None,
        False,
        0,
        "",
        [],
        {},
        {"_durable_agent_response": 99, "business": "keep"},
        {"items": [{"_durable_agent_response": 99, "business": "keep"}]},
        _VALID_LOOKING_RESPONSE,
        {"_durable_agent_response": 1, "response": None},
        {"__pickled__": "literal", "__type__": "business", "_durable_agent_response": 99},
    ],
)
async def test_activity_and_child_route_explicit_values_through_both_hosts(
    adapter: str, origin: str, value: Any
) -> None:
    source = _SendValue(deepcopy(value))
    child_source = _FixedOutput(deepcopy(value))
    inner = WorkflowBuilder(name="inner-literal", start_executor=child_source, output_from=[child_source]).build()
    child = WorkflowExecutor(inner, id="child", allow_direct_output=False)
    start = child if origin == "child" else source
    sink = _OutputSink()
    outer = (
        WorkflowBuilder(name="outer-literal", start_executor=start, output_from=[sink]).add_edge(start, sink).build()
    )
    # Core starts on a non-null input. The explicit None is produced inside the
    # running activity/child, not confused with Workflow.run's absent start input.
    expected = await outer.run("start")
    _assert_same_value(expected.get_outputs(), [value])
    _assert_same_value(sink.received, [value])
    sink.received.clear()

    host = _Adapter(adapter)
    generator = run_workflow_orchestrator(host.context, outer, "start")
    yielded = next(generator)
    if origin == "child":
        kind, _, _, kwargs = host.pending[0]
        assert kind == "child"
        child_input = unwrap_workflow_input(kwargs["input"] if adapter == "dt" else kwargs["input_"])
        child_host = _Adapter(adapter)
        child_host.native.instance_id = kwargs["instance_id"]
        child_generator = run_workflow_orchestrator(child_host.context, inner, child_input)
        child_yielded = next(child_generator)
        activity_result = await asyncio.to_thread(
            execute_workflow_activity, child_source, child_host.activity_input(), inner
        )
        result = _finish_raw(child_host, child_generator, child_yielded, activity_result)
        assert result[SUBWORKFLOW_RESULT_KEY] is True
        _assert_same_value(deserialize_value(result["outputs"]), [value])
    else:
        result = await asyncio.to_thread(execute_workflow_activity, source, host.activity_input(), outer)
    try:
        yielded = generator.send(host.complete(yielded, result))
    except StopIteration:
        pytest.fail("An explicit activity/child message was dropped before scheduling the sink")
    encoded_input = host.activity_input()
    _assert_same_value(deserialize_value(json.loads(encoded_input)["message"]), value)
    activity_result = await asyncio.to_thread(execute_workflow_activity, sink, encoded_input, outer)
    raw = _finish_raw(host, generator, yielded, activity_result)
    _assert_same_value(sink.received, [value])
    _assert_same_value(deserialize_workflow_output(raw), expected.get_outputs())
    if adapter == "af":
        assert all("events" not in status for status in host.statuses)
    else:
        state = SimpleNamespace(
            name="dafx-outer-literal",
            runtime_status=SimpleNamespace(name="COMPLETED"),
            serialized_output=json.dumps(raw),
            serialized_custom_status=json.dumps(host.statuses[-1]),
        )
        native = Mock(spec=TaskHubGrpcClient)
        native.wait_for_orchestration_completion.return_value = state
        native.get_orchestration_state.return_value = state
        client = DurableWorkflowClient(native, workflow_name="outer-literal")
        _assert_same_value(client.await_workflow_output("contract-run"), [value])
        events = [event async for event in client.stream_workflow("contract-run")]
        _assert_same_value([event.data for event in events if event.type == "output"], [value])


@pytest.mark.parametrize("route", ["edge", "explicit", "fan-in"])
@pytest.mark.parametrize("present", [False, True])
def test_routing_distinguishes_missing_payload_from_explicit_null(route: str, present: bool) -> None:
    workflow = Mock(spec=Workflow)
    group = FanInEdgeGroup(["source", "other"], "sink") if route == "fan-in" else SingleEdgeGroup("source", "sink")
    workflow.edge_groups = [] if route == "explicit" else [group]
    message: dict[str, Any] = {"target_id": "sink" if route == "explicit" else None}
    if present:
        message["message"] = None
    result = ExecutorResult(
        executor_id="source",
        output_message=None,
        activity_result={"sent_messages": [message]},
        task_type=TaskType.ACTIVITY,
    )
    pending: dict[str, list[tuple[Any, str]]] = {}
    fan_in: dict[str, dict[str, list[tuple[Any, str]]]] = {group.id: defaultdict(list)}
    _route_result_messages(result, workflow, pending, fan_in)
    if route == "fan-in":
        assert bool(fan_in[group.id].get("source")) is present
        fan_in[group.id]["other"] = [(False, "other")]
        _check_fan_in_ready(workflow, fan_in, pending)
    if present:
        assert len(pending["sink"]) == 1
        _assert_same_value(pending["sink"][0][0], [None, False] if route == "fan-in" else None)
    else:
        assert pending == {}
