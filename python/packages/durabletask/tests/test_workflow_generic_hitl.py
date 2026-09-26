# Copyright (c) Microsoft. All rights reserved.

"""Generic HITL contracts through Core, real producers and public SDK replies.

The transport completes real SDK tasks, while registered closures own workflow
execution and admission. These are not live-service or recorded-history tests.
"""

import asyncio
import builtins
import importlib
import json
import pickle
import sys
from copy import deepcopy
from dataclasses import dataclass
from types import ModuleType, SimpleNamespace
from typing import Any, Literal, Optional, Union
from unittest.mock import Mock

import pytest
from _workflow_generic_hitl_test_support import (
    _CONCRETE_REPLAY_CASES,
    _PENDING_STATUS_CASES,
    _complete_generic_activity,
    _concrete_replay_trial,
    _core_trial,
    _CountedDataclass,
    _CountedDecision,
    _generic_workflow,
    _handler_failure_trial,
    _invalid_generic_replay_trial,
    _Models,
    _validator_replay_trial,
)
from _workflow_generic_hitl_test_support import (
    _VALIDATOR_CALLS as _VALIDATOR_CALLS,
)
from _workflow_lifecycle_test_support import _Transport
from agent_framework import Executor, WorkflowContext, WorkflowEvent, handler, response_handler
from agent_framework._workflows._typing_utils import is_instance_of, try_coerce_to_type
from durabletask.client import OrchestrationStatus, TaskHubGrpcClient

from agent_framework_durabletask import DurableWorkflowClient, execute_workflow_activity
from agent_framework_durabletask._workflows import activity as activity_module
from agent_framework_durabletask._workflows import orchestrator as engine
from agent_framework_durabletask._workflows.serialization import (
    deserialize_response_type,
    deserialize_workflow_event,
    deserialize_workflow_output,
    reconstruct_to_type,
    resolve_type,
    serialize_response_type,
    serialize_workflow_event,
)


@dataclass
class _DataclassDecision:
    count: int


_CASES = [
    pytest.param(list[int], [1, True], True, [7], id="bool-is-int-subclass"),
    pytest.param(list[int], ["bad"], False, [7], id="bad-list-member"),
    pytest.param(list[int], ["1"], False, [7], id="no-numeric-string-coercion"),
    pytest.param(list[int], [1.0], False, [7], id="no-float-coercion"),
    pytest.param(list[bool], [1], False, [True], id="int-is-not-bool"),
    pytest.param(list[int], [], True, [7], id="empty-list"),
    pytest.param(list[int], None, False, [7], id="null-is-not-list"),
    pytest.param(dict[str, list[int]], {"x": ["bad"]}, False, {"x": [7]}, id="nested-invalid"),
    pytest.param(dict[str, list[int]], {"x": [True, 7]}, True, {"x": [7]}, id="nested-valid"),
    pytest.param(list[int | str], [1, "yes"], True, [7], id="pep604-member-union"),
    pytest.param(Union[list[int], str], ["bad"], False, "yes", id="typing-union-invalid"),
    pytest.param(list[int] | str, ["bad"], False, "yes", id="pep604-union-invalid"),
    pytest.param(Optional[list[int]], None, True, [7], id="explicit-null-optional"),
    pytest.param(Optional[list[int]], [None], False, [7], id="optional-list-not-members"),
    pytest.param(list[int | None], [None, 7], True, [7], id="nullable-member"),
    pytest.param(list[Any], [{"response_type": "os:system"}], True, [], id="any-members-are-data"),
    pytest.param(
        list[int],
        {"response": ["bad"], "response_type": "builtins:object"},
        False,
        [7],
        id="forged-legacy-type-in-reply",
    ),
    pytest.param(
        list[int],
        {"response": ["bad"], "response_type": {"_durable_response_type": 1, "kind": "list", "args": ["typing:Any"]}},
        False,
        [7],
        id="forged-generic-type-in-reply",
    ),
]


@pytest.mark.parametrize(("requested", "answer", "accepted", "correction"), _CASES)
def test_registered_generic_request_matches_core_and_invalid_reply_remains_pending(
    requested: Any, answer: Any, accepted: bool, correction: Any
) -> None:
    core_accepted, oracle = asyncio.run(_core_trial(requested, answer))
    assert core_accepted is accepted
    workflow, seen = _generic_workflow(requested)
    transport = _Transport()
    native = Mock(spec=TaskHubGrpcClient)
    public = DurableWorkflowClient(native)
    try:
        run = transport.start(workflow)
        native.get_orchestration_state.side_effect = transport.state
        pending = public.get_pending_hitl_requests("root-run")
        assert len(pending) == 1 and pending[0]["request_id"] == "approval"
        assert deserialize_response_type(pending[0]["response_type"]) == requested
        request_events = [e for e in run.host.statuses[-1]["events"] if e["type"] == "request_info"]
        assert request_events[0]["response_type"] == pending[0]["response_type"]
        assert deserialize_workflow_event(request_events[0]).response_type == requested

        public.send_hitl_response("root-run", "approval", deepcopy(answer))
        wire = native.raise_orchestration_event.call_args.kwargs["data"]
        assert wire == answer and type(wire) is type(answer)
        transport.event("approval", wire)
        assert seen == oracle
        if not accepted:
            assert not run.done and len(run.calls) == 2
            assert public.get_pending_hitl_requests("root-run") == pending
            # Reject twice, replacing only this event wait, then accept a correction.
            public.send_hitl_response("root-run", "approval", deepcopy(answer))
            transport.event("approval", native.raise_orchestration_event.call_args.kwargs["data"])
            assert not seen and not run.done and len(run.calls) == 3
            public.send_hitl_response("root-run", "approval", correction)
            transport.event("approval", native.raise_orchestration_event.call_args.kwargs["data"])
            assert len(transport.events["root-run"]["approval"]) == 3
            assert seen == [correction]
        assert run.done and len(run.calls) == (2 if accepted else 4)
        assert deserialize_workflow_output(run.output) == [{"value": seen[0]}]
        assert public.get_pending_hitl_requests("root-run") == []
        native.raise_orchestration_event.reset_mock()
        # The transport has no runtime status. Absence must not be invented as a
        # terminal state or used to reintroduce a pending-membership restriction.
        public.send_hitl_response("root-run", "approval", correction)
        native.raise_orchestration_event.assert_called_once()
    finally:
        transport.close()


def test_real_activity_and_event_producers_preserve_generic_arguments_as_json() -> None:
    workflow, seen = _generic_workflow(dict[str, list[int | None]])
    result = json.loads(execute_workflow_activity(workflow.executors["gate"], json.dumps({"message": "go"}), workflow))
    expected = {
        "_durable_response_type": 1,
        "kind": "dict",
        "args": [
            "builtins:str",
            {
                "_durable_response_type": 1,
                "kind": "list",
                "args": [{"_durable_response_type": 1, "kind": "union", "args": ["builtins:int", "builtins:NoneType"]}],
            },
        ],
    }
    assert result["pending_request_info_events"][0]["response_type"] == expected
    assert next(e for e in result["events"] if e["type"] == "request_info")["response_type"] == expected
    assert not seen


@pytest.mark.parametrize(
    ("annotation", "wire", "typed"),
    [
        (list[_Models.Decision], [{"count": 7, "verdict": "approve"}], [_Models.Decision(count=7, verdict="approve")]),
        (
            dict[str, list[_Models.Decision]],
            {"x": [{"count": 7, "verdict": "approve"}]},
            {"x": [_Models.Decision(count=7, verdict="approve")]},
        ),
        (list[_DataclassDecision], [{"count": 7}], [_DataclassDecision(7)]),
        (tuple[int, str], [7, "yes"], (7, "yes")),
        (tuple[int, ...], [7, 8], (7, 8)),
        (tuple[list[int], str], [[7], "yes"], ([7], "yes")),
        (set[int], [7, 8], {7, 8}),
    ],
)
def test_nested_models_and_tuple_json_follow_installed_core_not_pydantic_container_coercion(
    annotation: Any, wire: Any, typed: Any
) -> None:
    core_accepted, oracle = asyncio.run(_core_trial(annotation, wire))
    # Core 1.13 cannot reconstruct these JSON containers. Core 1.16 can. Native
    # typed controls must work in both, so rejecting everything cannot pass.
    assert asyncio.run(_core_trial(annotation, typed)) == (True, [typed])
    workflow, seen = _generic_workflow(annotation)
    transport = _Transport()
    try:
        run = transport.start(workflow)
        transport.event("approval", wire)
        assert seen == oracle
        assert run.done is core_accepted
        if core_accepted:
            assert seen == [typed]
        else:
            assert len(run.calls) == 2
            assert set(run.host.statuses[-1]["pending_requests"]) == {"approval"}
    finally:
        transport.close()


@pytest.mark.parametrize("payload", [[{"count": "bad", "verdict": "approve"}], [{"count": 1, "verdict": "other"}]])
def test_nested_model_validation_rejects_invalid_fields_and_literal_members(payload: Any) -> None:
    assert asyncio.run(_core_trial(list[_Models.Decision], payload)) == (False, [])
    workflow, seen = _generic_workflow(list[_Models.Decision])
    transport = _Transport()
    try:
        run = transport.start(workflow)
        transport.event("approval", payload)
        assert not run.done and not seen and len(run.calls) == 2
        assert set(run.host.statuses[-1]["pending_requests"]) == {"approval"}
    finally:
        transport.close()


@pytest.mark.parametrize(
    "annotation", [int, object, list[int], dict[str, list[int | None]], tuple[int, ...], set[int], Any]
)
def test_annotation_roundtrip_has_no_pickle_eval_or_selected_import(
    annotation: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    forbidden = Mock(side_effect=AssertionError("Type descriptors must not import or unpickle"))
    monkeypatch.setattr(builtins, "eval", forbidden)
    monkeypatch.setattr(importlib, "import_module", forbidden)
    monkeypatch.setattr(pickle, "loads", forbidden)
    descriptor = json.loads(json.dumps(serialize_response_type(annotation)))
    assert deserialize_response_type(descriptor) == annotation
    forbidden.assert_not_called()


def test_exact_nested_qualname_resolution_and_no_module_attribute_hooks(monkeypatch: pytest.MonkeyPatch) -> None:
    key = "_workflow_generic_hitl_test_support:_Models.Decision"
    assert serialize_response_type(_Models.Decision) == key
    assert deserialize_response_type(key) is _Models.Decision
    module = ModuleType("generic_hitl_untrusted_namespace")
    hook = Mock(side_effect=AssertionError("Module __getattr__ must not run"))
    monkeypatch.setattr(module, "__getattr__", hook, raising=False)
    monkeypatch.setitem(sys.modules, module.__name__, module)
    with pytest.raises(ValueError, match="Unknown HITL response type"):
        deserialize_response_type(f"{module.__name__}:Missing")
    hook.assert_not_called()


@pytest.mark.parametrize(
    "descriptor",
    [
        "missing_hitl_module:SomeType",
        "os:system",
        "builtins:DoesNotExist",
        "list[int]",
        "builtins:eval('1')",
        "",
        None,
        False,
        [],
        {},
        {"_durable_response_type": True, "kind": "list", "args": ["builtins:int"]},
        {"_durable_response_type": 1.0, "kind": "list", "args": ["builtins:int"]},
        {"_durable_response_type": 2, "kind": "list", "args": ["builtins:int"]},
        {"_durable_response_type": 1, "kind": "list", "args": ["builtins:int"], "extra": None},
        {"_durable_response_type": 1, "kind": "custom", "args": ["builtins:int"]},
        {"_durable_response_type": 1, "kind": "list", "args": []},
        {"_durable_response_type": 1, "kind": "dict", "args": ["builtins:str"]},
        {"_durable_response_type": 1, "kind": "union", "args": ["builtins:int"]},
        {"_durable_response_type": 1, "kind": "list", "args": [{"kind": "ellipsis"}]},
        {"_durable_response_type": 1, "kind": "tuple", "args": [{"kind": "ellipsis"}, "builtins:int"]},
        {"_durable_response_type": 1, "kind": "list", "args": [{"__pickled__": "inert"}]},
    ],
)
def test_malformed_and_unknown_descriptors_fail_closed_without_imports(
    descriptor: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    forbidden = Mock(side_effect=AssertionError("No payload-selected imports"))
    monkeypatch.setattr(importlib, "import_module", forbidden)
    with pytest.raises(ValueError, match="HITL"):
        deserialize_response_type(descriptor)
    assert resolve_type(descriptor) is None
    forbidden.assert_not_called()


def test_descriptor_depth_and_cycles_fail_with_bounded_errors() -> None:
    descriptor: Any = "builtins:int"
    for _ in range(70):
        descriptor = {"_durable_response_type": 1, "kind": "list", "args": [descriptor]}
    with pytest.raises(ValueError, match="complexity"):
        deserialize_response_type(descriptor)
    cyclic: dict[str, Any] = {"_durable_response_type": 1, "kind": "list"}
    cyclic["args"] = [cyclic]
    with pytest.raises(ValueError, match="complexity"):
        deserialize_response_type(cyclic)


@pytest.mark.parametrize("annotation", [Literal["approve"], list[Literal["approve"]]])
def test_unsupported_literal_annotation_is_not_silently_widened(annotation: Any) -> None:
    sample = "approve" if annotation == Literal["approve"] else ["approve"]
    # Core's field-level coercer knows Literal but public HITL admission does not.
    with pytest.raises(TypeError):
        is_instance_of(sample, annotation)
    with pytest.raises(ValueError, match="Unsupported HITL response annotation"):
        serialize_response_type(annotation)


def test_missing_legacy_annotation_differs_from_explicit_none_type() -> None:
    event: WorkflowEvent = WorkflowEvent.request_info("approval", "gate", "decision", object)
    legacy = serialize_workflow_event(event)
    legacy.pop("response_type")
    assert deserialize_workflow_event(legacy).response_type is object
    legacy["response_type"] = None
    assert deserialize_workflow_event(legacy).response_type is object
    legacy["response_type"] = "builtins:NoneType"
    assert deserialize_workflow_event(legacy).response_type is type(None)
    invalid_descriptors: list[Any] = ["", {}, []]
    for invalid in invalid_descriptors:
        legacy["response_type"] = invalid
        with pytest.raises(ValueError, match="HITL"):
            deserialize_workflow_event(legacy)


def test_legacy_concrete_records_are_not_reinterpreted_as_generics() -> None:
    assert serialize_response_type(list) == "builtins:list"
    assert resolve_type("builtins:list") is list
    assert engine._deserialize_hitl_response(["bad"], "builtins:list") == ["bad"]
    with pytest.raises(TypeError, match="HITL response type mismatch"):
        engine._deserialize_hitl_response(["bad"], serialize_response_type(list[int]))


def test_raw_generic_reconstruction_does_not_decode_external_checkpoint_markers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    forbidden = Mock(side_effect=AssertionError("No external pickle decode"))
    monkeypatch.setattr(pickle, "loads", forbidden)
    annotation = list[int | None]
    value = [{"__pickled__": "inert", "__type__": "builtins:int"}]
    assert reconstruct_to_type(value, annotation, encoded=False) == [None]
    assert try_coerce_to_type(["1"], list[int]) == ["1"]
    forbidden.assert_not_called()


@pytest.mark.parametrize("descriptor", ["", {}, [], False, "unloaded_reply_types:Arbitrary"])
def test_admission_cannot_treat_a_malformed_annotation_as_absent(descriptor: Any) -> None:
    with pytest.raises(ValueError, match="HITL"):
        engine._deserialize_hitl_response(["bad"], descriptor)


def test_response_handler_distinguishes_missing_payload_from_explicit_null() -> None:
    workflow, seen = _generic_workflow(Optional[list[int]])
    message: dict[str, Any] = {
        "request_id": "approval",
        "original_request": "decision",
        "response_type": serialize_response_type(Optional[list[int]]),
    }
    envelope = {"message": message, "is_hitl_response": True}
    with pytest.raises(ValueError, match="HITL response payload is required"):
        execute_workflow_activity(workflow.executors["gate"], json.dumps(envelope), workflow)
    assert seen == []
    message["response"] = None
    result = json.loads(execute_workflow_activity(workflow.executors["gate"], json.dumps(envelope), workflow))
    assert seen == [None] and result["outputs"] == [{"value": None}]


def test_unpublished_event_does_not_consume_a_different_pending_request() -> None:
    workflow, seen = _generic_workflow(list[int])
    transport = _Transport()
    native = Mock(spec=TaskHubGrpcClient)
    public = DurableWorkflowClient(native)
    try:
        run = transport.start(workflow)
        native.get_orchestration_state.side_effect = transport.state
        public.send_hitl_response("root-run", "future-fixed-id", [1])
        native.raise_orchestration_event.assert_called_once_with("root-run", event_name="future-fixed-id", data=[1])
        assert not run.done and seen == [] and len(run.calls) == 1
        assert public.get_pending_hitl_requests("root-run")[0]["request_id"] == "approval"
    finally:
        transport.close()


@pytest.mark.parametrize("descriptor", ["", {}, [], False, "unloaded_reply_types:Arbitrary"])
def test_invalid_producer_annotation_fails_before_creating_an_unanswerable_request(descriptor: Any) -> None:
    pending: dict[str, engine.PendingHITLRequest] = {}
    result = engine.ExecutorResult(
        executor_id="gate",
        output_message=None,
        activity_result={"pending_request_info_events": [{"request_id": "approval", "response_type": descriptor}]},
        task_type=engine.TaskType.ACTIVITY,
    )
    with pytest.raises(ValueError, match="HITL"):
        engine._collect_hitl_requests(result, pending)
    assert pending == {}


@pytest.mark.parametrize("descriptor", ["", {}, "unloaded_reply_types:Arbitrary"])
def test_registered_orchestrator_rejects_broken_producer_metadata_before_waiting(
    descriptor: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Corrupt only the stored pending descriptor. The executor, captured events,
    # activity body and orchestrator are real, not replacement routing logic.
    monkeypatch.setattr(activity_module, "serialize_response_type", lambda annotation: descriptor)
    workflow, seen = _generic_workflow(list[int])
    transport = _Transport()
    try:
        with pytest.raises(ValueError, match="HITL"):
            transport.start(workflow)
        host = transport.hosts["root-run"]
        host.wait_for_external_event.assert_not_called()
        assert all(not status.get("pending_requests") for status in host.statuses)
        assert seen == []
    finally:
        transport.close()


def test_mismatch_diagnostic_includes_resolved_generic_arguments() -> None:
    descriptor = serialize_response_type(dict[str, list[int]])
    with pytest.raises(TypeError) as failure:
        engine._deserialize_hitl_response({"items": ["bad"]}, descriptor)
    assert str(failure.value) == "HITL response type mismatch: expected dict[str, list[int]], got dict."


@pytest.mark.parametrize("include_descriptor", [False, True])
def test_legacy_absent_annotation_remains_untyped_without_widening_none_type(include_descriptor: bool) -> None:
    request: dict[str, Any] = {"request_id": "approval", "data": "decision"}
    if include_descriptor:
        request["response_type"] = None
    pending: dict[str, engine.PendingHITLRequest] = {}
    result = engine.ExecutorResult(
        executor_id="gate",
        output_message=None,
        activity_result={"pending_request_info_events": [request]},
        task_type=engine.TaskType.ACTIVITY,
    )
    engine._collect_hitl_requests(result, pending)
    assert pending["approval"].response_type is None
    assert engine._deserialize_hitl_response(["legacy"], pending["approval"].response_type) == ["legacy"]
    assert engine._deserialize_hitl_response(None, "builtins:NoneType") is None
    with pytest.raises(TypeError, match="HITL response type mismatch"):
        engine._deserialize_hitl_response("not-null", "builtins:NoneType")


def test_loaded_namespace_alias_cannot_impersonate_the_recorded_type(monkeypatch: pytest.MonkeyPatch) -> None:
    alias = ModuleType("generic_hitl_alias_namespace")
    monkeypatch.setattr(alias, "Decision", _Models.Decision, raising=False)
    monkeypatch.setitem(sys.modules, alias.__name__, alias)
    with pytest.raises(ValueError, match="does not match the loaded type"):
        deserialize_response_type(f"{alias.__name__}:Decision")
    # A namespace that no longer holds the exact annotation must fail on write,
    # rather than emitting a descriptor resolving to a different class.
    original = _Models.Decision
    monkeypatch.setattr(_Models, "Decision", _DataclassDecision)
    with pytest.raises(ValueError, match="does not match the loaded type"):
        serialize_response_type(original)


@pytest.mark.parametrize("annotation", [Literal["approve"], list[Literal["approve"]]])
def test_real_activity_rejects_unsupported_annotation_before_returning_pending_status(annotation: Any) -> None:
    workflow, seen = _generic_workflow(annotation)
    with pytest.raises(ValueError, match="Unsupported HITL response annotation"):
        execute_workflow_activity(workflow.executors["gate"], json.dumps({"message": "go"}), workflow)
    assert seen == []


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize(("custom_status", "request_id", "accepted"), _PENDING_STATUS_CASES)
def test_sdk_leaf_delivery_does_not_require_a_published_pending_record(
    custom_status: Any, request_id: str, accepted: bool, nested: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    statuses = {"child" if nested else "root": custom_status}
    if nested:
        statuses["root"] = {"subworkflows": {"sub": {"0": "child"}}}
    native = Mock(spec=TaskHubGrpcClient)
    native.get_orchestration_state.side_effect = lambda instance: SimpleNamespace(
        serialized_custom_status=json.dumps(statuses[instance])
    )
    public = DurableWorkflowClient(native)
    qualified_id = f"sub~0~{request_id}" if nested else request_id
    forbidden = Mock(side_effect=AssertionError("Client routing must not load descriptor-selected modules"))
    monkeypatch.setattr(importlib, "import_module", forbidden)
    payload = {"response_type": "builtins:object", "request_id": qualified_id, "response": None}
    before = deepcopy(statuses)
    public.send_hitl_response("root", qualified_id, payload)
    native.raise_orchestration_event.assert_called_once_with(
        "child" if nested else "root", event_name=request_id, data=payload
    )
    # The old membership expectation still describes discovery, not admission.
    published = public.get_pending_hitl_requests("root")
    assert (qualified_id in {item["request_id"] for item in published}) is accepted
    assert statuses == before
    forbidden.assert_not_called()


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize(
    "runtime_status",
    [OrchestrationStatus.COMPLETED, OrchestrationStatus.FAILED, OrchestrationStatus.TERMINATED],
)
def test_sdk_real_terminal_state_overrides_stale_pending_status(runtime_status: Any, nested: bool) -> None:
    stale = {"state": "waiting_for_human_input", "pending_requests": {"approval": {}}}
    states = {"root": SimpleNamespace(runtime_status=runtime_status, serialized_custom_status=json.dumps(stale))}
    if nested:
        states["child"] = states["root"]
        states["root"] = SimpleNamespace(
            runtime_status=OrchestrationStatus.RUNNING,
            serialized_custom_status=json.dumps({"subworkflows": {"sub": {"0": "child"}}}),
        )
    native = Mock(spec=TaskHubGrpcClient)
    native.get_orchestration_state.side_effect = states.get
    public = DurableWorkflowClient(native)
    assert public.get_pending_hitl_requests("root") == []
    with pytest.raises(ValueError, match="terminal runtime status"):
        public.send_hitl_response("root", "sub~0~approval" if nested else "approval", [7])
    native.raise_orchestration_event.assert_not_called()


@pytest.mark.parametrize("runtime_status", [None, Mock(), OrchestrationStatus.RUNNING, OrchestrationStatus.PENDING])
def test_sdk_unknown_or_nonterminal_runtime_state_preserves_buffering(runtime_status: Any) -> None:
    native = Mock(spec=TaskHubGrpcClient)
    native.get_orchestration_state.return_value = SimpleNamespace(
        runtime_status=runtime_status, serialized_custom_status=None
    )
    DurableWorkflowClient(native).send_hitl_response("root", "approval", [7])
    native.raise_orchestration_event.assert_called_once_with("root", event_name="approval", data=[7])


@pytest.mark.parametrize("request_id", ["missing~0~approval", "sub~1~approval"])
def test_early_reply_cannot_supply_its_own_child_address(request_id: str) -> None:
    native = Mock(spec=TaskHubGrpcClient)
    native.get_orchestration_state.return_value = SimpleNamespace(
        runtime_status=OrchestrationStatus.RUNNING,
        serialized_custom_status=json.dumps({"subworkflows": {"sub": {"0": "trusted-child"}}}),
    )
    with pytest.raises(ValueError, match="No active sub-workflow"):
        DurableWorkflowClient(native).send_hitl_response(
            "root", request_id, {"subworkflows": {"sub": {"1": "attacker-child"}}, "response": [7]}
        )
    native.raise_orchestration_event.assert_not_called()


@pytest.mark.parametrize(("requested", "answer"), [(float, 7), (float, True), (int, 7.0), (int, "7")])
def test_concrete_numeric_coercion_matches_installed_core(requested: Any, answer: Any) -> None:
    accepted, expected = asyncio.run(_core_trial(requested, answer))
    workflow, seen = _generic_workflow(requested)
    transport = _Transport()
    try:
        run = transport.start(workflow)
        transport.event("approval", answer)
        assert seen == expected and run.done is accepted
        if accepted:
            assert type(seen[0]) is type(expected[0])
        else:
            assert set(run.host.statuses[-1]["pending_requests"]) == {"approval"}
    finally:
        transport.close()


def test_numeric_overflow_is_a_sanitized_rejection_then_corrected() -> None:
    workflow, seen = _generic_workflow(float)
    transport = _Transport()
    try:
        run = transport.start(workflow)
        transport.event("approval", 10**1000)
        assert not run.done and not seen and len(run.calls) == 2
        assert set(run.host.statuses[-1]["pending_requests"]) == {"approval"}
        transport.event("approval", 7)
        assert run.done and seen == [7.0] and type(seen[0]) is float
    finally:
        transport.close()


@pytest.mark.parametrize(("requested", "answer", "correction"), _CONCRETE_REPLAY_CASES)
def test_concrete_core_admission_is_checkpointed_before_pending_request_is_retired(
    requested: type, answer: Any, correction: Any
) -> None:
    _concrete_replay_trial(requested, answer, correction)


@pytest.mark.parametrize("annotation", [_CountedDecision, _CountedDataclass, list[_CountedDecision] | str])
def test_validator_runs_in_registered_activity_not_cold_sdk_replay(annotation: Any) -> None:
    _validator_replay_trial(annotation)


def test_real_sdk_buffers_fixed_id_before_request_activity_completes() -> None:
    from _workflow_replay_test_support import _Episodes

    workflow, seen = _generic_workflow(list[int])
    episodes = _Episodes(workflow)
    episodes.client.start_workflow("go", instance_id="root")
    assert episodes.pending() == set()
    episodes.reply("approval", [7])
    assert not seen
    _complete_generic_activity(episodes)
    assert len(episodes.actions["root"]) == 1 and not seen
    result = _complete_generic_activity(episodes)
    assert result["hitl_admission"]["status"] == "accepted"
    assert seen == [[7]] and "root" in episodes.completions


@pytest.mark.parametrize(
    "error", [ValueError("handler failure"), TypeError("handler failure"), OverflowError("handler failure")]
)
def test_handler_errors_are_activity_failures_not_invalidreply(error: Exception) -> None:
    class Broken(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            await ctx.request_info("decision", response_type=int, request_id="approval")

        @response_handler(request=str, response=int)
        async def answer(self, original_request: str, response: int, ctx: WorkflowContext) -> None:
            raise error

    executor = Broken(id="gate")
    message = {"request_id": "approval", "original_request": "decision", "response": 7, "response_type": "builtins:int"}
    with pytest.raises(type(error), match="handler failure") as failure:
        execute_workflow_activity(executor, json.dumps({"message": message, "is_hitl_response": True}))
    assert failure.value is error


def test_rejected_activity_returns_only_safe_admission_metadata() -> None:
    workflow, seen = _generic_workflow(_Models.Decision)
    secret = "PRIVATE_HITL_VALIDATION_SENTINEL"
    message = {
        "request_id": "approval",
        "original_request": "decision",
        "response_type": serialize_response_type(_Models.Decision),
        "response": {"count": secret, "verdict": "approve"},
    }
    result = execute_workflow_activity(
        workflow.executors["gate"], json.dumps({"message": message, "is_hitl_response": True}), workflow
    )
    assert secret not in result and "__pickled__" not in result
    assert json.loads(result) == {
        "hitl_admission": {"request_id": "approval", "status": "invalidreply"},
        "sent_messages": [],
        "outputs": [],
        "events": [],
        "shared_state_updates": {},
        "shared_state_deletes": [],
        "pending_request_info_events": [],
    }
    assert seen == []


def test_invalid_generic_checkpoint_then_corrected_reply_on_cold_sdk() -> None:
    _invalid_generic_replay_trial()


def test_rejected_responses_do_not_spend_handler_convergence_budget() -> None:
    # Required scheduler integration, deliberately not xfailed. Two executor
    # steps suffice for the request and accepted handler, regardless of retries.
    workflow, seen = _generic_workflow(list[int])
    workflow.max_iterations = 2
    transport = _Transport()
    try:
        run = transport.start(workflow)
        for _ in range(4):
            transport.event("approval", ["bad"])
            assert not run.done and not seen
            assert set(run.host.statuses[-1]["pending_requests"]) == {"approval"}
        transport.event("approval", [7])
        assert run.done and seen == [[7]]
    finally:
        transport.close()


@pytest.mark.parametrize("output_failure", [False, True])
def test_handler_and_output_errors_become_sdk_terminal_failures(output_failure: bool) -> None:
    _handler_failure_trial(functions_host=False, output_failure=output_failure)


def test_invalid_reply_preserves_sibling_wait_and_ready_sibling_delivery() -> None:
    from _workflow_lifecycle_test_support import _siblings

    workflow, seen = _siblings()
    transport = _Transport()
    try:
        run = transport.start(workflow)
        sibling_wait = transport.events["root-run"]["qB"][0]
        transport.event("qA", 7)  # qA requested str, not int.
        assert not seen and not run.done
        assert transport.events["root-run"]["qB"] == [sibling_wait]
        assert set(run.host.statuses[-1]["pending_requests"]) == {"qA", "qB"}
        # Complete the retained SDK task itself, not a reconstructed wait.
        sibling_wait.complete("sibling")
        transport.pump()
        assert seen == [("qB", "sibling")]
        assert set(run.host.statuses[-1]["pending_requests"]) == {"qA"}
        transport.event("qA", "corrected")
        transport.event("qC", "follow-up")
        assert run.done and seen == [("qB", "sibling"), ("qA", "corrected"), ("qC", "follow-up")]
    finally:
        transport.close()
