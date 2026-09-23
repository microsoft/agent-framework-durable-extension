# Copyright (c) Microsoft. All rights reserved.

"""Generated starts with actual SDK parent history, not input-derived parentage.

Histories are constructed from registered SDK actions, not live service captures.
The pickle probe only increments a local counter and rebuilds a benign value.
No production decoder, provenance check or orchestration body is replaced.
"""

import json
from copy import deepcopy
from typing import Any
from unittest.mock import Mock

import pytest
from _workflow_provenance_test_support import (
    _INVALID_CHILD_CASES,
    _PICKLE_CALLS,
    _DTStarts,
    _internal,
    _invalid_child,
    _leaf,
    _only_action,
    _PickleProbe,
    _tree,
)
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb

from agent_framework_durabletask import wrap_workflow_input
from agent_framework_durabletask._workflows.naming import subworkflow_instance_id
from agent_framework_durabletask._workflows.protocol import validate_workflow_start_input
from agent_framework_durabletask._workflows.serialization import (
    SUBWORKFLOW_ADDRESS_KEY,
    SUBWORKFLOW_INPUT_KEY,
    deserialize_value,
    serialize_value,
)


@pytest.fixture(autouse=True)
def _reset_probe() -> None:
    _PICKLE_CALLS.clear()


def test_probe_detects_actual_checkpoint_construction_but_json_validation_does_not() -> None:
    encoded = serialize_value(_PickleProbe("control"))
    assert "__pickled__" in encoded and _PICKLE_CALLS == []
    validate_workflow_start_input(_internal(encoded))
    assert _PICKLE_CALLS == []
    decoded = deserialize_value(encoded)
    assert type(decoded) is _PickleProbe and decoded.value == "control"
    assert _PICKLE_CALLS == ["control"]


@pytest.mark.parametrize(
    "parent",
    [None, "", " \t", False, 1, {}, Mock(), Mock(spec=str)],
    ids=["none", "empty", "blank", "bool", "number", "mapping", "mock", "string-spec-mock"],
)
def test_provenance_requires_real_nonblank_parent_string(parent: Any) -> None:
    # Exercise the provenance boundary directly with an otherwise valid child ID.
    from agent_framework_durabletask._workflows.protocol import validate_workflow_start_provenance

    value = _internal(serialize_value(_PickleProbe("rejected")))
    with pytest.raises(ValueError, match="requires SDK parent instance metadata"):
        validate_workflow_start_provenance(
            value, instance_id=subworkflow_instance_id("root", "child", 0), parent_instance_id=parent
        )
    assert _PICKLE_CALLS == []


@pytest.mark.parametrize("parent", [None, "", " \t"])
@pytest.mark.parametrize("marker", ["both", "input", "address"])
def test_registered_dt_root_rejects_internal_markers_before_decoding(parent: str | None, marker: str) -> None:
    workflow, echo = _leaf()
    host = _DTStarts(workflow)
    value = _internal(serialize_value(_PickleProbe("rejected")))
    if marker == "input":
        del value[SUBWORKFLOW_ADDRESS_KEY]
    elif marker == "address":
        del value[SUBWORKFLOW_INPUT_KEY]
    # Application-supplied metadata cannot substitute for the service field.
    value["parent_instance_id"] = "root"
    before = deepcopy(value)
    result = host.start(
        "dafx-provenance-leaf", subworkflow_instance_id("root", "child", 0), wrap_workflow_input(value), parent
    )
    failure = _only_action(result, "completeOrchestration").completeOrchestration
    assert failure.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert failure.failureDetails.errorType == "ValueError"
    assert "requires SDK parent instance metadata" in failure.failureDetails.errorMessage
    assert not result.encoded_custom_status and echo.seen == [] and _PICKLE_CALLS == []
    assert value == before


@pytest.mark.parametrize("case", _INVALID_CHILD_CASES)
def test_registered_dt_child_rejects_inconsistent_address_before_decoding(case: str) -> None:
    workflow, echo = _leaf()
    host = _DTStarts(workflow)
    result = host.start(
        "dafx-provenance-leaf",
        subworkflow_instance_id("root", "child", 0),
        wrap_workflow_input(_invalid_child(case)),
        "root",
    )
    failure = _only_action(result, "completeOrchestration").completeOrchestration
    assert failure.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert failure.failureDetails.errorType == "ValueError"
    assert "workflow child" in failure.failureDetails.errorMessage
    assert not result.encoded_custom_status and echo.seen == [] and _PICKLE_CALLS == []


@pytest.mark.parametrize(
    ("parent", "instance"),
    [
        ("unrelated", subworkflow_instance_id("root", "child", 0)),
        ("root", subworkflow_instance_id("root", "other", 0)),
        ("root", subworkflow_instance_id(subworkflow_instance_id("root", "child", 0), "grand", 0)),
    ],
)
def test_registered_dt_metadata_must_match_immediate_parent_and_current_child(parent: str, instance: str) -> None:
    workflow, echo = _leaf()
    host = _DTStarts(workflow)
    value = _internal(serialize_value(_PickleProbe("rejected")))
    result = host.start("dafx-provenance-leaf", instance, wrap_workflow_input(value), parent)
    failure = _only_action(result, "completeOrchestration").completeOrchestration
    assert failure.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    assert failure.failureDetails.errorType == "ValueError"
    assert "does not match SDK instance metadata" in failure.failureDetails.errorMessage
    assert not result.encoded_custom_status and echo.seen == [] and _PICKLE_CALLS == []


@pytest.mark.parametrize("parent", [None, "native-parent"])
def test_registered_dt_plain_json_and_nested_markers_are_application_data(parent: str | None) -> None:
    workflow, echo = _leaf()
    host = _DTStarts(workflow)
    value = {"business": [_internal({"ordinary": [False, None, "世界"]})], "parent_instance_id": "application"}
    result = host.start("dafx-provenance-leaf", "arbitrary::native~id", wrap_workflow_input(value), parent)
    action = _only_action(result, "scheduleTask")
    payload = json.loads(json.loads(action.scheduleTask.input.value))
    assert payload["message"] == value
    assert payload["host_context"]["instance_id"] == "arbitrary::native~id"
    assert payload["host_context"]["request_path_prefix"] == ""
    finished = host.complete_activity("arbitrary::native~id", action)
    terminal = _only_action(finished, "completeOrchestration").completeOrchestration
    assert terminal.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.loads(terminal.result.value) == [value]
    assert echo.seen == [value] and _PICKLE_CALLS == []


def test_registered_dt_child_and_grandchild_use_real_parent_history_and_typed_values() -> None:
    workflow, echo = _tree()
    host = _DTStarts(workflow)
    root = "root::native~世界"
    initial = host.start("dafx-provenance-root", root, wrap_workflow_input({"value": "trusted"}))
    parent = host.complete_activity(root, _only_action(initial, "scheduleTask"))
    child_action = _only_action(parent, "createSubOrchestration")
    child_id, child = host.child(root, child_action)
    grand_action = _only_action(child, "createSubOrchestration")
    grand_id, grand = host.child(child_id, grand_action)
    assert child_id == subworkflow_instance_id(root, "sub:: 世界", 0)
    assert grand_id == subworkflow_instance_id(child_id, "grand hop", 0)
    for instance, expected_parent in ((child_id, root), (grand_id, child_id)):
        start = next(e.executionStarted for e in host.histories[instance] if e.HasField("executionStarted"))
        assert start.parentInstance.orchestrationInstance.instanceId == expected_parent
    leaf_action = _only_action(grand, "scheduleTask")
    payload = json.loads(json.loads(leaf_action.scheduleTask.input.value))
    assert payload["host_context"] == {
        "instance_id": root,
        "workflow_name": "provenance-root",
        "request_path_prefix": "sub:: 世界~0~grand hop~0~",
    }
    assert _PICKLE_CALLS and echo.seen == []
    completed = host.complete_activity(grand_id, leaf_action)
    terminal = _only_action(completed, "completeOrchestration").completeOrchestration
    assert terminal.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert len(echo.seen) == 1 and type(echo.seen[0]) is _PickleProbe and echo.seen[0].value == "trusted"
    for instance, action in ((child_id, grand_action), (root, child_action)):
        result = host.replay(instance, helpers.new_sub_orchestration_completed_event(action.id, terminal.result.value))
        terminal = _only_action(result, "completeOrchestration").completeOrchestration
        assert terminal.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    cold = host.replay(root)
    assert _only_action(cold, "completeOrchestration").completeOrchestration.result == terminal.result
    assert len(echo.seen) == 1


def test_registered_dt_native_orchestrator_keeps_its_unwrapped_input_contract() -> None:
    workflow, _ = _leaf()
    host = _DTStarts(workflow)

    def native(context: Any, value: Any) -> Any:
        return value

    host.worker.add_orchestrator(native)
    value = {"ordinary": True, SUBWORKFLOW_INPUT_KEY: {"business": 1}}
    result = host.start("native", "native-id", value)
    terminal = _only_action(result, "completeOrchestration").completeOrchestration
    assert terminal.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.loads(terminal.result.value) == value and _PICKLE_CALLS == []


def test_registered_native_dt_parent_can_call_generated_workflow_with_plain_application_json() -> None:
    workflow, echo = _leaf()
    host = _DTStarts(workflow)

    def native_parent(context: Any, value: Any) -> Any:
        result = yield context.call_sub_orchestrator(
            "dafx-provenance-leaf", input=wrap_workflow_input(value), instance_id="native-chosen-child"
        )
        return result  # noqa: B901

    host.worker.add_orchestrator(native_parent)
    value = {"business": [False, None, "世界"], "parent_instance_id": "just data"}
    parent = host.start("native_parent", "native-root", value)
    child_action = _only_action(parent, "createSubOrchestration")
    instance, child = host.child("native-root", child_action)
    child_result = host.complete_activity(instance, _only_action(child, "scheduleTask"))
    terminal = _only_action(child_result, "completeOrchestration").completeOrchestration
    assert terminal.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.loads(terminal.result.value) == [value]
    result = host.replay(
        "native-root", helpers.new_sub_orchestration_completed_event(child_action.id, terminal.result.value)
    )
    completed = _only_action(result, "completeOrchestration").completeOrchestration
    assert completed.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.loads(completed.result.value) == [value] and echo.seen == [value] and _PICKLE_CALLS == []
