# Copyright (c) Microsoft. All rights reserved.

"""Generated Functions starts use framework JSON and actual SDK parent metadata.

The app's registered orchestrators replay constructed histories with the app's own
converter. Service metadata follows returned child actions, never application
envelope keys. These offline histories do not establish live Functions-host persistence.
"""

import json
from copy import deepcopy
from typing import Any

import pytest
from _workflow_provenance_test_support import (
    _INVALID_CHILD_CASES,
    _PICKLE_CALLS,
    _internal,
    _invalid_child,
    _leaf,
    _only_action,
    _PickleProbe,
    _tree,
)
from _workflow_provenance_test_support_af import _af_starts
from agent_framework_durabletask import wrap_workflow_input
from agent_framework_durabletask._workflows.naming import subworkflow_instance_id
from agent_framework_durabletask._workflows.serialization import (
    SUBWORKFLOW_ADDRESS_KEY,
    SUBWORKFLOW_INPUT_KEY,
    serialize_value,
)
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb

_DECODER_CALLS: list[Any] = []
# Not in sys.modules. The Functions object hook fails on it rather than importing it.
_UNKNOWN_MODULE = "_workflow_start_unloaded_sentinel"


class _StartDecoderProbe:
    @classmethod
    def from_json(cls, value: Any) -> Any:
        _DECODER_CALLS.append(value)
        return {"constructed": value}


def _metadata(module: str = __name__) -> dict[str, Any]:
    return {"__class__": "_StartDecoderProbe", "__module__": module, "__data__": {"value": 7}}


@pytest.fixture(autouse=True)
def _reset_probes() -> None:
    _PICKLE_CALLS.clear()
    _DECODER_CALLS.clear()


def _failed(result: Any, message: str) -> None:
    failure = _only_action(result, "completeOrchestration").completeOrchestration
    assert failure.orchestrationStatus == pb.ORCHESTRATION_STATUS_FAILED
    # durabletask reports builtins by qualified name in newer releases.
    assert failure.failureDetails.errorType.rsplit(".", 1)[-1] == "ValueError"
    assert message in failure.failureDetails.errorMessage
    assert not result.encoded_custom_status


@pytest.mark.parametrize("parent", [None, "", " \t"])
@pytest.mark.parametrize("marker", ["both", "input", "address"])
def test_registered_af_root_rejects_before_pickle_or_sdk_custom_decoding(parent: str | None, marker: str) -> None:
    workflow, echo = _leaf()
    host = _af_starts(workflow)
    value = _internal(serialize_value(_PickleProbe("rejected")))
    if marker == "input":
        del value[SUBWORKFLOW_ADDRESS_KEY]
    elif marker == "address":
        del value[SUBWORKFLOW_INPUT_KEY]
    value["sdk_constructor"] = _metadata()
    value["parent_instance_id"] = "root"
    before = deepcopy(value)
    result = host.start(
        "dafx-provenance-leaf", subworkflow_instance_id("root", "child", 0), wrap_workflow_input(value), parent
    )
    _failed(result, "requires SDK parent instance metadata")
    assert _DECODER_CALLS == [] and _PICKLE_CALLS == [] and echo.seen == []
    assert value == before


@pytest.mark.parametrize("case", _INVALID_CHILD_CASES)
def test_registered_af_child_rejects_inconsistent_address_before_decoding(case: str) -> None:
    workflow, echo = _leaf()
    host = _af_starts(workflow)
    value = _invalid_child(case)
    value["sdk_constructor"] = _metadata()
    result = host.start(
        "dafx-provenance-leaf", subworkflow_instance_id("root", "child", 0), wrap_workflow_input(value), "root"
    )
    _failed(result, "workflow child")
    assert _DECODER_CALLS == [] and _PICKLE_CALLS == [] and echo.seen == []


@pytest.mark.parametrize(
    ("parent", "instance"),
    [
        ("unrelated", subworkflow_instance_id("root", "child", 0)),
        ("root", subworkflow_instance_id("root", "other", 0)),
        ("root", subworkflow_instance_id(subworkflow_instance_id("root", "child", 0), "grand", 0)),
    ],
)
def test_registered_af_metadata_must_match_immediate_parent_and_current_child(parent: str, instance: str) -> None:
    workflow, echo = _leaf()
    host = _af_starts(workflow)
    value = _internal(serialize_value(_PickleProbe("rejected")))
    value["sdk_constructor"] = _metadata()
    result = host.start("dafx-provenance-leaf", instance, wrap_workflow_input(value), parent)
    _failed(result, "does not match SDK instance metadata")
    assert _DECODER_CALLS == [] and _PICKLE_CALLS == [] and echo.seen == []


@pytest.mark.parametrize("parent", [None, "native-parent"])
@pytest.mark.parametrize("module", [__name__, _UNKNOWN_MODULE])
def test_registered_af_plain_json_keeps_nested_markers_and_sdk_metadata_as_data(
    parent: str | None, module: str
) -> None:
    workflow, echo = _leaf()
    host = _af_starts(workflow)
    value = {"business": [_internal({"ordinary": [False, None, "世界"]})], "sdk": _metadata(module)}
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
    assert echo.seen == [value] and _DECODER_CALLS == [] and _PICKLE_CALLS == []


def test_registered_af_child_and_grandchild_preserve_typed_checkpoint_values() -> None:
    workflow, echo = _tree()
    host = _af_starts(workflow)
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
    assert len(echo.seen) == 1 and _DECODER_CALLS == []


def test_core_sdk_decoder_remains_active_for_unrelated_native_registration() -> None:
    workflow, _ = _leaf()
    host = _af_starts(workflow)
    value = {"ordinary": _metadata(), SUBWORKFLOW_INPUT_KEY: {"business": 1}}
    result = host.start("native-input", "native-id", value)
    terminal = _only_action(result, "completeOrchestration").completeOrchestration
    assert terminal.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
    assert json.loads(terminal.result.value) == {
        "ordinary": {"constructed": {"value": 7}},
        SUBWORKFLOW_INPUT_KEY: {"business": 1},
    }
    assert _DECODER_CALLS == [{"value": 7}] and _PICKLE_CALLS == []


def test_registered_native_af_parent_can_call_generated_workflow_with_plain_application_json() -> None:
    workflow, echo = _leaf()
    host = _af_starts(workflow)
    value = {"business": [False, None, "世界"], "parent_instance_id": "just data"}
    parent = host.start("native-parent", "native-root", value)
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
    assert json.loads(completed.result.value) == [value] and echo.seen == [value]
    assert _PICKLE_CALLS == [] and _DECODER_CALLS == []
