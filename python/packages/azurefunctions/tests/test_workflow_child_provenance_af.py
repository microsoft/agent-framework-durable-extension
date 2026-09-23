# Copyright (c) Microsoft. All rights reserved.

"""Generated Functions starts use raw JSON and actual SDK parent metadata.

Registered SDK wrappers construct fresh contexts and replay constructed histories.
Service metadata follows returned child actions, never application envelope keys.
These offline histories do not establish live Functions-host persistence.
"""

import importlib
import json
from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import pytest
from _workflow_provenance_test_support import (
    _INVALID_CHILD_CASES,
    _PICKLE_CALLS,
    _internal,
    _invalid_child,
    _leaf,
    _PickleProbe,
    _tree,
)
from _workflow_provenance_test_support_af import _actions, _AFStarts, _last_action
from agent_framework_durabletask import wrap_workflow_input
from agent_framework_durabletask._workflows.naming import subworkflow_instance_id
from agent_framework_durabletask._workflows.serialization import (
    SUBWORKFLOW_ADDRESS_KEY,
    SUBWORKFLOW_INPUT_KEY,
    serialize_value,
)
from azure.functions import _durable_functions as sdk_codec

_DECODER_CALLS: list[Any] = []
_UNKNOWN_MODULE = "_workflow_start_unloaded_sentinel"


class _StartDecoderProbe:
    @classmethod
    def from_json(cls, value: Any) -> Any:
        _DECODER_CALLS.append(value)
        return {"constructed": value}


def _metadata(module: str = __name__) -> dict[str, Any]:
    return {"__class__": "_StartDecoderProbe", "__module__": module, "__data__": {"value": 7}}


@pytest.fixture(autouse=True)
def _observe_construction(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    _PICKLE_CALLS.clear()
    _DECODER_CALLS.clear()
    attempts: list[str] = []
    original = importlib.import_module

    def observe(name: str, package: str | None = None) -> Any:
        if name in (__name__, _UNKNOWN_MODULE):
            attempts.append(name)
        if name == _UNKNOWN_MODULE:
            raise AssertionError("Start-selected module import before admission")
        return original(name, package)

    monkeypatch.setattr(importlib, "import_module", observe)
    monkeypatch.setattr(sdk_codec, "import_module", observe)
    return attempts


def _failure(error: pytest.ExceptionInfo[Exception]) -> dict[str, Any]:
    assert type(error.value.__cause__) is ValueError
    marker = "\n\n$OutOfProcData$:"
    assert marker in str(error.value)
    state = json.loads(str(error.value).split(marker, 1)[1])
    assert _actions(state["actions"]) == []
    assert not state.get("customStatus") and state.get("output") is None
    return state


@pytest.mark.parametrize("parent", [None, "", " \t"])
@pytest.mark.parametrize("marker", ["both", "input", "address"])
def test_registered_af_root_rejects_before_pickle_or_sdk_custom_decoding(
    parent: str | None, marker: str, _observe_construction: list[str]
) -> None:
    workflow, echo = _leaf()
    host = _AFStarts(workflow)
    value = _internal(serialize_value(_PickleProbe("rejected")))
    if marker == "input":
        del value[SUBWORKFLOW_ADDRESS_KEY]
    elif marker == "address":
        del value[SUBWORKFLOW_INPUT_KEY]
    value["sdk_constructor"] = _metadata()
    value["parent_instance_id"] = "root"
    before = deepcopy(value)
    with pytest.raises(Exception, match="requires SDK parent instance metadata") as error:
        host.start(
            "dafx-provenance-leaf", subworkflow_instance_id("root", "child", 0), wrap_workflow_input(value), parent
        )
    _failure(error)
    assert _DECODER_CALLS == [] and _observe_construction == [] and _PICKLE_CALLS == [] and echo.seen == []
    assert value == before


@pytest.mark.parametrize("case", _INVALID_CHILD_CASES)
def test_registered_af_child_rejects_inconsistent_address_before_decoding(
    case: str, _observe_construction: list[str]
) -> None:
    workflow, echo = _leaf()
    host = _AFStarts(workflow)
    value = _invalid_child(case)
    value["sdk_constructor"] = _metadata()
    with pytest.raises(Exception, match="workflow child") as error:
        host.start(
            "dafx-provenance-leaf", subworkflow_instance_id("root", "child", 0), wrap_workflow_input(value), "root"
        )
    _failure(error)
    assert _DECODER_CALLS == [] and _observe_construction == [] and _PICKLE_CALLS == [] and echo.seen == []


@pytest.mark.parametrize(
    ("parent", "instance"),
    [
        ("unrelated", subworkflow_instance_id("root", "child", 0)),
        ("root", subworkflow_instance_id("root", "other", 0)),
        ("root", subworkflow_instance_id(subworkflow_instance_id("root", "child", 0), "grand", 0)),
    ],
)
def test_registered_af_metadata_must_match_immediate_parent_and_current_child(
    parent: str, instance: str, _observe_construction: list[str]
) -> None:
    workflow, echo = _leaf()
    host = _AFStarts(workflow)
    value = _internal(serialize_value(_PickleProbe("rejected")))
    value["sdk_constructor"] = _metadata()
    with pytest.raises(Exception, match="does not match SDK instance metadata") as error:
        host.start("dafx-provenance-leaf", instance, wrap_workflow_input(value), parent)
    _failure(error)
    assert _DECODER_CALLS == [] and _observe_construction == [] and _PICKLE_CALLS == [] and echo.seen == []


@pytest.mark.parametrize("parent", [None, "native-parent"])
@pytest.mark.parametrize("module", [__name__, _UNKNOWN_MODULE])
def test_registered_af_plain_json_keeps_nested_markers_and_sdk_metadata_as_data(
    parent: str | None, module: str, _observe_construction: list[str]
) -> None:
    workflow, echo = _leaf()
    host = _AFStarts(workflow)
    value = {"business": [_internal({"ordinary": [False, None, "世界"]})], "sdk": _metadata(module)}
    state = host.start("dafx-provenance-leaf", "arbitrary::native~id", wrap_workflow_input(value), parent)
    action = _last_action(state, 0)
    payload = json.loads(json.loads(action["input"]))
    assert payload["message"] == value
    assert payload["host_context"]["instance_id"] == "arbitrary::native~id"
    assert payload["host_context"]["request_path_prefix"] == ""
    terminal = host.complete_activity("arbitrary::native~id", action)
    assert terminal["isDone"] and not terminal.get("error") and terminal["output"] == [value]
    assert echo.seen == [value] and _DECODER_CALLS == [] and _observe_construction == [] and _PICKLE_CALLS == []


def test_registered_af_child_and_grandchild_preserve_typed_checkpoint_values(
    _observe_construction: list[str],
) -> None:
    workflow, echo = _tree()
    host = _AFStarts(workflow)
    root = "root::native~世界"
    initial = host.start("dafx-provenance-root", root, wrap_workflow_input({"value": "trusted"}))
    parent = host.complete_activity(root, _last_action(initial, 0))
    child_id, child = host.child(root, _last_action(parent, 2), 1)
    grand_id, grand = host.child(child_id, _last_action(child, 2), 0)
    assert child_id == subworkflow_instance_id(root, "sub:: 世界", 0)
    assert grand_id == subworkflow_instance_id(child_id, "grand hop", 0)
    assert host.starts[child_id]["parentInstanceId"] == root
    assert host.starts[grand_id]["parentInstanceId"] == child_id
    action = _last_action(grand, 0)
    payload = json.loads(json.loads(action["input"]))
    assert payload["host_context"] == {
        "instance_id": root,
        "workflow_name": "provenance-root",
        "request_path_prefix": "sub:: 世界~0~grand hop~0~",
    }
    assert _PICKLE_CALLS and echo.seen == []
    terminal = host.complete_activity(grand_id, action)
    assert terminal["isDone"] and not terminal.get("error")
    assert len(echo.seen) == 1 and type(echo.seen[0]) is _PickleProbe and echo.seen[0].value == "trusted"
    terminal = host.complete_child(child_id, 0, terminal["output"])
    assert terminal["isDone"] and not terminal.get("error")
    terminal = host.complete_child(root, 1, terminal["output"])
    assert terminal["isDone"] and not terminal.get("error")
    assert host.replay(root) == terminal and len(echo.seen) == 1
    assert _DECODER_CALLS == [] and _observe_construction == []


def test_core_sdk_decoder_remains_active_for_unrelated_native_registration(
    _observe_construction: list[str],
) -> None:
    workflow, _ = _leaf()
    host = _AFStarts(workflow)
    value = {"ordinary": _metadata(), SUBWORKFLOW_INPUT_KEY: {"business": 1}}
    state = host.start("native-input", "native-id", value)
    assert state["isDone"] and state["output"] == {
        "ordinary": {"constructed": {"value": 7}},
        SUBWORKFLOW_INPUT_KEY: {"business": 1},
    }
    assert _DECODER_CALLS == [{"value": 7}] and _observe_construction == [__name__] and _PICKLE_CALLS == []


def test_registered_native_af_parent_can_call_generated_workflow_with_plain_application_json() -> None:
    workflow, echo = _leaf()
    host = _AFStarts(workflow)
    value = {"business": [False, None, "世界"], "parent_instance_id": "just data"}
    parent = host.start("native-parent", "native-root", value)
    instance, child = host.child("native-root", _last_action(parent, 2), 0)
    child_result = host.complete_activity(instance, _last_action(child, 0))
    assert child_result["isDone"] and not child_result.get("error") and child_result["output"] == [value]
    terminal = host.complete_child("native-root", 0, child_result["output"])
    assert terminal["isDone"] and not terminal.get("error") and terminal["output"] == [value]
    assert echo.seen == [value] and _PICKLE_CALLS == [] and _DECODER_CALLS == []


@pytest.mark.parametrize("raw", [{}, [], b"{}", 1, SimpleNamespace()])
def test_generated_start_reader_fails_closed_on_unknown_sdk_raw_representation(raw: Any) -> None:
    from agent_framework_azurefunctions._workflow_af_context import get_workflow_start_input

    context: Any = SimpleNamespace(_input=raw)
    with pytest.raises(RuntimeError, match="Unsupported Durable Functions workflow start input representation"):
        get_workflow_start_input(context)


def test_generated_start_reader_does_not_fall_back_to_get_input() -> None:
    from agent_framework_azurefunctions._workflow_af_context import get_workflow_start_input

    def forbidden() -> Any:
        raise AssertionError("SDK hook must not run")

    context: Any = SimpleNamespace(get_input=forbidden)
    with pytest.raises(RuntimeError, match="Unsupported Durable Functions workflow start input representation"):
        get_workflow_start_input(context)
