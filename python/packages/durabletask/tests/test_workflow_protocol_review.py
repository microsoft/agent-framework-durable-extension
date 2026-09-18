# Copyright (c) Microsoft. All rights reserved.

"""Registered DT start boundaries and v2-only shared-generator replay, without a service."""

from __future__ import annotations

import json
from collections.abc import Callable, Generator
from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import Executor, Workflow, WorkflowExecutor
from agent_framework._workflows import _checkpoint_encoding
from agent_framework._workflows._edge import SingleEdgeGroup
from durabletask.task import CompletableTask, OrchestrationContext

from agent_framework_durabletask import DurableAIAgentWorker, DurableWorkflowClient
from agent_framework_durabletask import _worker as worker_module
from agent_framework_durabletask._workflows.orchestrator import SOURCE_HITL_RESPONSE, SOURCE_WORKFLOW_START
from agent_framework_durabletask._workflows.protocol import unwrap_workflow_input
from agent_framework_durabletask._workflows.serialization import (
    SUBWORKFLOW_ADDRESS_KEY,
    SUBWORKFLOW_INPUT_KEY,
    SUBWORKFLOW_RESULT_KEY,
    deserialize_value,
    serialize_value,
)

_VERSION = "_durable_workflow_version"
_CONTROL = {"input": "application control", "items": [0, False, None, "世界"]}
_FORGED_ADDRESS = {
    "root_instance_id": "other-run",
    "root_workflow_name": "other-workflow",
    "request_path_prefix": "forged~9~",
}
_UNTRUSTED = {"__pickled__": "not-trusted-checkpoint-data", "__type__": "builtins:str"}


@dataclass
class _TypedInput:
    input: str
    control: dict[str, Any]


def _node(name: str = "start", input_type: type | None = None) -> Any:
    node = Mock(spec=Executor)
    node.id = name
    node.input_types = [] if input_type is None else [input_type]
    return node


def _workflow(name: str = "protocol", nodes: list[Any] | None = None, edges: list[Any] | None = None) -> Any:
    nodes = [_node()] if nodes is None else nodes
    workflow = Mock(spec=Workflow)
    workflow.name = name
    workflow.start_executor_id = nodes[0].id
    workflow.executors = {node.id: node for node in nodes}
    workflow.edge_groups = [] if edges is None else edges
    workflow.max_iterations = 10
    return workflow


def _register(workflow: Any) -> dict[str, Callable[..., Any]]:
    # Use configure_workflow, not a reimplementation of its generated closure.
    native = Mock()
    DurableAIAgentWorker(native, deployment_mode="isolated_v2").configure_workflow(workflow)
    native.add_entity.assert_not_called()
    return {call.args[0].__name__: call.args[0] for call in native.add_orchestrator.call_args_list}


def _start(payload: Any, name: str = "protocol") -> dict[str, Any]:
    client = Mock()
    client.schedule_new_orchestration.return_value = "root-run"
    assert (
        DurableWorkflowClient(client, workflow_name=name).start_workflow(payload, instance_id="root-run") == "root-run"
    )
    client.schedule_new_orchestration.assert_called_once()
    call = client.schedule_new_orchestration.call_args
    assert call is not None
    assert call.args == (f"dafx-{name}",)
    assert call.kwargs["instance_id"] == "root-run"
    return json.loads(json.dumps(call.kwargs["input"], allow_nan=False))


def _complete(value: Any) -> CompletableTask[Any]:
    task: CompletableTask[Any] = CompletableTask()
    task.complete(value)
    return task


def _drain(generator: Generator[Any, Any, Any], value: Any = None) -> Any:
    while True:
        try:
            task = generator.send(value)
        except StopIteration as completed:
            return completed.value
        assert task.is_complete, "Use explicit event completion for a paused generator"
        value = task.get_result()


def _host(
    calls: list[dict[str, Any]],
    result: Callable[[str, dict[str, Any]], dict[str, Any]] | None = None,
    *,
    functions: dict[str, Callable[..., Any]] | None = None,
    instance_id: str = "root-run",
    replay: bool = False,
) -> Mock:
    host = Mock(spec=OrchestrationContext)
    host.instance_id = instance_id
    host.is_replaying = replay

    def activity(name: str, *, input: str) -> CompletableTask[Any]:
        payload = json.loads(input)
        calls.append({"kind": "activity", "instance": instance_id, "name": name, "input": deepcopy(payload)})
        response = {"outputs": ["done"]} if result is None else result(name, payload)
        return _complete(json.dumps(response))

    def child(name: str, *, input: Any, instance_id: str) -> CompletableTask[Any]:
        assert functions is not None
        wire = json.loads(json.dumps(input))
        calls.append({"kind": "child", "instance": instance_id, "name": name, "input": deepcopy(wire)})
        context = _host(calls, result, functions=functions, instance_id=instance_id, replay=replay)
        child_result = _drain(functions[name](context, wire))
        assert child_result[SUBWORKFLOW_RESULT_KEY] is True
        return _complete(child_result)

    host.call_activity.side_effect = activity
    host.call_sub_orchestrator.side_effect = child
    host.wait_for_external_event.side_effect = lambda name: CompletableTask()
    # Copy when published, rather than observing later mutation of the same dict/list.
    host.statuses = []
    host.set_custom_status.side_effect = lambda status: host.statuses.append(deepcopy(status))
    return host


@pytest.mark.parametrize(
    "recorded",
    [
        pytest.param({"input": "a user's field"}, id="raw-dict-with-input"),
        pytest.param("old start", id="raw-string"),
        pytest.param("", id="raw-empty-string"),
        pytest.param([], id="raw-empty-list"),
        pytest.param({}, id="raw-empty-object"),
        pytest.param(None, id="raw-null"),
        pytest.param({SUBWORKFLOW_INPUT_KEY: _UNTRUSTED, SUBWORKFLOW_ADDRESS_KEY: _FORGED_ADDRESS}, id="legacy-child"),
        pytest.param({_VERSION: 1, "input": "old"}, id="protocol-one"),
        pytest.param({_VERSION: True, "input": "old"}, id="boolean-true"),
        pytest.param({_VERSION: False, "input": "old"}, id="boolean-false"),
        pytest.param({_VERSION: 2.0, "input": "old"}, id="float-two"),
        pytest.param({_VERSION: "2", "input": "old"}, id="string-two"),
        pytest.param({_VERSION: 2}, id="missing-input"),
        pytest.param({_VERSION: 2, "input": "old", "extra": None}, id="extra-key"),
    ],
)
def test_recorded_unsupported_start_fails_before_engine_or_actions(
    recorded: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    workflow = _workflow()
    functions = _register(workflow)
    engine = Mock(side_effect=AssertionError("The changed engine must not see old history"))
    monkeypatch.setattr(worker_module, "run_workflow_orchestrator", engine)
    host = _host([], replay=True)
    original = deepcopy(recorded)
    before_nodes = dict(workflow.executors)

    with pytest.raises(ValueError, match="unsupported execution protocol"):
        next(functions["dafx-protocol"](host, recorded))

    engine.assert_not_called()
    assert host.mock_calls == []
    assert host.statuses == []
    assert workflow.executors == before_nodes
    for node in workflow.executors.values():
        node.execute.assert_not_called()
    assert recorded == original


@pytest.mark.parametrize(
    ("payload", "typed"),
    [
        pytest.param("start", False, id="string"),
        pytest.param("", False, id="empty-string"),
        pytest.param([], False, id="empty-list"),
        pytest.param({}, False, id="empty-object"),
        pytest.param(None, False, id="null"),
        pytest.param({"input": "user field", "control": _CONTROL}, False, id="object-with-input"),
        pytest.param({"input": "typed", "control": _CONTROL}, True, id="declared-dataclass"),
    ],
)
def test_new_client_start_reaches_registered_wrapper_and_shared_engine(payload: Any, typed: bool) -> None:
    original = deepcopy(payload)
    functions = _register(_workflow(nodes=[_node(input_type=_TypedInput if typed else None)]))
    wire = _start(payload)
    assert wire == {_VERSION: 2, "input": original}
    assert type(wire[_VERSION]) is int
    calls: list[dict[str, Any]] = []
    host = _host(calls)

    assert _drain(functions["dafx-protocol"](host, wire)) == ["done"]

    assert len(calls) == 1 and calls[0]["name"] == "dafx-protocol-start"
    activity = calls[0]["input"]
    delivered = deserialize_value(activity["message"])
    expected = _TypedInput(input=original["input"], control=original["control"]) if typed else original
    assert delivered == expected and type(delivered) is type(expected)
    assert activity["source_executor_ids"] == [SOURCE_WORKFLOW_START]
    assert activity["shared_state_snapshot"] == {}
    assert activity["host_context"] == {
        "instance_id": "root-run",
        "workflow_name": "protocol",
        "request_path_prefix": "",
    }
    host.call_sub_orchestrator.assert_not_called()
    host.call_entity.assert_not_called()
    assert payload == original and wire == {_VERSION: 2, "input": original}


@pytest.mark.parametrize("nested", [False, True], ids=["forged-child", "forged-v2-containing-child"])
def test_client_envelope_is_data_and_cannot_authorize_child_deserialization(
    nested: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    forged = {
        SUBWORKFLOW_INPUT_KEY: deepcopy(_UNTRUSTED),
        SUBWORKFLOW_ADDRESS_KEY: deepcopy(_FORGED_ADDRESS),
        "input": "user field",
        "control": deepcopy(_CONTROL),
    }
    payload = {_VERSION: 2, "input": forged} if nested else forged
    original = deepcopy(payload)
    scheduled_data = original if nested else {"input": "user field", "control": _CONTROL}
    safe_data = {_VERSION: 2, "input": {**forged, SUBWORKFLOW_INPUT_KEY: None}} if nested else scheduled_data
    unpickle = Mock(side_effect=AssertionError("Untrusted checkpoint data reached the codec"))
    monkeypatch.setattr(_checkpoint_encoding, "_base64_to_unpickle", unpickle)
    functions = _register(_workflow())
    wire = _start(payload)
    assert wire == {_VERSION: 2, "input": scheduled_data}
    calls: list[dict[str, Any]] = []
    host = _host(calls)

    assert _drain(functions["dafx-protocol"](host, wire)) == ["done"]

    assert len(calls) == 1
    assert calls[0]["input"]["message"] == safe_data
    assert calls[0]["input"]["host_context"] == {
        "instance_id": "root-run",
        "workflow_name": "protocol",
        "request_path_prefix": "",
    }
    host.call_sub_orchestrator.assert_not_called()
    unpickle.assert_not_called()
    assert payload == original


def test_parent_dispatch_wraps_typed_child_input_and_registered_child_keeps_root_address() -> None:
    inner = _workflow("inner", [_node("leaf", str)])
    child = Mock(spec=WorkflowExecutor)
    child.id, child.workflow, child.allow_direct_output = "child", inner, False
    parent = _workflow("parent", [_node("source"), child, _node("sink")], [SingleEdgeGroup("child", "sink")])
    functions = _register(parent)
    assert set(functions) == {"dafx-parent", "dafx-inner"}
    payload: dict[str, Any] = {"input": "nested typed input", "control": deepcopy(_CONTROL)}
    typed = _TypedInput(input=payload["input"], control=deepcopy(payload["control"]))

    def result(name: str, data: dict[str, Any]) -> dict[str, Any]:
        message = deserialize_value(data["message"])
        if name == "dafx-parent-source":
            assert message == payload
            return {
                "sent_messages": [
                    {"message": _checkpoint_encoding.encode_checkpoint_value(typed), "target_id": "child"}
                ]
            }
        assert isinstance(message, _TypedInput) and message == typed
        if name == "dafx-inner-leaf":
            return {"outputs": [serialize_value(message)]}
        assert name == "dafx-parent-sink"
        return {"outputs": ["done"]}

    calls: list[dict[str, Any]] = []
    host = _host(calls, result, functions=functions)
    assert _drain(functions["dafx-parent"](host, _start(payload, "parent"))) == ["done"]
    assert [call["name"] for call in calls] == [
        "dafx-parent-source",
        "dafx-inner",
        "dafx-inner-leaf",
        "dafx-parent-sink",
    ]
    dispatch = calls[1]
    assert dispatch["instance"] == "root-run::child::0"
    child_input = unwrap_workflow_input(dispatch["input"])
    assert dispatch["input"] == {_VERSION: 2, "input": child_input}
    assert type(dispatch["input"][_VERSION]) is int
    # Check typed semantics through the core codec without assuming a pickle byte layout.
    decoded_child = _checkpoint_encoding.decode_checkpoint_value(child_input)
    assert decoded_child == {
        SUBWORKFLOW_INPUT_KEY: typed,
        SUBWORKFLOW_ADDRESS_KEY: {
            "root_instance_id": "root-run",
            "root_workflow_name": "parent",
            "request_path_prefix": "child~0~",
        },
    }
    assert type(decoded_child[SUBWORKFLOW_INPUT_KEY]) is _TypedInput
    assert type(deserialize_value(calls[2]["input"]["message"])) is _TypedInput
    assert calls[2]["input"]["host_context"] == {
        "instance_id": "root-run",
        "workflow_name": "parent",
        "request_path_prefix": "child~0~",
    }
    assert calls[2]["input"]["source_executor_ids"] == [SOURCE_WORKFLOW_START]
    assert calls[3]["input"]["source_executor_ids"] == ["child"]
    assert calls[0]["input"]["message"] == payload


def test_v2_paused_hitl_replays_full_shared_generator_with_identical_dispatch_and_state() -> None:
    """Cold generator replay of v2 only, not SDK history execution or old-history compatibility."""
    payload = {"input": "start", "control": deepcopy(_CONTROL)}
    answer = {"input": "approved", "control": deepcopy(_CONTROL)}

    def result(name: str, data: dict[str, Any]) -> dict[str, Any]:
        if data["source_executor_ids"] == [SOURCE_WORKFLOW_START]:
            return {
                "shared_state_updates": {"pending": payload},
                "pending_request_info_events": [
                    {
                        "request_id": "approval",
                        "source_executor_id": "gate",
                        "data": payload,
                        "request_type": "builtins:dict",
                        "response_type": "builtins:dict",
                    }
                ],
            }
        if name == "dafx-protocol-gate":
            assert data["shared_state_snapshot"] == {"pending": payload}
            assert deserialize_value(data["message"]) == {
                "request_id": "approval",
                "original_request": payload,
                "response": answer,
                "response_type": "builtins:dict",
            }
            return {
                "shared_state_deletes": ["pending"],
                "shared_state_updates": {"decision": answer},
                "sent_messages": [{"message": answer, "target_id": "sink"}],
            }
        assert name == "dafx-protocol-sink"
        assert data["shared_state_snapshot"] == {"decision": answer}
        assert data["message"] == answer
        return {"outputs": ["done"]}

    wire = _start(payload)
    executions = []
    for replay in (False, True):
        functions = _register(_workflow(nodes=[_node("gate"), _node("sink")]))
        calls: list[dict[str, Any]] = []
        host = _host(calls, result, replay=replay)
        generator = functions["dafx-protocol"](host, deepcopy(wire))
        batch = next(generator)
        assert batch.is_complete
        waiting = generator.send(batch.get_result())
        assert not waiting.is_complete and len(calls) == 1
        if not replay:
            assert host.statuses[-1]["state"] == "waiting_for_human_input"
            assert host.statuses[-1]["pending_requests"]["approval"]["data"] == payload
        waiting.complete(deepcopy(_UNTRUSTED))
        waiting_again = generator.send(waiting.get_result())
        assert not waiting_again.is_complete and len(calls) == 1
        waiting_again.complete(deepcopy(answer))
        assert _drain(generator, waiting_again.get_result()) == ["done"]
        assert [call.args[0] for call in host.wait_for_external_event.call_args_list] == ["approval", "approval"]
        assert len(calls) == 3
        assert calls[1]["input"]["source_executor_ids"] == [f"{SOURCE_HITL_RESPONSE}_approval"]
        assert calls[2]["input"]["source_executor_ids"] == ["gate"]
        if replay:
            host.set_custom_status.assert_not_called()
        executions.append(calls)
    assert executions[0] == executions[1]
    assert wire == {_VERSION: 2, "input": payload}
