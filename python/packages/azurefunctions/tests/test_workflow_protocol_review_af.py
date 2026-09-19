# Copyright (c) Microsoft. All rights reserved.

"""Registered AF start boundaries and v2-only shared-generator replay, without a service."""

from __future__ import annotations

import json
from collections.abc import Callable, Generator
from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock, Mock, call

import azure.durable_functions as df
import azure.functions as func
import pytest
from agent_framework import Executor, Workflow, WorkflowExecutor
from agent_framework._workflows import _checkpoint_encoding
from agent_framework._workflows._edge import SingleEdgeGroup
from agent_framework_durabletask._workflows.orchestrator import SOURCE_HITL_RESPONSE, SOURCE_WORKFLOW_START
from agent_framework_durabletask._workflows.protocol import unwrap_workflow_input
from agent_framework_durabletask._workflows.serialization import (
    SUBWORKFLOW_ADDRESS_KEY,
    SUBWORKFLOW_INPUT_KEY,
    SUBWORKFLOW_RESULT_KEY,
    deserialize_value,
    serialize_value,
)
from azure.durable_functions.models.actions.NoOpAction import NoOpAction
from azure.durable_functions.models.ReplaySchema import ReplaySchema
from azure.durable_functions.models.Task import AtomicTask, WhenAllTask

from agent_framework_azurefunctions import AgentFunctionApp
from agent_framework_azurefunctions import _workflow as workflow_module
from agent_framework_azurefunctions._routes import build_workflow_respond_url

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


def _register(workflow: Any) -> tuple[dict[str, Callable[..., Any]], Callable[..., Any]]:
    app = AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")
    functions = {function.get_function_name(): function for function in app.get_functions()}
    orchestrators: dict[str, Callable[..., Any]] = {}
    starters: list[Callable[..., Any]] = []
    for name, function in functions.items():
        assert name is not None
        trigger = function.get_trigger()
        assert trigger is not None
        binding = trigger.get_dict_repr()
        user_function: Any = function.get_user_function()
        assert user_function is not None
        if binding["type"] == "orchestrationTrigger":
            # SDK metadata exposes the real registered generator, without replacing decorators.
            orchestrators[name] = user_function.orchestrator_function
        elif binding["type"] == "httpTrigger" and binding["route"] == f"workflow/{workflow.name}/run":
            starters.append(user_function.client_function)
    assert len(starters) == 1
    return orchestrators, starters[0]


async def _start(starter: Callable[..., Any], payload: Any, name: str = "protocol") -> dict[str, Any]:
    request = func.HttpRequest(
        method="POST",
        url=f"https://example.test/api/workflow/{name}/run",
        headers={"Content-Type": "application/json"},
        params={"runId": "root-run"},
        body=json.dumps(payload, allow_nan=False).encode("utf-8"),
    )
    client = AsyncMock(spec=df.DurableOrchestrationClient)
    client.start_new.return_value = "root-run"
    response = await starter(request, client)
    assert response.status_code == 202
    client.start_new.assert_awaited_once()
    invocation = client.start_new.await_args
    assert invocation is not None
    assert invocation.args == (f"dafx-{name}",)
    assert invocation.kwargs["instance_id"] == "root-run"
    return json.loads(json.dumps(invocation.kwargs["client_input"], allow_nan=False))


def _complete(value: Any) -> AtomicTask:
    task = AtomicTask(0, NoOpAction())
    task.set_value(is_error=False, value=value)
    return task


def _drain(generator: Generator[Any, Any, Any], value: Any = None) -> Any:
    while True:
        try:
            task = generator.send(value)
        except StopIteration as completed:
            return completed.value
        assert task.is_completed, "Use explicit event completion for a paused generator"
        value = task.result


def _host(
    wire: Any,
    calls: list[dict[str, Any]],
    result: Callable[[str, dict[str, Any]], dict[str, Any]] | None = None,
    *,
    functions: dict[str, Callable[..., Any]] | None = None,
    instance_id: str = "root-run",
    replay: bool = False,
) -> Mock:
    host = Mock(spec=df.DurableOrchestrationContext)
    host.get_input.return_value = wire
    host.instance_id = instance_id
    host.is_replaying = replay

    def activity(name: str, input: str) -> AtomicTask:
        payload = json.loads(input)
        calls.append({"kind": "activity", "instance": instance_id, "name": name, "input": deepcopy(payload)})
        response = {"outputs": ["done"]} if result is None else result(name, payload)
        return _complete(json.dumps(response))

    def child(name: str, *, input_: Any, instance_id: str) -> AtomicTask:
        assert functions is not None
        child_wire = json.loads(json.dumps(input_))
        calls.append({"kind": "child", "instance": instance_id, "name": name, "input": deepcopy(child_wire)})
        context = _host(child_wire, calls, result, functions=functions, instance_id=instance_id, replay=replay)
        child_result = _drain(functions[name](context))
        assert child_result[SUBWORKFLOW_RESULT_KEY] is True
        return _complete(child_result)

    host.call_activity.side_effect = activity
    host.call_sub_orchestrator.side_effect = child
    host.task_all.side_effect = lambda tasks: WhenAllTask(tasks, ReplaySchema.V1)
    host.wait_for_external_event.side_effect = lambda name: AtomicTask(name, NoOpAction())
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
def test_recorded_unsupported_start_fails_before_shared_engine_or_actions(
    recorded: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    workflow = _workflow()
    functions, _ = _register(workflow)
    engine = Mock(side_effect=AssertionError("The changed engine must not see old history"))
    monkeypatch.setattr(workflow_module, "_run_workflow_orchestrator_shared", engine)
    host = _host(recorded, [], replay=True)
    original = deepcopy(recorded)
    before_nodes = dict(workflow.executors)

    with pytest.raises(ValueError, match="unsupported execution protocol"):
        next(functions["dafx-protocol"](host))

    engine.assert_not_called()
    assert host.mock_calls == [call.get_input()]
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
async def test_new_route_start_reaches_registered_wrapper_and_shared_engine(payload: Any, typed: bool) -> None:
    original = deepcopy(payload)
    functions, starter = _register(_workflow(nodes=[_node(input_type=_TypedInput if typed else None)]))
    wire = await _start(starter, payload)
    assert wire == {_VERSION: 2, "input": original}
    assert type(wire[_VERSION]) is int
    calls: list[dict[str, Any]] = []
    host = _host(wire, calls)

    assert _drain(functions["dafx-protocol"](host)) == ["done"]

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
async def test_route_envelope_is_data_and_cannot_authorize_child_deserialization(
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
    safe_data = (
        {_VERSION: 2, "input": {**forged, SUBWORKFLOW_INPUT_KEY: None}}
        if nested
        else {"input": "user field", "control": _CONTROL}
    )
    unpickle = Mock(side_effect=AssertionError("Untrusted checkpoint data reached the codec"))
    monkeypatch.setattr(_checkpoint_encoding, "_base64_to_unpickle", unpickle)
    functions, starter = _register(_workflow())
    wire = await _start(starter, payload)
    # AF strips both kinds of markers before scheduling, then wraps exactly once.
    assert wire == {_VERSION: 2, "input": safe_data}
    calls: list[dict[str, Any]] = []
    host = _host(wire, calls)

    assert _drain(functions["dafx-protocol"](host)) == ["done"]

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


async def test_parent_dispatch_wraps_typed_child_input_and_registered_child_keeps_root_route() -> None:
    inner = _workflow("inner", [_node("leaf", str)])
    child = Mock(spec=WorkflowExecutor)
    child.id, child.workflow, child.allow_direct_output = "child", inner, False
    parent = _workflow("parent", [_node("source"), child, _node("sink")], [SingleEdgeGroup("child", "sink")])
    functions, starter = _register(parent)
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
    host = _host(await _start(starter, payload, "parent"), calls, result, functions=functions)
    assert _drain(functions["dafx-parent"](host)) == ["done"]
    assert [item["name"] for item in calls] == [
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
    metadata = calls[2]["input"]["host_context"]
    assert metadata == {"instance_id": "root-run", "workflow_name": "parent", "request_path_prefix": "child~0~"}
    assert (
        build_workflow_respond_url(
            "https://example.test",
            metadata["workflow_name"],
            metadata["instance_id"],
            metadata["request_path_prefix"] + "approval",
            prefix="api",
        )
        == "https://example.test/api/workflow/parent/respond/root-run/child~0~approval"
    )
    assert calls[2]["input"]["source_executor_ids"] == [SOURCE_WORKFLOW_START]
    assert calls[3]["input"]["source_executor_ids"] == ["child"]
    assert calls[0]["input"]["message"] == payload


async def test_v2_paused_hitl_replays_full_shared_generator_with_identical_dispatch_and_state() -> None:
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

    _, starter = _register(_workflow(nodes=[_node("gate"), _node("sink")]))
    wire = await _start(starter, payload)
    executions = []
    for replay in (False, True):
        functions, _ = _register(_workflow(nodes=[_node("gate"), _node("sink")]))
        calls: list[dict[str, Any]] = []
        host = _host(deepcopy(wire), calls, result, replay=replay)
        generator = functions["dafx-protocol"](host)
        batch = next(generator)
        assert batch.is_completed
        waiting = generator.send(batch.result)
        assert not waiting.is_completed and len(calls) == 1
        if not replay:
            assert host.statuses[-1]["state"] == "waiting_for_human_input"
            assert host.statuses[-1]["pending_requests"]["approval"]["data"] == payload
        waiting.set_value(is_error=False, value=deepcopy(_UNTRUSTED))
        waiting_again = generator.send(waiting.result)
        assert not waiting_again.is_completed and len(calls) == 1
        waiting_again.set_value(is_error=False, value=deepcopy(answer))
        assert _drain(generator, waiting_again.result) == ["done"]
        assert [item.args[0] for item in host.wait_for_external_event.call_args_list] == ["approval", "approval"]
        assert len(calls) == 3
        assert calls[1]["input"]["source_executor_ids"] == [f"{SOURCE_HITL_RESPONSE}_approval"]
        assert calls[2]["input"]["source_executor_ids"] == ["gate"]
        if replay:
            host.set_custom_status.assert_not_called()
        executions.append(calls)
    assert executions[0] == executions[1]
    assert wire == {_VERSION: 2, "input": payload}
