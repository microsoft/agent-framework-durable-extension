# Copyright (c) Microsoft. All rights reserved.

"""Registered AF start boundaries and v2-only shared-generator replay, without a service."""

from __future__ import annotations

import json
from copy import deepcopy
from typing import Any
from unittest.mock import AsyncMock, Mock

import azure.durable_functions as df
import azure.functions as func
import pytest
from _workflow_protocol_test_support_af import (
    _CONTROL,
    _FORGED_ADDRESS,
    _UNTRUSTED,
    _VERSION,
    _drain,
    _host,
    _node,
    _register,
    _start,
    _TypedInput,
    _workflow,
)
from agent_framework import WorkflowExecutor
from agent_framework._workflows import _checkpoint_encoding
from agent_framework._workflows._edge import SingleEdgeGroup
from agent_framework_durabletask._workflows.naming import subworkflow_instance_id
from agent_framework_durabletask._workflows.orchestrator import SOURCE_HITL_RESPONSE, SOURCE_WORKFLOW_START
from agent_framework_durabletask._workflows.protocol import unwrap_workflow_input
from agent_framework_durabletask._workflows.serialization import (
    SUBWORKFLOW_ADDRESS_KEY,
    SUBWORKFLOW_INPUT_KEY,
    SUBWORKFLOW_RESULT_KEY,
    deserialize_value,
    serialize_value,
)

from agent_framework_azurefunctions import _workflow as workflow_module
from agent_framework_azurefunctions._routes import build_workflow_respond_url


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
    host.get_input.assert_not_called()
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


async def test_new_route_rejects_invalid_json_before_scheduling() -> None:
    workflow = _workflow()
    _, starter = _register(workflow)
    request = Mock(spec=func.HttpRequest)
    request.headers = {}
    request.params = {"runId": "root-run"}
    request.get_json.return_value = {"value": float("nan")}
    client = AsyncMock(spec=df.DurableOrchestrationClient)

    response = await starter(request, client)

    assert response.status_code == 400
    assert "strict JSON with string keys and finite numbers" in response.get_body().decode("utf-8")
    client.start_new.assert_not_awaited()


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
    # AF validates the raw client payload first, then strips markers and wraps exactly once.
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


async def test_parent_dispatch_wraps_typed_child_input_and_registered_child_keeps_root_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pathlib import Path

    # Reuse the local SDK-history harness even when this test is selected alone.
    tests = Path(__file__).resolve().parent  # noqa: ASYNC240 - Synchronous test fixture import.
    monkeypatch.syspath_prepend(str(tests.parents[1] / "durabletask" / "tests"))
    monkeypatch.syspath_prepend(str(tests))
    import _workflow_provenance_test_support_af as sdk_history

    assert Path(sdk_history.__file__).resolve() == tests / "_workflow_provenance_test_support_af.py"  # noqa: ASYNC240
    inner = _workflow("inner", [_node("leaf", str)])
    child = Mock(spec=WorkflowExecutor)
    child.id, child.workflow, child.allow_direct_output = "child", inner, False
    parent = _workflow(
        "parent",
        [_node("source"), child, _node("sink")],
        [SingleEdgeGroup("source", "child"), SingleEdgeGroup("child", "sink")],
    )
    host = sdk_history._AFStarts(parent)
    functions = {
        name: function
        for name, function in host.functions.items()
        if hasattr(function, "orchestrator_function") and name not in ("native-input", "native-parent")
    }
    assert set(functions) == {"dafx-parent", "dafx-inner"}
    starter = host.functions["dafx-parent-start"].client_function
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

    def complete_activity(instance: str, state: dict[str, Any], task_id: int) -> dict[str, Any]:
        action = sdk_history._last_action(state, 0)
        name = action["functionName"]

        def activity(input_data: str) -> str:
            data = json.loads(input_data)
            calls.append({"kind": "activity", "instance": instance, "name": name, "input": deepcopy(data)})
            return json.dumps(result(name, data))

        # Only leaf business execution is substituted. The harness preserves the
        # SDK action input and both activity-result JSON layers in native history.
        host.functions[name] = activity
        return host.complete_activity(instance, action, task_id)

    state = host.start("dafx-parent", "root-run", await _start(starter, payload, "parent"))
    state = complete_activity("root-run", state, 0)
    action = sdk_history._last_action(state, 2)
    calls.append({
        "kind": "child",
        "instance": action["instanceId"],
        "name": action["functionName"],
        "input": json.loads(action["input"]),
    })
    # The parent SDK action supplies the child's name, instance ID and wire input.
    # Each replay invokes the registered SDK wrapper with a fresh native context.
    child_instance, child_state = host.child("root-run", action, 1)
    assert host.starts[child_instance]["parentInstanceId"] == "root-run"
    assert host.starts[child_instance]["history"][1]["Name"] == action["functionName"]
    assert json.loads(host.starts[child_instance]["input"]) == calls[1]["input"]
    child_state = complete_activity(child_instance, child_state, 0)
    assert child_state["isDone"] is True and not child_state.get("error")
    assert child_state["output"][SUBWORKFLOW_RESULT_KEY] is True
    state = host.complete_child("root-run", 1, child_state["output"])
    state = complete_activity("root-run", state, 2)
    assert state["isDone"] is True and not state.get("error")
    assert state["output"] == ["done"]
    recorded_calls = deepcopy(calls)
    assert host.replay(child_instance) == child_state
    assert host.replay("root-run") == state
    assert calls == recorded_calls
    assert [item["name"] for item in calls] == [
        "dafx-parent-source",
        "dafx-inner",
        "dafx-inner-leaf",
        "dafx-parent-sink",
    ]
    dispatch = calls[1]
    assert dispatch["instance"] == subworkflow_instance_id("root-run", "child", 0)
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
            if data["message"].get("validation_error") is True:
                assert deserialize_value(data["message"]) == {
                    "request_id": "approval",
                    "original_request": payload,
                    "response": None,
                    "response_type": "builtins:dict",
                    "validation_error": True,
                }
                return {
                    "hitl_admission": {"request_id": "approval", "status": "invalidreply"},
                    "sent_messages": [],
                    "outputs": [],
                    "events": [],
                    "shared_state_updates": {},
                    "shared_state_deletes": [],
                    "pending_request_info_events": [],
                }
            assert deserialize_value(data["message"]) == {
                "request_id": "approval",
                "original_request": payload,
                "response": answer,
                "response_type": "builtins:dict",
            }
            return {
                "hitl_admission": {"request_id": "approval", "status": "accepted"},
                "shared_state_deletes": ["pending"],
                "shared_state_updates": {"decision": answer},
                "sent_messages": [{"message": answer, "target_id": "sink"}],
            }
        assert name == "dafx-protocol-sink"
        assert data["shared_state_snapshot"] == {"decision": answer}
        assert data["message"] == answer
        return {"outputs": ["done"]}

    _, starter = _register(_workflow(nodes=[_node("gate"), _node("sink")], edges=[SingleEdgeGroup("gate", "sink")]))
    wire = await _start(starter, payload)
    executions = []
    for replay in (False, True):
        functions, _ = _register(
            _workflow(nodes=[_node("gate"), _node("sink")], edges=[SingleEdgeGroup("gate", "sink")])
        )
        calls: list[dict[str, Any]] = []
        host = _host(deepcopy(wire), calls, result, replay=replay)
        generator = functions["dafx-protocol"](host)
        batch = next(generator)
        assert batch.is_completed
        waiting = generator.send(batch.result)
        assert not waiting.is_completed and len(calls) == 1
        assert host.statuses[-1]["state"] == "waiting_for_human_input"
        assert host.statuses[-1]["pending_requests"]["approval"]["data"] == payload
        pending = deepcopy(host.statuses[-1]["pending_requests"])
        waiting.set_value(is_error=False, value=deepcopy(_UNTRUSTED))
        rejected = generator.send(waiting.result)
        assert rejected.is_completed and len(calls) == 2
        assert [json.loads(value)["hitl_admission"] for value in rejected.result] == [
            {"request_id": "approval", "status": "invalidreply"}
        ]
        assert host.statuses[-1]["pending_requests"] == pending
        waiting_again = generator.send(rejected.result)
        assert waiting_again is not waiting and not waiting_again.is_completed and len(calls) == 2
        assert host.statuses[-1]["pending_requests"] == pending
        waiting_again.set_value(is_error=False, value=deepcopy(answer))
        accepted = generator.send(waiting_again.result)
        assert accepted.is_completed and len(calls) == 3
        assert [json.loads(value)["hitl_admission"] for value in accepted.result] == [
            {"request_id": "approval", "status": "accepted"}
        ]
        assert _drain(generator, accepted.result) == ["done"]
        assert [item.args[0] for item in host.wait_for_external_event.call_args_list] == ["approval", "approval"]
        assert len(calls) == 4 and not host.statuses[-1].get("pending_requests")
        assert [item["input"]["source_executor_ids"] for item in calls[1:3]] == [
            [f"{SOURCE_HITL_RESPONSE}_approval"],
            [f"{SOURCE_HITL_RESPONSE}_approval"],
        ]
        assert calls[3]["input"]["source_executor_ids"] == ["gate"]
        executions.append((calls, deepcopy(host.statuses)))
    assert executions[0] == executions[1]
    assert wire == {_VERSION: 2, "input": payload}
