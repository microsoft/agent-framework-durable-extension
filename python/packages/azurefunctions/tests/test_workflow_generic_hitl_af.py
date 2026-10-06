# Copyright (c) Microsoft. All rights reserved.

"""Generic HITL admission through registered Functions HTTP/activity closures.

The registered closures and durabletask tasks are real. The transport and HTTP
client are in-process doubles, not a deployed Functions host or backend service.
"""

import asyncio
import importlib
import json
from copy import deepcopy
from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import AsyncMock, Mock

import azure.durable_functions as df
import azure.functions as func
import pytest
from _af_worker_test_support import _af_host, _af_worker, _event_wire_value, _orchestration_state
from _workflow_admission_test_support_af import _registered_af_run
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
from _workflow_generic_hitl_test_support_af import _request
from _workflow_protocol_test_support_af import _drain
from _workflow_replay_test_support import _Episodes, _replay
from agent_framework_durabletask._workflows import activity as activity_module
from agent_framework_durabletask._workflows.serialization import deserialize_response_type, deserialize_workflow_output
from durabletask.client import OrchestrationStatus

from agent_framework_azurefunctions import AgentFunctionApp


@pytest.mark.parametrize("descriptor", ["", {}, "unloaded_reply_types:Arbitrary"])
def test_registered_functions_orchestrator_rejects_broken_descriptor_before_waiting(
    descriptor: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(activity_module, "serialize_response_type", lambda annotation: descriptor)
    workflow, seen = _generic_workflow(list[int])
    generator, host, calls, _ = _registered_af_run(workflow)
    try:
        batch = next(generator)
        with pytest.raises(ValueError, match="HITL"):
            generator.send(batch.get_result())
        host.wait_for_external_event.assert_not_called()
        assert all(not status.get("pending_requests") for status in host.statuses)
        assert seen == [] and len(calls) == 1
    finally:
        generator.close()


@pytest.mark.parametrize(
    ("annotation", "bad", "good"),
    [
        (list[int], ["bad"], [1, True]),
        (list[bool], [1], [True]),
        (dict[str, list[int]], {"x": ["1"]}, {"x": [1]}),
        (Optional[list[int]], [None], None),
        (list[int] | str, ["bad"], "yes"),
        (list[int], {"response_type": "builtins:object", "response": ["bad"]}, [1]),
        (
            list[int],
            {"response_type": {"_durable_response_type": 1, "kind": "list", "args": ["typing:Any"]}},
            [1],
        ),
    ],
)
def test_http_generic_response_keeps_pending_after_invalid_then_accepts_correction(
    annotation: Any, bad: Any, good: Any
) -> None:
    assert asyncio.run(_core_trial(annotation, bad)) == (False, [])
    accepted, oracle = asyncio.run(_core_trial(annotation, good))
    assert accepted
    workflow, seen = _generic_workflow(annotation)
    generator, host, calls, respond = _registered_af_run(workflow)
    client = AsyncMock(spec=df.DurableFunctionsClient)

    async def status(instance: str) -> Any:
        return _orchestration_state(instance, f"dafx-{workflow.name}", custom_status=deepcopy(host.statuses[-1]))

    client.get_orchestration_state.side_effect = status

    def submit(body: bytes) -> Any:
        request = func.HttpRequest(
            method="POST",
            url=f"https://example.test/api/workflow/{workflow.name}/respond/root-run/approval",
            headers={"Content-Type": "application/json"},
            params={},
            route_params={"instanceId": "root-run", "requestId": "approval"},
            body=body,
        )
        return asyncio.run(respond(request, client))

    try:
        batch = next(generator)
        waiting = generator.send(batch.get_result())
        assert not waiting.is_complete and len(calls) == 1
        pending = deepcopy(host.statuses[-1]["pending_requests"])
        assert deserialize_response_type(pending["approval"]["response_type"]) == annotation

        # An omitted HTTP body is not a JSON null response.
        assert submit(b"").status_code == 400
        client.raise_orchestration_event.assert_not_awaited()
        for _ in range(2):
            delivered = submit(json.dumps(bad).encode("utf-8"))
            assert delivered.status_code == 200  # Delivery is not admission.
            wire = client.raise_orchestration_event.await_args.kwargs["data"]
            assert wire == bad
            waiting.complete(_event_wire_value(wire))
            waiting = generator.send(waiting.get_result())
            # Validation is a real activity now. Consume its checkpointed
            # invalidreply result before expecting a new external-event wait.
            assert waiting.is_complete
            waiting = generator.send(waiting.get_result())
            assert not waiting.is_complete and not seen
            assert host.statuses[-1]["pending_requests"] == pending
        assert submit(json.dumps(good).encode("utf-8")).status_code == 200
        waiting.complete(_event_wire_value(client.raise_orchestration_event.await_args.kwargs["data"]))
        output = deserialize_workflow_output(_drain(generator, waiting.get_result()))
        assert seen == oracle == [good]
        assert output == [{"value": good}] and len(calls) == 4
        client.raise_orchestration_event.reset_mock()
        assert submit(json.dumps(good).encode("utf-8")).status_code == 200
        client.raise_orchestration_event.assert_awaited_once()
    finally:
        generator.close()


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize(("custom_status", "request_id", "accepted"), _PENDING_STATUS_CASES)
def test_http_leaf_delivery_does_not_require_a_published_pending_record(
    custom_status: Any, request_id: str, accepted: bool, nested: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    workflow, seen = _generic_workflow(list[int])
    generator, _, calls, respond = _registered_af_run(workflow)
    statuses = {"child" if nested else "root": custom_status}
    if nested:
        statuses["root"] = {"subworkflows": {"sub": {"0": "child"}}}
    client = AsyncMock(spec=df.DurableFunctionsClient)
    client.get_orchestration_state.side_effect = lambda instance: _orchestration_state(
        instance, f"dafx-{workflow.name}", custom_status=deepcopy(statuses[instance])
    )
    qualified_id = f"sub~0~{request_id}" if nested else request_id
    payload = {"response_type": "builtins:object", "request_id": qualified_id, "response": None}
    request = func.HttpRequest(
        method="POST",
        url=f"https://example.test/api/workflow/{workflow.name}/respond/root/{qualified_id}",
        headers={"Content-Type": "application/json"},
        params={},
        route_params={"instanceId": "root", "requestId": qualified_id},
        body=json.dumps(payload).encode("utf-8"),
    )
    forbidden = Mock(side_effect=AssertionError("HTTP routing must not load descriptor-selected modules"))
    monkeypatch.setattr(importlib, "import_module", forbidden)
    before = deepcopy(statuses)
    try:
        response = asyncio.run(respond(request, client))
        assert response.status_code == 200
        client.raise_orchestration_event.assert_awaited_once_with(
            "child" if nested else "root", request_id, data=payload
        )
        assert statuses == before and not seen and not calls
        forbidden.assert_not_called()
    finally:
        generator.close()


def test_http_nested_custom_model_uses_installed_core_admission_rules() -> None:
    annotation = dict[str, list[_Models.Decision]]
    wire = {"x": [{"count": 7, "verdict": "approve"}]}
    accepted, oracle = asyncio.run(_core_trial(annotation, wire))
    workflow, seen = _generic_workflow(annotation)
    generator, host, calls, respond = _registered_af_run(workflow)
    client = AsyncMock(spec=df.DurableFunctionsClient)
    try:
        batch = next(generator)
        waiting = generator.send(batch.get_result())
        client.get_orchestration_state.return_value = _orchestration_state(
            "root-run", f"dafx-{workflow.name}", custom_status=deepcopy(host.statuses[-1])
        )
        request = func.HttpRequest(
            method="POST",
            url=f"https://example.test/api/workflow/{workflow.name}/respond/root-run/approval",
            headers={"Content-Type": "application/json"},
            params={},
            route_params={"instanceId": "root-run", "requestId": "approval"},
            body=json.dumps(wire).encode("utf-8"),
        )
        response = asyncio.run(respond(request, client))
        assert response.status_code == 200
        waiting.complete(_event_wire_value(client.raise_orchestration_event.await_args.kwargs["data"]))
        if accepted:
            output = deserialize_workflow_output(_drain(generator, waiting.get_result()))
            assert output == [{"value": oracle[0]}] and seen == oracle and len(calls) == 2
        else:
            retry = generator.send(waiting.get_result())
            assert retry.is_complete
            retry = generator.send(retry.get_result())
            assert not retry.is_complete and not seen and len(calls) == 2
            assert set(host.statuses[-1]["pending_requests"]) == {"approval"}
    finally:
        generator.close()


def test_http_unpublished_event_does_not_consume_a_different_pending_request() -> None:
    workflow, seen = _generic_workflow(list[int])
    generator, host, calls, respond = _registered_af_run(workflow)
    client = AsyncMock(spec=df.DurableFunctionsClient)
    try:
        batch = next(generator)
        waiting = generator.send(batch.get_result())
        client.get_orchestration_state.return_value = _orchestration_state(
            "root-run", f"dafx-{workflow.name}", custom_status=deepcopy(host.statuses[-1])
        )
        request = func.HttpRequest(
            method="POST",
            url=f"https://example.test/api/workflow/{workflow.name}/respond/root-run/arbitrary-request-id",
            headers={"Content-Type": "application/json"},
            params={},
            route_params={"instanceId": "root-run", "requestId": "arbitrary-request-id"},
            body=b"[1]",
        )
        response = asyncio.run(respond(request, client))
        assert response.status_code == 200
        client.raise_orchestration_event.assert_awaited_once_with("root-run", "arbitrary-request-id", data=[1])
        assert not waiting.is_complete and not seen and len(calls) == 1
    finally:
        generator.close()


@pytest.mark.parametrize("annotation", [_CountedDecision, _CountedDataclass, list[_CountedDecision] | str])
def test_functions_registered_validation_and_cold_replay_do_not_repeat_user_validators(annotation: Any) -> None:
    _validator_replay_trial(annotation, host=_af_host)


def test_functions_invalid_generic_checkpoint_and_corrected_reply_survive_cold_replay() -> None:
    _invalid_generic_replay_trial(host=_af_host)


@pytest.mark.parametrize(("requested", "answer", "correction"), _CONCRETE_REPLAY_CASES)
def test_functions_concrete_core_admission_preserves_pending_until_activity_checkpoint(
    requested: type, answer: Any, correction: Any
) -> None:
    _concrete_replay_trial(requested, answer, correction, host=_af_host)


def _http_functions(workflow: Any) -> dict[str, Any]:
    app = AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")
    functions: dict[str, Any] = {}
    for function in app.get_functions():
        trigger = function.get_trigger()
        if trigger is not None:
            route = trigger.get_dict_repr().get("route")
            if route is not None:
                name = "status" if "/status/" in route else "respond" if "/respond/" in route else "run"
                user_function: Any = function.get_user_function()
                functions[name] = user_function.client_function
    return functions


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize(
    "runtime_status",
    [OrchestrationStatus.COMPLETED, OrchestrationStatus.FAILED, OrchestrationStatus.TERMINATED],
)
def test_http_real_terminal_state_never_advertises_or_accepts_stale_requests(
    runtime_status: OrchestrationStatus, nested: bool
) -> None:
    workflow, _ = _generic_workflow(list[int])
    functions = _http_functions(workflow)
    stale = {"state": "waiting_for_human_input", "pending_requests": {"approval": {}}}
    states = {
        "root": _orchestration_state("root", "dafx-generic-hitl", runtime_status=runtime_status, custom_status=stale)
    }
    if nested:
        states["child"] = _orchestration_state(
            "child", "dafx-generic-hitl", runtime_status=runtime_status, custom_status=stale
        )
        states["root"] = _orchestration_state(
            "root",
            "dafx-generic-hitl",
            runtime_status=OrchestrationStatus.RUNNING,
            custom_status={"subworkflows": {"sub": {"0": "child"}}},
        )
    client = AsyncMock(spec=df.DurableFunctionsClient)
    client.get_orchestration_state.side_effect = states.get
    response = asyncio.run(functions["status"](_request("status"), client))
    assert response.status_code == 200
    assert "pendingHumanInputRequests" not in json.loads(response.get_body())
    response = asyncio.run(
        functions["respond"](_request("respond", "sub~0~approval" if nested else "approval", [7]), client)
    )
    assert response.status_code == (404 if nested else 409)
    client.raise_orchestration_event.assert_not_awaited()


@pytest.mark.parametrize(
    "runtime_status",
    [
        Mock(),
        OrchestrationStatus.RUNNING,
        OrchestrationStatus.PENDING,
        OrchestrationStatus.SUSPENDED,
        OrchestrationStatus.CONTINUED_AS_NEW,
    ],
)
def test_http_absent_or_nonterminal_runtime_status_preserves_early_delivery(runtime_status: Any) -> None:
    workflow, _ = _generic_workflow(list[int])
    functions = _http_functions(workflow)
    client = AsyncMock(spec=df.DurableFunctionsClient)
    client.get_orchestration_state.return_value = SimpleNamespace(
        name="dafx-generic-hitl", runtime_status=runtime_status, serialized_custom_status=None
    )
    response = asyncio.run(functions["respond"](_request("respond", payload=[7]), client))
    assert response.status_code == 200
    client.raise_orchestration_event.assert_awaited_once_with("root", "approval", data=[7])


def test_http_early_fixed_id_is_buffered_by_real_sdk_then_validated_by_registered_activity() -> None:
    workflow, seen = _generic_workflow(list[int])
    functions = _http_functions(workflow)
    episodes = _Episodes(workflow, worker=_af_host(workflow))
    episodes.client.start_workflow("go", instance_id="root")
    client = AsyncMock(spec=df.DurableFunctionsClient)
    client.get_orchestration_state.return_value = _orchestration_state("root", "dafx-generic-hitl")

    async def deliver(instance_id: str, event_name: str, *, data: Any) -> None:
        episodes.signal(instance_id, event_name=event_name, data=data)

    client.raise_orchestration_event.side_effect = deliver
    response = asyncio.run(functions["respond"](_request("respond", payload=[7]), client))
    assert response.status_code == 200
    episodes.flush()
    assert not seen and episodes.pending() == set()
    _complete_generic_activity(episodes)
    _complete_generic_activity(episodes)
    assert seen == [[7]]
    replay = _replay(episodes.worker, "root", episodes.histories["root"])
    assert len(replay.actions) == 1 and replay.actions[0].HasField("completeOrchestration")
    output = json.loads(replay.actions[0].completeOrchestration.result.value)
    assert deserialize_workflow_output(output) == [{"value": [7]}]


@pytest.mark.parametrize("output_failure", [False, True])
def test_functions_handler_and_output_errors_remain_terminal_failures(output_failure: bool) -> None:
    _handler_failure_trial(host=_af_host, output_failure=output_failure)


def test_functions_numeric_overflow_keeps_request_pending_for_correction() -> None:
    workflow, seen = _generic_workflow(float)
    generator, host, calls, _ = _registered_af_run(workflow)
    try:
        batch = next(generator)
        waiting = generator.send(batch.get_result())
        waiting.complete(_event_wire_value(10**1000))
        validation = generator.send(waiting.get_result())
        assert validation.is_complete and not seen
        waiting = generator.send(validation.get_result())
        assert not waiting.is_complete and len(calls) == 2
        assert set(host.statuses[-1]["pending_requests"]) == {"approval"}
        waiting.complete(_event_wire_value(7))
        assert deserialize_workflow_output(_drain(generator, waiting.get_result())) == [{"value": 7.0}]
        assert seen == [7.0] and type(seen[0]) is float
    finally:
        generator.close()


def test_af_worker_harness_uses_the_apps_registered_functions() -> None:
    workflow, _ = _generic_workflow(list[int])
    worker = _af_worker(AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2"))
    assert worker._registry.get_orchestrator("dafx-generic-hitl") is not None
    assert worker._registry.get_activity("dafx-generic-hitl-gate") is not None
