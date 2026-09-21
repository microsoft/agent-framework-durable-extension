# Copyright (c) Microsoft. All rights reserved.

"""Generic HITL admission through registered Functions HTTP/activity closures.

The Functions SDK tasks and registered closures are real. The transport and HTTP
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
from agent_framework_durabletask._workflows import activity as activity_module
from agent_framework_durabletask._workflows.serialization import deserialize_response_type, deserialize_workflow_output
from test_workflow_admission_review_af import _registered_af_run
from test_workflow_generic_hitl_review import (
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
from test_workflow_protocol_review_af import _drain

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
            generator.send(batch.result)
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
    client = AsyncMock(spec=df.DurableOrchestrationClient)

    async def status(instance: str) -> Any:
        return SimpleNamespace(name=f"dafx-{workflow.name}", custom_status=deepcopy(host.statuses[-1]))

    client.get_status.side_effect = status

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
        waiting = generator.send(batch.result)
        assert not waiting.is_completed and len(calls) == 1
        pending = deepcopy(host.statuses[-1]["pending_requests"])
        assert deserialize_response_type(pending["approval"]["response_type"]) == annotation

        # An omitted HTTP body is not a JSON null response.
        assert submit(b"").status_code == 400
        client.raise_event.assert_not_awaited()
        for _ in range(2):
            delivered = submit(json.dumps(bad).encode("utf-8"))
            assert delivered.status_code == 200  # Delivery is not admission.
            wire = client.raise_event.await_args.kwargs["event_data"]
            assert wire == bad
            waiting.set_value(is_error=False, value=wire)
            waiting = generator.send(waiting.result)
            # Validation is a real activity now. Consume its checkpointed
            # invalidreply result before expecting a new external-event wait.
            assert waiting.is_completed
            waiting = generator.send(waiting.result)
            assert not waiting.is_completed and not seen
            assert host.statuses[-1]["pending_requests"] == pending
        assert submit(json.dumps(good).encode("utf-8")).status_code == 200
        waiting.set_value(is_error=False, value=client.raise_event.await_args.kwargs["event_data"])
        output = deserialize_workflow_output(_drain(generator, waiting.result))
        assert seen == oracle == [good]
        assert output == [{"value": good}] and len(calls) == 4
        client.raise_event.reset_mock()
        assert submit(json.dumps(good).encode("utf-8")).status_code == 200
        client.raise_event.assert_awaited_once()
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
    client = AsyncMock(spec=df.DurableOrchestrationClient)
    client.get_status.side_effect = lambda instance: SimpleNamespace(
        name=f"dafx-{workflow.name}", custom_status=deepcopy(statuses[instance])
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
        client.raise_event.assert_awaited_once_with(
            instance_id="child" if nested else "root", event_name=request_id, event_data=payload
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
    client = AsyncMock(spec=df.DurableOrchestrationClient)
    client.get_status.return_value.name = f"dafx-{workflow.name}"
    try:
        batch = next(generator)
        waiting = generator.send(batch.result)
        client.get_status.return_value.custom_status = deepcopy(host.statuses[-1])
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
        waiting.set_value(is_error=False, value=client.raise_event.await_args.kwargs["event_data"])
        if accepted:
            output = deserialize_workflow_output(_drain(generator, waiting.result))
            assert output == [{"value": oracle[0]}] and seen == oracle and len(calls) == 2
        else:
            retry = generator.send(waiting.result)
            assert retry.is_completed
            retry = generator.send(retry.result)
            assert not retry.is_completed and not seen and len(calls) == 2
            assert set(host.statuses[-1]["pending_requests"]) == {"approval"}
    finally:
        generator.close()


def test_http_unpublished_event_does_not_consume_a_different_pending_request() -> None:
    workflow, seen = _generic_workflow(list[int])
    generator, host, calls, respond = _registered_af_run(workflow)
    client = AsyncMock(spec=df.DurableOrchestrationClient)
    client.get_status.return_value.name = f"dafx-{workflow.name}"
    try:
        batch = next(generator)
        waiting = generator.send(batch.result)
        client.get_status.return_value.custom_status = deepcopy(host.statuses[-1])
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
        client.raise_event.assert_awaited_once_with(
            instance_id="root-run", event_name="arbitrary-request-id", event_data=[1]
        )
        assert not waiting.is_completed and not seen and len(calls) == 1
    finally:
        generator.close()


@pytest.mark.parametrize("annotation", [_CountedDecision, _CountedDataclass, list[_CountedDecision] | str])
def test_functions_registered_validation_and_cold_replay_do_not_repeat_user_validators(annotation: Any) -> None:
    _validator_replay_trial(annotation, functions_host=True)


def test_functions_invalid_generic_checkpoint_and_corrected_reply_survive_cold_replay() -> None:
    _invalid_generic_replay_trial(functions_host=True)


@pytest.mark.parametrize(("requested", "answer", "correction"), _CONCRETE_REPLAY_CASES)
def test_functions_concrete_core_admission_preserves_pending_until_activity_checkpoint(
    requested: type, answer: Any, correction: Any
) -> None:
    _concrete_replay_trial(requested, answer, correction, functions_host=True)


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


def _request(operation: str, request_id: str = "approval", payload: Any = None) -> func.HttpRequest:
    return func.HttpRequest(
        method="GET" if operation == "status" else "POST",
        url=f"https://example.test/api/workflow/generic-hitl/{operation}/root/{request_id}",
        headers={"Content-Type": "application/json"},
        params={},
        route_params={"instanceId": "root", "requestId": request_id},
        body=json.dumps(payload).encode("utf-8"),
    )


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize(
    "runtime_status",
    [
        df.OrchestrationRuntimeStatus.Completed,
        df.OrchestrationRuntimeStatus.Failed,
        df.OrchestrationRuntimeStatus.Canceled,
        df.OrchestrationRuntimeStatus.Terminated,
    ],
)
def test_http_real_terminal_state_never_advertises_or_accepts_stale_requests(runtime_status: Any, nested: bool) -> None:
    workflow, _ = _generic_workflow(list[int])
    functions = _http_functions(workflow)
    stale = {"state": "waiting_for_human_input", "pending_requests": {"approval": {}}}
    states = {
        "root": SimpleNamespace(
            name="dafx-generic-hitl",
            instance_id="root",
            runtime_status=runtime_status,
            custom_status=stale,
            output=None,
            created_time=None,
            last_updated_time=None,
        )
    }
    if nested:
        states["child"] = deepcopy(states["root"])
        states["root"].runtime_status = df.OrchestrationRuntimeStatus.Running
        states["root"].custom_status = {"subworkflows": {"sub": {"0": "child"}}}
    client = AsyncMock(spec=df.DurableOrchestrationClient)
    client.get_status.side_effect = states.get
    response = asyncio.run(functions["status"](_request("status"), client))
    assert response.status_code == 200
    assert "pendingHumanInputRequests" not in json.loads(response.get_body())
    response = asyncio.run(
        functions["respond"](_request("respond", "sub~0~approval" if nested else "approval", [7]), client)
    )
    assert response.status_code == (404 if nested else 409)
    client.raise_event.assert_not_awaited()


@pytest.mark.parametrize("runtime_status", [None, Mock(), df.OrchestrationRuntimeStatus.Running])
def test_http_absent_or_nonterminal_runtime_status_preserves_early_delivery(runtime_status: Any) -> None:
    workflow, _ = _generic_workflow(list[int])
    functions = _http_functions(workflow)
    client = AsyncMock(spec=df.DurableOrchestrationClient)
    client.get_status.return_value = SimpleNamespace(
        name="dafx-generic-hitl", runtime_status=runtime_status, custom_status=None
    )
    response = asyncio.run(functions["respond"](_request("respond", payload=[7]), client))
    assert response.status_code == 200
    client.raise_event.assert_awaited_once_with(instance_id="root", event_name="approval", event_data=[7])


def test_http_early_fixed_id_is_buffered_by_real_sdk_then_validated_by_registered_activity() -> None:
    from test_workflow_mixed_hitl_review import _Episodes
    from test_workflow_recorded_replay_review import _af_replay

    workflow, seen = _generic_workflow(list[int])
    app = AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")
    activities: dict[str, Any] = {}
    for item in app.get_functions():
        name = item.get_function_name()
        assert name is not None
        activities[name] = item.get_user_function()
    functions = _http_functions(workflow)
    episodes = _Episodes(workflow)
    episodes.client.start_workflow("go", instance_id="root")
    client = AsyncMock(spec=df.DurableOrchestrationClient)
    client.get_status.return_value = SimpleNamespace(
        name="dafx-generic-hitl", runtime_status=df.OrchestrationRuntimeStatus.Running, custom_status=None
    )

    async def deliver(instance_id: str, event_name: str, event_data: Any) -> None:
        episodes.signal(instance_id, event_name=event_name, data=event_data)

    client.raise_event.side_effect = deliver
    response = asyncio.run(functions["respond"](_request("respond", payload=[7]), client))
    assert response.status_code == 200
    episodes.flush()
    assert not seen and episodes.pending() == set()
    _complete_generic_activity(episodes, functions=activities)
    _complete_generic_activity(episodes, functions=activities)
    assert seen == [[7]]
    replay = _af_replay(episodes.histories["root"], workflow, instance="root")
    assert replay["isDone"] and replay["output"] == [{"value": [7]}]


@pytest.mark.parametrize("output_failure", [False, True])
def test_functions_handler_and_output_errors_remain_terminal_failures(output_failure: bool) -> None:
    _handler_failure_trial(functions_host=True, output_failure=output_failure)


def test_functions_numeric_overflow_keeps_request_pending_for_correction() -> None:
    workflow, seen = _generic_workflow(float)
    generator, host, calls, _ = _registered_af_run(workflow)
    try:
        batch = next(generator)
        waiting = generator.send(batch.result)
        waiting.set_value(is_error=False, value=10**1000)
        validation = generator.send(waiting.result)
        assert validation.is_completed and not seen
        waiting = generator.send(validation.result)
        assert not waiting.is_completed and len(calls) == 2
        assert set(host.statuses[-1]["pending_requests"]) == {"approval"}
        waiting.set_value(is_error=False, value=7)
        assert deserialize_workflow_output(_drain(generator, waiting.result)) == [{"value": 7.0}]
        assert seen == [7.0] and type(seen[0]) is float
    finally:
        generator.close()
