# Copyright (c) Microsoft. All rights reserved.

"""HITL HTTP admission and fan-in through registered Azure Functions closures."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Generator
from copy import deepcopy
from typing import Any
from unittest.mock import AsyncMock, Mock

import azure.durable_functions as df
import azure.functions as func
import pytest
from agent_framework import Workflow, WorkflowBuilder
from agent_framework_durabletask import wrap_workflow_input
from agent_framework_durabletask._workflows.serialization import deserialize_workflow_output
from test_workflow_admission_review import _hitl_workflow, _Relay, _Sink
from test_workflow_protocol_review_af import _drain, _host

from agent_framework_azurefunctions import AgentFunctionApp


def _registered_af_run(
    workflow: Workflow,
) -> tuple[Generator[Any, Any, Any], Mock, list[dict[str, Any]], Callable[..., Any]]:
    app = AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")
    functions = {function.get_function_name(): function for function in app.get_functions()}
    activities: dict[str, Callable[..., Any]] = {}
    responders: list[Any] = []
    for name, function in functions.items():
        trigger = function.get_trigger()
        assert name is not None and trigger is not None
        binding = trigger.get_dict_repr()
        if binding["type"] == "activityTrigger":
            activities[name] = function.get_user_function()
        elif binding.get("route") == f"workflow/{workflow.name}/respond/{{instanceId}}/{{requestId}}":
            responders.append(function.get_user_function())
    orchestrator: Any = functions[f"dafx-{workflow.name}"].get_user_function()
    assert len(responders) == 1
    responder: Any = responders[0]

    def activity(name: str, payload: dict[str, Any]) -> dict[str, Any]:
        return json.loads(activities[name](json.dumps(payload, allow_nan=False)))

    calls: list[dict[str, Any]] = []
    host = _host(wrap_workflow_input("go"), calls, activity)
    return orchestrator.orchestrator_function(host), host, calls, responder.client_function


@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("right_emissions", [0, 1])
def test_af_registered_fanin_preserves_barrier_and_sources(explicit: bool, right_emissions: int) -> None:
    source, sink = _Relay("source"), _Sink()
    target = "sink" if explicit else None
    left, right = _Relay("left", target=target), _Relay("right", target=target, emissions=right_emissions)
    workflow = (
        WorkflowBuilder(name="fanin-af", start_executor=source, output_from=[sink])
        .add_fan_out_edges(source, [left, right])
        .add_fan_in_edges([left, right], sink)
        .build()
    )
    generator, _, calls, _ = _registered_af_run(workflow)

    expected = [{"payload": ["left", "right"], "sources": ["left", "right"]}] if right_emissions else []
    assert _drain(generator) == expected
    assert sink.seen == expected
    sink_calls = [call for call in calls if call["name"] == "dafx-fanin-af-sink"]
    assert len(sink_calls) == right_emissions
    if sink_calls:
        assert sink_calls[0]["input"]["source_executor_ids"] == ["left", "right"]


@pytest.mark.parametrize("marker", [{"__type__": "builtins:dict"}, {"__pickled__": "inert"}])
@pytest.mark.parametrize(
    ("answer", "expected"),
    [
        (None, None),
        ({"approved": True}, {"approved": True}),
        (
            {"nested": {"type": "business", "_durable_agent_response": 99, "input": [0, False, None]}},
            {"nested": {"type": "business", "_durable_agent_response": 99, "input": [0, False, None]}},
        ),
        ({"nested": {"__type__": "builtins:dict"}, "approved": True}, {"nested": None, "approved": True}),
    ],
)
def test_http_hitl_rejects_root_marker_before_event_and_accepts_corrected_reply(
    marker: dict[str, Any], answer: Any, expected: Any
) -> None:
    workflow = _hitl_workflow()
    generator, host, calls, respond = _registered_af_run(workflow)
    batch = next(generator)
    waiting = generator.send(batch.result)
    assert not waiting.is_completed and len(calls) == 1
    client = AsyncMock(spec=df.DurableOrchestrationClient)
    client.get_status.return_value = Mock(name="status")
    client.get_status.return_value.name = f"dafx-{workflow.name}"

    def submit(payload: Any) -> Any:
        request = func.HttpRequest(
            method="POST",
            url=f"https://example.test/api/workflow/{workflow.name}/respond/root-run/approval",
            headers={"Content-Type": "application/json"},
            params={},
            route_params={"instanceId": "root-run", "requestId": "approval"},
            body=json.dumps(payload, allow_nan=False).encode("utf-8"),
        )
        return asyncio.run(respond(request, client))

    rejected = submit(marker)
    assert rejected.status_code == 400
    assert "disallowed pickle/type markers" in json.loads(rejected.get_body())["error"]
    client.raise_event.assert_not_awaited()
    assert not waiting.is_completed and len(calls) == 1
    assert set(host.statuses[-1]["pending_requests"]) == {"approval"}

    before = deepcopy(answer)
    accepted = submit(answer)
    assert accepted.status_code == 200 and answer == before
    client.raise_event.assert_awaited_once_with(instance_id="root-run", event_name="approval", event_data=expected)
    waiting.set_value(is_error=False, value=client.raise_event.await_args.kwargs["event_data"])
    output = deserialize_workflow_output(_drain(generator, waiting.result))
    assert output == [{"response": expected, "response_type": type(expected).__name__}]
    assert len(calls) == 2
