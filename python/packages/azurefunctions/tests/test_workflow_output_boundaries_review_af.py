# Copyright (c) Microsoft. All rights reserved.

"""Registered AF output/status boundaries preserve generated response JSON values."""

from __future__ import annotations

import json
from collections.abc import Callable
from copy import deepcopy
from datetime import date, datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock, patch
from uuid import UUID

import azure.durable_functions as df
import azure.functions as func
import pytest
from agent_framework import AgentExecutor, AgentResponse, AgentSession, Message, WorkflowBuilder, WorkflowExecutor
from agent_framework._workflows import _checkpoint_encoding
from agent_framework_durabletask import load_agent_response, serialize_agent_response
from agent_framework_durabletask._workflows.protocol import wrap_workflow_input
from azure.durable_functions.models.actions.NoOpAction import NoOpAction
from azure.durable_functions.models.ReplaySchema import ReplaySchema
from azure.durable_functions.models.Task import AtomicTask, WhenAllTask
from pydantic import BaseModel, Field

from agent_framework_azurefunctions import AgentFunctionApp
from agent_framework_azurefunctions._app import _json_default


class _Agent:
    id = name = "A"
    description = None

    def __init__(self, response_format: type[BaseModel] | None) -> None:
        self.default_options = {"response_format": response_format}

    def create_session(self, **kwargs: Any) -> AgentSession:
        return AgentSession(**kwargs)

    async def run(self, *args: Any, **kwargs: Any) -> AgentResponse:
        raise AssertionError("Entity completion is supplied at the native task boundary")


def _response_case(kind: str) -> tuple[AgentResponse, type[BaseModel] | None, Any, bool]:
    class AliasAnswer(BaseModel):
        answer: bool = Field(alias="wireAnswer")
        day: date

    class ByNameAnswer(BaseModel):
        answer: bool = Field(validation_alias="inputAnswer", serialization_alias="outputAnswer")
        day: date

    model: type[BaseModel] | None = None
    expected: Any = False if kind == "false" else None
    value = expected
    if kind == "alias":
        model = AliasAnswer
        value = AliasAnswer(wireAnswer=False, day=date(2026, 9, 9))
        expected = {"wireAnswer": False, "day": "2026-09-09"}
    elif kind == "by_name":
        model = ByNameAnswer
        value = ByNameAnswer(inputAnswer=False, day=date(2026, 9, 9))
        expected = {"answer": False, "day": "2026-09-09"}
    response = AgentResponse(
        messages=[Message("assistant", ["not structured text"], message_id="message-id")],
        response_id="response-id",
        value=value,
        additional_properties={"flag": False, "nullable": None},
    )
    if kind == "null":
        response = load_agent_response({**response.to_dict(), "value": None})
    return response, model, expected, kind == "by_name"


def _register(model: type[BaseModel] | None, nested: bool) -> tuple[dict[str, Any], dict[str, Any]]:
    agent: Any = _Agent(model)
    workflow = WorkflowBuilder(name="inner" if nested else "portable", start_executor=AgentExecutor(agent)).build()
    if nested:
        child = WorkflowExecutor(workflow, id="child", allow_direct_output=True)
        workflow = WorkflowBuilder(name="portable", start_executor=child, output_from=[child]).build()
    app = AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")
    orchestrators: dict[str, Any] = {}
    routes: dict[str, Any] = {}
    for function in app.get_functions():
        trigger = function.get_trigger()
        assert trigger is not None
        binding = trigger.get_dict_repr()
        user_function: Any = function.get_user_function()
        if binding["type"] == "orchestrationTrigger":
            name = function.get_function_name()
            assert name is not None
            orchestrators[name] = user_function.orchestrator_function
        elif binding["type"] == "httpTrigger":
            routes[binding["route"]] = user_function.client_function
    return orchestrators, routes


def _run_registered(orchestrators: dict[str, Any], response: AgentResponse, nested: bool) -> tuple[Any, list[Any]]:
    statuses: list[Any] = []

    def complete(value: Any) -> AtomicTask:
        task = AtomicTask(0, NoOpAction())
        task.set_value(is_error=False, value=json.loads(json.dumps(value, allow_nan=False)))
        return task

    def run(name: str, input_data: Any, instance_id: str) -> Any:
        host = Mock(spec=df.DurableOrchestrationContext)
        host.instance_id = instance_id
        host.is_replaying = False
        host.current_utc_datetime = datetime(2026, 9, 9, tzinfo=timezone.utc)
        host.new_uuid.side_effect = [str(UUID(int=i)) for i in range(1, 10)]
        host.get_input.return_value = input_data
        host.call_entity.side_effect = lambda *args: complete(serialize_agent_response(response))
        host.call_sub_orchestrator.side_effect = lambda name, *, input_, instance_id: complete(
            run(name, input_, instance_id)
        )
        host.task_all.side_effect = lambda tasks: WhenAllTask(tasks, ReplaySchema.V1)
        host.set_custom_status.side_effect = lambda status: statuses.append(deepcopy(status))
        generator = orchestrators[name](host)
        value = None
        while True:
            try:
                task = generator.send(value)
            except StopIteration as completed:
                host.call_activity.assert_not_called()
                if name == "dafx-portable" and nested:
                    host.call_sub_orchestrator.assert_called_once()
                else:
                    host.call_entity.assert_called_once()
                return json.loads(json.dumps(completed.value, allow_nan=False))
            assert task.is_completed
            value = task.result

    return run("dafx-portable", wrap_workflow_input("question"), "output-run"), statuses


@pytest.mark.parametrize("endpoint", ["status", "wait"])
@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("kind", ["false", "null", "alias", "by_name"])
async def test_registered_status_and_terminal_run_return_generated_response_value(
    endpoint: str, nested: bool, kind: str
) -> None:
    response, model, expected, by_name = _response_case(kind)
    orchestrators, routes = _register(model, nested)
    with patch.object(_checkpoint_encoding, "_pickle_to_base64", side_effect=AssertionError("No worker pickle")):
        raw, statuses = _run_registered(orchestrators, response, nested)
    assert all("events" not in status for status in statuses)
    assert len(raw) == 1 and raw[0]["_durable_agent_response"] == 1
    assert "__pickled__" not in json.dumps(raw)
    client = AsyncMock(spec=df.DurableOrchestrationClient)
    client.start_new.return_value = "output-run"
    client.wait_for_completion_or_create_check_status_response.return_value = func.HttpResponse(status_code=200)
    client.get_status.return_value = SimpleNamespace(
        name="dafx-portable",
        instance_id="output-run",
        runtime_status=df.OrchestrationRuntimeStatus.Completed,
        output=raw,
        custom_status=statuses[-1],
        created_time=None,
        last_updated_time=None,
    )
    route = "workflow/portable/status/{instanceId}" if endpoint == "status" else "workflow/portable/run"
    request = func.HttpRequest(
        method="GET" if endpoint == "status" else "POST",
        url="https://example.test/api/" + route.replace("{instanceId}", "output-run"),
        headers={"Content-Type": "application/json"},
        params={} if endpoint == "status" else {"waitForResponse": "true", "runId": "output-run"},
        route_params={"instanceId": "output-run"} if endpoint == "status" else {},
        body=b'"question"',
    )
    handler: Callable[..., Any] = routes[route]
    with (
        patch("importlib.import_module", side_effect=AssertionError("HTTP readers do not import worker models")),
        patch.object(_checkpoint_encoding, "_base64_to_unpickle", side_effect=AssertionError("No response pickle")),
    ):
        http_response = await handler(request, client)
    assert http_response.status_code == 200
    body = json.loads(http_response.get_body())
    assert body["runtimeStatus"] == "Completed"
    assert len(body["output"]) == 1
    delivered = body["output"][0]
    assert delivered == raw[0]["response"]
    assert "value" in delivered and delivered["value"] == expected
    assert type(delivered["value"]) is type(expected)
    assert delivered.get("_durable_value_by_name", False) is by_name
    assert delivered["response_id"] == "response-id"
    assert delivered["additional_properties"] == {"flag": False, "nullable": None}
    assert delivered["messages"][0]["message_id"] == "message-id"
    client.get_status.assert_awaited_once_with("output-run")
    if endpoint == "wait":
        client.wait_for_completion_or_create_check_status_response.assert_awaited_once()
    else:
        client.start_new.assert_not_awaited()


@pytest.mark.parametrize("kind", ["false", "null", "alias", "by_name"])
def test_json_default_uses_base_response_serializer_before_overridden_to_dict(kind: str) -> None:
    response, _, expected, by_name = _response_case(kind)

    class WorkerResponse(AgentResponse):
        def to_dict(self, **kwargs: Any) -> dict[str, Any]:
            raise AssertionError("Provider override must not replace the durable response contract")

    subclass = WorkerResponse(
        messages=response.messages,
        value=load_agent_response(serialize_agent_response(response)).value,
        response_id=response.response_id,
        additional_properties=response.additional_properties,
    )
    if kind == "null":
        subclass._value_parsed = True
    if by_name:
        typed_snapshot: Any = subclass
        typed_snapshot._durable_value_by_name = True
    encoded = _json_default(subclass)
    assert "value" in encoded and encoded["value"] == expected
    assert encoded.get("_durable_value_by_name", False) is by_name
    assert encoded["additional_properties"] == {"flag": False, "nullable": None}
