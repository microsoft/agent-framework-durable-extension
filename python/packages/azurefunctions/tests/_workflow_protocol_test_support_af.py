# Copyright (c) Microsoft. All rights reserved.

"""Azure Functions registration and HTTP start helpers for workflow boundary tests.

azure-functions-durable 2.x hands generated orchestrators a durabletask
``OrchestrationContext``, so the Durable Task host double drives them unchanged.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock

import azure.durable_functions as df
import azure.functions as func
from _workflow_protocol_test_support import (
    _CONTROL,
    _FORGED_ADDRESS,
    _UNTRUSTED,
    _VERSION,
    _complete,
    _drain,
    _host,
    _node,
    _TypedInput,
    _workflow,
)

from agent_framework_azurefunctions import AgentFunctionApp

__all__ = [
    "_CONTROL",
    "_FORGED_ADDRESS",
    "_UNTRUSTED",
    "_VERSION",
    "_TypedInput",
    "_complete",
    "_drain",
    "_host",
    "_node",
    "_register",
    "_start",
    "_workflow",
]


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
    client = AsyncMock(spec=df.DurableFunctionsClient)
    client.schedule_new_orchestration.return_value = "root-run"
    response = await starter(request, client)
    assert response.status_code == 202
    client.schedule_new_orchestration.assert_awaited_once()
    invocation = client.schedule_new_orchestration.await_args
    assert invocation is not None
    assert invocation.args == (f"dafx-{name}",)
    assert invocation.kwargs["instance_id"] == "root-run"
    return json.loads(json.dumps(invocation.kwargs["input"], allow_nan=False))
