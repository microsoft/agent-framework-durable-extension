# Copyright (c) Microsoft. All rights reserved.

"""Registered Azure Functions workflow dispatch for admission tests."""

from __future__ import annotations

import json
from collections.abc import Callable, Generator
from typing import Any
from unittest.mock import Mock

from _workflow_protocol_test_support_af import _host
from agent_framework import Workflow
from agent_framework_durabletask import wrap_workflow_input

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
