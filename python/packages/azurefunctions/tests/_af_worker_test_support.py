# Copyright (c) Microsoft. All rights reserved.

"""Replay an AgentFunctionApp's registered durable functions through durabletask.

azure-functions-durable 2.x runs orchestrators and entities on durabletask's own
executors. Each generated function holds a ``DurableFunctionsWorker`` that registers
the user function on first use. This helper registers the same functions, adapted
the same way, on a plain durabletask worker that shares the app's data converter, so
DT-style history replays exercise the Functions host's orchestrator, activities and
payload decoding without a Functions host.
"""

import inspect
import json
from datetime import datetime
from typing import Any

from agent_framework import Workflow
from agent_framework_durabletask._json_payload import JsonPayload, _JsonPayloadConverter
from azure.durable_functions.internal.compat.entity_context import wrap_entity
from azure.durable_functions.internal.compat.orchestration_context import wrap_orchestrator
from azure.durable_functions.internal.serialization import DEFAULT_FUNCTIONS_DATA_CONVERTER
from durabletask import task
from durabletask.client import OrchestrationState, OrchestrationStatus
from durabletask.task import FailureDetails
from durabletask.worker import TaskHubGrpcWorker

from agent_framework_azurefunctions import AgentFunctionApp

# The converter every framework-generated Functions worker uses.
FUNCTIONS_FRAMEWORK_CONVERTER = _JsonPayloadConverter(DEFAULT_FUNCTIONS_DATA_CONVERTER)


def _event_wire_value(data: Any) -> Any:
    """Return what a waiting workflow receives for an event raised by the Functions client."""
    return FUNCTIONS_FRAMEWORK_CONVERTER.deserialize(DEFAULT_FUNCTIONS_DATA_CONVERTER.serialize(data), JsonPayload)


def _orchestration_state(
    instance_id: str,
    name: str,
    *,
    runtime_status: OrchestrationStatus = OrchestrationStatus.RUNNING,
    custom_status: Any = None,
    output: Any = None,
    failure_details: FailureDetails | None = None,
) -> OrchestrationState:
    """A real OrchestrationState as the Functions client returns it.

    Payloads are serialized JSON, timestamps are naive UTC and reads use the Functions
    converter, so a regression to ``get_output()`` would construct envelope objects.
    """
    moment = datetime(2026, 9, 30)
    return OrchestrationState(
        instance_id=instance_id,
        name=name,
        runtime_status=runtime_status,
        created_at=moment,
        last_updated_at=moment,
        serialized_input=None,
        serialized_output=None if output is None else json.dumps(output),
        serialized_custom_status=None if custom_status is None else json.dumps(custom_status),
        failure_details=failure_details,
        _data_converter=DEFAULT_FUNCTIONS_DATA_CONVERTER,
    )


def _host_activity(registered: Any) -> Any:
    # The host binds the decoded activity input to the single trigger parameter.
    def run(_ctx: Any, value: Any) -> Any:
        return registered(value)

    return run


def _function_worker(registered: Any) -> Any:
    return inspect.getclosurevars(inspect.unwrap(registered)).nonlocals["worker"]


def _af_worker(app: AgentFunctionApp) -> Any:
    """Return a durabletask worker holding ``app``'s durable functions and converter."""
    native = TaskHubGrpcWorker(host_address="localhost:1")
    converters: list[Any] = []
    for function in app.get_functions():
        name = function.get_function_name()
        assert name is not None
        registered: Any = function.get_user_function()
        trigger = function.get_trigger()
        kind = trigger.get_binding_name() if trigger is not None else None
        if kind == "orchestrationTrigger":
            native._registry.add_named_orchestrator(name, wrap_orchestrator(registered.orchestrator_function))
            converters.append(_function_worker(registered)._data_converter)
        elif kind == "entityTrigger":
            entity = registered.entity_function
            native._registry.add_entity(wrap_entity(entity), name=task.get_entity_name(entity).lower())
            converters.append(_function_worker(registered)._data_converter)
        elif kind == "activityTrigger":
            native._registry.add_named_activity(name, _host_activity(registered))
    assert converters and all(isinstance(converter, _JsonPayloadConverter) for converter in converters)
    native._data_converter = converters[0]
    return native


def _af_host(workflow: Workflow) -> Any:
    """Worker factory for shared DT replay trials, hosting ``workflow`` on a Functions app."""
    return _af_worker(AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2"))
