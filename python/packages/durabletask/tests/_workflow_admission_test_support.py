# Copyright (c) Microsoft. All rights reserved.

"""Registered workflow dispatch and graph fixtures for admission tests."""

import json
from collections.abc import Callable, Generator
from copy import deepcopy
from itertools import count
from typing import Any
from unittest.mock import Mock

import pytest
from _workflow_protocol_test_support import _host
from _workflow_test_support import create_registration_worker
from agent_framework import Executor, Workflow, WorkflowBuilder, WorkflowContext, handler, response_handler
from typing_extensions import Never

from agent_framework_durabletask import DurableAIAgentWorker, wrap_workflow_input
from agent_framework_durabletask._json_payload import JsonPayload


async def _run_core_workflow(workflow: Workflow) -> None:
    await workflow.run("go")


def _registered_run(
    workflow: Workflow, entity_call: Callable[..., Any] | None = None
) -> tuple[Generator[Any, Any, Any], Mock, list[dict[str, Any]]]:
    """Use existing SDK tasks with real registered activities and orchestration closures."""
    native = create_registration_worker()
    DurableAIAgentWorker(native, deployment_mode="isolated_v2").configure_workflow(workflow)
    activities = {call.args[0].__name__: call.args[0] for call in native.add_activity.call_args_list}
    functions = {call.args[0].__name__: call.args[0] for call in native.add_orchestrator.call_args_list}
    calls: list[dict[str, Any]] = []

    def activity(name: str, payload: dict[str, Any]) -> dict[str, Any]:
        return json.loads(activities[name](None, json.dumps(payload, allow_nan=False)))

    def call_entity(entity_id: Any, operation: str, request: dict[str, Any], *, return_type: Any) -> Any:
        assert return_type is JsonPayload
        if entity_call is None:
            raise AssertionError("Unexpected agent dispatch")
        return entity_call(entity_id, operation, request)

    host = _host(calls, activity, functions=functions)
    ordinals = count()
    host.new_uuid.side_effect = lambda: f"correlation-{next(ordinals)}"
    host.call_entity.side_effect = call_entity
    return functions[f"dafx-{workflow.name}"](host, wrap_workflow_input("go")), host, calls


class _Relay(Executor):
    def __init__(self, name: str, *, target: str | None = None, emissions: int = 1) -> None:
        self.target, self.emissions = target, emissions
        super().__init__(id=name)

    @handler(input=str, output=str)
    async def handle(self, message: str, ctx: WorkflowContext[str]) -> None:
        for _ in range(self.emissions):
            await ctx.send_message(self.id, target_id=self.target)


class _Sink(Executor):
    def __init__(self, name: str = "sink") -> None:
        self.seen: list[dict[str, Any]] = []
        super().__init__(id=name)

    @handler(input=str | list[str], workflow_output=dict)
    async def handle(self, message: Any, ctx: WorkflowContext[Never, dict[str, Any]]) -> None:
        sources = list(ctx.source_executor_ids)
        if len(sources) > 1:
            with pytest.raises(RuntimeError):
                ctx.get_source_executor_id()
        else:
            assert ctx.get_source_executor_id() == sources[0]
        row = {"payload": deepcopy(message), "sources": sources}
        self.seen.append(row)
        await ctx.yield_output(row)


def _hitl_workflow() -> Workflow:
    class Gate(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            await ctx.request_info("decision", response_type=object, request_id="approval")

        @response_handler(request=str, response=object, workflow_output=dict)
        async def answer(
            self, original_request: str, response: Any, ctx: WorkflowContext[Never, dict[str, Any]]
        ) -> None:
            assert original_request == "decision" and ctx.request_id == "approval"
            await ctx.yield_output({"response": response, "response_type": type(response).__name__})

    gate = Gate(id="gate")
    return WorkflowBuilder(name="hitl-admission", start_executor=gate, output_from=[gate]).build()
