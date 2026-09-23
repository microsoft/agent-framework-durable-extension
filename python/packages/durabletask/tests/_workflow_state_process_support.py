# Copyright (c) Microsoft. All rights reserved.

"""Registered activity producers for cross-process state tests, without pytest startup."""

import json
import logging
import os
import sys
from importlib.metadata import version
from pathlib import Path
from typing import Any

import agent_framework
from agent_framework import Executor, Workflow, WorkflowBuilder, WorkflowContext, WorkflowExecutor, handler
from durabletask.worker import TaskHubGrpcWorker, _ActivityExecutor
from typing_extensions import Never

import agent_framework_durabletask
from agent_framework_durabletask import DurableAIAgentWorker
from agent_framework_durabletask._workflows import activity
from agent_framework_durabletask._workflows.serialization import serialize_value

SET_MEMBERS = tuple(f"member-{index:02d}" for index in range(64))


def process_identity() -> dict[str, str]:
    return {
        "python": str(Path(sys.executable).resolve()),
        "core_version": version("agent-framework-core"),
        "core": str(Path(agent_framework.__file__).resolve()),
        "durable": str(Path(agent_framework_durabletask.__file__).resolve()),
        "activity": str(Path(activity.__file__).resolve()),
    }


def state_workflow(*, mixed: bool) -> tuple[Workflow, dict[str, Any]]:
    observed: dict[str, Any] = {}

    class Seed(Executor):
        @handler(input=str, output=str)
        async def handle(self, message: str, ctx: WorkflowContext[str]) -> None:
            ctx.set_state("x", set(SET_MEMBERS))
            ctx.set_state("keep", [False, 0])
            await ctx.send_message("go")

    class Writer(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            ctx.set_state("x", "sibling-write")

    class Reader(Executor):
        @handler(input=str, output=str)
        async def handle(self, message: str, ctx: WorkflowContext[str]) -> None:
            value = ctx.get_state("x")
            assert type(value) is set and value == set(SET_MEMBERS)
            observed["members"] = sorted(value)
            observed["encoding"] = serialize_value(value)
            await ctx.send_message("observe")

    class Sink(Executor):
        @handler(input=str, workflow_output=dict)
        async def handle(self, message: str, ctx: WorkflowContext[Never, dict]) -> None:
            value = ctx.get_state("x")
            await ctx.yield_output({
                "kind": type(value).__name__,
                "value": sorted(value) if type(value) is set else value,
                "keep": ctx.get_state("keep"),
            })

    seed, writer, reader, sink = Seed(id="seed"), Writer(id="writer"), Reader(id="reader"), Sink(id="sink")
    targets: list[Executor] = [writer, reader]
    if mixed:

        class Child(Executor):
            @handler(input=str, workflow_output=str)
            async def handle(self, message: str, ctx: WorkflowContext[Never, str]) -> None:
                await ctx.yield_output("child")

        child = Child(id="child")
        inner = WorkflowBuilder(name="state-process-child", start_executor=child, output_from=[child]).build()
        targets.append(WorkflowExecutor(inner, id="sub"))
    workflow = (
        WorkflowBuilder(name="state-process", start_executor=seed, output_from=[sink])
        .add_fan_out_edges(seed, targets)
        .add_edge(reader, sink)
        .build()
    )
    return workflow, observed


def _execute_child_activity() -> None:
    request = json.load(sys.stdin)
    workflow, observed = state_workflow(mixed=request["mixed"])
    native = TaskHubGrpcWorker(host_address="localhost:1")
    DurableAIAgentWorker(native, deployment_mode="isolated_v2").configure_workflow(workflow)
    wire = _ActivityExecutor(native._registry, logging.getLogger(__name__), native._data_converter).execute(
        "root", request["name"], request["task_id"], request["input"]
    )
    assert type(wire) is str
    sys.stdout.write(
        json.dumps({
            "wire": wire,
            "observed": observed,
            "identity": process_identity(),
            "pid": os.getpid(),
            "hash_seed": os.environ["PYTHONHASHSEED"],
        })
    )


if __name__ == "__main__":
    _execute_child_activity()
