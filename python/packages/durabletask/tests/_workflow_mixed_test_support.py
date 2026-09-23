# Copyright (c) Microsoft. All rights reserved.

"""Mixed local and child HITL graph fixtures."""

from typing import Any

from agent_framework import (
    Executor,
    Workflow,
    WorkflowBuilder,
    WorkflowContext,
    WorkflowExecutor,
    handler,
    response_handler,
)
from typing_extensions import Never


def _mixed(
    *, nested: bool = False, child_first: bool = False, twice: bool = False, sibling_request: bool = False
) -> tuple[Workflow, dict[str, Any]]:
    controls: dict[str, Any] = {"seen": [], "snapshots": [], "send": None, "joined_routing": True}

    class Seed(Executor):
        @handler(input=str, output=str)
        async def handle(self, message: str, ctx: WorkflowContext[str]) -> None:
            await ctx.send_message(message)
            if twice:
                await ctx.send_message(message, target_id="sub")

    class Parent(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            ctx.set_state("conflict", "parent")
            await ctx.request_info("parent", response_type=int, request_id="a")
            if sibling_request:
                await ctx.request_info("sibling", response_type=int, request_id="d")

        @response_handler(request=str, response=int, output=dict, workflow_output=dict)
        async def answer(self, original_request: str, response: int, ctx: WorkflowContext[dict, dict]) -> None:
            controls["seen"].append((ctx.request_id, response))
            controls["snapshots"].append(ctx.get_state("conflict"))
            if ctx.request_id == "a":
                assert ctx.get_state("conflict") == "writer"
                ctx.set_state("conflict", "reply")
                if controls["send"] is not None:
                    controls["send"](response + 1)
                await ctx.request_info("follow-up", response_type=int, request_id="c")
            else:
                assert ctx.request_id in ("c", "d") and ctx.get_state("conflict") == "reply"
            await ctx.yield_output({ctx.request_id: response})
            await ctx.send_message({ctx.request_id: response})

    class Writer(Executor):
        @handler(input=str, output=dict, workflow_output=dict)
        async def handle(self, message: str, ctx: WorkflowContext[dict, dict]) -> None:
            ctx.set_state("conflict", "writer")
            await ctx.yield_output({"writer": 1})
            await ctx.send_message({"writer": 1})

    class Child(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            await ctx.request_info("child", response_type=int, request_id="b")

        @response_handler(request=str, response=int, workflow_output=dict)
        async def answer(self, original_request: str, response: int, ctx: WorkflowContext[Never, dict]) -> None:
            controls["seen"].append(("b", response))
            await ctx.yield_output({"b": response})

    class Sink(Executor):
        @handler(input=dict, workflow_output=dict)
        async def handle(self, message: dict, ctx: WorkflowContext[Never, dict]) -> None:
            if controls["joined_routing"]:
                assert ctx.get_state("conflict") == ("writer" if "writer" in message else "reply")
            await ctx.yield_output({"sink": message})

    child = Child(id="child")
    inner = WorkflowBuilder(name="mixed-leaf", start_executor=child, output_from=[child]).build()
    if nested:
        leaf = WorkflowExecutor(inner, id="leaf", propagate_request=True, allow_direct_output=True)
        inner = WorkflowBuilder(name="mixed-middle", start_executor=leaf, output_from=[leaf]).build()
    sub = WorkflowExecutor(inner, id="sub", propagate_request=True, allow_direct_output=True)
    seed, parent, writer, sink = Seed(id="seed"), Parent(id="parent"), Writer(id="writer"), Sink(id="sink")
    targets = [sub, parent, writer] if child_first else [parent, writer, sub]
    workflow = (
        WorkflowBuilder(name="mixed-root", start_executor=seed, output_from=[parent, writer, sub, sink])
        .add_fan_out_edges(seed, targets)
        .add_edge(parent, sink)
        .add_edge(writer, sink)
        .build()
    )
    return workflow, controls
