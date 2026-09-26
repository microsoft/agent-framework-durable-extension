# Copyright (c) Microsoft. All rights reserved.

"""Registered workflow transport and HITL lifecycle graph fixtures."""

import json
from collections.abc import Generator
from copy import deepcopy
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import _workflow_admission_test_support as admission
import pytest
from _workflow_protocol_test_support import _host
from agent_framework import Executor, Workflow, WorkflowBuilder, WorkflowContext, handler, response_handler
from durabletask.task import CompletableTask
from typing_extensions import Never

from agent_framework_durabletask._json_payload import JsonPayload


@dataclass
class _Run:
    generator: Generator[Any, Any, Any]
    host: Any
    calls: list[dict[str, Any]]
    completion: Any = None
    waiting: Any = None
    output: Any = None
    done: bool = False

    def advance(self, value: Any = None) -> None:
        for _ in range(128):
            try:
                task = self.generator.send(value)
            except StopIteration as completed:
                self.output, self.done, self.waiting = completed.value, True, None
                if self.completion is not None:
                    self.completion.complete(deepcopy(self.output))
                return
            assert hasattr(task, "is_complete"), "Expected a real SDK Task"
            if not task.is_complete:
                self.waiting = task
                return
            value = task.get_result()
        pytest.fail("Exceeded the bounded transport's 128-yield limit")


class _Transport:
    def __init__(self, *, replay: bool = False) -> None:
        self.replay = replay
        self.runs: dict[str, _Run] = {}
        self.hosts: dict[str, Any] = {}
        self.names: dict[str, str] = {}
        self.events: dict[str, dict[str, list[Any]]] = {}
        self.child_ids: list[str] = []
        self.activity_results: list[dict[str, Any]] = []

    def host(
        self,
        calls: Any,
        result: Any,
        *,
        functions: Any,
        instance_id: str = "root-run",
        parent_instance_id: str | None = None,
    ) -> Any:
        def activity(name: str, payload: dict[str, Any]) -> dict[str, Any]:
            # Observe the real registered activity, without fabricating admission.
            outcome = result(name, payload)
            self.activity_results.append(deepcopy(outcome))
            return outcome

        host = _host(
            calls,
            activity,
            functions=functions,
            instance_id=instance_id,
            parent_instance_id=parent_instance_id,
            replay=self.replay,
        )
        self.hosts[instance_id] = host
        events = self.events.setdefault(instance_id, {})

        def wait(name: str, *, data_type: Any) -> Any:
            assert data_type is JsonPayload
            task: CompletableTask[Any] = CompletableTask()
            events.setdefault(name, []).append(task)
            return task

        def child(name: str, *, input: Any, instance_id: str, return_type: Any) -> Any:
            assert return_type is JsonPayload
            assert instance_id not in self.runs and name in functions
            wire = json.loads(json.dumps(input, allow_nan=False))
            calls.append({"kind": "child", "instance": instance_id, "name": name, "input": deepcopy(wire)})
            child_host = self.host(
                calls, result, functions=functions, instance_id=instance_id, parent_instance_id=host.instance_id
            )
            completion: CompletableTask[Any] = CompletableTask()
            run = _Run(functions[name](child_host, wire), child_host, calls, completion)
            self.runs[instance_id], self.names[instance_id] = run, name
            self.child_ids.append(instance_id)
            run.advance()
            return completion

        host.wait_for_external_event.side_effect = wait
        host.call_sub_orchestrator.side_effect = child
        return host

    def start(self, workflow: Workflow, entity_call: Any = None) -> _Run:
        with patch.object(admission, "_host", side_effect=self.host):
            generator, host, calls = admission._registered_run(workflow, entity_call)
        run = _Run(generator, host, calls)
        self.runs["root-run"], self.names["root-run"] = run, f"dafx-{workflow.name}"
        run.advance()
        return run

    def state(self, instance_id: str) -> Any:
        host = self.hosts.get(instance_id)
        if host is None:
            return None
        return SimpleNamespace(
            name=self.names[instance_id],
            serialized_custom_status=json.dumps(host.statuses[-1] if host.statuses else {}),
        )

    def event(self, name: str, value: Any, *, instance_id: str = "root-run") -> None:
        task = self.events[instance_id][name][-1]
        assert not task.is_complete, "Only deliver to the exact outstanding event wait"
        task.complete(deepcopy(value))
        self.pump()

    def pump(self) -> None:
        for _ in range(128):
            ready = [run for run in self.runs.values() if run.waiting is not None and run.waiting.is_complete]
            if not ready:
                return
            for run in ready:
                run.advance(run.waiting.get_result())
        pytest.fail("Exceeded the bounded transport's 128-resume limit")

    def close(self) -> None:
        for run in self.runs.values():
            run.generator.close()


def _typed_workflow(requested: type) -> tuple[Workflow, list[Any]]:
    seen: list[Any] = []

    class Gate(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            await ctx.request_info("decision", response_type=requested, request_id="approval")

        @response_handler(request=str, response=object, workflow_output=dict)
        async def answer(self, original_request: str, response: Any, ctx: WorkflowContext[Never, dict]) -> None:
            assert original_request == "decision" and ctx.request_id == "approval"
            seen.append(response)
            await ctx.yield_output({"value": response, "type": type(response).__name__})

    gate = Gate(id="gate")
    return WorkflowBuilder(name="lifecycle-types", start_executor=gate, output_from=[gate]).build(), seen


def _siblings() -> tuple[Workflow, list[tuple[str, Any]]]:
    seen: list[tuple[str, Any]] = []

    class Seed(Executor):
        @handler(input=str, output=str)
        async def handle(self, message: str, ctx: WorkflowContext[str]) -> None:
            await ctx.send_message("ask")

    # One broad handler handles qA's follow-up as well as its first reply.
    class A(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            await ctx.request_info("initial", response_type=str, request_id="qA")

        @response_handler(request=str, response=object, workflow_output=dict)
        async def answer(self, original_request: str, response: Any, ctx: WorkflowContext[Never, dict]) -> None:
            assert ctx.request_id is not None
            seen.append((ctx.request_id, response))
            if ctx.request_id == "qA":
                await ctx.yield_output({"answer_for_qB": f"derived-{response}"})
                await ctx.request_info("follow-up", response_type=str, request_id="qC")
            else:
                assert ctx.request_id == "qC" and original_request == "follow-up"

    class B(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            await ctx.request_info("other", response_type=str, request_id="qB")

        @response_handler(request=str, response=str, workflow_output=dict)
        async def answer(self, original_request: str, response: str, ctx: WorkflowContext[Never, dict]) -> None:
            seen.append(("qB", response))
            await ctx.yield_output({"qB": response})

    seed, a, b = Seed(id="seed"), A(id="a"), B(id="b")
    workflow = (
        WorkflowBuilder(name="lifecycle-siblings", start_executor=seed, output_from=[a, b])
        .add_fan_out_edges(seed, [a, b])
        .build()
    )
    return workflow, seen
