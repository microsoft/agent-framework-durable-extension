# Copyright (c) Microsoft. All rights reserved.

"""HITL lifecycle controls using Core and registered closures with real SDK tasks.

The transport only completes actual waits and child tasks. It does not implement
workflow routing, admission, request storage or response-handler execution.
These are in-process tests, not service history replay or crash-recovery tests.
"""

import asyncio
import json
from collections.abc import Generator
from copy import deepcopy
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest
import test_workflow_admission_review as admission
from _execution_test_support import RecordingChatClient
from agent_framework import (
    Agent,
    AgentExecutor,
    AgentResponse,
    Content,
    Executor,
    Message,
    Workflow,
    WorkflowBuilder,
    WorkflowContext,
    WorkflowExecutor,
    handler,
    response_handler,
)
from durabletask.client import TaskHubGrpcClient
from durabletask.task import CompletableTask
from pydantic import BaseModel
from test_workflow_protocol_review import _complete, _host
from typing_extensions import Never

from agent_framework_durabletask import DurableWorkflowClient, serialize_agent_response
from agent_framework_durabletask._workflows import orchestrator as engine
from agent_framework_durabletask._workflows.serialization import deserialize_workflow_output


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

        def wait(name: str) -> Any:
            task: CompletableTask[Any] = CompletableTask()
            events.setdefault(name, []).append(task)
            return task

        def child(name: str, *, input: Any, instance_id: str) -> Any:
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


@pytest.mark.parametrize(
    ("requested", "answer", "valid", "correction"),
    [
        (int, 123, True, 456),
        (int, True, True, 456),  # Core assignability includes bool as an int subclass.
        (int, "123", False, 456),
        (int, "not-an-int", False, 456),
        (int, 1.0, False, 456),
        (int, None, False, 456),
        (str, "123", True, "corrected"),
        (str, "null", True, "corrected"),
        (str, '"quoted"', True, "corrected"),
        (str, '{"approved":true}', True, "corrected"),
        (str, 123, False, "corrected"),
        (object, None, True, "corrected"),
        (object, {"type": "business", "response_type": "os:system", "_durable_agent_response": 99}, True, None),
    ],
)
def test_recorded_type_controls_admission_before_broad_handler(
    requested: type, answer: Any, valid: bool, correction: Any
) -> None:
    async def core_trial() -> tuple[list[Any], list[Any]]:
        workflow, seen = _typed_workflow(requested)
        start = await workflow.run("go")
        assert start.get_request_info_events()[0].response_type is requested
        if valid:
            await workflow.run(responses={"approval": deepcopy(answer)})
            return seen, seen
        with pytest.raises((TypeError, ValueError), match="Response type mismatch"):
            await workflow.run(responses={"approval": deepcopy(answer)})
        assert not seen
        assert set(await workflow._runner.context.get_pending_request_info_events()) == {"approval"}
        rejected = list(seen)
        await workflow.run(responses={"approval": deepcopy(correction)})
        return rejected, seen

    oracle, final_oracle = asyncio.run(core_trial())
    assert oracle == ([answer] if valid else [])
    workflow, seen = _typed_workflow(requested)
    transport = _Transport()
    try:
        run = transport.start(workflow)
        pending = deepcopy(run.host.statuses[-1]["pending_requests"])
        native = Mock(spec=TaskHubGrpcClient)
        native.get_orchestration_state.side_effect = transport.state
        DurableWorkflowClient(native).send_hitl_response("root-run", "approval", deepcopy(answer))
        wire = native.raise_orchestration_event.call_args.kwargs["data"]
        assert wire == answer and type(wire) is type(answer)
        # This also covers the direct event boundary, not only a client-side check.
        transport.event("approval", wire)
        assert seen == oracle
        if not valid:
            # Rejection is checkpointed by the registered response activity,
            # not decided synchronously by the event-wait generator.
            assert not run.done and len(run.calls) == len(transport.activity_results) == 2
            assert transport.activity_results[-1] == {
                "hitl_admission": {"request_id": "approval", "status": "invalidreply"},
                "sent_messages": [],
                "outputs": [],
                "events": [],
                "shared_state_updates": {},
                "shared_state_deletes": [],
                "pending_request_info_events": [],
            }
            assert run.host.statuses[-1]["pending_requests"] == pending
            transport.event("approval", correction)
            assert seen == [correction]
            assert len(transport.events["root-run"]["approval"]) == 2
        assert run.done and len(run.calls) == len(transport.activity_results) == (2 if valid else 3)
        assert transport.activity_results[-1]["hitl_admission"] == {
            "request_id": "approval",
            "status": "accepted",
        }
        assert not run.host.statuses[-1].get("pending_requests")
        assert seen == final_oracle
        expected = answer if valid else correction
        assert type(seen[0]) is type(expected)
        assert deserialize_workflow_output(run.output) == [{"value": expected, "type": type(expected).__name__}]
    finally:
        transport.close()


class _Decision(BaseModel):
    approved: bool
    note: str


def test_declared_model_reconstructs_but_invalid_mapping_keeps_request() -> None:
    async def core_trial() -> list[Any]:
        workflow, seen = _typed_workflow(_Decision)
        await workflow.run("go")
        with pytest.raises((TypeError, ValueError), match="Response type mismatch"):
            await workflow.run(responses={"approval": {"approved": True}})
        assert not seen
        await workflow.run(responses={"approval": {"approved": True, "note": "123"}})
        return seen

    oracle = asyncio.run(core_trial())
    workflow, seen = _typed_workflow(_Decision)
    transport = _Transport()
    try:
        run = transport.start(workflow)
        pending = deepcopy(run.host.statuses[-1]["pending_requests"])
        for invalid, activity_count in (
            ({"approved": True}, 2),
            ({"__type__": "os:system"}, 3),  # Raw malformed envelopes also checkpoint rejection.
            ({"approved": True, "note": None}, 4),
        ):
            transport.event("approval", invalid)
            assert not run.done and not seen
            assert len(run.calls) == len(transport.activity_results) == activity_count
            assert transport.activity_results[-1] == {
                "hitl_admission": {"request_id": "approval", "status": "invalidreply"},
                "sent_messages": [],
                "outputs": [],
                "events": [],
                "shared_state_updates": {},
                "shared_state_deletes": [],
                "pending_request_info_events": [],
            }
            assert run.host.statuses[-1]["pending_requests"] == pending
        transport.event("approval", {"approved": True, "note": "123"})
        assert run.done and seen == oracle == [_Decision(approved=True, note="123")]
        assert len(run.calls) == len(transport.activity_results) == 5
        assert len(transport.events["root-run"]["approval"]) == 4
        assert transport.activity_results[-1]["hitl_admission"]["status"] == "accepted"
        assert not run.host.statuses[-1].get("pending_requests")
        assert type(seen[0]) is _Decision
    finally:
        transport.close()


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


@pytest.mark.parametrize("first", ["qA", "qB"])
def test_ready_handler_runs_without_other_reply_and_old_wait_survives(first: str) -> None:
    async def core_trial() -> list[tuple[str, Any]]:
        workflow, seen = _siblings()
        await workflow.run("go")
        resumed = await workflow.run(responses={first: "one"})
        assert seen == [(first, "one")]
        if first == "qA":
            assert {"answer_for_qB": "derived-one"} in resumed.get_outputs()
            await workflow.run(responses={"qC": "follow-up"})
            pending = await workflow._runner_context.get_pending_request_info_events()
            assert set(pending) == {"qB"}
            await workflow.run(responses={"qB": "derived-one"})
        else:
            await workflow.run(responses={"qA": "one"})
            await workflow.run(responses={"qC": "follow-up"})
        return seen

    oracle = asyncio.run(core_trial())
    executions: list[tuple[list[dict[str, Any]], list[str], list[dict[str, Any]]]] = []
    for replay in (False, True):
        transport = _Transport(replay=replay)
        workflow, seen = _siblings()
        try:
            run = transport.start(workflow)
            assert set(transport.events["root-run"]) == {"qA", "qB"}
            older = transport.events["root-run"]["qB"][-1]
            transport.event(first, "one")
            assert seen == [(first, "one")] and not run.done
            if first == "qA":
                assert set(run.host.statuses[-1]["pending_requests"]) == {"qB", "qC"}
                outputs = [event.get("data") for event in run.host.statuses[-1]["events"]]
                assert {"answer_for_qB": "derived-one"} in outputs
                transport.event("qC", "follow-up")
                assert not run.done and transport.events["root-run"]["qB"] == [older]
                assert set(run.host.statuses[-1]["pending_requests"]) == {"qB"}
                transport.event("qB", "derived-one")
            else:
                transport.event("qA", "one")
                transport.event("qC", "follow-up")
            assert run.done and seen == oracle
            executions.append((
                run.calls,
                [call.args[0] for call in run.host.wait_for_external_event.call_args_list],
                deepcopy(run.host.statuses),
            ))
        finally:
            transport.close()
    assert executions[0] == executions[1]


def test_invalid_first_reply_does_not_block_valid_sibling_or_replace_its_wait() -> None:
    workflow, seen = _siblings()
    transport = _Transport()
    try:
        run = transport.start(workflow)
        older = transport.events["root-run"]["qB"][-1]
        transport.event("qA", 123)
        assert not seen and set(run.host.statuses[-1]["pending_requests"]) == {"qA", "qB"}
        assert transport.events["root-run"]["qB"] == [older]
        transport.event("qB", "independent")
        assert seen == [("qB", "independent")]
        transport.event("qA", "fixed")
        transport.event("qC", "follow-up")
        assert run.done and seen == [("qB", "independent"), ("qA", "fixed"), ("qC", "follow-up")]
        assert len(transport.events["root-run"]["qA"]) == 2
    finally:
        transport.close()


def test_already_ready_sibling_reply_is_not_lost_during_handler_iteration() -> None:
    workflow, seen = _siblings()
    transport = _Transport()
    try:
        run = transport.start(workflow)
        a, b = transport.events["root-run"]["qA"][0], transport.events["root-run"]["qB"][0]
        a.complete("one")
        b.complete("already-ready")
        transport.pump()
        assert seen == [("qA", "one"), ("qB", "already-ready")]
        assert set(run.host.statuses[-1]["pending_requests"]) == {"qC"}
        assert transport.events["root-run"]["qB"] == [b]
        transport.event("qC", "follow-up")
        assert run.done
    finally:
        transport.close()


def test_new_request_cannot_overwrite_an_unanswered_sibling() -> None:
    original = engine.PendingHITLRequest("qB", "b", "original", "builtins:str", "builtins:str")
    pending = {"qB": original}
    result = engine.ExecutorResult(
        executor_id="a",
        output_message=None,
        activity_result={"pending_request_info_events": [{"request_id": "qB", "data": "replacement"}]},
        task_type=engine.TaskType.ACTIVITY,
    )
    with pytest.raises(ValueError, match="collides with an outstanding"):
        engine._collect_hitl_requests(result, pending)
    assert pending == {"qB": original} and pending["qB"].request_data == "original"


def test_response_type_fields_in_data_never_select_a_constructor(monkeypatch: pytest.MonkeyPatch) -> None:
    workflow, seen = _typed_workflow(object)
    transport = _Transport()
    resolver = Mock(wraps=engine.resolve_type)
    monkeypatch.setattr(engine, "resolve_type", resolver)
    answer = {"response_type": "os:system", "type": "business", "_durable_agent_response": 99}
    try:
        run = transport.start(workflow)
        transport.event("approval", answer)
        assert run.done and seen == [answer]
        assert resolver.call_args_list
        assert all(call.args == ("builtins:object",) for call in resolver.call_args_list)
    finally:
        transport.close()


def _child_loop(*, nested: bool) -> tuple[Workflow, list[dict[str, Any]]]:
    seen: list[dict[str, Any]] = []

    class Seed(Executor):
        @handler(input=str, output=dict)
        async def handle(self, message: str, ctx: WorkflowContext[dict]) -> None:
            await ctx.send_message({"round": 1})

    class Gate(Executor):
        @handler(input=dict)
        async def handle(self, message: dict[str, Any], ctx: WorkflowContext) -> None:
            await ctx.request_info(message, response_type=dict, request_id="approval")

        @response_handler(request=dict, response=dict, workflow_output=dict)
        async def answer(self, original_request: dict, response: dict, ctx: WorkflowContext[Never, dict]) -> None:
            row = {"round": original_request["round"], "answer": response}
            seen.append(deepcopy(row))
            await ctx.yield_output(row)

    class Loop(Executor):
        @handler(input=dict, output=dict, workflow_output=dict)
        async def handle(self, message: dict, ctx: WorkflowContext[dict, dict]) -> None:
            if message["round"] == 1:
                await ctx.send_message({"round": 2})
            else:
                await ctx.yield_output(message)

    gate = Gate(id="gate")
    inner = WorkflowBuilder(name="lifecycle-leaf", start_executor=gate, output_from=[gate]).build()
    if nested:

        class Output(Executor):
            @handler(input=dict, workflow_output=dict)
            async def handle(self, message: dict, ctx: WorkflowContext[Never, dict]) -> None:
                await ctx.yield_output(message)

        leaf = WorkflowExecutor(inner, id="leaf", propagate_request=True)
        output = Output(id="output")
        inner = (
            WorkflowBuilder(name="lifecycle-middle", start_executor=leaf, output_from=[output])
            .add_edge(leaf, output)
            .build()
        )
    sub = WorkflowExecutor(inner, id="sub", propagate_request=True)
    seed, loop = Seed(id="seed"), Loop(id="loop")
    workflow = (
        WorkflowBuilder(name="lifecycle-parent", start_executor=seed, output_from=[loop], max_iterations=24)
        .add_edge(seed, sub)
        .add_edge(sub, loop)
        .add_edge(loop, sub)
        .build()
    )
    return workflow, seen


@pytest.mark.parametrize("nested", [False, True])
def test_repeated_child_retires_public_path_and_keeps_host_prefix_in_sync(nested: bool) -> None:
    workflow, seen = _child_loop(nested=nested)
    transport = _Transport()
    native = Mock(spec=TaskHubGrpcClient)
    native.get_orchestration_state.side_effect = transport.state
    client = DurableWorkflowClient(native)

    def send(request_id: str, answer: dict) -> None:
        client.send_hitl_response("root-run", request_id, answer)
        call = native.raise_orchestration_event.call_args
        transport.event(call.kwargs["event_name"], call.kwargs["data"], instance_id=call.args[0])

    try:
        root = transport.start(workflow)
        first = client.get_pending_hitl_requests("root-run")
        first_id = "sub~0~leaf~0~approval" if nested else "sub~0~approval"
        assert [request["request_id"] for request in first] == [first_id]
        send(first_id, {"for": 1})
        second = client.get_pending_hitl_requests("root-run")
        second_id = "sub~1~leaf~0~approval" if nested else "sub~1~approval"
        assert [request["request_id"] for request in second] == [second_id]
        assert root.host.statuses[-1]["subworkflows"] == {"sub": {"1": "root-run::sub::1"}}
        native.raise_orchestration_event.reset_mock()
        with pytest.raises(ValueError, match="No active sub-workflow"):
            client.send_hitl_response("root-run", first_id, {"for": 1})
        native.raise_orchestration_event.assert_not_called()
        assert seen == [{"round": 1, "answer": {"for": 1}}] and not root.done
        send(second_id, {"for": 2})
        assert root.done and seen == [
            {"round": 1, "answer": {"for": 1}},
            {"round": 2, "answer": {"for": 2}},
        ]
        assert client.get_pending_hitl_requests("root-run") == []
        addresses = [
            call["input"]["host_context"]
            for call in root.calls
            if call["kind"] == "activity" and call["name"] == "dafx-lifecycle-leaf-gate"
        ]
        assert [address["request_path_prefix"] + "approval" for address in addresses] == [
            first_id,
            first_id,
            second_id,
            second_id,
        ]
        assert all(address["instance_id"] == "root-run" for address in addresses)
        assert all(address["workflow_name"] == "lifecycle-parent" for address in addresses)
    finally:
        transport.close()


def test_global_child_ordinal_spans_other_nodes_and_supersteps() -> None:
    workflow, _ = _child_loop(nested=False)
    sub = workflow.executors["sub"]
    assert isinstance(sub, WorkflowExecutor)
    other = WorkflowExecutor(sub.workflow, id="other", propagate_request=True)
    workflow.executors["other"] = other
    ctx = Mock(instance_id="root-run")
    counter = [0]
    address = {"root_instance_id": "root-run", "root_workflow_name": workflow.name, "request_path_prefix": ""}
    _, first, _ = engine._prepare_all_tasks(ctx, workflow, {"other": [({}, "seed")]}, {}, counter, address)
    _, second, _ = engine._prepare_all_tasks(ctx, workflow, {"sub": [({}, "seed"), ({}, "seed")]}, {}, counter, address)
    assert engine._index_subworkflows(first) == {"other": {"0": "root-run::other::0"}}
    assert engine._index_subworkflows(second) == {"sub": {"1": "root-run::sub::1", "2": "root-run::sub::2"}}
    prefixes = [
        call.args[1]["input"]["__subworkflow_address__"]["request_path_prefix"]
        for call in ctx.call_sub_orchestrator.call_args_list
    ]
    assert prefixes == ["other~0~", "sub~1~", "sub~2~"]


@pytest.mark.parametrize("children", [["new-child"], {"1": "new-child"}])
def test_old_slot_and_retired_path_never_route_to_current_child(children: Any) -> None:
    native = Mock(spec=TaskHubGrpcClient)
    statuses = {
        "root-run": {"subworkflows": {"sub": children}},
        "new-child": {"pending_requests": {"approval": {"request_id": "approval"}}},
    }
    native.get_orchestration_state.side_effect = lambda instance: SimpleNamespace(
        serialized_custom_status=json.dumps(statuses[instance])
    )
    client = DurableWorkflowClient(native)
    with pytest.raises(ValueError, match="No active sub-workflow"):
        client.send_hitl_response("root-run", "sub~0~approval", True)
    native.raise_orchestration_event.assert_not_called()
    assert [item["request_id"] for item in client.get_pending_hitl_requests("root-run")] == (
        [] if isinstance(children, list) else ["sub~1~approval"]
    )


@pytest.mark.parametrize("order", [("first", "second"), ("second", "first"), tuple(f"approval-{i}" for i in range(12))])
def test_agent_approval_ledger_still_waits_for_every_approval(order: tuple[str, ...]) -> None:
    agent = AgentExecutor(Agent(client=RecordingChatClient(), name="agent"), id="agent")
    workflow = WorkflowBuilder(
        name="lifecycle-agent", start_executor=agent, output_from=[agent], max_iterations=3
    ).build()
    approvals = {
        key: Content.from_function_approval_request(key, Content.from_function_call(key, "tool"))
        for key in sorted(order)
    }
    requests: list[Any] = []

    def entity_call(entity_id: Any, operation: str, request: Any) -> Any:
        requests.append(deepcopy(request))
        contents = list(approvals.values()) if len(requests) == 1 else [Content.from_text("done")]
        return _complete(serialize_agent_response(AgentResponse(messages=[Message("assistant", contents)])))

    transport = _Transport()
    try:
        run = transport.start(workflow, entity_call)
        for key in order[:-1]:
            transport.event(key, approvals[key].to_function_approval_response(True).to_dict())
            assert len(requests) == 1 and not run.done
        transport.event(order[-1], approvals[order[-1]].to_function_approval_response(True).to_dict())
        assert run.done and len(requests) == 2
        contents = requests[1]["contextMessages"][0]["contents"]
        assert [item["id"] for item in contents] == list(order)
        assert all(item["type"] == "function_approval_response" for item in contents)
    finally:
        transport.close()


@pytest.mark.parametrize("edge_kind", ["single", "fanout", "selection"])
@pytest.mark.parametrize("explicit", [False, True])
def test_heterogeneous_routing_skips_nonhandlers_before_edge_condition(edge_kind: str, explicit: bool) -> None:
    observations = []
    for core in (True, False):
        seen: list[Any] = []
        predicate_calls: list[Any] = []

        class Seed(Executor):
            @handler(input=str, output=str | int)
            async def handle(self, message: str, ctx: WorkflowContext[str | int]) -> None:
                await ctx.send_message("word", target_id="text" if explicit else None)
                await ctx.send_message(123, target_id="number" if explicit else None)

        class Text(Executor):
            observations = seen

            @handler(input=str, workflow_output=str)
            async def handle(self, message: str, ctx: WorkflowContext[Never, str]) -> None:
                self.observations.append((self.id, message))
                await ctx.yield_output(message)

        class Number(Executor):
            observations = seen

            @handler(input=int, workflow_output=int)
            async def handle(self, message: int, ctx: WorkflowContext[Never, int]) -> None:
                self.observations.append((self.id, message))
                await ctx.yield_output(message)

        def text_only(value: Any, calls: list[Any] = predicate_calls) -> bool:
            assert isinstance(value, str), "An ineligible input must not reach the edge predicate"
            calls.append(value)
            return True

        seed, text, number = Seed(id="seed"), Text(id="text"), Number(id="number")
        builder = WorkflowBuilder(name="lifecycle-routing", start_executor=seed, output_from=[text, number])
        if edge_kind == "single":
            builder.add_edge(seed, text, condition=text_only).add_edge(seed, number)
        elif edge_kind == "fanout":
            builder.add_fan_out_edges(seed, [text, number])
        else:
            builder.add_multi_selection_edge_group(seed, [text, number], lambda value, targets: targets)
        workflow = builder.build()
        if core:
            asyncio.run(admission._run_core_workflow(workflow))
        else:
            transport = _Transport()
            try:
                run = transport.start(workflow)
                assert run.done and len(run.calls) == 3
            finally:
                transport.close()
        assert seen == [("text", "word"), ("number", 123)]
        assert predicate_calls == (["word"] if edge_kind == "single" else [])
        observations.append(seen)
    assert observations[0] == observations[1]


def test_fanin_eligibility_uses_list_payload_not_individual_item() -> None:
    for core in (True, False):
        seen: list[Any] = []

        class Seed(Executor):
            @handler(input=str, output=str)
            async def handle(self, message: str, ctx: WorkflowContext[str]) -> None:
                await ctx.send_message("go")

        class Source(Executor):
            @handler(input=str, output=str | int)
            async def handle(self, message: str, ctx: WorkflowContext[str | int]) -> None:
                await ctx.send_message(123)
                await ctx.send_message(self.id)

        class Sink(Executor):
            observations = seen

            @handler(input=list[str], workflow_output=list)
            async def handle(self, message: list[str], ctx: WorkflowContext[Never, list]) -> None:
                self.observations.append((message, list(ctx.source_executor_ids)))
                await ctx.yield_output(message)

        seed, left, right, sink = Seed(id="seed"), Source(id="left"), Source(id="right"), Sink(id="sink")
        workflow = (
            WorkflowBuilder(name="lifecycle-fanin-types", start_executor=seed, output_from=[sink])
            .add_fan_out_edges(seed, [left, right])
            .add_fan_in_edges([left, right], sink)
            .build()
        )
        if core:
            asyncio.run(admission._run_core_workflow(workflow))
        else:
            transport = _Transport()
            try:
                assert transport.start(workflow).done
            finally:
                transport.close()
        assert seen == [(["left", "right"], ["left", "right"])]


def _af_run(workflow: Workflow, *, replay: bool = False) -> Any:
    # Functions is an optional companion package when this test file is run alone.
    af = pytest.importorskip("agent_framework_azurefunctions")
    import azure.durable_functions as df
    from azure.durable_functions.models.actions.NoOpAction import NoOpAction
    from azure.durable_functions.models.ReplaySchema import ReplaySchema
    from azure.durable_functions.models.Task import AtomicTask, WhenAllTask, WhenAnyTask

    from agent_framework_durabletask import wrap_workflow_input

    app = af.AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")
    activities: dict[str, Any] = {}
    generator_function: Any = None
    respond: Any = None
    for function in app.get_functions():
        binding = function.get_trigger().get_dict_repr()
        user: Any = function.get_user_function()
        if binding["type"] == "activityTrigger":
            activities[function.get_function_name()] = user
        elif binding["type"] == "orchestrationTrigger":
            generator_function = user.orchestrator_function
        elif binding.get("route") == f"workflow/{workflow.name}/respond/{{instanceId}}/{{requestId}}":
            respond = user.client_function
    assert generator_function is not None and respond is not None
    host = Mock(spec=df.DurableOrchestrationContext)
    host.instance_id, host.is_replaying = "root-run", replay
    host.parent_instance_id = None
    host._input = json.dumps(wrap_workflow_input("go"))
    host.get_input.side_effect = AssertionError("Generated workflow starts must not use SDK custom decoding")
    host.task_all.side_effect = lambda tasks: WhenAllTask(tasks, ReplaySchema.V1)
    host.task_any.side_effect = lambda tasks: WhenAnyTask(tasks, ReplaySchema.V1)
    events: dict[str, list[Any]] = {}

    def wait(name: str) -> Any:
        task = AtomicTask(name, NoOpAction())
        events.setdefault(name, []).append(task)
        return task

    host.wait_for_external_event.side_effect = wait
    statuses: list[Any] = []
    calls: list[Any] = []
    host.set_custom_status.side_effect = lambda status: statuses.append(deepcopy(status))

    def activity(name: str, input: str) -> Any:
        calls.append((name, json.loads(input)))
        task = AtomicTask(0, NoOpAction())
        task.set_value(is_error=False, value=activities[name](input))
        return task

    host.call_activity.side_effect = activity
    client = AsyncMock(spec=df.DurableOrchestrationClient)
    client.get_status.side_effect = lambda instance: SimpleNamespace(
        name=f"dafx-{workflow.name}", custom_status=statuses[-1]
    )
    return SimpleNamespace(
        generator=generator_function(host),
        host=host,
        client=client,
        respond=respond,
        events=events,
        statuses=statuses,
        calls=calls,
    )


@pytest.mark.parametrize("answer", ["123", "null", '{"type":"business"}', 123, None])
def test_af_http_schedules_opaque_json_and_worker_validates_recorded_type(answer: Any) -> None:
    workflow, seen = _typed_workflow(str)
    run = _af_run(workflow)
    import azure.functions as func

    generator, client = run.generator, run.client
    try:
        batch = next(generator)
        assert batch.is_completed
        waiting = generator.send(batch.result)
        pending = deepcopy(run.statuses[-1]["pending_requests"])
        request = func.HttpRequest(
            method="POST",
            url=f"https://example.test/api/workflow/{workflow.name}/respond/root-run/approval",
            headers={"Content-Type": "application/json"},
            params={},
            route_params={"instanceId": "root-run", "requestId": "approval"},
            body=json.dumps(answer).encode("utf-8"),
        )
        response = asyncio.run(run.respond(request, client))
        assert response.status_code == 200
        wire = client.raise_event.await_args.kwargs["event_data"]
        assert wire == answer and type(wire) is type(answer)
        waiting.set_value(is_error=False, value=wire)
        task = generator.send(waiting.result)
        # The real registered activity has completed, but its admission outcome
        # has not been consumed by the orchestration yet.
        assert task.is_completed and len(run.calls) == 2
        assert len(task.result) == 1
        admission_result = json.loads(task.result[0])
        if not isinstance(answer, str):
            assert admission_result == {
                "hitl_admission": {"request_id": "approval", "status": "invalidreply"},
                "sent_messages": [],
                "outputs": [],
                "events": [],
                "shared_state_updates": {},
                "shared_state_deletes": [],
                "pending_request_info_events": [],
            }
            task = generator.send(task.result)
            assert not task.is_completed and not seen
            assert run.statuses[-1]["pending_requests"] == pending
            task.set_value(is_error=False, value="corrected")
            task = generator.send(task.result)
        assert task.is_completed
        assert json.loads(task.result[0])["hitl_admission"] == {"request_id": "approval", "status": "accepted"}
        with pytest.raises(StopIteration) as completed:
            generator.send(task.result)
        expected = answer if isinstance(answer, str) else "corrected"
        assert seen == [expected]
        assert deserialize_workflow_output(completed.value.value) == [{"value": expected, "type": "str"}]
        assert len(run.calls) == (2 if isinstance(answer, str) else 3)
        assert len(run.events["approval"]) == (1 if isinstance(answer, str) else 2)
        assert not run.statuses[-1].get("pending_requests")
    finally:
        generator.close()


def test_af_any_reply_preserves_older_wait_through_new_request_and_replay() -> None:
    executions = []
    for replay in (False, True):
        workflow, seen = _siblings()
        run = _af_run(workflow, replay=replay)
        waiting: Any = None

        def advance(value: Any = None, current: Any = run) -> bool:
            nonlocal waiting
            for _ in range(128):
                try:
                    task = current.generator.send(value)
                except StopIteration:
                    waiting = None
                    return True
                if not task.is_completed:
                    waiting = task
                    return False
                value = task.result
            pytest.fail("Exceeded bounded AF transport limit")

        try:
            assert not advance()
            assert set(run.events) == {"qA", "qB"}
            older = run.events["qB"][-1]
            run.events["qA"][-1].set_value(is_error=False, value="one")
            assert waiting.is_completed
            assert not advance(waiting.result)
            assert seen == [("qA", "one")] and run.events["qB"] == [older]
            assert set(run.statuses[-1]["pending_requests"]) == {"qB", "qC"}
            run.events["qC"][-1].set_value(is_error=False, value="follow-up")
            assert not advance(waiting.result)
            assert run.events["qB"] == [older] and waiting is older
            older.set_value(is_error=False, value="derived-one")
            assert advance(waiting.result)
            assert seen == [("qA", "one"), ("qC", "follow-up"), ("qB", "derived-one")]
            executions.append((
                run.calls,
                [call.args[0] for call in run.host.wait_for_external_event.call_args_list],
                deepcopy(run.statuses),
            ))
        finally:
            run.generator.close()
    assert executions[0] == executions[1]


@pytest.mark.parametrize("children", [["current-child"], {"1": "current-child"}])
def test_af_child_lookup_and_discovery_reject_retired_or_legacy_slot(children: Any) -> None:
    af = pytest.importorskip("agent_framework_azurefunctions")
    workflow, _ = _typed_workflow(str)
    app = af.AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")
    statuses = {
        "root-run": {"subworkflows": {"sub": children}},
        "current-child": {"pending_requests": {"approval": {"request_id": "approval"}}},
    }
    client = AsyncMock()
    client.get_status.side_effect = lambda instance: SimpleNamespace(custom_status=statuses[instance])
    assert asyncio.run(app._resolve_hitl_target(client, "root-run", "sub~0~approval")) is None
    requests = asyncio.run(app._gather_pending_hitl_requests(client, statuses["root-run"]))
    assert [request_id for request_id, _ in requests] == ([] if isinstance(children, list) else ["sub~1~approval"])
    if isinstance(children, dict):
        assert asyncio.run(app._resolve_hitl_target(client, "root-run", "sub~1~approval")) == (
            "current-child",
            "approval",
        )
