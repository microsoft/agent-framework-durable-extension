# Copyright (c) Microsoft. All rights reserved.

"""HITL lifecycle controls using Core and registered closures with real SDK tasks.

The transport only completes actual waits and child tasks. It does not implement
workflow routing, admission, request storage or response-handler execution.
These are in-process tests, not service history replay or crash-recovery tests.
"""

import asyncio
import json
from copy import deepcopy
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import _workflow_admission_test_support as admission
import pytest
from _workflow_lifecycle_test_support import (
    _INVALID_AGENT_APPROVAL_REPLIES,
    _agent_approval_workflow,
    _siblings,
    _Transport,
    _typed_workflow,
)
from _workflow_protocol_test_support import _complete
from agent_framework import (
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
from pydantic import BaseModel
from typing_extensions import Never

from agent_framework_durabletask import DurableWorkflowClient, serialize_agent_response
from agent_framework_durabletask._workflows import orchestrator as engine
from agent_framework_durabletask._workflows.naming import subworkflow_instance_id
from agent_framework_durabletask._workflows.serialization import deserialize_workflow_output


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
        assert root.host.statuses[-1]["subworkflows"] == {"sub": {"1": subworkflow_instance_id("root-run", "sub", 1)}}
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
    assert engine._index_subworkflows(first) == {"other": {"0": subworkflow_instance_id("root-run", "other", 0)}}
    assert engine._index_subworkflows(second) == {
        "sub": {str(ordinal): subworkflow_instance_id("root-run", "sub", ordinal) for ordinal in (1, 2)}
    }
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
    workflow, approvals = _agent_approval_workflow(tuple(sorted(order)))
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


@pytest.mark.parametrize("invalid_reply", _INVALID_AGENT_APPROVAL_REPLIES)
@pytest.mark.parametrize("sibling_first", [False, True])
def test_public_agent_invalid_approval_remains_pending_and_correctable(invalid_reply: Any, sibling_first: bool) -> None:
    workflow, approvals = _agent_approval_workflow()
    requests: list[Any] = []

    def entity_call(entity_id: Any, operation: str, request: Any) -> Any:
        requests.append(deepcopy(request))
        contents = list(approvals.values()) if len(requests) == 1 else [Content.from_text("done")]
        return _complete(serialize_agent_response(AgentResponse(messages=[Message("assistant", contents)])))

    transport = _Transport()
    try:
        run = transport.start(workflow, entity_call)
        native = Mock(spec=TaskHubGrpcClient)
        native.get_orchestration_state.side_effect = transport.state
        native.raise_orchestration_event.side_effect = lambda instance, event_name, data: transport.event(
            event_name, data, instance_id=instance
        )
        client = DurableWorkflowClient(native)
        pending = deepcopy(client.get_pending_hitl_requests("root-run"))
        sibling_wait = transport.events["root-run"]["b"][-1]
        for _ in range(2):
            client.send_hitl_response("root-run", "a", deepcopy(invalid_reply))
            assert client.get_pending_hitl_requests("root-run") == pending
            assert transport.events["root-run"]["b"] == [sibling_wait]
            assert len(requests) == 1 and not run.done
        order = ("b", "a") if sibling_first else ("a", "b")
        for index, key in enumerate(order):
            client.send_hitl_response(
                "root-run", key, approvals[key].to_function_approval_response(key == "b").to_dict()
            )
            if index == 0:
                assert {item["request_id"] for item in client.get_pending_hitl_requests("root-run")} == {order[1]}
                assert len(requests) == 1 and not run.done
        assert run.done and len(requests) == 2
        assert client.get_pending_hitl_requests("root-run") == []
        contents = requests[1]["contextMessages"][0]["contents"]
        assert [(item["type"], item["id"], item["approved"]) for item in contents] == [
            ("function_approval_response", key, key == "b") for key in order
        ]
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
