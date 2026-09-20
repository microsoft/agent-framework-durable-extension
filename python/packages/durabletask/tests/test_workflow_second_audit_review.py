# Copyright (c) Microsoft. All rights reserved.

"""Second audit regressions through registered dispatch, with Core routing controls."""

import asyncio
import json
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

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
from durabletask.worker import TaskHubGrpcWorker
from test_workflow_admission_review import _registered_run, _run_core_workflow
from test_workflow_protocol_review import _complete, _drain, _host
from typing_extensions import Never

from agent_framework_durabletask import DurableAIAgentWorker, DurableWorkflowClient, serialize_agent_response
from agent_framework_durabletask._workflows.orchestrator import SOURCE_HITL_RESPONSE
from agent_framework_durabletask._workflows.serialization import deserialize_value, serialize_value


@dataclass
class _TypedValue:
    value: Any


def _fanin_graph(
    *, targeted: bool, typed: bool, left_values: list[str], right_values: list[str], later_left: bool
) -> tuple[Workflow, list[dict[str, Any]], list[tuple[str, str]]]:
    seen: list[dict[str, Any]] = []
    emissions: list[tuple[str, str]] = []
    # Typed batches also guard plural source IDs that look like control metadata.
    left_id = SOURCE_HITL_RESPONSE if typed else "left"

    class Seed(Executor):
        @handler(input=str, output=str)
        async def handle(self, message: str, ctx: WorkflowContext[str]) -> None:
            await ctx.send_message("first", target_id=left_id)
            await ctx.send_message("right-trigger", target_id="delay")

    class Left(Executor):
        @handler(input=str, output=str | _TypedValue)
        async def handle(self, message: str, ctx: WorkflowContext[str | _TypedValue]) -> None:
            assert message in ("first", "second")
            for value in left_values if message == "first" else ["L2"]:
                emissions.append((self.id, value))
                await ctx.send_message(_TypedValue(value) if typed else value, target_id="sink" if targeted else None)

    class Right(Executor):
        @handler(input=str, output=str | _TypedValue)
        async def handle(self, message: str, ctx: WorkflowContext[str | _TypedValue]) -> None:
            for value in right_values:
                emissions.append((self.id, value))
                await ctx.send_message(_TypedValue(value) if typed else value, target_id="sink" if targeted else None)
            if later_left:
                await ctx.send_message("second", target_id=left_id)

    class Sink(Executor):
        @handler(input=list[str | _TypedValue], workflow_output=dict)
        async def handle(self, message: list[Any], ctx: WorkflowContext[Never, dict[str, Any]]) -> None:
            assert {source for source, _ in emissions} == {left_id, "right"}
            assert all(type(item) is (_TypedValue if typed else str) for item in message)
            assert list(ctx.source_executor_ids) == [left_id, "right"]
            with pytest.raises(RuntimeError):
                ctx.get_source_executor_id()
            row = {
                "payload": [item.value if typed else item for item in message],
                "sources": list(ctx.source_executor_ids),
            }
            seen.append(row)
            await ctx.yield_output(row)

    seed, left, right = Seed(id="seed"), Left(id=left_id), Right(id="right")
    delay, sink = admission._Relay("delay"), Sink(id="sink")
    workflow = (
        WorkflowBuilder(name="second-fanin", start_executor=seed, output_from=[sink], max_iterations=24)
        .add_fan_out_edges(seed, [left, delay])
        .add_edge(delay, right)
        .add_fan_in_edges([left, right], sink)
        .add_edge(right, left, condition=lambda value: value == "second")
        .build()
    )
    return workflow, seen, emissions


@pytest.mark.parametrize("targeted", [False, True])
@pytest.mark.parametrize("typed", [False, True])
@pytest.mark.parametrize(
    ("left_values", "right_values", "later_left", "expected"),
    [
        pytest.param(["L1"], ["R1"], False, [["L1", "R1"]], id="single-pair-control"),
        pytest.param(["L1"], [], False, [], id="missing-source-never-dispatched"),
        pytest.param(["L1", "L1b"], ["R1"], False, [["L1", "L1b", "R1"]], id="flush-entire-buffer"),
        pytest.param(["L1"], ["R1", "R2"], True, [["L1", "R1"], ["L2", "R2"]], id="burst-later-partner"),
        pytest.param(["L1"], ["R1", "R2"], False, [["L1", "R1"]], id="unmatched-tail-stays-buffered"),
    ],
)
def test_fanin_flushes_each_arrival_not_each_result_or_superstep(
    targeted: bool, typed: bool, left_values: list[str], right_values: list[str], later_left: bool, expected: list[Any]
) -> None:
    observations = []
    for core in (True, False):
        workflow, seen, emissions = _fanin_graph(
            targeted=targeted, typed=typed, left_values=left_values, right_values=right_values, later_left=later_left
        )
        if core:
            asyncio.run(_run_core_workflow(workflow))
        else:
            generator, _, calls = _registered_run(workflow)
            assert _drain(generator) == seen
            sink_calls = [call for call in calls if call["name"] == "dafx-second-fanin-sink"]
            assert len(sink_calls) == len(expected)
            assert all(call["input"]["is_hitl_response"] is False for call in sink_calls)
        assert [row["payload"] for row in seen] == expected
        left_id = SOURCE_HITL_RESPONSE if typed else "left"
        assert emissions == [
            *((left_id, value) for value in left_values),
            *(("right", value) for value in right_values),
            *([(left_id, "L2")] if later_left else []),
        ]
        observations.append(seen)
    assert observations[0] == observations[1]


_BUSINESS_IDS = [
    "business",
    "business__hitl_response__",
    "__hitl_response__business",
    SOURCE_HITL_RESPONSE,
    f"{SOURCE_HITL_RESPONSE}_approval",
]


class _BusinessSource(Executor):
    def __init__(self, source_id: str, payload: Any) -> None:
        self.payload = payload
        super().__init__(id=source_id)

    @handler(input=str, output=str | dict)
    async def handle(self, message: str, ctx: WorkflowContext[str | dict[str, Any]]) -> None:
        await ctx.send_message(deepcopy(self.payload))


class _BusinessSink(Executor):
    def __init__(self) -> None:
        self.seen: list[dict[str, Any]] = []
        super().__init__(id="sink")

    @handler(input=str | dict, workflow_output=dict)
    async def handle(self, message: Any, ctx: WorkflowContext[Never, dict[str, Any]]) -> None:
        row = {"payload": message, "sources": list(ctx.source_executor_ids)}
        self.seen.append(row)
        await ctx.yield_output(row)


@pytest.mark.parametrize("source_id", _BUSINESS_IDS)
@pytest.mark.parametrize(
    "payload",
    [
        "ordinary-business",
        {"request_id": "approval", "original_request": "decision", "response": True, "is_hitl_response": True},
    ],
)
def test_business_source_names_and_payload_fields_never_authorize_hitl(source_id: str, payload: Any) -> None:
    expected = [{"payload": payload, "sources": [source_id]}]
    for core in (True, False):
        source, sink = _BusinessSource(source_id, payload), _BusinessSink()
        workflow = (
            WorkflowBuilder(name="second-prefix", start_executor=source, output_from=[sink])
            .add_edge(source, sink)
            .build()
        )
        if core:
            asyncio.run(_run_core_workflow(workflow))
        else:
            generator, _, calls = _registered_run(workflow)
            assert _drain(generator) == expected
            assert calls[-1]["input"]["is_hitl_response"] is False
            assert calls[-1]["input"]["source_executor_ids"] == [source_id]
        assert sink.seen == expected


@pytest.mark.parametrize("source_id", _BUSINESS_IDS)
def test_business_control_names_reach_agent_dispatch(source_id: str) -> None:
    source = _BusinessSource(source_id, "ordinary-business")
    sink = AgentExecutor(Agent(client=RecordingChatClient(), name="sink"), id="sink")
    workflow = WorkflowBuilder(name="second-agent-prefix", start_executor=source).add_edge(source, sink).build()
    requests: list[dict[str, Any]] = []

    def entity_call(entity_id: Any, operation: str, request: dict[str, Any]) -> Any:
        requests.append(request)
        return _complete(serialize_agent_response(AgentResponse(messages=[Message("assistant", ["done"])])))

    _drain(_registered_run(workflow, entity_call)[0])
    assert len(requests) == 1
    assert requests[0]["message"] == "ordinary-business"


def _typed_gate_workflow() -> Workflow:
    class Gate(Executor):
        @handler(input=str)
        async def handle(self, message: str, ctx: WorkflowContext) -> None:
            await ctx.request_info(_TypedValue("decision"), response_type=dict, request_id="approval")

        @response_handler(request=_TypedValue, response=dict, workflow_output=dict)
        async def answer(
            self, original_request: _TypedValue, response: dict[str, Any], ctx: WorkflowContext[Never, dict[str, Any]]
        ) -> None:
            assert type(original_request) is _TypedValue and original_request.value == "decision"
            assert ctx.request_id == "approval" and list(ctx.source_executor_ids) == [SOURCE_HITL_RESPONSE]
            await ctx.yield_output({"approved": response["approved"]})

    gate = Gate(id="gate")
    return WorkflowBuilder(name="second-gate", start_executor=gate, output_from=[gate]).build()


@pytest.mark.parametrize("nested", [False, True])
def test_genuine_typed_hitl_keeps_registered_root_and_child_routing(nested: bool) -> None:
    inner = _typed_gate_workflow()
    sink = _BusinessSink()
    child_id = f"{SOURCE_HITL_RESPONSE}_approval"
    if nested:
        child = WorkflowExecutor(inner, id=child_id, propagate_request=True)
        workflow = (
            WorkflowBuilder(name="second-parent", start_executor=child, output_from=[sink])
            .add_edge(child, sink)
            .build()
        )
    else:
        workflow = inner
    children: dict[str, Any] = {}

    def host_factory(calls: Any, result: Any, *, functions: Any, **kwargs: Any) -> Any:
        host = _host(calls, result, functions=functions, **kwargs)

        def start_child(name: str, *, input: Any, instance_id: str) -> Any:
            calls.append({"kind": "child", "instance": instance_id, "name": name, "input": deepcopy(input)})
            child_host = _host(calls, result, functions=functions, instance_id=instance_id)
            generator = functions[name](child_host, json.loads(json.dumps(input)))
            batch = next(generator)
            assert batch.is_complete
            waiting = generator.send(batch.get_result())
            assert not waiting.is_complete
            completion: CompletableTask[Any] = CompletableTask()
            children[instance_id] = SimpleNamespace(
                host=child_host, generator=generator, waiting=waiting, completion=completion, name=name
            )
            return completion

        host.call_sub_orchestrator.side_effect = start_child
        return host

    with patch.object(admission, "_host", side_effect=host_factory):
        generator, host, calls = _registered_run(workflow)
    try:
        batch = next(generator)
        if nested:
            assert not batch.is_complete and len(children) == 1
            target, run = next(iter(children.items()))
            waiting = run.waiting
        else:
            target = "root-run"
            assert batch.is_complete
            waiting = generator.send(batch.get_result())
        assert not waiting.is_complete
        native = Mock(spec=TaskHubGrpcClient)

        def state(instance_id: str) -> Any:
            context = host if instance_id == "root-run" else children[instance_id].host
            name = f"dafx-{workflow.name}" if instance_id == "root-run" else children[instance_id].name
            return SimpleNamespace(name=name, serialized_custom_status=json.dumps(context.statuses[-1]))

        native.get_orchestration_state.side_effect = state
        public = DurableWorkflowClient(native)
        pending = public.get_pending_hitl_requests("root-run")
        assert len(pending) == 1
        public.send_hitl_response("root-run", pending[0]["request_id"], {"approved": True})
        native.raise_orchestration_event.assert_called_once_with(target, event_name="approval", data={"approved": True})
        waiting.complete(native.raise_orchestration_event.call_args.kwargs["data"])
        if nested:
            run.completion.complete(_drain(run.generator, waiting.get_result()))
            output = _drain(generator, batch.get_result())
            assert output == [{"payload": {"approved": True}, "sources": [child_id]}]
            assert calls[-1]["input"]["is_hitl_response"] is False
        else:
            assert _drain(generator, waiting.get_result()) == [{"approved": True}]
        gate_calls = [call["input"] for call in calls if call["name"] == "dafx-second-gate-gate"]
        assert [item["is_hitl_response"] for item in gate_calls] == [False, True]
        assert gate_calls[1]["source_executor_ids"] == [f"{SOURCE_HITL_RESPONSE}_approval"]
    finally:
        generator.close()
        for run in children.values():
            run.generator.close()


def test_genuine_agent_hitl_metadata_survives_multiple_reply_queue() -> None:
    agent = AgentExecutor(Agent(client=RecordingChatClient(), name="agent"), id="agent")
    workflow = WorkflowBuilder(name="second-agent-hitl", start_executor=agent, output_from=[agent]).build()
    approvals = [
        Content.from_function_approval_request(request_id, Content.from_function_call(request_id, "tool"))
        for request_id in ("first", "second")
    ]
    requests: list[dict[str, Any]] = []

    def entity_call(entity_id: Any, operation: str, request: dict[str, Any]) -> Any:
        requests.append(deepcopy(request))
        contents = approvals if len(requests) == 1 else [Content.from_text("done")]
        return _complete(serialize_agent_response(AgentResponse(messages=[Message("assistant", contents)])))

    generator, host, _ = _registered_run(workflow, entity_call)
    waits: dict[str, CompletableTask[Any]] = {}

    def wait(name: str) -> CompletableTask[Any]:
        task: CompletableTask[Any] = CompletableTask()
        waits[name] = task
        return task

    host.wait_for_external_event.side_effect = wait
    try:
        batch = next(generator)
        waiting = generator.send(batch.get_result())
        for index, approval in enumerate(approvals):
            assert not waiting.is_complete and len(requests) == 1
            assert approval.id is not None
            waits[approval.id].complete(approval.to_function_approval_response(True).to_dict())
            assert waiting.is_complete
            if index == 0:
                waiting = generator.send(waiting.get_result())
        output = deserialize_value(_drain(generator, waiting.get_result()))
        assert output[0].text == "done" and len(requests) == 2
        contents = requests[1]["contextMessages"][0]["contents"]
        assert [item["id"] for item in contents] == ["first", "second"]
        assert all(item["type"] == "function_approval_response" and item["approved"] is True for item in contents)
    finally:
        generator.close()


def _registered_activity(executor: Executor) -> Callable[[dict[str, Any]], dict[str, Any]]:
    workflow = WorkflowBuilder(name="second-direct", start_executor=executor).build()
    native = Mock(spec=TaskHubGrpcWorker)
    DurableAIAgentWorker(native, deployment_mode="isolated_v2").configure_workflow(workflow)
    activities = {call.args[0].__name__: call.args[0] for call in native.add_activity.call_args_list}
    activity = activities[f"dafx-second-direct-{executor.id}"]

    def invoke(payload: dict[str, Any]) -> dict[str, Any]:
        return json.loads(activity(None, json.dumps(payload, allow_nan=False)))

    return invoke


@pytest.mark.parametrize("flag", [None, False, 0, 1, "true"])
def test_registered_activity_does_not_infer_control_from_names_or_truthiness(flag: Any) -> None:
    sink = _BusinessSink()
    activity = _registered_activity(sink)
    payload: dict[str, Any] = {
        "message": "ordinary-business",
        "source_executor_ids": [SOURCE_HITL_RESPONSE, f"{SOURCE_HITL_RESPONSE}_approval"],
    }
    if flag is not None:
        payload["is_hitl_response"] = flag
    activity(payload)
    assert sink.seen == [{"payload": "ordinary-business", "sources": payload["source_executor_ids"]}]


class _StateWriter(Executor):
    def __init__(self, value: Any, *, mutate: bool = False) -> None:
        self.value, self.mutate = value, mutate
        super().__init__(id="writer")

    @handler(input=str, output=str)
    async def handle(self, message: str, ctx: WorkflowContext[str]) -> None:
        if self.mutate:
            value = ctx.get_state("value")
            value["items"][0] = deepcopy(self.value["items"][0])
            ctx.set_state("value", value)
        else:
            ctx.set_state("value", deepcopy(self.value))
        await ctx.send_message("observe")


@pytest.mark.parametrize(
    ("before", "after", "changed", "mutate"),
    [
        pytest.param(False, 0, True, False, id="false-to-zero"),
        pytest.param(0, False, True, False, id="zero-to-false"),
        pytest.param(True, 1, True, False, id="true-to-one"),
        pytest.param(1, True, True, False, id="one-to-true"),
        pytest.param(1, 1.0, True, False, id="int-to-float"),
        pytest.param(1.0, 1, True, False, id="float-to-int"),
        pytest.param({"items": [False]}, {"items": [0]}, True, True, id="nested-mutation"),
        pytest.param({"items": [1]}, {"items": [1.0]}, True, True, id="nested-int-float"),
        pytest.param(_TypedValue(False), _TypedValue(0), True, False, id="typed-custom-equal-values"),
        pytest.param(_TypedValue({"items": [False]}), _TypedValue({"items": [False]}), False, False, id="custom-noop"),
        pytest.param(False, False, False, False, id="false-noop"),
        pytest.param(None, None, False, False, id="null-noop"),
        pytest.param({"a": False, "b": 1.0}, {"b": 1.0, "a": False}, False, False, id="object-order-noop"),
    ],
)
def test_registered_state_diff_preserves_wire_types_across_later_dispatch(
    before: Any, after: Any, changed: bool, mutate: bool
) -> None:
    encoded_before = serialize_value(before)
    # First exercise the registered activity itself, not the diff implementation.
    activity = _registered_activity(_StateWriter(after, mutate=mutate))
    result = activity({"message": "go", "shared_state_snapshot": {"value": encoded_before}})
    expected_updates = {"value": serialize_value(after)} if changed else {}
    assert json.dumps(result["shared_state_updates"], sort_keys=True) == json.dumps(expected_updates, sort_keys=True)
    assert result["shared_state_deletes"] == []

    seen: list[Any] = []

    class Seed(Executor):
        @handler(input=str, output=str)
        async def handle(self, message: str, ctx: WorkflowContext[str]) -> None:
            ctx.set_state("value", deepcopy(before))
            ctx.set_state("custom", _TypedValue({"unchanged": [False, 0, 1.0]}))
            await ctx.send_message("write")

    class Observe(Executor):
        @handler(input=str, workflow_output=dict)
        async def handle(self, message: str, ctx: WorkflowContext[Never, dict[str, Any]]) -> None:
            value, custom = ctx.get_state("value"), ctx.get_state("custom")
            assert type(value) is type(after)
            assert type(custom) is _TypedValue and custom.value == {"unchanged": [False, 0, 1.0]}
            seen.append(value)
            await ctx.yield_output({"value": value})

    seed, writer, observe = Seed(id="seed"), _StateWriter(after, mutate=mutate), Observe(id="observe")
    workflow = (
        WorkflowBuilder(name="second-state", start_executor=seed, output_from=[observe])
        .add_edge(seed, writer)
        .add_edge(writer, observe)
        .build()
    )
    generator, _, calls = _registered_run(workflow)
    output = _drain(generator)
    assert len(calls) == 3 and len(seen) == 1
    # The later invocation's serialized snapshot and final output must contain
    # the replacement, not merely a locally changed value in the writer.
    persisted = calls[-1]["input"]["shared_state_snapshot"]
    assert json.dumps(persisted["value"], sort_keys=True) == json.dumps(serialize_value(after), sort_keys=True)
    assert json.dumps(output, sort_keys=True) == json.dumps([{"value": serialize_value(after)}], sort_keys=True)
    assert persisted["custom"] == calls[1]["input"]["shared_state_snapshot"]["custom"]
