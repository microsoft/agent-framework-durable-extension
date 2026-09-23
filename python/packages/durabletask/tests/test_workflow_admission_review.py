# Copyright (c) Microsoft. All rights reserved.

"""Composed routing, occurrence and HITL admission regressions without a service."""

import asyncio
import json
from collections.abc import Callable, Generator
from copy import deepcopy
from itertools import count
from typing import Any
from unittest.mock import Mock

import pytest
from _execution_test_support import JsonStateProvider, RecordingChatClient
from _workflow_test_support import create_registration_worker
from agent_framework import (
    Agent,
    AgentExecutor,
    AgentExecutorResponse,
    AgentResponse,
    Case,
    Default,
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
from test_workflow_protocol_review import _complete, _drain, _host, _node, _workflow
from typing_extensions import Never

from agent_framework_durabletask import (
    AgentEntity,
    DurableAgentState,
    DurableAIAgentWorker,
    DurableWorkflowClient,
    load_agent_response,
    serialize_agent_response,
    wrap_workflow_input,
)
from agent_framework_durabletask._json_payload import JsonPayload
from agent_framework_durabletask._workflows.orchestrator import (
    SOURCE_WORKFLOW_START,
    TaskMetadata,
    TaskType,
    _match_occurrences,
    _prepare_all_tasks,
)
from agent_framework_durabletask._workflows.serialization import deserialize_workflow_output


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


@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("allowed", [False, True])
def test_targeted_single_edge_still_evaluates_condition(explicit: bool, allowed: bool) -> None:
    for core in (True, False):
        source, sink = _Relay("source", target="sink" if explicit else None), _Sink()
        predicate = Mock(return_value=allowed)
        workflow = (
            WorkflowBuilder(name="condition", start_executor=source, output_from=[sink])
            .add_edge(source, sink, condition=predicate)
            .build()
        )
        if core:
            asyncio.run(_run_core_workflow(workflow))
        else:
            _drain(_registered_run(workflow)[0])
        assert sink.seen == ([{"payload": "source", "sources": ["source"]}] if allowed else [])
        predicate.assert_called_once_with("source")


@pytest.mark.parametrize("kind", ["selection", "switch"])
@pytest.mark.parametrize("target", ["sink", "other"])
def test_explicit_target_is_intersected_with_graph_selection(kind: str, target: str) -> None:
    for core in (True, False):
        source, sink, other = _Relay("source", target=target), _Sink(), _Sink("other")
        builder = WorkflowBuilder(name="selection", start_executor=source, output_from=[sink, other])
        if kind == "selection":
            # Explicit selection delivers once, unlike a broadcast with repeated targets.
            predicate = Mock(return_value=["sink", "sink"])
            builder.add_multi_selection_edge_group(source, [sink, other], predicate)
        else:
            predicate = Mock(return_value=True)
            builder.add_switch_case_edge_group(source, [Case(predicate, sink), Default(other)])
        workflow = builder.build()
        if core:
            asyncio.run(_run_core_workflow(workflow))
        else:
            _drain(_registered_run(workflow)[0])
        assert sink.seen == ([{"payload": "source", "sources": ["source"]}] if target == "sink" else [])
        assert other.seen == []
        assert predicate.call_count == 1


def test_explicit_target_does_not_create_an_edge() -> None:
    for core in (True, False):
        source, relay, sink = _Relay("source", target="sink"), _Relay("relay"), _Sink()
        workflow = (
            WorkflowBuilder(name="off-graph", start_executor=source, output_from=[sink])
            .add_edge(source, relay)
            .add_edge(relay, sink)
            .build()
        )
        if core:
            asyncio.run(_run_core_workflow(workflow))
        else:
            generator, _, calls = _registered_run(workflow)
            assert _drain(generator) == []
            assert [call["name"] for call in calls] == ["dafx-off-graph-source"]
        assert sink.seen == []


@pytest.mark.parametrize("explicit", [False, True])
@pytest.mark.parametrize("right_target", [None, "sink", "other"])
@pytest.mark.parametrize("right_emissions", [0, 1])
def test_fanin_waits_for_every_source_and_preserves_source_ids(
    explicit: bool, right_target: str | None, right_emissions: int
) -> None:
    for core in (True, False):
        source, sink, other = _Relay("source"), _Sink(), _Sink("other")
        left = _Relay("left", target="sink" if explicit else None, emissions=2)
        right = _Relay("right", target=right_target, emissions=right_emissions)
        workflow = (
            WorkflowBuilder(name="fanin", start_executor=source, output_from=[sink, other])
            .add_fan_out_edges(source, [left, right])
            .add_fan_in_edges([left, right], sink)
            .add_edge(right, other)
            .build()
        )
        if core:
            asyncio.run(_run_core_workflow(workflow))
        else:
            _drain(_registered_run(workflow)[0])
        expected = [{"payload": ["left", "left", "right"], "sources": ["left", "right"]}]
        assert sink.seen == (expected if right_emissions and right_target != "other" else [])
        assert other.seen == (
            [{"payload": "right", "sources": ["right"]}] if right_emissions and right_target != "sink" else []
        )


def test_plural_sources_survive_all_task_metadata_and_sequential_agent_queue() -> None:
    agent = AgentExecutor(Agent(client=RecordingChatClient(), name="agent"), id="agent")
    child = Mock(spec=WorkflowExecutor)
    child.id, child.workflow = "child", _workflow("inner")
    workflow = _workflow(nodes=[_node("activity"), agent, child])
    ctx = Mock(instance_id="root-run")
    address = {"root_instance_id": "root-run", "root_workflow_name": "protocol", "request_path_prefix": ""}
    pending = {name: [("first", ["left", "right"])] for name in workflow.executors}
    pending["agent"].append(("second", ["right", "left"]))

    _, metadata, remaining = _prepare_all_tasks(ctx, workflow, pending, {}, [0], address)

    assert {meta.task_type for meta in metadata} == {TaskType.ACTIVITY, TaskType.AGENT, TaskType.SUBWORKFLOW}
    assert all(meta.source_executor_ids == ["left", "right"] for meta in metadata)
    assert remaining == [("agent", "second", ["right", "left"])]
    assert json.loads(ctx.prepare_activity_task.call_args.args[1])["source_executor_ids"] == ["left", "right"]
    assert TaskMetadata("activity", "first", "source", TaskType.ACTIVITY).source_executor_ids == ["source"]


@pytest.mark.parametrize("public_id", [None, "same-id"])
@pytest.mark.parametrize("aliased", [False, True])
def test_matcher_counts_occurrences_not_branch_copy_positions(public_id: str | None, aliased: bool) -> None:
    first = Message("assistant", ["same-content"], message_id=public_id)
    second = first if aliased else deepcopy(first)
    selected = first if aliased else deepcopy(first)
    assert _match_occurrences([selected], [first, second], ["ancestor", "ancestor"]) == ["ancestor"]
    # Same ID/content does not make two independent emissions one occurrence.
    assert _match_occurrences([deepcopy(first)], [first, second], ["left", "right"]) == [None]
    assert _match_occurrences([selected, selected, selected], [first, second], ["ancestor", "ancestor"]) == [
        "ancestor",
        None,
        None,
    ]
    # Full positional copies retain both independently emitted occurrences.
    assert _match_occurrences(deepcopy([first, second]), [first, second], ["left", "right"]) == ["left", "right"]


@pytest.mark.parametrize("public_id", [None, "same-id"])
def test_distinct_equal_emissions_and_repeated_selection_keep_distinct_transport_ids(public_id: str | None) -> None:
    class Seed(Executor):
        @handler(input=str, output=AgentExecutorResponse)
        async def handle(self, message: str, ctx: WorkflowContext[AgentExecutorResponse]) -> None:
            messages = [Message("assistant", ["equal"], message_id=public_id) for _ in range(2)]
            await ctx.send_message(AgentExecutorResponse("seed", AgentResponse(messages=messages), messages))

    seed = Seed(id="seed")
    sink = AgentExecutor(
        Agent(client=RecordingChatClient(), name="sink"),
        id="sink",
        context_mode="custom",
        context_filter=lambda messages: [messages[1], messages[0], messages[0]],
    )
    workflow = (
        WorkflowBuilder(name="multiplicity", start_executor=seed, output_from=[sink]).add_edge(seed, sink).build()
    )
    requests: list[dict[str, Any]] = []

    def entity_call(entity_id: Any, operation: str, request: dict[str, Any]) -> Any:
        requests.append(deepcopy(request))
        return _complete(serialize_agent_response(AgentResponse(messages=[Message("assistant", ["done"])])))

    generator, host, _ = _registered_run(workflow, entity_call)
    _drain(generator)
    host.call_entity.assert_called_once()
    assert host.call_entity.call_args.kwargs == {"return_type": JsonPayload}
    assert len(requests) == 1
    assert requests[0]["contextMessages"] == [Message("assistant", ["equal"], message_id=public_id).to_dict()] * 3
    assert len(set(requests[0]["contextMessageIds"])) == 3


@pytest.mark.parametrize("shared", [False, True], ids=["single-predecessor-control", "shared-ancestor-join"])
def test_activity_join_keeps_ancestor_occurrence_through_agent_ingestion(shared: bool) -> None:
    ancestor = Message("assistant", ["ancestor"], message_id="ancestor")
    emissions: list[dict[str, Any]] = []
    joined_sources: list[list[str]] = []
    selected_histories: list[list[str | None]] = []

    class Seed(Executor):
        @handler(input=str, output=AgentExecutorResponse)
        async def handle(self, message: str, ctx: WorkflowContext[AgentExecutorResponse]) -> None:
            emissions.append(ancestor.to_dict())
            await ctx.send_message(AgentExecutorResponse("seed", AgentResponse(messages=[ancestor]), [ancestor]))

    class Branch(Executor):
        @handler(input=AgentExecutorResponse, output=AgentExecutorResponse)
        async def handle(self, prior: AgentExecutorResponse, ctx: WorkflowContext[AgentExecutorResponse]) -> None:
            own = Message("assistant", [self.id], message_id=self.id)
            await ctx.send_message(
                AgentExecutorResponse(self.id, AgentResponse(messages=[own]), [*prior.full_conversation, own])
            )

    class Join(Executor):
        @handler(input=AgentExecutorResponse | list[AgentExecutorResponse], output=AgentExecutorResponse)
        async def handle(self, incoming: Any, ctx: WorkflowContext[AgentExecutorResponse]) -> None:
            priors = incoming if isinstance(incoming, list) else [incoming]
            assert [prior.executor_id for prior in priors] == (["left", "right"] if shared else ["left"])
            copies = [prior.full_conversation[0] for prior in priors]
            assert len({id(message) for message in copies}) == len(priors)
            assert all(message.to_dict() == ancestor.to_dict() for message in copies)
            joined_sources.append(list(ctx.source_executor_ids))
            # Fixture policy: the seed emitted exactly one ancestor. Keep it once,
            # followed by each branch's new message, without consulting the ledger.
            history = [copies[0], *(prior.full_conversation[-1] for prior in priors)]
            own = Message("assistant", ["joined"], message_id="joined")
            await ctx.send_message(AgentExecutorResponse("join", AgentResponse(messages=[own]), [*history, own]))

    def select(messages: list[Message]) -> list[Message]:
        selected_histories.append([message.message_id for message in messages])
        return messages

    model = RecordingChatClient()
    agent = Agent(client=model, name="sink")
    sink = AgentExecutor(agent, id="sink", context_mode="custom", context_filter=select)
    seed, left, join = Seed(id="seed"), Branch(id="left"), Join(id="join")
    builder = WorkflowBuilder(name="occurrence", start_executor=seed, output_from=[sink])
    branches = [left, Branch(id="right")] if shared else [left]
    builder.add_fan_out_edges(seed, [*branches, sink])
    if shared:
        builder.add_fan_in_edges(branches, join)
    else:
        builder.add_edge(left, join)
    workflow = builder.add_edge(join, sink).build()
    provider = JsonStateProvider(session_id="root-run", entity_name="dafx-occurrence-sink")
    requests: list[dict[str, Any]] = []
    receipts: list[dict[str, Any]] = []

    def entity_call(entity_id: Any, operation: str, request: dict[str, Any]) -> Any:
        assert (entity_id.entity, entity_id.key, operation) == ("dafx-occurrence-sink", "root-run", "run")
        requests.append(deepcopy(request))
        # Cold entity reconstruction prevents a live entity cache from hiding loss.
        response = asyncio.run(AgentEntity(agent, state_provider=provider).run(deepcopy(request)))
        assert response.additional_properties.get("durable_status") != "error"
        receipts.append(deepcopy(DurableAgentState.from_dict(provider.raw).data.ingested_messages))
        return _complete(serialize_agent_response(response))

    _drain(_registered_run(workflow, entity_call)[0])

    branch_ids = ["left", "right"] if shared else ["left"]
    assert emissions == [ancestor.to_dict()]
    assert joined_sources == [branch_ids]
    assert selected_histories == [["ancestor"], ["ancestor", *branch_ids, "joined"]]
    assert len(requests) == len(model.received_messages) == provider.successful_writes == 2
    batches = [load_agent_response({"messages": request["contextMessages"]}).messages for request in requests]
    assert [[message.message_id for message in batch] for batch in batches] == [["ancestor"], [*branch_ids, "joined"]]
    assert batches[0][0].to_dict() == ancestor.to_dict()
    assert [sum(message.text == "ancestor" for message in batch) for batch in model.received_messages] == [1, 1]
    wire_ids = [occurrence for request in requests for occurrence in request["contextMessageIds"]]
    assert len(wire_ids) == len(set(wire_ids)) == len(branch_ids) + 2
    assert set(receipts[0]) == {wire_ids[0]}
    assert set(receipts[1]) == set(wire_ids)
    assert receipts[1][wire_ids[0]] == receipts[0][wire_ids[0]]
    assert all(len(revisions) == 1 for revisions in receipts[1].values())
    cold = DurableAgentState.from_dict(deepcopy(provider.raw))
    persisted_messages = [message for entry in cold.data.conversation_history for message in entry.messages]
    assert sum(message.text == "ancestor" for message in persisted_messages) == 1


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


@pytest.mark.parametrize("marker", [{"__type__": "builtins:dict"}, {"__pickled__": "inert"}])
@pytest.mark.parametrize(
    ("answer", "expected"),
    [
        (None, None),
        ({"approved": True}, {"approved": True}),
        (
            {"nested": {"type": "business", "_durable_agent_response": 99, "input": [0, False, None]}},
            {"nested": {"type": "business", "_durable_agent_response": 99, "input": [0, False, None]}},
        ),
        ({"nested": {"__type__": "builtins:dict"}, "approved": True}, {"nested": None, "approved": True}),
    ],
)
def test_public_hitl_rejection_keeps_request_pending_for_corrected_reply(
    marker: dict[str, Any], answer: Any, expected: Any
) -> None:
    generator, host, calls = _registered_run(_hitl_workflow())
    batch = next(generator)
    waiting = generator.send(batch.get_result())
    assert not waiting.is_complete and len(calls) == 1
    client = Mock(spec=TaskHubGrpcClient)
    client.get_orchestration_state.side_effect = lambda instance: Mock(
        serialized_custom_status=json.dumps(host.statuses[-1])
    )
    public = DurableWorkflowClient(client)
    before = deepcopy(marker)

    with pytest.raises(ValueError, match="disallowed pickle/type markers"):
        public.send_hitl_response("root-run", "approval", marker)

    client.raise_orchestration_event.assert_not_called()
    assert marker == before and not waiting.is_complete and len(calls) == 1
    assert set(host.statuses[-1]["pending_requests"]) == {"approval"}
    pending = deepcopy(host.statuses[-1]["pending_requests"])
    # Raw malformed events now checkpoint rejection before rearming the wait.
    waiting.complete(marker)
    rejected = generator.send(waiting.get_result())
    assert rejected.is_complete and len(calls) == 2
    assert [json.loads(value) for value in rejected.get_result()] == [
        {
            "hitl_admission": {"request_id": "approval", "status": "invalidreply"},
            "sent_messages": [],
            "outputs": [],
            "events": [],
            "shared_state_updates": {},
            "shared_state_deletes": [],
            "pending_request_info_events": [],
        }
    ]
    assert calls[-1]["input"]["message"] == {
        "request_id": "approval",
        "original_request": "decision",
        "response": None,
        "response_type": "builtins:object",
        "validation_error": True,
    }
    assert host.statuses[-1]["pending_requests"] == pending
    waiting_again = generator.send(rejected.get_result())
    assert waiting_again is not waiting and not waiting_again.is_complete
    assert host.statuses[-1]["pending_requests"] == pending
    public.send_hitl_response("root-run", "approval", deepcopy(answer))
    client.raise_orchestration_event.assert_called_once_with("root-run", event_name="approval", data=expected)
    waiting_again.complete(client.raise_orchestration_event.call_args.kwargs["data"])
    accepted = generator.send(waiting_again.get_result())
    assert accepted.is_complete and len(calls) == 3
    assert [json.loads(value)["hitl_admission"] for value in accepted.get_result()] == [
        {"request_id": "approval", "status": "accepted"}
    ]
    output = deserialize_workflow_output(_drain(generator, accepted.get_result()))
    assert output == [{"response": expected, "response_type": type(expected).__name__}]
    assert len(calls) == 3 and not host.statuses[-1].get("pending_requests")
    assert [call.args[0] for call in host.wait_for_external_event.call_args_list] == ["approval", "approval"]
    assert calls[0]["input"]["source_executor_ids"] == [SOURCE_WORKFLOW_START]
