# Copyright (c) Microsoft. All rights reserved.

"""Known selection occurrences survive filtering, activity checkpoints and cold ingestion."""

import asyncio
import json
import logging
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from unittest.mock import Mock

import pytest
from _execution_test_support import JsonStateProvider, RecordingChatClient
from _workflow_admission_test_support import _registered_run
from _workflow_protocol_test_support import _complete, _drain
from _workflow_replay_test_support import _replay, _worker
from _workflow_selection_test_support import _assert_same_action_contract, _request_contract
from agent_framework import (
    Agent,
    AgentExecutor,
    AgentExecutorResponse,
    AgentResponse,
    Executor,
    Message,
    Workflow,
    WorkflowBuilder,
    WorkflowContext,
    handler,
)
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.worker import _ActivityExecutor
from typing_extensions import Never, Self

from agent_framework_durabletask import AgentEntity, DurableAgentState, _models, serialize_agent_response
from agent_framework_durabletask._workflows import orchestrator as engine
from agent_framework_durabletask._workflows.orchestrator import (
    TaskMetadata,
    TaskType,
    _prepare_agent_task,
    _WorkflowDeliveryLedger,
)


def test_request_oracle_excludes_only_valid_top_level_bookkeeping_timestamp() -> None:
    request = {
        "created_at": "2026-09-21T00:00:00+00:00",
        "contextMessages": [{"additional_properties": {"created_at": "application timestamp"}}],
        "contextMessageIds": ["occurrence"],
        "correlationId": "correlation",
        "options": {"created_at": "application option"},
    }
    later = {**request, "created_at": "2040-01-01T00:00:00+00:00"}
    assert _request_contract(request) == _request_contract(later)
    assert request["created_at"] == "2026-09-21T00:00:00+00:00"
    for key, value in (
        ("contextMessageIds", ["different occurrence"]),
        ("correlationId", "different correlation"),
        ("options", {"created_at": "changed application option"}),
        ("contextMessages", [{"additional_properties": {"created_at": "changed application timestamp"}}]),
    ):
        assert _request_contract({**later, key: value}) != _request_contract(request)
    for left, right in ((False, 0), (1, 1.0)):
        assert _request_contract({**request, "options": {"value": left}}) != _request_contract({
            **later,
            "options": {"value": right},
        })
    with pytest.raises(KeyError, match="created_at"):
        _request_contract({})
    for invalid in (None, "invalid", "2040-01-01T00:00:00", "2040-01-01T00:00:00+01:00"):
        with pytest.raises((AssertionError, ValueError)):
            _request_contract({**request, "created_at": invalid})


def _selection_workflow(
    public_id: str | None, copies: int, reverse: bool
) -> tuple[Workflow, Agent, RecordingChatClient, list[list[dict[str, Any]]]]:
    selections: list[list[dict[str, Any]]] = []

    class Seed(Executor):
        @handler(input=str, output=AgentExecutorResponse)
        async def handle(self, message: str, ctx: WorkflowContext[AgentExecutorResponse]) -> None:
            # Equal values and equal public IDs do not make these one emission.
            equal_a = Message("assistant", ["equal"], message_id=public_id)
            equal_b = Message("assistant", ["equal"], message_id=public_id)
            messages = [equal_a, equal_b]
            await ctx.send_message(AgentExecutorResponse(self.id, AgentResponse(messages=messages), messages))

    class Relay(Executor):
        @handler(input=AgentExecutorResponse, output=AgentExecutorResponse)
        async def handle(self, prior: AgentExecutorResponse, ctx: WorkflowContext[AgentExecutorResponse]) -> None:
            marker = Message("assistant", ["later"], message_id="marker")
            await ctx.send_message(
                AgentExecutorResponse(self.id, AgentResponse(messages=[marker]), [*prior.full_conversation, marker])
            )

    class Observe(Executor):
        @handler(input=AgentExecutorResponse, workflow_output=list)
        async def handle(self, prior: AgentExecutorResponse, ctx: WorkflowContext[Never, list]) -> None:
            await ctx.yield_output([item.to_dict() for item in prior.full_conversation])

    def select(messages: list[Message]) -> list[Message]:
        if len(messages) == 2:
            sources = messages
        else:
            assert len(messages) == 3 and messages[-1].text == "later"
            sources = [messages[1], messages[0]] if reverse else [messages[1]]
        selected = [item for item in sources for _ in range(copies)]
        selections.append([item.to_dict() for item in selected])
        return selected

    client = RecordingChatClient()
    agent = Agent(client=client, name="sink")
    sink = AgentExecutor(agent, id="sink", context_mode="custom", context_filter=select)
    seed, relay, observe = Seed(id="seed"), Relay(id="relay"), Observe(id="observe")
    workflow = (
        WorkflowBuilder(name="selection-occurrence", start_executor=seed, output_from=[observe])
        .add_fan_out_edges(seed, [sink, relay])
        .add_edge(relay, sink)
        .add_edge(sink, observe)
        .build()
    )
    return workflow, agent, client, selections


@pytest.mark.parametrize("public_id", [None, "same-id"])
@pytest.mark.parametrize("copies", [1, 2], ids=["distinct-equal-source-control", "repeated-source-selection"])
@pytest.mark.parametrize("reverse", [False, True], ids=["remove-first-source", "reorder-sources"])
def test_known_selected_occurrences_are_not_redelivered_after_filtering(
    public_id: str | None, copies: int, reverse: bool
) -> None:
    core, _, _, core_selections = _selection_workflow(public_id, copies, reverse)

    async def core_run() -> list[Any]:
        return (await core.run("go")).get_outputs()

    core_outputs = asyncio.run(core_run())
    equal = Message("assistant", ["equal"], message_id=public_id).to_dict()
    expected = [[equal] * (2 * copies), [equal] * ((2 if reverse else 1) * copies)]
    assert core_selections == expected
    assert [output[:-1] for output in core_outputs] == expected

    histories: list[list[dict[str, Any]]] = []
    # Fresh object graphs and cold entities must regenerate the same transport
    # identities. These are in-process registered dispatches, not SDK replay.
    for _ in range(2):
        workflow, agent, client, selections = _selection_workflow(public_id, copies, reverse)
        provider = JsonStateProvider(session_id="root-run", entity_name="dafx-selection-occurrence-sink")
        requests: list[dict[str, Any]] = []
        writes: list[int] = []

        # Bind this iteration's objects. The nonlocal provider is updated only
        # during the synchronous drain below, before the next iteration starts.
        def entity_call(
            entity_id: Any,
            operation: str,
            request: dict[str, Any],
            *,
            requests: list[dict[str, Any]] = requests,
            agent: Agent = agent,
            writes: list[int] = writes,
        ) -> Any:
            nonlocal provider
            assert (entity_id.entity, entity_id.key, operation) == ("dafx-selection-occurrence-sink", "root-run", "run")
            requests.append(deepcopy(request))
            # A new entity alone is not cold: the provider also caches state.
            provider = JsonStateProvider(
                provider.raw, session_id="root-run", entity_name="dafx-selection-occurrence-sink"
            )
            response = asyncio.run(AgentEntity(agent, state_provider=provider).run(deepcopy(request)))
            writes.append(provider.successful_writes)
            assert response.additional_properties.get("durable_status") != "error"
            return _complete(serialize_agent_response(response))

        output = _drain(_registered_run(workflow, entity_call)[0])
        assert selections == expected
        assert [row[:-1] for row in output] == expected
        assert len(requests) == len(client.received_messages) == 2 and writes == [1, 1]
        assert requests[0]["contextMessages"] == expected[0]
        assert len(set(requests[0]["contextMessageIds"])) == 2 * copies
        # Both B and any repeated selection of B have already been delivered.
        assert requests[1]["contextMessages"] == []
        assert requests[1]["contextMessageIds"] == []
        assert [sum(item.text == "equal" for item in batch) for batch in client.received_messages] == [2 * copies] * 2
        state = DurableAgentState.from_dict(deepcopy(provider.raw))
        assert set(state.data.ingested_messages) == set(requests[0]["contextMessageIds"])
        assert all(revisions is not None and len(revisions) == 1 for revisions in state.data.ingested_messages.values())
        histories.append(requests)
    assert [_request_contract(request) for request in histories[0]] == [
        _request_contract(request) for request in histories[1]
    ]


def test_repeat_selection_keeps_first_wire_id_and_stages_receipts_until_dispatch_succeeds() -> None:
    message = Message("assistant", ["equal"], message_id="same-id")
    prior = AgentExecutorResponse("source", AgentResponse(messages=[message]), [message])
    sink = AgentExecutor(
        Agent(client=RecordingChatClient(), name="sink"),
        id="sink",
        context_mode="custom",
        context_filter=lambda messages: [messages[0], messages[0]],
    )
    host = Mock(instance_id="run")
    ledger = _WorkflowDeliveryLedger(instance_id="run")
    host.prepare_agent_task.side_effect = ValueError("dispatch rejected")
    with pytest.raises(ValueError, match="dispatch rejected"):
        _prepare_agent_task(host, sink, "sink", prior, "workflow", ledger)
    assert ledger.selection_copies == {} and ledger.handoffs == {} and ledger.sent == {}
    host.prepare_agent_task.side_effect = None
    _prepare_agent_task(host, sink, "sink", prior, "workflow", ledger)
    ids = host.prepare_agent_task.call_args.kwargs["context_message_ids"]
    assert len(set(ids)) == 2
    # Preserve the pre-correction first-dispatch ID. Only a subsequent duplicate
    # delivery changes, not the historical address assigned on first selection.
    assert ids[1] == ledger.occurrence("selection", "sink", 0, 0, 1)
    _prepare_agent_task(host, sink, "sink", prior, "workflow", ledger)
    assert host.prepare_agent_task.call_args.args[3] == []
    assert host.prepare_agent_task.call_args.kwargs["context_message_ids"] == []


def test_full_projection_of_shared_ancestor_copies_still_collapses_one_occurrence() -> None:
    first = Message("assistant", ["ancestor"])
    second = deepcopy(first)
    prior = AgentExecutorResponse("join", AgentResponse(messages=[]), [first, second])
    ledger = _WorkflowDeliveryLedger(instance_id="run")
    ledger.remember(prior, ["one-ancestor", "one-ancestor"], [])
    sink = AgentExecutor(Agent(client=RecordingChatClient(), name="sink"), id="sink")
    host = Mock(instance_id="run")
    _prepare_agent_task(host, sink, "sink", prior, "workflow", ledger)
    assert host.prepare_agent_task.call_args.args[3] == [first.to_dict()]
    assert host.prepare_agent_task.call_args.kwargs["context_message_ids"] == ["one-ancestor"]
    assert ledger.selection_copies == {}


@pytest.mark.parametrize("public_id", [None, "same-id"])
@pytest.mark.parametrize("copied", [False, True], ids=["source-alias", "copied-unique-source"])
def test_selection_count_and_revision_transitions_reach_cold_entity_once(public_id: str | None, copied: bool) -> None:
    client = RecordingChatClient()
    agent = Agent(client=client, name="sink")
    source = Message("assistant", ["equal"], message_id=public_id)
    prior = AgentExecutorResponse("source", AgentResponse(messages=[source]), [source])
    count = 0

    def select(messages: list[Message]) -> list[Message]:
        return [deepcopy(messages[0]) if copied else messages[0] for _ in range(count)]

    sink = AgentExecutor(agent, id="sink", context_mode="custom", context_filter=select)
    host = Mock(instance_id="count-run")
    ledger = _WorkflowDeliveryLedger(instance_id="count-run")
    provider = JsonStateProvider(session_id="count-run", entity_name="dafx-counts-sink")
    requests: list[dict[str, Any]] = []
    writes: list[int] = []

    def dispatch(
        scoped_id: str,
        message: str,
        instance: str,
        context: list[dict[str, Any]],
        *,
        context_message_ids: list[str],
    ) -> Any:
        nonlocal provider
        assert (scoped_id, instance) == ("counts-sink", "count-run")
        request = _models.RunRequest(
            message=message,
            correlation_id=f"call-{len(requests)}",
            context_messages=context,
            context_message_ids=context_message_ids,
            orchestration_id=instance,
        ).to_dict()
        requests.append(deepcopy(request))
        provider = JsonStateProvider(provider.raw, session_id="count-run", entity_name="dafx-counts-sink")
        response = asyncio.run(AgentEntity(agent, state_provider=provider).run(request))
        writes.append(provider.successful_writes)
        assert response.additional_properties.get("durable_status") != "error"
        return _complete(serialize_agent_response(response))

    host.prepare_agent_task.side_effect = dispatch
    ranks: list[str] = []
    # Expected new deliveries are a simple multiset high-water mark per content
    # revision, independent of how the ledger assigns its opaque wire IDs.
    steps = [
        (0, 1, 1),
        (0, 3, 2),
        (0, 1, 0),
        (0, 2, 0),
        (0, 4, 1),
        (0, 0, 0),
        (0, 4, 0),
        (1, 2, 2),
        (1, 1, 0),
        (1, 4, 2),
        (1, 4, 0),
        (0, 4, 0),
    ]
    delivered = 0
    for revision, count, expected_delta in steps:
        source.additional_properties["revision"] = revision
        metadata = TaskMetadata("sink", prior, "source", TaskType.AGENT)
        _prepare_agent_task(host, sink, "sink", prior, "counts", ledger, metadata)
        ids = metadata.selected_context_ids
        assert ids is not None and len(ids) == len(set(ids)) == count
        assert ids[: len(ranks)] == ranks[:count]
        if count > len(ranks):
            ranks = list(ids)
        assert requests[-1]["contextMessages"] == [source.to_dict()] * expected_delta
        assert len(requests[-1]["contextMessageIds"]) == expected_delta
        delivered += expected_delta
        assert sum(item.text == "equal" for item in client.received_messages[-1]) == delivered
    assert delivered == 8 and len(ranks) == 4
    state = DurableAgentState.from_dict(deepcopy(provider.raw))
    assert set(state.data.ingested_messages) == set(ranks)
    assert all(revisions is not None and len(revisions) == 2 for revisions in state.data.ingested_messages.values())
    assert len(requests) == len(client.received_messages) == len(steps) and writes == [1] * len(steps)


@pytest.mark.parametrize("public_id", [None, "same-id"])
@pytest.mark.parametrize("reuse_object", [False, True], ids=["distinct-objects", "reused-object-new-emission"])
def test_equal_new_emissions_never_share_selection_copy_receipts(public_id: str | None, reuse_object: bool) -> None:
    host = Mock(instance_id="run")
    ledger = _WorkflowDeliveryLedger(instance_id="run")
    sink = AgentExecutor(
        Agent(client=RecordingChatClient(), name="sink"),
        id="sink",
        context_mode="custom",
        context_filter=lambda messages: [messages[0], messages[0]],
    )
    source = Message("assistant", ["equal"], message_id=public_id)
    all_ids: set[str] = set()
    for _ in range(3):
        if not reuse_object:
            source = deepcopy(source)
        # A new producer envelope's latest output is a new emission even when
        # the producer reuses both the object and its public message ID.
        prior = AgentExecutorResponse("producer", AgentResponse(messages=[source]), [source])
        _prepare_agent_task(host, sink, "sink", prior, "workflow", ledger)
        ids = host.prepare_agent_task.call_args.kwargs["context_message_ids"]
        assert len(ids) == len(set(ids)) == 2 and not all_ids.intersection(ids)
        all_ids.update(ids)
        _prepare_agent_task(host, sink, "sink", prior, "workflow", ledger)
        assert host.prepare_agent_task.call_args.args[3] == []
        assert host.prepare_agent_task.call_args.kwargs["context_message_ids"] == []
    assert len(all_ids) == 6


def test_two_output_slots_of_same_object_keep_distinct_emission_ids() -> None:
    source = Message("assistant", ["equal"], message_id="same-id")
    messages = [source, source]
    prior = AgentExecutorResponse("producer", AgentResponse(messages=messages), messages)
    sink = AgentExecutor(Agent(client=RecordingChatClient(), name="sink"), id="sink")
    host, ledger = Mock(instance_id="run"), _WorkflowDeliveryLedger(instance_id="run")
    _prepare_agent_task(host, sink, "sink", prior, "workflow", ledger)
    ids = host.prepare_agent_task.call_args.kwargs["context_message_ids"]
    assert len(set(ids)) == 2
    assert host.prepare_agent_task.call_args.args[3] == [source.to_dict(), source.to_dict()]
    _prepare_agent_task(host, sink, "sink", prior, "workflow", ledger)
    assert host.prepare_agent_task.call_args.kwargs["context_message_ids"] == []
    assert ledger.selection_copies == {}


def test_branch_aliases_keep_shared_ancestry_but_explicit_repetitions_have_stable_ranks() -> None:
    first = Message("assistant", ["ancestor"])
    second = deepcopy(first)
    prior = AgentExecutorResponse("join", AgentResponse(messages=[]), [first, second])
    host = Mock(instance_id="run")
    ledger = _WorkflowDeliveryLedger(instance_id="run")
    ledger.remember(prior, ["ancestor", "ancestor"], [])
    selection = [first, first]
    sink = AgentExecutor(
        Agent(client=RecordingChatClient(), name="sink"),
        id="sink",
        context_mode="custom",
        context_filter=lambda messages: selection,
    )
    _prepare_agent_task(host, sink, "sink", prior, "workflow", ledger)
    original_ids = host.prepare_agent_task.call_args.kwargs["context_message_ids"]
    assert len(set(original_ids)) == 2 and original_ids[0] == "ancestor"
    # Both branch copies have the SAME known provenance, unlike two distinct
    # equal producer emissions. Switching branch aliases cannot redeliver them.
    selection[:] = [second, second]
    _prepare_agent_task(host, sink, "sink", prior, "workflow", ledger)
    assert host.prepare_agent_task.call_args.kwargs["context_message_ids"] == []
    # Forwarding both original branch positions still collapses their ancestor.
    selection[:] = [first, second]
    _prepare_agent_task(host, sink, "sink", prior, "workflow", ledger)
    assert host.prepare_agent_task.call_args.kwargs["context_message_ids"] == []
    selection[:] = [deepcopy(first)] * 3
    _prepare_agent_task(host, sink, "sink", prior, "workflow", ledger)
    additional = host.prepare_agent_task.call_args.kwargs["context_message_ids"]
    assert len(additional) == 1 and additional[0] not in original_ids


@pytest.mark.parametrize("shared_object", [False, True], ids=["branch-copies", "aliased-branch-slots"])
def test_adding_repetition_does_not_turn_inherited_branch_slots_into_new_emissions(shared_object: bool) -> None:
    first = Message("assistant", ["ancestor"], message_id="public")
    second = first if shared_object else deepcopy(first)
    prior = AgentExecutorResponse("join", AgentResponse(messages=[]), [first, second])
    host = Mock(instance_id="run")
    ledger = _WorkflowDeliveryLedger(instance_id="run")
    ledger.remember(prior, ["ancestor", "ancestor"], [])
    selection = [first, second, first]
    sink = AgentExecutor(
        Agent(client=RecordingChatClient(), name="sink"),
        id="sink",
        context_mode="custom",
        context_filter=lambda messages: selection,
    )

    def dispatch() -> list[str]:
        metadata = TaskMetadata("sink", prior, "join", TaskType.AGENT)
        _prepare_agent_task(host, sink, "sink", prior, "workflow", ledger, metadata)
        assert metadata.selected_context == selection
        assert metadata.selected_context_ids is not None
        return metadata.selected_context_ids

    ids = dispatch()
    # The first two slots are the SAME already-known ancestor. Only the third
    # slot is extra. Before slot-aware ranking this dispatched three IDs.
    assert ids[:2] == ["ancestor", "ancestor"] and ids[2] != "ancestor"
    assert host.prepare_agent_task.call_args.kwargs["context_message_ids"] == ["ancestor", ids[2]]
    selection[:] = [second, first, second]
    assert dispatch() == ids
    assert host.prepare_agent_task.call_args.kwargs["context_message_ids"] == []
    # Repeat the complete original slot set three times. Aliased and detached
    # branches must both describe three selections of one ancestor, not six.
    selection[:] = [first, second] * 3
    expanded = dispatch()
    assert expanded == ["ancestor", "ancestor", ids[2], ids[2], expanded[-1], expanded[-1]]
    assert len(set(expanded)) == 3
    assert host.prepare_agent_task.call_args.kwargs["context_message_ids"] == [expanded[-1]]
    selection[:] = [first, second]
    assert dispatch() == ["ancestor", "ancestor"]
    assert host.prepare_agent_task.call_args.kwargs["context_message_ids"] == []
    # An unrelated synthesized message must not switch known ancestor slots to
    # fallback ranking or redeliver the existing repeated selection.
    unrelated = Message("assistant", ["newly synthesized"])
    selection[:] = [first, second, first, unrelated]
    mixed = dispatch()
    assert mixed[:3] == ids
    assert host.prepare_agent_task.call_args.args[3] == [unrelated.to_dict()]
    assert host.prepare_agent_task.call_args.kwargs["context_message_ids"] == [mixed[-1]]


def test_selection_copy_receipts_are_isolated_per_target() -> None:
    source = Message("assistant", ["equal"])
    prior = AgentExecutorResponse("source", AgentResponse(messages=[source]), [source])
    host = Mock(instance_id="run")
    ledger = _WorkflowDeliveryLedger(instance_id="run")
    deliveries: dict[str, list[str]] = {}
    for target in ("left", "right", "left", "right"):
        sink = AgentExecutor(
            Agent(client=RecordingChatClient(), name=target),
            id=target,
            context_mode="custom",
            context_filter=lambda messages: [messages[0], messages[0]],
        )
        _prepare_agent_task(host, sink, target, prior, "workflow", ledger)
        ids = host.prepare_agent_task.call_args.kwargs["context_message_ids"]
        if target not in deliveries:
            assert len(set(ids)) == 2
            deliveries[target] = ids
        else:
            assert ids == []
    assert deliveries["left"][0] == deliveries["right"][0]
    assert deliveries["left"][1] != deliveries["right"][1]


def test_registered_branch_join_does_not_redeliver_ancestor_when_one_slot_is_repeated() -> None:
    class Seed(Executor):
        @handler(input=str, output=AgentExecutorResponse)
        async def handle(self, message: str, ctx: WorkflowContext[AgentExecutorResponse]) -> None:
            ancestor = Message("assistant", ["ancestor"], message_id="ancestor")
            await ctx.send_message(AgentExecutorResponse(self.id, AgentResponse(messages=[ancestor]), [ancestor]))

    class Branch(Executor):
        @handler(input=AgentExecutorResponse, output=AgentExecutorResponse)
        async def handle(self, prior: AgentExecutorResponse, ctx: WorkflowContext[AgentExecutorResponse]) -> None:
            marker = Message("assistant", [self.id], message_id=self.id)
            await ctx.send_message(
                AgentExecutorResponse(self.id, AgentResponse(messages=[marker]), [*prior.full_conversation, marker])
            )

    class Join(Executor):
        @handler(input=list[AgentExecutorResponse], output=AgentExecutorResponse)
        async def handle(
            self, priors: list[AgentExecutorResponse], ctx: WorkflowContext[AgentExecutorResponse]
        ) -> None:
            first, second = (prior.full_conversation[0] for prior in priors)
            assert first is not second and first.to_dict() == second.to_dict()
            # Forward the complete positional histories so both ancestor slots
            # retain their checkpoint provenance, then project at the sink.
            history = [message for prior in priors for message in prior.full_conversation]
            await ctx.send_message(AgentExecutorResponse(self.id, AgentResponse(messages=[]), history))

    def select(messages: list[Message]) -> list[Message]:
        if len(messages) == 1:
            return [messages[0], messages[0]]
        assert len(messages) == 4
        return [messages[0], messages[2], messages[0]]

    seed, left, right, join = Seed(id="seed"), Branch(id="left"), Branch(id="right"), Join(id="join")
    client = RecordingChatClient()
    agent = Agent(client=client, name="sink")
    sink = AgentExecutor(agent, id="sink", context_mode="custom", context_filter=select)
    workflow = (
        WorkflowBuilder(name="branch-selection", start_executor=seed, output_from=[sink])
        .add_fan_out_edges(seed, [sink, left, right])
        .add_fan_in_edges([left, right], join)
        .add_edge(join, sink)
        .build()
    )
    provider = JsonStateProvider(session_id="root-run", entity_name="dafx-branch-selection-sink")
    requests: list[dict[str, Any]] = []

    def entity_call(entity_id: Any, operation: str, request: dict[str, Any]) -> Any:
        nonlocal provider
        assert (entity_id.entity, entity_id.key, operation) == ("dafx-branch-selection-sink", "root-run", "run")
        requests.append(deepcopy(request))
        provider = JsonStateProvider(provider.raw, session_id="root-run", entity_name="dafx-branch-selection-sink")
        response = asyncio.run(AgentEntity(agent, state_provider=provider).run(deepcopy(request)))
        assert provider.successful_writes == 1 and response.additional_properties.get("durable_status") != "error"
        return _complete(serialize_agent_response(response))

    _drain(_registered_run(workflow, entity_call)[0])
    assert len(requests) == len(client.received_messages) == 2
    assert len(set(requests[0]["contextMessageIds"])) == 2
    assert requests[1]["contextMessageIds"] == requests[1]["contextMessages"] == []
    assert [sum(message.text == "ancestor" for message in batch) for batch in client.received_messages] == [2, 2]


def test_known_selection_regression_oracle_rejects_ephemeral_copy_receipts(monkeypatch: pytest.MonkeyPatch) -> None:
    # Isolated mutation of the new receipt storage, not a source checkout/reset.
    # The mutant must violate the zero-redelivery contract after an identical
    # first dispatch. No requests or wall clocks are used in this probe.
    class ForgetCopies(dict[tuple[str, str, int], str]):
        def setdefault(self, key: tuple[str, str, int], default: str = "") -> str:
            return default

    original_fork = engine._WorkflowDeliveryLedger.fork

    def forget_fork(ledger: Any) -> Any:
        staged = original_fork(ledger)
        staged.selection_copies = ForgetCopies(staged.selection_copies)
        return staged

    source = Message("assistant", ["equal"])
    prior = AgentExecutorResponse("source", AgentResponse(messages=[source]), [source])
    sink = AgentExecutor(
        Agent(client=RecordingChatClient(), name="sink"),
        id="sink",
        context_mode="custom",
        context_filter=lambda messages: [messages[0], messages[0]],
    )

    def deliveries() -> list[list[str]]:
        host, ledger = Mock(instance_id="run"), _WorkflowDeliveryLedger(instance_id="run")
        results = []
        for _ in range(2):
            _prepare_agent_task(host, sink, "sink", prior, "workflow", ledger)
            results.append(host.prepare_agent_task.call_args.kwargs["context_message_ids"])
        return results

    ordinary = deliveries()
    assert len(ordinary[0]) == 2 and ordinary[1] == []
    monkeypatch.setattr(engine._WorkflowDeliveryLedger, "fork", forget_fork)
    ephemeral = deliveries()
    assert ephemeral[0] == ordinary[0]
    assert len(ephemeral[1]) == 1 and ephemeral[1][0] not in ephemeral[0]


@pytest.mark.parametrize("copies", [1, 2])
def test_sdk_episode_replay_rebuilds_known_selection_receipts(copies: int, monkeypatch: pytest.MonkeyPatch) -> None:
    workflow, agent, client, _ = _selection_workflow("same-id", copies, False)
    native = _worker(workflow)
    instance = "selection-run"
    provider = JsonStateProvider(session_id=instance, entity_name="dafx-selection-occurrence-sink")
    history: list[Any] = []
    new = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(
            "dafx-selection-occurrence", instance, json.dumps({"_durable_workflow_version": 2, "input": "go"})
        ),
    ]
    snapshots: list[tuple[list[Any], list[Any], list[Any]]] = []
    requests: list[dict[str, Any]] = []
    activity_executor = _ActivityExecutor(native._registry, logging.getLogger(__name__), native._data_converter)
    for _ in range(16):
        result = _replay(native, instance, history, new)
        snapshots.append((deepcopy(history), deepcopy(new), deepcopy(list(result.actions))))
        history.extend(new)
        if len(result.actions) == 1 and result.actions[0].HasField("completeOrchestration"):
            assert result.actions[0].completeOrchestration.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
            break
        assert result.actions, "A workflow without HITL must make progress"
        completions: list[Any] = []
        for action in result.actions:
            if action.HasField("scheduleTask"):
                scheduled = action.scheduleTask
                history.append(helpers.new_task_scheduled_event(action.id, scheduled.name, scheduled.input.value))
                produced = activity_executor.execute(instance, scheduled.name, action.id, scheduled.input.value)
                completions.append(helpers.new_task_completed_event(action.id, produced))
            else:
                assert action.HasField("sendEntityMessage")
                called = action.sendEntityMessage.entityOperationCalled
                assert called.operation == "run"
                history.append(pb.HistoryEvent(eventId=action.id, entityOperationCalled=called))
                request = json.loads(called.input.value)
                requests.append(request)
                provider = JsonStateProvider(
                    provider.raw, session_id=instance, entity_name="dafx-selection-occurrence-sink"
                )
                response = asyncio.run(AgentEntity(agent, state_provider=provider).run(deepcopy(request)))
                assert provider.successful_writes == 1
                assert response.additional_properties.get("durable_status") != "error"
                completions.append(
                    pb.HistoryEvent(
                        eventId=-1,
                        entityOperationCompleted=pb.EntityOperationCompletedEvent(
                            requestId=called.requestId,
                            output=helpers.get_string_value(json.dumps(serialize_agent_response(response))),
                        ),
                    )
                )
        new = [helpers.new_orchestrator_started_event(), *completions]
    else:
        pytest.fail("Exceeded the bounded SDK episode limit")

    assert len(requests) == len(client.received_messages) == 2
    assert len(requests[0]["contextMessageIds"]) == 2 * copies
    assert requests[1]["contextMessages"] == requests[1]["contextMessageIds"] == []

    class ReplayClock(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> Self:
            assert tz is timezone.utc
            return cls(2040, 1, 1, tzinfo=timezone.utc)

    # Only the request factory's wall clock changes. SDK history timestamps,
    # recorded producer results, occurrence IDs and UUID generation stay real.
    monkeypatch.setattr(_models, "datetime", ReplayClock)
    fresh_workflow, _, fresh_client, _ = _selection_workflow("same-id", copies, False)
    fresh = _worker(fresh_workflow)
    changed_timestamps = 0
    for old, incoming, expected in snapshots:
        replayed = _replay(fresh, instance, old, incoming)
        assert len(replayed.actions) == len(expected)
        for actual, recorded in zip(replayed.actions, expected, strict=True):
            if recorded.HasField("sendEntityMessage"):
                actual_request = json.loads(actual.sendEntityMessage.entityOperationCalled.input.value)
                recorded_request = json.loads(recorded.sendEntityMessage.entityOperationCalled.input.value)
                assert actual_request["created_at"] == "2040-01-01T00:00:00+00:00"
                assert actual_request["created_at"] != recorded_request["created_at"]
                changed_timestamps += 1
            _assert_same_action_contract(actual, recorded)
    assert changed_timestamps == len(requests) == 2
    assert fresh_client.received_messages == []
