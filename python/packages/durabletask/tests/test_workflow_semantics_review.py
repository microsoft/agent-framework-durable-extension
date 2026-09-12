# Copyright (c) Microsoft. All rights reserved.

"""Logical core conversations are independent of durable transport occurrences."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from agent_framework import (
    AgentExecutor,
    AgentExecutorRequest,
    AgentExecutorResponse,
    AgentResponse,
    AgentSession,
    Content,
    Executor,
    Message,
    Workflow,
    WorkflowBuilder,
    WorkflowExecutor,
)
from agent_framework._workflows._edge import EdgeGroup, FanInEdgeGroup, FanOutEdgeGroup, SingleEdgeGroup
from durabletask.task import CompletableTask, OrchestrationContext

from agent_framework_durabletask import AgentEntity, AgentEntityStateProviderMixin, RunRequest, serialize_agent_response
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._workflows.dt_context import DurableTaskWorkflowContext
from agent_framework_durabletask._workflows.orchestrator import (
    TaskMetadata,
    TaskType,
    _build_context_messages,
    _prepare_agent_task,
    _process_agent_response,
    _WorkflowDeliveryLedger,
    run_workflow_orchestrator,
)
from agent_framework_durabletask._workflows.serialization import (
    SUBWORKFLOW_RESULT_KEY,
    deserialize_value,
    serialize_value,
)


class _Agent:
    name = "stub"
    id = "stub"
    description = None

    def __init__(self, response: AgentResponse | None = None) -> None:
        self.response = response if response is not None else AgentResponse(messages=[])
        self.inputs: list[list[dict[str, Any]]] = []

    def create_session(self, **kwargs: Any) -> AgentSession:
        return AgentSession(**kwargs)

    async def run(self, messages: list[Message], **kwargs: Any) -> AgentResponse:
        self.inputs.append(_wire(messages))
        return self.response


def _agent(name: str, response: AgentResponse | None = None, **kwargs: Any) -> AgentExecutor:
    stub: Any = _Agent(response)
    return AgentExecutor(stub, id=name, **kwargs)


def _wire(messages: list[Message]) -> list[dict[str, Any]]:
    return [message.to_dict() for message in messages]


def _envelope(
    messages: list[Message], producer: str = "source", latest: list[Message] | None = None
) -> AgentExecutorResponse:
    return AgentExecutorResponse(
        producer, AgentResponse(messages=messages[-1:] if latest is None else latest), messages
    )


def _activity(name: str) -> Any:
    node = Mock(spec=Executor)
    node.id = name
    node.input_types = [str]
    return node


def _workflow(nodes: list[Any], edges: list[EdgeGroup]) -> Any:
    workflow = Mock(spec=Workflow)
    workflow.name = "review"
    workflow.start_executor_id = nodes[0].id
    workflow.executors = {node.id: node for node in nodes}
    workflow.edge_groups = edges
    workflow.max_iterations = 30
    return workflow


def _send(messages: list[Any], target: str | None = None, *, wait: bool = False) -> dict[str, Any]:
    result: dict[str, Any] = {
        "sent_messages": [{"message": serialize_value(message), "target_id": target} for message in messages]
    }
    if wait:
        result["pending_request_info_events"] = [
            {"request_id": "approval", "source_executor_id": "gate", "data": "review"}
        ]
    return result


class _Host:
    supports_event_streaming = False
    current_utc_datetime = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def __init__(
        self,
        responses: Mapping[str, AgentResponse | dict[str, Any]] | None = None,
        *,
        activities: dict[str, list[dict[str, Any]]] | None = None,
        children: list[Any] | None = None,
        instance_id: str = "run",
        is_replaying: bool = False,
    ) -> None:
        self.instance_id = instance_id
        self.is_replaying = is_replaying
        self.calls: list[dict[str, Any]] = []
        self.responses = responses or {}
        self.activities = {key: iter(values) for key, values in (activities or {}).items()}
        self.children = iter(children or [])
        self.child_ids: list[str | None] = []
        self.waits: list[str] = []
        self.batches: list[int] = []
        self.fail_prepare = False

    def prepare_agent_task(
        self,
        executor_id: str,
        message: str,
        orchestration_instance_id: str,
        context_messages: list[dict[str, Any]] | None = None,
        context_message_ids: list[str] | None = None,
    ) -> AgentResponse | dict[str, Any]:
        assert (context_messages is None) == (context_message_ids is None)
        if context_messages is not None:
            assert context_message_ids is not None
            assert len(context_messages) == len(context_message_ids)
        self.calls.append(
            json.loads(
                json.dumps(
                    {
                        "executor": executor_id,
                        "instance": orchestration_instance_id,
                        "message": message,
                        "contextMessages": context_messages,
                        "contextMessageIds": context_message_ids,
                    },
                    allow_nan=False,
                )
            )
        )
        if self.fail_prepare:
            raise OSError("prepare failed")
        return self.responses.get(executor_id, AgentResponse(messages=[Message("assistant", ["approved"])]))

    def prepare_activity_task(self, activity_name: str, input_json: str) -> str:
        return json.dumps(next(self.activities[json.loads(input_json)["executor_id"]]))

    def call_sub_orchestrator(self, name: str, input: Any, instance_id: str | None = None) -> Any:
        self.child_ids.append(instance_id)
        return next(self.children)

    def task_all(self, tasks: list[Any]) -> list[Any]:
        self.batches.append(len(tasks))
        return tasks

    def task_any(self, tasks: list[Any]) -> Any:
        raise AssertionError("These workflows do not race tasks")

    def set_custom_status(self, status: Any) -> None:
        pass

    def wait_for_external_event(self, name: str) -> str:
        self.waits.append(name)
        return "approved"

    def create_timer(self, fire_at: datetime) -> Any:
        raise AssertionError("These workflows have no timers")

    def new_uuid(self) -> str:
        raise AssertionError("Message identity must not require UUIDs")

    def cancel_task(self, task: Any) -> None:
        raise AssertionError("These workflows do not cancel tasks")

    def get_task_result(self, task: Any) -> Any:
        return task


def _run(host: _Host, workflow: Any, message: Any = "start") -> Any:
    generator = run_workflow_orchestrator(host, workflow, message)
    result: Any = None
    while True:
        try:
            result = generator.send(result)
        except StopIteration as completed:
            return completed.value


def _turn(
    host: _Host, executor: AgentExecutor, message: Any, ledger: _WorkflowDeliveryLedger
) -> tuple[dict[str, Any], AgentExecutorResponse]:
    metadata = TaskMetadata(executor.id, message, "source", TaskType.AGENT)
    result = _prepare_agent_task(host, executor, executor.id, message, "review", ledger, metadata)
    response = _process_agent_response(result, executor.id, message, ledger, metadata).output_message
    assert response is not None
    return host.calls[-1], response


def _ids(call: dict[str, Any]) -> list[str]:
    ids = call["contextMessageIds"]
    assert isinstance(ids, list)
    assert len(ids) == len(call["contextMessages"])
    assert all(isinstance(value, str) and value for value in ids)
    return ids


def _texts(call: dict[str, Any]) -> list[str]:
    return [Message.from_dict(message).text for message in call["contextMessages"]]


async def _core_turn(executor: AgentExecutor, message: Any) -> AgentExecutorResponse:
    context = Mock()
    context.source_executor_ids = ["source"]
    context.is_streaming.return_value = False
    context.get_state.return_value = {}
    context.send_message = AsyncMock()
    context.yield_output = AsyncMock()
    if isinstance(message, AgentExecutorResponse):
        await executor.from_response(message, context)
    elif isinstance(message, str):
        await executor.from_str(message, context)
    elif isinstance(message, AgentExecutorRequest):
        await executor.run(message, context)
    elif isinstance(message, Message):
        await executor.from_message(message, context)
    else:
        await executor.from_messages(message, context)
    return context.send_message.call_args.args[0]


def _redact(messages: list[Message]) -> list[Message]:
    selected = Message.from_dict(next(message for message in messages if message.message_id == "opaque").to_dict())
    selected.contents = [Content.from_text("redacted")]
    return [selected, Message("system", ["summary"], additional_properties={"_is_summary": True})]


@pytest.mark.parametrize("mode", ["full", "last_agent", "custom"])
@pytest.mark.parametrize("serialized", [False, True])
async def test_three_hops_match_real_core_selected_cache_and_actual_response(mode: str, serialized: bool) -> None:
    responses = {
        "A": AgentResponse(messages=[Message("assistant", ["secret"], message_id="opaque")]),
        "B": AgentResponse(
            messages=[
                Message(
                    "assistant", [Content.from_function_call("call", "lookup", arguments={"id": 7})], message_id="call"
                ),
                Message("tool", [Content.from_function_result("call", result={"answer": 42})], message_id="tool"),
                Message(
                    "assistant",
                    [Content.from_uri("https://example.com/image.png", media_type="image/png")],
                    message_id="picture",
                    additional_properties={"label": "original"},
                ),
            ],
            response_id="original-response",
            additional_properties={"provider": {"opaque": True}},
        ),
        "C": AgentResponse(messages=[]),
    }
    options: dict[str, Any] = {"context_mode": mode, "context_filter": _redact if mode == "custom" else None}
    core_a = await _core_turn(_agent("A", responses["A"]), "question")
    core_b = await _core_turn(_agent("B", responses["B"], **options), core_a)
    core_c = await _core_turn(_agent("C", responses["C"]), core_b)
    before = {name: response.to_dict() for name, response in responses.items()}
    seen: list[AgentExecutorResponse] = []

    def capture(response: AgentExecutorResponse) -> bool:
        seen.append(response)
        return True

    workflow = _workflow(
        [_agent("A"), _agent("B", **options), _agent("C")],
        [SingleEdgeGroup("A", "B"), SingleEdgeGroup("B", "C", condition=capture)],
    )
    payloads = {
        f"review-{name}": serialize_agent_response(response) if serialized else response
        for name, response in responses.items()
    }
    host = _Host(payloads)
    assert _run(host, workflow, "question") == []
    assert host.calls[1]["contextMessages"] == _wire(core_b.full_conversation[: -len(responses["B"].messages)])
    assert host.calls[2]["contextMessages"] == _wire(core_c.full_conversation)
    assert seen[0].agent_response.to_dict() == responses["B"].to_dict()
    assert _wire(seen[0].full_conversation) == _wire(core_b.full_conversation)
    if not serialized:
        assert seen[0].agent_response is responses["B"]
    if mode == "custom":
        assert "secret" not in json.dumps(host.calls[2])
        assert _texts(host.calls[2])[:2] == ["redacted", "summary"]
    assert {name: response.to_dict() for name, response in responses.items()} == before


def test_serialized_response_tolerates_unknown_delivery_fields_without_mutating_payload() -> None:
    response = AgentResponse(
        messages=[Message("assistant", ["approved"], message_id="opaque")],
        response_id="actual-response",
        additional_properties={"provider": {"opaque": True}},
    )
    payload = serialize_agent_response(response)
    payload["future_delivery_metadata"] = {"type": "provider_extension", "opaque": [1]}
    before = deepcopy(payload)
    _, outgoing = _turn(_Host({"review-A": payload}), _agent("A"), "question", _WorkflowDeliveryLedger())
    assert outgoing.agent_response.to_dict() == response.to_dict()
    assert outgoing.full_conversation[-1].message_id == "opaque"
    assert payload == before


@pytest.mark.parametrize("application_id", ["opaque", "wf_source_0", "wf:external:" + "a" * 64])
def test_later_filter_observes_original_application_ids_not_transport_namespaces(application_id: str) -> None:
    original = Message("assistant", ["approved"], message_id=application_id, additional_properties={"nested": [1]})
    upstream = _envelope([original])
    host, ledger = _Host(), _WorkflowDeliveryLedger()
    _, outgoing = _turn(host, _agent("B"), upstream, ledger)
    executor = _agent(
        "C",
        context_mode="custom",
        context_filter=lambda messages: [m for m in messages if m.message_id == application_id],
    )
    call, _ = _turn(host, executor, outgoing, ledger)
    assert call["contextMessages"] == [original.to_dict()]
    assert _ids(call) == _ids(host.calls[0])
    assert _build_context_messages(executor, outgoing) == [original.to_dict()]
    assert outgoing.full_conversation[0] is original


def test_selected_full_context_not_just_delta_becomes_the_next_conversation() -> None:
    messages = [Message("user", [str(i)], message_id=f"app-{i}") for i in range(4)]
    upstream = _envelope(messages)
    host, ledger = _Host(), _WorkflowDeliveryLedger()
    first_executor = _agent("B", context_mode="custom", context_filter=lambda values: [values[1], values[3]])
    _turn(host, first_executor, upstream, ledger)
    next_executor = _agent("B", context_mode="custom", context_filter=lambda values: [values[2], values[0], values[3]])
    call, outgoing = _turn(host, next_executor, upstream, ledger)
    assert _texts(call) == ["2", "0"]
    assert _wire(outgoing.full_conversation[:-1]) == _wire([messages[2], messages[0], messages[3]])
    repeated, _ = _turn(host, _agent("B"), upstream, ledger)
    assert repeated["contextMessages"] == _ids(repeated) == []
    assert repeated["message"] == ""
    downstream, _ = _turn(host, _agent("C"), outgoing, ledger)
    assert _texts(downstream) == ["2", "0", "3", "approved"]


def test_detached_copy_updates_keep_occurrence_but_changed_fingerprint_is_delivered() -> None:
    original = Message("assistant", ["secret"], message_id="opaque")
    upstream = _envelope([original])
    host, ledger = _Host(), _WorkflowDeliveryLedger()
    first, _ = _turn(host, _agent("B"), upstream, ledger)
    redacted = _agent("B", context_mode="custom", context_filter=lambda messages: _redact(messages)[:1])
    changed, outgoing = _turn(host, redacted, upstream, ledger)
    repeated, _ = _turn(host, redacted, upstream, ledger)
    assert _ids(changed) == _ids(first)
    assert _texts(changed) == ["redacted"]
    assert changed["contextMessages"][0]["message_id"] == "opaque"
    assert repeated["contextMessages"] == _ids(repeated) == []
    assert message_identity(original) != message_identity(outgoing.full_conversation[0])
    assert original.text == "secret"


@pytest.mark.parametrize("detached", [False, True])
def test_source_selection_order_and_copies_have_parallel_occurrence_ids(detached: bool) -> None:
    messages = [Message("user", [str(i)]) for i in range(4)]
    source = _envelope(messages, latest=[])

    def projection(indices: list[int]) -> Callable[[list[Message]], list[Message]]:
        return lambda values: [Message.from_dict(values[i].to_dict()) if detached else values[i] for i in indices]

    host, ledger = _Host(), _WorkflowDeliveryLedger()
    first, _ = _turn(host, _agent("B", context_mode="custom", context_filter=projection([1, 3])), source, ledger)
    second, _ = _turn(host, _agent("B", context_mode="custom", context_filter=projection([2, 0, 3])), source, ledger)
    other, _ = _turn(host, _agent("C", context_mode="custom", context_filter=projection([3, 1])), source, ledger)
    assert _texts(first) == ["1", "3"]
    assert _texts(second) == ["2", "0"]
    assert _ids(other) == list(reversed(_ids(first)))
    assert len(set(_ids(first) + _ids(second))) == 4
    assert all(message.message_id is None for message in messages)


def test_synthesized_detached_messages_are_handoff_scoped_even_with_equal_ids() -> None:
    source = _envelope([Message("user", ["source"])])
    executor = _agent(
        "B",
        context_mode="custom",
        context_filter=lambda _: [
            Message("system", ["summary"], message_id="summary"),
            Message("system", ["summary"], message_id="summary"),
        ],
    )

    def replay() -> list[dict[str, Any]]:
        host, ledger = _Host(), _WorkflowDeliveryLedger()
        for _ in range(2):
            _turn(host, executor, deepcopy(source), ledger)
        return host.calls

    calls = replay()
    assert len(set(_ids(calls[0]) + _ids(calls[1]))) == 4
    assert calls == replay()
    assert all(message["message_id"] == "summary" for call in calls for message in call["contextMessages"])


@pytest.mark.parametrize("kind", ["raw", "anonymous", "same-id"])
@pytest.mark.parametrize("pause", [False, True])
def test_independent_producer_events_do_not_collide_across_sequential_or_hitl_dispatch(kind: str, pause: bool) -> None:
    values: list[Any] = (
        ["same prompt", "same prompt"]
        if kind == "raw"
        else [
            _envelope([Message("assistant", ["approved"], message_id="opaque" if kind == "same-id" else None)])
            for _ in range(2)
        ]
    )
    activities = {
        "gate": [_send(values[:1], "A", wait=True), _send(values[1:], "A")] if pause else [_send(values, "A")]
    }
    workflow = _workflow([_activity("gate"), _agent("A"), _agent("B")], [SingleEdgeGroup("A", "B")])
    live, replay = _Host(activities=activities), _Host(activities=activities, is_replaying=True)
    assert _run(live, workflow) == _run(replay, workflow) == []
    assert live.calls == replay.calls
    consumers = [call for call in live.calls if call["executor"] == "review-B"]
    assert len(consumers) == 2
    assert _texts(consumers[0]) == _texts(consumers[1]) == ["same prompt" if kind == "raw" else "approved", "approved"]
    assert set(_ids(consumers[0])).isdisjoint(_ids(consumers[1]))
    assert live.waits == (["approval"] if pause else [])


def test_child_invocations_scope_equal_logical_ids_at_the_child_boundary() -> None:
    child = Mock(spec=WorkflowExecutor)
    child.id = "child"
    child.workflow = Mock(name="inner")
    child.workflow.name = "inner"
    child.allow_direct_output = False
    outputs = [_envelope([Message("assistant", ["approved"], message_id="wf_inner_0")], "inner") for _ in range(2)]
    children = [
        {SUBWORKFLOW_RESULT_KEY: True, "outputs": [serialize_value(output)], "events": []} for output in outputs
    ]
    workflow = _workflow([_activity("gate"), child, _agent("B")], [SingleEdgeGroup("child", "B")])
    activities = {"gate": [_send(["same", "same"], "child")]}
    host, replay = _Host(activities=activities, children=children), _Host(activities=activities, children=children)
    assert _run(host, workflow) == _run(replay, workflow) == []
    assert host.child_ids == ["run::child::0", "run::child::1"]
    assert host.calls == replay.calls
    assert _texts(host.calls[0]) == _texts(host.calls[1]) == ["approved"]
    assert set(_ids(host.calls[0])).isdisjoint(_ids(host.calls[1]))
    assert [call["contextMessages"][0]["message_id"] for call in host.calls] == ["wf_inner_0"] * 2


def test_activity_forwarded_copies_reuse_source_positions_but_new_output_is_an_event() -> None:
    original = Message("user", ["question"], message_id="question")
    source = _envelope([original, Message("assistant", ["approved"], message_id="opaque")], "A")
    host, ledger = _Host(), _WorkflowDeliveryLedger(instance_id="run")
    first, _ = _turn(host, _agent("B"), source, ledger)
    transformed = _envelope(
        [Message.from_dict(original.to_dict()), Message("assistant", ["approved"], message_id="opaque")], "A"
    )
    # This is the association routing makes between an activity's input and output.
    ledger.identify(transformed, source)
    second, _ = _turn(host, _agent("B"), transformed, ledger)
    assert _texts(first) == ["question", "approved"]
    assert _texts(second) == ["approved"]
    assert set(_ids(first)).isdisjoint(_ids(second))


def test_fanin_keeps_logical_selected_order_and_deduplicates_transport_per_target() -> None:
    responses = {"review-A": AgentResponse(messages=[Message("assistant", ["secret"], message_id="opaque")])}
    seen: list[AgentExecutorResponse] = []

    def capture(response: AgentExecutorResponse) -> bool:
        seen.append(response)
        return True

    workflow = _workflow(
        [
            _agent("A"),
            _agent("left", context_mode="custom", context_filter=_redact),
            _agent("right", context_mode="custom", context_filter=_redact),
            _agent("join"),
            _agent("end"),
        ],
        [
            FanOutEdgeGroup("A", ["left", "right"]),
            FanInEdgeGroup(["left", "right"], "join"),
            SingleEdgeGroup("join", "end", condition=capture),
        ],
    )
    host = _Host(responses)
    assert _run(host, workflow) == []
    joined = next(call for call in host.calls if call["executor"] == "review-join")
    assert _texts(joined) == ["redacted", "summary", "approved", "summary", "approved"]
    assert [message.text for message in seen[0].full_conversation] == [
        "redacted",
        "summary",
        "approved",
        "redacted",
        "summary",
        "approved",
        "approved",
    ]
    assert "secret" not in json.dumps(joined)
    assert len(_ids(joined)) == 5


@pytest.mark.parametrize("kind", ["string", "message", "messages", "mixed", "request"])
async def test_all_core_input_handlers_keep_all_messages_and_contents(kind: str) -> None:
    message = Message(
        "tool",
        [Content.from_function_result("call", result={"data": [0, False, None]})],
        message_id="opaque",
        additional_properties={"source": "app"},
    )
    inputs: dict[str, Any] = {
        "string": "hello",
        "message": message,
        "messages": [message, message],
        "mixed": ["hello", message],
        "request": AgentExecutorRequest([message, message]),
    }
    core = await _core_turn(_agent("A"), inputs[kind])
    host = _Host({"review-A": AgentResponse(messages=[])})
    workflow = _workflow([_agent("A"), _agent("B")], [SingleEdgeGroup("A", "B")])
    assert _run(host, workflow, inputs[kind]) == []
    assert host.calls[1]["contextMessages"] == _wire(core.full_conversation)
    assert len(set(_ids(host.calls[1]))) == len(core.full_conversation)
    assert message.message_id == "opaque"


@pytest.mark.parametrize("value", [None, [], AgentExecutorRequest([])])
def test_empty_inputs_are_explicit_empty_context_not_text_fallback(value: Any) -> None:
    host = _Host()
    assert _run(host, _workflow([_agent("A")], []), value) == []
    assert host.calls[0]["contextMessages"] == _ids(host.calls[0]) == []
    assert host.calls[0]["message"] == ""


@pytest.mark.parametrize("pause", [False, True])
@pytest.mark.parametrize("prior_run", [False, True])
def test_cache_only_requests_schedule_nothing_and_flush_all_messages_on_next_run(pause: bool, prior_run: bool) -> None:
    cached = AgentExecutorRequest([Message("system", ["rules"], message_id="rules"), Message("user", ["draft"])], False)
    prefix: list[Any] = ["first"] if prior_run else []
    batches = (
        [_send([*prefix, cached], "A", wait=True), _send(["answer"], "A")]
        if pause
        else [_send([*prefix, cached, "answer"], "A")]
    )
    host = _Host(activities={"gate": batches})
    workflow = _workflow([_activity("gate"), _agent("A")], [])
    assert _run(host, workflow) == []
    assert len(host.calls) == 1 + int(prior_run)
    assert _texts(host.calls[-1]) == ["rules", "draft", "answer"]
    assert len(set(_ids(host.calls[-1]))) == 3
    assert host.waits == (["approval"] if pause else [])


def test_cache_only_workflow_does_not_yield_a_model_task() -> None:
    host = _Host()
    assert _run(host, _workflow([_agent("A")], []), AgentExecutorRequest([Message("user", ["later"])], False)) == []
    assert host.calls == host.batches == []


@pytest.mark.parametrize("failure", ["prepare", "serialization", "filter"])
def test_failed_preparation_does_not_consume_delivery_or_occurrence_ordinals(failure: str) -> None:
    source = _envelope([Message("user", ["question"])])
    host, ledger = _Host(), _WorkflowDeliveryLedger()

    def projection(messages: list[Message]) -> list[Message]:
        if failure == "filter":
            raise ValueError("filter failed")
        if failure == "serialization":
            return [Message("system", ["summary"], additional_properties={"bad": float("nan")})]
        return messages

    host.fail_prepare = failure == "prepare"
    with pytest.raises((TypeError, ValueError, OSError)):
        _turn(host, _agent("B", context_mode="custom", context_filter=projection), source, ledger)
    assert ledger == _WorkflowDeliveryLedger()
    host.fail_prepare = False
    call, _ = _turn(host, _agent("B"), source, ledger)
    clean, _ = _turn(_Host(), _agent("B"), deepcopy(source), _WorkflowDeliveryLedger())
    assert call == clean


@pytest.mark.parametrize("mode", ["full", "last_agent", "custom"])
def test_empty_projection_never_leaks_unselected_context_into_next_hop(mode: str) -> None:
    secret = Message("assistant", ["secret"])
    source = _envelope([] if mode == "full" else [secret], latest=[] if mode == "last_agent" else [secret])
    executor = _agent("B", context_mode=mode, context_filter=(lambda _: []) if mode == "custom" else None)
    host, ledger = _Host(), _WorkflowDeliveryLedger()
    first, outgoing = _turn(host, executor, source, ledger)
    assert first["contextMessages"] == _ids(first) == []
    assert first["message"] == ""
    second, _ = _turn(host, _agent("C"), outgoing, ledger)
    assert _texts(second) == ["approved"]
    assert "secret" not in json.dumps(second)


def test_growing_conversations_send_only_new_messages_and_parallel_ids() -> None:
    host, ledger = _Host(), _WorkflowDeliveryLedger()
    source: Any = "question"
    for _ in range(80):
        _, source = _turn(host, _agent("A"), source, ledger)
        call, _ = _turn(host, _agent("B"), source, ledger)
    assert _texts(call) == ["approved"]
    assert len(_ids(call)) == 1
    assert set(call) == {"executor", "instance", "message", "contextMessages", "contextMessageIds"}
    assert len(json.dumps(call)) < 500
    assert len(json.dumps(_wire(source.full_conversation))) > 5 * len(json.dumps(call))


@pytest.mark.parametrize("sequential", [False, True])
@pytest.mark.parametrize("status", ["error", "already_completed"])
def test_terminal_results_stop_routing_before_next_pending_model_call(sequential: bool, status: str) -> None:
    host = _Host(activities={"gate": [_send(["first", "second", "must not run"], "A")]})
    workflow = _workflow([_activity("gate"), _agent("A"), _agent("B")], [SingleEdgeGroup("A", "B")])
    generator = run_workflow_orchestrator(host, workflow, "start")
    yielded = generator.send(next(generator))
    if sequential:
        generator.send(yielded)
    error = AgentResponse(messages=[], additional_properties={"durable_status": status}).to_dict()
    error["unknown_field"] = {"private": "must not deserialize"}
    with pytest.raises(RuntimeError, match="expired durable response|terminal runtime error"):
        generator.send(error if sequential else [error])
    assert [call["executor"] for call in host.calls] == ["review-A"] * (2 if sequential else 1)


def test_application_identity_survives_activity_serialization() -> None:
    original = Message("assistant", ["approved"], message_id="opaque", additional_properties={"app": [1]})
    restored = deserialize_value(json.loads(json.dumps(serialize_value(_envelope([original])))))
    host, ledger = _Host(), _WorkflowDeliveryLedger()
    call, outgoing = _turn(host, _agent("B"), restored, ledger)
    assert call["contextMessages"] == [original.to_dict()]
    assert outgoing.full_conversation[0].to_dict() == original.to_dict()


def test_exact_whole_list_copy_preserves_repeated_anonymous_positions() -> None:
    source = _envelope([Message("user", ["same"]), Message("user", ["same"])], latest=[])
    host, ledger = _Host(), _WorkflowDeliveryLedger()
    first, _ = _turn(host, _agent("B"), source, ledger)
    copied, _ = _turn(
        host, _agent("B", context_mode="custom", context_filter=lambda messages: deepcopy(messages)), source, ledger
    )
    assert len(set(_ids(first))) == 2
    assert copied["contextMessages"] == _ids(copied) == []


def test_last_agent_uses_output_occurrence_when_same_object_is_also_input() -> None:
    shared = Message("assistant", ["same"], message_id="opaque")
    host = _Host({"review-A": AgentResponse(messages=[shared])})
    ledger = _WorkflowDeliveryLedger()
    _, outgoing = _turn(host, _agent("A"), AgentExecutorRequest([shared]), ledger)
    full, _ = _turn(host, _agent("B"), outgoing, ledger)
    latest, _ = _turn(host, _agent("C", context_mode="last_agent"), outgoing, ledger)
    assert len(set(_ids(full))) == 2
    assert _ids(latest) == _ids(full)[1:]
    assert shared.message_id == "opaque"


def test_workflow_instance_scopes_occurrences_without_changing_logical_messages() -> None:
    workflow = _workflow([_agent("A"), _agent("B")], [SingleEdgeGroup("A", "B")])
    first, second = _Host(instance_id="first"), _Host(instance_id="second")
    assert _run(first, workflow) == _run(second, workflow) == []
    assert first.calls[1]["contextMessages"] == second.calls[1]["contextMessages"]
    assert set(_ids(first.calls[1])).isdisjoint(_ids(second.calls[1]))


def test_projection_can_exclude_non_json_source_messages() -> None:
    excluded = Message("assistant", ["secret"], additional_properties={"invalid": float("nan")})
    selected = Message("user", ["selected"])
    source = _envelope([excluded, selected])
    executor = _agent(
        "B", context_mode="custom", context_filter=lambda messages: [Message.from_dict(messages[-1].to_dict())]
    )
    call, _ = _turn(_Host(), executor, source, _WorkflowDeliveryLedger())
    assert call["contextMessages"] == [selected.to_dict()]
    assert len(_ids(call)) == 1
    assert "secret" not in json.dumps(call)


def test_empty_string_retains_its_user_message_without_falling_back_to_none() -> None:
    host = _Host()
    assert _run(host, _workflow([_agent("A")], []), "") == []
    assert host.calls[0]["contextMessages"] == [Message("user", [""]).to_dict()]
    assert len(_ids(host.calls[0])) == 1
    assert host.calls[0]["message"] == ""


def test_reused_output_alias_retains_only_two_ambiguity_witnesses() -> None:
    shared = Message("assistant", ["same"], message_id="opaque")
    host = _Host({"review-A": AgentResponse(messages=[shared])})
    ledger = _WorkflowDeliveryLedger()
    calls: list[dict[str, Any]] = []
    for _ in range(12):
        _, output = _turn(host, _agent("A"), "question", ledger)
        call, _ = _turn(host, _agent("B", context_mode="last_agent"), output, ledger)
        calls.append(call)
    assert len(ledger.aliases[id(shared)][1]) == 2
    assert len({identity for call in calls for identity in _ids(call)}) == len(calls)
    assert all(call["contextMessages"] == [shared.to_dict()] for call in calls)
    assert shared.message_id == "opaque"


class _JsonStateProvider(AgentEntityStateProviderMixin):
    def __init__(self, name: str) -> None:
        self.name = name
        self.raw: dict[str, Any] = {}

    def _get_state_dict(self) -> dict[str, Any]:
        return json.loads(json.dumps(self.raw, allow_nan=False))

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        self.raw = json.loads(json.dumps(state, allow_nan=False))

    def _get_session_id_from_entity(self) -> str:
        return "adapter-run"

    def _get_entity_name_from_entity(self) -> str:
        return self.name


@pytest.mark.parametrize("adapter", ["dt", "af"])
@pytest.mark.parametrize("mode", ["full", "last_agent", "custom"])
async def test_real_adapter_three_hops_preserve_selected_context_through_receiver(adapter: str, mode: str) -> None:
    responses = {
        "A": AgentResponse(messages=[Message("assistant", ["secret"], message_id="opaque")]),
        "B": AgentResponse(
            messages=[
                Message(
                    "assistant",
                    [Content.from_function_call("call", "lookup", arguments={"id": 7})],
                    message_id="same-id",
                ),
                Message("tool", [Content.from_function_result("call", result={"answer": 42})], message_id="same-id"),
                Message(
                    "assistant",
                    [Content.from_uri("https://example.com/image.png", media_type="image/png")],
                    message_id="same-id",
                    additional_properties={"label": "original"},
                ),
            ],
            response_id="actual-response",
            additional_properties={"provider": {"opaque": True}},
        ),
        "C": AgentResponse(messages=[]),
    }
    options: dict[str, Any] = {"context_mode": mode, "context_filter": _redact if mode == "custom" else None}
    core_a = await _core_turn(_agent("A", responses["A"]), "question")
    core_b = await _core_turn(_agent("B", responses["B"], **options), core_a)
    core_c = await _core_turn(_agent("C", responses["C"]), core_b)
    expected_b = _wire(core_b.full_conversation[: -len(responses["B"].messages)])
    expected_c = _wire(core_c.full_conversation)
    original_responses = {name: response.to_dict() for name, response in responses.items()}
    agents: dict[str, Any] = {name: _Agent(response) for name, response in responses.items()}
    providers = {name: _JsonStateProvider(name) for name in agents}
    entities = {name: AgentEntity(agent, state_provider=providers[name]) for name, agent in agents.items()}
    observed: list[AgentExecutorResponse] = []

    def capture(response: AgentExecutorResponse) -> bool:
        observed.append(response)
        return True

    a, b, c = _agent("A"), _agent("B", **options), _agent("C")
    workflow = (
        WorkflowBuilder(name="review", start_executor=a, output_from=[c])
        .add_edge(a, b)
        .add_edge(b, c, condition=capture)
        .build()
    )
    if adapter == "dt":
        native = Mock(spec=OrchestrationContext)
        children: list[Any] = [CompletableTask() for _ in agents]
        context: Any = DurableTaskWorkflowContext(native)
    else:
        df = pytest.importorskip("azure.durable_functions")
        af_context = pytest.importorskip("agent_framework_azurefunctions._workflow_af_context")
        from azure.durable_functions.models.actions.NoOpAction import NoOpAction
        from azure.durable_functions.models.Task import AtomicTask

        native = Mock(spec=df.DurableOrchestrationContext)
        children = [AtomicTask(index, NoOpAction()) for index in range(len(agents))]
        native.task_all.side_effect = lambda tasks: tasks
        context = af_context.AzureFunctionsWorkflowContext(native)
    native.instance_id = "adapter-run"
    native.is_replaying = False
    native.current_utc_datetime = datetime(2026, 1, 1, tzinfo=timezone.utc)
    native.new_uuid.side_effect = [str(UUID(int=index + 1)) for index in range(len(agents))]
    native.call_entity.side_effect = children
    orchestration = run_workflow_orchestrator(context, workflow, "question")
    yielded = next(orchestration)
    wires: list[dict[str, Any]] = []
    for index, name in enumerate(agents):
        assert native.call_entity.call_count == index + 1
        entity_id, operation, payload = native.call_entity.call_args.args
        expected_name = f"dafx-review-{name}".lower() if adapter == "dt" else f"dafx-review-{name}"
        assert (entity_id.entity if adapter == "dt" else entity_id.name) == expected_name
        assert entity_id.key == "adapter-run"
        assert operation == "run"
        wire = json.loads(json.dumps(payload, allow_nan=False))
        wires.append(wire)
        response = await entities[name].run(wire)
        result = json.loads(json.dumps(serialize_agent_response(response), allow_nan=False))
        assert result == original_responses[name]
        if adapter == "dt":
            children[index].complete(result)
            completed_results = context.get_task_result(yielded)
        else:
            children[index].set_value(is_error=False, value=result)
            completed_results = [context.get_task_result(task) for task in yielded]
        assert len(completed_results) == 1
        assert completed_results[0].to_dict() == result
        if name == "C":
            with pytest.raises(StopIteration) as completed:
                orchestration.send(completed_results)
            # C is the designated output executor, even when its original response has no messages.
            final_outputs = [deserialize_value(output) for output in completed.value.value]
            assert len(final_outputs) == 1
            assert isinstance(final_outputs[0], AgentResponse)
            assert final_outputs[0].to_dict() == original_responses["C"]
        else:
            yielded = orchestration.send(completed_results)

    for index, (name, expected) in enumerate([("B", expected_b), ("C", expected_c)], start=1):
        wire = wires[index]
        assert wire["contextMessages"] == expected
        assert len(wire["contextMessageIds"]) == len(expected)
        assert RunRequest.from_dict(wire).context_message_ids == wire["contextMessageIds"]
        assert agents[name].inputs == [expected]
        receipts = providers[name].raw["data"]["ingestedMessages"]
        assert receipts == {
            identity: [message_identity(Message.from_dict(message))]
            for identity, message in zip(wire["contextMessageIds"], expected, strict=True)
        }
    assert wires[2]["contextMessageIds"][: len(expected_b)] == wires[1]["contextMessageIds"]
    assert len(set(wires[2]["contextMessageIds"][-3:])) == 3
    assert [message.get("message_id") for message in wires[2]["contextMessages"][-3:]] == ["same-id"] * 3
    assert observed[0].agent_response.to_dict() == original_responses["B"]
    assert _wire(observed[0].full_conversation) == expected_c
    assert {name: response.to_dict() for name, response in responses.items()} == original_responses
    if mode == "custom":
        assert "secret" not in json.dumps(wires[2])
