# Copyright (c) Microsoft. All rights reserved.

"""Core 1.16 agent yields, approval barriers and locally declared structured output."""

from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import Mock, patch
from uuid import UUID

import pytest
from agent_framework import (
    AgentExecutor,
    AgentExecutorResponse,
    AgentResponse,
    AgentSession,
    Content,
    Executor,
    Message,
    Workflow,
    WorkflowBuilder,
    WorkflowContext,
    WorkflowExecutor,
    handler,
)
from durabletask.task import CompletableTask, OrchestrationContext
from pydantic import BaseModel, Field

from agent_framework_durabletask import AgentEntity, AgentEntityStateProviderMixin, serialize_agent_response
from agent_framework_durabletask._workflows.activity import execute_workflow_activity
from agent_framework_durabletask._workflows.dt_context import DurableTaskWorkflowContext
from agent_framework_durabletask._workflows.orchestrator import (
    SOURCE_HITL_RESPONSE,
    ExecutorResult,
    TaskMetadata,
    TaskType,
    _collect_hitl_requests,
    _prepare_agent_task,
    _process_agent_response,
    _WorkflowDeliveryLedger,
    run_workflow_orchestrator,
)
from agent_framework_durabletask._workflows.protocol import unwrap_workflow_input
from agent_framework_durabletask._workflows.serialization import deserialize_value


class Answer(BaseModel):
    answer: int = Field(validation_alias="inputAnswer", serialization_alias="outputAnswer")


class _Agent:
    description = None

    def __init__(self, name: str, responses: list[AgentResponse], response_format: Any = None) -> None:
        self.id = self.name = name
        self.responses = iter(responses)
        self.default_options = {"response_format": response_format}
        self.inputs: list[list[Message]] = []

    def create_session(self, **kwargs: Any) -> AgentSession:
        return AgentSession(**kwargs)

    async def run(
        self, messages: list[Message], *, session: AgentSession | None = None, **kwargs: Any
    ) -> AgentResponse:
        self.inputs.append(deepcopy(messages))
        return next(self.responses)


def _agent(name: str, responses: list[AgentResponse] | None = None, response_format: Any = None) -> AgentExecutor:
    agent: Any = _Agent(name, responses or [], response_format)
    return AgentExecutor(agent, id=name)


def _response(text: str = "done", **kwargs: Any) -> AgentResponse:
    return AgentResponse(messages=[Message("assistant", [text])], **kwargs)


def _approval(request_id: str) -> Content:
    return Content.from_function_approval_request(
        request_id, Content.from_function_call(f"call-{request_id}", "lookup", arguments={"flag": False})
    )


def _pending(requests: list[Content]) -> AgentResponse:
    # Non-request content is intentionally suppressed, just as in core non-streaming mode.
    return AgentResponse(messages=[Message("assistant", [Content.from_text("not final"), *requests])])


def _wire(response: AgentResponse) -> dict[str, Any]:
    return json.loads(json.dumps(serialize_agent_response(response), allow_nan=False))


class _Adapter:
    """Use actual host adapters and task wrappers, mocking only native scheduling."""

    def __init__(self, kind: str, *, replay: bool = False) -> None:
        self.kind = kind
        self.pending: list[tuple[str, Any, tuple[Any, ...], dict[str, Any]]] = []
        self.calls: list[tuple[str, Any, tuple[Any, ...], dict[str, Any]]] = []
        self.statuses: list[dict[str, Any]] = []
        self.ordinal = 0
        if kind == "dt":
            self.native = Mock(spec=OrchestrationContext)
            self.context: Any = DurableTaskWorkflowContext(self.native)
        else:
            df = pytest.importorskip("azure.durable_functions")
            module = pytest.importorskip("agent_framework_azurefunctions._workflow_af_context")
            self.native = Mock(spec=df.DurableOrchestrationContext)
            self.native.task_all.side_effect = lambda tasks: tasks
            self.context = module.AzureFunctionsWorkflowContext(self.native)
        self.native.instance_id = "contract-run"
        self.native.is_replaying = replay
        self.native.current_utc_datetime = datetime(2026, 9, 9, tzinfo=timezone.utc)
        self.native.new_uuid.side_effect = [str(UUID(int=i)) for i in range(1, 100)]
        self.native.call_entity.side_effect = lambda *a, **kw: self.schedule("entity", *a, **kw)
        self.native.call_activity.side_effect = lambda *a, **kw: self.schedule("activity", *a, **kw)
        self.native.call_sub_orchestrator.side_effect = lambda *a, **kw: self.schedule("child", *a, **kw)
        self.native.wait_for_external_event.side_effect = lambda *a, **kw: self.schedule("event", *a, **kw)
        self.native.set_custom_status.side_effect = lambda status: self.statuses.append(deepcopy(status))

    def schedule(self, kind: str, *args: Any, **kwargs: Any) -> Any:
        if self.kind == "dt":
            task: Any = CompletableTask()
        else:
            from azure.durable_functions.models.actions.NoOpAction import NoOpAction
            from azure.durable_functions.models.Task import AtomicTask

            task = AtomicTask(self.ordinal, NoOpAction())
        self.ordinal += 1
        call = (kind, task, args, kwargs)
        self.pending.append(call)
        self.calls.append(call)
        return task

    def complete(self, yielded: Any, *values: Any) -> Any:
        assert len(values) == len(self.pending)
        pending, self.pending = self.pending, []
        for (_, task, _, _), value in zip(pending, values, strict=True):
            value = json.loads(json.dumps(value, allow_nan=False))
            if self.kind == "dt":
                task.complete(value)
            else:
                task.set_value(is_error=False, value=value)
        if isinstance(yielded, list):
            return [self.context.get_task_result(task) for task in yielded]
        return self.context.get_task_result(yielded)

    def payload(self) -> dict[str, Any]:
        assert len(self.pending) == 1
        kind, _, args, _ = self.pending[0]
        assert kind == "entity"
        assert args[1] == "run"
        return json.loads(json.dumps(args[2], allow_nan=False))

    def activity_input(self) -> str:
        kind, _, args, kwargs = self.pending[0]
        assert kind == "activity"
        return args[1] if len(args) > 1 else kwargs["input"]

    def finish(self, generator: Any, yielded: Any, *values: Any) -> Any:
        with pytest.raises(StopIteration) as completed:
            generator.send(self.complete(yielded, *values))
        return deserialize_value(completed.value.value)


@pytest.mark.parametrize("adapter", ["dt", "af"])
def test_one_agent_output_matches_core_type_count_and_designation(adapter: str) -> None:
    response = _response("answer", response_id="actual-response")
    core = WorkflowBuilder(name="core", start_executor=_agent("A", [response])).build()

    async def core_run() -> list[Any]:
        return (await core.run("question")).get_outputs()

    expected: list[Any] = asyncio.run(core_run())
    assert len(expected) == 1
    assert isinstance(expected[0], AgentResponse)

    a = _agent("A")
    workflow = WorkflowBuilder(name="review", start_executor=a).build()
    host = _Adapter(adapter)
    generator = run_workflow_orchestrator(host.context, workflow, "question")
    yielded = next(generator)
    output = host.finish(generator, yielded, _wire(response))
    assert len(output) == len(expected)
    assert type(output[0]) is AgentResponse
    assert output[0].to_dict() == expected[0].to_dict()


@pytest.mark.parametrize("adapter", ["dt", "af"])
@pytest.mark.parametrize("designation", ["output", "intermediate", "hidden"])
def test_agent_yields_follow_real_workflow_designation_and_streaming_gate(adapter: str, designation: str) -> None:
    a, b = _agent("A"), _agent("B")
    workflow = (
        WorkflowBuilder(
            name="review",
            start_executor=a,
            output_from=[a, b] if designation == "output" else [b],
            intermediate_output_from=[a] if designation == "intermediate" else [],
        )
        .add_edge(a, b)
        .build()
    )
    host = _Adapter(adapter)
    generator = run_workflow_orchestrator(host.context, workflow, "question")
    yielded = next(generator)
    yielded = generator.send(host.complete(yielded, _wire(_response("A"))))
    assert [Message.from_dict(m).text for m in host.payload()["contextMessages"]] == ["question", "A"]
    output = host.finish(generator, yielded, _wire(_response("B")))
    assert [value.text for value in output] == (["A", "B"] if designation == "output" else ["B"])
    if adapter == "af":
        assert all("events" not in status for status in host.statuses)
    else:
        events = host.statuses[-1]["events"]
        yields = [event for event in events if event["type"] in ("output", "intermediate")]
        assert [(event["executor_id"], event["type"]) for event in yields] == (
            [("A", designation), ("B", "output")] if designation != "hidden" else [("B", "output")]
        )


class _InspectEnvelope(Executor):
    def __init__(self) -> None:
        super().__init__(id="inspect")

    @handler
    async def inspect_response(
        self, response: AgentExecutorResponse, ctx: WorkflowContext[None, AgentResponse]
    ) -> None:
        assert isinstance(response.agent_response.value, Answer)
        assert response.agent_response.value.answer == 42
        ctx.set_state("verified", True)
        ctx.state.delete("remove")
        await ctx.yield_output(response.agent_response)


@pytest.mark.parametrize("adapter", ["dt", "af"])
@pytest.mark.parametrize("retained", [False, True])
def test_declared_model_survives_entity_wire_condition_activity_and_output(adapter: str, retained: bool) -> None:
    response = (
        _response("not structured text", value=Answer(inputAnswer=42)) if retained else _response('{"inputAnswer":42}')
    )
    a, inspect_response = _agent("A", response_format=Answer), _InspectEnvelope()
    observed: list[AgentExecutorResponse] = []

    def condition(value: AgentExecutorResponse) -> bool:
        observed.append(value)
        assert isinstance(value.agent_response.value, Answer)
        return value.agent_response.value.answer == 42

    workflow = (
        WorkflowBuilder(name="review", start_executor=a, output_from=[a, inspect_response])
        .add_edge(a, inspect_response, condition=condition)
        .build()
    )
    state = {"remove": True, "keep": False}
    host = _Adapter(adapter)
    generator = run_workflow_orchestrator(host.context, workflow, "question", state)
    yielded = next(generator)
    assert "response_format" not in host.payload()
    payload = _wire(response)
    # Persisted Python class names are not a source of declared response types.
    payload["response_format"] = "untrusted.module:Model"
    with patch("importlib.import_module", side_effect=AssertionError("must not resolve wire types")):
        yielded = generator.send(host.complete(yielded, payload))
    assert isinstance(observed[0].agent_response.value, Answer)
    encoded_input = host.activity_input()
    restored = deserialize_value(json.loads(encoded_input)["message"])
    assert isinstance(restored, AgentExecutorResponse)
    assert isinstance(restored.agent_response.value, Answer)
    result = execute_workflow_activity(inspect_response, encoded_input, workflow)
    output = host.finish(generator, yielded, result)
    assert len(output) == 2
    assert all(isinstance(value, AgentResponse) for value in output)
    # Generated agent output is portable JSON; an explicit activity yield retains
    # the existing arbitrary-object checkpoint contract.
    assert output[0].value == {"answer": 42}
    assert isinstance(output[1].value, Answer) and output[1].value.answer == 42
    assert state == {"keep": False, "verified": True}


class _InspectChild(Executor):
    def __init__(self) -> None:
        super().__init__(id="inspect-child")

    @handler
    async def inspect_response(self, response: AgentResponse, ctx: WorkflowContext[None, str]) -> None:
        assert type(response) is AgentResponse
        await ctx.yield_output(response.text)


@pytest.mark.parametrize("adapter", ["dt", "af"])
@pytest.mark.parametrize("direct", [False, True])
def test_child_ending_in_agent_forwards_agent_response_not_executor_envelope(adapter: str, direct: bool) -> None:
    inner = WorkflowBuilder(name="inner", start_executor=_agent("A")).build()
    child = WorkflowExecutor(inner, id="child", allow_direct_output=direct)
    inspector = _InspectChild()
    builder = WorkflowBuilder(name="outer", start_executor=child, output_from=[child] if direct else [inspector])
    if not direct:
        builder.add_edge(child, inspector)
    outer = builder.build()
    host = _Adapter(adapter)
    generator = run_workflow_orchestrator(host.context, outer, "question")
    yielded = next(generator)
    _, _, _, kwargs = host.pending[0]
    child_input = unwrap_workflow_input(kwargs["input"] if adapter == "dt" else kwargs["input_"])
    child_host = _Adapter(adapter)
    child_host.native.instance_id = kwargs["instance_id"]
    child_generator = run_workflow_orchestrator(child_host.context, inner, child_input)
    child_yielded = next(child_generator)
    with pytest.raises(StopIteration) as completed:
        child_generator.send(child_host.complete(child_yielded, _wire(_response("child answer"))))
    child_result = completed.value.value
    assert type(deserialize_value(child_result["outputs"])[0]) is AgentResponse
    if direct:
        output = host.finish(generator, yielded, child_result)
        assert len(output) == 1 and type(output[0]) is AgentResponse
    else:
        yielded = generator.send(host.complete(yielded, child_result))
        encoded_input = host.activity_input()
        assert type(deserialize_value(json.loads(encoded_input)["message"])) is AgentResponse
        result = execute_workflow_activity(inspector, encoded_input, outer)
        assert host.finish(generator, yielded, result) == ["child answer"]


@pytest.mark.parametrize("adapter", ["dt", "af"])
@pytest.mark.parametrize("count", [1, 2])
@pytest.mark.parametrize("reply_kind", ["approval", "results", "mixed"])
@pytest.mark.parametrize("batch", [False, True])
def test_approval_barrier_resumes_once_after_all_answers_with_core_content_and_role(
    adapter: str, count: int, reply_kind: str, batch: bool
) -> None:
    request_ids = [f"request-{i}" for i in range(count)]
    requests = [_approval(request_id) for request_id in request_ids]
    replies = [
        Content.from_function_result(f"call-request-{i}", result=False if i == 0 else 0)
        if reply_kind == "results" or (reply_kind == "mixed" and i == 0)
        else request.to_function_approval_response(False)
        for i, request in enumerate(requests)
    ]
    expected_role = "tool" if all(reply.type == "function_result" for reply in replies) else "user"
    core_a, core_b = _agent("A", [_pending(requests), _response("final")]), _agent("B", [_response("B")])
    core_agent_a = cast(_Agent, core_a.agent)
    core_agent_b = cast(_Agent, core_b.agent)
    core = WorkflowBuilder(name="core", start_executor=core_a, output_from=[core_b]).add_edge(core_a, core_b).build()

    async def core_run() -> None:
        pending = await core.run("question")
        assert pending.get_outputs() == []
        assert [event.request_id for event in pending.get_request_info_events()] == [r.id for r in requests]
        if batch:
            await core.run(
                responses={request_id: reply for request_id, reply in zip(request_ids, replies, strict=True)}
            )
            assert len(core_agent_b.inputs) == 1
        else:
            for index, (request_id, reply) in enumerate(zip(request_ids, replies, strict=True)):
                await core.run(responses={request_id: reply})
                assert len(core_agent_b.inputs) == int(index == count - 1)

    asyncio.run(core_run())
    expected_input = core_agent_a.inputs[-1]
    assert len(expected_input) == 1 and expected_input[0].role == expected_role

    a, b = _agent("A", response_format=Answer), _agent("B")
    workflow = WorkflowBuilder(name="review", start_executor=a, output_from=[b]).add_edge(a, b).build()
    before = deepcopy(a._session.to_dict())
    host = _Adapter(adapter)
    generator = run_workflow_orchestrator(host.context, workflow, "question")
    yielded = next(generator)
    first_payload = host.payload()
    first_entity = host.native.call_entity.call_args.args[0]
    yielded = generator.send(host.complete(yielded, _wire(_pending(requests))))
    for index, (request, reply) in enumerate(zip(requests, replies, strict=True)):
        assert host.pending[0][0] == "event"
        assert host.pending[0][2] == (request.id,)
        assert host.native.call_entity.call_count == 1
        waiting = host.statuses[-1]
        assert waiting["state"] == "waiting_for_human_input"
        assert list(waiting["pending_requests"]) == [r.id for r in requests[index:]]
        event = waiting["pending_requests"][request.id]
        assert deserialize_value(event["data"]) == request
        assert event["response_type"] == f"{Content.__module__}:{Content.__name__}"
        yielded = generator.send(host.complete(yielded, reply.to_dict()))
    assert host.native.call_entity.call_count == 2
    assert host.native.call_activity.call_count == 0
    resumed = host.payload()
    assert resumed["correlationId"] != first_payload["correlationId"]
    assert host.native.call_entity.call_args.args[0] == first_entity
    assert resumed["contextMessages"] == [message.to_dict() for message in expected_input]
    assert len(resumed["contextMessageIds"]) == 1
    assert resumed["message"] == ""
    final = _response("final", value=Answer(inputAnswer=42))
    yielded = generator.send(host.complete(yielded, _wire(final)))
    downstream = host.payload()
    assert [Message.from_dict(m).to_dict() for m in downstream["contextMessages"]] == [
        *[message.to_dict() for message in expected_input],
        *[message.to_dict() for message in final.messages],
    ]
    assert downstream["contextMessageIds"][0] == resumed["contextMessageIds"][0]
    output = host.finish(generator, yielded, _wire(_response("B")))
    assert len(output) == 1 and output[0].text == "B"
    assert a._pending_agent_requests == {} and a._pending_responses_to_agent == [] and a._cache == []
    assert a._session.to_dict() == before
    if adapter == "dt":
        events = host.statuses[-1]["events"]
        assert [event["request_id"] for event in events if event["type"] == "request_info"] == [r.id for r in requests]
        assert [event["executor_id"] for event in events if event["type"] == "output"] == ["B"]


@pytest.mark.parametrize("adapter", ["dt", "af"])
@pytest.mark.parametrize("bad", [None, False, {"type": ""}, {"__pickled__": "bad", "__type__": "bad:Type"}])
def test_invalid_agent_reply_does_not_consume_request_and_can_be_corrected(adapter: str, bad: Any) -> None:
    request = _approval("request")
    a = _agent("A", response_format=Answer)
    workflow = WorkflowBuilder(name="review", start_executor=a).build()
    host = _Adapter(adapter)
    generator = run_workflow_orchestrator(host.context, workflow, "question")
    yielded = generator.send(host.complete(next(generator), _wire(_pending([request]))))
    yielded = generator.send(host.complete(yielded, bad))
    assert host.pending[0][2] == ("request",)
    assert host.native.call_entity.call_count == 1
    assert "request" in host.statuses[-1]["pending_requests"]
    yielded = generator.send(host.complete(yielded, request.to_function_approval_response(False).to_dict()))
    assert host.native.call_entity.call_count == 2
    assert host.finish(generator, yielded, _wire(_response(value=Answer(inputAnswer=42))))[0].value == {"answer": 42}


def test_duplicate_unknown_and_out_of_order_replies_keep_accumulated_content_and_prepare_is_atomic() -> None:
    a, host, ledger = _agent("A"), _Adapter("dt"), _WorkflowDeliveryLedger(instance_id="contract-run")
    metadata = TaskMetadata("A", "question", "start", TaskType.AGENT)
    _prepare_agent_task(host.context, a, "A", "question", "review", ledger, metadata)
    requests = [_approval("first"), _approval("second")]
    result = _process_agent_response(_wire(_pending(requests)), "A", "question", ledger, metadata)
    assert result.output_message is None

    def respond(request_id: str, response: Content) -> Any:
        message = {"request_id": request_id, "response": response.to_dict(), "response_type": "unsafe.module:Type"}
        meta = TaskMetadata("A", message, f"{SOURCE_HITL_RESPONSE}_{request_id}", TaskType.AGENT)
        return _prepare_agent_task(host.context, a, "A", message, "review", ledger, meta)

    second = requests[1].to_function_approval_response(False)
    assert respond("second", second) is None
    snapshot = ledger.fork()
    assert respond("second", second) is None
    assert respond("unknown", second) is None
    assert ledger == snapshot
    first = Content.from_function_result("call-first", result=0)
    with (
        patch.object(host.context, "prepare_agent_task", side_effect=OSError("prepare failed")),
        pytest.raises(OSError, match="prepare failed"),
    ):
        respond("first", first)
    assert ledger == snapshot
    assert respond("first", first) is not None
    wire = host.native.call_entity.call_args.args[2]
    assert wire["contextMessages"] == [Message("user", [second, first]).to_dict()]
    assert ledger.pending_agent_requests == ledger.pending_agent_responses == {}
    assert host.native.call_entity.call_count == 2


class _SessionAgent(_Agent):
    """Exercise the entity's session branch without a network model dependency."""

    def __init__(self, responses: list[AgentResponse]) -> None:
        super().__init__("A", responses, Answer)
        self.context_providers: list[Any] = []
        self.default_options["store"] = True
        self.sessions: list[tuple[str, Any, dict[str, Any]]] = []

    async def run(
        self, messages: list[Message], *, session: AgentSession | None = None, **kwargs: Any
    ) -> AgentResponse:
        assert session is not None
        self.sessions.append((session.session_id, session.service_session_id, deepcopy(session.state)))
        session.service_session_id = "service-conversation"
        session.state["application"] = {"pending": False, "turn": len(self.sessions)}
        return await super().run(messages, session=session, **kwargs)


class _StateProvider(AgentEntityStateProviderMixin):
    def __init__(self, raw: dict[str, Any] | None = None) -> None:
        self.raw = raw or {}

    def _get_state_dict(self) -> dict[str, Any]:
        return json.loads(json.dumps(self.raw))

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        self.raw = json.loads(json.dumps(state, allow_nan=False))

    def _get_session_id_from_entity(self) -> str:
        return "contract-run"

    def _get_entity_name_from_entity(self) -> str:
        return "dafx-review-a"


@pytest.mark.parametrize("adapter", ["dt", "af"])
async def test_actual_entity_cold_resume_preserves_service_session_and_fresh_correlation(adapter: str) -> None:
    request = _approval("real-request")
    agent: Any = _SessionAgent([_pending([request]), _response(value=Answer(inputAnswer=42))])
    a = AgentExecutor(agent, id="A")
    workflow = WorkflowBuilder(name="review", start_executor=a).build()
    provider = _StateProvider()
    host = _Adapter(adapter)
    generator = run_workflow_orchestrator(host.context, workflow, "question")
    yielded = next(generator)
    first_payload = host.payload()
    response = await AgentEntity(agent, state_provider=provider).run(first_payload)
    yielded = generator.send(host.complete(yielded, _wire(response)))
    yielded = generator.send(host.complete(yielded, request.to_function_approval_response(False).to_dict()))
    resumed_payload = host.payload()
    assert resumed_payload["correlationId"] != first_payload["correlationId"]
    cold_provider = _StateProvider(provider.raw)
    response = await AgentEntity(agent, state_provider=cold_provider).run(resumed_payload)
    output = host.finish(generator, yielded, _wire(response))
    assert len(agent.sessions) == 2
    assert agent.sessions[0][0] == agent.sessions[1][0]
    assert agent.sessions[1][1] == "service-conversation"
    assert agent.sessions[1][2]["application"] == {"pending": False, "turn": 1}
    assert len(agent.inputs[1]) == 1
    assert agent.inputs[1][0].contents == [request.to_function_approval_response(False)]
    assert output[0].value == {"answer": 42}


@pytest.mark.parametrize("status", ["error", "already_completed"])
def test_terminal_failure_precedes_structured_parse_and_does_not_emit_output(status: str) -> None:
    a = _agent("A", response_format=Answer)
    host = _Adapter("dt")
    workflow = WorkflowBuilder(name="review", start_executor=a).build()
    generator = run_workflow_orchestrator(host.context, workflow, "question")
    yielded = next(generator)
    payload = _wire(_response("invalid json", additional_properties={"durable_status": status}))
    with (
        patch(
            "agent_framework_durabletask._workflows.orchestrator.ensure_response_format",
            side_effect=AssertionError("must not parse terminal response"),
        ),
        pytest.raises(RuntimeError, match="expired durable response|terminal runtime error"),
    ):
        generator.send(host.complete(yielded, payload))
    assert host.native.call_entity.call_count == 1


def test_unconfigured_mock_workflow_does_not_accidentally_designate_every_agent() -> None:
    a = _agent("A")
    workflow = Mock(spec=Workflow)
    workflow.name = "review"
    workflow.executors = {"A": a}
    workflow.start_executor_id = "A"
    workflow.edge_groups = []
    workflow.max_iterations = 5
    host = _Adapter("dt")
    generator = run_workflow_orchestrator(host.context, workflow, "question")
    assert host.finish(generator, next(generator), _wire(_response())) == []


@pytest.mark.parametrize("adapter", ["dt", "af"])
def test_two_approval_rounds_rebuild_on_replay_without_duplicate_requests_or_correlations(adapter: str) -> None:
    a = _agent("A", response_format=Answer)
    workflow = WorkflowBuilder(name="review", start_executor=a).build()

    def run(replay: bool) -> tuple[list[dict[str, Any]], list[AgentResponse]]:
        host = _Adapter(adapter, replay=replay)
        generator = run_workflow_orchestrator(host.context, workflow, "question")
        yielded = next(generator)
        wires = [host.payload()]
        for request_id in ["first-round", "second-round"]:
            request = _approval(request_id)
            yielded = generator.send(host.complete(yielded, _wire(_pending([request]))))
            assert host.pending[0][2] == (request_id,)
            yielded = generator.send(host.complete(yielded, request.to_function_approval_response(False).to_dict()))
            wires.append(host.payload())
        outputs = host.finish(generator, yielded, _wire(_response(value=Answer(inputAnswer=42))))
        assert len({wire["correlationId"] for wire in wires}) == 3
        assert len({wire["contextMessageIds"][0] for wire in wires[1:]}) == 2
        if replay:
            assert host.statuses == []
        elif adapter == "dt":
            events = host.statuses[-1]["events"]
            assert [event["request_id"] for event in events if event["type"] == "request_info"] == [
                "first-round",
                "second-round",
            ]
            assert len([event for event in events if event["type"] == "output"]) == 1
        return wires, outputs

    live_wires, live = run(False)
    replay_wires, replay = run(True)
    # RunRequest's existing created_at default is wall-clock metadata, not an
    # orchestration-generated correlation or occurrence identity.
    assert [{key: value for key, value in wire.items() if key != "created_at"} for wire in live_wires] == [
        {key: value for key, value in wire.items() if key != "created_at"} for wire in replay_wires
    ]
    assert len(live) == len(replay) == 1
    assert live[0].value == replay[0].value == {"answer": 42}
    assert a._pending_agent_requests == {} and a._pending_responses_to_agent == []


@pytest.mark.parametrize("adapter", ["dt", "af"])
@pytest.mark.parametrize("response_kind", ["function_approval_response", "function_result"])
def test_wrong_response_identity_is_rejected_without_losing_real_request(adapter: str, response_kind: str) -> None:
    request = _approval("actual-request")
    wrong = (
        _approval("unknown-request").to_function_approval_response(True)
        if response_kind == "function_approval_response"
        else Content.from_function_result("unknown-call", result=False)
    )
    a = _agent("A")
    workflow = WorkflowBuilder(name="review", start_executor=a).build()
    host = _Adapter(adapter)
    generator = run_workflow_orchestrator(host.context, workflow, "question")
    yielded = generator.send(host.complete(next(generator), _wire(_pending([request]))))
    yielded = generator.send(host.complete(yielded, wrong.to_dict()))
    assert host.pending[0][2] == ("actual-request",)
    assert host.native.call_entity.call_count == 1
    response = Content.from_function_result(
        "call-actual-request", result={"type": "untrusted.module:Value", "flag": False}
    )
    with patch("importlib.import_module", side_effect=AssertionError("must not import external content types")):
        yielded = generator.send(host.complete(yielded, response.to_dict()))
    assert host.payload()["contextMessages"] == [Message("tool", [response]).to_dict()]
    assert len(host.finish(generator, yielded, _wire(_response()))) == 1


class _TwoInputs(Executor):
    def __init__(self) -> None:
        super().__init__(id="source")

    @handler
    async def send_inputs(self, message: str, ctx: WorkflowContext[str]) -> None:
        await ctx.send_message("first")
        await ctx.send_message("second")


@pytest.mark.parametrize("adapter", ["dt", "af"])
def test_sequential_agent_completion_uses_same_output_and_structured_contract(adapter: str) -> None:
    source, a = _TwoInputs(), _agent("A", response_format=Answer)
    workflow = WorkflowBuilder(name="review", start_executor=source, output_from=[a]).add_edge(source, a).build()
    host = _Adapter(adapter)
    generator = run_workflow_orchestrator(host.context, workflow, "question")
    yielded = next(generator)
    result = execute_workflow_activity(source, host.activity_input(), workflow)
    yielded = generator.send(host.complete(yielded, result))
    assert host.payload()["message"] == "first"
    yielded = generator.send(host.complete(yielded, _wire(_response(value=Answer(inputAnswer=1)))))
    assert host.payload()["message"] == "second"
    outputs = host.finish(generator, yielded, _wire(_response(value=Answer(inputAnswer=2))))
    assert [output.value for output in outputs] == [{"answer": 1}, {"answer": 2}]


@pytest.mark.parametrize("declared", [None, "untrusted.module:Model", {"type": "json_object"}, int])
def test_only_locally_declared_pydantic_classes_trigger_reconstruction(declared: Any) -> None:
    a = _agent("A", response_format=declared)
    workflow = WorkflowBuilder(name="review", start_executor=a).build()
    host = _Adapter("dt")
    generator = run_workflow_orchestrator(host.context, workflow, "question")
    yielded = next(generator)
    with patch(
        "agent_framework_durabletask._workflows.orchestrator.ensure_response_format",
        side_effect=AssertionError("must only parse locally declared Pydantic classes"),
    ):
        outputs = host.finish(generator, yielded, _wire(_response(value={"answer": 42})))
    assert outputs[0].value == {"answer": 42}


@pytest.mark.parametrize("agent_first", [False, True])
def test_agent_activity_request_id_collision_cannot_silently_overwrite_either_request(agent_first: bool) -> None:
    def result(task_type: TaskType) -> ExecutorResult:
        return ExecutorResult(
            executor_id=task_type.value,
            output_message=None,
            activity_result={"pending_request_info_events": [{"request_id": "collision", "data": task_type.value}]},
            task_type=task_type,
        )

    first, second = (TaskType.AGENT, TaskType.ACTIVITY) if agent_first else (TaskType.ACTIVITY, TaskType.AGENT)
    pending: dict[str, Any] = {}
    _collect_hitl_requests(result(first), pending)
    with pytest.raises(ValueError, match="collides"):
        _collect_hitl_requests(result(second), pending)
    assert pending["collision"].source_executor_id == first.value


@pytest.mark.parametrize("request_id", [None, "", "duplicate"])
def test_malformed_agent_request_ids_fail_before_registering_partial_batch(request_id: str | None) -> None:
    host, ledger, a = _Adapter("dt"), _WorkflowDeliveryLedger(), _agent("A")
    metadata = TaskMetadata("A", "question", "start", TaskType.AGENT)
    _prepare_agent_task(host.context, a, "A", "question", "review", ledger, metadata)
    malformed = Content("function_call", id=request_id, user_input_request=True, call_id="call")
    requests = [_approval("duplicate"), malformed]
    with pytest.raises(ValueError, match="without an id|duplicate user input request"):
        _process_agent_response(_wire(_pending(requests)), "A", "question", ledger, metadata)
    assert ledger.pending_agent_requests == ledger.pending_agent_responses == {}


@pytest.mark.parametrize("reply", ["clarification", {"type": "function_result", "call_id": "external", "result": None}])
def test_general_core_content_requests_use_text_coercion_and_preserve_null_results(reply: Any) -> None:
    request = Content("function_call", id="input", call_id="external", user_input_request=True)
    host, a = _Adapter("dt"), _agent("A")
    workflow = WorkflowBuilder(name="review", start_executor=a).build()
    generator = run_workflow_orchestrator(host.context, workflow, "question")
    yielded = generator.send(host.complete(next(generator), _wire(_pending([request]))))
    yielded = generator.send(host.complete(yielded, reply))
    contents = host.payload()["contextMessages"][0]
    if isinstance(reply, str):
        assert contents == Message("user", [Content.from_text(reply)]).to_dict()
    else:
        assert contents["role"] == "tool"
        content = Content.from_dict(contents["contents"][0])
        assert content.type == "function_result" and content.result is None and content.call_id == "external"
    assert len(host.finish(generator, yielded, _wire(_response()))) == 1
