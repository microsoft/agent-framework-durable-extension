# Copyright (c) Microsoft. All rights reserved.

"""Presence-aware context crosses registered activities, agents and cold SDK episodes.

Histories are constructed with real registered activity/entity results, not a live
service capture. Full projection is paired with a copied-full custom projection.
Literal public message JSON, not the production serializer, defines the oracle.
"""

import hashlib
import json
import logging
from copy import deepcopy
from typing import Any

import pytest
from _execution_test_support import RecordingChatClient
from agent_framework import (
    Agent,
    AgentExecutor,
    AgentExecutorResponse,
    AgentResponse,
    Content,
    Executor,
    InMemoryHistoryProvider,
    Message,
    Workflow,
    WorkflowBuilder,
    WorkflowContext,
    handler,
)
from durabletask.entities import EntityContext, EntityInstanceId
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.internal.entity_state_shim import StateShim
from durabletask.serialization import JsonDataConverter
from durabletask.worker import _ActivityExecutor
from test_workflow_sdk_history_replay import _replay, _worker
from test_workflow_selection_occurrences import _assert_same_action_contract, _request_contract

from agent_framework_durabletask import DurableAgentState, load_agent_response, wrap_workflow_input
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._response_utils import preserve_input_envelope, serialize_input_message
from agent_framework_durabletask._workflows.serialization import deserialize_workflow_output

_INSTANCE = "context-presence-run"
_WORKFLOW = "context-presence"
_LOGGER = logging.getLogger(__name__)
_SHAPES = ["result", "error", "approval", "items", "inputs", "code-outputs", "shell-outputs", "image-outputs"]


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


class _RawRepresentation:
    def to_dict(self, **kwargs: Any) -> dict[str, Any]:
        raise AssertionError("Raw SDK representations must not be serialized into public context")

    @classmethod
    def from_dict(cls, value: Any, **kwargs: Any) -> Any:
        raise AssertionError("Raw SDK representations must not be reconstructed from public context")


def _payload(shape: str, presence: str, *, extras: bool = False) -> dict[str, Any]:
    leaf: dict[str, Any] = {
        "type": "function_result",
        "call_id": "call",
        "additional_properties": {"metadata": {"flag": False, "count": 0}},
        "annotations": [{"type": "citation", "url": "https://example.invalid/evidence"}],
    }
    field = "result"
    if shape == "error":
        leaf = {**leaf, "type": "error", "message": "tool diagnostic", "error_code": "tool_error"}
        leaf.pop("call_id")
        field = "error_details"
    elif shape == "approval":
        leaf.update(type="function_call", name="lookup")
        field = "arguments"
    if presence != "absent":
        leaf[field] = (
            None
            if presence == "null"
            else {"flag": False, "count": 0, "opaque": {"type": "function_result", "result": None}}
        )
    if extras:
        leaf["future_content"] = {"type": "unregistered.payload", "value": None}

    content = leaf
    if shape == "approval":
        content = {
            "type": "function_approval_request",
            "id": "approval",
            "user_input_request": True,
            "function_call": leaf,
            "additional_properties": {"approval_metadata": {"enabled": False}},
        }
    elif shape in ("items", "inputs", "code-outputs", "shell-outputs", "image-outputs"):
        kind, edge = {
            "items": ("function_result", "items"),
            "inputs": ("code_interpreter_tool_call", "inputs"),
            "code-outputs": ("code_interpreter_tool_result", "outputs"),
            "shell-outputs": ("shell_tool_result", "outputs"),
            "image-outputs": ("image_generation_tool_result", "outputs"),
        }[shape]
        content = {"type": kind, "call_id": "outer", edge: [leaf], "additional_properties": {"outer": True}}
    if extras:
        content["future_outer"] = [None, False, 0]
    result: dict[str, Any] = {
        "type": "message",
        "role": "tool",
        "message_id": "source",
        "author_name": "producer",
        "additional_properties": {"source_metadata": {"flag": False, "count": 0}},
        "contents": [content],
    }
    if extras:
        result["future_message"] = {"type": "unregistered.message", "value": None}
    return result


def _source(payload: dict[str, Any]) -> Message:
    message = load_agent_response({"messages": [deepcopy(payload)]}).messages[0]
    preserve_input_envelope(message, payload)
    message.raw_representation = _RawRepresentation()

    def attach_raw(content: Content) -> None:
        content.raw_representation = _RawRepresentation()
        if isinstance(content.function_call, Content):
            attach_raw(content.function_call)
        for name in ("items", "inputs", "outputs"):
            values = getattr(content, name, None)
            if isinstance(values, list):
                for value in values:
                    if isinstance(value, Content):
                        attach_raw(value)

    for content in message.contents:
        attach_raw(content)
    return message


def _graph(payload: dict[str, Any], copied: bool) -> tuple[Workflow, dict[str, RecordingChatClient], list[str]]:
    trace: list[str] = []

    def observe(label: str, message: Message) -> None:
        assert _json(serialize_input_message(message)) == _json(payload)
        assert message_identity(message) == hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()
        trace.append(label)

    class Seed(Executor):
        @handler(input=str, output=AgentExecutorResponse)
        async def handle(self, message: str, ctx: WorkflowContext[AgentExecutorResponse]) -> None:
            source = _source(payload)
            observe("seed", source)
            await ctx.send_message(AgentExecutorResponse(self.id, AgentResponse(messages=[source]), [source]))

    class Relay(Executor):
        @handler(input=AgentExecutorResponse, output=AgentExecutorResponse, workflow_output=list)
        async def handle(self, prior: AgentExecutorResponse, ctx: WorkflowContext[AgentExecutorResponse, list]) -> None:
            observe("relay", prior.full_conversation[0])
            # One real cycle invokes agent a again from cold entity
            # state. Its already-delivered source must not be sent or ingested again.
            if sum(message.message_id == "reply-a" for message in prior.full_conversation) == 2:
                await ctx.yield_output(prior.full_conversation)
            else:
                await ctx.send_message(prior)

    class Return(Executor):
        @handler(input=AgentExecutorResponse, output=AgentExecutorResponse)
        async def handle(self, prior: AgentExecutorResponse, ctx: WorkflowContext[AgentExecutorResponse]) -> None:
            observe("return", prior.full_conversation[0])
            await ctx.send_message(prior)

    clients = {name: RecordingChatClient(response_message_id=f"reply-{name}") for name in ("a", "b")}
    nodes: dict[str, AgentExecutor] = {}
    for name, client in clients.items():
        agent = Agent(client=client, name=name)
        nodes[name] = (
            AgentExecutor(agent, id=name, context_mode="custom", context_filter=lambda messages: deepcopy(messages))
            if copied
            else AgentExecutor(agent, id=name)
        )
    seed, relay, back = Seed(id="seed"), Relay(id="relay"), Return(id="return")
    workflow = (
        WorkflowBuilder(name=_WORKFLOW, start_executor=seed, output_from=[relay])
        .add_edge(seed, nodes["a"])
        .add_edge(nodes["a"], relay)
        .add_edge(relay, nodes["b"])
        .add_edge(nodes["b"], back)
        .add_edge(back, nodes["a"])
        .build()
    )
    return workflow, clients, trace


def _run_registered(payload: dict[str, Any], copied: bool) -> dict[str, Any]:
    workflow, clients, trace = _graph(payload, copied)
    native = _worker(workflow)
    activities = _ActivityExecutor(native._registry, _LOGGER, native._data_converter)
    converter = JsonDataConverter()
    history: list[Any] = []
    incoming = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(f"dafx-{_WORKFLOW}", _INSTANCE, json.dumps(wrap_workflow_input("go"))),
    ]
    snapshots: list[tuple[list[Any], list[Any], list[Any], Any]] = []
    requests: list[tuple[str, dict[str, Any]]] = []
    states: dict[str, str | None] = {}
    receipt_snapshots: list[dict[str, list[str] | None]] = []
    fingerprint = hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()
    origin: str | None = None
    final_output: Any = None
    for _ in range(12):
        result = _replay(native, _INSTANCE, history, incoming)
        actions = list(result.actions)
        snapshots.append((deepcopy(history), deepcopy(incoming), deepcopy(actions), result.encoded_custom_status))
        history.extend(incoming)
        assert len(actions) == 1, "The serial workflow must schedule one task or its terminal result"
        action = actions[0]
        if action.HasField("completeOrchestration"):
            completed = action.completeOrchestration
            assert completed.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED
            final_output = deserialize_workflow_output(json.loads(completed.result.value))
            break
        if action.HasField("scheduleTask"):
            scheduled = action.scheduleTask
            history.append(helpers.new_task_scheduled_event(action.id, scheduled.name, scheduled.input.value))
            output = activities.execute(_INSTANCE, scheduled.name, action.id, scheduled.input.value)
            assert output is not None
            completion = helpers.new_task_completed_event(action.id, output)
        else:
            assert action.HasField("sendEntityMessage")
            called = action.sendEntityMessage.entityOperationCalled
            entity_id = EntityInstanceId.parse(called.targetInstanceId.value)
            assert entity_id.key == _INSTANCE and called.operation == "run"
            name = entity_id.entity
            assert name in (f"dafx-{_WORKFLOW}-a", f"dafx-{_WORKFLOW}-b")
            request = json.loads(called.input.value)
            requests.append((name, deepcopy(request)))
            context, ids = request["contextMessages"], request["contextMessageIds"]
            assert len(context) == len(ids) == len(set(ids))
            if len(requests) < 3:
                assert _json(context[0]) == _json(payload), "Public context must retain literal field presence"
                assert hashlib.sha256(_json(context[0]).encode("utf-8")).hexdigest() == fingerprint
                if origin is None:
                    origin = ids[0]
                assert ids[0] == origin, "Forwarding through a registered activity must preserve sender occurrence IDs"
            else:
                assert origin not in ids
                assert all(message.get("message_id") != "source" for message in context)

            # Use the SDK's actual registered entity class and a new state shim
            # on every call. Reconstructing AgentEntity alone would not be cold.
            shim = StateShim(states.get(name), converter, is_serialized=True)
            entity = native._registry.get_entity(name)()
            entity._initialize_entity_context(EntityContext(_INSTANCE, "run", shim, entity_id, converter))
            response = entity.run(deepcopy(request))
            assert response.get("additional_properties", {}).get("durable_status") != "error"
            encoded = shim.encode_state()
            assert isinstance(encoded, str)
            assert "raw_representation" not in encoded
            states[name] = encoded
            state = DurableAgentState.from_json(encoded)
            assert origin is not None
            assert state.data.ingested_messages[origin] == [fingerprint], (
                "Receiver must record the sender's fingerprint"
            )
            for occurrence, message in zip(ids, context, strict=True):
                expected_fingerprint = hashlib.sha256(_json(message).encode("utf-8")).hexdigest()
                assert expected_fingerprint in (state.data.ingested_messages[occurrence] or [])
            receipt_snapshots.append(deepcopy(state.data.ingested_messages))
            history.append(pb.HistoryEvent(eventId=action.id, entityOperationCalled=called))
            completion = pb.HistoryEvent(
                eventId=-1,
                entityOperationCompleted=pb.EntityOperationCompletedEvent(
                    requestId=called.requestId,
                    output=helpers.get_string_value(json.dumps(response)),
                ),
            )
        incoming = [helpers.new_orchestrator_started_event(), completion]
    else:
        pytest.fail("Exceeded bounded registered workflow episodes")

    assert trace == ["seed", "relay", "return", "relay"]
    assert [name for name, _ in requests] == [f"dafx-{_WORKFLOW}-{name}" for name in ("a", "b", "a")]
    assert [len(request["contextMessages"]) for _, request in requests] == [1, 2, 2]
    assert len(final_output) == 1
    assert [message.message_id for message in final_output[0]] == ["source", "reply-a", "reply-b", "reply-a"]
    assert _json(serialize_input_message(final_output[0][0])) == _json(payload)
    for name, expected_calls in (("a", 2), ("b", 1)):
        assert len(clients[name].received_messages) == expected_calls
        for call_index, messages in enumerate(clients[name].received_messages):
            sources = [message for message in messages if message.message_id == "source"]
            assert len(sources) == 1, "Cold history must contain the original occurrence only once"
            # Envelope extras stay inert rather than becoming model attributes.
            assert not hasattr(sources[0], "future_message")
            if "future_message" not in payload:
                expected_source = deepcopy(payload)
                if call_index > 0:
                    # Core attributes provider-loaded history, not current input.
                    # The source occurrence is history only on a's second call.
                    assert name == "a" and call_index == 1
                    expected_source["additional_properties"]["_attribution"] = {
                        "source_id": "in_memory",
                        "source_type": "DurableHistoryProvider",
                    }
                assert _json(serialize_input_message(sources[0])) == _json(expected_source)
        current = clients[name].received_messages[0][0]
        assert _json(serialize_input_message(current)) == _json(payload)

    # Rebuild every recorded suspension/terminal point twice from fresh object
    # graphs. Check all action fields, exempting only RunRequest.created_at using
    # the existing strict timestamp oracle. No activities/entities run on replay.
    for _ in range(2):
        fresh_workflow, fresh_clients, fresh_trace = _graph(payload, copied)
        fresh = _worker(fresh_workflow)
        for old, new, expected, status in snapshots:
            replayed = _replay(fresh, _INSTANCE, old, new)
            assert replayed.encoded_custom_status == status
            assert len(replayed.actions) == len(expected)
            for actual, recorded in zip(replayed.actions, expected, strict=True):
                _assert_same_action_contract(actual, recorded)
        assert fresh_trace == []
        assert all(client.received_messages == [] for client in fresh_clients.values())
    return {
        "requests": [(name, _request_contract(request)) for name, request in requests],
        "receipts": receipt_snapshots,
        "output": [serialize_input_message(message) for message in final_output[0]],
    }


@pytest.mark.parametrize("stream", [False, True])
async def test_plain_core_attributes_history_but_not_current_input(stream: bool) -> None:
    # No durable entity, delivery loader, presence attachment or history helper.
    # This control establishes the Core hook behavior independently of transport.
    client = RecordingChatClient(response_message_id="reply")
    agent = Agent(client=client, context_providers=[InMemoryHistoryProvider(source_id="in_memory")])
    session = agent.create_session(session_id="core-attribution-control")
    source = Message(
        "user",
        [Content.from_text("control")],
        message_id="source",
        author_name="producer",
        additional_properties={"source_metadata": {"flag": False, "count": 0}},
    )
    expected_input: dict[str, Any] = {
        "type": "message",
        "role": "user",
        "contents": [{"type": "text", "text": "control", "additional_properties": {}}],
        "message_id": "source",
        "author_name": "producer",
        "additional_properties": {"source_metadata": {"flag": False, "count": 0}},
    }
    assert not hasattr(source, "_durable_original_core_message")
    assert not hasattr(source.contents[0], "_durable_original_core_content")
    assert _json(source.to_dict()) == _json(expected_input)
    for inputs in ([source], [Message("user", ["next"], message_id="next")]):
        if stream:
            async for _ in agent.run(inputs, session=session, stream=True):
                pass
        else:
            await agent.run(inputs, session=session)

    assert len(client.received_messages) == 2
    expected_history = deepcopy(expected_input)
    expected_history["additional_properties"]["_attribution"] = {
        "source_id": "in_memory",
        "source_type": "InMemoryHistoryProvider",
    }
    for messages, expected in zip(client.received_messages, (expected_input, expected_history), strict=True):
        sources = [message for message in messages if message.message_id == "source"]
        assert len(sources) == 1
        assert _json(sources[0].to_dict()) == _json(expected)
    # Attribution must not mutate the caller or the provider's retained input.
    assert _json(source.to_dict()) == _json(expected_input)
    stored: list[Message] = session.state["in_memory"]["messages"]
    stored_sources = [message for message in stored if message.message_id == "source"]
    assert len(stored_sources) == 1
    assert _json(stored_sources[0].to_dict()) == _json(expected_input)


@pytest.mark.parametrize("shape", _SHAPES)
@pytest.mark.parametrize("presence", ["absent", "null", "non-null"])
def test_registered_multihop_context_presence_matches_full_projection(shape: str, presence: str) -> None:
    payload = _payload(shape, presence)
    before = _json(payload)
    full = _run_registered(payload, copied=False)
    copied = _run_registered(payload, copied=True)
    assert copied == full
    assert _json(payload) == before


@pytest.mark.parametrize("shape", ["result", "approval", "shell-outputs"])
def test_registered_context_keeps_inert_extras_without_exposing_raw_sdk_objects(shape: str) -> None:
    payload = _payload(shape, "null", extras=True)
    before = _json(payload)
    full = _run_registered(payload, copied=False)
    copied = _run_registered(payload, copied=True)
    assert copied == full
    assert _json(payload) == before
