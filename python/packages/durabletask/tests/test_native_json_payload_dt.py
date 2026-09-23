# Copyright (c) Microsoft. All rights reserved.

"""Native SDK boundaries, without later-stack runtime or migration test helpers."""

import json
import logging
from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, get_type_hints
from unittest.mock import Mock

import pytest
from agent_framework import (
    Agent,
    BaseChatClient,
    ChatResponse,
    Executor,
    Message,
    WorkflowBuilder,
    WorkflowContext,
    WorkflowExecutor,
    handler,
)
from durabletask.client import TaskHubGrpcClient
from durabletask.entities import EntityContext, EntityInstanceId
from durabletask.internal import helpers, type_discovery
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.internal.entity_state_shim import StateShim
from durabletask.serialization import JsonDataConverter
from durabletask.worker import TaskHubGrpcWorker, _ActivityExecutor, _EntityExecutor, _OrchestrationExecutor
from typing_extensions import Never

from agent_framework_durabletask import DurableAIAgentWorker, DurableWorkflowClient, RunRequest
from agent_framework_durabletask._entities import DurableTaskEntityStateProvider
from agent_framework_durabletask._executors import OrchestrationAgentExecutor
from agent_framework_durabletask._workflows.dt_context import DurableTaskWorkflowContext

_LOG = logging.getLogger(__name__)
_NOW = datetime(2026, 9, 23, tzinfo=timezone.utc)


def _payload(marker: bool) -> dict[str, Any]:
    return {
        "__durabletask_autoobject__": marker,
        "keep": [None, False, 0, "雪"],
        "nested": {"__durabletask_autoobject__": marker, "value": "nested"},
    }


class _Client(BaseChatClient):
    def __init__(self, value: Any) -> None:
        super().__init__()
        self.value = value
        self.options: list[dict[str, Any]] = []

    async def _inner_get_response(
        self, *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
    ) -> ChatResponse:
        assert not stream and messages
        self.options.append(deepcopy(dict(options)))
        return ChatResponse(messages=[Message("assistant", ["done"], additional_properties={"opaque": self.value})])


class _NonStreamingAgent(Agent):
    def run(self, *args: Any, **kwargs: Any) -> Any:
        if kwargs.get("stream"):
            raise TypeError("stream is not supported")
        return super().run(*args, **kwargs)


class _Echo(Executor):
    def __init__(self) -> None:
        super().__init__(id="echo")
        self.seen: list[dict[str, Any]] = []

    @handler(input=dict, workflow_output=dict)
    async def handle(self, message: dict[str, Any], ctx: WorkflowContext[Never, dict[str, Any]]) -> None:
        self.seen.append(deepcopy(message))
        await ctx.yield_output(message)


class _Replay:
    def __init__(self) -> None:
        self.worker: Any = TaskHubGrpcWorker(channel=Mock())
        self.host = DurableAIAgentWorker(self.worker)
        self.histories: dict[str, list[Any]] = {}

    def start(self, name: str, instance_id: str, wire: str, *, parent: str | None = None) -> Any:
        self.histories[instance_id] = []
        event = helpers.new_execution_started_event(name, instance_id, wire)
        if parent is not None:
            event.executionStarted.parentInstance.orchestrationInstance.instanceId = parent
        return self.replay(instance_id, event)

    def replay(self, instance_id: str, *events: Any) -> Any:
        old = self.histories[instance_id]
        new = [helpers.new_orchestrator_started_event(_NOW), *events]
        result = _OrchestrationExecutor(self.worker._registry, _LOG, self.worker._data_converter).execute(
            instance_id, old, new
        )
        self.histories[instance_id] = [*old, *new]
        return result

    def activity(self, instance_id: str, action: Any) -> Any:
        scheduled = action.scheduleTask
        wire = _ActivityExecutor(self.worker._registry, _LOG, self.worker._data_converter).execute(
            instance_id, scheduled.name, action.id, scheduled.input.value
        )
        return self.replay(
            instance_id,
            helpers.new_task_scheduled_event(action.id, scheduled.name, scheduled.input.value),
            helpers.new_task_completed_event(action.id, wire),
        )


def _action(result: Any, kind: str) -> Any:
    assert len(result.actions) == 1, result.actions
    action = result.actions[0]
    assert action.HasField(kind), action
    return action


def _completed(result: Any) -> Any:
    completion = _action(result, "completeOrchestration").completeOrchestration
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED, completion
    return completion


@pytest.mark.parametrize("marker", [False, True])
@pytest.mark.parametrize("nested", [False, True], ids=["root", "child"])
def test_generated_workflow_preserves_native_start_and_child_result(marker: bool, nested: bool) -> None:
    value = _payload(marker)
    echo = _Echo()
    leaf = WorkflowBuilder(name="json-leaf", start_executor=echo, output_from=[echo]).build()
    workflow = leaf
    if nested:
        child = WorkflowExecutor(leaf, id="child", allow_direct_output=True)
        workflow = WorkflowBuilder(name="json-parent", start_executor=child, output_from=[child]).build()
    replay = _Replay()
    replay.host.configure_workflow(workflow)

    native: Any = TaskHubGrpcClient(channel=Mock())
    native._stub.StartInstance = Mock(return_value=pb.CreateInstanceResponse(instanceId="root"))
    client = DurableWorkflowClient(native, workflow_name=workflow.name)
    assert client.start_workflow(value, instance_id="root") == "root"
    request = native._stub.StartInstance.call_args.args[0]
    first = replay.start(request.name, "root", request.input.value)

    if nested:
        action = _action(first, "createSubOrchestration")
        scheduled = action.createSubOrchestration
        waiting = replay.replay(
            "root",
            helpers.new_sub_orchestration_created_event(
                action.id, scheduled.name, scheduled.instanceId, scheduled.input.value
            ),
        )
        assert waiting.actions == []
        started = replay.start(scheduled.name, scheduled.instanceId, scheduled.input.value, parent="root")
        produced = _completed(replay.activity(scheduled.instanceId, _action(started, "scheduleTask")))
        assert json.loads(produced.result.value)["outputs"] == [value]
        final = replay.replay("root", helpers.new_sub_orchestration_completed_event(action.id, produced.result.value))
    else:
        scheduled = _action(first, "scheduleTask")
        assert json.loads(json.loads(scheduled.scheduleTask.input.value))["message"] == value
        final = replay.activity("root", scheduled)

    assert json.loads(_completed(final).result.value) == [value]
    assert json.loads(_completed(replay.replay("root")).result.value) == [value]
    assert echo.seen == [value]


@pytest.mark.parametrize("marker", [False, True])
@pytest.mark.parametrize("protocol", ["current", "legacy"])
def test_registered_agent_input_and_native_entity_completion(marker: bool, protocol: str) -> None:
    value = _payload(marker)
    client = _Client(value)
    replay = _Replay()
    replay.host.add_agent(_NonStreamingAgent(client=client, name="json-agent"))

    def invoke(context: Any, _: Any) -> Any:
        request = RunRequest("go", correlation_id="request", created_at=_NOW, options={"metadata": value})
        response = yield OrchestrationAgentExecutor(context).run_durable_agent("json-agent", request)
        return response.messages[0].additional_properties["opaque"]  # noqa: B901

    replay.worker.add_orchestrator(invoke)
    action = _action(replay.start("invoke", "root", "null"), "sendEntityMessage")
    called = action.sendEntityMessage.entityOperationCalled
    entity_id = EntityInstanceId.parse(called.targetInstanceId.value)
    state = StateShim(None, replay.worker._data_converter, is_serialized=True)
    produced = _EntityExecutor(replay.worker._registry, _LOG, replay.worker._data_converter).execute(
        "root", entity_id, called.operation, state, called.input.value
    )
    assert produced is not None
    assert client.options[0]["metadata"] == value
    assert json.loads(produced)["messages"][0]["additional_properties"]["opaque"] == value

    if protocol == "current":
        scheduled = pb.HistoryEvent(eventId=action.id, entityOperationCalled=called)
        done = pb.HistoryEvent(
            eventId=-1,
            entityOperationCompleted=pb.EntityOperationCompletedEvent(
                requestId=called.requestId, output=helpers.get_string_value(produced)
            ),
        )
    else:
        scheduled = helpers.new_event_sent_event(action.id, str(entity_id), json.dumps({"id": called.requestId}))
        done = helpers.new_event_raised_event(called.requestId, json.dumps({"result": produced}))
    assert json.loads(_completed(replay.replay("root", scheduled, done)).result.value) == value
    assert json.loads(_completed(replay.replay("root")).result.value) == value
    assert len(client.options) == 1


@pytest.mark.parametrize("marker", [False, True])
def test_framework_state_reads_plain_json_through_real_state_shim(marker: bool) -> None:
    replay = _Replay()
    value = {**_payload(marker), "schemaVersion": "1.1.0", "data": {"conversationHistory": []}}
    wire = json.dumps(value)
    for _ in range(2):
        shim = StateShim(wire, replay.worker._data_converter, is_serialized=True)
        provider = DurableTaskEntityStateProvider()
        provider._initialize_entity_context(
            EntityContext(
                "root", "read", shim, EntityInstanceId("dafx-json-agent", "key"), replay.worker._data_converter
            )
        )
        actual = provider._get_state_dict()
        assert actual == value
        assert shim.encode_state() == wire  # Reading must not write or re-encode.
        actual["keep"].append("local")
        assert provider._get_state_dict() == value  # Every SDK read is detached.
        provider._set_state_dict(value)
        shim.commit()
        committed = shim.encode_state()
        assert committed is not None and json.loads(committed) == value
        wire = committed


@pytest.mark.parametrize("value", [False, 0, "", "text", [], [["key", "value"]]])
def test_state_tag_does_not_coerce_nonobject_state(value: Any) -> None:
    replay = _Replay()
    shim = StateShim(json.dumps(value), replay.worker._data_converter, is_serialized=True)
    provider = DurableTaskEntityStateProvider()
    provider._initialize_entity_context(
        EntityContext("root", "read", shim, EntityInstanceId("dafx-json-agent", "key"), replay.worker._data_converter)
    )
    with pytest.raises(ValueError, match="Durable entity state must be a JSON object"):
        provider._get_state_dict()
    assert shim.encode_state() == json.dumps(value)


@pytest.mark.parametrize("marker", [False, True])
@pytest.mark.parametrize("early", [False, True], ids=["waiting", "buffered"])
def test_framework_event_preserves_json_for_waiting_and_buffered_delivery(marker: bool, early: bool) -> None:
    value = _payload(marker)
    replay = _Replay()

    def wait(context: Any, _: Any) -> Any:
        yield context.call_activity("gate")
        result = yield DurableTaskWorkflowContext(context).wait_for_external_event("business")
        return result  # noqa: B901

    replay.worker.add_orchestrator(wait)
    gate = _action(replay.start("wait", "root", "null"), "scheduleTask")
    scheduled = helpers.new_task_scheduled_event(gate.id, "gate")
    done = helpers.new_task_completed_event(gate.id, "null")
    event = helpers.new_event_raised_event("business", json.dumps(value))
    events = [scheduled, event, done] if early else [scheduled, done, event]
    assert json.loads(_completed(replay.replay("root", *events)).result.value) == value
    assert json.loads(_completed(replay.replay("root")).result.value) == value


@pytest.mark.parametrize("marker", [False, True])
def test_native_cohost_keeps_default_marker_semantics(marker: bool) -> None:
    replay = _Replay()
    replay.host.add_agent(_NonStreamingAgent(client=_Client(None), name="json-agent"))

    def describe(value: Any) -> dict[str, Any]:
        return {"kind": type(value).__name__, "value": vars(value) if isinstance(value, SimpleNamespace) else value}

    def native(context: Any, value: Any) -> Any:
        return describe(value)

    def native_entity(context: Any, value: Any) -> Any:
        return {"input": describe(value), "state": describe(context.get_state())}

    replay.worker.add_orchestrator(native)
    replay.worker.add_entity(native_entity, name="native-json")
    wire = json.dumps(_payload(marker))
    expected = {
        "kind": "SimpleNamespace" if marker else "dict",
        "value": {"keep": [None, False, 0, "雪"], "nested": {"value": "nested"}},
    }
    assert json.loads(_completed(replay.start("native", "root", wire)).result.value) == expected
    shim = StateShim(wire, replay.worker._data_converter, is_serialized=True)
    result = _EntityExecutor(replay.worker._registry, _LOG, replay.worker._data_converter).execute(
        "root", EntityInstanceId("native-json", "key"), "read", shim, wire
    )
    assert result is not None and json.loads(result) == {"input": expected, "state": expected}
    assert shim.encode_state() == wire


def test_generated_annotations_reach_native_type_discovery() -> None:
    # Import new internals only in wiring/unit tests so baseline behavior tests
    # can be selected without a collection failure on the unmodified package.
    from agent_framework_durabletask._json_payload import JsonPayload

    replay = _Replay()
    echo = _Echo()
    replay.host.configure_workflow(WorkflowBuilder(name="json", start_executor=echo, output_from=[echo]).build())
    replay.host.add_agent(_NonStreamingAgent(client=_Client(None), name="json-agent"))
    workflow = replay.worker._registry.get_orchestrator("dafx-json")
    entity = replay.worker._registry.get_entity("dafx-json-agent")
    assert not isinstance(JsonPayload, type)
    assert get_type_hints(workflow)["input_data"] is JsonPayload
    assert get_type_hints(entity.run)["request"] is JsonPayload
    assert type_discovery.orchestrator_input_type(workflow, replay.worker._data_converter) is JsonPayload
    assert type_discovery.entity_input_type(entity, "run", replay.worker._data_converter) is JsonPayload


def test_native_annotated_input_still_uses_custom_converter() -> None:
    class NativeInput:  # noqa: B903 - deliberately not a default-converter dataclass
        def __init__(self, value: Any) -> None:
            self.value = value

    class Converter(JsonDataConverter):
        def can_reconstruct(self, target_type: Any) -> bool:
            return target_type is NativeInput or super().can_reconstruct(target_type)

        def deserialize(self, data: str | None, target_type: Any = None) -> Any:
            if target_type is NativeInput:
                assert data is not None
                return NativeInput(json.loads(data))
            return super().deserialize(data, target_type)

    worker: Any = TaskHubGrpcWorker(channel=Mock(), data_converter=Converter())
    DurableAIAgentWorker(worker).add_agent(_NonStreamingAgent(client=_Client(None), name="json-agent"))

    def native(context: Any, value: NativeInput) -> Any:
        assert isinstance(value, NativeInput)
        return value.value

    worker.add_orchestrator(native)
    value = _payload(True)
    result = _OrchestrationExecutor(worker._registry, _LOG, worker._data_converter).execute(
        "root",
        [],
        [
            helpers.new_orchestrator_started_event(_NOW),
            helpers.new_execution_started_event("native", "root", json.dumps(value)),
        ],
    )
    assert json.loads(_completed(result).result.value) == value


def test_converter_delegation_and_worker_local_idempotence() -> None:
    from agent_framework_durabletask._json_payload import JsonPayload

    # A typed interface need not inherit the SDK ABC. Keep custom converter
    # answers, identities, exceptions and call arguments, not just default JSON.
    inner: Any = SimpleNamespace(
        serialize=Mock(return_value="custom-wire"),
        deserialize=Mock(return_value=object()),
        coerce=Mock(return_value=object()),
        can_reconstruct=Mock(return_value=True),
    )
    worker: Any = TaskHubGrpcWorker(channel=Mock(), data_converter=inner)
    other: Any = TaskHubGrpcWorker(channel=Mock(), data_converter=inner)
    DurableAIAgentWorker(worker)
    converter = worker._data_converter
    DurableAIAgentWorker(worker)
    assert worker._data_converter is converter and other._data_converter is inner
    value = _payload(True)
    assert converter.can_reconstruct(JsonPayload) is True
    inner.can_reconstruct.assert_not_called()
    assert converter.deserialize(json.dumps(value), JsonPayload) == value
    inner.deserialize.assert_not_called()
    assert converter.serialize(value) == "custom-wire"
    inner.serialize.assert_called_once_with(value)
    for target in (None, dict, SimpleNamespace):
        assert converter.deserialize("custom-wire", target) is inner.deserialize.return_value
        inner.deserialize.assert_called_with("custom-wire", target)
        assert converter.coerce(value, target) is inner.coerce.return_value
        inner.coerce.assert_called_with(value, target)
        assert converter.can_reconstruct(target) is True
        inner.can_reconstruct.assert_called_with(target)
    assert converter.coerce(value, JsonPayload) is inner.coerce.return_value
    inner.coerce.assert_called_with(value, JsonPayload)
    inner.can_reconstruct.return_value = False
    assert converter.can_reconstruct(dict) is False
    error = ValueError("custom decoder rejected input")
    inner.deserialize.side_effect = error
    with pytest.raises(ValueError) as caught:
        converter.deserialize("custom-wire")
    assert caught.value is error
    assert isinstance(JsonDataConverter().deserialize(json.dumps(value)), SimpleNamespace)


@pytest.mark.parametrize(
    ("wire", "expected"),
    [
        (None, None),
        ("", None),
        ("null", None),
        ("false", False),
        ("0", 0),
        (json.dumps('{"not":"parsed twice"}'), '{"not":"parsed twice"}'),
        ("[]", []),
        ("{}", {}),
    ],
)
def test_tag_only_decodes_one_json_layer(wire: str | None, expected: Any) -> None:
    from agent_framework_durabletask._json_payload import JsonPayload

    replay = _Replay()
    assert replay.worker._data_converter.deserialize(wire, JsonPayload) == expected


def test_invalid_framework_json_does_not_fall_back_to_custom_decoder() -> None:
    from agent_framework_durabletask._json_payload import JsonPayload

    inner: Any = Mock(wraps=JsonDataConverter())
    worker: Any = TaskHubGrpcWorker(channel=Mock(), data_converter=inner)
    DurableAIAgentWorker(worker)
    with pytest.raises(json.JSONDecodeError):
        worker._data_converter.deserialize("not-json", JsonPayload)
    inner.deserialize.assert_not_called()


def test_installation_rejects_running_worker_without_replacing_converter() -> None:
    worker: Any = TaskHubGrpcWorker(channel=Mock())
    original = worker._data_converter
    worker._is_running = True  # No worker thread or network connection is started.
    try:
        with pytest.raises(RuntimeError, match="before starting"):
            DurableAIAgentWorker(worker)
        assert worker._data_converter is original
    finally:
        worker._is_running = False


@pytest.mark.parametrize("converter", [None, object()])
def test_installation_checks_private_converter_interface(converter: Any) -> None:
    worker: Any = SimpleNamespace(_data_converter=converter, _is_running=False)
    with pytest.raises(RuntimeError, match="data converter interface"):
        DurableAIAgentWorker(worker)
    assert worker._data_converter is converter


def test_registration_double_does_not_treat_mock_running_flag_as_true() -> None:
    worker = Mock()
    agent = Mock(name="agent")
    agent.name = "test-agent"
    DurableAIAgentWorker(worker).add_agent(agent)
    worker.add_entity.assert_called_once()
