# Copyright (c) Microsoft. All rights reserved.

"""Framework payloads stay plain JSON on the handlers the Functions host invokes.

azure-functions-durable 2.x runs every orchestrator and entity on its own
``DurableFunctionsWorker``. That worker's converter rebuilds ``__class__``,
``__module__`` and ``__data__`` envelopes by calling ``from_json`` on a loaded class.
These tests call the generated handlers with serialized protobuf requests, as the
host does, while a probe records every construction. Histories are constructed
from the actions the handlers return, not captured from a live host.
"""

import asyncio
import inspect
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, get_type_hints
from unittest.mock import AsyncMock

import azure.durable_functions as df
import azure.functions as func
import pytest
from _af_handler_test_support import (
    _action,
    _app,
    _Client,
    _completed,
    _entity_function,
    _Host,
    _NonStreamingAgent,
    _result,
    _succeeded,
    _worker,
)
from _af_worker_test_support import FUNCTIONS_FRAMEWORK_CONVERTER, _event_wire_value, _orchestration_state
from agent_framework import (
    Executor,
    WorkflowBuilder,
    WorkflowContext,
    WorkflowExecutor,
    handler,
)
from agent_framework_durabletask import wrap_workflow_input
from agent_framework_durabletask._entities import AgentEntity
from agent_framework_durabletask._json_payload import JsonMigration, JsonPayload, _JsonPayloadConverter
from agent_framework_durabletask._workflows.dt_context import DurableTaskWorkflowContext
from azure.durable_functions.decorators.metadata import OrchestrationTrigger
from azure.durable_functions.http.builtin import BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME
from azure.durable_functions.internal.compat.orchestration_context import wrap_orchestrator
from azure.durable_functions.internal.serialization import DEFAULT_FUNCTIONS_DATA_CONVERTER
from azure.functions.decorators.function_app import FunctionBuilder
from durabletask.client import OrchestrationStatus
from durabletask.internal import helpers, type_discovery
from durabletask.internal import orchestrator_service_pb2 as pb
from typing_extensions import Never

from agent_framework_azurefunctions._app import _install_framework_json_decoding

_CONSTRUCTIONS: list[Any] = []


class _Probe:
    @classmethod
    def from_json(cls, value: Any) -> Any:
        _CONSTRUCTIONS.append(deepcopy(value))
        return {"constructed": value}


def _envelope() -> dict[str, Any]:
    # This module is loaded, so the Functions object hook can resolve the probe.
    return {"__class__": "_Probe", "__module__": __name__, "__data__": {"value": 7}}


def _payload() -> dict[str, Any]:
    return {"keep": [None, False, 0, "雪"], "sdk": _envelope(), "nested": [{"inner": _envelope()}]}


_CONSTRUCTED = {
    "keep": [None, False, 0, "雪"],
    "sdk": {"constructed": {"value": 7}},
    "nested": [{"inner": {"constructed": {"value": 7}}}],
}


@pytest.fixture(autouse=True)
def _reset_probe() -> None:
    _CONSTRUCTIONS.clear()


def _assert_json(actual: Any, expected: Any) -> None:
    assert json.dumps(actual, sort_keys=True, allow_nan=False) == json.dumps(expected, sort_keys=True, allow_nan=False)


class _Echo(Executor):
    def __init__(self) -> None:
        super().__init__(id="echo")
        self.seen: list[dict[str, Any]] = []

    @handler(input=dict, workflow_output=dict)
    async def handle(self, message: dict[str, Any], ctx: WorkflowContext[Never, dict[str, Any]]) -> None:
        self.seen.append(deepcopy(message))
        await ctx.yield_output(message)


def _client_event_wire(value: Any) -> str | None:
    """Return the event payload the real Functions client sends for ``value``."""

    async def capture() -> Any:
        client = df.DurableFunctionsClient(json.dumps({"taskHubName": "hub", "rpcBaseUrl": "localhost:1"}))
        stub = SimpleNamespace(RaiseEvent=AsyncMock())
        # Capture the request rather than send it over the client's channel.
        client._get_stub = lambda: stub  # type: ignore[method-assign]
        await client.raise_orchestration_event("root", "business", data=value)
        return stub.RaiseEvent.await_args.args[0]

    request = asyncio.run(capture())
    return request.input.value if request.HasField("input") else None


# Event values whose JSON type the Functions transport must keep.
_EVENT_VALUES = [_payload(), '{"looks": "like json"}', "null", None]
_EVENT_IDS = ["envelopes", "json-looking-string", "null-string", "none"]


def test_every_durable_function_on_the_app_uses_the_framework_converter() -> None:
    echo = _Echo()
    app = _app(
        agents=[_NonStreamingAgent(client=_Client(None), name="json-agent")],
        workflow=WorkflowBuilder(name="json", start_executor=echo, output_from=[echo]).build(),
    )

    @app.orchestration_trigger(context_name="context")
    def user_orchestrator(context: Any, value: Any) -> Any:
        return value

    @app.entity_trigger(context_name="context", entity_name="user-entity")
    def user_entity(context: Any, value: Any) -> Any:
        return value

    blueprint = df.Blueprint()

    @blueprint.orchestration_trigger(context_name="context")
    def blueprint_orchestrator(context: Any, value: Any) -> Any:
        return value

    @blueprint.entity_trigger(context_name="context", entity_name="blueprint-entity")
    def blueprint_entity(context: Any, value: Any) -> Any:
        return value

    app.register_blueprint(blueprint)
    functions = {function.get_function_name(): function for function in app.get_functions()}
    orchestrators = {
        name: _worker(function)
        for name, function in functions.items()
        if hasattr(function.get_user_function(), "orchestrator_function")
    }
    entities = {
        name: _worker(_entity_function(functions, name))
        for name in ("dafx-json-agent", "user-entity", "blueprint-entity")
    }
    assert {BUILTIN_HTTP_POLL_ORCHESTRATOR_NAME, "dafx-json", "user_orchestrator", "blueprint_orchestrator"} <= set(
        orchestrators
    )
    workers = [*orchestrators.values(), *entities.values()]
    assert all(isinstance(worker._data_converter, _JsonPayloadConverter) for worker in workers)
    assert len({id(worker) for worker in workers}) == len(workers)
    assert entities["dafx-json-agent"]._data_converter.deserializes_untagged_json is True
    assert entities["user-entity"]._data_converter.deserializes_untagged_json is False
    assert entities["blueprint-entity"]._data_converter.deserializes_untagged_json is False


def test_install_is_idempotent_and_an_unknown_layout_fails_closed() -> None:
    app = _app()

    @app.orchestration_trigger(context_name="context")
    def native(context: Any, value: Any) -> Any:
        return value

    builder = next(item for item in app._function_builders if item._function.get_function_name() == "native")
    worker = inspect.getclosurevars(inspect.unwrap(builder._function._func)).nonlocals["worker"]
    converter = worker._data_converter
    assert isinstance(converter, _JsonPayloadConverter)
    assert converter._inner is DEFAULT_FUNCTIONS_DATA_CONVERTER
    # Blueprint registration can reach an installed builder again.
    assert _install_framework_json_decoding(builder) is builder
    assert worker._data_converter is converter

    unknown: Any = SimpleNamespace(
        _function=SimpleNamespace(_func=lambda context: context, get_function_name=lambda: "unknown")
    )
    with pytest.raises(RuntimeError, match="durable worker for 'unknown' could not be located"):
        _install_framework_json_decoding(unknown)


def test_blueprint_durable_trigger_without_the_sdk_handler_fails_closed() -> None:
    """Durable functions are found by trigger type, so an unfamiliar handler cannot slip past."""
    blueprint = df.Blueprint()
    builder = FunctionBuilder(lambda context: context, function_script_file="function_app.py")
    builder.add_trigger(OrchestrationTrigger(name="context"))
    blueprint._function_builders.append(builder)

    with pytest.raises(RuntimeError, match="could not be located"):
        _app().register_blueprint(blueprint)


def test_generated_annotations_reach_the_handler_type_discovery() -> None:
    echo = _Echo()
    app = _app(
        agents=[_NonStreamingAgent(client=_Client(None), name="json-agent")],
        workflow=WorkflowBuilder(name="json", start_executor=echo, output_from=[echo]).build(),
    )
    functions = {function.get_function_name(): function for function in app.get_functions()}
    workflow = functions["dafx-json"]
    registered: Any = workflow.get_user_function()
    orchestrator = registered.orchestrator_function
    converter = _worker(workflow)._data_converter
    assert get_type_hints(orchestrator)["input_data"] is JsonPayload
    # The handler's worker registers the compat-wrapped orchestrator.
    assert type_discovery.orchestrator_input_type(wrap_orchestrator(orchestrator), converter) is JsonPayload
    agent = _entity_function(functions, "dafx-json-agent")
    entity = agent.get_user_function().entity_function
    assert get_type_hints(entity.run)["request"] is JsonPayload
    assert type_discovery.entity_input_type(entity, "run", _worker(agent)._data_converter) is JsonPayload
    assert type_discovery.entity_input_type(entity, "migrate", _worker(agent)._data_converter) is JsonMigration


@pytest.mark.parametrize("operation", ["reset", "expire_responses", "missing", "delete"])
def test_agent_entity_untagged_operation_input_stays_plain_json(operation: str) -> None:
    host = _Host(_app(agents=[_NonStreamingAgent(client=_Client(None), name="json-agent")]))

    result = host.entity("@dafx-json-agent@key", operation, json.dumps(_envelope()))

    assert len(result.results) == 1, result
    assert result.results[0].HasField("success" if operation == "delete" else "failure"), result
    assert _CONSTRUCTIONS == []


@pytest.mark.parametrize(
    "operation",
    [
        "set_state",
        "_set_state_dict",
        "get_state",
        "_get_state_dict",
        "persist_state",
        "_initialize_entity_context",
        "signal_entity",
        "schedule_new_orchestration",
        "__init__",
        "__class__",
        "run_agent",
    ],
)
def test_generated_agent_rejects_unsupported_operations_without_state_changes(operation: str) -> None:
    client = _Client(None)
    host = _Host(_app(agents=[_NonStreamingAgent(client=client, name="json-agent")]))
    state = '{"schemaVersion":"2.0.0","data":{"conversationHistory":[],"terminalResults":{},"completionReceipts":{}}}'

    batch = host.entity("@dafx-json-agent@key", operation, json.dumps(_envelope()), state)

    assert batch.results[0].HasField("failure"), batch
    assert batch.entityState.value == state
    assert client.options == []
    assert _CONSTRUCTIONS == []


def test_generated_agent_maintenance_results_and_delete_contract() -> None:
    client = _Client(None)
    host = _Host(_app(agents=[_NonStreamingAgent(client=client, name="json-agent")]))
    state = None
    for correlation in ("expired", "live"):
        batch = host.entity(
            "@dafx-json-agent@key", "run", json.dumps({"message": "go", "correlationId": correlation}), state
        )
        _succeeded(batch)
        state = batch.entityState.value
    assert isinstance(state, str)
    raw = json.loads(state)
    expired_at = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    completed_at = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    for field in ("terminalResults", "completionReceipts"):
        raw["data"][field]["expired"]["completedAt"] = completed_at
        raw["data"][field]["expired"]["resultExpiresAt"] = expired_at

    expired = host.entity("@dafx-json-agent@key", "expire_responses", None, json.dumps(raw))
    assert json.loads(_succeeded(expired)) == 1
    repeated = host.entity("@dafx-json-agent@key", "expire_responses", None, expired.entityState.value)
    assert json.loads(_succeeded(repeated)) == 0
    before_reset = json.loads(repeated.entityState.value)["data"]
    assert before_reset["conversationHistory"]
    assert before_reset["completionReceipts"]["expired"]["resultState"] == "unavailable"
    reset = host.entity("@dafx-json-agent@key", "reset", None, repeated.entityState.value)
    assert reset.results[0].HasField("success"), reset
    assert FUNCTIONS_FRAMEWORK_CONVERTER.deserialize(_succeeded(reset), JsonPayload) is None
    after_reset = json.loads(reset.entityState.value)["data"]
    assert after_reset["conversationHistory"] == []
    assert after_reset.get("session") is None
    assert after_reset["terminalResults"] == before_reset["terminalResults"]
    assert after_reset["completionReceipts"] == before_reset["completionReceipts"]
    deleted = host.entity("@dafx-json-agent@key", "delete", None, reset.entityState.value)
    assert deleted.results[0].HasField("success"), deleted
    assert not deleted.entityState.value
    assert FUNCTIONS_FRAMEWORK_CONVERTER.deserialize(_succeeded(deleted), JsonPayload) is None
    assert len(client.options) == 2
    assert _CONSTRUCTIONS == []


@pytest.mark.parametrize("nested", [False, True], ids=["root", "child"])
def test_generated_workflow_start_and_child_result_keep_envelopes_as_data(nested: bool) -> None:
    value = _payload()
    echo = _Echo()
    leaf = WorkflowBuilder(name="json-leaf", start_executor=echo, output_from=[echo]).build()
    workflow = leaf
    if nested:
        child = WorkflowExecutor(leaf, id="child", allow_direct_output=True)
        workflow = WorkflowBuilder(name="json-parent", start_executor=child, output_from=[child]).build()
    host = _Host(_app(workflow=workflow))
    # The Functions client serializes the start input with the Functions converter.
    wire = DEFAULT_FUNCTIONS_DATA_CONVERTER.serialize(wrap_workflow_input(value))
    first = host.start(f"dafx-{workflow.name}", "root", wire)

    if nested:
        action = _action(first, "createSubOrchestration")
        scheduled = action.createSubOrchestration
        waiting = host.replay(
            "root",
            helpers.new_sub_orchestration_created_event(
                action.id, scheduled.name, scheduled.instanceId, scheduled.input.value
            ),
        )
        assert waiting.actions == []
        started = host.start(scheduled.name, scheduled.instanceId, scheduled.input.value, parent="root")
        produced = _completed(host.activity(scheduled.instanceId, _action(started, "scheduleTask")))
        assert json.loads(produced.result.value)["outputs"] == [value]
        final = host.replay("root", helpers.new_sub_orchestration_completed_event(action.id, produced.result.value))
    else:
        scheduled = _action(first, "scheduleTask")
        assert json.loads(json.loads(scheduled.scheduleTask.input.value))["message"] == value
        final = host.activity("root", scheduled)

    assert json.loads(_completed(final).result.value) == [value]
    assert json.loads(_completed(host.replay("root")).result.value) == [value]
    assert echo.seen == [value] and _CONSTRUCTIONS == []


@pytest.mark.parametrize("protocol", ["current", "legacy"])
def test_get_agent_input_result_and_entity_state_keep_envelopes_as_data(protocol: str) -> None:
    value = _payload()
    client = _Client(value)
    app = _app(agents=[_NonStreamingAgent(client=client, name="json-agent")], enable_http_endpoints=False)

    @app.orchestration_trigger(context_name="context")
    def invoke(context: Any, _: Any) -> Any:
        response = yield app.get_agent(context, "json-agent").run("go", options={"metadata": value})
        return response.messages[0].additional_properties["opaque"]  # noqa: B901

    host = _Host(app)
    action = _action(host.start("invoke", "root", "null"), "sendEntityMessage")
    called = action.sendEntityMessage.entityOperationCalled
    batch = host.entity(called.targetInstanceId.value, called.operation, called.input.value)
    produced = _succeeded(batch)
    _assert_json(client.options[0]["metadata"], value)
    _assert_json(json.loads(produced)["messages"][0]["additional_properties"]["opaque"], value)
    # A later batch hydrates the stored response and replays it without the model.
    retried = host.entity(called.targetInstanceId.value, called.operation, called.input.value, batch.entityState.value)
    assert json.loads(_succeeded(retried)) == json.loads(produced)

    if protocol == "current":
        scheduled = pb.HistoryEvent(eventId=action.id, entityOperationCalled=called)
        done = pb.HistoryEvent(
            eventId=-1,
            entityOperationCompleted=pb.EntityOperationCompletedEvent(
                requestId=called.requestId, output=helpers.get_string_value(produced)
            ),
        )
    else:
        entity_id = called.targetInstanceId.value
        scheduled = helpers.new_event_sent_event(action.id, entity_id, json.dumps({"id": called.requestId}))
        done = helpers.new_event_raised_event(called.requestId, json.dumps({"result": produced}))
    _assert_json(json.loads(_completed(host.replay("root", scheduled, done)).result.value), value)
    _assert_json(json.loads(_completed(host.replay("root")).result.value), value)
    assert len(client.options) == 1 and _CONSTRUCTIONS == []


@pytest.mark.parametrize("value", _EVENT_VALUES, ids=_EVENT_IDS)
def test_event_wire_helper_matches_the_real_client(value: Any) -> None:
    """Replays built with _event_wire_value receive what the real client sends."""
    wire = _client_event_wire(value)

    assert _event_wire_value(value) == FUNCTIONS_FRAMEWORK_CONVERTER.deserialize(wire, JsonPayload)


@pytest.mark.parametrize("value", _EVENT_VALUES, ids=_EVENT_IDS)
@pytest.mark.parametrize("early", [False, True], ids=["waiting", "buffered"])
def test_framework_event_keeps_envelopes_as_data(early: bool, value: Any) -> None:
    app = _app()

    @app.orchestration_trigger(context_name="context")
    def wait(context: Any, _: Any) -> Any:
        yield context.call_activity("gate")
        result = yield DurableTaskWorkflowContext(context).wait_for_external_event("business")
        return result  # noqa: B901

    host = _Host(app)
    gate = _action(host.start("wait", "root", "null"), "scheduleTask")
    scheduled = helpers.new_task_scheduled_event(gate.id, "gate")
    done = helpers.new_task_completed_event(gate.id, "null")
    # The event carries exactly what the real Functions client sends for the value.
    event = helpers.new_event_raised_event("business", _client_event_wire(value))
    events = [scheduled, event, done] if early else [scheduled, done, event]
    assert _result(_completed(host.replay("root", *events))) == value
    assert _result(_completed(host.replay("root"))) == value
    assert _CONSTRUCTIONS == []


def test_native_cohost_keeps_functions_object_decoding() -> None:
    app = _app(agents=[_NonStreamingAgent(client=_Client(None), name="json-agent")])

    # Unannotated native inputs and state keep the Functions converter's decoding.
    @app.orchestration_trigger(context_name="context")
    def native(context: Any, value):
        return value

    @app.entity_trigger(context_name="context", entity_name="native-json")
    def native_entity(context: Any, value):
        return {"input": value, "state": context.get_state()}

    host = _Host(app)
    wire = json.dumps(_payload())
    assert json.loads(_completed(host.start("native", "root", wire)).result.value) == _CONSTRUCTED
    assert _CONSTRUCTIONS == [{"value": 7}] * 2
    result = host.entity("@native-json@key", "read", wire, wire)
    assert json.loads(_succeeded(result)) == {"input": _CONSTRUCTED, "state": _CONSTRUCTED}
    assert _CONSTRUCTIONS == [{"value": 7}] * 6


async def test_workflow_status_keeps_output_and_custom_status_envelopes_as_data() -> None:
    echo = _Echo()
    app = _app(workflow=WorkflowBuilder(name="json", start_executor=echo, output_from=[echo]).build())
    functions = {function.get_function_name(): function for function in app.get_functions()}
    registered: Any = functions["dafx-json-status"].get_user_function()
    status = registered.client_function
    client = AsyncMock()
    client.get_orchestration_state.return_value = _orchestration_state(
        "root",
        "dafx-json",
        runtime_status=OrchestrationStatus.COMPLETED,
        custom_status=_payload(),
        output=[_payload()],
    )
    request = func.HttpRequest(
        method="GET",
        url="https://example.test/api/workflow/json/status/root",
        body=b"",
        route_params={"instanceId": "root"},
    )

    response = await status(req=request, client=client)

    assert response.status_code == 200
    body = json.loads(response.get_body())
    _assert_json(body["output"], [_payload()])
    _assert_json(body["customStatus"], _payload())
    assert _CONSTRUCTIONS == []


@pytest.mark.parametrize(
    ("token", "expected"),
    [
        ("9223372036854775807.0", 2**63 - 1),
        ("1.00000000000000001", None),
        ("9223372036854775808.0", None),
        ("0e9999999999999999999", 0),
        ("1e-9999999999999999999", None),
    ],
)
def test_entity_state_counter_tokens_decode_exactly(
    token: str, expected: int | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed: list[Any] = []

    def read(self: AgentEntity) -> int:
        observed.append(self.state.data.ingested_positions)
        return 0

    monkeypatch.setattr(AgentEntity, "expire_responses", read)
    state = (
        '{"schemaVersion":"2.0.0","data":{"conversationHistory":[],"terminalResults":{},"completionReceipts":{},'
        '"ingestedPositions":{"producer":' + token + "}}}"
    )
    host = _Host(_app(agents=[_NonStreamingAgent(client=_Client(None), name="json-agent")]))

    batch = host.entity("@dafx-json-agent@key", "expire_responses", None, state)

    if expected is None:
        assert batch.results[0].HasField("failure"), batch
        assert batch.results[0].failure.failureDetails.errorType.endswith("ValueError")
        assert observed == []
    else:
        assert json.loads(_succeeded(batch)) == 0
        assert observed == [{"producer": expected}]
