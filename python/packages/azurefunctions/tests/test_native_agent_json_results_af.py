# Copyright (c) Microsoft. All rights reserved.

"""Standalone agent results through registered entities and native orchestrators.

Histories are constructed service fixtures, not live host captures. Entity input
is ordinary JSON. SDK-looking metadata originates only in the actual response,
so these cases do not require the separate entity ingress/state JSON adapter.
"""

import json
from collections import defaultdict
from collections.abc import Generator
from copy import deepcopy
from typing import Any, cast
from unittest.mock import Mock

import azure.durable_functions as df
import pytest
from _execution_test_support import NonStreamingAgent, RecordingChatClient
from agent_framework import AgentResponse, Message
from agent_framework_durabletask import DurableAIAgent, RunRequest
from azure.durable_functions.models.actions.NoOpAction import NoOpAction
from azure.durable_functions.models.history.HistoryEvent import HistoryEvent
from azure.durable_functions.models.history.HistoryEventType import HistoryEventType
from azure.durable_functions.models.ReplaySchema import ReplaySchema
from azure.durable_functions.models.Task import AtomicTask, TaskState
from azure.durable_functions.models.TaskOrchestrationExecutor import TaskOrchestrationExecutor

from agent_framework_azurefunctions import AgentFunctionApp
from agent_framework_azurefunctions._orchestration import AzureFunctionsAgentExecutor
from agent_framework_azurefunctions._workflow_af_context import AzureFunctionsWorkflowContext, _JsonEntityContext

_CONSTRUCTIONS: list[Any] = []


class _ResultProbe:
    @classmethod
    def from_json(cls, value: Any) -> Any:
        _CONSTRUCTIONS.append(deepcopy(value))
        return {"constructed": value}


def _payload(marked: bool) -> dict[str, Any]:
    value: dict[str, Any] = {"keep": [None, False, 0, 0.0, 7, "雪"]}
    if marked:
        value.update(__module__=__name__, __class__="_ResultProbe", __data__={"value": 7})
    return value


@pytest.fixture(autouse=True)
def _reset_probe() -> None:
    _CONSTRUCTIONS.clear()


class _MetadataClient(RecordingChatClient):
    def __init__(self, value: dict[str, Any]) -> None:
        super().__init__()
        self.value = value

    async def _inner_get_response(self, **kwargs: Any) -> Any:
        assert kwargs["stream"] is False
        pending: Any = super()._inner_get_response(**kwargs)
        response = await pending
        response.messages[0].additional_properties["opaque"] = deepcopy(self.value)
        return response


def _app(value: dict[str, Any]) -> tuple[AgentFunctionApp, _MetadataClient]:
    client = _MetadataClient(value)
    app = AgentFunctionApp(
        agents=[NonStreamingAgent(client=client, name="json-agent")],
        enable_health_check=False,
        enable_http_endpoints=False,
        deployment_mode="isolated_v2",
    )
    return app, client


def _functions(app: AgentFunctionApp) -> dict[str, Any]:
    functions: dict[str, Any] = {}
    for function in app.get_functions():
        name = function.get_function_name()
        assert name is not None
        functions[name] = function.get_user_function()
    return functions


def _event(kind: int, event_id: int = -1, **fields: Any) -> dict[str, Any]:
    return {
        "EventType": kind,
        "EventId": event_id,
        "IsPlayed": True,
        "Timestamp": "2026-09-23T00:00:00Z",
        "Version": None,
        **fields,
    }


def _prefix() -> list[dict[str, Any]]:
    return [_event(12), _event(0, Name="standalone", Input="null")]


def _context_wire(rows: list[dict[str, Any]]) -> str:
    return json.dumps({
        "history": rows,
        "instanceId": "root",
        "isReplaying": True,
        "parentInstanceId": None,
        "input": "null",
        "upperSchemaVersion": ReplaySchema.V3.value,
    })


def _replay(function: Any, rows: list[dict[str, Any]]) -> dict[str, Any]:
    before = deepcopy(rows)
    result = json.loads(function(_context_wire(rows)))
    assert rows == before
    return result


def _entity_result(function: Any, name: str, key: str, request: Any) -> str:
    wire = json.dumps({
        "self": {"name": name, "key": key},
        "exists": False,
        "state": None,
        # The native entity input binding unwraps two JSON layers.
        "batch": [{"name": "run", "input": json.dumps(json.dumps(request))}],
    })
    batch = json.loads(function(wire))
    assert len(batch["results"]) == 1 and batch["results"][0]["isError"] is False
    return batch["results"][0]["result"]


@pytest.mark.parametrize("entry", ["get_agent", "executor", "workflow"])
@pytest.mark.parametrize("marked", [False, True], ids=["plain", "sdk-metadata"])
@pytest.mark.parametrize("reply_first", [False, True], ids=["waiting", "precompleted"])
def test_registered_agent_result_stays_json(entry: str, marked: bool, reply_first: bool) -> None:
    value = _payload(marked)
    app, client = _app(value)
    second_yield_completed: list[bool] = []

    @app.activity_trigger(input_name="value")
    def native_gate(value: Any) -> str:
        return "gate"

    @app.orchestration_trigger(context_name="context")
    def standalone(context: Any) -> Generator[Any, Any, Any]:
        deferred = context.deferred_tasks
        if entry == "workflow":
            task = AzureFunctionsWorkflowContext(context).prepare_agent_task("json-agent", "go", context.instance_id)
        else:
            agent = (
                app.get_agent(context, "json-agent")
                if entry == "get_agent"
                else DurableAIAgent(AzureFunctionsAgentExecutor(context), "json-agent")
            )
            task = agent.run("go")
            assert context.deferred_tasks is deferred and type(deferred) is dict
        child = task.children[0]
        action = child.action_repr
        assert task.action_repr is action and task.id is child.id is None
        gate = context.call_activity("native_gate")
        winner = yield context.task_any([task, gate])
        second_yield_completed.append(task.is_completed)
        assert winner is (task if reply_first else gate)
        assert child.id == 0 and gate.id == 1
        response = yield task
        assert response is task.result and child.action_repr is action and child.parent is task
        joined = yield context.task_all([task, gate])
        assert joined[0] is response and joined[1] == "gate"
        return response.messages[0].additional_properties["opaque"]  # noqa: B901 - Durable orchestrator result.

    functions = _functions(app)  # Index once, after all registrations.
    rows = _prefix()
    initial = _replay(functions["standalone"], rows)
    assert not initial["isDone"] and _CONSTRUCTIONS == []
    assert len(initial["actions"]) == len(initial["actions"][0]) == 1
    composite = initial["actions"][0][0]
    assert composite["actionType"] == 11
    entity, gate = composite["compoundActions"]
    assert [entity["actionType"], gate["actionType"]] == [7, 0]
    name, key = entity["instanceId"].split("@")[1:]
    assert name == "dafx-json-agent" and entity["operation"] == "run"
    request = json.loads(entity["input"])
    assert request["message"] == "go" and request["orchestrationId"] == "root"
    result_wire = _entity_result(functions[name], name, key, request)
    expected_request = dict(request)
    # The existing RunRequest timestamp is wall-clock based. Compare every
    # other action field, including deterministic correlation and session IDs.
    assert isinstance(expected_request.pop("created_at"), str)
    produced = json.loads(result_wire)["messages"][0]["additional_properties"]["opaque"]
    assert json.dumps(produced, sort_keys=True) == json.dumps(value, sort_keys=True)
    assert _CONSTRUCTIONS == [] and len(client.received_messages) == 1
    rows.extend([
        _event(4, 1, Name="native_gate", Input=gate["input"]),
        _event(14, 0, Name="op", Input='{"id":"agent-reply"}'),
    ])
    reply = _event(15, 101, Name="agent-reply", Input=json.dumps({"result": result_wire}))
    gate_done = _event(5, TaskScheduledId=1, Result=json.dumps(functions["native_gate"](None)))
    rows.extend([reply, gate_done] if reply_first else [gate_done, reply])
    for _ in range(2):
        final = _replay(functions["standalone"], rows)
        assert _CONSTRUCTIONS == []
        assert final["isDone"] and not final.get("error")
        assert json.dumps(final["output"], sort_keys=True) == json.dumps(value, sort_keys=True)
        assert second_yield_completed[-1] is reply_first
        assert len(final["actions"]) == len(final["actions"][0]) == 1
        final_composite = final["actions"][0][0]
        assert final_composite["actionType"] == 11
        final_entity, final_gate = final_composite["compoundActions"]
        assert final_gate == gate
        assert {k: v for k, v in final_entity.items() if k != "input"} == {
            k: v for k, v in entity.items() if k != "input"
        }
        final_request = json.loads(final_entity["input"])
        assert isinstance(final_request.pop("created_at"), str)
        assert final_request == expected_request
        assert len(client.received_messages) == 1


def test_cohosted_native_tasks_keep_their_decoder_and_deferred_callbacks() -> None:
    value = _payload(True)
    app, client = _app(value)

    @app.entity_trigger(context_name="context", entity_name="native")
    def native_entity(context: Any) -> None:
        context.set_result(value)

    functions = _functions(app)
    wire = _entity_result(functions["native_entity"], "native", "key", None)
    assert json.loads(wire) == value and _CONSTRUCTIONS == []
    rows = [
        *_prefix(),
        _event(4, 0, Name="gate", Input="null"),
        _event(15, 101, Name="native-event", Input=json.dumps(value)),
        _event(15, 102, Name="native-event", Input=json.dumps({"second": value})),
        _event(5, TaskScheduledId=0, Result="null"),
        _event(14, 1, Name="op", Input='{"id":"native-reply"}'),
        _event(15, 103, Name="native-reply", Input=json.dumps({"result": wire})),
        _event(5, TaskScheduledId=2, Result=json.dumps(value)),
        _event(HistoryEventType.SUB_ORCHESTRATION_INSTANCE_COMPLETED, TaskScheduledId=3, Result=json.dumps(value)),
    ]
    results: list[tuple[dict[str, Any], list[Any]]] = []
    for guarded in (False, True):
        _CONSTRUCTIONS.clear()

        def native(context: Any, *, guarded: bool = guarded) -> Generator[Any, Any, Any]:
            deferred = context.deferred_tasks
            if guarded:
                AzureFunctionsAgentExecutor(context)
            assert context.deferred_tasks is deferred and type(deferred) is dict
            yield context.call_activity("gate")
            tasks = [
                context.call_entity(df.EntityId("native", "key"), "run"),
                context.wait_for_external_event("native-event"),
                context.call_activity("native-activity"),
                context.call_sub_orchestrator("native-child"),
            ]
            values = yield context.task_all(tasks)
            values.append((yield context.wait_for_external_event("native-event")))
            assert context.deferred_tasks is deferred
            return values  # noqa: B901 - Durable orchestrator result.

        result = _replay(df.Orchestrator.create(native), rows)
        assert result["isDone"] and _CONSTRUCTIONS and not client.received_messages
        results.append((result, deepcopy(_CONSTRUCTIONS)))
    # The native control, not a version-specific expectation, defines decoding
    # and buffered-event semantics, including SDKs with a plain inner JSON load.
    assert results[1] == results[0]


def test_prepared_proxy_and_existing_native_task_identities_are_retained() -> None:
    context: Any = df.DurableOrchestrationContext.from_json(_context_wire(_prefix()))
    native = context.wait_for_external_event("native")
    context._add_to_open_tasks(native)
    native_waits = context.open_tasks["native"]
    deferred = context.deferred_tasks
    first = AzureFunctionsAgentExecutor(context)
    registry = context.open_tasks
    assert context.deferred_tasks is deferred
    assert AzureFunctionsAgentExecutor(cast(Any, first.context)).context is first.context
    adapter = AzureFunctionsWorkflowContext(context)
    proxy = _JsonEntityContext(context)
    assert AzureFunctionsAgentExecutor(cast(Any, proxy)).context is proxy
    task = adapter.prepare_agent_task("json-agent", "go", context.instance_id)
    assert task.state is TaskState.RUNNING
    assert context.open_tasks is registry and registry["native"] is native_waits
    assert native_waits == [native] and native.state is TaskState.RUNNING


@pytest.mark.parametrize("layout", ["none-history", "tuple-history", "missing-registry", "dict", "wrong-factory"])
def test_real_sdk_unknown_layout_fails_before_entity_call(layout: str) -> None:
    context: Any = df.DurableOrchestrationContext.from_json(_context_wire(_prefix()))
    context.call_entity = Mock(wraps=context.call_entity)
    if layout.endswith("history"):
        context._histories = None if layout == "none-history" else tuple(context.histories)
    elif layout == "missing-registry":
        del context.open_tasks
    else:
        context.open_tasks = {} if layout == "dict" else defaultdict(dict)
    executor = AzureFunctionsAgentExecutor(context)
    with pytest.raises(RuntimeError, match="Unsupported Durable Functions workflow entity"):
        executor.run_durable_agent("json-agent", RunRequest(message="go", correlation_id="request"))
    context.call_entity.assert_not_called()
    assert context._sequence_number == 0


def test_real_history_layout_change_fails_before_reply_decoding() -> None:
    reply = _event(15, 101, Name="agent-reply", Input=json.dumps({"result": json.dumps(_payload(True))}))
    context: Any = df.DurableOrchestrationContext.from_json(_context_wire([*_prefix(), reply]))
    task = AzureFunctionsAgentExecutor(context).run_durable_agent(
        "json-agent", RunRequest(message="go", correlation_id="request")
    )
    context._add_to_open_tasks(task)
    executor = TaskOrchestrationExecutor()
    executor.context = context
    executor.process_event(HistoryEvent(**_event(14, 0, Name="op", Input='{"id":"agent-reply"}')))
    context._histories = None
    with pytest.raises(RuntimeError, match="workflow entity history representation"):
        executor.set_task_value(HistoryEvent(**reply), True, "Name")
    assert _CONSTRUCTIONS == [] and task.state is TaskState.RUNNING


@pytest.mark.parametrize("spec", [False, True])
@pytest.mark.parametrize("precompleted", [False, True])
@pytest.mark.parametrize("failed", [False, True])
def test_history_free_doubles_forward_original_tasks(spec: bool, precompleted: bool, failed: bool) -> None:
    context = Mock(spec=df.DurableOrchestrationContext) if spec else Mock()
    child = AtomicTask(7, NoOpAction())
    error = ValueError("original entity failure")
    result = error if failed else AgentResponse(messages=[Message("assistant", ["done"])]).to_dict()
    if precompleted:
        child.set_value(failed, result)
    context.call_entity.return_value = child
    task = AzureFunctionsAgentExecutor(context).run_durable_agent(
        "json-agent", RunRequest(message="go", correlation_id="request")
    )
    assert task.children[0] is child and task.action_repr is child.action_repr and task.id == child.id
    if not precompleted:
        assert task.state is TaskState.RUNNING
        child.set_value(failed, result)
    assert task.state is (TaskState.FAILED if failed else TaskState.SUCCEEDED)
    if failed:
        assert task.result is error
    else:
        assert isinstance(task.result, AgentResponse) and task.result.text == "done"
    context.call_entity.assert_called_once()
