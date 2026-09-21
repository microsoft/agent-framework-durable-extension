# Copyright (c) Microsoft. All rights reserved.

"""Reply security through native Functions decoding and registered activities.

Service histories are constructed, not live host captures. The native client
serializer, context from_json, SDK replay and registered functions are real.
Only the client's network transport/status lookup is replaced.
"""

import asyncio
import importlib
import json
from collections.abc import Generator
from copy import deepcopy
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import azure.durable_functions as df
import pytest
from agent_framework import AgentResponse, Message
from agent_framework_durabletask import RunRequest, load_agent_response, wrap_workflow_input
from agent_framework_durabletask._workflows.serialization import deserialize_workflow_output
from azure.durable_functions.models.ReplaySchema import ReplaySchema
from azure.durable_functions.models.Task import TaskState
from azure.functions import _durable_functions as sdk_codec
from test_workflow_agent_rejection_backlog_review import _History, _workflow
from test_workflow_buffered_events_review_af import _event, _execute, _prefix
from test_workflow_generic_hitl_review import (
    _VALIDATOR_CALLS,
    _complete_generic_activity,
    _CountedDecision,
    _generic_workflow,
)
from test_workflow_generic_hitl_review_af import _request
from test_workflow_mixed_hitl_review import _atomic_actions, _Episodes
from test_workflow_recorded_replay_review import _replay

from agent_framework_azurefunctions import AgentFunctionApp
from agent_framework_azurefunctions._orchestration import AzureFunctionsAgentExecutor
from agent_framework_azurefunctions._workflow_af_context import AzureFunctionsWorkflowContext, _JsonEntityContext

_DECODER_CALLS: list[Any] = []
_UNLOADED_MODULE = "_af_untrusted_event_import_sentinel"


class _DecoderProbe:
    @classmethod
    def from_json(cls, value: Any) -> Any:
        _DECODER_CALLS.append(value)
        return {"unexpected_constructor": value}


def _metadata(module: str = __name__) -> dict[str, Any]:
    return {"__class__": "_DecoderProbe", "__module__": module, "__data__": {"value": 7}, "business": "keep"}


@pytest.fixture
def import_attempts(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    _DECODER_CALLS.clear()
    _VALIDATOR_CALLS.clear()
    attempts: list[str] = []
    original = importlib.import_module

    def observe(name: str, package: str | None = None) -> Any:
        if name in (__name__, _UNLOADED_MODULE):
            attempts.append(name)
        if name == _UNLOADED_MODULE:
            raise AssertionError("Reply-selected module import before admission")
        return original(name, package)

    # Older SDKs import the function by value. Cover that alias as well as the
    # normal importlib entry point, without replacing the decoder itself.
    monkeypatch.setattr(importlib, "import_module", observe)
    monkeypatch.setattr(sdk_codec, "import_module", observe)
    return attempts


def test_benign_probe_detects_the_native_sdk_custom_object_hook(import_attempts: list[str]) -> None:
    result = json.loads(json.dumps(_metadata()), object_hook=sdk_codec._deserialize_custom_object)
    assert result == {"unexpected_constructor": {"value": 7}}
    assert _DECODER_CALLS == [{"value": 7}] and import_attempts == [__name__]
    with pytest.raises(AssertionError, match="Reply-selected module import"):
        json.loads(json.dumps(_metadata(_UNLOADED_MODULE)), object_hook=sdk_codec._deserialize_custom_object)


class _NativeReplies:
    def __init__(self, annotation: Any = dict) -> None:
        self.workflow, self.seen = _generic_workflow(annotation)
        app = AgentFunctionApp(workflow=self.workflow, enable_health_check=False, deployment_mode="isolated_v2")
        self.functions: dict[str, Any] = {}
        self.respond: Any = None
        for function in app.get_functions():
            name = function.get_function_name()
            assert name is not None
            self.functions[name] = function.get_user_function()
            trigger = function.get_trigger()
            assert trigger is not None
            if "/respond/" in trigger.get_dict_repr().get("route", ""):
                self.respond = cast(Any, function.get_user_function()).client_function
        self.start = json.dumps(wrap_workflow_input("go"))
        self.rows = [_event(12), _event(0, Name="dafx-generic-hitl", Input=self.start)]
        self.results: list[dict[str, Any]] = []
        self.client: Any = df.DurableOrchestrationClient(
            json.dumps({
                "taskHubName": "security-test",
                "creationUrls": {},
                "managementUrls": {},
                "rpcBaseUrl": "https://example.test/",
            })
        )
        self.client.get_status = AsyncMock(
            return_value=SimpleNamespace(
                name="dafx-generic-hitl", runtime_status=df.OrchestrationRuntimeStatus.Running, custom_status=None
            )
        )
        self.posts: list[str] = []

        async def post(url: str, body: str, *args: Any, **kwargs: Any) -> list[Any]:
            assert "/instances/root/raiseEvent/approval" in url
            self.posts.append(body)
            self.rows.append(_event(15, 100 + len(self.posts), Name="approval", Input=body))
            return [202, None]

        self.client._post_async_request = post

    def deliver(self, payload: Any, *, http: bool) -> None:
        if http:
            result = asyncio.run(self.respond(_request("respond", payload=payload), self.client))
            assert result.status_code == 200
        else:
            asyncio.run(self.client.raise_event("root", "approval", payload))
        assert json.loads(self.posts[-1]) == payload

    def replay(self) -> dict[str, Any]:
        # Invoke the exact registered SDK wrapper, including from_json. Each
        # invocation creates a new context and native TaskOrchestrationExecutor.
        before = deepcopy(self.rows)
        state = json.loads(
            self.functions["dafx-generic-hitl"](
                json.dumps({
                    "history": self.rows,
                    "instanceId": "root",
                    "isReplaying": True,
                    "parentInstanceId": None,
                    "input": self.start,
                    "upperSchemaVersion": ReplaySchema.V3.value,
                })
            )
        )
        assert self.rows == before
        return state

    def complete(self, state: dict[str, Any]) -> dict[str, Any]:
        scheduled = [action for action in _atomic_actions(state["actions"]) if action["actionType"] == 0]
        assert len(scheduled) == len(self.results) + 1
        action = scheduled[-1]
        assert action["functionName"] == "dafx-generic-hitl-gate"
        task_id = len(self.results)
        result = self.functions[action["functionName"]](json.loads(action["input"]))
        self.results.append(json.loads(result))
        self.rows.extend([
            _event(4, task_id, Name=action["functionName"], Input=action["input"]),
            _event(5, TaskScheduledId=task_id, Result=json.dumps(result)),
        ])
        return self.results[-1]


@pytest.mark.parametrize("http", [False, True], ids=["native", "http"])
@pytest.mark.parametrize("early", [False, True])
@pytest.mark.parametrize("shape", ["root", "nested", "unknown"])
def test_native_reply_metadata_is_data_before_and_after_activity_admission(
    http: bool, early: bool, shape: str, import_attempts: list[str]
) -> None:
    payload: dict[str, Any] = (
        _metadata() if shape == "root" else {"items": [_metadata(_UNLOADED_MODULE if shape == "unknown" else __name__)]}
    )
    payload.update({"count": 7, "unknown_business_field": [False, None, {"input": "unchanged"}]})
    native = _NativeReplies()
    if early:
        native.deliver(payload, http=http)
    native.complete(native.replay())
    if not early:
        native.deliver(payload, http=http)
    before = native.replay()
    pending = deepcopy(before["customStatus"]["pending_requests"])
    for _ in range(2):
        state = native.replay()
        assert not state["isDone"] and state["customStatus"]["pending_requests"] == pending
        assert native.seen == [] and _DECODER_CALLS == [] and import_attempts == []
        action = [a for a in _atomic_actions(state["actions"]) if a["actionType"] == 0][-1]
        assert json.loads(json.loads(action["input"]))["message"]["response"] == payload
    accepted = native.complete(before)
    assert accepted["hitl_admission"] == {"request_id": "approval", "status": "accepted"}
    assert native.seen == [payload]
    for _ in range(2):
        final = native.replay()
        assert final["isDone"] and final["output"] == [{"value": payload}]
        assert not final["customStatus"].get("pending_requests")
        assert native.seen == [payload] and len(native.results) == 2
        assert _DECODER_CALLS == [] and import_attempts == []


@pytest.mark.parametrize("http", [False, True], ids=["native", "http"])
def test_only_declared_model_is_constructed_in_registered_response_activity(
    http: bool, import_attempts: list[str]
) -> None:
    native = _NativeReplies(_CountedDecision)
    native.complete(native.replay())
    for _ in range(2):
        native.deliver({"count": "bad", "extra": _metadata(_UNLOADED_MODULE)}, http=http)
        waiting = native.replay()
        pending = deepcopy(waiting["customStatus"]["pending_requests"])
        assert _VALIDATOR_CALLS == _DECODER_CALLS == [] and import_attempts == []
        assert native.replay() == waiting
        rejected = native.complete(waiting)
        assert rejected["hitl_admission"] == {"request_id": "approval", "status": "invalidreply"}
        for _ in range(2):
            cold = native.replay()
            assert not cold["isDone"] and cold["customStatus"]["pending_requests"] == pending
            assert not native.seen and _VALIDATOR_CALLS == _DECODER_CALLS == [] and import_attempts == []
    native.deliver({"count": 7, "extra": _metadata()}, http=http)
    ready = native.replay()
    assert native.replay() == ready and _VALIDATOR_CALLS == _DECODER_CALLS == []
    assert native.complete(ready)["hitl_admission"]["status"] == "accepted"
    assert _VALIDATOR_CALLS == [7] and len(native.seen) == 1 and native.seen[0].count == 8
    for _ in range(3):
        assert native.replay()["isDone"]
        assert _VALIDATOR_CALLS == [7] and len(native.seen) == 1
        assert _DECODER_CALLS == [] and import_attempts == []


@pytest.mark.parametrize(
    "value",
    [
        None,
        False,
        0,
        1.25,
        "",
        "123",
        '{"count":7}',
        ["{}"],
        [],
        {},
        {"request_id": "business-id", "response": {"count": 7}},
    ],
)
def test_native_reply_projection_preserves_json_types_and_json_looking_strings(value: Any) -> None:
    native = _NativeReplies(Any)
    native.deliver(value, http=False)
    native.complete(native.replay())
    native.complete(native.replay())
    for _ in range(2):
        result = native.replay()
        assert result["isDone"] and result["output"] == [{"value": value}]
        assert native.seen == [value] and type(native.seen[0]) is type(value)


@pytest.mark.parametrize("early", [False, True])
def test_projection_is_once_per_event_and_keeps_fifo_task_any_and_other_history(
    early: bool, import_attempts: list[str]
) -> None:
    payload = {"items": [_metadata()], "unknown": "retain"}
    arrivals = [_event(15, 100 + index, Name="a", Input=json.dumps(payload)) for index in range(2)]
    sibling = _event(15, 200, Name="b", Input='"json string"')
    unused = _event(15, 300, Name="unused", Input=json.dumps(_metadata(_UNLOADED_MODULE)))
    ack = _event(5, TaskScheduledId=0, Result="null")
    rows = [*_prefix(), *([*arrivals, sibling, unused, ack] if early else [ack, *arrivals, sibling, unused])]

    def run(context: Any) -> Generator[Any, Any, Any]:
        adapter = AzureFunctionsWorkflowContext(context)
        yield adapter.prepare_activity_task("gate", "null")
        other = adapter.wait_for_external_event("b")
        first = adapter.wait_for_external_event("a")
        winner = yield adapter.task_any([first, other])
        assert winner is first and adapter.get_task_result(winner) == payload
        second = AzureFunctionsWorkflowContext(context).wait_for_external_event("a")
        assert (yield second) == payload
        assert (yield other) == "json string"
        # Neither the first nor second occurrence can satisfy this third wait.
        yield adapter.wait_for_external_event("a")
        pytest.fail("Reused a consumed event")

    for _ in range(2):
        state, context = _execute(rows, run)
        assert not state["isDone"]
        assert _DECODER_CALLS == [] and import_attempts == []
        for source, event in zip(rows, context.histories):
            if source.get("Name") == "a":
                assert json.loads(event.Input) == [source["Input"]]
            else:
                assert event.Input == source.get("Input")
        assert [a["externalEventName"] for a in _atomic_actions(state["actions"]) if a["actionType"] == 6] == [
            "a",
            "b",
            "a",
            "a",
        ]


def test_invalid_json_after_object_prefix_cannot_run_sdk_hook(import_attempts: list[str]) -> None:
    wire = json.dumps(_metadata()) + " trailing-invalid-json"
    rows = [*_prefix()[:2], _event(15, 100, Name="a", Input=wire)]

    def run(context: Any) -> Generator[Any, Any, Any]:
        yield AzureFunctionsWorkflowContext(context).wait_for_external_event("a")

    with pytest.raises(json.JSONDecodeError):
        _execute(rows, run)
    assert _DECODER_CALLS == [] and import_attempts == []


def test_non_framework_native_wait_keeps_sdk_semantics(import_attempts: list[str]) -> None:
    rows = [*_prefix()[:2], _event(15, 100, Name="a", Input=json.dumps(_metadata()))]

    def run(context: Any) -> Generator[Any, Any, Any]:
        return (yield context.wait_for_external_event("a"))  # noqa: B901 - Durable orchestrator result.

    state, _ = _execute(rows, run)
    assert state["isDone"] and state["output"] == {"unexpected_constructor": {"value": 7}}
    assert _DECODER_CALLS == [{"value": 7}]


def test_real_agent_entity_correlation_replies_remain_native_and_approvals_work(import_attempts: list[str]) -> None:
    workflow, client, effects = _workflow()
    history = _History(True, workflow)
    first = history.complete_entity(history.replay())
    requests = load_agent_response(first).user_input_requests
    assert len(requests) == 2
    assert len(client.calls) == len(history.entity_inputs) == 1 and effects == []
    correlation = deepcopy([row for row in history.rows if row.get("Name", "").startswith("entity-call-")])
    assert len(correlation) == 1
    for index, request in enumerate(requests):
        assert isinstance(request.id, str)
        history.event(request.id, request.to_function_approval_response(True).to_dict(), 500 + index)
    ready = history.replay()
    assert len(client.calls) == 1 and effects == []
    history.complete_entity(ready)
    for _ in range(2):
        assert history.replay()["isDone"]
        assert len(client.calls) == len(history.entity_inputs) == 2 and sorted(effects) == ["a", "b"]
        assert [row for row in history.rows if row.get("Name") == "entity-call-0"] == correlation
        assert _DECODER_CALLS == [] and import_attempts == []


@pytest.mark.parametrize("http", [False, True], ids=["native", "http"])
@pytest.mark.parametrize("shape", ["outer", "outer-nested", "result", "result-nested"])
@pytest.mark.parametrize("unknown", [False, True], ids=["counter", "unloaded-module"])
def test_public_reply_at_active_entity_correlation_cannot_construct_sdk_objects(
    http: bool, shape: str, unknown: bool, import_attempts: list[str]
) -> None:
    workflow, client, effects = _workflow()
    history = _History(True, workflow)
    initial = history.replay()
    assert [action["actionType"] for action in initial["scheduled"]] == [7]
    assert client.calls == history.entity_inputs == effects == []
    # This is the service-assigned correlation of the actually open entity task,
    # not a pending human request or the application's RunRequest.correlationId.
    correlation = "active-entity-correlation"
    history.rows.append(_event(14, 0, Name="op", Input=json.dumps({"id": correlation})))
    metadata = _metadata(_UNLOADED_MODULE if unknown else __name__)
    raw_response: dict[str, Any] = {"type": "agent_response", "messages": [], "value": {"keep": [False, None]}}
    envelope: dict[str, Any] = {"result": ""}
    if shape == "outer":
        envelope.update(metadata)
    elif shape == "outer-nested":
        envelope["unknown_business_field"] = [metadata]
    elif shape == "result":
        raw_response.update(metadata)
    else:
        raw_response["value"] = {"keep": [False, None, metadata]}
    envelope["result"] = json.dumps(raw_response)

    app = AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")
    respond: Any = None
    for function in app.get_functions():
        trigger = function.get_trigger()
        if trigger is not None and "/respond/" in trigger.get_dict_repr().get("route", ""):
            respond = cast(Any, function.get_user_function()).client_function
    assert respond is not None
    transport: Any = df.DurableOrchestrationClient(
        json.dumps({
            "taskHubName": "security-test",
            "creationUrls": {},
            "managementUrls": {},
            "rpcBaseUrl": "https://example.test/",
        })
    )
    transport.get_status = AsyncMock(
        return_value=SimpleNamespace(
            name="dafx-agent-backlog", runtime_status=df.OrchestrationRuntimeStatus.Running, custom_status=None
        )
    )
    posts: list[str] = []

    async def post(url: str, body: str, *args: Any, **kwargs: Any) -> list[Any]:
        assert f"/instances/root/raiseEvent/{correlation}" in url
        posts.append(body)
        history.rows.append(_event(15, 101, Name=correlation, Input=body))
        return [202, None]

    transport._post_async_request = post
    if http:
        delivered = asyncio.run(respond(_request("respond", correlation, envelope), transport))
        assert delivered.status_code == 200
    else:
        asyncio.run(transport.raise_event("root", correlation, envelope))
    assert len(posts) == 1 and json.loads(posts[0]) == envelope
    original_rows = deepcopy(history.rows)
    for _ in range(2):
        # Exact registered wrapper uses context.from_json and native replay.
        state = json.loads(
            history.functions["dafx-agent-backlog"](
                json.dumps({
                    "history": history.rows,
                    "instanceId": "root",
                    "isReplaying": True,
                    "parentInstanceId": None,
                    "input": json.dumps(wrap_workflow_input("go")),
                    "upperSchemaVersion": ReplaySchema.V3.value,
                })
            )
        )
        assert state["isDone"]
        outputs = deserialize_workflow_output(state["output"])
        assert len(outputs) == 1 and isinstance(outputs[0], AgentResponse)
        assert outputs[0].value == raw_response["value"]
        assert _DECODER_CALLS == [] and import_attempts == []
        assert client.calls == history.entity_inputs == effects == []
        assert history.rows == original_rows
        actions = _atomic_actions(state["actions"])
        assert [action["actionType"] for action in actions] == [7]
        assert actions[0]["instanceId"] == initial["scheduled"][0]["instanceId"]
        assert actions[0]["operation"] == "run"
        before_input = json.loads(initial["scheduled"][0]["input"])
        after_input = json.loads(actions[0]["input"])
        # The existing RunRequest producer uses wall-clock created_at. Every
        # other scheduled field must match, including its deterministic ID.
        assert isinstance(before_input.pop("created_at"), str)
        assert isinstance(after_input.pop("created_at"), str)
        assert after_input == before_input


@pytest.mark.parametrize("is_error", [False, True])
@pytest.mark.parametrize("kind", ["value", "timeout"])
def test_entity_envelope_keeps_native_errors_and_json_business_data(
    is_error: bool, kind: str, import_attempts: list[str]
) -> None:
    value: Any = {
        "type": "agent_response",
        "messages": [],
        "value": {"items": [_metadata(), _metadata(_UNLOADED_MODULE)], "empty": [], "null": None},
    }
    result = json.dumps(value)
    if kind == "timeout":
        value = "Timeout value of 00:01:00 exceeded"
        result = value  # Host timeout results are deliberately not JSON-encoded.
    envelope: dict[str, Any] = {"result": result, "ignored": [_metadata(_UNLOADED_MODULE)], **_metadata()}
    if is_error:
        envelope["exceptionType"] = _metadata(_UNLOADED_MODULE)
    # isError/id are not the SDK's failure predicate. Preserve exceptionType
    # presence and the native timeout special case rather than inventing one.
    envelope.update(isError=not is_error, id={"nested": _metadata()})
    rows = [
        *_prefix()[:2],
        _event(14, 0, Name="op", Input=json.dumps({"id": "entity-correlation"})),
        _event(15, 101, Name="entity-correlation", Input=json.dumps(envelope)),
    ]
    should_fail = is_error or kind == "timeout"

    def run(context: Any) -> Generator[Any, Any, Any]:
        adapter = AzureFunctionsWorkflowContext(context)
        agent_task = adapter.prepare_agent_task("agent", "go", context.instance_id)
        # Observe the real wrapped SDK child, not a substituted decoder/task.
        child = agent_task.children[0]
        assert child.action_repr is agent_task.action_repr
        assert child._api_name == "CallEntityAction"
        try:
            response = yield agent_task
        except Exception as error:
            assert should_fail and error.args == (value,)
            assert child.state is TaskState.FAILED and child.result is error
            return {"error": error.args[0]}  # noqa: B901 - Durable orchestrator result.
        assert not should_fail
        assert child.state is TaskState.SUCCEEDED and child.result == value
        assert isinstance(response, AgentResponse) and response.value == value["value"]
        return response.value  # noqa: B901 - Durable orchestrator result.

    for _ in range(2):
        state, context = _execute(rows, run)
        assert state["isDone"] and state["output"] == ({"error": value} if should_fail else value["value"])
        assert [action["actionType"] for action in _atomic_actions(state["actions"])] == [7]
        assert context.histories[2].Input == rows[2]["Input"]
        assert _DECODER_CALLS == [] and import_attempts == []


@pytest.mark.parametrize("layer", ["outer", "result"])
def test_malformed_entity_json_does_not_construct_before_parse_failure(layer: str, import_attempts: list[str]) -> None:
    malformed = json.dumps(_metadata()) + " trailing-invalid-json"
    wire = malformed if layer == "outer" else json.dumps({"result": malformed})
    rows = [
        *_prefix()[:2],
        _event(14, 0, Name="op", Input='{"id":"entity-correlation"}'),
        _event(15, 101, Name="entity-correlation", Input=wire),
    ]

    def run(context: Any) -> Generator[Any, Any, Any]:
        yield AzureFunctionsWorkflowContext(context).prepare_agent_task("agent", "go", context.instance_id)

    with pytest.raises(json.JSONDecodeError):
        _execute(rows, run)
    assert _DECODER_CALLS == [] and import_attempts == []


@pytest.mark.parametrize("shape", ["missing-result", "null-result", "object-result"])
def test_invalid_entity_envelope_fails_without_running_outer_metadata(shape: str, import_attempts: list[str]) -> None:
    envelope = _metadata()
    if shape != "missing-result":
        envelope["result"] = None if shape == "null-result" else _metadata(_UNLOADED_MODULE)
    rows = [
        *_prefix()[:2],
        _event(14, 0, Name="op", Input='{"id":"invalid-correlation"}'),
        _event(15, 101, Name="invalid-correlation", Input=json.dumps(envelope)),
    ]

    def run(context: Any) -> Generator[Any, Any, Any]:
        yield AzureFunctionsWorkflowContext(context).prepare_agent_task("agent", "go", context.instance_id)

    expected_error = KeyError if shape == "missing-result" else AttributeError
    with pytest.raises(expected_error):
        _execute(rows, run)
    assert _DECODER_CALLS == [] and import_attempts == []


@pytest.mark.parametrize("event_first", [False, True])
def test_entity_correlation_and_public_wait_preserve_original_json(
    event_first: bool, import_attempts: list[str]
) -> None:
    payload = {"result": json.dumps({"messages": [], "value": [1, False]}), **_metadata()}
    correlation = "shared-unqualified-name"
    # Two distinct occurrences have identical bytes. The task kind, not a
    # reserved public name or payload tag, chooses how each one is restored.
    sent = _event(14, 0, Name="op", Input=json.dumps({"id": correlation}))
    first = _event(15, 100, Name=correlation, Input=json.dumps(payload))
    second = _event(15, 101, Name=correlation, Input=json.dumps(payload))
    rows = [*_prefix()[:2], *([first, sent] if event_first else [sent, first]), second]

    def run(context: Any) -> Generator[Any, Any, Any]:
        adapter = AzureFunctionsWorkflowContext(context)
        if event_first:
            assert (yield adapter.wait_for_external_event(correlation)) == payload
        agent_task = adapter.prepare_agent_task("agent", "go", context.instance_id)
        response = yield agent_task
        assert response.value == [1, False]
        if not event_first:
            # Construct another adapter to prove registry/projection idempotence.
            assert (yield AzureFunctionsWorkflowContext(context).wait_for_external_event(correlation)) == payload
        return "done"  # noqa: B901 - Durable orchestrator result.

    for _ in range(2):
        state, _ = _execute(rows, run)
        assert state["isDone"] and state["output"] == "done"
        expected_actions = [6, 7] if event_first else [7, 6]
        assert [action["actionType"] for action in _atomic_actions(state["actions"])] == expected_actions
        assert _DECODER_CALLS == [] and import_attempts == []


def test_non_framework_entity_in_adapted_context_keeps_native_decoder(import_attempts: list[str]) -> None:
    rows = [
        *_prefix()[:2],
        _event(14, 0, Name="op", Input='{"id":"native-correlation"}'),
        _event(15, 101, Name="native-correlation", Input=json.dumps({"result": '"native"', "ignored": _metadata()})),
    ]

    def run(context: Any) -> Generator[Any, Any, Any]:
        AzureFunctionsWorkflowContext(context)
        return (yield context.call_entity(df.EntityId("native", "root"), "run"))  # noqa: B901

    state, context = _execute(rows, run)
    assert state["isDone"] and state["output"] == "native"
    assert _DECODER_CALLS == [{"value": 7}]
    assert context.histories[-1].Input == rows[-1]["Input"]


def test_native_wait_reusing_workflow_entity_correlation_gets_original_input(import_attempts: list[str]) -> None:
    correlation = "reused-native-name"
    envelope = {"result": json.dumps({"messages": [], "value": 7}), "ignored": _metadata()}
    rows = [
        *_prefix()[:2],
        _event(14, 0, Name="op", Input=json.dumps({"id": correlation})),
        _event(15, 100, Name=correlation, Input=json.dumps(envelope)),
        _event(15, 101, Name=correlation, Input=json.dumps(envelope)),
    ]

    def run(context: Any) -> Generator[Any, Any, Any]:
        adapter = AzureFunctionsWorkflowContext(context)
        response = yield adapter.prepare_agent_task("agent", "go", context.instance_id)
        assert response.value == 7 and _DECODER_CALLS == [] and import_attempts == []
        native = yield context.wait_for_external_event(correlation)
        assert native == {"result": envelope["result"], "ignored": {"unexpected_constructor": {"value": 7}}}
        return "done"  # noqa: B901 - Durable orchestrator result.

    state, context = _execute(rows, run)
    assert state["isDone"] and state["output"] == "done"
    assert _DECODER_CALLS == [{"value": 7}] and import_attempts == [__name__]
    assert [event.Input for event in context.histories[-2:]] == [row["Input"] for row in rows[-2:]]


def test_workflow_entity_calls_reject_unknown_task_registry_without_scheduling() -> None:
    def run(context: Any) -> Generator[Any, Any, Any]:
        context.open_tasks = dict(context.open_tasks)
        adapter = AzureFunctionsWorkflowContext(context)
        with pytest.raises(RuntimeError, match="Unsupported Durable Functions workflow entity task registry"):
            adapter.prepare_agent_task("agent", "go", context.instance_id)
        yield adapter.prepare_activity_task("still-native", "null")

    state, _ = _execute(_prefix()[:2], run)
    assert not state["isDone"]
    assert [action["actionType"] for action in _atomic_actions(state["actions"])] == [0]


def test_json_entity_decode_preserves_trusted_response_format(import_attempts: list[str]) -> None:
    raw = AgentResponse(
        messages=[Message("assistant", ['{"count":7}'])],
        additional_properties={"business": [_metadata(), _metadata(_UNLOADED_MODULE)]},
    ).to_dict()
    rows = [
        *_prefix()[:2],
        _event(14, 0, Name="op", Input='{"id":"typed-correlation"}'),
        _event(15, 101, Name="typed-correlation", Input=json.dumps({"result": json.dumps(raw)})),
    ]

    def run(context: Any) -> Generator[Any, Any, Any]:
        AzureFunctionsWorkflowContext(context)
        executor = AzureFunctionsAgentExecutor(cast(Any, _JsonEntityContext(context)))
        request = RunRequest(message="go", correlation_id="trusted", response_format=_CountedDecision)
        response = yield executor.run_durable_agent("agent", request)
        assert isinstance(response, AgentResponse) and isinstance(response.value, _CountedDecision)
        assert response.value.count == 8
        assert response.additional_properties == raw["additional_properties"]
        return response.value.count  # noqa: B901 - Durable orchestrator result.

    state, _ = _execute(rows, run)
    assert state["isDone"] and state["output"] == 8
    assert _VALIDATOR_CALLS == [7] and _DECODER_CALLS == [] and import_attempts == []


def test_entity_proxy_preserves_signal_action_and_native_sequence() -> None:
    def run(context: Any) -> Generator[Any, Any, Any]:
        adapter = AzureFunctionsWorkflowContext(context)
        registry = context.open_tasks
        AzureFunctionsWorkflowContext(context)
        assert context.open_tasks is registry
        proxy = _JsonEntityContext(context)
        proxy.signal_entity(df.EntityId("native", "root"), "signal", {"keep": [False, None]})
        task = adapter.prepare_activity_task("after-signal", "null")
        yield task
        assert task.id == 1  # SignalEntityAction consumes the native sequence slot.
        return "done"  # noqa: B901 - Durable orchestrator result.

    rows = [*_prefix()[:2], _event(5, TaskScheduledId=1, Result='"ack"')]
    state, _ = _execute(rows, run)
    actions = _atomic_actions(state["actions"])
    assert state["isDone"] and state["output"] == "done"
    assert [action["actionType"] for action in actions] == [9, 0]
    assert actions[0]["operation"] == "signal" and json.loads(actions[0]["input"]) == {"keep": [False, None]}


def test_standalone_dt_reply_preserves_metadata_without_sdk_construction(import_attempts: list[str]) -> None:
    payload = {"items": [_metadata(), _metadata(_UNLOADED_MODULE)], "business": "keep"}
    workflow, seen = _generic_workflow(dict)
    episodes = _Episodes(workflow)
    episodes.client.start_workflow("go", instance_id="root")
    episodes.reply("approval", payload)
    _complete_generic_activity(episodes)
    episodes.cold("root")
    assert not seen and _DECODER_CALLS == [] and import_attempts == []
    _complete_generic_activity(episodes)
    for _ in range(2):
        cold = _replay(episodes.worker, "root", episodes.histories["root"])
        assert len(cold.actions) == 1 and cold.actions[0].HasField("completeOrchestration")
        assert json.loads(cold.actions[0].completeOrchestration.result.value) == [{"value": payload}]
        assert seen == [payload] and _DECODER_CALLS == [] and import_attempts == []
