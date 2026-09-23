# Copyright (c) Microsoft. All rights reserved.

"""HTTP reply bytes through native SDK replay and registered response activities.

The native raise_event method and, in the unadapted control, its HTTP helper are
real. Route calls replace only aiohttp sessions and status lookup. A separate
native call records transport arguments to establish optional header support.
The session double uses aiohttp's payload encoders and treats json=None as an absent JSON body.
Captured bodies become constructed service histories, not live host captures.
"""

import asyncio
import json
from copy import deepcopy
from inspect import signature
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import aiohttp
import azure.durable_functions as df
import pytest
from agent_framework_durabletask import wrap_workflow_input
from agent_framework_durabletask._workflows.serialization import deserialize_workflow_output
from aiohttp.payload import BytesPayload, JsonPayload
from azure.durable_functions.models.ReplaySchema import ReplaySchema
from azure.durable_functions.models.utils import http_utils
from pydantic import BaseModel, field_validator
from test_workflow_buffered_events_review_af import _event
from test_workflow_generic_hitl_review import _generic_workflow
from test_workflow_generic_hitl_review_af import _request
from test_workflow_mixed_hitl_review import _atomic_actions

from agent_framework_azurefunctions import AgentFunctionApp

_VALIDATIONS: list[bool] = []


class _Approval(BaseModel):
    approved: bool

    @field_validator("approved")
    @classmethod
    def observe(cls, value: bool) -> bool:
        _VALIDATIONS.append(value)
        return value


def _client() -> Any:
    return df.DurableOrchestrationClient(
        json.dumps({
            "taskHubName": "http-roundtrip",
            "creationUrls": {},
            "managementUrls": {},
            "rpcBaseUrl": "https://example.test/",
        })
    )


class _Writer:
    def __init__(self) -> None:
        self.chunks: list[bytes] = []

    async def write(self, chunk: bytes) -> None:
        self.chunks.append(chunk)


class _Exchange:
    def __init__(self, network: "_Network", url: str, payload: Any, headers: dict[str, str]) -> None:
        self.network = network
        self.url = url
        self.payload = payload
        self.headers = headers
        self.status = network.status

    async def __aenter__(self) -> "_Exchange":
        writer = _Writer()
        if self.payload is not None:
            await self.payload.write(writer)
        body = b"".join(writer.chunks).decode("utf-8")
        self.network.requests.append({"url": self.url, "body": body, "headers": self.headers})
        # Do not let the double hide aiohttp's json=None behavior. The Functions
        # raise-event endpoint requires application/json before it reads a value.
        if self.headers.get("Content-Type") != "application/json":
            self.status = 400
        else:
            json.loads(body)
        if self.network.barrier is not None:
            if len(self.network.requests) == self.network.participants:
                self.network.barrier.set()
            await self.network.barrier.wait()
        if self.network.failure is not None:
            raise self.network.failure
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass

    async def json(self, *, content_type: Any = None) -> None:
        assert content_type is None


class _Session:
    def __init__(self, network: "_Network") -> None:
        self.network = network
        self.closed = False

    async def __aenter__(self) -> "_Session":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def close(self) -> None:
        self.closed = True

    def post(self, url: str, *, data: Any = None, json: Any = None, headers: dict[str, str] | None = None) -> _Exchange:
        assert not self.closed
        assert data is None or json is None
        # Mirror ClientSession._request's choice, then use the actual aiohttp
        # payload writer. In particular, json=None is NOT JsonPayload(None).
        payload = JsonPayload(json) if json is not None else BytesPayload(data) if data is not None else None
        actual_headers = dict(payload.headers) if payload is not None else {}
        actual_headers.update(headers or {})
        return _Exchange(self.network, url, payload, actual_headers)


class _Network:
    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []
        self.sessions: list[_Session] = []
        self.status = 202
        self.failure: BaseException | None = None
        self.barrier: asyncio.Event | None = None
        self.participants = 0

    def session(self, *args: Any, **kwargs: Any) -> _Session:
        session = _Session(self)
        self.sessions.append(session)
        return session


@pytest.fixture
def network(monkeypatch: pytest.MonkeyPatch) -> _Network:
    network = _Network()
    monkeypatch.setattr(aiohttp, "ClientSession", network.session)
    # Newer SDK helpers pool sessions, older ones create a session per call.
    # Leave the real helper and its session acquisition logic in either case.
    monkeypatch.setattr(http_utils, "_client_session", None, raising=False)
    _VALIDATIONS.clear()
    return network


class _HttpWorkflow:
    def __init__(self, requested: Any = Any) -> None:
        workflow, self.seen = _generic_workflow(requested)
        app = AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")
        self.functions: dict[str, Any] = {}
        self.respond: Any = None
        for function in app.get_functions():
            name = function.get_function_name()
            assert name is not None
            self.functions[name] = function.get_user_function()
            trigger = function.get_trigger()
            if trigger is not None and "/respond/" in trigger.get_dict_repr().get("route", ""):
                self.respond = cast(Any, function.get_user_function()).client_function
        assert self.respond is not None
        self.client = _client()
        self.client.get_status = AsyncMock(
            return_value=SimpleNamespace(
                name="dafx-generic-hitl", runtime_status=df.OrchestrationRuntimeStatus.Running, custom_status=None
            )
        )
        self.start = json.dumps(wrap_workflow_input("go"))
        self.rows = [_event(12), _event(0, Name="dafx-generic-hitl", Input=self.start)]
        self.results: list[dict[str, Any]] = []

    def replay(self) -> dict[str, Any]:
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
        actions = [a for a in _atomic_actions(state["actions"]) if a["actionType"] == 0]
        assert len(actions) == len(self.results) + 1
        action = actions[-1]
        assert action["functionName"] == "dafx-generic-hitl-gate"
        result = self.functions[action["functionName"]](json.loads(action["input"]))
        task_id = len(self.results)
        self.results.append(json.loads(result))
        self.rows.extend([
            _event(4, task_id, Name=action["functionName"], Input=action["input"]),
            _event(5, TaskScheduledId=task_id, Result=json.dumps(result)),
        ])
        return self.results[-1]

    def deliver(self, network: _Network, value: Any) -> None:
        original = dict(vars(self.client))
        response = asyncio.run(self.respond(_request("respond", payload=value), self.client))
        assert response.status_code == 200
        assert vars(self.client).keys() == original.keys()
        assert all(vars(self.client)[key] is old for key, old in original.items())
        post = network.requests[-1]
        assert post["url"] == "https://example.test/instances/root/raiseEvent/approval"
        assert post["headers"]["Content-Type"] == "application/json"
        self.rows.append(_event(15, 100 + len(network.requests), Name="approval", Input=post["body"]))


_JSON_VALUES = [
    pytest.param({"approved": True}, id="object"),
    pytest.param([1, False, None, {"approved": True}], id="array"),
    pytest.param(True, id="true"),
    pytest.param(False, id="false"),
    pytest.param(0, id="zero"),
    pytest.param(1.25, id="float"),
    pytest.param(None, id="null"),
    pytest.param({}, id="empty-object"),
    pytest.param([], id="empty-array"),
    pytest.param("", id="empty-string"),
    pytest.param("123", id="numeric-string"),
    pytest.param("null", id="null-string"),
    pytest.param("true", id="boolean-string"),
    pytest.param('{"approved": true}', id="object-string"),
    pytest.param("[1, false, null]", id="array-string"),
    pytest.param('"quoted"\n\u00e9', id="escaped-unicode-string"),
    pytest.param({"request_id": "business", "response": None}, id="business-envelope"),
]


@pytest.mark.parametrize("early", [False, True], ids=["waiting", "buffered"])
@pytest.mark.parametrize("value", _JSON_VALUES)
def test_http_body_preserves_json_value_through_native_replay_and_activity(
    network: _Network, value: Any, early: bool
) -> None:
    native = _HttpWorkflow()
    if early:
        native.deliver(network, value)
    native.complete(native.replay())
    if not early:
        waiting = native.replay()
        assert not waiting["isDone"] and "approval" in waiting["customStatus"]["pending_requests"]
        native.deliver(network, value)
    ready = native.replay()
    assert native.replay() == ready and native.seen == []
    result = native.complete(ready)
    # Before the producer fix, the SDK HTTP body carries serialized text, not
    # the original value. Do not parse it again in the oracle.
    assert native.seen == [value] and type(native.seen[0]) is type(value)
    assert result["hitl_admission"] == {"request_id": "approval", "status": "accepted"}
    wire_value = json.loads(network.requests[-1]["body"])
    assert wire_value == value and type(wire_value) is type(value)
    for _ in range(2):
        final = native.replay()
        assert final["isDone"] and final["output"] == [{"value": value}]
        assert not final["customStatus"].get("pending_requests")
        assert native.seen == [value] and len(native.results) == 2
    assert all(session.closed for session in network.sessions)


def test_http_pydantic_approval_is_admitted_only_in_the_recorded_activity(network: _Network) -> None:
    native = _HttpWorkflow(_Approval)
    native.complete(native.replay())
    waiting = native.replay()
    pending = deepcopy(waiting["customStatus"]["pending_requests"])
    native.deliver(network, {"approved": True})
    ready = native.replay()
    assert native.replay() == ready and native.seen == [] and _VALIDATIONS == []
    assert ready["customStatus"]["pending_requests"] == pending
    result = native.complete(ready)
    assert result["hitl_admission"] == {"request_id": "approval", "status": "accepted"}
    assert len(native.seen) == 1 and isinstance(native.seen[0], _Approval)
    assert native.seen[0].approved is True and _VALIDATIONS == [True]
    assert network.requests[-1]["body"] == '{"approved": true}'
    for _ in range(2):
        final = native.replay()
        assert final["isDone"] and not final["customStatus"].get("pending_requests")
        output = deserialize_workflow_output(final["output"])
        assert output[0]["value"].approved is True
        assert len(native.seen) == 1 and _VALIDATIONS == [True] and len(native.results) == 2


@pytest.mark.parametrize(
    ("requested", "bad", "good"), [(dict, '{"approved": true}', {"approved": True}), (int, "123", 123)]
)
def test_json_looking_strings_still_reject_then_accept_a_typed_correction(
    network: _Network, requested: type, bad: str, good: Any
) -> None:
    native = _HttpWorkflow(requested)
    native.complete(native.replay())
    pending = deepcopy(native.replay()["customStatus"]["pending_requests"])
    native.deliver(network, bad)
    result = native.complete(native.replay())
    assert result["hitl_admission"] == {"request_id": "approval", "status": "invalidreply"}
    assert native.replay()["customStatus"]["pending_requests"] == pending and native.seen == []
    assert json.loads(network.requests[-1]["body"]) == bad
    native.deliver(network, good)
    result = native.complete(native.replay())
    assert result["hitl_admission"]["status"] == "accepted"
    for _ in range(2):
        final = native.replay()
        assert final["isDone"] and final["output"] == [{"value": good}]
        assert native.seen == [good] and type(native.seen[0]) is type(good)
        assert len(native.results) == 3


@pytest.mark.parametrize("value", [{"approved": True}, None, "123"])
def test_unadapted_native_sdk_and_real_http_helper_characterize_double_encoding(network: _Network, value: Any) -> None:
    client = _client()
    assert client._post_async_request is http_utils.post_async_request
    asyncio.run(client.raise_event("root", "approval", value))
    wire = network.requests[-1]["body"]
    assert json.loads(wire) == json.dumps(value)
    assert isinstance(json.loads(wire), str)
    assert client._post_async_request is http_utils.post_async_request


def test_unadapted_sdk_body_records_invalid_model_reply_before_route_correction(network: _Network) -> None:
    native = _HttpWorkflow(_Approval)
    native.complete(native.replay())
    pending = deepcopy(native.replay()["customStatus"]["pending_requests"])
    asyncio.run(native.client.raise_event("root", "approval", {"approved": True}))
    wire = network.requests[-1]["body"]
    assert json.loads(wire) == '{"approved": true}'
    native.rows.append(_event(15, 101, Name="approval", Input=wire))
    rejected = native.complete(native.replay())
    assert rejected["hitl_admission"] == {"request_id": "approval", "status": "invalidreply"}
    assert native.replay()["customStatus"]["pending_requests"] == pending
    assert native.seen == [] and _VALIDATIONS == []
    native.deliver(network, {"approved": True})
    accepted = native.complete(native.replay())
    assert accepted["hitl_admission"] == {"request_id": "approval", "status": "accepted"}
    for _ in range(2):
        final = native.replay()
        assert final["isDone"] and not final["customStatus"].get("pending_requests")
        assert len(native.seen) == 1 and native.seen[0].approved is True and _VALIDATIONS == [True]
        assert len(native.results) == 3


def test_raw_sender_uses_the_sanitized_value_not_the_original_http_bytes(network: _Network) -> None:
    native = _HttpWorkflow(dict)
    native.complete(native.replay())
    native.deliver(network, {"items": [{"__type__": "untrusted"}], "approved": True})
    expected = {"items": [None], "approved": True}
    assert json.loads(network.requests[-1]["body"]) == expected
    assert native.complete(native.replay())["hitl_admission"]["status"] == "accepted"
    assert native.replay()["isDone"] and native.seen == [expected]


def test_real_sdk_helper_cannot_send_explicit_null_by_passing_none(network: _Network) -> None:
    url = "https://example.test/instances/root/raiseEvent/approval"
    response = asyncio.run(http_utils.post_async_request(url, None))
    assert response[0] == 400
    assert network.requests[-1]["body"] == ""
    assert network.requests[-1]["headers"].get("Content-Type") != "application/json"


@pytest.mark.parametrize(
    ("status", "message"),
    [
        (400, "Only application/json request content is supported"),
        (404, "No instance with ID root found."),
        (410, "Instance with ID root is gone: either completed or failed"),
        (503, "Webhook returned unrecognized status code 503"),
    ],
)
def test_native_raise_event_status_errors_are_preserved(network: _Network, status: int, message: str) -> None:
    native = _HttpWorkflow()
    network.status = status
    with pytest.raises(Exception) as error:
        asyncio.run(native.respond(_request("respond", payload=None), native.client))
    assert str(error.value) == message
    assert native.client._post_async_request is http_utils.post_async_request
    assert all(session.closed for session in network.sessions)
    network.status = 202
    native.deliver(network, None)
    assert network.requests[-1]["body"] == "null"


@pytest.mark.parametrize("cancel", [False, True], ids=["connection-error", "cancel"])
def test_transport_failure_or_cancellation_does_not_modify_shared_client(network: _Network, cancel: bool) -> None:
    native = _HttpWorkflow()
    failure = asyncio.CancelledError() if cancel else aiohttp.ClientConnectionError("offline")
    network.failure = failure
    with pytest.raises(type(failure)) as error:
        asyncio.run(native.respond(_request("respond", payload={"approved": True}), native.client))
    # Python 3.10 can recreate CancelledError across asyncio.run's task boundary.
    if not cancel:
        assert error.value is failure
    assert native.client._post_async_request is http_utils.post_async_request
    assert all(session.closed for session in network.sessions)
    network.failure = None
    native.deliver(network, {"approved": True})


def test_concurrent_route_and_native_calls_do_not_share_a_transport_override(network: _Network) -> None:
    native = _HttpWorkflow()

    async def submit() -> None:
        network.barrier = asyncio.Event()
        network.participants = 3
        first, second, _ = await asyncio.wait_for(
            asyncio.gather(
                native.respond(_request("respond", request_id="first", payload={"approved": True}), native.client),
                native.respond(_request("respond", request_id="second", payload="123"), native.client),
                native.client.raise_event("root", "direct", {"native": True}),
            ),
            timeout=5,
        )
        assert first.status_code == second.status_code == 200

    asyncio.run(submit())
    bodies = {post["url"].rsplit("/", 1)[-1]: post["body"] for post in network.requests}
    assert json.loads(bodies["first"]) == {"approved": True}
    assert json.loads(bodies["second"]) == "123"
    assert json.loads(bodies["direct"]) == json.dumps({"native": True})
    assert native.client._post_async_request is http_utils.post_async_request


def test_nested_route_uses_the_native_child_url_and_invocation_header(network: _Network) -> None:
    # Probe the installed native method's actual transport call. Older SDKs do
    # not forward an invocation ID even when the client has the attribute.
    control = _client()
    control._function_invocation_id = "test-invocation"
    transport = AsyncMock(return_value=[202, None])
    control._post_async_request = transport
    asyncio.run(control.raise_event(instance_id="child", event_name="approval", event_data=0))
    transport.assert_awaited_once()
    native_call = transport.await_args
    assert native_call is not None
    forwarded = signature(http_utils.post_async_request).bind(*native_call.args, **native_call.kwargs).arguments
    assert forwarded["url"] == "https://example.test/instances/child/raiseEvent/approval"
    assert forwarded["data"] == "0"
    expected_headers = {"Content-Type": "application/json"}
    invocation_id = forwarded.get("function_invocation_id")
    assert invocation_id in (None, "test-invocation")
    if invocation_id:
        expected_headers["X-Azure-Functions-InvocationId"] = invocation_id

    native = _HttpWorkflow()
    native.client._function_invocation_id = "test-invocation"
    native.client.get_status.side_effect = lambda instance: SimpleNamespace(
        name="dafx-generic-hitl",
        runtime_status=df.OrchestrationRuntimeStatus.Running,
        custom_status={"subworkflows": {"sub": {"0": "child"}}} if instance == "root" else None,
    )
    response = asyncio.run(native.respond(_request("respond", request_id="sub~0~approval", payload=0), native.client))
    assert response.status_code == 200
    assert network.requests == [
        {
            "url": "https://example.test/instances/child/raiseEvent/approval",
            "body": "0",
            "headers": expected_headers,
        }
    ]
    assert native.client.raise_event.__func__ is df.DurableOrchestrationClient.raise_event
    assert native.client._post_async_request is http_utils.post_async_request


@pytest.mark.parametrize("override", ["method", "transport"])
def test_custom_client_contract_is_not_replaced(network: _Network, override: str) -> None:
    native = _HttpWorkflow()
    if override == "method":
        native.client.raise_event = AsyncMock()
    else:
        native.client._post_async_request = AsyncMock(return_value=[202, None])
    response = asyncio.run(native.respond(_request("respond", payload="123"), native.client))
    assert response.status_code == 200 and network.requests == []
    if override == "method":
        native.client.raise_event.assert_awaited_once_with(instance_id="root", event_name="approval", event_data="123")
    else:
        native.client._post_async_request.assert_awaited_once()
        assert native.client._post_async_request.await_args.args[:2] == (
            "https://example.test/instances/root/raiseEvent/approval",
            '"123"',
        )
