# Copyright (c) Microsoft. All rights reserved.

"""Exercise indexed Functions entities through the real SDK batch engine."""

import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from types import SimpleNamespace
from typing import Any, get_type_hints

import azure.durable_functions as df
import azure.functions as func
import pytest
from agent_framework import Agent, BaseChatClient, ChatResponse, Message
from agent_framework_durabletask import RunRequest

from agent_framework_azurefunctions import AgentFunctionApp

_CONSTRUCTIONS: list[Any] = []


class _BenignProbe:
    @classmethod
    def from_json(cls, value: Any) -> Any:
        _CONSTRUCTIONS.append(deepcopy(value))
        return {"constructed": value}


def _payload(marked: bool) -> dict[str, Any]:
    value: dict[str, Any] = {"keep": [None, False, 0, "雪"]}
    if marked:
        value.update(__module__=__name__, __class__="_BenignProbe", __data__={"value": 7})
    return value


@pytest.fixture(autouse=True)
def _reset_probe() -> None:
    _CONSTRUCTIONS.clear()


class _Client(BaseChatClient):
    def __init__(self) -> None:
        super().__init__()
        self.options: list[dict[str, Any]] = []
        self.messages: list[list[Message]] = []

    async def _inner_get_response(
        self, *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
    ) -> ChatResponse:
        assert not stream and messages
        self.options.append(deepcopy(dict(options)))
        self.messages.append(deepcopy(list(messages)))
        return ChatResponse(messages=[Message("assistant", ["done"])])


class _NonStreamingAgent(Agent):
    def run(self, *args: Any, **kwargs: Any) -> Any:
        if kwargs.get("stream"):
            raise TypeError("stream is not supported")
        return super().run(*args, **kwargs)


def _app(client: _Client) -> Any:
    return AgentFunctionApp(
        agents=[_NonStreamingAgent(client=client, name="json-agent")],
        enable_health_check=False,
        enable_http_endpoints=False,
    )


def _wire(operations: list[tuple[str, Any]], raw: str | None = None) -> str:
    return json.dumps({
        "self": {"name": "dafx-json-agent", "key": "key"},
        "exists": raw is not None,
        "state": raw,
        "batch": [{"name": name, "input": json.dumps(json.dumps(value))} for name, value in operations],
    })


def test_indexed_entity_worker_signature_keeps_native_input_unannotated() -> None:
    client = _Client()
    app = _app(client)

    @app.entity_trigger(context_name="context", entity_name="native-json")
    def native(context: df.DurableEntityContext) -> None:
        raise AssertionError("Indexing must not invoke the entity body")

    functions = {function.get_function_name(): function for function in app.get_functions()}
    generated = functions["dafx-json-agent"].get_user_function()
    native_handle = functions["native"].get_user_function()
    assert "context" not in get_type_hints(generated)
    assert "context" not in get_type_hints(native_handle)
    assert get_type_hints(generated)["return"] is get_type_hints(native_handle)["return"] is str
    for function in functions.values():
        bindings = function.get_bindings()
        assert len(bindings) == 1
        assert bindings[0].name == "context" and bindings[0].type == "entityTrigger"
    assert client.options == client.messages == []


def test_entity_trigger_binding_accepts_generated_worker_annotation() -> None:
    client = _Client()
    functions = {function.get_function_name(): function for function in _app(client).get_functions()}
    generated = functions["dafx-json-agent"].get_user_function()
    assert "context" not in get_type_hints(generated)
    # Exercise the public SDK converter the worker uses, not just its decorator
    # metadata. The external worker probe covers Registry.add_indexed_function.
    binding = func.get_binding_registry().get("entityTrigger")
    assert binding is not None
    assert binding.check_input_type_annotation(func.EntityContext)
    assert not binding.check_input_type_annotation(str)
    assert client.options == client.messages == []


@pytest.mark.parametrize("marked", [False, True], ids=["plain", "sdk-shaped"])
@pytest.mark.parametrize("body_wrapper", [False, True], ids=["string", "body"])
def test_indexed_agent_keeps_json_input_and_cold_state(marked: bool, body_wrapper: bool) -> None:
    client = _Client()
    app = _app(client)
    # Index exactly once. Native duplicate-name checks make repeated indexing an
    # invalid way to look up a function on some supported Functions SDK versions.
    functions = {function.get_function_name(): function for function in app.get_functions()}
    function = functions["dafx-json-agent"]
    handle: Any = function.get_user_function()
    assert handle.__name__ == handle.entity_function.__name__ == "dafx-json-agent"
    binding = function.get_bindings_dict()["bindings"][0]
    assert binding["type"] == "entityTrigger"
    assert binding["name"] == "context" and binding["entityName"] == "dafx-json-agent"

    value = _payload(marked)
    request = RunRequest("go", correlation_id="request", options={"metadata": value}).to_dict()
    incoming = _wire([("run", request)])
    batch = json.loads(handle(SimpleNamespace(body=incoming) if body_wrapper else incoming))
    assert len(batch["results"]) == 1 and not batch["results"][0]["isError"], batch
    assert json.loads(batch["results"][0]["result"])["messages"][0]["contents"][0]["text"] == "done"
    assert client.options[0]["metadata"] == value
    assert _CONSTRUCTIONS == []

    state = json.loads(batch["entityState"])
    state["futureRoot"] = value
    committed = json.dumps(state)
    # A no-write operation isolates native hydration/re-encoding from the
    # legacy writer's intentional projection of unknown fields.
    for _ in range(2):
        cold = json.loads(handle(_wire([("unknown", None)], committed)))
        assert not cold["results"][0]["isError"], cold
        assert json.loads(cold["entityState"]) == state
        assert _CONSTRUCTIONS == []
        committed = cold["entityState"]
    assert len(client.options) == 1


@pytest.mark.parametrize("marked", [False, True], ids=["plain", "sdk-shaped"])
def test_indexed_agent_reads_cold_state_before_model_execution(marked: bool) -> None:
    client = _Client()
    functions = {
        function.get_function_name(): function.get_user_function() for function in _app(client).get_functions()
    }
    value = _payload(marked)
    state = {
        "schemaVersion": "1.1.0",
        "data": {
            "conversationHistory": [
                {
                    "$type": "request",
                    "correlationId": "seed",
                    "createdAt": "2024-01-03T04:05:06+00:00",
                    "messages": [
                        {
                            "role": "user",
                            "contents": [{"$type": "text", "text": "previous"}],
                            "extensionData": {"opaque": value},
                        }
                    ],
                }
            ],
        },
    }
    request = RunRequest("next", correlation_id="next").to_dict()
    batch = json.loads(functions["dafx-json-agent"](_wire([("run", request)], json.dumps(state))))
    assert not batch["results"][0]["isError"], batch
    assert json.loads(batch["results"][0]["result"])["messages"][0]["contents"][0]["text"] == "done"
    assert len(client.messages) == 1
    assert client.messages[0][0].text == "previous"
    assert client.messages[0][0].additional_properties["opaque"] == value
    assert _CONSTRUCTIONS == []


@pytest.mark.parametrize("marked", [False, True], ids=["plain", "sdk-shaped"])
def test_unmarked_native_entity_keeps_sdk_custom_object_semantics(marked: bool) -> None:
    client = _Client()
    app = _app(client)

    @app.entity_trigger(context_name="context", entity_name="native-json")
    def native(context: df.DurableEntityContext) -> None:
        context.set_result({"input": context.get_input(), "state": context.get_state()})

    functions = {function.get_function_name(): function.get_user_function() for function in app.get_functions()}
    assert "dafx-json-agent" in functions
    value = _payload(marked)
    envelope = json.loads(_wire([("read", value)], json.dumps(value)))
    envelope["self"]["name"] = "native-json"
    batch = json.loads(functions["native"](json.dumps(envelope)))
    assert not batch["results"][0]["isError"], batch
    expected = {"constructed": {"value": 7}} if marked else value
    assert json.loads(batch["results"][0]["result"]) == {"input": expected, "state": expected}
    assert json.loads(batch["entityState"]) == expected
    expected_constructions = [{"value": 7}, {"value": 7}] if marked else []
    assert expected_constructions == _CONSTRUCTIONS
    assert client.options == []


def test_json_wrapper_preserves_sdk_batch_order_errors_and_live_state() -> None:
    from agent_framework_azurefunctions._entity_json import create_json_entity

    seen: list[tuple[str | None, Any, Any]] = []

    def entity(context: df.DurableEntityContext) -> None:
        value = context.get_input()
        seen.append((context.operation_name, value, deepcopy(context.get_state())))
        if context.operation_name == "fail":
            raise ValueError("operation failed")
        if context.operation_name == "write":
            context.set_state(value)
        elif context.operation_name == "delete":
            context.destruct_on_exit()
            return
        context.set_result(context.get_state())

    value = _payload(True)
    operations = [("write", value), ("fail", None), ("read", None), ("delete", None)]
    handle: Any = create_json_entity(entity)
    assert handle.entity_function is entity
    batch = json.loads(handle(_wire(operations)))
    assert seen == [("write", value, None), ("fail", None, value), ("read", None, value), ("delete", None, value)]
    assert [result["isError"] for result in batch["results"]] == [False, True, False, False]
    assert json.loads(batch["results"][0]["result"]) == value
    assert json.loads(batch["results"][1]["result"]) == "operation failed"
    assert json.loads(batch["results"][2]["result"]) == value
    assert batch["entityExists"] is False and json.loads(batch["entityState"]) is None
    assert batch["signals"] == [] and _CONSTRUCTIONS == []


@pytest.mark.parametrize("value", [None, False, 0, "", '{"looks":"json"}', [], {}, {"nested": [_payload(True)]}])
def test_json_context_decodes_exactly_two_native_input_layers(value: Any) -> None:
    from agent_framework_azurefunctions._entity_json import create_json_entity

    def entity(context: df.DurableEntityContext) -> None:
        context.set_result(context.get_input())

    batch = json.loads(create_json_entity(entity)(_wire([("read", value)])))
    assert not batch["results"][0]["isError"], batch
    assert json.loads(batch["results"][0]["result"]) == value
    assert _CONSTRUCTIONS == []


def test_json_context_keeps_absent_input_and_state_initializer_behavior() -> None:
    from agent_framework_azurefunctions._entity_json import create_json_entity

    seen: list[Any] = []

    def entity(context: df.DurableEntityContext) -> None:
        seen.append(context.get_input())
        context.set_result(context.get_state(lambda: {"initial": True}))

    envelope = json.loads(_wire([("read", None)]))
    envelope["batch"][0]["input"] = "null"  # Native omitted-operation-input sentinel.
    batch = json.loads(create_json_entity(entity)(json.dumps(envelope)))
    assert not batch["results"][0]["isError"], batch
    assert seen == [None]
    assert json.loads(batch["results"][0]["result"]) == {"initial": True}
    assert json.loads(batch["entityState"]) is None  # Initializer is not an implicit write.


def test_json_wrapper_keeps_malformed_input_as_an_sdk_operation_error() -> None:
    from agent_framework_azurefunctions._entity_json import create_json_entity

    def entity(context: df.DurableEntityContext) -> None:
        context.set_result(context.get_input())

    envelope = json.loads(_wire([("read", None), ("read", False)]))
    envelope["batch"][0]["input"] = json.dumps("not-json")
    batch = json.loads(create_json_entity(entity)(json.dumps(envelope)))
    assert [result["isError"] for result in batch["results"]] == [True, False]
    assert json.loads(batch["results"][1]["result"]) is False
    assert _CONSTRUCTIONS == []
