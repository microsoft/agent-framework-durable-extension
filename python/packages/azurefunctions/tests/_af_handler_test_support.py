# Copyright (c) Microsoft. All rights reserved.

"""Call the handlers the Functions host invokes, with serialized protobuf requests.

azure-functions-durable 2.x hands each orchestrator and entity function its own durable
worker. These helpers drive those generated handlers directly rather than a replay of
the registrations, so the worker's converter and failure handling are the real ones.
"""

import asyncio
import base64
import inspect
import json
from collections.abc import Mapping, Sequence
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from agent_framework import Agent, BaseChatClient, ChatResponse, Message
from azure.durable_functions.internal.payloads import ActivityPayload, get_payload_store
from durabletask import task
from durabletask.entities import EntityInstanceId
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.payload import LargePayloadStorageOptions, PayloadStore

from agent_framework_azurefunctions import AgentFunctionApp

_NOW = datetime(2026, 9, 23, tzinfo=timezone.utc)


class _MemoryPayloadStore(PayloadStore):
    def __init__(self) -> None:
        self.values: dict[str, bytes] = {}

    @property
    def options(self) -> LargePayloadStorageOptions:
        return LargePayloadStorageOptions(threshold_bytes=64)

    def upload(self, data: bytes, *, instance_id: str | None = None) -> str:
        token = f"memory:payload:{len(self.values)}"
        self.values[token] = data
        return token

    async def upload_async(self, data: bytes, *, instance_id: str | None = None) -> str:
        return self.upload(data, instance_id=instance_id)

    def download(self, token: str) -> bytes:
        return self.values[token]

    async def download_async(self, token: str) -> bytes:
        return self.download(token)

    def is_known_token(self, value: str) -> bool:
        return value.startswith("memory:payload:")


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


def _app(**kwargs: Any) -> AgentFunctionApp:
    return AgentFunctionApp(enable_health_check=False, deployment_mode="isolated_v2", **kwargs)


def _worker(function: Any) -> Any:
    return inspect.getclosurevars(inspect.unwrap(function.get_user_function())).nonlocals["worker"]


def _entity_function(functions: Mapping[Any, Any], name: str) -> Any:
    return next(
        function
        for function in functions.values()
        if hasattr(function.get_user_function(), "entity_function")
        and task.get_entity_name(function.get_user_function().entity_function).lower() == name
    )


class _Host:
    """Calls the handlers the Functions host invokes, with serialized protobuf requests."""

    def __init__(self, app: AgentFunctionApp) -> None:
        self.functions = {function.get_function_name(): function for function in app.get_functions()}
        self.histories: dict[str, list[Any]] = {}
        self.names: dict[str, str] = {}

    @staticmethod
    def _invoke(function: Any, request: Any, response: Any) -> Any:
        handle = inspect.unwrap(function.get_user_function())
        body = base64.b64encode(request.SerializeToString()).decode("ascii")
        response.ParseFromString(base64.b64decode(asyncio.run(handle(body))))
        return response

    def start(self, name: str, instance_id: str, wire: str, *, parent: str | None = None) -> Any:
        self.histories[instance_id] = []
        self.names[instance_id] = name
        event = helpers.new_execution_started_event(name, instance_id, wire)
        if parent is not None:
            event.executionStarted.parentInstance.orchestrationInstance.instanceId = parent
        return self.replay(instance_id, event)

    def replay(self, instance_id: str, *events: Any) -> Any:
        old = self.histories[instance_id]
        new = [helpers.new_orchestrator_started_event(_NOW), *events]
        request = pb.OrchestratorRequest(instanceId=instance_id, pastEvents=old, newEvents=new)
        response = self._invoke(self.functions[self.names[instance_id]], request, pb.OrchestratorResponse())
        self.histories[instance_id] = [*old, *new]
        return response

    def activity(self, instance_id: str, action: Any) -> Any:
        scheduled = action.scheduleTask
        # The host binds the decoded activity input to the trigger parameter.
        activity_input = (
            ActivityPayload(scheduled.input.value)
            if get_payload_store() is not None
            else json.loads(scheduled.input.value)
        )
        output = self.functions[scheduled.name].get_user_function()(activity_input)
        wire = output.value if isinstance(output, ActivityPayload) else json.dumps(output)
        return self.replay(
            instance_id,
            helpers.new_task_scheduled_event(action.id, scheduled.name, scheduled.input.value),
            helpers.new_task_completed_event(action.id, wire),
        )

    def entity(self, instance_id: str, operation: str, wire: str | None, state: str | None = None) -> Any:
        request = pb.EntityBatchRequest(
            instanceId=instance_id,
            entityState=helpers.get_string_value(state),
            operations=[
                pb.OperationRequest(operation=operation, requestId="request", input=helpers.get_string_value(wire))
            ],
        )
        function = _entity_function(self.functions, EntityInstanceId.parse(instance_id).entity)
        return self._invoke(function, request, pb.EntityBatchResult())


def _action(result: Any, kind: str) -> Any:
    assert len(result.actions) == 1, result.actions
    action = result.actions[0]
    assert action.HasField(kind), action
    return action


def _completed(result: Any) -> Any:
    completion = _action(result, "completeOrchestration").completeOrchestration
    assert completion.orchestrationStatus == pb.ORCHESTRATION_STATUS_COMPLETED, completion
    return completion


def _result(completion: Any) -> Any:
    return json.loads(completion.result.value) if completion.HasField("result") else None


def _succeeded(result: Any) -> str:
    assert len(result.results) == 1 and result.results[0].HasField("success"), result
    return result.results[0].success.result.value
