# Copyright (c) Microsoft. All rights reserved.

"""Runtime reproduction for the real sample 11 subworkflow host path."""

from __future__ import annotations

import importlib.util
import json
import sys
from collections.abc import AsyncIterable, Awaitable, Mapping, Sequence
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast
from unittest.mock import Mock

import pytest
from agent_framework import BaseChatClient, ChatResponse, ChatResponseUpdate, Message, ResponseStream
from durabletask.entities import EntityContext, EntityInstanceId
from durabletask.internal.entity_state_shim import StateShim
from durabletask.serialization import JsonDataConverter
from durabletask.task import CompletableTask
from durabletask.worker import _Registry as TaskRegistry

from agent_framework_durabletask import DurableAIAgentWorker
from agent_framework_durabletask._workflows.naming import subworkflow_instance_id
from agent_framework_durabletask._workflows.orchestrator import SUBWORKFLOW_ADDRESS_KEY, SUBWORKFLOW_INPUT_KEY
from agent_framework_durabletask._workflows.protocol import unwrap_workflow_input
from agent_framework_durabletask._workflows.serialization import deserialize_value


class _SyntheticChatClient(BaseChatClient):
    STORES_BY_DEFAULT = False

    def __init__(self, payload: dict[str, Any]) -> None:
        super().__init__()
        self.payload = json.loads(json.dumps(payload, allow_nan=False))
        self.calls: list[list[Message]] = []
        self.stream_flags: list[bool] = []
        self.options_history: list[dict[str, Any]] = []

    def _inner_get_response(
        self,
        *,
        messages: Sequence[Message],
        stream: bool,
        options: Mapping[str, Any],
        **kwargs: Any,
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        del kwargs
        self.calls.append(deepcopy(list(messages)))
        self.stream_flags.append(stream)
        self.options_history.append(dict(options))

        response = ChatResponse(
            messages=[Message("assistant", [json.dumps(self.payload, allow_nan=False)])],
            response_id="sample-11-response",
            finish_reason="stop",
        )

        if stream:

            async def updates() -> AsyncIterable[ChatResponseUpdate]:
                yield ChatResponseUpdate(
                    role="assistant",
                    contents=response.messages[0].contents,
                    response_id=response.response_id,
                    finish_reason=response.finish_reason,
                )

            return ResponseStream(updates(), finalizer=ChatResponse.from_updates)

        async def get() -> ChatResponse:
            return response

        return get()


class _RecordingBackend:
    def __init__(self) -> None:
        self.registry = TaskRegistry()
        self.activities: dict[str, Any] = {}
        self.orchestrators: dict[str, Any] = {}
        self.entity_registrations: list[str] = []
        self.activity_registrations: list[str] = []
        self.orchestrator_registrations: list[str] = []
        self.entity_calls: list[dict[str, Any]] = []
        self.activity_calls: list[dict[str, Any]] = []
        self.sub_orchestrator_calls: list[dict[str, Any]] = []
        self.sub_orchestrator_results: list[Any] = []
        self.entity_contexts: list[EntityContext] = []
        self.entity_state_snapshots: list[dict[str, Any]] = []

    def add_entity(self, cls: type[Any]) -> str:
        registered = self.registry.add_entity(cls)
        self.entity_registrations.append(registered)
        return registered

    def add_activity(self, fn: Any) -> str:
        self.activities[fn.__name__] = fn
        self.activity_registrations.append(fn.__name__)
        return fn.__name__

    def add_orchestrator(self, fn: Any) -> str:
        self.orchestrators[fn.__name__] = fn
        self.orchestrator_registrations.append(fn.__name__)
        return fn.__name__


class _MockSDKContext:
    def __init__(self, backend: _RecordingBackend, *, instance_id: str, parent_instance_id: str | None = None) -> None:
        self._backend = backend
        self.instance_id = instance_id
        self.parent_instance_id = parent_instance_id
        self.is_replaying = False
        self.current_utc_datetime = datetime.now(timezone.utc)
        self.statuses: list[Any] = []
        self._uuid = 0

    def new_uuid(self) -> str:
        self._uuid += 1
        return f"uuid-{self._uuid}"

    def set_custom_status(self, status: Any) -> None:
        self.statuses.append(deepcopy(status))

    def wait_for_external_event(self, name: str) -> Any:
        raise AssertionError(name)

    def create_timer(self, fire_at: datetime) -> Any:
        raise AssertionError(str(fire_at))

    def signal_entity(self, *args: Any, **kwargs: Any) -> None:
        raise AssertionError((args, kwargs))

    def call_entity(self, entity_id: EntityInstanceId, operation: str, request: dict[str, Any]) -> CompletableTask[Any]:
        request = json.loads(json.dumps(request, allow_nan=False))
        self._backend.entity_calls.append({
            "entity": entity_id.entity,
            "key": entity_id.key,
            "request": deepcopy(request),
        })
        entity_class = cast(type[Any], self._backend.registry.get_entity(entity_id.entity))
        assert entity_class is not None
        entity = entity_class()
        state_shim = StateShim(None, JsonDataConverter(), is_serialized=True)
        context = EntityContext(
            "orchestration",
            operation,
            state_shim,
            entity_id,
            JsonDataConverter(),
        )
        self._backend.entity_contexts.append(context)
        entity._initialize_entity_context(context)
        task: CompletableTask[Any] = CompletableTask()
        result = entity.run(request)
        encoded_state = state_shim.encode_state()
        self._backend.entity_state_snapshots.append(json.loads(encoded_state) if encoded_state is not None else {})
        task.complete(json.loads(json.dumps(result, allow_nan=False)))
        return task

    def call_activity(self, name: str, *, input: str) -> CompletableTask[Any]:
        self._backend.activity_calls.append({"name": name, "input": json.loads(input)})
        task: CompletableTask[Any] = CompletableTask()
        task.complete(self._backend.activities[name](Mock(), input))
        return task

    def call_sub_orchestrator(self, name: str, *, input: Any, instance_id: str | None = None) -> CompletableTask[Any]:
        input = json.loads(json.dumps(input, allow_nan=False))
        child_id = instance_id or f"{self.instance_id}:child"
        self._backend.sub_orchestrator_calls.append({"name": name, "input": deepcopy(input), "instance_id": child_id})
        child = _MockSDKContext(self._backend, instance_id=child_id, parent_instance_id=self.instance_id)
        task: CompletableTask[Any] = CompletableTask()
        result = _run_orchestrator(self._backend.orchestrators[name], child, input)
        wire_result = json.loads(json.dumps(result, allow_nan=False))
        self._backend.sub_orchestrator_results.append(deepcopy(wire_result))
        task.complete(wire_result)
        return task


def _run_orchestrator(orchestrator: Any, host: _MockSDKContext, input_data: Any) -> Any:
    generator = orchestrator(host, input_data)
    value = None
    while True:
        try:
            yielded = generator.send(value)
        except StopIteration as completed:
            return completed.value
        assert yielded.is_complete
        value = yielded.get_result()


def _load_sample_worker(monkeypatch: pytest.MonkeyPatch) -> Any:
    import dotenv

    monkeypatch.setattr(dotenv, "load_dotenv", lambda *args, **kwargs: None)
    path = Path(__file__).resolve().parents[3] / "samples" / "11_subworkflow" / "worker.py"
    name = "_sample11_runtime_subworkflow"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("review", "sentiment_payload"),
    [
        pytest.param(
            "Absolutely love this espresso machine - it heats up fast and the coffee is consistently great.",
            {"sentiment": "positive", "confidence": 0.93},
            id="positive-review",
        ),
        pytest.param(
            "Disappointed. The device stopped working after two weeks and support never replied.",
            {"sentiment": "negative", "confidence": 0.08},
            id="negative-review",
        ),
    ],
)
def test_real_sample_subworkflow_completes_through_registered_hosts(
    monkeypatch: pytest.MonkeyPatch,
    review: str,
    sentiment_payload: dict[str, Any],
) -> None:
    sample = _load_sample_worker(monkeypatch)
    chat_client = _SyntheticChatClient(sentiment_payload)
    monkeypatch.setattr(sample, "_create_chat_client", lambda: chat_client)

    backend = _RecordingBackend()
    DurableAIAgentWorker(cast(Any, backend), deployment_mode="isolated_v2").configure_workflow(sample.create_workflow())

    assert backend.entity_registrations == ["dafx-sentiment_analysis-SentimentAgent"]
    assert list(backend.registry.entities) == ["dafx-sentiment_analysis-sentimentagent"]
    assert backend.activity_registrations == [
        "dafx-review_pipeline-intake",
        "dafx-review_pipeline-reporter",
        "dafx-sentiment_analysis-sentiment_formatter",
        "dafx__hitl-sentiment_analysis",
    ]
    assert backend.orchestrator_registrations == ["dafx-review_pipeline", "dafx-sentiment_analysis"]

    host = _MockSDKContext(backend, instance_id="root-run")

    result = _run_orchestrator(
        backend.orchestrators["dafx-review_pipeline"],
        host,
        {"_durable_workflow_version": 2, "input": review},
    )

    assert result == [
        (
            f"Review analysis complete -> sentiment: {sentiment_payload['sentiment']} "
            f"(confidence {sentiment_payload['confidence']:.0%})"
        )
    ]

    assert len(chat_client.calls) == 1
    assert chat_client.options_history[0]["response_format"] is sample.SentimentResult
    assert [call["name"] for call in backend.sub_orchestrator_calls] == ["dafx-sentiment_analysis"]
    assert [call["name"] for call in backend.activity_calls] == [
        "dafx-review_pipeline-intake",
        "dafx-sentiment_analysis-sentiment_formatter",
        "dafx-review_pipeline-reporter",
    ]
    assert [call["entity"] for call in backend.entity_calls] == ["dafx-sentiment_analysis-sentimentagent"]

    intake_input = backend.activity_calls[0]["input"]
    assert intake_input["message"] == review
    assert intake_input["host_context"] == {
        "instance_id": "root-run",
        "workflow_name": sample.OUTER_WORKFLOW_NAME,
        "request_path_prefix": "",
    }

    child_wire = unwrap_workflow_input(backend.sub_orchestrator_calls[0]["input"])
    assert deserialize_value(child_wire[SUBWORKFLOW_INPUT_KEY]) == review.strip()
    assert child_wire[SUBWORKFLOW_ADDRESS_KEY] == {
        "root_instance_id": "root-run",
        "root_workflow_name": sample.OUTER_WORKFLOW_NAME,
        "request_path_prefix": "sentiment_sub~0~",
    }

    child_result = backend.sub_orchestrator_results[0]
    assert child_result["__subworkflow_result__"] is True
    assert child_result["outputs"] == [
        f"{sentiment_payload['sentiment']} (confidence {sentiment_payload['confidence']:.0%})"
    ]

    request = backend.entity_calls[0]["request"]
    assert request["message"] == review.strip()
    assert request["contextMessages"] == [Message("user", [review.strip()]).to_dict()]
    assert len(request["contextMessageIds"]) == 1
    assert backend.sub_orchestrator_calls[0]["instance_id"] == subworkflow_instance_id("root-run", "sentiment_sub", 0)
    assert len(backend.entity_contexts) == 1

    chat_messages = chat_client.calls[0]
    assert [message.text for message in chat_messages if message.role == "user"] == [review.strip()]

    formatter_input = backend.activity_calls[1]["input"]
    formatted_response = deserialize_value(formatter_input["message"])
    assert formatted_response.agent_response.text == json.dumps(sentiment_payload, allow_nan=False)

    reporter_input = backend.activity_calls[2]["input"]
    assert (
        reporter_input["message"]
        == f"{sentiment_payload['sentiment']} (confidence {sentiment_payload['confidence']:.0%})"
    )

    entity_state = backend.entity_state_snapshots[0]
    assert [entry["correlationId"] for entry in entity_state["data"]["conversationHistory"]] == [
        backend.entity_calls[0]["request"]["correlationId"],
        backend.entity_calls[0]["request"]["correlationId"],
    ]
    assert entity_state["data"]["conversationHistory"][0]["messages"][0]["contents"][0]["text"] == review.strip()
    assert entity_state["data"]["conversationHistory"][1]["messages"][0]["contents"][0]["text"] == json.dumps(
        sentiment_payload, allow_nan=False
    )
