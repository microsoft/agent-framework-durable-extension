# Copyright (c) Microsoft. All rights reserved.

"""Copied into a temporary app by test_16, never deployed as a sample.

Only model I/O is substituted. AgentFunctionApp registers the production entity
handler and the Functions worker supplies its DurableEntityContext.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import AsyncIterable, Awaitable, Mapping, Sequence
from pathlib import Path
from typing import Any

import agent_framework_durabletask
import azure.durable_functions as df
import azure.functions as func
from agent_framework import Agent, BaseChatClient, ChatResponse, ChatResponseUpdate, Content, Message, ResponseStream
from agent_framework_durabletask import AgentSessionId, DurableHistoryProvider, RunRequest
from agent_framework_durabletask._history_provider import current_durable_history_binding
from opentelemetry.metrics import get_meter_provider, set_meter_provider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, Sum

import agent_framework_azurefunctions
from agent_framework_azurefunctions import AgentFunctionApp
from agent_framework_azurefunctions._entities import AzureFunctionEntityStateProvider

ROOT = Path(__file__).resolve().parent
CONFIG = json.loads((ROOT / "live_config.json").read_text(encoding="utf-8"))
BOOT_ID = uuid.uuid4().hex
ENTITY = df.EntityId(AgentSessionId.to_entity_name(CONFIG["agent"]), CONFIG["session"])

for package, expected in (
    (agent_framework_azurefunctions, CONFIG["azurefunctions_source"]),
    (agent_framework_durabletask, CONFIG["durabletask_source"]),
):
    assert package.__file__ is not None
    if Path(package.__file__).resolve().parent != Path(expected).resolve():
        raise RuntimeError("Live Functions test imported an extension from a different checkout")

READER = InMemoryMetricReader()
METERS = MeterProvider(metric_readers=[READER], shutdown_on_exit=False)
set_meter_provider(METERS)
if get_meter_provider() is not METERS:
    raise RuntimeError("Live test requires its own in-memory metric reader in the Functions worker")


class RecordingModel(BaseChatClient):
    def __init__(self) -> None:
        super().__init__()
        self.calls = 0

    def _inner_get_response(
        self,
        *,
        messages: Sequence[Message],
        stream: bool,
        options: Mapping[str, Any],
        **kwargs: Any,
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        async def update() -> ChatResponseUpdate:
            await self._validate_options(options)
            binding = current_durable_history_binding()
            if binding is None or not isinstance(binding.state_provider, AzureFunctionEntityStateProvider):
                raise RuntimeError("Expected the production Functions state provider on the async bridge")
            context = binding.state_provider._context
            if not isinstance(context, df.DurableEntityContext):
                raise RuntimeError("Expected a real Functions DurableEntityContext")
            if context.entity_name != ENTITY.name or context.entity_key != ENTITY.key:
                raise RuntimeError("Functions context addressed the wrong test entity")
            current_id = next(message.message_id for message in reversed(messages) if message.role == "user")
            self.calls += 1
            await asyncio.to_thread(
                (ROOT / f"model-{BOOT_ID}-{self.calls}.json").write_text,
                json.dumps({
                    "boot": BOOT_ID,
                    "calls": self.calls,
                    "current_id": current_id,
                    "messages": [message.to_dict() for message in messages],
                    "context": {
                        "provider": type(binding.state_provider).__name__,
                        "type": type(context).__name__,
                        "entity_name": context.entity_name,
                        "entity_key": context.entity_key,
                        "operation": context.operation_name,
                    },
                }),
                encoding="utf-8",
            )
            return ChatResponseUpdate(
                role="assistant",
                author_name="retention-model",
                contents=[Content.from_text(f"answer:{current_id}")],
                message_id=f"{current_id}-answer",
                response_id=f"response:{current_id}",
                finish_reason="stop",
            )

        async def updates() -> AsyncIterable[ChatResponseUpdate]:
            yield await update()

        async def response() -> ChatResponse:
            return ChatResponse.from_updates([await update()])

        return ResponseStream(updates(), finalizer=ChatResponse.from_updates) if stream else response()


MODEL = RecordingModel()
app = AgentFunctionApp(
    agents=[
        Agent(
            client=MODEL,
            name=CONFIG["agent"],
            id=CONFIG["agent"],
            default_options={"store": False},
            context_providers=[DurableHistoryProvider()],
        )
    ],
    http_auth_level=func.AuthLevel.ANONYMOUS,
    enable_http_endpoints=False,
    deployment_mode="isolated_v2",
    retention="keep_all",
    max_state_bytes=CONFIG["max_state_bytes"],
    response_delivery_window_seconds=CONFIG["delivery_window_seconds"],
)


def _json(value: Any, status: int = 200) -> func.HttpResponse:
    return func.HttpResponse(json.dumps(value), status_code=status, mimetype="application/json")


@app.route(route="retention/run", methods=["POST"])
@app.durable_client_input(client_name="client")
async def run(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    # The destination is fixed by the test, not a caller-supplied entity or path.
    if len(req.get_body()) > 100_000:
        return _json({"error": "oversized test request"}, 413)
    try:
        payload = req.get_json()
        if not isinstance(payload, dict):
            raise ValueError("Object required")
        request = RunRequest.from_dict(payload)
        if not request.context_messages:
            raise ValueError("Projected messages required")
    except (KeyError, TypeError, ValueError):
        return _json({"error": "invalid test request"}, 400)
    await client.signal_entity(ENTITY, "run", request.to_dict())
    return _json({"correlation": request.correlation_id, "session": ENTITY.key}, 202)


@app.route(route="retention/state", methods=["GET"])
@app.durable_client_input(client_name="client")
async def state(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    result = await client.read_entity_state(ENTITY)
    # No typed-state reserialization here. Return the backend JSON unchanged.
    return _json(result.entity_state) if result.entity_exists else _json(None, 404)


@app.route(route="retention/capture", methods=["GET"])
def capture(req: func.HttpRequest) -> func.HttpResponse:
    # Only the latest synthetic artifact is readable. A restarted worker must not
    # mistake the old process's on-disk capture for a new model invocation.
    path = ROOT / f"model-{BOOT_ID}-{MODEL.calls}.json"
    record = json.loads(path.read_text(encoding="utf-8")) if MODEL.calls else None
    return _json({"boot": BOOT_ID, "pid": os.getpid(), "calls": MODEL.calls, "capture": record})


@app.route(route="retention/metrics", methods=["GET"])
def metrics(req: func.HttpRequest) -> func.HttpResponse:
    rows: list[dict[str, Any]] = []
    data = READER.get_metrics_data()
    if data is not None:
        for resource in data.resource_metrics:
            for scope in resource.scope_metrics:
                for metric in scope.metrics:
                    if metric.name.startswith("durable.retention.") and isinstance(metric.data, Sum):
                        rows.extend(
                            {"name": metric.name, "value": point.value, "attributes": dict(point.attributes or {})}
                            for point in metric.data.data_points
                        )
    return _json({"boot": BOOT_ID, "rows": rows})
