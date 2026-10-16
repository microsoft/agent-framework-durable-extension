# Copyright (c) Microsoft. All rights reserved.

"""Template for a test-only Functions app with deterministic model output."""

from __future__ import annotations

import json
from collections.abc import AsyncIterable, Awaitable, Mapping, Sequence
from pathlib import Path
from typing import Any

import azure.durable_functions as df
import azure.functions as func
from agent_framework import Agent, BaseChatClient, ChatResponse, ChatResponseUpdate, Content, Message, ResponseStream
from agent_framework_durabletask import AgentSessionId, LegacyMigrationEvidence

from agent_framework_azurefunctions import AgentFunctionApp

CONFIG = json.loads(Path("live_config.json").read_text(encoding="utf-8"))
ENTITY = df.EntityId(AgentSessionId.to_entity_name(CONFIG["agent"]), CONFIG["session"])


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
        self.calls += 1
        current = next(message.message_id for message in reversed(messages) if message.role == "user")

        async def updates() -> AsyncIterable[ChatResponseUpdate]:
            yield ChatResponseUpdate(
                role="assistant",
                contents=[Content.from_text(f"answer:{current}")],
                message_id=f"{current}-answer",
                response_id=f"response:{current}",
                finish_reason="stop",
            )

        async def response() -> ChatResponse:
            return ChatResponse.from_updates([update async for update in updates()])

        return ResponseStream(updates(), finalizer=ChatResponse.from_updates) if stream else response()


MODEL = RecordingModel()
app = AgentFunctionApp(
    agents=[Agent(client=MODEL, name=CONFIG["agent"], default_options={"store": False})],
    http_auth_level=func.AuthLevel.ANONYMOUS,
    enable_http_endpoints=False,
    deployment_mode=CONFIG.get("mode"),
    legacy_migration_evidence=(
        (lambda _: LegacyMigrationEvidence(completion_evidence=CONFIG["completion_journal"]))
        if "completion_journal" in CONFIG
        else None
    ),
)


def _json(value: Any, status: int = 200) -> func.HttpResponse:
    return func.HttpResponse(json.dumps(value), status_code=status, mimetype="application/json")


@app.orchestration_trigger(context_name="context")
def compatibility_call(context: df.DurableOrchestrationContext) -> Any:
    result = yield context.call_entity(ENTITY, "run", context.get_input())
    return result  # noqa: B901


@app.route(route="compatibility/run", methods=["POST"])
@app.durable_client_input(client_name="client")
async def run(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    instance = await client.start_new("compatibility_call", client_input=req.get_json())
    return _json({"instance": instance}, 202)


@app.route(route="compatibility/result/{instance}", methods=["GET"])
@app.durable_client_input(client_name="client")
async def result(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    status = await client.get_status(req.route_params["instance"])
    if status.runtime_status == df.OrchestrationRuntimeStatus.Failed:
        return _json({"error": status.output}, 500)
    if status.runtime_status != df.OrchestrationRuntimeStatus.Completed:
        return _json(None, 404)
    return _json(status.output)


@app.route(route="retention/state", methods=["GET"])
@app.durable_client_input(client_name="client")
async def state(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    result = await client.read_entity_state(ENTITY)
    return _json(result.entity_state) if result.entity_exists else _json(None, 404)


@app.route(route="compatibility/calls", methods=["GET"])
def calls(req: func.HttpRequest) -> func.HttpResponse:
    return _json(MODEL.calls)
