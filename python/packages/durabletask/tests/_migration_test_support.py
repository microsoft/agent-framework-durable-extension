# Copyright (c) Microsoft. All rights reserved.

"""Shared legacy migration fixtures for entity host-boundary tests."""

from __future__ import annotations

from collections.abc import Sequence
from copy import deepcopy
from typing import Any
from unittest.mock import MagicMock, Mock

from _execution_test_support import JsonStateProvider, RecordingChatClient
from agent_framework import Agent, HistoryProvider, Message
from durabletask.entities import EntityContext, EntityInstanceId
from durabletask.internal.entity_state_shim import StateShim
from durabletask.serialization import JsonDataConverter

from agent_framework_durabletask import AgentEntity, DurableAIAgentWorker, state_snapshot_digest

WINDOW = 60
SOURCE_SESSION_ID = "legacy-provider:source-session"
ENTITY_NAME = "dafx-migration-agent"


def _legacy_source(*entries: dict[str, Any], version: str = "1.1.0") -> dict[str, Any]:
    return {"schemaVersion": version, "data": {"conversationHistory": deepcopy(list(entries))}}


def _error_response_entry(correlation_id: str = "done") -> dict[str, Any]:
    return {
        "$type": "errorResponse",
        "correlationId": correlation_id,
        "createdAt": "2024-01-02T03:04:06+00:00",
        "messages": [{"role": "assistant", "contents": [{"$type": "error", "message": "legacy failure"}]}],
    }


def _original_result(correlation_id: str = "done", *, outcome: str = "failed") -> dict[str, Any]:
    result: dict[str, Any] = {
        "correlationId": correlation_id,
        "outcome": outcome,
        "completedAt": "2024-01-03T04:05:06+00:00",
        "response": {"messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "original result"}]}]},
    }
    if outcome == "failed":
        result["error"] = {"code": "provider_failure", "message": "Original invocation failed."}
    return result


def _completion_journal(source: dict[str, Any], *results: dict[str, Any]) -> dict[str, Any]:
    return {
        "sourceDigest": state_snapshot_digest(source),
        "evidenceId": "completion-journal-1",
        "complete": True,
        "results": deepcopy(list(results)),
    }


def _snapshot_digest(source: dict[str, Any]) -> str:
    return state_snapshot_digest(source)


def _migration_request(source: dict[str, Any], destination_session_id: str, **overrides: Any) -> dict[str, Any]:
    request: dict[str, Any] = {
        "source": deepcopy(source),
        "sourceDigest": _snapshot_digest(source),
        "sourceSessionId": SOURCE_SESSION_ID,
        "destinationSessionId": destination_session_id,
        "migrationId": "migration-1",
        "ownershipTransferId": "transfer-1",
    }
    request.update(overrides)
    return request


def _json_provider_entity(
    raw: dict[str, Any] | None = None,
    *,
    provider_cls: type[JsonStateProvider] = JsonStateProvider,
    session_id: str = "dest-session",
    entity_name: str = ENTITY_NAME,
    agent: Agent | None = None,
) -> tuple[AgentEntity, JsonStateProvider, RecordingChatClient]:
    client = RecordingChatClient(response_message_id="response-message")
    provider = provider_cls(raw, session_id=session_id, entity_name=entity_name)
    entity = AgentEntity(agent or Agent(client=client, name="migration-agent"), state_provider=provider)
    return entity, provider, client


def _sdk_registered_entity(
    state_json: str | None = None,
    *,
    session_id: str = "sdk-destination",
    response_delivery_window_seconds: int = WINDOW,
) -> tuple[Any, StateShim, RecordingChatClient, Mock]:
    client = RecordingChatClient(response_message_id="sdk-message")
    native = Mock()
    native.add_entity = MagicMock(return_value=ENTITY_NAME)
    worker = DurableAIAgentWorker(
        native,
        deployment_mode="isolated_v2",
        response_delivery_window_seconds=response_delivery_window_seconds,
    )
    worker.add_agent(Agent(client=client, name="migration-agent"), entity_id="migration-agent")
    registered_class = native.add_entity.call_args.args[0]
    converter = JsonDataConverter()
    shim = StateShim(state_json, converter, is_serialized=True)
    context = EntityContext(
        "orchestration",
        "operation",
        shim,
        EntityInstanceId(ENTITY_NAME, session_id),
        converter,
    )
    instance = registered_class()
    instance._initialize_entity_context(context)
    return instance, shim, client, native


class _ObservedExternalHistory(HistoryProvider):
    def __init__(self) -> None:
        super().__init__("external")
        self.calls: list[tuple[str, str | None]] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        del kwargs
        self.calls.append(("load", session_id))
        return []

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        del messages, kwargs
        self.calls.append(("save", session_id))
