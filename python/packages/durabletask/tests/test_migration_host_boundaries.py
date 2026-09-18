# Copyright (c) Microsoft. All rights reserved.

"""Real host-boundary tests for explicit legacy migration."""

from __future__ import annotations

import json
from collections.abc import Sequence
from copy import deepcopy
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest
from _execution_test_support import JsonStateProvider, LostAcknowledgementJsonStateProvider, RecordingChatClient
from agent_framework import Agent, HistoryProvider, Message
from durabletask.entities import EntityContext, EntityInstanceId
from durabletask.internal.entity_state_shim import StateShim
from durabletask.serialization import JsonDataConverter

from agent_framework_durabletask import AgentEntity, DurableAgentState, DurableAIAgentWorker, state_snapshot_digest

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


class _LiteralStateProvider(JsonStateProvider):
    def __init__(self, raw: Any, *, session_id: str = "dest-session", entity_name: str = ENTITY_NAME) -> None:
        super().__init__(None, session_id=session_id, entity_name=entity_name)
        self.raw = deepcopy(raw)

    def _get_state_dict(self) -> dict[str, Any]:
        return deepcopy(self.raw)


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


def test_registered_dt_factory_migrate_uses_real_entity_boundary_without_model_or_core_decode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Migration must retain canonical result JSON and never decode through Core.")

    monkeypatch.setattr("agent_framework_durabletask._state_migration.load_agent_response", forbidden)
    source = _legacy_source(_error_response_entry())
    request = _migration_request(
        source,
        "@dafx-migration-agent@sdk-destination",
        completionEvidence=_completion_journal(source, _original_result()),
    )
    entity, shim, client, native = _sdk_registered_entity()

    result = entity.migrate(request)

    assert result == {
        "status": "migrated",
        "migrationId": "migration-1",
        "sessionId": "@dafx-migration-agent@sdk-destination",
    }
    assert client.received_messages == []
    assert native.add_entity.call_count == 1
    encoded_state = shim.encode_state()
    assert encoded_state is not None
    payload = json.loads(encoded_state)
    assert payload["data"]["migration"]["destinationSessionId"] == "@dafx-migration-agent@sdk-destination"
    assert payload["data"]["migration"]["sourceSessionId"] == SOURCE_SESSION_ID
    assert payload["data"]["terminalResults"]["done"]["response"] == _original_result()["response"]


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        pytest.param(
            lambda request, destination: request.update({"destinationSessionId": destination + "-wrong"}),
            "destinationSessionId does not match",
            id="wrong-destination",
        ),
        pytest.param(
            lambda request, destination: request.update({"sourceSessionId": destination}),
            "requires a separately addressed destination",
            id="in-place-rewrite",
        ),
        pytest.param(
            lambda request, destination: request.update({"migrationId": ""}),
            "migrationId must be a nonblank string",
            id="blank-migration-id",
        ),
        pytest.param(
            lambda request, destination: request.update({"unexpected": True}),
            "complete explicit source and destination request",
            id="extra-field",
        ),
    ],
)
def test_agent_entity_migrate_rejects_identity_mismatches_before_write(mutate: Any, message: str) -> None:
    entity, provider, client = _json_provider_entity()
    source = _legacy_source()
    request = _migration_request(source, provider.core_session_id)
    before = deepcopy(provider.raw)

    mutate(request, provider.core_session_id)

    with pytest.raises(ValueError, match=message):
        entity.migrate(request)

    assert provider.attempted_writes == 0
    assert provider.successful_writes == 0
    assert provider.raw == before == {}
    assert client.received_messages == []


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        pytest.param(False, "JSON object", id="bool"),
        pytest.param([], "JSON object", id="list"),
        pytest.param(
            {"schemaVersion": "1.1.0", "data": {"conversationHistory": []}}, "Legacy state is read-only", id="legacy"
        ),
        pytest.param(
            {"schemaVersion": "2.0.0", "data": {"conversationHistory": []}},
            "terminalResults",
            id="malformed-v2",
        ),
    ],
)
def test_agent_entity_migrate_rejects_malformed_existing_backing_before_write(raw: Any, message: str) -> None:
    entity, provider, client = _json_provider_entity(agent=Agent(client=RecordingChatClient(), name="migration-agent"))
    provider = _LiteralStateProvider(raw)
    entity = AgentEntity(Agent(client=client, name="migration-agent"), state_provider=provider)
    request = _migration_request(_legacy_source(), provider.core_session_id)
    snapshot = deepcopy(provider.raw)

    with pytest.raises(ValueError, match=message):
        entity.migrate(request)

    assert provider.attempted_writes == 0
    assert provider.successful_writes == 0
    assert provider.raw == snapshot
    assert client.received_messages == []


def test_agent_entity_migrate_failed_commit_rolls_back_empty_destination() -> None:
    source = _legacy_source(_error_response_entry())
    request = _migration_request(
        source, "@dafx-migration-agent@dest-session", completionEvidence=_completion_journal(source, _original_result())
    )
    entity, provider, _client = _json_provider_entity()
    provider.fail_before_write = True

    with pytest.raises(OSError, match="commit failure"):
        entity.migrate(request)

    assert provider.attempted_writes == 1
    assert provider.successful_writes == 0
    assert provider.raw == {}
    assert provider.state.to_dict() == DurableAgentState().to_dict()

    provider.fail_before_write = False
    committed = entity.migrate(request)
    assert committed["status"] == "migrated"
    assert provider.attempted_writes == 2
    assert provider.successful_writes == 1


def test_agent_entity_migrate_unknown_acknowledgement_refreshes_same_provider_without_refreshing_grace() -> None:
    source = _legacy_source(_error_response_entry())
    request = _migration_request(
        source, "@dafx-migration-agent@dest-session", completionEvidence=_completion_journal(source, _original_result())
    )
    entity, provider, client = _json_provider_entity(provider_cls=LostAcknowledgementJsonStateProvider)

    with pytest.raises(OSError, match="acknowledgement lost"):
        entity.migrate(request)

    committed_once = deepcopy(provider.raw)
    expiry = committed_once["data"]["terminalResults"]["done"]["resultExpiresAt"]
    repeated = entity.migrate(request)

    assert repeated == {
        "status": "migrated",
        "migrationId": "migration-1",
        "sessionId": "@dafx-migration-agent@dest-session",
    }
    assert provider.attempted_writes == 1
    assert provider.successful_writes == 1
    assert provider.raw == committed_once
    assert provider.raw["data"]["terminalResults"]["done"]["resultExpiresAt"] == expiry
    assert client.received_messages == []


async def test_migration_idempotency_survives_cold_reload_without_refreshing_receipt_grace() -> None:
    source = _legacy_source(_error_response_entry())
    request = _migration_request(
        source, "@dafx-migration-agent@dest-session", completionEvidence=_completion_journal(source, _original_result())
    )
    entity, provider, _client = _json_provider_entity()
    entity.migrate(request)
    migrated_payload = deepcopy(provider.raw)
    original_expiry = migrated_payload["data"]["terminalResults"]["done"]["resultExpiresAt"]

    cold_entity, cold_provider, client = _json_provider_entity(raw=deepcopy(provider.raw))
    repeated = cold_entity.migrate(request)
    fresh = await cold_entity.run({"message": "follow-up", "correlationId": "fresh-1x"})

    assert repeated == {
        "status": "migrated",
        "migrationId": "migration-1",
        "sessionId": "@dafx-migration-agent@dest-session",
    }
    assert fresh.text == "reply-1"
    assert cold_provider.raw["data"]["terminalResults"]["done"]["resultExpiresAt"] == original_expiry
    assert cold_provider.raw["data"]["completionReceipts"]["done"]["outcome"] == "failed"
    assert cold_provider.raw["data"]["terminalResults"]["fresh-1x"]["outcome"] == "succeeded"
    assert len(client.received_messages) == 1

    changed = _migration_request(
        source,
        cold_provider.core_session_id,
        completionEvidence=_completion_journal(source, _original_result()),
        ownershipTransferId="transfer-2",
    )
    with pytest.raises(ValueError, match="destination must be empty"):
        cold_entity.migrate(changed)


async def test_migration_preserves_source_session_identity_for_later_external_history_runs() -> None:
    source = _legacy_source()
    source["data"]["session"] = {
        "session_id": SOURCE_SESSION_ID,
        "state": {"external-store": {"provider-key": "original", "nested": [1]}},
    }
    request = _migration_request(
        source, "@dafx-migration-agent@dest-session", completionEvidence=_completion_journal(source)
    )
    entity, provider, _client = _json_provider_entity()
    entity.migrate(request)

    external = _ObservedExternalHistory()
    client = RecordingChatClient(response_message_id="follow-up-message")
    first_provider = JsonStateProvider(deepcopy(provider.raw), session_id="dest-session", entity_name=ENTITY_NAME)
    cold_entity = AgentEntity(
        Agent(client=client, name="migration-agent", context_providers=[external]),
        state_provider=first_provider,
    )

    response = await cold_entity.run({"message": "question", "correlationId": "fresh-1x"})

    assert response.text == "reply-1"
    assert external.calls == [("load", SOURCE_SESSION_ID), ("save", SOURCE_SESSION_ID)]
    assert cold_entity.state.data.session is not None
    assert cold_entity.state.data.session["session_id"] == SOURCE_SESSION_ID

    second_external = _ObservedExternalHistory()
    second_client = RecordingChatClient()
    second_provider = JsonStateProvider(
        deepcopy(first_provider.raw), session_id="dest-session", entity_name=ENTITY_NAME
    )
    second_entity = AgentEntity(
        Agent(client=second_client, name="migration-agent", context_providers=[second_external]),
        state_provider=second_provider,
    )
    second_response = await second_entity.run({"message": "another question", "correlationId": "fresh-2x"})
    assert second_response.text == "reply-1"
    assert second_external.calls == [("load", SOURCE_SESSION_ID), ("save", SOURCE_SESSION_ID)]
    assert second_provider.raw["data"]["session"]["session_id"] == SOURCE_SESSION_ID
    before_retry = deepcopy(second_provider.raw)
    writes = second_provider.attempted_writes
    assert second_entity.migrate(request)["status"] == "migrated"
    assert second_provider.raw == before_retry
    assert second_provider.attempted_writes == writes


@pytest.mark.parametrize(
    "field,value", [("destinationSessionId", "@other@entity"), ("sourceSessionId", ""), ("sourceSessionId", None)]
)
async def test_invalid_committed_migration_binding_blocks_provider_access(field: str, value: Any) -> None:
    entity, provider, _ = _json_provider_entity()
    source = _legacy_source()
    entity.migrate(_migration_request(source, provider.core_session_id))
    raw = deepcopy(provider.raw)
    raw["data"]["migration"][field] = value
    external = _ObservedExternalHistory()
    client = RecordingChatClient()
    restored = AgentEntity(
        Agent(client=client, name="migration-agent", context_providers=[external]),
        state_provider=JsonStateProvider(raw, session_id="dest-session", entity_name=ENTITY_NAME),
    )
    result = await restored.run({"message": "question", "correlationId": "invalid-binding"})
    assert result.additional_properties["durable_status"] == "error"
    assert "migration session binding" in result.text
    assert external.calls == []
    assert client.received_messages == []
