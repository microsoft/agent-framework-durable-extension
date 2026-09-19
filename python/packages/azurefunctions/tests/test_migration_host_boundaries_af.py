# Copyright (c) Microsoft. All rights reserved.

"""Real Azure Functions host-boundary tests for explicit legacy migration."""

from __future__ import annotations

import json
from copy import deepcopy
from typing import Any
from unittest.mock import Mock

import pytest
from _execution_test_support import RecordingChatClient
from agent_framework import Agent, AgentSession
from agent_framework_durabletask import state_snapshot_digest

from agent_framework_azurefunctions import AgentFunctionApp
from agent_framework_azurefunctions._entities import create_agent_entity

SOURCE_SESSION_ID = "legacy-provider:source-session"


def _legacy_source(*entries: dict[str, Any], version: str = "1.1.0") -> dict[str, Any]:
    return {"schemaVersion": version, "data": {"conversationHistory": deepcopy(list(entries))}}


def _error_response_entry(correlation_id: str = "done") -> dict[str, Any]:
    return {
        "$type": "errorResponse",
        "correlationId": correlation_id,
        "createdAt": "2024-01-02T03:04:06+00:00",
        "messages": [{"role": "assistant", "contents": [{"$type": "error", "message": "legacy failure"}]}],
    }


def _original_result(correlation_id: str = "done") -> dict[str, Any]:
    return {
        "correlationId": correlation_id,
        "outcome": "failed",
        "completedAt": "2024-01-03T04:05:06+00:00",
        "response": {"messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "original result"}]}]},
        "error": {"code": "provider_failure", "message": "Original invocation failed."},
    }


def _completion_journal(source: dict[str, Any], *results: dict[str, Any]) -> dict[str, Any]:
    return {
        "sourceDigest": state_snapshot_digest(source),
        "evidenceId": "completion-journal-1",
        "complete": True,
        "results": deepcopy(list(results)),
    }


def _migration_request(source: dict[str, Any], destination_session_id: str, **overrides: Any) -> dict[str, Any]:
    request: dict[str, Any] = {
        "source": deepcopy(source),
        "sourceDigest": state_snapshot_digest(source),
        "sourceSessionId": SOURCE_SESSION_ID,
        "destinationSessionId": destination_session_id,
        "migrationId": "migration-1",
        "ownershipTransferId": "transfer-1",
    }
    request.update(overrides)
    return request


class _MigrationContext:
    def __init__(
        self,
        raw: Any,
        *,
        request: dict[str, Any],
        operation: str = "migrate",
        entity_name: str = "dafx-af-migration-agent",
        entity_key: str = "dest-session",
        lose_acknowledgement: bool = False,
    ) -> None:
        self.raw = deepcopy(raw)
        self._lose_acknowledgement = lose_acknowledgement
        self._write_count = 0
        self.operation_name = operation
        self.entity_name = entity_name
        self.entity_key = entity_key
        self.get_input = Mock(return_value=deepcopy(request))
        self.set_result = Mock()

    def get_state(self, factory: Any = None) -> Any:
        del factory
        return {} if self.raw is None else self.raw

    def set_state(self, value: dict[str, Any]) -> None:
        self._write_count += 1
        self.raw = deepcopy(value)
        if self._lose_acknowledgement and self._write_count == 1:
            raise OSError("storage acknowledgement lost after write")


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize(
    "service_session_id",
    [
        None,
        "",
        " \t\n",
        " provider-id ",
        {},
        {"conversation": "provider-id"},
        {
            "conversation_id": "c",
            "response_id": "r",
            "future_id": "opaque",
            "metadata": {"type": "message", "values": [None, False, 0, 1.5, {}, [], "雪"]},
        },
    ],
)
def test_af_entity_migrate_operation_commits_without_model_or_core_decode(
    version: str, service_session_id: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Migration must retain canonical result JSON and never decode through Core.")

    monkeypatch.setattr("agent_framework_durabletask._state_migration.load_agent_response", forbidden)
    monkeypatch.setattr(AgentSession, "from_dict", forbidden)
    monkeypatch.setattr("agent_framework_durabletask._entities._register_loaded_state_types", forbidden)
    client = RecordingChatClient(response_message_id="af-message")
    entity_function = create_agent_entity(
        Agent(client=client, name="af-migration-agent"), deployment_mode="isolated_v2"
    )
    source = _legacy_source(_error_response_entry(), version=version)
    source["data"]["session"] = {
        "session_id": SOURCE_SESSION_ID,
        "service_session_id": deepcopy(service_session_id),
        "state": {"typed": {"type": "message", "opaque": [None, False, 0]}},
        "futureSession": {"keep": [1]},
    }
    context = _MigrationContext(
        None,
        request=_migration_request(
            source,
            "@dafx-af-migration-agent@dest-session",
            completionEvidence=_completion_journal(source, _original_result()),
        ),
    )
    before = deepcopy((source, context.get_input.return_value))

    entity_function(context)

    context.set_result.assert_called_once_with({
        "status": "migrated",
        "migrationId": "migration-1",
        "sessionId": "@dafx-af-migration-agent@dest-session",
    })
    assert client.received_messages == []
    assert context.raw["data"]["migration"]["destinationSessionId"] == "@dafx-af-migration-agent@dest-session"
    assert context.raw["data"]["terminalResults"]["done"]["response"] == _original_result()["response"]
    assert context.raw["data"]["session"] == source["data"]["session"]
    assert json.dumps(context.raw["data"]["session"], sort_keys=True) == json.dumps(
        source["data"]["session"], sort_keys=True
    )
    assert context._write_count == 1
    assert (source, context.get_input.return_value) == before


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
            lambda request, destination: request.update({"unexpected": True}),
            "complete explicit source and destination request",
            id="extra-field",
        ),
    ],
)
def test_af_entity_migrate_rejects_identity_mismatches_before_write(mutate: Any, message: str) -> None:
    entity_function = create_agent_entity(
        Agent(client=RecordingChatClient(), name="af-migration-agent"), deployment_mode="isolated_v2"
    )
    source = _legacy_source()
    request = _migration_request(source, "@dafx-af-migration-agent@dest-session")
    mutate(request, "@dafx-af-migration-agent@dest-session")
    context = _MigrationContext(None, request=request)

    entity_function(context)

    context.set_result.assert_called_once()
    result = context.set_result.call_args.args[0]
    assert result["status"] == "error"
    assert message in result["error"]
    assert context._write_count == 0
    assert context.raw is None


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize(
    "marker",
    [
        None,
        {},
        {"profile": "foreign", "version": 1, "messages": {"accepted": ["a" * 64]}},
        {"profile": "agent-framework-python.ingestion", "version": 1, "messages": {"accepted": ["a" * 64]}},
    ],
)
def test_af_entity_migrate_rejects_reserved_ingestion_without_write(version: str, marker: Any) -> None:
    client = RecordingChatClient()
    entity_function = create_agent_entity(
        Agent(client=client, name="af-migration-agent"), deployment_mode="isolated_v2"
    )
    source = _legacy_source(version=version)
    source["data"]["pythonIngestion"] = deepcopy(marker)
    request = _migration_request(source, "@dafx-af-migration-agent@dest-session")
    context = _MigrationContext(None, request=request)
    before = deepcopy((source, request, context.get_input.return_value))

    entity_function(context)

    context.set_result.assert_called_once()
    result = context.set_result.call_args.args[0]
    assert result["status"] == "error"
    assert "Legacy data contains reserved pythonIngestion metadata" in result["error"]
    assert context._write_count == 0
    assert context.raw is None
    assert (source, request, context.get_input.return_value) == before
    assert client.received_messages == []


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("state", []),
        ("state", None),
        ("state", 7),
        ("state", False),
        ("state", "opaque"),
        ("service_session_id", []),
        ("service_session_id", ["provider-id"]),
        ("service_session_id", 7),
        ("service_session_id", 1.5),
        ("service_session_id", False),
        ("service_session_id", True),
    ],
)
def test_af_entity_migrate_rejects_invalid_session_without_side_effects(
    version: str, field: str, value: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = RecordingChatClient()
    entity_function = create_agent_entity(
        Agent(client=client, name="af-migration-agent"), deployment_mode="isolated_v2"
    )
    source = _legacy_source(version=version)
    source["data"]["session"] = {"session_id": SOURCE_SESSION_ID, field: deepcopy(value)}
    request = _migration_request(
        source, "@dafx-af-migration-agent@dest-session", completionEvidence=_completion_journal(source)
    )
    context = _MigrationContext(None, request=request)
    before = deepcopy((source, request, context.get_input.return_value))

    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("Migration must not deserialize session values or register provider types.")

    monkeypatch.setattr(AgentSession, "from_dict", forbidden)
    monkeypatch.setattr("agent_framework_durabletask._entities._register_loaded_state_types", forbidden)
    entity_function(context)

    context.set_result.assert_called_once()
    result = context.set_result.call_args.args[0]
    assert result["status"] == "error"
    assert f"Legacy session.{field} must be" in result["error"]
    assert context._write_count == 0
    assert context.raw is None
    assert (source, request, context.get_input.return_value) == before
    assert client.received_messages == []


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        pytest.param(False, "JSON object", id="bool"),
        pytest.param([], "JSON object", id="list"),
        pytest.param(
            {"schemaVersion": "1.1.0", "data": {"conversationHistory": []}}, "Legacy state is read-only", id="legacy"
        ),
    ],
)
def test_af_entity_migrate_rejects_malformed_backing_without_write(raw: Any, message: str) -> None:
    entity_function = create_agent_entity(
        Agent(client=RecordingChatClient(), name="af-migration-agent"), deployment_mode="isolated_v2"
    )
    context = _MigrationContext(
        raw, request=_migration_request(_legacy_source(), "@dafx-af-migration-agent@dest-session")
    )
    before = deepcopy(context.raw)

    entity_function(context)

    result = context.set_result.call_args.args[0]
    assert result["status"] == "error"
    assert message in result["error"]
    assert context._write_count == 0
    assert context.raw == before


def test_af_entity_migrate_lost_acknowledgement_replays_same_request_idempotently() -> None:
    entity_function = create_agent_entity(
        Agent(client=RecordingChatClient(), name="af-migration-agent"), deployment_mode="isolated_v2"
    )
    source = _legacy_source(_error_response_entry())
    request = _migration_request(
        source,
        "@dafx-af-migration-agent@dest-session",
        completionEvidence=_completion_journal(source, _original_result()),
    )
    context = _MigrationContext(None, request=request, lose_acknowledgement=True)

    entity_function(context)

    first = context.set_result.call_args.args[0]
    assert first == {"error": "storage acknowledgement lost after write", "status": "error"}
    committed_once = deepcopy(context.raw)
    expiry = committed_once["data"]["terminalResults"]["done"]["resultExpiresAt"]

    entity_function(context)

    second = context.set_result.call_args.args[0]
    assert second == {
        "status": "migrated",
        "migrationId": "migration-1",
        "sessionId": "@dafx-af-migration-agent@dest-session",
    }
    assert context._write_count == 1
    assert context.raw == committed_once
    assert context.raw["data"]["terminalResults"]["done"]["resultExpiresAt"] == expiry


def test_af_entity_migrate_changed_request_with_same_id_is_rejected_on_nonempty_destination() -> None:
    entity_function = create_agent_entity(
        Agent(client=RecordingChatClient(), name="af-migration-agent"), deployment_mode="isolated_v2"
    )
    source = _legacy_source(_error_response_entry())
    request = _migration_request(
        source,
        "@dafx-af-migration-agent@dest-session",
        completionEvidence=_completion_journal(source, _original_result()),
    )
    context = _MigrationContext(None, request=request)
    entity_function(context)
    write_count = context._write_count

    changed = _migration_request(
        source,
        "@dafx-af-migration-agent@dest-session",
        completionEvidence=_completion_journal(source, _original_result()),
        ownershipTransferId="transfer-2",
    )
    context.get_input.return_value = changed
    entity_function(context)

    result = context.set_result.call_args.args[0]
    assert result["status"] == "error"
    assert "destination must be empty" in result["error"]
    assert context._write_count == write_count


def test_agent_function_app_exposes_migration_only_through_entity_operation(monkeypatch: pytest.MonkeyPatch) -> None:
    routes: list[dict[str, Any]] = []
    entities: list[dict[str, Any]] = []
    tools: list[dict[str, Any]] = []

    app = AgentFunctionApp(enable_health_check=False, deployment_mode="isolated_v2")

    def identity(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        return lambda handler: handler

    def route(*args: Any, **kwargs: Any) -> Any:
        del args
        routes.append(dict(kwargs))
        return lambda handler: handler

    def entity_trigger(*args: Any, **kwargs: Any) -> Any:
        del args
        entities.append(dict(kwargs))
        return lambda handler: handler

    def mcp_tool_trigger(*args: Any, **kwargs: Any) -> Any:
        del args
        tools.append(dict(kwargs))
        return lambda handler: handler

    monkeypatch.setattr(app, "function_name", identity)
    monkeypatch.setattr(app, "durable_client_input", identity)
    monkeypatch.setattr(app, "route", route)
    monkeypatch.setattr(app, "entity_trigger", entity_trigger)
    monkeypatch.setattr(app, "mcp_tool_trigger", mcp_tool_trigger)
    agent = Mock()
    agent.name = "af-migration-agent"
    agent.description = "migration subject"
    app.add_agent(
        agent,
        enable_http_endpoint=True,
        enable_mcp_tool_trigger=True,
    )

    assert routes == [{"route": "agents/af-migration-agent/run", "methods": ["POST"]}]
    assert entities == [{"context_name": "context", "entity_name": "dafx-af-migration-agent"}]
    assert tools and tools[0]["tool_name"] == "af-migration-agent"
    assert all("migrate" not in str(item).lower() for item in routes)
    assert all("migrate" not in str(item).lower() for item in tools)
    assert entities[0]["entity_name"].startswith("dafx-")
