# Copyright (c) Microsoft. All rights reserved.

"""Maintenance contracts through real entities and registered Durable Task methods."""

import hashlib
import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import Agent, AgentResponse, Content, ContextProvider, HistoryProvider, Message
from durabletask.entities import EntityInstanceId
from test_durable_history_provider import RecordingChatClient
from test_revision_contract import JsonStateProvider
from typing_extensions import Self

from agent_framework_durabletask import AgentEntity, DurableAgentState, DurableAIAgentWorker, serialize_agent_response
from agent_framework_durabletask import _durable_agent_state as state_module
from agent_framework_durabletask import _entities as entities_module
from agent_framework_durabletask import _retention as retention_module
from agent_framework_durabletask import _state_migration as migration_module
from agent_framework_durabletask._message_identity import message_identity

NOW = datetime(2040, 1, 1, 12, tzinfo=timezone.utc)
WINDOW = 60
SOURCE_ID = "@dafx-maintenance@legacy-source"
DESTINATION_ID = "@dafx-maintenance@destination"


class _ClockType(type):
    def __instancecheck__(cls, instance: Any) -> bool:
        # Parsed timestamps remain real datetime objects, not instances of the test subclass.
        return isinstance(instance, datetime)


class Clock(datetime, metaclass=_ClockType):
    current = NOW

    @classmethod
    def now(cls, tz: Any = None) -> Self:
        return cls.fromtimestamp(cls.current.timestamp(), tz=tz)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> type[Clock]:
    monkeypatch.setattr(Clock, "current", NOW)
    for module in (state_module, entities_module, retention_module, migration_module):
        monkeypatch.setattr(module, "datetime", Clock)
    return Clock


class Store(JsonStateProvider):
    def __init__(self, raw: dict[str, Any] | None = None) -> None:
        super().__init__(raw)
        self.attempts = 0

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        self.attempts += 1
        super()._set_state_dict(state)

    def _get_session_id_from_entity(self) -> str:
        return "destination"

    def _get_entity_name_from_entity(self) -> str:
        return "dafx-maintenance"


class Hooks(ContextProvider):
    def __init__(self) -> None:
        super().__init__("maintenance-probe")
        self.calls: list[str] = []

    async def before_run(self, **kwargs: Any) -> None:
        self.calls.append("before")

    async def after_run(self, **kwargs: Any) -> None:
        self.calls.append("after")


class ExternalHistory(HistoryProvider):
    def __init__(self) -> None:
        super().__init__("external")
        self.calls: list[tuple[str, str | None]] = []
        self.rows: dict[str | None, list[Message]] = {
            SOURCE_ID: [Message("user", ["already in the external store"], message_id="external-old")]
        }

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        self.calls.append(("get", session_id))
        return deepcopy(self.rows.get(session_id, []))

    async def save_messages(self, session_id: str | None, messages: Any, **kwargs: Any) -> None:
        self.calls.append(("save", session_id))
        self.rows.setdefault(session_id, []).extend(deepcopy(list(messages)))


def _agent() -> tuple[Agent, RecordingChatClient, Hooks, Mock]:
    client: Any = RecordingChatClient()
    hooks = Hooks()
    callback = Mock(spec=["on_streaming_response_update", "on_agent_response"])
    return Agent(client=client, name="maintenance", context_providers=[hooks]), client, hooks, callback


def _quiet(client: RecordingChatClient, hooks: Hooks, callback: Mock) -> None:
    assert client.received_messages == []
    assert hooks.calls == []
    assert callback.mock_calls == []


def _digest(raw: dict[str, Any]) -> str:
    encoded = json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _size(raw: dict[str, Any]) -> int:
    return len(json.dumps(raw, allow_nan=False))


def _source() -> dict[str, Any]:
    # Z is intentional: digest the export, not its timestamp-normalized reader projection.
    return {
        "schemaVersion": "1.1.0",
        "futureRoot": {"keep": ["雪", None]},
        "data": {
            "conversationHistory": [
                {
                    "$type": kind,
                    "correlationId": "legacy-done",
                    "createdAt": "2024-01-01T00:00:00Z",
                    "messages": [
                        {
                            "role": role,
                            "messageId": identity,
                            "contents": [{"$type": "text", "text": text}],
                        }
                    ],
                }
                for kind, role, identity, text in (
                    ("request", "user", "legacy-input", "retained legacy input"),
                    ("response", "assistant", "legacy-answer", "retained legacy answer"),
                )
            ],
            "session": {"session_id": SOURCE_ID, "state": {"opaque": {"keep": [1, 3]}}},
            "futureData": {"keep": [False, 0]},
        },
    }


def _request(source: dict[str, Any] | None = None, *, evidence: bool = False) -> dict[str, Any]:
    source = _source() if source is None else source
    messages = [Message("user", [f"accepted {position}"], message_id=f"wf_upstream_{position}") for position in (1, 3)]
    if evidence:
        source["data"]["ingestedPositions"] = {"upstream": 3}
        source["data"]["conversationHistory"][0]["messages"][0]["messageId"] = "wf_upstream_3"
    digest = _digest(source)
    request: dict[str, Any] = {
        "source": source,
        "sourceDigest": digest,
        "sourceSessionId": SOURCE_ID,
        "destinationSessionId": DESTINATION_ID,
        "migrationId": "migration-1",
        "ownershipTransferId": "operator-transfer-1",
    }
    if evidence:
        request["deliveryEvidence"] = {
            "sourceDigest": digest,
            "evidenceId": "operator-journal-1",
            "complete": True,
            "messages": [message.to_dict() for message in messages],
        }
    return request


def _mailboxes() -> dict[str, Any]:
    raw = _source()
    raw["schemaVersion"] = "2.0.0"
    state = DurableAgentState.from_dict(raw)
    state.data.ingested_messages = {"old-input": ["a" * 64]}
    state.data.completed_correlations["long-gone"] = {"completedAt": "2020-01-01T00:00:00+00:00"}
    for correlation, error in (("expired-success", False), ("expired-error", True), ("live", False)):
        response = AgentResponse[Any](
            messages=[
                Message(
                    "assistant",
                    [Content.from_error(message="original failure", error_code="OriginalError")]
                    if error
                    else [Content.from_text("original result", additional_properties={"tags": ["雪"]})],
                    message_id=f"answer-{correlation}",
                )
            ],
            response_id=f"response-{correlation}",
            additional_properties={"durable_status": "error" if error else "success", "nested": {"keep": [1]}},
            value={"original": [1, 2]} if not error else None,
        )
        state.record_response(
            correlation,
            response,
            delivery_window_seconds=WINDOW,
            now=NOW if correlation == "live" else NOW - timedelta(minutes=2),
        )
        assert state.data.response_mailbox[correlation]["response"] == serialize_agent_response(response)
    return json.loads(state.to_json())


def _without_expired(raw: dict[str, Any]) -> dict[str, Any]:
    expected = deepcopy(raw)
    for correlation in ("expired-success", "expired-error"):
        del expected["data"]["responseMailbox"][correlation]
    return expected


@pytest.mark.parametrize("operation", ["new-run", "duplicate-run", "reset", "expire_responses"])
async def test_legacy_entity_is_readable_but_every_writer_fails_before_execution(operation: str) -> None:
    raw = _source()
    store = Store(raw)
    agent, client, hooks, callback = _agent()
    entity = AgentEntity(agent, state_provider=store, callback=callback)
    cached = entity.state
    before = cached.to_dict()
    legacy = cached.try_get_agent_response("legacy-done")
    assert legacy is not None and legacy.text == "retained legacy answer"

    with pytest.raises(ValueError, match="[Ll]egacy.*read-only"):
        if operation.endswith("run"):
            correlation = "legacy-done" if operation == "duplicate-run" else "new"
            await entity.run({"message": "must not execute", "correlationId": correlation})
        else:
            getattr(entity, operation)()

    assert entity.state is cached and entity.state.to_dict() == before
    assert store.raw == raw and store.attempts == store.writes == 0
    _quiet(client, hooks, callback)


@pytest.mark.parametrize("evidence", [False, True])
def test_migrate_empty_destination_binds_raw_export_and_optional_sparse_evidence(
    clock: type[Clock], evidence: bool
) -> None:
    request = _request(evidence=evidence)
    before = deepcopy(request)
    assert request["sourceDigest"] != _digest(DurableAgentState.from_dict(request["source"]).to_dict())
    store = Store()
    agent, client, hooks, callback = _agent()
    entity = AgentEntity(agent, callback=callback, state_provider=store, response_delivery_window_seconds=WINDOW)

    result = entity.migrate(request)

    assert result == {"status": "migrated", "migrationId": "migration-1", "sessionId": store.core_session_id}
    assert store.core_session_id == DESTINATION_ID != SOURCE_ID
    assert store.writes == store.attempts == 1
    cold = Store(store.raw).state
    assert cold.schema_version == "2.0.0"
    metadata = cold.data.unknown_fields["migration"]
    assert metadata == {
        "id": "migration-1",
        "sourceDigest": request["sourceDigest"],
        "sourceSessionId": SOURCE_ID,
        "destinationSessionId": DESTINATION_ID,
        "ownershipTransferId": "operator-transfer-1",
        "requestDigest": _digest(request),
        "createdAt": NOW.isoformat(),
        **({"evidenceId": "operator-journal-1"} if evidence else {}),
    }
    assert cold.data.session == request["source"]["data"]["session"]
    expected_history = DurableAgentState.from_dict(request["source"]).to_dict()["data"]["conversationHistory"]
    assert cold.to_dict()["data"]["conversationHistory"] == expected_history
    assert cold.data.response_mailbox["legacy-done"]["expiresAt"] == (NOW + timedelta(seconds=WINDOW)).isoformat()
    assert cold.data.completed_correlations["legacy-done"]["legacy"] is True
    assert cold.to_dict()["futureRoot"] == request["source"]["futureRoot"]
    assert cold.data.unknown_fields["futureData"] == request["source"]["data"]["futureData"]
    if evidence:
        expected = {
            message["message_id"]: [message_identity(Message.from_dict(deepcopy(message)))]
            for message in request["deliveryEvidence"]["messages"]
        }
        assert cold.data.ingested_messages == expected
        assert "wf_upstream_2" not in cold.data.ingested_messages
    assert request == before
    _quiet(client, hooks, callback)


@pytest.mark.parametrize(
    "field",
    ["sourceDigest", "sourceSessionId", "destinationSessionId", "migrationId", "ownershipTransferId"],
)
def test_migration_requires_nonblank_explicit_identifiers(field: str) -> None:
    store = Store()
    agent, client, hooks, callback = _agent()
    entity = AgentEntity(agent, state_provider=store, callback=callback)
    request = _request()
    request[field] = " \t"
    with pytest.raises(ValueError, match="nonblank"):
        entity.migrate(request)
    assert store.raw == {} and store.attempts == 0
    _quiet(client, hooks, callback)


@pytest.mark.parametrize("invalid", ["raw-digest", "wrong-destination", "same-source", "missing-id", "non-json"])
def test_migration_rejects_invalid_export_or_address_without_mutating_destination(invalid: str) -> None:
    store = Store()
    agent, client, hooks, callback = _agent()
    entity = AgentEntity(agent, state_provider=store, callback=callback)
    request = _request()
    if invalid == "raw-digest":
        request["sourceDigest"] = _digest(DurableAgentState.from_dict(request["source"]).to_dict())
    elif invalid == "wrong-destination":
        request["destinationSessionId"] = "@dafx-another-agent@destination"
    elif invalid == "same-source":
        request["sourceSessionId"] = DESTINATION_ID
    elif invalid == "missing-id":
        del request["ownershipTransferId"]
    else:
        request["source"]["futureRoot"] = {"bad": (1, 2)}
    before = deepcopy(request)
    cached = entity.state
    with pytest.raises(ValueError):
        entity.migrate(request)
    assert entity.state is cached and store.raw == {} and store.attempts == 0
    assert request == before
    _quiet(client, hooks, callback)


async def test_cold_exact_retry_after_a_v2_run_never_writes_or_refreshes_grace(clock: type[Clock]) -> None:
    request = _request()
    before_source = deepcopy(request)
    store = Store()
    agent, client, hooks, callback = _agent()
    first = AgentEntity(agent, state_provider=store, callback=callback)
    result = first.migrate(request)
    expiry = store.raw["data"]["responseMailbox"]["legacy-done"]["expiresAt"]
    clock.current = NOW + timedelta(seconds=10)
    response = await first.run({"message": "v2 turn", "correlationId": "v2-done"})
    assert response.text == "reply-1" and len(client.received_messages) == 1
    assert hooks.calls == ["before", "after"]
    before = deepcopy(store.raw)
    calls = list(callback.mock_calls)

    clock.current = NOW + timedelta(days=1)
    cold_store = Store(before)
    cold = AgentEntity(agent, state_provider=cold_store, callback=callback)
    assert cold.migrate(deepcopy(request)) == result
    assert cold.migrate(deepcopy(request)) == result
    assert cold_store.raw == before and cold.state.to_dict() == before
    assert cold_store.writes == cold_store.attempts == 0
    assert cold.state.data.response_mailbox["legacy-done"]["expiresAt"] == expiry
    expired = cold.state.try_get_agent_response("legacy-done")
    assert expired is not None and expired.additional_properties["durable_status"] == "already_completed"
    assert len(client.received_messages) == 1 and hooks.calls == ["before", "after"]
    assert callback.mock_calls == calls and request == before_source


@pytest.mark.parametrize(
    "change", ["migrationId", "source", "sourceSessionId", "ownershipTransferId", "deliveryEvidence"]
)
def test_existing_migration_rejects_reused_identity_with_any_changed_request(clock: type[Clock], change: str) -> None:
    request = _request()
    store = Store()
    agent, client, hooks, callback = _agent()
    entity = AgentEntity(agent, state_provider=store, callback=callback)
    entity.migrate(request)
    changed = deepcopy(request)
    if change == "source":
        changed["source"]["futureRoot"]["keep"].append("different source")
        changed["sourceDigest"] = _digest(changed["source"])
    elif change == "sourceSessionId":
        changed[change] = "@dafx-maintenance@other-source"
        changed["source"]["data"]["session"]["session_id"] = changed[change]
        changed["sourceDigest"] = _digest(changed["source"])
    elif change == "deliveryEvidence":
        changed[change] = {
            "sourceDigest": changed["sourceDigest"],
            "evidenceId": "new-journal",
            "complete": True,
            "messages": [],
        }
    else:
        changed[change] += "-different"
    before = deepcopy(store.raw)
    cold_store = Store(before)
    cold = AgentEntity(agent, state_provider=cold_store, callback=callback)
    with pytest.raises(ValueError, match="empty|different migration"):
        cold.migrate(changed)
    assert cold_store.raw == before and cold_store.attempts == 0 and cold.state.to_dict() == before
    _quiet(client, hooks, callback)


def test_nonempty_destination_without_migration_is_never_overwritten(clock: type[Clock]) -> None:
    store = Store(_mailboxes())
    before = deepcopy(store.raw)
    agent, client, hooks, callback = _agent()
    with pytest.raises(ValueError, match="empty"):
        AgentEntity(agent, state_provider=store, callback=callback).migrate(_request())
    assert store.raw == before and store.attempts == 0
    _quiet(client, hooks, callback)


def test_failed_migration_commit_restores_warm_cache_and_retry_can_commit(clock: type[Clock]) -> None:
    store = Store()
    agent, client, hooks, callback = _agent()
    entity = AgentEntity(agent, state_provider=store, callback=callback)
    original = entity.state
    request = _request(evidence=True)
    before = deepcopy(request)
    store.fail_writes = True
    with pytest.raises(OSError, match="commit failure"):
        entity.migrate(request)
    assert entity.state is original and original.to_dict() == DurableAgentState().to_dict()
    assert store.raw == {} and store.writes == 0 and store.attempts == 1
    store.fail_writes = False
    assert entity.migrate(request)["status"] == "migrated"
    assert store.writes == 1 and store.attempts == 2 and request == before
    _quiet(client, hooks, callback)


def test_migration_budget_counts_full_destination_metadata_and_never_prunes(clock: type[Clock]) -> None:
    request = _request()
    request["source"]["data"]["conversationHistory"][0]["messages"][0]["contents"][0]["text"] = "雪" * 500
    request["sourceDigest"] = _digest(request["source"])
    before = deepcopy(request)
    agent, client, hooks, callback = _agent()
    sizing_store = Store()
    AgentEntity(agent, state_provider=sizing_store).migrate(request)
    full_size = _size(sizing_store.raw)
    assert full_size > _size(request["source"])
    expected_history = DurableAgentState.from_dict(request["source"]).to_dict()["data"]["conversationHistory"]
    assert sizing_store.raw["data"]["conversationHistory"] == expected_history

    rejected = Store()
    entity = AgentEntity(agent, state_provider=rejected, callback=callback, max_state_bytes=full_size - 1)
    cached = entity.state
    with pytest.raises(ValueError, match="max_state_bytes|capacity|budget"):
        entity.migrate(request)
    assert entity.state is cached and rejected.raw == {} and rejected.attempts == 0

    accepted = Store()
    AgentEntity(agent, state_provider=accepted, callback=callback, max_state_bytes=full_size).migrate(request)
    assert accepted.raw == sizing_store.raw and accepted.writes == 1
    assert "truncation" not in accepted.raw["data"] and request == before
    _quiet(client, hooks, callback)


async def test_external_get_and_save_keep_source_logical_identity_after_cold_run_and_retry(clock: type[Clock]) -> None:
    external = ExternalHistory()
    client: Any = RecordingChatClient()
    agent = Agent(client=client, name="maintenance", context_providers=[external])
    request = _request()
    before_request = deepcopy(request)
    external_before = {key: [message.to_dict() for message in messages] for key, messages in external.rows.items()}
    store = Store()
    AgentEntity(agent, state_provider=store).migrate(request)
    # The operator authorizes transfer outside this API; migration must not copy provider history.
    assert external.calls == []
    assert {
        key: [message.to_dict() for message in messages] for key, messages in external.rows.items()
    } == external_before
    for index in range(2):
        store = Store(store.raw)
        entity = AgentEntity(agent, state_provider=store)
        if index:
            before = deepcopy(store.raw)
            clock.current = NOW + timedelta(seconds=20)
            assert entity.migrate(request)["status"] == "migrated"
            assert store.raw == before and store.attempts == 0
        response = await entity.run({"message": f"destination turn {index}", "correlationId": f"new-{index}"})
        assert response.text == f"reply-{index + 1}"
        assert store.raw["data"]["session"]["session_id"] == SOURCE_ID
    assert external.calls == [(phase, SOURCE_ID) for _ in range(2) for phase in ("get", "save")]
    assert set(external.rows) == {SOURCE_ID} and store.core_session_id == DESTINATION_ID
    assert [message.text for message in external.rows[SOURCE_ID]] == [
        "already in the external store",
        "destination turn 0",
        "reply-1",
        "destination turn 1",
        "reply-2",
    ]
    assert "retained legacy input" not in [message.text for batch in client.received_messages for message in batch]
    assert request == before_request


@pytest.mark.parametrize("correlation", ["expired-success", "expired-error"])
async def test_expired_duplicate_run_removes_physical_payloads_without_model_or_hooks(
    clock: type[Clock], correlation: str
) -> None:
    raw = _mailboxes()
    before = deepcopy(raw)
    read_only = DurableAgentState.from_dict(raw)
    lookup = read_only.try_get_agent_response(correlation)
    assert lookup is not None and lookup.additional_properties["durable_status"] == "already_completed"
    assert read_only.to_dict() == before
    assert before["data"]["responseMailbox"][correlation]["response"]["response_id"] == f"response-{correlation}"
    store = Store(raw)
    agent, client, hooks, callback = _agent()
    entity = AgentEntity(agent, state_provider=store, callback=callback)

    response = await entity.run({"message": "duplicate", "correlationId": correlation})

    assert response.additional_properties == {"durable_status": "already_completed", "correlation_id": correlation}
    assert response.messages[0].contents[0].error_code == "response_expired"
    assert store.raw == _without_expired(before) and store.writes == store.attempts == 1
    assert entity.state.to_dict() == store.raw and raw == before
    _quiet(client, hooks, callback)


def test_idle_expiry_only_writes_for_removal_and_keeps_history_and_receipts_indefinitely(clock: type[Clock]) -> None:
    raw = _mailboxes()
    store = Store(raw)
    agent, client, hooks, callback = _agent()
    entity = AgentEntity(agent, state_provider=store, callback=callback)
    assert entity.expire_responses() == 2
    assert store.raw == _without_expired(raw)
    assert entity.expire_responses() == 0 and store.writes == store.attempts == 1
    live = entity.state.try_get_agent_response("live")
    assert live is not None and serialize_agent_response(live) == raw["data"]["responseMailbox"]["live"]["response"]

    clock.current = NOW + timedelta(days=36500)
    cold_store = Store(store.raw)
    cold = AgentEntity(agent, state_provider=cold_store, callback=callback)
    assert cold.expire_responses() == 1
    expected = _without_expired(raw)
    del expected["data"]["responseMailbox"]
    assert cold_store.raw == expected
    assert cold.expire_responses() == 0 and cold_store.writes == cold_store.attempts == 1
    for correlation in raw["data"]["completedCorrelations"]:
        result = cold.state.try_get_agent_response(correlation)
        assert result is not None and result.additional_properties["durable_status"] == "already_completed"
    _quiet(client, hooks, callback)


def test_expiry_commit_failure_restores_cache_and_payloads_before_successful_retry(clock: type[Clock]) -> None:
    raw = _mailboxes()
    store = Store(raw)
    agent, client, hooks, callback = _agent()
    entity = AgentEntity(agent, state_provider=store, callback=callback)
    cached = entity.state
    store.fail_writes = True
    with pytest.raises(OSError, match="commit failure"):
        entity.expire_responses()
    assert entity.state is cached and cached.to_dict() == raw
    assert store.raw == raw and store.writes == 0 and store.attempts == 1
    store.fail_writes = False
    assert entity.expire_responses() == 2
    assert store.raw == _without_expired(raw) and store.writes == 1
    _quiet(client, hooks, callback)


def test_expiry_budget_uses_whole_retained_floor_without_pruning_live_result_or_history(clock: type[Clock]) -> None:
    raw = _mailboxes()
    expected = _without_expired(raw)
    full_size = _size(expected)
    assert full_size > _size(expected["data"]["responseMailbox"])
    agent, client, hooks, callback = _agent()
    rejected = Store(raw)
    entity = AgentEntity(agent, state_provider=rejected, callback=callback, max_state_bytes=full_size - 1)
    cached = entity.state
    with pytest.raises(ValueError, match="max_state_bytes|capacity|budget"):
        entity.expire_responses()
    assert entity.state is cached and cached.to_dict() == raw
    assert rejected.raw == raw and rejected.attempts == 0

    accepted = Store(raw)
    entity = AgentEntity(agent, state_provider=accepted, callback=callback, max_state_bytes=full_size)
    assert entity.expire_responses() == 2
    assert accepted.raw == expected and accepted.writes == accepted.attempts == 1
    _quiet(client, hooks, callback)


def _registered(agent: Agent, callback: Mock, **settings: Any) -> Any:
    native = Mock()
    host = DurableAIAgentWorker(native, deployment_mode="isolated_v2", callback=callback, **settings)
    host.add_agent(agent)
    entity_type = native.add_entity.call_args.args[0]
    assert entity_type.__name__ == "dafx-maintenance"
    return entity_type


def _host_entity(entity_type: Any, store: Store) -> Any:
    context = Mock()
    context.entity_id = EntityInstanceId("dafx-maintenance", "destination")
    context.get_state.side_effect = lambda *args, **kwargs: store._get_state_dict()
    context.set_state.side_effect = store._set_state_dict
    entity = entity_type()
    entity._initialize_entity_context(context)
    assert isinstance(entity._agent_entity, AgentEntity)
    assert entity.core_session_id == DESTINATION_ID
    return entity


@pytest.mark.parametrize("operation", ["new-run", "duplicate-run", "reset", "expire_responses"])
def test_registered_dt_legacy_writer_guards_execute_actual_entity(operation: str) -> None:
    raw = _source()
    store = Store(raw)
    agent, client, hooks, callback = _agent()
    hosted = _host_entity(_registered(agent, callback), store)
    assert hosted.state.try_get_agent_response("legacy-done").text == "retained legacy answer"
    with pytest.raises(ValueError, match="[Ll]egacy.*read-only"):
        if operation.endswith("run"):
            hosted.run({
                "message": "blocked",
                "correlationId": "legacy-done" if operation == "duplicate-run" else "new",
            })
        else:
            getattr(hosted, operation)()
    assert store.raw == raw and store.attempts == 0
    _quiet(client, hooks, callback)


def test_registered_dt_migrate_cold_retry_and_expiry_use_configured_window(clock: type[Clock]) -> None:
    agent, client, hooks, callback = _agent()
    entity_type = _registered(agent, callback, response_delivery_window_seconds=17)
    store = Store()
    hosted = _host_entity(entity_type, store)
    request = _request(evidence=True)
    before_request = deepcopy(request)
    result = hosted.migrate(request)
    assert result == {"status": "migrated", "migrationId": "migration-1", "sessionId": DESTINATION_ID}
    assert store.raw["data"]["responseMailbox"]["legacy-done"]["expiresAt"] == (NOW + timedelta(seconds=17)).isoformat()
    _quiet(client, hooks, callback)
    assert hosted.run({"message": "v2 turn", "correlationId": "v2-done"})["type"] == "agent_response"
    before = deepcopy(store.raw)
    model_calls, hook_calls, callbacks = len(client.received_messages), list(hooks.calls), list(callback.mock_calls)
    assert model_calls == 1
    clock.current = NOW + timedelta(seconds=18)
    cold_store = Store(before)
    cold = _host_entity(entity_type, cold_store)
    assert cold.migrate(request) == result and cold_store.raw == before and cold_store.attempts == 0
    assert cold.expire_responses() == 2
    expected = deepcopy(before)
    del expected["data"]["responseMailbox"]
    assert cold_store.raw == expected
    assert cold.expire_responses() == 0 and cold_store.writes == cold_store.attempts == 1
    assert len(client.received_messages) == model_calls
    assert hooks.calls == hook_calls and callback.mock_calls == callbacks
    assert request == before_request


@pytest.mark.parametrize("operation", ["migrate", "expire_responses"])
def test_registered_dt_maintenance_commit_failure_rolls_back_actual_entity_cache(
    clock: type[Clock], operation: str
) -> None:
    raw = {} if operation == "migrate" else _mailboxes()
    store = Store(raw)
    agent, client, hooks, callback = _agent()
    hosted = _host_entity(_registered(agent, callback), store)
    cached = hosted.state
    request = _request()
    store.fail_writes = True
    with pytest.raises(OSError, match="commit failure"):
        hosted.migrate(request) if operation == "migrate" else hosted.expire_responses()
    assert hosted.state is cached
    assert cached.to_dict() == (raw or DurableAgentState().to_dict())
    assert store.raw == raw and store.writes == 0 and store.attempts == 1
    store.fail_writes = False
    result = hosted.migrate(request) if operation == "migrate" else hosted.expire_responses()
    expected = (
        {"status": "migrated", "migrationId": "migration-1", "sessionId": DESTINATION_ID}
        if operation == "migrate"
        else 2
    )
    assert result == expected
    assert store.writes == 1
    _quiet(client, hooks, callback)


@pytest.mark.parametrize("case", ["digest", "destination", "same-source", "blank-id", "nonempty", "changed-retry"])
def test_registered_dt_migration_rejections_leave_storage_and_execution_untouched(
    clock: type[Clock], case: str
) -> None:
    agent, client, hooks, callback = _agent()
    entity_type = _registered(agent, callback)
    store = Store(_mailboxes() if case == "nonempty" else None)
    hosted = _host_entity(entity_type, store)
    request = _request()
    if case == "changed-retry":
        hosted.migrate(request)
        hosted = _host_entity(entity_type, store)
        request["ownershipTransferId"] = "different-operator-transfer"
    elif case == "digest":
        request["sourceDigest"] = _digest(DurableAgentState.from_dict(request["source"]).to_dict())
    elif case == "destination":
        request["destinationSessionId"] = "@dafx-other@destination"
    elif case == "same-source":
        request["sourceSessionId"] = DESTINATION_ID
    elif case == "blank-id":
        request["migrationId"] = " \t"
    before, before_request, attempts = deepcopy(store.raw), deepcopy(request), store.attempts
    with pytest.raises(ValueError):
        hosted.migrate(request)
    assert store.raw == before and request == before_request and store.attempts == attempts
    _quiet(client, hooks, callback)


@pytest.mark.parametrize("operation", ["migrate", "expire_responses"])
def test_registered_dt_maintenance_budget_rejects_one_byte_short_accepts_full_floor(
    clock: type[Clock], operation: str
) -> None:
    raw = {} if operation == "migrate" else _mailboxes()
    request = _request()
    agent, client, hooks, callback = _agent()
    sizing_store = Store(raw)
    sizing = _host_entity(_registered(agent, callback), sizing_store)
    result = sizing.migrate(request) if operation == "migrate" else sizing.expire_responses()
    expected = deepcopy(sizing_store.raw)
    full_size = _size(expected)
    if operation == "migrate":
        normalized = DurableAgentState.from_dict(request["source"]).to_dict()
        assert expected["data"]["conversationHistory"] == normalized["data"]["conversationHistory"]
    else:
        assert expected == _without_expired(raw)
    rejected_store = Store(raw)
    rejected = _host_entity(_registered(agent, callback, max_state_bytes=full_size - 1), rejected_store)
    cached = rejected.state
    with pytest.raises(ValueError, match="max_state_bytes|capacity|budget"):
        rejected.migrate(request) if operation == "migrate" else rejected.expire_responses()
    assert rejected.state is cached and rejected_store.raw == raw and rejected_store.attempts == 0
    accepted_store = Store(raw)
    accepted = _host_entity(_registered(agent, callback, max_state_bytes=full_size), accepted_store)
    actual = accepted.migrate(request) if operation == "migrate" else accepted.expire_responses()
    assert actual == result and accepted_store.raw == expected and accepted_store.writes == 1
    _quiet(client, hooks, callback)


@pytest.mark.parametrize("correlation", ["expired-success", "expired-error"])
def test_registered_dt_expired_duplicate_removes_mailbox_without_execution(
    clock: type[Clock], correlation: str
) -> None:
    raw = _mailboxes()
    agent, client, hooks, callback = _agent()
    store = Store(raw)
    hosted = _host_entity(_registered(agent, callback), store)
    result = hosted.run({"message": "duplicate", "correlationId": correlation})
    assert result["additional_properties"] == {"durable_status": "already_completed", "correlation_id": correlation}
    assert result["messages"][0]["contents"][0]["error_code"] == "response_expired"
    assert store.raw == _without_expired(raw) and store.writes == 1
    _quiet(client, hooks, callback)


def test_registered_dt_external_identity_survives_cold_destination_and_migration_retry(clock: type[Clock]) -> None:
    external = ExternalHistory()
    agent, client, hooks, callback = _agent()
    agent.context_providers = [external, hooks]
    entity_type = _registered(agent, callback)
    store = Store()
    request = _request()
    before_request = deepcopy(request)
    external_before = {key: [message.to_dict() for message in messages] for key, messages in external.rows.items()}
    _host_entity(entity_type, store).migrate(request)
    assert external.calls == []
    assert {
        key: [message.to_dict() for message in messages] for key, messages in external.rows.items()
    } == external_before
    for index in range(2):
        store = Store(store.raw)
        hosted = _host_entity(entity_type, store)
        if index:
            before = deepcopy(store.raw)
            assert hosted.migrate(request)["status"] == "migrated"
            assert store.raw == before and store.attempts == 0
        result = hosted.run({"message": f"new turn {index}", "correlationId": f"new-{index}"})
        assert result["type"] == "agent_response"
        assert store.raw["data"]["session"]["session_id"] == SOURCE_ID
    assert external.calls == [(phase, SOURCE_ID) for _ in range(2) for phase in ("get", "save")]
    assert [message.text for message in external.rows[SOURCE_ID]] == [
        "already in the external store",
        "new turn 0",
        "reply-1",
        "new turn 1",
        "reply-2",
    ]
    assert set(external.rows) == {SOURCE_ID} and request == before_request
    assert "retained legacy input" not in [message.text for batch in client.received_messages for message in batch]
