# Copyright (c) Microsoft. All rights reserved.

"""Committed retry identities must agree with the complete original migration request."""

from __future__ import annotations

import json
from collections.abc import Callable
from copy import deepcopy
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from _execution_test_support import JsonStateProvider, RecordingChatClient
from _migration_test_support import (
    SOURCE_SESSION_ID,
    _completion_journal,
    _error_response_entry,
    _json_provider_entity,
    _legacy_source,
    _migration_request,
    _ObservedExternalHistory,
    _original_result,
    _sdk_registered_entity,
)
from agent_framework import Agent, AgentResponse, Message

from agent_framework_durabletask import AgentEntity, state_snapshot_digest

_Host = tuple[AgentEntity, JsonStateProvider, RecordingChatClient, _ObservedExternalHistory]
_Retry = tuple[AgentEntity, JsonStateProvider, RecordingChatClient, _ObservedExternalHistory, dict[str, Any]]
_BINDING_ERROR = "Committed migration binding does not match the retry request"
_SESSION_BINDING_ERROR = "Committed migration session binding is invalid"


def _host(raw: dict[str, Any] | None = None) -> _Host:
    external = _ObservedExternalHistory()
    client = RecordingChatClient()
    entity, provider, _ = _json_provider_entity(
        raw=raw,
        agent=Agent(client=client, name="migration-agent", context_providers=[external]),
    )
    return entity, provider, client, external


@pytest.fixture(params=["warm", "cold"])
def retry_host(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> Callable[..., _Retry]:
    def create(*, journals: str = "populated", mutate: Callable[[dict[str, Any]], None] | None = None) -> _Retry:
        assert journals in ("omitted", "null", "empty", "populated", "delivery-only", "completion-only")
        source = _legacy_source(_error_response_entry()) if journals == "populated" else _legacy_source()
        entity, provider, client, external = _host()
        migration = _migration_request(source, provider.core_session_id)
        if journals in ("empty", "populated", "delivery-only"):
            migration["deliveryEvidence"] = {
                "sourceDigest": state_snapshot_digest(source),
                "evidenceId": "delivery-journal-1",
                "complete": True,
                "messages": [Message("user", ["accepted input"], message_id="accepted-1").to_dict()]
                if journals == "populated"
                else [],
            }

        if journals in ("empty", "populated", "completion-only"):
            migration["completionEvidence"] = (
                _completion_journal(source, _original_result())
                if journals == "populated"
                else _completion_journal(source)
            )
        if journals == "null":
            migration.update({"deliveryEvidence": None, "completionEvidence": None})
        original_request = deepcopy(migration)
        assert entity.migrate(migration)["status"] == "migrated"
        assert migration == original_request
        assert external.calls == client.received_messages == []
        assert provider.attempted_writes == provider.successful_writes == 1

        if request.param == "cold":
            raw = json.loads(json.dumps(provider.raw, allow_nan=False))
            if mutate is not None:
                mutate(raw["data"]["migration"])
            entity, provider, client, external = _host(raw)
            assert provider._state_cache is None
        elif mutate is not None:
            mutate(provider.state.data.unknown_fields["migration"])

        # Count only writes attempted by the retry, not the initial migration.
        provider.attempted_writes = provider.successful_writes = 0

        def forbidden(*args: Any, **kwargs: Any) -> None:
            pytest.fail("A committed retry must not re-stage the source or recalculate its grace.")

        monkeypatch.setattr("agent_framework_durabletask._entities.migrate_legacy_state", forbidden)
        return entity, provider, client, external, migration

    return create


def _assert_retry_rejected(retry: _Retry, message: str) -> None:
    entity, provider, client, external, request = retry
    backing_before = deepcopy(provider.raw)
    cache_before = provider._state_cache
    snapshot_before = deepcopy(provider._persisted_state_snapshot)
    expected_state = cache_before.to_dict() if cache_before is not None else deepcopy(backing_before)
    request_before = deepcopy(request)
    assert expected_state["data"]["migration"]["requestDigest"] == state_snapshot_digest(request)

    if cache_before is not None and message == _BINDING_ERROR:
        # Warm mutations now fail against the committed snapshot at entry. Cold
        # state still needs the original request to detect a consistent rebind.
        message = "Committed migration binding fields cannot be removed or changed."
    with pytest.raises(ValueError, match=message):
        entity.migrate(request)

    assert provider.raw == backing_before
    assert provider.state.to_dict() == expected_state
    if cache_before is not None:
        assert provider.state is cache_before
        assert provider._persisted_state_snapshot == snapshot_before
    else:
        assert provider._persisted_state_snapshot == backing_before
    assert provider.attempted_writes == provider.successful_writes == 0
    assert external.calls == client.received_messages == []
    assert request == request_before


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("id", "another-migration"),
        ("sourceDigest", "b" * 64),
        ("sourceSessionId", "another-provider:source-session"),
        ("ownershipTransferId", "another-transfer"),
    ],
)
def test_matching_digest_rejects_changed_committed_identity(
    retry_host: Callable[..., _Retry], field: str, replacement: str
) -> None:
    retry = retry_host(mutate=lambda binding: binding.update({field: replacement}))
    # The stored session cross-check now rejects a changed source before retry admission.
    _assert_retry_rejected(retry, _SESSION_BINDING_ERROR if field == "sourceSessionId" else _BINDING_ERROR)


def test_consistently_changed_source_still_rejects_original_retry(retry_host: Callable[..., _Retry]) -> None:
    replacement = "another-provider:source-session"
    retry = retry_host(mutate=lambda binding: binding.update({"sourceSessionId": replacement}))
    _, provider, _, _, _ = retry
    if provider._state_cache is None:
        provider.raw["data"]["session"]["session_id"] = replacement
    else:
        assert provider.state.data.session is not None
        provider.state.data.session["session_id"] = replacement

    # Agreement between stored identities does not establish agreement with the original request.
    _assert_retry_rejected(retry, _BINDING_ERROR)


@pytest.mark.parametrize("journals", ["empty", "populated"])
@pytest.mark.parametrize("field", ["evidenceId", "completionEvidenceId"])
@pytest.mark.parametrize("change", ["replace", "remove"])
def test_matching_digest_requires_every_supplied_evidence_id(
    retry_host: Callable[..., _Retry], journals: str, field: str, change: str
) -> None:
    def mutate(binding: dict[str, Any]) -> None:
        assert field in binding
        if change == "remove":
            del binding[field]
        else:
            binding[field] = "another-evidence-journal"

    _assert_retry_rejected(retry_host(journals=journals, mutate=mutate), _BINDING_ERROR)


@pytest.mark.parametrize("journals", ["omitted", "null"])
@pytest.mark.parametrize("field", ["evidenceId", "completionEvidenceId"])
def test_matching_digest_rejects_evidence_id_absent_from_request(
    retry_host: Callable[..., _Retry], journals: str, field: str
) -> None:
    def mutate(binding: dict[str, Any]) -> None:
        assert field not in binding
        binding[field] = "unrequested-evidence-journal"

    _assert_retry_rejected(retry_host(journals=journals, mutate=mutate), _BINDING_ERROR)


def test_wrong_committed_destination_is_rejected_by_runtime_preoperation_guard(
    retry_host: Callable[..., _Retry],
) -> None:
    retry = retry_host(mutate=lambda binding: binding.update({"destinationSessionId": "@other@destination"}))
    _assert_retry_rejected(retry, "Committed migration session binding is invalid")


@pytest.mark.parametrize("cold", [False, True], ids=["warm", "cold"])
def test_registered_backend_migrate_rejects_changed_source_binding_without_write(
    cold: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    entity, shim, client, _ = _sdk_registered_entity()
    request = _migration_request(_legacy_source(), entity.core_session_id)
    request_before = deepcopy(request)
    assert entity.migrate(request)["status"] == "migrated"
    committed = shim.encode_state()
    assert committed is not None
    raw = json.loads(committed)
    if cold:
        raw["data"]["migration"]["sourceSessionId"] = "another-provider:source-session"
        entity, shim, client, _ = _sdk_registered_entity(json.dumps(raw, allow_nan=False))
        assert entity._state_cache is None
    else:
        entity.state.data.unknown_fields["migration"]["sourceSessionId"] = "another-provider:source-session"
    before = shim.encode_state()
    warm = entity._state_cache
    expected_state = warm.to_dict() if warm is not None else raw
    write = Mock(wraps=entity._set_state_dict)
    monkeypatch.setattr(entity, "_set_state_dict", write)

    with pytest.raises(ValueError, match=_SESSION_BINDING_ERROR):
        entity.migrate(request)

    write.assert_not_called()
    assert shim.encode_state() == before
    assert entity.state.to_dict() == expected_state
    if warm is not None:
        assert entity.state is warm
    assert entity.state.data.unknown_fields["migration"]["requestDigest"] == state_snapshot_digest(request)
    assert client.received_messages == []
    assert request == request_before


@pytest.mark.parametrize("cold", [False, True], ids=["warm", "cold"])
@pytest.mark.parametrize(
    "operation", ["run", "run-no-context", "create-session", "reset", "expire_responses", "migrate"]
)
@pytest.mark.parametrize(
    "change",
    ["metadata-source", "other-id", "null-id", "empty-id", "blank-id", "number-id", "missing-id", "empty-session"],
)
async def test_stored_session_binding_rejects_before_operation_without_mutation(
    cold: bool, operation: str, change: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    entity, provider, client, external = _host()
    request = _migration_request(_legacy_source(), provider.core_session_id)
    assert entity.migrate(request)["status"] == "migrated"
    raw = deepcopy(provider.raw)
    if change == "metadata-source":
        raw["data"]["migration"]["sourceSessionId"] = "another-provider:source-session"
    elif change == "empty-session":
        raw["data"]["session"] = {}
    elif change == "missing-id":
        # Even nonempty opaque session state cannot substitute for its identity.
        raw["data"]["session"] = {"state": {"external": {"key": "original"}}}
    else:
        raw["data"]["session"]["session_id"] = {
            "other-id": "another-provider:source-session",
            "null-id": None,
            "empty-id": "",
            "blank-id": " \t\n",
            "number-id": 7,
        }[change]
    if cold:
        entity, provider, client, external = _host(raw)
        assert provider._state_cache is None
    else:
        provider.state.data.session = deepcopy(raw["data"]["session"])
        provider.state.data.unknown_fields["migration"] = deepcopy(raw["data"]["migration"])
    provider.attempted_writes = provider.successful_writes = 0
    backing_before = deepcopy(provider.raw)
    cache_before = provider._state_cache
    snapshot_before = deepcopy(provider._persisted_state_snapshot)
    request_before = deepcopy(request)
    create_session = Mock(wraps=entity.agent.create_session)
    monkeypatch.setattr(entity.agent, "create_session", create_session)
    no_context = Mock(spec=["run"])
    no_context.run = AsyncMock(return_value=AgentResponse(messages=[Message("assistant", ["unexpected"])]))
    if operation == "run-no-context":
        entity = AgentEntity(no_context, state_provider=provider)
        assert not entity._has_context_pipeline()

    with pytest.raises(ValueError, match=_SESSION_BINDING_ERROR):
        if operation in ("run", "run-no-context"):
            await entity.run({"message": "follow-up", "correlationId": "invalid-session"})
        elif operation == "create-session":
            entity._create_session()
        elif operation == "migrate":
            entity.migrate(request)
        else:
            getattr(entity, operation)()

    assert provider.raw == backing_before
    assert provider.state.to_dict() == raw
    if cache_before is not None:
        assert provider.state is cache_before
        assert provider._persisted_state_snapshot == snapshot_before
    else:
        assert provider._persisted_state_snapshot == backing_before
    assert provider.attempted_writes == provider.successful_writes == 0
    assert external.calls == client.received_messages == []
    create_session.assert_not_called()
    no_context.run.assert_not_called()
    assert request == request_before


@pytest.mark.parametrize("stored", [[], "session", 7, False])
def test_nonobject_warm_migrated_session_is_rejected_without_repair(stored: Any) -> None:
    entity, provider, client, external = _host()
    request = _migration_request(_legacy_source(), provider.core_session_id)
    entity.migrate(request)
    committed = deepcopy(provider.raw)
    warm = provider.state
    warm.data.session = stored

    with pytest.raises(ValueError, match=_SESSION_BINDING_ERROR):
        provider.migration_session_id()

    assert provider.state is warm
    assert warm.data.session is stored
    assert warm.data.unknown_fields["migration"] == committed["data"]["migration"]
    assert provider.raw == committed
    assert provider.attempted_writes == provider.successful_writes == 1
    assert external.calls == client.received_messages == []


@pytest.mark.parametrize("cold", [False, True], ids=["warm", "cold"])
async def test_reset_clears_local_session_but_preserves_migration_identity(cold: bool) -> None:
    # Reset with durable local history is supported. External clears are provider-owned.
    entity, provider, client = _json_provider_entity()
    request = _migration_request(_legacy_source(), provider.core_session_id)
    assert entity.migrate(request)["status"] == "migrated"
    metadata = deepcopy(provider.raw["data"]["migration"])
    entity.reset()
    assert provider.state.data.session is None
    assert "session" not in provider.raw["data"]
    assert provider.raw["data"]["migration"] == metadata
    assert client.received_messages == []
    if cold:
        entity, provider, client = _json_provider_entity(json.loads(json.dumps(provider.raw, allow_nan=False)))
    reset_state = deepcopy(provider.raw)
    writes = (provider.attempted_writes, provider.successful_writes)
    assert entity.expire_responses() == 0
    assert entity.migrate(request)["status"] == "migrated"
    assert entity._create_session().session_id == SOURCE_SESSION_ID
    assert provider.state.data.session is None
    assert provider.raw == reset_state
    assert (provider.attempted_writes, provider.successful_writes) == writes

    response = await entity.run({"message": "after reset", "correlationId": "after-reset"})

    assert response.text == "reply-1"
    assert len(client.received_messages) == 1
    assert provider.raw["data"]["session"]["session_id"] == SOURCE_SESSION_ID
    assert provider.raw["data"]["migration"] == metadata
    # A later cold external-history run must still use the original source key.
    entity, provider, client, external = _host(json.loads(json.dumps(provider.raw, allow_nan=False)))
    response = await entity.run({"message": "continued", "correlationId": "after-reset-external"})
    assert response.text == "reply-1"
    assert external.calls == [("load", SOURCE_SESSION_ID), ("save", SOURCE_SESSION_ID)]
    assert provider.raw["data"]["session"]["session_id"] == SOURCE_SESSION_ID
    assert provider.raw["data"]["migration"] == metadata


@pytest.mark.parametrize("field", ["source", "ownershipTransferId", "deliveryEvidence", "completionEvidence"])
def test_changed_request_still_rejects_nonempty_destination(retry_host: Callable[..., _Retry], field: str) -> None:
    entity, provider, client, external, request = retry_host()
    changed = deepcopy(request)
    if field == "source":
        changed[field]["exportExtension"] = {"changed": True}
    elif field == "ownershipTransferId":
        changed[field] = "different-request-transfer"
    else:
        changed[field]["evidenceId"] = "different-request-journal"
    before = deepcopy(provider.raw)
    cache = provider._state_cache

    with pytest.raises(ValueError, match="Migration destination must be empty"):
        entity.migrate(changed)

    assert provider.raw == before
    assert provider.state.to_dict() == before
    if cache is not None:
        assert provider.state is cache
    assert provider.attempted_writes == provider.successful_writes == 0
    assert external.calls == client.received_messages == []
    assert state_snapshot_digest(request) == before["data"]["migration"]["requestDigest"]


@pytest.mark.parametrize("journals", ["omitted", "null", "empty", "populated", "delivery-only", "completion-only"])
async def test_exact_retry_preserves_binding_and_grace_across_two_later_runs(
    retry_host: Callable[..., _Retry], journals: str
) -> None:
    entity, provider, client, external, request = retry_host(journals=journals)
    original_request = deepcopy(request)
    committed = deepcopy(provider.raw)
    metadata = committed["data"]["migration"]
    if journals in ("empty", "populated", "delivery-only"):
        assert metadata["evidenceId"] == "delivery-journal-1"
    else:
        assert "evidenceId" not in metadata
    if journals in ("empty", "populated", "completion-only"):
        assert metadata["completionEvidenceId"] == "completion-journal-1"
    else:
        assert "completionEvidenceId" not in metadata
    expected = {"status": "migrated", "migrationId": "migration-1", "sessionId": provider.core_session_id}

    assert entity.migrate(request) == expected
    assert provider.raw == committed
    assert provider.state.to_dict() == committed
    assert provider.attempted_writes == provider.successful_writes == 0
    assert external.calls == client.received_messages == []

    for correlation_id in ("after-migration-1", "after-migration-2"):
        response = await entity.run({"message": "follow-up", "correlationId": correlation_id})
        assert response.text == "reply-1"
        assert len(client.received_messages) == 1
        assert external.calls == [("load", SOURCE_SESSION_ID), ("save", SOURCE_SESSION_ID)]
        assert provider.raw["data"]["session"]["session_id"] == SOURCE_SESSION_ID
        assert provider.raw["data"]["migration"] == metadata
        if journals == "populated":
            assert provider.raw["data"]["terminalResults"]["done"] == committed["data"]["terminalResults"]["done"]
            assert provider.raw["data"]["completionReceipts"]["done"] == committed["data"]["completionReceipts"]["done"]
        after_run = deepcopy(provider.raw)
        cache = provider.state
        writes = (provider.attempted_writes, provider.successful_writes)
        assert entity.migrate(request) == expected
        assert provider.raw == after_run
        assert provider.state is cache
        assert provider.state.to_dict() == after_run
        assert (provider.attempted_writes, provider.successful_writes) == writes
        assert len(client.received_messages) == 1
        assert external.calls == [("load", SOURCE_SESSION_ID), ("save", SOURCE_SESSION_ID)]
        entity, provider, client, external = _host(json.loads(json.dumps(provider.raw, allow_nan=False)))

    assert request == original_request
