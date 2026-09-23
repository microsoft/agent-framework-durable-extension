# Copyright (c) Microsoft. All rights reserved.

"""Public writers preserve committed migration identity, not arbitrary metadata."""

from __future__ import annotations

import asyncio
import json
from copy import copy, deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import Mock

import pytest
from _execution_test_support import JsonStateProvider, RecordingChatClient
from agent_framework import Agent
from test_migration_host_boundaries import (
    ENTITY_NAME,
    SOURCE_SESSION_ID,
    _completion_journal,
    _error_response_entry,
    _legacy_source,
    _migration_request,
    _ObservedExternalHistory,
    _original_result,
)
from typing_extensions import Self

from agent_framework_durabletask import AgentEntity, DurableAgentState

NOW = datetime(2026, 9, 23, 12, tzinfo=timezone.utc)
WINDOW = 3600


@pytest.fixture(autouse=True)
def migration_clock(monkeypatch: pytest.MonkeyPatch) -> Any:
    class Clock(datetime):
        current = NOW

        @classmethod
        def now(cls, tz: Any = None) -> Self:
            return cls.fromtimestamp(cls.current.timestamp(), tz)

    for module in ("_state_migration", "_delivery_state", "_shared_state_validation"):
        monkeypatch.setattr(f"agent_framework_durabletask.{module}.datetime", Clock)
    return Clock


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":"))


def _host(
    raw: dict[str, Any] | None = None, *, external: _ObservedExternalHistory | None = None
) -> tuple[AgentEntity, JsonStateProvider, RecordingChatClient]:
    client = RecordingChatClient()
    provider = JsonStateProvider(raw, session_id="dest-session", entity_name=ENTITY_NAME)
    agent = Agent(client=client, name="migration-agent", context_providers=[external] if external is not None else [])
    entity = AgentEntity(agent, state_provider=provider, response_delivery_window_seconds=WINDOW)
    return entity, provider, client


def _migrated(
    *, journals: bool = True, external: _ObservedExternalHistory | None = None
) -> tuple[AgentEntity, JsonStateProvider, RecordingChatClient, dict[str, Any]]:
    entity, provider, client = _host(external=external)
    source = _legacy_source(_error_response_entry()) if journals else _legacy_source()
    if journals:
        source["data"]["session"] = {
            "session_id": SOURCE_SESSION_ID,
            "state": {"application": {"values": [False, 0, 0.0]}},
        }
    request = _migration_request(source, provider.core_session_id)
    if journals:
        request["completionEvidence"] = _completion_journal(source, _original_result())
        request["deliveryEvidence"] = {
            "sourceDigest": request["sourceDigest"],
            "evidenceId": "delivery-journal-1",
            "complete": True,
            "messages": [],
        }
    before = _json(request)
    assert entity.migrate(request) == {
        "status": "migrated",
        "migrationId": "migration-1",
        "sessionId": provider.core_session_id,
    }
    assert _json(request) == before
    assert provider.attempted_writes == provider.successful_writes == 1
    assert provider.raw["data"]["session"]["session_id"] == SOURCE_SESSION_ID
    assert client.received_messages == []
    return entity, provider, client, request


@pytest.mark.parametrize("change", ["delete-binding", "rebind"])
@pytest.mark.parametrize("aliased", [True, False], ids=["warm-cache", "detached-unsaved"])
async def test_warm_run_checks_committed_binding_before_history_or_model(change: str, aliased: bool) -> None:
    external = _ObservedExternalHistory()
    entity, provider, client, _ = _migrated(external=external)
    committed = _json(provider.raw)
    binding = deepcopy(provider.raw["data"]["migration"])
    warm = entity.state
    candidate = warm if aliased else deepcopy(warm)
    if change == "delete-binding":
        del candidate.data.unknown_fields["migration"]
    else:
        assert candidate.data.session is not None
        candidate.data.session["session_id"] = "another-provider:session"
        candidate.data.unknown_fields["migration"]["sourceSessionId"] = "another-provider:session"
    candidate_before = _json(candidate.to_dict())
    assert candidate_before != committed
    assert entity.state is warm
    assert _json(provider.raw) == committed
    assert external.calls == client.received_messages == []

    # No setter or explicit persist: the real entry point must reject before effects,
    # not just reject the final write after another provider key has already been used.
    request = {"message": "continue", "correlationId": "warm-binding-check"}
    if aliased:
        with pytest.raises(ValueError, match="Committed migration binding fields cannot be removed or changed"):
            await entity.run(request)
        assert entity.state is warm
        assert _json(entity.state.to_dict()) == candidate_before
        assert _json(provider.raw) == committed
        assert provider.attempted_writes == provider.successful_writes == 1
        assert external.calls == client.received_messages == client.received_options == []
    else:
        # Editing a detached value is not a state transition and must not block a run.
        assert _json(warm.to_dict()) == committed
        assert (await entity.run(request)).text == "reply-1"
        assert external.calls == [("load", SOURCE_SESSION_ID), ("save", SOURCE_SESSION_ID)]
        assert [[message.text for message in messages] for messages in client.received_messages] == [["continue"]]
        assert provider.raw["data"]["session"]["session_id"] == SOURCE_SESSION_ID
        assert _json(provider.raw["data"]["migration"]) == _json(binding)
        assert provider.attempted_writes == provider.successful_writes == 2
    assert _json(candidate.to_dict()) == candidate_before


@pytest.mark.parametrize("change", ["delete-binding", "restore-valid-binding"])
async def test_warm_cache_cannot_hide_invalid_committed_binding(change: str) -> None:
    _, donor, _, _ = _migrated()
    raw = deepcopy(donor.raw)
    raw["data"]["migration"]["requestDigest"] = "invalid"
    external = _ObservedExternalHistory()
    entity, provider, client = _host(raw, external=external)
    committed = _json(provider.raw)
    warm = entity.state  # Establish the committed snapshot before changing only the cache.
    if change == "delete-binding":
        del warm.data.unknown_fields["migration"]
    else:
        warm.data.unknown_fields["migration"] = deepcopy(donor.raw["data"]["migration"])
    candidate_before = _json(warm.to_dict())
    assert candidate_before != committed

    with pytest.raises(ValueError, match="Committed migration session binding is invalid"):
        await entity.run({"message": "continue", "correlationId": "invalid-committed-binding"})

    assert entity.state is warm
    assert _json(warm.to_dict()) == candidate_before
    assert _json(provider.raw) == committed
    assert provider.attempted_writes == provider.successful_writes == 0
    assert external.calls == client.received_messages == client.received_options == []


@pytest.mark.parametrize("destination", ["absent", "initialized-empty", "migrated"])
def test_warm_session_identity_checks_only_binding_projection(
    destination: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    if destination == "migrated":
        entity, provider, _, _ = _migrated()
        expected = SOURCE_SESSION_ID
    else:
        entity, provider, _ = _host(DurableAgentState().to_dict() if destination == "initialized-empty" else None)
        expected = provider.core_session_id
    warm = entity.state
    committed = _json(provider.raw)
    writes = (provider.attempted_writes, provider.successful_writes)
    warm.data.unknown_fields["application"] = {"pending": [False, 0, 0.0]}
    if destination == "migrated":
        warm.data.unknown_fields["migration"]["operatorNotes"] = {"pending": [False, 0, 0.0]}
    pending = _json(warm.to_dict())

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        pytest.fail("Warm identity checks must not copy, serialize, or reread the complete state.")

    with monkeypatch.context() as guard:
        guard.setattr(warm, "to_dict", forbidden)
        guard.setattr(provider, "_get_state_dict", forbidden)
        guard.setattr("agent_framework_durabletask._entities.deepcopy", forbidden)
        assert provider.migration_session_id() == expected
        assert provider.migration_session_id() == expected

    assert entity.state is warm
    assert _json(warm.to_dict()) == pending
    assert _json(provider.raw) == committed
    assert (provider.attempted_writes, provider.successful_writes) == writes


@pytest.mark.parametrize("writer", ["detached-setter", "aliased-setter", "shallow-setter", "persist"])
@pytest.mark.parametrize("change", ["delete-binding", "rebind", "session-only"])
async def test_public_write_rejects_changed_committed_binding_before_write(writer: str, change: str) -> None:
    entity, provider, client, _ = _migrated()
    committed = _json(provider.raw)
    original = entity.state
    candidate = (
        deepcopy(original)
        if writer == "detached-setter"
        else copy(original)
        if writer == "shallow-setter"
        else original
    )
    if writer == "shallow-setter":
        assert candidate is not original and candidate.data is original.data
    if change == "delete-binding":
        del candidate.data.unknown_fields["migration"]
    else:
        assert candidate.data.session is not None
        candidate.data.session["session_id"] = "another-provider:session"
        if change == "rebind":
            candidate.data.unknown_fields["migration"]["sourceSessionId"] = "another-provider:session"
    # Each candidate is valid shared JSON. Only the binding transition is invalid.
    candidate_before = _json(candidate.to_dict())
    assert candidate_before != committed
    with pytest.raises(ValueError, match="[Mm]igration"):
        if writer == "persist":
            entity.persist_state()
        else:
            entity.state = candidate

    assert _json(provider.raw) == _json(provider._persisted_state_snapshot) == committed
    assert _json(entity.state.to_dict()) == committed
    assert provider._persisted_state_snapshot is not provider.raw
    assert provider.attempted_writes == provider.successful_writes == 1
    assert not provider._write_acknowledgement_uncertain
    assert _json(candidate.to_dict()) == candidate_before
    assert (entity.state is original) is (writer == "detached-setter")
    assert client.received_messages == []

    external = _ObservedExternalHistory()
    cold, backing, _ = _host(json.loads(committed), external=external)
    assert (await cold.run({"message": "continue", "correlationId": "after-rejection"})).text == "reply-1"
    assert external.calls == [("load", SOURCE_SESSION_ID), ("save", SOURCE_SESSION_ID)]
    assert backing.raw["data"]["session"]["session_id"] == SOURCE_SESSION_ID


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("id", "migration-2"),
        ("sourceDigest", "b" * 64),
        ("sourceSessionId", "another-provider:session"),
        ("ownershipTransferId", "transfer-2"),
        ("createdAt", "2026-09-23T12:00:00Z"),  # Same instant, different committed representation.
        ("requestDigest", "c" * 64),
        ("destinationSessionId", "@another@destination"),
        ("evidenceId", "delivery-journal-2"),
        ("completionEvidenceId", "completion-journal-2"),
    ],
)
@pytest.mark.parametrize("change", ["replace", "remove"])
def test_each_committed_binding_field_is_immutable(field: str, replacement: str, change: str) -> None:
    entity, provider, _, _ = _migrated()
    committed = _json(provider.raw)
    candidate = deepcopy(entity.state)
    binding = candidate.data.unknown_fields["migration"]
    assert binding[field] != replacement
    if change == "remove":
        del binding[field]
    else:
        binding[field] = replacement
        if field == "sourceSessionId":
            assert candidate.data.session is not None
            candidate.data.session["session_id"] = replacement
    before = _json(candidate.to_dict())
    with pytest.raises(ValueError, match="[Mm]igration"):
        entity.state = candidate
    assert _json(provider.raw) == _json(provider._persisted_state_snapshot) == committed
    assert _json(entity.state.to_dict()) == committed
    assert _json(candidate.to_dict()) == before
    assert provider.attempted_writes == provider.successful_writes == 1


@pytest.mark.parametrize("field", ["evidenceId", "completionEvidenceId"])
def test_absent_committed_evidence_id_cannot_be_added(field: str) -> None:
    entity, provider, _, _ = _migrated(journals=False)
    committed = _json(provider.raw)
    assert field not in entity.state.data.unknown_fields["migration"]
    entity.state.data.unknown_fields["migration"][field] = "unrequested-journal"
    with pytest.raises(ValueError, match="migration binding fields"):
        entity.persist_state()
    assert _json(provider.raw) == _json(provider._persisted_state_snapshot) == committed
    assert _json(entity.state.to_dict()) == committed
    assert provider.attempted_writes == provider.successful_writes == 1


@pytest.mark.parametrize("writer", ["setter", "persist", "shallow-setter"])
def test_cold_writer_reads_committed_binding_instead_of_replacement_cache(writer: str) -> None:
    _, donor, _, _ = _migrated()
    entity, provider, client = _host(donor.raw)
    committed = _json(provider.raw)
    candidate = DurableAgentState.from_dict(donor.raw)
    assert provider._state_cache is provider._persisted_state_snapshot is None
    if writer == "shallow-setter":
        provider.replace_cached_state(candidate)
        candidate = copy(candidate)
        assert candidate is not provider._state_cache and candidate.data is provider.state.data
        assert provider._persisted_state_snapshot is None
    del candidate.data.unknown_fields["migration"]
    with pytest.raises(ValueError, match="migration binding fields"):
        if writer == "persist":
            provider.replace_cached_state(candidate)
            entity.persist_state()
        else:
            entity.state = candidate
    assert _json(provider.raw) == _json(entity.state.to_dict()) == committed
    assert _json(provider._persisted_state_snapshot) == committed
    assert provider.attempted_writes == provider.successful_writes == 0
    assert client.received_messages == []


def test_rejected_detached_candidate_keeps_unrelated_pending_cache_edits() -> None:
    entity, provider, client, _ = _migrated()
    committed = _json(provider.raw)
    pending = entity.state
    pending.data.unknown_fields["migration"]["operatorNotes"] = {"pending": [False, 0, 0.0]}
    candidate = deepcopy(pending)
    candidate.data.unknown_fields["migration"]["id"] = "migration-2"
    pending_before = _json(pending.to_dict())
    with pytest.raises(ValueError, match="migration binding fields"):
        entity.state = candidate
    assert entity.state is pending
    assert _json(pending.to_dict()) == pending_before
    assert _json(provider.raw) == _json(provider._persisted_state_snapshot) == committed
    assert provider.attempted_writes == provider.successful_writes == 1
    entity.persist_state()
    assert _json(provider.raw) == pending_before
    assert provider.attempted_writes == provider.successful_writes == 2
    assert client.received_messages == []


def test_migrate_can_initialize_a_committed_empty_v2_destination() -> None:
    entity, provider, client = _host()
    entity.persist_state()
    assert provider.raw == DurableAgentState().to_dict()
    request = _migration_request(_legacy_source(), provider.core_session_id)
    assert entity.migrate(request)["status"] == "migrated"
    assert provider.raw["data"]["session"]["session_id"] == SOURCE_SESSION_ID
    assert provider.raw["data"]["migration"]["destinationSessionId"] == provider.core_session_id
    assert provider.attempted_writes == provider.successful_writes == 2
    assert getattr(provider, "_migration_candidate", None) is None
    committed = _json(provider.raw)
    assert entity.migrate(request)["status"] == "migrated"
    assert _json(provider.raw) == committed
    assert provider.attempted_writes == provider.successful_writes == 2
    assert client.received_messages == []


@pytest.mark.parametrize("writer", ["setter", "persist"])
@pytest.mark.parametrize("destination", ["fresh", "initialized-empty", "used"])
def test_public_write_cannot_install_a_valid_binding_from_another_migration(writer: str, destination: str) -> None:
    _, donor, _, _ = _migrated()
    entity, provider, client = _host()
    if destination != "fresh":
        if destination == "used":
            entity.state.data.unknown_fields["application"] = {"already": "used"}
        entity.persist_state()
    original = entity.state
    committed = _json(provider.raw)
    writes = (provider.attempted_writes, provider.successful_writes)
    # Use a real produced binding, not hand-fabricated metadata that could fail shape validation.
    candidate = DurableAgentState.from_dict(donor.raw)
    candidate_before = _json(candidate.to_dict())
    with pytest.raises(ValueError, match="Initial migration binding requires migrate"):
        if writer == "setter":
            entity.state = candidate
        else:
            provider.replace_cached_state(candidate)
            entity.persist_state()
    assert _json(provider.raw) == _json(provider._persisted_state_snapshot) == committed
    assert entity.state.to_dict() == original.to_dict()
    assert _json(candidate.to_dict()) == candidate_before
    assert (provider.attempted_writes, provider.successful_writes) == writes
    assert client.received_messages == []


@pytest.mark.parametrize("marker", [None, {}, [], "opaque", False, 7])
@pytest.mark.parametrize("writer", ["setter", "persist"])
def test_first_public_write_cannot_introduce_malformed_reserved_metadata(marker: Any, writer: str) -> None:
    entity, provider, client = _host()
    candidate = deepcopy(entity.state) if writer == "setter" else entity.state
    candidate.data.unknown_fields["migration"] = deepcopy(marker)
    with pytest.raises(ValueError, match="migration session binding"):
        if writer == "setter":
            entity.state = candidate
        else:
            entity.persist_state()
    assert provider.raw == provider._persisted_state_snapshot == {}
    assert entity.state.to_dict() == DurableAgentState().to_dict()
    assert provider.attempted_writes == provider.successful_writes == 0
    assert client.received_messages == []


@pytest.mark.parametrize("migrated", [False, True], ids=["ordinary-state", "committed-migration"])
def test_empty_cache_cannot_hide_used_committed_destination(migrated: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    if migrated:
        entity, provider, client, _ = _migrated(journals=False)
    else:
        entity, provider, client = _host()
        entity.state.data.unknown_fields["application"] = {"already": "used"}
        entity.persist_state()
    committed = _json(provider.raw)
    request = _migration_request(_legacy_source(), provider.core_session_id)
    provider.replace_cached_state(DurableAgentState())
    stage = Mock(side_effect=AssertionError("Used committed state must be rejected before migration staging."))
    monkeypatch.setattr("agent_framework_durabletask._entities.migrate_legacy_state", stage)
    message = (
        "Committed migration binding fields cannot be removed or changed"
        if migrated
        else "Migration destination must be empty"
    )
    with pytest.raises(ValueError, match=message):
        entity.migrate(request)
    stage.assert_not_called()
    assert _json(provider.raw) == _json(provider._persisted_state_snapshot) == committed
    assert provider.attempted_writes == provider.successful_writes == 1
    assert getattr(provider, "_migration_candidate", None) is None
    assert client.received_messages == []


def test_retry_cannot_treat_an_uncommitted_cache_as_a_migration_receipt() -> None:
    _, donor, _, request = _migrated()
    entity, provider, client = _host()
    candidate = DurableAgentState.from_dict(donor.raw)
    provider.replace_cached_state(candidate)
    assert provider._persisted_state_snapshot is None
    with pytest.raises(ValueError, match="Initial migration binding requires migrate"):
        entity.migrate(request)
    assert provider.raw == provider._persisted_state_snapshot == {}
    assert provider.state is candidate  # Entry rejection is read-only, unlike a rejected write.
    assert provider.attempted_writes == provider.successful_writes == 0
    assert client.received_messages == []


def test_migration_admission_does_not_extend_to_a_replacement_candidate(monkeypatch: pytest.MonkeyPatch) -> None:
    entity, provider, client = _host()
    request = _migration_request(_legacy_source(), provider.core_session_id)
    persist = provider.persist_state

    def replace_then_persist() -> None:
        provider.replace_cached_state(deepcopy(entity.state))
        persist()

    monkeypatch.setattr(provider, "persist_state", replace_then_persist)
    with pytest.raises(ValueError, match="Initial migration binding requires migrate"):
        entity.migrate(request)
    assert provider.raw == provider._persisted_state_snapshot == {}
    assert entity.state.to_dict() == DurableAgentState().to_dict()
    assert getattr(provider, "_migration_candidate", None) is None
    assert provider.attempted_writes == provider.successful_writes == 0
    assert client.received_messages == []


def test_nested_state_setter_rejection_preserves_outer_migration(monkeypatch: pytest.MonkeyPatch) -> None:
    entity, provider, client = _host()
    source = _legacy_source(_error_response_entry())
    source["data"]["session"] = {
        "session_id": SOURCE_SESSION_ID,
        "state": {"application": {"values": [False, 0, 0.0]}},
    }
    request = _migration_request(
        source, provider.core_session_id, completionEvidence=_completion_journal(source, _original_result())
    )
    request_before = _json(request)
    real_write = provider._set_state_dict
    ensure_writable = Mock(wraps=provider.ensure_v2_writable)
    rejected: list[str] = []
    observed: dict[str, Any] = {}

    def custom_set_state_dict(payload: dict[str, Any]) -> None:
        candidate = provider.state
        snapshot = provider._persisted_state_snapshot
        before = _json(candidate.to_dict())
        with monkeypatch.context() as nested:
            nested.setattr(provider, "ensure_v2_writable", ensure_writable)
            try:
                provider.state = deepcopy(provider.state)
            except ValueError as exc:
                rejected.append(str(exc))
        observed.update(
            same_candidate=provider.state is candidate is provider._migration_candidate,
            same_snapshot=provider._persisted_state_snapshot is snapshot,
            before=before,
            after=_json(provider.state.to_dict()),
            snapshot=_json(provider._persisted_state_snapshot),
            raw=_json(provider.raw),
            uncertain=provider._write_acknowledgement_uncertain,
        )
        # Let the real outer write finish before checking the nested rejection.
        real_write(payload)

    monkeypatch.setattr(provider, "_set_state_dict", custom_set_state_dict)
    expected = {"status": "migrated", "migrationId": "migration-1", "sessionId": provider.core_session_id}
    assert entity.migrate(request) == expected
    assert provider.attempted_writes == provider.successful_writes == 1
    committed = _json(provider.raw)
    assert _json(entity.state.to_dict()) == _json(provider._persisted_state_snapshot) == committed
    assert observed == {
        "same_candidate": True,
        "same_snapshot": True,
        "before": committed,
        "after": committed,
        "snapshot": "{}",
        "raw": "{}",
        "uncertain": False,
    }
    ensure_writable.assert_not_called()
    assert rejected == ["Cannot replace entity state during migration persistence."]
    assert provider._migration_candidate is None
    assert not provider._write_acknowledgement_uncertain
    assert _json(provider.raw["data"]["session"]) == _json(source["data"]["session"])
    assert provider.raw["data"]["terminalResults"]["done"]["response"] == _original_result()["response"]

    warm = entity.state
    cold, cold_provider, cold_client = _host(json.loads(committed))
    assert cold_provider._state_cache is cold_provider._persisted_state_snapshot is None
    for target, backing, model in ((entity, provider, client), (cold, cold_provider, cold_client)):
        writes = (backing.attempted_writes, backing.successful_writes)
        assert _json(target.state.to_dict()) == committed
        assert target.migrate(request) == expected
        assert (
            _json(target.state.to_dict()) == _json(backing._persisted_state_snapshot) == _json(backing.raw) == committed
        )
        assert (backing.attempted_writes, backing.successful_writes) == writes
        assert model.received_messages == []
    assert provider.state is warm
    assert _json(request) == request_before


@pytest.mark.parametrize("nested_setter", [False, True], ids=["plain-setter", "rejected-nested-setter"])
@pytest.mark.parametrize("after_write", [False, True], ids=["before-write", "lost-acknowledgement"])
@pytest.mark.parametrize("failure", ["error", "cancellation"])
def test_migration_admission_is_cleared_on_every_setter_exit(
    after_write: bool, failure: str, nested_setter: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    entity, provider, client = _host()
    source = _legacy_source(_error_response_entry())
    request = _migration_request(
        source, provider.core_session_id, completionEvidence=_completion_journal(source, _original_result())
    )
    before = _json(request)
    error = OSError("setter interrupted") if failure == "error" else asyncio.CancelledError("setter interrupted")
    real_write = provider._set_state_dict
    payloads: list[dict[str, Any]] = []

    def interrupted(payload: dict[str, Any]) -> None:
        payloads.append(deepcopy(payload))
        if nested_setter:
            candidate = provider.state
            snapshot = provider._persisted_state_snapshot
            candidate_before = _json(candidate.to_dict())
            with pytest.raises(ValueError, match=r"^Cannot replace entity state during migration persistence\.$"):
                provider.state = deepcopy(provider.state)
            assert provider.state is candidate is provider._migration_candidate
            assert _json(candidate.to_dict()) == candidate_before
            assert provider._persisted_state_snapshot is snapshot
            assert snapshot == {}
            assert not provider._write_acknowledgement_uncertain
        if after_write:
            real_write(payload)
        raise error

    write = Mock(side_effect=interrupted)
    monkeypatch.setattr(provider, "_set_state_dict", write)
    with pytest.raises(type(error)) as caught:
        entity.migrate(request)
    assert caught.value is error
    assert write.call_count == 1
    assert getattr(provider, "_migration_candidate", None) is None
    assert provider._write_acknowledgement_uncertain
    assert entity.state.to_dict() == DurableAgentState().to_dict()
    assert provider._persisted_state_snapshot == {}
    assert provider.raw == (payloads[0] if after_write else {})

    if not after_write:
        # The failed migration must not leave an initial-write bypass behind.
        with pytest.raises(ValueError, match="Initial migration binding requires migrate"):
            entity.state = DurableAgentState.from_dict(payloads[0])
        assert write.call_count == 1
    write.side_effect = real_write
    assert entity.migrate(request)["status"] == "migrated"
    assert write.call_count == (1 if after_write else 2)
    assert provider.attempted_writes == provider.successful_writes == 1
    assert _json(provider.raw) == _json(payloads[0])
    assert _json(provider._persisted_state_snapshot) == _json(provider.raw)
    assert getattr(provider, "_migration_candidate", None) is None
    assert not provider._write_acknowledgement_uncertain
    assert client.received_messages == []
    assert _json(request) == before


async def test_normal_edits_retry_reset_and_expiry_preserve_binding(migration_clock: Any) -> None:
    entity, provider, client, request = _migrated()
    original = deepcopy(provider.raw["data"])
    candidate = deepcopy(entity.state)
    assert candidate.data.session is not None
    candidate.data.session["state"]["application"]["edited"] = True
    candidate.unknown_fields["applicationRoot"] = {"values": [False, 0, 0.0]}
    candidate.data.unknown_fields["applicationMetadata"] = {"values": [False, 0, 0.0]}
    candidate.data.unknown_fields["migration"]["operatorNotes"] = {"values": [False, 0, 0.0]}
    entity.state = candidate
    assert provider.raw["data"]["session"]["state"]["application"]["edited"] is True
    entity.state.data.unknown_fields["migration"]["operatorNotes"]["values"][0] = 0
    entity.persist_state()
    assert _json(provider.raw["data"]["migration"]["operatorNotes"]) == _json({"values": [0, 0, 0.0]})
    for location in (provider.raw["applicationRoot"], provider.raw["data"]["applicationMetadata"]):
        assert _json(location) == _json({"values": [False, 0, 0.0]})
    before_retry = _json(provider.raw)
    writes = provider.attempted_writes
    assert entity.migrate(request)["status"] == "migrated"
    assert _json(provider.raw) == before_retry and provider.attempted_writes == writes
    del entity.state.data.unknown_fields["migration"]["operatorNotes"]
    entity.persist_state()

    entity.reset()
    assert entity.state.data.session is None and "session" not in provider.raw["data"]
    assert provider.raw["data"]["conversationHistory"] == []
    for field in ("migration", "terminalResults", "completionReceipts"):
        assert _json(provider.raw["data"][field]) == _json(original[field])
    writes = provider.attempted_writes
    assert entity.expire_responses() == 0
    assert entity.migrate(request)["status"] == "migrated"
    assert provider.attempted_writes == writes

    migration_clock.current = NOW + timedelta(seconds=WINDOW)
    assert entity.expire_responses() == 1
    assert provider.raw["data"]["terminalResults"] == {}
    assert provider.raw["data"]["completionReceipts"]["done"] == {
        **original["completionReceipts"]["done"],
        "resultState": "unavailable",
        "resultUnavailableAt": (NOW + timedelta(seconds=WINDOW)).isoformat(),
    }
    assert _json(provider.raw["data"]["migration"]) == _json(original["migration"])
    assert client.received_messages == []
    expired = _json(provider.raw)
    writes = provider.attempted_writes
    assert entity.migrate(request)["status"] == "migrated"
    assert _json(provider.raw) == expired and provider.attempted_writes == writes

    external = _ObservedExternalHistory()
    cold, backing, _ = _host(json.loads(expired), external=external)
    assert cold.migrate(request)["status"] == "migrated"
    assert backing.attempted_writes == 0 and _json(backing.raw) == expired
    assert (await cold.run({"message": "after reset", "correlationId": "after-reset"})).text == "reply-1"
    assert external.calls == [("load", SOURCE_SESSION_ID), ("save", SOURCE_SESSION_ID)]
    assert backing.raw["data"]["session"]["session_id"] == SOURCE_SESSION_ID
    assert _json(backing.raw["data"]["migration"]) == _json(original["migration"])
