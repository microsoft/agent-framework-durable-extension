# Copyright (c) Microsoft. All rights reserved.

"""Read-only duplicate delivery is independent of capacity-protected maintenance writes."""

import json
from copy import deepcopy
from datetime import datetime, timezone, tzinfo
from typing import Any
from unittest.mock import AsyncMock

import pytest
from _execution_test_support import JsonStateProvider, RecordingChatClient
from _migration_test_support import (
    SOURCE_SESSION_ID,
    _completion_journal,
    _error_response_entry,
    _legacy_source,
    _migration_request,
    _original_result,
)
from agent_framework import Agent, AgentResponse, Content, Message

from agent_framework_durabletask import (
    AgentEntity,
    StateCapacityError,
    _delivery_state,
    _entities,
    _shared_state_validation,
    _state_migration,
    state_snapshot_digest,
)
from agent_framework_durabletask._state_capacity import StateCapacityError as SharedStateCapacityError

NOW = datetime(2026, 9, 18, 12, tzinfo=timezone.utc)
EXPIRES = "2026-09-18T12:01:00+00:00"
SMALL_BUDGET = 256


class _Clock(datetime):
    @classmethod
    def now(cls, tz: tzinfo | None = None) -> "_Clock":
        return cls.fromtimestamp(NOW.timestamp(), tz)


@pytest.fixture(autouse=True)
def maintenance_clock_and_no_pruning(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    for module in (_delivery_state, _shared_state_validation, _state_migration):
        monkeypatch.setattr(module, "datetime", _Clock)
    pruning = AsyncMock(side_effect=AssertionError("Maintenance must not prune state."))
    monkeypatch.setattr(_entities, "enforce_budget", pruning)
    return pruning


@pytest.fixture(params=["oversized", "exact", "unbounded"])
def budget_mode(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _size(raw: dict[str, Any]) -> int:
    return len(json.dumps(raw, ensure_ascii=True, allow_nan=False).encode("utf-8"))


def _budget(mode: str, candidate: dict[str, Any]) -> int | None:
    assert _size(candidate) > SMALL_BUDGET
    return {"oversized": SMALL_BUDGET, "exact": _size(candidate), "unbounded": None}[mode]


def _entity(provider: JsonStateProvider, budget: int | None) -> tuple[AgentEntity, RecordingChatClient]:
    client = RecordingChatClient()
    return (
        AgentEntity(
            Agent(client=client, name="maintenance"),
            state_provider=provider,
            retention="follow_compaction",
            max_state_bytes=budget,
        ),
        client,
    )


def _control_state() -> dict[str, Any]:
    results: dict[str, Any] = {}
    receipts: dict[str, Any] = {}
    for correlation, deadline in (("expired", "2026-09-17T12:01:00+00:00"), ("live", EXPIRES)):
        common = {
            "correlationId": correlation,
            "outcome": "succeeded",
            "completedAt": "2026-09-17T12:00:00+00:00",
            "resultExpiresAt": deadline,
        }
        results[correlation] = {
            **common,
            "response": {"messages": [{"role": "assistant", "contents": [{"$type": "text", "text": correlation}]}]},
        }
        receipts[correlation] = {**common, "resultState": "available", "opaqueReceipt": "雪" * 100}
    return {
        "schemaVersion": "2.0.0",
        "data": {
            "conversationHistory": [
                {
                    "$type": "request",
                    "correlationId": "history-only",
                    "messages": [{"role": "user", "contents": [{"$type": "text", "text": "retain on expiry"}]}],
                }
            ],
            "session": {"session_id": "runtime-session", "state": {"opaque": [None, False, "雪"]}},
            "terminalResults": results,
            "completionReceipts": receipts,
        },
    }


def _expired_candidate(raw: dict[str, Any]) -> dict[str, Any]:
    candidate = deepcopy(raw)
    del candidate["data"]["terminalResults"]["expired"]
    candidate["data"]["completionReceipts"]["expired"].update(
        resultState="unavailable", resultUnavailableAt=NOW.isoformat()
    )
    return candidate


def _assert_capacity(error: StateCapacityError, candidate: dict[str, Any]) -> None:
    assert type(error) is StateCapacityError is SharedStateCapacityError
    assert isinstance(error, ValueError)
    assert error.size_bytes == error.floor_bytes == _size(candidate)
    assert error.max_state_bytes == error.target_bytes == SMALL_BUDGET


def _assert_unchanged(provider: JsonStateProvider, original: Any, before: dict[str, Any]) -> None:
    assert provider.state is original
    assert provider.state.to_dict() == before
    assert provider.raw == before
    assert provider._persisted_state_snapshot == before


def _assert_rollback(provider: JsonStateProvider, original: Any, before: dict[str, Any]) -> None:
    _assert_unchanged(provider, original, before)
    assert provider.attempted_writes == provider.successful_writes == 0


def test_reset_retains_receipt_floor_and_rolls_back_on_capacity_failure(budget_mode: str) -> None:
    before = _control_state()
    candidate = _expired_candidate(before)
    candidate["data"]["conversationHistory"] = []
    del candidate["data"]["session"]
    assert _size({"receipts": candidate["data"]["completionReceipts"]}) > SMALL_BUDGET
    provider = JsonStateProvider(before)
    entity, client = _entity(provider, _budget(budget_mode, candidate))
    original = provider.state

    if budget_mode == "oversized":
        with pytest.raises(StateCapacityError) as caught:
            entity.reset()
        _assert_capacity(caught.value, candidate)
        _assert_rollback(provider, original, before)
    else:
        entity.reset()
        assert provider.raw == provider.state.to_dict() == candidate
        assert provider.attempted_writes == provider.successful_writes == 1
    assert client.received_messages == []


def test_expiry_retains_receipt_floor_and_rolls_back_on_capacity_failure(budget_mode: str) -> None:
    before = _control_state()
    candidate = _expired_candidate(before)
    assert _size({"receipt": candidate["data"]["completionReceipts"]["expired"]}) > SMALL_BUDGET
    provider = JsonStateProvider(before)
    entity, client = _entity(provider, _budget(budget_mode, candidate))
    original = provider.state

    if budget_mode == "oversized":
        with pytest.raises(StateCapacityError) as caught:
            entity.expire_responses()
        _assert_capacity(caught.value, candidate)
        _assert_rollback(provider, original, before)
    else:
        assert entity.expire_responses() == 1
        assert provider.raw == provider.state.to_dict() == candidate
        assert entity.expire_responses() == 0
        assert provider.attempted_writes == provider.successful_writes == 1
    assert client.received_messages == []


@pytest.mark.parametrize("budget_mode", ["exact", "unbounded"])
def test_expiry_backend_failure_rolls_back_without_pruning(
    budget_mode: str, maintenance_clock_and_no_pruning: AsyncMock
) -> None:
    before = _control_state()
    provider = JsonStateProvider(before)
    provider.fail_before_write = True
    entity, client = _entity(provider, _budget(budget_mode, _expired_candidate(before)))
    original = provider.state

    with pytest.raises(OSError, match="injected commit failure"):
        entity.expire_responses()

    _assert_unchanged(provider, original, before)
    assert provider.attempted_writes == 1
    assert provider.successful_writes == 0
    assert client.received_messages == []
    maintenance_clock_and_no_pruning.assert_not_called()


@pytest.mark.parametrize("fail_before_write", [False, True], ids=["healthy-backend", "failing-backend"])
async def test_live_duplicate_returns_committed_result_without_maintenance(
    budget_mode: str, fail_before_write: bool, maintenance_clock_and_no_pruning: AsyncMock
) -> None:
    before = _control_state()
    provider = JsonStateProvider(before)
    provider.fail_before_write = fail_before_write
    entity, client = _entity(provider, _budget(budget_mode, _expired_candidate(before)))
    original = provider.state
    expected = AgentResponse(messages=[Message("assistant", ["live"])])

    # An unrelated expired payload must not turn delivery into a maintenance write.
    for _ in range(2):
        response = await entity.run({"message": "must not execute", "correlationId": "live"})

        assert response.text == "live"
        assert response.to_dict() == expected.to_dict()
        _assert_rollback(provider, original, before)
        assert client.received_messages == []
        maintenance_clock_and_no_pruning.assert_not_called()


@pytest.mark.parametrize("fail_before_write", [False, True], ids=["healthy-backend", "failing-backend"])
@pytest.mark.parametrize("result_state", ["available", "unavailable"])
async def test_expired_duplicate_reports_unavailability_without_maintenance(
    budget_mode: str, fail_before_write: bool, result_state: str, maintenance_clock_and_no_pruning: AsyncMock
) -> None:
    before = _control_state()
    if result_state == "unavailable":
        before = _expired_candidate(before)
    provider = JsonStateProvider(before)
    provider.fail_before_write = fail_before_write
    entity, client = _entity(provider, _budget(budget_mode, before))
    original = provider.state
    expected = AgentResponse(
        messages=[
            Message(
                "system",
                [
                    Content.from_error(
                        message="This request completed, but its response delivery window has expired.",
                        error_code="response_expired",
                    )
                ],
            )
        ],
        additional_properties={
            "durable_status": "already_completed",
            "correlation_id": "expired",
            "durable_outcome": "succeeded",
        },
    )

    for _ in range(2):
        response = await entity.run({"message": "must not execute", "correlationId": "expired"})

        assert response.to_dict() == expected.to_dict()
        _assert_rollback(provider, original, before)
        assert client.received_messages == []
        maintenance_clock_and_no_pruning.assert_not_called()


def test_migrate_protects_fresh_results_and_binding_and_leaves_destination_empty(budget_mode: str) -> None:
    source = _legacy_source(_error_response_entry())
    provider = JsonStateProvider()
    request = _migration_request(
        source, provider.core_session_id, completionEvidence=_completion_journal(source, _original_result())
    )
    inputs_before = deepcopy((source, request))
    common = {
        "correlationId": "done",
        "outcome": "failed",
        "completedAt": _original_result()["completedAt"],
        "resultExpiresAt": EXPIRES,
    }
    candidate = {
        "schemaVersion": "2.0.0",
        "data": {
            **deepcopy(source["data"]),
            "session": {"session_id": SOURCE_SESSION_ID, "state": {}},
            "terminalResults": {"done": {**_original_result(), "resultExpiresAt": EXPIRES}},
            "completionReceipts": {"done": {**common, "resultState": "available"}},
            "migration": {
                "id": request["migrationId"],
                "sourceDigest": request["sourceDigest"],
                "sourceSessionId": SOURCE_SESSION_ID,
                "ownershipTransferId": request["ownershipTransferId"],
                "createdAt": NOW.isoformat(),
                "completionEvidenceId": request["completionEvidence"]["evidenceId"],
                "requestDigest": state_snapshot_digest(request),
                "destinationSessionId": provider.core_session_id,
            },
        },
    }
    entity, client = _entity(provider, _budget(budget_mode, candidate))
    original = provider.state
    empty = deepcopy(original.to_dict())

    if budget_mode == "oversized":
        with pytest.raises(StateCapacityError) as caught:
            entity.migrate(request)
        _assert_capacity(caught.value, candidate)
        assert provider.state is original
        assert provider.state.to_dict() == empty
        assert provider.raw == provider._persisted_state_snapshot == {}
        assert provider.attempted_writes == provider.successful_writes == 0
    else:
        result = entity.migrate(request)
        assert result == {"status": "migrated", "migrationId": "migration-1", "sessionId": provider.core_session_id}
        assert provider.raw == provider.state.to_dict() == candidate
        assert entity.migrate(request) == result
        assert provider.attempted_writes == provider.successful_writes == 1
    assert (source, request) == inputs_before
    assert client.received_messages == []
