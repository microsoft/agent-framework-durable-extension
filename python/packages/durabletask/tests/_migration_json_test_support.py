# Copyright (c) Microsoft. All rights reserved.

"""Constructed SDK entity batches, not live-host captures or pipeline mocks.

Each call reloads serialized state into a fresh SDK context/entity. The worker or
indexed app is reused. No collected test modules or new JSON adapter internals
are imported. Baseline collection does not require the new JSON adapters.
"""

import hashlib
import json
import logging
from collections.abc import Callable
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import Agent, BaseChatClient
from durabletask.entities import EntityInstanceId
from durabletask.internal.entity_state_shim import StateShim
from durabletask.worker import TaskHubGrpcWorker, _EntityExecutor

from agent_framework_durabletask import DurableAIAgentWorker

_MARKER = "__durabletask_autoobject__"
_NAME = "dafx-json-agent"
_DESTINATION = f"@{_NAME}@dest"
_NOW = datetime(2026, 9, 23, 12, tzinfo=timezone.utc)
_EXPIRES = "2026-09-23T13:00:00+00:00"
_MIGRATED = {"status": "migrated", "migrationId": "migration-1", "sessionId": _DESTINATION}
_CONSTRUCTIONS: list[Any] = []


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_json(value).encode("utf-8")).hexdigest()


@pytest.fixture
def migration_clock(monkeypatch: pytest.MonkeyPatch) -> Any:
    class Clock(datetime):
        current = _NOW

        @classmethod
        def now(cls, tz: Any = None) -> "Clock":
            return cls.fromtimestamp(cls.current.timestamp(), tz)

    _CONSTRUCTIONS.clear()
    for module in ("_state_migration", "_delivery_state", "_shared_state_validation"):
        monkeypatch.setattr(f"agent_framework_durabletask.{module}.datetime", Clock)
    return Clock


class _BenignProbe:
    @classmethod
    def from_json(cls, value: Any) -> Any:
        _CONSTRUCTIONS.append(deepcopy(value))
        return {"constructed": value}


def _payload(case: str) -> dict[str, Any]:
    value: dict[str, Any] = {"keep": [None, False, 0, 0.0, "雪"]}
    if case in ("dt-false", "dt-true"):
        value[_MARKER] = case == "dt-true"
    elif case == "af-sentinel":
        value.update(__module__=__name__, __class__="_BenignProbe", __data__={"value": 7})
    else:
        assert case == "plain"
    return value


def _request(value: dict[str, Any], location: str) -> dict[str, Any]:
    source: dict[str, Any] = {"schemaVersion": "1.1.0", "data": {"conversationHistory": []}}
    result: dict[str, Any] = {
        "correlationId": "done",
        "outcome": "succeeded",
        "completedAt": "2024-01-03T04:05:06+00:00",
        "response": {"messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "done"}]}]},
    }
    if location == "source":
        source["futureRoot"] = deepcopy(value)
    else:
        assert location == "completion"
        result["futureResult"] = deepcopy(value)
    return {
        "source": source,
        "sourceDigest": _digest(source),
        "sourceSessionId": "legacy-source",
        "destinationSessionId": _DESTINATION,
        "migrationId": "migration-1",
        "ownershipTransferId": "transfer-1",
        "completionEvidence": {
            "sourceDigest": _digest(source),
            "evidenceId": "journal-1",
            "complete": True,
            "results": [result],
        },
    }


class _NoModelClient(BaseChatClient):
    def __init__(self) -> None:
        super().__init__()
        self.calls = 0

    def _inner_get_response(self, **kwargs: Any) -> Any:
        self.calls += 1
        raise AssertionError("Migration and reset must not invoke the model")


class _CountedState(StateShim):
    def __init__(self, raw: str | None, converter: Any) -> None:
        super().__init__(raw, converter, is_serialized=True)
        self.writes = 0

    def set_state(self, state: Any) -> None:
        self.writes += 1
        super().set_state(state)


class _EntityHost:
    def __init__(self, register: Callable[[Agent], Any] | None = None) -> None:
        self.raw: str | None = None
        self.client = _NoModelClient()
        agent = Agent(client=self.client, name="json-agent")
        if register is None:
            self.worker: Any = TaskHubGrpcWorker(channel=Mock())  # Never started, no network.
            DurableAIAgentWorker(
                self.worker, deployment_mode="isolated_v2", response_delivery_window_seconds=3600
            ).add_agent(agent)
        else:
            # Another host registers the agent and returns a worker holding its entity.
            self.worker = register(agent)

    def call(self, operation: str, value: Any = None) -> Any:
        self.shim = _CountedState(self.raw, self.worker._data_converter)
        executor = _EntityExecutor(self.worker._registry, logging.getLogger(__name__), self.worker._data_converter)
        result = executor.execute("migration-json", EntityInstanceId(_NAME, "dest"), operation, self.shim, _json(value))
        self.shim.commit()
        self.raw = self.shim.encode_state()
        return json.loads(result) if result is not None else None

    def snapshot(self) -> dict[str, Any]:
        assert self.raw is not None
        return json.loads(self.raw)

    def assert_idle(self) -> None:
        assert self.client.calls == 0
        assert _CONSTRUCTIONS == [], "SDK constructed opaque migration JSON"

    def assert_writes(self, expected: int) -> None:
        assert self.shim.writes == expected


def _assert_ingress_and_retry(host: _EntityHost, case: str, location: str, clock: Any) -> None:
    value = _payload(case)
    request = _request(value, location)
    before = _json(request)
    assert _json(host.call("migrate", request)) == _json(_MIGRATED)
    host.assert_idle()
    host.assert_writes(1)
    state = host.snapshot()
    actual = state["futureRoot"] if location == "source" else state["data"]["terminalResults"]["done"]["futureResult"]
    assert _json(actual) == _json(value)
    migration = state["data"]["migration"]
    assert migration["requestDigest"] == _digest(request) and migration["sourceDigest"] == _digest(request["source"])
    assert migration["createdAt"] == "2026-09-23T12:00:00+00:00"
    original = request["completionEvidence"]["results"][0]
    assert _json(state["data"]["terminalResults"]) == _json({"done": {**original, "resultExpiresAt": _EXPIRES}})
    assert _json(state["data"]["completionReceipts"]) == _json({
        "done": {
            "correlationId": "done",
            "outcome": "succeeded",
            "completedAt": "2024-01-03T04:05:06+00:00",
            "resultExpiresAt": _EXPIRES,
            "resultState": "available",
        },
    })
    clock.current += timedelta(minutes=5)
    for _ in range(2):
        assert _json(host.call("migrate", request)) == _json(_MIGRATED)
        assert _json(host.snapshot()) == _json(state)  # Includes the original, unrefreshed grace window.
        host.assert_writes(0)
        host.assert_idle()
    assert _json(request) == before


def _assert_cold_reset(host: _EntityHost, case: str, clock: Any) -> None:
    request = _request(_payload("plain"), "completion")
    before = _json(request)
    assert _json(host.call("migrate", request)) == _json(_MIGRATED)
    host.assert_idle()
    state = host.snapshot()
    # Seed only an opaque result sibling after a real migration. This isolates
    # state hydration from ingress, retaining the genuine binding and receipt.
    state["data"]["terminalResults"]["done"]["futureResult"] = _payload(case)
    assert state["data"]["terminalResults"]["done"]["resultExpiresAt"] == _EXPIRES
    host.raw = _json(state)
    clock.current += timedelta(minutes=5)  # Reset stays strictly inside the delivery window.
    assert host.call("reset") is None
    host.assert_idle()
    host.assert_writes(1)
    expected = deepcopy(state)
    del expected["data"]["session"]  # Reset must not alter terminal results, receipts or the binding.
    assert _json(host.snapshot()) == _json(expected)
    assert _json(host.call("migrate", request)) == _json(_MIGRATED)
    assert _json(host.snapshot()) == _json(expected)
    host.assert_writes(0)
    host.assert_idle()
    assert _json(request) == before
