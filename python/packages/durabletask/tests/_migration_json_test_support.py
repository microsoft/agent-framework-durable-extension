# Copyright (c) Microsoft. All rights reserved.

"""Constructed SDK entity batches, not live-host captures or pipeline mocks.

Each call reloads serialized state into a fresh SDK context/entity. The worker or
indexed app is reused. No collected test modules or new JSON adapter internals
are imported. Baseline collection does not require the new JSON adapters.
"""

import hashlib
import json
import logging
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import Agent, BaseChatClient, Message
from durabletask.entities import EntityInstanceId
from durabletask.internal.entity_state_shim import StateShim
from durabletask.worker import TaskHubGrpcWorker, _EntityExecutor

from agent_framework_durabletask import DurableAIAgentWorker, state_snapshot_digest

_MARKER = "__durabletask_autoobject__"
_NAME = "dafx-json-agent"
_DESTINATION = f"@{_NAME}@dest"
_NOW = datetime(2026, 9, 23, 12, tzinfo=timezone.utc)
_EXPIRES = "2026-09-23T13:00:00+00:00"
_MIGRATED = {"status": "migrated", "migrationId": "migration-1", "sessionId": _DESTINATION}
_CONSTRUCTIONS: list[Any] = []
_COUNTER_CASES = [
    ("1e0", 1.0),
    ("9007199254740993.0", 2**53 + 1),
    ("1000000000000000128.0", 1000000000000000128),
    ("9223372036854775807.0", 2**63 - 1),
    ("9.223372036854775807e18", 2**63 - 1),
    ("1.00000000000000001", None),
    ("9223372036854775807.1", None),
    ("9223372036854775808.0", None),
    ("1e-9999", None),
]


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


def _af_batch(
    function: Any, operation: str, value: Any, raw: str | None, input_json: str | None = None
) -> dict[str, Any]:
    wire = _json({
        "self": {"name": _NAME, "key": "dest"},
        "exists": raw is not None,
        "state": raw,
        # Both native input layers are required before the SDK decoder is reached.
        "batch": [{"name": operation, "input": _json(_json(value) if input_json is None else input_json)}],
    })
    batch = json.loads(function(wire))
    assert len(batch["results"]) == 1 and batch["results"][0]["isError"] is False, batch
    assert batch["entityExists"] is True and batch["signals"] == []
    return batch


class _EntityHost:
    def __init__(self, backend: str) -> None:
        self.backend = backend
        self.raw: str | None = None
        self.client = _NoModelClient()
        agent = Agent(client=self.client, name="json-agent")
        if backend == "dt":
            self.worker: Any = TaskHubGrpcWorker(channel=Mock())  # Never started, no network.
            DurableAIAgentWorker(
                self.worker, deployment_mode="isolated_v2", response_delivery_window_seconds=3600
            ).add_agent(agent)
        else:
            assert backend == "af"
            # AF-only cases need this sibling package, DT-only collection does not.
            from agent_framework_azurefunctions import AgentFunctionApp

            app: Any = AgentFunctionApp(
                agents=[agent],
                enable_health_check=False,
                enable_http_endpoints=False,
                deployment_mode="isolated_v2",
                response_delivery_window_seconds=3600,
            )
            functions = {function.get_function_name(): function for function in app.get_functions()}
            registered = functions[_NAME]  # Index once, retain the real df.Entity factory result.
            binding = registered.get_bindings_dict()["bindings"][0]
            assert binding["type"] == "entityTrigger" and binding["entityName"] == _NAME
            self.function = registered.get_user_function()
            assert callable(self.function.entity_function)

    def call(self, operation: str, value: Any = None, *, input_json: str | None = None) -> Any:
        if self.backend == "af":
            batch = _af_batch(self.function, operation, value, self.raw, input_json)
            self.raw = batch["entityState"]
            return json.loads(batch["results"][0]["result"])
        self.shim = _CountedState(self.raw, self.worker._data_converter)
        executor = _EntityExecutor(self.worker._registry, logging.getLogger(__name__), self.worker._data_converter)
        result = executor.execute(
            "migration-json",
            EntityInstanceId(_NAME, "dest"),
            operation,
            self.shim,
            _json(value) if input_json is None else input_json,
        )
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
        # Functions returns final state, not a storage-write count or acknowledgement.
        if self.backend == "dt":
            assert self.shim.writes == expected


def _assert_counter_ingress(host: _EntityHost, location: str, token: str, expected: int | float | None) -> None:
    request = _request(_payload("plain"), "completion")
    if location == "source-position":
        owner: dict[str, Any] = {"producer": expected if expected is not None else 1.0}
        request["source"]["data"]["ingestedPositions"] = owner
        request["deliveryEvidence"] = {
            "evidenceId": "accepted-inputs",
            "complete": True,
            "messages": [Message("user", ["accepted"], message_id="accepted").to_dict()],
            "messagePositions": [{"producer": "producer", "position": int(expected) if expected is not None else 1}],
        }
        field = "producer"
    elif location == "source":
        owner = {
            "evictedMessageCount": expected if expected is not None else 1.0,
            "firstEvictedAt": "2024-01-01T00:00:00Z",
            "lastEvictedAt": "2024-01-01T00:00:00Z",
        }
        request["source"]["data"]["truncation"] = owner
        field = "evictedMessageCount"
    else:
        owner = {"totalTokenCount": expected if expected is not None else 1.0}
        if location == "source-usage":
            request["source"]["data"]["conversationHistory"] = [
                {"$type": "response", "correlationId": "done", "usage": owner}
            ]
        elif location == "completion-content":
            request["completionEvidence"]["results"][0]["response"]["messages"][0]["contents"] = [
                {"$type": "usage", "usage": owner}
            ]
        else:
            assert location == "completion"
            request["completionEvidence"]["results"][0]["response"]["usage"] = owner
        field = "totalTokenCount"
    request["sourceDigest"] = request["completionEvidence"]["sourceDigest"] = _digest(request["source"])
    if "deliveryEvidence" in request:
        request["deliveryEvidence"]["sourceDigest"] = request["sourceDigest"]
    owner[field] = "COUNTER_TOKEN"
    wire = _json(request).replace('"COUNTER_TOKEN"', token)
    if expected is None:
        if host.backend == "dt":
            with pytest.raises(ValueError):
                host.call("migrate", input_json=wire)
            assert host.shim.encode_state() is None
        else:
            result = host.call("migrate", input_json=wire)
            assert result["status"] == "error"
            assert json.loads(host.raw or "null") is None
        host.assert_writes(0)
    else:
        assert host.call("migrate", input_json=wire) == _MIGRATED
        state = host.snapshot()
        if location == "source-position":
            stored = state["data"]["ingestedPositions"]
        elif location == "source":
            stored = state["data"]["truncation"]
        elif location == "source-usage":
            stored = state["data"]["conversationHistory"][0]["usage"]
        elif location == "completion-content":
            stored = state["data"]["terminalResults"]["done"]["response"]["messages"][0]["contents"][0]["usage"]
        else:
            stored = state["data"]["terminalResults"]["done"]["response"]["usage"]
        assert stored[field] == expected
        assert Decimal(json.dumps(stored[field])) == Decimal(token)
        before = host.raw
        assert host.call("migrate", input_json=wire) == _MIGRATED
        assert host.raw == before
        host.assert_writes(0)
    host.assert_idle()


def _assert_float_digest_compatibility(host: _EntityHost, location: str) -> None:
    request = _request(_payload("plain"), "completion")
    count = 1.0000000000000001e18
    if location == "source":
        request["source"]["data"]["truncation"] = {
            "evictedMessageCount": count,
            "firstEvictedAt": "2024-01-01T00:00:00Z",
            "lastEvictedAt": "2024-01-01T00:00:00Z",
        }
    else:
        request["completionEvidence"]["results"][0]["response"]["usage"] = {"totalTokenCount": count}
    request["sourceDigest"] = request["completionEvidence"]["sourceDigest"] = state_snapshot_digest(request["source"])
    request_digest = state_snapshot_digest(request)
    assert host.call("migrate", request) == _MIGRATED
    snapshot = host.snapshot()
    assert snapshot["data"]["migration"]["sourceDigest"] == request["sourceDigest"]
    assert snapshot["data"]["migration"]["requestDigest"] == request_digest
    stored = (
        snapshot["data"]["truncation"]["evictedMessageCount"]
        if location == "source"
        else snapshot["data"]["terminalResults"]["done"]["response"]["usage"]["totalTokenCount"]
    )
    assert _json(stored) == _json(count)
    before = host.raw
    assert host.call("migrate", request) == _MIGRATED
    assert host.raw is not None and before is not None
    assert json.loads(host.raw, parse_float=Decimal) == json.loads(before, parse_float=Decimal)
    if host.backend == "dt":
        assert host.raw == before
    host.assert_writes(0)
    host.assert_idle()


def _assert_opaque_counter_ingress(host: _EntityHost, location: str, token: str) -> None:
    expected = {"usage": {"totalTokenCount": json.loads(token)}, "literal": token}
    request = _request(expected, location)
    owner = (
        request["source"]["futureRoot"]
        if location == "source"
        else request["completionEvidence"]["results"][0]["futureResult"]
    )
    owner["usage"]["totalTokenCount"] = "OPAQUE_TOKEN"
    wire = _json(request).replace('"OPAQUE_TOKEN"', token)
    assert host.call("migrate", input_json=wire) == _MIGRATED
    state = host.snapshot()
    actual = state["futureRoot"] if location == "source" else state["data"]["terminalResults"]["done"]["futureResult"]
    assert _json(actual) == _json(expected)
    host.assert_idle()


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
    assert _json(host.call("reset")) == _json(None if host.backend == "dt" else {"status": "reset"})
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
