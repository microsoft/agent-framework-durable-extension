# Copyright (c) Microsoft. All rights reserved.

"""Source-pinned conversion through the actual native entity batch executor."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import marshal
import os
import struct
import sys
import threading
from copy import deepcopy
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import AgentResponse, Message
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from durabletask.entities import EntityContext
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.worker import TaskHubGrpcWorker

from agent_framework_durabletask import checkpoint_entity_migration as migration
from agent_framework_durabletask._checkpoint_store import CheckpointScope, CheckpointStore
from agent_framework_durabletask._checkpoints._agent import create_entity, digest
from agent_framework_durabletask._checkpoints._authority import verify_capability
from agent_framework_durabletask.checkpoint_entity_migration import (
    convert_agent_observations,
    create_authorized_entity_converter,
    entity_conversion_binding,
    issue_entity_conversion,
)

ADDRESS = "@dafx-migration-test@original"
SCOPE = CheckpointScope("owned-backend", "owned-hub", "a" * 64)
BUNDLE = "b" * 64


def _raw() -> dict:
    return {
        "schemaVersion": "1.1.0",
        "data": {
            "conversationHistory": [
                {
                    "$type": kind,
                    "correlationId": "same-correlation",
                    "createdAt": "2026-09-22T12:00:00+00:00",
                    "messages": [{"role": role, "contents": [{"$type": "text", "text": text}]}],
                }
                for kind, role, text in (
                    ("request", "user", "old prompt"),
                    ("response", "assistant", "old answer"),
                )
            ],
        },
    }


@pytest.fixture
def published_reader() -> Path:
    value = os.environ.get("CHECKPOINT_PUBLISHED_AGENT_READER")
    if not value:
        pytest.skip("Set CHECKPOINT_PUBLISHED_AGENT_READER to the exact pinned published reader.")
    return Path(value)


class _Agent:
    def __init__(self) -> None:
        self.id = "migration-test"
        self.name: str | None = "migration-test"
        self.description: str | None = "Deterministic custom agent"
        self.calls: list[list[str]] = []

    def create_session(self, **kwargs: Any) -> Any:
        raise NotImplementedError

    def get_session(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def run(self, messages: Any = None, *, stream: bool = False, **kwargs: Any) -> Any:
        if stream:
            raise TypeError("stream is not supported")
        self.calls.append([content.text for message in messages for content in message.contents])

        async def result() -> AgentResponse:
            return AgentResponse(messages=[Message("assistant", ["new answer"])])

        return result()


def _worker(tmp_path: Path, reader: Path, raw: dict, *, authorized: dict | None = None) -> tuple:
    store = CheckpointStore(tmp_path / "authority.db", create=True)
    store.prepare(SCOPE, "g1", bundle_sha256=BUNDLE)
    private = Ed25519PrivateKey.generate()
    envelope = issue_entity_conversion(
        key_id="operator",
        operator_private_key=private,
        scope=SCOPE,
        entity=ADDRESS,
        source_state=raw if authorized is None else authorized,
    )
    store.put_entity_conversion(SCOPE, "g1", entity=ADDRESS, payload=envelope)
    store.begin_conversion(SCOPE, "g1", expected_active=None, stop_attestation=b"old workers stopped")
    agent = _Agent()
    entity = create_authorized_entity_converter(
        agent,
        entity_id="migration-test",
        scope=SCOPE,
        store=store,
        generation="g1",
        bundle_sha256=BUNDLE,
        trusted_public_keys={"operator": private.public_key()},
        published_reader=reader,
    )
    worker = TaskHubGrpcWorker(channel=Mock(), emit_trace_spans=False)
    worker._registry.add_entity(entity)
    return worker, agent, store


def _execute(worker: TaskHubGrpcWorker, raw: dict, operations: list[tuple[str, str, dict]]) -> Any:
    return worker._execute_entity_batch(
        pb.EntityBatchRequest(
            instanceId=ADDRESS,
            entityState=helpers.get_string_value(json.dumps(raw)),
            operations=[
                pb.OperationRequest(
                    operation=name,
                    requestId=identity,
                    input=helpers.get_string_value(json.dumps(value)),
                )
                for name, identity, value in operations
            ],
        ),
        Mock(),
        "source-pinned-conversion",
    )


def test_bad_source_pin_and_invalid_usage_do_not_silently_reset(tmp_path: Path) -> None:
    reader = tmp_path / "not-published.py"
    reader.write_text("raise AssertionError('must not execute')", encoding="utf-8")
    with pytest.raises(ValueError, match="pinned release"):
        convert_agent_observations(_raw(), published_reader=reader)
    raw = _raw()
    raw["data"]["conversationHistory"][1]["usage"] = {"inputTokenCount": True}
    with pytest.raises(ValueError, match="integer or null"):
        convert_agent_observations(raw, published_reader=reader)


def test_original_reader_executes_verified_bytes_not_timestamp_valid_bytecode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = b"reader_value = 'verified source'\n"
    reader = tmp_path / "reader.py"
    reader.write_bytes(source)
    timestamp = int(reader.stat().st_mtime)
    bytecode = compile("raise AssertionError('unverified cache executed')", str(reader), "exec")
    cache = Path(importlib.util.cache_from_source(str(reader)))
    cache.parent.mkdir(parents=True)
    cache.write_bytes(
        importlib.util.MAGIC_NUMBER
        + struct.pack("<III", 0, timestamp, len(source))
        + marshal.dumps(bytecode)
    )
    monkeypatch.setattr(migration, "PUBLISHED_READER_SHA256", hashlib.sha256(source).hexdigest())
    module_name = "agent_framework_durabletask._migration_original_state"
    previous = sys.modules.pop(module_name, None)

    try:
        loaded = migration._original_reader(reader)
        assert loaded.reader_value == "verified source"
    finally:
        sys.modules.pop(module_name, None)
        if previous is not None:
            sys.modules[module_name] = previous


def test_original_reader_serializes_concurrent_first_load(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = b"DurableAgentState = type('DurableAgentState', (), {})\n"
    reader = tmp_path / "reader.py"
    reader.write_bytes(source)
    monkeypatch.setattr(migration, "PUBLISHED_READER_SHA256", hashlib.sha256(source).hexdigest())
    module_name = "agent_framework_durabletask._migration_original_state"
    previous = sys.modules.pop(module_name, None)
    first_exec_started = threading.Event()
    allow_first_exec = threading.Event()
    second_lock_attempted = threading.Event()
    actual_exec = __import__("builtins").exec
    results: dict[str, Any] = {}
    failures: list[BaseException] = []

    class ObservedLock:
        def __init__(self) -> None:
            self._lock = threading.RLock()

        def __enter__(self) -> ObservedLock:
            if threading.current_thread().name == "reader-second":
                second_lock_attempted.set()
            self._lock.acquire()
            return self

        def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
            self._lock.release()

    def gated_exec(code: Any, globals: Any = None, locals: Any = None) -> None:
        if globals is not None and globals.get("__name__") == module_name:
            first_exec_started.set()
            assert allow_first_exec.wait(timeout=5)
        if locals is None:
            actual_exec(code, globals)
        else:
            actual_exec(code, globals, locals)

    def load(key: str) -> None:
        try:
            results[key] = migration._original_reader(reader)
        except BaseException as error:
            failures.append(error)

    monkeypatch.setattr(__import__("builtins"), "exec", gated_exec)
    monkeypatch.setattr(migration, "_ORIGINAL_READER_LOCK", ObservedLock())
    first = threading.Thread(target=load, args=("first",), name="reader-first")
    second = threading.Thread(target=load, args=("second",), name="reader-second")

    try:
        first.start()
        assert first_exec_started.wait(timeout=5)
        second.start()
        assert second_lock_attempted.wait(timeout=5)
        allow_first_exec.set()
        first.join(timeout=5)
        second.join(timeout=5)

        assert not first.is_alive()
        assert not second.is_alive()
        assert failures == []
        assert results["first"] is results["second"]
        assert hasattr(results["second"], "DurableAgentState")
    finally:
        allow_first_exec.set()
        if first.ident is not None:
            first.join(timeout=5)
        if second.ident is not None:
            second.join(timeout=5)
        sys.modules.pop(module_name, None)
        if previous is not None:
            sys.modules[module_name] = previous


def test_original_reader_removes_module_after_initialization_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = b"raise RuntimeError('reader initialization failed')\n"
    reader = tmp_path / "reader.py"
    reader.write_bytes(source)
    monkeypatch.setattr(migration, "PUBLISHED_READER_SHA256", hashlib.sha256(source).hexdigest())
    module_name = "agent_framework_durabletask._migration_original_state"
    previous = sys.modules.pop(module_name, None)

    try:
        with pytest.raises(RuntimeError, match="reader initialization failed"):
            migration._original_reader(reader)
        assert module_name not in sys.modules
    finally:
        sys.modules.pop(module_name, None)
        if previous is not None:
            sys.modules[module_name] = previous


def test_public_entity_authority_binds_exact_snapshot_and_address() -> None:
    private = Ed25519PrivateKey.generate()
    envelope = issue_entity_conversion(
        key_id="operator",
        operator_private_key=private,
        scope=SCOPE,
        entity=ADDRESS,
        source_state=_raw(),
    )
    assert json.loads(
        verify_capability(
            envelope,
            purpose="checkpoint",
            trusted_public_keys={"operator": private.public_key()},
            expected_binding=entity_conversion_binding(SCOPE, entity=ADDRESS),
        )
    ) == {"sourceDigest": digest(_raw())}
    with pytest.raises(ValueError):
        verify_capability(
            envelope,
            purpose="checkpoint",
            trusted_public_keys={"operator": private.public_key()},
            expected_binding=entity_conversion_binding(SCOPE, entity="@dafx-migration-test@other"),
        )


def test_source_observations_are_not_completion_receipts(published_reader: Path) -> None:
    raw = _raw()
    raw["data"]["conversationHistory"][1]["messages"][0]["contents"].insert(
        0,
        {"$type": "reasoning", "text": "private rationale"},
    )
    raw["data"]["conversationHistory"][1]["usage"] = {
        "inputTokenCount": 7,
        "outputTokenCount": None,
        "totalTokenCount": None,
    }
    converted = convert_agent_observations(raw, published_reader=published_reader)
    assert converted["invocationFailurePolicy"] == "current-v1"
    assert [row["contents"][0]["text"] for row in converted["publishedReplayMessages"]] == [
        "old prompt",
        "old answer",
    ]
    assert converted["sourceDigest"] == digest(raw)
    assert converted["observations"][0]["evidence"] == "observed-original-response"
    assert [content["text"] for message in converted["publishedReplayMessages"] for content in message["contents"]] == [
        "old prompt",
        "old answer",
    ]
    assert converted["observations"][0]["response"]["messages"][0]["contents"][0]["type"] == "text_reasoning"
    assert converted["nativeOperations"] == converted["retryAttempts"] == {}
    assert converted["runtime"]["data"]["completionReceipts"] == {}
    assert converted["sourceOptionalUsage"]["entries"][0]["usage"]["outputTokenCount"] is None


def test_queued_operations_convert_once_and_cold_entity_needs_no_reader(
    tmp_path: Path,
    published_reader: Path,
) -> None:
    if not hasattr(EntityContext, "operation_id"):
        pytest.skip("Requires the proposed SDK entity operation identity API.")
    raw = _raw()
    worker, agent, _ = _worker(tmp_path, published_reader, raw)
    request = {"message": "new prompt", "correlationId": "same-correlation"}
    first = _execute(
        worker,
        raw,
        [
            ("run", "native-first", request),
            ("import_checkpoint", "native-import", {"sourceDigest": digest(raw)}),
        ],
    )
    assert all(result.HasField("success") for result in first.results), first
    assert agent.calls == [["old prompt", "old answer", "new prompt"]]
    state = json.loads(first.entityState.value)
    assert len(state["observations"]) == 2
    assert state["observations"][0].get("nativeOperationId") is None
    assert set(state["nativeOperations"]) == {"native-first"}

    cold = TaskHubGrpcWorker(channel=Mock(), emit_trace_spans=False)
    cold._registry.add_entity(create_entity(agent, entity_id="migration-test"))
    repeated = _execute(cold, state, [("run", "native-first", request)])
    assert repeated.results[0].HasField("success"), repeated
    assert repeated.results[0].success.result == first.results[0].success.result
    assert len(agent.calls) == 1
    explicit = _execute(cold, json.loads(repeated.entityState.value), [("run", "native-second", request)])
    assert explicit.results[0].HasField("success"), explicit
    assert len(agent.calls) == 2
    assert set(json.loads(explicit.entityState.value)["nativeOperations"]) == {"native-first", "native-second"}


def test_changed_snapshot_fails_before_old_reader_or_agent(tmp_path: Path) -> None:
    authorized = _raw()
    changed = deepcopy(authorized)
    changed["data"]["conversationHistory"][0]["messages"][0]["contents"][0]["text"] = "changed"
    worker, agent, _ = _worker(tmp_path, tmp_path / "must-not-load.py", changed, authorized=authorized)
    result = _execute(worker, changed, [("run", "native-first", {"message": "new", "correlationId": "same"})])
    assert result.results[0].HasField("failure"), result
    assert "changed after" in str(result.results[0].failure)
    assert agent.calls == []
