# Copyright (c) Microsoft. All rights reserved.

"""Live DTS persistence tests, not live LLM or graceful cancellation tests.

Requires the installed worktree package, pytest, pytest-timeout, redis and
python-dotenv (for the existing conftest), and opentelemetry-sdk. The only required service is DTS at
ENDPOINT (default http://localhost:8080). No model credentials or sample marker.
"""

from __future__ import annotations

import hashlib
import json
import os
import struct
import subprocess
import sys
import time
import uuid
import zlib
from collections import namedtuple
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from queue import Empty, Queue
from threading import Event, Thread
from typing import Any

import grpc
import pytest
from agent_framework import Content, Message
from durabletask.azuremanaged.client import DurableTaskSchedulerClient
from durabletask.client import OrchestrationStatus
from durabletask.entities import EntityInstanceId
from live_retention_worker import AGENT_NAME, DELIVERY_WINDOW_SECONDS, MAX_STATE_BYTES

import agent_framework_durabletask
from agent_framework_durabletask import (
    DTS_MAX_STATE_BYTES,
    DurableAgentState,
    DurableHistoryProvider,
    serialize_agent_response,
)
from agent_framework_durabletask._shared_response import load_terminal_response

pytestmark = [pytest.mark.integration, pytest.mark.requires_dts, pytest.mark.timeout(150)]
WAIT_SECONDS = 30
PACKAGE_ROOT = Path(__file__).resolve().parents[2]
WORKER_SCRIPT = Path(__file__).with_name("live_retention_worker.py")


class _CallDetails(
    namedtuple("CallDetails", "method timeout metadata credentials wait_for_ready compression"), grpc.ClientCallDetails
):
    pass


class _RpcDeadline(grpc.UnaryUnaryClientInterceptor):
    def intercept_unary_unary(self, continuation: Any, details: Any, request: Any) -> Any:
        # SDK get_entity/signal_entity have no timeout parameter. Bound the actual RPC,
        # not just the polling loop around it, while preserving the DTS routing metadata.
        bounded = _CallDetails(
            details.method,
            min(details.timeout, 3.0) if details.timeout is not None else 3.0,
            details.metadata,
            details.credentials,
            details.wait_for_ready,
            details.compression,
        )
        return continuation(bounded, request)


@pytest.fixture
def live_taskhub(unique_taskhub: str) -> str:
    # The existing fixture is module-scoped. Isolate parameter cases too, including
    # pending work left behind when a deliberately killed worker's test fails.
    return f"{unique_taskhub}-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def live_client(dts_available: bool, dts_endpoint: str, live_taskhub: str) -> Iterator[DurableTaskSchedulerClient]:
    assert dts_available
    loaded_package = Path(agent_framework_durabletask.__file__).resolve().parent
    assert loaded_package == PACKAGE_ROOT / "agent_framework_durabletask", (
        "Run with this exact worktree package installed, not an editable install from another checkout"
    )
    client = DurableTaskSchedulerClient(
        host_address=dts_endpoint,
        taskhub=live_taskhub,
        token_credential=None,
        secure_channel=False,
        interceptors=[_RpcDeadline()],
    )
    with client:
        yield client


class _WorkerProcess:
    def __init__(
        self, endpoint: str, taskhub: str, artifacts: Path, block_message_id: str, *, budget_policy: str = "small"
    ) -> None:
        artifacts.mkdir()
        self.artifacts = artifacts
        self.events: Queue[dict[str, Any]] = Queue(maxsize=128)
        self.exited = Event()
        self.log = (artifacts / "worker.log").open("w", encoding="utf-8")
        env = {
            **os.environ,
            "ENDPOINT": endpoint,
            "TASKHUB": taskhub,
            "DURABLE_AGENTS_DEPLOYMENT_MODE": "isolated_v2",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": str(PACKAGE_ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""),
        }
        try:
            self.process = subprocess.Popen(
                [
                    sys.executable,
                    "-B",
                    "-u",
                    str(WORKER_SCRIPT),
                    "--endpoint",
                    endpoint,
                    "--taskhub",
                    taskhub,
                    "--artifacts",
                    str(artifacts),
                    "--block-message-id",
                    block_message_id,
                    "--budget-policy",
                    budget_policy,
                ],
                cwd=artifacts,
                env=env,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=self.log,
                text=True,
                encoding="utf-8",
                shell=False,
            )
        except BaseException:
            self.log.close()
            raise
        self.reader = Thread(target=self._read_events, name="live-retention-control", daemon=True)
        try:
            self.reader.start()
        except BaseException:
            self.hard_stop()
            if self.process.stdin is not None:
                self.process.stdin.close()
            if self.process.stdout is not None:
                self.process.stdout.close()
            self.log.close()
            raise

    def _read_events(self) -> None:
        try:
            assert self.process.stdout is not None
            while line := self.process.stdout.readline(4097):
                if len(line) > 4096:
                    raise ValueError("Oversized worker control record")
                self.events.put_nowait(json.loads(line))
        except Exception:
            with suppress(Exception):
                self.events.put_nowait({"event": "protocol_error"})
        finally:
            self.exited.set()

    def event(self, expected: str) -> dict[str, Any]:
        deadline = time.monotonic() + WAIT_SECONDS
        while time.monotonic() < deadline:
            try:
                record = self.events.get(timeout=min(0.1, max(0.001, deadline - time.monotonic())))
            except Empty:
                self.check_alive()
                continue
            assert record.get("event") == expected, f"Expected {expected}, received control event {record.get('event')}"
            return record
        raise TimeoutError(f"No {expected} control record within {WAIT_SECONDS}s. Inspect temporary worker.log")

    def check_alive(self) -> None:
        if self.exited.is_set() or self.process.poll() is not None:
            raise RuntimeError("Worker exited or control pipe failed. Inspect temporary worker.log")

    def command(self, command: str) -> None:
        self.check_alive()
        assert self.process.stdin is not None
        self.process.stdin.write(json.dumps({"command": command}) + "\n")
        self.process.stdin.flush()

    def hard_stop(self) -> None:
        self.process.kill()
        self.process.wait(timeout=10)
        assert self.process.returncode is not None

    def close(self) -> None:
        try:
            if self.process.poll() is None:
                with suppress(BrokenPipeError, OSError, RuntimeError):
                    self.command("stop")
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.hard_stop()
        finally:
            if self.process.poll() is None:
                self.hard_stop()
            if self.process.stdin is not None:
                with suppress(BrokenPipeError, OSError):
                    self.process.stdin.close()
            self.reader.join(timeout=5)
            if self.process.stdout is not None:
                self.process.stdout.close()
            self.log.close()
            assert not self.reader.is_alive(), "Worker control thread did not terminate"

    def captured(self, message_id: str) -> list[dict[str, Any]]:
        record = self.event("model_entered")
        assert record["message_id"] == message_id
        return json.loads((self.artifacts / f"model-{record['ordinal']}.json").read_text(encoding="utf-8"))

    def removed_measurement(self) -> int:
        self.command("metrics")
        self.event("metrics")
        rows = json.loads((self.artifacts / "metrics.json").read_text(encoding="utf-8"))
        for row in rows:
            assert row["attributes"] == {
                "mechanism": "pressure",
                "outcome": "staged",
                "commit_status": "not_attempted",
            }
        return sum(row["value"] for row in rows)


@contextmanager
def _worker(
    endpoint: str, hub: str, artifacts: Path, block: str = "", *, budget_policy: str = "small"
) -> Iterator[_WorkerProcess]:
    worker = _WorkerProcess(endpoint, hub, artifacts, block, budget_policy=budget_policy)
    try:
        worker.event("started")
        yield worker
    finally:
        worker.close()


def _poll(worker: _WorkerProcess, probe: Callable[[], Any], description: str) -> Any:
    deadline = time.monotonic() + WAIT_SECONDS
    while time.monotonic() < deadline:
        worker.check_alive()
        if result := probe():
            return result
        # A bounded backend poll, never a sleep used as evidence of completion.
        worker.exited.wait(min(0.1, max(0, deadline - time.monotonic())))
    raise TimeoutError(f"DTS did not expose {description} within {WAIT_SECONDS}s")


def _snapshot(client: DurableTaskSchedulerClient, entity: EntityInstanceId) -> dict[str, Any]:
    metadata = client.get_entity(entity)
    assert metadata is not None, "Expected an existing backend entity"
    raw = metadata.get_state()
    state = json.loads(raw) if isinstance(raw, str) else raw
    assert isinstance(state, dict), "Backend returned no JSON entity state"
    return {
        "id": str(metadata.id),
        "last_modified": metadata.last_modified.isoformat(),
        "backlog_queue_size": metadata.backlog_queue_size,
        "state": state,
    }


def _committed(
    client: DurableTaskSchedulerClient, entity: EntityInstanceId, correlation: str, worker: _WorkerProcess
) -> dict[str, Any]:
    def probe() -> dict[str, Any] | None:
        metadata = client.get_entity(entity)
        if metadata is None or not metadata.get_state():
            return None
        raw = metadata.get_state()
        state = json.loads(raw) if isinstance(raw, str) else raw
        if correlation not in state.get("data", {}).get("completionReceipts", {}):
            return None
        return _snapshot(client, entity)

    snapshot = _poll(worker, probe, f"completion receipt for {correlation}")
    (worker.artifacts / f"committed-{correlation}.json").write_text(json.dumps(snapshot), encoding="utf-8")
    receipt = snapshot["state"]["data"]["completionReceipts"][correlation]
    assert receipt["outcome"] == "succeeded", f"The real Agent failed for {correlation}"
    return snapshot["state"]


def _equal(actual: Any, expected: Any, label: str) -> None:
    # Compare entire payloads without leaking large media into pytest assertion output.
    actual_json = json.dumps(actual, sort_keys=True, separators=(",", ":"), allow_nan=False)
    expected_json = json.dumps(expected, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if actual_json != expected_json:

        def digest(encoded: str) -> str:
            return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

        pytest.fail(f"{label}: full JSON mismatch ({digest(actual_json)} != {digest(expected_json)})")


def _stored(raw: dict[str, Any]) -> list[dict[str, Any]]:
    state = DurableAgentState.from_json(json.dumps(raw))
    return [
        message.to_chat_message().to_dict() for entry in state.data.conversation_history for message in entry.messages
    ]


def _ingested(raw: dict[str, Any]) -> dict[str, Any]:
    profile = raw["data"]["pythonIngestion"]
    assert profile["profile"] == "agent-framework-python.ingestion"
    assert type(profile["version"]) is int and profile["version"] == 1
    assert isinstance(profile["messages"], dict)
    return profile["messages"]


def _model_history(stored: list[dict[str, Any]]) -> list[dict[str, Any]]:
    expected = deepcopy(stored)
    for message in expected:
        message.setdefault("additional_properties", {})["_attribution"] = {
            "source_id": DurableHistoryProvider.DEFAULT_SOURCE_ID,
            "source_type": "DurableHistoryProvider",
        }
    return expected


def _input(kind: str, turn: str) -> list[Message]:
    def chunk(tag: bytes, value: bytes) -> bytes:
        return struct.pack(">I", len(value)) + tag + value + struct.pack(">I", zlib.crc32(tag + value))

    width, height = 128, 64
    pixels = hashlib.shake_256(b"durable-media-pressure").digest(width * height)
    rows = b"".join(b"\x00" + pixels[row * width : (row + 1) * width] for row in range(height))
    png = (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 0, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(rows))
        + chunk(b"IEND", b"")
    )
    if kind == "inline-png":
        media = Content.from_data(png, "image/png")
    elif kind == "inline-file":
        media = Content.from_data((f"{turn}: inline document 界\n" * 256).encode(), "text/plain")
    else:
        raise ValueError(f"Unexpected media case: {kind}")
    properties = {"application": {"type": "text", "values": [turn, "界", 0, False, None]}}
    return [
        Message(
            "user",
            [Content.from_text(f"{turn}: " + "context " * 100), media],
            message_id=f"{turn}-input",
            author_name="media-user",
            additional_properties=deepcopy(properties),
        ),
        Message(
            "assistant",
            [Content.from_function_call(f"{turn}-call", "lookup", arguments={"query": turn})],
            message_id=f"{turn}-call-message",
            author_name="planner",
            additional_properties=deepcopy(properties),
        ),
        Message(
            "tool",
            [Content.from_function_result(f"{turn}-call", result={"records": [turn, "界", False]})],
            message_id=f"{turn}-result-message",
            author_name="lookup",
            additional_properties=deepcopy(properties),
        ),
    ]


def _request(correlation: str, messages: list[Message]) -> dict[str, Any]:
    return {
        "message": "projected test input",
        "correlationId": correlation,
        "contextMessages": [message.to_dict() for message in messages],
    }


def _answer(message_id: str) -> dict[str, Any]:
    return Message(
        "assistant", [f"answer:{message_id}"], message_id=f"{message_id}-answer", author_name="retention-model"
    ).to_dict()


@pytest.mark.parametrize("kind", ["inline-png", "inline-file"])
def test_live_media_pressure_cold_read_and_exact_model_input(
    kind: str, live_client: DurableTaskSchedulerClient, dts_endpoint: str, live_taskhub: str, tmp_path: Path
) -> None:
    entity = EntityInstanceId(entity=f"dafx-{AGENT_NAME}", key=uuid.uuid4().hex)
    originals: dict[str, dict[str, Any]] = {}
    previous: list[dict[str, Any]] = []
    raw: dict[str, Any] = {}
    previous_removed = 0
    previous_measured = 0
    with _worker(dts_endpoint, live_taskhub, tmp_path / "warm") as warm:
        for index in range(8):
            correlation = f"turn-{index}"
            inputs = _input(kind, correlation)
            current_id = f"{correlation}-input"
            live_client.signal_entity(entity, "run", _request(correlation, inputs))
            expected_input = [*_model_history(previous), *[message.to_dict() for message in inputs]]
            _equal(warm.captured(current_id), expected_input, "model input")
            raw = _committed(live_client, entity, correlation, warm)
            expected_turn = [*[message.to_dict() for message in inputs], _answer(current_id)]
            originals.update({message["message_id"]: message for message in expected_turn})
            retained = _stored(raw)
            retained_ids = {message["message_id"] for message in retained}
            assert {message["message_id"] for message in expected_turn} <= retained_ids
            _equal(
                retained, [value for key, value in originals.items() if key in retained_ids], "retained payload/order"
            )
            for turn in range(index + 1):
                pair = {f"turn-{turn}-call-message", f"turn-{turn}-result-message"}
                assert pair <= retained_ids or pair.isdisjoint(retained_ids), "Pressure split an atomic tool pair"
            removed = len(originals) - len(retained)
            assert (raw["data"].get("truncation") or {}).get("evictedMessageCount", 0) == removed
            measured = warm.removed_measurement()
            assert measured - previous_measured == removed - previous_removed
            assert len(json.dumps(raw)) < int(MAX_STATE_BYTES * 0.85)
            previous, previous_removed, previous_measured = retained, removed, measured
        assert previous_removed >= 4, "This must exercise real pressure eviction, not merely media serialization"
        assert sum(message["role"] == "user" for message in previous) >= 2, "Retain older media for cold model replay"
        assert len(raw["data"]["completionReceipts"]) == len(raw["data"]["terminalResults"]) == 8

    with _worker(dts_endpoint, live_taskhub, tmp_path / "cold") as cold:
        snapshot = _snapshot(live_client, entity)
        (cold.artifacts / "cold-read.json").write_text(json.dumps(snapshot), encoding="utf-8")
        _equal(snapshot["state"], raw, "cold backend read")
        current = Message("user", ["next turn"], message_id="cold-input")
        # Resending an evicted projected input must not resurrect it after a process restart.
        evicted = set(originals) - {message["message_id"] for message in previous}
        assert "turn-0-input" in evicted
        live_client.signal_entity(entity, "run", _request("cold", [*_input(kind, "turn-0"), current]))
        _equal(cold.captured("cold-input"), [*_model_history(previous), current.to_dict()], "cold model input")
        final = _committed(live_client, entity, "cold", cold)
        final_messages = _stored(final)
        assert evicted.isdisjoint(message["message_id"] for message in final_messages)
        assert {"cold-input", "cold-input-answer"} <= {message["message_id"] for message in final_messages}
        originals.update({"cold-input": current.to_dict(), "cold-input-answer": _answer("cold-input")})
        final_ids = {message["message_id"] for message in final_messages}
        _equal(
            final_messages, [value for key, value in originals.items() if key in final_ids], "cold persisted payloads"
        )
        for turn in range(8):
            pair = {f"turn-{turn}-call-message", f"turn-{turn}-result-message"}
            assert pair <= final_ids or pair.isdisjoint(final_ids), "Cold pressure split an atomic tool pair"
        _equal(
            {key: _ingested(final)[key] for key in _ingested(raw)},
            _ingested(raw),
            "cold replay preserves ingestion receipts",
        )
        assert set(_ingested(final)) == {*_ingested(raw), "cold-input"}
        total_removed = len(originals) - len(final_messages)
        assert final["data"]["truncation"]["evictedMessageCount"] == total_removed
        assert cold.removed_measurement() == total_removed - previous_removed
        assert len(json.dumps(final)) < int(MAX_STATE_BYTES * 0.85)
        _equal(
            final["data"]["completionReceipts"]["turn-0"],
            raw["data"]["completionReceipts"]["turn-0"],
            "evicted turn completion receipt",
        )
        for correlation, mailbox in raw["data"]["terminalResults"].items():
            _equal(final["data"]["terminalResults"][correlation], mailbox, "retained mailbox through pressure")
            assert (
                datetime.fromisoformat(mailbox["resultExpiresAt"]) - datetime.fromisoformat(mailbox["completedAt"])
            ).total_seconds() == DELIVERY_WINDOW_SECONDS
        assert set(final["data"]["terminalResults"]) == {*raw["data"]["terminalResults"], "cold"}


@pytest.mark.parametrize("kind", ["ascii", "unicode", "escaped", "tool-result"])
@pytest.mark.parametrize("budget_policy", ["default", "disabled"])
def test_live_dts_default_pressure_and_opt_out_near_backend_limit(
    kind: str,
    budget_policy: str,
    live_client: DurableTaskSchedulerClient,
    dts_endpoint: str,
    live_taskhub: str,
    tmp_path: Path,
) -> None:
    entity = EntityInstanceId(entity=f"dafx-{AGENT_NAME}", key=uuid.uuid4().hex)
    chunk = {
        "ascii": "x" * 200_000,
        "unicode": "\u754c" * 33_333,
        "escaped": '"\\\n' * 33_333,
        "tool-result": "x" * 100_000,
    }[kind]
    high = int(DTS_MAX_STATE_BYTES * 0.85)
    expected_messages: list[dict[str, Any]] = []
    retained: list[dict[str, Any]] = []
    committed: dict[str, Any] = {}
    with _worker(dts_endpoint, live_taskhub, tmp_path / "warm", budget_policy=budget_policy) as warm:
        for index in range(5):
            correlation = f"boundary-{index}"
            current_id = f"{correlation}-input"
            inputs = [Message("user", [chunk if kind != "tool-result" else "tool boundary"], message_id=current_id)]
            if kind == "tool-result":
                inputs.extend([
                    Message(
                        "assistant",
                        [Content.from_function_call(f"{correlation}-call", "lookup", arguments={})],
                        message_id=f"{correlation}-call-message",
                    ),
                    Message(
                        "tool",
                        [Content.from_function_result(f"{correlation}-call", result={"data": chunk})],
                        message_id=f"{correlation}-result-message",
                    ),
                ])
            request = _request(correlation, inputs)
            assert len(json.dumps(request).encode("utf-8")) < DTS_MAX_STATE_BYTES
            live_client.signal_entity(entity, "run", request)
            _equal(
                warm.captured(current_id),
                [*_model_history(retained), *[item.to_dict() for item in inputs]],
                "model input",
            )
            committed = _committed(live_client, entity, correlation, warm)
            expected_messages.extend([*[item.to_dict() for item in inputs], _answer(current_id)])
            retained = _stored(committed)
            retained_ids = {item["message_id"] for item in retained}
            _equal(
                retained,
                [item for item in expected_messages if item["message_id"] in retained_ids],
                "pressure preserves retained values and ordering",
            )
            assert current_id in retained_ids and f"{current_id}-answer" in retained_ids
            metadata = live_client.get_entity(entity)
            assert metadata is not None
            serialized = metadata.get_state()
            assert isinstance(serialized, str)
            actual_bytes = len(serialized.encode("utf-8"))
            assert actual_bytes == len(json.dumps(committed))
            assert actual_bytes < DTS_MAX_STATE_BYTES
            if budget_policy == "default":
                assert actual_bytes < high
            for previous in range(index + 1):
                if kind == "tool-result":
                    pair = {f"boundary-{previous}-call-message", f"boundary-{previous}-result-message"}
                    assert pair <= retained_ids or pair.isdisjoint(retained_ids)
            if index < 4:
                assert not committed["data"].get("truncation")
        removed = len(expected_messages) - len(retained)
        if budget_policy == "default":
            assert removed > 0
            assert committed["data"]["truncation"]["evictedMessageCount"] == removed
            assert warm.removed_measurement() == removed
        else:
            assert removed == 0 and high < actual_bytes < DTS_MAX_STATE_BYTES
            assert not committed["data"].get("truncation")
        assert len(committed["data"]["completionReceipts"]) == len(committed["data"]["terminalResults"]) == 5

    with _worker(dts_endpoint, live_taskhub, tmp_path / "cold", budget_policy=budget_policy) as cold:
        _equal(_snapshot(live_client, entity)["state"], committed, "cold read preserves pressure result")
        current = Message("user", ["cold continuation"], message_id="cold-input")
        live_client.signal_entity(entity, "run", _request("cold", [current]))
        _equal(cold.captured("cold-input"), [*_model_history(retained), current.to_dict()], "cold model input")
        final = _committed(live_client, entity, "cold", cold)
        assert len(json.dumps(final)) < (high if budget_policy == "default" else DTS_MAX_STATE_BYTES)
        _equal(
            {key: final["data"]["completionReceipts"][key] for key in committed["data"]["completionReceipts"]},
            committed["data"]["completionReceipts"],
            "pressure preserves completion receipts",
        )


@pytest.mark.parametrize("kind", ["ascii", "unicode", "escaped"])
@pytest.mark.parametrize("delta", [-1, 0, 1], ids=["below", "at", "above"])
def test_live_entity_input_serialization_boundary(
    kind: str, delta: int, live_client: DurableTaskSchedulerClient
) -> None:
    unit = {"ascii": "x", "unicode": "\u754c", "escaped": '"\\\n'}[kind]
    encoded_unit_size = len(json.dumps(unit).encode("utf-8")) - 2
    size = DTS_MAX_STATE_BYTES + delta
    repetitions, remainder = divmod(size - len(json.dumps({"payload": ""})), encoded_unit_size)
    payload = {"payload": unit * repetitions + "x" * remainder}
    assert len(json.dumps(payload).encode("utf-8")) == size
    entity = EntityInstanceId(entity="retention-input-boundary", key=uuid.uuid4().hex)
    if delta <= 0:
        live_client.signal_entity(entity, "probe", payload)
    else:
        with pytest.raises(grpc.RpcError) as rejected:
            live_client.signal_entity(entity, "probe", payload)
        assert rejected.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert "1048576 UTF8-encoded bytes" in rejected.value.details()


def test_live_default_pressure_rejects_protected_state_without_losing_previous_commit(
    live_client: DurableTaskSchedulerClient, dts_endpoint: str, live_taskhub: str, tmp_path: Path
) -> None:
    entity = EntityInstanceId(entity=f"dafx-{AGENT_NAME}", key=uuid.uuid4().hex)
    with _worker(dts_endpoint, live_taskhub, tmp_path / "protected", budget_policy="default") as worker:
        seed = Message("user", ["preserve this commit"], message_id="seed-input")
        live_client.signal_entity(entity, "run", _request("seed", [seed]))
        worker.captured("seed-input")
        baseline = _committed(live_client, entity, "seed", worker)
        oversized = Message("user", ["x" * 920_000], message_id="protected-input")
        payload = {"key": entity.key, "request": _request("protected", [oversized])}
        assert len(json.dumps(payload).encode("utf-8")) < DTS_MAX_STATE_BYTES
        instance = live_client.schedule_new_orchestration("live_retention_call", input=payload)
        finished = False
        try:
            worker.captured("protected-input")

            def terminal() -> Any:
                state = live_client.get_orchestration_state(instance)
                return (
                    state
                    if state is not None
                    and state.runtime_status in (OrchestrationStatus.COMPLETED, OrchestrationStatus.FAILED)
                    else None
                )

            failed = _poll(worker, terminal, "protected-state capacity failure")
            finished = True
            assert failed.runtime_status == OrchestrationStatus.FAILED
            assert failed.failure_details is not None and "capacity" in failed.failure_details.message
            _equal(_snapshot(live_client, entity)["state"], baseline, "capacity failure preserves committed state")
            smaller = Message("user", ["retry with a smaller input"], message_id="protected-input")
            live_client.signal_entity(entity, "run", _request("protected", [smaller]))
            _equal(
                worker.captured("protected-input"),
                [*_model_history(_stored(baseline)), smaller.to_dict()],
                "failed oversized input does not enter retry history",
            )
            final = _committed(live_client, entity, "protected", worker)
            _equal(
                _stored(final),
                [*_stored(baseline), smaller.to_dict(), _answer("protected-input")],
                "smaller retry commits normally",
            )
        finally:
            if not finished:
                with suppress(grpc.RpcError):
                    live_client.terminate_orchestration(instance)


def test_live_hard_stop_before_commit_repeats_effect_but_committed_duplicate_does_not(
    live_client: DurableTaskSchedulerClient, dts_endpoint: str, live_taskhub: str, tmp_path: Path
) -> None:
    entity = EntityInstanceId(entity=f"dafx-{AGENT_NAME}", key=uuid.uuid4().hex)
    target = Message("user", ["simulated external effect"], message_id="target-input")
    request = _request("target", [target])
    with _worker(dts_endpoint, live_taskhub, tmp_path / "interrupted", "target-input") as first:
        seed = Message("user", ["establish committed baseline"], message_id="seed-input")
        live_client.signal_entity(entity, "run", _request("seed", [seed]))
        first.captured("seed-input")
        baseline = _committed(live_client, entity, "seed", first)
        live_client.signal_entity(entity, "run", request)  # Accepted is not committed.
        captured = first.captured("target-input")
        _equal(captured, [*_model_history(_stored(baseline)), target.to_dict()], "interrupted model input")
        first.hard_stop()  # No model response, history after-hook, or set_state can finish.
        snapshot = _snapshot(live_client, entity)
        (tmp_path / "after-hard-stop.json").write_text(json.dumps(snapshot), encoding="utf-8")
        _equal(snapshot["state"], baseline, "backend state after hard stop")
        assert "target" not in snapshot["state"]["data"]["completionReceipts"]
        assert "target" not in snapshot["state"]["data"]["terminalResults"]

    with _worker(dts_endpoint, live_taskhub, tmp_path / "retry", "target-input") as retry:
        # The killed work item may redeliver before this explicit retry. Either must use
        # committed state, and the same correlation must execute once in this new process.
        live_client.signal_entity(entity, "run", request)
        _equal(retry.captured("target-input"), captured, "retried model input")
        _equal(_snapshot(live_client, entity)["state"], baseline, "blocked retry is still uncommitted")
        retry.command("release")
        committed = _committed(live_client, entity, "target", retry)
        assert committed["data"]["completionReceipts"]["target"]["outcome"] == "succeeded"
        _equal(_stored(committed), [*_stored(baseline), target.to_dict(), _answer("target-input")], "committed retry")
        retry.hard_stop()  # Only after authoritative scheduler readback, never a warm-cache acknowledgement.

    with _worker(dts_endpoint, live_taskhub, tmp_path / "duplicate") as duplicate:
        _equal(_snapshot(live_client, entity)["state"], committed, "post-commit cold read")
        instance = live_client.schedule_new_orchestration(
            "live_retention_duplicate", input={"key": entity.key, "request": request}
        )
        barrier_completed = False
        try:

            def completed_barrier() -> Any:
                state = live_client.get_orchestration_state(instance)
                if state is not None:
                    state.raise_if_failed()
                    if state.runtime_status == OrchestrationStatus.COMPLETED:
                        return state
                return None

            barrier = _poll(duplicate, completed_barrier, "acknowledged duplicate signal/call")
            barrier_completed = True
            _equal(
                json.loads(barrier.serialized_output),
                serialize_agent_response(
                    load_terminal_response(committed["data"]["terminalResults"]["target"]["response"])
                ),
                "duplicate response",
            )
            snapshot = _snapshot(live_client, entity)
            (duplicate.artifacts / "duplicate-read.json").write_text(json.dumps(snapshot), encoding="utf-8")
            _equal(snapshot["state"], committed, "duplicate must not rewrite completion or expiry")
            assert not list(duplicate.artifacts.glob("model-*.json")), "Committed duplicate executed the model"
            assert not list(duplicate.artifacts.glob("effect-*.json")), "Committed duplicate repeated the effect"
        finally:
            if not barrier_completed:
                with suppress(grpc.RpcError):
                    live_client.terminate_orchestration(instance)

    effects = [json.loads(path.read_text(encoding="utf-8")) for path in tmp_path.glob("*/effect-*.json")]
    assert sum(effect["message_id"] == "target-input" for effect in effects) == 2
    # The delivery window is not shortened to make duplicate suppression or pressure fit.
    mailbox = committed["data"]["terminalResults"]["target"]
    window = datetime.fromisoformat(mailbox["resultExpiresAt"]) - datetime.fromisoformat(mailbox["completedAt"])
    assert window.total_seconds() == DELIVERY_WINDOW_SECONDS
