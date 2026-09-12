# Copyright (c) Microsoft. All rights reserved.

"""Live Functions/Azure Storage retention with a deterministic core model, not Foundry.

Requires func v4, Azurite on 10000/10001/10002 (with --skipApiVersionCheck),
DTS on 8080, and the selected venv's test dependencies including psutil and
opentelemetry-sdk. The test generates its app/settings under tmp_path and supplies
local emulator defaults. It never uses the sample-starting fixture.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import site
import struct
import subprocess
import sys
import time
import uuid
import zlib
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from threading import Event
from typing import Any

import agent_framework_durabletask
import psutil
import pytest
import requests
from agent_framework import Content, Message
from agent_framework_durabletask import AgentSessionId, DurableAgentState, DurableHistoryProvider

import agent_framework_azurefunctions

pytestmark = [
    pytest.mark.integration,
    pytest.mark.orchestration,
    pytest.mark.timeout(170),
    # Collection-only reuse of the existing no-LLM category. Without this marker
    # conftest requires Foundry even for generated apps. No sample is launched.
    pytest.mark.sample("13_subworkflow_hitl"),
]
PYTHON_ROOT = Path(__file__).resolve().parents[4]
TEMPLATE = Path(__file__).with_name("live_media_app") / "function_app.py"
AGENT = "live-media-retention"
MAX_STATE_BYTES = 50_000
DELIVERY_WINDOW_SECONDS = 3600
TURNS = 8


def _equal(actual: Any, expected: Any, label: str) -> None:
    # Equality covers full JSON, not just text/counts. Only hashes reach failures.
    if actual != expected:

        def digest(value: Any) -> str:
            return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()

        pytest.fail(f"{label}: full JSON mismatch ({digest(actual)} != {digest(expected)})", pytrace=False)


def _stored(raw: dict[str, Any]) -> list[dict[str, Any]]:
    state = DurableAgentState.from_json(json.dumps(raw))
    return [
        message.to_chat_message().to_dict() for entry in state.data.conversation_history for message in entry.messages
    ]


def _model_history(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = deepcopy(messages)
    for message in result:
        message.setdefault("additional_properties", {})["_attribution"] = {
            "source_id": DurableHistoryProvider.DEFAULT_SOURCE_ID,
            "source_type": "DurableHistoryProvider",
        }
    return result


def _inputs(kind: str, turn: str) -> list[dict[str, Any]]:
    # Valid PNG scanlines with incompressible pixels make binary payload bytes a
    # substantial part of pressure. The text is not the dominant storage cost.
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
        raise ValueError(f"Unknown media case: {kind}")
    properties = {"application": {"type": "text", "values": [turn, "界", 0, False, None]}}
    return [
        Message(
            "user",
            [Content.from_text(f"{turn}: " + "context " * 100), media],
            message_id=f"{turn}-input",
            author_name="media-user",
            additional_properties=deepcopy(properties),
        ).to_dict(),
        Message(
            "assistant",
            [Content.from_function_call(f"{turn}-call", "lookup", arguments={"query": turn})],
            message_id=f"{turn}-call-message",
            author_name="planner",
            additional_properties=deepcopy(properties),
        ).to_dict(),
        Message(
            "tool",
            [Content.from_function_result(f"{turn}-call", result={"records": [turn, "界", False]})],
            message_id=f"{turn}-result-message",
            author_name="lookup",
            additional_properties=deepcopy(properties),
        ).to_dict(),
    ]


def _answer(current_id: str) -> dict[str, Any]:
    return Message(
        "assistant", [f"answer:{current_id}"], message_id=f"{current_id}-answer", author_name="retention-model"
    ).to_dict()


class _Host:
    def __init__(self, app: Path, port: int, env: dict[str, str], deadline: float, epoch: str) -> None:
        self.url = f"http://127.0.0.1:{port}/api"
        self.deadline = deadline
        self.log = (app / f"{epoch}-host.log").open("w", encoding="utf-8")
        try:
            self.process = subprocess.Popen(
                ["func", "start", "--port", str(port)],
                cwd=app,
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=self.log,
                stderr=subprocess.STDOUT,
                shell=sys.platform == "win32",  # Core Tools can be a .cmd shim.
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
                start_new_session=sys.platform != "win32",
            )
        except BaseException:
            self.log.close()
            raise

    def wait(self, probe: Callable[[], Any], description: str, seconds: int = 30) -> Any:
        end = min(self.deadline, time.monotonic() + seconds)
        pause = Event()
        while time.monotonic() < end:
            assert self.process.poll() is None, "Functions host exited. Inspect the temporary host log"
            try:
                if result := probe():
                    return result
            except (requests.ConnectionError, requests.Timeout):
                pass
            # Pacing is not proof of completion. Only the HTTP/backend predicate is.
            pause.wait(min(0.1, max(0, end - time.monotonic())))
        pytest.fail(f"Timed out waiting for {description}. Inspect the temporary host log", pytrace=False)

    def get(self, path: str, missing_ok: bool = False) -> Any:
        response = requests.get(f"{self.url}/{path}", timeout=3)
        if missing_ok and response.status_code in (404, 503):
            return None
        assert response.status_code == 200, f"GET {path}: HTTP {response.status_code}"
        return response.json()

    def turn(self, correlation: str, messages: list[dict[str, Any]], session: str) -> dict[str, Any]:
        response = requests.post(
            f"{self.url}/retention/run",
            json={"message": "synthetic projected input", "correlationId": correlation, "contextMessages": messages},
            timeout=5,
        )
        assert response.status_code == 202, f"Signal returned HTTP {response.status_code}"
        _equal(response.json(), {"correlation": correlation, "session": session}, "signal acknowledgement")

        def committed() -> dict[str, Any] | None:
            raw = self.get("retention/state", missing_ok=True)
            if raw and correlation in raw.get("data", {}).get("completedCorrelations", {}):
                receipt = raw["data"]["completedCorrelations"][correlation]
                assert receipt["outcome"] == "succeeded", "The real Functions agent operation failed"
                return raw
            return None

        return self.wait(committed, f"persisted completion receipt for {correlation}")


@contextmanager
def _host(app: Path, env: dict[str, str], deadline: float, epoch: str, harness: Any) -> Iterator[_Host]:
    host = _Host(app, harness._find_available_port(), env, deadline, epoch)
    try:
        host.wait(lambda: host.get("health", missing_ok=True), "Functions health", seconds=50)
        yield host
    finally:
        # Require psutil (imported above) so the existing cleanup also kills workers.
        descendants: list[psutil.Process] = []
        try:
            with suppress(psutil.NoSuchProcess):
                descendants = psutil.Process(host.process.pid).children(recursive=True)
        finally:
            try:
                harness._cleanup_function_app(host.process)
                host.process.wait(timeout=5)
                assert not any(child.is_running() for child in descendants), "Functions worker survived host cleanup"
            finally:
                host.log.close()


def _prepare(app: Path, session: str, hub: str) -> dict[str, str]:
    app.mkdir()
    source_paths = [PYTHON_ROOT / "packages" / name for name in ("azurefunctions", "durabletask")]
    for package, path in zip((agent_framework_azurefunctions, agent_framework_durabletask), source_paths):
        assert package.__file__ is not None
        assert Path(package.__file__).resolve().parent == path / package.__name__, (
            "Run using packages from this exact pr59 worktree"
        )
    shutil.copyfile(TEMPLATE, app / "function_app.py")
    config = {
        "agent": AGENT,
        "session": session,
        "max_state_bytes": MAX_STATE_BYTES,
        "delivery_window_seconds": DELIVERY_WINDOW_SECONDS,
        "azurefunctions_source": str(source_paths[0] / "agent_framework_azurefunctions"),
        "durabletask_source": str(source_paths[1] / "agent_framework_durabletask"),
    }
    (app / "live_config.json").write_text(json.dumps(config), encoding="utf-8")
    (app / "host.json").write_text(
        json.dumps({
            "version": "2.0",
            "extensionBundle": {"id": "Microsoft.Azure.Functions.ExtensionBundle", "version": "[4.*, 5.0.0)"},
            "extensions": {"durableTask": {"hubName": hub}},
            "logging": {"logLevel": {"default": "Warning"}},
        }),
        encoding="utf-8",
    )
    settings = {
        "FUNCTIONS_WORKER_RUNTIME": "python",
        "FUNCTIONS_WORKER_PROCESS_COUNT": "1",
        "AzureWebJobsStorage": "UseDevelopmentStorage=true",
        "DURABLE_TASK_SCHEDULER_CONNECTION_STRING": "Endpoint=http://localhost:8080;Authentication=None",
        "TASKHUB_NAME": hub,
        "AzureFunctionsJobHost__extensions__durableTask__hubName": hub,
        "DURABLE_AGENTS_DEPLOYMENT_MODE": "isolated_v2",
        "languageWorkers__python__defaultExecutablePath": sys.executable,
    }
    (app / "local.settings.json").write_text(json.dumps({"IsEncrypted": False, "Values": settings}), encoding="utf-8")
    return {
        **os.environ,
        **settings,
        "VIRTUAL_ENV": sys.prefix,
        "PATH": str(Path(sys.executable).parent) + os.pathsep + os.environ.get("PATH", ""),
        "PYTHONDONTWRITEBYTECODE": "1",
        # Keep parent worker/grpc workarounds, but prefer both exact source roots.
        "PYTHONPATH": os.pathsep.join([*map(str, source_paths), os.getenv("PYTHONPATH", ""), *site.getsitepackages()]),
    }


def _capture(host: _Host, boot: str, calls: int, current_id: str, session: str, expected: Any) -> None:
    record = host.get("retention/capture")
    assert record["boot"] == boot and record["calls"] == calls
    capture = record["capture"]
    assert capture["boot"] == boot and capture["calls"] == calls and capture["current_id"] == current_id
    _equal(capture["messages"], expected, "full next-model input")
    _equal(
        capture["context"],
        {
            "provider": "AzureFunctionEntityStateProvider",
            "type": "DurableEntityContext",
            "entity_name": AgentSessionId.to_entity_name(AGENT),
            "entity_key": session,
            "operation": "run",
        },
        "real Functions context on the async bridge",
    )


def _retained(raw: dict[str, Any], originals: dict[str, dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    retained = _stored(raw)
    ids = {message["message_id"] for message in retained}
    _equal(retained, [message for key, message in originals.items() if key in ids], "persisted media/metadata/order")
    for turn in range(TURNS):
        pair = {f"turn-{turn}-call-message", f"turn-{turn}-result-message"}
        assert pair <= ids or pair.isdisjoint(ids), "Pressure split an atomic tool pair"
    removed = len(originals) - len(retained)
    assert (raw["data"].get("truncation") or {}).get("evictedMessageCount", 0) == removed
    assert len(json.dumps(raw)) < int(MAX_STATE_BYTES * 0.85)
    return retained, removed


def _metrics(host: _Host, boot: str, removed: int, calls: int) -> None:
    record = host.get("retention/metrics")
    assert record["boot"] == boot
    rows = record["rows"]
    deletions = [row for row in rows if row["name"] == "durable.retention.removed_messages"]
    for row in deletions:
        _equal(
            row["attributes"],
            {"mechanism": "pressure", "outcome": "staged", "commit_status": "not_attempted"},
            "bounded deletion metric labels",
        )
    assert sum(row["value"] for row in deletions) == removed
    for metric, extra in (
        ("write_attempts", {"stage": "set_state"}),
        ("operations", {}),
    ):
        observations = [row for row in rows if row["name"] == f"durable.retention.{metric}"]
        assert sum(row["value"] for row in observations) == calls
        for row in observations:
            attributes = row["attributes"]
            assert isinstance(attributes["deletion_staged"], bool)
            _equal(
                attributes,
                {
                    **extra,
                    "outcome": "returned",
                    "commit_status": "unknown",
                    "deletion_staged": attributes["deletion_staged"],
                },
                "host write observations are not commit proof",
            )


@pytest.mark.parametrize("kind", ["inline-png", "inline-file"])
def test_live_functions_media_pressure_cold_json_and_exact_next_model(
    kind: str, tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    # Resolve the already-loaded local conftest, not an identically named DTS module.
    harness_path = Path(__file__).with_name("conftest.py").resolve()
    harness = next(
        plugin
        for plugin in request.config.pluginmanager.get_plugins()
        if getattr(plugin, "__file__", None) and Path(plugin.__file__).resolve() == harness_path
    )
    deadline = time.monotonic() + 150
    session = f"media-{kind}-{uuid.uuid4().hex[:12]}"
    app = tmp_path / "app"
    env = _prepare(app, session, f"media{uuid.uuid4().hex[:16]}")
    originals: dict[str, dict[str, Any]] = {}
    previous: list[dict[str, Any]] = []
    raw: dict[str, Any] = {}
    removed = 0

    with _host(app, env, deadline, "warm", harness) as warm:
        boot = warm.get("retention/capture")["boot"]
        for index in range(TURNS):
            correlation = f"turn-{index}"
            inputs = _inputs(kind, correlation)
            raw = warm.turn(correlation, inputs, session)
            _capture(warm, boot, index + 1, f"{correlation}-input", session, [*_model_history(previous), *inputs])
            current = [*inputs, _answer(f"{correlation}-input")]
            originals.update({message["message_id"]: message for message in current})
            previous, removed = _retained(raw, originals)
            assert {message["message_id"] for message in current} <= {message["message_id"] for message in previous}
            _metrics(warm, boot, removed, index + 1)
            (tmp_path / f"warm-{index}-state.json").write_text(json.dumps(raw), encoding="utf-8")
        assert removed >= 4, "Must actually evict messages, not merely round-trip media"
        assert sum(message["role"] == "user" for message in previous) >= 2, "Keep older media for cold replay"
        assert len(raw["data"]["completedCorrelations"]) == len(raw["data"]["responseMailbox"]) == TURNS

    with _host(app, env, deadline, "cold", harness) as cold:
        initial = cold.get("retention/capture")
        cold_boot = initial["boot"]
        assert cold_boot != boot and initial["calls"] == 0 and initial["capture"] is None
        cold_read = cold.get("retention/state")
        (tmp_path / "cold-read-state.json").write_text(json.dumps(cold_read), encoding="utf-8")
        _equal(cold_read, raw, "exact backend JSON after killing and restarting the host")
        _metrics(cold, cold_boot, 0, 0)
        evicted = set(originals) - {message["message_id"] for message in previous}
        assert "turn-0-input" in evicted
        next_input = Message("user", ["next turn"], message_id="cold-input").to_dict()
        final = cold.turn("cold", [*_inputs(kind, "turn-0"), next_input], session)
        _capture(cold, cold_boot, 1, "cold-input", session, [*_model_history(previous), next_input])
        originals.update({"cold-input": next_input, "cold-input-answer": _answer("cold-input")})
        retained, total_removed = _retained(final, originals)
        ids = {message["message_id"] for message in retained}
        assert evicted.isdisjoint(ids), "Cold projected replay resurrected evicted media"
        assert {"cold-input", "cold-input-answer"} <= ids
        _metrics(cold, cold_boot, total_removed - removed, 1)
        _equal(
            {key: final["data"]["ingestedMessages"][key] for key in raw["data"]["ingestedMessages"]},
            raw["data"]["ingestedMessages"],
            "cold replay preserves ingestion receipts",
        )
        assert set(final["data"]["ingestedMessages"]) == {*raw["data"]["ingestedMessages"], "cold-input"}
        for field in ("completedCorrelations", "responseMailbox"):
            _equal({key: final["data"][field][key] for key in raw["data"][field]}, raw["data"][field], field)
            assert set(final["data"][field]) == {*raw["data"][field], "cold"}
        for mailbox in final["data"]["responseMailbox"].values():
            assert (
                datetime.fromisoformat(mailbox["expiresAt"]) - datetime.fromisoformat(mailbox["createdAt"])
            ).total_seconds() == DELIVERY_WINDOW_SECONDS
        (tmp_path / "cold-final-state.json").write_text(json.dumps(final), encoding="utf-8")
