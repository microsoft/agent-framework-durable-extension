# Copyright (c) Microsoft. All rights reserved.

"""Exclusive live Functions worker replacements, no model credentials."""

from __future__ import annotations

import json
import shutil
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
import requests
from agent_framework import Message
from agent_framework_durabletask import RunRequest, state_snapshot_digest
from agent_framework_durabletask._shared_response import serialize_terminal_response
from test_16_live_media_retention import _Host, _host, _prepare

pytestmark = [
    pytest.mark.integration,
    pytest.mark.orchestration,
    pytest.mark.timeout(240),
    pytest.mark.sample("13_subworkflow_hitl"),
]


def _turn(host: _Host, correlation: str) -> tuple[dict[str, Any], dict[str, Any]]:
    response = requests.post(
        f"{host.url}/compatibility/run",
        json=RunRequest(
            "",
            correlation_id=correlation,
            context_messages=[Message("user", [correlation], message_id=correlation).to_dict()],
        ).to_dict(),
        timeout=5,
    )
    assert response.status_code == 202
    instance = response.json()["instance"]
    result = host.wait(
        lambda: host.get(f"compatibility/result/{instance}", missing_ok=True),
        "acknowledged entity call",
    )
    assert result["messages"][0]["contents"][0]["text"] == f"answer:{correlation}"
    return result, host.get("retention/state")


def test_live_functions_legacy_continuation_in_place_conversion_and_cold_duplicate(
    tmp_path: Path,
    request: pytest.FixtureRequest,
) -> None:
    harness_path = Path(__file__).with_name("conftest.py").resolve()
    harness = next(
        plugin
        for plugin in request.config.pluginmanager.get_plugins()
        if getattr(plugin, "__file__", None) and Path(plugin.__file__).resolve() == harness_path
    )
    app = tmp_path / "app"
    env = _prepare(app, "compatibility-session", f"compat{uuid.uuid4().hex[:16]}")
    shutil.copyfile(
        Path(__file__).with_name("live_compatibility_app") / "compatibility_function_app.py", app / "function_app.py"
    )
    settings_path = app / "local.settings.json"
    settings = json.loads(settings_path.read_text(encoding="utf-8"))
    settings["Values"].pop("DURABLE_AGENTS_DEPLOYMENT_MODE")
    settings_path.write_text(json.dumps(settings), encoding="utf-8")
    env.pop("DURABLE_AGENTS_DEPLOYMENT_MODE", None)
    config_path = app / "live_config.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["mode"] = "legacy"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    deadline = time.monotonic() + 220
    results: list[dict[str, Any]] = []

    def record(correlation: str, response: dict[str, Any], source: dict[str, Any]) -> None:
        stored = next(
            entry
            for entry in source["data"]["conversationHistory"]
            if entry["$type"] == "response" and entry["correlationId"] == correlation
        )
        results.append({
            "correlationId": correlation,
            "outcome": "succeeded",
            "completedAt": stored["createdAt"],
            "response": serialize_terminal_response(response),
        })

    with _host(app, env, deadline, "legacy", harness) as legacy:
        response, source = _turn(legacy, "first")
        assert source["schemaVersion"] == "1.1.0"
        assert legacy.get("compatibility/calls") == 1
        record("first", response, source)

    config.pop("mode")
    config_path.write_text(json.dumps(config), encoding="utf-8")
    with _host(app, env, deadline, "bridge", harness) as bridge:
        response, source = _turn(bridge, "second")
        assert source["schemaVersion"] == "1.1.0"
        record("second", response, source)

    config["completion_journal"] = {
        "sourceDigest": state_snapshot_digest(source),
        "evidenceId": "acknowledged-functions-calls",
        "complete": True,
        "results": results,
    }
    config_path.write_text(json.dumps(config), encoding="utf-8")
    with _host(app, env, deadline, "convert", harness) as convert:
        _, committed = _turn(convert, "third")
        assert committed["schemaVersion"] == "2.0.0"
        assert committed["data"]["migration"]["mode"] == "in_place"
        assert set(committed["data"]["completionReceipts"]) == {"first", "second", "third"}

    config.pop("completion_journal")
    config_path.write_text(json.dumps(config), encoding="utf-8")
    with _host(app, env, deadline, "cold", harness) as cold:
        _, duplicate = _turn(cold, "third")
        assert duplicate == committed
        assert cold.get("compatibility/calls") == 0
        _, continued = _turn(cold, "fourth")
        assert continued["schemaVersion"] == "2.0.0"
        assert cold.get("compatibility/calls") == 1
