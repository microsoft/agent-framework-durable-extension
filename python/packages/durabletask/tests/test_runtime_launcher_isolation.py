# Copyright (c) Microsoft. All rights reserved.

"""Real integration launchers acknowledge only their isolated child task hubs."""

import importlib.util
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework_azurefunctions import AgentFunctionApp

from agent_framework_durabletask import DurableAIAgentWorker


def _load(monkeypatch: pytest.MonkeyPatch, package: str) -> Any:
    import dotenv

    monkeypatch.setattr(dotenv, "load_dotenv", lambda *args, **kwargs: None)
    path = Path(__file__).resolve().parents[2] / package / "tests" / "integration_tests" / "conftest.py"
    name = f"_isolated_launcher_{package}"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("package", ["durabletask", "azurefunctions"])
def test_launchers_set_only_child_isolation_and_unique_hubs(monkeypatch: pytest.MonkeyPatch, package: str) -> None:
    monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    host_hub_key = "AzureFunctionsJobHost__extensions__durableTask__hubName"
    monkeypatch.setenv(host_hub_key, "parent-hub")
    monkeypatch.setenv("TASKHUB_NAME", "parent-hub")
    launcher = _load(monkeypatch, package)
    process = Mock()
    process.poll.return_value = None
    popen = Mock(return_value=process)
    monkeypatch.setattr(launcher.subprocess, "Popen", popen)
    monkeypatch.setattr(launcher.time, "sleep", lambda _: None)
    before = dict(os.environ)
    captured: list[dict[str, str]] = []
    for _ in range(2):
        if package == "durabletask":
            hub = launcher.unique_taskhub.__wrapped__()
            request = SimpleNamespace(
                node=SimpleNamespace(get_closest_marker=lambda _: SimpleNamespace(args=("01_single_agent",)))
            )
            fixture = launcher.worker_process.__wrapped__(True, None, "http://localhost:8080", hub, request)
            next(fixture)
            fixture.close()
        else:
            sample = Path(__file__).resolve().parents[3] / "samples" / "azure_functions" / "01_single_agent"
            launcher._start_function_app(sample, 7071)
        captured.append(popen.call_args.kwargs["env"])
    assert os.environ == before
    hub_key = "TASKHUB" if package == "durabletask" else "TASKHUB_NAME"
    assert captured[0][hub_key] != captured[1][hub_key]
    assert all(env["DURABLE_AGENTS_DEPLOYMENT_MODE"] == "isolated_v2" for env in captured)
    assert all(env is not os.environ for env in captured)
    if package == "azurefunctions":
        assert all(env[host_hub_key] == env[hub_key] for env in captured)
        assert all(env[host_hub_key] != "parent-hub" for env in captured)
    assert popen.call_count == 2


@pytest.mark.parametrize(
    "connection_string",
    [
        None,
        "Endpoint=http://localhost:8080;Authentication=None;TaskHub=default",
        "Endpoint=http://localhost:8080;Authentication=None",
        "Endpoint=http://localhost:8080; tAsKhUb =parent;Authentication=None;",
        "TaskHub=first;Endpoint=http://localhost:8080;TaskHub=second;Authentication=None",
        "Endpoint=http://localhost:8080;Authentication=None;Extension=a=b==;TaskHub=parent;",
    ],
)
def test_functions_launcher_aligns_scheduler_connection_without_mutating_parent(
    monkeypatch: pytest.MonkeyPatch, connection_string: str | None
) -> None:
    key = "DURABLE_TASK_SCHEDULER_CONNECTION_STRING"
    if connection_string is None:
        monkeypatch.delenv(key, raising=False)
    else:
        monkeypatch.setenv(key, connection_string)
    launcher = _load(monkeypatch, "azurefunctions")
    popen = Mock()
    monkeypatch.setattr(launcher.subprocess, "Popen", popen)
    before = dict(os.environ)

    launcher._start_function_app(Path("sample"), 7071)

    child = popen.call_args.kwargs["env"]
    assert os.environ == before
    assert child["DURABLE_AGENTS_DEPLOYMENT_MODE"] == "isolated_v2"
    assert child["TASKHUB_NAME"] == child["AzureFunctionsJobHost__extensions__durableTask__hubName"]
    if connection_string is None:
        assert key not in child
        return
    components = child[key].split(";")
    hubs = [part.partition("=")[2] for part in components if part.partition("=")[0].strip().casefold() == "taskhub"]
    assert hubs == [child["TASKHUB_NAME"]]
    expected = [
        part for part in connection_string.split(";") if part and part.partition("=")[0].strip().casefold() != "taskhub"
    ]
    actual = [part for part in components if part and part.partition("=")[0].strip().casefold() != "taskhub"]
    assert actual == expected


def test_mcp_sample_template_overrides_host_with_the_same_task_hub() -> None:
    sample = Path(__file__).resolve().parents[3] / "samples" / "azure_functions" / "08_mcp_server"
    values = json.loads((sample / "local.settings.json.template").read_text(encoding="utf-8"))["Values"]

    assert values["AzureFunctionsJobHost__extensions__durableTask__hubName"] == values["TASKHUB_NAME"]
    assert f"TaskHub={values['TASKHUB_NAME']};" in values["DURABLE_TASK_SCHEDULER_CONNECTION_STRING"]


def test_real_host_constructor_rejects_absent_acknowledgement(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    with pytest.raises(ValueError, match="isolated task hub"):
        DurableAIAgentWorker(Mock())
    with pytest.raises(ValueError, match="isolated task hub"):
        AgentFunctionApp(enable_health_check=False)
    assert DurableAIAgentWorker(Mock(), deployment_mode="isolated_v2") is not None
    assert AgentFunctionApp(enable_health_check=False, deployment_mode="isolated_v2") is not None
