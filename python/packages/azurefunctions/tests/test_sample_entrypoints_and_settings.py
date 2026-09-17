# Copyright (c) Microsoft. All rights reserved.

"""Check documented sample settings and real entrypoint branching without services."""

import json
import re
import runpy
import sys
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import Workflow
from agent_framework_durabletask._configuration import validate_runtime_deployment

SAMPLES = Path(__file__).resolve().parents[3] / "samples" / "azure_functions"
TEMPLATES = sorted(SAMPLES.glob("*/local.settings.json.template"))
SAMPLE_SETTINGS = sorted(SAMPLES.glob("*/local.settings.json.sample"))
SETTINGS = sorted([*TEMPLATES, *SAMPLE_SETTINGS])
MAF_APPS = sorted(path for path in SAMPLES.glob("*/function_app.py") if '"--maf"' in path.read_text(encoding="utf-8"))


def _documented_settings():
    readme = (SAMPLES / "README.md").read_text(encoding="utf-8")
    blocks = re.findall(r"```json\n(.*?)```", readme, flags=re.DOTALL)
    return next(json.loads(block) for block in blocks if "DURABLE_AGENTS_DEPLOYMENT_MODE" in block)


def test_sample_settings_categories_are_complete():
    # Workflow .sample files count as settings, not as additional templates.
    assert len(TEMPLATES) == 9
    app_dirs = {path.parent for path in SAMPLES.glob("*/function_app.py")}
    assert len(SETTINGS) == len(app_dirs)
    assert {path.parent for path in SETTINGS} == app_dirs
    assert {path.parent for path in TEMPLATES}.isdisjoint(path.parent for path in SAMPLE_SETTINGS)
    assert {path.parent for path in MAF_APPS} == {path.parent for path in SAMPLE_SETTINGS}


@pytest.mark.parametrize("path", TEMPLATES, ids=lambda path: path.parent.name)
def test_functions_templates_require_operator_acknowledgement(path, monkeypatch):
    settings = json.loads(path.read_text(encoding="utf-8"))
    assert settings["Values"]["DURABLE_AGENTS_DEPLOYMENT_MODE"] == ""
    assert "isolated_v2" in settings["_comment"]
    assert "new isolated task hub" in settings["_comment"]
    assert "compatible clients and workers" in settings["_comment"]
    monkeypatch.setenv("DURABLE_AGENTS_DEPLOYMENT_MODE", settings["Values"]["DURABLE_AGENTS_DEPLOYMENT_MODE"])
    with pytest.raises(ValueError, match="operator acknowledgement"):
        validate_runtime_deployment(None)


@pytest.mark.parametrize("path", SETTINGS, ids=lambda path: path.parent.name)
def test_functions_documented_settings_enable_registration(path, monkeypatch):
    monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    values = json.loads(path.read_text(encoding="utf-8"))["Values"]
    values.update(_documented_settings())
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    validate_runtime_deployment(None)
    hub = values["TASKHUB_NAME"]
    assert hub and hub != "default"
    assert values["AzureFunctionsJobHost__extensions__durableTask__hubName"] == hub
    assert f";TaskHub={hub};" in values["DURABLE_TASK_SCHEDULER_CONNECTION_STRING"]
    readme = (SAMPLES / "README.md").read_text(encoding="utf-8")
    assert "not runtime proof of isolation" in readme
    assert "old engine" in readme


class _NoNetworkChatClient:
    additional_properties: dict[str, Any] = {}

    def get_response(self, *args, **kwargs):
        raise AssertionError("Building a workflow must not invoke a model")


@pytest.fixture
def entrypoint_services(monkeypatch):
    hosts: list[Any] = []

    # A class, not a Mock instance, is required for AgentFunctionApp | None annotations.
    class Host:
        def __init__(self, *, workflow, **kwargs):
            validate_runtime_deployment(None)
            assert isinstance(workflow, Workflow)
            hosts.append(self)

    devui = ModuleType("agent_framework.devui")
    serve = Mock()
    monkeypatch.setattr(devui, "serve", serve, raising=False)
    monkeypatch.setitem(sys.modules, "agent_framework.devui", devui)
    monkeypatch.setattr("agent_framework_azurefunctions.AgentFunctionApp", Host)
    monkeypatch.setattr("agent_framework.foundry.FoundryChatClient", lambda **kwargs: _NoNetworkChatClient())
    monkeypatch.setattr("azure.identity.aio.AzureCliCredential", Mock())
    monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    monkeypatch.delenv("FOUNDRY_PROJECT_ENDPOINT", raising=False)
    monkeypatch.delenv("FOUNDRY_MODEL", raising=False)

    def load_env(**kwargs):
        # MAF's own dotenv load must happen before any model configuration is read.
        monkeypatch.setenv("FOUNDRY_PROJECT_ENDPOINT", "https://example.invalid")
        monkeypatch.setenv("FOUNDRY_MODEL", "offline")
        return True

    dotenv = Mock(side_effect=load_env)
    monkeypatch.setattr("dotenv.load_dotenv", dotenv)
    return hosts, serve, dotenv


@pytest.mark.parametrize("path", MAF_APPS, ids=lambda path: path.parent.name)
def test_functions_maf_entrypoint_never_constructs_durable_host(path, entrypoint_services, monkeypatch):
    hosts, serve, dotenv = entrypoint_services
    monkeypatch.setattr(sys, "argv", [str(path), "--maf"])

    result = runpy.run_path(str(path), run_name="__main__")

    assert hosts == []
    assert "app" not in result
    dotenv.assert_called_once_with(dotenv_path=path.parent / ".env")
    serve.assert_called_once()
    entities = serve.call_args.kwargs["entities"]
    assert len(entities) == 1 and isinstance(entities[0], Workflow)


@pytest.mark.parametrize("path", MAF_APPS, ids=lambda path: path.parent.name)
def test_functions_import_constructs_one_host_even_with_maf_argv(path, entrypoint_services, monkeypatch):
    hosts, serve, dotenv = entrypoint_services
    monkeypatch.setattr(sys, "argv", [str(path), "--maf"])
    monkeypatch.setenv("FOUNDRY_PROJECT_ENDPOINT", "https://example.invalid")
    monkeypatch.setenv("FOUNDRY_MODEL", "offline")
    for key, value in _documented_settings().items():
        monkeypatch.setenv(key, value)

    result = runpy.run_path(str(path), run_name=f"sample_{path.parent.name}")

    assert hosts == [result["app"]]
    serve.assert_not_called()
    dotenv.assert_not_called()


@pytest.mark.parametrize("path", MAF_APPS, ids=lambda path: path.parent.name)
def test_functions_import_does_not_silently_acknowledge_deployment(path, entrypoint_services, monkeypatch):
    hosts, serve, _ = entrypoint_services
    monkeypatch.setenv("FOUNDRY_PROJECT_ENDPOINT", "https://example.invalid")
    monkeypatch.setenv("FOUNDRY_MODEL", "offline")

    with pytest.raises(ValueError, match="operator acknowledgement"):
        runpy.run_path(str(path), run_name=f"unacknowledged_{path.parent.name}")

    assert hosts == []
    serve.assert_not_called()
