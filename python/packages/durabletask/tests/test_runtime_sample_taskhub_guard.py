# Copyright (c) Microsoft. All rights reserved.

"""Offline checks for standalone sample task hub guards."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import patch

import pytest

SAMPLES_ROOT = Path(__file__).resolve().parents[3] / "samples"

WORKER_MODULES = [
    ("01_single_agent", "worker.py"),
    ("02_multi_agent", "worker.py"),
    ("03_single_agent_streaming", "worker.py"),
    ("04_single_agent_orchestration_chaining", "worker.py"),
    ("05_multi_agent_orchestration_concurrency", "worker.py"),
    ("06_multi_agent_orchestration_conditionals", "worker.py"),
    ("07_single_agent_orchestration_hitl", "worker.py"),
    ("08_workflow", "worker.py"),
    ("09_workflow_hitl", "worker.py"),
    ("10_workflow_streaming", "worker.py"),
    ("11_subworkflow", "worker.py"),
    ("12_subworkflow_hitl", "worker.py"),
    ("13_conversation_compaction", "worker.py"),
    ("14_external_history_redis", "worker.py"),
]

CLIENT_MODULES = [
    ("01_single_agent", "client.py", "agent"),
    ("02_multi_agent", "client.py", "agent"),
    ("03_single_agent_streaming", "client.py", "agent"),
    ("04_single_agent_orchestration_chaining", "client.py", "workflow"),
    ("05_multi_agent_orchestration_concurrency", "client.py", "workflow"),
    ("06_multi_agent_orchestration_conditionals", "client.py", "workflow"),
    ("07_single_agent_orchestration_hitl", "client.py", "workflow"),
    ("08_workflow", "client.py", "workflow"),
    ("09_workflow_hitl", "client.py", "workflow"),
    ("10_workflow_streaming", "client.py", "workflow"),
    ("11_subworkflow", "client.py", "workflow"),
    ("12_subworkflow_hitl", "client.py", "workflow"),
    ("13_conversation_compaction", "client.py", "agent"),
    ("14_external_history_redis", "client.py", "agent"),
]

INVALID_TASKHUBS = [
    "default",
    "DEFAULT",
    "DeFaUlT",
    " default ",
    "\tDEFAULT\n",
    "",
    " ",
    "\t\n",
    " MyTaskHub",
    "MyTaskHub ",
    "\tMyTaskHub\n",
]


def test_guard_cases_cover_all_standalone_workers_and_clients() -> None:
    assert len(WORKER_MODULES) == len(CLIENT_MODULES) == 14
    assert {SAMPLES_ROOT / sample / module for sample, module in WORKER_MODULES} == set(
        SAMPLES_ROOT.glob("[0-9]*/worker.py")
    )
    assert {SAMPLES_ROOT / sample / module for sample, module, _ in CLIENT_MODULES} == set(
        SAMPLES_ROOT.glob("[0-9]*/client.py")
    )


def _load_sample_module(sample_name: str, module_file: str) -> ModuleType:
    sample_dir = SAMPLES_ROOT / sample_name
    module_path = sample_dir / module_file
    module_name = f"sample_taskhub_guard_{sample_name}_{module_file.replace('.', '_')}"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(sample_dir))
    try:
        with patch("dotenv.load_dotenv", return_value=False):
            spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


@pytest.mark.parametrize(("sample_name", "module_file"), WORKER_MODULES)
@pytest.mark.parametrize("taskhub", [None, ""], ids=["missing", "blank"])
def test_worker_requires_non_default_taskhub_before_constructing_scheduler(
    sample_name: str, module_file: str, taskhub: str | None
) -> None:
    module = _load_sample_module(sample_name, module_file)

    with (
        patch.dict(os.environ, {"TASKHUB": "", "ENDPOINT": "https://scheduler.example.invalid"}, clear=False),
        patch.object(module, "AzureCliCredential") as credential,
        patch.object(module, "DurableTaskSchedulerWorker") as scheduler_worker,
    ):
        if taskhub is None:
            os.environ.pop("TASKHUB", None)
        with pytest.raises(ValueError, match="non-default, non-blank hub name"):
            module.get_worker()

    credential.assert_not_called()
    scheduler_worker.assert_not_called()


@pytest.mark.parametrize(("sample_name", "module_file"), WORKER_MODULES)
@pytest.mark.parametrize("taskhub", INVALID_TASKHUBS)
@pytest.mark.parametrize("from_env", [False, True])
def test_worker_rejects_invalid_taskhub(sample_name: str, module_file: str, taskhub: str, from_env: bool) -> None:
    module = _load_sample_module(sample_name, module_file)

    with (
        patch.dict(os.environ, {"TASKHUB": taskhub if from_env else "EnvHub"}, clear=False),
        patch.object(module, "AzureCliCredential") as credential,
        patch.object(module, "DurableTaskSchedulerWorker") as scheduler_worker,
        pytest.raises(ValueError, match="non-default, non-blank hub name"),
    ):
        module.get_worker(taskhub=None if from_env else taskhub, endpoint="https://scheduler.example.invalid")

    credential.assert_not_called()
    scheduler_worker.assert_not_called()


@pytest.mark.parametrize(("sample_name", "module_file"), WORKER_MODULES)
def test_worker_accepts_env_taskhub(sample_name: str, module_file: str) -> None:
    module = _load_sample_module(sample_name, module_file)

    with (
        patch.dict(os.environ, {"TASKHUB": "EnvHub", "ENDPOINT": "http://localhost:8080"}, clear=False),
        patch.object(module, "DurableTaskSchedulerWorker") as scheduler_worker,
    ):
        assert module.get_worker() is scheduler_worker.return_value

    assert scheduler_worker.call_args.kwargs["taskhub"] == "EnvHub"


@pytest.mark.parametrize(("sample_name", "module_file"), WORKER_MODULES)
def test_worker_preserves_explicit_taskhub_case(sample_name: str, module_file: str) -> None:
    module = _load_sample_module(sample_name, module_file)

    with (
        patch.dict(os.environ, {"TASKHUB": "default"}, clear=False),
        patch.object(module, "DurableTaskSchedulerWorker") as scheduler_worker,
    ):
        module.get_worker(taskhub="MyTaskHub", endpoint="http://localhost:8080")

    assert scheduler_worker.call_args.kwargs["taskhub"] == "MyTaskHub"


@pytest.mark.parametrize(("sample_name", "module_file", "_client_kind"), CLIENT_MODULES)
def test_client_requires_non_default_taskhub_before_constructing_scheduler(
    sample_name: str, module_file: str, _client_kind: str
) -> None:
    module = _load_sample_module(sample_name, module_file)

    with (
        patch.dict(os.environ, {"ENDPOINT": "https://scheduler.example.invalid"}, clear=False),
        patch.object(module, "AzureCliCredential") as credential,
        patch.object(module, "DurableTaskSchedulerClient") as scheduler_client,
    ):
        os.environ.pop("TASKHUB", None)
        with pytest.raises(ValueError, match="non-default, non-blank hub name"):
            module.get_client()

    credential.assert_not_called()
    scheduler_client.assert_not_called()


@pytest.mark.parametrize(("sample_name", "module_file", "_client_kind"), CLIENT_MODULES)
@pytest.mark.parametrize("taskhub", INVALID_TASKHUBS)
@pytest.mark.parametrize("from_env", [False, True])
def test_client_rejects_invalid_taskhub(
    sample_name: str, module_file: str, _client_kind: str, taskhub: str, from_env: bool
) -> None:
    module = _load_sample_module(sample_name, module_file)

    with (
        patch.dict(os.environ, {"TASKHUB": taskhub if from_env else "EnvHub"}, clear=False),
        patch.object(module, "AzureCliCredential") as credential,
        patch.object(module, "DurableTaskSchedulerClient") as scheduler_client,
        pytest.raises(ValueError, match="non-default, non-blank hub name"),
    ):
        module.get_client(taskhub=None if from_env else taskhub, endpoint="https://scheduler.example.invalid")

    credential.assert_not_called()
    scheduler_client.assert_not_called()


@pytest.mark.parametrize(("sample_name", "module_file", "client_kind"), CLIENT_MODULES)
def test_client_accepts_env_taskhub(sample_name: str, module_file: str, client_kind: str) -> None:
    module = _load_sample_module(sample_name, module_file)

    with patch.object(module, "DurableTaskSchedulerClient", return_value="scheduler") as scheduler_client:
        if client_kind == "agent":
            with patch.object(module, "DurableAIAgentClient", return_value="agent-client") as agent_client:
                with patch.dict(os.environ, {"TASKHUB": "EnvHub", "ENDPOINT": "http://localhost:8080"}, clear=False):
                    assert module.get_client() == "agent-client"
                agent_client.assert_called_once_with("scheduler")
        else:
            with patch.dict(os.environ, {"TASKHUB": "EnvHub", "ENDPOINT": "http://localhost:8080"}, clear=False):
                assert module.get_client() == "scheduler"

    assert scheduler_client.call_args.kwargs["taskhub"] == "EnvHub"


@pytest.mark.parametrize(("sample_name", "module_file", "client_kind"), CLIENT_MODULES)
def test_client_preserves_explicit_taskhub_case(sample_name: str, module_file: str, client_kind: str) -> None:
    module = _load_sample_module(sample_name, module_file)

    with (
        patch.dict(os.environ, {"TASKHUB": "default"}, clear=False),
        patch.object(module, "DurableTaskSchedulerClient", return_value="scheduler") as scheduler_client,
    ):
        if client_kind == "agent":
            with patch.object(module, "DurableAIAgentClient", return_value="agent-client") as agent_client:
                assert module.get_client(taskhub="MyTaskHub", endpoint="http://localhost:8080") == "agent-client"
                agent_client.assert_called_once_with("scheduler")
        else:
            assert module.get_client(taskhub="MyTaskHub", endpoint="http://localhost:8080") == "scheduler"

    assert scheduler_client.call_args.kwargs["taskhub"] == "MyTaskHub"


@pytest.mark.parametrize(
    "template",
    sorted((SAMPLES_ROOT / "azure_functions").glob("*/local.settings.json.template"))
    + sorted((SAMPLES_ROOT / "azure_functions").glob("*/local.settings.json.sample")),
    ids=lambda path: path.parent.name,
)
def test_function_settings_templates_require_fresh_hub_acknowledgement(template: Path) -> None:
    settings = json.loads(template.read_text(encoding="utf-8"))
    values = settings["Values"]

    assert values["TASKHUB_NAME"] == "durablesamplev2UNIQUE"
    assert "TaskHub=durablesamplev2UNIQUE;" in values["DURABLE_TASK_SCHEDULER_CONNECTION_STRING"]
    assert values["DURABLE_AGENTS_DEPLOYMENT_MODE"] == ""
    assert "fresh, unique hub" in settings["_comment"]
    assert "isolated_v2" in settings["_comment"]


def test_function_env_template_requires_fresh_hub_acknowledgement() -> None:
    template = SAMPLES_ROOT / "azure_functions" / "11_workflow_parallel" / ".env.template"
    text = template.read_text(encoding="utf-8")
    values = dict(line.split("=", 1) for line in text.splitlines() if line and not line.startswith("#"))

    assert values["TASKHUB_NAME"] == "durablesamplev2UNIQUE"
    assert "TaskHub=durablesamplev2UNIQUE;" in values["DURABLE_TASK_SCHEDULER_CONNECTION_STRING"]
    assert values["DURABLE_AGENTS_DEPLOYMENT_MODE"] == ""
    assert "Leave blank until the fresh hub is configured" in text
    assert "isolated_v2" in text
