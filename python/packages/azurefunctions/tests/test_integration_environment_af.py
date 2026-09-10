# Copyright (c) Microsoft. All rights reserved.

"""Regression tests for the integration function app's subprocess environment."""

import os
import subprocess
import sys
import uuid
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import Mock, call

import pytest


@pytest.fixture
def _af_harness(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> ModuleType:
    path = Path(__file__).parent / "integration_tests" / "conftest.py"
    spec = spec_from_file_location(f"_af_integration_environment_{uuid.uuid4().hex}", path)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    # Keep the integration hooks out of unit-test discovery and avoid local dotenv inputs.
    assert not request.config.pluginmanager.is_registered(module)
    monkeypatch.setattr(module, "_load_env_file_if_present", Mock())
    return module


@pytest.mark.parametrize("parent_mode", [None, "legacy"], ids=["missing-mode", "invalid-mode"])
@pytest.mark.parametrize("platform", ["win32", "linux"], ids=["windows", "unix"])
@pytest.mark.parametrize("startup_failures", [0, 2], ids=["first-start", "third-start"])
def test_function_app_subprocess_opts_into_isolated_mode_on_every_start(
    _af_harness: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    parent_mode: str | None,
    platform: str,
    startup_failures: int,
) -> None:
    harness = _af_harness
    # Set the case after importing the harness so the root fixture cannot mask it.
    if parent_mode is None:
        monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    else:
        monkeypatch.setenv("DURABLE_AGENTS_DEPLOYMENT_MODE", parent_mode)
    monkeypatch.setenv("TASKHUB_NAME", "parent-hub")
    monkeypatch.setenv("AzureWebJobsStorage", "UseDevelopmentStorage=true")
    monkeypatch.setenv(
        "DURABLE_TASK_SCHEDULER_CONNECTION_STRING",
        "Endpoint=http://localhost:8080;TaskHub=parent-hub;Authentication=None",
    )
    monkeypatch.setenv("FUNCTIONS_WORKER_RUNTIME", "python")
    parent_env = dict(os.environ)

    processes = [Mock(spec=subprocess.Popen) for _ in range(startup_failures + 1)]
    pending_processes = iter(processes)

    def start_app(*_args: object, **kwargs: Any) -> Mock:
        assert dict(os.environ) == parent_env
        assert kwargs["env"].get("DURABLE_AGENTS_DEPLOYMENT_MODE") == "isolated_v2"
        return next(pending_processes)

    popen = Mock(side_effect=start_app)
    monkeypatch.setattr(harness, "sys", SimpleNamespace(platform=platform))
    monkeypatch.setattr(harness, "subprocess", SimpleNamespace(Popen=popen, CREATE_NEW_PROCESS_GROUP=512))
    monkeypatch.setattr(
        harness,
        "time",
        SimpleNamespace(monotonic=Mock(return_value=0), sleep=Mock(side_effect=AssertionError("Unexpected sleep"))),
    )
    ports = list(range(17071, 17071 + len(processes)))
    find_port = Mock(side_effect=ports)
    monkeypatch.setattr(harness, "_find_available_port", find_port)
    readiness = Mock(
        side_effect=[harness.FunctionAppStartupError("Retry this test startup") for _ in range(startup_failures)]
        + [None]
    )
    monkeypatch.setattr(harness, "_wait_for_function_app_ready", readiness)
    cleanup = Mock()
    monkeypatch.setattr(harness, "_cleanup_function_app", cleanup)
    for probe in ("_check_func_cli_available", "_check_azurite_available", "_check_dts_emulator_available"):
        monkeypatch.setattr(harness, probe, Mock(side_effect=AssertionError("Unexpected infrastructure probe")))

    assert harness.__file__ is not None
    python_root = Path(harness.__file__).resolve().parents[4]
    monkeypatch.setattr(harness, "_resolve_repo_root", Mock(return_value=python_root))
    sample_name = "13_subworkflow_hitl"
    sample_path = python_root / "samples" / "azure_functions" / sample_name
    request_stub = Mock(spec=pytest.FixtureRequest)
    request_stub.node.get_closest_marker.return_value = SimpleNamespace(args=(sample_name,))
    lifecycle = harness.function_app_for_test.__wrapped__(request=request_stub)
    try:
        app_info = next(lifecycle)
        assert popen.call_count == len(processes)
        hubs: set[str] = set()
        for invocation, port in zip(popen.call_args_list, ports, strict=True):
            child_env = invocation.kwargs["env"]
            assert child_env is not os.environ
            hub = child_env["TASKHUB_NAME"]
            assert hub.startswith("test") and hub != parent_env["TASKHUB_NAME"]
            hubs.add(hub)
            expected_options: dict[str, Any] = {
                "cwd": str(sample_path),
                "env": {**parent_env, "TASKHUB_NAME": hub, "DURABLE_AGENTS_DEPLOYMENT_MODE": "isolated_v2"},
            }
            if platform == "win32":
                expected_options.update(creationflags=512, shell=True)
            else:
                expected_options["start_new_session"] = True
            assert invocation == call(["func", "start", "--port", str(port)], **expected_options)
        assert len(hubs) == len(processes)
        assert app_info == {"base_url": f"http://localhost:{ports[-1]}", "port": ports[-1]}
        assert find_port.call_count == len(processes)
        assert readiness.call_args_list == [
            call(process, port, max_wait=60) for process, port in zip(processes, ports, strict=True)
        ]
        request_stub.node.get_closest_marker.assert_called_once_with("sample")
        assert dict(os.environ) == parent_env
    finally:
        lifecycle.close()

    assert cleanup.call_args_list == [call(process) for process in processes]
    assert dict(os.environ) == parent_env
