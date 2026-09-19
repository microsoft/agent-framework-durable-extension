# Copyright (c) Microsoft. All rights reserved.

"""Regression tests for the integration worker's subprocess environment."""

import os
import subprocess
import sys
import uuid
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest


@pytest.fixture
def _dt_harness(monkeypatch: pytest.MonkeyPatch, request: pytest.FixtureRequest) -> ModuleType:
    path = Path(__file__).parent / "integration_tests" / "conftest.py"
    spec = spec_from_file_location(f"_dt_integration_environment_{uuid.uuid4().hex}", path)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    # Import as an ordinary module, without loading local secrets or registering pytest hooks.
    with monkeypatch.context() as import_patch:
        import_patch.setattr("dotenv.load_dotenv", Mock(return_value=False))
        import_patch.setattr("logging.basicConfig", Mock())
        spec.loader.exec_module(module)
    assert not request.config.pluginmanager.is_registered(module)
    return module


@pytest.mark.parametrize("parent_mode", [None, "legacy"], ids=["missing-mode", "invalid-mode"])
@pytest.mark.parametrize("platform", ["win32", "linux"], ids=["windows", "unix"])
def test_worker_subprocess_opts_into_isolated_mode(
    _dt_harness: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    parent_mode: str | None,
    platform: str,
) -> None:
    harness = _dt_harness
    # Set the case after importing the harness so neither dotenv nor the root fixture can mask it.
    if parent_mode is None:
        monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    else:
        monkeypatch.setenv("DURABLE_AGENTS_DEPLOYMENT_MODE", parent_mode)
    monkeypatch.setenv("TASKHUB", "parent-hub")
    monkeypatch.setenv("ENDPOINT", "http://parent.invalid:8080")
    parent_env = dict(os.environ)

    process = Mock(spec=subprocess.Popen)
    process.poll.return_value = None
    process.wait.return_value = 0

    def start_worker(*_args: object, **kwargs: Any) -> Mock:
        assert dict(os.environ) == parent_env
        assert kwargs["env"].get("DURABLE_AGENTS_DEPLOYMENT_MODE") == "isolated_v2"
        return process

    popen = Mock(side_effect=start_worker)
    monkeypatch.setattr(harness, "sys", SimpleNamespace(platform=platform, executable=sys.executable))
    monkeypatch.setattr(
        harness,
        "subprocess",
        SimpleNamespace(Popen=popen, CREATE_NEW_PROCESS_GROUP=512, TimeoutExpired=subprocess.TimeoutExpired),
    )
    monkeypatch.setattr(harness, "time", SimpleNamespace(sleep=Mock()))
    for probe in ("_check_dts_available", "_check_redis_available"):
        monkeypatch.setattr(harness, probe, Mock(side_effect=AssertionError("Unexpected infrastructure probe")))

    sample_name = "12_subworkflow_hitl"
    assert harness.__file__ is not None
    sample_path = Path(harness.__file__).parents[4] / "samples" / sample_name
    request_stub = Mock(spec=pytest.FixtureRequest)
    request_stub.node.get_closest_marker.return_value = SimpleNamespace(args=(sample_name,))
    taskhub = harness.unique_taskhub.__wrapped__()
    assert taskhub.startswith("test-") and taskhub != parent_env["TASKHUB"]
    endpoint = "http://localhost:8080"
    lifecycle = harness.worker_process.__wrapped__(
        dts_available=True,
        check_sample_env=None,
        dts_endpoint=endpoint,
        unique_taskhub=taskhub,
        request=request_stub,
    )
    try:
        worker_info = next(lifecycle)
        expected_options: dict[str, Any] = {
            "cwd": str(sample_path),
            "env": {
                **parent_env,
                "ENDPOINT": endpoint,
                "TASKHUB": taskhub,
                "DURABLE_AGENTS_DEPLOYMENT_MODE": "isolated_v2",
            },
            "text": True,
        }
        if platform == "win32":
            expected_options.update(creationflags=512, shell=True)
        popen.assert_called_once_with([sys.executable, str(sample_path / "worker.py")], **expected_options)
        assert popen.call_args.kwargs["env"] is not os.environ
        assert worker_info == {"process": process, "endpoint": endpoint, "taskhub": taskhub}
        request_stub.node.get_closest_marker.assert_called_once_with("sample")
        assert dict(os.environ) == parent_env
    finally:
        lifecycle.close()

    process.terminate.assert_called_once_with()
    process.wait.assert_called_once_with(timeout=5)
    assert dict(os.environ) == parent_env
