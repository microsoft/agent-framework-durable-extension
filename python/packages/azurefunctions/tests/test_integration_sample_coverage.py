# Copyright (c) Microsoft. All rights reserved.

"""Coverage checks for the Azure Functions sample integration tests."""

import ast
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock

import pytest


@pytest.fixture
def integration_conftest(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    path = Path(__file__).parent / "integration_tests" / "conftest.py"
    spec = importlib.util.spec_from_file_location("azure_functions_integration_conftest", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "_load_env_file_if_present", lambda: None)
    return module


@pytest.mark.parametrize(
    ("func_available", "azurite_available", "expected_skip", "expected_reason"),
    [
        (True, True, False, "Integration tests enabled."),
        (False, True, True, "Azure Functions Core Tools"),
        (True, False, True, "Azurite not running"),
    ],
)
def test_integration_prerequisites_do_not_require_dts(
    integration_conftest: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    func_available: bool,
    azurite_available: bool,
    expected_skip: bool,
    expected_reason: str,
) -> None:
    checked_ports: list[int] = []

    def port_available(port: int, host: str = "localhost") -> bool:
        assert host == "localhost"
        checked_ports.append(port)
        return port == 10000 and azurite_available

    monkeypatch.setattr(integration_conftest, "_check_func_cli_available", lambda: func_available)
    monkeypatch.setattr(integration_conftest, "_check_port_listening", port_available)

    skipped, reason = integration_conftest._should_skip_azure_functions_integration_tests()

    assert skipped is expected_skip
    assert expected_reason in reason
    assert checked_ports == ([10000] if func_available else [])


@pytest.mark.parametrize(
    ("func_available", "azurite_available", "sample_name", "foundry_available", "expected_reason"),
    [
        (True, True, "13_subworkflow_hitl", False, None),
        (True, True, "01_single_agent", True, None),
        (True, True, "01_single_agent", False, "Missing required environment variables"),
        (False, True, "13_subworkflow_hitl", False, "Azure Functions Core Tools"),
        (True, False, "13_subworkflow_hitl", False, "Azurite not running"),
    ],
)
def test_integration_collection_preserves_required_service_skips(
    integration_conftest: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    func_available: bool,
    azurite_available: bool,
    sample_name: str,
    foundry_available: bool,
    expected_reason: str | None,
) -> None:
    monkeypatch.setattr(integration_conftest, "_check_func_cli_available", lambda: func_available)
    monkeypatch.setattr(
        integration_conftest,
        "_check_port_listening",
        lambda port, host="localhost": port == 10000 and azurite_available,
    )
    for name in ("FOUNDRY_PROJECT_ENDPOINT", "FOUNDRY_MODEL"):
        if foundry_available:
            monkeypatch.setenv(name, "configured")
        else:
            monkeypatch.delenv(name, raising=False)
    item = Mock()
    item.fspath = Path(__file__).parent / "integration_tests" / "test_sample.py"
    item.get_closest_marker.return_value = Mock(args=(sample_name,))

    integration_conftest.pytest_collection_modifyitems(Mock(), [item])

    if expected_reason is None:
        item.add_marker.assert_not_called()
    else:
        item.add_marker.assert_called_once()
        marker = item.add_marker.call_args.args[0]
        assert marker.name == "skip"
        assert expected_reason in marker.kwargs["reason"]


@pytest.mark.parametrize(
    ("sample_name", "foundry_available", "expected_reason"),
    [
        ("13_subworkflow_hitl", False, None),
        ("01_single_agent", True, None),
        ("01_single_agent", False, "FOUNDRY_PROJECT_ENDPOINT"),
    ],
)
def test_integration_environment_does_not_require_scheduler_configuration(
    integration_conftest: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    sample_name: str,
    foundry_available: bool,
    expected_reason: str | None,
) -> None:
    monkeypatch.setenv("AzureWebJobsStorage", "UseDevelopmentStorage=true")
    monkeypatch.setenv("FUNCTIONS_WORKER_RUNTIME", "python")
    monkeypatch.delenv("DURABLE_TASK_SCHEDULER_CONNECTION_STRING", raising=False)
    for name in ("FOUNDRY_PROJECT_ENDPOINT", "FOUNDRY_MODEL"):
        if foundry_available:
            monkeypatch.setenv(name, "configured")
        else:
            monkeypatch.delenv(name, raising=False)

    if expected_reason is None:
        integration_conftest._load_and_validate_env(Path(sample_name))
    else:
        with pytest.raises(pytest.fail.Exception, match=expected_reason):
            integration_conftest._load_and_validate_env(Path(sample_name))


def _sample_markers(test_file: Path) -> set[str]:
    """Return literal pytest.mark.sample values declared in a test module."""
    tree = ast.parse(test_file.read_text(encoding="utf-8"), filename=str(test_file))
    markers: set[str] = set()

    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "sample"
            and len(node.args) == 1
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            markers.add(node.args[0].value)

    return markers


def _is_pytest_skip_marker(node: ast.expr) -> bool:
    """Return whether an expression is pytest.mark.skip, with or without arguments."""
    target = node.func if isinstance(node, ast.Call) else node
    return (
        isinstance(target, ast.Attribute)
        and target.attr == "skip"
        and isinstance(target.value, ast.Attribute)
        and target.value.attr == "mark"
        and isinstance(target.value.value, ast.Name)
        and target.value.value.id == "pytest"
    )


def _unconditional_skip_lines(test_file: Path) -> list[int]:
    """Return lines containing unconditional pytest skip markers."""
    tree = ast.parse(test_file.read_text(encoding="utf-8"), filename=str(test_file))
    skip_lines: list[int] = []

    for node in ast.walk(tree):
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            skip_lines.extend(
                decorator.lineno for decorator in node.decorator_list if _is_pytest_skip_marker(decorator)
            )

    for statement in tree.body:
        if (
            isinstance(statement, (ast.Assign, ast.AnnAssign))
            and any(isinstance(target, ast.Name) and target.id == "pytestmark" for target in ast.walk(statement))
            and any(_is_pytest_skip_marker(node) for node in ast.walk(statement) if isinstance(node, ast.expr))
        ):
            skip_lines.append(statement.lineno)

    return sorted(set(skip_lines))


def test_every_sample_has_integration_coverage() -> None:
    """Require each Azure Functions sample to have an enabled integration test module."""
    python_root = Path(__file__).resolve().parents[3]
    sample_root = python_root / "samples" / "azure_functions"
    integration_test_root = Path(__file__).parent / "integration_tests"

    sample_names = {
        path.name for path in sample_root.iterdir() if path.is_dir() and (path / "function_app.py").is_file()
    }
    markers_by_test = {
        test_file.name: _sample_markers(test_file) for test_file in integration_test_root.glob("test_*.py")
    }

    invalid_tests = {test_name: markers for test_name, markers in markers_by_test.items() if len(markers) != 1}
    assert not invalid_tests, f"Integration test modules must declare exactly one sample marker: {invalid_tests}"

    skipped_tests = {
        test_file.name: skip_lines
        for test_file in integration_test_root.glob("test_*.py")
        if (skip_lines := _unconditional_skip_lines(test_file))
    }
    assert not skipped_tests, f"Integration test modules must not contain unconditional skips: {skipped_tests}"

    covered_samples = set().union(*markers_by_test.values())
    assert covered_samples == sample_names, (
        f"Missing integration coverage: {sorted(sample_names - covered_samples)}; "
        f"stale integration markers: {sorted(covered_samples - sample_names)}"
    )


def test_integration_pipelines_run_azure_functions_suite() -> None:
    """Require every Python integration pipeline to execute the Azure Functions suite."""
    repo_root = Path(__file__).resolve().parents[4]
    pipeline_paths = [
        repo_root / ".github" / "workflows" / "python-integration-tests.yml",
        repo_root / "eng" / "templates" / "jobs" / "python-integration-tests.yml",
    ]
    # This package is a standalone uv project with its own environment, so the suite
    # runs from the package directory and the pytest target is relative to it.
    required_fragments = ("python/packages/azurefunctions", "pytest tests/integration_tests")

    missing_fragments = {
        str(path.relative_to(repo_root)): missing
        for path in pipeline_paths
        if (
            missing := [fragment for fragment in required_fragments if fragment not in path.read_text(encoding="utf-8")]
        )
    }
    assert not missing_fragments, f"Azure Functions integration suite is not invoked by: {missing_fragments}"


def test_sample_hosts_use_the_compatible_ga_bundle_and_azure_storage() -> None:
    sample_root = Path(__file__).resolve().parents[3] / "samples" / "azure_functions"
    hosts = list(sample_root.glob("*/host.json"))
    assert len(hosts) == 14
    for path in hosts:
        config = json.loads(path.read_text(encoding="utf-8"))
        assert config["extensionBundle"] == {
            "id": "Microsoft.Azure.Functions.ExtensionBundle",
            "version": "[4.38.1, 5.0.0)",
        }, path
        assert config["extensions"]["durableTask"]["storageProvider"]["type"] == "AzureStorage", path


def test_integration_setup_uses_current_azurite_without_api_validation_bypass() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    for path in (
        repo_root / ".github" / "actions" / "azure-functions-integration-setup" / "action.yml",
        repo_root / "eng" / "templates" / "jobs" / "setup-integration-infra.yml",
    ):
        setup = path.read_text(encoding="utf-8")
        assert "mcr.microsoft.com/azure-storage/azurite:3.37.0" in setup, path
        assert "--skipApiVersionCheck" not in setup, path


def test_sample_guidance_keeps_azurite_api_validation_enabled() -> None:
    sample_root = Path(__file__).resolve().parents[3] / "samples" / "azure_functions"
    for path in sample_root.rglob("*"):
        if path.suffix in (".md", ".http", ".py"):
            assert "--skipApiVersionCheck" not in path.read_text(encoding="utf-8"), path


def _obsolete_dts_prerequisite_lines(test_file: Path) -> list[str]:
    tree = ast.parse(test_file.read_text(encoding="utf-8"), filename=str(test_file))
    docstring = ast.get_docstring(tree) or ""
    obsolete_phrases = (
        "Durable Task Scheduler emulator running",
        "Core Tools, Durable Task Scheduler",
        "DTS on 8080",
    )
    return [line for line in docstring.splitlines() if any(phrase in line for phrase in obsolete_phrases)]


def test_azure_storage_integration_guidance_has_no_obsolete_dts_prerequisites() -> None:
    integration_root = Path(__file__).parent / "integration_tests"
    stale_guidance = {
        path.name: lines
        for path in integration_root.glob("test_*.py")
        if (lines := _obsolete_dts_prerequisite_lines(path))
    }
    assert not stale_guidance, f"Azure Storage integration tests do not require DTS: {stale_guidance}"


@pytest.mark.parametrize(
    "prerequisite",
    [
        "- Durable Task Scheduler emulator running on localhost:8080",
        "- Azure Functions Core Tools, Durable Task Scheduler, and Azurite configured",
        "DTS on 8080, and the selected venv's test dependencies",
    ],
)
def test_dts_prerequisite_guard_detects_stale_module_guidance(tmp_path: Path, prerequisite: str) -> None:
    test_file = tmp_path / "test_sample.py"
    test_file.write_text(f'"""Prerequisites:\n{prerequisite}\n"""\n', encoding="utf-8")
    assert _obsolete_dts_prerequisite_lines(test_file) == [prerequisite]


def test_dts_prerequisite_guard_ignores_runtime_configuration(tmp_path: Path) -> None:
    test_file = tmp_path / "test_sample.py"
    test_file.write_text(
        '"""Requires Core Tools and Azurite."""\nconnection_key = "DURABLE_TASK_SCHEDULER_CONNECTION_STRING"\n',
        encoding="utf-8",
    )
    assert _obsolete_dts_prerequisite_lines(test_file) == []


def test_skip_guard_detects_decorators_and_module_markers(tmp_path: Path) -> None:
    """Detect called, bare, and module-level unconditional skip markers."""
    test_file = tmp_path / "test_sample.py"
    test_file.write_text(
        """
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.skip(reason="disabled")]

@pytest.mark.skip
def test_bare_skip():
    pass

@pytest.mark.skip(reason="disabled")
def test_called_skip():
    pass
""",
        encoding="utf-8",
    )

    assert len(_unconditional_skip_lines(test_file)) == 3


def test_skip_guard_ignores_conditional_runtime_skips(tmp_path: Path) -> None:
    """Allow runtime skips that depend on conditions inside a test."""
    test_file = tmp_path / "test_sample.py"
    test_file.write_text(
        """
import pytest

def test_conditionally_skipped(condition):
    if condition:
        pytest.skip("not supported in this environment")
""",
        encoding="utf-8",
    )

    assert _unconditional_skip_lines(test_file) == []
