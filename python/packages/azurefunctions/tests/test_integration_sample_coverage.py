# Copyright (c) Microsoft. All rights reserved.

"""Coverage checks for the Azure Functions sample integration tests."""

import ast
from pathlib import Path


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
