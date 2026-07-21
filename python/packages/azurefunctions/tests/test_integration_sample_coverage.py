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


def test_every_sample_has_integration_coverage() -> None:
    """Require each Azure Functions sample to have one integration test module."""
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

    covered_samples = set().union(*markers_by_test.values())
    assert covered_samples == sample_names, (
        f"Missing integration coverage: {sorted(sample_names - covered_samples)}; "
        f"stale integration markers: {sorted(covered_samples - sample_names)}"
    )
