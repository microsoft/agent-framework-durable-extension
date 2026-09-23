# Copyright (c) Microsoft. All rights reserved.

"""Unit discrimination for both live suites' full-JSON equality helpers.

Load the modules directly, without collecting their integration directories or
requesting any integration fixture. No service or worker is started.
"""

import importlib.util
import json
import sys
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import Mock

import pytest

PACKAGES = Path(__file__).resolve().parents[2]


def _load(path: Path, name: str, monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    assert module.__file__ is not None and Path(module.__file__).resolve() == path.resolve()
    return module


@pytest.fixture(params=["durabletask", "azurefunctions"])
def live_equal(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> Callable[[Any, Any, str], None]:
    # Guard accidental launch if either integration module later acquires import-time work.
    monkeypatch.setattr("subprocess.Popen", Mock(side_effect=AssertionError("Unit JSON checks cannot start a host")))
    package = request.param
    if package == "durabletask":
        # The real module imports sibling constants. Give it this exact sibling,
        # then restore sys.modules at teardown, with no permanent sys.path edits.
        _load(
            PACKAGES / "durabletask/tests/integration_tests/live_retention_worker.py",
            "live_retention_worker",
            monkeypatch,
        )
        filename = "test_15_dt_live_retention.py"
    else:
        filename = "test_16_live_media_retention.py"
    module = _load(
        PACKAGES / package / "tests/integration_tests" / filename,
        f"_live_json_oracle_{package}",
        monkeypatch,
    )
    return module._equal


@pytest.mark.parametrize(
    ("original", "replacement"),
    [(False, 0), (True, 1), (0, 0.0), (1, 1.0), (0.0, -0.0)],
    ids=["false-int", "true-int", "zero-float", "one-float", "signed-zero"],
)
def test_live_json_oracles_reject_python_equal_but_wire_distinct_values(
    live_equal: Callable[[Any, Any, str], None], original: Any, replacement: Any
) -> None:
    expected: dict[str, Any] = {"nested": {"values": [original], "media": "private-media-sentinel"}, "nullable": None}
    actual = deepcopy(expected)
    actual["nested"]["values"][0] = replacement
    assert actual == expected
    assert json.dumps(actual, sort_keys=True, allow_nan=False) != json.dumps(expected, sort_keys=True, allow_nan=False)
    with pytest.raises(pytest.fail.Exception, match="full JSON mismatch") as caught:
        live_equal(actual, expected, "typed payload")
    assert "private-media-sentinel" not in str(caught.value)


def test_live_json_oracles_accept_object_key_order_but_preserve_array_order_and_null_presence(
    live_equal: Callable[[Any, Any, str], None],
) -> None:
    live_equal({"b": None, "a": [False, 0, "界"]}, {"a": [False, 0, "界"], "b": None}, "key order")
    for actual, expected in (
        ({"a": [1, 2]}, {"a": [2, 1]}),
        ({"a": None}, {}),
        ({"a": "0"}, {"a": 0}),
        ({"a": True}, {"a": False}),
    ):
        with pytest.raises(pytest.fail.Exception, match="full JSON mismatch"):
            live_equal(actual, expected, "JSON structure")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
@pytest.mark.parametrize("side", ["actual", "expected", "both"])
def test_live_json_oracles_reject_nonfinite_values_even_when_python_equality_would_skip_serialization(
    live_equal: Callable[[Any, Any, str], None], value: float, side: str
) -> None:
    invalid = {"nested": [value]}
    actual = invalid if side in ("actual", "both") else {"nested": [0]}
    expected = invalid if side in ("expected", "both") else {"nested": [0]}
    with pytest.raises(ValueError):
        live_equal(actual, expected, "nonfinite")


def test_live_json_oracles_reject_identical_non_json_objects(live_equal: Callable[[Any, Any, str], None]) -> None:
    invalid = {"nested": [object()]}
    with pytest.raises(TypeError):
        live_equal(invalid, invalid, "non-JSON")
