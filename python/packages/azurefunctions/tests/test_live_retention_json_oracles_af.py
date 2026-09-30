# Copyright (c) Microsoft. All rights reserved.

"""The shared live JSON oracle checks, run against the Functions live suite's helper."""

from collections.abc import Callable
from typing import Any

import pytest
from test_live_retention_json_oracles import (
    _live_equal,
    test_live_json_oracles_accept_object_key_order_but_preserve_array_order_and_null_presence,
    test_live_json_oracles_reject_identical_non_json_objects,
    test_live_json_oracles_reject_nonfinite_values_even_when_python_equality_would_skip_serialization,
    test_live_json_oracles_reject_python_equal_but_wire_distinct_values,
)

__all__ = [
    "test_live_json_oracles_accept_object_key_order_but_preserve_array_order_and_null_presence",
    "test_live_json_oracles_reject_identical_non_json_objects",
    "test_live_json_oracles_reject_nonfinite_values_even_when_python_equality_would_skip_serialization",
    "test_live_json_oracles_reject_python_equal_but_wire_distinct_values",
]


@pytest.fixture
def live_equal(monkeypatch: pytest.MonkeyPatch) -> Callable[[Any, Any, str], None]:
    return _live_equal("azurefunctions", monkeypatch)
