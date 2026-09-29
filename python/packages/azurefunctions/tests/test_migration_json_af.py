# Copyright (c) Microsoft. All rights reserved.

"""Migration JSON through indexed Functions entity registration and SDK batches."""

from typing import Any

import pytest
from _migration_json_test_support import _assert_cold_reset, _assert_ingress_and_retry, _EntityHost
from _migration_json_test_support import migration_clock as migration_clock


@pytest.mark.parametrize("case", ["plain", "af-sentinel"])
@pytest.mark.parametrize("location", ["source", "completion"])
def test_registered_migration_preserves_json_and_digest(case: str, location: str, migration_clock: Any) -> None:
    _assert_ingress_and_retry(_EntityHost("af"), case, location, migration_clock)


@pytest.mark.parametrize("case", ["plain", "af-sentinel"])
def test_registered_cold_migration_reset_preserves_results(case: str, migration_clock: Any) -> None:
    _assert_cold_reset(_EntityHost("af"), case, migration_clock)
