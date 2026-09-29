# Copyright (c) Microsoft. All rights reserved.

"""Migration JSON admission and cold state through the registered DT SDK entity."""

from copy import deepcopy
from typing import Any

import pytest
from _migration_json_test_support import (
    _MARKER,
    _MIGRATED,
    _assert_cold_reset,
    _assert_ingress_and_retry,
    _digest,
    _EntityHost,
    _json,
    _payload,
    _request,
)
from _migration_json_test_support import migration_clock as migration_clock


@pytest.mark.parametrize("case", ["plain", "dt-false", "dt-true"])
@pytest.mark.parametrize("location", ["source", "completion"])
def test_native_migration_preserves_json_and_digest(case: str, location: str, migration_clock: Any) -> None:
    _assert_ingress_and_retry(_EntityHost("dt"), case, location, migration_clock)


@pytest.mark.parametrize("case", ["plain", "dt-false", "dt-true"])
def test_native_cold_migration_reset_preserves_results(case: str, migration_clock: Any) -> None:
    _assert_cold_reset(_EntityHost("dt"), case, migration_clock)


@pytest.mark.parametrize("change", ["erase-marker", "false-to-zero", "change-value"])
def test_native_migration_retry_compares_original_json(change: str, migration_clock: Any) -> None:
    host = _EntityHost("dt")
    request = _request(_payload("dt-false"), "completion")
    before = _json(request)
    assert _json(host.call("migrate", request)) == _json(_MIGRATED)
    host.assert_idle()
    committed = host.raw
    changed = deepcopy(request)
    future = changed["completionEvidence"]["results"][0]["futureResult"]
    if change == "erase-marker":
        del future[_MARKER]
    elif change == "false-to-zero":
        assert future[_MARKER] is False
        future[_MARKER] = 0
        assert type(future[_MARKER]) is int
    else:
        future["keep"] = ["different"]
    changed_before = _json(changed)
    assert changed_before != before and _digest(request) != _digest(changed)
    # Do not check first-write fidelity here. The old decoder can complete that
    # write, then conflate erased/false/zero markers at this distinct retry boundary.
    with pytest.raises(ValueError, match="Migration destination must be empty"):
        host.call("migrate", changed)
    host.assert_writes(0)
    assert host.shim.encode_state() == host.raw == committed
    host.assert_idle()
    assert _json(request) == before and _json(changed) == changed_before
