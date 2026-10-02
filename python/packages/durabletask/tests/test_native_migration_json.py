# Copyright (c) Microsoft. All rights reserved.

"""Migration JSON admission and cold state through the registered DT SDK entity."""

from copy import deepcopy
from typing import Any

import pytest
from _migration_json_test_support import (
    _COUNTER_CASES,
    _MARKER,
    _MIGRATED,
    _assert_cold_reset,
    _assert_counter_ingress,
    _assert_float_digest_compatibility,
    _assert_ingress_and_retry,
    _assert_opaque_counter_ingress,
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


@pytest.mark.parametrize("location", ["source", "source-position", "source-usage", "completion", "completion-content"])
@pytest.mark.parametrize("token,expected", _COUNTER_CASES)
def test_native_migration_exact_counters(
    location: str, token: str, expected: int | float | None, migration_clock: Any
) -> None:
    _assert_counter_ingress(_EntityHost("dt"), location, token, expected)


@pytest.mark.parametrize("location", ["source", "completion"])
@pytest.mark.parametrize("token", ["1.00000000000000001", "1e-9999999999999999999"])
def test_native_migration_opaque_numbers(location: str, token: str, migration_clock: Any) -> None:
    _assert_opaque_counter_ingress(_EntityHost("dt"), location, token)


@pytest.mark.parametrize("location", ["source", "completion"])
def test_native_migration_public_digest_and_float_retry(location: str, migration_clock: Any) -> None:
    _assert_float_digest_compatibility(_EntityHost("dt"), location)


def test_migration_shaped_business_input_keeps_normal_decoding() -> None:
    import json

    from agent_framework_durabletask._json_payload import JsonPayload

    wire = '{"source":{"schemaVersion":"1.1.0","data":{"truncation":{"evictedMessageCount":1.00000000000000001}}}}'
    host = _EntityHost("dt")
    assert host.worker._data_converter.deserialize(wire, JsonPayload) == json.loads(wire)


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
