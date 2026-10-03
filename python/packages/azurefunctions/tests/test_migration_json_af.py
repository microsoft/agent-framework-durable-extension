# Copyright (c) Microsoft. All rights reserved.

"""Migration JSON through indexed Functions entity registration and SDK batches."""

from typing import Any

import pytest
from _migration_json_test_support import (
    _COUNTER_CASES,
    _af_batch,
    _assert_cold_reset,
    _assert_counter_ingress,
    _assert_float_digest_compatibility,
    _assert_ingress_and_retry,
    _assert_opaque_counter_ingress,
    _EntityHost,
)
from _migration_json_test_support import migration_clock as migration_clock


@pytest.mark.parametrize("case", ["plain", "af-sentinel"])
@pytest.mark.parametrize("location", ["source", "completion"])
def test_registered_migration_preserves_json_and_digest(case: str, location: str, migration_clock: Any) -> None:
    _assert_ingress_and_retry(_EntityHost("af"), case, location, migration_clock)


@pytest.mark.parametrize("location", ["source", "source-position", "source-usage", "completion", "completion-content"])
@pytest.mark.parametrize("token,expected", _COUNTER_CASES)
def test_registered_migration_exact_counters(
    location: str, token: str, expected: int | float | None, migration_clock: Any
) -> None:
    _assert_counter_ingress(_EntityHost("af"), location, token, expected)


@pytest.mark.parametrize("location", ["source", "completion"])
@pytest.mark.parametrize("token", ["1.00000000000000001", "1e-9999999999999999999"])
def test_registered_migration_opaque_numbers(location: str, token: str, migration_clock: Any) -> None:
    _assert_opaque_counter_ingress(_EntityHost("af"), location, token)


@pytest.mark.parametrize("location", ["source", "completion"])
def test_registered_migration_public_digest_and_float_retry(location: str, migration_clock: Any) -> None:
    _assert_float_digest_compatibility(_EntityHost("af"), location)


def test_migration_shaped_business_input_keeps_normal_decoding() -> None:
    import json

    from agent_framework_azurefunctions._entity_json import create_json_entity

    def echo(context: Any) -> None:
        context.set_result(context.get_input())

    wire = '{"source":{"schemaVersion":"1.1.0","data":{"truncation":{"evictedMessageCount":1.00000000000000001}}}}'
    batch = _af_batch(create_json_entity(echo), "run", None, None, wire)
    assert json.loads(batch["results"][0]["result"]) == json.loads(wire)


@pytest.mark.parametrize("case", ["plain", "af-sentinel"])
def test_registered_cold_migration_reset_preserves_results(case: str, migration_clock: Any) -> None:
    _assert_cold_reset(_EntityHost("af"), case, migration_clock)
