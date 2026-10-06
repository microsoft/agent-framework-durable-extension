# Copyright (c) Microsoft. All rights reserved.

"""Migration JSON through the Functions app's registered entity and converter."""

import json
from typing import Any

import pytest
from _af_worker_test_support import _af_worker
from _migration_json_test_support import (
    _COUNTER_CASES,
    _assert_cold_reset,
    _assert_counter_ingress,
    _assert_float_digest_compatibility,
    _assert_ingress_and_retry,
    _assert_opaque_counter_ingress,
    _EntityHost,
)
from _migration_json_test_support import migration_clock as migration_clock
from agent_framework import Agent
from agent_framework_durabletask._json_payload import JsonPayload

from agent_framework_azurefunctions import AgentFunctionApp


def _register(agent: Agent) -> Any:
    app = AgentFunctionApp(
        agents=[agent],
        enable_health_check=False,
        enable_http_endpoints=False,
        deployment_mode="isolated_v2",
        response_delivery_window_seconds=3600,
    )
    return _af_worker(app)


@pytest.mark.parametrize("case", ["plain", "af-sentinel"])
@pytest.mark.parametrize("location", ["source", "completion"])
def test_registered_migration_preserves_json_and_digest(case: str, location: str, migration_clock: Any) -> None:
    _assert_ingress_and_retry(_EntityHost(_register), case, location, migration_clock)


@pytest.mark.parametrize("location", ["source", "source-position", "source-usage", "completion", "completion-content"])
@pytest.mark.parametrize("token,expected", _COUNTER_CASES)
def test_registered_migration_exact_counters(
    location: str, token: str, expected: int | float | None, migration_clock: Any
) -> None:
    _assert_counter_ingress(_EntityHost(_register), location, token, expected)


@pytest.mark.parametrize("location", ["source", "completion"])
@pytest.mark.parametrize("token", ["1.00000000000000001", "1e-9999999999999999999"])
def test_registered_migration_opaque_numbers(location: str, token: str, migration_clock: Any) -> None:
    _assert_opaque_counter_ingress(_EntityHost(_register), location, token)


@pytest.mark.parametrize("location", ["source", "completion"])
def test_registered_migration_public_digest_and_float_retry(location: str, migration_clock: Any) -> None:
    _assert_float_digest_compatibility(_EntityHost(_register), location)


def test_migration_shaped_business_input_keeps_normal_decoding() -> None:
    wire = '{"source":{"schemaVersion":"1.1.0","data":{"truncation":{"evictedMessageCount":1.00000000000000001}}}}'
    host = _EntityHost(_register)
    assert host.worker._data_converter.deserialize(wire, JsonPayload) == json.loads(wire)


@pytest.mark.parametrize("case", ["plain", "af-sentinel"])
def test_registered_cold_migration_reset_preserves_results(case: str, migration_clock: Any) -> None:
    _assert_cold_reset(_EntityHost(_register), case, migration_clock)
