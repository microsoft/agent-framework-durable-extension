# Copyright (c) Microsoft. All rights reserved.

"""Migration JSON through the Functions app's registered entity and converter."""

from typing import Any

import pytest
from _af_worker_test_support import _af_worker
from _migration_json_test_support import _assert_cold_reset, _assert_ingress_and_retry, _EntityHost
from _migration_json_test_support import migration_clock as migration_clock
from agent_framework import Agent

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


@pytest.mark.parametrize("case", ["plain", "af-sentinel"])
def test_registered_cold_migration_reset_preserves_results(case: str, migration_clock: Any) -> None:
    _assert_cold_reset(_EntityHost(_register), case, migration_clock)
