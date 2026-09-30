# Copyright (c) Microsoft. All rights reserved.

"""Functions HITL lookup of nested child instances."""

import asyncio
import json
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock

import pytest
from _workflow_lifecycle_test_support import _typed_workflow
from azure.durable_functions.internal.serialization import DEFAULT_FUNCTIONS_DATA_CONVERTER
from durabletask.client import OrchestrationState, OrchestrationStatus

from agent_framework_azurefunctions import AgentFunctionApp


def _state(instance_id: str, custom_status: dict[str, Any]) -> OrchestrationState:
    moment = datetime(2026, 9, 30)
    return OrchestrationState(
        instance_id=instance_id,
        name="dafx-lifecycle",
        runtime_status=OrchestrationStatus.RUNNING,
        created_at=moment,
        last_updated_at=moment,
        serialized_input=None,
        serialized_output=None,
        serialized_custom_status=json.dumps(custom_status),
        failure_details=None,
        _data_converter=DEFAULT_FUNCTIONS_DATA_CONVERTER,
    )


@pytest.mark.parametrize("children", [["current-child"], {"1": "current-child"}])
def test_af_child_lookup_and_discovery_reject_retired_or_legacy_slot(children: Any) -> None:
    workflow, _ = _typed_workflow(str)
    app = AgentFunctionApp(workflow=workflow, enable_health_check=False, deployment_mode="isolated_v2")
    statuses = {
        "root-run": {"subworkflows": {"sub": children}},
        "current-child": {"pending_requests": {"approval": {"request_id": "approval"}}},
    }
    client = AsyncMock()
    client.get_orchestration_state.side_effect = lambda instance: _state(instance, statuses[instance])
    assert asyncio.run(app._resolve_hitl_target(client, "root-run", "sub~0~approval")) is None
    requests = asyncio.run(app._gather_pending_hitl_requests(client, statuses["root-run"]))
    assert [request_id for request_id, _ in requests] == ([] if isinstance(children, list) else ["sub~1~approval"])
    if isinstance(children, dict):
        assert asyncio.run(app._resolve_hitl_target(client, "root-run", "sub~1~approval")) == (
            "current-child",
            "approval",
        )
