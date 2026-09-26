# Copyright (c) Microsoft. All rights reserved.

"""Paired registered activity and native Core state controls."""

import asyncio
import json
from copy import deepcopy
from typing import Any

from _workflow_replay_test_support import _LOGGER, _worker
from agent_framework import Executor, WorkflowBuilder
from agent_framework._workflows._state import State
from durabletask.worker import _ActivityExecutor

from agent_framework_durabletask._workflows.runner_context import CapturingRunnerContext
from agent_framework_durabletask._workflows.serialization import serialize_value


def _registered_result(executor: Executor, snapshot: dict[str, Any]) -> dict[str, Any]:
    workflow = WorkflowBuilder(name="state-intent", start_executor=executor).build()
    native = _worker(workflow)
    payload = json.dumps({
        "message": "go",
        "source_executor_ids": ["seed"],
        "shared_state_snapshot": serialize_value(snapshot),
    })
    raw = _ActivityExecutor(native._registry, _LOGGER, native._data_converter).execute(
        "state-intent-instance", f"dafx-state-intent-{executor.id}", 1, json.dumps(payload)
    )
    assert raw is not None
    return json.loads(json.loads(raw))


def _core_committed_state(executor: Executor, snapshot: dict[str, Any]) -> dict[str, Any]:
    # A real, unmodified Core State and Executor.execute are the paired control.
    # No activity journal, durable delta extraction or version-detection probe.
    state = State()
    state.import_state(deepcopy(snapshot))
    asyncio.run(executor.execute("go", ["seed"], state, CapturingRunnerContext()))
    state.commit()
    return serialize_value(state.export_state())
