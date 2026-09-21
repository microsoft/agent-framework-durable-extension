# Copyright (c) Microsoft. All rights reserved.

"""Private durable boundary for discarded agent replies, without executing an agent."""

from __future__ import annotations

import json
from typing import Any, cast


def workflow_hitl_checkpoint_name(workflow_name: str) -> str:
    """Name one checkpoint per workflow, disjoint from ``dafx-`` executor names."""
    return f"dafx__hitl-{workflow_name}"


def execute_hitl_checkpoint(input_data: str) -> str:
    """Acknowledge a trusted rejection instruction and return no application data.

    The orchestrator has already discarded the external payload. This activity
    receives only the server-owned request ID and a fixed instruction, never a
    response, type descriptor, executor, agent, or shared state. Re-execution is
    harmless. The durable completion breaks chains of already-buffered waits.
    """
    instruction: Any = json.loads(input_data)
    if not isinstance(instruction, dict):
        raise ValueError("Malformed internal HITL checkpoint instruction.")
    instruction = cast(dict[str, Any], instruction)
    if (
        set(instruction) != {"request_id", "status"}
        or not isinstance(instruction["request_id"], str)
        or not instruction["request_id"]
        or instruction["status"] != "invalidreply"
    ):
        raise ValueError("Malformed internal HITL checkpoint instruction.")
    return "null"
