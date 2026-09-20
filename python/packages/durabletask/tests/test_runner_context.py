# Copyright (c) Microsoft. All rights reserved.

import pytest
from agent_framework import WorkflowEvent
from agent_framework._workflows._state import State

from agent_framework_durabletask._workflows.runner_context import CapturingRunnerContext


@pytest.mark.asyncio
async def test_build_checkpoint_raises_not_implemented() -> None:
    """Checkpoint construction must not fall through to the protocol stub."""
    context = CapturingRunnerContext()

    with pytest.raises(NotImplementedError):
        await context.build_checkpoint("test_workflow", "abc123", State(), None, 1)


def test_runtime_tools_can_be_set_and_cleared() -> None:
    context = CapturingRunnerContext()
    tools = [object()]

    context.set_runtime_tools(tools)

    assert context.get_runtime_tools() == tools
    context.clear_runtime_tools()
    assert context.get_runtime_tools() is None


@pytest.mark.asyncio
async def test_cancel_request_info_events_removes_selected_requests() -> None:
    context = CapturingRunnerContext()
    event = WorkflowEvent.request_info("request-1", "executor", {"question": "approve"}, str)
    await context.add_request_info_event(event)

    cancelled = await context.cancel_request_info_events({"request-1", "missing"})

    assert cancelled == {"request-1": event}
    assert await context.get_pending_request_info_events() == {}
