# Copyright (c) Microsoft. All rights reserved.

import pytest
from agent_framework import RunnerContext, ToolTypes, WorkflowEvent, WorkflowMessage, tool
from agent_framework._workflows._state import State

from agent_framework_durabletask._workflows.runner_context import CapturingRunnerContext


@tool
def echo(value: str) -> str:
    """Echo a runtime tool argument."""
    return value


def test_runner_context_implements_current_protocol() -> None:
    assert isinstance(CapturingRunnerContext(), RunnerContext)


@pytest.mark.asyncio
async def test_build_checkpoint_raises_not_implemented() -> None:
    """Checkpoint construction must not fall through to the protocol stub."""
    context = CapturingRunnerContext()

    with pytest.raises(NotImplementedError):
        await context.build_checkpoint("test_workflow", "abc123", State(), None, 1)


def test_runtime_tools_can_be_set_and_cleared() -> None:
    context = CapturingRunnerContext()
    tools: list[ToolTypes] = [echo]

    context.set_runtime_tools(tools)

    assert context.get_runtime_tools() == tools
    context.clear_runtime_tools()
    assert context.get_runtime_tools() is None


@pytest.mark.asyncio
async def test_reset_clears_runtime_tools_and_request_scoped_state() -> None:
    context = CapturingRunnerContext()
    other = CapturingRunnerContext()
    tools: list[ToolTypes] = [echo]
    context.set_runtime_tools(tools)
    other.set_runtime_tools(tools)
    context.set_streaming(True)
    await context.send_message(WorkflowMessage(data="test", target_id="target", source_id="source"))
    await context.add_request_info_event(WorkflowEvent.request_info("request-1", "executor", "approve", str))

    context.reset_for_new_run()

    assert context.get_runtime_tools() is None
    assert context.is_streaming() is False
    assert await context.has_messages() is False
    assert await context.has_events() is False
    assert await context.get_pending_request_info_events() == {}
    assert other.get_runtime_tools() == tools
    context.set_runtime_tools([])
    assert context.get_runtime_tools() == []
    context.set_runtime_tools(None)
    assert context.get_runtime_tools() is None


@pytest.mark.asyncio
async def test_cancel_request_info_events_removes_selected_requests() -> None:
    context = CapturingRunnerContext()
    event = WorkflowEvent.request_info("request-1", "executor", {"question": "approve"}, str)
    retained = WorkflowEvent.request_info("request-2", "executor", {"question": "keep"}, str)
    await context.add_request_info_event(event)
    await context.add_request_info_event(retained)

    cancelled = await context.cancel_request_info_events({"request-1", "missing"})

    assert cancelled == {"request-1": event}
    assert await context.get_pending_request_info_events() == {"request-2": retained}
    assert await context.cancel_request_info_events({"request-1"}) == {}
