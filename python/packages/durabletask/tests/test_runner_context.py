# Copyright (c) Microsoft. All rights reserved.

import pytest
from agent_framework._workflows._state import State

from agent_framework_durabletask._workflows.runner_context import CapturingRunnerContext


@pytest.mark.asyncio
async def test_build_checkpoint_raises_not_implemented() -> None:
    """Checkpoint construction must not fall through to the protocol stub."""
    context = CapturingRunnerContext()

    with pytest.raises(NotImplementedError):
        await context.build_checkpoint("test_workflow", "abc123", State(), None, 1)
