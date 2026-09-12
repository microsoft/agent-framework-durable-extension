# Copyright (c) Microsoft. All rights reserved.

"""Host an agent whose conversation history is compacted as it grows, inside Azure Functions.

The agent is configured exactly as it would be for in-process Agent Framework: an
``InMemoryHistoryProvider`` plus a ``CompactionProvider``. Registering it with
``AgentFunctionApp`` transparently swaps the history provider for a durable-backed one, so
the provider persists the inputs and outputs selected by its storage flags in the agent's durable
entity. Compaction annotations are persisted alongside those messages. Only the history groups
compaction keeps are sent to the model on the next turn.

This is the Azure Functions counterpart to the standalone ``13_conversation_compaction`` sample.

Compaction applies to history the client owns. On a service-owned turn the durable history
provider neither loads nor appends a local transcript. The entity still persists session state,
original responses in its delivery mailbox, and completion receipts. This sample sets
``store=False`` so the history provider owns model context instead of the service.

The sample explicitly keeps ``retention="keep_all"`` and ``max_state_bytes=None``. A sliding
window limits history groups, not message size or total state. Neither pruning nor an optional
pressure budget provides unlimited capacity.

Prerequisites: set `FOUNDRY_PROJECT_ENDPOINT`, `FOUNDRY_MODEL`, and sign in
with Azure CLI before starting the Functions host."""

import os
from typing import Any

from agent_framework import Agent, CompactionProvider, InMemoryHistoryProvider, SlidingWindowStrategy
from agent_framework.foundry import FoundryChatClient
from agent_framework_azurefunctions import AgentFunctionApp
from azure.identity.aio import AzureCliCredential
from dotenv import load_dotenv

load_dotenv()

# Keep only the most recent turns in the model's context. Deliberately small so the
# effect is easy to observe in a short sample conversation.
KEEP_LAST_GROUPS = 4


# 1. Instantiate the agent the ordinary core way - no durable-specific configuration.
def _create_agent() -> Any:
    """Create the Historian agent."""
    # A plain in-memory history provider: the durable runtime replaces it with a
    # durable-backed provider at registration, preserving this ``source_id`` so the
    # compaction provider below stays wired to it.
    history = InMemoryHistoryProvider(skip_excluded=True)

    compaction = CompactionProvider(
        after_strategy=SlidingWindowStrategy(keep_last_groups=KEEP_LAST_GROUPS),
        history_source_id=history.source_id,
    )

    return Agent(
        client=FoundryChatClient(
            project_endpoint=os.environ["FOUNDRY_PROJECT_ENDPOINT"],
            model=os.environ["FOUNDRY_MODEL"],
            credential=AzureCliCredential(),
        ),
        name="Historian",
        instructions=(
            "You are a concise assistant. Answer in one short sentence. "
            "When the user tells you a fact, remember it and repeat it exactly when asked."
        ),
        # Keep the conversation client-side so the history provider (and therefore compaction)
        # owns the model's context.
        default_options={"store": False},
        context_providers=[history, compaction],
    )


# 2. Register the agent with AgentFunctionApp so Azure Functions exposes the required triggers.
#    Choose retention="follow_compaction" to prune eligible exclusions. Independently, set a
#    positive integer max_state_bytes to enable pressure eviction. Functions cannot resolve
#    "backend_limit". Allow space for live responses, completion receipts and session state.
app = AgentFunctionApp(
    agents=[_create_agent()],
    enable_health_check=True,
    max_poll_retries=50,
    retention="keep_all",
    max_state_bytes=None,
)

"""
Expected behavior when posting several turns with the same `session_id`:

- each turn uses the recent history groups kept by compaction,
- the number of history groups sent to the model stops growing once the sliding window fills,
- this configuration keeps stored inputs and outputs, with compacted-out messages marked excluded,
- original responses and completion receipts are separate from the compacted local transcript.
"""
