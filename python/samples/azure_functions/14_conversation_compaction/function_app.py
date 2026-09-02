# Copyright (c) Microsoft. All rights reserved.

"""Host an agent whose conversation history is compacted as it grows, inside Azure Functions.

The agent is configured exactly as it would be for in-process Agent Framework: an
``InMemoryHistoryProvider`` plus a ``CompactionProvider``. Registering it with
``AgentFunctionApp`` transparently swaps the history provider for a durable-backed one, so
history is persisted in the agent's durable entity, the compaction strategy still runs, and its
annotations are persisted alongside the messages. Only the messages compaction keeps are sent to
the model, bounding context growth.

This is the Azure Functions counterpart to the standalone ``13_conversation_compaction`` sample.

Note on service-managed conversations: compaction applies to history the *client* owns. When a
chat client keeps the conversation on the service (Foundry and the Responses API both do so by
default), the service owns the model's context and the durable entity keeps the full transcript
purely as a record. This sample therefore sets ``store=False`` so history is client-side and
compaction has something to compact.

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
#    Set retention="follow_compaction" here to delete compacted-out messages immediately, with
#    pressure eviction as a fallback if the remaining state is still too large.
app = AgentFunctionApp(agents=[_create_agent()], enable_health_check=True, max_poll_retries=50)

"""
Expected behavior when posting several turns with the same `session_id`:

- every turn is answered with the earlier turns in context,
- the model's context stops growing once the sliding window fills,
- the durable entity keeps the whole conversation, with compacted-out messages marked excluded.
"""
