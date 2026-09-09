# Copyright (c) Microsoft. All rights reserved.

"""Worker hosting an agent whose conversation history is compacted as it grows.

The agent is configured exactly as it would be for in-process Agent Framework: an
``InMemoryHistoryProvider`` plus a ``CompactionProvider``. Registering it with the durable
runtime transparently swaps the history provider for a durable-backed one, so:

- the history provider persists the inputs and outputs selected by its storage flags,
- that client-owned history lives in the agent's durable entity and survives restarts,
- the compaction strategy still runs, and its annotations are persisted alongside the
    messages for later turns,
- only the history groups compaction keeps are sent to the model on the next turn.

No durable-specific configuration is required on the agent itself.

The sample keeps the default ``retention="keep_all"`` and ``max_state_bytes=None``. Compaction
limits the number of history groups sent to the model, not total entity size or message size.
Pruning exclusions and pressure eviction are separate opt-ins. Neither provides unlimited capacity.

Compaction applies to history the client owns. On a service-owned turn the durable history
provider neither loads nor appends a local transcript. The entity keeps session state, original
responses in its delivery mailbox, and completion receipts. This sample sets ``store=False``
so the history provider owns the model's context instead of Foundry's service-managed history.

Prerequisites:
- Set FOUNDRY_PROJECT_ENDPOINT and FOUNDRY_MODEL
- Sign in with Azure CLI for AzureCliCredential authentication
- Start a Durable Task Scheduler (e.g., using Docker)
"""

import asyncio
import logging
import os

from agent_framework import Agent, CompactionProvider, InMemoryHistoryProvider, SlidingWindowStrategy
from agent_framework.foundry import FoundryChatClient
from agent_framework_durabletask import DurableAIAgentWorker
from azure.identity import AzureCliCredential
from azure.identity.aio import AzureCliCredential as AsyncAzureCliCredential
from dotenv import load_dotenv
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

# Keep only the most recent turns in the model's context. Deliberately small so the
# effect is easy to observe in a short sample conversation.
KEEP_LAST_GROUPS = 4


def create_historian_agent() -> Agent:
    """Create an agent that recalls facts within a sliding history window.

    Returns:
        Agent: The configured Historian agent.
    """
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
            credential=AsyncAzureCliCredential(),
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


def get_worker(
    taskhub: str | None = None, endpoint: str | None = None, log_handler: logging.Handler | None = None
) -> DurableTaskSchedulerWorker:
    """Create a configured DurableTaskSchedulerWorker.

    Args:
        taskhub: Task hub name (defaults to TASKHUB env var or "default")
        endpoint: Scheduler endpoint (defaults to ENDPOINT env var or "http://localhost:8080")
        log_handler: Optional logging handler for worker logging

    Returns:
        Configured DurableTaskSchedulerWorker instance
    """
    taskhub_name = taskhub or os.getenv("TASKHUB", "default")
    endpoint_url = endpoint or os.getenv("ENDPOINT", "http://localhost:8080")

    credential = None if endpoint_url == "http://localhost:8080" else AzureCliCredential()

    return DurableTaskSchedulerWorker(
        host_address=endpoint_url,
        secure_channel=endpoint_url != "http://localhost:8080",
        taskhub=taskhub_name,
        token_credential=credential,
        log_handler=log_handler,
    )


def setup_worker(worker: DurableTaskSchedulerWorker) -> DurableAIAgentWorker:
    """Register the compacting agent with the durable worker.

    Args:
        worker: The DurableTaskSchedulerWorker instance

    Returns:
        DurableAIAgentWorker with agents registered
    """
    # Keep compacted-out history by default. To delete eligible exclusions, choose
    # retention="follow_compaction". Pressure eviction is independent: opt in with
    # max_state_bytes="backend_limit" (1 MiB on DTS) or a positive integer budget.
    # Budget for live response payloads, receipts and session state as well as history.
    agent_worker = DurableAIAgentWorker(worker, retention="keep_all", max_state_bytes=None)

    agent = create_historian_agent()
    agent_worker.add_agent(agent)

    logger.debug(f"✓ Registered agent: {agent.name}")
    return agent_worker


async def main():
    """Main entry point for the worker process."""
    worker = get_worker()
    setup_worker(worker)

    logger.info("Worker is ready and listening for requests...")

    try:
        worker.start()
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.debug("Worker shutdown initiated")


if __name__ == "__main__":
    asyncio.run(main())
