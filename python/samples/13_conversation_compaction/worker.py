# Copyright (c) Microsoft. All rights reserved.

"""Worker hosting an agent whose conversation history is compacted as it grows.

The agent is configured exactly as it would be for in-process Agent Framework: an
``InMemoryHistoryProvider`` plus a ``CompactionProvider``. Registering it with the durable
runtime transparently swaps the history provider for a durable-backed one, so:

- conversation history is persisted in the agent's durable entity and survives restarts,
- the compaction strategy still runs, and its annotations are persisted alongside the
  messages, so compaction state is not recomputed on every turn,
- only the messages compaction keeps are sent to the model, bounding context growth.

No durable-specific configuration is required on the agent itself.

Note on service-managed conversations: compaction applies to history the *client* owns. When a
chat client keeps the conversation on the service (Foundry and the Responses API both do so by
default), the service owns the model's context and the durable entity keeps the full transcript
purely as a record. This sample therefore sets ``store=False`` so history is client-side and
compaction has something to compact.

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
    """Create an agent that remembers facts while its context stays bounded.

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
    agent_worker = DurableAIAgentWorker(worker)

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
