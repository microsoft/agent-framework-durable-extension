# Copyright (c) Microsoft. All rights reserved.

"""Worker hosting an agent whose conversation history lives in Redis, not in durable state.

The agent is configured exactly as it would be for in-process Agent Framework: a history
provider the user chose (here Redis) is passed as a context provider. Registering it with the
durable runtime requires no changes:

- the runtime **leaves the provider alone** - the user picked where their conversation lives,
- it hands the provider the entity's **stable** session id on every turn, so history continues
  across turns and across worker restarts,
- durable state still records the conversation for audit, and execution stays durable.

Contrast with ``13_conversation_compaction``, where an in-memory provider is transparently
swapped for a durable-backed one.

Prerequisites:
- Set AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_MODEL
- Sign in with Azure CLI for AzureCliCredential authentication
- Start a Durable Task Scheduler and a Redis instance (e.g., using Docker)
"""

import asyncio
import logging
import os

from agent_framework import Agent
from agent_framework.openai import OpenAIChatClient
from agent_framework_durabletask import DurableAIAgentWorker
from azure.identity import AzureCliCredential
from azure.identity.aio import AzureCliCredential as AsyncAzureCliCredential
from dotenv import load_dotenv
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker
from redis_history_provider import RedisHistoryProvider  # pyrefly: ignore[missing-import]

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


def create_archivist_agent() -> Agent:
    """Create an agent whose history is stored in Redis.

    Returns:
        Agent: The configured Archivist agent.
    """
    history = RedisHistoryProvider(os.getenv("REDIS_CONNECTION_STRING", "redis://localhost:6379"))

    return Agent(
        client=OpenAIChatClient(
            azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
            model=os.environ["AZURE_OPENAI_MODEL"],
            credential=AsyncAzureCliCredential(),
        ),
        name="Archivist",
        instructions=(
            "You are a concise assistant. Answer in one short sentence. "
            "When the user tells you a fact, remember it and repeat it exactly when asked."
        ),
        # Keep the conversation client-side so the history provider owns the model's context.
        default_options={"store": False},
        context_providers=[history],
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
    """Register the Redis-backed agent with the durable worker.

    Args:
        worker: The DurableTaskSchedulerWorker instance

    Returns:
        DurableAIAgentWorker with agents registered
    """
    agent_worker = DurableAIAgentWorker(worker)

    agent = create_archivist_agent()
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
