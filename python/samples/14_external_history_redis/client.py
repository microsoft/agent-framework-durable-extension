# Copyright (c) Microsoft. All rights reserved.

"""Client that exercises a durable agent whose history lives in Redis.

Runs a multi-turn conversation against the ``Archivist`` agent hosted by ``worker.py`` and shows
that a user-chosen external store keeps the conversation going under the durable runtime.
"""

import logging
import os

from agent_framework_durabletask import DurableAIAgentClient
from azure.identity import AzureCliCredential
from dotenv import load_dotenv
from durabletask.azuremanaged.client import DurableTaskSchedulerClient

load_dotenv()

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

FACT = "My library card number is 4417."


def _resolve_taskhub(taskhub: str | None) -> str:
    taskhub_name = taskhub if taskhub is not None else os.getenv("TASKHUB")
    if not taskhub_name or taskhub_name != taskhub_name.strip() or taskhub_name.strip().casefold() == "default":
        raise ValueError("Set TASKHUB to a non-default, non-blank hub name before running this sample.")
    return taskhub_name


def get_client(
    taskhub: str | None = None, endpoint: str | None = None, log_handler: logging.Handler | None = None
) -> DurableAIAgentClient:
    """Create a configured DurableAIAgentClient.

    Args:
        taskhub: Task hub name, or TASKHUB from the environment
        endpoint: Scheduler endpoint (defaults to ENDPOINT env var or "http://localhost:8080")
        log_handler: Optional logging handler for client logging

    Returns:
        Configured DurableAIAgentClient instance
    """
    taskhub_name = _resolve_taskhub(taskhub)
    endpoint_url = endpoint or os.getenv("ENDPOINT", "http://localhost:8080")

    credential = None if endpoint_url == "http://localhost:8080" else AzureCliCredential()

    dts_client = DurableTaskSchedulerClient(
        host_address=endpoint_url,
        secure_channel=endpoint_url != "http://localhost:8080",
        taskhub=taskhub_name,
        token_credential=credential,
        log_handler=log_handler,
    )

    return DurableAIAgentClient(dts_client)


def run_client(agent_client: DurableAIAgentClient) -> None:
    """Run a multi-turn conversation served from the external Redis store.

    Args:
        agent_client: The durable agent client to use.
    """
    agent = agent_client.get_agent("Archivist")
    session = agent.create_session()

    print("Running a multi-turn conversation backed by Redis...\n")

    print(f"[user]  {FACT}")
    print(f"[agent] {agent.run(FACT, session=session).text}\n")

    question = "What is my library card number? Reply with just the number."
    answer = agent.run(question, session=session)
    print(f"[user]  {question}")
    print(f"[agent] {answer.text}\n")

    if "4417" in answer.text:
        print("The agent recalled the fact, consistent with Redis supplying the prior turn.")
    else:
        print("The agent did not recall the fact. Inspect its response and Redis history for errors.")


def main() -> None:
    """Client entry point."""
    try:
        run_client(get_client())
    except Exception as e:
        logger.exception(f"Error during agent interaction: {e}")


if __name__ == "__main__":
    main()
