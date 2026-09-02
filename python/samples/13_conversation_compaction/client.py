# Copyright (c) Microsoft. All rights reserved.

"""Client that exercises a durable agent whose history is compacted as it grows.

Runs a multi-turn conversation against the ``Historian`` agent hosted by ``worker.py`` and
shows that the conversation keeps working while the model's context stays bounded.
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

# Turns that fill the conversation before recall is tested.
FILLER_TURNS = [
    "Name a color.",
    "Name a country.",
    "Name a fruit.",
    "Name a musical instrument.",
]

CODENAME = "BLUEHERON"


def get_client(
    taskhub: str | None = None, endpoint: str | None = None, log_handler: logging.Handler | None = None
) -> DurableAIAgentClient:
    """Create a configured DurableAIAgentClient.

    Args:
        taskhub: Task hub name (defaults to TASKHUB env var or "default")
        endpoint: Scheduler endpoint (defaults to ENDPOINT env var or "http://localhost:8080")
        log_handler: Optional logging handler for client logging

    Returns:
        Configured DurableAIAgentClient instance
    """
    taskhub_name = taskhub or os.getenv("TASKHUB", "default")
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
    """Run a multi-turn conversation against the compacting agent.

    Args:
        agent_client: The durable agent client to use.
    """
    agent = agent_client.get_agent("Historian")
    session = agent.create_session()

    print("Running a multi-turn conversation...\n")

    for turn in FILLER_TURNS:
        response = agent.run(turn, session=session)
        print(f"[user]  {turn}")
        print(f"[agent] {response.text}\n")

    fact = f"My project codename is {CODENAME}."
    print(f"[user]  {fact}")
    print(f"[agent] {agent.run(fact, session=session).text}\n")

    question = "What is my project codename? Reply with just the codename."
    answer = agent.run(question, session=session)
    print(f"[user]  {question}")
    print(f"[agent] {answer.text}\n")

    if CODENAME.lower() in answer.text.lower():
        print("Recent context was retained while the conversation stayed compacted.")
    else:
        print("The codename fell outside the retained window.")


def main() -> None:
    """Client entry point."""
    try:
        run_client(get_client())
    except Exception as e:
        logger.exception(f"Error during agent interaction: {e}")


if __name__ == "__main__":
    main()
