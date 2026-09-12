# Copyright (c) Microsoft. All rights reserved.

"""External History (Redis) Sample - Durable Task Integration (Combined Worker + Client)

Runs both the worker and client in a single process. The worker is started first to register
the Redis-backed agent, then the client drives a multi-turn conversation.

Prerequisites:
- Set FOUNDRY_PROJECT_ENDPOINT and FOUNDRY_MODEL
- Sign in with Azure CLI for AzureCliCredential authentication
- Durable Task Scheduler and Redis must be running (e.g., using Docker)

To run this sample:
    python sample.py
"""

import logging

from client import get_client, run_client  # pyrefly: ignore[missing-import]
from dotenv import load_dotenv
from worker import get_worker, setup_worker  # pyrefly: ignore[missing-import]

# Configure logging (must be after imports to override their basicConfig)
logging.basicConfig(level=logging.INFO, force=True)
logger = logging.getLogger(__name__)


def main():
    """Main entry point - runs both worker and client in single process."""
    silent_handler = logging.NullHandler()

    dts_worker = get_worker(log_handler=silent_handler)
    with dts_worker:
        setup_worker(dts_worker)
        dts_worker.start()
        logger.debug("Worker started and listening for requests...")

        agent_client = get_client(log_handler=silent_handler)
        try:
            run_client(agent_client)
        except Exception as e:
            logger.exception(f"Error during agent interaction: {e}")

        logger.debug("Sample completed. Worker shutting down...")


if __name__ == "__main__":
    load_dotenv()
    main()
