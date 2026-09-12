# Copyright (c) Microsoft. All rights reserved.

"""Integration tests for multi-agent orchestration with conditionals.

Tests conditional orchestration patterns:
- Conditional branching in orchestrations
- Agent-based decision making
- Activity function execution
- Structured output handling
- Conditional routing based on agent responses
"""

import logging
from typing import Any, Protocol

import pytest
from durabletask.client import OrchestrationStatus

from agent_framework_durabletask import DurableAIAgentClient


class AgentClientFactoryProtocol(Protocol):
    """Protocol for the agent client factory fixture."""

    @classmethod
    def create(cls, max_poll_retries: int = 90) -> tuple[Any, DurableAIAgentClient]: ...


# Agent names from the 06_multi_agent_orchestration_conditionals sample
SPAM_AGENT_NAME: str = "SpamDetectionAgent"
EMAIL_AGENT_NAME: str = "EmailAssistantAgent"

# Configure logging
logging.basicConfig(level=logging.WARNING)

# Module-level markers
pytestmark = [
    pytest.mark.flaky,
    pytest.mark.integration,
    pytest.mark.sample("06_multi_agent_orchestration_conditionals"),
    pytest.mark.integration_test,
    pytest.mark.requires_dts,
]


class TestMultiAgentOrchestrationConditionals:
    """Test suite for multi-agent orchestration with conditionals."""

    @pytest.fixture(autouse=True)
    def setup(self, agent_client_factory: type[AgentClientFactoryProtocol], orchestration_helper) -> None:
        """Setup test fixtures."""
        # Create agent client using the factory fixture
        self.dts_client, self.agent_client = agent_client_factory.create()
        self.orch_helper = orchestration_helper

    def test_agents_registered(self):
        """Test that both agents are registered."""
        spam_agent = self.agent_client.get_agent(SPAM_AGENT_NAME)
        email_agent = self.agent_client.get_agent(EMAIL_AGENT_NAME)

        assert spam_agent is not None
        assert spam_agent.name == SPAM_AGENT_NAME
        assert email_agent is not None
        assert email_agent.name == EMAIL_AGENT_NAME

    def test_conditional_branching(self) -> None:
        """Spam takes the spam-handler branch and legitimate mail takes the reply branch.

        Asserting only that the orchestration completed would pass even if the condition sent
        every email down the same branch, so each case checks the branch-specific output.
        """
        spam_instance_id = self.dts_client.schedule_new_orchestration(
            orchestrator="spam_detection_orchestration",
            input={
                "email_id": "spam-001",
                "email_content": "Buy cheap medications online! No prescription needed! Limited time offer!",
            },
        )
        spam_metadata, spam_output = self.orch_helper.wait_for_orchestration_with_output(
            instance_id=spam_instance_id,
            timeout=300.0,
        )

        assert spam_metadata.runtime_status == OrchestrationStatus.COMPLETED
        # The spam handler returns "Email marked as spam: ..."; the other branch returns "Email sent: ...".
        assert "marked as spam" in str(spam_output).lower(), f"spam took the wrong branch: {spam_output}"

        legit_instance_id = self.dts_client.schedule_new_orchestration(
            orchestrator="spam_detection_orchestration",
            input={
                "email_id": "legit-001",
                "email_content": (
                    "Hi team, please confirm receipt of purchase order PRJ-4417 for the new lab "
                    "hardware, and let me know the expected delivery date."
                ),
            },
        )
        legit_metadata, legit_output = self.orch_helper.wait_for_orchestration_with_output(
            instance_id=legit_instance_id,
            timeout=300.0,
        )

        assert legit_metadata.runtime_status == OrchestrationStatus.COMPLETED
        assert "email sent" in str(legit_output).lower(), f"legitimate mail took the wrong branch: {legit_output}"
