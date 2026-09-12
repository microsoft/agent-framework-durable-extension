# Copyright (c) Microsoft. All rights reserved.

"""Integration tests for single agent functionality.

Tests basic agent operations including:
- Agent registration and retrieval
- Single agent interactions
- Conversation continuity across multiple messages
- Concurrent agent usage
- Empty session ID handling
"""

from typing import Any, Protocol

import pytest

from agent_framework_durabletask import DurableAIAgentClient


class AgentClientFactoryProtocol(Protocol):
    """Protocol for the agent client factory fixture."""

    @classmethod
    def create(cls, max_poll_retries: int = 90) -> tuple[Any, DurableAIAgentClient]: ...


# Module-level markers - applied to all tests in this module
pytestmark = [
    pytest.mark.flaky,
    pytest.mark.integration,
    pytest.mark.sample("01_single_agent"),
    pytest.mark.integration_test,
    pytest.mark.requires_foundry,
    pytest.mark.requires_dts,
]


class TestSingleAgent:
    """Test suite for single agent functionality."""

    @pytest.fixture(autouse=True)
    def setup(self, agent_client_factory: type[AgentClientFactoryProtocol]) -> None:
        """Setup test fixtures."""
        # Create agent client using the factory fixture
        _, self.agent_client = agent_client_factory.create()

    def test_agent_registration(self) -> None:
        """Test that the Joker agent is registered and accessible."""
        agent = self.agent_client.get_agent("Joker")
        assert agent is not None
        assert agent.name == "Joker"

    def test_single_interaction(self):
        """Test a single interaction with the agent."""
        agent = self.agent_client.get_agent("Joker")
        session = agent.create_session()

        response = agent.run("Tell me a short joke about programming.", session=session)

        assert response is not None
        assert response.text is not None
        assert len(response.text) > 0

    def test_conversation_continuity(self):
        """Prior turns must reach the model, not just be recorded.

        The second turn is only answerable from persisted history, so this fails if durable
        history is not actually being loaded and delivered to the agent.
        """
        agent = self.agent_client.get_agent("Joker")
        session = agent.create_session()

        # First turn establishes a fact that exists nowhere else.
        response1 = agent.run("My favorite animal is the axolotl. Tell me a joke about it.", session=session)
        assert response1 is not None
        assert len(response1.text) > 0

        # Second turn can only be answered from the conversation history.
        response2 = agent.run("What is my favorite animal? Reply with just the animal name.", session=session)
        assert response2 is not None
        assert "axolotl" in response2.text.lower(), (
            f"Agent lost conversation context across turns. Got: {response2.text!r}"
        )

    def test_multiple_sessions(self):
        """Test that different sessions maintain separate contexts."""
        agent = self.agent_client.get_agent("Joker")

        # Create two separate sessions
        session1 = agent.create_session()
        session2 = agent.create_session()

        assert session1.durable_session_id != session2.durable_session_id

        # Send different messages to each session
        response1 = agent.run("Tell me a joke about dogs.", session=session1)
        response2 = agent.run("Tell me a joke about birds.", session=session2)

        assert response1 is not None
        assert response2 is not None
        assert response1.text != response2.text
