# Copyright (c) Microsoft. All rights reserved.

"""Unit tests for multi-agent support in AgentFunctionApp."""

from unittest.mock import Mock, patch

import pytest

from agent_framework_azurefunctions import AgentFunctionApp


class TestMultiAgentInit:
    """Test suite for multi-agent initialization."""

    def test_init_with_agents_list(self) -> None:
        """Test initialization with list of agents."""
        agent1 = Mock()
        agent1.name = "Agent1"
        agent2 = Mock()
        agent2.name = "Agent2"

        app = AgentFunctionApp(agents=[agent1, agent2])

        assert len(app.agents) == 2
        assert "Agent1" in app.agents
        assert "Agent2" in app.agents
        assert app.agents["Agent1"] == agent1
        assert app.agents["Agent2"] == agent2

    def test_init_with_empty_agents_list(self) -> None:
        """Test initialization with empty list of agents."""
        app = AgentFunctionApp(agents=[])

        assert len(app.agents) == 0

    def test_init_with_no_agents(self) -> None:
        """Test initialization without any agents."""
        app = AgentFunctionApp()

        assert len(app.agents) == 0

    def test_init_with_duplicate_agent_names(self) -> None:
        """Different agents with the same name fail before any registration."""
        agent1 = Mock()
        agent1.name = "TestAgent"
        agent2 = Mock()
        agent2.name = "TestAgent"

        with (
            patch.object(AgentFunctionApp, "_setup_agent_functions") as setup,
            pytest.raises(ValueError, match="collides"),
        ):
            AgentFunctionApp(agents=[agent1, agent2])
        setup.assert_not_called()

    def test_init_with_agent_without_name(self) -> None:
        """Test initialization with agent missing name attribute raises error."""
        agent1 = Mock()
        agent1.name = "Agent1"
        agent2 = Mock(spec=[])  # Mock without name attribute

        with pytest.raises(ValueError, match="Agent must have a name"):
            AgentFunctionApp(agents=[agent1, agent2])


class TestAddAgentMethod:
    """Test suite for add_agent() method."""

    def test_add_agent_to_empty_app(self) -> None:
        """Test adding agent to app initialized without agents."""
        app = AgentFunctionApp()

        agent = Mock()
        agent.name = "NewAgent"

        app.add_agent(agent)

        assert len(app.agents) == 1
        assert "NewAgent" in app.agents
        assert app.agents["NewAgent"] == agent

    def test_add_multiple_agents(self) -> None:
        """Test adding multiple agents sequentially."""
        app = AgentFunctionApp()

        agent1 = Mock()
        agent1.name = "Agent1"
        agent2 = Mock()
        agent2.name = "Agent2"

        app.add_agent(agent1)
        app.add_agent(agent2)

        assert len(app.agents) == 2
        assert "Agent1" in app.agents
        assert "Agent2" in app.agents

    def test_add_agent_with_duplicate_name_raises(self) -> None:
        """A different agent cannot replace or reuse an existing registration."""
        agent1 = Mock()
        agent1.name = "MyAgent"
        agent2 = Mock()
        agent2.name = "MyAgent"

        app = AgentFunctionApp(agents=[agent1])

        with (
            patch.object(app, "_setup_agent_functions") as setup,
            pytest.raises(ValueError, match="collides"),
        ):
            app.add_agent(agent2)

        setup.assert_not_called()
        assert app.agents == {"MyAgent": agent1}

    def test_add_agent_to_app_with_existing_agents(self) -> None:
        """Test adding agent to app that already has agents."""
        agent1 = Mock()
        agent1.name = "Agent1"
        agent2 = Mock()
        agent2.name = "Agent2"

        app = AgentFunctionApp(agents=[agent1])
        app.add_agent(agent2)

        assert len(app.agents) == 2
        assert "Agent1" in app.agents
        assert "Agent2" in app.agents

    def test_add_agent_without_name_raises_error(self) -> None:
        """Test that adding agent without name attribute raises error."""
        app = AgentFunctionApp()

        agent = Mock(spec=[])  # Mock without name attribute

        with pytest.raises(ValueError, match="does not have a 'name' attribute"):
            app.add_agent(agent)


class TestHealthCheckWithMultipleAgents:
    """Test suite for health check with multiple agents."""

    def test_health_check_returns_all_agents(self) -> None:
        """Test that health check returns information about all agents."""
        agent1 = Mock()
        agent1.name = "Agent1"
        agent2 = Mock()
        agent2.name = "Agent2"

        app = AgentFunctionApp(agents=[agent1, agent2])

        # Note: We can't easily test the actual health check endpoint without running the app
        # But we can verify the agents dictionary is properly populated
        assert len(app.agents) == 2
        assert app.enable_health_check is True


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
