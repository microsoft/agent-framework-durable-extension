# Copyright (c) Microsoft. All rights reserved.
"""
Integration Tests for the Conversation Compaction Sample

Verifies that an agent configured the ordinary core way - an in-memory history provider plus a
compaction provider - runs durably under the Azure Functions host with no durable-specific
configuration, mirroring the standalone durabletask coverage.

The function app is automatically started by the test fixture.

Prerequisites:
- Azure OpenAI credentials configured (see packages/azurefunctions/tests/integration_tests/.env.example)
- Azurite or Azure Storage account configured

Usage:
    uv run pytest packages/azurefunctions/tests/integration_tests/test_14_conversation_compaction.py -v
"""

import uuid

import pytest

# Matches function_app.py: only the most recent groups stay in the model's context.
KEEP_LAST_GROUPS = 4

# Module-level markers - applied to all tests in this file
pytestmark = [
    pytest.mark.flaky,
    pytest.mark.integration,
    pytest.mark.sample("14_conversation_compaction"),
    pytest.mark.usefixtures("function_app_for_test"),
]


class TestSampleConversationCompaction:
    """Tests for 14_conversation_compaction sample."""

    @pytest.fixture(autouse=True)
    def _setup(self, base_url: str, sample_helper) -> None:
        """Provide agent-specific base URL and helper for the tests."""
        self.base_url = f"{base_url}/api/agents/Historian"
        self.helper = sample_helper

    def _run(self, message: str, session_id: str) -> dict:
        """Send one turn to the agent and return the parsed response.

        Args:
            message: The user message for this turn.
            session_id: The session id tying the turns into one conversation.

        Returns:
            The parsed JSON response body.
        """
        response = self.helper.post_json(f"{self.base_url}/run", {"message": message, "session_id": session_id})
        assert response.status_code in [200, 202]
        return response.json()

    def test_health_check(self, base_url: str, sample_helper) -> None:
        """Test health check endpoint."""
        response = sample_helper.get(f"{base_url}/api/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"

    def test_recent_context_survives_compaction(self) -> None:
        """A fact inside the retained window is still answerable after the window fills."""
        session_id = f"compaction-recent-{uuid.uuid4().hex[:8]}"

        for index in range(KEEP_LAST_GROUPS):
            self._run(f"Name animal number {index + 1}.", session_id)

        self._run("My project codename is BLUEHERON.", session_id)
        answer = self._run("What is my project codename? Reply with just the codename.", session_id)

        assert "blueheron" in str(answer["response"]).lower()

    def test_conversation_continues_across_turns(self) -> None:
        """Durable history reaches the model, so the agent recalls an earlier turn."""
        session_id = f"compaction-continuity-{uuid.uuid4().hex[:8]}"

        self._run("My favorite animal is the axolotl.", session_id)
        answer = self._run("What is my favorite animal? Reply with just the animal name.", session_id)

        assert "axolotl" in str(answer["response"]).lower()
