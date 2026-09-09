# Copyright (c) Microsoft. All rights reserved.
"""
Integration Tests for the Conversation Compaction Sample

Verifies that an agent configured the ordinary core way - an in-memory history provider plus a
compaction provider - runs durably under the Azure Functions host with no durable-specific
agent configuration. The sample explicitly uses ``store=False``, ``retention="keep_all"`` and
``max_state_bytes=None``. Transcript counts below apply only to its local input/output provider,
not to external or service-managed history. Completed HTTP results carry the original response
payload separately from the local transcript count.

The function app is automatically started by the test fixture.

Prerequisites:
- FOUNDRY_PROJECT_ENDPOINT and FOUNDRY_MODEL configured, with Azure CLI authentication
- Azure Functions Core Tools, Durable Task Scheduler, and Azurite or Azure Storage configured

Usage:
    uv run pytest packages/azurefunctions/tests/integration_tests/test_14_conversation_compaction.py -v
"""

import json
import uuid

import pytest
from agent_framework import AgentResponse
from agent_framework_durabletask import serialize_agent_response

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
        response = self.helper.post_json(
            f"{self.base_url}/run",
            {"message": message, "session_id": session_id, "wait_for_response": True},
        )
        assert response.status_code == 200, response.text
        result = response.json()
        assert result["status"] == "success", result
        assert result["session_id"] == session_id
        assert result["correlation_id"]

        # A 202 acceptance is not an agent result. Successful polling returns the mailbox
        # snapshot, including original result metadata, not a transcript reconstruction.
        snapshot = result["agent_response"]
        assert snapshot["type"] == "agent_response"
        assert snapshot["created_at"], "the Foundry result timestamp was lost"
        delivered = AgentResponse.from_dict(snapshot)
        assert delivered.text == result["response"]
        assert all(content.type != "error" for message in delivered.messages for content in message.contents)
        assert json.loads(json.dumps(serialize_agent_response(delivered))) == snapshot
        return result

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

    def test_local_transcript_count_is_separate_from_the_original_response_payload(self) -> None:
        """Only this store=False input/output provider has two transcript entries per turn."""
        session_id = f"compaction-delivery-{uuid.uuid4().hex[:8]}"
        correlations: set[str] = set()

        for turn, prompt in enumerate(("Name a river.", "Name an ocean."), start=1):
            result = self._run(prompt, session_id)
            assert result["correlation_id"] not in correlations
            correlations.add(result["correlation_id"])
            assert result["message_count"] == turn * 2

            # The original payload's message count and date must round-trip on their own.
            # They are not synthesized from the echoed request or message_count above.
            snapshot = result["agent_response"]
            delivered = AgentResponse.from_dict(snapshot)
            round_tripped = json.loads(json.dumps(serialize_agent_response(delivered)))
            assert len(delivered.messages) == len(snapshot["messages"])
            assert delivered.messages
            assert round_tripped["created_at"] == snapshot["created_at"]
