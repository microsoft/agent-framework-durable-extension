# Copyright (c) Microsoft. All rights reserved.
"""
Integration Tests for Single Agent Sample

Tests the single agent sample with various message formats and session management.

The function app is automatically started by the test fixture.

Prerequisites:
- Azure OpenAI credentials configured (see packages/azurefunctions/tests/integration_tests/.env.example)
- Azurite or Azure Storage account configured

Usage:
    uv run pytest packages/azurefunctions/tests/integration_tests/test_01_single_agent.py -v
"""

import json

import pytest
from agent_framework import AgentResponse
from agent_framework_durabletask import SESSION_ID_HEADER, serialize_agent_response

# Module-level markers - applied to all tests in this file
pytestmark = [
    pytest.mark.flaky,
    pytest.mark.integration,
    pytest.mark.sample("01_single_agent"),
    pytest.mark.usefixtures("function_app_for_test"),
]


class TestSampleSingleAgent:
    """Tests for 01_single_agent sample."""

    @pytest.fixture(autouse=True)
    def _setup(self, base_url: str, sample_helper) -> None:
        """Provide agent-specific base URL and helper for the tests."""
        self.base_url = f"{base_url}/api/agents/Joker"
        self.helper = sample_helper

    def _assert_success_response(self, response, session_id: str) -> dict:
        """Check actual response delivery independently of local transcript storage."""
        assert response.status_code == 200, response.text
        data = response.json()
        assert data["status"] == "success", data
        assert data["session_id"] == session_id
        assert data["correlation_id"]
        assert data["response"].strip()

        # The legacy field counts local transcript entries, not completed executions.
        # Sample 01 uses Foundry's default service-managed history.
        assert data["message_count"] == 0

        snapshot = data["agent_response"]
        assert snapshot["type"] == "agent_response"
        assert snapshot["created_at"], "the Foundry result timestamp was lost"
        delivered = AgentResponse.from_dict(snapshot)
        assert delivered.messages
        assert delivered.text == data["response"]
        assert all(content.type != "error" for message in delivered.messages for content in message.contents)
        assert json.loads(json.dumps(serialize_agent_response(delivered))) == snapshot
        return data

    def test_health_check(self, base_url: str, sample_helper) -> None:
        """Test health check endpoint."""
        response = sample_helper.get(f"{base_url}/api/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"

    def test_simple_message_json(self) -> None:
        """Test sending a simple message with JSON payload."""
        response = self.helper.post_json(
            f"{self.base_url}/run",
            {"message": "Tell me a short joke about cloud computing.", "session_id": "test-simple-json"},
        )
        self._assert_success_response(response, "test-simple-json")

    def test_simple_message_plain_text(self) -> None:
        """Test sending a message with plain text payload."""
        response = self.helper.post_text(f"{self.base_url}/run", "Tell me a short joke about networking.")
        assert response.status_code == 200, response.text

        # Agent responded with plain text when the request body was text/plain.
        assert response.text.strip()
        assert response.headers.get(SESSION_ID_HEADER) is not None

    def test_session_id_in_query(self) -> None:
        """Test using session_id in query parameter."""
        response = self.helper.post_text(
            f"{self.base_url}/run?session_id=test-query-session", "Tell me a short joke about weather in Texas."
        )
        assert response.status_code == 200, response.text

        assert response.text.strip()
        assert response.headers.get(SESSION_ID_HEADER) == "test-query-session"

    def test_legacy_thread_id_in_query_still_accepted(self) -> None:
        """The deprecated thread_id query parameter is still honored on incoming requests."""
        response = self.helper.post_text(
            f"{self.base_url}/run?thread_id=test-legacy-query", "Tell me a short joke about weather in Texas."
        )
        assert response.status_code == 200, response.text

        assert response.text.strip()
        assert response.headers.get(SESSION_ID_HEADER) == "test-legacy-query"
        # The deprecated response header is no longer emitted.
        assert response.headers.get("x-ms-thread-id") is None

    def test_conversation_continuity(self) -> None:
        """Service-managed context must reach the model without a local transcript."""
        session_id = "test-continuity"

        # First message establishes a fact that exists nowhere else.
        response1 = self.helper.post_json(
            f"{self.base_url}/run",
            {"message": "My favorite animal is the axolotl. Tell me a short joke about it.", "session_id": session_id},
        )
        data1 = self._assert_success_response(response1, session_id)

        # The follow-up needs the same session's service-managed context.
        response2 = self.helper.post_json(
            f"{self.base_url}/run",
            {"message": "What is my favorite animal? Reply with just the animal name.", "session_id": session_id},
        )
        data2 = self._assert_success_response(response2, session_id)
        assert data2["correlation_id"] != data1["correlation_id"]
        assert "axolotl" in data2["response"].lower(), (
            f"Agent lost conversation context across turns. Got: {data2['response']!r}"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
