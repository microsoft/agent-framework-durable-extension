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

import uuid

import pytest
from agent_framework_durabletask import SESSION_ID_HEADER

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
        # Agent can return 200 (immediate) or 202 (async with wait_for_response=false)
        assert response.status_code in [200, 202]
        data = response.json()

        if response.status_code == 200:
            # Synchronous response - check result directly
            assert data["status"] == "success"
            assert data["response"].strip()
            assert data["agent_response"]["messages"]
            # This is local transcript size, not the service-owned conversation size.
            assert type(data["message_count"]) is int and data["message_count"] >= 0
        else:
            # Async response - check we got correlation info
            assert "correlation_id" in data or "session_id" in data

    def test_simple_message_plain_text(self) -> None:
        """Test sending a message with plain text payload."""
        response = self.helper.post_text(f"{self.base_url}/run", "Tell me a short joke about networking.")
        assert response.status_code in [200, 202]

        # Agent responded with plain text when the request body was text/plain.
        assert response.text.strip()
        assert response.headers.get(SESSION_ID_HEADER) is not None

    def test_session_id_in_query(self) -> None:
        """Test using session_id in query parameter."""
        response = self.helper.post_text(
            f"{self.base_url}/run?session_id=test-query-session", "Tell me a short joke about weather in Texas."
        )
        assert response.status_code in [200, 202]

        assert response.text.strip()
        assert response.headers.get(SESSION_ID_HEADER) == "test-query-session"

    def test_legacy_thread_id_in_query_still_accepted(self) -> None:
        """The deprecated thread_id query parameter is still honored on incoming requests."""
        response = self.helper.post_text(
            f"{self.base_url}/run?thread_id=test-legacy-query", "Tell me a short joke about weather in Texas."
        )
        assert response.status_code in [200, 202]

        assert response.text.strip()
        assert response.headers.get(SESSION_ID_HEADER) == "test-legacy-query"
        # The deprecated response header is no longer emitted.
        assert response.headers.get("x-ms-thread-id") is None

    def test_conversation_continuity(self) -> None:
        """Verify remembered content, without assuming client-owned transcript storage."""
        session_id = f"test-continuity-{uuid.uuid4().hex[:8]}"

        response1 = self.helper.post_json(
            f"{self.base_url}/run",
            {
                "message": "My project codename is BLUEHERON. Tell me a short joke that mentions my project codename.",
                "session_id": session_id,
                "wait_for_response": True,
            },
        )
        assert response1.status_code == 200, response1.text
        data1 = response1.json()
        assert data1["status"] == "success" and data1["session_id"] == session_id
        assert data1["response"].strip() and data1["agent_response"]["messages"]

        response2 = self.helper.post_json(
            f"{self.base_url}/run",
            {
                "message": "What is my project codename? Include the exact codename in a short joke.",
                "session_id": session_id,
                "wait_for_response": True,
            },
        )
        assert response2.status_code == 200, response2.text
        data2 = response2.json()
        assert data2["status"] == "success" and data2["session_id"] == session_id
        assert data2["correlation_id"] != data1["correlation_id"]
        assert "blueheron" in data2["response"].lower()
        assert data2["agent_response"]["messages"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
