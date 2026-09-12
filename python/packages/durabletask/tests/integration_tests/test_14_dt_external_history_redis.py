# Copyright (c) Microsoft. All rights reserved.

"""Integration tests for agents whose history lives in an external store.

A user who deliberately configured their own history provider (Redis here, but Cosmos DB or a
file behaves the same) must get the same behavior under the durable runtime as in core:

- the provider is not swapped out for durable-backed history,
- it participates in the run and its stored history reaches the model on later turns,
- it is handed the entity's stable session id, so its keys line up across turns,
- fresh durable state has no local transcript mirror or metadata-only exchange envelopes,
- responses and completion evidence are stored separately, keyed by correlation id.

The stable session id matters because the entity builds a fresh session per operation, and if
that session carried a generated id an externally keyed store would silently start over every turn.
This sample uses blind Redis appends. These tests do not assert exactly-once external writes across
an interrupted operation or portable reset support.
"""

import json
import os
from datetime import datetime
from typing import Any, Protocol

import pytest
import redis.asyncio as aioredis

from agent_framework_durabletask import DurableAgentState, DurableAIAgentClient, serialize_agent_response


class AgentClientFactoryProtocol(Protocol):
    """Protocol for the agent client factory fixture."""

    @classmethod
    def create(cls, max_poll_retries: int = 90) -> tuple[Any, DurableAIAgentClient]: ...


pytestmark = [
    pytest.mark.flaky,
    pytest.mark.integration,
    pytest.mark.sample("14_external_history_redis"),
    pytest.mark.integration_test,
    pytest.mark.requires_foundry,
    pytest.mark.requires_dts,
    pytest.mark.requires_redis,
]

# Matches redis_history_provider.py in the sample.
KEY_PREFIX = "durable_sample:history"


class TestExternalHistoryProvider:
    """An external history provider works durably with no durable-specific configuration."""

    @pytest.fixture(autouse=True)
    def setup(self, agent_client_factory: type[AgentClientFactoryProtocol]) -> None:
        """Setup test fixtures."""
        self.dts_client, self.agent_client = agent_client_factory.create()
        self.redis_url = os.environ.get("REDIS_CONNECTION_STRING", "redis://localhost:6379")

    async def _history_entries(self, session_id: Any) -> list[str]:
        """Read the raw history entries the sample's provider wrote for a session.

        The provider keys on the core session id, which qualifies the entity key with the entity
        name so that agent nodes sharing a key in a workflow run stay separate. The exact name
        casing is the runtime's, so the key is discovered rather than reconstructed.

        Args:
            session_id: The durable session id used for the conversation.

        Returns:
            The serialized messages stored in Redis, oldest first.
        """
        client = aioredis.from_url(self.redis_url, decode_responses=True)
        try:
            matches: Any = await client.keys(f"{KEY_PREFIX}:*{session_id.key}")  # type: ignore[misc]
            keys = [k if isinstance(k, str) else k.decode() for k in matches]
            assert len(keys) <= 1, f"the conversation was scattered across keys: {keys}"
            if not keys:
                return []
            # The client is configured with decode_responses, so entries come back as strings.
            # Coerce anyway, since redis-py types lrange as bytes or str depending on version.
            entries: Any = await client.lrange(keys[0], 0, -1)  # type: ignore[misc]
            return [entry if isinstance(entry, str) else entry.decode() for entry in entries]
        finally:
            await client.aclose()

    def test_agent_registration(self) -> None:
        """The externally backed agent is registered like any other agent."""
        agent = self.agent_client.get_agent("Archivist")
        assert agent is not None
        assert agent.name == "Archivist"

    def test_history_from_the_external_store_reaches_the_model(self) -> None:
        """Nothing else could supply the earlier turn, so recall proves the provider ran."""
        agent = self.agent_client.get_agent("Archivist")
        session = agent.create_session()

        assert agent.run("My library card number is 4417.", session=session) is not None
        answer = agent.run("What is my library card number? Reply with just the number.", session=session)

        assert answer is not None
        assert "4417" in answer.text

    async def test_provider_is_keyed_by_the_stable_session_id(self) -> None:
        """All turns must land under one key; a per-operation id would scatter them."""
        agent = self.agent_client.get_agent("Archivist")
        session = agent.create_session()

        assert agent.run("Remember that my favorite number is 12.", session=session) is not None
        assert agent.run("Remember that my favorite color is teal.", session=session) is not None

        entries = await self._history_entries(session.durable_session_id)

        # Two turns, each storing its input and the model's reply, all under the entity's own id.
        assert len(entries) >= 4, f"expected the whole conversation under one key, found {len(entries)}"
        assert any("12" in entry for entry in entries)
        assert any("teal" in entry for entry in entries)

    def test_external_history_has_no_local_transcript_but_keeps_correlated_delivery(self) -> None:
        """Fresh external history needs no local transcript to deliver completed results."""
        agent = self.agent_client.get_agent("Archivist")
        session = agent.create_session()
        completed: set[str] = set()

        for prompt in ("Note that the archive opens at nine.", "Name a weekday."):
            response = agent.run(prompt, session=session)
            assert response.text
            assert all(content.type != "error" for message in response.messages for content in message.contents)
            expected = json.loads(json.dumps(serialize_agent_response(response)))
            assert expected["created_at"], "the Foundry result timestamp was lost"

            state = self._read_state(session.durable_session_id)
            assert state.data.conversation_history == [], "external history must not create a local transcript mirror"
            assert state.message_count == 0, "transcript count is not a delivery or completion count"

            # Discover the new correlation from completion state, never from transcript entries.
            correlations = set(state.data.completed_correlations)
            assert completed <= correlations, "earlier completion receipts were lost"
            new_correlations = correlations - completed
            assert len(new_correlations) == 1
            correlation_id = new_correlations.pop()
            assert correlation_id
            completed = correlations

            # Check this turn immediately rather than assuming older payloads remain within
            # their delivery window after another model call. Receipts outlive those payloads.
            assert correlation_id in state.data.response_mailbox
            assert set(state.data.response_mailbox) <= completed
            mailbox = state.data.response_mailbox[correlation_id]
            assert mailbox["response"] == expected
            assert mailbox["response"]["created_at"] == expected["created_at"]
            assert len(mailbox["response"]["messages"]) == len(response.messages)
            assert state.data.completed_correlations[correlation_id]["completedAt"] == mailbox["createdAt"]
            assert datetime.fromisoformat(mailbox["expiresAt"]) > datetime.fromisoformat(mailbox["createdAt"])

            delivered = state.try_get_agent_response(correlation_id)
            assert delivered is not None
            assert json.loads(json.dumps(serialize_agent_response(delivered))) == expected

        assert len(completed) == 2, "two completed turns must not require two local transcript exchanges"

    def _read_state(self, session_id: Any) -> DurableAgentState:
        """Load the agent entity's persisted state straight from the scheduler.

        Args:
            session_id: The durable session id used for the conversation.

        Returns:
            The deserialized durable agent state.
        """
        from durabletask.entities import EntityInstanceId

        entity_id = EntityInstanceId(entity=session_id.entity_name, key=session_id.key)
        metadata = self.dts_client.get_entity(entity_id)
        assert metadata is not None, f"no durable state found for {entity_id}"

        raw = metadata.get_state()
        # The scheduler returns the entity payload as serialized JSON.
        if isinstance(raw, str):
            return DurableAgentState.from_json(raw)
        assert isinstance(raw, dict), f"unexpected entity state payload: {type(raw)}"
        return DurableAgentState.from_dict(raw)
