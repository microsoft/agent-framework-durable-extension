# Copyright (c) Microsoft. All rights reserved.

"""Integration tests for durable conversation compaction.

Covers the behavior an agent gets by simply being registered with the durable runtime:

- history is persisted in the agent's durable entity and reaches the model on later turns,
- the configured compaction strategy runs and its annotations are persisted, so compaction
  state survives entity state serialization rather than being recomputed each turn,
- the full conversation record is retained in storage even though the model sees less.
"""

from typing import Any, Protocol

import pytest
from durabletask.entities import EntityInstanceId

from agent_framework_durabletask import DurableAgentState, DurableAIAgentClient

# Matches worker.py: only the most recent groups stay in the model's context.
KEEP_LAST_GROUPS = 4


class AgentClientFactoryProtocol(Protocol):
    """Protocol for the agent client factory fixture."""

    @classmethod
    def create(cls, max_poll_retries: int = 90) -> tuple[Any, DurableAIAgentClient]: ...


pytestmark = [
    pytest.mark.flaky,
    pytest.mark.integration,
    pytest.mark.sample("13_conversation_compaction"),
    pytest.mark.integration_test,
    pytest.mark.requires_foundry,
    pytest.mark.requires_dts,
]


class TestConversationCompaction:
    """Compaction runs durably without any durable-specific agent configuration."""

    @pytest.fixture(autouse=True)
    def setup(self, agent_client_factory: type[AgentClientFactoryProtocol]) -> None:
        """Setup test fixtures."""
        self.dts_client, self.agent_client = agent_client_factory.create()

    def _read_state(self, session_id: Any) -> DurableAgentState:
        """Load the agent entity's persisted state straight from the scheduler."""
        entity_id = EntityInstanceId(entity=session_id.entity_name, key=session_id.key)
        metadata = self.dts_client.get_entity(entity_id)
        assert metadata is not None, f"no durable state found for {entity_id}"

        raw = metadata.get_state()
        # The scheduler returns the entity payload as serialized JSON.
        if isinstance(raw, str):
            return DurableAgentState.from_json(raw)
        assert isinstance(raw, dict), f"unexpected entity state payload: {type(raw)}"
        return DurableAgentState.from_dict(raw)

    def test_agent_registration(self) -> None:
        """The compacting agent is registered like any other agent."""
        agent = self.agent_client.get_agent("Historian")
        assert agent is not None
        assert agent.name == "Historian"

    def test_session_is_persisted_and_scoped(self) -> None:
        """The serialized session survives real entity storage with the right shape.

        Unit tests keep the session dict in memory, so they cannot show that the blob survives the
        entity's JSON encoding, that it carries the entity's **own** session id, or that the durable
        history provider's slice really is kept out of it.
        """
        agent = self.agent_client.get_agent("Historian")
        session = agent.create_session()

        assert agent.run("Name a color.", session=session) is not None
        assert agent.run("Name a fruit.", session=session) is not None

        stored = self._read_state(session.durable_session_id).data.session
        assert stored is not None, "the session was not persisted"

        # The entity's own identity rather than a per-operation id. External history providers key
        # their storage on this, so a generated id would restart their conversation every turn.
        # It carries the entity name as well as the key, because agent nodes in one workflow run
        # share a key and would otherwise all resolve to the same conversation.
        assert session.durable_session_id is not None
        key = session.durable_session_id.key
        assert stored["session_id"].endswith(f"@{key}"), (
            f"expected the session id to end with the entity key {key}, got {stored['session_id']}"
        )
        # The runtime lowercases entity names, so compare that way.
        entity_name = session.durable_session_id.entity_name.lower()
        assert entity_name in stored["session_id"].lower(), (
            f"expected the entity name in the session id, got {stored['session_id']}"
        )

        slices = stored["state"]
        # The compaction provider's own slice is carried across turns...
        assert "compaction" in slices, f"expected provider state to be persisted, got {slices}"
        # ...but the durable history provider's is not, since it is derived from
        # conversationHistory and would otherwise duplicate the transcript. "in_memory" is the
        # source_id the sample's provider keeps after the durable swap.
        assert "in_memory" not in slices, f"durable history slice leaked into the session: {slices}"

    def test_recent_context_survives_compaction(self) -> None:
        """A fact inside the retained window is still answerable after several turns."""
        agent = self.agent_client.get_agent("Historian")
        session = agent.create_session()

        for filler in ("Name a color.", "Name a country.", "Name a fruit."):
            assert agent.run(filler, session=session) is not None

        agent.run("My project codename is BLUEHERON.", session=session)
        answer = agent.run("What is my project codename? Reply with just the codename.", session=session)

        assert "blueheron" in answer.text.lower(), (
            f"Recent context was lost despite being inside the retained window. Got: {answer.text!r}"
        )

    def test_compaction_annotations_are_persisted(self) -> None:
        """Compaction state must survive durable state serialization.

        This is what stops compaction from being recomputed on every turn, and it only works
        because message-level metadata and ids are persisted with the conversation.
        """
        agent = self.agent_client.get_agent("Historian")
        session = agent.create_session()

        # Run enough turns that the sliding window must exclude earlier ones.
        for index in range(KEEP_LAST_GROUPS + 3):
            assert agent.run(f"Name animal number {index + 1}.", session=session) is not None

        state = self._read_state(session.durable_session_id)

        stored = [message for entry in state.data.conversation_history for message in entry.messages]
        assert stored, "expected the conversation to be persisted"

        # Compaction excluded older messages, and that annotation round-tripped through storage.
        annotated = [m for m in stored if m.extension_data]
        assert annotated, "expected compaction annotations to be persisted in durable state"

        excluded = [m for m in annotated if (m.extension_data or {}).get("_excluded")]
        assert excluded, "expected the sliding window to exclude older messages"

        # Reconciling compaction results across turns relies on stable ids, so every message
        # the provider has processed must carry one. (The newest turn is annotated on the
        # following load, so it is not required to have an id yet.)
        assert all(m.message_id for m in annotated), "annotated messages must carry stable message ids"

    def test_full_record_is_retained(self) -> None:
        """Compaction bounds what the model sees; it does not delete the record by default."""
        agent = self.agent_client.get_agent("Historian")
        session = agent.create_session()

        turns = KEEP_LAST_GROUPS + 3
        for index in range(turns):
            assert agent.run(f"Name city number {index + 1}.", session=session) is not None

        state = self._read_state(session.durable_session_id)

        # One request entry and one response entry per turn: nothing was pruned.
        assert len(state.data.conversation_history) == turns * 2
