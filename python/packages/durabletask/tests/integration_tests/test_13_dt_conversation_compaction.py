# Copyright (c) Microsoft. All rights reserved.

"""Integration tests for compaction of client-owned durable history.

Covers the sample's ``store=False`` agent with input/output storage enabled and explicit
``retention="keep_all", max_state_bytes=None``:

- provider-selected history reaches the model on later turns,
- compaction annotations and message identities survive entity state serialization,
- excluded local history is retained without coupling delivery to transcript entries,
- original response payloads live in the correlation-keyed mailbox with completion receipts.

This is not a bounded-capacity stress test. Live mailbox payloads and completion receipts still
consume state, and no delivery window is shortened to make the sample fit a small budget.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

import pytest
from durabletask.entities import EntityInstanceId

from agent_framework_durabletask import (
    DurableAgentState,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
    DurableAIAgentClient,
    serialize_agent_response,
)

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
    """Local provider history compacts without changing the sample's keep-all policy."""

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

    def test_persisted_state_matches_the_shared_schema(self) -> None:
        """Real scheduler round-tripped state must satisfy the cross-language contract.

        Unit tests validate a synthetic dict. This validates what the entity actually wrote and
        the scheduler actually stored, which is where drift between the two would show up.
        """
        jsonschema = pytest.importorskip("jsonschema")
        schema_path = Path(__file__).resolve().parents[5] / "schemas" / "durable-agent-entity-state.json"
        schema = json.loads(schema_path.read_text(encoding="utf-8"))

        agent = self.agent_client.get_agent("Historian")
        session = agent.create_session()
        assert agent.run("Name a city.", session=session) is not None
        # A second turn exercises loading persisted ids and annotations as well as assigning
        # identities to new messages in the provider's append hooks.
        assert agent.run("Name another.", session=session) is not None

        state = self._read_state(session.durable_session_id)
        jsonschema.Draft202012Validator(schema).validate(state.to_dict())

        # The fields compaction depends on must actually be present, not merely permitted.
        stored = [m for entry in state.data.conversation_history for m in entry.messages]
        assert any(m.message_id for m in stored), "no message carried an id through real storage"

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

        The strategy still runs each turn. Persisted message metadata and ids let it operate on
        the annotated history rather than losing prior exclusions across entity operations.
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

        # Provider appends assign identities without changing caller messages. Compaction
        # reconciles by those ids, including the newest turn, not by transcript position.
        assert all(m.message_id for m in stored), "stored messages must carry stable message ids"
        assert len({m.message_id for m in stored}) == len(stored), "stored message ids must be unique"

    def test_local_provider_retains_selected_inputs_and_outputs_with_keep_all(self) -> None:
        """This local provider stores both sides; compaction alone does not delete them."""
        agent = self.agent_client.get_agent("Historian")
        session = agent.create_session()

        turns = KEEP_LAST_GROUPS + 3
        prompts = [f"Name city number {index + 1}." for index in range(turns)]
        replies = [agent.run(prompt, session=session) for prompt in prompts]
        assert all(reply.text for reply in replies)
        assert all(
            content.type != "error" for reply in replies for message in reply.messages for content in message.contents
        )

        state = self._read_state(session.durable_session_id)

        # These counts follow this sample's local provider flags and single-call, tool-free turns.
        # They are not a delivery invariant for external or service-managed history.
        requests = [entry for entry in state.data.conversation_history if isinstance(entry, DurableAgentStateRequest)]
        responses = [entry for entry in state.data.conversation_history if isinstance(entry, DurableAgentStateResponse)]
        assert len(requests) == len(responses) == turns
        assert [message.text for entry in requests for message in entry.messages] == prompts
        assert [[message.text for message in entry.messages] for entry in responses] == [
            [message.text for message in reply.messages] for reply in replies
        ]
        assert len(state.data.completed_correlations) == turns
        assert state.data.truncation is None

    def test_mailbox_delivers_original_response_without_transcript_entries(self) -> None:
        """A real stored result remains readable without reconstructing a transcript response."""
        agent = self.agent_client.get_agent("Historian")
        session = agent.create_session()
        response = agent.run("Name a river.", session=session)
        assert response.text
        assert all(content.type != "error" for message in response.messages for content in message.contents)
        expected = json.loads(json.dumps(serialize_agent_response(response)))
        assert expected["created_at"], "the Foundry result timestamp was lost"

        state = self._read_state(session.durable_session_id)
        assert len(state.data.completed_correlations) == 1
        correlation_id = next(iter(state.data.completed_correlations))
        assert correlation_id
        assert set(state.data.response_mailbox) == {correlation_id}
        mailbox = state.data.response_mailbox[correlation_id]
        assert mailbox["response"] == expected
        # The result's date and message count are not the request's date or the transcript count.
        assert mailbox["response"]["created_at"] == expected["created_at"]
        assert len(mailbox["response"]["messages"]) == len(response.messages)
        assert state.data.completed_correlations[correlation_id]["completedAt"] == mailbox["createdAt"]
        assert datetime.fromisoformat(mailbox["expiresAt"]) > datetime.fromisoformat(mailbox["createdAt"])

        # Mutate only this detached read, not scheduler state. Version 2 lookup must still use
        # the mailbox even when no local transcript entry can provide an answer.
        assert state.data.conversation_history
        state.data.conversation_history.clear()
        restored = DurableAgentState.from_json(state.to_json())
        assert restored.message_count == 0
        delivered = restored.try_get_agent_response(correlation_id)
        assert delivered is not None
        assert json.loads(json.dumps(serialize_agent_response(delivered))) == expected
