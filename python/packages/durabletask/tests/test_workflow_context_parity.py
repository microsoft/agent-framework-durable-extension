# Copyright (c) Microsoft. All rights reserved.

"""Tests for workflow context parity (ADR-0032 L3).

In-process workflows hand a downstream ``AgentExecutor`` the upstream conversation via
``AgentExecutorResponse.full_conversation``. These tests cover the durable equivalent:
the orchestrator projects that conversation into ``RunRequest.context_messages`` honoring
``context_mode``/``context_filter``, and the entity records it without duplication.
"""

from typing import Any

from agent_framework import (
    AgentExecutor,
    AgentExecutorResponse,
    AgentResponse,
    Message,
)

from agent_framework_durabletask import (
    AgentEntity,
    AgentEntityStateProviderMixin,
    DurableAgentState,
    DurableAgentStateRequest,
    RunRequest,
)
from agent_framework_durabletask._workflows.orchestrator import (
    _build_context_messages,
    build_agent_executor_response,
)


class _StubAgent:
    """Minimal agent stand-in for constructing an AgentExecutor."""

    def __init__(self, name: str = "stub") -> None:
        self.name = name
        self.id = name
        self.description = None

    async def run(self, messages: Any = None, **kwargs: Any) -> AgentResponse:
        return AgentResponse(messages=[Message(role="assistant", contents=["ok"])])

    def create_session(self, **kwargs: Any) -> Any:
        from agent_framework import AgentSession

        return AgentSession()


class _InMemoryStateProvider(AgentEntityStateProviderMixin):
    def __init__(self, *, session_id: str = "wf-session", entity_name: str = "") -> None:
        self._session_id = session_id
        self._entity_name = entity_name
        self._state_dict: dict[str, Any] = {}

    def _get_state_dict(self) -> dict[str, Any]:
        return self._state_dict

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        self._state_dict = state

    def _get_session_id_from_entity(self) -> str:
        return self._session_id

    def _get_entity_name_from_entity(self) -> str:
        return self._entity_name


def _upstream_response(*, texts: list[str], agent_text: str) -> AgentExecutorResponse:
    conversation = [Message(role="user", contents=[t], message_id=f"m{i}") for i, t in enumerate(texts)]
    agent_message = Message(role="assistant", contents=[agent_text], message_id="agent-msg")
    conversation.append(agent_message)
    return AgentExecutorResponse(
        executor_id="upstream",
        agent_response=AgentResponse(messages=[agent_message]),
        full_conversation=conversation,
    )


def _stub_agent() -> Any:
    """Return the stub agent typed loosely.

    It implements the parts of the agent protocol these tests exercise but not its full signature,
    so the type is relaxed here rather than at every call site.
    """
    return _StubAgent()


class TestContextProjection:
    """The orchestrator projects upstream conversation per context_mode."""

    def test_full_mode_forwards_entire_conversation(self) -> None:
        executor = AgentExecutor(_stub_agent(), id="downstream")
        upstream = _upstream_response(texts=["first", "second"], agent_text="reply")

        projected = _build_context_messages(executor, upstream)

        assert projected is not None
        assert len(projected) == 3

    def test_last_agent_mode_forwards_only_agent_messages(self) -> None:
        executor = AgentExecutor(_stub_agent(), id="downstream", context_mode="last_agent")
        upstream = _upstream_response(texts=["first", "second"], agent_text="reply")

        projected = _build_context_messages(executor, upstream)

        assert projected is not None
        assert len(projected) == 1

    def test_custom_mode_uses_context_filter(self) -> None:
        executor = AgentExecutor(
            _stub_agent(),
            id="downstream",
            context_mode="custom",
            context_filter=lambda messages: messages[-2:],
        )
        upstream = _upstream_response(texts=["first", "second"], agent_text="reply")

        projected = _build_context_messages(executor, upstream)

        assert projected is not None
        assert len(projected) == 2

    def test_non_agent_input_has_no_upstream_context(self) -> None:
        """The first node receives raw input, so there is no conversation to forward."""
        executor = AgentExecutor(_stub_agent(), id="downstream")

        assert _build_context_messages(executor, "plain input") is None


class TestEntityContextIngestion:
    """The entity records forwarded context and does not duplicate it."""

    def _request(self, messages: list[Message], correlation_id: str) -> RunRequest:
        return RunRequest(
            message=messages[-1].text or "",
            correlation_id=correlation_id,
            context_messages=[m.to_dict() for m in messages],
        )

    def test_context_messages_become_request_messages(self) -> None:
        messages = [
            Message(role="user", contents=["hello"], message_id="m0"),
            Message(role="assistant", contents=["hi"], message_id="m1"),
        ]

        entry = DurableAgentStateRequest.from_run_request(self._request(messages, "corr-0"))

        assert [m.message_id for m in entry.messages] == ["m0", "m1"]

    def test_repeated_context_is_not_duplicated(self) -> None:
        """A node that runs twice in a cycle must not re-record the same conversation."""
        provider = _InMemoryStateProvider()
        entity = AgentEntity(_stub_agent(), state_provider=provider)

        first = [Message(role="user", contents=["hello"], message_id="m0")]
        entity.state.data.conversation_history.append(
            DurableAgentStateRequest.from_run_request(self._request(first, "corr-0"))
        )

        repeated = [
            Message(role="user", contents=["hello"], message_id="m0"),
            Message(role="assistant", contents=["new"], message_id="m1"),
        ]
        entry = DurableAgentStateRequest.from_run_request(self._request(repeated, "corr-1"))
        entry.messages = entity._drop_already_stored(entry.messages)

        assert [m.message_id for m in entry.messages] == ["m1"]

    def test_fully_duplicate_context_keeps_last_message(self) -> None:
        """The agent must always receive at least one input message.

        The kept copy loses its id, because storing two messages under one id would collide in the
        compaction position map and send annotations or pruning to the wrong stored message.
        """
        provider = _InMemoryStateProvider()
        entity = AgentEntity(_stub_agent(), state_provider=provider)

        messages = [Message(role="user", contents=["hello"], message_id="m0")]
        entity.state.data.conversation_history.append(
            DurableAgentStateRequest.from_run_request(self._request(messages, "corr-0"))
        )

        entry = DurableAgentStateRequest.from_run_request(self._request(messages, "corr-1"))
        entry.messages = entity._drop_already_stored(entry.messages)

        assert len(entry.messages) == 1
        assert entry.messages[0].message_id is None
        assert entry.messages[0].to_chat_message().text == "hello"

    def test_repeated_context_does_not_duplicate_message_ids(self) -> None:
        """A cycle that re-delivers the whole upstream conversation must not collide ids."""
        provider = _InMemoryStateProvider()
        entity = AgentEntity(_stub_agent(), state_provider=provider)

        messages = [Message(role="user", contents=["hello"], message_id="m0")]
        for index in range(3):
            entry = DurableAgentStateRequest.from_run_request(self._request(messages, f"corr-{index}"))
            entry.messages = entity._drop_already_stored(entry.messages)
            entity.state.data.conversation_history.append(entry)

        stored_ids = [
            m.message_id for entry in entity.state.data.conversation_history for m in entry.messages if m.message_id
        ]
        assert len(stored_ids) == len(set(stored_ids)), f"duplicate message ids persisted: {stored_ids}"


class TestWorkflowConversationIdentity:
    """Messages the workflow itself builds must carry ids, or a repeated node cannot spot them.

    Core leaves ``message_id`` unset, and the entity's duplicate check treats a message without one
    as new. An unstamped conversation therefore defeats the check entirely, and a node in a cycle
    re-records the whole conversation on every visit.
    """

    def _cycle_ids(self) -> list[str]:
        conversation: Any = "start"
        for node in ["A", "B", "A", "B"]:
            conversation = build_agent_executor_response(node, f"{node} says", None, conversation)
        return [m.message_id or "" for m in conversation.full_conversation]

    def test_every_built_message_carries_an_id(self) -> None:
        response = build_agent_executor_response("writer", "drafted", None, "start")

        ids = [m.message_id for m in response.full_conversation]
        assert all(ids), f"a message went out without an id: {ids}"

    def test_ids_stay_unique_around_a_cycle(self) -> None:
        ids = self._cycle_ids()

        assert all(ids), f"a message went out without an id: {ids}"
        assert len(ids) == len(set(ids)), f"ids collided around the cycle: {ids}"

    def test_ids_are_replay_stable(self) -> None:
        """The orchestrator rebuilds this conversation on replay, so the ids must not move."""
        assert self._cycle_ids() == self._cycle_ids()

    def test_a_revisited_node_records_only_what_is_new(self) -> None:
        provider = _InMemoryStateProvider()
        entity = AgentEntity(_stub_agent(), state_provider=provider)

        def _deliver_to_a(context: list[Message], correlation_id: str) -> int:
            request = RunRequest(
                message=context[-1].text or "",
                correlation_id=correlation_id,
                context_messages=[m.to_dict() for m in context],
            )
            entry = DurableAgentStateRequest.from_run_request(request)
            entry.messages = entity._drop_already_stored(entry.messages)
            entity.state.data.conversation_history.append(entry)
            return len(entry.messages)

        conversation: Any = "start"
        conversation = build_agent_executor_response("A", "a1", None, conversation)
        conversation = build_agent_executor_response("B", "b1", None, conversation)
        first = _deliver_to_a(list(conversation.full_conversation), "corr-1")

        conversation = build_agent_executor_response("A", "a2", None, conversation)
        conversation = build_agent_executor_response("B", "b2", None, conversation)
        second = _deliver_to_a(list(conversation.full_conversation), "corr-2")

        assert first == 3, f"expected the first delivery to be recorded whole, got {first}"
        assert second == 2, f"expected only the two new messages, got {second} of 5 delivered"


class TestDedupSurvivesRetention:
    """Duplicate detection must not depend on the messages still being there.

    Retention deletes oldest-first, which removes exactly the ids an identity check relies on. The
    orchestrator's own conversation is never evicted, so it re-sends them, and an entity comparing
    against stored ids would re-record precisely what was just deleted.
    """

    def _deliver(self, entity: AgentEntity, context: list[Message], correlation_id: str) -> int:
        request = RunRequest(
            message=context[-1].text or "",
            correlation_id=correlation_id,
            context_messages=[m.to_dict() for m in context],
        )
        entry = DurableAgentStateRequest.from_run_request(request)
        entry.messages = entity._drop_already_stored(entry.messages)
        entity.state.data.conversation_history.append(entry)
        return len(entry.messages)

    def test_evicted_context_is_not_re_ingested(self) -> None:
        provider = _InMemoryStateProvider()
        entity = AgentEntity(_stub_agent(), state_provider=provider)

        conversation: Any = "start"
        conversation = build_agent_executor_response("A", "a1", None, conversation)
        conversation = build_agent_executor_response("B", "b1", None, conversation)
        self._deliver(entity, list(conversation.full_conversation), "corr-1")

        # Retention deletes the oldest messages, taking their ids with them.
        entity.state.data.conversation_history.clear()

        conversation = build_agent_executor_response("A", "a2", None, conversation)
        conversation = build_agent_executor_response("B", "b2", None, conversation)
        recorded = self._deliver(entity, list(conversation.full_conversation), "corr-2")

        assert recorded == 2, f"expected only the two new messages after eviction, got {recorded} of 5"

    def test_the_mark_is_kept_per_executor(self) -> None:
        """A fan-out gives two branches the same position, so one global mark would conflate them."""
        provider = _InMemoryStateProvider()
        entity = AgentEntity(_stub_agent(), state_provider=provider)

        conversation: Any = "start"
        conversation = build_agent_executor_response("A", "a1", None, conversation)
        conversation = build_agent_executor_response("B", "b1", None, conversation)
        self._deliver(entity, list(conversation.full_conversation), "corr-1")

        marks = entity.state.data.ingested_positions or {}
        assert set(marks) == {"input", "A", "B"}, f"expected a mark per producing executor, got {marks}"

    def test_the_mark_round_trips_through_durable_state(self) -> None:
        provider = _InMemoryStateProvider()
        entity = AgentEntity(_stub_agent(), state_provider=provider)

        conversation: Any = build_agent_executor_response("A", "a1", None, "start")
        self._deliver(entity, list(conversation.full_conversation), "corr-1")
        entity.persist_state()

        restored = DurableAgentState.from_dict(provider._get_state_dict())
        assert restored.data.ingested_positions == entity.state.data.ingested_positions


class TestCoreSessionIdentity:
    """The id handed to core must identify one entity, not one workflow run."""

    def test_workflow_nodes_do_not_share_a_core_session_id(self) -> None:
        """Nodes of one workflow share the entity key and differ only by entity name.

        An external history provider keys its storage on the core session id, so taking the key
        alone would file every node's conversation under one entry.
        """
        writer = _InMemoryStateProvider(session_id="run-1", entity_name="dafx-writer")
        reviewer = _InMemoryStateProvider(session_id="run-1", entity_name="dafx-reviewer")

        assert writer.session_id == reviewer.session_id
        assert writer.core_session_id != reviewer.core_session_id, f"both nodes resolved to {writer.core_session_id}"

    def test_core_session_id_falls_back_to_the_key(self) -> None:
        """State providers predating the entity-name hook keep working."""
        assert _InMemoryStateProvider(session_id="solo").core_session_id == "solo"


class TestRunRequestRoundTrip:
    """context_messages survives the entity wire format."""

    def test_context_messages_round_trip(self) -> None:
        messages = [Message(role="user", contents=["hello"], message_id="m0")]
        request = RunRequest(
            message="hello",
            correlation_id="corr-0",
            context_messages=[m.to_dict() for m in messages],
        )

        restored = RunRequest.from_dict(request.to_dict())

        assert restored.context_messages is not None
        assert len(restored.context_messages) == 1

    def test_absent_context_messages_stay_none(self) -> None:
        request = RunRequest(message="hello", correlation_id="corr-0")

        restored = RunRequest.from_dict(request.to_dict())

        assert restored.context_messages is None
        assert "contextMessages" not in request.to_dict()
