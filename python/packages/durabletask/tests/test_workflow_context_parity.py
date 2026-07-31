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
    DurableAgentStateRequest,
    RunRequest,
)
from agent_framework_durabletask._workflows.orchestrator import _build_context_messages


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
    def __init__(self, *, session_id: str = "wf-session") -> None:
        self._session_id = session_id
        self._state_dict: dict[str, Any] = {}

    def _get_state_dict(self) -> dict[str, Any]:
        return self._state_dict

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        self._state_dict = state

    def _get_session_id_from_entity(self) -> str:
        return self._session_id


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
