# Copyright (c) Microsoft. All rights reserved.

"""Regression tests for the revised durable execution and history contract."""

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from agent_framework import Agent, AgentSession, HistoryProvider, InMemoryHistoryProvider, Message
from test_durable_history_provider import RecordingChatClient

from agent_framework_durabletask import (
    AgentEntity,
    AgentEntityStateProviderMixin,
    DurableAgentState,
    DurableAgentStateErrorResponse,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    RunRequest,
)
from agent_framework_durabletask._history_provider import ensure_durable_history
from agent_framework_durabletask._retention import DEFAULT_MAX_STATE_BYTES, DEFAULT_RETENTION, enforce_budget


class JsonStateProvider(AgentEntityStateProviderMixin):
    """Storage boundary that never aliases staged state and supports cold reloads."""

    def __init__(self, raw: dict[str, Any] | None = None) -> None:
        self.raw = deepcopy(raw or {})
        self.writes = 0
        self.fail_writes = False

    def _get_state_dict(self) -> dict[str, Any]:
        return deepcopy(self.raw)

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        if self.fail_writes:
            raise OSError("injected commit failure")
        self.raw = json.loads(json.dumps(state))
        self.writes += 1

    def _get_session_id_from_entity(self) -> str:
        return "revision-session"


class ExternalHistory(HistoryProvider):
    def __init__(self) -> None:
        super().__init__("external")
        self.messages: list[Message] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        return list(self.messages)

    async def save_messages(self, session_id: str | None, messages: Any, **kwargs: Any) -> None:
        self.messages.extend(messages)


def make_agent(client: Any, providers: list[Any] | None = None) -> Agent:
    return Agent(client=client, name="revision", context_providers=providers)


def test_retention_defaults_do_not_enable_deletion() -> None:
    assert DEFAULT_RETENTION == "keep_all"
    assert DEFAULT_MAX_STATE_BYTES is None


def test_multiple_primary_providers_fail_without_mutating_agent() -> None:
    providers = [InMemoryHistoryProvider("first"), InMemoryHistoryProvider("second")]
    agent = make_agent(RecordingChatClient(), providers)
    with pytest.raises(ValueError, match="primary"):
        ensure_durable_history(agent)
    assert agent.context_providers == providers


@pytest.mark.parametrize("context", [["bad"], [1], [{}, None], "not-a-list", {}])
def test_malformed_context_is_rejected_at_the_request_boundary(context: Any) -> None:
    with pytest.raises(ValueError, match="contextMessages"):
        RunRequest.from_dict({"message": "input", "correlationId": "c0", "contextMessages": context})


def test_empty_projection_does_not_become_the_unfiltered_input() -> None:
    request = RunRequest(message="must not leak", correlation_id="empty", context_messages=[])
    restored = RunRequest.from_dict(request.to_dict())
    assert restored.context_messages == []
    assert DurableAgentStateRequest.from_run_request(restored).messages == []


async def test_original_response_survives_transcript_mutation_and_cold_reload() -> None:
    provider = JsonStateProvider()
    client = RecordingChatClient()
    entity = AgentEntity(make_agent(client), state_provider=provider)
    response = await entity.run({"message": "first", "correlationId": "c0"})
    original = response.to_dict()
    entity.state.data.conversation_history.clear()
    entity.persist_state()
    restored = AgentEntity(make_agent(client), state_provider=JsonStateProvider(provider.raw))
    duplicate = await restored.run({"message": "first", "correlationId": "c0"})
    assert duplicate.to_dict() == original
    assert len(client.received_messages) == 1


async def test_external_history_needs_no_contentless_request_mirror() -> None:
    external = ExternalHistory()
    provider = JsonStateProvider()
    entity = AgentEntity(make_agent(RecordingChatClient(), [external]), state_provider=provider)
    await entity.run({"message": "first", "correlationId": "external-0"})
    assert [m.text for m in external.messages] == ["first", "reply-1"]
    assert entity.state.data.conversation_history == []
    assert entity.state.try_get_agent_response("external-0") is not None


async def test_external_reset_does_not_claim_to_clear_an_untouched_store() -> None:
    external = ExternalHistory()
    provider = JsonStateProvider()
    entity = AgentEntity(make_agent(RecordingChatClient(), [external]), state_provider=provider)
    await entity.run({"message": "first", "correlationId": "external-0"})
    before = deepcopy(provider.raw)
    with pytest.raises(NotImplementedError, match="external"):
        entity.reset()
    assert provider.raw == before


def test_sparse_context_is_not_lost_after_an_older_position_was_skipped() -> None:
    provider = JsonStateProvider()
    entity = AgentEntity(make_agent(RecordingChatClient()), state_provider=provider)

    def deliver(positions: list[int]) -> list[int]:
        messages = [
            DurableAgentStateMessage.from_chat_message(
                Message("user", [str(position)], message_id=f"wf_source_{position}")
            )
            for position in positions
        ]
        return [int(m.text) for m in entity._drop_already_stored(messages)]

    assert deliver([1, 3]) == [1, 3]
    entity.state.data.conversation_history.clear()
    entity.persist_state()
    entity = AgentEntity(make_agent(RecordingChatClient()), state_provider=JsonStateProvider(provider.raw))
    assert deliver([2, 4]) == [2, 4]


async def test_unreachable_protected_floor_does_not_destroy_old_history() -> None:
    state = DurableAgentState()
    old = datetime.now(timezone.utc) - timedelta(hours=1)
    for index in range(10):
        state.data.conversation_history.append(
            DurableAgentStateRequest(
                correlation_id=f"old-{index}",
                created_at=old,
                messages=[DurableAgentStateMessage.from_chat_message(Message("user", ["x" * 1000]))],
            )
        )
    state.data.session = {"protected": "s" * 30_000}
    before = state.to_json()
    with pytest.raises(ValueError, match="[Cc]apacity|budget|floor"):
        await enforce_budget(state, max_state_bytes=12_000)
    assert state.to_json() == before


async def test_old_failed_turns_are_storage_candidates_not_model_history() -> None:
    state = DurableAgentState()
    old = datetime.now(timezone.utc) - timedelta(hours=1)
    for index in range(80):
        state.data.conversation_history.append(
            DurableAgentStateErrorResponse(
                correlation_id=f"failed-{index}",
                created_at=old,
                messages=[DurableAgentStateMessage.from_chat_message(Message("assistant", ["e" * 400]))],
            )
        )
    assert await enforce_budget(state, max_state_bytes=12_000) > 0
    assert len(state.to_json()) < 12_000


async def test_commit_failure_does_not_leave_an_in_memory_completed_request() -> None:
    provider = JsonStateProvider()
    client = RecordingChatClient()
    entity = AgentEntity(make_agent(client), state_provider=provider)
    provider.fail_writes = True
    with pytest.raises(OSError, match="commit failure"):
        await entity.run({"message": "first", "correlationId": "c0"})
    assert provider.raw == {}
    assert entity.state.try_get_agent_response("c0") is None
    provider.fail_writes = False
    await entity.run({"message": "first", "correlationId": "c0"})
    assert len(client.received_messages) == 2
    assert provider.writes == 1


async def test_inactive_service_id_is_not_sent_to_a_client_owned_run() -> None:
    from agent_framework import AgentResponse

    seen: list[Any] = []

    class ServiceAgent:
        name = "service"
        client = type("Client", (), {"STORES_BY_DEFAULT": True})()
        context_providers: list[Any] = []

        def create_session(self, **kwargs: Any) -> AgentSession:
            return AgentSession(**kwargs)

        async def run(self, *, session: AgentSession, stream: bool = False, **kwargs: Any) -> AgentResponse:
            if stream:
                raise TypeError("stream is not supported")
            seen.append(session.service_session_id)
            if kwargs["options"].get("store", True):
                session.service_session_id = "service-branch"
            return AgentResponse(messages=[Message("assistant", ["ok"])])

    provider = JsonStateProvider()
    for index, store in enumerate([True, False, True]):
        agent: Any = ServiceAgent()
        entity = AgentEntity(agent, state_provider=provider)
        await entity.run({"message": f"m{index}", "correlationId": f"c{index}", "options": {"store": store}})
        provider = JsonStateProvider(provider.raw)
    assert seen == [None, None, "service-branch"]


def test_unknown_state_data_and_entry_fields_survive_round_trip() -> None:
    state = DurableAgentState("1.2.0").to_dict()
    state["futureRoot"] = {"opaque": [1, 2]}
    state["data"]["futureData"] = {"opaque": [3, 4]}
    state["data"]["conversationHistory"] = [
        {"$type": "futureKind", "correlationId": "x", "createdAt": "2026-01-01T00:00:00+00:00", "extra": 9}
    ]
    assert DurableAgentState.from_dict(state).to_dict() == state
