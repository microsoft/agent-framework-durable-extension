# Copyright (c) Microsoft. All rights reserved.

"""Shared helpers for runtime session persistence and entity session store tests."""

from __future__ import annotations

from collections.abc import Sequence
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from _execution_test_support import JsonStateProvider
from agent_framework import (
    Agent,
    AgentResponse,
    AgentSession,
    Content,
    ContextProvider,
    HistoryProvider,
    Message,
    SessionContext,
)

from agent_framework_durabletask import AgentEntity, DurableAgentState
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._shared_agent_state import (
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
)

EXTERNAL_SOURCE = "reset-external-primary"
LOCAL_SOURCE = "reset-local-primary"
AUDIT_SOURCES = ("reset-audit-0", "reset-audit-1")
SESSION_ID = "revision-session"
PRIOR_CORRELATION = "reset-prior-completion"
PRIOR_OCCURRENCE = "reset-prior-occurrence"


def _request(correlation_id: str, message: str, **kwargs: Any) -> dict[str, Any]:
    return {"message": message, "correlationId": correlation_id, **kwargs}


def _clone_provider(provider: JsonStateProvider) -> JsonStateProvider:
    return JsonStateProvider(
        deepcopy(provider.raw),
        session_id=provider.session_id,
        entity_name=provider._get_entity_name_from_entity(),
    )


def _cold(agent: Agent, provider: JsonStateProvider) -> tuple[AgentEntity, JsonStateProvider]:
    cold_provider = _clone_provider(provider)
    return AgentEntity(agent, state_provider=cold_provider), cold_provider


class _ControlProvider(ContextProvider):
    def __init__(self) -> None:
        super().__init__("control")
        self.loaded: list[dict[str, Any]] = []
        self.responses: list[AgentResponse] = []
        self.sessions: list[AgentSession] = []
        self.agents: list[Any] = []

    async def before_run(
        self,
        *,
        agent: Any,
        session: AgentSession,
        context: SessionContext,
        state: dict[str, Any],
    ) -> None:
        del context
        self.loaded.append(deepcopy(state))
        self.sessions.append(session)
        self.agents.append(agent)
        state["before_runs"] = state.get("before_runs", 0) + 1

    async def after_run(self, *, context: SessionContext, state: dict[str, Any], **kwargs: Any) -> None:
        del kwargs
        assert isinstance(context.response, AgentResponse)
        self.responses.append(context.response)
        state["after_runs"] = state.get("after_runs", 0) + 1


class _ExternalHistory(HistoryProvider):
    def __init__(
        self, store: dict[str, list[Message]], events: list[str], *, source_id: str = "external", load: bool = True
    ) -> None:
        super().__init__(source_id, load_messages=load)
        self.store = store
        self.events = events
        self.calls: list[tuple[str, str | None, dict[str, Any]]] = []
        self.partial_failure = False

    async def before_run(self, **kwargs: Any) -> None:
        self.events.append(f"{self.source_id}.before")
        await super().before_run(**kwargs)

    async def after_run(self, **kwargs: Any) -> None:
        self.events.append(f"{self.source_id}.after")
        await super().after_run(**kwargs)

    async def get_messages(self, session_id: str | None, *, state: Any = None, **kwargs: Any) -> list[Message]:
        assert isinstance(state, dict)
        self.events.append(f"{self.source_id}.load")
        self.calls.append(("load", session_id, deepcopy(state)))
        state["loads"] = state.get("loads", 0) + 1
        return deepcopy(self.store.get(f"{self.source_id}:{session_id}", []))

    async def save_messages(
        self, session_id: str | None, messages: Sequence[Message], *, state: Any = None, **kwargs: Any
    ) -> None:
        assert isinstance(state, dict)
        self.events.append(f"{self.source_id}.save")
        self.calls.append(("save", session_id, deepcopy(state)))
        state["saves"] = state.get("saves", 0) + 1
        accepted = messages[:1] if self.partial_failure else messages
        self.store.setdefault(f"{self.source_id}:{session_id}", []).extend(deepcopy(list(accepted)))
        if self.partial_failure:
            raise OSError("partial external save")


def _seed(state: dict[str, Any], **siblings: Any) -> JsonStateProvider:
    initial = DurableAgentState()
    initial.data.session = {
        "type": "session",
        "session_id": "previous-session-id",
        "service_session_id": {"remote": "saved"},
        "state": deepcopy(state),
        **siblings,
    }
    return JsonStateProvider(initial.to_dict())


def _prior_input() -> Message:
    return Message(
        "user",
        [Content.from_text("prior question", additional_properties={"content": ["original", None]})],
        message_id="reset-prior-input",
        additional_properties={"input": {"keep": [False, 0, "雪"]}},
    )


def _original_response() -> AgentResponse[Any]:
    return AgentResponse(
        messages=[
            Message(
                "assistant",
                [Content.from_text("prior answer", additional_properties={"content": ["answer", None]})],
                message_id="reset-prior-answer",
                author_name="original-author",
                additional_properties={"answer": {"keep": [True, 1, "雪"]}},
            )
        ],
        response_id="reset-original-response",
        created_at="2026-09-01T00:00:00+00:00",
        finish_reason="stop",
        usage_details={"input_token_count": 3, "output_token_count": 5, "total_token_count": 8},
        additional_properties={"result": {"keep": ["original", False, 0, None]}},
    )


def _opaque_slice(label: str) -> dict[str, Any]:
    return {
        "messages": [{"opaque": [label, False, 0, None]}],
        "_positions": {"opaque": [3, 1]},
        "metadata": {"messages": ["nested, not transient"], "_positions": [2, 0]},
    }


def _reset_seed(service_session_id: str | None) -> DurableAgentState:
    state = DurableAgentState()
    now = datetime.now(timezone.utc)
    state.data.conversation_history = [
        DurableAgentStateRequest(PRIOR_CORRELATION, now, [DurableAgentStateMessage.from_chat_message(_prior_input())]),
        DurableAgentStateResponse.from_run_response(PRIOR_CORRELATION, _original_response()),
    ]
    state.record_response(PRIOR_CORRELATION, _original_response(), delivery_window_seconds=86_400, now=now)
    state.data.ingested_messages = {PRIOR_OCCURRENCE: [message_identity(_prior_input())]}
    state.unknown_fields = {"futureRoot": {"keep": [False, 0, "雪", None]}}
    state.data.unknown_fields = {"futureData": {"keep": [True, 1, "雪", None]}}
    state.data.extension_data = {"metadata": {"keep": ["data", None]}}
    session = AgentSession(session_id=SESSION_ID, service_session_id=service_session_id)
    session.state = {
        EXTERNAL_SOURCE: _opaque_slice("external"),
        "unrelated": _opaque_slice("unrelated"),
        **{
            source: {"metadata": {"messages": ["keep", source], "_positions": [1, 0]}}
            for source in (LOCAL_SOURCE, *AUDIT_SOURCES)
        },
    }
    state.data.session = {
        **session.to_dict(),
        "futureSession": {"keep": [False, 0, "雪", None]},
        "messages": [{"opaque": ["top-level", None]}],
        "_positions": {"top-level": [2, 0]},
    }
    return state
