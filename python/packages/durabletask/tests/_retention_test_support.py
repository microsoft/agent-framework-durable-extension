# Copyright (c) Microsoft. All rights reserved.

"""Focused donor test doubles for retention, telemetry and JSON storage boundaries."""

import json
from collections.abc import AsyncIterable, Awaitable, Sequence
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

from agent_framework import ChatResponse, ChatResponseUpdate, Content, Message, ResponseStream

from agent_framework_durabletask import (
    AgentEntityStateProviderMixin,
    DurableAgentState,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
)


def _state(turns: int, *, chars: int = 400, excluded_before: int = 0, excluded_recent: int = 0) -> DurableAgentState:
    """Build legacy transcript-delivered state with the given number of user/assistant turns.

    Args:
        turns: How many exchanges to record.
        chars: Size of each message's text.

    Keyword Args:
        excluded_before: Mark this many leading messages as compaction-excluded, as a user's own
            sliding window would.
        excluded_recent: Mark this many of the most recent messages as compaction-excluded, as a
            tool-result strategy can do without touching the oldest turns.

    Returns:
        The populated state.
    """
    # These manually appended responses use legacy history lookup. Version 2 fixtures must
    # record independent mailbox results instead of treating transcript entries as delivery.
    state = DurableAgentState(schema_version="1.2.0")
    now = datetime.now(tz=timezone.utc)
    # Space legacy turns a minute apart so their delivery windows have elapsed. Tests of live
    # delivery explicitly refresh timestamps rather than depending on the test's running time.
    marked = 0
    for index in range(turns):
        occurred_at = now - timedelta(minutes=turns - index)
        request = DurableAgentStateRequest(
            correlation_id=f"c{index}",
            created_at=occurred_at,
            messages=[
                DurableAgentStateMessage.from_chat_message(
                    Message(role="user", contents=["u" * chars], message_id=f"u{index}")
                )
            ],
        )
        response = DurableAgentStateResponse(
            correlation_id=f"c{index}",
            created_at=occurred_at,
            messages=[
                DurableAgentStateMessage.from_chat_message(
                    Message(role="assistant", contents=["a" * chars], message_id=f"a{index}")
                )
            ],
        )
        for entry in (request, response):
            for stored in entry.messages:
                if marked < excluded_before:
                    stored.extension_data = {"_excluded": True, "_excluded_reason": "sliding_window"}
                    marked += 1
        state.data.conversation_history.extend([request, response])

    if excluded_recent:
        stored_messages = [m for entry in state.data.conversation_history for m in entry.messages]
        for stored in stored_messages[-excluded_recent:]:
            stored.extension_data = {"_excluded": True, "_excluded_reason": "tool_result_compaction"}
    return state


def _ingestion_messages(raw: dict[str, Any]) -> dict[str, list[str]]:
    profile = raw["data"].get("pythonIngestion")
    if profile is None:
        return {}
    assert profile["profile"] == "agent-framework-python.ingestion"
    assert type(profile["version"]) is int and profile["version"] == 1
    messages = profile["messages"]
    assert isinstance(messages, dict)
    assert all(
        isinstance(values, list) and all(isinstance(value, str) for value in values) for values in messages.values()
    )
    return messages


class RecordingChatClient:
    """Minimal chat client that records the message list it receives per call."""

    def __init__(self) -> None:
        self.additional_properties: dict[str, Any] = {}
        self.received_messages: list[list[Message]] = []
        self._counter = 0

    def get_response(
        self,
        messages: str | Message | list[str] | list[Message],
        *,
        stream: bool = False,
        options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        options = options or {}
        normalized = [m for m in messages if isinstance(m, Message)] if isinstance(messages, list) else []
        self.received_messages.append(normalized)

        if stream:
            return self._stream(options)

        async def _get() -> ChatResponse:
            self._counter += 1
            return ChatResponse(messages=Message(role="assistant", contents=[f"reply-{self._counter}"]))

        return _get()

    def _stream(self, options: dict[str, Any]) -> ResponseStream[ChatResponseUpdate, ChatResponse]:
        async def _updates() -> AsyncIterable[ChatResponseUpdate]:
            self._counter += 1
            yield ChatResponseUpdate(contents=[Content.from_text(f"reply-{self._counter}")], role="assistant")

        def _finalize(updates: Sequence[ChatResponseUpdate]) -> ChatResponse:
            return ChatResponse.from_updates(updates, output_format_type=options.get("response_format"))

        return ResponseStream(_updates(), finalizer=_finalize)


class _InMemoryStateProvider(AgentEntityStateProviderMixin):
    """Test-only state provider that keeps the serialized entity state in memory."""

    def __init__(self, *, session_id: str = "durable-history-session", raw: dict[str, Any] | None = None) -> None:
        self._session_id = session_id
        self._state_dict: dict[str, Any] = json.loads(json.dumps(raw or {}))
        self.writes = 0

    def _get_state_dict(self) -> dict[str, Any]:
        return deepcopy(self._state_dict)

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        # Reject non-JSON state and avoid aliasing the staged operation snapshot.
        self._state_dict = json.loads(json.dumps(state))
        self.writes += 1

    def _get_session_id_from_entity(self) -> str:
        return self._session_id


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
