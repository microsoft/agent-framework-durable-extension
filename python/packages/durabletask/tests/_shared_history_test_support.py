# Copyright (c) Microsoft. All rights reserved.

"""Shared transcript-only state and provider doubles for history tests."""

from __future__ import annotations

import json
from collections.abc import AsyncIterable, Awaitable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from agent_framework import BaseChatClient, ChatResponse, ChatResponseUpdate, HistoryProvider, Message, ResponseStream

from agent_framework_durabletask._history_provider import (
    DurableHistoryBinding,
    bind_durable_history,
    unbind_durable_history,
)
from agent_framework_durabletask._shared_agent_state import (
    DurableAgentState,
    DurableAgentStateEntry,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    _parse_history_entries,
)

OLD = datetime(2026, 1, 1, tzinfo=timezone.utc)


class _CanonicalStateProvider:
    """State-provider double backed by canonical durable state."""

    def __init__(self, history: list[DurableAgentStateEntry] | None = None) -> None:
        self.persist_count = 0
        self.state = DurableAgentState()
        self.state.data.conversation_history = list(history or [])

    def persist(self) -> None:
        self.persist_count += 1

    def clone(self) -> _CanonicalStateProvider:
        raw = [entry.to_dict() for entry in self.state.data.conversation_history]
        return _CanonicalStateProvider(
            _parse_history_entries({"conversationHistory": json.loads(json.dumps(raw, allow_nan=False))})
        )


class _PassiveChatClient(BaseChatClient):
    STORES_BY_DEFAULT = False

    def __init__(self, *, service_conversation_id: str | None = None) -> None:
        super().__init__()
        self.service_conversation_id = service_conversation_id
        self.received_messages: list[list[Message]] = []

    def _inner_get_response(
        self,
        *,
        messages: Sequence[Message],
        stream: bool,
        options: Mapping[str, Any],
        **kwargs: Any,
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        self.received_messages.append(deepcopy(list(messages)))
        response = ChatResponse(
            messages=[Message("assistant", [f"answer-{len(self.received_messages)}"])],
            conversation_id=self.service_conversation_id if options.get("store") else None,
            finish_reason="stop",
        )
        if stream:

            async def updates() -> AsyncIterable[ChatResponseUpdate]:
                yield ChatResponseUpdate(
                    role="assistant",
                    contents=response.messages[0].contents,
                    conversation_id=response.conversation_id,
                    finish_reason=response.finish_reason,
                )

            return ResponseStream(updates(), finalizer=ChatResponse.from_updates)

        async def get() -> ChatResponse:
            return response

        return get()


class OrdinaryExternalHistory(HistoryProvider):
    def __init__(self, source_id: str = "external", **kwargs: Any) -> None:
        super().__init__(source_id, **kwargs)
        self.calls: list[tuple[str, str | None]] = []
        self.saved: list[Message] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        self.calls.append(("load", session_id))
        return deepcopy(self.saved)

    async def save_messages(self, session_id: str | None, messages: Any, **kwargs: Any) -> None:
        self.calls.append(("save", session_id))
        self.saved.extend(deepcopy(list(messages)))


@contextmanager
def _bound(
    provider: _CanonicalStateProvider,
    correlation_id: str | None = "current",
    *,
    service_owns_history: bool = False,
) -> Iterator[Any]:
    binding = DurableHistoryBinding(provider, correlation_id, service_owns_history)
    token = bind_durable_history(binding)
    try:
        yield binding
    finally:
        unbind_durable_history(token)


def _stored(
    text: str,
    *,
    role: str = "user",
    message_id: str | None = None,
    excluded: bool = False,
) -> Any:
    stored = DurableAgentStateMessage.from_chat_message(Message(role, [text], message_id=message_id))
    if excluded:
        stored.extension_data = {"_excluded": True}
    return stored


def _request(correlation_id: str, *messages: Any) -> Any:
    return DurableAgentStateRequest(correlation_id, OLD, list(messages))


def _history_texts(provider: _CanonicalStateProvider) -> list[str]:
    return [message.text for entry in provider.state.data.conversation_history for message in entry.messages]


def _history_ids(provider: _CanonicalStateProvider) -> list[str | None]:
    return [message.message_id for entry in provider.state.data.conversation_history for message in entry.messages]
