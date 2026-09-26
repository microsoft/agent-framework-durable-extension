# Copyright (c) Microsoft. All rights reserved.

"""Shared execution test doubles for runtime transaction boundaries."""

from __future__ import annotations

import json
from collections.abc import AsyncIterable, Awaitable, Mapping, Sequence
from copy import deepcopy
from typing import Any

from _history_pipeline_test_support import CountingHistory, ToolChatClient
from agent_framework import Agent, BaseChatClient, ChatResponse, ChatResponseUpdate, Message, ResponseStream

from agent_framework_durabletask import AgentEntityStateProviderMixin


def _roundtrip_json(value: Any) -> Any:
    return json.loads(json.dumps(value, allow_nan=False))


class JsonStateProvider(AgentEntityStateProviderMixin):
    """JSON-only entity state provider that exposes committed-write evidence."""

    def __init__(
        self,
        raw: dict[str, Any] | None = None,
        *,
        session_id: str = "runtime-session",
        entity_name: str = "runtime",
    ) -> None:
        self.raw = _roundtrip_json(raw or {})
        self._session_id = session_id
        self._entity_name = entity_name
        self.attempted_writes = 0
        self.successful_writes = 0
        self.fail_before_write = False

    def _get_state_dict(self) -> dict[str, Any]:
        return _roundtrip_json(self.raw)

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        self.attempted_writes += 1
        if self.fail_before_write:
            raise OSError("injected commit failure")
        self.raw = _roundtrip_json(state)
        self.successful_writes += 1

    def _get_session_id_from_entity(self) -> str:
        return self._session_id

    def _get_entity_name_from_entity(self) -> str:
        return self._entity_name


class LostAcknowledgementJsonStateProvider(JsonStateProvider):
    """Backend write succeeds, but the caller loses the acknowledgement."""

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        super()._set_state_dict(state)
        raise OSError("storage acknowledgement lost after write")


class RecordingChatClient(BaseChatClient):
    """Minimal BaseChatClient that records the concrete messages core sends."""

    STORES_BY_DEFAULT = False

    def __init__(
        self,
        *,
        fail: bool = False,
        response_message_id: str | None = None,
        conversation_id: str | None = None,
    ) -> None:
        super().__init__()
        self.fail = fail
        self.response_message_id = response_message_id
        self.conversation_id = conversation_id
        self.received_messages: list[list[Message]] = []
        self.received_options: list[dict[str, Any]] = []

    def _inner_get_response(
        self,
        *,
        messages: Sequence[Message],
        stream: bool,
        options: Mapping[str, Any],
        **kwargs: Any,
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        del kwargs
        self.received_messages.append(deepcopy(list(messages)))
        self.received_options.append(dict(options))
        if self.fail:
            raise RuntimeError("model failed before history persistence")

        call = len(self.received_messages)
        response = ChatResponse(
            messages=[
                Message(
                    "assistant",
                    [f"reply-{call}"],
                    message_id=self.response_message_id,
                    additional_properties={"model_metadata": {"calls": call}},
                )
            ],
            response_id=f"response-{call}",
            conversation_id=self.conversation_id if options.get("store") else None,
            finish_reason="stop",
        )
        if stream:

            async def updates() -> AsyncIterable[ChatResponseUpdate]:
                yield ChatResponseUpdate(
                    role="assistant",
                    contents=response.messages[0].contents,
                    message_id=response.messages[0].message_id,
                    additional_properties=deepcopy(response.messages[0].additional_properties),
                    response_id=response.response_id,
                    conversation_id=response.conversation_id,
                    finish_reason=response.finish_reason,
                )

            return ResponseStream(updates(), finalizer=ChatResponse.from_updates)

        async def get() -> ChatResponse:
            return response

        return get()


class NonStreamingAgent(Agent):
    """Reject streaming before any model or tool execution starts."""

    def run(self, *args: Any, **kwargs: Any) -> Any:
        if kwargs.get("stream"):
            raise TypeError("stream is not supported")
        return super().run(*args, **kwargs)


__all__ = [
    "CountingHistory",
    "JsonStateProvider",
    "LostAcknowledgementJsonStateProvider",
    "NonStreamingAgent",
    "RecordingChatClient",
    "ToolChatClient",
]
