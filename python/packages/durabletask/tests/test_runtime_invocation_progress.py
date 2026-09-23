# Copyright (c) Microsoft. All rights reserved.

"""Runtime retry guards for streamed output, tool execution, and rejected response ids."""

from __future__ import annotations

from collections.abc import AsyncIterable, Awaitable, Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any, cast

import pytest
from _execution_test_support import JsonStateProvider, NonStreamingAgent, ToolChatClient
from _invocation_progress_test_support import _ScriptedNonStreamingClient
from agent_framework import (
    Agent,
    AgentResponse,
    AgentResponseUpdate,
    AgentSession,
    BaseChatClient,
    ChatResponse,
    ChatResponseUpdate,
    Content,
    ContextProvider,
    Message,
    ResponseStream,
    SessionContext,
    tool,
)

from agent_framework_durabletask import AgentEntity, DurableAgentState
from agent_framework_durabletask import _entities as durable_entities
from agent_framework_durabletask._callbacks import AgentCallbackContext
from agent_framework_durabletask._message_identity import message_identity

_TOOL_EFFECTS: list[str] = []


def _request(correlation_id: str, message: str, **kwargs: Any) -> dict[str, Any]:
    options = {"store": True}
    if "options" in kwargs:
        options.update(cast("dict[str, Any]", kwargs.pop("options")))
    return {"message": message, "correlationId": correlation_id, "options": options, **kwargs}


def _provider(*, session_id: str = "runtime-session", entity_name: str = "runtime") -> JsonStateProvider:
    state = DurableAgentState()
    state.data.session = AgentSession(session_id=session_id, service_session_id="saved-service-id").to_dict()
    return JsonStateProvider(deepcopy(state.to_dict()), session_id=session_id, entity_name=entity_name)


def _session_payload(provider: JsonStateProvider) -> dict[str, Any]:
    payload = DurableAgentState.from_dict(deepcopy(provider.raw)).data.session
    assert isinstance(payload, dict)
    return payload


def _error_codes(response: AgentResponse) -> list[str]:
    return [
        content.error_code or ""
        for message in response.messages
        for content in message.contents
        if content.type == "error"
    ]


def _missing_previous_response(message: str = "previous response is not readable yet") -> Exception:
    return StructuredServiceError(message, body={"code": durable_entities._MISSING_PREVIOUS_RESPONSE_CODE})


class StructuredServiceError(RuntimeError):
    def __init__(self, message: str, *, code: str | None = None, body: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.body = body


class _RecordingCallback:
    def __init__(self) -> None:
        self.updates: list[AgentResponseUpdate] = []
        self.responses: list[AgentResponse] = []

    async def on_streaming_response_update(self, update: AgentResponseUpdate, context: AgentCallbackContext) -> None:
        del context
        self.updates.append(deepcopy(update))

    async def on_agent_response(self, response: AgentResponse, context: AgentCallbackContext) -> None:
        del context
        self.responses.append(deepcopy(response))


class _SessionProbe(ContextProvider):
    def __init__(self) -> None:
        super().__init__("session-probe")
        self.service_ids: list[str | Mapping[str, Any] | None] = []
        self.inputs: list[list[Message]] = []

    async def before_run(self, *, session: AgentSession, context: SessionContext, **kwargs: Any) -> None:
        del kwargs
        self.service_ids.append(session.service_session_id)
        self.inputs.append(deepcopy(context.input_messages))


class _StreamRejectedClient(BaseChatClient):
    STORES_BY_DEFAULT = True

    def __init__(self) -> None:
        super().__init__()
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
        assert stream is True

        async def updates() -> AsyncIterable[ChatResponseUpdate]:
            yield ChatResponseUpdate(
                role="assistant",
                contents=[Content.from_text("partial")],
                additional_properties={"model_metadata": {"tags": ["keep"]}},
                response_id="response-1",
                conversation_id="saved-service-id",
                finish_reason="length",
            )
            raise _missing_previous_response("streamed response id was rejected after partial output")

        return ResponseStream(updates(), finalizer=ChatResponse.from_updates)


class _RejectAfterToolClient(ToolChatClient):
    STORES_BY_DEFAULT = True

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
        call = len(self.received_messages)
        if call == 2:
            raise _missing_previous_response("tool follow-up response was rejected")

        response = ChatResponse(
            messages=[
                Message(
                    "assistant",
                    [Content.from_function_call(call_id="call-1", name="lookup", arguments='{"key":"durable"}')],
                    additional_properties={"model_metadata": {"tags": ["keep"]}},
                )
            ],
            response_id=f"response-{call}",
            conversation_id="saved-service-id",
            finish_reason="tool_calls",
        )
        if stream:

            async def updates() -> AsyncIterable[ChatResponseUpdate]:
                yield ChatResponseUpdate(
                    role="assistant",
                    contents=response.messages[0].contents,
                    additional_properties=deepcopy(response.messages[0].additional_properties),
                    response_id=response.response_id,
                    conversation_id=response.conversation_id,
                    finish_reason=response.finish_reason,
                )

            return ResponseStream(updates(), finalizer=ChatResponse.from_updates)

        async def get() -> ChatResponse:
            return response

        return get()


class _DelegatingClient:
    def __init__(self, inner: ToolChatClient) -> None:
        self.inner = inner

    def __getattr__(self, name: str) -> object:
        if name in {"STORES_BY_DEFAULT", "function_invocation_configuration", "additional_properties"}:
            return getattr(self.inner, name)
        raise AttributeError(name)

    def get_response(
        self, messages: Sequence[Message], *, stream: bool = False, **kwargs: Any
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        return cast(Callable[..., Any], self.inner.get_response)(messages=messages, stream=stream, **kwargs)


class _ToolProvider(ContextProvider):
    async def before_run(self, *, context: SessionContext, **kwargs: Any) -> None:
        del kwargs
        context.tools.append(lookup)


@tool(name="lookup", approval_mode="never_require")
def lookup(key: str) -> str:
    _TOOL_EFFECTS.append(key)
    return f"value:{key}"


@pytest.fixture(autouse=True)
def _zero_rejected_id_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    assert durable_entities._REJECTED_ID_BACKOFF_SECONDS > 0
    monkeypatch.setattr(durable_entities, "_REJECTED_ID_BACKOFF_SECONDS", 0)
    _TOOL_EFFECTS.clear()


async def test_started_stream_failure_preserves_metadata_without_retry() -> None:
    provider = _provider(entity_name="stream-owner")
    client = _StreamRejectedClient()
    probe = _SessionProbe()
    callback = _RecordingCallback()
    entity = AgentEntity(
        Agent(client=client, name="stream-owner", default_options={"store": True}, context_providers=[probe]),
        callback=callback,
        state_provider=provider,
    )

    response = await entity.run(_request("stream-corr", "hello"))

    assert response.additional_properties["durable_status"] == "error"
    assert _error_codes(response) == ["StructuredServiceError"]
    assert len(client.received_messages) == 1
    assert probe.service_ids == ["saved-service-id"]
    assert len(callback.updates) == 1
    assert callback.updates[0].additional_properties == {"model_metadata": {"tags": ["keep"]}}
    assert callback.responses == []
    assert provider.successful_writes == 1
    assert AgentSession.from_dict(_session_payload(provider)).service_session_id == "saved-service-id"


async def test_zero_output_failure_retries_identical_input_with_bound_and_saved_session() -> None:
    context_message = Message("developer", ["carry this context"], message_id="ctx-1")
    provider = _provider(entity_name="retry-owner")
    probe = _SessionProbe()
    client = _ScriptedNonStreamingClient([
        lambda: (_ for _ in ()).throw(_missing_previous_response("attempt-1")),
        lambda: (_ for _ in ()).throw(_missing_previous_response("attempt-2")),
        lambda: (_ for _ in ()).throw(_missing_previous_response("attempt-3")),
        lambda: (_ for _ in ()).throw(_missing_previous_response("attempt-4")),
    ])
    entity = AgentEntity(
        NonStreamingAgent(
            client=client,
            name="retry-owner",
            default_options={"store": True},
            context_providers=[probe],
        ),
        state_provider=provider,
    )

    response = await entity.run(
        _request(
            "retry-corr",
            "question",
            contextMessages=[context_message.to_dict()],
            contextMessageIds=[message_identity(context_message)],
        )
    )

    serialized_calls = [[message.to_dict() for message in batch] for batch in client.received_messages]
    assert response.additional_properties["durable_status"] == "error"
    assert _error_codes(response) == ["StructuredServiceError"]
    assert len(client.received_messages) == 1 + durable_entities._REJECTED_ID_RETRIES
    assert serialized_calls[1:] == [serialized_calls[0]] * durable_entities._REJECTED_ID_RETRIES
    assert probe.service_ids == ["saved-service-id"] * (1 + durable_entities._REJECTED_ID_RETRIES)
    assert AgentSession.from_dict(_session_payload(provider)).service_session_id == "saved-service-id"


async def test_unrelated_structured_error_code_does_not_retry() -> None:
    provider = _provider(entity_name="unrelated-owner")
    client = _ScriptedNonStreamingClient([
        lambda: (_ for _ in ()).throw(
            StructuredServiceError("different provider failure", body={"code": "content_filter"})
        )
    ])
    entity = AgentEntity(
        NonStreamingAgent(client=client, name="unrelated-owner", default_options={"store": True}, context_providers=[]),
        state_provider=provider,
    )

    response = await entity.run(_request("unrelated-corr", "question"))

    assert response.additional_properties["durable_status"] == "error"
    assert _error_codes(response) == ["StructuredServiceError"]
    assert len(client.received_messages) == 1
    assert AgentSession.from_dict(_session_payload(provider)).service_session_id == "saved-service-id"


async def test_wrapped_retry_cause_equal_to_the_original_missing_response_error_does_not_continue_retrying() -> None:
    provider = _provider(entity_name="wrapped-owner")
    original = cast("StructuredServiceError", _missing_previous_response("first rejection"))

    def _first() -> ChatResponse:
        raise original

    def _second() -> ChatResponse:
        raise RuntimeError("wrapped retry failure") from original

    client = _ScriptedNonStreamingClient([_first, _second])
    entity = AgentEntity(
        NonStreamingAgent(client=client, name="wrapped-owner", default_options={"store": True}, context_providers=[]),
        state_provider=provider,
    )

    response = await entity.run(_request("wrapped-corr", "question"))

    assert response.additional_properties["durable_status"] == "error"
    assert _error_codes(response) == ["RuntimeError"]
    assert len(client.received_messages) == 2


async def test_stale_implicit_context_from_the_original_missing_response_error_does_not_continue_retrying() -> None:
    provider = _provider(entity_name="context-owner")
    original = cast("StructuredServiceError", _missing_previous_response("first rejection"))

    def _first() -> ChatResponse:
        raise original

    def _second() -> ChatResponse:
        try:
            raise original
        except StructuredServiceError:
            raise RuntimeError("stale implicit context")  # noqa: B904 - intentionally test implicit exception context

    client = _ScriptedNonStreamingClient([_first, _second])
    entity = AgentEntity(
        NonStreamingAgent(client=client, name="context-owner", default_options={"store": True}, context_providers=[]),
        state_provider=provider,
    )

    response = await entity.run(_request("context-corr", "question"))

    assert response.additional_properties["durable_status"] == "error"
    assert _error_codes(response) == ["RuntimeError"]
    assert len(client.received_messages) == 2


@pytest.mark.parametrize("wrapped", [False, True], ids=["direct-client", "delegated-client"])
async def test_tool_progress_blocks_retry_after_a_real_tool_side_effect(wrapped: bool) -> None:
    provider = _provider(entity_name="tool-owner")
    inner = _RejectAfterToolClient()
    client: Any = _DelegatingClient(inner) if wrapped else inner
    entity = AgentEntity(
        NonStreamingAgent(
            client=client,
            name="tool-owner",
            default_options={"store": True},
            context_providers=[_ToolProvider("tool-provider")],
        ),
        state_provider=provider,
    )

    response = await entity.run(_request("tool-corr", "use lookup"))

    assert response.additional_properties["durable_status"] == "error"
    assert _error_codes(response) == ["StructuredServiceError"]
    assert _TOOL_EFFECTS == ["durable"]
    assert len(inner.received_messages) == 2
    assert any(
        content.type == "function_result" for message in inner.received_messages[1] for content in message.contents
    )


@pytest.mark.parametrize("wrapped", [False, True], ids=["direct-client", "delegated-client"])
async def test_disabled_tools_are_a_no_side_effect_positive_control_even_through_wrappers(wrapped: bool) -> None:
    provider = _provider(entity_name="disabled-owner")
    inner = ToolChatClient()
    client: Any = _DelegatingClient(inner) if wrapped else inner
    entity = AgentEntity(
        NonStreamingAgent(
            client=client,
            name="disabled-owner",
            default_options={"store": True},
            context_providers=[_ToolProvider("tool-provider")],
        ),
        state_provider=provider,
    )

    response = await entity.run(_request("disabled-corr", "use lookup", enable_tool_calls=False))

    assert _TOOL_EFFECTS == []
    if not wrapped:
        # The direct Core invocation layer is disabled, so the proposed call is
        # returned without a tool execution or a second model request.
        assert len(inner.received_messages) == 1
        assert any(content.type == "function_call" for m in response.messages for content in m.contents)
        return
    assert response.text == "answer-2"
    assert len(inner.received_messages) == 2
    function_results = [
        content
        for message in inner.received_messages[1]
        for content in message.contents
        if content.type == "function_result"
    ]
    assert len(function_results) == 1
    assert function_results[0].result == "Tool execution is disabled for this invocation."
