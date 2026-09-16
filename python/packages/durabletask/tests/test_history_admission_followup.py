# Copyright (c) Microsoft. All rights reserved.

"""Admission, empty public IDs, middleware composition and current envelope extras."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any, cast

import agent_framework as core
import pytest
from agent_framework import (
    Agent,
    ChatContext,
    ChatMiddleware,
    ChatResponse,
    ContextProvider,
    FunctionInvocationContext,
    FunctionMiddleware,
    Message,
    SessionContext,
    tool,
)
from test_execution_followup_review import _DelegatingClient, _ObservedAgent
from test_history_identity_acceptance import (
    _assert_no_private_fields,
    _assert_positions,
    _bound,
    _Probe,
    _projection,
    _rows,
    _seed_history,
    _wire,
)
from test_history_pipeline_revision import NonStreamingAgent, ToolChatClient
from test_revision_contract import JsonStateProvider

from agent_framework_durabletask import AgentEntity, DurableAgentState, DurableHistoryProvider, RunRequest
from agent_framework_durabletask._durable_agent_state import DurableAgentStateMessage
from agent_framework_durabletask._history_provider import WORKING_BUFFER_KEY, current_durable_history_binding
from agent_framework_durabletask._invocation_safety import DurableServiceAcceptance, DurableToolGuard
from agent_framework_durabletask._message_identity import message_identity

_RESERVED_EXTRAS = [
    pytest.param("originalMessageId", None, id="original-null"),
    pytest.param("originalMessageId", {}, id="original-object"),
    pytest.param("originalMessageId", "forged-public-id", id="original-string"),
    pytest.param("messageId", "forged-private-id", id="message-id"),
    pytest.param("authorName", "forged-author", id="author"),
    pytest.param("createdAt", "2026-01-01T00:00:00+00:00", id="created-at"),
    pytest.param("extensionData", {"forged": True}, id="extension-data"),
]


@pytest.mark.parametrize(("field", "value"), _RESERVED_EXTRAS)
@pytest.mark.parametrize("source", ["dict", "provided-request"])
@pytest.mark.parametrize("corrupt_index", [0, 1], ids=["first-element", "second-element"])
async def test_reserved_context_envelope_is_rejected_before_model_or_commit(
    field: str, value: Any, source: str, corrupt_index: int
) -> None:
    messages = [
        Message("user", [f"body-{index}"], message_id=f"public-{index}", author_name="caller") for index in range(2)
    ]
    context_json = _wire([message.to_dict() for message in messages])
    payload = {
        "message": "logging only",
        "correlationId": "invalid-envelope",
        "contextMessages": context_json,
        "contextMessageIds": ["occ-0", "occ-1"],
    }
    request: RunRequest | dict[str, Any]
    if source == "provided-request":
        # Mutate an already constructed request. Constructor-only checks are insufficient.
        request = RunRequest.from_dict(deepcopy(payload))
        assert request.context_messages is not None
        request.context_messages[corrupt_index][field] = deepcopy(value)
    else:
        request = payload
        context_json[corrupt_index][field] = deepcopy(value)
    before_request = deepcopy(request.to_dict() if isinstance(request, RunRequest) else request)
    before_context = deepcopy(context_json)
    client = ToolChatClient(tool_calls=False)
    provider = JsonStateProvider(_seed_history())
    before_raw = deepcopy(provider.raw)
    cached = provider.state
    before_cached = deepcopy(cached.to_dict())
    entity = AgentEntity(Agent(client=client), state_provider=provider)
    error: ValueError | None = None

    try:
        await entity.run(request)
    except ValueError as exc:
        error = exc

    assert client.received_messages == [], "reserved envelope data must fail admission, not the model run"
    assert provider.writes == 0, "admission failure must not commit even a failed completion"
    assert provider.raw == before_raw
    assert provider.state is cached and cached.to_dict() == before_cached
    assert "invalid-envelope" not in cached.data.completed_correlations
    assert "invalid-envelope" not in cached.data.response_mailbox
    assert (request.to_dict() if isinstance(request, RunRequest) else request) == before_request
    assert context_json == before_context
    assert error is not None and field in str(error)
    assert current_durable_history_binding() is None


@pytest.mark.parametrize("value", [{}, "forged-public-id"], ids=["object", "string"])
def test_output_original_message_id_extra_is_rejected_by_durable_serialization(value: Any) -> None:
    output = Message("assistant", ["model output"], message_id="real-output-id")
    output.originalMessageId = deepcopy(value)  # type: ignore[attr-defined]
    before = deepcopy(output.to_dict())
    assert "originalMessageId" in before, "the test must actually exercise a serialized top-level extra"
    with pytest.raises(ValueError, match="originalMessageId"):
        DurableAgentStateMessage.from_chat_message(output)
    assert output.to_dict() == before


class _OutputExtraClient(ToolChatClient):
    def __init__(self, value: Any) -> None:
        super().__init__(tool_calls=False, response_message_id="real-output-id")
        self.value = value
        self.output: Message | None = None

    async def _inner_get_response(
        self, *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
    ) -> ChatResponse:
        assert not stream
        response = await cast(
            Awaitable[ChatResponse],
            super()._inner_get_response(messages=messages, stream=False, options=options, **kwargs),
        )
        self.output = response.messages[0]
        self.output.originalMessageId = deepcopy(self.value)  # type: ignore[attr-defined]
        assert "originalMessageId" in self.output.to_dict()
        return response


@pytest.mark.parametrize("value", [{}, "forged-public-id"], ids=["object", "string"])
async def test_model_output_reserved_extra_cannot_commit_poisoned_history(value: Any) -> None:
    client = _OutputExtraClient(value)
    provider = JsonStateProvider()
    response = await AgentEntity(NonStreamingAgent(client=client), state_provider=provider).run({
        "message": "answer once",
        "correlationId": "output-extra",
    })
    assert len(client.received_messages) == 1 and client.output is not None
    assert response.additional_properties.get("durable_status") == "error" and "originalMessageId" in response.text
    assert provider.writes == 1, "a failed completion may be committed, but never the invalid output"
    rows = _rows(provider)
    assert all("originalMessageId" not in row and row.get("messageId") != "real-output-id" for row in rows)
    assert provider.raw["data"]["completedCorrelations"]["output-extra"]["outcome"] == "failed"
    restored = DurableAgentState.from_dict(_wire(provider.raw))
    assert restored.to_dict() == provider.raw
    delivered = restored.try_get_agent_response("output-extra")
    assert delivered is not None and delivered.to_dict() == response.to_dict()
    assert client.output.to_dict()["originalMessageId"] == value


async def test_reserved_names_remain_opaque_inside_additional_properties() -> None:
    opaque = {
        "originalMessageId": None,
        "messageId": {},
        "authorName": [False, ""],
        "createdAt": "business timestamp, not a durable timestamp",
        "extensionData": {"originalMessageId": {"nested": [None, False, 0]}},
    }
    message = Message("user", ["opaque metadata"], message_id="public-input", additional_properties=deepcopy(opaque))
    request = _projection("opaque-reserved", [message], ["opaque-occ"])
    before = deepcopy(request)
    client = ToolChatClient(tool_calls=False)
    provider = JsonStateProvider()
    response = await AgentEntity(Agent(client=client), state_provider=provider).run(request)
    assert response.text == "answer-1" and response.additional_properties.get("durable_status") != "error"
    assert provider.writes == 1 and request == before and message.additional_properties == opaque
    row = next(row for row in _rows(provider) if row["role"] == "user")
    assert row["extensionData"] == opaque and "originalMessageId" not in row
    cold = JsonStateProvider(_wire(provider.raw))
    history = DurableHistoryProvider(prune_excluded=False)
    with _bound(cold):
        loaded = await history.get_messages("revision-session", state={})
    assert loaded[0].message_id == "public-input" and loaded[0].additional_properties == opaque
    assert DurableAgentState.from_dict(provider.raw).to_dict() == provider.raw


async def _assert_empty_id_cold_reconciliation(provider: JsonStateProvider, texts: list[str]) -> None:
    history = DurableHistoryProvider(prune_excluded=False)
    state: dict[str, Any] = {}
    with _bound(provider):
        loaded = await history.get_messages("revision-session", state=state)
        users = [message for message in loaded if message.role == "user"]
        assert [message.message_id for message in users] == ["", ""]
        assert [message.text for message in users] == texts
        private_ids = [getattr(message, "_durable_history_id", None) for message in users]
        assert all(private_ids) and len(set(private_ids)) == 2
        # Copies of equal public IDs still target one exact occurrence during flush.
        state[WORKING_BUFFER_KEY] = deepcopy(state[WORKING_BUFFER_KEY])
        copied_users = [message for message in state[WORKING_BUFFER_KEY] if message.role == "user"]
        copied_users[1].additional_properties["second_only"] = True
        history.flush(state)
        snapshot = deepcopy(provider.state.to_dict())
        history.flush(state)
        assert provider.state.to_dict() == snapshot
        _assert_positions(provider, state)
    rows = [row for entry in snapshot["data"]["conversationHistory"] for row in entry["messages"]]
    users_raw = [row for row in rows if row["role"] == "user"]
    assert [row["messageId"] for row in users_raw] == private_ids
    assert [row["originalMessageId"] for row in users_raw] == ["", ""]
    assert "second_only" not in users_raw[0].get("extensionData", {})
    assert users_raw[1]["extensionData"]["second_only"] is True
    _assert_no_private_fields(snapshot)
    cold = JsonStateProvider(_wire(snapshot))
    client = ToolChatClient(tool_calls=False)
    response = await AgentEntity(Agent(client=client), state_provider=cold).run(
        _projection("cold-followup", [Message("user", ["third"], message_id="third-id")], ["third-occ"])
    )
    assert response.text == "answer-1"
    model_users = [message for message in client.received_messages[0] if message.role == "user"]
    assert [message.message_id for message in model_users] == ["", "", "third-id"]
    assert [message.text for message in model_users] == [*texts, "third"]
    cold_users = [row for row in _rows(cold) if row["role"] == "user"]
    assert [row["messageId"] for row in cold_users[:2]] == private_ids
    assert [row["originalMessageId"] for row in cold_users[:2]] == ["", ""]
    assert DurableAgentState.from_dict(cold.raw).to_dict() == cold.raw


async def test_two_new_empty_public_ids_survive_append_reconciliation_and_cold_model() -> None:
    messages = [Message("user", [text], message_id="") for text in ("first body", "different body")]
    assert [message.to_dict()["message_id"] for message in messages] == ["", ""]
    request = _projection("empty-inputs", messages, ["empty-0", "empty-1"])
    before = deepcopy(request)
    client = ToolChatClient(tool_calls=False)
    provider = JsonStateProvider()
    response = await AgentEntity(Agent(client=client), state_provider=provider).run(request)
    assert response.text == "answer-1" and provider.writes == 1
    assert [message.message_id for message in client.received_messages[0]] == ["", ""]
    assert request == before and [message.message_id for message in messages] == ["", ""]
    assert provider.raw["data"]["ingestedMessages"] == {
        f"empty-{index}": [message_identity(message)] for index, message in enumerate(messages)
    }
    await _assert_empty_id_cold_reconciliation(
        JsonStateProvider(_wire(provider.raw)), [message.text for message in messages]
    )


async def test_two_legacy_empty_ids_keep_empty_originals_and_distinct_cold_occurrences() -> None:
    # Legacy-shaped rows in an admitted v2 envelope isolate identity repair from schema migration.
    raw = DurableAgentState().to_dict()
    raw["data"]["conversationHistory"] = [
        {
            "$type": "request",
            "correlationId": "legacy-empty",
            "createdAt": "2026-01-01T00:00:00+00:00",
            "messages": [
                {"role": "user", "messageId": "", "contents": [{"$type": "text", "text": "equal body"}]}
                for _ in range(2)
            ],
        }
    ]
    before = deepcopy(raw)
    provider = JsonStateProvider(_wire(raw))
    await _assert_empty_id_cold_reconciliation(provider, ["equal body", "equal body"])
    assert raw == before and provider.raw == before and provider.writes == 0


class _PassChat(ChatMiddleware):
    def __init__(self) -> None:
        self.calls: list[ChatContext] = []

    async def process(self, context: ChatContext, call_next: Callable[[], Awaitable[None]]) -> None:
        self.calls.append(context)
        await call_next()


class _PassFunction(FunctionMiddleware):
    def __init__(self) -> None:
        self.calls: list[FunctionInvocationContext] = []

    async def process(self, context: FunctionInvocationContext, call_next: Callable[[], Awaitable[None]]) -> None:
        self.calls.append(context)
        await call_next()


# Core 1.13 uses a sequence at the Agent boundary. The newer bundle API also supports bare sources.
# Keep the list matrix on both versions and add the newer forms only when that API is exported.
_BUNDLE = getattr(core, "MiddlewareBundle", None)
_MIDDLEWARE_CASES = [(kind, "list") for kind in ("chat", "function", "both")]
if _BUNDLE is not None:
    _MIDDLEWARE_CASES += [("chat", "singleton"), ("function", "singleton")]
    _MIDDLEWARE_CASES += [(kind, "bundle") for kind in ("chat", "function", "both")]


def _middleware(kind: str, shape: str, chat: _PassChat, function: _PassFunction) -> tuple[Any, list[Any]]:
    caller_list: list[Any] = ([chat] if kind in ("chat", "both") else []) + (
        [function] if kind in ("function", "both") else []
    )
    if shape == "singleton":
        assert len(caller_list) == 1
        return caller_list[0], caller_list
    if shape == "bundle":
        assert _BUNDLE is not None
        return _BUNDLE(caller_list), caller_list
    return caller_list, caller_list


@pytest.mark.parametrize(("kind", "shape"), _MIDDLEWARE_CASES)
@pytest.mark.parametrize("stream", [False, True], ids=["non-streaming", "streaming"])
async def test_agent_middleware_preserves_completed_service_receipt_after_provider_failure(
    kind: str, shape: str, stream: bool
) -> None:
    chat, function = _PassChat(), _PassFunction()
    middleware, caller_list = _middleware(kind, shape, chat, function)
    caller_before = list(caller_list)
    probe = _Probe()
    probe.fail_at = "after"
    inner = ToolChatClient(tool_calls=False)
    wrapper = _DelegatingClient(inner)
    agent = _ObservedAgent(client=wrapper, streaming=stream, middleware=middleware, context_providers=[probe])
    registered_middleware = agent.middleware
    raw = _seed_history()
    raw["data"]["session"]["service_session_id"] = "prior-service"
    provider = JsonStateProvider(raw)
    message = Message("user", ["service input"], message_id="public-id")
    request = {**_projection("middleware-failed", [message], ["service-occ"]), "options": {"store": True}}
    before_request = deepcopy(request)

    response = await AgentEntity(agent, state_provider=provider).run(request)

    assert response.additional_properties["durable_status"] == "error" and "probe failed after model" in response.text
    assert len(inner.received_messages) == 1 and len(probe.responses) == 1
    assert len(chat.calls) == int(kind in ("chat", "both")) and function.calls == []
    assert caller_list == caller_before and agent.middleware is registered_middleware
    assert request == before_request and provider.writes == 1
    assert provider.raw["data"]["session"]["service_session_id"] == "service-thread"
    assert provider.raw["data"]["ingestedMessages"] == {
        "old-occurrence": ["old-fingerprint"],
        "service-occ": [message_identity(message)],
    }
    assert probe.accepted == [{("service-occ", message_identity(message))}]
    forwarded = wrapper.forwarded[0]["middleware"]
    assert isinstance(forwarded, list)
    assert sum(isinstance(item, DurableServiceAcceptance) for item in forwarded) == 1
    assert sum(isinstance(item, DurableToolGuard) for item in forwarded) == 1
    assert provider.raw["data"]["conversationHistory"] == raw["data"]["conversationHistory"]
    assert provider.raw["data"]["completedCorrelations"]["middleware-failed"]["outcome"] == "failed"
    cold = JsonStateProvider(_wire(provider.raw))
    calls = len(inner.received_messages)
    assert (await AgentEntity(agent, state_provider=cold).run(request)).to_dict() == response.to_dict()
    assert len(inner.received_messages) == calls and cold.writes == 0
    cold_probe = _Probe()
    cold_client = ToolChatClient(tool_calls=False)
    followup = await AgentEntity(Agent(client=cold_client, context_providers=[cold_probe]), state_provider=cold).run({
        **request,
        "correlationId": "middleware-next",
    })
    assert followup.text == "answer-1" and cold_probe.inputs == [[]]
    assert cold_client.received_options[0]["conversation_id"] == "service-thread"
    _assert_no_private_fields(cold.raw)


@pytest.mark.parametrize(("kind", "shape"), _MIDDLEWARE_CASES)
@pytest.mark.parametrize("enabled", [False, True], ids=["disabled", "enabled-control"])
async def test_agent_middleware_does_not_replace_delegated_tool_guard_or_duplicate_caller_middleware(
    kind: str, shape: str, enabled: bool
) -> None:
    calls: list[str] = []

    @tool(name="lookup", approval_mode="never_require")
    def lookup(key: str) -> str:
        calls.append(key)
        return f"value:{key}"

    class ProviderTools(ContextProvider):
        async def before_run(self, *, context: SessionContext, **kwargs: Any) -> None:
            context.tools.append(lookup)

    chat, function = _PassChat(), _PassFunction()
    middleware, caller_list = _middleware(kind, shape, chat, function)
    caller_before = list(caller_list)
    inner = ToolChatClient()
    wrapper = _DelegatingClient(inner)
    configuration = inner.function_invocation_configuration
    before_configuration = deepcopy(configuration)
    agent = _ObservedAgent(client=wrapper, middleware=middleware, context_providers=[ProviderTools("tools")])
    registered_middleware = agent.middleware
    provider = JsonStateProvider()
    request = {"message": "use lookup", "correlationId": "guard-composition", "enable_tool_calls": enabled}
    before_request = deepcopy(request)

    response = await AgentEntity(agent, state_provider=provider).run(request)

    assert response.text == "answer-2" and response.additional_properties.get("durable_status") != "error"
    assert len(inner.received_messages) == 2 and calls == (["durable"] if enabled else [])
    assert len(chat.calls) == (2 if kind in ("chat", "both") else 0)
    assert len(function.calls) == int(kind in ("function", "both"))
    assert caller_list == caller_before and agent.middleware is registered_middleware
    assert request == before_request and provider.writes == 1
    assert inner.function_invocation_configuration is configuration and configuration == before_configuration
    assert wrapper.inner_configurations == [before_configuration]
    assert len(wrapper.forwarded) == 1
    forwarded = wrapper.forwarded[0]["middleware"]
    assert isinstance(forwarded, list)
    guards = [item for item in forwarded if isinstance(item, DurableToolGuard)]
    assert len(guards) == 1 and guards[0].enabled is enabled
    assert guards[0].progress.function_started is enabled
    assert all(sum(item is original for item in forwarded) == 1 for original in caller_list)
    results = [
        content
        for message in inner.received_messages[1]
        for content in message.contents
        if content.type == "function_result"
    ]
    assert len(results) == 1 and results[0].call_id == "call-1"
    assert results[0].result == ("value:durable" if enabled else "Tool execution is disabled for this invocation.")
    assert current_durable_history_binding() is None


class _ReplaceFutureExtra(DurableHistoryProvider):
    def __init__(self) -> None:
        super().__init__(prune_excluded=False)
        self.transformed: list[Message] = []

    async def save_messages(
        self, session_id: str | None, messages: Sequence[Message], *, state: dict[str, Any] | None = None, **kwargs: Any
    ) -> None:
        copied = deepcopy(list(messages))
        for message in copied:
            if message.role == "user":
                message.future_message = "new"  # type: ignore[attr-defined]
                assert message.to_dict()["future_message"] == "new"
                self.transformed.append(message)
        await super().save_messages(session_id, copied, state=state, **kwargs)


async def test_save_override_current_unknown_extra_wins_over_original_raw_extra() -> None:
    raw = Message("user", ["unchanged body"], message_id="future-input").to_dict()
    raw["future_message"] = "old"
    request = {
        "message": "logging only",
        "correlationId": "future-extra",
        "contextMessages": [raw],
        "contextMessageIds": ["future-occ"],
    }
    before = deepcopy(request)
    history = _ReplaceFutureExtra()
    client = ToolChatClient(tool_calls=False)
    provider = JsonStateProvider()
    agent = NonStreamingAgent(client=client, context_providers=[history])
    response = await AgentEntity(agent, state_provider=provider).run(request)
    assert response.text == "answer-1" and len(history.transformed) == 1
    assert request == before and raw["future_message"] == "old"
    saved = next(row for row in _rows(provider) if row["role"] == "user")
    assert saved["future_message"] == "new", "current save transformations must override the raw optional-field bridge"
    assert saved["messageId"] == "future-input" and saved["contents"][0]["text"] == "unchanged body"
    cold = JsonStateProvider(_wire(provider.raw))
    state: dict[str, Any] = {}
    with _bound(cold):
        await history.get_messages("revision-session", state=state)
        history.flush(state)
        snapshot = deepcopy(cold.state.to_dict())
        history.flush(state)
        assert cold.state.to_dict() == snapshot
        _assert_positions(cold, state)
    assert snapshot["data"]["conversationHistory"][0]["messages"][0]["future_message"] == "new"
    assert DurableAgentState.from_dict(snapshot).to_dict() == snapshot
    _assert_no_private_fields(snapshot)
