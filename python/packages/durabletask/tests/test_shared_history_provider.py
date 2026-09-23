# Copyright (c) Microsoft. All rights reserved.

"""Focused tests for the extracted shared-history provider without a real host."""

from __future__ import annotations

from collections.abc import AsyncIterable, Awaitable, Mapping, Sequence
from copy import deepcopy
from typing import Any, cast

import pytest
from _shared_history_test_support import (
    OLD,
    OrdinaryExternalHistory,
    _bound,
    _CanonicalStateProvider,
    _history_ids,
    _history_texts,
    _PassiveChatClient,
    _request,
    _stored,
)
from agent_framework import (
    Agent,
    AgentSession,
    BaseChatClient,
    ChatMiddlewareLayer,
    ChatResponse,
    ChatResponseUpdate,
    ContextProvider,
    FunctionInvocationLayer,
    InMemoryHistoryProvider,
    Message,
    ResponseStream,
    SessionContext,
    tool,
)

from agent_framework_durabletask._history_provider import (
    WORKING_BUFFER_KEY,
    DurableHistoryProvider,
    current_durable_history_binding,
    prepare_history_owner,
    validate_history_providers,
)
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._shared_agent_state import (
    DurableAgentStateFunctionResultContent,
    DurableAgentStateResponse,
)

PROMPT = "Use lookup for durable."


class ToolChatClient(FunctionInvocationLayer, ChatMiddlewareLayer, BaseChatClient):
    """Minimal real core invocation client for tool-loop history tests."""

    STORES_BY_DEFAULT = False

    def __init__(self, *, fail_on_call: int | None = None) -> None:
        super().__init__(middleware=[])
        self.fail_on_call = fail_on_call
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
        self.received_messages.append(deepcopy(list(messages)))
        self.received_options.append(dict(options))
        call = len(self.received_messages)
        if call == self.fail_on_call:
            raise RuntimeError("model failed before history persistence")
        contents = (
            [{"type": "function_call", "call_id": "call-1", "name": "lookup", "arguments": '{"key":"durable"}'}]
            if call == 1
            else ["answer-2"]
        )
        response = ChatResponse(
            messages=[Message("assistant", contents)],
            response_id=f"response-{call}",
            finish_reason="tool_calls" if call == 1 else "stop",
        )
        if stream:

            async def updates() -> AsyncIterable[ChatResponseUpdate]:
                yield ChatResponseUpdate(
                    role="assistant",
                    contents=response.messages[0].contents,
                    response_id=response.response_id,
                    finish_reason=response.finish_reason,
                )

            return ResponseStream(updates(), finalizer=ChatResponse.from_updates)

        async def get() -> ChatResponse:
            return response

        return get()


class ProviderTools(ContextProvider):
    async def before_run(self, *, context: SessionContext, **kwargs: Any) -> None:
        context.tools.append(lookup)


@tool(name="lookup", approval_mode="never_require")
def lookup(key: str) -> str:
    return f"value:{key}"


def _response(correlation_id: str, *messages: Any) -> Any:
    return DurableAgentStateResponse(correlation_id, OLD, list(messages))


def _tool_result_count(provider: _CanonicalStateProvider) -> int:
    return sum(
        1
        for entry in provider.state.data.conversation_history
        for message in entry.messages
        for content in message.contents
        if isinstance(content, DurableAgentStateFunctionResultContent)
    )


def test_validate_history_providers_rejects_multiple_primaries_but_allows_store_only_audit() -> None:
    validate_history_providers(
        Agent(client=_PassiveChatClient(), context_providers=[InMemoryHistoryProvider("audit", load_messages=False)])
    )

    agent = Agent(
        client=_PassiveChatClient(),
        context_providers=[InMemoryHistoryProvider("first"), InMemoryHistoryProvider("second")],
    )
    with pytest.raises(ValueError, match="load-enabled primary history provider"):
        validate_history_providers(agent)


async def test_skip_excluded_hides_context_but_never_physically_deletes_history() -> None:
    provider = _CanonicalStateProvider([
        _request("seed", _stored("visible", message_id="visible-id"), _stored("hidden", excluded=True)),
        _response("seed", _stored("answer", role="assistant", message_id="answer-id")),
    ])
    history = DurableHistoryProvider(skip_excluded=True)
    state: dict[str, Any] = {}

    with _bound(provider):
        loaded = await history.get_messages("session", state=state)

    assert [message.text for message in loaded] == ["visible", "answer"]
    assert [message.text for message in state[WORKING_BUFFER_KEY]] == ["visible", "hidden", "answer"]
    assert _history_texts(provider) == ["visible", "hidden", "answer"]


async def test_service_owner_true_false_true_preserves_external_primary_and_store_only_sink() -> None:
    primary = OrdinaryExternalHistory(store_context_messages=False)
    sink = InMemoryHistoryProvider("audit", load_messages=False)
    client = _PassiveChatClient(service_conversation_id="service-thread")
    agent = Agent(client=client, context_providers=[primary, sink])
    session = agent.create_session(session_id="external-session")
    providers = agent.context_providers

    for turn, service_owned in enumerate((True, False, True), start=1):
        prepared = prepare_history_owner(agent, service_owned)
        assert isinstance(prepared, Agent)
        if service_owned:
            assert prepared is not agent
            assert prepared.context_providers is not providers
            assert getattr(prepared.context_providers[0], "__wrapped__", None) is primary
            assert prepare_history_owner(prepared, True) is prepared
        else:
            assert prepared is agent
            assert prepared.context_providers[0] is primary
        await prepared.run(f"turn-{turn}", session=session, options={"store": service_owned})

    assert primary.calls == [("load", "external-session"), ("save", "external-session")]
    assert [message.text for message in primary.saved] == ["turn-2", "answer-2"]
    assert [message.text for message in session.state["audit"]["messages"]] == [
        "turn-1",
        "answer-1",
        "turn-2",
        "answer-2",
        "turn-3",
        "answer-3",
    ]
    assert agent.context_providers is providers and providers[0] is primary and providers[1] is sink


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
async def test_tool_loop_failure_finalizes_the_pending_tool_result_once_before_manual_persist(per_call: bool) -> None:
    provider = _CanonicalStateProvider()
    history = DurableHistoryProvider(skip_excluded=False)
    client = ToolChatClient(fail_on_call=2)
    agent = Agent(
        client=client,
        context_providers=[history, ProviderTools("provider-tools")],
        require_per_service_call_history_persistence=per_call,
    )
    session = AgentSession(session_id="revision-session")
    session.state["foreign"] = {"pending": [False, 0]}

    with _bound(provider, "corr-1"):
        with pytest.raises(RuntimeError, match="model failed before history persistence"):
            await agent.run(PROMPT, session=session)
        provider_state = cast("dict[str, Any]", session.state[history.source_id])
        assert provider.persist_count == 0
        history.finalize_failed_run(provider_state)
        history.flush(provider_state)
        provider.persist()
        assert current_durable_history_binding() is not None

    assert provider.persist_count == 1
    assert session.state["foreign"] == {"pending": [False, 0]}
    expected_tool_results = 1 if per_call else 0
    assert _history_texts(provider).count(PROMPT) == expected_tool_results
    assert _tool_result_count(provider) == expected_tool_results
    if per_call:
        assert _history_texts(provider)[-1] == ""
    else:
        assert _history_texts(provider) == []
    cold = provider.clone()
    with _bound(cold, "corr-1"):
        reloaded = await history.get_messages("session", state={})
    assert [message.text for message in reloaded].count(PROMPT) == expected_tool_results
    assert _tool_result_count(cold) == expected_tool_results


async def test_generated_internal_ids_survive_json_cold_reload_without_rewriting_public_ids() -> None:
    provider = _CanonicalStateProvider([
        _request("seed", _stored("question", message_id="shared"), _stored("follow-up", message_id="shared")),
        _response("seed", _stored("answer", role="assistant", message_id=None)),
    ])
    history = DurableHistoryProvider(skip_excluded=False)

    with _bound(provider):
        loaded = await history.get_messages("session", state={})

    assert [message.message_id for message in loaded] == ["shared", "shared", None]
    first_internal = _history_ids(provider)
    assert len({item for item in first_internal if item is not None}) == 3

    cold = provider.clone()
    with _bound(cold):
        await history.get_messages("session", state={})

    assert _history_ids(cold) == first_internal
    assert message_identity(loaded[0]) == message_identity(Message.from_dict(loaded[0].to_dict()))
