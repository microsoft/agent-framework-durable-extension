# Copyright (c) Microsoft. All rights reserved.

"""Terminal delivery versus local model history, using real core hooks and JSON storage."""

from __future__ import annotations

import json
from collections.abc import AsyncIterable, Awaitable, Mapping, Sequence
from copy import deepcopy
from datetime import datetime, timezone
from inspect import signature
from typing import Any, cast

import pytest
from agent_framework import (
    GROUP_ANNOTATION_KEY,
    GROUP_ID_KEY,
    Agent,
    AgentResponse,
    AgentResponseUpdate,
    AgentSession,
    BaseChatClient,
    ChatMiddlewareLayer,
    ChatResponse,
    ChatResponseUpdate,
    Content,
    ContextProvider,
    FunctionInvocationLayer,
    InMemoryHistoryProvider,
    Message,
    ResponseStream,
    SessionContext,
    SupportsAgentRun,
    annotate_message_groups,
    tool,
)
from pydantic import BaseModel, ValidationError

from agent_framework_durabletask import AgentEntity, AgentEntityStateProviderMixin, DurableHistoryProvider, RunRequest
from agent_framework_durabletask._callbacks import AgentCallbackContext
from agent_framework_durabletask._durable_agent_state import (
    DurableAgentState,
    DurableAgentStateErrorResponse,
    DurableAgentStateMessage,
    DurableAgentStateResponse,
)
from agent_framework_durabletask._history_provider import (
    POSITIONS_KEY,
    WORKING_BUFFER_KEY,
    DurableHistoryBinding,
    bind_durable_history,
    unbind_durable_history,
)
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._response_utils import is_terminal_agent_response, serialize_agent_response


def _json(value: Any) -> Any:
    return json.loads(json.dumps(value, allow_nan=False))


class _JsonState(AgentEntityStateProviderMixin):
    def __init__(self, raw: dict[str, Any] | None = None) -> None:
        self.raw = _json(raw or {})
        self.writes = 0

    def _get_state_dict(self) -> dict[str, Any]:
        return _json(self.raw)

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        self.raw = _json(state)
        self.writes += 1

    def _get_session_id_from_entity(self) -> str:
        return "terminal-review"


def _seed() -> dict[str, Any]:
    state = DurableAgentState()
    message = Message(
        "assistant",
        ["previous valid answer"],
        message_id="previous-answer",
        additional_properties={"provider_metadata": {"keep": [None, False, 7]}},
    )
    state.data.conversation_history.append(
        DurableAgentStateResponse(
            "previous", datetime(2026, 1, 1, tzinfo=timezone.utc), [DurableAgentStateMessage.from_chat_message(message)]
        )
    )
    state.data.session = AgentSession(session_id="terminal-review").to_dict()
    state.data.session["state"]["foreign"] = {"opaque": [None, False, {"keep": "original"}]}
    return _json(state.to_dict())


def _mailbox(provider: _JsonState, correlation: str) -> dict[str, Any]:
    return provider.raw["data"]["responseMailbox"][correlation]["response"]


def _request() -> dict[str, Any]:
    message = Message("user", ["first input"], message_id="input-id")
    return {
        "message": "first input",
        "correlationId": "first",
        "contextMessages": [message.to_dict()],
        "contextMessageIds": ["input-occurrence"],
    }


def _error_message() -> Message:
    return Message(
        "assistant",
        [Content.from_error(message="original model failure", error_code="model_error"), "original terminal text"],
        message_id="terminal-output",
        author_name="review",
        additional_properties={"provider_metadata": {"keep": [1, None, False]}},
    )


class _Count(BaseModel):
    count: int


class _LegacyAgent:
    name = "legacy-review"
    id = "legacy-review"
    description = None

    def __init__(self, response: AgentResponse[Any]) -> None:
        self.response = response
        self.inputs: list[list[Message]] = []

    # Deliberately no stream or **kwargs: exercise the existing signature fallback.
    async def run(self, messages: list[Message], *, options: Mapping[str, Any]) -> AgentResponse[Any]:
        self.inputs.append(deepcopy(messages))
        return self.response


@pytest.mark.parametrize("text", ['{"count":"not an integer"}', "not JSON", '{"count":7}'])
async def test_lazy_value_failure_is_mailbox_only_and_never_legacy_history(text: str) -> None:
    original = AgentResponse(messages=[Message("assistant", [text])], response_format=_Count)
    valid = text == '{"count":7}'
    assert not is_terminal_agent_response(original)
    if not valid:
        with pytest.raises(ValidationError):
            _ = deepcopy(original).value
    agent = _LegacyAgent(original)
    seed = _seed()
    provider = _JsonState(seed)
    request = RunRequest("first input", "first", response_format=_Count)

    response = await AgentEntity(cast(SupportsAgentRun, agent), state_provider=provider).run(request)

    assert provider.writes == 1 and len(agent.inputs) == 1
    if valid:
        assert response is original and response.value == _Count(count=7)
    else:
        assert response is not original
        assert response.additional_properties["durable_status"] == "error"
        assert response.messages[0].contents[0].error_code == "ValidationError"
        assert response.text.startswith("ValidationError:")
        assert original.text == text
    assert _mailbox(provider, "first") == _json(serialize_agent_response(response))
    entries = provider.raw["data"]["conversationHistory"]
    assert entries[0] == seed["data"]["conversationHistory"][0]
    assert [entry["$type"] for entry in entries[1:]] == (["request", "response"] if valid else ["request"])

    cold_provider = _JsonState(provider.raw)
    next_agent = _LegacyAgent(AgentResponse(messages=[Message("assistant", ["next answer"])]))
    cold = AgentEntity(cast(SupportsAgentRun, next_agent), state_provider=cold_provider)
    duplicate = await cold.run(request)
    assert serialize_agent_response(duplicate) == _mailbox(provider, "first")
    assert next_agent.inputs == [] and cold_provider.writes == 0
    await cold.run({"message": "second input", "correlationId": "second"})
    assert [message.text for message in next_agent.inputs[0]] == [
        "previous valid answer",
        "first input",
        *([text] if valid else []),
        "second input",
    ]
    assert not any(content.type == "error" for message in next_agent.inputs[0] for content in message.contents)
    assert _mailbox(cold_provider, "first") == _mailbox(provider, "first")


class _ScriptedClient(FunctionInvocationLayer, ChatMiddlewareLayer, BaseChatClient):
    def __init__(self, replies: Sequence[Message]) -> None:
        super().__init__(middleware=[])
        self.replies = deepcopy(list(replies))
        self.inputs: list[list[Message]] = []

    def _inner_get_response(
        self, *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        self.inputs.append(deepcopy(list(messages)))
        message = deepcopy(self.replies[len(self.inputs) - 1])
        response = ChatResponse(
            messages=[message],
            response_id=f"model-{len(self.inputs)}",
            usage_details={"input_token_count": 3, "output_token_count": 2},
            conversation_id="service-history" if options.get("store") else None,
            finish_reason="tool_calls" if any(c.type == "function_call" for c in message.contents) else "stop",
        )
        if stream:

            async def updates() -> AsyncIterable[ChatResponseUpdate]:
                yield ChatResponseUpdate(
                    role=cast(Any, message.role),
                    contents=message.contents,
                    message_id=message.message_id,
                    author_name=message.author_name,
                    additional_properties=deepcopy(message.additional_properties),
                    response_id=response.response_id,
                    conversation_id=response.conversation_id,
                    finish_reason=response.finish_reason,
                )
                assert response.usage_details is not None
                yield ChatResponseUpdate(contents=[Content.from_usage(response.usage_details)])

            return ResponseStream(updates(), finalizer=ChatResponse.from_updates)

        async def get() -> ChatResponse:
            return response

        return get()


class _NonStreamingAgent(Agent):
    def run(self, *args: Any, **kwargs: Any) -> Any:
        if kwargs.get("stream"):
            raise TypeError("stream is not supported")
        return super().run(*args, **kwargs)


class _ObservedHistory(DurableHistoryProvider):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(prune_excluded=False, **kwargs)
        self.responses: list[AgentResponse[Any]] = []
        self.buffers: list[list[Message]] = []

    async def after_run(self, *, context: SessionContext, state: dict[str, Any], **kwargs: Any) -> None:
        assert isinstance(context.response, AgentResponse), "core adapts per-call ChatResponse before the hook"
        self.responses.append(deepcopy(context.response))
        await super().after_run(context=context, state=state, **kwargs)
        self.buffers.append(deepcopy(state.get(WORKING_BUFFER_KEY, [])))


@pytest.mark.parametrize("per_call", [False, True])
@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("terminal", [False, True], ids=["successful-control", "terminal-error"])
async def test_real_core_terminal_outputs_are_not_replayed_after_json_reload(
    per_call: bool, stream: bool, terminal: bool
) -> None:
    output = _error_message() if terminal else Message("assistant", ["valid answer"], message_id="valid-output")
    before_output = deepcopy(output.to_dict())
    client = _ScriptedClient([output])
    history = _ObservedHistory()
    agent_type = Agent if stream else _NonStreamingAgent
    agent = agent_type(
        client=client, context_providers=[history], require_per_service_call_history_persistence=per_call
    )
    seed = _seed()
    provider = _JsonState(seed)
    request = _request()
    original_request = deepcopy(request)

    response = await AgentEntity(agent, state_provider=provider).run(request)

    assert is_terminal_agent_response(response) is terminal
    assert response.text == output.text and len(client.inputs) == 1 and provider.writes == 1
    assert [content.to_dict() for content in response.messages[0].contents] == [c.to_dict() for c in output.contents]
    assert output.to_dict() == before_output and request == original_request
    assert _mailbox(provider, "first") == _json(serialize_agent_response(response))
    entries = provider.raw["data"]["conversationHistory"]
    assert entries[0] == seed["data"]["conversationHistory"][0]
    assert [entry["$type"] for entry in entries[1:]] == ["request", "errorResponse" if terminal else "response"]
    saved_response = provider.state.data.conversation_history[-1]
    assert isinstance(saved_response, DurableAgentStateResponse) and saved_response.usage is not None
    assert saved_response.usage.to_usage_details()["input_token_count"] == 3
    assert len(history.responses) == 1
    assert [m.text for m in history.buffers[0]] == [
        "previous valid answer",
        "first input",
        *([] if terminal else [output.text]),
    ]
    assert provider.raw["data"]["session"]["state"] == seed["data"]["session"]["state"]
    message = Message.from_dict(request["contextMessages"][0])
    assert provider.raw["data"]["ingestedMessages"] == {"input-occurrence": [message_identity(message)]}

    cold_provider = _JsonState(provider.raw)
    cold_client = _ScriptedClient([Message("assistant", ["next answer"])])
    cold = AgentEntity(
        agent_type(client=cold_client, require_per_service_call_history_persistence=per_call),
        state_provider=cold_provider,
    )
    assert (await cold.run(request)).to_dict() == _mailbox(provider, "first")
    assert cold_client.inputs == [] and cold_provider.writes == 0
    await cold.run({"message": "second input", "correlationId": "second"})
    assert [m.text for m in cold_client.inputs[0]] == [
        "previous valid answer",
        "first input",
        *([] if terminal else [output.text]),
        "second input",
    ]
    assert not any(c.type == "error" for m in cold_client.inputs[0] for c in m.contents)
    assert _mailbox(cold_provider, "first") == _mailbox(provider, "first")


class _GroupAfter(ContextProvider):
    def __init__(self, source_id: str) -> None:
        super().__init__("last-after")
        self.history_source = source_id
        self.groups: dict[str, dict[str, Any]] = {}

    async def after_run(self, *, session: AgentSession, **kwargs: Any) -> None:
        buffer = session.state[self.history_source][WORKING_BUFFER_KEY]
        annotate_message_groups(buffer, force_reannotate=True)
        for message in buffer:
            if any(c.type in ("function_call", "function_result") for c in message.contents):
                message.additional_properties["last_after"] = {"keep": [None, False, 1]}
                self.groups[message.message_id] = deepcopy(message.additional_properties[GROUP_ANNOTATION_KEY])


@pytest.mark.parametrize("stream", [False, True])
async def test_later_terminal_call_keeps_prior_tool_pair_and_final_hook_group_metadata(stream: bool) -> None:
    tool_invocations: list[str] = []

    @tool(name="lookup", approval_mode="never_require")
    def lookup(key: str) -> str:
        """Return a deterministic lookup result."""
        tool_invocations.append(key)
        return f"value:{key}"

    call = Message(
        "assistant", [Content.from_function_call("call-1", "lookup", arguments='{"key":"kept"}')], message_id="call"
    )
    client = _ScriptedClient([call, _error_message()])
    history = _ObservedHistory()
    last_after = _GroupAfter(history.source_id)
    agent_type = Agent if stream else _NonStreamingAgent
    provider = _JsonState(_seed())
    response = await AgentEntity(
        agent_type(
            client=client,
            tools=[lookup],
            context_providers=[last_after, history],
            require_per_service_call_history_persistence=True,
        ),
        state_provider=provider,
    ).run(_request())

    assert is_terminal_agent_response(response) and response.text == "original terminal text"
    assert tool_invocations == ["kept"] and len(client.inputs) == 2
    assert len(history.responses) == 2
    assert not is_terminal_agent_response(history.responses[0]) and is_terminal_agent_response(history.responses[1])
    assert [entry.json_type for entry in provider.state.data.conversation_history[1:]] == [
        "request",
        "response",
        "request",
        "errorResponse",
    ]
    assert len(last_after.groups) == 2
    assert len({group[GROUP_ID_KEY] for group in last_after.groups.values()}) == 1
    assert not any(c.type == "error" for batch in history.buffers for m in batch for c in m.contents)
    assert _mailbox(provider, "first") == _json(serialize_agent_response(response))

    cold_client = _ScriptedClient([Message("assistant", ["next answer"])])
    cold_provider = _JsonState(provider.raw)
    await AgentEntity(
        agent_type(client=cold_client, require_per_service_call_history_persistence=True), state_provider=cold_provider
    ).run({"message": "second input", "correlationId": "second"})
    replayed = cold_client.inputs[0]
    assert [m.text for m in replayed] == ["previous valid answer", "first input", "", "", "second input"]
    calls = [c for m in replayed for c in m.contents if c.type == "function_call"]
    results = [c for m in replayed for c in m.contents if c.type == "function_result"]
    assert len(calls) == len(results) == 1 and calls[0].call_id == results[0].call_id == "call-1"
    assert results[0].result == "value:kept" and tool_invocations == ["kept"]
    for message in replayed:
        if message.message_id in last_after.groups:
            assert message.additional_properties[GROUP_ANNOTATION_KEY] == last_after.groups[message.message_id]
            assert message.additional_properties["last_after"] == {"keep": [None, False, 1]}
    assert _mailbox(cold_provider, "first") == _mailbox(provider, "first")


@pytest.mark.parametrize("per_call", [False, True])
@pytest.mark.parametrize("store_inputs", [False, True])
@pytest.mark.parametrize("store_outputs", [False, True])
@pytest.mark.parametrize("service_owned", [False, True])
async def test_terminal_core_hooks_respect_storage_flags_and_service_ownership(
    per_call: bool, store_inputs: bool, store_outputs: bool, service_owned: bool
) -> None:
    seed = _seed()
    provider = _JsonState(seed)
    history = _ObservedHistory(store_inputs=store_inputs, store_outputs=store_outputs)
    client = _ScriptedClient([_error_message()])
    agent = Agent(client=client, context_providers=[history], require_per_service_call_history_persistence=per_call)
    request = {**_request(), "options": {"store": service_owned}}

    response = await AgentEntity(agent, state_provider=provider).run(request)

    assert is_terminal_agent_response(response) and response.text == "original terminal text"
    entries = provider.raw["data"]["conversationHistory"]
    assert entries[0] == seed["data"]["conversationHistory"][0]
    expected = (
        [] if service_owned else (["request"] if store_inputs else []) + (["errorResponse"] if store_outputs else [])
    )
    assert [entry["$type"] for entry in entries[1:]] == expected
    assert bool(provider.raw["data"].get("ingestedMessages")) is (store_inputs and not service_owned)
    assert [m.text for m in client.inputs[0]] == ([] if service_owned else ["previous valid answer"]) + ["first input"]
    assert _mailbox(provider, "first") == _json(serialize_agent_response(response))
    assert provider.writes == 1


@pytest.mark.parametrize("kind", ["error", "already_completed", "tool-error", "approval", "success"])
@pytest.mark.parametrize("store_context", [False, True])
@pytest.mark.parametrize("context_sources", [None, set(), {"selected"}])
async def test_shared_classifier_and_context_masks_exclude_only_terminal_output_batches(
    kind: str, store_context: bool, context_sources: set[str] | None
) -> None:
    metadata = {"durable_status": kind} if kind in ("error", "already_completed") else {}
    messages = [Message("assistant", ["not structured JSON"])]
    if kind == "tool-error":
        messages = [
            Message("assistant", [Content.from_function_call("call", "lookup", arguments="{}")]),
            Message(
                "tool", [Content.from_function_result("call", result="failed"), Content.from_error(message="tool")]
            ),
            Message("assistant", ["recovered"]),
        ]
    elif kind == "approval":
        messages[0].contents.append(
            Content.from_function_approval_request(
                "approval", Content.from_function_call("call", "lookup", arguments="{}")
            )
        )
    response = AgentResponse(messages=messages, additional_properties=metadata)
    terminal = is_terminal_agent_response(response)
    assert terminal is (kind in ("error", "already_completed"))
    before = deepcopy(response.to_dict())
    provider = _JsonState(_seed())
    history = DurableHistoryProvider(
        store_context_messages=store_context, store_context_from=context_sources, prune_excluded=False
    )
    context = SessionContext(input_messages=[Message("user", ["accepted input"])])
    for source in ("selected", "other"):
        context.extend_messages(source, [Message("user", [f"context-{source}"])])
    context.extend_messages(history, [Message("assistant", ["must not duplicate own history"])])
    context._response = response
    state: dict[str, Any] = {}
    token = bind_durable_history(DurableHistoryBinding(provider, "current"))
    try:
        await history.after_run(agent=None, session=None, context=context, state=state)
        expected_inputs = [
            f"context-{source}"
            for source in ("selected", "other")
            if store_context and (context_sources is None or source in context_sources)
        ] + ["accepted input"]
        expected = ["previous valid answer", *expected_inputs, *([] if terminal else [m.text for m in messages])]
        assert [m.text for m in state[WORKING_BUFFER_KEY]] == expected
        entry = provider.state.data.conversation_history[-1]
        assert isinstance(entry, DurableAgentStateErrorResponse) is terminal
        assert len(state[POSITIONS_KEY]) == len(expected)
        snapshot = _json(provider.state.to_dict())
        history.flush(state)
        history.flush(state)
        assert provider.state.to_dict() == snapshot, "terminal outputs must not be resurrected as compaction entries"
        assert [m.text for m in await history.get_messages("terminal-review", state={})] == expected
        assert provider.writes == 0 and response.to_dict() == before
    finally:
        unbind_durable_history(token)


@pytest.mark.parametrize("working_state", [False, True])
async def test_aggregated_terminal_batch_does_not_erase_or_duplicate_prior_tool_pair(working_state: bool) -> None:
    provider = _JsonState(_seed())
    history = DurableHistoryProvider(prune_excluded=False)
    pair = [
        Message("assistant", [Content.from_function_call("call", "lookup", arguments="{}")], message_id="call"),
        Message("tool", [Content.from_function_result("call", result="kept")], message_id="result"),
    ]
    annotate_message_groups(pair, force_reannotate=True)
    original_pair = [deepcopy(m.to_dict()) for m in pair]
    state: dict[str, Any] | None = {} if working_state else None
    binding = DurableHistoryBinding(provider, "current")
    token = bind_durable_history(binding)
    try:
        history._append_messages(binding, pair, state=state, response=AgentResponse(messages=pair))
        prior_entry = deepcopy(provider.state.data.conversation_history[-1].to_dict())
        terminal = AgentResponse(messages=[*deepcopy(pair), _error_message()])
        history._append_messages(binding, terminal.messages, state=state, response=terminal)
        if state is not None:
            history.flush(state)
        assert isinstance(provider.state.data.conversation_history[-1], DurableAgentStateErrorResponse)
        assert provider.state.data.conversation_history[-2].to_dict() == prior_entry
        provider.persist_state()
    finally:
        unbind_durable_history(token)

    cold = _JsonState(provider.raw)
    token = bind_durable_history(DurableHistoryBinding(cold, "next"))
    try:
        replayed = await history.get_messages("terminal-review", state={})
    finally:
        unbind_durable_history(token)
    assert [m.message_id for m in replayed] == ["previous-answer", "call", "result"]
    assert [m.to_dict() for m in replayed[1:]] == original_pair
    assert [m.to_dict() for m in pair] == original_pair


@pytest.mark.parametrize("per_call", [False, True])
async def test_real_core_pending_approval_still_skips_lazy_typed_validation(per_call: bool) -> None:
    output = Message(
        "assistant",
        [
            "approval needed, not JSON",
            Content.from_function_approval_request(
                "approval", Content.from_function_call("call", "lookup", arguments="{}")
            ),
        ],
    )
    client = _ScriptedClient([output])
    provider = _JsonState()
    response = await AgentEntity(
        Agent(client=client, require_per_service_call_history_persistence=per_call), state_provider=provider
    ).run(RunRequest("first input", "first", response_format=_Count))

    assert len(client.inputs) == 1 and not is_terminal_agent_response(response)
    assert response.text == output.text and len(response.user_input_requests) == 1
    with pytest.raises(ValidationError):
        _ = deepcopy(response).value
    assert "value" not in _mailbox(provider, "first")
    assert [entry["$type"] for entry in provider.raw["data"]["conversationHistory"]] == ["request", "response"]


class _ExternalHistory(InMemoryHistoryProvider):
    """A custom primary keeps its own transcript semantics, including terminal messages."""


async def test_external_primary_is_not_rewritten_or_given_a_second_durable_transcript() -> None:
    seed = _seed()
    external = _ExternalHistory("external")
    history_message = Message("assistant", ["external prior"], message_id="external-prior")
    seed["data"]["session"]["state"]["external"] = {
        "messages": [history_message.to_dict()],
        "opaque": {"keep": [None, False, 1]},
    }
    provider = _JsonState(seed)
    client = _ScriptedClient([_error_message()])
    agent = Agent(client=client, context_providers=[external])
    entity = AgentEntity(agent, state_provider=provider)

    response = await entity.run(_request())

    assert entity.agent is agent and agent.context_providers == [external]
    assert response.text == "original terminal text"
    assert provider.raw["data"]["conversationHistory"] == seed["data"]["conversationHistory"]
    saved = AgentSession.from_dict(provider.raw["data"]["session"]).state["external"]
    assert saved["opaque"] == {"keep": [None, False, 1]}
    assert saved["messages"][0].to_dict() == history_message.to_dict()
    assert [m.text for m in saved["messages"]] == ["external prior", "first input", "original terminal text"]

    cold_client = _ScriptedClient([Message("assistant", ["next answer"])])
    cold_provider = _JsonState(provider.raw)
    await AgentEntity(
        Agent(client=cold_client, context_providers=[_ExternalHistory("external")]), state_provider=cold_provider
    ).run({"message": "second input", "correlationId": "second"})
    # No promise to filter an opaque primary: only DurableHistoryProvider owns the new policy.
    assert [m.text for m in cold_client.inputs[0]] == [
        "external prior",
        "first input",
        "original terminal text",
        "second input",
    ]
    assert provider.raw["data"]["conversationHistory"] == cold_provider.raw["data"]["conversationHistory"]
    assert _mailbox(cold_provider, "first") == _mailbox(provider, "first")


class _Callback:
    def __init__(self) -> None:
        self.responses: list[AgentResponse[Any]] = []
        self.contexts: list[AgentCallbackContext] = []
        self.updates: list[AgentResponseUpdate] = []

    async def on_streaming_response_update(self, update: AgentResponseUpdate, context: AgentCallbackContext) -> None:
        self.updates.append(update)

    async def on_agent_response(self, response: AgentResponse[Any], context: AgentCallbackContext) -> None:
        self.responses.append(response)
        self.contexts.append(context)
        response.messages[0].contents[0].text = "callback copy only"


async def test_nonstreaming_signature_fallback_and_final_callback_contract_are_unchanged() -> None:
    assert list(signature(_LegacyAgent.run).parameters) == ["self", "messages", "options"]
    original = AgentResponse(messages=[Message("assistant", ['{"count":7}'])], response_format=_Count)
    agent = _LegacyAgent(original)
    callback = _Callback()
    provider = _JsonState()

    response = await AgentEntity(cast(SupportsAgentRun, agent), callback=callback, state_provider=provider).run(
        RunRequest("first input", "first", response_format=_Count)
    )

    assert len(agent.inputs) == len(callback.responses) == 1 and callback.updates == []
    assert response is original and response.value == _Count(count=7)
    assert response.text == '{"count":7}' and callback.responses[0].text == "callback copy only"
    assert callback.responses[0] is not response and callback.responses[0]._response_format is _Count
    assert callback.contexts == [AgentCallbackContext("legacy-review", "first", "terminal-review", "first input")]
    assert _mailbox(provider, "first") == _json(serialize_agent_response(response))
