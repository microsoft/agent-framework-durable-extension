# Copyright (c) Microsoft. All rights reserved.

"""Execution follow-ups through real core invocation and detached JSON entity storage."""

from __future__ import annotations

import json
from collections.abc import AsyncIterable, Awaitable, Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, cast

import pytest
from agent_framework import (
    Agent,
    AgentExecutor,
    AgentResponse,
    AgentResponseUpdate,
    AgentSession,
    ChatResponse,
    ChatResponseUpdate,
    Content,
    ContextProvider,
    FunctionInvocationLayer,
    Message,
    ResponseStream,
    SessionContext,
    SupportsAgentRun,
    WorkflowBuilder,
    tool,
)
from pydantic import BaseModel, ValidationError
from test_history_pipeline_revision import ToolChatClient
from test_revision_contract import JsonStateProvider

from agent_framework_durabletask import AgentEntity, DurableAgentState, DurableHistoryProvider, RunRequest
from agent_framework_durabletask import _entities as entities_module
from agent_framework_durabletask._callbacks import AgentCallbackContext
from agent_framework_durabletask._history_provider import (
    POSITIONS_KEY,
    WORKING_BUFFER_KEY,
    current_durable_history_binding,
)
from agent_framework_durabletask._invocation_safety import DurableToolGuard
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._response_utils import ensure_response_format, serialize_agent_response
from agent_framework_durabletask._state_migration import migrate_legacy_state, state_snapshot_digest
from agent_framework_durabletask._workflows.naming import workflow_message_id


def _object(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    assert all(isinstance(key, str) for key in value)
    return cast(dict[str, object], value)


def _array(value: object) -> list[object]:
    assert isinstance(value, list)
    return cast(list[object], value)


def _wire(value: object) -> dict[str, object]:
    return _object(json.loads(json.dumps(value, allow_nan=False)))


def _data(provider: JsonStateProvider) -> dict[str, object]:
    return _object(_wire(provider.raw)["data"])


def _mailbox(provider: JsonStateProvider, correlation: str) -> dict[str, object]:
    return _object(_object(_object(_data(provider)["responseMailbox"])[correlation])["response"])


def _delivered(provider: JsonStateProvider, correlation: str) -> AgentResponse[Any]:
    response = DurableAgentState.from_json(json.dumps(provider.raw)).try_get_agent_response(correlation)
    assert isinstance(response, AgentResponse)
    return response


class _ObservedAgent(Agent):
    """Observe the Agent.run boundary without replacing core execution."""

    def __init__(self, *, client: Any, streaming: bool = True, **kwargs: Any) -> None:
        super().__init__(client=client, **kwargs)
        self.streaming = streaming
        self.run_modes: list[bool] = []
        self.run_client_kwargs: list[dict[str, object]] = []
        self.sessions: list[AgentSession] = []

    def run(self, *args: Any, **kwargs: Any) -> Any:
        if kwargs.get("stream") and not self.streaming:
            raise TypeError("stream is not supported")
        self.run_modes.append(bool(kwargs.get("stream")))
        self.run_client_kwargs.append(dict(kwargs.get("client_kwargs") or {}))
        session = kwargs.get("session")
        assert isinstance(session, AgentSession)
        self.sessions.append(session)
        return super().run(*args, **kwargs)


class _DelegatingClient:
    """A non-invocation wrapper whose declared configuration belongs to its inner client."""

    def __init__(self, inner: ToolChatClient) -> None:
        self.inner = inner
        self.forwarded: list[dict[str, object]] = []
        self.inner_configurations: list[dict[str, object]] = []

    def __getattr__(self, name: str) -> object:
        # Do not delegate copy/pickle special methods or fabricate arbitrary attributes.
        if name in {"function_invocation_configuration", "additional_properties"}:
            return getattr(self.inner, name)
        raise AttributeError(name)

    def get_response(
        self, messages: Sequence[Message], *, stream: bool = False, **kwargs: Any
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        self.forwarded.append(dict(kwargs.get("client_kwargs") or {}))
        self.inner_configurations.append(dict(self.inner.function_invocation_configuration))
        # Crucially, this uses inner's configuration, not a shadow assigned to a wrapper copy.
        return cast(Callable[..., Any], self.inner.get_response)(messages=messages, stream=stream, **kwargs)


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
@pytest.mark.parametrize("stream", [False, True], ids=["non-streaming", "streaming"])
@pytest.mark.parametrize("enabled", [False, True], ids=["disabled", "enabled-control"])
async def test_tool_guard_reaches_delegated_core_invocation_without_mutating_configuration(
    per_call: bool, stream: bool, enabled: bool
) -> None:
    calls: list[str] = []

    @tool(name="lookup", approval_mode="never_require")
    def lookup(key: str) -> str:
        calls.append(key)
        return f"value:{key}"

    class ProviderTools(ContextProvider):
        async def before_run(self, *, context: SessionContext, **kwargs: Any) -> None:
            context.tools.append(lookup)

    inner = ToolChatClient()
    wrapper = _DelegatingClient(inner)
    assert not isinstance(wrapper, FunctionInvocationLayer)
    configuration = inner.function_invocation_configuration
    original_configuration = deepcopy(configuration)
    assert wrapper.function_invocation_configuration is configuration
    agent = _ObservedAgent(
        client=wrapper,
        streaming=stream,
        context_providers=[ProviderTools("provider-tools")],
        require_per_service_call_history_persistence=per_call,
    )
    defaults, providers = agent.default_options, agent.context_providers
    original_defaults = deepcopy(defaults)
    initial_session = AgentSession(session_id="revision-session", service_session_id="parked-service-id")
    initial_session.state["foreign"] = {"pending": ["keep"]}
    session_snapshot = deepcopy(initial_session.to_dict())
    initial = DurableAgentState()
    initial.data.session = initial_session.to_dict()
    provider = JsonStateProvider(_wire(initial.to_dict()))
    entity = AgentEntity(agent, state_provider=provider)
    registered = entity.agent
    request = {"message": "use lookup", "correlationId": "guard", "enable_tool_calls": enabled}
    before_request = deepcopy(request)

    response = await entity.run(request)

    assert response.text == "answer-2"
    assert len(inner.received_messages) == 2 and agent.run_modes == [stream]
    assert calls == (["durable"] if enabled else [])
    assert len(wrapper.forwarded) == 1
    guards = [item for item in _array(agent.run_client_kwargs[0]["middleware"]) if isinstance(item, DurableToolGuard)]
    assert len(guards) == 1 and guards[0].enabled is enabled
    assert guards[0] in _array(wrapper.forwarded[0]["middleware"])
    assert guards[0].progress.function_started is enabled
    assert wrapper.forwarded[0]["session"] is agent.sessions[0]
    assert wrapper.inner_configurations == [original_configuration]
    results = [
        content
        for message in inner.received_messages[1]
        for content in message.contents
        if content.type == "function_result"
    ]
    assert len(results) == 1 and results[0].call_id == "call-1"
    assert results[0].result == ("value:durable" if enabled else "Tool execution is disabled for this invocation.")
    if not enabled:
        assert inner.received_options[0]["tool_choice"] == "none"
    assert inner.function_invocation_configuration is configuration
    assert configuration == original_configuration
    assert wrapper.function_invocation_configuration is configuration
    assert agent.default_options is defaults and defaults == original_defaults
    assert agent.context_providers is providers and agent.client is wrapper
    assert entity.agent is registered and request == before_request
    assert initial_session.to_dict() == session_snapshot
    assert agent.sessions[0] is not initial_session
    saved_session = _object(_data(provider)["session"])
    assert saved_session["service_session_id"] == "parked-service-id"
    assert _object(saved_session["state"])["foreign"] == {"pending": ["keep"]}
    assert _delivered(provider, "guard").to_dict() == response.to_dict()
    assert current_durable_history_binding() is None


class _PreviousResponseMissing(RuntimeError):
    code = "previous_response_not_found"


class _VisibilityClient(ToolChatClient):
    """Fail a real model boundary, with a successful third call available to expose restarts."""

    def __init__(self, *, tool_followup: bool = False, partial: bool = False) -> None:
        super().__init__(tool_calls=tool_followup)
        self.tool_followup = tool_followup
        self.partial = partial
        self.failure = _PreviousResponseMissing("service parent is not visible")

    def _inner_get_response(
        self, *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        call = len(self.received_messages) + 1
        failing_call = 2 if self.tool_followup else 1
        if call != failing_call:
            return super()._inner_get_response(messages=messages, stream=stream, options=options, **kwargs)
        self.received_messages.append(deepcopy(list(messages)))
        self.received_options.append(dict(options))
        if stream:

            async def updates() -> AsyncIterable[ChatResponseUpdate]:
                if self.partial:
                    # No conversation ID: this case must be stopped by output progress alone.
                    yield ChatResponseUpdate(role="assistant", contents=[Content.from_text("partial visible answer")])
                raise self.failure

            return ResponseStream(updates(), finalizer=ChatResponse.from_updates)

        async def get() -> ChatResponse:
            raise self.failure

        return get()


class _CallbackRecorder:
    def __init__(self) -> None:
        self.updates: list[AgentResponseUpdate] = []
        self.responses: list[AgentResponse[Any]] = []

    async def on_streaming_response_update(self, update: AgentResponseUpdate, context: AgentCallbackContext) -> None:
        self.updates.append(update)

    async def on_agent_response(self, response: AgentResponse[Any], context: AgentCallbackContext) -> None:
        self.responses.append(response)


def _service_state() -> dict[str, object]:
    state = DurableAgentState()
    state.data.session = AgentSession(
        session_id="revision-session", service_session_id="original-service-parent"
    ).to_dict()
    return _wire(state.to_dict())


def _assert_missing_error(provider: JsonStateProvider, response: AgentResponse[Any], correlation: str) -> None:
    assert response.additional_properties["durable_status"] == "error"
    assert response.text == "_PreviousResponseMissing: service parent is not visible"
    errors = [content for message in response.messages for content in message.contents if content.type == "error"]
    assert len(errors) == 1 and errors[0].error_code == "_PreviousResponseMissing"
    assert _delivered(provider, correlation).to_dict() == response.to_dict()
    assert correlation in _object(_data(provider)["completedCorrelations"])
    assert provider.writes == 1


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
@pytest.mark.parametrize("stream", [False, True], ids=["non-streaming", "streaming"])
async def test_missing_parent_on_tool_followup_does_not_restart_the_agent(
    monkeypatch: pytest.MonkeyPatch, per_call: bool, stream: bool
) -> None:
    monkeypatch.setattr(entities_module, "_REJECTED_ID_BACKOFF_SECONDS", 0)
    calls: list[str] = []

    @tool(name="lookup", approval_mode="never_require")
    def lookup(key: str) -> str:
        calls.append(key)
        return f"value:{key}"

    client = _VisibilityClient(tool_followup=True)
    agent = _ObservedAgent(
        client=_DelegatingClient(client),
        streaming=stream,
        tools=[lookup],
        default_options={"store": True},
        require_per_service_call_history_persistence=per_call,
    )
    provider = JsonStateProvider(_service_state())
    request = {"message": "use lookup", "correlationId": "failed-followup"}

    response = await AgentEntity(agent, state_provider=provider).run(request)

    assert len(client.received_messages) == 2, "a successful third model call must not hide the follow-up error"
    assert agent.run_modes == [stream], "retrying the whole Agent.run would restart the tool loop"
    assert calls == ["durable"]
    assert [options.get("conversation_id") for options in client.received_options] == [
        "original-service-parent",
        "service-thread",
    ]
    results = [
        content
        for message in client.received_messages[1]
        for content in message.contents
        if content.type == "function_result"
    ]
    assert len(results) == 1 and results[0].call_id == "call-1" and results[0].result == "value:durable"
    assert agent.sessions[0].service_session_id == "service-thread", "the first response advanced the session"
    assert _object(_data(provider)["session"])["service_session_id"] == "service-thread"
    _assert_missing_error(provider, response, "failed-followup")
    cold_provider = JsonStateProvider(_wire(provider.raw))
    duplicate = await AgentEntity(agent, state_provider=cold_provider).run(request)
    assert duplicate.to_dict() == response.to_dict() and cold_provider.writes == 0
    assert len(client.received_messages) == 2 and calls == ["durable"] and agent.run_modes == [stream]


async def test_partial_stream_missing_parent_is_not_retryable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(entities_module, "_REJECTED_ID_BACKOFF_SECONDS", 0)
    client = _VisibilityClient(partial=True)
    agent = _ObservedAgent(client=client, default_options={"store": True})
    callback = _CallbackRecorder()
    provider = JsonStateProvider(_service_state())

    response = await AgentEntity(agent, callback=callback, state_provider=provider).run({
        "message": "continue",
        "correlationId": "partial-stream",
    })

    assert [update.text for update in callback.updates] == ["partial visible answer"]
    assert callback.responses == []
    assert agent.sessions[0].service_session_id == "original-service-parent"
    assert len(client.received_messages) == 1 and agent.run_modes == [True]
    _assert_missing_error(provider, response, "partial-stream")


@pytest.mark.parametrize("stream", [False, True], ids=["non-streaming", "streaming"])
async def test_zero_output_first_request_missing_parent_still_retries_identically(
    monkeypatch: pytest.MonkeyPatch, stream: bool
) -> None:
    monkeypatch.setattr(entities_module, "_REJECTED_ID_BACKOFF_SECONDS", 0)
    client = _VisibilityClient()
    agent = _ObservedAgent(client=client, streaming=stream, default_options={"store": True})
    provider = JsonStateProvider(_service_state())
    callback = _CallbackRecorder()

    response = await AgentEntity(agent, callback=callback, state_provider=provider).run({
        "message": "continue",
        "correlationId": "zero-output",
    })

    assert response.text == "answer-2" and response.additional_properties.get("durable_status") != "error"
    assert len(client.received_messages) == 2 and agent.run_modes == [stream, stream]
    assert client.received_options[0] == client.received_options[1]
    assert client.received_options[0]["conversation_id"] == "original-service-parent"
    assert [[message.to_dict() for message in batch] for batch in client.received_messages] == [
        [message.to_dict() for message in client.received_messages[0]]
    ] * 2
    assert agent.sessions[0] is agent.sessions[1]
    assert len(callback.responses) == 1
    assert len(callback.updates) == int(stream)
    assert all(update.text == "answer-2" for update in callback.updates)
    assert _delivered(provider, "zero-output").text == "answer-2" and provider.writes == 1


class _NestedValue(BaseModel):
    values: list[int]


class ReviewValue(BaseModel):
    nested: _NestedValue


@dataclass
class _SDKPayload:
    labels: list[str]


class _UncopyableSDK:
    def __deepcopy__(self, memo: dict[int, object]) -> _UncopyableSDK:
        raise TypeError("opaque SDK handle cannot be copied")


class _ReplyAgent:
    """Custom non-pipeline agent for responses that should not be interpreted as core runs."""

    name = "custom-response"
    id = "custom-response"
    description = None

    def __init__(self, response: AgentResponse[Any]) -> None:
        self.response = response
        self.inputs: list[list[Message]] = []

    def create_session(self, **kwargs: Any) -> AgentSession:
        return AgentSession(**kwargs)

    async def run(self, messages: list[Message], **kwargs: Any) -> AgentResponse[Any]:
        self.inputs.append(deepcopy(messages))
        return self.response


class _TypedMutatingCallback(_CallbackRecorder):
    def __init__(self) -> None:
        super().__init__()
        self.mutations: list[str] = []

    async def on_agent_response(self, response: AgentResponse[Any], context: AgentCallbackContext) -> None:
        self.responses.append(response)
        value = response.value
        if isinstance(value, ReviewValue):
            value.nested.values.append(99)
            self.mutations.append("typed-value")
        response.messages[0].contents[0].text = "callback changed text"
        response.messages[0].contents[0].additional_properties["source"]["labels"].append("callback")
        self.mutations.append("content")
        if isinstance(response.raw_representation, _SDKPayload):
            response.raw_representation.labels.append("callback")
            self.mutations.append("raw")


@pytest.mark.parametrize("lazy", [False, True], ids=["already-typed", "lazy-typed"])
@pytest.mark.parametrize("opaque", [False, True], ids=["copyable-sdk", "uncopyable-sdk"])
async def test_final_callback_keeps_model_format_and_detaches_value_content_and_sdk(lazy: bool, opaque: bool) -> None:
    model = ReviewValue(nested=_NestedValue(values=[1, 2]))
    sdk = _UncopyableSDK() if opaque else _SDKPayload(["original"])
    original: AgentResponse[Any] = AgentResponse(
        messages=[
            Message(
                "assistant",
                [
                    Content.from_text(
                        model.model_dump_json(), additional_properties={"source": {"labels": ["original"]}}
                    )
                ],
            )
        ],
        value=None if lazy else model,
        response_format=ReviewValue,
        raw_representation=sdk,
    )
    expected = _wire(serialize_agent_response(original))
    agent = _ReplyAgent(original)
    callback = _TypedMutatingCallback()
    provider = JsonStateProvider()
    request = RunRequest("return typed output", "typed-callback", response_format=ReviewValue)

    response = await AgentEntity(cast(SupportsAgentRun, agent), callback=callback, state_provider=provider).run(request)

    # Callback exceptions are swallowed by the host, so verify completed mutations outside it.
    assert callback.mutations == ["typed-value", "content"] + ([] if opaque else ["raw"])
    assert len(callback.responses) == 1 and callback.updates == []
    snapshot = callback.responses[0]
    assert snapshot is not original and response is original
    assert isinstance(snapshot.value, ReviewValue) and isinstance(original.value, ReviewValue)
    assert snapshot.value is not original.value and snapshot.value.nested is not original.value.nested
    assert snapshot.value.nested.values == [1, 2, 99]
    assert snapshot._response_format is ReviewValue
    assert original._response_format is ReviewValue
    assert original.value.nested.values == [1, 2]
    assert snapshot.messages[0].contents[0] is not original.messages[0].contents[0]
    assert original.messages[0].contents[0].additional_properties == {"source": {"labels": ["original"]}}
    assert original.raw_representation is sdk
    if opaque:
        # Only an uncopyable opaque SDK field may be omitted, never the rest of the callback response.
        assert snapshot.raw_representation is None
    else:
        assert isinstance(sdk, _SDKPayload) and sdk.labels == ["original"]
        assert isinstance(snapshot.raw_representation, _SDKPayload)
        assert snapshot.raw_representation is not sdk and snapshot.raw_representation.labels == ["original", "callback"]
    assert _wire(serialize_agent_response(original)) == expected
    assert _mailbox(provider, "typed-callback") == expected
    assert "raw_representation" not in expected and "response_format" not in expected
    delivered = _delivered(provider, "typed-callback")
    ensure_response_format(ReviewValue, "typed-callback", delivered)
    assert delivered.value == ReviewValue(nested=_NestedValue(values=[1, 2]))
    before = _wire(provider.raw)
    snapshot.value.nested.values.append(101)
    snapshot.messages[0].contents[0].text = "late callback mutation"
    assert _wire(provider.raw) == before and original.value.nested.values == [1, 2]
    cold_provider = JsonStateProvider(before)
    duplicate = await AgentEntity(cast(SupportsAgentRun, agent), callback=callback, state_provider=cold_provider).run(
        request
    )
    assert _wire(serialize_agent_response(duplicate)) == expected
    assert len(agent.inputs) == 1 and len(callback.responses) == 1 and cold_provider.writes == 0


async def test_custom_terminal_text_skips_typed_validation_and_is_not_replayed_next_turn() -> None:
    original = AgentResponse(
        messages=[Message("assistant", ["original terminal text, not JSON"])],
        response_format=ReviewValue,
        additional_properties={"durable_status": "error", "provider_detail": {"labels": ["keep"]}},
    )
    # This is a genuinely invalid lazy value, not an inert format marker.
    with pytest.raises(ValidationError):
        _ = deepcopy(original).value
    agent = _ReplyAgent(original)
    provider = JsonStateProvider()
    request = RunRequest("first input", "terminal", response_format=ReviewValue)

    response = await AgentEntity(cast(SupportsAgentRun, agent), state_provider=provider).run(request)

    assert response is original and response.text == "original terminal text, not JSON"
    delivered = _delivered(provider, "terminal")
    assert delivered.text == original.text and delivered.additional_properties == original.additional_properties
    assert delivered.value is None
    assert all(content.type == "text" for message in delivered.messages for content in message.contents)
    assert "value" not in _mailbox(provider, "terminal")
    assert all(_object(entry)["$type"] == "request" for entry in _array(_data(provider)["conversationHistory"]))
    cold_provider = JsonStateProvider(_wire(provider.raw))
    cold = AgentEntity(cast(SupportsAgentRun, agent), state_provider=cold_provider)
    assert (await cold.run(request)).to_dict() == delivered.to_dict()
    assert len(agent.inputs) == 1 and cold_provider.writes == 0
    agent.response = AgentResponse(messages=[Message("assistant", ["next answer"])])
    assert (await cold.run({"message": "second input", "correlationId": "next"})).text == "next answer"
    assert [message.text for message in agent.inputs[1]] == ["first input", "second input"]
    assert _mailbox(cold_provider, "terminal") == _mailbox(provider, "terminal")


class _MessageClient(ToolChatClient):
    def __init__(self, response_message: Message) -> None:
        super().__init__(tool_calls=False)
        self.response_message = response_message

    def _inner_get_response(
        self, *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        self.received_messages.append(deepcopy(list(messages)))
        self.received_options.append(dict(options))
        message = deepcopy(self.response_message)
        if stream:

            async def updates() -> AsyncIterable[ChatResponseUpdate]:
                yield ChatResponseUpdate(
                    role=cast(Any, message.role),
                    contents=message.contents,
                    message_id=message.message_id,
                    additional_properties=message.additional_properties,
                )

            return ResponseStream(updates(), finalizer=ChatResponse.from_updates)

        async def get() -> ChatResponse:
            return ChatResponse(messages=[message])

        return get()


@pytest.mark.parametrize("stream", [False, True], ids=["non-streaming", "streaming"])
async def test_core_user_input_request_precedes_lazy_typed_parsing(stream: bool) -> None:
    client = _MessageClient(
        Message(
            "assistant",
            [
                Content.from_text("Approval required, not a typed JSON answer"),
                Content.from_function_approval_request(
                    "approval-1", Content.from_function_call("call-1", "lookup", arguments={"key": "durable"})
                ),
            ],
        )
    )
    agent = _ObservedAgent(client=client, streaming=stream)
    provider = JsonStateProvider()
    request = RunRequest("request approval", "approval", response_format=ReviewValue)

    response = await AgentEntity(agent, state_provider=provider).run(request)

    assert len(client.received_messages) == 1
    assert response.additional_properties.get("durable_status") != "error"
    assert response.text == "Approval required, not a typed JSON answer"
    assert len(response.user_input_requests) == 1 and response.user_input_requests[0].id == "approval-1"
    with pytest.raises(ValidationError):
        _ = deepcopy(response).value
    assert "value" not in _mailbox(provider, "approval")
    delivered = _delivered(provider, "approval")
    ensure_response_format(ReviewValue, "approval", delivered)
    assert delivered.value is None and delivered.text == response.text
    assert delivered.user_input_requests[0].to_dict() == response.user_input_requests[0].to_dict()
    cold_provider = JsonStateProvider(_wire(provider.raw))
    assert (await AgentEntity(agent, state_provider=cold_provider).run(request)).to_dict() == delivered.to_dict()
    assert len(client.received_messages) == 1 and cold_provider.writes == 0


class _Inputs(ContextProvider):
    def __init__(self) -> None:
        super().__init__("input-probe")
        self.inputs: list[list[Message]] = []

    async def before_run(self, *, context: SessionContext, **kwargs: Any) -> None:
        self.inputs.append(deepcopy(context.input_messages))


@pytest.mark.parametrize("stream", [False, True], ids=["non-streaming", "streaming"])
async def test_rich_image_context_and_paired_ids_cross_entity_without_decoding_opaque_outputs(stream: bool) -> None:
    outputs = [{"type": "text", "provider_only": {"type": "error", "pixels": [0, False, None, "雪"]}}]
    message = Message(
        "assistant",
        [Content.from_image_generation_tool_result(image_id="image-1", outputs=deepcopy(outputs))],
        message_id="shared-app-id",
        additional_properties={"origin": {"labels": ["keep"]}},
    )
    raw_message = _wire(message.to_dict())
    raw_message["future_message"] = {"opaque": [1]}
    _object(_array(raw_message["contents"])[0])["future_content"] = {"opaque": [2]}
    request = {
        "message": "logging only",
        "correlationId": "rich-input",
        "contextMessages": [deepcopy(raw_message), deepcopy(raw_message)],
        "contextMessageIds": ["image-occurrence-1", "image-occurrence-2"],
    }
    before = deepcopy(request)
    client = _MessageClient(deepcopy(message))
    probe = _Inputs()
    provider = JsonStateProvider()
    agent = _ObservedAgent(client=client, streaming=stream, context_providers=[probe])

    response = await AgentEntity(agent, state_provider=provider).run(request)

    assert response.additional_properties.get("durable_status") != "error"
    assert response.messages[0].contents[0].outputs == outputs and len(client.received_messages) == 1
    assert [item.to_dict() for item in probe.inputs[0]] == [message.to_dict()] * 2
    assert len(client.received_messages[0]) == 2
    for item in client.received_messages[0]:
        assert item.message_id == "shared-app-id"
        assert item.contents[0].type == "image_generation_tool_result"
        assert item.contents[0].outputs == outputs
        assert isinstance(item.contents[0].outputs[0], dict)
    assert request == before and message.contents[0].outputs == outputs
    assert _data(provider)["ingestedMessages"] == {
        identity: [message_identity(message)] for identity in ("image-occurrence-1", "image-occurrence-2")
    }
    raw = _wire(provider.raw)
    mailbox = _object(_object(_object(_object(raw["data"])["responseMailbox"])["rich-input"])["response"])
    assert mailbox == _wire(serialize_agent_response(response))
    mailbox["future_response"] = {"opaque": [3]}
    # Simulate a newer writer adding optional fields to the actual committed model response.
    saved_message = _object(_array(mailbox["messages"])[0])
    saved_message["future_message"] = {"opaque": [1]}
    _object(_array(saved_message["contents"])[0])["future_content"] = {"opaque": [2]}
    expected_raw = deepcopy(mailbox)
    cold_provider = JsonStateProvider(raw)
    cold = AgentEntity(agent, state_provider=cold_provider)
    delivered = await cold.run(request)
    assert delivered.messages[0].contents[0].outputs == outputs
    delivered_output = delivered.messages[0].contents[0].outputs[0]
    assert isinstance(delivered_output, dict)
    delivered_output["provider_only"]["pixels"].append("consumer edit")
    assert _mailbox(cold_provider, "rich-input") == expected_raw
    assert len(client.received_messages) == 1 and cold_provider.writes == 0
    followup = await cold.run({**request, "correlationId": "rich-next"})
    assert followup.additional_properties.get("durable_status") != "error"
    assert probe.inputs[-1] == []
    assert len(client.received_messages) == 2
    assert len(client.received_messages[1]) == 3, "two ingested occurrences plus the actual model response"
    assert all(item.contents[0].outputs == outputs for item in client.received_messages[1])
    assert _mailbox(cold_provider, "rich-input") == expected_raw and cold_provider.writes == 1
    assert request == before


async def test_contentless_migrated_workflow_id_never_backfills_an_ingestion_receipt() -> None:
    identity = workflow_message_id("upstream", 3)
    source = {
        "schemaVersion": "1.1.0",
        "data": {
            "conversationHistory": [
                {
                    "$type": "request",
                    "correlationId": "legacy",
                    "createdAt": "2024-01-01T00:00:00+00:00",
                    "messages": [{"role": "user", "messageId": identity, "contents": []}],
                }
            ]
        },
    }
    before_source = deepcopy(source)
    migrated = migrate_legacy_state(
        source,
        source_digest=state_snapshot_digest(source),
        source_session_id="legacy-session",
        migration_id="execution-followup",
        ownership_transfer_id="quiesced-owner",
        delivery_window_seconds=3600,
    )
    assert migrated.data.ingested_messages == {}
    probe = _Inputs()
    client = ToolChatClient(tool_calls=False)
    agent = Agent(client=client, context_providers=[probe])
    provider = JsonStateProvider(_wire(migrated.to_dict()))
    await AgentEntity(agent, state_provider=provider).run({"message": "unrelated turn", "correlationId": "unrelated"})
    assert _data(provider).get("ingestedMessages", {}) == {}, "loading old history is not proof of ingestion"
    cold_provider = JsonStateProvider(_wire(provider.raw))
    message = Message("user", ["complete incoming payload"], message_id=identity)
    request = {"message": "logging only", "correlationId": "incoming", "contextMessages": [message.to_dict()]}

    response = await AgentEntity(agent, state_provider=cold_provider).run(request)

    assert response.text == "answer-2"
    assert [item.to_dict() for item in probe.inputs[-1]] == [message.to_dict()]
    assert [item.text for item in client.received_messages[-1]].count(message.text) == 1
    assert _data(cold_provider)["ingestedMessages"] == {identity: [message_identity(message)]}
    stored = _array(_data(cold_provider)["conversationHistory"])
    legacy = next(_object(entry) for entry in stored if _object(entry).get("correlationId") == "legacy")
    assert _object(_array(legacy["messages"])[0])["contents"] == []
    assert source == before_source


async def test_new_direct_context_same_application_id_uses_actual_ingestion_receipt_after_cold_reload() -> None:
    probe = _Inputs()
    client = ToolChatClient(tool_calls=False)
    agent = Agent(client=client, context_providers=[probe])
    provider = JsonStateProvider()
    message = Message("user", ["direct projected input"], message_id="application-id")
    request = {"message": "logging only", "correlationId": "direct-first", "contextMessages": [message.to_dict()]}
    assert (await AgentEntity(agent, state_provider=provider).run(request)).text == "answer-1"
    assert [item.to_dict() for item in probe.inputs[0]] == [message.to_dict()]
    expected_receipts = {"application-id": [message_identity(message)]}
    assert _data(provider)["ingestedMessages"] == expected_receipts
    cold_provider = JsonStateProvider(_wire(provider.raw))

    response = await AgentEntity(agent, state_provider=cold_provider).run({**request, "correlationId": "direct-next"})

    assert response.text == "answer-2" and probe.inputs[-1] == []
    assert [item.text for item in client.received_messages[-1]].count(message.text) == 1
    assert _data(cold_provider)["ingestedMessages"] == expected_receipts
    assert request["contextMessages"] == [message.to_dict()]


class _StatefulHistory(DurableHistoryProvider):
    def __init__(self) -> None:
        super().__init__(source_id="stateful-history", prune_excluded=False)
        self.loaded_counters: list[int] = []
        self.live_states: list[dict[str, object]] = []

    async def before_run(
        self, *, agent: SupportsAgentRun, session: AgentSession, context: SessionContext, state: dict[str, Any]
    ) -> None:
        counter = state.get("counter", 0)
        assert isinstance(counter, int)
        self.loaded_counters.append(counter)
        state["counter"] = counter + 1
        self.live_states.append(state)
        await super().before_run(agent=agent, session=session, context=context, state=state)


async def test_custom_history_state_and_unknown_session_envelope_survive_two_json_cold_runs() -> None:
    initial = DurableAgentState()
    session = AgentSession(session_id="revision-session")
    pending = {"approval": {"ids": ["pending-1"], "approved": False}, "additional": {"cursor": [1, 3]}}
    session.state["stateful-history"] = {"counter": 0, "pending": deepcopy(pending)}
    session.state["foreign"] = {"pending": ["untouched"]}
    initial.data.session = session.to_dict()
    future = {"type": "future_session_metadata", "opaque": [None, False, {"labels": ["keep"]}]}
    initial.data.session["future_session"] = deepcopy(future)
    raw = _wire(initial.to_dict())
    original = deepcopy(raw)
    histories: list[_StatefulHistory] = []
    clients: list[ToolChatClient] = []
    for turn in (1, 2):
        history = _StatefulHistory()
        histories.append(history)
        client = ToolChatClient(tool_calls=False)
        clients.append(client)
        provider = JsonStateProvider(_wire(raw))
        agent = Agent(client=client, context_providers=[history])

        response = await AgentEntity(agent, state_provider=provider).run({
            "message": f"input-{turn}",
            "correlationId": f"state-{turn}",
        })

        assert response.text == "answer-1" and provider.writes == 1
        assert history.loaded_counters == [turn - 1]
        saved_session = _object(_data(provider)["session"])
        saved_state = _object(saved_session["state"])
        assert saved_state[history.source_id] == {"counter": turn, "pending": pending}
        assert saved_state["foreign"] == {"pending": ["untouched"]}
        assert saved_session["future_session"] == future
        assert WORKING_BUFFER_KEY in history.live_states[0] and POSITIONS_KEY in history.live_states[0]
        assert len(_array(history.live_states[0][WORKING_BUFFER_KEY])) == turn * 2
        raw = _wire(provider.raw)
    assert histories[0] is not histories[1] and histories[0].live_states[0] is not histories[1].live_states[0]
    assert [message.text for message in clients[1].received_messages[0]] == ["input-1", "answer-1", "input-2"]
    assert _wire(initial.to_dict()) == original


def test_optional_af_unnamed_agent_registers_under_explicit_workflow_executor_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app_module = pytest.importorskip("agent_framework_azurefunctions._app")
    original_factory = app_module.create_agent_entity
    created: list[SupportsAgentRun] = []

    def capture_factory(agent: SupportsAgentRun, *args: Any, **kwargs: Any) -> Any:
        created.append(agent)
        return original_factory(agent, *args, **kwargs)

    monkeypatch.setattr(app_module, "create_agent_entity", capture_factory)
    client = ToolChatClient(tool_calls=False)
    agent = Agent(client=client)
    assert agent.name is None
    executor = AgentExecutor(agent, id="reviewer")
    workflow = WorkflowBuilder(name="execution_followup", start_executor=executor, output_from=[executor]).build()
    app = app_module.AgentFunctionApp(
        workflow=workflow,
        deployment_mode="isolated_v2",
        enable_health_check=False,
        enable_http_endpoints=False,
        enable_mcp_tool_trigger=False,
    )
    assert app.agents == {"execution_followup-reviewer": agent}
    assert created == [agent] and agent.name is None
    functions = {function.get_function_name(): function for function in app.get_functions()}
    registered = functions["dafx-execution_followup-reviewer"]
    assert registered.get_trigger().get_dict_repr()["type"] == "entityTrigger"
    assert callable(registered.get_user_function())
    assert client.received_messages == []
    # A missing standalone identity is still rejected rather than silently inventing a name.
    with pytest.raises(ValueError, match="name"):
        app.add_agent(agent)
    assert created == [agent]
