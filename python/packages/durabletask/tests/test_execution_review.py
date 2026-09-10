# Copyright (c) Microsoft. All rights reserved.

"""Execution regressions using real core pipelines and detached JSON storage."""

import json
from collections.abc import AsyncIterable, Sequence
from copy import deepcopy
from typing import Any

import pytest
from agent_framework import (
    Agent,
    AgentResponse,
    AgentResponseUpdate,
    AgentSession,
    ChatResponse,
    ChatResponseUpdate,
    Content,
    ContextProvider,
    HistoryProvider,
    Message,
    ResponseStream,
    SessionContext,
    tool,
)
from test_durable_history_provider import RecordingChatClient
from test_history_pipeline_revision import CountingHistory, NonStreamingAgent, ToolChatClient
from test_revision_contract import JsonStateProvider

from agent_framework_durabletask import AgentEntity, DurableAgentState, DurableHistoryProvider, RunRequest
from agent_framework_durabletask import _entities as entities_module
from agent_framework_durabletask._durable_agent_state import DurableAgentStateRequest
from agent_framework_durabletask._history_provider import current_durable_history_binding
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._retention import enforce_budget


def _wire(value: Any) -> Any:
    return json.loads(json.dumps(value, allow_nan=False))


def _committed(provider: JsonStateProvider) -> dict[str, Any]:
    return _wire(provider.raw)


def _make_agent(client: Any, **kwargs: Any) -> Agent:
    return Agent(client=client, **kwargs)


def _projection(correlation: str, messages: list[Message], occurrences: list[str]) -> dict[str, Any]:
    return {
        "message": "logging-only input must not become model context",
        "correlationId": correlation,
        "contextMessages": _wire([message.to_dict() for message in messages]),
        "contextMessageIds": list(occurrences),
    }


def _delivered(provider: JsonStateProvider, correlation: str) -> AgentResponse[Any]:
    response = DurableAgentState.from_json(json.dumps(_committed(provider))).try_get_agent_response(correlation)
    assert isinstance(response, AgentResponse)
    return response


class _Probe(ContextProvider):
    def __init__(self) -> None:
        super().__init__("execution-probe")
        self.inputs: list[list[Message]] = []
        self.agents: list[Any] = []
        self.sessions: list[AgentSession] = []
        self.responses: list[AgentResponse[Any]] = []
        self.response_snapshots: list[dict[str, Any]] = []
        self.fail_at: str | None = None

    async def before_run(self, *, agent: Any, session: AgentSession, context: SessionContext, **kwargs: Any) -> None:
        self.inputs.append(deepcopy(context.input_messages))
        self.agents.append(agent)
        self.sessions.append(session)
        if self.fail_at == "before":
            raise RuntimeError("probe before-run failure")

    async def after_run(self, *, context: SessionContext, **kwargs: Any) -> None:
        assert isinstance(context.response, AgentResponse)
        self.responses.append(context.response)
        self.response_snapshots.append(_wire(context.response.to_dict()))
        if self.fail_at == "after":
            raise RuntimeError("probe after-run failure")


class _RichClient(RecordingChatClient):
    text = '{"nested":{"items":[1,2]}}'

    @staticmethod
    def _content() -> Content:
        return Content.from_text(_RichClient.text, additional_properties={"source": {"tags": ["original"]}})

    def get_response(self, messages: Any, *, stream: bool = False, **kwargs: Any) -> Any:
        if stream:
            return super().get_response(messages, stream=True, **kwargs)
        self.received_messages.append(list(messages))

        async def get() -> ChatResponse[Any]:
            return ChatResponse(
                messages=[Message("assistant", [self._content()], message_id="rich-answer")],
                response_id="rich-response",
                additional_properties={"result": {"tags": ["original"]}},
                value={"nested": {"items": [1, 2]}},
            )

        return get()

    def _stream(self, options: dict[str, Any]) -> ResponseStream[ChatResponseUpdate, ChatResponse]:
        async def updates() -> AsyncIterable[ChatResponseUpdate]:
            yield ChatResponseUpdate(
                role="assistant",
                contents=[self._content()],
                message_id="rich-answer",
                response_id="rich-response",
                additional_properties={"result": {"tags": ["original"]}},
            )

        return ResponseStream(updates(), finalizer=ChatResponse.from_updates)


class _MutatingCallback:
    def __init__(self, *, mutate_updates: bool, mutate_final: bool) -> None:
        self.mutate_updates = mutate_updates
        self.mutate_final = mutate_final
        self.updates: list[AgentResponseUpdate] = []
        self.responses: list[AgentResponse[Any]] = []
        self.mutations: list[str] = []

    async def on_streaming_response_update(self, update: AgentResponseUpdate, context: Any) -> None:
        self.updates.append(update)
        if self.mutate_updates:
            update.contents[0].text = '{"nested":{"items":[99]}}'
            update.contents[0].additional_properties["source"]["tags"].append("callback-update")
            assert update.additional_properties is not None
            update.additional_properties["result"]["tags"].append("callback-update")
            self.mutations.append("update")

    async def on_agent_response(self, response: AgentResponse[Any], context: Any) -> None:
        self.responses.append(response)
        if self.mutate_final:
            value = response.value
            assert value is not None
            value["nested"]["items"].append(99)
            response.messages[0].contents[0].text = "callback final text"
            response.messages[0].contents[0].additional_properties["source"]["tags"].append("callback-final")
            response.additional_properties["result"]["tags"].append("callback-final")
            self.mutations.append("final")


@pytest.mark.parametrize(
    ("stream", "mutate_updates", "mutate_final"),
    [(True, True, False), (True, False, True), (True, True, True), (False, False, True)],
    ids=["stream-update", "stream-final", "stream-both", "non-streaming-final"],
)
async def test_callbacks_cannot_change_core_response_history_or_cold_mailbox(
    stream: bool, mutate_updates: bool, mutate_final: bool
) -> None:
    client = _RichClient()
    probe = _Probe()
    agent = (Agent if stream else NonStreamingAgent)(client=client, context_providers=[probe])
    callback = _MutatingCallback(mutate_updates=mutate_updates, mutate_final=mutate_final)
    provider = JsonStateProvider()
    entity = AgentEntity(agent, callback=callback, state_provider=provider)
    request = {
        "message": "return a nested value",
        "correlationId": "callback-copy",
        "options": {"response_format": {"type": "object"}},
    }

    response = await entity.run(request)

    assert callback.mutations == (["update"] if mutate_updates else []) + (["final"] if mutate_final else [])
    assert len(callback.updates) == int(stream) and len(callback.responses) == 1
    assert len(probe.responses) == 1 and probe.responses[0] is response
    assert callback.responses[0] is not response
    assert callback.responses[0].messages[0].contents[0] is not response.messages[0].contents[0]
    assert response.text == _RichClient.text
    assert response.value == {"nested": {"items": [1, 2]}}
    assert response.additional_properties["result"] == {"tags": ["original"]}
    assert response.messages[0].contents[0].additional_properties == {"source": {"tags": ["original"]}}
    assert probe.response_snapshots[0]["messages"][0]["contents"][0]["text"] == _RichClient.text
    stored = DurableAgentState.from_json(json.dumps(_committed(provider)))
    answers = [
        message.to_chat_message()
        for entry in stored.data.conversation_history
        for message in entry.messages
        if message.role == "assistant"
    ]
    assert len(answers) == 1 and answers[0].text == _RichClient.text
    assert answers[0].contents[0].additional_properties == {"source": {"tags": ["original"]}}
    delivered = _delivered(provider, "callback-copy")
    assert delivered.text == response.text and delivered.value == response.value
    assert delivered.additional_properties == response.additional_properties
    assert delivered.messages[0].contents[0].to_dict() == response.messages[0].contents[0].to_dict()
    before = _committed(provider)
    # A callback may retain its objects and mutate them after the entity has committed.
    retained_value = callback.responses[0].value
    assert retained_value is not None
    retained_value["nested"]["items"].append(101)
    callback.responses[0].messages[0].contents[0].additional_properties["source"]["tags"].append("late")
    assert response.value == {"nested": {"items": [1, 2]}}
    assert _committed(provider) == before
    cold_provider = JsonStateProvider(before)
    duplicate = await AgentEntity(agent, callback=callback, state_provider=cold_provider).run(request)
    assert duplicate.to_dict() == delivered.to_dict()
    assert len(client.received_messages) == 1 and cold_provider.writes == 0
    assert len(callback.responses) == 1


class _StructuredFailure(RuntimeError):
    def __init__(self, code: str, *, body: bool = False) -> None:
        super().__init__(code)
        if body:
            self.body = {"code": code}
        else:
            self.code = code


class _RetryClient(RecordingChatClient):
    STORES_BY_DEFAULT = True

    def __init__(self, second: str, *, body: bool = False) -> None:
        super().__init__()
        self.second = second
        self.body = body
        self.errors: list[BaseException] = []
        self.options: list[dict[str, Any]] = []

    def get_response(self, messages: Any, *, options: Any = None, **kwargs: Any) -> Any:
        self.options.append(deepcopy(dict(options or {})))
        return super().get_response(messages, options=options, **kwargs)

    def _stream(self, options: dict[str, Any]) -> ResponseStream[ChatResponseUpdate, ChatResponse]:
        async def updates() -> AsyncIterable[ChatResponseUpdate]:
            attempt = len(self.received_messages)
            try:
                if attempt == 1:
                    raise _StructuredFailure("previous_response_not_found", body=self.body)
                if attempt == 2:
                    if self.second == "implicit":
                        raise RuntimeError("unrelated second failure")
                    code = "previous_response_not_found" if self.second == "wrapped-missing" else "invalid_api_key"
                    current = _StructuredFailure(code, body=self.body)
                    if self.second.startswith("wrapped"):
                        try:
                            raise current
                        except _StructuredFailure as cause:
                            raise RuntimeError(f"current wrapper: {code}") from cause
                    raise current
            except Exception as exc:
                self.errors.append(exc)
                raise
            yield ChatResponseUpdate(role="assistant", contents=[Content.from_text("retry recovered")])

        return ResponseStream(updates(), finalizer=ChatResponse.from_updates)


@pytest.mark.parametrize("second", ["different-code", "implicit", "wrapped-different"])
@pytest.mark.parametrize("body", [False, True], ids=["code-attribute", "body-code"])
async def test_retry_delivers_current_failure_without_following_stale_implicit_context(
    monkeypatch: pytest.MonkeyPatch, second: str, body: bool
) -> None:
    monkeypatch.setattr(entities_module, "_REJECTED_ID_BACKOFF_SECONDS", 0)
    client = _RetryClient(second, body=body)
    provider = JsonStateProvider()
    entity = AgentEntity(Agent(client=client), state_provider=provider)
    request = {"message": "continue", "correlationId": "retry-current"}

    response = await entity.run(request)

    assert len(client.received_messages) == 2, "only the missing-response failure authorizes another attempt"
    assert len(client.errors) == 2
    current = client.errors[1].__cause__ if second == "wrapped-different" else client.errors[1]
    assert current is not None
    assert current.__context__ is client.errors[0], "exercise Python's implicit prior-exception chain"
    assert response.additional_properties["durable_status"] == "error"
    expected = "unrelated second failure" if second == "implicit" else "invalid_api_key"
    assert expected in response.text and "previous_response_not_found" not in response.text
    assert client.options[0] == client.options[1]
    assert [[message.to_dict() for message in batch] for batch in client.received_messages] == [
        [message.to_dict() for message in client.received_messages[0]]
    ] * 2
    assert _delivered(provider, "retry-current").to_dict() == response.to_dict()
    cold_provider = JsonStateProvider(_committed(provider))
    assert (await AgentEntity(Agent(client=client), state_provider=cold_provider).run(request)).text == response.text
    assert len(client.received_messages) == 2 and cold_provider.writes == 0


@pytest.mark.parametrize("body", [False, True], ids=["code-attribute", "body-code"])
async def test_explicit_current_missing_cause_remains_retryable(monkeypatch: pytest.MonkeyPatch, body: bool) -> None:
    monkeypatch.setattr(entities_module, "_REJECTED_ID_BACKOFF_SECONDS", 0)
    client = _RetryClient("wrapped-missing", body=body)
    provider = JsonStateProvider()

    response = await AgentEntity(Agent(client=client), state_provider=provider).run({
        "message": "continue",
        "correlationId": "wrapped-retry",
    })

    assert response.text == "retry recovered" and len(client.received_messages) == 3
    assert len(client.errors) == 2
    assert isinstance(client.errors[1].__cause__, _StructuredFailure)
    assert client.errors[1].__cause__ is not client.errors[0]
    assert client.options == [client.options[0]] * 3
    assert _delivered(provider, "wrapped-retry").text == "retry recovered"
    assert provider.writes == 1


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
@pytest.mark.parametrize("stream", [False, True], ids=["non-streaming", "streaming"])
@pytest.mark.parametrize("tool_source", ["default", "context-provider"])
async def test_disabled_tools_never_execute_even_when_the_model_returns_a_function_call(
    per_call: bool, stream: bool, tool_source: str
) -> None:
    calls: list[str] = []

    @tool(name="lookup", approval_mode="never_require")
    def lookup(key: str) -> str:
        calls.append(key)
        return f"value:{key}"

    class ToolProvider(ContextProvider):
        async def before_run(self, *, context: SessionContext, **kwargs: Any) -> None:
            context.tools.append(lookup)

    def make(client: ToolChatClient) -> Agent:
        return (Agent if stream else NonStreamingAgent)(
            client=client,
            tools=[lookup] if tool_source == "default" else [],
            context_providers=[ToolProvider("tools")] if tool_source == "context-provider" else [],
            require_per_service_call_history_persistence=per_call,
        )

    # Positive control proves that the exact helper/model request reaches real core invocation.
    enabled_client = ToolChatClient()
    enabled = await AgentEntity(make(enabled_client), state_provider=JsonStateProvider()).run({
        "message": "use lookup",
        "correlationId": "tools-enabled",
    })
    assert enabled.text == "answer-2" and calls == ["durable"]
    assert len(enabled_client.received_messages) == 2
    calls.clear()
    client = ToolChatClient()
    agent = make(client)
    original_options = agent.default_options
    original_tools = original_options["tools"]
    original_tool_items = list(original_tools)
    original_providers = agent.context_providers
    config = deepcopy(client.function_invocation_configuration)
    provider = JsonStateProvider()
    entity = AgentEntity(agent, state_provider=provider)
    registered = entity.agent
    request = {"message": "use lookup", "correlationId": "tools-disabled", "enable_tool_calls": False}

    response = await entity.run(request)

    assert client.received_messages, "reach the model that requests lookup despite tool_choice=none"
    assert client.received_options[0].get("tool_choice") == "none"
    assert calls == [], "forwarding tool_choice is insufficient if the invocation layer still has a callable"
    assert not any(
        content.type == "function_result" and content.result == "value:durable"
        for batch in client.received_messages
        for message in batch
        for content in message.contents
    )
    assert entity.agent is registered
    assert agent.default_options is original_options and original_options["tools"] is original_tools
    assert original_tools == original_tool_items and agent.context_providers is original_providers
    assert client.function_invocation_configuration == config
    assert current_durable_history_binding() is None
    # An error response or an unexecuted function call are both safe outcomes.
    assert _delivered(provider, "tools-disabled").to_dict() == response.to_dict()
    count = len(client.received_messages)
    await AgentEntity(agent, state_provider=JsonStateProvider(_committed(provider))).run(request)
    assert len(client.received_messages) == count and calls == []


class _ExternalHistory(HistoryProvider):
    """Ordinary blind-append storage with no awareness of store options or durable bindings."""

    def __init__(self, source_id: str, **kwargs: Any) -> None:
        super().__init__(source_id, **kwargs)
        self.messages: list[Message] = []
        self.loads: list[str | None] = []
        self.saves: list[tuple[str | None, list[Message]]] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        self.loads.append(session_id)
        return deepcopy(self.messages)

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.saves.append((session_id, deepcopy(list(messages))))
        self.messages.extend(deepcopy(list(messages)))


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
async def test_store_true_false_true_parks_external_primary_but_not_store_only_sinks(per_call: bool) -> None:
    primary = _ExternalHistory("external")
    primary.messages = [Message("user", ["external seed"], message_id="seed")]
    outputs = _ExternalHistory("audit-outputs", load_messages=False, store_inputs=False)
    inputs = _ExternalHistory("audit-inputs", load_messages=False, store_outputs=False)
    probe = _Probe()
    client = ToolChatClient(tool_calls=False)
    agent = Agent(
        client=client,
        context_providers=[primary, outputs, inputs, probe],
        require_per_service_call_history_persistence=per_call,
    )
    providers = agent.context_providers
    defaults = agent.default_options
    original_defaults = deepcopy(defaults)
    raw: dict[str, Any] = {}
    for index, (store, text) in enumerate(((True, "service-first"), (False, "local"), (True, "service-again")), 1):
        provider = JsonStateProvider(_wire(raw))
        entity = AgentEntity(agent, state_provider=provider)
        registered = entity.agent
        request = {"message": text, "correlationId": f"ownership-{index}", "options": {"store": store}}
        original_request = deepcopy(request)

        response = await entity.run(request)

        assert response.text == f"answer-{index}"
        assert entity.agent is registered and agent.context_providers is providers
        assert agent.default_options is defaults and defaults == original_defaults and request == original_request
        invocation = probe.agents[-1]
        if store:
            assert invocation is not registered
            assert invocation.context_providers[0].__wrapped__ is primary
        else:
            assert invocation.context_providers[0] is primary
        assert invocation.context_providers[1] is outputs and invocation.context_providers[2] is inputs
        assert outputs.load_messages is False and outputs.store_inputs is False and outputs.store_outputs is True
        assert inputs.load_messages is False and inputs.store_inputs is True and inputs.store_outputs is False
        assert outputs.loads == inputs.loads == []
        assert [batch[0].text for _, batch in outputs.saves] == [f"answer-{i}" for i in range(1, index + 1)]
        assert all(len(batch) == 1 for _, batch in outputs.saves + inputs.saves)
        raw = _committed(provider)
        assert raw["data"]["conversationHistory"] == []
        assert raw["data"]["session"]["service_session_id"] == "service-thread"
        assert provider.writes == 1 and current_durable_history_binding() is None
    assert primary.loads == ["revision-session"] and len(primary.saves) == 1
    assert [message.text for message in primary.messages] == ["external seed", "local", "answer-2"]
    assert [batch[0].text for _, batch in inputs.saves] == ["service-first", "local", "service-again"]
    assert {session_id for session_id, _ in primary.saves + inputs.saves + outputs.saves} == {"revision-session"}
    assert [[message.text for message in batch] for batch in client.received_messages] == [
        ["service-first"],
        ["external seed", "local"],
        ["service-again"],
    ]
    assert [options.get("conversation_id") for options in client.received_options] == [None, None, "service-thread"]


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
@pytest.mark.parametrize("failure", ["before", "model", "after", "commit"])
async def test_service_owner_view_is_restored_on_provider_model_and_commit_errors(per_call: bool, failure: str) -> None:
    primary = _ExternalHistory("external")
    sink = _ExternalHistory("audit", load_messages=False)
    probe = _Probe()
    probe.fail_at = failure
    client = ToolChatClient(tool_calls=False, fail=failure == "model")
    agent = Agent(
        client=client,
        context_providers=[primary, sink, probe],
        require_per_service_call_history_persistence=per_call,
    )
    providers = agent.context_providers
    provider = JsonStateProvider()
    provider.fail_writes = failure == "commit"
    entity = AgentEntity(agent, state_provider=provider)
    registered = entity.agent
    original_state = entity.state
    request = {"message": "service failure", "correlationId": "owner-error", "options": {"store": True}}

    if failure == "commit":
        with pytest.raises(OSError, match="commit failure"):
            await entity.run(request)
        assert entity.state is original_state and _committed(provider) == {} and provider.writes == 0
        assert entity.state.try_get_agent_response("owner-error") is None
    else:
        response = await entity.run(request)
        assert response.additional_properties["durable_status"] == "error"
        assert _delivered(provider, "owner-error").to_dict() == response.to_dict()
    assert probe.agents and probe.agents[0] is not registered
    assert probe.agents[0].context_providers[0].__wrapped__ is primary
    assert probe.agents[0].context_providers[1] is sink
    assert entity.agent is registered and agent.context_providers is providers and providers[0] is primary
    assert primary.loads == [] and primary.saves == []
    assert current_durable_history_binding() is None
    probe.fail_at = None
    client.fail = False
    provider.fail_writes = False
    recovered = await entity.run({
        "message": "local recovery",
        "correlationId": "owner-recovered",
        "options": {"store": False},
    })
    assert recovered.additional_properties.get("durable_status") != "error"
    assert primary.loads == [provider.core_session_id] and len(primary.saves) == 1
    assert entity.agent is registered and agent.context_providers is providers


@pytest.mark.parametrize("invalid", [17, 1.5, None, "", " \t", True, False, [], ["unsafe"]])
@pytest.mark.parametrize("boundary", ["constructor", "from-dict", "entity-dict", "entity-json"])
async def test_correlation_id_requires_a_nonblank_string_before_any_client_or_state_write(
    invalid: Any, boundary: str
) -> None:
    provider = JsonStateProvider()
    client = RecordingChatClient()
    entity = AgentEntity(_make_agent(client), state_provider=provider)
    original = entity.state
    payload = {"message": "must not execute", "correlationId": invalid}
    with pytest.raises(ValueError, match="correlationId"):
        if boundary == "constructor":
            request: Any = RunRequest(message="must not execute", correlation_id=invalid)
        elif boundary == "from-dict":
            request = RunRequest.from_dict(payload)
        else:
            request = json.dumps(payload) if boundary == "entity-json" else payload
        await entity.run(request)
    assert client.received_messages == [] and provider.writes == 0 and _committed(provider) == {}
    assert entity.state is original


@pytest.mark.parametrize("payload", [None, [], [1], 17, 1.5, True, False, "input"])
async def test_nonobject_json_requests_are_rejected_before_execution(payload: Any) -> None:
    provider = JsonStateProvider()
    client = RecordingChatClient()
    with pytest.raises(ValueError, match="object"):
        await AgentEntity(_make_agent(client), state_provider=provider).run(json.dumps(payload))
    assert client.received_messages == [] and provider.writes == 0 and provider.raw == {}


async def test_string_numeric_correlation_survives_cold_reload_without_reexecution() -> None:
    provider = JsonStateProvider()
    client = RecordingChatClient()
    agent = _make_agent(client)
    request = RunRequest(message="valid string ID", correlation_id="17")
    response = await AgentEntity(agent, state_provider=provider).run(request)
    cold_provider = JsonStateProvider(_committed(provider))
    duplicate = await AgentEntity(agent, state_provider=cold_provider).run(request.to_dict())
    assert duplicate.to_dict() == response.to_dict()
    assert set(provider.raw["data"]["completedCorrelations"]) == {"17"}
    assert len(client.received_messages) == 1 and cold_provider.writes == 0


@pytest.mark.parametrize("anonymous", [False, True], ids=["application-id", "anonymous"])
async def test_paired_occurrences_preserve_exact_raw_messages_and_canonical_content_metadata(anonymous: bool) -> None:
    message = Message(
        "user",
        [Content.from_text("identical payload", additional_properties={"source": {"tags": ["keep"]}})],
        message_id=None if anonymous else "same-application-id",
        additional_properties={"application": {"labels": [1, 2]}},
    )
    request = _projection("paired", [message, deepcopy(message)], ["o1", "o2"])
    original = deepcopy(request)
    client = RecordingChatClient()
    provider = JsonStateProvider()

    response = await AgentEntity(_make_agent(client), state_provider=provider).run(request)

    assert response.text == "reply-1"
    assert len(client.received_messages) == 1
    assert [item.to_dict() for item in client.received_messages[0]] == original["contextMessages"]
    assert [item.message_id for item in client.received_messages[0]] == [message.message_id] * 2
    assert request == original and message.to_dict() == original["contextMessages"][0]
    assert provider.raw["data"]["ingestedMessages"] == {key: [message_identity(message)] for key in ("o1", "o2")}
    restored = DurableAgentState.from_json(json.dumps(_committed(provider)))
    inputs = [
        stored.to_chat_message()
        for entry in restored.data.conversation_history
        if isinstance(entry, DurableAgentStateRequest)
        for stored in entry.messages
    ]
    assert len(inputs) == 2
    assert [item.contents[0].to_dict() for item in inputs] == [message.contents[0].to_dict()] * 2


@pytest.mark.parametrize("evict", [False, True], ids=["retained-history", "pressure-evicted-history"])
async def test_occurrence_receipts_survive_cold_reload_and_eviction_without_blocking_new_runs(evict: bool) -> None:
    message = Message("user", ["projected input " * 1000], message_id="application-id")
    client = RecordingChatClient()
    probe = _Probe()
    agent = _make_agent(client, context_providers=[probe])
    provider = JsonStateProvider()
    entity = AgentEntity(agent, state_provider=provider)
    original_request = _projection("first", [message], ["o1"])
    original_response = await entity.run(original_request)
    await entity.run({"message": "newest exchange", "correlationId": "anchor"})
    if evict:
        removed = await enforce_budget(entity.state, max_state_bytes=6000)
        assert removed > 0
        entity.persist_state()
        assert not any(
            stored.text == message.text for entry in entity.state.data.conversation_history for stored in entry.messages
        )
    assert provider.raw["data"]["ingestedMessages"] == {"o1": [message_identity(message)]}
    cold_provider = JsonStateProvider(_committed(provider))
    cold = AgentEntity(agent, state_provider=cold_provider)
    calls_before = len(client.received_messages)
    inputs_before = len(probe.inputs)
    duplicate = await cold.run(original_request)
    assert duplicate.to_dict() == original_response.to_dict()
    assert len(client.received_messages) == calls_before and len(probe.inputs) == inputs_before
    assert cold_provider.writes == 0
    repeated = await cold.run(_projection("new-correlation-same-occurrence", [message], ["o1"]))
    assert repeated.text == "reply-3" and probe.inputs[-1] == []
    assert len(client.received_messages) == calls_before + 1 and cold_provider.writes == 1
    assert "new-correlation-same-occurrence" in cold_provider.raw["data"]["completedCorrelations"]
    await cold.run(_projection("new-occurrence", [message], ["o2"]))
    assert [item.to_dict() for item in probe.inputs[-1]] == [message.to_dict()]
    revised = deepcopy(message)
    revised.contents[0].text = "revised payload"
    await cold.run(_projection("revised-occurrence", [revised], ["o1"]))
    assert [item.to_dict() for item in probe.inputs[-1]] == [revised.to_dict()]
    assert cold_provider.raw["data"]["ingestedMessages"] == {
        "o1": [message_identity(message), message_identity(revised)],
        "o2": [message_identity(message)],
    }


async def test_paired_empty_projection_roundtrips_and_never_falls_back_to_logging_text() -> None:
    request = RunRequest(
        message="must not reach the model", correlation_id="empty-pair", context_messages=[], context_message_ids=[]
    )
    for restored in (RunRequest.from_dict(request.to_dict()), RunRequest.from_json(json.dumps(request.to_dict()))):
        assert restored.context_messages == restored.context_message_ids == []
        assert restored.to_dict()["contextMessages"] == restored.to_dict()["contextMessageIds"] == []
    client = RecordingChatClient()
    provider = JsonStateProvider()
    response = await AgentEntity(_make_agent(client), state_provider=provider).run(request)
    assert response.text == "reply-1" and client.received_messages == [[]]
    assert provider.raw["data"].get("ingestedMessages", {}) == {}


@pytest.mark.parametrize("occurrences", [[], ["o1", "o2"], "o1", [None], [17], [""]])
@pytest.mark.parametrize("direct", [False, True], ids=["wire", "constructor"])
async def test_malformed_paired_occurrence_ids_are_rejected_before_client_calls(occurrences: Any, direct: bool) -> None:
    client = RecordingChatClient()
    provider = JsonStateProvider()
    message = Message("user", ["input"], message_id="raw-id")
    with pytest.raises(ValueError, match="contextMessageIds"):
        request: Any = (
            RunRequest(
                message="input",
                correlation_id="bad-pair",
                context_messages=[message.to_dict()],
                context_message_ids=occurrences,
            )
            if direct
            else {
                "message": "input",
                "correlationId": "bad-pair",
                "contextMessages": [message.to_dict()],
                "contextMessageIds": occurrences,
            }
        )
        await AgentEntity(_make_agent(client), state_provider=provider).run(request)
    assert client.received_messages == [] and provider.writes == 0 and provider.raw == {}


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
async def test_failed_tool_followup_retains_receipts_by_occurrence_not_application_id(per_call: bool) -> None:
    calls: list[str] = []

    @tool(name="lookup", approval_mode="never_require")
    def lookup(key: str) -> str:
        calls.append(key)
        return f"value:{key}"

    history = CountingHistory([])
    client = ToolChatClient(fail_on_call=2)
    provider = JsonStateProvider()
    agent = Agent(
        client=client,
        tools=[lookup],
        context_providers=[history],
        require_per_service_call_history_persistence=per_call,
    )
    message = Message("user", ["use lookup"], message_id="shared-application-id")
    request = _projection("partial", [message, deepcopy(message)], ["o1", "o2"])
    response = await AgentEntity(agent, state_provider=provider).run(request)
    assert response.additional_properties["durable_status"] == "error"
    assert "model failed before history persistence" in response.text
    assert calls == ["durable"] and len(client.received_messages) == 2
    assert history.after_calls == int(per_call)
    raw = _committed(provider)
    expected = {identity: [message_identity(message)] for identity in ("o1", "o2")} if per_call else {}
    assert raw["data"].get("ingestedMessages", {}) == expected
    saved_inputs = [
        stored
        for entry in raw["data"]["conversationHistory"]
        if entry["$type"] == "request"
        for stored in entry["messages"]
        if stored["role"] == "user"
    ]
    assert len(saved_inputs) == (2 if per_call else 0)
    probe = _Probe()
    cold_client = RecordingChatClient()
    cold_provider = JsonStateProvider(raw)
    cold = AgentEntity(_make_agent(cold_client, context_providers=[probe]), state_provider=cold_provider)
    assert (await cold.run(request)).to_dict() == response.to_dict()
    assert cold_client.received_messages == [] and cold_provider.writes == 0
    await cold.run(_projection("after-partial", [message, deepcopy(message)], ["o1", "o3"]))
    assert [item.to_dict() for item in probe.inputs[-1]] == [message.to_dict()] * (1 if per_call else 2)
    expected.update({identity: [message_identity(message)] for identity in ("o1", "o3")})
    assert cold_provider.raw["data"]["ingestedMessages"] == expected


@pytest.mark.parametrize("saved_count", [0, 1, 2], ids=["no-append", "partial-append", "full-append"])
async def test_partial_append_does_not_consume_an_unsaved_equal_occurrence(saved_count: int) -> None:
    class InterruptedHistory(DurableHistoryProvider):
        def __init__(self) -> None:
            super().__init__(prune_excluded=False)
            self.appended = 0

        async def after_run(
            self, *, session: AgentSession, context: SessionContext, state: dict[str, Any], **kwargs: Any
        ) -> None:
            batch = context.input_messages[:saved_count]
            await self.save_messages(session.session_id, batch, state=state)
            self.appended = len(batch)
            raise OSError("history interrupted after selected inputs")

    history = InterruptedHistory()
    client = RecordingChatClient()
    provider = JsonStateProvider()
    message = Message("user", ["equal input"], message_id="same-application-id")
    request = _projection("append-failed", [message, deepcopy(message)], ["o1", "o2"])
    entity = AgentEntity(_make_agent(client, context_providers=[history]), state_provider=provider)

    response = await entity.run(request)

    assert response.additional_properties["durable_status"] == "error"
    assert "history interrupted after selected inputs" in response.text
    assert len(client.received_messages) == 1 and history.appended == saved_count
    raw = _committed(provider)
    saved = [item for entry in raw["data"]["conversationHistory"] for item in entry["messages"]]
    assert len(saved) == saved_count, "the failure must occur after the selected real durable appends"
    assert raw["data"].get("ingestedMessages", {}) == {
        occurrence: [message_identity(message)] for occurrence in ["o1", "o2"][:saved_count]
    }
    assert provider.writes == 1
    probe = _Probe()
    cold_client = RecordingChatClient()
    cold_provider = JsonStateProvider(raw)
    cold = AgentEntity(_make_agent(cold_client, context_providers=[probe]), state_provider=cold_provider)
    assert (await cold.run(request)).to_dict() == response.to_dict()
    assert cold_client.received_messages == [] and cold_provider.writes == 0
    await cold.run(_projection("append-recovered", [message, deepcopy(message)], ["o1", "o2"]))
    assert [item.to_dict() for item in probe.inputs[-1]] == [message.to_dict()] * (2 - saved_count)
    assert cold_provider.raw["data"]["ingestedMessages"] == {
        occurrence: [message_identity(message)] for occurrence in ("o1", "o2")
    }


def test_reset_cannot_commit_a_retained_floor_above_the_configured_budget() -> None:
    initial = DurableAgentState()
    initial.data.session = AgentSession(session_id="revision-session", service_session_id="keep-on-rollback").to_dict()
    initial.data.conversation_history.append(DurableAgentStateRequest.from_run_request(RunRequest("history", "old")))
    initial.record_response(
        "protected", AgentResponse(messages=[Message("assistant", ["x" * 8000])]), delivery_window_seconds=3600
    )
    provider = JsonStateProvider(_wire(initial.to_dict()))
    client = RecordingChatClient()
    entity = AgentEntity(_make_agent(client), state_provider=provider, max_state_bytes=512)
    original = entity.state
    before = _committed(provider)

    with pytest.raises(ValueError, match="[Bb]udget|max_state_bytes|[Cc]apacity|floor"):
        entity.reset()

    assert entity.state is original and entity.state.to_dict() == before
    assert _committed(provider) == before and provider.writes == 0 and client.received_messages == []
    assert _delivered(provider, "protected").text == "x" * 8000


async def test_nan_session_state_aborts_commit_and_does_not_cache_completion() -> None:
    class NonFiniteProvider(ContextProvider):
        poison = True

        async def after_run(self, *, state: dict[str, Any], **kwargs: Any) -> None:
            state["nested"] = {"value": float("nan") if self.poison else 1}

    control = NonFiniteProvider("nonfinite")
    initial = DurableAgentState()
    initial.data.session = AgentSession(session_id="revision-session").to_dict()
    initial.data.session["state"] = {"foreign": {"pending": ["keep"]}}
    initial.record_response(
        "prior", AgentResponse(messages=[Message("assistant", ["prior answer"])]), delivery_window_seconds=3600
    )
    provider = JsonStateProvider(_wire(initial.to_dict()))
    client = RecordingChatClient()
    entity = AgentEntity(_make_agent(client, context_providers=[control]), state_provider=provider)
    original = entity.state
    before = _committed(provider)
    request = _projection("nan-run", [Message("user", ["input"], message_id="raw-id")], ["nan-occurrence"])

    with pytest.raises(ValueError, match="JSON|finite|NaN"):
        await entity.run(request)

    assert len(client.received_messages) == 1
    assert entity.state is original and entity.state.to_dict() == before
    assert _committed(provider) == before and provider.writes == 0
    assert entity.state.try_get_agent_response("nan-run") is None
    assert "nan-run" not in entity.state.data.completed_correlations
    assert "nan-occurrence" not in entity.state.data.ingested_messages
    assert current_durable_history_binding() is None
    control.poison = False
    response = await entity.run(request)
    assert response.text == "reply-2" and len(client.received_messages) == 2 and provider.writes == 1
    assert provider.raw["data"]["session"]["state"]["foreign"] == {"pending": ["keep"]}
    assert _delivered(provider, "prior").text == "prior answer"
