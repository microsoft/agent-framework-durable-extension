# Copyright (c) Microsoft. All rights reserved.

"""Adversarial entity-operation boundaries using core agents and JSON storage, without live services."""

import json
from collections.abc import AsyncIterable, Awaitable, Mapping, Sequence
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from agent_framework import (
    Agent,
    AgentResponse,
    AgentResponseUpdate,
    AgentSession,
    ChatOptions,
    ChatResponse,
    ChatResponseUpdate,
    Content,
    ContextProvider,
    HistoryProvider,
    InMemoryHistoryProvider,
    Message,
    ResponseStream,
    SessionContext,
    tool,
)
from test_durable_history_provider import RecordingChatClient
from test_history_pipeline_revision import CountingHistory, ToolChatClient
from test_revision_contract import JsonStateProvider

from agent_framework_durabletask import AgentEntity, DurableAgentState, DurableHistoryProvider
from agent_framework_durabletask._callbacks import AgentCallbackContext
from agent_framework_durabletask._durable_agent_state import DurableAgentStateResponse
from agent_framework_durabletask._history_provider import current_durable_history_binding
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._state_migration import migrate_legacy_state, state_snapshot_digest


class _RecoverableExternalHistory(HistoryProvider):
    """A primary whose reads fail until the test explicitly repairs the backing store."""

    def __init__(self) -> None:
        super().__init__("external")
        self.fail_reads = True
        self.read_sessions: list[str | None] = []
        self.saved: list[list[Message]] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        self.read_sessions.append(session_id)
        if self.fail_reads:
            raise OSError("temporary external history outage")
        return deepcopy([message for batch in self.saved for message in batch])

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.saved.append(deepcopy(list(messages)))


class _ControlProvider(ContextProvider):
    """Observe real core hooks without resolving the response's lazy structured value."""

    def __init__(self) -> None:
        super().__init__("control")
        self.loaded: list[dict[str, Any]] = []
        self.inputs: list[list[Message]] = []
        self.sessions: list[AgentSession] = []
        self.agents: list[Any] = []
        self.responses: list[AgentResponse] = []

    async def before_run(
        self,
        *,
        agent: Any,
        session: AgentSession,
        context: SessionContext,
        state: dict[str, Any],
    ) -> None:
        self.loaded.append(deepcopy(state))
        self.inputs.append(deepcopy(context.input_messages))
        self.sessions.append(session)
        self.agents.append(agent)
        state["before_runs"] = state.get("before_runs", 0) + 1

    async def after_run(self, *, context: SessionContext, state: dict[str, Any], **kwargs: Any) -> None:
        assert isinstance(context.response, AgentResponse)
        self.responses.append(context.response)
        state["after_runs"] = state.get("after_runs", 0) + 1


class _CountingAgent(Agent):
    def __init__(self, *, client: Any, **kwargs: Any) -> None:
        super().__init__(client=client, **kwargs)
        self.run_modes: list[bool] = []

    def run(self, *args: Any, **kwargs: Any) -> Any:
        self.run_modes.append(bool(kwargs.get("stream", False)))
        return super().run(*args, **kwargs)


class _FinalFlushFailureHistory(DurableHistoryProvider):
    def __init__(self) -> None:
        # Pin the policy so registration retains this provider rather than replacing it.
        super().__init__(prune_excluded=False)
        self.fail_final_flush = False
        self.after_run_finished = False
        self.failed_snapshot: dict[str, Any] | None = None
        self.failures = 0

    async def after_run(self, **kwargs: Any) -> None:
        self.after_run_finished = False
        await super().after_run(**kwargs)
        self.after_run_finished = True

    def flush(self, state: dict[str, Any]) -> None:
        if self.fail_final_flush and self.after_run_finished:
            binding = current_durable_history_binding()
            assert binding is not None
            self.failed_snapshot = deepcopy(binding.state_provider.state.to_dict())
            self.failures += 1
            raise OSError("final durable history flush failed")
        super().flush(state)


class _FailAfterFirstServiceCall(ToolChatClient):
    def _inner_get_response(
        self, *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        # The first call requests a real tool. Every subsequent model attempt fails,
        # including any inappropriate non-streaming fallback made by the entity.
        self.fail = bool(self.received_messages)
        return super()._inner_get_response(messages=messages, stream=stream, options=options, **kwargs)


class _InterruptedStreamClient(ToolChatClient):
    """Yield a model update, then fail after an optional real core tool invocation."""

    def __init__(self, *, use_tool: bool) -> None:
        super().__init__(tool_calls=False)
        self.use_tool = use_tool
        self.stream_modes: list[bool] = []

    def _inner_get_response(
        self, *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        self.received_messages.append(deepcopy(list(messages)))
        self.received_options.append(dict(options))
        self.stream_modes.append(stream)
        has_result = any(
            content.type == "function_result" and content.call_id == "boundary-lookup"
            for message in messages
            for content in message.contents
        )
        calls_tool = self.use_tool and not has_result
        contents = (
            [Content.from_function_call("boundary-lookup", "lookup", arguments={"key": "durable"})]
            if calls_tool
            else [Content.from_text("partial answer")]
        )
        response = ChatResponse(
            messages=[Message("assistant", contents)],
            response_id=f"boundary-response-{len(self.received_messages)}",
            finish_reason="tool_calls" if calls_tool else "stop",
        )
        if stream:

            async def updates() -> AsyncIterable[ChatResponseUpdate]:
                yield ChatResponseUpdate(
                    role="assistant",
                    contents=contents,
                    response_id=response.response_id,
                    finish_reason=response.finish_reason,
                )
                if not calls_tool:
                    raise RuntimeError("model stream interrupted after an update")

            return ResponseStream(updates(), finalizer=ChatResponse.from_updates)

        async def get() -> ChatResponse:
            # A second invocation can succeed, but it must never hide a failed stream
            # or repeat the tool requested by the first invocation.
            return response

        return get()


class _NonStreamingClient(RecordingChatClient):
    def get_response(self, messages: Any, *, stream: bool = False, **kwargs: Any) -> Any:
        if stream:
            raise TypeError("stream is not supported")
        return super().get_response(messages, stream=False, **kwargs)


class _RecordingCallback:
    def __init__(self) -> None:
        self.updates: list[AgentResponseUpdate] = []
        self.responses: list[AgentResponse] = []

    async def on_streaming_response_update(self, update: AgentResponseUpdate, context: AgentCallbackContext) -> None:
        self.updates.append(deepcopy(update))

    async def on_agent_response(self, response: AgentResponse, context: AgentCallbackContext) -> None:
        self.responses.append(response)


def _committed(provider: JsonStateProvider) -> dict[str, Any]:
    # Neither a cached state object nor a to_dict() alias proves a receipt was committed.
    return json.loads(json.dumps(provider.raw))


def _request(correlation_id: str, message: Message) -> dict[str, Any]:
    return {
        "message": message.text,
        "correlationId": correlation_id,
        "contextMessages": [deepcopy(message.to_dict())],
    }


def _assert_committed_error(
    provider: JsonStateProvider,
    correlation_id: str,
    response: AgentResponse,
    *,
    error_code: str,
    detail: str,
) -> dict[str, Any]:
    raw = _committed(provider)
    assert raw["schemaVersion"] == "2.0.0"
    data = raw["data"]
    mailbox = data["responseMailbox"][correlation_id]
    assert mailbox["response"] == json.loads(json.dumps(response.to_dict()))
    assert data["completedCorrelations"][correlation_id] == {"completedAt": mailbox["createdAt"]}
    assert datetime.fromisoformat(mailbox["expiresAt"]) > datetime.fromisoformat(mailbox["createdAt"])
    delivered = DurableAgentState.from_json(json.dumps(raw)).try_get_agent_response(correlation_id)
    assert isinstance(delivered, AgentResponse)
    errors = [content for message in delivered.messages for content in message.contents if content.type == "error"]
    assert len(errors) == 1
    assert errors[0].error_code == error_code
    assert detail in (errors[0].message or "")
    assert detail in delivered.text
    assert delivered.value is None
    return data


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
async def test_external_load_failure_commits_error_without_consuming_projected_input(per_call: bool) -> None:
    external = _RecoverableExternalHistory()
    client = ToolChatClient(tool_calls=False)
    agent = Agent(
        client=client,
        context_providers=[external],
        require_per_service_call_history_persistence=per_call,
    )
    provider = JsonStateProvider()
    entity = AgentEntity(agent, state_provider=provider)
    message = Message("user", ["same projected payload"], message_id="upstream-0")
    request = _request("external-failed", message)

    failed = await entity.run(request)

    data = _assert_committed_error(
        provider, "external-failed", failed, error_code="OSError", detail="temporary external history outage"
    )
    assert provider.writes == 1
    assert external.read_sessions and set(external.read_sessions) == {provider.core_session_id}
    assert external.saved == [] and client.received_messages == []
    assert data["conversationHistory"] == []
    assert "upstream-0" not in data.get("ingestedMessages", {})
    original_failure = deepcopy(data["responseMailbox"]["external-failed"])
    original_receipt = deepcopy(data["completedCorrelations"]["external-failed"])

    external.fail_reads = False
    cold_provider = JsonStateProvider(_committed(provider))
    cold = AgentEntity(agent, state_provider=cold_provider)
    reads_before_duplicate = len(external.read_sessions)
    duplicate = await cold.run(request)
    assert duplicate.to_dict() == failed.to_dict()
    assert len(external.read_sessions) == reads_before_duplicate
    assert client.received_messages == [] and cold_provider.writes == 0

    # Same identity AND payload, but a new execution. The failed read never delivered it.
    recovered = await cold.run(_request("external-recovered", message))
    assert recovered.text == "answer-1"
    assert [[item.to_dict() for item in batch] for batch in client.received_messages] == [[message.to_dict()]]
    assert [[item.text for item in batch] for batch in external.saved] == [[message.text, "answer-1"]]
    assert set(external.read_sessions) == {cold_provider.core_session_id}
    saved = _committed(cold_provider)["data"]
    assert saved["conversationHistory"] == []
    assert saved["ingestedMessages"] == {"upstream-0": [message_identity(message)]}
    assert saved["responseMailbox"]["external-failed"] == original_failure
    assert saved["completedCorrelations"]["external-failed"] == original_receipt
    assert cold_provider.writes == 1

    before = _committed(cold_provider)
    reads_before_duplicate = len(external.read_sessions)
    assert (await cold.run(request)).to_dict() == failed.to_dict()
    assert len(external.read_sessions) == reads_before_duplicate
    assert len(client.received_messages) == 1 and len(external.saved) == 1
    assert _committed(cold_provider) == before and cold_provider.writes == 1


async def test_lazy_invalid_structured_value_is_a_committed_error_with_provider_control_state() -> None:
    session = AgentSession(session_id="revision-session")
    control_state = {"approval": {"call_id": "pending-approval", "approved": False}, "cursor": [1, 3]}
    session.state = {"control": deepcopy(control_state), "foreign-provider": {"pending": ["keep"]}}
    initial = DurableAgentState()
    initial.data.session = session.to_dict()
    provider = JsonStateProvider(json.loads(initial.to_json()))
    client: Any = RecordingChatClient()
    control = _ControlProvider()
    agent = Agent(client=client, context_providers=[control])
    entity = AgentEntity(agent, state_provider=provider)
    request = {
        "message": "return structured output",
        "correlationId": "invalid-json",
        "options": {"response_format": {"type": "object", "properties": {"answer": {"type": "integer"}}}},
    }

    response = await entity.run(request)

    data = _assert_committed_error(
        provider, "invalid-json", response, error_code="ValueError", detail="Response text is not valid JSON"
    )
    assert provider.writes == 1 and len(client.received_messages) == 1
    assert control.loaded == [control_state]
    assert len(control.responses) == 1 and control.responses[0].text == "reply-1"
    # This is core's actual lazy parser, not a fabricated exception from a response mock.
    with pytest.raises(ValueError, match="not valid JSON"):
        _ = control.responses[0].value
    expected_control = {**control_state, "before_runs": 1, "after_runs": 1}
    assert data["session"]["state"]["control"] == expected_control
    assert data["session"]["state"]["foreign-provider"] == {"pending": ["keep"]}

    cold_control = _ControlProvider()
    cold_provider = JsonStateProvider(_committed(provider))
    cold = AgentEntity(Agent(client=client, context_providers=[cold_control]), state_provider=cold_provider)
    assert (await cold.run(request)).to_dict() == response.to_dict()
    assert cold_control.loaded == [] and cold_control.responses == []
    assert len(client.received_messages) == 1 and cold_provider.writes == 0
    await cold.run({"message": "continue without a schema", "correlationId": "after-invalid-json"})
    assert cold_control.loaded == [expected_control]
    assert _committed(cold_provider)["data"]["session"]["state"]["foreign-provider"] == {"pending": ["keep"]}


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
@pytest.mark.parametrize("explicit_run_id", [False, True], ids=["default-id", "default-and-run-id"])
async def test_store_false_removes_default_and_saved_conversation_ids_without_mutating_caller(
    per_call: bool, explicit_run_id: bool
) -> None:
    class ServiceClient(ToolChatClient):
        STORES_BY_DEFAULT = True

    client = ServiceClient(tool_calls=False)
    defaults: ChatOptions = {"conversation_id": "stale", "store": True, "metadata": {"labels": ["caller"]}}
    caller_defaults = deepcopy(defaults)
    agent = Agent(
        client=client,
        default_options=defaults,
        require_per_service_call_history_persistence=per_call,
    )
    original_options = agent.default_options
    original_options_value = deepcopy(original_options)
    original_providers = agent.context_providers
    provider = JsonStateProvider()
    await AgentEntity(agent, state_provider=provider).run({"message": "service turn", "correlationId": "service"})
    first = _committed(provider)
    assert first["data"]["session"]["service_session_id"] == "service-thread"
    assert first["data"]["conversationHistory"] == []
    assert len(client.received_messages) == 1

    cold_provider = JsonStateProvider(first)
    cold = AgentEntity(agent, state_provider=cold_provider)
    prepared_agent = cold.agent
    options: dict[str, Any] = {"store": False}
    if explicit_run_id:
        options["conversation_id"] = "stale-per-run"
    original_run_options = deepcopy(options)
    response = await cold.run({"message": "client-owned turn", "correlationId": "local", "options": options})

    assert response.text == "answer-2"
    assert len(client.received_options) == 2
    assert client.received_options[-1]["store"] is False
    assert "conversation_id" not in client.received_options[-1]
    assert [message.text for message in client.received_messages[-1]] == ["client-owned turn"]
    assert _committed(cold_provider)["data"]["session"]["service_session_id"] == "service-thread"
    assert cold.agent is prepared_agent
    assert agent.default_options is original_options and agent.default_options == original_options_value
    assert agent.context_providers is original_providers
    assert defaults == caller_defaults and options == original_run_options
    assert current_durable_history_binding() is None


async def test_final_flush_failure_unbinds_restores_agent_and_rolls_back_all_local_state() -> None:
    history = _FinalFlushFailureHistory()
    control = _ControlProvider()
    client = ToolChatClient(tool_calls=False)
    agent = Agent(
        client=client,
        default_options={"conversation_id": "stale", "store": False},
        context_providers=[history, control],
    )
    initial = DurableAgentState()
    initial.data.session = AgentSession(session_id="revision-session", service_session_id="saved-service-id").to_dict()
    provider = JsonStateProvider(json.loads(initial.to_json()))
    entity = AgentEntity(agent, state_provider=provider)
    await entity.run(_request("previous", Message("user", ["previous input"], message_id="previous-input")))
    before = _committed(provider)
    original_state = entity.state
    original_agent = entity.agent
    original_options = agent.default_options
    original_options_value = deepcopy(original_options)
    history.fail_final_flush = True
    request = _request("flush-failed", Message("user", ["uncommitted input"], message_id="uncommitted-input"))
    assert current_durable_history_binding() is None

    with pytest.raises(OSError, match="final durable history flush failed"):
        await entity.run(request)

    assert history.failures == 1
    assert history.failed_snapshot is not None
    assert any(
        entry.get("correlationId") == "flush-failed" for entry in history.failed_snapshot["data"]["conversationHistory"]
    ), "the failure must occur after real history hooks staged this turn"
    assert len(client.received_messages) == 2 and len(control.responses) == 2
    assert current_durable_history_binding() is None
    assert control.agents[-1] is not original_agent, "exercise the temporary default-options clone"
    assert control.sessions[-1].service_session_id == "saved-service-id"
    assert entity.agent is original_agent
    assert agent.default_options is original_options and agent.default_options == original_options_value
    assert entity.state is original_state and entity.state.to_dict() == before
    assert _committed(provider) == before and provider.writes == 1
    assert entity.state.try_get_agent_response("flush-failed") is None
    assert "uncommitted-input" not in _committed(provider)["data"]["ingestedMessages"]

    history.fail_final_flush = False
    response = await entity.run(request)
    assert response.text == "answer-3"
    assert len(client.received_messages) == 3 and provider.writes == 2
    assert control.loaded[-1] == before["data"]["session"]["state"]["control"]
    assert "flush-failed" in _committed(provider)["data"]["completedCorrelations"]
    assert current_durable_history_binding() is None and entity.agent is original_agent


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
@pytest.mark.parametrize("after_first_response", [False, True], ids=["before-first-response", "after-first-response"])
async def test_failed_model_turn_consumes_only_inputs_actually_saved_by_history(
    per_call: bool, after_first_response: bool
) -> None:
    prior = Message("user", ["previously consumed"], message_id="prior-input")
    initial = DurableAgentState()
    initial.data.ingested_messages = {"prior-input": [message_identity(prior)]}
    provider = JsonStateProvider(json.loads(initial.to_json()))
    history = CountingHistory([])
    client = _FailAfterFirstServiceCall() if after_first_response else ToolChatClient(fail=True)
    tool_calls: list[str] = []

    @tool(name="lookup", approval_mode="never_require")
    def lookup(key: str) -> str:
        tool_calls.append(key)
        return f"value:{key}"

    agent = Agent(
        client=client,
        tools=[lookup],
        context_providers=[history],
        require_per_service_call_history_persistence=per_call,
    )
    entity = AgentEntity(agent, state_provider=provider)
    message = Message("user", ["Use lookup for durable."], message_id="current-input")
    request = _request("failed-model", message)

    response = await entity.run(request)

    data = _assert_committed_error(
        provider, "failed-model", response, error_code="RuntimeError", detail="model failed before history persistence"
    )
    was_saved = per_call and after_first_response
    assert history.after_calls == int(was_saved)
    assert tool_calls == (["durable"] if after_first_response else [])
    expected_receipts = {"prior-input": [message_identity(prior)]}
    if was_saved:
        expected_receipts["current-input"] = [message_identity(message)]
    assert data["ingestedMessages"] == expected_receipts
    stored_inputs = [
        item
        for entry in data["conversationHistory"]
        if entry["$type"] == "request" and entry.get("correlationId") == "failed-model"
        for item in entry["messages"]
    ]
    if was_saved:
        assert len(stored_inputs) == 2
        assert stored_inputs[0]["messageId"] == "current-input"
        assert stored_inputs[1]["role"] == "tool"
        assert stored_inputs[1]["contents"][0]["$type"] == "functionResult"
    else:
        assert stored_inputs == []
    assert provider.writes == 1
    calls_before_duplicate = len(client.received_messages)
    assert (await entity.run(request)).to_dict() == response.to_dict()
    assert len(client.received_messages) == calls_before_duplicate and provider.writes == 1

    healthy_client = ToolChatClient(tool_calls=False)
    probe = _ControlProvider()
    cold = AgentEntity(
        Agent(
            client=healthy_client,
            context_providers=[probe],
            require_per_service_call_history_persistence=per_call,
        ),
        state_provider=JsonStateProvider(_committed(provider)),
    )
    recovered = await cold.run(_request("model-recovered", message))
    assert recovered.text == "answer-1"
    expected_input_ids = [[]] if was_saved else [["current-input"]]
    assert [[item.message_id for item in batch] for batch in probe.inputs] == expected_input_ids
    assert [item.message_id for item in healthy_client.received_messages[0]].count("current-input") == 1


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-service-call"])
async def test_partial_external_save_does_not_claim_a_local_ingestion_receipt(per_call: bool) -> None:
    external = _RecoverableExternalHistory()
    external.fail_reads = False
    client = _FailAfterFirstServiceCall()
    tool_calls: list[str] = []

    @tool(name="lookup", approval_mode="never_require")
    def lookup(key: str) -> str:
        tool_calls.append(key)
        return f"value:{key}"

    agent = Agent(
        client=client,
        tools=[lookup],
        context_providers=[external],
        require_per_service_call_history_persistence=per_call,
    )
    provider = JsonStateProvider()
    entity = AgentEntity(agent, state_provider=provider)
    message = Message("user", ["Use lookup for durable."], message_id="external-partial-input")
    request = _request("external-partial", message)

    response = await entity.run(request)

    data = _assert_committed_error(
        provider,
        "external-partial",
        response,
        error_code="RuntimeError",
        detail="model failed before history persistence",
    )
    assert tool_calls == ["durable"] and provider.writes == 1
    assert len(external.saved) == int(per_call)
    if per_call:
        assert external.saved[0][0].message_id == message.message_id
        assert any(content.type == "function_call" for item in external.saved[0] for content in item.contents)
    assert data["conversationHistory"] == []
    # External appends are outside the entity transaction. Even a saved first call
    # cannot establish a portable local receipt for the interrupted whole run.
    assert "external-partial-input" not in data.get("ingestedMessages", {})
    calls_before_duplicate = len(client.received_messages)
    saved_before_duplicate = deepcopy(external.saved)
    assert (await entity.run(request)).to_dict() == response.to_dict()
    assert len(client.received_messages) == calls_before_duplicate
    assert [[item.to_dict() for item in batch] for batch in external.saved] == [
        [item.to_dict() for item in batch] for batch in saved_before_duplicate
    ]
    assert tool_calls == ["durable"] and provider.writes == 1


@pytest.mark.parametrize("use_tool", [False, True], ids=["model-stream", "tool-then-model-stream"])
async def test_started_stream_failure_is_not_reexecuted_as_a_non_streaming_run(use_tool: bool) -> None:
    client = _InterruptedStreamClient(use_tool=use_tool)
    tool_calls: list[str] = []

    @tool(name="lookup", approval_mode="never_require")
    def lookup(key: str) -> str:
        tool_calls.append(key)
        return f"value:{key}"

    agent = _CountingAgent(client=client, tools=[lookup] if use_tool else [])
    callback = _RecordingCallback()
    provider = JsonStateProvider()
    entity = AgentEntity(agent, callback=callback, state_provider=provider)
    request = {"message": "start the operation", "correlationId": "interrupted-stream"}

    response = await entity.run(request)

    assert any(update.text == "partial answer" for update in callback.updates), "the model stream must actually start"
    if use_tool:
        assert any(content.type == "function_result" for update in callback.updates for content in update.contents)
    assert agent.run_modes == [True], "a runtime stream failure must not start another agent/tool execution"
    assert client.stream_modes == [True] * (2 if use_tool else 1)
    assert tool_calls == (["durable"] if use_tool else [])
    assert callback.responses == []
    _assert_committed_error(
        provider,
        "interrupted-stream",
        response,
        error_code="RuntimeError",
        detail="model stream interrupted after an update",
    )
    assert provider.writes == 1
    cold_provider = JsonStateProvider(_committed(provider))
    cold = AgentEntity(agent, state_provider=cold_provider)
    assert (await cold.run(request)).to_dict() == response.to_dict()
    assert agent.run_modes == [True] and cold_provider.writes == 0
    assert tool_calls == (["durable"] if use_tool else [])


async def test_unsupported_stream_type_error_still_allows_one_non_streaming_invocation() -> None:
    client = _NonStreamingClient()
    agent = _CountingAgent(client=client)
    callback = _RecordingCallback()
    provider = JsonStateProvider()
    entity = AgentEntity(agent, callback=callback, state_provider=provider)
    request = {"message": "non-streaming client", "correlationId": "unsupported-stream"}

    response = await entity.run(request)

    assert agent.run_modes == [True, False]
    assert len(client.received_messages) == 1 and response.text == "reply-1"
    assert callback.updates == [] and len(callback.responses) == 1
    assert callback.responses[0] is not response
    assert callback.responses[0].to_dict() == response.to_dict()
    assert callback.responses[0].messages[0] is not response.messages[0]
    data = _committed(provider)["data"]
    assert data["responseMailbox"]["unsupported-stream"]["response"] == response.to_dict()
    assert "unsupported-stream" in data["completedCorrelations"] and provider.writes == 1
    assert (await entity.run(request)).to_dict() == response.to_dict()
    assert agent.run_modes == [True, False] and provider.writes == 1


def test_registration_fails_if_public_provider_list_cannot_be_replaced() -> None:
    class ReadOnlyProvidersAgent(_CountingAgent):
        @property
        def context_providers(self) -> list[ContextProvider]:
            return self._context_providers

        @context_providers.setter
        def context_providers(self, providers: list[ContextProvider]) -> None:
            if hasattr(self, "_context_providers"):
                raise AttributeError("context_providers cannot be replaced after construction")
            self._context_providers = providers

    agent = ReadOnlyProvidersAgent(
        client=RecordingChatClient(),
        name="read-only-providers",
        context_providers=[InMemoryHistoryProvider("registered-history")],
    )
    original = agent.context_providers
    provider = JsonStateProvider()
    with pytest.raises(ValueError, match="attach durable history"):
        AgentEntity(agent, state_provider=provider)
    assert agent.context_providers is original and agent.run_modes == []
    assert isinstance(original[0], InMemoryHistoryProvider)
    assert _committed(provider) == {} and provider.writes == 0


def test_registration_fails_if_core_agent_cannot_be_copied() -> None:
    class UncopyableAgent(_CountingAgent):
        def __copy__(self) -> Any:
            raise TypeError("agent cannot be copied")

    agent = UncopyableAgent(client=RecordingChatClient(), context_providers=[InMemoryHistoryProvider()])
    original = agent.context_providers
    provider = JsonStateProvider()
    with pytest.raises(ValueError, match="attach durable history"):
        AgentEntity(agent, state_provider=provider)
    assert agent.context_providers is original and agent.run_modes == []
    assert _committed(provider) == {} and provider.writes == 0


async def test_reset_clears_local_context_but_keeps_delivery_and_ingestion_receipts() -> None:
    initial = DurableAgentState().to_dict()
    initial["futureRoot"] = {"opaque": [1, 2]}
    initial["data"]["futureData"] = {"opaque": [3, 4]}
    initial["data"]["conversationHistory"] = [{"$type": "future-kind", "opaque": ["old local history"]}]
    provider = JsonStateProvider(initial)
    control = _ControlProvider()
    initial_client: Any = RecordingChatClient()
    entity = AgentEntity(Agent(client=initial_client, context_providers=[control]), state_provider=provider)
    message = Message("user", ["before reset"], message_id="before-reset-input")
    request = _request("before-reset", message)
    original_response = await entity.run(request)
    before = _committed(provider)
    assert len(before["data"]["conversationHistory"]) > 1
    assert before["data"]["session"]["state"]["control"] == {"before_runs": 1, "after_runs": 1}

    entity.reset()

    reset = _committed(provider)
    assert reset["data"]["conversationHistory"] == []
    assert "session" not in reset["data"]
    # Explicit reset may delete even opaque local history; unrelated data is not history.
    assert reset["futureRoot"] == before["futureRoot"]
    assert reset["data"]["futureData"] == before["data"]["futureData"]
    for field in ("responseMailbox", "completedCorrelations", "ingestedMessages"):
        assert reset["data"][field] == before["data"][field]
    assert provider.writes == 2

    client: Any = RecordingChatClient()
    cold_control = _ControlProvider()
    cold_provider = JsonStateProvider(reset)
    cold = AgentEntity(Agent(client=client, context_providers=[cold_control]), state_provider=cold_provider)
    assert (await cold.run(request)).to_dict() == original_response.to_dict()
    assert client.received_messages == [] and cold_control.loaded == [] and cold_provider.writes == 0
    await cold.run(_request("after-reset", Message("user", ["after reset"], message_id="after-reset-input")))
    assert cold_control.loaded == [{}]
    assert [[item.text for item in batch] for batch in client.received_messages] == [["after reset"]]
    saved = _committed(cold_provider)
    assert saved["data"]["session"]["state"]["control"] == {"before_runs": 1, "after_runs": 1}
    assert saved["data"]["responseMailbox"]["before-reset"] == before["data"]["responseMailbox"]["before-reset"]
    assert (
        saved["data"]["completedCorrelations"]["before-reset"]
        == before["data"]["completedCorrelations"]["before-reset"]
    )
    assert saved["data"]["ingestedMessages"]["before-reset-input"] == [message_identity(message)]
    assert (await cold.run(request)).to_dict() == original_response.to_dict()
    assert len(client.received_messages) == 1 and cold_provider.writes == 1


@pytest.mark.parametrize("legacy", [False, True], ids=["writer", "legacy-migration"])
def test_response_writer_and_migration_keep_completion_evidence_after_payload_expiry(legacy: bool) -> None:
    response = AgentResponse(messages=[Message("assistant", ["original result"])])
    state = DurableAgentState("1.1.0" if legacy else "2.0.0")
    if legacy:
        state.data.conversation_history.append(DurableAgentStateResponse.from_run_response("completed", response))
        source = state.to_dict()
        state = migrate_legacy_state(
            source,
            source_digest=state_snapshot_digest(source),
            source_session_id="source-session",
            migration_id="expiry-migration",
            ownership_transfer_id="quiesced-owner",
            delivery_window_seconds=3600,
        )
    else:
        state.record_response("completed", response, delivery_window_seconds=3600)
    raw = json.loads(state.to_json())
    assert raw["schemaVersion"] == "2.0.0"
    mailbox = raw["data"]["responseMailbox"]["completed"]
    receipt = raw["data"]["completedCorrelations"]["completed"]
    assert receipt["completedAt"] == mailbox["createdAt"]
    assert receipt.get("legacy", False) is legacy

    restored = DurableAgentState.from_json(json.dumps(raw))
    restored.expire_responses(now=datetime.fromisoformat(mailbox["expiresAt"]))
    expired = DurableAgentState.from_json(restored.to_json())
    assert expired.data.response_mailbox == {}
    assert expired.data.completed_correlations["completed"] == receipt
    delivered = expired.try_get_agent_response("completed")
    assert isinstance(delivered, AgentResponse)
    assert delivered.additional_properties["durable_status"] == "already_completed"
    assert delivered.messages[0].contents[0].error_code == "response_expired"
    before = expired.to_json()
    expired.record_response(
        "completed", AgentResponse(messages=[Message("assistant", ["replacement"])]), delivery_window_seconds=3600
    )
    assert expired.to_json() == before


@pytest.mark.parametrize("expired", [False, True], ids=["live-mailbox", "expired-mailbox"])
@pytest.mark.parametrize("receipt_shape", ["missing-container", "empty-container", "unrelated-receipt"])
def test_version_two_mailbox_without_matching_receipt_is_rejected_on_initial_read(
    expired: bool, receipt_shape: str
) -> None:
    state = DurableAgentState()
    now = datetime.now(timezone.utc) - (timedelta(days=2) if expired else timedelta())
    state.record_response(
        "completed",
        AgentResponse(messages=[Message("assistant", ["original result"])]),
        delivery_window_seconds=3600,
        now=now,
    )
    raw = json.loads(state.to_json())
    # Positive control: this is an otherwise valid writer-produced mailbox and receipt.
    assert DurableAgentState.from_json(json.dumps(raw)).to_dict() == raw
    receipt = raw["data"]["completedCorrelations"].pop("completed")
    if receipt_shape == "missing-container":
        del raw["data"]["completedCorrelations"]
    elif receipt_shape == "unrelated-receipt":
        raw["data"]["completedCorrelations"]["different-correlation"] = receipt
    original = deepcopy(raw)

    # An orphan mailbox must not be the last completion evidence: expiry could delete
    # it and reopen execution. Reject corruption before polling or writing another turn.
    with pytest.raises(ValueError):
        DurableAgentState.from_dict(raw)
    with pytest.raises(ValueError):
        DurableAgentState.from_json(json.dumps(original))
    assert raw == original
