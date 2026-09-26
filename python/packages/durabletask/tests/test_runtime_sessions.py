# Copyright (c) Microsoft. All rights reserved.

"""Runtime session restoration boundaries for durable entity execution."""

from __future__ import annotations

from collections.abc import AsyncIterable, Awaitable, Mapping, Sequence
from copy import deepcopy
from typing import Any, cast

import pytest
from _execution_test_support import JsonStateProvider, NonStreamingAgent, RecordingChatClient, ToolChatClient
from _session_persistence_test_support import _cold, _request
from agent_framework import (
    Agent,
    AgentResponse,
    AgentSession,
    ChatOptions,
    ChatResponse,
    ChatResponseUpdate,
    ContextProvider,
    HistoryProvider,
    Message,
    ResponseStream,
    SessionContext,
)
from agent_framework._serialization import SerializationMixin

from agent_framework_durabletask import AgentEntity, DurableAgentState, DurableHistoryProvider
from agent_framework_durabletask._history_provider import current_durable_history_binding
from agent_framework_durabletask._message_identity import message_identity


def _committed(provider: JsonStateProvider) -> DurableAgentState:
    return DurableAgentState.from_dict(deepcopy(provider.raw))


def _session_payload(provider: JsonStateProvider) -> dict[str, Any]:
    payload = _committed(provider).data.session
    assert isinstance(payload, dict)
    return payload


def _snapshot_session(session: AgentSession) -> AgentSession:
    return AgentSession.from_dict(deepcopy(session.to_dict()))


class _SessionProbe(DurableHistoryProvider):
    def __init__(self) -> None:
        super().__init__(skip_excluded=False)
        self.before: list[AgentSession] = []
        self.after: list[AgentSession] = []

    async def before_run(self, *, session: AgentSession, **kwargs: Any) -> None:
        self.before.append(_snapshot_session(session))
        await super().before_run(session=session, **kwargs)

    async def after_run(self, *, session: AgentSession, **kwargs: Any) -> None:
        self.after.append(_snapshot_session(session))
        await super().after_run(session=session, **kwargs)


class _SessionMutationProbe(DurableHistoryProvider):
    def __init__(self, *, mutate_service_id: str | None = None) -> None:
        super().__init__(skip_excluded=False)
        self.mutate_service_id = mutate_service_id
        self.before_service_ids: list[str | Mapping[str, Any] | None] = []
        self.after_service_ids: list[str | Mapping[str, Any] | None] = []

    async def before_run(self, *, session: AgentSession, **kwargs: Any) -> None:
        self.before_service_ids.append(session.service_session_id)
        if self.mutate_service_id is not None:
            session.service_session_id = self.mutate_service_id
        await super().before_run(session=session, **kwargs)

    async def after_run(self, *, session: AgentSession, **kwargs: Any) -> None:
        self.after_service_ids.append(session.service_session_id)
        await super().after_run(session=session, **kwargs)


class _RecordingExternalHistory(HistoryProvider):
    def __init__(self, source_id: str = "external") -> None:
        super().__init__(source_id)
        self.calls: list[tuple[str, str | None]] = []
        self.saved: list[list[Message]] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        self.calls.append(("load", session_id))
        return []

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.calls.append(("save", session_id))
        self.saved.append(deepcopy(list(messages)))


class _StoreOnlyAudit(HistoryProvider):
    def __init__(self) -> None:
        super().__init__("audit", load_messages=False)
        self.saved: list[list[Message]] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        return []

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.saved.append(deepcopy(list(messages)))


class _InputProbe(ContextProvider):
    def __init__(self) -> None:
        super().__init__("input-probe")
        self.inputs: list[list[Message]] = []

    async def before_run(self, *, context: SessionContext, **kwargs: Any) -> None:
        self.inputs.append(deepcopy(context.input_messages))


class _PartialSaveThenRaise(HistoryProvider):
    def __init__(self) -> None:
        super().__init__("external")
        self.saved: list[Message] = []
        self.fail = True

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        return deepcopy(self.saved)

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        if not self.fail:
            self.saved.extend(deepcopy(list(messages)))
            return
        accepted = deepcopy(list(messages[:1]))
        binding = current_durable_history_binding()
        assert binding is not None
        binding.accept(accepted)
        self.saved.extend(accepted)
        raise OSError("partial save failed after accepting the first input")


class _ConversationClient(RecordingChatClient):
    def __init__(self, conversation_ids: Sequence[str | None]) -> None:
        super().__init__()
        self._conversation_ids = list(conversation_ids)

    def _next_conversation_id(self, *, store: bool) -> str | None:
        if not store or not self._conversation_ids:
            return None
        return self._conversation_ids.pop(0)

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

        response = ChatResponse(
            messages=[
                Message(
                    "assistant",
                    [f"reply-{call}"],
                    message_id=f"answer-{call}",
                    additional_properties={"model_metadata": {"calls": call}},
                )
            ],
            response_id=f"response-{call}",
            conversation_id=self._next_conversation_id(store=bool(options.get("store"))),
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


class _TypedMessageState(SerializationMixin):
    def __init__(self, label: str, count: int) -> None:
        self.label = label
        self.count = count


def _typed_state_type() -> str:
    return _TypedMessageState("stored", 1).to_dict()["type"]


class _SessionStateWriter(NonStreamingAgent):
    def __init__(self, client: RecordingChatClient) -> None:
        super().__init__(client=client, name="session-writer")

    def run(self, *args: Any, **kwargs: Any) -> Any:
        if kwargs.get("stream"):
            raise TypeError("stream is not supported")

        async def execute() -> AgentResponse[Any]:
            session = kwargs["session"]
            response = await super(_SessionStateWriter, self).run(*args, **kwargs)
            session.state.setdefault("turns", []).append(kwargs["messages"][0].text)
            session.state["typed"] = _TypedMessageState("stored", len(session.state["turns"]))
            return response

        return execute()


@pytest.mark.parametrize("stream", [False, True], ids=["non-streaming", "streaming"])
async def test_cold_restart_restores_same_session_id_to_provider_hooks(stream: bool) -> None:
    history = _SessionProbe()
    client = ToolChatClient(tool_calls=False)
    agent_type = Agent if stream else NonStreamingAgent
    agent: Agent = agent_type(client=client, name="runtime", context_providers=[history])
    provider = JsonStateProvider(session_id="same-thread", entity_name="public-entity")

    warm = AgentEntity(agent, state_provider=provider)
    first = await warm.run(_request("first", "hello"))
    assert first.text == "answer-1"

    cold_history = _SessionProbe()
    cold_client = ToolChatClient(tool_calls=False)
    cold_agent = agent_type(client=cold_client, name="runtime", context_providers=[cold_history])
    cold, provider = _cold(cold_agent, provider)
    second = await cold.run(_request("second", "again"))

    assert second.text == "answer-1"
    assert [session.session_id for session in history.before] == ["@public-entity@same-thread"]
    assert [session.session_id for session in cold_history.before] == ["@public-entity@same-thread"]
    assert [session.session_id for session in cold_history.after] == ["@public-entity@same-thread"]


async def test_cold_restart_exposes_restored_session_to_provider_before_hook() -> None:
    history = _SessionProbe()
    client = RecordingChatClient()
    agent = _SessionStateWriter(client)
    agent.context_providers = [history]  # type: ignore[attr-defined]
    provider = JsonStateProvider(session_id="restore", entity_name="qualified")

    await AgentEntity(agent, state_provider=provider).run(_request("first", "alpha"))

    cold_history = _SessionProbe()
    cold_agent = _SessionStateWriter(RecordingChatClient())
    cold_agent.context_providers = [cold_history]  # type: ignore[attr-defined]
    cold, provider = _cold(cold_agent, provider)
    await cold.run(_request("second", "beta"))

    restored = cold_history.before[0]
    assert restored.session_id == "@qualified@restore"
    assert restored.state["turns"] == ["alpha"]


async def test_public_entity_qualified_id_isolated_for_same_thread_two_agents() -> None:
    shared_thread = "shared"
    alpha_provider = JsonStateProvider(session_id=shared_thread, entity_name="Alpha")
    beta_provider = JsonStateProvider(session_id=shared_thread, entity_name="Beta")
    alpha = AgentEntity(Agent(client=RecordingChatClient(), name="alpha"), state_provider=alpha_provider)
    beta = AgentEntity(Agent(client=RecordingChatClient(), name="beta"), state_provider=beta_provider)

    await alpha.run(_request("alpha-1", "first alpha"))
    await beta.run(_request("beta-1", "first beta"))

    assert alpha_provider.core_session_id == "@Alpha@shared"
    assert beta_provider.core_session_id == "@Beta@shared"
    assert [entry.correlation_id for entry in _committed(alpha_provider).data.conversation_history] == [
        "alpha-1",
        "alpha-1",
    ]
    assert [entry.correlation_id for entry in _committed(beta_provider).data.conversation_history] == [
        "beta-1",
        "beta-1",
    ]


async def test_custom_provider_stored_state_restores_typed_message_state_via_registry() -> None:
    provider = JsonStateProvider(session_id="typed", entity_name="typed-entity")
    agent = _SessionStateWriter(RecordingChatClient())
    await AgentEntity(agent, state_provider=provider).run(_request("typed-1", "one"))

    payload = _session_payload(provider)
    assert payload["state"]["typed"]["type"] == _typed_state_type()

    cold, provider = _cold(_SessionStateWriter(RecordingChatClient()), provider)
    await cold.run(_request("typed-2", "two"))

    restored_typed = _session_payload(provider)["state"]["typed"]
    assert isinstance(restored_typed, dict)
    assert restored_typed["type"] == _typed_state_type()
    restored_session = AgentSession.from_dict(_session_payload(provider))
    typed = restored_session.state["typed"]
    assert type(typed).__name__ == "_TypedMessageState"
    assert typed.label == "stored"
    assert typed.count == 2


@pytest.mark.parametrize(
    ("stores_by_default", "default_store", "call_store", "expected_service_id"),
    [
        pytest.param(True, None, None, "service-thread-2", id="service-default"),
        pytest.param(False, None, None, "service-thread-2", id="external-default"),
        pytest.param(True, False, True, "service-thread-2", id="per-call-override-enables-service"),
    ],
)
async def test_service_true_false_true_external_primary_and_store_only_audit_do_not_duplicate_load(
    stores_by_default: bool,
    default_store: bool | None,
    call_store: bool | None,
    expected_service_id: str | None,
) -> None:
    external = _RecordingExternalHistory()
    audit = _StoreOnlyAudit()
    client = _ConversationClient(["service-thread-1", "service-thread-2"])
    # Each parametrized client has its own class so defaults cannot leak to other cases.
    type_for_case = type("ConversationClientForCase", (_ConversationClient,), {"STORES_BY_DEFAULT": stores_by_default})
    client = type_for_case(["service-thread-1", "service-thread-2"])
    options: dict[str, Any] = {}
    if default_store is not None:
        options["store"] = default_store
    agent = Agent(
        client=client,
        name="owner",
        default_options=cast(ChatOptions[None], options),
        context_providers=[external, audit],
    )
    provider = JsonStateProvider(session_id="shared", entity_name="owner")

    for turn, store in enumerate((True, False, True), start=1):
        if turn > 1:
            entity, provider = _cold(agent, provider)
        else:
            entity = AgentEntity(agent, state_provider=provider)
        request_options = {"store": call_store if turn == 1 and call_store is not None else store}
        await entity.run(_request(f"corr-{turn}", f"turn-{turn}", options=request_options))

    assert external.calls == [("load", "@owner@shared"), ("save", "@owner@shared")]
    assert [message.text for batch in external.saved for message in batch] == ["turn-2", "reply-2"]
    assert [message.text for batch in audit.saved for message in batch] == [
        "turn-1",
        "reply-1",
        "turn-2",
        "reply-2",
        "turn-3",
        "reply-3",
    ]
    final_session = AgentSession.from_dict(_session_payload(provider))
    assert final_session.service_session_id == expected_service_id


async def test_store_false_ignores_inactive_conversation_id_but_restores_original_service_id_after_run() -> None:
    history = _SessionMutationProbe()
    client = _ConversationClient([])
    agent = NonStreamingAgent(
        client=client,
        name="owner",
        default_options={"store": False, "conversation_id": "inactive-default-id"},
        context_providers=[history],
    )
    initial = DurableAgentState()
    session = AgentSession(session_id="runtime-session", service_session_id="saved-service-id")
    session.state = {"keep": True}
    initial.data.session = session.to_dict()
    provider = JsonStateProvider(deepcopy(initial.to_dict()))

    await AgentEntity(agent, state_provider=provider).run(_request("turn-1", "hello"))

    assert history.before_service_ids == [None]
    assert history.after_service_ids == [None]
    restored = AgentSession.from_dict(_session_payload(provider))
    assert restored.service_session_id == "saved-service-id"
    assert restored.state == {"keep": True}


async def test_unknown_session_siblings_preserved_through_cold_restart_and_next_commit() -> None:
    initial = DurableAgentState()
    session = AgentSession(session_id="runtime-session", service_session_id="saved-service-id")
    session.state = {"keep": [False, 0]}
    initial.data.session = {**session.to_dict(), "name": "original", "default": {"audit": True}}
    provider = JsonStateProvider(deepcopy(initial.to_dict()))
    entity = AgentEntity(Agent(client=RecordingChatClient(), name="owner"), state_provider=provider)

    await entity.run(_request("turn-1", "hello"))
    cold, provider = _cold(Agent(client=RecordingChatClient(), name="owner"), provider)
    await cold.run(_request("turn-2", "again"))

    payload = _session_payload(provider)
    assert payload["name"] == "original"
    assert payload["default"] == {"audit": True}
    assert payload["state"] == {"keep": [False, 0]}


async def test_partial_acceptance_records_ingestion_for_unsaved_inputs_only_when_later_failure_occurs() -> None:
    provider = JsonStateProvider()
    history = _PartialSaveThenRaise()
    probe = _InputProbe()
    client = RecordingChatClient()
    agent = Agent(client=client, name="partial", context_providers=[probe, history])
    entity = AgentEntity(agent, state_provider=provider)
    accepted = Message("user", ["accepted"], message_id="shared")
    unaccepted = Message("user", ["unaccepted"], message_id="shared")
    request = _request(
        "corr-1",
        "question",
        contextMessages=[message.to_dict() for message in (accepted, unaccepted)],
        contextMessageIds=["accepted-occurrence", "unaccepted-occurrence"],
    )

    response = await entity.run(request)

    receipts = entity.state.data.ingested_messages
    assert "partial save failed after accepting the first input" in response.text
    assert [message.text for message in probe.inputs[0]] == ["accepted", "unaccepted"]
    assert receipts == {"accepted-occurrence": [message_identity(accepted)]}
    assert _committed(provider).data.completed_correlations["corr-1"]["outcome"] == "failed"

    history.fail = False
    retry_probe = _InputProbe()
    retry_client = RecordingChatClient()
    retry_agent = Agent(client=retry_client, name="partial", context_providers=[retry_probe, history])
    cold, provider = _cold(retry_agent, provider)
    retry = await cold.run(
        _request(
            "corr-2",
            "question",
            contextMessages=[message.to_dict() for message in (accepted, unaccepted)],
            contextMessageIds=["accepted-occurrence", "unaccepted-occurrence"],
        )
    )

    assert retry.text == "reply-1"
    assert [message.text for message in retry_probe.inputs[0]] == ["unaccepted"]


async def test_cold_retry_reexecutes_only_unsaved_turn_after_commit_failure() -> None:
    provider = JsonStateProvider()
    provider.fail_before_write = True
    client = RecordingChatClient()
    agent = Agent(client=client, name="retry")
    entity = AgentEntity(agent, state_provider=provider)

    with pytest.raises(OSError, match="injected commit failure"):
        await entity.run(_request("corr-1", "question"))

    provider.fail_before_write = False
    cold, provider = _cold(agent, provider)
    response = await cold.run(_request("corr-1", "question"))
    duplicate_entity, provider = _cold(agent, provider)
    duplicate = await duplicate_entity.run(_request("corr-1", "question"))

    assert response.text == "reply-2"
    assert duplicate.to_dict() == response.to_dict()
    assert len(client.received_messages) == 2


async def test_service_owned_turn_restores_new_service_id_after_cold_restart() -> None:
    client = _ConversationClient(["service-1", "service-2"])
    agent = Agent(
        client=client, name="service-owner", default_options={"store": True}, context_providers=[_StoreOnlyAudit()]
    )
    provider = JsonStateProvider(session_id="service", entity_name="service-owner")

    first = AgentEntity(agent, state_provider=provider)
    await first.run(_request("corr-1", "hello"))
    second, provider = _cold(agent, provider)
    await second.run(_request("corr-2", "again"))

    restored = AgentSession.from_dict(_session_payload(provider))
    assert restored.service_session_id == "service-2"


async def test_external_primary_restores_original_service_id_even_if_provider_mutates_session() -> None:
    history = _SessionMutationProbe(mutate_service_id="provider-mutated")
    client = _ConversationClient([])
    agent = NonStreamingAgent(
        client=client,
        name="external",
        default_options={"store": False},
        context_providers=[history],
    )
    initial = DurableAgentState()
    session = AgentSession(session_id="runtime-session", service_session_id="saved-service-id")
    initial.data.session = session.to_dict()
    provider = JsonStateProvider(deepcopy(initial.to_dict()))

    await AgentEntity(agent, state_provider=provider).run(_request("corr-1", "hello"))

    restored = AgentSession.from_dict(_session_payload(provider))
    assert history.before_service_ids == [None]
    assert history.after_service_ids == ["provider-mutated"]
    assert restored.service_session_id == "saved-service-id"


async def test_accepted_result_cold_restart_does_not_reexecute_same_correlation() -> None:
    provider = JsonStateProvider()
    client = RecordingChatClient()
    entity = AgentEntity(Agent(client=client, name="accepted"), state_provider=provider)

    first = await entity.run(_request("same", "hello"))
    duplicate_entity, provider = _cold(Agent(client=client, name="accepted"), provider)
    duplicate = await duplicate_entity.run(_request("same", "hello"))

    assert duplicate.to_dict() == first.to_dict()
    assert len(client.received_messages) == 1


async def test_store_only_audit_receives_messages_once_per_turn_after_cold_restarts() -> None:
    audit = _StoreOnlyAudit()
    agent = Agent(client=ToolChatClient(tool_calls=False), name="audit", context_providers=[audit])
    provider = JsonStateProvider(session_id="audit-session", entity_name="audit")

    await AgentEntity(agent, state_provider=provider).run(_request("corr-1", "one"))
    cold, provider = _cold(agent, provider)
    await cold.run(_request("corr-2", "two"))

    assert [message.text for batch in audit.saved for message in batch] == ["one", "answer-1", "two", "answer-2"]
