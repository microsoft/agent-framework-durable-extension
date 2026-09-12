# Copyright (c) Microsoft. All rights reserved.

"""Durable history substitution and ownership unit tests with recording doubles, without live services."""

import json
from collections.abc import AsyncIterable, Awaitable, Sequence
from copy import deepcopy
from typing import Any

import pytest
from agent_framework import (
    Agent,
    AgentSession,
    ChatResponse,
    ChatResponseUpdate,
    Content,
    ContextProvider,
    HistoryProvider,
    InMemoryHistoryProvider,
    Message,
    ResponseStream,
    SessionContext,
)

from agent_framework_durabletask import (
    AgentEntity,
    AgentEntityStateProviderMixin,
    DurableAgentState,
    DurableHistoryProvider,
    _entities,
)
from agent_framework_durabletask._history_provider import ensure_durable_history


class _StubClient:
    """Chat client stand-in that stores history locally (the common case)."""

    STORES_BY_DEFAULT = False

    def __init__(self) -> None:
        self.additional_properties: dict[str, Any] = {}


class _ServiceStoringClient(_StubClient):
    """Chat client whose service keeps the conversation server-side."""

    STORES_BY_DEFAULT = True


class _RecordingClient(_StubClient):
    """Client that records the message list handed to it on each call.

    Needed to tell "the provider is attached" apart from "the provider is answering", which is the
    distinction that keeps a service-backed agent from being sent its own transcript.
    """

    def __init__(self) -> None:
        super().__init__()
        self.received: list[list[Message]] = []
        self._counter = 0

    def get_response(
        self,
        messages: str | Message | list[str] | list[Message],
        *,
        stream: bool = False,
        options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        options = options or {}
        normalized = [m for m in messages if isinstance(m, Message)] if isinstance(messages, list) else []
        self.received.append(normalized)

        if stream:
            return self._stream(options)

        async def _get() -> ChatResponse:
            self._counter += 1
            return ChatResponse(messages=Message(role="assistant", contents=[f"reply-{self._counter}"]))

        return _get()

    def _stream(self, options: dict[str, Any]) -> ResponseStream[ChatResponseUpdate, ChatResponse]:
        async def _updates() -> AsyncIterable[ChatResponseUpdate]:
            self._counter += 1
            yield ChatResponseUpdate(contents=[Content.from_text(f"reply-{self._counter}")], role="assistant")

        def _finalize(updates: Sequence[ChatResponseUpdate]) -> ChatResponse:
            return ChatResponse.from_updates(updates, output_format_type=options.get("response_format"))

        return ResponseStream(_updates(), finalizer=_finalize)


class _RecordingServiceClient(_RecordingClient):
    """The same, but its service keeps the conversation server-side."""

    STORES_BY_DEFAULT = True


class _ConversationIdClient(_StubClient):
    """Recording double that returns conversation IDs through core response types."""

    def __init__(self, *, stores_by_default: bool, supports_streaming: bool) -> None:
        super().__init__()
        self.STORES_BY_DEFAULT = stores_by_default
        self.supports_streaming = supports_streaming
        self.calls: list[dict[str, Any]] = []
        self._counter = 0

    def get_response(
        self,
        messages: str | Message | list[str] | list[Message],
        *,
        stream: bool = False,
        options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        options = options or {}
        self.calls.append(deepcopy({"messages": messages, "stream": stream, "options": options, "kwargs": kwargs}))
        if stream and not self.supports_streaming:
            raise TypeError("stream is not supported")

        self._counter += 1
        text = f"reply-{self._counter}"
        response_id = f"result-{self._counter}"
        conversation_id = f"service-branch-{self._counter}" if options.get("store", self.STORES_BY_DEFAULT) else None

        if stream:

            async def _updates() -> AsyncIterable[ChatResponseUpdate]:
                yield ChatResponseUpdate(
                    contents=[Content.from_text(text)],
                    role="assistant",
                    response_id=response_id,
                    conversation_id=conversation_id,
                )

            return ResponseStream(_updates(), finalizer=ChatResponse.from_updates)

        async def _get() -> ChatResponse:
            return ChatResponse(
                messages=Message(role="assistant", contents=[text]),
                response_id=response_id,
                conversation_id=conversation_id,
            )

        return _get()


class _ExternalHistoryProvider(HistoryProvider):
    """Stand-in for Cosmos/Redis/file-backed history the user chose deliberately."""

    def __init__(self) -> None:
        super().__init__(source_id="external")

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        return []

    async def save_messages(self, session_id: str | None, messages: Any, **kwargs: Any) -> None:
        return None


class _InMemoryStateProvider(AgentEntityStateProviderMixin):
    """JSON storage boundary without a durable backend."""

    def __init__(self, *, session_id: str = "autoswap-session", raw: dict[str, Any] | None = None) -> None:
        self._session_id = session_id
        self._state_dict: dict[str, Any] = json.loads(json.dumps(raw or {}))
        self.writes = 0

    def _get_state_dict(self) -> dict[str, Any]:
        return deepcopy(self._state_dict)

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        self._state_dict = json.loads(json.dumps(state))
        self.writes += 1

    def _get_session_id_from_entity(self) -> str:
        return self._session_id


class _PreviousResponseNotFound(Exception):
    """Shaped like the provider's refusal of a conversation id it previously issued.

    Mirrors the real payload field for field, because the entity matches on the structured
    ``code`` rather than on the message text.
    """

    def __init__(self) -> None:
        super().__init__(
            "Error code: 400 - {'error': {'message': \"Previous response with id 'resp_x' not "
            "found.\", 'type': 'invalid_request_error', 'param': 'previous_response_id', "
            "'code': 'previous_response_not_found'}}"
        )
        self.status_code = 400
        self.code = "previous_response_not_found"
        self.param = "previous_response_id"
        self.body = {
            "message": "Previous response with id 'resp_x' not found.",
            "type": "invalid_request_error",
            "param": "previous_response_id",
            "code": "previous_response_not_found",
        }


class _ContextLengthExceeded(Exception):
    """A different 400, which must not be mistaken for a lost conversation."""

    def __init__(self) -> None:
        super().__init__("Error code: 400 - context_length_exceeded")
        self.status_code = 400
        self.code = "context_length_exceeded"


def _agent(client: Any = None, **kwargs: Any) -> Agent:
    """Build an agent with a stub client.

    The stubs cover the parts of the client protocol these tests exercise but not its full generic
    signature, so the type is relaxed here rather than at every call site.
    """
    chat_client: Any = client if client is not None else _StubClient()
    return Agent(client=chat_client, name="a", **kwargs)


def _history_providers(agent: Any) -> list[Any]:
    return [p for p in agent.context_providers if isinstance(p, HistoryProvider)]


class TestAutomaticDurableHistory:
    """The durable runtime substitutes durable-backed history where appropriate."""

    def test_agent_without_providers_gets_durable_history(self) -> None:
        agent = _agent()

        prepared = ensure_durable_history(agent)

        providers = _history_providers(prepared)
        assert len(providers) == 1
        assert isinstance(providers[0], DurableHistoryProvider)
        # Uses the source id core's auto-injected provider would have, so a
        # default-configured CompactionProvider still resolves it.
        assert providers[0].source_id == InMemoryHistoryProvider.DEFAULT_SOURCE_ID

    def test_in_memory_history_is_replaced_preserving_source_id(self) -> None:
        agent = _agent(context_providers=[InMemoryHistoryProvider(source_id="custom_slot", skip_excluded=True)])

        prepared = ensure_durable_history(agent)

        providers = _history_providers(prepared)
        assert len(providers) == 1
        replacement = providers[0]
        assert isinstance(replacement, DurableHistoryProvider)
        # Preserving these is what keeps an existing CompactionProvider wired up.
        assert replacement.source_id == "custom_slot"
        assert replacement.skip_excluded is True

    def test_external_history_provider_is_left_alone(self) -> None:
        """The user deliberately chose their own storage; durable must not override it."""
        external = _ExternalHistoryProvider()
        agent = _agent(context_providers=[external])

        prepared = ensure_durable_history(agent)

        assert prepared is agent
        assert _history_providers(prepared) == [external]

    def test_service_managed_history_still_gets_a_provider(self) -> None:
        """The service owning the conversation is a per-run fact, not a per-registration one.

        Leaving a service-backed agent with no provider used to look right, because the service
        holds the transcript. But ``store`` is an ordinary run option, so a single run can put the
        conversation back in the client's hands, and core then injects a history provider of its
        own. Its state is persisted along with the entity and retention cannot see it, so it grows
        without bound. Claiming the slot up front is what keeps those turns reachable.
        """
        agent = _agent(_ServiceStoringClient())

        prepared = ensure_durable_history(agent)

        providers = _history_providers(prepared)
        assert len(providers) == 1
        assert isinstance(providers[0], DurableHistoryProvider)

    def test_store_false_overrides_a_service_storing_client(self) -> None:
        """``store=False`` puts history back in the client's hands, so durable must back it.

        Mirrors core's precedence: an explicit ``store`` wins over ``STORES_BY_DEFAULT``. Without
        this, an agent using the Responses API with ``store=False`` would keep a plain in-memory
        provider that the durable runtime never persists, silently losing the conversation.
        """
        agent = _agent(_ServiceStoringClient(), default_options={"store": False})

        prepared = ensure_durable_history(agent)

        providers = _history_providers(prepared)
        assert len(providers) == 1
        assert isinstance(providers[0], DurableHistoryProvider)

    def test_store_true_still_gets_a_provider(self) -> None:
        """Attached, but it yields nothing while the service owns the run.

        Attaching is about occupying the slot, not about taking over storage. What stops the model
        being handed the transcript twice is the provider returning no history on a service-owned
        run, which :class:`TestServiceManagedSessions` covers.
        """
        agent = _agent(default_options={"store": True})

        prepared = ensure_durable_history(agent)

        providers = _history_providers(prepared)
        assert len(providers) == 1
        assert isinstance(providers[0], DurableHistoryProvider)

    def test_existing_durable_provider_is_untouched(self) -> None:
        """Explicit configuration (for example to enable pruning) wins."""
        explicit = DurableHistoryProvider(prune_excluded=True)
        agent = _agent(context_providers=[explicit])

        prepared = ensure_durable_history(agent)

        assert prepared is agent
        assert _history_providers(prepared) == [explicit]

    def test_agent_without_context_pipeline_is_left_alone(self) -> None:
        """Custom agents that do not expose context_providers keep legacy replay."""

        class _CustomAgent:
            name = "custom"

            async def run(self, *args: Any, **kwargs: Any) -> Any: ...

        agent = _CustomAgent()

        assert ensure_durable_history(agent) is agent  # type: ignore[arg-type]


class TestUserAgentIsNotMutated:
    """Substitution must not change the object the caller handed us."""

    def test_original_agent_keeps_its_providers(self) -> None:
        original_provider = InMemoryHistoryProvider()
        agent = _agent(context_providers=[original_provider])
        original_list = agent.context_providers

        prepared = ensure_durable_history(agent)

        assert prepared is not agent
        assert agent.context_providers is original_list
        assert agent.context_providers == [original_provider]

    def test_entity_construction_does_not_mutate_the_agent(self) -> None:
        agent = _agent(context_providers=[InMemoryHistoryProvider()])

        entity = AgentEntity(agent, state_provider=_InMemoryStateProvider())

        assert isinstance(_history_providers(entity.agent)[0], DurableHistoryProvider)
        assert isinstance(_history_providers(agent)[0], InMemoryHistoryProvider)


class TestFollowCompactionRetention:
    """Follow-compaction retention physically deletes exclusions."""

    def test_off_by_default(self) -> None:
        entity = AgentEntity(_agent(), state_provider=_InMemoryStateProvider())

        assert entity._retention == "keep_all"
        assert entity._max_state_bytes is None
        assert _history_providers(entity.agent)[0].prune_excluded is False

    def test_enabled_via_registration(self) -> None:
        agent = _agent(context_providers=[InMemoryHistoryProvider()])

        prepared = ensure_durable_history(agent, prune_excluded=True)

        assert _history_providers(prepared)[0].prune_excluded is True

    def test_entity_forwards_the_flag(self) -> None:
        agent = _agent()

        entity = AgentEntity(agent, state_provider=_InMemoryStateProvider(), retention="follow_compaction")

        assert _history_providers(entity.agent)[0].prune_excluded is True

    @pytest.mark.parametrize("max_state_bytes", [None, 100_000])
    def test_keep_all_does_not_prune_on_write(self, max_state_bytes: int | None) -> None:
        """A pressure budget does not enable eager pruning."""
        entity = AgentEntity(
            _agent(),
            state_provider=_InMemoryStateProvider(),
            retention="keep_all",
            max_state_bytes=max_state_bytes,
        )

        assert _history_providers(entity.agent)[0].prune_excluded is False

    def test_auto_is_not_a_retention_mode(self) -> None:
        invalid_mode: Any = "auto"
        with pytest.raises(ValueError, match="retention"):
            AgentEntity(_agent(), state_provider=_InMemoryStateProvider(), retention=invalid_mode)

    def test_explicit_provider_configuration_wins(self) -> None:
        """A hand-configured provider is never overridden by the registration flag."""
        explicit = DurableHistoryProvider(prune_excluded=False)
        agent = _agent(context_providers=[explicit])

        prepared = ensure_durable_history(agent, prune_excluded=True)

        assert _history_providers(prepared)[0] is explicit
        assert explicit.prune_excluded is False

    def test_an_unset_provider_inherits_the_retention_mode(self) -> None:
        """Constructing the provider by hand must not silently disable ``follow_compaction``.

        A caller who writes ``DurableHistoryProvider()`` has expressed no opinion about pruning,
        so the entity's retention mode is the only instruction available. Treating the unset
        default as a deliberate "no" made ``retention='follow_compaction'`` do nothing at all for
        anyone who wired the provider themselves.
        """
        unset = DurableHistoryProvider()
        assert unset.prune_excluded is None
        agent = _agent(context_providers=[unset])

        prepared = ensure_durable_history(agent, prune_excluded=True)

        providers = _history_providers(prepared)
        assert providers[0] is not unset
        assert isinstance(providers[0], DurableHistoryProvider)
        assert providers[0].prune_excluded is True
        # The caller's own object is never mutated.
        assert unset.prune_excluded is None

    def test_an_unset_provider_stays_unpruned_under_keep_all(self) -> None:
        unset = DurableHistoryProvider()
        agent = _agent(context_providers=[unset])

        entity = AgentEntity(agent, state_provider=_InMemoryStateProvider(), retention="keep_all")

        providers = _history_providers(entity.agent)
        assert providers[0].prune_excluded is False


class _StoringExternalProvider(HistoryProvider):
    """External-store double with a blind append, not its own input deduplication."""

    def __init__(self) -> None:
        super().__init__(source_id="external-store")
        self.saved: list[Message] = []
        self.saved_batches: list[list[Message]] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        return deepcopy(self.saved)

    async def save_messages(self, session_id: str | None, messages: Any, **kwargs: Any) -> None:
        batch = deepcopy(list(messages))
        self.saved_batches.append(batch)
        self.saved.extend(batch)


class _ServiceAwareExternalProvider(_StoringExternalProvider):
    """Test provider whose hooks defer to a service ID on the active session."""

    async def before_run(
        self, *, agent: Any, session: AgentSession, context: SessionContext, state: dict[str, Any]
    ) -> None:
        if session.service_session_id is None:
            await super().before_run(agent=agent, session=session, context=context, state=state)

    async def after_run(
        self, *, agent: Any, session: AgentSession, context: SessionContext, state: dict[str, Any]
    ) -> None:
        if session.service_session_id is None:
            await super().after_run(agent=agent, session=session, context=context, state=state)


class _SessionObserver(ContextProvider):
    def __init__(self) -> None:
        super().__init__("session-observer")
        self.before: list[dict[str, Any]] = []
        self.after: list[dict[str, Any]] = []

    @staticmethod
    def _snapshot(session: AgentSession, context: SessionContext) -> dict[str, Any]:
        return {
            "service_session_id": session.service_session_id,
            "context_service_session_id": context.service_session_id,
            "texts": [message.text for message in context.get_messages(include_input=True)],
        }

    async def before_run(
        self, *, agent: Any, session: AgentSession, context: SessionContext, state: dict[str, Any]
    ) -> None:
        self.before.append(self._snapshot(session, context))

    async def after_run(
        self, *, agent: Any, session: AgentSession, context: SessionContext, state: dict[str, Any]
    ) -> None:
        self.after.append(self._snapshot(session, context))


class TestWeDoNotKeepASecondCopyOfSomeoneElsesConversation:
    """External history needs delivery and ingestion receipts, not a local message mirror."""

    def _content_items(self, entity: AgentEntity, kind: str) -> int:
        return sum(
            len(m.contents)
            for entry in entity.state.data.conversation_history
            for m in entry.messages
            if entry.json_type == kind
        )

    async def _run(self, providers: list[Any], turns: int = 4) -> AgentEntity:
        agent = _agent(_RecordingClient(), context_providers=providers)
        entity = AgentEntity(agent, state_provider=_InMemoryStateProvider())
        for index in range(turns):
            await entity.run({"message": f"a reasonably long question number {index}", "correlationId": f"c{index}"})
        return entity

    async def test_requests_are_not_kept_twice(self) -> None:
        external = _StoringExternalProvider()

        entity = await self._run([external])

        assert len(external.saved) == 8
        assert entity.state.data.conversation_history == []

    async def test_responses_are_kept_in_the_mailbox_for_delivery(self) -> None:
        external = _StoringExternalProvider()

        entity = await self._run([external])

        restored = DurableAgentState.from_json(entity.state.to_json())
        assert restored.data.conversation_history == []
        assert set(restored.data.response_mailbox) == {f"c{index}" for index in range(4)}
        for index in range(4):
            response = restored.try_get_agent_response(f"c{index}")
            assert response is not None
            assert response.text == f"reply-{index + 1}"
            assert response.to_dict() == restored.data.response_mailbox[f"c{index}"]["response"]

    async def test_completion_is_recorded_separately_from_the_transcript(self) -> None:
        external = _StoringExternalProvider()

        entity = await self._run([external])

        data = json.loads(entity.state.to_json())["data"]
        assert data["conversationHistory"] == []
        assert set(data["completedCorrelations"]) == {f"c{index}" for index in range(4)}
        assert all(receipt["completedAt"] for receipt in data["completedCorrelations"].values())

    @pytest.mark.parametrize("include_new_message", [False, True], ids=["repeated-only", "repeated-and-new"])
    async def test_custom_context_ids_are_deduplicated_after_json_cold_reload(self, include_new_message: bool) -> None:
        external = _StoringExternalProvider()
        client = _RecordingClient()
        provider = _InMemoryStateProvider()
        entity = AgentEntity(_agent(client, context_providers=[external]), state_provider=provider)
        original = Message(role="user", contents=["upstream original"], message_id="custom-source-id")
        fresh = Message(role="user", contents=["upstream new"], message_id="another-custom-id")

        first = await entity.run({
            "message": "upstream original",
            "correlationId": "first-delivery",
            "contextMessages": [original.to_dict()],
        })
        raw = json.loads(json.dumps(provider._get_state_dict()))
        assert raw["schemaVersion"] == "2.0.0"
        assert raw["data"]["conversationHistory"] == []
        original_receipt = raw["data"]["ingestedMessages"]["custom-source-id"]
        assert original_receipt
        assert external.saved_batches[0][0].message_id == "custom-source-id"

        restarted_provider = _InMemoryStateProvider(raw=raw)
        restarted = AgentEntity(_agent(client, context_providers=[external]), state_provider=restarted_provider)
        follow_up = [original, fresh] if include_new_message else [original]
        await restarted.run({
            "message": "logging-only input must not be replayed",
            "correlationId": "new-delivery",
            "contextMessages": [message.to_dict() for message in follow_up],
        })

        new_texts = [fresh.text] if include_new_message else []
        assert len(client.received) == 2, "a new correlation must run even when its projected input is already ingested"
        assert [message.text for message in client.received[1]] == [original.text, "reply-1", *new_texts]
        assert [message.text for message in external.saved_batches[1]] == [*new_texts, "reply-2"]
        assert sum(message.message_id == original.message_id for message in external.saved) == 1

        restored = DurableAgentState.from_json(json.dumps(restarted_provider._get_state_dict()))
        assert restored.data.conversation_history == []
        assert restored.data.ingested_messages["custom-source-id"] == original_receipt
        expected_ids = {"custom-source-id", "another-custom-id"} if include_new_message else {"custom-source-id"}
        assert set(restored.data.ingested_messages) == expected_ids
        assert set(restored.data.completed_correlations) == {"first-delivery", "new-delivery"}
        delivered = restored.try_get_agent_response("first-delivery")
        assert delivered is not None
        assert delivered.to_dict() == first.to_dict()

    async def test_our_own_history_is_kept_in_full(self) -> None:
        """Nothing else is holding it, so forgetting it would lose the conversation."""
        entity = await self._run([])

        assert self._content_items(entity, "request") > 0
        assert self._content_items(entity, "response") > 0


class TestServiceManagedSessions:
    """Service-backed agents let the service own the conversation."""

    @pytest.mark.parametrize("streaming", [False, True], ids=["nonstream-fallback", "streaming"])
    @pytest.mark.parametrize("external_history", [False, True], ids=["durable-primary", "external-primary"])
    @pytest.mark.parametrize(
        ("stores_by_default", "default_options", "service_options", "local_options"),
        [
            pytest.param(True, {}, {}, {"store": False}, id="client-default-true"),
            pytest.param(False, {"store": True}, {}, {"store": False}, id="agent-default-true"),
            pytest.param(False, {}, {"store": True}, {}, id="client-default-false"),
            pytest.param(True, {"store": False}, {"store": True}, {}, id="agent-default-false"),
        ],
    )
    async def test_core_pipeline_isolates_service_and_local_branches_after_json_reload(
        self,
        streaming: bool,
        external_history: bool,
        stores_by_default: bool,
        default_options: dict[str, Any],
        service_options: dict[str, Any],
        local_options: dict[str, Any],
    ) -> None:
        """True/False/False/True through core Agent, with recording doubles rather than live services."""
        client = _ConversationIdClient(stores_by_default=stores_by_default, supports_streaming=streaming)
        observer = _SessionObserver()
        external = _ServiceAwareExternalProvider() if external_history else None
        providers: list[ContextProvider] = [external, observer] if external is not None else [observer]
        prompts = ["service-first", "local-first", "local-second", "service-resumed"]
        expected_inputs = [
            ["service-first"],
            ["local-first"],
            ["local-first", "reply-2", "local-second"],
            ["service-resumed"],
        ]
        expected_local_history = [
            [],
            ["local-first", "reply-2"],
            ["local-first", "reply-2", "local-second", "reply-3"],
            ["local-first", "reply-2", "local-second", "reply-3"],
        ]
        raw: dict[str, Any] = {}
        originals: dict[str, dict[str, Any]] = {}
        attempts = [True] if streaming else [True, False]

        for index, prompt in enumerate(prompts):
            # Rebuild the agent, entity and state provider; only the recording doubles survive.
            provider = _InMemoryStateProvider(raw=raw)
            entity = AgentEntity(
                _agent(client, context_providers=providers, default_options=default_options),
                state_provider=provider,
            )
            history_providers = _history_providers(entity.agent)
            if external is not None:
                assert history_providers == [external]
            else:
                assert len(history_providers) == 1
                assert isinstance(history_providers[0], DurableHistoryProvider)

            store = index in (0, 3)
            options = dict(service_options if store else local_options)
            start = len(client.calls)
            response = await entity.run({"message": prompt, "correlationId": f"c{index}", "options": options})
            assert response.text == f"reply-{index + 1}"
            assert response.response_id == f"result-{index + 1}"
            originals[f"c{index}"] = json.loads(json.dumps(response.to_dict()))

            calls = client.calls[start:]
            assert [call["stream"] for call in calls] == attempts
            active_id = "service-branch-1" if index == 3 else None
            for call in calls:
                assert [message.text for message in call["messages"]] == expected_inputs[index]
                assert call["options"].get("store", stores_by_default) is store
                assert call["options"].get("conversation_id") == active_id
                assert call["kwargs"].get("conversation_id") is None
                assert call["kwargs"]["client_kwargs"].get("conversation_id") is None
                forwarded_session = call["kwargs"]["client_kwargs"]["session"]
                assert forwarded_session.service_session_id == active_id
                assert forwarded_session.session_id == "autoswap-session"

            raw = json.loads(json.dumps(provider._get_state_dict()))
            data = raw["data"]
            assert provider.writes == 1
            assert data["session"]["service_session_id"] == ("service-branch-4" if index == 3 else "service-branch-1")
            assert InMemoryHistoryProvider.DEFAULT_SOURCE_ID not in data["session"]["state"]
            local_texts = [
                message.text for entry in entity.state.data.conversation_history for message in entry.messages
            ]
            assert local_texts == ([] if external is not None else expected_local_history[index])
            assert set(data["responseMailbox"]) == set(originals)
            assert set(data["completedCorrelations"]) == set(originals)
            assert {key: entry["response"] for key, entry in data["responseMailbox"].items()} == originals

        expected_active_ids = [None, None, None, "service-branch-1"]
        assert [entry["service_session_id"] for entry in observer.before] == [
            value for value in expected_active_ids for _ in attempts
        ]
        assert [entry["context_service_session_id"] for entry in observer.before] == [
            value for value in expected_active_ids for _ in attempts
        ]
        # Implicit durable history is appended like Core's automatic provider, after the observer.
        # Only this before-hook sees raw input; keep the full model-input checks above unchanged.
        # An explicit external primary still runs before the observer and supplies its history.
        expected_before_inputs = expected_inputs if external is not None else [[prompt] for prompt in prompts]
        assert [entry["texts"] for entry in observer.before] == [
            batch for batch in expected_before_inputs for _ in attempts
        ]
        assert [entry["service_session_id"] for entry in observer.after] == [
            "service-branch-1",
            None,
            None,
            "service-branch-4",
        ]
        assert [entry["context_service_session_id"] for entry in observer.after] == expected_active_ids
        assert [entry["texts"] for entry in observer.after] == expected_inputs
        if external is not None:
            assert [message.text for message in external.saved] == expected_local_history[-1]
            assert [[message.text for message in batch] for batch in external.saved_batches] == [
                ["local-first", "reply-2"],
                ["local-second", "reply-3"],
            ]

        reloaded = DurableAgentState.from_json(json.dumps(raw))
        for correlation_id, original in originals.items():
            delivered = reloaded.try_get_agent_response(correlation_id)
            assert delivered is not None
            assert delivered.to_dict() == original

    async def test_a_service_owned_run_is_not_sent_its_own_history(self) -> None:
        """A service-owned run receives only new input, even before a service ID has been issued."""
        client = _RecordingServiceClient()
        agent = _agent(client)
        entity = AgentEntity(agent, state_provider=_InMemoryStateProvider())

        for index in range(4):
            await entity.run({"message": f"m{index}", "correlationId": f"c{index}"})

        assert [len(batch) for batch in client.received] == [1, 1, 1, 1]

    async def test_a_client_side_run_does_get_its_history(self) -> None:
        """The same provider, on runs the service is not holding, supplies the conversation."""
        client = _RecordingServiceClient()
        agent = _agent(client)
        entity = AgentEntity(agent, state_provider=_InMemoryStateProvider())

        for index in range(4):
            await entity.run({"message": f"m{index}", "correlationId": f"c{index}", "options": {"store": False}})

        assert [len(batch) for batch in client.received] == [1, 3, 5, 7]

    async def test_a_client_side_run_does_not_grow_opaque_session_state(self) -> None:
        """Client-owned turns stay in the local transcript, not a second history slice in the session bag."""
        provider = _InMemoryStateProvider()
        entity = AgentEntity(_agent(_RecordingServiceClient()), state_provider=provider)

        sizes: list[int] = []
        for index in range(6):
            await entity.run({"message": f"m{index}", "correlationId": f"c{index}", "options": {"store": False}})
            session_slice = provider._get_state_dict().get("data", {}).get("session", {})
            sizes.append(len(json.dumps(session_slice)))

        assert sizes[0] == sizes[-1], f"session state grew: {sizes}"
        assert len(entity.state.data.conversation_history) == 12

    async def test_only_new_messages_are_sent(self) -> None:
        """History must not be replayed locally when the service already holds it."""
        recorded: list[list[Message]] = []

        class _ServiceAgent:
            name = "svc"
            client = _ServiceStoringClient()
            context_providers: list[Any] = []

            def create_session(self, **kwargs: Any) -> Any:
                from agent_framework import AgentSession

                return AgentSession()

            async def run(self, messages: Any = None, *, stream: bool = False, **kwargs: Any) -> Any:
                from agent_framework import AgentResponse

                if stream:
                    raise TypeError("stream is not supported")
                recorded.append(list(messages or []))
                return AgentResponse(messages=[Message(role="assistant", contents=["ok"])])

        entity = AgentEntity(_ServiceAgent(), state_provider=_InMemoryStateProvider())  # type: ignore[arg-type]

        await entity.run({"message": "first", "correlationId": "c0"})
        await entity.run({"message": "second", "correlationId": "c1"})

        # Each turn delivers only its own message; the service supplies the rest.
        assert len(recorded[1]) == 1
        assert recorded[1][0].text == "second"

    async def test_service_conversation_id_is_persisted_and_restored(self) -> None:
        """Without this the service would start a new thread on every turn."""
        seen_ids: list[str | None] = []

        class _ThreadingAgent:
            name = "svc"
            client = _ServiceStoringClient()
            context_providers: list[Any] = []

            def create_session(self, **kwargs: Any) -> Any:
                from agent_framework import AgentSession

                return AgentSession()

            async def run(
                self,
                messages: Any = None,
                *,
                stream: bool = False,
                session: Any = None,
                **kwargs: Any,
            ) -> Any:
                from agent_framework import AgentResponse

                if stream:
                    raise TypeError("stream is not supported")
                seen_ids.append(getattr(session, "service_session_id", None))
                # The service issues (or confirms) the thread id on the session.
                session.service_session_id = "svc-thread-1"
                return AgentResponse(messages=[Message(role="assistant", contents=["ok"])])

        provider = _InMemoryStateProvider()
        entity = AgentEntity(_ThreadingAgent(), state_provider=provider)  # type: ignore[arg-type]

        await entity.run({"message": "first", "correlationId": "c0"})
        await entity.run({"message": "second", "correlationId": "c1"})

        assert seen_ids[0] is None  # first turn has no thread yet
        assert seen_ids[1] == "svc-thread-1"  # second turn continues the same thread
        assert provider._get_state_dict()["data"]["session"]["service_session_id"] == "svc-thread-1"


class TestRejectedConversationIdRecovery:
    """Injected service refusals exercise bounded identical-request retries, not transcript recovery."""

    @pytest.fixture(autouse=True)
    def _no_backoff(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Remove the retry waits, which are about a real service catching up, not about tests."""
        monkeypatch.setattr(_entities, "_REJECTED_ID_BACKOFF_SECONDS", 0.0)

    async def test_a_late_id_is_recovered_without_resending_the_transcript(self) -> None:
        """The common case: the id resolves a moment later, so nothing needs resending."""
        calls: list[dict[str, Any]] = []

        class _SlowToCommitAgent:
            name = "svc"
            client = _ServiceStoringClient()
            context_providers: list[Any] = []

            def create_session(self, **kwargs: Any) -> Any:
                from agent_framework import AgentSession

                return AgentSession()

            async def run(
                self,
                messages: Any = None,
                *,
                stream: bool = False,
                session: Any = None,
                **kwargs: Any,
            ) -> Any:
                from agent_framework import AgentResponse

                if stream:
                    raise TypeError("stream is not supported")
                previous = getattr(session, "service_session_id", None)
                calls.append({"previous": previous, "texts": [m.text for m in (messages or [])]})
                if previous is None:
                    session.service_session_id = "thread-1"
                    return AgentResponse(messages=[Message(role="assistant", contents=["ok"])])
                # Refused once, then the service catches up.
                if len([c for c in calls if c["previous"] is not None]) == 1:
                    raise _PreviousResponseNotFound
                return AgentResponse(messages=[Message(role="assistant", contents=["ok"])])

        provider = _InMemoryStateProvider()
        entity = AgentEntity(_SlowToCommitAgent(), state_provider=provider)  # type: ignore[arg-type]

        await entity.run({"message": "first", "correlationId": "c0"})
        response = await entity.run({"message": "second", "correlationId": "c1"})

        assert response.text == "ok"
        # First turn, the refusal, then one retry that succeeded. No transcript replay.
        assert len(calls) == 3
        assert calls[2]["previous"] == "thread-1"
        assert calls[2]["texts"] == ["second"]
        # The conversation continued on the same thread rather than starting a new one.
        assert provider._get_state_dict()["data"]["session"]["service_session_id"] == "thread-1"

    async def test_an_id_that_never_resolves_fails_the_turn(self) -> None:
        calls: list[dict[str, Any]] = []

        class _ForgetfulAgent:
            name = "svc"
            client = _ServiceStoringClient()
            context_providers: list[Any] = []

            def create_session(self, **kwargs: Any) -> Any:
                from agent_framework import AgentSession

                return AgentSession()

            async def run(
                self,
                messages: Any = None,
                *,
                stream: bool = False,
                session: Any = None,
                **kwargs: Any,
            ) -> Any:
                from agent_framework import AgentResponse

                if stream:
                    raise TypeError("stream is not supported")
                previous = getattr(session, "service_session_id", None)
                calls.append({"previous": previous, "texts": [m.text for m in (messages or [])]})
                # Any turn that arrives carrying a conversation id is refused.
                if previous is not None:
                    raise _PreviousResponseNotFound
                session.service_session_id = f"thread-{len(calls)}"
                return AgentResponse(messages=[Message(role="assistant", contents=["ok"])])

        provider = _InMemoryStateProvider()
        entity = AgentEntity(_ForgetfulAgent(), state_provider=provider)  # type: ignore[arg-type]

        await entity.run({"message": "first", "correlationId": "c0"})
        response = await entity.run({"message": "second", "correlationId": "c1"})

        # Five calls: the first turn, the refused attempt, and three retries of the identical
        # request. Nothing else is tried, because the only remaining recovery would be resending
        # our own transcript, and that is only possible if the entity keeps a full second copy of
        # a conversation the service is already holding.
        assert len(calls) == 5
        # Every attempt after the first was the same request, unchanged, still chained on the id.
        assert all(call["texts"] == ["second"] and call["previous"] == "thread-1" for call in calls[1:])
        # The turn is reported as failed rather than silently answered without its context.
        assert any(content.type == "error" for content in response.messages[0].contents)
        # The stored id is left alone, so a service that recovers later still works.
        assert provider._get_state_dict()["data"]["session"]["service_session_id"] == "thread-1"

    async def test_streaming_rejection_does_not_add_a_nonstreamed_attempt(self) -> None:
        """Falling back to a non-streamed call with the refused id only wastes a round trip."""
        attempts: list[tuple[str, str | None]] = []

        class _StreamingForgetfulAgent:
            name = "svc"
            client = _ServiceStoringClient()
            context_providers: list[Any] = []

            def create_session(self, **kwargs: Any) -> Any:
                from agent_framework import AgentSession

                return AgentSession()

            async def run(
                self,
                messages: Any = None,
                *,
                stream: bool = False,
                session: Any = None,
                **kwargs: Any,
            ) -> Any:
                from agent_framework import AgentResponse

                previous = getattr(session, "service_session_id", None)
                attempts.append(("stream" if stream else "nonstream", previous))
                if previous is not None:
                    raise _PreviousResponseNotFound
                if stream:
                    raise TypeError("stream is not supported")
                session.service_session_id = "thread-1"
                return AgentResponse(messages=[Message(role="assistant", contents=["ok"])])

        entity = AgentEntity(
            _StreamingForgetfulAgent(),  # type: ignore[arg-type]
            state_provider=_InMemoryStateProvider(),
        )

        await entity.run({"message": "first", "correlationId": "c0"})
        await entity.run({"message": "second", "correlationId": "c1"})

        # Retry the streamed invocation at the entity boundary, without clearing the ID
        # or adding a non-streamed attempt carrying the same refused ID.
        assert ("stream", "thread-1") in attempts
        assert ("nonstream", "thread-1") not in attempts

    async def test_unrelated_bad_request_is_not_replayed(self) -> None:
        """Replaying on any 400 would answer without the context the caller asked for."""
        calls: list[str | None] = []

        class _FailingAgent:
            name = "svc"
            client = _ServiceStoringClient()
            context_providers: list[Any] = []

            def create_session(self, **kwargs: Any) -> Any:
                from agent_framework import AgentSession

                return AgentSession()

            async def run(
                self,
                messages: Any = None,
                *,
                stream: bool = False,
                session: Any = None,
                **kwargs: Any,
            ) -> Any:
                if stream:
                    raise TypeError("stream is not supported")
                calls.append(getattr(session, "service_session_id", None))
                raise _ContextLengthExceeded

        entity = AgentEntity(_FailingAgent(), state_provider=_InMemoryStateProvider())  # type: ignore[arg-type]

        response = await entity.run({"message": "first", "correlationId": "c0"})

        assert len(calls) == 1  # attempted once, not retried
        assert any(content.type == "error" for content in response.messages[0].contents)

    async def test_retries_are_bounded(self) -> None:
        """A retry loop against a service that keeps refusing would never terminate."""
        calls: list[str | None] = []

        class _AlwaysRejectingAgent:
            name = "svc"
            client = _ServiceStoringClient()
            context_providers: list[Any] = []

            def create_session(self, **kwargs: Any) -> Any:
                from agent_framework import AgentSession

                return AgentSession()

            async def run(
                self,
                messages: Any = None,
                *,
                stream: bool = False,
                session: Any = None,
                **kwargs: Any,
            ) -> Any:
                if stream:
                    raise TypeError("stream is not supported")
                calls.append(getattr(session, "service_session_id", None))
                raise _PreviousResponseNotFound

        entity = AgentEntity(_AlwaysRejectingAgent(), state_provider=_InMemoryStateProvider())  # type: ignore[arg-type]

        response = await entity.run({"message": "first", "correlationId": "c0"})

        # The original attempt plus a fixed number of retries, then the failure is reported rather
        # than retried forever.
        assert len(calls) == 4
        assert any(content.type == "error" for content in response.messages[0].contents)
