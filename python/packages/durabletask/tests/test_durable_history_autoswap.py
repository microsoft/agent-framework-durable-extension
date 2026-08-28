# Copyright (c) Microsoft. All rights reserved.

"""Tests for automatic durable history backing (ADR-0032).

A user should be able to take an agent that already works in core, register it with the
durable runtime, and get durable conversation history with no configuration change.
These tests cover the substitution rules and confirm the user's agent is never mutated.
"""

import json
from collections.abc import AsyncIterable, Awaitable, Sequence
from typing import Any

from agent_framework import (
    Agent,
    ChatResponse,
    ChatResponseUpdate,
    Content,
    HistoryProvider,
    InMemoryHistoryProvider,
    Message,
    ResponseStream,
)

from agent_framework_durabletask import AgentEntity, AgentEntityStateProviderMixin, DurableHistoryProvider
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


class _ExternalHistoryProvider(HistoryProvider):
    """Stand-in for Cosmos/Redis/file-backed history the user chose deliberately."""

    def __init__(self) -> None:
        super().__init__(source_id="external")

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        return []

    async def save_messages(self, session_id: str | None, messages: Any, **kwargs: Any) -> None:
        return None


class _InMemoryStateProvider(AgentEntityStateProviderMixin):
    def __init__(self, *, session_id: str = "autoswap-session") -> None:
        self._session_id = session_id
        self._state_dict: dict[str, Any] = {}

    def _get_state_dict(self) -> dict[str, Any]:
        return self._state_dict

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        self._state_dict = state

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
        agent = _agent()

        prepared = ensure_durable_history(agent)

        assert _history_providers(prepared)[0].prune_excluded is False

    def test_enabled_via_registration(self) -> None:
        agent = _agent(context_providers=[InMemoryHistoryProvider()])

        prepared = ensure_durable_history(agent, prune_excluded=True)

        assert _history_providers(prepared)[0].prune_excluded is True

    def test_entity_forwards_the_flag(self) -> None:
        agent = _agent()

        entity = AgentEntity(agent, state_provider=_InMemoryStateProvider(), retention="follow_compaction")

        assert _history_providers(entity.agent)[0].prune_excluded is True

    def test_other_retention_modes_do_not_prune_on_write(self) -> None:
        """Only ``follow_compaction`` treats a compaction exclusion as consent to delete."""
        for mode in ("auto", "keep_all"):
            entity = AgentEntity(_agent(), state_provider=_InMemoryStateProvider(), retention=mode)

            assert _history_providers(entity.agent)[0].prune_excluded is False, mode

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

    def test_an_unset_provider_stays_unpruned_under_auto(self) -> None:
        unset = DurableHistoryProvider()
        agent = _agent(context_providers=[unset])

        prepared = ensure_durable_history(agent, prune_excluded=False)

        providers = _history_providers(prepared)
        assert providers[0].prune_excluded is False


class _StoringExternalProvider(HistoryProvider):
    """External store that actually keeps what it is given, so both copies can be compared."""

    def __init__(self) -> None:
        super().__init__(source_id="external-store")
        self.saved: list[Message] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        return list(self.saved)

    async def save_messages(self, session_id: str | None, messages: Any, **kwargs: Any) -> None:
        self.saved.extend(messages)


class TestWeDoNotKeepASecondCopyOfSomeoneElsesConversation:
    """When the caller brought their own store, the entity records the exchange, not the content.

    The entity has to record every exchange in every configuration, because correlation ids and
    delivery are its job and nothing else can do them. It does not have to be a second copy of the
    conversation. Being one puts the customer's content under two different retention, residency
    and deletion policies when they deliberately chose one store for it.

    Responses are the exception, and not an arbitrary one. A caller collects its answer by polling
    the entity for a correlation id, so the entity is the only thing that can produce it.
    """

    def _content_items(self, entity: AgentEntity, kind: str) -> int:
        return sum(
            len(m.contents)
            for entry in entity.state.data.conversation_history
            for m in entry.messages
            if entry.json_type.value == kind
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

        assert len(external.saved) > 0
        assert self._content_items(entity, "request") == 0

    async def test_responses_are_kept_so_callers_can_collect_them(self) -> None:
        external = _StoringExternalProvider()

        entity = await self._run([external])

        assert self._content_items(entity, "response") > 0
        assert entity.state.try_get_agent_response("c0") is not None

    async def test_the_exchange_is_still_recorded(self) -> None:
        """Envelopes survive, because delivery and correlation depend on them."""
        external = _StoringExternalProvider()

        entity = await self._run([external])

        history = entity.state.data.conversation_history
        assert len(history) == 8
        assert [e.correlation_id for e in history] == [f"c{i // 2}" for i in range(8)]
        assert all(e.created_at is not None for e in history)

    async def test_request_message_ids_survive_for_deduplication(self) -> None:
        """Workflow fan-out is deduplicated by id, so forgetting ids would double-ingest."""
        external = _StoringExternalProvider()

        entity = await self._run([external])

        request_messages = [
            m
            for entry in entity.state.data.conversation_history
            if entry.json_type.value == "request"
            for m in entry.messages
        ]
        assert request_messages
        assert all(m.role for m in request_messages)

    async def test_our_own_history_is_kept_in_full(self) -> None:
        """Nothing else is holding it, so forgetting it would lose the conversation."""
        entity = await self._run([])

        assert self._content_items(entity, "request") > 0
        assert self._content_items(entity, "response") > 0


class TestServiceManagedSessions:
    """Service-backed agents let the service own the conversation."""

    async def test_a_service_owned_run_is_not_sent_its_own_history(self) -> None:
        """The provider is attached, so it must stay quiet while the service holds the thread.

        Attaching a provider to a service-backed agent is what stops core injecting one whose
        state nothing bounds. But core continues a stored conversation by id rather than by
        resending it, so a provider that also loaded history would hand the model the whole
        transcript on top of the copy the service already has. Measured before this was fixed, the
        prompt went from one message a turn to the entire conversation every turn.
        """
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
        """The point of attaching: those turns land where retention can reach them.

        Without a provider of ours, core injects its own and the transcript is persisted inside
        the session bag, which retention never evicts from. It grew about 321 bytes a turn and
        nothing would ever have reclaimed it.
        """
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
    """A service can hand back a conversation id it will not accept on the next turn.

    The id is captured correctly and the conversation still exists, it is just briefly
    unreachable. Losing the turn over that would be unreasonable, so the entity drops the id and
    resends the transcript, which is what it already does for agents whose history it owns.
    """

    async def test_rejected_id_replays_the_full_transcript(self) -> None:
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

        # Three calls: the first turn, the refused attempt, and the replay.
        assert len(calls) == 3
        # The refused attempt chained on the stored id and sent only the new message.
        assert calls[1]["previous"] == "thread-1"
        assert calls[1]["texts"] == ["second"]
        # The replay dropped the id and carried the whole conversation instead.
        assert calls[2]["previous"] is None
        assert calls[2]["texts"] == ["first", "ok", "second"]
        # The turn succeeded rather than surfacing an empty reply.
        assert response.text == "ok"
        # The fresh id is persisted, so the session recovers instead of failing every turn.
        assert provider._get_state_dict()["data"]["session"]["service_session_id"] == "thread-3"

    async def test_streaming_rejection_does_not_retry_with_the_same_id(self) -> None:
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

        entity = AgentEntity(_StreamingForgetfulAgent(), state_provider=_InMemoryStateProvider())  # type: ignore[arg-type]

        await entity.run({"message": "first", "correlationId": "c0"})
        await entity.run({"message": "second", "correlationId": "c1"})

        # The streamed attempt carrying the stale id is refused, and no non-streamed call
        # repeats it. The recovery happens a level up, with the id cleared.
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

    async def test_replay_is_attempted_only_once(self) -> None:
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

        # The original attempt plus exactly one replay, then the failure is reported.
        assert len(calls) == 2
        assert any(content.type == "error" for content in response.messages[0].contents)
