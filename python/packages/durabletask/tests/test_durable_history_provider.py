# Copyright (c) Microsoft. All rights reserved.

"""Tests for :class:`DurableHistoryProvider` (ADR-0032 Option 6).

The provider makes durable entity state the store behind core's ``HistoryProvider``
interface, so conversation history is persisted exactly once and core compaction
plugs in unchanged.
"""

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
    CompactionProvider,
    Content,
    ContextProvider,
    HistoryProvider,
    InMemoryHistoryProvider,
    Message,
    ResponseStream,
)

from agent_framework_durabletask import (
    AgentEntity,
    AgentEntityStateProviderMixin,
    DurableHistoryProvider,
)
from agent_framework_durabletask._history_provider import replayable_entries

KEEP_LAST_MESSAGES = 2


class RecordingChatClient:
    """Minimal chat client that records the message list it receives per call."""

    def __init__(self) -> None:
        self.additional_properties: dict[str, Any] = {}
        self.received_messages: list[list[Message]] = []
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
        self.received_messages.append(normalized)

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


class _InMemoryStateProvider(AgentEntityStateProviderMixin):
    """Test-only state provider that keeps the serialized entity state in memory."""

    def __init__(self, *, session_id: str = "durable-history-session") -> None:
        self._session_id = session_id
        self._state_dict: dict[str, Any] = {}
        self.writes = 0

    def _get_state_dict(self) -> dict[str, Any]:
        return self._state_dict

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        # The durable SDK serializes entity state as it is set, so a value it cannot encode
        # surfaces here rather than later. Mirrored so tests see the same failure the host does.
        json.dumps(state)
        self.writes += 1
        self._state_dict = state

    def _get_session_id_from_entity(self) -> str:
        return self._session_id


async def _keep_last_messages(messages: list[Message]) -> bool:
    """Compaction strategy: mark everything except the most recent messages as excluded."""
    if len(messages) <= KEEP_LAST_MESSAGES:
        return False
    changed = False
    for message in messages[:-KEEP_LAST_MESSAGES]:
        if not message.additional_properties.get("_excluded"):
            message.additional_properties["_excluded"] = True
            changed = True
    return changed


async def _summarize_oldest(messages: list[Message]) -> bool:
    """Strategy that *inserts* a summary message, mimicking ToolResultCompactionStrategy.

    Uses a stable summary id derived from the messages it replaces, so re-running it must
    not create duplicates.
    """
    if len(messages) <= KEEP_LAST_MESSAGES:
        return False

    older = [m for m in messages[:-KEEP_LAST_MESSAGES] if not m.additional_properties.get("_excluded")]
    if not older:
        return False

    summary_id = "summary_" + "_".join(sorted(m.message_id or "" for m in older))
    if any(m.message_id == summary_id for m in messages):
        return False

    for message in older:
        message.additional_properties["_excluded"] = True
        message.additional_properties["_summarized_by_summary_id"] = summary_id

    summary = Message(
        role="assistant",
        contents=[f"[summary of {len(older)} messages]"],
        message_id=summary_id,
        additional_properties={"_summary_of_message_ids": [m.message_id for m in older]},
    )
    messages.insert(messages.index(older[-1]) + 1, summary)
    return True


def _agent(providers: list[Any], client: RecordingChatClient | None = None) -> Agent:
    """Build an agent with the given context providers.

    The stub client covers the parts of the client protocol these tests exercise but not its full
    generic signature, so the type is relaxed here rather than at every call site.
    """
    chat_client: Any = client or RecordingChatClient()
    return Agent(client=chat_client, name="assistant", context_providers=providers)


def _providers_of(entity: AgentEntity) -> list[Any]:
    """Return the context providers on the entity's (possibly substituted) agent."""
    return list(getattr(entity.agent, "context_providers", []))


class _StubExternalProvider(HistoryProvider):
    """Stand-in for a provider the user configured deliberately (Cosmos, Redis, file)."""

    def __init__(self) -> None:
        super().__init__(source_id="external")

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        return []

    async def save_messages(self, session_id: str | None, messages: Any, **kwargs: Any) -> None:
        return None


def _build_agent(
    client: RecordingChatClient,
    *,
    with_compaction: bool = False,
    prune_excluded: bool = False,
    strategy: Any = None,
) -> Agent:
    history = DurableHistoryProvider(prune_excluded=prune_excluded)
    providers: list[Any] = [history]
    if with_compaction:
        providers.append(
            CompactionProvider(
                after_strategy=strategy or _keep_last_messages,
                history_source_id=history.source_id,
            )
        )
    return _agent(providers, client)


def _make_entity(agent: Agent, provider: _InMemoryStateProvider) -> AgentEntity:
    return AgentEntity(agent, state_provider=provider)


async def _run_turns(entity: AgentEntity, prompts: list[str]) -> None:
    for index, prompt in enumerate(prompts):
        await entity.run({"message": prompt, "correlationId": f"corr-{index}"})


def _stored_messages(entity: AgentEntity) -> list[Any]:
    return [m for entry in entity.state.data.conversation_history for m in entry.messages]


class TestDurableHistoryProvider:
    """Durable entity state is the single store behind core's HistoryProvider."""

    async def test_state_is_written_once_per_turn(self) -> None:
        """Each write serializes the whole conversation, so a spare one is not free.

        The provider used to persist at the end of ``flush``, which meant every turn serialized
        the entire transcript twice: once mid-turn, before the response even existed, and again
        when the entity finished. The mid-turn copy was always superseded, and with no compaction
        configured it wrote back state nothing had touched. Cost grows with the conversation, so
        this is pinned rather than left to drift back.
        """
        for label, agent in (
            ("no compaction", _build_agent(RecordingChatClient())),
            ("compaction", _build_agent(RecordingChatClient(), with_compaction=True)),
            (
                "compaction and pruning",
                _build_agent(RecordingChatClient(), with_compaction=True, prune_excluded=True),
            ),
        ):
            provider = _InMemoryStateProvider()
            entity = _make_entity(agent, provider)

            await _run_turns(entity, ["first", "second", "third"])

            assert provider.writes == 3, f"{label}: expected one write per turn, got {provider.writes}"

    async def test_compaction_annotations_survive_the_turn(self) -> None:
        """Removing the mid-turn write must not cost the annotations it used to persist."""
        provider = _InMemoryStateProvider()
        entity = _make_entity(_build_agent(RecordingChatClient(), with_compaction=True), provider)

        await _run_turns(entity, ["first", "second", "third", "fourth"])

        # Read from the serialized copy, not the in-memory objects, so this proves the
        # annotations actually reached durable state.
        persisted = provider._get_state_dict()["data"]["conversationHistory"]
        annotated = [
            message
            for entry in persisted
            for message in entry.get("messages", [])
            if (message.get("extensionData") or {}).get("_excluded")
        ]
        assert annotated, "compaction marked messages excluded but none of it was persisted"

    async def test_a_failed_turn_never_becomes_model_context(self) -> None:
        """A failure is for the caller, not for the model, and that has to survive a reload.

        This used to be a boolean on the response entry that was never serialized. Every cold
        start turned a failed turn back into an ordinary assistant reply, and the stored exception
        text was replayed to the model as something it had said.
        """
        from agent_framework_durabletask import DurableAgentState

        class _FailingClient(RecordingChatClient):
            def get_response(self, messages: Any, **kwargs: Any) -> Any:
                raise RuntimeError("kaboom")

        provider = _InMemoryStateProvider()
        entity = _make_entity(_build_agent(_FailingClient()), provider)  # type: ignore[arg-type]

        await entity.run({"message": "please fail", "correlationId": "corr-fail"})

        reloaded = DurableAgentState.from_dict(provider._get_state_dict())
        replayed = [
            entry.messages[index].to_chat_message().text
            for entry, index in replayable_entries(reloaded.data.conversation_history)
        ]

        assert not any("kaboom" in text for text in replayed), (
            f"the failure was replayed to the model after reload: {replayed}"
        )
        # It must still be readable by the caller that was waiting on it.
        assert reloaded.try_get_agent_response("corr-fail") is not None

    async def test_a_summary_is_never_returned_as_an_answer(self) -> None:
        """Compaction output belongs to the transcript, not to any caller's response.

        Summaries used to be inserted into whichever entry they followed. When that entry was a
        response, polling its correlation returned the agent's answer plus a summary it never
        produced.
        """
        entity = _make_entity(
            _build_agent(RecordingChatClient(), with_compaction=True, strategy=_summarize_oldest),
            _InMemoryStateProvider(),
        )

        await _run_turns(entity, ["t1", "t2", "t3", "t4", "t5", "t6"])

        summaries = [m for m in _stored_messages(entity) if "[summary of" in (m.to_chat_message().text or "")]
        assert summaries, "compaction produced no summary, so this proves nothing"

        delivered = [
            f"corr-{index}"
            for index in range(6)
            if (response := entity.state.try_get_agent_response(f"corr-{index}")) is not None
            and any("[summary of" in (m.text or "") for m in response.messages)
        ]
        assert not delivered, f"a summary was returned as the agent's answer for {delivered}"

    async def test_history_is_stored_once(self) -> None:
        """Messages live only in conversation history, never duplicated into the session blob."""
        client = RecordingChatClient()
        provider = _InMemoryStateProvider()
        entity = _make_entity(_build_agent(client), provider)

        await _run_turns(entity, ["first", "second"])

        persisted = provider._get_state_dict()["data"]
        assert "conversationHistory" in persisted
        # The session is persisted for provider state, but the history provider's slice - the
        # only place messages would appear - is excluded from it.
        session_state = persisted["session"]["state"]
        assert not any("messages" in slice_ for slice_ in session_state.values() if isinstance(slice_, dict))
        assert len(entity.state.data.conversation_history) == 4

    async def test_provider_supplies_history_across_turns(self) -> None:
        """Prior turns are loaded from durable state, not replayed by the entity."""
        client = RecordingChatClient()
        entity = _make_entity(_build_agent(client), _InMemoryStateProvider())

        await _run_turns(entity, ["first", "second", "third"])

        assert len(client.received_messages[0]) == 1
        assert len(client.received_messages[1]) > len(client.received_messages[0])
        assert len(client.received_messages[2]) > len(client.received_messages[1])
        assert client.received_messages[1][0].text == "first"

    async def test_no_duplicate_of_in_flight_request(self) -> None:
        """The in-flight request is delivered as input, not also loaded as history."""
        client = RecordingChatClient()
        entity = _make_entity(_build_agent(client), _InMemoryStateProvider())

        await _run_turns(entity, ["only-once"])

        texts = [m.text for m in client.received_messages[0]]
        assert texts.count("only-once") == 1

    async def test_compaction_annotations_persist_in_durable_state(self) -> None:
        """Core compaction plugs in and its annotations are stored with the messages."""
        client = RecordingChatClient()
        entity = _make_entity(_build_agent(client, with_compaction=True), _InMemoryStateProvider())

        await _run_turns(entity, ["t1", "t2", "t3", "t4", "t5"])

        excluded = [m for m in _stored_messages(entity) if (m.extension_data or {}).get("_excluded")]
        assert excluded, "expected compaction annotations persisted in conversation history"

        # Annotations survive a full serialize/deserialize round-trip of entity state.
        from agent_framework_durabletask import DurableAgentState

        restored = DurableAgentState.from_dict(entity.state.to_dict())
        restored_excluded = [
            m
            for entry in restored.data.conversation_history
            for m in entry.messages
            if (m.extension_data or {}).get("_excluded")
        ]
        assert len(restored_excluded) == len(excluded)

    async def test_compaction_bounds_model_input(self) -> None:
        """Excluded messages are withheld from the model, so context stops growing."""
        turns = ["t1", "t2", "t3", "t4", "t5", "t6"]

        plain_client = RecordingChatClient()
        await _run_turns(_make_entity(_build_agent(plain_client), _InMemoryStateProvider()), turns)

        compacted_client = RecordingChatClient()
        await _run_turns(
            _make_entity(_build_agent(compacted_client, with_compaction=True), _InMemoryStateProvider()),
            turns,
        )

        assert len(compacted_client.received_messages[-1]) < len(plain_client.received_messages[-1])

    async def test_prune_excluded_bounds_persisted_state(self) -> None:
        """Opt-in pruning physically shrinks durable storage (the lossy L2 step)."""
        turns = ["t1", "t2", "t3", "t4", "t5", "t6"]

        kept_entity = _make_entity(_build_agent(RecordingChatClient(), with_compaction=True), _InMemoryStateProvider())
        await _run_turns(kept_entity, turns)

        pruned_entity = _make_entity(
            _build_agent(RecordingChatClient(), with_compaction=True, prune_excluded=True),
            _InMemoryStateProvider(),
        )
        await _run_turns(pruned_entity, turns)

        assert len(_stored_messages(pruned_entity)) < len(_stored_messages(kept_entity))
        # Nothing marked excluded is left behind in storage.
        assert not [m for m in _stored_messages(pruned_entity) if (m.extension_data or {}).get("_excluded")]

    async def test_summarizing_strategy_persists_inserted_messages(self) -> None:
        """Strategies that insert a summary (not just annotate) are reconciled by message id."""
        client = RecordingChatClient()
        entity = _make_entity(
            _build_agent(client, with_compaction=True, strategy=_summarize_oldest),
            _InMemoryStateProvider(),
        )

        await _run_turns(entity, ["t1", "t2", "t3", "t4"])

        stored = _stored_messages(entity)
        summaries = [m for m in stored if m.message_id and m.message_id.startswith("summary_")]
        assert summaries, "expected the inserted summary message to be persisted"

        # Identity and annotations survive a durable state round-trip.
        from agent_framework_durabletask import DurableAgentState

        restored = DurableAgentState.from_dict(entity.state.to_dict())
        restored_ids = [
            m.message_id
            for entry in restored.data.conversation_history
            for m in entry.messages
            if m.message_id and m.message_id.startswith("summary_")
        ]
        assert restored_ids == [m.message_id for m in summaries]

    async def test_summary_is_not_duplicated_across_turns(self) -> None:
        """Re-running compaction with a stable summary id must not append duplicates."""
        client = RecordingChatClient()
        entity = _make_entity(
            _build_agent(client, with_compaction=True, strategy=_summarize_oldest),
            _InMemoryStateProvider(),
        )

        await _run_turns(entity, ["t1", "t2", "t3", "t4", "t5", "t6"])

        ids = [m.message_id for m in _stored_messages(entity) if m.message_id]
        assert len(ids) == len(set(ids)), f"duplicate message ids persisted: {ids}"

    async def test_insertion_keeps_later_positions_valid(self) -> None:
        """Inserting into an entry shifts its later messages, so recorded positions must follow.

        Entries normally hold a single message, which hides this. A workflow node receives the
        upstream conversation as several messages in one request entry, so a summary inserted in
        the middle of that entry invalidates the recorded index of everything after it, and the
        annotation lands on the wrong stored message.
        """

        async def _insert_then_exclude_up3(messages: list[Message]) -> bool:
            if any((m.additional_properties or {}).get("_marker") for m in messages):
                return False
            summary = Message(
                role="assistant",
                contents=["summary"],
                message_id="summary_mid",
                additional_properties={"_marker": True},
            )
            # Insert near the front, so messages later in the *same* durable entry shift.
            messages.insert(1, summary)
            for message in messages:
                if message.message_id == "up-3":
                    message.additional_properties = dict(message.additional_properties or {}) | {"_excluded": True}
            return True

        client = RecordingChatClient()
        entity = _make_entity(
            _build_agent(client, with_compaction=True, strategy=_insert_then_exclude_up3),
            _InMemoryStateProvider(),
        )

        # An upstream conversation delivered as one multi-message request entry.
        context = [
            Message(role="user", contents=["upstream one"], message_id="up-1").to_dict(),
            Message(role="assistant", contents=["upstream two"], message_id="up-2").to_dict(),
            Message(role="user", contents=["upstream three"], message_id="up-3").to_dict(),
        ]
        await entity.run({"message": "upstream three", "correlationId": "c0", "contextMessages": context})
        await entity.run({"message": "next", "correlationId": "c1"})

        stored = {m.message_id: m for m in _stored_messages(entity) if m.message_id}
        assert "up-3" in stored, f"expected the upstream messages to be persisted: {list(stored)}"

        # The annotation must land on up-3 itself, not on the neighbour that shifted when the
        # summary was inserted earlier in the same entry.
        assert (stored["up-3"].extension_data or {}).get("_excluded"), "annotation did not reach up-3"
        assert not (stored["up-2"].extension_data or {}).get("_excluded"), "annotation shifted onto up-2"

    async def test_generated_ids_survive_a_cold_start(self) -> None:
        """Ids synthesized for messages stored without one must derive from persisted state.

        A cold start or a retried flush rebuilds the entry objects at fresh addresses, so an id
        taken from object identity would differ every run, and a recycled address could even
        collide with an id an earlier run already persisted.
        """
        provider = _InMemoryStateProvider()
        entity = _make_entity(_build_agent(RecordingChatClient()), provider)
        await _run_turns(entity, ["first", "second"])

        # History as a producer that does not stamp message ids would have written it.
        raw = deepcopy(provider._get_state_dict())
        for entry in raw["data"]["conversationHistory"]:
            for message in entry["messages"]:
                message.pop("messageId", None)

        async def _synthesized_ids() -> list[str]:
            restarted_provider = _InMemoryStateProvider()
            restarted_provider._set_state_dict(deepcopy(raw))
            restarted = _make_entity(_build_agent(RecordingChatClient()), restarted_provider)
            await restarted.run({"message": "third", "correlationId": "corr-restart"})
            return [m.message_id for m in _stored_messages(restarted) if (m.message_id or "").startswith("durable_")]

        first = await _synthesized_ids()
        second = await _synthesized_ids()

        assert first, "expected ids to be synthesized for the messages that had none"
        assert len(first) == len(set(first)), f"synthesized ids collided within one run: {first}"
        assert first == second, f"synthesized ids changed across a cold start: {first} != {second}"

    async def test_service_managed_session_is_skipped(self) -> None:
        """When the model service owns the conversation, the provider must not participate."""
        from types import SimpleNamespace

        from agent_framework_durabletask._history_provider import (
            DurableHistoryBinding,
            bind_durable_history,
            unbind_durable_history,
        )

        client = RecordingChatClient()
        provider = _InMemoryStateProvider()
        entity = _make_entity(_build_agent(client), provider)
        await _run_turns(entity, ["first", "second"])

        history = DurableHistoryProvider()
        token = bind_durable_history(DurableHistoryBinding(state_provider=provider))
        try:
            state: dict[str, Any] = {}
            context = SimpleNamespace(session_id="s", extend_messages=lambda *_: None)
            service_session = SimpleNamespace(service_session_id="svc-123", state={})

            await history.before_run(agent=None, session=service_session, context=context, state=state)
            # Nothing was loaded, so no working buffer was published.
            assert "messages" not in state

            # Flushing is likewise a no-op and must not raise.
            await history.after_run(agent=None, session=service_session, context=context, state=state)
        finally:
            unbind_durable_history(token)

    async def test_core_configured_agent_gets_durable_history_automatically(self) -> None:
        """An agent configured the ordinary core way runs durably with no changes."""
        client = RecordingChatClient()
        agent = _agent([InMemoryHistoryProvider()], client)
        entity = _make_entity(agent, _InMemoryStateProvider())

        await _run_turns(entity, ["first", "second"])

        # The entity swapped in durable-backed history without the user asking.
        assert any(isinstance(p, DurableHistoryProvider) for p in _providers_of(entity))
        # The caller's agent is untouched.
        assert any(isinstance(p, InMemoryHistoryProvider) for p in agent.context_providers)
        # History is served from durable state, so turn 2 sees turn 1.
        assert len(client.received_messages[1]) > len(client.received_messages[0])
        assert len(entity.state.data.conversation_history) == 4


class TestExternalHistoryProviders:
    """Providers that own their own storage (Cosmos, Redis, file) keep working durably."""

    async def test_external_provider_receives_the_entity_session_id(self) -> None:
        """Their storage is keyed by session id, so it must be the entity's stable id.

        The entity builds a fresh session per operation. If that session carried a generated id,
        an external provider would read and write a different key every turn and never see prior
        history - broken continuity with no error to show for it.
        """
        seen: list[str | None] = []

        class _RecordingExternalProvider(HistoryProvider):
            def __init__(self) -> None:
                super().__init__(source_id="external")

            async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
                seen.append(session_id)
                return []

            async def save_messages(self, session_id: str | None, messages: Any, **kwargs: Any) -> None:
                seen.append(session_id)

        agent = _agent([_RecordingExternalProvider()])
        entity = _make_entity(agent, _InMemoryStateProvider(session_id="stable-session"))

        await _run_turns(entity, ["first", "second"])

        assert seen, "the external provider should have taken part in the run"
        assert set(seen) == {"stable-session"}

    async def test_external_provider_is_not_replaced(self) -> None:
        """The user chose their own storage; durable must not swap it out."""
        external = _StubExternalProvider()
        agent = _agent([external])

        entity = _make_entity(agent, _InMemoryStateProvider())

        assert _providers_of(entity)[0] is external


class TestSessionStatePersistence:
    """Provider state kept in the session bag survives across turns.

    Core documents the per-provider ``state`` dict as durable for the life of the session and
    persists it through ``AgentSession.to_dict()``. The entity builds a fresh session per
    operation, so it has to carry that state forward - otherwise providers silently start from
    scratch every turn (tool approval rules and queued approval requests, todo lists, memory
    extraction state).
    """

    async def test_provider_state_survives_across_turns(self) -> None:
        seen: list[dict[str, Any]] = []

        class _CountingProvider(ContextProvider):
            def __init__(self) -> None:
                super().__init__("counter")

            async def before_run(self, *, agent: Any, session: Any, context: Any, state: dict[str, Any]) -> None:
                seen.append(dict(state))

            async def after_run(self, *, agent: Any, session: Any, context: Any, state: dict[str, Any]) -> None:
                state["runs"] = state.get("runs", 0) + 1

        agent = _agent([_CountingProvider()])
        entity = _make_entity(agent, _InMemoryStateProvider())

        await _run_turns(entity, ["first", "second", "third"])

        assert seen[0] == {}  # nothing stored yet on the first turn
        assert seen[1] == {"runs": 1}
        assert seen[2] == {"runs": 2}

    async def test_state_is_persisted_as_plain_data(self) -> None:
        """Values go through core's serialization, so entity state stays JSON-safe."""

        class _StoringProvider(ContextProvider):
            def __init__(self) -> None:
                super().__init__("storer")

            async def after_run(self, *, agent: Any, session: Any, context: Any, state: dict[str, Any]) -> None:
                state.setdefault("note", Message(role="user", contents=["remember me"]))

        provider = _InMemoryStateProvider()
        agent = _agent([_StoringProvider()])
        await _run_turns(_make_entity(agent, provider), ["first"])

        session_payload = provider._get_state_dict()["data"]["session"]
        assert isinstance(session_payload["state"]["storer"]["note"], dict)
        # ...and comes back as a Message, because core pre-registers that type.
        restored = AgentSession.from_dict(dict(session_payload))
        assert isinstance(restored.state["storer"]["note"], Message)

    async def test_service_conversation_id_rides_along(self) -> None:
        """It is part of the serialized session, so it needs no field of its own."""
        provider = _InMemoryStateProvider()
        agent = _agent([InMemoryHistoryProvider()])
        entity = _make_entity(agent, provider)

        await _run_turns(entity, ["first"])
        assert "service_session_id" in provider._get_state_dict()["data"]["session"]

    async def test_tool_approval_state_survives_a_turn(self) -> None:
        """The motivating case: standing approvals must outlive the turn that granted them.

        It also comes back as ``ToolApprovalState`` rather than a plain dict. Core seeds its state
        type registry with only ``Message``, so the entity registers the serializable types loaded
        in this process before restoring.
        """
        # The harness is experimental; skip rather than fail if it moves.
        tool_approval = pytest.importorskip("agent_framework._harness._tool_approval")
        ToolApprovalRule = tool_approval.ToolApprovalRule
        ToolApprovalState = tool_approval.ToolApprovalState

        seen: list[Any] = []
        approval_key = "_tool_approval"

        class _ApprovalCarryingProvider(ContextProvider):
            def __init__(self) -> None:
                super().__init__("approvals")

            async def before_run(self, *, agent: Any, session: Any, context: Any, state: dict[str, Any]) -> None:
                seen.append(session.state.get(approval_key))

            async def after_run(self, *, agent: Any, session: Any, context: Any, state: dict[str, Any]) -> None:
                session.state.setdefault(
                    approval_key,
                    ToolApprovalState(rules=[ToolApprovalRule("delete_file")]),
                )

        agent = _agent([_ApprovalCarryingProvider()])
        await _run_turns(_make_entity(agent, _InMemoryStateProvider()), ["first", "second"])

        assert seen[0] is None  # nothing granted yet
        restored = seen[1]
        assert isinstance(restored, ToolApprovalState), f"approval state came back as {type(restored).__name__}"
        assert restored.rules[0].tool_name == "delete_file"

    async def test_durable_history_slice_is_not_persisted(self) -> None:
        """That slice is derived from conversation_history; storing it would duplicate it."""
        provider = _InMemoryStateProvider()
        agent = _agent([InMemoryHistoryProvider()])
        entity = _make_entity(agent, provider)

        await _run_turns(entity, ["first", "second"])

        durable_history = next(p for p in _providers_of(entity) if isinstance(p, DurableHistoryProvider))
        session_state = provider._get_state_dict()["data"]["session"]["state"]
        assert durable_history.source_id not in session_state

    async def test_durable_history_slice_is_dropped_before_serializing(self) -> None:
        """Not after. That slice holds the working buffer, so serializing it is wasted work.

        It also keeps a position index whose values reference durable state objects, so the less
        of it that reaches core's serializer the better.
        """
        serialized_keys: list[list[str]] = []

        class _SpySession:
            def __init__(self, state: dict[str, Any]) -> None:
                self.state = state
                self.service_session_id = None

            def to_dict(self) -> dict[str, Any]:
                serialized_keys.append(sorted(self.state))
                return {"session_id": "spy", "state": dict(self.state)}

        entity = _make_entity(_build_agent(RecordingChatClient()), _InMemoryStateProvider())
        durable_history = next(p for p in _providers_of(entity) if isinstance(p, DurableHistoryProvider))
        session = _SpySession({durable_history.source_id: {"messages": ["transcript"]}, "other": {"keep": 1}})

        entity._capture_session(session)

        assert serialized_keys == [["other"]], f"the durable slice was serialized: {serialized_keys}"
        assert durable_history.source_id in session.state, "the caller's session was left modified"

    async def test_unserializable_provider_state_does_not_break_the_turn(self) -> None:
        """Core passes a value it cannot serialize straight through, without raising or warning.

        Assigning that to entity state fails the save, and the error handler saves again with the
        same payload, so the second failure escapes and masks whatever the agent returned. The
        payload is checked first instead, keeping the last good session.
        """

        class _UnserializableProvider(ContextProvider):
            def __init__(self) -> None:
                super().__init__("unserializable")

            async def after_run(self, *, agent: Any, session: Any, context: Any, state: dict[str, Any]) -> None:
                state["handle"] = object()

        provider = _InMemoryStateProvider()
        agent = _agent([InMemoryHistoryProvider(), _UnserializableProvider()])
        entity = _make_entity(agent, provider)

        await _run_turns(entity, ["first"])

        stored = provider._get_state_dict()
        assert stored["data"].get("session") is None, "an unusable session payload was persisted"
        # The turn still completed and the conversation was recorded.
        assert len(entity.state.data.conversation_history) == 2
        json.dumps(stored)


class TestARequestIsAnsweredOnce:
    """A repeated correlation id returns the recorded answer instead of running again.

    Entity signals are delivered at least once, and every path mints a fresh correlation id per
    request, so a repeat is a duplicate delivery rather than a caller deliberately asking again.
    Running the agent a second time spends another model call, re-runs its tools, and produces a
    different answer that nothing can collect, because pollers read by correlation id and take the
    first match. Returning the recorded answer is what turns at-least-once delivery into a single
    effect.
    """

    async def test_the_agent_does_not_run_twice(self) -> None:
        client = RecordingChatClient()
        entity = _make_entity(_build_agent(client), _InMemoryStateProvider())

        await entity.run({"message": "what is the capital of Norway?", "correlationId": "dup"})
        await entity.run({"message": "what is the capital of Norway?", "correlationId": "dup"})

        assert len(client.received_messages) == 1

    async def test_the_same_answer_comes_back(self) -> None:
        entity = _make_entity(_build_agent(RecordingChatClient()), _InMemoryStateProvider())

        first = await entity.run({"message": "hello", "correlationId": "dup"})
        second = await entity.run({"message": "hello", "correlationId": "dup"})

        assert first.text == second.text

    async def test_the_conversation_is_not_recorded_twice(self) -> None:
        provider = _InMemoryStateProvider()
        entity = _make_entity(_build_agent(RecordingChatClient()), provider)

        await entity.run({"message": "hello", "correlationId": "dup"})
        await entity.run({"message": "hello", "correlationId": "dup"})

        entries = [e for e in entity.state.data.conversation_history if e.correlation_id == "dup"]
        assert len(entries) == 2, "expected one request and one response, not a second pair"

    async def test_a_failed_turn_is_also_answered_once(self) -> None:
        """The recorded failure comes back rather than the agent being run again.

        A caller retrying after a failure mints a new correlation id, so a repeat of this one is
        still a duplicate delivery of the same request.
        """

        class _FailingClient(RecordingChatClient):
            def get_response(self, messages: Any, **kwargs: Any) -> Any:
                super().get_response(messages, **kwargs)
                raise RuntimeError("kaboom")

        client = _FailingClient()
        entity = _make_entity(_build_agent(client), _InMemoryStateProvider())

        first = await entity.run({"message": "hello", "correlationId": "dup"})
        # A failing client makes the entity try streaming and then fall back, so one turn is more
        # than one client call. What matters is that the count does not grow on the repeat.
        after_first = len(client.received_messages)
        second = await entity.run({"message": "hello", "correlationId": "dup"})

        assert len(client.received_messages) == after_first
        assert any(content.type == "error" for content in first.messages[0].contents)
        assert any(content.type == "error" for content in second.messages[0].contents)

    async def test_a_different_request_still_runs(self) -> None:
        """Only an exact repeat is short-circuited."""
        client = RecordingChatClient()
        entity = _make_entity(_build_agent(client), _InMemoryStateProvider())

        await entity.run({"message": "first", "correlationId": "c0"})
        await entity.run({"message": "second", "correlationId": "c1"})

        assert len(client.received_messages) == 2
