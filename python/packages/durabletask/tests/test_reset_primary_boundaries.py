# Copyright (c) Microsoft. All rights reserved.

"""Reset follows the primary, while registration permits only one durable transcript owner."""

import json
from collections.abc import Sequence
from copy import deepcopy
from typing import Any
from unittest.mock import Mock

import pytest
from _retention_test_support import JsonStateProvider, RecordingChatClient
from _session_persistence_test_support import (
    AUDIT_SOURCES,
    EXTERNAL_SOURCE,
    LOCAL_SOURCE,
    PRIOR_CORRELATION,
    PRIOR_OCCURRENCE,
    SESSION_ID,
    _opaque_slice,
    _original_response,
    _prior_input,
)
from _session_persistence_test_support import _reset_seed as _seed
from agent_framework import (
    Agent,
    AgentSession,
    ContextProvider,
    HistoryProvider,
    InMemoryHistoryProvider,
    Message,
)

from agent_framework_durabletask import AgentEntity
from agent_framework_durabletask._history_provider import DurableHistoryProvider
from agent_framework_durabletask._message_identity import message_identity

PROBE_SOURCE = "reset-final-flush-probe"


def make_agent(client: Any, providers: list[Any] | None = None) -> Agent:
    return Agent(client=client, name="revision", context_providers=providers)


def _json(value: Any) -> str:
    return json.dumps(value, allow_nan=False, ensure_ascii=False, sort_keys=True)


def _wire(value: Any) -> Any:
    return json.loads(_json(value))


def _assert_json_equal(actual: Any, expected: Any) -> None:
    # JSON comparison also distinguishes booleans from numerically equal integers.
    assert _json(actual) == _json(expected)


def _messages(messages: Sequence[Message]) -> list[dict[str, Any]]:
    return _wire([message.to_dict() for message in messages])


class _Rows:
    def __init__(self) -> None:
        self.messages = [_prior_input(), *_original_response().messages]
        self.calls: list[str] = []

    async def load(self) -> list[Message]:
        self.calls.append("load")
        return deepcopy(self.messages)

    async def save(self, messages: Sequence[Message]) -> None:
        self.calls.append("save")
        self.messages.extend(deepcopy(list(messages)))

    def clear(self) -> None:
        self.calls.append("clear")
        self.messages.clear()


class _ExternalHistory(HistoryProvider):
    def __init__(self, *, load_messages: bool = True, source_id: str = EXTERNAL_SOURCE) -> None:
        super().__init__(source_id, load_messages=load_messages)
        self.rows = _Rows()

    async def before_run(self, **kwargs: Any) -> None:
        self.rows.calls.append("before")
        await super().before_run(**kwargs)

    async def after_run(self, **kwargs: Any) -> None:
        self.rows.calls.append("after")
        await super().after_run(**kwargs)

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        assert session_id == SESSION_ID
        return await self.rows.load()

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        assert session_id == SESSION_ID
        await self.rows.save(messages)

    def clear(self, *args: Any, **kwargs: Any) -> None:
        self.rows.clear()


class _ExternalInMemoryHistory(InMemoryHistoryProvider):
    """A deliberate custom store whose base type must not authorize local reset."""

    def __init__(self) -> None:
        super().__init__(EXTERNAL_SOURCE, load_messages=True)
        self.rows = _Rows()

    async def before_run(self, **kwargs: Any) -> None:
        self.rows.calls.append("before")
        await super().before_run(**kwargs)

    async def after_run(self, **kwargs: Any) -> None:
        self.rows.calls.append("after")
        await super().after_run(**kwargs)

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        assert session_id == SESSION_ID
        return await self.rows.load()

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        assert session_id == SESSION_ID
        await self.rows.save(messages)

    def clear(self, *args: Any, **kwargs: Any) -> None:
        self.rows.clear()


def _durable_audits(count: int, *, store_messages: bool = False) -> list[DurableHistoryProvider]:
    audits = [
        DurableHistoryProvider(
            source,
            store_inputs=store_messages,
            store_outputs=store_messages,
            prune_excluded=False,
        )
        for source in AUDIT_SOURCES[:count]
    ]
    for audit in audits:
        audit.load_messages = False
    # Multiple durable adapters are only used by rejection tests, even with writes disabled.
    return audits


def _ordinary_audits(count: int) -> list[_ExternalHistory]:
    return [_ExternalHistory(source_id=source, load_messages=False) for source in AUDIT_SOURCES[:count]]


def _ordered(primary: HistoryProvider, audits: Sequence[HistoryProvider], order: str) -> list[ContextProvider]:
    return [primary, *audits] if order == "primary-first" else [*audits, primary]


class _NoPipeline:
    """Delegate to a real Core Agent without exposing its context-provider pipeline."""

    name = "reset-no-pipeline"

    def __init__(self, client: RecordingChatClient) -> None:
        self._agent = make_agent(client)

    def run(self, **kwargs: Any) -> Any:
        return self._agent.run(**kwargs)


def _entity(
    client: RecordingChatClient,
    providers: list[ContextProvider] | None,
    *,
    cold: bool,
    service_session_id: str | None = None,
    no_pipeline: bool = False,
) -> tuple[AgentEntity, JsonStateProvider]:
    store = JsonStateProvider()
    store.state = _seed(service_session_id)
    if cold:
        store = JsonStateProvider(_wire(store.raw))
    agent: Any = _NoPipeline(client) if no_pipeline else make_agent(client, providers)
    return AgentEntity(agent, state_provider=store), store


def _assert_rejected_without_mutation(
    entity: AgentEntity,
    store: JsonStateProvider,
    client: RecordingChatClient,
    primary: _ExternalHistory | _ExternalInMemoryHistory,
    audits: Sequence[_ExternalHistory] = (),
) -> dict[str, Any]:
    cached = entity.state
    cached_data = cached.data
    session = cached_data.session
    before = _wire(store.raw)
    warm_before = _wire(cached.to_dict())
    histories = [primary, *audits]
    rows = [_messages(provider.rows.messages) for provider in histories]
    calls = [list(provider.rows.calls) for provider in histories]
    model_calls = [_messages(batch) for batch in client.received_messages]
    writes = store.writes

    with pytest.raises(NotImplementedError, match="external"):
        entity.reset()

    assert entity.state is cached
    assert entity.state.data is cached_data
    assert entity.state.data.session is session
    _assert_json_equal(entity.state.to_dict(), warm_before)
    _assert_json_equal(store.raw, before)
    _assert_json_equal([_messages(provider.rows.messages) for provider in histories], rows)
    _assert_json_equal([provider.rows.calls for provider in histories], calls)
    _assert_json_equal([_messages(batch) for batch in client.received_messages], model_calls)
    assert store.writes == writes
    return before


@pytest.mark.parametrize(
    ("kind", "audit_count"),
    [("durable", 1), ("in-memory-autoconverted", 1), ("injected", 1), ("custom", 2), ("in-memory-subclass", 2)],
)
@pytest.mark.parametrize("store_messages", [False, True], ids=["no-writes", "writing"])
@pytest.mark.parametrize("order", ["primary-first", "audit-first"])
@pytest.mark.parametrize("cold", [False, True], ids=["warm", "cold"])
def test_multiple_durable_owners_reject_registration_before_writes_or_models(
    kind: str, audit_count: int, store_messages: bool, order: str, cold: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary: HistoryProvider | None = None
    if kind == "durable":
        primary = DurableHistoryProvider(LOCAL_SOURCE, prune_excluded=False)
    elif kind == "in-memory-autoconverted":
        primary = InMemoryHistoryProvider(LOCAL_SOURCE)
    elif kind == "custom":
        primary = _ExternalHistory()
    elif kind == "in-memory-subclass":
        primary = _ExternalInMemoryHistory()
    audits = _durable_audits(audit_count, store_messages=store_messages)
    providers: list[ContextProvider] = list(audits) if primary is None else _ordered(primary, audits, order)
    client = RecordingChatClient()
    agent = make_agent(client, providers)
    original_providers = agent.context_providers
    original = tuple(original_providers)
    histories: list[HistoryProvider] = list(audits)
    if primary is not None:
        histories.append(primary)
    flags = [
        (provider.load_messages, provider.store_inputs, provider.store_outputs, provider.store_context_messages)
        for provider in histories
    ]
    before = _wire(_seed("parked-service-session").to_dict())
    store = JsonStateProvider(before)
    if not cold:
        _assert_json_equal(store.state.to_dict(), before)
    cached = store._state_cache
    persisted = deepcopy(store._persisted_state_snapshot)
    write = Mock(wraps=store._set_state_dict)
    monkeypatch.setattr(store, "_set_state_dict", write)

    with pytest.raises(ValueError, match="only one DurableHistoryProvider"):
        AgentEntity(agent, state_provider=store)

    write.assert_not_called()
    assert store.writes == 0 and client.received_messages == []
    assert store._state_cache is cached
    if cached is not None:
        _assert_json_equal(cached.to_dict(), before)
    _assert_json_equal(store._persisted_state_snapshot, persisted)
    _assert_json_equal(store.raw, before)
    assert agent.context_providers is original_providers
    assert tuple(agent.context_providers) == original
    assert [
        (provider.load_messages, provider.store_inputs, provider.store_outputs, provider.store_context_messages)
        for provider in histories
    ] == flags
    if isinstance(primary, (_ExternalHistory, _ExternalInMemoryHistory)):
        assert primary.rows.calls == []
        _assert_json_equal(
            _messages(primary.rows.messages), _messages([_prior_input(), *_original_response().messages])
        )


@pytest.mark.parametrize("order", ["primary-first", "audit-first"])
@pytest.mark.parametrize("audit_count", [0, 1, 2])
@pytest.mark.parametrize("cold", [False, True], ids=["warm", "cold"])
@pytest.mark.parametrize("kind", ["custom", "in-memory-subclass"])
@pytest.mark.parametrize("service_session_id", [None, "parked-service-session"], ids=["no-service-id", "parked-id"])
async def test_external_primary_reset_rejects_before_mutation_and_next_turn_keeps_context(
    order: str, audit_count: int, cold: bool, kind: str, service_session_id: str | None
) -> None:
    primary = _ExternalHistory() if kind == "custom" else _ExternalInMemoryHistory()
    audits = _ordinary_audits(audit_count)
    providers = _ordered(primary, audits, order)
    client = RecordingChatClient()
    entity, store = _entity(client, providers, cold=cold, service_session_id=service_session_id)
    configured = list(getattr(entity.agent, "context_providers", []))
    assert configured == providers
    assert not any(isinstance(provider, DurableHistoryProvider) for provider in configured)
    assert [
        provider for provider in configured if isinstance(provider, HistoryProvider) and provider.load_messages
    ] == [primary]
    assert primary.rows.calls == [] and client.received_messages == []
    prior_response = entity.state.try_get_agent_response(PRIOR_CORRELATION)
    assert prior_response is not None
    _assert_json_equal(prior_response.to_dict(), _original_response().to_dict())

    before = _assert_rejected_without_mutation(entity, store, client, primary, audits)
    writes = store.writes
    response = await entity.run({
        "message": "next question",
        "correlationId": "reset-next",
        "options": {"store": False},
    })

    assert response.text == "reply-1"
    assert [[message.text for message in batch] for batch in client.received_messages] == [
        ["prior question", "prior answer", "next question"]
    ]
    assert primary.rows.calls == ["before", "load", "after", "save"]
    assert [message.text for message in primary.rows.messages] == [
        "prior question",
        "prior answer",
        "next question",
        "reply-1",
    ]
    _assert_json_equal(
        _messages(primary.rows.messages[:2]), _messages([_prior_input(), *_original_response().messages])
    )
    for audit in audits:
        assert audit.rows.calls == ["after", "save"]
        _assert_json_equal(_messages(audit.rows.messages), _messages(primary.rows.messages))
    _assert_json_equal(store.raw["data"]["conversationHistory"], before["data"]["conversationHistory"])
    _assert_json_equal(store.raw["data"]["session"], before["data"]["session"])
    for field in ("terminalResults", "completionReceipts"):
        _assert_json_equal(store.raw["data"][field][PRIOR_CORRELATION], before["data"][field][PRIOR_CORRELATION])
    _assert_json_equal(store.raw["data"]["pythonIngestion"], before["data"]["pythonIngestion"])
    assert store.writes == writes + 1


@pytest.mark.parametrize("order", ["primary-first", "audit-first"])
@pytest.mark.parametrize("cold", [False, True], ids=["warm", "cold"])
async def test_successful_external_turn_with_writing_audit_still_rejects_reset(order: str, cold: bool) -> None:
    primary = _ExternalHistory()
    audits = _durable_audits(1, store_messages=True)
    client = RecordingChatClient()
    entity, store = _entity(client, _ordered(primary, audits, order), cold=False)
    response = await entity.run({"message": "successful question", "correlationId": "actual-success"})
    assert response.text == "reply-1"
    assert [[message.text for message in batch] for batch in client.received_messages] == [
        ["prior question", "prior answer", "successful question"]
    ]
    history = entity.state.data.conversation_history
    assert [message.to_chat_message().text for entry in history for message in entry.messages] == [
        "prior question",
        "prior answer",
        "successful question",
        "reply-1",
    ]
    if cold:
        store = JsonStateProvider(_wire(store.raw))
        entity = AgentEntity(
            make_agent(client, _ordered(primary, _durable_audits(1, store_messages=True), order)), state_provider=store
        )
    before = _assert_rejected_without_mutation(entity, store, client, primary)
    duplicate = await entity.run({"message": "must not execute", "correlationId": "actual-success"})
    _assert_json_equal(duplicate.to_dict(), response.to_dict())
    _assert_json_equal(store.raw, before)
    assert len(client.received_messages) == 1
    next_response = await entity.run({"message": "after rejection", "correlationId": "actual-next"})
    assert next_response.text == "reply-2"
    assert [message.text for message in client.received_messages[-1]] == [
        "prior question",
        "prior answer",
        "successful question",
        "reply-1",
        "after rejection",
    ]


async def _assert_local_reset(
    providers: list[ContextProvider] | None,
    *,
    cold: bool,
    no_pipeline: bool = False,
) -> None:
    client = RecordingChatClient()
    entity, store = _entity(
        client, providers, cold=cold, service_session_id="parked-service-session", no_pipeline=no_pipeline
    )
    if not no_pipeline:
        primaries = [
            provider
            for provider in getattr(entity.agent, "context_providers", [])
            if isinstance(provider, HistoryProvider) and provider.load_messages
        ]
        assert len(primaries) == 1 and isinstance(primaries[0], DurableHistoryProvider)
    before = _wire(store.raw)
    expected = deepcopy(before)
    expected["data"]["conversationHistory"] = []
    del expected["data"]["session"]
    writes = store.writes

    entity.reset()

    assert entity.state.data.conversation_history == []
    assert entity.state.data.session is None
    _assert_json_equal(store.raw, expected)
    _assert_json_equal(entity.state.to_dict(), expected)
    assert store.writes == writes + 1 and client.received_messages == []
    duplicate = await entity.run({"message": "must not execute", "correlationId": PRIOR_CORRELATION})
    _assert_json_equal(duplicate.to_dict(), _original_response().to_dict())
    _assert_json_equal(store.raw, expected)
    assert store.writes == writes + 1 and client.received_messages == []

    next_input = Message("user", ["next question"], message_id="reset-next-input")
    response = await entity.run({
        "message": "next question",
        "correlationId": "local-next",
        "contextMessages": _messages([_prior_input(), next_input]),
        "contextMessageIds": [PRIOR_OCCURRENCE, "reset-next-occurrence"],
        "options": {"store": False},
    })

    assert response.text == "reply-1"
    assert [[message.text for message in batch] for batch in client.received_messages] == [["next question"]]
    assert [
        message.to_chat_message().text for entry in entity.state.data.conversation_history for message in entry.messages
    ] == ["next question", "reply-1"]
    for field in ("terminalResults", "completionReceipts"):
        _assert_json_equal(store.raw["data"][field][PRIOR_CORRELATION], before["data"][field][PRIOR_CORRELATION])
    ingestion = deepcopy(before["data"]["pythonIngestion"])
    ingestion["messages"]["reset-next-occurrence"] = [message_identity(next_input)]
    _assert_json_equal(store.raw["data"]["pythonIngestion"], ingestion)
    _assert_json_equal(store.raw["futureRoot"], before["futureRoot"])
    _assert_json_equal(store.raw["data"]["futureData"], before["data"]["futureData"])
    assert store.writes == writes + 2


@pytest.mark.parametrize("kind", ["default", "no-pipeline", "durable", "in-memory-autoconverted"])
@pytest.mark.parametrize("cold", [False, True], ids=["warm", "cold"])
async def test_local_reset_clears_only_history_and_session_preserving_delivery_and_ingestion(
    kind: str, cold: bool
) -> None:
    providers: list[ContextProvider] | None = None
    if kind == "durable":
        providers = [DurableHistoryProvider(LOCAL_SOURCE, prune_excluded=False)]
    elif kind == "in-memory-autoconverted":
        providers = [InMemoryHistoryProvider(LOCAL_SOURCE)]
    await _assert_local_reset(providers, cold=cold, no_pipeline=kind == "no-pipeline")


@pytest.mark.parametrize("kind", ["durable", "in-memory-autoconverted"])
@pytest.mark.parametrize("audit_count", [1, 2])
@pytest.mark.parametrize("order", ["primary-first", "audit-first"])
@pytest.mark.parametrize("cold", [False, True], ids=["warm", "cold"])
async def test_store_only_audits_do_not_make_local_primary_reset_external(
    kind: str, audit_count: int, order: str, cold: bool
) -> None:
    primary = (
        DurableHistoryProvider(LOCAL_SOURCE, prune_excluded=False)
        if kind == "durable"
        else InMemoryHistoryProvider(LOCAL_SOURCE)
    )
    audits = _ordinary_audits(audit_count)
    await _assert_local_reset(_ordered(primary, audits, order), cold=cold)
    for audit in audits:
        assert audit.rows.calls == ["after", "save"]
        assert [message.text for message in audit.rows.messages] == [
            "prior question",
            "prior answer",
            "next question",
            "reply-1",
        ]
        _assert_json_equal(
            _messages(audit.rows.messages[:2]), _messages([_prior_input(), *_original_response().messages])
        )


class _AnnotateAfterAudit(ContextProvider):
    """Run after the writing audit so only the entity's final flush can save this edit."""

    def __init__(self) -> None:
        super().__init__(PROBE_SOURCE)
        self.buffer_texts: list[list[str]] = []

    async def after_run(self, *, session: AgentSession, state: dict[str, Any], **kwargs: Any) -> None:
        audit_state = session.state[AUDIT_SOURCES[0]]
        buffer: list[Message] = audit_state["messages"]
        self.buffer_texts.append([message.text for message in buffer])
        assert isinstance(audit_state["_positions"], dict) and audit_state["_positions"]
        buffer[-1].additional_properties["after_audit"] = {"keep": [False, 0, "雪", None]}
        state["visited"] = ["after-audit"]


@pytest.mark.parametrize("order", ["primary-first", "audit-first"])
async def test_ordinary_external_run_keeps_audit_final_flush_and_only_prunes_durable_transients(order: str) -> None:
    primary = _ExternalHistory()
    audit = _durable_audits(1, store_messages=True)[0]
    probe = _AnnotateAfterAudit()
    client = RecordingChatClient()
    # Core runs after hooks in reverse order. The probe must follow the audit's save.
    entity, store = _entity(client, [probe, *_ordered(primary, [audit], order)], cold=True)
    before = _wire(store.raw)

    response = await entity.run({
        "message": "next question",
        "correlationId": "flush-control",
        "options": {"store": False},
    })

    assert response.text == "reply-1"
    assert [[message.text for message in batch] for batch in client.received_messages] == [
        ["prior question", "prior answer", "next question"]
    ]
    assert probe.buffer_texts == [["prior question", "prior answer", "next question", "reply-1"]]
    history = entity.state.data.conversation_history
    assert [message.to_chat_message().text for entry in history for message in entry.messages] == [
        "prior question",
        "prior answer",
        "next question",
        "reply-1",
    ]
    _assert_json_equal(
        store.raw["data"]["conversationHistory"][-1]["messages"][-1]["extensionData"]["after_audit"],
        {"keep": [False, 0, "雪", None]},
    )
    expected_session = deepcopy(before["data"]["session"])
    expected_session["state"][PROBE_SOURCE] = {"visited": ["after-audit"]}
    _assert_json_equal(store.raw["data"]["session"], expected_session)
    # Positive exact equality retains both opaque same-name keys, at all non-durable levels.
    _assert_json_equal(store.raw["data"]["session"]["state"][EXTERNAL_SOURCE], _opaque_slice("external"))
    _assert_json_equal(
        store.raw["data"]["session"]["state"][audit.source_id],
        {"metadata": {"messages": ["keep", audit.source_id], "_positions": [1, 0]}},
    )
    assert primary.rows.calls == ["before", "load", "after", "save"]
    assert [message.text for message in primary.rows.messages] == [
        "prior question",
        "prior answer",
        "next question",
        "reply-1",
    ]
    _assert_json_equal(_messages(primary.rows.messages[-1:]), _messages(response.messages))
    assert store.writes == 1


@pytest.mark.parametrize("order", ["primary-first", "audit-first"])
async def test_service_owned_external_run_keeps_primary_hooks_silent_but_reset_still_rejects(order: str) -> None:
    primary = _ExternalHistory()
    client = RecordingChatClient()
    entity, store = _entity(
        client,
        _ordered(primary, _durable_audits(1, store_messages=True), order),
        cold=True,
        service_session_id="parked-service-session",
    )
    before = _wire(store.raw)
    rows = _messages(primary.rows.messages)

    response = await entity.run({
        "message": "service question",
        "correlationId": "service-owned",
        "options": {"store": True},
    })

    assert response.text == "reply-1"
    assert [[message.text for message in batch] for batch in client.received_messages] == [["service question"]]
    assert primary.rows.calls == []
    _assert_json_equal(_messages(primary.rows.messages), rows)
    _assert_json_equal(store.raw["data"]["conversationHistory"], before["data"]["conversationHistory"])
    _assert_rejected_without_mutation(entity, store, client, primary)
