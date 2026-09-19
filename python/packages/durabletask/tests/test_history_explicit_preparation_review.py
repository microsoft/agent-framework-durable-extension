# Copyright (c) Microsoft. All rights reserved.

"""Intentional namespace rejection and explicit history-provider hook ordering."""

from typing import Any

import pytest
from agent_framework import Agent, CompactionProvider, ContextProvider, InMemoryHistoryProvider, Message, SessionContext
from test_shared_history_provider import _bound, _CanonicalStateProvider, _history_texts, _request, _stored
from test_shared_history_provider import _PassiveChatClient as RecordingChatClient

from agent_framework_durabletask._history_provider import DurableHistoryProvider, ensure_durable_history


class CurrentContext(ContextProvider):
    async def before_run(self, *, context: SessionContext, **kwargs: Any) -> None:
        context.extend_messages(self, [Message("user", ["current-context"])])


class SimpleCallable:
    def __init__(self) -> None:
        self.seen: list[list[str]] = []

    async def __call__(self, messages: list[Message]) -> bool:
        self.seen.append([message.text for message in messages])
        return False


def test_store_only_default_namespace_without_primary_or_compaction_raises() -> None:
    audit = InMemoryHistoryProvider(load_messages=False)
    client = RecordingChatClient()
    agent = Agent(client=client, context_providers=[audit])
    assert audit.source_id == InMemoryHistoryProvider.DEFAULT_SOURCE_ID == "in_memory"

    with pytest.raises(ValueError, match="Cannot inject durable history: 'in_memory' is already used"):
        ensure_durable_history(agent)

    assert agent.context_providers == [audit]
    assert audit.source_id == "in_memory"
    assert audit.load_messages is False
    assert client.received_messages == []


async def test_renamed_store_only_audit_coexists_with_injected_core_default_history() -> None:
    audit = InMemoryHistoryProvider("audit", load_messages=False)
    client = RecordingChatClient()
    agent = Agent(client=client, context_providers=[audit])

    prepared = ensure_durable_history(agent)

    assert isinstance(prepared, Agent)
    assert [provider.source_id for provider in prepared.context_providers] == ["audit", "in_memory"]
    assert prepared.context_providers[0] is audit
    history = prepared.context_providers[1]
    assert isinstance(history, DurableHistoryProvider)
    assert history.source_id == InMemoryHistoryProvider.DEFAULT_SOURCE_ID
    assert history.load_messages is True
    assert audit.load_messages is False
    assert agent.context_providers == [audit]

    provider = _CanonicalStateProvider([_request("old", _stored("seed", message_id="seed-user"))])
    session = prepared.create_session()
    session.state["audit"] = {"messages": [Message("user", ["audit-only"])]}
    with _bound(provider):
        await prepared.run("current", session=session)

    assert [[message.text for message in batch] for batch in client.received_messages] == [["seed", "current"]]
    assert [message.text for message in session.state["audit"]["messages"]] == ["audit-only", "current", "answer-1"]
    assert [message.text for message in session.state["in_memory"]["messages"]] == ["seed", "current", "answer-1"]
    assert session.state["audit"] is not session.state["in_memory"]
    assert _history_texts(provider) == ["seed", "current", "answer-1"]
    assert agent.context_providers == [audit]


@pytest.mark.parametrize("provider_kind", ["in-memory", "durable"])
@pytest.mark.parametrize("history_first", [False, True])
async def test_explicit_history_preserves_before_and_after_strategy_inputs(
    provider_kind: str, history_first: bool
) -> None:
    current = CurrentContext("current-context")
    history = (
        InMemoryHistoryProvider("explicit-history")
        if provider_kind == "in-memory"
        else DurableHistoryProvider("explicit-history")
    )
    strategy = SimpleCallable()
    after_strategy = SimpleCallable()
    compaction = CompactionProvider(
        before_strategy=strategy, after_strategy=after_strategy, history_source_id=history.source_id
    )
    client = RecordingChatClient()
    agent = Agent(
        client=client,
        context_providers=[current, history, compaction] if history_first else [current, compaction, history],
    )
    original_providers = agent.context_providers
    original = tuple(agent.context_providers)

    prepared = ensure_durable_history(agent)

    assert isinstance(prepared, Agent)
    assert [provider.source_id for provider in prepared.context_providers] == [p.source_id for p in original]
    assert prepared.context_providers[0] is current
    prepared_compaction = prepared.context_providers[2 if history_first else 1]
    assert isinstance(prepared_compaction, CompactionProvider)
    assert getattr(prepared_compaction, "__wrapped__", None) is compaction
    assert prepared_compaction.before_strategy is strategy
    assert prepared_compaction.after_strategy is after_strategy
    assert prepared_compaction.history_source_id == history.source_id
    prepared_history = prepared.context_providers[1 if history_first else 2]
    assert isinstance(prepared_history, DurableHistoryProvider)
    assert (prepared_history is history) is (provider_kind == "durable")
    assert tuple(agent.context_providers) == original

    provider = _CanonicalStateProvider([_request("old", _stored("seed", message_id="seed-user"))])
    session = prepared.create_session()
    with _bound(provider):
        await prepared.run("current", session=session)
        prepared_history.flush(session.state[history.source_id])

    # Nonempty earlier context makes Core invoke the strategy instead of skipping it.
    assert strategy.seen == ([["current-context", "seed"]] if history_first else [["current-context"]])
    assert after_strategy.seen == ([["seed"]] if history_first else [["seed", "current", "answer-1"]])
    assert [[(message.role, message.text) for message in batch] for batch in client.received_messages] == [
        [("user", "current-context"), ("user", "seed"), ("user", "current")]
    ]
    assert _history_texts(provider) == ["seed", "current", "answer-1"]
    assert agent.context_providers is original_providers
    assert tuple(agent.context_providers) == original
