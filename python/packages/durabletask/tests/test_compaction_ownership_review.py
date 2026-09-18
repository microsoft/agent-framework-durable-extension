# Copyright (c) Microsoft. All rights reserved.

"""Repro tests for compaction ownership, durable ID wrapping, and response timestamps."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest
from agent_framework import (
    Agent,
    AgentResponse,
    CompactionProvider,
    ContextProvider,
    InMemoryHistoryProvider,
    Message,
    SessionContext,
)
from test_private_history_pipeline import ToolChatClient
from test_shared_history_provider import (
    OrdinaryExternalHistory,
    _bound,
    _CanonicalStateProvider,
    _history_ids,
    _history_texts,
    _PassiveChatClient,
    _request,
    _stored,
)

from agent_framework_durabletask._history_provider import (
    DurableHistoryProvider,
    ensure_durable_history,
    prepare_history_owner,
)
from agent_framework_durabletask._shared_agent_state import DurableAgentStateResponse


class _AlwaysContext(ContextProvider):
    async def before_run(self, *, context: SessionContext, **kwargs: Any) -> None:
        context.extend_messages(self, [Message("user", [f"context-{self.source_id}"])])


async def test_service_owned_external_primary_suppresses_only_matching_after_compaction() -> None:
    before_seen: list[list[str]] = []
    after_seen: list[list[str]] = []

    async def before_strategy(messages: list[Message]) -> bool:
        before_seen.append([message.text for message in messages])
        return False

    async def after_strategy(messages: list[Message]) -> bool:
        after_seen.append([message.text for message in messages])
        for message in messages:
            message.additional_properties["after_seen"] = True
        return True

    primary = OrdinaryExternalHistory("primary")
    primary.saved = [Message("user", ["external-seed"], message_id="seed")]
    audit = InMemoryHistoryProvider("audit", load_messages=False)
    client = _PassiveChatClient(service_conversation_id="service-thread")
    compaction = CompactionProvider(
        before_strategy=before_strategy,
        after_strategy=after_strategy,
        history_source_id="primary",
    )
    agent = Agent(
        client=client,
        context_providers=[primary, audit, _AlwaysContext("always"), compaction],
    )
    session = agent.create_session(session_id="external-session")
    session.state["primary"] = {"messages": [Message("assistant", ["stale-state"], message_id="stale")]}

    for turn, service_owned in enumerate((True, False, True), start=1):
        prepared = prepare_history_owner(agent, service_owned)
        assert isinstance(prepared, Agent)
        await prepared.run(f"turn-{turn}", session=session, options={"store": service_owned})

    assert before_seen == [
        ["context-always"],
        ["external-seed", "context-always"],
        ["context-always"],
    ]
    assert all(f"turn-{turn}" not in batch for turn in (1, 2, 3) for batch in before_seen)
    assert after_seen == [["stale-state"]]
    assert [message.text for message in primary.saved] == ["external-seed", "turn-2", "answer-2"]
    assert [message.text for message in session.state["audit"]["messages"]] == [
        "turn-1",
        "answer-1",
        "turn-2",
        "answer-2",
        "turn-3",
        "answer-3",
    ]


@pytest.mark.parametrize("provider_mode", ["auto-nohistory", "existing-durable"])
@pytest.mark.parametrize("hook", ["before", "after"])
async def test_ensure_durable_history_wraps_compaction_hooks_with_unique_internal_ids(
    provider_mode: str, hook: str
) -> None:
    seen_ids: list[list[str | None]] = []

    async def before_strategy(messages: list[Message]) -> bool:
        seen_ids.append([message.message_id for message in messages])
        return False

    async def after_strategy(messages: list[Message]) -> bool:
        seen_ids.append([message.message_id for message in messages])
        return False

    history = DurableHistoryProvider(skip_excluded=False, source_id="durable")
    compaction = CompactionProvider(
        before_strategy=before_strategy if hook == "before" else None,
        after_strategy=after_strategy if hook == "after" else None,
        history_source_id=(
            InMemoryHistoryProvider.DEFAULT_SOURCE_ID if provider_mode == "auto-nohistory" else history.source_id
        ),
    )
    agent = Agent(
        client=ToolChatClient(tool_calls=False),
        context_providers=[compaction] if provider_mode == "auto-nohistory" else [history, compaction],
    )
    original_providers = tuple(agent.context_providers)

    prepared = ensure_durable_history(agent)
    prepared_again = ensure_durable_history(prepared)

    assert isinstance(prepared, Agent)
    assert isinstance(prepared_again, Agent)
    assert prepared is not agent
    assert tuple(agent.context_providers) == original_providers
    assert any(getattr(p, "__wrapped__", None) is compaction for p in prepared.context_providers)
    assert prepared_again.context_providers == prepared.context_providers

    provider = _CanonicalStateProvider([
        _request("seed", _stored("question", message_id="shared")),
        _request("seed", _stored("answer", role="assistant", message_id="shared")),
    ])
    session = prepared.create_session(session_id="wrapped-compaction")

    with _bound(provider):
        await prepared.run("live input", session=session)
        durable = next(
            provider_item
            for provider_item in prepared.context_providers
            if isinstance(provider_item, DurableHistoryProvider)
        )
        durable.flush(session.state[durable.source_id])

    assert seen_ids
    assert all(message_id for message_id in seen_ids[0])
    assert len(seen_ids[0]) == len(set(seen_ids[0]))
    cold = provider.clone()
    with _bound(cold):
        loaded = await DurableHistoryProvider(skip_excluded=False, source_id="durable").get_messages(
            "session", state={}
        )
    assert [message.message_id for message in loaded[:2]] == ["shared", "shared"]
    assert [message.message_id for message in loaded[2:]] == [None, None]
    assert _history_texts(provider) == ["question", "answer", "live input", "answer-1"]
    assert len(_history_ids(provider)) == len(set(_history_ids(provider))) == 4


@pytest.mark.parametrize("provider_mode", ["auto-nohistory", "existing-durable"])
def test_ensure_durable_history_is_idempotent_without_mutating_the_original_agent(provider_mode: str) -> None:
    async def strategy(messages: list[Message]) -> bool:
        return False

    source = InMemoryHistoryProvider.DEFAULT_SOURCE_ID if provider_mode == "auto-nohistory" else "target"
    compaction = CompactionProvider(after_strategy=strategy, history_source_id=source)
    history = DurableHistoryProvider(skip_excluded=False, source_id="target")
    agent = Agent(
        client=_PassiveChatClient(),
        context_providers=[compaction] if provider_mode == "auto-nohistory" else [history, compaction],
    )
    original = tuple(agent.context_providers)

    prepared = ensure_durable_history(agent)
    prepared_again = ensure_durable_history(prepared)

    assert isinstance(prepared, Agent)
    assert isinstance(prepared_again, Agent)
    assert prepared is not agent
    assert tuple(agent.context_providers) == original
    assert prepared_again.context_providers == prepared.context_providers
    assert any(getattr(p, "__wrapped__", None) is compaction for p in prepared.context_providers)


async def test_history_after_run_preserves_raw_response_timestamp_and_allocates_unique_internal_ids() -> None:
    provider = _CanonicalStateProvider()
    history = DurableHistoryProvider(store_inputs=False, skip_excluded=False)
    created_at = "2026-09-18T01:02:03.123456789+05:30"
    response = AgentResponse(
        created_at=created_at,
        messages=[
            Message("assistant", ["first"], message_id="duplicate"),
            Message("assistant", ["second"]),
            Message("assistant", ["third"], message_id="duplicate"),
        ],
    )
    context = SessionContext(input_messages=[Message("user", ["input"])])
    context._response = response

    with _bound(provider, "corr"):
        await history.after_run(agent=None, session=None, context=context, state={})

    assert len(provider.state.data.conversation_history) == 1
    stored = provider.state.data.conversation_history[0]
    assert isinstance(stored, DurableAgentStateResponse)
    assert stored.to_dict()["createdAt"] == created_at
    rows = [message.to_dict() for message in stored.messages]
    assert [row.get("messageId") for row in rows] == ["duplicate", None, "duplicate"]
    assert len({row.get("pythonHistoryId", row.get("messageId")) for row in rows}) == 3
    restored = DurableAgentStateResponse.to_run_response(stored)
    assert restored.created_at == created_at


@pytest.mark.parametrize("created_at", [None, "not-a-timestamp"], ids=["missing", "invalid"])
async def test_history_after_run_uses_an_aware_timestamp_fallback_for_missing_or_invalid_response_timestamps(
    created_at: str | None,
) -> None:
    provider = _CanonicalStateProvider()
    history = DurableHistoryProvider(store_inputs=False, skip_excluded=False)
    response = AgentResponse(created_at=created_at, messages=[Message("assistant", ["done"])])
    context = SessionContext(input_messages=[Message("user", ["input"])])
    context._response = response

    with _bound(provider, "corr"):
        await history.after_run(agent=None, session=None, context=context, state={})

    stored = provider.state.data.conversation_history[0]
    assert isinstance(stored, DurableAgentStateResponse)
    assert stored.created_at is not None
    assert stored.created_at.tzinfo is not None
    restored = DurableAgentStateResponse.to_run_response(stored)
    parsed = datetime.fromisoformat(str(restored.created_at).replace("Z", "+00:00"))
    assert parsed.tzinfo is not None
