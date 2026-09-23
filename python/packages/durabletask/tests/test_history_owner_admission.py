# Copyright (c) Microsoft. All rights reserved.

"""Admission requires one canonical transcript adapter, not one per source ID."""

from collections.abc import Callable
from copy import deepcopy

import pytest
from _history_pipeline_test_support import ToolChatClient
from _shared_history_test_support import (
    OrdinaryExternalHistory,
    _bound,
    _CanonicalStateProvider,
    _history_texts,
    _PassiveChatClient,
    _request,
    _stored,
)
from agent_framework import Agent, HistoryProvider, InMemoryHistoryProvider, SupportsAgentRun

from agent_framework_durabletask._history_provider import (
    DurableHistoryProvider,
    ensure_durable_history,
    validate_history_providers,
)


@pytest.mark.parametrize("primary_kind", ["durable", "in-memory", "injected"])
@pytest.mark.parametrize("writes", ["both", "inputs", "outputs", "context", "none"])
@pytest.mark.parametrize("admit", [validate_history_providers, ensure_durable_history])
def test_second_canonical_adapter_rejected_before_model_or_state_changes(
    primary_kind: str, writes: str, admit: Callable[[SupportsAgentRun], object]
) -> None:
    sink = DurableHistoryProvider(
        "distinct-audit",
        store_inputs=writes in ("both", "inputs"),
        store_outputs=writes in ("both", "outputs"),
        store_context_messages=writes == "context",
    )
    sink.load_messages = False
    configured: list[HistoryProvider] = [sink]
    if primary_kind == "durable":
        configured.insert(0, DurableHistoryProvider("primary"))
    elif primary_kind == "in-memory":
        configured.insert(0, InMemoryHistoryProvider("primary"))
    client = _PassiveChatClient()
    agent = Agent(client=client, context_providers=configured)
    original_providers = agent.context_providers
    original = tuple(original_providers)
    settings = [deepcopy(vars(provider)) for provider in original]
    owner = _CanonicalStateProvider([_request("seed", _stored("old", message_id="old"))])
    snapshot = owner.state.to_dict()

    with _bound(owner), pytest.raises(ValueError, match="only one DurableHistoryProvider"):
        admit(agent)

    assert client.received_messages == []
    assert owner.state.to_dict() == snapshot and owner.persist_count == 0
    assert agent.context_providers is original_providers
    assert tuple(agent.context_providers) == original
    assert [vars(provider) for provider in original] == settings


@pytest.mark.parametrize("all_load_disabled", [False, True])
def test_two_nonwriting_durable_adapters_are_still_unsupported(all_load_disabled: bool) -> None:
    first = DurableHistoryProvider("first", store_inputs=False, store_outputs=False)
    second = DurableHistoryProvider("second", store_inputs=False, store_outputs=False)
    first.load_messages = not all_load_disabled
    second.load_messages = False
    client = _PassiveChatClient()
    agent = Agent(client=client, context_providers=[first, second])

    with pytest.raises(ValueError, match="only one DurableHistoryProvider"):
        ensure_durable_history(agent)

    assert client.received_messages == []
    assert agent.context_providers == [first, second]


async def test_single_zero_store_durable_adapter_remains_supported() -> None:
    history = DurableHistoryProvider("primary", store_inputs=False, store_outputs=False, prune_excluded=False)
    client = _PassiveChatClient()
    agent = Agent(client=client, context_providers=[history])
    prepared = ensure_durable_history(agent)
    owner = _CanonicalStateProvider([_request("seed", _stored("old", message_id="old"))])
    before = owner.state.to_dict()
    for turn in range(2):
        with _bound(owner, f"turn-{turn}"):
            await prepared.run(f"input-{turn}", session=prepared.create_session())
        assert [message.text for message in client.received_messages[-1]] == ["old", f"input-{turn}"]
        assert owner.state.to_dict() == before
        owner = owner.clone()
    assert agent.context_providers == [history]


@pytest.mark.parametrize("audit_kind", ["external", "in-memory"])
@pytest.mark.parametrize("per_call", [False, True])
async def test_one_durable_owner_and_ordinary_audit_append_once_on_each_cold_turn(
    audit_kind: str, per_call: bool
) -> None:
    # Pin the policy so preparation preserves the adapter whose identity and flush we assert.
    history = DurableHistoryProvider("primary", prune_excluded=False)
    audit = (
        OrdinaryExternalHistory("audit", load_messages=False)
        if audit_kind == "external"
        else InMemoryHistoryProvider("audit", load_messages=False)
    )
    client = ToolChatClient(tool_calls=False)
    agent = Agent(
        client=client,
        context_providers=[history, audit],
        require_per_service_call_history_persistence=per_call,
    )
    prepared = ensure_durable_history(agent)
    assert isinstance(prepared, Agent)
    assert prepared.context_providers == [history, audit]
    owner = _CanonicalStateProvider([_request("seed", _stored("old", message_id="old"))])
    expected = ["old"]
    for turn in range(2):
        session = prepared.create_session()
        with _bound(owner, f"turn-{turn}"):
            await prepared.run(f"input-{turn}", session=session)
            history.flush(session.state[history.source_id])
        assert [message.text for message in client.received_messages[-1]] == [*expected, f"input-{turn}"]
        expected.extend([f"input-{turn}", f"answer-{turn + 1}"])
        assert _history_texts(owner) == expected
        if isinstance(audit, OrdinaryExternalHistory):
            assert [message.text for message in audit.saved] == expected[1:]
        else:
            assert [message.text for message in session.state["audit"]["messages"]] == expected[-2:]
        owner = owner.clone()
    assert agent.context_providers == [history, audit]
