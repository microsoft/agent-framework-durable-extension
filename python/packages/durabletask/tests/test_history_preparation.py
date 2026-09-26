# Copyright (c) Microsoft. All rights reserved.

"""Prepared history ordering, ownership and forward-compatible input identity."""

from copy import deepcopy
from typing import Any

import pytest
from _shared_history_test_support import _bound, _CanonicalStateProvider, _PassiveChatClient, _request, _stored
from agent_framework import Agent, AgentSession, CompactionProvider, InMemoryHistoryProvider, Message, SessionContext

from agent_framework_durabletask._history_provider import (
    DurableHistoryProvider,
    ensure_durable_history,
    prepare_history_owner,
)
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._response_utils import load_agent_response, preserve_input_envelope
from agent_framework_durabletask._shared_agent_state import DurableAgentStateMessage


@pytest.mark.parametrize("before_enabled", [False, True])
async def test_replaced_builtin_history_preserves_core_before_and_after_order(before_enabled: bool) -> None:
    observed: list[list[str]] = []
    after_observed: list[list[str]] = []

    async def before(messages: list[Message]) -> bool:
        observed.append([m.text for m in messages])
        return False

    async def after(messages: list[Message]) -> bool:
        after_observed.append([m.text for m in messages])
        return False

    primary = InMemoryHistoryProvider("custom")
    compaction = CompactionProvider(
        before_strategy=before if before_enabled else None, after_strategy=after, history_source_id="custom"
    )
    client = _PassiveChatClient()
    agent = Agent(client=client, context_providers=[compaction, primary])
    providers = agent.context_providers
    primary_before = deepcopy(vars(primary))

    # Compare against the actual Core pipeline, not an assumed hook schedule.
    baseline_session = agent.create_session()
    baseline_session.state["custom"] = {"messages": [Message("user", ["prior"], message_id="prior")]}
    await agent.run("current", session=baseline_session)
    expected_before = deepcopy(observed)
    expected_after = deepcopy(after_observed)
    assert expected_before == []  # Compaction precedes history loading.
    assert expected_after == [["prior", "current", "answer-1"]]
    expected_model = [message.text for message in client.received_messages[0]]
    observed.clear()
    after_observed.clear()
    client.received_messages.clear()

    prepared = ensure_durable_history(agent)
    assert isinstance(prepared, Agent)
    assert [p.source_id for p in prepared.context_providers] == [compaction.source_id, primary.source_id]
    assert isinstance(prepared.context_providers[1], DurableHistoryProvider)
    provider = _CanonicalStateProvider([_request("seed", _stored("prior", message_id="prior"))])
    session = prepared.create_session()
    with _bound(provider):
        await prepared.run("current", session=session)
        prepared.context_providers[1].flush(session.state["custom"])
    assert observed == expected_before
    assert after_observed == expected_after
    assert [message.text for message in client.received_messages[0]] == expected_model == ["prior", "current"]
    assert [m.text for entry in provider.state.data.conversation_history for m in entry.messages] == [
        "prior",
        "current",
        "answer-1",
    ]
    assert agent.context_providers is providers and providers == [compaction, primary]
    assert vars(primary) == primary_before
    assert compaction.before_strategy is (before if before_enabled else None)
    assert compaction.after_strategy is after


async def test_service_owned_durable_primary_does_not_compact_parked_history() -> None:
    calls: list[str] = []

    async def after(messages: list[Message]) -> bool:
        calls.append("after")
        messages.clear()
        return True

    history = DurableHistoryProvider("durable")
    compaction = CompactionProvider(after_strategy=after, history_source_id="durable")
    agent = Agent(client=_PassiveChatClient(), context_providers=[history, compaction])
    session = AgentSession(session_id="parked")
    parked = [Message("user", ["retained"])]
    session.state["durable"] = {"messages": parked}
    prepared = prepare_history_owner(agent, True)
    assert isinstance(prepared, Agent)
    prepared_compaction = next(p for p in prepared.context_providers if isinstance(p, CompactionProvider))
    await prepared_compaction.after_run(
        agent=prepared, session=session, context=SessionContext(input_messages=[]), state={}
    )
    assert calls == []
    assert [m.text for m in parked] == ["retained"]


@pytest.mark.parametrize("location", ["message", "content", "nested"])
def test_forward_compatible_fields_participate_in_ingestion_identity(location: str) -> None:
    raw = Message("user", ["same"], message_id="public").to_dict()
    if location == "message":
        target = raw
    elif location == "content":
        target = raw["contents"][0]
    else:
        raw["contents"] = [
            {
                "type": "function_approval_request",
                "id": "approval",
                "function_call": {
                    "type": "function_call",
                    "call_id": "call",
                    "name": "lookup",
                    "arguments": "{}",
                },
            }
        ]
        target = raw["contents"][0]["function_call"]
    target["future"] = {"revision": 1}
    first = DurableAgentStateMessage.from_core_dict(raw)
    raw_before = deepcopy(raw)
    target["future"]["revision"] = 2
    second = DurableAgentStateMessage.from_core_dict(raw)
    assert first.ingestion_identity != second.ingestion_identity
    loaded = load_agent_response({"messages": [raw_before]}).messages[0]
    preserve_input_envelope(loaded, raw_before)
    assert first.ingestion_identity == message_identity(loaded)


def test_known_input_defaults_keep_existing_fingerprint() -> None:
    raw: dict[str, Any] = {"role": "user", "contents": [{"type": "text", "text": "same"}]}
    loaded = load_agent_response({"messages": [raw]}).messages[0]
    expected = message_identity(loaded)
    assert DurableAgentStateMessage.from_core_dict(raw).ingestion_identity == expected
