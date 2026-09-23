# Copyright (c) Microsoft. All rights reserved.

"""Ensure-only audit compaction must not depend on later per-run preparation."""

from copy import deepcopy
from typing import Any

import pytest
from agent_framework import (
    GROUP_ANNOTATION_KEY,
    GROUP_ID_KEY,
    SUMMARIZED_BY_SUMMARY_ID_KEY,
    SUMMARY_OF_MESSAGE_IDS_KEY,
    Agent,
    CompactionProvider,
    Message,
)
from test_private_history_pipeline import ToolChatClient
from test_shared_history_provider import OrdinaryExternalHistory, _bound, _CanonicalStateProvider, _request, _stored

from agent_framework_durabletask._history_provider import (
    WORKING_BUFFER_KEY,
    DurableHistoryProvider,
    ensure_durable_history,
    prepare_history_owner,
)
from agent_framework_durabletask._shared_agent_state import DurableAgentStateCompaction

SHARED_ID = "shared-public-id"
SESSION_ID = "external-audit-compaction"


class _ExternalPrimary(OrdinaryExternalHistory):
    def __init__(self, events: list[str]) -> None:
        super().__init__("external")
        self.events = events
        self.saved = [
            Message("user", ["external prior"], message_id=SHARED_ID),
            Message("assistant", ["external reply"], message_id=SHARED_ID),
        ]

    async def before_run(self, **kwargs: Any) -> None:
        self.events.append("primary-before")
        await super().before_run(**kwargs)

    async def after_run(self, **kwargs: Any) -> None:
        self.events.append("primary-after")
        await super().after_run(**kwargs)


class _HookedCompaction(CompactionProvider):
    def __init__(self, events: list[str], **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.events = events

    async def before_run(self, **kwargs: Any) -> None:
        self.events.append("compaction-before")
        await super().before_run(**kwargs)

    async def after_run(self, **kwargs: Any) -> None:
        self.events.append("compaction-after")
        await super().after_run(**kwargs)


def _audit() -> DurableHistoryProvider:
    audit = DurableHistoryProvider("audit", skip_excluded=False)
    audit.load_messages = False
    return audit


@pytest.mark.parametrize("audit_first", [False, True], ids=["primary-first", "audit-first"])
@pytest.mark.parametrize("nested_links", [False, True], ids=["top-level-links", "core-group-links"])
@pytest.mark.parametrize("already_prepared", [False, True], ids=["ensure-only", "per-run-wrapped-control"])
async def test_ensure_alone_adapts_audit_compaction_and_preserves_collision_lineage(
    audit_first: bool, nested_links: bool, already_prepared: bool
) -> None:
    events: list[str] = []
    seen_ids: list[list[str | None]] = []
    seen_texts: list[list[str]] = []

    async def summarize(messages: list[Message]) -> bool:
        seen_ids.append([message.message_id for message in messages])
        seen_texts.append([message.text for message in messages])
        sources = [message for message in messages if message.text in ("audit second", "audit anonymous")]
        assert len(sources) == 2
        for source in sources:
            source.additional_properties["_excluded"] = True
            links = (
                source.additional_properties.setdefault(GROUP_ANNOTATION_KEY, {})
                if nested_links
                else source.additional_properties
            )
            links[SUMMARIZED_BY_SUMMARY_ID_KEY] = SHARED_ID
        summary_links = {SUMMARY_OF_MESSAGE_IDS_KEY: [message.message_id for message in sources]}
        properties: dict[str, Any] = (
            {GROUP_ANNOTATION_KEY: {GROUP_ID_KEY: f"group_{SHARED_ID}", **summary_links}}
            if nested_links
            else summary_links
        )
        # Deliberately collide with an unrelated original's public AND canonical ID.
        messages.insert(
            messages.index(sources[-1]) + 1,
            Message("assistant", ["audit summary"], message_id=SHARED_ID, additional_properties=properties),
        )
        return True

    primary = _ExternalPrimary(events)
    audit = _audit()
    compaction = _HookedCompaction(events, after_strategy=summarize, history_source_id=audit.source_id)
    client = ToolChatClient(tool_calls=False, response_message_id=SHARED_ID, events=events)
    # Reverse Core after hooks must save the audit before compacting it. Moving
    # the primary across that pair must not reorder any configured providers.
    ordered = [compaction, audit, primary] if audit_first else [primary, compaction, audit]
    agent = Agent(client=client, context_providers=ordered)
    original_providers = agent.context_providers
    candidate = agent
    if already_prepared:
        # An explicit control, not part of the ensure-only reproducer. Run it
        # outside a binding so external-primary observation is not introduced.
        per_run = prepare_history_owner(agent, False)
        assert isinstance(per_run, Agent)
        assert per_run is not agent
        candidate = per_run
    candidate_providers = candidate.context_providers
    prepared = ensure_durable_history(candidate)
    assert isinstance(prepared, Agent)
    if already_prepared:
        assert prepared is candidate
        assert prepared.context_providers is candidate_providers
    prepared_providers = prepared.context_providers
    assert ensure_durable_history(prepared) is prepared
    assert prepared.context_providers is prepared_providers
    assert [provider.source_id for provider in prepared_providers] == [provider.source_id for provider in ordered]
    assert next(provider for provider in prepared_providers if provider.source_id == "external") is primary
    assert next(provider for provider in prepared_providers if provider.source_id == "audit") is audit

    owner = _CanonicalStateProvider([
        _request(
            "seed",
            _stored("audit first", message_id=SHARED_ID),
            _stored("audit second", message_id=SHARED_ID),
            _stored("audit anonymous"),
        )
    ])
    session = prepared.create_session(session_id=SESSION_ID)
    session.state["unrelated"] = {"keep": [False, 0, None]}
    current = Message("user", ["current input"], message_id=SHARED_ID)
    original_input = deepcopy(current.to_dict())
    expected_texts = ["audit first", "audit second", "audit anonymous", "current input", "answer-1"]
    expected_public_ids = [SHARED_ID, SHARED_ID, None, SHARED_ID, SHARED_ID]

    with _bound(owner) as binding:
        # No downstream preparation, activity, or entity can repair the candidate.
        response = await prepared.run(current, session=session)
        original_response = deepcopy(response.to_dict())
        stored_before = [message for entry in owner.state.data.conversation_history for message in entry.messages]
        canonical_ids = [message.message_id for message in stored_before]
        assert [message.text for message in stored_before] == expected_texts
        assert [message.public_message_id for message in stored_before] == expected_public_ids
        assert len(set(canonical_ids)) == 5 and all(canonical_ids)
        assert seen_texts == [expected_texts]
        # Core can invent IDs for anonymous messages but leaves duplicates intact.
        # Neither public duplicates nor Core-generated IDs identify durable occurrences.
        assert seen_ids == [canonical_ids]
        working = session.state[audit.source_id][WORKING_BUFFER_KEY]
        assert [message.message_id for message in working if message.text != "audit summary"] == expected_public_ids
        assert binding.append_ordinal == 2
        assert not any(
            isinstance(entry, DurableAgentStateCompaction) for entry in owner.state.data.conversation_history
        )

        # The final flush is real. No later save is allowed to hide its absence.
        audit.flush(session.state[audit.source_id])
        stored = [message for entry in owner.state.data.conversation_history for message in entry.messages]
        assert [message.text for message in stored] == [*expected_texts[:3], "audit summary", *expected_texts[3:]]
        assert len({message.message_id for message in stored}) == 6
        summaries = [
            entry for entry in owner.state.data.conversation_history if isinstance(entry, DurableAgentStateCompaction)
        ]
        assert len(summaries) == 1 and len(summaries[0].messages) == 1
        summary = summaries[0].messages[0]
        assert summary.message_id and summary.message_id not in canonical_ids
        assert summary.public_message_id == summary.message_id
        for message in stored:
            properties = message.extension_data or {}
            links = properties.get(GROUP_ANNOTATION_KEY, {}) if nested_links else properties
            if message.text == "audit summary":
                assert links[SUMMARY_OF_MESSAGE_IDS_KEY] == canonical_ids[1:3]
                if nested_links:
                    assert links[GROUP_ID_KEY] == f"group_{summary.message_id}"
            elif message.text in ("audit second", "audit anonymous"):
                assert properties["_excluded"] is True
                assert links[SUMMARIZED_BY_SUMMARY_ID_KEY] == summary.message_id
            else:
                assert not properties.get("_excluded")
                assert SUMMARIZED_BY_SUMMARY_ID_KEY not in links
        originals = [message for message in stored if message.text != "audit summary"]
        assert [message.message_id for message in originals] == canonical_ids
        assert [message.public_message_id for message in originals] == expected_public_ids
        snapshot = deepcopy(owner.state.to_dict())
        assert binding.append_ordinal == 3
        audit.flush(session.state[audit.source_id])
        assert owner.state.to_dict() == snapshot and binding.append_ordinal == 3
        assert response.to_dict() == original_response

    cold = owner.clone()
    cold_state: dict[str, Any] = {}
    with _bound(cold):
        replay = await DurableHistoryProvider(skip_excluded=True).get_messages(SESSION_ID, state=cold_state)
        assert [message.text for message in replay] == ["audit first", "audit summary", "current input", "answer-1"]
        assert [message.message_id for message in replay] == [SHARED_ID, summary.message_id, SHARED_ID, SHARED_ID]
        audit.flush(cold_state)
    assert cold.state.to_dict() == snapshot
    assert owner.persist_count == cold.persist_count == 0
    assert current.to_dict() == original_input
    assert response.messages[0].message_id == SHARED_ID
    assert session.state["unrelated"] == {"keep": [False, 0, None]}
    assert agent.context_providers is original_providers and original_providers == ordered
    assert primary.load_messages is True and audit.load_messages is False
    assert compaction.after_strategy is summarize and compaction.history_source_id == audit.source_id
    assert primary.calls == [("load", SESSION_ID), ("save", SESSION_ID)]
    assert [message.text for message in primary.saved] == [
        "external prior",
        "external reply",
        "current input",
        "answer-1",
    ]
    assert all(message.message_id == SHARED_ID for message in primary.saved)
    assert [[message.text for message in batch] for batch in client.received_messages] == [
        ["external prior", "external reply", "current input"]
    ]
    assert events == (
        ["compaction-before", "primary-before", "model-1", "primary-after", "compaction-after"]
        if audit_first
        else ["primary-before", "compaction-before", "model-1", "compaction-after", "primary-after"]
    )


@pytest.mark.parametrize("with_audit", [False, True], ids=["external-only-noop", "external-with-audit"])
async def test_external_target_compaction_remains_unwrapped_with_original_hooks(with_audit: bool) -> None:
    events: list[str] = []
    before_seen: list[list[str | None]] = []
    after_seen: list[list[str | None]] = []

    async def before(messages: list[Message]) -> bool:
        before_seen.append([message.message_id for message in messages])
        return False

    async def after(messages: list[Message]) -> bool:
        after_seen.append([message.message_id for message in messages])
        messages[0].additional_properties["external_compacted"] = True
        return True

    primary = _ExternalPrimary(events)
    audit = _audit()
    compaction = _HookedCompaction(
        events, before_strategy=before, after_strategy=after, history_source_id=primary.source_id
    )
    client = ToolChatClient(tool_calls=False, response_message_id=SHARED_ID, events=events)
    agent = Agent(client=client, context_providers=[primary, compaction, *([audit] if with_audit else [])])
    providers = agent.context_providers
    owner = _CanonicalStateProvider([_request("seed", _stored("parked audit", message_id="parked"))])
    original_history = deepcopy(owner.state.to_dict())
    session = agent.create_session(session_id=SESSION_ID)
    # Core after compaction targets a session-state slice, not the external store.
    cached = Message("user", ["external cache"], message_id=SHARED_ID)
    session.state[primary.source_id] = {WORKING_BUFFER_KEY: [cached]}

    with _bound(owner):
        prepared = ensure_durable_history(agent)
        assert prepared is agent and ensure_durable_history(prepared) is agent
        assert agent.context_providers is providers
        assert providers[0] is primary and providers[1] is compaction
        await prepared.run(Message("user", ["current input"], message_id=SHARED_ID), session=session)
        if with_audit:
            assert providers[2] is audit
            audit.flush(session.state[audit.source_id])
            stored = [message for entry in owner.state.data.conversation_history for message in entry.messages]
            assert [message.text for message in stored] == ["parked audit", "current input", "answer-1"]
            assert all("external_compacted" not in (message.extension_data or {}) for message in stored)
        else:
            assert owner.state.to_dict() == original_history

    assert before_seen == [[SHARED_ID, SHARED_ID]]
    assert after_seen == [[SHARED_ID]]
    assert cached.message_id == SHARED_ID and cached.additional_properties["external_compacted"] is True
    assert compaction.before_strategy is before and compaction.after_strategy is after
    assert primary.calls == [("load", SESSION_ID), ("save", SESSION_ID)]
    assert [message.text for message in primary.saved] == [
        "external prior",
        "external reply",
        "current input",
        "answer-1",
    ]
    assert all("external_compacted" not in message.additional_properties for message in primary.saved)
    assert events == ["primary-before", "compaction-before", "model-1", "compaction-after", "primary-after"]
    assert [[message.text for message in batch] for batch in client.received_messages] == [
        ["external prior", "external reply", "current input"]
    ]
    assert owner.persist_count == 0
