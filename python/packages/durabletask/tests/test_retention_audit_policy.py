# Copyright (c) Microsoft. All rights reserved.

"""Canonical audit retention is independent of primary history acceptance."""

import hashlib
import json
from collections.abc import Sequence
from copy import copy, deepcopy
from datetime import datetime, timezone
from typing import Any

import pytest
from _execution_test_support import JsonStateProvider, NonStreamingAgent, ToolChatClient
from agent_framework import Agent, CompactionProvider, HistoryProvider, Message, SessionContext

from agent_framework_durabletask import AgentEntity
from agent_framework_durabletask._history_provider import (
    DurableHistoryProvider,
    current_durable_history_binding,
    ensure_durable_history,
)
from agent_framework_durabletask._models import RunRequest
from agent_framework_durabletask._retention import RetentionMode

OLD = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


class _ExternalPrimary(HistoryProvider):
    def __init__(self, *, load_messages: bool = True) -> None:
        super().__init__("external", load_messages=load_messages)
        self.calls: list[tuple[str, str | None]] = []
        self.saved = [
            Message("user", ["old question"], message_id="old-user"),
            Message("assistant", ["old answer"], message_id="old-assistant"),
        ]

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        self.calls.append(("load", session_id))
        return deepcopy(self.saved)

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.calls.append(("save", session_id))
        self.saved.extend(deepcopy(list(messages)))

    async def after_run(self, *, context: SessionContext, state: dict[str, Any], **kwargs: Any) -> None:
        assert [message.text for message in context.input_messages] == ["selected", "unselected"]
        selected = [message for message in context.input_messages if message.text == "selected"]
        await self.save_messages(context.session_id, selected, state=state)
        binding = current_durable_history_binding()
        assert binding is not None
        binding.accept(selected)


@pytest.mark.parametrize("load_messages", [False, True], ids=["audit", "primary"])
@pytest.mark.parametrize("explicit", [None, False, True], ids=["inherited", "pinned-off", "pinned-on"])
@pytest.mark.parametrize("canonical_first", [False, True])
def test_canonical_policy_repreparation_preserves_pins_order_and_caller_objects(
    load_messages: bool, explicit: bool | None, canonical_first: bool
) -> None:
    history = DurableHistoryProvider(
        "canonical",
        store_inputs=False,
        store_outputs=True,
        store_context_messages=True,
        store_context_from={"selected"},
        skip_excluded=False,
        prune_excluded=explicit,
    )
    history.load_messages = load_messages
    external = _ExternalPrimary(load_messages=not load_messages)
    ordered = [history, external] if canonical_first else [external, history]
    original = Agent(client=ToolChatClient(tool_calls=False), context_providers=ordered)
    original_providers = original.context_providers
    prepared = original
    current = history
    for policy in (False, True, False):
        prior, prior_policy = current, current.prune_excluded
        updated = ensure_durable_history(prepared, prune_excluded=policy)
        assert isinstance(updated, Agent)
        canonical = [provider for provider in updated.context_providers if isinstance(provider, DurableHistoryProvider)]
        assert len(canonical) == 1
        current = canonical[0]
        expected = explicit if explicit is not None else policy
        # None already means non-deleting. Preserve existing audit no-op identity.
        if not load_messages and explicit is None and prior_policy is None and not policy:
            assert current.prune_excluded is None and updated is prepared
        else:
            assert current.prune_excluded is expected
        if explicit is not None:
            assert current is history and updated is original
        if current is not prior:
            assert current.store_context_from is not prior.store_context_from
        assert prior.prune_excluded is prior_policy
        assert current.load_messages is load_messages
        assert current.store_inputs is False and current.store_outputs is True
        assert current.store_context_messages is True and current.skip_excluded is False
        assert current.store_context_from == history.store_context_from == {"selected"}
        assert [provider.source_id for provider in updated.context_providers] == [p.source_id for p in ordered]
        assert next(p for p in updated.context_providers if p.source_id == "external") is external
        assert ensure_durable_history(updated, prune_excluded=policy) is updated
        prepared = updated
    assert original.context_providers is original_providers
    assert all(actual is expected for actual, expected in zip(original.context_providers, ordered, strict=True))
    assert history.prune_excluded is explicit and external.calls == []


@pytest.mark.parametrize("initial_loading", [False, True], ids=["audit-to-primary", "primary-to-audit"])
@pytest.mark.parametrize("initial_policy", [False, True])
def test_inherited_policy_can_change_after_the_caller_moves_the_adapter_between_roles(
    initial_loading: bool, initial_policy: bool
) -> None:
    history = DurableHistoryProvider("canonical", store_context_from={"selected"})
    history.load_messages = initial_loading
    original = Agent(
        client=ToolChatClient(tool_calls=False),
        context_providers=[_ExternalPrimary(load_messages=not initial_loading), history],
    )
    first = ensure_durable_history(original, prune_excluded=initial_policy)
    assert isinstance(first, Agent)
    inherited = next(p for p in first.context_providers if isinstance(p, DurableHistoryProvider))
    before = inherited.prune_excluded
    # A caller changes the role on its own copy, not on an existing prepared agent.
    moved = copy(inherited)
    moved.load_messages = not initial_loading
    reassigned = Agent(
        client=ToolChatClient(tool_calls=False),
        context_providers=[moved, _ExternalPrimary(load_messages=initial_loading)],
    )
    result = ensure_durable_history(reassigned, prune_excluded=not initial_policy)
    assert isinstance(result, Agent)
    active = next(p for p in result.context_providers if isinstance(p, DurableHistoryProvider))
    assert active.prune_excluded is (not initial_policy)
    assert active.load_messages is (not initial_loading)
    assert active is not moved and active.store_context_from is not moved.store_context_from
    assert moved.prune_excluded is before and inherited.prune_excluded is before
    assert inherited.load_messages is initial_loading and history.prune_excluded is None


def _seed() -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    for kind, correlation, role, message_id, contents in (
        ("request", "system", "system", "policy", [{"$type": "text", "text": "keep policy"}]),
        ("request", "old", "user", "old-user", [{"$type": "text", "text": "old question"}]),
        ("response", "old", "assistant", "old-assistant", [{"$type": "text", "text": "old answer"}]),
        (
            "response",
            "pending",
            "assistant",
            "pending-call",
            [{"$type": "functionCall", "callId": "pending-tool", "name": "lookup", "arguments": "{}"}],
        ),
    ):
        entries.append({
            "$type": kind,
            "correlationId": correlation,
            "createdAt": OLD.isoformat(),
            "messages": [{"role": role, "messageId": message_id, "contents": contents}],
        })
    return {
        "schemaVersion": "2.0.0",
        "data": {
            "conversationHistory": entries,
            "terminalResults": {},
            "completionReceipts": {},
            "pythonIngestion": {
                "profile": "agent-framework-python.ingestion",
                "version": 1,
                "messages": {"prior-occurrence": ["a" * 64]},
            },
            "session": {
                "session_id": "@history-findings@retention-probe",
                "state": {"unrelated": {"values": [False, 0, None]}},
            },
            "extensionData": {"opaque": [False, 0, None]},
        },
    }


@pytest.mark.parametrize(
    ("explicit", "retention", "service_owned", "expected_policy", "removed"),
    [
        pytest.param(None, "follow_compaction", False, True, 2, id="unset-follow"),
        pytest.param(True, "follow_compaction", False, True, 2, id="pinned-on-control"),
        pytest.param(False, "follow_compaction", False, False, 0, id="pinned-off-control"),
        pytest.param(None, "keep_all", False, None, 0, id="unset-keep-all-control"),
        pytest.param(True, "keep_all", False, True, 2, id="pin-overrides-keep-all"),
        pytest.param(None, "follow_compaction", True, True, 0, id="inactive-service-owned-control"),
    ],
)
async def test_real_entity_audit_inherits_retention_without_accepting_the_primarys_unselected_input(
    explicit: bool | None,
    retention: RetentionMode,
    service_owned: bool,
    expected_policy: bool | None,
    removed: int,
) -> None:
    seed = _seed()
    if service_owned:
        for entry in seed["data"]["conversationHistory"]:
            if entry["correlationId"] == "old":
                entry["messages"][0]["extensionData"] = {"_excluded": True}
    primary = _ExternalPrimary()
    external_before = _json([message.to_dict() for message in primary.saved])
    audit = DurableHistoryProvider("audit", prune_excluded=explicit)
    audit.load_messages = False
    compacted: list[list[str | None]] = []
    all_ids = ["policy", "old-user", "old-assistant", "pending-call"]
    complete_ids = [*all_ids, "selected-public", "unselected-public", "answer-public"]

    async def exclude(messages: list[Message]) -> bool:
        ids = [message.message_id for message in messages]
        assert ids == complete_ids
        assert all(not message.additional_properties.get("_excluded") for message in messages)
        compacted.append(ids)
        for message in messages:
            message.additional_properties["_excluded"] = True
        return True

    compaction = CompactionProvider(after_strategy=exclude, history_source_id="audit")
    client = ToolChatClient(tool_calls=False, response_message_id="answer-public")
    original = NonStreamingAgent(client=client, name="history-findings", context_providers=[primary, compaction, audit])
    original_providers = original.context_providers
    prepared = ensure_durable_history(original, prune_excluded=retention == "follow_compaction")
    assert isinstance(prepared, Agent) and prepared.context_providers[0] is primary
    prepared_audit = next(p for p in prepared.context_providers if isinstance(p, DurableHistoryProvider))
    store = JsonStateProvider(seed, session_id="retention-probe", entity_name="history-findings")
    entity = AgentEntity(prepared, state_provider=store, retention=retention, max_state_bytes=None)
    assert isinstance(entity.agent, Agent)
    active_audit = next(p for p in entity.agent.context_providers if isinstance(p, DurableHistoryProvider))
    assert active_audit.load_messages is False and entity._max_state_bytes is None
    assert _json(store.state.to_dict()) == _json(seed)
    # Public envelopes plus stdlib SHA provide an independent receipt oracle.
    inputs = [
        {
            "type": "message",
            "role": "user",
            "message_id": f"{text}-public",
            "contents": [{"type": "text", "text": text, "additional_properties": {}}],
            "additional_properties": {},
        }
        for text in ("selected", "unselected")
    ]
    request = RunRequest(
        "unused when context is supplied",
        "current",
        created_at=OLD,
        context_messages=deepcopy(inputs),
        context_message_ids=["selected-occurrence", "unselected-occurrence"],
        options={"store": service_owned},
    )
    request_before = _json(request.to_dict())
    response = await entity.run(request)
    assert response.text == "answer-1" and response.additional_properties.get("durable_status") != "error"
    assert store.successful_writes == store.attempted_writes == 1
    assert all(
        message.ingestion_occurrence is None
        for entry in store.state.data.conversation_history
        for message in entry.messages
    ), "The audit cannot create primary acceptance markers"
    assert [[message.text for message in batch] for batch in client.received_messages] == [
        ["selected", "unselected"] if service_owned else ["old question", "old answer", "selected", "unselected"]
    ]
    assert _json(request.to_dict()) == request_before
    assert original.context_providers is original_providers
    assert all(a is b for a, b in zip(original_providers, [primary, compaction, audit], strict=True))
    assert audit.prune_excluded is explicit and audit.load_messages is False
    assert compaction.after_strategy is exclude and compaction.history_source_id == "audit"
    assert len(compacted) == (0 if service_owned else 1)
    assert _json([message.to_dict() for message in primary.saved[:2]]) == external_before
    assert [message.text for message in primary.saved] == (
        ["old question", "old answer"] if service_owned else ["old question", "old answer", "selected"]
    )
    assert primary.calls == (
        []
        if service_owned
        else [("load", "@history-findings@retention-probe"), ("save", "@history-findings@retention-probe")]
    )

    # Reload the complete committed state, not a transcript-only clone.
    cold = JsonStateProvider(json.loads(_json(store.raw)), session_id="retention-probe", entity_name="history-findings")
    assert cold.state is not store.state and _json(cold.state.to_dict()) == _json(store.raw)
    data = cold.state.to_dict()["data"]
    assert data["completionReceipts"]["current"]["outcome"] == "succeeded"
    assert data["terminalResults"]["current"]["outcome"] == "succeeded"
    assert _json(data["session"]["state"]["unrelated"]) == '{"values":[false,0,null]}'
    assert _json(data["extensionData"]) == '{"opaque":[false,0,null]}'
    assert {"messages", "_positions"}.isdisjoint(data["session"]["state"].get("audit", {}))
    receipts = {"prior-occurrence": ["a" * 64]}
    receipts["selected-occurrence"] = [hashlib.sha256(_json(inputs[0]).encode("utf-8")).hexdigest()]
    if service_owned:
        receipts["unselected-occurrence"] = [hashlib.sha256(_json(inputs[1]).encode("utf-8")).hexdigest()]
    assert data["pythonIngestion"]["messages"] == receipts
    rows = [message for entry in data["conversationHistory"] for message in entry["messages"]]
    if service_owned:
        assert _json(data["conversationHistory"]) == _json(seed["data"]["conversationHistory"])
    else:
        assert all(row["extensionData"]["_excluded"] is True for row in rows)
        pending = next(row for row in rows if row["messageId"] == "pending-call")
        assert pending["contents"][0]["callId"] == "pending-tool"
    expected_ids = all_ids if service_owned else [mid for mid in complete_ids if not removed or mid not in all_ids[1:3]]
    assert [row["messageId"] for row in rows] == expected_ids
    assert data.get("truncation", {}).get("evictedMessageCount", 0) == removed
    assert prepared_audit.prune_excluded is expected_policy and active_audit.prune_excluded is expected_policy

    calls_before = deepcopy(primary.calls)
    repeated = await AgentEntity(original, state_provider=cold, retention=retention, max_state_bytes=None).run(request)
    assert _json(repeated.to_dict()) == _json(response.to_dict())
    assert cold.successful_writes == cold.attempted_writes == 0
    assert primary.calls == calls_before and len(client.received_messages) == 1
    assert len(compacted) == (0 if service_owned else 1)
    assert _json(cold.raw) == _json(store.raw)
