# Copyright (c) Microsoft. All rights reserved.

"""Primary receipts, canonical audit snapshots and real Core cold delivery boundaries."""

from collections.abc import Sequence
from copy import deepcopy
from typing import Any

import pytest
from _execution_test_support import JsonStateProvider, NonStreamingAgent, ToolChatClient
from agent_framework import (
    AgentSession,
    ContextProvider,
    HistoryProvider,
    InMemoryHistoryProvider,
    Message,
    SessionContext,
)

from agent_framework_durabletask import AgentEntity, DurableAgentState, DurableHistoryProvider
from agent_framework_durabletask._history_provider import current_durable_history_binding
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._shared_agent_state import DurableAgentStateRequest


class _ExternalPrimary(HistoryProvider):
    def __init__(self, failure: str | None = None) -> None:
        super().__init__("primary")
        self.failure = failure
        self.saved: list[Message] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        return deepcopy(self.saved)

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.saved.extend(deepcopy(list(messages)))

    async def after_run(self, *, context: SessionContext, state: dict[str, Any], **kwargs: Any) -> None:
        if self.failure == "before-accept":
            raise RuntimeError("primary failed before acceptance")
        selected = [message for message in context.input_messages if message.text == "A"]
        await self.save_messages(context.session_id, selected, state=state)
        binding = current_durable_history_binding()
        assert binding is not None
        binding.accept(selected)
        if self.failure == "after-accept":
            raise RuntimeError("primary failed after acceptance")


class _CustomInMemoryPrimary(InMemoryHistoryProvider):
    def __init__(self, failure: str | None = None) -> None:
        super().__init__("primary")
        self.failure = failure

    async def after_run(self, *, context: SessionContext, state: dict[str, Any], **kwargs: Any) -> None:
        if self.failure == "before-accept":
            raise RuntimeError("primary failed before acceptance")
        selected = [message for message in context.input_messages if message.text == "A"]
        await self.save_messages(context.session_id, selected, state=state)
        binding = current_durable_history_binding()
        assert binding is not None
        binding.accept(selected)
        if self.failure == "after-accept":
            raise RuntimeError("primary failed after acceptance")


class _Probe(ContextProvider):
    def __init__(self) -> None:
        super().__init__("probe")
        self.inputs: list[list[str]] = []
        self.accepted: set[tuple[str, str]] = set()
        self.staged: list[tuple[str | None, str | None, str | None]] = []
        self.annotations: list[str] = []

    async def before_run(self, *, context: SessionContext, **kwargs: Any) -> None:
        self.inputs.append([message.text for message in context.input_messages])
        context.extend_messages(self, [Message("system", ["audit context"], message_id="context")])

    async def after_run(self, *, session: AgentSession, **kwargs: Any) -> None:
        binding = current_durable_history_binding()
        assert binding is not None
        self.accepted = set(binding.accepted_inputs)
        self.staged = [
            (message.public_message_id, message.ingestion_occurrence, message.ingestion_identity)
            for entry in binding.state_provider.state.data.conversation_history
            if isinstance(entry, DurableAgentStateRequest) and entry.correlation_id == binding.correlation_id
            for message in entry.messages
        ]
        for message in session.state.get("audit", {}).get("messages", []):
            self.annotations.append(message.text)
            message.additional_properties["after_audit"] = {"keep": [False, 0, None]}


def _request(correlation: str, *, explicit_occurrences: bool = True, store: bool = False) -> dict[str, Any]:
    request: dict[str, Any] = {
        "message": "contextMessages are authoritative",
        "correlationId": correlation,
        "contextMessages": [Message("user", [key], message_id=key).to_dict() for key in ("A", "B")],
        "options": {"store": store},
    }
    if explicit_occurrences:
        request["contextMessageIds"] = ["occ-A", "occ-B"]
    return request


def _receipts(*keys: str, explicit_occurrences: bool = True) -> dict[str, list[str]]:
    return {
        f"occ-{key}" if explicit_occurrences else key: [message_identity(Message("user", [key], message_id=key))]
        for key in keys
    }


def _build(
    provider: JsonStateProvider,
    primary: HistoryProvider,
    *,
    audit_first: bool,
    per_call: bool,
    writes: str = "both",
) -> tuple[AgentEntity, ToolChatClient, _Probe, DurableHistoryProvider]:
    audit = DurableHistoryProvider(
        "audit",
        store_inputs=writes in ("both", "inputs"),
        store_outputs=writes in ("both", "outputs"),
        store_context_messages=writes == "context",
        store_context_from={"probe"},
    )
    audit.load_messages = False
    probe = _Probe()
    client = ToolChatClient(tool_calls=False, response_message_id="public-answer")
    ordered = [audit, primary] if audit_first else [primary, audit]
    agent = NonStreamingAgent(
        client=client,
        name="audit-receipts",
        context_providers=[probe, *ordered],
        require_per_service_call_history_persistence=per_call,
    )
    # Exercise actual admission and preparation. Do not bypass the old guard.
    return AgentEntity(agent, state_provider=provider), client, probe, audit


def _committed(provider: JsonStateProvider) -> DurableAgentState:
    return DurableAgentState.from_dict(deepcopy(provider.raw))


@pytest.mark.parametrize("primary_type", [_ExternalPrimary, _CustomInMemoryPrimary])
@pytest.mark.parametrize("audit_first", [False, True])
@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-call"])
@pytest.mark.parametrize("writes", ["both", "inputs", "outputs", "context", "none"])
@pytest.mark.parametrize("explicit_occurrences", [False, True], ids=["public-id", "occurrence-id"])
async def test_canonical_audit_keeps_exact_transcript_but_only_primary_subset_is_consumed_after_cold_start(
    primary_type: type[_ExternalPrimary] | type[_CustomInMemoryPrimary],
    audit_first: bool,
    per_call: bool,
    writes: str,
    explicit_occurrences: bool,
) -> None:
    primary = primary_type()
    store = JsonStateProvider()
    entity, client, probe, audit = _build(store, primary, audit_first=audit_first, per_call=per_call, writes=writes)
    response = await entity.run(_request("first", explicit_occurrences=explicit_occurrences))
    assert response.text == "answer-1"
    expected_receipts = _receipts("A", explicit_occurrences=explicit_occurrences)
    assert probe.accepted == {(key, value[0]) for key, value in expected_receipts.items()}
    assert probe.inputs == [["A", "B"]]
    assert [message.text for message in client.received_messages[0]] == ["audit context", "A", "B"]
    assert _committed(store).data.ingested_messages == expected_receipts
    assert store.successful_writes == 1
    request_texts = ["audit context"] if writes == "context" else []
    request_ids = ["context"] if writes == "context" else []
    if writes in ("both", "inputs"):
        request_texts += ["A", "B"]
        request_ids += ["A", "B"]
    output_texts = ["answer-1"] if writes in ("both", "outputs") else []
    history = _committed(store).data.conversation_history
    assert [[message.text for message in entry.messages] for entry in history] == [
        *([request_texts] if request_texts else []),
        *([output_texts] if output_texts else []),
    ]
    assert [public_id for public_id, _, _ in probe.staged] == request_ids
    assert all(occurrence is None and fingerprint for _, occurrence, fingerprint in probe.staged)
    # Semantic/public identity equality, not stored-object identity across retention or JSON copies.
    assert [message.public_message_id for entry in history for message in entry.messages] == [
        *request_ids,
        *(["public-answer"] if output_texts else []),
    ]
    assert probe.annotations == [*request_texts, *output_texts]
    for entry in history:
        for message in entry.messages:
            assert (message.extension_data or {})["after_audit"] == {"keep": [False, 0, None]}
    assert primary.load_messages is True and audit.load_messages is False
    session = _committed(store).data.session
    assert session is not None
    assert "messages" not in session["state"].get("audit", {})
    assert "_positions" not in session["state"].get("audit", {})

    # Rebuild the entity, session and audit from committed JSON. The external
    # primary retains its independent store, the custom in-memory primary restores
    # its accepted A through the normal persisted Core session.
    cold = JsonStateProvider(deepcopy(store.raw))
    cold_primary = primary if isinstance(primary, _ExternalPrimary) else _CustomInMemoryPrimary()
    cold_entity, cold_client, cold_probe, _ = _build(
        cold, cold_primary, audit_first=audit_first, per_call=per_call, writes=writes
    )
    assert (await cold_entity.run(_request("second", explicit_occurrences=explicit_occurrences))).text == "answer-1"
    assert cold_probe.inputs == [["B"]]
    assert [message.text for message in cold_client.received_messages[0]].count("B") == 1
    assert [message.text for message in cold_client.received_messages[0]].count("A") == 1
    assert _committed(cold).data.ingested_messages == expected_receipts
    assert cold.successful_writes == 1
    before_duplicate = deepcopy(cold.raw)
    await cold_entity.run(_request("second", explicit_occurrences=explicit_occurrences))
    assert cold.raw == before_duplicate and cold.successful_writes == 1
    assert len(cold_client.received_messages) == 1


@pytest.mark.parametrize("primary_type", [_ExternalPrimary, _CustomInMemoryPrimary])
@pytest.mark.parametrize("audit_first", [False, True])
@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-call"])
@pytest.mark.parametrize("failure", ["before-accept", "after-accept"])
async def test_failed_primary_before_or_after_audit_leaves_unaccepted_inputs_for_cold_redelivery(
    primary_type: type[_ExternalPrimary] | type[_CustomInMemoryPrimary],
    audit_first: bool,
    per_call: bool,
    failure: str,
) -> None:
    primary = primary_type(failure)
    store = JsonStateProvider()
    entity, _, _, _ = _build(store, primary, audit_first=audit_first, per_call=per_call)
    response = await entity.run(_request("failed"))
    assert response.additional_properties["durable_status"] == "error"
    expected = _receipts("A") if failure == "after-accept" else {}
    assert _committed(store).data.ingested_messages == expected
    assert [message.text for entry in _committed(store).data.conversation_history for message in entry.messages] == (
        [] if audit_first else ["A", "B", "answer-1"]
    )
    cold = JsonStateProvider(deepcopy(store.raw))
    primary.failure = None
    cold_primary = primary if isinstance(primary, _ExternalPrimary) else _CustomInMemoryPrimary()
    cold_entity, _, cold_probe, _ = _build(cold, cold_primary, audit_first=audit_first, per_call=per_call)
    assert (await cold_entity.run(_request("recovery"))).text == "answer-1"
    assert cold_probe.inputs == ([["B"]] if failure == "after-accept" else [["A", "B"]])
    assert _committed(cold).data.ingested_messages == _receipts("A")


class _OutputSaveFailure(DurableHistoryProvider):
    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        if messages[0].role == "assistant":
            raise RuntimeError("output save failed after primary input save")
        await super().save_messages(session_id, messages, **kwargs)


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-call"])
@pytest.mark.parametrize("store_inputs", [False, True])
async def test_durable_primary_partial_input_save_remains_receipt_evidence(per_call: bool, store_inputs: bool) -> None:
    store = JsonStateProvider()
    history = _OutputSaveFailure("primary", store_inputs=store_inputs)
    probe = _Probe()
    agent = NonStreamingAgent(
        client=ToolChatClient(tool_calls=False),
        context_providers=[probe, history],
        require_per_service_call_history_persistence=per_call,
    )
    response = await AgentEntity(agent, state_provider=store).run(_request("partial"))
    assert response.additional_properties["durable_status"] == "error"
    expected = _receipts("A", "B") if store_inputs else {}
    assert _committed(store).data.ingested_messages == expected
    assert [message.text for entry in _committed(store).data.conversation_history for message in entry.messages] == (
        ["A", "B"] if store_inputs else []
    )
    cold = JsonStateProvider(deepcopy(store.raw))
    cold_probe = _Probe()
    cold_agent = NonStreamingAgent(
        client=ToolChatClient(tool_calls=False),
        context_providers=[cold_probe, DurableHistoryProvider("primary", store_inputs=store_inputs)],
        require_per_service_call_history_persistence=per_call,
    )
    assert (await AgentEntity(cold_agent, state_provider=cold).run(_request("recovery"))).text == "answer-1"
    assert cold_probe.inputs == ([[]] if store_inputs else [["A", "B"]])
    assert _committed(cold).data.ingested_messages == _receipts("A", "B")


@pytest.mark.parametrize("primary_type", [_ExternalPrimary, _CustomInMemoryPrimary])
@pytest.mark.parametrize("audit_first", [False, True])
@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-call"])
async def test_service_true_false_true_cold_runs_do_not_promote_audit_to_receipt_owner(
    primary_type: type[_ExternalPrimary] | type[_CustomInMemoryPrimary], audit_first: bool, per_call: bool
) -> None:
    primary = primary_type()
    store = JsonStateProvider()
    expected: dict[str, list[str]] = {}
    for turn, service_owned in enumerate((True, False, True), start=1):
        if turn > 1:
            store = JsonStateProvider(deepcopy(store.raw))
        entity, _, probe, audit = _build(store, primary, audit_first=audit_first, per_call=per_call)
        request = _request(f"turn-{turn}", store=service_owned)
        request["contextMessageIds"] = [f"turn-{turn}-A", f"turn-{turn}-B"]
        assert (await entity.run(request)).text == "answer-1"
        accepted_keys = ("A", "B") if service_owned else ("A",)
        expected.update({f"turn-{turn}-{key}": _receipts(key)[f"occ-{key}"] for key in accepted_keys})
        assert _committed(store).data.ingested_messages == expected
        assert probe.inputs == [["A", "B"]]
        assert [
            message.text for entry in _committed(store).data.conversation_history for message in entry.messages
        ] == ([] if turn == 1 else ["A", "B", "answer-1"])
        assert primary.load_messages is True and audit.load_messages is False
