# Copyright (c) Microsoft. All rights reserved.

"""Canonical audits preserve transcripts without broadening primary acceptance."""

from collections.abc import Callable, Sequence
from typing import Any

import pytest
from _history_pipeline_test_support import AddContext, ToolChatClient
from _shared_history_test_support import _bound, _CanonicalStateProvider, _request, _stored
from agent_framework import (
    Agent,
    AgentSession,
    ContextProvider,
    HistoryProvider,
    InMemoryHistoryProvider,
    Message,
    SessionContext,
    SupportsAgentRun,
)

from agent_framework_durabletask._history_provider import (
    WORKING_BUFFER_KEY,
    DurableHistoryProvider,
    current_durable_history_binding,
    ensure_durable_history,
    prepare_history_owner,
    validate_history_providers,
)
from agent_framework_durabletask._shared_agent_state import DurableAgentStateRequest

PRIOR = ("prior-occurrence", "a" * 64)
SELECTED = ("selected-occurrence", "b" * 64)


def _inputs() -> list[Message]:
    messages = [
        Message("user", ["selected"], message_id="selected-occurrence"),
        Message("user", ["unselected"], message_id="unselected-occurrence"),
    ]
    for message, receipt in zip(messages, [SELECTED, ("unselected-occurrence", "c" * 64)]):
        message._durable_ingestion_receipt = receipt  # type: ignore[attr-defined]
    return messages


class _ExternalPrimary(HistoryProvider):
    def __init__(self, failure: str | None = None) -> None:
        super().__init__("primary")
        self.failure = failure
        self.calls: list[str] = []
        self.saved: list[Message] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        self.calls.append("load")
        return []

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.calls.append("save")
        self.saved.extend(messages)

    async def after_run(self, *, context: SessionContext, state: dict[str, Any], **kwargs: Any) -> None:
        self.calls.append("after")
        if self.failure == "before-accept":
            raise RuntimeError("primary failed before acceptance")
        selected = [message for message in context.input_messages if message.text == "selected"]
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
        self.calls: list[str] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        self.calls.append("load")
        return await super().get_messages(session_id, **kwargs)

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.calls.append("save")
        await super().save_messages(session_id, messages, **kwargs)

    async def after_run(self, *, context: SessionContext, state: dict[str, Any], **kwargs: Any) -> None:
        self.calls.append("after")
        if self.failure == "before-accept":
            raise RuntimeError("primary failed before acceptance")
        selected = [message for message in context.input_messages if message.text == "selected"]
        await self.save_messages(context.session_id, selected, state=state)
        binding = current_durable_history_binding()
        assert binding is not None
        binding.accept(selected)
        if self.failure == "after-accept":
            raise RuntimeError("primary failed after acceptance")


class _DurableAudit(DurableHistoryProvider):
    def __init__(self, writes: str) -> None:
        super().__init__(
            "audit",
            store_inputs=writes in ("both", "inputs"),
            store_outputs=writes in ("both", "outputs"),
            store_context_messages=writes == "context",
            store_context_from={"extra"},
        )
        self.load_messages = False
        self.calls: list[str] = []

    async def before_run(self, **kwargs: Any) -> None:
        self.calls.append("before")
        await super().before_run(**kwargs)

    async def after_run(self, **kwargs: Any) -> None:
        self.calls.append("after")
        await super().after_run(**kwargs)

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.calls.append("save")
        await super().save_messages(session_id, messages, **kwargs)


class _OrdinaryAudit(HistoryProvider):
    def __init__(self) -> None:
        super().__init__("audit", load_messages=False)
        self.saved: list[Message] = []
        self.save_calls = 0

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        raise AssertionError("A store-only audit must not load history")

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.save_calls += 1
        self.saved.extend(messages)


class _FinalAnnotations(ContextProvider):
    def __init__(self) -> None:
        super().__init__("final-annotations")
        self.seen: list[str] = []

    async def after_run(self, *, session: AgentSession, **kwargs: Any) -> None:
        for message in session.state.get("audit", {}).get(WORKING_BUFFER_KEY, []):
            self.seen.append(message.text)
            message.additional_properties["after_audit"] = {"keep": [False, 0, None]}


@pytest.mark.parametrize(
    ("conflict", "diagnostic"),
    [
        ("two-audits", "only one DurableHistoryProvider"),
        ("two-primaries", "only one load-enabled primary"),
        ("duplicate-source", "unique source_id"),
    ],
)
def test_single_audit_compatibility_does_not_relax_other_admission_guards(conflict: str, diagnostic: str) -> None:
    primary = _ExternalPrimary()
    audit = _DurableAudit("none")
    providers: list[HistoryProvider] = [primary, audit]
    if conflict == "two-audits":
        second = _DurableAudit("none")
        second.source_id = "second-audit"
        providers.append(second)
    elif conflict == "two-primaries":
        audit.load_messages = True
    else:
        audit.source_id = primary.source_id
    client = ToolChatClient(tool_calls=False)
    agent = Agent(client=client, context_providers=providers)
    with pytest.raises(ValueError, match=diagnostic):
        ensure_durable_history(agent)
    assert primary.calls == audit.calls == []
    assert client.received_messages == []


@pytest.mark.parametrize("primary_type", [_ExternalPrimary, _CustomInMemoryPrimary])
@pytest.mark.parametrize("audit_first", [False, True])
@pytest.mark.parametrize("writes", ["both", "inputs", "outputs", "context", "none"])
@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-call"])
@pytest.mark.parametrize("admit", [validate_history_providers, ensure_durable_history])
async def test_durable_audit_is_admitted_and_keeps_exact_appends_and_final_annotations(
    primary_type: type[_ExternalPrimary] | type[_CustomInMemoryPrimary],
    audit_first: bool,
    writes: str,
    per_call: bool,
    admit: Callable[[SupportsAgentRun], object],
) -> None:
    primary = primary_type()
    audit = _DurableAudit(writes)
    annotations = _FinalAnnotations()
    client = ToolChatClient(tool_calls=False, response_message_id="public-answer")
    ordered = [audit, primary] if audit_first else [primary, audit]
    agent = Agent(
        client=client,
        context_providers=[annotations, *ordered, AddContext("extra")],
        require_per_service_call_history_persistence=per_call,
    )
    providers = agent.context_providers
    owner = _CanonicalStateProvider([_request("seed", _stored("parked", message_id="seed"))])
    session = agent.create_session()
    session.state["unrelated"] = {"retained": [1]}
    inputs = _inputs()
    before_inputs = [message.to_dict() for message in inputs]
    expected_requests = ["context-extra"] if writes == "context" else []
    if writes in ("both", "inputs"):
        expected_requests += ["selected", "unselected"]
    expected_outputs = ["answer-1"] if writes in ("both", "outputs") else []

    with _bound(owner) as binding:
        binding.accepted_inputs.add(PRIOR)
        admit(agent)
        assert ensure_durable_history(agent) is agent
        prepared = prepare_history_owner(agent, False)
        response = await prepared.run(inputs, session=session)
        assert response.text == "answer-1"
        assert binding.accepted_inputs == {PRIOR, SELECTED}
        assert binding.pending_inputs == []
        assert binding.append_ordinal == int(bool(expected_requests)) + int(bool(expected_outputs))
        assert binding.append_response is None
        # No later after hook has another save to hide a missing final flush.
        assert all(
            "after_audit" not in (message.extension_data or {})
            for entry in owner.state.data.conversation_history
            for message in entry.messages
        )
        audit.flush(session.state["audit"])
        assert binding.accepted_inputs == {PRIOR, SELECTED}

    assert primary.calls == ["load", "after", "save"]
    assert audit.calls == ["after", *(["save"] * (int(bool(expected_requests)) + int(bool(expected_outputs))))]
    assert [[message.text for message in batch] for batch in client.received_messages] == [
        ["context-extra", "selected", "unselected"]
    ]
    history = owner.state.data.conversation_history
    assert [[message.text for message in entry.messages] for entry in history] == [
        ["parked"],
        *([expected_requests] if expected_requests else []),
        *([expected_outputs] if expected_outputs else []),
    ]
    stored = [message for entry in history for message in entry.messages]
    assert all(message.ingestion_occurrence is None for message in stored)
    assert all(message.ingestion_identity for entry in history[1:] for message in entry.messages)
    # Audit public IDs may equal occurrence IDs without becoming receipt evidence.
    assert [message.public_message_id for message in stored] == [
        "seed",
        *([None] if writes == "context" else []),
        *(["selected-occurrence", "unselected-occurrence"] if writes in ("both", "inputs") else []),
        *(["public-answer"] if expected_outputs else []),
    ]
    assert annotations.seen == (["parked", *expected_requests, *expected_outputs] if writes != "none" else [])
    for message in stored:
        assert (message.extension_data or {}).get("after_audit") == (
            {"keep": [False, 0, None]} if writes != "none" else None
        )
    assert owner.clone().state.to_dict() == owner.state.to_dict()
    assert owner.persist_count == 0
    assert session.state["unrelated"] == {"retained": [1]}
    assert agent.context_providers is providers
    assert primary.load_messages is True and audit.load_messages is False
    assert [message.to_dict() for message in inputs] == before_inputs


@pytest.mark.parametrize("primary_type", [_ExternalPrimary, _CustomInMemoryPrimary])
@pytest.mark.parametrize("audit_first", [False, True])
@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-call"])
@pytest.mark.parametrize("failure", ["model", "before-accept", "after-accept"])
async def test_failure_before_or_after_audit_never_accepts_its_unselected_snapshot(
    primary_type: type[_ExternalPrimary] | type[_CustomInMemoryPrimary],
    audit_first: bool,
    per_call: bool,
    failure: str,
) -> None:
    primary = primary_type(failure)
    audit = _DurableAudit("both")
    client = ToolChatClient(tool_calls=False, fail=failure == "model")
    agent = Agent(
        client=client,
        context_providers=[audit, primary] if audit_first else [primary, audit],
        require_per_service_call_history_persistence=per_call,
    )
    ensure_durable_history(agent)
    owner = _CanonicalStateProvider()
    session = agent.create_session()
    with _bound(owner) as binding:
        binding.accepted_inputs.add(PRIOR)
        prepared = prepare_history_owner(agent, False)
        with pytest.raises(RuntimeError, match="failed"):
            await prepared.run(_inputs(), session=session)
        audit.flush(session.state.get("audit", {}))
        assert binding.accepted_inputs == ({PRIOR, SELECTED} if failure == "after-accept" else {PRIOR})
        assert binding.append_response is None
        assert {
            (stored.ingestion_occurrence, stored.ingestion_identity)
            for entry in owner.state.data.conversation_history
            if isinstance(entry, DurableAgentStateRequest)
            for stored in entry.messages
            if stored.ingestion_occurrence
        } == set()
    saved = failure != "model" and not audit_first
    assert [message.text for entry in owner.state.data.conversation_history for message in entry.messages] == (
        ["selected", "unselected", "answer-1"] if saved else []
    )
    assert owner.persist_count == 0


@pytest.mark.parametrize("primary_type", [_ExternalPrimary, _CustomInMemoryPrimary])
@pytest.mark.parametrize("audit_first", [False, True])
@pytest.mark.parametrize("writes", ["both", "inputs", "outputs", "context", "none"])
@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-call"])
async def test_durable_audit_service_true_false_true_preserves_configuration_and_primary_authority(
    primary_type: type[_ExternalPrimary] | type[_CustomInMemoryPrimary],
    audit_first: bool,
    writes: str,
    per_call: bool,
) -> None:
    primary = primary_type()
    audit = _DurableAudit(writes)
    client = ToolChatClient(tool_calls=False)
    agent = Agent(
        client=client,
        context_providers=[audit, primary] if audit_first else [primary, audit],
        require_per_service_call_history_persistence=per_call,
    )
    assert ensure_durable_history(agent) is agent
    owner = _CanonicalStateProvider([_request("seed", _stored("parked", message_id="seed"))])
    expected = ["parked"]
    for turn, service_owned in enumerate((True, False, True), start=1):
        session = agent.create_session()
        with _bound(owner, service_owns_history=service_owned) as binding:
            prepared = prepare_history_owner(agent, service_owned)
            assert isinstance(prepared, Agent)
            await prepared.run(_inputs(), session=session, options={"store": service_owned})
            audit.flush(session.state.get("audit", {}))
            assert binding.accepted_inputs == (set() if service_owned else {SELECTED})
        if not service_owned:
            if writes in ("both", "inputs"):
                expected += ["selected", "unselected"]
            if writes in ("both", "outputs"):
                expected += ["answer-2"]
        assert [
            message.text for entry in owner.state.data.conversation_history for message in entry.messages
        ] == expected
        assert primary.calls == ([] if turn == 1 else ["load", "after", "save"])
        assert primary.load_messages is True and audit.load_messages is False
        assert owner.persist_count == 0


@pytest.mark.parametrize("primary_type", [_ExternalPrimary, _CustomInMemoryPrimary])
@pytest.mark.parametrize("audit_first", [False, True])
@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-call"])
async def test_ordinary_audit_preserves_primary_subset_and_service_ownership_transitions(
    primary_type: type[_ExternalPrimary] | type[_CustomInMemoryPrimary], audit_first: bool, per_call: bool
) -> None:
    primary = primary_type()
    audit = _OrdinaryAudit()
    client = ToolChatClient(tool_calls=False)
    agent = Agent(
        client=client,
        context_providers=[audit, primary] if audit_first else [primary, audit],
        require_per_service_call_history_persistence=per_call,
    )
    assert ensure_durable_history(agent) is agent
    providers = agent.context_providers
    owner = _CanonicalStateProvider([_request("seed", _stored("parked", message_id="seed"))])
    before = owner.state.to_dict()

    for turn, service_owned in enumerate((True, False, True), start=1):
        # A fresh session keeps this a provider-ownership test, not continuation
        # migration. The same configured providers serve all three runs.
        session = agent.create_session()
        with _bound(owner, service_owns_history=service_owned) as binding:
            binding.accepted_inputs.add(PRIOR)
            prepared = prepare_history_owner(agent, service_owned)
            assert isinstance(prepared, Agent)
            response = await prepared.run(_inputs(), session=session, options={"store": service_owned})
            assert response.text == f"answer-{turn}"
            assert binding.accepted_inputs == ({PRIOR} if service_owned else {PRIOR, SELECTED})
            assert binding.pending_inputs == []
        assert primary.calls == ([] if turn == 1 else ["load", "after", "save"])
        assert owner.state.to_dict() == before
        assert owner.persist_count == 0
        assert [message.text for message in client.received_messages[-1]] == ["selected", "unselected"]
        if isinstance(primary, _CustomInMemoryPrimary):
            stored = session.state.get("primary", {}).get("messages", [])
            assert [message.text for message in stored] == ([] if service_owned else ["selected"])

    assert audit.save_calls == 3
    assert [message.text for message in audit.saved] == [
        "selected",
        "unselected",
        "answer-1",
        "selected",
        "unselected",
        "answer-2",
        "selected",
        "unselected",
        "answer-3",
    ]
    if isinstance(primary, _ExternalPrimary):
        assert [message.text for message in primary.saved] == ["selected"]
    assert agent.context_providers is providers


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-call"])
async def test_single_explicit_nonwriting_durable_primary_remains_available_for_client_owned_run(
    per_call: bool,
) -> None:
    history = DurableHistoryProvider("primary", store_inputs=False, store_outputs=False)
    client = ToolChatClient(tool_calls=False)
    agent = Agent(
        client=client,
        context_providers=[history],
        require_per_service_call_history_persistence=per_call,
    )
    prepared_agent = ensure_durable_history(agent)
    assert isinstance(prepared_agent, Agent)
    # Retention may return an isolated provider copy to apply its default policy.
    # This control asserts the history contract, not preparation object identity.
    assert len(prepared_agent.context_providers) == 1
    prepared_history = prepared_agent.context_providers[0]
    assert isinstance(prepared_history, DurableHistoryProvider)
    assert prepared_history.source_id == "primary" and prepared_history.load_messages
    assert not prepared_history.store_inputs and not prepared_history.store_outputs
    owner = _CanonicalStateProvider([_request("seed", _stored("parked", message_id="seed"))])
    before = owner.state.to_dict()
    for service_owned in (True, False, True):
        with _bound(owner, service_owns_history=service_owned) as binding:
            binding.accepted_inputs.add(PRIOR)
            prepared = prepare_history_owner(prepared_agent, service_owned)
            assert isinstance(prepared, Agent)
            await prepared.run(_inputs()[:1], session=prepared.create_session(), options={"store": service_owned})
            assert binding.accepted_inputs == ({PRIOR} if service_owned else {PRIOR, SELECTED})
        expected = ["selected"] if service_owned else ["parked", "selected"]
        assert [message.text for message in client.received_messages[-1]] == expected
        assert owner.state.to_dict() == before
        assert owner.persist_count == 0
    assert agent.context_providers == [history]
