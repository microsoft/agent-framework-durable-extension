# Copyright (c) Microsoft. All rights reserved.

"""External/custom primaries must not compete with a canonical durable sink."""

from collections.abc import Callable, Sequence
from copy import deepcopy
from typing import Any

import pytest
from agent_framework import (
    Agent,
    HistoryProvider,
    InMemoryHistoryProvider,
    Message,
    SessionContext,
    SupportsAgentRun,
)
from test_private_history_pipeline import ToolChatClient
from test_shared_history_provider import _bound, _CanonicalStateProvider, _request, _stored

from agent_framework_durabletask._history_provider import (
    DurableHistoryProvider,
    current_durable_history_binding,
    ensure_durable_history,
    prepare_history_owner,
    validate_history_providers,
)

PRIOR = ("prior-occurrence", "a" * 64)
SELECTED = ("selected-occurrence", "b" * 64)


def _inputs() -> list[Message]:
    messages = [Message("user", ["selected"]), Message("user", ["unselected"])]
    for message, receipt in zip(messages, [SELECTED, ("unselected-occurrence", "c" * 64)]):
        message._durable_ingestion_receipt = receipt  # type: ignore[attr-defined]
    return messages


class _ExternalPrimary(HistoryProvider):
    def __init__(self) -> None:
        super().__init__("primary")
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
        selected = [message for message in context.input_messages if message.text == "selected"]
        await self.save_messages(context.session_id, selected, state=state)
        binding = current_durable_history_binding()
        assert binding is not None
        binding.accept(selected)


class _CustomInMemoryPrimary(InMemoryHistoryProvider):
    def __init__(self) -> None:
        super().__init__("primary")
        self.calls: list[str] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        self.calls.append("load")
        return await super().get_messages(session_id, **kwargs)

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.calls.append("save")
        await super().save_messages(session_id, messages, **kwargs)

    async def after_run(self, *, context: SessionContext, state: dict[str, Any], **kwargs: Any) -> None:
        self.calls.append("after")
        selected = [message for message in context.input_messages if message.text == "selected"]
        await self.save_messages(context.session_id, selected, state=state)
        binding = current_durable_history_binding()
        assert binding is not None
        binding.accept(selected)


class _DurableAudit(DurableHistoryProvider):
    def __init__(self, writes: str) -> None:
        super().__init__(
            "audit",
            store_inputs=writes in ("both", "inputs"),
            store_outputs=writes in ("both", "outputs"),
            store_context_messages=writes == "context",
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


@pytest.mark.parametrize("primary_type", [_ExternalPrimary, _CustomInMemoryPrimary])
@pytest.mark.parametrize("audit_first", [False, True])
@pytest.mark.parametrize("writes", ["both", "inputs", "outputs", "context", "none"])
@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-call"])
@pytest.mark.parametrize("admit", [validate_history_providers, ensure_durable_history])
async def test_durable_secondary_rejected_before_any_hook_client_or_state_change(
    primary_type: type[_ExternalPrimary] | type[_CustomInMemoryPrimary],
    audit_first: bool,
    writes: str,
    per_call: bool,
    admit: Callable[[SupportsAgentRun], object],
) -> None:
    primary = primary_type()
    audit = _DurableAudit(writes)
    client = ToolChatClient(tool_calls=False)
    agent = Agent(
        client=client,
        context_providers=[audit, primary] if audit_first else [primary, audit],
        require_per_service_call_history_persistence=per_call,
    )
    providers = agent.context_providers
    settings = [deepcopy(vars(provider)) for provider in providers]
    owner = _CanonicalStateProvider([_request("seed", _stored("parked", message_id="seed"))])
    before = owner.state.to_dict()
    session = agent.create_session()
    session.state["unrelated"] = {"retained": [1]}
    session_before = deepcopy(session.state)
    inputs = _inputs()

    with _bound(owner) as binding:
        binding.accepted_inputs.add(PRIOR)
        # Do not bypass validation to simulate the old implementation. Running
        # this same test against the old source must reach the real Core hooks
        # and fail the expected rejection, not a mocked receipt calculation.
        with pytest.raises(ValueError, match="cannot be a secondary history provider"):
            admit(agent)
            prepared = prepare_history_owner(agent, False)
            await prepared.run(inputs, session=session)
        assert binding.accepted_inputs == {PRIOR}
        assert binding.pending_inputs == []
        assert binding.append_ordinal == 0
        assert binding.append_response is None

    assert primary.calls == audit.calls == []
    assert client.received_messages == client.events == []
    assert owner.state.to_dict() == before
    assert owner.persist_count == 0
    assert session.state == session_before
    assert agent.context_providers is providers
    assert [vars(provider) for provider in providers] == settings
    assert [message.text for message in inputs] == ["selected", "unselected"]


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
    assert ensure_durable_history(agent) is agent
    owner = _CanonicalStateProvider([_request("seed", _stored("parked", message_id="seed"))])
    before = owner.state.to_dict()
    for service_owned in (True, False, True):
        with _bound(owner, service_owns_history=service_owned) as binding:
            binding.accepted_inputs.add(PRIOR)
            prepared = prepare_history_owner(agent, service_owned)
            assert isinstance(prepared, Agent)
            await prepared.run(_inputs()[:1], session=prepared.create_session(), options={"store": service_owned})
            assert binding.accepted_inputs == ({PRIOR} if service_owned else {PRIOR, SELECTED})
        expected = ["selected"] if service_owned else ["parked", "selected"]
        assert [message.text for message in client.received_messages[-1]] == expected
        assert owner.state.to_dict() == before
        assert owner.persist_count == 0
    assert agent.context_providers == [history]
