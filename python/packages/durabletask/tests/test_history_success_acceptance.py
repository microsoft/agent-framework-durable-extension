# Copyright (c) Microsoft. All rights reserved.

"""Successful client-owned delivery receipts are independent of transcript retention."""

from collections.abc import Awaitable, Callable, Sequence
from types import MethodType
from typing import Any

import pytest
from agent_framework import Agent, AgentContext, AgentMiddleware, HistoryProvider, Message, SessionContext
from test_private_history_pipeline import ToolChatClient, _bound, _CanonicalStateProvider, lookup

from agent_framework_durabletask._history_provider import (
    DurableHistoryProvider,
    current_durable_history_binding,
    prepare_history_owner,
)
from agent_framework_durabletask._shared_agent_state import DurableAgentStateRequest

PRIOR = ("occ-prior", "fingerprint-prior")
ACCEPTED = ("occ-B", "fingerprint-B")


def _input(text: str) -> Message:
    message = Message("user", [text])
    message._durable_ingestion_receipt = (f"occ-{text}", f"fingerprint-{text}")  # type: ignore[attr-defined]
    return message


class _DropA(AgentMiddleware):
    async def process(self, context: AgentContext, call_next: Callable[[], Awaitable[None]]) -> None:
        context.messages = [message for message in context.messages if message.text != "A"]
        await call_next()


class _ExternalHistory(HistoryProvider):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__("external", **kwargs)
        self.saved: list[Message] = []
        self.save_calls = 0
        self.load_calls = 0

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        self.load_calls += 1
        return []

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.save_calls += 1
        self.saved.extend(messages)


def _history(kind: str, *, store_inputs: bool, store_outputs: bool = True) -> HistoryProvider:
    provider_type = DurableHistoryProvider if kind == "durable" else _ExternalHistory
    return provider_type(store_inputs=store_inputs, store_outputs=store_outputs)


def _stored_texts(history: HistoryProvider, owner: _CanonicalStateProvider) -> list[str]:
    if isinstance(history, _ExternalHistory):
        assert owner.state.data.conversation_history == []
        return [message.text for message in history.saved]
    return [message.text for entry in owner.state.data.conversation_history for message in entry.messages]


@pytest.mark.parametrize("kind", ["durable", "external"])
@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-call"])
@pytest.mark.parametrize("store_inputs", [False, True])
@pytest.mark.parametrize("store_outputs", [False, True])
@pytest.mark.parametrize("stream", [False, True])
async def test_success_accepts_only_post_middleware_inputs_without_extra_writes(
    kind: str, per_call: bool, store_inputs: bool, store_outputs: bool, stream: bool
) -> None:
    owner = _CanonicalStateProvider()
    history = _history(kind, store_inputs=store_inputs, store_outputs=store_outputs)
    client = ToolChatClient(tool_calls=False)
    agent = Agent(
        client=client,
        context_providers=[history],
        middleware=[_DropA()],
        require_per_service_call_history_persistence=per_call,
    )
    inputs = [_input("A"), _input("B")]
    with _bound(owner) as binding:
        binding.accepted_inputs.add(PRIOR)
        prepared = prepare_history_owner(agent, False)
        session = prepared.create_session()
        if stream:
            response = await prepared.run(inputs, session=session, stream=True).get_final_response()
        else:
            response = await prepared.run(inputs, session=session)

        assert response.text == "answer-1"
        assert [[message.text for message in batch] for batch in client.received_messages] == [["B"]]
        assert binding.accepted_inputs == {PRIOR, ACCEPTED}
        assert binding.pending_inputs == []

    assert [message.text for message in inputs] == ["A", "B"]
    assert _stored_texts(history, owner) == (["B"] if store_inputs else []) + (["answer-1"] if store_outputs else [])
    if isinstance(history, _ExternalHistory):
        assert history.save_calls == int(store_inputs or store_outputs)
    else:
        assert len(owner.state.data.conversation_history) == int(store_inputs) + int(store_outputs)
    assert owner.persist_count == 0


@pytest.mark.parametrize("kind", ["durable", "external"])
@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-call"])
@pytest.mark.parametrize("store_inputs", [False, True])
@pytest.mark.parametrize("failure_point", ["model", "save"])
async def test_failed_model_or_save_does_not_accept_inputs(
    kind: str, per_call: bool, store_inputs: bool, failure_point: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner = _CanonicalStateProvider()
    history = _history(kind, store_inputs=store_inputs)
    save_attempts: list[list[str]] = []

    async def fail_save(session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        binding = current_durable_history_binding()
        assert binding is not None and binding.accepted_inputs == {PRIOR}
        save_attempts.append([message.text for message in messages])
        raise RuntimeError("save failed")

    if failure_point == "save":
        monkeypatch.setattr(history, "save_messages", fail_save)
    client = ToolChatClient(tool_calls=False, fail=failure_point == "model")
    agent = Agent(
        client=client,
        context_providers=[history],
        middleware=[_DropA()],
        require_per_service_call_history_persistence=per_call,
    )
    with _bound(owner) as binding:
        binding.accepted_inputs.add(PRIOR)
        prepared = prepare_history_owner(agent, False)
        with pytest.raises(RuntimeError, match=f"{failure_point} failed"):
            await prepared.run([_input("A"), _input("B")], session=prepared.create_session())
        assert binding.accepted_inputs == {PRIOR}

    assert [[message.text for message in batch] for batch in client.received_messages] == [["B"]]
    assert bool(save_attempts) is (failure_point == "save")
    assert _stored_texts(history, owner) == []


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-call"])
async def test_durable_output_save_failure_preserves_successful_input_save(
    per_call: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    owner = _CanonicalStateProvider()
    history = DurableHistoryProvider()
    save = history.save_messages

    async def fail_output_save(session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        binding = current_durable_history_binding()
        assert binding is not None and binding.accepted_inputs == {PRIOR}
        if messages[0].role == "assistant":
            raise RuntimeError("output save failed")
        await save(session_id, messages, **kwargs)

    monkeypatch.setattr(history, "save_messages", fail_output_save)
    agent = Agent(
        client=ToolChatClient(tool_calls=False),
        context_providers=[history],
        middleware=[_DropA()],
        require_per_service_call_history_persistence=per_call,
    )
    with _bound(owner) as binding:
        binding.accepted_inputs.add(PRIOR)
        with pytest.raises(RuntimeError, match="output save failed"):
            await agent.run([_input("A"), _input("B")], session=agent.create_session())
        assert binding.accepted_inputs == {PRIOR}
        assert binding.append_response is None

    assert _stored_texts(history, owner) == ["B"]
    assert {
        (message.ingestion_occurrence, message.ingestion_identity)
        for entry in owner.state.data.conversation_history
        if isinstance(entry, DurableAgentStateRequest)
        for message in entry.messages
    } == {ACCEPTED}


@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-call"])
@pytest.mark.parametrize("explicit_accept", [False, True])
@pytest.mark.parametrize("fail_after_save", [False, True])
async def test_opaque_hook_requires_explicit_acceptance_and_preserves_it_on_failure(
    per_call: bool, explicit_accept: bool, fail_after_save: bool
) -> None:
    owner = _CanonicalStateProvider()
    history = _ExternalHistory(store_inputs=False, store_outputs=False)

    async def custom_after_run(self: _ExternalHistory, *, context: SessionContext, **kwargs: Any) -> None:
        assert [message.text for message in context.input_messages] == ["B", "C"]
        selected = context.input_messages[:1]
        await self.save_messages(context.session_id, selected, state=kwargs["state"])
        binding = current_durable_history_binding()
        assert binding is not None and binding.accepted_inputs == {PRIOR}
        if explicit_accept:
            binding.accept(selected)
        if fail_after_save:
            raise RuntimeError("partial save failed")

    history.after_run = MethodType(custom_after_run, history)  # type: ignore[method-assign]
    client = ToolChatClient(tool_calls=False)
    agent = Agent(
        client=client,
        context_providers=[history],
        middleware=[_DropA()],
        require_per_service_call_history_persistence=per_call,
    )
    with _bound(owner) as binding:
        binding.accepted_inputs.add(PRIOR)
        prepared = prepare_history_owner(agent, False)
        inputs = [_input("A"), _input("B"), _input("C")]
        if fail_after_save:
            with pytest.raises(RuntimeError, match="partial save failed"):
                await prepared.run(inputs, session=prepared.create_session())
        else:
            await prepared.run(inputs, session=prepared.create_session())
        assert binding.accepted_inputs == ({PRIOR, ACCEPTED} if explicit_accept else {PRIOR})

    assert [[message.text for message in batch] for batch in client.received_messages] == [["B", "C"]]
    assert _stored_texts(history, owner) == ["B"]
    assert history.save_calls == 1


@pytest.mark.parametrize("kind", ["durable", "external"])
@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-call"])
@pytest.mark.parametrize("store_inputs", [False, True])
async def test_service_owned_history_hooks_do_not_accept_or_store_inputs(
    kind: str, per_call: bool, store_inputs: bool
) -> None:
    owner = _CanonicalStateProvider()
    history = _history(kind, store_inputs=store_inputs)
    client = ToolChatClient(tool_calls=False)
    agent = Agent(
        client=client,
        default_options={"store": True},
        context_providers=[history],
        middleware=[_DropA()],
        require_per_service_call_history_persistence=per_call,
    )
    with _bound(owner, service_owns_history=True) as binding:
        binding.accepted_inputs.add(PRIOR)
        prepared = prepare_history_owner(agent, True)
        response = await prepared.run([_input("A"), _input("B")], session=prepared.create_session())
        assert response.text == "answer-1"
        assert binding.accepted_inputs == {PRIOR}
        assert binding.pending_inputs == []

    assert [[message.text for message in batch] for batch in client.received_messages] == [["B"]]
    assert _stored_texts(history, owner) == []
    if isinstance(history, _ExternalHistory):
        assert history.load_calls == history.save_calls == 0


@pytest.mark.parametrize("kind", ["durable", "external"])
@pytest.mark.parametrize("per_call", [False, True], ids=["per-run", "per-call"])
@pytest.mark.parametrize("store_inputs", [False, True])
async def test_successful_per_call_receipts_survive_a_later_model_failure(
    kind: str, per_call: bool, store_inputs: bool
) -> None:
    owner = _CanonicalStateProvider()
    history = _history(kind, store_inputs=store_inputs)
    client = ToolChatClient(fail_on_call=2)
    agent = Agent(
        client=client,
        tools=[lookup],
        context_providers=[history],
        middleware=[_DropA()],
        require_per_service_call_history_persistence=per_call,
    )
    with _bound(owner) as binding:
        binding.accepted_inputs.add(PRIOR)
        prepared = prepare_history_owner(agent, False)
        with pytest.raises(RuntimeError, match="model failed"):
            await prepared.run([_input("A"), _input("B")], session=prepared.create_session())
        assert binding.accepted_inputs == ({PRIOR, ACCEPTED} if per_call else {PRIOR})

    assert len(client.received_messages) == 2
    assert [message.text for message in client.received_messages[0]] == ["B"]
    assert _stored_texts(history, owner) == ((["B"] if store_inputs else []) + [""] if per_call else [])
