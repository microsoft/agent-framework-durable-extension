# Copyright (c) Microsoft. All rights reserved.

"""Dispatch real Core history hooks without inventing acceptance for custom hooks."""

from collections.abc import Sequence
from types import MethodType
from typing import Any, cast

import pytest
from _history_pipeline_test_support import ToolChatClient
from _shared_history_test_support import _bound, _CanonicalStateProvider
from agent_framework import Agent, AgentResponse, AgentSession, HistoryProvider, Message, SessionContext

from agent_framework_durabletask._history_provider import (
    _ObservedHistoryProvider,
    current_durable_history_binding,
    prepare_history_owner,
)


class ExternalHistory(HistoryProvider):
    def __init__(self, source_id: str = "external", **kwargs: Any) -> None:
        super().__init__(source_id, **kwargs)
        self.saved: list[Message] = []
        self.states: list[dict[str, Any] | None] = []

    async def get_messages(
        self, session_id: str | None, *, state: dict[str, Any] | None = None, **kwargs: Any
    ) -> list[Message]:
        return []

    async def save_messages(
        self,
        session_id: str | None,
        messages: Sequence[Message],
        *,
        state: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.saved.extend(messages)
        self.states.append(state)


def _context() -> SessionContext:
    first = Message("user", ["first"])
    second = Message("user", ["second"])
    cast(Any, first)._durable_ingestion_receipt = ("first", "fingerprint-1")
    cast(Any, second)._durable_ingestion_receipt = ("second", "fingerprint-2")
    context = SessionContext(session_id="session", input_messages=[first, second])
    context._response = AgentResponse(messages=[Message("assistant", ["answer"])])
    return context


@pytest.mark.parametrize("explicit_base_binding", [False, True])
@pytest.mark.parametrize("store_inputs", [False, True])
async def test_base_hook_keeps_save_observation_and_original_state(
    explicit_base_binding: bool, store_inputs: bool
) -> None:
    provider = ExternalHistory(store_inputs=store_inputs, store_context_messages=True, store_context_from={"selected"})
    if explicit_base_binding:
        cast(Any, provider).after_run = MethodType(HistoryProvider.after_run, provider)
    context = _context()
    context.extend_messages("selected", [Message("developer", ["selected context"])])
    context.extend_messages("ignored", [Message("developer", ["ignored context"])])
    session = AgentSession(session_id="session")
    state: dict[str, Any] = {"existing": ["keep"]}

    with _bound(_CanonicalStateProvider()) as binding:
        await _ObservedHistoryProvider(provider).after_run(
            agent=object(), session=session, context=context, state=state
        )
        # A completed normal hook accepts delivered inputs independently of
        # whether its configured retention policy saves their transcript.
        expected_receipts = {("first", "fingerprint-1"), ("second", "fingerprint-2")}
        assert binding.accepted_inputs == expected_receipts

    assert [message.text for message in provider.saved] == [
        "selected context",
        *(["first", "second"] if store_inputs else []),
        "answer",
    ]
    assert len(provider.states) == 1 and provider.states[0] is state
    assert state == {"existing": ["keep"]}


@pytest.mark.parametrize("binding_kind", ["method", "function", "callable", "inherited"])
@pytest.mark.parametrize("accept_subset", [False, True])
async def test_custom_hook_gets_exact_arguments_and_owns_subset_acceptance(
    binding_kind: str, accept_subset: bool
) -> None:
    calls: list[tuple[Any, ...]] = []

    async def custom_after_run(
        self: ExternalHistory, *, agent: Any, session: AgentSession, context: SessionContext, state: dict[str, Any]
    ) -> None:
        calls.append((self, agent, session, context, state))
        state["custom"] = ["ran"]
        selected = context.input_messages[1:]
        await self.save_messages(context.session_id, selected, state=state)
        if accept_subset:
            binding = current_durable_history_binding()
            assert binding is not None
            binding.accept(selected)

    class CustomBase(ExternalHistory):
        async def after_run(
            self, *, agent: Any, session: AgentSession, context: SessionContext, state: dict[str, Any]
        ) -> None:
            await custom_after_run(self, agent=agent, session=session, context=context, state=state)

    class InheritedCustom(CustomBase):
        pass

    provider = InheritedCustom() if binding_kind == "inherited" else ExternalHistory()

    async def instance_function(
        *, agent: Any, session: AgentSession, context: SessionContext, state: dict[str, Any]
    ) -> None:
        await custom_after_run(provider, agent=agent, session=session, context=context, state=state)

    class CallableHook:
        # Function-like attributes alone must not classify an opaque callable as the base hook.
        def __init__(self) -> None:
            self.__self__ = provider
            self.__func__ = HistoryProvider.after_run

        async def __call__(
            self, *, agent: Any, session: AgentSession, context: SessionContext, state: dict[str, Any]
        ) -> None:
            await instance_function(agent=agent, session=session, context=context, state=state)

    if binding_kind == "method":
        cast(Any, provider).after_run = MethodType(custom_after_run, provider)
    elif binding_kind == "function":
        cast(Any, provider).after_run = instance_function
    elif binding_kind == "callable":
        cast(Any, provider).after_run = CallableHook()
    context = _context()
    session = AgentSession(session_id="session")
    agent = object()
    state: dict[str, Any] = {"existing": ["keep"]}

    with _bound(_CanonicalStateProvider()) as binding:
        await _ObservedHistoryProvider(provider).after_run(agent=agent, session=session, context=context, state=state)
        assert binding.accepted_inputs == ({("second", "fingerprint-2")} if accept_subset else set())

    assert len(calls) == 1
    assert all(actual is expected for actual, expected in zip(calls[0], (provider, agent, session, context, state)))
    assert provider.saved == context.input_messages[1:]
    assert provider.states[0] is state
    assert state == {"existing": ["keep"], "custom": ["ran"]}


async def test_base_hook_bound_to_another_provider_is_delegated_without_observation() -> None:
    provider = ExternalHistory()
    other = ExternalHistory("other", store_outputs=False)
    cast(Any, provider).after_run = MethodType(HistoryProvider.after_run, other)
    context = _context()
    state: dict[str, Any] = {}

    with _bound(_CanonicalStateProvider()) as binding:
        await _ObservedHistoryProvider(provider).after_run(
            agent=object(), session=AgentSession(), context=context, state=state
        )
        assert binding.accepted_inputs == set()

    assert provider.saved == []
    assert other.saved == context.input_messages
    assert other.states[0] is state


async def test_instance_hook_exception_propagates_without_base_save_or_acceptance() -> None:
    provider = ExternalHistory()
    failure = RuntimeError("custom persistence failed")

    async def fail(self: ExternalHistory, **kwargs: Any) -> None:
        assert self is provider
        kwargs["state"]["attempted"] = True
        raise failure

    cast(Any, provider).after_run = MethodType(fail, provider)
    state: dict[str, Any] = {}
    with _bound(_CanonicalStateProvider()) as binding:
        with pytest.raises(RuntimeError, match="custom persistence failed") as caught:
            await _ObservedHistoryProvider(provider).after_run(
                agent=object(), session=AgentSession(), context=_context(), state=state
            )
        assert caught.value is failure
        assert binding.accepted_inputs == set()
    assert state == {"attempted": True}
    assert provider.saved == []


@pytest.mark.parametrize("stream", [False, True])
@pytest.mark.parametrize("per_call", [False, True])
async def test_real_core_pipeline_dispatches_instance_hook(stream: bool, per_call: bool) -> None:
    provider = ExternalHistory()
    calls: list[tuple[Any, ...]] = []

    async def custom_after_run(
        self: ExternalHistory, *, agent: Any, session: AgentSession, context: SessionContext, state: dict[str, Any]
    ) -> None:
        calls.append((self, agent, session, context, state))
        state["custom_runs"] = state.get("custom_runs", 0) + 1
        assert context.response is not None
        await self.save_messages(context.session_id, context.response.messages, state=state)

    cast(Any, provider).after_run = MethodType(custom_after_run, provider)
    agent = Agent(
        client=ToolChatClient(tool_calls=False),
        context_providers=[provider],
        require_per_service_call_history_persistence=per_call,
    )
    session = agent.create_session(session_id="instance-hook")
    session.state[provider.source_id] = {"existing": ["keep"]}
    with _bound(_CanonicalStateProvider()) as binding:
        prepared = prepare_history_owner(agent, False)
        if stream:
            response = await prepared.run("question", session=session, stream=True).get_final_response()
        else:
            response = await prepared.run("question", session=session, stream=False)
        assert response.text == "answer-1"
        assert binding.accepted_inputs == set()

    assert len(calls) == 1
    assert calls[0][0] is provider and calls[0][1] is prepared and calls[0][2] is session
    assert calls[0][4] is session.state[provider.source_id]
    assert session.state[provider.source_id] == {"existing": ["keep"], "custom_runs": 1}
    assert [message.text for message in provider.saved] == ["answer-1"]
    assert agent.context_providers == [provider]
