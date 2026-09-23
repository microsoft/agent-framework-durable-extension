# Copyright (c) Microsoft. All rights reserved.

"""Private provider boundaries without host activation or storage commits."""

from copy import deepcopy
from typing import Any

import pytest
from _shared_history_test_support import (
    OrdinaryExternalHistory,
    _bound,
    _CanonicalStateProvider,
    _PassiveChatClient,
    _request,
    _stored,
)
from agent_framework import Agent, Content, ContextProvider, Message, SessionContext

from agent_framework_durabletask._history_provider import (
    POSITIONS_KEY,
    WORKING_BUFFER_KEY,
    DurableHistoryProvider,
    prepare_history_owner,
)
from agent_framework_durabletask._shared_agent_state import DurableAgentStateMessage, DurableAgentStateResponse


@pytest.mark.parametrize("append", [False, True])
async def test_full_owner_replacement_drops_only_stale_working_references(append: bool) -> None:
    provider = _CanonicalStateProvider([_request("old", _stored("old", message_id="old"))])
    history = DurableHistoryProvider()
    working: dict[str, Any] = {}
    with _bound(provider):
        await history.get_messages("session", state=working)
        replacement = _request("replacement", _stored("new", message_id="new"))
        provider.state.data.conversation_history = [replacement]
        expected = deepcopy(provider.state.to_dict())
        history.flush(working)
        assert working[WORKING_BUFFER_KEY] == []
        assert working[POSITIONS_KEY] == {}
        if append:
            await history.save_messages(
                "session", [Message("user", ["appended"], message_id="appended")], state=working
            )
            assert set(working[POSITIONS_KEY]) == {"appended"}
            expected["data"]["conversationHistory"].append(provider.state.data.conversation_history[-1].to_dict())
        history.flush(working)
        assert provider.state.to_dict() == expected
        assert provider.state.data.conversation_history[0] is replacement
        assert provider.persist_count == 0


async def test_inactive_custom_primary_hooks_are_silent_but_other_providers_run() -> None:
    events: list[str] = []

    class HookedHistory(OrdinaryExternalHistory):
        async def before_run(self, **kwargs: Any) -> None:
            events.append(f"{self.source_id}-before")
            await super().before_run(**kwargs)

        async def after_run(self, **kwargs: Any) -> None:
            events.append(f"{self.source_id}-after")
            await super().after_run(**kwargs)

    class AlwaysContext(ContextProvider):
        async def before_run(self, *, context: SessionContext, **kwargs: Any) -> None:
            events.append("context-before")

        async def after_run(self, *, context: SessionContext, **kwargs: Any) -> None:
            events.append("context-after")

    primary = HookedHistory("primary")
    audit = HookedHistory("audit", load_messages=False)
    context = AlwaysContext("context")
    agent = Agent(client=_PassiveChatClient(), context_providers=[primary, audit, context])
    original_providers = agent.context_providers
    session = agent.create_session(session_id="external")
    for service_owned in (True, False, True):
        events.clear()
        prepared = prepare_history_owner(agent, service_owned)
        assert isinstance(prepared, Agent)
        await prepared.run("input", session=session, options={"store": service_owned})
        assert ("primary-before" in events) is not service_owned
        assert ("primary-after" in events) is not service_owned
        # Core does not invoke load-disabled sinks' before hook.
        assert "audit-before" not in events
        assert {"audit-after", "context-before", "context-after"} <= set(events)
        assert prepared.context_providers[1] is audit
        assert prepared.context_providers[2] is context
    assert primary.calls == [("load", "external"), ("save", "external")]
    assert agent.context_providers is original_providers
    assert agent.context_providers[0] is primary


async def test_failure_finalization_keeps_unflushed_removal_evidence() -> None:
    removed = _stored("old", message_id="old")
    call = DurableAgentStateMessage.from_chat_message(
        Message("assistant", [Content.from_function_call(call_id="call", name="lookup")], message_id="call-message")
    )
    provider = _CanonicalStateProvider([
        _request("old", removed),
        DurableAgentStateResponse("current", None, [call]),
    ])
    history = DurableHistoryProvider()
    working: dict[str, Any] = {}
    with _bound(provider) as binding:
        await history.get_messages("session", state=working)
        working[WORKING_BUFFER_KEY].pop(0)
        binding.pending_inputs = [Message("tool", [Content.from_function_result(call_id="call", result="done")])]
        history.finalize_failed_run(working)
        history.flush(working)
        assert (removed.extension_data or {}).get("_excluded") is True
        reloaded = await history.get_messages("session", state={})
        assert all(message.message_id != "old" for message in reloaded)
        assert any(content.type == "function_result" for message in reloaded for content in message.contents)
        assert provider.state.data.conversation_history[0].messages[0] is removed
