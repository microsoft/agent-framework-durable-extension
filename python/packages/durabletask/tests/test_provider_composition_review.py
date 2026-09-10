# Copyright (c) Microsoft. All rights reserved.

"""Provider ownership, namespace and atomic reconciliation regressions against Core."""

import json
from copy import deepcopy
from itertools import combinations
from typing import Any

import pytest
from agent_framework import (
    Agent,
    AgentSession,
    Content,
    ContextProvider,
    HistoryProvider,
    InMemoryHistoryProvider,
    Message,
    SessionContext,
)
from test_durable_history_provider import _InMemoryStateProvider
from test_history_pipeline_revision import OLD, AddContext, ToolChatClient, bound, ids, seed, stored, transcript

from agent_framework_durabletask import DurableHistoryProvider
from agent_framework_durabletask import _history_provider as history_module
from agent_framework_durabletask._durable_agent_state import (
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
)
from agent_framework_durabletask._history_provider import WORKING_BUFFER_KEY, ensure_durable_history


class OrdinaryExternalHistory(HistoryProvider):
    """Blind append storage, deliberately unaware of service ownership."""

    def __init__(self, source_id: str = "external", **kwargs: Any) -> None:
        super().__init__(source_id, **kwargs)
        self.saved: list[Message] = []
        self.calls: list[tuple[str, str | None]] = []
        self.resource = object()
        self.lifecycle: list[str] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        self.calls.append(("load", session_id))
        return deepcopy(self.saved)

    async def save_messages(self, session_id: str | None, messages: Any, **kwargs: Any) -> None:
        self.calls.append(("save", session_id))
        self.saved.extend(deepcopy(list(messages)))

    async def __aenter__(self) -> "OrdinaryExternalHistory":
        self.lifecycle.append("enter")
        return self

    async def __aexit__(self, *args: Any) -> None:
        self.lifecycle.append("exit")


def prepare_owner(agent: Any, service_owned: bool) -> Any:
    prepare = getattr(history_module, "prepare_history_owner", None)
    assert callable(prepare), "Durable must provide per-run ownership for ordinary external providers"
    return prepare(agent, service_owns_history=service_owned)


@pytest.mark.parametrize("per_call", [False, True])
@pytest.mark.parametrize("stream", [False, True])
async def test_external_service_branches_and_sink_choices_survive_cold_session_reload(
    per_call: bool, stream: bool
) -> None:
    primary = OrdinaryExternalHistory(store_context_messages=True, store_context_from={"selected"})
    sink = InMemoryHistoryProvider("audit", load_messages=False, store_inputs=False, store_outputs=True)
    client = ToolChatClient(tool_calls=False)
    agent = Agent(
        client=client,
        context_providers=[primary, AddContext("selected"), sink],
        require_per_service_call_history_persistence=per_call,
    )
    providers = agent.context_providers
    mask = primary.store_context_from
    assert ensure_durable_history(agent) is agent
    session = agent.create_session(session_id="external-session")
    saved_service_id = None
    for turn, service_owned in enumerate((True, False, True, False), start=1):
        # The parent owns service-ID parking. Exercise its contract without editing entities.
        session.service_session_id = saved_service_id if service_owned else None
        prepared = prepare_owner(agent, service_owned)
        if service_owned:
            assert prepared is not agent and prepared.context_providers is not providers
            wrapper = prepared.context_providers[0]
            assert isinstance(wrapper, HistoryProvider)
            adapter: Any = wrapper
            assert adapter.__wrapped__ is primary
            assert wrapper.source_id == primary.source_id
            assert wrapper.load_messages is primary.load_messages
            assert wrapper.store_inputs is primary.store_inputs
            assert wrapper.store_outputs is primary.store_outputs
            assert wrapper.store_context_messages is primary.store_context_messages
            assert wrapper.store_context_from == mask
            assert adapter.resource is primary.resource
            assert prepare_owner(prepared, True) is prepared
            local_view = prepare_owner(prepared, False)
            assert local_view.context_providers[0] is primary
            assert prepared.client is agent.client
        else:
            assert prepared is agent and prepared.context_providers[0] is primary
        assert prepared.context_providers[-1] is sink
        options = {"store": service_owned}
        if stream:
            await prepared.run(f"turn-{turn}", session=session, options=options, stream=True).get_final_response()
        else:
            await prepared.run(f"turn-{turn}", session=session, options=options)
        if service_owned:
            saved_service_id = session.service_session_id
        session = AgentSession.from_dict(json.loads(json.dumps(session.to_dict())))
        assert [message.text for message in session.state["audit"]["messages"]] == [
            f"answer-{index}" for index in range(1, turn + 1)
        ]
        assert agent.context_providers is providers and providers[0] is primary
        assert primary.store_context_from is mask
        assert sink.source_id == "audit" and sink.store_inputs is False and sink.load_messages is False
    assert [message.text for message in primary.saved] == [
        "context-selected",
        "turn-2",
        "answer-2",
        "context-selected",
        "turn-4",
        "answer-4",
    ]
    assert primary.calls == [(phase, "external-session") for _ in range(2) for phase in ("load", "save")]
    assert primary.lifecycle == []
    assert all("turn-2" not in [message.text for message in client.received_messages[index]] for index in (0, 2))
    assert [message.text for message in client.received_messages[3]].count("turn-2") == 1
    assert not {"turn-1", "turn-3"} & {message.text for message in client.received_messages[3]}
    loaded = next(message for message in client.received_messages[3] if message.text == "turn-2")
    assert loaded.additional_properties["_attribution"] == {
        "source_id": "external",
        "source_type": "OrdinaryExternalHistory",
    }


async def test_service_view_never_calls_custom_primary_hooks_or_direct_storage_methods() -> None:
    class CustomExternal(OrdinaryExternalHistory):
        async def before_run(self, **kwargs: Any) -> None:
            self.calls.append(("before", None))
            await super().before_run(**kwargs)

        async def after_run(self, **kwargs: Any) -> None:
            self.calls.append(("after", None))
            await super().after_run(**kwargs)

    primary = CustomExternal()
    agent = Agent(client=ToolChatClient(tool_calls=False), context_providers=[primary])
    service = prepare_owner(agent, True)
    wrapper = service.context_providers[0]
    state = {"cursor": {"keep": [1, 3]}}
    before = deepcopy(state)
    assert await wrapper.get_messages("session", state=state) == []
    await wrapper.save_messages("session", [Message("user", ["must not save"])], state=state)
    await service.run("service", session=service.create_session(), options={"store": True})
    assert state == before and primary.calls == [] and primary.lifecycle == []
    await prepare_owner(agent, False).run("local", session=agent.create_session(), options={"store": False})
    assert [phase for phase, _ in primary.calls] == ["before", "load", "after", "save"]


def test_default_sink_collision_fails_without_reconfiguring_the_caller() -> None:
    sink = InMemoryHistoryProvider(load_messages=False)
    agent = Agent(client=ToolChatClient(tool_calls=False), context_providers=[sink])
    providers = agent.context_providers
    with pytest.raises(ValueError, match="in_memory.*source_id"):
        ensure_durable_history(agent)
    assert agent.context_providers is providers and providers == [sink]
    assert sink.source_id == "in_memory" and sink.load_messages is False


@pytest.mark.parametrize("other", [ContextProvider("same"), InMemoryHistoryProvider("same", load_messages=False)])
def test_duplicate_source_ids_fail_before_substitution(other: ContextProvider) -> None:
    primary = InMemoryHistoryProvider("same")
    agent = Agent(client=ToolChatClient(tool_calls=False), context_providers=[primary, other])
    with pytest.raises(ValueError, match="source_id.*same"):
        ensure_durable_history(agent)
    assert agent.context_providers == [primary, other]


async def test_uniquely_named_sink_only_keeps_separate_durable_and_sink_history() -> None:
    sink = InMemoryHistoryProvider("audit", load_messages=False)
    client = ToolChatClient(tool_calls=False)
    agent: Any = ensure_durable_history(Agent(client=client, context_providers=[sink]))
    # Like Core's automatic history, the implicit durable primary follows the caller's sink.
    assert len(agent.context_providers) == 2
    assert agent.context_providers[0] is sink
    primary = agent.context_providers[-1]
    assert isinstance(primary, DurableHistoryProvider) and primary.source_id == "in_memory"
    provider = _InMemoryStateProvider()
    session = agent.create_session()
    for turn in range(3):
        with bound(provider, f"turn-{turn}"):
            await agent.run(f"turn-{turn}", session=session)
            primary.flush(session.state[primary.source_id])
        assert len(transcript(provider)) == len(session.state["audit"]["messages"]) == (turn + 1) * 2
        session.state.pop(primary.source_id)
        session = AgentSession.from_dict(json.loads(json.dumps(session.to_dict())))
    assert [len(messages) for messages in client.received_messages] == [1, 3, 5]


@pytest.mark.parametrize("per_call", [False, True])
@pytest.mark.parametrize("factory", [InMemoryHistoryProvider, DurableHistoryProvider])
async def test_self_context_mask_does_not_reappend_history(factory: Any, per_call: bool) -> None:
    original = factory("history", store_context_messages=True, store_context_from={"history"})
    client = ToolChatClient(tool_calls=False)
    agent: Any = ensure_durable_history(
        Agent(client=client, context_providers=[original], require_per_service_call_history_persistence=per_call)
    )
    provider = _InMemoryStateProvider()
    session = agent.create_session()
    history = agent.context_providers[0]
    counts = []
    for turn in range(3):
        with bound(provider, f"turn-{turn}"):
            await agent.run(f"turn-{turn}", session=session)
            history.flush(session.state[history.source_id])
        counts.append(len(transcript(provider)))
    core_history = InMemoryHistoryProvider("history", store_context_messages=True, store_context_from={"history"})
    core = Agent(client=ToolChatClient(tool_calls=False), context_providers=[core_history])
    core_session = core.create_session()
    core_counts = []
    for turn in range(3):
        await core.run(f"turn-{turn}", session=core_session)
        core_counts.append(len(core_session.state["history"]["messages"]))
    assert counts == [2, 4, 6]
    # Core 1.13 still re-appends its own contribution; later core releases fix it.
    # Durable must not copy that historical duplication bug into its append path.
    assert core_counts in ([2, 4, 6], [2, 6, 14])
    assert [len(messages) for messages in client.received_messages] == [1, 3, 5]
    assert original.store_context_from == {"history"}


@pytest.mark.parametrize("mask", [None, set(), {"history"}, {"selected"}, {"history", "selected"}])
def test_context_mask_excludes_only_self_not_selected_sources(mask: set[str] | None) -> None:
    history = DurableHistoryProvider("history", store_context_messages=True, store_context_from=mask)
    context = SessionContext(input_messages=[])
    for source in ("history", "selected", "other"):
        context.extend_messages(source, [Message("user", [source])])
    assert [message.text for message in history._get_context_messages_to_store(context)] == [
        source for source in ("selected", "other") if mask is None or source in mask
    ]


@pytest.mark.parametrize("prune", [False, True])
async def test_unset_durable_subclass_keeps_overrides_and_resources(prune: bool) -> None:
    class CustomDurable(DurableHistoryProvider):
        def __init__(self, resource: object) -> None:
            super().__init__("custom", store_context_messages=True, store_context_from={"selected"})
            self.resource = resource
            self.events: list[str] = []

        async def before_run(self, **kwargs: Any) -> None:
            self.events.append("before")
            await super().before_run(**kwargs)

        async def after_run(self, **kwargs: Any) -> None:
            self.events.append("after")
            await super().after_run(**kwargs)

    original = CustomDurable(object())
    agent = Agent(client=ToolChatClient(tool_calls=False), context_providers=[original])
    prepared: Any = ensure_durable_history(agent, prune_excluded=prune)
    replacement = prepared.context_providers[0]
    assert type(replacement) is CustomDurable
    assert replacement is not original and replacement.resource is original.resource
    assert replacement.prune_excluded is prune and original.prune_excluded is None
    assert replacement.store_context_from == original.store_context_from
    assert replacement.store_context_from is not original.store_context_from
    with bound(_InMemoryStateProvider()):
        await prepared.run("input", session=prepared.create_session())
    assert replacement.events == ["before", "after"]


@pytest.mark.parametrize(
    "excluded",
    [set(members) for size in range(4) for members in combinations(("reason", "call", "result"), size)],
)
@pytest.mark.parametrize("non_contiguous", [False, True])
async def test_eager_pruning_requires_the_entire_old_atomic_group_to_be_excluded(
    excluded: set[str], non_contiguous: bool
) -> None:
    provider = _InMemoryStateProvider()
    messages = [
        Message("assistant", [Content.from_text_reasoning(text="reason")], message_id="reason"),
        Message("assistant", [Content.from_function_call(call_id="t", name="tool", arguments="{}")], message_id="call"),
        Message("tool", [Content.from_function_result(call_id="t", result="result")], message_id="result"),
    ]
    if non_contiguous:
        messages.insert(2, Message("user", ["gap"], message_id="gap"))
    provider.state.data.conversation_history.extend([
        DurableAgentStateResponse("old", OLD, [DurableAgentStateMessage.from_chat_message(m) for m in messages]),
        DurableAgentStateRequest("current", OLD, [stored("current", "current")]),
    ])
    history = DurableHistoryProvider(prune_excluded=True)
    state: dict[str, Any] = {}
    with bound(provider):
        await history.get_messages("session", state=state)
        for message in state[WORKING_BUFFER_KEY]:
            if message.message_id in excluded:
                message.additional_properties["_excluded"] = True
        history.flush(state)
        removed = 3 if len(excluded) == 3 else 0
        assert len(transcript(provider)) == len(messages) + 1 - removed
        assert {"reason", "call", "result"} & set(ids(provider)) == (set() if removed else {"reason", "call", "result"})
        assert (provider.state.data.truncation or {}).get("evictedMessageCount", 0) == removed
        snapshot = provider.state.to_dict()
        history.flush(state)
        assert provider.state.to_dict() == snapshot


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("remove_old", [False, True])
async def test_same_body_summary_id_reuse_keeps_distinct_lineage(nested: bool, remove_old: bool) -> None:
    def links(message: Message) -> dict[str, Any]:
        return message.additional_properties.setdefault("_group", {}) if nested else message.additional_properties

    provider = _InMemoryStateProvider()
    seed(provider)
    history = DurableHistoryProvider(prune_excluded=False)
    state: dict[str, Any] = {}
    with bound(provider) as binding:
        await history.get_messages("session", state=state)
        for source_id in ("seed-user", "seed-assistant"):
            buffer = state[WORKING_BUFFER_KEY]
            source = next(message for message in buffer if message.message_id == source_id)
            source.additional_properties["_excluded"] = True
            links(source)["_summarized_by_summary_id"] = "summary"
            if remove_old and source_id == "seed-assistant":
                buffer[:] = [message for message in buffer if message.message_id != "summary"]
            summary = Message("assistant", ["identical summary"], message_id="summary")
            links(summary)["_summary_of_message_ids"] = [source_id]
            buffer.insert(buffer.index(source) + 1, summary)
            history.flush(state)
        assert summary.message_id != "summary"
        assert ids(provider) == ["seed-user", "summary", "seed-assistant", summary.message_id]
        saved = {message.message_id: message.to_chat_message() for message in transcript(provider)}
        assert links(saved["summary"])["_summary_of_message_ids"] == ["seed-user"]
        assert links(saved[summary.message_id])["_summary_of_message_ids"] == ["seed-assistant"]
        assert links(saved["seed-user"])["_summarized_by_summary_id"] == "summary"
        assert links(saved["seed-assistant"])["_summarized_by_summary_id"] == summary.message_id
        snapshot = provider.state.to_dict()
        ordinal = binding.append_ordinal
        history.flush(state)
        assert provider.state.to_dict() == snapshot and binding.append_ordinal == ordinal
    cold = _InMemoryStateProvider(raw=json.loads(provider.state.to_json()))
    with bound(cold):
        cold_state: dict[str, Any] = {}
        loaded = await history.get_messages("session", state=cold_state)
        # keep_all retains both bodies and their distinct lineage, but a removed summary is not replayed.
        assert [message.text for message in loaded] == ["identical summary"] * (1 if remove_old else 2)
        assert [message.message_id for message in loaded] == [
            *([] if remove_old else ["summary"]),
            summary.message_id,
        ]
        history.flush(cold_state)
        assert cold.state.to_dict() == provider.state.to_dict()
