# Copyright (c) Microsoft. All rights reserved.

"""Public history identity and affirmative acceptance through real core hooks."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterable, Awaitable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

import pytest
from agent_framework import (
    Agent,
    AgentResponse,
    AgentSession,
    ChatResponse,
    ChatResponseUpdate,
    CompactionProvider,
    Content,
    ContextProvider,
    HistoryProvider,
    Message,
    ResponseStream,
    SessionContext,
)
from test_history_pipeline_revision import NonStreamingAgent, ToolChatClient, lookup
from test_revision_contract import JsonStateProvider

from agent_framework_durabletask import AgentEntity, DurableAgentState, DurableHistoryProvider
from agent_framework_durabletask._durable_agent_state import (
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
)
from agent_framework_durabletask._history_provider import (
    POSITIONS_KEY,
    WORKING_BUFFER_KEY,
    DurableHistoryBinding,
    bind_durable_history,
    current_durable_history_binding,
    unbind_durable_history,
)
from agent_framework_durabletask._message_identity import message_identity


def _wire(value: Any) -> Any:
    return json.loads(json.dumps(value, allow_nan=False))


def _projection(correlation: str, messages: Sequence[Message], occurrences: Sequence[str]) -> dict[str, Any]:
    return {
        "message": "logging only, never model input",
        "correlationId": correlation,
        "contextMessages": _wire([message.to_dict() for message in messages]),
        "contextMessageIds": list(occurrences),
    }


def _rows(provider: JsonStateProvider) -> list[dict[str, Any]]:
    return [message for entry in provider.raw["data"]["conversationHistory"] for message in entry["messages"]]


def _assert_no_private_fields(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            assert not key.startswith("_durable_"), f"operation-local attribute serialized: {key}"
            assert key != POSITIONS_KEY
            _assert_no_private_fields(item)
    elif isinstance(value, list):
        for item in value:
            _assert_no_private_fields(item)


@contextmanager
def _bound(provider: JsonStateProvider) -> Iterator[DurableHistoryBinding]:
    binding = DurableHistoryBinding(provider, "cold-flush")
    token = bind_durable_history(binding)
    try:
        yield binding
    finally:
        unbind_durable_history(token)


def _assert_positions(provider: JsonStateProvider, state: dict[str, Any]) -> None:
    positions = state[POSITIONS_KEY]
    for message in state[WORKING_BUFFER_KEY]:
        history_id = getattr(message, "_durable_history_id", None) or message.message_id
        entry, index = positions[history_id]
        assert any(entry is candidate for candidate in provider.state.data.conversation_history)
        stored = entry.messages[index]
        assert stored.message_id == history_id
        assert stored.public_message_id == message.message_id
        assert stored.role == message.role and stored.text == message.text
        assert [item.to_dict() for item in stored.to_chat_message().contents] == [
            item.to_dict() for item in message.contents
        ]
    assert len(positions) == sum(len(entry.messages) for entry in provider.state.data.conversation_history)


class _Probe(ContextProvider):
    def __init__(self, history_source: str = "durable_history") -> None:
        super().__init__("identity-probe")
        self.history_source = history_source
        self.fail_at: str | None = None
        self.inputs: list[list[Message]] = []
        self.contexts: list[list[Message]] = []
        self.aliases: list[list[Message]] = []
        self.responses: list[AgentResponse[Any]] = []
        self.agents: list[Any] = []
        self.accepted: list[set[tuple[str, str]]] = []

    async def before_run(self, *, agent: Any, context: SessionContext, **kwargs: Any) -> None:
        self.agents.append(agent)
        self.inputs.append(deepcopy(context.input_messages))
        self.contexts.append(deepcopy(context.get_messages(include_input=True)))
        self.aliases.append(list(context.get_messages(include_input=True)))
        if self.fail_at == "before":
            raise RuntimeError("probe failed before load")

    async def after_run(
        self, *, context: SessionContext, session: AgentSession, state: dict[str, Any], **kwargs: Any
    ) -> None:
        assert isinstance(context.response, AgentResponse)
        self.responses.append(context.response)
        binding = current_durable_history_binding()
        assert binding is not None
        self.accepted.append(set(binding.accepted_inputs))
        state["response"] = deepcopy(context.response.to_dict())
        state["history_ids"] = [
            message.message_id for message in session.state.get(self.history_source, {}).get(WORKING_BUFFER_KEY, [])
        ]
        if self.fail_at == "after":
            raise RuntimeError("probe failed after model")


class _Audit(HistoryProvider):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__("identity-audit", load_messages=False, **kwargs)
        self.saved: list[list[Message]] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        pytest.fail("store-only audit must never become the primary")

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.saved.append(deepcopy(list(messages)))


@pytest.mark.parametrize("case", ["normal", "duplicate-different", "duplicate-identical"])
async def test_public_ids_survive_two_appends_and_a_cold_third_model_probe_and_audit(case: str) -> None:
    duplicate = case != "normal"
    messages = [
        Message("user", ["first"], message_id="shared" if duplicate else "user-0"),
        Message(
            "user",
            ["first" if case == "duplicate-identical" else "second"],
            message_id="shared" if duplicate else "user-1",
        ),
    ]
    history = DurableHistoryProvider(prune_excluded=False)
    probe = _Probe()
    audit = _Audit(store_context_messages=True)
    client = ToolChatClient(tool_calls=False)
    agent = Agent(client=client, context_providers=[history, probe, audit])
    original_providers = agent.context_providers
    raw: dict[str, Any] = {}
    expected_ids: list[str | None] = []
    expected_texts: list[str] = []
    for index, message in enumerate(messages):
        client.response_message_id = "shared" if duplicate else f"answer-{index}"
        provider = JsonStateProvider(_wire(raw))
        request = _projection(f"turn-{index}", [message], [f"occ-{index}"])
        before = deepcopy(request)
        response = await AgentEntity(agent, state_provider=provider).run(request)
        assert response.text == f"answer-{index + 1}"
        assert response.messages[0].message_id == client.response_message_id
        assert request == before and message.to_dict() == before["contextMessages"][0]
        expected_ids.extend([message.message_id, client.response_message_id])
        expected_texts.extend([message.text, response.text])
        raw = _wire(provider.raw)
        assert agent.context_providers is original_providers
        _assert_no_private_fields(response.to_dict())
    rows = _rows(provider)
    assert len({row["messageId"] for row in rows}) == len(rows) == 4
    assert [row.get("originalMessageId", row["messageId"]) for row in rows] == expected_ids
    assert ["originalMessageId" in row for row in rows] == ([False, True, True, True] if duplicate else [False] * 4)
    assert DurableAgentState.from_dict(raw).to_dict() == raw
    client.response_message_id = "third-answer"
    cold = JsonStateProvider(raw)
    third = Message("user", ["third"], message_id="third-input")
    response = await AgentEntity(agent, state_provider=cold).run(_projection("third", [third], ["occ-third"]))
    assert response.text == "answer-3"
    for batch in (client.received_messages[-1], probe.contexts[-1], audit.saved[-1][:-1]):
        assert [message.message_id for message in batch] == [*expected_ids, "third-input"]
        assert [message.text for message in batch] == [*expected_texts, "third"]
        assert [message.role for message in batch] == ["user", "assistant", "user", "assistant", "user"]
        _assert_no_private_fields([message.to_dict() for message in batch])
    assert audit.saved[-1][-1].message_id == "third-answer"
    assert [message.message_id for message in probe.inputs[-1]] == ["third-input"]
    assert cold.raw["data"]["ingestedMessages"] == {
        **{f"occ-{index}": [message_identity(message)] for index, message in enumerate(messages)},
        "occ-third": [message_identity(third)],
    }
    _assert_no_private_fields(cold.raw)


def _seed_history() -> dict[str, Any]:
    state = DurableAgentState()
    when = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for index, correlation in enumerate(("old-0", "old-1", "anchor")):
        for role, entry_type in (("user", DurableAgentStateRequest), ("assistant", DurableAgentStateResponse)):
            message = DurableAgentStateMessage.from_chat_message(
                Message(role, ["equal" if role == "user" else f"answer-{index}"], message_id="shared")
            )
            message.message_id = f"stored-{role}-{index}"
            message.original_message_id = "shared"
            state.data.conversation_history.append(entry_type(correlation, when, [message]))
    state.data.session = AgentSession(session_id="revision-session").to_dict()
    state.data.ingested_messages = {"old-occurrence": ["old-fingerprint"]}
    return _wire(state.to_dict())


class _CompactOccurrence:
    def __init__(self, *, copy_messages: bool) -> None:
        self.copy_messages = copy_messages
        self.calls = 0
        self.originals: list[Message] = []
        self.survivors: list[Message] = []

    async def __call__(self, messages: list[Message]) -> bool:
        self.calls += 1
        self.originals = list(messages)
        history = [message for message in messages if getattr(message, "_durable_history_id", None)]
        assert len({message.message_id for message in history}) == len(history) == 6
        assert all(message.message_id == getattr(message, "_durable_history_id", None) for message in history)
        target = next(message for message in messages if message.message_id == "stored-user-1")
        target.additional_properties.update({"_excluded": True, "exact_occurrence": {"keep": [1, False]}})
        if self.copy_messages:
            target.additional_properties["_summarized_by_summary_id"] = "occurrence-summary"
            summary = Message(
                "assistant",
                ["summary of only the second equal input"],
                message_id="occurrence-summary",
                additional_properties={"_summary_of_message_ids": [target.message_id]},
            )
            messages.insert(messages.index(target) + 1, summary)
            # Copies must reconcile by private occurrence, not text, public ID or object identity.
            messages[:] = deepcopy(messages)
        self.survivors = list(messages)
        return True


@pytest.mark.parametrize("phase", ["before", "after"])
@pytest.mark.parametrize("prune", [False, True])
async def test_compaction_targets_one_equal_occurrence_and_restores_public_ids(phase: str, prune: bool) -> None:
    history = DurableHistoryProvider(prune_excluded=prune)
    before_strategy = _CompactOccurrence(copy_messages=False) if phase == "before" else None
    strategy = _CompactOccurrence(copy_messages=True)
    compaction = CompactionProvider(
        before_strategy=before_strategy,
        after_strategy=strategy,
        history_source_id=history.source_id,
    )
    probe = _Probe()
    audit = _Audit(store_context_messages=True)
    client = ToolChatClient(tool_calls=False, response_message_id="shared")
    # The final probe runs after compaction and the save, and keeps a serialized response/session witness.
    agent = Agent(client=client, context_providers=[probe, history, compaction, audit])
    providers = agent.context_providers
    provider = JsonStateProvider(_seed_history())
    request = _projection("compact", [Message("user", ["current"], message_id="shared")], ["current-occ"])
    before = deepcopy(request)
    response = await AgentEntity(agent, state_provider=provider).run(request)
    assert response.text == "answer-1" and response.messages[0].message_id == "shared"
    assert request == before and strategy.calls == 1 and agent.context_providers is providers
    assert compaction.before_strategy is before_strategy and compaction.after_strategy is strategy
    if before_strategy is not None:
        assert before_strategy.calls == 1
        assert all(message.message_id == "shared" for message in before_strategy.originals)
        assert not any(
            getattr(message, "_durable_history_id", None) == "stored-user-1" for message in client.received_messages[0]
        )
    else:
        assert any(
            getattr(message, "_durable_history_id", None) == "stored-user-1" for message in client.received_messages[0]
        )
    for batch in (strategy.originals, strategy.survivors, client.received_messages[0], audit.saved[0]):
        assert all(
            message.message_id == "shared"
            for message in batch
            if getattr(message, "_durable_history_id", None) != "occurrence-summary"
            and message.message_id != "occurrence-summary"
        )
    rows = {row["messageId"]: row for row in _rows(provider)}
    assert ("stored-user-1" in rows) is not prune
    assert "stored-user-0" in rows and "stored-user-2" in rows
    for key in ("stored-user-0", "stored-user-2"):
        assert not rows[key].get("extensionData", {}).get("_excluded")
        assert "exact_occurrence" not in rows[key].get("extensionData", {})
    if not prune:
        assert rows["stored-user-1"]["extensionData"]["exact_occurrence"] == {"keep": [1, False]}
        assert rows["stored-user-1"]["extensionData"]["_excluded"] is True
    assert rows["occurrence-summary"]["extensionData"]["_summary_of_message_ids"] == ["stored-user-1"]
    assert "originalMessageId" not in rows["occurrence-summary"]
    assert len(rows) == 9 - int(prune)
    saved_probe = provider.raw["data"]["session"]["state"][probe.source_id]
    assert saved_probe["response"]["messages"][0]["message_id"] == "shared"
    assert set(saved_probe["history_ids"]) <= {"shared", "occurrence-summary"}
    _assert_no_private_fields(provider.raw)
    cold = JsonStateProvider(_wire(provider.raw))
    state: dict[str, Any] = {}
    with _bound(cold):
        loaded = await history.get_messages("revision-session", state=state)
        assert all(getattr(message, "_durable_history_id", None) != "stored-user-1" for message in loaded)
        assert [message.text for message in loaded].count("equal") == 2
        history.flush(state)
        snapshot = deepcopy(cold.state.to_dict())
        history.flush(state)
        assert cold.state.to_dict() == snapshot
        _assert_positions(cold, state)


class _InterleavedCompaction:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.ready = asyncio.Event()
        self.agent: Agent | None = None
        self.child_state: JsonStateProvider | None = None
        self.child_response: AgentResponse[Any] | None = None
        self.bindings: dict[str, DurableHistoryBinding] = {}
        self.aliases: dict[str, list[Message]] = {}

    async def __call__(self, messages: list[Message]) -> bool:
        binding = current_durable_history_binding()
        assert binding is not None and binding.correlation_id is not None
        correlation = binding.correlation_id
        assert correlation not in self.bindings
        self.bindings[correlation] = binding
        self.aliases[correlation] = list(messages)
        assert len(messages) == 8 and len({message.message_id for message in messages}) == 8
        assert all(message.message_id == getattr(message, "_durable_history_id", None) for message in messages)
        if self.mode == "nested" and correlation == "outer":
            assert self.agent is not None and self.child_state is not None
            self.child_response = await AgentEntity(self.agent, state_provider=self.child_state).run(
                _projection("inner", [Message("user", ["inner input"], message_id="shared")], ["inner-occ"])
            )
            assert self.child_response.messages[0].message_id == "shared"
            assert all(message.message_id == "shared" for message in self.aliases["inner"])
        elif self.mode == "concurrent":
            if len(self.bindings) == 2:
                self.ready.set()
            await self.ready.wait()
        assert current_durable_history_binding() is binding
        assert all(message.message_id == getattr(message, "_durable_history_id", None) for message in messages)
        messages[:] = deepcopy(messages)
        for message in messages:
            message.additional_properties["operation_annotation"] = correlation
        self.aliases[correlation].extend(messages)
        return True


@pytest.mark.parametrize("mode", ["nested", "concurrent"])
async def test_compaction_scopes_are_isolated_for_two_entities_sharing_one_agent(mode: str) -> None:
    history = DurableHistoryProvider(prune_excluded=False)
    strategy = _InterleavedCompaction(mode)
    compaction = CompactionProvider(after_strategy=strategy, history_source_id=history.source_id)
    probe = _Probe()
    audit = _Audit()
    client = ToolChatClient(tool_calls=False, response_message_id="shared")
    agent = Agent(client=client, context_providers=[probe, compaction, history, audit])
    original_providers = agent.context_providers
    states = {key: JsonStateProvider(_seed_history()) for key in ("outer", "inner")}
    strategy.agent = agent
    strategy.child_state = states["inner"]

    async def run(correlation: str) -> AgentResponse[Any]:
        return await AgentEntity(agent, state_provider=states[correlation]).run(
            _projection(
                correlation,
                [Message("user", [f"{correlation} input"], message_id="shared")],
                [f"{correlation}-occ"],
            )
        )

    responses: list[AgentResponse[Any] | None]
    if mode == "nested":
        responses = [await run("outer"), strategy.child_response]
    else:
        responses = list(await asyncio.wait_for(asyncio.gather(run("outer"), run("inner")), timeout=5))
    assert len(strategy.bindings) == 2 and strategy.bindings["outer"] is not strategy.bindings["inner"]
    assert agent.context_providers is original_providers and compaction.after_strategy is strategy
    assert current_durable_history_binding() is None
    for response in responses:
        assert response is not None and response.additional_properties.get("durable_status") != "error"
        assert all(message.message_id == "shared" for message in response.messages)
        assert not any(message.additional_properties.get("operation_annotation") for message in response.messages)
        _assert_no_private_fields(response.to_dict())
    for correlation, provider in states.items():
        assert provider.writes == 1
        assert all(message.message_id == "shared" for message in strategy.aliases[correlation])
        assert {row["extensionData"]["operation_annotation"] for row in _rows(provider)} == {correlation}
        assert provider.raw["data"]["ingestedMessages"] == {
            "old-occurrence": ["old-fingerprint"],
            f"{correlation}-occ": [message_identity(Message("user", [f"{correlation} input"], message_id="shared"))],
        }
        witness = provider.raw["data"]["session"]["state"][probe.source_id]
        assert witness["history_ids"] == ["shared"] * 8
        assert witness["response"]["messages"][0]["message_id"] == "shared"
        _assert_no_private_fields(provider.raw)
    assert len(audit.saved) == 2
    assert all(message.message_id == "shared" for batch in audit.saved for message in batch)
    assert all(message.message_id == "shared" for batch in client.received_messages for message in batch)


class _SaveOverride(DurableHistoryProvider):
    def __init__(self, mode: str) -> None:
        super().__init__(prune_excluded=False)
        self.mode = mode
        self.calls: list[tuple[bool, list[Message]]] = []
        self.states: list[dict[str, Any]] = []
        self.bindings: list[DurableHistoryBinding] = []

    async def save_messages(
        self, session_id: str | None, messages: Sequence[Message], *, state: dict[str, Any] | None = None, **kwargs: Any
    ) -> None:
        binding = current_durable_history_binding()
        assert binding is not None and state is not None and session_id == "revision-session"
        response_batch = binding.append_response is not None
        self.calls.append((response_batch, deepcopy(list(messages))))
        self.states.append(state)
        self.bindings.append(binding)
        if self.mode == ("raise-output" if response_batch else "raise-input"):
            raise ValueError("save override rejected batch")
        transformed = deepcopy(list(messages))
        for message in transformed:
            if self.mode == "transform":
                message.author_name = "validated-by-save"
                message.additional_properties["validated"] = True
                for content in message.contents:
                    if content.type == "text":
                        content.text = f"stored:{content.text}"
        await super().save_messages(session_id, transformed, state=state, **kwargs)


@pytest.mark.parametrize("per_call", [False, True])
async def test_public_save_override_transforms_batches_without_losing_response_provenance(per_call: bool) -> None:
    history = _SaveOverride("transform")
    probe = _Probe()
    client = ToolChatClient()
    agent = Agent(
        client=client,
        tools=[lookup],
        context_providers=[probe, history],
        require_per_service_call_history_persistence=per_call,
    )
    provider = JsonStateProvider()
    request = _projection("save-transform", [Message("user", ["use lookup"], message_id="input")], ["occ"])
    before = deepcopy(request)
    response = await AgentEntity(agent, state_provider=provider).run(request)
    assert response.text == "answer-2" and request == before
    assert len(client.received_messages) == 2
    assert [is_response for is_response, _ in history.calls] == [False, True] * (2 if per_call else 1)
    assert all(state is history.states[0] for state in history.states)
    assert all(binding.append_response is None for binding in history.bindings)
    entries = provider.raw["data"]["conversationHistory"]
    assert [entry["$type"] for entry in entries] == ["request", "response"] * (2 if per_call else 1)
    assert [row["role"] for row in _rows(provider)] == ["user", "assistant", "tool", "assistant"]
    assert all(row["authorName"] == "validated-by-save" for row in _rows(provider))
    assert all(row["extensionData"]["validated"] is True for row in _rows(provider))
    assert _rows(provider)[0]["contents"][0]["text"] == "stored:use lookup"
    assert _rows(provider)[-1]["contents"][0]["text"] == "stored:answer-2"
    assert not any(message.additional_properties.get("validated") for message in response.messages)
    assert provider.raw["data"]["ingestedMessages"] == {
        "occ": [message_identity(Message.from_dict(before["contextMessages"][0]))]
    }
    assert history.source_id not in provider.raw["data"]["session"]["state"]
    _assert_no_private_fields(provider.raw)


@pytest.mark.parametrize("mode", ["raise-input", "raise-output"])
@pytest.mark.parametrize("per_call", [False, True])
async def test_public_save_override_can_reject_without_promoting_unsaved_inputs(mode: str, per_call: bool) -> None:
    history = _SaveOverride(mode)
    client = ToolChatClient(tool_calls=False)
    provider = JsonStateProvider()
    message = Message("user", ["validate"], message_id="shared")
    request = _projection("rejected", [message, deepcopy(message)], ["o1", "o2"])
    response = await AgentEntity(
        Agent(client=client, context_providers=[history], require_per_service_call_history_persistence=per_call),
        state_provider=provider,
    ).run(request)
    assert "save override rejected batch" in response.text
    assert response.additional_properties["durable_status"] == "error"
    assert len(client.received_messages) == 1 and len(history.calls) == (2 if mode == "raise-output" else 1)
    assert [row["role"] for row in _rows(provider)] == (["user"] * 2 if mode == "raise-output" else [])
    assert provider.raw["data"].get("ingestedMessages", {}) == (
        {key: [message_identity(message)] for key in ("o1", "o2")} if mode == "raise-output" else {}
    )
    assert all(binding.append_response is None for binding in history.bindings)
    assert provider.raw["data"]["completedCorrelations"]["rejected"]["outcome"] == "failed"
    _assert_no_private_fields(provider.raw)


class _External(HistoryProvider):
    def __init__(self, mode: str, **kwargs: Any) -> None:
        super().__init__("external", **kwargs)
        self.mode = mode
        self.events: list[str] = []
        self.saved: list[Message] = []
        self.accepted_during_save: list[set[tuple[str, str]]] = []

    async def before_run(self, **kwargs: Any) -> None:
        self.events.append("before")
        await super().before_run(**kwargs)

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        self.events.append("load")
        if self.mode == "load-failure":
            raise OSError("external load failed")
        return deepcopy(self.saved)

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.events.append("save-start")
        binding = current_durable_history_binding()
        assert binding is not None
        self.accepted_during_save.append(set(binding.accepted_inputs))
        self.saved.extend(deepcopy(list(messages[:1] if self.mode == "interrupted-save" else messages)))
        if self.mode == "interrupted-save":
            raise OSError("external save did not return")
        self.events.append("save-return")


class _OpaqueExternal(_External):
    async def after_run(self, *, session: AgentSession, context: SessionContext, **kwargs: Any) -> None:
        self.events.append("custom-after")
        if self.mode == "opaque-partial":
            await self.save_messages(session.session_id, context.input_messages[:1])


@pytest.mark.parametrize(
    "mode",
    ["completed", "before-failure", "load-failure", "interrupted-save", "opaque-noop", "opaque-partial", "no-inputs"],
)
async def test_failed_external_runs_keep_only_affirmative_completed_primary_receipts(mode: str) -> None:
    external_type: type[_External] = _OpaqueExternal if mode.startswith("opaque") else _External
    external = external_type(mode, store_inputs=mode != "no-inputs")
    probe = _Probe()
    probe.fail_at = "before" if mode == "before-failure" else "after"
    client = ToolChatClient(tool_calls=False)
    agent = Agent(client=client, context_providers=[probe, external])
    original_providers = agent.context_providers
    original_before = external.before_run
    original_after = external.after_run
    original_save = external.save_messages
    provider = JsonStateProvider(_seed_history())
    message = Message("user", ["equal delivery"], message_id="shared")
    request = _projection("failed", [message, deepcopy(message)], ["o1", "o2"])
    before = deepcopy(request)
    response = await AgentEntity(agent, state_provider=provider).run(request)
    accepted = mode == "completed"
    assert response.additional_properties["durable_status"] == "error" and request == before
    assert agent.context_providers is original_providers and original_providers[1] is external
    assert external.before_run == original_before and external.after_run == original_after
    assert external.save_messages == original_save
    assert probe.agents[0].context_providers[1].__wrapped__ is external
    assert external.accepted_during_save == ([set()] if "save-start" in external.events else [])
    expected = {"old-occurrence": ["old-fingerprint"]}
    if accepted:
        expected.update({key: [message_identity(message)] for key in ("o1", "o2")})
        assert external.events == ["before", "load", "save-start", "save-return"]
        assert [item.role for item in external.saved] == ["user", "user", "assistant"]
        assert probe.accepted == [{(key, message_identity(message)) for key in ("o1", "o2")}]
    elif mode == "before-failure":
        assert external.events == []
    elif mode == "load-failure":
        assert external.events == ["before", "load"]
    elif mode == "interrupted-save":
        assert external.events == ["before", "load", "save-start"] and len(external.saved) == 1
    elif mode == "opaque-noop":
        assert external.events == ["before", "load", "custom-after"] and external.saved == []
    elif mode == "opaque-partial":
        assert external.events == ["before", "load", "custom-after", "save-start", "save-return"]
        assert len(external.saved) == 1 and probe.accepted == [set()]
    else:
        assert [item.role for item in external.saved] == ["assistant"] and probe.accepted == [set()]
    assert len(client.received_messages) == int(mode not in ("before-failure", "load-failure"))
    assert provider.raw["data"].get("ingestedMessages", {}) == expected
    assert provider.raw["data"]["conversationHistory"] == _seed_history()["data"]["conversationHistory"]
    assert provider.raw["data"]["completedCorrelations"]["failed"]["outcome"] == "failed"
    _assert_no_private_fields(provider.raw)
    probe.fail_at = None
    external.mode = "completed"
    cold = JsonStateProvider(_wire(provider.raw))
    entity = AgentEntity(agent, state_provider=cold)
    calls = len(client.received_messages)
    assert (await entity.run(request)).to_dict() == response.to_dict()
    assert len(client.received_messages) == calls and cold.writes == 0
    await entity.run({**request, "correlationId": "retry-inputs"})
    assert [item.to_dict() for item in probe.inputs[-1]] == [message.to_dict()] * (0 if accepted else 2)


async def test_completed_store_only_sink_is_not_evidence_of_primary_acceptance() -> None:
    probe = _Probe()
    probe.fail_at = "after"
    history = DurableHistoryProvider(store_inputs=False, store_outputs=False, prune_excluded=False)
    sink = _Audit()
    message = Message("user", ["sink only"], message_id="shared")
    provider = JsonStateProvider()
    agent = Agent(client=ToolChatClient(tool_calls=False), context_providers=[probe, history, sink])
    response = await AgentEntity(agent, state_provider=provider).run(_projection("sink", [message], ["o1"]))
    assert response.additional_properties["durable_status"] == "error"
    assert [[item.role for item in batch] for batch in sink.saved] == [["user", "assistant"]]
    assert probe.agents[0].context_providers[-1] is sink
    assert probe.accepted == [set()] and provider.raw["data"].get("ingestedMessages", {}) == {}
    assert provider.raw["data"]["conversationHistory"] == []


class _InterruptedClient(ToolChatClient):
    def _inner_get_response(
        self, *, messages: Sequence[Message], stream: bool, options: Mapping[str, Any], **kwargs: Any
    ) -> Awaitable[ChatResponse] | ResponseStream[ChatResponseUpdate, ChatResponse]:
        assert stream
        self.received_messages.append(deepcopy(list(messages)))
        self.received_options.append(dict(options))

        async def updates() -> AsyncIterable[ChatResponseUpdate]:
            yield ChatResponseUpdate(
                role="assistant",
                contents=[Content.from_text("partial")],
                message_id="shared",
                response_id="partial-response",
                conversation_id="unconfirmed-service",
            )
            raise OSError("stream interrupted before completion")

        return ResponseStream(updates(), finalizer=ChatResponse.from_updates)


@pytest.mark.parametrize(
    ("service_owned", "stream", "interrupted", "per_call"),
    [
        (True, False, False, False),
        (True, True, False, False),
        (True, False, False, True),
        (True, True, False, True),
        (True, True, True, False),
        (True, True, True, True),
        (False, False, False, False),
        (False, True, False, False),
    ],
)
async def test_service_completion_not_dispatch_or_local_model_response_affirms_acceptance(
    service_owned: bool, stream: bool, interrupted: bool, per_call: bool
) -> None:
    history = DurableHistoryProvider(store_inputs=not service_owned, prune_excluded=False)
    probe = _Probe()
    probe.fail_at = "after"
    client = (_InterruptedClient if interrupted else ToolChatClient)(tool_calls=False, response_message_id="shared")
    agent = (Agent if stream else NonStreamingAgent)(
        client=client,
        context_providers=[history, probe],
        require_per_service_call_history_persistence=per_call,
    )
    raw = _seed_history()
    raw["data"]["session"]["service_session_id"] = "prior-service"
    provider = JsonStateProvider(raw)
    message = Message("user", ["equal service inputs"], message_id="shared")
    request = {**_projection("failed", [message, deepcopy(message)], ["o1", "o2"]), "options": {"store": service_owned}}
    response = await AgentEntity(agent, state_provider=provider).run(request)
    assert response.additional_properties["durable_status"] == "error" and len(client.received_messages) == 1
    expected = {"old-occurrence": ["old-fingerprint"]}
    if service_owned and not interrupted:
        expected.update({key: [message_identity(message)] for key in ("o1", "o2")})
        assert probe.accepted == [{(key, message_identity(message)) for key in ("o1", "o2")}]
        assert provider.raw["data"]["session"]["service_session_id"] == "service-thread"
    else:
        assert probe.accepted == ([] if interrupted else [set()])
        # Core propagates streaming continuation IDs eagerly. That is not an input acceptance receipt.
        assert provider.raw["data"]["session"]["service_session_id"] == (
            "unconfirmed-service" if interrupted else "prior-service"
        )
    assert provider.raw["data"]["ingestedMessages"] == expected
    assert provider.raw["data"]["conversationHistory"] == raw["data"]["conversationHistory"]
    assert provider.raw["data"]["completedCorrelations"]["failed"]["outcome"] == "failed"
    assert client.received_options[0].get("conversation_id") == ("prior-service" if service_owned else None)
    _assert_no_private_fields(provider.raw)
    cold_probe = _Probe()
    cold_client = ToolChatClient(tool_calls=False)
    cold = JsonStateProvider(_wire(provider.raw))
    await AgentEntity(Agent(client=cold_client, context_providers=[cold_probe]), state_provider=cold).run({
        **request,
        "correlationId": "next",
    })
    assert [item.to_dict() for item in cold_probe.inputs[-1]] == [message.to_dict()] * (
        0 if service_owned and not interrupted else 2
    )
    assert cold_client.received_options[0].get("conversation_id") == (
        ("service-thread" if not interrupted else "unconfirmed-service") if service_owned else None
    )


@pytest.mark.parametrize(
    ("scope", "transform"), [("none", False), ("message", False), ("content", False), ("both", False), ("both", True)]
)
async def test_optional_input_json_survives_append_and_cold_flush_without_overwriting_transformations(
    scope: str, transform: bool
) -> None:
    message = Message(
        "user",
        [Content.from_text("original text", additional_properties={"known": [1]})],
        message_id="opaque-input",
        author_name="caller",
        additional_properties={"known": [2]},
    )
    raw = message.to_dict()
    opaque = {"nested": [None, False, 0, "", {"type": "business", "items": [1, 2.5]}]}
    if scope in ("message", "both"):
        raw["future_message"] = deepcopy(opaque)
    if scope in ("content", "both"):
        raw["contents"][0]["future_content"] = deepcopy(opaque)
    request = {
        "message": "log only",
        "correlationId": "opaque",
        "contextMessages": [raw],
        "contextMessageIds": ["occ"],
    }
    before = deepcopy(request)
    history = _SaveOverride("transform" if transform else "validate")
    client = ToolChatClient(tool_calls=False)
    provider = JsonStateProvider()
    entity = AgentEntity(Agent(client=client, context_providers=[history]), state_provider=provider)
    response = await entity.run(request)
    assert response.text == "answer-1" and request == before
    assert message.text == "original text" and message.author_name == "caller"
    saved = next(row for row in _rows(provider) if row["messageId"] == "opaque-input")
    assert saved["contents"][0]["text"] == ("stored:original text" if transform else "original text")
    assert saved["authorName"] == ("validated-by-save" if transform else "caller")
    assert saved["extensionData"]["known"] == [2]
    assert saved["extensionData"].get("validated", False) is transform
    assert saved.get("future_message") == (opaque if scope in ("message", "both") else None)
    assert saved["contents"][0].get("extensionData", {}).get("coreContent", {}).get("future_content") == (
        opaque if scope in ("content", "both") else None
    )
    assert DurableAgentState.from_dict(provider.raw).to_dict() == provider.raw
    _assert_no_private_fields(provider.raw)
    cold = JsonStateProvider(_wire(provider.raw))
    state: dict[str, Any] = {}
    with _bound(cold):
        loaded = await history.get_messages("revision-session", state=state)
        assert loaded[0].message_id == "opaque-input" and loaded[0].text == saved["contents"][0]["text"]
        assert loaded[0].contents[0].additional_properties == {"known": [1]}
        loaded[0].additional_properties["cold_annotation"] = {"keep": [False, None]}
        history.flush(state)
        snapshot = deepcopy(cold.state.to_dict())
        history.flush(state)
        assert cold.state.to_dict() == snapshot
        _assert_positions(cold, state)
    cold_saved = snapshot["data"]["conversationHistory"][0]["messages"][0]
    assert cold_saved == {
        **saved,
        "extensionData": {**saved["extensionData"], "cold_annotation": {"keep": [False, None]}},
    }
    assert cold_saved["contents"] == saved["contents"]
    assert request == before
    _assert_no_private_fields(snapshot)


@pytest.mark.parametrize("invalid", [None, False, 17, [], {}])
def test_present_original_message_id_requires_a_string(invalid: Any) -> None:
    raw = {"role": "user", "contents": [{"$type": "text", "text": "input"}], "messageId": "internal"}
    with pytest.raises(ValueError, match="originalMessageId"):
        DurableAgentStateMessage.from_dict({**raw, "originalMessageId": invalid})
    control = DurableAgentStateMessage.from_dict(raw)
    assert control.public_message_id == "internal" and "originalMessageId" not in control.to_dict()
    restored = DurableAgentStateMessage.from_dict({**raw, "originalMessageId": "shared"})
    assert restored.message_id == "internal" and restored.to_chat_message().message_id == "shared"
    assert restored.to_dict()["originalMessageId"] == "shared"
