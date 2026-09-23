# Copyright (c) Microsoft. All rights reserved.

"""Focused reconciliation, mutable Core hook settings and append timestamp regressions."""

from __future__ import annotations

import json
from collections.abc import Sequence
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from agent_framework import Agent, AgentResponse, AgentSession, HistoryProvider, Message, SessionContext
from test_private_history_pipeline import ToolChatClient, _bound, _CanonicalStateProvider, _request, _stored, lookup
from test_shared_history_provider import OrdinaryExternalHistory

from agent_framework_durabletask._history_provider import (
    DurableHistoryProvider,
    current_durable_history_binding,
    ensure_durable_history,
    prepare_history_owner,
)
from agent_framework_durabletask._shared_agent_state import (
    DurableAgentState,
    DurableAgentStateCompaction,
    DurableAgentStateEntry,
    DurableAgentStateMessage,
    DurableAgentStateResponse,
)

OLD = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
REVISION = "durable_revision_compaction_current_0_0"
RECEIPT = ("current-occurrence", "b" * 64)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _owner(history: list[DurableAgentStateEntry] | None = None) -> _CanonicalStateProvider:
    owner = _CanonicalStateProvider(history)
    owner.state.data.session = {"session_id": "quality", "state": {"unrelated": [False, 0, 0.0, None]}}
    owner.state.data.ingested_messages = {"prior-occurrence": ["a" * 64]}
    owner.state.data.extension_data = {"opaque": [False, 0, None]}
    return owner


def _controls(owner: _CanonicalStateProvider) -> str:
    snapshot = owner.state.to_dict()
    snapshot["data"].pop("conversationHistory")
    return _json(snapshot)


async def _cold_replay(owner: _CanonicalStateProvider, controls: str) -> tuple[_CanonicalStateProvider, list[Message]]:
    snapshot = _json(owner.state.to_dict())
    cold = _CanonicalStateProvider()
    cold.state = DurableAgentState.from_json(snapshot)  # Full state, not a transcript-only clone.
    assert cold.state is not owner.state and _controls(cold) == controls
    history = DurableHistoryProvider(skip_excluded=False)
    working: dict[str, Any] = {}
    with _bound(cold) as binding:
        replay = await history.get_messages("quality", state=working)
        history.flush(working)
        assert binding.append_ordinal == 0
    assert _json(cold.state.to_dict()) == snapshot
    assert owner.persist_count == cold.persist_count == 0
    return cold, replay


@pytest.mark.parametrize("grouped", [False, True])
@pytest.mark.parametrize(("candidate_id", "prepend"), [("same", False), ("same", True), ("distinct", False)])
async def test_summary_candidate_cannot_shadow_its_loaded_source(
    grouped: bool, candidate_id: str, prepend: bool
) -> None:
    owner = _owner([_request("seed", _stored("source", message_id="same"))])
    controls = _controls(owner)
    history = DurableHistoryProvider(skip_excluded=False)
    working: dict[str, Any] = {}
    with _bound(owner) as binding:
        source = (await history.get_messages("quality", state=working))[0]
        assert source.message_id == source._durable_history_id == "same"  # type: ignore[attr-defined]
        source.additional_properties["_excluded"] = True
        backlink = {"_summarized_by_summary_id": candidate_id}
        links = {"_summary_of_message_ids": ["same"]}
        if grouped:
            source.additional_properties["_group"] = {"id": "source-group", **backlink}
            properties: dict[str, Any] = {"_group": {"id": f"group_{candidate_id}", **links}}
        else:
            source.additional_properties.update(backlink)
            properties = links
        summary = Message("assistant", ["summary"], message_id=candidate_id, additional_properties=properties)
        assert not hasattr(summary, "_durable_history_id")
        working["messages"].insert(0 if prepend else 1, summary)
        history.flush(working)
        snapshot = _json(owner.state.to_dict())
        history.flush(working)
        assert _json(owner.state.to_dict()) == snapshot and binding.append_ordinal == 1
        assert source.message_id == "same"

    cold, replay = await _cold_replay(owner, controls)
    summary_id = REVISION if candidate_id == "same" else candidate_id
    expected_ids = [summary_id, "same"] if prepend else ["same", summary_id]
    rows = [message.to_dict() for entry in cold.state.data.conversation_history for message in entry.messages]
    assert [row["messageId"] for row in rows] == [message.message_id for message in replay] == expected_ids
    expected_source: dict[str, Any] = {"_excluded": True}
    expected_backlink = {"_summarized_by_summary_id": summary_id}
    expected_summary: dict[str, Any] = {"_summary_of_message_ids": ["same"]}
    if grouped:
        expected_source["_group"] = {"id": "source-group", **expected_backlink}
        expected_summary = {"_group": {"id": f"group_{summary_id}", **expected_summary}}
    else:
        expected_source.update(expected_backlink)
    expected = {"same": expected_source, summary_id: expected_summary}
    assert _json({row["messageId"]: row["extensionData"] for row in rows}) == _json(expected)
    assert _json({message.message_id: message.additional_properties for message in replay}) == _json(expected)


async def test_same_flush_source_is_indexed_after_allocation_before_a_later_summary() -> None:
    owner = _owner([_request("seed", _stored("unrelated", message_id="same"))])
    controls = _controls(owner)
    source = Message(
        "user", ["new source"], message_id="new-source", additional_properties={"_summarized_by_summary_id": "same"}
    )
    summary = Message(
        "assistant", ["summary"], message_id="same", additional_properties={"_summary_of_message_ids": ["new-source"]}
    )
    history = DurableHistoryProvider(skip_excluded=False)
    working: dict[str, Any] = {}
    with _bound(owner) as binding:
        await history.get_messages("quality", state=working)
        assert not hasattr(source, "_durable_history_id") and not hasattr(summary, "_durable_history_id")
        working["messages"].extend([source, summary])
        history.flush(working)
        snapshot = _json(owner.state.to_dict())
        history.flush(working)
        assert binding.append_ordinal == 2 and _json(owner.state.to_dict()) == snapshot
    cold, replay = await _cold_replay(owner, controls)
    summary_id = "durable_revision_compaction_current_1_0"
    assert [message.message_id for message in replay] == ["same", "new-source", summary_id]
    assert replay[1].additional_properties == {"_summarized_by_summary_id": summary_id}
    assert replay[2].additional_properties == {"_summary_of_message_ids": ["new-source"]}
    stored_source = cold.state.to_dict()["data"]["conversationHistory"][1]["messages"][0]
    assert stored_source["extensionData"] == {"_summarized_by_summary_id": summary_id}


@pytest.mark.parametrize("summary", [True, False], ids=["summary-revision", "ordinary-working-only"])
@pytest.mark.parametrize(
    ("original_metadata", "updated_metadata", "changed"),
    [
        pytest.param({"quality": False}, {"quality": 0}, True, id="false-to-zero"),
        pytest.param({"quality": 0}, {"quality": 0.0}, True, id="int-to-float"),
        pytest.param({"quality": 0.0}, {"quality": -0.0}, True, id="signed-zero"),
        pytest.param({"nested": [False, None]}, {"nested": [0, None]}, True, id="nested-type"),
        pytest.param({}, {"quality": None}, True, id="absent-to-null"),
        pytest.param({"quality": False}, {"quality": False}, False, id="unchanged"),
        pytest.param({"quality": False}, {"quality": "revised"}, True, id="value-change-control"),
    ],
)
async def test_loaded_content_metadata_revision_is_json_exact(
    summary: bool, original_metadata: dict[str, Any], updated_metadata: dict[str, Any], changed: bool
) -> None:
    item = Message(
        "assistant" if summary else "user",
        [{"type": "text", "text": "retained", "additional_properties": deepcopy(original_metadata)}],
        message_id="item",
        additional_properties={"_summary_of_message_ids": ["source"]} if summary else {},
    )
    stored = DurableAgentStateMessage.from_chat_message(item)
    if not changed:
        stored.set_history_id("private-item")  # The transient marker must not create a public-payload revision.
    original_contents = _json(stored.to_dict()["contents"])
    owner = _owner([
        _request("seed", _stored("source", message_id="source")),
        DurableAgentStateCompaction(OLD, [stored]) if summary else _request("ordinary", stored),
    ])
    controls = _controls(owner)
    history = DurableHistoryProvider(skip_excluded=False)
    working: dict[str, Any] = {}
    expected_revision = summary and changed
    with _bound(owner) as binding:
        loaded = await history.get_messages("quality", state=working)
        target = next(message for message in loaded if message.message_id == "item")
        assert _json(target.contents[0].additional_properties) == _json(original_metadata)
        assert target._durable_history_id == ("item" if changed else "private-item")  # type: ignore[attr-defined]
        target.contents[0].additional_properties = deepcopy(updated_metadata)
        target.additional_properties["annotation"] = {"keep": [False, 0, None]}
        assert _json(target.to_dict()["contents"][0]["additional_properties"]) == _json(updated_metadata)
        if changed and original_metadata == updated_metadata:
            assert _json(original_metadata) != _json(updated_metadata), "Exercise the Python equality trap"
        history.flush(working)
        snapshot = _json(owner.state.to_dict())
        history.flush(working)
        assert _json(owner.state.to_dict()) == snapshot and binding.append_ordinal == int(expected_revision)
        assert _json(stored.to_dict()["contents"]) == original_contents

    cold, replay = await _cold_replay(owner, controls)
    rows = [message.to_dict() for entry in cold.state.data.conversation_history for message in entry.messages]
    stored_metadata = {
        row["messageId"]: row["contents"][0]["pythonCoreFields"]["fields"]["additional_properties"]
        for row in rows
        if row["messageId"] != "source"
    }
    replay_metadata = {
        message.message_id: message.contents[0].additional_properties
        for message in replay
        if message.message_id != "source"
    }
    expected = {"item": original_metadata, **({REVISION: updated_metadata} if expected_revision else {})}
    assert _json(stored_metadata) == _json(replay_metadata) == _json(expected)
    for message_id, metadata in expected.items():
        for key, value in metadata.items():
            assert type(stored_metadata[message_id][key]) is type(replay_metadata[message_id][key]) is type(value)
    current_id = REVISION if expected_revision else "item"
    assert next(row for row in rows if row["messageId"] == current_id)["extensionData"]["annotation"] == {
        "keep": [False, 0, None]
    }


class _MutableFlagsHistory(OrdinaryExternalHistory):
    def __init__(self, initial: tuple[bool, bool], updated: tuple[bool, bool] | None, hook: str) -> None:
        super().__init__("external", store_inputs=initial[0], store_outputs=initial[1])
        self.updated = updated
        self.hook = hook
        self.saved = [Message("user", ["external prior"], message_id="prior")]
        self.batches: list[list[str]] = []
        self.before_flags: list[tuple[bool, bool]] = []

    def _change_flags(self, hook: str) -> None:
        if self.hook == hook and self.updated is not None:
            self.store_inputs, self.store_outputs = self.updated

    async def before_run(self, **kwargs: Any) -> None:
        self._change_flags("before")
        self.before_flags.append((self.store_inputs, self.store_outputs))
        await super().before_run(**kwargs)

    def _get_context_messages_to_store(self, context: SessionContext) -> list[Message]:
        self._change_flags("selection")
        return super()._get_context_messages_to_store(context)

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.batches.append([message.text for message in messages])
        self._change_flags("save")
        await super().save_messages(session_id, messages, **kwargs)


@pytest.mark.parametrize(
    ("initial", "updated", "hook", "per_call", "expected"),
    [
        pytest.param((True, False), (False, False), "before", False, [], id="before-input-off"),
        pytest.param((False, False), (True, False), "before", False, [["question"]], id="before-input-on"),
        pytest.param((False, True), (False, False), "before", True, [], id="before-output-off"),
        pytest.param((False, False), (False, True), "before", True, [["answer-1"]], id="before-output-on"),
        pytest.param((True, True), None, "before", False, [["question", "answer-1"]], id="unchanged"),
        pytest.param((True, False), (False, True), "selection", False, [["answer-1"]], id="selection-flips"),
        pytest.param((False, True), (True, False), "selection", True, [["question"]], id="selection-reverses"),
        pytest.param((True, False), (False, True), "save", True, [["question"], ["answer-2"]], id="save-next-call"),
    ],
)
async def test_external_current_store_flags_match_bare_core(
    initial: tuple[bool, bool], updated: tuple[bool, bool] | None, hook: str, per_call: bool, expected: list[list[str]]
) -> None:
    observations: list[str] = []
    for wrapped in (False, True):
        primary = _MutableFlagsHistory(initial, updated, hook)
        assert type(primary).after_run is HistoryProvider.after_run
        tool_calls = hook == "save"
        client = ToolChatClient(tool_calls=tool_calls, response_message_id="answer-public")
        agent = Agent(
            client=client,
            name="flag-parity",
            tools=[lookup] if tool_calls else [],
            context_providers=[primary],
            require_per_service_call_history_persistence=per_call,
        )
        providers = agent.context_providers
        session = agent.create_session(session_id="quality")
        current = Message("user", ["question"], message_id="current-public")
        current._durable_ingestion_receipt = RECEIPT  # type: ignore[attr-defined]
        input_before, prior_before = _json(current.to_dict()), _json(primary.saved[0].to_dict())
        owner = _owner()
        state_before = _json(owner.state.to_dict())
        if wrapped:
            with _bound(owner) as binding:
                prepared = prepare_history_owner(ensure_durable_history(agent), False)
                assert isinstance(prepared, Agent) and prepared is not agent
                assert prepared.context_providers[0].__wrapped__ is primary  # type: ignore[attr-defined]
                assert (primary.store_inputs, primary.store_outputs) == initial and primary.before_flags == []
                response = await prepared.run(current, session=session)
                assert binding.accepted_inputs == {RECEIPT}
        else:
            assert current_durable_history_binding() is None
            response = await agent.run(current, session=session)
        # The bare arm must establish the contract before checking the wrapper.
        assert primary.batches == expected
        assert response.text == ("answer-2" if tool_calls else "answer-1")
        assert len(client.received_messages) == (2 if tool_calls else 1)
        assert [message.text for message in client.received_messages[0]] == ["external prior", "question"]
        assert (primary.store_inputs, primary.store_outputs) == (updated if updated is not None else initial)
        if tool_calls:
            assert primary.before_flags == [initial, updated]
        assert _json(current.to_dict()) == input_before and _json(primary.saved[0].to_dict()) == prior_before
        assert _json(owner.state.to_dict()) == state_before and owner.persist_count == 0
        assert agent.context_providers is providers and providers == [primary]
        observations.append(
            _json({
                "saved": [message.to_dict() for message in primary.saved],
                "model": [[message.to_dict() for message in batch] for batch in client.received_messages],
            })
        )
    assert observations[1] == observations[0]


async def test_custom_hook_mutations_do_not_make_canonical_audit_the_primary(monkeypatch: pytest.MonkeyPatch) -> None:
    primary = _MutableFlagsHistory((True, True), (False, False), "before")
    audit = DurableHistoryProvider("audit")
    audit.load_messages = False
    calls: list[AgentSession] = []

    async def custom_after(
        *, context: SessionContext, session: AgentSession, state: dict[str, Any], **kwargs: Any
    ) -> None:
        calls.append(session)
        primary.store_inputs = True
        await primary.save_messages(context.session_id, context.input_messages, state=state)

    monkeypatch.setattr(primary, "after_run", custom_after)
    agent = Agent(client=ToolChatClient(tool_calls=False), context_providers=[primary, audit])
    session = agent.create_session(session_id="quality")
    current = Message("user", ["question"], message_id="current-public")
    current._durable_ingestion_receipt = RECEIPT  # type: ignore[attr-defined]
    owner = _owner()
    controls = _controls(owner)
    with _bound(owner) as binding:
        prepared = prepare_history_owner(ensure_durable_history(agent), False)
        assert isinstance(prepared, Agent) and prepared.context_providers[1] is audit
        await prepared.run(current, session=session)
        audit.flush(session.state[audit.source_id])
        assert binding.accepted_inputs == set(), "Neither an opaque hook nor an audit can invent primary acceptance"
    assert calls == [session] and primary.after_run is custom_after
    assert (primary.store_inputs, primary.store_outputs, audit.load_messages) == (True, False, False)
    assert primary.batches == [["question"]]
    cold, replay = await _cold_replay(owner, controls)
    assert [message.text for message in replay] == ["question", "answer-1"]
    assert all(
        message.ingestion_occurrence is None
        for entry in cold.state.data.conversation_history
        for message in entry.messages
    )


@pytest.mark.parametrize(
    ("created_at", "expected"),
    [
        ("2026-01-02T03:04:05", "2026-01-02T03:04:05+00:00"),
        (datetime(2026, 1, 2, 3, 4, 5), "2026-01-02T03:04:05+00:00"),
        ("2026-01-02T03:04:05.123456", "2026-01-02T03:04:05.123456+00:00"),
        ("2026-01-02T03:04:05.123456789+05:30", "2026-01-02T03:04:05.123456789+05:30"),
        (None, None),
        ("not-a-timestamp", None),
    ],
)
async def test_normal_response_append_matches_direct_timestamp_contract(created_at: Any, expected: str | None) -> None:
    response = AgentResponse(messages=[Message("assistant", ["answer"], message_id="answer")], created_at=created_at)
    owner = _owner()
    controls = _controls(owner)
    history = DurableHistoryProvider(store_inputs=False)
    context = SessionContext(session_id="quality", input_messages=[])
    context._response = response
    working: dict[str, Any] = {}
    lower = datetime.now(tz=timezone.utc)
    direct = DurableAgentStateResponse.from_run_response("current", response).to_dict()["createdAt"]
    with _bound(owner):
        await history.after_run(agent=object(), session=AgentSession(), context=context, state=working)
        history.flush(working)
    upper = datetime.now(tz=timezone.utc)
    cold, replay = await _cold_replay(owner, controls)
    entries = cold.state.to_dict()["data"]["conversationHistory"]
    assert len(entries) == 1 and entries[0]["$type"] == "response"
    assert [message.message_id for message in replay] == ["answer"]
    assert type(response.created_at) is type(created_at) and response.created_at == created_at
    actual = entries[0]["createdAt"]
    if expected is not None:
        assert direct == expected and actual == expected
    else:
        for timestamp in (direct, actual):
            parsed = datetime.fromisoformat(timestamp)
            assert parsed.utcoffset() == timedelta(0) and lower <= parsed <= upper
