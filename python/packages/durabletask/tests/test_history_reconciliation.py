# Copyright (c) Microsoft. All rights reserved.

"""Focused reconciliation, mutable Core hook settings and append timestamp regressions."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from agent_framework import Agent, AgentResponse, AgentSession, HistoryProvider, Message, SessionContext
from test_history_flush_atomicity import _reference_check
from test_private_history_pipeline import ToolChatClient, _bound, _CanonicalStateProvider, _request, _stored, lookup
from test_shared_history_provider import OrdinaryExternalHistory

from agent_framework_durabletask._history_provider import (
    DurableHistoryBinding,
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


@pytest.mark.parametrize("grouped", [False, True], ids=["top-level", "core-group"])
@pytest.mark.parametrize("source_first", [True, False], ids=["source-before-summary", "summary-before-source"])
@pytest.mark.parametrize("source_is_summary", [False, True], ids=["new-message-source", "new-summary-source"])
async def test_same_flush_source_backlinks_do_not_depend_on_candidate_order(
    grouped: bool, source_first: bool, source_is_summary: bool
) -> None:
    original = _stored("unrelated", message_id="same")
    original_before = _json(original.to_dict())
    owner = _owner([_request("seed", original)])
    controls = _controls(owner)
    before = _json(owner.state.to_dict())
    source_properties: dict[str, Any] = {"_summarized_by_summary_id": "same"}
    summary_properties: dict[str, Any] = {"_summary_of_message_ids": ["new-source"]}
    if source_is_summary:
        source_properties["_summary_of_message_ids"] = []
    if grouped:
        source_properties = {"_group": {"id": "source-group", **source_properties}}
        summary_properties = {"_group": {"id": "group_same", **summary_properties}}
    source = Message(
        "assistant" if source_is_summary else "user",
        ["new source"],
        message_id="new-source",
        additional_properties=source_properties,
    )
    summary = Message("assistant", ["summary"], message_id="same", additional_properties=summary_properties)
    source_annotations, source_contents = source.additional_properties, source.contents
    source_before = deepcopy(source.to_dict())
    history = DurableHistoryProvider(skip_excluded=False)
    working: dict[str, Any] = {}
    with _bound(owner) as binding:
        loaded = await history.get_messages("quality", state=working)
        buffer = working["messages"]
        assert not hasattr(source, "_durable_history_id") and not hasattr(summary, "_durable_history_id")
        pending = [source, summary] if source_first else [summary, source]
        buffer.extend(pending)
        assert _json(owner.state.to_dict()) == before and binding.append_ordinal == 0
        history.flush(working)
        snapshot = _json(owner.state.to_dict())
        history.flush(working)
        assert binding.append_ordinal == 2 and _json(owner.state.to_dict()) == snapshot
        assert working["messages"] is buffer and buffer[0] is loaded[0]
        assert all(actual is expected for actual, expected in zip(buffer[1:], pending, strict=True))
        assert source.additional_properties is source_annotations and source.contents is source_contents
        assert source.message_id == source._durable_history_id == "new-source"  # type: ignore[attr-defined]
        assert _json(original.to_dict()) == original_before
    cold, replay = await _cold_replay(owner, controls)
    summary_id = "durable_revision_compaction_current_1_0" if source_first else REVISION
    expected_ids = ["same", *(["new-source", summary_id] if source_first else [summary_id, "new-source"])]
    stored = [message for entry in cold.state.data.conversation_history for message in entry.messages]
    assert [message.message_id for message in stored] == [message.message_id for message in replay] == expected_ids
    assert len(set(expected_ids)) == len(stored) == 3
    expected_source: dict[str, Any] = {"_summarized_by_summary_id": summary_id}
    expected_summary: dict[str, Any] = {"_summary_of_message_ids": ["new-source"]}
    if source_is_summary:
        expected_source["_summary_of_message_ids"] = []
    if grouped:
        expected_source = {"_group": {"id": "source-group", **expected_source}}
        expected_summary = {"_group": {"id": f"group_{summary_id}", **expected_summary}}
    source_before["additional_properties"] = expected_source
    assert _json(source.to_dict()) == _json(source_before)
    expected = {"same": {}, "new-source": expected_source, summary_id: expected_summary}
    assert _json({message.message_id: message.extension_data or {} for message in stored}) == _json(expected)
    assert _json({message.message_id: message.additional_properties for message in replay}) == _json(expected)


@pytest.mark.parametrize("grouped", [False, True], ids=["top-level", "core-group"])
@pytest.mark.parametrize("source_first", [True, False], ids=["source-before-summary", "summary-before-source"])
@pytest.mark.parametrize("pending_summary", [False, True], ids=["ordinary-candidate", "summary-candidate"])
async def test_loaded_source_wins_over_new_candidates_with_the_same_public_id(
    grouped: bool, source_first: bool, pending_summary: bool
) -> None:
    def annotations(group_id: str, **links: Any) -> dict[str, Any]:
        return {"_group": {"id": group_id, **links}} if grouped else links

    old_source = _stored("old source", message_id="old-source")
    old_source.extension_data = annotations("old-source-group", _summarized_by_summary_id="same")
    old_summary = _stored("old summary", role="assistant", message_id="same")
    old_summary.extension_data = annotations("group_same", _summary_of_message_ids=["old-source"])
    original = _stored("existing source", message_id="new-source")
    original.extension_data = annotations("existing-group", _summarized_by_summary_id="same")
    old_lineage = _json([old_source.to_dict(), old_summary.to_dict()])
    owner = _owner([_request("seed", old_source, old_summary, original)])
    controls = _controls(owner)
    pending_links: dict[str, Any] = {"_summarized_by_summary_id": "same"}
    if pending_summary:
        pending_links["_summary_of_message_ids"] = []
    source = Message(
        "assistant" if pending_summary else "user",
        ["pending source"],
        message_id="new-source",
        additional_properties=annotations("pending-group", **pending_links),
    )
    pending_before = deepcopy(source.additional_properties)
    summary = Message(
        "assistant",
        ["new summary"],
        message_id="same",
        additional_properties=annotations("group_same", _summary_of_message_ids=["new-source"]),
    )
    history = DurableHistoryProvider(skip_excluded=False)
    working: dict[str, Any] = {}
    with _bound(owner) as binding:
        loaded = await history.get_messages("quality", state=working)
        assert loaded[2]._durable_history_id == "new-source"  # type: ignore[attr-defined]
        assert not hasattr(source, "_durable_history_id") and not hasattr(summary, "_durable_history_id")
        working["messages"].extend([source, summary] if source_first else [summary, source])
        history.flush(working)
        snapshot = _json(owner.state.to_dict())
        history.flush(working)
        assert binding.append_ordinal == 2 and _json(owner.state.to_dict()) == snapshot
    cold, replay = await _cold_replay(owner, controls)
    second_id = "durable_revision_compaction_current_1_0"
    source_id, summary_id = (REVISION, second_id) if source_first else (second_id, REVISION)
    source_public_id = source_id if pending_summary else "new-source"
    assert source.message_id == source_public_id
    assert source._durable_history_id == source_id  # type: ignore[attr-defined]
    if grouped and pending_summary:
        pending_before["_group"]["id"] = f"group_{source_id}"
    assert source.additional_properties == pending_before
    assert original.extension_data == annotations("existing-group", _summarized_by_summary_id=summary_id)
    assert _json([old_source.to_dict(), old_summary.to_dict()]) == old_lineage
    stored = [message for entry in cold.state.data.conversation_history for message in entry.messages]
    expected_ids = [
        "old-source",
        "same",
        "new-source",
        *([source_id, summary_id] if source_first else [summary_id, source_id]),
    ]
    expected_public = [
        "old-source",
        "same",
        "new-source",
        *([source_public_id, summary_id] if source_first else [summary_id, source_public_id]),
    ]
    assert [message.message_id for message in stored] == expected_ids and len(set(expected_ids)) == 5
    assert [message.public_message_id for message in stored] == expected_public
    assert [message.message_id for message in replay] == expected_public
    assert _json([message.to_dict() for message in stored[:2]]) == old_lineage
    assert stored[2].extension_data == replay[2].additional_properties == original.extension_data
    pending_index = 3 if source_first else 4
    assert stored[pending_index].extension_data == replay[pending_index].additional_properties == pending_before


@pytest.mark.parametrize("grouped", [False, True], ids=["top-level", "core-group"])
@pytest.mark.parametrize("source_first", [True, False], ids=["source-before-summary", "summary-before-source"])
@pytest.mark.parametrize(
    "error_type",
    [None, RuntimeError, asyncio.CancelledError, GeneratorExit],
    ids=["success", "repair-error", "repair-cancel", "repair-close"],
)
async def test_loaded_summary_revision_repairs_the_original_named_occurrence(
    grouped: bool,
    source_first: bool,
    error_type: type[BaseException] | None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def annotations(group_id: str, **links: Any) -> dict[str, Any]:
        return {"_group": {"id": group_id, **links}} if grouped else links

    def links(properties: dict[str, Any]) -> dict[str, Any]:
        return properties["_group"] if grouped else properties

    leaf = _stored("leaf", message_id="leaf-public")
    leaf.set_history_id("leaf")
    leaf.extension_data = annotations("leaf-group", _summarized_by_summary_id="source")
    source = _stored("stored source summary", role="assistant", message_id="source")
    source.extension_data = {
        **annotations("source-group", _summary_of_message_ids=["leaf"], _summarized_by_summary_id="same"),
        "opaque": {"values": [False, 0, 0.0, None]},
    }
    previous = _stored("previous summary", role="assistant", message_id="same")
    previous.extension_data = annotations("group_same", _summary_of_message_ids=["source"])
    original_leaf, original_source = deepcopy(leaf.to_dict()), deepcopy(source.to_dict())
    original_previous = _json(previous.to_dict())
    owner = _owner([_request("seed", leaf), DurableAgentStateCompaction(OLD, [source, previous])])
    owner.state.unknown_fields["futureRoot"] = {"keep": [False, 0.0]}
    owner.state.data.unknown_fields["futureData"] = {"keep": [0, None]}
    owner.state.record_response(
        "finished",
        AgentResponse(messages=[Message("assistant", ["retained result"], message_id="result-public")]),
        delivery_window_seconds=3600,
        now=OLD,
    )
    controls = _controls(owner)
    history = DurableHistoryProvider(skip_excluded=False)
    working: dict[str, Any] = {}
    summary = Message(
        "assistant",
        ["new summary"],
        message_id="same",
        additional_properties=annotations("group_same", _summary_of_message_ids=["source"]),
    )
    second_id = "durable_revision_compaction_current_1_0"
    source_revision_id, summary_id = (REVISION, second_id) if source_first else (second_id, REVISION)
    with _bound(owner) as binding:
        loaded = await history.get_messages("quality", state=working)
        loaded_source = loaded[1]
        assert [message.message_id for message in loaded] == ["leaf-public", "source", "same"]
        assert loaded_source._durable_history_id == "source"  # type: ignore[attr-defined]
        assert links(loaded_source.additional_properties)["_summarized_by_summary_id"] == "same"
        loaded_source.contents[0].text = "revised source summary"
        source_annotations, source_contents = loaded_source.additional_properties, loaded_source.contents
        buffer = working["messages"]
        buffer.insert(2 if source_first else 0, summary)
        assert not hasattr(summary, "_durable_history_id")
        assert _json(source.to_dict()) == _json(original_source)

        if error_type is not None:
            error = error_type("after the original source backlink was repaired")
            original_repair = history._repair_summary_links
            reached: list[str] = []

            def fail_after_repair(*args: Any, **kwargs: Any) -> None:
                original_repair(*args, **kwargs)
                if args[0] is summary:
                    assert source.extension_data is not None
                    assert links(source.extension_data)["_summarized_by_summary_id"] == summary_id
                    assert links(loaded_source.additional_properties)["_summarized_by_summary_id"] == "same"
                    assert binding.append_ordinal == 2
                    reached.append(summary_id)
                    raise error

            objects: list[Any] = [owner, owner.state, owner.state.data, binding, *buffer]
            for entry in owner.state.data.conversation_history:
                objects.extend((entry, *entry.messages))
                objects.extend(content for message in entry.messages for content in message.contents)
            objects.extend(content for message in buffer for content in message.contents)
            check_references = _reference_check(working, *(vars(value) for value in objects))
            before = _json(owner.state.to_dict())
            working_before = _json([message.to_dict() for message in buffer])
            with monkeypatch.context() as patch:
                patch.setattr(history, "_repair_summary_links", fail_after_repair)
                for _ in range(2):
                    try:
                        with pytest.raises(error_type) as caught:
                            history.flush(working)
                        assert caught.value is error
                    finally:
                        check_references()
                        assert _json(owner.state.to_dict()) == before
                        assert _json([message.to_dict() for message in buffer]) == working_before
                        assert loaded_source._durable_history_id == "source"  # type: ignore[attr-defined]
                        assert not hasattr(summary, "_durable_history_id")
                        assert binding.append_ordinal == 0 and owner.persist_count == 0
            assert reached == [summary_id, summary_id]

        history.flush(working)
        snapshot = _json(owner.state.to_dict())
        history.flush(working)
        assert _json(owner.state.to_dict()) == snapshot and binding.append_ordinal == 2
        assert working["messages"] is buffer
        assert any(message is loaded_source for message in buffer) and any(message is summary for message in buffer)
        assert loaded_source.additional_properties is source_annotations and loaded_source.contents is source_contents
        assert loaded_source.message_id == source_revision_id
        assert loaded_source._durable_history_id == source_revision_id  # type: ignore[attr-defined]
        assert summary.message_id == summary._durable_history_id == summary_id  # type: ignore[attr-defined]

    expected_source = deepcopy(original_source)
    links(expected_source["extensionData"])["_summarized_by_summary_id"] = summary_id
    expected_source["extensionData"]["_excluded"] = True
    expected_leaf = deepcopy(original_leaf)
    links(expected_leaf["extensionData"])["_summarized_by_summary_id"] = source_revision_id
    expected_revision = deepcopy(original_source["extensionData"])
    if grouped:
        expected_revision["_group"]["id"] = f"group_{source_revision_id}"
    expected_annotations = {
        "leaf": expected_leaf["extensionData"],
        "source": expected_source["extensionData"],
        "same": previous.extension_data,
        source_revision_id: expected_revision,
        summary_id: annotations(f"group_{summary_id}", _summary_of_message_ids=["source"]),
    }
    expected_order = (
        ["leaf", source_revision_id, summary_id, "source", "same"]
        if source_first
        else [summary_id, "leaf", source_revision_id, "source", "same"]
    )
    assert _json(source.to_dict()) == _json(expected_source)
    assert _json(leaf.to_dict()) == _json(expected_leaf) and _json(previous.to_dict()) == original_previous
    assert _json(loaded_source.additional_properties) == _json(expected_revision)
    assert _controls(owner) == controls and owner.persist_count == 0
    cold, replay = await _cold_replay(owner, controls)
    stored = [message for entry in cold.state.data.conversation_history for message in entry.messages]
    assert [message.message_id for message in stored] == expected_order and len(stored) == 5
    expected_public_ids = ["leaf-public" if message_id == "leaf" else message_id for message_id in expected_order]
    assert (
        [message.public_message_id for message in stored]
        == [message.message_id for message in replay]
        == (expected_public_ids)
    )
    assert _json({message.message_id: message.extension_data for message in stored}) == _json(expected_annotations)
    replay_annotations = {
        getattr(message, "_durable_history_id", None): message.additional_properties for message in replay
    }
    assert _json(replay_annotations) == _json(expected_annotations)
    assert {message.message_id: message.text for message in stored} == {
        "leaf": "leaf",
        "source": "stored source summary",
        "same": "previous summary",
        source_revision_id: "revised source summary",
        summary_id: "new summary",
    }
    assert _json(cold.state.to_dict()) == snapshot


@pytest.mark.parametrize("error_type", [RuntimeError, asyncio.CancelledError, GeneratorExit])
async def test_deferred_backlink_failure_restores_retained_aliases_before_retry(
    error_type: type[BaseException],
) -> None:
    owner = _owner([_request("seed", _stored("unrelated", message_id="same"))])
    source = Message(
        "user",
        ["new source"],
        message_id="new-source",
        additional_properties={
            "_summarized_by_summary_id": "same",
            "_group": {"id": "source-group", "_summarized_by_summary_id": "same"},
        },
    )
    summary = Message(
        "assistant",
        ["summary"],
        message_id="same",
        additional_properties={"_group": {"id": "group_same", "_summary_of_message_ids": ["new-source"]}},
    )
    error = error_type("after deferred backlink repair")
    reached: list[bool] = []

    class FailAfterFlush(DurableHistoryProvider):
        def _flush(self, binding: DurableHistoryBinding, state: dict[str, Any], buffer: list[Message]) -> None:
            super()._flush(binding, state, buffer)
            assert source.additional_properties["_summarized_by_summary_id"] == REVISION
            assert source.additional_properties["_group"]["_summarized_by_summary_id"] == REVISION
            assert summary.additional_properties["_group"]["id"] == f"group_{REVISION}"
            assert binding.append_ordinal == 2
            reached.append(True)
            raise error

    history = FailAfterFlush(skip_excluded=False)
    working: dict[str, Any] = {}
    with _bound(owner) as binding:
        await history.get_messages("quality", state=working)
        buffer = working["messages"]
        buffer.extend([summary, source])
        objects: list[Any] = [owner, owner.state, owner.state.data, binding, *buffer]
        for entry in owner.state.data.conversation_history:
            objects.extend((entry, *entry.messages))
        check_references = _reference_check(working, *(vars(value) for value in objects))
        before = _json(owner.state.to_dict())
        working_before = _json([message.to_dict() for message in buffer])
        for _ in range(2):
            try:
                with pytest.raises(error_type) as caught:
                    history.flush(working)
                assert caught.value is error
            finally:
                check_references()
                assert _json(owner.state.to_dict()) == before
                assert _json([message.to_dict() for message in buffer]) == working_before
                assert binding.append_ordinal == 0 and owner.persist_count == 0
                assert not hasattr(source, "_durable_history_id") and not hasattr(summary, "_durable_history_id")
    assert reached == [True, True]


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
