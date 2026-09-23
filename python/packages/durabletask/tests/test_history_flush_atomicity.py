# Copyright (c) Microsoft. All rights reserved.

"""Flush-local rejection invariants, separate from the owner's backend transaction."""

import asyncio
import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from copy import deepcopy
from typing import Any, cast
from unittest.mock import Mock

import pytest
from _history_atomicity_test_support import _owner, _reference_check, _snapshot, _summary
from _shared_history_test_support import _bound, _CanonicalStateProvider, _request, _stored
from agent_framework import (
    GROUP_ANNOTATION_KEY,
    GROUP_ID_KEY,
    SUMMARIZED_BY_SUMMARY_ID_KEY,
    SUMMARY_OF_MESSAGE_IDS_KEY,
    Message,
)

from agent_framework_durabletask import _history_provider as history_module
from agent_framework_durabletask._history_provider import (
    EXCLUDED_KEY,
    POSITIONS_KEY,
    WORKING_BUFFER_KEY,
    DurableHistoryBinding,
    DurableHistoryProvider,
)
from agent_framework_durabletask._response_utils import serialize_input_message
from agent_framework_durabletask._shared_agent_state import (
    DurableAgentStateEntry,
    DurableAgentStateMessage,
)


@contextmanager
def _unchanged_on_rejection(
    owner: _CanonicalStateProvider, working: dict[str, Any], binding: DurableHistoryBinding
) -> Iterator[None]:
    canonical = owner.state
    captured = canonical.to_dict()
    before = _snapshot(captured)
    stored_messages = [message for entry in canonical.data.conversation_history for message in entry.messages]
    identities = [(message.message_id, message.public_message_id) for message in stored_messages]
    objects: list[Any] = [owner, canonical, canonical.data, binding]
    for entry in canonical.data.conversation_history:
        objects.append(entry)
        for stored in entry.messages:
            objects.extend((stored, *stored.contents))
    for message in working[WORKING_BUFFER_KEY]:
        objects.extend((message, *message.contents))
    check_references = _reference_check(working, *(vars(value) for value in objects))
    try:
        yield
    finally:
        # Check even if the exception differs from the expected rejection. Value-only
        # restoration by replacing owner.state would leave retained aliases dirty.
        assert owner.persist_count == 0
        assert owner.state is canonical
        assert [(message.message_id, message.public_message_id) for message in stored_messages] == identities
        check_references()
        assert _snapshot(canonical.to_dict()) == before
        assert _snapshot(captured) == before


def _invalid_message() -> Message:
    # A direct object-valued metadata member can be skipped by Core serialization.
    # A nested dictionary survives that projection and reaches durable JSON admission.
    message = Message("assistant", ["invalid strategy output"], additional_properties={"payload": {"bad": object()}})
    payload = serialize_input_message(message)
    assert type(payload["additional_properties"]["payload"]["bad"]) is object
    with pytest.raises(TypeError, match="not JSON serializable"):
        json.dumps(payload, allow_nan=False)
    with pytest.raises(TypeError, match="not JSON serializable"):
        DurableAgentStateMessage.from_chat_message(deepcopy(message))
    return message


@pytest.mark.parametrize("identity", ["anonymous", "duplicate", "unique"])
@pytest.mark.parametrize("valid_prefix", [False, True], ids=["invalid-first", "valid-summary-first"])
def test_flush_invalid_working_message_leaves_repairs_insertions_and_binding_unchanged(
    identity: str, valid_prefix: bool
) -> None:
    owner = _owner(identity)
    buffer = ([_summary()] if valid_prefix else []) + [_invalid_message()]
    working: dict[str, Any] = {
        WORKING_BUFFER_KEY: buffer,
        POSITIONS_KEY: {},
        "caller": {"keep": [None, False, 0, 0.0]},
    }
    # Do not call get_messages first: that would repair the anonymous/duplicate IDs
    # before the operation whose atomicity is under test.
    with _bound(owner) as binding:
        binding.append_ordinal = 7
        for _ in range(2):
            with (
                _unchanged_on_rejection(owner, working, binding),
                pytest.raises(TypeError, match="not JSON serializable"),
            ):
                DurableHistoryProvider().flush(working)


class _CannotCopy:
    def __init__(self, error: BaseException) -> None:
        self.error = error

    def __deepcopy__(self, memo: dict[int, Any]) -> Any:
        raise self.error


@pytest.mark.parametrize("failure", ["copy", "non-json"])
async def test_flush_late_annotation_failure_restores_split_entries_and_stale_buffer(failure: str) -> None:
    owner = _owner("unique")
    provider = DurableHistoryProvider()
    working: dict[str, Any] = {}
    error = RuntimeError("annotation copy failed")
    with _bound(owner) as binding:
        await provider.get_messages("session", state=working)
        buffer = working[WORKING_BUFFER_KEY]
        first = buffer[0]
        second = buffer[1]
        first.additional_properties[GROUP_ANNOTATION_KEY] = {SUMMARIZED_BY_SUMMARY_ID_KEY: "second"}
        first.additional_properties[SUMMARIZED_BY_SUMMARY_ID_KEY] = "second"
        if failure == "copy":
            second.additional_properties["payload"] = _CannotCopy(error)
        else:
            second.additional_properties["payload"] = {"bad": object()}
        buffer.insert(
            1,
            Message(
                "assistant",
                ["new summary"],
                message_id="second",
                additional_properties={
                    GROUP_ANNOTATION_KEY: {
                        GROUP_ID_KEY: "group_second",
                        SUMMARY_OF_MESSAGE_IDS_KEY: ["first"],
                    }
                },
            ),
        )
        stale = Message("assistant", ["stale occurrence"], message_id="gone")
        cast(Any, stale)._durable_history_id = "gone"
        buffer.insert(0, stale)
        # Reusing second's ID splits the request and rewrites first's summary links
        # before the later annotation copy. Restore lists AND nested metadata aliases.
        with _unchanged_on_rejection(owner, working, binding):
            if failure == "copy":
                with pytest.raises(RuntimeError, match="annotation copy failed") as caught:
                    provider.flush(working)
                assert caught.value is error
            else:
                with pytest.raises((TypeError, ValueError), match="JSON"):
                    provider.flush(working)


async def test_flush_json_valid_unhashable_summary_links_do_not_leave_an_inserted_revision() -> None:
    owner = _owner("unique")
    provider = DurableHistoryProvider()
    working: dict[str, Any] = {}
    with _bound(owner) as binding:
        await provider.get_messages("session", state=working)
        summary = Message(
            "assistant",
            ["reused summary ID"],
            message_id="first",
            additional_properties={SUMMARY_OF_MESSAGE_IDS_KEY: [["first"]]},
        )
        # Strict JSON conversion alone cannot detect malformed summary members.
        # Reject them deliberately, not via set(summary_ids) after insertion.
        json.dumps(serialize_input_message(summary), allow_nan=False)
        DurableAgentStateMessage.from_chat_message(deepcopy(summary))
        working[WORKING_BUFFER_KEY].insert(1, summary)
        with _unchanged_on_rejection(owner, working, binding), pytest.raises((TypeError, ValueError)) as caught:
            provider.flush(working)
        # On the baseline, the reference oracle above fails before this deliberate
        # validation check. It must not be masked by the old unhashable exception.
        assert isinstance(caught.value, ValueError)
        assert str(caught.value) == "Summary links must contain only string message IDs."


@pytest.mark.parametrize("identity", ["anonymous", "duplicate", "unique"])
async def test_valid_summary_stages_once_without_replacing_existing_messages(identity: str) -> None:
    owner = _owner(identity)
    provider = DurableHistoryProvider()
    canonical = owner.state
    data = canonical.data
    history = data.conversation_history
    entry = history[0]
    originals = tuple(entry.messages)
    public_ids = tuple(message.public_message_id for message in originals)
    working: dict[str, Any] = {}
    with _bound(owner) as binding:
        await provider.get_messages("session", state=working)
        summary = _summary()
        buffer = working[WORKING_BUFFER_KEY]
        buffer.insert(1, summary)
        provider.flush(working)
        assert [message.text for part in history for message in part.messages] == ["first", "new summary", "second"]
        assert len(history) == 3
        assert history[0] is entry and entry.messages[0] is originals[0]
        assert history[2].messages[0] is originals[1]
        assert tuple(message.public_message_id for message in originals) == public_ids
        assert summary.message_id and cast(Any, summary)._durable_history_id == summary.message_id
        assert binding.append_ordinal == 1
        after = _snapshot(canonical.to_dict())
        for _ in range(2):
            provider.flush(working)
            assert _snapshot(canonical.to_dict()) == after
            assert binding.append_ordinal == 1
            assert working[WORKING_BUFFER_KEY] is buffer
    assert owner.state is canonical and canonical.data is data and data.conversation_history is history
    assert owner.persist_count == 0


async def test_unique_ids_and_unchanged_buffer_are_a_noop_for_canonical_objects() -> None:
    owner = _owner("unique")
    provider = DurableHistoryProvider()
    canonical = owner.state
    entry = canonical.data.conversation_history[0]
    objects: list[Any] = [owner, canonical, canonical.data, entry, *entry.messages]
    objects.extend(content for message in entry.messages for content in message.contents)
    check_references = _reference_check(*(vars(value) for value in objects))
    before = _snapshot(canonical.to_dict())
    working: dict[str, Any] = {}
    with _bound(owner) as binding:
        await provider.get_messages("session", state=working)
        for _ in range(2):
            provider.flush(working)
            check_references()
            assert _snapshot(canonical.to_dict()) == before
            assert binding.append_ordinal == 0
    assert owner.persist_count == 0


@pytest.mark.parametrize("error_type", [RuntimeError, asyncio.CancelledError, GeneratorExit])
async def test_failure_after_complete_reconciliation_restores_hidden_links_exclusions_and_positions(
    error_type: type[BaseException],
) -> None:
    owner = _owner("unique")
    first, second = owner.state.data.conversation_history[0].messages
    first.extension_data = {
        GROUP_ANNOTATION_KEY: {SUMMARIZED_BY_SUMMARY_ID_KEY: "second"},
        SUMMARIZED_BY_SUMMARY_ID_KEY: "second",
    }
    error = error_type("after final synchronization")
    summary = Message(
        "assistant",
        ["replacement summary"],
        message_id="second",
        additional_properties={
            GROUP_ANNOTATION_KEY: {GROUP_ID_KEY: "group_second", SUMMARY_OF_MESSAGE_IDS_KEY: ["first"]}
        },
    )
    reached: list[bool] = []

    class FailAfterFlush(DurableHistoryProvider):
        def _flush(self, binding: DurableHistoryBinding, state: dict[str, Any], buffer: list[Message]) -> None:
            original_positions = state[POSITIONS_KEY]
            super()._flush(binding, state, buffer)
            assert state[POSITIONS_KEY] is not original_positions
            assert summary.message_id != "second"
            assert first.extension_data is not None
            assert first.extension_data[EXCLUDED_KEY] is True
            assert first.extension_data[SUMMARIZED_BY_SUMMARY_ID_KEY] == summary.message_id
            assert first.extension_data[GROUP_ANNOTATION_KEY][SUMMARIZED_BY_SUMMARY_ID_KEY] == summary.message_id
            assert binding.append_ordinal == 1
            reached.append(True)
            raise error

    provider = FailAfterFlush()
    working: dict[str, Any] = {}
    with _bound(owner) as binding:
        await provider.get_messages("session", state=working)
        buffer = working[WORKING_BUFFER_KEY]
        buffer[:] = [summary, buffer[1]]  # first is a hidden source repaired through stored_by_id.
        for _ in range(2):
            with _unchanged_on_rejection(owner, working, binding), pytest.raises(error_type) as caught:
                provider.flush(working)
            assert caught.value is error
            assert summary.message_id == "second"
            assert not hasattr(summary, "_durable_history_id")
            assert owner.state.data.conversation_history[0].messages[1] is second
    assert reached == [True, True]


async def test_cancelled_annotation_copy_rolls_back_and_preserves_successful_earlier_input_save() -> None:
    owner = _owner("unique")
    provider = DurableHistoryProvider()
    working: dict[str, Any] = {}
    error = asyncio.CancelledError("cancelled during annotation copy")
    supplied = Message("user", ["earlier accepted input"], message_id="accepted")
    receipt = ("accepted-occurrence", "a" * 64)
    cast(Any, supplied)._durable_ingestion_receipt = receipt
    with _bound(owner) as binding:
        await provider.get_messages("session", state=working)
        await provider.save_messages("session", [supplied], state=working)
        binding.accept([supplied])
        binding.pending_inputs.append(supplied)
        assert binding.append_ordinal == 1 and binding.accepted_inputs == {receipt}
        earlier = owner.state.data.conversation_history[-1]
        working[WORKING_BUFFER_KEY].insert(1, _summary())
        working[WORKING_BUFFER_KEY][-1].additional_properties["payload"] = _CannotCopy(error)
        for _ in range(2):
            with _unchanged_on_rejection(owner, working, binding), pytest.raises(asyncio.CancelledError) as caught:
                await provider.save_messages("session", [Message("assistant", ["must not append"])], state=working)
            assert caught.value is error
            assert binding.append_ordinal == 1 and binding.accepted_inputs == {receipt}
            assert binding.pending_inputs == [supplied]
            assert owner.state.data.conversation_history[-1] is earlier
            assert earlier.messages[0].text == "earlier accepted input"
            assert earlier.messages[0].ingestion_occurrence == receipt[0]


@pytest.mark.parametrize("new_message", [False, True], ids=["loaded-annotation", "new-conversion"])
@pytest.mark.parametrize("bad_value", [object(), float("nan"), float("inf"), {1: "bad key"}, (1, 2)])
async def test_actual_written_annotations_require_strict_json(new_message: bool, bad_value: Any) -> None:
    owner = _owner("unique")
    provider = DurableHistoryProvider()
    working: dict[str, Any] = {}
    with _bound(owner) as binding:
        await provider.get_messages("session", state=working)
        # A valid earlier insertion ensures validation is not merely an ID preflight.
        working[WORKING_BUFFER_KEY].insert(1, _summary())
        if new_message:
            target = Message("assistant", ["strategy message"], message_id="new")
            working[WORKING_BUFFER_KEY].append(target)
        else:
            target = working[WORKING_BUFFER_KEY][-1]
        target.additional_properties["payload"] = bad_value
        with _unchanged_on_rejection(owner, working, binding), pytest.raises((TypeError, ValueError)):
            provider.flush(working)


@pytest.mark.parametrize("invalid_content", [False, True])
async def test_loaded_non_summary_content_edits_are_not_persisted_or_newly_validated(invalid_content: bool) -> None:
    owner = _owner("unique")
    provider = DurableHistoryProvider()
    working: dict[str, Any] = {}
    with _bound(owner) as binding:
        loaded = await provider.get_messages("session", state=working)
        original = owner.state.data.conversation_history[0].messages[1]
        before_contents = _snapshot(original.to_dict()["contents"])
        loaded[1].contents[0].text = "working-only edit"
        if invalid_content:
            loaded[1].contents[0].additional_properties["payload"] = {"bad": object()}
        loaded[1].additional_properties["annotation"] = {"values": [None, False, 0, 0.0]}
        buffer = working[WORKING_BUFFER_KEY]
        content = loaded[1].contents[0]
        for _ in range(2):
            provider.flush(working)
            assert _snapshot(original.to_dict()["contents"]) == before_contents
            assert original.extension_data == {"annotation": {"values": [None, False, 0, 0.0]}}
            assert original.public_message_id == "second"
            assert working[WORKING_BUFFER_KEY] is buffer
            assert buffer[1] is loaded[1] and buffer[1].contents[0] is content
            assert binding.append_ordinal == 0
    assert owner.persist_count == 0


@pytest.mark.parametrize("count", [16, 32])
async def test_normal_flush_keeps_linear_visits_and_avoids_canonical_or_opaque_payload_copies(
    count: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    # These are untouched payloads, not annotations being persisted by this flush.
    # A whole-state deepcopy or JSON preflight would traverse them and fail.
    poison = _CannotCopy(AssertionError("untouched payload was copied"))
    owner = _CanonicalStateProvider([_request("seed", *[_stored(str(i), message_id=str(i)) for i in range(count)])])
    provider = DurableHistoryProvider()
    working: dict[str, Any] = {}
    with _bound(owner) as binding:
        await provider.get_messages("session", state=working)
        owner.state.data.session = {"opaque": poison}
        for message in working[WORKING_BUFFER_KEY]:
            message.raw_representation = poison
            message.contents[0].raw_representation = poison
            message.additional_properties["annotation"] = {"values": [None, False, 0, 0.0]}
        cast(Any, owner.state.data.conversation_history[-1].messages[-1].contents[0]).untouched = poison
        canonical_json = Mock(side_effect=AssertionError("whole-state serialization during flush"))
        monkeypatch.setattr(owner.state, "to_dict", canonical_json)
        positions = Mock(wraps=provider._positions)
        replay = Mock(wraps=provider._to_message)
        annotations = Mock(wraps=history_module._copy_history_annotations)
        json_snapshot = Mock(wraps=history_module._json_snapshot)
        visited: list[tuple[DurableAgentStateEntry, int]] = []
        original_replayable = provider._replayable_entries
        original_dictionary = history_module._HistoryFlushSnapshot._dictionary
        original_sequence = history_module._HistoryFlushSnapshot._sequence
        dictionary_sizes: list[int] = []
        sequence_sizes: list[int] = []

        def replayable(active: DurableHistoryBinding) -> Iterator[tuple[DurableAgentStateEntry, int]]:
            for position in original_replayable(active):
                visited.append(position)
                yield position

        def dictionary(snapshot: Any, value: dict[str, Any]) -> None:
            dictionary_sizes.append(len(value))
            original_dictionary(snapshot, value)

        def sequence(snapshot: Any, value: list[Any]) -> None:
            sequence_sizes.append(len(value))
            original_sequence(snapshot, value)

        monkeypatch.setattr(provider, "_positions", positions)
        monkeypatch.setattr(provider, "_to_message", replay)
        monkeypatch.setattr(provider, "_replayable_entries", replayable)
        monkeypatch.setattr(history_module, "_copy_history_annotations", annotations)
        monkeypatch.setattr(history_module, "_json_snapshot", json_snapshot)
        monkeypatch.setattr(history_module._HistoryFlushSnapshot, "_dictionary", dictionary)
        monkeypatch.setattr(history_module._HistoryFlushSnapshot, "_sequence", sequence)
        for repeat in range(1, 3):
            provider.flush(working)
            assert positions.call_count == 2 * repeat
            assert replay.call_count == repeat
            assert len(visited) == (2 * count + 1) * repeat
            assert annotations.call_count == count * repeat
            assert json_snapshot.call_count == count * repeat
            assert len(dictionary_sizes) == (3 * count + 6) * repeat
            assert sum(dictionary_sizes) <= (128 * count + 128) * repeat
            assert len(sequence_sizes) == 3 * repeat
            assert sum(sequence_sizes) == (2 * count + 1) * repeat
            assert not canonical_json.called
            assert binding.append_ordinal == 0
        assert owner.state.data.session["opaque"] is poison
        assert all(message.raw_representation is poison for message in working[WORKING_BUFFER_KEY])
        assert all(message.contents[0].raw_representation is poison for message in working[WORKING_BUFFER_KEY])
    assert owner.persist_count == 0


@pytest.mark.skipif(not hasattr(DurableHistoryProvider, "_prune"), reason="eager pruning belongs to the integrated tip")
@pytest.mark.parametrize("error_type", [RuntimeError, asyncio.CancelledError, GeneratorExit])
async def test_integrated_prune_failure_restores_actual_removed_objects_and_truncation(
    error_type: type[BaseException],
) -> None:
    # Run this unchanged on the integrated retention tip. Fail AFTER its actual
    # prune implementation, not a substitute that invents deletion behavior.
    prune = getattr(DurableHistoryProvider, "_prune", None)
    assert callable(prune)
    prune_actual = cast(Callable[[DurableHistoryBinding, Any], None], prune)
    owner = _CanonicalStateProvider([
        _request("old", _stored("old first", message_id="old-first"), _stored("old second", message_id="old-second")),
        _request("new", _stored("newest protected", message_id="new")),
    ])
    owner.state.data.truncation = {
        "evictedMessageCount": 4,
        "firstEvictedAt": "2026-09-01T00:00:00+00:00",
        "lastEvictedAt": "2026-09-01T00:00:00+00:00",
        "future": {"keep": [False, 0, 0.0]},
    }
    error = error_type("after actual prune")
    reached: list[bool] = []

    class FailAfterPrune(DurableHistoryProvider):
        @staticmethod
        def _prune(binding: DurableHistoryBinding, pruned: Any) -> None:
            prune_actual(binding, pruned)
            assert [
                message.text
                for entry in binding.state_provider.state.data.conversation_history
                for message in entry.messages
            ] == ["newest protected"]
            truncation = binding.state_provider.state.data.truncation
            assert truncation is not None and truncation["evictedMessageCount"] == 6
            reached.append(True)
            raise error

    provider = FailAfterPrune()
    cast(Any, provider).prune_excluded = True
    working: dict[str, Any] = {}
    with _bound(owner) as binding:
        await provider.get_messages("session", state=working)
        working[WORKING_BUFFER_KEY][:] = working[WORKING_BUFFER_KEY][-1:]
        for _ in range(2):
            with _unchanged_on_rejection(owner, working, binding), pytest.raises(error_type) as caught:
                provider.flush(working)
            assert caught.value is error
    assert reached == [True, True]
