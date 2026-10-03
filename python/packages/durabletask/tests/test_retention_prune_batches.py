# Copyright (c) Microsoft. All rights reserved.

"""Stable pruning preserves occurrence counts, aliases and changed-envelope rules."""

import asyncio
import json
import operator
from collections.abc import Iterable, Iterator
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, SupportsIndex

import pytest
from _execution_test_support import JsonStateProvider
from agent_framework import Message

from agent_framework_durabletask import _history_provider as history_module
from agent_framework_durabletask._history_provider import (
    POSITIONS_KEY,
    WORKING_BUFFER_KEY,
    DurableHistoryBinding,
    DurableHistoryProvider,
    bind_durable_history,
    prune_messages,
    unbind_durable_history,
)
from agent_framework_durabletask._shared_agent_state import (
    DurableAgentState,
    DurableAgentStateEntry,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateUnknownEntry,
)

OLD = datetime(2026, 1, 2, tzinfo=timezone.utc)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _message(message_id: str) -> DurableAgentStateMessage:
    return DurableAgentStateMessage.from_chat_message(Message("user", [message_id], message_id=message_id))


def _entry(correlation: str, *messages: DurableAgentStateMessage) -> DurableAgentStateRequest:
    return DurableAgentStateRequest(correlation, OLD, list(messages))


class _CountingList(list[Any]):
    """Count visited and moved slots without a wall-clock or exact implementation oracle."""

    def __init__(self, values: Iterable[Any]) -> None:
        super().__init__(values)
        self.visits = 0
        self.moves = 0
        self.writes = 0

    def __iter__(self) -> Iterator[Any]:
        for item in super().__iter__():
            self.visits += 1
            yield item

    def __getitem__(self, key: Any) -> Any:
        value = super().__getitem__(key)
        self.visits += len(value) if isinstance(key, slice) else 1
        return value

    def __delitem__(self, key: Any) -> None:
        self.writes += 1
        if isinstance(key, int):
            index = key if key >= 0 else len(self) + key
            self.moves += len(self) - index - 1
        else:
            self.moves += len(self)
        super().__delitem__(key)

    def __setitem__(self, key: Any, value: Any) -> None:
        self.writes += 1
        if isinstance(key, slice):
            value = list(value)
            self.moves += len(range(*key.indices(len(self)))) + len(value)
        super().__setitem__(key, value)

    def pop(self, index: SupportsIndex = -1) -> Any:
        position = operator.index(index)
        normalized = position if position >= 0 else len(self) + position
        self.moves += len(self) - normalized - 1
        self.writes += 1
        return super().pop(position)

    def clear(self) -> None:
        self.moves += len(self)
        self.writes += 1
        super().clear()


@pytest.mark.parametrize("reverse", [False, True], ids=["forward", "reverse"])
def test_pruning_work_is_bounded_by_messages_not_the_number_of_individual_deletions(
    reverse: bool, record_property: Any
) -> None:
    costs: list[int] = []
    for count in (32, 64):
        old = _entry("old", *[_message(f"old-{index}") for index in range(count)])
        old.messages = messages = _CountingList(old.messages)
        newest = _entry("current", _message("newest"))
        history = _CountingList([old, newest])
        selected: list[tuple[DurableAgentStateEntry, DurableAgentStateMessage]] = [
            (old, message) for message in list.__iter__(messages)
        ]
        if reverse:
            selected.reverse()
        prune_messages(history, selected)
        work = messages.visits + messages.moves
        costs.append(work)
        record_property(f"list_work_{count}", work)
        assert messages.visits > 0, "The instrumentation must observe real message-list work"
        assert work <= 4 * count + 16
        assert history.visits + history.moves <= 20
        assert old.messages is messages and messages == []
        assert len(history) == 1 and history[0] is newest
        assert newest.messages[0].public_message_id == "newest"
    assert costs[1] <= 2 * costs[0] + 16


def test_pruning_preserves_identity_multiplicity_stale_selections_and_unknown_envelopes() -> None:
    repeated = _message("same")
    equal_but_distinct = deepcopy(repeated)
    survivor = _message("survivor")
    old = _entry("old", repeated, equal_but_distinct, survivor, repeated)
    stale = deepcopy(old)
    metadata = _entry("metadata", _message("metadata-message"))
    metadata.unknown_fields["future"] = {"values": [False, 0, 0.0]}
    empty = _entry("already-empty")
    opaque = DurableAgentStateUnknownEntry({"$type": "future", "payload": {"keep": True}})
    opaque.messages = [_message("opaque")]
    unknown = DurableAgentStateEntry("future", "unknown", OLD, [_message("unknown")])
    newest = _entry("current", _message("newest"))
    history: list[DurableAgentStateEntry] = [old, metadata, empty, opaque, unknown, newest]
    history_alias, messages_alias = history, old.messages
    metadata_alias, opaque_alias, unknown_alias = metadata.messages, opaque.messages, unknown.messages
    stale_before = _json(stale.to_dict())
    survivors_before = _json([
        equal_but_distinct.to_dict(),
        survivor.to_dict(),
        newest.to_dict(),
        opaque.to_dict(),
        unknown.to_dict(),
    ])
    assert repeated is not equal_but_distinct and _json(repeated.to_dict()) == _json(equal_but_distinct.to_dict())

    prune_messages(history, [(stale, stale.messages[0]), (old, deepcopy(repeated)), (old, repeated)])
    assert history is history_alias and old.messages is messages_alias
    assert [id(message) for message in messages_alias] == [id(equal_but_distinct), id(survivor), id(repeated)]
    assert _json(stale.to_dict()) == stale_before
    prune_messages(
        history,
        [
            (old, repeated),
            (old, repeated),
            (metadata, metadata.messages[0]),
            (opaque, opaque.messages[0]),
            (unknown, unknown.messages[0]),
        ],
    )
    assert [id(message) for message in messages_alias] == [id(equal_but_distinct), id(survivor)]
    assert metadata.messages is metadata_alias and metadata_alias == []
    assert _json(metadata.to_dict()["future"]) == '{"values":[false,0,0.0]}'
    assert opaque.messages is opaque_alias and len(opaque_alias) == 1
    assert unknown.messages is unknown_alias and len(unknown_alias) == 1
    assert [id(entry) for entry in history] == [id(e) for e in (old, metadata, empty, opaque, unknown, newest)]
    assert (
        _json([equal_but_distinct.to_dict(), survivor.to_dict(), newest.to_dict(), opaque.to_dict(), unknown.to_dict()])
        == survivors_before
    )
    prune_messages(history, [(old, equal_but_distinct), (old, survivor)])
    assert old.messages is messages_alias and messages_alias == []
    assert history is history_alias
    assert [id(entry) for entry in history] == [id(e) for e in (metadata, empty, opaque, unknown, newest)]


@pytest.mark.parametrize("selections", [[], ["missing"], ["stale"]])
def test_no_actual_removal_does_not_write_any_list(selections: list[str]) -> None:
    entry = _entry("old", _message("kept"))
    entry.messages = messages = _CountingList(entry.messages)
    history = _CountingList([entry, _entry("already-empty")])
    selected: list[tuple[DurableAgentStateEntry, DurableAgentStateMessage]] = [
        (entry, _message("missing")) if choice == "missing" else (_entry("old"), messages[0]) for choice in selections
    ]
    prune_messages(history, selected)
    assert history.writes == messages.writes == 0
    assert entry.messages is messages and messages[0].public_message_id == "kept"
    assert len(history) == 2


@pytest.mark.parametrize("right_first", [False, True])
def test_shared_message_list_attributes_removal_to_the_first_selected_owner(right_first: bool) -> None:
    message = _message("shared")
    left, right = _entry("left", message), _entry("right")
    right.messages = alias = left.messages
    history: list[DurableAgentStateEntry] = [left, right]
    first, second = (right, left) if right_first else (left, right)
    prune_messages(history, [(first, message), (second, message)])
    assert len(history) == 1 and history[0] is second
    assert left.messages is right.messages is alias and alias == []


def test_shared_list_interleaving_counts_repeated_occurrences_before_dropping_owners() -> None:
    repeated, other = _message("repeated"), _message("other")
    left, right = _entry("left", repeated, other, repeated), _entry("right")
    right.messages = alias = left.messages
    unselected = _entry("unselected")
    unselected.messages = alias
    history: list[DurableAgentStateEntry] = [left, right, unselected]
    prune_messages(history, [(right, repeated), (left, other), (left, repeated), (right, repeated)])
    assert len(history) == 1 and history[0] is unselected
    assert left.messages is right.messages is unselected.messages is alias and alias == []


@pytest.mark.parametrize("error_type", [RuntimeError, asyncio.CancelledError, GeneratorExit])
async def test_failure_after_actual_batch_pruning_restores_the_flush_aliases_and_control_state(
    error_type: type[BaseException], monkeypatch: pytest.MonkeyPatch
) -> None:
    state = DurableAgentState()
    state.data.conversation_history = [_entry("old", _message("old")), _entry("current", _message("new"))]
    state.data.session = {"session_id": "session", "state": {"foreign": {"values": [False, 0, None]}}}
    state.data.ingested_messages = {"prior": ["a" * 64]}
    state.data.extension_data = {"foreign": [False, 0, None]}
    owner = JsonStateProvider(state.to_dict())
    canonical = owner.state
    data = canonical.data
    history = data.conversation_history
    entries = tuple(history)
    lists = [entry.messages for entry in history]
    originals = [tuple(messages) for messages in lists]
    before = _json(canonical.to_dict())
    provider = DurableHistoryProvider(prune_excluded=True)
    binding = DurableHistoryBinding(owner, "current")
    working: dict[str, Any] = {}
    real_prune = history_module.prune_messages
    reached: list[bool] = []
    error = error_type("after actual deletion")

    def fail_after_prune(
        current: list[DurableAgentStateEntry], selected: list[tuple[DurableAgentStateEntry, DurableAgentStateMessage]]
    ) -> None:
        real_prune(current, selected)
        assert len(current) == 1 and current[0] is entries[1] and lists[0] == []
        reached.append(True)
        raise error

    token = bind_durable_history(binding)
    try:
        await provider.get_messages("session", state=working)
        buffer = working[WORKING_BUFFER_KEY]
        buffer[:] = buffer[-1:]
        buffer_items = tuple(buffer)
        positions = working[POSITIONS_KEY]
        position_items = dict(positions)
        monkeypatch.setattr(history_module, "prune_messages", fail_after_prune)
        for _ in range(2):
            with pytest.raises(error_type) as caught:
                provider.flush(working)
            assert caught.value is error
            assert owner.state is canonical and canonical.data is data and data.conversation_history is history
            assert len(history) == len(entries) and all(a is b for a, b in zip(history, entries, strict=True))
            for entry, messages, original in zip(entries, lists, originals, strict=True):
                assert entry.messages is messages
                assert len(messages) == len(original) and all(a is b for a, b in zip(messages, original, strict=True))
            assert working[WORKING_BUFFER_KEY] is buffer and len(buffer) == len(buffer_items)
            assert all(a is b for a, b in zip(buffer, buffer_items, strict=True))
            assert working[POSITIONS_KEY] is positions
            assert set(positions) == set(position_items)
            assert all(positions[key] is value for key, value in position_items.items())
            assert binding.append_ordinal == 0 and binding.accepted_inputs == set()
            assert _json(canonical.to_dict()) == before
    finally:
        unbind_durable_history(token)
    assert reached == [True, True]
    assert owner.attempted_writes == owner.successful_writes == 0
