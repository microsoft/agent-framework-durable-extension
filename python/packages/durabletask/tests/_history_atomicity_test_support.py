# Copyright (c) Microsoft. All rights reserved.

"""Shared canonical-state and reference oracles for local history atomicity tests."""

import json
from collections.abc import Callable
from typing import Any

from _shared_history_test_support import _CanonicalStateProvider
from agent_framework import SUMMARY_OF_MESSAGE_IDS_KEY, Message

from agent_framework_durabletask._shared_agent_state import DurableAgentState


def _owner(identity: str) -> _CanonicalStateProvider:
    messages: list[dict[str, Any]] = [
        {"role": "user", "contents": [{"$type": "text", "text": text}]} for text in ("first", "second")
    ]
    if identity != "anonymous":
        for message, public_id in zip(messages, ("first", "second"), strict=True):
            message["messageId"] = "duplicate" if identity == "duplicate" else public_id
    raw = {
        "schemaVersion": "2.0.0",
        "futureRoot": {"values": [None, False, 0, 0.0]},
        "data": {
            "conversationHistory": [{"$type": "request", "correlationId": "seed", "messages": messages}],
            "terminalResults": {},
            "completionReceipts": {},
            "session": {"session_id": "session", "state": {"other": {"keep": True}}},
        },
    }
    owner = _CanonicalStateProvider()
    owner.state = DurableAgentState.from_dict(raw)
    assert owner.state.to_dict() == raw
    assert [message.to_chat_message().text for message in owner.state.data.conversation_history[0].messages] == [
        "first",
        "second",
    ]
    return owner


def _snapshot(value: Any) -> str:
    return json.dumps(value, sort_keys=True, allow_nan=False)


def _reference_check(*roots: Any) -> Callable[[], None]:
    """Remember built-in containers without copying or invoking opaque payload hooks."""
    seen: set[int] = set()
    dictionaries: list[tuple[dict[Any, Any], list[tuple[Any, Any]]]] = []
    lists: list[tuple[list[Any], tuple[Any, ...]]] = []
    sets: list[tuple[set[Any], frozenset[Any]]] = []

    def remember(value: Any) -> None:
        if id(value) in seen:
            return
        seen.add(id(value))
        if type(value) is dict:
            items = list(value.items())
            dictionaries.append((value, items))
            for _, item in items:
                remember(item)
        elif type(value) is list:
            items_tuple = tuple(value)
            lists.append((value, items_tuple))
            for item in items_tuple:
                remember(item)
        elif type(value) is set:
            sets.append((value, frozenset(value)))

    for root in roots:
        remember(root)

    def check() -> None:
        for dictionary, dictionary_items in dictionaries:
            assert list(dictionary) == [key for key, _ in dictionary_items]
            assert all(dictionary[key] is value for key, value in dictionary_items)
        for sequence, list_items in lists:
            assert len(sequence) == len(list_items)
            assert all(value is item for value, item in zip(sequence, list_items, strict=True))
        for members, set_items in sets:
            assert members == set_items

    return check


def _summary() -> Message:
    return Message("assistant", ["new summary"], additional_properties={SUMMARY_OF_MESSAGE_IDS_KEY: []})
