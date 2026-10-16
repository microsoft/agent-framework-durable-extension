# Copyright (c) Microsoft. All rights reserved.

"""Lossless legacy history ordering without changing legacy request/response envelopes."""

from __future__ import annotations

import json
from collections import deque
from collections.abc import Iterator
from copy import copy, deepcopy
from typing import Any, cast
from uuid import uuid4

from agent_framework import (
    GROUP_ANNOTATION_KEY,
    GROUP_ID_KEY,
    SUMMARIZED_BY_SUMMARY_ID_KEY,
    SUMMARY_OF_MESSAGE_IDS_KEY,
    Message,
)

from ._response_utils import serialize_input_message
from ._shared_agent_state import (
    DurableAgentState,
    DurableAgentStateCompaction,
    DurableAgentStateEntry,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
    _json_snapshot,  # pyright: ignore[reportPrivateUsage]
)

_FIELD = "_dafx_legacy_history"
_PROFILE = "dafx-legacy-history-v1"
_PREFIX = "dafx_legacy:"
HistorySlot = tuple[str, DurableAgentStateEntry, DurableAgentStateMessage]


def _summary_properties(message: Message) -> dict[str, Any]:
    group = message.additional_properties.get(GROUP_ANNOTATION_KEY)
    if isinstance(group, dict):
        properties = cast("dict[str, Any]", group)
        if properties.get(SUMMARY_OF_MESSAGE_IDS_KEY) is not None:
            return properties
    return message.additional_properties


def _slots(state: DurableAgentState) -> list[HistorySlot]:
    return [
        (f"{entry_index}:{message_index}", entry, message)
        for entry_index, entry in enumerate(state.data.conversation_history)
        for message_index, message in enumerate(entry.messages)
    ]


def legacy_history_profile(state: DurableAgentState) -> dict[str, Any] | None:
    """Validate only declared legacy ordering; the source profile is inert after migration."""
    if state.schema_version == "2.0.0":
        return None
    extensions = state.data.extension_data
    if extensions is None or _FIELD not in extensions:
        return None
    raw = extensions[_FIELD]
    if not isinstance(raw, dict):
        raise ValueError("Legacy history ordering requires an object profile.")
    profile = cast("dict[str, Any]", raw)
    count, order, generated = profile.get("entryCount"), profile.get("order"), profile.get("generated")
    if (
        profile.get("profile") != _PROFILE
        or type(count) is not int
        or not 0 <= count <= len(state.data.conversation_history)
        or not isinstance(order, list)
        or not isinstance(generated, list)
    ):
        raise ValueError("Legacy history ordering has an unsupported profile.")
    order = cast("list[Any]", order)
    generated = cast("list[Any]", generated)
    if any(not isinstance(reference, str) for reference in [*order, *generated]):
        raise ValueError("Legacy history references must be strings.")
    expected = {
        f"{entry_index}:{message_index}"
        for entry_index, entry in enumerate(state.data.conversation_history[:count])
        for message_index in range(len(entry.messages))
    }
    if len(order) != len(set(order)) or set(order) != expected or len(generated) != len(set(generated)):
        raise ValueError("Legacy history ordering must cover every recorded message exactly once.")
    by_reference = {reference: entry for reference, entry, _ in _slots(state)}
    if any(
        reference not in expected or not isinstance(by_reference[reference], DurableAgentStateRequest)
        for reference in generated
    ):
        raise ValueError("Generated legacy history must refer to recorded request-message slots.")
    return profile


def ordered_legacy_history(state: DurableAgentState) -> Iterator[HistorySlot]:
    """Replay declared ordering, then any later messages appended by a legacy writer."""
    slots = _slots(state)
    profile = legacy_history_profile(state)
    if profile is None:
        yield from slots
        return
    by_reference = {reference: (reference, entry, message) for reference, entry, message in slots}
    order = cast("list[str]", profile["order"])
    yield from (by_reference[reference] for reference in order)
    recorded = set(order)
    yield from (slot for slot in slots if slot[0] not in recorded)


def generated_legacy_history(state: DurableAgentState) -> set[str]:
    """Generated summaries/revisions are not historical delivered-input evidence."""
    profile = legacy_history_profile(state)
    return set(cast("list[str]", profile["generated"])) if profile is not None else set()


def clear_legacy_history(state: DurableAgentState) -> None:
    """Clear owned ordering fields on reset while retaining opaque profile siblings."""
    profile = legacy_history_profile(state)
    if profile is not None:
        profile.update(entryCount=0, order=[], generated=[])


def materialize_legacy_history(state: DurableAgentState) -> None:
    """Translate legacy replay order to v2 entries after source journals have been validated."""
    profile = legacy_history_profile(state)
    if profile is None:
        return
    generated = generated_legacy_history(state)
    original = state.data.conversation_history
    ordered = list(ordered_legacy_history(state))
    empty = [(index, entry) for index, entry in enumerate(original) if not entry.messages]
    output: list[DurableAgentStateEntry] = []
    for reference, entry, message in ordered:
        message.set_history_id(f"{_PREFIX}{reference}")
        if reference in generated:
            derived = DurableAgentStateCompaction(
                created_at=entry.created_at,
                messages=[message],
                extension_data=deepcopy(entry.extension_data),
            )
            derived.unknown_fields = deepcopy(entry.unknown_fields)
            output.append(derived)
        else:
            fragment = copy(entry)
            fragment.messages = [message]
            output.append(fragment)
    for index, entry in empty:
        output.insert(min(index, len(output)), entry)
    # Opaque empty envelopes must not silently change their relative order.
    if [id(entry) for entry in output if not entry.messages] != [id(entry) for entry in original if not entry.messages]:
        raise ValueError("Legacy history ordering cannot relocate opaque empty envelopes losslessly.")
    state.data.conversation_history = output


class LegacyMemoryHistory:
    """Reconcile the final primary memory buffer without rewriting original envelope bodies."""

    def __init__(self, state: DurableAgentState, correlation_id: str) -> None:
        from ._history_provider import _HISTORY_ID_ATTRIBUTE  # pyright: ignore[reportPrivateUsage]

        self._attribute = _HISTORY_ID_ATTRIBUTE
        self._correlation_id = correlation_id
        self._linked: dict[str, DurableAgentStateMessage] = {}
        self._working: dict[str, Message] = {}
        self._candidate_ids: dict[int, str] = {}
        self._public_ids: dict[str, str | None] = {}
        self._bodies: dict[str, str] = {}
        self._snapshots: dict[str, DurableAgentStateMessage] = {}
        self._writes: dict[int, str] = {}
        self._archived: dict[str, None] = {}
        self._ordinal = 0
        self._scope = uuid4().hex
        generated = generated_legacy_history(state)
        self._generated = {id(message) for reference, _, message in _slots(state) if reference in generated}

    @staticmethod
    def _body(message: Message) -> str:
        payload = serialize_input_message(message)
        payload.pop("additional_properties", None)
        sources = _summary_properties(message).get(SUMMARY_OF_MESSAGE_IDS_KEY)
        version: dict[str, Any] = {"message": payload, "summarySources": sources if isinstance(sources, list) else None}
        return json.dumps(version, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)

    def bind(self, reference: str, stored: DurableAgentStateMessage, message: Message) -> None:
        token = f"{_PREFIX}{reference}"
        setattr(message, self._attribute, token)
        self._linked[token] = stored
        self._working[token] = message
        self._public_ids[token] = message.message_id
        self._bodies[token] = self._body(message)

    def observe(self, messages: list[Message], *, rebind_seed: bool = False) -> None:
        for message in messages:
            token = getattr(message, self._attribute, None)
            known = self._working.get(token) if isinstance(token, str) else None
            if known is None or (known is not message and not rebind_seed):
                if known is not None and message.message_id == known.message_id:
                    self._candidate_ids[id(message)] = cast("str", token)
                setattr(message, self._attribute, f"{_PREFIX}pending:{self._scope}:{self._ordinal}")
                self._ordinal += 1
                self._public_ids[cast(str, getattr(message, self._attribute))] = message.message_id
            self._working[cast(str, getattr(message, self._attribute))] = message

    def _remember(self, message: Message) -> None:
        token = cast(str, getattr(message, self._attribute))
        if token not in self._snapshots:
            self._snapshots[token] = DurableAgentStateMessage.from_core_dict(serialize_input_message(message))
        self._bodies.setdefault(token, self._body(message))

    def capture_write(self, messages: list[Message]) -> None:
        self.observe(messages)
        for message in messages:
            token = cast(str, getattr(message, self._attribute))
            self._writes.setdefault(id(message), token)
            self._remember(message)

    def _separate_revision(self, message: Message) -> None:
        token = cast(str, getattr(message, self._attribute))
        original = self._linked.get(token) or self._snapshots.get(token)
        if original is None or self._body(message) == self._bodies[token]:
            return
        archived = original.to_chat_message()
        archived.additional_properties = deepcopy(original.extension_data or {})
        setattr(archived, self._attribute, token)
        raw_group = message.additional_properties.get(GROUP_ANNOTATION_KEY)
        group = cast("dict[str, Any]", raw_group) if isinstance(raw_group, dict) else None
        links = _summary_properties(message)
        sources = links.get(SUMMARY_OF_MESSAGE_IDS_KEY, [])
        candidates = {token, message.message_id, self._candidate_ids.get(id(message))} - {None}
        if isinstance(sources, list) and token in sources:
            for properties, target in (
                (message.additional_properties, archived.additional_properties),
                (group, archived.additional_properties.setdefault(GROUP_ANNOTATION_KEY, {})),
            ):
                if isinstance(properties, dict) and isinstance(target, dict):
                    source_properties = properties
                    value = source_properties.get(SUMMARIZED_BY_SUMMARY_ID_KEY)
                    if SUMMARIZED_BY_SUMMARY_ID_KEY in source_properties and any(
                        value == candidate for candidate in candidates
                    ):
                        target[SUMMARIZED_BY_SUMMARY_ID_KEY] = value
                        del source_properties[SUMMARIZED_BY_SUMMARY_ID_KEY]
        self._working[token] = archived
        self._archived.setdefault(token, None)
        self.observe([message])
        self._remember(message)

    def observe_compaction(self, messages: list[Message], within_strategy: bool) -> None:
        if not within_strategy:
            for message in messages:
                token = getattr(message, self._attribute, None)
                if isinstance(token, str) and token in self._public_ids:
                    message.message_id = self._public_ids[token]
            self.observe(messages)
            return
        public_ids: dict[int, str | None] = {}
        for message in messages:
            token = getattr(message, self._attribute, None)
            candidate = message.message_id
            public_ids[id(message)] = (
                self._public_ids.get(token) if isinstance(token, str) and candidate == token else candidate
            )
            if candidate is not None:
                self._candidate_ids[id(message)] = candidate
        self.observe(messages)
        for message in messages:
            message.message_id = public_ids[id(message)]
            self._separate_revision(message)
            self._remember(message)
            token = cast(str, getattr(message, self._attribute))
            self._public_ids[token] = message.message_id
            message.message_id = token
        identities = {
            cast(str, getattr(message, self._attribute)): cast(str, message.message_id) for message in messages
        }
        self._repair_links(messages, identities)

    def link(self, message: Message, stored: DurableAgentStateMessage) -> DurableAgentStateMessage:
        self.observe([message])
        self._separate_revision(message)
        token = cast("str", getattr(message, self._attribute))
        original = self._writes.get(id(message), token)
        if original != token:
            stored = deepcopy(self._snapshots[original])
            self._linked[original] = stored
            return stored
        self._linked[token] = stored
        self._bodies[token] = self._body(message)
        return stored

    @staticmethod
    def _annotations(message: Message) -> dict[str, Any]:
        return _json_snapshot(deepcopy(message.additional_properties))

    def _repair_links(self, messages: list[Message], identities: dict[str, str]) -> None:
        for summary in messages:
            token = cast(str, getattr(summary, self._attribute))
            group = summary.additional_properties.get(GROUP_ANNOTATION_KEY)
            links = _summary_properties(summary)
            source_ids = links.get(SUMMARY_OF_MESSAGE_IDS_KEY)
            if not isinstance(source_ids, list):
                continue
            if any(not isinstance(source_id, str) for source_id in cast("list[Any]", source_ids)):
                raise ValueError("Legacy summary links require string message identities.")
            source_ids = cast("list[str]", source_ids)
            summary_id = identities[token]
            candidates = {summary.message_id, token, self._candidate_ids.get(id(summary))} - {None}
            if isinstance(group, dict) and GROUP_ID_KEY in group:
                group[GROUP_ID_KEY] = f"group_{summary_id}"
            for source in self._working.values():
                if getattr(source, self._attribute, None) not in source_ids:
                    continue
                for properties in (
                    source.additional_properties,
                    source.additional_properties.get(GROUP_ANNOTATION_KEY),
                ):
                    if isinstance(properties, dict):
                        annotations = cast("dict[str, Any]", properties)
                        if SUMMARIZED_BY_SUMMARY_ID_KEY in annotations and any(
                            annotations[SUMMARIZED_BY_SUMMARY_ID_KEY] == candidate for candidate in candidates
                        ):
                            annotations[SUMMARIZED_BY_SUMMARY_ID_KEY] = summary_id
            links[SUMMARY_OF_MESSAGE_IDS_KEY] = [identities.get(source_id, source_id) for source_id in source_ids]

    def flush(self, state: DurableAgentState, buffer: list[Message]) -> None:
        """Stage annotations, generated messages and ordering within the enclosing turn rollback."""
        profile = legacy_history_profile(state)
        self.observe(buffer)
        for message in buffer:
            self._separate_revision(message)
        working: list[tuple[Message, DurableAgentStateMessage]] = []
        derived: list[DurableAgentStateRequest] = []
        seen: set[str] = set()
        terminal = state.data.conversation_history[-1]
        if not isinstance(terminal, DurableAgentStateResponse):
            raise ValueError("Legacy memory reconciliation requires the current terminal response.")
        required = deque([*buffer, *(self._working[token] for token in self._archived)])
        while required:
            message = required.popleft()
            sources = _summary_properties(message).get(SUMMARY_OF_MESSAGE_IDS_KEY, [])
            if isinstance(sources, list):
                for token in cast("list[Any]", sources):
                    if (
                        isinstance(token, str)
                        and token in self._snapshots
                        and token not in self._linked
                        and token not in self._archived
                    ):
                        self._archived[token] = None
                        required.append(self._working[token])
        for token in self._archived:
            if token not in self._linked:
                archived = deepcopy(self._snapshots[token])
                self._linked[token] = archived
                self._generated.add(id(archived))
                derived.append(DurableAgentStateRequest(self._correlation_id, terminal.created_at, [archived]))
        for message in buffer:
            token = getattr(message, self._attribute, None)
            if isinstance(token, str) and token in seen:
                raise ValueError("Legacy memory occurrences must have distinct reconciliation identities.")
            if isinstance(token, str):
                seen.add(token)
            stored = self._linked.get(token) if isinstance(token, str) else None
            if stored is None:
                stored = DurableAgentStateMessage.from_core_dict(serialize_input_message(message))
                self._generated.add(id(stored))
                derived.append(DurableAgentStateRequest(self._correlation_id, terminal.created_at, [stored]))
            else:
                stored.extension_data = self._annotations(message) or None
            working.append((message, stored))
        present = {id(stored) for _, stored in working}
        if derived:
            state.data.conversation_history[-1:-1] = derived
        slots = _slots(state)
        locations = {id(stored): reference for reference, _, stored in slots}
        aliases = {token: f"{_PREFIX}{locations[id(stored)]}" for token, stored in self._linked.items()}
        aliases.update({
            cast(str, getattr(message, self._attribute)): f"{_PREFIX}{locations[id(stored)]}"
            for message, stored in working
            if isinstance(getattr(message, self._attribute, None), str)
        })
        self._repair_links(
            [message for token, message in self._working.items() if token in aliases],
            aliases,
        )
        for message, stored in working:
            stored.extension_data = self._annotations(message) or None
        for token, stored in self._linked.items():
            if id(stored) not in present:
                stored.extension_data = {**self._annotations(self._working[token]), "_excluded": True}
        order = [locations[id(stored)] for _, stored in working]
        absent = [reference for reference, _, stored in slots if id(stored) not in present]
        extensions = state.data.extension_data
        if extensions is None:
            extensions = {}
            state.data.extension_data = extensions
        extensions[_FIELD] = {
            **(profile or {}),
            "profile": _PROFILE,
            "entryCount": len(state.data.conversation_history),
            "order": [*absent, *order],
            "generated": [reference for reference, _, stored in slots if id(stored) in self._generated],
        }
        legacy_history_profile(state)
