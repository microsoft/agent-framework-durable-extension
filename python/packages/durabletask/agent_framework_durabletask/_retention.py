# Copyright (c) Microsoft. All rights reserved.

"""Independent eager-pruning policy and opt-in whole-entity pressure eviction."""

from __future__ import annotations

import json
import logging
import math
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, TypeAlias, cast

from agent_framework import (
    EXCLUDED_KEY,
    GROUP_ANNOTATION_KEY,
    GROUP_ID_KEY,
    GROUP_INDEX_KEY,
    CharacterEstimatorTokenizer,
    Message,
    TokenBudgetComposedStrategy,
    annotate_message_groups,
    included_token_count,
)

from ._constants import DurableStateFields
from ._durable_agent_state import (
    DurableAgentState,
    DurableAgentStateEntry,
    DurableAgentStateEntryJsonType,
    DurableAgentStateMessage,
)

__all__ = [
    "DEFAULT_MAX_STATE_BYTES",
    "DEFAULT_RETENTION",
    "DELIVERY_WINDOW_SECONDS",
    "DTS_MAX_STATE_BYTES",
    "HIGH_WATERMARK",
    "LOW_WATERMARK",
    "RetentionMode",
    "StateBudget",
    "StateCapacityError",
    "enforce_budget",
    "prunes_excluded",
    "resolve_state_budget",
    "validate_retention",
]

logger = logging.getLogger("agent_framework.durabletask")

RetentionMode: TypeAlias = Literal["keep_all", "follow_compaction"]
"""Whether to eagerly prune compaction exclusions, independently of a pressure budget."""

StateBudget: TypeAlias = int | Literal["backend_limit"] | None
"""An explicit byte budget, a host-resolved limit, or disabled pressure eviction."""

DEFAULT_RETENTION: RetentionMode = "keep_all"
DEFAULT_MAX_STATE_BYTES: StateBudget = None
DTS_MAX_STATE_BYTES = 1_048_576
HIGH_WATERMARK = 0.85
LOW_WATERMARK = 0.70
DELIVERY_WINDOW_SECONDS = 60
"""Legacy response protection when independent completion bookkeeping is absent."""

_SYSTEM_ROLE = "system"
_MAX_PASSES = 3
_Origin: TypeAlias = tuple[int, int]

_EXCHANGE_KINDS = {
    DurableAgentStateEntryJsonType.REQUEST,
    DurableAgentStateEntryJsonType.RESPONSE,
    DurableAgentStateEntryJsonType.ERROR_RESPONSE,
}
_TRANSCRIPT_KINDS = _EXCHANGE_KINDS | {DurableAgentStateEntryJsonType.COMPACTION}
_BARE_ENTRY_FIELDS = {
    DurableStateFields.TYPE_DISCRIMINATOR,
    DurableStateFields.CORRELATION_ID,
    DurableStateFields.CREATED_AT,
    DurableStateFields.MESSAGES,
}


class StateCapacityError(ValueError):
    """The protected state or an unreachable retention target prevents a safe commit."""

    def __init__(self, *, size_bytes: int, max_state_bytes: int, floor_bytes: int, target_bytes: int) -> None:
        """Describe the measured state, configured budget, protected floor and target."""
        self.size_bytes = size_bytes
        self.max_state_bytes = max_state_bytes
        self.floor_bytes = floor_bytes
        self.target_bytes = target_bytes
        super().__init__(
            f"Durable state capacity cannot meet the {target_bytes}-byte retention target: "
            f"serialized size is {size_bytes} bytes, budget is {max_state_bytes} bytes, "
            f"and the protected floor is {floor_bytes} bytes. No transcript changes were applied."
        )


def _positive_budget(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer, not a boolean or another value type.")
    return value


def resolve_state_budget(value: StateBudget, *, backend_limit: int | None = None) -> int | None:
    """Resolve a pressure budget without enabling eager pruning or assuming a backend.

    Raises:
        ValueError: The value is invalid, or ``backend_limit`` is requested but unresolved.
    """
    if backend_limit is not None:
        _positive_budget(backend_limit, "backend_limit")
    if value is None:
        return None
    if isinstance(value, str) and value == "backend_limit":
        if backend_limit is None:
            raise ValueError("max_state_bytes='backend_limit' requires a known backend_limit from the host.")
        return backend_limit
    return _positive_budget(value, "max_state_bytes")


def validate_retention(
    retention: RetentionMode,
    high_watermark: float = HIGH_WATERMARK,
    low_watermark: float = LOW_WATERMARK,
) -> None:
    """Validate the eager-pruning mode and finite, ordered numeric watermarks.

    Raises:
        ValueError: The mode or watermarks do not satisfy the retention contract.
    """
    if not isinstance(retention, str) or retention not in ("keep_all", "follow_compaction"):
        raise ValueError("retention must be 'keep_all' or 'follow_compaction'.")
    for name, value in (("high_watermark", high_watermark), ("low_watermark", low_watermark)):
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not 0 < value <= 1
            or not math.isfinite(value)
        ):
            raise ValueError(f"{name} must be a finite number in (0, 1], not a boolean.")
    if low_watermark >= high_watermark:
        raise ValueError("watermarks must satisfy 0 < low_watermark < high_watermark <= 1.")


def prunes_excluded(retention: RetentionMode) -> bool:
    """Whether compaction exclusions should be deleted as they are made."""
    validate_retention(retention)
    return retention == "follow_compaction"


async def enforce_budget(
    state: DurableAgentState,
    *,
    max_state_bytes: int,
    high_watermark: float = HIGH_WATERMARK,
    low_watermark: float = LOW_WATERMARK,
) -> int:
    """Evict eligible oldest atomic groups using detached, byte-checked plans.

    Args:
        state: Modified only after a plan fits, including its truncation record.

    Keyword Args:
        max_state_bytes: An already resolved positive budget. Callers skip this function for None.
        high_watermark: The fraction at which pressure eviction starts.
        low_watermark: The desired retained fraction, raised to the protected floor if necessary.

    Returns:
        The number of transcript messages removed.

    Raises:
        ValueError: A budget or watermark is invalid.
        StateCapacityError: No safe target is reachable. The input state remains unchanged.
    """
    _positive_budget(max_state_bytes, "max_state_bytes")
    validate_retention(DEFAULT_RETENTION, high_watermark, low_watermark)
    high = int(max_state_bytes * high_watermark)
    size = _serialized_size(state)
    if size < high:
        return 0

    baseline = deepcopy(state)
    now = datetime.now(tz=timezone.utc)
    messages, origins = _candidates(baseline, now=now)
    floor_state = _stage_eviction(baseline, set(origins))
    floor_without_record = _serialized_size(floor_state)
    if origins:
        record_truncation(floor_state, len(origins), now=now)
    floor = _serialized_size(floor_state)
    target = max(int(max_state_bytes * low_watermark), floor)
    if floor >= high:
        raise StateCapacityError(
            size_bytes=size, max_state_bytes=max_state_bytes, floor_bytes=floor, target_bytes=high - 1
        )

    groups: dict[str, list[int]] = {}
    for index, message in enumerate(messages):
        groups.setdefault(_group_id(message), []).append(index)
    ordered_groups = list(groups.values())
    group_tokens = [included_token_count([messages[index] for index in group]) for group in ordered_groups]
    group_sizes = _prefix_sizes(
        baseline,
        origins,
        ordered_groups,
        size=size,
        record_cost=floor - floor_without_record,
    )
    stored_origins = [
        (baseline.data.conversation_history[entry], baseline.data.conversation_history[entry].messages[message])
        for entry, message in origins
    ]
    evictable_bytes = sum(_message_size(stored) for _, stored in stored_origins)
    planning_target = target

    for _ in range(_MAX_PASSES):
        cutoff = next(
            (index + 1 for index, projected_size in enumerate(group_sizes) if projected_size <= planning_target),
            len(ordered_groups),
        )
        retained_tokens = sum(group_tokens[cutoff:])
        estimate = _token_budget(
            stored_origins,
            serialized_size=size,
            evictable_bytes=evictable_bytes,
            target_bytes=planning_target,
            floor_bytes=floor,
            evictable_tokens=sum(group_tokens),
        )
        # Align the estimate to a byte-measured group boundary. A global bytes/token ratio
        # alone can over-delete mixed Unicode, tool payloads and small prose messages.
        token_budget = min(max(estimate, retained_tokens), retained_tokens + group_tokens[cutoff - 1] - 1)
        planned = deepcopy(messages)
        # Core 1.16 retains its last non-system group even above budget. A detached, empty
        # user anchor occupies that slot, so the last eligible OLD group is not pinned.
        anchor = Message("user", [], message_id="retention_anchor")
        annotate_message_groups([anchor], tokenizer=CharacterEstimatorTokenizer())
        planned.append(anchor)
        strategy = TokenBudgetComposedStrategy(
            token_budget=token_budget + included_token_count([anchor]),
            tokenizer=CharacterEstimatorTokenizer(),
            strategies=[],
        )
        await strategy(planned)
        removed = {
            origin
            for origin, message in zip(origins, planned)
            if message.additional_properties.get(EXCLUDED_KEY, False)
        }
        staged = _stage_eviction(baseline, removed)
        if removed:
            record_truncation(staged, len(removed), now=now)
        measured = _serialized_size(staged)
        if measured <= target and measured < high:
            state.data.conversation_history[:] = staged.data.conversation_history
            state.data.truncation = staged.data.truncation
            logger.warning(
                "[Retention] Evicted %d oldest transcript message(s), leaving %d serialized bytes "
                "against a %d-byte budget. Set max_state_bytes=None to disable pressure eviction.",
                len(removed),
                measured,
                max_state_bytes,
            )
            return len(removed)
        # Correct the observed planning error, not an arbitrary fraction of the target.
        # Subtracting only the excess over target can select the same group boundary again.
        planning_error = max(measured - group_sizes[cutoff - 1], 1)
        planning_target = max(floor, target - planning_error)

    raise StateCapacityError(size_bytes=size, max_state_bytes=max_state_bytes, floor_bytes=floor, target_bytes=target)


def record_truncation(state: DurableAgentState, removed: int, *, now: datetime | None = None) -> None:
    """Accumulate bounded eviction evidence without discarding unknown metadata."""
    timestamp = (now or datetime.now(tz=timezone.utc)).isoformat()
    existing = state.data.truncation or {}
    state.data.truncation = {
        **existing,
        DurableStateFields.EVICTED_MESSAGE_COUNT: int(existing.get(DurableStateFields.EVICTED_MESSAGE_COUNT, 0))
        + removed,
        DurableStateFields.FIRST_EVICTED_AT: existing.get(DurableStateFields.FIRST_EVICTED_AT, timestamp),
        DurableStateFields.LAST_EVICTED_AT: timestamp,
    }


def _serialized_size(state: DurableAgentState) -> int:
    """Measure default JSON serialization, including ASCII escapes but excluding transport framing."""
    return len(json.dumps(state.to_dict()))


def _detached_message(stored: DurableAgentStateMessage) -> Message:
    message: Message = deepcopy(stored).to_chat_message()
    message.additional_properties.pop(EXCLUDED_KEY, None)
    # Recount with this tokenizer rather than trusting another strategy's cached token count.
    message.additional_properties.pop(GROUP_ANNOTATION_KEY, None)
    return message


def _group_id(message: Message) -> str:
    return cast("str", message.additional_properties[GROUP_ANNOTATION_KEY][GROUP_ID_KEY])


def _saved_group_id(stored: DurableAgentStateMessage) -> str | None:
    annotation = (stored.extension_data or {}).get(GROUP_ANNOTATION_KEY)
    if isinstance(annotation, Mapping):
        group_id = cast("Mapping[str, object]", annotation).get(GROUP_ID_KEY)
        if isinstance(group_id, str):
            return group_id
    return None


def _link_atomic_groups(messages: list[Message], saved_ids: list[str | None]) -> list[int]:
    """Unite core-inferred groups with persisted atomic links, including non-contiguous spans."""
    parents = list(range(len(messages)))

    def root(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    for group_ids in (saved_ids, [_group_id(message) for message in messages]):
        first: dict[str, int] = {}
        for index, group_id in enumerate(group_ids):
            if group_id is not None:
                left, right = root(first.setdefault(group_id, index)), root(index)
                parents[max(left, right)] = min(left, right)

    roots = [root(index) for index in range(len(messages))]
    for message, group in zip(messages, roots):
        annotation = cast("dict[str, Any]", message.additional_properties[GROUP_ANNOTATION_KEY])
        annotation[GROUP_ID_KEY] = f"retention_group_{group}"
        annotation[GROUP_INDEX_KEY] = group
    return roots


def _candidates(state: DurableAgentState, *, now: datetime) -> tuple[list[Message], list[_Origin]]:
    history = state.data.conversation_history
    completed = cast("Mapping[str, object] | None", getattr(state.data, "completed_correlations", None))
    protected = {id(entry) for entry in _protected_entries(history, completed_correlations=completed, now=now)}
    messages: list[Message] = []
    origins: list[_Origin | None] = []
    saved_ids: list[str | None] = []
    held: set[int] = set()
    reserved = {stored.message_id for entry in history for stored in entry.messages if stored.message_id}
    seen: set[str] = set()

    for entry_index, entry in enumerate(history):
        known = entry.json_type in _TRANSCRIPT_KINDS
        # Unknown entries are opaque barriers, not model-conversion inputs or deletion candidates.
        for message_index, stored in enumerate(entry.messages if known else (entry.messages or [None])):
            index = len(messages)
            eligible = known and stored is not None and bool(stored.contents)
            message = _detached_message(stored) if known and stored is not None else Message(_SYSTEM_ROLE, [])
            if not eligible or id(entry) in protected or message.role == _SYSTEM_ROLE:
                held.add(index)
            message_id = message.message_id
            if not message_id or message_id in seen:
                suffix = index
                message_id = f"retention_message_{suffix}"
                while message_id in reserved:
                    suffix += 1
                    message_id = f"retention_message_{suffix}"
                message.message_id = message_id
                reserved.add(message_id)
            seen.add(message_id)
            messages.append(message)
            origins.append((entry_index, message_index) if eligible else None)
            saved_ids.append(_saved_group_id(stored) if stored is not None else None)

    annotate_message_groups(messages, force_reannotate=True, tokenizer=CharacterEstimatorTokenizer())
    roots = _link_atomic_groups(messages, saved_ids)
    protected_groups = {roots[index] for index in held}
    candidates: list[Message] = []
    candidate_origins: list[_Origin] = []
    for index, origin in enumerate(origins):
        if origin is not None and roots[index] not in protected_groups:
            candidates.append(messages[index])
            candidate_origins.append(origin)
    return candidates, candidate_origins


def _can_drop_entry(entry: DurableAgentStateEntry) -> bool:
    # Only a bare transcript envelope may disappear with its final message. Keep usage,
    # response schemas, orchestration metadata and unknown fields in the protected floor.
    return (
        entry.json_type in _TRANSCRIPT_KINDS
        and not entry.extension_data
        and entry.to_dict().keys() <= _BARE_ENTRY_FIELDS
    )


def _stage_eviction(state: DurableAgentState, removed: set[_Origin]) -> DurableAgentState:
    staged = deepcopy(state)
    history: list[DurableAgentStateEntry] = []
    for entry_index, entry in enumerate(staged.data.conversation_history):
        remaining = [message for index, message in enumerate(entry.messages) if (entry_index, index) not in removed]
        changed = len(remaining) != len(entry.messages)
        entry.messages = remaining
        # Do not incidentally remove an already-empty or unknown entry.
        if remaining or not changed or not _can_drop_entry(entry):
            history.append(entry)
    staged.data.conversation_history = history
    return staged


def _prefix_sizes(
    state: DurableAgentState,
    origins: list[_Origin],
    groups: list[list[int]],
    *,
    size: int,
    record_cost: int,
) -> list[int]:
    """Compute default-JSON byte costs at core group boundaries without repeated whole-state copies."""
    history = state.data.conversation_history
    remaining = [len(entry.messages) for entry in history]
    entry_sizes = [len(json.dumps(entry.to_dict())) for entry in history]
    droppable = [_can_drop_entry(entry) for entry in history]
    entry_count = len(history)
    previous_count = int((state.data.truncation or {}).get(DurableStateFields.EVICTED_MESSAGE_COUNT, 0))
    final_count_digits = len(str(previous_count + len(origins)))
    removed = 0
    sizes: list[int] = []
    for group in groups:
        for index in group:
            entry_index, message_index = origins[index]
            if remaining[entry_index] == 1 and droppable[entry_index]:
                saved = entry_sizes[entry_index] + (2 if entry_count > 1 else 0)
                entry_count -= 1
            else:
                stored = history[entry_index].messages[message_index]
                saved = _message_size(stored) + (2 if remaining[entry_index] > 1 else 0)
                entry_sizes[entry_index] -= saved
            remaining[entry_index] -= 1
            size -= saved
            removed += 1
        # The timestamp and unknown truncation fields are fixed across plans. Only the
        # decimal width of the aggregate count varies with the chosen prefix.
        count_correction = len(str(previous_count + removed)) - final_count_digits
        sizes.append(size + record_cost + count_correction)
    return sizes


def _newest_exchange(history: list[DurableAgentStateEntry]) -> list[DurableAgentStateEntry]:
    """Return the entries belonging to the most recent exchange.

    Grouped by correlation id, so a request and the response it produced are protected together.

    Compaction entries answer no request and carry no correlation, so they are skipped when
    deciding which exchange is newest. Taking the last entry blindly would let a summary appended
    at the end stand in for the turn that actually just happened, leaving that turn unprotected.
    """
    for entry in reversed(history):
        if entry.json_type in _EXCHANGE_KINDS and entry.correlation_id is not None:
            newest = entry.correlation_id
            return [candidate for candidate in history if candidate.correlation_id == newest]
    return [history[-1]] if history else []


def _as_utc(value: datetime) -> datetime:
    """Persisted timestamps can come back without a timezone, so read those as UTC."""
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _protected_entries(
    history: list[DurableAgentStateEntry],
    *,
    completed_correlations: Mapping[str, object] | None = None,
    now: datetime | None = None,
) -> list[DurableAgentStateEntry]:
    """Protect the newest exchange and recent responses lacking independent completion records."""
    protected = list(_newest_exchange(history))
    completed = completed_correlations or {}
    cutoff = (now or datetime.now(tz=timezone.utc)) - timedelta(seconds=DELIVERY_WINDOW_SECONDS)
    responses = [
        entry
        for entry in history
        if entry.json_type in (DurableAgentStateEntryJsonType.RESPONSE, DurableAgentStateEntryJsonType.ERROR_RESPONSE)
        and entry.correlation_id not in completed
        and _as_utc(entry.created_at) > cutoff
    ]
    undelivered = {entry.correlation_id for entry in responses if entry.correlation_id is not None}
    response_ids = {id(entry) for entry in responses}
    protected_ids = {id(entry) for entry in protected}
    protected.extend(
        entry
        for entry in history
        if (id(entry) in response_ids or entry.correlation_id in undelivered) and id(entry) not in protected_ids
    )
    return protected


def _message_size(stored: DurableAgentStateMessage) -> int:
    """The persisted message payload size, including non-text contents and metadata."""
    return len(json.dumps(stored.to_dict()))


def _token_budget(
    origins: list[tuple[DurableAgentStateEntry, DurableAgentStateMessage]],
    *,
    serialized_size: int,
    evictable_bytes: int,
    target_bytes: int,
    floor_bytes: int | None = None,
    evictable_tokens: int | None = None,
) -> int:
    """Estimate tokens from persisted candidate bytes and core's actual token annotations.

    The optional measurements let the engine reuse its detached grouping pass and exact floor.
    The four original arguments remain usable by callers that only need a conservative estimate.
    """
    if evictable_bytes <= 0:
        return 1
    if floor_bytes is None:
        floor_bytes = max(serialized_size - evictable_bytes, 0)
    allowed_bytes = max(target_bytes - floor_bytes, 0)
    if evictable_tokens is None:
        messages = [_detached_message(stored) for _, stored in origins]
        annotate_message_groups(messages, tokenizer=CharacterEstimatorTokenizer())
        evictable_tokens = included_token_count(messages)
    return max(allowed_bytes * evictable_tokens // evictable_bytes, 1)
