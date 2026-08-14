# Copyright (c) Microsoft. All rights reserved.

"""Bounding durable entity state so an agent does not simply stop working at the backend limit.

Retention is a **capacity** concern, deliberately separate from compaction. Compaction decides what
the model should read. Retention decides what durable state can afford to hold. An exclusion made
for token cost is not consent to delete the record, so the two never share a decision.

See ADR 0032, "Retention".
"""

from __future__ import annotations

import json
import logging
import warnings
from typing import Literal, cast

from agent_framework import (
    CharacterEstimatorTokenizer,
    Message,
    TokenBudgetComposedStrategy,
)

from ._durable_agent_state import (
    DurableAgentState,
    DurableAgentStateEntry,
    DurableAgentStateMessage,
)
from ._history_provider import EXCLUDED_KEY, prune_messages, replayable_entries

logger = logging.getLogger("agent_framework.durabletask")

RetentionMode = Literal["keep_all", "auto", "follow_compaction"]
"""How much of the conversation durable state is allowed to discard.

``keep_all``
    Never delete. The entity may reach the backend limit and fail. The honest choice when the
    complete record matters more than availability.
``auto``
    Delete only under storage pressure, and only down to the low watermark. The default.
``follow_compaction``
    Also delete whatever compaction excluded, every turn.
"""

DEFAULT_RETENTION: RetentionMode = "auto"

DEFAULT_MAX_STATE_BYTES = 1_048_576
"""The Durable Task Scheduler message limit. Raise it when large payload offload is configured."""

HIGH_WATERMARK = 0.85
"""Fraction of the budget that triggers eviction.

Below 0.9 because the budget is approximate twice over, once in the byte-to-token estimate and once
because a message's non-text content is not counted when calibrating that estimate.
"""

LOW_WATERMARK = 0.70
"""Fraction of the budget to evict down to.

The gap from the high watermark is hysteresis. Evicting to just under the trigger would evict again
on every subsequent turn.
"""

_BYTES_PER_TOKEN = 4
"""Matches ``CharacterEstimatorTokenizer``, which is a flat 4 characters per token."""

_MAX_PASSES = 3
"""Eviction re-measures rather than trusting the estimate, but must not loop indefinitely."""


def prunes_excluded(retention: RetentionMode) -> bool:
    """Whether compaction exclusions should be deleted as they are made."""
    return retention == "follow_compaction"


def resolve_retention(retention: RetentionMode, prune_history: bool | None) -> RetentionMode:
    """Fold the deprecated ``prune_history`` flag into the retention setting.

    Args:
        retention: The retention mode the caller asked for.
        prune_history: The deprecated flag, or None when it was not supplied.

    Returns:
        The effective retention mode.
    """
    if prune_history is None:
        return retention
    warnings.warn(
        "prune_history is deprecated; use retention='follow_compaction' to delete what compaction "
        "excluded, or retention='keep_all' to never delete.",
        DeprecationWarning,
        stacklevel=3,
    )
    return "follow_compaction" if prune_history else retention


async def enforce_budget(state: DurableAgentState, *, max_state_bytes: int = DEFAULT_MAX_STATE_BYTES) -> int:
    """Evict oldest conversation groups when persisted state approaches the backend limit.

    The measurement is exact rather than estimated. Serializing state at the 1 MB limit costs a few
    milliseconds against a turn dominated by a model call, and ``to_dict()`` already runs on every
    persist, so the incremental cost is small and only paid once per turn.

    Args:
        state: The entity state, modified in place.

    Keyword Args:
        max_state_bytes: The budget for serialized state.

    Returns:
        How many messages were removed. Zero is the common case.
    """
    high = int(max_state_bytes * HIGH_WATERMARK)
    size = _serialized_size(state)
    if size < high:
        return 0

    history = state.data.conversation_history
    target = int(max_state_bytes * LOW_WATERMARK)
    removed = 0

    for attempt in range(_MAX_PASSES):
        # Tighten on each pass, since the byte-to-token conversion is a heuristic and a first
        # attempt can land short of the target.
        evicted = await _evict_once(history, serialized_size=size, target_bytes=target >> attempt)
        if not evicted:
            break
        removed += evicted
        size = _serialized_size(state)
        if size < high:
            break

    if removed:
        logger.warning(
            "[Retention] Durable state reached %d bytes of a %d budget, so %d message(s) were "
            "evicted oldest-first to %d bytes. Configure retention='keep_all' to disable this, or "
            "raise max_state_bytes if large payload offload is enabled.",
            high,
            max_state_bytes,
            removed,
            size,
        )
    elif size >= high:
        logger.error(
            "[Retention] Durable state is %d bytes against a %d budget and nothing could be "
            "evicted. A single turn is likely larger than the budget itself, which retention "
            "cannot resolve.",
            size,
            max_state_bytes,
        )
    return removed


def _serialized_size(state: DurableAgentState) -> int:
    """Measure the state exactly as it will be persisted."""
    return len(json.dumps(state.to_dict()))


async def _evict_once(
    history: list[DurableAgentStateEntry],
    *,
    serialized_size: int,
    target_bytes: int,
) -> int:
    """Run one eviction pass, returning how many messages were removed.

    Core already knows how to drop oldest groups to a budget while preserving system messages and
    keeping tool-call groups whole, so that judgement is borrowed rather than reimplemented.
    """
    candidates: list[Message] = []
    origins: list[tuple[DurableAgentStateEntry, DurableAgentStateMessage]] = []
    protected = _newest_exchange(history)
    for entry, index in replayable_entries(history):
        if entry in protected:
            # Never evict the exchange that just happened. Core's budget fallback will drop
            # everything if the budget demands it, and losing the current turn would break
            # response polling and discard the result the caller is waiting for.
            continue
        stored = entry.messages[index]
        message = cast("Message", stored.to_chat_message())
        # The budget is computed over *included* messages, so a user's own compaction exclusions
        # would make an over-budget conversation look empty. Clearing them here makes the budget
        # reflect what is stored. This is a detached copy, so the persisted annotation is untouched.
        message.additional_properties.pop(EXCLUDED_KEY, None)
        candidates.append(message)
        origins.append((entry, stored))

    if not candidates:
        return 0

    strategy = TokenBudgetComposedStrategy(
        token_budget=_token_budget(candidates, serialized_size=serialized_size, target_bytes=target_bytes),
        tokenizer=CharacterEstimatorTokenizer(),
        # No strategies, so this goes straight to core's deterministic oldest-group eviction.
        # Passing the user's strategy would satisfy the budget immediately under early stop, and
        # everything it had excluded for context reasons would then be deleted.
        strategies=[],
    )
    await strategy(candidates)

    evicted = [
        origins[position]
        for position, message in enumerate(candidates)
        if message.additional_properties.get(EXCLUDED_KEY)
    ]
    if not evicted:
        return 0
    prune_messages(history, evicted)
    return len(evicted)


def _newest_exchange(history: list[DurableAgentStateEntry]) -> list[DurableAgentStateEntry]:
    """Return the entries belonging to the most recent exchange.

    Grouped by correlation id, so a request and the response it produced are protected together.
    """
    if not history:
        return []
    newest = history[-1].correlation_id
    if newest is None:
        return [history[-1]]
    return [entry for entry in history if entry.correlation_id == newest]


def _token_budget(candidates: list[Message], *, serialized_size: int, target_bytes: int) -> int:
    """Convert a byte budget into the token budget the strategy expects.

    Serialized state is larger than the text it contains, because of keys, escaping, ids and
    annotations. Rather than assume an overhead constant, the ratio is measured from the state in
    hand, so a conversation of long prose and one full of tool-call metadata are both handled.
    """
    content_chars = sum(len(message.text or "") for message in candidates)
    ratio = (content_chars / serialized_size) if serialized_size else 1.0
    return max(int(target_bytes * ratio) // _BYTES_PER_TOKEN, 1)
