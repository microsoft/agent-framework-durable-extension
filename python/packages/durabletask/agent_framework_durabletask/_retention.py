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
from datetime import datetime, timedelta, timezone
from typing import Literal, cast

from agent_framework import (
    CharacterEstimatorTokenizer,
    Message,
    TokenBudgetComposedStrategy,
)

from ._constants import DurableStateFields
from ._durable_agent_state import (
    DurableAgentState,
    DurableAgentStateEntry,
    DurableAgentStateMessage,
    DurableAgentStateResponse,
)
from ._history_provider import EXCLUDED_KEY, prune_messages, replayable_entries

logger = logging.getLogger("agent_framework.durabletask")

DELIVERY_WINDOW_SECONDS = 60
"""How long a completed response stays safe from eviction.

A caller reads its response by correlation id, from outside the entity, and has no way to say it
has finished reading. So the entity cannot know a response was collected, only that enough time
has passed that nobody plausibly still wants it. Until then the response is not evictable, or a
run that succeeded would be reported to its caller as a timeout.

The exposure this covers is smaller than a caller's total wait. Callers poll roughly once a
second, so a response normally has to survive only until the next poll. The window is generous
against that, which leaves room for a client that stalls or retries, while staying short enough
that a busy session ages entries out rather than pinning them and defeating the budget.
"""

RetentionMode = Literal["keep_all", "auto", "follow_compaction"]
"""How much of the conversation durable state is allowed to discard.

``keep_all``
    Never delete. The entity may reach the backend limit and fail. The honest choice when the
    complete record matters more than availability.
``auto``
    Delete only under storage pressure, and only down to the low watermark. The default.
``follow_compaction``
    Delete whatever compaction excluded every turn, then use the same pressure eviction as
    ``auto`` if the remaining state is still too large.
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

_SYSTEM_ROLE = "system"
"""Role of the messages retention refuses to evict, whatever the budget says."""

_MAX_PASSES = 3
"""Eviction re-measures rather than trusting the estimate, but must not loop indefinitely."""


def prunes_excluded(retention: RetentionMode) -> bool:
    """Whether compaction exclusions should be deleted as they are made."""
    return retention == "follow_compaction"


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
    removed: list[str] = []

    for attempt in range(_MAX_PASSES):
        # Tighten on each pass, since the byte-to-token conversion is a heuristic and a first
        # attempt can land short of the target.
        evicted = await _evict_once(history, serialized_size=size, target_bytes=target >> attempt)
        if not evicted:
            break
        removed.extend(evicted)
        size = _serialized_size(state)
        if size < high:
            break

    undelivered_sacrificed = 0
    if size >= high:
        # Holding a response back for its caller is a strong preference, not a promise that
        # outranks staying storable. A conversation busy enough to fill the budget inside the
        # delivery window would otherwise protect everything and evict nothing, and state that
        # cannot be persisted ends the session for every caller. Losing one response costs the
        # caller a retry, so that is the cheaper failure.
        forced = await _evict_once(history, serialized_size=size, target_bytes=target, honor_delivery_window=False)
        if forced:
            undelivered_sacrificed = len(forced)
            removed.extend(forced)
            size = _serialized_size(state)

    if removed:
        _record_truncation(state, len(removed))
        logger.warning(
            "[Retention] Durable state passed %d bytes of a %d budget, so %d message(s) were "
            "evicted oldest-first (%s .. %s), leaving %d bytes. Set retention='keep_all' to "
            "disable this, or raise max_state_bytes if large payload offload is enabled.",
            high,
            max_state_bytes,
            len(removed),
            removed[0],
            removed[-1],
            size,
        )
    if undelivered_sacrificed:
        logger.error(
            "[Retention] Staying inside the %d byte budget required evicting %d message(s) from "
            "responses completed in the last %d seconds, which their callers may not have read "
            "yet. Those callers will see a missing response and need to retry. This means turns "
            "are arriving faster than the budget can hold them, so raise max_state_bytes.",
            max_state_bytes,
            undelivered_sacrificed,
            DELIVERY_WINDOW_SECONDS,
        )
    if size >= high:
        # Reported whether or not anything was evicted. Retention did what it could and the state
        # is still over budget, so the next write is the one that fails, and saying so here is the
        # only warning anybody gets.
        logger.error(
            "[Retention] Durable state is still %d bytes against a %d budget after retention ran. "
            "The exchange in flight is never evicted, so a single turn larger than the budget "
            "cannot be resolved this way. Raise max_state_bytes or reduce what each turn stores.",
            size,
            max_state_bytes,
        )
    return len(removed)


def _record_truncation(state: DurableAgentState, removed: int) -> None:
    """Record in the state itself that this conversation is no longer complete.

    Eviction is a lossy act performed by the runtime rather than by the user, and a log line is
    only evidence to whoever happened to be watching at the time. Anyone reading this state later,
    including the user asking why an answer lost context, needs to be able to tell that content
    was removed. So the fact is persisted alongside the conversation.

    Deliberately a counter and two timestamps rather than a list of what went. A list would grow
    without bound in exactly the situation where state is already too large, which is the problem
    this is part of solving. The absence of the record is itself meaningful: it says nothing has
    ever been dropped.

    Args:
        state: The entity state, modified in place.
        removed: How many messages this pass evicted.
    """
    now = datetime.now(tz=timezone.utc).isoformat()
    existing = state.data.truncation or {}
    state.data.truncation = {
        DurableStateFields.EVICTED_MESSAGE_COUNT: int(existing.get(DurableStateFields.EVICTED_MESSAGE_COUNT, 0))
        + removed,
        DurableStateFields.FIRST_EVICTED_AT: existing.get(DurableStateFields.FIRST_EVICTED_AT, now),
        DurableStateFields.LAST_EVICTED_AT: now,
    }


def _serialized_size(state: DurableAgentState) -> int:
    """Measure the state exactly as it will be persisted.

    Counting characters is counting bytes here. ``json.dumps`` escapes non-ASCII by default, so
    the result is pure ASCII, and the durable SDK serializes state with that same default. Text in
    any language therefore costs the same against this budget as it does in storage.
    """
    return len(json.dumps(state.to_dict()))


async def _evict_once(
    history: list[DurableAgentStateEntry],
    *,
    serialized_size: int,
    target_bytes: int,
    honor_delivery_window: bool = True,
) -> list[str]:
    """Run one eviction pass, returning the ids of the messages removed.

    Core already knows how to drop oldest groups to a budget while preserving system messages and
    keeping tool-call groups whole, so that judgement is borrowed rather than reimplemented.

    Args:
        history: The conversation history, modified in place.

    Keyword Args:
        serialized_size: Current size of the whole serialized state, used to relate bytes to text.
        target_bytes: The size this pass is aiming to reach.
        honor_delivery_window: When False, responses whose callers may still be reading them
            become evictable. Reserved for the case where protecting them would leave state too
            large to persist at all.

    Returns:
        The ids of the messages this pass removed.
    """
    candidates: list[Message] = []
    origins: list[tuple[DurableAgentStateEntry, DurableAgentStateMessage]] = []
    evictable_bytes = 0
    protected = _protected_entries(history, honor_delivery_window=honor_delivery_window)
    for entry, index in replayable_entries(history):
        if entry in protected:
            # Never evict the exchange that just happened, nor one whose caller could still be
            # reading it. Core's budget fallback will drop everything if the budget demands it,
            # and losing either would discard a result somebody is waiting for.
            continue
        stored = entry.messages[index]
        if stored.role == _SYSTEM_ROLE:
            # Kept out of the candidate set rather than trusted to core's protection. Core skips
            # system groups in its first fallback but its *strict* fallback exists precisely to
            # evict them, so a budget small enough to reach that stage would delete the agent's
            # instructions. Excluded here, they are simply not evictable, and their bytes count
            # toward the floor instead.
            continue
        message = cast("Message", stored.to_chat_message())
        # The budget is computed over *included* messages, so a user's own compaction exclusions
        # would make an over-budget conversation look empty. Clearing them here makes the budget
        # reflect what is stored. This is a detached copy, so the persisted annotation is untouched.
        message.additional_properties.pop(EXCLUDED_KEY, None)
        candidates.append(message)
        origins.append((entry, stored))
        evictable_bytes += _message_size(stored)

    if not candidates:
        return []

    strategy = TokenBudgetComposedStrategy(
        token_budget=_token_budget(
            origins,
            serialized_size=serialized_size,
            evictable_bytes=evictable_bytes,
            target_bytes=target_bytes,
        ),
        tokenizer=CharacterEstimatorTokenizer(),
        # No strategies, so this goes straight to core's deterministic oldest-group eviction.
        # Passing the user's strategy would satisfy the budget immediately under early stop, and
        # everything it had excluded for context reasons would then be deleted.
        strategies=[],
    )
    await strategy(candidates)

    evicted = [
        (position, origins[position])
        for position, message in enumerate(candidates)
        if message.additional_properties.get(EXCLUDED_KEY)
    ]
    if not evicted:
        return []
    prune_messages(history, [origin for _, origin in evicted])
    return [candidates[position].message_id or "<no id>" for position, _ in evicted]


def _newest_exchange(history: list[DurableAgentStateEntry]) -> list[DurableAgentStateEntry]:
    """Return the entries belonging to the most recent exchange.

    Grouped by correlation id, so a request and the response it produced are protected together.

    Compaction entries answer no request and carry no correlation, so they are skipped when
    deciding which exchange is newest. Taking the last entry blindly would let a summary appended
    at the end stand in for the turn that actually just happened, leaving that turn unprotected.
    """
    for entry in reversed(history):
        if entry.correlation_id is not None:
            newest = entry.correlation_id
            return [candidate for candidate in history if candidate.correlation_id == newest]
    return [history[-1]] if history else []


def _as_utc(value: datetime) -> datetime:
    """Persisted timestamps can come back without a timezone, so read those as UTC."""
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _protected_entries(
    history: list[DurableAgentStateEntry], *, honor_delivery_window: bool = True
) -> list[DurableAgentStateEntry]:
    """Return the entries retention is not allowed to evict.

    Two reasons an entry is off limits. It belongs to the exchange that just happened, which is
    absolute because its caller is waiting on this very operation. Or it is a response recent
    enough that its caller could still be polling for it, which is a preference that yields when
    honoring it would leave state too large to persist.

    Protection is by correlation, so a reply is never kept without the request that produced it.
    """
    protected = list(_newest_exchange(history))
    if not honor_delivery_window:
        return protected

    cutoff = datetime.now(tz=timezone.utc) - timedelta(seconds=DELIVERY_WINDOW_SECONDS)
    undelivered = {
        entry.correlation_id
        for entry in history
        if isinstance(entry, DurableAgentStateResponse)
        and entry.correlation_id is not None
        and _as_utc(entry.created_at) > cutoff
    }
    if undelivered:
        protected.extend(entry for entry in history if entry.correlation_id in undelivered and entry not in protected)
    return protected


def _message_size(stored: DurableAgentStateMessage) -> int:
    """Bytes this message contributes to persisted state.

    Measured the same way the whole state is measured, so the two are directly comparable.
    """
    return len(json.dumps(stored.to_dict()))


def _token_budget(
    origins: list[tuple[DurableAgentStateEntry, DurableAgentStateMessage]],
    *,
    serialized_size: int,
    evictable_bytes: int,
    target_bytes: int,
) -> int:
    """Convert a byte budget into the token budget the strategy expects.

    The budget has to be expressed in tokens because that is what the strategy counts, but the
    constraint being enforced is a byte limit. So the conversion is measured from the messages in
    hand rather than assumed.

    Only part of the state is evictable. Envelopes, the exchange in flight, responses inside the
    delivery window and system messages all stay no matter what, so their bytes are a floor the
    budget cannot reach below. What is left is what the evictable messages are allowed to occupy.

    Tokens are related to bytes by the same shape core uses, ``max(1, size // 4)`` per message,
    applied to the persisted form. Taking the ratio from these specific messages is what makes a
    conversation of tool calls behave like one of prose. An earlier version used ``message.text``
    as the numerator, which is empty for tool calls, so a tool-only history produced a budget of
    one token and evicted everything it was allowed to touch.

    Args:
        origins: The evictable messages, each with the entry that owns it.

    Keyword Args:
        serialized_size: Current size of the whole serialized state.
        evictable_bytes: How much of that size the evictable messages account for.
        target_bytes: The size this pass is aiming to reach.

    Returns:
        A token budget of at least one.
    """
    if evictable_bytes <= 0:
        return 1
    floor_bytes = max(serialized_size - evictable_bytes, 0)
    allowed_bytes = max(target_bytes - floor_bytes, 0)
    evictable_tokens = sum(max(1, _message_size(stored) // _BYTES_PER_TOKEN) for _, stored in origins)
    return max(int(allowed_bytes * evictable_tokens / evictable_bytes), 1)
