# Copyright (c) Microsoft. All rights reserved.

"""Tests for retention (ADR-0032, "Retention").

Retention bounds durable entity state so an agent does not simply stop working when it reaches the
backend limit. It is a capacity concern and deliberately separate from compaction: an exclusion made
for token cost is not consent to delete the record.
"""

import json
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any, cast

from agent_framework import (
    Agent,
    BaseChatClient,
    ChatResponse,
    ChatResponseUpdate,
    Content,
    Message,
    ResponseStream,
)

from agent_framework_durabletask import (
    AgentEntity,
    AgentEntityStateProviderMixin,
    DurableAgentState,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
)
from agent_framework_durabletask._retention import (
    HIGH_WATERMARK,
    LOW_WATERMARK,
    enforce_budget,
    prunes_excluded,
)

BUDGET = 40_000
"""Small enough to keep these tests fast, large enough to hold a realistic conversation."""


def _state(turns: int, *, chars: int = 400, excluded_before: int = 0, excluded_recent: int = 0) -> DurableAgentState:
    """Build entity state with the given number of user/assistant turns.

    Args:
        turns: How many exchanges to record.
        chars: Size of each message's text.

    Keyword Args:
        excluded_before: Mark this many leading messages as compaction-excluded, as a user's own
            sliding window would.
        excluded_recent: Mark this many of the most recent messages as compaction-excluded, as a
            tool-result strategy can do without touching the oldest turns.

    Returns:
        The populated state.
    """
    state = DurableAgentState()
    now = datetime.now(tz=timezone.utc)
    marked = 0
    for index in range(turns):
        request = DurableAgentStateRequest(
            correlation_id=f"c{index}",
            created_at=now,
            messages=[
                DurableAgentStateMessage.from_chat_message(
                    Message(role="user", contents=["u" * chars], message_id=f"u{index}")
                )
            ],
        )
        response = DurableAgentStateResponse(
            correlation_id=f"c{index}",
            created_at=now,
            messages=[
                DurableAgentStateMessage.from_chat_message(
                    Message(role="assistant", contents=["a" * chars], message_id=f"a{index}")
                )
            ],
        )
        for entry in (request, response):
            for stored in entry.messages:
                if marked < excluded_before:
                    stored.extension_data = {"_excluded": True, "_excluded_reason": "sliding_window"}
                    marked += 1
        state.data.conversation_history.extend([request, response])

    if excluded_recent:
        stored_messages = [m for entry in state.data.conversation_history for m in entry.messages]
        for stored in stored_messages[-excluded_recent:]:
            stored.extension_data = {"_excluded": True, "_excluded_reason": "tool_result_compaction"}
    return state


def _size(state: DurableAgentState) -> int:
    return len(json.dumps(state.to_dict()))


def _message_ids(state: DurableAgentState) -> list[str]:
    return [m.message_id or "" for entry in state.data.conversation_history for m in entry.messages]


class TestRetentionModes:
    """The mode decides whether an exclusion may become a deletion."""

    def test_only_follow_compaction_prunes_on_write(self) -> None:
        assert prunes_excluded("follow_compaction") is True
        assert prunes_excluded("auto") is False
        assert prunes_excluded("keep_all") is False

    def test_the_default_is_auto(self) -> None:
        """Which is the deliberate behavior change: previously nothing bounded storage."""
        from agent_framework_durabletask import DurableAIAgentWorker

        assert DurableAIAgentWorker(cast(Any, object()))._retention == "auto"


class TestBudgetEnforcement:
    """Nothing happens until state is genuinely close to the limit."""

    async def test_below_the_watermark_nothing_is_touched(self) -> None:
        state = _state(turns=4)
        before = _message_ids(state)

        removed = await enforce_budget(state, max_state_bytes=BUDGET)

        assert removed == 0
        assert _message_ids(state) == before

    async def test_over_the_watermark_evicts_to_the_low_watermark(self) -> None:
        state = _state(turns=60)
        assert _size(state) > BUDGET * HIGH_WATERMARK, "the fixture must start over the trigger"

        removed = await enforce_budget(state, max_state_bytes=BUDGET)

        assert removed > 0
        assert _size(state) < BUDGET * HIGH_WATERMARK, "eviction did not get back under the trigger"

    async def test_the_newest_turn_survives(self) -> None:
        """Evicting the turn that just happened would defeat the point of running it."""
        state = _state(turns=60)

        await enforce_budget(state, max_state_bytes=BUDGET)

        assert _message_ids(state)[-1] == "a59"

    async def test_eviction_is_hysteretic(self) -> None:
        """Evicting to just under the trigger would evict again on every following turn."""
        state = _state(turns=60)
        await enforce_budget(state, max_state_bytes=BUDGET)

        second = await enforce_budget(state, max_state_bytes=BUDGET)

        assert second == 0, "a second pass evicted again immediately, so there is no headroom"

    async def test_keep_all_is_the_caller_s_decision(self) -> None:
        """``keep_all`` is enforced by the entity, so the budget helper itself always acts."""
        state = _state(turns=60)

        assert await enforce_budget(state, max_state_bytes=BUDGET) > 0


class TestExclusionsAreNotConsentToDelete:
    """A context decision must not silently become a storage decision."""

    async def test_a_user_s_exclusions_survive_eviction(self) -> None:
        """The budget is measured over a detached copy, so stored annotations are untouched.

        Exclusions are placed on recent messages here, which a tool-result strategy does, so they
        sit inside the window eviction keeps. Had the annotation itself been the criterion they
        would have gone regardless of where they were.
        """
        state = _state(turns=60, excluded_recent=6)

        await enforce_budget(state, max_state_bytes=BUDGET)

        surviving = [
            stored
            for entry in state.data.conversation_history
            for stored in entry.messages
            if (stored.extension_data or {}).get("_excluded")
        ]
        assert surviving, "every excluded message was evicted, so exclusion was treated as consent"
        assert all((s.extension_data or {}).get("_excluded_reason") == "tool_result_compaction" for s in surviving)

    async def test_eviction_is_not_limited_to_what_compaction_excluded(self) -> None:
        """The budget is computed over everything stored, not just the included messages.

        A user's own window can mark almost everything excluded. If those exclusions were left in
        place the strategy would see a tiny included set, conclude it was already under budget, and
        evict nothing while state kept growing.
        """
        state = _state(turns=60, excluded_before=110)

        removed = await enforce_budget(state, max_state_bytes=BUDGET)

        assert removed > 0, "prior exclusions hid the real size and nothing was evicted"


class TestSingleOversizedTurn:
    """Retention cannot save a conversation whose newest turn alone exceeds the budget."""

    async def test_the_current_turn_is_never_evicted(self) -> None:
        """Core's fallback will drop everything if asked, which would lose the result being polled."""
        state = _state(turns=1, chars=BUDGET * 2)

        removed = await enforce_budget(state, max_state_bytes=BUDGET)

        assert removed == 0
        assert _message_ids(state) == ["u0", "a0"], "the turn that just ran was evicted"

    async def test_an_oversized_newest_turn_does_not_take_the_history_with_it(self) -> None:
        state = _state(turns=10)
        state.data.conversation_history.extend(_state(turns=1, chars=BUDGET * 2).data.conversation_history)

        await enforce_budget(state, max_state_bytes=BUDGET)

        assert _message_ids(state)[-2:] == ["u0", "a0"], "the newest exchange must survive"


class TestStateShape:
    """Eviction must leave durable state usable."""

    async def test_empty_entries_are_removed(self) -> None:
        state = _state(turns=60)

        await enforce_budget(state, max_state_bytes=BUDGET)

        assert all(entry.messages for entry in state.data.conversation_history)

    async def test_state_still_round_trips(self) -> None:
        state = _state(turns=60)

        await enforce_budget(state, max_state_bytes=BUDGET)

        restored: Any = DurableAgentState.from_dict(state.to_dict())
        assert _message_ids(restored) == _message_ids(state)

    async def test_nothing_is_evicted_from_an_empty_conversation(self) -> None:
        assert await enforce_budget(DurableAgentState(), max_state_bytes=BUDGET) == 0


def test_watermarks_leave_room_to_work() -> None:
    """The gap between them is what stops eviction running on every turn."""
    assert 0 < LOW_WATERMARK < HIGH_WATERMARK < 1


class _VerboseClient(BaseChatClient):
    """A client whose answers are long enough to reach the budget in a handful of turns."""

    def __init__(self, *, reply_chars: int = 4_000) -> None:
        super().__init__()
        self._reply_chars = reply_chars

    def _inner_get_response(self, *, messages: Any, stream: bool, options: Any, **kwargs: Any) -> Any:
        del options, kwargs
        # Keyed off the question rather than a counter, so a retried call answers the same thing.
        asked = next(
            (m.text for m in reversed(list(messages)) if str(getattr(m.role, "value", m.role)) == "user"),
            "?",
        )
        body = f"answering:{asked} " + ("x" * self._reply_chars)
        if stream:

            async def _updates() -> AsyncIterator[ChatResponseUpdate]:
                yield ChatResponseUpdate(role="assistant", contents=[Content.from_text(text=body)])

            return ResponseStream(_updates(), finalizer=ChatResponse.from_updates)

        async def _response() -> ChatResponse:
            return ChatResponse(messages=[Message(role="assistant", contents=[body])])

        return _response()


class _EntityState(AgentEntityStateProviderMixin):
    def __init__(self) -> None:
        self._state_dict: dict[str, Any] = {}

    def _get_state_dict(self) -> dict[str, Any]:
        return self._state_dict

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        # The real provider hands state to the SDK, which serializes it eagerly.
        json.dumps(state)
        self._state_dict = state

    def _get_session_id_from_entity(self) -> str:
        return "retention-e2e"


class TestTheWholeLoopStaysUnderBudget:
    """Drives the real entity, not just enforce_budget, because the value is in the wiring."""

    LIMIT = 60_000
    TURNS = 20

    async def _drive(self, **entity_kwargs: Any) -> tuple[_EntityState, list[str]]:
        client = _VerboseClient()
        agent = Agent(client=cast(Any, client), name="verbose")
        provider = _EntityState()
        entity = AgentEntity(agent, state_provider=provider, **entity_kwargs)

        replies: list[str] = []
        for turn in range(self.TURNS):
            result = await entity.run({"message": f"question {turn}", "correlationId": f"corr-{turn}"})
            replies.append(result.text)
        return provider, replies

    async def test_state_stays_bounded_across_many_turns(self) -> None:
        provider, _ = await self._drive(max_state_bytes=self.LIMIT)
        assert len(json.dumps(provider._get_state_dict())) <= self.LIMIT

    async def test_follow_compaction_falls_back_to_pressure_eviction(self) -> None:
        """With nothing to prune, only the shared pressure fallback can bound this run."""
        provider, _ = await self._drive(retention="follow_compaction", max_state_bytes=self.LIMIT)
        state = DurableAgentState.from_dict(provider._get_state_dict())

        assert len(json.dumps(provider._get_state_dict())) <= self.LIMIT
        assert 0 < len(state.data.conversation_history) < self.TURNS * 2

    async def test_every_turn_still_gets_its_own_answer(self) -> None:
        """Eviction must not disturb the response the caller is waiting on."""
        _, replies = await self._drive(max_state_bytes=self.LIMIT)
        assert [r.split(" x")[0] for r in replies] == [f"answering:question {i}" for i in range(self.TURNS)]

    async def test_history_is_actually_trimmed_not_just_small(self) -> None:
        """Without this the bounded assertion above could pass for the wrong reason."""
        provider, _ = await self._drive(max_state_bytes=self.LIMIT)
        kept = len(DurableAgentState.from_dict(provider._get_state_dict()).data.conversation_history)
        assert 0 < kept < self.TURNS * 2

    async def test_keep_all_lets_it_grow_past_the_limit(self) -> None:
        """Proves the run is genuinely over budget, so the bounded case is a real result."""
        provider, _ = await self._drive(retention="keep_all", max_state_bytes=self.LIMIT)
        assert len(json.dumps(provider._get_state_dict())) > self.LIMIT
