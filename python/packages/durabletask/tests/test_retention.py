# Copyright (c) Microsoft. All rights reserved.

"""Tests for retention (ADR-0032, "Retention").

Retention bounds durable entity state so an agent does not simply stop working when it reaches the
backend limit. It is a capacity concern and deliberately separate from compaction: an exclusion made
for token cost is not consent to delete the record.
"""

import json
from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
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
    DurableAgentStateErrorResponse,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
)
from agent_framework_durabletask._retention import (
    HIGH_WATERMARK,
    LOW_WATERMARK,
    _token_budget,
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
    # Turns are spaced a minute apart rather than all stamped "now". Retention refuses to evict a
    # response recent enough that its caller could still be reading it, so a conversation where
    # every turn happened this instant is entirely protected and nothing can be evicted at all.
    # Real conversations are spread over time, and the tests need to look like one.
    marked = 0
    for index in range(turns):
        occurred_at = now - timedelta(minutes=turns - index)
        request = DurableAgentStateRequest(
            correlation_id=f"c{index}",
            created_at=occurred_at,
            messages=[
                DurableAgentStateMessage.from_chat_message(
                    Message(role="user", contents=["u" * chars], message_id=f"u{index}")
                )
            ],
        )
        response = DurableAgentStateResponse(
            correlation_id=f"c{index}",
            created_at=occurred_at,
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

    async def test_every_surviving_exclusion_keeps_its_annotation(self) -> None:
        """Measuring the budget must not strip annotations off the messages it measured.

        To size the conversation, eviction clears ``_excluded`` on the message copies it hands to
        the strategy. That is only safe while those really are copies. If the copy ever shared its
        annotations with stored state, the clear would erase compaction's work from storage.

        Asserting merely that *some* exclusion survives is too weak to catch that: the newest
        exchange is never a candidate, so its annotations would survive either way. This checks
        every message that outlived eviction, which includes ones that were candidates.
        """
        state = _state(turns=60, excluded_recent=40)
        excluded_before_run = {
            stored.message_id
            for entry in state.data.conversation_history
            for stored in entry.messages
            if (stored.extension_data or {}).get("_excluded")
        }

        removed = await enforce_budget(state, max_state_bytes=BUDGET)
        assert removed > 0, "nothing was evicted, so the measuring path never ran"

        still_stored = {
            stored.message_id: stored for entry in state.data.conversation_history for stored in entry.messages
        }
        survivors = excluded_before_run & still_stored.keys()
        assert survivors, "every excluded message was evicted, so this proves nothing"

        stripped = [
            message_id
            for message_id in survivors
            if not (still_stored[message_id].extension_data or {}).get("_excluded")
        ]
        assert not stripped, f"eviction erased stored compaction annotations from {len(stripped)} message(s)"


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


class TestAResponseIsNotEvictedBeforeItsCallerReadsIt:
    """A caller reads its response by correlation id, from outside the entity.

    Nothing tells the entity that a response was collected, so a turn completing is not permission
    to delete the previous one. Evicting a response somebody is still polling for turns a run that
    succeeded into a client timeout.
    """

    async def test_a_recent_response_is_not_evicted(self) -> None:
        """The turn is early in the conversation, so oldest-first eviction reaches it.

        That is the whole point. Picking a recent turn would prove nothing, because eviction would
        never have got that far and the test would pass with no protection at all.
        """
        state = _state(turns=60)
        # Second oldest turn, so it is squarely inside what eviction removes, but it completed
        # seconds ago, so its caller may still be polling for it.
        early = state.data.conversation_history[2:4]
        for entry in early:
            entry.created_at = datetime.now(tz=timezone.utc)
        correlation = early[0].correlation_id
        assert correlation is not None
        assert state.try_get_agent_response(correlation) is not None

        removed = await enforce_budget(state, max_state_bytes=BUDGET)

        assert removed > 0, "nothing was evicted, so this proves nothing"
        assert state.try_get_agent_response(correlation) is not None, (
            "a response completed seconds ago was evicted before its caller could read it"
        )

    async def test_an_old_response_is_still_evictable(self) -> None:
        """Protection has to expire, or a long conversation could never be trimmed at all."""
        state = _state(turns=60)

        removed = await enforce_budget(state, max_state_bytes=BUDGET)

        assert removed > 0
        assert state.try_get_agent_response("c0") is None, "an ancient response was kept forever"

    async def test_the_budget_wins_when_protection_cannot_be_honored(self) -> None:
        """Turns arriving faster than the window can age them out must not pin state.

        Losing a response costs one caller a retry. State too large to persist ends the session
        for every caller, so protection yields rather than letting that happen.
        """
        state = _state(turns=60)
        # Every turn happened just now, which is what a busy session looks like.
        for entry in state.data.conversation_history:
            entry.created_at = datetime.now(tz=timezone.utc)

        removed = await enforce_budget(state, max_state_bytes=BUDGET)

        assert removed > 0, "protection was treated as absolute and state stayed over budget"
        assert _size(state) <= BUDGET

    async def test_a_failed_turn_is_protected_too(self) -> None:
        """The caller waiting on a failed turn still needs to be told it failed."""
        state = _state(turns=60)
        failure = DurableAgentStateErrorResponse(
            correlation_id="boom",
            created_at=datetime.now(tz=timezone.utc),
            messages=[
                DurableAgentStateMessage.from_chat_message(
                    Message(role="assistant", contents=["it broke"], message_id="err0")
                )
            ],
        )
        # Early in the conversation, where eviction would otherwise reach it.
        state.data.conversation_history.insert(2, failure)

        removed = await enforce_budget(state, max_state_bytes=BUDGET)

        assert removed > 0
        assert state.try_get_agent_response("boom") is not None


def _tool_state(turns: int, *, chars: int = 400) -> DurableAgentState:
    """Build a history of tool calls, which carry real bytes but no ``message.text``.

    This is the shape that broke the budget. A function call serializes to as much storage as
    prose of the same length, but reading ``.text`` off it returns an empty string.
    """
    state = DurableAgentState()
    now = datetime.now(tz=timezone.utc)
    for index in range(turns):
        occurred_at = now - timedelta(minutes=turns - index)
        call: dict[str, Any] = {
            "type": "function_call",
            "call_id": f"call{index}",
            "name": "lookup",
            "arguments": json.dumps({"query": "q" * chars}),
        }
        result: dict[str, Any] = {
            "type": "function_call",
            "call_id": f"call{index}",
            "name": "lookup",
            "arguments": json.dumps({"result": "r" * chars}),
        }
        state.data.conversation_history.extend([
            DurableAgentStateRequest(
                correlation_id=f"c{index}",
                created_at=occurred_at,
                messages=[
                    DurableAgentStateMessage.from_chat_message(
                        Message(role="user", contents=[call], message_id=f"u{index}")
                    )
                ],
            ),
            DurableAgentStateResponse(
                correlation_id=f"c{index}",
                created_at=occurred_at,
                messages=[
                    DurableAgentStateMessage.from_chat_message(
                        Message(role="assistant", contents=[result], message_id=f"a{index}")
                    )
                ],
            ),
        ])
    return state


class TestTheBudgetDoesNotAssumeProse:
    """A conversation of tool calls must be budgeted like any other.

    The budget converts bytes into tokens. Deriving that conversion from ``message.text`` made it
    depend on the *kind* of content rather than its size, and a function call has no text at all.
    A tool-only history therefore produced a budget of one token and evicted everything it was
    permitted to touch, rather than evicting down to the watermark like any other conversation.
    """

    async def test_a_tool_only_history_keeps_roughly_what_prose_keeps(self) -> None:
        prose = _state(turns=40)
        tools = _tool_state(turns=40)

        await enforce_budget(prose, max_state_bytes=BUDGET)
        await enforce_budget(tools, max_state_bytes=BUDGET)

        prose_left = len(_message_ids(prose))
        tools_left = len(_message_ids(tools))
        # Not identical, since the two shapes do not serialize to the same size per message, but
        # the same order of magnitude. Before the fix this was 8 against 1.
        assert tools_left > 1
        assert abs(prose_left - tools_left) <= max(2, prose_left // 2)

    async def test_a_tool_only_history_is_evicted_down_to_the_watermark(self) -> None:
        state = _tool_state(turns=40)

        removed = await enforce_budget(state, max_state_bytes=BUDGET)

        assert removed > 0
        assert _size(state) < BUDGET

    async def test_the_budget_scales_with_bytes_not_text(self) -> None:
        """Two histories of similar serialized size get similar budgets."""
        prose = _state(turns=10)
        tools = _tool_state(turns=10)

        def budget_for(state: DurableAgentState) -> int:
            origins = [(entry, m) for entry in state.data.conversation_history for m in entry.messages]
            size = _size(state)
            evictable = sum(len(json.dumps(m.to_dict())) for _, m in origins)
            return _token_budget(origins, serialized_size=size, evictable_bytes=evictable, target_bytes=size // 2)

        prose_budget = budget_for(prose)
        tools_budget = budget_for(tools)

        assert prose_budget > 1
        # The old formula gave exactly 1 here, whatever the tool payload weighed.
        assert tools_budget > 1
        assert 0.4 < (tools_budget / prose_budget) < 2.5


class TestTheAgentsInstructionsSurviveTheBudget:
    """A system message is never evicted, however tight the budget gets.

    Core protects system groups in its first fallback but then has a *strict* fallback whose whole
    job is to evict them when anchors alone exceed the budget. Relying on core's protection
    therefore holds only until the budget is small enough to matter. Keeping system messages out
    of the candidate set entirely makes them unevictable, and their bytes count as a floor.
    """

    def _with_system(self, turns: int, *, chars: int = 400) -> DurableAgentState:
        state = _state(turns=turns, chars=chars)
        anchor = DurableAgentStateRequest(
            correlation_id="system-anchor",
            created_at=datetime.now(tz=timezone.utc) - timedelta(minutes=turns + 5),
            messages=[
                DurableAgentStateMessage.from_chat_message(
                    Message(role="system", contents=["S" * chars], message_id="system-0")
                )
            ],
        )
        state.data.conversation_history.insert(0, anchor)
        return state

    def _system_count(self, state: DurableAgentState) -> int:
        return sum(1 for entry in state.data.conversation_history for m in entry.messages if m.role == "system")

    async def test_the_system_message_survives_a_comfortable_budget(self) -> None:
        state = self._with_system(turns=30)

        await enforce_budget(state, max_state_bytes=40_000)

        assert self._system_count(state) == 1

    async def test_the_system_message_survives_a_tight_budget(self) -> None:
        state = self._with_system(turns=30)

        removed = await enforce_budget(state, max_state_bytes=6_000)

        assert removed > 0
        assert self._system_count(state) == 1

    async def test_the_system_message_survives_a_budget_it_cannot_fit(self) -> None:
        """Even when retention cannot reach the target, the instructions stay."""
        state = self._with_system(turns=30)

        await enforce_budget(state, max_state_bytes=1_500)

        assert self._system_count(state) == 1

    async def test_ordinary_messages_are_still_evicted_around_it(self) -> None:
        state = self._with_system(turns=30)

        removed = await enforce_budget(state, max_state_bytes=6_000)

        surviving = _message_ids(state)
        assert removed > 0
        assert "system-0" in surviving


class TestEvictionLeavesEvidence:
    """A conversation that has lost content must say so in the state, not only in a log.

    Eviction is lossy and performed by the runtime rather than by the user. A warning is only
    evidence to whoever happened to be watching at the time, which is nobody by the point someone
    asks why an answer lost context.
    """

    async def test_nothing_is_recorded_when_nothing_is_evicted(self) -> None:
        state = _state(turns=2)

        await enforce_budget(state, max_state_bytes=BUDGET)

        assert state.data.truncation is None

    async def test_eviction_is_recorded(self) -> None:
        state = _state(turns=60)

        removed = await enforce_budget(state, max_state_bytes=BUDGET)

        assert removed > 0
        assert state.data.truncation is not None
        assert state.data.truncation["evictedMessageCount"] == removed
        assert state.data.truncation["firstEvictedAt"]
        assert state.data.truncation["lastEvictedAt"]

    async def test_the_count_accumulates_across_evictions(self) -> None:
        state = _state(turns=60)

        first = await enforce_budget(state, max_state_bytes=BUDGET)
        for index in range(60, 120):
            occurred_at = datetime.now(tz=timezone.utc) - timedelta(minutes=200 - index)
            state.data.conversation_history.append(
                DurableAgentStateRequest(
                    correlation_id=f"c{index}",
                    created_at=occurred_at,
                    messages=[
                        DurableAgentStateMessage.from_chat_message(
                            Message(role="user", contents=["u" * 400], message_id=f"u{index}")
                        )
                    ],
                )
            )
        second = await enforce_budget(state, max_state_bytes=BUDGET)

        assert second > 0
        assert state.data.truncation is not None
        assert state.data.truncation["evictedMessageCount"] == first + second

    async def test_the_record_survives_a_round_trip(self) -> None:
        state = _state(turns=60)

        await enforce_budget(state, max_state_bytes=BUDGET)
        restored = DurableAgentState.from_dict(json.loads(json.dumps(state.to_dict())))

        assert restored.data.truncation == state.data.truncation


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
