# Copyright (c) Microsoft. All rights reserved.

"""Tests for retention (ADR-0032, "Retention").

An explicit pressure budget evicts eligible transcript history independently of eager compaction
pruning. An exclusion made for token cost is not consent to delete the record, and an unreachable
protected floor reports capacity failure without deleting state.
"""

import json
from collections.abc import AsyncIterator
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, cast, get_args

import pytest
from agent_framework import (
    Agent,
    AgentResponse,
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
    DELIVERY_WINDOW_SECONDS,
    HIGH_WATERMARK,
    LOW_WATERMARK,
    RetentionMode,
    StateCapacityError,
    _token_budget,
    enforce_budget,
    prunes_excluded,
)

BUDGET = 40_000
"""Small enough to keep these tests fast, large enough to hold a realistic conversation."""


def _state(turns: int, *, chars: int = 400, excluded_before: int = 0, excluded_recent: int = 0) -> DurableAgentState:
    """Build legacy transcript-delivered state with the given number of user/assistant turns.

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
    # These manually appended responses use legacy history lookup. Version 2 fixtures must
    # record independent mailbox results instead of treating transcript entries as delivery.
    state = DurableAgentState(schema_version="1.2.0")
    now = datetime.now(tz=timezone.utc)
    # Space legacy turns a minute apart so their delivery windows have elapsed. Tests of live
    # delivery explicitly refresh timestamps rather than depending on the test's running time.
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
        modes = get_args(RetentionMode)
        assert set(modes) == {"keep_all", "follow_compaction"}
        for mode in modes:
            assert prunes_excluded(mode) is (mode == "follow_compaction")

    def test_auto_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="retention"):
            prunes_excluded(cast(Any, "auto"))

    def test_the_defaults_do_not_enable_deletion(self) -> None:
        from agent_framework_durabletask import DurableAIAgentWorker

        worker = DurableAIAgentWorker(cast(Any, object()))
        assert worker._retention == "keep_all"
        assert worker._max_state_bytes is None


class TestBudgetEnforcement:
    """Nothing happens until state is genuinely close to the limit."""

    async def test_below_the_watermark_nothing_is_touched(self) -> None:
        state = _state(turns=4)
        before = state.to_json()

        removed = await enforce_budget(state, max_state_bytes=BUDGET)

        assert removed == 0
        assert state.to_json() == before

    async def test_over_the_watermark_evicts_to_the_low_watermark(self) -> None:
        state = _state(turns=60)
        assert _size(state) > BUDGET * HIGH_WATERMARK, "the fixture must start over the trigger"

        removed = await enforce_budget(state, max_state_bytes=BUDGET)

        assert removed > 0
        assert _size(state) <= BUDGET * LOW_WATERMARK, "eviction did not reach the low watermark"

    async def test_the_newest_turn_survives(self) -> None:
        """Evicting the turn that just happened would defeat the point of running it."""
        state = _state(turns=60)

        await enforce_budget(state, max_state_bytes=BUDGET)

        assert _message_ids(state)[-2:] == ["u59", "a59"]

    async def test_eviction_is_hysteretic(self) -> None:
        """Evicting to just under the trigger would evict again on every following turn."""
        state = _state(turns=60)
        await enforce_budget(state, max_state_bytes=BUDGET)

        second = await enforce_budget(state, max_state_bytes=BUDGET)

        assert second == 0, "a second pass evicted again immediately, so there is no headroom"

    async def test_pressure_eviction_does_not_require_compaction_exclusions(self) -> None:
        """An explicit budget can evict old groups without opting into eager pruning."""
        state = _state(turns=60)

        assert await enforce_budget(state, max_state_bytes=BUDGET) > 0

    @pytest.mark.parametrize("turns", [0, 10])
    async def test_metadata_floor_fails_without_mutating_state(self, turns: int) -> None:
        state = _state(turns=turns)
        state.data.session = {"state": {"pending_approvals": ["p" * (BUDGET * 2)]}}
        state.data.ingested_positions = {"source": 7}
        before = state.to_json()

        with pytest.raises(StateCapacityError) as error:
            await enforce_budget(state, max_state_bytes=BUDGET)

        assert error.value.floor_bytes > BUDGET
        assert state.to_json() == before


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
        """An unretainable current exchange reports capacity failure without deleting state."""
        state = _state(turns=1, chars=BUDGET * 2)
        before = state.to_json()

        with pytest.raises(StateCapacityError) as error:
            await enforce_budget(state, max_state_bytes=BUDGET)

        assert error.value.floor_bytes == len(before)
        assert state.to_json() == before
        assert _message_ids(state) == ["u0", "a0"], "the turn that just ran was evicted"

    async def test_an_oversized_newest_turn_does_not_take_the_history_with_it(self) -> None:
        state = _state(turns=10)
        state.data.conversation_history[-1].messages = [
            DurableAgentStateMessage.from_chat_message(
                Message(role="assistant", contents=["a" * (BUDGET * 2)], message_id="a9")
            )
        ]
        before = state.to_json()

        with pytest.raises(StateCapacityError):
            await enforce_budget(state, max_state_bytes=BUDGET)

        assert state.to_json() == before, "capacity failure must preserve the entire original history"


class TestAResponseIsNotEvictedBeforeItsCallerReadsIt:
    """A legacy caller reads its response by correlation id from transcript entries.

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
        original = state.try_get_agent_response(correlation)
        assert original is not None
        original_payload = deepcopy(original.to_dict())

        removed = await enforce_budget(state, max_state_bytes=BUDGET)

        assert removed > 0, "nothing was evicted, so this proves nothing"
        retained = state.try_get_agent_response(correlation)
        assert retained is not None, "a recent response was evicted before its caller could read it"
        assert retained.to_dict() == original_payload

    async def test_an_old_response_is_still_evictable(self) -> None:
        """Protection has to expire, or a long conversation could never be trimmed at all."""
        state = _state(turns=60)
        assert state.try_get_agent_response("c0") is not None

        removed = await enforce_budget(state, max_state_bytes=BUDGET)

        assert removed > 0
        assert state.try_get_agent_response("c0") is None, "an ancient response was kept forever"

    async def test_capacity_failure_preserves_every_recent_response(self) -> None:
        """A full delivery window reports capacity failure instead of sacrificing responses."""
        state = _state(turns=60)
        # Every turn happened just now, which is what a busy session looks like.
        for entry in state.data.conversation_history:
            entry.created_at = datetime.now(tz=timezone.utc)
        before = state.to_json()

        with pytest.raises(StateCapacityError) as error:
            await enforce_budget(state, max_state_bytes=BUDGET)

        assert error.value.floor_bytes == len(before)
        assert state.to_json() == before

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


class TestMailboxDeliverySurvivesTranscriptEviction:
    async def test_original_result_is_retained_until_expiry_and_completion_outlives_it(self) -> None:
        state = DurableAgentState()
        state.data.conversation_history = _state(turns=60).data.conversation_history
        now = datetime.now(tz=timezone.utc)
        for entry in state.data.conversation_history[:2]:
            entry.created_at = now
        response = AgentResponse(
            messages=[Message("assistant", ["a" * 400], message_id="a0")],
            additional_properties={"delivery": {"original": True}},
        )
        state.record_response("c0", response, delivery_window_seconds=DELIVERY_WINDOW_SECONDS, now=now)
        mailbox = deepcopy(state.data.response_mailbox)
        completed = deepcopy(state.data.completed_correlations)
        assert _size(state) > BUDGET * HIGH_WATERMARK

        removed = await enforce_budget(state, max_state_bytes=BUDGET)

        assert removed > 0
        assert not {"u0", "a0"} & set(_message_ids(state)), "the recent transcript copy was not evicted"
        restored = DurableAgentState.from_json(state.to_json())
        expiry = now + timedelta(seconds=DELIVERY_WINDOW_SECONDS)
        restored.expire_responses(now=expiry - timedelta(microseconds=1))
        assert restored.data.response_mailbox == mailbox
        assert restored.data.completed_correlations == completed
        retained = restored.try_get_agent_response("c0")
        assert retained is not None
        assert retained.to_dict() == response.to_dict()

        restored.expire_responses(now=expiry)

        assert restored.data.response_mailbox == {}
        assert restored.data.completed_correlations == completed
        expired = DurableAgentState.from_json(restored.to_json()).try_get_agent_response("c0")
        assert expired is not None
        assert expired.additional_properties["durable_status"] == "already_completed"
        assert expired.additional_properties["correlation_id"] == "c0"
        assert expired.messages[0].contents[0].error_code == "response_expired"


def _tool_state(turns: int, *, chars: int = 400) -> DurableAgentState:
    """Build a history of tool calls, which carry real bytes but no ``message.text``.

    This is the shape that broke the budget. A function call serializes to as much storage as
    prose of the same length, but reading ``.text`` off it returns an empty string.
    """
    state = DurableAgentState(schema_version="1.2.0")
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
        assert tools_left > 2, "the budget retained only the protected newest exchange"
        assert abs(prose_left - tools_left) <= max(2, prose_left // 2)

    async def test_a_tool_only_history_is_evicted_down_to_the_watermark(self) -> None:
        state = _tool_state(turns=40)

        removed = await enforce_budget(state, max_state_bytes=BUDGET)

        assert removed > 0
        assert _size(state) <= BUDGET * LOW_WATERMARK

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
        """An unreachable protected floor leaves instructions and the entire history intact."""
        state = self._with_system(turns=30)
        before = state.to_json()

        with pytest.raises(StateCapacityError):
            await enforce_budget(state, max_state_bytes=1_500)

        assert state.to_json() == before
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

    async def test_bare_transcript_entries_emptied_by_eviction_are_removed(self) -> None:
        state = _state(turns=60)

        removed = await enforce_budget(state, max_state_bytes=BUDGET)

        assert removed > 0
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
        self._state_dict = json.loads(json.dumps(state))

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
        budget = entity_kwargs.get("max_state_bytes")

        replies: list[str] = []
        for turn in range(self.TURNS):
            correlation_id = f"corr-{turn}"
            result = await entity.run({"message": f"question {turn}", "correlationId": correlation_id})
            persisted = DurableAgentState.from_dict(provider._get_state_dict())
            polled = persisted.try_get_agent_response(correlation_id)
            assert polled is not None
            assert polled.to_dict() == result.to_dict()
            replies.append(polled.text)
            assert set(persisted.data.response_mailbox) == {correlation_id}
            assert set(persisted.data.completed_correlations) == {f"corr-{index}" for index in range(turn + 1)}
            if budget is not None:
                assert _size(persisted) < int(budget * HIGH_WATERMARK)

            if turn < self.TURNS - 1:
                # Simulate the next operation arriving after delivery expires, but only after
                # polling this result. Let the entity remove the payload on its next operation;
                # neither transcript timestamps nor completion receipts are changed here.
                persisted.data.response_mailbox[correlation_id]["expiresAt"] = (
                    datetime.now(tz=timezone.utc) - timedelta(seconds=1)
                ).isoformat()
                provider.replace_cached_state(persisted)
                provider.persist_state()
        return provider, replies

    @pytest.mark.parametrize("budget", [BUDGET, LIMIT])
    async def test_keep_all_with_a_budget_stays_bounded_across_many_turns(self, budget: int) -> None:
        provider, _ = await self._drive(retention="keep_all", max_state_bytes=budget)
        assert len(json.dumps(provider._get_state_dict())) <= budget

    async def test_follow_compaction_falls_back_to_pressure_eviction(self) -> None:
        """With nothing to prune, only the shared pressure fallback can bound this run."""
        provider, _ = await self._drive(retention="follow_compaction", max_state_bytes=self.LIMIT)
        state = DurableAgentState.from_dict(provider._get_state_dict())

        assert len(json.dumps(provider._get_state_dict())) <= self.LIMIT
        assert 2 <= len(_message_ids(state)) < self.TURNS * 2

    async def test_every_turn_still_gets_its_own_answer(self) -> None:
        """Eviction must not disturb the response the caller is waiting on."""
        _, replies = await self._drive(max_state_bytes=self.LIMIT)
        assert [r.split(" x")[0] for r in replies] == [f"answering:question {i}" for i in range(self.TURNS)]

    async def test_history_is_actually_trimmed_not_just_small(self) -> None:
        """Without this the bounded assertion above could pass for the wrong reason."""
        provider, _ = await self._drive(max_state_bytes=self.LIMIT)
        state = DurableAgentState.from_dict(provider._get_state_dict())
        # Metadata-only envelopes are protected state, not retained transcript messages.
        kept = len(_message_ids(state))
        assert 2 <= kept < self.TURNS * 2
        assert state.data.truncation is not None
        assert state.data.truncation["evictedMessageCount"] == self.TURNS * 2 - kept

    async def test_keep_all_without_a_budget_lets_it_grow_past_the_limit(self) -> None:
        """Proves the run is genuinely over budget, so the bounded case is a real result."""
        provider, _ = await self._drive(retention="keep_all", max_state_bytes=None)
        state = DurableAgentState.from_dict(provider._get_state_dict())
        assert len(json.dumps(provider._get_state_dict())) > self.LIMIT
        assert len(_message_ids(state)) == self.TURNS * 2
        assert state.data.truncation is None
