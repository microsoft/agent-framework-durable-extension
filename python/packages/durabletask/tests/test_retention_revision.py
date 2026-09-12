# Copyright (c) Microsoft. All rights reserved.

"""Pressure-retention regressions for the independent ADR-0032 controls."""

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, get_args
from unittest.mock import AsyncMock, Mock

import pytest
from agent_framework import CharacterEstimatorTokenizer, Content, Message, annotate_message_groups, included_token_count

from agent_framework_durabletask import _retention as retention
from agent_framework_durabletask._durable_agent_state import (
    DurableAgentState,
    DurableAgentStateCompaction,
    DurableAgentStateEntry,
    DurableAgentStateEntryJsonType,
    DurableAgentStateErrorResponse,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
    DurableAgentStateUnknownContent,
    DurableAgentStateUsage,
)

NOW = datetime(2026, 9, 8, 12, 0, 0, 123456, tzinfo=timezone.utc)
OLD = NOW - timedelta(hours=1)


@pytest.fixture(autouse=True)
def fixed_retention_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = Mock(wraps=datetime)
    clock.now.return_value = NOW
    monkeypatch.setattr(retention, "datetime", clock)


def _message(message_id: str | None, role: str = "user", text: str = "x" * 400) -> DurableAgentStateMessage:
    return DurableAgentStateMessage.from_chat_message(Message(role, [text], message_id=message_id))


def _state(turns: int = 40, *, chars: int = 400) -> DurableAgentState:
    state = DurableAgentState()
    for index in range(turns):
        state.data.conversation_history.extend([
            DurableAgentStateRequest(f"c{index}", OLD, [_message(f"u{index}", text="u" * chars)]),
            DurableAgentStateResponse(f"c{index}", OLD, [_message(f"a{index}", "assistant", "a" * chars)]),
        ])
    return state


def _ids(state: DurableAgentState) -> list[str | None]:
    return [message.message_id for entry in state.data.conversation_history for message in entry.messages]


def _project_plain(
    state: DurableAgentState, removed_ids: list[str | None], *, record: bool = True
) -> DurableAgentState:
    """Independent byte oracle for fixtures containing only bare transcript envelopes."""
    projected = deepcopy(state)
    removed = set(removed_ids)
    for entry in projected.data.conversation_history:
        entry.messages = [message for message in entry.messages if message.message_id not in removed]
    projected.data.conversation_history = [entry for entry in projected.data.conversation_history if entry.messages]
    if removed and record:
        previous = projected.data.truncation or {}
        projected.data.truncation = {
            **previous,
            "evictedMessageCount": previous.get("evictedMessageCount", 0) + len(removed_ids),
            "firstEvictedAt": previous.get("firstEvictedAt", NOW.isoformat()),
            "lastEvictedAt": NOW.isoformat(),
        }
    return projected


def _smallest_plain_prefix(state: DurableAgentState, target: int) -> tuple[int, DurableAgentState]:
    eligible = _ids(state)[:-2]
    for count in range(1, len(eligible) + 1):
        projected = _project_plain(state, eligible[:count])
        if retention._serialized_size(projected) <= target:
            return count, projected
    raise AssertionError("fixture has no reachable prefix at the requested target")


def _delivery(state: DurableAgentState, correlations: list[str], *, payload_chars: int = 20) -> None:
    # Use the real state data serializer, not a mock that could hide mailbox bytes from the floor.
    data: Any = state.data
    data.response_mailbox = {
        correlation: {
            "response": {"messages": [Message("assistant", ["r" * payload_chars]).to_dict()], "metadata": {"v": [1]}},
            "createdAt": NOW.isoformat(),
            "expiresAt": (NOW + timedelta(seconds=retention.DELIVERY_WINDOW_SECONDS)).isoformat(),
            "futureMailboxField": {"keep": True},
        }
        for correlation in correlations
    }
    data.completed_correlations = {
        correlation: {"completedAt": NOW.isoformat(), "futureReceiptField": [1, 3]} for correlation in correlations
    }
    assert state.to_dict()["data"]["responseMailbox"] == data.response_mailbox
    assert state.to_dict()["data"]["completedCorrelations"] == data.completed_correlations


class TestConfiguration:
    def test_public_aliases_and_non_deleting_defaults(self) -> None:
        assert get_args(retention.RetentionMode) == ("keep_all", "follow_compaction")
        budget_members = get_args(retention.StateBudget)
        assert int in budget_members and type(None) in budget_members
        assert any(get_args(member) == ("backend_limit",) for member in budget_members)
        assert retention.DEFAULT_RETENTION == "keep_all"
        assert retention.DEFAULT_MAX_STATE_BYTES is None
        assert retention.DTS_MAX_STATE_BYTES == 1_048_576
        assert retention.HIGH_WATERMARK == 0.85
        assert retention.LOW_WATERMARK == 0.70
        assert retention.DELIVERY_WINDOW_SECONDS == 60
        assert {
            "RetentionMode",
            "StateBudget",
            "StateCapacityError",
            "resolve_state_budget",
            "validate_retention",
        } <= set(retention.__all__)

    @pytest.mark.parametrize("mode", ["keep_all", "follow_compaction"])
    @pytest.mark.parametrize("budget", [None, 1, 24_000, "backend_limit"])
    def test_pruning_and_pressure_are_independent(self, mode: Any, budget: Any) -> None:
        retention.validate_retention(mode)
        assert retention.prunes_excluded(mode) is (mode == "follow_compaction")
        expected = retention.DTS_MAX_STATE_BYTES if budget == "backend_limit" else budget
        assert retention.resolve_state_budget(budget, backend_limit=retention.DTS_MAX_STATE_BYTES) == expected

    @pytest.mark.parametrize(
        "value",
        [
            True,
            False,
            0,
            -1,
            1.5,
            1.0,
            float("nan"),
            float("inf"),
            "auto",
            "1",
            "",
            [],
            {},
            (),
            b"backend_limit",
            object(),
        ],
    )
    def test_invalid_budgets_raise_value_error(self, value: Any) -> None:
        with pytest.raises(ValueError, match="max_state_bytes"):
            retention.resolve_state_budget(value)

    @pytest.mark.parametrize("limit", [None, True, False, 0, -1, 1.0, float("nan"), float("inf"), "1000", [], {}])
    def test_backend_limit_must_be_resolved_and_positive(self, limit: Any) -> None:
        with pytest.raises(ValueError, match="backend_limit"):
            retention.resolve_state_budget("backend_limit", backend_limit=limit)

    @pytest.mark.parametrize(
        "mode",
        ["auto", "", "KEEP_ALL", "follow-compaction", None, True, False, 1, 1.0, [], {}, (), b"keep_all", object()],
    )
    def test_invalid_modes_raise_value_error(self, mode: Any) -> None:
        with pytest.raises(ValueError, match="retention"):
            retention.validate_retention(mode)
        with pytest.raises(ValueError, match="retention"):
            retention.prunes_excluded(mode)

    @pytest.mark.parametrize("name", ["high_watermark", "low_watermark"])
    @pytest.mark.parametrize(
        "value",
        [None, True, False, 0, -0.1, 1.1, float("nan"), float("inf"), float("-inf"), "0.8", [], {}, 1j, 10**400],
    )
    def test_invalid_watermark_types_and_ranges(self, name: str, value: Any) -> None:
        kwargs = {"high_watermark": 0.85, "low_watermark": 0.70, name: value}
        with pytest.raises(ValueError, match="watermark"):
            retention.validate_retention("keep_all", **kwargs)

    @pytest.mark.parametrize(("high", "low"), [(0.7, 0.7), (0.6, 0.7), (1, 1)])
    def test_watermarks_must_be_strictly_ordered(self, high: float, low: float) -> None:
        with pytest.raises(ValueError, match="watermark"):
            retention.validate_retention("keep_all", high, low)

    def test_high_watermark_may_equal_one(self) -> None:
        retention.validate_retention("keep_all", 1, 0.5)

    @pytest.mark.parametrize("budget", [None, True, False, 0, -1, 1.0, "backend_limit", [], {}])
    async def test_enforcement_requires_a_resolved_integer(self, budget: Any) -> None:
        state = _state(1)
        before = state.to_json()
        with pytest.raises(ValueError, match="max_state_bytes"):
            await retention.enforce_budget(state, max_state_bytes=budget)
        assert state.to_json() == before

    async def test_enforcement_validates_watermarks_even_below_pressure(self) -> None:
        with pytest.raises(ValueError, match="watermark"):
            await retention.enforce_budget(_state(1), max_state_bytes=1_000_000, high_watermark=float("nan"))


class TestProtectedFloor:
    @pytest.mark.parametrize("empty_entries", [0, 30])
    async def test_metadata_only_floor_fails_without_any_mutation(
        self, monkeypatch: pytest.MonkeyPatch, empty_entries: int
    ) -> None:
        state = _state(0)
        state.data.session = {"approvals": "p" * 20_000}
        state.data.conversation_history = [DurableAgentStateRequest(f"c{i}", OLD, []) for i in range(empty_entries)]
        before = state.to_json()
        history = state.data.conversation_history
        strategy = Mock(side_effect=AssertionError("no eviction pass is permitted"))
        monkeypatch.setattr(retention, "TokenBudgetComposedStrategy", strategy)
        with pytest.raises(retention.StateCapacityError) as error:
            await retention.enforce_budget(state, max_state_bytes=12_000)
        assert error.value.size_bytes == error.value.floor_bytes == len(before)
        assert error.value.max_state_bytes == 12_000
        assert "floor" in str(error.value) and "budget" in str(error.value)
        assert state.to_json() == before
        assert state.data.conversation_history is history
        strategy.assert_not_called()

    async def test_floor_between_high_and_hard_limit_prevents_futile_deletion(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        state = _state()
        state.data.session = {"protected": "p" * 9_000}
        floor = retention._serialized_size(_project_plain(state, _ids(state)[:-2]))
        assert 12_000 * 0.8 <= floor < 12_000
        before = state.to_json()
        strategy = Mock(side_effect=AssertionError("floor is unreachable"))
        monkeypatch.setattr(retention, "TokenBudgetComposedStrategy", strategy)
        with pytest.raises(retention.StateCapacityError) as error:
            await retention.enforce_budget(state, max_state_bytes=12_000, high_watermark=0.8, low_watermark=0.6)
        assert error.value.floor_bytes == floor
        assert state.to_json() == before
        strategy.assert_not_called()

    async def test_truncation_cost_is_in_the_floor_before_any_eviction(self, monkeypatch: pytest.MonkeyPatch) -> None:
        state = _state(12)
        old_ids = _ids(state)[:-2]
        without_record = retention._serialized_size(_project_plain(state, old_ids, record=False))
        floor = retention._serialized_size(_project_plain(state, old_ids))
        budget = (without_record + floor) // 2
        assert without_record < budget < floor
        before = state.to_json()
        strategy = Mock(side_effect=AssertionError("truncation cannot fit"))
        monkeypatch.setattr(retention, "TokenBudgetComposedStrategy", strategy)
        with pytest.raises(retention.StateCapacityError) as error:
            await retention.enforce_budget(state, max_state_bytes=budget, high_watermark=1, low_watermark=0.5)
        assert error.value.floor_bytes == floor
        assert state.to_json() == before
        strategy.assert_not_called()

    async def test_unreachable_low_uses_the_reachable_floor_below_high(self) -> None:
        state = _state(25)
        state.data.session = {"protected": "p" * 9_000}
        expected = _project_plain(state, _ids(state)[:-2])
        assert 13_000 * 0.5 < retention._serialized_size(expected) < 13_000 * 0.9
        removed = await retention.enforce_budget(state, max_state_bytes=13_000, high_watermark=0.9, low_watermark=0.5)
        assert removed == 48
        assert state.to_dict() == expected.to_dict()

    async def test_all_recent_legacy_results_are_absolute_protections(self, monkeypatch: pytest.MonkeyPatch) -> None:
        state = _state()
        for entry in state.data.conversation_history:
            entry.created_at = NOW
        before = state.to_json()
        strategy = Mock(side_effect=AssertionError("recent results cannot be sacrificed"))
        monkeypatch.setattr(retention, "TokenBudgetComposedStrategy", strategy)
        with pytest.raises(retention.StateCapacityError):
            await retention.enforce_budget(state, max_state_bytes=12_000)
        assert state.to_json() == before
        strategy.assert_not_called()

    async def test_unknown_entry_payloads_contribute_to_the_floor(self, monkeypatch: pytest.MonkeyPatch) -> None:
        state = _state()
        unknown_kind: Any = "futureKind"
        state.data.conversation_history.insert(
            0, DurableAgentStateEntry(unknown_kind, "opaque", OLD, [_message("opaque", text="p" * 30_000)])
        )
        before = state.to_json()
        strategy = Mock(side_effect=AssertionError("unknown state cannot be evicted"))
        monkeypatch.setattr(retention, "TokenBudgetComposedStrategy", strategy)
        with pytest.raises(retention.StateCapacityError) as error:
            await retention.enforce_budget(state, max_state_bytes=12_000)
        assert error.value.floor_bytes > 30_000
        assert state.to_json() == before
        strategy.assert_not_called()

    async def test_mailbox_receipts_and_control_fields_survive_transcript_eviction(self) -> None:
        state = _state()
        _delivery(state, ["c0", "c1"])
        state.data.session = {"service_session_id": "branch", "state": {"approvals": ["keep"]}}
        state.data.ingested_positions = {"source": 99}
        state.data.extension_data = {"customIds": ["opaque-id"], "futureControl": {"keep": [1, 3]}}
        state.data.conversation_history[1].created_at = NOW
        before = deepcopy(state.to_dict()["data"])
        removed = await retention.enforce_budget(state, max_state_bytes=16_000)
        assert removed > 0 and "a0" not in _ids(state)
        after = state.to_dict()["data"]
        for field in ("responseMailbox", "completedCorrelations", "session", "ingestedPositions", "extensionData"):
            assert after[field] == before[field]

    @pytest.mark.parametrize("has_mailbox", [False, True])
    async def test_only_independently_completed_recent_results_are_evictable(self, has_mailbox: bool) -> None:
        state = _state()
        _delivery(state, ["c0"])
        data: Any = state.data
        if not has_mailbox:
            data.response_mailbox.clear()
        for entry in state.data.conversation_history[:4]:
            entry.created_at = NOW
        before = deepcopy(data.completed_correlations)
        assert await retention.enforce_budget(state, max_state_bytes=12_000) > 0
        assert "a0" not in _ids(state)
        assert {"u1", "a1"} <= set(_ids(state))
        assert data.completed_correlations == before

    @pytest.mark.parametrize("field", ["response_mailbox", "completed_correlations"])
    async def test_delivery_records_alone_can_fill_the_floor(self, field: str) -> None:
        state = _state()
        _delivery(state, ["c0"], payload_chars=30_000 if field == "response_mailbox" else 1)
        if field == "completed_correlations":
            data: Any = state.data
            data.completed_correlations["c0"]["futureReceiptField"] = "p" * 30_000
        before = state.to_json()
        with pytest.raises(retention.StateCapacityError) as error:
            await retention.enforce_budget(state, max_state_bytes=12_000)
        assert error.value.floor_bytes > 30_000
        assert state.to_json() == before

    async def test_entry_schema_and_usage_metadata_are_not_transcript_capacity(self) -> None:
        state = _state()
        request = state.data.conversation_history[0]
        assert isinstance(request, DurableAgentStateRequest)
        request.response_schema = {"largeControl": "s" * 2_000}
        request.orchestration_id = "workflow"
        response = state.data.conversation_history[1]
        assert isinstance(response, DurableAgentStateResponse)
        response.usage = DurableAgentStateUsage(input_token_count=10, extensionData={"opaque": "keep"})
        before = [deepcopy(entry.to_dict()) for entry in (request, response)]
        assert await retention.enforce_budget(state, max_state_bytes=10_000) > 0
        retained = [entry for entry in state.data.conversation_history if entry.correlation_id == "c0"]
        assert len(retained) == 2
        for entry, original in zip(retained, before):
            assert entry.messages == []
            original["messages"] = []
            assert entry.to_dict() == original


class TestSelectionAndMeasurements:
    @pytest.mark.parametrize("kind", list(DurableAgentStateEntryJsonType))
    async def test_each_known_transcript_kind_is_storage_eligible(self, kind: DurableAgentStateEntryJsonType) -> None:
        state = _state(0)
        for index in range(30):
            state.data.conversation_history.append(
                DurableAgentStateEntry(kind, f"old-{index}", OLD, [_message(f"old-{index}")])
            )
        state.data.conversation_history.extend(_state(1).data.conversation_history)
        assert await retention.enforce_budget(state, max_state_bytes=8_000) > 0
        assert "old-0" not in _ids(state)
        assert _ids(state)[-2:] == ["u0", "a0"]

    async def test_expired_runtime_error_content_is_evictable(self) -> None:
        state = _state(0)
        for index in range(35):
            error = Message("assistant", [Content.from_error(message="e" * 500)], message_id=f"error-{index}")
            state.data.conversation_history.append(
                DurableAgentStateErrorResponse(
                    f"failed-{index}", OLD, [DurableAgentStateMessage.from_chat_message(error)]
                )
            )
        assert await retention.enforce_budget(state, max_state_bytes=10_000) > 0
        assert "error-0" not in _ids(state)
        assert "error-34" in _ids(state)
        assert retention._serialized_size(state) <= 7_000

    @pytest.mark.parametrize("recent", [False, True])
    async def test_error_delivery_protection_applies_only_inside_legacy_window(self, recent: bool) -> None:
        state = _state()
        occurred_at = NOW - timedelta(seconds=30 if recent else retention.DELIVERY_WINDOW_SECONDS)
        failure = DurableAgentStateErrorResponse("failed", occurred_at.replace(tzinfo=None), [_message("failure")])
        state.data.conversation_history.insert(0, failure)
        assert await retention.enforce_budget(state, max_state_bytes=12_000) > 0
        assert ("failure" in _ids(state)) is recent

    async def test_custom_high_watermark_controls_trigger_and_low_controls_target(self) -> None:
        state = _state(16)
        before = state.to_json()
        budget = len(before) * 2
        assert await retention.enforce_budget(state, max_state_bytes=budget, high_watermark=0.75) == 0
        assert state.to_json() == before
        count, expected = _smallest_plain_prefix(state, int(budget * 0.25))
        removed = await retention.enforce_budget(state, max_state_bytes=budget, high_watermark=0.4, low_watermark=0.25)
        assert removed == count
        assert state.to_dict() == expected.to_dict()

    @pytest.mark.parametrize("previous_count", [0, 9, 99, 999])
    async def test_low_target_includes_truncation_and_does_not_over_evict(self, previous_count: int) -> None:
        state = _state()
        if previous_count:
            state.data.truncation = {
                "evictedMessageCount": previous_count,
                "firstEvictedAt": OLD.isoformat(),
                "lastEvictedAt": OLD.isoformat(),
                "futureEvidence": {"keep": [1, 3]},
            }
        count, expected = _smallest_plain_prefix(state, 18_000)
        removed = await retention.enforce_budget(state, max_state_bytes=20_000, high_watermark=0.95, low_watermark=0.9)
        assert removed == count
        assert state.to_dict() == expected.to_dict()
        assert (
            await retention.enforce_budget(state, max_state_bytes=20_000, high_watermark=0.95, low_watermark=0.9) == 0
        )

    async def test_actual_bytes_correct_an_optimistic_plan_without_halving_the_target(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        state = _state()
        count, expected = _smallest_plain_prefix(state, 8_400)
        prefix_sizes = retention._prefix_sizes

        def optimistic_sizes(*args: Any, **kwargs: Any) -> list[int]:
            return [size - 1_500 for size in prefix_sizes(*args, **kwargs)]

        factory = Mock(wraps=retention.TokenBudgetComposedStrategy)
        monkeypatch.setattr(retention, "_prefix_sizes", optimistic_sizes)
        monkeypatch.setattr(retention, "TokenBudgetComposedStrategy", factory)
        assert await retention.enforce_budget(state, max_state_bytes=12_000) == count
        assert state.to_dict() == expected.to_dict()
        assert 2 <= factory.call_count <= 3

    async def test_mixed_unicode_and_large_tool_payloads_choose_the_smallest_atomic_prefix(self) -> None:
        state = _state(0)
        for index in range(30):
            body = "\u754c\U0001f680" * 200 if index % 2 else "plain" * 100
            call = Content.from_function_call(call_id=f"call-{index}", name="lookup", arguments=json.dumps({"q": body}))
            result = Content.from_function_result(call_id=f"call-{index}", result={"records": [body]})
            state.data.conversation_history.append(
                DurableAgentStateResponse(
                    f"tools-{index}",
                    OLD,
                    [
                        DurableAgentStateMessage.from_chat_message(
                            Message("assistant", [call], message_id=f"call-{index}")
                        ),
                        DurableAgentStateMessage.from_chat_message(
                            Message("tool", [result], message_id=f"result-{index}")
                        ),
                    ],
                )
            )
        state.data.conversation_history.extend(_state(1).data.conversation_history)
        candidates = _ids(state)[:-2]
        expected_count = 0
        for count in range(2, len(candidates) + 1, 2):
            projected = _project_plain(state, candidates[:count])
            if retention._serialized_size(projected) <= 28_000:
                expected_count = count
                break
        assert 0 < expected_count < len(candidates)
        expected = _project_plain(state, candidates[:expected_count])
        assert await retention.enforce_budget(state, max_state_bytes=40_000) == expected_count
        assert state.to_dict() == expected.to_dict()
        assert retention._serialized_size(state) == len(state.to_json().encode("utf-8"))

    def test_token_budget_uses_core_tokens_not_escaped_json_or_text_length(self) -> None:
        message = Message(
            "assistant",
            [Content.from_function_call(call_id="call", name="lookup", arguments=json.dumps({"q": "\u754c" * 100}))],
            message_id="tool",
        )
        state = _state(0)
        entry = DurableAgentStateResponse("tools", OLD, [DurableAgentStateMessage.from_chat_message(message)])
        entry.messages.append(_message("unicode", text="\U0001f680\u754c" * 100))
        state.data.conversation_history.append(entry)
        origins: list[tuple[DurableAgentStateEntry, DurableAgentStateMessage]] = [
            (entry, stored) for stored in entry.messages
        ]
        messages = [deepcopy(stored).to_chat_message() for stored in entry.messages]
        annotate_message_groups(messages, tokenizer=CharacterEstimatorTokenizer())
        tokens = included_token_count(messages)
        persisted_bytes = sum(len(json.dumps(stored.to_dict())) for stored in entry.messages)
        assert retention._token_budget(
            origins,
            serialized_size=persisted_bytes + 1_000,
            evictable_bytes=persisted_bytes,
            target_bytes=1_000 + persisted_bytes // 2,
        ) == max((persisted_bytes // 2) * tokens // persisted_bytes, 1)

    async def test_summaries_do_not_replace_the_newest_exchange(self) -> None:
        state = _state()
        newest = state.data.conversation_history[-2:]
        state.data.conversation_history.append(DurableAgentStateCompaction(NOW, [_message("summary")], "summary-cid"))
        assert retention._newest_exchange(state.data.conversation_history) == newest
        assert await retention.enforce_budget(state, max_state_bytes=12_000) > 0
        assert {"u39", "a39"} <= set(_ids(state))

    async def test_unknown_entries_and_already_empty_envelopes_remain_opaque(self) -> None:
        state = _state()
        future_kind: Any = "futureKind"
        opaque = DurableAgentStateEntry(
            future_kind,
            "unknown",
            OLD,
            [DurableAgentStateMessage("assistant", [DurableAgentStateUnknownContent({})], message_id="opaque")],
        )
        empty = DurableAgentStateRequest("metadata-only", OLD, [], response_schema={"keep": True})
        state.data.conversation_history[:0] = [opaque, empty]
        before = [deepcopy(entry.to_dict()) for entry in (opaque, empty)]
        assert await retention.enforce_budget(state, max_state_bytes=12_000) > 0
        assert [entry.to_dict() for entry in state.data.conversation_history[:2]] == before


class TestAtomicityAndIsolation:
    @pytest.mark.parametrize("non_contiguous", [False, True])
    async def test_reasoning_call_and_result_are_one_oldest_group(self, non_contiguous: bool) -> None:
        state = _state(20)
        group = [
            Message("assistant", [Content.from_text_reasoning(text="reason" * 100)], message_id="reason"),
            Message(
                "assistant", [Content.from_function_call(call_id="t", name="tool", arguments="{}")], message_id="call"
            ),
            Message("tool", [Content.from_function_result(call_id="t", result="r" * 500)], message_id="result"),
        ]
        if non_contiguous:
            group.insert(2, Message("user", ["gap"], message_id="gap"))
        state.data.conversation_history.insert(
            0, DurableAgentStateResponse("tools", OLD, [DurableAgentStateMessage.from_chat_message(m) for m in group])
        )
        budget = retention._serialized_size(state) - 1
        assert await retention.enforce_budget(state, max_state_bytes=budget, high_watermark=1, low_watermark=0.99) == 3
        assert not {"reason", "call", "result"} & set(_ids(state))
        assert ("gap" in _ids(state)) is non_contiguous

    async def test_system_intersection_protects_the_entire_persisted_group(self) -> None:
        state = _state()
        messages = [_message("policy", "system"), _message("linked", "assistant")]
        for message in messages:
            message.extension_data = {"_group": {"id": "atomic-policy"}}
        state.data.conversation_history.insert(0, DurableAgentStateRequest("policy", OLD, messages))
        before = [deepcopy(message.to_dict()) for message in messages]
        assert await retention.enforce_budget(state, max_state_bytes=12_000) > 0
        assert [message.to_dict() for message in state.data.conversation_history[0].messages] == before

    async def test_current_exchange_protects_its_non_contiguous_tool_declaration(self) -> None:
        state = _state()
        call = Message(
            "assistant", [Content.from_function_call(call_id="t", name="tool", arguments="{}")], message_id="call"
        )
        state.data.conversation_history.insert(
            0, DurableAgentStateResponse("earlier", OLD, [DurableAgentStateMessage.from_chat_message(call)])
        )
        result = Message("tool", [Content.from_function_result(call_id="t", result="result")], message_id="result")
        state.data.conversation_history[-1].messages.append(DurableAgentStateMessage.from_chat_message(result))
        assert await retention.enforce_budget(state, max_state_bytes=12_000) > 0
        assert {"call", "result", "u39", "a39"} <= set(_ids(state))

    async def test_nested_annotations_and_payloads_never_alias_planning_copies(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        state = _state()
        original_messages = [message for entry in state.data.conversation_history for message in entry.messages]
        for index, message in enumerate(original_messages):
            message.extension_data = {
                "_excluded": True,
                "_exclude_reason": "user_compaction",
                "_group": {"id": f"saved-{index}", "token_count": 999_999, "future": {"values": [1, 2]}},
                "future": {"values": [1, 3]},
            }
        original = [deepcopy(message.to_dict()) for message in original_messages]
        converter = DurableAgentStateMessage.to_chat_message
        strategy_class = retention.TokenBudgetComposedStrategy

        def aliasing_converter(stored: DurableAgentStateMessage) -> Message:
            converted: Message = converter(stored)
            if stored.extension_data is not None:
                converted.additional_properties = stored.extension_data
            return converted

        def mutating_strategy_factory(**kwargs: Any) -> Any:
            strategy = strategy_class(**kwargs)

            async def mutate_and_evict(messages: list[Message]) -> bool:
                for message in messages[:-1]:
                    message.additional_properties["future"]["values"].append("planning-only")
                return await strategy(messages)

            return mutate_and_evict

        monkeypatch.setattr(DurableAgentStateMessage, "to_chat_message", aliasing_converter)
        monkeypatch.setattr(retention, "TokenBudgetComposedStrategy", mutating_strategy_factory)
        assert await retention.enforce_budget(state, max_state_bytes=24_000) > 0
        assert [message.to_dict() for message in original_messages] == original
        by_id = {message["messageId"]: message for message in original}
        survivors = [message for entry in state.data.conversation_history for message in entry.messages]
        assert len(survivors) > 2
        assert all(message.to_dict() == by_id[message.message_id] for message in survivors)

    @pytest.mark.parametrize("ids", [[None, None], ["duplicate", "duplicate"]])
    async def test_missing_or_duplicate_message_ids_do_not_alias_eviction_origins(self, ids: list[str | None]) -> None:
        state = _state()
        for index, entry in enumerate(state.data.conversation_history):
            entry.messages[0].message_id = ids[index % 2]
        before = len(_ids(state))
        removed = await retention.enforce_budget(state, max_state_bytes=12_000)
        assert 0 < removed < before - 2
        assert len(_ids(state)) == before - removed
        assert _ids(state)[-2:] == ids

    async def test_strategy_is_deterministic_and_has_no_user_strategies(self, monkeypatch: pytest.MonkeyPatch) -> None:
        strategy = Mock(wraps=retention.TokenBudgetComposedStrategy)
        monkeypatch.setattr(retention, "TokenBudgetComposedStrategy", strategy)
        first = _state()
        second = deepcopy(first)
        assert await retention.enforce_budget(first, max_state_bytes=12_000) > 0
        assert await retention.enforce_budget(second, max_state_bytes=12_000) > 0
        assert first.to_dict() == second.to_dict()
        assert 2 <= strategy.call_count <= 6
        for call in strategy.call_args_list:
            assert call.kwargs["strategies"] == []
            assert isinstance(call.kwargs["tokenizer"], CharacterEstimatorTokenizer)

    async def test_unsatisfied_strategy_stops_after_three_passes_and_rolls_back(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        state = _state()
        before = state.to_json()
        strategy = AsyncMock(return_value=False)
        factory = Mock(return_value=strategy)
        monkeypatch.setattr(retention, "TokenBudgetComposedStrategy", factory)
        with pytest.raises(retention.StateCapacityError):
            await retention.enforce_budget(state, max_state_bytes=12_000)
        assert strategy.await_count == 3
        assert state.to_json() == before

    async def test_strategy_failure_cannot_leak_annotation_changes(self, monkeypatch: pytest.MonkeyPatch) -> None:
        state = _state()
        before = state.to_json()

        async def failing_strategy(messages: list[Message]) -> bool:
            messages[0].additional_properties["poison"] = {"mutated": True}
            messages[0].contents.clear()
            raise RuntimeError("injected strategy failure")

        monkeypatch.setattr(retention, "TokenBudgetComposedStrategy", Mock(return_value=failing_strategy))
        with pytest.raises(RuntimeError, match="injected strategy failure"):
            await retention.enforce_budget(state, max_state_bytes=12_000)
        assert state.to_json() == before
