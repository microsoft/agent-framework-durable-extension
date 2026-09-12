# Copyright (c) Microsoft. All rights reserved.

"""Real SDK measurements of staged retention, isolated from the global meter provider."""

import asyncio
import json
from collections.abc import Iterator
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from agent_framework import Agent, Message
from opentelemetry.metrics import NoOpMeterProvider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import Histogram, InMemoryMetricReader, Metric, Sum
from test_durable_history_provider import RecordingChatClient

from agent_framework_durabletask import (
    AgentEntity,
    AgentEntityStateProviderMixin,
    DurableAgentState,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
    DurableHistoryProvider,
    _history_provider,
)
from agent_framework_durabletask import _retention as retention
from agent_framework_durabletask import _retention_telemetry as telemetry
from agent_framework_durabletask._history_provider import (
    DurableHistoryBinding,
    bind_durable_history,
    unbind_durable_history,
)

NOW = datetime(2026, 9, 11, 12, 0, 0, 123456, tzinfo=timezone.utc)
BUDGET = 12_000
PREFIX = "durable.retention."


@pytest.fixture
def reader(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemoryMetricReader]:
    metric_reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[metric_reader], shutdown_on_exit=False)
    monkeypatch.setattr(telemetry, "get_meter", provider.get_meter)
    telemetry._instruments.cache_clear()
    clock = Mock(wraps=datetime)
    clock.now.return_value = NOW
    monkeypatch.setattr(retention, "datetime", clock)
    try:
        yield metric_reader
    finally:
        telemetry._instruments.cache_clear()
        provider.shutdown()


def _metrics(reader: InMemoryMetricReader) -> dict[str, Metric]:
    data = reader.get_metrics_data()
    if data is None:
        return {}
    result = {}
    for resource in data.resource_metrics:
        for scope in resource.scope_metrics:
            assert scope.scope.name == "agent_framework.durabletask"
            for metric in scope.metrics:
                assert metric.name.startswith(PREFIX)
                result[metric.name.removeprefix(PREFIX)] = metric
    return result


def _counter(metric: Metric, attributes: dict[str, Any], value: int) -> None:
    assert isinstance(metric.data, Sum)
    assert metric.data.is_monotonic
    matches = [point for point in metric.data.data_points if point.attributes == attributes]
    assert len(matches) == 1
    assert matches[0].value == value


def _histogram(metric: Metric, attributes: dict[str, Any], total: int, count: int = 1) -> None:
    assert metric.unit == "By"
    assert isinstance(metric.data, Histogram)
    matches = [point for point in metric.data.data_points if point.attributes == attributes]
    assert len(matches) == 1
    assert matches[0].count == count
    assert matches[0].sum == total


def _attributes(mechanism: str = "pressure", outcome: str = "staged") -> dict[str, Any]:
    return {"mechanism": mechanism, "outcome": outcome, "commit_status": "not_attempted"}


def _state(turns: int = 40) -> DurableAgentState:
    state = DurableAgentState()
    for index in range(turns):
        for role, kind in (("user", DurableAgentStateRequest), ("assistant", DurableAgentStateResponse)):
            state.data.conversation_history.append(
                kind(
                    correlation_id=f"private-correlation-{index}",
                    created_at=NOW - timedelta(days=1),
                    messages=[
                        DurableAgentStateMessage.from_chat_message(
                            Message(role, ["private payload " * 30], message_id=f"private-{role}-{index}")
                        )
                    ],
                )
            )
    return state


def _size(state: DurableAgentState) -> int:
    return len(json.dumps(state.to_dict()))


def _counts(state: DurableAgentState) -> tuple[int, int]:
    history = state.data.conversation_history
    return sum(len(entry.messages) for entry in history), len(history)


class _Storage(AgentEntityStateProviderMixin):
    def __init__(self, state: DurableAgentState, failure: BaseException | None = None) -> None:
        self.raw = state.to_dict()
        self.failure = failure
        self.attempts: list[dict[str, Any]] = []

    def _get_state_dict(self) -> dict[str, Any]:
        return deepcopy(self.raw)

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        self.attempts.append(deepcopy(state))
        if self.failure is not None:
            raise self.failure
        self.raw = json.loads(json.dumps(state))

    def _get_session_id_from_entity(self) -> str:
        return "private-session"


def _entity(
    storage: _Storage, *, budget: int | None = BUDGET, history: DurableHistoryProvider | None = None
) -> AgentEntity:
    client: Any = RecordingChatClient()
    return AgentEntity(
        Agent(client=client, context_providers=[history] if history is not None else None),
        state_provider=storage,
        max_state_bytes=budget,
    )


async def test_under_budget_records_one_unchanged_size_pair(reader: InMemoryMetricReader) -> None:
    state = _state(1)
    before = state.to_dict()
    assert await retention.enforce_budget(state, max_state_bytes=BUDGET) == 0
    assert state.to_dict() == before
    metrics = _metrics(reader)
    assert set(metrics) == {"evaluations", "budget", "state.size"}
    attrs = _attributes(outcome="below_threshold")
    _counter(metrics["evaluations"], attrs, 1)
    _histogram(metrics["budget"], attrs, BUDGET)
    _histogram(metrics["state.size"], {**attrs, "phase": "before"}, _size(state))
    _histogram(metrics["state.size"], {**attrs, "phase": "after"}, _size(state))


async def test_pressure_reports_only_applied_plan_and_exact_bytes(reader: InMemoryMetricReader) -> None:
    state = _state()
    before_bytes = _size(state)
    before_messages, before_entries = _counts(state)
    removed = await retention.enforce_budget(state, max_state_bytes=BUDGET)
    assert removed > 0
    after_messages, after_entries = _counts(state)
    assert removed == before_messages - after_messages
    assert state.data.truncation is not None
    assert state.data.truncation["evictedMessageCount"] == removed
    metrics = _metrics(reader)
    assert set(metrics) == {
        "evaluations",
        "budget",
        "state.size",
        "removed_messages",
        "removed_entries",
        "reclaimed_bytes",
    }
    attrs = _attributes()
    _counter(metrics["evaluations"], attrs, 1)
    _counter(metrics["removed_messages"], attrs, removed)
    _counter(metrics["removed_entries"], attrs, before_entries - after_entries)
    _counter(metrics["reclaimed_bytes"], attrs, before_bytes - _size(state))
    _histogram(metrics["budget"], attrs, BUDGET)
    _histogram(metrics["state.size"], {**attrs, "phase": "before"}, before_bytes)
    _histogram(metrics["state.size"], {**attrs, "phase": "after"}, _size(state))


@pytest.mark.parametrize("floor", [True, False])
async def test_capacity_failure_never_reports_detached_deletion(
    reader: InMemoryMetricReader, monkeypatch: pytest.MonkeyPatch, floor: bool
) -> None:
    state = _state()
    if floor:
        state.data.session = {"private_control": "p" * BUDGET}
        strategy = Mock(side_effect=AssertionError("floor must be checked before planning"))
    else:

        async def insufficient_plan(messages: list[Message]) -> bool:
            messages[0].additional_properties["_excluded"] = True
            return True

        # A detached plan deletes something but cannot reach the byte target.
        strategy = Mock(return_value=AsyncMock(side_effect=insufficient_plan))
    monkeypatch.setattr(retention, "TokenBudgetComposedStrategy", strategy)
    before = state.to_dict()
    with pytest.raises(retention.StateCapacityError):
        await retention.enforce_budget(state, max_state_bytes=BUDGET)
    assert state.to_dict() == before
    assert strategy.call_count == (0 if floor else 3)
    metrics = _metrics(reader)
    assert set(metrics) == {"evaluations", "budget", "state.size", "capacity_failures"}
    attrs = _attributes(outcome="protected_floor" if floor else "unreachable_target")
    _counter(metrics["evaluations"], attrs, 1)
    _counter(metrics["capacity_failures"], attrs, 1)
    _histogram(metrics["budget"], attrs, BUDGET)
    for phase in ("before", "after"):
        _histogram(metrics["state.size"], {**attrs, "phase": phase}, _size(state))


@pytest.mark.parametrize("failure", [None, OSError("private write failure"), asyncio.CancelledError()])
async def test_entity_write_outcomes_remain_unconfirmed_and_failure_rolls_back(
    reader: InMemoryMetricReader, failure: BaseException | None
) -> None:
    storage = _Storage(_state(), failure)
    entity = _entity(storage)
    original = entity.state
    before = deepcopy(storage.raw)
    if failure is None:
        await entity.run({"message": "new", "correlationId": "private-current"})
        assert storage.raw != before
    else:
        with pytest.raises(type(failure)) as caught:
            await entity.run({"message": "new", "correlationId": "private-current"})
        assert caught.value is failure
        assert entity.state is original
        assert entity.state.to_dict() == before
        assert storage.raw == before
    assert len(storage.attempts) == 1
    attempted = DurableAgentState.from_dict(storage.attempts[0])
    assert attempted.data.truncation is not None
    removed = attempted.data.truncation["evictedMessageCount"]
    assert removed > 0
    metrics = _metrics(reader)
    _counter(metrics["removed_messages"], _attributes(), removed)
    attrs = {
        "outcome": "returned" if failure is None else "failed",
        "commit_status": "unknown",
        "deletion_staged": True,
    }
    _counter(metrics["operations"], attrs, 1)
    _counter(metrics["write_attempts"], {**attrs, "stage": "set_state"}, 1)
    assert telemetry._current(attempted) is None


async def test_floor_failure_has_no_host_write_attempt(reader: InMemoryMetricReader) -> None:
    state = _state()
    state.data.session = {"session_id": "private-session", "state": {"private_control": "p" * BUDGET}}
    storage = _Storage(state)
    entity = _entity(storage)
    original = entity.state
    with pytest.raises(retention.StateCapacityError):
        await entity.run({"message": "new", "correlationId": "private-current"})
    assert entity.state is original
    assert storage.attempts == []
    metrics = _metrics(reader)
    assert "write_attempts" not in metrics
    assert "removed_messages" not in metrics
    _counter(
        metrics["operations"],
        {"outcome": "failed", "commit_status": "not_attempted", "deletion_staged": False},
        1,
    )


async def test_eager_only_flush_measures_after_truncation_and_never_claims_confirmation(
    reader: InMemoryMetricReader,
) -> None:
    state = _state(5)
    for entry in state.data.conversation_history[:2]:
        entry.messages[0].extension_data = {"_excluded": True}
    storage = _Storage(state)
    history = DurableHistoryProvider(prune_excluded=True)
    # Real entity -> provider flush -> set_state path, with pressure budgeting disabled.
    await _entity(storage, budget=None, history=history).run({"message": "new", "correlationId": "private-current"})
    metrics = _metrics(reader)
    assert "budget" not in metrics
    assert "capacity_failures" not in metrics
    attrs = _attributes("eager")
    _counter(metrics["evaluations"], attrs, 1)
    _counter(metrics["removed_messages"], attrs, 2)
    _counter(metrics["removed_entries"], attrs, 2)
    # This flush precedes the new append/mailbox, so derive its independent boundary oracle.
    after = deepcopy(state)
    after.data.conversation_history = after.data.conversation_history[2:]
    after.data.truncation = storage.raw["data"]["truncation"]
    _histogram(metrics["state.size"], {**attrs, "phase": "before"}, _size(state))
    _histogram(metrics["state.size"], {**attrs, "phase": "after"}, _size(after))
    _counter(metrics["reclaimed_bytes"], attrs, _size(state) - _size(after))
    _counter(metrics["operations"], {"outcome": "returned", "commit_status": "unknown", "deletion_staged": True}, 1)


async def test_eager_protected_exclusions_do_not_serialize_or_count_deletion(
    reader: InMemoryMetricReader, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = _state(1)
    for entry in state.data.conversation_history:
        entry.messages[0].extension_data = {"_excluded": True}
    storage = _Storage(state)
    history = DurableHistoryProvider(prune_excluded=True)
    token = bind_durable_history(DurableHistoryBinding(storage))
    try:
        bag: dict[str, Any] = {}
        await history.get_messages(None, state=bag)
        serialized = Mock(side_effect=AssertionError("protected exclusions need no telemetry serialization"))
        monkeypatch.setattr(_history_provider, "eager_state_size", serialized)
        before = storage.state.to_dict()
        history.flush(bag)
        assert storage.state.to_dict() == before
        serialized.assert_not_called()
    finally:
        unbind_durable_history(token)
    metrics = _metrics(reader)
    assert set(metrics) == {"evaluations"}
    _counter(metrics["evaluations"], _attributes("eager", "protected"), 1)


async def test_concurrent_scopes_do_not_share_write_status_or_deletion(reader: InMemoryMetricReader) -> None:
    ready = asyncio.Event()
    release = asyncio.Event()
    deleting = _Storage(_state())
    small = _Storage(_state(1))

    async def evict() -> None:
        with telemetry.retention_operation(deleting.state):
            await retention.enforce_budget(deleting.state, max_state_bytes=BUDGET)
            ready.set()
            await release.wait()
            deleting.persist_state()

    async def check() -> None:
        await ready.wait()
        with telemetry.retention_operation(small.state):
            await retention.enforce_budget(small.state, max_state_bytes=BUDGET)
        release.set()

    await asyncio.gather(evict(), check())
    metrics = _metrics(reader)
    assert len(metrics["operations"].data.data_points) == 2
    _counter(metrics["operations"], {"outcome": "returned", "commit_status": "unknown", "deletion_staged": True}, 1)
    _counter(
        metrics["operations"],
        {"outcome": "returned", "commit_status": "not_attempted", "deletion_staged": False},
        1,
    )
    assert len(metrics["write_attempts"].data.data_points) == 1


async def test_nested_scope_state_identity_and_closed_inherited_context(reader: InMemoryMetricReader) -> None:
    outer = _Storage(_state(1))
    inner = _Storage(_state(1))
    release = asyncio.Event()

    async def inherited() -> None:
        await release.wait()
        # A task copied the ContextVar, but its originating operation has ended.
        assert telemetry._current(outer.state) is None
        await retention.enforce_budget(outer.state, max_state_bytes=BUDGET)
        outer.persist_state()

    with telemetry.retention_operation(outer.state):
        await retention.enforce_budget(outer.state, max_state_bytes=BUDGET)
        assert telemetry._current(inner.state) is None
        inner.persist_state()  # A different provider must not mark outer as written.
        with telemetry.retention_operation(inner.state):
            await retention.enforce_budget(inner.state, max_state_bytes=BUDGET)
            inner.persist_state()
        assert telemetry._current(outer.state) is not None
        task = asyncio.create_task(inherited())
    release.set()
    await task
    metrics = _metrics(reader)
    _counter(metrics["evaluations"], _attributes(outcome="below_threshold"), 3)
    _counter(
        metrics["operations"],
        {"outcome": "returned", "commit_status": "not_attempted", "deletion_staged": False},
        1,
    )
    _counter(metrics["operations"], {"outcome": "returned", "commit_status": "unknown", "deletion_staged": False}, 1)
    _counter(
        metrics["write_attempts"],
        {"stage": "set_state", "outcome": "returned", "commit_status": "unknown", "deletion_staged": False},
        1,
    )


async def test_dimensions_are_exact_bounded_values_and_never_state_data(reader: InMemoryMetricReader) -> None:
    await _entity(_Storage(_state())).run({"message": "private input", "correlationId": "private-request"})
    dimensions: dict[str, set[Any]] = {
        "mechanism": {"pressure", "eager"},
        "outcome": {"staged", "returned"},
        "commit_status": {"not_attempted", "unknown"},
        "phase": {"before", "after"},
        "stage": {"set_state"},
        "deletion_staged": {True, False},
    }
    for metric in _metrics(reader).values():
        for point in metric.data.data_points:
            assert point.attributes is not None
            for name, value in point.attributes.items():
                assert name in dimensions
                assert value in dimensions[name]


@pytest.mark.parametrize("mechanism", ["eager", "pressure"])
async def test_telemetry_on_off_and_broken_meter_do_not_change_state_or_return(
    reader: InMemoryMetricReader, monkeypatch: pytest.MonkeyPatch, mechanism: str
) -> None:
    initial = _state()
    for entry in initial.data.conversation_history[:2]:
        entry.messages[0].extension_data = {"_excluded": True}

    async def snapshot() -> tuple[int, dict[str, Any]]:
        state = deepcopy(initial)
        if mechanism == "pressure":
            removed = await retention.enforce_budget(state, max_state_bytes=BUDGET)
        else:
            storage = _Storage(state)
            state = storage.state
            DurableHistoryProvider._prune(
                DurableHistoryBinding(storage),
                [(entry, entry.messages[0]) for entry in state.data.conversation_history[:2]],
            )
            assert state.data.truncation is not None
            removed = state.data.truncation["evictedMessageCount"]
        return removed, state.to_dict()

    expected = await snapshot()
    assert expected[0] > 0
    assert _metrics(reader)
    for get_meter in (NoOpMeterProvider().get_meter, Mock(side_effect=RuntimeError("broken instrumentation"))):
        monkeypatch.setattr(telemetry, "get_meter", get_meter)
        telemetry._instruments.cache_clear()
        assert await snapshot() == expected


async def test_no_budget_and_no_eager_deletion_does_not_initialize_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    meter = Mock(side_effect=AssertionError("ordinary writes must not initialize retention instruments"))
    monkeypatch.setattr(telemetry, "get_meter", meter)
    telemetry._instruments.cache_clear()
    await _entity(_Storage(_state(1)), budget=None).run({"message": "new", "correlationId": "private-current"})
    meter.assert_not_called()


def test_commit_serialization_failure_keeps_not_attempted_status(
    reader: InMemoryMetricReader, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = _Storage(_state(1))
    state = storage.state
    failure = ValueError("private serialization failure")
    with pytest.raises(ValueError) as caught, telemetry.retention_operation(state):
        telemetry.record_retention(state, mechanism="eager", outcome="staged", removed_messages=1)
        monkeypatch.setattr(DurableAgentState, "to_dict", Mock(side_effect=failure))
        storage.persist_state()
    assert caught.value is failure
    assert storage.attempts == []
    attrs = {"outcome": "failed", "commit_status": "not_attempted", "deletion_staged": True}
    metrics = _metrics(reader)
    _counter(metrics["operations"], attrs, 1)
    _counter(metrics["write_attempts"], {**attrs, "stage": "serialization"}, 1)


async def test_eager_then_pressure_in_one_operation_accumulates_without_double_counting(
    reader: InMemoryMetricReader,
) -> None:
    storage = _Storage(_state())
    state = storage.state
    before_messages, before_entries = _counts(state)
    initial_bytes = _size(state)
    with telemetry.retention_operation(state):
        DurableHistoryProvider._prune(
            DurableHistoryBinding(storage),
            [(entry, entry.messages[0]) for entry in state.data.conversation_history[:2]],
        )
        eager_bytes = _size(state)
        pressure_removed = await retention.enforce_budget(state, max_state_bytes=BUDGET)
        assert pressure_removed > 0
        storage.persist_state()
    assert state.data.truncation is not None
    assert state.data.truncation["evictedMessageCount"] == pressure_removed + 2
    after_messages, after_entries = _counts(state)
    assert before_messages - after_messages == before_entries - after_entries == pressure_removed + 2
    metrics = _metrics(reader)
    for metric in ("removed_messages", "removed_entries"):
        _counter(metrics[metric], _attributes("eager"), 2)
        _counter(metrics[metric], _attributes(), pressure_removed)
    _counter(metrics["reclaimed_bytes"], _attributes("eager"), initial_bytes - eager_bytes)
    _counter(metrics["reclaimed_bytes"], _attributes(), eager_bytes - _size(state))
    _counter(metrics["operations"], {"outcome": "returned", "commit_status": "unknown", "deletion_staged": True}, 1)


def test_explicit_noop_skips_eager_serialization(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(telemetry, "get_meter", NoOpMeterProvider().get_meter)
    telemetry._instruments.cache_clear()
    serialized = Mock(side_effect=AssertionError("no-op instrumentation must not serialize"))
    monkeypatch.setattr(DurableAgentState, "to_dict", serialized)
    try:
        assert telemetry.eager_state_size(DurableAgentState()) is None
        serialized.assert_not_called()
    finally:
        telemetry._instruments.cache_clear()


async def test_recording_failure_does_not_replace_capacity_error(
    reader: InMemoryMetricReader, monkeypatch: pytest.MonkeyPatch
) -> None:
    instruments = telemetry._instruments()
    broken = Mock(side_effect=RuntimeError("reader failure"))
    monkeypatch.setattr(instruments.evaluations, "add", broken)
    state = _state(1)
    state.data.session = {"private_control": "p" * BUDGET}
    before = state.to_dict()
    with pytest.raises(retention.StateCapacityError) as caught:
        await retention.enforce_budget(state, max_state_bytes=BUDGET)
    assert caught.value.size_bytes == _size(state)
    assert state.to_dict() == before
    broken.assert_called_once()
