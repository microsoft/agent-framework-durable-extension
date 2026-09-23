# Copyright (c) Microsoft. All rights reserved.

"""Operation-count bounds through real SDK replay, not wall-clock thresholds.

Observers delegate every history read and JSON parse. No guard helper supplies
the oracle, and neither SDK task lookup nor callback delivery is replaced.
Histories are constructed service-event fixtures, not live host captures.
"""

from __future__ import annotations

import json
from collections.abc import Generator
from types import SimpleNamespace
from typing import Any

import pytest
from _workflow_event_test_support_af import _context, _event, _prefix
from _workflow_replay_test_support import _atomic_actions
from azure.durable_functions.models.history.HistoryEvent import HistoryEvent
from azure.durable_functions.models.Task import TaskState
from azure.durable_functions.models.TaskOrchestrationExecutor import TaskOrchestrationExecutor

from agent_framework_azurefunctions import _workflow_af_context as adapter_module
from agent_framework_azurefunctions._workflow_af_context import AzureFunctionsWorkflowContext


class _EqualityCountedWire(str):
    comparisons: list[int]

    def __new__(cls, wire: str, comparisons: list[int]) -> _EqualityCountedWire:
        value = super().__new__(cls, wire)
        value.comparisons = comparisons
        return value

    def __eq__(self, other: object) -> bool:
        self.comparisons[0] += 1
        return str.__eq__(self, other)

    def __ne__(self, other: object) -> bool:
        self.comparisons[0] += 1
        return str.__ne__(self, other)

    __hash__ = str.__hash__


def _measure_replay(monkeypatch: pytest.MonkeyPatch, count: int, *, early: bool, mode: str) -> dict[str, int]:
    expected = {"keep": [None, False]} if mode == "framework-object" else None
    wire = json.dumps(expected)
    comparisons = [0]
    names = [f"native-{index}" if mode == "native" else "reply" for index in range(count)]
    arrivals = [
        _event(15, 10000 + index, Name=name, Input=_EqualityCountedWire(wire, comparisons))
        for index, name in enumerate(names)
    ]
    # Unmatched names and non-event rows must not be revisited on every pop.
    unused = [_event(15, 20000 + index, Name=f"unused-{index}", Input='{"keep":true}') for index in range(count)]
    acknowledgements = [_event(5, TaskScheduledId=index, Result="null") for index in range(count + 1)]
    rows = [*_prefix(), *unused]
    if early:
        rows.extend([*arrivals, *acknowledgements])
    else:
        rows.append(acknowledgements[0])
        for arrival, ack in zip(arrivals, acknowledgements[1:]):
            rows.extend([arrival, ack])
    context = _context(rows)
    history_ids = {id(event) for event in context.histories}
    counts = {"event_type": 0, "Name": 0, "Input": 0, "plain_json": 0}
    original_getattribute = HistoryEvent.__getattribute__

    def read(event: HistoryEvent, name: str) -> Any:
        if id(event) in history_ids and name in counts:
            counts[name] += 1
        return original_getattribute(event, name)

    def loads(value: Any, *args: Any, **kwargs: Any) -> Any:
        counts["plain_json"] += 1
        return json.loads(value, *args, **kwargs)

    received: list[Any] = []
    waits: list[Any] = []

    def run(native: Any) -> Generator[Any, Any, None]:
        adapter = AzureFunctionsWorkflowContext(native)
        yield adapter.prepare_activity_task("gate", "null")
        for name in names:
            # Exercise negative lookups without allocating event-index entries.
            adapter.wait_for_external_event(f"absent-{len(received)}")
            selected = native if mode == "native" else AzureFunctionsWorkflowContext(native)
            waiting = selected.wait_for_external_event(name)
            waits.append(waiting)
            received.append((yield waiting))
            # A real checkpoint bounds native resume recursion even when every
            # event is buffered before the first activity acknowledgement.
            yield adapter.prepare_activity_task("checkpoint", "null")
        final = adapter.wait_for_external_event("next-native" if mode == "native" else "reply")
        waits.append(final)
        yield final
        pytest.fail("A consumed occurrence satisfied another wait")

    with monkeypatch.context() as observer:
        observer.setattr(HistoryEvent, "__getattribute__", read)
        # Only the adapter's plain parser is observed. SDK decoders stay real.
        observer.setattr(adapter_module, "json", SimpleNamespace(loads=loads, dumps=json.dumps))
        state = json.loads(TaskOrchestrationExecutor().execute(context, context.histories, run))
    assert not state["isDone"] and received == [expected] * count
    assert len(waits) == count + 1 and waits[-1].state is TaskState.RUNNING
    assert all(wait.state is TaskState.SUCCEEDED for wait in waits[:-1])
    actions = _atomic_actions(state["actions"])
    assert sum(action["actionType"] == 0 for action in actions) == count + 1
    assert sum(action["actionType"] == 6 for action in actions) == count + 1
    assert comparisons == [0]  # Equal bytes never identify or deduplicate an occurrence.
    # One scalar classification, or a classification plus one task-level parse
    # for each structured value. Native waits never use the adapter JSON parser.
    assert counts["plain_json"] == count * {"native": 0, "framework-null": 1, "framework-object": 2}[mode]
    return counts


@pytest.mark.parametrize("early", [False, True])
@pytest.mark.parametrize("mode", ["framework-null", "framework-object", "native"])
def test_wait_and_pop_history_reads_and_json_work_scale_linearly(
    early: bool, mode: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    small = _measure_replay(monkeypatch, 32, early=early, mode=mode)
    large = _measure_replay(monkeypatch, 64, early=early, mode=mode)
    for operation in small:
        # Fixed SDK prologue work is allowed. A repeated full-history or
        # same-name bucket scan grows fourfold and violates these bounds.
        assert large[operation] <= 2 * small[operation] + 20, (operation, small, large)
        assert large[operation] <= 30 * 64 + 50, (operation, large)


@pytest.mark.parametrize("early", [False, True])
def test_1100_equal_null_events_keep_occurrences_without_payload_comparisons(
    early: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    counts = _measure_replay(monkeypatch, 1100, early=early, mode="framework-null")
    for operation, value in counts.items():
        assert value <= 30 * 1100 + 50, (operation, counts)
