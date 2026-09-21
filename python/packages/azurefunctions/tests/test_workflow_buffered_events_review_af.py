# Copyright (c) Microsoft. All rights reserved.

"""Buffered event occurrence/FIFO controls using the real Functions SDK.

Native SDK histories below are explicit service-event fixtures. Mixed workflow
histories come from registered activities and fresh DT SDK episodes, translated
to AF by the existing replay helper. Neither is a live Functions-host capture.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Generator
from copy import deepcopy
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest
from azure.durable_functions import DurableOrchestrationContext
from azure.durable_functions.models.ReplaySchema import ReplaySchema
from azure.durable_functions.models.Task import TaskState
from azure.durable_functions.models.TaskOrchestrationExecutor import TaskOrchestrationExecutor
from test_workflow_mixed_hitl_review import _atomic_actions, _Episodes, _mixed
from test_workflow_recorded_replay_review import _af_replay

from agent_framework_azurefunctions._workflow_af_context import AzureFunctionsWorkflowContext


def _event(kind: int, event_id: int = -1, **fields: Any) -> dict[str, Any]:
    return {
        "EventType": kind,
        "EventId": event_id,
        "IsPlayed": True,
        "Timestamp": "2026-09-21T00:00:00Z",
        "Version": None,
        **fields,
    }


def _prefix() -> list[dict[str, Any]]:
    return [_event(12), _event(0, Name="buffered-events", Input="null"), _event(4, 0, Name="gate", Input="null")]


def _context(rows: list[dict[str, Any]]) -> Any:
    return DurableOrchestrationContext(
        rows,
        instanceId="buffered-events",
        isReplaying=True,
        parentInstanceId=None,
        input="null",
        upperSchemaVersion=ReplaySchema.V3.value,
    )


def _execute(rows: list[dict[str, Any]], function: Callable[..., Any]) -> tuple[dict[str, Any], Any]:
    context = _context(rows)
    result = TaskOrchestrationExecutor().execute(context, context.histories, function)
    return json.loads(result), context


@pytest.mark.parametrize("early", [False, True])
def test_distinct_same_payload_events_each_complete_one_wait_and_never_a_third(early: bool) -> None:
    payload = {"answer": [10, False, None]}
    first = _event(15, 101, Name="a", Input=json.dumps(payload))
    second = _event(15, 102, Name="a", Input=json.dumps(payload))
    ack = _event(5, TaskScheduledId=0, Result="null")
    initial = [*_prefix(), *([first, ack] if early else [ack, first])]
    assert first["EventId"] != second["EventId"] and first["Input"] == second["Input"]

    for history, expected in ((initial, [payload]), ([*initial, second], [payload, payload])):
        received: list[Any] = []
        waits: list[Any] = []

        # Each trial is fully driven before the loop advances. Bind its
        # observations explicitly so suspended cold generators cannot alias it.
        def run(
            context: Any, *, received: list[Any] = received, waits: list[Any] = waits
        ) -> Generator[Any, Any, list[Any]]:
            adapter = AzureFunctionsWorkflowContext(context)
            yield adapter.prepare_activity_task("gate", "null")
            # Bounded even without the fix: stale callbacks must not satisfy
            # the next wait, and a later identical event must not be deduped.
            for _ in range(3):
                waiting = adapter.wait_for_external_event("a")
                waits.append(waiting)
                received.append((yield waiting))
            return received  # noqa: B901 - Durable orchestrator result.

        for _ in range(2):
            received.clear()
            waits.clear()
            state, context = _execute(history, run)
            assert not state["isDone"] and received == expected
            assert len(waits) == len(expected) + 1
            assert all(wait.state is TaskState.SUCCEEDED for wait in waits[:-1])
            assert waits[-1].state is TaskState.RUNNING
            assert "a" not in context.deferred_tasks
            assert context.open_tasks["a"] == [waits[-1]]
            actions = _atomic_actions(state["actions"])
            assert [a["externalEventName"] for a in actions if a["actionType"] == 6] == ["a"] * len(waits)


@pytest.mark.parametrize("early", [False, True])
def test_buffered_events_are_fifo_per_name_and_keep_the_losing_wait(early: bool) -> None:
    arrivals = [
        _event(15, 101, Name="a", Input='"first"'),
        _event(15, 102, Name="b", Input='"sibling"'),
        _event(15, 103, Name="a", Input='"second"'),
        _event(15, 104, Name="a", Input='"second"'),
    ]
    ack = _event(5, TaskScheduledId=0, Result="null")
    rows = [*_prefix(), *(arrivals + [ack] if early else [ack, *arrivals])]
    for _ in range(2):
        received: list[Any] = []
        waits: dict[str, list[Any]] = {"a": [], "b": []}

        def run(
            context: Any, *, received: list[Any] = received, waits: dict[str, list[Any]] = waits
        ) -> Generator[Any, Any, list[Any]]:
            adapter = AzureFunctionsWorkflowContext(context)
            yield adapter.prepare_activity_task("gate", "null")
            sibling = adapter.wait_for_external_event("b")
            waits["b"].append(sibling)
            first = adapter.wait_for_external_event("a")
            waits["a"].append(first)
            winner = yield adapter.task_any([first, sibling])
            assert winner is first
            received.append(adapter.get_task_result(winner))
            for _ in range(2):
                waiting = adapter.wait_for_external_event("a")
                waits["a"].append(waiting)
                received.append((yield waiting))
            # The losing task may already be complete. Reuse the very same
            # object instead of registering another wait that steals its event.
            received.append((yield sibling))
            extra = adapter.wait_for_external_event("a")
            waits["a"].append(extra)
            yield extra
            pytest.fail("No fifth event exists to satisfy the extra wait")

        state, context = _execute(rows, run)
        assert not state["isDone"]
        assert received == ["first", "second", "second", "sibling"]
        assert len(waits["a"]) == 4 and len(waits["b"]) == 1
        assert waits["b"][0].state is TaskState.SUCCEEDED
        assert waits["a"][-1].state is TaskState.RUNNING
        assert "a" not in context.deferred_tasks and "b" not in context.deferred_tasks
        actions = _atomic_actions(state["actions"])
        assert [a["externalEventName"] for a in actions if a["actionType"] == 6] == ["a", "b", "a", "a", "a"]


@pytest.mark.parametrize("early", [False, True])
def test_mixed_rejections_consume_each_buffered_occurrence_before_correction(
    early: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    workflow, controls = _mixed(sibling_request=True)
    transport = _Episodes(workflow)
    transport.client.start_workflow("go", instance_id="root")

    def deliver() -> None:
        for value in ("first-invalid", "second-invalid", 10):
            transport.signal("root", event_name="a", data=value)
        transport.flush()

    if early:
        deliver()
    transport.complete_named("root", "seed")
    transport.complete_named("root::sub::0", "child")
    for executor in ("parent", "writer", "sink"):
        transport.complete_named("root", executor)
    if not early:
        deliver()

    original_wait = AzureFunctionsWorkflowContext.wait_for_external_event
    registrations: list[str] = []

    def observe_wait(context: AzureFunctionsWorkflowContext, name: str) -> Any:
        registrations.append(name)
        assert registrations.count(name) <= (3 if name == "a" else 1), registrations
        return original_wait(context, name)

    monkeypatch.setattr(AzureFunctionsWorkflowContext, "wait_for_external_event", observe_wait)

    def cold(expected_waits: list[str]) -> None:
        transport.cold("root")
        before = deepcopy(controls["seen"])
        registrations.clear()
        replay = _af_replay(transport.histories["root"], workflow, instance="root")
        assert not replay["isDone"]
        assert replay["customStatus"] == {
            key: value for key, value in transport.statuses["root"].items() if key != "events"
        }
        assert replay["customStatus"]["subworkflows"] == {"sub": {"0": "root::sub::0"}}
        assert registrations == expected_waits
        actions = _atomic_actions(replay["actions"])
        assert [
            (action["functionName"], json.loads(json.loads(action["input"])))
            for action in actions
            if action["actionType"] == 0
        ] == [
            (event.taskScheduled.name, json.loads(json.loads(event.taskScheduled.input.value)))
            for event in transport.histories["root"]
            if event.HasField("taskScheduled")
        ]
        assert controls["seen"] == before

    cold(["a", "d"])
    for rejected in range(2):
        transport.complete_named("root", "parent")
        assert controls["seen"] == [] and transport.pending() == {"a", "d", "sub~0~b"}
        cold(["a", "d", *(["a"] * (rejected + 1))])
    assert [
        payload["message"]["response"]
        for instance, _, payload in transport.executed
        if instance == "root" and payload["is_hitl_response"]
    ] == ["first-invalid", "second-invalid"]

    # d completes while a's accepted response activity is still unacknowledged.
    # Its original losing wait must survive both rejections and that activity.
    transport.reply("d", 13)
    cold(["a", "d", "a", "a"])
    transport.complete_named("root", "parent")
    transport.complete_named("root", "sink")
    assert controls["seen"] == [("a", 10)]
    cold(["a", "d", "a", "a", "c"])
    transport.complete_named("root", "parent")
    transport.complete_named("root", "sink")
    assert controls["seen"] == [("a", 10), ("d", 13)]
    assert transport.pending() == {"c", "sub~0~b"}
    cold(["a", "d", "a", "a", "c"])
    assert [
        payload["message"]["response"]
        for instance, _, payload in transport.executed
        if instance == "root" and payload["is_hitl_response"]
    ] == ["first-invalid", "second-invalid", 10, 13]


def test_adapter_installs_once_and_leaves_non_event_callback_reads_unchanged() -> None:
    context = _context(_prefix())
    first, second, activity, entity = Mock(), Mock(), Mock(), Mock()
    context.deferred_tasks = {"a": first, 7: activity, "entity-response": entity}
    adapter = AzureFunctionsWorkflowContext(context)
    buffer = context.deferred_tasks
    buffer["a"] = second
    AzureFunctionsWorkflowContext(context)
    assert context.deferred_tasks is buffer
    assert buffer[7] is activity and buffer[7] is activity
    assert buffer["entity-response"] is entity and buffer["entity-response"] is entity
    waiting = adapter.wait_for_external_event("a")
    assert waiting.state is TaskState.RUNNING
    assert buffer["a"] is first and "a" in buffer
    assert buffer["a"] is second and "a" not in buffer
    for callback in (first, second, activity, entity):
        callback.assert_not_called()


@pytest.mark.parametrize("layout", ["absent", "none", "tuple-values", "custom-mapping"])
def test_unknown_private_buffer_layout_is_not_replaced(layout: str) -> None:
    class FutureBuffer(dict):
        pass

    context: Any = SimpleNamespace(wait_for_external_event=Mock(return_value=object()))
    if layout != "absent":
        context.deferred_tasks = {
            "none": None,
            "tuple-values": {"a": (object(), True, "Name")},
            "custom-mapping": FutureBuffer(),
        }[layout]
    before = getattr(context, "deferred_tasks", None)
    adapter = AzureFunctionsWorkflowContext(context)
    assert adapter.wait_for_external_event("a") is context.wait_for_external_event.return_value
    context.wait_for_external_event.assert_called_once_with("a")
    assert getattr(context, "deferred_tasks", None) is before
    assert hasattr(context, "deferred_tasks") is (layout != "absent")


def test_buffer_workaround_does_not_swallow_activity_failure_or_change_timer_cancellation() -> None:
    rows = [
        *_prefix(),
        _event(15, 101, Name="a", Input="10"),
        _event(6, TaskScheduledId=0, Reason="gate failed", Details="activity error"),
    ]
    waits: list[Any] = []

    def run(context: Any) -> Generator[Any, Any, Any]:
        adapter = AzureFunctionsWorkflowContext(context)
        yield adapter.prepare_activity_task("gate", "null")
        waiting = adapter.wait_for_external_event("a")
        waits.append(waiting)
        return (yield waiting)  # noqa: B901 - Durable orchestrator result.

    with pytest.raises(Exception, match="gate failed"):
        _execute(rows, run)
    assert waits == []
    context = _context(_prefix())
    adapter = AzureFunctionsWorkflowContext(context)
    timer = adapter.create_timer(context.current_utc_datetime)
    adapter.cancel_task(timer)
    assert timer.is_cancelled
    completed = adapter.create_timer(context.current_utc_datetime)
    completed.set_value(is_error=False, value=None)
    with pytest.raises(ValueError, match="completed"):
        adapter.cancel_task(completed)
    failed = adapter.wait_for_external_event("a")
    error = RuntimeError("failed wait")
    failed.set_value(is_error=True, value=error)
    with pytest.raises(RuntimeError, match="failed wait") as raised:
        adapter.get_task_result(failed)
    assert raised.value is error
