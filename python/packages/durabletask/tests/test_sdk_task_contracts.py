# Copyright (c) Microsoft. All rights reserved.

"""SDK baseline contracts exercised through the framework task adapter."""

from typing import Any
from unittest.mock import Mock

import pytest
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.task import CompletableTask, OrchestrationContext, TaskFailedError

from agent_framework_durabletask._workflows.dt_context import DurableTaskWorkflowContext


def test_task_all_waits_for_remaining_children_after_failure() -> None:
    context = DurableTaskWorkflowContext(Mock(spec=OrchestrationContext))
    failed: CompletableTask[Any] = CompletableTask()
    pending: CompletableTask[Any] = CompletableTask()
    group = context.task_all([failed, pending])

    failed.fail("activity failed", ValueError("invalid value"))
    assert not group.is_complete
    pending.complete("finished")

    assert group.is_complete and group.is_failed
    with pytest.raises(TaskFailedError) as raised:
        group.get_result()
    assert raised.value.details.is_caused_by(ValueError)


@pytest.mark.parametrize("cancel_first", [False, True], ids=["work-wins", "cancel-wins"])
def test_nested_task_race_keeps_first_completion_as_winner(cancel_first: bool) -> None:
    context = DurableTaskWorkflowContext(Mock(spec=OrchestrationContext))
    first: CompletableTask[Any] = CompletableTask()
    second: CompletableTask[Any] = CompletableTask()
    cancel: CompletableTask[Any] = CompletableTask()
    work = context.task_all([first, second])
    race = context.task_any([cancel, work])

    if cancel_first:
        cancel.complete("cancelled")
        assert race.is_complete and race.get_result() is cancel

    first.complete("first")
    assert not work.is_complete
    if not cancel_first:
        assert not race.is_complete
    second.complete("second")
    assert work.is_complete and work.get_result() == ["first", "second"]
    assert race.is_complete
    assert race.get_result() is (cancel if cancel_first else work)

    if not cancel_first:
        cancel.complete("late cancellation")
        assert race.get_result() is work


@pytest.mark.parametrize("error_type", ["ValueError", "builtins.ValueError"])
def test_recorded_failure_names_remain_readable_without_rewriting_history(error_type: str) -> None:
    recorded = pb.TaskFailureDetails(errorType=error_type, errorMessage="recorded failure")
    failure = TaskFailedError("activity failed", recorded)

    assert failure.details.is_caused_by(ValueError)
    assert not failure.details.is_caused_by(TypeError)
    assert failure.details.error_type == error_type
    assert recorded.errorType == error_type


def test_new_failure_uses_qualified_name() -> None:
    task: CompletableTask[Any] = CompletableTask()
    task.fail("activity failed", ValueError("invalid value"))

    assert task.get_exception().details.error_type == "builtins.ValueError"
