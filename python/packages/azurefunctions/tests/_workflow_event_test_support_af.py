# Copyright (c) Microsoft. All rights reserved.

"""Buffered-event history and native Functions SDK replay helpers."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from azure.durable_functions import DurableOrchestrationContext
from azure.durable_functions.models.ReplaySchema import ReplaySchema
from azure.durable_functions.models.TaskOrchestrationExecutor import TaskOrchestrationExecutor


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
        maximumShortTimerDuration="00:05:00",
        longRunningTimerIntervalDuration="00:03:00",
    )


def _execute(rows: list[dict[str, Any]], function: Callable[..., Any]) -> tuple[dict[str, Any], Any]:
    context = _context(rows)
    result = TaskOrchestrationExecutor().execute(context, context.histories, function)
    return json.loads(result), context
