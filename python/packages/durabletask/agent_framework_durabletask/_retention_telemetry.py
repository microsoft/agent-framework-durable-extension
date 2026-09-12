# Copyright (c) Microsoft. All rights reserved.

"""Bounded observations of local retention, never proof of a durable commit.

Deletion measurements describe staged state at the retention boundary. Operation and
write measurements describe the later host call, which may itself only stage a write.
Only the OpenTelemetry API is required. No provider or exporter is configured here.
"""

from __future__ import annotations

import json
from collections.abc import Generator
from contextlib import contextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, Literal

from opentelemetry.metrics import NoOpMeter, get_meter

if TYPE_CHECKING:
    from ._durable_agent_state import DurableAgentState

_Mechanism = Literal["eager", "pressure"]
_Outcome = Literal["below_threshold", "staged", "protected_floor", "unreachable_target", "protected"]


class _Instruments:
    def __init__(self) -> None:
        meter = get_meter("agent_framework.durabletask")
        self.noop = isinstance(meter, NoOpMeter)
        self.evaluations = meter.create_counter(
            "durable.retention.evaluations", unit="{evaluation}", description="Local retention evaluations."
        )
        self.budget = meter.create_histogram(
            "durable.retention.budget", unit="By", description="Requested resolved whole-entity pressure budget."
        )
        self.size = meter.create_histogram(
            "durable.retention.state.size", unit="By", description="Serialized entity JSON at a retention boundary."
        )
        self.messages = meter.create_counter(
            "durable.retention.removed_messages", unit="{message}", description="Messages removed from staged state."
        )
        self.entries = meter.create_counter(
            "durable.retention.removed_entries", unit="{entry}", description="Entries removed from staged state."
        )
        self.reclaimed = meter.create_counter(
            "durable.retention.reclaimed_bytes", unit="By", description="Nonnegative byte reduction in staged state."
        )
        self.capacity_failures = meter.create_counter(
            "durable.retention.capacity_failures", unit="{failure}", description="Unreachable pressure targets."
        )
        self.writes = meter.create_counter(
            "durable.retention.write_attempts",
            unit="{attempt}",
            description="State serialization or host set_state outcomes, not durable commit confirmation.",
        )
        self.operations = meter.create_counter(
            "durable.retention.operations",
            unit="{operation}",
            description="Run operations with retention observations and their host write status.",
        )


@lru_cache(maxsize=1)
def _instruments() -> _Instruments:
    # Cache the API's proxy too: it can bind to an SDK installed after import.
    return _Instruments()


@dataclass
class _Operation:
    state: DurableAgentState
    active: bool = True
    observed: bool = False
    removed_messages: int = 0
    removed_entries: int = 0
    commit_status: Literal["not_attempted", "unknown"] = "not_attempted"


_operation: ContextVar[_Operation | None] = ContextVar("durable_retention_operation", default=None)


def _current(state: DurableAgentState) -> _Operation | None:
    operation = _operation.get()
    if operation is not None and operation.active and operation.state is state:
        return operation
    return None


@contextmanager
def retention_operation(state: DurableAgentState) -> Generator[None]:
    """Isolate run observations through rollback and the host write attempt.

    The identity check prevents attributing another state's retention to this run.
    Closing the object also invalidates contexts inherited by unfinished child tasks.
    """
    operation = _Operation(state)
    token = _operation.set(operation)
    failed = False
    try:
        yield
    except BaseException:
        failed = True
        raise
    finally:
        operation.active = False
        _operation.reset(token)
        if operation.observed:
            # Optional telemetry must not replace the operation's result or error.
            with suppress(Exception):
                _instruments().operations.add(
                    1,
                    {
                        "outcome": "failed" if failed else "returned",
                        "commit_status": operation.commit_status,
                        "deletion_staged": operation.removed_messages > 0,
                    },
                )


def eager_state_size(state: DurableAgentState) -> int | None:
    """Measure only an eligible eager-prune boundary, skipping an explicit no-op meter.

    The API has no portable enabled check for a proxy or an SDK without readers.
    Those meters still measure eligible eager deletions, but never ordinary flushes.
    """
    try:
        if _instruments().noop:
            return None
        return len(json.dumps(state.to_dict()))
    except Exception:
        return None


def record_retention(
    state: DurableAgentState,
    *,
    mechanism: _Mechanism,
    outcome: _Outcome,
    before_bytes: int | None = None,
    after_bytes: int | None = None,
    budget_bytes: int | None = None,
    removed_messages: int = 0,
    removed_entries: int = 0,
) -> None:
    """Record actual staged changes, never the exclusions on a detached trial plan."""
    operation = _current(state)
    if operation is not None:
        operation.observed = True
        operation.removed_messages += removed_messages
        operation.removed_entries += removed_entries
    attributes = {"mechanism": mechanism, "outcome": outcome, "commit_status": "not_attempted"}
    with suppress(Exception):
        instruments = _instruments()
        instruments.evaluations.add(1, attributes)
        if budget_bytes is not None:
            instruments.budget.record(budget_bytes, attributes)
        if before_bytes is not None:
            instruments.size.record(before_bytes, {**attributes, "phase": "before"})
        if after_bytes is not None:
            instruments.size.record(after_bytes, {**attributes, "phase": "after"})
        if removed_messages:
            instruments.messages.add(removed_messages, attributes)
            if before_bytes is not None and after_bytes is not None:
                instruments.reclaimed.add(max(0, before_bytes - after_bytes), attributes)
        if removed_entries:
            instruments.entries.add(removed_entries, attributes)
        if outcome in ("protected_floor", "unreachable_target"):
            instruments.capacity_failures.add(1, attributes)


def record_write(
    state: DurableAgentState,
    *,
    stage: Literal["serialization", "set_state"],
    outcome: Literal["returned", "failed"],
) -> None:
    """Observe a host write boundary without interpreting its return as persistence."""
    operation = _current(state)
    if operation is None:
        return
    if stage == "set_state":
        # Even a failed host call may have staged work before it raised.
        operation.commit_status = "unknown"
    if operation.observed:
        with suppress(Exception):
            _instruments().writes.add(
                1,
                {
                    "stage": stage,
                    "outcome": outcome,
                    "commit_status": operation.commit_status,
                    "deletion_staged": operation.removed_messages > 0,
                },
            )
