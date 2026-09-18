# Copyright (c) Microsoft. All rights reserved.

"""Private, functional delivery operations on complete canonical v2 JSON snapshots.

Staging returns detached candidates, never mutates its inputs, and does not persist
or authorize runtime writes. Existing response profiles remain opaque except for
a requested live lookup. Completion facts are independent of transcript retention.
"""

from __future__ import annotations

from collections.abc import Callable
from copy import copy, deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

from agent_framework import AgentResponse, Content, Message

from ._response_utils import (
    invocation_outcome,
    is_terminal_agent_response,
    load_agent_response,
    serialize_agent_response,
)
from ._shared_response import load_terminal_response, serialize_terminal_response, terminal_error
from ._shared_state_validation import (
    timestamp_reached,
    validate_completion_transition,
    validate_identifier,
    validate_shared_state,
)


def validate_delivery_state(state: dict[str, Any]) -> None:
    """Require canonical v2 and reject raw success/failure conflicts across all results.

    Shared snapshot validation precedes outcome checks. A non-tool shared error
    or reserved durable_status=error conflicts with succeeded, while a tool error
    need not mean the invocation failed. Opaque content and native profiles are
    not examined as runtime projections.
    """
    validate_shared_state(state)
    if state["schemaVersion"] != "2.0.0":
        raise ValueError("Delivery state requires schemaVersion 2.0.0.")
    for result in state["data"]["terminalResults"].values():
        if result["outcome"] != "succeeded":
            continue
        response = result["response"]
        failed = response.get("extensionData", {}).get("durable_status") == "error" or any(
            content["$type"] == "error"
            for message in response["messages"]
            if message["role"] != "tool"
            for content in message.get("contents", [])
        )
        if failed:
            raise ValueError("A succeeded terminal result conflicts with its response failure evidence.")


def _utc_now(now: datetime | None) -> datetime:
    timestamp = now if now is not None else datetime.now(timezone.utc)
    if not isinstance(timestamp, datetime) or timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("now must be an offset-aware datetime.")
    # Elapsed delivery windows must not use local wall-clock arithmetic across DST.
    return timestamp.astimezone(timezone.utc)


def stage_response(
    state: dict[str, Any],
    correlation_id: str,
    response: AgentResponse,
    *,
    delivery_window_seconds: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return a detached candidate with one original result and its completion receipt.

    Validate the entire source before duplicate detection. A duplicate returns an
    unchanged copy without inspecting the replacement response, window or clock,
    even if its result has expired. New results use a positive integer window and
    an aware clock, normalized to UTC before computing the elapsed deadline.
    Acknowledgements and delivery-expiry replies cannot establish new completions.
    """
    validate_delivery_state(state)
    validate_identifier(correlation_id, "correlation_id")
    data = state["data"]
    if correlation_id in data["completionReceipts"]:
        return deepcopy(state)
    if (
        isinstance(delivery_window_seconds, bool)
        or not isinstance(delivery_window_seconds, int)
        or delivery_window_seconds <= 0
    ):
        raise ValueError("delivery_window_seconds must be a positive integer.")
    timestamp = _utc_now(now)
    if not isinstance(response, AgentResponse):
        raise TypeError("response must be an AgentResponse.")
    if response.additional_properties.get("durable_status") in ("accepted", "already_completed") or any(
        content.type == "error" and content.error_code == "response_expired"
        for message in response.messages
        if message.role != "tool"
        for content in message.contents
    ):
        raise ValueError("A new completion requires a known invocation outcome, not an acknowledgement.")

    # Preserve the existing Core serializer's lazy-value and by-name policy. A
    # shared-origin response must also retain its opaque original wire snapshot.
    core_payload = serialize_agent_response(response)
    if getattr(response, "_original_shared_response", None) is not None:
        # The codec's base snapshot must not select aliases again for a typed
        # shared value. Use the already-validated JSON value on a shallow copy,
        # retaining the codec's original-projection comparison and wire shadow.
        projection = copy(response)
        projection._value = core_payload.get("value")  # pyright: ignore[reportPrivateUsage]
        projection._value_parsed = "value" in core_payload  # pyright: ignore[reportPrivateUsage]
        payload = serialize_terminal_response(projection)
    else:
        payload = serialize_terminal_response(core_payload)
    snapshot = load_agent_response(core_payload)
    outcome = invocation_outcome(snapshot)
    if outcome is None:
        raise ValueError("A new completion requires a known invocation outcome, not an acknowledgement.")
    completion = {
        "correlationId": correlation_id,
        "outcome": outcome,
        "completedAt": timestamp.isoformat(),
        "resultExpiresAt": (timestamp + timedelta(seconds=delivery_window_seconds)).isoformat(),
    }
    result: dict[str, Any] = {**completion, "response": payload}
    if outcome == "failed":
        result["error"] = terminal_error(snapshot)
    # Replace paths before copying, so even aliased source JSON subtrees cannot
    # accidentally change an unrelated field in the candidate.
    candidate = deepcopy({
        **state,
        "data": {
            **data,
            "terminalResults": {**data["terminalResults"], correlation_id: result},
            "completionReceipts": {
                **data["completionReceipts"],
                correlation_id: {**completion, "resultState": "available"},
            },
        },
    })
    validate_delivery_state(candidate)
    validate_completion_transition(state, candidate, now=timestamp)
    return candidate


def stage_expiry(state: dict[str, Any], *, now: datetime | None = None) -> tuple[dict[str, Any], int]:
    """Return a detached candidate and the number of payloads whose stored expiry is due.

    Only result payloads are removed. Receipts retain their original completion,
    outcome, expiry and unknown fields, adding an aware UTC unavailable timestamp.
    All comparisons are exact, including submicrosecond stored deadlines. No Core
    response, session or native profile is constructed to perform this operation.
    """
    validate_delivery_state(state)
    timestamp = _utc_now(now)
    data = state["data"]
    results = dict(data["terminalResults"])
    receipts = dict(data["completionReceipts"])
    removed = 0
    for correlation_id, result in data["terminalResults"].items():
        expiry = result.get("resultExpiresAt")
        if expiry is not None and timestamp_reached(expiry, now=timestamp):
            del results[correlation_id]
            receipts[correlation_id] = {
                **receipts[correlation_id],
                "resultState": "unavailable",
                "resultUnavailableAt": timestamp.isoformat(),
            }
            removed += 1
    candidate = deepcopy({**state, "data": {**data, "terminalResults": results, "completionReceipts": receipts}})
    validate_delivery_state(candidate)
    validate_completion_transition(state, candidate, now=timestamp)
    return candidate, removed


def _available_response(
    result: dict[str, Any],
    correlation_id: str,
    *,
    load_response: Callable[[dict[str, Any]], AgentResponse],
) -> AgentResponse:
    # Give the codec its own copy even though its current implementation also
    # detaches. Neither consumer edits nor synthesized error details may alias state.
    response = load_response(deepcopy(result["response"]))
    properties = response.additional_properties
    if properties.get("durable_status") in ("accepted", "already_completed"):
        properties.pop("durable_status")
    if "durable_outcome" in properties:
        properties["durable_outcome"] = result["outcome"]

    if result["outcome"] == "succeeded":
        # A recognized native content profile can project an error that the raw
        # shared snapshot cannot classify. Reject this requested projection rather
        # than deliver a failure in contradiction to the authoritative receipt.
        if is_terminal_agent_response(response):
            raise ValueError("A succeeded terminal result conflicts with its response failure evidence.")
        return response

    error = deepcopy(result["error"])
    code = error["code"] if error["code"] != "response_expired" else "agent_error"
    errors = [
        content
        for message in response.messages
        if message.role != "tool"
        for content in message.contents
        if content.type == "error"
    ]
    for content in errors:
        if not content.error_code or not content.error_code.strip() or content.error_code == "response_expired":
            content.error_code = code
        if not content.message or not content.message.strip():
            content.message = error["message"]
    properties.update(durable_status="error", correlation_id=correlation_id)
    if not errors:
        response.messages.append(
            Message(
                "system",
                [Content.from_error(message=error["message"], error_code=code, error_details=error.get("details"))],
            )
        )
    return response


def _lookup_response(
    state: dict[str, Any],
    correlation_id: str,
    *,
    now: datetime | None,
    clock: Callable[[], datetime],
    load_response: Callable[[dict[str, Any]], AgentResponse],
) -> AgentResponse | None:
    """Share lookup logic while retaining the reader's lazy clock and codec boundaries."""
    validate_delivery_state(state)
    validate_identifier(correlation_id, "correlation_id")
    data = state["data"]
    receipt = data["completionReceipts"].get(correlation_id)
    if receipt is None:
        return None
    expiry = receipt.get("resultExpiresAt")
    if receipt["resultState"] == "available" and (
        expiry is None or not timestamp_reached(expiry, now=_utc_now(now if now is not None else clock()))
    ):
        return _available_response(data["terminalResults"][correlation_id], correlation_id, load_response=load_response)
    return AgentResponse(
        messages=[
            Message(
                "system",
                [
                    Content.from_error(
                        message="This request completed, but its response delivery window has expired.",
                        error_code="response_expired",
                    )
                ],
            )
        ],
        additional_properties={
            "durable_status": "already_completed",
            "correlation_id": correlation_id,
            "durable_outcome": receipt["outcome"],
        },
    )


def lookup_response(state: dict[str, Any], correlation_id: str, *, now: datetime | None = None) -> AgentResponse | None:
    """Validate raw v2 state and project only the requested live response, without mutation.

    Return None for an absent receipt. An unavailable receipt or reached deadline
    returns an already-completed response carrying the retained outcome. No
    transcript fallback or response-profile projection occurs for those cases.
    """
    return _lookup_response(
        state,
        correlation_id,
        now=now,
        clock=lambda: datetime.now(timezone.utc),
        load_response=load_terminal_response,
    )
