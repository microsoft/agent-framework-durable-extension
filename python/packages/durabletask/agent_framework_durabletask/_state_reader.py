# Copyright (c) Microsoft. All rights reserved.

"""Read shared v2 snapshots without opting mutable entity state into v2 writes.

The reader owns detached JSON, not a runtime session or a mutable state model.
Construction validates the whole snapshot but never projects response profiles.
Only a requested, available result is decoded through the standalone response
codec. Exporting the snapshot does not serialize that consumer projection.
"""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from agent_framework import AgentResponse, Content, Message

from ._durable_agent_state import DurableAgentState
from ._response_utils import is_terminal_agent_response
from ._shared_response import load_terminal_response
from ._shared_state_validation import timestamp_reached, validate_identifier, validate_shared_state

__all__ = ["SharedAgentStateReader", "read_agent_state"]


def _validate_completion_outcomes(results: dict[str, Any]) -> None:
    """Reject affirmative failure evidence on a succeeded result without decoding it.

    This deliberately adopts only the donor's source-level outcome check after
    shared snapshot validation. The shared schema alone does not classify error
    content. A non-tool shared error or reserved durable_status=error conflicts
    with succeeded, while a tool error need not mean the invocation failed.
    Opaque content and native profiles are not examined as runtime projections.
    """
    for result in results.values():
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


def _available_response(result: dict[str, Any], correlation_id: str) -> AgentResponse:
    # Give the codec its own copy even though its current implementation also
    # detaches. Neither consumer edits nor synthesized error details may alias state.
    response = load_terminal_response(deepcopy(result["response"]))
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


class SharedAgentStateReader:
    """Read-only view of one canonical schema 2.0.0 snapshot.

    There is no data model or mutation API. Unknown JSON, exact timestamp strings,
    sessions, bindings and response profiles remain in a private detached snapshot.
    Validation establishes snapshot consistency, not runtime-profile authorization
    or compatibility for resuming an agent. Response projection is lazy per lookup.
    """

    __slots__ = ("_raw",)

    def __init__(self, raw: dict[str, Any]) -> None:
        """Validate the complete v2 snapshot before retaining a detached copy."""
        validate_shared_state(raw)
        if raw["schemaVersion"] != "2.0.0":
            raise ValueError("SharedAgentStateReader requires schemaVersion 2.0.0.")
        _validate_completion_outcomes(raw["data"]["terminalResults"])
        self._raw = deepcopy(raw)

    @property
    def schema_version(self) -> str:
        """Return the exact shared schema version without changing writer defaults."""
        return "2.0.0"

    @property
    def message_count(self) -> int:
        """Count conversation entries, matching the legacy reader's counting convention."""
        return len(self._raw["data"]["conversationHistory"])

    def to_dict(self) -> dict[str, Any]:
        """Return detached original JSON, never a re-serialized response projection."""
        return deepcopy(self._raw)

    def to_json(self) -> str:
        """Encode the original snapshot without normalizing timestamps or adding fields."""
        return json.dumps(self._raw, allow_nan=False)

    def try_get_agent_response(self, correlation_id: str, *, now: datetime | None = None) -> AgentResponse | None:
        """Read a canonical result or receipt, never a transcript response.

        A missing receipt returns None. An unavailable receipt or a reached result
        deadline returns an already-completed response with the retained outcome.
        Exact fractional expiry comparisons use an offset-aware clock, defaulting
        to current UTC time. A live failed receipt overrides provider delivery hints
        only in the detached consumer response. Incompatible targeted native
        profiles may fail projection without affecting stored JSON or other lookups.
        """
        validate_identifier(correlation_id, "correlation_id")
        data = self._raw["data"]
        receipt = data["completionReceipts"].get(correlation_id)
        if receipt is None:
            return None
        expiry = receipt.get("resultExpiresAt")
        if receipt["resultState"] == "available" and (
            expiry is None or not timestamp_reached(expiry, now=now if now is not None else datetime.now(timezone.utc))
        ):
            return _available_response(data["terminalResults"][correlation_id], correlation_id)
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


def read_agent_state(raw: dict[str, Any] | str) -> SharedAgentStateReader | DurableAgentState:
    """Dispatch exact shared versions to a read-only v2 view or the existing legacy loader.

    An empty object explicitly requests fresh legacy state. Other inputs must be
    objects with a supported schemaVersion and object-valued data. Legacy payloads
    retain the existing loader's capabilities and limitations, including nullable
    usage counts. They are not passed through strict shared snapshot validation.
    No private prototype detection, migration or writer policy is applied.
    """
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            raise ValueError("The durable agent state is not valid JSON.") from None
    if not isinstance(raw, dict):
        raise ValueError("The durable agent state must be a JSON object.")
    state = raw
    if not state:
        return DurableAgentState()
    version = state.get("schemaVersion")
    if not isinstance(version, str) or version not in ("1.0.0", "1.1.0", "1.2.0", "2.0.0"):
        raise ValueError("The durable agent state schemaVersion is missing or unsupported.")
    if version == "2.0.0":
        return SharedAgentStateReader(state)
    if not isinstance(state.get("data"), dict):
        raise ValueError("The durable agent state data must be a JSON object.")
    return DurableAgentState.from_dict(state)
