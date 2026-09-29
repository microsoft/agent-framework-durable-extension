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

from agent_framework import AgentResponse

from ._delivery_state import (
    _lookup_response,  # pyright: ignore[reportPrivateUsage]
    validate_delivery_state,
)
from ._durable_agent_state import DurableAgentState
from ._shared_response import load_terminal_response

__all__ = ["SharedAgentStateReader", "read_agent_state"]


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
        validate_delivery_state(raw)
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
        return _lookup_response(
            self._raw,
            correlation_id,
            now=now,
            clock=lambda: datetime.now(timezone.utc),
            load_response=load_terminal_response,
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
