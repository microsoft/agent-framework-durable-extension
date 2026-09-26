# Copyright (c) Microsoft. All rights reserved.

"""Operation-scoped Core session snapshots, staged in the existing entity payload."""

from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any, cast

from agent_framework import AgentSession, SessionStore, register_state_type

from ._shared_agent_state import (
    DurableAgentStateData,
    _json_snapshot,  # pyright: ignore[reportPrivateUsage]
)

logger = logging.getLogger("agent_framework.durabletask")

try:
    from agent_framework._serialization import SerializationMixin

    _SerializableStateRoot: type | None = SerializationMixin
except ImportError:  # pragma: no cover - depends on the installed core version
    _SerializableStateRoot = None

_registered_state_types: set[type] = set()


def _register_loaded_state_types() -> None:
    if _SerializableStateRoot is None:
        return
    seen: set[type] = set()
    pending: list[type] = [_SerializableStateRoot]
    while pending:
        for subclass in pending.pop().__subclasses__():
            if subclass in seen:
                continue
            seen.add(subclass)
            pending.append(subclass)
            if subclass in _registered_state_types:
                continue
            _registered_state_types.add(subclass)
            try:
                register_state_type(subclass)
            except Exception:
                logger.debug("Could not register session state type %s", subclass, exc_info=True)


class EntitySessionStore(SessionStore):
    """Adapt one entity operation's session slot to Core's async store contract.

    This private adapter neither commits nor maintains a second cache. Construct
    it against the current staged data, never retain it across entity operations.
    The key scopes access to this slot, independently of a snapshot's logical ID.
    Provider instances, transcript ownership and completion receipts are not stored
    here. The entity retains responsibility for migration, reset and final commit.
    """

    def __init__(
        self,
        data: DurableAgentStateData,
        session_id: str,
        *,
        transient_history_source: str | None = None,
    ) -> None:
        # Do not initialize SessionStore's unrelated in-memory dictionary.
        SessionStore.validate_session_id(session_id)
        self._data = data
        self._session_id = session_id
        self._transient_history_source = transient_history_source

    def _check_key(self, session_id: str) -> None:
        SessionStore.validate_session_id(session_id)
        if session_id != self._session_id:
            raise ValueError("The session store is scoped to a different durable entity session.")

    async def get(self, session_id: str) -> AgentSession | None:
        """Restore an independent working snapshot, without reading another backend."""
        self._check_key(session_id)
        stored = self._data.session
        if not stored or "session_id" not in stored:
            return None
        _register_loaded_state_types()
        return AgentSession.from_dict(_json_snapshot(stored))

    async def set(self, session_id: str, session: AgentSession) -> None:
        """Stage a JSON-compatible snapshot without persisting the enclosing entity."""
        self._check_key(session_id)
        to_dict = getattr(session, "to_dict", None)
        if not callable(to_dict):
            return

        source = self._transient_history_source
        session_state = getattr(session, "state", None)
        transient: Any = None
        has_transient = False
        if source is not None and isinstance(session_state, dict):
            bag = cast("dict[str, Any]", session_state)
            if source in bag:
                transient = bag.pop(source)
                has_transient = True
                if isinstance(transient, dict):
                    persistent = {
                        key: value
                        for key, value in cast("dict[str, Any]", transient).items()
                        if key not in ("messages", "_positions")
                    }
                    if persistent:
                        bag[source] = persistent
        try:
            payload = cast("dict[str, Any]", to_dict())
        finally:
            if has_transient:
                cast("dict[str, Any]", session_state)[cast(str, source)] = transient

        try:
            # Validate before copying so tuples/non-string keys are not silently
            # normalized. JSON serializers need not implement deepcopy.
            payload = _json_snapshot(payload)
        except (TypeError, ValueError) as exc:
            raise ValueError("Agent session state is not JSON-compatible; the operation cannot commit.") from exc
        previous = self._data.session
        if isinstance(previous, dict):
            opaque = {
                key: deepcopy(value)
                for key, value in previous.items()
                if key not in {"type", "session_id", "service_session_id", "state"} and key not in payload
            }
            payload = {**opaque, **payload}
        self._data.session = payload

    async def delete(self, session_id: str) -> None:
        """Stage removal of the session only, never transcript, results or receipts."""
        self._check_key(session_id)
        self._data.session = None
