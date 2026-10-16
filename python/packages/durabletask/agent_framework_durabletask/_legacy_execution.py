# Copyright (c) Microsoft. All rights reserved.

"""Legacy wire persistence with current Core provider/session integration."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Literal, cast
from uuid import uuid4

from agent_framework import (
    AgentResponse,
    AgentSession,
    HistoryProvider,
    InMemoryHistoryProvider,
    Message,
    SupportsAgentRun,
)

from ._history_provider import (
    DurableHistoryProvider,
    _copy_with_history_providers,  # pyright: ignore[reportPrivateUsage]
    _DurableCompactionProvider,  # pyright: ignore[reportPrivateUsage]
    _ObservedHistoryProvider,  # pyright: ignore[reportPrivateUsage]
    ensure_durable_history,
    prepare_history_owner,
    service_stores_history,
)
from ._legacy_delivery import LEGACY_DELIVERY_FIELD as _DELIVERIES
from ._legacy_delivery import read_legacy_deliveries
from ._legacy_history import LegacyMemoryHistory
from ._message_identity import message_identity
from ._models import RunRequest
from ._response_utils import serialize_input_message
from ._session_store import EntitySessionStore
from ._shared_agent_state import (
    DurableAgentState,
    DurableAgentStateData,
    DurableAgentStateEntry,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
)
from ._shared_response import serialize_terminal_response
from ._state_migration import (
    _apply_journal,  # pyright: ignore[reportPrivateUsage]
    _journal_receipts,  # pyright: ignore[reportPrivateUsage]
    state_snapshot_digest,
)

_LEDGER = "_dafx_legacy_ingestion"
_OWNER = "_dafx_legacy_owner"
_PUBLIC_LEDGER = "_dafx_legacy_public_ingestion"


@dataclass(frozen=True)
class LegacyHistoryView:
    """One operation's pipeline; registered providers are never replaced in place."""

    agent: SupportsAgentRun
    owner: Literal["entity", "external", "service"]
    primary_owner: Literal["entity", "external"]
    source_id: str | None
    memory_sources: tuple[str, ...]


@dataclass
class _LegacyHistoryWrite:
    inputs: list[Message | DurableAgentStateMessage]
    outputs: list[Message | DurableAgentStateMessage]
    response: DurableAgentStateResponse | None


class LegacyInputAcceptance:
    """Observe request-local inputs without binding a v2 state writer to legacy state."""

    def __init__(self, state: DurableAgentState, correlation_id: str) -> None:
        self._previous = legacy_ingestion(state) or {}
        self._correlation_id = correlation_id
        self._scope = uuid4().hex
        self._accepted: set[int] = set()
        self._accepted_messages: dict[int, Message] = {}
        self._saved: list[_LegacyHistoryWrite] = []
        self.memory_history: LegacyMemoryHistory | None = None

    def mark(self, message: Message, index: int) -> None:
        message._dafx_legacy_input = (self._scope, index)  # type: ignore[attr-defined]

    def accept(self, messages: Sequence[Message]) -> None:
        for message in messages:
            marker = getattr(message, "_dafx_legacy_input", None)
            if isinstance(marker, tuple):
                values = cast("tuple[Any, ...]", marker)
                if len(values) == 2 and values[0] == self._scope and type(values[1]) is int:
                    self._accepted.add(values[1])
                    self._accepted_messages[values[1]] = message

    def record(self, messages: Sequence[Message], response: AgentResponse | None, live_memory: bool) -> None:
        if live_memory and self.memory_history is not None:
            self.memory_history.capture_write(list(messages))
        outputs = {id(message) for message in response.messages} if response is not None else set[int]()
        inputs: list[Message | DurableAgentStateMessage] = []
        replies: list[Message | DurableAgentStateMessage] = []
        for message in messages:
            # Memory bodies remain live until the enclosing session snapshot, not an earlier save hook.
            captured = (
                message if live_memory else DurableAgentStateMessage.from_core_dict(serialize_input_message(message))
            )
            (replies if id(message) in outputs else inputs).append(captured)
        metadata = (
            DurableAgentStateResponse.from_run_response(
                self._correlation_id,
                AgentResponse(
                    messages=[],
                    created_at=response.created_at,
                    usage_details=deepcopy(response.usage_details),
                    additional_properties=deepcopy(response.additional_properties),
                ),
            )
            if response is not None
            else None
        )
        self._saved.append(_LegacyHistoryWrite(inputs, replies, metadata))

    def observe_compaction(self, messages: Sequence[Message], within_strategy: bool) -> None:
        if self.memory_history is not None:
            self.memory_history.observe_compaction(list(messages), within_strategy)

    def reconcile(self, state: DurableAgentState, entry: DurableAgentStateRequest) -> None:
        known = deepcopy(self._previous)
        for index, stored in enumerate(entry.messages):
            if index not in self._accepted:
                continue
            identity, fingerprint = stored.ingestion_occurrence, stored.ingestion_identity
            if identity is not None and fingerprint is not None:
                prior = known.get(identity, [])
                if prior is not None and fingerprint not in prior:
                    known[identity] = [*prior, fingerprint]
        profile = _legacy_ingestion_profile(state)
        if profile is None:
            raise ValueError("Legacy input acceptance requires a staged ingestion profile.")
        profile["messages"] = known

    def replay(
        self,
        state: DurableAgentState,
        entry: DurableAgentStateRequest,
        response: AgentResponse,
        *,
        owner: Literal["entity", "external", "service"],
        uses_pipeline: bool,
    ) -> DurableAgentStateResponse:
        """Project successful owner writes, independently of ingress receipts and delivery."""
        correlation_id = entry.correlation_id
        if correlation_id is None:
            raise ValueError("Legacy replay persistence requires an operation correlation ID.")
        terminal = DurableAgentStateResponse.from_run_response(correlation_id, response)
        if not uses_pipeline:
            return terminal
        if owner == "service":
            entry.messages = [
                DurableAgentStateMessage.from_chat_message(message)
                for _, message in sorted(self._accepted_messages.items())
            ]
            return terminal
        records: list[DurableAgentStateEntry] = []

        def stored(message: Message | DurableAgentStateMessage) -> DurableAgentStateMessage:
            if not isinstance(message, Message):
                return deepcopy(message)
            snapshot = DurableAgentStateMessage.from_chat_message(message)
            if self.memory_history is not None:
                snapshot = self.memory_history.link(message, snapshot)
            return snapshot

        for write in self._saved:
            if write.inputs:
                records.append(
                    DurableAgentStateRequest(
                        correlation_id,
                        entry.created_at,
                        [stored(message) for message in write.inputs],
                    )
                )
            if write.outputs and write.response is not None:
                saved = deepcopy(write.response)
                saved.messages = [stored(message) for message in write.outputs]
                records.append(saved)
        terminal.messages = []
        if records and isinstance(records[-1], DurableAgentStateResponse):
            terminal.messages = records.pop().messages
        index = next(index for index, previous in enumerate(state.data.conversation_history) if previous is entry)
        state.data.conversation_history[index : index + 1] = records
        return terminal


def prepare_legacy_history(
    agent: SupportsAgentRun,
    options: dict[str, Any],
    accept_inputs: Callable[[Sequence[Message]], None] | None = None,
    record_history: Callable[[Sequence[Message], AgentResponse | None, bool], None] | None = None,
    observe_compaction: Callable[[Sequence[Message], bool], None] | None = None,
) -> LegacyHistoryView:
    """Use exactly one history owner while retaining ordinary audit/context hooks."""
    service = service_stores_history(agent, options)
    providers = getattr(agent, "context_providers", ())
    providers = cast("Sequence[Any]", providers) if isinstance(providers, (list, tuple)) else ()
    primary = next((p for p in providers if isinstance(p, HistoryProvider) and p.load_messages), None)
    owner: Literal["entity", "external", "service"] = (
        "service"
        if service
        else "external"
        if primary is not None
        and type(primary) is not InMemoryHistoryProvider
        and not isinstance(primary, DurableHistoryProvider)
        else "entity"
    )
    prepared = ensure_durable_history(agent)
    pipeline = getattr(prepared, "context_providers", ())
    source_id = (
        next(
            (
                p.source_id
                for p in cast("Sequence[Any]", pipeline)
                if isinstance(p, HistoryProvider) and p.load_messages
            ),
            None,
        )
        if isinstance(pipeline, (list, tuple))
        else None
    )
    prepared = prepare_history_owner(prepared, service)
    prepared_providers = getattr(prepared, "context_providers", None)
    if not isinstance(prepared_providers, (list, tuple)):
        return LegacyHistoryView(prepared, owner, "external" if owner == "external" else "entity", None, ())
    updated: list[Any] = []
    memory: list[str] = []
    for provider in cast("Sequence[Any]", prepared_providers):
        if isinstance(provider, DurableHistoryProvider):
            original = provider
            memory.append(provider.source_id)
            provider = InMemoryHistoryProvider(
                source_id=provider.source_id,
                load_messages=provider.load_messages and not service,
                store_inputs=provider.store_inputs,
                store_outputs=provider.store_outputs,
                store_context_messages=provider.store_context_messages,
                store_context_from=provider.store_context_from,
                skip_excluded=provider.skip_excluded,
            )
            if hasattr(original, "after_run_once_per_turn"):
                provider.after_run_once_per_turn = original.after_run_once_per_turn
        if (
            accept_inputs is not None
            and not service
            and isinstance(provider, HistoryProvider)
            and provider.load_messages
        ):
            provider = _ObservedHistoryProvider(provider, accept_inputs, record_history)
        elif (
            type(provider) is _DurableCompactionProvider
            and not service
            and provider.history_source_id == source_id
            and observe_compaction is not None
        ):
            provider = _DurableCompactionProvider(provider.__wrapped__, observe_compaction)
        updated.append(provider)
    prepared = _copy_with_history_providers(prepared, updated)
    primary_owner: Literal["entity", "external"] = (
        "external"
        if primary is not None
        and type(primary) is not InMemoryHistoryProvider
        and not isinstance(primary, DurableHistoryProvider)
        else "entity"
    )
    return LegacyHistoryView(prepared, owner, primary_owner, source_id, tuple(memory))


def bind_legacy_owner(state: DurableAgentState, view: LegacyHistoryView) -> None:
    """Reject a changed known owner without interpreting arbitrary session keys."""
    extensions = state.data.extension_data
    if extensions is None:
        extensions = {}
        state.data.extension_data = extensions
    selected = {"profile": "dafx-legacy-owner-v1", "owner": view.primary_owner, "sourceId": view.source_id}
    previous = extensions.get(_OWNER)
    if _OWNER in extensions and previous != selected:
        raise ValueError("Legacy history ownership does not match the committed provider binding.")
    extensions[_OWNER] = selected


def legacy_session_identity(source: dict[str, Any], default: str) -> str:
    """Retain the logical identity of a source provider independently of its entity address."""
    session = source["data"].get("session")
    if session is None:
        return default
    if not isinstance(session, dict):
        raise ValueError("Legacy session continuation must be an object.")
    session = cast(dict[str, Any], session)
    if not isinstance(session.get("state", {}), dict):
        raise ValueError("Legacy session continuation must contain an object-valued state.")
    identity = session.get("session_id")
    if identity is not None and (not isinstance(identity, str) or not identity.strip()):
        raise ValueError("Legacy session continuation has an invalid session_id.")
    return identity or default


def _legacy_ingestion_profile(state: DurableAgentState) -> dict[str, Any] | None:
    extensions = state.data.extension_data
    if extensions is None or _LEDGER not in extensions:
        return None
    profile = extensions[_LEDGER]
    if not isinstance(profile, dict):
        raise ValueError("Legacy ingestion metadata has an unsupported profile.")
    profile = cast(dict[str, Any], profile)
    if profile.get("profile") not in ("dafx-legacy-ingestion-v1", "dafx-legacy-ingestion-v2"):
        raise ValueError("Legacy ingestion metadata has an unsupported profile.")
    if not isinstance(profile.get("messages"), dict):
        raise ValueError("Legacy ingestion metadata requires a messages object.")
    return profile


def legacy_ingestion(state: DurableAgentState) -> dict[str, list[str] | None] | None:
    """Read only declared occurrence acceptances, never guess from cursor maxima."""
    profile = _legacy_ingestion_profile(state)
    if profile is None:
        return None
    validation = DurableAgentState()
    validated = cast(dict[str, list[str] | None], profile["messages"])
    validation.data.ingested_messages = deepcopy(validated)
    validation.to_dict()
    return deepcopy(validated)


def _public_receipts(messages: Any) -> dict[str, list[str]]:
    if not isinstance(messages, dict):
        raise ValueError("Public ingestion metadata requires a messages object.")
    messages = cast(dict[str, list[str]], messages)
    if any(not isinstance(fingerprints, list) for fingerprints in messages.values()):
        raise ValueError("Public ingestion receipts require exact content fingerprints.")
    validation = DurableAgentState()
    validation.data.ingested_messages.update(deepcopy(messages))
    validation.to_dict()
    return deepcopy(messages)


def legacy_public_ingestion(state: DurableAgentState) -> dict[str, list[str]]:
    """Keep recorded public lookups separate from the bridge occurrence ledger."""
    profile = _legacy_ingestion_profile(state)
    if profile is None or profile["profile"] != "dafx-legacy-ingestion-v2":
        return {}
    return _public_receipts(profile.get("publicMessages", {}))


def migrated_public_ingestion(state: DurableAgentState) -> dict[str, list[str]]:
    """Read public lookup receipts only from the declared in-place migration profile."""
    binding = state.data.unknown_fields.get("migration")
    if not isinstance(binding, dict) or cast(dict[str, Any], binding).get("mode") != "in_place":
        return {}
    extensions = state.data.extension_data or {}
    if _PUBLIC_LEDGER not in extensions:
        return {}
    profile = extensions[_PUBLIC_LEDGER]
    if not isinstance(profile, dict):
        raise ValueError("Migrated public ingestion metadata has an unsupported profile.")
    profile = cast(dict[str, Any], profile)
    if (
        profile.keys() != {"profile", "messages"}
        or profile.get("profile") != "dafx-legacy-public-ingestion-v1"
        or not isinstance(profile.get("messages"), dict)
    ):
        raise ValueError("Migrated public ingestion metadata has an unsupported profile.")
    return _public_receipts(profile["messages"])


def bind_migrated_public_ingestion(state: DurableAgentState, journal: dict[str, list[str]]) -> None:
    """Keep public journal lookups separate from explicit occurrence acceptances."""
    extensions = state.data.extension_data or {}
    if _PUBLIC_LEDGER in extensions:
        raise ValueError("Legacy state contains reserved migrated public ingestion metadata.")
    if journal:
        extensions[_PUBLIC_LEDGER] = {"profile": "dafx-legacy-public-ingestion-v1", "messages": deepcopy(journal)}
        state.data.extension_data = extensions


def legacy_delivery_snapshots(state: DurableAgentState) -> dict[str, dict[str, Any]]:
    """Validate only declared, newly recorded full responses, never infer old outcomes."""
    return read_legacy_deliveries(state.data.extension_data)


def record_legacy_delivery(state: DurableAgentState, correlation_id: str, snapshot: dict[str, Any]) -> None:
    """Stage a complete response in backward-readable entity extension metadata."""
    responses = legacy_delivery_snapshots(state)
    serialize_terminal_response(snapshot)
    responses[correlation_id] = deepcopy(snapshot)
    extensions = state.data.extension_data
    if extensions is None:
        extensions = {}
        state.data.extension_data = extensions
    previous = cast(dict[str, Any], extensions.get(_DELIVERIES, {}))
    extensions[_DELIVERIES] = {
        **previous,
        "profile": "dafx-legacy-delivery-v1",
        "responses": responses,
    }


def clear_legacy_deliveries(state: DurableAgentState) -> None:
    """Reset known delivery results with the transcript while preserving opaque siblings."""
    legacy_delivery_snapshots(state)
    extensions = state.data.extension_data or {}
    if _DELIVERIES in extensions:
        profile = cast(dict[str, Any], extensions[_DELIVERIES])
        extensions[_DELIVERIES] = {**profile, "responses": {}}


def stage_legacy_request(
    state: DurableAgentState,
    request: RunRequest,
    source: dict[str, Any],
    delivery_evidence: dict[str, Any] | None,
) -> DurableAgentStateRequest:
    """Stage occurrence-aware context without manufacturing scalar cursor provenance."""
    entry = DurableAgentStateRequest.from_run_request(request)
    if request.context_messages is None and delivery_evidence is None:
        return entry
    profile = _legacy_ingestion_profile(state) or {}
    known = legacy_ingestion(state)
    public = legacy_public_ingestion(state)
    positions = state.data.ingested_positions or {}
    if delivery_evidence is not None:
        _, journal = _journal_receipts(
            delivery_evidence, source_digest=state_snapshot_digest(source), positions=positions
        )
        _apply_journal(DurableAgentState.from_dict(source), journal)
        if any(
            fingerprint not in journal.get(identity, [])
            for identity, values in public.items()
            for fingerprint in values
        ):
            raise ValueError("Recorded delivery evidence omits committed public lookup fingerprints.")
        public = journal
    elif known is None and positions:
        raise ValueError("Legacy ingestion positions require trusted delivery evidence before context delivery.")
    if public and profile.get("profile") == "dafx-legacy-ingestion-v1" and "publicMessages" in profile:
        raise ValueError("Legacy ingestion profile promotion would overwrite opaque publicMessages metadata.")
    known = known or {}
    kept: list[DurableAgentStateMessage] = []
    for index, stored in enumerate(entry.messages):
        if request.context_messages is None:
            kept.append(stored)
            continue
        identity = request.context_message_ids[index] if request.context_message_ids is not None else stored.message_id
        if not identity:
            kept.append(stored)
            continue
        fingerprint = stored.ingestion_identity or message_identity(stored.to_chat_message())
        if request.context_message_ids is None and fingerprint in public.get(stored.message_id or "", []):
            continue
        prior = known.get(identity, [])
        if prior is None or fingerprint in prior:
            continue
        known[identity] = [*prior, fingerprint]
        stored.ingestion_occurrence = identity
        stored.ingestion_identity = fingerprint
        kept.append(stored)
    entry.messages = kept
    extensions = state.data.extension_data
    if extensions is None:
        extensions = {}
        state.data.extension_data = extensions
    extensions[_LEDGER] = {
        **profile,
        "profile": "dafx-legacy-ingestion-v2" if public else profile.get("profile", "dafx-legacy-ingestion-v1"),
        "messages": known,
        **({"publicMessages": public} if public else {}),
    }
    return entry


async def restore_legacy_session(
    data: DurableAgentStateData,
    scope: str,
    identity: str,
) -> AgentSession | None:
    """Normalize the resolved identity only on a detached restoration snapshot."""
    if data.session is None:
        return None
    snapshot = deepcopy(data.session)
    snapshot["session_id"] = identity
    restored = DurableAgentStateData(session=snapshot)
    return await EntitySessionStore(restored, scope).get(scope)


def seed_legacy_memory(view: LegacyHistoryView, session: Any, prior: list[Message]) -> None:
    """Seed only the entity-owned memory slot; external/service history stays authoritative."""
    for source_id in view.memory_sources:
        bag = session.state.setdefault(source_id, {})
        if not isinstance(bag, dict):
            raise ValueError("Legacy memory provider state must be an object.")
        bag = cast(dict[str, Any], bag)
        bag.pop("messages", None)
        bag.pop("_positions", None)
        if view.owner == "entity" and source_id == view.source_id:
            bag["messages"] = deepcopy(prior)


async def save_legacy_session(
    store: EntitySessionStore, scope: str, session: AgentSession, sources: tuple[str, ...]
) -> None:
    """Snapshot provider continuation without duplicating transient entity history."""
    originals: dict[str, Any] = {}
    for source in sources:
        if source in session.state:
            original = session.state[source]
            originals[source] = original
            if not isinstance(original, dict):
                raise ValueError("Legacy memory provider state must be an object.")
            session.state[source] = {
                key: value
                for key, value in cast(dict[str, Any], original).items()
                if key not in ("messages", "_positions")
            }
    try:
        await store.set(scope, session)
    finally:
        session.state.update(originals)
