# Copyright (c) Microsoft. All rights reserved.

"""Legacy continuation and evidence-backed migration; not a fleet-isolation protocol."""

from __future__ import annotations

import inspect
import json
import logging
from collections.abc import Callable, Mapping, Sequence
from copy import copy, deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from agent_framework import (
    Agent,
    AgentResponse,
    CompactionProvider,
    Content,
    Message,
    SupportsAgentRun,
)

from ._callbacks import AgentResponseCallbackProtocol
from ._configuration import resolve_runtime_deployment
from ._constants import DELIVERY_WINDOW_SECONDS
from ._entities import (
    AgentEntity,
    AgentEntityStateProviderMixin,
    _validation_diagnostic,  # pyright: ignore[reportPrivateUsage]
)
from ._history_provider import ensure_durable_history
from ._invocation_safety import DurableServiceClient, DurableToolGuard, InvocationProgress
from ._legacy_execution import (
    LegacyInputAcceptance,
    bind_legacy_owner,
    bind_migrated_public_ingestion,
    clear_legacy_deliveries,
    legacy_delivery_snapshots,
    legacy_ingestion,
    legacy_public_ingestion,
    legacy_session_identity,
    migrated_public_ingestion,
    prepare_legacy_history,
    record_legacy_delivery,
    restore_legacy_session,
    save_legacy_session,
    seed_legacy_memory,
    stage_legacy_request,
)
from ._legacy_history import (
    LegacyMemoryHistory,
    clear_legacy_history,
    legacy_history_profile,
    ordered_legacy_history,
)
from ._message_identity import message_identity
from ._models import RunRequest
from ._response_utils import serialize_agent_response
from ._retention import (
    DEFAULT_MAX_STATE_BYTES,
    DEFAULT_RETENTION,
    HIGH_WATERMARK,
    LOW_WATERMARK,
    RetentionMode,
    StateBudget,
    prunes_excluded,
)
from ._session_store import EntitySessionStore
from ._shared_agent_state import DurableAgentState, DurableAgentStateMessage, DurableAgentStateResponse
from ._shared_response import serialize_terminal_response
from ._shared_state_validation import validate_identifier
from ._state_capacity import StateCapacityError
from ._state_migration import (
    _apply_journal,  # pyright: ignore[reportPrivateUsage]
    _completion_journal,  # pyright: ignore[reportPrivateUsage]
    _journal_receipts,  # pyright: ignore[reportPrivateUsage]
    _validate_retained_completions,  # pyright: ignore[reportPrivateUsage]
    migrate_legacy_state,
    state_snapshot_digest,
)

logger = logging.getLogger("agent_framework.durabletask")


@dataclass(frozen=True)
class LegacyMigrationEvidence:
    """Trusted operator evidence, never inferred from transcript entries."""

    completion_evidence: dict[str, Any] | None = None
    delivery_evidence: dict[str, Any] | None = None


LegacyMigrationEvidenceProvider = Callable[[dict[str, Any]], LegacyMigrationEvidence | None]


class _CompatibilityStateProvider(AgentEntityStateProviderMixin):
    """Confine prototype raw-state access to the existing provider extension boundary."""

    def __init__(self, parent: AgentEntityStateProviderMixin) -> None:
        self.parent = parent

    def snapshot(self) -> dict[str, Any]:
        if self.parent._write_acknowledgement_uncertain:
            self.parent.ensure_v2_writable()
        source = deepcopy(self._get_state_dict())
        committed = self.parent._persisted_state_snapshot
        if committed is not None and state_snapshot_digest(source) != state_snapshot_digest(committed):
            raise ValueError("Entity backing state changed after its continuation was loaded.")
        return source

    def invalidate(self) -> None:
        self.parent._state_cache = None
        self.parent._persisted_state_snapshot = None

    def commit_legacy(self, source: dict[str, Any], state: DurableAgentState) -> None:
        if state_snapshot_digest(self._get_state_dict()) != state_snapshot_digest(source):
            raise ValueError("Entity state changed while staging legacy execution.")
        self._set_state_dict(state.to_dict())

    def _get_state_dict(self) -> dict[str, Any]:
        return self.parent._read_raw_state_dict()

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        self.parent._set_state_dict(state)

    def _get_session_id_from_entity(self) -> str:
        return self.parent._get_session_id_from_entity()

    def _get_entity_name_from_entity(self) -> str:
        return self.parent._get_entity_name_from_entity()


class _MigrationStateProvider(_CompatibilityStateProvider):
    """Stage conversion and the v2 turn through one underlying entity state setter."""

    def __init__(
        self, parent: AgentEntityStateProviderMixin, source: dict[str, Any], converted: dict[str, Any]
    ) -> None:
        super().__init__(parent)
        self.source = deepcopy(source)
        self.converted = deepcopy(converted)
        self.committed: dict[str, Any] | None = None

    def _get_state_dict(self) -> dict[str, Any]:
        actual = self.parent._read_raw_state_dict()
        expected = self.source if self.committed is None else self.committed
        if state_snapshot_digest(actual) != state_snapshot_digest(expected):
            raise ValueError("Entity state changed while staging automatic migration.")
        return deepcopy(self.converted if self.committed is None else self.committed)

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        self._get_state_dict()
        if state["data"].get("migration") != self.converted["data"]["migration"]:
            raise ValueError("Automatic migration provenance cannot change during its commit.")
        self.parent._set_state_dict(state)
        self.committed = deepcopy(state)


class CompatibleAgentEntity(AgentEntity):
    """Dispatch agent execution by persisted schema and the host's writer policy."""

    def __init__(
        self,
        agent: SupportsAgentRun,
        callback: AgentResponseCallbackProtocol | None = None,
        *,
        state_provider: AgentEntityStateProviderMixin,
        deployment_mode: str | None = None,
        legacy_migration_evidence: LegacyMigrationEvidenceProvider | None = None,
        retention: RetentionMode = DEFAULT_RETENTION,
        max_state_bytes: StateBudget = DEFAULT_MAX_STATE_BYTES,
        high_watermark: float = HIGH_WATERMARK,
        low_watermark: float = LOW_WATERMARK,
        response_delivery_window_seconds: int = DELIVERY_WINDOW_SECONDS,
    ) -> None:
        super().__init__(
            agent,
            callback,
            state_provider=state_provider,
            prepare_history=False,
            retention=retention,
            max_state_bytes=max_state_bytes,
            high_watermark=high_watermark,
            low_watermark=low_watermark,
            response_delivery_window_seconds=response_delivery_window_seconds,
        )
        self._registered_agent = self.agent
        self._storage = _CompatibilityStateProvider(state_provider)
        self._deployment_mode = resolve_runtime_deployment(deployment_mode)
        if self._deployment_mode == "legacy" and retention != "keep_all":
            raise ValueError("The legacy compatibility spike requires retention='keep_all'.")
        if legacy_migration_evidence is not None and not callable(legacy_migration_evidence):
            raise TypeError("legacy_migration_evidence must be callable or None.")
        self._legacy_migration_evidence = legacy_migration_evidence
        self._legacy_state: DurableAgentState | None = None

    @property
    def state(self) -> DurableAgentState:
        if self._legacy_state is not None:
            return self._legacy_state
        return super().state

    @state.setter
    def state(self, value: DurableAgentState) -> None:
        self._state_provider.state = value

    def _prepare_v2(self) -> None:
        self.agent = ensure_durable_history(self._registered_agent, prune_excluded=prunes_excluded(self._retention))

    def _snapshot(self) -> dict[str, Any]:
        source = self._storage.snapshot()
        if source:
            version = source.get("schemaVersion")
            if not isinstance(version, str) or version not in ("1.0.0", "1.1.0", "1.2.0", "2.0.0"):
                raise ValueError("Durable entity state schemaVersion is missing or unsupported.")
            state = DurableAgentState.from_dict(source)
            if version == "2.0.0":
                migrated_public_ingestion(state)
        return source

    def _validate_legacy_profile(self, source: dict[str, Any], options: dict[str, Any]) -> None:
        data = source["data"]
        if any(name in data for name in ("historyBinding", "pythonIngestion", "terminalResults", "completionReceipts")):
            raise ValueError("Legacy state contains schema-2 control fields.")
        legacy_session_identity(source, self._state_provider.core_session_id)
        state = DurableAgentState.from_dict(source)
        legacy_ingestion(state)
        legacy_public_ingestion(state)
        legacy_delivery_snapshots(state)
        legacy_history_profile(state)
        # Prepare a detached view before side effects to reject ambiguous providers.
        bind_legacy_owner(state, prepare_legacy_history(self._registered_agent, options))

    def _check_legacy_capacity(self, state: DurableAgentState) -> None:
        if self._max_state_bytes is not None:
            size = len(json.dumps(state.to_dict(), allow_nan=False).encode("utf-8"))
            if size > self._max_state_bytes:
                raise StateCapacityError(
                    size_bytes=size,
                    max_state_bytes=self._max_state_bytes,
                    floor_bytes=size,
                    target_bytes=self._max_state_bytes,
                )

    def _commit_legacy(self, source: dict[str, Any], state: DurableAgentState) -> None:
        self._check_legacy_capacity(state)
        self._storage.commit_legacy(source, state)

    def _resolve_legacy_evidence(self, source: dict[str, Any]) -> LegacyMigrationEvidence | None:
        evidence = (
            self._legacy_migration_evidence(deepcopy(source)) if self._legacy_migration_evidence is not None else None
        )
        if evidence is not None and not isinstance(evidence, LegacyMigrationEvidence):
            raise TypeError("legacy_migration_evidence must return LegacyMigrationEvidence or None.")
        return evidence

    def _converted(self, source: dict[str, Any], evidence: LegacyMigrationEvidence | None) -> DurableAgentState | None:
        data = source["data"]
        legacy_state = DurableAgentState.from_dict(source)
        bridge_receipts = legacy_ingestion(legacy_state) or {}
        public_receipts = legacy_public_ingestion(legacy_state)
        deliveries = legacy_delivery_snapshots(legacy_state)
        used = bool(data.get("conversationHistory") or data.get("session") or data.get("ingestedPositions"))
        used = used or "truncation" in data or bool(bridge_receipts) or bool(public_receipts) or bool(deliveries)
        digest = state_snapshot_digest(source)
        completion = evidence.completion_evidence if evidence is not None else None
        delivery = evidence.delivery_evidence if evidence is not None else None
        positions = legacy_state.data.ingested_positions or {}
        journal: dict[str, list[str]] = {}
        if delivery is not None:
            _, journal = _journal_receipts(delivery, source_digest=digest, positions=positions)
            _apply_journal(legacy_state, journal)
            if any(
                fingerprint not in journal.get(identity, [])
                for identity, fingerprints in public_receipts.items()
                for fingerprint in fingerprints
            ):
                raise ValueError("Recorded delivery evidence omits committed public lookup fingerprints.")
            public_receipts = journal
        if completion is not None:
            expires_at = (
                datetime.now(timezone.utc) + timedelta(seconds=self._response_delivery_window_seconds)
            ).isoformat()
            _, results, _ = _completion_journal(completion, source_digest=digest, expires_at=expires_at)
            _validate_retained_completions(data.get("conversationHistory", []), results)
            for correlation_id, snapshot in deliveries.items():
                result = results.get(correlation_id)
                original = serialize_terminal_response(snapshot)
                if result is None or state_snapshot_digest(result["response"]) != state_snapshot_digest(original):
                    raise ValueError("Completion evidence must preserve every known full legacy delivery snapshot.")
        if ((used or journal) and completion is None) or (positions and delivery is None):
            logger.info("Automatic migration deferred: required legacy migration evidence is unavailable.")
            return None
        if "prototypeMigration" in data:
            raise ValueError("Legacy state contains reserved prototype migration provenance.")
        converted = migrate_legacy_state(
            source,
            source_digest=digest,
            source_session_id=legacy_session_identity(source, self._state_provider.core_session_id),
            migration_id=f"prototype:{digest}",
            ownership_transfer_id=f"prototype-in-place:{digest}",
            delivery_window_seconds=self._response_delivery_window_seconds,
            completion_evidence=evidence.completion_evidence if evidence is not None else None,
            delivery_evidence=evidence.delivery_evidence if evidence is not None else None,
        )
        binding = converted.data.unknown_fields["migration"]
        scope = self._state_provider.core_session_id
        binding.update({
            "mode": "in_place",
            "sourceEntitySessionId": scope,
            "destinationSessionId": scope,
            "sourceSchema": source["schemaVersion"],
            "requestDigest": state_snapshot_digest({
                "sourceDigest": digest,
                "destination": scope,
                "completionEvidence": evidence.completion_evidence if evidence is not None else None,
                "deliveryEvidence": evidence.delivery_evidence if evidence is not None else None,
            }),
        })
        # Historical journals key public lookups, not delivery occurrences.
        # Only the declared bridge ledger establishes active occurrence receipts.
        converted.data.ingested_messages = deepcopy(bridge_receipts)
        bind_migrated_public_ingestion(converted, public_receipts)
        return converted

    def _drop_already_stored(
        self,
        messages: list[DurableAgentStateMessage],
        *,
        occurrence_ids: list[str] | None = None,
    ) -> list[DurableAgentStateMessage]:
        if occurrence_ids is None:
            public = migrated_public_ingestion(self.state)
            messages = [
                message
                for message in messages
                if (message.ingestion_identity or message_identity(message.to_chat_message()))
                not in public.get(message.message_id or "", [])
            ]
        return super()._drop_already_stored(messages, occurrence_ids=occurrence_ids)

    async def run(self, request: RunRequest | dict[str, Any] | str) -> AgentResponse:
        source = self._snapshot()
        if source and source["schemaVersion"] != "2.0.0":
            self._validate_legacy_profile(source, {})
        run_request = (
            RunRequest.from_json(request)
            if isinstance(request, str)
            else RunRequest.from_dict(request)
            if isinstance(request, dict)
            else request
        )
        run_request.validate_context()
        validate_identifier(run_request.correlation_id, "correlationId")
        if source and source["schemaVersion"] == "2.0.0":
            if self._deployment_mode == "legacy":
                completed = DurableAgentState.from_dict(source).try_get_agent_response(run_request.correlation_id)
                if completed is not None:
                    return completed
                raise ValueError("Legacy deployment mode cannot write existing schema-2 state.")
            self._prepare_v2()
            return await super().run(run_request)
        if not source and self._deployment_mode != "legacy":
            self._prepare_v2()
            return await super().run(run_request)
        legacy = source or DurableAgentState(schema_version="1.1.0").to_dict()
        self._validate_legacy_profile(legacy, run_request.options)
        if self._deployment_mode == "legacy":
            completed = DurableAgentState.from_dict(legacy).try_get_agent_response(run_request.correlation_id)
            if completed is not None:
                return completed
        evidence = (
            self._resolve_legacy_evidence(legacy)
            if (source and self._deployment_mode != "legacy") or run_request.context_messages is not None
            else None
        )
        converted = self._converted(legacy, evidence) if source and self._deployment_mode != "legacy" else None
        if converted is None:
            return await self._run_legacy(source, legacy, run_request, evidence)
        parent = self._state_provider
        self._state_provider = _MigrationStateProvider(parent, source, converted.to_dict())
        try:
            self._prepare_v2()
            return await super().run(run_request)
        finally:
            self._state_provider = parent
            self._storage.invalidate()

    async def _run_legacy(
        self,
        source: dict[str, Any],
        legacy: dict[str, Any],
        request: RunRequest,
        evidence: LegacyMigrationEvidence | None,
    ) -> AgentResponse:
        if self._retention != "keep_all":
            raise ValueError("Transcript compaction retention requires v2; this legacy session was not migrated.")
        working = DurableAgentState.from_dict(legacy)
        completed = working.try_get_agent_response(request.correlation_id)
        if completed is not None:
            return completed
        acceptance = LegacyInputAcceptance(working, request.correlation_id)
        view = prepare_legacy_history(
            self._registered_agent,
            request.options,
            acceptance.accept,
            acceptance.record,
            acceptance.observe_compaction,
        )
        self.agent = view.agent
        uses_pipeline = self._has_context_pipeline()
        providers = getattr(self.agent, "context_providers", ())
        if (
            view.owner == "entity"
            and uses_pipeline
            and (
                legacy_history_profile(working) is not None
                or any(
                    isinstance(provider, CompactionProvider)
                    and provider.after_strategy is not None
                    and provider.history_source_id == view.source_id
                    for provider in cast("Sequence[Any]", providers)
                )
            )
        ):
            acceptance.memory_history = LegacyMemoryHistory(working, request.correlation_id)
        original_extensions = deepcopy(working.data.extension_data)
        bind_legacy_owner(working, view)
        entry = stage_legacy_request(
            working,
            request,
            legacy,
            evidence.delivery_evidence if isinstance(evidence, LegacyMigrationEvidence) else None,
        )
        prior: list[Message] = []
        for reference, old, stored in ordered_legacy_history(working):
            if self._is_error_response(old):
                continue
            message = self._to_replayable_message(stored)
            if message is not None:
                if acceptance.memory_history is not None:
                    acceptance.memory_history.bind(reference, stored, message)
                prior.append(message)
        original_history = deepcopy(working.data.conversation_history)
        original_session = deepcopy(working.data.session)
        working.data.conversation_history.append(entry)
        self._check_legacy_capacity(working)
        self._legacy_state = working
        try:
            options = {**request.options}
            options.setdefault("response_format", request.response_format)
            if not request.enable_tool_calls:
                self._disable_tool_calls(options)
            current: list[Message] = []
            for index, stored in enumerate(entry.messages):
                message = self._to_current_message(stored, request)
                if message is not None:
                    acceptance.mark(message, index)
                    current.append(message)
            session: Any = None
            create = getattr(self.agent, "create_session", None)
            store = EntitySessionStore(working.data, self._state_provider.core_session_id)
            inactive_service_id: Any = None
            if callable(create):
                identity = legacy_session_identity(legacy, self._state_provider.core_session_id)
                session = create(session_id=identity)
                if inspect.isawaitable(session):
                    session = await session
                if not isinstance(getattr(session, "state", None), dict):
                    raise TypeError("Legacy create_session must return a session with object-valued state.")
                restored = await restore_legacy_session(working.data, self._state_provider.core_session_id, identity)
                if restored is not None:
                    session.state.update(restored.state)
                    session.service_session_id = restored.service_session_id
                inactive_service_id = session.service_session_id
                seed_legacy_memory(view, session, prior)
                if acceptance.memory_history is not None:
                    acceptance.memory_history.observe(session.state[view.source_id]["messages"], rebind_seed=True)
                if view.owner != "service":
                    session.service_session_id = None
                    defaults = getattr(self.agent, "default_options", None)
                    if isinstance(defaults, Mapping) and "conversation_id" in defaults:
                        invocation_agent = copy(self.agent)
                        invocation_agent.default_options = {  # type: ignore[attr-defined]
                            key: value
                            for key, value in cast("Mapping[str, Any]", defaults).items()
                            if key != "conversation_id"
                        }
                        self.agent = invocation_agent
                    options.pop("conversation_id", None)
            elif working.data.session or view.owner != "entity":
                raise TypeError("Legacy continuation requires an agent with create_session().")
            messages = current if session is not None and uses_pipeline else [*prior, *current]
            if view.owner == "service" and session is not None and not session.service_session_id:
                replay = prior
                if legacy_history_profile(working) is not None:
                    history = next(
                        provider
                        for provider in cast("Sequence[Any]", providers)
                        if getattr(provider, "source_id", None) == view.source_id
                    )
                    if history.skip_excluded:
                        replay = [message for message in prior if not message.additional_properties.get("_excluded")]
                messages = [*replay, *current]
            kwargs: dict[str, Any] = {"messages": messages, "options": options}
            if session is not None:
                kwargs["session"] = session
            progress = InvocationProgress()
            middleware_agent = self.agent
            if isinstance(middleware_agent, Agent):
                if view.owner == "service":
                    invocation_agent = copy(cast(Any, middleware_agent))
                    invocation_agent.client = DurableServiceClient(invocation_agent.client, acceptance.accept)
                    self.agent = invocation_agent
                guard = [DurableToolGuard(progress, enabled=request.enable_tool_calls)]
                kwargs.update({"middleware": guard, "client_kwargs": {"middleware": guard}})
            try:
                response = await self._invoke_agent(
                    kwargs, request.correlation_id, self._state_provider.session_id, request.message, progress
                )
                snapshot = serialize_agent_response(response)
                serialize_terminal_response(snapshot)
                if request.context_messages is not None and uses_pipeline:
                    acceptance.reconcile(working, entry)
                terminal = acceptance.replay(working, entry, response, owner=view.owner, uses_pipeline=uses_pipeline)
                working.data.conversation_history.append(terminal)
                if session is not None:
                    if acceptance.memory_history is not None:
                        state = session.state.get(view.source_id, {})
                        if not isinstance(state, dict):
                            raise ValueError("Legacy history compaction requires object-valued provider state.")
                        buffer = cast("dict[str, Any]", state).get("messages", [])
                        if not isinstance(buffer, list) or any(
                            not isinstance(item, Message) for item in cast("list[Any]", buffer)
                        ):
                            raise ValueError("Legacy history compaction requires a Core message buffer.")
                        acceptance.memory_history.flush(working, cast("list[Message]", buffer))
                    if view.owner != "service":
                        session.service_session_id = inactive_service_id
                    await save_legacy_session(store, self._state_provider.core_session_id, session, view.memory_sources)
            except Exception as exc:
                diagnostic = _validation_diagnostic(exc)
                if diagnostic is not None:
                    logger.error("Legacy agent execution failed. %s", diagnostic)
                else:
                    logger.exception("Legacy agent execution failed.")
                working.data.extension_data = original_extensions
                working.data.session = original_session
                if uses_pipeline or request.context_messages is not None:
                    working.data.conversation_history = original_history
                response = AgentResponse(
                    messages=[
                        Message(
                            "assistant",
                            [
                                Content.from_error(
                                    message=diagnostic or str(exc),
                                    error_code=type(exc).__name__,
                                )
                            ],
                        )
                    ],
                    created_at=datetime.now(timezone.utc).isoformat(),
                    additional_properties={"durable_status": "error", "correlation_id": request.correlation_id},
                )
                snapshot = serialize_agent_response(response)
                terminal = DurableAgentStateResponse.from_run_response(request.correlation_id, response)
                working.data.conversation_history.append(terminal)
            record_legacy_delivery(working, request.correlation_id, snapshot)
            self._commit_legacy(source, working)
            return response
        finally:
            self._legacy_state = None
            self._storage.invalidate()

    def reset(self) -> None:
        source = self._snapshot()
        if (source and source["schemaVersion"] != "2.0.0") or (not source and self._deployment_mode == "legacy"):
            legacy = source or DurableAgentState(schema_version="1.1.0").to_dict()
            self._validate_legacy_profile(legacy, {})
            working = DurableAgentState.from_dict(legacy)
            if prepare_legacy_history(self._registered_agent, {}).primary_owner == "external":
                raise NotImplementedError("Reset of external history requires a provider-owned clear operation.")
            clear_legacy_history(working)
            working.data.conversation_history.clear()
            working.data.session = None
            clear_legacy_deliveries(working)
            self._commit_legacy(source, working)
            self._storage.invalidate()
            return
        if self._deployment_mode == "legacy":
            raise ValueError("Legacy deployment mode cannot reset schema-2 state.")
        self._prepare_v2()
        super().reset()

    def expire_responses(self) -> int:
        source = self._snapshot()
        if not source or source["schemaVersion"] != "2.0.0":
            return 0
        if self._deployment_mode == "legacy":
            raise ValueError("Legacy deployment mode cannot expire schema-2 results.")
        return super().expire_responses()

    def migrate(self, request: dict[str, Any]) -> dict[str, str]:
        if self._deployment_mode == "legacy":
            raise ValueError("Legacy deployment mode cannot migrate state.")
        return super().migrate(request)
