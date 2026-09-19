# Copyright (c) Microsoft. All rights reserved.

"""Durable Task entity implementations for Microsoft Agent Framework."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import re
import warnings
from collections.abc import Mapping, Sequence
from copy import copy, deepcopy
from datetime import datetime, timezone
from typing import Any, cast

from agent_framework import (
    Agent,
    AgentResponse,
    AgentResponseUpdate,
    AgentSession,
    ChatResponse,
    Content,
    HistoryProvider,
    Message,
    ResponseStream,
    SupportsAgentRun,
    register_state_type,
)
from agent_framework._sessions import is_local_history_conversation_id
from durabletask.entities import DurableEntity

from ._callbacks import AgentCallbackContext, AgentResponseCallbackProtocol
from ._configuration import validate_response_delivery_window
from ._constants import DELIVERY_WINDOW_SECONDS, DurableStateFields
from ._delivery_state import validate_delivery_state
from ._history_provider import (
    DurableHistoryBinding,
    DurableHistoryProvider,
    bind_durable_history,
    ensure_durable_history,
    prepare_history_owner,
    service_stores_history,
    unbind_durable_history,
)
from ._invocation_safety import DurableServiceClient, DurableToolGuard, InvocationProgress
from ._message_identity import message_identity
from ._models import RunRequest
from ._response_utils import is_terminal_agent_response, load_agent_response, preserve_input_envelope
from ._retention import (
    DEFAULT_MAX_STATE_BYTES,
    DEFAULT_RETENTION,
    HIGH_WATERMARK,
    LOW_WATERMARK,
    RetentionMode,
    StateBudget,
    enforce_budget,
    prunes_excluded,
    resolve_state_budget,
    validate_retention,
)
from ._retention_telemetry import record_write, retention_operation
from ._shared_agent_state import (
    DurableAgentState,
    DurableAgentStateEntry,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
    DurableAgentStateUnknownEntry,
)
from ._shared_state_validation import validate_completion_transition, validate_identifier, validate_timestamp
from ._state_capacity import StateCapacityError
from ._state_migration import migrate_legacy_state, state_snapshot_digest

logger = logging.getLogger("agent_framework.durabletask")

_SESSION_ID_KEY = "session_id"
_DELIVERY_WINDOW_SECONDS = DELIVERY_WINDOW_SECONDS

try:
    from agent_framework._serialization import SerializationMixin

    _SerializableStateRoot: type | None = SerializationMixin
except ImportError:  # pragma: no cover - depends on the installed core version
    _SerializableStateRoot = None

_registered_state_types: set[type] = set()
_MISSING_PREVIOUS_RESPONSE_CODE = "previous_response_not_found"
_REJECTED_ID_RETRIES = 3
_REJECTED_ID_BACKOFF_SECONDS = 0.5


def _is_missing_previous_response(exc: BaseException, *, prior_error: BaseException | None = None) -> bool:
    """Return whether the service refused the conversation id from the previous turn."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        if current is prior_error:
            return False
        seen.add(id(current))
        code = getattr(current, "code", None)
        if code is not None:
            return code == _MISSING_PREVIOUS_RESPONSE_CODE
        body = getattr(current, "body", None)
        if isinstance(body, Mapping):
            details = cast("Mapping[str, Any]", body)
            if "code" in details:
                return details["code"] == _MISSING_PREVIOUS_RESPONSE_CODE
        current = current.__cause__ or current.__context__
    return False


def _retry_snapshot(session: Any, run_kwargs: dict[str, Any], provider_sources: set[str]) -> str | None:
    """Capture observable invocation state without retaining aliases to mutable continuations."""
    try:
        payload = deepcopy(session.to_dict())
        state = payload.get("state")
        if isinstance(state, dict):
            state = cast("dict[str, Any]", state)
            # Core lazily creates empty provider bags. Their creation alone is
            # scaffolding, not advancement. Other empty application values remain.
            for source in provider_sources:
                if state.get(source) == {}:
                    state.pop(source)
        inputs = [message.to_dict() for message in run_kwargs["messages"]]
        return json.dumps({"session": payload, "messages": inputs}, sort_keys=True, allow_nan=False)
    except (AttributeError, TypeError, ValueError, RecursionError):
        # A non-comparable custom state must never authorize a whole-run retry.
        return None


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


class AgentEntityStateProviderMixin:
    """Mixin implementing durable agent state caching + (de)serialization + persistence.

    Concrete classes must implement:
    - _get_state_dict(): fetch raw persisted state dict (default should be {})
    - _set_state_dict(): persist raw state dict
    - _get_session_id_from_entity(): fetch the session ID from the underlying context
    """

    _state_cache: DurableAgentState | None = None
    _persisted_state_snapshot: dict[str, Any] | None = None
    _write_acknowledgement_uncertain: bool = False

    def _get_state_dict(self) -> dict[str, Any]:
        raise NotImplementedError

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        raise NotImplementedError

    def _get_session_id_from_entity(self) -> str:
        # Subclasses written against the previous API may still override the old hook name.
        legacy_hook = getattr(self, "_get_thread_id_from_entity", None)
        if legacy_hook is not None:
            warnings.warn(
                f"{type(self).__name__}._get_thread_id_from_entity is deprecated; "
                "rename it to _get_session_id_from_entity.",
                DeprecationWarning,
                stacklevel=2,
            )
            return cast(str, legacy_hook())
        raise NotImplementedError

    def _get_entity_name_from_entity(self) -> str:
        return ""

    def _response_delivery_window_seconds(self) -> int:
        agent_entity = getattr(self, "_agent_entity", None)
        seconds = getattr(agent_entity, "_response_delivery_window_seconds", _DELIVERY_WINDOW_SECONDS)
        validate_response_delivery_window(seconds)
        return cast(int, seconds)

    def _read_raw_state_dict(self) -> dict[str, Any]:
        raw_state = self._get_state_dict()
        if not isinstance(raw_state, dict):
            raise ValueError("The durable agent state must be a JSON object.")
        return raw_state

    def _read_committed_snapshot(self) -> dict[str, Any]:
        snapshot = self._persisted_state_snapshot
        if snapshot is None:
            snapshot = deepcopy(self._read_raw_state_dict())
            self._persisted_state_snapshot = snapshot
        return deepcopy(snapshot)

    def _read_writable_state_dict(self) -> dict[str, Any]:
        raw_state = self._read_raw_state_dict()
        if raw_state == {}:
            return raw_state
        if raw_state.get(DurableStateFields.SCHEMA_VERSION) != DurableAgentState.SCHEMA_VERSION:
            raise ValueError(
                "Legacy state is read-only in this runtime. Keep it on its original deployment or use "
                "an isolated v2 entity instead."
            )
        validate_delivery_state(raw_state)
        return raw_state

    @staticmethod
    def _transition_validation_baseline(snapshot: dict[str, Any]) -> dict[str, Any]:
        return DurableAgentState().to_dict() if snapshot == {} else deepcopy(snapshot)

    def is_v2_writable(self) -> bool:
        raw_state = self._read_raw_state_dict()
        if raw_state == {}:
            return True
        try:
            self._read_writable_state_dict()
        except ValueError:
            return False
        return True

    def ensure_v2_writable(self) -> None:
        raw_state = self._read_writable_state_dict()
        if self._write_acknowledgement_uncertain:
            # A setter exception cannot prove that the backend rejected the write.
            # Refresh before another operation can execute or overwrite completions.
            self._state_cache = DurableAgentState() if raw_state == {} else DurableAgentState.from_dict(raw_state)
            self._persisted_state_snapshot = deepcopy(raw_state)
            self._write_acknowledgement_uncertain = False

    @property
    def session_id(self) -> str:
        return self._get_session_id_from_entity()

    @property
    def core_session_id(self) -> str:
        name = self._get_entity_name_from_entity()
        key = self.session_id
        return f"@{name}@{key}" if name else key

    @property
    def thread_id(self) -> str:
        """Deprecated alias for :attr:`session_id`."""
        warnings.warn(
            "AgentEntityStateProviderMixin.thread_id is deprecated; use session_id instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.session_id

    @property
    def state(self) -> DurableAgentState:
        if self._state_cache is None:
            raw_state = self._read_writable_state_dict()
            self._state_cache = DurableAgentState() if raw_state == {} else DurableAgentState.from_dict(raw_state)
            self._persisted_state_snapshot = deepcopy(raw_state)
        return self._state_cache

    @state.setter
    def state(self, value: DurableAgentState) -> None:
        self.ensure_v2_writable()
        prior_cache = self._state_cache
        prior_snapshot = deepcopy(self._persisted_state_snapshot)
        try:
            candidate = deepcopy(value)
            candidate.prepare_for_write(delivery_window_seconds=self._response_delivery_window_seconds())
            self._state_cache = candidate
            self.persist_state()
        except BaseException:
            if value is prior_cache:
                self._restore_persisted_cache(prior_snapshot)
            else:
                self._state_cache = prior_cache
                self._persisted_state_snapshot = prior_snapshot
            raise

    def persist_state(self) -> None:
        """Pass state to the host, which may stage rather than confirm a durable write."""
        from ._history_provider import current_durable_history_binding

        binding = current_durable_history_binding()
        if binding is not None and binding.state_provider is self:
            raise ValueError("Provider hooks cannot commit independently of the enclosing agent operation.")
        self.ensure_v2_writable()
        state = self.state
        committed_snapshot = self._read_committed_snapshot()
        transition_baseline = self._transition_validation_baseline(committed_snapshot)
        try:
            state.prepare_for_write(delivery_window_seconds=self._response_delivery_window_seconds())
            payload = state.to_dict()
            validate_completion_transition(transition_baseline, payload)
        except BaseException:
            self._restore_persisted_cache(committed_snapshot)
            record_write(state, stage="serialization", outcome="failed")
            raise
        record_write(state, stage="serialization", outcome="returned")
        try:
            self._set_state_dict(payload)
        except BaseException:
            self._write_acknowledgement_uncertain = True
            self._restore_persisted_cache(committed_snapshot)
            record_write(state, stage="set_state", outcome="failed")
            raise
        self._persisted_state_snapshot = deepcopy(payload)
        record_write(state, stage="set_state", outcome="returned")

    def _restore_persisted_cache(self, snapshot: dict[str, Any] | None = None) -> None:
        restored_snapshot = deepcopy(self._persisted_state_snapshot if snapshot is None else snapshot)
        self._persisted_state_snapshot = restored_snapshot
        self._state_cache = (
            None
            if restored_snapshot is None
            else DurableAgentState()
            if restored_snapshot == {}
            else DurableAgentState.from_dict(restored_snapshot)
        )

    def replace_cached_state(self, state: DurableAgentState) -> None:
        self._state_cache = state

    def reset(self) -> None:
        """Clear the entity through the full agent reset path when available."""
        agent_entity = getattr(self, "_agent_entity", None)
        if agent_entity is not None:
            agent_entity.reset()
            return
        self.ensure_v2_writable()
        original = self._state_cache
        self._state_cache = DurableAgentState()
        try:
            self.persist_state()
        except BaseException:
            self._state_cache = original
            raise
        logger.debug("[AgentEntityStateProviderMixin.reset] State reset complete")


class AgentEntity:
    """Platform-agnostic agent execution logic.

    This class encapsulates the core logic for executing an agent within a durable entity context.
    """

    agent: SupportsAgentRun
    callback: AgentResponseCallbackProtocol | None

    def __init__(
        self,
        agent: SupportsAgentRun,
        callback: AgentResponseCallbackProtocol | None = None,
        *,
        state_provider: AgentEntityStateProviderMixin,
        retention: RetentionMode = DEFAULT_RETENTION,
        max_state_bytes: StateBudget = DEFAULT_MAX_STATE_BYTES,
        high_watermark: float = HIGH_WATERMARK,
        low_watermark: float = LOW_WATERMARK,
        response_delivery_window_seconds: int = DELIVERY_WINDOW_SECONDS,
    ) -> None:
        validate_retention(retention, high_watermark, low_watermark)
        validate_response_delivery_window(response_delivery_window_seconds)
        self.agent = ensure_durable_history(agent, prune_excluded=prunes_excluded(retention))
        self.callback = callback
        self._state_provider = state_provider
        self._retention = retention
        self._max_state_bytes = resolve_state_budget(max_state_bytes)
        self._high_watermark = high_watermark
        self._low_watermark = low_watermark
        self._response_delivery_window_seconds = response_delivery_window_seconds

        logger.debug("[AgentEntity] Initialized with agent type: %s", type(agent).__name__)

    @property
    def state(self) -> DurableAgentState:
        return self._state_provider.state

    @state.setter
    def state(self, value: DurableAgentState) -> None:
        self._state_provider.state = value

    def persist_state(self) -> None:
        self._state_provider.persist_state()

    def expire_responses(self) -> int:
        """Remove expired delivery payloads without model execution or deleting receipts."""
        self._state_provider.ensure_v2_writable()
        self._migration_session_id()
        original = self.state
        staged = deepcopy(original)
        staged.prepare_for_write(delivery_window_seconds=self._response_delivery_window_seconds)
        before = len(staged.data.response_mailbox)
        staged.expire_responses()
        removed = before - len(staged.data.response_mailbox)
        if not removed:
            return 0
        self._state_provider.replace_cached_state(staged)
        try:
            self._validate_control_budget()
            self.persist_state()
        except BaseException:
            self._state_provider.replace_cached_state(original)
            raise
        return removed

    def reset(self) -> None:
        self._state_provider.ensure_v2_writable()
        self._migration_session_id()
        if self._has_context_pipeline():
            providers = cast("Sequence[Any]", self.agent.context_providers)  # type: ignore[attr-defined]
            if any(
                isinstance(provider, HistoryProvider)
                and provider.load_messages
                and not isinstance(provider, DurableHistoryProvider)
                for provider in providers
            ):
                raise NotImplementedError("Reset of external history requires a provider-owned clear operation.")
        original = self.state
        self._state_provider.replace_cached_state(deepcopy(original))
        try:
            self.state.prepare_for_write(delivery_window_seconds=self._response_delivery_window_seconds)
            self.state.data.conversation_history.clear()
            self.state.data.session = None
            self.state.expire_responses()
            self._validate_control_budget()
            self.persist_state()
        except BaseException:
            self._state_provider.replace_cached_state(original)
            raise

    def migrate(self, request: dict[str, Any]) -> dict[str, str]:
        """Import an authorized quiesced legacy export into a separate empty entity.

        This backend-only operation cannot authorize the operator or fence the old
        deployment. HTTP and MCP routes do not expose it. Identical retries return
        the recorded migration without rewriting results or refreshing their grace.

        Args:
            request: Source, sourceDigest, sourceSessionId, destinationSessionId,
                migrationId, ownershipTransferId, and optional deliveryEvidence,
                completionEvidence and requireKnownOutcomes.

        Returns:
            The committed migration identity and destination session identity.
        """
        self._state_provider.ensure_v2_writable()
        self._migration_session_id()
        required = {
            "source",
            "sourceDigest",
            "sourceSessionId",
            "destinationSessionId",
            "migrationId",
            "ownershipTransferId",
        }
        if (
            not isinstance(request, dict)
            or not required <= request.keys()
            or request.keys() - required - {"deliveryEvidence", "completionEvidence", "requireKnownOutcomes"}
        ):
            raise ValueError("Migration requires a complete explicit source and destination request.")
        for name in required - {"source"}:
            if not isinstance(request[name], str) or not request[name].strip():
                raise ValueError(f"Migration {name} must be a nonblank string.")
        destination = self._state_provider.core_session_id
        if request["destinationSessionId"] != destination:
            raise ValueError("Migration destinationSessionId does not match this entity.")
        if request["sourceSessionId"] == destination:
            raise ValueError("Migration requires a separately addressed destination, never an in-place rewrite.")
        if not isinstance(request["source"], dict):
            raise ValueError("Migration source must be an exported state object.")
        digest = state_snapshot_digest(request)
        original = self.state
        original_snapshot = original.to_dict()
        existing = original.data.unknown_fields.get("migration")
        if isinstance(existing, dict) and cast("dict[str, Any]", existing).get("requestDigest") == digest:
            expected_binding = {
                "id": request["migrationId"],
                "sourceDigest": request["sourceDigest"],
                "sourceSessionId": request["sourceSessionId"],
                "ownershipTransferId": request["ownershipTransferId"],
                "destinationSessionId": destination,
            }
            for request_field, binding_field in (
                ("deliveryEvidence", "evidenceId"),
                ("completionEvidence", "completionEvidenceId"),
            ):
                evidence = request.get(request_field)
                if evidence is not None:
                    expected_binding[binding_field] = (
                        cast("dict[str, Any]", evidence).get("evidenceId") if isinstance(evidence, dict) else None
                    )
            committed_binding = {
                field: value
                for field, value in cast("dict[str, Any]", existing).items()
                if field in expected_binding or field in ("evidenceId", "completionEvidenceId")
            }
            if committed_binding != expected_binding:
                raise ValueError("Committed migration binding does not match the retry request")
            return {"status": "migrated", "migrationId": request["migrationId"], "sessionId": destination}
        if original_snapshot != DurableAgentState().to_dict():
            raise ValueError(
                "Migration destination must be empty; an existing or different migration cannot be replaced."
            )
        staged = migrate_legacy_state(
            cast("dict[str, Any]", request["source"]),
            source_digest=request["sourceDigest"],
            source_session_id=request["sourceSessionId"],
            migration_id=request["migrationId"],
            ownership_transfer_id=request["ownershipTransferId"],
            delivery_window_seconds=self._response_delivery_window_seconds,
            delivery_evidence=request.get("deliveryEvidence"),
            completion_evidence=request.get("completionEvidence"),
            require_known_outcomes=request.get("requireKnownOutcomes", False),
        )
        staged.data.unknown_fields["migration"].update({"requestDigest": digest, "destinationSessionId": destination})
        self._state_provider.replace_cached_state(staged)
        try:
            self._validate_control_budget()
            self.persist_state()
        except BaseException:
            self._state_provider.replace_cached_state(original)
            raise
        return {"status": "migrated", "migrationId": request["migrationId"], "sessionId": destination}

    def _validate_control_budget(self) -> None:
        """Reject an oversized maintenance commit, without pruning any protected state."""
        if self._max_state_bytes is not None:
            size = len(json.dumps(self.state.to_dict(), allow_nan=False))
            if size > self._max_state_bytes:
                raise StateCapacityError(
                    size_bytes=size,
                    max_state_bytes=self._max_state_bytes,
                    floor_bytes=size,
                    target_bytes=self._max_state_bytes,
                )

    def _is_error_response(self, entry: DurableAgentStateEntry) -> bool:
        """Check if a conversation history entry records a failed turn."""
        return entry.is_error_response or isinstance(entry, DurableAgentStateUnknownEntry)

    async def run(
        self,
        request: RunRequest | dict[str, Any] | str,
    ) -> AgentResponse:
        """Execute the agent with a message."""
        self._state_provider.ensure_v2_writable()
        self._migration_session_id()
        if isinstance(request, str):
            run_request = RunRequest.from_json(request)
        elif isinstance(request, dict):
            run_request = RunRequest.from_dict(request)
        else:
            run_request = request

        run_request.validate_context()
        validate_identifier(run_request.correlation_id, "correlationId")
        original = self.state
        original.prepare_for_write(delivery_window_seconds=self._response_delivery_window_seconds)
        already_answered = original.try_get_agent_response(run_request.correlation_id)
        if already_answered is not None:
            return already_answered
        self._state_provider.replace_cached_state(deepcopy(original))
        with retention_operation(self.state):
            try:
                self.state.expire_responses()
                response = await self._execute_request(run_request)
                await self._enforce_retention()
                self.persist_state()
                return response
            except BaseException:
                self._state_provider.replace_cached_state(original)
                raise

    async def _execute_request(self, run_request: RunRequest) -> AgentResponse:
        """Stage a turn without committing until every local slice is valid."""
        message = run_request.message
        session_id = self._state_provider.session_id
        correlation_id = run_request.correlation_id
        if not session_id:
            raise ValueError("Entity State Provider must provide a session_id")
        options: dict[str, Any] = dict(run_request.options)
        options.setdefault("response_format", run_request.response_format)

        logger.debug("[AgentEntity.run] Received SessionId %s Message: %s", session_id, run_request)

        durable_history = self._find_durable_history_provider()
        uses_context_pipeline = self._has_context_pipeline()
        service_owns_history = service_stores_history(self.agent, options)
        prior_receipts = deepcopy(self.state.data.ingested_messages)
        state_request = DurableAgentStateRequest.from_run_request(run_request)
        if run_request.context_messages is not None:
            state_request.messages = self._drop_already_stored(
                state_request.messages, occurrence_ids=run_request.context_message_ids
            )
        if not uses_context_pipeline:
            self.state.data.conversation_history.append(state_request)

        history_binding = DurableHistoryBinding(
            state_provider=self._state_provider,
            correlation_id=correlation_id,
            service_owns_history=service_owns_history,
        )
        binding_token = bind_durable_history(history_binding) if uses_context_pipeline else None

        session: Any = None
        inactive_service_id: Any = None
        succeeded = False
        original_agent = self.agent
        progress = InvocationProgress()
        service_observation_exact = True

        try:
            self.agent = prepare_history_owner(self.agent, service_owns_history)
            if not run_request.enable_tool_calls:
                invocation_agent = copy(self.agent)
                defaults = getattr(invocation_agent, "default_options", None)
                if isinstance(defaults, Mapping):
                    invocation_agent.default_options = {  # type: ignore[attr-defined]
                        **cast("Mapping[str, Any]", defaults),
                        "tools": [],
                        "tool_choice": "none",
                    }
                client = getattr(invocation_agent, "client", None)
                invocation_configuration = getattr(client, "function_invocation_configuration", None)
                if client is not None and isinstance(invocation_configuration, Mapping):
                    invocation_client = copy(client)
                    invocation_client.function_invocation_configuration = {
                        **cast("Mapping[str, Any]", invocation_configuration),
                        "enabled": False,
                    }
                    invocation_agent.client = invocation_client  # type: ignore[attr-defined]
                if isinstance(getattr(invocation_agent, "mcp_tools", None), list):
                    invocation_agent.mcp_tools = []  # type: ignore[attr-defined]
                self.agent = invocation_agent
                options["tools"] = []
                options["tool_choice"] = "none"
            if uses_context_pipeline:
                session = self._create_session()
                if not service_owns_history:
                    inactive_service_id = getattr(session, "service_session_id", None)
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
                chat_messages: list[Message] = []
                for stored in state_request.messages:
                    current = self._to_current_message(stored, run_request)
                    if current is None:
                        continue
                    if stored.ingestion_occurrence and stored.ingestion_identity:
                        current._durable_ingestion_receipt = (  # type: ignore[attr-defined]
                            stored.ingestion_occurrence,
                            stored.ingestion_identity,
                        )
                    chat_messages.append(current)
                run_kwargs: dict[str, Any] = {
                    "messages": chat_messages,
                    "session": session,
                    "options": options,
                }
            else:
                session = None
                chat_messages = self._replay_all_messages()
                run_kwargs = {"messages": chat_messages, "options": options}

            middleware_agent = self.agent
            if isinstance(middleware_agent, Agent):
                if service_owns_history:
                    invocation_agent = copy(cast(Any, middleware_agent))

                    def completed_service(response: ChatResponse) -> None:
                        progress.service_completed = True
                        continuation = response.conversation_id
                        if (
                            session is not None
                            and isinstance(continuation, str)
                            and continuation
                            and not is_local_history_conversation_id(continuation)
                            and not response.has_internal_conversation_id()
                        ):
                            session.service_session_id = deepcopy(continuation)

                    invocation_agent.client = DurableServiceClient(
                        invocation_agent.client, history_binding.accept, completed_service
                    )
                    service_observation_exact = invocation_agent.client.exact_acceptance
                    self.agent = invocation_agent
                middleware = [DurableToolGuard(progress, enabled=run_request.enable_tool_calls)]
                run_kwargs["middleware"] = middleware
                run_kwargs["client_kwargs"] = {"middleware": middleware}
            provider_sources: set[str] = (
                {
                    provider.source_id
                    for provider in cast("Sequence[Any]", self.agent.context_providers)  # type: ignore[attr-defined]
                }
                if uses_context_pipeline
                else set[str]()
            )
            original_snapshot = _retry_snapshot(session, run_kwargs, provider_sources) if session is not None else None
            try:
                agent_run_response: AgentResponse = await self._invoke_agent(
                    run_kwargs=run_kwargs,
                    correlation_id=correlation_id,
                    session_id=session_id,
                    request_message=message,
                    progress=progress,
                )
            except Exception as exc:
                if (
                    session is None
                    or not service_owns_history
                    or not service_observation_exact
                    or not _is_missing_previous_response(exc)
                    or progress.stream_started
                    or progress.function_started
                    or progress.service_completed
                    or original_snapshot is None
                    or _retry_snapshot(session, run_kwargs, provider_sources) != original_snapshot
                ):
                    raise
                retried = await self._retry_rejected_conversation_id(
                    run_kwargs=run_kwargs,
                    correlation_id=correlation_id,
                    session_id=session_id,
                    request_message=message,
                    cause=exc,
                    progress=progress,
                    original_snapshot=original_snapshot,
                    provider_sources=provider_sources,
                )
                if retried is None:
                    raise
                agent_run_response = retried

            succeeded = not is_terminal_agent_response(agent_run_response)
            if (
                succeeded
                and not agent_run_response.user_input_requests
                and agent_run_response.additional_properties.get("durable_status") != "accepted"
            ):
                _ = agent_run_response.value

        except Exception as exc:
            succeeded = False
            logger.exception("[AgentEntity.run] Agent execution failed.")

            detail = f"{type(exc).__name__}: {exc}"
            error_message = Message(
                role="assistant",
                contents=[
                    Content.from_error(message=str(exc), error_code=type(exc).__name__),
                    Content.from_text(detail),
                ],
            )
            agent_run_response = AgentResponse(
                messages=[error_message],
                created_at=datetime.now(tz=timezone.utc).isoformat(),
                additional_properties={"durable_status": "error", "correlation_id": correlation_id},
            )

        finally:
            try:
                if session is not None and durable_history is not None and not service_owns_history:
                    if not succeeded:
                        durable_history.finalize_failed_run(session.state.get(durable_history.source_id, {}))
                    durable_history.flush(session.state.get(durable_history.source_id, {}))
            finally:
                if session is not None and not service_owns_history:
                    session.service_session_id = inactive_service_id
                if binding_token is not None:
                    unbind_durable_history(binding_token)
                self.agent = original_agent

        if uses_context_pipeline and (not succeeded or service_owns_history):
            staged_inputs = {
                (stored.ingestion_occurrence, stored.ingestion_identity)
                for entry in self.state.data.conversation_history
                if isinstance(entry, DurableAgentStateRequest) and entry.correlation_id == correlation_id
                for stored in entry.messages
            }
            staged_inputs.update(history_binding.accepted_inputs)
            self.state.data.ingested_messages = prior_receipts
            for stored in state_request.messages:
                identity = stored.ingestion_occurrence or stored.message_id
                if identity and (identity, stored.ingestion_identity) in staged_inputs:
                    fingerprints = self.state.data.ingested_messages.get(identity, [])
                    if fingerprints is not None and stored.ingestion_identity:
                        if stored.ingestion_identity not in fingerprints:
                            fingerprints.append(stored.ingestion_identity)
                        self.state.data.ingested_messages[identity] = fingerprints
        self.state.record_response(
            correlation_id,
            agent_run_response,
            delivery_window_seconds=self._response_delivery_window_seconds,
        )
        if not uses_context_pipeline and succeeded:
            self.state.data.conversation_history.append(
                DurableAgentStateResponse.from_run_response(correlation_id, agent_run_response)
            )
        self._capture_session(session)
        # First completion and duplicate delivery use the same canonical result
        # projection, including explicit approval/value transport classifications.
        delivered = self.state.try_get_agent_response(correlation_id)
        if delivered is None:
            raise ValueError("A staged completion must have an authoritative delivery result.")
        return delivered

    @staticmethod
    def _to_replayable_message(message: DurableAgentStateMessage) -> Message | None:
        """Convert persisted history into a message safe to replay into chat clients."""
        chat_message = message.to_chat_message()
        replayable_contents = [
            content for content in chat_message.contents if content.type not in ("reasoning", "text_reasoning")
        ]
        if not replayable_contents:
            return None

        return Message(
            role=chat_message.role,
            contents=replayable_contents,
            author_name=chat_message.author_name,
            message_id=chat_message.message_id,
            additional_properties=chat_message.additional_properties,
        )

    @staticmethod
    def _to_current_message(message: DurableAgentStateMessage, request: RunRequest) -> Message | None:
        """Preserve core input content metadata rather than round-tripping through legacy types."""
        raw = getattr(message, "_original_core_message", None)
        if request.context_messages is not None and isinstance(raw, dict):
            original = load_agent_response({"messages": [raw]}).messages[0]
            preserve_input_envelope(original, cast("dict[str, Any]", raw))
            return original
        return AgentEntity._to_replayable_message(message)

    async def _invoke_agent(
        self,
        run_kwargs: dict[str, Any],
        correlation_id: str,
        session_id: str,
        request_message: str,
        progress: InvocationProgress | None = None,
    ) -> AgentResponse:
        """Execute the agent, preferring streaming when available."""
        callback_context: AgentCallbackContext | None = None
        if self.callback is not None:
            callback_context = self._build_callback_context(
                correlation_id=correlation_id,
                session_id=session_id,
                request_message=request_message,
            )

        run_callable = self.agent.run

        try:
            stream_candidate = run_callable(stream=True, **run_kwargs)
            if inspect.isawaitable(stream_candidate):
                stream_candidate = await stream_candidate
        except TypeError as type_error:
            detail = str(type_error)
            if not (
                "stream is not supported" in detail
                or "streaming not supported" in detail
                or "unexpected keyword argument 'stream'" in detail
                or 'unexpected keyword argument "stream"' in detail
            ):
                raise
            logger.debug(
                "Agent does not support streaming; invoking non-streaming run(): %s",
                type_error,
            )
        else:
            if isinstance(stream_candidate, AgentResponse):
                direct_response = cast(AgentResponse, stream_candidate)
                await self._notify_final_response(direct_response, callback_context)
                return direct_response
            return await self._consume_stream(
                stream=stream_candidate,
                callback_context=callback_context,
                progress=progress,
            )
        agent_run_response = run_callable(**run_kwargs)
        if inspect.isawaitable(agent_run_response):
            agent_run_response = await agent_run_response

        if not isinstance(agent_run_response, AgentResponse):
            raise TypeError(
                f"Agent run() must return an AgentResponse instance; received {type(agent_run_response).__name__}"
            )
        await self._notify_final_response(agent_run_response, callback_context)
        return agent_run_response

    async def _consume_stream(
        self,
        stream: ResponseStream[AgentResponseUpdate, AgentResponse],
        callback_context: AgentCallbackContext | None = None,
        progress: InvocationProgress | None = None,
    ) -> AgentResponse:
        """Consume streaming responses and build the final AgentResponse."""
        async for update in stream:
            if progress is not None:
                progress.stream_started = True
            await self._notify_stream_update(update, callback_context)

        response = await stream.get_final_response()

        await self._notify_final_response(response, callback_context)
        return response

    async def _notify_stream_update(
        self,
        update: AgentResponseUpdate,
        context: AgentCallbackContext | None,
    ) -> None:
        """Invoke the streaming callback if one is registered."""
        if self.callback is None or context is None:
            return

        try:
            callback_result = self.callback.on_streaming_response_update(deepcopy(update), context)
            if inspect.isawaitable(callback_result):
                await callback_result
        except Exception as exc:
            logger.warning(
                "[AgentEntity] Streaming callback raised an exception: %s",
                exc,
                exc_info=True,
            )

    async def _notify_final_response(
        self,
        response: AgentResponse,
        context: AgentCallbackContext | None,
    ) -> None:
        """Invoke the final response callback if one is registered."""
        if self.callback is None or context is None:
            return

        try:
            snapshot = deepcopy(response)
            try:
                snapshot.raw_representation = deepcopy(response.raw_representation)
            except Exception:
                snapshot.raw_representation = None
            callback_result = self.callback.on_agent_response(snapshot, context)
            if inspect.isawaitable(callback_result):
                await callback_result
        except Exception as exc:
            logger.warning(
                "[AgentEntity] Response callback raised an exception: %s",
                exc,
                exc_info=True,
            )

    def _build_callback_context(
        self,
        correlation_id: str,
        session_id: str,
        request_message: str,
    ) -> AgentCallbackContext:
        """Create the callback context provided to consumers."""
        agent_name = getattr(self.agent, "name", None) or type(self.agent).__name__
        return AgentCallbackContext(
            agent_name=agent_name,
            correlation_id=correlation_id,
            session_id=session_id,
            request_message=request_message,
        )

    async def _retry_rejected_conversation_id(
        self,
        *,
        run_kwargs: dict[str, Any],
        correlation_id: str,
        session_id: str,
        request_message: Any,
        cause: BaseException,
        progress: InvocationProgress,
        original_snapshot: str,
        provider_sources: set[str],
    ) -> AgentResponse | None:
        for attempt in range(1, _REJECTED_ID_RETRIES + 1):
            await asyncio.sleep(_REJECTED_ID_BACKOFF_SECONDS * attempt)
            try:
                response: AgentResponse = await self._invoke_agent(
                    run_kwargs=run_kwargs,
                    correlation_id=correlation_id,
                    session_id=session_id,
                    request_message=request_message,
                    progress=progress,
                )
            except Exception as retry_exc:
                if (
                    not _is_missing_previous_response(retry_exc, prior_error=cause)
                    or progress.stream_started
                    or progress.function_started
                    or progress.service_completed
                    or _retry_snapshot(run_kwargs.get("session"), run_kwargs, provider_sources) != original_snapshot
                ):
                    raise
                logger.debug(
                    "[AgentEntity.run] Conversation id still not accepted for session %s (attempt %d of %d).",
                    session_id,
                    attempt,
                    _REJECTED_ID_RETRIES,
                )
                continue
            logger.info(
                "[AgentEntity.run] Conversation id for session %s was accepted on attempt %d, "
                "so the turn continued without resending the transcript.",
                session_id,
                attempt,
            )
            return response

        logger.debug(
            "[AgentEntity.run] Conversation id for session %s was refused on every attempt. %s",
            session_id,
            cause,
        )
        return None

    async def _enforce_retention(self) -> None:
        """Apply optional whole-state pressure budgeting independently of eager pruning."""
        if self._max_state_bytes is None:
            return
        await enforce_budget(
            self.state,
            max_state_bytes=self._max_state_bytes,
            high_watermark=self._high_watermark,
            low_watermark=self._low_watermark,
        )

    def _has_context_pipeline(self) -> bool:
        return isinstance(getattr(self.agent, "context_providers", None), (list, tuple))

    def _capture_session(self, session: Any) -> None:
        if session is None:
            return
        to_dict = getattr(session, "to_dict", None)
        if not callable(to_dict):
            return

        durable_history = self._find_durable_history_provider()
        session_state = getattr(session, "state", None)
        transient: Any = None
        has_transient = False
        if durable_history is not None and isinstance(session_state, dict):
            bag = cast("dict[str, Any]", session_state)
            if durable_history.source_id in bag:
                transient = bag.pop(durable_history.source_id)
                has_transient = True
                if isinstance(transient, dict):
                    persistent = {
                        key: value
                        for key, value in cast("dict[str, Any]", transient).items()
                        if key not in ("messages", "_positions")
                    }
                    if persistent:
                        bag[durable_history.source_id] = persistent
        try:
            payload = cast("dict[str, Any]", to_dict())
        finally:
            if has_transient:
                cast("dict[str, Any]", session_state)[durable_history.source_id] = transient  # type: ignore[union-attr]

        try:
            json.dumps(payload, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ValueError("Agent session state is not JSON-compatible; the operation cannot commit.") from exc
        previous = self.state.data.session
        if isinstance(previous, dict):
            opaque = {
                key: deepcopy(value)
                for key, value in previous.items()
                if key not in {"type", "session_id", "service_session_id", "state"} and key not in payload
            }
            payload = {**opaque, **payload}
        self.state.data.session = payload

    def _drop_already_stored(
        self, messages: list[DurableAgentStateMessage], *, occurrence_ids: list[str] | None = None
    ) -> list[DurableAgentStateMessage]:
        receipts = self.state.data.ingested_messages
        kept: list[DurableAgentStateMessage] = []
        for index, message in enumerate(messages):
            identity = occurrence_ids[index] if occurrence_ids is not None else message.message_id
            if identity and identity.strip():
                fingerprint = message.ingestion_identity or message_identity(message.to_chat_message())
                known = receipts.get(identity, [])
                if known is None or fingerprint in known:
                    continue
                known.append(fingerprint)
                receipts[identity] = known
                message.ingestion_occurrence = identity
            kept.append(message)
        return kept

    def _find_durable_history_provider(self) -> DurableHistoryProvider | None:
        providers = getattr(self.agent, "context_providers", None)
        if not isinstance(providers, (list, tuple)):
            return None
        for provider in cast("Sequence[Any]", providers):
            if isinstance(provider, DurableHistoryProvider):
                return provider
        return None

    def _migration_session_id(self) -> str:
        """Validate reserved committed binding metadata before any operation can use it."""
        session_id = self._state_provider.core_session_id
        metadata = self.state.data.unknown_fields
        if "migration" not in metadata:
            return session_id
        message = "Committed migration session binding is invalid or does not match this destination entity."
        migration = metadata["migration"]
        if not isinstance(migration, dict):
            raise ValueError(message)
        migration = cast("dict[str, Any]", migration)
        for field in ("id", "sourceSessionId", "ownershipTransferId", "destinationSessionId"):
            value = migration.get(field)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(message)
        for field in ("sourceDigest", "requestDigest"):
            value = migration.get(field)
            if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
                raise ValueError(message)
        try:
            validate_timestamp(migration.get("createdAt"))
        except ValueError as exc:
            raise ValueError(message) from exc
        for field in ("evidenceId", "completionEvidenceId"):
            if field in migration and (not isinstance(migration[field], str) or not migration[field].strip()):
                raise ValueError(message)
        source_session_id = cast(str, migration["sourceSessionId"])
        if migration["destinationSessionId"] != session_id or source_session_id == session_id:
            raise ValueError(message)
        # Reset clears the local session, but does not relinquish migration ownership.
        stored = self.state.data.session
        if stored is not None:
            if not isinstance(stored, dict):
                raise ValueError(message)
            stored_id = stored.get(_SESSION_ID_KEY)
            if not isinstance(stored_id, str) or not stored_id.strip() or stored_id != source_session_id:
                raise ValueError(message)
        return source_session_id

    def _create_session(self) -> Any:
        create_session = getattr(self.agent, "create_session", None)
        if not callable(create_session):
            raise TypeError(
                f"Agent {type(self.agent).__name__} exposes context providers but does not support create_session()."
            )
        session: Any = create_session(session_id=self._migration_session_id())
        self._restore_session(session)
        return session

    def _restore_session(self, session: Any) -> None:
        stored = self.state.data.session
        if not stored or _SESSION_ID_KEY not in stored:
            return

        _register_loaded_state_types()

        restored = AgentSession.from_dict(dict(stored))
        session.state.update(restored.state)
        if getattr(session, "service_session_id", None) is None:
            session.service_session_id = restored.service_session_id

    def _replay_all_messages(self) -> list[Message]:
        return [
            replayable_message
            for entry in self.state.data.conversation_history
            if not self._is_error_response(entry)
            for m in entry.messages
            if (replayable_message := self._to_replayable_message(m)) is not None
        ]


class DurableTaskEntityStateProvider(DurableEntity, AgentEntityStateProviderMixin):
    """DurableTask Durable Entity state provider for AgentEntity.

    This class utilizes the Durable Entity context from `durabletask` package
    to get and set the state of the agent entity.
    """

    def __init__(self) -> None:
        super().__init__()

    def _get_state_dict(self) -> dict[str, Any]:
        raw = self.get_state(default={})
        if raw is None:
            return {}
        if not isinstance(raw, dict):
            raise ValueError("Durable entity state must be a JSON object.")
        return cast(dict[str, Any], raw)

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        self.set_state(state)

    def _get_session_id_from_entity(self) -> str:
        return self.entity_context.entity_id.key

    def _get_entity_name_from_entity(self) -> str:
        entity_id = self.entity_context.entity_id
        entity_name = getattr(entity_id, "entity", None)
        if isinstance(entity_name, str):
            return entity_name
        name = getattr(entity_id, "name", "")
        return name if isinstance(name, str) else ""
