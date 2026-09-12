# Copyright (c) Microsoft. All rights reserved.

"""Durable Task entity implementations for Microsoft Agent Framework."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
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
    Content,
    Message,
    ResponseStream,
    SupportsAgentRun,
    register_state_type,
)
from durabletask.entities import DurableEntity

from ._callbacks import AgentCallbackContext, AgentResponseCallbackProtocol
from ._configuration import validate_response_delivery_window
from ._durable_agent_state import (
    DurableAgentState,
    DurableAgentStateEntry,
    DurableAgentStateErrorResponse,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
    DurableAgentStateUnknownEntry,
)
from ._history_provider import (
    DurableHistoryBinding,
    DurableHistoryProvider,
    bind_durable_history,
    ensure_durable_history,
    prepare_history_owner,
    service_stores_history,
    unbind_durable_history,
)
from ._invocation_safety import DurableToolGuard, InvocationProgress
from ._message_identity import message_identity
from ._models import RunRequest
from ._response_utils import is_terminal_agent_response, load_agent_response
from ._retention import (
    DEFAULT_MAX_STATE_BYTES,
    DEFAULT_RETENTION,
    DELIVERY_WINDOW_SECONDS,
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
from ._state_migration import migrate_legacy_state, state_snapshot_digest

logger = logging.getLogger("agent_framework.durabletask")

# Key produced by core's ``AgentSession.to_dict()``.
_SESSION_ID_KEY = "session_id"

try:
    # Root of core's serializable state types. Not part of core's public surface, so a move must
    # not break the entity: without it, restored provider state simply stays as plain dicts,
    # which is core's own behavior.
    from agent_framework._serialization import SerializationMixin

    _SerializableStateRoot: type | None = SerializationMixin
except ImportError:  # pragma: no cover - depends on the installed core version
    _SerializableStateRoot = None

_registered_state_types: set[type] = set()

# Provider error code for a conversation id the service will not accept as a parent turn.
_MISSING_PREVIOUS_RESPONSE_CODE = "previous_response_not_found"

_REJECTED_ID_RETRIES = 3
"""How many times to re-send a request whose conversation id the service would not accept.

Few, because a retry only helps when the id is late rather than gone, and the two are
indistinguishable from the error alone. Enough to cover the gap that was measured, which was
under a second on the chaining path.
"""

_REJECTED_ID_BACKOFF_SECONDS = 0.5
"""Multiplied by the attempt number, so the waits are 0.5s, 1s, 1.5s."""


def _is_missing_previous_response(exc: BaseException, *, prior_error: BaseException | None = None) -> bool:
    """Return whether the service refused the conversation id from the previous turn.

    A service that keeps the conversation can hand back the id of a finished response before that
    response is durably readable, so the next turn is refused even though the id is genuine and
    was captured correctly. Bounded identical-request retries may recover visibility delays;
    genuinely expired IDs still fail. No transcript recovery is attempted.

    Match only the structured error code, including wrapped causes, so unrelated
    request failures are not retried as conversation visibility failures.
    """
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


def _register_loaded_state_types() -> None:
    """Let core restore session state values as their own classes after a cold start.

    Core deserializes session state through a type registry that it seeds with exactly one entry
    (``Message``). Anything else must be registered explicitly, and the registry is process-local.
    A durable entity routinely restores state in a process that never serialized it, so without
    this a provider's state comes back as a plain dict rather than its own class.

    Only classes already imported in this process are registered - nothing is imported from
    persisted data - so this cannot load code the application has not already loaded itself. That
    is enough in practice, because whoever put a value in the state bag had to import its class to
    construct it.
    """
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
        """Return the entity name, when the host exposes one.

        Optional, so state providers written before this hook existed keep working. They fall
        back to a core session id built from the key alone.
        """
        return ""

    @property
    def session_id(self) -> str:
        return self._get_session_id_from_entity()

    @property
    def core_session_id(self) -> str:
        """Identity handed to core's ``create_session``, unique to this entity.

        ``session_id`` is only the entity key, which is not unique on its own. Every agent node
        in one workflow run shares a key (the orchestration instance id) and is told apart by
        entity name, so an external history provider keyed on the key alone would mix the
        histories of different nodes. The name is included here to keep them separate.

        Uses the same ``@name@key`` form as :class:`AgentSessionId`, so the result parses back.
        """
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
            raw_state = self._get_state_dict()
            self._state_cache = DurableAgentState.from_dict(raw_state) if raw_state else DurableAgentState()
        return self._state_cache

    @state.setter
    def state(self, value: DurableAgentState) -> None:
        self._state_cache = value
        self.persist_state()

    def persist_state(self) -> None:
        """Pass state to the host, which may stage rather than confirm a durable write."""
        if self._state_cache is None:
            self._state_cache = DurableAgentState()
        state = self._state_cache
        try:
            payload = state.to_dict()
        except BaseException:
            record_write(state, stage="serialization", outcome="failed")
            raise
        try:
            self._set_state_dict(payload)
        except BaseException:
            record_write(state, stage="set_state", outcome="failed")
            raise
        record_write(state, stage="set_state", outcome="returned")

    def replace_cached_state(self, state: DurableAgentState) -> None:
        """Stage or restore an operation snapshot without writing to the backend."""
        self._state_cache = state

    def reset(self) -> None:
        """Clear conversation history by resetting state to a fresh DurableAgentState."""
        self._state_cache = DurableAgentState()
        self.persist_state()
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
        # Back the agent's conversation history with durable entity state so an agent that
        # already works in core runs durably without any configuration change.
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
        """Remove expired delivery payloads without model execution or deleting receipts.

        Hosts expose this maintenance operation for an application-owned schedule.
        An idle entity has no timer of its own; availability expires independently.

        Returns:
            The number of payloads removed by this operation.
        """
        original = self.state
        original.prepare_for_write(delivery_window_seconds=self._response_delivery_window_seconds)
        staged = deepcopy(original)
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

    def migrate(self, request: dict[str, Any]) -> dict[str, str]:
        """Import a quiesced legacy snapshot into an empty, separately addressed entity.

        This privileged backend operation is not exposed through the generated HTTP
        or MCP routes. The deployment owner must authorize the source export, journal
        and ownership transfer. No runtime can inspect or fence a legacy deployment.
        Retries with the exact same request return the recorded migration, even after
        subsequent runs, without rewriting state or refreshing response grace.

        Args:
            request: Source snapshot/digest, sourceSessionId, destinationSessionId,
                migrationId, ownershipTransferId and optional deliveryEvidence and
                requireKnownOutcomes, which rejects imports without outcome evidence.

        Returns:
            The committed migration ID and destination session identity.
        """
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
            or request.keys() - required - {"deliveryEvidence", "requireKnownOutcomes"}
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
        existing = original.data.unknown_fields.get("migration")
        if isinstance(existing, dict) and cast("dict[str, Any]", existing).get("requestDigest") == digest:
            return {"status": "migrated", "migrationId": request["migrationId"], "sessionId": destination}
        if original.to_dict() != DurableAgentState().to_dict():
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
                raise ValueError("Retained delivery/control state cannot fit within max_state_bytes.")

    def reset(self) -> None:
        """Clear local history/session context without erasing execution receipts."""
        if self._has_context_pipeline() and self._find_durable_history_provider() is None:
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

    def _is_error_response(self, entry: DurableAgentStateEntry) -> bool:
        """Check if a conversation history entry records a failed turn."""
        return isinstance(entry, (DurableAgentStateErrorResponse, DurableAgentStateUnknownEntry))

    async def run(
        self,
        request: RunRequest | dict[str, Any] | str,
    ) -> AgentResponse:
        """Execute the agent with a message."""
        if isinstance(request, str):
            run_request = RunRequest.from_json(request)
        elif isinstance(request, dict):
            run_request = RunRequest.from_dict(request)
        else:
            run_request = request

        # A read-compatible legacy layout is not permission to run a new writer.
        self.state.prepare_for_write(delivery_window_seconds=self._response_delivery_window_seconds)
        already_answered = self.state.try_get_agent_response(run_request.correlation_id)
        if already_answered is not None:
            self.expire_responses()
            return already_answered
        original = self.state
        self._state_provider.replace_cached_state(deepcopy(original))
        with retention_operation(self.state):
            try:
                self.state.expire_responses()
                response = await self._execute_request(run_request)
                await self._enforce_retention()
                self.persist_state()
                return response
            except BaseException:
                # A failed commit must not leave a warm worker with staged completion or
                # ingestion receipts. External effects are outside this local rollback.
                self._state_provider.replace_cached_state(original)
                raise

    async def _execute_request(self, run_request: RunRequest) -> AgentResponse:
        """Stage a turn without committing until every local slice and budget is valid."""
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
        # A property of the run rather than of the registration, since ``store`` is an ordinary
        # run option. The provider stays attached either way so core never injects one of its own.
        service_owns_history = service_stores_history(self.agent, options)
        prior_receipts = deepcopy(self.state.data.ingested_messages)
        state_request = DurableAgentStateRequest.from_run_request(run_request)
        if run_request.context_messages is not None:
            state_request.messages = self._drop_already_stored(
                state_request.messages, occurrence_ids=run_request.context_message_ids
            )
        if not uses_context_pipeline:
            self.state.data.conversation_history.append(state_request)

        binding_token = (
            bind_durable_history(
                DurableHistoryBinding(
                    state_provider=self._state_provider,
                    correlation_id=correlation_id,
                    # The provider stays attached either way so core never injects one of its own,
                    # but it must not load history on a turn the service is already carrying.
                    service_owns_history=service_owns_history,
                )
            )
            if durable_history is not None
            else None
        )

        # Bound before the try so the failure path can always reach it. ``_create_session`` can
        # raise, and referencing an unbound name while handling that would replace the agent's
        # error with a NameError.
        session: Any = None
        inactive_service_id: Any = None
        succeeded = False
        original_agent = self.agent
        progress = InvocationProgress()

        try:
            self.agent = prepare_history_owner(self.agent, service_owns_history)
            if not run_request.enable_tool_calls:
                invocation_agent = copy(self.agent)
                # Core merges default, context-provider, MCP and additional tools. A
                # model option alone cannot disable the local invocation loop.
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
                # The agent's own context providers supply prior turns - durable-backed history,
                # an external store (Cosmos/Redis/file), or the model service itself. Only the
                # newly received request messages are passed as run input, so history lives in
                # its selected store, subject to the service-owned branch's inactive-primary gate.
                session = self._create_session()
                if not service_owns_history:
                    inactive_service_id = getattr(session, "service_session_id", None)
                    session.service_session_id = None
                    # A conversation ID supplied through defaults/options must not
                    # override the client-owned branch either. Copy, never mutate
                    # the agent the application may be using elsewhere.
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
                # Core's operation-local copies retain private attributes, while its
                # serializers exclude them. This receipt follows the actual appended
                # input, even when two equal inputs carry different transport IDs.
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
                # Fallback for agents without the core context pipeline (for example a fully
                # custom agent): the entity replays the persisted conversation on every turn.
                session = None
                chat_messages = self._replay_all_messages()
                run_kwargs = {"messages": chat_messages, "options": options}

            if isinstance(self.agent, Agent):
                run_kwargs["client_kwargs"] = {
                    "middleware": [DurableToolGuard(progress, enabled=run_request.enable_tool_calls)]
                }
            original_service_id = getattr(session, "service_session_id", None)
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
                    or not _is_missing_previous_response(exc)
                    or progress.stream_started
                    or progress.function_started
                    or getattr(session, "service_session_id", None) != original_service_id
                ):
                    raise
                retried = await self._retry_rejected_conversation_id(
                    run_kwargs=run_kwargs,
                    correlation_id=correlation_id,
                    session_id=session_id,
                    request_message=message,
                    cause=exc,
                    progress=progress,
                    original_service_id=original_service_id,
                )
                if retried is None:
                    raise
                agent_run_response = retried

            # Resolve structured output inside the runtime-error boundary. A parsing
            # error is a committed error result, not an invisible post-run failure.
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

            # The entity absorbs failures rather than faulting, so the session survives and the
            # caller can take the next turn. That is only reasonable if the caller can tell what
            # happened: error content alone leaves ``response.text`` empty, which reads as the
            # agent having nothing to say. The text carries the same message the error content
            # already holds, so callers inspecting contents see no change.
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

        if not succeeded and uses_context_pipeline:
            # A failed pre-invocation/provider load did not deliver these messages.
            # Retain receipts only for inputs actually staged by durable history;
            # no portable external provider API proves an interrupted append.
            staged_inputs = {
                (stored.ingestion_occurrence, stored.ingestion_identity)
                for entry in self.state.data.conversation_history
                if isinstance(entry, DurableAgentStateRequest) and entry.correlation_id == correlation_id
                for stored in entry.messages
            }
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
        return agent_run_response

    async def _retry_rejected_conversation_id(
        self,
        *,
        run_kwargs: dict[str, Any],
        correlation_id: str,
        session_id: str,
        request_message: Any,
        cause: BaseException,
        progress: InvocationProgress,
        original_service_id: Any,
    ) -> AgentResponse | None:
        """Re-send an identical request whose conversation id the service refused.

        The refusal we are recovering from is a read-after-write gap rather than a lost
        conversation. Measured against Azure OpenAI, a streamed response reports its id in the
        completion event before that response is retrievable, so the very next turn can be
        rejected for naming an id that is perfectly valid and simply not readable yet. Waiting
        briefly and asking again is the cheapest thing that works, and it leaves the conversation
        continuing from the same point rather than restarting it from a resent transcript.

        A retry cannot rescue an id that has genuinely expired, and the error is identical either
        way, so the attempts are few and short. Exhausting them fails the turn without
        reconstructing a transcript or starting a different service conversation.

        Args:
            run_kwargs: The unchanged arguments of the request that was refused.
            correlation_id: Correlation id of the in-flight request.
            session_id: Session the request belongs to.
            request_message: The originating message, for logging.
            cause: The refusal that triggered this, so a give-up is reported with its reason.
            progress: Run-local observations that prohibit restarting after stream or tool progress.
            original_service_id: Session continuation before the first attempt; retries must not advance it.

        Returns:
            The response, or None when every attempt was refused the same way.
        """
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
                    or getattr(run_kwargs.get("session"), "service_session_id", None) != original_service_id
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
        """Whether the agent exposes core's context-provider pipeline.

        When it does, the providers own conversation context and the entity delivers only the
        new messages. Agents without it fall back to replaying persisted history.
        """
        return isinstance(getattr(self.agent, "context_providers", None), (list, tuple))

    def _capture_session(self, session: Any) -> None:
        """Persist the session so provider state survives to the next turn.

        The entity creates a fresh session per operation, so anything the context providers keep
        in the session state bag - tool approval rules and queued approval requests, todo lists,
        memory extraction state - would otherwise be discarded at the end of every turn. Core
        documents that state as durable for the life of the session, so agents that rely on it
        must behave the same way here. The serialized session also carries the service-issued
        conversation id, so service-backed agents continue the same thread.

        Omit only the durable provider's working message buffer and position index, which are
        rebuilt from ``conversation_history`` each turn. Keep its other JSON-compatible state.
        Removing those transient fields before serialization avoids encoding a second transcript.

        Core can return live objects from session serialization. Validate the payload before
        staging it so an unusable session fails the operation without replacing committed state.
        """
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
        """Remember actual identities, including skipped positions and content revisions.

        Receipts outlive transcript eviction. Anonymous direct inputs are not content-
        deduplicated; the workflow sender supplies scoped IDs for anonymous projections.
        Entirely repeated projections stay empty instead of re-ingesting their last item.
        """
        receipts = self.state.data.ingested_messages
        kept: list[DurableAgentStateMessage] = []
        for index, message in enumerate(messages):
            identity = occurrence_ids[index] if occurrence_ids is not None else message.message_id
            if identity:
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
        """Return the agent's :class:`DurableHistoryProvider`, if it is configured with one."""
        providers = getattr(self.agent, "context_providers", None)
        if not isinstance(providers, (list, tuple)):
            return None
        for provider in cast("Sequence[Any]", providers):
            if isinstance(provider, DurableHistoryProvider):
                return provider
        return None

    def _create_session(self) -> Any:
        """Create the session for this operation and restore what the last turn left on it.

        Conversation history lives in the agent's context providers (durable entity state, an
        external store, or the model service), so a fresh session per operation is enough - but it
        must carry the entity's **stable** session id. External history providers (Cosmos, Redis,
        file) key their storage on ``session.session_id``, and with a freshly generated id they would
        read and write a different key every turn and never see prior history.

        The id is qualified with the entity name (see ``core_session_id``) because the key alone
        collides across the agent nodes of one workflow run.
        """
        create_session = getattr(self.agent, "create_session", None)
        if not callable(create_session):
            raise TypeError(
                f"Agent {type(self.agent).__name__} exposes context providers but does not support create_session()."
            )
        migration = self.state.data.unknown_fields.get("migration")
        logical_session_id = self._state_provider.core_session_id
        if isinstance(migration, dict):
            source_session_id = cast("dict[str, Any]", migration).get("sourceSessionId")
            if not isinstance(source_session_id, str) or not source_session_id.strip():
                raise ValueError("Migration sourceSessionId must preserve the original logical session identity.")
            logical_session_id = source_session_id
        session: Any = create_session(session_id=logical_session_id)
        self._restore_session(session)
        return session

    def _restore_session(self, session: Any) -> None:
        """Apply the previous turn's session state onto a freshly created session.

        The agent's own ``create_session`` is used so its session type is preserved. Only the
        state bag and the service conversation id are carried over.
        """
        stored = self.state.data.session
        if not stored or _SESSION_ID_KEY not in stored:
            return

        # Done here rather than at import: by now the agent and its providers are built, so the
        # classes their state uses are loaded and can be resolved.
        _register_loaded_state_types()

        restored = AgentSession.from_dict(dict(stored))
        session.state.update(restored.state)
        if getattr(session, "service_session_id", None) is None:
            session.service_session_id = restored.service_session_id

    def _replay_all_messages(self) -> list[Message]:
        """Build run input from the whole persisted transcript.

        Used only for agents without the core context pipeline. Service conversation
        errors do not trigger local transcript reconstruction.

        Failed turns are skipped so an error reply is never presented back to the model as
        something it said.
        """
        return [
            replayable_message
            for entry in self.state.data.conversation_history
            if not self._is_error_response(entry)
            for m in entry.messages
            if (replayable_message := self._to_replayable_message(m)) is not None
        ]

    @staticmethod
    def _to_current_message(message: DurableAgentStateMessage, request: RunRequest) -> Message | None:
        """Preserve core input content metadata rather than round-tripping through legacy types."""
        if request.context_messages is not None and message.ingestion_identity:
            for raw in request.context_messages:
                original = load_agent_response({"messages": [raw]}).messages[0]
                if (
                    original.message_id == message.message_id
                    and message_identity(original) == message.ingestion_identity
                ):
                    return original
        return AgentEntity._to_replayable_message(message)

    @staticmethod
    def _to_replayable_message(message: DurableAgentStateMessage) -> Message | None:
        """Convert persisted history into a message safe to replay into chat clients."""
        chat_message = message.to_chat_message()
        replayable_contents = [content for content in chat_message.contents if content.type != "reasoning"]
        if not replayable_contents:
            return None

        return Message(
            role=chat_message.role,
            contents=replayable_contents,
            author_name=chat_message.author_name,
            message_id=chat_message.message_id,
            additional_properties=chat_message.additional_properties,
        )

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

        # Only negotiate an unsupported streaming signature before consuming a stream.
        # Errors raised while consuming it must never restart model/tool execution.
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
                stream=stream_candidate, callback_context=callback_context, progress=progress
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
            # Core deliberately shares opaque SDK representations during deepcopy.
            # Detach them when possible, otherwise omit only that opaque field.
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


class DurableTaskEntityStateProvider(DurableEntity, AgentEntityStateProviderMixin):
    """DurableTask Durable Entity state provider for AgentEntity.

    This class utilizes the Durable Entity context from `durabletask` package
    to get and set the state of the agent entity.
    """

    def __init__(self) -> None:
        super().__init__()

    def _get_state_dict(self) -> dict[str, Any]:
        raw = self.get_state(dict, default={})
        return cast(dict[str, Any], raw)

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        self.set_state(state)

    def _get_session_id_from_entity(self) -> str:
        return self.entity_context.entity_id.key

    def _get_entity_name_from_entity(self) -> str:
        return self.entity_context.entity_id.entity
