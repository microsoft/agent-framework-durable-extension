# Copyright (c) Microsoft. All rights reserved.

"""Durable Task entity implementations for Microsoft Agent Framework."""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import warnings
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, cast

from agent_framework import (
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
from ._durable_agent_state import (
    DurableAgentState,
    DurableAgentStateEntry,
    DurableAgentStateErrorResponse,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
)
from ._history_provider import (
    DurableHistoryBinding,
    DurableHistoryProvider,
    bind_durable_history,
    ensure_durable_history,
    service_stores_history,
    unbind_durable_history,
)
from ._models import RunRequest
from ._retention import (
    DEFAULT_MAX_STATE_BYTES,
    DEFAULT_RETENTION,
    RetentionMode,
    enforce_budget,
    prunes_excluded,
)
from ._workflows.naming import parse_workflow_message_id

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


def _forget_message_content(messages: list[DurableAgentStateMessage]) -> None:
    """Drop the content of these messages, keeping the record that they happened.

    Used when a history provider the caller configured owns the conversation. The entity always
    records the exchange, in every configuration, because correlation ids and delivery are its
    job. It does not need to be a second copy of the conversation itself, and being one would put
    the customer's content under two different retention, residency and deletion policies while
    only one of them is the store they chose.

    What survives is the envelope and the message id. Ids matter because deduplicating repeated
    upstream context in a workflow is done by id, so forgetting them would let the same message be
    ingested twice.

    Args:
        messages: The stored messages, emptied in place.
    """
    for stored in messages:
        stored.contents = []


def _is_missing_previous_response(exc: BaseException) -> bool:
    """Return whether the service refused the conversation id from the previous turn.

    A service that keeps the conversation can hand back the id of a finished response before that
    response is durably readable, so the next turn is refused even though the id is genuine and
    was captured correctly. The conversation is not lost, it is simply unreachable by id, and
    resending the transcript recovers it.

    Matching is deliberately narrow. Replaying the transcript is only correct for this one
    failure, and a looser test would swallow real request errors and quietly answer without the
    context the caller asked for. So the provider's structured error ``code`` is used rather than
    a substring of the message, and the cause chain is walked because layers above the provider
    may wrap the original error.
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if getattr(current, "code", None) == _MISSING_PREVIOUS_RESPONSE_CODE:
            return True
        body = getattr(current, "body", None)
        if isinstance(body, Mapping) and cast("Mapping[str, Any]", body).get("code") == (
            _MISSING_PREVIOUS_RESPONSE_CODE
        ):
            return True
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
        """Persist the current state to the underlying storage provider."""
        if self._state_cache is None:
            self._state_cache = DurableAgentState()
        self._set_state_dict(self._state_cache.to_dict())

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
        max_state_bytes: int = DEFAULT_MAX_STATE_BYTES,
    ) -> None:
        # Back the agent's conversation history with durable entity state so an agent that
        # already works in core runs durably without any configuration change.
        self.agent = ensure_durable_history(agent, prune_excluded=prunes_excluded(retention))
        self.callback = callback
        self._state_provider = state_provider
        self._retention = retention
        self._max_state_bytes = max_state_bytes

        logger.debug("[AgentEntity] Initialized with agent type: %s", type(agent).__name__)

    @property
    def state(self) -> DurableAgentState:
        return self._state_provider.state

    @state.setter
    def state(self, value: DurableAgentState) -> None:
        self._state_provider.state = value

    def persist_state(self) -> None:
        self._state_provider.persist_state()

    def reset(self) -> None:
        self._state_provider.reset()

    def _is_error_response(self, entry: DurableAgentStateEntry) -> bool:
        """Check if a conversation history entry records a failed turn."""
        return isinstance(entry, DurableAgentStateErrorResponse)

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

        message = run_request.message
        session_id = self._state_provider.session_id
        correlation_id = run_request.correlation_id
        if not session_id:
            raise ValueError("Entity State Provider must provide a session_id")
        options: dict[str, Any] = dict(run_request.options)
        options.setdefault("response_format", run_request.response_format)
        if not run_request.enable_tool_calls:
            options.setdefault("tools", None)

        logger.debug("[AgentEntity.run] Received SessionId %s Message: %s", session_id, run_request)

        already_answered = self.state.try_get_agent_response(correlation_id)
        if already_answered is not None:
            # This exact request has already been answered. Entity signals are delivered at least
            # once, and every path mints a fresh correlation id per request, so a repeat is a
            # duplicate delivery rather than a caller deliberately asking again. Running the agent
            # a second time would spend another model call, re-run the tools, and produce a
            # different answer that nothing could collect: pollers read by correlation id and take
            # the first match, so the second response was already unreachable. Returning the
            # recorded answer is what turns at-least-once delivery into a single effect.
            logger.info(
                "[AgentEntity.run] Correlation id %s on session %s has already been answered, "
                "returning the recorded response rather than running the agent again.",
                correlation_id,
                session_id,
            )
            return already_answered

        durable_history = self._find_durable_history_provider()
        uses_context_pipeline = self._has_context_pipeline()
        # A property of the run rather than of the registration, since ``store`` is an ordinary
        # run option. The provider stays attached either way so core never injects one of its own.
        service_owns_history = service_stores_history(self.agent, options)

        state_request = DurableAgentStateRequest.from_run_request(run_request)
        if run_request.context_messages:
            state_request.messages = self._drop_already_stored(state_request.messages)
        self.state.data.conversation_history.append(state_request)

        # Some other store holds this conversation, either one the caller configured or the model
        # service itself, so our copy of what the user said is redundant. Keeping it would put the
        # same content under two retention, residency and deletion policies while only one of them
        # is the store actually being used. Forgotten *after* the run rather than before it,
        # because the run input is built from these same messages.
        forget_request_content = uses_context_pipeline and (durable_history is None or service_owns_history)

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

        try:
            if uses_context_pipeline:
                # The agent's own context providers supply prior turns - durable-backed history,
                # an external store (Cosmos/Redis/file), or the model service itself. Only the
                # newly received request messages are passed as run input, so history lives in
                # exactly one place and core providers work unchanged on the durable runtime.
                session = self._create_session()
                chat_messages = [
                    replayable_message
                    for m in state_request.messages
                    if (replayable_message := self._to_replayable_message(m)) is not None
                ]
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

            try:
                agent_run_response: AgentResponse = await self._invoke_agent(
                    run_kwargs=run_kwargs,
                    correlation_id=correlation_id,
                    session_id=session_id,
                    request_message=message,
                )
            except Exception as exc:
                if session is None or not _is_missing_previous_response(exc):
                    raise
                # The service is holding this conversation but will not accept the id it issued
                # for the previous turn. Measured against Azure OpenAI, a streamed response
                # reports its id before that response is readable, so the id is genuine and was
                # captured correctly, it just resolves a moment later. Re-sending the identical
                # request is enough to recover that, costs about a second, and needs nothing
                # stored. The same failure is handled the same way in Microsoft.Extensions.AI.
                #
                # An id that has genuinely expired cannot be recovered this way, and the error
                # looks identical, so those turns fail. Resending our own transcript would rescue
                # them, but only if the entity kept a full second copy of a conversation the
                # service is already holding, on every turn, against the chance of needing it.
                # Core does not make that trade and neither do we. If the case turns out to
                # matter, it comes back as an explicit opt-in rather than a silent cost.
                retried = await self._retry_rejected_conversation_id(
                    run_kwargs=run_kwargs,
                    correlation_id=correlation_id,
                    session_id=session_id,
                    request_message=message,
                    cause=exc,
                )
                if retried is None:
                    raise
                agent_run_response = retried

            state_response = DurableAgentStateResponse.from_run_response(correlation_id, agent_run_response)
            self.state.data.conversation_history.append(state_response)
            if forget_request_content:
                _forget_message_content(state_request.messages)
            self._capture_session(session)
            await self._enforce_retention()
            self.persist_state()

            return agent_run_response

        except Exception as exc:
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
            error_response = AgentResponse(
                messages=[error_message],
                created_at=datetime.now(tz=timezone.utc).isoformat(),
            )

            error_state_response = DurableAgentStateErrorResponse.from_run_response(correlation_id, error_response)
            self.state.data.conversation_history.append(error_state_response)
            if forget_request_content:
                _forget_message_content(state_request.messages)
            await self._enforce_retention()
            self.persist_state()

            return error_response

        finally:
            if binding_token is not None:
                unbind_durable_history(binding_token)

    async def _retry_rejected_conversation_id(
        self,
        *,
        run_kwargs: dict[str, Any],
        correlation_id: str,
        session_id: str,
        request_message: Any,
        cause: BaseException,
    ) -> AgentResponse | None:
        """Re-send an identical request whose conversation id the service refused.

        The refusal we are recovering from is a read-after-write gap rather than a lost
        conversation. Measured against Azure OpenAI, a streamed response reports its id in the
        completion event before that response is retrievable, so the very next turn can be
        rejected for naming an id that is perfectly valid and simply not readable yet. Waiting
        briefly and asking again is the cheapest thing that works, and it leaves the conversation
        continuing from the same point rather than restarting it from a resent transcript.

        A retry cannot rescue an id that has genuinely expired, and the error is identical either
        way, so the attempts are few and short. Exhausting them is not a failure, it is the signal
        to fall back to something that does not depend on the service still holding the thread.

        Args:
            run_kwargs: The unchanged arguments of the request that was refused.
            correlation_id: Correlation id of the in-flight request.
            session_id: Session the request belongs to.
            request_message: The originating message, for logging.
            cause: The refusal that triggered this, so a give-up is reported with its reason.

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
                )
            except Exception as retry_exc:
                if not _is_missing_previous_response(retry_exc):
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
        """Bound durable state before it is persisted, unless the caller asked to keep everything.

        This lives on the entity rather than the history provider because the entity records the
        conversation in every configuration, including external providers, service-managed agents
        and agents with no context pipeline. Those are exactly the cases with no other mitigation.
        """
        if self._retention == "keep_all":
            return
        await enforce_budget(self.state, max_state_bytes=self._max_state_bytes)

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

        The durable history provider's own slice is dropped before persisting: it is derived from
        ``conversation_history`` on every turn, so storing it would duplicate the transcript and
        let the copy drift from the record of truth. It is removed *before* serializing rather
        than after, because that slice holds the working message buffer and its position index,
        and serializing the whole transcript only to discard it is pure waste.

        Provider state is arbitrary, so the payload is checked before it replaces the last good
        one. Core neither raises nor warns on a value it cannot serialize, it passes the live
        object through, and the entity state provider serializes eagerly. An unusable payload
        would therefore fail the save, and fail it again from the error handler, masking whatever
        the agent actually returned.
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
        try:
            payload = cast("dict[str, Any]", to_dict())
        finally:
            if has_transient:
                cast("dict[str, Any]", session_state)[durable_history.source_id] = transient  # type: ignore[union-attr]

        try:
            json.dumps(payload)
        except (TypeError, ValueError) as exc:
            logger.warning(
                "[AgentEntity] Session state could not be serialized and was not persisted, so the "
                "previous turn's state is kept. A context provider is holding a value that is not "
                "JSON-compatible: %s",
                exc,
            )
            return
        self.state.data.session = payload

    def _drop_already_stored(self, messages: list[DurableAgentStateMessage]) -> list[DurableAgentStateMessage]:
        """Filter out chained conversation this entity has already recorded.

        A workflow node that runs more than once (for example in a cycle) receives the whole
        upstream conversation each time. Without filtering it re-records all of it on every visit.

        Filtering is by **position**, not by stored identity. The obvious check, "is this id
        already in my history", stops working the moment retention evicts anything: those ids
        leave the comparison set, the orchestrator re-sends them because its own conversation is
        never evicted, and the entity re-records exactly what was deleted. That oscillates instead
        of settling. A high-water mark per producing executor is unaffected by deletion, and is
        per executor rather than global because a fan-out gives two branches the same position.

        Messages without a workflow id fall back to the identity check, which is enough for them
        because nothing re-delivers them.

        The final message is always kept so the agent still receives an input.
        """
        ingested = dict(self.state.data.ingested_positions or {})
        seen: dict[str, int] = {}
        kept: list[DurableAgentStateMessage] = []

        known_ids = {
            stored.message_id
            for entry in self.state.data.conversation_history
            for stored in entry.messages
            if stored.message_id
        }

        for message in messages:
            marker = parse_workflow_message_id(message.message_id)
            if marker is not None:
                executor, position = marker
                seen[executor] = max(seen.get(executor, -1), position)
                if position <= ingested.get(executor, -1):
                    continue
            elif message.message_id and message.message_id in known_ids:
                continue
            kept.append(message)

        for executor, position in seen.items():
            ingested[executor] = max(ingested.get(executor, -1), position)
        if ingested:
            self.state.data.ingested_positions = ingested

        if not kept and messages:
            # Keep the newest message so the agent still has an input, but drop the id it shares
            # with the copy already in history. Two stored messages under one id collide in the
            # compaction position map, so annotations and pruning would target the wrong one.
            repeated = messages[-1]
            repeated.message_id = None
            return [repeated]
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
        session: Any = create_session(session_id=self._state_provider.core_session_id)
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

        Used whenever history cannot come from anywhere else: agents with no context pipeline,
        where the entity owns the conversation outright, and recovery for a service-managed agent
        whose stored conversation id the service would not accept.

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
            additional_properties=chat_message.additional_properties,
        )

    async def _invoke_agent(
        self,
        run_kwargs: dict[str, Any],
        correlation_id: str,
        session_id: str,
        request_message: str,
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

        # Try streaming first with run(stream=True)
        try:
            stream_candidate = run_callable(stream=True, **run_kwargs)
            if inspect.isawaitable(stream_candidate):
                stream_candidate = await stream_candidate

            return await self._consume_stream(
                stream=stream_candidate,
                callback_context=callback_context,
            )
        except TypeError as type_error:
            if "__aiter__" not in str(type_error) and "stream" not in str(type_error):
                raise
            logger.debug(
                "run(stream=True) returned a non-async result; falling back to run(): %s",
                type_error,
            )
        except Exception as stream_error:
            if _is_missing_previous_response(stream_error):
                # Falling back to run() would resend the id the service just refused and fail the
                # same way. Surface it so the caller can rebuild the request without that id.
                raise
            logger.warning(
                "run(stream=True) failed; falling back to run(): %s",
                stream_error,
                exc_info=True,
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
    ) -> AgentResponse:
        """Consume streaming responses and build the final AgentResponse."""
        updates: list[AgentResponseUpdate] = []

        async for update in stream:
            updates.append(update)
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
            callback_result = self.callback.on_streaming_response_update(update, context)
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
            callback_result = self.callback.on_agent_response(response, context)
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
