# Copyright (c) Microsoft. All rights reserved.

"""Durable Task entity implementations for Microsoft Agent Framework."""

from __future__ import annotations

import inspect
import logging
import warnings
from collections.abc import Sequence
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
)
from durabletask.entities import DurableEntity

from ._callbacks import AgentCallbackContext, AgentResponseCallbackProtocol
from ._durable_agent_state import (
    DurableAgentState,
    DurableAgentStateEntry,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
)
from ._history_provider import (
    DurableHistoryBinding,
    DurableHistoryProvider,
    bind_durable_history,
    ensure_durable_history,
    unbind_durable_history,
)
from ._models import RunRequest

logger = logging.getLogger("agent_framework.durabletask")

# Keys produced by core's ``AgentSession.to_dict()``.
_SESSION_ID_KEY = "session_id"
_SESSION_STATE_KEY = "state"


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

    @property
    def session_id(self) -> str:
        return self._get_session_id_from_entity()

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
        prune_history: bool = False,
    ) -> None:
        # Back the agent's conversation history with durable entity state so an agent that
        # already works in core runs durably without any configuration change.
        self.agent = ensure_durable_history(agent, prune_history=prune_history)
        self.callback = callback
        self._state_provider = state_provider

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
        """Check if a conversation history entry is an error response."""
        if isinstance(entry, DurableAgentStateResponse):
            return entry.is_error
        return False

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

        state_request = DurableAgentStateRequest.from_run_request(run_request)
        if run_request.context_messages:
            state_request.messages = self._drop_already_stored(state_request.messages)
        self.state.data.conversation_history.append(state_request)

        durable_history = self._find_durable_history_provider()
        uses_context_pipeline = self._has_context_pipeline()
        binding_token = (
            bind_durable_history(
                DurableHistoryBinding(state_provider=self._state_provider, correlation_id=correlation_id)
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
                chat_messages = [
                    replayable_message
                    for entry in self.state.data.conversation_history
                    if not self._is_error_response(entry)
                    for m in entry.messages
                    if (replayable_message := self._to_replayable_message(m)) is not None
                ]
                run_kwargs = {"messages": chat_messages, "options": options}

            agent_run_response: AgentResponse = await self._invoke_agent(
                run_kwargs=run_kwargs,
                correlation_id=correlation_id,
                session_id=session_id,
                request_message=message,
            )

            state_response = DurableAgentStateResponse.from_run_response(correlation_id, agent_run_response)
            self.state.data.conversation_history.append(state_response)
            self._capture_session(session)
            self.persist_state()

            return agent_run_response

        except Exception as exc:
            logger.exception("[AgentEntity.run] Agent execution failed.")

            error_message = Message(
                role="assistant", contents=[Content.from_error(message=str(exc), error_code=type(exc).__name__)]
            )
            error_response = AgentResponse(
                messages=[error_message],
                created_at=datetime.now(tz=timezone.utc).isoformat(),
            )

            error_state_response = DurableAgentStateResponse.from_run_response(correlation_id, error_response)
            error_state_response.is_error = True
            self.state.data.conversation_history.append(error_state_response)
            self.persist_state()

            return error_response

        finally:
            if binding_token is not None:
                unbind_durable_history(binding_token)

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
        let the copy drift from the record of truth.
        """
        if session is None:
            return
        to_dict = getattr(session, "to_dict", None)
        if not callable(to_dict):
            return

        payload = cast("dict[str, Any]", to_dict())
        state = payload.get(_SESSION_STATE_KEY)
        durable_history = self._find_durable_history_provider()
        if isinstance(state, dict) and durable_history is not None:
            cast("dict[str, Any]", state).pop(durable_history.source_id, None)
        self.state.data.session = payload

    def _drop_already_stored(self, messages: list[DurableAgentStateMessage]) -> list[DurableAgentStateMessage]:
        """Filter out upstream context messages this entity has already recorded.

        A workflow node that runs more than once (for example in a cycle) receives the whole
        upstream conversation each time. Messages carrying an id that is already in this
        entity's history are dropped so the conversation is not duplicated. The final message
        is always kept so the agent still receives an input.
        """
        known_ids = {
            stored.message_id
            for entry in self.state.data.conversation_history
            for stored in entry.messages
            if stored.message_id
        }
        if not known_ids:
            return messages

        deduped = [m for m in messages if not m.message_id or m.message_id not in known_ids]
        if not deduped and messages:
            return [messages[-1]]
        return deduped

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
        file) key their storage on ``session.session_id``; with a freshly generated id they would
        read and write a different key every turn and never see prior history.
        """
        create_session = getattr(self.agent, "create_session", None)
        if not callable(create_session):
            raise TypeError(
                f"Agent {type(self.agent).__name__} exposes context providers but does not support create_session()."
            )
        session: Any = create_session(session_id=self._state_provider.session_id)
        self._restore_session(session)
        return session

    def _restore_session(self, session: Any) -> None:
        """Apply the previous turn's session state onto a freshly created session.

        The agent's own ``create_session`` is used so its session type is preserved; only the
        state bag and the service conversation id are carried over.
        """
        stored = self.state.data.session
        if not stored or _SESSION_ID_KEY not in stored:
            return

        restored = AgentSession.from_dict(dict(stored))
        session.state.update(restored.state)
        if getattr(session, "service_session_id", None) is None:
            session.service_session_id = restored.service_session_id

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
