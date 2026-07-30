# Copyright (c) Microsoft. All rights reserved.

"""A core ``HistoryProvider`` backed by durable entity state.

This lets the durable runtime plug into the Agent Framework context-provider pipeline
instead of managing conversation history itself. Because the agent's own history
provider supplies context, core compaction (``CompactionProvider``) works unchanged and
its annotations are persisted alongside the messages in durable entity state - a single
stored copy, no side-car session blob.

See ADR-0032 (durable thread compaction).
"""

from __future__ import annotations

import copy
import logging
from collections.abc import Iterator, Sequence
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from agent_framework import HistoryProvider, InMemoryHistoryProvider, Message, SupportsAgentRun

from ._durable_agent_state import DurableAgentStateEntry, DurableAgentStateMessage, DurableAgentStateResponse

if TYPE_CHECKING:
    from ._entities import AgentEntityStateProviderMixin

logger = logging.getLogger("agent_framework.durabletask")

WORKING_BUFFER_KEY = "messages"
POSITIONS_KEY = "_positions"
EXCLUDED_KEY = "_excluded"


@dataclass
class DurableHistoryBinding:
    """Per-operation binding between a durable entity and the history provider."""

    state_provider: AgentEntityStateProviderMixin
    """The entity state provider whose conversation history backs the agent."""

    correlation_id: str | None = None
    """Correlation id of the in-flight request, whose entry is excluded from loaded history."""


_current_binding: ContextVar[DurableHistoryBinding | None] = ContextVar(
    "durable_history_binding",
    default=None,
)


def bind_durable_history(binding: DurableHistoryBinding) -> Token[DurableHistoryBinding | None]:
    """Bind the durable entity state for the current operation.

    Returns a token that must be passed to :func:`unbind_durable_history`.
    """
    return _current_binding.set(binding)


def unbind_durable_history(token: Token[DurableHistoryBinding | None]) -> None:
    """Release a binding created by :func:`bind_durable_history`."""
    _current_binding.reset(token)


def current_durable_history_binding() -> DurableHistoryBinding | None:
    """Return the binding for the current durable operation, if any."""
    return _current_binding.get()


class DurableHistoryProvider(HistoryProvider):
    """History provider whose store is the durable entity's conversation history.

    The durable entity remains the writer of record for requests and responses, so this
    provider does not append messages itself (``store_inputs``/``store_outputs`` are off).
    What it does provide is:

    * **load** - flattens persisted conversation history into ``Message`` objects, restoring
      any compaction annotations that were stored with them.
    * **flush** - writes annotations that compaction applied during the run back into the
      persisted messages, so compaction state survives across entity operations.

    Attributes:
        skip_excluded: When True, messages marked ``_excluded`` by compaction are omitted
            from the context loaded for the model. The messages remain in durable storage.
        prune_excluded: When True, excluded messages are physically removed from durable
            storage on flush. This is **lossy** and opt-in - it is what actually bounds the
            size of persisted state.
    """

    DEFAULT_SOURCE_ID = "durable_history"

    def __init__(
        self,
        source_id: str | None = None,
        *,
        skip_excluded: bool = True,
        prune_excluded: bool = False,
    ) -> None:
        """Initialize the durable history provider.

        Args:
            source_id: Unique identifier for this provider instance.
            skip_excluded: Omit compaction-excluded messages from loaded context.
            prune_excluded: Physically delete excluded messages from durable storage
                on flush. Lossy; disabled by default.
        """
        super().__init__(
            source_id=source_id or self.DEFAULT_SOURCE_ID,
            load_messages=True,
            # The durable entity owns appends to conversation history.
            store_inputs=False,
            store_outputs=False,
        )
        self.skip_excluded = skip_excluded
        self.prune_excluded = prune_excluded

    def _binding(self) -> DurableHistoryBinding | None:
        binding = current_durable_history_binding()
        if binding is None:
            logger.warning(
                "[DurableHistoryProvider] No durable binding is active; the provider yields no history. "
                "This provider only works inside a durable agent entity operation."
            )
        return binding

    def _replayable_entries(self, binding: DurableHistoryBinding) -> Iterator[tuple[DurableAgentStateEntry, int]]:
        """Yield (entry, message_index) pairs that participate in model context."""
        for entry in binding.state_provider.state.data.conversation_history:
            if isinstance(entry, DurableAgentStateResponse) and entry.is_error:
                continue
            if binding.correlation_id is not None and entry.correlation_id == binding.correlation_id:
                # The in-flight request is delivered as run input, not as history.
                continue
            for index in range(len(entry.messages)):
                yield entry, index

    @staticmethod
    def _to_message(stored: DurableAgentStateMessage) -> Message | None:
        """Convert a persisted message into one that is safe to replay to a chat client."""
        chat_message: Message = stored.to_chat_message()
        replayable = [content for content in chat_message.contents if content.type != "reasoning"]
        if not replayable:
            return None
        return Message(
            role=chat_message.role,
            contents=replayable,
            author_name=chat_message.author_name,
            message_id=stored.message_id,
            additional_properties=chat_message.additional_properties,
        )

    async def get_messages(
        self, session_id: str | None, *, state: dict[str, Any] | None = None, **kwargs: Any
    ) -> list[Message]:
        """Load conversation history from durable entity state."""
        binding = self._binding()
        if binding is None:
            return []

        loaded: list[Message] = []
        id_map: dict[str, tuple[DurableAgentStateEntry, int]] = {}
        for entry, index in self._replayable_entries(binding):
            stored = entry.messages[index]
            message = self._to_message(stored)
            if message is None:
                continue
            if not message.message_id:
                # Give every loaded message a stable identity so compaction results can be
                # reconciled back onto durable state on flush.
                message.message_id = f"durable_{id(entry):x}_{index}"
                stored.message_id = message.message_id
            loaded.append(message)
            id_map[message.message_id] = (entry, index)

        if state is not None:
            # Expose the loaded messages as the working buffer so CompactionProvider's
            # after_strategy can annotate them (core reads session.state[source_id]["messages"]).
            state[WORKING_BUFFER_KEY] = loaded
            state[POSITIONS_KEY] = id_map

        if self.skip_excluded:
            return [m for m in loaded if not m.additional_properties.get(EXCLUDED_KEY)]
        return list(loaded)

    async def save_messages(
        self,
        session_id: str | None,
        messages: Sequence[Message],
        *,
        state: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """No-op: the durable entity appends requests and responses to its own state."""
        return

    @staticmethod
    def _is_service_managed(session: Any) -> bool:
        """Return whether the conversation is stored by the model service, not by us."""
        return bool(getattr(session, "service_session_id", None))

    async def before_run(
        self,
        *,
        agent: Any,
        session: Any,
        context: Any,
        state: dict[str, Any],
    ) -> None:
        """Load durable history into context, unless the service owns the conversation."""
        if self._is_service_managed(session):
            logger.debug("[DurableHistoryProvider] Session is service-managed; skipping durable history load.")
            return
        await super().before_run(agent=agent, session=session, context=context, state=state)

    async def after_run(
        self,
        *,
        agent: Any,
        session: Any,
        context: Any,
        state: dict[str, Any],
    ) -> None:
        """Flush compaction annotations from the working buffer into durable state."""
        if self._is_service_managed(session):
            return
        self.flush(state)

    def flush(self, state: dict[str, Any]) -> None:
        """Persist compaction results back into durable entity state.

        Reconciliation is by ``message_id`` rather than position, so strategies that
        *insert* messages (for example ``ToolResultCompactionStrategy``, which replaces a
        tool-call group with a summary) are handled as well as ones that only annotate.

        Args:
            state: The provider-scoped session state holding the working buffer.
        """
        binding = current_durable_history_binding()
        if binding is None:
            return

        raw_buffer = state.get(WORKING_BUFFER_KEY)
        raw_positions = state.get(POSITIONS_KEY)
        if not isinstance(raw_buffer, list) or not isinstance(raw_positions, dict):
            return
        buffer = cast("list[Message]", raw_buffer)
        stored_by_id = cast("dict[str, tuple[DurableAgentStateEntry, int]]", raw_positions)

        pruned: list[tuple[DurableAgentStateEntry, int]] = []
        # Messages that compaction added (summaries) are inserted right after the last
        # known message so ordering in durable state matches the compacted conversation.
        last_known: tuple[DurableAgentStateEntry, int] | None = None

        for message in buffer:
            annotations = dict(message.additional_properties) if message.additional_properties else None
            position = stored_by_id.get(message.message_id) if message.message_id else None

            if position is None:
                inserted = self._insert_new_message(binding, message, after=last_known)
                if inserted is not None:
                    last_known = inserted
                continue

            entry, index = position
            stored = entry.messages[index]
            stored.extension_data = annotations
            last_known = position
            if self.prune_excluded and annotations and annotations.get(EXCLUDED_KEY):
                pruned.append(position)

        if pruned:
            self._prune(binding, pruned)

        binding.state_provider.persist_state()

    @staticmethod
    def _insert_new_message(
        binding: DurableHistoryBinding,
        message: Message,
        *,
        after: tuple[DurableAgentStateEntry, int] | None,
    ) -> tuple[DurableAgentStateEntry, int] | None:
        """Persist a message that compaction produced (for example a summary)."""
        stored = DurableAgentStateMessage.from_chat_message(message)
        if after is not None:
            entry, index = after
            entry.messages.insert(index + 1, stored)
            return entry, index + 1

        history = binding.state_provider.state.data.conversation_history
        if not history:
            return None
        first = history[0]
        first.messages.insert(0, stored)
        return first, 0

    @staticmethod
    def _prune(binding: DurableHistoryBinding, pruned: list[tuple[DurableAgentStateEntry, int]]) -> None:
        """Physically remove excluded messages (and any entries left empty)."""
        by_entry: dict[int, list[int]] = {}
        for entry, index in pruned:
            by_entry.setdefault(id(entry), []).append(index)

        for entry, _ in pruned:
            indexes = by_entry.pop(id(entry), None)
            if indexes is None:
                continue
            for index in sorted(indexes, reverse=True):
                del entry.messages[index]

        history = binding.state_provider.state.data.conversation_history
        remaining = [entry for entry in history if entry.messages]
        if len(remaining) != len(history):
            history[:] = remaining


def _service_stores_history(agent: Any) -> bool:
    """Return whether the agent's client keeps conversation history server-side."""
    client = getattr(agent, "client", None)
    return bool(getattr(client, "STORES_BY_DEFAULT", False))


def ensure_durable_history(agent: SupportsAgentRun, *, prune_history: bool = False) -> SupportsAgentRun:
    """Back an agent's conversation history with durable entity state.

    Lets a user register an agent that already works in core and get durable behavior with no
    configuration change. The agent is never mutated: when a substitution is needed a shallow
    copy is returned with its own provider list.

    The rules mirror what core would do, so behavior stays predictable:

    * **No history provider** - a :class:`DurableHistoryProvider` is added. It uses the same
      ``source_id`` core's auto-injected provider would have, so a ``CompactionProvider`` left on
      its defaults still finds it.
    * **In-memory history** - replaced by a :class:`DurableHistoryProvider` carrying the *same*
      ``source_id`` and ``skip_excluded``, so any compaction wired to it keeps working untouched.
    * **Any other history provider** (Cosmos, Redis, file, custom) - left alone. The user chose
      where their conversation lives; durable still provides execution durability.
    * **Service-managed history** - left alone. The model service owns the conversation.
    * **Agents without the core context pipeline** - left alone; the entity falls back to
      replaying its own persisted history.

    Args:
        agent: The agent being registered with the durable runtime.

    Keyword Args:
        prune_history: When True, the injected provider physically deletes messages that
            compaction excluded, bounding durable storage. This is a **lossy retention policy**
            and is off by default. It only affects providers this function creates.

    Returns:
        The agent to run, either unchanged or a shallow copy with durable-backed history.
    """
    providers = getattr(agent, "context_providers", None)
    if not isinstance(providers, (list, tuple)):
        return agent

    if _service_stores_history(agent):
        logger.debug(
            "[DurableHistoryProvider] Agent %s stores history service-side; leaving providers unchanged.",
            getattr(agent, "name", type(agent).__name__),
        )
        return agent

    provider_list = list(cast("Sequence[Any]", providers))
    existing = next(
        (p for p in provider_list if isinstance(p, HistoryProvider) and p.load_messages),
        None,
    )

    if existing is None:
        # Match the source_id core's auto-injected provider would use so default-wired
        # compaction keeps resolving.
        updated = [
            DurableHistoryProvider(
                source_id=InMemoryHistoryProvider.DEFAULT_SOURCE_ID,
                prune_excluded=prune_history,
            ),
            *provider_list,
        ]
    elif isinstance(existing, InMemoryHistoryProvider):
        replacement = DurableHistoryProvider(
            source_id=existing.source_id,
            skip_excluded=existing.skip_excluded,
            prune_excluded=prune_history,
        )
        updated = [replacement if p is existing else p for p in provider_list]
    else:
        # A deliberate storage choice (external or custom); do not override it.
        return agent

    try:
        clone = copy.copy(agent)
        clone.context_providers = updated  # type: ignore[attr-defined]
    except Exception:
        logger.warning(
            "[DurableHistoryProvider] Could not attach durable history to agent %s; "
            "falling back to replaying persisted history.",
            getattr(agent, "name", type(agent).__name__),
            exc_info=True,
        )
        return agent

    return clone
