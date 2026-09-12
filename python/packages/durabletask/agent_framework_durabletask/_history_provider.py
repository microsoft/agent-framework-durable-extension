# Copyright (c) Microsoft. All rights reserved.

"""A core ``HistoryProvider`` backed by durable entity state.

Core history hooks stage transcript appends, while compaction annotations and summaries
are reconciled from a transient working buffer. The entity commits the transcript together
with its independent delivery and control state at the operation boundary.

See ADR-0032 (durable thread compaction).
"""

from __future__ import annotations

import copy
import logging
from collections.abc import Iterator, Mapping, Sequence
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, cast

from agent_framework import (
    GROUP_ANNOTATION_KEY,
    GROUP_ID_KEY,
    SUMMARIZED_BY_SUMMARY_ID_KEY,
    SUMMARY_OF_MESSAGE_IDS_KEY,
    AgentResponse,
    HistoryProvider,
    InMemoryHistoryProvider,
    Message,
    SessionContext,
    SupportsAgentRun,
    annotate_message_groups,
)

from ._durable_agent_state import (
    DurableAgentStateCompaction,
    DurableAgentStateEntry,
    DurableAgentStateEntryJsonType,
    DurableAgentStateErrorResponse,
    DurableAgentStateFunctionCallContent,
    DurableAgentStateFunctionResultContent,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
    DurableAgentStateUnknownEntry,
    DurableAgentStateUsage,
)
from ._response_utils import is_terminal_agent_response
from ._retention_telemetry import eager_state_size, record_retention

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
    """Owner of every request and response appended during this operation."""

    service_owns_history: bool = False
    """Whether the model service is holding the conversation for *this* run.

    The provider stays available when no external primary was selected, so that core never injects
    a separate in-memory transcript outside retention's reach. A
    service-backed run continues the conversation by id rather than by resending it, so loading
    history here as well would hand the model the whole transcript on top of the copy the service
    already has. Whoever owns a given run is only known once its options are resolved, which is
    why this rides on the binding rather than on the provider.
    """

    append_ordinal: int = 0
    """Operation-local append counter used to give anonymous messages stable stored identities."""

    pending_inputs: list[Message] = field(default_factory=lambda: list[Message](), repr=False)
    """Detached inputs of the latest per-service call, never serialized into session state."""


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
    """Core history hooks backed by the entity's staged transcript.

    Loading restores persisted messages, IDs and compaction annotations. After-run hooks append
    the configured inputs, context and outputs, once per core hook. Reconciliation writes working
    buffer annotations and summaries back to the transcript without committing to the backend.
    The entity owns response delivery and the final flush after all core after-run providers.

    Attributes:
        skip_excluded: When True, messages marked ``_excluded`` by compaction are omitted
            from the context loaded for the model. The messages remain in durable storage.
        prune_excluded: When True, excluded messages are physically removed from durable
            storage on flush, preserving system messages and the newest/current exchange.
            This is **lossy** and opt-in, independently of any configured pressure budget.
    """

    DEFAULT_SOURCE_ID = "durable_history"

    def __init__(
        self,
        source_id: str | None = None,
        *,
        store_inputs: bool = True,
        store_outputs: bool = True,
        store_context_messages: bool = False,
        store_context_from: set[str] | None = None,
        skip_excluded: bool = True,
        prune_excluded: bool | None = None,
    ) -> None:
        """Initialize the durable history provider.

        Args:
            source_id: Unique identifier for this provider instance.
            store_inputs: Store each hook's input messages.
            store_outputs: Store each hook's response messages.
            store_context_messages: Store context contributed by other providers.
            store_context_from: Restrict stored context to these source identifiers, when set.
            skip_excluded: Omit compaction-excluded messages from loaded context.
            prune_excluded: Physically delete excluded messages from durable storage on flush.
                Lossy, so it is off unless asked for. Leaving it unset defers to the entity's
                ``retention`` mode, which resolves it when the provider is prepared for a run.
                Passing it explicitly pins the behaviour and retention will not override it, which
                is what lets a caller who wires this provider by hand opt in or out independently
                of the mode. Unset and unresolved, as when this provider is not the one the entity
                prepared, it does not prune.
        """
        super().__init__(
            source_id=source_id or self.DEFAULT_SOURCE_ID,
            load_messages=True,
            store_inputs=store_inputs,
            store_outputs=store_outputs,
            store_context_messages=store_context_messages,
            store_context_from=set(store_context_from) if store_context_from is not None else None,
        )
        self.skip_excluded = skip_excluded
        self.prune_excluded = prune_excluded

    def _binding(self) -> DurableHistoryBinding | None:
        binding = current_durable_history_binding()
        if binding is None:
            logger.warning(
                "[DurableHistoryProvider] No durable binding is active, so the provider yields no history. "
                "This provider only works inside a durable agent entity operation."
            )
        return binding

    def _replayable_entries(self, binding: DurableHistoryBinding) -> Iterator[tuple[DurableAgentStateEntry, int]]:
        """Yield (entry, message_index) pairs that participate in model context."""
        # A tool loop must see messages saved by earlier calls in this same operation.
        # The entity does not pre-append pipeline inputs, so there is no current request to hide.
        yield from replayable_entries(binding.state_provider.state.data.conversation_history)

    @staticmethod
    def _synthetic_message_id(entry: DurableAgentStateEntry, index: int) -> str:
        """Build a deterministic ID candidate for a legacy message.

        The id comes from persisted fields, so a cold start or a retried flush regenerates the
        same value. An id derived from object identity would not, and a recycled address could
        collide with an id an earlier run already persisted.

        Args:
            entry: History entry holding the message.
            index: Position of the message within that entry.

        Returns:
            An ID candidate, disambiguated against the current history by ``_positions``.
        """
        # A request and its response share a correlation id, so the entry type is what tells the
        # two sides of an exchange apart.
        scope = entry.correlation_id or entry.created_at.isoformat()
        kind = entry.json_type.value if isinstance(entry.json_type, DurableAgentStateEntryJsonType) else entry.json_type
        return f"durable_{kind}_{scope}_{index}"

    @staticmethod
    def _to_message(stored: DurableAgentStateMessage) -> Message | None:
        """Convert a persisted message into one that is safe to replay to a chat client."""
        chat_message: Message = copy.deepcopy(stored).to_chat_message()
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

    @staticmethod
    def _unique_message_id(candidate: str, reserved: set[str]) -> str:
        """Disambiguate generated identities, including collisions with caller-supplied IDs."""
        message_id = candidate
        revision = 0
        while message_id in reserved:
            revision += 1
            message_id = f"{candidate}_{revision}"
        reserved.add(message_id)
        return message_id

    def _positions(self, binding: DurableHistoryBinding) -> dict[str, tuple[DurableAgentStateEntry, int]]:
        """Index current storage, repairing anonymous or duplicate identities in legacy entries."""
        history = binding.state_provider.state.data.conversation_history
        reserved = {message.message_id for entry in history for message in entry.messages if message.message_id}
        positions: dict[str, tuple[DurableAgentStateEntry, int]] = {}
        for entry, index in self._replayable_entries(binding):
            stored = entry.messages[index]
            if not stored.message_id or stored.message_id in positions:
                stored.message_id = self._unique_message_id(self._synthetic_message_id(entry, index), reserved)
            positions[stored.message_id] = (entry, index)
        return positions

    async def get_messages(
        self, session_id: str | None, *, state: dict[str, Any] | None = None, **kwargs: Any
    ) -> list[Message]:
        """Load conversation history from durable entity state."""
        binding = self._binding()
        if binding is None:
            return []

        if binding.service_owns_history:
            # The service is holding this conversation and core will continue it by id. Returning
            # history as well would send the model everything twice. The provider is still
            # attached, which is what keeps core from injecting one whose state nothing bounds.
            return []

        id_map = self._positions(binding)
        loaded: list[Message] = []
        for entry, index in self._replayable_entries(binding):
            stored = entry.messages[index]
            message = self._to_message(stored)
            if message is None:
                continue
            loaded.append(message)

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
        """Stage a generic message batch as a request entry, without committing entity state."""
        binding = self._binding()
        if binding is None or binding.service_owns_history:
            return
        if state is not None:
            self.flush(state)
        self._append_messages(binding, messages, state=state)

    def _append_messages(
        self,
        binding: DurableHistoryBinding,
        messages: Sequence[Message],
        *,
        state: dict[str, Any] | None,
        response: AgentResponse | None = None,
    ) -> None:
        """Append one hook batch, exposing only nonterminal batches to core compaction.

        Terminal outputs are excluded from this provider's local model history, not from
        opaque external transcripts. Inputs remain separate accepted receipts, and earlier
        per-call appends are not rewritten when a later response is terminal.
        """
        if not messages or binding.service_owns_history:
            return
        if state is not None and WORKING_BUFFER_KEY not in state:
            # Direct save_messages callers need the same complete compaction buffer as callers
            # that loaded through before_run. Do not replace an already annotated buffer.
            state[POSITIONS_KEY] = self._positions(binding)
            state[WORKING_BUFFER_KEY] = [
                message
                for entry, index in self._replayable_entries(binding)
                if (message := self._to_message(entry.messages[index])) is not None
            ]
        created_at = datetime.now(tz=timezone.utc)
        response_type: type[DurableAgentStateResponse] = (
            DurableAgentStateErrorResponse
            if response is not None and is_terminal_agent_response(response)
            else DurableAgentStateResponse
        )
        kind = DurableAgentStateEntryJsonType.REQUEST if response is None else response_type.JSON_TYPE
        stored_messages, working_messages = self._copy_append_messages(binding, messages, kind, created_at)
        entry: DurableAgentStateEntry
        if response is None:
            entry = DurableAgentStateRequest(binding.correlation_id, created_at, stored_messages)
        else:
            entry = response_type(
                binding.correlation_id,
                created_at,
                stored_messages,
                usage=copy.deepcopy(DurableAgentStateUsage.from_usage(response.usage_details)),
            )
        binding.state_provider.state.data.conversation_history.append(entry)
        if state is not None:
            buffer = cast("list[Message]", state.setdefault(WORKING_BUFFER_KEY, []))
            if not isinstance(entry, DurableAgentStateErrorResponse):
                # Otherwise flush would mistake these non-replayable outputs for new summaries.
                buffer.extend(working_messages)
            state[POSITIONS_KEY] = self._positions(binding)

    def _copy_append_messages(
        self,
        binding: DurableHistoryBinding,
        messages: Sequence[Message],
        kind: DurableAgentStateEntryJsonType,
        created_at: datetime,
    ) -> tuple[list[DurableAgentStateMessage], list[Message]]:
        """Allocate stored identities without changing input messages or caller responses."""
        history = binding.state_provider.state.data.conversation_history
        used = {message.message_id for entry in history for message in entry.messages if message.message_id}
        reserved = used | {message.message_id for message in messages if message.message_id}
        scope = binding.correlation_id or created_at.isoformat()
        ordinal = binding.append_ordinal
        binding.append_ordinal += 1
        stored_messages: list[DurableAgentStateMessage] = []
        working_messages: list[Message] = []
        for index, message in enumerate(messages):
            # Conversion can retain nested tool payloads, so neither stored content nor the
            # compaction working copy may share those objects with the caller or each other.
            stored = DurableAgentStateMessage.from_chat_message(copy.deepcopy(message))
            receipt = getattr(message, "_durable_ingestion_receipt", None)
            if (
                kind == DurableAgentStateEntryJsonType.REQUEST
                and isinstance(receipt, tuple)
                and len(cast("tuple[Any, ...]", receipt)) == 2
            ):
                occurrence, fingerprint = cast("tuple[str, str]", receipt)
                stored.ingestion_occurrence = occurrence
                stored.ingestion_identity = fingerprint
            if not stored.message_id or stored.message_id in used:
                prefix = "durable_revision" if stored.message_id else "durable"
                candidate = f"{prefix}_{kind.value}_{scope}_{ordinal}_{index}"
                stored.message_id = self._unique_message_id(candidate, reserved)
            used.add(stored.message_id)
            working = copy.deepcopy(message)
            working.message_id = stored.message_id
            stored_messages.append(stored)
            working_messages.append(working)
        return stored_messages, working_messages

    async def before_run(
        self,
        *,
        agent: Any,
        session: Any,
        context: Any,
        state: dict[str, Any],
    ) -> None:
        """Load durable history into context, unless the service owns the conversation."""
        binding = current_durable_history_binding()
        if binding is not None:
            binding.pending_inputs.clear()
            if binding.service_owns_history:
                return
            if self.store_inputs and getattr(agent, "require_per_service_call_history_persistence", False):
                # Capture only. Core still decides whether the after-run persistence hook runs.
                binding.pending_inputs = copy.deepcopy(context.input_messages)
        await super().before_run(agent=agent, session=session, context=context, state=state)

    def _get_context_messages_to_store(self, context: SessionContext) -> list[Message]:
        # Our own contribution is already persisted. Core's in-memory save deduplicates it,
        # but durable appends allocate new identities, so exclude it even from explicit masks.
        if not self.store_context_messages:
            return []
        return context.get_messages(sources=self.store_context_from, exclude_sources={self.source_id})

    async def after_run(
        self,
        *,
        agent: Any,
        session: Any,
        context: Any,
        state: dict[str, Any],
    ) -> None:
        """Reconcile compaction, then append exactly the messages selected for this core hook."""
        binding = self._binding()
        if binding is None:
            return
        if binding.service_owns_history:
            binding.pending_inputs.clear()
            return
        self.flush(state)
        request_messages = self._get_context_messages_to_store(context)
        if self.store_inputs:
            request_messages.extend(context.input_messages)
        self._append_messages(binding, request_messages, state=state)
        if self.store_outputs and context.response and context.response.messages:
            self._append_messages(binding, context.response.messages, state=state, response=context.response)
        binding.pending_inputs.clear()

    def finalize_failed_run(self, state: dict[str, Any]) -> None:
        """Stage actual tool results left unsaved when a later service call fails.

        Call from the entity before its final flush, with the operation binding still active.
        Only result-only tool messages answering unresolved calls already stored under this
        correlation are eligible. Fresh requests, results for earlier correlations and calls whose
        persistence core deferred or disabled do not authorize an append. No backend write occurs.

        Args:
            state: The provider-scoped session state holding the working buffer.
        """
        binding = current_durable_history_binding()
        if binding is None:
            return
        pending_inputs = binding.pending_inputs
        binding.pending_inputs = []
        if (
            not pending_inputs
            or not self.store_inputs
            or binding.service_owns_history
            or binding.correlation_id is None
        ):
            return

        pending_calls: set[str] = set()
        for entry, index in self._replayable_entries(binding):
            if entry.correlation_id != binding.correlation_id:
                continue
            for content in entry.messages[index].contents:
                if isinstance(content, DurableAgentStateFunctionCallContent):
                    pending_calls.add(content.call_id)
                elif isinstance(content, DurableAgentStateFunctionResultContent):
                    pending_calls.discard(content.call_id)

        messages: list[Message] = []
        for message in pending_inputs:
            if message.role != "tool":
                continue
            result_ids = {
                content.call_id for content in message.contents if content.type == "function_result" and content.call_id
            }
            if result_ids and len(result_ids) == len(message.contents) and result_ids <= pending_calls:
                # Keep the entire original message so its ingestion hash still identifies the
                # caller's input, even if append allocates a different stored message ID.
                messages.append(message)
                pending_calls.difference_update(result_ids)
        self._append_messages(binding, messages, state=state)

    def flush(self, state: dict[str, Any]) -> None:
        """Apply compaction results to durable entity state.

        Reconciliation is by ``message_id`` rather than position, so strategies that
        *insert* messages (for example ``ToolResultCompactionStrategy``, which replaces a
        tool-call group with a summary) are handled as well as ones that only annotate.
        Previously loaded messages removed from the list become exclusions, not implicit
        permission to physically delete them. Opt-in pruning still applies its atomic-group floor.

        These edits affect only cached state. Repeated flushes reconcile annotations without
        repeating appends or strategies. The entity performs the final flush while this operation's
        binding is still active, after all core after-run providers and before its single commit.

        Args:
            state: The provider-scoped session state holding the working buffer.
        """
        binding = current_durable_history_binding()
        if binding is None or binding.service_owns_history:
            return

        raw_buffer = state.get(WORKING_BUFFER_KEY)
        raw_positions = state.get(POSITIONS_KEY)
        if not isinstance(raw_buffer, list):
            return
        buffer = cast("list[Message]", raw_buffer)
        previous_ids: set[str] = set()
        if isinstance(raw_positions, dict):
            previous_ids.update(cast("dict[str, Any]", raw_positions))
        # Positions can refer to entries replaced by pressure eviction, or indices invalidated
        # by an earlier flush. Only the previous keys are useful for recognizing removed messages.
        stored_by_id = self._positions(binding)

        # Messages that compaction added (summaries) are inserted right after the last
        # known message so ordering in durable state matches the compacted conversation.
        last_known: tuple[DurableAgentStateEntry, int] | None = None

        for message in buffer:
            position = stored_by_id.get(message.message_id) if message.message_id else None
            summary_ids = self._summary_original_ids(message)
            summary_revision = False
            if position is not None and summary_ids is not None:
                owner, index = position
                original = self._to_message(owner.messages[index])
                # Compare the replayed storage shape on both sides. Metadata discarded by
                # content conversion must not make every later flush look like a new summary.
                working = self._to_message(DurableAgentStateMessage.from_chat_message(copy.deepcopy(message)))
                original_payload = original.to_dict() if original is not None else {}
                working_payload = working.to_dict() if working is not None else {}
                original_payload.pop("additional_properties", None)
                working_payload.pop("additional_properties", None)
                # Core's summary_{len(messages)} can recur after pruning. Different source
                # messages identify a new summary even when the generated body is identical.
                original_ids = self._summary_original_ids(original) if original is not None else None
                summary_revision = original_payload != working_payload or original_ids != summary_ids

            if position is None or summary_revision:
                if not summary_revision and message.message_id in previous_ids:
                    # This was persisted before, not a newly generated summary. Never resurrect
                    # a message removed since the working buffer was assembled.
                    continue
                original_id = message.message_id
                position = self._insert_new_message(binding, message, after=last_known)
                if original_id and original_id != message.message_id and summary_ids is not None:
                    group = message.additional_properties.get(GROUP_ANNOTATION_KEY)
                    if isinstance(group, dict):
                        group[GROUP_ID_KEY] = f"group_{message.message_id}"
                    # Only the sources named by this summary now point to its new ID. Older
                    # sources may still point to the old summary, including when that summary
                    # is itself a source here. Its forward ID must therefore remain unchanged.
                    for source in buffer:
                        if source is message or source.message_id not in summary_ids:
                            continue
                        source_group = source.additional_properties.get(GROUP_ANNOTATION_KEY)
                        if (
                            isinstance(source_group, dict)
                            and cast("dict[str, Any]", source_group).get(SUMMARIZED_BY_SUMMARY_ID_KEY) == original_id
                        ):
                            source_group[SUMMARIZED_BY_SUMMARY_ID_KEY] = message.message_id
                        if source.additional_properties.get(SUMMARIZED_BY_SUMMARY_ID_KEY) == original_id:
                            source.additional_properties[SUMMARIZED_BY_SUMMARY_ID_KEY] = message.message_id
                stored_by_id = self._positions(binding)
            last_known = position

        # Link repair can touch sources that precede an inserted summary. Persist annotations
        # only after every insertion, using the final positions after any entry splits.
        for message in buffer:
            position = stored_by_id.get(message.message_id) if message.message_id else None
            if position is None:
                continue
            entry, index = position
            stored = entry.messages[index]
            stored.extension_data = (
                copy.deepcopy(message.additional_properties) if message.additional_properties else None
            )

        # A strategy may shrink the list without setting _excluded. Compare final identities
        # after summary revision IDs have been allocated, so replacing a summary does not leave
        # its old revision active. Preserve stored metadata and backlinks on absent messages.
        remaining_ids = {message.message_id for message in buffer if message.message_id}
        for message_id in previous_ids - remaining_ids:
            position = stored_by_id.get(message_id)
            if position is not None:
                entry, index = position
                stored = entry.messages[index]
                if self._to_message(stored) is None:
                    # Empty/non-replayable payloads were never exposed to the strategy.
                    continue
                stored.extension_data = {**(stored.extension_data or {}), EXCLUDED_KEY: True}

        if self.prune_excluded:
            # Resolve owners after all insertions. Splitting a multi-message entry may have
            # moved a previously annotated message into the tail entry.
            self._prune(
                binding,
                [
                    (entry, entry.messages[index])
                    for entry, index in stored_by_id.values()
                    if (entry.messages[index].extension_data or {}).get(EXCLUDED_KEY)
                ],
            )
        stored_by_id = self._positions(binding)
        buffer[:] = [message for message in buffer if message.message_id in stored_by_id]
        state[POSITIONS_KEY] = stored_by_id

    @staticmethod
    def _summary_original_ids(message: Message) -> list[str] | None:
        """Recognize Core's summary links, also accepting top-level custom-strategy links."""
        group = message.additional_properties.get(GROUP_ANNOTATION_KEY)
        original_ids = (
            cast("Mapping[str, Any]", group).get(SUMMARY_OF_MESSAGE_IDS_KEY) if isinstance(group, Mapping) else None
        )
        if original_ids is None:
            original_ids = message.additional_properties.get(SUMMARY_OF_MESSAGE_IDS_KEY)
        return cast("list[str]", original_ids) if isinstance(original_ids, list) else None

    def _insert_new_message(
        self,
        binding: DurableHistoryBinding,
        message: Message,
        *,
        after: tuple[DurableAgentStateEntry, int] | None,
    ) -> tuple[DurableAgentStateEntry, int]:
        """Persist a message compaction produced, such as a summary, as an entry of its own.

        It takes its place in conversation order, but as a compaction entry rather than inside
        whichever request or response it happened to follow. Folding it into a response made it
        part of that response, so a caller polling that correlation was handed back a summary the
        agent never produced.

        Having its own entry also means nothing downstream has to be told to skip it. It is not a
        response, so the lookup that serves waiting callers cannot match it.
        """
        history = binding.state_provider.state.data.conversation_history
        created_at = datetime.now(tz=timezone.utc)
        stored, _ = self._copy_append_messages(
            binding,
            [message],
            DurableAgentStateEntryJsonType.COMPACTION,
            created_at,
        )
        message.message_id = stored[0].message_id
        entry = DurableAgentStateCompaction(
            created_at=created_at,
            messages=stored,
        )

        if after is not None:
            owner, message_index = after
            position = next(index for index, candidate in enumerate(history) if candidate is owner) + 1
            if message_index + 1 < len(owner.messages):
                # [a, b] + a summary after a must become [a], [summary], [b], not
                # [a, b], [summary]. Keep message identities while detaching envelope metadata.
                tail = copy.copy(owner)
                tail.messages = owner.messages[message_index + 1 :]
                tail.extension_data = copy.deepcopy(owner.extension_data)
                tail.unknown_fields = copy.deepcopy(owner.unknown_fields)
                if isinstance(tail, DurableAgentStateRequest):
                    tail.response_schema = copy.deepcopy(tail.response_schema)
                if isinstance(tail, DurableAgentStateResponse):
                    tail.usage = copy.deepcopy(tail.usage)
                owner.messages = owner.messages[: message_index + 1]
                history[position:position] = [entry, tail]
            else:
                history.insert(position, entry)
            return entry, 0

        history.insert(0, entry)
        return entry, 0

    @staticmethod
    def _prune(
        binding: DurableHistoryBinding,
        pruned: list[tuple[DurableAgentStateEntry, DurableAgentStateMessage]],
    ) -> None:
        """Remove eligible exclusions and record only actual removals.

        Removal is by identity rather than index, since insertions earlier in this flush may have
        moved messages within their entry. System messages and the current exchange are a floor.
        """
        if not pruned:
            return

        from ._retention import (
            _detached_message,  # pyright: ignore[reportPrivateUsage]
            _link_atomic_groups,  # pyright: ignore[reportPrivateUsage]
            _newest_exchange,  # pyright: ignore[reportPrivateUsage]
            _saved_group_id,  # pyright: ignore[reportPrivateUsage]
            record_truncation,
        )

        state = binding.state_provider.state
        history = state.data.conversation_history
        protected = {id(entry) for entry in _newest_exchange(history)}
        protected.update(
            id(entry)
            for entry in history
            if binding.correlation_id is not None and entry.correlation_id == binding.correlation_id
        )
        # Protect the whole atomic group when a system/current message holds any member,
        # including non-contiguous tool results and persisted links beyond Core's grouping.
        originals = [(entry, entry.messages[index]) for entry, index in replayable_entries(history)]
        messages = [_detached_message(stored) for _, stored in originals]
        annotate_message_groups(messages, force_reannotate=True)
        groups = _link_atomic_groups(messages, [_saved_group_id(stored) for _, stored in originals])
        protected_groups = {
            group
            for (entry, stored), group in zip(originals, groups)
            if id(entry) in protected or stored.role == "system"
        }
        # Eager pruning may delete only exclusions, not the included partners of a tool or
        # reasoning group. Defer the whole group until all its members are excluded.
        excluded_messages = {id(stored) for _, stored in pruned}
        protected_groups.update(
            group for (_, stored), group in zip(originals, groups) if id(stored) not in excluded_messages
        )
        protected_messages = {id(stored) for (_, stored), group in zip(originals, groups) if group in protected_groups}
        eligible = [
            (entry, stored)
            for entry, stored in pruned
            if id(entry) not in protected and stored.role != "system" and id(stored) not in protected_messages
        ]
        before = sum(len(entry.messages) for entry in history)
        before_entries = len(history)
        before_bytes = eager_state_size(state) if eligible else None
        prune_messages(history, eligible)
        removed = before - sum(len(entry.messages) for entry in history)
        if removed:
            record_truncation(state, removed)
        record_retention(
            state,
            mechanism="eager",
            outcome="staged" if removed else "protected",
            before_bytes=before_bytes,
            after_bytes=eager_state_size(state) if before_bytes is not None else None,
            removed_messages=removed,
            removed_entries=before_entries - len(history),
        )


def replayable_entries(
    history: list[DurableAgentStateEntry],
    *,
    correlation_id: str | None = None,
) -> Iterator[tuple[DurableAgentStateEntry, int]]:
    """Yield (entry, message_index) pairs that participate in model context.

    Storage eviction has separate eligibility rules. A failed turn is not model
    context, but its expired transcript payload can still consume evictable storage.

    Args:
        history: The entity's conversation history.
        correlation_id: Optional legacy exclusion. The core pipeline includes current-correlation appends.

    Yields:
        Each replayable message as its owning entry and its index within that entry.
    """
    for entry in history:
        if (
            isinstance(entry, (DurableAgentStateErrorResponse, DurableAgentStateUnknownEntry))
            or entry.json_type not in tuple(DurableAgentStateEntryJsonType)
            or entry.json_type == DurableAgentStateEntryJsonType.ERROR_RESPONSE
        ):
            # Runtime-error entries and opaque future entries are not model messages.
            continue
        if correlation_id is not None and entry.correlation_id == correlation_id:
            continue
        for index in range(len(entry.messages)):
            yield entry, index


def prune_messages(
    history: list[DurableAgentStateEntry],
    pruned: list[tuple[DurableAgentStateEntry, DurableAgentStateMessage]],
) -> None:
    """Remove messages, dropping only changed, bare, known transcript envelopes.

    Removal is by identity rather than index, since an insertion elsewhere in the same pass may
    have moved messages within their entry.

    Args:
        history: The entity's conversation history, modified in place.
        pruned: The messages to remove, each with the entry that owns it.
    """
    from ._retention import _can_drop_entry  # pyright: ignore[reportPrivateUsage]

    live_entries = {id(entry) for entry in history}
    changed: set[int] = set()
    for entry, stored in pruned:
        if (
            id(entry) not in live_entries
            or isinstance(entry, DurableAgentStateUnknownEntry)
            or entry.json_type not in tuple(DurableAgentStateEntryJsonType)
        ):
            continue
        for index, candidate in enumerate(entry.messages):
            if candidate is stored:
                del entry.messages[index]
                changed.add(id(entry))
                break

    remaining = [entry for entry in history if entry.messages or id(entry) not in changed or not _can_drop_entry(entry)]
    if len(remaining) != len(history):
        history[:] = remaining


def service_stores_history(agent: Any, options: Mapping[str, Any] | None = None) -> bool:
    """Return whether the service keeps conversation history for this run.

    Mirrors core's precedence, most specific first: the option passed on the run itself, then an
    explicit ``store`` in the agent's default options, and only when both are unset does the
    client's ``STORES_BY_DEFAULT`` apply. Clients that store by default (such as the Responses
    API) can therefore be put back in client-side mode either permanently or for a single run, and
    in that case durable history is what makes the conversation survive.

    Resolved per run rather than once at registration because ``store`` is an ordinary run option.
    An agent registered against a storing client can still be asked to keep one turn client-side,
    and whoever answers that turn's history has to be decided at that point.

    Args:
        agent: The agent being run.
        options: The effective options for this run, when there is a run in progress.

    Returns:
        True when the model service is holding this conversation.
    """
    if options is not None:
        run_store = options.get("store")
        if run_store is not None:
            return bool(run_store)
    default_options = getattr(agent, "default_options", None)
    if isinstance(default_options, Mapping):
        explicit_store = cast("Mapping[str, Any]", default_options).get("store")
        if explicit_store is not None:
            return bool(explicit_store)
    client = getattr(agent, "client", None)
    return bool(getattr(client, "STORES_BY_DEFAULT", False))


def validate_history_providers(agent: SupportsAgentRun) -> None:
    """Reject competing primaries and shared state namespaces, allowing distinct store-only sinks."""
    providers = getattr(agent, "context_providers", None)
    if not isinstance(providers, (list, tuple)):
        return
    primaries = [p for p in cast("Sequence[Any]", providers) if isinstance(p, HistoryProvider) and p.load_messages]
    if len(primaries) > 1:
        raise ValueError("A durable agent supports only one load-enabled primary history provider.")
    sources: set[str] = set()
    for provider in cast("Sequence[Any]", providers):
        source_id = provider.source_id
        if source_id in sources:
            raise ValueError(
                f"Context providers must have unique source_id values; {source_id!r} is duplicated. "
                "Assign distinct source_id values to history, audit and other context providers."
            )
        sources.add(source_id)
    if not primaries and InMemoryHistoryProvider.DEFAULT_SOURCE_ID in sources:
        raise ValueError(
            "Cannot inject durable history: 'in_memory' is already used by a context provider or store-only sink. "
            "Set that provider's source_id to a unique value such as 'audit', or explicitly configure a "
            "DurableHistoryProvider with a distinct source_id and matching compaction history_source_id."
        )


class _ServiceOwnedHistoryProvider(HistoryProvider):
    """Occupy the primary slot without loading or saving the inactive external branch."""

    def __init__(self, provider: HistoryProvider) -> None:
        """Borrow the original provider without modifying its configuration or lifecycle."""
        super().__init__(
            source_id=provider.source_id,
            load_messages=provider.load_messages,
            store_inputs=provider.store_inputs,
            store_outputs=provider.store_outputs,
            store_context_messages=provider.store_context_messages,
            store_context_from=provider.store_context_from,
        )
        self.__wrapped__ = provider
        # Core 1.13 predates this optional hook-cadence hint.
        if hasattr(provider, "after_run_once_per_turn"):
            self.after_run_once_per_turn = provider.after_run_once_per_turn

    def __getattr__(self, name: str) -> Any:
        # Expose the original configuration/resources without copying or taking their ownership.
        return getattr(self.__wrapped__, name)

    async def get_messages(
        self, session_id: str | None, *, state: dict[str, Any] | None = None, **kwargs: Any
    ) -> list[Message]:
        """Do not load the external transcript into a service-owned invocation."""
        return []

    async def save_messages(
        self,
        session_id: str | None,
        messages: Sequence[Message],
        *,
        state: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Do not append a service-owned turn to the external primary."""

    async def before_run(self, *, agent: Any, session: Any, context: Any, state: dict[str, Any]) -> None:
        """Suppress custom loading hooks as well as the base implementation."""
        # Do not call custom hooks either: an ordinary primary need not know about ownership.

    async def after_run(self, *, agent: Any, session: Any, context: Any, state: dict[str, Any]) -> None:
        """Suppress custom persistence hooks for the inactive primary."""


def prepare_history_owner(agent: SupportsAgentRun, service_owns_history: bool) -> SupportsAgentRun:
    """Return a per-run view that silences only a service-owned run's external primary.

    Call after registration and ownership resolution. Client-owned runs keep the original
    provider, including custom hooks and context attribution. Store-only sinks are never wrapped.
    The view borrows the agent's resources: preparation neither enters nor closes them. Keep the
    registered agent for lifecycle/reset decisions; wrappers also expose ``__wrapped__``.
    """
    providers = getattr(agent, "context_providers", None)
    if not isinstance(providers, (list, tuple)):
        return agent
    updated: list[Any] = []
    changed = False
    for provider in cast("Sequence[Any]", providers):
        original = provider.__wrapped__ if isinstance(provider, _ServiceOwnedHistoryProvider) else provider
        replacement = original
        if (
            service_owns_history
            and isinstance(original, HistoryProvider)
            and original.load_messages
            and not isinstance(original, DurableHistoryProvider)
            and type(original) is not InMemoryHistoryProvider
        ):
            replacement = (
                provider
                if isinstance(provider, _ServiceOwnedHistoryProvider)
                else _ServiceOwnedHistoryProvider(original)
            )
        changed |= replacement is not provider
        updated.append(replacement)
    if not changed:
        return agent
    return _copy_with_history_providers(agent, updated)


def _copy_with_history_providers(agent: SupportsAgentRun, providers: list[Any]) -> SupportsAgentRun:
    try:
        clone = copy.copy(agent)
        clone.context_providers = providers  # type: ignore[attr-defined]
    except Exception as exc:
        raise ValueError(
            f"Could not attach durable history to agent {getattr(agent, 'name', type(agent).__name__)}. "
            "Configure a supported history provider explicitly."
        ) from exc
    return clone


def ensure_durable_history(agent: SupportsAgentRun, *, prune_excluded: bool = False) -> SupportsAgentRun:
    """Back an agent's conversation history with durable entity state.

    Lets a user register an agent that already works in core and get durable behavior with no
    configuration change. The agent is never mutated: when a substitution is needed a shallow
    copy is returned with its own provider list.

        With no load-enabled primary, append a :class:`DurableHistoryProvider` after existing
        providers using core's default history source. This matches core's automatic injection order,
        so default compaction resolves that source and its reverse-order after hook sees this turn.
        Explicit registration order is preserved. Only exact built-in :class:`InMemoryHistoryProvider`
        instances are replaced, preserving source, storage flags, exclusion policy and once-per-turn
        hook setting. A hand-configured durable provider keeps explicit ``prune_excluded`` values;
        otherwise a shallow copy inherits the registration retention policy.

        Other primaries, including in-memory subclasses, keep their original hooks and state without
        an additional durable provider. Their session state may contain a transcript. Such state is
        part of the non-evictable floor, not managed by durable transcript retention.

        Service ownership is resolved per run. Without a custom primary, durable history remains
        available for client-owned runs and silent for service-owned runs, preventing core from
        injecting a separate unmanaged history slice. Agents without the core context pipeline are
        left alone and the entity falls back to replaying its own persisted history.

    Args:
        agent: The agent being registered with the durable runtime.

    Keyword Args:
        prune_excluded: When True, the injected provider physically deletes messages that
            compaction excluded, bounding durable storage. This is a **lossy retention policy**
            and is off by default.

    Returns:
        The agent to run, either unchanged or a shallow copy with durable-backed history.
    """
    validate_history_providers(agent)
    providers = getattr(agent, "context_providers", None)
    if not isinstance(providers, (list, tuple)):
        return agent

    provider_list = list(cast("Sequence[Any]", providers))
    existing = next(
        (p for p in provider_list if isinstance(p, HistoryProvider) and p.load_messages),
        None,
    )

    if existing is None:
        # Match core's source_id and append order. After hooks run in reverse, so automatic
        # history must save this turn before an earlier compaction provider reads its buffer.
        updated = [
            *provider_list,
            DurableHistoryProvider(
                source_id=InMemoryHistoryProvider.DEFAULT_SOURCE_ID,
                prune_excluded=prune_excluded,
            ),
        ]
    elif isinstance(existing, DurableHistoryProvider):
        # Already durable. If the caller pinned ``prune_excluded`` themselves that decision
        # stands, but an unset one means they never expressed a preference, and leaving it unset
        # would make the entity's retention mode silently do nothing.
        if existing.prune_excluded is not None:
            return agent
        replacement = copy.copy(existing)
        replacement.prune_excluded = prune_excluded
        if existing.store_context_from is not None:
            replacement.store_context_from = set(existing.store_context_from)
        updated = [replacement if p is existing else p for p in provider_list]
    elif type(existing) is InMemoryHistoryProvider:
        replacement = DurableHistoryProvider(
            source_id=existing.source_id,
            store_inputs=existing.store_inputs,
            store_outputs=existing.store_outputs,
            store_context_messages=existing.store_context_messages,
            store_context_from=existing.store_context_from,
            skip_excluded=existing.skip_excluded,
            prune_excluded=prune_excluded,
        )
        if hasattr(existing, "after_run_once_per_turn"):
            replacement.after_run_once_per_turn = existing.after_run_once_per_turn
        updated = [replacement if p is existing else p for p in provider_list]
    else:
        # A deliberate storage choice (external or custom), so do not override it.
        return agent

    return _copy_with_history_providers(agent, updated)
