# Copyright (c) Microsoft. All rights reserved.

"""A core ``HistoryProvider`` backed by canonical durable state.

Core history hooks stage transcript appends, while compaction annotations and summaries
are reconciled from a transient working buffer. The owning state provider commits the
transcript together with its independent delivery and control state at the operation
boundary. This module stays private until the host session and workflow activation slice.
"""

from __future__ import annotations

import copy
import logging
from collections.abc import Generator, Iterator, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol, cast

from agent_framework import (
    GROUP_ANNOTATION_KEY,
    GROUP_ID_KEY,
    SUMMARIZED_BY_SUMMARY_ID_KEY,
    SUMMARY_OF_MESSAGE_IDS_KEY,
    AgentResponse,
    CompactionProvider,
    HistoryProvider,
    InMemoryHistoryProvider,
    Message,
    SessionContext,
    SupportsAgentRun,
    annotate_message_groups,
)

from ._response_utils import is_terminal_agent_response
from ._retention_telemetry import eager_state_size, record_retention
from ._shared_agent_state import (
    DurableAgentState,
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

logger = logging.getLogger("agent_framework.durabletask")

WORKING_BUFFER_KEY = "messages"
POSITIONS_KEY = "_positions"
EXCLUDED_KEY = "_excluded"
_HISTORY_ID_ATTRIBUTE = "_durable_history_id"


class _DurableStateProvider(Protocol):
    """Minimal binding surface for shared durable history ownership."""

    @property
    def state(self) -> DurableAgentState:
        """Return the currently staged canonical state."""
        ...


def _history_message_id(message: Message) -> str | None:
    """Resolve transient reconciliation identity without changing the public message ID."""
    return getattr(message, _HISTORY_ID_ATTRIBUTE, None) or message.message_id


def _collect_used_message_ids(history: Sequence[DurableAgentStateEntry]) -> set[str]:
    """Collect every stored history identity, including opaque non-replayable entries."""
    return {message.message_id for entry in history for message in entry.messages if message.message_id is not None}


@dataclass
class DurableHistoryBinding:
    """Per-operation binding between a durable owner and the history provider."""

    state_provider: _DurableStateProvider
    correlation_id: str | None = None
    service_owns_history: bool = False
    append_ordinal: int = 0
    pending_inputs: list[Message] = field(default_factory=lambda: list[Message](), repr=False)
    accepted_inputs: set[tuple[str, str]] = field(default_factory=lambda: set[tuple[str, str]](), repr=False)
    append_response: AgentResponse | None = field(default=None, repr=False)

    def accept(self, messages: Sequence[Message]) -> None:
        """Record only operation-supplied receipts carried by actually accepted messages."""
        for message in messages:
            receipt = getattr(message, "_durable_ingestion_receipt", None)
            if isinstance(receipt, tuple):
                items = cast("tuple[Any, ...]", receipt)
                if len(items) == 2 and all(isinstance(item, str) for item in items):
                    self.accepted_inputs.add(cast("tuple[str, str]", items))


_current_binding: ContextVar[DurableHistoryBinding | None] = ContextVar(
    "durable_history_binding",
    default=None,
)


def bind_durable_history(binding: DurableHistoryBinding) -> Token[DurableHistoryBinding | None]:
    """Bind the durable owner for the current operation."""
    return _current_binding.set(binding)


def unbind_durable_history(token: Token[DurableHistoryBinding | None]) -> None:
    """Release a binding created by :func:`bind_durable_history`."""
    _current_binding.reset(token)


def current_durable_history_binding() -> DurableHistoryBinding | None:
    """Return the binding for the current durable operation, if any."""
    return _current_binding.get()


class DurableHistoryProvider(HistoryProvider):
    """Core history hooks backed by canonical durable state.

    Loading restores persisted messages, IDs and compaction annotations. After-run hooks append
    the configured inputs, context and outputs, once per core hook. Reconciliation writes working
    buffer annotations and summaries back to the transcript without committing to the backend.
    The owner performs the final flush after all core after-run providers.

    Attributes:
        skip_excluded: When True, messages marked ``_excluded`` by compaction are omitted
            from the context loaded for the model, independently of storage retention.
        prune_excluded: When True, excluded messages are physically removed on flush,
            preserving protected messages and complete atomic groups. This is lossy and
            opt-in, independently of any configured pressure budget.
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
        """Initialize history with an optional explicit eager-pruning policy.

        Leaving ``prune_excluded`` unset inherits the owning runtime's retention policy
        during preparation. Explicit True or False overrides that policy. An unset,
        unprepared provider does not prune.
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
        self._prune_excluded_explicit = prune_excluded is not None

    def _binding(self) -> DurableHistoryBinding | None:
        binding = current_durable_history_binding()
        if binding is None:
            logger.warning(
                "[DurableHistoryProvider] No durable binding is active, so the provider yields no history. "
                "This provider only works inside a durable owner operation."
            )
        else:
            self._require_writable_history(binding)
        return binding

    @staticmethod
    def _require_writable_history(binding: DurableHistoryBinding) -> None:
        """Require the exact writable version without validating the complete snapshot per hook."""
        if binding.state_provider.state.schema_version != "2.0.0":
            raise ValueError(
                "Durable history requires schemaVersion '2.0.0'. Legacy state is read-only. "
                "Use read_agent_state() for inspection."
            )

    def _replayable_entries(self, binding: DurableHistoryBinding) -> Iterator[tuple[DurableAgentStateEntry, int]]:
        """Yield ``(entry, message_index)`` pairs that participate in model context."""
        yield from replayable_entries(binding.state_provider.state.data.conversation_history)

    @staticmethod
    def _synthetic_message_id(entry: DurableAgentStateEntry, index: int) -> str:
        """Build a deterministic ID candidate for an anonymous or duplicate message."""
        scope = entry.correlation_id or (entry.created_at.isoformat() if entry.created_at is not None else "undated")
        kind = entry.json_type.value if isinstance(entry.json_type, DurableAgentStateEntryJsonType) else entry.json_type
        return f"durable_{kind}_{scope}_{index}"

    @staticmethod
    def _to_message(stored: DurableAgentStateMessage) -> Message | None:
        """Convert a persisted message into one that is safe to replay to a chat client."""
        chat_message: Message = copy.deepcopy(stored).to_chat_message()
        replayable = [
            content for content in chat_message.contents if content.type not in ("reasoning", "text_reasoning")
        ]
        if not replayable:
            return None
        message = Message(
            role=chat_message.role,
            contents=replayable,
            author_name=chat_message.author_name,
            message_id=stored.public_message_id,
            additional_properties=chat_message.additional_properties,
        )
        setattr(message, _HISTORY_ID_ATTRIBUTE, stored.message_id)
        return message

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

    def _positions(
        self,
        binding: DurableHistoryBinding,
        *,
        used_ids: set[str] | None = None,
    ) -> dict[str, tuple[DurableAgentStateEntry, int]]:
        """Index writable v2 storage, repairing anonymous or duplicate identities."""
        self._require_writable_history(binding)
        history = binding.state_provider.state.data.conversation_history
        reserved = used_ids if used_ids is not None else _collect_used_message_ids(history)
        positions: dict[str, tuple[DurableAgentStateEntry, int]] = {}
        repairs: list[tuple[DurableAgentStateMessage, str]] = []
        stored_objects: set[int] = set()
        for entry in history:
            for stored in entry.messages:
                if id(stored) in stored_objects:
                    raise ValueError("History occurrences require distinct stored message objects.")
                stored_objects.add(id(stored))
        for entry, index in self._replayable_entries(binding):
            stored = entry.messages[index]
            history_id = stored.message_id
            if not history_id or history_id in positions:
                stored.validate_history_identity_update()
                history_id = self._unique_message_id(self._synthetic_message_id(entry, index), reserved)
                repairs.append((stored, history_id))
            positions[history_id] = (entry, index)
        # Admit the entire repair batch before changing any caller-owned message.
        if repairs:
            for entry, index in self._replayable_entries(binding):
                self._to_message(entry.messages[index])
        for stored, history_id in repairs:
            stored.set_history_id(history_id)
        return positions

    async def get_messages(
        self, session_id: str | None, *, state: dict[str, Any] | None = None, **kwargs: Any
    ) -> list[Message]:
        """Load a writable v2 snapshot, possibly repairing IDs. Inspect legacy state with ``read_agent_state``."""
        binding = self._binding()
        if binding is None:
            return []

        if binding.service_owns_history:
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
            state[WORKING_BUFFER_KEY] = loaded
            state[POSITIONS_KEY] = id_map

        if self.skip_excluded:
            return [message for message in loaded if not message.additional_properties.get(EXCLUDED_KEY)]
        return list(loaded)

    async def save_messages(
        self,
        session_id: str | None,
        messages: Sequence[Message],
        *,
        state: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Stage a generic message batch as a request entry, without committing state."""
        binding = self._binding()
        if binding is None or binding.service_owns_history:
            return
        if state is not None:
            self.flush(state)
        self._append_messages(binding, messages, state=state, response=binding.append_response)

    def _append_messages(
        self,
        binding: DurableHistoryBinding,
        messages: Sequence[Message],
        *,
        state: dict[str, Any] | None,
        response: AgentResponse | None = None,
    ) -> None:
        """Append one hook batch, exposing only nonterminal batches to core compaction."""
        self._require_writable_history(binding)
        if not messages or binding.service_owns_history:
            return
        if state is not None and WORKING_BUFFER_KEY not in state:
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
        used_ids = _collect_used_message_ids(binding.state_provider.state.data.conversation_history)
        stored_messages, working_messages = self._copy_append_messages(
            binding,
            messages,
            kind,
            created_at,
            used_ids=used_ids,
        )
        if response is None:
            entry = DurableAgentStateRequest(binding.correlation_id, created_at, stored_messages)
        else:
            entry = response_type(
                binding.correlation_id,
                created_at,
                stored_messages,
                usage=copy.deepcopy(DurableAgentStateUsage.from_usage(response.usage_details)),
                extension_data=copy.deepcopy(response.additional_properties)
                if response.additional_properties
                else None,
            )
            entry.preserve_response_timestamp(response.created_at)
        binding.state_provider.state.data.conversation_history.append(entry)
        if state is not None:
            buffer = cast("list[Message]", state.setdefault(WORKING_BUFFER_KEY, []))
            if not isinstance(entry, DurableAgentStateErrorResponse):
                buffer.extend(working_messages)
            positions = self._positions(binding, used_ids=used_ids)
            exposed_ids = {_history_message_id(message) for message in buffer}
            previous_positions = state.get(POSITIONS_KEY)
            if isinstance(previous_positions, dict):
                # Failure finalization can append before the strategy's removals have
                # been flushed. Preserve that evidence, not unexposed owner entries.
                exposed_ids.update(cast("dict[str, Any]", previous_positions))
            state[POSITIONS_KEY] = {key: position for key, position in positions.items() if key in exposed_ids}

    def _copy_append_messages(
        self,
        binding: DurableHistoryBinding,
        messages: Sequence[Message],
        kind: DurableAgentStateEntryJsonType,
        created_at: datetime,
        *,
        used_ids: set[str] | None = None,
    ) -> tuple[list[DurableAgentStateMessage], list[Message]]:
        """Allocate stored identities without changing input messages or caller responses."""
        history = binding.state_provider.state.data.conversation_history
        used = used_ids if used_ids is not None else _collect_used_message_ids(history)
        reserved = (
            used
            if used_ids is not None and kind == DurableAgentStateEntryJsonType.COMPACTION and len(messages) == 1
            else used | {message.message_id for message in messages if message.message_id}
        )
        scope = binding.correlation_id or created_at.isoformat()
        ordinal = binding.append_ordinal
        binding.append_ordinal += 1
        stored_messages: list[DurableAgentStateMessage] = []
        working_messages: list[Message] = []
        for index, message in enumerate(messages):
            stored = DurableAgentStateMessage.from_chat_message(copy.deepcopy(message))
            receipt = getattr(message, "_durable_ingestion_receipt", None)
            if kind == DurableAgentStateEntryJsonType.REQUEST and isinstance(receipt, tuple):
                items = cast("tuple[Any, ...]", receipt)
                if len(items) == 2 and all(isinstance(item, str) for item in items):
                    occurrence, fingerprint = cast("tuple[str, str]", items)
                    stored.ingestion_occurrence = occurrence
                    stored.ingestion_identity = fingerprint
            if not stored.message_id or stored.message_id in used:
                prefix = "durable_revision" if stored.message_id else "durable"
                candidate = f"{prefix}_{kind.value}_{scope}_{ordinal}_{index}"
                internal_id = self._unique_message_id(candidate, reserved)
                if kind == DurableAgentStateEntryJsonType.COMPACTION and (
                    stored.public_message_id is None or self._summary_original_ids(message) is not None
                ):
                    stored.message_id = internal_id
                else:
                    stored.set_history_id(internal_id)
            used.add(cast(str, stored.message_id))
            working = copy.deepcopy(message)
            working.message_id = stored.public_message_id
            setattr(working, _HISTORY_ID_ATTRIBUTE, stored.message_id)
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
            self._require_writable_history(binding)
            binding.pending_inputs.clear()
            if binding.service_owns_history:
                return
            if self.store_inputs and getattr(agent, "require_per_service_call_history_persistence", False):
                binding.pending_inputs = copy.deepcopy(context.input_messages)
        await super().before_run(agent=agent, session=session, context=context, state=state)

    def _get_context_messages_to_store(self, context: SessionContext) -> list[Message]:
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
        if request_messages:
            await self.save_messages(context.session_id, request_messages, state=state)
        if self.store_outputs and context.response and context.response.messages:
            previous_response = binding.append_response
            binding.append_response = context.response
            try:
                await self.save_messages(context.session_id, context.response.messages, state=state)
            finally:
                binding.append_response = previous_response
        binding.pending_inputs.clear()

    def finalize_failed_run(self, state: dict[str, Any]) -> None:
        """Stage actual tool results left unsaved when a later service call fails."""
        binding = current_durable_history_binding()
        if binding is None:
            return
        self._require_writable_history(binding)
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
                messages.append(message)
                pending_calls.difference_update(result_ids)
        self._append_messages(binding, messages, state=state)

    def flush(self, state: dict[str, Any]) -> None:
        """Apply compaction results to canonical durable state.

        Reconciliation is by ``message_id`` rather than position, so strategies that insert
        messages are handled as well as ones that only annotate. Messages removed from the
        working buffer become excluded. Only opt-in pruning can physically delete them,
        subject to the protected-message and atomic-group floors.
        """
        binding = current_durable_history_binding()
        if binding is None or binding.service_owns_history:
            return
        self._require_writable_history(binding)

        raw_buffer = state.get(WORKING_BUFFER_KEY)
        raw_positions = state.get(POSITIONS_KEY)
        if not isinstance(raw_buffer, list):
            return
        buffer = cast("list[Message]", raw_buffer)
        previous_ids: set[str] = set()
        if isinstance(raw_positions, dict):
            previous_ids.update(cast("dict[str, Any]", raw_positions))
        history = binding.state_provider.state.data.conversation_history
        used_ids = _collect_used_message_ids(history)
        stored_by_id = self._positions(binding, used_ids=used_ids)
        # Only loaded occurrences carry this transient marker. Drop stale loaded
        # references before a newly inserted occurrence can reuse their public ID.
        buffer[:] = [
            message
            for message in buffer
            if not isinstance(getattr(message, _HISTORY_ID_ATTRIBUTE, None), str)
            or getattr(message, _HISTORY_ID_ATTRIBUTE) in stored_by_id
        ]
        last_known: tuple[DurableAgentStateEntry, int] | None = None
        # A prepended working-buffer message belongs before the first exposed
        # occurrence, not before opaque/error/reasoning-only canonical prefixes.
        first_exposed = next(
            (
                (entry, index)
                for entry, index in self._replayable_entries(binding)
                if self._to_message(entry.messages[index]) is not None
            ),
            None,
        )
        for entry in history:
            if first_exposed is not None and entry is first_exposed[0]:
                if first_exposed[1] > 0:
                    last_known = (entry, first_exposed[1] - 1)
                break
            last_known = (entry, len(entry.messages) - 1)
        buffer_by_history_id = {
            history_id: message for message in buffer if (history_id := _history_message_id(message)) is not None
        }

        for message in buffer:
            loaded_occurrence = isinstance(getattr(message, _HISTORY_ID_ATTRIBUTE, None), str)
            history_id = _history_message_id(message)
            position = stored_by_id.get(history_id) if history_id and loaded_occurrence else None
            summary_ids = self._summary_original_ids(message)
            summary_revision = False
            if position is not None and summary_ids is not None:
                owner, index = position
                original = self._to_message(owner.messages[index])
                working = self._to_message(DurableAgentStateMessage.from_chat_message(copy.deepcopy(message)))
                original_payload = original.to_dict() if original is not None else {}
                working_payload = working.to_dict() if working is not None else {}
                original_payload.pop("additional_properties", None)
                working_payload.pop("additional_properties", None)
                original_ids = self._summary_original_ids(original) if original is not None else None
                summary_revision = original_payload != working_payload or original_ids != summary_ids

            if position is None or summary_revision:
                if not summary_revision and loaded_occurrence and history_id in previous_ids:
                    continue
                original_id = message.message_id
                position = self._insert_new_message(
                    binding,
                    message,
                    after=last_known,
                    index=stored_by_id,
                    used_ids=used_ids,
                )
                if original_id and original_id != message.message_id and summary_ids is not None:
                    summary_source_ids = set(summary_ids)
                    group = message.additional_properties.get(GROUP_ANNOTATION_KEY)
                    if isinstance(group, dict):
                        group[GROUP_ID_KEY] = f"group_{message.message_id}"
                    for summary_source_id in summary_source_ids:
                        source = buffer_by_history_id.get(summary_source_id)
                        stored_source: DurableAgentStateMessage | None = None
                        if source is None:
                            source_position = stored_by_id.get(summary_source_id)
                            if source_position is None:
                                continue
                            source_entry, source_index = source_position
                            stored_source = source_entry.messages[source_index]
                            source = self._to_message(stored_source)
                            if source is None:
                                continue
                            setattr(source, _HISTORY_ID_ATTRIBUTE, source_entry.messages[source_index].message_id)
                        if source is message:
                            continue
                        source_group = source.additional_properties.get(GROUP_ANNOTATION_KEY)
                        if (
                            isinstance(source_group, dict)
                            and cast("dict[str, Any]", source_group).get(SUMMARIZED_BY_SUMMARY_ID_KEY) == original_id
                        ):
                            repaired_group = copy.deepcopy(cast("dict[str, Any]", source_group))
                            repaired_group[SUMMARIZED_BY_SUMMARY_ID_KEY] = message.message_id
                            source.additional_properties[GROUP_ANNOTATION_KEY] = repaired_group
                        if source.additional_properties.get(SUMMARIZED_BY_SUMMARY_ID_KEY) == original_id:
                            source.additional_properties[SUMMARIZED_BY_SUMMARY_ID_KEY] = message.message_id
                        if stored_source is not None:
                            stored_source.extension_data = copy.deepcopy(source.additional_properties)
                        source_history_id = _history_message_id(source)
                        if source_history_id is not None:
                            buffer_by_history_id[source_history_id] = source
            last_known = position

        for message in buffer:
            history_id = _history_message_id(message)
            position = stored_by_id.get(history_id) if history_id else None
            if position is None:
                continue
            entry, index = position
            stored = entry.messages[index]
            if message.additional_properties or stored.extension_data:
                stored.extension_data = (
                    copy.deepcopy(message.additional_properties) if message.additional_properties else None
                )

        remaining_ids = {_history_message_id(message) for message in buffer if _history_message_id(message)}
        for message_id in previous_ids - remaining_ids:
            position = stored_by_id.get(message_id)
            if position is not None:
                entry, index = position
                stored = entry.messages[index]
                if self._to_message(stored) is None:
                    continue
                stored.extension_data = {**(stored.extension_data or {}), EXCLUDED_KEY: True}

        if self.prune_excluded:
            # Resolve owners after insertions, including messages moved into split tails.
            self._prune(
                binding,
                [
                    (entry, entry.messages[index])
                    for entry, index in stored_by_id.values()
                    if (entry.messages[index].extension_data or {}).get(EXCLUDED_KEY)
                ],
            )

        # Synchronize transient references if the owner replaced its staged history.
        # This removes no stored messages and prevents a later flush from resurrecting
        # entries that no longer belong to the current snapshot.
        stored_by_id = self._positions(binding, used_ids=used_ids)
        buffer[:] = [message for message in buffer if _history_message_id(message) in stored_by_id]
        # Only previously exposed entries can be considered removed by a strategy.
        # A replacement owner may contain new entries this buffer has never loaded.
        exposed_ids = previous_ids | remaining_ids
        state[POSITIONS_KEY] = {key: position for key, position in stored_by_id.items() if key in exposed_ids}

    @staticmethod
    def _summary_original_ids(message: Message) -> list[str] | None:
        """Recognize core summary links, also accepting top-level custom-strategy links."""
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
        index: dict[str, tuple[DurableAgentStateEntry, int]] | None = None,
        used_ids: set[str] | None = None,
    ) -> tuple[DurableAgentStateEntry, int]:
        """Persist a compaction-produced message as an entry of its own."""
        history = binding.state_provider.state.data.conversation_history
        created_at = datetime.now(tz=timezone.utc)
        stored, _ = self._copy_append_messages(
            binding,
            [message],
            DurableAgentStateEntryJsonType.COMPACTION,
            created_at,
            used_ids=used_ids,
        )
        message.message_id = stored[0].public_message_id
        setattr(message, _HISTORY_ID_ATTRIBUTE, stored[0].message_id)
        entry = DurableAgentStateCompaction(created_at=created_at, messages=stored)

        if after is not None:
            owner, message_index = after
            position = next(index for index, candidate in enumerate(history) if candidate is owner) + 1
            if message_index + 1 < len(owner.messages):
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
                if index is not None:
                    for tail_index, moved_message in enumerate(tail.messages):
                        if moved_message.message_id is not None:
                            index[moved_message.message_id] = (tail, tail_index)
            else:
                history.insert(position, entry)
            if index is not None:
                index[cast(str, stored[0].message_id)] = (entry, 0)
            return entry, 0

        history.insert(0, entry)
        if index is not None:
            index[cast(str, stored[0].message_id)] = (entry, 0)
        return entry, 0

    @staticmethod
    def _prune(
        binding: DurableHistoryBinding,
        pruned: list[tuple[DurableAgentStateEntry, DurableAgentStateMessage]],
    ) -> None:
        """Remove eligible exclusions and record only actual removals.

        Removal is by identity, since insertions can move messages within their entry.
        System messages, pending tool calls and the newest/current exchange are a floor.
        Included members protect their entire tool, reasoning or persisted atomic group.
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
        replayable = {id(entry.messages[index]) for entry, index in replayable_entries(history)}
        originals = [(entry, stored) for entry in history for stored in entry.messages]
        # Skipped entries still own persisted group links. Keep them as opaque
        # placeholders, never projecting hidden content or its Python profiles.
        messages = [
            _detached_message(stored)
            if id(stored) in replayable
            else Message(
                "system",
                [],
                additional_properties={GROUP_ANNOTATION_KEY: {GROUP_ID_KEY: f"retention_opaque_{index}"}},
            )
            for index, (_, stored) in enumerate(originals)
        ]
        annotate_message_groups(
            [message for message, (_, stored) in zip(messages, originals) if id(stored) in replayable],
            force_reannotate=True,
        )
        groups = _link_atomic_groups(messages, [_saved_group_id(stored) for _, stored in originals])
        protected_flags = [
            id(stored) not in replayable or id(entry) in protected or stored.role == "system"
            for entry, stored in originals
        ]
        pending_calls: dict[str, set[int]] = {}
        for index, (_, stored) in enumerate(originals):
            if id(stored) not in replayable:
                continue
            for content in stored.contents:
                if isinstance(content, DurableAgentStateFunctionCallContent):
                    pending_calls.setdefault(content.call_id, set()).add(index)
                elif isinstance(content, DurableAgentStateFunctionResultContent):
                    pending_calls.pop(content.call_id, None)
        for indices in pending_calls.values():
            for index in indices:
                protected_flags[index] = True
        protected_groups = {group for group, held in zip(groups, protected_flags) if held}
        # Eager pruning cannot delete included partners. Defer the whole group until
        # every member is excluded, including non-contiguous persisted atomic links.
        excluded_messages = {
            id(stored)
            for _, stored in pruned
            if id(stored) in replayable and (stored.extension_data or {}).get(EXCLUDED_KEY)
        }
        protected_groups.update(
            group for (_, stored), group in zip(originals, groups) if id(stored) not in excluded_messages
        )
        protected_messages = {id(stored) for (_, stored), group in zip(originals, groups) if group in protected_groups}
        eligible = [
            (entry, stored)
            for entry, stored in pruned
            if id(stored) in excluded_messages
            and id(entry) not in protected
            and stored.role != "system"
            and id(stored) not in protected_messages
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
    """Yield ``(entry, message_index)`` pairs that participate in model context."""
    for entry in history:
        if (
            isinstance(
                entry,
                (
                    DurableAgentStateErrorResponse,
                    DurableAgentStateUnknownEntry,
                ),
            )
            or entry.json_type not in tuple(DurableAgentStateEntryJsonType)
            or entry.is_error_response
        ):
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
        history: The owner's conversation history, modified in place.
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
    """Return whether the service keeps conversation history for this run."""
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
    """Reject competing canonical adapters after preparation, allowing ordinary store-only sinks."""
    providers = getattr(agent, "context_providers", None)
    if not isinstance(providers, (list, tuple)):
        return
    primaries = [
        provider
        for provider in cast("Sequence[Any]", providers)
        if isinstance(provider, HistoryProvider) and provider.load_messages
    ]
    if len(primaries) > 1:
        raise ValueError("A durable agent supports only one load-enabled primary history provider.")
    durable_count = sum(isinstance(provider, DurableHistoryProvider) for provider in cast("Sequence[Any]", providers))
    if not primaries or type(primaries[0]) is InMemoryHistoryProvider:
        durable_count += 1  # Preparation injects or replaces the primary with a durable adapter.
    if durable_count > 1:
        # source_id isolates session namespaces, not the canonical transcript or
        # binding. Even zero-store adapters can repair IDs and flush annotations.
        raise ValueError(
            "A durable agent supports only one DurableHistoryProvider, including injected or replaced history. "
            "Multiple durable adapters are unsupported even with loading or all store flags disabled. "
            "Use an ordinary store-only HistoryProvider with a distinct source_id for audits."
        )
    sources: set[str] = set()
    for provider in cast("Sequence[Any]", providers):
        source_id = provider.source_id
        if source_id in sources:
            raise ValueError(
                f"Context providers must have unique source_id values; {source_id!r} is duplicated. "
                "Assign distinct source_id values to history, audit and other context providers."
            )
        sources.add(source_id)
    if not primaries:
        source_id = _injected_history_source(cast("Sequence[Any]", providers))
        if source_id in sources:
            raise ValueError(
                f"Cannot inject durable history: {source_id!r} is already used by a context provider "
                "or store-only sink. "
                "Set that provider's source_id to a unique value such as 'audit', or explicitly configure a "
                "DurableHistoryProvider with a distinct source_id and matching compaction history_source_id."
            )


def _injected_history_source(providers: Sequence[Any]) -> str:
    sources = {provider.history_source_id for provider in providers if isinstance(provider, CompactionProvider)}
    if len(sources) > 1:
        raise ValueError("Cannot inject ambiguous compaction history sources; configure a primary explicitly.")
    return next(iter(sources), InMemoryHistoryProvider.DEFAULT_SOURCE_ID)


class _ObservedHistoryProvider(HistoryProvider):
    """Delegate the original primary and observe only the base save hook path.

    Custom hook overrides remain responsible for any extra observability they require.
    This wrapper preserves explicit binding.accept responsibility on the completed base
    persistence surface only and does not instrument opaque custom hook behavior here.
    """

    def __init__(self, provider: HistoryProvider) -> None:
        super().__init__(
            source_id=provider.source_id,
            load_messages=provider.load_messages,
            store_inputs=provider.store_inputs,
            store_outputs=provider.store_outputs,
            store_context_messages=provider.store_context_messages,
            store_context_from=provider.store_context_from,
        )
        self.__wrapped__ = provider
        if hasattr(provider, "after_run_once_per_turn"):
            self.after_run_once_per_turn = provider.after_run_once_per_turn

    def __getattr__(self, name: str) -> Any:
        return getattr(self.__wrapped__, name)

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        return await self.__wrapped__.get_messages(session_id, **kwargs)

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        await self.__wrapped__.save_messages(session_id, messages, **kwargs)
        binding = current_durable_history_binding()
        if binding is not None and self.store_inputs:
            binding.accept(messages)

    async def before_run(self, *, agent: Any, session: Any, context: Any, state: dict[str, Any]) -> None:
        await self.__wrapped__.before_run(agent=agent, session=session, context=context, state=state)

    def _get_context_messages_to_store(self, context: SessionContext) -> list[Message]:
        return self.__wrapped__._get_context_messages_to_store(context)

    async def after_run(self, *, agent: Any, session: Any, context: Any, state: dict[str, Any]) -> None:
        if type(self.__wrapped__).after_run is HistoryProvider.after_run:
            await super().after_run(agent=agent, session=session, context=context, state=state)
        else:
            await self.__wrapped__.after_run(agent=agent, session=session, context=context, state=state)


@contextmanager
def _compaction_id_scope(messages: list[Message]) -> Generator[None]:
    """Give a strategy unique reconciliation IDs only for the duration of its hook."""
    original_messages = list(messages)
    original_state: dict[int, tuple[Message, str | None]] = {}
    public_ids: dict[str, str | None] = {}
    for message in original_messages:
        original_state.setdefault(id(message), (message, message.message_id))
        history_id = getattr(message, _HISTORY_ID_ATTRIBUTE, None)
        if isinstance(history_id, str):
            public_ids.setdefault(history_id, original_state[id(message)][1])
            message.message_id = history_id
    try:
        yield
    finally:
        for message, public_id in original_state.values():
            message.message_id = public_id
        for message in messages:
            if id(message) in original_state:
                continue
            history_id = getattr(message, _HISTORY_ID_ATTRIBUTE, None)
            if history_id in public_ids and message.message_id == history_id:
                message.message_id = public_ids[history_id]


class _DurableCompactionProvider(CompactionProvider):
    """Preserve a configured compaction provider while isolating its internal IDs."""

    def __init__(self, provider: CompactionProvider) -> None:
        super().__init__(
            before_strategy=provider.before_strategy,
            after_strategy=provider.after_strategy,
            tokenizer=provider.tokenizer,
            source_id=provider.source_id,
            history_source_id=provider.history_source_id,
        )
        self.__wrapped__ = provider
        if hasattr(provider, "after_run_once_per_turn"):
            self.after_run_once_per_turn = provider.after_run_once_per_turn

    def __getattr__(self, name: str) -> Any:
        return getattr(self.__wrapped__, name)

    async def before_run(self, *, agent: Any, session: Any, context: Any, state: dict[str, Any]) -> None:
        messages = context.get_messages()
        with _compaction_id_scope(messages):
            await self.__wrapped__.before_run(agent=agent, session=session, context=context, state=state)

    async def after_run(self, *, agent: Any, session: Any, context: Any, state: dict[str, Any]) -> None:
        history_state: Any = session.state.get(self.history_source_id, {}) if session else {}
        messages = (
            cast("dict[str, Any]", history_state).get(WORKING_BUFFER_KEY) if isinstance(history_state, dict) else None
        )
        with _compaction_id_scope(cast("list[Message]", messages) if isinstance(messages, list) else []):
            await self.__wrapped__.after_run(agent=agent, session=session, context=context, state=state)


class _ServiceOwnedHistoryProvider(HistoryProvider):
    """Occupy the primary slot without invoking the inactive external branch.

    Custom primary hooks can load or persist directly, so this adapter suppresses the
    whole primary path explicitly instead of partially bypassing selected methods.
    Hooks that must always run belong in a separate context provider or store-only sink.
    """

    def __init__(self, provider: HistoryProvider) -> None:
        super().__init__(
            source_id=provider.source_id,
            load_messages=provider.load_messages,
            store_inputs=provider.store_inputs,
            store_outputs=provider.store_outputs,
            store_context_messages=provider.store_context_messages,
            store_context_from=provider.store_context_from,
        )
        self.__wrapped__ = provider
        if hasattr(provider, "after_run_once_per_turn"):
            self.after_run_once_per_turn = provider.after_run_once_per_turn

    def __getattr__(self, name: str) -> Any:
        return getattr(self.__wrapped__, name)

    async def get_messages(
        self, session_id: str | None, *, state: dict[str, Any] | None = None, **kwargs: Any
    ) -> list[Message]:
        return []

    async def save_messages(
        self,
        session_id: str | None,
        messages: Sequence[Message],
        *,
        state: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        return None

    async def before_run(self, *, agent: Any, session: Any, context: Any, state: dict[str, Any]) -> None:
        return None

    async def after_run(self, *, agent: Any, session: Any, context: Any, state: dict[str, Any]) -> None:
        return None


class _InactiveHistoryCompactionProvider(_DurableCompactionProvider):
    """Keep context compaction active without touching an inactive history branch."""

    async def before_run(self, *, agent: Any, session: Any, context: Any, state: dict[str, Any]) -> None:
        # Core's before hook operates on current context, not history_source_id.
        await self.__wrapped__.before_run(agent=agent, session=session, context=context, state=state)

    async def after_run(self, *, agent: Any, session: Any, context: Any, state: dict[str, Any]) -> None:
        return None


def prepare_history_owner(agent: SupportsAgentRun, service_owns_history: bool) -> SupportsAgentRun:
    """Return a per-run view that silences only a service-owned run's external primary."""
    providers = getattr(agent, "context_providers", None)
    if not isinstance(providers, (list, tuple)):
        return agent
    updated: list[Any] = []
    changed = False
    binding = current_durable_history_binding()
    durable_sources = {
        provider.source_id
        for provider in cast("Sequence[Any]", providers)
        if isinstance(provider, DurableHistoryProvider)
    }
    inactive_sources: set[str] = set()
    if service_owns_history:
        inactive_sources.update(durable_sources)
        for provider in cast("Sequence[Any]", providers):
            primary = (
                provider.__wrapped__
                if isinstance(provider, (_ServiceOwnedHistoryProvider, _ObservedHistoryProvider))
                else provider
            )
            if (
                isinstance(primary, HistoryProvider)
                and primary.load_messages
                and not isinstance(primary, DurableHistoryProvider)
                and type(primary) is not InMemoryHistoryProvider
            ):
                inactive_sources.add(primary.source_id)
    for provider in cast("Sequence[Any]", providers):
        original = (
            provider.__wrapped__
            if isinstance(
                provider,
                (_ServiceOwnedHistoryProvider, _ObservedHistoryProvider, _DurableCompactionProvider),
            )
            else provider
        )
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
        elif (
            binding is not None
            and not service_owns_history
            and isinstance(original, HistoryProvider)
            and original.load_messages
            and not isinstance(original, DurableHistoryProvider)
        ):
            replacement = _ObservedHistoryProvider(original)
        elif isinstance(original, CompactionProvider) and original.history_source_id in inactive_sources:
            replacement = (
                provider
                if isinstance(provider, _InactiveHistoryCompactionProvider)
                else _InactiveHistoryCompactionProvider(original)
            )
        elif isinstance(original, CompactionProvider) and original.history_source_id in durable_sources:
            replacement = (
                provider if type(provider) is _DurableCompactionProvider else _DurableCompactionProvider(original)
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
    """Back an agent's conversation history with canonical durable state.

    With no load-enabled primary, inject a :class:`DurableHistoryProvider` using core's
    default history source. Place it before a matching before-compaction provider so
    Core's forward hooks can load prior context first. Otherwise append it, retaining
    the existing reverse after-hook cadence. Only exact built-in
    :class:`InMemoryHistoryProvider` instances are replaced, in their original position.
    Explicit provider order is preserved for both before and after hooks. Other primaries, including
    in-memory subclasses, keep their original hooks and state without an additional durable
    provider. Service ownership is resolved per run.

    Hand-configured durable providers retain explicit pruning preferences. Otherwise a
    shallow copy inherits ``prune_excluded``, including when preparing an already prepared
    agent under a different policy. Caller-owned providers are never mutated.
    """
    validate_history_providers(agent)
    providers = getattr(agent, "context_providers", None)
    if not isinstance(providers, (list, tuple)):
        return agent

    provider_list = list(cast("Sequence[Any]", providers))
    existing = next(
        (provider for provider in provider_list if isinstance(provider, HistoryProvider) and provider.load_messages),
        None,
    )

    if existing is None:
        source_id = _injected_history_source(provider_list)
        insertion = next(
            (
                index
                for index, provider in enumerate(provider_list)
                if isinstance(provider, CompactionProvider)
                and provider.history_source_id == source_id
                and provider.before_strategy is not None
            ),
            len(provider_list),
        )
        updated = list(provider_list)
        replacement = DurableHistoryProvider(source_id=source_id)
        replacement.prune_excluded = prune_excluded
        updated.insert(insertion, replacement)
    elif isinstance(existing, DurableHistoryProvider):
        if existing._prune_excluded_explicit or existing.prune_excluded is prune_excluded:  # pyright: ignore[reportPrivateUsage]
            updated = list(provider_list)
        else:
            replacement = copy.copy(existing)
            replacement.prune_excluded = prune_excluded
            if existing.store_context_from is not None:
                replacement.store_context_from = set(existing.store_context_from)
            updated = [replacement if provider is existing else provider for provider in provider_list]
    elif type(existing) is InMemoryHistoryProvider:
        replacement = DurableHistoryProvider(
            source_id=existing.source_id,
            store_inputs=existing.store_inputs,
            store_outputs=existing.store_outputs,
            store_context_messages=existing.store_context_messages,
            store_context_from=existing.store_context_from,
            skip_excluded=existing.skip_excluded,
        )
        replacement.prune_excluded = prune_excluded
        if hasattr(existing, "after_run_once_per_turn"):
            replacement.after_run_once_per_turn = existing.after_run_once_per_turn
        updated = [replacement if provider is existing else provider for provider in provider_list]
    else:
        return agent

    durable_sources = {provider.source_id for provider in updated if isinstance(provider, DurableHistoryProvider)}
    updated = [
        _DurableCompactionProvider(provider)
        if isinstance(provider, CompactionProvider)
        and not isinstance(provider, _DurableCompactionProvider)
        and provider.history_source_id in durable_sources
        else provider
        for provider in updated
    ]
    if len(updated) == len(provider_list) and all(a is b for a, b in zip(updated, provider_list, strict=True)):
        return agent
    return _copy_with_history_providers(agent, updated)
