# Copyright (c) Microsoft. All rights reserved.

"""Host-agnostic workflow orchestration engine.

This module provides the shared workflow orchestration logic that executes MAF
Workflows as durable task orchestrations.  It programs against the
:class:`WorkflowOrchestrationContext` protocol so that the same code runs on
both Azure Functions and standalone durabletask hosts.

Key components:

* :func:`run_workflow_orchestrator` — main generator-based orchestrator
* Routing helpers (edge groups, fan-in, HITL)
* Result processing helpers

All host-specific task creation (agent dispatch, activity dispatch, task_all /
task_any) is delegated to the ``WorkflowOrchestrationContext`` adapter.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
from collections import Counter, defaultdict
from collections.abc import Generator, Mapping, Sequence
from copy import copy
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, Literal, cast

from agent_framework import (
    AgentExecutor,
    AgentExecutorRequest,
    AgentExecutorResponse,
    AgentResponse,
    Content,
    Executor,
    Message,
    Workflow,
    WorkflowConvergenceException,
    WorkflowEvent,
    WorkflowExecutor,
)
from agent_framework._workflows._edge import (
    Edge,
    EdgeGroup,
    FanInEdgeGroup,
    FanOutEdgeGroup,
    SingleEdgeGroup,
    SwitchCaseEdgeGroup,
)
from agent_framework._workflows._message_utils import normalize_messages_input
from agent_framework._workflows._runner_context import WorkflowMessage
from agent_framework._workflows._state import State
from agent_framework._workflows._typing_utils import is_instance_of, try_coerce_to_type
from pydantic import BaseModel

from .._message_identity import message_identity
from .._response_utils import ensure_response_format, load_agent_response
from .context import WorkflowOrchestrationContext
from .hitl_checkpoint import workflow_hitl_checkpoint_name
from .naming import (
    WORKFLOW_INPUT_EXECUTOR_ID,
    qualify_subworkflow_request_id,
    workflow_executor_activity_name,
    workflow_message_id,
    workflow_orchestrator_name,
    workflow_scoped_executor_id,
)
from .protocol import wrap_workflow_input
from .runner_context import (
    HOST_METADATA_INSTANCE_ID,
    HOST_METADATA_REQUEST_PATH_PREFIX,
    HOST_METADATA_WORKFLOW_NAME,
)
from .serialization import (
    SUBWORKFLOW_ADDRESS_KEY,
    SUBWORKFLOW_INPUT_KEY,
    SUBWORKFLOW_RESULT_KEY,
    deserialize_response_type,
    deserialize_value,
    reconstruct_to_type,
    resolve_type,
    serialize_value,
    serialize_workflow_agent_response,
    serialize_workflow_event,
    strip_pickle_markers,
    validate_workflow_json,
    validate_workflow_numbers,
)

logger = logging.getLogger(__name__)


# ============================================================================
# Source Marker Constants
# ============================================================================

SOURCE_WORKFLOW_START = "__workflow_start__"
SOURCE_ORCHESTRATOR = "__orchestrator__"
SOURCE_HITL_RESPONSE = "__hitl_response__"

# Private checkpoint provenance on dispatch copies, never application message IDs.
_FORWARDING_PROVENANCE = "_durable_workflow_forwarding"

# A WorkflowExecutor node runs its inner workflow as a durable child orchestration.
# The parent wraps the node's input in SUBWORKFLOW_INPUT_KEY (defined alongside the
# trust-boundary sanitizer in serialization.py) so the child orchestrator can tell a
# trusted sub-orchestration payload apart from untrusted top-level client input.
#
# Nesting is intentionally *not* capped by a depth counter: a workflow graph cannot
# express unbounded recursion (a WorkflowExecutor wraps a concrete Workflow instance,
# so the nesting tree is finite and fixed at build time), and the recursively-derived
# child instance ids grow with depth, so the durable backend's instance-id length
# limit is the natural ceiling for any pathological construction.


# ============================================================================
# Task Types and Data Structures
# ============================================================================


class TaskType(Enum):
    """Type of executor task."""

    AGENT = "agent"
    ACTIVITY = "activity"
    SUBWORKFLOW = "subworkflow"


@dataclass(frozen=True)
class _HITLMessageSources:
    """Mark an admitted reply independently of application executor names."""

    source_executor_ids: list[str]


# Accept the legacy singular source at helper boundaries, while routed messages
# carry all source IDs. The distinct control variant stays replay-local until
# activity preparation writes an explicit boolean into the dispatch envelope.
_MessageSources = str | list[str] | _HITLMessageSources


def _source_executor_ids(sources: _MessageSources) -> list[str]:
    if isinstance(sources, _HITLMessageSources):
        return sources.source_executor_ids
    return [sources] if isinstance(sources, str) else sources


@dataclass
class TaskMetadata:
    """Metadata for a pending task."""

    executor_id: str
    message: Any
    source_executor_id: _MessageSources
    task_type: TaskType
    remaining_messages: list[tuple[str, Any, _MessageSources]] | None = None
    # For SUBWORKFLOW tasks: the deterministic child orchestration instance id. The
    # parent records these in its custom status before awaiting the child so the read
    # side can reach nested pending HITL requests while the parent is suspended.
    child_instance_id: str | None = None
    selected_context: list[Message] | None = None
    selected_context_ids: list[str] | None = None
    invocation_ordinal: int = 0
    response_format: type[BaseModel] | None = None
    skip_dispatch: bool = False
    child_ordinal: int | None = None

    @property
    def source_executor_ids(self) -> list[str]:
        """Return all sources, accepting the legacy singular constructor argument."""
        return _source_executor_ids(self.source_executor_id)

    @property
    def is_hitl_response(self) -> bool:
        """Identify admitted replies, never infer control from a business name."""
        return isinstance(self.source_executor_id, _HITLMessageSources)


@dataclass
class ExecutorResult:
    """Result from executing an agent or activity."""

    executor_id: str
    output_message: AgentExecutorResponse | None
    activity_result: dict[str, Any] | None
    task_type: TaskType
    source_message: Any = None
    child_instance_id: str | None = None


@dataclass
class PendingHITLRequest:
    """Tracks a pending Human-in-the-Loop request."""

    request_id: str
    source_executor_id: str
    request_data: Any
    request_type: str | None
    response_type: str | dict[str, Any] | None
    task_type: TaskType = TaskType.ACTIVITY


@dataclass
class _WorkflowDeliveryLedger:
    """Logical conversations and occurrence receipts rebuilt by deterministic replay.

    Application IDs are opaque. Object addresses only look up live envelopes and
    aliases in this episode; retained references prevent address reuse. Wire IDs
    contain only deterministic structural addresses, never memory addresses.
    """

    instance_id: str = ""
    sent: dict[str, set[tuple[str, str]]] = field(default_factory=lambda: dict[str, set[tuple[str, str]]]())
    handoffs: dict[str, int] = field(default_factory=lambda: dict[str, int]())
    completions: int = 0
    envelopes: dict[int, tuple[AgentExecutorResponse, list[str], list[str]]] = field(
        default_factory=lambda: dict[int, tuple[AgentExecutorResponse, list[str], list[str]]]()
    )
    aliases: dict[int, tuple[Message, set[str]]] = field(default_factory=lambda: dict[int, tuple[Message, set[str]]]())
    selection_copies: dict[tuple[str, str, int], str] = field(default_factory=lambda: dict[tuple[str, str, int], str]())
    cached: dict[str, tuple[list[Message], list[str]]] = field(
        default_factory=lambda: dict[str, tuple[list[Message], list[str]]]()
    )
    pending_agent_requests: dict[str, dict[str, Content]] = field(
        default_factory=lambda: dict[str, dict[str, Content]]()
    )
    pending_agent_responses: dict[str, list[Content]] = field(default_factory=lambda: dict[str, list[Content]]())

    def occurrence(self, *address: Any) -> str:
        """Identify an occurrence without changing its application message."""
        framed = json.dumps([self.instance_id, *address], ensure_ascii=False)
        return "wf:occurrence:" + hashlib.sha256(framed.encode("utf-8")).hexdigest()

    def fork(self) -> _WorkflowDeliveryLedger:
        """Stage new associations until projection and task preparation succeed."""
        return replace(
            self,
            sent=dict(self.sent),
            handoffs=dict(self.handoffs),
            envelopes=dict(self.envelopes),
            aliases=dict(self.aliases),
            selection_copies=dict(self.selection_copies),
            cached=dict(self.cached),
            pending_agent_requests={key: dict(value) for key, value in self.pending_agent_requests.items()},
            pending_agent_responses={key: list(value) for key, value in self.pending_agent_responses.items()},
        )

    def remember(self, response: AgentExecutorResponse, ids: list[str], latest_ids: list[str]) -> None:
        """Associate a logical envelope with parallel full/latest occurrence lists."""
        self.envelopes[id(response)] = (response, ids, latest_ids)
        for message, occurrence in zip(response.full_conversation, ids, strict=True):
            previous = self.aliases.get(id(message))
            if previous is None:
                self.aliases[id(message)] = (message, {occurrence})
            elif len(previous[1]) < 2 and occurrence not in previous[1]:
                self.aliases[id(message)] = (message, previous[1] | {occurrence})

    def identify(
        self, response: AgentExecutorResponse, source: Any = None, *, scope: str | None = None
    ) -> tuple[list[str], list[str]]:
        """Register a new producer envelope once, before fan-out or projection."""
        known = self.envelopes.get(id(response))
        if known is not None:
            return known[1], known[2]
        ordinal = self.completions
        self.completions += 1
        latest = list(response.agent_response.messages) if response.agent_response else []
        latest_ids = [self.occurrence(scope, response.executor_id, ordinal, "output", i) for i in range(len(latest))]
        output_positions = _match_occurrences(
            list(reversed(latest)),
            list(reversed(response.full_conversation)),
            [str(i) for i in reversed(range(len(response.full_conversation)))],
        )
        if (
            latest
            and len(latest) <= len(response.full_conversation)
            and all(a is b for a, b in zip(latest, response.full_conversation[-len(latest) :], strict=True))
        ):
            output_positions = [
                str(i)
                for i in reversed(range(len(response.full_conversation) - len(latest), len(response.full_conversation)))
            ]
        source_messages: list[Message] = []
        source_ids: list[str] = []
        provenance = cast(
            tuple[str, list[Message]] | None, getattr(response.agent_response, _FORWARDING_PROVENANCE, None)
        )
        for prior in _upstream_responses(source) or []:
            prior_ids, prior_latest_ids = self.identify(prior)
            source_messages.extend(prior.full_conversation)
            source_ids.extend(prior_ids)
            prior_latest = list(prior.agent_response.messages) if prior.agent_response else []
            if (
                isinstance(provenance, tuple)
                and len(provenance) == 2
                and provenance[0] == self.forwarding_key(prior, prior_ids, prior_latest_ids)
                and response.executor_id == prior.executor_id
                and isinstance(provenance[1], list)
                and len(latest) == len(prior_latest) == len(provenance[1])
                and all(a is b for a, b in zip(latest, provenance[1], strict=True))
                and _same_message_values(latest, prior_latest)
            ):
                latest_ids = list(prior_latest_ids)
                if _same_message_values(response.full_conversation, prior.full_conversation):
                    full_matches = _match_occurrences(response.full_conversation, prior.full_conversation, prior_ids)
                    if all(occurrence is not None for occurrence in full_matches):
                        full_ids = cast(list[str], full_matches)
                        self.remember(response, full_ids, latest_ids)
                        return full_ids, latest_ids
        output_matches = {
            int(position): occurrence
            for position, occurrence in zip(output_positions, reversed(latest_ids), strict=True)
            if position is not None
        }
        history_positions = [i for i in range(len(response.full_conversation)) if i not in output_matches]
        history_matches = _match_occurrences(
            [response.full_conversation[i] for i in history_positions], source_messages, source_ids
        )
        forwarded = dict(zip(history_positions, history_matches, strict=True))
        alias_counts = Counter(id(message) for message in response.full_conversation)
        ids: list[str] = []
        for index, message in enumerate(response.full_conversation):
            occurrence = output_matches.get(index)
            if occurrence is None:
                occurrence = forwarded.get(index)
                alias = self.aliases.get(id(message))
                if (
                    occurrence is None
                    and scope is None
                    and alias_counts[id(message)] == 1
                    and alias is not None
                    and len(alias[1]) == 1
                ):
                    occurrence = next(iter(alias[1]))
            ids.append(occurrence or self.occurrence(scope, response.executor_id, ordinal, "context", index))
        self.remember(response, ids, latest_ids)
        return ids, latest_ids

    def forwarding_key(self, prior: AgentExecutorResponse, ids: list[str], latest_ids: list[str]) -> str:
        """Retain an inherited witness while forwarding through a child workflow."""
        provenance = cast(tuple[str, list[Message]] | None, getattr(prior.agent_response, _FORWARDING_PROVENANCE, None))
        latest = list(prior.agent_response.messages) if prior.agent_response else []
        if (
            isinstance(provenance, tuple)
            and len(provenance) == 2
            and isinstance(provenance[0], str)
            and isinstance(provenance[1], list)
            and len(latest) == len(provenance[1])
            and all(a is b for a, b in zip(latest, provenance[1], strict=True))
        ):
            return provenance[0]
        return self.occurrence("forward", ids, latest_ids)

    def forwarding_input(self, message: Any) -> Any:
        """Copy upstream envelopes with replay-stable, checkpoint-only witnesses."""
        upstream = _upstream_responses(message)
        if upstream is None:
            return message
        forwarded: list[AgentExecutorResponse] = []
        for prior in upstream:
            ids, latest_ids = self.identify(prior)
            response = copy(prior.agent_response)
            setattr(
                response,
                _FORWARDING_PROVENANCE,
                (self.forwarding_key(prior, ids, latest_ids), list(response.messages)),
            )
            forwarded.append(replace(prior, agent_response=response))
        return forwarded[0] if isinstance(message, AgentExecutorResponse) else forwarded


def _same_message_values(left: list[Message], right: list[Message]) -> bool:
    """Compare JSON message values without requiring excluded data to serialize."""
    try:
        return len(left) == len(right) and all(
            message_identity(a) == message_identity(b) for a, b in zip(left, right, strict=True)
        )
    except (TypeError, ValueError):
        return False


def _match_occurrences(
    selected: list[Message],
    originals: list[Message],
    ids: list[str],
    *,
    allow_positional: bool = True,
    preserve_repeated_matches: bool = False,
) -> list[str | None]:
    """Match unique occurrences, including one ancestor copied into several branches."""
    aliases: dict[int, set[str]] = defaultdict(set)
    application_ids: dict[str, set[str]] = defaultdict(set)
    for original, occurrence in zip(originals, ids, strict=True):
        aliases[id(original)].add(occurrence)
        if original.message_id is not None:
            application_ids[original.message_id].add(occurrence)
    if allow_positional and selected and len(selected) == len(originals):
        copied_aliases: dict[int, int] = {}
        try:
            if all(
                (a is b or (id(a) not in aliases and message_identity(a) == message_identity(b)))
                and copied_aliases.setdefault(id(a), id(b)) == id(b)
                for a, b in zip(selected, originals, strict=True)
            ):
                return list(ids)
        except (TypeError, ValueError):
            pass
    fingerprints: dict[str, set[str]] | None = None
    used: set[str] = set()
    matches: list[str | None] = []
    for message in selected:
        candidates: set[str] = aliases.get(id(message), set[str]())
        if not candidates and message.message_id is not None:
            candidates = application_ids.get(message.message_id, set[str]())
            if len(candidates) != 1:
                candidates = set[str]()
        if not candidates and originals:
            try:
                fingerprint = message_identity(message)
            except (TypeError, ValueError):
                fingerprint = None
            if fingerprint is not None:
                if fingerprints is None:
                    fingerprints = defaultdict(set)
                    for original, occurrence in zip(originals, ids, strict=True):
                        try:
                            fingerprints[message_identity(original)].add(occurrence)
                        except (TypeError, ValueError):
                            continue
                candidates = fingerprints.get(fingerprint, set[str]())
                if len(candidates) != 1:
                    candidates = set[str]()
        occurrence = next(iter(candidates)) if len(candidates) == 1 else None
        # By default a second selection is unmatched. Projection can retain the
        # source match to assign a separate, stable identity to each repetition.
        matches.append(occurrence if preserve_repeated_matches or occurrence not in used else None)
        if occurrence is not None:
            used.add(occurrence)
    return matches


# ============================================================================
# Routing Functions
# ============================================================================


def _evaluate_edge_condition_sync(edge: Edge, message: Any) -> bool:
    """Evaluate an edge's condition synchronously.

    Durable orchestrators run as generators, so conditions are evaluated
    synchronously here; the durabletask host does not support ``async`` edge
    conditions. A condition that returns an awaitable cannot be evaluated in
    this context, so the edge is treated as *not matched* (not traversed)
    rather than assuming a result.
    """
    condition = edge._condition  # pyright: ignore[reportPrivateUsage]
    if condition is None:
        return True
    result = condition(message)
    if inspect.isawaitable(result):
        # Async conditions cannot be evaluated in a synchronous orchestrator.
        # Close the unawaited coroutine to avoid a "never awaited" warning and
        # decline to traverse the edge (treated as not matched).
        if inspect.iscoroutine(result):
            result.close()
        logger.warning(
            "Edge condition for %s->%s is async and cannot be evaluated by the durabletask host; "
            "the edge is not traversed. Use a synchronous condition.",
            edge.source_id,
            edge.target_id,
        )
        return False
    return bool(result)


def route_message_through_edge_groups(
    edge_groups: list[EdgeGroup],
    source_id: str,
    message: Any,
    target_id: str | None = None,
    *,
    executors: Mapping[str, Executor] | None = None,
) -> list[str]:
    """Route through graph predicates, optionally restricted to an explicit target."""
    targets: list[str] = []

    def can_handle(candidate: str) -> bool:
        if executors is None:
            return True
        executor = executors.get(candidate)
        return executor is not None and bool(
            executor.can_handle(WorkflowMessage(data=message, source_id=source_id, target_id=target_id))
        )

    for group in edge_groups:
        if source_id not in group.source_executor_ids:
            continue

        if isinstance(group, (SwitchCaseEdgeGroup, FanOutEdgeGroup)):
            if group.selection_func is not None:
                target_ids = group.target_executor_ids
                selected = list(group.selection_func(message, list(target_ids)))
                if not all(selected_id in target_ids for selected_id in selected):
                    raise RuntimeError(
                        f"Invalid selection result: {selected}. "
                        f"Expected selections to be a subset of valid target executor IDs: {target_ids}."
                    )
            else:
                selected = list(group.target_executor_ids)
            # Core delivers a targeted fan-out once even if the selector repeats
            # that target. Broadcast selection retains its order and duplicates.
            if target_id:
                selected = [target_id] if target_id in selected else []
            edges_by_target = {edge.target_id: edge for edge in group.edges}
            for selected_id in selected:
                if can_handle(selected_id) and _evaluate_edge_condition_sync(edges_by_target[selected_id], message):
                    targets.append(selected_id)

        elif isinstance(group, SingleEdgeGroup):
            edge = group.edges[0]
            if (
                (not target_id or target_id == edge.target_id)
                and can_handle(edge.target_id)
                and _evaluate_edge_condition_sync(edge, message)
            ):
                targets.append(edge.target_id)

        elif isinstance(group, FanInEdgeGroup):
            pass  # Handled separately in the orchestrator loop

        else:
            for edge in group.edges:
                if (
                    edge.source_id == source_id
                    and (not target_id or target_id == edge.target_id)
                    and can_handle(edge.target_id)
                    and _evaluate_edge_condition_sync(edge, message)
                ):
                    targets.append(edge.target_id)

    return targets


def build_agent_executor_response(
    executor_id: str,
    response_text: str | None,
    structured_response: dict[str, Any] | None,
    previous_message: Any,
    *,
    position: int | None = None,
) -> AgentExecutorResponse:
    """Build a legacy text response, leaving upstream application messages untouched.

    Production agent completions retain the actual AgentResponse instead. This
    compatibility helper assigns IDs only to the messages it creates itself.
    """
    final_text: str = response_text or ""
    if structured_response:
        final_text = json.dumps(structured_response)

    assistant_message = Message(role="assistant", contents=[final_text])
    agent_response = AgentResponse(messages=[assistant_message])

    full_conversation: list[Message] = []
    upstream = _upstream_responses(previous_message)
    if upstream is not None:
        for prior in upstream:
            full_conversation.extend(prior.full_conversation)
    elif isinstance(previous_message, str):
        full_conversation.append(
            Message(
                role="user",
                contents=[previous_message],
                message_id=workflow_message_id(WORKFLOW_INPUT_EXECUTOR_ID, 0),
            )
        )
    else:
        full_conversation.extend(
            normalize_messages_input(
                previous_message.messages if isinstance(previous_message, AgentExecutorRequest) else previous_message
            )
        )
    assistant_message.message_id = workflow_message_id(
        executor_id, len(full_conversation) if position is None else position
    )
    full_conversation.append(assistant_message)

    return AgentExecutorResponse(
        executor_id=executor_id,
        agent_response=agent_response,
        full_conversation=full_conversation,
    )


# ============================================================================
# Task Preparation Helpers
# ============================================================================


def _upstream_responses(message: Any) -> list[AgentExecutorResponse] | None:
    """Recognize a chained response or a fan-in batch of chained responses."""
    if isinstance(message, AgentExecutorResponse):
        return [message]
    if isinstance(message, list):
        items = cast(list[Any], message)
        if all(isinstance(item, AgentExecutorResponse) for item in items):
            return cast(list[AgentExecutorResponse], items)
    return None


def _select_context_messages(executor: AgentExecutor, message: AgentExecutorResponse) -> list[Message]:
    """Apply core's projection before assigning any transport-only identities."""
    mode = getattr(executor, "_context_mode", "full")
    if mode == "last_agent":
        return list(message.agent_response.messages) if message.agent_response else []
    if mode == "custom":
        context_filter = getattr(executor, "_context_filter", None)
        if context_filter is None:
            raise ValueError("context_filter must be provided for 'custom' context_mode.")
        return list(context_filter(list(message.full_conversation)))
    return list(message.full_conversation)


def _identify_context_messages(
    prior: AgentExecutorResponse,
    selected: list[Message],
    target: str,
    handoff: int,
    response_ordinal: int,
    ledger: _WorkflowDeliveryLedger,
    *,
    latest_only: bool = False,
) -> list[str]:
    """Associate selected copies with source occurrences, never rewrite their IDs."""
    ids, latest_ids = ledger.identify(prior)
    if latest_only:
        return list(latest_ids)
    matches = _match_occurrences(selected, prior.full_conversation, ids)
    latest = list(prior.agent_response.messages) if prior.agent_response else []
    unmatched = [index for index, occurrence in enumerate(matches) if occurrence is None]
    if unmatched:
        combined_messages = list(prior.full_conversation)
        combined_ids = list(ids)
        full_ids = set(ids)
        for message, occurrence in zip(latest, latest_ids, strict=True):
            if occurrence not in full_ids:
                combined_messages.append(message)
                combined_ids.append(occurrence)
        latest_matches = _match_occurrences(selected, combined_messages, combined_ids, allow_positional=False)
        for index in unmatched:
            matches[index] = latest_matches[index]
        repeated_matches = _match_occurrences(
            selected, combined_messages, combined_ids, allow_positional=False, preserve_repeated_matches=True
        )
    else:
        repeated_matches = matches
    # Distinct branch aliases can carry one inherited occurrence. Selecting each
    # original slot once must not manufacture repetitions just because a filter
    # also repeats another slot (and thereby disables positional matching).
    # Only use slot multiplicity when that source's selections have direct,
    # unambiguous provenance. Other sources cannot change its ranks. Value-equal
    # copies without that witness retain the fallback for their source.
    alias_sources: dict[int, set[str]] = defaultdict(set)
    alias_slots: Counter[tuple[int, str]] = Counter()
    for message, occurrence_id in zip(prior.full_conversation, ids, strict=True):
        alias_sources[id(message)].add(occurrence_id)
        alias_slots[id(message), occurrence_id] += 1
    sources = [occurrence or repeated_matches[index] for index, occurrence in enumerate(matches)]
    indirect_sources = {
        source
        for message, source in zip(selected, sources, strict=True)
        if source is not None and alias_sources.get(id(message)) != {source}
    }
    alias_selections: Counter[tuple[int, str]] = Counter()
    repetitions: dict[str, int] = defaultdict(int)
    selected_ids: list[str] = []
    for index, occurrence in enumerate(matches):
        source = sources[index]
        if source is None:
            selected_ids.append(ledger.occurrence("selection", target, handoff, response_ordinal, index))
            continue
        rank = repetitions[source]
        repetitions[source] += 1
        if source not in indirect_sources:
            alias = (id(selected[index]), source)
            rank = alias_selections[alias] // alias_slots[alias]
            alias_selections[alias] += 1
            if rank == 0:
                occurrence = source
        # Repeating a known source is a distinct selection occurrence, but not a
        # new occurrence on every handoff. Filtering an earlier, equal-valued
        # source must not renumber this source's repeated selections.
        if occurrence is None:
            occurrence = ledger.selection_copies.setdefault(
                (target, source, rank),
                ledger.occurrence("selection", target, handoff, response_ordinal, index),
            )
        selected_ids.append(occurrence)
    return selected_ids


_AGENT_TASK_MESSAGE_PREVIEW_LIMIT = 1024


def _prepare_agent_task(
    ctx: WorkflowOrchestrationContext,
    executor: AgentExecutor,
    executor_id: str,
    message: Any,
    workflow_name: str,
    delivery_ledger: _WorkflowDeliveryLedger | None = None,
    metadata: TaskMetadata | None = None,
) -> Any:
    """Prepare an agent task for execution via the context adapter.

    The agent entity is addressed by the workflow-scoped identity
    ``{workflow_name}-{executor_id}`` so two co-hosted workflows that reuse an
    executor id dispatch to distinct entities (the entity layer prefixes this with
    ``dafx-``). The session *key* stays the orchestration instance id, so
    conversation state remains isolated per run.
    """
    if delivery_ledger is None:
        delivery_ledger = _WorkflowDeliveryLedger(instance_id=ctx.instance_id)
    staged = delivery_ledger.fork()
    staged.instance_id = ctx.instance_id
    if metadata is not None:
        options = getattr(executor.agent, "default_options", None)
        response_format = (
            cast(Mapping[str, Any], options).get("response_format") if isinstance(options, Mapping) else None
        )
        if isinstance(response_format, type) and issubclass(response_format, BaseModel):
            metadata.response_format = response_format
        if metadata.is_hitl_response:
            message = _prepare_agent_hitl_message(executor_id, message, staged)
            if message is None:
                metadata.skip_dispatch = True
                delivery_ledger.__dict__.update(staged.__dict__)
                return None
    upstream = _upstream_responses(message)
    handoff = staged.handoffs.get(executor_id, 0)
    cached_messages, cached_ids = staged.cached.get(executor_id, ([], []))
    selected_context = list(cached_messages)
    selected_ids = list(cached_ids)
    if upstream is None:
        inputs = normalize_messages_input(message.messages if isinstance(message, AgentExecutorRequest) else message)
        selected_context.extend(inputs)
        selected_ids.extend(staged.occurrence("input", executor_id, handoff, i) for i in range(len(inputs)))
    else:
        for response_ordinal, prior in enumerate(upstream):
            selected = _select_context_messages(executor, prior)
            selected_context.extend(selected)
            selected_ids.extend(
                _identify_context_messages(
                    prior,
                    selected,
                    executor_id,
                    handoff,
                    response_ordinal,
                    staged,
                    latest_only=getattr(executor, "_context_mode", "full") == "last_agent",
                )
            )

    cache_only = isinstance(message, AgentExecutorRequest) and not message.should_respond
    context_messages: list[dict[str, Any]] | None = []
    context_message_ids: list[str] | None = []
    pending_keys: set[tuple[str, str]] = set()
    message_content = ""
    sent = staged.sent.get(executor_id, set())
    for selected, occurrence in zip(selected_context, selected_ids, strict=True):
        key = (occurrence, message_identity(selected))
        if key in sent or key in pending_keys:
            continue
        context_messages.append(selected.to_dict())
        context_message_ids.append(occurrence)
        pending_keys.add(key)
        message_content = selected.text[:_AGENT_TASK_MESSAGE_PREVIEW_LIMIT]

    if isinstance(message, str) and message and not cached_messages:
        # Keep the original preview, but deliver the normalized input with its
        # occurrence receipt just like every other agent input. Dropping these
        # keys lets a cycle ingest the initial user message a second time.
        message_content = message

    task = None
    if not cache_only:
        scoped_id = workflow_scoped_executor_id(workflow_name, executor_id)
        task = ctx.prepare_agent_task(
            scoped_id, message_content, ctx.instance_id, context_messages, context_message_ids=context_message_ids
        )
    if cache_only:
        staged.cached[executor_id] = (selected_context, selected_ids)
    else:
        staged.cached.pop(executor_id, None)
        if pending_keys:
            staged.sent[executor_id] = sent | pending_keys
    staged.handoffs[executor_id] = handoff + 1
    delivery_ledger.__dict__.update(staged.__dict__)
    if metadata is not None:
        metadata.selected_context = selected_context
        metadata.selected_context_ids = selected_ids
        metadata.invocation_ordinal = handoff
    return task


def _prepare_activity_task(
    ctx: WorkflowOrchestrationContext,
    executor_id: str,
    message: Any,
    source_executor_id: _MessageSources,
    shared_state_snapshot: dict[str, Any] | None,
    workflow_name: str,
    address: dict[str, str],
    delivery_ledger: _WorkflowDeliveryLedger | None = None,
) -> Any:
    """Prepare an activity task for execution via the context adapter.

    The activity is dispatched under the workflow-scoped name
    ``dafx-{workflow_name}-{executor_id}`` so two co-hosted workflows that reuse an
    executor id register and dispatch to distinct activity functions.
    """
    staged = delivery_ledger.fork() if delivery_ledger is not None else None
    activity_input = {
        "executor_id": executor_id,
        "message": serialize_value(staged.forwarding_input(message) if staged else message),
        "shared_state_snapshot": shared_state_snapshot,
        "source_executor_ids": list(_source_executor_ids(source_executor_id)),
        "is_hitl_response": isinstance(source_executor_id, _HITLMessageSources),
        # host_context addresses the *root* (HTTP-routable) orchestration so an executor
        # can build a HITL respond URL (see CapturingRunnerContext.host_metadata):
        # instance_id / workflow_name name the top-level instance, and
        # request_path_prefix is the accumulated ``{executor}~{ordinal}~`` hops from the
        # root down to this workflow level. For a top-level workflow the prefix is empty,
        # so this reduces to addressing the instance directly.
        "host_context": {
            HOST_METADATA_INSTANCE_ID: address["root_instance_id"],
            HOST_METADATA_WORKFLOW_NAME: address["root_workflow_name"],
            HOST_METADATA_REQUEST_PATH_PREFIX: address["request_path_prefix"],
        },
    }
    activity_input_json = json.dumps(activity_input, allow_nan=False)
    activity_name = workflow_executor_activity_name(workflow_name, executor_id)
    task = ctx.prepare_activity_task(activity_name, activity_input_json)
    if delivery_ledger is not None and staged is not None:
        delivery_ledger.__dict__.update(staged.__dict__)
    return task


def _prepare_subworkflow_task(
    ctx: WorkflowOrchestrationContext,
    executor: WorkflowExecutor,
    message: Any,
    child_instance_id: str,
    child_address: dict[str, str],
    delivery_ledger: _WorkflowDeliveryLedger | None = None,
) -> Any:
    """Prepare a child-orchestration task that runs a ``WorkflowExecutor``'s inner workflow.

    The inner workflow runs as its own durable orchestration (``dafx-{innerName}``),
    so its executors are independently durable/observable. The node's message is
    serialized and wrapped in a marker so the child orchestrator reconstructs the
    original typed object (trusted internal input). A sibling address marker carries
    the root instance / workflow name and this child's request-path prefix, so an
    executor inside the child can build a respond URL that targets the top-level
    instance with a qualified request id.
    """
    staged = delivery_ledger.fork() if delivery_ledger is not None else None
    inner_orchestration_name = workflow_orchestrator_name(executor.workflow.name)
    child_input = {
        SUBWORKFLOW_INPUT_KEY: serialize_value(staged.forwarding_input(message) if staged else message),
        SUBWORKFLOW_ADDRESS_KEY: child_address,
    }
    validate_workflow_json(child_input)
    task = ctx.call_sub_orchestrator(
        inner_orchestration_name, wrap_workflow_input(child_input), instance_id=child_instance_id
    )
    if delivery_ledger is not None and staged is not None:
        delivery_ledger.__dict__.update(staged.__dict__)
    return task


# ============================================================================
# Result Processing Helpers
# ============================================================================


def _raise_for_agent_failure(agent_response: AgentResponse | dict[str, Any], executor_id: str) -> None:
    """Reject terminal durable results before reducing them to downstream text."""
    if isinstance(agent_response, AgentResponse):
        properties: dict[str, Any] = agent_response.additional_properties
        error_codes = [
            content.error_code
            for message in agent_response.messages
            if message.role != "tool"
            for content in message.contents
            if content.type == "error"
        ]
    elif isinstance(agent_response, dict) and agent_response.get("type") == "agent_response":
        properties = cast(dict[str, Any], agent_response.get("additional_properties") or {})
        messages = cast(list[dict[str, Any]], agent_response.get("messages") or [])
        error_codes = [
            content.get("error_code")
            for message in messages
            if isinstance(message, dict) and message.get("role") != "tool"
            for content in cast(list[dict[str, Any]], message.get("contents") or [])
            if isinstance(content, dict) and content.get("type") == "error"
        ]
    else:
        return

    status = properties.get("durable_status")
    if status == "already_completed" or "response_expired" in error_codes:
        raise RuntimeError(f"Agent executor {executor_id!r} returned an expired durable response.")
    if status == "error" or error_codes:
        raise RuntimeError(f"Agent executor {executor_id!r} returned a terminal runtime error.")


def _process_agent_response(
    agent_response: AgentResponse | dict[str, Any],
    executor_id: str,
    message: Any,
    delivery_ledger: _WorkflowDeliveryLedger,
    metadata: TaskMetadata | None = None,
) -> ExecutorResult:
    """Emit core's selected cache plus the unaltered agent response messages."""
    _raise_for_agent_failure(agent_response, executor_id)
    if isinstance(agent_response, dict) and agent_response.get("type") == "agent_response":
        agent_response = load_agent_response(agent_response)
    if isinstance(agent_response, dict):
        value = agent_response.get("value")
        model_dump = getattr(value, "model_dump", None)
        if callable(model_dump):
            value = model_dump()
        text = json.dumps(value) if isinstance(value, dict) else agent_response.get("text") or ""
        agent_response = AgentResponse(messages=[Message("assistant", [text])])

    requests = agent_response.user_input_requests
    if requests:
        pending = dict(delivery_ledger.pending_agent_requests.get(executor_id, {}))
        events: list[dict[str, Any]] = []
        for request in requests:
            request_id = request.id
            if not isinstance(request_id, str) or not request_id:
                raise ValueError(f"Agent executor {executor_id!r} returned a user input request without an id.")
            if request_id in pending:
                raise ValueError(f"Agent executor {executor_id!r} returned a duplicate user input request id.")
            pending[request_id] = request
            event = serialize_workflow_event(
                WorkflowEvent.request_info(
                    request_id=request_id, source_executor_id=executor_id, request_data=request, response_type=Content
                )
            )
            event["request_type"] = f"{Content.__module__}:{Content.__name__}"
            events.append(event)
        delivery_ledger.pending_agent_requests[executor_id] = pending
        return ExecutorResult(
            executor_id=executor_id,
            output_message=None,
            activity_result={"pending_request_info_events": events, "events": events},
            task_type=TaskType.AGENT,
        )

    if metadata is None or metadata.selected_context is None or metadata.selected_context_ids is None:
        raise ValueError("Agent completion requires its prepared logical context.")
    if metadata.response_format is not None:
        agent_response = copy(agent_response)
        ensure_response_format(metadata.response_format, f"{executor_id}:{metadata.invocation_ordinal}", agent_response)
    latest_ids = [
        delivery_ledger.occurrence("agent", executor_id, metadata.invocation_ordinal, index)
        for index in range(len(agent_response.messages))
    ]
    output_message = AgentExecutorResponse(
        executor_id,
        agent_response,
        full_conversation=[*metadata.selected_context, *agent_response.messages],
    )
    delivery_ledger.remember(output_message, [*metadata.selected_context_ids, *latest_ids], latest_ids)

    return ExecutorResult(
        executor_id=executor_id,
        output_message=output_message,
        activity_result=None,
        task_type=TaskType.AGENT,
    )


def _process_activity_result(
    result_json: str | None,
    executor_id: str,
    shared_state: dict[str, Any] | None,
    workflow_outputs: list[Any],
) -> ExecutorResult:
    """Process an activity result and apply shared state updates."""
    result = json.loads(result_json) if result_json else None
    validate_workflow_json(result)

    if shared_state is not None and result:
        if result.get("shared_state_updates"):
            updates = result["shared_state_updates"]
            logger.debug("[workflow] Applying SharedState updates from %s: %s", executor_id, updates)
            shared_state.update(updates)
        if result.get("shared_state_deletes"):
            deletes = result["shared_state_deletes"]
            logger.debug("[workflow] Applying SharedState deletes from %s: %s", executor_id, deletes)
            for key in deletes:
                shared_state.pop(key, None)

    if result and result.get("outputs"):
        workflow_outputs.extend(result["outputs"])

    return ExecutorResult(
        executor_id=executor_id,
        output_message=None,
        activity_result=result,
        task_type=TaskType.ACTIVITY,
        source_message=None,
    )


def _unpack_subworkflow_result(child_result: Any) -> tuple[list[Any], list[dict[str, Any]]]:
    """Split a child orchestration's return value into ``(outputs, events)``.

    A child run by this engine returns a :data:`SUBWORKFLOW_RESULT_KEY` envelope of
    ``{"outputs": [...], "events": [...]}``. A bare list / ``None`` (a child that
    produced no envelope, or a defensively-handled legacy shape) is treated as
    outputs with no events.
    """
    if isinstance(child_result, dict):
        envelope = cast("dict[str, Any]", child_result)
        if envelope.get(SUBWORKFLOW_RESULT_KEY):
            raw_outputs = envelope.get("outputs")
            outputs = cast("list[Any]", raw_outputs) if isinstance(raw_outputs, list) else []
            raw_events = envelope.get("events")
            events = cast("list[dict[str, Any]]", raw_events) if isinstance(raw_events, list) else []
            return outputs, events
    if isinstance(child_result, list):
        return cast("list[Any]", child_result), []
    if child_result is None:
        return [], []
    return [child_result], []


def _classify_workflow_output(workflow: Workflow, executor_id: str) -> str | None:
    """Use core's yield designation for both agent and direct child outputs."""
    # A truthy mock return is not an explicit designation.
    if workflow.is_terminal_executor(executor_id) is True:
        return "output"
    if workflow.is_intermediate_executor(executor_id) is True:
        return "intermediate"
    return None


def _process_subworkflow_result(
    child_result: Any,
    executor: WorkflowExecutor,
    workflow_outputs: list[Any],
    workflow: Workflow | None = None,
) -> ExecutorResult:
    """Process a child orchestration's result into an ``ExecutorResult``.

        The child orchestration returns a result envelope (see
        :data:`SUBWORKFLOW_RESULT_KEY`) carrying the inner workflow's outputs (a list of
        already encoded activity values or generated agent response envelopes) plus
        its accumulated event timeline. Mirroring the in-process
        :class:`~agent_framework.WorkflowExecutor`:

        * ``allow_direct_output`` is ``False`` (default): each inner output becomes a
            message routed through the ``WorkflowExecutor`` node's outgoing edges.
        * ``allow_direct_output`` is ``True``: each inner output follows the parent
            workflow's yield designation for this node (output, intermediate, or hidden).
            Omitting ``workflow`` retains the helper's legacy direct-output behavior.

    The inner workflow's *intermediate* events are bubbled into the parent's event
    stream **re-tagged with this node's id** (``executor.id``), matching the
    in-process ``WorkflowExecutor`` which forwards child intermediate emissions as
    ``WorkflowEvent("intermediate", executor_id=self.id, ...)`` so an outer observer
    sees nested progress without needing to know the child's internal executor
    layout. Other inner event types are intentionally not re-emitted: inner *outputs*
    already flow back as this node's outputs/messages above, and inner lifecycle
    events (invoked/completed) are child-internal detail.
    """
    validate_workflow_json(child_result)
    outputs, child_events = _unpack_subworkflow_result(child_result)

    sent_messages: list[dict[str, Any]] = []
    output_events: list[dict[str, Any]] = []
    if executor.allow_direct_output:
        event_type = _classify_workflow_output(workflow, executor.id) if workflow is not None else "output"
        # Inner outputs are already encoded. Reuse them without decoding/re-pickling
        # a portable agent response or a checkpoint value.
        if event_type == "output":
            workflow_outputs.extend(outputs)
        if workflow is not None and event_type is not None:
            output_events = [{"type": event_type, "executor_id": executor.id, "data": output} for output in outputs]
    else:
        # Route each inner output as a message from the node; _route_result_messages
        # deserializes each "message" value before routing through edge groups.
        sent_messages = [{"message": output, "target_id": None, "source_id": executor.id} for output in outputs]

    # Bubble the child's intermediate events up, re-tagged with this node's id (see
    # docstring). These already-serialized event dicts are appended to the parent's
    # timeline by the caller via append_activity_events (which re-stamps iteration).
    bubbled_events = [
        {**event, "executor_id": executor.id} for event in child_events if event.get("type") == "intermediate"
    ]

    return ExecutorResult(
        executor_id=executor.id,
        output_message=None,
        activity_result={"sent_messages": sent_messages, "outputs": [], "events": [*output_events, *bubbled_events]},
        task_type=TaskType.SUBWORKFLOW,
    )


# ============================================================================
# Routing Helpers
# ============================================================================


def _route_result_messages(
    result: ExecutorResult,
    workflow: Workflow,
    next_pending_messages: dict[str, list[tuple[Any, _MessageSources]]],
    fan_in_pending: dict[str, dict[str, list[tuple[Any, str]]]],
    delivery_ledger: _WorkflowDeliveryLedger | None = None,
) -> None:
    """Route messages from an executor result to their targets."""
    executor_id = result.executor_id
    messages_to_route: list[tuple[Any, str | None]] = []

    if result.output_message:
        messages_to_route.append((result.output_message, None))

    if result.activity_result and result.activity_result.get("sent_messages"):
        for msg_data in result.activity_result["sent_messages"]:
            # Missing payloads are ignored, but an explicit None is a message,
            # including when a child workflow yields None to its parent.
            if "message" in msg_data:
                sent_msg = deserialize_value(msg_data["message"])
                target_id = msg_data.get("target_id")
                messages_to_route.append((sent_msg, target_id))

    for msg_to_route, explicit_target in messages_to_route:
        logger.debug("Routing output from %s", executor_id)
        if delivery_ledger is not None:
            for response in _upstream_responses(msg_to_route) or []:
                delivery_ledger.identify(response, result.source_message, scope=result.child_instance_id)

        for group in workflow.edge_groups:
            if (
                isinstance(group, FanInEdgeGroup)
                and executor_id in group.source_executor_ids
                and (not explicit_target or explicit_target in group.target_executor_ids)
                and workflow.executors[group.target_executor_ids[0]].can_handle(
                    WorkflowMessage(data=[msg_to_route], source_id=executor_id)
                )
            ):
                fan_in_pending[group.id][executor_id].append((msg_to_route, executor_id))
                logger.debug("Accumulated message for FanIn group %s from %s", group.id, executor_id)

        targets = route_message_through_edge_groups(
            workflow.edge_groups, executor_id, msg_to_route, explicit_target, executors=workflow.executors
        )

        # Core flushes the whole buffer on the arrival completing the source set.
        # Later messages, even in this result, belong to the next batch.
        _check_fan_in_ready(workflow, fan_in_pending, next_pending_messages)

        for target_id in targets:
            logger.debug("Routing to %s", target_id)
            if target_id not in next_pending_messages:
                next_pending_messages[target_id] = []
            next_pending_messages[target_id].append((msg_to_route, [executor_id]))


def _check_fan_in_ready(
    workflow: Workflow,
    fan_in_pending: dict[str, dict[str, list[tuple[Any, str]]]],
    next_pending_messages: dict[str, list[tuple[Any, _MessageSources]]],
) -> None:
    """Check if any FanInEdgeGroups are ready and deliver their messages."""
    for group in workflow.edge_groups:
        if not isinstance(group, FanInEdgeGroup):
            continue

        pending_sources = fan_in_pending.get(group.id, {})

        if not all(src in pending_sources and pending_sources[src] for src in group.source_executor_ids):
            continue

        aggregated: list[Any] = []
        source_ids = [edge.source_id for edge in group.edges]
        for src in source_ids:
            for msg, _ in pending_sources[src]:
                aggregated.append(msg)

        target_id = group.target_executor_ids[0]
        logger.debug("FanIn group %s ready, delivering %d messages to %s", group.id, len(aggregated), target_id)

        if target_id not in next_pending_messages:
            next_pending_messages[target_id] = []

        # Core identifies contributing edges, not one source per buffered message.
        next_pending_messages[target_id].append((aggregated, source_ids))

        fan_in_pending[group.id] = defaultdict(list)


# ============================================================================
# HITL Helpers
# ============================================================================


def _collect_hitl_requests(
    result: ExecutorResult,
    pending_hitl_requests: dict[str, PendingHITLRequest],
) -> None:
    """Apply checkpointed admission before collecting any new activity requests."""
    if result.activity_result and "hitl_admission" in result.activity_result:
        outcome = result.activity_result["hitl_admission"]
        if not isinstance(outcome, dict):
            raise ValueError("Malformed checkpointed HITL admission outcome.")
        outcome = cast(dict[str, Any], outcome)
        request_id = outcome.get("request_id")
        status = outcome.get("status")
        if (
            set(outcome) != {"request_id", "status"}
            or not isinstance(request_id, str)
            or status not in ("accepted", "invalidreply")
        ):
            raise ValueError("Malformed checkpointed HITL admission outcome.")
        pending = pending_hitl_requests.get(request_id)
        if pending is None or pending.source_executor_id != result.executor_id:
            raise ValueError("Checkpointed HITL admission does not match an outstanding request.")
        if status == "invalidreply":
            # The activity rejected before invoking the handler. Retain the
            # original request, not a descriptor or request supplied by the reply.
            return
        del pending_hitl_requests[request_id]

    if result.activity_result and result.activity_result.get("pending_request_info_events"):
        for req_data in result.activity_result["pending_request_info_events"]:
            request_id = req_data.get("request_id")
            if request_id:
                existing = pending_hitl_requests.get(request_id)
                if existing is not None:
                    raise ValueError("User input request id collides with an outstanding workflow request.")
                response_type = req_data.get("response_type")
                if response_type is not None:
                    # Invalid producer metadata is not a retryable human reply.
                    # Fail before publishing a request that nobody can answer.
                    deserialize_response_type(response_type)
                pending_hitl_requests[request_id] = PendingHITLRequest(
                    request_id=request_id,
                    source_executor_id=req_data.get("source_executor_id", result.executor_id),
                    request_data=req_data.get("data"),
                    request_type=req_data.get("request_type"),
                    response_type=response_type,
                    task_type=result.task_type,
                )
                logger.debug(
                    "Collected HITL request %s from executor %s",
                    request_id,
                    result.executor_id,
                )


def _route_hitl_response(
    hitl_request: PendingHITLRequest,
    raw_response: Any,
    pending_messages: dict[str, list[tuple[Any, _MessageSources]]],
    *,
    validation_error: bool = False,
) -> None:
    """Route a HITL response back to the source executor's @response_handler."""
    response_message = {
        "request_id": hitl_request.request_id,
        "original_request": hitl_request.request_data,
        "response": raw_response,
        "response_type": hitl_request.response_type,
    }
    if validation_error:
        # Only admission sets this field on the internal dispatch envelope.
        # Identically named fields inside a user's response remain opaque data.
        response_message["validation_error"] = True

    target_id = hitl_request.source_executor_id
    if target_id not in pending_messages:
        pending_messages[target_id] = []

    source_id = f"{SOURCE_HITL_RESPONSE}_{hitl_request.request_id}"
    pending_messages[target_id].append((response_message, _HITLMessageSources([source_id])))

    logger.debug(
        "Routed HITL response for request %s to executor %s",
        hitl_request.request_id,
        target_id,
    )


# ============================================================================
# Message Content Extraction
# ============================================================================


def _extract_message_content(message: Any) -> str:  # pyright: ignore[reportUnusedFunction]
    """Extract text content from various message types."""
    message_content = ""
    if isinstance(message, AgentExecutorResponse) and message.agent_response:
        if message.agent_response.text:
            message_content = message.agent_response.text
        elif message.agent_response.messages:
            message_content = message.agent_response.messages[-1].text or ""
    elif isinstance(message, AgentExecutorRequest) and message.messages:
        message_content = message.messages[-1].text or ""
    elif isinstance(message, dict):
        key_names = list(message.keys())  # type: ignore[union-attr]
        logger.warning("Unexpected dict message in _extract_message_content. Keys: %s", key_names)  # type: ignore
    elif isinstance(message, str):
        message_content = message
    return message_content


def _select_primary_input_type(executor: Executor) -> type | None:
    """Return the executor's primary concrete declared input type, if any.

    The first declared input type that is a concrete class is used; union or
    unannotated types yield ``None`` (the caller then passes the value through
    unchanged).
    """
    for input_type in executor.input_types:
        if isinstance(input_type, type):
            return input_type
    return None


def _try_unwrap_subworkflow_input(raw_value: Any) -> tuple[bool, Any]:
    """Detect and unwrap a sub-orchestration input marker.

    Returns ``(True, inner)`` when ``raw_value`` is the parent-supplied marker
    payload (see :data:`SUBWORKFLOW_INPUT_KEY`), with ``inner`` reconstructed from
    the wrapped, parent-serialized message. Returns ``(False, None)`` otherwise.

    Kept separate from :func:`_coerce_initial_input` so the ``isinstance`` narrowing
    here does not leak into that function's untyped ``raw_value`` coercion path.
    """
    if isinstance(raw_value, dict):
        marker_input = cast("dict[str, Any]", raw_value)
        if SUBWORKFLOW_INPUT_KEY in marker_input:
            return True, deserialize_value(marker_input[SUBWORKFLOW_INPUT_KEY])
    return False, None


def _resolve_workflow_address(initial_message: Any, instance_id: str, workflow_name: str) -> dict[str, str]:
    """Resolve this orchestration's HITL address context.

    Returns ``{root_instance_id, root_workflow_name, request_path_prefix}`` -- the
    values an executor needs to build a respond URL that targets the addressable
    top-level instance with a (possibly qualified) request id:

    * A **child** orchestration receives its address from the parent in the
      :data:`SUBWORKFLOW_ADDRESS_KEY` marker (the root instance/workflow plus the
      ``{executor}~{ordinal}~`` path prefix down to this level), since its own
      ``ctx.instance_id`` is a non-addressable child id.
    * A **top-level** workflow has no such marker (it is stripped from untrusted input
      at the host boundary by :func:`strip_subworkflow_markers`), so it is its own root
      with an empty prefix.
    """
    if isinstance(initial_message, dict):
        marker = cast("dict[str, Any]", initial_message)
        addr = marker.get(SUBWORKFLOW_ADDRESS_KEY)
        if isinstance(addr, dict):
            typed = cast("dict[str, Any]", addr)
            root_instance_id = typed.get("root_instance_id")
            root_workflow_name = typed.get("root_workflow_name")
            request_path_prefix = typed.get("request_path_prefix")
            if (
                isinstance(root_instance_id, str)
                and isinstance(root_workflow_name, str)
                and isinstance(request_path_prefix, str)
            ):
                return {
                    "root_instance_id": root_instance_id,
                    "root_workflow_name": root_workflow_name,
                    "request_path_prefix": request_path_prefix,
                }
    return {
        "root_instance_id": instance_id,
        "root_workflow_name": workflow_name,
        "request_path_prefix": "",
    }


def _coerce_initial_input(workflow: Workflow, raw_value: Any) -> Any:
    """Coerce the client's initial workflow input to the start executor's type.

    A durable workflow runs as a durable orchestration, so its initial payload
    arrives as plain JSON via ``context.get_input()`` -- without the type markers
    that inter-executor messages carry (those are reconstructed by
    :func:`deserialize_value`). This single entry hop therefore needs explicit
    reconstruction to mirror in-process delivery, where the start executor
    receives its declared type:

        * Agent start executors preserve core's typed inputs and message lists. Other
            JSON payloads retain the legacy stringification fallback.
    * Other executors get their primary declared input type reconstructed
      (``dict`` -> Pydantic/dataclass, ``str`` -> ``str``, ...) via
      :func:`reconstruct_to_type`; union/unannotated types pass through unchanged.

    A sub-orchestration payload (a ``WorkflowExecutor`` invoking this workflow as a
    child) carries the node's message wrapped in :data:`SUBWORKFLOW_INPUT_KEY`. That
    is trusted internal data the parent produced with :func:`serialize_value`, so it
    is reconstructed directly to the original typed object -- mirroring the
    in-process ``WorkflowExecutor`` which passes its input straight to the inner
    workflow -- without the HTTP-boundary pickle-marker stripping.
    """
    validate_workflow_numbers(raw_value)
    unwrapped, inner_input = _try_unwrap_subworkflow_input(raw_value)
    if unwrapped:
        return inner_input

    start_executor = workflow.executors.get(workflow.start_executor_id)
    if start_executor is None:
        return raw_value

    if isinstance(start_executor, AgentExecutor):
        if raw_value is None or isinstance(raw_value, (str, Message, AgentExecutorRequest, AgentExecutorResponse)):
            return raw_value
        if isinstance(raw_value, list):
            items = cast(list[Any], raw_value)
            if all(isinstance(item, (str, Message)) for item in items):
                return items
        if isinstance(raw_value, (dict, list)):
            return json.dumps(raw_value)
        return str(raw_value)

    input_type = _select_primary_input_type(start_executor)
    if input_type is None:
        return strip_pickle_markers(raw_value)
    # Raw application JSON is not an encoded checkpoint, even after sanitization.
    # Reconstruct only the declared type, leaving literal response markers opaque.
    return reconstruct_to_type(raw_value, input_type, encoded=False)


# ============================================================================
# HITL Response Handler Execution
# ============================================================================


async def execute_hitl_response_handler(
    executor: Any,
    hitl_message: dict[str, Any],
    shared_state: State,
    runner_context: Any,
) -> Literal["accepted", "invalidreply"]:
    """Validate in the activity, then execute the response handler.

    Only pre-handler input rejection returns ``invalidreply``. Handler lookup,
    handler execution, and output serialization failures remain activity failures.
    The orchestrator checkpoints this outcome, not a typed external reply that
    would need to be revalidated on every replay.
    Activity retry/redelivery may still execute application code again.

    Args:
        executor: The executor instance that has a @response_handler.
        hitl_message: The HITL response message dict.
        shared_state: The shared state for the workflow context.
        runner_context: The runner context for capturing outputs.
    """
    from agent_framework._workflows._workflow_context import WorkflowContext

    if "response" not in hitl_message:
        raise ValueError("HITL response payload is required.")
    original_request_data = hitl_message.get("original_request")
    response_data = hitl_message["response"]
    response_type_str = hitl_message.get("response_type")

    original_request = deserialize_value(original_request_data)
    # Broken producer metadata is a programming/deployment failure, not a human
    # response to retry forever. Resolve it outside the reply-rejection boundary.
    if response_type_str is not None:
        deserialize_response_type(response_type_str)
    if hitl_message.get("validation_error") is True:
        # A malformed external envelope was discarded before dispatch. Do not
        # reinterpret its null placeholder as a valid nullable response or run
        # user validators. Checkpoint the same rejection as typed admission.
        return "invalidreply"
    try:
        response = _deserialize_hitl_response(response_data, response_type_str)
    except (TypeError, ValueError, OverflowError):
        # Never render validation exceptions or payloads into durable results.
        return "invalidreply"

    handler = executor._find_response_handler(original_request, response)

    if handler is None:
        raise ValueError(
            f"No response handler found for HITL response in executor {executor.id!r}. "
            f"Request type: {type(original_request).__name__}, Response type: {type(response).__name__}"
        )

    ctx = WorkflowContext(
        executor=executor,
        source_executor_ids=[SOURCE_HITL_RESPONSE],
        runner_context=runner_context,
        state=shared_state,
        request_id=hitl_message.get("request_id"),
    )

    logger.debug(
        "Invoking response handler for HITL request in executor %s",
        executor.id,
    )
    await handler(response, ctx)
    return "accepted"


def _validate_hitl_response_json(response: Any) -> None:
    """Validate public reply data before normalization or consuming a request.

    An internal Content reply has already crossed its checkpoint boundary, so
    check its public content fields rather than inspecting arbitrary objects.
    """
    if isinstance(response, Content):
        validate_workflow_json(response.to_dict())
    else:
        validate_workflow_numbers(response)


def _load_agent_hitl_content(request_id: str, original_request: Content, raw_response: Any) -> Content:
    """Rebuild a reply using the fixed local Content type, never a supplied type name."""
    _validate_hitl_response_json(raw_response)
    sanitized = strip_pickle_markers(raw_response)
    response = (
        Content.from_text(sanitized)
        if isinstance(sanitized, str)
        else reconstruct_to_type(sanitized, Content, encoded=False)
    )
    if not isinstance(response, Content):
        raise TypeError("Agent user input responses must be Content objects or Content mappings.")
    _validate_hitl_response_json(response)
    if response.type == "function_approval_response" and response.id != request_id:
        raise ValueError("Agent approval response does not match the pending request id.")
    if response.type == "function_result":
        call = original_request.function_call
        call_id = call.call_id if isinstance(call, Content) else original_request.call_id
        if call_id is not None and response.call_id != call_id:
            raise ValueError("Agent function result does not match the pending call id.")
    return response


def _prepare_agent_hitl_message(executor_id: str, message: Any, ledger: _WorkflowDeliveryLedger) -> Message | None:
    """Accumulate replies like core's response handler before scheduling one agent turn."""
    if not isinstance(message, dict):
        raise TypeError("Agent HITL message must be a response envelope.")
    envelope = cast("dict[str, Any]", message)
    request_id = envelope.get("request_id")
    pending = ledger.pending_agent_requests.get(executor_id, {})
    if not isinstance(request_id, str) or request_id not in pending:
        # Duplicate or unknown responses must not resume the agent or erase replies.
        logger.warning("Ignoring unknown or already-handled agent response for executor %s", executor_id)
        return None
    response = _load_agent_hitl_content(request_id, pending[request_id], envelope.get("response"))
    responses = ledger.pending_agent_responses.setdefault(executor_id, [])
    responses.append(response)
    del pending[request_id]
    if pending:
        return None
    role = "tool" if all(reply.type == "function_result" for reply in responses) else "user"
    combined = Message(role=role, contents=list(responses))
    ledger.pending_agent_requests.pop(executor_id, None)
    ledger.pending_agent_responses.pop(executor_id, None)
    # Core replaces its cache on resumption. Durable service/session state stays
    # in the same entity; only this new combined reply is dispatched as a delta.
    ledger.cached.pop(executor_id, None)
    return combined


def _deserialize_hitl_response(response_data: Any, response_type_str: str | dict[str, Any] | None) -> Any:
    """Reconstruct and validate against the recorded server-owned request type.

    The type key comes from a registered executor's request, never the reply.
    JSON strings stay strings. Structured JSON may rebuild the declared model,
    but a broader response handler does not relax the original request contract.
    """
    _validate_hitl_response_json(response_data)
    response = strip_pickle_markers(response_data)
    if response is None and response_data is not None:
        raise ValueError("HITL response contained disallowed pickle/type markers.")
    if response_type_str is None:
        return response
    response_type = resolve_type(response_type_str)
    if response_type is None:
        raise ValueError(f"Cannot resolve the recorded HITL response type {response_type_str!r}.")
    # Core owns both concrete and generic coercion, including int-to-float.
    # Keep only the fixed safe Content/Message constructors. This function runs
    # in the registered response activity, not the replaying event-wait loop.
    response = (
        reconstruct_to_type(response, response_type, encoded=False)
        if response_type is Content or response_type is Message
        else try_coerce_to_type(response, response_type)
    )
    if not is_instance_of(response, response_type):
        raise TypeError(f"HITL response type mismatch: expected {response_type!r}, got {type(response).__name__}.")
    return response


# ============================================================================
# Task Preparation (All Tasks)
# ============================================================================


def _prepare_all_tasks(
    ctx: WorkflowOrchestrationContext,
    workflow: Workflow,
    pending_messages: Mapping[str, Sequence[tuple[Any, _MessageSources]]],
    shared_state: dict[str, Any] | None,
    subworkflow_counter: list[int],
    address: dict[str, str],
    delivery_ledger: _WorkflowDeliveryLedger | None = None,
) -> tuple[list[Any], list[TaskMetadata], list[tuple[str, Any, _MessageSources]]]:
    """Prepare all pending tasks for parallel execution.

    Groups agent messages by executor ID so that only the first message per agent
    runs in the parallel batch.  Additional messages to the same agent are returned
    for sequential processing. A :class:`~agent_framework.WorkflowExecutor` node is
    dispatched as a durable child orchestration (one per message), with a
    deterministic child instance id derived from the parent so replay is stable.

    Args:
        ctx: The orchestration context used to schedule activities, entity calls,
            and child orchestrations.
        workflow: The workflow whose executors are being dispatched.
        pending_messages: Messages to deliver this superstep, grouped by target
            executor id, each paired with all source executor IDs.
        shared_state: Optional dict for cross-executor state sharing.
        subworkflow_counter: A single-element mutable counter, persistent across
            supersteps, used to derive unique deterministic child instance ids.
        address: This orchestration's HITL address context
            (``{root_instance_id, root_workflow_name, request_path_prefix}``). Surfaced
            to activity executors via ``host_context`` and extended by one
            ``{executor}~{ordinal}~`` hop for each dispatched sub-workflow child.
        delivery_ledger: Replay-local agent delivery receipts shared with sequential
            dispatch and later supersteps. Standalone calls default to a fresh ledger.
    """
    # Shared state is already checkpoint-encoded. Validate the complete snapshot
    # before any task kind can be scheduled or advance a dispatch counter.
    validate_workflow_json(shared_state)
    if delivery_ledger is None:
        delivery_ledger = _WorkflowDeliveryLedger(instance_id=ctx.instance_id)
    all_tasks: list[Any] = []
    task_metadata_list: list[TaskMetadata] = []
    remaining_agent_messages: list[tuple[str, Any, _MessageSources]] = []

    agent_messages_by_executor: dict[str, list[tuple[str, Any, _MessageSources]]] = defaultdict(list)

    for executor_id, messages_with_sources in pending_messages.items():
        executor = workflow.executors[executor_id]

        if isinstance(executor, AgentExecutor):
            for message, source_executor_id in messages_with_sources:
                agent_messages_by_executor[executor_id].append((executor_id, message, source_executor_id))
        elif isinstance(executor, WorkflowExecutor):
            for message, source_executor_id in messages_with_sources:
                # Derive a deterministic, globally-unique child instance id. The counter
                # persists across supersteps, so two invocations of the same node (in the
                # same or different supersteps, e.g. fan-out) never collide, and the ids
                # are stable across orchestration replay.
                ordinal = subworkflow_counter[0]
                child_instance_id = f"{ctx.instance_id}::{executor_id}::{ordinal}"
                subworkflow_counter[0] += 1
                # Extend this orchestration's request-path prefix by one hop
                # (``{executor}~{ordinal}~``) so an executor inside the child builds a
                # respond URL qualified all the way back to the root instance.
                child_address = {
                    "root_instance_id": address["root_instance_id"],
                    "root_workflow_name": address["root_workflow_name"],
                    "request_path_prefix": address["request_path_prefix"]
                    + qualify_subworkflow_request_id(executor_id, ordinal, ""),
                }
                logger.debug("Preparing sub-workflow task: %s -> %s", executor_id, child_instance_id)
                task = _prepare_subworkflow_task(
                    ctx, executor, message, child_instance_id, child_address, delivery_ledger
                )
                all_tasks.append(task)
                task_metadata_list.append(
                    TaskMetadata(
                        executor_id=executor_id,
                        message=message,
                        source_executor_id=source_executor_id,
                        task_type=TaskType.SUBWORKFLOW,
                        child_instance_id=child_instance_id,
                        child_ordinal=ordinal,
                    )
                )
        else:
            for message, source_executor_id in messages_with_sources:
                logger.debug("Preparing activity task: %s", executor_id)
                task = _prepare_activity_task(
                    ctx, executor_id, message, source_executor_id, shared_state, workflow.name, address, delivery_ledger
                )
                all_tasks.append(task)
                task_metadata_list.append(
                    TaskMetadata(
                        executor_id=executor_id,
                        message=message,
                        source_executor_id=source_executor_id,
                        task_type=TaskType.ACTIVITY,
                    )
                )

    for executor_id, messages_list in agent_messages_by_executor.items():
        for index, (_, message, source_executor_id) in enumerate(messages_list):
            metadata = TaskMetadata(executor_id, message, source_executor_id, TaskType.AGENT)
            logger.debug("Preparing agent task: %s", executor_id)
            task = _prepare_agent_task(
                ctx,
                cast(AgentExecutor, workflow.executors[executor_id]),
                executor_id,
                message,
                workflow.name,
                delivery_ledger,
                metadata,
            )
            if metadata.skip_dispatch or (isinstance(message, AgentExecutorRequest) and not message.should_respond):
                continue
            all_tasks.append(task)
            task_metadata_list.append(metadata)
            remaining_agent_messages.extend(messages_list[index + 1 :])
            break

    return all_tasks, task_metadata_list, remaining_agent_messages


def _index_subworkflows(task_metadata_list: list[TaskMetadata]) -> dict[str, dict[str, str]]:
    """Publish only this batch's children, keyed by their never-reused run ordinal.

    Completed children disappear rather than leaving addressable stale status.
    A later invocation cannot take their path, even when it reuses a leaf request ID.
    """
    subworkflows: dict[str, dict[str, str]] = {}
    for meta in task_metadata_list:
        if meta.task_type == TaskType.SUBWORKFLOW and meta.child_instance_id is not None:
            if meta.child_ordinal is None:
                raise ValueError("Sub-workflow dispatch is missing its run ordinal.")
            subworkflows.setdefault(meta.executor_id, {})[str(meta.child_ordinal)] = meta.child_instance_id
    return subworkflows


# ============================================================================
# Main Orchestrator
# ============================================================================


def run_workflow_orchestrator(
    ctx: WorkflowOrchestrationContext,
    workflow: Workflow,
    initial_message: Any,
    shared_state: dict[str, Any] | None = None,
) -> Generator[Any, Any, list[Any] | dict[str, Any]]:
    """Traverse and execute the workflow graph as a durable orchestration.

    This is a generator-based orchestrator that works with any host by
    programming against the :class:`WorkflowOrchestrationContext` protocol.

    Supports:
    - SingleEdgeGroup: Direct 1:1 routing with optional condition
    - SwitchCaseEdgeGroup: First matching condition wins
    - FanOutEdgeGroup: Broadcast to multiple targets (parallel execution)
    - FanInEdgeGroup: Aggregates messages from multiple sources
    - SharedState: Cross-executor state sharing (local to orchestration)
    - HITL: Human-in-the-loop via request_info / @response_handler

    Mixed child/parent scheduling uses ready waves. All local tasks in a wave
    read the dispatched state snapshot, then their reported updates/deletes
    merge in dispatch order before any downstream wave or reply handler runs.
    Child tasks have independent state and can remain pending across waves.
    Results ready in the same wave route in dispatch order, preserving each
    result's message order. Across waves, readiness recorded in SDK history
    defines order, not a global join or original child invocation order.
    Ordinary ready messages drain before further local replies are admitted.
    This preserves durable snapshot semantics, not Core's in-process visibility
    of other executors' uncommitted writes. It changes the action graph and is
    not a claim of compatibility with histories written by the join scheduler.

    Args:
        ctx: Host-specific orchestration context adapter.
        workflow: The MAF Workflow instance to execute.
        initial_message: Initial message to send to the start executor. When this
            workflow runs as a sub-workflow, this is the parent-supplied marker
            payload (see :data:`SUBWORKFLOW_INPUT_KEY`).
        shared_state: Optional dict for cross-executor state sharing.

    Returns:
        For a top-level run, the list of workflow outputs collected from executor
        activities and designated agents. For a sub-workflow run (``initial_message`` carries
        :data:`SUBWORKFLOW_INPUT_KEY`), a :data:`SUBWORKFLOW_RESULT_KEY` envelope
        ``{"outputs": [...], "events": [...]}`` so the parent can bubble nested
        progress.
    """
    pending_messages: dict[str, list[tuple[Any, _MessageSources]]] = {
        workflow.start_executor_id: [(_coerce_initial_input(workflow, initial_message), [SOURCE_WORKFLOW_START])]
    }
    workflow_outputs: list[Any] = []
    iteration = 0

    # When this run is itself a sub-workflow (the parent dispatched it via
    # call_sub_orchestrator with a SUBWORKFLOW_INPUT_KEY envelope), the orchestrator
    # returns a SUBWORKFLOW_RESULT_KEY envelope so the parent recovers both the inner
    # outputs and the inner event timeline. A top-level run returns a bare list, so the
    # external client output path is unchanged.
    is_subworkflow = isinstance(initial_message, dict) and SUBWORKFLOW_INPUT_KEY in initial_message

    # Resolve the HITL address context once: a child orchestration inherits the root
    # instance/workflow + path prefix from the parent's address marker; a top-level
    # workflow is its own root with an empty prefix. Threaded into task dispatch so an
    # executor at any depth can build a respond URL targeting the addressable top-level
    # instance.
    workflow_address = _resolve_workflow_address(initial_message, ctx.instance_id, workflow.name)

    # Monotonic, replay-stable counter for deriving child orchestration instance ids;
    # persists across supersteps so repeated sub-workflow invocations never collide.
    subworkflow_counter: list[int] = [0]

    # Rebuilt by executing this generator on replay, not checkpointed separately or
    # attached to the shared Workflow/AgentExecutor objects. Survives cycles and HITL
    # waits within this invocation and is shared by parallel and sequential dispatch.
    delivery_ledger = _WorkflowDeliveryLedger(instance_id=ctx.instance_id)

    # Accumulate workflow events and publish them to the orchestration custom status
    # after each superstep so an external client can stream progress by polling.
    # Non-agent executors are run inside a durable activity that captures their events
    # with data payloads (replayed via append_activity_events); agents contribute
    # lifecycle, request-info and designated output events. Events are per executor / per
    # yielded output, not token-level, and accumulate for the run.
    #
    # Only hosts that stream this timeline (ctx.supports_event_streaming) accumulate
    # and publish it. The Azure Functions host opts out: its custom status is capped
    # at 16 KB and it has no event-streaming endpoint, so accumulating the log would
    # only grow orchestrator memory and overflow the cap on publish.
    live_events: list[dict[str, Any]] = []

    def emit_event(event_type: str, executor_id: str) -> None:
        if not ctx.supports_event_streaming:
            return
        live_events.append({"type": event_type, "executor_id": executor_id, "iteration": iteration})

    def append_activity_events(activity_result: dict[str, Any] | None) -> None:
        # Replay the events captured inside the activity, tagging each with the current
        # superstep iteration so clients can group events by superstep.
        if not ctx.supports_event_streaming or not activity_result:
            return
        captured = activity_result.get("events")
        if not isinstance(captured, list):
            return
        for serialized_event in cast("list[dict[str, Any]]", captured):
            enriched = dict(serialized_event)
            enriched["iteration"] = iteration
            live_events.append(enriched)

    def record_agent_result(result: ExecutorResult, outputs: list[Any] | None = None) -> None:
        if outputs is None:
            outputs = workflow_outputs
        append_activity_events(result.activity_result)
        if result.output_message is not None:
            event_type = _classify_workflow_output(workflow, result.executor_id)
            if event_type == "output" or (event_type == "intermediate" and ctx.supports_event_streaming):
                encoded = serialize_workflow_agent_response(result.output_message.agent_response)
                if event_type == "output":
                    outputs.append(encoded)
                append_activity_events({
                    "events": [{"type": event_type, "executor_id": result.executor_id, "data": encoded}]
                })
        emit_event("executor_completed", result.executor_id)

    def publish_live_status(
        state: str,
        pending_requests: dict[str, Any] | None = None,
        subworkflows: dict[str, dict[str, str]] | None = None,
    ) -> None:
        # This is a replacement snapshot, not an external emission. A fresh SDK
        # context must rebuild it even when the last suspension is in old history.
        # live_events is replay-local and each result contributes exactly once.
        status: dict[str, Any] = {"state": state}
        # Hosts that don't stream the event timeline (e.g. Azure Functions, whose
        # custom status is 16 KB-capped) omit the events key entirely, preserving the
        # compact {state, pending_requests} status those hosts expect.
        if ctx.supports_event_streaming:
            status["events"] = live_events
        if pending_requests is None and pending_hitl_requests:
            pending_requests = {
                req_id: {
                    "request_id": req.request_id,
                    "source_executor_id": req.source_executor_id,
                    "data": req.request_data,
                    "request_type": req.request_type,
                    "response_type": req.response_type,
                }
                for req_id, req in pending_hitl_requests.items()
            }
        if pending_requests is not None:
            status["pending_requests"] = pending_requests
        # Only active children are published. Ordinal keys are global within this
        # parent run, not positions in a per-superstep list.
        if subworkflows:
            status["subworkflows"] = subworkflows
        validate_workflow_json(status)
        ctx.set_custom_status(status)

    fan_in_pending: dict[str, dict[str, list[tuple[Any, str]]]] = {
        group.id: defaultdict(list) for group in workflow.edge_groups if isinstance(group, FanInEdgeGroup)
    }

    pending_hitl_requests: dict[str, PendingHITLRequest] = {}
    # Keep the actual SDK waits across handler iterations. Recreating a losing
    # wait can steal or strand an event, especially during history replay.
    pending_hitl_tasks: dict[str, Any] = {}

    # A durable child stays pending at HITL, unlike Core's WorkflowExecutor,
    # which returns at idle-with-pending-requests. Keep those SDK tasks across
    # parent waves rather than making them a barrier to ordinary edge routing.
    # Children have their own state. Parent writes still commit in dispatch
    # order after ALL local work in a wave, independent of completion order.
    held_children: list[tuple[Any, TaskMetadata]] = []

    def publish_pending_status() -> None:
        publish_live_status(
            "waiting_for_human_input",
            pending_requests={
                req_id: {
                    "request_id": req.request_id,
                    "source_executor_id": req.source_executor_id,
                    "data": req.request_data,
                    "request_type": req.request_type,
                    "response_type": req.response_type,
                }
                for req_id, req in pending_hitl_requests.items()
            },
        )

    def admit_hitl_response(
        request_id: str, raw_response: Any, messages: dict[str, list[tuple[Any, _MessageSources]]]
    ) -> Generator[Any, Any, None]:
        # Replace only the completed wait on rejection. Losing SDK waits must
        # survive both a mixed batch and subsequent handler supersteps.
        del pending_hitl_tasks[request_id]
        hitl_request = pending_hitl_requests[request_id]
        is_agent = isinstance(workflow.executors[hitl_request.source_executor_id], AgentExecutor)
        try:
            _validate_hitl_response_json(raw_response)
            sanitized_response = strip_pickle_markers(raw_response)
            if sanitized_response is None and raw_response is not None:
                raise ValueError("HITL response contained disallowed pickle/type markers.")
            if is_agent:
                original_request = delivery_ledger.pending_agent_requests[hitl_request.source_executor_id][request_id]
                sanitized_response = _load_agent_hitl_content(request_id, original_request, sanitized_response)
        except (TypeError, ValueError):
            logger.warning("Rejected malformed HITL response for request %s. Awaiting a new response.", request_id)
            if is_agent:
                # Agent nodes register entities, not executor activities. Await
                # a private workflow checkpoint before creating a replacement
                # wait: buffered AF waits otherwise resume recursively. Neither
                # the untrusted reply nor the approval ledger crosses this
                # boundary. Keep the request and all losing waits untouched.
                acknowledgement = yield ctx.prepare_activity_task(
                    workflow_hitl_checkpoint_name(workflow.name),
                    json.dumps({"request_id": request_id, "status": "invalidreply"}),
                )
                if acknowledgement != "null":
                    raise ValueError("Malformed internal HITL checkpoint acknowledgement.") from None
            else:
                # Yield the existing admission activity, not another immediately
                # completed buffered wait. The AF SDK recursively resumes those
                # waits, so synchronous rejection can exhaust its call stack.
                # Drop untrusted data before serialization and retain only the
                # server-owned pending request plus a private rejection flag.
                _route_hitl_response(hitl_request, None, messages, validation_error=True)
            return
        if is_agent:
            del pending_hitl_requests[request_id]
        # Non-agent reconstruction can execute user validators. Only the existing
        # registered response activity may do that. Keep its request outstanding
        # until _collect_hitl_requests consumes the checkpointed admission outcome.
        _route_hitl_response(hitl_request, sanitized_response, messages)

    while pending_messages or pending_hitl_requests or held_children:
        # A previously dispatched child may finish at the budget boundary.
        # Waiting does not execute another superstep, dispatching new work does.
        if pending_messages and iteration >= workflow.max_iterations:
            raise WorkflowConvergenceException(f"Workflow did not converge after {workflow.max_iterations} iterations.")
        logger.debug("Orchestrator iteration %d", iteration)
        next_pending_messages: dict[str, list[tuple[Any, _MessageSources]]] = {}

        # Phase 1: Prepare all tasks
        all_tasks, task_metadata_list, remaining_agent_messages = _prepare_all_tasks(
            ctx, workflow, pending_messages, shared_state, subworkflow_counter, workflow_address, delivery_ledger
        )
        # A newly started child spends one wave now, not again on completion.
        # Local response activities may only validate/reject without invoking
        # a handler. Their checkpointed outcome decides whether to spend a wave.
        dispatched = any(meta.task_type == TaskType.SUBWORKFLOW for meta in task_metadata_list)

        # Agents and sub-workflows bypass the per-executor activity, so synthesize their
        # invoked event here; activity executors emit their own events from inside the
        # activity.
        for task_meta in task_metadata_list:
            if task_meta.task_type in (TaskType.AGENT, TaskType.SUBWORKFLOW):
                emit_event("executor_invoked", task_meta.executor_id)

        # Older children retain their dispatch position among ready results.
        # A missing child result is not a hole that blocks unrelated routing.
        all_tasks = [task for task, _ in held_children] + all_tasks
        task_metadata_list = [meta for _, meta in held_children] + task_metadata_list
        held_children = []

        # Phase 2: Execute all tasks in parallel
        all_results: list[ExecutorResult] = []
        collected_result_count = 0
        if all_tasks:
            logger.debug("Executing %d tasks in parallel (agents + activities)", len(all_tasks))
            active_subworkflows = _index_subworkflows(task_metadata_list)
            mixed_batch = bool(active_subworkflows)
            prepared_results: dict[int, ExecutorResult] = {}
            prepared_outputs: dict[int, list[Any]] = {}
            if mixed_batch:
                # Children cannot update parent shared state. Collect local requests
                # as activities finish, but commit ALL local writes in dispatch
                # order before routing or admitting a reply. Then run the next
                # ready wave, even if one or more children still need that work.
                outstanding = dict(enumerate(all_tasks))
                raw_by_index: dict[int, Any] = {}
                local_indices = [
                    index for index, meta in enumerate(task_metadata_list) if meta.task_type != TaskType.SUBWORKFLOW
                ]
                local_committed = False

                while True:
                    if not local_committed and all(index in raw_by_index for index in local_indices):
                        for index in local_indices:
                            if index in prepared_results:
                                continue
                            meta = task_metadata_list[index]
                            output_buffer: list[Any] = []
                            if meta.task_type == TaskType.AGENT:
                                result = _process_agent_response(
                                    raw_by_index[index], meta.executor_id, meta.message, delivery_ledger, meta
                                )
                                record_agent_result(result, output_buffer)
                                _collect_hitl_requests(result, pending_hitl_requests)
                            else:
                                result = _process_activity_result(
                                    raw_by_index[index], meta.executor_id, shared_state, output_buffer
                                )
                                append_activity_events(result.activity_result)
                            result.source_message = meta.message
                            prepared_results[index] = result
                            prepared_outputs[index] = output_buffer
                        local_committed = True

                    # Sequential messages to the same agent belong to this
                    # superstep too. Do not strand their approval requests behind
                    # the child join, or run a reply ahead of their original work.
                    while local_committed and remaining_agent_messages:
                        executor_id, message, sources = remaining_agent_messages.pop(0)
                        meta = TaskMetadata(executor_id, message, sources, TaskType.AGENT)
                        task = _prepare_agent_task(
                            ctx,
                            cast(AgentExecutor, workflow.executors[executor_id]),
                            executor_id,
                            message,
                            workflow.name,
                            delivery_ledger,
                            meta,
                        )
                        if meta.skip_dispatch or (
                            isinstance(message, AgentExecutorRequest) and not message.should_respond
                        ):
                            continue
                        emit_event("executor_invoked", executor_id)
                        index = len(all_tasks)
                        all_tasks.append(task)
                        task_metadata_list.append(meta)
                        outstanding[index] = task
                        local_indices.append(index)
                        local_committed = False
                        # Preserve same-agent sequencing, including ledger updates,
                        # before preparing its next message.
                        break
                    if local_committed and (local_indices or raw_by_index or next_pending_messages):
                        break
                    if not outstanding:
                        break

                    active_subworkflows = _index_subworkflows([task_metadata_list[index] for index in outstanding])
                    publish_live_status(
                        "waiting_for_human_input" if pending_hitl_requests else "running",
                        subworkflows=active_subworkflows,
                    )
                    # Only an idle parent admits replies. Ready ordinary messages
                    # and all local writes in this wave take precedence. Retain
                    # losing waits across waves, including already-ready events.
                    can_reply = local_committed and not remaining_agent_messages
                    if can_reply:
                        for request_id in pending_hitl_requests:
                            if request_id not in pending_hitl_tasks:
                                pending_hitl_tasks[request_id] = ctx.wait_for_external_event(request_id)
                    waiting_tasks = list(outstanding.values())
                    if can_reply:
                        waiting_tasks.extend(pending_hitl_tasks.values())
                    try:
                        if len(waiting_tasks) == 1:
                            completed = waiting_tasks[0]
                            raw_result = yield completed
                        else:
                            completed = yield ctx.task_any(waiting_tasks)
                            raw_result = ctx.get_task_result(completed)
                    except Exception:
                        # Do not advertise actionable requests after a failed
                        # wait. Host termination and already-dispatched work
                        # retain the SDK's cancellation semantics. GeneratorExit
                        # also occurs on normal cold-episode disposal, not only
                        # cancellation, so it must not mutate the snapshot.
                        publish_live_status("failed", pending_requests={})
                        raise

                    index = next((key for key, task in outstanding.items() if task is completed), None)
                    if index is not None:
                        del outstanding[index]
                        meta = task_metadata_list[index]
                        raw_by_index[index] = raw_result
                        if meta.task_type == TaskType.ACTIVITY:
                            # Discovery is independent of the shared-state commit.
                            preview = _process_activity_result(raw_result, meta.executor_id, None, [])
                            _collect_hitl_requests(preview, pending_hitl_requests)
                    else:
                        request_id = next(key for key, task in pending_hitl_tasks.items() if task is completed)
                        yield from admit_hitl_response(request_id, raw_result, next_pending_messages)

                if any(task_metadata_list[index].task_type != TaskType.SUBWORKFLOW for index in outstanding):
                    raise RuntimeError("Only subworkflow tasks may remain outstanding after local work is committed.")
                held_children = [(task, task_metadata_list[index]) for index, task in outstanding.items()]
                # Preserve each result's send order and dispatch order among
                # this wave's ready results. Across waves, history readiness
                # defines order. Holding a ready child behind an earlier pending
                # invocation would deadlock if its downstream work answers that
                # invocation. Core's per-edge send order is not invocation order
                # across independently paused/resumed children.
                ready_indices = sorted(raw_by_index)
                raw_results = [raw_by_index[index] for index in ready_indices]
            else:
                raw_results = yield ctx.task_all(all_tasks)
                ready_indices = list(range(len(all_tasks)))
            logger.debug("Completed %d tasks, holding %d children", len(raw_results), len(held_children))

            for idx, raw_result in zip(ready_indices, raw_results):
                metadata = task_metadata_list[idx]
                prepared = prepared_results.get(idx)
                if prepared is not None:
                    result = prepared
                    workflow_outputs.extend(prepared_outputs[idx])
                elif metadata.task_type == TaskType.AGENT:
                    result = _process_agent_response(
                        raw_result, metadata.executor_id, metadata.message, delivery_ledger, metadata
                    )
                    record_agent_result(result)
                elif metadata.task_type == TaskType.SUBWORKFLOW:
                    subworkflow_executor = cast(WorkflowExecutor, workflow.executors[metadata.executor_id])
                    result = _process_subworkflow_result(raw_result, subworkflow_executor, workflow_outputs, workflow)
                    # Publish classified direct outputs and re-tagged child progress
                    # before the node's completed event, as in core WorkflowExecutor.
                    append_activity_events(result.activity_result)
                    emit_event("executor_completed", metadata.executor_id)
                else:
                    result = _process_activity_result(raw_result, metadata.executor_id, shared_state, workflow_outputs)
                    append_activity_events(result.activity_result)
                result.source_message = metadata.message
                result.child_instance_id = metadata.child_instance_id
                all_results.append(result)
                if metadata.task_type != TaskType.SUBWORKFLOW:
                    admission = result.activity_result.get("hitl_admission") if result.activity_result else None
                    if (
                        not isinstance(admission, dict)
                        or cast(dict[str, Any], admission).get("status") != "invalidreply"
                    ):
                        dispatched = True
                if mixed_batch and prepared is None and metadata.task_type == TaskType.AGENT:
                    # Original activity requests were previewed, agent requests
                    # were collected on commit. Child results have none of their own.
                    _collect_hitl_requests(result, pending_hitl_requests)

            if mixed_batch:
                collected_result_count = len(all_results)

        # Phase 3: Process sequential agent messages
        for executor_id, message, source_executor_id in remaining_agent_messages:
            logger.debug("Processing sequential message for agent: %s", executor_id)
            metadata = TaskMetadata(executor_id, message, source_executor_id, TaskType.AGENT)
            task = _prepare_agent_task(
                ctx,
                cast(AgentExecutor, workflow.executors[executor_id]),
                executor_id,
                message,
                workflow.name,
                delivery_ledger,
                metadata,
            )
            if metadata.skip_dispatch or (isinstance(message, AgentExecutorRequest) and not message.should_respond):
                continue
            emit_event("executor_invoked", executor_id)
            dispatched = True
            agent_response: AgentResponse | dict[str, Any] = yield task
            logger.debug("Agent %s sequential response completed", executor_id)

            result = _process_agent_response(agent_response, executor_id, message, delivery_ledger, metadata)
            all_results.append(result)
            record_agent_result(result)

        # Phase 4: Collect HITL requests
        for result in all_results[collected_result_count:]:
            _collect_hitl_requests(result, pending_hitl_requests)

        # Phase 5: Route results
        for result in all_results:
            _route_result_messages(result, workflow, next_pending_messages, fan_in_pending, delivery_ledger)

        pending_messages = next_pending_messages

        # Publish accumulated events after each superstep. When the workflow is about
        # to pause for human input, the HITL block below publishes the waiting status
        # with the pending requests instead.
        if pending_messages or not pending_hitl_requests:
            publish_live_status("running", subworkflows=_index_subworkflows([meta for _, meta in held_children]))

        # Phase 7: HITL wait
        if not pending_messages and pending_hitl_requests and not held_children:
            logger.debug("Workflow paused for HITL - %d pending requests", len(pending_hitl_requests))

            publish_pending_status()

            while not pending_messages:
                for request_id in pending_hitl_requests:
                    if request_id not in pending_hitl_tasks:
                        pending_hitl_tasks[request_id] = ctx.wait_for_external_event(request_id)

                if len(pending_hitl_tasks) == 1:
                    request_id, waiting = next(iter(pending_hitl_tasks.items()))
                    raw_response = yield waiting
                else:
                    completed = yield ctx.task_any(list(pending_hitl_tasks.values()))
                    request_id = next(key for key, task in pending_hitl_tasks.items() if task is completed)
                    raw_response = ctx.get_task_result(completed)
                yield from admit_hitl_response(request_id, raw_response, pending_messages)

            # Run an admitted handler now, rather than waiting for unrelated human
            # replies. Agent multi-approval turns still wait in the delivery ledger.
            publish_live_status("running")

        # Accumulating approvals or rejecting an invalid response does not run
        # an executor handler. Neither spends the convergence budget.
        if dispatched:
            iteration += 1

    # A sub-workflow returns the outputs + event timeline envelope so the parent can
    # bubble nested progress; a top-level run returns the bare outputs list.
    if is_subworkflow:
        return {SUBWORKFLOW_RESULT_KEY: True, "outputs": workflow_outputs, "events": live_events}
    return workflow_outputs  # noqa: B901
