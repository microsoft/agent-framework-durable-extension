# Copyright (c) Microsoft. All rights reserved.

"""Durable naming helpers for hosting MAF Workflows.

A hosted workflow maps to durable primitives (an orchestration, plus an activity
or entity per executor) whose names must be **stable** across worker restarts:
durable replay only resumes an in-flight orchestration if the orchestration,
activity, and entity names still resolve to the same functions. This module
centralizes how those names are derived from a workflow name so every host (the
Azure Functions host and the standalone durabletask worker) and the client agree
on one scheme.

Naming scheme (the orchestration name is aligned byte-for-byte with .NET's
``WorkflowNamingHelper``)::

    orchestration:       dafx-{workflowName}
    non-agent activity:  dafx-{workflowName}-{executorId}
    agent entity:        dafx-{workflowName}-{executorId}

The orchestration name is the identifier the Durable Task tooling/UI surfaces, so
it matches .NET exactly. The inner activity/entity names are scoped by workflow in
Python (unlike .NET's bare ``dafx-{executorId}``) so two co-hosted workflows that
reuse an executor id cannot collide.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterator
from typing import Any, cast

__all__ = [
    "DURABLE_NAME_PREFIX",
    "MAX_EXECUTOR_ID_LENGTH",
    "SUBWORKFLOW_REQUEST_SEPARATOR",
    "WORKFLOW_INPUT_EXECUTOR_ID",
    "is_auto_generated_workflow_name",
    "iter_subworkflow_instances",
    "parse_workflow_message_id",
    "qualify_subworkflow_request_id",
    "split_subworkflow_request_id",
    "subworkflow_instance_id",
    "validate_dts_instance_id",
    "validate_executor_id",
    "validate_workflow_name",
    "workflow_executor_activity_name",
    "workflow_message_id",
    "workflow_name_from_orchestrator",
    "workflow_orchestrator_name",
    "workflow_scoped_executor_id",
]

# Shared prefix for every durable name this hosting layer registers. Matches
# .NET's ``WorkflowNamingHelper.OrchestrationFunctionPrefix`` and the existing
# ``AgentSessionId.ENTITY_NAME_PREFIX``.
DURABLE_NAME_PREFIX = "dafx-"

# Identifies the workflow's own input in the conversation chained between agent nodes. It has no
# producing executor, so it carries a reserved id in that position.
WORKFLOW_INPUT_EXECUTOR_ID = "input"

_WORKFLOW_MESSAGE_ID_PREFIX = "wf_"
_WORKFLOW_MESSAGE_ID_RE = re.compile(rf"^{_WORKFLOW_MESSAGE_ID_PREFIX}(?P<executor>.+)_(?P<position>\d+)$")


def workflow_message_id(executor_id: str, position: int) -> str:
    """Build the id for a message the workflow itself puts in the chained conversation.

    Core leaves ``message_id`` unset, so without this an agent node cannot tell context it has
    already recorded from genuinely new input. The position is the message's index in the chained
    conversation, which is fixed once the message joins it and is reproduced identically when the
    orchestrator replays.

    Args:
        executor_id: The node that produced the message, or ``WORKFLOW_INPUT_EXECUTOR_ID``.
        position: The message's index in the chained conversation.

    Returns:
        An id unique within one workflow run.
    """
    return f"{_WORKFLOW_MESSAGE_ID_PREFIX}{executor_id}_{position}"


def parse_workflow_message_id(message_id: str | None) -> tuple[str, int] | None:
    """Recover the producing executor and conversation position from a message id.

    Args:
        message_id: The id to parse, if the message has one.

    Returns:
        The executor id and position, or None when the id was not produced by
        :func:`workflow_message_id`.
    """
    if not message_id:
        return None
    match = _WORKFLOW_MESSAGE_ID_RE.match(message_id)
    if match is None:
        return None
    return match.group("executor"), int(match.group("position"))


# Separator used to qualify a nested sub-workflow's pending HITL request when it is
# bubbled up to the top-level instance (one top-level addressing surface). A qualified id
# is a path of ``{executorId}~{ordinal}`` hops ending in the leaf's bare request id,
# e.g. ``review~0~approve~1~<requestId>``. Both hosts and the client must agree on it
# so a qualified id round-trips: the read side prepends hops; the respond side peels
# them to route the response to the owning child orchestration.
#
# ``~`` (RFC 3986 "unreserved", so URL-path-safe) is deliberately **not** ``::``:
# core emits ``auto::{index}`` request ids for functional ``@workflow`` HITL, so a
# ``::`` separator would mis-parse those leaf ids. ``~`` does not appear in core
# request ids (uuid4 or ``auto::N``); executor ids are validated to exclude it (see
# :func:`validate_executor_id`), so only the structural hops carry the separator.
SUBWORKFLOW_REQUEST_SEPARATOR = "~"

# Executor IDs remain in registered names and semantic HITL paths. This limit is
# independent of backend instance-ID constraints. Generated children use a bounded
# physical identity instead of concatenating the entire ancestry.
MAX_EXECUTOR_ID_LENGTH = 128

_SUBWORKFLOW_INSTANCE_DOMAIN = "dafx/subworkflow-instance/v1"
_SUBWORKFLOW_INSTANCE_PREFIX = "dafxsw_v1_"


def subworkflow_instance_id(parent_instance_id: str, executor_id: str, ordinal: int) -> str:
    """Derive a replay-stable 74-character ASCII identity for a generated child.

    Scheme 1 hashes four UTF-8 fields in order: the fixed domain, exact immediate
    parent ID, exact executor ID, and canonical decimal ordinal. Each field is
    prefixed by its byte length as an unsigned eight-byte big-endian integer.
    Length framing avoids delimiter ambiguity. Never normalize names or truncate
    the digest. These bytes are a replay contract, not an authorization mechanism.
    """
    if not isinstance(parent_instance_id, str) or not parent_instance_id:
        raise ValueError("Parent instance ID must be a non-empty string.")
    if not isinstance(executor_id, str):
        raise ValueError("Executor ID must be a string.")
    validate_executor_id(executor_id)
    if type(ordinal) is not int or ordinal < 0:
        raise ValueError("Child dispatch ordinal must be a nonnegative integer.")
    digest = hashlib.sha256()
    try:
        for field in (_SUBWORKFLOW_INSTANCE_DOMAIN, parent_instance_id, executor_id, str(ordinal)):
            encoded = field.encode("utf-8")
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
    except UnicodeEncodeError as exc:
        raise ValueError("Child identity components must be valid UTF-8 strings.") from exc
    return _SUBWORKFLOW_INSTANCE_PREFIX + digest.hexdigest()


def validate_dts_instance_id(instance_id: str) -> None:
    """Validate an explicit Scheduler root ID without changing the caller's ID."""
    if (
        not isinstance(instance_id, str)
        or not 1 <= len(instance_id) <= 100
        or not instance_id.strip()
        or instance_id.startswith("@")
        or any(not 0x20 <= ord(character) <= 0x7E for character in instance_id)
    ):
        raise ValueError("DTS instance ID must be nonblank, 1-100 printable ASCII characters and not start with '@'.")


# A workflow name is interpolated into durable orchestration/activity/entity names
# *and* into HTTP route segments (``workflow/{workflowName}/run``), so it must be
# conservative enough to be safe in every position: ASCII letters, digits, '_' or
# '-', starting with a letter, at most 63 characters. The length cap leaves room
# for the ``dafx-`` prefix and an ``-{executorId}`` suffix within typical durable
# name limits.
_WORKFLOW_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,62}$")

# Names auto-generated by ``WorkflowBuilder`` when the caller does not pass one,
# e.g. ``"WorkflowBuilder-3f2b1c0a-1234-5678-9abc-def012345678"``. They embed a
# fresh ``uuid4`` per process build, so they are not stable identities and must be
# rejected for durable hosting (see :func:`validate_workflow_name`).
_AUTO_GENERATED_NAME_RE = re.compile(
    r"^WorkflowBuilder-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def workflow_orchestrator_name(workflow_name: str) -> str:
    """Return the durable orchestration name for a workflow.

    Args:
        workflow_name: The workflow's name. Must satisfy
            :func:`validate_workflow_name`.

    Returns:
        ``"dafx-{workflow_name}"``.

    Raises:
        ValueError: If ``workflow_name`` is not a valid, stable workflow name.
    """
    validate_workflow_name(workflow_name)
    return f"{DURABLE_NAME_PREFIX}{workflow_name}"


def workflow_name_from_orchestrator(orchestrator_name: str) -> str | None:
    """Recover the workflow name from a durable orchestration name.

    The inverse of :func:`workflow_orchestrator_name`. Intended to be applied to
    orchestration names (for example a durable instance's ``status.name``); it
    strips the shared :data:`DURABLE_NAME_PREFIX`.

    Args:
        orchestrator_name: A durable orchestration name.

    Returns:
        The workflow name, or ``None`` if ``orchestrator_name`` does not carry the
        expected prefix (so a caller can treat it as "not one of ours").
    """
    if not orchestrator_name.startswith(DURABLE_NAME_PREFIX):
        return None
    name = orchestrator_name[len(DURABLE_NAME_PREFIX) :]
    return name or None


def workflow_scoped_executor_id(workflow_name: str, executor_id: str) -> str:
    """Return the workflow-scoped identity for an executor.

    Inner executors (non-agent activities and agent entities) are scoped by
    workflow so two co-hosted workflows that reuse an ``executor_id`` register and
    dispatch to distinct durable primitives instead of colliding on one global
    name. This is the **unprefixed** identity (e.g. used as
    :class:`~agent_framework_durabletask.AgentSessionId` ``name``, which the entity
    layer then prefixes); see :func:`workflow_executor_activity_name` for the full
    activity function name.

    Args:
        workflow_name: The owning workflow's name.
        executor_id: The executor's id within that workflow.

    Returns:
        ``"{workflow_name}-{executor_id}"``.
    """
    return f"{workflow_name}-{executor_id}"


def workflow_executor_activity_name(workflow_name: str, executor_id: str) -> str:
    """Return the durable activity function name for a non-agent executor.

    Args:
        workflow_name: The owning workflow's name.
        executor_id: The executor's id within that workflow.

    Returns:
        ``"dafx-{workflow_name}-{executor_id}"``.
    """
    return f"{DURABLE_NAME_PREFIX}{workflow_scoped_executor_id(workflow_name, executor_id)}"


def validate_workflow_name(workflow_name: str) -> None:
    """Validate that a workflow name is usable as a stable durable identity.

    The name is **validated and rejected** rather than silently sanitized. A
    workflow name is an identity baked into durable orchestration/activity/entity
    names and HTTP routes, so transforming it could either (a) collapse two
    distinct names into one and reintroduce the cross-workflow collision this
    scheme exists to prevent, or (b) change the resolved name across versions and
    break resume of in-flight instances. A loud error is safer than a silent
    rename.

    Args:
        workflow_name: The candidate name.

    Raises:
        ValueError: If the name is empty, an auto-generated ``WorkflowBuilder``
            name, or contains characters outside
            ``[A-Za-z][A-Za-z0-9_-]{0,62}``.
    """
    if not workflow_name:
        raise ValueError("Workflow name must be a non-empty string.")
    if is_auto_generated_workflow_name(workflow_name):
        raise ValueError(
            f"Workflow name '{workflow_name}' is an auto-generated WorkflowBuilder name, which is "
            "not stable across restarts. Pass an explicit, stable name to WorkflowBuilder(name=...) "
            "before hosting the workflow durably."
        )
    if not _WORKFLOW_NAME_RE.match(workflow_name):
        raise ValueError(
            f"Workflow name '{workflow_name}' is invalid. Use 1-63 characters consisting of ASCII "
            "letters, digits, '_' or '-', and starting with a letter."
        )


def is_auto_generated_workflow_name(workflow_name: str) -> bool:
    """Return whether a name looks like ``WorkflowBuilder``'s auto-generated default.

    ``WorkflowBuilder`` names an otherwise-unnamed workflow
    ``f"WorkflowBuilder-{uuid4()}"``, which changes on every process build and is
    therefore not a stable durable identity.

    Args:
        workflow_name: The candidate name.

    Returns:
        ``True`` if the name matches the auto-generated pattern.
    """
    return bool(_AUTO_GENERATED_NAME_RE.match(workflow_name))


def validate_executor_id(executor_id: str) -> None:
    """Validate a stable executor ID for durable hosting.

    Executor IDs must be nonempty and at most MAX_EXECUTOR_ID_LENGTH characters.
    They must not contain the '~' separator used by qualified nested-HITL request
    IDs. Physical child IDs are
    derived separately, so this does not validate backend instance-ID limits.

    Args:
        executor_id: The executor's id within a hosted workflow.

    Raises:
        ValueError: If the executor ID is not valid for durable hosting.
    """
    if not executor_id:
        raise ValueError("Executor id must be a non-empty string.")
    if SUBWORKFLOW_REQUEST_SEPARATOR in executor_id:
        raise ValueError(
            f"Executor id '{executor_id}' contains the reserved sub-workflow request separator "
            f"'{SUBWORKFLOW_REQUEST_SEPARATOR}', which is used to address nested human-in-the-loop "
            "requests. Rename the executor so its id does not contain that sequence."
        )
    if len(executor_id) > MAX_EXECUTOR_ID_LENGTH:
        raise ValueError(
            f"Executor id '{executor_id[:32]}...' is too long ({len(executor_id)} > "
            f"{MAX_EXECUTOR_ID_LENGTH}). Durable registered names and logical request paths are "
            "derived from it; use a shorter id."
        )


def qualify_subworkflow_request_id(executor_id: str, ordinal: int, inner_request_id: str) -> str:
    """Prepend one sub-workflow hop to a (possibly already-qualified) request id.

    Produces ``{executor_id}~{ordinal}~{inner_request_id}``. ``ordinal`` selects the
    specific child orchestration using the parent's run-wide dispatch ordinal,
    so later invocations cannot reuse retired public addresses.
    ``inner_request_id`` is the child's bare leaf request id or its own
    already-qualified path for deeper nesting.

    Args:
        executor_id: The sub-workflow node's executor id (separator-free; see
            :func:`validate_executor_id`).
        ordinal: The child's never-reused key in the parent's ``subworkflows`` status map.
        inner_request_id: The request id (bare or qualified) within the child.

    Returns:
        The qualified request id one level higher.
    """
    sep = SUBWORKFLOW_REQUEST_SEPARATOR
    return f"{executor_id}{sep}{ordinal}{sep}{inner_request_id}"


def iter_subworkflow_instances(children: Any) -> Iterator[tuple[int, str]]:
    """Read active children keyed by global dispatch ordinal, never legacy slots.

    Old list-shaped status cannot establish a stable address and is deliberately
    unsupported. Falling back to enumerate would reroute a retired path to a new child.
    """
    if not isinstance(children, dict):
        return
    for key, child in cast(dict[Any, Any], children).items():
        if not isinstance(key, str) or not isinstance(child, str) or not child:
            continue
        try:
            ordinal = int(key)
        except ValueError:
            continue
        if ordinal >= 0 and str(ordinal) == key:
            yield ordinal, child


def split_subworkflow_request_id(request_id: str) -> tuple[str, int, str] | None:
    """Peel the outermost sub-workflow hop off a qualified request id.

    The inverse of :func:`qualify_subworkflow_request_id` for a single level.
    Returns ``(executor_id, ordinal, remainder)`` where ``remainder`` is the still
    (possibly) qualified id one level deeper, or ``None`` when ``request_id`` carries
    no well-formed hop -- i.e. it is a bare leaf request id that targets the current
    instance directly. A leaf id may itself contain the separator (e.g. core's
    ``auto::N`` does not, but a custom id could); because only structural hops use the
    ``{executor}~{int-ordinal}~`` shape, a value whose second segment is not an integer
    is treated as a bare leaf rather than a hop.

    Args:
        request_id: A bare or qualified request id.

    Returns:
        ``(executor_id, ordinal, remainder)`` for a qualified id, else ``None``.
    """
    sep = SUBWORKFLOW_REQUEST_SEPARATOR
    if sep not in request_id:
        return None
    parts = request_id.split(sep, 2)
    if len(parts) < 3:
        return None
    executor_id, ordinal_str, remainder = parts
    try:
        ordinal = int(ordinal_str)
    except ValueError:
        return None
    return executor_id, ordinal, remainder
