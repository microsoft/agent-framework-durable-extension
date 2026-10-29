# Copyright (c) Microsoft. All rights reserved.

"""Current-only completed and partial capture prefixes, without migration imports."""

from __future__ import annotations

import json
from collections.abc import Callable, Generator, Mapping, Sequence
from copy import deepcopy
from typing import Any, Protocol

from durabletask.entities import EntityInstanceId
from durabletask.task import when_all

from .._json_payload import JsonPayload
from .._models import RunRequest
from .._workflows.protocol import WORKFLOW_ENGINE_VERSION, unwrap_workflow_input, wrap_workflow_input
from ._native_prefix import (
    decode_prefix_value as _decode,
)
from ._native_prefix import (
    prefix_object as _object,
)
from ._native_prefix import (
    prefix_text as _text,
)
from ._native_prefix import (
    prefix_values_equal as _equal,
)
from ._native_prefix import (
    prepare_prefix_operation as _prepare_operation,
)
from ._native_prefix import (
    validate_prefix_json as _json_value,
)
from ._types import is_list, is_string_keyed_dict

COMPLETED_CONTRACT = "mechanical-native-prefix/4"
FRONTIER_CONTRACT = "mechanical-native-prefix/3"
MIXED_CONTRACT = "mechanical-native-prefix/5"
CLASSIC = "classic-functions-v1"
NATIVE = "native-sdk-v1"
PreparedOperation = tuple[dict[str, Any], Any, Any]


class AcquiredEvent(Protocol):
    """Public defensive history event supplied by the native SDK acquisition."""

    def to_dict(self) -> dict[str, Any]:
        """Return the acquired event's fields, not signed operator metadata."""
        ...


def _acquired_rows(context: Any) -> list[dict[str, Any]]:
    getter: Any = getattr(context, "get_acquired_history", None)
    if not callable(getter):
        raise ValueError("Native capture replay requires context.get_acquired_history().")
    get_history: Callable[[], Sequence[AcquiredEvent]] = context.get_acquired_history
    events = get_history()
    return [dict(event.to_dict(), type=type(event).__name__) for event in events]


def _prepare(
    item: dict[str, Any],
    instance: str,
    requests: set[str],
    tokens: set[str],
    *,
    pending: bool,
    profile: str,
) -> PreparedOperation:
    original = deepcopy(item)
    if profile == NATIVE and item.get("kind") == "activity":
        if item.get("runtime_input_policy") != "native-sdk-exact-input/1":
            raise ValueError("Native activities require exact input validation.")
        argument = item.get("python_input")
        if not _equal(_decode(item.get("source_input"), "activity input"), argument):
            raise ValueError("Native activity argument changed.")
        # Reuse the baseline descriptor/schema validation, not its classic
        # transport policy. Only this validation copy has an envelope.
        item = dict(
            item,
            runtime_input_policy="classic-host-omits-scheduled-input/1",
            source_input=json.dumps([json.dumps(argument)]),
        )
    _, argument, expected = _prepare_operation(
        item,
        instance,
        requests,
        tokens,
        pending=pending,
        completed_children=True,
    )
    if profile == NATIVE and original.get("kind") == "child" and "python_input" in original:
        raise ValueError("Native child descriptors cannot contain a classic converter argument.")
    return original, argument, expected


def _preflight(context: Any, operations: list[dict[str, Any]], profile: str) -> None:
    if profile == CLASSIC:
        for item in operations:
            kind = item["kind"]
            if kind == "activity":
                context.validate_replay_activity(
                    item["task_id"],
                    item["name"],
                    item["source_input"],
                    allow_input_omission=True,
                )
            elif kind == "entity":
                context.validate_replay_entity(
                    item["task_id"],
                    EntityInstanceId.parse(item["entity"]),
                    item["operation"],
                    item["source_input"],
                    item["native_request_id"],
                )
            elif kind == "child":
                context.validate_replay_child(
                    item["task_id"],
                    item["name"],
                    item["instance"],
                    item["input"],
                    allow_input_omission=True,
                )
        return
    rows = _acquired_rows(context)
    starts = [row for row in rows if row["type"] == "ExecutionStartedEvent"]
    if len(starts) != 1:
        raise ValueError("Acquired history must contain one native execution.")
    identity = _object(starts[0].get("orchestration_instance"), "acquired execution")
    if identity.get("instance_id") != context.instance_id or identity.get("execution_id") != context.execution_id:
        raise ValueError("Acquired history does not match runtime execution.")
    schedules: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        if row["type"] in (
            "TaskScheduledEvent",
            "EntityOperationCalledEvent",
            "SubOrchestrationInstanceCreatedEvent",
        ):
            schedules.setdefault(row["event_id"], []).append(row)
    for item in operations:
        kind = item["kind"]
        if kind == "external_wait":
            continue
        matches = schedules.get(item["task_id"], [])
        if len(matches) != 1:
            raise ValueError("Original native operation is absent or ambiguous in acquired history.")
        row = matches[0]
        if kind == "activity":
            valid = (
                row["type"] == "TaskScheduledEvent"
                and row["name"] == item["name"]
                and row.get("input") == item["source_input"]
            )
        elif kind == "child":
            valid = (
                row["type"] == "SubOrchestrationInstanceCreatedEvent"
                and row["name"] == item["name"]
                and row["instance_id"] == item["instance"]
                and row.get("input") == item["input"]
            )
        else:
            valid = (
                row["type"] == "EntityOperationCalledEvent"
                and row["operation"] == item["operation"]
                and row["target_instance_id"] == item["entity"]
                and row["request_id"] == item["native_request_id"]
                and row.get("input") == item["source_input"]
            )
        if not valid:
            raise ValueError("Original native operation identity or input changed in acquired history.")


def _dispatch(context: Any, item: dict[str, Any], argument: Any, profile: str) -> Any:
    if item["kind"] == "activity":
        return context.call_activity(item["name"], input=argument, return_type=JsonPayload)
    if item["kind"] == "child":
        return context.call_sub_orchestrator(
            item["name"], input=argument, instance_id=item["instance"], return_type=JsonPayload
        )
    if item["kind"] == "external_wait":
        return context.wait_for_external_event(item["event_name"], data_type=JsonPayload)
    if context.new_uuid() != item["correlation_id"]:
        raise ValueError("Original public correlation UUID changed.")
    if profile == CLASSIC:
        return context.call_entity(
            EntityInstanceId.parse(item["entity"]),
            item["operation"],
            input=argument,
            request_id=item["native_request_id"],
            return_type=JsonPayload,
        )
    return context.call_entity(
        EntityInstanceId.parse(item["entity"]), item["operation"], input=argument, return_type=JsonPayload
    )


def _prepare_future(item: dict[str, Any], instance: str, tokens: set[str]) -> None:
    required = {"kind", "task_id", "authorization"}
    kind = item.get("kind")
    if kind == "activity":
        required |= {"name", "python_input"}
        _text(item.get("name"), "future activity name")
        _text(item.get("python_input"), "future activity argument")
    elif kind == "child":
        required |= {"name", "instance", "python_input"}
        _text(item.get("name"), "future child name")
        address = _text(item.get("instance"), "future child instance")
        if address == instance or "child:" + address in tokens:
            raise ValueError("Future child address is duplicated or equals the parent.")
        tokens.add("child:" + address)
    elif kind == "entity":
        required |= {"entity", "operation", "message"}
        entity = _text(item.get("entity"), "future entity")
        if str(EntityInstanceId.parse(entity)) != entity or item.get("operation") != "run":
            raise ValueError("Invalid future published entity operation.")
        if not isinstance(item.get("message"), str):
            raise ValueError("Future published entity message must be text.")
    else:
        raise ValueError("Unsupported future operation kind.")
    if set(item) != required or item.get("authorization") != "source-future-operation/1":
        raise ValueError("Future work cannot contain historical input, result, or UUID receipts.")


def _validate_future_acquisition(
    context: Any,
    originals: list[dict[str, Any]],
    future: list[dict[str, Any]],
    profile: str,
) -> dict[int, dict[str, Any]]:
    rows = _acquired_rows(context)
    starts = [row for row in rows if row["type"] == "ExecutionStartedEvent"]
    if len(starts) != 1:
        raise ValueError("Mixed batch requires one authoritative acquired execution.")
    identity = _object(starts[0].get("orchestration_instance"), "acquired execution")
    if identity.get("instance_id") != context.instance_id or identity.get("execution_id") != context.execution_id:
        raise ValueError("Mixed batch acquired execution identity changed.")
    schedules = [
        row
        for row in rows
        if row["type"]
        in (
            "TaskScheduledEvent",
            "EntityOperationCalledEvent",
            "SubOrchestrationInstanceCreatedEvent",
        )
    ]
    origin = 0 if profile == CLASSIC else 1
    if [row["event_id"] for row in schedules] != list(range(origin, origin + len(schedules))):
        raise ValueError("Acquired action history has missing or duplicate operations amidst accepted work.")
    acquired = {row["event_id"]: row for row in schedules}
    if any(item["task_id"] not in acquired for item in originals if item["kind"] != "external_wait"):
        raise ValueError("Accepted prefix is absent from authoritative acquired history.")
    known_future: dict[int, dict[str, Any]] = {}
    for item in future:
        row = acquired.get(item["task_id"])
        if row is None:
            continue
        kind = item["kind"]
        if kind == "entity":
            request = _decode(row.get("input"), "future entity acquired input")
            if isinstance(request, str):
                request = _decode(request, "future entity acquired request")
            request = _object(request, "future entity acquired request")
            valid = (
                row["type"] == "EntityOperationCalledEvent"
                and row.get("target_instance_id") == item["entity"]
                and row.get("operation") == "run"
                and request.get("message") == item["message"]
                and request.get("orchestrationId") == context.instance_id
                and request.get("wait_for_response") is True
                and request.get("enable_tool_calls") is True
                and request.get("role") == "user"
                and request.get("request_response_format") == "text"
                and request.get("options") == {}
                and set(request)
                == {
                    "message",
                    "orchestrationId",
                    "wait_for_response",
                    "enable_tool_calls",
                    "role",
                    "request_response_format",
                    "options",
                    "correlationId",
                    "created_at",
                }
            )
        else:
            argument = _decode(row.get("input"), "future acquired argument")
            if kind == "activity" and profile == CLASSIC:
                if not is_list(argument) or len(argument) != 1:
                    raise ValueError("Known classic future activity has changed transport.")
                argument = _decode(argument[0], "future classic activity argument")
            elif kind == "child" and profile == CLASSIC and isinstance(argument, str):
                # A now-acquired suffix may have been committed by the original
                # classic producer, with its explicit child converter layer.
                argument = _decode(argument, "future classic source child argument")
            elif (
                kind == "child"
                and is_string_keyed_dict(argument)
                and argument.get("_durable_workflow_version") == WORKFLOW_ENGINE_VERSION
            ):
                argument = unwrap_workflow_input(argument)
            valid = (
                row["type"] == ("TaskScheduledEvent" if kind == "activity" else "SubOrchestrationInstanceCreatedEvent")
                and row.get("name") == item["name"]
                and (kind == "activity" or row.get("instance_id") == item["instance"])
                and _equal(argument, item["python_input"])
            )
        if not valid:
            raise ValueError(
                f"Known accepted {kind} effect at task {item['task_id']} does not match "
                "authorized future suffix. Refusing rescheduling.",
            )
        known_future[item["task_id"]] = row
    return known_future


def _dispatch_future(
    context: Any,
    item: dict[str, Any],
    known: dict[int, dict[str, Any]],
    profile: str,
) -> Any:
    kind = item["kind"]
    if kind == "activity":
        return context.call_activity(item["name"], input=item["python_input"], return_type=JsonPayload)
    if kind == "child":
        row = known.get(item["task_id"])
        argument = (
            _decode(row["input"], "known future child argument")
            if row is not None
            else wrap_workflow_input(item["python_input"])
        )
        return context.call_sub_orchestrator(
            item["name"], input=argument, instance_id=item["instance"], return_type=JsonPayload
        )
    correlation = context.new_uuid()
    row = known.get(item["task_id"])
    if row is not None:
        request = _decode(row["input"], "future accepted entity input")
        if isinstance(request, str):
            request = _decode(request, "future accepted entity request")
        if request["correlationId"] != correlation:
            raise ValueError("Known future entity correlation allocator position changed.")
        argument = _decode(row["input"], "known future entity argument")
        if profile == CLASSIC:
            return context.call_entity(
                EntityInstanceId.parse(item["entity"]),
                "run",
                input=argument,
                request_id=row["request_id"],
                return_type=JsonPayload,
            )
        return context.call_entity(
            EntityInstanceId.parse(item["entity"]), "run", input=argument, return_type=JsonPayload
        )
    request = RunRequest(
        message=item["message"],
        correlation_id=correlation,
        orchestration_id=context.instance_id,
        created_at=context.current_utc_datetime,
    )
    argument = request.to_dict()
    return context.call_entity(
        EntityInstanceId.parse(item["entity"]), "run", input=argument, return_type=JsonPayload
    )


def _replay(
    context: Any,
    prefix: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    *,
    instance_id: str,
    execution_id: str,
    frontier: bool,
) -> Generator[Any, Any, dict[str, Any]]:
    instance_id, execution_id = _text(instance_id, "instance_id"), _text(execution_id, "execution_id")
    if context.instance_id != instance_id or context.execution_id != execution_id:
        raise ValueError("Authenticated capture prefix does not match runtime execution.")
    signed = deepcopy(dict(prefix))
    _json_value(signed)
    profile = signed.get("source_transport_profile")
    if profile not in (CLASSIC, NATIVE):
        raise ValueError("Explicit supported source transport profile is required.")
    mixed = frontier and signed.get("contract") == MIXED_CONTRACT
    required = {"contract", "source_transport_profile", "operation_batches", "children"}
    if frontier:
        required.add("pending_batch")
    if mixed:
        required.add("future_batch")
    if (
        set(signed) != required
        or signed["contract"] != (MIXED_CONTRACT if mixed else FRONTIER_CONTRACT if frontier else COMPLETED_CONTRACT)
        or signed["children"] != []
        or checkpoint.get("children") != []
    ):
        raise ValueError("Unsupported capture prefix contract.")
    if not frontier and checkpoint.get("phase") not in ("prepare", "hitl", "sequential_prepare"):
        raise ValueError("Completed prefix requires a semantic future-work or wait frontier.")
    batches: list[list[dict[str, Any]]] = signed["operation_batches"]
    if not isinstance(batches, list):
        raise ValueError("Completed operation groups must be lists.")
    next_id = 0 if profile == CLASSIC else 1
    requests: set[str] = set()
    tokens: set[str] = set()
    prepared: list[list[PreparedOperation]] = []
    originals: list[dict[str, Any]] = []

    def prepare_group(group: list[dict[str, Any]], pending: bool) -> list[PreparedOperation]:
        nonlocal next_id
        if not isinstance(group, list) or not group:
            raise ValueError("Operation groups must be nonempty lists.")
        result: list[PreparedOperation] = []
        for item in group:
            if not is_string_keyed_dict(item):
                raise ValueError("Operation must be an object.")
            operation = _prepare(item, instance_id, requests, tokens, pending=pending, profile=profile)
            if item["kind"] != "external_wait":
                if type(item["task_id"]) is not int or item["task_id"] != next_id:
                    raise ValueError("Original native action IDs are not contiguous for the source profile.")
                next_id += 1
            result.append(operation)
            originals.append(operation[0])
        return result

    for batch in batches:
        prepared.append(prepare_group(batch, False))
    final: list[PreparedOperation] = []
    future: list[dict[str, Any]] = []
    if frontier:
        pending: list[dict[str, Any]] = signed["pending_batch"]
        native = _object(checkpoint.get("nativeBatch"), "nativeBatch")
        future = signed["future_batch"] if mixed else []
        if (
            checkpoint.get("phase") != ("native_mixed_batch" if mixed else "native_batch")
            or not _equal(native.get("operations"), pending + future)
            or (mixed and native.get("accepted_count") != len(pending))
        ):
            raise ValueError("Semantic native batch does not match its authenticated prefix.")
        final = prepare_group(pending, True)
        if not mixed and all("source_result" in item for item, _, _ in final):
            raise ValueError("Pending native batch must contain an unresolved operation.")
        if mixed:
            if not isinstance(future, list) or not future:
                raise ValueError("Mixed batch requires a nonempty future suffix.")
            for item in future:
                _prepare_future(item, instance_id, tokens)
                if type(item["task_id"]) is not int or item["task_id"] != next_id:
                    raise ValueError("Future native task positions must follow the accepted prefix contiguously.")
                next_id += 1
    known = _validate_future_acquisition(context, originals, future, profile) if mixed else {}
    _preflight(context, originals, profile)

    def replay() -> Generator[Any, Any, dict[str, Any]]:
        for group in prepared:
            tasks = [_dispatch(context, item, argument, profile) for item, argument, _ in group]
            joined = when_all(tasks)
            results = joined.get_result() if joined.is_complete else (yield joined)
            for actual, (_, _, expected) in zip(results, group, strict=True):
                if not _equal(actual, expected):
                    raise ValueError("Actual native result does not match the authenticated completed prefix.")
        return {  # noqa: B901
            "children": {},
            "batch": [
                *[_dispatch(context, item, argument, profile) for item, argument, _ in final],
                *[_dispatch_future(context, item, known, profile) for item in future],
            ],
        }

    return replay()


def replay_checkpoint_prefix(
    context: Any,
    prefix: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    *,
    instance_id: str,
    execution_id: str,
) -> Generator[Any, Any, dict[str, Any]]:
    """Replay v4 completed native groups before semantic future work or HITL.

    Authenticate first. Native profile requires the SDK's defensive public
    get_acquired_history() surface. No operator-supplied history is accepted.
    """
    return _replay(context, prefix, checkpoint, instance_id=instance_id, execution_id=execution_id, frontier=False)


def replay_capture_frontier(
    context: Any,
    prefix: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    *,
    instance_id: str,
    execution_id: str,
) -> Generator[Any, Any, dict[str, Any]]:
    """Replay v3 accepted groups or v5 accepted-prefix/future-suffix groups.

    V5 requires authoritative acquired history for both transport profiles.
    Only its genuinely absent suffix is new work. If a previously future call
    is now accepted, validate its actual native argument before replaying it.
    Future descriptors contain no historical input, result, or entity UUID.
    Immediately pass the complete ordered result["batch"] to continue_checkpoint
    with the upgraded SDK's published batch-join behavior. Native failures
    retain the SDK's task-list result and failure semantics.
    """
    return _replay(context, prefix, checkpoint, instance_id=instance_id, execution_id=execution_id, frontier=True)
