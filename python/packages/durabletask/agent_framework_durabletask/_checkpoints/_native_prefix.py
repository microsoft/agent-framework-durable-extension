# Copyright (c) Microsoft. All rights reserved.

"""Release-owned mechanical native prefix, not an application replay engine.

Caller obligations
------------------
Authenticate prefix/checkpoint bytes and the trusted definition before calling.
Bind ``instance_id``/``execution_id`` to operator authority, never application
input. Register the native worker with the replay profile matching the acquired
history and start this component before creating any other durable tasks.
Immediately pass its returned handles to checkpoint_target.run(...,
native_child_tasks=handles). The SDK owns control-event buffering and native
join/failure routing. This module never resumes, terminates, cancels, parks,
continues-as-new, polls or captures an application.

Supported source shape
----------------------
Zero or more fully completed ordered activity batches, followed by one ordered
explicit-address child batch, all non-retrying. Native task IDs must be exactly
0..N in creation/yield order, without other prior operations. Each activity
batch is yielded before creating the next batch. Every final child, including
completed siblings, must appear in checkpoint["children"] in identical order.
No timers, locks, retries or arbitrary replay. Completed external waits require
the explicit operation-batch form below, not the activity-only form.
Empty children support a clean HITL continuation after the completed prefix.

Explicit operation batches
--------------------------
{"contract": "mechanical-native-prefix/2", "operation_batches": [[operation]],
 "children": [original_child_descriptor, ...]} supports repeated/fan-out completed
activity/entity batches followed by the original child batch. Each batch is
created and yielded as one task_all, in declared order. Batches are sequential.
Descriptors use contiguous native task_id values from zero across all batches
and children. Activity fields are kind="activity", task_id, name, python_input
(the actual JSON argument VALUE), source_input (exact classic argument envelope),
source_result (exact raw native result), and runtime_input_policy equal to
"classic-host-omits-scheduled-input/1". Entity fields are kind="entity", task_id,
entity (canonical address), operation, python_input (actual JSON argument VALUE),
source_input (raw converter encoding), source_result (raw native result),
native_request_id and correlation_id (canonical UUIDs). Entity argument may be
an object or a JSON object string; it must contain the bound orchestrationId and
correlationId. Native request IDs must be distinct.

Completed external-wait descriptors use kind="external_wait", wait_id (unique
source task-object capture token), event_name and source_result (exact JSON
encoding of the observed wait result). They do NOT have task_id: native waits
are name/FIFO registrations, not numbered scheduled actions. The original
classic Python wait action likewise uses its name rather than a numeric ID.
Register/yield each wait in the explicitly captured batch, compare its actual
result, and consume its actual event before the next batch/current continuation.
Repeated event names are allowed with distinct wait_id values. This prevents
previously consumed approvals remaining buffered for a later same-name wait.

Capture MUST supply the original creation/task_all batch partition explicitly
from a release-owned semantic snapshot/replay hook or non-dispatching trusted
source adapter, plus exact task IDs/arguments/results and source public UUID
allocation order. For waits, capture the public prepare call, task object
identity token, creation/yield grouping and observed wait_checkpointable result.
EventRaised arrival order does NOT identify consumed waits or yield boundaries.
History completion order does NOT identify yield boundaries.
No batch partition is inferred from history or generator frames. This profile
allows exactly one public new_uuid allocation per entity descriptor, in batch
order, with no other public UUID consumers or undeclared operations.
UUIDs for a batch are checked before its tasks are created. All native results
are supplied by history and compared in task-list order. This remains bounded
mechanical replay, not arbitrary SDK control flow or a source-state reader.
Partially completed batches are NOT represented by v2. V3's separate
replay_native_frontier entry below admits a final flat activity/entity/child
task_all without yielding it inside the prefix. V3 additionally admits completed
child operations in its explicit operation_batches, using the original child
descriptor plus kind="child" and required source_result. These are joined and
their actual results checked before advancing to the next batch. V3 preflights
every numbered operation against acquired native creation history before any
replay calls. This extension does not change v2 or its separate child return map.
Task_any/losing waits, creation
across future batches, reuse of an existing task object in multiple yields and
nested task graphs remain unrepresented. A compiler must reject them or emit a
separately specified faithful task-graph contract, never flatten them into
completed batches. A partially fulfilled approval/wait task_all is still not
covered merely because its already-consumed waits can be inventoried.

Versioned form:
  {"contract": "mechanical-native-prefix/1",
   "activity_batches": [[{"task_id": 0, "name": "...",
                         "python_input": <JSON value>,
                         "source_input": <exact backend JSON argument string>,
                         "result": <exact backend JSON result string>}]],
   "children": [{"task_id": 1, "name": "...", "instance": "...",
                 "input": <exact backend JSON argument string>,
                 "python_input": <optional original classic Python argument>}]}

The captured legacy {"activity": descriptor, "children": [...]} form is also
accepted. Optional legacy "completed_sibling" is opaque signed evidence, not a
receipt or a synthesized result. Native history supplies actual completions.

Producer/consumer trust
----------------------
Activity source_input must be the classic one-argument backend envelope
[JSON-encoded python_input]. Child input is one JSON converter layer; optional
python_input documents its original classic argument (one additional layer).
Exact signed source arguments/addresses are operator trust. The native runtime
checks historical task IDs/types/names, not every argument or address. Classic
transports may omit inputs altogether. No omission is claimed runtime checked.
Actual completed activity results ARE compared after the native SDK supplies
them, using one converter decode of the signed raw result. No result is injected.
Native public calls allocate IDs naturally; no private counters are accessed.
The prefix descriptors are JSON, not the whole authenticated semantic state.
Trusted original typed values/tuples in pending messages or shared state remain
typed. Only the child's actual transport binding fields are inspected here.

Completed-agent form
--------------------
Also accepts the captured list [activity0, entity1, activity2], with each
descriptor's kind/task_id and exact source_input/source_result. Activity
python_input is itself a JSON string in this captured form. Entity descriptors
include entity address, operation, python_input, native_request_id and
correlation_id. Only a fully completed entity call with no checkpoint children
is supported. Public new_uuid must reproduce the original Python correlation;
public call_entity(request_id=...) uses the signed ORIGINAL native UUID and
verifies it, address, operation and raw input against EntityOperationCalled.
No old invoker/reader or decoded agent framework type is loaded. Actual native
result is checked against signed source_result, never injected as a receipt.

The original .NET host UUID and public Python correlation use separate
allocators. Public new_uuid reproduces Python's allocator. .NET uses seven
fractional digits, a different namespace and independent counter; historical
JSON exports can lose the seventh digit. Therefore the native entity UUID is
explicit authenticated source metadata, never guessed from truncated time or
substituted with application correlationId. Missing identity fails explicitly.
"""

from __future__ import annotations

import json
import math
from copy import deepcopy
from typing import Any, Generator, Mapping

from .._json_payload import JsonPayload
from ._types import is_list, is_string_keyed_dict

CONTRACT = "mechanical-native-prefix/1"
BATCH_CONTRACT = "mechanical-native-prefix/2"
FRONTIER_CONTRACT = "mechanical-native-prefix/3"
_UNRESOLVED = object()


def _object(value: Any, label: str) -> dict[str, Any]:
    if not is_string_keyed_dict(value):
        raise ValueError(f"{label} must be an object.")
    return value


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a nonempty string.")
    return value


def _json_value(value: Any) -> None:
    if value is None or type(value) in (str, bool, int):
        return
    if type(value) is float and math.isfinite(value):
        return
    if is_list(value):
        for item in value:
            _json_value(item)
        return
    if is_string_keyed_dict(value):
        for item in value.values():
            _json_value(item)
        return
    raise ValueError("Prefix values must be finite JSON values.")


def _decode(raw: Any, label: str) -> Any:
    value = json.loads(_text(raw, label))
    _json_value(value)
    return value


def _equal(left: Any, right: Any) -> bool:
    # Reject bool/int conflation and other Python equality coercions.
    return json.dumps(left, sort_keys=True, allow_nan=False) == json.dumps(right, sort_keys=True, allow_nan=False)


def replay_native_prefix(
    context: Any,
    prefix: Mapping[str, Any] | list[dict[str, Any]],
    checkpoint: Mapping[str, Any],
    *,
    instance_id: str,
    execution_id: str,
) -> Generator[Any, Any, dict[str, Any]]:
    """Validate eagerly, snapshot, then replay through actual public native calls.

    Usage: ``handles = yield from replay_native_prefix(...)`` followed immediately
    by ``yield from run(adapter, graph, checkpoint, native_child_tasks=handles)``.
    Returns all original native child handles, not results or outcome wrappers.
    Validation errors schedule nothing. A runtime activity-result mismatch fails
    before later batches/children are created. Native task failure is untouched.
    Input objects are copied, so mutation while replay is yielded cannot alter
    authorized scheduling semantics. No cryptographic verification occurs here.
    """
    return _replay_native_prefix(context, prefix, checkpoint, instance_id=instance_id, execution_id=execution_id)


def _replay_native_prefix(
    context: Any,
    prefix: Any,
    checkpoint: Mapping[str, Any],
    *,
    instance_id: str,
    execution_id: str,
    completed_children: bool = False,
) -> Generator[Any, Any, dict[str, Any]]:
    instance_id = _text(instance_id, "instance_id")
    execution_id = _text(execution_id, "execution_id")
    if context.instance_id != instance_id or context.execution_id != execution_id:
        raise ValueError("Authenticated native prefix does not match runtime execution.")
    prefix = deepcopy(prefix)
    checkpoint = deepcopy(_object(checkpoint, "checkpoint"))
    _json_value(prefix)
    if is_list(prefix):
        prefix = _normalize_completed_agent(prefix, checkpoint)
    prefix = _object(prefix, "prefix")
    operation_batches = prefix.get("contract") == BATCH_CONTRACT
    batches: list[list[dict[str, Any]]]
    if "activity" in prefix:
        if set(prefix) - {"activity", "children", "completed_sibling"}:
            raise ValueError("Unsupported legacy prefix fields.")
        batches = [[prefix["activity"]]]
    elif operation_batches:
        if set(prefix) != {"contract", "operation_batches", "children"}:
            raise ValueError("Unsupported operation batch prefix fields.")
        batches = prefix["operation_batches"]
    else:
        if set(prefix) != {"contract", "activity_batches", "children"} or prefix.get("contract") != CONTRACT:
            raise ValueError("Unsupported mechanical native prefix contract.")
        batches = prefix["activity_batches"]
    children: list[dict[str, Any]] = prefix.get("children", [])
    if type(batches) is not list or type(children) is not list:
        raise ValueError("Prefix batches and children must be lists.")
    next_id = 0
    validated_batches: list[list[tuple[dict[str, Any], Any, Any]]] = []
    native_requests: set[str] = set()
    wait_ids: set[str] = set()
    for batch in batches:
        if type(batch) is not list or not batch:
            raise ValueError("Activity batches must be nonempty lists.")
        prepared: list[tuple[dict[str, Any], Any, Any]] = []
        for activity in batch:
            activity = _object(activity, "activity")
            if operation_batches:
                item, argument, expected = _prepare_operation(
                    activity, instance_id, native_requests, wait_ids, completed_children=completed_children
                )
                if item["kind"] != "external_wait":
                    if type(item["task_id"]) is not int or item["task_id"] != next_id:
                        raise ValueError("Native task IDs must be contiguous from zero.")
                    next_id += 1
                prepared.append((item, argument, expected))
                continue
            if set(activity) != {"task_id", "name", "python_input", "source_input", "result"}:
                raise ValueError("Unsupported activity descriptor fields.")
            if type(activity["task_id"]) is not int or activity["task_id"] != next_id:
                raise ValueError("Native task IDs must be contiguous from zero.")
            next_id += 1
            _text(activity["name"], "activity name")
            arguments = _decode(activity["source_input"], "activity source_input")
            if (
                not is_list(arguments)
                or len(arguments) != 1
                or not _equal(_decode(arguments[0], "activity argument"), activity["python_input"])
            ):
                raise ValueError("Activity replay input does not match signed source envelope.")
            expected = _decode(activity["result"], "activity result")
            prepared.append((dict(activity, kind="activity"), activity["python_input"], expected))
        validated_batches.append(prepared)
    addresses: list[str] = []
    prepared_children: list[tuple[dict[str, Any], Any]] = []
    for child in children:
        child = _object(child, "child")
        required = {"task_id", "name", "instance", "input"}
        if not required <= set(child) or set(child) - required - {"python_input"}:
            raise ValueError("Unsupported child descriptor fields.")
        if type(child["task_id"]) is not int or child["task_id"] != next_id:
            raise ValueError("Native task IDs must be contiguous from zero.")
        next_id += 1
        _text(child["name"], "child name")
        address = _text(child["instance"], "child instance")
        if address == instance_id or address in addresses:
            raise ValueError("Child addresses must be distinct and not the parent.")
        addresses.append(address)
        argument = _decode(child["input"], "child input")
        if "python_input" in child and (
            not isinstance(argument, str)
            or not _equal(_decode(argument, "original classic child argument"), child["python_input"])
        ):
            raise ValueError("Child replay input does not match original classic argument.")
        prepared_children.append((child, argument))
    checkpoint_children = checkpoint.get("children")
    if not is_list(checkpoint_children) or len(checkpoint_children) != len(children):
        raise ValueError("Checkpoint must declare every original child including completed siblings.")
    for declared, (child, _) in zip(checkpoint_children, prepared_children, strict=True):
        declared = _object(declared, "checkpoint child")
        parent = _object(declared.get("source_parent"), "checkpoint source_parent")
        if (
            declared.get("instance") != child["instance"]
            or parent.get("instance") != instance_id
            or parent.get("execution") != execution_id
            or type(parent.get("task")) is not int
            or parent["task"] != child["task_id"]
        ):
            raise ValueError("Checkpoint child order or original native parent identity changed.")

    def replay() -> Generator[Any, Any, dict[str, Any]]:
        from durabletask.entities import EntityInstanceId
        from durabletask.task import when_all

        for batch in validated_batches:
            for item, _, _ in batch:
                if item["kind"] == "entity" and context.new_uuid() != item["correlation_id"]:
                    raise ValueError("Original public correlation UUID changed.")
            tasks = [
                context.call_activity(item["name"], input=argument, return_type=JsonPayload)
                if item["kind"] == "activity"
                else context.wait_for_external_event(item["event_name"], data_type=JsonPayload)
                if item["kind"] == "external_wait"
                else context.call_sub_orchestrator(
                    item["name"], input=argument, instance_id=item["instance"], return_type=JsonPayload
                )
                if item["kind"] == "child"
                else context.call_entity(
                    EntityInstanceId.parse(item["entity"]),
                    item["operation"],
                    input=argument,
                    request_id=item["native_request_id"],
                    return_type=JsonPayload,
                )
                for item, argument, _ in batch
            ]
            results = yield when_all(tasks)
            for result, (_item, _, expected) in zip(results, batch, strict=True):
                if not _equal(result, expected):
                    label = "completed-agent" if operation_batches else "activity"
                    raise ValueError(f"Actual native {label} result does not match authenticated prefix.")
        return {  # noqa: B901
            child["instance"]: context.call_sub_orchestrator(
                child["name"], input=argument, instance_id=child["instance"], return_type=JsonPayload
            )
            for child, argument in prepared_children
        }

    return replay()


def _normalize_completed_agent(
    prefix: list[dict[str, Any]], checkpoint: Mapping[str, Any],
) -> dict[str, Any]:
    if (
        len(prefix) != 3
        or any(type(item) is not dict for item in prefix)
        or [(item.get("kind"), item.get("task_id")) for item in prefix]
        != [("activity", 0), ("entity", 1), ("activity", 2)]
        or any(type(item.get("task_id")) is not int for item in prefix)
    ):
        raise ValueError("Only the completed activity0/entity1/activity2 prefix is supported.")
    if checkpoint.get("children") != []:
        raise ValueError("Completed-agent prefix cannot overlap a native child batch.")
    batches: list[list[dict[str, Any]]] = []
    for item in prefix:
        normalized = dict(item)
        if item["kind"] == "activity":
            normalized["python_input"] = _decode(item["python_input"], "activity python_input")
        elif item["kind"] == "entity":
            original = _decode(item["python_input"], "entity python_input")
            argument = _decode(item["source_input"], "entity source_input")
            if not isinstance(argument, str) or not _equal(_decode(argument, "entity argument"), original):
                raise ValueError("Entity source argument changed.")
            normalized["python_input"] = argument
        batches.append([normalized])
    return {"contract": BATCH_CONTRACT, "operation_batches": batches, "children": []}


def _prepare_operation(
    item: dict[str, Any],
    instance_id: str,
    native_requests: set[str],
    wait_ids: set[str],
    *,
    pending: bool = False,
    completed_children: bool = False,
) -> tuple[dict[str, Any], Any, Any]:
    import uuid

    from durabletask.entities import EntityInstanceId

    keys = set(item)
    if pending and "source_result" not in item:
        keys.add("source_result")
    if item.get("kind") == "activity":
        if keys != {"kind", "task_id", "name", "python_input", "source_input", "source_result", "runtime_input_policy"}:
            raise ValueError("Unsupported completed-agent activity descriptor.")
        _text(item["name"], "activity name")
        if item["runtime_input_policy"] != "classic-host-omits-scheduled-input/1":
            raise ValueError("Unsupported historical activity input omission policy.")
        argument = item["python_input"]
        source = _decode(item["source_input"], "activity source_input")
        if (
            not is_list(source)
            or len(source) != 1
            or not _equal(_decode(source[0], "activity argument"), argument)
        ):
            raise ValueError("Activity replay input does not match signed source envelope.")
    elif item.get("kind") == "entity":
        if keys != {
            "kind",
            "task_id",
            "entity",
            "operation",
            "python_input",
            "source_input",
            "source_result",
            "native_request_id",
            "correlation_id",
        }:
            raise ValueError("Unsupported completed-agent entity descriptor.")
        entity = EntityInstanceId.parse(_text(item["entity"], "entity"))
        if str(entity) != item["entity"]:
            raise ValueError("Noncanonical entity address.")
        _text(item["operation"], "operation")
        for name in ("native_request_id", "correlation_id"):
            value = _text(item[name], name)
            if str(uuid.UUID(value)) != value:
                raise ValueError("Entity identity must be a canonical UUID.")
        if item["native_request_id"] in native_requests:
            raise ValueError("Native entity request IDs must be distinct.")
        native_requests.add(item["native_request_id"])
        argument = item["python_input"]
        if not _equal(_decode(item["source_input"], "entity source_input"), argument):
            raise ValueError("Entity source argument changed.")
        request = _decode(argument, "entity argument") if isinstance(argument, str) else argument
        if (
            not is_string_keyed_dict(request)
            or request.get("orchestrationId") != instance_id
            or request.get("correlationId") != item["correlation_id"]
        ):
            raise ValueError("Entity public correlation or orchestration identity changed.")
    elif item.get("kind") == "external_wait" and not pending:
        if set(item) != {"kind", "wait_id", "event_name", "source_result"}:
            raise ValueError("Unsupported completed external wait descriptor.")
        token = _text(item["wait_id"], "wait_id")
        _text(item["event_name"], "event_name")
        if token in wait_ids:
            raise ValueError("Captured wait task identities must be distinct, not reused across yields.")
        wait_ids.add(token)
        argument = None
    elif item.get("kind") == "child" and (pending or completed_children):
        required = {"kind", "task_id", "name", "instance", "input"}
        if not pending:
            required.add("source_result")
        if not required <= set(item) or set(item) - required - {"python_input", "source_result"}:
            raise ValueError("Unsupported original child descriptor.")
        _text(item["name"], "child name")
        address = _text(item["instance"], "child instance")
        if address == instance_id or "child:" + address in wait_ids:
            raise ValueError("Original child addresses must be distinct and not the parent.")
        wait_ids.add("child:" + address)
        argument = _decode(item["input"], "child input")
        if "python_input" in item and (
            not isinstance(argument, str)
            or not _equal(_decode(argument, "original classic child argument"), item["python_input"])
        ):
            raise ValueError("Child replay input does not match original classic argument.")
    else:
        raise ValueError("Unsupported operation kind. Only completed activity/entity/wait calls are supported.")
    expected = _decode(item["source_result"], "source_result") if "source_result" in item else _UNRESOLVED
    return item, argument, expected


def replay_native_frontier(
    context: Any,
    prefix: Mapping[str, Any],
    checkpoint: Mapping[str, Any],
    *,
    instance_id: str,
    execution_id: str,
) -> Generator[Any, Any, dict[str, Any]]:
    """Replay v3's completed prefix, return its original partial task_all handles.

    v3 keys: contract="mechanical-native-prefix/3", operation_batches (v2
    completed groups extended to include child descriptors), pending_batch
    (nonempty activity/entity/child descriptors), children=[] (no separate child
    graph in this version). Completed child rows require source_result and use
    the same original child identity/input fields as pending child rows. All
    completed and pending native calls are preflighted before scheduling.
    Pending descriptors
    Activity/entity descriptors match v2 except source_result is OPTIONAL and must be OMITTED for unresolved
    source tasks. It is never synthesized. IDs remain contiguous after completed
    operations. Original calls must already exist in acquired native history.
    Child rows are the existing original child descriptor plus kind="child" and
    optional source_result: task_id, name, instance, input (raw native argument),
    optional python_input documenting the original classic argument. Child
    address/creation is checked against acquired history, and input when present.
    Omitted classic child input remains explicit signed operator authority.

    Checkpoint requires phase="native_batch", children=[], and nativeBatch
    object with operations EXACTLY equal to pending_batch in order. Metadata,
    remaining messages and other semantic checkpoint fields are opaque here.
    No whole-checkpoint JSON validation or interpretation occurs.

    Caller authenticates the checkpoint, prefix, execution and source grouping.
    ``frontier = yield from replay_native_frontier(...)`` returns
    {"children": {}, "batch": [actual Task, ...]} without yielding the final
    batch. Caller MUST immediately yield task.when_all(frontier["batch"]), then
    validate actual ordered results before semantic processing. Native failures
    pass through the target SDK's join behavior. Completed siblings are retained
    in the final task_all, never replaced by receipts. This represents a single
    published task_all, not task_any or nested graphs.
    """
    instance_id = _text(instance_id, "instance_id")
    execution_id = _text(execution_id, "execution_id")
    if context.instance_id != instance_id or context.execution_id != execution_id:
        raise ValueError("Authenticated native frontier does not match runtime execution.")
    prefix = deepcopy(_object(prefix, "prefix"))
    _json_value(prefix)
    if (
        set(prefix) != {"contract", "operation_batches", "pending_batch", "children"}
        or prefix["contract"] != FRONTIER_CONTRACT
        or prefix["children"] != []
    ):
        raise ValueError("Unsupported partial native frontier contract.")
    pending = prefix["pending_batch"]
    if not is_list(pending) or not pending:
        raise ValueError("Partial native batch must be a nonempty ordered list.")
    checkpoint = _object(checkpoint, "checkpoint")
    native = _object(checkpoint.get("nativeBatch"), "checkpoint nativeBatch")
    if (
        checkpoint.get("phase") != "native_batch"
        or checkpoint.get("children") != []
        or not _equal(native.get("operations"), pending)
    ):
        raise ValueError("Checkpoint native frontier operations do not match signed pending batch.")
    completed: dict[str, Any] = {
        "contract": BATCH_CONTRACT, "operation_batches": prefix["operation_batches"], "children": [],
    }
    # Eagerly validates completed groups, but creates nothing until iterated.
    prior = _replay_native_prefix(
        context,
        completed,
        {"children": []},
        instance_id=instance_id,
        execution_id=execution_id,
        completed_children=True,
    )
    prior_rows = [item for batch in prefix["operation_batches"] for item in batch]
    next_id = sum(item["kind"] != "external_wait" for item in prior_rows)
    requests: set[str] = {item["native_request_id"] for item in prior_rows if item["kind"] == "entity"}
    prepared: list[tuple[dict[str, Any], Any]] = []
    child_addresses: set[str] = {"child:" + item["instance"] for item in prior_rows if item["kind"] == "child"}
    for item in pending:
        item = _object(item, "pending operation")
        item, argument, _expected = _prepare_operation(item, instance_id, requests, child_addresses, pending=True)
        if type(item["task_id"]) is not int or item["task_id"] != next_id:
            raise ValueError("Native pending task IDs must be contiguous after completed batches.")
        next_id += 1
        prepared.append((item, argument))
    if all("source_result" in item for item, _ in prepared):
        raise ValueError("Partial native batch must contain an unresolved source task without a receipt.")

    from durabletask.entities import EntityInstanceId

    # Check EVERY original before replaying any completed task. In
    # particular, absent source work must never be scheduled as new work.
    for item in [*prior_rows, *(item for item, _ in prepared)]:
        if item["kind"] == "activity":
            context.validate_replay_activity(
                item["task_id"],
                item["name"],
                item["source_input"],
                allow_input_omission=item["runtime_input_policy"] == "classic-host-omits-scheduled-input/1",
            )
        elif item["kind"] == "entity":
            context.validate_replay_entity(
                item["task_id"],
                EntityInstanceId.parse(item["entity"]),
                item["operation"],
                item["source_input"],
                item["native_request_id"],
            )
        elif item["kind"] == "child":
            context.validate_replay_child(
                item["task_id"], item["name"], item["instance"], item["input"], allow_input_omission=True
            )

    def replay() -> Generator[Any, Any, dict[str, Any]]:
        yield from prior
        for item, _ in prepared:
            if item["kind"] == "entity" and context.new_uuid() != item["correlation_id"]:
                raise ValueError("Original public correlation UUID changed.")
        batch = [
            context.call_activity(item["name"], input=argument, return_type=JsonPayload)
            if item["kind"] == "activity"
            else context.call_sub_orchestrator(
                item["name"], input=argument, instance_id=item["instance"], return_type=JsonPayload
            )
            if item["kind"] == "child"
            else context.call_entity(
                EntityInstanceId.parse(item["entity"]),
                item["operation"],
                input=argument,
                request_id=item["native_request_id"],
                return_type=JsonPayload,
            )
            for item, argument in prepared
        ]
        return {"children": {}, "batch": batch}  # noqa: B901

    return replay()


def validate_native_batch_results(operations: list[dict[str, Any]], results: list[Any]) -> None:
    """Check signed completed siblings against ACTUAL ordered native join results.

    Unresolved descriptors have no expected result. No values are substituted or
    returned. Call only AFTER the real native task_all succeeds.
    """
    if type(operations) is not list or type(results) is not list or len(operations) != len(results):
        raise ValueError("Native batch results must align with every original task.")
    for item, actual in zip(operations, results, strict=True):
        item = _object(item, "native batch operation")
        if "source_result" in item and not _equal(
            actual, _decode(item["source_result"], "completed sibling source_result")
        ):
            raise ValueError("Actual native completed sibling result changed.")


decode_prefix_value = _decode
prefix_values_equal = _equal
validate_prefix_json = _json_value
prefix_object = _object
prepare_prefix_operation = _prepare_operation
prefix_text = _text
