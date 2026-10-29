# Copyright (c) Microsoft. All rights reserved.

"""Migration-only compiler for one exact published workflow producer.

Operators supply the original definition, all six pinned producer source paths,
and hydrated history. This executes trusted deterministic customer predicates,
not an orchestrator generator. Do not import this module in a cold worker.
"""

from __future__ import annotations

import ast
import hashlib
import importlib.util
import inspect
import json
import sys
from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass, fields, is_dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, NoReturn

from agent_framework import AgentResponse, Workflow

from ._checkpoints._target import project_checkpoint
from ._checkpoints._types import is_list_or_tuple, is_string_keyed_dict

RELEASE = "v1.16.0-preview.260922.1"
COMMIT = "8ca913651249fc4606a2646d17dd8090f0369774"
PINS = {
    "context": "5bb388fef2bc410f39d1195ae07a5c1c67b334c5641af170a547f9a9c041a868",
    "naming": "c97a2053ee01ad343c97511f6a4e9a03e295bba325eff40a4f79f2a6177e13a2",
    "serialization": "5571ea905f92f704bbd66e6c48e306c95f3c0bd4931b905b6571fe39d2171cdb",
    "runner_context": "27aa36f7528c5143deaa26ad20460b3fb0607c379ad1177e05c616caaf9fbda0",
    "orchestrator": "93ee507ecdaf959f2c03b7f921d9a9e5e4ebc473a77de4c0d09ecb92fe90aa05",
    "activity": "3a9ff1423d393f5dcd2edf51d8bcf113e389434c8e17a9f88a686b0e40ae8378",
}
Projection = Callable[[dict[str, Any], Workflow], dict[str, Any]]
Row = dict[str, Any]


class UnsupportedContract(ValueError):
    """Concrete producer/history shape that cannot be faithfully represented."""

    def __init__(self, code: str, detail: str, evidence: Any = None) -> None:
        """Preserve the exact unsupported shape with an actionable code."""
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.evidence: Any = deepcopy(evidence)


def _path(module: ModuleType) -> Path:
    if module.__file__ is None:
        raise UnsupportedContract("source-path-required", module.__name__)
    return Path(module.__file__).resolve()


def _load(name: str, path: Path, expected: str) -> ModuleType:
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected:
        raise UnsupportedContract("source-changed-before-load", str(path))
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise UnsupportedContract("source-loader-unavailable", str(path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        # Execute the verified bytes, never an unpinned adjacent bytecode cache.
        exec(compile(raw, str(path), "exec"), vars(module))  # noqa: S102
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return module


@dataclass(frozen=True)
class TrustedModule:
    """Explicit operator assertion about an original customer module."""

    module: ModuleType
    path: Path
    sha256: str

    def verify(self) -> None:
        """Check path identity and exact bytes before executing predicates."""
        path = self.path.resolve()
        if _path(self.module) != path or hashlib.sha256(path.read_bytes()).hexdigest() != self.sha256:
            raise UnsupportedContract("trusted-module-changed", str(path))


@dataclass(frozen=True)
class TrustedDefinition:
    """Original workflow and its explicitly trusted customer module closure."""

    workflow: Workflow
    modules: tuple[TrustedModule, ...]

    def verify(self) -> None:
        """Verify source assertions, never fabricate a historical fingerprint."""
        if not self.modules:
            raise UnsupportedContract("original-definition-required", "Supply exact original customer modules.")
        for module in self.modules:
            module.verify()


def load_definition(entrypoint: str, module_files: Mapping[str, tuple[Path, str]]) -> TrustedDefinition:
    """Load an explicitly ordered flat customer dependency closure and factory.

    Original package modules can instead be supplied with TrustedDefinition.
    Dependencies must precede importers. No sys.path discovery is performed.
    """
    name, separator, factory = entrypoint.partition(":")
    if not separator or not factory.isidentifier() or name not in module_files:
        raise ValueError("Entrypoint must be an explicitly declared module:factory.")
    for module_name, (path, expected) in module_files.items():
        if not module_name.isidentifier() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise UnsupportedContract("trusted-module-changed", module_name)
    loaded: list[TrustedModule] = []
    for module_name, (path, expected) in module_files.items():
        module = sys.modules.get(module_name)
        if module is None:
            module = _load(module_name, path, expected)
        trusted = TrustedModule(module, path, expected)
        trusted.verify()
        loaded.append(trusted)
    definition = TrustedDefinition(getattr(sys.modules[name], factory)(), tuple(loaded))
    definition.verify()
    return definition


@dataclass(frozen=True)
class PublishedSource:
    """Six explicitly selected and byte-pinned original producer modules."""

    modules: Mapping[str, ModuleType]

    @classmethod
    def load(cls, files: Mapping[str, Path]) -> PublishedSource:
        """Verify all six files before executing any, without source discovery."""
        if set(files) != set(PINS):
            raise UnsupportedContract("exact-release-modules-required", repr(sorted(PINS)))
        for name, expected in PINS.items():
            if hashlib.sha256(files[name].read_bytes()).hexdigest() != expected:
                raise UnsupportedContract("published-source-changed", name)
        package_name = "_migration_capture_published"
        if package_name in sys.modules:
            raise UnsupportedContract("source-package-already-loaded", "Reuse the loaded PublishedSource instance.")
        package = ModuleType(package_name)
        vars(package)["__path__"] = []
        sys.modules[package_name] = package
        modules: dict[str, ModuleType] = {}
        try:
            for name in PINS:
                modules[name] = _load(f"{package_name}.{name}", files[name], PINS[name])
        except BaseException:
            for name in (package_name, *(f"{package_name}.{key}" for key in PINS)):
                sys.modules.pop(name, None)
            raise
        return cls(modules)

    def verify(self) -> None:
        """Recheck loaded source bytes before lowering."""
        if set(self.modules) != set(PINS):
            raise UnsupportedContract("exact-release-modules-required", repr(sorted(PINS)))
        for name, expected in PINS.items():
            if hashlib.sha256(_path(self.modules[name]).read_bytes()).hexdigest() != expected:
                raise UnsupportedContract("published-source-changed", name)


def comparable(value: Any) -> Any:
    """Project trusted source values for strict transport-aware comparison."""
    response_value: Any = getattr(value, "value", None)
    if isinstance(value, AgentResponse):
        return {"response": comparable(value.to_dict()), "value": comparable(response_value)}
    if value is None or type(value) in (str, int, bool, float):
        return value
    if is_string_keyed_dict(value):
        return {key: comparable(item) for key, item in value.items()}
    if is_list_or_tuple(value):
        return [comparable(item) for item in value]
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: comparable(getattr(value, field.name)) for field in fields(value)}
    for name in ("to_dict", "model_dump"):
        method = getattr(value, name, None)
        if callable(method):
            return comparable(method())
    raise UnsupportedContract("noncomparable-trusted-value", type(value).__qualname__)


def same(left: Any, right: Any) -> bool:
    """Compare JSON without bool/int coercion or non-finite values."""
    return json.dumps(comparable(left), sort_keys=True, allow_nan=False) == json.dumps(
        comparable(right),
        sort_keys=True,
        allow_nan=False,
    )


SNAPSHOT = """
def capture_snapshot(phase="children"):
    return {
        "phase": phase, "pending_messages": pending_messages,
        "workflow_outputs": workflow_outputs, "iteration": iteration,
        "is_subworkflow": is_subworkflow, "workflow_address": workflow_address,
        "subworkflow_counter": subworkflow_counter, "live_events": live_events,
        "pending_hitl_requests": pending_hitl_requests, "shared_state": shared_state,
        "event_streaming": ctx.supports_event_streaming,
        "fan_in_pending": {key: dict(value) for key, value in fan_in_pending.items()}
    }
"""


def compile_source(source: PublishedSource) -> tuple[Callable[..., Any], str, list[tuple[int, str]]]:
    """Lower all exact source yields to synchronous history reductions.

    Explicit lexical snapshots do not inspect frames. The resulting function is
    not a generator and must never be registered as a native orchestrator.
    """
    source.verify()
    published = source.modules["orchestrator"]
    tree = ast.parse(_path(published).read_text(encoding="utf-8"))
    function = next(
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "run_workflow_orchestrator"
    )
    sites = [
        (node.lineno, ast.unparse(node.value))
        for node in ast.walk(function)
        if isinstance(node, ast.Yield) and node.value is not None
    ]
    if sorted(sites) != [
        (1144, "ctx.task_all(all_tasks)"),
        (1169, "task"),
        (1222, "ctx.wait_for_external_event(request_id)"),
    ]:
        raise UnsupportedContract("source-yield-inventory-changed", repr(sorted(sites)))
    boundary = next(i for i, node in enumerate(function.body) if isinstance(node, ast.While))
    function.body.insert(boundary, ast.parse(SNAPSHOT).body[0])
    loop = next(node for node in function.body if isinstance(node, ast.While))
    loop.body.insert(0, ast.parse("ctx.before_prepare(capture_snapshot('prepare'))").body[0])
    sequential = next(
        node
        for node in ast.walk(function)
        if isinstance(node, ast.For)
        and ast.unparse(node.iter) == "remaining_agent_messages"
        and any(isinstance(part, ast.Yield) for part in ast.walk(node))
    )
    sequential.body.insert(
        0,
        ast.parse(
            "ctx.before_sequential(remaining_agent_messages, all_results, capture_snapshot())",
        ).body[0],
    )
    convergence = next(
        node
        for node in ast.walk(function)
        if isinstance(node, ast.If) and any(isinstance(part, ast.Raise) and part.lineno == 1265 for part in node.body)
    )
    convergence.body.insert(0, ast.parse("ctx.before_convergence(capture_snapshot('prepare'))").body[0])

    class Lower(ast.NodeTransformer):
        def visit_Return(self, node: ast.Return) -> ast.AST:
            if node.lineno in (1270, 1271):
                if node.value is None:
                    raise UnsupportedContract("source-return-changed", str(node.lineno))
                return ast.copy_location(
                    ast.Return(
                        value=ast.Call(
                            func=ast.Attribute(
                                value=ast.Name(id="ctx", ctx=ast.Load()), attr="reduce_return", ctx=ast.Load()
                            ),
                            args=[node.value, ast.parse("capture_snapshot('prepare')", mode="eval").body],
                            keywords=[],
                        )
                    ),
                    node,
                )
            return self.generic_visit(node)

        def visit_Yield(self, node: ast.Yield) -> ast.AST:
            if node.value is None:
                raise UnsupportedContract("source-yield-inventory-changed", str(node.lineno))
            replacement = {
                "ctx.task_all(all_tasks)": (
                    "ctx.reduce_batch(all_tasks, task_metadata_list, remaining_agent_messages, capture_snapshot())"
                ),
                "task": (
                    "ctx.reduce_single(task, executor_id, message, _source_executor_id, "
                    "remaining_agent_messages, all_results, capture_snapshot())"
                ),
                "ctx.wait_for_external_event(request_id)": "ctx.reduce_external(request_id, capture_snapshot('hitl'))",
            }[ast.unparse(node.value)]
            return ast.copy_location(ast.parse(replacement, mode="eval").body, node)

    Lower().visit(function)
    function.name = "reduce_published_history"
    lowered = ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[]))
    if any(isinstance(node, (ast.Yield, ast.YieldFrom)) for node in ast.walk(lowered)):
        raise UnsupportedContract("source-lowering-incomplete", "Generator operation remains.")
    namespace = dict(vars(published))
    exec(compile(lowered, "<migration-only-release-history-reducer>", "exec"), namespace)  # noqa: S102
    reducer: Callable[..., Any] = namespace[function.name]
    if inspect.isgeneratorfunction(reducer):
        raise UnsupportedContract("source-lowering-incomplete", "Reducer is a generator.")
    return reducer, ast.unparse(lowered), sorted(sites)


def source_inventory(source: PublishedSource) -> Row:
    """Enumerate every source context scheduling call and yield location."""
    source.verify()
    tree = ast.parse(_path(source.modules["orchestrator"]).read_text(encoding="utf-8"))
    schedule_names = {
        "prepare_agent_task",
        "prepare_activity_task",
        "call_sub_orchestrator",
        "task_all",
        "wait_for_external_event",
    }
    calls = sorted(
        (node.lineno, ast.unparse(node))
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "ctx"
        and node.func.attr in schedule_names
    )
    yields = sorted(
        (node.lineno, ast.unparse(node)) for node in ast.walk(tree) if isinstance(node, (ast.Yield, ast.YieldFrom))
    )
    return {"release": RELEASE, "commit": COMMIT, "pins": dict(PINS), "scheduling_calls": calls, "yield_sites": yields}


SCHEDULES = {
    "TaskScheduledEvent": "activity",
    "EntityOperationCalledEvent": "entity",
    "SubOrchestrationInstanceCreatedEvent": "child",
}
SUCCESSES = {
    "TaskCompletedEvent": ("activity", "task_scheduled_id", "result"),
    "EntityOperationCompletedEvent": ("entity", "request_id", "output"),
    "SubOrchestrationInstanceCompletedEvent": ("child", "task_scheduled_id", "result"),
}
FAILURES = {
    "TaskFailedEvent": ("activity", "task_scheduled_id"),
    "EntityOperationFailedEvent": ("entity", "request_id"),
    "SubOrchestrationInstanceFailedEvent": ("child", "task_scheduled_id"),
}
CONTROLS = {"ExecutionSuspendedEvent", "ExecutionResumedEvent", "ExecutionTerminatedEvent"}
NEUTRAL = {"ExecutionStartedEvent", "OrchestratorStartedEvent", "OrchestratorCompletedEvent"}
TRANSPORTS = {"classic-functions-v1", "native-sdk-v1"}


class History:
    """Reduce acquired native facts with the explicitly selected producer control policy."""

    def __init__(self, rows: list[Row], transport: str) -> None:
        """Snapshot native facts and preserve producer-specific control ordering."""
        self.rows = deepcopy(rows)
        starts = [row for row in self.rows if row["type"] == "ExecutionStartedEvent"]
        if len(starts) != 1:
            raise UnsupportedContract("one-original-execution-required", str(len(starts)))
        self.start = starts[0]
        self.instance: str = self.start["orchestration_instance"]["instance_id"]
        self.execution: str = self.start["orchestration_instance"]["execution_id"]
        self.schedules: list[Row] = []
        self.outcomes: dict[tuple[str, Any], Row] = {}
        self.external: dict[str, list[tuple[int, Row]]] = {}
        self.suspended = False
        self.buffered: list[tuple[int, Row]] = []
        self.terminal: Row | None = None
        self.failure_order: list[Row] = []
        for index, row in enumerate(self.rows):
            kind = row["type"]
            if kind in SCHEDULES:
                self.schedules.append(row)
            if transport == "native-sdk-v1":
                self.ingest_native_control(index, row)
            elif kind == "ExecutionSuspendedEvent":
                self.suspended = True
            elif kind == "ExecutionResumedEvent":
                self.suspended = False
                for buffered_index, buffered in self.buffered:
                    self.ingest(buffered_index, buffered)
                self.buffered.clear()
            elif kind in ("ExecutionTerminatedEvent", "ExecutionCompletedEvent"):
                self.terminal = row
            elif kind in SUCCESSES or kind in FAILURES or kind == "EventRaisedEvent":
                if self.suspended:
                    self.buffered.append((index, row))
                else:
                    self.ingest(index, row)
            elif kind not in NEUTRAL and kind not in SCHEDULES:
                raise UnsupportedContract("unrepresented-native-event", f"history[{index}] {kind}", row)
        origin = 0 if transport == "classic-functions-v1" else 1
        if [row["event_id"] for row in self.schedules] != list(range(origin, origin + len(self.schedules))):
            raise UnsupportedContract("noncontiguous-original-actions", f"{transport} action IDs start at {origin}.")
        identities = {
            (
                SCHEDULES[row["type"]],
                row["request_id"] if row["type"] == "EntityOperationCalledEvent" else row["event_id"],
            )
            for row in self.schedules
        }
        if set(self.outcomes) - identities:
            raise UnsupportedContract("orphan-native-outcome", repr(set(self.outcomes) - identities))

    def ingest_native_control(self, index: int, row: Row) -> None:
        """Preserve native SDK recursive suspension behavior, not classic no-op behavior."""
        kind = row["type"]
        if kind not in (
            set(SCHEDULES)
            | set(SUCCESSES)
            | set(FAILURES)
            | CONTROLS
            | NEUTRAL
            | {"ExecutionCompletedEvent", "EventRaisedEvent"}
        ):
            raise UnsupportedContract("unrepresented-native-event", f"history[{index}] {kind}", row)
        if kind == "ExecutionCompletedEvent":
            self.terminal = row
        elif self.suspended and kind not in ("ExecutionResumedEvent", "ExecutionTerminatedEvent"):
            self.buffered.append((index, row))
        elif kind == "ExecutionSuspendedEvent":
            self.suspended = True
        elif kind == "ExecutionResumedEvent" and self.suspended:
            self.suspended = False
            buffered, self.buffered = self.buffered, []
            for buffered_index, buffered_row in buffered:
                self.ingest_native_control(buffered_index, buffered_row)
        elif kind == "ExecutionTerminatedEvent":
            self.terminal = row
        elif kind in SUCCESSES or kind in FAILURES or kind == "EventRaisedEvent":
            self.ingest(index, row)
        elif kind not in NEUTRAL and kind not in SCHEDULES and kind != "ExecutionResumedEvent":
            raise UnsupportedContract("unrepresented-native-event", f"history[{index}] {kind}", row)

    def ingest(self, index: int, row: Row) -> None:
        """Keep event FIFO, exact failed outcomes, and first-failure arrival evidence."""
        kind = row["type"]
        if kind == "EventRaisedEvent":
            if not isinstance(row.get("input"), str):
                raise UnsupportedContract("missing-external-event-bytes", f"history[{index}]", row)
            self.external.setdefault(row["name"], []).append((index, row))
            return
        definition = SUCCESSES.get(kind) or FAILURES[kind]
        key = (definition[0], row[definition[1]])
        if key in self.outcomes:
            raise UnsupportedContract("duplicate-native-outcome", repr(key), row)
        self.outcomes[key] = row
        if kind in FAILURES or row.get("failure_details") is not None:
            self.failure_order.append({"history_index": index, "event": row})


class _Selected(BaseException):
    pass


class Capture:
    """Synchronous lexical hook adapter, with no provider or backend operations."""

    is_replaying = True

    def __init__(
        self,
        source: PublishedSource,
        definition: TrustedDefinition,
        history: History,
        project: Projection,
        event_streaming: bool,
        transport: str,
    ) -> None:
        """Bind trusted original source and a synchronous projection hook."""
        self.source = source.modules["orchestrator"]
        self.workflow, self.history, self.project = definition.workflow, history, project
        self.instance_id, self.supports_event_streaming = history.instance, event_streaming
        self.created: list[Row] = []
        self.future: list[Row] = []
        self.batches: list[list[Row]] = []
        self.consumed: list[Row] = []
        self.sequential_index = 0
        self.admitted: dict[str, Any] = {}
        self.state: Row | None = None
        self.transport = transport
        self.return_value: Any = None
        self.returning = False
        self.convergence_pending = False

    def before_prepare(self, snapshot: Row) -> None:
        """Cut only before genuinely future dispatch, not before accepted work."""
        if len(self.created) == len(self.history.schedules):
            self.state = self.project(deepcopy(snapshot), self.workflow)
            raise _Selected

    def reduce_return(self, value: Any, snapshot: Row) -> NoReturn:
        """Preserve source return while leaving terminal commitment to the backend."""
        self.state = self.project(deepcopy(snapshot), self.workflow)
        self.return_value, self.returning = deepcopy(value), True
        raise _Selected

    def before_convergence(self, snapshot: Row) -> NoReturn:
        """Let the current semantic target commit the original convergence failure."""
        self.state = self.project(deepcopy(snapshot), self.workflow)
        self.convergence_pending = True
        raise _Selected

    def before_sequential(self, remaining: list[Any], results: list[Any], snapshot: Row) -> None:
        """Cut before an unaccepted sequential tail without duplicating applied outputs."""
        if len(self.created) != len(self.history.schedules):
            return
        self.state = self.project(deepcopy(snapshot), self.workflow)
        self.state["phase"] = "sequential_prepare"
        self.state["sequentialPrepare"] = {
            "remaining": deepcopy(remaining[self.sequential_index :]),
            "staged": self._staged(results),
        }
        raise _Selected

    @staticmethod
    def _staged(results: list[Any]) -> list[Row]:
        return [
            {
                "executor": result.executor_id,
                "output_message": result.output_message,
                "activity_result": result.activity_result,
                "kind": result.task_type.value,
            }
            for result in results
        ]

    def next_scheduled(self, kind: str) -> Row | None:
        """Require exact source creation order and accepted work identity."""
        index = len(self.created)
        if index >= len(self.history.schedules):
            return None
        row = self.history.schedules[index]
        if row["type"] != kind:
            raise UnsupportedContract("original-creation-order-changed", repr(row), row)
        return row

    def append_future(self, item: Row) -> Row:
        """Describe future source work without fabricating an accepted receipt or UUID."""
        item["task_id"] = len(self.created) + (0 if self.transport == "classic-functions-v1" else 1)
        item["authorization"] = "source-future-operation/1"
        self.future.append(item)
        self.created.append(item)
        return item

    def append(self, item: Row, key: tuple[str, Any]) -> Row:
        """Retain failures as native unresolved handles, never convert to success."""
        outcome = self.history.outcomes.get(key)
        if outcome is not None and outcome["type"] in SUCCESSES and outcome.get("failure_details") is None:
            item["source_result"] = outcome[SUCCESSES[outcome["type"]][2]]
        self.created.append(item)
        return item

    def prepare_activity_task(self, name: str, input_json: str) -> Row:
        """Match the exact original Python argument through its explicit transport."""
        row = self.next_scheduled("TaskScheduledEvent")
        if row is None:
            return self.append_future({"kind": "activity", "name": name, "python_input": input_json})
        if row["name"] != name or not isinstance(row.get("input"), str):
            raise UnsupportedContract("missing-original-activity-argument", name, row)
        native: Any = json.loads(row["input"])
        if self.transport == "classic-functions-v1":
            if not is_list_or_tuple(native) or len(native) != 1:
                raise UnsupportedContract("unsupported-activity-transport", "Expected one-argument transport.", row)
            argument: Any = json.loads(native[0])
            policy = "classic-host-omits-scheduled-input/1"
        else:
            argument = native
            policy = "native-sdk-exact-input/1"
        if not isinstance(argument, str):
            raise UnsupportedContract("unsupported-activity-transport", "Expected one JSON string argument.", row)
        if not same(
            self.source.deserialize_value(json.loads(argument)),
            self.source.deserialize_value(json.loads(input_json)),
        ):
            raise UnsupportedContract("original-activity-argument-mismatch", name, row)
        return self.append(
            {
                "kind": "activity",
                "task_id": row["event_id"],
                "name": name,
                "python_input": argument,
                "source_input": row["input"],
                "runtime_input_policy": policy,
            },
            ("activity", row["event_id"]),
        )

    def prepare_agent_task(self, scoped_id: str, message: Any, orchestration_instance_id: str) -> Row:
        """Validate published blocking text entity request fields without invoking it."""
        row = self.next_scheduled("EntityOperationCalledEvent")
        if row is None:
            return self.append_future({
                "kind": "entity",
                "entity": f"@dafx-{scoped_id}@{self.instance_id}",
                "operation": "run",
                "message": message,
            })
        argument: Any = json.loads(row["input"])
        request: Row = json.loads(argument) if isinstance(argument, str) else argument
        entity = f"@dafx-{scoped_id}@{self.instance_id}"
        if (
            row["target_instance_id"] != entity
            or row["operation"] != "run"
            or not same(request["message"], message)
            or request["orchestrationId"] != orchestration_instance_id
            or orchestration_instance_id != self.instance_id
            or request["wait_for_response"] is not True
        ):
            raise UnsupportedContract("original-agent-request-mismatch", scoped_id, row)
        expected_fields = {
            "message",
            "enable_tool_calls",
            "wait_for_response",
            "role",
            "request_response_format",
            "correlationId",
            "options",
            "created_at",
            "orchestrationId",
        }
        if (
            set(request) != expected_fields
            or request["enable_tool_calls"] is not True
            or request["role"] != "user"
            or request["request_response_format"] != "text"
            or request["options"] != {}
        ):
            raise UnsupportedContract("unrepresented-agent-request-options", "Expected default blocking text.", row)
        return self.append(
            {
                "kind": "entity",
                "task_id": row["event_id"],
                "entity": entity,
                "operation": row["operation"],
                "python_input": argument,
                "source_input": row["input"],
                "native_request_id": row["request_id"],
                "correlation_id": request["correlationId"],
            },
            ("entity", row["request_id"]),
        )

    def call_sub_orchestrator(self, name: str, input: Any, instance_id: str | None = None) -> Row:
        """Validate an original child's name, identity, argument, and converter layer."""
        row = self.next_scheduled("SubOrchestrationInstanceCreatedEvent")
        if row is None:
            return self.append_future({
                "kind": "child",
                "name": name,
                "instance": instance_id,
                "python_input": deepcopy(input),
            })
        if row["name"] != name or row["instance_id"] != instance_id or not isinstance(row.get("input"), str):
            raise UnsupportedContract("original-child-binding-mismatch", name, row)
        argument: Any = json.loads(row["input"])
        if self.transport == "classic-functions-v1":
            if not isinstance(argument, str):
                raise UnsupportedContract("unsupported-child-transport", "Expected classic converter layer.", row)
            native: Any = json.loads(argument)
        else:
            native = argument
        if not same(self.source.deserialize_value(native), self.source.deserialize_value(input)):
            raise UnsupportedContract("original-child-argument-mismatch", name, row)
        item: Row = {
            "kind": "child",
            "task_id": row["event_id"],
            "name": name,
            "instance": instance_id,
            "input": row["input"],
        }
        if isinstance(argument, str):
            item["python_input"] = input
        return self.append(item, ("child", row["event_id"]))

    def set_custom_status(self, value: Any) -> None:
        """Do not publish source custom status while compiling history."""

    @staticmethod
    def result(item: Row) -> Any:
        """Decode only a completed native outcome with original public response loading."""
        value: Any = json.loads(item["source_result"])
        if item["kind"] == "entity":
            from ._response_utils import load_agent_response

            return load_agent_response(value)
        return value

    def reduce_batch(self, tasks: list[Row], metadata: list[Any], remaining: list[Any], snapshot: Row) -> list[Any]:
        """Reduce complete groups in source list order, preserving pending siblings."""
        self.sequential_index = 0
        if all("source_result" in task for task in tasks):
            self.batches.append(deepcopy(tasks))
            return [self.result(task) for task in tasks]
        return self.select_native(tasks, metadata, remaining, [], snapshot)

    def reduce_single(
        self,
        task: Row,
        executor: str,
        message: Any,
        origin: Any,
        remaining: list[Any],
        results: list[Any],
        snapshot: Row,
    ) -> Any:
        """Reduce one accepted sequential agent, or preserve its actual native task."""
        self.sequential_index += 1
        if "source_result" in task:
            self.batches.append([deepcopy(task)])
            return self.result(task)
        metadata = [self.source.TaskMetadata(executor, message, origin, self.source.TaskType.AGENT)]
        return self.select_native([task], metadata, remaining[self.sequential_index :], self._staged(results), snapshot)

    def select_native(
        self,
        tasks: list[Row],
        metadata: list[Any],
        remaining: list[Any],
        staged: list[Row],
        snapshot: Row,
    ) -> NoReturn:
        """Preserve every accepted native handle, even a successful or failed sibling."""
        self.state = self.project(deepcopy(snapshot), self.workflow)
        self.state["phase"] = "native_mixed_batch" if self.future else "native_batch"
        native_plan: Row = {
            "operations": deepcopy(tasks),
            "metadata": [
                {
                    "kind": item["kind"],
                    "executor": meta.executor_id,
                    "message": meta.message,
                    "source": meta.source_executor_id,
                }
                for item, meta in zip(tasks, metadata, strict=True)
            ],
            "remaining": deepcopy(remaining),
            "staged": deepcopy(staged),
        }
        self.state["nativeBatch"] = native_plan
        if self.future:
            native_plan["accepted_count"] = len(tasks) - len(self.future)
            if native_plan["accepted_count"] <= 0:
                raise UnsupportedContract("mixed-batch-without-accepted-prefix", "Use a prepare frontier instead.")
        raise _Selected

    def reduce_external(self, name: str, snapshot: Row) -> Any:
        """Consume FIFO events only at actual source waits, not on arrival."""
        available = self.history.external.get(name, [])
        if available:
            index, row = available.pop(0)
            self.batches.append([
                {
                    "kind": "external_wait",
                    "wait_id": f"wait/{len(self.consumed)}",
                    "event_name": name,
                    "source_result": row["input"],
                }
            ])
            self.consumed.append({"history_index": index, "event": row})
            self.admitted[name] = snapshot["pending_hitl_requests"][name]
            return json.loads(row["input"])
        admitted: list[Any] = []
        for key, request in self.admitted.items():
            if key in snapshot["pending_hitl_requests"]:
                continue
            matches = [
                message
                for rows in snapshot["pending_messages"].values()
                for message, origin in rows
                if is_string_keyed_dict(message)
                and message.get("request_id") == key
                and origin == f"{self.source.SOURCE_HITL_RESPONSE}_{key}"
            ]
            if matches:
                admitted.append(request)
        snapshot["admitted_hitl_responses"] = admitted
        self.state = self.project(deepcopy(snapshot), self.workflow)
        raise _Selected


@dataclass(frozen=True)
class Captured:
    """Semantic frontier or exact terminal disposition plus source-backed evidence."""

    boundary: str
    control: str
    state: Row | None
    prefix: Row | None
    evidence: Row
    lowered_source: str

    def checkpoint_bytes(self, *, original_input: Any) -> bytes:
        """Encode a same-execution checkpoint for protected operator signing.

        Original trusted values use the published serializer. This does not
        authenticate the history, authorize takeover or alter native controls.
        Terminal dispositions without a continuation must not be restarted.
        """
        from ._checkpoints._format import encode_checkpoint_document
        from ._workflows.serialization import serialize_value

        if self.state is None or self.prefix is None:
            raise ValueError("This capture has no same-execution continuation to authorize.")
        return encode_checkpoint_document(
            {
                "contract": "checkpoint-only-prefix/1",
                "state": serialize_value(self.state),
                "prefix": self.prefix,
                "input": original_input,
            }
        )


def capture(
    source: PublishedSource,
    definition: TrustedDefinition,
    hydrated_history: list[Row],
    *,
    event_streaming: bool,
    shared_state: Row | None,
    source_transport_profile: str,
    project: Projection = project_checkpoint,
) -> Captured:
    """Compile an original execution without dispatch, generator replay, or frame inspection.

    The SDK-first upgrade contract selects the target SDK's batch semantics.
    The source transport remains explicit because it changes the serialized
    history shape. Callers own hydration, quiescence, authentication, signing,
    and same-execution hosting. Native consumers acquire history from their
    actual SDK context, never from this capture's evidence.
    """
    definition.verify()
    if type(event_streaming) is not bool:
        raise ValueError("Original event-streaming policy must be explicit.")
    if source_transport_profile not in TRANSPORTS:
        raise ValueError("Explicit supported source_transport_profile is required.")
    reducer, generated, sites = compile_source(source)
    history = History(hydrated_history, source_transport_profile)
    evidence: Row = {
        "release": RELEASE,
        "commit": COMMIT,
        "source_yields": sites,
        "instance": history.instance,
        "execution": history.execution,
        "definition_modules": {str(item.path): item.sha256 for item in definition.modules},
        "terminal": deepcopy(history.terminal),
        "failure_order": deepcopy(history.failure_order),
        "buffered": deepcopy(history.buffered),
        "adapter_backend_calls": 0,
        "generator_replay": False,
        "frame_inspection": False,
        "source_callback_effects_not_monitored": True,
        "source_inventory": source_inventory(source),
        "source_transport_profile": source_transport_profile,
        "target_join_contract": "published-batch-join/1",
    }
    if history.terminal is not None:
        row = history.terminal
        boundary = (
            "terminated"
            if row["type"] == "ExecutionTerminatedEvent"
            else {1: "completed", 3: "failed", 5: "terminated"}.get(row["orchestration_status"])
        )
        if boundary is None:
            raise UnsupportedContract("unsupported-terminal-status", str(row["orchestration_status"]), row)
        return Captured(boundary, boundary, None, None, evidence, generated)
    context = Capture(source, definition, history, project, event_streaming, source_transport_profile)
    initial: Any = json.loads(history.start["input"])
    if source_transport_profile == "classic-functions-v1":
        if not isinstance(initial, str):
            raise UnsupportedContract("unsupported-start-transport", "Expected classic converter layer.")
        initial = json.loads(initial)
    try:
        result = reducer(context, definition.workflow, initial, deepcopy(shared_state))
    except _Selected:
        pass
    else:
        raise UnsupportedContract("no-terminal-native-record", "Reducer returned without a terminal event.", result)
    if len(context.created) - len(context.future) != len(history.schedules):
        raise UnsupportedContract("operations-beyond-selected-frontier", "History and original definition disagree.")
    state = context.state
    if state is None:
        raise UnsupportedContract("missing-selected-state", "Source hooks did not preserve their selected state.")
    prefix: Row = {
        "contract": "mechanical-native-prefix/4",
        "source_transport_profile": source_transport_profile,
        "operation_batches": context.batches,
        "children": [],
    }
    if state["phase"] == "native_batch":
        prefix.update(contract="mechanical-native-prefix/3", pending_batch=state["nativeBatch"]["operations"])
    elif state["phase"] == "native_mixed_batch":
        accepted_count = state["nativeBatch"]["accepted_count"]
        prefix.update(
            contract="mechanical-native-prefix/5",
            pending_batch=state["nativeBatch"]["operations"][:accepted_count],
            future_batch=state["nativeBatch"]["operations"][accepted_count:],
        )
        evidence["future_operations"] = deepcopy(context.future)
        evidence["backend_atomicity_verified"] = False
    evidence["consumed_external_events"] = context.consumed
    evidence["unconsumed_external_events"] = deepcopy(history.external)
    if context.returning:
        evidence["source_return_value"] = context.return_value
        evidence["native_terminal_commitment_pending"] = True
    if context.convergence_pending:
        evidence["source_failure"] = {
            "type": "WorkflowConvergenceException",
            "message": f"Workflow did not converge after {definition.workflow.max_iterations} iterations.",
        }
        evidence["native_terminal_commitment_pending"] = True
    return Captured(
        (
            "awaiting_terminal_commitment"
            if context.returning
            else "awaiting_failure_commitment"
            if context.convergence_pending
            else state["phase"]
        ),
        "suspended" if history.suspended else "running",
        state,
        prefix,
        evidence,
        generated,
    )
