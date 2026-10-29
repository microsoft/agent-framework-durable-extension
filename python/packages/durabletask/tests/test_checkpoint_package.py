# Copyright (c) Microsoft. All rights reserved.

"""Packaged public registration, immutable authority and cold import boundaries."""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, cast
from unittest.mock import Mock, patch

import pytest
from agent_framework import Executor, WorkflowBuilder, WorkflowContext, WorkflowExecutor, handler
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from durabletask import task
from durabletask.internal import helpers, type_discovery
from durabletask.serialization import JsonDataConverter
from durabletask.worker import TaskHubGrpcWorker, _OrchestrationExecutor

from agent_framework_durabletask._checkpoints import _release as checkpoint_release
from agent_framework_durabletask._checkpoints._admission import AdmissionLedger
from agent_framework_durabletask._checkpoints._format import encode_checkpoint_document
from agent_framework_durabletask._checkpoints._registration import RegistrationError, validate_checkpoint
from agent_framework_durabletask._checkpoints._target import fan_bindings
from agent_framework_durabletask._checkpoints._target import run as continue_checkpoint
from agent_framework_durabletask._json_payload import JsonPayload, install_json_payload_converter
from agent_framework_durabletask._workflows.protocol import wrap_workflow_input
from agent_framework_durabletask._workflows.serialization import (
    SUBWORKFLOW_ADDRESS_KEY,
    SUBWORKFLOW_INPUT_KEY,
    serialize_value,
)
from agent_framework_durabletask.checkpoint import (
    BackendBinding,
    CheckpointScope,
    CheckpointStore,
    build_checkpoint_bundle,
    issue_checkpoint,
    public_key_bytes,
    register_current_worker,
    validate_registration,
    verify_checkpoint_bundle,
)


class Finish(Executor):
    @handler
    async def finish(self, value: str, ctx: WorkflowContext[str]) -> None:
        await cast(Any, ctx).yield_output(value)


def _customer(root: Path) -> Path:
    root.mkdir()
    module = root / "customer.py"
    module.write_text(
        "from agent_framework import Executor, WorkflowBuilder, WorkflowContext, handler\n"
        "class Finish(Executor):\n"
        "    @handler\n"
        "    async def finish(self, value: str, ctx: WorkflowContext):\n"
        "        await ctx.yield_output(value)\n"
        "def workflow():\n"
        "    return WorkflowBuilder(name='package-proof', start_executor=Finish(id='finish')).build()\n",
        encoding="utf-8",
    )
    manifest = root / "registration.json"
    manifest.write_text(
        json.dumps({
            "contract": "trusted-workflow-registration/1",
            "entrypoint": "customer:workflow",
            "sources": {"customer": {"path": "customer.py", "sha256": hashlib.sha256(module.read_bytes()).hexdigest()}},
        }),
        encoding="utf-8",
    )
    return manifest


def _state() -> dict:
    return {
        "contract": "semantic-join-checkpoint/3",
        "routingPolicy": "published-batch-join/1",
        "phase": "prepare",
        "eventStreaming": False,
        "pending": {"finish": [("preserved", ["finish"])]},
        "outputs": ["prior"],
        "iteration": 1,
        "child_counter": 0,
        "is_child": False,
        "address": {"root_instance_id": "original", "root_workflow_name": "package-proof", "request_path_prefix": ""},
        "events": [],
        "shared_state": {},
        "requests": {},
        "waiting": [],
        "admittedReplyKeys": [],
        "fanBindings": {},
        "fan": {},
        "children": [],
        "notify_parent": None,
    }


@pytest.mark.parametrize("shared", [None, {}, {"retained": 17}])
def test_registration_preserves_nullable_shared_state(tmp_path: Path, shared: dict | None) -> None:
    manifest = _customer(tmp_path / "customer")
    registration = validate_registration(manifest)
    try:
        graph = registration.load_workflow(continuation_profile="published-checkpoint")
        state = json.loads(json.dumps({**_state(), "shared_state": shared}))
        assert validate_checkpoint(graph, state)["shared_state"] == shared
        with pytest.raises(RegistrationError, match="dictionary or null"):
            validate_checkpoint(graph, {**state, "shared_state": []})
    finally:
        sys.modules.pop("customer", None)


def test_omitted_native_replay_profile_uses_sdk_default(monkeypatch: pytest.MonkeyPatch) -> None:
    default = object()
    profiles = SimpleNamespace(DEFAULT=default, CLASSIC_FUNCTIONS_V1=object())
    monkeypatch.setattr(checkpoint_release.durabletask_task, "ReplayProfile", profiles, raising=False)

    assert checkpoint_release._normalize_replay_profile(None) is default
    explicit = object()
    assert checkpoint_release._normalize_replay_profile(explicit) is explicit


@pytest.mark.parametrize(("host_supports_events", "published"), [(False, False), (True, True)])
def test_imported_event_log_publication_respects_host_capability(host_supports_events: bool, published: bool) -> None:
    class StatusContext:
        instance_id = "original"
        supports_event_streaming = host_supports_events

        def __init__(self) -> None:
            self.statuses: list[dict[str, Any]] = []

        def set_custom_status(self, status: dict[str, Any]) -> None:
            self.statuses.append(status)

        def wait_for_external_event(self, name: str) -> SimpleNamespace:
            return SimpleNamespace(is_complete=False)

    workflow = WorkflowBuilder(name="package-proof", start_executor=Finish(id="finish")).build()
    state = _state()
    event = {"type": "executor_invoked", "executor_id": "finish", "iteration": 1, "payload": "x" * 17_000}
    state.update({
        "phase": "hitl",
        "eventStreaming": True,
        "events": [event],
        "pending": {},
        "requests": {
            "decision": {
                "contract": "semantic-hitl-request/1",
                "request_id": "decision",
                "source_executor_id": "finish",
                "request_data": "approve",
                "request_type": "builtins.str",
                "response_type": "builtins.str",
            }
        },
        "waiting": ["decision"],
        "fanBindings": fan_bindings(workflow)[0],
        "fan": {},
    })
    context = StatusContext()

    next(continue_checkpoint(cast(Any, context), workflow, state))

    status = context.statuses[0]
    assert ("events" in status) is published
    assert state["eventStreaming"] is True and state["events"] == [event]
    if not host_supports_events:
        assert len(json.dumps(status, separators=(",", ":")).encode("utf-8")) < 16 * 1024


def test_unsigned_root_type_marker_uses_native_json_payload_boundary(tmp_path: Path) -> None:
    class CustomerMarker:
        decoded = 0

        def __init__(self, value: str) -> None:
            self.value = value

        @classmethod
        def from_json(cls, value: dict[str, Any]) -> CustomerMarker:
            cls.decoded += 1
            return cls(value["value"])

    class MarkerAwareConverter(JsonDataConverter):
        def deserialize(self, data: str | None, target_type: Any = None) -> Any:
            if data:
                value = json.loads(data)
                if isinstance(value, dict) and value.get("$type") == "customer-marker":
                    return CustomerMarker.from_json(value)
            return super().deserialize(data, target_type)

    class Sink:
        def __init__(self) -> None:
            self.orchestrators: list[Callable[..., Any]] = []

        def add_activity(self, function: Callable[..., Any], /) -> None:
            pass

        def add_entity(self, entity: Any, /) -> None:
            pass

        def add_orchestrator(
            self, function: Callable[..., Any], /, *, replay_profile: object | None = None
        ) -> None:
            self.orchestrators.append(function)

    manifest = _customer(tmp_path / "customer")
    try:
        registration = validate_registration(manifest)
        cold = tmp_path / "cold"
        digest = build_checkpoint_bundle(cold, manifest.parent, customer_files=["customer.py", "registration.json"])
        scope = CheckpointScope("local", "owned", registration.definition_digest)
        store = CheckpointStore(tmp_path / "protected" / "authority.db", create=True)
        store.prepare(scope, "release", bundle_sha256=digest)
        store.activate(scope, "release", expected_active=None, stop_attestation=b"local test")
        sink = Sink()
        checkpoint_release.register_current_handlers(
            sink,
            registration,
            backend_id="local",
            hub="owned",
            keys={},
            store=store,
            generation="release",
            bundle_sha256=digest,
            bundle_directory=cold,
            replay_profile=object(),
        )
        orchestrator = sink.orchestrators[0]

        marker_wire = json.dumps({"$type": "customer-marker", "value": "unsigned"})
        inner = MarkerAwareConverter()
        worker = TaskHubGrpcWorker(channel=Mock(), data_converter=inner)
        install_json_payload_converter(worker)
        worker.add_orchestrator(orchestrator)
        registered = worker._registry.get_orchestrator(orchestrator.__name__)
        assert registered is orchestrator

        # Sensitivity control: the ordinary native converter would materialize this marker.
        assert isinstance(inner.deserialize(marker_wire), CustomerMarker)
        assert CustomerMarker.decoded == 1
        CustomerMarker.decoded = 0

        input_type = type_discovery.orchestrator_input_type(registered, worker._data_converter)
        assert input_type is JsonPayload
        decoded = worker._data_converter.deserialize(marker_wire, input_type)
        assert decoded == {"$type": "customer-marker", "value": "unsigned"}
        assert not isinstance(decoded, CustomerMarker)
        assert CustomerMarker.decoded == 0

        context: Any = SimpleNamespace(
            instance_id="unsigned-root",
            execution_id="native-execution",
            parent_instance_id=None,
            parent_execution_id=None,
        )
        with pytest.raises(ValueError, match="Migration-only roots require a bound operator capability"):
            next(orchestrator(context, decoded))
        assert CustomerMarker.decoded == 0

        start = helpers.new_execution_started_event(orchestrator.__name__, "unsigned-root")
        start.executionStarted.input.value = marker_wire
        result = _OrchestrationExecutor(
            worker._registry,
            logging.getLogger("unsigned-checkpoint-root"),
            worker._data_converter,
        ).execute(
            "unsigned-root",
            [],
            [helpers.new_orchestrator_started_event(), start],
        )
        assert any(action.HasField("completeOrchestration") for action in result.actions)
        assert CustomerMarker.decoded == 0
    finally:
        sys.modules.pop("customer", None)


def test_registration_loads_nested_package_with_portable_manifest_paths(tmp_path: Path) -> None:
    package = tmp_path / "customer"
    package.mkdir()
    init = package / "__init__.py"
    init.write_text("from .workflow import workflow\n", encoding="utf-8")
    module = package / "workflow.py"
    module.write_text(
        "from agent_framework import Executor, WorkflowBuilder, WorkflowContext, handler\n"
        "class Finish(Executor):\n"
        "    @handler(input=str, output=str)\n"
        "    async def finish(self, value: str, ctx: WorkflowContext[str]):\n"
        "        await ctx.yield_output(value)\n"
        "def workflow():\n"
        "    finish = Finish(id='finish')\n"
        "    return WorkflowBuilder(name='nested-package', start_executor=finish).build()\n",
        encoding="utf-8",
    )
    manifest = tmp_path / "registration.json"
    sources = {
        "customer": {"path": "customer/__init__.py", "sha256": hashlib.sha256(init.read_bytes()).hexdigest()},
        "customer.workflow": {
            "path": "customer/workflow.py",
            "sha256": hashlib.sha256(module.read_bytes()).hexdigest(),
        },
    }
    manifest.write_text(
        json.dumps(
            {"contract": "trusted-workflow-registration/1", "entrypoint": "customer:workflow", "sources": sources}
        ),
        encoding="utf-8",
    )
    try:
        assert validate_registration(manifest).load_workflow().name == "nested-package"
        sources["customer.workflow"]["path"] = r"customer\workflow.py"
        manifest.write_text(
            json.dumps(
                {"contract": "trusted-workflow-registration/1", "entrypoint": "customer:workflow", "sources": sources}
            ),
            encoding="utf-8",
        )
        with pytest.raises(RegistrationError, match="Source path must match"):
            validate_registration(manifest)
    finally:
        sys.modules.pop("customer.workflow", None)
        sys.modules.pop("customer", None)


def test_cold_bundle_loads_declared_deferred_handler_imports(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    workflow = source / "workflow.py"
    workflow.write_text(
        "from agent_framework import Executor, WorkflowBuilder, WorkflowContext, handler\n"
        "class Finish(Executor):\n"
        "    @handler(input=str, output=str)\n"
        "    async def finish(self, value: str, ctx: WorkflowContext[str]):\n"
        "        from helper import transform\n"
        "        await ctx.yield_output(transform(value))\n"
        "def workflow():\n"
        "    return WorkflowBuilder(name='deferred-import', start_executor=Finish(id='finish')).build()\n",
        encoding="utf-8",
    )
    helper = source / "helper.py"
    helper.write_text("def transform(value):\n    return value + ':trusted-helper'\n", encoding="utf-8")
    sources = {
        "helper": {"path": "helper.py", "sha256": hashlib.sha256(helper.read_bytes()).hexdigest()},
        "workflow": {"path": "workflow.py", "sha256": hashlib.sha256(workflow.read_bytes()).hexdigest()},
    }
    (source / "registration.json").write_text(
        json.dumps(
            {
                "contract": "trusted-workflow-registration/1",
                "entrypoint": "workflow:workflow",
                "sources": sources,
            }
        ),
        encoding="utf-8",
    )
    cold = tmp_path / "cold"
    digest = build_checkpoint_bundle(
        cold,
        source,
        customer_files=["workflow.py", "helper.py", "registration.json"],
    )
    verify_checkpoint_bundle(cold, bundle_sha256=digest)
    env = {**os.environ, "PYTHONPATH": str(cold), "PYTHONDONTWRITEBYTECODE": "1"}
    script = """
import asyncio
import sys
from pathlib import Path
from agent_framework_durabletask._checkpoints._registration import validate_registration

assert 'helper' not in sys.modules

class Context:
    def __init__(self):
        self.outputs = []
    async def yield_output(self, value):
        self.outputs.append(value)

async def main():
    registration = validate_registration(Path('customer/registration.json'))
    assert 'helper' not in sys.modules
    workflow = registration.load_workflow(continuation_profile='published-checkpoint')
    assert 'helper' in sys.modules
    context = Context()
    await workflow.executors['finish'].finish('cold', context)
    assert context.outputs == ['cold:trusted-helper'], context.outputs
    print('cold-deferred-import')

asyncio.run(main())
"""
    result = subprocess.run(
        [sys.executable, "-B", "-c", script], cwd=cold, env=env, capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "cold-deferred-import"


@pytest.mark.parametrize("wrapped", [False, True], ids=["published-legacy-child", "current-child"])
def test_child_admission_uses_native_parent_and_logical_path_not_physical_ordinal(
    tmp_path: Path,
    wrapped: bool,
) -> None:
    leaf_executor = Finish(id="leaf-finish")
    leaf = WorkflowBuilder(name="admission-leaf", start_executor=leaf_executor).build()
    grandchild = WorkflowExecutor(leaf, id="grand")
    parent = WorkflowBuilder(name="admission-middle", start_executor=grandchild).build()
    scope = CheckpointScope("local", "owned", "a" * 64)
    store = CheckpointStore(tmp_path / "admission.db", create=True)
    digest = "b" * 64
    store.prepare(scope, "release", bundle_sha256=digest)
    store.activate(scope, "release", expected_active=None, stop_attestation=b"authorized local test")
    ledger = AdmissionLedger(store, scope, generation="release", bundle_sha256=digest)
    parent_context = SimpleNamespace(
        instance_id="root::second::1",
        execution_id="parent-execution",
        parent_instance_id="root",
        parent_execution_id="root-execution",
    )
    parent_state = {
        "address": {
            "root_instance_id": "root",
            "root_workflow_name": "admission-root",
            "request_path_prefix": "second~0~",
        },
        "eventStreaming": False,
    }
    ledger.record(parent_context, parent, parent_state)
    raw_child_value = {
        SUBWORKFLOW_INPUT_KEY: serialize_value("typed child input"),
        SUBWORKFLOW_ADDRESS_KEY: {
        "root_instance_id": "root",
        "root_workflow_name": "admission-root",
        "request_path_prefix": "second~0~grand~0~",
        },
    }
    child_value = wrap_workflow_input(raw_child_value) if wrapped else raw_child_value
    child_context = SimpleNamespace(
        instance_id="root::second::1::grand::0",
        execution_id="child-execution",
        parent_instance_id=parent_context.instance_id,
        parent_execution_id=parent_context.execution_id,
    )

    admitted = ledger.admit_child(
        child_context,
        leaf,
        child_value,
        {parent.name.casefold(): parent, leaf.name.casefold(): leaf},
    )
    assert admitted["is_child"] is True
    assert admitted["address"]["request_path_prefix"] == "second~0~grand~0~"
    assert admitted["pending"][leaf.start_executor_id][0][0] == "typed child input"

    root_context = SimpleNamespace(
        instance_id="copied-root",
        execution_id="copied-execution",
        parent_instance_id=None,
        parent_execution_id=None,
    )
    with pytest.raises(ValueError, match="requires SDK parent instance metadata"):
        ledger.admit_new_root(root_context, leaf, wrap_workflow_input(raw_child_value))


def test_cold_package_rejects_legacy_imports_and_checks_exact_bytes(tmp_path: Path) -> None:
    manifest = _customer(tmp_path / "customer")
    cold = tmp_path / "cold"
    digest = build_checkpoint_bundle(cold, manifest.parent, customer_files=["customer.py", "registration.json"])
    verify_checkpoint_bundle(cold, bundle_sha256=digest)
    env = {**os.environ, "PYTHONPATH": str(cold), "PYTHONDONTWRITEBYTECODE": "1"}
    script = """
import importlib, pathlib
from agent_framework_durabletask.checkpoint import CheckpointStore, validate_registration
import agent_framework_durabletask
assert pathlib.Path(agent_framework_durabletask.__file__).is_relative_to(pathlib.Path.cwd())
for name in ('agent_framework_durabletask._durable_agent_state',
             'agent_framework_durabletask._state_migration',
             'agent_framework_durabletask.checkpoint_migration',
             'agent_framework_durabletask.checkpoint_entity_migration', 'checkpoint_agent_import', 'published'):
    try:
        importlib.import_module(name)
    except ModuleNotFoundError:
        continue
    raise AssertionError(name)
print('cold-current-only')
"""
    result = subprocess.run(
        [sys.executable, "-B", "-c", script], cwd=cold, env=env, capture_output=True, text=True, timeout=30
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "cold-current-only"
    (cold / "extra.py").write_text("raise RuntimeError('not pinned')", encoding="utf-8")
    with pytest.raises(ValueError, match="missing, extra or changed"):
        verify_checkpoint_bundle(cold, bundle_sha256=digest)


def _execute_with_proposed_execution_identity(
    executor: _OrchestrationExecutor,
    instance_id: str,
    execution_id: str,
    events: list[Any],
) -> Any:
    execute = cast(Callable[..., Any], executor.execute)
    return execute(instance_id, [], events, execution_id=execution_id)


@pytest.mark.parametrize("shared_state", [None, {}, {"retained": 17}])
@pytest.mark.parametrize("format_version", [1, 2])
def test_public_registration_uses_committed_exact_execution_authority(
    tmp_path: Path,
    shared_state: dict | None,
    format_version: int,
) -> None:
    if "execution_id" not in inspect.signature(_OrchestrationExecutor.execute).parameters:
        pytest.skip("Requires the proposed SDK execution-identity API.")
    manifest = _customer(tmp_path / "customer")
    registration = validate_registration(manifest)
    cold = tmp_path / "cold"
    digest = build_checkpoint_bundle(cold, manifest.parent, customer_files=["customer.py", "registration.json"])
    scope = CheckpointScope("local", "owned", registration.definition_digest)
    store = CheckpointStore(tmp_path / "protected" / "authority.db", create=True)
    store.prepare(scope, "release", bundle_sha256=digest)
    private = Ed25519PrivateKey.generate()
    state = {**_state(), "shared_state": shared_state}
    value = encode_checkpoint_document({"contract": "checkpoint-only-start/1", "state": serialize_value(state)})
    if format_version != 1:
        changed = json.loads(value)
        changed["formatVersion"] = format_version
        value = json.dumps(changed).encode()
    envelope = issue_checkpoint(
        key_id="operator",
        operator_private_key=private,
        binding=BackendBinding("local", "owned", "original", "native-execution", registration.definition_digest),
        checkpoint_bytes=value,
    )
    store.put(scope, "release", kind="capability", instance="original", execution="native-execution", payload=envelope)
    store.activate(scope, "release", expected_active=None, stop_attestation=b"all old workers stopped")
    worker = TaskHubGrpcWorker(emit_trace_spans=False)
    try:
        register_current_worker(
            worker,
            registration,
            backend_id="local",
            hub="owned",
            keys={"operator": public_key_bytes(private.public_key())},
            store=store,
            generation="release",
            bundle_sha256=digest,
            bundle_directory=cold,
        )
        executions = (
            (("native-execution", True), ("another-execution", False))
            if format_version == 1
            else (("native-execution", False),)
        )
        for execution, expected_schedule in executions:
            start = helpers.new_execution_started_event("dafx-package-proof", "original")
            start.executionStarted.orchestrationInstance.executionId.value = execution
            start.executionStarted.input.value = value.decode()
            executor = _OrchestrationExecutor(
                worker._registry, logging.getLogger("checkpoint-package"), worker._data_converter
            )
            if format_version != 1:
                with patch(
                    "agent_framework_durabletask._checkpoints._release.deserialize_value",
                    side_effect=AssertionError("Unknown format must reject before customer decoding"),
                ) as decode:
                    result = _execute_with_proposed_execution_identity(
                        executor,
                        "original",
                        execution,
                        [helpers.new_orchestrator_started_event(), start],
                    )
                    decode.assert_not_called()
                terminal = next(a.completeOrchestration for a in result.actions if a.HasField("completeOrchestration"))
                assert "Unsupported checkpoint format version" in terminal.failureDetails.errorMessage
            else:
                result = _execute_with_proposed_execution_identity(
                    executor,
                    "original",
                    execution,
                    [helpers.new_orchestrator_started_event(), start],
                )
            scheduled = [action.scheduleTask for action in result.actions if action.HasField("scheduleTask")]
            assert bool(scheduled) is expected_schedule, result.actions
            if expected_schedule:
                assert scheduled[0].name == "dafx-package-proof-finish"
                assert "preserved" in scheduled[0].input.value
                assert json.loads(json.loads(scheduled[0].input.value))["shared_state_snapshot"] == shared_state
        admission = store.get(
            scope,
            "release",
            bundle_sha256=digest,
            kind="admission",
            instance="original",
            execution="native-execution",
        )
        if format_version == 1:
            assert admission is not None
            assert json.loads(admission)["address"]["root_instance_id"] == "original"
        else:
            assert admission is None
    finally:
        sys.modules.pop("customer", None)


@pytest.mark.parametrize("contract", ["checkpoint-only-start/1", "checkpoint-only-prefix/1"])
def test_runtime_checkpoint_input_binding_rejects_bool_integer_equivalence(
    tmp_path: Path,
    contract: str,
) -> None:
    if (
        "execution_id" not in inspect.signature(_OrchestrationExecutor.execute).parameters
        or not hasattr(task, "ReplayProfile")
    ):
        pytest.skip("Requires the proposed SDK execution-identity and replay-profile APIs.")

    manifest = _customer(tmp_path / "customer")
    registration = validate_registration(manifest)
    cold = tmp_path / "cold"
    digest = build_checkpoint_bundle(cold, manifest.parent, customer_files=["customer.py", "registration.json"])
    scope = CheckpointScope("local", "owned", registration.definition_digest)
    store = CheckpointStore(tmp_path / "protected" / "authority.db", create=True)
    store.prepare(scope, "release", bundle_sha256=digest)
    private = Ed25519PrivateKey.generate()

    if contract == "checkpoint-only-start/1":
        payload = {"contract": contract, "state": serialize_value(_state())}
        checkpoint_bytes = encode_checkpoint_document(payload)
        runtime_document = json.loads(checkpoint_bytes)
        runtime_document["formatVersion"] = True
        runtime_input = json.dumps(runtime_document)
        expected_error = "Runtime input differs from the operator-authorized semantic cut."
    else:
        prefix = {
            "contract": "mechanical-native-prefix/4",
            "source_transport_profile": "native-sdk-v1",
            "operation_batches": [],
            "children": [],
        }
        payload = {
            "contract": contract,
            "input": True,
            "state": serialize_value(_state()),
            "prefix": prefix,
        }
        checkpoint_bytes = encode_checkpoint_document(payload)
        runtime_input = "1"
        expected_error = "Same-execution prefix requires its signed original input."

    envelope = issue_checkpoint(
        key_id="operator",
        operator_private_key=private,
        binding=BackendBinding(
            "local", "owned", "original", "native-execution", registration.definition_digest
        ),
        checkpoint_bytes=checkpoint_bytes,
    )
    store.put(
        scope,
        "release",
        kind="capability",
        instance="original",
        execution="native-execution",
        payload=envelope,
    )
    store.activate(scope, "release", expected_active=None, stop_attestation=b"all old workers stopped")
    native = TaskHubGrpcWorker(emit_trace_spans=False)
    try:
        register_current_worker(
            native,
            registration,
            backend_id="local",
            hub="owned",
            keys={"operator": public_key_bytes(private.public_key())},
            store=store,
            generation="release",
            bundle_sha256=digest,
            bundle_directory=cold,
        )
        start = helpers.new_execution_started_event("dafx-package-proof", "original")
        start.executionStarted.orchestrationInstance.executionId.value = "native-execution"
        start.executionStarted.input.value = runtime_input
        executor = _OrchestrationExecutor(
            native._registry, logging.getLogger("checkpoint-input-binding"), native._data_converter
        )
        result = _execute_with_proposed_execution_identity(
            executor,
            "original",
            "native-execution",
            [helpers.new_orchestrator_started_event(), start],
        )
        terminals = [
            action.completeOrchestration
            for action in result.actions
            if action.HasField("completeOrchestration")
        ]
        assert len(terminals) == 1, result.actions
        terminal = terminals[0]
        assert expected_error in terminal.failureDetails.errorMessage
        assert not any(action.HasField("scheduleTask") for action in result.actions)
    finally:
        sys.modules.pop("customer", None)
