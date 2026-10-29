# Copyright (c) Microsoft. All rights reserved.

"""Exercise public checkpoint registration through actual native decorators."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from agent_framework_durabletask._workflows.protocol import wrap_workflow_input
from agent_framework_durabletask.checkpoint import (
    CheckpointScope,
    CheckpointStore,
    build_checkpoint_bundle,
    validate_registration,
)
from durabletask.internal import helpers
from durabletask.internal.orchestrator_service_pb2 import OrchestratorRequest, OrchestratorResponse

from agent_framework_azurefunctions.checkpoint import (
    _require_checkpoint_host_api,
    create_checkpoint_function_app,
    validate_checkpoint_host_configuration,
)


def _candidate_host_available() -> bool:
    try:
        _require_checkpoint_host_api()
    except RuntimeError:
        return False
    return True


def _deployment(tmp_path: Path):
    customer = tmp_path / "customer"
    customer.mkdir()
    module = customer / "checkpoint_customer.py"
    module.write_text(
        "from agent_framework import Executor, WorkflowBuilder, WorkflowContext, handler\n"
        "class Finish(Executor):\n"
        "    @handler\n"
        "    async def finish(self, value: str, ctx: WorkflowContext):\n"
        "        await ctx.yield_output(value)\n"
        "def workflow():\n"
        "    return WorkflowBuilder(name='functions-checkpoint', start_executor=Finish(id='finish')).build()\n",
        encoding="utf-8",
    )
    manifest = customer / "registration.json"
    manifest.write_text(
        json.dumps({
            "contract": "trusted-workflow-registration/1",
            "entrypoint": "checkpoint_customer:workflow",
            "sources": {
                "checkpoint_customer": {
                    "path": module.name,
                    "sha256": hashlib.sha256(module.read_bytes()).hexdigest(),
                }
            },
        }),
        encoding="utf-8",
    )
    registration = validate_registration(manifest)
    cold = tmp_path / "cold"
    digest = build_checkpoint_bundle(
        cold, customer, customer_files=[module.name, manifest.name], include_functions=True
    )
    scope = CheckpointScope("storage-test", "owned", registration.definition_digest)
    store = CheckpointStore(tmp_path / "protected" / "authority.db", create=True)
    store.prepare(scope, "release", bundle_sha256=digest)
    store.activate(scope, "release", expected_active=None, stop_attestation=b"deployment test has no old workers")
    return registration, cold, digest, store


@pytest.mark.asyncio
@pytest.mark.parametrize("versioned", [True, False])
@pytest.mark.skipif(not _candidate_host_available(), reason="Requires unpublished SDK and Functions host proposals.")
async def test_checkpoint_factory_native_dispatch_and_activity(tmp_path: Path, versioned: bool) -> None:
    registration, cold, digest, store = _deployment(tmp_path)
    host = tmp_path / "host.json"
    host.write_text(
        json.dumps({"extensions": {"durableTask": {"storageProvider": {"workItemFilteringEnabled": True}}}}),
        encoding="utf-8",
    )
    try:
        app = create_checkpoint_function_app(
            registration,
            backend_id="storage-test",
            hub="owned",
            keys={},
            store=store,
            generation="release",
            bundle_sha256=digest,
            bundle_directory=str(cold),
            host_configuration_path=host,
            admission_profile="new-workflows-only",
        )
        registered = app.get_functions()
        functions = {function.get_function_name(): function.get_user_function() for function in registered}
        routes = {
            binding["route"]
            for function in registered
            for binding in json.loads(function.get_function_json())["bindings"]
            if binding["type"] == "httpTrigger"
        }
        assert routes == {
            "workflow/functions-checkpoint/run",
            "workflow/functions-checkpoint/status/{instanceId}",
            "workflow/functions-checkpoint/respond/{instanceId}/{requestId}",
            "health",
        }
        start = helpers.new_execution_started_event("dafx-functions-checkpoint", "original")
        start.executionStarted.orchestrationInstance.executionId.value = "native-execution"
        value = wrap_workflow_input("accepted") if versioned else "accepted"
        start.executionStarted.input.value = json.dumps(value)
        request = OrchestratorRequest(
            instanceId="original",
            newEvents=[helpers.new_orchestrator_started_event(), start],
        )
        encoded = await functions["dafx-functions-checkpoint"](
            base64.b64encode(request.SerializeToString()).decode("ascii")
        )
        response = OrchestratorResponse.FromString(base64.b64decode(encoded))
        scheduled = [action.scheduleTask for action in response.actions if action.HasField("scheduleTask")]
        if not versioned:
            failures = [
                action.completeOrchestration for action in response.actions if action.HasField("completeOrchestration")
            ]
            assert len(failures) == 1
            assert "unsupported execution protocol" in failures[0].failureDetails.errorMessage
            assert not scheduled
            return
        assert len(scheduled) == 1, response
        scheduled_id = next(action.id for action in response.actions if action.HasField("scheduleTask"))
        assert scheduled[0].name == "dafx-functions-checkpoint-finish"
        assert "accepted" in scheduled[0].input.value
        value = json.loads(scheduled[0].input.value)
        activity_result = await asyncio.to_thread(functions[scheduled[0].name], value)
        assert json.loads(activity_result)["outputs"] == ["accepted"]
        request = OrchestratorRequest(
            instanceId="original",
            pastEvents=[
                helpers.new_orchestrator_started_event(),
                start,
                helpers.new_task_scheduled_event(scheduled_id, scheduled[0].name, scheduled[0].input.value),
            ],
            newEvents=[
                helpers.new_orchestrator_started_event(),
                helpers.new_task_completed_event(scheduled_id, json.dumps(activity_result)),
            ],
        )
        encoded = await functions["dafx-functions-checkpoint"](
            base64.b64encode(request.SerializeToString()).decode("ascii")
        )
        response = OrchestratorResponse.FromString(base64.b64decode(encoded))
        completions = [
            action.completeOrchestration for action in response.actions if action.HasField("completeOrchestration")
        ]
        assert len(completions) == 1
        assert completions[0].orchestrationStatus == 1
        assert json.loads(completions[0].result.value) == ["accepted"]
        assert "events" not in json.loads(response.customStatus.value)
    finally:
        sys.modules.pop("checkpoint_customer", None)


@pytest.mark.skipif(not _candidate_host_available(), reason="Requires unpublished SDK and Functions host proposals.")
def test_checkpoint_factory_preserves_accepted_activity_inputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from azure.durable_functions.worker import DurableFunctionsWorker

    registration, cold, digest, store = _deployment(tmp_path)
    host = tmp_path / "host.json"
    host.write_text(
        json.dumps({"extensions": {"durableTask": {"storageProvider": {"workItemFilteringEnabled": True}}}}),
        encoding="utf-8",
    )
    original = getattr(DurableFunctionsWorker, "configure_replay_profile", None)
    assert callable(original)
    calls: list[bool] = []

    def record(self, profile, *, preserve_activity_inputs=False):
        calls.append(preserve_activity_inputs)
        return original(self, profile, preserve_activity_inputs=preserve_activity_inputs)

    monkeypatch.setattr(DurableFunctionsWorker, "configure_replay_profile", record)
    try:
        app = create_checkpoint_function_app(
            registration,
            backend_id="storage-test",
            hub="owned",
            keys={},
            store=store,
            generation="release",
            bundle_sha256=digest,
            bundle_directory=str(cold),
            host_configuration_path=host,
            admission_profile="new-workflows-only",
        )
        app.get_functions()
        assert calls
        assert all(calls)
    finally:
        sys.modules.pop("checkpoint_customer", None)


def test_checkpoint_host_requires_unreleased_public_runtime_surfaces() -> None:
    if _candidate_host_available():
        pytest.skip("The proposed SDK and Functions host APIs are available.")
    with pytest.raises(RuntimeError, match="Checkpoint Functions require"):
        _require_checkpoint_host_api()


def test_functions_cold_bundle_omits_ordinary_legacy_host(tmp_path: Path) -> None:
    _, cold, _, _ = _deployment(tmp_path)
    script = """
import importlib, pathlib
from agent_framework_azurefunctions.checkpoint import create_checkpoint_function_app
import azure.durable_functions
assert pathlib.Path(azure.durable_functions.__file__).is_relative_to(pathlib.Path.cwd())
for name in ('agent_framework_azurefunctions._app',
             'agent_framework_durabletask._durable_agent_state',
             'agent_framework_durabletask._state_migration',
             'agent_framework_durabletask.checkpoint_migration'):
    try:
        importlib.import_module(name)
    except ModuleNotFoundError:
        continue
    raise AssertionError(name)
print('cold-native-functions')
"""
    result = subprocess.run(
        [sys.executable, "-B", "-c", script],
        cwd=cold,
        env={**os.environ, "PYTHONPATH": str(cold), "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "cold-native-functions"


@pytest.mark.parametrize(
    "durable",
    [
        {},
        {"workItemFilteringEnabled": True},
        {"storageProvider": {}},
        {"storageProvider": {"workItemFilteringEnabled": False}},
        {"storageProvider": {"workItemFilteringEnabled": "true"}},
    ],
)
def test_host_filtering_must_be_enabled_on_actual_provider(tmp_path: Path, durable: dict) -> None:
    host = tmp_path / "host.json"
    host.write_text(json.dumps({"extensions": {"durableTask": durable}}), encoding="utf-8")
    with pytest.raises(ValueError, match="storageProvider.workItemFilteringEnabled=true"):
        validate_checkpoint_host_configuration(host)
    host.write_text(
        json.dumps({"extensions": {"durableTask": {"storageProvider": {"workItemFilteringEnabled": True}}}}),
        encoding="utf-8",
    )
    validate_checkpoint_host_configuration(host)
