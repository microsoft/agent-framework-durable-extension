# Copyright (c) Microsoft. All rights reserved.

"""Native Functions registration for the experimental checkpoint-only candidate.

Requires the proposed DFApp.worker_configuration and worker replay-profile APIs.
No generated handler closure/private registry discovery is used. HTTP/public
client routes can be registered separately on the returned DFApp using ordinary
durable client bindings. This factory does not replace AgentFunctionApp defaults.
"""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Any, Callable, cast

import azure.durable_functions as df
import azure.functions as func
from agent_framework_durabletask._checkpoints._release import register_current_handlers
from agent_framework_durabletask._checkpoints._types import is_json_object
from agent_framework_durabletask._json_payload import install_json_payload_converter
from agent_framework_durabletask.checkpoint import CheckpointStore, Registration
from durabletask import task as durabletask_task

from ._checkpoint_routes import register_checkpoint_routes


def _require_checkpoint_host_api() -> object:
    """Fail closed unless the proposed native SDK and Functions host APIs exist."""
    profiles = getattr(durabletask_task, "ReplayProfile", None)
    context_type = getattr(durabletask_task, "OrchestrationContext", None)
    if profiles is None or context_type is None:
        raise RuntimeError("Checkpoint Functions require the matching proposed Durable Task SDK.")
    if any(not hasattr(context_type, name) for name in ("execution_id", "parent_instance_id", "parent_execution_id")):
        raise RuntimeError("Checkpoint Functions require native orchestration execution identity fields.")
    from azure.durable_functions.worker import DurableFunctionsWorker

    if (
        "worker_configuration" not in inspect.signature(df.DFApp).parameters
        or not hasattr(DurableFunctionsWorker, "configure_replay_profile")
    ):
        raise RuntimeError("Checkpoint Functions require the matching proposed azure-functions-durable host.")
    default_profile = getattr(profiles, "DEFAULT", None)
    if default_profile is None:
        raise RuntimeError("The proposed Durable Task SDK does not provide its default replay profile.")
    return default_profile


def validate_checkpoint_host_configuration(path: str | Path) -> None:
    """Reject absent, misplaced or disabled provider work-item filtering.

    This validates deployment wiring, not the running provider's capabilities.
    Environment overrides and actual backend selection must also agree.
    """
    configuration = json.loads(Path(path).read_bytes())
    if not is_json_object(configuration):
        raise ValueError("Checkpoint host configuration must be a JSON object.")
    extensions = configuration.get("extensions")
    durable = extensions.get("durableTask") if is_json_object(extensions) else None
    provider = durable.get("storageProvider") if is_json_object(durable) else None
    if not is_json_object(provider) or provider.get("workItemFilteringEnabled") is not True:
        raise ValueError(
            "Checkpoint hosting requires extensions.durableTask.storageProvider.workItemFilteringEnabled=true."
        )


def create_checkpoint_function_app(
    registration: Registration,
    *,
    backend_id: str,
    hub: str,
    keys: dict[str, bytes],
    store: CheckpointStore,
    generation: str,
    bundle_sha256: str,
    bundle_directory: str,
    host_configuration_path: str | Path = "host.json",
    replay_profile: object | None = None,
    admission_profile: str = "migration-only",
    http_auth_level: func.AuthLevel = func.AuthLevel.FUNCTION,
) -> df.DFApp:
    """Register the exact current-only workflow addresses through native decorators.

    Host configuration must enable the storage provider's work-item filtering.
    Backend and hub identities come from protected deployment configuration.
    The factory does not choose AzureStorage versus DTS or grant authority.
    """
    default_profile = _require_checkpoint_host_api()
    if replay_profile is None:
        replay_profile = default_profile
    validate_checkpoint_host_configuration(host_configuration_path)

    def configure(worker: Any, kind: str) -> None:
        install_json_payload_converter(worker, deserialize_untagged_json=kind == "entity")
        worker.use_work_item_filters()
        worker.configure_replay_profile(replay_profile, preserve_activity_inputs=True)

    create_app = cast(Callable[..., df.DFApp], df.DFApp)
    app = create_app(http_auth_level=http_auth_level, worker_configuration=configure)

    class FunctionsRegistration:
        def add_activity(self, function: Any) -> None:
            app.function_name(name=function.__name__)(
                app.activity_trigger(input_name="value", activity=function.__name__)(function)
            )

        def add_entity(self, entity: Any) -> None:
            app.function_name(name=entity.__name__)(
                app.entity_trigger(context_name="context", entity_name=entity.__name__)(entity)
            )

        def add_orchestrator(self, function: Any, *, replay_profile: object | None = None) -> None:
            app.function_name(name=function.__name__)(
                app.orchestration_trigger(context_name="context", orchestration=function.__name__)(function)
            )

    workflow = register_current_handlers(
        FunctionsRegistration(),
        registration,
        backend_id=backend_id,
        hub=hub,
        keys=keys,
        store=store,
        generation=generation,
        bundle_sha256=bundle_sha256,
        bundle_directory=bundle_directory,
        replay_profile=replay_profile,
        admission_profile=admission_profile,
        supports_event_streaming=False,
    )
    register_checkpoint_routes(app, workflow, admission_profile=admission_profile)
    return app
