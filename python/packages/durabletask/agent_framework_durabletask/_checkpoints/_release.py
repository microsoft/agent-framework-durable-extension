# Copyright (c) Microsoft. All rights reserved.

"""Current-only worker registration for an explicitly authorized semantic cut."""

from __future__ import annotations

import json
from collections.abc import Callable, Generator, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast

from agent_framework import Workflow
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from durabletask import task as durabletask_task
from durabletask.worker import TaskHubGrpcWorker

if TYPE_CHECKING:
    from durabletask.task import OrchestrationContext
else:
    OrchestrationContext = durabletask_task.OrchestrationContext

from agent_framework_durabletask._json_payload import JsonPayload, install_json_payload_converter
from agent_framework_durabletask._workflows.activity import execute_workflow_activity
from agent_framework_durabletask._workflows.dt_context import DurableTaskWorkflowContext
from agent_framework_durabletask._workflows.hitl_checkpoint import (
    execute_hitl_checkpoint,
    workflow_hitl_checkpoint_name,
)
from agent_framework_durabletask._workflows.naming import (
    workflow_executor_activity_name,
    workflow_orchestrator_name,
    workflow_scoped_executor_id,
)
from agent_framework_durabletask._workflows.protocol import unwrap_workflow_input
from agent_framework_durabletask._workflows.registration import collect_hosted_workflows, plan_workflow_registration
from agent_framework_durabletask._workflows.serialization import deserialize_value

from .._checkpoint_store import CheckpointScope, CheckpointStore
from ._admission import AdmissionLedger, CheckpointContext
from ._authority import BackendBinding, verify_checkpoint
from ._bundle import verify_checkpoint_bundle
from ._continuation import continue_checkpoint
from ._format import FORMAT, JOIN_CONTRACT, decode_checkpoint_document
from ._native_prefix import prefix_values_equal
from ._registration import Registration
from ._types import is_json_object, is_list, is_string_keyed_dict


def validate_join_contract(authorized: dict[str, Any]) -> None:
    """Reject unknown join semantics before decoding checkpoint values."""
    if authorized.get("joinContract") != JOIN_CONTRACT:
        raise ValueError("Unsupported checkpoint join contract.")
    prefix = authorized.get("prefix")
    if prefix is not None and (
        not is_json_object(prefix)
        or prefix.get("source_transport_profile") not in ("classic-functions-v1", "native-sdk-v1")
    ):
        raise ValueError("Checkpoint prefix requires an explicit supported source transport.")


def _replay_profile(name: str) -> object:
    profiles = getattr(durabletask_task, "ReplayProfile", None)
    if profiles is None:
        raise RuntimeError("Checkpoint registration requires the matching public SDK/host candidate.")
    profile = getattr(profiles, name, None)
    if profile is None:
        raise RuntimeError(f"Checkpoint SDK does not provide replay profile {name!r}.")
    return profile


def _normalize_replay_profile(replay_profile: object | None) -> object:
    return _replay_profile("DEFAULT") if replay_profile is None else replay_profile


_NATIVE_AUTO_CHECKPOINT_CONTRACT = "native-automatic-checkpoint/1"
_NATIVE_CONTINUATION_PROFILE = "native-can-v1"


def _has_native_emission_capability() -> bool:
    context_type = getattr(durabletask_task, "OrchestrationContext", None)
    return (
        getattr(durabletask_task, "NATIVE_CONTINUATION_PROFILE", None) == _NATIVE_CONTINUATION_PROFILE
        and context_type is not None
        and hasattr(context_type, "native_continuation_profile")
        and hasattr(context_type, "native_continuation")
        and callable(getattr(context_type, "create_dispatch_checkpoint", None))
    )


def _is_checkpoint_document(value: Any) -> bool:
    return is_json_object(value) and (
        value.get("format") == FORMAT
        or value.get("contract") == "checkpoint-only-start/1"
    )


def _restore_native_checkpoint(
    value: Any,
    context: CheckpointContext,
    registration: Registration,
    graph: Workflow,
) -> dict[str, Any]:
    continuation = getattr(context, "native_continuation", None)
    if (
        getattr(context, "native_continuation_profile", None) != _NATIVE_CONTINUATION_PROFILE
        or continuation is None
        or context.parent_instance_id is not None
        or not context.execution_id
        or continuation.successor_execution_id != context.execution_id
        or not continuation.predecessor_execution_id
        or len(continuation.input_sha256) != 64
        or any(character not in "0123456789abcdef" for character in continuation.input_sha256)
    ):
        raise ValueError("Native checkpoint admission requires a verified native CAN fact for this execution.")
    if not _is_checkpoint_document(value):
        raise ValueError("A native continuation fact requires its exact checkpoint input.")
    document = decode_checkpoint_document(
        json.dumps(value, allow_nan=False, separators=(",", ":")).encode("utf-8")
    )
    validate_join_contract(document)
    if document.get("contract") != "checkpoint-only-start/1":
        raise ValueError("Native continuation input is not a checkpoint-only start document.")
    state = document.get("state")
    if (
        not is_json_object(state)
        or set(state) != {"contract", "definition_sha256", "workflow_name", "semantic"}
        or state.get("contract") != _NATIVE_AUTO_CHECKPOINT_CONTRACT
        or state.get("definition_sha256") != registration.definition_digest
        or state.get("workflow_name") != graph.name
        or not is_json_object(state.get("semantic"))
    ):
        raise ValueError("Native checkpoint definition or semantic envelope does not match this worker.")
    semantic = deserialize_value(state["semantic"])
    expected_fields = {
        "contract",
        "routingPolicy",
        "phase",
        "eventStreaming",
        "pending",
        "outputs",
        "iteration",
        "child_counter",
        "is_child",
        "address",
        "events",
        "shared_state",
        "requests",
        "waiting",
        "admittedReplyKeys",
        "fanBindings",
        "fan",
        "children",
        "notify_parent",
    }
    address = semantic.get("address") if is_string_keyed_dict(semantic) else None
    if (
        not is_string_keyed_dict(semantic)
        or set(semantic) != expected_fields
        or semantic.get("phase") not in ("prepare", "hitl")
        or semantic.get("is_child") is not False
        or semantic.get("children") != []
        or not is_string_keyed_dict(address)
        or address.get("root_instance_id") != context.instance_id
        or address.get("root_workflow_name") != graph.name
        or address.get("request_path_prefix") != ""
    ):
        raise ValueError("Native checkpoint is not a supported root semantic frontier.")
    return semantic


class _RegistrationSink(Protocol):
    def add_activity(self, function: Callable[..., Any], /) -> Any: ...
    def add_entity(self, entity: Any, /) -> Any: ...
    def add_orchestrator(self, function: Callable[..., Any], /, *, replay_profile: object | None = None) -> Any: ...


class _WorkerRegistrationAdapter:
    def __init__(self, worker: TaskHubGrpcWorker) -> None:
        self._worker = worker

    def add_activity(self, function: Callable[..., Any], /) -> Any:
        return self._worker.add_activity(function)

    def add_entity(self, entity: Any, /) -> Any:
        return self._worker.add_entity(entity)

    def add_orchestrator(
        self, function: Callable[..., Any], /, *, replay_profile: object | None = None
    ) -> Any:
        register = cast(Callable[..., Any], self._worker.add_orchestrator)
        if replay_profile is None:
            return register(function)
        return register(function, replay_profile=replay_profile)


def _native_context(context: OrchestrationContext) -> CheckpointContext:
    required = ("execution_id", "parent_instance_id", "parent_execution_id")
    if any(not hasattr(context, name) for name in required):
        raise RuntimeError("Checkpoint execution requires the matching public SDK execution identity API.")
    execution_id = cast(CheckpointContext, context).execution_id
    if not isinstance(execution_id, str) or not execution_id:
        raise ValueError("Checkpoint execution requires the authoritative native execution identity.")
    for name in ("parent_instance_id", "parent_execution_id"):
        value = getattr(context, name)
        if value is not None and (not isinstance(value, str) or not value):
            raise ValueError(f"Native {name} must be a non-empty string or null.")
    return cast(CheckpointContext, context)


def register_current_handlers(
    worker: _RegistrationSink,
    registration: Registration,
    *,
    backend_id: str,
    hub: str,
    keys: Mapping[str, Ed25519PublicKey | bytes],
    store: CheckpointStore,
    generation: str,
    bundle_sha256: str,
    bundle_directory: str | Path,
    replay_profile: object | None = None,
    admission_profile: str = "migration-only",
    emission_profile: str = "disabled",
    supports_event_streaming: bool = True,
) -> Workflow:
    """Register trusted deployment code, never a caller-selected definition.

    Registration loads customer callbacks to register activities/entities. That
    code is deployment authority. Untrusted checkpoint objects are decoded only
    after signature and actual acquired execution binding verification.
    """
    if admission_profile not in ("migration-only", "new-workflows-only"):
        raise ValueError("Unsupported deployment admission profile.")
    if emission_profile not in ("disabled", _NATIVE_CONTINUATION_PROFILE):
        raise ValueError("Unsupported checkpoint emission profile.")
    if emission_profile != "disabled" and admission_profile != "new-workflows-only":
        raise ValueError("Automatic native emission is available only for new-workflows-only deployments.")
    if emission_profile != "disabled" and not _has_native_emission_capability():
        raise RuntimeError("Automatic native emission requires the matching SDK/provider proposal capability.")
    replay_profile = _normalize_replay_profile(replay_profile)
    verify_checkpoint_bundle(bundle_directory, bundle_sha256=bundle_sha256)
    scope = CheckpointScope(backend_id, hub, registration.definition_digest)
    store.require_active(scope, generation, bundle_sha256=bundle_sha256)
    workflow = registration.load_workflow(continuation_profile="published-checkpoint")
    workflows = list(collect_hosted_workflows(workflow))
    lineage = AdmissionLedger(
        store,
        scope,
        generation=generation,
        bundle_sha256=bundle_sha256,
    )
    hosted = {graph.name.casefold(): graph for graph in workflows}
    for graph in workflows:
        plan = plan_workflow_registration(graph)
        for executor in plan.activity_executors:

            def activity(ctx: Any, value: Any, executor: Any = executor, graph: Workflow = graph) -> str:
                if replay_profile == _replay_profile("CLASSIC_FUNCTIONS_V1"):
                    if not is_list(value) or len(value) != 1 or not isinstance(value[0], str):
                        raise ValueError("Classic activity requires its single JSON-string argument envelope.")
                    value = json.loads(value[0])
                    if not isinstance(value, str):
                        raise ValueError("Classic activity argument must decode to its original JSON input string.")
                return execute_workflow_activity(executor, value, graph)

            activity.__name__ = workflow_executor_activity_name(graph.name, executor.id)
            worker.add_activity(activity)
        for executor in plan.agent_executors:
            from ._agent import create_entity

            worker.add_entity(
                create_entity(
                    executor.agent,
                    entity_id=workflow_scoped_executor_id(graph.name, executor.id),
                )
            )
        if plan.agent_executors:

            def checkpoint_activity(ctx: Any, value: Any) -> Any:
                return execute_hitl_checkpoint(value)

            checkpoint_activity.__name__ = workflow_hitl_checkpoint_name(graph.name)
            worker.add_activity(checkpoint_activity)

        def orchestrator(
            context: OrchestrationContext,
            value: JsonPayload,
            graph: Workflow = graph,
        ) -> Generator[Any, Any, list[Any] | dict[str, Any]]:
            checkpoint_context = _native_context(context)
            expected = BackendBinding(
                backend_id,
                hub,
                checkpoint_context.instance_id,
                checkpoint_context.execution_id,
                registration.definition_digest,
            )
            continuation = getattr(checkpoint_context, "native_continuation", None)
            if continuation is not None:
                if emission_profile != _NATIVE_CONTINUATION_PROFILE or admission_profile != "new-workflows-only":
                    raise ValueError("Native continuation checkpoints are not enabled for this deployment.")
                state = _restore_native_checkpoint(value, checkpoint_context, registration, graph)
                lineage.record(checkpoint_context, graph, state)
                return (
                    yield from continue_checkpoint(
                        DurableTaskWorkflowContext(
                            context,
                            supports_event_streaming=supports_event_streaming,
                            acceptance_response_policy="published-v1",
                            request_time_policy="orchestration-clock",
                        ),
                        graph,
                        state,
                        automatic_emission={"definition_sha256": registration.definition_digest},
                    )
                )
            envelope = store.get(
                scope,
                generation,
                bundle_sha256=bundle_sha256,
                kind="capability",
                instance=checkpoint_context.instance_id,
                execution=checkpoint_context.execution_id,
            )
            if envelope is None:
                if checkpoint_context.parent_instance_id is None:
                    if admission_profile != "new-workflows-only":
                        raise ValueError("Migration-only roots require a bound operator capability.")
                    if _is_checkpoint_document(unwrap_workflow_input(value)):
                        raise ValueError("Copied checkpoint input is not authorized without native CAN lineage.")
                    if (
                        emission_profile == _NATIVE_CONTINUATION_PROFILE
                        and getattr(checkpoint_context, "native_continuation_profile", None)
                        != _NATIVE_CONTINUATION_PROFILE
                    ):
                        raise RuntimeError("The acquired backend does not advertise the native CAN proposal.")
                    state = lineage.admit_new_root(checkpoint_context, graph, value)
                else:
                    state = lineage.admit_child(checkpoint_context, graph, value, hosted)
                lineage.record(checkpoint_context, graph, state)
                return (
                    yield from continue_checkpoint(
                        DurableTaskWorkflowContext(
                            context,
                            supports_event_streaming=supports_event_streaming,
                            acceptance_response_policy="published-v1",
                            request_time_policy="orchestration-clock",
                        ),
                        graph,
                        state,
                        automatic_emission=(
                            {"definition_sha256": registration.definition_digest}
                            if emission_profile == _NATIVE_CONTINUATION_PROFILE
                            and checkpoint_context.parent_instance_id is None
                            else None
                        ),
                    )
                )
            if admission_profile == "new-workflows-only":
                raise ValueError("A new-workflows-only task hub cannot import migration checkpoints.")
            authenticated = verify_checkpoint(
                envelope,
                trusted_public_keys=keys,
                expected=expected,
            )
            authorized = decode_checkpoint_document(authenticated)
            validate_join_contract(authorized)
            contract = authorized.get("contract")
            if contract == "checkpoint-only-start/1":
                if not prefix_values_equal(authorized, value):
                    raise ValueError("Runtime input differs from the operator-authorized semantic cut.")
            elif contract == "checkpoint-only-prefix/1":
                if not prefix_values_equal(authorized["input"], value):
                    raise ValueError("Same-execution prefix requires its signed original input.")
                prefix = authorized["prefix"]
                if not is_json_object(prefix):
                    raise ValueError("An authenticated mechanical prefix must be an object.")
                source_profile = prefix.get("source_transport_profile")
                expected_profile = (
                    _replay_profile("DEFAULT")
                    if source_profile == "native-sdk-v1"
                    else _replay_profile("CLASSIC_FUNCTIONS_V1")
                )
                if source_profile not in (None, "classic-functions-v1", "native-sdk-v1"):
                    raise ValueError("Unsupported explicit source transport profile.")
                if replay_profile != expected_profile:
                    raise ValueError("Source transport and deployment replay profile do not match.")
            else:
                raise ValueError("Unsupported checkpoint-only start contract.")
            state = deserialize_value(authorized["state"])
            if not is_string_keyed_dict(state):
                raise ValueError("The authenticated semantic checkpoint must be an object.")
            child_tasks = None
            batch_tasks = None
            if contract == "checkpoint-only-prefix/1":
                prefix = authorized["prefix"]
                if is_string_keyed_dict(prefix) and prefix.get("source_transport_profile") is not None:
                    from ._capture_prefix import replay_capture_frontier, replay_checkpoint_prefix

                    replay = (
                        replay_checkpoint_prefix
                        if prefix.get("contract") == "mechanical-native-prefix/4"
                        else replay_capture_frontier
                    )
                    frontier = yield from replay(
                        context,
                        prefix,
                        state,
                        instance_id=context.instance_id,
                        execution_id=checkpoint_context.execution_id,
                    )
                    child_tasks, batch_tasks = frontier["children"] or None, frontier["batch"] or None
                elif is_string_keyed_dict(prefix) and prefix.get("contract") == "mechanical-native-prefix/3":
                    from ._native_prefix import replay_native_frontier

                    frontier = yield from replay_native_frontier(
                        context,
                        prefix,
                        state,
                        instance_id=context.instance_id,
                        execution_id=checkpoint_context.execution_id,
                    )
                    child_tasks, batch_tasks = frontier["children"] or None, frontier["batch"]
                else:
                    from ._native_prefix import replay_native_prefix

                    child_tasks = yield from replay_native_prefix(
                        context,
                        prefix,
                        state,
                        instance_id=context.instance_id,
                        execution_id=checkpoint_context.execution_id,
                    )
            if state.get("children") and child_tasks is None:
                raise ValueError(
                    "Active children require the authenticated original native prefix, not polled results."
                )
            lineage.record(checkpoint_context, graph, state)
            native_handles: dict[str, Any] = {}
            if child_tasks is not None:
                native_handles["native_child_tasks"] = child_tasks
            if batch_tasks is not None:
                native_handles["native_batch_tasks"] = batch_tasks
            return (  # noqa: B901
                yield from continue_checkpoint(
                    DurableTaskWorkflowContext(
                        context,
                        supports_event_streaming=supports_event_streaming,
                        acceptance_response_policy="published-v1",
                        request_time_policy="orchestration-clock",
                    ),
                    graph,
                    state,
                    **native_handles,
                )
            )

        orchestrator.__name__ = workflow_orchestrator_name(graph.name)
        if replay_profile is None:
            worker.add_orchestrator(orchestrator)
        else:
            worker.add_orchestrator(orchestrator, replay_profile=replay_profile)
    return workflow


def register_current_worker(
    worker: TaskHubGrpcWorker,
    registration: Registration,
    *,
    backend_id: str,
    hub: str,
    keys: Mapping[str, Ed25519PublicKey | bytes],
    store: CheckpointStore,
    generation: str,
    bundle_sha256: str,
    bundle_directory: str | Path,
    replay_profile: object | None = None,
    admission_profile: str = "migration-only",
    emission_profile: str = "disabled",
) -> Workflow:
    """Register the current-only native worker using public candidate SDK APIs."""
    if (
        not all(
            callable(getattr(worker, name, None))
            for name in (
                "configure_data_converter",
                "use_work_item_filters",
            )
        )
        or getattr(durabletask_task, "ReplayProfile", None) is None
        or any(
            not hasattr(durabletask_task.OrchestrationContext, name)
            for name in ("execution_id", "parent_instance_id", "parent_execution_id")
        )
    ):
        raise RuntimeError("Checkpoint registration requires the matching public SDK/host candidate.")
    if emission_profile != "disabled" and (
        emission_profile != _NATIVE_CONTINUATION_PROFILE or not _has_native_emission_capability()
    ):
        raise RuntimeError("Automatic native emission requires the matching SDK/provider proposal capability.")
    install_json_payload_converter(worker)
    workflow = register_current_handlers(
        _WorkerRegistrationAdapter(worker),
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
        emission_profile=emission_profile,
    )
    worker.use_work_item_filters()
    return workflow
