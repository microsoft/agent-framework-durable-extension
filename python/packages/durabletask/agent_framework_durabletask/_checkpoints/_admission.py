# Copyright (c) Microsoft. All rights reserved.

"""Deployment-owned admission facts for descendants created by current code.

These are not historical completion receipts or automatic backend ancestry.
They require trusted native immediate-parent metadata and the protected local
deployment ledger. Privileged ledger/deployment reset is outside this boundary.
"""

from __future__ import annotations

import json
from typing import Any, Protocol

from agent_framework import Workflow, WorkflowExecutor

from agent_framework_durabletask._workflows import orchestrator as current
from agent_framework_durabletask._workflows.naming import split_subworkflow_request_id, validate_executor_id
from agent_framework_durabletask._workflows.protocol import (
    WORKFLOW_ENGINE_VERSION,
    unwrap_workflow_input,
    validate_workflow_start_input,
    validate_workflow_start_provenance,
)
from agent_framework_durabletask._workflows.serialization import SUBWORKFLOW_ADDRESS_KEY, SUBWORKFLOW_INPUT_KEY

from .._checkpoint_store import CheckpointScope, CheckpointStore
from ._addresses import published_subworkflow_instance_id
from ._target import CONTRACT, fan_bindings
from ._types import is_json_object


class CheckpointContext(Protocol):
    @property
    def instance_id(self) -> str: ...

    @property
    def execution_id(self) -> str: ...

    @property
    def parent_instance_id(self) -> str | None: ...

    @property
    def parent_execution_id(self) -> str | None: ...

    @property
    def native_continuation_profile(self) -> str | None: ...

    @property
    def native_continuation(self) -> Any: ...


class AdmissionLedger:
    def __init__(
        self,
        store: CheckpointStore,
        scope: CheckpointScope,
        *,
        generation: str,
        bundle_sha256: str,
    ) -> None:
        self.store = store
        self.binding = scope
        self.generation = generation
        self.bundle_sha256 = bundle_sha256
        self.scope = {"backend": scope.backend, "hub": scope.hub, "definition": scope.definition_sha256}

    def record(self, context: CheckpointContext, graph: Workflow, state: dict[str, Any]) -> None:
        if not context.execution_id:
            raise ValueError("Admission requires the authoritative native execution identity.")
        value = {
            **self.scope,
            "instance": context.instance_id,
            "execution": context.execution_id,
            "workflow": graph.name,
            "address": state["address"],
            "eventStreaming": state["eventStreaming"],
        }
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        self.store.put(
            self.binding,
            self.generation,
            kind="admission",
            instance=context.instance_id,
            execution=context.execution_id,
            payload=encoded,
        )

    def admit_child(
        self,
        context: CheckpointContext,
        graph: Workflow,
        value: Any,
        workflows: dict[str, Workflow],
    ) -> dict[str, Any]:
        parent_id = context.parent_instance_id
        parent_execution = context.parent_execution_id
        if not parent_id or not parent_execution:
            raise ValueError("Current child requires authoritative native parent instance and execution identities.")
        parent_bytes = self.store.get(
            self.binding,
            self.generation,
            bundle_sha256=self.bundle_sha256,
            kind="admission",
            instance=parent_id,
            execution=parent_execution,
        )
        if parent_bytes is None:
            raise ValueError("Current child requires its exact native parent's committed admission.")
        parent = json.loads(parent_bytes)
        if (
            {key: parent[key] for key in self.scope} != self.scope
            or parent["instance"] != parent_id
            or parent["execution"] != parent_execution
        ):
            raise ValueError("Current child parent is outside deployment admission.")
        if (
            is_json_object(value)
            and set(value) == {"_durable_workflow_version", "input"}
            and value["_durable_workflow_version"] == WORKFLOW_ENGINE_VERSION
        ):
            unwrapped = unwrap_workflow_input(value)
        else:
            unwrapped = value
        validate_workflow_start_input(unwrapped)
        if not is_json_object(unwrapped) or set(unwrapped) != {SUBWORKFLOW_INPUT_KEY, SUBWORKFLOW_ADDRESS_KEY}:
            raise ValueError("A new child requires the native current child input contract.")
        address = unwrapped[SUBWORKFLOW_ADDRESS_KEY]
        inherited = parent["address"]
        if (
            not is_json_object(address)
            or set(address) != {"root_instance_id", "root_workflow_name", "request_path_prefix"}
            or not is_json_object(inherited)
            or set(inherited) != {"root_instance_id", "root_workflow_name", "request_path_prefix"}
            or not all(
                isinstance(address.get(key), str) and address[key]
                for key in ("root_instance_id", "root_workflow_name", "request_path_prefix")
            )
            or not all(
                isinstance(inherited.get(key), str)
                for key in ("root_instance_id", "root_workflow_name", "request_path_prefix")
            )
        ):
            raise ValueError("Current child requires a complete logical address.")
        if (
            address["root_instance_id"] != inherited["root_instance_id"]
            or address["root_workflow_name"] != inherited["root_workflow_name"]
            or not address["request_path_prefix"].startswith(inherited["request_path_prefix"])
        ):
            raise ValueError("Current child changed its authorized root address.")
        tail = address["request_path_prefix"][len(inherited["request_path_prefix"]) :]
        hop = split_subworkflow_request_id(tail)
        if hop is None or hop[1] < 0 or hop[2] or tail != f"{hop[0]}~{hop[1]}~":
            raise ValueError("Current child must be exactly one native descendant hop.")
        validate_executor_id(hop[0])
        parent_graph = workflows[parent["workflow"].casefold()]
        executor = parent_graph.executors.get(hop[0])
        if not isinstance(executor, WorkflowExecutor) or executor.workflow is not graph:
            raise ValueError("Current child does not match its trusted parent's graph edge.")
        # Decode trusted internal customer types only after the full admission.
        message = current.coerce_initial_input(graph, unwrapped)
        return self.initial_state(
            graph,
            message,
            address=address,
            is_child=True,
            event_streaming=parent["eventStreaming"],
        )

    def admit_new_root(
        self,
        context: CheckpointContext,
        graph: Workflow,
        value: Any,
    ) -> dict[str, Any]:
        """Only for a deployment-selected new-workflows-only task hub."""
        unwrapped = unwrap_workflow_input(value)
        validate_workflow_start_provenance(
            unwrapped,
            instance_id=context.instance_id,
            parent_instance_id=None,
            child_instance_id_factory=published_subworkflow_instance_id,
        )
        message = current.coerce_initial_input(graph, unwrapped)
        return self.initial_state(
            graph,
            message,
            address={
                "root_instance_id": context.instance_id,
                "root_workflow_name": graph.name,
                "request_path_prefix": "",
            },
            is_child=False,
            event_streaming=False,
        )

    @staticmethod
    def initial_state(
        graph: Workflow,
        message: Any,
        *,
        address: dict[str, str],
        is_child: bool,
        event_streaming: bool,
    ) -> dict[str, Any]:
        bindings, _ = fan_bindings(graph)
        return {
            "contract": CONTRACT,
            "routingPolicy": "published-batch-join/1",
            "phase": "prepare",
            "eventStreaming": event_streaming,
            "pending": {graph.start_executor_id: [(message, [current.SOURCE_WORKFLOW_START])]},
            "outputs": [],
            "iteration": 0,
            "child_counter": 0,
            "is_child": is_child,
            "address": address,
            "events": [],
            "shared_state": {},
            "requests": {},
            "waiting": [],
            "admittedReplyKeys": [],
            "fanBindings": bindings,
            "fan": {name: {} for name in bindings},
            "children": [],
            "notify_parent": None,
        }
