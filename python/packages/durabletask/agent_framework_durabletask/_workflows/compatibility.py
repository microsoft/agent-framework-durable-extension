# Copyright (c) Microsoft. All rights reserved.

"""Dispatch supported recorded execution contracts without rewriting their histories."""

from __future__ import annotations

import json
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Literal, cast

from agent_framework import Executor, Workflow

from ._legacy_activity import execute_workflow_activity as execute_legacy_activity
from .activity import execute_workflow_activity
from .protocol import unwrap_workflow_input, validate_workflow_start_input, validate_workflow_start_provenance
from .serialization import SUBWORKFLOW_ADDRESS_KEY, SUBWORKFLOW_INPUT_KEY, validate_workflow_json


@dataclass(frozen=True)
class WorkflowStart:
    """Backend-established identity plus detached recorded input for a trusted resolver."""

    instance_id: str
    parent_instance_id: str | None
    input: Any


WorkflowExecutionResolver = Callable[[WorkflowStart], Literal["legacy", "v2"]]


def _resolve_profile(
    value: Any,
    *,
    instance_id: str,
    parent_instance_id: str | None,
    resolver: WorkflowExecutionResolver,
) -> Literal["legacy", "v2"]:
    profile = resolver(WorkflowStart(instance_id, parent_instance_id, deepcopy(value)))
    if profile not in ("legacy", "v2"):
        raise ValueError("workflow_execution_resolver must select 'legacy' or 'v2'.")
    return profile


def uses_legacy_workflow_serialization(
    recorded_input: Any,
    custom_status: Any,
    *,
    instance_id: str | None = None,
    parent_instance_id: str | None = None,
    resolver: WorkflowExecutionResolver | None = None,
) -> bool:
    """Choose a result codec from recorded execution provenance, never output shape."""
    if isinstance(custom_status, dict):
        protocol = cast(dict[str, Any], custom_status).get("executionProtocol")
        if type(protocol) is int and protocol == 1:
            return True
    if resolver is not None:
        validate_workflow_start_input(recorded_input)
        if not isinstance(instance_id, str) or not instance_id.strip():
            raise ValueError("Trusted workflow codec selection requires a backend instance ID.")
        return (
            _resolve_profile(
                recorded_input,
                instance_id=instance_id,
                parent_instance_id=parent_instance_id,
                resolver=resolver,
            )
            == "legacy"
        )
    if type(recorded_input) not in (dict, list, str, int, float, bool, type(None)):
        return False
    return not (isinstance(recorded_input, dict) and "_durable_workflow_version" in recorded_input)


def recorded_legacy_child_ordinal(
    recorded_input: Any,
    *,
    executor_id: str,
    parent_instance_id: str,
    child_instance_id: str,
) -> int | None:
    """Recover the recorded public ordinal, never re-enumerate a shrinking child list."""
    if not child_instance_id.startswith(f"{parent_instance_id}::{executor_id}::"):
        return None
    if isinstance(recorded_input, str):
        try:
            recorded_input = json.loads(recorded_input)
        except json.JSONDecodeError:
            return None
    if not isinstance(recorded_input, dict):
        return None
    address: Any = cast(dict[str, Any], recorded_input).get(SUBWORKFLOW_ADDRESS_KEY)
    if not isinstance(address, dict):
        return None
    prefix: Any = cast(dict[str, Any], address).get("request_path_prefix")
    if not isinstance(prefix, str) or not prefix.endswith("~"):
        return None
    parts = prefix[:-1].split("~")
    if len(parts) < 2 or len(parts) % 2 or parts[-2] != executor_id:
        return None
    try:
        ordinal = int(parts[-1])
    except ValueError:
        return None
    return ordinal if ordinal >= 0 and str(ordinal) == parts[-1] else None


def execute_compatible_activity(executor: Executor, input_json: str, workflow: Workflow | None = None) -> str:
    """Choose the frozen predecessor envelope; explicit v2 control always wins."""
    data = json.loads(input_json)
    validate_workflow_json(data)
    legacy_keys = {"executor_id", "message", "shared_state_snapshot", "source_executor_ids", "host_context"}
    if isinstance(data, dict) and "is_hitl_response" not in data and legacy_keys.issubset(cast(dict[str, Any], data)):
        return execute_legacy_activity(executor, input_json, workflow)
    return execute_workflow_activity(executor, input_json, workflow)


def select_workflow_execution(
    value: Any,
    *,
    instance_id: str,
    parent_instance_id: str | None,
    deployment_mode: str,
    resolver: WorkflowExecutionResolver | None = None,
) -> tuple[Literal["legacy", "v2"], Any]:
    """Use the explicit v2 start contract or the published unversioned predecessor."""
    recorded: Any = value
    validate_workflow_start_input(value)
    if resolver is not None:
        profile = _resolve_profile(
            value,
            instance_id=instance_id,
            parent_instance_id=parent_instance_id,
            resolver=resolver,
        )
    else:
        profile = "v2" if isinstance(value, dict) and "_durable_workflow_version" in value else "legacy"
    if profile == "v2":
        if deployment_mode == "legacy":
            raise ValueError("Legacy deployment mode cannot execute a v2 workflow contract.")
        decoded = unwrap_workflow_input(value)
        validate_workflow_start_provenance(
            decoded,
            instance_id=instance_id,
            parent_instance_id=parent_instance_id,
        )
        return "v2", decoded
    if isinstance(value, dict):
        value = cast(dict[str, Any], value)
    if isinstance(value, dict) and (SUBWORKFLOW_INPUT_KEY in value or SUBWORKFLOW_ADDRESS_KEY in value):
        if not parent_instance_id or not instance_id.startswith(parent_instance_id + "::"):
            raise ValueError("Legacy child input requires matching SDK immediate-parent metadata.")
        address: Any = cast(dict[str, Any], value).get(SUBWORKFLOW_ADDRESS_KEY)
        if (
            SUBWORKFLOW_INPUT_KEY not in value
            or not isinstance(address, dict)
            or not all(
                isinstance(cast(dict[str, Any], address).get(key), str)
                and bool(cast(dict[str, Any], address)[key].strip())
                for key in ("root_instance_id", "root_workflow_name", "request_path_prefix")
            )
        ):
            raise ValueError("Legacy child input requires a complete recorded child address.")
    return "legacy", recorded
