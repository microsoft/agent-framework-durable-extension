# Copyright (c) Microsoft. All rights reserved.

"""Explicit start-envelope versioning for the incompatible workflow execution engine."""

from typing import Any, cast

from .._shared_state_validation import _json_value  # pyright: ignore[reportPrivateUsage]
from .naming import split_subworkflow_request_id, subworkflow_instance_id, validate_executor_id
from .serialization import SUBWORKFLOW_ADDRESS_KEY, SUBWORKFLOW_INPUT_KEY

WORKFLOW_ENGINE_VERSION = 2
_VERSION_KEY = "_durable_workflow_version"


def validate_workflow_start_input(value: Any) -> None:
    """Reject malformed raw input before sanitizers or scheduling can consume it."""
    try:
        _json_value(value)
    except ValueError as exc:
        raise ValueError("Workflow input must be strict JSON with string keys and finite numbers.") from exc


def validate_workflow_start_provenance(value: Any, *, instance_id: str, parent_instance_id: Any) -> None:
    """Gate internal child decoding on host-supplied immediate-parent metadata.

    Call only at generated workflow entries, after protocol unwrapping and before
    coercion or address resolution. JSON validation is not authorization. Only
    the two markers at this context position select internal semantics, never
    identical keys nested in application data. A native parent may pass ordinary
    JSON without either marker.

    The host establishes the immediate parent, not the full ancestry or the
    root workflow name. Parent application code is trusted to construct internal
    checkpoint values and supply the root address. The checks below establish
    consistency with that claim, not authorization of arbitrary parent code.
    """
    validate_workflow_start_input(value)
    if not isinstance(value, dict):
        return
    data = cast(dict[str, Any], value)
    if SUBWORKFLOW_INPUT_KEY not in data and SUBWORKFLOW_ADDRESS_KEY not in data:
        return
    if type(parent_instance_id) is not str or not parent_instance_id.strip():
        raise ValueError("Internal workflow child input requires SDK parent instance metadata.")
    if SUBWORKFLOW_INPUT_KEY not in data or SUBWORKFLOW_ADDRESS_KEY not in data:
        raise ValueError("Internal workflow child input requires both input and address markers.")
    address = data[SUBWORKFLOW_ADDRESS_KEY]
    if not isinstance(address, dict):
        raise ValueError("Invalid internal workflow child address.")
    fields = cast(dict[str, Any], address)
    root = fields.get("root_instance_id")
    workflow_name = fields.get("root_workflow_name")
    prefix = fields.get("request_path_prefix")
    if not all(type(field) is str and field.strip() for field in (root, workflow_name, prefix)):
        raise ValueError("Invalid internal workflow child address.")

    # Reconstruct each physical hop using the dispatch naming contract. Logical
    # paths retain executor names, while physical IDs never expose their ancestry.
    claimed_parent = cast(str, root)
    remainder = cast(str, prefix)
    while remainder:
        hop = split_subworkflow_request_id(remainder)
        if hop is None:
            raise ValueError("Invalid internal workflow child address prefix.")
        executor_id, ordinal, tail = hop
        validate_executor_id(executor_id)
        if ordinal < 0 or remainder != f"{executor_id}~{ordinal}~{tail}":
            raise ValueError("Invalid internal workflow child address prefix.")
        child = subworkflow_instance_id(claimed_parent, executor_id, ordinal)
        if not tail:
            if claimed_parent != parent_instance_id or child != instance_id:
                raise ValueError("Internal workflow child address does not match SDK instance metadata.")
            return
        claimed_parent, remainder = child, tail


def wrap_workflow_input(value: Any) -> dict[str, Any]:
    """Mark a newly scheduled workflow input without changing its application payload.

    This envelope is not an authorization boundary. It distinguishes recorded starts
    from the prior engine, which must remain on their original deployment.

    Args:
        value: The application input, or trusted internal child input.

    Returns:
        The versioned scheduling envelope.
    """
    validate_workflow_start_input(value)
    return {_VERSION_KEY: WORKFLOW_ENGINE_VERSION, "input": value}


def unwrap_workflow_input(envelope: Any) -> Any:
    """Reject old starts before a hosted orchestrator executes any revised actions.

    Rewrapping recorded history does not migrate it. Deploy the old engine to finish
    old instances, and schedule only new instances with this engine's clients.

    Args:
        envelope: The durable orchestration's recorded start input.

    Returns:
        The original application payload for a supported new start.
    """
    data = cast("dict[str, Any]", envelope) if isinstance(envelope, dict) else {}
    if (
        data.keys() != {_VERSION_KEY, "input"}
        or type(data[_VERSION_KEY]) is not int
        or data[_VERSION_KEY] != WORKFLOW_ENGINE_VERSION
    ):
        raise ValueError(
            "This workflow start belongs to an unsupported execution protocol. Keep old workflow histories on their "
            "original deployment; use the v2 client/start route for new instances in an isolated-v2 deployment."
        )
    return data["input"]
