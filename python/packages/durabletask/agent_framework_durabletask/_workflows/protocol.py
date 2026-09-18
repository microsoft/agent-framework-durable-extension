# Copyright (c) Microsoft. All rights reserved.

"""Explicit start-envelope versioning for the incompatible workflow execution engine."""

from typing import Any, cast

from .._shared_state_validation import _json_value  # pyright: ignore[reportPrivateUsage]

WORKFLOW_ENGINE_VERSION = 2
_VERSION_KEY = "_durable_workflow_version"


def validate_workflow_start_input(value: Any) -> None:
    """Reject malformed raw input before sanitizers or scheduling can consume it."""
    try:
        _json_value(value)
    except ValueError as exc:
        raise ValueError("Workflow input must be strict JSON with string keys and finite numbers.") from exc


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
