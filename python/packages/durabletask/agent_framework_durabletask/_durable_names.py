# Copyright (c) Microsoft. All rights reserved.

"""Durable address formatting and opt-in registration namespace checks."""

DURABLE_NAME_PREFIX = "dafx-"


def durable_name_prefix(entity_version: int = 1) -> str:
    """Return an exact supported address-generation prefix."""
    if type(entity_version) is not int or entity_version not in (1, 2):
        raise ValueError("Unsupported DAFX entity address version.")
    return DURABLE_NAME_PREFIX if entity_version == 1 else f"{DURABLE_NAME_PREFIX}v2-"


def durable_name(name: str, *, entity_version: int = 1) -> str:
    """Format a logical name without reinterpreting historical version-like names."""
    prefix = durable_name_prefix(entity_version)
    if not isinstance(name, str) or not name:
        raise ValueError("Durable logical names must be nonempty strings.")
    return f"{prefix}{name}"


def validate_two_phase_name(name: str) -> None:
    """Reject historical names occupying the opted-in generation-two namespace."""
    if durable_name(name).casefold().startswith(durable_name_prefix(2)):
        raise ValueError(
            "The two_phase registration namespace reserves historical names starting with v2-. "
            "Keep these existing identities on their isolated deployment."
        )
