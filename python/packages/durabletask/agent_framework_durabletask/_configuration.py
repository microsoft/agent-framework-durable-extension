# Copyright (c) Microsoft. All rights reserved.

"""Shared, typed overrides for durable agent registration."""

from __future__ import annotations

from enum import Enum
from typing import Final, TypeAlias

from ._retention import StateBudget, resolve_state_budget

__all__ = [
    "INHERIT",
    "Inherit",
    "StateBudgetOverride",
    "resolve_state_budget_override",
    "validate_response_delivery_window",
]


class Inherit(Enum):
    """Use the enclosing host's setting instead of an explicit override."""

    INHERIT = "inherit"


INHERIT: Final[Inherit] = Inherit.INHERIT
"""Inherit the configured budget; unlike None, this does not disable pressure eviction."""

StateBudgetOverride: TypeAlias = StateBudget | Inherit


def resolve_state_budget_override(
    value: StateBudgetOverride,
    default: int | None,
    *,
    backend_limit: int | None = None,
) -> int | None:
    """Resolve an inherited or explicit budget without conflating None with omission."""
    return resolve_state_budget(default if isinstance(value, Inherit) else value, backend_limit=backend_limit)


def validate_response_delivery_window(response_delivery_window_seconds: int) -> None:
    """Require a positive integer delivery window, excluding booleans and non-finite floats."""
    if (
        isinstance(response_delivery_window_seconds, bool)
        or not isinstance(response_delivery_window_seconds, int)
        or response_delivery_window_seconds <= 0
    ):
        raise ValueError("response_delivery_window_seconds must be a positive integer, not a boolean or another type.")
