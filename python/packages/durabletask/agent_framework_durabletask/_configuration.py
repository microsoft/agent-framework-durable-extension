# Copyright (c) Microsoft. All rights reserved.

"""Shared registration validation and prototype state-writer policy."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Final, Literal, TypeAlias

from agent_framework import SupportsAgentRun

from ._callbacks import AgentResponseCallbackProtocol
from ._history_provider import ensure_durable_history
from ._retention import (
    DEFAULT_RETENTION,
    HIGH_WATERMARK,
    LOW_WATERMARK,
    RetentionMode,
    StateBudget,
    resolve_state_budget,
    validate_retention,
)

__all__ = [
    "INHERIT",
    "AgentRegistrationSettings",
    "Inherit",
    "RegistrationIdentity",
    "StateBudgetOverride",
    "resolve_runtime_deployment",
    "resolve_state_budget_override",
    "validate_agent_configuration",
    "validate_response_delivery_window",
    "validate_runtime_deployment",
]


class Inherit(Enum):
    """Use the enclosing host's setting instead of an explicit override."""

    INHERIT = "inherit"


INHERIT: Final[Inherit] = Inherit.INHERIT
"""Inherit the configured budget, unlike None which disables pressure eviction."""

StateBudgetOverride: TypeAlias = StateBudget | Inherit


def resolve_state_budget_override(
    value: StateBudgetOverride,
    default: int | None,
    *,
    backend_limit: int | None = None,
) -> int | None:
    """Resolve an inherited or explicit budget without conflating None with omission."""
    return resolve_state_budget(default if isinstance(value, Inherit) else value, backend_limit=backend_limit)


def resolve_runtime_deployment(deployment_mode: str | None = None) -> Literal["legacy", "isolated_v2"]:
    """Resolve the prototype writer policy without claiming fleet isolation."""
    effective_mode = os.getenv("DURABLE_AGENTS_DEPLOYMENT_MODE") if deployment_mode is None else deployment_mode
    if effective_mode is None:
        return "isolated_v2"
    if isinstance(effective_mode, str):
        if effective_mode == "isolated_v2":
            return "isolated_v2"
        if effective_mode == "legacy":
            return "legacy"
    raise ValueError("deployment_mode must be unset, 'isolated_v2', or 'legacy'; other values are not supported.")


def validate_runtime_deployment(deployment_mode: str | None = None) -> None:
    """Validate the prototype writer policy; this does not detect peer workers."""
    resolve_runtime_deployment(deployment_mode)


def validate_response_delivery_window(response_delivery_window_seconds: int) -> None:
    """Require a positive integer window, excluding booleans and other types."""
    if (
        isinstance(response_delivery_window_seconds, bool)
        or not isinstance(response_delivery_window_seconds, int)
        or response_delivery_window_seconds <= 0
    ):
        raise ValueError("response_delivery_window_seconds must be a positive integer, not a boolean or another type.")


def validate_agent_configuration(agent: SupportsAgentRun, *, retention: RetentionMode = DEFAULT_RETENTION) -> None:
    """Dry-prepare history without replacing the registered caller-owned agent."""
    validate_retention(retention)
    try:
        ensure_durable_history(agent, prune_excluded=retention == "follow_compaction")
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("Could not prepare the agent's durable history configuration.") from exc


@dataclass(frozen=True)
class AgentRegistrationSettings:
    """Resolved values and callback identity for a reusable hosted registration."""

    # Keep the original positional constructor before the additive settings.
    response_delivery_window_seconds: int
    callback: AgentResponseCallbackProtocol | None = field(default=None, compare=False)
    retention: RetentionMode = field(default=DEFAULT_RETENTION, kw_only=True)
    max_state_bytes: int | None = field(default=None, kw_only=True)
    high_watermark: float = field(default=HIGH_WATERMARK, kw_only=True)
    low_watermark: float = field(default=LOW_WATERMARK, kw_only=True)

    def matches(self, other: AgentRegistrationSettings) -> bool:
        """Require value equality and the same callback instance."""
        return self == other and self.callback is other.callback


@dataclass(frozen=True)
class RegistrationIdentity:
    """Ownership of one derived host name, independent of backend registration APIs."""

    owner: object
    target: object
    kind: str
    settings: AgentRegistrationSettings
    label: str
    endpoints: tuple[bool, bool] = (False, False)

    def reserve(
        self,
        registrations: dict[tuple[str, str], RegistrationIdentity],
        name: str,
        *,
        namespace: Literal["entity-name", "activity-name", "orchestrator-name", "function-name"],
    ) -> None:
        """Preflight a case-insensitive name on a temporary registration mapping."""
        key = (namespace, name.casefold())
        existing = registrations.get(key)
        if existing is not None:
            if (
                existing.owner is not self.owner
                or existing.target is not self.target
                or existing.kind != self.kind
                or existing.label != self.label
            ):
                raise ValueError(
                    f"Derived name '{name}' for {self.label} collides with already registered "
                    f"{existing.label}. Names are compared case-insensitively; "
                    "different registrations must not share a durable identity."
                )
            if not existing.settings.matches(self.settings) or existing.endpoints != self.endpoints:
                raise ValueError(
                    f"'{name}' is already registered with different settings for {existing.label}; "
                    "shared registrations require identical configuration."
                )
            return
        registrations[key] = self
