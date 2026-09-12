# Copyright (c) Microsoft. All rights reserved.

"""Shared, typed overrides for durable agent registration."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Final, Literal, TypeAlias

from agent_framework import SupportsAgentRun

from ._callbacks import AgentResponseCallbackProtocol
from ._history_provider import ensure_durable_history
from ._retention import DEFAULT_RETENTION, RetentionMode, StateBudget, resolve_state_budget, validate_retention

__all__ = [
    "INHERIT",
    "AgentRegistrationSettings",
    "Inherit",
    "RegistrationIdentity",
    "StateBudgetOverride",
    "resolve_state_budget_override",
    "validate_agent_configuration",
    "validate_response_delivery_window",
    "validate_runtime_deployment",
]


def validate_runtime_deployment(deployment_mode: str | None = None) -> None:
    """Require explicit acknowledgement of an isolated schema 2 deployment.

    Schema 2 requires an isolated task hub/deployment with upgraded clients.
    Old workflow histories must remain on the old engine. This is an operator
    acknowledgement, not runtime proof of isolation, and cannot detect peer workers.

    Args:
        deployment_mode: Exactly ``isolated_v2``. Only when None, read
            ``DURABLE_AGENTS_DEPLOYMENT_MODE`` instead.

    Raises:
        ValueError: The deployment mode is missing or is not exactly ``isolated_v2``.
    """
    effective_mode = os.getenv("DURABLE_AGENTS_DEPLOYMENT_MODE") if deployment_mode is None else deployment_mode
    if not isinstance(effective_mode, str) or effective_mode != "isolated_v2":
        raise ValueError(
            "Schema 2 requires an isolated task hub/deployment with upgraded clients. "
            "Old workflow histories must remain on the old engine. "
            "Set deployment_mode='isolated_v2' or DURABLE_AGENTS_DEPLOYMENT_MODE='isolated_v2'; "
            "no other deployment mode is accepted. This is an explicit operator acknowledgement, "
            "not runtime proof of isolation, and cannot detect peer workers."
        )


class Inherit(Enum):
    """Use the enclosing host's setting instead of an explicit override."""

    INHERIT = "inherit"


INHERIT: Final[Inherit] = Inherit.INHERIT
"""Inherit the configured budget; unlike None, this does not disable pressure eviction."""

StateBudgetOverride: TypeAlias = StateBudget | Inherit


@dataclass(frozen=True)
class AgentRegistrationSettings:
    """Resolved settings used to check whether a hosted registration can be reused."""

    retention: RetentionMode
    max_state_bytes: int | None
    high_watermark: float
    low_watermark: float
    response_delivery_window_seconds: int
    callback: AgentResponseCallbackProtocol | None = field(default=None, compare=False)

    def matches(self, other: AgentRegistrationSettings) -> bool:
        """Compare values, but require the same callback instance."""
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
        """Reserve a case-insensitive name in its backend artifact namespace.

        Call on a temporary mapping during preflight. Publishing that mapping is the
        host's responsibility, after all backend registrations have succeeded.
        """
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


def validate_agent_configuration(agent: SupportsAgentRun, *, retention: RetentionMode = DEFAULT_RETENTION) -> None:
    """Dry-prepare durable history, including copy/attachment validation.

    Discard the prepared view so the host's agent registry retains the caller's
    original instance. Entity construction prepares its own view at invocation.
    """
    validate_retention(retention)
    try:
        ensure_durable_history(agent, prune_excluded=retention == "follow_compaction")
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("Could not prepare the agent's durable history configuration.") from exc


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
