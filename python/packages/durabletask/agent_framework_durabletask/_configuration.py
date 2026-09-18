# Copyright (c) Microsoft. All rights reserved.

"""Shared registration validation for isolated canonical-state deployments."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Literal

from agent_framework import SupportsAgentRun

from ._callbacks import AgentResponseCallbackProtocol
from ._history_provider import ensure_durable_history


def validate_runtime_deployment(deployment_mode: str | None = None) -> None:
    """Require operator acknowledgement, not runtime proof, of deployment isolation."""
    effective_mode = os.getenv("DURABLE_AGENTS_DEPLOYMENT_MODE") if deployment_mode is None else deployment_mode
    if not isinstance(effective_mode, str) or effective_mode != "isolated_v2":
        raise ValueError(
            "Schema 2 requires an isolated task hub/deployment with upgraded clients. "
            "Old workflow histories must remain on the old engine. "
            "Set deployment_mode='isolated_v2' or DURABLE_AGENTS_DEPLOYMENT_MODE='isolated_v2'; "
            "no other deployment mode is accepted. This is an explicit operator acknowledgement, "
            "not runtime proof of isolation, and cannot detect peer workers."
        )


def validate_response_delivery_window(response_delivery_window_seconds: int) -> None:
    """Require a positive integer window, excluding booleans and other types."""
    if (
        isinstance(response_delivery_window_seconds, bool)
        or not isinstance(response_delivery_window_seconds, int)
        or response_delivery_window_seconds <= 0
    ):
        raise ValueError("response_delivery_window_seconds must be a positive integer, not a boolean or another type.")


def validate_agent_configuration(agent: SupportsAgentRun) -> None:
    """Dry-prepare history without replacing the registered caller-owned agent."""
    try:
        ensure_durable_history(agent)
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("Could not prepare the agent's durable history configuration.") from exc


@dataclass(frozen=True)
class AgentRegistrationSettings:
    """Resolved values and callback identity for a reusable hosted registration."""

    response_delivery_window_seconds: int
    callback: AgentResponseCallbackProtocol | None = field(default=None, compare=False)

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
