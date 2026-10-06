# Copyright (c) Microsoft. All rights reserved.

"""Functions host deployment acknowledgement and delivery window configuration."""

from typing import Any

import pytest

from agent_framework_azurefunctions import AgentFunctionApp


def test_app_constructor_requires_explicit_isolated_v2_when_env_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    with pytest.raises(ValueError, match="Schema 2 requires an isolated task hub/deployment"):
        AgentFunctionApp(enable_health_check=False)


@pytest.mark.parametrize("value", ["legacy", "isolated", "", "ISOLATED_V2"])
def test_app_constructor_rejects_invalid_deployment_mode_values(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    with pytest.raises(ValueError, match="no other deployment mode is accepted"):
        AgentFunctionApp(enable_health_check=False, deployment_mode=value)


def test_app_constructor_accepts_explicit_isolated_v2_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    app = AgentFunctionApp(enable_health_check=False, deployment_mode="isolated_v2")
    assert app._deployment_mode == "isolated_v2"


@pytest.mark.parametrize("value", ["legacy", "isolated", "", "ISOLATED_V2"])
def test_app_constructor_rejects_invalid_deployment_environment(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("DURABLE_AGENTS_DEPLOYMENT_MODE", value)
    with pytest.raises(ValueError, match="no other deployment mode is accepted"):
        AgentFunctionApp(enable_health_check=False)


@pytest.mark.parametrize(
    ("value", "valid"),
    [
        pytest.param(True, False, id="bool-true"),
        pytest.param(False, False, id="bool-false"),
        pytest.param(17, True, id="int"),
        pytest.param("17", False, id="string"),
    ],
)
def test_app_constructor_delivery_window_matrix_matches_configuration(value: Any, valid: bool) -> None:
    if valid:
        app = AgentFunctionApp(
            enable_health_check=False, deployment_mode="isolated_v2", response_delivery_window_seconds=value
        )
        assert app._response_delivery_window_seconds == value
    else:
        with pytest.raises(ValueError, match="positive integer"):
            AgentFunctionApp(
                enable_health_check=False, deployment_mode="isolated_v2", response_delivery_window_seconds=value
            )  # type: ignore[arg-type]
