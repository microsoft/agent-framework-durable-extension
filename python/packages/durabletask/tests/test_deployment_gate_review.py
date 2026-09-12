# Copyright (c) Microsoft. All rights reserved.

"""Deployment acknowledgement validation for the shared configuration and worker."""

from typing import Any
from unittest.mock import Mock

import pytest
from durabletask.worker import TaskHubGrpcWorker

from agent_framework_durabletask import DurableAIAgentWorker
from agent_framework_durabletask import _configuration as configuration_module
from agent_framework_durabletask import _worker as worker_module
from agent_framework_durabletask._configuration import validate_runtime_deployment

_ENVIRONMENT_VARIABLE = "DURABLE_AGENTS_DEPLOYMENT_MODE"
_INVALID_MODES = ("", "isolated_v1", "mixed", "ISOLATED_V2", " isolated_v2", "isolated_v2 ", "isolated_v2\n")


def test_missing_deployment_mode_explains_the_required_acknowledgement(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(_ENVIRONMENT_VARIABLE, raising=False)

    with pytest.raises(ValueError) as error:
        validate_runtime_deployment()

    message = str(error.value)
    assert "Schema 2 requires an isolated task hub/deployment with upgraded clients" in message
    assert "Old workflow histories must remain on the old engine" in message
    assert "deployment_mode='isolated_v2'" in message
    assert _ENVIRONMENT_VARIABLE in message
    assert "explicit operator acknowledgement" in message
    assert "not runtime proof" in message
    assert "cannot detect peer workers" in message


def test_none_deployment_mode_reads_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(_ENVIRONMENT_VARIABLE, "isolated_v2")
    validate_runtime_deployment()
    validate_runtime_deployment(deployment_mode=None)


@pytest.mark.parametrize("environment_mode", [None, "", "mixed"])
def test_explicit_valid_mode_overrides_missing_or_invalid_environment(
    monkeypatch: pytest.MonkeyPatch, environment_mode: str | None
) -> None:
    if environment_mode is None:
        monkeypatch.delenv(_ENVIRONMENT_VARIABLE, raising=False)
    else:
        monkeypatch.setenv(_ENVIRONMENT_VARIABLE, environment_mode)
    validate_runtime_deployment(deployment_mode="isolated_v2")


def test_explicit_mode_never_reads_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    getenv = Mock(side_effect=AssertionError("Explicit deployment mode must not read the environment"))
    with monkeypatch.context() as scoped:
        scoped.setattr(configuration_module.os, "getenv", getenv)
        validate_runtime_deployment(deployment_mode="isolated_v2")
        with pytest.raises(ValueError, match="isolated_v2"):
            validate_runtime_deployment(deployment_mode="")
        getenv.assert_not_called()


@pytest.mark.parametrize("deployment_mode", _INVALID_MODES)
def test_invalid_environment_mode_is_rejected(monkeypatch: pytest.MonkeyPatch, deployment_mode: str) -> None:
    monkeypatch.setenv(_ENVIRONMENT_VARIABLE, deployment_mode)
    with pytest.raises(ValueError, match="isolated_v2"):
        validate_runtime_deployment()


@pytest.mark.parametrize("deployment_mode", [*_INVALID_MODES, False, 2, ["isolated_v2"]])
def test_invalid_explicit_mode_is_not_overridden_by_valid_environment(
    monkeypatch: pytest.MonkeyPatch, deployment_mode: Any
) -> None:
    monkeypatch.setenv(_ENVIRONMENT_VARIABLE, "isolated_v2")
    with pytest.raises(ValueError, match="isolated_v2"):
        validate_runtime_deployment(deployment_mode=deployment_mode)


def test_worker_missing_mode_fails_before_configuration_or_registry_changes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(_ENVIRONMENT_VARIABLE, raising=False)
    native = Mock(spec=TaskHubGrpcWorker)
    agent_configuration = Mock(side_effect=AssertionError("Agent configuration ran before deployment validation"))
    retention = Mock(side_effect=AssertionError("Retention configuration ran before deployment validation"))
    monkeypatch.setattr(worker_module, "validate_agent_configuration", agent_configuration)
    monkeypatch.setattr(worker_module, "validate_retention", retention)
    host = DurableAIAgentWorker.__new__(DurableAIAgentWorker)

    with pytest.raises(ValueError, match="isolated_v2"):
        DurableAIAgentWorker.__init__(host, native)

    assert vars(host) == {}
    assert native.mock_calls == []
    agent_configuration.assert_not_called()
    retention.assert_not_called()


@pytest.mark.parametrize("deployment_mode", _INVALID_MODES)
def test_worker_rejects_explicit_invalid_mode_despite_valid_environment(
    monkeypatch: pytest.MonkeyPatch, deployment_mode: str
) -> None:
    monkeypatch.setenv(_ENVIRONMENT_VARIABLE, "isolated_v2")
    native = Mock(spec=TaskHubGrpcWorker)
    with pytest.raises(ValueError, match="isolated_v2"):
        DurableAIAgentWorker(native, deployment_mode=deployment_mode)
    assert native.mock_calls == []


@pytest.mark.parametrize("deployment_mode", _INVALID_MODES)
def test_worker_rejects_invalid_environment_mode(monkeypatch: pytest.MonkeyPatch, deployment_mode: str) -> None:
    monkeypatch.setenv(_ENVIRONMENT_VARIABLE, deployment_mode)
    native = Mock(spec=TaskHubGrpcWorker)
    with pytest.raises(ValueError, match="isolated_v2"):
        DurableAIAgentWorker(native)
    assert native.mock_calls == []


@pytest.mark.parametrize("source", ["explicit", "environment"])
def test_worker_accepts_isolated_mode_and_preserves_entity_names(monkeypatch: pytest.MonkeyPatch, source: str) -> None:
    kwargs: dict[str, Any] = {}
    if source == "explicit":
        monkeypatch.delenv(_ENVIRONMENT_VARIABLE, raising=False)
        kwargs["deployment_mode"] = "isolated_v2"
    else:
        monkeypatch.setenv(_ENVIRONMENT_VARIABLE, "isolated_v2")
    native = Mock(spec=TaskHubGrpcWorker)
    native.add_entity.return_value = "dafx-assistant"
    host = DurableAIAgentWorker(native, **kwargs)

    # The private worker factory relies on the host's completed deployment validation.
    monkeypatch.delenv(_ENVIRONMENT_VARIABLE, raising=False)
    agent = Mock(context_providers=None)
    agent.name = "assistant"
    host.add_agent(agent)

    assert host.registered_agent_names == ["assistant"]
    native.add_entity.assert_called_once()
    assert native.add_entity.call_args.args[0].__name__ == "dafx-assistant"
