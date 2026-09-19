# Copyright (c) Microsoft. All rights reserved.

"""Deployment acknowledgement validation for Functions hosts and entity factories."""

from typing import Any
from unittest.mock import Mock

import pytest

from agent_framework_azurefunctions import AgentFunctionApp
from agent_framework_azurefunctions import _app as app_module
from agent_framework_azurefunctions import _entities as entities_module
from agent_framework_azurefunctions._entities import create_agent_entity

_ENVIRONMENT_VARIABLE = "DURABLE_AGENTS_DEPLOYMENT_MODE"
_INVALID_MODES = ("", "isolated_v1", "mixed", "ISOLATED_V2", " isolated_v2", "isolated_v2 ", "isolated_v2\n")


@pytest.fixture
def agent() -> Mock:
    instance = Mock(context_providers=None)
    instance.name = "assistant"
    return instance


@pytest.mark.parametrize("surface", ["app", "factory"])
def test_missing_mode_fails_before_native_constructor_or_agent_configuration(
    monkeypatch: pytest.MonkeyPatch, agent: Mock, surface: str
) -> None:
    monkeypatch.delenv(_ENVIRONMENT_VARIABLE, raising=False)
    native_init = Mock(side_effect=AssertionError("Native constructor ran before deployment validation"))
    configuration = Mock(side_effect=AssertionError("Agent configuration ran before deployment validation"))
    reserve = Mock(side_effect=AssertionError("Registry reservation ran before deployment validation"))
    monkeypatch.setattr(app_module.DFAppBase, "__init__", native_init)
    monkeypatch.setattr(app_module, "validate_agent_configuration", configuration)
    monkeypatch.setattr(entities_module, "validate_agent_configuration", configuration)
    monkeypatch.setattr(app_module.RegistrationIdentity, "reserve", reserve)

    with pytest.raises(ValueError, match="isolated_v2"):
        if surface == "app":
            AgentFunctionApp(agents=[agent])
        else:
            create_agent_entity(agent)

    native_init.assert_not_called()
    configuration.assert_not_called()
    reserve.assert_not_called()


@pytest.mark.parametrize("surface", ["app", "factory"])
@pytest.mark.parametrize("deployment_mode", _INVALID_MODES)
def test_invalid_explicit_mode_is_not_overridden_by_valid_environment(
    monkeypatch: pytest.MonkeyPatch, agent: Mock, surface: str, deployment_mode: str
) -> None:
    monkeypatch.setenv(_ENVIRONMENT_VARIABLE, "isolated_v2")
    with pytest.raises(ValueError, match="isolated_v2"):
        if surface == "app":
            AgentFunctionApp(agents=[agent], deployment_mode=deployment_mode)
        else:
            create_agent_entity(agent, deployment_mode=deployment_mode)


@pytest.mark.parametrize("surface", ["app", "factory"])
@pytest.mark.parametrize("deployment_mode", _INVALID_MODES)
def test_invalid_environment_mode_is_rejected(
    monkeypatch: pytest.MonkeyPatch, agent: Mock, surface: str, deployment_mode: str
) -> None:
    monkeypatch.setenv(_ENVIRONMENT_VARIABLE, deployment_mode)
    with pytest.raises(ValueError, match="isolated_v2"):
        if surface == "app":
            AgentFunctionApp(agents=[agent])
        else:
            create_agent_entity(agent)


@pytest.mark.parametrize("source", ["explicit", "environment"])
def test_direct_factory_accepts_isolated_mode(monkeypatch: pytest.MonkeyPatch, agent: Mock, source: str) -> None:
    if source == "explicit":
        monkeypatch.delenv(_ENVIRONMENT_VARIABLE, raising=False)
        handler = create_agent_entity(agent, deployment_mode="isolated_v2")
    else:
        monkeypatch.setenv(_ENVIRONMENT_VARIABLE, "isolated_v2")
        handler = create_agent_entity(agent)
    assert callable(handler)


@pytest.mark.parametrize("source", ["explicit", "environment"])
def test_app_passes_effective_mode_to_initial_and_later_factories(
    monkeypatch: pytest.MonkeyPatch, agent: Mock, source: str
) -> None:
    kwargs: dict[str, Any] = {}
    if source == "explicit":
        monkeypatch.delenv(_ENVIRONMENT_VARIABLE, raising=False)
        kwargs["deployment_mode"] = "isolated_v2"
    else:
        monkeypatch.setenv(_ENVIRONMENT_VARIABLE, "isolated_v2")
    factory = Mock(wraps=entities_module.create_agent_entity)
    monkeypatch.setattr(app_module, "create_agent_entity", factory)
    app = AgentFunctionApp(agents=[agent], enable_health_check=False, enable_http_endpoints=False, **kwargs)
    assert app._deployment_mode == "isolated_v2"

    # Later registrations retain the acknowledged mode instead of reading the environment again.
    monkeypatch.delenv(_ENVIRONMENT_VARIABLE, raising=False)
    later = Mock(context_providers=None)
    later.name = "later"
    app.add_agent(later)

    assert factory.call_count == 2
    assert all(call.kwargs["deployment_mode"] == "isolated_v2" for call in factory.call_args_list)
    assert app.agents == {"assistant": agent, "later": later}
    names: list[str] = []
    for function in app.get_functions():
        name = function.get_function_name()
        assert name is not None
        names.append(name)
    assert sorted(names) == ["dafx-assistant", "dafx-later"]
