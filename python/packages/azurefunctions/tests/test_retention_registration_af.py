# Copyright (c) Microsoft. All rights reserved.

"""Functions registration validation and settings forwarded to the AgentEntity consumer."""

from collections.abc import Callable, Iterator
from inspect import signature
from typing import Any, get_args
from unittest.mock import Mock, patch

import azure.durable_functions as df
import pytest
from agent_framework import Agent, AgentExecutor, Executor, InMemoryHistoryProvider, WorkflowExecutor
from agent_framework_durabletask import (
    DEFAULT_MAX_STATE_BYTES,
    DEFAULT_RETENTION,
    DELIVERY_WINDOW_SECONDS,
    HIGH_WATERMARK,
    INHERIT,
    LOW_WATERMARK,
    RetentionMode,
)

from agent_framework_azurefunctions import AgentFunctionApp
from agent_framework_azurefunctions._entities import AzureFunctionEntityStateProvider, create_agent_entity

EntityHandler = Callable[[df.DurableEntityContext], None]


def _agent(name: str = "assistant", *, ambiguous_history: bool = False) -> Agent:
    client: Any = Mock(additional_properties={}, STORES_BY_DEFAULT=False)
    providers = (
        [InMemoryHistoryProvider(source_id="first"), InMemoryHistoryProvider(source_id="second")]
        if ambiguous_history
        else [InMemoryHistoryProvider(source_id="primary")]
    )
    return Agent(client=client, name=name, context_providers=providers)


def _workflow(name: str, *agents: Agent, child: Mock | None = None) -> Mock:
    executors: dict[str, Mock] = {}
    for index, agent in enumerate(agents):
        node = Mock(spec=AgentExecutor)
        node.id = f"node{index}"
        node.agent = agent
        executors[node.id] = node
    if child is not None:
        nested = Mock(spec=WorkflowExecutor)
        nested.id = "child"
        nested.workflow = child
        executors[nested.id] = nested
    activity = Mock(spec=Executor)
    activity.id = "activity"
    executors[activity.id] = activity
    workflow = Mock()
    workflow.name = name
    workflow.executors = executors
    return workflow


@pytest.fixture
def registered_entities() -> Iterator[dict[str, EntityHandler]]:
    registered: dict[str, EntityHandler] = {}

    def capture(*, context_name: str, entity_name: str) -> Callable[[EntityHandler], EntityHandler]:
        assert context_name == "context"

        def decorate(handler: EntityHandler) -> EntityHandler:
            registered[entity_name] = handler
            return handler

        return decorate

    with patch.object(AgentFunctionApp, "entity_trigger", side_effect=capture):
        yield registered


def _consumer_settings(handler: EntityHandler) -> dict[str, Any]:
    context = Mock()
    context.operation_name = "reset"
    with patch("agent_framework_azurefunctions._entities.AgentEntity") as consumer:
        handler(context)
    consumer.assert_called_once()
    consumer.return_value.reset.assert_called_once_with()
    context.set_result.assert_called_once_with({"status": "reset"})
    kwargs = consumer.call_args.kwargs
    assert isinstance(kwargs["state_provider"], AzureFunctionEntityStateProvider)
    return dict(kwargs)


def _assert_settings(actual: dict[str, Any], **expected: Any) -> None:
    assert {key: actual[key] for key in expected} == expected


def _app(**kwargs: Any) -> AgentFunctionApp:
    return AgentFunctionApp(enable_health_check=False, enable_http_endpoints=False, **kwargs)


def test_functions_uses_the_public_inheritance_sentinel() -> None:
    assert signature(AgentFunctionApp).parameters["workflow_max_state_bytes"].default is INHERIT
    assert signature(AgentFunctionApp.add_agent).parameters["max_state_bytes"].default is INHERIT
    assert signature(AgentFunctionApp.configure_workflow).parameters["max_state_bytes"].default is INHERIT


def test_app_defaults_reach_the_entity_consumer(registered_entities: dict[str, EntityHandler]) -> None:
    _app(agents=[_agent()])

    assert DEFAULT_RETENTION == "keep_all"
    assert DEFAULT_MAX_STATE_BYTES is None
    _assert_settings(
        _consumer_settings(registered_entities["dafx-assistant"]),
        retention=DEFAULT_RETENTION,
        max_state_bytes=None,
        high_watermark=HIGH_WATERMARK,
        low_watermark=LOW_WATERMARK,
        response_delivery_window_seconds=DELIVERY_WINDOW_SECONDS,
    )


@pytest.mark.parametrize("retention", get_args(RetentionMode))
@pytest.mark.parametrize("budget", [None, 8192])
def test_pressure_budget_is_independent_of_retention(
    registered_entities: dict[str, EntityHandler], retention: RetentionMode, budget: int | None
) -> None:
    _app(
        agents=[_agent()],
        retention=retention,
        max_state_bytes=budget,
        high_watermark=0.95,
        low_watermark=0.8,
        response_delivery_window_seconds=120,
    )
    _assert_settings(
        _consumer_settings(registered_entities["dafx-assistant"]),
        retention=retention,
        max_state_bytes=budget,
        high_watermark=0.95,
        low_watermark=0.8,
        response_delivery_window_seconds=120,
    )


@pytest.mark.parametrize("surface", ["agent", "workflow", "workflow_default"])
@pytest.mark.parametrize(
    "overrides,expected",
    [
        ({}, 8192),
        ({"max_state_bytes": INHERIT}, 8192),
        ({"max_state_bytes": None}, None),
        ({"max_state_bytes": 4096}, 4096),
    ],
)
def test_budget_override_distinguishes_omitted_and_disabled(
    registered_entities: dict[str, EntityHandler], surface: str, overrides: dict[str, Any], expected: int | None
) -> None:
    if surface == "workflow_default":
        _app(
            workflow=_workflow("flow", _agent()),
            max_state_bytes=8192,
            **{f"workflow_{key}": value for key, value in overrides.items()},
        )
    else:
        app = _app(max_state_bytes=8192)
        if surface == "agent":
            app.add_agent(_agent(), **overrides)
        else:
            app.configure_workflow(_workflow("flow", _agent()), **overrides)

    assert len(registered_entities) == 1
    handler = next(iter(registered_entities.values()))
    assert _consumer_settings(handler)["max_state_bytes"] == expected


def test_per_agent_overrides_leave_host_defaults_unchanged(registered_entities: dict[str, EntityHandler]) -> None:
    app = _app(
        retention="follow_compaction",
        max_state_bytes=8192,
        high_watermark=0.95,
        low_watermark=0.8,
        response_delivery_window_seconds=120,
    )
    app.add_agent(
        _agent("override"),
        retention="keep_all",
        max_state_bytes=None,
        high_watermark=0.9,
        low_watermark=0.6,
        response_delivery_window_seconds=15,
    )
    app.add_agent(_agent("inherited"))

    _assert_settings(
        _consumer_settings(registered_entities["dafx-override"]),
        retention="keep_all",
        max_state_bytes=None,
        high_watermark=0.9,
        low_watermark=0.6,
        response_delivery_window_seconds=15,
    )
    _assert_settings(
        _consumer_settings(registered_entities["dafx-inherited"]),
        retention="follow_compaction",
        max_state_bytes=8192,
        high_watermark=0.95,
        low_watermark=0.8,
        response_delivery_window_seconds=120,
    )


@pytest.mark.parametrize("retention", get_args(RetentionMode))
def test_workflow_defaults_apply_to_nested_agents_not_standalone_agents(
    registered_entities: dict[str, EntityHandler], retention: RetentionMode
) -> None:
    inner = _workflow("inner", _agent("inneragent"))
    outer = _workflow("outer", _agent("outeragent"), child=inner)
    _app(
        agents=[_agent("standalone")],
        workflow=outer,
        max_state_bytes=8192,
        workflow_retention=retention,
        workflow_max_state_bytes=None,
        workflow_high_watermark=0.9,
        workflow_low_watermark=0.6,
        workflow_response_delivery_window_seconds=20,
    )

    for name in ("dafx-outer-node0", "dafx-inner-node0"):
        _assert_settings(
            _consumer_settings(registered_entities[name]),
            retention=retention,
            max_state_bytes=None,
            high_watermark=0.9,
            low_watermark=0.6,
            response_delivery_window_seconds=20,
        )
    _assert_settings(
        _consumer_settings(registered_entities["dafx-standalone"]),
        retention=DEFAULT_RETENTION,
        max_state_bytes=8192,
        high_watermark=HIGH_WATERMARK,
        low_watermark=LOW_WATERMARK,
        response_delivery_window_seconds=DELIVERY_WINDOW_SECONDS,
    )


def test_per_workflow_overrides_apply_to_all_new_nested_agents(
    registered_entities: dict[str, EntityHandler],
) -> None:
    app = _app(
        workflow_retention="follow_compaction",
        workflow_max_state_bytes=8192,
        workflow_high_watermark=0.95,
        workflow_low_watermark=0.8,
        workflow_response_delivery_window_seconds=120,
    )
    inner = _workflow("inner", _agent("inneragent"))
    outer = _workflow("outer", _agent("outeragent"), child=inner)
    app.configure_workflow(
        outer,
        retention="keep_all",
        max_state_bytes=None,
        high_watermark=0.9,
        low_watermark=0.6,
        response_delivery_window_seconds=15,
    )
    assert app.workflow is outer
    app.configure_workflow(_workflow("inherited", _agent("other")))
    assert app.workflow is None

    for name in ("dafx-outer-node0", "dafx-inner-node0"):
        _assert_settings(
            _consumer_settings(registered_entities[name]),
            retention="keep_all",
            max_state_bytes=None,
            high_watermark=0.9,
            low_watermark=0.6,
            response_delivery_window_seconds=15,
        )
    _assert_settings(
        _consumer_settings(registered_entities["dafx-inherited-node0"]),
        retention="follow_compaction",
        max_state_bytes=8192,
        high_watermark=0.95,
        low_watermark=0.8,
        response_delivery_window_seconds=120,
    )


_INVALID_SETTINGS: list[dict[str, Any]] = [
    {"retention": "auto"},
    {"retention": "invalid"},
    *({"max_state_bytes": value} for value in [0, -1, True, False, 1.5, "8192", "inherit", "backend_limit"]),
    *({"high_watermark": value} for value in [0, 1.1, True, float("nan"), float("inf")]),
    *({"low_watermark": value} for value in [0, -0.1, True, float("nan"), float("inf")]),
    {"high_watermark": 0.7, "low_watermark": 0.7},
    {"high_watermark": 0.6, "low_watermark": 0.7},
    *(
        {"response_delivery_window_seconds": value}
        for value in [0, -1, True, False, 1.5, "60", float("nan"), float("inf")]
    ),
]


@pytest.mark.parametrize("settings", _INVALID_SETTINGS)
@pytest.mark.parametrize("surface", ["host", "workflow_default", "agent", "workflow", "factory"])
def test_invalid_settings_fail_before_registration(
    registered_entities: dict[str, EntityHandler], surface: str, settings: dict[str, Any]
) -> None:
    with (
        patch.object(AgentFunctionApp, "_setup_http_run_route") as http,
        patch.object(AgentFunctionApp, "_setup_mcp_tool_trigger") as mcp,
        patch.object(AgentFunctionApp, "_setup_executor_activity") as activity,
        patch.object(AgentFunctionApp, "_setup_workflow_orchestration") as orchestration,
        patch.object(AgentFunctionApp, "_register_workflow_routes") as routes,
    ):
        if surface in ("host", "workflow_default", "factory"):
            with pytest.raises(ValueError):
                if surface == "host":
                    _app(agents=[_agent()], **settings)
                elif surface == "workflow_default":
                    _app(
                        workflow=_workflow("flow", _agent()),
                        **{f"workflow_{key}": value for key, value in settings.items()},
                    )
                else:
                    create_agent_entity(_agent(), **settings)
        else:
            app = _app()
            with pytest.raises(ValueError):
                if surface == "agent":
                    app.add_agent(_agent(), **settings)
                else:
                    app.configure_workflow(_workflow("flow", _agent()), **settings)
            assert app.agents == {}
            assert app.workflows == {}
            assert app._registered_orchestrations == {}
        for registration in (http, mcp, activity, orchestration, routes):
            registration.assert_not_called()
    assert registered_entities == {}


@pytest.mark.parametrize("surface", ["agent", "workflow", "nested_workflow"])
def test_ambiguous_history_fails_before_any_registration(
    registered_entities: dict[str, EntityHandler], surface: str
) -> None:
    app = _app()
    agent = _agent(ambiguous_history=True)
    original_providers = agent.context_providers
    with pytest.raises(ValueError, match="primary"):
        if surface == "agent":
            app.add_agent(agent)
        elif surface == "workflow":
            app.configure_workflow(_workflow("flow", _agent("good"), agent))
        else:
            app.configure_workflow(_workflow("outer", _agent("good"), child=_workflow("inner", agent)))

    assert agent.context_providers is original_providers
    assert all(isinstance(provider, InMemoryHistoryProvider) for provider in original_providers)
    assert app.agents == {}
    assert app.workflows == {}
    assert app._registered_orchestrations == {}
    assert registered_entities == {}


@pytest.mark.parametrize("surface", ["agents", "workflow", "workflows"])
def test_constructor_preflights_all_initial_agents_and_workflows(
    registered_entities: dict[str, EntityHandler], surface: str
) -> None:
    good, bad = _agent("good"), _agent("bad", ambiguous_history=True)
    with (
        patch.object(AgentFunctionApp, "_setup_agent_functions") as setup_agent,
        patch.object(AgentFunctionApp, "_register_workflow_primitives") as setup_workflow,
        pytest.raises(ValueError, match="primary"),
    ):
        if surface == "agents":
            _app(agents=[good, bad])
        elif surface == "workflow":
            _app(workflow=_workflow("outer", good, child=_workflow("inner", bad)))
        else:
            _app(workflows=[_workflow("first", good), _workflow("second", bad)])
    setup_agent.assert_not_called()
    setup_workflow.assert_not_called()
    assert registered_entities == {}


def test_registration_and_factory_validation_do_not_replace_history(
    registered_entities: dict[str, EntityHandler],
) -> None:
    agent = _agent()
    original_providers = agent.context_providers
    with patch("agent_framework_azurefunctions._entities.AgentEntity") as consumer:
        _app(agents=[agent], retention="follow_compaction")
    consumer.assert_not_called()
    assert "dafx-assistant" in registered_entities
    assert agent.context_providers is original_providers
    assert isinstance(agent.context_providers[0], InMemoryHistoryProvider)


def test_functions_backend_limit_error_is_raised_before_invocation() -> None:
    with pytest.raises(ValueError, match="max_state_bytes.*backend_limit"):
        create_agent_entity(_agent(), max_state_bytes="backend_limit")


@pytest.mark.parametrize("invalid_name", [None, "", "invalid name"])
def test_constructor_keeps_name_validation_before_workflow_traversal(
    registered_entities: dict[str, EntityHandler], invalid_name: Any
) -> None:
    with pytest.raises(ValueError, match="Workflow name"):
        _app(workflows=[_workflow("valid", _agent()), _workflow(invalid_name, _agent("invalid"))])
    assert registered_entities == {}


def test_function_setup_failure_does_not_record_agent_metadata() -> None:
    app = _app()
    with (
        patch.object(app, "_setup_agent_functions", side_effect=RuntimeError("registration failed")),
        pytest.raises(RuntimeError, match="registration failed"),
    ):
        app.add_agent(_agent())
    assert app.agents == {}
