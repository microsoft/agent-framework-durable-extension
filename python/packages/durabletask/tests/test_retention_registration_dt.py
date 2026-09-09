# Copyright (c) Microsoft. All rights reserved.

"""Registration-time validation and forwarding through the worker's real entity factory."""

from enum import Enum
from inspect import signature
from typing import Any, get_args
from unittest.mock import Mock, patch

import pytest
from agent_framework import Agent, AgentExecutor, Executor, InMemoryHistoryProvider, WorkflowExecutor

import agent_framework_durabletask as durabletask
from agent_framework_durabletask import (
    DEFAULT_MAX_STATE_BYTES,
    DEFAULT_RETENTION,
    DELIVERY_WINDOW_SECONDS,
    DTS_MAX_STATE_BYTES,
    HIGH_WATERMARK,
    INHERIT,
    LOW_WATERMARK,
    DurableAIAgentWorker,
    Inherit,
    RetentionMode,
    StateBudgetOverride,
    _configuration,
    resolve_state_budget_override,
    validate_history_providers,
)


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


def _consumer_settings(grpc_worker: Mock, index: int = 0) -> dict[str, Any]:
    entity_class = grpc_worker.add_entity.call_args_list[index].args[0]
    with patch("agent_framework_durabletask._worker.AgentEntity") as consumer:
        entity = entity_class()
    consumer.assert_called_once()
    kwargs = consumer.call_args.kwargs
    assert kwargs["state_provider"] is entity
    return dict(kwargs)


def _assert_settings(actual: dict[str, Any], **expected: Any) -> None:
    assert {key: actual[key] for key in expected} == expected


def test_public_inheritance_contract_is_typed_and_exported() -> None:
    assert isinstance(INHERIT, Enum)
    assert INHERIT is Inherit.INHERIT
    assert Inherit in get_args(StateBudgetOverride)
    assert signature(DurableAIAgentWorker.add_agent).parameters["max_state_bytes"].default is INHERIT
    assert signature(DurableAIAgentWorker.configure_workflow).parameters["max_state_bytes"].default is INHERIT
    for name in _configuration.__all__:
        assert name in durabletask.__all__
        assert getattr(durabletask, name) is getattr(_configuration, name)
    assert callable(validate_history_providers)


@pytest.mark.parametrize("inherited", list(Inherit))
def test_only_the_enum_inherits_a_budget(inherited: Inherit) -> None:
    assert resolve_state_budget_override(inherited, 8192) == 8192
    assert resolve_state_budget_override(inherited, None) is None
    assert resolve_state_budget_override(None, 8192) is None
    with pytest.raises(ValueError, match="max_state_bytes"):
        resolve_state_budget_override("inherit", 8192)  # type: ignore[arg-type]


def test_worker_defaults_reach_the_entity_consumer() -> None:
    grpc_worker = Mock()
    worker = DurableAIAgentWorker(grpc_worker)
    worker.add_agent(_agent())

    assert DEFAULT_RETENTION == "keep_all"
    assert DEFAULT_MAX_STATE_BYTES is None
    _assert_settings(
        _consumer_settings(grpc_worker),
        retention=DEFAULT_RETENTION,
        max_state_bytes=None,
        high_watermark=HIGH_WATERMARK,
        low_watermark=LOW_WATERMARK,
        response_delivery_window_seconds=DELIVERY_WINDOW_SECONDS,
    )


@pytest.mark.parametrize("retention", get_args(RetentionMode))
@pytest.mark.parametrize("budget,expected", [(None, None), (8192, 8192), ("backend_limit", DTS_MAX_STATE_BYTES)])
def test_worker_pressure_budget_is_independent_of_retention(
    retention: RetentionMode, budget: Any, expected: int | None
) -> None:
    grpc_worker = Mock()
    worker = DurableAIAgentWorker(
        grpc_worker,
        retention=retention,
        max_state_bytes=budget,
        high_watermark=0.95,
        low_watermark=0.8,
        response_delivery_window_seconds=120,
    )
    worker.add_agent(_agent())

    _assert_settings(
        _consumer_settings(grpc_worker),
        retention=retention,
        max_state_bytes=expected,
        high_watermark=0.95,
        low_watermark=0.8,
        response_delivery_window_seconds=120,
    )


@pytest.mark.parametrize("surface", ["agent", "workflow"])
@pytest.mark.parametrize(
    "overrides,expected",
    [
        ({}, 8192),
        ({"max_state_bytes": INHERIT}, 8192),
        ({"max_state_bytes": None}, None),
        ({"max_state_bytes": 4096}, 4096),
        ({"max_state_bytes": "backend_limit"}, DTS_MAX_STATE_BYTES),
    ],
)
def test_budget_override_distinguishes_omitted_and_disabled(
    surface: str, overrides: dict[str, Any], expected: int | None
) -> None:
    grpc_worker = Mock()
    worker = DurableAIAgentWorker(grpc_worker, max_state_bytes=8192)
    if surface == "agent":
        worker.add_agent(_agent(), **overrides)
    else:
        worker.configure_workflow(_workflow("flow", _agent()), **overrides)

    assert _consumer_settings(grpc_worker)["max_state_bytes"] == expected


def test_per_agent_overrides_do_not_change_the_host_defaults_or_callbacks() -> None:
    grpc_worker = Mock()
    default_callback, specific_callback = Mock(), Mock()
    worker = DurableAIAgentWorker(
        grpc_worker,
        callback=default_callback,
        retention="follow_compaction",
        max_state_bytes=8192,
        high_watermark=0.95,
        low_watermark=0.8,
        response_delivery_window_seconds=120,
    )
    worker.add_agent(
        _agent("override"),
        callback=specific_callback,
        retention="keep_all",
        max_state_bytes=None,
        high_watermark=0.9,
        low_watermark=0.6,
        response_delivery_window_seconds=15,
    )
    worker.add_agent(_agent("inherited"))

    _assert_settings(
        _consumer_settings(grpc_worker),
        callback=specific_callback,
        retention="keep_all",
        max_state_bytes=None,
        high_watermark=0.9,
        low_watermark=0.6,
        response_delivery_window_seconds=15,
    )
    _assert_settings(
        _consumer_settings(grpc_worker, 1),
        callback=default_callback,
        retention="follow_compaction",
        max_state_bytes=8192,
        high_watermark=0.95,
        low_watermark=0.8,
        response_delivery_window_seconds=120,
    )


@pytest.mark.parametrize("retention", get_args(RetentionMode))
def test_workflow_overrides_reach_every_new_nested_entity(retention: RetentionMode) -> None:
    grpc_worker = Mock()
    worker = DurableAIAgentWorker(grpc_worker, max_state_bytes=8192)
    inner = _workflow("inner", _agent("inneragent"))
    outer = _workflow("outer", _agent("outeragent"), child=inner)
    worker.configure_workflow(
        outer,
        retention=retention,
        max_state_bytes=None,
        high_watermark=0.9,
        low_watermark=0.6,
        response_delivery_window_seconds=20,
    )

    assert worker.registered_agent_names == ["outer-node0", "inner-node0"]
    for index in range(grpc_worker.add_entity.call_count):
        _assert_settings(
            _consumer_settings(grpc_worker, index),
            retention=retention,
            max_state_bytes=None,
            high_watermark=0.9,
            low_watermark=0.6,
            response_delivery_window_seconds=20,
        )


_INVALID_SETTINGS: list[dict[str, Any]] = [
    {"retention": "auto"},
    {"retention": "invalid"},
    *({"max_state_bytes": value} for value in [0, -1, True, False, 1.5, "8192", "inherit"]),
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
@pytest.mark.parametrize("surface", ["host", "agent", "workflow"])
def test_invalid_settings_fail_before_registration(surface: str, settings: dict[str, Any]) -> None:
    grpc_worker = Mock()
    if surface == "host":
        with pytest.raises(ValueError):
            DurableAIAgentWorker(grpc_worker, **settings)
    else:
        worker = DurableAIAgentWorker(grpc_worker)
        with pytest.raises(ValueError):
            if surface == "agent":
                worker.add_agent(_agent(), **settings)
            else:
                worker.configure_workflow(_workflow("flow", _agent()), **settings)
        assert worker.registered_agent_names == []
        assert worker.registered_workflow_names == []
        assert worker._registered_orchestrations == {}
    assert grpc_worker.mock_calls == []


@pytest.mark.parametrize("surface", ["agent", "workflow", "nested_workflow"])
def test_ambiguous_history_fails_before_any_registration(surface: str) -> None:
    grpc_worker = Mock()
    worker = DurableAIAgentWorker(grpc_worker)
    agent = _agent(ambiguous_history=True)
    original_providers = agent.context_providers
    with pytest.raises(ValueError, match="primary"):
        if surface == "agent":
            worker.add_agent(agent)
        elif surface == "workflow":
            worker.configure_workflow(_workflow("flow", _agent("good"), agent))
        else:
            worker.configure_workflow(_workflow("outer", _agent("good"), child=_workflow("inner", agent)))

    assert agent.context_providers is original_providers
    assert all(isinstance(provider, InMemoryHistoryProvider) for provider in original_providers)
    assert worker.registered_agent_names == []
    assert worker.registered_workflow_names == []
    assert worker._registered_orchestrations == {}
    assert grpc_worker.mock_calls == []


def test_registration_validates_without_replacing_the_users_history_provider() -> None:
    grpc_worker = Mock()
    worker = DurableAIAgentWorker(grpc_worker, retention="follow_compaction")
    agent = _agent()
    original_providers = agent.context_providers
    with patch("agent_framework_durabletask._worker.AgentEntity") as consumer:
        worker.add_agent(agent)
    consumer.assert_not_called()
    assert agent.context_providers is original_providers
    assert isinstance(agent.context_providers[0], InMemoryHistoryProvider)


def test_backend_registration_failure_does_not_record_an_agent() -> None:
    grpc_worker = Mock()
    grpc_worker.add_entity.side_effect = RuntimeError("registration failed")
    worker = DurableAIAgentWorker(grpc_worker)
    with pytest.raises(RuntimeError, match="registration failed"):
        worker.add_agent(_agent())
    assert worker.registered_agent_names == []
