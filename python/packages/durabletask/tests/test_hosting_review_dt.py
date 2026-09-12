# Copyright (c) Microsoft. All rights reserved.

"""Derived-name ownership and registration failure boundaries for the worker host."""

from collections.abc import Callable
from dataclasses import fields
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import Agent, AgentExecutor, Executor, InMemoryHistoryProvider, WorkflowExecutor
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker
from durabletask.worker import TaskHubGrpcWorker, _Registry

from agent_framework_durabletask import DTS_MAX_STATE_BYTES, DurableAIAgentWorker, DurableHistoryProvider
from agent_framework_durabletask._configuration import AgentRegistrationSettings, validate_agent_configuration


class RecordingWorker:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.entities: dict[str, Any] = {}
        self.fail_at: int | None = None

    def _record(self, kind: str, name: str) -> str:
        self.calls.append((kind, name))
        if len(self.calls) == self.fail_at:
            raise RuntimeError("injected native registration failure")
        return name

    def add_entity(self, entity: Any) -> str:
        self.entities[entity.__name__] = entity
        return self._record("entity", entity.__name__)

    def add_activity(self, activity: Callable[..., Any]) -> str:
        return self._record("activity", activity.__name__)

    def add_orchestrator(self, orchestrator: Callable[..., Any]) -> str:
        return self._record("orchestration", orchestrator.__name__)

    def start(self) -> None:
        self._record("start", "worker")


def _worker(native: Any, **kwargs: Any) -> DurableAIAgentWorker:
    return DurableAIAgentWorker(native, **kwargs)


def _agent(name: str = "assistant") -> Agent:
    client: Any = Mock(additional_properties={}, STORES_BY_DEFAULT=False)
    return Agent(client=client, name=name, context_providers=[InMemoryHistoryProvider("primary")])


def _workflow(name: str, executor_id: str = "node", *, agent: Any = None, children: tuple[Any, ...] = ()) -> Any:
    executor = Mock(spec=Executor if agent is None else AgentExecutor)
    executor.id = executor_id
    if agent is not None:
        executor.agent = agent
    executors = {executor_id: executor}
    for index, child in enumerate(children):
        nested = Mock(spec=WorkflowExecutor)
        nested.id = f"child{index}"
        nested.workflow = child
        executors[nested.id] = nested
    workflow = Mock()
    workflow.name = name
    workflow.executors = executors
    return workflow


@pytest.mark.parametrize("kinds", [(False, False), (True, True)])
@pytest.mark.parametrize("nested", [False, True])
def test_ambiguous_concatenations_fail_before_backend_or_metadata_changes(
    kinds: tuple[bool, bool], nested: bool
) -> None:
    native = RecordingWorker()
    host = _worker(native)
    left = _workflow("alpha-beta", "gamma", agent=_agent("left") if kinds[0] else None)
    right = _workflow("alpha", "beta-gamma", agent=_agent("right") if kinds[1] else None)
    if nested:
        candidate = _workflow("root", children=(left, right))
    else:
        host.configure_workflow(left)
        candidate = right
    calls = list(native.calls)
    agents, workflows = host.registered_agent_names, host.registered_workflow_names
    with pytest.raises(ValueError, match="Derived name.*collides"):
        host.configure_workflow(candidate)
    assert native.calls == calls
    assert host.registered_agent_names == agents
    assert host.registered_workflow_names == workflows
    host.configure_workflow(_workflow("corrected"))


@pytest.mark.parametrize("standalone_first", [False, True])
@pytest.mark.parametrize("same_agent", [False, True])
def test_standalone_and_workflow_owners_cannot_share_an_entity(standalone_first: bool, same_agent: bool) -> None:
    native = RecordingWorker()
    host = _worker(native)
    standalone = _agent("alpha-beta-node")
    workflow = _workflow("alpha-beta", agent=standalone if same_agent else _agent())
    if standalone_first:
        host.add_agent(standalone)
    else:
        host.configure_workflow(workflow)
    calls = list(native.calls)
    with pytest.raises(ValueError):
        if standalone_first:
            host.configure_workflow(workflow)
        else:
            host.add_agent(standalone)
    assert native.calls == calls


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("nested", [False, True])
def test_native_registry_allows_the_same_name_for_different_artifact_kinds(reverse: bool, nested: bool) -> None:
    registry = _Registry()
    native = Mock(spec=TaskHubGrpcWorker)
    native.add_entity.side_effect = registry.add_entity
    native.add_activity.side_effect = registry.add_activity
    native.add_orchestrator.side_effect = registry.add_orchestrator
    host = _worker(native)
    workflows = [
        _workflow("alpha-beta", "gamma", agent=_agent()),
        _workflow("alpha", "beta-gamma"),
        _workflow("alpha-beta-gamma"),
    ]
    if reverse:
        workflows.reverse()
    if nested:
        host.configure_workflow(_workflow("root", children=tuple(workflows)))
    else:
        for workflow in workflows:
            host.configure_workflow(workflow)

    name = "dafx-alpha-beta-gamma"
    assert name in registry.entities
    assert name in registry.activities
    assert name in registry.orchestrators
    assert {namespace for namespace, registered_name in host._registration_identities if registered_name == name} == {
        "entity-name",
        "activity-name",
        "orchestrator-name",
    }
    assert set(host.registered_workflow_names) == ({"root"} if nested else {workflow.name for workflow in workflows})


def test_case_folded_agent_identity_is_checked_before_native_registration() -> None:
    native = RecordingWorker()
    host = _worker(native)
    host.add_agent(_agent("Assistant"))
    with pytest.raises(ValueError, match="case-insensitively"):
        host.add_agent(_agent("assistant"))
    assert native.calls == [("entity", "dafx-Assistant")]


_CHANGED_SETTINGS: dict[str, Any] = {
    "retention": "follow_compaction",
    "max_state_bytes": 8192,
    "high_watermark": 0.99,
    "low_watermark": 0.1,
    "response_delivery_window_seconds": 17,
    "callback": Mock(),
}


def test_shared_configuration_cases_cover_every_setting_field() -> None:
    assert set(_CHANGED_SETTINGS) == {field.name for field in fields(AgentRegistrationSettings)}


@pytest.mark.parametrize("setting", _CHANGED_SETTINGS)
def test_shared_workflow_reuse_requires_identical_resolved_settings(setting: str) -> None:
    native = RecordingWorker()
    host = _worker(native)
    child = _workflow("shared", agent=_agent())
    host.configure_workflow(_workflow("first", children=(child,)))
    calls = list(native.calls)
    with pytest.raises(ValueError, match="different settings"):
        host.configure_workflow(_workflow("second", children=(child,)), **{setting: _CHANGED_SETTINGS[setting]})
    assert native.calls == calls
    assert host.registered_workflow_names == ["first"]
    host.configure_workflow(_workflow("second", children=(child,)))
    assert native.calls.count(("entity", "dafx-shared-node")) == 1


def test_repeated_identical_workflow_is_benign_and_names_are_unchanged() -> None:
    native = RecordingWorker()
    host = _worker(native)
    workflow = _workflow("Orders", "review", agent=_agent())
    host.configure_workflow(workflow)
    host.configure_workflow(workflow)
    assert native.calls == [("entity", "dafx-Orders-review"), ("orchestration", "dafx-Orders")]


class UncopyableAgent:
    name = "uncopyable"
    context_providers = [InMemoryHistoryProvider("history")]

    def __copy__(self) -> Any:
        raise TypeError("cannot copy")


class ReadOnlyProviders:
    name = "readonly"

    @property
    def context_providers(self) -> list[Any]:
        return [InMemoryHistoryProvider("history")]


@pytest.mark.parametrize("factory", [UncopyableAgent, ReadOnlyProviders])
@pytest.mark.parametrize("surface", ["agent", "nested"])
def test_actual_adapter_preparation_fails_during_registration(factory: Any, surface: str) -> None:
    native = RecordingWorker()
    host = _worker(native)
    agent = factory()
    with pytest.raises(ValueError, match="attach durable history"):
        if surface == "agent":
            host.add_agent(agent)
        else:
            host.configure_workflow(_workflow("root", agent=_agent(), children=(_workflow("child", agent=agent),)))
    assert native.calls == []
    assert host.registered_agent_names == []
    assert host.registered_workflow_names == []
    host.add_agent(_agent("valid"))


def test_dry_preparation_preserves_original_agent_and_provider_configuration() -> None:
    native = RecordingWorker()
    host = _worker(native, retention="follow_compaction")
    agent = _agent()
    providers = agent.context_providers
    provider = providers[0]
    validate_agent_configuration(agent, retention="follow_compaction")
    host.add_agent(agent)
    assert host._registered_agents["assistant"] is agent
    assert agent.context_providers is providers and providers[0] is provider
    assert isinstance(provider, InMemoryHistoryProvider)


def test_uncopyable_unresolved_durable_provider_fails_at_registration() -> None:
    class UncopyableHistory(DurableHistoryProvider):
        def __copy__(self) -> Any:
            raise TypeError("provider cannot copy")

    native = RecordingWorker()
    agent = _agent()
    provider = UncopyableHistory()
    agent.context_providers = [provider]
    with pytest.raises(ValueError, match="prepare.*durable history"):
        _worker(native).add_agent(agent, retention="follow_compaction")
    assert native.calls == []
    assert agent.context_providers == [provider] and provider.prune_excluded is None


def test_standalone_native_failure_also_blocks_reuse_and_start() -> None:
    native = RecordingWorker()
    native.fail_at = 1
    host = _worker(native)
    with pytest.raises(RuntimeError, match="injected native"):
        host.add_agent(_agent())
    assert host.registered_agent_names == []
    for action in (lambda: host.add_agent(_agent()), host.start):
        with pytest.raises(RuntimeError, match="partially registered"):
            action()
    assert len(native.calls) == 1


@pytest.mark.parametrize("fail_at", [1, 2, 3])
def test_partial_native_failure_blocks_start_and_retry_without_false_metadata(fail_at: int) -> None:
    native = RecordingWorker()
    host = _worker(native)
    host.add_agent(_agent("existing"))
    previous_calls = len(native.calls)
    native.fail_at = previous_calls + fail_at
    workflow = _workflow("root", agent=_agent(), children=(_workflow("child"),))
    with pytest.raises(RuntimeError, match="injected native"):
        host.configure_workflow(workflow)
    assert host.registered_agent_names == ["existing"]
    assert host.registered_workflow_names == []
    assert host._registered_orchestrations == {}
    calls = list(native.calls)
    for action in (lambda: host.add_agent(_agent("retry")), lambda: host.configure_workflow(workflow), host.start):
        with pytest.raises(RuntimeError, match="partially registered"):
            action()
    assert native.calls == calls


@pytest.mark.parametrize("surface", ["host", "agent", "workflow"])
def test_generic_grpc_worker_does_not_imply_a_dts_budget(surface: str) -> None:
    native = Mock(spec=TaskHubGrpcWorker)
    with pytest.raises(ValueError, match="known backend_limit"):
        if surface == "host":
            _worker(native, max_state_bytes="backend_limit")
        else:
            host = _worker(native)
            if surface == "agent":
                host.add_agent(_agent(), max_state_bytes="backend_limit")
            else:
                host.configure_workflow(_workflow("flow", agent=_agent()), max_state_bytes="backend_limit")
    assert native.mock_calls == []


@pytest.mark.parametrize("surface", ["host", "agent", "workflow"])
def test_dts_worker_resolves_backend_budget_on_every_surface(surface: str) -> None:
    native = Mock(spec=DurableTaskSchedulerWorker)
    host = _worker(native, **({"max_state_bytes": "backend_limit"} if surface == "host" else {}))
    if surface == "workflow":
        host.configure_workflow(_workflow("flow", agent=_agent()), max_state_bytes="backend_limit")
    elif surface == "agent":
        host.add_agent(_agent(), max_state_bytes="backend_limit")
    else:
        host.add_agent(_agent())
    entity = native.add_entity.call_args.args[0]()
    assert entity._agent_entity._max_state_bytes == DTS_MAX_STATE_BYTES


def test_explicit_budget_works_with_an_unknown_backend() -> None:
    native = RecordingWorker()
    host = _worker(native, max_state_bytes=8192)
    host.add_agent(_agent())
    entity = native.entities["dafx-assistant"]()
    assert entity._agent_entity._max_state_bytes == 8192
