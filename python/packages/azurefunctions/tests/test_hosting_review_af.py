# Copyright (c) Microsoft. All rights reserved.

"""Functions hosting preflight, fail-closed registration, and response classification."""

import json
from collections.abc import Awaitable, Callable
from copy import deepcopy
from dataclasses import fields
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import azure.durable_functions as df
import azure.functions as func
import pytest
from agent_framework import (
    Agent,
    AgentExecutor,
    AgentResponse,
    Content,
    Executor,
    InMemoryHistoryProvider,
    Message,
    WorkflowExecutor,
)
from agent_framework_durabletask import DurableAgentState, ensure_response_format, load_agent_response
from agent_framework_durabletask._configuration import AgentRegistrationSettings
from pydantic import BaseModel

from agent_framework_azurefunctions import AgentFunctionApp
from agent_framework_azurefunctions._entities import AzureFunctionEntityStateProvider, create_agent_entity


class RecordingApp(AgentFunctionApp):
    def __init__(self, *, calls: list[tuple[str, str]] | None = None, **kwargs: Any) -> None:
        self.calls = calls if calls is not None else []
        self.fail_at: int | None = None
        super().__init__(enable_health_check=False, **kwargs)

    def _record(self, kind: str, name: str) -> None:
        self.calls.append((kind, name))
        if len(self.calls) == self.fail_at:
            raise RuntimeError("injected trigger registration failure")

    def _setup_agent_functions(
        self,
        agent: Any,
        agent_name: str,
        callback: Any,
        enable_http_endpoint: bool,
        enable_mcp_tool_trigger: bool,
        **kwargs: Any,
    ) -> None:
        create_agent_entity(agent, callback, **kwargs)
        self._record("entity", f"dafx-{agent_name}")

    def _setup_executor_activity(self, workflow: Any, executor_id: str) -> None:
        self._record("activity", f"dafx-{workflow.name}-{executor_id}")

    def _setup_workflow_orchestration(self, workflow: Any) -> None:
        self._record("orchestration", f"dafx-{workflow.name}")

    def _register_workflow_routes(self, workflow: Any) -> None:
        self._record("routes", workflow.name)


def _agent(name: str = "assistant") -> Agent:
    client: Any = Mock(additional_properties={}, STORES_BY_DEFAULT=False)
    return Agent(client=client, name=name, context_providers=[InMemoryHistoryProvider("history")])


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


@pytest.mark.parametrize("surface", ["constructor", "nested", "later"])
@pytest.mark.parametrize("kinds", [(False, False), (False, True), (True, False), (True, True)])
def test_ambiguous_derived_names_are_preflighted_for_the_entire_composition(
    surface: str, kinds: tuple[bool, bool]
) -> None:
    left = _workflow("alpha-beta", "gamma", agent=_agent("left") if kinds[0] else None)
    right = _workflow("alpha", "beta-gamma", agent=_agent("right") if kinds[1] else None)
    calls: list[tuple[str, str]] = []
    if surface == "constructor":
        with pytest.raises(ValueError, match="Derived name.*collides"):
            RecordingApp(calls=calls, workflows=[left, right])
        assert calls == []
        return
    app = RecordingApp(calls=calls)
    if surface == "later":
        app.configure_workflow(left)
        candidate = right
    else:
        candidate = _workflow("root", children=(left, right))
    before = list(calls)
    agents, workflows = app.agents, app.workflows
    with pytest.raises(ValueError, match="Derived name.*collides"):
        app.configure_workflow(candidate)
    assert calls == before
    assert app.agents == agents and app.workflows == workflows
    app.configure_workflow(_workflow("corrected"))


@pytest.mark.parametrize("surface", ["constructor", "standalone_first", "workflow_first"])
@pytest.mark.parametrize("same_agent", [False, True])
def test_standalone_agent_cannot_occupy_a_workflow_owned_identity(surface: str, same_agent: bool) -> None:
    standalone = _agent("flow-node")
    workflow = _workflow("flow", agent=standalone if same_agent else _agent())
    calls: list[tuple[str, str]] = []
    if surface == "constructor":
        with pytest.raises(ValueError, match="collides"):
            RecordingApp(calls=calls, agents=[standalone], workflow=workflow)
        assert calls == []
        return
    app = RecordingApp(calls=calls)
    if surface == "standalone_first":
        app.add_agent(standalone)
    else:
        app.configure_workflow(workflow)
    before = list(calls)
    with pytest.raises(ValueError, match="collides"):
        if surface == "standalone_first":
            app.configure_workflow(workflow)
        else:
            app.add_agent(standalone)
    assert calls == before


def test_different_agent_with_same_name_is_not_silently_skipped() -> None:
    first = _agent()
    app = RecordingApp(agents=[first])
    calls = list(app.calls)
    with pytest.raises(ValueError, match="collides"):
        app.add_agent(_agent())
    app.add_agent(first)
    assert app.calls == calls
    assert app.agents == {"assistant": first}


def test_constructor_rejects_different_agents_with_duplicate_names_before_any_setup() -> None:
    calls: list[tuple[str, str]] = []
    with pytest.raises(ValueError, match="collides"):
        RecordingApp(calls=calls, agents=[_agent(), _agent()])
    assert calls == []


def test_case_only_agent_names_fail_before_setup() -> None:
    app = RecordingApp(agents=[_agent("Assistant")])
    calls = list(app.calls)
    with pytest.raises(ValueError, match="case-insensitively"):
        app.add_agent(_agent("assistant"))
    assert app.calls == calls


_CHANGED_SETTINGS: dict[str, Any] = {
    "retention": "follow_compaction",
    "max_state_bytes": 8192,
    "high_watermark": 0.99,
    "low_watermark": 0.1,
    "response_delivery_window_seconds": 17,
    "callback": Mock(),
}


def test_changed_settings_cover_every_shared_configuration_field() -> None:
    assert set(_CHANGED_SETTINGS) == {field.name for field in fields(AgentRegistrationSettings)}


@pytest.mark.parametrize("setting", [*_CHANGED_SETTINGS, "enable_http_endpoint", "enable_mcp_tool_trigger"])
def test_same_agent_with_different_configuration_is_rejected(setting: str) -> None:
    agent = _agent()
    app = RecordingApp(agents=[agent])
    changed = {**_CHANGED_SETTINGS, "enable_http_endpoint": False, "enable_mcp_tool_trigger": True}
    calls = list(app.calls)
    with pytest.raises(ValueError, match="different settings"):
        app.add_agent(agent, **{setting: changed[setting]})
    assert app.calls == calls and app.agents["assistant"] is agent


@pytest.mark.parametrize("setting", _CHANGED_SETTINGS)
def test_shared_child_requires_identical_workflow_configuration(setting: str) -> None:
    child = _workflow("shared", agent=_agent())
    app = RecordingApp(workflow=_workflow("first", children=(child,)))
    calls = list(app.calls)
    if setting == "callback":
        app.default_callback = _CHANGED_SETTINGS[setting]
        overrides: dict[str, Any] = {}
    else:
        overrides = {setting: _CHANGED_SETTINGS[setting]}
    with pytest.raises(ValueError, match="different settings"):
        app.configure_workflow(_workflow("second", children=(child,)), **overrides)
    assert app.calls == calls and list(app.workflows) == ["first"]


def test_shared_workflow_and_explicit_original_agent_remain_benign() -> None:
    agent = _agent()
    providers = agent.context_providers
    child = _workflow("shared", agent=agent)
    first = _workflow("first", children=(child,))
    app = RecordingApp(agents=[agent, agent], workflows=[first, first, _workflow("second", children=(child,))])
    assert app.calls.count(("entity", "dafx-shared-node")) == 1
    assert app.calls.count(("routes", "first")) == 1
    assert app.agents["assistant"] is agent and app.agents["shared-node"] is agent
    assert agent.context_providers is providers
    assert isinstance(providers[0], InMemoryHistoryProvider)


@pytest.mark.parametrize("endpoint", ["http", "mcp"])
def test_sanitized_endpoint_names_are_also_preflighted(endpoint: str) -> None:
    calls: list[tuple[str, str]] = []
    with pytest.raises(ValueError, match="Derived name.*collides"):
        RecordingApp(
            calls=calls,
            agents=[_agent("alpha-beta"), _agent("alpha_beta")],
            enable_http_endpoints=endpoint == "http",
            enable_mcp_tool_trigger=endpoint == "mcp",
        )
    assert calls == []


@pytest.mark.parametrize("suffix", ["start", "status", "respond"])
@pytest.mark.parametrize("agent_executor", [False, True])
@pytest.mark.parametrize("uppercase", [False, True])
@pytest.mark.parametrize("surface", ["constructor", "later"])
def test_workflow_route_suffixes_remain_valid_executor_ids_with_real_triggers(
    suffix: str, agent_executor: bool, uppercase: bool, surface: str
) -> None:
    executor_id = suffix.upper() if uppercase else suffix
    workflow = _workflow("input_boundary", executor_id, agent=_agent() if agent_executor else None)
    app = AgentFunctionApp(
        workflow=workflow if surface == "constructor" else None,
        enable_health_check=False,
        enable_http_endpoints=False,
    )
    if surface == "later":
        app.configure_workflow(workflow)
    functions = app.get_functions()
    names = [function.get_function_name() for function in functions]
    durable_name = f"dafx-input_boundary-{executor_id}"
    assert len(names) == len(set(names)) == 5
    assert durable_name in names
    assert f"http-dafx-input_boundary-{suffix}" in names
    assert "dafx-input_boundary" in names
    routes = {}
    for function in functions:
        trigger = function.get_trigger()
        assert trigger is not None
        binding = trigger.get_dict_repr()
        if binding["type"] == "httpTrigger":
            routes[binding["route"]] = function.get_function_name()
        if function.get_function_name() == durable_name:
            assert binding["type"] == ("entityTrigger" if agent_executor else "activityTrigger")
    assert set(routes) == {
        "workflow/input_boundary/run",
        "workflow/input_boundary/status/{instanceId}",
        "workflow/input_boundary/respond/{instanceId}/{requestId}",
    }
    assert set(routes.values()) == {
        f"{'http-' if route_suffix == suffix else ''}dafx-input_boundary-{route_suffix}"
        for route_suffix in ("start", "status", "respond")
    }
    namespace = "entity-name" if agent_executor else "activity-name"
    assert (namespace, durable_name.casefold()) in app._registration_identities
    assert ("function-name", durable_name.casefold()) in app._registration_identities


@pytest.mark.parametrize("agent_executor", [False, True])
@pytest.mark.parametrize("workflow_first", [False, True])
def test_real_native_function_collisions_still_fail_before_setup(agent_executor: bool, workflow_first: bool) -> None:
    app = AgentFunctionApp(enable_health_check=False, enable_http_endpoints=False)
    first = _workflow("alpha", "beta", agent=_agent() if agent_executor else None)
    second = _workflow("alpha-beta")
    if workflow_first:
        first, second = second, first
    app.configure_workflow(first)
    identities = dict(app._registration_identities)
    agents = app.agents
    with pytest.raises(ValueError, match="Derived name.*collides"):
        app.configure_workflow(second)
    assert app._registration_identities == identities
    assert app.agents == agents and app.workflows == {first.name: first}
    assert app._registered_orchestrations == {first.name.casefold(): first}
    assert len(app.get_functions()) == 5


def test_logical_agent_name_can_match_an_http_function_name() -> None:
    app = AgentFunctionApp(agents=[_agent("Assistant"), _agent("http-Assistant")], enable_health_check=False)
    names = [function.get_function_name() for function in app.get_functions()]
    assert set(names) == {"dafx-Assistant", "http-Assistant", "dafx-http-Assistant", "http-http_Assistant"}
    assert len(names) == 4


def test_cross_workflow_route_function_collision_is_still_preflighted() -> None:
    calls: list[tuple[str, str]] = []
    with pytest.raises(ValueError, match="collides"):
        RecordingApp(calls=calls, workflows=[_workflow("flow"), _workflow("flow-start")])
    assert calls == []


def test_real_trigger_names_keep_deployment_compatibility() -> None:
    app = AgentFunctionApp(
        enable_health_check=False,
        agents=[_agent("Assistant")],
        workflow=_workflow("Orders", "review", agent=_agent("reviewer")),
    )
    names = {function.get_function_name() for function in app.get_functions()}
    assert names == {
        "dafx-Assistant",
        "http-Assistant",
        "dafx-Orders-review",
        "http-Orders_review",
        "dafx-Orders",
        "dafx-Orders-start",
        "dafx-Orders-status",
        "dafx-Orders-respond",
    }


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
@pytest.mark.parametrize("surface", ["constructor", "agent", "workflow", "factory"])
def test_adapter_copy_and_attachment_fail_before_any_triggers(factory: Any, surface: str) -> None:
    agent = factory()
    calls: list[tuple[str, str]] = []
    app = RecordingApp(calls=calls)
    with pytest.raises(ValueError, match="attach durable history"):
        if surface == "constructor":
            RecordingApp(calls=calls, agents=[_agent(), agent])
        elif surface == "agent":
            app.add_agent(agent)
        elif surface == "workflow":
            app.configure_workflow(_workflow("outer", agent=_agent(), children=(_workflow("inner", agent=agent),)))
        else:
            create_agent_entity(agent)
    assert calls == [] and app.agents == {} and app.workflows == {}


@pytest.mark.parametrize("fail_at", [1, 2, 3, 4, 5])
def test_backend_failure_prevents_retry_and_function_indexing(fail_at: int) -> None:
    app = RecordingApp(agents=[_agent("existing")])
    app.fail_at = len(app.calls) + fail_at
    workflow = _workflow("root", agent=_agent(), children=(_workflow("child"),))
    with pytest.raises(RuntimeError, match="injected trigger"):
        app.configure_workflow(workflow)
    calls = list(app.calls)
    assert list(app.agents) == ["existing"]
    assert app.workflows == {} and app._registered_orchestrations == {}
    assert app.workflow is None
    for action in (lambda: app.add_agent(_agent("retry")), lambda: app.configure_workflow(workflow), app.get_functions):
        with pytest.raises(RuntimeError, match="partially registered"):
            action()
    assert app.calls == calls


def test_standalone_setup_failure_also_blocks_indexing_and_retry() -> None:
    app = RecordingApp()
    app.fail_at = 1
    with pytest.raises(RuntimeError, match="injected trigger"):
        app.add_agent(_agent())
    assert app.agents == {}
    for action in (lambda: app.add_agent(_agent()), app.get_functions):
        with pytest.raises(RuntimeError, match="partially registered"):
            action()
    assert len(app.calls) == 1


def _provider(raw_state: Any) -> tuple[AzureFunctionEntityStateProvider, Mock]:
    context = Mock(spec=df.DurableEntityContext)
    context.get_state.return_value = raw_state
    return AzureFunctionEntityStateProvider(context), context


@pytest.mark.parametrize("raw", [[], ["state"], "state", 0, False, 2.5])
def test_non_dictionary_existing_state_is_rejected_not_replaced(raw: Any) -> None:
    provider, context = _provider(raw)
    with pytest.raises(ValueError, match="Existing durable entity state"):
        _ = provider.state
    context.set_state.assert_not_called()


@pytest.mark.parametrize("raw", [None, {}])
def test_absent_state_still_initializes(raw: Any) -> None:
    provider, context = _provider(raw)
    assert provider.state.message_count == 0
    context.set_state.assert_not_called()


def test_future_state_fields_survive_adapter_read_and_write() -> None:
    raw: dict[str, Any] = {
        "schemaVersion": "2.0.0",
        "futureEnvelope": {"opaque": [1, {"nested": True}]},
        "data": {"conversationHistory": [], "futureSidecar": {"records": [{"version": 9}]}},
    }
    before = deepcopy(raw)
    provider, context = _provider(raw)
    assert provider._get_state_dict() is raw
    _ = provider.state
    provider.persist_state()
    saved = context.set_state.call_args.args[0]
    assert saved["futureEnvelope"] == raw["futureEnvelope"]
    assert saved["data"]["futureSidecar"] == raw["data"]["futureSidecar"]
    assert raw == before


class Answer(BaseModel):
    answer: int


HttpHandler = Callable[[func.HttpRequest, Any], Awaitable[func.HttpResponse]]


def _http_handler(app: AgentFunctionApp, monkeypatch: pytest.MonkeyPatch) -> HttpHandler:
    handlers: list[HttpHandler] = []

    def identity(*args: Any, **kwargs: Any) -> Callable[[HttpHandler], HttpHandler]:
        return lambda handler: handler

    def route(*args: Any, **kwargs: Any) -> Callable[[HttpHandler], HttpHandler]:
        def capture(handler: HttpHandler) -> HttpHandler:
            handlers.append(handler)
            return handler

        return capture

    monkeypatch.setattr(app, "function_name", identity)
    monkeypatch.setattr(app, "route", route)
    monkeypatch.setattr(app, "durable_client_input", identity)
    monkeypatch.setattr(app, "_generate_unique_id", lambda: "correlation")
    monkeypatch.setattr("agent_framework_azurefunctions._app.asyncio.sleep", AsyncMock())
    app._setup_http_run_route("assistant")
    return handlers[0]


@pytest.mark.parametrize("kind,expected", [("recovered_tool", 200), ("explicit_error", 500), ("direct_error", 500)])
async def test_http_uses_shared_terminal_classification_and_canonical_delivery(
    monkeypatch: pytest.MonkeyPatch, kind: str, expected: int
) -> None:
    app = AgentFunctionApp(enable_health_check=False, enable_http_endpoints=False, max_poll_retries=1)
    handler = _http_handler(app, monkeypatch)
    messages = [
        Message("tool", [Content.from_error(message="recovered tool failure", error_code="response_expired")]),
        Message("assistant", [Content.from_text('{"answer":42}')]),
    ]
    properties = {"durable_status": "error"} if kind == "explicit_error" else {}
    if kind == "direct_error":
        messages.append(Message("system", [Content.from_error(message="runtime failure", error_code="runtime")]))
    original: AgentResponse[Any] = AgentResponse(
        messages=messages, value=Answer(answer=42), additional_properties=properties
    )
    state = DurableAgentState()
    state.record_response("correlation", original, delivery_window_seconds=3600)
    stored = json.loads(state.to_json())
    before = deepcopy(stored)
    client = Mock(spec=df.DurableOrchestrationClient)
    client.signal_entity = AsyncMock()
    client.read_entity_state = AsyncMock(return_value=SimpleNamespace(entity_exists=True, entity_state=stored))
    request = func.HttpRequest(
        method="POST",
        url="https://example.test/api/agents/assistant/run",
        headers={"Content-Type": "application/json"},
        body=b'{"message":"question","session_id":"session"}',
    )

    response = await handler(request, client)

    assert response.status_code == expected
    result = json.loads(response.get_body())
    assert result["status"] == ("success" if expected == 200 else "error")
    assert result["agent_response"]["type"] == "agent_response"
    assert result["agent_response"] == stored["data"]["responseMailbox"]["correlation"]["response"]
    assert stored == before
    delivered = load_agent_response(result["agent_response"])
    assert type(delivered) is AgentResponse
    assert delivered.to_dict() == original.to_dict()
    if expected == 200:
        ensure_response_format(Answer, "correlation", delivered)
        assert delivered.value == Answer(answer=42)
        assert result["response"] == original.text
        assert result["message_count"] == 0 and result["message"] == "question"
        assert result["session_id"] == "session" and result["correlation_id"] == "correlation"
    else:
        assert result["response"] is None
        assert result["error_code"] != "response_expired"
    client.read_entity_state.assert_awaited_once()
