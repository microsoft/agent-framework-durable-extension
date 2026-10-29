# Copyright (c) Microsoft. All rights reserved.

"""Generated Functions handlers share in-place schema admission and address rules."""

import json
from copy import deepcopy
from typing import Any
from unittest.mock import AsyncMock, Mock

import azure.functions as func
import pytest
from _af_handler_test_support import _Client, _Host, _NonStreamingAgent
from _migration_test_support import _migration_request
from agent_framework import AgentExecutor, AgentResponse, Message, WorkflowBuilder
from agent_framework_durabletask import (
    DurableAgentState,
    EntityMigrationContext,
    LegacyDurableAgentState,
    RunRequest,
    agent_entity_schema_registry,
    wrap_workflow_input,
)
from agent_framework_durabletask._durable_agent_state import DurableAgentStateRequest as LegacyRequest
from agent_framework_durabletask._durable_agent_state import DurableAgentStateResponse as LegacyResponse

from agent_framework_azurefunctions import AgentFunctionApp


def _app(
    *, legacy: bool = True, max_state_bytes: int | None = 1_048_576, evidence: Any = None
) -> tuple[AgentFunctionApp, _Client]:
    model = _Client({"value": "original"})
    app = AgentFunctionApp(
        agents=[_NonStreamingAgent(client=model, name="assistant")],
        deployment_mode="two_phase",
        acknowledge_host_filtering=True,
        process_legacy_entities=legacy,
        max_state_bytes=max_state_bytes,
        legacy_evidence_provider=evidence,
        enable_health_check=False,
        enable_http_endpoints=False,
    )
    return app, model


def test_functions_requires_explicit_host_filtering_configuration_acknowledgement() -> None:
    with pytest.raises(ValueError, match="host.json.*workItemFilteringEnabled=true"):
        AgentFunctionApp(deployment_mode="two_phase")
    app, _ = _app(legacy=False)
    assert app._deployment_mode == "two_phase"
    functions = {
        function.get_function_name()
        for function in app.get_functions()
        if (trigger := function.get_trigger()) is not None and trigger.get_binding_name() == "entityTrigger"
    }
    assert functions == {"dafx-v2-assistant"}
    phase_two, _ = _app()
    assert {
        function.get_function_name()
        for function in phase_two.get_functions()
        if (trigger := function.get_trigger()) is not None and trigger.get_binding_name() == "entityTrigger"
    } == {
        "dafx-v2-assistant",
        "dafx-assistant",
    }


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
def test_functions_handler_upgrades_original_entity_and_cold_retry_is_noop(version: str) -> None:
    source = LegacyDurableAgentState().to_dict()
    source["schemaVersion"] = version
    app, model = _app()
    host = _Host(app)
    request = json.dumps({"message": "hello", "correlationId": "turn-1"})
    result = host.entity("@dafx-assistant@original-key", "run", request, json.dumps(source))
    assert result.results[0].HasField("success"), result
    upgraded = json.loads(result.entityState.value)
    assert upgraded["schemaVersion"] == upgraded["dafxSchema"]["version"] == "2.0.0"
    assert upgraded["data"]["session"]["session_id"] == "@dafx-assistant@original-key"
    assert "migration" not in upgraded["data"]
    assert len(model.options) == 1
    restarted, cold_model = _app()
    retry = _Host(restarted).entity("@dafx-assistant@original-key", "run", request, result.entityState.value)
    assert retry.results[0].success.result == result.results[0].success.result
    assert json.loads(retry.entityState.value) == upgraded
    assert cold_model.options == []


@pytest.mark.parametrize("evidence", [[], {"completion_evidence": None}, {"delivery_evidence": None}])
@pytest.mark.parametrize("populated", [False, True])
def test_functions_supplied_malformed_evidence_rejects_before_model_and_preserves_state(
    evidence: Any, populated: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    legacy = LegacyDurableAgentState()
    if populated:
        legacy.data.conversation_history.append(
            LegacyRequest.from_run_request(RunRequest(message="old request", correlation_id="old-turn"))
        )
    source = legacy.to_dict()
    before = deepcopy(source)
    app, model = _app(evidence=lambda *_: evidence)
    decode = Mock(side_effect=AssertionError("must not decode rejected input"))
    monkeypatch.setattr("agent_framework_durabletask._entities.RunRequest.from_dict", decode)
    result = _Host(app).entity(
        "@dafx-assistant@original-key",
        "run",
        json.dumps({"message": "hello", "correlationId": "turn-1"}),
        json.dumps(source),
    )
    assert result.results[0].HasField("failure"), result
    assert json.loads(result.entityState.value) == before == source
    decode.assert_not_called()
    assert model.options == []


@pytest.mark.parametrize("headroom", [-1, 0, 1])
def test_functions_reset_budget_includes_the_committed_schema_marker(headroom: int) -> None:
    source = DurableAgentState().to_dict()
    expected = deepcopy(source)
    expected["dafxSchema"] = {"contract": "dafx.agent-state", "version": "2.0.0"}
    size = len(json.dumps(expected, allow_nan=False).encode("ascii"))
    app, model = _app(max_state_bytes=size + headroom)
    result = _Host(app).entity("@dafx-v2-assistant@original-key", "reset", None, json.dumps(source))
    if headroom < 0:
        assert result.results[0].HasField("failure"), result
        assert json.loads(result.entityState.value) == source
    else:
        assert result.results[0].HasField("success"), result
        assert json.loads(result.entityState.value) == expected
    assert model.options == []


def test_functions_export_import_accepts_reset_destination_and_preserves_cold_retry_identity() -> None:
    app, model = _app()
    host = _Host(app)
    destination = "@dafx-v2-assistant@original-key"
    reset = host.entity(destination, "reset", None)
    assert reset.results[0].HasField("success"), reset
    assert json.loads(reset.entityState.value)["dafxSchema"] == {"contract": "dafx.agent-state", "version": "2.0.0"}
    request = _migration_request(LegacyDurableAgentState().to_dict(), destination)
    result = host.entity(destination, "migrate", json.dumps(request), reset.entityState.value)
    assert result.results[0].HasField("success"), result
    committed = json.loads(result.entityState.value)
    assert committed["data"]["migration"]["destinationSessionId"] == destination
    restarted, cold_model = _app()
    cold_host = _Host(restarted)
    duplicate = cold_host.entity(destination, "migrate", json.dumps(request), result.entityState.value)
    assert duplicate.results[0].success.result == result.results[0].success.result
    assert json.loads(duplicate.entityState.value) == committed
    changed = {**request, "migrationId": "different-migration"}
    rejected = cold_host.entity(destination, "migrate", json.dumps(changed), result.entityState.value)
    assert rejected.results[0].HasField("failure"), rejected
    assert json.loads(rejected.entityState.value) == committed
    assert model.options == cold_model.options == []


@pytest.mark.parametrize("operation", ["run", "reset", "expire_responses", "delete"])
@pytest.mark.parametrize("invalid", ["future", "metadata", "ambiguous", "truncation", "session"])
def test_functions_handler_rejects_unsupported_state_without_changes_or_model_calls(
    operation: str, invalid: str
) -> None:
    source = DurableAgentState().to_dict()
    if invalid == "future":
        source["schemaVersion"] = "3.0.0"
    elif invalid == "metadata":
        source["dafxSchema"] = None
    else:
        source = LegacyDurableAgentState().to_dict()
        if invalid == "ambiguous":
            source["data"]["ingestedPositions"] = {"producer": 5}
        elif invalid == "truncation":
            source["data"]["truncation"] = {"evictedMessageCount": 1}
        else:
            source["data"]["session"] = {"session_id": "different", "state": {}}
    before = deepcopy(source)
    app, model = _app()
    result = _Host(app).entity(
        "@dafx-assistant@original-key",
        operation,
        json.dumps({"message": "hello", "correlationId": "turn-1"}) if operation == "run" else None,
        json.dumps(source),
    )
    assert result.results[0].HasField("failure"), result
    assert json.loads(result.entityState.value) == before
    assert model.options == []


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize("late_retry", [False, True])
@pytest.mark.parametrize("failed", [False, True])
def test_functions_populated_cutover_protects_new_attempts_and_keeps_old_history(
    monkeypatch: pytest.MonkeyPatch, version: str, late_retry: bool, failed: bool
) -> None:
    legacy = LegacyDurableAgentState()
    legacy.data.conversation_history.extend([
        LegacyRequest.from_run_request(RunRequest(message="old request", correlation_id="old-turn")),
        LegacyResponse.from_run_response("old-turn", AgentResponse(messages=[Message("assistant", ["old reply"])])),
    ])
    source = legacy.to_dict()
    source["schemaVersion"] = version
    candidate = agent_entity_schema_registry(process_legacy_entities=True).upgrade(
        source, EntityMigrationContext("dafx-assistant", "original-key")
    )
    assert candidate["data"]["conversationHistory"] == source["data"]["conversationHistory"]
    assert candidate["data"]["completionReceipts"] == candidate["data"]["terminalResults"] == {}
    app, model = _app()
    fail = AsyncMock(side_effect=RuntimeError("new invocation failed"))
    if failed:
        monkeypatch.setattr(model, "_inner_get_response", fail)
    correlation = "old-turn" if late_retry else "new-turn"
    request = json.dumps({"message": "hello", "correlationId": correlation})
    result = _Host(app).entity("@dafx-assistant@original-key", "run", request, json.dumps(source))
    assert result.results[0].HasField("success"), result
    upgraded = json.loads(result.entityState.value)
    retained = deepcopy(upgraded["data"]["conversationHistory"][:2])
    for entry in retained:
        for message in entry["messages"]:
            # Normal history processing allocates private replay identities.
            message.pop("pythonHistoryIdentity", None)
            message.pop("pythonHistoryId", None)
    assert retained == source["data"]["conversationHistory"]
    assert set(upgraded["data"]["completionReceipts"]) == {correlation}
    assert upgraded["data"]["completionReceipts"][correlation]["outcome"] == ("failed" if failed else "succeeded")
    if failed:
        fail.assert_awaited_once()
    else:
        assert len(model.options) == 1
    restarted, cold_model = _app()
    cold_host = _Host(restarted)
    duplicate = cold_host.entity("@dafx-assistant@original-key", "run", request, result.entityState.value)
    assert duplicate.results[0].success.result == result.results[0].success.result
    assert json.loads(duplicate.entityState.value) == upgraded
    assert cold_model.options == []
    if not late_retry:
        old_retry = cold_host.entity(
            "@dafx-assistant@original-key",
            "run",
            json.dumps({"message": "retry old", "correlationId": "old-turn"}),
            result.entityState.value,
        )
        assert old_retry.results[0].HasField("success"), old_retry
        assert len(cold_model.options) == 1


def test_functions_http_and_mcp_keep_existing_addresses_and_version_new_sessions() -> None:
    app, _ = _app()
    assert app._create_session_id("assistant", None).entity_name == "dafx-v2-assistant"
    assert app._create_session_id("assistant", "original-key").entity_name == "dafx-assistant"
    assert app._create_session_id("assistant", "@assistant@original-key").entity_name == "dafx-assistant"
    assert app._create_session_id("assistant", "@dafx-v2-assistant@new-key").entity_name == "dafx-v2-assistant"
    with pytest.raises(ValueError, match="Unsupported"):
        app._create_session_id("assistant", "@dafx-v3-assistant@key")
    context = Mock()
    assert app.get_agent(context, "assistant")._executor.entity_version == 1
    assert app.get_agent(context, "assistant", entity_version=2)._executor.entity_version == 2


@pytest.mark.parametrize("name", ["v2-assistant", "V3-customer"])
def test_functions_isolated_historical_version_looking_names_keep_registered_and_parsed_addresses(name: str) -> None:
    app = AgentFunctionApp(
        agents=[_NonStreamingAgent(client=_Client(None), name=name)],
        deployment_mode="isolated_v2",
        enable_health_check=False,
        enable_http_endpoints=False,
    )
    names = {function.get_function_name() for function in app.get_functions()}
    assert f"dafx-{name}" in names
    assert app._create_session_id(name, f"@{name}@key").entity_name == f"dafx-{name}"
    assert app._create_session_id(name, "key").entity_name == f"dafx-{name}"


@pytest.mark.parametrize("legacy", [False, True])
def test_functions_two_phase_namespace_policy_does_not_apply_to_other_generations(legacy: bool) -> None:
    app, _ = _app(legacy=legacy)
    with pytest.raises(ValueError, match="two.phase.*namespace"):
        app.add_agent(_NonStreamingAgent(client=_Client(None), name="V2-assistant"))
    app.add_agent(_NonStreamingAgent(client=_Client(None), name="V3-customer"))
    names = {function.get_function_name() for function in app.get_functions()}
    assert "dafx-v2-V3-customer" in names
    assert ("dafx-V3-customer" in names) == legacy


@pytest.mark.parametrize("mode", ["isolated_v2", "two_phase"])
def test_functions_workflow_registration_checks_only_opted_in_namespace(mode: str) -> None:
    agent = AgentExecutor(_NonStreamingAgent(client=_Client(None), name="assistant"), id="chat")
    workflow = WorkflowBuilder(name="v2-orders", start_executor=agent, output_from=[agent]).build()
    app = AgentFunctionApp(
        deployment_mode=mode,
        acknowledge_host_filtering=mode == "two_phase",
        enable_health_check=False,
        enable_http_endpoints=False,
    )
    if mode == "two_phase":
        with pytest.raises(ValueError, match="two.phase.*namespace"):
            app.configure_workflow(workflow)
    else:
        app.configure_workflow(workflow)
        assert "dafx-v2-orders" in {function.get_function_name() for function in app.get_functions()}


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("plain_text", [False, True])
@pytest.mark.parametrize("supplied", [None, "", "original-key", "@dafx-v2-web@new-key"])
async def test_registered_http_handler_selects_new_generation_and_returns_reusable_address(
    legacy: bool, plain_text: bool, supplied: str | None
) -> None:
    app, _ = _app(legacy=legacy)
    app.add_agent(
        _NonStreamingAgent(client=_Client(None), name="web"),
        enable_http_endpoint=True,
    )
    registered = next(item for item in app.get_functions() if item.get_function_name() == "http-web")
    handler = getattr(registered.get_user_function(), "client_function", None)
    assert callable(handler)
    client = Mock()
    client.signal_entity = AsyncMock()
    params = {"waitForResponse": "false"}
    if supplied is not None:
        params["sessionId"] = supplied
    content_type = "text/plain" if plain_text else "application/json"
    body = b"hello" if plain_text else json.dumps({"message": "hello"}).encode()

    def request() -> func.HttpRequest:
        return func.HttpRequest(
            method="POST",
            url="http://localhost/agents/web/run",
            headers={"content-type": content_type},
            params=params,
            body=body,
        )

    response = await handler(request(), client)
    assert response.status_code == 202
    entity = client.signal_entity.call_args.args[0]
    assert entity.entity == ("dafx-web" if supplied == "original-key" else "dafx-v2-web")
    returned = response.headers["x-ms-session-id"] if plain_text else json.loads(response.get_body())["sessionId"]
    if not supplied:
        assert returned == f"@dafx-v2-web@{entity.key}"
    else:
        assert returned == supplied
    params["sessionId"] = returned
    retry = await handler(request(), client)
    assert retry.status_code == 202
    assert client.signal_entity.call_args.args[0] == entity


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("supplied", [None, "original-key", "@dafx-v2-web@new-key", "@dafx-v3-web@key"])
async def test_registered_mcp_handler_selects_address_and_rejects_unknown_generations(
    monkeypatch: pytest.MonkeyPatch, legacy: bool, supplied: str | None
) -> None:
    app, _ = _app(legacy=legacy)
    app.add_agent(_NonStreamingAgent(client=_Client(None), name="web"), enable_mcp_tool_trigger=True)
    registered = next(item for item in app.get_functions() if item.get_function_name() == "mcptool-web")
    handler = getattr(registered.get_user_function(), "client_function", None)
    assert callable(handler)
    poll = AsyncMock(return_value={"status": "success", "response": "hello"})
    monkeypatch.setattr(app, "_get_response_from_entity", poll)
    client = Mock()
    client.signal_entity = AsyncMock()
    arguments = {"query": "hello"}
    if supplied is not None:
        arguments["sessionId"] = supplied
    context = json.dumps({"arguments": arguments})
    if supplied == "@dafx-v3-web@key":
        with pytest.raises(ValueError, match="Unsupported"):
            await handler(context, client)
        client.signal_entity.assert_not_awaited()
        poll.assert_not_awaited()
        return
    assert await handler(context, client) == "hello"
    entity = client.signal_entity.call_args.args[0]
    assert entity.entity == ("dafx-web" if supplied == "original-key" else "dafx-v2-web")
    assert poll.call_args.kwargs["entity_instance_id"] == entity


@pytest.mark.asyncio
@pytest.mark.parametrize("protocol", ["http", "mcp"])
@pytest.mark.parametrize("generation", [1, 2])
async def test_registered_handlers_preserve_ambiguous_historical_logical_names(
    monkeypatch: pytest.MonkeyPatch, protocol: str, generation: int
) -> None:
    app, _ = _app()
    app.add_agent(
        _NonStreamingAgent(client=_Client(None), name="dafx-v2-web"),
        enable_http_endpoint=protocol == "http",
        enable_mcp_tool_trigger=protocol == "mcp",
    )
    prefix = "http" if protocol == "http" else "mcptool"
    registered = next(item for item in app.get_functions() if item.get_function_name() == f"{prefix}-dafx_v2_web")
    handler = getattr(registered.get_user_function(), "client_function", None)
    assert callable(handler)
    client = Mock()
    client.signal_entity = AsyncMock()
    supplied = "@dafx-v2-web@original-key" if generation == 1 else "@dafx-v2-dafx-v2-web@original-key"
    poll = AsyncMock(return_value={"status": "success", "response": "hello"})
    monkeypatch.setattr(app, "_get_response_from_entity", poll)
    if protocol == "http":
        response = await handler(
            func.HttpRequest(
                method="POST",
                url="http://localhost/agents/dafx-v2-web/run",
                headers={"content-type": "application/json"},
                params={"sessionId": supplied, "waitForResponse": "false"},
                body=json.dumps({"message": "hello"}).encode(),
            ),
            client,
        )
        assert response.status_code == 202
        assert json.loads(response.get_body())["sessionId"] == supplied
    else:
        assert await handler(json.dumps({"arguments": {"query": "hello", "sessionId": supplied}}), client) == "hello"
    target = client.signal_entity.call_args.args[0]
    assert target.entity == ("dafx-dafx-v2-web" if generation == 1 else "dafx-v2-dafx-v2-web")
    assert target.key == "original-key"


@pytest.mark.parametrize("entity_version", [1, 2])
def test_generated_functions_workflow_dispatch_depends_on_recorded_name_not_fleet(entity_version: int) -> None:
    model = _Client(None)
    agent = AgentExecutor(_NonStreamingAgent(client=model, name="assistant"), id="chat")
    workflow = WorkflowBuilder(name="schema-workflow", start_executor=agent, output_from=[agent]).build()
    app = AgentFunctionApp(
        workflow=workflow,
        deployment_mode="two_phase",
        acknowledge_host_filtering=True,
        process_legacy_entities=True,
        enable_health_check=False,
        enable_http_endpoints=False,
    )
    prefix = "dafx-" if entity_version == 1 else "dafx-v2-"
    host = _Host(app)
    result = host.start(f"{prefix}schema-workflow", "original-workflow-id", json.dumps(wrap_workflow_input("hello")))
    calls = [
        action.sendEntityMessage.entityOperationCalled
        for action in result.actions
        if action.HasField("sendEntityMessage")
    ]
    assert len(calls) == 1
    assert calls[0].targetInstanceId.value == f"@{prefix}schema-workflow-chat@original-workflow-id"
    assert model.options == []
