# Copyright (c) Microsoft. All rights reserved.

"""In-place upgrades through released SDK execution and public registration."""

import json
import logging
from copy import deepcopy
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Any
from unittest.mock import Mock

import pytest
from _execution_test_support import JsonStateProvider, NonStreamingAgent, RecordingChatClient
from _migration_test_support import _migration_request
from agent_framework import AgentExecutor, AgentResponse, Message, WorkflowBuilder
from durabletask.azuremanaged.worker import DurableTaskSchedulerWorker
from durabletask.client import TaskHubGrpcClient
from durabletask.internal import helpers
from durabletask.internal import orchestrator_service_pb2 as pb
from durabletask.worker import TaskHubGrpcWorker, WorkItemFilters, _OrchestrationExecutor

from agent_framework_durabletask import (
    AgentEntity,
    AgentSessionId,
    DurableAgentSession,
    DurableAgentState,
    DurableAIAgentClient,
    DurableAIAgentWorker,
    DurableWorkflowClient,
    EntityMigrationContext,
    EntitySchemaRegistry,
    LegacyDurableAgentState,
    RunRequest,
    StateCapacityError,
    agent_entity_schema_registry,
    read_agent_state,
    state_snapshot_digest,
    wrap_workflow_input,
)
from agent_framework_durabletask._durable_agent_state import DurableAgentStateRequest as LegacyRequest
from agent_framework_durabletask._durable_agent_state import DurableAgentStateResponse as LegacyResponse


@pytest.mark.parametrize(
    "endpoint",
    [
        "https://cloud.example:443",
        "cloud.example:8080",
        "127.0.0.1:8080@cloud.example",
        "localhost:0",
        "localhost:65536",
    ],
)
def test_local_acceptance_rejects_non_loopback_or_invalid_endpoints(endpoint: str) -> None:
    from test_entity_migration_dts import _local_emulator_endpoint

    with pytest.raises(ValueError, match="local emulator"):
        _local_emulator_endpoint(endpoint)


@pytest.mark.parametrize("endpoint", ["localhost:8080", "127.0.0.1:18080"])
def test_local_acceptance_accepts_only_explicit_loopback_ports(endpoint: str) -> None:
    from test_entity_migration_dts import _local_emulator_endpoint

    assert _local_emulator_endpoint(endpoint) == endpoint


def _native(
    *, legacy: bool = True, evidence: Any = None, max_state_bytes: int | None = 1_048_576
) -> tuple[Any, RecordingChatClient]:
    worker = DurableTaskSchedulerWorker(host_address="localhost:1", taskhub="test", token_credential=None)
    model = RecordingChatClient()
    DurableAIAgentWorker(
        worker,
        deployment_mode="two_phase",
        process_legacy_entities=legacy,
        legacy_evidence_provider=evidence,
        max_state_bytes=max_state_bytes,
    ).add_agent(NonStreamingAgent(client=model, name="assistant"))
    return worker, model


def _batch(
    worker: Any,
    source: Any,
    operation: str = "run",
    *,
    name: str = "dafx-assistant",
    correlation_id: str = "turn-1",
    payload: dict[str, Any] | None = None,
) -> Any:
    wire = (
        json.dumps(payload if payload is not None else {"message": "hello", "correlationId": correlation_id})
        if operation == "run" or payload is not None
        else None
    )
    request = pb.EntityBatchRequest(
        instanceId=f"@{name}@original-key",
        entityState=helpers.get_string_value(None if source is None else json.dumps(source)),
        operations=[
            pb.OperationRequest(
                operation=operation,
                requestId="operation-1",
                input=helpers.get_string_value(wire),
            )
        ],
    )
    stub = Mock()
    worker._execute_entity_batch(request, stub, None)
    return stub.CompleteEntityTask.call_args.args[0]


def _legacy() -> dict[str, Any]:
    # This is the actual historical writer, not a v2 snapshot relabeled as v1.
    return LegacyDurableAgentState().to_dict()


def _legacy_with_response() -> dict[str, Any]:
    state = LegacyDurableAgentState()
    request = RunRequest(message="old request", correlation_id="old-turn")
    response = AgentResponse(messages=[Message("assistant", ["old reply"])])
    state.data.conversation_history.extend([
        LegacyRequest.from_run_request(request),
        LegacyResponse.from_run_response("old-turn", response),
    ])
    return state.to_dict()


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
def test_registered_legacy_conversion_commits_at_original_address_and_survives_restart(version: str) -> None:
    source = _legacy()
    source["schemaVersion"] = version
    before = deepcopy(source)
    worker, model = _native()
    result = _batch(worker, source)
    assert result.results[0].HasField("success"), result
    upgraded = json.loads(result.entityState.value)
    assert upgraded["schemaVersion"] == "2.0.0"
    assert upgraded["dafxSchema"] == {"contract": "dafx.agent-state", "version": "2.0.0"}
    assert upgraded["data"]["session"]["session_id"] == "@dafx-assistant@original-key"
    assert "migration" not in upgraded["data"]
    assert source == before
    assert len(model.received_messages) == 1
    restarted, cold_model = _native()
    retry = _batch(restarted, upgraded)
    assert retry.results[0].success.result == result.results[0].success.result
    assert json.loads(retry.entityState.value) == upgraded
    assert cold_model.received_messages == []


@pytest.mark.parametrize("source", [_legacy(), DurableAgentState().to_dict(), None])
def test_successful_processing_stamps_current_schema_without_address_replacement(source: Any) -> None:
    worker, _ = _native()
    result = _batch(worker, source, name="dafx-v2-assistant")
    assert result.results[0].HasField("success"), result
    state = json.loads(result.entityState.value)
    assert state["data"]["session"]["session_id"] == "@dafx-v2-assistant@original-key"
    assert state["schemaVersion"] == state["dafxSchema"]["version"] == "2.0.0"


@pytest.mark.parametrize("source", [None, DurableAgentState().to_dict()])
@pytest.mark.parametrize("headroom", [-1, 0, 1])
def test_reset_budget_measures_the_complete_marked_sdk_commit(source: Any, headroom: int) -> None:
    expected = DurableAgentState().to_dict()
    expected["dafxSchema"] = {"contract": "dafx.agent-state", "version": "2.0.0"}
    size = len(json.dumps(expected, allow_nan=False).encode("ascii"))
    worker, model = _native(max_state_bytes=size + headroom)
    result = _batch(worker, source, "reset", name="dafx-v2-assistant")
    if headroom < 0:
        assert result.results[0].HasField("failure"), result
        if source is None:
            assert not result.entityState.value
        else:
            assert json.loads(result.entityState.value) == source
    else:
        assert result.results[0].HasField("success"), result
        assert json.loads(result.entityState.value) == expected
    assert not model.received_messages


@pytest.mark.parametrize("source", [None, DurableAgentState().to_dict()])
def test_run_retention_measures_the_complete_marked_sdk_commit(source: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    from agent_framework_durabletask import _entities

    measured: list[dict[str, Any]] = []
    enforce = _entities.enforce_budget

    async def observe(state: DurableAgentState, **kwargs: Any) -> int:
        measured.append(deepcopy(state.to_dict()))
        return await enforce(state, **kwargs)

    monkeypatch.setattr(_entities, "enforce_budget", observe)
    worker, _ = _native()
    result = _batch(worker, source, name="dafx-v2-assistant")
    assert result.results[0].HasField("success"), result
    assert measured == [json.loads(result.entityState.value)]


@pytest.mark.parametrize("headroom", [-1, 0, 1])
def test_in_place_conversion_budget_excludes_export_binding_and_includes_schema_marker(headroom: int) -> None:
    source = _legacy_with_response()
    before = deepcopy(source)
    registry = agent_entity_schema_registry(process_legacy_entities=True)
    expected = registry.upgrade(source, EntityMigrationContext("dafx-assistant", "original-key"))
    assert "migration" not in expected["data"]
    assert expected["dafxSchema"] == {"contract": "dafx.agent-state", "version": "2.0.0"}
    size = len(json.dumps(expected, allow_nan=False).encode("ascii"))
    context = EntityMigrationContext("dafx-assistant", "original-key", max_state_bytes=size + headroom)
    if headroom < 0:
        with pytest.raises(StateCapacityError) as caught:
            registry.upgrade(source, context)
        assert caught.value.size_bytes == size
    else:
        assert registry.upgrade(source, context) == expected
    assert source == before


@pytest.mark.parametrize("initialized", [False, True])
def test_export_import_accepts_empty_two_phase_destination_and_protects_committed_binding(initialized: bool) -> None:
    worker, model = _native()
    name = "dafx-v2-assistant"
    source = None
    if initialized:
        reset = _batch(worker, None, "reset", name=name)
        assert reset.results[0].HasField("success"), reset
        source = json.loads(reset.entityState.value)
        assert source["dafxSchema"] == {"contract": "dafx.agent-state", "version": "2.0.0"}
    request = _migration_request(_legacy(), f"@{name}@original-key")
    result = _batch(worker, source, "migrate", name=name, payload=request)
    assert result.results[0].HasField("success"), result
    committed = json.loads(result.entityState.value)
    assert committed["data"]["migration"]["destinationSessionId"] == f"@{name}@original-key"
    assert committed["dafxSchema"] == {"contract": "dafx.agent-state", "version": "2.0.0"}
    restarted, cold_model = _native()
    duplicate = _batch(restarted, committed, "migrate", name=name, payload=request)
    assert duplicate.results[0].success.result == result.results[0].success.result
    assert json.loads(duplicate.entityState.value) == committed
    changed = {**request, "migrationId": "different-migration"}
    rejected = _batch(restarted, committed, "migrate", name=name, payload=changed)
    assert rejected.results[0].HasField("failure"), rejected
    assert json.loads(rejected.entityState.value) == committed
    assert not model.received_messages and not cold_model.received_messages


@pytest.mark.parametrize("occupied", ["root", "data", "session", "receipt"])
def test_schema_metadata_does_not_make_occupied_migration_destinations_empty(occupied: str) -> None:
    worker, model = _native()
    name = "dafx-v2-assistant"
    if occupied == "receipt":
        prior = _batch(worker, None, name=name)
        assert prior.results[0].HasField("success"), prior
        source = json.loads(prior.entityState.value)
        source["data"]["conversationHistory"] = []
        source["data"].pop("session", None)
    else:
        reset = _batch(worker, None, "reset", name=name)
        assert reset.results[0].HasField("success"), reset
        source = json.loads(reset.entityState.value)
        if occupied == "root":
            source["extensionData"] = {"application": False}
        elif occupied == "data":
            source["data"]["application"] = {"retained": []}
        else:
            source["data"]["session"] = {"session_id": f"@{name}@original-key", "state": {}}
    calls = len(model.received_messages)
    before = deepcopy(source)
    result = _batch(
        worker, source, "migrate", name=name, payload=_migration_request(_legacy(), f"@{name}@original-key")
    )
    assert result.results[0].HasField("failure"), result
    assert "empty" in result.results[0].failure.failureDetails.errorMessage
    assert json.loads(result.entityState.value) == before
    assert len(model.received_messages) == calls


@pytest.mark.parametrize(
    "metadata",
    [
        None,
        {},
        [],
        "2.0.0",
        {"contract": "other", "version": "2.0.0"},
        {"contract": "dafx.agent-state", "version": "3.0.0"},
    ],
)
@pytest.mark.parametrize("operation", ["run", "reset", "expire_responses", "delete"])
def test_bad_metadata_rejects_all_operations_without_state_or_model_effects(metadata: Any, operation: str) -> None:
    source = DurableAgentState().to_dict()
    source["dafxSchema"] = metadata
    worker, model = _native()
    result = _batch(worker, source, operation)
    assert result.results[0].HasField("failure"), result
    assert json.loads(result.entityState.value) == source
    assert not model.received_messages


@pytest.mark.parametrize("version", [None, True, 2, "2.1.0", "3.0.0", "unknown"])
def test_unknown_version_rejects_before_version_sensitive_decode(version: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    source = {"schemaVersion": version, "data": {"conversationHistory": "not a history"}}
    worker, model = _native()
    decode = Mock(side_effect=AssertionError("must not decode a request"))
    monkeypatch.setattr("agent_framework_durabletask._entities.RunRequest.from_dict", decode)
    result = _batch(worker, source)
    assert result.results[0].HasField("failure"), result
    assert json.loads(result.entityState.value) == source
    decode.assert_not_called()
    assert not model.received_messages


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize("late_retry", [False, True])
@pytest.mark.parametrize("failed", [False, True])
def test_populated_legacy_cutover_preserves_history_and_protects_only_committed_new_attempts(
    version: str, late_retry: bool, failed: bool
) -> None:
    source = _legacy_with_response()
    source["schemaVersion"] = version
    before = deepcopy(source)
    candidate = agent_entity_schema_registry(process_legacy_entities=True).upgrade(
        source, EntityMigrationContext("dafx-assistant", "original-key")
    )
    assert candidate["data"]["conversationHistory"] == source["data"]["conversationHistory"]
    assert candidate["data"]["completionReceipts"] == candidate["data"]["terminalResults"] == {}
    correlation = "old-turn" if late_retry else "new-turn"
    worker, model = _native()
    model.fail = failed
    result = _batch(worker, source, correlation_id=correlation)
    assert result.results[0].HasField("success"), result
    upgraded = json.loads(result.entityState.value)
    assert len(model.received_messages) == 1
    assert set(upgraded["data"]["completionReceipts"]) == {correlation}
    assert upgraded["data"]["completionReceipts"][correlation]["outcome"] == ("failed" if failed else "succeeded")
    assert source == before
    restarted, cold_model = _native()
    duplicate = _batch(restarted, upgraded, correlation_id=correlation)
    assert duplicate.results[0].success.result == result.results[0].success.result
    assert json.loads(duplicate.entityState.value) == upgraded
    assert cold_model.received_messages == []
    if not late_retry:
        old_retry = _batch(restarted, upgraded, correlation_id="old-turn")
        assert old_retry.results[0].HasField("success"), old_retry
        assert len(cold_model.received_messages) == 1


@pytest.mark.parametrize("invalid", ["truncation", "cursor", "session", "reserved", "lossy"])
def test_cutover_does_not_authorize_other_ambiguous_state(invalid: str) -> None:
    source = _legacy_with_response()
    data = source["data"]
    if invalid == "truncation":
        data["truncation"] = {"evictedMessageCount": 1}
    elif invalid == "cursor":
        data["ingestedPositions"] = {"producer": 1}
    elif invalid == "session":
        data["session"] = {"session_id": "different-address", "state": {}}
    elif invalid == "reserved":
        data["pythonIngestion"] = {}
    else:
        data["conversationHistory"][0]["$type"] = "unknown"
    worker, model = _native()
    result = _batch(worker, source)
    assert result.results[0].HasField("failure"), result
    assert json.loads(result.entityState.value) == source
    assert model.received_messages == []


@pytest.mark.parametrize("populated", [False, True])
@pytest.mark.parametrize(
    "evidence",
    [
        None,
        [],
        (),
        [("completion_evidence", {})],
        {"completion_evidence": None},
        {"delivery_evidence": None},
        {"completion_evidence": []},
        {"delivery_evidence": []},
        {"completion_evidence": {}},
        {"delivery_evidence": {}},
    ],
)
def test_invalid_supplied_evidence_rejects_before_request_decode_or_effects(
    evidence: Any, populated: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = _legacy_with_response() if populated else _legacy()
    before = deepcopy(source)
    worker, model = _native(evidence=lambda snapshot, context: evidence)
    decode = Mock(side_effect=AssertionError("must not decode rejected input"))
    monkeypatch.setattr("agent_framework_durabletask._entities.RunRequest.from_dict", decode)
    result = _batch(worker, source)
    assert result.results[0].HasField("failure"), result
    assert json.loads(result.entityState.value) == before == source
    decode.assert_not_called()
    assert model.received_messages == []


@pytest.mark.parametrize("provider", [None, lambda *_: {}, lambda *_: MappingProxyType({})])
def test_omitted_evidence_and_empty_mapping_intentionally_start_cutover(provider: Any) -> None:
    source = _legacy_with_response()
    worker, model = _native(evidence=provider)
    result = _batch(worker, source)
    assert result.results[0].HasField("success"), result
    state = json.loads(result.entityState.value)
    assert set(state["data"]["completionReceipts"]) == {"turn-1"}
    assert "old-turn" not in state["data"]["terminalResults"]
    assert len(model.received_messages) == 1


def test_cutover_preserves_valid_session_and_opaque_json_without_recovering_old_completions() -> None:
    source = _legacy_with_response()
    source["data"]["session"] = {
        "session_id": "@dafx-assistant@original-key",
        "state": {"customer": {"retained": [False, 1, None]}},
        "service_session_id": {"provider": "opaque-continuation"},
    }
    source["extensionData"] = {"customer": {"preserved": True}}
    upgraded = agent_entity_schema_registry(process_legacy_entities=True).upgrade(
        source, EntityMigrationContext("dafx-assistant", "original-key")
    )
    assert upgraded["data"]["session"] == source["data"]["session"]
    assert upgraded["extensionData"] == source["extensionData"]
    assert upgraded["data"]["conversationHistory"] == source["data"]["conversationHistory"]
    assert upgraded["data"]["completionReceipts"] == upgraded["data"]["terminalResults"] == {}


def test_legacy_writer_does_not_persist_its_in_memory_failure_flag() -> None:
    response = AgentResponse(
        messages=[Message("assistant", ["retained response"])],
        created_at="2024-01-02T03:04:05+00:00",
    )
    entry = LegacyResponse.from_run_response("old-turn", response)
    original = deepcopy(entry.to_dict())
    entry.is_error = True
    assert entry.to_dict() == original
    assert original["createdAt"] == response.created_at
    restored = LegacyResponse.from_dict(original)
    assert not restored.is_error


def test_evidence_backed_legacy_conversion_does_not_invent_completion_time() -> None:
    source = _legacy_with_response()
    # Explicit synthetic operator ground truth. The legacy transcript does not
    # supply this completion timestamp or prove journal completeness.
    completed = "2024-01-03T04:05:06+00:00"
    calls: list[str] = []

    def evidence(snapshot: dict[str, Any], context: EntityMigrationContext) -> dict[str, Any]:
        calls.append(context.session_id)
        return {
            "completion_evidence": {
                "sourceDigest": state_snapshot_digest(snapshot),
                "evidenceId": "test-authoritative-journal",
                "complete": True,
                "results": [
                    {
                        "correlationId": "old-turn",
                        "outcome": "succeeded",
                        "completedAt": completed,
                        "response": {
                            "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "old reply"}]}]
                        },
                    }
                ],
            }
        }

    worker, _ = _native(evidence=evidence)
    result = _batch(worker, source)
    assert result.results[0].HasField("success"), result
    state = json.loads(result.entityState.value)
    assert state["data"]["completionReceipts"]["old-turn"]["completedAt"] == completed
    assert calls == ["@dafx-assistant@original-key"]

    duplicate_worker, duplicate_model = _native(evidence=evidence)
    duplicate = _batch(duplicate_worker, source, correlation_id="old-turn")
    assert duplicate.results[0].HasField("success"), duplicate
    assert json.loads(duplicate.entityState.value)["schemaVersion"] == "2.0.0"
    assert duplicate_model.received_messages == []


async def test_failed_write_does_not_commit_upgrade_and_retry_reconverts_after_restart() -> None:
    source = _legacy_with_response()
    provider = JsonStateProvider(source, entity_name="dafx-assistant", session_id="original-key")
    provider._entity_schema_registry = agent_entity_schema_registry(process_legacy_entities=True)
    provider.fail_before_write = True
    model = RecordingChatClient()
    entity = AgentEntity(NonStreamingAgent(client=model, name="assistant"), state_provider=provider)
    with pytest.raises(OSError, match="injected commit failure"):
        await entity.run({"message": "hello", "correlationId": "turn-1"})
    assert provider.raw == source and provider.successful_writes == 0
    worker, _ = _native()
    result = _batch(worker, provider.raw)
    assert result.results[0].HasField("success"), result
    assert json.loads(result.entityState.value)["schemaVersion"] == "2.0.0"


def test_phase_one_registers_only_new_names_and_advertises_actual_sdk_filters() -> None:
    worker, _ = _native(legacy=False)
    filters = WorkItemFilters._from_registry(worker._registry)
    assert worker._auto_generate_work_item_filters
    assert [entry.name for entry in filters.entities] == ["dafx-v2-assistant"]
    assert worker._registry.get_entity("dafx-assistant") is None
    phase_two, _ = _native()
    assert {entry.name for entry in WorkItemFilters._from_registry(phase_two._registry).entities} == {
        "dafx-v2-assistant",
        "dafx-assistant",
    }


def test_two_phase_rejects_unverified_backend_and_invalid_cutover_options() -> None:
    with pytest.raises(ValueError, match="DurableTaskSchedulerWorker"):
        DurableAIAgentWorker(TaskHubGrpcWorker(channel=Mock()), deployment_mode="two_phase")
    with pytest.raises(ValueError, match="requires deployment_mode"):
        DurableAIAgentWorker(
            TaskHubGrpcWorker(channel=Mock()), deployment_mode="isolated_v2", process_legacy_entities=True
        )
    with pytest.raises(ValueError, match="boolean"):
        agent_entity_schema_registry(process_legacy_entities="true")  # type: ignore[arg-type]


def test_client_new_addresses_and_serialized_old_addresses_are_not_retargeted() -> None:
    client = DurableAIAgentClient(Mock(), entity_version=2)
    new = client.get_agent("assistant").create_session()
    assert str(new.durable_session_id).startswith("@dafx-v2-assistant@")
    restored = DurableAgentSession.from_dict(new.to_dict())
    assert restored.durable_session_id == new.durable_session_id
    old = DurableAgentSession(durable_session_id=AgentSessionId("assistant", "original-key"))
    assert client._executor._create_session_id("assistant", old).entity_name == "dafx-assistant"
    assert AgentSessionId.parse("@dafx-assistant@original-key", agent_name="assistant").entity_name == "dafx-assistant"
    prefixed = DurableAgentSession(durable_session_id=AgentSessionId("dafx-assistant", "original-key"))
    assert DurableAgentSession.from_dict(prefixed.to_dict()).durable_session_id == prefixed.durable_session_id
    ambiguous_old = DurableAgentSession(durable_session_id=AgentSessionId("dafx-v2-assistant", "original-key"))
    assert DurableAgentSession.from_dict(ambiguous_old.to_dict()).durable_session_id == ambiguous_old.durable_session_id
    with pytest.raises(ValueError, match="Unsupported"):
        AgentSessionId.parse("@dafx-v3-assistant@original-key")


@pytest.mark.parametrize("name", ["v2-assistant", "V3-customer"])
def test_historical_version_looking_names_keep_client_serialization_and_isolated_registration(name: str) -> None:
    client = DurableAIAgentClient(Mock())
    session = client.get_agent(name).create_session()
    assert session.durable_session_id is not None
    address = session.durable_session_id.entity_name
    assert address == f"dafx-{name}"
    restored = DurableAgentSession.from_dict(session.to_dict())
    assert restored.durable_session_id == session.durable_session_id
    assert AgentSessionId.parse(str(session.durable_session_id), agent_name=name).entity_name == address
    native = TaskHubGrpcWorker(channel=Mock())
    DurableAIAgentWorker(native, deployment_mode="isolated_v2").add_agent(
        NonStreamingAgent(client=RecordingChatClient(), name=name)
    )
    assert native._registry.get_entity(address.lower()) is not None


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("name", ["v2-assistant", "V2-assistant"])
def test_two_phase_rejects_names_in_its_actual_generation_two_namespace_before_registration(
    legacy: bool, name: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    native = DurableTaskSchedulerWorker(host_address="localhost:1", taskhub="test", token_credential=None)
    host = DurableAIAgentWorker(native, deployment_mode="two_phase", process_legacy_entities=legacy)
    add = Mock(wraps=native.add_entity)
    monkeypatch.setattr(native, "add_entity", add)
    with pytest.raises(ValueError, match="two.phase.*namespace"):
        host.add_agent(NonStreamingAgent(client=RecordingChatClient(), name=name))
    add.assert_not_called()
    host.add_agent(NonStreamingAgent(client=RecordingChatClient(), name="V3-customer"))
    assert native._registry.get_entity("dafx-v2-v3-customer") is not None


@pytest.mark.parametrize("mode", ["isolated_v2", "two_phase"])
def test_workflow_registration_separates_historical_formatting_and_two_phase_eligibility(mode: str) -> None:
    native = DurableTaskSchedulerWorker(host_address="localhost:1", taskhub="test", token_credential=None)
    executor = AgentExecutor(NonStreamingAgent(client=RecordingChatClient(), name="assistant"), id="chat")
    workflow = WorkflowBuilder(name="v2-orders", start_executor=executor, output_from=[executor]).build()
    host = DurableAIAgentWorker(native, deployment_mode=mode)
    if mode == "two_phase":
        with pytest.raises(ValueError, match="two.phase.*namespace"):
            host.configure_workflow(workflow)
        assert not native._registry.orchestrators
    else:
        host.configure_workflow(workflow)
        assert native._registry.get_orchestrator("dafx-v2-orders") is not None


@pytest.mark.parametrize("generation", [1, 2])
def test_nested_workflow_dispatch_and_cold_replay_preserve_recorded_generation(generation: int) -> None:
    from _workflow_replay_test_support import _Episodes
    from test_workflow_sdk_history_replay import _graph

    workflow, _ = _graph(mixed=True)
    native = DurableTaskSchedulerWorker(host_address="localhost:1", taskhub="test", token_credential=None)
    DurableAIAgentWorker(native, deployment_mode="two_phase", process_legacy_entities=True).configure_workflow(workflow)
    episodes = _Episodes(workflow, worker=native)
    prefix = "dafx-" if generation == 1 else "dafx-v2-"
    episodes.start(f"{prefix}audit-mixed", input=wrap_workflow_input("go"), instance_id="original-root")
    episodes.complete_named("original-root", "seed")
    children = [instance for instance in episodes.names if instance != "original-root"]
    assert len(children) == 1
    child = children[0]
    assert episodes.names[child] == f"{prefix}audit-inner"
    from agent_framework_durabletask._workflows.naming import subworkflow_instance_id

    assert child == subworkflow_instance_id("original-root", "sub", 0)
    episodes.complete_named(child, "b")
    episodes.cold("original-root")
    episodes.cold(child)


def test_registry_extends_synthetic_versions_without_guessing_production_v3() -> None:
    def validate(state: dict[str, Any]) -> None:
        assert isinstance(state["data"]["value"], int)

    def convert(state: dict[str, Any], context: EntityMigrationContext) -> dict[str, Any]:
        assert context.entity_key == "original-key"
        state["schemaVersion"] = "test-b"
        state["data"]["value"] += 1
        return state

    registry = EntitySchemaRegistry(
        contract="test-only",
        current_version="test-b",
        validators={"test-a": validate, "test-b": validate},
        conversions={"test-a": ("test-b", convert)},
        unmarked_versions=("test-a",),
    )
    source: dict[str, Any] = {"schemaVersion": "test-a", "data": {"value": 2}}
    context = EntityMigrationContext("original", "original-key")
    converted = registry.upgrade(source, context)
    assert source["data"]["value"] == 2
    assert converted["data"]["value"] == 3
    assert registry.upgrade(converted, context) == converted
    with pytest.raises(ValueError, match="Unsupported"):
        registry.upgrade({"schemaVersion": "test-future"}, context)


def test_converter_failure_cannot_mutate_original_json() -> None:
    def broken(state: dict[str, Any], _: EntityMigrationContext) -> dict[str, Any]:
        state["data"]["value"].append("damaged")
        raise RuntimeError("converter failed")

    registry = EntitySchemaRegistry(
        contract="test-only",
        current_version="test-b",
        validators={"test-a": lambda _: None, "test-b": lambda _: None},
        conversions={"test-a": ("test-b", broken)},
        unmarked_versions=("test-a",),
    )
    source = {"schemaVersion": "test-a", "data": {"value": ["original"]}}
    before = deepcopy(source)
    with pytest.raises(RuntimeError, match="converter failed"):
        registry.upgrade(source, EntityMigrationContext("original", "original-key"))
    assert source == before


@pytest.mark.parametrize("version", ["1.1.0", "2.0.0"])
def test_readers_reject_incompatible_metadata_without_projection(version: str) -> None:
    state = _legacy() if version.startswith("1.") else DurableAgentState().to_dict()
    state["dafxSchema"] = {"contract": "future-contract", "version": version}
    with pytest.raises(ValueError, match="metadata"):
        read_agent_state(state)
    with pytest.raises(ValueError, match="metadata"):
        DurableAgentState.from_dict(state)


def test_current_registry_noop_is_detached_and_idle_legacy_state_is_not_written() -> None:
    registry = agent_entity_schema_registry(process_legacy_entities=True)
    state = DurableAgentState().to_dict()
    registry.stamp(state)
    context = EntityMigrationContext("dafx-assistant", "original-key")
    assert registry.upgrade(state, context) == state
    detached = registry.upgrade(state, context)
    detached["data"]["conversationHistory"].append("local mutation")
    assert state["data"]["conversationHistory"] == []
    provider = JsonStateProvider(_legacy())
    provider._entity_schema_registry = registry
    assert provider.state.schema_version == "2.0.0"
    assert provider.raw == _legacy() and provider.attempted_writes == 0


def test_rejected_candidate_is_not_cached_as_admitted_state(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = JsonStateProvider(_legacy())
    provider._entity_schema_registry = agent_entity_schema_registry(process_legacy_entities=True)
    admission = Mock(side_effect=ValueError("injected final admission failure"))
    monkeypatch.setattr("agent_framework_durabletask._entities.validate_delivery_state", admission)
    for _ in range(2):
        with pytest.raises(ValueError, match="final admission"):
            provider.ensure_v2_writable()
    assert admission.call_count == 2
    assert not provider.schema_upgrade_pending
    assert provider.raw == _legacy() and provider.attempted_writes == 0


def test_conversion_retry_does_not_confuse_boolean_and_integer_source_changes() -> None:
    source = _legacy()
    source["opaque"] = False
    provider = JsonStateProvider(source)
    provider._entity_schema_registry = agent_entity_schema_registry(process_legacy_entities=True)
    assert provider.state.unknown_fields["opaque"] is False
    provider.raw["opaque"] = 0
    provider._write_acknowledgement_uncertain = True
    provider.ensure_v2_writable()
    value = provider.state.unknown_fields["opaque"]
    assert type(value) is int and value == 0


def test_generated_workflow_replays_historical_dispatch_and_versions_only_new_starts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FixedClock(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> "FixedClock":
            return cls(2026, 10, 1, tzinfo=timezone.utc)

    monkeypatch.setattr("agent_framework_durabletask._models.datetime", FixedClock)
    model = RecordingChatClient()
    agent = AgentExecutor(NonStreamingAgent(client=model, name="assistant"), id="chat")
    workflow = WorkflowBuilder(name="schema-workflow", start_executor=agent, output_from=[agent]).build()
    baseline = TaskHubGrpcWorker(channel=Mock())
    DurableAIAgentWorker(baseline, deployment_mode="isolated_v2").configure_workflow(workflow)
    upgraded = DurableTaskSchedulerWorker(host_address="localhost:1", taskhub="test", token_credential=None)
    DurableAIAgentWorker(upgraded, deployment_mode="two_phase", process_legacy_entities=True).configure_workflow(
        workflow
    )
    instance = "original-workflow-id"
    events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event("dafx-schema-workflow", instance, json.dumps(wrap_workflow_input("hello"))),
    ]

    def replay(worker: Any, history: list[Any]) -> Any:
        return _OrchestrationExecutor(worker._registry, logging.getLogger(__name__), worker._data_converter).execute(
            instance, [], history
        )

    old = replay(baseline, events)
    resumed = replay(upgraded, events)
    assert list(resumed.actions) == list(old.actions)
    call = next(
        action.sendEntityMessage.entityOperationCalled
        for action in resumed.actions
        if action.HasField("sendEntityMessage")
    )
    assert call.targetInstanceId.value == "@dafx-schema-workflow-chat@original-workflow-id"
    new_events = deepcopy(events)
    new_events[1].executionStarted.name = "dafx-v2-schema-workflow"
    started = replay(upgraded, new_events)
    new_call = next(
        action.sendEntityMessage.entityOperationCalled
        for action in started.actions
        if action.HasField("sendEntityMessage")
    )
    assert new_call.targetInstanceId.value == "@dafx-v2-schema-workflow-chat@original-workflow-id"
    assert old.actions and not model.received_messages
    native = Mock(spec=TaskHubGrpcClient)
    DurableWorkflowClient(native, workflow_name=workflow.name, entity_version=2).start_workflow(
        "hello", instance_id=instance
    )
    assert native.schedule_new_orchestration.call_args.args == ("dafx-v2-schema-workflow",)
    assert native.schedule_new_orchestration.call_args.kwargs["instance_id"] == instance
