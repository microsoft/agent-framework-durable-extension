# Copyright (c) Microsoft. All rights reserved.

"""Provider continuation and cold SDK replay for the complete local prototype."""

from __future__ import annotations

import json
from copy import deepcopy
from types import SimpleNamespace
from typing import Any, Literal
from unittest.mock import Mock

import pytest
from _execution_test_support import JsonStateProvider, LostAcknowledgementJsonStateProvider, RecordingChatClient
from _workflow_replay_test_support import _af_replay, _Episodes, _replay
from agent_framework import (
    Agent,
    AgentResponse,
    Executor,
    HistoryProvider,
    Message,
    WorkflowBuilder,
    WorkflowContext,
    WorkflowExecutor,
    handler,
    response_handler,
)
from durabletask.client import OrchestrationStatus
from durabletask.worker import TaskHubGrpcWorker
from test_agent_compatibility_prototype import _evidence, _Host, _legacy

from agent_framework_durabletask import (
    AgentEntity,
    CompatibleAgentEntity,
    DurableAIAgentWorker,
    DurableWorkflowClient,
    LegacyMigrationEvidence,
    RunRequest,
    WorkflowStart,
    state_snapshot_digest,
)
from agent_framework_durabletask._shared_response import serialize_terminal_response
from agent_framework_durabletask._workflows.compatibility import select_workflow_execution
from agent_framework_durabletask._workflows.serialization import serialize_workflow_agent_response


class _ExternalHistory(HistoryProvider):
    def __init__(self, *, fail: bool = False, source_id: str = "external") -> None:
        super().__init__(source_id)
        self.fail = fail
        self.ids: list[str | None] = []
        self.continuations: list[Any] = []

    async def get_messages(
        self,
        session_id: str | None,
        *,
        state: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> list[Message]:
        self.ids.append(session_id)
        self.continuations.append((state or {}).get("continuation"))
        return [Message("user", ["external prior"])]

    async def save_messages(
        self,
        session_id: str | None,
        messages: Any,
        *,
        state: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        if state is not None:
            state["continuation"] = "new"
        if self.fail:
            raise OSError("remote history save failed after its side effect")


def _continued() -> dict[str, Any]:
    raw = _legacy()
    raw["data"]["session"] = {
        "session_id": "original-logical-session",
        "service_session_id": None,
        "state": {"external": {"continuation": "old"}},
        "futureSession": {"keep": True},
    }
    return raw


@pytest.mark.asyncio
@pytest.mark.parametrize("migrate", [False, True])
async def test_external_owner_restores_original_identity_before_and_after_migration(migrate: bool) -> None:
    source = _continued()
    external = _ExternalHistory()
    client = RecordingChatClient()
    provider = JsonStateProvider(source, session_id="physical")
    entity = CompatibleAgentEntity(
        Agent(client=client, name="complete", context_providers=[external]),
        state_provider=provider,
        legacy_migration_evidence=_evidence if migrate else None,
    )
    await entity.run(RunRequest("new question", correlation_id="next"))
    assert external.ids == ["original-logical-session"]
    assert external.continuations == ["old"]
    assert [m.text for m in client.received_messages[0]] == ["external prior", "new question"]
    assert provider.raw["data"]["session"]["futureSession"] == {"keep": True}
    cold_external = _ExternalHistory()
    cold = CompatibleAgentEntity(
        Agent(client=RecordingChatClient(), name="complete", context_providers=[cold_external]),
        state_provider=JsonStateProvider(provider.raw, session_id="physical"),
    )
    await cold.run(RunRequest("another", correlation_id="later"))
    assert cold_external.ids == ["original-logical-session"]
    assert cold_external.continuations == ["new"]
    assert provider.raw["schemaVersion"] == ("2.0.0" if migrate else "1.1.0")


@pytest.mark.asyncio
async def test_failed_external_store_retains_last_committed_continuation() -> None:
    source = _continued()
    provider = JsonStateProvider(source)
    entity = CompatibleAgentEntity(
        Agent(client=RecordingChatClient(), name="complete", context_providers=[_ExternalHistory(fail=True)]),
        state_provider=provider,
        deployment_mode="legacy",
    )
    result = await entity.run(RunRequest("new question", correlation_id="failure"))
    assert result.additional_properties["durable_status"] == "error"
    assert provider.raw["data"]["session"] == source["data"]["session"]


@pytest.mark.asyncio
async def test_service_branch_restores_and_retains_its_inactive_continuation() -> None:
    source = _continued()
    source["data"]["session"]["service_session_id"] = "previous-remote"
    source["data"]["session"]["state"] = {}
    provider = JsonStateProvider(source)
    client = RecordingChatClient(conversation_id="new-remote")
    entity = CompatibleAgentEntity(
        Agent(client=client, name="complete"), state_provider=provider, deployment_mode="legacy"
    )
    await entity.run(RunRequest("first", correlation_id="first", options={"store": True}))
    assert [m.text for m in client.received_messages[-1]] == ["first"]
    assert provider.raw["data"]["session"]["service_session_id"] == "new-remote"
    await entity.run(RunRequest("local", correlation_id="local", options={"store": False}))
    assert provider.raw["data"]["session"]["service_session_id"] == "new-remote"
    await entity.run(RunRequest("remote", correlation_id="remote", options={"store": True}))
    assert [m.text for m in client.received_messages[-1]] == ["remote"]


@pytest.mark.asyncio
async def test_local_legacy_branch_does_not_leak_default_service_conversation() -> None:
    client = RecordingChatClient()
    agent = Agent(
        client=client,
        name="complete",
        default_options={"conversation_id": "inactive-remote", "store": True},
    )
    entity = CompatibleAgentEntity(
        agent,
        state_provider=JsonStateProvider(_legacy()),
        deployment_mode="legacy",
    )
    await entity.run(
        RunRequest(
            "local",
            correlation_id="local",
            options={"store": False, "conversation_id": "request-remote"},
        )
    )
    assert "conversation_id" not in client.received_options[0]
    assert agent.default_options["conversation_id"] == "inactive-remote"


@pytest.mark.asyncio
async def test_migration_preserves_legacy_all_content_acceptance_marker() -> None:
    source = _legacy()
    source["data"]["extensionData"]["_dafx_legacy_ingestion"] = {
        "profile": "dafx-legacy-ingestion-v1",
        "messages": {"accepted": None},
    }
    accepted = Message("user", ["old delivery"], message_id="accepted")
    evidence = _evidence(source)
    evidence = LegacyMigrationEvidence(
        completion_evidence=evidence.completion_evidence,
        delivery_evidence={
            "sourceDigest": state_snapshot_digest(source),
            "evidenceId": "accepted",
            "complete": True,
            "messages": [accepted.to_dict()],
        },
    )
    provider = JsonStateProvider(source)
    entity = CompatibleAgentEntity(
        Agent(client=RecordingChatClient(), name="complete"),
        state_provider=provider,
        legacy_migration_evidence=lambda _: evidence,
    )
    await entity.run(RunRequest("new", correlation_id="new"))
    assert provider.raw["data"]["pythonIngestion"]["messages"]["accepted"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize("migrate", [False, True])
@pytest.mark.parametrize("first_occurrence", ["item", "first-occurrence"])
@pytest.mark.parametrize("revision", ["revision-one", "revision-two"])
async def test_bridge_exact_ingestion_does_not_expand_to_all_revisions_on_conversion(
    migrate: bool,
    first_occurrence: str,
    revision: str,
) -> None:
    provider = JsonStateProvider()
    client = RecordingChatClient()
    legacy = CompatibleAgentEntity(
        Agent(client=client, name="complete"),
        state_provider=provider,
        deployment_mode="legacy",
    )
    response = await legacy.run(
        RunRequest(
            "",
            correlation_id="first",
            context_messages=[Message("user", ["revision-one"], message_id="item").to_dict()],
            context_message_ids=[first_occurrence],
        )
    )
    completed = provider.raw["data"]["conversationHistory"][-1]["createdAt"]
    evidence = LegacyMigrationEvidence(
        completion_evidence={
            "sourceDigest": state_snapshot_digest(provider.raw),
            "evidenceId": "observed-completion",
            "complete": True,
            "results": [
                {
                    "correlationId": "first",
                    "outcome": "succeeded",
                    "completedAt": completed,
                    "response": serialize_terminal_response(response),
                }
            ],
        }
    )
    cold = CompatibleAgentEntity(
        Agent(client=client, name="complete"),
        state_provider=provider,
        deployment_mode=None if migrate else "legacy",
        legacy_migration_evidence=(lambda _: evidence) if migrate else None,
    )
    await cold.run(RunRequest("boundary", correlation_id="boundary"))
    await cold.run(
        RunRequest(
            "",
            correlation_id="revision",
            context_messages=[Message("user", [revision], message_id="item").to_dict()],
            context_message_ids=["item"],
        )
    )
    count = sum(message.text == revision for message in client.received_messages[-1])
    assert count == (2 if revision == "revision-one" and first_occurrence != "item" else 1)


@pytest.mark.asyncio
async def test_conversion_does_not_infer_occurrence_acceptance_from_public_history_ids() -> None:
    source = _legacy()
    source["data"]["conversationHistory"][0]["messages"][0]["messageId"] = "public"
    client = RecordingChatClient()
    provider = JsonStateProvider(source)
    entity = CompatibleAgentEntity(
        Agent(client=client, name="complete"),
        state_provider=provider,
        legacy_migration_evidence=_evidence,
    )
    await entity.run(RunRequest("convert", correlation_id="convert"))
    await entity.run(
        RunRequest(
            "",
            correlation_id="independent-delivery",
            context_messages=[Message("user", ["independent"], message_id="public").to_dict()],
            context_message_ids=["public"],
        )
    )
    assert any(message.text == "independent" for message in client.received_messages[-1])


@pytest.mark.asyncio
@pytest.mark.parametrize("migrate", [False, True])
@pytest.mark.parametrize("explicit", [False, True])
async def test_delivery_journal_public_lookup_ids_are_not_explicit_occurrence_receipts(
    migrate: bool,
    explicit: bool,
) -> None:
    source = _legacy()
    source["data"]["conversationHistory"][0]["messages"][0]["messageId"] = "public"
    completion = _evidence(source).completion_evidence
    evidence = LegacyMigrationEvidence(
        completion_evidence=completion,
        delivery_evidence={
            "sourceDigest": state_snapshot_digest(source),
            "evidenceId": "accepted-inputs",
            "complete": True,
            "messages": [Message("user", ["old question"], message_id="public").to_dict()],
        },
    )
    client = RecordingChatClient()
    provider = JsonStateProvider(source)

    def current_evidence(raw: dict[str, Any]) -> LegacyMigrationEvidence:
        journal = deepcopy(evidence.delivery_evidence)
        assert journal is not None
        journal["sourceDigest"] = state_snapshot_digest(raw)
        return LegacyMigrationEvidence(completion_evidence=evidence.completion_evidence, delivery_evidence=journal)

    entity = CompatibleAgentEntity(
        Agent(client=client, name="complete"),
        state_provider=provider,
        deployment_mode=None if migrate else "legacy",
        legacy_migration_evidence=current_evidence,
    )
    await entity.run(RunRequest("boundary", correlation_id="boundary"))
    await entity.run(
        RunRequest(
            "",
            correlation_id="independent",
            context_messages=[Message("user", ["old question"], message_id="public").to_dict()],
            context_message_ids=["public"] if explicit else None,
        )
    )
    count = sum(message.text == "old question" for message in client.received_messages[-1])
    assert count == (1 if not explicit else 2)


@pytest.mark.asyncio
async def test_conversion_does_not_overwrite_opaque_public_ingestion_metadata() -> None:
    source = _legacy()
    source["data"]["extensionData"]["_dafx_legacy_public_ingestion"] = {"application": "opaque"}
    provider = JsonStateProvider(source)
    client = RecordingChatClient()
    entity = CompatibleAgentEntity(
        Agent(client=client, name="complete"),
        state_provider=provider,
        legacy_migration_evidence=_evidence,
    )
    with pytest.raises(ValueError, match="reserved migrated public ingestion"):
        await entity.run(RunRequest("convert", correlation_id="convert"))
    assert provider.raw == source and client.received_messages == []


@pytest.mark.asyncio
async def test_invalid_active_migrated_public_receipts_fail_before_model_execution() -> None:
    provider = JsonStateProvider(_legacy())
    entity = CompatibleAgentEntity(
        Agent(client=RecordingChatClient(), name="complete"),
        state_provider=provider,
        legacy_migration_evidence=_evidence,
    )
    await entity.run(RunRequest("convert", correlation_id="convert"))
    source = deepcopy(provider.raw)
    source["data"]["extensionData"]["_dafx_legacy_public_ingestion"] = {
        "profile": "dafx-legacy-public-ingestion-v1",
        "messages": {"public": None},
    }
    client = RecordingChatClient()
    cold = CompatibleAgentEntity(Agent(client=client, name="complete"), state_provider=JsonStateProvider(source))
    with pytest.raises(ValueError, match="exact content fingerprints"):
        await cold.run(RunRequest("new", correlation_id="new"))
    assert client.received_messages == []


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize("present", [False, True])
async def test_completion_evidence_accepts_absent_legacy_history(version: str, present: bool) -> None:
    source: dict[str, Any] = {"schemaVersion": version, "data": {"conversationHistory": []} if present else {}}
    before = deepcopy(source)
    provider = JsonStateProvider(source)
    entity = CompatibleAgentEntity(
        Agent(client=RecordingChatClient(), name="complete"),
        state_provider=provider,
        legacy_migration_evidence=lambda raw: LegacyMigrationEvidence(
            completion_evidence={
                "sourceDigest": state_snapshot_digest(raw),
                "evidenceId": "empty-journal",
                "complete": True,
                "results": [],
            }
        ),
    )
    await entity.run(RunRequest("first", correlation_id="first"))
    assert provider.raw["schemaVersion"] == "2.0.0"
    assert source == before


@pytest.mark.asyncio
@pytest.mark.parametrize("history", [None, {}])
async def test_absent_history_normalization_does_not_replace_invalid_present_history(history: Any) -> None:
    source = {"schemaVersion": "1.1.0", "data": {"conversationHistory": history}}
    provider = JsonStateProvider(source)
    client = RecordingChatClient()
    entity = CompatibleAgentEntity(
        Agent(client=client, name="complete"),
        state_provider=provider,
        legacy_migration_evidence=lambda raw: LegacyMigrationEvidence(
            completion_evidence={
                "sourceDigest": state_snapshot_digest(raw),
                "evidenceId": "empty-journal",
                "complete": True,
                "results": [],
            }
        ),
    )
    with pytest.raises(ValueError):
        await entity.run(RunRequest("first", correlation_id="first"))
    assert provider.raw == source
    assert client.received_messages == []


@pytest.mark.asyncio
@pytest.mark.parametrize("entity_type", [AgentEntity, CompatibleAgentEntity])
async def test_lost_v2_acknowledgement_reloads_before_same_provider_retry(entity_type: Any) -> None:
    client = RecordingChatClient()
    provider = LostAcknowledgementJsonStateProvider()
    entity = entity_type(Agent(client=client, name="complete"), state_provider=provider)
    request = RunRequest("question", correlation_id="same")
    with pytest.raises(OSError, match="acknowledgement lost"):
        await entity.run(request)
    await entity.run(request)
    assert len(client.received_messages) == 1
    assert provider.successful_writes == 1


@pytest.mark.asyncio
async def test_legacy_context_occurrences_are_not_duplicated_across_cold_turns() -> None:
    source = _legacy()
    context = [{"role": "user", "contents": [{"type": "text", "text": "delivered"}], "message_id": "public"}]
    provider = JsonStateProvider(source)
    client = RecordingChatClient()
    entity = CompatibleAgentEntity(
        Agent(client=client, name="complete"), state_provider=provider, deployment_mode="legacy"
    )
    await entity.run(
        RunRequest(
            "",
            correlation_id="delivery",
            context_messages=context,
            context_message_ids=["occurrence"],
        )
    )
    cold_client = RecordingChatClient()
    cold = CompatibleAgentEntity(
        Agent(client=cold_client, name="complete"),
        state_provider=JsonStateProvider(provider.raw),
        deployment_mode="legacy",
    )
    await cold.run(
        RunRequest(
            "",
            correlation_id="different-turn",
            context_messages=context,
            context_message_ids=["occurrence"],
        )
    )
    assert sum(m.text == "delivered" for m in cold_client.received_messages[0]) == 1
    assert provider.raw["schemaVersion"] == "1.1.0"


@pytest.mark.asyncio
async def test_scalar_ingestion_requires_recorded_evidence_for_new_context() -> None:
    source = _legacy()
    source["data"]["ingestedPositions"] = {"upstream": 0}
    provider = JsonStateProvider(source)
    client = RecordingChatClient()
    entity = CompatibleAgentEntity(
        Agent(client=client, name="complete"), state_provider=provider, deployment_mode="legacy"
    )
    with pytest.raises(ValueError, match="trusted delivery evidence"):
        await entity.run(
            RunRequest(
                "",
                correlation_id="delivery",
                context_messages=[Message("user", ["new"]).to_dict()],
                context_message_ids=["new-occurrence"],
            )
        )
    assert provider.raw == source
    assert client.received_messages == []


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["owner", "session", "ingestion"])
async def test_invalid_legacy_continuation_is_rejected_before_provider_io(corruption: str) -> None:
    source = _continued()
    if corruption == "owner":
        source["data"]["extensionData"]["_dafx_legacy_owner"] = {
            "profile": "dafx-legacy-owner-v1",
            "owner": "external",
            "sourceId": "different",
        }
    elif corruption == "session":
        source["data"]["session"]["state"] = []
    else:
        source["data"]["extensionData"]["_dafx_legacy_ingestion"] = {"profile": "unknown"}
    history = _ExternalHistory()
    client = RecordingChatClient()
    provider = JsonStateProvider(source)
    entity = CompatibleAgentEntity(
        Agent(client=client, name="complete", context_providers=[history]),
        state_provider=provider,
        deployment_mode="legacy",
    )
    with pytest.raises(ValueError):
        await entity.run(RunRequest("new", correlation_id="new"))
    assert provider.raw == source
    assert history.ids == []
    assert client.received_messages == []


def test_trusted_resolver_can_select_ambiguous_legacy_application_data_without_mutating_it() -> None:
    source = {"_durable_workflow_version": 2, "input": {"business": "value"}}
    before = deepcopy(source)

    def resolve(start: WorkflowStart) -> Literal["legacy"]:
        assert start.instance_id == "recorded-root" and start.parent_instance_id is None
        start.input["input"]["business"] = "detached"
        return "legacy"

    profile, selected = select_workflow_execution(
        source,
        instance_id="recorded-root",
        parent_instance_id=None,
        deployment_mode="isolated_v2",
        resolver=resolve,
    )
    assert profile == "legacy" and selected == before and source == before


class _Echo(Executor):
    @handler(input=str, output=str)
    async def echo(self, value: str, ctx: WorkflowContext[Any, str]) -> None:
        await ctx.yield_output(value + "-done")


def _simple_workflow() -> Any:
    return WorkflowBuilder(name="complete-flow", start_executor=_Echo(id="echo")).build()


class _DictionaryEcho(Executor):
    @handler(input=dict, output=dict)
    async def echo(self, value: dict[str, Any], ctx: WorkflowContext[Any, dict[str, Any]]) -> None:
        await ctx.yield_output(value)


def test_legacy_application_response_marker_is_literal_through_replay_and_activity() -> None:
    payload = {"_durable_agent_response": 1, "response": "ordinary business value"}
    workflow = WorkflowBuilder(name="literal-flow", start_executor=_DictionaryEcho(id="echo")).build()
    episodes = _legacy_episodes(workflow)
    episodes.client.start_workflow(payload, instance_id="literal-root")
    assert len(episodes.actions["literal-root"]) == 1
    episodes.complete_named("literal-root", "echo")
    replayed = _af_replay(episodes.histories["literal-root"], workflow, instance="literal-root")
    assert replayed["isDone"] is True
    assert replayed["output"] == [payload]
    native = Mock()
    native.wait_for_orchestration_completion.return_value = SimpleNamespace(
        name="dafx-literal-flow",
        runtime_status=OrchestrationStatus.COMPLETED,
        serialized_input=json.dumps(payload),
        serialized_output=json.dumps([payload]),
        serialized_custom_status=json.dumps({"executionProtocol": 1}),
    )
    assert DurableWorkflowClient(native, workflow_name="literal-flow").await_workflow_output("literal-root") == [
        payload
    ]


@pytest.mark.parametrize("host", ["native", "functions"])
@pytest.mark.parametrize("legacy", [False, True])
def test_output_consumers_use_recorded_protocol_instead_of_application_key_shape(host: str, legacy: bool) -> None:
    from agent_framework_azurefunctions import AgentFunctionApp

    payload = {"_durable_agent_response": 1, "response": "ordinary business value"}
    output = (
        [payload]
        if legacy
        else [serialize_workflow_agent_response(AgentResponse(messages=[Message("assistant", ["ok"])]))]
    )
    recorded = None if legacy else {"_durable_workflow_version": 2, "input": None}
    status = SimpleNamespace(
        input=recorded,
        output=output,
        custom_status=None,
        serialized_input=json.dumps(recorded),
        serialized_output=json.dumps(output),
        serialized_custom_status=None,
        runtime_status=OrchestrationStatus.COMPLETED,
    )
    if host == "native":
        native = Mock()
        native.wait_for_orchestration_completion.return_value = status
        decoded = DurableWorkflowClient(native).await_workflow_output("root")
    else:
        decoded = AgentFunctionApp(enable_health_check=False)._decode_workflow_output(status)
    if legacy:
        assert decoded == [payload]
    else:
        assert isinstance(decoded[0], AgentResponse) and decoded[0].text == "ok"


@pytest.mark.parametrize("host", ["native", "functions"])
@pytest.mark.parametrize("profile", ["legacy", "v2"])
def test_completed_ambiguous_workflow_output_honors_instance_aware_trusted_resolver(
    host: str,
    profile: Literal["legacy", "v2"],
) -> None:
    from agent_framework_azurefunctions import AgentFunctionApp

    recorded = {"_durable_workflow_version": 2, "input": {"business": "literal"}}
    before = deepcopy(recorded)
    literal = {"_durable_agent_response": 1, "response": "literal legacy data"}
    output = (
        [literal]
        if profile == "legacy"
        else [serialize_workflow_agent_response(AgentResponse(messages=[Message("assistant", ["v2"])]))]
    )
    calls: list[str] = []

    def resolve(start: WorkflowStart) -> Literal["legacy", "v2"]:
        assert start.instance_id == "historical-root" and start.parent_instance_id is None
        assert start.input == before
        start.input["input"]["business"] = "detached"
        calls.append(start.instance_id)
        return profile

    status = SimpleNamespace(
        instance_id="historical-root",
        input=recorded,
        output=output,
        custom_status={"state": "completed"},
        serialized_input=json.dumps(recorded),
        serialized_output=json.dumps(output),
        serialized_custom_status=json.dumps({"state": "completed"}),
        runtime_status=OrchestrationStatus.COMPLETED,
    )
    if host == "native":
        native = Mock()
        native.wait_for_orchestration_completion.return_value = status
        decoded = DurableWorkflowClient(native, workflow_execution_resolver=resolve).await_workflow_output(
            "historical-root"
        )
    else:
        app = AgentFunctionApp(workflow_execution_resolver=resolve, enable_health_check=False)
        decoded = app._decode_workflow_output(status)
    assert calls == ["historical-root"]
    assert recorded == before
    if profile == "legacy":
        assert decoded == [literal]
    else:
        assert isinstance(decoded[0], AgentResponse) and decoded[0].text == "v2"


@pytest.mark.asyncio
async def test_completed_ambiguous_legacy_event_stream_uses_the_trusted_resolver() -> None:
    literal = {"_durable_agent_response": 1, "response": "literal legacy data"}
    native = Mock()
    native.get_orchestration_state.return_value = SimpleNamespace(
        instance_id="historical-root",
        runtime_status=OrchestrationStatus.COMPLETED,
        serialized_input=json.dumps({"_durable_workflow_version": 2, "input": "business"}),
        serialized_custom_status=json.dumps({"events": [{"type": "output", "data": literal}]}),
    )

    def resolve(start: WorkflowStart) -> Literal["legacy"]:
        assert start.instance_id == "historical-root"
        return "legacy"

    client = DurableWorkflowClient(native, workflow_execution_resolver=resolve)
    events = [event async for event in client.stream_workflow("historical-root")]
    assert len(events) == 1 and events[0].data == literal


def _legacy_episodes(workflow: Any) -> _Episodes:
    episodes = _Episodes(workflow)
    native = TaskHubGrpcWorker(host_address="localhost:1")
    DurableAIAgentWorker(native, deployment_mode="legacy").configure_workflow(workflow)
    episodes.worker = native
    episodes.client = DurableWorkflowClient(episodes.native, workflow_name=workflow.name, deployment_mode="legacy")
    return episodes


@pytest.mark.parametrize("host", ["native", "functions"])
def test_published_legacy_activity_history_replays_on_a_cold_current_host(host: str) -> None:
    workflow = _simple_workflow()
    episodes = _legacy_episodes(workflow)
    episodes.client.start_workflow("start", instance_id="legacy-instance")
    histories = deepcopy(episodes.histories["legacy-instance"])
    assert len(episodes.actions["legacy-instance"]) == 1
    scheduled = next(iter(episodes.actions["legacy-instance"].values())).scheduleTask
    assert scheduled.name == "dafx-complete-flow-echo"
    task_input = json.loads(scheduled.input.value)
    assert "is_hitl_response" not in task_input
    if host == "native":
        cold = TaskHubGrpcWorker(host_address="localhost:1")
        DurableAIAgentWorker(cold).configure_workflow(workflow)
        result = _replay(cold, "legacy-instance", histories)
        assert result.actions == []
        assert result.encoded_custom_status is None
    else:
        result = _af_replay(histories, workflow, instance="legacy-instance")
        assert result["isDone"] is False
        assert result.get("customStatus") is None


class _Gate(Executor):
    def __init__(self, id: str = "gate") -> None:
        super().__init__(id=id)
        self.replies: list[int] = []

    @handler(input=str)
    async def begin(self, value: str, ctx: WorkflowContext) -> None:
        await ctx.request_info(value, response_type=int, request_id="decision")

    @response_handler(request=str, response=int, workflow_output=str)
    async def finish(self, request: str, response: int, ctx: WorkflowContext[Any, str]) -> None:
        self.replies.append(response)
        await ctx.yield_output(f"{request}:{response}")


@pytest.mark.parametrize("nested", [False, True])
def test_legacy_human_wait_survives_cold_worker_and_resumes_without_v2_checkpoint(nested: bool) -> None:
    gate = _Gate()
    inner = WorkflowBuilder(name="complete-inner", start_executor=gate).build()
    workflow = (
        WorkflowBuilder(
            name="complete-parent",
            start_executor=WorkflowExecutor(inner, id="child", propagate_request=True),
        ).build()
        if nested
        else inner
    )
    episodes = _legacy_episodes(workflow)
    episodes.client.start_workflow("question", instance_id="root")
    instance = next(name for name in episodes.histories if name != "root") if nested else "root"
    episodes.complete_named(instance, "gate")
    assert episodes.statuses[instance]["state"] == "waiting_for_human_input"
    history = deepcopy(episodes.histories[instance])
    # Fresh registry, executor and contexts; no hand-driven replay flag.
    cold = TaskHubGrpcWorker(host_address="localhost:1")
    DurableAIAgentWorker(cold).configure_workflow(workflow)
    replayed = _replay(cold, instance, history)
    assert replayed.actions == []
    assert json.loads(replayed.encoded_custom_status)["pending_requests"]["decision"]["response_type"] == "builtins:int"
    af_wait = _af_replay(history, workflow, instance=instance)
    assert af_wait["isDone"] is False
    assert af_wait["customStatus"]["pending_requests"]["decision"]["response_type"] == "builtins:int"
    episodes.worker = cold
    episodes.signal(instance, event_name="decision", data=17)
    episodes.flush()
    episodes.complete_named(instance, "gate")
    assert gate.replies == [17]
    af_complete = _af_replay(episodes.histories[instance], workflow, instance=instance)
    assert af_complete["isDone"] is True
    if nested:
        assert af_complete["output"] == {
            "__subworkflow_result__": True,
            "outputs": ["question:17"],
            "events": [],
        }
    else:
        assert af_complete["output"] == ["question:17"]
    assert not any(
        "-hitl" in action["message"].get("request_id", "")
        for _, _, action in episodes.executed
        if isinstance(action.get("message"), dict)
    )


@pytest.mark.parametrize("kind", ["native", "functions"])
def test_registered_external_continuation_uses_the_same_full_agent_path(kind: str) -> None:
    provider = _ExternalHistory()
    client = RecordingChatClient()
    host = _Host(
        kind,
        _continued(),
        mode="legacy",
        agent_override=Agent(client=client, name="prototype", context_providers=[provider]),
    )
    host.run()
    assert provider.ids == ["original-logical-session"]
    assert provider.continuations == ["old"]
    assert host.raw()["schemaVersion"] == "1.1.0"


def test_legacy_nested_route_uses_recorded_ordinal_not_current_list_position() -> None:
    native = Mock()
    parent = "root"
    child = "root::child::4"
    recorded = {
        "__subworkflow_input__": "start",
        "__subworkflow_address__": {
            "root_instance_id": parent,
            "root_workflow_name": "complete-parent",
            "request_path_prefix": "child~2~",
        },
    }
    native.get_orchestration_state.side_effect = lambda instance: SimpleNamespace(
        name="dafx-complete-parent" if instance == parent else "dafx-complete-inner",
        serialized_custom_status=json.dumps(
            {"subworkflows": {"child": [child]}} if instance == parent else {"pending_requests": {"decision": {}}},
        ),
        serialized_input=json.dumps(recorded) if instance == child else json.dumps("start"),
    )
    client = DurableWorkflowClient(native, workflow_name="complete-parent", deployment_mode="legacy")
    client.send_hitl_response(parent, "child~2~decision", 17)
    native.raise_orchestration_event.assert_called_once_with(child, event_name="decision", data=17)
    with pytest.raises(ValueError):
        client.send_hitl_response(parent, "child~0~decision", 17)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["legacy", None])
@pytest.mark.parametrize("explicit", [False, True])
async def test_recorded_scalar_delivery_evidence_preserves_public_lookup_semantics(
    mode: str | None,
    explicit: bool,
) -> None:
    source = _legacy()
    source["data"]["ingestedPositions"] = {"upstream": 0}
    accepted = Message("user", ["accepted"], message_id="accepted-id")
    delivery = {
        "sourceDigest": state_snapshot_digest(source),
        "evidenceId": "delivery-ledger",
        "complete": True,
        "messages": [accepted.to_dict()],
        "messagePositions": [{"producer": "upstream", "position": 0}],
    }
    client = RecordingChatClient()
    provider = JsonStateProvider(source)
    entity = CompatibleAgentEntity(
        Agent(client=client, name="complete"),
        state_provider=provider,
        deployment_mode=mode,
        legacy_migration_evidence=lambda _: LegacyMigrationEvidence(delivery_evidence=delivery),
    )
    await entity.run(
        RunRequest(
            "",
            correlation_id="delivery",
            context_messages=[accepted.to_dict()],
            context_message_ids=["accepted-id"] if explicit else None,
        )
    )
    assert any(message.text == "accepted" for message in client.received_messages[0]) is explicit
    assert provider.raw["data"]["ingestedPositions"] == {"upstream": 0}
    assert provider.raw["schemaVersion"] == "1.1.0"


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["mode", "sourceEntitySessionId", "sourceSchema", "requestDigest"])
async def test_in_place_binding_rejects_tampering_before_provider_or_model(field: str) -> None:
    provider = JsonStateProvider(_continued(), session_id="physical")
    original = _ExternalHistory()
    entity = CompatibleAgentEntity(
        Agent(client=RecordingChatClient(), name="complete", context_providers=[original]),
        state_provider=provider,
        legacy_migration_evidence=_evidence,
    )
    await entity.run(RunRequest("question", correlation_id="first"))
    corrupted = deepcopy(provider.raw)
    corrupted["data"]["migration"][field] = "wrong"
    history = _ExternalHistory()
    client = RecordingChatClient()
    cold = CompatibleAgentEntity(
        Agent(client=client, name="complete", context_providers=[history]),
        state_provider=JsonStateProvider(corrupted, session_id="physical"),
    )
    with pytest.raises(ValueError, match="migration session binding"):
        await cold.run(RunRequest("another", correlation_id="second"))
    assert history.ids == []
    assert client.received_messages == []
