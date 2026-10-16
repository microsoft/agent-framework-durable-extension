# Copyright (c) Microsoft. All rights reserved.

"""Legacy-path controls for the published prototype review findings."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest
from _execution_test_support import JsonStateProvider, RecordingChatClient
from _history_pipeline_test_support import ToolChatClient
from _invocation_test_support import lookup
from agent_framework import Agent, AgentResponse, AgentSession, ContextProvider, InMemoryHistoryProvider, Message
from test_agent_compatibility_prototype import _evidence, _legacy
from test_complete_compatibility import _continued, _ExternalHistory
from test_history_success_acceptance import _DropA
from test_history_success_acceptance import _ExternalHistory as _AcceptingExternalHistory

from agent_framework_durabletask import (
    CompatibleAgentEntity,
    DurableAgentState,
    LegacyDurableAgentState,
    LegacyMigrationEvidence,
    RunRequest,
    read_agent_state,
    serialize_agent_response,
    state_snapshot_digest,
)
from agent_framework_durabletask._shared_response import serialize_terminal_response


class _ToolPolicyAgent(Agent):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.invocations: list[tuple[dict[str, Any], Any]] = []

    def run(self, *args: Any, **kwargs: Any) -> Any:
        configuration = getattr(self.client, "function_invocation_configuration", {})
        self.invocations.append((dict(kwargs.get("options", {})), configuration.get("enabled")))
        return super().run(*args, **kwargs)


class _RichReplyAgent(Agent):
    def __init__(self, response: AgentResponse[Any]) -> None:
        super().__init__(client=RecordingChatClient(), name="delivery")
        self.response = response
        self.invocations: list[str] = []

    def run(self, *args: Any, **kwargs: Any) -> Any:
        async def complete() -> AgentResponse[Any]:
            self.invocations.append("model")
            return deepcopy(self.response)

        return complete()


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", ["entity", "external", "service"])
@pytest.mark.parametrize("store_inputs", [False, True])
@pytest.mark.parametrize("identity", ["occurrence", "public", "unkeyed"])
async def test_legacy_acceptance_follows_primary_owner_after_middleware_on_cold_redelivery(
    owner: str, store_inputs: bool, identity: str
) -> None:
    source = _legacy()
    provider = JsonStateProvider(source)
    client = RecordingChatClient(conversation_id="remote")
    history = (
        _AcceptingExternalHistory(store_inputs=store_inputs)
        if owner == "external"
        else InMemoryHistoryProvider(store_inputs=store_inputs)
    )
    first = CompatibleAgentEntity(
        Agent(client=client, context_providers=[history], middleware=[_DropA()]),
        state_provider=provider,
        deployment_mode="legacy",
    )
    inputs = [
        Message("user", [text], message_id=text if identity != "unkeyed" else None).to_dict() for text in ("A", "B")
    ]
    options = {"store": owner == "service"}
    result = await first.run(
        RunRequest(
            "",
            correlation_id="first",
            context_messages=inputs,
            context_message_ids=["occurrence-A", "occurrence-B"] if identity == "occurrence" else None,
            options=options,
        )
    )
    assert not result.additional_properties.get("durable_status")
    assert "A" not in [message.text for message in client.received_messages[0]]
    accepted = provider.raw["data"]["extensionData"]["_dafx_legacy_ingestion"]["messages"]
    assert set(accepted) == ({"occurrence-B"} if identity == "occurrence" else {"B"} if identity == "public" else set())
    assert [message["contents"][0]["text"] for message in inputs] == ["A", "B"]
    client = RecordingChatClient(conversation_id="remote")
    cold = CompatibleAgentEntity(
        Agent(
            client=client,
            context_providers=[
                _AcceptingExternalHistory(store_inputs=store_inputs)
                if owner == "external"
                else InMemoryHistoryProvider(store_inputs=store_inputs)
            ],
        ),
        state_provider=JsonStateProvider(provider.raw),
        deployment_mode="legacy",
    )
    await cold.run(
        RunRequest(
            "",
            correlation_id="redelivery",
            context_messages=inputs,
            context_message_ids=["occurrence-A", "occurrence-B"] if identity == "occurrence" else None,
            options=options,
        )
    )
    assert sum(message.text == "A" for message in client.received_messages[0]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
async def test_conversion_never_promotes_middleware_filtered_legacy_acceptance(version: str) -> None:
    provider = JsonStateProvider(_legacy(version=version))
    first = CompatibleAgentEntity(
        Agent(
            client=RecordingChatClient(),
            context_providers=[_AcceptingExternalHistory()],
            middleware=[_DropA()],
        ),
        state_provider=provider,
        deployment_mode="legacy",
    )
    inputs = [Message("user", [text], message_id=text).to_dict() for text in ("A", "B")]
    response = await first.run(
        RunRequest(
            "",
            correlation_id="first",
            context_messages=inputs,
            context_message_ids=["occurrence-A", "occurrence-B"],
        )
    )
    completed_at = provider.raw["data"]["conversationHistory"][-1]["createdAt"]

    def evidence(raw: dict[str, Any]) -> LegacyMigrationEvidence:
        completion = _evidence(raw).completion_evidence
        assert completion is not None
        completion["results"].append({
            "correlationId": "first",
            "outcome": "succeeded",
            "completedAt": completed_at,
            "response": serialize_terminal_response(serialize_agent_response(response)),
        })
        return LegacyMigrationEvidence(completion_evidence=completion)

    converted = CompatibleAgentEntity(
        Agent(client=RecordingChatClient(), context_providers=[_AcceptingExternalHistory()]),
        state_provider=provider,
        legacy_migration_evidence=evidence,
    )
    await converted.run(RunRequest("convert", correlation_id="convert"))
    assert provider.raw["schemaVersion"] == "2.0.0"
    assert set(DurableAgentState.from_dict(provider.raw).data.ingested_messages) == {"occurrence-B"}
    client = RecordingChatClient()
    cold = CompatibleAgentEntity(
        Agent(client=client, context_providers=[_AcceptingExternalHistory()]),
        state_provider=JsonStateProvider(provider.raw),
    )
    await cold.run(
        RunRequest(
            "",
            correlation_id="redelivery",
            context_messages=inputs,
            context_message_ids=["occurrence-A", "occurrence-B"],
        )
    )
    assert [message.text for message in client.received_messages[0]] == ["A"]


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", ["entity", "external", "service"])
async def test_legacy_store_only_memory_audit_preserves_prior_and_new_messages(owner: str) -> None:
    source = _legacy()
    session = AgentSession(session_id="original-logical-session")
    session.service_session_id = "remote" if owner == "service" else None
    session.state["audit"] = {"messages": [Message("user", ["audit-prior"])], "keep": True}
    source["data"]["session"] = session.to_dict()
    provider = JsonStateProvider(source)
    for correlation in ("first", "cold"):
        history = [_AcceptingExternalHistory()] if owner == "external" else []
        audit = InMemoryHistoryProvider("audit", load_messages=False)
        client = RecordingChatClient(conversation_id="remote", response_message_id=f"response-{correlation}")
        entity = CompatibleAgentEntity(
            Agent(client=client, context_providers=[*history, audit]),
            state_provider=provider,
            deployment_mode="legacy",
        )
        result = await entity.run(
            RunRequest(correlation, correlation_id=correlation, options={"store": owner == "service"})
        )
        assert not result.additional_properties.get("durable_status")
        restored = AgentSession.from_dict(provider.raw["data"]["session"])
        messages = restored.state["audit"].get("messages", [])
        assert [message.text for message in messages] == (
            ["audit-prior", "first", "reply-1"]
            if correlation == "first"
            else ["audit-prior", "first", "reply-1", "cold", "reply-1"]
        )
        assert restored.state["audit"]["keep"] is True
        assert "audit-prior" not in [message.text for message in client.received_messages[0]]
        assert audit.load_messages is False
        assert all(
            "messages" not in bag for key, bag in restored.state.items() if key != "audit" and isinstance(bag, dict)
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("reader_kind", ["dispatcher", "legacy", "shared"])
async def test_legacy_readers_share_complete_delivery_lookup_without_mutating_state(reader_kind: str) -> None:
    provider = JsonStateProvider(_legacy())
    response = AgentResponse[Any](
        messages=[Message("assistant", ["done"])],
        value={"answer": 42},
        response_id="full",
        finish_reason="stop",
    )
    producer = CompatibleAgentEntity(_RichReplyAgent(response), state_provider=provider, deployment_mode="legacy")
    await producer.run(RunRequest("new", correlation_id="new"))
    raw = deepcopy(provider.raw)
    raw["data"].pop("conversationHistory")
    before = deepcopy(raw)
    reader = (
        read_agent_state(raw)
        if reader_kind == "dispatcher"
        else LegacyDurableAgentState.from_dict(raw)
        if reader_kind == "legacy"
        else DurableAgentState.from_dict(raw)
    )
    delivered: AgentResponse[Any] | None = reader.try_get_agent_response("new")
    assert delivered is not None and delivered.value == {"answer": 42}
    delivered.value["answer"] = 99
    repeated = reader.try_get_agent_response("new")
    assert repeated is not None and repeated.value == {"answer": 42}
    assert raw == before


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [{"answer": [1, False, None]}, 0, False, None])
@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
async def test_new_legacy_deliveries_preserve_complete_response_on_cold_duplicates(value: Any, version: str) -> None:
    response = AgentResponse(
        messages=[Message("assistant", ["done"])],
        value=value,
        response_id="original-response",
        finish_reason="stop",
        additional_properties={"application": {"keep": True}},
    )
    provider = JsonStateProvider(_legacy(version=version))
    first_agent = _RichReplyAgent(response)
    first = CompatibleAgentEntity(first_agent, state_provider=provider, deployment_mode="legacy")
    original = await first.run(RunRequest("new", correlation_id="new"))
    committed = deepcopy(provider.raw)
    cold_agent = _RichReplyAgent(AgentResponse(messages=[Message("assistant", ["must not execute"])]))
    cold_provider = JsonStateProvider(committed)
    cold = CompatibleAgentEntity(cold_agent, state_provider=cold_provider, deployment_mode="legacy")
    duplicate = await cold.run(RunRequest("new", correlation_id="new"))
    assert serialize_agent_response(duplicate) == serialize_agent_response(original)
    assert (
        duplicate.value == value and duplicate.response_id == "original-response" and duplicate.finish_reason == "stop"
    )
    assert first_agent.invocations == ["model"] and cold_agent.invocations == []
    assert cold_provider.raw == committed and cold_provider.attempted_writes == 0


@pytest.mark.asyncio
async def test_historical_legacy_projection_is_not_promoted_to_a_complete_delivery() -> None:
    source = _legacy()
    agent = _RichReplyAgent(AgentResponse(messages=[Message("assistant", ["unused"])]))
    provider = JsonStateProvider(source)
    entity = CompatibleAgentEntity(agent, state_provider=provider, deployment_mode="legacy")
    result = await entity.run(RunRequest("old", correlation_id="old"))
    assert result.text == "old answer" and agent.invocations == []
    assert provider.raw == source and provider.attempted_writes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize("history", ["absent", "empty"])
@pytest.mark.parametrize("cached", [False, True])
async def test_partial_legacy_state_is_not_fresh_when_a_known_delivery_remains(
    version: str,
    history: str,
    cached: bool,
) -> None:
    provider = JsonStateProvider(_legacy(version=version))
    original = AgentResponse[Any](
        messages=[Message("assistant", ["known"])],
        value={"known": True},
        response_id="known-response",
        finish_reason="stop",
    )
    first = CompatibleAgentEntity(_RichReplyAgent(original), state_provider=provider, deployment_mode="legacy")
    await first.run(RunRequest("known", correlation_id="known"))
    source = deepcopy(provider.raw)
    source["data"].pop("session", None)
    if history == "absent":
        source["data"].pop("conversationHistory")
    else:
        source["data"]["conversationHistory"] = []
    if not cached:
        source["data"]["extensionData"]["_dafx_legacy_delivery"]["responses"] = {}
    cold_provider = JsonStateProvider(source)
    agent = _RichReplyAgent(AgentResponse[Any](messages=[Message("assistant", ["fresh"])], value={"fresh": True}))
    cold = CompatibleAgentEntity(agent, state_provider=cold_provider)
    result = await cold.run(RunRequest("known", correlation_id="known"))
    if cached:
        assert result.value == {"known": True} and result.response_id == "known-response"
        assert agent.invocations == [] and cold_provider.raw == source and cold_provider.attempted_writes == 0
    else:
        assert result.value == {"fresh": True} and agent.invocations == ["model"]
        assert cold_provider.raw["schemaVersion"] == "2.0.0"


def _context_extra(revision: int) -> dict[str, Any]:
    raw = Message("user", ["delivered"], message_id="public").to_dict()
    raw["future_message"] = {"revision": revision}
    return raw


@pytest.mark.asyncio
async def test_legacy_occurrence_revisions_include_original_message_extras() -> None:
    provider = JsonStateProvider(_legacy())
    client = RecordingChatClient()
    for revision in (1, 2):
        entity = CompatibleAgentEntity(
            Agent(client=client, name="extras"),
            state_provider=provider,
            deployment_mode="legacy",
        )
        await entity.run(
            RunRequest(
                "",
                correlation_id=f"revision-{revision}",
                context_messages=[_context_extra(revision)],
                context_message_ids=["occurrence"],
            )
        )
    fingerprints = provider.raw["data"]["extensionData"]["_dafx_legacy_ingestion"]["messages"]["occurrence"]
    assert len(fingerprints) == 2
    assert sum(message.text == "delivered" for message in client.received_messages[-1]) == 2


@pytest.mark.asyncio
async def test_original_extra_fingerprint_survives_conversion_and_cold_redelivery() -> None:
    provider = JsonStateProvider(_legacy())
    first = CompatibleAgentEntity(
        Agent(client=RecordingChatClient(), name="extras"),
        state_provider=provider,
        deployment_mode="legacy",
    )
    response = await first.run(
        RunRequest(
            "",
            correlation_id="first",
            context_messages=[_context_extra(1)],
            context_message_ids=["occurrence"],
        )
    )
    completed_at = provider.raw["data"]["conversationHistory"][-1]["createdAt"]

    def evidence(raw: dict[str, Any]) -> LegacyMigrationEvidence:
        completion = _evidence(raw).completion_evidence
        assert completion is not None
        completion["results"].append({
            "correlationId": "first",
            "outcome": "succeeded",
            "completedAt": completed_at,
            "response": serialize_terminal_response(serialize_agent_response(response)),
        })
        return LegacyMigrationEvidence(completion_evidence=completion)

    converted = CompatibleAgentEntity(
        Agent(client=RecordingChatClient(), name="extras"),
        state_provider=provider,
        legacy_migration_evidence=evidence,
    )
    await converted.run(RunRequest("convert", correlation_id="convert"))
    client = RecordingChatClient()
    cold = CompatibleAgentEntity(
        Agent(client=client, name="extras"),
        state_provider=JsonStateProvider(provider.raw),
    )
    await cold.run(
        RunRequest(
            "",
            correlation_id="repeat",
            context_messages=[_context_extra(1)],
            context_message_ids=["occurrence"],
        )
    )
    assert sum(message.text == "delivered" for message in client.received_messages[0]) == 1


@pytest.mark.asyncio
async def test_legacy_reset_clears_new_delivery_results_but_not_opaque_profile_siblings() -> None:
    agent = _RichReplyAgent(
        AgentResponse[Any](
            messages=[Message("assistant", ["done"])],
            value={"kept": True},
        )
    )
    provider = JsonStateProvider(_legacy())
    entity = CompatibleAgentEntity(agent, state_provider=provider, deployment_mode="legacy")
    await entity.run(RunRequest("new", correlation_id="new"))
    provider.raw["data"]["extensionData"]["_dafx_legacy_delivery"]["future"] = {"keep": True}
    entity.reset()
    profile = provider.raw["data"]["extensionData"]["_dafx_legacy_delivery"]
    assert profile["responses"] == {} and profile["future"] == {"keep": True}
    await entity.run(RunRequest("new", correlation_id="new"))
    assert agent.invocations == ["model", "model"]


@pytest.mark.asyncio
async def test_unsupported_legacy_delivery_profile_is_not_overwritten() -> None:
    source = _legacy()
    source["data"]["extensionData"]["_dafx_legacy_delivery"] = {"application": "opaque"}
    provider = JsonStateProvider(source)
    agent = _RichReplyAgent(AgentResponse(messages=[Message("assistant", ["unused"])]))
    entity = CompatibleAgentEntity(agent, state_provider=provider, deployment_mode="legacy")
    with pytest.raises(ValueError, match="Legacy delivery metadata"):
        await entity.run(RunRequest("new", correlation_id="new"))
    assert provider.raw == source and agent.invocations == []


@pytest.mark.asyncio
@pytest.mark.parametrize("preserve", [False, True])
async def test_conversion_evidence_must_preserve_known_full_delivery_fields(preserve: bool) -> None:
    response = AgentResponse[Any](
        messages=[Message("assistant", ["done"])],
        value={"answer": 42},
        response_id="full-response",
        finish_reason="stop",
    )
    provider = JsonStateProvider(_legacy())
    legacy = CompatibleAgentEntity(_RichReplyAgent(response), state_provider=provider, deployment_mode="legacy")
    delivered = await legacy.run(RunRequest("new", correlation_id="new"))
    source = deepcopy(provider.raw)
    completed_at = source["data"]["conversationHistory"][-1]["createdAt"]

    def evidence(raw: dict[str, Any]) -> LegacyMigrationEvidence:
        completion = _evidence(raw).completion_evidence
        assert completion is not None
        snapshot = serialize_terminal_response(serialize_agent_response(delivered))
        if not preserve:
            snapshot.pop("value")
        completion["results"].append({
            "correlationId": "new",
            "outcome": "succeeded",
            "completedAt": completed_at,
            "response": snapshot,
        })
        return LegacyMigrationEvidence(completion_evidence=completion)

    client = RecordingChatClient()
    converted = CompatibleAgentEntity(
        Agent(client=client, name="delivery"),
        state_provider=provider,
        legacy_migration_evidence=evidence,
    )
    if not preserve:
        with pytest.raises(ValueError, match="known full legacy delivery"):
            await converted.run(RunRequest("convert", correlation_id="convert"))
        assert provider.raw == source and client.received_messages == []
    else:
        await converted.run(RunRequest("convert", correlation_id="convert"))
        cold = CompatibleAgentEntity(
            Agent(client=RecordingChatClient(), name="delivery"),
            state_provider=JsonStateProvider(provider.raw),
        )
        result = await cold.run(RunRequest("new", correlation_id="new"))
        assert result.value == {"answer": 42} and result.response_id == "full-response"


class _AfterRunFailure(ContextProvider):
    async def after_run(self, **kwargs: Any) -> None:
        raise OSError("audit save failed after model completion")


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["model", "after-run"])
@pytest.mark.parametrize("owner", ["entity", "external", "service"])
async def test_failed_context_acceptance_does_not_replay_staged_request_on_cold_redelivery(
    phase: str, owner: str
) -> None:
    source = _legacy()
    provider = JsonStateProvider(source)
    history: list[ContextProvider] = [_AfterRunFailure("audit")] if phase == "after-run" else []
    if owner == "external":
        history.append(_AcceptingExternalHistory())
    failing = Agent(
        client=RecordingChatClient(fail=phase == "model", conversation_id="remote"),
        name="rollback",
        context_providers=history,
    )
    first = CompatibleAgentEntity(failing, state_provider=provider, deployment_mode="legacy")
    failed = await first.run(
        RunRequest(
            "",
            correlation_id="failed",
            context_messages=[_context_extra(1)],
            context_message_ids=["occurrence"],
            options={"store": owner == "service"},
        )
    )
    assert failed.additional_properties["durable_status"] == "error"
    assert "_dafx_legacy_ingestion" not in provider.raw["data"]["extensionData"]
    assert not any(
        entry.get("$type") == "request" and entry.get("correlationId") == "failed"
        for entry in provider.raw["data"]["conversationHistory"]
    )
    assert provider.raw["data"]["conversationHistory"][-1]["correlationId"] == "failed"
    client = RecordingChatClient()
    cold = CompatibleAgentEntity(
        Agent(client=client, name="rollback"),
        state_provider=JsonStateProvider(provider.raw),
        deployment_mode="legacy",
    )
    await cold.run(
        RunRequest(
            "",
            correlation_id="retry",
            context_messages=[_context_extra(1)],
            context_message_ids=["occurrence"],
        )
    )
    assert sum(message.text == "delivered" for message in client.received_messages[0]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("migrate", [False, True])
@pytest.mark.parametrize("replacement", ["same", "different", "entity"])
async def test_committed_legacy_owner_applies_before_both_execution_paths(
    migrate: bool,
    replacement: str,
) -> None:
    source = _continued()
    source["data"]["extensionData"]["_dafx_legacy_owner"] = {
        "profile": "dafx-legacy-owner-v1",
        "owner": "external",
        "sourceId": "external",
    }
    external = _ExternalHistory(source_id="different" if replacement == "different" else "external")
    client = RecordingChatClient()
    provider = JsonStateProvider(source)
    agent = Agent(client=client, name="owner", context_providers=[] if replacement == "entity" else [external])
    entity = CompatibleAgentEntity(
        agent,
        state_provider=provider,
        legacy_migration_evidence=_evidence if migrate else None,
    )
    if replacement != "same":
        with pytest.raises(ValueError, match="ownership"):
            await entity.run(RunRequest("question", correlation_id="next"))
        assert provider.raw == source and client.received_messages == [] and external.ids == []
    else:
        await entity.run(RunRequest("question", correlation_id="next"))
        assert external.ids == ["original-logical-session"]
        assert provider.raw["schemaVersion"] == ("2.0.0" if migrate else "1.1.0")


@pytest.mark.asyncio
@pytest.mark.parametrize("enabled", [False, True])
async def test_legacy_tool_policy_is_invocation_local_and_controls_advertisement(enabled: bool) -> None:
    client = ToolChatClient(tool_calls=False)
    agent = _ToolPolicyAgent(client=client, name="tools", tools=[lookup], default_options={"tool_choice": "auto"})
    defaults = dict(agent.default_options)
    configuration = client.function_invocation_configuration
    before = deepcopy(configuration)
    entity = CompatibleAgentEntity(agent, state_provider=JsonStateProvider(_legacy()), deployment_mode="legacy")
    await entity.run(
        RunRequest(
            "question",
            correlation_id="next",
            enable_tool_calls=enabled,
            options={"tools": [lookup], "tool_choice": "required"},
        )
    )
    options = client.received_options[0]
    if enabled:
        assert options["tools"] and options["tool_choice"] == "required"
    else:
        invocation, invocation_enabled = agent.invocations[0]
        assert invocation["tools"] == [] and invocation["tool_choice"] == "none" and invocation_enabled is False
        assert not options.get("tools") and options["tool_choice"] == "none"
    assert agent.default_options == defaults
    assert agent.client is client
    assert client.function_invocation_configuration is configuration and configuration == before
    await entity.run(RunRequest("enabled again", correlation_id="later"))
    assert client.received_options[-1]["tools"]


@pytest.mark.asyncio
async def test_legacy_disabled_tools_do_not_advertise_service_hosted_definitions() -> None:
    client = RecordingChatClient()
    hosted: Any = {"type": "web_search"}
    agent = _ToolPolicyAgent(client=client, name="hosted", tools=[hosted])
    entity = CompatibleAgentEntity(agent, state_provider=JsonStateProvider(_legacy()), deployment_mode="legacy")
    await entity.run(
        RunRequest(
            "question",
            correlation_id="next",
            enable_tool_calls=False,
            options={"tools": [hosted], "tool_choice": "auto"},
        )
    )
    assert agent.invocations[0][0]["tools"] == []
    assert not client.received_options[0].get("tools")
    assert client.received_options[0]["tool_choice"] == "none"
    assert agent.default_options["tools"]


def _delivery_only(source: dict[str, Any]) -> LegacyMigrationEvidence:
    return LegacyMigrationEvidence(
        delivery_evidence={
            "sourceDigest": state_snapshot_digest(source),
            "evidenceId": "recorded-public-lookups",
            "complete": True,
            "messages": [Message("user", ["accepted"], message_id="accepted-id").to_dict()],
            "messagePositions": [{"producer": "upstream", "position": 0}],
        }
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("first_explicit", [False, True])
@pytest.mark.parametrize("second_explicit", [False, True])
async def test_scalar_public_receipts_remain_distinct_from_actual_legacy_occurrences(
    first_explicit: bool,
    second_explicit: bool,
) -> None:
    source = _legacy()
    source["data"]["ingestedPositions"] = {"upstream": 0}
    provider = JsonStateProvider(source)
    first = CompatibleAgentEntity(
        Agent(client=RecordingChatClient(), name="delivery"),
        state_provider=provider,
        legacy_migration_evidence=_delivery_only,
    )

    def request(correlation: str, explicit: bool) -> RunRequest:
        return RunRequest(
            "",
            correlation_id=correlation,
            context_messages=[Message("user", ["accepted"], message_id="accepted-id").to_dict()],
            context_message_ids=["accepted-id"] if explicit else None,
        )

    await first.run(request("first", first_explicit))
    receipts = provider.raw["data"]["extensionData"]["_dafx_legacy_ingestion"]["messages"]
    assert ("accepted-id" in receipts) is first_explicit
    client = RecordingChatClient()
    cold_provider = JsonStateProvider(provider.raw)
    cold = CompatibleAgentEntity(
        Agent(client=client, name="delivery"),
        state_provider=cold_provider,
        legacy_migration_evidence=_delivery_only,
    )
    await cold.run(request("second", second_explicit))
    assert sum(message.text == "accepted" for message in client.received_messages[0]) == int(
        first_explicit or second_explicit
    )
    assert cold_provider.raw["data"]["ingestedPositions"] == {"upstream": 0}


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["v1", "v2"])
async def test_only_the_declared_public_lookup_profile_interprets_its_field(version: str) -> None:
    source = _legacy()
    profile = {
        "profile": f"dafx-legacy-ingestion-{version}",
        "messages": {},
        "publicMessages": {"business": 42},
    }
    source["data"]["extensionData"]["_dafx_legacy_ingestion"] = profile
    provider = JsonStateProvider(source)
    client = RecordingChatClient()
    entity = CompatibleAgentEntity(
        Agent(client=client, name="delivery"),
        state_provider=provider,
        deployment_mode="legacy",
    )
    request = RunRequest(
        "",
        correlation_id="next",
        context_messages=[Message("user", ["new"]).to_dict()],
        context_message_ids=["occurrence"],
    )
    if version == "v2":
        with pytest.raises(ValueError, match="exact content fingerprints"):
            await entity.run(request)
        assert provider.raw == source and client.received_messages == []
    else:
        await entity.run(request)
        assert provider.raw["data"]["extensionData"]["_dafx_legacy_ingestion"]["publicMessages"] == {"business": 42}
        assert any(message.text == "new" for message in client.received_messages[0])


@pytest.mark.asyncio
async def test_failed_legacy_delivery_does_not_commit_either_receipt_namespace() -> None:
    source = _legacy()
    source["data"]["ingestedPositions"] = {"upstream": 0}
    provider = JsonStateProvider(source)
    entity = CompatibleAgentEntity(
        Agent(client=RecordingChatClient(fail=True), name="delivery"),
        state_provider=provider,
        deployment_mode="legacy",
        legacy_migration_evidence=_delivery_only,
    )
    response = await entity.run(
        RunRequest(
            "",
            correlation_id="failed",
            context_messages=[Message("user", ["accepted"], message_id="accepted-id").to_dict()],
            context_message_ids=["independent"],
        )
    )
    assert response.additional_properties["durable_status"] == "error"
    extensions = deepcopy(provider.raw["data"]["extensionData"])
    delivery = extensions.pop("_dafx_legacy_delivery")
    assert extensions == source["data"]["extensionData"]
    assert delivery["responses"]["failed"] == serialize_agent_response(response)
    assert provider.raw["data"]["ingestedPositions"] == {"upstream": 0}


@pytest.mark.asyncio
async def test_legacy_public_journal_is_not_promoted_into_v2_occurrence_receipts() -> None:
    source = _legacy()
    source["data"]["ingestedPositions"] = {"upstream": 0}
    provider = JsonStateProvider(source)
    legacy = CompatibleAgentEntity(
        Agent(client=RecordingChatClient(), name="delivery"),
        state_provider=provider,
        deployment_mode="legacy",
        legacy_migration_evidence=_delivery_only,
    )
    response = await legacy.run(
        RunRequest(
            "",
            correlation_id="first",
            context_messages=[Message("user", ["accepted"], message_id="accepted-id").to_dict()],
        )
    )
    completed_at = provider.raw["data"]["conversationHistory"][-1]["createdAt"]

    def evidence(raw: dict[str, Any]) -> LegacyMigrationEvidence:
        completion = _evidence(raw).completion_evidence
        assert completion is not None
        completion["results"].append({
            "correlationId": "first",
            "outcome": "succeeded",
            "completedAt": completed_at,
            "response": serialize_terminal_response(response),
        })
        return LegacyMigrationEvidence(
            completion_evidence=completion,
            delivery_evidence=_delivery_only(raw).delivery_evidence,
        )

    converted = CompatibleAgentEntity(
        Agent(client=RecordingChatClient(), name="delivery"),
        state_provider=provider,
        legacy_migration_evidence=evidence,
    )
    await converted.run(RunRequest("convert", correlation_id="convert"))
    assert provider.raw["schemaVersion"] == "2.0.0"
    assert "accepted-id" not in provider.raw["data"].get("pythonIngestion", {}).get("messages", {})
    client = RecordingChatClient()
    cold = CompatibleAgentEntity(
        Agent(client=client, name="delivery"),
        state_provider=JsonStateProvider(provider.raw),
    )
    await cold.run(
        RunRequest(
            "",
            correlation_id="independent",
            context_messages=[Message("user", ["accepted"], message_id="accepted-id").to_dict()],
            context_message_ids=["accepted-id"],
        )
    )
    assert sum(message.text == "accepted" for message in client.received_messages[0]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("identity", ["absent", "null"])
@pytest.mark.parametrize("owner", ["external", "service"])
async def test_legacy_continuation_without_saved_identity_uses_detached_fallback(
    identity: str,
    owner: str,
) -> None:
    source = _continued()
    if identity == "absent":
        source["data"]["session"].pop("session_id")
    else:
        source["data"]["session"]["session_id"] = None
    client = RecordingChatClient(conversation_id="next-remote")
    external = _ExternalHistory()
    if owner == "service":
        source["data"]["session"]["state"] = {}
        source["data"]["session"]["service_session_id"] = "existing-remote"
    provider = JsonStateProvider(source, session_id="physical")
    entity = CompatibleAgentEntity(
        Agent(client=client, name="continuation", context_providers=[external] if owner == "external" else []),
        state_provider=provider,
        deployment_mode="legacy",
    )
    await entity.run(
        RunRequest(
            "current",
            correlation_id="next",
            options={"store": owner == "service"},
        )
    )
    if owner == "external":
        assert external.ids == ["@runtime@physical"] and external.continuations == ["old"]
    else:
        assert [message.text for message in client.received_messages[0]] == ["current"]
    assert provider.raw["data"]["session"]["session_id"] == "@runtime@physical"
    assert provider.raw["data"]["session"]["futureSession"] == {"keep": True}
    assert source["data"]["session"].get("session_id") is None


@pytest.mark.asyncio
async def test_legacy_missing_identity_restoration_does_not_commit_on_provider_failure() -> None:
    source = _continued()
    source["data"]["session"].pop("session_id")
    provider = JsonStateProvider(source)
    external = _ExternalHistory(fail=True)
    entity = CompatibleAgentEntity(
        Agent(client=RecordingChatClient(), name="continuation", context_providers=[external]),
        state_provider=provider,
        deployment_mode="legacy",
    )
    response = await entity.run(RunRequest("current", correlation_id="failure"))
    assert response.additional_properties["durable_status"] == "error"
    assert external.continuations == ["old"]
    assert provider.raw["data"]["session"] == source["data"]["session"]


@pytest.mark.parametrize("external", [False, True])
def test_legacy_reset_respects_primary_history_ownership(external: bool) -> None:
    source = _continued() if external else _legacy()
    provider = JsonStateProvider(source)
    history = _ExternalHistory()
    entity = CompatibleAgentEntity(
        Agent(client=RecordingChatClient(), name="reset", context_providers=[history] if external else []),
        state_provider=provider,
        deployment_mode="legacy",
    )
    if external:
        with pytest.raises(NotImplementedError, match="provider-owned clear"):
            entity.reset()
        assert provider.raw == source and history.ids == []
    else:
        entity.reset()
        assert provider.raw["schemaVersion"] == "1.1.0"
        assert provider.raw["data"]["conversationHistory"] == []
