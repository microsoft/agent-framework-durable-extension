# Copyright (c) Microsoft. All rights reserved.

"""Owner persistence, ingress identity and delivery remain independent on legacy wire."""

import hashlib
import json
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import pytest
from _execution_test_support import JsonStateProvider, NonStreamingAgent, RecordingChatClient
from _history_pipeline_test_support import ToolChatClient, lookup
from agent_framework import (
    Agent,
    AgentContext,
    AgentMiddleware,
    AgentResponse,
    AgentSession,
    ChatOptions,
    HistoryProvider,
    InMemoryHistoryProvider,
    Message,
    ServiceSessionId,
)
from test_agent_compatibility_prototype import _legacy

from agent_framework_durabletask import (
    CompatibleAgentEntity,
    DurableAgentState,
    LegacyMigrationEvidence,
    RunRequest,
    serialize_agent_response,
    state_snapshot_digest,
)


@pytest.mark.parametrize("store_inputs,store_outputs", [(True, True), (False, True), (True, False), (False, False)])
async def test_cold_replay_respects_primary_storage_flags(store_inputs: bool, store_outputs: bool) -> None:
    provider = JsonStateProvider(_legacy())
    client = RecordingChatClient()
    first = CompatibleAgentEntity(
        Agent(
            client=client,
            context_providers=[InMemoryHistoryProvider(store_inputs=store_inputs, store_outputs=store_outputs)],
        ),
        state_provider=provider,
        deployment_mode="legacy",
    )
    response = await first.run(
        RunRequest(
            "",
            correlation_id="first",
            context_messages=[Message("user", ["incoming"], message_id="public").to_dict()],
            context_message_ids=["occurrence"],
        )
    )
    assert not response.additional_properties.get("durable_status")
    assert "occurrence" in provider.raw["data"]["extensionData"]["_dafx_legacy_ingestion"]["messages"]
    assert "first" in provider.raw["data"]["extensionData"]["_dafx_legacy_delivery"]["responses"]
    client = RecordingChatClient()
    cold_provider = JsonStateProvider(provider.raw)
    cold = CompatibleAgentEntity(
        Agent(
            client=client,
            context_providers=[InMemoryHistoryProvider(store_inputs=store_inputs, store_outputs=store_outputs)],
        ),
        state_provider=cold_provider,
        deployment_mode="legacy",
    )
    duplicate = await cold.run(RunRequest("incoming", correlation_id="first"))
    assert duplicate.text == response.text and client.received_messages == []
    assert cold_provider.attempted_writes == 0
    await cold.run(RunRequest("next", correlation_id="next"))
    replay = [message.text for message in client.received_messages[0]]
    assert ("incoming" in replay) is store_inputs
    assert ("reply-1" in replay) is store_outputs
    assert replay[:2] == ["old question", "old answer"]


@pytest.mark.parametrize("context", [False, True])
async def test_failed_pipeline_does_not_replay_unstored_current_input(context: bool) -> None:
    provider = JsonStateProvider(_legacy())
    first = CompatibleAgentEntity(
        Agent(client=RecordingChatClient(fail=True)),
        state_provider=provider,
        deployment_mode="legacy",
    )
    failed = await first.run(
        RunRequest(
            "" if context else "unstored",
            correlation_id="failed",
            context_messages=[Message("user", ["unstored"]).to_dict()] if context else None,
        )
    )
    assert failed.additional_properties["durable_status"] == "error"
    client = RecordingChatClient()
    cold = CompatibleAgentEntity(
        Agent(client=client),
        state_provider=JsonStateProvider(provider.raw),
        deployment_mode="legacy",
    )
    await cold.run(RunRequest("next", correlation_id="next"))
    assert "unstored" not in [message.text for message in client.received_messages[0]]


class _Redact(AgentMiddleware):
    async def process(self, context: AgentContext, call_next: Callable[[], Awaitable[None]]) -> None:
        for message in context.messages:
            if message.text == "RAW":
                message.contents[0].text = "REDACTED"
        await call_next()


class _SnapshotHistory(HistoryProvider):
    def __init__(self) -> None:
        super().__init__("external")
        self.saved: list[dict[str, Any]] = []

    async def get_messages(self, session_id: str | None, **kwargs: Any) -> list[Message]:
        return []

    async def save_messages(self, session_id: str | None, messages: Sequence[Message], **kwargs: Any) -> None:
        self.saved.extend(serialize_agent_response(AgentResponse(messages=list(messages)))["messages"])


class _AfterSaveMutation(AgentMiddleware):
    def __init__(self, replace: bool) -> None:
        self.replace = replace

    async def process(self, context: AgentContext, call_next: Callable[[], Awaitable[None]]) -> None:
        await call_next()
        context.messages[0].contents[0].text = "AFTER_INPUT"
        assert isinstance(context.result, AgentResponse)
        if self.replace:
            context.result.messages = [Message("assistant", ["REPLACED"])]
        else:
            context.result.messages[0].contents[0].text = "AFTER_OUTPUT"


@pytest.mark.parametrize("memory,replace", [(False, False), (False, True), (True, False), (True, True)])
async def test_replay_preserves_owner_write_boundary_and_classification(memory: bool, replace: bool) -> None:
    history = _SnapshotHistory()
    provider = JsonStateProvider(_legacy())
    first = CompatibleAgentEntity(
        NonStreamingAgent(
            client=RecordingChatClient(),
            context_providers=[] if memory else [history],
            middleware=[_AfterSaveMutation(replace)],
        ),
        state_provider=provider,
        deployment_mode="legacy",
    )
    result = await first.run(RunRequest("RAW", correlation_id="first"))
    assert result.text == ("REPLACED" if replace else "AFTER_OUTPUT")
    if not memory:
        assert [Message.from_dict(message).text for message in history.saved] == ["RAW", "reply-1"]
    new = provider.raw["data"]["conversationHistory"][2:]
    assert [entry["$type"] for entry in new] == ["request", "response"]
    assert [len(entry["messages"]) for entry in new] == [1, 1]
    assert [entry["messages"][0]["contents"][0]["text"] for entry in new] == [
        "AFTER_INPUT" if memory else "RAW",
        "AFTER_OUTPUT" if memory and not replace else "reply-1",
    ]


@pytest.mark.parametrize("extra", [False, True])
async def test_cold_replay_uses_post_middleware_content(extra: bool) -> None:
    provider = JsonStateProvider(_legacy())
    client = RecordingChatClient()
    first = CompatibleAgentEntity(
        Agent(client=client, middleware=[_Redact()]),
        state_provider=provider,
        deployment_mode="legacy",
    )
    raw = Message("user", ["RAW"], message_id="public").to_dict()
    if extra:
        raw["future_message"] = {"keep": [False, 0, None]}
    response = await first.run(
        RunRequest(
            "",
            correlation_id="first",
            context_messages=[raw],
            context_message_ids=["occurrence"],
        )
    )
    assert not response.additional_properties.get("durable_status")
    assert "REDACTED" in [message.text for message in client.received_messages[0]]
    client = RecordingChatClient()
    cold = CompatibleAgentEntity(
        Agent(client=client),
        state_provider=JsonStateProvider(provider.raw),
        deployment_mode="legacy",
    )
    await cold.run(RunRequest("next", correlation_id="next"))
    replay = [message.text for message in client.received_messages[0]]
    assert "RAW" not in replay
    assert "REDACTED" in replay
    assert raw["contents"][0]["text"] == "RAW"
    assert provider.raw["data"]["extensionData"]["_dafx_legacy_ingestion"]["messages"]["occurrence"] == [
        hashlib.sha256(
            json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
        ).hexdigest()
    ]
    stored = provider.raw["data"]["conversationHistory"][2]["messages"][0]
    if extra:
        assert stored["future_message"] == {"keep": [False, 0, None]}


@pytest.mark.parametrize("per_call", [False, True])
@pytest.mark.parametrize("store_inputs,store_outputs", [(True, True), (False, True), (True, False), (False, False)])
async def test_legacy_replay_matches_real_core_persisted_tool_history(
    per_call: bool, store_inputs: bool, store_outputs: bool
) -> None:
    def agent(client: ToolChatClient) -> Agent[Any]:
        return Agent(
            client=client,
            tools=[lookup],
            context_providers=[InMemoryHistoryProvider(store_inputs=store_inputs, store_outputs=store_outputs)],
            require_per_service_call_history_persistence=per_call,
        )

    reference = agent(ToolChatClient())
    session = reference.create_session()
    session.state[InMemoryHistoryProvider.DEFAULT_SOURCE_ID] = {
        "messages": [Message("user", ["old question"]), Message("assistant", ["old answer"])]
    }
    await reference.run([Message("user", ["incoming"], message_id="public")], session=session)
    expected = session.state[InMemoryHistoryProvider.DEFAULT_SOURCE_ID]["messages"]
    provider = JsonStateProvider(_legacy())
    legacy = CompatibleAgentEntity(agent(ToolChatClient()), state_provider=provider, deployment_mode="legacy")
    response = await legacy.run(
        RunRequest(
            "",
            correlation_id="first",
            context_messages=[Message("user", ["incoming"], message_id="public").to_dict()],
            context_message_ids=["occurrence"],
        )
    )
    assert not response.additional_properties.get("durable_status")
    cold_client = RecordingChatClient()
    cold = CompatibleAgentEntity(
        Agent(client=cold_client),
        state_provider=JsonStateProvider(provider.raw),
        deployment_mode="legacy",
    )
    await cold.run(RunRequest("next", correlation_id="next"))

    def shape(messages: Sequence[Message]) -> list[tuple[str, str, list[str]]]:
        return [(message.role, message.text, [content.type for content in message.contents]) for message in messages]

    assert shape(cold_client.received_messages[0][:-1]) == shape(expected)


@pytest.mark.parametrize(
    "cursor,bridge,explicit",
    [(False, False, False), (True, False, False), (True, True, False), (False, False, True)],
)
async def test_delivery_only_evidence_survives_legacy_fallback(cursor: bool, bridge: bool, explicit: bool) -> None:
    source = _legacy()
    if cursor:
        source["data"]["ingestedPositions"] = {"producer": 0}
    if bridge:
        source["data"]["extensionData"]["_dafx_legacy_ingestion"] = {
            "profile": "dafx-legacy-ingestion-v1",
            "messages": {},
        }
    accepted = Message("user", ["recorded-accepted"], message_id="public").to_dict()
    evidence_calls: list[str] = []

    def evidence(raw: dict[str, Any]) -> LegacyMigrationEvidence:
        evidence_calls.append(state_snapshot_digest(raw))
        journal = {
            "sourceDigest": state_snapshot_digest(raw),
            "evidenceId": "synthetic-accepted-journal",
            "complete": True,
            "messages": [accepted],
        }
        if cursor:
            journal["messagePositions"] = [{"producer": "producer", "position": 0}]
        return LegacyMigrationEvidence(delivery_evidence=journal)

    provider = JsonStateProvider(source)
    client = RecordingChatClient()
    entity = CompatibleAgentEntity(
        Agent(client=client),
        state_provider=provider,
        legacy_migration_evidence=evidence,
    )
    response = await entity.run(
        RunRequest(
            "",
            correlation_id="next",
            context_messages=[accepted],
            context_message_ids=["public"] if explicit else None,
        )
    )
    assert not response.additional_properties.get("durable_status")
    assert provider.raw["schemaVersion"] == "1.1.0"
    assert sum(message.text == "recorded-accepted" for message in client.received_messages[0]) == int(explicit)
    assert evidence_calls == [state_snapshot_digest(source)]


@pytest.mark.parametrize("empty_context", [False, True])
@pytest.mark.parametrize("explicit", [False, True])
async def test_plain_turn_retains_delivery_evidence_for_cold_public_lookup(empty_context: bool, explicit: bool) -> None:
    accepted = Message("user", ["recorded"], message_id="public").to_dict()

    def evidence(raw: dict[str, Any]) -> LegacyMigrationEvidence:
        return LegacyMigrationEvidence(
            delivery_evidence={
                "sourceDigest": state_snapshot_digest(raw),
                "evidenceId": "accepted-journal",
                "complete": True,
                "messages": [accepted],
            }
        )

    provider = JsonStateProvider(_legacy())
    first = CompatibleAgentEntity(
        Agent(client=RecordingChatClient()),
        state_provider=provider,
        legacy_migration_evidence=evidence,
    )
    await first.run(RunRequest("normal", correlation_id="normal", context_messages=[] if empty_context else None))
    client = RecordingChatClient()
    cold = CompatibleAgentEntity(
        Agent(client=client), state_provider=JsonStateProvider(provider.raw), deployment_mode="legacy"
    )
    await cold.run(
        RunRequest(
            "",
            correlation_id="cold",
            context_messages=[accepted],
            context_message_ids=["public"] if explicit else None,
        )
    )
    assert sum(message.text == "recorded" for message in client.received_messages[0]) == int(explicit)


@pytest.mark.parametrize("opaque", [None, {}, 42])
@pytest.mark.parametrize("empty_context", [False, True])
async def test_profile_promotion_rejects_opaque_public_lookup_sibling_before_execution(
    opaque: Any, empty_context: bool
) -> None:
    source = _legacy()
    source["data"]["extensionData"]["_dafx_legacy_ingestion"] = {
        "profile": "dafx-legacy-ingestion-v1",
        "messages": {},
        "publicMessages": opaque,
    }
    provider = JsonStateProvider(source)
    client = RecordingChatClient()

    def evidence(raw: dict[str, Any]) -> LegacyMigrationEvidence:
        return LegacyMigrationEvidence(
            delivery_evidence={
                "sourceDigest": state_snapshot_digest(raw),
                "evidenceId": "accepted-journal",
                "complete": True,
                "messages": [Message("user", ["recorded"], message_id="public").to_dict()],
            }
        )

    entity = CompatibleAgentEntity(Agent(client=client), state_provider=provider, legacy_migration_evidence=evidence)
    with pytest.raises(ValueError, match="publicMessages"):
        await entity.run(RunRequest("normal", correlation_id="normal", context_messages=[] if empty_context else None))
    assert provider.raw == source and provider.attempted_writes == 0 and client.received_messages == []


async def test_empty_journal_does_not_activate_or_replace_opaque_public_lookup_sibling() -> None:
    source = _legacy()
    profile = {"profile": "dafx-legacy-ingestion-v1", "messages": {}, "publicMessages": 42}
    source["data"]["extensionData"]["_dafx_legacy_ingestion"] = profile
    provider = JsonStateProvider(source)
    entity = CompatibleAgentEntity(
        Agent(client=RecordingChatClient()),
        state_provider=provider,
        legacy_migration_evidence=lambda raw: LegacyMigrationEvidence(
            delivery_evidence={
                "sourceDigest": state_snapshot_digest(raw),
                "evidenceId": "empty-journal",
                "complete": True,
                "messages": [],
            }
        ),
    )
    await entity.run(RunRequest("", correlation_id="empty", context_messages=[]))
    assert provider.raw["data"]["extensionData"]["_dafx_legacy_ingestion"] == profile


class _IterationOptions(ChatOptions[None]):
    _agent_loop_iteration: str


@pytest.mark.parametrize("profile", [None, False, 0, "", [], {}])
@pytest.mark.parametrize("mode", [None, "legacy"])
async def test_present_invalid_ingestion_profile_rejects_before_execution(profile: Any, mode: str | None) -> None:
    source = _legacy()
    source["data"]["extensionData"]["_dafx_legacy_ingestion"] = profile
    provider = JsonStateProvider(source)
    client = RecordingChatClient()
    entity = CompatibleAgentEntity(Agent(client=client), state_provider=provider, deployment_mode=mode)
    with pytest.raises(ValueError, match="ingestion metadata"):
        await entity.run(RunRequest("", correlation_id="next", context_messages=[]))
    assert provider.raw == source and provider.attempted_writes == 0 and client.received_messages == []


@pytest.mark.parametrize("operation", ["plain", "reset", "convert"])
async def test_null_ingestion_profile_is_not_absence_on_other_mutating_paths(operation: str) -> None:
    source = _legacy(used=operation != "convert")
    source["data"]["extensionData"]["_dafx_legacy_ingestion"] = None
    provider = JsonStateProvider(source)
    client = RecordingChatClient()
    entity = CompatibleAgentEntity(Agent(client=client), state_provider=provider)
    with pytest.raises(ValueError, match="ingestion metadata"):
        if operation == "reset":
            entity.reset()
        else:
            await entity.run(RunRequest("normal", correlation_id="next"))
    assert provider.raw == source and provider.attempted_writes == 0 and client.received_messages == []


@pytest.mark.parametrize("version", [None, "v1", "v2"])
async def test_absent_or_declared_empty_ingestion_profile_remains_usable(version: str | None) -> None:
    source = _legacy()
    if version is not None:
        source["data"]["extensionData"]["_dafx_legacy_ingestion"] = {
            "profile": f"dafx-legacy-ingestion-{version}",
            "messages": {},
        }
    provider = JsonStateProvider(source)
    client = RecordingChatClient()
    entity = CompatibleAgentEntity(Agent(client=client), state_provider=provider, deployment_mode="legacy")
    result = await entity.run(RunRequest("", correlation_id="next", context_messages=[]))
    assert not result.additional_properties.get("durable_status")
    assert provider.successful_writes == 1 and len(client.received_messages) == 1


@pytest.mark.parametrize("once", [False, True])
async def test_legacy_replacement_preserves_configured_iteration_persistence_cadence(once: bool) -> None:
    reference_history = InMemoryHistoryProvider()
    configured_history = InMemoryHistoryProvider()
    reference_history.after_run_once_per_turn = once
    configured_history.after_run_once_per_turn = once
    reference = Agent(client=RecordingChatClient(), context_providers=[reference_history])
    session = reference.create_session()
    await reference.run("iteration", session=session, options=_IterationOptions(_agent_loop_iteration="turn"))
    expected = session.state[reference_history.source_id].get("messages", [])
    provider = JsonStateProvider(_legacy(used=False))
    entity = CompatibleAgentEntity(
        Agent(client=RecordingChatClient(), context_providers=[configured_history]),
        state_provider=provider,
        deployment_mode="legacy",
    )
    await entity.run(RunRequest("iteration", correlation_id="iteration", options={"_agent_loop_iteration": "turn"}))
    actual = [
        message.to_chat_message().text
        for entry in DurableAgentState.from_dict(provider.raw).data.conversation_history
        for message in entry.messages
    ]
    assert actual == [message.text for message in expected]
    assert configured_history.after_run_once_per_turn is once


class _SessionAgent:
    name: str | None = "session-only"
    id = "session-only"
    description: str | None = "Session-capable SupportsAgentRun implementation without a context pipeline."

    def __init__(self, *, null_providers: bool) -> None:
        if null_providers:
            self.context_providers = None
        self.received: list[list[Message]] = []

    def create_session(self, *, session_id: str | None = None) -> AgentSession:
        return AgentSession(session_id=session_id)

    def get_session(self, service_session_id: str | ServiceSessionId, *, session_id: str | None = None) -> AgentSession:
        return AgentSession(service_session_id=service_session_id, session_id=session_id)

    def run(self, messages: Any = None, **kwargs: Any) -> Any:
        if kwargs.get("stream"):
            raise TypeError("stream is not supported")

        async def complete() -> AgentResponse[Any]:
            self.received.append(list(messages or []))
            return AgentResponse(messages=[Message("assistant", ["done"])])

        return complete()


@pytest.mark.parametrize("null_providers", [False, True])
async def test_session_without_pipeline_gets_legacy_history(null_providers: bool) -> None:
    agent = _SessionAgent(null_providers=null_providers)
    entity = CompatibleAgentEntity(
        agent,
        state_provider=JsonStateProvider(_legacy()),
        deployment_mode="legacy",
    )
    response = await entity.run(RunRequest("next", correlation_id="next"))
    assert not response.additional_properties.get("durable_status")
    assert [message.text for message in agent.received[0]] == ["old question", "old answer", "next"]
