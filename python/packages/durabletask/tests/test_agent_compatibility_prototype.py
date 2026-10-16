# Copyright (c) Microsoft. All rights reserved.

"""Compatibility spike exercised through both real registered host entry points."""

import json
from copy import deepcopy
from typing import Any
from unittest.mock import Mock

import pytest
from _execution_test_support import JsonStateProvider, RecordingChatClient
from agent_framework import Agent
from agent_framework_azurefunctions import AgentFunctionApp
from agent_framework_azurefunctions._entities import create_agent_entity
from durabletask.entities import EntityContext, EntityInstanceId
from durabletask.internal.entity_state_shim import StateShim
from durabletask.serialization import JsonDataConverter

from agent_framework_durabletask import (
    CompatibleAgentEntity,
    DurableAIAgentWorker,
    LegacyMigrationEvidence,
    RunRequest,
    read_agent_state,
    state_snapshot_digest,
)

ENTITY = "dafx-prototype"
NOW = "2026-01-01T00:00:00+00:00"


def _legacy(*, used: bool = True, version: str = "1.1.0") -> dict[str, Any]:
    history = [
        {
            "$type": "request",
            "correlationId": "old",
            "createdAt": NOW,
            "messages": [{"role": "user", "contents": [{"$type": "text", "text": "old question"}]}],
        },
        {
            "$type": "response",
            "correlationId": "old",
            "createdAt": NOW,
            "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "old answer"}]}],
        },
    ]
    return {
        "schemaVersion": version,
        "futureRoot": {"keep": [False, 0, 1.0, None]},
        "data": {
            "conversationHistory": history if used else [],
            "extensionData": {"keep": {"nested": True}},
            "futureData": {"type": "message", "opaque": [None, 0]},
        },
    }


def _evidence(source: dict[str, Any]) -> LegacyMigrationEvidence:
    return LegacyMigrationEvidence(
        completion_evidence={
            "sourceDigest": state_snapshot_digest(source),
            "evidenceId": "trusted-completions",
            "complete": True,
            "results": [
                {
                    "correlationId": "old",
                    "outcome": "succeeded",
                    "completedAt": NOW,
                    "response": {
                        "messages": [
                            {
                                "role": "assistant",
                                "contents": [{"$type": "text", "text": "old answer"}],
                            }
                        ]
                    },
                }
            ],
        }
    )


class _FunctionsContext:
    def __init__(self, raw: dict[str, Any] | None) -> None:
        self.encoded = json.dumps(raw) if raw is not None else None
        self.operation_name = "run"
        self.entity_name = ENTITY
        self.entity_key = "session"
        self.request = RunRequest("new question", correlation_id="next").to_dict()
        self.result: Any = None
        self.writes = 0

    def get_state(self, default: Any = None) -> Any:
        return json.loads(self.encoded) if self.encoded is not None else default()

    def set_state(self, state: dict[str, Any]) -> None:
        self.writes += 1
        self.encoded = json.dumps(state)

    def get_input(self) -> Any:
        return self.request

    def set_result(self, result: Any) -> None:
        self.result = result


class _Host:
    def __init__(
        self,
        kind: str,
        source: dict[str, Any] | None,
        *,
        mode: str | None = None,
        evidence: Any = None,
        agent_override: Agent | None = None,
    ) -> None:
        self.client = RecordingChatClient()
        agent = agent_override or Agent(client=self.client, name="prototype")
        if agent_override is not None:
            assert isinstance(agent_override.client, RecordingChatClient)
            self.client = agent_override.client
        self.kind = kind
        if kind == "native":
            native = Mock()
            wrapper = DurableAIAgentWorker(
                native,
                deployment_mode=mode,
                legacy_migration_evidence=evidence,
                max_state_bytes=None,
            )
            wrapper.add_agent(agent)
            entity_class = native.add_entity.call_args.args[0]
            converter = JsonDataConverter()
            self.shim = StateShim(json.dumps(source) if source is not None else None, converter, is_serialized=True)
            context = EntityContext(
                "orchestration",
                "operation",
                self.shim,
                EntityInstanceId(ENTITY, "session"),
                converter,
            )
            self.entity = entity_class()
            self.entity._initialize_entity_context(context)
        else:
            self.context = _FunctionsContext(source)
            self.handler = create_agent_entity(
                agent,
                deployment_mode=mode,
                legacy_migration_evidence=evidence,
                max_state_bytes=None,
            )

    def run(self, correlation: str = "next") -> Any:
        request = RunRequest("new question", correlation_id=correlation).to_dict()
        if self.kind == "native":
            return self.entity.run(request)
        self.context.request = request
        self.handler(self.context)
        return self.context.result

    def raw(self) -> dict[str, Any]:
        encoded = self.shim.encode_state() if self.kind == "native" else self.context.encoded
        assert encoded is not None
        return json.loads(encoded)


@pytest.mark.parametrize("kind", ["native", "functions"])
@pytest.mark.parametrize(("mode", "version"), [(None, "2.0.0"), ("legacy", "1.1.0")])
def test_new_sessions_follow_writer_policy_without_startup_acknowledgement(
    monkeypatch: pytest.MonkeyPatch, kind: str, mode: str | None, version: str
) -> None:
    monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    host = _Host(kind, None, mode=mode)
    host.run()
    assert host.raw()["schemaVersion"] == version
    assert len(host.client.received_messages) == 1


@pytest.mark.parametrize("kind", ["native", "functions"])
@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
def test_used_legacy_without_evidence_continues_and_preserves_opaque_state(
    monkeypatch: pytest.MonkeyPatch, kind: str, version: str
) -> None:
    monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    source = _legacy(version=version)
    before = deepcopy(source)
    host = _Host(kind, source)
    host.run()
    actual = host.raw()
    assert actual["schemaVersion"] == version
    assert actual["futureRoot"] == before["futureRoot"]
    assert actual["data"]["futureData"] == before["data"]["futureData"]
    assert actual["data"]["extensionData"]["keep"] == before["data"]["extensionData"]["keep"]
    assert actual["data"]["extensionData"]["_dafx_legacy_owner"]["owner"] == "entity"
    assert actual["data"]["conversationHistory"][:2] == before["data"]["conversationHistory"]
    assert "terminalResults" not in actual["data"]
    assert [message.text for message in host.client.received_messages[0]] == [
        "old question",
        "old answer",
        "new question",
    ]
    cold = _Host(kind, actual)
    cold.run()
    assert cold.client.received_messages == []
    assert cold.raw() == actual
    assert source == before


@pytest.mark.parametrize("kind", ["native", "functions"])
@pytest.mark.parametrize("used", [False, True])
def test_automatic_migration_and_turn_commit_then_cold_duplicates_do_not_execute(
    monkeypatch: pytest.MonkeyPatch, kind: str, used: bool
) -> None:
    monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    source = _legacy(used=used)
    host = _Host(kind, source, evidence=_evidence if used else None)
    host.run()
    actual = host.raw()
    assert actual["schemaVersion"] == "2.0.0"
    assert actual["data"]["migration"]["sourceDigest"] == state_snapshot_digest(source)
    assert actual["data"]["migration"]["mode"] == "in_place"
    assert actual["data"]["migration"]["sourceEntitySessionId"] == actual["data"]["migration"]["destinationSessionId"]
    expected = {"next", "old"} if used else {"next"}
    assert set(actual["data"]["completionReceipts"]) == expected
    if kind == "functions":
        assert host.context.writes == 1
    cold = _Host(kind, actual)
    cold.run()
    assert cold.client.received_messages == []
    assert cold.raw() == actual
    if used:
        prior = read_agent_state(actual).try_get_agent_response("old")
        assert prior is not None
        assert prior.text == "old answer"


@pytest.mark.parametrize("kind", ["native", "functions"])
def test_legacy_mode_does_not_call_migration_resolver(kind: str) -> None:
    resolver = Mock(side_effect=AssertionError("Legacy mode must not request migration evidence"))
    host = _Host(kind, _legacy(), mode="legacy", evidence=resolver)
    host.run()
    assert host.raw()["schemaVersion"] == "1.1.0"
    resolver.assert_not_called()


@pytest.mark.parametrize("kind", ["native", "functions"])
def test_legacy_mode_reads_v2_duplicates_but_rejects_new_v2_writes(kind: str) -> None:
    seed = _Host(kind, None, mode="isolated_v2")
    seed.run()
    committed = seed.raw()
    cold = _Host(kind, committed, mode="legacy")
    cold.run()
    assert cold.client.received_messages == []
    if kind == "native":
        with pytest.raises(ValueError, match="cannot write existing schema-2"):
            cold.run("different")
    else:
        assert cold.run("different")["status"] == "error"
    assert cold.raw() == committed
    assert cold.client.received_messages == []


@pytest.mark.parametrize("kind", ["native", "functions"])
def test_bad_migration_evidence_never_falls_back_or_executes(kind: str) -> None:
    def bad(source: dict[str, Any]) -> LegacyMigrationEvidence:
        evidence = _evidence(source)
        assert evidence.completion_evidence is not None
        evidence.completion_evidence["complete"] = False
        return evidence

    source = _legacy()
    host = _Host(kind, source, evidence=bad)
    if kind == "native":
        with pytest.raises(ValueError, match="complete=True"):
            host.run()
    else:
        assert host.run()["status"] == "error"
    assert host.client.received_messages == []
    assert host.raw() == source


@pytest.mark.parametrize("kind", ["native", "functions"])
@pytest.mark.parametrize(
    "source",
    [
        {"data": {"conversationHistory": []}},
        {"schemaVersion": "3.0.0", "data": {"conversationHistory": []}},
        {"schemaVersion": "1.1.0", "data": []},
    ],
)
def test_malformed_state_is_not_reinitialized(kind: str, source: dict[str, Any]) -> None:
    host = _Host(kind, source)
    if kind == "native":
        with pytest.raises(ValueError):
            host.run()
    else:
        assert host.run()["status"] == "error"
    assert host.client.received_messages == []
    assert host.raw() == source


@pytest.mark.parametrize("acknowledged", [False, True])
@pytest.mark.asyncio
async def test_migration_setter_failure_and_unknown_acknowledgement_are_retry_safe(acknowledged: bool) -> None:
    class FaultingProvider(JsonStateProvider):
        def _set_state_dict(self, state: dict[str, Any]) -> None:
            if acknowledged:
                super()._set_state_dict(state)
            raise OSError("commit acknowledgement failed")

    source = _legacy()
    provider = FaultingProvider(source, session_id="session")
    client = RecordingChatClient()
    entity = CompatibleAgentEntity(
        Agent(client=client, name="prototype"),
        state_provider=provider,
        legacy_migration_evidence=_evidence,
    )
    request = RunRequest("new question", correlation_id="next")
    with pytest.raises(OSError, match="acknowledgement"):
        await entity.run(request)
    if acknowledged:
        raw = provider._get_state_dict()
        cold_client = RecordingChatClient()
        cold = CompatibleAgentEntity(
            Agent(client=cold_client, name="prototype"),
            state_provider=JsonStateProvider(raw, session_id="session"),
        )
        await cold.run(request)
        assert cold_client.received_messages == []
    else:
        assert provider._get_state_dict() == source


def test_function_app_resolves_the_same_prototype_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    assert AgentFunctionApp(enable_health_check=False)._deployment_mode == "isolated_v2"
    monkeypatch.setenv("DURABLE_AGENTS_DEPLOYMENT_MODE", "legacy")
    assert AgentFunctionApp(enable_health_check=False)._deployment_mode == "legacy"


@pytest.mark.parametrize("kind", ["native", "functions"])
def test_legacy_reset_and_cleanup_preserve_the_profile_and_unrelated_state(kind: str) -> None:
    source = _legacy()
    host = _Host(kind, source, mode="legacy")
    if kind == "native":
        assert host.entity.expire_responses() == 0
    else:
        host.context.operation_name = "expire_responses"
        host.handler(host.context)
        assert host.context.result == {"expired": 0}
        assert host.context.writes == 0
    assert host.raw() == source
    if kind == "native":
        host.entity.reset()
    else:
        host.context.operation_name = "reset"
        host.handler(host.context)
    expected = deepcopy(source)
    expected["data"]["conversationHistory"] = []
    assert host.raw() == expected
    assert host.client.received_messages == []


@pytest.mark.parametrize("kind", ["native", "functions"])
def test_cleanup_does_not_initialize_an_absent_entity(kind: str) -> None:
    host = _Host(kind, None, mode="legacy")
    if kind == "native":
        assert host.entity.expire_responses() == 0
        assert host.shim.encode_state() is None
    else:
        host.context.operation_name = "expire_responses"
        host.handler(host.context)
        assert host.context.encoded is None
        assert host.context.writes == 0


@pytest.mark.parametrize("kind", ["native", "functions"])
def test_evidence_resolver_cannot_mutate_the_legacy_source(kind: str) -> None:
    def defer(source: dict[str, Any]) -> None:
        source["data"]["conversationHistory"].clear()
        source["futureRoot"].clear()

    source = _legacy()
    host = _Host(kind, source, evidence=defer)
    host.run()
    assert host.raw()["futureRoot"] == source["futureRoot"]
    assert host.raw()["data"]["conversationHistory"][:2] == source["data"]["conversationHistory"]


@pytest.mark.asyncio
async def test_legacy_capacity_rejection_preserves_state_without_pruning() -> None:
    source = _legacy()
    provider = JsonStateProvider(source)
    client = RecordingChatClient()
    entity = CompatibleAgentEntity(
        Agent(client=client, name="prototype"),
        state_provider=provider,
        deployment_mode="legacy",
        max_state_bytes=50,
    )
    from agent_framework_durabletask import StateCapacityError

    with pytest.raises(StateCapacityError):
        await entity.run(RunRequest("new question", correlation_id="next"))
    assert client.received_messages == []
    assert provider.raw == source
    assert provider.attempted_writes == 0


@pytest.mark.asyncio
async def test_legacy_model_failure_is_persisted_and_cold_duplicate_does_not_execute() -> None:
    provider = JsonStateProvider(_legacy())
    client = RecordingChatClient(fail=True)
    entity = CompatibleAgentEntity(
        Agent(client=client, name="prototype"),
        state_provider=provider,
        deployment_mode="legacy",
    )
    request = RunRequest("new question", correlation_id="next")
    response = await entity.run(request)
    assert response.additional_properties["durable_status"] == "error"
    assert provider.raw["schemaVersion"] == "1.1.0"
    cold_client = RecordingChatClient()
    cold = CompatibleAgentEntity(
        Agent(client=cold_client, name="prototype"),
        state_provider=JsonStateProvider(provider.raw),
        deployment_mode="legacy",
    )
    replay = await cold.run(request)
    assert replay.messages[0].contents[0].type == "error"
    assert cold_client.received_messages == []
