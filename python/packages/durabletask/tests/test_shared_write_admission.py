# Copyright (c) Microsoft. All rights reserved.

"""Offline contracts for explicit legacy models and canonical v2 host write admission."""

from collections.abc import AsyncIterator
from copy import deepcopy
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from agent_framework import AgentResponse, AgentResponseUpdate, Content, Message, ResponseStream

from agent_framework_durabletask import (
    AgentEntity,
    AgentEntityStateProviderMixin,
    DurableAgentState,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
    DurableAIAgentWorker,
    LegacyDurableAgentState,
    RunRequest,
)
from agent_framework_durabletask._entities import DurableTaskEntityStateProvider
from agent_framework_durabletask._response_utils import load_agent_response
from agent_framework_durabletask._shared_state_validation import validate_shared_state

LEGACY_VERSIONS = ("1.0.0", "1.1.0", "1.2.0")
LEGACY_REJECTED_VERSIONS = [
    pytest.param(None, id="null"),
    pytest.param(False, id="false"),
    pytest.param(0, id="zero"),
    pytest.param(2, id="integer"),
    pytest.param([], id="list"),
    pytest.param({}, id="object"),
    "",
    "1",
    "1.1",
    "1.0.1",
    "1.1.1",
    "1.2.1",
    "1.3.0",
    "01.1.0",
    "1.1.0 ",
    "v1.1.0",
    "1.1.0-preview",
    "1.1.0+build",
    "2.0.0",
    "2.0.1",
    "3.0.0",
]
INVALID_BACKING = [
    pytest.param(False, id="false"),
    pytest.param(0, id="zero"),
    pytest.param([], id="empty-list"),
    pytest.param([{"data": {}}], id="nonempty-list"),
    pytest.param("", id="empty-string"),
    pytest.param({"data": {"conversationHistory": []}}, id="missing-version"),
    pytest.param({"schemaVersion": "1.1.0"}, id="missing-data"),
    pytest.param({"schemaVersion": "1.1.0", "data": None}, id="null-data"),
    pytest.param({"schemaVersion": "1.1.0", "data": False}, id="false-data"),
    pytest.param({"schemaVersion": "1.1.0", "data": []}, id="list-data"),
    pytest.param({"schemaVersion": "1.1.0", "data": "invalid"}, id="string-data"),
    pytest.param({"schemaVersion": "2.0.0"}, id="v2-missing-data"),
    pytest.param({"schemaVersion": "2.0.0", "data": None}, id="v2-null-data"),
    pytest.param({"schemaVersion": "2.0.0", "data": False}, id="v2-false-data"),
    pytest.param({"schemaVersion": "2.0.0", "data": []}, id="v2-list-data"),
    pytest.param({"schemaVersion": "2.0.0", "data": "invalid"}, id="v2-string-data"),
    pytest.param({"schemaVersion": "2.0.0", "data": {}}, id="v2-missing-collections"),
    pytest.param({"schemaVersion": "3.0.0", "data": {}}, id="future-version"),
]
SHARED_KINDS = ("empty", "succeeded", "failed", "unavailable")
REQUEST_KINDS = ("new", "duplicate", "typed", "json", "invalid-json", "missing-correlation")
V2_MAPS = {"terminalResults", "completionReceipts"}
PRIVATE_MAPS = {"responseMailbox", "completedCorrelations"}


def _canonical_state(kind: str) -> dict[str, Any]:
    # Literal canonical wire shape, independent of mutable models and codec producers.
    raw: dict[str, Any] = {
        "schemaVersion": "2.0.0",
        "data": {"conversationHistory": [], "terminalResults": {}, "completionReceipts": {}},
    }
    if kind == "empty":
        return raw
    outcome = "failed" if kind == "failed" else "succeeded"
    raw["data"]["completionReceipts"]["completed"] = {
        "correlationId": "completed",
        "outcome": outcome,
        "completedAt": "2026-09-16T11:00:00Z",
        "resultState": "unavailable" if kind == "unavailable" else "available",
    }
    if kind == "unavailable":
        raw["data"]["completionReceipts"]["completed"]["resultUnavailableAt"] = "2026-09-16T12:00:00Z"
    else:
        result: dict[str, Any] = {
            "correlationId": "completed",
            "outcome": outcome,
            "completedAt": "2026-09-16T11:00:00Z",
            "response": {
                "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "retained result"}]}],
            },
        }
        if kind == "failed":
            result["error"] = {"code": "provider_failure", "message": "Retained failure."}
        raw["data"]["terminalResults"]["completed"] = result
    return raw


def _legacy_state(version: str = "1.1.0", *, populated: bool = False) -> dict[str, Any]:
    history = (
        [
            {
                "$type": "request",
                "correlationId": "previous",
                "createdAt": "2026-09-16T10:00:00Z",
                "messages": [{"role": "user", "contents": [{"$type": "text", "text": "previous question"}]}],
            },
            {
                "$type": "response",
                "correlationId": "previous",
                "createdAt": "2026-09-16T10:01:00Z",
                "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": "previous answer"}]}],
            },
        ]
        if populated
        else []
    )
    return LegacyDurableAgentState.from_dict({
        "schemaVersion": version,
        "data": {"conversationHistory": history},
    }).to_dict()


def _request(kind: str) -> Any:
    if kind == "typed":
        return RunRequest(message="question", correlation_id="new")
    if kind == "json":
        return '{"message":"question","correlationId":"new"}'
    if kind == "invalid-json":
        return "{not json"
    if kind == "missing-correlation":
        return {"message": "question"}
    return {"message": "question", "correlationId": "completed" if kind == "duplicate" else "new"}


class MockSupportsAgentRun:
    """Small agent double, not a core Agent or an external model provider."""

    name = "admission-agent"

    def __init__(self, scenario: str = "success") -> None:
        self.scenario = scenario
        self.run_calls = Mock()
        self.model_calls = Mock()
        self.response = AgentResponse(
            messages=[Message(role="assistant", contents=[Content.from_text(text="writer response")])],
            created_at="2026-09-16T12:00:00Z",
        )

    def run(self, *args: Any, stream: bool = False, **kwargs: Any) -> Any:
        self.run_calls(*args, stream=stream, **kwargs)
        if stream:
            if self.scenario != "streaming":
                raise TypeError("streaming not supported")
            return ResponseStream(self._updates(**kwargs), finalizer=AgentResponse.from_updates)
        return self._complete(**kwargs)

    async def _complete(self, **kwargs: Any) -> AgentResponse:
        self.model_calls(**kwargs)
        if self.scenario == "failure":
            raise RuntimeError("offline model failure")
        return self.response

    async def _updates(self, **kwargs: Any) -> AsyncIterator[AgentResponseUpdate]:
        self.model_calls(**kwargs)
        yield AgentResponseUpdate(contents=[Content.from_text(text="writer ")])
        yield AgentResponseUpdate(contents=[Content.from_text(text="response")])


class RecordingCallback:
    def __init__(self) -> None:
        self.on_streaming_response_update = AsyncMock()
        self.on_agent_response = AsyncMock()


class BackingStore:
    def __init__(self, raw: Any) -> None:
        self.raw = deepcopy(raw)
        self.get_state = Mock(side_effect=lambda *args, **kwargs: self.raw if self.raw is not None else {})
        self.set_state = Mock(side_effect=self._write)

    def _write(self, value: dict[str, Any]) -> None:
        self.raw = deepcopy(value)


class InMemoryStateProvider(AgentEntityStateProviderMixin):
    def __init__(self, backing: BackingStore) -> None:
        self.backing = backing

    def _get_state_dict(self) -> dict[str, Any]:
        return self.backing.get_state()

    def _set_state_dict(self, state: dict[str, Any]) -> None:
        self.backing.set_state(state)

    def _get_session_id_from_entity(self) -> str:
        return "session"


def _host(
    monkeypatch: pytest.MonkeyPatch, surface: str, raw: Any, scenario: str = "success"
) -> tuple[Any, AgentEntityStateProviderMixin, BackingStore, Any, RecordingCallback]:
    backing = BackingStore(raw)
    agent: Any = MockSupportsAgentRun(scenario)
    callback = RecordingCallback()
    if surface == "plain":
        provider = InMemoryStateProvider(backing)
        return AgentEntity(agent, callback, state_provider=provider), provider, backing, agent, callback

    grpc_worker = Mock()
    grpc_worker.add_entity.return_value = "dafx-admission-agent"
    DurableAIAgentWorker(grpc_worker, callback=callback, deployment_mode="isolated_v2").add_agent(agent)
    grpc_worker.add_entity.assert_called_once()
    entity_class = grpc_worker.add_entity.call_args.args[0]
    assert issubclass(entity_class, DurableTaskEntityStateProvider)
    # Keep the actual configured run/reset methods and their MRO. Only replace SDK storage/context.
    monkeypatch.setattr(entity_class, "get_state", backing.get_state)
    monkeypatch.setattr(entity_class, "set_state", backing.set_state)
    context = SimpleNamespace(entity_id=SimpleNamespace(name="dafx-admission-agent", key="session"))
    monkeypatch.setattr(entity_class, "entity_context", property(lambda self: context), raising=False)
    entity = entity_class()
    return entity, entity, backing, agent, callback


def _blocked_execution_spies(monkeypatch: pytest.MonkeyPatch) -> list[Mock]:
    spies = []
    for owner, name in (
        (RunRequest, "from_dict"),
        (RunRequest, "from_json"),
        (DurableAgentStateRequest, "from_run_request"),
        (DurableAgentStateResponse, "from_run_response"),
    ):
        spy = Mock(side_effect=AssertionError(f"{name} ran before write admission"))
        monkeypatch.setattr(owner, name, spy)
        spies.append(spy)
    return spies


def _no_effects(backing: BackingStore, original: Any, snapshot: Any, agent: Any, callback: RecordingCallback) -> None:
    assert backing.raw is original
    assert backing.raw == snapshot
    backing.set_state.assert_not_called()
    agent.run_calls.assert_not_called()
    agent.model_calls.assert_not_called()
    callback.on_streaming_response_update.assert_not_called()
    callback.on_agent_response.assert_not_called()


@pytest.mark.parametrize("version", LEGACY_VERSIONS)
def test_explicit_legacy_state_accepts_exact_legacy_versions(version: str) -> None:
    assert LegacyDurableAgentState(schema_version=version).to_dict() == _legacy_state(version)
    raw = _legacy_state(version, populated=True)
    snapshot = deepcopy(raw)
    state = LegacyDurableAgentState.from_dict(raw)
    assert state.schema_version == version
    assert state.to_dict()["schemaVersion"] == version
    assert state.message_count == 2
    assert V2_MAPS.isdisjoint(state.to_dict()["data"])
    assert raw == snapshot


def test_explicit_legacy_empty_object_and_default_remain_1_1() -> None:
    assert LegacyDurableAgentState.SCHEMA_VERSION == "1.1.0"
    assert LegacyDurableAgentState().to_dict() == _legacy_state()
    assert LegacyDurableAgentState.from_dict({}).to_dict() == _legacy_state()


@pytest.mark.parametrize("version", LEGACY_REJECTED_VERSIONS)
@pytest.mark.parametrize("entry_point", ["init", "from_dict", "to_dict"])
def test_explicit_legacy_state_rejects_shared_future_and_invalid_versions(version: Any, entry_point: str) -> None:
    if entry_point == "init":
        with pytest.raises(ValueError):
            LegacyDurableAgentState(schema_version=version)
    elif entry_point == "from_dict":
        raw = _canonical_state("succeeded") if version == "2.0.0" else {"schemaVersion": version, "data": {}}
        snapshot = deepcopy(raw)
        with pytest.raises(ValueError):
            LegacyDurableAgentState.from_dict(raw)
        assert raw == snapshot
    else:
        state = LegacyDurableAgentState()
        state.schema_version = version
        with pytest.raises(ValueError):
            state.to_dict()
        assert state.schema_version is version


@pytest.mark.parametrize("raw", INVALID_BACKING)
def test_explicit_legacy_loader_rejects_malformed_existing_state(raw: Any) -> None:
    snapshot = deepcopy(raw)
    with pytest.raises(ValueError):
        LegacyDurableAgentState.from_dict(raw)
    assert raw == snapshot


@pytest.mark.parametrize("kind", SHARED_KINDS)
def test_explicit_legacy_loader_rejects_every_canonical_v2_shape(kind: str) -> None:
    raw = _canonical_state(kind)
    snapshot = deepcopy(raw)
    with pytest.raises(ValueError):
        LegacyDurableAgentState.from_dict(raw)
    assert raw == snapshot


def test_public_mutable_state_defaults_to_canonical_v2() -> None:
    assert DurableAgentState is not LegacyDurableAgentState
    assert DurableAgentState.SCHEMA_VERSION == "2.0.0"
    assert DurableAgentState().to_dict() == _canonical_state("empty")


@pytest.mark.parametrize("kind", SHARED_KINDS)
def test_public_mutable_loader_preserves_every_canonical_v2_shape(kind: str) -> None:
    raw = _canonical_state(kind)
    snapshot = deepcopy(raw)
    state = DurableAgentState.from_dict(raw)
    assert state.schema_version == "2.0.0"
    assert state.to_dict() == snapshot
    raw["data"]["conversationHistory"].append({"$type": "compaction"})
    exported = state.to_dict()
    exported["data"]["completionReceipts"].clear()
    assert state.to_dict() == snapshot


@pytest.mark.parametrize("surface", ["plain", "registered"])
@pytest.mark.parametrize("kind", SHARED_KINDS)
def test_mutable_state_getter_accepts_canonical_v2(monkeypatch: pytest.MonkeyPatch, surface: str, kind: str) -> None:
    entity, provider, backing, agent, callback = _host(monkeypatch, surface, _canonical_state(kind))
    original, snapshot = backing.raw, deepcopy(backing.raw)
    state = entity.state
    assert isinstance(state, DurableAgentState)
    assert provider._state_cache is state
    assert state.to_dict() == snapshot
    assert provider._persisted_state_snapshot == snapshot
    assert provider._persisted_state_snapshot is not backing.raw
    _no_effects(backing, original, snapshot, agent, callback)


@pytest.mark.parametrize("surface", ["plain", "registered"])
@pytest.mark.parametrize("version", LEGACY_VERSIONS)
@pytest.mark.parametrize("populated", [False, True])
@pytest.mark.parametrize("request_kind", REQUEST_KINDS)
@pytest.mark.parametrize("cache_kind", ["cold", "loaded-v2", "altered-v2"])
async def test_run_rejects_legacy_before_parsing_execution_callbacks_or_error_recording(
    monkeypatch: pytest.MonkeyPatch,
    surface: str,
    version: str,
    populated: bool,
    request_kind: str,
    cache_kind: str,
) -> None:
    entity, provider, backing, agent, callback = _host(monkeypatch, surface, _canonical_state("empty"))
    if cache_kind != "cold":
        cached = entity.state
        if cache_kind == "altered-v2":
            cached.schema_version = "1.0.0"
    backing.raw = _legacy_state(version, populated=populated)
    assert V2_MAPS.isdisjoint(backing.raw["data"])
    original, snapshot = backing.raw, deepcopy(backing.raw)
    cache = provider._state_cache
    cache_snapshot = cache.to_dict() if cache is not None else None
    committed_snapshot = deepcopy(provider._persisted_state_snapshot)
    request = _request(request_kind)
    spies = _blocked_execution_spies(monkeypatch)
    with pytest.raises(ValueError):
        if surface == "plain":
            await entity.run(request)
        else:
            entity.run(request)
    assert provider._state_cache is cache
    assert provider._persisted_state_snapshot == committed_snapshot
    if cache is not None:
        assert cache.to_dict() == cache_snapshot
    for spy in spies:
        spy.assert_not_called()
    _no_effects(backing, original, snapshot, agent, callback)


@pytest.mark.parametrize("surface", ["plain", "registered"])
@pytest.mark.parametrize("operation", ["setter", "persist", "reset"])
@pytest.mark.parametrize("cache_kind", ["cold", "loaded-v2", "altered-v2"])
@pytest.mark.parametrize("version", LEGACY_VERSIONS)
@pytest.mark.parametrize("populated", [False, True])
def test_writes_check_legacy_backing_not_replacement_or_cached_schema(
    monkeypatch: pytest.MonkeyPatch,
    surface: str,
    operation: str,
    cache_kind: str,
    version: str,
    populated: bool,
) -> None:
    entity, provider, backing, agent, callback = _host(monkeypatch, surface, _canonical_state("empty"))
    if cache_kind != "cold":
        cached = entity.state
        if cache_kind == "altered-v2":
            cached.schema_version = "1.0.0"
    # A real canonical cache cannot authorize writes to newly observed legacy backing.
    backing.raw = _legacy_state(version, populated=populated)
    assert V2_MAPS.isdisjoint(backing.raw["data"])
    original, snapshot = backing.raw, deepcopy(backing.raw)
    cache = provider._state_cache
    cache_snapshot = cache.to_dict() if cache is not None else None
    committed_snapshot = deepcopy(provider._persisted_state_snapshot)
    replacement = DurableAgentState()
    replacement.data.extension_data = {"candidate": "must not replace cache"}
    with pytest.raises(ValueError):
        if operation == "setter":
            entity.state = replacement
        elif operation == "persist":
            entity.persist_state()
        else:
            entity.reset()
    assert provider._state_cache is cache
    assert provider._persisted_state_snapshot == committed_snapshot
    if cache is not None:
        assert cache.to_dict() == cache_snapshot
    assert replacement.data.extension_data == {"candidate": "must not replace cache"}
    _no_effects(backing, original, snapshot, agent, callback)


@pytest.mark.parametrize("surface", ["plain", "registered"])
@pytest.mark.parametrize("warm_cache", [False, True])
@pytest.mark.parametrize("version", [*LEGACY_VERSIONS, "3.0.0", "1.1.1", "2.0.1"])
def test_invalid_replacement_does_not_displace_existing_cache_or_write_v2_backing(
    monkeypatch: pytest.MonkeyPatch, surface: str, warm_cache: bool, version: str
) -> None:
    entity, provider, backing, agent, callback = _host(monkeypatch, surface, _canonical_state("succeeded"))
    if warm_cache:
        _ = entity.state
    cache = provider._state_cache
    committed_snapshot = deepcopy(provider._persisted_state_snapshot)
    original, snapshot = backing.raw, deepcopy(backing.raw)
    replacement = DurableAgentState()
    replacement.schema_version = version
    with pytest.raises(ValueError):
        entity.state = replacement
    assert provider._state_cache is cache
    assert provider._persisted_state_snapshot == committed_snapshot
    _no_effects(backing, original, snapshot, agent, callback)


@pytest.mark.parametrize("surface", ["plain", "registered"])
@pytest.mark.parametrize("version", [*LEGACY_VERSIONS, "3.0.0", "1.1.1", "2.0.1"])
def test_persist_rejects_altered_cached_version_and_restores_committed_v2(
    monkeypatch: pytest.MonkeyPatch, surface: str, version: str
) -> None:
    entity, provider, backing, agent, callback = _host(monkeypatch, surface, _canonical_state("succeeded"))
    cache = entity.state
    cache.schema_version = version
    original, snapshot = backing.raw, deepcopy(backing.raw)
    with pytest.raises(ValueError):
        entity.persist_state()
    assert provider._state_cache is not cache
    assert provider.state.to_dict() == snapshot
    assert provider._persisted_state_snapshot == snapshot
    assert cache.schema_version == version
    _no_effects(backing, original, snapshot, agent, callback)


@pytest.mark.parametrize("surface", ["plain", "registered"])
@pytest.mark.parametrize("raw", INVALID_BACKING)
@pytest.mark.parametrize(
    ("operation", "warm_cache"),
    [("getter", False)]
    + [(operation, warm) for operation in ("setter", "persist", "reset", "run") for warm in (False, True)],
)
async def test_malformed_backing_is_never_treated_as_fresh_or_overwritten(
    monkeypatch: pytest.MonkeyPatch, surface: str, raw: Any, operation: str, warm_cache: bool
) -> None:
    # A getter may return an already admitted v2 cache. Write paths must still
    # inspect the actual backing state, not that stale cache.
    entity, provider, backing, agent, callback = _host(monkeypatch, surface, raw)
    if warm_cache:
        backing.raw = _canonical_state("succeeded")
        _ = entity.state
        backing.raw = deepcopy(raw)
    cache = provider._state_cache
    cache_snapshot = cache.to_dict() if cache is not None else None
    committed_snapshot = deepcopy(provider._persisted_state_snapshot)
    original, snapshot = backing.raw, deepcopy(backing.raw)
    spies = _blocked_execution_spies(monkeypatch) if operation == "run" else []
    with pytest.raises(ValueError):
        if operation == "getter":
            _ = entity.state
        elif operation == "setter":
            entity.state = DurableAgentState()
        elif operation == "persist":
            entity.persist_state()
        elif operation == "reset":
            entity.reset()
        elif surface == "plain":
            await entity.run(_request("new"))
        else:
            entity.run(_request("new"))
    assert provider._state_cache is cache
    assert provider._persisted_state_snapshot == committed_snapshot
    if cache is not None:
        assert cache.to_dict() == cache_snapshot
    for spy in spies:
        spy.assert_not_called()
    _no_effects(backing, original, snapshot, agent, callback)


@pytest.mark.parametrize("surface", ["plain", "registered"])
@pytest.mark.parametrize("operation", ["setter", "persist"])
@pytest.mark.parametrize("kind", ["fresh", *SHARED_KINDS])
def test_v2_state_assignment_and_persistence_preserve_matching_committed_baseline(
    monkeypatch: pytest.MonkeyPatch, surface: str, operation: str, kind: str
) -> None:
    raw = {} if kind == "fresh" else _canonical_state(kind)
    entity, provider, backing, agent, callback = _host(monkeypatch, surface, raw)
    candidate = DurableAgentState() if kind == "fresh" else DurableAgentState.from_dict(raw)
    candidate.data.extension_data = {"writer": "supported"}
    if operation == "setter":
        entity.state = candidate
    else:
        entity.state.data.extension_data = {"writer": "supported"}
        entity.persist_state()
    backing.set_state.assert_called_once_with(candidate.to_dict())
    assert backing.raw["schemaVersion"] == "2.0.0"
    assert backing.raw["data"].keys() >= V2_MAPS
    assert PRIVATE_MAPS.isdisjoint(backing.raw["data"])
    validate_shared_state(backing.raw)
    assert provider.state.to_dict() == backing.raw
    assert provider._persisted_state_snapshot == backing.raw
    assert provider._persisted_state_snapshot is not backing.raw
    agent.run_calls.assert_not_called()
    agent.model_calls.assert_not_called()
    callback.on_agent_response.assert_not_called()
    callback.on_streaming_response_update.assert_not_called()


@pytest.mark.parametrize("surface", ["plain", "registered"])
@pytest.mark.parametrize("kind", SHARED_KINDS)
@pytest.mark.parametrize("warm_cache", [False, True])
def test_persist_without_edits_preserves_canonical_backing_and_committed_baseline(
    monkeypatch: pytest.MonkeyPatch, surface: str, kind: str, warm_cache: bool
) -> None:
    raw = _canonical_state(kind)
    raw["data"]["extensionData"] = {"committed": [False, 0]}
    entity, provider, backing, agent, callback = _host(monkeypatch, surface, raw)
    if warm_cache:
        _ = entity.state
    entity.persist_state()
    backing.set_state.assert_called_once_with(raw)
    assert backing.raw == raw
    assert provider.state.to_dict() == raw
    assert provider._persisted_state_snapshot == raw
    agent.run_calls.assert_not_called()
    agent.model_calls.assert_not_called()
    callback.on_agent_response.assert_not_called()
    callback.on_streaming_response_update.assert_not_called()


@pytest.mark.parametrize("surface", ["plain", "registered"])
@pytest.mark.parametrize("operation", ["setter", "persist"])
@pytest.mark.parametrize("kind", [kind for kind in SHARED_KINDS if kind != "empty"])
def test_v2_writes_cannot_discard_committed_completions(
    monkeypatch: pytest.MonkeyPatch, surface: str, operation: str, kind: str
) -> None:
    raw = _canonical_state(kind)
    entity, provider, backing, agent, callback = _host(monkeypatch, surface, raw)
    original, snapshot = backing.raw, deepcopy(backing.raw)
    if operation == "persist":
        entity.state.data.response_mailbox.clear()
        entity.state.data.completed_correlations.clear()
    with pytest.raises(ValueError):
        if operation == "setter":
            entity.state = DurableAgentState()
        else:
            entity.persist_state()
    assert provider.state.to_dict() == snapshot
    assert provider._persisted_state_snapshot == snapshot
    _no_effects(backing, original, snapshot, agent, callback)


@pytest.mark.parametrize("surface", ["plain", "registered"])
@pytest.mark.parametrize("populated", [False, True])
@pytest.mark.parametrize("scenario", ["success", "streaming", "failure"])
async def test_v2_default_writes_success_and_failure_with_canonical_delivery(
    monkeypatch: pytest.MonkeyPatch, surface: str, populated: bool, scenario: str
) -> None:
    raw = _canonical_state("succeeded") if populated else {}
    if populated:
        raw["data"]["conversationHistory"] = _legacy_state(populated=True)["data"]["conversationHistory"]
    entity, provider, backing, agent, callback = _host(monkeypatch, surface, raw, scenario)
    response = (
        await entity.run(_request("new"))
        if surface == "plain"
        else load_agent_response(entity.run(_request("new")))
    )
    assert isinstance(response, AgentResponse)
    assert response.additional_properties.get("durable_status") == ("error" if scenario == "failure" else None)
    agent.model_calls.assert_called_once()
    backing.set_state.assert_called_once()
    for call in backing.set_state.call_args_list:
        assert call.args[0]["schemaVersion"] == "2.0.0"
        assert call.args[0]["data"].keys() >= V2_MAPS
        assert PRIVATE_MAPS.isdisjoint(call.args[0]["data"])
        validate_shared_state(call.args[0])
    assert provider._persisted_state_snapshot == backing.raw
    assert provider.state.to_dict() == backing.raw
    history = backing.raw["data"]["conversationHistory"]
    new_history = history[2:] if populated else history
    assert [entry["$type"] for entry in new_history] == (
        ["request"] if scenario == "failure" else ["request", "response"]
    )
    assert all(entry["correlationId"] == "new" for entry in new_history)
    result = backing.raw["data"]["terminalResults"]["new"]
    receipt = backing.raw["data"]["completionReceipts"]["new"]
    assert result["correlationId"] == receipt["correlationId"] == "new"
    assert result["outcome"] == receipt["outcome"] == ("failed" if scenario == "failure" else "succeeded")
    assert result["completedAt"] == receipt["completedAt"]
    assert result["resultExpiresAt"] == receipt["resultExpiresAt"]
    assert receipt["resultState"] == "available"
    if populated:
        assert history[:2] == raw["data"]["conversationHistory"]
        for field in V2_MAPS:
            assert backing.raw["data"][field]["completed"] == raw["data"][field]["completed"]
    if scenario == "failure":
        assert response.messages[0].contents[0].type == "error"
        assert response.messages[0].contents[0].error_code == "RuntimeError"
        assert "offline model failure" in (response.messages[0].contents[0].message or "")
        assert result["error"]["code"] == "RuntimeError"
        assert "offline model failure" in result["error"]["message"]
        assert result["response"]["messages"][0]["contents"][0]["$type"] == "error"
        callback.on_agent_response.assert_not_called()
        callback.on_streaming_response_update.assert_not_called()
    else:
        assert response.text == "writer response"
        callback.on_agent_response.assert_awaited_once()
        assert callback.on_streaming_response_update.await_count == (2 if scenario == "streaming" else 0)


@pytest.mark.parametrize("surface", ["plain", "registered"])
@pytest.mark.parametrize("kind", ["fresh", *SHARED_KINDS])
def test_v2_reset_uses_real_host_dispatch_and_preserves_committed_delivery(
    monkeypatch: pytest.MonkeyPatch, surface: str, kind: str
) -> None:
    raw = {} if kind == "fresh" else _canonical_state(kind)
    expected = _canonical_state("empty") if kind == "fresh" else deepcopy(raw)
    if kind != "fresh":
        raw["data"]["conversationHistory"] = _legacy_state(populated=True)["data"]["conversationHistory"]
        raw["data"]["session"] = {"session_id": "session", "state": {"before_reset": True}}
    entity, provider, backing, agent, callback = _host(monkeypatch, surface, raw)
    entity.reset()
    backing.set_state.assert_called_once_with(expected)
    assert provider.state.to_dict() == expected
    assert provider._persisted_state_snapshot == expected
    validate_shared_state(backing.raw)
    agent.run_calls.assert_not_called()
    agent.model_calls.assert_not_called()
    callback.on_agent_response.assert_not_called()
    callback.on_streaming_response_update.assert_not_called()
