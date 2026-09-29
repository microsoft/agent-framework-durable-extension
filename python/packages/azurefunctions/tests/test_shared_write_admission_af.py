# Copyright (c) Microsoft. All rights reserved.

"""Offline AF factory contracts for the read-only shared-state first slice."""

from collections.abc import AsyncIterator
from copy import deepcopy
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from agent_framework import AgentResponse, AgentResponseUpdate, Content, Message, ResponseStream
from agent_framework_durabletask import (
    DurableAgentState,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
    RunRequest,
)

from agent_framework_azurefunctions._entities import AzureFunctionEntityStateProvider, create_agent_entity

SHARED_KINDS = ("empty", "succeeded", "failed", "unavailable")
V2_MAPS = {"terminalResults", "completionReceipts", "responseMailbox", "completedCorrelations"}
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
    pytest.param({"schemaVersion": "3.0.0", "data": {}}, id="future-version"),
]


def _canonical_state(kind: str) -> dict[str, Any]:
    # Do not call a v2 codec or mutate a legacy schema label to produce this payload.
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


def _legacy_state(*, populated: bool = False) -> dict[str, Any]:
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
    return {"schemaVersion": "1.1.0", "data": {"conversationHistory": history}}


class MockSupportsAgentRun:
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
    def __init__(self, raw: Any, operation: str = "run", request: Any = None) -> None:
        self.raw = deepcopy(raw)
        self.context = Mock()
        self.context.operation_name = operation
        self.context.entity_key = "session"
        self.context.get_input.return_value = request
        # Only None means absent. In particular, False and [] are existing state.
        self.context.get_state.side_effect = lambda *args: self.raw if self.raw is not None else {}
        self.context.set_state.side_effect = self._write

    def _write(self, value: dict[str, Any]) -> None:
        self.raw = deepcopy(value)


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


def _assert_no_execution(agent: Any, callback: RecordingCallback) -> None:
    agent.run_calls.assert_not_called()
    agent.model_calls.assert_not_called()
    callback.on_agent_response.assert_not_called()
    callback.on_streaming_response_update.assert_not_called()


@pytest.mark.parametrize("kind", SHARED_KINDS)
@pytest.mark.parametrize("operation", ["run", "run_agent", "reset"])
@pytest.mark.parametrize("request_kind", ["new", "duplicate", "invalid-json"])
def test_actual_factory_rejects_v2_before_parsing_or_any_write(
    monkeypatch: pytest.MonkeyPatch, kind: str, operation: str, request_kind: str
) -> None:
    request = (
        "{not json"
        if request_kind == "invalid-json"
        else {"message": "question", "correlationId": "completed" if request_kind == "duplicate" else "new"}
    )
    backing = BackingStore(_canonical_state(kind), operation, request)
    original, snapshot = backing.raw, deepcopy(backing.raw)
    agent: Any = MockSupportsAgentRun()
    callback = RecordingCallback()
    spies = _blocked_execution_spies(monkeypatch)

    create_agent_entity(agent, callback)(backing.context)

    backing.context.set_result.assert_called_once()
    result = backing.context.set_result.call_args.args[0]
    assert result["status"] == "error"
    assert result["error"]
    backing.context.set_state.assert_not_called()
    assert backing.raw is original
    assert backing.raw == snapshot
    for spy in spies:
        spy.assert_not_called()
    _assert_no_execution(agent, callback)


@pytest.mark.parametrize("kind", SHARED_KINDS)
def test_af_mutable_getter_rejects_shared_state_without_caching(kind: str) -> None:
    backing = BackingStore(_canonical_state(kind))
    provider = AzureFunctionEntityStateProvider(backing.context)
    original, snapshot = backing.raw, deepcopy(backing.raw)
    with pytest.raises(ValueError):
        _ = provider.state
    assert provider._state_cache is None
    backing.context.set_state.assert_not_called()
    assert backing.raw is original
    assert backing.raw == snapshot


@pytest.mark.parametrize("kind", SHARED_KINDS)
@pytest.mark.parametrize("operation", ["setter", "persist", "reset"])
@pytest.mark.parametrize("cache_kind", ["cold", "loaded-legacy", "altered-legacy"])
def test_af_mutations_check_backing_provenance_and_preserve_cache_on_rejection(
    kind: str, operation: str, cache_kind: str
) -> None:
    backing = BackingStore(_legacy_state(populated=True))
    provider = AzureFunctionEntityStateProvider(backing.context)
    if cache_kind != "cold":
        cached = provider.state
        if cache_kind == "altered-legacy":
            cached.schema_version = "1.0.0"
    backing.raw = _canonical_state(kind)
    original, snapshot = backing.raw, deepcopy(backing.raw)
    cache = provider._state_cache
    cache_snapshot = cache.to_dict() if cache is not None else None
    replacement = DurableAgentState()
    replacement.data.extension_data = {"candidate": "must not replace cache"}
    with pytest.raises(ValueError):
        if operation == "setter":
            provider.state = replacement
        elif operation == "persist":
            provider.persist_state()
        else:
            provider.reset()
    assert provider._state_cache is cache
    if cache is not None:
        assert cache.to_dict() == cache_snapshot
    assert replacement.data.extension_data == {"candidate": "must not replace cache"}
    backing.context.set_state.assert_not_called()
    assert backing.raw is original
    assert backing.raw == snapshot


@pytest.mark.parametrize("warm_cache", [False, True])
@pytest.mark.parametrize("version", ["2.0.0", "3.0.0", "1.1.1"])
def test_af_rejected_candidate_preserves_existing_cache(warm_cache: bool, version: str) -> None:
    backing = BackingStore(_legacy_state(populated=True))
    provider = AzureFunctionEntityStateProvider(backing.context)
    if warm_cache:
        _ = provider.state
    cache = provider._state_cache
    original, snapshot = backing.raw, deepcopy(backing.raw)
    replacement = DurableAgentState()
    replacement.schema_version = version
    with pytest.raises(ValueError):
        provider.state = replacement
    assert provider._state_cache is cache
    backing.context.set_state.assert_not_called()
    assert backing.raw is original
    assert backing.raw == snapshot


@pytest.mark.parametrize("version", ["2.0.0", "3.0.0", "1.1.1"])
def test_af_persist_rejects_altered_cached_version_over_legacy_backing(version: str) -> None:
    backing = BackingStore(_legacy_state(populated=True))
    provider = AzureFunctionEntityStateProvider(backing.context)
    cache = provider.state
    cache.schema_version = version
    original, snapshot = backing.raw, deepcopy(backing.raw)
    with pytest.raises(ValueError):
        provider.persist_state()
    assert provider._state_cache is cache
    assert cache.schema_version == version
    backing.context.set_state.assert_not_called()
    assert backing.raw is original
    assert backing.raw == snapshot


@pytest.mark.parametrize("raw", INVALID_BACKING)
@pytest.mark.parametrize(
    ("operation", "warm_cache"),
    [("getter", False)] + [(operation, warm) for operation in ("setter", "persist", "reset") for warm in (False, True)],
)
def test_af_provider_rejects_malformed_backing_before_loading_or_writing(
    raw: Any, operation: str, warm_cache: bool
) -> None:
    backing = BackingStore(raw)
    provider = AzureFunctionEntityStateProvider(backing.context)
    if warm_cache:
        backing.raw = _legacy_state(populated=True)
        _ = provider.state
        backing.raw = deepcopy(raw)
    cache = provider._state_cache
    cache_snapshot = cache.to_dict() if cache is not None else None
    original, snapshot = backing.raw, deepcopy(backing.raw)
    with pytest.raises(ValueError):
        if operation == "getter":
            _ = provider.state
        elif operation == "setter":
            provider.state = DurableAgentState()
        elif operation == "persist":
            provider.persist_state()
        else:
            provider.reset()
    assert provider._state_cache is cache
    if cache is not None:
        assert cache.to_dict() == cache_snapshot
    backing.context.set_state.assert_not_called()
    assert backing.raw is original
    assert backing.raw == snapshot


@pytest.mark.parametrize("raw", INVALID_BACKING)
@pytest.mark.parametrize("operation", ["run", "run_agent", "reset"])
def test_factory_rejects_malformed_existing_backend_state_without_overwrite(raw: Any, operation: str) -> None:
    backing = BackingStore(raw, operation, {"message": "question", "correlationId": "new"})
    original, snapshot = backing.raw, deepcopy(backing.raw)
    agent: Any = MockSupportsAgentRun()
    callback = RecordingCallback()

    create_agent_entity(agent, callback)(backing.context)

    backing.context.set_result.assert_called_once()
    result = backing.context.set_result.call_args.args[0]
    assert result["status"] == "error"
    assert result["error"]
    backing.context.set_state.assert_not_called()
    assert backing.raw is original
    assert backing.raw == snapshot
    _assert_no_execution(agent, callback)


@pytest.mark.parametrize("operation", ["setter", "persist"])
def test_af_legacy_state_assignment_and_persistence_remain_writable(operation: str) -> None:
    backing = BackingStore({})
    provider = AzureFunctionEntityStateProvider(backing.context)
    candidate = DurableAgentState()
    candidate.data.extension_data = {"legacy": "supported"}
    if operation == "setter":
        provider.state = candidate
    else:
        provider.state.data.extension_data = {"legacy": "supported"}
        provider.persist_state()
    backing.context.set_state.assert_called_once_with(candidate.to_dict())
    assert backing.raw["schemaVersion"] == "1.1.0"
    assert V2_MAPS.isdisjoint(backing.raw["data"])


def test_af_persistence_serializes_a_warm_history_once(monkeypatch: pytest.MonkeyPatch) -> None:
    backing = BackingStore(_legacy_state(populated=True))
    provider = AzureFunctionEntityStateProvider(backing.context)
    cache = provider.state
    entries = [Mock(wraps=entry.to_dict) for entry in cache.data.conversation_history]
    for entry, serializer in zip(cache.data.conversation_history, entries):
        monkeypatch.setattr(entry, "to_dict", serializer)
    backing.context.get_state.reset_mock()

    provider.persist_state()

    for serializer in entries:
        serializer.assert_called_once_with()
    backing.context.get_state.assert_called_once()
    backing.context.set_state.assert_called_once()
    expected = _legacy_state(populated=True)
    for entry in expected["data"]["conversationHistory"]:
        entry["createdAt"] = entry["createdAt"].replace("Z", "+00:00")
    assert backing.raw == expected
    assert provider._state_cache is cache


@pytest.mark.parametrize("initial", ["absent", "empty", "legacy"])
@pytest.mark.parametrize("operation", ["run", "run_agent"])
@pytest.mark.parametrize("scenario", ["success", "streaming", "failure"])
def test_factory_legacy_writers_keep_1_1_success_and_failure_protocol(
    initial: str, operation: str, scenario: str
) -> None:
    raw = None if initial == "absent" else (_legacy_state(populated=True) if initial == "legacy" else {})
    backing = BackingStore(raw, operation, {"message": "question", "correlationId": "new"})
    agent: Any = MockSupportsAgentRun(scenario)
    callback = RecordingCallback()

    create_agent_entity(agent, callback)(backing.context)

    backing.context.set_result.assert_called_once()
    wire_result = backing.context.set_result.call_args.args[0]
    assert "status" not in wire_result
    response = AgentResponse.from_dict(wire_result)
    assert "durable_status" not in response.additional_properties
    agent.model_calls.assert_called_once()
    assert backing.context.set_state.called
    for call in backing.context.set_state.call_args_list:
        assert call.args[0]["schemaVersion"] == "1.1.0"
        assert V2_MAPS.isdisjoint(call.args[0]["data"])
    history = backing.raw["data"]["conversationHistory"]
    assert len(history) == (4 if initial == "legacy" else 2)
    assert [entry["$type"] for entry in history[-2:]] == ["request", "response"]
    assert [entry["correlationId"] for entry in history[-2:]] == ["new", "new"]
    if initial == "legacy":
        assert [entry["correlationId"] for entry in history[:2]] == ["previous", "previous"]
    if scenario == "failure":
        assert response.messages[0].contents[0].type == "error"
        assert response.messages[0].contents[0].error_code == "RuntimeError"
        assert "offline model failure" in (response.messages[0].contents[0].message or "")
        assert history[-1]["messages"][0]["contents"][0]["$type"] == "error"
        callback.on_agent_response.assert_not_called()
        callback.on_streaming_response_update.assert_not_called()
    else:
        assert response.text == "writer response"
        callback.on_agent_response.assert_awaited_once()
        assert callback.on_streaming_response_update.await_count == (2 if scenario == "streaming" else 0)


@pytest.mark.parametrize("initial", ["absent", "empty", "legacy"])
def test_factory_legacy_reset_writes_fresh_1_1(initial: str) -> None:
    raw = None if initial == "absent" else (_legacy_state(populated=True) if initial == "legacy" else {})
    backing = BackingStore(raw, "reset")
    agent: Any = MockSupportsAgentRun()
    callback = RecordingCallback()

    create_agent_entity(agent, callback)(backing.context)

    backing.context.set_result.assert_called_once_with({"status": "reset"})
    # Admission before replacing the old cache, then re-admission at persistence.
    assert backing.context.get_state.call_count == 2
    backing.context.set_state.assert_called_once_with(_legacy_state())
    assert backing.raw == _legacy_state()
    _assert_no_execution(agent, callback)
