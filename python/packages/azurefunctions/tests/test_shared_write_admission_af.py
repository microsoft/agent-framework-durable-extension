# Copyright (c) Microsoft. All rights reserved.

"""Offline AF factory contracts for canonical writes and read-only legacy state."""

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
    SharedAgentStateReader,
    load_agent_response,
)

from agent_framework_azurefunctions._entities import AzureFunctionEntityStateProvider, create_agent_entity

SHARED_KINDS = ("empty", "succeeded", "failed", "unavailable")
LEGACY_VERSIONS = ("1.0.0", "1.1.0", "1.2.0")
REJECTED_WRITE_VERSIONS = (*LEGACY_VERSIONS, "1.1.1", "2.0.1", "3.0.0")
CANONICAL_DELIVERY_MAPS = {"terminalResults", "completionReceipts"}
PROTOTYPE_DELIVERY_MAPS = {"responseMailbox", "completedCorrelations"}
LEGACY_WRITE_ERROR = (
    "Legacy state is read-only in this runtime. Keep it on its original deployment or use "
    "an isolated v2 entity instead."
)
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
    pytest.param(
        {"schemaVersion": "2.0.0", "data": {"conversationHistory": [], "completionReceipts": {}}},
        id="v2-missing-terminal-results",
    ),
    pytest.param(
        {"schemaVersion": "2.0.0", "data": {"conversationHistory": [], "terminalResults": {}}},
        id="v2-missing-completion-receipts",
    ),
    pytest.param(
        {
            "schemaVersion": "2.0.0",
            "data": {"conversationHistory": [], "terminalResults": None, "completionReceipts": {}},
        },
        id="v2-null-terminal-results",
    ),
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


def _legacy_state(*, populated: bool = False, version: str = "1.1.0") -> dict[str, Any]:
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
    return {"schemaVersion": version, "data": {"conversationHistory": history}}


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
        self.context.entity_name = "dafx-admission-agent"
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


@pytest.mark.parametrize("version", LEGACY_VERSIONS)
@pytest.mark.parametrize("populated", [False, True])
@pytest.mark.parametrize("operation", ["run", "run_agent", "reset", "expire_responses"])
@pytest.mark.parametrize("request_kind", ["new", "duplicate", "invalid-json"])
def test_actual_factory_rejects_legacy_before_parsing_or_any_write(
    monkeypatch: pytest.MonkeyPatch, version: str, populated: bool, operation: str, request_kind: str
) -> None:
    request = (
        "{not json"
        if request_kind == "invalid-json"
        else {"message": "question", "correlationId": "previous" if request_kind == "duplicate" else "new"}
    )
    backing = BackingStore(_legacy_state(populated=populated, version=version), operation, request)
    original, snapshot = backing.raw, deepcopy(backing.raw)
    agent: Any = MockSupportsAgentRun()
    callback = RecordingCallback()
    spies = _blocked_execution_spies(monkeypatch)

    create_agent_entity(agent, callback)(backing.context)

    backing.context.set_result.assert_called_once()
    result = backing.context.set_result.call_args.args[0]
    assert result == {"status": "error", "error": LEGACY_WRITE_ERROR}
    backing.context.set_state.assert_not_called()
    assert backing.raw is original
    assert backing.raw == snapshot
    for spy in spies:
        spy.assert_not_called()
    _assert_no_execution(agent, callback)


@pytest.mark.parametrize("kind", SHARED_KINDS)
def test_af_mutable_getter_loads_detached_canonical_state_without_writing(kind: str) -> None:
    backing = BackingStore(_canonical_state(kind))
    provider = AzureFunctionEntityStateProvider(backing.context)
    original, snapshot = backing.raw, deepcopy(backing.raw)
    state = provider.state
    assert isinstance(state, DurableAgentState)
    assert provider._state_cache is state and provider.state is state
    assert state.schema_version == "2.0.0"
    assert state.to_dict() == snapshot
    state.data.extension_data = {"detached": True}
    backing.context.set_state.assert_not_called()
    assert backing.raw is original
    assert backing.raw == snapshot


@pytest.mark.parametrize("kind", SHARED_KINDS)
@pytest.mark.parametrize("version", LEGACY_VERSIONS)
@pytest.mark.parametrize("operation", ["setter", "persist", "reset"])
@pytest.mark.parametrize("cache_kind", ["cold", "loaded-v2", "altered-v2"])
def test_af_mutations_check_backing_provenance_and_preserve_cache_on_rejection(
    kind: str, version: str, operation: str, cache_kind: str
) -> None:
    backing = BackingStore(_canonical_state(kind))
    provider = AzureFunctionEntityStateProvider(backing.context)
    if cache_kind != "cold":
        cached = provider.state
        if cache_kind == "altered-v2":
            cached.data.extension_data = {"uncommitted": True}
    backing.raw = _legacy_state(populated=True, version=version)
    original, snapshot = backing.raw, deepcopy(backing.raw)
    cache = provider._state_cache
    cache_snapshot = cache.to_dict() if cache is not None else None
    replacement = DurableAgentState()
    replacement.data.extension_data = {"candidate": "must not replace cache"}
    with pytest.raises(ValueError, match="Legacy state is read-only"):
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
@pytest.mark.parametrize("version", REJECTED_WRITE_VERSIONS)
def test_af_rejected_candidate_preserves_existing_cache(warm_cache: bool, version: str) -> None:
    backing = BackingStore(_canonical_state("succeeded"))
    provider = AzureFunctionEntityStateProvider(backing.context)
    if warm_cache:
        _ = provider.state
    cache = provider._state_cache
    original, snapshot = backing.raw, deepcopy(backing.raw)
    replacement = DurableAgentState()
    replacement.schema_version = version
    diagnostic = "Legacy state is read-only" if version.startswith("1.") else "Unsupported durable agent state"
    with pytest.raises(ValueError, match=diagnostic):
        provider.state = replacement
    assert provider._state_cache is cache
    if cache is not None:
        assert cache.to_dict() == snapshot
    assert replacement.schema_version == version
    backing.context.set_state.assert_not_called()
    assert backing.raw is original
    assert backing.raw == snapshot


@pytest.mark.parametrize("version", REJECTED_WRITE_VERSIONS)
def test_af_persist_rejects_altered_cached_version_and_restores_committed_v2(version: str) -> None:
    backing = BackingStore(_canonical_state("succeeded"))
    provider = AzureFunctionEntityStateProvider(backing.context)
    cache = provider.state
    cache.schema_version = version
    original, snapshot = backing.raw, deepcopy(backing.raw)
    diagnostic = "Legacy state is read-only" if version.startswith("1.") else "Unsupported durable agent state"
    with pytest.raises(ValueError, match=diagnostic):
        provider.persist_state()
    assert provider._state_cache is not cache
    assert provider.state.to_dict() == snapshot
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
        backing.raw = _canonical_state("empty")
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
@pytest.mark.parametrize("operation", ["run", "run_agent", "reset", "expire_responses"])
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


@pytest.mark.parametrize("initial", ["absent", "empty", "canonical"])
@pytest.mark.parametrize("operation", ["setter", "persist"])
def test_af_canonical_state_assignment_and_persistence_are_writable(initial: str, operation: str) -> None:
    raw = None if initial == "absent" else (_canonical_state("empty") if initial == "canonical" else {})
    backing = BackingStore(raw)
    provider = AzureFunctionEntityStateProvider(backing.context)
    candidate = DurableAgentState()
    candidate.data.extension_data = {"canonical": "supported"}
    if operation == "setter":
        provider.state = candidate
    else:
        provider.state.data.extension_data = {"canonical": "supported"}
        provider.persist_state()
    backing.context.set_state.assert_called_once_with(candidate.to_dict())
    assert backing.raw["schemaVersion"] == "2.0.0"
    assert backing.raw["data"].keys() >= CANONICAL_DELIVERY_MAPS
    assert PROTOTYPE_DELIVERY_MAPS.isdisjoint(backing.raw["data"])
    assert SharedAgentStateReader(backing.raw).to_dict() == backing.raw


@pytest.mark.parametrize("initial", ["absent", "empty", "canonical"])
@pytest.mark.parametrize("operation", ["run", "run_agent"])
@pytest.mark.parametrize("scenario", ["success", "streaming", "failure"])
def test_factory_canonical_writers_persist_success_and_failure_delivery_protocol(
    initial: str, operation: str, scenario: str
) -> None:
    raw: Any = None if initial == "absent" else (_canonical_state("succeeded") if initial == "canonical" else {})
    if initial == "canonical":
        raw["data"]["conversationHistory"] = _legacy_state(populated=True)["data"]["conversationHistory"]
    original = deepcopy(raw)
    backing = BackingStore(raw, operation, {"message": "question", "correlationId": "new"})
    agent: Any = MockSupportsAgentRun(scenario)
    callback = RecordingCallback()

    create_agent_entity(agent, callback)(backing.context)

    backing.context.set_result.assert_called_once()
    wire_result = backing.context.set_result.call_args.args[0]
    assert "status" not in wire_result
    response = load_agent_response(wire_result)
    agent.model_calls.assert_called_once()
    assert backing.context.set_state.called
    for call in backing.context.set_state.call_args_list:
        payload = call.args[0]
        assert payload["schemaVersion"] == "2.0.0"
        assert payload["data"].keys() >= CANONICAL_DELIVERY_MAPS
        assert PROTOTYPE_DELIVERY_MAPS.isdisjoint(payload["data"])
        assert SharedAgentStateReader(payload).to_dict() == payload
        if initial == "canonical":
            for field in CANONICAL_DELIVERY_MAPS:
                assert payload["data"][field]["completed"] == original["data"][field]["completed"]
    history = backing.raw["data"]["conversationHistory"]
    previous_count = 2 if initial == "canonical" else 0
    if previous_count:
        assert history[:previous_count] == original["data"]["conversationHistory"]
    new_history = history[previous_count:]
    expected_entry_types = ["request"] if scenario == "failure" else ["request", "response"]
    assert [entry["$type"] for entry in new_history] == expected_entry_types
    assert all(entry["correlationId"] == "new" for entry in new_history)
    result = backing.raw["data"]["terminalResults"]["new"]
    receipt = backing.raw["data"]["completionReceipts"]["new"]
    assert receipt == {
        "correlationId": "new",
        "outcome": "failed" if scenario == "failure" else "succeeded",
        "completedAt": result["completedAt"],
        "resultExpiresAt": result["resultExpiresAt"],
        "resultState": "available",
    }
    delivered = SharedAgentStateReader(backing.raw).try_get_agent_response("new")
    assert delivered is not None and delivered.text == response.text
    if scenario == "failure":
        assert response.additional_properties["durable_status"] == "error"
        assert response.messages[0].contents[0].type == "error"
        assert response.messages[0].contents[0].error_code == "RuntimeError"
        assert "offline model failure" in (response.messages[0].contents[0].message or "")
        assert result["error"]["code"] == "RuntimeError"
        assert result["error"]["message"] == "offline model failure"
        assert result["response"]["messages"][0]["contents"][0]["$type"] == "error"
        assert delivered.additional_properties["durable_status"] == "error"
        assert delivered.messages[0].contents[0].message == "offline model failure"
        callback.on_agent_response.assert_not_called()
        callback.on_streaming_response_update.assert_not_called()
    else:
        assert "durable_status" not in response.additional_properties
        assert "error" not in result
        assert response.text == "writer response"
        callback.on_agent_response.assert_awaited_once()
        assert callback.on_streaming_response_update.await_count == (2 if scenario == "streaming" else 0)


@pytest.mark.parametrize("initial", ["absent", "empty", "canonical"])
@pytest.mark.parametrize("kind", SHARED_KINDS)
def test_factory_reset_writes_canonical_state_without_removing_completion_facts(initial: str, kind: str) -> None:
    raw: Any = None if initial == "absent" else (_canonical_state(kind) if initial == "canonical" else {})
    expected = deepcopy(raw) if raw else _canonical_state("empty")
    if initial == "canonical":
        raw["data"]["conversationHistory"] = _legacy_state(populated=True)["data"]["conversationHistory"]
        raw["data"]["session"] = {"session_id": "session", "state": {"application": {"turn": 1}}}
    backing = BackingStore(raw, "reset")
    agent: Any = MockSupportsAgentRun()
    callback = RecordingCallback()

    create_agent_entity(agent, callback)(backing.context)

    backing.context.set_result.assert_called_once_with({"status": "reset"})
    backing.context.set_state.assert_called_once_with(expected)
    assert backing.raw == expected
    assert SharedAgentStateReader(backing.raw).to_dict() == expected
    _assert_no_execution(agent, callback)


@pytest.mark.parametrize("kind", ["succeeded", "failed", "unavailable"])
@pytest.mark.parametrize("operation", ["run", "run_agent"])
def test_factory_v2_duplicate_delivers_retained_outcome_without_execution_or_write(kind: str, operation: str) -> None:
    raw = _canonical_state(kind)
    backing = BackingStore(raw, operation, {"message": "question", "correlationId": "completed"})
    original = backing.raw
    agent: Any = MockSupportsAgentRun()
    callback = RecordingCallback()

    create_agent_entity(agent, callback)(backing.context)

    backing.context.set_result.assert_called_once()
    wire_result = backing.context.set_result.call_args.args[0]
    assert "status" not in wire_result
    response = load_agent_response(wire_result)
    expected = SharedAgentStateReader(raw).try_get_agent_response("completed")
    assert expected is not None
    assert response.text == expected.text
    assert response.additional_properties == expected.additional_properties
    actual_messages = [message.to_dict() for message in response.messages]
    assert actual_messages == [message.to_dict() for message in expected.messages]
    backing.context.set_state.assert_not_called()
    assert backing.raw is original and backing.raw == raw
    _assert_no_execution(agent, callback)


def test_factory_requires_explicit_deployment_acknowledgement_without_test_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DURABLE_AGENTS_DEPLOYMENT_MODE", raising=False)
    agent: Any = MockSupportsAgentRun()
    callback = RecordingCallback()
    with pytest.raises(ValueError, match="Schema 2 requires an isolated task hub/deployment"):
        create_agent_entity(agent, callback)
    _assert_no_execution(agent, callback)

    backing = BackingStore(_canonical_state("empty"), "run", {"message": "question", "correlationId": "new"})
    create_agent_entity(agent, callback, deployment_mode="isolated_v2")(backing.context)

    agent.model_calls.assert_called_once()
    backing.context.set_result.assert_called_once()
    response = load_agent_response(backing.context.set_result.call_args.args[0])
    assert response.text == "writer response"
    assert backing.raw["schemaVersion"] == "2.0.0"
    assert backing.raw["data"]["completionReceipts"]["new"]["outcome"] == "succeeded"
