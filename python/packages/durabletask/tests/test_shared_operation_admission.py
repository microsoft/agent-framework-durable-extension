# Copyright (c) Microsoft. All rights reserved.

"""Operation admission, rollback, migration authority, and entry-local replay boundaries."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import Agent, AgentResponse, Content, ContextProvider, Message
from test_history_pipeline_revision import ToolChatClient
from test_revision_contract import JsonStateProvider

from agent_framework_durabletask import AgentEntity, DurableAgentState, RunRequest
from agent_framework_durabletask import _durable_agent_state as state_module
from agent_framework_durabletask import _entities as entities_module
from agent_framework_durabletask import _shared_state_validation as validation_module
from agent_framework_durabletask._history_provider import current_durable_history_binding
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._response_utils import invocation_outcome
from agent_framework_durabletask._shared_state_validation import validate_shared_state
from agent_framework_durabletask._state_migration import migrate_legacy_state, state_snapshot_digest

NOW = datetime(2026, 9, 16, 12, tzinfo=timezone.utc)
COMPLETED = (NOW - timedelta(hours=1)).isoformat()
DEADLINE = NOW + timedelta(hours=1)
OPAQUE: dict[str, Any] = {"nested": [None, False, 0, 0.0, "", [], {}], "unicode": "雪😀"}


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, allow_nan=False)


def _same(actual: Any, expected: Any) -> None:
    assert _json(actual) == _json(expected)


@pytest.fixture(autouse=True)
def clock(monkeypatch: pytest.MonkeyPatch) -> Callable[[datetime], None]:
    class Clock(datetime):
        instant = NOW

        @classmethod
        def now(cls, tz: Any = None) -> Clock:
            assert tz is not None
            return cls.fromtimestamp(cls.instant.timestamp(), tz)

    def set_time(instant: datetime) -> None:
        Clock.instant = instant

    # Both the state accessor and operation admission must observe the same instant.
    for module in (state_module, entities_module, validation_module):
        monkeypatch.setattr(module, "datetime", Clock)
    return set_time


def _response(text: str) -> dict[str, Any]:
    return {
        "messages": [{"role": "assistant", "contents": [{"$type": "text", "text": text}]}],
        "extensionData": deepcopy(OPAQUE),
    }


def _raw(*, expiry: datetime = DEADLINE) -> dict[str, Any]:
    common = {
        "correlationId": "A",
        "outcome": "succeeded",
        "completedAt": COMPLETED,
        "resultExpiresAt": expiry.isoformat(),
    }
    return {
        "schemaVersion": "2.0.0",
        "data": {
            "conversationHistory": [],
            "terminalResults": {"A": {**common, "response": _response("original-A")}},
            "completionReceipts": {"A": {**common, "resultState": "available", "futureReceipt": deepcopy(OPAQUE)}},
            "futureData": deepcopy(OPAQUE),
        },
        "futureRoot": deepcopy(OPAQUE),
    }


class _CaptureAgent:
    """A real entity invocation target without Core's context-provider pipeline."""

    name = "no-pipeline-admission"

    def __init__(self) -> None:
        self.received_messages: list[list[Message]] = []

    async def run(self, *, messages: Sequence[Message], **kwargs: Any) -> AgentResponse[Any]:
        self.received_messages.append(deepcopy(list(messages)))
        return AgentResponse(messages=[Message("assistant", ["captured-answer"])])


async def _duplicate_without_execution(provider: JsonStateProvider) -> None:
    before = deepcopy(provider.raw)
    writes = provider.writes
    agent = _CaptureAgent()
    entity = AgentEntity(agent, state_provider=provider)  # type: ignore[arg-type]
    assert entity.agent is agent
    result = await entity.run({"message": "do not execute A", "correlationId": "A"})
    assert result.text == "original-A"
    assert invocation_outcome(result) == "succeeded"
    assert agent.received_messages == [] and provider.writes == writes
    _same(provider.raw, before)
    _same(provider.state.to_dict(), before)


@pytest.mark.parametrize("operation", ["setter-warm", "setter-cold", "setter-alias", "in-place-persist", "mixin-reset"])
async def test_rejected_direct_provider_operation_restores_committed_cache(
    operation: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = _raw()
    before = deepcopy(raw)
    provider = JsonStateProvider(raw)
    backend = Mock(wraps=provider._set_state_dict)
    monkeypatch.setattr(provider, "_set_state_dict", backend)
    if operation != "setter-cold":
        _same(provider.state.to_dict(), before)
    changed = deepcopy(raw)
    changed["data"]["terminalResults"]["A"]["response"] = _response("replacement-A")
    # The changed result and both maps are individually canonical and consistent.
    validate_shared_state(changed)
    candidate = DurableAgentState.from_dict(changed)

    with pytest.raises(ValueError):
        if operation == "setter-alias":
            alias = provider.state
            alias.data.response_mailbox["A"]["response"] = deepcopy(changed["data"]["terminalResults"]["A"]["response"])
            provider.state = alias
        elif operation.startswith("setter-"):
            provider.state = candidate
        elif operation == "in-place-persist":
            provider.state.data.response_mailbox["A"]["response"] = deepcopy(
                changed["data"]["terminalResults"]["A"]["response"]
            )
            validate_shared_state(provider.state.to_dict())
            provider.persist_state()
        else:
            provider.reset()

    backend.assert_not_called()
    assert provider.writes == 0
    _same(provider.raw, before)
    _same(provider.state.to_dict(), before)
    await _duplicate_without_execution(provider)
    await _duplicate_without_execution(JsonStateProvider(provider.raw))
    backend.assert_not_called()
    _same(candidate.to_dict(), changed)
    _same(raw, before)


async def test_entity_reset_preserves_live_completion_records() -> None:
    raw = _raw()
    raw["data"]["conversationHistory"] = [_entry("request", "old-user", "user", [{"$type": "text", "text": "old"}])]
    raw["data"]["session"] = {"session_id": "revision-session", "state": {"context": deepcopy(OPAQUE)}}
    before = deepcopy(raw)
    provider = JsonStateProvider(raw)
    agent = _CaptureAgent()
    entity = AgentEntity(agent, state_provider=provider)  # type: ignore[arg-type]

    entity.reset()

    assert provider.writes == 1 and agent.received_messages == []
    assert provider.state.data.conversation_history == [] and provider.state.data.session is None
    for field in ("terminalResults", "completionReceipts", "futureData"):
        _same(provider.raw["data"][field], before["data"][field])
    _same(provider.raw["futureRoot"], before["futureRoot"])
    validate_shared_state(provider.raw)
    await _duplicate_without_execution(provider)
    await _duplicate_without_execution(JsonStateProvider(provider.raw))
    _same(raw, before)


class _UnavailableHook(ContextProvider):
    def __init__(self, phase: str, *, set_time: Callable[[datetime], None] | None = None) -> None:
        super().__init__("operation-clock-admission")
        self.phase = phase
        self.set_time = set_time
        self.applied = False
        self.snapshot: dict[str, Any] | None = None

    async def before_run(self, **kwargs: Any) -> None:
        self._apply("before")

    async def after_run(self, **kwargs: Any) -> None:
        self._apply("after")

    def _apply(self, phase: str) -> None:
        if phase != self.phase or self.applied:
            return
        binding = current_durable_history_binding()
        assert binding is not None and binding.correlation_id == "B"
        state = binding.state_provider.state
        if self.set_time is not None:
            self.set_time(DEADLINE)
            state.expire_responses(now=DEADLINE)
        else:
            # For already-due A, eager operation cleanup has removed the payload.
            # Replacing its real removal time with a future time is still invalid.
            expiry = datetime.fromisoformat(state.data.completed_correlations["A"]["resultExpiresAt"])
            if expiry > NOW:
                assert "A" in state.data.response_mailbox
                assert state.data.completed_correlations["A"]["resultState"] == "available"
            else:
                assert "A" not in state.data.response_mailbox
                assert state.data.completed_correlations["A"]["resultUnavailableAt"] == NOW.isoformat()
            state.data.response_mailbox.pop("A", None)
            state.data.completed_correlations["A"].update(
                resultState="unavailable", resultUnavailableAt=DEADLINE.isoformat()
            )
        self.applied = True
        self.snapshot = deepcopy(state.to_dict())
        # Snapshot ordering alone admits both attacks: completed <= expiry <= unavailable.
        validate_shared_state(self.snapshot)


@pytest.mark.parametrize("phase", ["before", "after"])
@pytest.mark.parametrize("already_due", [False, True], ids=["live-result", "fabricated-future-removal-time"])
async def test_future_unavailability_is_rejected_before_backend_write(
    phase: str, already_due: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    expiry = NOW - timedelta(minutes=10) if already_due else DEADLINE
    raw = _raw(expiry=expiry)
    before = deepcopy(raw)
    provider = JsonStateProvider(raw)
    backend = Mock(wraps=provider._set_state_dict)
    monkeypatch.setattr(provider, "_set_state_dict", backend)
    hook = _UnavailableHook(phase)
    client = ToolChatClient(tool_calls=False)
    entity = AgentEntity(Agent(client=client, context_providers=[hook]), state_provider=provider)

    with pytest.raises(ValueError):
        await entity.run({"message": "B cannot remove A ahead of the real clock", "correlationId": "B"})

    assert hook.applied and hook.snapshot is not None
    receipt = hook.snapshot["data"]["completionReceipts"]["A"]
    assert receipt["resultState"] == "unavailable" and receipt["resultUnavailableAt"] == DEADLINE.isoformat()
    assert "A" not in hook.snapshot["data"]["terminalResults"]
    assert datetime.fromisoformat(receipt["resultUnavailableAt"]) > NOW
    assert datetime.fromisoformat(receipt["resultExpiresAt"]) == expiry
    backend.assert_not_called()
    assert provider.writes == 0 and current_durable_history_binding() is None
    _same(provider.raw, before)
    _same(entity.state.to_dict(), before)
    assert entity.state.try_get_agent_response("B") is None
    if not already_due:
        await _duplicate_without_execution(provider)
        await _duplicate_without_execution(JsonStateProvider(provider.raw))
    _same(raw, before)


@pytest.mark.parametrize("phase", ["before", "after"])
async def test_actual_deadline_cleanup_during_B_is_allowed(phase: str, clock: Callable[[datetime], None]) -> None:
    raw = _raw()
    before = deepcopy(raw)
    provider = JsonStateProvider(raw)
    hook = _UnavailableHook(phase, set_time=clock)
    client = ToolChatClient(tool_calls=False)
    entity = AgentEntity(Agent(client=client, context_providers=[hook]), state_provider=provider)

    response = await entity.run({"message": "B reaches the real expiry", "correlationId": "B"})

    assert hook.applied and response.text == "answer-1"
    assert len(client.received_messages) == provider.writes == 1
    validate_shared_state(provider.raw)
    expected = {
        **before["data"]["completionReceipts"]["A"],
        "resultState": "unavailable",
        "resultUnavailableAt": DEADLINE.isoformat(),
    }
    _same(provider.raw["data"]["completionReceipts"]["A"], expected)
    assert "A" not in provider.raw["data"]["terminalResults"]
    assert provider.raw["data"]["completionReceipts"]["B"]["outcome"] == "succeeded"
    cold = JsonStateProvider(provider.raw)
    agent = _CaptureAgent()
    duplicate = await AgentEntity(agent, state_provider=cold).run({  # type: ignore[arg-type]
        "message": "A must not execute after actual expiry",
        "correlationId": "A",
    })
    assert duplicate.additional_properties["durable_status"] == "already_completed"
    assert invocation_outcome(duplicate) == "succeeded"
    assert agent.received_messages == [] and cold.writes == 0
    _same(cold.raw, provider.raw)
    _same(cold.state.to_dict(), provider.raw)
    _same(raw, before)


def _completion_journal(source: dict[str, Any], *results: dict[str, Any]) -> dict[str, Any]:
    return {
        "sourceDigest": state_snapshot_digest(source),
        "evidenceId": "operation-completion-journal",
        "complete": True,
        "results": deepcopy(list(results)),
    }


def _migrate(source: dict[str, Any], **options: Any) -> DurableAgentState:
    return migrate_legacy_state(
        source,
        source_digest=state_snapshot_digest(source),
        source_session_id="original-legacy-session",
        migration_id="operation-admission-migration",
        ownership_transfer_id="operation-admission-transfer",
        delivery_window_seconds=3600,
        now=NOW,
        **options,
    )


@pytest.mark.parametrize("empty_positions", [False, True], ids=["empty-data", "explicit-empty-positions"])
@pytest.mark.parametrize(
    "explicit_completion_journal", [False, True], ids=["omitted-completion", "complete-empty-journal"]
)
@pytest.mark.parametrize("strict", [False, True])
def test_custom_accepted_delivery_proves_legacy_source_was_used(
    empty_positions: bool, explicit_completion_journal: bool, strict: bool
) -> None:
    source: dict[str, Any] = {"schemaVersion": "1.1.0", "data": {}}
    if empty_positions:
        source["data"]["ingestedPositions"] = {}
    actual_message = Message(
        "user",
        [Content.from_text("actually accepted", additional_properties=deepcopy(OPAQUE))],
        message_id="custom-accepted-message",
        additional_properties={"source": deepcopy(OPAQUE)},
    )
    delivery = {
        "sourceDigest": state_snapshot_digest(source),
        "evidenceId": "custom-delivery-journal",
        "complete": True,
        "messages": [actual_message.to_dict()],
    }
    completion = _completion_journal(source)
    before = deepcopy(source)
    delivery_before, completion_before = deepcopy(delivery), deepcopy(completion)
    message_before = deepcopy(actual_message.to_dict())
    options: dict[str, Any] = {"delivery_evidence": delivery, "require_known_outcomes": strict}
    if explicit_completion_journal:
        options["completion_evidence"] = completion

    try:
        if explicit_completion_journal:
            migrated = _migrate(source, **options)
            raw = migrated.to_dict()
            validate_shared_state(raw)
            cold = DurableAgentState.from_json(_json(raw))
            _same(cold.to_dict(), raw)
            assert cold.data.response_mailbox == cold.data.completed_correlations == {}
            assert cold.data.conversation_history == []
            assert cold.data.ingested_messages == {"custom-accepted-message": [message_identity(actual_message)]}
            migration = cold.data.unknown_fields["migration"]
            assert migration["evidenceId"] == delivery["evidenceId"]
            assert migration["completionEvidenceId"] == completion["evidenceId"]
        else:
            with pytest.raises(ValueError, match="authoritative completion evidence"):
                _migrate(source, **options)
    finally:
        _same(source, before)
        _same(delivery, delivery_before)
        _same(completion, completion_before)
        _same(actual_message.to_dict(), message_before)


def _entry(kind: str, message_id: str, role: str, contents: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "$type": kind,
        "correlationId": "A",
        "createdAt": COMPLETED,
        "messages": [{"role": role, "messageId": message_id, "contents": deepcopy(contents)}],
        "futureEntry": deepcopy(OPAQUE),
    }


def _retained_history(failure_evidence: str) -> list[dict[str, Any]]:
    history = [
        _entry("request", "original-question", "user", [{"$type": "text", "text": "original question"}]),
        _entry(
            "response",
            "successful-call",
            "assistant",
            [
                {"$type": "text", "text": "successful intermediate call"},
                {"$type": "functionCall", "callId": "prior-call", "name": "lookup", "arguments": {"key": "prior"}},
            ],
        ),
        _entry(
            "request",
            "successful-result",
            "tool",
            [
                {"$type": "functionResult", "callId": "prior-call", "result": {"ok": True}},
            ],
        ),
        # Tool-role errors are not evidence that this response entry failed.
        _entry(
            "response",
            "tool-error-control",
            "tool",
            [
                {"$type": "error", "errorCode": "tool_warning", "message": "tool-local warning"},
                {"$type": "text", "text": "retained tool response"},
            ],
        ),
        _entry("response", "ordinary-partial", "assistant", [{"$type": "text", "text": "retained partial text"}]),
    ]
    last = _entry("response", "last-response", "assistant", [{"$type": "text", "text": "last partial text"}])
    if failure_evidence != "partial-text-only":
        last["messages"][0]["contents"] = [{"$type": "text", "text": "RuntimeError: retained runtime failure"}]
        if failure_evidence == "non-tool-error":
            last["messages"][0]["contents"].append({
                "$type": "error",
                "errorCode": "RuntimeError",
                "message": "retained runtime failure",
            })
        else:
            last["extensionData"] = {"durable_status": "error", "future": deepcopy(OPAQUE)}
    history.append(last)
    return history


@pytest.mark.parametrize("pipeline", [False, True], ids=["custom-no-pipeline", "core-pipeline"])
@pytest.mark.parametrize("migrated", [False, True], ids=["native-shared", "migrated-legacy"])
@pytest.mark.parametrize("failure_evidence", ["non-tool-error", "response-status-error", "partial-text-only"])
async def test_cold_replay_filters_failure_entries_not_whole_failed_correlations(
    pipeline: bool, migrated: bool, failure_evidence: str
) -> None:
    history = _retained_history(failure_evidence)
    source = {"schemaVersion": "1.1.0", "data": {"conversationHistory": deepcopy(history)}}
    original = {
        "correlationId": "A",
        "outcome": "failed",
        "completedAt": COMPLETED,
        "response": _response("original journal failure, not retained partial text"),
        "error": {"code": "OriginalFailure", "message": "Independent recorded failure", "details": deepcopy(OPAQUE)},
    }
    journal = _completion_journal(source, original)
    source_before, journal_before, original_before = deepcopy(source), deepcopy(journal), deepcopy(original)
    if migrated:
        raw = _migrate(source, completion_evidence=journal).to_dict()
    else:
        raw = _raw()
        raw["data"]["conversationHistory"] = deepcopy(history)
        raw["data"]["terminalResults"]["A"] = {**deepcopy(original), "resultExpiresAt": DEADLINE.isoformat()}
        raw["data"]["completionReceipts"]["A"]["outcome"] = "failed"
    validate_shared_state(raw)
    _same(raw["data"]["conversationHistory"], history)
    raw_before = deepcopy(raw)
    provider = JsonStateProvider(json.loads(_json(raw)))
    capture: Any = ToolChatClient(tool_calls=False) if pipeline else _CaptureAgent()
    agent: Any = Agent(client=capture) if pipeline else capture
    entity = AgentEntity(agent, state_provider=provider)
    assert entity._has_context_pipeline() is pipeline
    _same(entity.state.to_dict(), raw_before)

    duplicate = await entity.run({"message": "do not repeat original failure", "correlationId": "A"})
    assert invocation_outcome(duplicate) == "failed"
    assert "original journal failure" in duplicate.text
    assert capture.received_messages == [] and provider.writes == 0
    _same(entity.state.to_dict(), raw_before)

    response = await entity.run({"message": "next independent turn", "correlationId": "B"})

    assert invocation_outcome(response) == "succeeded"
    assert len(capture.received_messages) == provider.writes == 1
    messages = capture.received_messages[0]
    # Derive the retained identities from entries, not the failed completion's correlation.
    replay_history = history if failure_evidence == "partial-text-only" else history[:-1]
    expected_ids = [message["messageId"] for entry in replay_history for message in entry["messages"]]
    assert [message.message_id for message in messages[:-1]] == expected_ids
    assert messages[-1].text == "next independent turn"
    assert any(content.type == "function_call" for content in messages[1].contents)
    assert any(content.type == "function_result" for content in messages[2].contents)
    assert any(content.type == "error" for content in messages[3].contents)
    assert messages[4].text == "retained partial text"
    assert not any("retained runtime failure" in _json(message.to_dict()) for message in messages)
    assert not any("original journal failure" in message.text for message in messages)
    # Read projection and replay must not rewrite the legacy or native raw transcript.
    _same(provider.raw["data"]["conversationHistory"][: len(history)], history)
    for field in ("terminalResults", "completionReceipts"):
        _same(provider.raw["data"][field]["A"], raw_before["data"][field]["A"])
    _same(source, source_before)
    _same(journal, journal_before)
    _same(original, original_before)
    _same(raw, raw_before)


def _request(correlation: str, *, object_request: bool) -> RunRequest | dict[str, Any]:
    request: dict[str, Any] = {
        "message": "new work",
        "correlationId": correlation,
        "created_at": NOW.isoformat(),
        "options": {"store": False, "custom": deepcopy(OPAQUE)},
    }
    if not object_request:
        return request
    # Also cover a request constructed validly but modified by its caller later.
    value = RunRequest.from_dict({**request, "correlationId": "valid-at-construction"})
    value.correlation_id = correlation
    return value


def _request_snapshot(request: RunRequest | dict[str, Any]) -> dict[str, Any]:
    return deepcopy(request.to_dict() if isinstance(request, RunRequest) else request)


@pytest.mark.parametrize("object_request", [False, True], ids=["dictionary", "mutable-run-request"])
@pytest.mark.parametrize(
    "correlation",
    [
        pytest.param("a" * 257, id="ascii-over-256"),
        pytest.param("before\nafter", id="embedded-newline"),
        pytest.param("雪😀" * 128 + "雪", id="unicode-over-256"),
    ],
)
async def test_invalid_correlation_rejects_before_custom_agent_execution(
    correlation: str, object_request: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = _raw()
    before = deepcopy(raw)
    provider = JsonStateProvider(raw)
    backend = Mock(wraps=provider._set_state_dict)
    monkeypatch.setattr(provider, "_set_state_dict", backend)
    agent = _CaptureAgent()
    entity = AgentEntity(agent, state_provider=provider)  # type: ignore[arg-type]
    assert entity.agent is agent and not entity._has_context_pipeline()
    request = _request(correlation, object_request=object_request)
    request_before = _request_snapshot(request)

    with pytest.raises(ValueError):
        await entity.run(request)

    assert agent.received_messages == [], "Late record_response rejection must not follow model execution."
    backend.assert_not_called()
    assert provider.writes == 0
    _same(provider.raw, before)
    _same(entity.state.to_dict(), before)
    _same(raw, before)
    _same(_request_snapshot(request), request_before)


@pytest.mark.parametrize("object_request", [False, True], ids=["dictionary", "mutable-run-request"])
async def test_256_unicode_code_points_execute_once_and_remain_duplicate_safe(object_request: bool) -> None:
    correlation = "雪😀" * 128
    assert len(correlation) == 256 and len(correlation.encode("utf-8")) > 256
    raw = _raw()
    before = deepcopy(raw)
    provider = JsonStateProvider(raw)
    agent = _CaptureAgent()
    entity = AgentEntity(agent, state_provider=provider)  # type: ignore[arg-type]
    assert entity.agent is agent and not entity._has_context_pipeline()
    request = _request(correlation, object_request=object_request)
    request_before = _request_snapshot(request)

    response = await entity.run(request)

    assert response.text == "captured-answer" and invocation_outcome(response) == "succeeded"
    assert len(agent.received_messages) == provider.writes == 1
    assert [message.text for message in agent.received_messages[0]] == ["new work"]
    validate_shared_state(provider.raw)
    for field in ("terminalResults", "completionReceipts"):
        assert provider.raw["data"][field][correlation]["correlationId"] == correlation
        _same(provider.raw["data"][field]["A"], before["data"][field]["A"])
    committed = deepcopy(provider.raw)
    assert (await entity.run(request)).text == "captured-answer"
    assert len(agent.received_messages) == provider.writes == 1
    cold_provider = JsonStateProvider(provider.raw)
    cold_agent = _CaptureAgent()
    duplicate = await AgentEntity(cold_agent, state_provider=cold_provider).run(request)  # type: ignore[arg-type]
    assert duplicate.text == "captured-answer"
    assert cold_agent.received_messages == [] and cold_provider.writes == 0
    _same(provider.raw, committed)
    _same(cold_provider.raw, committed)
    _same(_request_snapshot(request), request_before)
    _same(raw, before)
