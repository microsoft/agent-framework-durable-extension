# Copyright (c) Microsoft. All rights reserved.

"""Context admission precedes durable effects without changing public message identity."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from agent_framework import Agent, ContextProvider, Message, SessionContext, tool
from durabletask.task import CompletableTask
from test_durable_history_provider import _ingestion_messages
from test_revision_contract import JsonStateProvider, RecordingChatClient

from agent_framework_durabletask import (
    AgentEntity,
    AgentSessionId,
    DurableAgentSession,
    DurableAgentState,
    DurableAgentStateRequest,
    DurableHistoryProvider,
    RunRequest,
)
from agent_framework_durabletask import _entities as entities_module
from agent_framework_durabletask import _models as models_module
from agent_framework_durabletask._executors import ClientAgentExecutor, OrchestrationAgentExecutor
from agent_framework_durabletask._history_provider import current_durable_history_binding
from agent_framework_durabletask._message_identity import message_identity
from agent_framework_durabletask._shared_state_validation import validate_shared_state
from agent_framework_durabletask._workflows.dt_context import DurableTaskWorkflowContext

_BLANK_IDS = (
    ("empty", ""),
    ("space", " "),
    ("spaces", "   "),
    ("tab", "\t"),
    ("crlf", "\r\n"),
    ("nbsp", "\u00a0"),
    ("em-space", "\u2003"),
    ("mixed-whitespace", " \t\r\n\u00a0\u2003"),
)
_VALID_IDS = (
    pytest.param("occurrence", id="ordinary"),
    pytest.param(" occurrence with spaces ", id="space-padding"),
    pytest.param("\t\u00a0occurrence\u2003\r\n", id="unicode-and-control-padding"),
    pytest.param("x" * 257, id="over-256-characters"),
    pytest.param(" " + "雪" * 300 + " ", id="long-padded-unicode"),
)
_CONTEXT = [{"role": "user", "message_id": "public-id", "contents": [{"type": "text", "text": "projected"}]}]
_PREVIEW = "logging only, never the context fallback"

# These values survive JSON unchanged. Python-only container mistakes are covered separately.
_INVALID_CONTEXTS = [
    *[
        pytest.param([{**_CONTEXT[0], "message_id": value}], None, "message_id", id=f"public-id-{name}")
        for name, value in (("zero", 0), ("false", False), ("number", 17), ("list", []), ("object", {}))
    ],
    pytest.param("not-a-list", None, "contextMessages", id="messages-string"),
    pytest.param({}, None, "contextMessages", id="messages-object"),
    pytest.param(False, None, "contextMessages", id="messages-bool"),
    pytest.param(0, None, "contextMessages", id="messages-number"),
    pytest.param([None], None, "contextMessages", id="message-null"),
    pytest.param([False], None, "contextMessages", id="message-bool"),
    pytest.param([1], None, "contextMessages", id="message-number"),
    pytest.param(["not-an-object"], None, "contextMessages", id="message-string"),
    pytest.param([{}, None], ["first", "second"], "contextMessages", id="second-message-null"),
    pytest.param(_CONTEXT, "occurrence", "contextMessageIds", id="ids-string"),
    pytest.param(_CONTEXT, {}, "contextMessageIds", id="ids-object"),
    pytest.param(_CONTEXT, False, "contextMessageIds", id="ids-bool"),
    pytest.param(_CONTEXT, 1, "contextMessageIds", id="ids-number"),
    pytest.param(_CONTEXT, [], "contextMessageIds", id="ids-too-short"),
    pytest.param(_CONTEXT, ["first", "second"], "contextMessageIds", id="ids-too-long"),
    pytest.param(_CONTEXT, [None], "contextMessageIds", id="id-null"),
    pytest.param(_CONTEXT, [False], "contextMessageIds", id="id-bool"),
    pytest.param(_CONTEXT, [0], "contextMessageIds", id="id-number"),
    pytest.param(_CONTEXT, [{}], "contextMessageIds", id="id-object"),
    pytest.param(_CONTEXT, [[]], "contextMessageIds", id="id-list"),
    pytest.param(None, [], "contextMessageIds", id="empty-ids-without-messages"),
    pytest.param(None, ["occurrence"], "contextMessageIds", id="ids-without-messages"),
    pytest.param([], ["occurrence"], "contextMessageIds", id="id-with-empty-context"),
    *[pytest.param(_CONTEXT, [blank], "contextMessageIds", id=f"id-{name}") for name, blank in _BLANK_IDS],
    *[
        pytest.param(_CONTEXT * 2, ["valid-first", blank], "contextMessageIds", id=f"second-id-{name}")
        for name, blank in _BLANK_IDS
    ],
]
_MUTATIONS = [
    pytest.param("context_messages", "set", None, "contextMessageIds", id="remove-context-with-ids"),
    pytest.param("context_messages", "set", "bad", "contextMessages", id="replace-context-with-string"),
    pytest.param("context_messages", "set", {}, "contextMessages", id="replace-context-with-object"),
    pytest.param("context_messages", "set", tuple(_CONTEXT), "contextMessages", id="replace-context-with-tuple"),
    pytest.param("context_messages", "item", None, "contextMessages", id="replace-message-in-place"),
    pytest.param("context_messages", "append", None, "contextMessages", id="append-invalid-message"),
    pytest.param("context_messages", "append", {}, "contextMessageIds", id="append-unpaired-message"),
    pytest.param("context_messages", "clear", None, "contextMessageIds", id="clear-messages-in-place"),
    pytest.param("context_message_ids", "set", "bad", "contextMessageIds", id="replace-ids-with-string"),
    pytest.param("context_message_ids", "set", {}, "contextMessageIds", id="replace-ids-with-object"),
    pytest.param("context_message_ids", "set", False, "contextMessageIds", id="replace-ids-with-bool"),
    pytest.param("context_message_ids", "set", ("occurrence",), "contextMessageIds", id="replace-ids-with-tuple"),
    pytest.param("context_message_ids", "item", None, "contextMessageIds", id="replace-id-with-null"),
    pytest.param("context_message_ids", "item", 1, "contextMessageIds", id="replace-id-with-number"),
    pytest.param("context_message_ids", "append", "extra", "contextMessageIds", id="append-unpaired-id"),
    pytest.param("context_message_ids", "clear", None, "contextMessageIds", id="clear-ids-in-place"),
    *[
        pytest.param(
            "context_message_ids",
            operation,
            [blank] if operation == "set" else blank,
            "contextMessageIds",
            id=f"{operation}-id-{name}",
        )
        for operation in ("set", "item")
        for name, blank in _BLANK_IDS
    ],
]


def _construct(source: str, messages: Any, occurrence_ids: Any) -> RunRequest:
    if source == "constructor":
        return RunRequest(
            message=_PREVIEW,
            correlation_id="completed",
            context_messages=messages,
            context_message_ids=occurrence_ids,
        )
    payload = {
        "message": _PREVIEW,
        "correlationId": "completed",
        "contextMessages": messages,
        "contextMessageIds": occurrence_ids,
    }
    return RunRequest.from_dict(payload) if source == "dict" else RunRequest.from_json(json.dumps(payload))


def _mutated_request(field: str, operation: str, value: Any) -> RunRequest:
    request = _construct("constructor", deepcopy(_CONTEXT), ["occurrence"])
    if operation == "set":
        setattr(request, field, deepcopy(value))
    else:
        values = getattr(request, field)
        assert isinstance(values, list)
        if operation == "item":
            values[0] = deepcopy(value)
        elif operation == "append":
            values.append(deepcopy(value))
        else:
            assert operation == "clear"
            values.clear()
    return request


def _snapshot(request: RunRequest | dict[str, Any] | str) -> Any:
    # An invalid object's serializer is an admission boundary, not a snapshot helper.
    return deepcopy(vars(request) if isinstance(request, RunRequest) else request)


@pytest.mark.parametrize("source", ["constructor", "dict", "json"])
@pytest.mark.parametrize(("messages", "occurrence_ids", "error"), _INVALID_CONTEXTS)
def test_request_entry_points_reject_invalid_context(
    source: str, messages: Any, occurrence_ids: Any, error: str
) -> None:
    messages, occurrence_ids = deepcopy(messages), deepcopy(occurrence_ids)
    before = deepcopy((messages, occurrence_ids))
    with pytest.raises(ValueError, match=error):
        _construct(source, messages, occurrence_ids)
    assert (messages, occurrence_ids) == before


@pytest.mark.parametrize("source", ["dict", "json"])
@pytest.mark.parametrize("occurrence_ids", [[], ["occurrence"]], ids=["empty-ids", "nonempty-ids"])
def test_wire_ids_require_context_messages_even_when_the_field_is_omitted(
    source: str, occurrence_ids: list[str]
) -> None:
    payload = {"message": _PREVIEW, "correlationId": "completed", "contextMessageIds": occurrence_ids}
    before = deepcopy(payload)
    with pytest.raises(ValueError, match="contextMessageIds"):
        if source == "dict":
            RunRequest.from_dict(payload)
        else:
            RunRequest.from_json(json.dumps(payload))
    assert payload == before


@pytest.mark.parametrize("source", ["constructor", "dict"])
@pytest.mark.parametrize(
    ("messages", "occurrence_ids", "error"),
    [
        pytest.param(tuple(_CONTEXT), ["occurrence"], "contextMessages", id="tuple-messages"),
        pytest.param(_CONTEXT, ("occurrence",), "contextMessageIds", id="tuple-ids"),
        pytest.param(_CONTEXT, {"occurrence"}, "contextMessageIds", id="set-ids"),
    ],
)
def test_request_does_not_coerce_non_list_context_containers(
    source: str, messages: Any, occurrence_ids: Any, error: str
) -> None:
    with pytest.raises(ValueError, match=error):
        _construct(source, deepcopy(messages), deepcopy(occurrence_ids))


@pytest.mark.parametrize("source", ["constructor", "dict", "json"])
@pytest.mark.parametrize("occurrence_id", _VALID_IDS)
def test_valid_occurrence_ids_are_preserved_not_normalized(source: str, occurrence_id: str) -> None:
    request = _construct(source, deepcopy(_CONTEXT), [occurrence_id])
    before = _snapshot(request)
    request.validate_context()
    wire = request.to_dict()
    restored = RunRequest.from_json(json.dumps(wire))
    assert wire["contextMessageIds"] == restored.context_message_ids == [occurrence_id]
    assert wire["contextMessages"] == restored.context_messages == _CONTEXT
    assert _snapshot(request) == before


@pytest.mark.parametrize("source", ["constructor", "dict", "json"])
@pytest.mark.parametrize(
    ("messages", "occurrence_ids"),
    [
        pytest.param(None, None, id="no-context"),
        pytest.param([], None, id="empty-context-without-ids"),
        pytest.param([], [], id="empty-context-with-empty-ids"),
        pytest.param(_CONTEXT, None, id="messages-without-explicit-ids"),
        pytest.param([{}, {}], ["same", "same"], id="shape-only-and-duplicate-ids"),
    ],
)
def test_context_validator_accepts_optional_empty_and_shape_only_context(
    source: str, messages: Any, occurrence_ids: Any
) -> None:
    request = _construct(source, deepcopy(messages), deepcopy(occurrence_ids))
    request.validate_context()
    wire = request.to_dict()
    restored = RunRequest.from_json(json.dumps(wire))
    assert restored.context_messages == messages
    assert restored.context_message_ids == occurrence_ids
    assert ("contextMessages" in wire) is (messages is not None)
    assert ("contextMessageIds" in wire) is (occurrence_ids is not None)
    if messages == []:
        assert DurableAgentStateRequest.from_run_request(restored).messages == []


@pytest.mark.parametrize("clear_messages", [False, True], ids=["remove-explicit-ids", "clear-both-lists"])
def test_valid_mutation_can_remove_ids_or_clear_the_paired_context(clear_messages: bool) -> None:
    request = _construct("constructor", deepcopy(_CONTEXT), ["occurrence"])
    assert request.context_messages is not None and request.context_message_ids is not None
    if clear_messages:
        request.context_messages.clear()
        request.context_message_ids.clear()
    else:
        request.context_message_ids = None
    before = _snapshot(request)
    request.validate_context()
    restored = RunRequest.from_dict(request.to_dict())
    assert _snapshot(request) == before
    assert restored.context_messages == ([] if clear_messages else _CONTEXT)
    assert restored.context_message_ids == ([] if clear_messages else None)
    if clear_messages:
        assert DurableAgentStateRequest.from_run_request(restored).messages == []


@pytest.mark.parametrize(("field", "operation", "value", "error"), _MUTATIONS)
def test_validate_context_rechecks_attribute_and_in_place_mutations(
    field: str, operation: str, value: Any, error: str
) -> None:
    request = _mutated_request(field, operation, value)
    before = _snapshot(request)
    with pytest.raises(ValueError, match=error):
        request.validate_context()
    assert _snapshot(request) == before


@pytest.mark.parametrize(("field", "operation", "value", "error"), _MUTATIONS)
def test_to_dict_rejects_mutated_context_before_other_serialization(
    field: str, operation: str, value: Any, error: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    request = _mutated_request(field, operation, value)
    request.response_format = cast(Any, str)
    before = _snapshot(request)
    serialize = Mock(side_effect=AssertionError("response format serialized before context admission"))
    monkeypatch.setattr(models_module, "serialize_response_format", serialize)
    with pytest.raises(ValueError, match=error):
        request.to_dict()
    serialize.assert_not_called()
    assert _snapshot(request) == before


class _InputProbe(ContextProvider):
    def __init__(self, *, fail_before: bool = False) -> None:
        super().__init__("occurrence-admission-probe")
        self.fail_before = fail_before
        self.inputs: list[list[Message]] = []
        self.after_calls = 0

    async def before_run(self, *, context: SessionContext, **kwargs: Any) -> None:
        self.inputs.append(deepcopy(context.input_messages))
        if self.fail_before:
            raise RuntimeError("input was not accepted by a history or model provider")

    async def after_run(self, **kwargs: Any) -> None:
        self.after_calls += 1


def _agent(client: RecordingChatClient, probe: _InputProbe, **kwargs: Any) -> Agent:
    return Agent(
        client=cast(Any, client),
        name="occurrence-admission",
        context_providers=[probe, DurableHistoryProvider(prune_excluded=False)],
        **kwargs,
    )


def _completed_raw() -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    common = {
        "correlationId": "completed",
        "outcome": "succeeded",
        "completedAt": (now - timedelta(minutes=1)).isoformat(),
        "resultExpiresAt": (now + timedelta(hours=1)).isoformat(),
    }
    raw = {
        "schemaVersion": "2.0.0",
        "data": {
            "conversationHistory": [],
            "terminalResults": {
                "completed": {
                    **common,
                    "response": {
                        "messages": [
                            {"role": "assistant", "contents": [{"$type": "text", "text": "already committed"}]}
                        ],
                    },
                },
            },
            "completionReceipts": {"completed": {**common, "resultState": "available"}},
        },
    }
    validate_shared_state(raw)
    response = DurableAgentState.from_dict(raw).try_get_agent_response("completed")
    assert response is not None and response.text == "already committed"
    return raw


async def _assert_entity_rejects_before_work(
    request: RunRequest | dict[str, Any] | str,
    error: str,
    completed: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = JsonStateProvider(_completed_raw() if completed else None)
    raw_before = deepcopy(provider.raw)
    request_before = _snapshot(request)
    client, probe = RecordingChatClient(), _InputProbe()
    tool_calls: list[str] = []

    @tool
    def admission_tool(value: str) -> str:
        """Record an invocation that must never occur for a rejected request."""
        tool_calls.append(value)
        return value

    callback = Mock()
    callback.on_streaming_response_update = AsyncMock()
    callback.on_agent_response = AsyncMock()
    entity = AgentEntity(_agent(client, probe, tools=[admission_tool]), callback, state_provider=provider)
    history = entity._find_durable_history_provider()
    assert history is not None and provider._state_cache is None
    guards: list[Mock] = []

    def guard(target: Any, name: str, *, asynchronous: bool = False) -> None:
        factory = AsyncMock if asynchronous else Mock
        witness = factory(name=name, side_effect=AssertionError(f"{name} ran before context admission"))
        monkeypatch.setattr(target, name, witness)
        guards.append(witness)

    guard(provider, "_get_state_dict")
    guard(provider, "_get_session_id_from_entity")
    guard(provider, "_set_state_dict")
    guard(provider, "persist_state")
    guard(DurableAgentState, "prepare_for_write")
    guard(DurableAgentState, "try_get_agent_response")
    guard(DurableAgentState, "expire_responses")
    guard(entities_module, "prepare_history_owner")
    guard(entity, "_enforce_retention", asynchronous=True)
    guard(entity, "persist_state")
    guard(client, "get_response")
    for name in ("before_run", "get_messages", "save_messages", "after_run"):
        guard(history, name, asynchronous=True)
    guard(history, "flush")

    with pytest.raises(ValueError, match=error):
        await entity.run(request)

    for witness in guards:
        witness.assert_not_called()
    assert client.received_messages == probe.inputs == tool_calls == []
    assert probe.after_calls == 0
    callback.on_streaming_response_update.assert_not_called()
    callback.on_agent_response.assert_not_called()
    assert provider.writes == 0 and provider.raw == raw_before
    assert provider._state_cache is None and provider._persisted_state_snapshot is None
    assert _snapshot(request) == request_before
    assert current_durable_history_binding() is None


@pytest.mark.parametrize("completed", [False, True], ids=["empty-state", "completed-correlation"])
@pytest.mark.parametrize(("field", "operation", "value", "error"), _MUTATIONS)
async def test_entity_revalidates_mutated_requests_before_any_durable_work(
    field: str, operation: str, value: Any, error: str, completed: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _assert_entity_rejects_before_work(_mutated_request(field, operation, value), error, completed, monkeypatch)


@pytest.mark.parametrize("source", ["dict", "json"])
@pytest.mark.parametrize("completed", [False, True], ids=["empty-state", "completed-correlation"])
@pytest.mark.parametrize(("messages", "occurrence_ids", "error"), _INVALID_CONTEXTS)
async def test_entity_rejects_invalid_wire_context_before_any_durable_work(
    source: str, messages: Any, occurrence_ids: Any, error: str, completed: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = {
        "message": _PREVIEW,
        "correlationId": "completed",
        "contextMessages": deepcopy(messages),
        "contextMessageIds": deepcopy(occurrence_ids),
    }
    request = payload if source == "dict" else json.dumps(payload)
    await _assert_entity_rejects_before_work(request, error, completed, monkeypatch)


def _native_host() -> Mock:
    native = Mock()
    native.instance_id = "occurrence-admission-orchestration"
    native.new_uuid.return_value = str(UUID(int=1))
    return native


def _executor(kind: str, native: Mock) -> Any:
    if kind == "client":
        return ClientAgentExecutor(native, max_poll_retries=1, poll_interval_seconds=0)
    if kind == "dt":
        return OrchestrationAgentExecutor(native)
    assert kind == "af"
    module = pytest.importorskip("agent_framework_azurefunctions._orchestration")
    return module.AzureFunctionsAgentExecutor(native)


def _session() -> DurableAgentSession:
    return DurableAgentSession(
        durable_session_id=AgentSessionId(name="target", key=str(UUID(int=2))),
        session_id=str(UUID(int=3)),
    )


def _workflow_adapter(kind: str, native: Mock) -> Any:
    if kind == "dt":
        native.call_entity.return_value = CompletableTask()
        return DurableTaskWorkflowContext(native)
    assert kind == "af"
    module = pytest.importorskip("agent_framework_azurefunctions._workflow_af_context")
    tasks = pytest.importorskip("azure.durable_functions.models.Task")
    actions = pytest.importorskip("azure.durable_functions.models.actions.NoOpAction")
    native.call_entity.return_value = tasks.AtomicTask(0, actions.NoOpAction())
    return module.AzureFunctionsWorkflowContext(native)


@pytest.mark.parametrize("kind", ["client", "dt", "af"])
@pytest.mark.parametrize("wait_for_response", [False, True], ids=["signal", "wait"])
@pytest.mark.parametrize("operation", ["set", "item"], ids=["attribute", "in-place"])
@pytest.mark.parametrize("blank", [pytest.param(value, id=name) for name, value in _BLANK_IDS])
def test_real_executors_reject_mutated_ids_before_scheduling_or_polling(
    kind: str, wait_for_response: bool, operation: str, blank: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    native = _native_host()
    executor = _executor(kind, native)
    request = _mutated_request("context_message_ids", operation, [blank] if operation == "set" else blank)
    request.wait_for_response = wait_for_response
    before = _snapshot(request)
    session = _session()
    session_before = deepcopy(session.to_dict())
    native.signal_entity.side_effect = AssertionError("invalid request scheduled a signal")
    native.call_entity.side_effect = AssertionError("invalid request scheduled a call")
    native.get_entity.side_effect = AssertionError("invalid request polled entity state")
    acceptance = Mock(side_effect=AssertionError("invalid request returned acceptance"))
    monkeypatch.setattr(executor, "_create_acceptance_response", acceptance)
    poll = Mock(side_effect=AssertionError("invalid request entered polling"))
    if kind == "client":
        monkeypatch.setattr(executor, "_poll_for_agent_response", poll)

    with pytest.raises(ValueError, match="contextMessageIds"):
        executor.run_durable_agent("target", request, session)

    native.signal_entity.assert_not_called()
    native.call_entity.assert_not_called()
    native.get_entity.assert_not_called()
    poll.assert_not_called()
    acceptance.assert_not_called()
    assert _snapshot(request) == before and session.to_dict() == session_before


@pytest.mark.parametrize("kind", ["dt", "af"])
@pytest.mark.parametrize("blank", [pytest.param(value, id=name) for name, value in _BLANK_IDS])
def test_workflow_adapters_reject_blank_occurrences_before_scheduling(kind: str, blank: str) -> None:
    native = _native_host()
    adapter = _workflow_adapter(kind, native)
    messages, ids = deepcopy(_CONTEXT), [blank]
    before = deepcopy((messages, ids))
    with pytest.raises(ValueError, match="contextMessageIds"):
        adapter.prepare_agent_task("target", _PREVIEW, native.instance_id, messages, ids)
    native.call_entity.assert_not_called()
    native.signal_entity.assert_not_called()
    assert (messages, ids) == before


@pytest.mark.parametrize("kind", ["client", "dt", "af"])
@pytest.mark.parametrize("ids", [None, []], ids=["absent-ids", "empty-ids"])
async def test_empty_context_survives_real_dispatch_and_entity_ingestion(kind: str, ids: list[str] | None) -> None:
    native = _native_host()
    if kind == "client":
        executor = _executor(kind, native)
        request = executor.get_run_request(
            _PREVIEW,
            options={"wait_for_response": False},
            context_messages=[],
            context_message_ids=deepcopy(ids),
        )
        executor.run_durable_agent("target", request, _session())
        native.signal_entity.assert_called_once()
        native.get_entity.assert_not_called()
        payload = native.signal_entity.call_args.args[2]
    else:
        adapter = _workflow_adapter(kind, native)
        adapter.prepare_agent_task("target", _PREVIEW, native.instance_id, [], deepcopy(ids))
        native.call_entity.assert_called_once()
        native.signal_entity.assert_not_called()
        payload = native.call_entity.call_args.args[2]
    wire = json.loads(json.dumps(payload))
    assert wire["contextMessages"] == [] and wire["message"] == _PREVIEW
    assert ("contextMessageIds" in wire) is (ids is not None)
    restored = RunRequest.from_dict(wire)
    assert restored.context_messages == [] and restored.context_message_ids == ids
    assert DurableAgentStateRequest.from_run_request(restored).messages == []
    client, probe, provider = RecordingChatClient(), _InputProbe(), JsonStateProvider()

    response = await AgentEntity(_agent(client, probe), state_provider=provider).run(restored)

    assert response.text == "reply-1"
    assert probe.inputs == client.received_messages == [[]]
    assert _ingestion_messages(provider.raw) == {} and provider.writes == 1


def _projection(correlation: str, messages: list[Message], ids: list[str] | None) -> RunRequest:
    return RunRequest(
        message=_PREVIEW,
        correlation_id=correlation,
        context_messages=[message.to_dict() for message in messages],
        context_message_ids=deepcopy(ids),
    )


def _public_inputs(messages: list[Message]) -> list[tuple[str | None, str]]:
    return [(message.message_id, message.text) for message in messages if message.role == "user"]


@pytest.mark.parametrize("occurrence_id", _VALID_IDS)
async def test_exact_occurrence_ids_and_revisions_survive_cold_repeated_delivery(occurrence_id: str) -> None:
    original = Message("user", ["original"], message_id=" public id ")
    revised = Message("user", ["revised"], message_id=" public id ")
    raw: dict[str, Any] = {}
    expected_receipts: list[str] = []
    for index, (message, delivered) in enumerate(((original, True), (original, False), (revised, True))):
        client, probe = RecordingChatClient(), _InputProbe()
        provider = JsonStateProvider(json.loads(json.dumps(raw)))
        request = _projection(f"revision-{index}", [message], [occurrence_id])
        before = _snapshot(request)
        response = await AgentEntity(_agent(client, probe), state_provider=provider).run(request)
        assert response.text == "reply-1"
        assert _public_inputs(probe.inputs[0]) == ([(message.message_id, message.text)] if delivered else [])
        if delivered:
            expected_receipts.append(message_identity(message))
        assert _ingestion_messages(provider.raw) == {occurrence_id: expected_receipts}
        assert _public_inputs(client.received_messages[0]) == [
            (" public id ", text) for text in (["original", "revised"] if index == 2 else ["original"])
        ]
        assert _snapshot(request) == before and request.context_message_ids == [occurrence_id]
        assert DurableAgentState.from_dict(provider.raw).to_dict() == provider.raw
        raw = provider.raw


@pytest.mark.parametrize("changed_revision", [False, True], ids=["equal-deduplicates", "changed-delivers-both"])
async def test_duplicate_occurrence_ids_use_content_fingerprints_not_uniqueness(changed_revision: bool) -> None:
    first = Message("user", ["first"], message_id="same-public-id")
    second = Message("user", ["changed" if changed_revision else "first"], message_id="same-public-id")
    messages = [first, second]
    expected = messages if changed_revision else messages[:1]
    client, probe, provider = RecordingChatClient(), _InputProbe(), JsonStateProvider()
    request = _projection("duplicates", messages, ["same-occurrence", "same-occurrence"])
    before = _snapshot(request)
    response = await AgentEntity(_agent(client, probe), state_provider=provider).run(request)
    assert response.text == "reply-1"
    assert _public_inputs(probe.inputs[0]) == _public_inputs(expected)
    assert _public_inputs(client.received_messages[0]) == _public_inputs(expected)
    receipts = {"same-occurrence": [message_identity(message) for message in expected]}
    assert _ingestion_messages(provider.raw) == receipts and _snapshot(request) == before

    cold = JsonStateProvider(json.loads(json.dumps(provider.raw)))
    cold_probe, cold_client = _InputProbe(), RecordingChatClient()
    repeated = _projection("duplicates-cold", messages, ["same-occurrence", "same-occurrence"])
    response = await AgentEntity(_agent(cold_client, cold_probe), state_provider=cold).run(repeated)
    assert response.text == "reply-1" and cold_probe.inputs == [[]]
    assert _public_inputs(cold_client.received_messages[0]) == _public_inputs(expected)
    assert _ingestion_messages(cold.raw) == receipts


@pytest.mark.parametrize("public_id", [pytest.param(value, id=name) for name, value in _BLANK_IDS])
@pytest.mark.parametrize("explicit_ids", [False, True], ids=["anonymous-fallback", "explicit-occurrence"])
async def test_whitespace_public_ids_remain_legal_and_only_explicit_occurrences_deduplicate(
    public_id: str, explicit_ids: bool
) -> None:
    message = Message("user", ["whitespace public identity"], message_id=public_id)
    original = deepcopy(message.to_dict())
    ids = [" explicit occurrence "] if explicit_ids else None
    raw: dict[str, Any] = {}
    for index in range(3):
        provider = JsonStateProvider(json.loads(json.dumps(raw)))
        client, probe = RecordingChatClient(), _InputProbe()
        request = _projection(f"public-id-{index}", [message], ids)
        before = _snapshot(request)
        response = await AgentEntity(_agent(client, probe), state_provider=provider).run(request)
        assert response.text == "reply-1"
        expected_input = [(public_id, message.text)] if index == 0 or not explicit_ids else []
        assert _public_inputs(probe.inputs[0]) == expected_input
        copies = 1 if explicit_ids else index + 1
        assert _public_inputs(client.received_messages[0]) == [(public_id, message.text)] * copies
        receipts = {ids[0]: [message_identity(message)]} if ids is not None else {}
        assert _ingestion_messages(provider.raw) == receipts
        rows = [
            row
            for entry in provider.raw["data"]["conversationHistory"]
            for row in entry["messages"]
            if row["role"] == "user"
        ]
        assert [row["messageId"] for row in rows] == [public_id] * copies
        assert _snapshot(request) == before and message.to_dict() == original
        assert DurableAgentState.from_dict(provider.raw).to_dict() == provider.raw
        raw = provider.raw


@pytest.mark.parametrize("public_id", [pytest.param(value, id=name) for name, value in _BLANK_IDS])
@pytest.mark.parametrize("explicit_ids", [False, True], ids=["anonymous-fallback", "explicit-occurrence"])
async def test_failure_before_acceptance_does_not_restore_blank_or_unaccepted_receipts(
    public_id: str, explicit_ids: bool
) -> None:
    message = Message("user", ["not accepted yet"], message_id=public_id)
    ids = [" accepted only later "] if explicit_ids else None
    request = _projection("unaccepted", [message], ids)
    before = _snapshot(request)
    prior_receipts = {"prior-occurrence": [message_identity(Message("user", ["previously accepted"]))]}
    seed = DurableAgentState()
    seed.data.ingested_messages = {key: list(values) for key, values in prior_receipts.items()}
    client, probe = RecordingChatClient(), _InputProbe(fail_before=True)
    provider = JsonStateProvider(seed.to_dict())
    failed = await AgentEntity(_agent(client, probe), state_provider=provider).run(request)

    assert failed.additional_properties.get("durable_status") == "error"
    assert "input was not accepted" in failed.text
    assert _public_inputs(probe.inputs[0]) == [(public_id, message.text)]
    assert client.received_messages == [] and provider.writes == 1
    assert _ingestion_messages(provider.raw) == prior_receipts
    assert provider.raw["data"]["completionReceipts"]["unaccepted"]["outcome"] == "failed"
    assert _snapshot(request) == before
    assert DurableAgentState.from_dict(provider.raw).to_dict() == provider.raw
    assert current_durable_history_binding() is None

    cold = JsonStateProvider(json.loads(json.dumps(provider.raw)))
    cold_client, cold_probe = RecordingChatClient(), _InputProbe()
    retried = _projection("new-correlation", [message], ids)
    response = await AgentEntity(_agent(cold_client, cold_probe), state_provider=cold).run(retried)
    assert response.text == "reply-1"
    assert _public_inputs(cold_probe.inputs[0]) == [(public_id, message.text)]
    assert _public_inputs(cold_client.received_messages[0]) == [(public_id, message.text)]
    assert _ingestion_messages(cold.raw) == {
        **prior_receipts,
        **({ids[0]: [message_identity(message)]} if ids is not None else {}),
    }
    assert DurableAgentState.from_dict(cold.raw).to_dict() == cold.raw
