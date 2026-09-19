# Copyright (c) Microsoft. All rights reserved.

"""The private history bridge must reject non-v2 bindings before any mutation."""

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import SUMMARY_OF_MESSAGE_IDS_KEY, AgentResponse, AgentSession, Content, Message, SessionContext
from test_shared_history_provider import _bound, _CanonicalStateProvider

from agent_framework_durabletask._history_provider import (
    POSITIONS_KEY,
    WORKING_BUFFER_KEY,
    DurableHistoryBinding,
    DurableHistoryProvider,
)
from agent_framework_durabletask._shared_agent_state import DurableAgentState


def _snapshot(value: Any) -> str:
    return json.dumps(value, sort_keys=True, allow_nan=False)


def _state_snapshot(state: DurableAgentState) -> str:
    # Capture every data field without asking the root serializer to admit a
    # future version. The schema label itself is captured separately, unchanged.
    return _snapshot({
        "schemaVersion": state.schema_version,
        "unknownRoot": state.unknown_fields,
        "data": state.data.to_dict(),
    })


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0", "2.0.1", "2.1.0", "3.0.0"])
@pytest.mark.parametrize("service_owned", [False, True], ids=["durable-owned", "service-owned"])
@pytest.mark.parametrize(
    ("operation", "working_kind"),
    [
        ("get_messages", "none"),
        ("get_messages", "empty"),
        ("get_messages", "populated"),
        ("save_messages", "none"),
        ("save_messages", "empty"),
        ("save_messages", "populated"),
        ("_append_messages", "none"),
        ("_append_messages", "empty"),
        ("_positions", "populated"),
        ("before_run", "populated"),
        ("after_run", "populated"),
        ("flush", "empty"),
        ("flush", "populated"),
        ("finalize_failed_run", "populated"),
    ],
)
async def test_non_v2_binding_preserves_stored_working_and_pending_state(
    version: str, service_owned: bool, operation: str, working_kind: str
) -> None:
    raw: dict[str, Any] = {
        "schemaVersion": version if version.startswith("1.") else "2.0.0",
        "futureRoot": {"values": [None, False, 0, 0.0]},
        "data": {
            "conversationHistory": [
                {
                    "$type": "request",
                    "correlationId": "current",
                    "futureEntry": {"keep": [None, False, 0, 0.0]},
                    "messages": [
                        {"role": "user", "contents": [{"$type": "text", "text": "anonymous"}]},
                        {
                            "role": "user",
                            "messageId": "duplicate",
                            "contents": [{"$type": "text", "text": "first"}],
                        },
                        {
                            "role": "assistant",
                            "messageId": "duplicate",
                            "contents": [{"$type": "functionCall", "callId": "call", "name": "lookup"}],
                            "futureMessage": {"keep": [None, False, 0, 0.0]},
                        },
                    ],
                }
            ],
            "session": {"session_id": "session", "state": {"other": {"keep": [False, 0]}}},
            "futureData": {"keep": [None, False, 0, 0.0]},
        },
    }
    if raw["schemaVersion"] == "2.0.0":
        raw["data"].update(terminalResults={}, completionReceipts={})
    raw_before = _snapshot(raw)
    owner = _CanonicalStateProvider()
    owner.state = DurableAgentState.from_dict(raw)
    # The parser rejects future roots already. A direct or replaced binding must
    # still enforce the exact version rather than trusting that earlier parse.
    owner.state.schema_version = version
    canonical = owner.state
    data = canonical.data
    transcript = data.conversation_history
    entry = transcript[0]
    stored_messages = entry.messages
    originals = tuple((message, message.contents, tuple(message.contents)) for message in stored_messages)
    stored_ids = [(message.message_id, message.public_message_id) for message in stored_messages]
    stored_before = _state_snapshot(canonical)

    history = DurableHistoryProvider()
    summary = Message(
        "assistant",
        ["pending summary"],
        message_id="summary",
        additional_properties={SUMMARY_OF_MESSAGE_IDS_KEY: ["duplicate"]},
    )
    stale = Message("assistant", ["stale loaded occurrence"], message_id="stale")
    stale._durable_history_id = "stale"  # type: ignore[attr-defined]
    buffer = [stale, summary]
    positions = {"duplicate": (entry, 1)}
    positions_before = dict(positions)
    caller_state = {"keep": [None, False, 0, 0.0]}
    caller_before = _snapshot(caller_state)
    working: dict[str, Any] | None = None if working_kind == "none" else {}
    if working_kind == "populated":
        working = {WORKING_BUFFER_KEY: buffer, POSITIONS_KEY: positions, "caller": caller_state}
    working_values = dict(working or {})
    original_buffer = tuple(buffer)
    buffer_before = _snapshot([message.to_dict() for message in buffer])
    inputs = [Message("user", ["new input"])]
    inputs_before = _snapshot([message.to_dict() for message in inputs])
    context = SessionContext(input_messages=inputs)
    response = AgentResponse(messages=[Message("assistant", ["new response"])])
    context._response = response
    context_before = _snapshot([message.to_dict() for message in context.get_messages()])
    response_before = _snapshot(response.to_dict())
    session = AgentSession(session_id="session")
    session_before = _snapshot(session.to_dict())

    with _bound(owner, service_owns_history=service_owned) as binding:
        pending = [Message("tool", [Content.from_function_result("call", result={"keep": [False, 0]})])]
        pending_message = pending[0]
        pending_before = _snapshot([message.to_dict() for message in pending])
        binding.pending_inputs = pending
        accepted = {("occurrence", "fingerprint")}
        binding.accepted_inputs = accepted
        binding.append_response = response
        binding.append_ordinal = 7

        async def invoke() -> None:
            if operation == "get_messages":
                await history.get_messages("session", state=working)
            elif operation == "save_messages":
                await history.save_messages("session", inputs, state=working)
            elif operation == "_append_messages":
                history._append_messages(binding, inputs, state=working)
            elif operation == "_positions":
                history._positions(binding)
            else:
                assert working is not None
                if operation in ("before_run", "after_run"):
                    hook = history.before_run if operation == "before_run" else history.after_run
                    await hook(
                        agent=SimpleNamespace(require_per_service_call_history_persistence=True),
                        session=session,
                        context=context,
                        state=working,
                    )
                elif operation == "flush":
                    history.flush(working)
                else:
                    assert operation == "finalize_failed_run"
                    history.finalize_failed_run(working)

        # Service-owned flush is a pure no-op. Hooks that clear pending inputs
        # must reject even when the service owns history.
        for _ in range(2):
            if service_owned and operation == "flush":
                await invoke()
            else:
                with pytest.raises(ValueError, match="Legacy state is read-only"):
                    await invoke()
            assert _state_snapshot(canonical) == stored_before
            assert binding.pending_inputs is pending
            assert len(pending) == 1 and pending[0] is pending_message
            assert _snapshot([message.to_dict() for message in pending]) == pending_before
            assert binding.accepted_inputs is accepted and accepted == {("occurrence", "fingerprint")}
            assert binding.append_response is response and binding.append_ordinal == 7

    assert owner.state is canonical and canonical.data is data
    assert canonical.schema_version == version
    assert data.conversation_history is transcript
    assert len(transcript) == 1 and transcript[0] is entry
    assert entry.messages is stored_messages and len(stored_messages) == len(originals)
    for message, (original, contents, items) in zip(stored_messages, originals, strict=True):
        assert message is original and message.contents is contents
        assert len(contents) == len(items)
        assert all(content is item for content, item in zip(contents, items, strict=True))
    assert [(message.message_id, message.public_message_id) for message in stored_messages] == stored_ids
    assert _snapshot(raw) == raw_before
    assert owner.persist_count == 0
    if working is not None:
        assert working.keys() == working_values.keys()
        assert all(working[key] is value for key, value in working_values.items())
    assert len(buffer) == len(original_buffer)
    assert all(message is original for message, original in zip(buffer, original_buffer, strict=True))
    assert _snapshot([message.to_dict() for message in buffer]) == buffer_before
    assert stale._durable_history_id == "stale"  # type: ignore[attr-defined]
    assert not hasattr(summary, "_durable_history_id")
    assert positions == positions_before
    assert all(positions[key] is value for key, value in positions_before.items())
    assert _snapshot(caller_state) == caller_before
    assert _snapshot([message.to_dict() for message in inputs]) == inputs_before
    assert _snapshot([message.to_dict() for message in context.get_messages()]) == context_before
    assert context.response is response and _snapshot(response.to_dict()) == response_before
    assert _snapshot(session.to_dict()) == session_before


def test_v2_version_guard_does_not_validate_the_complete_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    owner = _CanonicalStateProvider()
    serialize = Mock(side_effect=AssertionError("Version admission must not serialize the entire snapshot"))
    prepare = Mock(side_effect=AssertionError("Version admission must not validate the entire snapshot"))
    monkeypatch.setattr(owner.state, "to_dict", serialize)
    monkeypatch.setattr(owner.state, "prepare_for_write", prepare)
    binding = DurableHistoryBinding(owner)

    for _ in range(2):
        DurableHistoryProvider._require_writable_history(binding)

    serialize.assert_not_called()
    prepare.assert_not_called()
