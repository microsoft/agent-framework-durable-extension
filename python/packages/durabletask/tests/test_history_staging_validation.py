# Copyright (c) Microsoft. All rights reserved.

"""Local staging regressions for PR #111 comments 4074585131 and 4074585193."""

import asyncio
import json
from copy import deepcopy
from typing import Any, cast

import pytest
from _history_acceptance_test_support import ACCEPTED, PRIOR, _input
from _history_append_test_support import CORRELATION, _append_snapshot, _CopyFailure, _ObservedAppend, _working
from _history_atomicity_test_support import _owner, _reference_check, _snapshot
from _shared_history_test_support import OLD, _bound, _CanonicalStateProvider
from agent_framework import AgentResponse, Content, Message, SessionContext

from agent_framework_durabletask._history_provider import (
    POSITIONS_KEY,
    WORKING_BUFFER_KEY,
    DurableHistoryBinding,
    DurableHistoryProvider,
)
from agent_framework_durabletask._shared_agent_state import (
    DurableAgentStateErrorResponse,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
    DurableAgentStateUsage,
)

TIMESTAMP = "2026-09-22T01:02:03.123456789+05:30"
TIMESTAMPS = [None, "not-a-timestamp", TIMESTAMP]


def _bad_response(kind: str, created_at: str | None) -> AgentResponse:
    bad = object() if kind.endswith("object") else float("nan")
    response = AgentResponse(
        messages=[Message("assistant", ["answer"])],
        created_at=created_at,
        additional_properties={"payload": {"bad": bad}} if kind.startswith("properties") else {},
        usage_details=cast(Any, {"input_token_count": bad}) if kind.startswith("usage") else None,
    )
    assert type(response) is AgentResponse and response.created_at == created_at
    metadata: Any = response.additional_properties if kind.startswith("properties") else response.usage_details
    assert metadata is not None
    assert (metadata["payload"]["bad"] if kind.startswith("properties") else metadata["input_token_count"]) is bad
    with pytest.raises((TypeError, ValueError)):
        json.dumps(deepcopy(metadata), allow_nan=False)
    return response


@pytest.mark.parametrize("created_at", TIMESTAMPS, ids=["missing", "invalid", "valid"])
@pytest.mark.parametrize("kind", ["properties-object", "properties-nan", "usage-object", "usage-nan"])
async def test_save_rejects_unencodable_response_before_publishing(kind: str, created_at: str | None) -> None:
    response = _bad_response(kind, created_at)
    # Admission is for the actual transcript entry, not the stricter terminal-response profile.
    candidate = DurableAgentStateResponse(
        CORRELATION,
        OLD,
        [DurableAgentStateMessage.from_chat_message(deepcopy(response.messages[0]))],
        extension_data=deepcopy(response.additional_properties),
        usage=DurableAgentStateUsage.from_usage(response.usage_details),
    )
    with pytest.raises(ValueError, match="strict JSON"):
        candidate.to_dict()
    caller_unchanged = _reference_check(vars(response), response.additional_properties, response.usage_details)
    owner = _owner("anonymous")
    provider = _ObservedAppend()
    state: dict[str, Any] = {"caller": {"keep": True}}
    with _bound(owner, CORRELATION) as binding:
        binding.append_ordinal = 7
        binding.append_response = response
        # Only save is inside this assertion. A later owner.to_dict failure is too late.
        with pytest.raises(ValueError, match="strict JSON"):
            await provider.save_messages("session", response.messages, state=state)
        provider.assert_unchanged()
        caller_unchanged()
        assert binding.append_response is response and binding.append_ordinal == 7
        assert set(state) == {"caller"} and owner.persist_count == 0


@pytest.mark.parametrize(
    ("created_at", "error"),
    [(None, False), ("not-a-timestamp", False), (TIMESTAMP, False), (None, True)],
    ids=["missing", "invalid", "valid", "error-missing"],
)
async def test_json_response_metadata_keeps_history_policy_and_detached_timestamp_fallback(
    created_at: str | None,
    error: bool,
) -> None:
    # Blank metadata keys and non-integer usage extensions are legal transcript JSON.
    properties: dict[str, Any] = {"": {"values": [None, False, 0, 0.0]}}
    if error:
        properties["durable_status"] = "error"
    usage: dict[str, Any] = {"input_token_count": 0.25, "output_token_count": 2, "provider": {"n": [False]}}
    response = AgentResponse(
        messages=[Message("assistant", ["answer"])],
        created_at=created_at,
        additional_properties=properties,
        usage_details=cast(Any, usage),
    )
    owner = _CanonicalStateProvider()
    state: dict[str, Any] = {}
    with _bound(owner, CORRELATION) as binding:
        binding.append_response = response
        await DurableHistoryProvider().save_messages("session", response.messages, state=state)
        entry = owner.state.data.conversation_history[0]
        assert type(entry) is (DurableAgentStateErrorResponse if error else DurableAgentStateResponse)
        if error:
            assert state[WORKING_BUFFER_KEY] == [] and state[POSITIONS_KEY] == {}
        wire = entry.to_dict()
        assert wire["$type"] == ("errorResponse" if error else "response")
        assert _snapshot(wire["extensionData"]) == _snapshot(properties)
        assert wire["usage"] == {
            "outputTokenCount": 2,
            "extensionData": {"input_token_count": 0.25, "provider": {"n": [False]}},
        }
        assert entry.created_at is not None and entry.created_at.tzinfo is not None
        if created_at == TIMESTAMP:
            assert wire["createdAt"] == TIMESTAMP
        else:
            assert wire["createdAt"] == entry.created_at.isoformat()
        before = _snapshot(owner.state.to_dict())
        response.additional_properties[""]["values"].append("later")
        usage["provider"]["n"].append("later")
        assert _snapshot(owner.state.to_dict()) == before
        assert binding.append_ordinal == 1 and owner.persist_count == 0


def _add_direct_opaque_metadata(message: Message) -> None:
    bad = object()
    message.additional_properties["bad"] = bad
    assert type(message) is Message and message.additional_properties["bad"] is bad
    core = message.to_dict()
    assert "bad" not in core["additional_properties"]
    json.dumps(core, allow_nan=False)
    # Unlike nested opaque metadata, this survives conversion, not durable admission.
    stored = DurableAgentStateMessage.from_chat_message(deepcopy(message))
    assert stored.extension_data is not None and type(stored.extension_data["bad"]) is object
    with pytest.raises(ValueError, match="strict JSON"):
        stored.to_dict()


@pytest.mark.parametrize("mode", ["none", "loaded"])
async def test_generic_request_rejects_direct_metadata_omitted_by_core_before_publishing(mode: str) -> None:
    owner = _owner("unique")
    provider = _ObservedAppend()
    messages = [Message("user", ["valid first"]), Message("user", ["opaque second"])]
    _add_direct_opaque_metadata(messages[1])
    before = _snapshot(owner.state.to_dict())
    with _bound(owner, CORRELATION) as binding:
        binding.append_ordinal = 7
        state = await _working(provider, mode)
        if state is not None:
            # No compaction edits or identity repairs should obscure this append failure.
            entry = owner.state.data.conversation_history[0]
            canonical_unchanged = _reference_check(vars(entry), *(vars(message) for message in entry.messages))
            provider.flush(state)
            canonical_unchanged()
        assert _snapshot(owner.state.to_dict()) == before
        with pytest.raises(ValueError, match="strict JSON"):
            await provider.save_messages("session", messages, state=state)
        provider.assert_unchanged()
        assert _snapshot(owner.state.to_dict()) == before
        assert binding.append_response is None and binding.append_ordinal == 7
    assert owner.persist_count == 0


async def test_generic_request_keeps_json_metadata_blank_keys_and_fractional_usage() -> None:
    properties = {"": {"values": [None, False, 0, 0.0]}, "usage": {"input_token_count": 0.25}}
    supplied = Message("user", ["request"], additional_properties=properties)
    owner = _owner("unique")
    provider = DurableHistoryProvider()
    with _bound(owner, CORRELATION) as binding:
        state = await _working(provider, "loaded")
        assert state is not None
        await provider.save_messages("session", [supplied], state=state)
        entry = owner.state.data.conversation_history[-1]
        assert type(entry) is DurableAgentStateRequest and len(owner.state.data.conversation_history) == 2
        assert _snapshot(entry.to_dict()["messages"][0]["extensionData"]) == _snapshot(properties)
        assert entry.messages[0].message_id == f"durable_request_{CORRELATION}_0_0"
        assert _snapshot(state[WORKING_BUFFER_KEY][-1].additional_properties) == _snapshot(properties)
        before = _snapshot(owner.state.to_dict())
        supplied.additional_properties[""]["values"].append("later")
        supplied.additional_properties["usage"]["input_token_count"] = 0.5
        assert _snapshot(owner.state.to_dict()) == before
        assert binding.append_ordinal == 1 and supplied.message_id is None
    assert owner.persist_count == 0


@pytest.mark.parametrize("created_at", [None, "not-a-timestamp"], ids=["missing", "invalid"])
@pytest.mark.parametrize("kind", ["properties-object", "usage-object"])
async def test_error_response_rejection_keeps_earlier_input_and_pending_delivery(
    kind: str, created_at: str | None
) -> None:
    response = _bad_response(kind, created_at)
    response.additional_properties["durable_status"] = "error"
    candidate = DurableAgentStateErrorResponse(
        CORRELATION,
        OLD,
        [DurableAgentStateMessage.from_chat_message(deepcopy(response.messages[0]))],
        extension_data=deepcopy(response.additional_properties),
        usage=DurableAgentStateUsage.from_usage(response.usage_details),
    )
    with pytest.raises(ValueError, match="strict JSON"):
        candidate.to_dict()
    caller_unchanged = _reference_check(vars(response), response.additional_properties, response.usage_details)
    owner = _CanonicalStateProvider()
    provider = _ObservedAppend()
    supplied = _input("B")
    context = SessionContext(session_id="session", input_messages=[supplied])
    context._response = response
    with _bound(owner, CORRELATION) as binding:
        binding.accepted_inputs.add(PRIOR)
        pending = binding.pending_inputs
        pending.append(supplied)
        with pytest.raises(ValueError, match="strict JSON"):
            await provider.after_run(agent=None, session=None, context=context, state={})
        provider.assert_unchanged()
        caller_unchanged()
        assert binding.pending_inputs is pending and pending == [supplied]
        assert binding.accepted_inputs == {PRIOR} and binding.append_response is None
        assert binding.append_ordinal == 1 and len(owner.state.data.conversation_history) == 1
        entry = owner.state.data.conversation_history[0]
        assert type(entry) is DurableAgentStateRequest and entry.messages[0].text == "B"
        assert entry.messages[0].ingestion_occurrence == ACCEPTED[0]
        assert entry.messages[0].ingestion_identity == ACCEPTED[1]
    assert owner.persist_count == 0


@pytest.mark.parametrize("primary", [False, True], ids=["store-only-audit", "primary"])
async def test_bad_response_metadata_keeps_earlier_input_save_without_accepting_delivery(primary: bool) -> None:
    owner = _CanonicalStateProvider()
    provider = _ObservedAppend()
    provider.load_messages = primary
    supplied = _input("B")
    context = SessionContext(session_id="session", input_messages=[supplied])
    context._response = _bad_response("properties-object", None)
    with _bound(owner, CORRELATION) as binding:
        binding.accepted_inputs.add(PRIOR)
        pending = binding.pending_inputs
        pending.append(supplied)
        with pytest.raises(ValueError, match="strict JSON"):
            await provider.after_run(agent=None, session=None, context=context, state={})
        provider.assert_unchanged()
        assert binding.pending_inputs is pending and pending == [supplied]
        assert binding.accepted_inputs == {PRIOR} and binding.append_response is None
        assert binding.append_ordinal == 1 and len(owner.state.data.conversation_history) == 1
        entry = owner.state.data.conversation_history[0]
        assert isinstance(entry, DurableAgentStateRequest) and entry.messages[0].text == "B"
        stored = entry.messages[0]
        assert stored.ingestion_occurrence == (ACCEPTED[0] if primary else None)
        assert (stored.ingestion_identity == ACCEPTED[1]) is primary
    assert owner.persist_count == 0


def _result(call_id: str) -> Message:
    return Message("tool", [Content("function_result", call_id=call_id, result=f"result:{call_id}")])


async def _start_calls(provider: DurableHistoryProvider, binding: DurableHistoryBinding) -> dict[str, Any]:
    state: dict[str, Any] = {}
    calls = Message("assistant", [Content("function_call", call_id=name, name="lookup") for name in ("one", "two")])
    binding.append_response = AgentResponse(messages=[calls])
    await provider.save_messages("session", [calls], state=state)
    binding.append_response = None
    return state


def _assert_result_batch(entry: Any) -> None:
    assert isinstance(entry, DurableAgentStateRequest)
    assert [message.message_id for message in entry.messages] == [
        f"durable_request_{CORRELATION}_1_0",
        f"durable_request_{CORRELATION}_1_1",
    ]
    assert [(row["contents"][0]["callId"], row["contents"][0]["result"]) for row in entry.to_dict()["messages"]] == [
        ("one", "result:one"),
        ("two", "result:two"),
    ]


@pytest.mark.parametrize("primary", [False, True], ids=["store-only-audit", "primary"])
async def test_failed_result_conversion_retains_pending_identity_and_corrected_retry_once(primary: bool) -> None:
    owner = _CanonicalStateProvider()
    provider = DurableHistoryProvider()
    provider.load_messages = primary
    pending = [_result("one"), _result("two")]
    cast(Any, pending[0])._durable_ingestion_receipt = ACCEPTED
    pending[1].additional_properties["payload"] = {"bad": object()}
    assert DurableAgentStateMessage.from_chat_message(deepcopy(pending[0])).contents[0].type == "functionResult"
    with pytest.raises(TypeError, match="not JSON serializable"):
        DurableAgentStateMessage.from_chat_message(deepcopy(pending[1]))
    with _bound(owner, CORRELATION) as binding:
        state = await _start_calls(provider, binding)
        earlier = owner.state.data.conversation_history[0]
        binding.pending_inputs = pending
        binding.accepted_inputs.add(PRIOR)
        unchanged = _append_snapshot(binding, state, [], None)
        for _ in range(2):
            with pytest.raises(TypeError, match="not JSON serializable"):
                provider.finalize_failed_run(state)
            assert binding.pending_inputs is pending
            unchanged()
        pending[1].additional_properties.clear()
        provider.finalize_failed_run(state)
        history = owner.state.data.conversation_history
        assert len(history) == 2 and history[0] is earlier
        _assert_result_batch(history[1])
        stored = history[1].messages[0]
        assert stored.ingestion_occurrence == (ACCEPTED[0] if primary else None)
        assert (stored.ingestion_identity == ACCEPTED[1]) is primary
        assert binding.pending_inputs == [] and binding.accepted_inputs == {PRIOR}
        assert len(pending) == 2 and all(message.message_id is None for message in pending)
        before = _snapshot(owner.state.to_dict())
        provider.finalize_failed_run(state)
        assert _snapshot(owner.state.to_dict()) == before and binding.append_ordinal == 2
    assert owner.persist_count == 0


async def test_finalizer_rejects_direct_opaque_metadata_without_consuming_pending_results() -> None:
    owner = _CanonicalStateProvider()
    provider = DurableHistoryProvider()
    pending = [_result("one"), _result("two")]
    _add_direct_opaque_metadata(pending[1])
    with _bound(owner, CORRELATION) as binding:
        state = await _start_calls(provider, binding)
        earlier = owner.state.data.conversation_history[0]
        binding.pending_inputs = pending
        binding.accepted_inputs.add(PRIOR)
        unchanged = _append_snapshot(binding, state, [], None)
        with pytest.raises(ValueError, match="strict JSON"):
            provider.finalize_failed_run(state)
        assert binding.pending_inputs is pending
        unchanged()
        pending[1].additional_properties.clear()
        provider.finalize_failed_run(state)
        history = owner.state.data.conversation_history
        assert len(history) == 2 and history[0] is earlier
        _assert_result_batch(history[1])
        assert binding.pending_inputs == [] and binding.accepted_inputs == {PRIOR}
        assert len(pending) == 2 and all(message.message_id is None for message in pending)
        before = _snapshot(owner.state.to_dict())
        provider.finalize_failed_run(state)
        assert _snapshot(owner.state.to_dict()) == before and binding.append_ordinal == 2
    assert owner.persist_count == 0


async def test_finalizer_filter_failure_keeps_pending_reference_before_append_and_retry_once() -> None:
    owner = _CanonicalStateProvider()
    provider = _ObservedAppend()
    pending = [_result("one")]
    content = pending[0].contents[0]
    bad_id = ["one"]
    cast(Any, content).call_id = bad_id
    assert type(content) is Content and content.type == "function_result" and content.call_id is bad_id
    assert content.to_dict()["call_id"] == ["one"]
    json.dumps(pending[0].to_dict(), allow_nan=False)
    with _bound(owner, CORRELATION) as binding:
        state = await _start_calls(provider, binding)
        earlier = owner.state.data.conversation_history[0]
        binding.pending_inputs = pending
        binding.accepted_inputs.add(PRIOR)
        provider.check_last_append = None
        unchanged = _append_snapshot(binding, state, [], None)
        with pytest.raises(TypeError, match="unhashable type: 'list'"):
            provider.finalize_failed_run(state)
        assert provider.check_last_append is None and binding.pending_inputs is pending
        unchanged()
        content.call_id = "one"
        provider.finalize_failed_run(state)
        history = owner.state.data.conversation_history
        assert len(history) == 2 and history[0] is earlier
        entry = history[1]
        assert type(entry) is DurableAgentStateRequest and len(entry.messages) == 1
        assert entry.messages[0].message_id == f"durable_request_{CORRELATION}_1_0"
        result = entry.to_dict()["messages"][0]["contents"][0]
        assert result["callId"] == "one" and result["result"] == "result:one"
        assert binding.pending_inputs == [] and binding.accepted_inputs == {PRIOR}
        assert len(pending) == 1 and pending[0].message_id is None
        before = _snapshot(owner.state.to_dict())
        provider.finalize_failed_run(state)
        assert _snapshot(owner.state.to_dict()) == before and binding.append_ordinal == 2
    assert owner.persist_count == 0


@pytest.mark.parametrize("error_type", [RuntimeError, asyncio.CancelledError, GeneratorExit])
async def test_finalizer_copy_failure_preserves_exact_exception_and_pending_retry(
    error_type: type[BaseException],
) -> None:
    owner = _CanonicalStateProvider()
    provider = DurableHistoryProvider()
    pending = [_result("one"), _result("two")]
    error = error_type("pending result copy failed")
    probe = _CopyFailure(error)
    pending[1].additional_properties["probe"] = probe
    with _bound(owner, CORRELATION) as binding:
        state = await _start_calls(provider, binding)
        binding.pending_inputs = pending
        unchanged = _append_snapshot(binding, state, [], None)
        with pytest.raises(error_type) as caught:
            provider.finalize_failed_run(state)
        assert caught.value is error and probe.calls == 1
        assert binding.pending_inputs is pending
        unchanged()
        pending[1].additional_properties.clear()
        provider.finalize_failed_run(state)
        _assert_result_batch(owner.state.data.conversation_history[1])
        before = _snapshot(owner.state.to_dict())
        provider.finalize_failed_run(state)
        assert _snapshot(owner.state.to_dict()) == before and binding.append_ordinal == 2
        assert binding.pending_inputs == [] and probe.calls == 1 and owner.persist_count == 0


async def test_finalizer_keeps_existing_unanswered_tool_result_filter() -> None:
    foreign = DurableAgentStateMessage.from_chat_message(
        Message("assistant", [Content("function_call", call_id="foreign", name="lookup")])
    )
    owner = _CanonicalStateProvider([DurableAgentStateResponse("other-correlation", OLD, [foreign])])
    provider = DurableHistoryProvider()
    with _bound(owner, CORRELATION) as binding:
        state = await _start_calls(provider, binding)
        await provider.save_messages("session", [_result("one")], state=state)
        wrong_role = _result("two")
        wrong_role.role = "assistant"
        mixed = _result("two")
        mixed.contents.append(Content.from_text("not only results"))
        unmatched = _result("foreign")
        unmatched.additional_properties["payload"] = {"bad": object()}
        selected = _result("two")
        pending = [wrong_role, mixed, unmatched, _result("one"), selected, _result("two")]
        binding.pending_inputs = pending
        provider.finalize_failed_run(state)
        history = owner.state.data.conversation_history
        assert len(history) == 4 and len(history[-1].messages) == 1
        assert history[-1].to_dict()["messages"][0]["contents"][0]["result"] == "result:two"
        before = _snapshot(owner.state.to_dict())
        provider.finalize_failed_run(state)
        assert _snapshot(owner.state.to_dict()) == before and binding.append_ordinal == 3
        assert binding.pending_inputs == [] and len(pending) == 6 and owner.persist_count == 0


@pytest.mark.parametrize("mode", ["empty", "store-disabled", "service-owned", "no-correlation", "no-matching-call"])
async def test_finalizer_noop_still_consumes_pending_without_validating_ineligible_payload(mode: str) -> None:
    owner = _CanonicalStateProvider()
    provider = DurableHistoryProvider()
    pending = [] if mode == "empty" else [_result("unanswered" if mode == "no-matching-call" else "one")]
    if pending:
        pending[0].additional_properties["payload"] = {"bad": object()}
    with _bound(owner, CORRELATION) as binding:
        state = await _start_calls(provider, binding)
        provider.store_inputs = mode != "store-disabled"
        if mode == "no-correlation":
            binding.correlation_id = None
        binding.pending_inputs = pending
        binding.service_owns_history = mode == "service-owned"
        binding.accepted_inputs.add(PRIOR)
        before = _snapshot(owner.state.to_dict())
        state_unchanged = _reference_check(state)
        provider.finalize_failed_run(state)
        state_unchanged()
        assert binding.pending_inputs == [] and binding.accepted_inputs == {PRIOR}
        assert _snapshot(owner.state.to_dict()) == before and binding.append_ordinal == 1
        assert len(pending) == (0 if mode == "empty" else 1)
    assert owner.persist_count == 0
