# Copyright (c) Microsoft. All rights reserved.

"""Append-local rollback regressions for PR #111 review comment 4073589287."""

import asyncio
from collections import UserList
from copy import deepcopy
from typing import Any, cast

import pytest
from _history_acceptance_test_support import ACCEPTED, PRIOR, _input
from _history_append_test_support import CORRELATION, _append_snapshot, _CopyFailure, _ObservedAppend, _working
from _history_atomicity_test_support import _owner, _snapshot, _summary
from _shared_history_test_support import _bound, _CanonicalStateProvider
from agent_framework import AgentResponse, Content, Message, SessionContext

from agent_framework_durabletask._history_provider import (
    WORKING_BUFFER_KEY,
    DurableHistoryBinding,
    DurableHistoryProvider,
)
from agent_framework_durabletask._response_utils import serialize_input_message
from agent_framework_durabletask._shared_agent_state import (
    DurableAgentStateEntry,
    DurableAgentStateMessage,
    DurableAgentStateRequest,
    DurableAgentStateResponse,
)

INITIAL_ORDINAL = 7
NAME_ERROR = "name is required for function call content"
ERROR_TYPES = [RuntimeError, asyncio.CancelledError, GeneratorExit]
STATE_MODES = ["absent", "loaded", "none"]


def _batch(*, invalid: bool = False) -> list[Message]:
    return [
        Message("assistant", ["valid first message"]),
        Message("assistant", [Content("function_call", call_id="call-2", name=None if invalid else "lookup")]),
    ]


def _identity_result(owner: _CanonicalStateProvider, binding: DurableHistoryBinding) -> tuple[Any, ...]:
    # Compare actual stored payloads and both identity surfaces. Entry timestamps
    # legitimately differ between runs, but fixed correlation makes IDs deterministic.
    entry = owner.state.data.conversation_history[-1]
    return (
        binding.append_ordinal,
        entry.json_type,
        entry.correlation_id,
        tuple((message.message_id, message.public_message_id) for message in entry.messages),
        _snapshot([message.to_dict() for message in entry.messages]),
    )


async def _unfailed(mode: str, identity: str, *, response: bool = False) -> tuple[Any, ...]:
    owner = _owner(identity)
    provider = DurableHistoryProvider()
    with _bound(owner, CORRELATION) as binding:
        binding.append_ordinal = INITIAL_ORDINAL
        state = await _working(provider, mode)
        messages = _batch()
        binding.append_response = AgentResponse(messages=messages) if response else None
        await provider.save_messages("session", messages, state=state)
        kind = "response" if response else "request"
        assert [message.message_id for message in owner.state.data.conversation_history[-1].messages] == [
            f"durable_{kind}_{CORRELATION}_{INITIAL_ORDINAL}_0",
            f"durable_{kind}_{CORRELATION}_{INITIAL_ORDINAL}_1",
        ]
        assert binding.append_ordinal == INITIAL_ORDINAL + 1
        assert owner.persist_count == 0
        return _identity_result(owner, binding)


def test_core_accepts_missing_function_name_but_real_durable_conversion_rejects_it() -> None:
    first, invalid = _batch(invalid=True)
    assert invalid.contents[0].type == "function_call" and invalid.contents[0].name is None
    assert Message.from_dict(invalid.to_dict()).contents[0].name is None
    _snapshot(serialize_input_message(invalid))  # Ordinary JSON-safe Core content, not a poison object.
    assert DurableAgentStateMessage.from_chat_message(deepcopy(first)).text == "valid first message"
    with pytest.raises(ValueError, match=NAME_ERROR):
        DurableAgentStateMessage.from_chat_message(deepcopy(invalid))


@pytest.mark.parametrize("mode", STATE_MODES)
@pytest.mark.parametrize("identity", ["anonymous", "duplicate", "unique"])
async def test_second_conversion_failure_restores_lazy_repairs_state_keys_and_ordinal(
    mode: str,
    identity: str,
) -> None:
    expected = await _unfailed(mode, identity)
    owner = _owner(identity)
    provider = _ObservedAppend()
    messages = _batch(invalid=True)
    with _bound(owner, CORRELATION) as binding:
        binding.append_ordinal = INITIAL_ORDINAL
        state = await _working(provider, mode)
        for _ in range(2):
            with pytest.raises(ValueError, match=NAME_ERROR):
                await provider.save_messages("session", messages, state=state)
            provider.assert_unchanged()
            assert binding.append_response is None
        messages[1].contents[0].name = "lookup"
        await provider.save_messages("session", messages, state=state)
        assert _identity_result(owner, binding) == expected
        assert all(message.message_id is None and not hasattr(message, "_durable_history_id") for message in messages)
    assert owner.persist_count == 0


@pytest.mark.parametrize("mode", STATE_MODES)
@pytest.mark.parametrize("error_type", ERROR_TYPES)
async def test_response_metadata_copy_failure_preserves_state_and_retry_ids(
    mode: str, error_type: type[BaseException]
) -> None:
    expected = await _unfailed(mode, "anonymous", response=True)
    owner = _owner("anonymous")
    provider = _ObservedAppend()
    messages = _batch()
    error = error_type("response metadata copy failed")
    probe = _CopyFailure(error)
    response = AgentResponse(messages=messages, additional_properties={"probe": probe})
    assert response.additional_properties["probe"] is probe and probe.calls == 0
    with _bound(owner, CORRELATION) as binding:
        binding.append_ordinal = INITIAL_ORDINAL
        state = await _working(provider, mode)
        binding.append_response = response
        for attempt in (1, 2):
            with pytest.raises(error_type) as caught:
                await provider.save_messages("session", messages, state=state)
            assert caught.value is error and probe.calls == attempt
            assert binding.append_response is response
            provider.assert_unchanged()
        response.additional_properties.clear()
        await provider.save_messages("session", messages, state=state)
        assert _identity_result(owner, binding) == expected
        assert probe.calls == 2
    assert owner.persist_count == 0


@pytest.mark.parametrize("mode", ["absent", "loaded"])
@pytest.mark.parametrize("error_type", ERROR_TYPES)
async def test_failure_after_real_entry_append_and_final_positions_restores_aliases(
    mode: str, error_type: type[BaseException], monkeypatch: pytest.MonkeyPatch
) -> None:
    expected = await _unfailed(mode, "anonymous")
    owner = _owner("anonymous")
    provider = _ObservedAppend()
    error = error_type("after final append positions")
    reached: list[DurableAgentStateEntry] = []
    with _bound(owner, CORRELATION) as binding:
        binding.append_ordinal = INITIAL_ORDINAL
        state = await _working(provider, mode)
        assert state is not None
        positions = provider._positions

        def fail_final_positions(
            active: DurableHistoryBinding, *, used_ids: set[str] | None = None
        ) -> dict[str, tuple[DurableAgentStateEntry, int]]:
            result = positions(active, used_ids=used_ids)
            entry = owner.state.data.conversation_history[-1]
            if entry.correlation_id == CORRELATION:
                # Explicit late-phase fault injection. Existing opaque projection
                # failures also run during lazy load/flush, so do not prove this boundary.
                assert isinstance(entry, DurableAgentStateRequest)
                assert len(entry.messages) == 2 and active.append_ordinal == INITIAL_ORDINAL + 1
                for stored, working in zip(entry.messages, state[WORKING_BUFFER_KEY][-2:], strict=True):
                    assert result[cast(str, stored.message_id)][0] is entry
                    assert cast(Any, working)._durable_history_id == stored.message_id
                reached.append(entry)
                raise error
            return result

        with monkeypatch.context() as patch:
            patch.setattr(provider, "_positions", fail_final_positions)
            with pytest.raises(error_type) as caught:
                await provider.save_messages("session", _batch(), state=state)
            assert caught.value is error and len(reached) == 1
            provider.assert_unchanged()
        assert all(entry is not reached[0] for entry in owner.state.data.conversation_history)
        await provider.save_messages("session", _batch(), state=state)
        assert _identity_result(owner, binding) == expected
    assert owner.persist_count == 0


@pytest.mark.parametrize("primary", [False, True], ids=["store-only-audit", "primary"])
@pytest.mark.parametrize("failure", ["conversion", *ERROR_TYPES])
async def test_real_after_run_response_failure_preserves_successful_input_save_and_prior_acceptance(
    primary: bool, failure: str | type[BaseException]
) -> None:
    owner = _CanonicalStateProvider()
    provider = _ObservedAppend()
    provider.load_messages = primary
    supplied = _input("B")
    context = SessionContext(session_id="session", input_messages=[supplied])
    messages = _batch(invalid=failure == "conversion")
    error_type = ValueError if isinstance(failure, str) else failure
    error = error_type(NAME_ERROR if failure == "conversion" else "response metadata copy failed")
    probe = None if failure == "conversion" else _CopyFailure(error)
    context._response = AgentResponse(messages=messages, additional_properties={"probe": probe} if probe else {})
    state: dict[str, Any] = {}
    with _bound(owner, CORRELATION) as binding:
        binding.accepted_inputs.add(PRIOR)
        binding.pending_inputs.append(supplied)
        with pytest.raises(error_type) as caught:
            await provider.after_run(agent=None, session=None, context=context, state=state)
        if probe is not None:
            assert caught.value is error and probe.calls == 1
        else:
            assert str(caught.value) == NAME_ERROR
        assert binding.append_response is None
        assert binding.accepted_inputs == {PRIOR}
        assert binding.pending_inputs == [supplied] and binding.pending_inputs[0] is supplied
        assert len(owner.state.data.conversation_history) == 1
        earlier = owner.state.data.conversation_history[0]
        assert isinstance(earlier, DurableAgentStateRequest)
        stored = earlier.messages[0]
        assert stored.text == "B" and stored.message_id == f"durable_request_{CORRELATION}_0_0"
        assert stored.ingestion_occurrence == (ACCEPTED[0] if primary else None)
        if primary:
            assert stored.ingestion_identity == ACCEPTED[1]
        else:
            assert stored.ingestion_identity and stored.ingestion_identity != ACCEPTED[1]
        provider.assert_unchanged()
        assert binding.append_ordinal == 1
    assert owner.persist_count == 0


@pytest.mark.parametrize("primary", [False, True], ids=["store-only-audit", "primary"])
async def test_real_after_run_success_accepts_only_primary_and_keeps_response_metadata(primary: bool) -> None:
    owner = _CanonicalStateProvider()
    provider = _ObservedAppend()
    provider.load_messages = primary
    supplied = _input("B")
    context = SessionContext(session_id="session", input_messages=[supplied])
    supplied_response = AgentResponse(messages=_batch(), additional_properties={"values": [None, False, 0, 0.0]})
    context._response = supplied_response
    before_response = _snapshot(supplied_response.to_dict())
    with _bound(owner, CORRELATION) as binding:
        binding.accepted_inputs.add(PRIOR)
        binding.pending_inputs.append(supplied)
        await provider.after_run(agent=None, session=None, context=context, state={})
        assert binding.accepted_inputs == ({PRIOR, ACCEPTED} if primary else {PRIOR})
        assert binding.pending_inputs == [] and binding.append_response is None
        assert binding.append_ordinal == 2
        request, response = owner.state.data.conversation_history
        assert isinstance(request, DurableAgentStateRequest) and isinstance(response, DurableAgentStateResponse)
        assert request.messages[0].ingestion_occurrence == (ACCEPTED[0] if primary else None)
        assert all(message.ingestion_occurrence is None for message in response.messages)
        assert _snapshot(response.extension_data) == _snapshot({"values": [None, False, 0, 0.0]})
        assert context.response is supplied_response
        assert _snapshot(supplied_response.to_dict()) == before_response
    assert owner.persist_count == 0


@pytest.mark.parametrize("flush_before_save", [False, True], ids=["save-flush", "earlier-flush"])
async def test_failed_append_does_not_roll_back_a_successful_flush(flush_before_save: bool) -> None:
    owner = _owner("unique")
    provider = _ObservedAppend()
    with _bound(owner, CORRELATION) as binding:
        state = await _working(provider, "loaded")
        assert state is not None
        summary = _summary()
        state[WORKING_BUFFER_KEY].insert(1, summary)
        earlier: DurableAgentStateEntry | None = None
        before: str | None = None
        if flush_before_save:
            provider.flush(state)
            earlier = owner.state.data.conversation_history[1]
            before = _snapshot(owner.state.to_dict())
        with pytest.raises(ValueError, match=NAME_ERROR):
            await provider.save_messages("session", _batch(invalid=True), state=state)
        history = owner.state.data.conversation_history
        assert [message.text for entry in history for message in entry.messages] == ["first", "new summary", "second"]
        assert summary.message_id == f"durable_compaction_{CORRELATION}_0_0"
        assert cast(Any, summary)._durable_history_id == summary.message_id
        if earlier is not None:
            assert history[1] is earlier and _snapshot(owner.state.to_dict()) == before
        provider.assert_unchanged()
        assert binding.append_ordinal == 1
    assert owner.persist_count == 0


@pytest.mark.parametrize("mode", STATE_MODES)
async def test_valid_batch_uses_one_ordinal_and_deterministic_internal_ids(mode: str) -> None:
    result = await _unfailed(mode, "anonymous")
    assert result[3] == (
        (f"durable_request_{CORRELATION}_7_0", None),
        (f"durable_request_{CORRELATION}_7_1", None),
    )


async def test_malformed_mutable_non_list_buffer_is_rejected_without_partial_extension() -> None:
    owner = _owner("unique")
    provider = DurableHistoryProvider()
    buffer: UserList[Any] = UserList([None])
    original_items = buffer.data
    state: dict[str, Any] = {WORKING_BUFFER_KEY: buffer, "caller": {"keep": True}}
    before = _snapshot(owner.state.to_dict())
    with _bound(owner, CORRELATION) as binding:
        binding.append_ordinal = INITIAL_ORDINAL
        with pytest.raises(ValueError, match="working buffer must be a list"):
            await provider.save_messages("session", _batch(), state=state)
        assert state[WORKING_BUFFER_KEY] is buffer and buffer.data is original_items
        assert buffer.data == [None]
        assert binding.append_ordinal == INITIAL_ORDINAL
        assert _snapshot(owner.state.to_dict()) == before
        assert set(state) == {WORKING_BUFFER_KEY, "caller"}


@pytest.mark.parametrize("mode", STATE_MODES)
@pytest.mark.parametrize("service_owned", [False, True], ids=["empty-batch", "service-owned"])
async def test_noop_save_does_not_allocate_or_lazily_initialize(mode: str, service_owned: bool) -> None:
    owner = _owner("unique")
    provider = _ObservedAppend()
    with _bound(owner, CORRELATION) as binding:
        binding.append_ordinal = INITIAL_ORDINAL
        state = await _working(provider, mode)
        binding.service_owns_history = service_owned
        messages = _batch(invalid=True) if service_owned else []
        before = _snapshot(owner.state.to_dict())
        if service_owned:
            check = _append_snapshot(binding, state, messages, None)
        await provider.save_messages("session", messages, state=state)
        if service_owned:
            assert provider.check_last_append is None
            check()
        else:
            provider.assert_unchanged()  # The preceding successful flush has its own boundary.
        assert binding.append_ordinal == INITIAL_ORDINAL
        assert _snapshot(owner.state.to_dict()) == before
        if mode == "absent":
            assert state is not None and set(state) == {"caller"}
    assert owner.persist_count == 0
