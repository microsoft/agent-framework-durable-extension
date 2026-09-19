# Copyright (c) Microsoft. All rights reserved.

"""Finite JSON admission before HITL replies consume pending requests or reach handlers."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import date
from typing import Any
from unittest.mock import patch

import pytest
from agent_framework import AgentExecutorRequest, AgentResponse, Content, Message, WorkflowBuilder
from agent_framework._workflows._state import State
from test_workflow_agent_contract_review import _Adapter, _agent, _approval, _pending, _response, _wire
from test_workflow_review_followup import _hitl_input, _host, _HumanGate, _Request

from agent_framework_durabletask._workflows.activity import execute_workflow_activity
from agent_framework_durabletask._workflows.orchestrator import (
    SOURCE_HITL_RESPONSE,
    TaskMetadata,
    TaskType,
    _deserialize_hitl_response,
    _load_agent_hitl_content,
    _prepare_agent_hitl_message,
    _prepare_agent_task,
    _process_agent_response,
    _WorkflowDeliveryLedger,
    execute_hitl_response_handler,
    run_workflow_orchestrator,
)
from agent_framework_durabletask._workflows.runner_context import CapturingRunnerContext
from agent_framework_durabletask._workflows.serialization import (
    deserialize_value,
    serialize_value,
    serialize_workflow_agent_response,
)

_CONTENT_TYPE = f"{Content.__module__}:{Content.__name__}"
_NUMBER_SLOT = "replace-with-an-unquoted-json-number"


def _nested(value: Any) -> dict[str, Any]:
    return {"items": [{"value": value}]}


def _reply(request: Content, kind: str, value: Any) -> Any:
    if kind == "result":
        assert isinstance(request.function_call, Content)
        assert isinstance(request.function_call.call_id, str)
        # The convenience factory formats results as text. Use the actual JSON
        # result field so this exercises a numeric value, not a string containing it.
        return Content(type="function_result", call_id=request.function_call.call_id, result=_nested(value)).to_dict()
    reply = request.to_function_approval_response(True)
    if kind in ("metadata", "runtime"):
        reply.additional_properties = _nested(value)
    elif kind == "approved":
        reply.approved = value
    elif kind == "marker":
        reply.additional_properties = {"blocked": {"__type__": "untrusted:Type", "value": _nested(value)}}
    elif kind != "discarded":
        raise AssertionError(f"Unexpected reply case: {kind}")
    if kind == "runtime":
        return reply
    raw = reply.to_dict()
    if kind == "discarded":
        raw["provider_extension"] = _nested(value)
    return raw


def _hitl_message(request_id: str, reply: Any) -> dict[str, Any]:
    return {"request_id": request_id, "response": reply, "response_type": _CONTENT_TYPE}


def _ledger_snapshot(ledger: _WorkflowDeliveryLedger) -> dict[str, Any]:
    # Compare detached values, not deep-copied Message objects with reference equality.
    return deepcopy({
        **vars(ledger),
        "cached": {
            key: ([message.to_dict() for message in messages], ids) for key, (messages, ids) in ledger.cached.items()
        },
        "pending_agent_requests": {
            key: {request_id: request.to_dict() for request_id, request in requests.items()}
            for key, requests in ledger.pending_agent_requests.items()
        },
        "pending_agent_responses": {
            key: [reply.to_dict() for reply in replies] for key, replies in ledger.pending_agent_responses.items()
        },
    })


def _prime_agent_ledger() -> tuple[Any, Any, _WorkflowDeliveryLedger, list[Content]]:
    host, agent = _host("json-admission-run"), _agent("A")
    ledger = _WorkflowDeliveryLedger(instance_id=host.instance_id)
    metadata = TaskMetadata("A", "question", "start", TaskType.AGENT)
    _prepare_agent_task(host, agent, "A", "question", "admission", ledger, metadata)
    requests = [_approval("first"), _approval("second")]
    result = _process_agent_response(_wire(_pending(requests)), "A", "question", ledger, metadata)
    assert result.output_message is None
    cache_input = AgentExecutorRequest(messages=[Message("user", ["cached before replies"])], should_respond=False)
    assert _prepare_agent_task(host, agent, "A", cache_input, "admission", ledger) is None
    assert list(ledger.pending_agent_requests["A"]) == ["first", "second"]
    assert ledger.pending_agent_responses == {}
    assert ledger.cached["A"][0][0].text == "cached before replies"
    assert host.prepare_agent_task.call_count == 1
    return host, agent, ledger, requests


def _assert_accepted_reply(reply: Content, request: Content, kind: str, value: Any) -> None:
    assert type(reply) is Content
    json.dumps(reply.to_dict(), allow_nan=False)
    if kind == "result":
        assert isinstance(request.function_call, Content)
        assert reply.type == "function_result" and reply.call_id == request.function_call.call_id
        assert reply.result == _nested(value)
    else:
        assert reply.type == "function_approval_response" and reply.id == request.id
        assert reply.approved is (value if kind == "approved" else True)
        if kind in ("metadata", "runtime"):
            assert reply.additional_properties == _nested(value)
        elif kind == "marker":
            assert reply.additional_properties == {"blocked": None}


@pytest.mark.parametrize(
    ("kind", "bad"),
    [
        pytest.param("metadata", float("nan"), id="approval-metadata-nan"),
        pytest.param("metadata", float("inf"), id="approval-metadata-infinity"),
        pytest.param("result", float("-inf"), id="function-result-negative-infinity"),
        pytest.param("approved", float("nan"), id="approval-flag-nan"),
        pytest.param("discarded", float("nan"), id="unknown-constructor-field-nan"),
        pytest.param("marker", float("inf"), id="stripped-marker-infinity"),
    ],
)
def test_first_of_two_replies_rejects_before_pending_replies_or_cache_change(kind: str, bad: float) -> None:
    host, _, ledger, requests = _prime_agent_ledger()
    before = _ledger_snapshot(ledger)
    raw = _reply(requests[0], kind, bad)
    raw_before = json.dumps(raw, sort_keys=True)

    with pytest.raises(ValueError):
        _prepare_agent_hitl_message("A", _hitl_message("first", raw), ledger)

    assert _ledger_snapshot(ledger) == before
    assert json.dumps(raw, sort_keys=True) == raw_before
    assert host.prepare_agent_task.call_count == 1

    finite = False if kind == "approved" else 1.25
    assert _prepare_agent_hitl_message("A", _hitl_message("first", _reply(requests[0], kind, finite)), ledger) is None
    assert list(ledger.pending_agent_requests["A"]) == ["second"]
    accepted = list(ledger.pending_agent_responses["A"])
    assert len(accepted) == 1
    _assert_accepted_reply(accepted[0], requests[0], kind, finite)
    assert _ledger_snapshot(ledger)["cached"] == before["cached"]

    second = requests[1].to_function_approval_response(False)
    combined = _prepare_agent_hitl_message("A", _hitl_message("second", second.to_dict()), ledger)
    assert isinstance(combined, Message)
    assert combined.to_dict() == Message("user", [accepted[0], second]).to_dict()
    assert ledger.pending_agent_requests == ledger.pending_agent_responses == ledger.cached == {}
    assert host.prepare_agent_task.call_count == 1


@pytest.mark.parametrize(
    ("kind", "bad"),
    [
        pytest.param("metadata", float("nan"), id="raw-approval"),
        pytest.param("result", float("inf"), id="raw-function-result"),
        pytest.param("runtime", float("nan"), id="runtime-approved-true-with-nan"),
    ],
)
def test_partial_task_preparation_cannot_commit_nonfinite_reply_before_message_identity(kind: str, bad: float) -> None:
    host, agent, ledger, requests = _prime_agent_ledger()
    before = _ledger_snapshot(ledger)

    def prepare(request_id: str, reply: Any) -> tuple[Any, TaskMetadata]:
        message = _hitl_message(request_id, reply)
        metadata = TaskMetadata("A", message, f"{SOURCE_HITL_RESPONSE}_{request_id}", TaskType.AGENT)
        task = _prepare_agent_task(host, agent, "A", message, "admission", ledger, metadata)
        return task, metadata

    # The first reply must fail at admission, not wait for a combined Message fingerprint.
    with (
        patch(
            "agent_framework_durabletask._workflows.orchestrator.message_identity",
            side_effect=AssertionError("Partial approval admission must precede message identity"),
        ),
        pytest.raises(ValueError),
    ):
        prepare("first", _reply(requests[0], kind, bad))

    assert _ledger_snapshot(ledger) == before
    assert host.prepare_agent_task.call_count == 1
    task, metadata = prepare("first", _reply(requests[0], kind, 1.25))
    assert task is None and metadata.skip_dispatch
    assert list(ledger.pending_agent_requests["A"]) == ["second"]
    accepted = list(ledger.pending_agent_responses["A"])
    assert len(accepted) == 1
    _assert_accepted_reply(accepted[0], requests[0], kind, 1.25)
    assert _ledger_snapshot(ledger)["cached"] == before["cached"]
    assert host.prepare_agent_task.call_count == 1

    second = requests[1].to_function_approval_response(False)
    task, metadata = prepare("second", second.to_dict())
    assert task is not None and not metadata.skip_dispatch
    assert host.prepare_agent_task.call_count == 2
    call = host.prepare_agent_task.call_args
    assert call.args[3] == [Message("user", [accepted[0], second]).to_dict()]
    assert len(call.kwargs["context_message_ids"]) == 1
    assert ledger.pending_agent_requests == ledger.pending_agent_responses == ledger.cached == {}


def _start_agent_wait(adapter: str) -> tuple[Any, Any, Any, _WorkflowDeliveryLedger, list[Content]]:
    requests = [_approval("first"), _approval("second")]
    workflow = WorkflowBuilder(name="admission", start_executor=_agent("A")).build()
    host = _Adapter(adapter)
    generator: Any = run_workflow_orchestrator(host.context, workflow, "question")
    yielded = generator.send(host.complete(next(generator), _wire(_pending(requests))))
    assert generator.gi_frame is not None
    ledger = generator.gi_frame.f_locals["delivery_ledger"]
    assert isinstance(ledger, _WorkflowDeliveryLedger)
    assert list(ledger.pending_agent_requests["A"]) == ["first", "second"]
    assert ledger.pending_agent_responses == {}
    assert host.pending[0][2] == ("first",)
    return host, generator, yielded, ledger, requests


def _finish_agent_wait(host: Any, generator: Any, yielded: Any, first: Any, expected: Content, second: Content) -> None:
    yielded = generator.send(host.complete(yielded, first))
    assert host.pending[0][0] == "event" and host.pending[0][2] == ("second",)
    assert list(host.statuses[-1]["pending_requests"]) == ["second"]
    assert host.native.call_entity.call_count == 1
    yielded = generator.send(host.complete(yielded, json.dumps(second.to_dict(), allow_nan=False)))
    assert host.native.call_entity.call_count == 2
    assert host.native.call_activity.call_count == 0
    assert host.payload()["contextMessages"] == [Message("user", [expected, second]).to_dict()]
    assert len(host.payload()["contextMessageIds"]) == 1
    assert generator.gi_frame is not None
    ledger = generator.gi_frame.f_locals["delivery_ledger"]
    assert ledger.pending_agent_requests == ledger.pending_agent_responses == ledger.cached == {}
    output = host.finish(generator, yielded, _wire(_response("done")))
    assert len(output) == 1 and output[0].text == "done"


@pytest.mark.parametrize(
    ("adapter", "kind", "token"),
    [
        pytest.param("dt", "metadata", "NaN", id="approval-json-nan"),
        pytest.param("dt", "result", "Infinity", id="function-result-json-infinity"),
        pytest.param("af", "metadata", "-Infinity", id="af-approval-json-negative-infinity"),
        pytest.param("dt", "metadata", "1e309", id="approval-json-overflow"),
        pytest.param("dt", "scalar", "NaN", id="unquoted-json-nan-is-not-text"),
        pytest.param("af", "scalar", "1e309", id="af-unquoted-json-overflow-is-not-text"),
        pytest.param("dt", "marker", "NaN", id="validate-json-before-marker-stripping"),
    ],
)
def test_generator_rewaits_on_bad_json_without_consuming_either_pending_request(
    adapter: str, kind: str, token: str
) -> None:
    host, generator, yielded, ledger, requests = _start_agent_wait(adapter)
    try:
        before = _ledger_snapshot(ledger)
        assert generator.gi_frame is not None
        pending_before = deepcopy(generator.gi_frame.f_locals["pending_hitl_requests"])
        status_before = deepcopy(host.statuses[-1])
        raw = token
        if kind != "scalar":
            raw = json.dumps(_reply(requests[0], kind, _NUMBER_SLOT), allow_nan=False)
            quoted_slot = json.dumps(_NUMBER_SLOT)
            assert raw.count(quoted_slot) == 1
            raw = raw.replace(quoted_slot, token)

        # The adapter serializes this outer string strictly. Only the real generator
        # parses the deliberately non-finite JSON contained inside the event string.
        yielded = generator.send(host.complete(yielded, raw))

        assert host.pending[0][0] == "event" and host.pending[0][2] == ("first",)
        assert host.native.call_entity.call_count == 1
        assert host.native.call_activity.call_count == 0
        assert _ledger_snapshot(ledger) == before
        assert generator.gi_frame is not None
        assert generator.gi_frame.f_locals["pending_hitl_requests"] == pending_before
        assert generator.gi_frame.f_locals["pending_messages"] == {}
        assert host.statuses[-1] == status_before

        corrected = requests[0].to_function_approval_response(True)
        corrected.additional_properties = _nested(1.25)
        _finish_agent_wait(
            host,
            generator,
            yielded,
            json.dumps(corrected.to_dict(), allow_nan=False),
            corrected,
            requests[1].to_function_approval_response(False),
        )
    finally:
        generator.close()


@pytest.mark.parametrize("kind", ["approval-json", "quoted-nan", "ordinary-text"])
def test_generator_accepts_finite_json_and_distinguishes_string_content_from_numeric_tokens(kind: str) -> None:
    host, generator, yielded, _, requests = _start_agent_wait("dt")
    try:
        if kind == "approval-json":
            expected = requests[0].to_function_approval_response(True)
            expected.additional_properties = {"values": [False, 0, None, "NaN", "Infinity", "1e309", 1.25]}
            raw = json.dumps(expected.to_dict(), allow_nan=False)
        elif kind == "quoted-nan":
            expected = Content.from_text("NaN")
            raw = json.dumps("NaN", allow_nan=False)
        else:
            expected = Content.from_text("please continue")
            raw = "please continue"
        _finish_agent_wait(host, generator, yielded, raw, expected, requests[1].to_function_approval_response(False))
    finally:
        generator.close()


def test_nonagent_generator_rewaits_on_bad_json_with_no_agent_ledger_or_shared_state() -> None:
    executor = _HumanGate()
    workflow = WorkflowBuilder(name="admission", start_executor=executor).build()
    host = _Adapter("dt")
    generator: Any = run_workflow_orchestrator(host.context, workflow, "Review", None)
    try:
        yielded = next(generator)
        initial_result = execute_workflow_activity(executor, host.activity_input(), workflow)
        yielded = generator.send(host.complete(yielded, initial_result))
        assert host.pending[0][0] == "event" and host.pending[0][2] == ("request-1",)
        assert generator.gi_frame is not None
        ledger = generator.gi_frame.f_locals["delivery_ledger"]
        ledger_before = _ledger_snapshot(ledger)
        pending_before = deepcopy(generator.gi_frame.f_locals["pending_hitl_requests"])
        assert ledger.pending_agent_requests == ledger.pending_agent_responses == {}
        request = _approval("request-1")
        raw = json.dumps(_reply(request, "metadata", float("nan")))

        yielded = generator.send(host.complete(yielded, raw))

        assert host.pending[0][0] == "event" and host.pending[0][2] == ("request-1",)
        assert host.native.call_activity.call_count == 1
        assert host.native.call_entity.call_count == 0
        assert executor.seen == [] and _ledger_snapshot(ledger) == ledger_before
        assert generator.gi_frame is not None
        assert generator.gi_frame.f_locals["pending_hitl_requests"] == pending_before
        assert generator.gi_frame.f_locals["pending_messages"] == {}

        corrected = _reply(request, "metadata", 1.25)
        yielded = generator.send(host.complete(yielded, json.dumps(corrected, allow_nan=False)))
        assert host.native.call_activity.call_count == 2
        result = execute_workflow_activity(executor, host.activity_input(), workflow)
        assert host.finish(generator, yielded, result) == []
        assert len(executor.seen) == 1 and executor.seen[0][0] == "request-1"
        _assert_accepted_reply(executor.seen[0][1], request, "metadata", 1.25)
    finally:
        generator.close()


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(_nested(float("nan")), id="untyped-mapping"),
        pytest.param([_nested(float("inf"))], id="untyped-list"),
        pytest.param(float("-inf"), id="untyped-number"),
    ],
)
def test_nonagent_deserializer_checks_parsed_json_without_a_declared_type(raw: Any) -> None:
    with pytest.raises(ValueError):
        _deserialize_hitl_response(raw, None)
    finite = {"values": [False, 0, None, "NaN", 1.25]}
    assert _deserialize_hitl_response(finite, None) == finite


@pytest.mark.parametrize("kind", ["metadata", "discarded", "marker"])
async def test_nonagent_raw_reply_rejects_before_handler_lookup_or_state_changes(kind: str) -> None:
    executor = _HumanGate()
    state = State()
    state.import_state({"keep": False})
    runner = CapturingRunnerContext()
    request = _approval("request-1")
    message = {
        **_hitl_message("request-1", _reply(request, kind, float("nan"))),
        "original_request": serialize_value(_Request("Review")),
    }

    with (
        patch.object(executor, "_find_response_handler", wraps=executor._find_response_handler) as lookup,
        pytest.raises(ValueError),
    ):
        await execute_hitl_response_handler(executor, message, state, runner)

    lookup.assert_not_called()
    assert executor.seen == []
    assert state.export_state() == {"keep": False}
    assert await runner.drain_events() == []
    assert await runner.drain_messages() == {}
    corrected = {**message, "response": _reply(request, kind, 1.25)}
    await execute_hitl_response_handler(executor, corrected, state, runner)
    assert len(executor.seen) == 1
    assert executor.seen[0][0] == "request-1"
    _assert_accepted_reply(executor.seen[0][1], request, kind, 1.25)


@pytest.mark.parametrize("bad", [pytest.param(float("nan"), id="nan"), pytest.param(float("inf"), id="infinity")])
def test_nonagent_activity_checks_runtime_content_after_checkpoint_decode_before_handler(bad: float) -> None:
    executor = _HumanGate()
    request = _approval("request-1")
    reply = _reply(request, "runtime", bad)
    assert isinstance(reply, Content) and reply.approved is True
    encoded_input = _hitl_input(reply, Content)
    # Checkpoint encoding can hide this Content's non-finite public metadata.
    json.dumps(json.loads(encoded_input), allow_nan=False)

    with (
        patch.object(executor, "_find_response_handler", wraps=executor._find_response_handler) as lookup,
        pytest.raises(ValueError),
    ):
        execute_workflow_activity(executor, encoded_input)

    lookup.assert_not_called()
    assert executor.seen == []
    corrected = _reply(request, "runtime", 1.25)
    execute_workflow_activity(executor, _hitl_input(corrected, Content))
    assert len(executor.seen) == 1 and executor.seen[0][0] == "request-1"
    _assert_accepted_reply(executor.seen[0][1], request, "runtime", 1.25)


def test_direct_helpers_preserve_finite_runtime_content_and_do_not_parse_ordinary_nan_text() -> None:
    request = _approval("first")
    runtime = _reply(request, "runtime", [False, 0, None, "NaN", 1.25])
    for restored in (
        _load_agent_hitl_content("first", request, runtime),
        _deserialize_hitl_response(runtime, _CONTENT_TYPE),
    ):
        assert type(restored) is Content
        assert restored.to_dict() == runtime.to_dict()
        json.dumps(restored.to_dict(), allow_nan=False)
    assert _load_agent_hitl_content("first", request, "NaN").to_dict() == Content.from_text("NaN").to_dict()
    assert _deserialize_hitl_response("NaN", _CONTENT_TYPE) == "NaN"


def test_decoded_checkpoint_reply_keeps_typed_descendants() -> None:
    response = {"day": date(2026, 9, 17), "nested": [False, None]}
    assert _deserialize_hitl_response(response, "builtins:dict") == response


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(float("nan"), id="nan"),
        pytest.param(_nested(float("inf")), id="nested-infinity"),
    ],
)
def test_generated_direct_agent_response_rejects_nonfinite_value(value: Any) -> None:
    response = AgentResponse(messages=[Message("assistant", ["done"])], value=value)
    before = json.dumps(response.value, sort_keys=True)
    with pytest.raises(ValueError):
        serialize_workflow_agent_response(response)
    assert json.dumps(response.value, sort_keys=True) == before
    assert response.text == "done"


def test_generated_direct_agent_response_preserves_finite_value_and_literal_numeric_strings() -> None:
    value = {"values": [False, 0, None, "NaN", "Infinity", "-Infinity", "1e309", 1.25]}
    response = AgentResponse[Any](messages=[Message("assistant", ["done"])], value=value)
    encoded = serialize_workflow_agent_response(response)
    restored = deserialize_value(json.loads(json.dumps(encoded, allow_nan=False)))
    assert type(restored) is AgentResponse
    assert restored.value == value and restored.text == "done"
