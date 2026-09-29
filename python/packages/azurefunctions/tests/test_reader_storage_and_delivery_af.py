# Copyright (c) Microsoft. All rights reserved.

"""Functions storage retries and response delivery through SDK tasks and HTTP handlers."""

import json
from copy import deepcopy
from typing import Any
from unittest.mock import AsyncMock, Mock, call

import pytest
from _reader_test_support import (
    CORRELATION_ID,
    ERROR_MESSAGE,
    SESSION_ID,
    Answer,
    HttpHandler,
    McpHandler,
    _assert_one_delivery,
    _client,
    _history_entry,
    _request,
    _shared_state,
    _wire_response,
)
from _reader_test_support import (
    app as app,
)
from _reader_test_support import (
    handlers as handlers,
)
from _reader_test_support import (
    sleep as sleep,
)
from agent_framework import AgentResponse, Content
from azure.durable_functions.models.actions.NoOpAction import NoOpAction
from azure.durable_functions.models.Task import AtomicTask, TaskState
from pydantic import ValidationError

from agent_framework_azurefunctions._orchestration import AgentTask

SPARSE_ERRORS = [
    pytest.param({}, id="missing-both"),
    pytest.param({"errorCode": ""}, id="empty-code-only"),
    pytest.param({"message": ""}, id="empty-message-only"),
    pytest.param({"errorCode": "", "message": ""}, id="empty-both"),
    pytest.param({"errorCode": "ProviderSpecific"}, id="preserve-code-fill-missing-message"),
    pytest.param({"message": "Provider diagnostic"}, id="preserve-message-fill-missing-code"),
    pytest.param({"errorCode": "ProviderSpecific", "message": ""}, id="preserve-code-fill-empty-message"),
    pytest.param({"errorCode": "", "message": "Provider diagnostic"}, id="preserve-message-fill-empty-code"),
    pytest.param({"errorCode": "  ", "message": "  "}, id="blank-both"),
    pytest.param(
        {"errorCode": "  ProviderSpecific  ", "message": "  Provider diagnostic  "},
        id="nonblank-fields-not-normalized",
    ),
]


def _legacy_inline(kind: str) -> dict[str, Any]:
    if kind == "approval-no-result":
        approval = Content.from_function_approval_request("approval-01", Content.from_function_call("call-01", "tool"))
        contents = [approval.to_dict()]
    else:
        contents = [{"type": "error", "error_code": "LegacyFailure", "message": "Legacy diagnostic"}]
        if kind != "bare-error":
            text = '{"answer":42}' if kind == "error-valid-json" else "not JSON"
            contents.append({"type": "text", "text": text})
    return {
        "type": "agent_response",
        "messages": [{"role": "assistant", "contents": contents}],
        "additional_properties": {"provider": {"keep": [False, 0]}},
    }


def _complete_inline(raw: dict[str, Any], precompleted: bool) -> AgentTask:
    child = AtomicTask(7, NoOpAction())
    if precompleted:
        child.set_value(is_error=False, value=raw)
    task = AgentTask(child, Answer, CORRELATION_ID)
    assert task.children == [child]
    if not precompleted:
        assert task.state is TaskState.RUNNING
        child.set_value(is_error=False, value=raw)
    assert child.state is TaskState.SUCCEEDED and child.result is raw
    assert task.is_completed
    return task


@pytest.mark.parametrize("precompleted", [False, True], ids=["delayed", "precompleted"])
@pytest.mark.parametrize("kind", ["bare-error", "error-invalid-json", "error-valid-json", "approval-no-result"])
def test_real_af_task_retains_legacy_typed_success_and_failure(kind: str, precompleted: bool) -> None:
    raw = _legacy_inline(kind)
    before = deepcopy(raw)

    task = _complete_inline(raw, precompleted)

    if kind == "error-valid-json":
        assert task.state is TaskState.SUCCEEDED
        assert isinstance(task.result, AgentResponse)
        assert task.result.value == Answer(answer=42)
        assert task.result.messages[0].contents[0].error_code == "LegacyFailure"
        assert "durable_status" not in task.result.additional_properties
    else:
        assert task.state is TaskState.FAILED
        if kind == "error-invalid-json":
            assert isinstance(task.result, ValidationError)
            assert task.result.errors()[0]["type"] == "json_invalid"
        else:
            assert type(task.result) is ValueError
            assert "could not be parsed into required format Answer" in str(task.result)
    assert raw == before


@pytest.mark.parametrize("precompleted", [False, True], ids=["delayed", "precompleted"])
@pytest.mark.parametrize("status", ["error", "already_completed", "accepted"])
def test_explicit_durable_status_still_skips_af_task_typed_validation(status: str, precompleted: bool) -> None:
    raw = {
        "type": "agent_response",
        "messages": [{"role": "assistant", "contents": [{"type": "text", "text": "not JSON"}]}],
        "value": {"answer": "not-an-integer"},
        "additional_properties": {"durable_status": status, "correlation_id": CORRELATION_ID},
    }
    before = deepcopy(raw)

    task = _complete_inline(raw, precompleted)

    assert task.state is TaskState.SUCCEEDED and isinstance(task.result, AgentResponse)
    assert type(task.result.value) is dict and task.result.value == raw["value"]
    assert task.result.additional_properties == raw["additional_properties"]
    assert raw == before


@pytest.mark.parametrize("encoded", [False, True], ids=["object-state", "json-state"])
async def test_first_transient_storage_error_retries_then_returns_legacy_http_200(
    encoded: bool, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    raw = {"schemaVersion": "1.1.0", "data": {"conversationHistory": [_history_entry()]}}
    stored = json.dumps(raw) if encoded else raw
    before = deepcopy(stored)
    client = _client(stored)
    recovered = client.read_entity_state.return_value
    client.read_entity_state.side_effect = [OSError("Transient storage read failure"), recovered]
    events = Mock()
    events.attach_mock(client.signal_entity, "signal")
    events.attach_mock(sleep, "sleep")
    events.attach_mock(client.read_entity_state, "read")

    response = await handlers[0](_request(), client)

    assert response.status_code == 200
    assert json.loads(response.get_body()) == {
        "response": "Legacy transcript answer",
        "message": "question",
        "session_id": SESSION_ID,
        "status": "success",
        "correlation_id": CORRELATION_ID,
        "message_count": 1,
    }
    client.signal_entity.assert_awaited_once()
    entity_id = client.signal_entity.call_args.args[0]
    assert client.read_entity_state.await_args_list == [call(entity_id), call(entity_id)]
    assert sleep.await_args_list == [call(0.01), call(0.01)]
    assert [event[0] for event in events.mock_calls] == ["signal", "sleep", "read", "sleep", "read"]
    assert recovered.entity_state == before
    assert stored == before


@pytest.mark.parametrize("kind", ["missing-collections", "invalid-json", "conflicting-outcome"])
async def test_deterministic_shared_read_error_is_terminal_without_retrying_a_later_good_state(
    kind: str, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    raw: Any = _shared_state()
    if kind == "missing-collections":
        del raw["data"]["terminalResults"]
    elif kind == "invalid-json":
        raw = "{not JSON"
    else:
        raw["data"]["terminalResults"][CORRELATION_ID]["response"]["extensionData"]["durable_status"] = "error"
    before = deepcopy(raw)
    client = _client(raw)
    good_state = _client(_shared_state()).read_entity_state.return_value
    client.read_entity_state.side_effect = [client.read_entity_state.return_value, good_state]

    response = await handlers[0](_request(), client)

    assert response.status_code == 500
    payload = json.loads(response.get_body())
    assert payload["status"] == "error" and payload["error_code"] == "state_read_error"
    assert payload["response"] is None and payload["error"]
    _assert_one_delivery(client, sleep, before)
    assert raw == before


@pytest.mark.parametrize("fields", SPARSE_ERRORS)
async def test_http_500_fills_only_missing_or_blank_failed_content_diagnostics(
    fields: dict[str, str], handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    stored = _wire_response("Partial answer, not the failure diagnostic")
    stored["messages"][0]["contents"].append({"$type": "error", **deepcopy(fields)})
    raw = _shared_state(outcome="failed", response=stored)
    before = deepcopy(raw)
    client = _client(raw)

    response = await handlers[0](_request(), client)

    assert response.status_code == 500
    payload = json.loads(response.get_body())
    expected_code = fields["errorCode"] if fields.get("errorCode", "").strip() else "schema_error"
    expected_message = fields["message"] if fields.get("message", "").strip() else ERROR_MESSAGE
    assert payload["status"] == "error" and payload["response"] is None
    assert payload["error_code"] == expected_code and payload["error"] == expected_message
    errors = [
        item
        for message in payload["agent_response"]["messages"]
        for item in message["contents"]
        if item["type"] == "error"
    ]
    assert len(errors) == 1
    assert errors[0]["error_code"] == expected_code and errors[0]["message"] == expected_message
    _assert_one_delivery(client, sleep, before)
    assert raw == before
    errors[0]["message"] = "Consumer edit"
    assert client.read_entity_state.return_value.entity_state == before
