# Copyright (c) Microsoft. All rights reserved.

"""Functions storage retries and response delivery through HTTP handlers."""

import json
from copy import deepcopy
from typing import Any
from unittest.mock import AsyncMock, Mock, call

import pytest
from _reader_test_support import (
    CORRELATION_ID,
    ERROR_MESSAGE,
    SESSION_ID,
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


@pytest.mark.parametrize("encoded", [False, True], ids=["object-state", "json-state"])
async def test_first_transient_storage_error_retries_then_returns_legacy_http_200(
    encoded: bool, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    raw = {"schemaVersion": "1.1.0", "data": {"conversationHistory": [_history_entry()]}}
    stored = json.dumps(raw) if encoded else raw
    before = deepcopy(stored)
    client = _client(stored)
    recovered = client.get_entity.return_value
    client.get_entity.side_effect = [OSError("Transient storage read failure"), recovered]
    events = Mock()
    events.attach_mock(client.signal_entity, "signal")
    events.attach_mock(sleep, "sleep")
    events.attach_mock(client.get_entity, "read")

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
    assert client.get_entity.await_args_list == [call(entity_id), call(entity_id)]
    assert sleep.await_args_list == [call(0.01), call(0.01)]
    assert [event[0] for event in events.mock_calls] == ["signal", "sleep", "read", "sleep", "read"]
    assert recovered.get_state() == before
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
    good_state = _client(_shared_state()).get_entity.return_value
    client.get_entity.side_effect = [client.get_entity.return_value, good_state]

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
    assert client.get_entity.return_value.get_state() == before
