# Copyright (c) Microsoft. All rights reserved.

"""Read-only shared snapshots through registered Functions HTTP, MCP and proxy paths."""

import json
from copy import deepcopy
from typing import Any
from unittest.mock import AsyncMock, Mock

import azure.durable_functions as df
import pytest
from _reader_test_support import (
    AGENT_NAME,
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
from agent_framework import AgentResponse
from agent_framework_durabletask import (
    MIMETYPE_APPLICATION_JSON,
    SESSION_ID_HEADER,
    DurableAgentState,
    LegacyDurableAgentState,
    SharedAgentStateReader,
    load_agent_response,
)
from azure.durable_functions.models.actions.NoOpAction import NoOpAction
from azure.durable_functions.models.Task import AtomicTask, TaskState
from pydantic import BaseModel, RootModel

from agent_framework_azurefunctions import AgentFunctionApp

EXPIRED_MESSAGE = "This request completed, but its response delivery window has expired."


class NullAnswer(RootModel[None]):
    pass


@pytest.mark.parametrize("value", [None, 0, False, "", [], {}, {"answer": 42}])
async def test_http_shared_success_delivers_snapshot_and_falsey_values(
    value: Any, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    stored = _wire_response()
    stored["value"] = deepcopy(value)
    stored["futureResponse"] = {"opaque": [0, False, None]}
    raw = _shared_state(response=stored)
    client = _client(raw)

    response = await handlers[0](_request(), client)

    assert response.status_code == 200 and response.mimetype == MIMETYPE_APPLICATION_JSON
    payload = json.loads(response.get_body())
    assert set(payload) == {
        "response",
        "message",
        "session_id",
        "status",
        "correlation_id",
        "message_count",
        "agent_response",
    }
    assert payload["response"] == "Readable answer" and payload["status"] == "success"
    assert payload["message"] == "question" and payload["session_id"] == SESSION_ID
    assert payload["correlation_id"] == CORRELATION_ID and payload["message_count"] == 0
    snapshot = payload["agent_response"]
    assert "value" in snapshot and snapshot["value"] == value
    assert type(snapshot["value"]) is type(value)
    assert "futureResponse" not in snapshot
    delivered = load_agent_response(snapshot)
    assert delivered.response_id == "response-1" and delivered.agent_id == "agent-1"
    assert delivered.finish_reason == "stop"
    assert delivered.usage_details == {"input_token_count": 3, "output_token_count": 2, "total_token_count": 5}
    assert delivered.messages[0].author_name == "writer" and delivered.messages[0].message_id == "answer-message"
    assert delivered.additional_properties == {"provider": {"labels": ["response"]}}
    _assert_one_delivery(client, sleep, raw)
    snapshot["additional_properties"]["provider"]["labels"].append("consumer edit")
    assert client.read_entity_state.return_value.entity_state == raw


@pytest.mark.parametrize("value", [None, False, 0, "", [], {}])
async def test_plain_text_shared_empty_success_never_echoes_input(
    value: Any, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    stored = _wire_response("")
    stored["value"] = value
    raw = _shared_state(response=stored)
    client = _client(raw)

    response = await handlers[0](_request(plain_text=True), client)

    assert response.status_code == 200 and response.get_body() == b""
    assert response.headers[SESSION_ID_HEADER] == SESSION_ID
    assert "x-ms-durable-outcome" not in response.headers
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
@pytest.mark.parametrize("availability", ["expired", "unavailable"])
@pytest.mark.parametrize("plain_text", [False, True])
async def test_http_unavailable_preserves_receipt_outcome_and_stops_polling(
    outcome: str,
    availability: str,
    plain_text: bool,
    handlers: tuple[HttpHandler, McpHandler],
    sleep: AsyncMock,
) -> None:
    raw = _shared_state(outcome=outcome, availability=availability)
    raw["data"]["conversationHistory"] = [_history_entry()]
    client = _client(raw)

    response = await handlers[0](_request(plain_text=plain_text), client)

    assert response.status_code == 410
    if plain_text:
        assert response.get_body().decode() == EXPIRED_MESSAGE
        assert response.headers[SESSION_ID_HEADER] == SESSION_ID
        assert response.headers["x-ms-durable-outcome"] == outcome
    else:
        payload = json.loads(response.get_body())
        assert payload["status"] == "completed_unavailable" and payload["response"] is None
        assert payload["error_code"] == "response_expired" and payload["error"] == EXPIRED_MESSAGE
        assert payload["outcome"] == outcome and payload["message_count"] == 1
        assert payload["agent_response"]["additional_properties"] == {
            "durable_status": "already_completed",
            "durable_outcome": outcome,
            "correlation_id": CORRELATION_ID,
        }
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize("hint", [None, "accepted", "already_completed"])
@pytest.mark.parametrize("plain_text", [False, True])
async def test_live_failed_receipt_overrides_provider_delivery_hints(
    hint: str | None, plain_text: bool, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    stored = _wire_response('{"answer":"invalid"}')
    stored["value"] = {"answer": "invalid"}
    if hint is not None:
        stored["extensionData"]["durable_status"] = hint
    stored["messages"].insert(0, {"role": "tool", "contents": [{"$type": "error", "message": "Tool-only error"}]})
    stored["messages"].append({
        "role": "system",
        "contents": [{"$type": "error", "errorCode": "response_expired", "message": ERROR_MESSAGE}],
    })
    raw = _shared_state(outcome="failed", response=stored)
    client = _client(raw)

    response = await handlers[0](_request(plain_text=plain_text), client)

    assert response.status_code == 500
    assert "x-ms-durable-outcome" not in response.headers
    if plain_text:
        assert response.get_body().decode() == ERROR_MESSAGE
    else:
        payload = json.loads(response.get_body())
        assert payload["status"] == "error" and payload["response"] is None
        assert payload["error"] == ERROR_MESSAGE and payload["error_code"] == "schema_error"
        assert "outcome" not in payload
        assert payload["agent_response"]["value"] == {"answer": "invalid"}
        assert payload["agent_response"]["additional_properties"]["durable_status"] == "error"
    _assert_one_delivery(client, sleep, raw)


async def test_failed_partial_without_error_content_delivers_record_diagnostic(
    handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    raw = _shared_state(outcome="failed", response=_wire_response("Incomplete answer"))
    client = _client(raw)

    response = await handlers[0](_request(), client)

    assert response.status_code == 500
    payload = json.loads(response.get_body())
    assert payload["error"] == ERROR_MESSAGE and payload["error_code"] == "schema_error"
    assert payload["agent_response"]["messages"][-1]["contents"][0]["error_details"] == {"retryable": False}
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize("hint", ["accepted", "already_completed"])
async def test_success_receipt_ignores_provider_delivery_hints_and_tool_errors(
    hint: str, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    stored = _wire_response()
    stored["extensionData"]["durable_status"] = hint
    stored["messages"].append({"role": "tool", "contents": [{"$type": "error", "errorCode": "response_expired"}]})
    raw = _shared_state(response=stored)
    client = _client(raw)

    response = await handlers[0](_request(), client)

    assert response.status_code == 200
    payload = json.loads(response.get_body())
    assert payload["status"] == "success" and payload["response"] == "Readable answer"
    assert "durable_status" not in payload["agent_response"]["additional_properties"]
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize("conflict", ["status", "content"])
async def test_conflicting_success_evidence_is_an_explicit_read_error(
    conflict: str, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    stored = _wire_response()
    if conflict == "status":
        stored["extensionData"]["durable_status"] = "error"
    else:
        stored["messages"].append({"role": "system", "contents": [{"$type": "error", "errorCode": "response_expired"}]})
    raw = _shared_state(response=stored)
    client = _client(raw)

    response = await handlers[0](_request(), client)

    assert response.status_code == 500
    payload = json.loads(response.get_body())
    assert payload["status"] == "error" and payload["error_code"] == "state_read_error"
    assert payload["error"] == "Failed to read the stored agent response."
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize(
    "raw",
    [
        None,
        [],
        False,
        0,
        "",
        "not JSON",
        {"data": {}},
        {"schemaVersion": "2.0.1", "data": {}},
        {"schemaVersion": "1.1.0", "data": None},
        {"schemaVersion": "2.0.0", "data": {"conversationHistory": []}},
    ],
)
async def test_existing_malformed_state_is_not_normalized_or_polled_until_timeout(
    raw: Any, app: AgentFunctionApp, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    direct_client = _client(raw)
    with pytest.raises(ValueError):
        await app._read_cached_state(direct_client, df.EntityId(f"dafx-{AGENT_NAME}", SESSION_ID))
    client = _client(raw)

    response = await handlers[0](_request(), client)

    assert response.status_code == 500
    payload = json.loads(response.get_body())
    assert payload["status"] == "error" and payload["error_code"] == "state_read_error"
    assert payload["response"] is None
    _assert_one_delivery(client, sleep, raw)


async def test_transport_read_failure_retains_bounded_retries(
    handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    client = _client({})
    client.read_entity_state.side_effect = OSError("Storage read failed")

    response = await handlers[0](_request(), client)

    assert response.status_code == 500
    payload = json.loads(response.get_body())
    assert payload["status"] == "timeout"
    assert client.read_entity_state.await_count == 3
    assert sleep.await_count == 3


@pytest.mark.parametrize("kind", ["missing", "empty-legacy", "shared-transcript-only"])
async def test_only_absent_completion_keeps_polling(
    kind: str, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    raw = _shared_state() if kind == "shared-transcript-only" else {}
    if kind == "shared-transcript-only":
        raw["data"]["completionReceipts"] = {}
        raw["data"]["terminalResults"] = {}
        raw["data"]["conversationHistory"] = [_history_entry()]
    client = _client(raw, exists=kind != "missing")

    response = await handlers[0](_request(), client)

    assert response.status_code == 500 and json.loads(response.get_body())["status"] == "timeout"
    assert client.read_entity_state.await_count == 3 and sleep.await_count == 3
    client.signal_entity.assert_awaited_once()
    assert client.read_entity_state.return_value.entity_state == raw


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize("error", [False, True])
async def test_legacy_delivery_keeps_exact_base_builder_shape(
    version: str, error: bool, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    raw = {"schemaVersion": version, "data": {"conversationHistory": [_history_entry(error=error)]}}
    original = LegacyDurableAgentState.from_dict(raw).try_get_agent_response(CORRELATION_ID)
    assert original is not None
    client = _client(raw)

    response = await handlers[0](_request(), client)

    assert response.status_code == 200
    assert json.loads(response.get_body()) == {
        "response": original.text,
        "message": "question",
        "session_id": SESSION_ID,
        "status": "success",
        "correlation_id": CORRELATION_ID,
        "message_count": 1,
    }
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize("encoded", [False, True])
async def test_read_cached_state_uses_shared_view_with_canonical_writer_and_explicit_legacy_reader(
    encoded: bool, app: AgentFunctionApp
) -> None:
    raw = _shared_state()
    client = _client(json.dumps(raw) if encoded else raw)

    state = await app._read_cached_state(client, df.EntityId(f"dafx-{AGENT_NAME}", SESSION_ID))

    assert isinstance(state, SharedAgentStateReader) and not isinstance(state, DurableAgentState)
    assert state.to_dict() == raw
    canonical = DurableAgentState()
    assert canonical.schema_version == "2.0.0"
    assert canonical.to_dict()["data"] == {
        "conversationHistory": [],
        "terminalResults": {},
        "completionReceipts": {},
    }
    legacy = LegacyDurableAgentState()
    assert legacy.schema_version == "1.1.0"
    assert "terminalResults" not in legacy.to_dict()["data"]
    assert "completionReceipts" not in legacy.to_dict()["data"]


@pytest.mark.parametrize("version", ["1.0.0", "1.1.0", "1.2.0"])
@pytest.mark.parametrize("encoded", [False, True])
async def test_read_cached_legacy_state_uses_compatibility_reader_without_migration(
    version: str, encoded: bool, app: AgentFunctionApp
) -> None:
    raw = {"schemaVersion": version, "data": {"conversationHistory": [_history_entry()]}}
    stored = json.dumps(raw) if encoded else raw
    client = _client(stored)

    state = await app._read_cached_state(client, df.EntityId(f"dafx-{AGENT_NAME}", SESSION_ID))

    assert isinstance(state, LegacyDurableAgentState)
    assert not isinstance(state, (DurableAgentState, SharedAgentStateReader))
    assert state.schema_version == version
    response = state.try_get_agent_response(CORRELATION_ID)
    assert response is not None and response.text == "Legacy transcript answer"
    assert state.to_dict()["schemaVersion"] == version
    assert "terminalResults" not in state.to_dict()["data"]
    assert "completionReceipts" not in state.to_dict()["data"]
    assert client.read_entity_state.return_value.entity_state == stored
    client.signal_entity.assert_not_awaited()


@pytest.mark.parametrize("plain_text", [False, True])
async def test_fire_and_forget_http_remains_202_without_state_read(
    plain_text: bool, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    client = _client(None)

    response = await handlers[0](_request(plain_text=plain_text, wait=False), client)

    assert response.status_code == 202
    if plain_text:
        assert response.get_body() == b"Agent request accepted"
    else:
        assert json.loads(response.get_body()) == {
            "response": "Agent request accepted",
            "message": "question",
            "session_id": SESSION_ID,
            "status": "accepted",
            "correlation_id": CORRELATION_ID,
        }
    client.signal_entity.assert_awaited_once()
    client.read_entity_state.assert_not_awaited()
    sleep.assert_not_awaited()


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
async def test_registered_agent_mcp_reports_unavailable_with_retained_outcome(
    outcome: str, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    raw = _shared_state(outcome=outcome, availability="unavailable")
    client = _client(raw)

    with pytest.raises(RuntimeError) as failure:
        await handlers[1](json.dumps({"arguments": {"query": "question", "sessionId": SESSION_ID}}), client)

    assert "unavailable" in str(failure.value) and f"outcome: {outcome}" in str(failure.value)
    assert EXPIRED_MESSAGE in str(failure.value)
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize("failed", [False, True])
async def test_registered_agent_mcp_empty_success_and_failure_are_distinct(
    failed: bool, handlers: tuple[HttpHandler, McpHandler], sleep: AsyncMock
) -> None:
    raw = _shared_state(outcome="failed" if failed else "succeeded", response=_wire_response(""))
    client = _client(raw)
    context = json.dumps({"arguments": {"query": "question", "sessionId": SESSION_ID}})

    if failed:
        with pytest.raises(RuntimeError, match=ERROR_MESSAGE):
            await handlers[1](context, client)
    else:
        assert await handlers[1](context, client) == ""
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize("precompleted", [False, True])
@pytest.mark.parametrize("value, response_format", [({"answer": 42}, Answer), (None, NullAnswer)])
async def test_http_snapshot_reaches_public_proxy_typed_task_without_reparsing_text(
    precompleted: bool,
    value: Any,
    response_format: type[BaseModel],
    app: AgentFunctionApp,
    handlers: tuple[HttpHandler, McpHandler],
    sleep: AsyncMock,
) -> None:
    stored = _wire_response("Not JSON, the retained value is authoritative")
    stored["value"] = deepcopy(value)
    raw = _shared_state(response=stored)
    client = _client(raw)
    http_response = await handlers[0](_request(), client)
    assert http_response.status_code == 200
    payload = json.loads(http_response.get_body())["agent_response"]
    before = deepcopy(payload)
    child = AtomicTask(7, NoOpAction())
    if precompleted:
        child.set_value(is_error=False, value=payload)
    context = Mock(spec=df.DurableOrchestrationContext)
    context.instance_id = "shared-orchestration"
    context.new_uuid.side_effect = [SESSION_ID, CORRELATION_ID]
    context.call_entity.return_value = child
    proxy = app.get_agent(context, AGENT_NAME)

    task = proxy.run("question", session=proxy.create_session(), options={"response_format": response_format})

    context.call_entity.assert_called_once()
    entity_id, operation, request = context.call_entity.call_args.args
    assert entity_id.name == f"dafx-{AGENT_NAME}" and entity_id.key == SESSION_ID
    assert operation == "run" and request["correlationId"] == CORRELATION_ID
    assert request["orchestrationId"] == "shared-orchestration"
    context.signal_entity.assert_not_called()
    if not precompleted:
        assert task.state is TaskState.RUNNING
        child.set_value(is_error=False, value=payload)
    assert task.state is TaskState.SUCCEEDED and isinstance(task.result, AgentResponse)
    assert isinstance(task.result.value, response_format)
    assert task.result.value.model_dump(mode="json") == value
    assert child.result is payload and payload == before
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize("precompleted", [False, True])
@pytest.mark.parametrize("shape", ["absent", "coerced", "dropped-field", "valid", "null"])
async def test_http_shared_value_policy_survives_loading_and_typed_task_delivery(
    precompleted: bool,
    shape: str,
    handlers: tuple[HttpHandler, McpHandler],
    sleep: AsyncMock,
) -> None:
    from agent_framework_durabletask import ensure_response_format, serialize_agent_response

    from agent_framework_azurefunctions._orchestration import AgentTask

    stored = _wire_response('{"answer":42}')
    values = {
        "coerced": {"answer": "42"},
        "dropped-field": {"answer": 42, "future": False},
        "valid": {"answer": 42},
        "null": None,
    }
    if shape != "absent":
        stored["value"] = deepcopy(values[shape])
    raw = _shared_state(response=stored)
    client = _client(raw)
    http = await handlers[0](_request(), client)
    assert http.status_code == 200
    snapshot = json.loads(http.get_body())["agent_response"]
    original_snapshot = deepcopy(snapshot)
    response_format = NullAnswer if shape == "null" else Answer
    valid = shape in ("valid", "null")
    for _ in range(2):
        direct = load_agent_response(deepcopy(snapshot))
        if valid:
            ensure_response_format(response_format, CORRELATION_ID, direct)
            assert isinstance(direct.value, response_format)
            assert direct.value.model_dump(mode="json") == values[shape]
        else:
            with pytest.raises(ValueError, match="no structured value|cannot preserve"):
                ensure_response_format(response_format, CORRELATION_ID, direct)
        child = AtomicTask(7, NoOpAction())
        if precompleted:
            child.set_value(is_error=False, value=deepcopy(snapshot))
        task = AgentTask(child, response_format, CORRELATION_ID)
        if not precompleted:
            child.set_value(is_error=False, value=deepcopy(snapshot))
        assert task.state is (TaskState.SUCCEEDED if valid else TaskState.FAILED)
        # A second consumer serialization must not remove the policy either.
        snapshot = json.loads(json.dumps(serialize_agent_response(load_agent_response(snapshot))))
        assert ("value" in snapshot) is (shape != "absent")
        if shape != "absent":
            assert snapshot["value"] == values[shape]
    assert client.read_entity_state.return_value.entity_state == raw
    assert json.loads(http.get_body())["agent_response"] == original_snapshot
    _assert_one_delivery(client, sleep, raw)


@pytest.mark.parametrize("status", ["error", "timeout", "accepted"])
def test_empty_non_success_text_still_uses_diagnostic(status: str, app: AgentFunctionApp) -> None:
    assert app._convert_payload_to_text({"status": status, "response": "", "error": "Diagnostic"}) == "Diagnostic"
