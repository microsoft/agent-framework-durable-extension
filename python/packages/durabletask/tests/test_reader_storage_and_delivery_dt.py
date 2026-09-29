# Copyright (c) Microsoft. All rights reserved.

"""State admission and response delivery at SDK storage and task completion boundaries."""

from __future__ import annotations

import json
from copy import deepcopy
from typing import Any
from unittest.mock import Mock, call

import pytest
from agent_framework import AgentResponse, Content
from durabletask.client import TaskHubGrpcClient
from durabletask.entities import EntityContext, EntityInstanceId
from durabletask.internal.entity_state_shim import StateShim
from durabletask.serialization import JsonDataConverter
from durabletask.task import CompletableTask, TaskFailedError
from pydantic import BaseModel, ValidationError

from agent_framework_durabletask import (
    AgentEntity,
    AgentSessionId,
    DurableAgentSession,
    DurableAgentState,
    DurableAIAgentClient,
    ensure_response_format,
    load_agent_response,
)
from agent_framework_durabletask._entities import DurableTaskEntityStateProvider
from agent_framework_durabletask._executors import ClientAgentExecutor, DurableAgentTask

CORRELATION = "reader-followup"
ENTITY = EntityInstanceId("dafx-assistant", "reader-session")
LEGACY_KINDS = ("bare-error", "error-invalid-json", "error-valid-json", "approval-no-result")
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


class Answer(BaseModel):
    answer: int


def _sdk_provider(persisted: str | None) -> tuple[DurableTaskEntityStateProvider, StateShim]:
    converter = JsonDataConverter()
    shim = StateShim(persisted, converter, is_serialized=True)
    context = EntityContext("reader-orchestration", "run", shim, ENTITY, converter)
    provider = DurableTaskEntityStateProvider()
    provider._initialize_entity_context(context)
    return provider, shim


@pytest.mark.parametrize(
    ("persisted", "coerced"),
    [
        pytest.param("[]", {}, id="empty-array"),
        pytest.param('""', {}, id="json-empty-string"),
        pytest.param(
            '[["schemaVersion","1.1.0"],["data",{"conversationHistory":[]}]]',
            {"schemaVersion": "1.1.0", "data": {"conversationHistory": []}},
            id="mapping-pair-array",
        ),
    ],
)
@pytest.mark.parametrize("operation", ["getter", "setter", "persist", "warm-persist", "reset", "run"])
async def test_sdk_coercible_nonobjects_are_rejected_before_cache_changes_or_writes(
    persisted: str, coerced: dict[str, Any], operation: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider, shim = _sdk_provider("{}" if operation == "warm-persist" else persisted)
    if operation == "warm-persist":
        _ = provider.state
        # Replace only the persisted wire value. All reads still traverse the SDK.
        monkeypatch.setattr(shim, "_current_state", persisted)
    cache = provider._state_cache
    cache_before = cache.to_dict() if cache is not None else None
    writes = Mock(wraps=shim.set_state)
    monkeypatch.setattr(shim, "set_state", writes)
    agent = Mock()
    agent.run.side_effect = AssertionError("The agent must not execute before state admission")

    # Pin the SDK behavior that a mock returning already-decoded state would miss.
    assert provider.entity_context.get_state() == json.loads(persisted)
    assert provider.entity_context.get_state(dict, default={}) == coerced
    assert shim.encode_state() == persisted

    with pytest.raises(ValueError, match="JSON object"):
        if operation == "getter":
            _ = provider.state
        elif operation == "setter":
            provider.state = DurableAgentState()
        elif operation in ("persist", "warm-persist"):
            provider.persist_state()
        elif operation == "reset":
            provider.reset()
        else:
            await AgentEntity(agent, state_provider=provider).run({"message": "question", "correlationId": CORRELATION})

    writes.assert_not_called()
    agent.run.assert_not_called()
    assert shim.encode_state() == persisted
    assert provider._state_cache is cache
    if cache is not None:
        assert cache.to_dict() == cache_before


@pytest.mark.parametrize("persisted", [None, "{}"], ids=["absent-state", "empty-object"])
def test_sdk_absent_state_and_real_empty_object_are_writable_v2(
    persisted: str | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider, shim = _sdk_provider(persisted)
    assert provider.entity_context.get_state() == (None if persisted is None else {})
    writes = Mock(wraps=shim.set_state)
    monkeypatch.setattr(shim, "set_state", writes)

    state = provider.state

    assert state.schema_version == "2.0.0"
    assert state.message_count == 0
    assert state.to_dict()["data"] == {
        "conversationHistory": [],
        "terminalResults": {},
        "completionReceipts": {},
    }
    assert shim.encode_state() == persisted
    writes.assert_not_called()
    provider.persist_state()
    writes.assert_called_once_with(state.to_dict())
    encoded = shim.encode_state()
    assert encoded is not None
    assert json.loads(encoded) == state.to_dict()


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


@pytest.mark.parametrize("kind", LEGACY_KINDS)
def test_legacy_content_does_not_suppress_required_response_format(kind: str) -> None:
    raw = _legacy_inline(kind)
    before = deepcopy(raw)
    response = load_agent_response(raw)
    assert "durable_status" not in response.additional_properties
    assert bool(response.user_input_requests) is (kind == "approval-no-result")
    baseline = AgentResponse(messages=deepcopy(response.messages), response_format=Answer)

    if kind == "error-valid-json":
        assert baseline.value == Answer(answer=42)
        ensure_response_format(Answer, CORRELATION, response)
        assert response.value == Answer(answer=42)
        assert response.messages[0].contents[0].error_code == "LegacyFailure"
    elif kind == "error-invalid-json":
        with pytest.raises(ValidationError) as baseline_error:
            _ = baseline.value
        with pytest.raises(ValidationError) as actual_error:
            ensure_response_format(Answer, CORRELATION, response)
        assert actual_error.value.errors() == baseline_error.value.errors()
    else:
        assert baseline.value is None
        with pytest.raises(ValueError, match="could not be parsed into required format Answer"):
            ensure_response_format(Answer, CORRELATION, response)
    assert raw == before


def _complete_inline(raw: dict[str, Any], precompleted: bool) -> DurableAgentTask:
    child: CompletableTask[Any] = CompletableTask()
    if precompleted:
        child.complete(raw)
    task = DurableAgentTask(child, Answer, CORRELATION)
    assert task.get_tasks() == [child]
    if not precompleted:
        assert not task.is_complete
        child.complete(raw)
    assert child.is_complete and not child.is_failed
    assert child.get_result() is raw
    assert task.is_complete
    return task


@pytest.mark.parametrize("precompleted", [False, True], ids=["delayed", "precompleted"])
@pytest.mark.parametrize("kind", LEGACY_KINDS)
def test_real_dt_task_retains_legacy_typed_success_and_failure(kind: str, precompleted: bool) -> None:
    raw = _legacy_inline(kind)
    before = deepcopy(raw)

    task = _complete_inline(raw, precompleted)

    if kind == "error-valid-json":
        assert not task.is_failed
        response = task.get_result()
        assert response.value == Answer(answer=42)
        assert response.messages[0].contents[0].error_code == "LegacyFailure"
        assert "durable_status" not in response.additional_properties
    else:
        assert task.is_failed
        failure = task.get_exception()
        expected_type = "ValidationError" if kind == "error-invalid-json" else "ValueError"
        assert failure.details.error_type == expected_type
        expected_message = "Invalid JSON" if kind == "error-invalid-json" else "required format Answer"
        assert expected_message in failure.details.message
        with pytest.raises(TaskFailedError) as raised:
            task.get_result()
        assert raised.value is failure
    assert raw == before


@pytest.mark.parametrize("precompleted", [False, True], ids=["delayed", "precompleted"])
@pytest.mark.parametrize("status", ["error", "already_completed", "accepted"])
def test_explicit_durable_status_still_skips_typed_validation(status: str, precompleted: bool) -> None:
    raw = {
        "type": "agent_response",
        "messages": [{"role": "assistant", "contents": [{"type": "text", "text": "not JSON"}]}],
        "value": {"answer": "not-an-integer"},
        "additional_properties": {"durable_status": status, "correlation_id": CORRELATION},
    }
    before = deepcopy(raw)
    direct = load_agent_response(raw)
    ensure_response_format(Answer, CORRELATION, direct)
    assert type(direct.value) is dict and direct.value == raw["value"]

    task = _complete_inline(raw, precompleted)

    assert not task.is_failed
    response = task.get_result()
    assert type(response.value) is dict and response.value == raw["value"]
    assert response.additional_properties == raw["additional_properties"]
    assert raw == before


def _failed_shared(fields: dict[str, str]) -> dict[str, Any]:
    common = {"correlationId": CORRELATION, "outcome": "failed", "completedAt": "2026-09-17T11:00:00Z"}
    return {
        "schemaVersion": "2.0.0",
        "data": {
            "conversationHistory": [],
            "completionReceipts": {CORRELATION: {**common, "resultState": "available"}},
            "terminalResults": {
                CORRELATION: {
                    **common,
                    "error": {"code": "record_failure", "message": "Required record diagnostic"},
                    "response": {
                        "messages": [
                            {
                                "role": "assistant",
                                "contents": [
                                    {"$type": "text", "text": "Partial answer, not the failure diagnostic"},
                                    {"$type": "error", **deepcopy(fields)},
                                ],
                            }
                        ],
                    },
                }
            },
        },
    }


@pytest.mark.parametrize("fields", SPARSE_ERRORS)
def test_dt_client_fills_only_missing_or_blank_failed_content_diagnostics(
    fields: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = _failed_shared(fields)
    before = deepcopy(raw)
    metadata = Mock(spec=["get_state"])
    metadata.get_state.return_value = raw
    rpc = Mock(spec=TaskHubGrpcClient)
    rpc.get_entity.return_value = metadata
    sleep = Mock()
    monkeypatch.setattr("agent_framework_durabletask._executors.time.sleep", sleep)
    monkeypatch.setattr(ClientAgentExecutor, "generate_unique_id", lambda self: CORRELATION)
    agent = DurableAIAgentClient(rpc, max_poll_retries=2, poll_interval_seconds=0.01).get_agent("assistant")
    session = DurableAgentSession.from_session_id(AgentSessionId("assistant", "reader-session"))

    response = agent.run("question", session=session, options={"response_format": Answer})

    errors = [item for message in response.messages for item in message.contents if item.type == "error"]
    assert len(errors) == 1
    expected_code = fields["errorCode"] if fields.get("errorCode", "").strip() else "record_failure"
    expected_message = fields["message"] if fields.get("message", "").strip() else "Required record diagnostic"
    assert errors[0].error_code == expected_code
    assert errors[0].message == expected_message
    assert response.additional_properties["durable_status"] == "error"
    assert response.text == "Partial answer, not the failure diagnostic"
    assert response.value is None
    rpc.signal_entity.assert_called_once()
    assert rpc.get_entity.call_args_list == [call(ENTITY, include_state=True)]
    metadata.get_state.assert_called_once_with()
    sleep.assert_called_once_with(0.01)
    assert raw == before
    errors[0].message = "Consumer edit"
    assert raw == before
