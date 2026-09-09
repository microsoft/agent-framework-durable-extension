# Copyright (c) Microsoft. All rights reserved.

"""Workflow dispatch through the real Azure Functions adapter and shared shim."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from unittest.mock import Mock
from uuid import UUID

import azure.durable_functions as df
import pytest
from agent_framework import AgentExecutor, AgentExecutorResponse, AgentResponse, AgentSession, Content, Message
from agent_framework_durabletask import DurableAgentStateRequest, RunRequest
from agent_framework_durabletask._workflows.orchestrator import _prepare_agent_task, _WorkflowDeliveryLedger
from azure.durable_functions.models.actions.NoOpAction import NoOpAction
from azure.durable_functions.models.Task import AtomicTask, TaskState

from agent_framework_azurefunctions._orchestration import AgentTask
from agent_framework_azurefunctions._workflow_af_context import AzureFunctionsWorkflowContext


class _StubAgent:
    name = "stub"
    id = "stub"
    description = None

    def create_session(self, **kwargs: Any) -> AgentSession:
        return AgentSession(**kwargs)

    async def run(self, messages: Any = None, **kwargs: Any) -> AgentResponse:
        raise AssertionError("Dispatch must schedule an entity, not invoke a model")


def _agent(**kwargs: Any) -> AgentExecutor:
    stub: Any = _StubAgent()
    return AgentExecutor(stub, id="target", **kwargs)


def _upstream(messages: list[Message]) -> AgentExecutorResponse:
    return AgentExecutorResponse(
        executor_id="source",
        agent_response=AgentResponse(messages=messages[-1:]),
        full_conversation=list(messages),
    )


def _context() -> tuple[AzureFunctionsWorkflowContext, Mock, list[AtomicTask]]:
    host = Mock(spec=df.DurableOrchestrationContext)
    host.instance_id = "dispatch-revision-run"
    host.is_replaying = False
    host.current_utc_datetime = datetime(2026, 1, 1, tzinfo=timezone.utc)
    host.new_uuid.side_effect = [str(UUID(int=index + 1)) for index in range(2)]
    children = [AtomicTask(index + 1, NoOpAction()) for index in range(2)]
    host.call_entity.side_effect = children
    return AzureFunctionsWorkflowContext(host), host, children


def _dispatch(
    context: AzureFunctionsWorkflowContext,
    host: Mock,
    executor: AgentExecutor,
    message: Any,
    ledger: _WorkflowDeliveryLedger,
) -> tuple[AgentTask, dict[str, Any]]:
    task = _prepare_agent_task(context, executor, executor.id, message, "dispatch-revision", ledger)
    assert isinstance(task, AgentTask)
    assert not task.is_completed
    entity_id, operation, payload = host.call_entity.call_args.args
    assert entity_id.name == "dafx-dispatch-revision-target"
    assert entity_id.key == context.instance_id
    assert operation == "run"
    # Capture the real executor's serialized RunRequest after build_agent_task and the shim.
    wire = json.loads(json.dumps(payload, allow_nan=False))
    assert wire["orchestrationId"] == context.instance_id
    assert wire["correlationId"] == str(UUID(int=host.call_entity.call_count))
    assert host.new_uuid.call_count == host.call_entity.call_count
    host.signal_entity.assert_not_called()
    return task, wire


def test_custom_empty_projection_reaches_the_af_entity_as_an_empty_list() -> None:
    context, host, _ = _context()
    executor = _agent(context_mode="custom", context_filter=lambda messages: [])
    excluded = Message("assistant", ["unselected secret" * 1000], message_id="wf_source_0")
    ledger = _WorkflowDeliveryLedger()

    _, wire = _dispatch(context, host, executor, _upstream([excluded]), ledger)

    assert wire["message"] == ""
    assert wire["contextMessages"] == []
    assert "unselected secret" not in json.dumps(wire)
    assert DurableAgentStateRequest.from_run_request(RunRequest.from_dict(wire)).messages == []
    assert ledger.sent == {}
    assert ledger.handoffs == {"target": 1}
    host.call_entity.assert_called_once()


def test_fully_duplicate_projection_reaches_the_af_entity_on_the_second_call() -> None:
    context, host, _ = _context()
    executor = _agent()
    messages = [
        Message("user", ["question"], message_id="wf_source_0"),
        Message("assistant", ["answer"], message_id="wf_source_1"),
    ]
    upstream = _upstream(messages)
    expected = [message.to_dict() for message in messages]
    ledger = _WorkflowDeliveryLedger()

    _, first = _dispatch(context, host, executor, upstream, ledger)
    assert first["contextMessages"] == expected
    _, repeated = _dispatch(context, host, executor, upstream, ledger)

    assert repeated["contextMessages"] == []
    assert repeated["message"] == ""
    assert first["correlationId"] != repeated["correlationId"]
    assert DurableAgentStateRequest.from_run_request(RunRequest.from_dict(repeated)).messages == []
    assert len(ledger.sent["target"]) == 2
    assert ledger.handoffs == {"target": 2}
    assert [message.to_dict() for message in messages] == expected
    assert host.call_entity.call_count == 2


def test_tool_only_projection_survives_af_dispatch_and_request_parsing() -> None:
    context, host, children = _context()
    result = {"type": "lookup_result", "items": [{"answer": 0, "label": "世界"}], "flags": [False, None]}
    message = Message(
        "tool",
        [Content.from_function_result("lookup-1", result=result)],
        message_id="wf_source_0",
        author_name="lookup",
        additional_properties={"provider": {"type": "context", "labels": []}},
    )
    expected = message.to_dict()
    ledger = _WorkflowDeliveryLedger()

    task, wire = _dispatch(context, host, _agent(), _upstream([message]), ledger)

    assert wire["message"] == ""
    assert wire["contextMessages"] == [expected]
    request = RunRequest.from_json(json.dumps(wire))
    entry = DurableAgentStateRequest.from_run_request(request)
    assert len(entry.messages) == 1
    forwarded = entry.messages[0].to_chat_message()
    assert isinstance(forwarded, Message)
    assert forwarded.role == "tool"
    assert forwarded.message_id == message.message_id
    assert forwarded.text == ""
    assert len(forwarded.contents) == 1
    assert forwarded.contents[0].type == "function_result"
    assert forwarded.contents[0].call_id == "lookup-1"
    assert forwarded.contents[0].result == message.contents[0].result
    assert json.loads(forwarded.contents[0].result) == result
    assert message.to_dict() == expected

    assert not children[0].is_completed
    children[0].set_value(is_error=False, value=AgentResponse(messages=[Message("assistant", ["received"])]).to_dict())
    assert task.state == TaskState.SUCCEEDED
    assert context.get_task_result(task).text == "received"


def test_af_adapter_does_not_preprocess_or_drop_raw_context_type_fields() -> None:
    context, host, _ = _context()
    context_messages = [
        {
            "type": "message",
            "role": "tool",
            "message_id": "wf_source_0",
            "contents": [
                {
                    "type": "function_result",
                    "call_id": "lookup-1",
                    "result": {"type": "application_payload", "items": [0, False, None, "世界"]},
                    "future_content_field": {"type": "opaque", "items": []},
                },
            ],
            "future_message_field": {"type": "opaque", "items": []},
        },
    ]
    before = deepcopy(context_messages)

    task = context.prepare_agent_task("dispatch-revision-target", "", context.instance_id, context_messages)

    assert isinstance(task, AgentTask)
    assert not task.is_completed
    host.call_entity.assert_called_once()
    wire = json.loads(json.dumps(host.call_entity.call_args.args[2], allow_nan=False))
    assert wire["message"] == ""
    assert wire["contextMessages"] == before
    assert RunRequest.from_dict(wire).context_messages == before
    assert context_messages == before


def test_standalone_af_input_is_not_truncated_or_deduplicated() -> None:
    context, host, _ = _context()
    executor = _agent()
    ledger = _WorkflowDeliveryLedger()
    prompt = "standalone input " * 1000

    for _ in range(2):
        _, wire = _dispatch(context, host, executor, prompt, ledger)
        assert wire["message"] == prompt
        assert "contextMessages" not in wire
        assert RunRequest.from_dict(wire).context_messages is None

    assert ledger.sent == {}
    assert host.call_entity.call_count == 2


def test_empty_standalone_af_input_still_fails_before_scheduling() -> None:
    context, host, _ = _context()
    ledger = _WorkflowDeliveryLedger()

    with pytest.raises(ValueError, match="only supports text message inputs"):
        _prepare_agent_task(context, _agent(), "target", "", "dispatch-revision", ledger)

    host.call_entity.assert_not_called()
    host.new_uuid.assert_not_called()
    assert ledger == _WorkflowDeliveryLedger()
