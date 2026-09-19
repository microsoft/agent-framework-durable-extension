# Copyright (c) Microsoft. All rights reserved.

"""Workflow dispatch through the real shim, request serializer and DurableTask adapter."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from unittest.mock import Mock
from uuid import UUID

import pytest
from agent_framework import AgentExecutor, AgentExecutorResponse, AgentResponse, AgentSession, Content, Message
from durabletask.task import CompletableTask, OrchestrationContext

from agent_framework_durabletask import DurableAgentStateRequest, RunRequest
from agent_framework_durabletask._executors import DurableAgentExecutor, DurableAgentTask
from agent_framework_durabletask._shim import DurableAIAgent
from agent_framework_durabletask._workflows.dt_context import DurableTaskWorkflowContext
from agent_framework_durabletask._workflows.orchestrator import (
    _AGENT_TASK_MESSAGE_PREVIEW_LIMIT,
    _prepare_agent_task,
    _WorkflowDeliveryLedger,
    build_agent_executor_response,
)


class _StubAgent:
    name = "stub"
    id = "stub"
    description = None

    def create_session(self, **kwargs: Any) -> AgentSession:
        return AgentSession(**kwargs)

    async def run(self, messages: Any = None, **kwargs: Any) -> AgentResponse:
        raise AssertionError("Dispatch must schedule an entity, not invoke a model")


class _CaptureExecutor(DurableAgentExecutor[RunRequest]):
    """Capture dispatch without replacing the inherited get_run_request implementation."""

    def __init__(self) -> None:
        self.requests: list[RunRequest] = []

    def generate_unique_id(self) -> str:
        return str(UUID(int=len(self.requests) + 1))

    def run_durable_agent(
        self, agent_name: str, run_request: RunRequest, session: AgentSession | None = None
    ) -> RunRequest:
        self.requests.append(run_request)
        return run_request


def _agent(**kwargs: Any) -> AgentExecutor:
    stub: Any = _StubAgent()
    return AgentExecutor(stub, id="target", **kwargs)


def _upstream(messages: list[Message]) -> AgentExecutorResponse:
    return AgentExecutorResponse(
        executor_id="source",
        agent_response=AgentResponse(messages=messages[-1:]),
        full_conversation=list(messages),
    )


def _context(calls: int = 2) -> tuple[DurableTaskWorkflowContext, Mock, list[CompletableTask[Any]]]:
    host = Mock(spec=OrchestrationContext)
    host.instance_id = "dispatch-revision-run"
    host.is_replaying = False
    host.current_utc_datetime = datetime(2026, 1, 1, tzinfo=timezone.utc)
    host.new_uuid.side_effect = [str(UUID(int=index + 1)) for index in range(calls)]
    children: list[CompletableTask[Any]] = [CompletableTask() for _ in range(calls)]
    host.call_entity.side_effect = children
    return DurableTaskWorkflowContext(host), host, children


def _dispatch(
    context: DurableTaskWorkflowContext,
    host: Mock,
    executor: AgentExecutor,
    message: Any,
    ledger: _WorkflowDeliveryLedger,
) -> tuple[DurableAgentTask, dict[str, Any]]:
    task = _prepare_agent_task(context, executor, executor.id, message, "dispatch-revision", ledger)
    assert isinstance(task, DurableAgentTask)
    assert not task.is_complete
    _, operation, payload = host.call_entity.call_args.args
    assert operation == "run"
    # This is the actual executor's RunRequest.to_dict(), not a reconstruction of its arguments.
    wire = json.loads(json.dumps(payload, allow_nan=False))
    assert wire["orchestrationId"] == context.instance_id
    assert wire["correlationId"] == str(UUID(int=host.call_entity.call_count))
    assert host.new_uuid.call_count == host.call_entity.call_count
    if "contextMessages" in wire:
        assert len(wire["contextMessageIds"]) == len(wire["contextMessages"])
        assert all(isinstance(identity, str) and identity for identity in wire["contextMessageIds"])
    else:
        assert "contextMessageIds" not in wire
    host.signal_entity.assert_not_called()
    return task, wire


@pytest.mark.parametrize("preview", ["", "unselected logging preview"])
def test_shim_preserves_explicit_empty_context_in_the_real_run_request(preview: str) -> None:
    executor = _CaptureExecutor()
    agent = DurableAIAgent(executor, "target")

    request = agent.run(preview, context_messages=[], context_message_ids=[])
    wire = json.loads(json.dumps(request.to_dict()))

    assert executor.requests == [request]
    assert wire["contextMessages"] == []
    assert wire["contextMessageIds"] == []
    restored = RunRequest.from_dict(wire)
    assert restored.context_messages == []
    assert restored.context_message_ids == []
    assert DurableAgentStateRequest.from_run_request(restored).messages == []


def test_shim_does_not_preprocess_or_drop_raw_context_type_fields() -> None:
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
    executor = _CaptureExecutor()

    request = DurableAIAgent(executor, "target").run(
        "", context_messages=context_messages, context_message_ids=["occurrence-0"]
    )
    wire = json.loads(json.dumps(request.to_dict(), allow_nan=False))

    assert wire["message"] == ""
    assert wire["contextMessages"] == before
    assert wire["contextMessageIds"] == ["occurrence-0"]
    assert RunRequest.from_dict(wire).context_messages == before
    assert RunRequest.from_dict(wire).context_message_ids == ["occurrence-0"]
    assert context_messages == before


@pytest.mark.parametrize(
    ("messages", "expected"),
    [
        pytest.param(None, "", id="none"),
        pytest.param([], "", id="empty-list"),
        pytest.param("standalone", "standalone", id="text"),
        pytest.param(Message("user", ["standalone"]), "standalone", id="message"),
        pytest.param(["first", "second"], "first\nsecond", id="text-list"),
    ],
)
def test_shim_without_context_retains_standalone_text_normalization(messages: Any, expected: str) -> None:
    executor = _CaptureExecutor()

    request = DurableAIAgent(executor, "target").run(messages, context_messages=None)

    assert request.message == expected
    assert request.context_messages is None
    assert "contextMessages" not in request.to_dict()
    assert executor.requests == [request]


@pytest.mark.parametrize(
    "messages",
    [
        pytest.param("", id="empty-text"),
        pytest.param(Message("user", []), id="contentless-message"),
        pytest.param(
            Message("tool", [Content.from_function_result("lookup-1", result={"answer": 42})]),
            id="nontext-message",
        ),
    ],
)
def test_shim_without_context_still_rejects_nontext_inputs(messages: Any) -> None:
    executor = _CaptureExecutor()

    with pytest.raises(ValueError, match="only supports text message inputs"):
        DurableAIAgent(executor, "target").run(messages, context_messages=None)

    assert executor.requests == []


def test_custom_empty_projection_reaches_the_dt_entity_as_an_empty_list() -> None:
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


def test_fully_duplicate_projection_reaches_the_dt_entity_on_the_second_call() -> None:
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
    assert len(set(first["contextMessageIds"])) == 2
    assert set(first["contextMessageIds"]).isdisjoint(message.message_id for message in messages)
    _, repeated = _dispatch(context, host, executor, upstream, ledger)

    assert repeated["contextMessages"] == []
    assert repeated["contextMessageIds"] == []
    assert repeated["message"] == ""
    assert first["correlationId"] != repeated["correlationId"]
    assert DurableAgentStateRequest.from_run_request(RunRequest.from_dict(repeated)).messages == []
    assert len(ledger.sent["target"]) == 2
    assert ledger.handoffs == {"target": 2}
    assert [message.to_dict() for message in messages] == expected
    assert host.call_entity.call_count == 2


def test_tool_only_projection_survives_dt_dispatch_and_request_parsing() -> None:
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
    assert request.context_message_ids == wire["contextMessageIds"]
    assert request.context_message_ids != [message.message_id]
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

    assert not children[0].is_complete
    children[0].complete(AgentResponse(messages=[Message("assistant", ["received"])]).to_dict())
    assert task.is_complete and not task.is_failed
    assert context.get_task_result(task).text == "received"


def test_standalone_dt_input_is_not_truncated_or_deduplicated() -> None:
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


def test_eight_hundred_turns_have_a_bounded_real_dt_request_envelope() -> None:
    context, host, _ = _context(calls=801)
    executor = _agent()
    ledger = _WorkflowDeliveryLedger()
    upstream: Any = "initial prompt"
    wire: dict[str, Any] = {}
    for turn in range(800):
        upstream = build_agent_executor_response("source", f"turn-{turn}:" + "x" * 1600, None, upstream)
        _, wire = _dispatch(context, host, executor, upstream, ledger)

    latest = upstream.full_conversation[-1]
    assert wire["contextMessages"] == [latest.to_dict()]
    assert len(wire["contextMessageIds"]) == 1
    assert wire["message"] == latest.text[:_AGENT_TASK_MESSAGE_PREVIEW_LIMIT]
    assert len(wire["message"]) == _AGENT_TASK_MESSAGE_PREVIEW_LIMIT
    payload_bytes = len(json.dumps(wire).encode("utf-8"))
    context_bytes = len(json.dumps([latest.to_dict()]).encode("utf-8"))
    full_bytes = len(json.dumps([message.to_dict() for message in upstream.full_conversation]).encode("utf-8"))
    assert payload_bytes <= context_bytes + _AGENT_TASK_MESSAGE_PREVIEW_LIMIT + 512
    assert full_bytes > 100 * payload_bytes
    assert "initial prompt" not in json.dumps(wire)

    _, repeated = _dispatch(context, host, executor, upstream, ledger)
    assert repeated["contextMessages"] == []
    assert repeated["contextMessageIds"] == []
    assert repeated["message"] == ""
    assert len(json.dumps(repeated).encode("utf-8")) < 512
    assert host.call_entity.call_count == 801
