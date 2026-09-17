# Copyright (c) Microsoft. All rights reserved.

"""Bare Functions operation failures, Core responses and native task failures stay distinct."""

import logging
from collections import UserDict
from copy import deepcopy
from typing import Any
from unittest.mock import Mock

import azure.durable_functions as df
import pytest
from agent_framework import AgentResponse, Content, Message
from agent_framework_durabletask import DurableAgentState, serialize_agent_response
from azure.durable_functions.models.actions.NoOpAction import NoOpAction
from azure.durable_functions.models.Task import AtomicTask, TaskState
from pydantic import BaseModel, RootModel

from agent_framework_azurefunctions import AgentFunctionApp
from agent_framework_azurefunctions import _orchestration as orchestration
from agent_framework_azurefunctions._entities import create_agent_entity

CORRELATION_ID = "trusted-operation-correlation"
SPOOFED_CORRELATION = "untrusted-payload-correlation"
AGENT_NAME = "operation-diagnostics"
SESSION_ID = "operation-session"
DIAGNOSTIC = "Operation rejected\n  原因: café ☃\n  keep trailing spaces  "
NON_STRING_ERRORS: tuple[Any, ...] = (None, 0, False, [], {})
NON_RESPONSE_TYPES: tuple[Any, ...] = (None, "", 0, [])
NON_MESSAGE_COLLECTIONS: tuple[Any, ...] = (None, "", False, {}, [])


class Answer(BaseModel):
    answer: int


class NullAnswer(RootModel[None]):
    pass


class Opaque:
    def __init__(self) -> None:
        self.touches: list[str] = []

    def __str__(self) -> str:
        self.touches.append("str")
        raise AssertionError("Do not render opaque siblings")

    def __repr__(self) -> str:
        self.touches.append("repr")
        raise AssertionError("Do not represent opaque siblings")

    def __deepcopy__(self, memo: dict[int, Any]) -> Any:
        self.touches.append("deepcopy")
        raise AssertionError("Do not copy opaque siblings")


@pytest.fixture(params=[False, True], ids=["delayed", "precompleted"])
def precompleted(request: pytest.FixtureRequest) -> bool:
    return request.param


@pytest.fixture(params=[None, Answer, NullAnswer], ids=["untyped", "typed", "null-root"])
def response_format(request: pytest.FixtureRequest) -> type[BaseModel] | None:
    return request.param


def _complete(
    raw: Any, response_format: type[BaseModel] | None, *, precompleted: bool, is_error: bool = False
) -> tuple[orchestration.AgentTask, AtomicTask]:
    child = AtomicTask(7, NoOpAction())
    if precompleted:
        child.set_value(is_error=is_error, value=raw)
    task = orchestration.AgentTask(child, response_format, CORRELATION_ID)
    if not precompleted:
        assert task.state is TaskState.RUNNING and not task.is_completed
        child.set_value(is_error=is_error, value=raw)
    assert task.is_completed and child.is_completed
    assert task.id == child.id and task.action_repr == child.action_repr
    assert child.result is raw
    return task, child


def _forbid_parsing(monkeypatch: pytest.MonkeyPatch) -> tuple[Mock, Mock]:
    load = Mock(side_effect=AssertionError("Operation failures must not enter response loading"))
    ensure = Mock(side_effect=AssertionError("Operation failures must not enter structured parsing"))
    monkeypatch.setattr(orchestration, "load_agent_response", load)
    monkeypatch.setattr(orchestration, "ensure_response_format", ensure)
    return load, ensure


def _assert_operation_failure(task: orchestration.AgentTask, child: AtomicTask, diagnostic: str) -> None:
    assert child.state is TaskState.SUCCEEDED and task.state is TaskState.FAILED
    assert type(task.result) is ValueError
    assert str(task.result) == f"Agent entity operation failed for correlation_id {CORRELATION_ID}: {diagnostic}"
    assert SPOOFED_CORRELATION not in str(task.result)


@pytest.mark.parametrize("diagnostic", ["", " \t\r\n  ", "Operation rejected", DIAGNOSTIC])
def test_bare_operation_failure_preserves_detail_and_ignores_opaque_siblings(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    precompleted: bool,
    response_format: type[BaseModel] | None,
    diagnostic: str,
) -> None:
    load, ensure = _forbid_parsing(monkeypatch)
    opaque = Opaque()
    raw = {
        "status": "error",
        "error": diagnostic,
        "correlation_id": SPOOFED_CORRELATION,
        "correlationId": SPOOFED_CORRELATION,
        "future": opaque,
        "response_format": {"module": "never_import_operation_metadata", "qualname": "NeverConstruct"},
    }
    before = deepcopy(raw, {id(opaque): opaque})

    with caplog.at_level(logging.DEBUG, logger="agent_framework.azurefunctions"):
        task, child = _complete(raw, response_format, precompleted=precompleted)

    _assert_operation_failure(task, child, diagnostic)
    load.assert_not_called()
    ensure.assert_not_called()
    assert raw == before and opaque.touches == []
    assert SPOOFED_CORRELATION not in caplog.text
    assert "never_import_operation_metadata" not in caplog.text


@pytest.mark.parametrize(
    "raw",
    [
        {"status": "ERROR", "error": DIAGNOSTIC},
        {"status": " error ", "error": DIAGNOSTIC},
        {"status": None, "error": DIAGNOSTIC},
        {"error": DIAGNOSTIC},
        {"status": "error"},
        *({"status": "error", "error": value} for value in NON_STRING_ERRORS),
        UserDict({"status": "error", "error": DIAGNOSTIC}),
        *({"status": "error", "error": DIAGNOSTIC, "type": value} for value in NON_RESPONSE_TYPES),
        *({"status": "error", "error": DIAGNOSTIC, "messages": value} for value in NON_MESSAGE_COLLECTIONS),
    ],
)
def test_nonmatching_envelopes_delegate_to_normal_loader_without_promoting_diagnostic(
    raw: Any,
    monkeypatch: pytest.MonkeyPatch,
    precompleted: bool,
    response_format: type[BaseModel] | None,
) -> None:
    # Classification must not depend on whether Core accepts an optional or malformed field.
    # The loader owns that decision, including permissive empty/value-only constructor inputs.
    normal_failure = TypeError("Normal Core loader decision")
    load = Mock(side_effect=normal_failure)
    ensure = Mock()
    monkeypatch.setattr(orchestration, "load_agent_response", load)
    monkeypatch.setattr(orchestration, "ensure_response_format", ensure)
    before = deepcopy(raw)

    task, child = _complete(raw, response_format, precompleted=precompleted)

    assert child.state is TaskState.SUCCEEDED and task.state is TaskState.FAILED
    assert task.result is normal_failure
    load.assert_called_once_with(raw)
    ensure.assert_not_called()
    assert raw == before


@pytest.mark.parametrize("envelope", ["typed", "messages-only", "type-only", "value-only", "empty"])
def test_normal_core_constructor_inputs_remain_supported(
    envelope: str, monkeypatch: pytest.MonkeyPatch, precompleted: bool
) -> None:
    original = AgentResponse[Any](
        messages=[Message("assistant", ["Readable answer"])],
        value={"answer": 42},
        additional_properties={"status": "error", "error": "legitimate provider metadata"},
    )
    raw = serialize_agent_response(original)
    expected_text = original.text
    if envelope == "messages-only":
        raw.pop("type")
    elif envelope in ("type-only", "value-only"):
        raw.pop("messages", None)
        expected_text = ""
        if envelope == "value-only":
            raw.pop("type")
    elif envelope == "empty":
        raw = {}
        expected_text = ""
    if envelope in ("typed", "messages-only", "type-only"):
        raw.update(status="error", error=DIAGNOSTIC)
    before = deepcopy(raw)
    load = Mock(wraps=orchestration.load_agent_response)
    monkeypatch.setattr(orchestration, "load_agent_response", load)

    task, child = _complete(raw, None, precompleted=precompleted)

    assert child.state is TaskState.SUCCEEDED and task.state is TaskState.SUCCEEDED
    assert isinstance(task.result, AgentResponse) and task.result.text == expected_text
    if envelope != "empty":
        assert task.result.value == {"answer": 42}
        assert task.result.additional_properties == original.additional_properties
    load.assert_called_once_with(raw)
    assert raw == before


@pytest.mark.parametrize("value, model", [({"answer": 42}, Answer), (None, NullAnswer)])
def test_typed_core_value_and_explicit_null_do_not_reparse_non_json_text(
    value: Any, model: type[BaseModel], precompleted: bool
) -> None:
    raw = {
        "type": "agent_response",
        "messages": [{"role": "assistant", "contents": [{"type": "text", "text": "Not JSON"}]}],
        "value": value,
    }
    before = deepcopy(raw)

    task, child = _complete(raw, model, precompleted=precompleted)

    assert child.state is TaskState.SUCCEEDED and task.state is TaskState.SUCCEEDED
    assert isinstance(task.result.value, model)
    assert task.result.value.model_dump(mode="json") == value
    assert raw == before


@pytest.mark.parametrize("terminal", ["error", "already_completed"])
def test_core_terminal_responses_remain_responses_without_structured_parsing(
    terminal: str, precompleted: bool, response_format: type[BaseModel] | None
) -> None:
    original = AgentResponse(
        messages=[Message("system", [Content.from_error(message=DIAGNOSTIC, error_code="agent_error")])],
        additional_properties={"durable_status": terminal, "durable_outcome": "failed"},
    )

    task, child = _complete(original, response_format, precompleted=precompleted)

    assert child.state is TaskState.SUCCEEDED and task.state is TaskState.SUCCEEDED
    assert task.result is original and task.result.value is None
    assert task.result.messages[0].contents[0].message == DIAGNOSTIC


def test_native_failed_child_preserves_exception_identity(
    monkeypatch: pytest.MonkeyPatch, precompleted: bool, response_format: type[BaseModel] | None
) -> None:
    load, ensure = _forbid_parsing(monkeypatch)
    error = OSError(DIAGNOSTIC)

    task, child = _complete(error, response_format, precompleted=precompleted, is_error=True)

    assert child.state is TaskState.FAILED and task.state is TaskState.FAILED
    assert task.result is error
    load.assert_not_called()
    ensure.assert_not_called()


@pytest.mark.parametrize("boundary", ["input", "read", "commit"])
def test_real_entity_wrapper_failure_reaches_public_proxy_with_original_diagnostic(
    boundary: str,
    monkeypatch: pytest.MonkeyPatch,
    precompleted: bool,
    response_format: type[BaseModel] | None,
) -> None:
    load, ensure = _forbid_parsing(monkeypatch)
    agent = Mock()
    agent.name = AGENT_NAME

    def run(*, stream: bool = False, **kwargs: Any) -> Any:
        if stream:
            raise TypeError("stream unsupported by local test agent")

        async def complete() -> AgentResponse:
            return AgentResponse(messages=[Message("assistant", ['{"answer": 42}'])])

        return complete()

    agent.run.side_effect = run
    handler = create_agent_entity(agent)
    entity_context = Mock(spec=df.DurableEntityContext)
    entity_context.entity_name = f"dafx-{AGENT_NAME}"
    entity_context.entity_key = SESSION_ID
    entity_context.operation_name = "run"
    persisted = {"schemaVersion": "1.1.0", "data": {"conversationHistory": []}}
    entity_context.get_state.side_effect = lambda *args: deepcopy(persisted)
    failing_method = {"input": "get_input", "read": "get_state", "commit": "set_state"}[boundary]
    getattr(entity_context, failing_method).side_effect = OSError(DIAGNOSTIC)
    child = AtomicTask(7, NoOpAction())
    context = Mock(spec=df.DurableOrchestrationContext)
    context.instance_id = "operation-orchestration"
    context.new_uuid.side_effect = [SESSION_ID, CORRELATION_ID]
    returned: list[dict[str, Any]] = []

    def finish(entity_id: df.EntityId, operation: str, request: dict[str, Any]) -> None:
        assert entity_id.name == entity_context.entity_name and entity_id.key == SESSION_ID
        assert operation == "run" and request["correlationId"] == CORRELATION_ID
        assert request["orchestrationId"] == "operation-orchestration"
        entity_context.get_input.return_value = request
        handler(entity_context)
        entity_context.set_result.assert_called_once()
        result = entity_context.set_result.call_args.args[0]
        returned.append(result)
        child.set_value(is_error=False, value=result)

    def call_entity(entity_id: df.EntityId, operation: str, request: dict[str, Any]) -> AtomicTask:
        if precompleted:
            finish(entity_id, operation, request)
        return child

    context.call_entity.side_effect = call_entity
    app = AgentFunctionApp(agents=[agent], enable_health_check=False, enable_http_endpoints=False)
    proxy = app.get_agent(context, AGENT_NAME)

    task = proxy.run("question", session=proxy.create_session(), options={"response_format": response_format})

    context.call_entity.assert_called_once()
    context.signal_entity.assert_not_called()
    if not precompleted:
        assert task.state is TaskState.RUNNING and returned == []
        finish(*context.call_entity.call_args.args)
    assert returned == [{"status": "error", "error": DIAGNOSTIC}]
    _assert_operation_failure(task, child, DIAGNOSTIC)
    load.assert_not_called()
    ensure.assert_not_called()
    if boundary == "commit":
        assert entity_context.set_state.call_count >= 1
        for call in entity_context.set_state.call_args_list:
            attempted = call.args[0]
            assert attempted["schemaVersion"] == "1.1.0"
            assert "terminalResults" not in attempted["data"] and "completionReceipts" not in attempted["data"]
    else:
        entity_context.set_state.assert_not_called()
        agent.run.assert_not_called()
    assert DurableAgentState().schema_version == "1.1.0"
