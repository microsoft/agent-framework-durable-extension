# Copyright (c) Microsoft. All rights reserved.

"""Operation failures at the Functions entity-result and orchestration-task boundary."""

import logging
from collections import UserDict
from collections.abc import AsyncIterable, Sequence
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any
from unittest.mock import Mock

import azure.durable_functions as df
import pytest
from agent_framework import (
    Agent,
    AgentResponse,
    ChatResponse,
    ChatResponseUpdate,
    Content,
    Message,
    ResponseStream,
)
from agent_framework_durabletask import (
    AgentEntity,
    AgentSessionId,
    DurableAgentSession,
    DurableAgentState,
    RunRequest,
    serialize_agent_response,
)
from azure.durable_functions.models.actions.NoOpAction import NoOpAction
from azure.durable_functions.models.Task import AtomicTask, TaskState
from pydantic import BaseModel

from agent_framework_azurefunctions import _entities as af_entities
from agent_framework_azurefunctions import _orchestration as orchestration

CORRELATION_ID = "trusted-operation-correlation"
AGENT_NAME = "operation-diagnostics"
SESSION_KEY = "operation-session"
OPAQUE_MARKER = "opaque-sibling-must-not-be-rendered"
SPOOFED_CORRELATION = "untrusted-payload-correlation"
COMMIT_ERROR = "set_state refused the commit\n  原因: storage unavailable  "
MODEL_ERROR = "model execution failed\n  原因: endpoint unavailable  "


class Answer(BaseModel):
    answer: int


class Opaque:
    """Fail if a diagnostic path renders or copies an unrelated payload value."""

    def __init__(self) -> None:
        self.touches: list[str] = []

    def __str__(self) -> str:
        self.touches.append("str")
        raise AssertionError("Opaque values must not be formatted")

    def __repr__(self) -> str:
        self.touches.append("repr")
        raise AssertionError("Opaque values must not be represented")

    def __deepcopy__(self, memo: dict[int, Any]) -> Any:
        self.touches.append("deepcopy")
        raise AssertionError("Opaque values must not be copied by the consumer")


@pytest.fixture(params=[False, True], ids=["delayed", "precompleted"])
def precompleted(request: pytest.FixtureRequest) -> bool:
    return request.param


@pytest.fixture(params=[None, Answer], ids=["untyped", "typed"])
def response_format(request: pytest.FixtureRequest) -> type[BaseModel] | None:
    return request.param


def _forbid_response_parsing(monkeypatch: pytest.MonkeyPatch) -> tuple[Mock, Mock]:
    load = Mock(side_effect=AssertionError("Operation failures must not enter response parsing"))
    ensure = Mock(side_effect=AssertionError("Operation failures must not enter structured parsing"))
    monkeypatch.setattr(orchestration, "load_agent_response", load)
    monkeypatch.setattr(orchestration, "ensure_response_format", ensure)
    return load, ensure


def _complete(
    payload: Any,
    response_format: type[BaseModel] | None,
    *,
    precompleted: bool,
    is_error: bool = False,
) -> tuple[orchestration.AgentTask, AtomicTask]:
    child = AtomicTask(1, NoOpAction())
    if precompleted:
        child.set_value(is_error=is_error, value=payload)
    task = orchestration.AgentTask(child, response_format, CORRELATION_ID)
    if not precompleted:
        assert child.state is TaskState.RUNNING
        assert task.state is TaskState.RUNNING
        assert not task.is_completed
        child.set_value(is_error=is_error, value=payload)
    assert task.is_completed and child.is_completed
    assert child.result is payload
    return task, child


def _assert_operation_failure(task: orchestration.AgentTask, child: AtomicTask, diagnostic: str) -> None:
    # Native success means the entity wrapper returned, not that agent execution committed.
    assert child.state is TaskState.SUCCEEDED
    assert task.state is TaskState.FAILED
    assert type(task.result) is ValueError
    assert not isinstance(task.result, AgentResponse)
    assert diagnostic in str(task.result)
    assert CORRELATION_ID in str(task.result)
    assert SPOOFED_CORRELATION not in str(task.result)


@pytest.mark.parametrize(
    "diagnostic",
    ["", " \t\r\n  ", "operation rejected", "commit failed\n  原因: café ☃\n  keep trailing spaces  "],
    ids=["empty", "whitespace", "plain", "multiline-unicode"],
)
@pytest.mark.parametrize("unknown_siblings", [False, True], ids=["minimal", "opaque-siblings"])
def test_operation_failure_keeps_exact_diagnostic_and_trusted_correlation_without_parsing(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    precompleted: bool,
    response_format: type[BaseModel] | None,
    diagnostic: str,
    unknown_siblings: bool,
) -> None:
    load, ensure = _forbid_response_parsing(monkeypatch)
    opaque = Opaque()
    raw: dict[str, Any] = {"status": "error", "error": diagnostic}
    if unknown_siblings:
        raw.update({
            "correlationId": SPOOFED_CORRELATION,
            "correlation_id": SPOOFED_CORRELATION,
            "future": {"marker": OPAQUE_MARKER, "value": opaque},
            "response_format": {
                "__response_schema_type__": "pydantic_model",
                "module": "never_import_operation_payload_types",
                "qualname": "NeverConstruct",
            },
        })
    before = deepcopy(raw, {id(opaque): opaque})

    with caplog.at_level(logging.DEBUG, logger="agent_framework.azurefunctions"):
        task, child = _complete(raw, response_format, precompleted=precompleted)

    _assert_operation_failure(task, child, diagnostic)
    load.assert_not_called()
    ensure.assert_not_called()
    assert raw == before
    assert opaque.touches == []
    for marker in (OPAQUE_MARKER, SPOOFED_CORRELATION, "never_import_operation_payload_types"):
        assert marker not in str(task.result)
        assert marker not in caplog.text


@pytest.mark.parametrize("envelope", ["type-and-messages", "messages-only", "type-only"])
def test_core_response_envelopes_with_status_error_extensions_are_not_operation_failures(
    monkeypatch: pytest.MonkeyPatch,
    precompleted: bool,
    response_format: type[BaseModel] | None,
    envelope: str,
) -> None:
    original = AgentResponse[Any](
        messages=[Message("assistant", ['{"answer": 42}'])] if envelope != "type-only" else [],
        value={"answer": 42},
        additional_properties={"status": "error", "error": "legitimate provider metadata", "future": [0, None]},
    )
    raw = serialize_agent_response(original)
    if envelope == "messages-only":
        raw.pop("type")
    elif envelope == "type-only":
        raw.pop("messages", None)
    raw.update({"status": "error", "error": "unknown top-level extension, not an operation failure"})
    before = deepcopy(raw)
    load = Mock(wraps=orchestration.load_agent_response)
    ensure = Mock(wraps=orchestration.ensure_response_format)
    monkeypatch.setattr(orchestration, "load_agent_response", load)
    monkeypatch.setattr(orchestration, "ensure_response_format", ensure)

    task, child = _complete(raw, response_format, precompleted=precompleted)

    assert child.state is TaskState.SUCCEEDED and task.state is TaskState.SUCCEEDED
    assert isinstance(task.result, AgentResponse)
    assert task.result.text == original.text
    assert task.result.additional_properties == original.additional_properties
    if response_format is None:
        assert task.result.value == {"answer": 42}
        ensure.assert_not_called()
    else:
        assert isinstance(task.result.value, Answer) and task.result.value.answer == 42
        ensure.assert_called_once_with(response_format, CORRELATION_ID, task.result)
    load.assert_called_once_with(raw)
    assert raw == before


@pytest.mark.parametrize(
    "envelope",
    [
        pytest.param({"type": None}, id="null-type"),
        pytest.param({"type": ""}, id="empty-type"),
        pytest.param({"type": 0}, id="numeric-type"),
        pytest.param({"type": []}, id="list-type"),
        pytest.param({"messages": None}, id="null-messages"),
        pytest.param({"messages": ""}, id="string-messages"),
        pytest.param({"messages": False}, id="boolean-messages"),
        pytest.param({"messages": {}}, id="mapping-messages"),
        pytest.param({"messages": [None]}, id="null-message-item"),
        pytest.param({"type": "agent_response", "messages": ""}, id="typed-malformed-messages"),
    ],
)
def test_present_malformed_response_fields_keep_strict_normal_parse_failure(
    monkeypatch: pytest.MonkeyPatch,
    precompleted: bool,
    response_format: type[BaseModel] | None,
    envelope: dict[str, Any],
) -> None:
    diagnostic = "must-not-replace-the-normal-envelope-validation-error"
    raw = {"status": "error", "error": diagnostic, **deepcopy(envelope)}
    before = deepcopy(raw)
    load = Mock(wraps=orchestration.load_agent_response)
    ensure = Mock(side_effect=AssertionError("Malformed responses must fail before structured parsing"))
    monkeypatch.setattr(orchestration, "load_agent_response", load)
    monkeypatch.setattr(orchestration, "ensure_response_format", ensure)

    task, child = _complete(raw, response_format, precompleted=precompleted)

    assert child.state is TaskState.SUCCEEDED and task.state is TaskState.FAILED
    assert type(task.result) in (TypeError, ValueError)
    assert not isinstance(task.result, AgentResponse)
    assert diagnostic not in str(task.result)
    load.assert_called_once_with(raw)
    ensure.assert_not_called()
    assert raw == before


@pytest.mark.parametrize("error_kind", ["null", "number", "boolean", "list", "mapping", "opaque"])
def test_nonstring_error_is_not_rendered_or_promoted_to_an_operation_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    precompleted: bool,
    response_format: type[BaseModel] | None,
    error_kind: str,
) -> None:
    opaque = Opaque()
    error_values: dict[str, Any] = {
        "null": None,
        "number": 42,
        "boolean": False,
        "list": [OPAQUE_MARKER, opaque],
        "mapping": {"marker": OPAQUE_MARKER, "value": opaque},
        "opaque": opaque,
    }
    raw = {"status": "error", "error": error_values[error_kind]}
    before = deepcopy(raw, {id(opaque): opaque})
    load = Mock(wraps=orchestration.load_agent_response)
    ensure = Mock(side_effect=AssertionError("Malformed responses must not reach structured parsing"))
    monkeypatch.setattr(orchestration, "load_agent_response", load)
    monkeypatch.setattr(orchestration, "ensure_response_format", ensure)

    with caplog.at_level(logging.DEBUG, logger="agent_framework.azurefunctions"):
        task, child = _complete(raw, response_format, precompleted=precompleted)

    assert child.state is TaskState.SUCCEEDED and task.state is TaskState.FAILED
    assert type(task.result) is ValueError
    assert "requires a response type or messages" in str(task.result)
    load.assert_called_once_with(raw)
    ensure.assert_not_called()
    assert raw == before and opaque.touches == []
    assert OPAQUE_MARKER not in str(task.result) and OPAQUE_MARKER not in caplog.text


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({"status": "ERROR", "error": "not-an-operation-diagnostic"}, id="uppercase-status"),
        pytest.param({"status": " error ", "error": "not-an-operation-diagnostic"}, id="padded-status"),
        pytest.param({"status": None, "error": "not-an-operation-diagnostic"}, id="null-status"),
        pytest.param({"error": "not-an-operation-diagnostic"}, id="missing-status"),
        pytest.param({"status": "error"}, id="missing-error"),
        pytest.param(UserDict({"status": "error", "error": "not-an-operation-diagnostic"}), id="not-a-dict"),
    ],
)
def test_only_an_actual_dictionary_with_exact_error_status_uses_operation_failure_path(
    monkeypatch: pytest.MonkeyPatch,
    precompleted: bool,
    response_format: type[BaseModel] | None,
    payload: Any,
) -> None:
    raw = deepcopy(payload)
    before = deepcopy(raw)
    load = Mock(wraps=orchestration.load_agent_response)
    monkeypatch.setattr(orchestration, "load_agent_response", load)

    task, child = _complete(raw, response_format, precompleted=precompleted)

    assert child.state is TaskState.SUCCEEDED and task.state is TaskState.FAILED
    assert type(task.result) in (TypeError, ValueError)
    assert "not-an-operation-diagnostic" not in str(task.result)
    load.assert_called_once_with(raw)
    assert raw == before


def test_native_child_failure_preserves_original_exception_identity(
    monkeypatch: pytest.MonkeyPatch, precompleted: bool, response_format: type[BaseModel] | None
) -> None:
    load, ensure = _forbid_response_parsing(monkeypatch)
    error = OSError("native entity task failure\n原因: transport unavailable")

    task, child = _complete(error, response_format, precompleted=precompleted, is_error=True)

    assert child.state is TaskState.FAILED and task.state is TaskState.FAILED
    assert task.result is error and child.result is error
    load.assert_not_called()
    ensure.assert_not_called()


class Model:
    """Local chat client exercised through the real Core Agent context pipeline."""

    def __init__(self, *, fail: bool = False) -> None:
        self.additional_properties: dict[str, Any] = {}
        self.calls: list[list[Message]] = []
        self.fail = fail

    def get_response(self, messages: Any, *, stream: bool = False, **kwargs: Any) -> Any:
        self.calls.append(list(messages))
        text = '{"answer": 42}'

        async def complete() -> ChatResponse:
            if self.fail:
                raise OSError(MODEL_ERROR)
            return ChatResponse(messages=[Message("assistant", [text])])

        async def updates() -> AsyncIterable[ChatResponseUpdate]:
            if self.fail:
                raise OSError(MODEL_ERROR)
            yield ChatResponseUpdate(role="assistant", contents=[Content.from_text(text)])

        def finalize(items: Sequence[ChatResponseUpdate]) -> ChatResponse:
            return ChatResponse.from_updates(items)

        return ResponseStream(updates(), finalizer=finalize) if stream else complete()


class Host:
    def __init__(self, monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
        self.raw = DurableAgentState().to_dict()
        if mode == "legacy":
            self.raw = {"schemaVersion": "1.1.0", "data": {"conversationHistory": []}}
        self.raw["futureRoot"] = {"keep": ["雪", None]}
        self.raw["data"]["futureData"] = {"keep": [False, 0]}
        self.raw["data"]["session"] = {
            "session_id": f"@dafx-{AGENT_NAME}@{SESSION_KEY}",
            "state": {"application": {"keep": [1, 3]}},
        }
        self.mode = mode
        self.model = Model(fail=mode == "runtime-error")
        self.entities: list[AgentEntity] = []
        self.contexts: list[Mock] = []
        self.attempts: list[dict[str, Any]] = []

        def capture_entity(*args: Any, **kwargs: Any) -> AgentEntity:
            entity = AgentEntity(*args, **kwargs)
            self.entities.append(entity)
            return entity

        # Capture the actual public entity without replacing dispatch, state caching or run().
        monkeypatch.setattr(af_entities, "AgentEntity", capture_entity)
        client: Any = self.model
        self.handler = af_entities.create_agent_entity(
            Agent(client=client, name=AGENT_NAME),
            deployment_mode="isolated_v2",
            max_state_bytes=64 if mode == "capacity" else None,
        )

    def invoke(self, entity_id: df.EntityId, operation: str, payload: dict[str, Any]) -> dict[str, Any]:
        context = Mock(spec=df.DurableEntityContext)
        context.entity_name = entity_id.name
        context.entity_key = entity_id.key
        context.operation_name = operation
        context.get_input.return_value = payload
        context.get_state.side_effect = lambda *args, **kwargs: deepcopy(self.raw)

        def write(raw: dict[str, Any]) -> None:
            self.attempts.append(deepcopy(raw))
            if self.mode == "commit":
                raise OSError(COMMIT_ERROR)
            self.raw = deepcopy(raw)

        context.set_state.side_effect = write
        self.contexts.append(context)
        self.handler(context)
        context.get_input.assert_called_once_with()
        context.get_state.assert_called_once()
        context.set_result.assert_called_once()
        return context.set_result.call_args.args[0]


def _request(response_format: type[BaseModel] | None, *, wait: bool = True) -> RunRequest:
    return RunRequest(
        message="produce an answer",
        correlation_id=CORRELATION_ID,
        response_format=response_format,
        wait_for_response=wait,
        created_at=datetime(2040, 1, 1, tzinfo=timezone.utc),
        orchestration_id="diagnostic-orchestration",
        options={"temperature": 0},
    )


def _session() -> DurableAgentSession:
    return DurableAgentSession(durable_session_id=AgentSessionId(name=AGENT_NAME, key=SESSION_KEY))


def _execute_through_functions(
    host: Host, response_format: type[BaseModel] | None, *, precompleted: bool
) -> tuple[orchestration.AgentTask, AtomicTask, dict[str, Any]]:
    request = _request(response_format)
    expected_request = deepcopy(request.to_dict())
    child = AtomicTask(7, NoOpAction())
    context = Mock(spec=df.DurableOrchestrationContext)
    returned: list[dict[str, Any]] = []
    snapshots: list[dict[str, Any]] = []

    def finish(entity_id: df.EntityId, operation: str, payload: dict[str, Any]) -> None:
        assert entity_id.name == f"dafx-{AGENT_NAME}" and entity_id.key == SESSION_KEY
        assert operation == "run" and payload == expected_request
        raw = host.invoke(entity_id, operation, payload)
        returned.append(raw)
        snapshots.append(deepcopy(raw))
        child.set_value(is_error=False, value=raw)

    def call_entity(entity_id: df.EntityId, operation: str, payload: dict[str, Any]) -> AtomicTask:
        if precompleted:
            finish(entity_id, operation, payload)
        return child

    context.call_entity.side_effect = call_entity
    task = orchestration.AzureFunctionsAgentExecutor(context).run_durable_agent(AGENT_NAME, request, session=_session())
    context.call_entity.assert_called_once()
    context.signal_entity.assert_not_called()
    if not precompleted:
        assert task.state is TaskState.RUNNING and child.state is TaskState.RUNNING
        assert host.model.calls == [] and host.attempts == [] and host.contexts == []
        finish(*context.call_entity.call_args.args)
    assert task.is_completed and child.is_completed
    assert child.result is returned[0]
    assert returned == snapshots
    assert request.to_dict() == expected_request
    assert context.call_entity.call_args.args[2] == expected_request
    assert host.contexts[0].get_input.return_value is context.call_entity.call_args.args[2]
    assert len(host.entities) == 1 and len(host.contexts) == 1
    return task, child, returned[0]


@pytest.mark.parametrize(
    ("mode", "diagnostic_fragment", "model_calls", "write_attempts"),
    [
        ("legacy", "read-only", 0, 0),
        ("capacity", "budget is 64 bytes", 1, 0),
        ("commit", COMMIT_ERROR, 1, 1),
    ],
)
def test_real_functions_operation_rejection_reaches_failed_task_without_committed_receipts(
    monkeypatch: pytest.MonkeyPatch,
    precompleted: bool,
    response_format: type[BaseModel] | None,
    mode: str,
    diagnostic_fragment: str,
    model_calls: int,
    write_attempts: int,
) -> None:
    load, ensure = _forbid_response_parsing(monkeypatch)
    host = Host(monkeypatch, mode)
    before = deepcopy(host.raw)

    task, child, raw = _execute_through_functions(host, response_format, precompleted=precompleted)

    assert set(raw) == {"status", "error"} and raw["status"] == "error"
    assert isinstance(raw["error"], str) and diagnostic_fragment in raw["error"]
    _assert_operation_failure(task, child, raw["error"])
    load.assert_not_called()
    ensure.assert_not_called()
    assert host.raw == before
    assert host.entities[0].state.to_dict() == before
    assert len(host.model.calls) == model_calls
    assert len(host.attempts) == write_attempts
    assert host.contexts[0].set_state.call_count == write_attempts
    for snapshot in (host.raw, host.entities[0].state.to_dict()):
        assert CORRELATION_ID not in snapshot["data"].get("completionReceipts", {})
        assert CORRELATION_ID not in snapshot["data"].get("terminalResults", {})
    if mode == "commit":
        assert raw["error"] == COMMIT_ERROR
        # The failed write attempted a genuine completed response, but did not persist it.
        assert host.attempts[0]["data"]["completionReceipts"][CORRELATION_ID]["outcome"] == "succeeded"
        assert CORRELATION_ID in host.attempts[0]["data"]["terminalResults"]


def test_real_committed_agent_runtime_error_remains_a_successful_task_with_semantic_error(
    monkeypatch: pytest.MonkeyPatch, precompleted: bool, response_format: type[BaseModel] | None
) -> None:
    host = Host(monkeypatch, "runtime-error")
    load = Mock(wraps=orchestration.load_agent_response)
    ensure = Mock(wraps=orchestration.ensure_response_format)
    monkeypatch.setattr(orchestration, "load_agent_response", load)
    monkeypatch.setattr(orchestration, "ensure_response_format", ensure)

    task, child, raw = _execute_through_functions(host, response_format, precompleted=precompleted)

    assert child.state is TaskState.SUCCEEDED and task.state is TaskState.SUCCEEDED
    assert isinstance(task.result, AgentResponse)
    assert raw["type"] == "agent_response" and raw["messages"]
    assert task.result.additional_properties["durable_status"] == "error"
    assert task.result.additional_properties["correlation_id"] == CORRELATION_ID
    assert MODEL_ERROR in task.result.text
    errors = [content for message in task.result.messages for content in message.contents if content.type == "error"]
    assert len(errors) == 1 and errors[0].error_code == "OSError" and errors[0].message == MODEL_ERROR
    assert not isinstance(task.result.value, Answer)
    assert len(host.model.calls) == 1 and len(host.attempts) == 1
    host.contexts[0].set_state.assert_called_once()
    assert host.entities[0].state.to_dict() == host.raw
    receipt = host.raw["data"]["completionReceipts"][CORRELATION_ID]
    terminal = host.raw["data"]["terminalResults"][CORRELATION_ID]
    assert receipt["outcome"] == terminal["outcome"] == "failed"
    assert receipt["resultState"] == "available"
    retained = DurableAgentState.from_dict(host.raw).try_get_agent_response(CORRELATION_ID)
    assert retained is not None and retained.additional_properties["durable_status"] == "error"
    assert MODEL_ERROR in retained.text
    load.assert_called_once_with(raw)
    if response_format is None:
        ensure.assert_not_called()
    else:
        ensure.assert_called_once_with(response_format, CORRELATION_ID, task.result)


def test_fire_and_forget_acceptance_does_not_claim_an_entity_execution_or_commit(
    monkeypatch: pytest.MonkeyPatch, response_format: type[BaseModel] | None
) -> None:
    host = Host(monkeypatch, "commit")
    before = deepcopy(host.raw)
    context = Mock(spec=df.DurableOrchestrationContext)
    request = _request(response_format, wait=False)
    expected = deepcopy(request.to_dict())

    task = orchestration.AzureFunctionsAgentExecutor(context).run_durable_agent(AGENT_NAME, request, session=_session())

    assert task.is_completed and task.state is TaskState.SUCCEEDED
    assert isinstance(task.result, AgentResponse)
    assert task.result.additional_properties == {"durable_status": "accepted", "correlation_id": CORRELATION_ID}
    assert "accepted" in task.result.text and CORRELATION_ID in task.result.text
    assert not isinstance(task.result.value, Answer)
    context.call_entity.assert_not_called()
    context.signal_entity.assert_called_once()
    entity_id, operation, payload = context.signal_entity.call_args.args
    assert entity_id.name == f"dafx-{AGENT_NAME}" and entity_id.key == SESSION_KEY
    assert operation == "run" and payload == expected and request.to_dict() == expected
    assert host.raw == before
    assert host.model.calls == [] and host.attempts == [] and host.entities == [] and host.contexts == []
    assert CORRELATION_ID not in host.raw["data"].get("completionReceipts", {})
    assert CORRELATION_ID not in host.raw["data"].get("terminalResults", {})
