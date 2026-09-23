# Copyright (c) Microsoft. All rights reserved.

"""Functions must not publish Pydantic input through operation error envelopes."""

from __future__ import annotations

import asyncio
import json
import logging
from copy import deepcopy
from typing import Any, cast

import pytest
from agent_framework_durabletask import serialize_agent_response
from azure.durable_functions.models.actions.NoOpAction import NoOpAction
from azure.durable_functions.models.Task import AtomicTask
from test_runtime_validation_privacy import (
    PRIVATE,
    SAFE,
    GroupedValidationAgent,
    StructuredAgent,
    assert_private_failure,
    assert_private_logs,
    grouped_error,
    validation_error,
)

from agent_framework_azurefunctions import _entities as entities
from agent_framework_azurefunctions._orchestration import AgentTask


class PrivacyContext:
    entity_name = "dafx-privacy"
    entity_key = "session"
    operation_name = "run"

    def __init__(
        self, raw: dict[str, Any] | None = None, *, failure: str = "", error: BaseException | None = None
    ) -> None:
        self.raw = deepcopy(raw or {})
        self.failure = failure
        self.error = error
        self.result: Any = None
        self.writes = 0

    def fail(self, phase: str) -> None:
        if self.failure == phase:
            assert self.error is not None
            raise self.error

    def get_state(self, default: Any) -> dict[str, Any]:
        self.fail("read")
        return deepcopy(self.raw)

    def set_state(self, value: dict[str, Any]) -> None:
        self.fail("commit")
        self.raw = json.loads(json.dumps(value, allow_nan=False))
        self.writes += 1

    def get_input(self) -> dict[str, Any]:
        self.fail("input")
        return {"message": "public question", "correlationId": "private"}

    def set_result(self, value: Any) -> None:
        if self.failure == "result":
            self.failure = ""  # The host can still publish the operation failure.
            assert self.error is not None
            raise self.error
        self.result = value


@pytest.mark.parametrize("grouped", [False, True], ids=["serialization", "group"])
def test_functions_post_execution_validation_commits_private_failure_and_duplicate(
    grouped: bool, caplog: pytest.LogCaptureFixture
) -> None:
    context = PrivacyContext()
    agent = GroupedValidationAgent() if grouped else StructuredAgent()
    handler = entities.create_agent_entity(cast(Any, agent), deployment_mode="isolated_v2")
    caplog.set_level(logging.WARNING)
    handler(cast(Any, context))

    child = AtomicTask(7, NoOpAction())
    task = AgentTask(child, None, "private")
    child.set_value(is_error=False, value=context.result)
    wire = assert_private_failure(task.result, code="ExceptionGroup" if grouped else "ValidationError")
    assert wire == context.result
    assert context.writes == agent.calls == 1
    assert PRIVATE not in json.dumps(context.raw)
    cold = PrivacyContext(context.raw)
    handler(cast(Any, cold))
    assert cold.result == serialize_agent_response(task.result)
    assert cold.writes == 0 and agent.calls == 1
    assert_private_logs(caplog)


@pytest.mark.parametrize("phase", ["input", "read", "commit", "result", "runner"])
@pytest.mark.parametrize("kind", ["plain", "wrapped", "group", "ordinary", "ordinary-group"])
def test_host_operation_failure_sanitizes_validation_only(
    phase: str, kind: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    ordinary = kind in {"ordinary", "ordinary-group"}
    error: BaseException = OSError("ordinary host detail") if ordinary else validation_error(wrapped=kind == "wrapped")
    if kind in {"group", "ordinary-group"}:
        error = grouped_error(error)
    context = PrivacyContext(failure=phase, error=error)
    agent = StructuredAgent()
    handler = entities.create_agent_entity(cast(Any, agent), deployment_mode="isolated_v2")
    if phase == "runner":

        def fail_runner(coroutine: Any) -> None:
            coroutine.close()
            raise error

        monkeypatch.setattr(entities, "run_agent_coroutine", fail_runner)
    caplog.set_level(logging.WARNING)
    handler(cast(Any, context))

    diagnostic = str(error) if ordinary else SAFE
    assert context.result == {"status": "error", "error": diagnostic}
    assert PRIVATE not in json.dumps(context.result) + caplog.text
    host_records = [record for record in caplog.records if record.name == "agent_framework.azurefunctions"]
    assert len(host_records) == 1
    assert (host_records[0].exc_info is not None) is ordinary
    if ordinary:
        assert "ordinary host detail" in caplog.text
    if phase in {"input", "read", "runner"}:
        assert agent.calls == context.writes == 0
    elif phase == "commit":
        assert agent.calls == 1 and context.writes == 0 and context.raw == {}
    else:
        assert agent.calls == context.writes == 1

    child = AtomicTask(7, NoOpAction())
    task = AgentTask(child, None, "private")
    child.set_value(is_error=False, value=context.result)
    assert isinstance(task.result, ValueError)
    assert str(task.result) == f"Agent entity operation failed for correlation_id private: {diagnostic}"


@pytest.mark.parametrize("phase", ["input", "read", "commit", "result", "runner"])
def test_host_cancellation_group_is_not_converted_to_an_operation_failure(
    phase: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    error = grouped_error(asyncio.CancelledError("cancelled"), validation_error(wrapped=False))
    assert not isinstance(error, Exception)
    context = PrivacyContext(failure=phase, error=error)
    handler = entities.create_agent_entity(cast(Any, StructuredAgent()), deployment_mode="isolated_v2")
    if phase == "runner":

        def fail_runner(coroutine: Any) -> None:
            coroutine.close()
            raise error

        monkeypatch.setattr(entities, "run_agent_coroutine", fail_runner)
    caplog.set_level(logging.WARNING)
    with pytest.raises(BaseException) as captured:
        handler(cast(Any, context))
    assert captured.value is error
    assert context.result is None
    # A result-publication failure happens after the existing durable commit.
    assert context.writes == int(phase == "result")
    assert PRIVATE not in json.dumps(context.raw) + caplog.text
