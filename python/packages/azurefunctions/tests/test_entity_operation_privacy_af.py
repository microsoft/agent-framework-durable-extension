# Copyright (c) Microsoft. All rights reserved.

"""Agent entity operations keep validation input out of the failures durabletask reports.

durabletask logs a failed operation and returns its message, stack trace and inner
errors to the caller. Pydantic validation messages can quote request or state values,
so the Functions host replaces them with an input-free diagnostic. These tests fail
each operation and run phase through the handler the Functions host invokes.
"""

import asyncio
import json
import logging
from collections.abc import Callable
from typing import Any

import pytest
from _af_handler_test_support import _NOW, _app, _Client, _entity_function, _Host, _NonStreamingAgent
from _validation_test_support import PRIVATE, SAFE, grouped_error, validation_error
from agent_framework_durabletask import RunRequest, _entities
from agent_framework_durabletask._entities import AgentEntity, DurableTaskEntityStateProvider

_ENTITY = "@dafx-privacy@session"
_OPERATIONS = ["run", "reset", "expire_responses", "migrate"]


def _error(kind: str) -> BaseException:
    if kind == "plain":
        return validation_error(wrapped=False)
    if kind == "wrapped":
        return validation_error(wrapped=True)
    if kind == "group":
        return grouped_error(validation_error(wrapped=False))
    if kind == "ordinary":
        return RuntimeError("ordinary failure")
    return grouped_error(RuntimeError("ordinary failure"))


def _raising(error: BaseException, *, is_async: bool = False) -> Callable[..., Any]:
    def fail(*args: Any, **kwargs: Any) -> Any:
        raise error

    async def fail_async(*args: Any, **kwargs: Any) -> Any:
        raise error

    return fail_async if is_async else fail


def _fail_operation(monkeypatch: pytest.MonkeyPatch, operation: str, error: BaseException) -> None:
    monkeypatch.setattr(AgentEntity, operation, _raising(error, is_async=operation == "run"))


def _request() -> dict[str, Any]:
    return RunRequest("go", correlation_id="request", created_at=_NOW).to_dict()


def _host(client: _Client | None = None) -> _Host:
    return _Host(_app(agents=[_NonStreamingAgent(client=client or _Client(None), name="privacy")]))


def _invoke(operation: str, host: _Host | None = None) -> Any:
    wire = json.dumps(_request()) if operation in {"run", "migrate"} else None
    return (host or _host()).entity(_ENTITY, operation, wire)


def _entity_class(host: _Host) -> Any:
    return _entity_function(host.functions, "dafx-privacy").get_user_function().entity_function


def _assert_private_failure(batch: Any, caplog: pytest.LogCaptureFixture) -> None:
    assert len(batch.results) == 1 and batch.results[0].HasField("failure"), batch
    details = batch.results[0].failure.failureDetails
    assert details.errorType.endswith("ValueError")
    assert details.errorMessage == SAFE
    assert not details.HasField("innerFailure")
    # The stack trace and the logged traceback stop at the replacement error.
    assert PRIVATE not in str(batch)
    assert SAFE in caplog.text
    assert PRIVATE not in caplog.text


@pytest.mark.parametrize("kind", ["plain", "wrapped", "group"])
@pytest.mark.parametrize("operation", _OPERATIONS)
def test_operation_validation_failures_omit_input(
    operation: str, kind: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    _fail_operation(monkeypatch, operation, _error(kind))

    _assert_private_failure(_invoke(operation), caplog)


@pytest.mark.parametrize("kind", ["plain", "group"])
@pytest.mark.parametrize(
    ("phase", "model_calls"), [("input", 0), ("read", 0), ("commit", 1), ("result", 1), ("runner", 0)]
)
def test_run_phase_validation_failures_omit_input(
    phase: str, model_calls: int, kind: str, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    error = _error(kind)
    if phase == "input":
        monkeypatch.setattr(RunRequest, "from_dict", _raising(error))
        monkeypatch.setattr(RunRequest, "from_json", _raising(error))
    elif phase == "read":
        monkeypatch.setattr(DurableTaskEntityStateProvider, "_get_state_dict", _raising(error))
    elif phase == "commit":
        monkeypatch.setattr(DurableTaskEntityStateProvider, "_set_state_dict", _raising(error))
    elif phase == "result":
        monkeypatch.setattr(_entities, "serialize_agent_response", _raising(error))
    else:

        def fail_runner(coroutine: Any) -> Any:
            coroutine.close()
            raise error

        monkeypatch.setattr(_entities, "run_agent_coroutine", fail_runner)
    client = _Client(None)

    _assert_private_failure(_invoke("run", _host(client)), caplog)
    # The model call count shows the failure came from the intended phase.
    assert len(client.options) == model_calls


@pytest.mark.parametrize("kind", ["ordinary", "ordinary-group"])
@pytest.mark.parametrize("operation", _OPERATIONS)
def test_operation_ordinary_failures_keep_their_details(
    operation: str, kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    error = _error(kind)
    _fail_operation(monkeypatch, operation, error)
    host = _host()

    batch = _invoke(operation, host)

    assert len(batch.results) == 1 and batch.results[0].HasField("failure"), batch
    details = batch.results[0].failure.failureDetails
    assert details.errorType.endswith(type(error).__name__)
    assert details.errorMessage == str(error)
    # Called directly, the registered operation raises the original exception.
    args = [_request()] if operation in {"run", "migrate"} else []
    with pytest.raises(type(error)) as raised:
        getattr(_entity_class(host)(), operation)(*args)
    assert raised.value is error


def test_every_operation_the_entity_class_defines_is_wrapped() -> None:
    entity_class = _entity_class(_host())
    operations = {name for name, value in vars(entity_class).items() if callable(value) and not name.startswith("_")}

    assert operations == set(_OPERATIONS)
    assert all(hasattr(getattr(entity_class, name), "__wrapped__") for name in operations)


@pytest.mark.parametrize("operation", _OPERATIONS)
def test_operation_cancellation_groups_propagate(operation: str, monkeypatch: pytest.MonkeyPatch) -> None:
    error = grouped_error(asyncio.CancelledError(), validation_error(wrapped=False))
    _fail_operation(monkeypatch, operation, error)

    with pytest.raises(BaseException) as raised:
        _invoke(operation)

    assert raised.value is error
