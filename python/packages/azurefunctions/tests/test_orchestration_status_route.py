# Copyright (c) Microsoft. All rights reserved.

"""Unit tests for the scoped orchestration status route and its canonical response builder."""

# pyright: reportPrivateUsage=false

import json
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any, TypeVar
from unittest.mock import AsyncMock, Mock, patch

import azure.functions as func
import pytest
from azure.durable_functions.internal.serialization import DEFAULT_FUNCTIONS_DATA_CONVERTER
from durabletask.client import OrchestrationState, OrchestrationStatus
from durabletask.task import FailureDetails

from agent_framework_azurefunctions import AgentFunctionApp, build_orchestration_status_response

FuncT = TypeVar("FuncT", bound=Callable[..., Any])

ORCHESTRATION_NAME = "sample_orchestration"


def _state(
    instance_id: str = "instance-1",
    runtime_status: OrchestrationStatus = OrchestrationStatus.COMPLETED,
    *,
    name: str = ORCHESTRATION_NAME,
    output: Any = None,
    custom_status: Any = None,
    serialized_input: str | None = None,
    serialized_output: str | None = None,
    failure_details: FailureDetails | None = None,
) -> OrchestrationState:
    """A real OrchestrationState as the Functions client returns it (naive UTC timestamps)."""
    moment = datetime(2026, 1, 1)
    return OrchestrationState(
        instance_id=instance_id,
        name=name,
        runtime_status=runtime_status,
        created_at=moment,
        last_updated_at=moment,
        serialized_input=serialized_input,
        serialized_output=serialized_output if output is None else json.dumps(output),
        serialized_custom_status=None if custom_status is None else json.dumps(custom_status),
        failure_details=failure_details,
        _data_converter=DEFAULT_FUNCTIONS_DATA_CONVERTER,
    )


class TestBuildOrchestrationStatusResponse:
    """The canonical response shape shared by every orchestration status endpoint."""

    def test_canonical_shape(self) -> None:
        body = build_orchestration_status_response(_state(output={"answer": 42}, custom_status={"step": 2}))
        assert body == {
            "instanceId": "instance-1",
            "runtimeStatus": "Completed",
            "customStatus": {"step": 2},
            "output": {"answer": 42},
            "error": None,
            "createdTime": "2026-01-01T00:00:00+00:00",
            "lastUpdatedTime": "2026-01-01T00:00:00+00:00",
        }

    def test_input_is_never_echoed(self) -> None:
        body = build_orchestration_status_response(_state(serialized_input=json.dumps({"secret": "payload"})))
        assert "input" not in body
        assert "secret" not in json.dumps(body)

    def test_failed_reports_the_message_without_the_stack(self) -> None:
        state = _state(
            runtime_status=OrchestrationStatus.FAILED,
            failure_details=FailureDetails("boom", "ValueError", "Traceback (secret frames)"),
        )
        body = build_orchestration_status_response(state)
        assert body["runtimeStatus"] == "Failed"
        assert body["error"] == "boom"
        assert "secret frames" not in json.dumps(body)

    def test_non_json_output_stays_a_string(self) -> None:
        state = _state(runtime_status=OrchestrationStatus.TERMINATED, serialized_output="stopped by operator")
        assert build_orchestration_status_response(state)["output"] == "stopped by operator"

    def test_marker_shaped_output_stays_plain_data(self) -> None:
        marker = {"__class__": "Secret", "__module__": "os", "value": 1}
        assert build_orchestration_status_response(_state(output=marker))["output"] == marker

    def test_none_runtime_status_renders_as_none(self) -> None:
        state = _state(runtime_status=None)  # type: ignore[arg-type] # pyrefly: ignore[bad-argument-type] # ty: ignore[invalid-argument-type]
        assert build_orchestration_status_response(state)["runtimeStatus"] is None


def _get_status_handler(
    orchestration_name: str = ORCHESTRATION_NAME, *, route: str | None = None
) -> Callable[[func.HttpRequest, Any], Awaitable[func.HttpResponse]]:
    captured: dict[str | None, Callable[..., Awaitable[func.HttpResponse]]] = {}

    def capture_decorator(*args: Any, **kwargs: Any) -> Callable[[FuncT], FuncT]:
        def decorator(fn: FuncT) -> FuncT:
            return fn

        return decorator

    def capture_route(*args: Any, **kwargs: Any) -> Callable[[FuncT], FuncT]:
        def decorator(fn: FuncT) -> FuncT:
            captured[kwargs.get("route")] = fn
            return fn

        return decorator

    app = AgentFunctionApp(enable_health_check=False)
    with (
        patch.object(AgentFunctionApp, "function_name", new=capture_decorator),
        patch.object(AgentFunctionApp, "route", new=capture_route),
        patch.object(AgentFunctionApp, "durable_client_input", new=capture_decorator),
    ):
        app.add_orchestration_status_route(orchestration_name, route=route)

    expected_route = route if route is not None else f"orchestration/{orchestration_name}/status/{{instanceId}}"
    return captured[expected_route]


def _request(instance_id: str | None = "instance-1") -> Mock:
    request = Mock()
    request.route_params = {"instanceId": instance_id} if instance_id is not None else {}
    request.url = f"http://localhost:7071/api/orchestration/{ORCHESTRATION_NAME}/status/instance-1"
    return request


class TestOrchestrationStatusRoute:
    """The convenience route must scope lookups to its own orchestration name."""

    async def test_own_instance_returns_the_canonical_shape(self) -> None:
        handler = _get_status_handler()
        client = AsyncMock()
        state = _state(output="done", serialized_input=json.dumps({"secret": "payload"}))
        client.get_orchestration_state.return_value = state

        response = await handler(_request(), client)

        assert response.status_code == 200
        assert json.loads(response.get_body()) == build_orchestration_status_response(state)

    async def test_foreign_orchestration_is_not_found(self) -> None:
        handler = _get_status_handler()
        client = AsyncMock()
        client.get_orchestration_state.return_value = _state(name="another_orchestration", output="secret")

        response = await handler(_request(), client)

        assert response.status_code == 404
        assert "secret" not in response.get_body().decode("utf-8")

    async def test_missing_instance_is_not_found(self) -> None:
        handler = _get_status_handler()
        client = AsyncMock()
        client.get_orchestration_state.return_value = None

        assert (await handler(_request(), client)).status_code == 404

    async def test_name_match_is_case_insensitive(self) -> None:
        handler = _get_status_handler()
        client = AsyncMock()
        client.get_orchestration_state.return_value = _state(name=ORCHESTRATION_NAME.upper())

        assert (await handler(_request(), client)).status_code == 200

    async def test_missing_instance_id_is_a_bad_request(self) -> None:
        handler = _get_status_handler()
        client = AsyncMock()

        assert (await handler(_request(instance_id=None), client)).status_code == 400
        client.get_orchestration_state.assert_not_called()

    async def test_custom_route_is_registered(self) -> None:
        handler = _get_status_handler(route="singleagent/status/{instanceId}")
        client = AsyncMock()
        client.get_orchestration_state.return_value = _state()

        assert (await handler(_request(), client)).status_code == 200

    def test_blank_name_is_rejected(self) -> None:
        app = AgentFunctionApp(enable_health_check=False)
        with pytest.raises(ValueError, match="nonblank"):
            app.add_orchestration_status_route("   ")

    def test_route_without_instance_id_is_rejected(self) -> None:
        app = AgentFunctionApp(enable_health_check=False)
        with pytest.raises(ValueError, match="instanceId"):
            app.add_orchestration_status_route(ORCHESTRATION_NAME, route="status/fixed")

    def test_duplicate_registration_is_rejected(self) -> None:
        app = AgentFunctionApp(enable_health_check=False)

        def capture_decorator(*args: Any, **kwargs: Any) -> Callable[[FuncT], FuncT]:
            def decorator(fn: FuncT) -> FuncT:
                return fn

            return decorator

        with (
            patch.object(AgentFunctionApp, "function_name", new=capture_decorator),
            patch.object(AgentFunctionApp, "route", new=capture_decorator),
            patch.object(AgentFunctionApp, "durable_client_input", new=capture_decorator),
        ):
            app.add_orchestration_status_route(ORCHESTRATION_NAME)

        with pytest.raises(ValueError, match="already registered"):
            app.add_orchestration_status_route(ORCHESTRATION_NAME.upper())
