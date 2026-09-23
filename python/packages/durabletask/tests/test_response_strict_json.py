# Copyright (c) Microsoft. All rights reserved.

"""Strict JSON must precede model validators, not follow their normalization."""

import json
import math
import traceback
from typing import Any
from unittest.mock import Mock

import pytest
from agent_framework import AgentResponse
from durabletask.task import CompletableTask
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializationInfo,
    SerializerFunctionWrapHandler,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_serializer,
    model_validator,
)

from agent_framework_durabletask import ensure_response_format, load_agent_response, serialize_agent_response
from agent_framework_durabletask._executors import DurableAgentTask
from agent_framework_durabletask._shared_response import serialize_terminal_response

PRIVATE_INPUT = "STRICT_JSON_PRIVATE_PAYLOAD"
NONFINITE = [
    pytest.param(float("nan"), id="nan"),
    pytest.param(float("inf"), id="positive-infinity"),
    pytest.param(float("-inf"), id="negative-infinity"),
]


def _assert_finite(value: Any) -> None:
    """An independent sentinel, not the codec's JSON encoder or comparison helper."""
    if isinstance(value, float):
        assert math.isfinite(value), "A model validator received a non-JSON number"
    elif isinstance(value, dict):
        for item in value.values():
            _assert_finite(item)
    elif isinstance(value, list):
        for item in value:
            _assert_finite(item)


@pytest.mark.parametrize("number", NONFINITE)
@pytest.mark.parametrize("shared", [False, True], ids=["inline", "shared"])
def test_alias_only_nonfinite_json_is_rejected_before_validation_or_delivery(number: float, shared: bool) -> None:
    observed: list[Any] = []

    class AliasOnlyExtra(BaseModel):
        # The default serializer can normalize the inserted float to null. This
        # supported configuration must actually emit a non-finite JSON-mode float.
        model_config = ConfigDict(extra="ignore", ser_json_inf_nan="constants")
        answer: int = Field(alias="outputAnswer")

        @model_serializer(mode="wrap")
        def serialize(self, handler: SerializerFunctionWrapHandler, info: SerializationInfo) -> dict[str, Any]:
            payload = handler(self)
            if info.by_alias:
                payload["ignored"] = {"private": PRIVATE_INPUT, "number": number}
            return payload

        @model_validator(mode="before")
        @classmethod
        def observe(cls, incoming: Any, info: ValidationInfo) -> Any:
            if info.mode == "json":
                observed.append(incoming)
            return incoming

    original = AliasOnlyExtra(outputAnswer=7)
    # The pre-existing field guard sees valid JSON. Only the custom alias dump is invalid.
    assert original.model_dump(mode="json", by_alias=False, round_trip=True) == {"answer": 7}
    alias = original.model_dump(mode="json", by_alias=True, round_trip=True)
    assert isinstance(alias["ignored"]["number"], float)
    assert not math.isfinite(alias["ignored"]["number"])
    with pytest.raises(ValueError):
        json.dumps(alias, allow_nan=False)
    response = AgentResponse[Any](value=original)
    persist = Mock()
    try:
        with pytest.raises(ValueError, match="finite numbers") as raised:
            payload = serialize_terminal_response(response) if shared else serialize_agent_response(response)
            persist(payload)
    finally:
        # Observe the real Pydantic before-validator without changing what it accepts.
        for incoming in observed:
            _assert_finite(incoming)
    assert observed == []
    persist.assert_not_called()
    assert PRIVATE_INPUT not in "".join(traceback.format_exception(raised.value))
    assert response.value is original and original.answer == 7


def test_finite_alias_only_serializer_keeps_its_existing_alias_payload() -> None:
    class AliasOnlyExtra(BaseModel):
        model_config = ConfigDict(extra="ignore")
        answer: int = Field(alias="outputAnswer")

        @model_serializer(mode="wrap")
        def serialize(self, handler: SerializerFunctionWrapHandler, info: SerializationInfo) -> dict[str, Any]:
            payload = handler(self)
            if info.by_alias:
                payload["ignored"] = {"number": 0.0}
            return payload

    original = AliasOnlyExtra(outputAnswer=7)
    payload = serialize_agent_response(AgentResponse[Any](value=original))
    assert payload["value"] == {"outputAnswer": 7, "ignored": {"number": 0.0}}
    assert "_durable_value_by_name" not in payload
    loaded = load_agent_response(json.loads(json.dumps(payload, allow_nan=False)))
    ensure_response_format(AliasOnlyExtra, "strict-json", loaded)
    assert loaded.value == original


@pytest.mark.parametrize("number", NONFINITE)
def test_configured_nonfinite_field_already_rejects_before_model_validation(number: float) -> None:
    observed: list[Any] = []

    class NonfiniteValue(BaseModel):
        model_config = ConfigDict(ser_json_inf_nan="constants")
        answer: float

        @model_validator(mode="before")
        @classmethod
        def observe(cls, incoming: Any, info: ValidationInfo) -> Any:
            if info.mode == "json":
                observed.append(incoming)
            return incoming

    original = NonfiniteValue(answer=number)
    assert not math.isfinite(original.model_dump(mode="json", by_alias=False)["answer"])
    with pytest.raises(ValueError):
        serialize_agent_response(AgentResponse[Any](value=original))
    assert observed == []
    assert not math.isfinite(original.answer)


@pytest.mark.parametrize("number", NONFINITE)
def test_retained_configured_nonfinite_field_is_rejected_before_requested_validation(number: float) -> None:
    observed: list[Any] = []

    class NonfiniteValue(BaseModel):
        model_config = ConfigDict(ser_json_inf_nan="constants")
        answer: float

        @model_validator(mode="before")
        @classmethod
        def observe(cls, incoming: Any, info: ValidationInfo) -> Any:
            if info.mode == "json":
                observed.append(incoming)
            return incoming

    original = NonfiniteValue(answer=number)
    dumped = original.model_dump(mode="json", round_trip=True)
    assert isinstance(dumped["answer"], float) and not math.isfinite(dumped["answer"])
    # Exercise the retained raw Core path, not the matching-model no-op. A strict
    # shared-state reader already rejects this input before typed reconstruction.
    response = load_agent_response({"type": "agent_response", "value": dumped})
    retained = response.value
    assert isinstance(retained, dict) and not math.isfinite(retained["answer"])
    try:
        with pytest.raises(ValueError) as raised:
            ensure_response_format(NonfiniteValue, "strict-json", response)
    finally:
        for incoming in observed:
            _assert_finite(incoming)
    assert type(raised.value) is ValueError
    assert str(raised.value) == "Structured response value must be JSON serializable with finite numbers."
    assert observed == [] and response.value is retained
    assert not math.isfinite(original.answer)


@pytest.mark.parametrize("number", NONFINITE)
@pytest.mark.parametrize("by_name", [False, True], ids=["alias", "field-name"])
@pytest.mark.parametrize("shared", [False, True], ids=["legacy", "shared-policy"])
def test_retained_nonfinite_json_never_reaches_the_requested_validator(
    number: float, by_name: bool, shared: bool
) -> None:
    observed: list[Any] = []

    class RequestedValue(BaseModel):
        model_config = ConfigDict(extra="ignore")
        answer: int = Field(alias="outputAnswer")

        @model_validator(mode="before")
        @classmethod
        def observe(cls, incoming: Any, info: ValidationInfo) -> Any:
            if info.mode == "json":
                observed.append(incoming)
            return incoming

    raw: dict[str, Any] = {
        "type": "agent_response",
        "value": {
            "answer" if by_name else "outputAnswer": 7,
            "ignored": {"private": PRIVATE_INPUT, "number": number},
        },
    }
    if by_name:
        raw["_durable_value_by_name"] = True
    if shared:
        raw["_durable_value_policy"] = {"profile": "agent-framework-python.shared-value", "version": 1}
    response = load_agent_response(raw)
    retained = response.value
    try:
        with pytest.raises(ValueError, match="finite numbers") as raised:
            ensure_response_format(RequestedValue, "strict-json", response)
    finally:
        for incoming in observed:
            _assert_finite(incoming)
    assert observed == []
    assert response.value is retained
    assert PRIVATE_INPUT not in "".join(traceback.format_exception(raised.value))


@pytest.mark.parametrize("kind", ["unsupported-object", "circular-container"])
def test_retained_encoding_errors_have_a_stable_private_diagnostic(kind: str) -> None:
    observed: list[Any] = []

    class RequestedValue(BaseModel):
        answer: int

        @model_validator(mode="before")
        @classmethod
        def observe(cls, incoming: Any) -> Any:
            observed.append(incoming)
            return incoming

    invalid: Any = object()
    if kind == "circular-container":
        invalid = []
        invalid.append(invalid)
    response = AgentResponse[Any](value={"answer": 7, "private": PRIVATE_INPUT, "ignored": invalid})
    retained = response.value
    with pytest.raises(ValueError) as raised:
        ensure_response_format(RequestedValue, "strict-json", response)
    assert str(raised.value) == "Structured response value must be JSON serializable with finite numbers."
    assert PRIVATE_INPUT not in "".join(traceback.format_exception(raised.value))
    assert observed == [] and response.value is retained


@pytest.mark.parametrize("consumer", ["durabletask", "azurefunctions"])
@pytest.mark.parametrize("precompleted", [False, True], ids=["delayed", "precompleted"])
def test_real_tasks_report_encoding_failure_without_private_input(
    consumer: str, precompleted: bool, caplog: pytest.LogCaptureFixture
) -> None:
    observed: list[Any] = []

    class RequestedValue(BaseModel):
        answer: int

        @model_validator(mode="before")
        @classmethod
        def observe(cls, incoming: Any) -> Any:
            observed.append(incoming)
            return incoming

    raw = {
        "type": "agent_response",
        "value": {"answer": 7, "ignored": {"private": PRIVATE_INPUT, "number": float("nan")}},
    }
    if consumer == "durabletask":
        child: CompletableTask[Any] = CompletableTask()
        if precompleted:
            child.complete(raw)
        task = DurableAgentTask(child, RequestedValue, "strict-json")
        if not precompleted:
            assert not task.is_complete
            child.complete(raw)
        assert task.is_complete and task.is_failed
        assert child.get_result() is raw
        failure = task.get_exception()
        assert failure.details.error_type == "ValueError"
        diagnostic = failure.details.message
    else:
        from agent_framework_azurefunctions._orchestration import AgentTask
        from azure.durable_functions.models.actions.NoOpAction import NoOpAction
        from azure.durable_functions.models.Task import AtomicTask, TaskState

        af_child = AtomicTask(7, NoOpAction())
        if precompleted:
            af_child.set_value(is_error=False, value=raw)
        af_task = AgentTask(af_child, RequestedValue, "strict-json")
        if not precompleted:
            assert af_task.state is TaskState.RUNNING
            af_child.set_value(is_error=False, value=raw)
        assert af_task.state is TaskState.FAILED
        assert af_child.result is raw
        assert type(af_task.result) is ValueError
        diagnostic = str(af_task.result)
    assert diagnostic == "Structured response value must be JSON serializable with finite numbers."
    assert PRIVATE_INPUT not in caplog.text
    assert observed == []


@pytest.mark.parametrize("number", NONFINITE)
@pytest.mark.parametrize("fallback", [False, True], ids=["alias-probe", "field-probe"])
def test_validator_produced_nonfinite_fields_cannot_escape_the_lossless_check(number: float, fallback: bool) -> None:
    observed: list[float] = []

    class ChangingValue(BaseModel):
        model_config = ConfigDict(ser_json_inf_nan="constants")
        answer: float = Field(serialization_alias="outputAnswer") if fallback else Field(alias="outputAnswer")

        @field_validator("answer")
        @classmethod
        def change(cls, incoming: float, info: ValidationInfo) -> float:
            if info.mode == "json":
                observed.append(incoming)
                return number
            return incoming

    original = ChangingValue(answer=7.0) if fallback else ChangingValue(outputAnswer=7.0)
    with pytest.raises(ValueError) as raised:
        serialize_agent_response(AgentResponse[Any](value=original))
    assert type(raised.value) is ValueError
    assert str(raised.value) == "Structured response value must be JSON serializable with finite numbers."
    # In particular, an encoding failure after successful alias validation must
    # not be caught as an alias mismatch and trigger another validator invocation.
    assert observed == [7.0]
    assert original.answer == 7.0


@pytest.mark.parametrize("defaulted", [False, True], ids=["validation-error", "lossless-mismatch"])
def test_valid_json_serialization_alias_keeps_checked_field_name_fallback(defaulted: bool) -> None:
    default: Any = 0 if defaulted else ...

    class SeparateAliases(BaseModel):
        answer: int = Field(
            default=default,
            validation_alias="inputAnswer",
            serialization_alias="outputAnswer",
        )

    original = SeparateAliases(inputAnswer=7)
    payload = serialize_agent_response(AgentResponse[Any](value=original))
    assert payload["value"] == {"answer": 7}
    assert payload["_durable_value_by_name"] is True
    loaded = load_agent_response(json.loads(json.dumps(payload, allow_nan=False)))
    ensure_response_format(SeparateAliases, "strict-json", loaded)
    assert loaded.value == original


@pytest.mark.parametrize("by_name", [False, True], ids=["alias", "field-name"])
def test_retained_valid_json_keeps_the_requested_models_validation_error(by_name: bool) -> None:
    observed: list[Any] = []

    class RequestedValue(BaseModel):
        answer: int = Field(alias="outputAnswer")

        @model_validator(mode="before")
        @classmethod
        def observe(cls, incoming: Any, info: ValidationInfo) -> Any:
            if info.mode == "json":
                observed.append(incoming)
            return incoming

    key = "answer" if by_name else "outputAnswer"
    raw: dict[str, Any] = {"type": "agent_response", "value": {key: PRIVATE_INPUT}}
    if by_name:
        raw["_durable_value_by_name"] = True
    response = load_agent_response(json.loads(json.dumps(raw, allow_nan=False)))
    retained = response.value
    # The private encoding diagnostic must not replace Pydantic's established
    # validation contract for finite JSON. General validation redaction is separate.
    with pytest.raises(ValidationError) as raised:
        ensure_response_format(RequestedValue, "strict-json", response)
    errors = raised.value.errors(include_url=False)
    assert len(errors) == 1 and errors[0]["type"] == "int_parsing" and errors[0]["loc"] == (key,)
    assert observed == [{key: PRIVATE_INPUT}]
    assert response.value is retained


@pytest.mark.parametrize("shared", [False, True], ids=["inline", "shared"])
def test_non_idempotent_validation_still_fails_after_the_existing_field_fallback(shared: bool) -> None:
    observed: list[Any] = []

    class NormalizingValue(BaseModel):
        answer: str = Field(alias="outputAnswer")

        @model_validator(mode="before")
        @classmethod
        def observe(cls, incoming: Any, info: ValidationInfo) -> Any:
            if info.mode == "json":
                observed.append(incoming)
            return incoming

        @field_validator("answer")
        @classmethod
        def normalize(cls, incoming: str) -> str:
            if not incoming.startswith("prefix:"):
                raise ValueError("expected prefixed input")
            return incoming.removeprefix("prefix:")

    original = NormalizingValue(outputAnswer="prefix:answer")
    assert original.answer == "answer"
    response = AgentResponse[Any](value=original)
    with pytest.raises(ValidationError, match="expected prefixed input") as raised:
        if shared:
            serialize_terminal_response(response)
        else:
            serialize_agent_response(response)
    errors = raised.value.errors(include_url=False)
    assert len(errors) == 1 and errors[0]["type"] == "value_error" and errors[0]["loc"] == ("answer",)
    assert observed == [{"outputAnswer": "answer"}, {"answer": "answer"}]
    assert response.value is original and original.answer == "answer"
