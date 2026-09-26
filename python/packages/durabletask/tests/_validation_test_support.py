# Copyright (c) Microsoft. All rights reserved.

"""Shared models, failures, and assertions for validation privacy tests."""

from __future__ import annotations

import builtins
import json
import logging
from typing import Any

import pytest
from agent_framework import AgentResponse
from agent_framework.exceptions import ChatClientException
from pydantic import BaseModel, ValidationError, field_validator

from agent_framework_durabletask._response_utils import serialize_agent_response

PRIVATE = "SYNTHETIC_PRIVATE_SERIALIZATION_INPUT_4926"
SAFE = "Validation failed with 1 error(s). Input details omitted."


class Answer(BaseModel):
    answer: int


class NormalizingAnswer(BaseModel):
    answer: str

    @field_validator("answer")
    @classmethod
    def normalize(cls, value: str) -> str:
        if not value.startswith("prefix:"):
            raise ValueError(f"expected prefixed input, received {value}")
        return value.removeprefix("prefix:")


class StructuredAgent:
    name = "privacy"

    def __init__(self) -> None:
        self.calls = 0

    async def run(self, **kwargs: Any) -> AgentResponse[Any]:
        self.calls += 1
        return AgentResponse(value=NormalizingAnswer(answer=f"prefix:{PRIVATE}"))


def validation_error(*, wrapped: bool) -> Exception:
    try:
        Answer.model_validate({"answer": PRIVATE})
    except ValidationError as exc:
        if not wrapped:
            return exc
        try:
            raise ChatClientException(f"SDK failed: {exc}", log_level=None) from exc
        except ChatClientException as outer:
            return outer
    raise AssertionError("Expected a real validation failure")


def grouped_error(*children: BaseException) -> BaseException:
    group_type = getattr(builtins, "BaseExceptionGroup", None)
    if group_type is None:
        pytest.skip("Built-in exception groups require Python 3.11+")
    # The child validation failure was captured before entering this helper.
    # Raising outside its except block leaves only group membership as evidence.
    try:
        raise group_type("parallel operation failed", children)
    except BaseException as group:
        assert group.__cause__ is None and group.__context__ is None
        return group


class GroupedValidationAgent(StructuredAgent):
    def __init__(self) -> None:
        super().__init__()
        self.error = grouped_error(validation_error(wrapped=False))

    async def run(self, **kwargs: Any) -> AgentResponse[Any]:
        self.calls += 1
        raise self.error


def sdk_response(text: str = "ok") -> dict[str, Any]:
    return {
        "id": "resp_next",
        "object": "response",
        "created_at": 1760000000,
        "status": "completed",
        "model": "offline-model",
        "output": [
            {
                "id": "msg_one",
                "type": "message",
                "role": "assistant",
                "status": "completed",
                "content": [{"type": "output_text", "text": text, "annotations": []}],
            }
        ],
        "parallel_tool_calls": True,
        "tool_choice": "auto",
        "tools": [],
        "error": None,
        "incomplete_details": None,
        "instructions": None,
        "metadata": {},
        "temperature": 1,
        "top_p": 1,
    }


def assert_private_failure(response: AgentResponse[Any], *, code: str = "ValidationError") -> dict[str, Any]:
    wire = serialize_agent_response(response)
    assert response.additional_properties["durable_status"] == "error"
    assert response.value is None
    assert response.text == SAFE
    errors = [c for m in response.messages for c in m.contents if c.type == "error"]
    assert len(errors) == 1 and errors[0].error_code == code and errors[0].message == SAFE
    assert PRIVATE not in json.dumps(wire)
    return wire


def assert_private_logs(caplog: pytest.LogCaptureFixture) -> None:
    records = [
        record
        for record in caplog.records
        if record.name in {"agent_framework.durabletask", "agent_framework.azurefunctions"}
        and record.levelno >= logging.WARNING
    ]
    assert records
    assert all(record.exc_info is None for record in records)
    assert PRIVATE not in caplog.text
