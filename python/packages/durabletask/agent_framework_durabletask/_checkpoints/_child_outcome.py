# Copyright (c) Microsoft. All rights reserved.

"""Versioned child terminal values. Authentication belongs before this decoder."""

import json
from dataclasses import asdict
from typing import Any

from durabletask.internal.orchestrator_service_pb2 import TaskFailureDetails
from durabletask.task import TaskFailedError
from google.protobuf.json_format import ParseDict

from ._types import JsonObject, is_json_object

CONTRACT = "native-child-outcome/1"


def capture(state: Any) -> JsonObject:
    """Read an actual backend state without inventing completion/failure facts."""
    if state is None:
        raise ValueError("Child has no authoritative state.")
    status = state.runtime_status.name
    if status == "COMPLETED":
        if state.serialized_output is None:
            raise ValueError("Completed child has no authoritative result payload.")
        return {"contract": CONTRACT, "outcome": status, "result": json.loads(state.serialized_output)}
    if status == "FAILED":
        if state.failure_details is None:
            raise ValueError("Failed child has no authoritative failure details.")
        failure = asdict(state.failure_details)
        failure_protobuf(failure)
        return {"contract": CONTRACT, "outcome": status, "failure": failure}
    raise ValueError("Child has no supported authoritative terminal outcome: " + status)


def failure_protobuf(value: object) -> TaskFailureDetails:
    if not is_json_object(value) or set(value) != {
        "message",
        "error_type",
        "stack_trace",
        "inner_failure",
        "properties",
        "is_non_retriable",
    }:
        raise ValueError("Child failure is missing lossless public failure fields.")
    if (
        not isinstance(value["message"], str)
        or not isinstance(value["error_type"], str)
        or not isinstance(value["is_non_retriable"], bool)
        or (value["stack_trace"] is not None and not isinstance(value["stack_trace"], str))
        or (value["properties"] is not None and not isinstance(value["properties"], dict))
    ):
        raise ValueError("Malformed child failure details.")
    wire: dict[str, Any] = {
        "errorType": value["error_type"],
        "errorMessage": value["message"],
        "isNonRetriable": value["is_non_retriable"],
    }
    if value["stack_trace"] is not None:
        wire["stackTrace"] = value["stack_trace"]
    if value["properties"] is not None:
        wire["properties"] = value["properties"]
    result = ParseDict(wire, TaskFailureDetails())
    if value["inner_failure"] is not None:
        result.innerFailure.CopyFrom(failure_protobuf(value["inner_failure"]))
    return result


def release(value: object, *, source_task_id: int) -> Any:
    """Apply only already authenticated terminal bytes to the matching waiter."""
    if not is_json_object(value) or value.get("contract") != CONTRACT:
        raise ValueError("Unsupported authenticated child outcome contract.")
    outcome = value.get("outcome")
    if outcome == "COMPLETED":
        if "result" not in value or "failure" in value:
            raise ValueError("Malformed successful child outcome.")
        return value["result"]
    if outcome == "FAILED":
        if "failure" not in value or "result" in value:
            raise ValueError("Malformed failed child outcome.")
        details = failure_protobuf(value["failure"])
        raise TaskFailedError(
            f"Sub-orchestration task #{source_task_id} failed: {details.errorMessage}",
            details,
        )
    raise ValueError("Unsupported authenticated child terminal outcome.")
