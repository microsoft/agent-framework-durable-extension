# Copyright (c) Microsoft. All rights reserved.

"""Published checkpoint semantics: durable-response keys remain application data."""

from __future__ import annotations

from typing import Any, cast

from agent_framework import WorkflowEvent
from agent_framework._workflows._checkpoint_encoding import decode_checkpoint_value, encode_checkpoint_value
from agent_framework._workflows._events import WorkflowEventType

from .serialization import resolve_type


def serialize_value(value: Any) -> Any:
    return encode_checkpoint_value(value)


def deserialize_value(value: Any) -> Any:
    return decode_checkpoint_value(value)


def _type_key(value: type[Any] | None) -> str | None:
    return f"{value.__module__}:{value.__name__}" if value is not None else None


def serialize_workflow_event(event: WorkflowEvent[Any]) -> dict[str, Any]:
    """Retain the predecessor's event data and simple declared-type descriptors."""
    encoded: dict[str, Any] = {"type": event.type}
    if event.executor_id is not None:
        encoded["executor_id"] = event.executor_id
    if event.data is not None:
        encoded["data"] = serialize_value(event.data)
    if event.type == "request_info":
        encoded["request_id"] = event.request_id
        encoded["source_executor_id"] = event.source_executor_id
        encoded["response_type"] = _type_key(event.response_type)
    return encoded


def deserialize_workflow_event(encoded: dict[str, Any]) -> WorkflowEvent[Any]:
    """Decode only trusted persisted predecessor events, never external input."""
    kind = cast(WorkflowEventType, encoded["type"])
    data = deserialize_value(encoded["data"]) if "data" in encoded else None
    if kind == "request_info":
        key = encoded.get("response_type")
        event = WorkflowEvent.request_info(
            request_id=encoded["request_id"],
            source_executor_id=encoded["source_executor_id"],
            request_data=data,
            response_type=(resolve_type(key) if key else None) or object,
        )
    else:
        event = WorkflowEvent(kind, data=data, executor_id=encoded.get("executor_id"))
    if encoded.get("iteration") is not None:
        event.iteration = encoded["iteration"]
    return event
