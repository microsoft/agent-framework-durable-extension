# Copyright (c) Microsoft. All rights reserved.

"""Shared utilities for handling AgentResponse parsing and validation."""

import json
import logging
from collections.abc import Mapping, Sequence
from copy import copy, deepcopy
from functools import lru_cache
from inspect import Parameter, signature
from typing import Any, Literal, cast

from agent_framework import AgentResponse, Content, Message
from pydantic import BaseModel, ValidationError

logger = logging.getLogger("agent_framework.durabletask")

# Optional reader marker; the serializer does not add it to response payloads.
_DELIVERY_VERSION_KEY = "_durable_response_version"
_DELIVERY_VERSION = 1
_VALUE_BY_NAME_KEY = "_durable_value_by_name"


@lru_cache(maxsize=3)
def _constructor_fields(cls: type[AgentResponse[Any]] | type[Message] | type[Content]) -> tuple[str, ...]:
    """Cache explicit public parameters, never names supplied by a stored type."""
    return tuple(
        name
        for name, parameter in signature(cls).parameters.items()
        if not name.startswith("_") and parameter.kind in (Parameter.POSITIONAL_OR_KEYWORD, Parameter.KEYWORD_ONLY)
    )


def _constructor_kwargs(
    data: Mapping[str, Any], cls: type[AgentResponse[Any]] | type[Message] | type[Content]
) -> dict[str, Any]:
    return {name: data[name] for name in _constructor_fields(cls) if name in data}


def _load_content(data: Any) -> Any:
    """Decode only Content envelope edges, not arbitrary dictionaries with a type key."""
    if not isinstance(data, Mapping):
        return data
    fields = _constructor_kwargs(cast(Mapping[str, Any], data), Content)
    if not isinstance(fields.get("type"), str) or not fields["type"]:
        raise ValueError("Content mapping requires 'type' to be a non-empty string")
    if isinstance(fields.get("function_call"), Mapping):
        fields["function_call"] = _load_content(fields["function_call"])
    for name in ("items", "inputs"):
        if isinstance(fields.get(name), list):
            fields[name] = [_load_content(item) for item in fields[name]]
    # Unlike code/shell outputs, image-generation outputs are arbitrary application data.
    if fields["type"] in ("code_interpreter_tool_result", "shell_tool_result") and isinstance(
        fields.get("outputs"), list
    ):
        fields["outputs"] = [_load_content(item) for item in fields["outputs"]]
    # arguments, result, output, annotations and additional_properties stay opaque.
    return Content(**fields)


def _load_message(data: Any) -> Message:
    if isinstance(data, Message):
        return data
    if not isinstance(data, Mapping):
        raise TypeError("Agent response messages must be Message instances or mappings")
    fields = _constructor_kwargs(cast(Mapping[str, Any], data), Message)
    if fields.get("contents") is not None:
        fields["contents"] = [_load_content(content) for content in fields["contents"]]
    return Message(**fields)


def _serialize_model_value(value: BaseModel) -> tuple[Any, bool]:
    """Prefer alias JSON; use field-name JSON when serialization aliases are not inputs."""
    payload = value.model_dump(mode="json", by_alias=True, round_trip=True)
    field_payload = value.model_dump(mode="json", by_alias=False, round_trip=True)
    try:
        restored = type(value).model_validate_json(json.dumps(payload))
    except ValidationError:
        pass
    else:
        if restored.model_dump(mode="json", by_alias=False, round_trip=True) == field_payload:
            return payload, False
    # A serialization alias may be ignored in favor of a default without raising an error.
    # Record the input mode, not a Python model name, for the caller's declared format.
    restored = type(value).model_validate_json(json.dumps(field_payload), by_alias=False, by_name=True)
    if restored.model_dump(mode="json", by_alias=False, round_trip=True) != field_payload:
        raise ValueError("Structured response value cannot round-trip through its declared model")
    return field_payload, True


def is_terminal_agent_response(response: AgentResponse[Any]) -> bool:
    """Identify durable failures/completions, retaining the legacy non-tool error fallback.

    Args:
        response: Agent response whose durable status and non-tool error contents to inspect.

    Returns:
        Whether the response reports a durable failure, completion, or non-tool error.
    """
    return response.additional_properties.get("durable_status") in ("error", "already_completed") or any(
        content.type == "error"
        for message in response.messages
        if message.role != "tool"
        for content in message.contents
    )


def invocation_outcome(response: AgentResponse[Any], *, legacy: bool = False) -> Literal["succeeded", "failed"] | None:
    """Classify invocation evidence, not delivery availability or an approval's pending action.

    Legacy transcript projections can have lost their error contents. Their absence
    does not prove success. An independent original mailbox does not have that loss.
    Accepted or already-unavailable replies likewise cannot establish a new outcome.
    """
    status = response.additional_properties.get("durable_status")
    if status == "accepted":
        return None
    if status == "already_completed" or any(
        content.type == "error" and content.error_code == "response_expired"
        for message in response.messages
        if message.role != "tool"
        for content in message.contents
    ):
        outcome = response.additional_properties.get("durable_outcome")
        return outcome if outcome in ("succeeded", "failed") else None
    if is_terminal_agent_response(response):
        return "failed"
    return None if legacy else "succeeded"


def serialize_agent_response(response: AgentResponse) -> dict[str, Any]:
    """Snapshot a response as inline base-response JSON for durable delivery.

    Public base fields are authoritative even for subclasses. Serializable extra
    fields may remain in the raw snapshot, but are not constructor arguments when
    delivering it. No response-format class or provider raw representation is stored.
    The containing entity schema versions delivery; a response version is not added.

    Args:
        response: Agent response whose public fields and structured value to snapshot.

    Returns:
        Detached response payload with canonical base-response fields.
    """
    base = AgentResponse(**{
        name: getattr(response, name)
        for name in _constructor_fields(AgentResponse)
        if name not in ("value", "response_format", "raw_representation") and hasattr(response, name)
    })
    # Use the base serializer, not an override that may omit or replace public response fields.
    payload = AgentResponse.to_dict(response)
    payload.update(base.to_dict())
    payload.pop("response_format", None)
    payload.pop("raw_representation", None)
    payload.pop("value", None)
    payload.pop(_VALUE_BY_NAME_KEY, None)
    payload["type"] = "agent_response"

    # Core's lazy value getter changes its cache. Parse a copy so recording is observational.
    source = copy(response)
    value = source._value  # pyright: ignore[reportPrivateUsage]
    if (
        not is_terminal_agent_response(source)
        and not source.user_input_requests
        and source.additional_properties.get("durable_status") != "accepted"
    ):
        value = source.value
    if value is not None or source._value_parsed:  # pyright: ignore[reportPrivateUsage]
        by_name = getattr(source, _VALUE_BY_NAME_KEY, False)
        if isinstance(value, BaseModel):
            value, by_name = _serialize_model_value(value)
        payload["value"] = value
        if by_name:
            payload[_VALUE_BY_NAME_KEY] = True
    return deepcopy(payload)


def load_agent_response(agent_response: AgentResponse | dict[str, Any] | None) -> AgentResponse:
    """Convert raw payloads into AgentResponse instance.

    Args:
        agent_response: The response to convert, can be an AgentResponse, dict, or None

    Returns:
        AgentResponse: The converted response object

    Raises:
        ValueError: If agent_response is None, its optional delivery version is unsupported,
            or a response or content envelope is malformed.
        TypeError: If the input type or required constructor fields are invalid.
    """
    if agent_response is None:
        raise ValueError("agent_response cannot be None")

    logger.debug("[load_agent_response] Loading agent response of type: %s", type(agent_response))

    if isinstance(agent_response, AgentResponse):
        return agent_response
    if isinstance(agent_response, dict):
        logger.debug("[load_agent_response] Constructing a base response from delivery fields")
        if _DELIVERY_VERSION_KEY in agent_response:
            version = agent_response[_DELIVERY_VERSION_KEY]
            if type(version) is not int or version != _DELIVERY_VERSION:
                raise ValueError("Unsupported durable response version")
        response_type = agent_response.get("type")
        if "type" in agent_response and (not isinstance(response_type, str) or not response_type):
            raise ValueError("Agent response type must be a non-empty string")
        # Internal callers supply messages without a type. Custom response types are
        # projected onto the base class, never imported, but must still be response-like.
        if response_type != "agent_response" and agent_response.get("messages") is None:
            raise ValueError("Agent response mapping requires a response type or messages")
        # Filtering is consumer-only. Neither construction nor subsequent consumer mutations
        # may remove or change unknown fields in the raw mailbox payload.
        data = deepcopy(agent_response)
        fields = _constructor_kwargs(data, AgentResponse)
        fields.pop("response_format", None)
        messages = fields.get("messages")
        if messages is not None and not isinstance(messages, Message):
            if not isinstance(messages, Sequence) or isinstance(messages, (str, bytes, bytearray)):
                raise TypeError("Agent response messages must be a sequence of messages")
            fields["messages"] = [_load_message(message) for message in cast("Sequence[Any]", messages)]
        response = AgentResponse(**fields)
        if "value" in data:
            # Core sets this to False for None, losing the distinction between absent and null.
            response._value_parsed = True  # pyright: ignore[reportPrivateUsage]
            if data.get(_VALUE_BY_NAME_KEY) is True:
                setattr(response, _VALUE_BY_NAME_KEY, True)
        return response

    raise TypeError(f"Unsupported type for agent_response: {type(agent_response)}")


def ensure_response_format(
    response_format: type[BaseModel] | None,
    correlation_id: str,
    response: AgentResponse[Any],
) -> None:
    """Ensure the AgentResponse value is parsed into the expected response_format.

    This function modifies the response in-place by parsing its value attribute
    into the specified Pydantic model format. Terminal responses and accepted
    acknowledgements are left unchanged. A retained value, including null,
    takes precedence over parsing message text again.

    Args:
        response_format: Optional Pydantic model class to parse the response value into
        correlation_id: Correlation ID for logging purposes
        response: The AgentResponse object to validate and parse

    Raises:
        ValueError: If response_format is specified but response.value cannot be parsed
    """
    if response_format is not None:
        if (
            is_terminal_agent_response(response)
            or response.user_input_requests
            or response.additional_properties.get("durable_status") == "accepted"
        ):
            return

        # Only reuse a retained value; an unparsed response must use the requested format.
        value = response._value  # pyright: ignore[reportPrivateUsage]
        value_present = value is not None or response._value_parsed  # pyright: ignore[reportPrivateUsage]
        # Set the response format on the response so .value knows how to parse
        response._response_format = response_format  # pyright: ignore[reportPrivateUsage]
        if value_present:
            if not isinstance(value, response_format):
                # Retained values crossed a JSON boundary, just like structured message text.
                by_name = getattr(response, _VALUE_BY_NAME_KEY, False)
                if isinstance(value, BaseModel):
                    value, by_name = _serialize_model_value(value)
                if by_name:
                    value = response_format.model_validate_json(json.dumps(value), by_alias=False, by_name=True)
                else:
                    value = response_format.model_validate_json(json.dumps(value))
            response._value = value  # pyright: ignore[reportPrivateUsage]
            response._value_parsed = True  # pyright: ignore[reportPrivateUsage]
        else:
            response._value_parsed = False  # pyright: ignore[reportPrivateUsage]

        # Access response.value to trigger parsing (may raise ValidationError)
        # Validate that parsing succeeded
        if not isinstance(response.value, response_format):
            raise ValueError(
                f"Response value could not be parsed into required format {response_format.__name__} "
                f"for correlation_id {correlation_id}"
            )

        logger.debug(
            "[ensure_response_format] Loaded AgentResponse.value for correlation_id %s with type: %s",
            correlation_id,
            type(response.value).__name__,
        )
