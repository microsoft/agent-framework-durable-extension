# Copyright (c) Microsoft. All rights reserved.

"""Shared utilities for handling AgentResponse parsing and validation."""

import json
import logging
from typing import Any

from agent_framework import AgentResponse
from pydantic import BaseModel

logger = logging.getLogger("agent_framework.durabletask")


def serialize_agent_response(response: AgentResponse) -> dict[str, Any]:
    """Serialize a response and its structured value for durable delivery.

    Core's ``to_dict()`` omits the private storage backing ``value``. Include
    that public value explicitly, converting Pydantic models to JSON data.
    """
    payload = response.to_dict()
    value = response.value
    if value is not None:
        payload["value"] = value.model_dump(mode="json") if isinstance(value, BaseModel) else value
    return payload


def load_agent_response(agent_response: AgentResponse | dict[str, Any] | None) -> AgentResponse:
    """Convert raw payloads into AgentResponse instance.

    Args:
        agent_response: The response to convert, can be an AgentResponse, dict, or None

    Returns:
        AgentResponse: The converted response object

    Raises:
        ValueError: If agent_response is None
        TypeError: If agent_response is an unsupported type
    """
    if agent_response is None:
        raise ValueError("agent_response cannot be None")

    logger.debug("[load_agent_response] Loading agent response of type: %s", type(agent_response))

    if isinstance(agent_response, AgentResponse):
        return agent_response
    if isinstance(agent_response, dict):
        logger.debug("[load_agent_response] Converting dict payload using AgentResponse.from_dict")
        return AgentResponse.from_dict(agent_response)

    raise TypeError(f"Unsupported type for agent_response: {type(agent_response)}")


def ensure_response_format(
    response_format: type[BaseModel] | None,
    correlation_id: str,
    response: AgentResponse[Any],
) -> None:
    """Ensure the AgentResponse value is parsed into the expected response_format.

    This function modifies the response in-place by parsing its value attribute
    into the specified Pydantic model format. Error responses and completed
    delivery statuses are left unchanged. A retained value takes precedence
    over parsing message text again.

    Args:
        response_format: Optional Pydantic model class to parse the response value into
        correlation_id: Correlation ID for logging purposes
        response: The AgentResponse object to validate and parse

    Raises:
        ValueError: If response_format is specified but response.value cannot be parsed
    """
    if response_format is not None:
        if response.additional_properties.get("durable_status") == "already_completed" or any(
            content.type == "error" for message in response.messages for content in message.contents
        ):
            return

        # Only reuse a retained value; an unparsed response must use the requested format.
        value = response._value  # pyright: ignore[reportPrivateUsage]
        # Set the response format on the response so .value knows how to parse
        response._response_format = response_format  # pyright: ignore[reportPrivateUsage]
        if value is not None:
            if not isinstance(value, response_format):
                # Retained values crossed a JSON boundary, just like structured message text.
                value_json = value.model_dump_json() if isinstance(value, BaseModel) else json.dumps(value)
                value = response_format.model_validate_json(value_json)
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
