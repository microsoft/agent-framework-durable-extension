# Copyright (c) Microsoft. All rights reserved.

"""Explicit transport expectations built from original fixtures, never delivery lookups."""

from copy import deepcopy
from typing import Any

from agent_framework import AgentResponse

from agent_framework_durabletask._response_utils import serialize_agent_response


def expected_shared_transport(original: AgentResponse[Any] | dict[str, Any]) -> dict[str, Any]:
    """Add only the known shared-value policy to an original Core snapshot."""
    expected = deepcopy(original if isinstance(original, dict) else serialize_agent_response(original))
    policy = {"profile": "agent-framework-python.shared-value", "version": 1}
    if "_durable_value_policy" in expected:
        assert expected["_durable_value_policy"] == policy
        assert type(expected["_durable_value_policy"]["version"]) is int
    expected["_durable_value_policy"] = policy
    return expected


def assert_shared_transport(actual: AgentResponse[Any], expected: dict[str, Any]) -> None:
    """Compare the entire transport, including an exact, integer-versioned policy."""
    snapshot = serialize_agent_response(actual)
    assert snapshot["_durable_value_policy"] == {
        "profile": "agent-framework-python.shared-value",
        "version": 1,
    }
    assert type(snapshot["_durable_value_policy"]["version"]) is int
    assert snapshot == expected
