# Copyright (c) Microsoft. All rights reserved.

"""State-model-independent lookup of declared legacy Core delivery snapshots."""

from __future__ import annotations

from copy import deepcopy
from typing import Any, cast

from agent_framework import AgentResponse

from ._response_utils import load_agent_response
from ._shared_response import serialize_terminal_response
from ._shared_state_validation import validate_identifier

LEGACY_DELIVERY_FIELD = "_dafx_legacy_delivery"


def read_legacy_deliveries(extensions: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Validate only declared full responses, never infer historical outcomes."""
    if extensions is None or LEGACY_DELIVERY_FIELD not in extensions:
        return {}
    profile = extensions[LEGACY_DELIVERY_FIELD]
    if not isinstance(profile, dict):
        raise ValueError("Legacy delivery metadata has an unsupported profile.")
    profile = cast(dict[str, Any], profile)
    if profile.get("profile") != "dafx-legacy-delivery-v1" or not isinstance(profile.get("responses"), dict):
        raise ValueError("Legacy delivery metadata has an unsupported profile.")
    responses = cast(dict[str, dict[str, Any]], profile["responses"])
    for correlation_id, snapshot in responses.items():
        validate_identifier(correlation_id, "Legacy delivery correlationId")
        if not isinstance(snapshot, dict):
            raise ValueError("Legacy delivery snapshots must be response objects.")
        serialize_terminal_response(snapshot)
    return deepcopy(responses)


def lookup_legacy_delivery(extensions: dict[str, Any] | None, correlation_id: str) -> AgentResponse | None:
    """Project a known full delivery for either legacy state reader without writes."""
    snapshot = read_legacy_deliveries(extensions).get(correlation_id)
    return None if snapshot is None else load_agent_response(snapshot)
