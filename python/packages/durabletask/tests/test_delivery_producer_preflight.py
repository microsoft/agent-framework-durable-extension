# Copyright (c) Microsoft. All rights reserved.

"""Rejected producers must not run serialization hooks on caller-owned extras."""

from datetime import datetime, timezone
from typing import Any

import pytest
from agent_framework import AgentResponse
from agent_framework._serialization import SerializationMixin

from agent_framework_durabletask._delivery_state import stage_response


class _Extra(SerializationMixin):
    def __init__(self) -> None:
        self.calls = 0

    def to_dict(self, **kwargs: Any) -> dict[str, Any]:
        self.calls += 1
        return {"value": 7}

    @classmethod
    def from_dict(cls, value: Any, **kwargs: Any) -> "_Extra":
        raise AssertionError("Producer extras must not be reconstructed")


@pytest.mark.parametrize("metadata", [False, True], ids=["extra", "metadata"])
def test_staging_rejects_live_extra_before_calling_its_serializer(metadata: bool) -> None:
    source: dict[str, Any] = {
        "schemaVersion": "2.0.0",
        "data": {
            "conversationHistory": [],
            "terminalResults": {},
            "completionReceipts": {},
        },
    }
    extra = _Extra()
    response = AgentResponse(messages=[])
    target = response.additional_properties if metadata else vars(response)
    target["future"] = extra
    with pytest.raises(ValueError, match="JSON"):
        stage_response(source, "turn", response, delivery_window_seconds=60, now=datetime.now(timezone.utc))
    assert extra.calls == 0
    assert source["data"]["terminalResults"] == source["data"]["completionReceipts"] == {}
