# Copyright (c) Microsoft. All rights reserved.

"""Stable request and SDK action comparisons for workflow occurrence tests."""

import json
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any


def _request_contract(request: dict[str, Any]) -> str:
    """Compare every request field except its wall-clock bookkeeping timestamp.

    RunRequest supplies created_at from datetime.now, even in an orchestrator.
    In durabletask 1.7.2, action sequence IDs and history-clock UUIDs identify
    entity calls. Replaying entityOperationCalled consumes the scheduled action
    without comparing or hashing its input. Occurrence/revision hashes cover
    Messages, not this request timestamp. created_at is still persisted metadata:
    this oracle does NOT establish byte-identical requests or stored timestamps.
    Never remove timestamps from nested application messages or options.
    """
    contract = deepcopy(request)
    created_at = contract.pop("created_at")
    assert isinstance(created_at, str)
    parsed = datetime.fromisoformat(created_at)
    assert parsed.tzinfo is not None and parsed.utcoffset() == timezone.utc.utcoffset(parsed)
    # Canonical JSON also distinguishes False/0 and integer/float payloads,
    # unlike Python dict equality. Only object member ordering is immaterial.
    return json.dumps(contract, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _assert_same_action_contract(actual: Any, recorded: Any) -> None:
    actual, recorded = deepcopy(actual), deepcopy(recorded)
    for action in (actual, recorded):
        if action.HasField("sendEntityMessage"):
            assert action.sendEntityMessage.HasField("entityOperationCalled")
            called = action.sendEntityMessage.entityOperationCalled
            called.input.value = _request_contract(json.loads(called.input.value))
    # Includes action ID, kind, all routing/UUID fields and every stable input
    # field. Non-entity actions retain full protobuf equality without exclusions.
    assert actual == recorded
