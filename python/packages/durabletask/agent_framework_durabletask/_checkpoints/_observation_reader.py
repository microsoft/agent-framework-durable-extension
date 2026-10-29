# Copyright (c) Microsoft. All rights reserved.

"""Shared published polling view. Observations are not native completion receipts."""

from collections.abc import Callable
from copy import deepcopy
from datetime import datetime
from typing import Any

from ._types import JsonObject, is_list, is_string_keyed_dict


class PublishedObservationReader:
    def __init__(self, raw: JsonObject, response_loader: Callable[[JsonObject], Any]) -> None:
        if raw.get("contract") != "agent-semantic-checkpoint/1":
            raise ValueError("Unsupported agent checkpoint.")
        if raw.get("publicInvocationPolicy") != "published-python-invocation-public-view/2":
            raise ValueError("Unsupported agent checkpoint public observation policy.")
        count, observations = raw.get("publishedEntryCount"), raw.get("observations")
        if type(count) is not int or count < 0 or not is_list(observations):
            raise ValueError("Invalid published checkpoint observation state.")
        if any(
            not is_string_keyed_dict(row)
            or not isinstance(row.get("correlation"), str)
            or not isinstance(row.get("response"), dict)
            for row in observations
        ):
            raise ValueError("Invalid published checkpoint response observation.")
        self._raw = deepcopy(raw)
        self._response_loader = response_loader

    @property
    def message_count(self) -> int:
        return self._raw["publishedEntryCount"]

    def to_dict(self) -> JsonObject:
        return deepcopy(self._raw)

    def try_get_agent_response(self, correlation_id: str, *, now: datetime | None = None) -> Any | None:
        for observation in self._raw["observations"]:
            if observation["correlation"] == correlation_id:
                return self._response_loader(deepcopy(observation["response"]))
        return None
