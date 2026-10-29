# Copyright (c) Microsoft. All rights reserved.

"""Public read projection for semantic observations and genuine new receipts."""

from agent_framework_durabletask._response_utils import load_agent_response
from agent_framework_durabletask._state_reader import SharedAgentStateReader

from ._observation_reader import PublishedObservationReader
from ._types import JsonObject


class SemanticAgentStateReader(PublishedObservationReader):
    def __init__(self, raw: JsonObject) -> None:
        super().__init__(raw, load_agent_response)
        self._current = SharedAgentStateReader(raw["runtime"])
