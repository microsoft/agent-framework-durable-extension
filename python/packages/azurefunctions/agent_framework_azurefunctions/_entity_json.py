# Copyright (c) Microsoft. All rights reserved.

"""Plain JSON ingress for framework-generated Functions entities only."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, cast

from azure.durable_functions import DurableEntityContext
from azure.durable_functions.entity import Entity


class _JsonEntityContext(DurableEntityContext):
    """Keep entity state and operation inputs as JSON, not SDK custom objects."""

    def __init__(self, name: str, key: str, exists: bool, state: Any) -> None:
        # Do not call the SDK's from_json: 1.3.1 eagerly constructs custom state
        # there. Passing a decoded value to the constructor also leaves 1.6's
        # _state_is_raw false, so its get_state/set_state lifecycle is unchanged.
        super().__init__(name=name, key=key, exists=exists, state=None if state is None else json.loads(state))

    def get_input(self, expected_type: type | None = None) -> Any:
        """Unwrap the two native input layers without object reconstruction."""
        del expected_type
        serialized = json.loads(self._input)
        return None if serialized is None else json.loads(serialized)


def create_json_entity(fn: Callable[[DurableEntityContext], None]) -> Callable[[Any], str]:
    """Use a JSON context with the SDK's unchanged entity batch executor."""

    def handle(context: Any) -> str:
        context_body = getattr(context, "body", None)
        if context_body is None:
            context_body = context
        envelope = json.loads(context_body)
        identity = envelope.pop("self")
        envelope["name"] = identity["name"]
        envelope["key"] = identity["key"]
        batch = envelope.pop("batch")
        return Entity(fn).handle(_JsonEntityContext(**envelope), batch)

    # Match Entity.create's unannotated rich-binding input. The Functions worker
    # rejects both Any and str annotations for entityTrigger during indexing.
    handle.__annotations__.pop("context")
    # Match Entity.create's inspection hook without changing the batch engine.
    cast(Any, handle).entity_function = fn
    return handle
