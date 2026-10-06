# Copyright (c) Microsoft. All rights reserved.

"""Opt-in plain JSON decoding for framework-owned Durable Task payloads."""

from __future__ import annotations

import json
from typing import Any, NewType, cast

from durabletask.serialization import DataConverter
from durabletask.worker import TaskHubGrpcWorker

from ._shared_state_validation import load_migration_json, load_state_json

# A trusted target annotation, never a wire marker or a concrete state class.
# NewType survives SDK type discovery without triggering StateShim's isinstance
# check for concrete intended_type values.
JsonPayload = NewType("JsonPayload", object)
JsonState = NewType("JsonState", object)
JsonMigration = NewType("JsonMigration", object)


class _JsonPayloadConverter(DataConverter):
    """Delegate native behavior, with opt-in plain JSON for framework-owned values."""

    def __init__(self, inner: DataConverter, *, deserialize_untagged_json: bool = False) -> None:
        self._inner = inner
        self._deserialize_untagged_json = deserialize_untagged_json

    @property
    def deserializes_untagged_json(self) -> bool:
        """Return whether values without a framework target use plain JSON."""
        return self._deserialize_untagged_json

    def with_untagged_json(self) -> _JsonPayloadConverter:
        """Return this converter or an upgraded copy that reads untagged JSON."""
        if self._deserialize_untagged_json:
            return self
        return _JsonPayloadConverter(self._inner, deserialize_untagged_json=True)

    def can_reconstruct(self, target_type: Any) -> bool:
        """Recognize the framework tag without changing native type discovery."""
        return (
            target_type is JsonPayload
            or target_type is JsonState
            or target_type is JsonMigration
            or self._inner.can_reconstruct(target_type)
        )

    def serialize(self, value: Any) -> str | None:
        """Keep the caller's serializer, including its native custom types."""
        return self._inner.serialize(value)

    def deserialize(self, data: str | None, target_type: Any = None) -> Any:
        """Read framework JSON without interpreting SDK object markers."""
        if target_type is JsonMigration:
            return None if data is None or data == "" else load_migration_json(data)
        if target_type is JsonState:
            return None if data is None or data == "" else load_state_json(data)
        if target_type is JsonPayload or self._deserialize_untagged_json:
            return None if data is None or data == "" else json.loads(data)
        return self._inner.deserialize(data, target_type)

    def coerce(self, value: Any, target_type: Any = None) -> Any:
        """Preserve the original converter's value-level coercion policy."""
        return self._inner.coerce(value, target_type)


def install_json_payload_converter(worker: TaskHubGrpcWorker, *, deserialize_untagged_json: bool = False) -> None:
    """Install the worker-local decoder before processing any work.

    durabletask >=1.7.1 supplies converter-aware input discovery, task result
    types and deferred raw state decoding. It has no public converter setter,
    so this guarded private-field integration is confined to worker setup.
    Custom converters remain supported for native work. Framework payloads
    still require JSON, not an arbitrary custom wire format. Generated Functions
    agent entities also opt into plain JSON for untagged operation input.
    """
    native = cast(Any, worker)
    converter = getattr(native, "_data_converter", None)
    if isinstance(converter, _JsonPayloadConverter):
        if deserialize_untagged_json and not converter.deserializes_untagged_json:
            if getattr(native, "_is_running", False) is True:
                raise RuntimeError("Configure framework JSON decoding before starting the Durable Task worker.")
            native._data_converter = converter.with_untagged_json()
        return
    if getattr(native, "_is_running", False) is True:
        raise RuntimeError("Configure framework JSON decoding before starting the Durable Task worker.")
    if converter is None or not all(
        callable(getattr(converter, method, None))
        for method in ("can_reconstruct", "serialize", "deserialize", "coerce")
    ):
        raise RuntimeError("Durable Task worker must expose the data converter interface from durabletask >=1.7.1.")
    native._data_converter = _JsonPayloadConverter(
        converter,
        deserialize_untagged_json=deserialize_untagged_json,
    )
