# Copyright (c) Microsoft. All rights reserved.

"""Versioned outer checkpoint envelopes, separate from semantic state versions."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from ._types import is_json_object

FORMAT = "agent_framework_durabletask.logical_checkpoint"
FORMAT_VERSION = 1
JOIN_CONTRACT = "published-batch-join/1"
_CONTRACT_FIELDS = {
    "checkpoint-only-start/1": {"contract", "state"},
    "checkpoint-only-prefix/1": {"contract", "state", "prefix", "input"},
}
_FormatDecoder = Callable[[dict[str, Any]], dict[str, Any]]


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate checkpoint field: {key}")
        result[key] = value
    return result


def _decode_v1(document: dict[str, Any]) -> dict[str, Any]:
    if document.get("joinContract") != JOIN_CONTRACT:
        raise ValueError("Unsupported checkpoint join contract.")
    contract = document.get("contract")
    required = _CONTRACT_FIELDS.get(contract) if isinstance(contract, str) else None
    if required is None or set(document) != required | {"format", "formatVersion", "joinContract"}:
        raise ValueError("Malformed or unsupported checkpoint payload contract.")
    return document


_FORMAT_DECODERS: dict[int, _FormatDecoder] = {1: _decode_v1}


def decode_checkpoint_document(data: bytes) -> dict[str, Any]:
    """Dispatch by explicit outer version before customer-value decoding.

    A future format adds a strict version-specific decoder/migrator to
    ``_FORMAT_DECODERS``. It must normalize to the current canonical document
    and must not reinterpret semantic, definition, or native protocol versions.
    """
    if not isinstance(data, bytes):
        raise TypeError("Checkpoint document must be bytes.")
    try:
        document = json.loads(data, object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Malformed checkpoint document.") from error
    if not is_json_object(document):
        raise ValueError("Checkpoint document must be a JSON object.")
    if document.get("format") != FORMAT:
        raise ValueError("Unsupported checkpoint format identifier.")
    version = document.get("formatVersion")
    if type(version) is not int:
        raise ValueError(f"Unsupported checkpoint format version: {version!r}.")
    decoder = _FORMAT_DECODERS.get(version)
    if decoder is None:
        raise ValueError(f"Unsupported checkpoint format version: {version!r}.")
    return decoder(document)


def encode_checkpoint_document(payload: dict[str, Any]) -> bytes:
    """Wrap a supported payload contract in the current outer format."""
    if not is_json_object(payload):
        raise TypeError("Checkpoint payload must be a JSON object.")
    contract = payload.get("contract")
    required = _CONTRACT_FIELDS.get(contract) if isinstance(contract, str) else None
    if required is None or set(payload) != required:
        raise ValueError("Malformed or unsupported checkpoint payload contract.")
    document = {
        "format": FORMAT,
        "formatVersion": FORMAT_VERSION,
        "joinContract": JOIN_CONTRACT,
        **payload,
    }
    return json.dumps(document, allow_nan=False, separators=(",", ":")).encode()
