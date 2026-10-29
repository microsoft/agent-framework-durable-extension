# Copyright (c) Microsoft. All rights reserved.

"""Outer format dispatch rejects unknown semantics before value decoding."""

import json

import pytest

from agent_framework_durabletask._checkpoints._format import (
    _FORMAT_DECODERS,
    FORMAT,
    FORMAT_VERSION,
    JOIN_CONTRACT,
    _decode_v1,
    decode_checkpoint_document,
    encode_checkpoint_document,
)


def test_checkpoint_format_is_separate_from_join_and_semantic_contracts():
    payload = {"contract": "checkpoint-only-start/1", "state": {"opaque": "trusted value"}}
    encoded = encode_checkpoint_document(payload)
    document = decode_checkpoint_document(encoded)

    assert document == {
        "format": FORMAT,
        "formatVersion": FORMAT_VERSION,
        "joinContract": JOIN_CONTRACT,
        **payload,
    }


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("format", "unknown.format", "format identifier"),
        ("formatVersion", 2, "format version"),
        ("formatVersion", True, "format version"),
        ("joinContract", "unknown-join/9", "join contract"),
        ("contract", "checkpoint-only-start/99", "payload contract"),
    ],
)
def test_unknown_versions_and_contracts_reject_without_customer_decode(field, value, message):
    document = {
        "format": FORMAT,
        "formatVersion": FORMAT_VERSION,
        "joinContract": JOIN_CONTRACT,
        "contract": "checkpoint-only-start/1",
        "state": {"must_not_decode": object.__name__},
    }
    document[field] = value
    with pytest.raises(ValueError, match=message):
        decode_checkpoint_document(json.dumps(document).encode())


def test_duplicate_and_extra_envelope_fields_reject():
    with pytest.raises(ValueError, match="Duplicate checkpoint field"):
        decode_checkpoint_document(
            b'{"format":"agent_framework_durabletask.logical_checkpoint",'
            b'"format":"agent_framework_durabletask.logical_checkpoint"}'
        )
    document = {
        "format": FORMAT,
        "formatVersion": FORMAT_VERSION,
        "joinContract": JOIN_CONTRACT,
        "contract": "checkpoint-only-start/1",
        "state": {},
        "unrecognized": True,
    }
    with pytest.raises(ValueError, match="payload contract"):
        decode_checkpoint_document(json.dumps(document).encode())


def test_registered_format_migration_normalizes_before_customer_decode(monkeypatch):
    seen = []

    def migrate_v2(document):
        seen.append(document["formatVersion"])
        return _decode_v1({**document, "formatVersion": FORMAT_VERSION})

    monkeypatch.setitem(_FORMAT_DECODERS, 2, migrate_v2)
    document = {
        "format": FORMAT,
        "formatVersion": 2,
        "joinContract": JOIN_CONTRACT,
        "contract": "checkpoint-only-start/1",
        "state": {"opaque": "unchanged"},
    }

    migrated = decode_checkpoint_document(json.dumps(document).encode())

    assert seen == [2]
    assert migrated["formatVersion"] == FORMAT_VERSION
    assert migrated["contract"] == "checkpoint-only-start/1"
    assert migrated["state"] == {"opaque": "unchanged"}
