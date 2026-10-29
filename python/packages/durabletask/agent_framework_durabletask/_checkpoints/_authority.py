# Copyright (c) Microsoft. All rights reserved.

"""Operator-signed, purpose-bound authority for opaque migration payloads.

The verifier authenticates payload bytes and trusted runtime identity before
returning those bytes. It intentionally does not deserialize Python objects.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from typing import Any, Mapping

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from ._types import JsonValue, is_json_object

FORMAT = "durabletask.checkpoint-authority/v1"
PURPOSE_CHECKPOINT = "checkpoint"
PURPOSE_CHILD_RESULT = "child-result"
_ALLOWED_PURPOSES = frozenset({PURPOSE_CHECKPOINT, PURPOSE_CHILD_RESULT})
MAX_ENVELOPE_BYTES = 64 * 1024 * 1024
_KEY_ID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class CheckpointAuthorityError(ValueError):
    """The supplied payload is not authorized for this backend execution."""


@dataclass(frozen=True, slots=True)
class BackendBinding:
    """Trusted dispatch identity supplied by the host, never parsed from input.

    ``backend_id`` and ``definition_sha256`` must come from deployment-owned
    configuration/registration. ``execution_id`` must be read from the current
    backend work item, not copied from payload JSON, tags, or version fields.
    """

    backend_id: str
    task_hub: str
    instance_id: str
    execution_id: str
    definition_sha256: str

    def __post_init__(self) -> None:
        for name in ("backend_id", "task_hub", "instance_id", "execution_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value or value != value.strip():
                raise ValueError(f"{name} must be a non-empty canonical string")
        if not _SHA256.fullmatch(self.definition_sha256):
            raise ValueError("definition_sha256 must be a lowercase SHA-256 hex digest")


@dataclass(frozen=True, slots=True)
class ChildResultBinding:
    """Identity of a child result and the exact parent/run that may receive it."""

    child: BackendBinding
    parent: BackendBinding
    migration_epoch: str

    def __post_init__(self) -> None:
        if not isinstance(self.child, BackendBinding) or not isinstance(self.parent, BackendBinding):
            raise TypeError("child and parent must be BackendBinding values")
        if (
            not isinstance(self.migration_epoch, str)
            or not self.migration_epoch
            or self.migration_epoch != self.migration_epoch.strip()
        ):
            raise ValueError("migration_epoch must be a non-empty canonical string")


def definition_digest(definition_bytes: bytes) -> str:
    """Return the deployment-owned SHA-256 digest for exact definition bytes."""
    if not isinstance(definition_bytes, bytes):
        raise TypeError("definition_bytes must be bytes")
    return hashlib.sha256(definition_bytes).hexdigest()


def public_key_bytes(key: Ed25519PublicKey) -> bytes:
    """Export an Ed25519 public key in raw 32-byte form for a runtime keyring."""
    if not isinstance(key, Ed25519PublicKey):
        raise TypeError("key must be an Ed25519PublicKey")
    return key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )


def load_public_key(raw: bytes) -> Ed25519PublicKey:
    """Load one raw Ed25519 public key from deployment-controlled key material."""
    if not isinstance(raw, bytes) or len(raw) != 32:
        raise ValueError("an Ed25519 public key must contain exactly 32 bytes")
    return Ed25519PublicKey.from_public_bytes(raw)


def issue_capability(
    *,
    purpose: str,
    key_id: str,
    operator_private_key: Ed25519PrivateKey,
    binding: Mapping[str, object],
    payload_bytes: bytes,
) -> bytes:
    """Sign opaque bytes and a purpose-specific backend binding.

    This function belongs in an operator-controlled process. The runtime needs
    only the public verification key. The signature authorizes the bound bytes;
    it does not prove backend ancestry or enforce one-time use.
    """
    _validate_purpose(purpose)
    if not isinstance(key_id, str) or not _KEY_ID.fullmatch(key_id):
        raise ValueError("key_id must use 1-64 ASCII letters, digits, '.', '_' or '-' characters")
    if not isinstance(operator_private_key, Ed25519PrivateKey):
        raise TypeError("operator_private_key must be an Ed25519PrivateKey")
    if not isinstance(payload_bytes, bytes):
        raise TypeError("payload_bytes must be bytes")
    normalized_binding = _json_value(dict(binding))
    payload: dict[str, object] = {
        "format": FORMAT,
        "purpose": purpose,
        "key_id": key_id,
        "binding": normalized_binding,
        "payload_sha256": hashlib.sha256(payload_bytes).hexdigest(),
    }
    signature = operator_private_key.sign(_canonical(payload))
    envelope = {
        "format": FORMAT,
        "payload": payload,
        "signature": _b64(signature),
        "data": _b64(payload_bytes),
    }
    return _canonical(envelope)


def verify_capability(
    envelope_bytes: bytes,
    *,
    purpose: str,
    trusted_public_keys: Mapping[str, Ed25519PublicKey | bytes],
    expected_binding: Mapping[str, object],
    max_envelope_bytes: int = MAX_ENVELOPE_BYTES,
) -> bytes:
    """Verify signature, purpose, trusted dispatch binding, then payload digest.

    The return value is still opaque bytes. The caller may deserialize only
    after this function returns. No marker, tag, or version is accepted as a
    substitute for the deployment-owned public key and binding.
    """
    _validate_purpose(purpose)
    if not isinstance(envelope_bytes, bytes):
        raise TypeError("envelope_bytes must be bytes")
    if not isinstance(max_envelope_bytes, int) or max_envelope_bytes <= 0:
        raise ValueError("max_envelope_bytes must be positive")
    if len(envelope_bytes) > max_envelope_bytes:
        raise CheckpointAuthorityError("capability envelope exceeds configured size limit")

    envelope = _strict_json(envelope_bytes)
    if not is_json_object(envelope) or set(envelope) != {"format", "payload", "signature", "data"}:
        raise CheckpointAuthorityError("malformed capability envelope")
    if envelope["format"] != FORMAT:
        raise CheckpointAuthorityError("unsupported capability envelope format")
    payload = envelope["payload"]
    if not is_json_object(payload) or set(payload) != {"format", "purpose", "key_id", "binding", "payload_sha256"}:
        raise CheckpointAuthorityError("malformed capability authorization payload")
    if payload["format"] != FORMAT or payload["purpose"] != purpose:
        raise CheckpointAuthorityError("capability purpose does not match this operation")

    key_id = payload["key_id"]
    if not isinstance(key_id, str) or not _KEY_ID.fullmatch(key_id):
        raise CheckpointAuthorityError("invalid signer key id")
    key_value = trusted_public_keys.get(key_id)
    if key_value is None:
        raise CheckpointAuthorityError("signer key is not trusted")
    try:
        public_key = load_public_key(key_value) if isinstance(key_value, bytes) else key_value
        if not isinstance(public_key, Ed25519PublicKey):
            raise TypeError("trusted key must be Ed25519 public material")
        signature = _unb64(envelope["signature"])
        public_key.verify(signature, _canonical(payload))
    except (InvalidSignature, TypeError, ValueError, binascii.Error) as exc:
        raise CheckpointAuthorityError("capability signature is invalid") from exc

    expected = _json_value(dict(expected_binding))
    if payload["binding"] != expected:
        raise CheckpointAuthorityError("capability is not authorized for this backend execution")
    digest = payload["payload_sha256"]
    if not isinstance(digest, str) or not _SHA256.fullmatch(digest):
        raise CheckpointAuthorityError("invalid payload digest")
    data_b64 = envelope["data"]
    if not isinstance(data_b64, str):
        raise CheckpointAuthorityError("malformed payload bytes")
    try:
        data = _unb64(data_b64)
    except (TypeError, ValueError, binascii.Error) as exc:
        raise CheckpointAuthorityError("malformed payload bytes") from exc
    if hashlib.sha256(data).hexdigest() != digest:
        raise CheckpointAuthorityError("payload bytes do not match signed digest")
    return data


def issue_checkpoint(
    *,
    key_id: str,
    operator_private_key: Ed25519PrivateKey,
    binding: BackendBinding,
    checkpoint_bytes: bytes,
) -> bytes:
    """Sign checkpoint bytes for one exact backend execution."""
    if not isinstance(binding, BackendBinding):
        raise TypeError("binding must be a BackendBinding")
    return issue_capability(
        purpose=PURPOSE_CHECKPOINT,
        key_id=key_id,
        operator_private_key=operator_private_key,
        binding=asdict(binding),
        payload_bytes=checkpoint_bytes,
    )


def verify_checkpoint(
    envelope_bytes: bytes,
    *,
    trusted_public_keys: Mapping[str, Ed25519PublicKey | bytes],
    expected: BackendBinding,
    max_envelope_bytes: int = MAX_ENVELOPE_BYTES,
) -> bytes:
    """Verify a checkpoint capability and return opaque checkpoint bytes."""
    if not isinstance(expected, BackendBinding):
        raise TypeError("expected must be a BackendBinding")
    return verify_capability(
        envelope_bytes,
        purpose=PURPOSE_CHECKPOINT,
        trusted_public_keys=trusted_public_keys,
        expected_binding=asdict(expected),
        max_envelope_bytes=max_envelope_bytes,
    )


def issue_child_result(
    *,
    key_id: str,
    operator_private_key: Ed25519PrivateKey,
    binding: ChildResultBinding,
    result_bytes: bytes,
) -> bytes:
    """Sign child-result bytes for one child, parent and migration epoch."""
    if not isinstance(binding, ChildResultBinding):
        raise TypeError("binding must be a ChildResultBinding")
    return issue_capability(
        purpose=PURPOSE_CHILD_RESULT,
        key_id=key_id,
        operator_private_key=operator_private_key,
        binding={
            "child": asdict(binding.child),
            "parent": asdict(binding.parent),
            "migration_epoch": binding.migration_epoch,
        },
        payload_bytes=result_bytes,
    )


def verify_child_result(
    envelope_bytes: bytes,
    *,
    trusted_public_keys: Mapping[str, Ed25519PublicKey | bytes],
    expected: ChildResultBinding,
    max_envelope_bytes: int = MAX_ENVELOPE_BYTES,
) -> bytes:
    """Verify a child-result capability and return opaque result bytes."""
    if not isinstance(expected, ChildResultBinding):
        raise TypeError("expected must be a ChildResultBinding")
    return verify_capability(
        envelope_bytes,
        purpose=PURPOSE_CHILD_RESULT,
        trusted_public_keys=trusted_public_keys,
        expected_binding={
            "child": asdict(expected.child),
            "parent": asdict(expected.parent),
            "migration_epoch": expected.migration_epoch,
        },
        max_envelope_bytes=max_envelope_bytes,
    )


def _validate_purpose(purpose: str) -> None:
    if purpose not in _ALLOWED_PURPOSES:
        raise ValueError(f"purpose must be one of {sorted(_ALLOWED_PURPOSES)}")


def _json_value(value: object) -> JsonValue:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("binding contains a non-finite number")
        return value
    if isinstance(value, list):
        return _json_array(value)
    if isinstance(value, dict):
        return _json_mapping(value)
    raise ValueError(f"binding contains unsupported value type: {type(value).__name__}")


def _json_array(value: Any) -> list[JsonValue]:
    return [_json_value(item) for item in value]


def _json_mapping(value: Any) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise ValueError("binding object keys must be strings")
        result[key] = _json_value(item)
    return result


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _unb64(value: object) -> bytes:
    if not isinstance(value, str):
        raise TypeError("base64 value must be a string")
    padded = value + "=" * (-len(value) % 4)
    return base64.b64decode(padded, altchars=b"-_", validate=True)


def _strict_json(data: bytes) -> object:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise CheckpointAuthorityError("duplicate JSON key")
            result[key] = value
        return result

    try:
        return json.loads(
            data.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise CheckpointAuthorityError("capability envelope is not strict UTF-8 JSON") from exc
