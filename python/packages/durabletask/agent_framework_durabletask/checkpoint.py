# Copyright (c) Microsoft. All rights reserved.

"""Experimental checkpoint-only transition APIs for the pinned Python release.

These proposed APIs require the matching SDK/host candidate. They are not an
all-states certification. Import/capture runs separately from current-only
workers. Ordinary framework entry points keep their existing defaults.
"""

from ._checkpoint_store import CheckpointDeployment, CheckpointScope, CheckpointStore, CheckpointStoreError
from ._checkpoints._authority import (
    BackendBinding,
    CheckpointAuthorityError,
    issue_checkpoint,
    load_public_key,
    public_key_bytes,
    verify_checkpoint,
)
from ._checkpoints._bundle import build_checkpoint_bundle, verify_checkpoint_bundle
from ._checkpoints._registration import Registration, RegistrationError, validate_registration
from ._checkpoints._release import register_current_worker

__all__ = [
    "BackendBinding",
    "CheckpointAuthorityError",
    "CheckpointDeployment",
    "CheckpointScope",
    "CheckpointStore",
    "CheckpointStoreError",
    "Registration",
    "RegistrationError",
    "build_checkpoint_bundle",
    "issue_checkpoint",
    "load_public_key",
    "public_key_bytes",
    "register_current_worker",
    "validate_registration",
    "verify_checkpoint",
    "verify_checkpoint_bundle",
]
