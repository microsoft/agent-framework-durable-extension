# Copyright (c) Microsoft. All rights reserved.

"""Real SQLite commits across coordinator/worker process boundaries."""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Literal

import pytest

from agent_framework_durabletask._checkpoint_store import CheckpointScope, CheckpointStore, CheckpointStoreError

SCOPE = CheckpointScope("backend", "owned-hub", "a" * 64)
BUNDLE = "b" * 64
STOP = b"operator observed all old workers stopped"


def _get(
    store: CheckpointStore,
    *,
    kind: Literal["capability", "admission"] = "capability",
    bundle_sha256: str = BUNDLE,
    instance: str = "original",
    execution: str = "native-execution",
) -> bytes | None:
    return store.get(
        SCOPE,
        "g1",
        bundle_sha256=bundle_sha256,
        kind=kind,
        instance=instance,
        execution=execution,
    )


def _put(
    store: CheckpointStore,
    payload: bytes = b"signed-opaque",
    *,
    kind: Literal["capability", "admission"] = "capability",
) -> None:
    store.put(SCOPE, "g1", kind=kind, instance="original", execution="native-execution", payload=payload)


def _active(path: Path) -> CheckpointStore:
    store = CheckpointStore(path, create=True)
    store.prepare(SCOPE, "g1", bundle_sha256=BUNDLE)
    _put(store)
    store.activate(SCOPE, "g1", expected_active=None, stop_attestation=STOP)
    return store


def test_no_missing_store_success_fallback(tmp_path: Path) -> None:
    with pytest.raises(sqlite3.OperationalError):
        CheckpointStore(tmp_path / "missing.db")
    assert not (tmp_path / "missing.db").exists()


def test_staged_authority_cannot_admit_or_decode(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "authority.db", create=True)
    store.prepare(SCOPE, "g1", bundle_sha256=BUNDLE)
    _put(store)
    with pytest.raises(CheckpointStoreError, match="active"):
        _get(store)
    with pytest.raises(CheckpointStoreError, match="active"):
        _put(store, kind="admission")
    with pytest.raises(ValueError, match="attestation"):
        store.activate(SCOPE, "g1", expected_active=None, stop_attestation=b"")


def test_immutable_retries_and_exact_execution_scope(tmp_path: Path) -> None:
    store = _active(tmp_path / "authority.db")
    _put(store)
    store.activate(SCOPE, "g1", expected_active=None, stop_attestation=STOP)
    assert _get(store) == b"signed-opaque"
    assert _get(store, execution="other-execution") is None
    assert _get(store, instance="other-instance") is None
    with pytest.raises(CheckpointStoreError, match="Conflicting|requires"):
        _put(store, b"changed")
    with pytest.raises(CheckpointStoreError, match="different cold bundle"):
        store.prepare(SCOPE, "g1", bundle_sha256="c" * 64)
    with pytest.raises(CheckpointStoreError, match="attestation"):
        store.activate(SCOPE, "g1", expected_active=None, stop_attestation=b"changed")
    for scope in (
        CheckpointScope("other", SCOPE.hub, SCOPE.definition_sha256),
        CheckpointScope(SCOPE.backend, "other", SCOPE.definition_sha256),
        CheckpointScope(SCOPE.backend, SCOPE.hub, "d" * 64),
    ):
        with pytest.raises(CheckpointStoreError, match="active exact"):
            store.get(
                scope,
                "g1",
                bundle_sha256=BUNDLE,
                kind="capability",
                instance="original",
                execution="native-execution",
            )
    with pytest.raises(CheckpointStoreError, match="active exact"):
        _get(store, bundle_sha256="e" * 64)


def test_active_replacement_cas_and_retired_worker_rejection(tmp_path: Path) -> None:
    store = _active(tmp_path / "authority.db")
    store.prepare(SCOPE, "g2", bundle_sha256="c" * 64)
    with pytest.raises(CheckpointStoreError, match="changed"):
        store.activate(SCOPE, "g2", expected_active=None, stop_attestation=STOP)
    assert _get(store) == b"signed-opaque"
    store.activate(SCOPE, "g2", expected_active="g1", stop_attestation=STOP)
    with pytest.raises(CheckpointStoreError, match="active exact"):
        _get(store)
    with pytest.raises(CheckpointStoreError, match="requires"):
        _put(store, kind="admission")
    with pytest.raises(CheckpointStoreError, match="retired"):
        store.activate(SCOPE, "g1", expected_active="g2", stop_attestation=STOP)


def test_concurrent_admission_is_immutable(tmp_path: Path) -> None:
    store = _active(tmp_path / "authority.db")
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda _: _put(store, b"native-parent-fact", kind="admission"), range(12)))
    assert _get(store, kind="admission") == b"native-parent-fact"
    with pytest.raises(CheckpointStoreError, match="Conflicting"):
        _put(store, b"other-native-parent", kind="admission")


def test_entity_authority_has_no_fabricated_execution_and_survives_reopen(tmp_path: Path) -> None:
    path = tmp_path / "authority.db"
    store = CheckpointStore(path, create=True)
    store.prepare(SCOPE, "g1", bundle_sha256=BUNDLE)
    entity = "@dafx-agent@original"
    payload = b"signed-source-snapshot"
    store.put_entity_conversion(SCOPE, "g1", entity=entity, payload=payload)
    with pytest.raises(CheckpointStoreError, match="active"):
        store.get_entity_conversion(SCOPE, "g1", bundle_sha256=BUNDLE, entity=entity)
    with pytest.raises(CheckpointStoreError, match="Conflicting"):
        store.put_entity_conversion(SCOPE, "g1", entity=entity, payload=b"changed")
    with pytest.raises(CheckpointStoreError, match="confirmed native"):
        store.activate(SCOPE, "g1", expected_active=None, stop_attestation=STOP)
    store.begin_conversion(SCOPE, "g1", expected_active=None, stop_attestation=STOP)
    with pytest.raises(CheckpointStoreError, match="active"):
        store.require_active(SCOPE, "g1", bundle_sha256=BUNDLE)
    with pytest.raises(CheckpointStoreError, match="confirmed native"):
        store.activate(SCOPE, "g1", expected_active=None, stop_attestation=STOP)
    store.confirm_entity_conversion(
        SCOPE,
        "g1",
        entity=entity,
        observation=b"observed committed entity snapshot",
    )
    store.activate(SCOPE, "g1", expected_active=None, stop_attestation=STOP)
    reopened = CheckpointStore(path)
    reopened.put_entity_conversion(SCOPE, "g1", entity=entity, payload=payload)
    assert (
        reopened.get_entity_conversion(
            SCOPE,
            "g1",
            bundle_sha256=BUNDLE,
            entity=entity,
        )
        == payload
    )
    assert (
        reopened.get_entity_conversion(
            SCOPE,
            "g1",
            bundle_sha256=BUNDLE,
            entity="@dafx-agent@other",
        )
        is None
    )
    with pytest.raises(CheckpointStoreError, match="before activation"):
        reopened.put_entity_conversion(SCOPE, "g1", entity="@dafx-agent@late", payload=b"late")
    store.prepare(SCOPE, "g2", bundle_sha256="c" * 64)
    store.activate(SCOPE, "g2", expected_active="g1", stop_attestation=STOP)
    with pytest.raises(CheckpointStoreError, match="active"):
        reopened.get_entity_conversion(SCOPE, "g1", bundle_sha256=BUNDLE, entity=entity)


def test_crashed_conversion_coordinator_cannot_start_cold_or_conflicting_generation(tmp_path: Path) -> None:
    path = tmp_path / "authority.db"
    store = _active(path)
    store.prepare(SCOPE, "g2", bundle_sha256="c" * 64)
    store.put_entity_conversion(SCOPE, "g2", entity="@dafx-agent@original", payload=b"signed-snapshot")
    script = """
import os, sys
from agent_framework_durabletask._checkpoint_store import CheckpointScope, CheckpointStore
scope = CheckpointScope('backend', 'owned-hub', 'a' * 64)
store = CheckpointStore(sys.argv[1])
store.begin_conversion(scope, 'g2', expected_active='g1', stop_attestation=b'old workers stopped')
os._exit(23)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(path)],
        env=os.environ.copy(),
        check=False,
        timeout=30,
    )
    assert result.returncode == 23
    reopened = CheckpointStore(path)
    with pytest.raises(CheckpointStoreError, match="active"):
        reopened.require_active(SCOPE, "g1", bundle_sha256=BUNDLE)
    with pytest.raises(CheckpointStoreError, match="active"):
        reopened.require_active(SCOPE, "g2", bundle_sha256="c" * 64)
    reopened.require_converting(SCOPE, "g2", bundle_sha256="c" * 64)
    reopened.begin_conversion(SCOPE, "g2", expected_active="g1", stop_attestation=b"old workers stopped")
    with pytest.raises(CheckpointStoreError, match="confirmed native"):
        reopened.activate(SCOPE, "g2", expected_active=None, stop_attestation=b"old workers stopped")
    reopened.prepare(SCOPE, "g3", bundle_sha256="d" * 64)
    with pytest.raises(CheckpointStoreError, match="conversion generation"):
        reopened.activate(SCOPE, "g3", expected_active=None, stop_attestation=STOP)
    reopened.confirm_entity_conversion(
        SCOPE,
        "g2",
        entity="@dafx-agent@original",
        observation=b"post-backend-read observation",
    )
    reopened.activate(SCOPE, "g2", expected_active=None, stop_attestation=b"old workers stopped")
    reopened.require_active(SCOPE, "g2", bundle_sha256="c" * 64)
    with pytest.raises(CheckpointStoreError, match="converting"):
        reopened.require_converting(SCOPE, "g2", bundle_sha256="c" * 64)


@pytest.mark.parametrize("phase", ["before-commit", "after-commit"])
def test_process_crash_and_cold_reopen(tmp_path: Path, phase: str) -> None:
    path = tmp_path / "authority.db"
    store = _active(path)
    store.prepare(SCOPE, "g2", bundle_sha256="c" * 64)
    script = """
import os, sys
from agent_framework_durabletask._checkpoint_store import CheckpointScope, CheckpointStore
scope = CheckpointScope('backend', 'owned-hub', 'a' * 64)
store = CheckpointStore(sys.argv[1])
if sys.argv[2] == 'before-commit':
    with store._transaction() as connection:
        connection.execute("UPDATE deployments SET state='retired' WHERE state='active'")
        os._exit(23)
store.activate(scope, 'g2', expected_active='g1', stop_attestation=b'old workers stopped')
os._exit(23)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(path), phase], env=os.environ.copy(), check=False, timeout=30
    )
    assert result.returncode == 23
    reopened = CheckpointStore(path)
    if phase == "before-commit":
        assert _get(reopened) == b"signed-opaque"
        reopened.activate(SCOPE, "g2", expected_active="g1", stop_attestation=b"old workers stopped")
    else:
        with pytest.raises(CheckpointStoreError, match="active exact"):
            _get(reopened)
        reopened.activate(SCOPE, "g2", expected_active="g1", stop_attestation=b"old workers stopped")
    assert reopened.require_active(SCOPE, "g2", bundle_sha256="c" * 64).generation == "g2"
