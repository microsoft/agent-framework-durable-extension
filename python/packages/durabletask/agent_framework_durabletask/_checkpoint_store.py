# Copyright (c) Microsoft. All rights reserved.

"""Transactional deployment facts for the experimental checkpoint transition.

This is an application database, not a Durable Task backend database. Deploy it
on an operator-protected persistent local volume. SQLite locking must work on
that volume. This implementation does not support network filesystems, multiple
machines, automatic old-worker fencing, or protection from privileged reset.

Only the deployment coordinator prepares and activates generations. Old workers
must already have stopped before activation. Their stop attestation is an
operator assertion, not a backend receipt. Workers validate signed checkpoints
separately and record only admissions derived from authenticated native dispatch.
No application input or external event is authority to call these APIs.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


class CheckpointStoreError(ValueError):
    """A conflicting, stale, or unavailable deployment fact."""


@dataclass(frozen=True)
class CheckpointScope:
    """Deployment-owned backend, hub and exact trusted definition identity."""

    backend: str
    hub: str
    definition_sha256: str

    def __post_init__(self) -> None:
        """Reject missing native/deployment binding rather than defaulting it."""
        _identity(self.backend)
        _identity(self.hub)
        _digest(self.definition_sha256)

    @property
    def key(self) -> str:
        """Return the canonical storage key, not a historical fingerprint."""
        return json.dumps([self.backend, self.hub, self.definition_sha256], separators=(",", ":"))


@dataclass(frozen=True)
class CheckpointDeployment:
    """An immutable generation and its separately committed activation state."""

    generation: str
    bundle_sha256: str
    state: Literal["prepared", "converting", "active", "retired"]
    stop_attestation: bytes | None


def _identity(value: str) -> None:
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ValueError("A nonempty canonical identity is required.")


def _digest(value: str) -> None:
    if not isinstance(value, str) or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise ValueError("A lowercase SHA256 digest is required.")


class CheckpointStore:
    """Persist immutable authority and admissions with crash-safe local commits.

    The directory and database are deployment-owned. This API is not a security
    sandbox for customer Python code. FULL synchronous WAL commits provide
    process-crash durability subject to the filesystem's durability guarantees.
    A commit acknowledged before a coordinator crash may be retried identically.
    There is no transaction spanning this store and the orchestration backend.
    """

    def __init__(self, path: str | Path, *, create: bool = False) -> None:
        """Open existing storage, or explicitly initialize it as the operator."""
        self.path = Path(path).resolve()
        if create:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self._connect(existing=False) as connection:
                connection.execute("PRAGMA journal_mode=WAL")
                connection.executescript("""
                    CREATE TABLE IF NOT EXISTS checkpoint_store_version (
                        version INTEGER PRIMARY KEY CHECK (version = 2)
                    );
                    INSERT OR IGNORE INTO checkpoint_store_version VALUES (2);
                    CREATE TABLE IF NOT EXISTS deployments (
                        scope TEXT NOT NULL, generation TEXT NOT NULL,
                        bundle TEXT NOT NULL, state TEXT NOT NULL,
                        stop_attestation BLOB,
                        PRIMARY KEY (scope, generation),
                        CHECK (state IN ('prepared', 'converting', 'active', 'retired'))
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS one_active_checkpoint_generation
                        ON deployments(scope) WHERE state IN ('active', 'converting');
                    CREATE TABLE IF NOT EXISTS checkpoint_facts (
                        scope TEXT NOT NULL, generation TEXT NOT NULL,
                        kind TEXT NOT NULL, instance TEXT NOT NULL,
                        execution TEXT NOT NULL, payload BLOB NOT NULL,
                        PRIMARY KEY (scope, generation, kind, instance, execution),
                        FOREIGN KEY (scope, generation) REFERENCES deployments(scope, generation),
                        CHECK (kind IN ('capability', 'admission'))
                    );
                    CREATE TABLE IF NOT EXISTS entity_conversion_capabilities (
                        scope TEXT NOT NULL, generation TEXT NOT NULL,
                        entity TEXT NOT NULL, payload BLOB NOT NULL, confirmation BLOB,
                        PRIMARY KEY (scope, generation, entity),
                        FOREIGN KEY (scope, generation) REFERENCES deployments(scope, generation)
                    );
                """)
        with self._connect() as connection:
            versions = connection.execute("SELECT version FROM checkpoint_store_version").fetchall()
            if versions != [(2,)]:
                raise CheckpointStoreError("Unsupported checkpoint store schema.")

    @contextmanager
    def _connect(self, *, existing: bool = True) -> Generator[sqlite3.Connection, None, None]:
        mode = "rw" if existing else "rwc"
        connection = sqlite3.connect(f"{self.path.as_uri()}?mode={mode}", uri=True, timeout=30)
        try:
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA synchronous=FULL")
            with connection:
                yield connection
        finally:
            connection.close()

    @contextmanager
    def _transaction(self) -> Generator[sqlite3.Connection, None, None]:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            yield connection

    def prepare(self, scope: CheckpointScope, generation: str, *, bundle_sha256: str) -> None:
        """Idempotently prepare an exact cold bundle before any activation."""
        _identity(generation)
        _digest(bundle_sha256)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT bundle FROM deployments WHERE scope=? AND generation=?", (scope.key, generation)
            ).fetchone()
            if row is not None:
                if row[0] != bundle_sha256:
                    raise CheckpointStoreError("Generation already binds a different cold bundle.")
                return
            connection.execute(
                "INSERT INTO deployments VALUES (?, ?, ?, 'prepared', NULL)",
                (scope.key, generation, bundle_sha256),
            )

    def activate(
        self,
        scope: CheckpointScope,
        generation: str,
        *,
        expected_active: str | None,
        stop_attestation: bytes,
    ) -> None:
        """CAS activation after an explicit operator assertion that old workers stopped.

        Retiring the previous generation and activating the new one commit
        atomically. This does not stop workers or unsuspend native executions.
        """
        _identity(generation)
        if expected_active is not None:
            _identity(expected_active)
        if not isinstance(stop_attestation, bytes) or not stop_attestation:
            raise ValueError("A nonempty operator stop attestation is required.")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT state, stop_attestation FROM deployments WHERE scope=? AND generation=?",
                (scope.key, generation),
            ).fetchone()
            if row is None:
                raise CheckpointStoreError("Generation must be prepared before activation.")
            if row[0] == "active":
                if row[1] != stop_attestation:
                    raise CheckpointStoreError("Activation retry changed its stop attestation.")
                return
            if row[0] not in ("prepared", "converting"):
                raise CheckpointStoreError("A retired generation cannot be reactivated.")
            pending = connection.execute(
                "SELECT COUNT(*), COUNT(confirmation) FROM entity_conversion_capabilities "
                "WHERE scope=? AND generation=?",
                (scope.key, generation),
            ).fetchone()
            if pending[0] and (row[0] != "converting" or pending[0] != pending[1]):
                raise CheckpointStoreError(
                    "Entity conversions require confirmed native observations before activation."
                )
            converting = connection.execute(
                "SELECT generation FROM deployments WHERE scope=? AND state='converting'",
                (scope.key,),
            ).fetchone()
            if converting is not None and converting[0] != generation:
                raise CheckpointStoreError("Another conversion generation is in progress.")
            if row[0] == "converting" and row[1] != stop_attestation:
                raise CheckpointStoreError("Activation changed the conversion stop attestation.")
            active = connection.execute(
                "SELECT generation FROM deployments WHERE scope=? AND state='active'", (scope.key,)
            ).fetchone()
            if (active[0] if active else None) != expected_active:
                raise CheckpointStoreError("Active generation changed before activation.")
            connection.execute("UPDATE deployments SET state='retired' WHERE scope=? AND state='active'", (scope.key,))
            connection.execute(
                "UPDATE deployments SET state='active', stop_attestation=? WHERE scope=? AND generation=?",
                (stop_attestation, scope.key, generation),
            )

    def begin_conversion(
        self,
        scope: CheckpointScope,
        generation: str,
        *,
        expected_active: str | None,
        stop_attestation: bytes,
    ) -> None:
        """CAS into a durable conversion-only phase after stopping source workers.

        Current-only orchestration workers reject this phase. A coordinator
        crash may restart conversion, but cannot accidentally activate a cold
        worker before the native entity results have been observed.
        """
        _identity(generation)
        if expected_active is not None:
            _identity(expected_active)
        if not isinstance(stop_attestation, bytes) or not stop_attestation:
            raise ValueError("A nonempty operator stop attestation is required.")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT state, stop_attestation FROM deployments WHERE scope=? AND generation=?",
                (scope.key, generation),
            ).fetchone()
            if row is None:
                raise CheckpointStoreError("Generation must be prepared before conversion.")
            if row[0] == "converting":
                if row[1] != stop_attestation:
                    raise CheckpointStoreError("Conversion retry changed its stop attestation.")
                return
            if row[0] != "prepared":
                raise CheckpointStoreError("Only a prepared generation may begin conversion.")
            current = connection.execute(
                "SELECT generation, state FROM deployments WHERE scope=? AND state IN ('active', 'converting')",
                (scope.key,),
            ).fetchone()
            if current is not None and current[1] == "converting":
                raise CheckpointStoreError("Another conversion generation is in progress.")
            if (current[0] if current else None) != expected_active:
                raise CheckpointStoreError("Active generation changed before conversion.")
            connection.execute("UPDATE deployments SET state='retired' WHERE scope=? AND state='active'", (scope.key,))
            connection.execute(
                "UPDATE deployments SET state='converting', stop_attestation=? WHERE scope=? AND generation=?",
                (stop_attestation, scope.key, generation),
            )

    def require_converting(
        self,
        scope: CheckpointScope,
        generation: str,
        *,
        bundle_sha256: str,
    ) -> None:
        """Reject a converter outside its exact conversion-only generation."""
        _identity(generation)
        _digest(bundle_sha256)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT bundle, state FROM deployments WHERE scope=? AND generation=?",
                (scope.key, generation),
            ).fetchone()
        if row != (bundle_sha256, "converting"):
            raise CheckpointStoreError("Entity converter requires the exact converting generation.")

    def confirm_entity_conversion(
        self,
        scope: CheckpointScope,
        generation: str,
        *,
        entity: str,
        observation: bytes,
    ) -> None:
        """Record an operator's post-commit backend observation, not a native receipt.

        Stop the converter, await its actual native result and read matching
        semantic entity state before recording this protected observation.
        Writing from inside an uncommitted entity invocation is not sufficient.
        """
        _identity(generation)
        _identity(entity)
        if not isinstance(observation, bytes) or not observation:
            raise ValueError("A nonempty observed native conversion result is required.")
        with self._transaction() as connection:
            deployment = connection.execute(
                "SELECT state FROM deployments WHERE scope=? AND generation=?",
                (scope.key, generation),
            ).fetchone()
            if deployment != ("converting",):
                raise CheckpointStoreError("Confirmation requires a converting generation.")
            row = connection.execute(
                "SELECT confirmation FROM entity_conversion_capabilities WHERE scope=? AND generation=? AND entity=?",
                (scope.key, generation, entity),
            ).fetchone()
            if row is None:
                raise CheckpointStoreError("Entity conversion has no staged authority.")
            if row[0] is not None and row[0] != observation:
                raise CheckpointStoreError("Conflicting immutable entity conversion observation.")
            connection.execute(
                "UPDATE entity_conversion_capabilities SET confirmation=? WHERE scope=? AND generation=? AND entity=?",
                (observation, scope.key, generation, entity),
            )

    def require_active(self, scope: CheckpointScope, generation: str, *, bundle_sha256: str) -> CheckpointDeployment:
        """Reject a stale, unprepared or byte-mismatched worker before decoding."""
        _identity(generation)
        _digest(bundle_sha256)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT bundle, state, stop_attestation FROM deployments WHERE scope=? AND generation=?",
                (scope.key, generation),
            ).fetchone()
        if row is None or row[0] != bundle_sha256 or row[1] != "active":
            raise CheckpointStoreError("Worker generation is not the active exact cold bundle.")
        return CheckpointDeployment(generation, row[0], "active", row[2])

    def put(
        self,
        scope: CheckpointScope,
        generation: str,
        *,
        kind: Literal["capability", "admission"],
        instance: str,
        execution: str,
        payload: bytes,
    ) -> None:
        """Commit opaque immutable bytes without fabricating native completion facts.

        Capability signature verification belongs to the authority consumer.
        Admissions must come only from authenticated acquired execution context.
        Capabilities are staged before activation. Admissions require an active
        generation, checked in the same transaction as the immutable write.
        """
        for identity in (generation, instance, execution):
            _identity(identity)
        if kind not in ("capability", "admission"):
            raise ValueError("Unsupported checkpoint fact kind.")
        if not isinstance(payload, bytes) or not payload:
            raise ValueError("A nonempty opaque payload is required.")
        with self._transaction() as connection:
            deployment = connection.execute(
                "SELECT state FROM deployments WHERE scope=? AND generation=?", (scope.key, generation)
            ).fetchone()
            required = "prepared" if kind == "capability" else "active"
            row = connection.execute(
                "SELECT payload FROM checkpoint_facts "
                "WHERE scope=? AND generation=? AND kind=? AND instance=? AND execution=?",
                (scope.key, generation, kind, instance, execution),
            ).fetchone()
            if (deployment is None or deployment[0] != required) and (
                row is None or row[0] != payload or deployment is None or deployment[0] == "retired"
            ):
                raise CheckpointStoreError(f"Writing {kind} requires a {required} generation.")
            if row is not None:
                if row[0] != payload:
                    raise CheckpointStoreError("Conflicting immutable execution fact.")
                return
            connection.execute(
                "INSERT INTO checkpoint_facts VALUES (?, ?, ?, ?, ?, ?)",
                (scope.key, generation, kind, instance, execution, payload),
            )

    def get(
        self,
        scope: CheckpointScope,
        generation: str,
        *,
        bundle_sha256: str,
        kind: Literal["capability", "admission"],
        instance: str,
        execution: str,
    ) -> bytes | None:
        """Read only through the active exact bundle in one database snapshot."""
        for identity in (generation, instance, execution):
            _identity(identity)
        _digest(bundle_sha256)
        if kind not in ("capability", "admission"):
            raise ValueError("Unsupported checkpoint fact kind.")
        with self._connect() as connection:
            connection.execute("BEGIN")
            deployment = connection.execute(
                "SELECT bundle, state FROM deployments WHERE scope=? AND generation=?",
                (scope.key, generation),
            ).fetchone()
            if deployment != (bundle_sha256, "active"):
                raise CheckpointStoreError("Worker generation is not the active exact cold bundle.")
            row = connection.execute(
                "SELECT payload FROM checkpoint_facts "
                "WHERE scope=? AND generation=? AND kind=? AND instance=? AND execution=?",
                (scope.key, generation, kind, instance, execution),
            ).fetchone()
        return row[0] if row else None

    def put_entity_conversion(
        self,
        scope: CheckpointScope,
        generation: str,
        *,
        entity: str,
        payload: bytes,
    ) -> None:
        """Stage immutable conversion authority for an exact native entity address.

        Entities have no orchestration execution identity. This separate key
        space never labels a snapshot digest as a native execution or receipt.
        The signed payload authenticates the original source snapshot digest.
        """
        _identity(generation)
        _identity(entity)
        if not isinstance(payload, bytes) or not payload:
            raise ValueError("A nonempty opaque payload is required.")
        with self._transaction() as connection:
            deployment = connection.execute(
                "SELECT state FROM deployments WHERE scope=? AND generation=?",
                (scope.key, generation),
            ).fetchone()
            row = connection.execute(
                "SELECT payload FROM entity_conversion_capabilities WHERE scope=? AND generation=? AND entity=?",
                (scope.key, generation, entity),
            ).fetchone()
            if deployment is None or deployment[0] == "retired":
                raise CheckpointStoreError("Entity conversion requires a prepared generation.")
            if row is not None:
                if row[0] != payload:
                    raise CheckpointStoreError("Conflicting immutable entity conversion authority.")
                return
            if deployment[0] != "prepared":
                raise CheckpointStoreError("Entity conversion authority must be staged before activation.")
            connection.execute(
                "INSERT INTO entity_conversion_capabilities (scope, generation, entity, payload) VALUES (?, ?, ?, ?)",
                (scope.key, generation, entity, payload),
            )

    def get_entity_conversion(
        self,
        scope: CheckpointScope,
        generation: str,
        *,
        bundle_sha256: str,
        entity: str,
    ) -> bytes | None:
        """Read entity conversion authority through the active exact deployment."""
        _identity(generation)
        _identity(entity)
        _digest(bundle_sha256)
        with self._connect() as connection:
            connection.execute("BEGIN")
            active = connection.execute(
                "SELECT bundle, state FROM deployments WHERE scope=? AND generation=?",
                (scope.key, generation),
            ).fetchone()
            if active is None or active[0] != bundle_sha256 or active[1] not in ("active", "converting"):
                raise CheckpointStoreError("Worker generation is not the active exact cold bundle.")
            row = connection.execute(
                "SELECT payload FROM entity_conversion_capabilities WHERE scope=? AND generation=? AND entity=?",
                (scope.key, generation, entity),
            ).fetchone()
        return row[0] if row else None
