# Copyright (c) Microsoft. All rights reserved.

"""Detached, explicitly registered DAFX entity schema conversions.

Schema admission is not dispatch isolation. Hosts must configure backend routing
before accepting historical entity names.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from ._retention import resolve_state_budget
from ._shared_state_validation import validate_schema_metadata, validate_shared_state
from ._state_capacity import StateCapacityError
from ._state_migration import migrate_legacy_state, state_snapshot_digest

_METADATA = "dafxSchema"
_CONTRACT = "dafx.agent-state"
_CURRENT = "2.0.0"
_LEGACY = ("1.0.0", "1.1.0", "1.2.0")


@dataclass(frozen=True)
class EntityMigrationContext:
    """Original entity address, never a new destination or workflow instance."""

    entity_name: str
    entity_key: str
    delivery_window_seconds: int = 60
    max_state_bytes: int | None = None

    @property
    def session_id(self) -> str:
        """Return the unchanged Core session identity."""
        return f"@{self.entity_name}@{self.entity_key}"


EntityConversion = Callable[[dict[str, Any], EntityMigrationContext], dict[str, Any]]
LegacyEvidenceProvider = Callable[[dict[str, Any], EntityMigrationContext], Mapping[str, Any]]


class EntitySchemaRegistry:
    """An immutable conversion graph with exact schema and compatibility contracts.

    Every supported version has an explicit validator. Every conversion edge must
    lead to a registered version. No semantic-version range implies compatibility.
    Converters and validators receive detached JSON. They must be deterministic and
    effect-free because an uncommitted operation can be retried after process loss.
    """

    def __init__(
        self,
        *,
        contract: str,
        current_version: str,
        validators: Mapping[str, Callable[[dict[str, Any]], None]],
        conversions: Mapping[str, tuple[str, EntityConversion]],
        unmarked_versions: tuple[str, ...] = (),
    ) -> None:
        """Freeze and validate the explicit compatibility and conversion graph."""
        if not isinstance(contract, str) or not contract.strip() or current_version not in validators:
            raise ValueError("An entity schema contract requires a registered current version.")
        self._contract = contract
        self._current_version = current_version
        self._validators = dict(validators)
        self._conversions = dict(conversions)
        self._unmarked = frozenset(unmarked_versions)
        if not self._unmarked.issubset(self._validators):
            raise ValueError("Unmarked schema versions must have registered validators.")
        for source in self._validators:
            seen: set[str] = set()
            version = source
            while version != current_version:
                if version in seen or version not in self._conversions:
                    raise ValueError("Every supported schema requires an acyclic path to the current version.")
                seen.add(version)
                version = self._conversions[version][0]
                if version not in self._validators:
                    raise ValueError("Conversion targets require registered validators.")
        if current_version in self._conversions or not self._conversions.keys() <= self._validators.keys():
            raise ValueError("Conversions cannot downgrade current state or introduce unregistered sources.")

    @property
    def contract(self) -> str:
        """Return the registered compatibility contract."""
        return self._contract

    @property
    def current_version(self) -> str:
        """Return the registered writer version."""
        return self._current_version

    def _version(self, source: dict[str, Any]) -> str:
        version = source.get("schemaVersion")
        if not isinstance(version, str) or version not in self._validators:
            raise ValueError("Unsupported DAFX entity schema version. State was not decoded or changed.")
        if _METADATA not in source:
            if version not in self._unmarked:
                raise ValueError("DAFX entity schema metadata is required.")
        else:
            validate_schema_metadata(source, contract=self.contract)
        return version

    def upgrade(self, source: dict[str, Any], context: EntityMigrationContext) -> dict[str, Any]:
        """Validate before decoding and return a detached current-version candidate."""
        version = self._version(source)
        # Strict JSON admission happens only after schema/contract admission.
        state_snapshot_digest(source)
        candidate = deepcopy(source)
        self._validators[version](deepcopy(candidate))
        while version != self.current_version:
            target, convert = self._conversions[version]
            candidate = convert(deepcopy(candidate), context)
            if not isinstance(candidate, dict) or candidate.get("schemaVersion") != target:
                raise ValueError("Entity conversion did not produce its declared target schema.")
            candidate = deepcopy(candidate)
            candidate[_METADATA] = {"contract": self.contract, "version": target}
            state_snapshot_digest(candidate)
            self._validators[target](deepcopy(candidate))
            version = target
        return candidate

    def stamp(self, state: dict[str, Any]) -> None:
        """Stamp a validated current write without changing its schema version."""
        if state.get("schemaVersion") != self.current_version:
            raise ValueError("Cannot stamp a noncurrent entity schema.")
        self._version(state)
        state[_METADATA] = {"contract": self.contract, "version": self.current_version}


def agent_entity_schema_registry(
    *, process_legacy_entities: bool = False, legacy_evidence_provider: LegacyEvidenceProvider | None = None
) -> EntitySchemaRegistry:
    """Build the supported Python agent-state registry.

    Without completion evidence, supported retained history starts completion
    protection at cutover. No historical outcomes or timestamps are invented.
    Truncation and ingestion cursor provenance still require authoritative evidence.
    The provider must return ``delivery_evidence`` and/or ``completion_evidence``
    accepted by ``migrate_legacy_state``. No storage or workflow replacement occurs.
    """
    if not isinstance(process_legacy_entities, bool):
        raise ValueError("process_legacy_entities must be a boolean.")
    if legacy_evidence_provider is not None and not process_legacy_entities:
        raise ValueError("Legacy evidence requires process_legacy_entities=True.")

    def convert(source: dict[str, Any], context: EntityMigrationContext) -> dict[str, Any]:
        budget = resolve_state_budget(context.max_state_bytes)
        supplied: Mapping[str, Any] = (
            {} if legacy_evidence_provider is None else legacy_evidence_provider(deepcopy(source), context)
        )
        if not isinstance(supplied, Mapping):
            raise ValueError("Legacy evidence provider must return a mapping.")
        evidence = dict(supplied)
        if evidence.keys() - {"delivery_evidence", "completion_evidence"}:
            raise ValueError("Legacy evidence provider returned unsupported evidence fields.")
        for field, value in evidence.items():
            if not isinstance(value, dict):
                raise ValueError(f"Supplied {field} must be a journal object, not null or another value type.")
        digest = state_snapshot_digest(source)
        migrated = migrate_legacy_state(
            source,
            source_digest=digest,
            source_session_id=context.session_id,
            migration_id=digest,
            ownership_transfer_id="in-place",
            delivery_window_seconds=context.delivery_window_seconds,
            completion_policy="historical" if "completion_evidence" in evidence else "cutover",
            **evidence,
        ).to_dict()
        # The export API's transfer binding describes a different destination.
        # In-place conversion retains the existing address and session identity.
        del migrated["data"]["migration"]
        migrated[_METADATA] = {"contract": _CONTRACT, "version": _CURRENT}
        if budget is not None:
            size = len(json.dumps(migrated, allow_nan=False))
            if size > budget:
                raise StateCapacityError(size_bytes=size, max_state_bytes=budget, floor_bytes=size, target_bytes=budget)
        return migrated

    versions = (_CURRENT, *_LEGACY) if process_legacy_entities else (_CURRENT,)
    return EntitySchemaRegistry(
        contract=_CONTRACT,
        current_version=_CURRENT,
        validators={version: validate_shared_state for version in versions},
        conversions={version: (_CURRENT, convert) for version in _LEGACY} if process_legacy_entities else {},
        unmarked_versions=versions,
    )
