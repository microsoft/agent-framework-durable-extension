# Copyright (c) Microsoft. All rights reserved.

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from agent_framework import Executor, WorkflowBuilder, WorkflowContext, handler

from agent_framework_durabletask._checkpoints import _continuation, _release
from agent_framework_durabletask._checkpoints._admission import AdmissionLedger
from agent_framework_durabletask._checkpoints._format import encode_checkpoint_document
from agent_framework_durabletask._checkpoints._registration import Registration
from agent_framework_durabletask._checkpoints._release import (
    _NATIVE_AUTO_CHECKPOINT_CONTRACT,
    _is_checkpoint_document,
    _restore_native_checkpoint,
    register_current_handlers,
)
from agent_framework_durabletask._checkpoints._target import CONTRACT
from agent_framework_durabletask._workflows.serialization import serialize_value


class _Start(Executor):
    @handler
    async def run(self, value: str, ctx: WorkflowContext[str]) -> None:
        await cast(Any, ctx).yield_output(value)


def _candidate():
    workflow = WorkflowBuilder(name="native-can-release", start_executor=_Start(id="start")).build()
    semantic = AdmissionLedger.initial_state(
        workflow,
        "first",
        address={
            "root_instance_id": "owned-instance",
            "root_workflow_name": workflow.name,
            "request_path_prefix": "",
        },
        is_child=False,
        event_streaming=False,
    )
    definition = "a" * 64
    encoded = encode_checkpoint_document(
        {
            "contract": "checkpoint-only-start/1",
            "state": {
                "contract": _NATIVE_AUTO_CHECKPOINT_CONTRACT,
                "definition_sha256": definition,
                "workflow_name": workflow.name,
                "semantic": serialize_value(semantic),
            },
        }
    )
    return workflow, definition, json.loads(encoded), encoded.decode("utf-8")


def _registration(definition: str) -> Registration:
    return Registration("native_can_release:build", (), definition, Path("unused"))


def _context(input_sha256: str, *, continuation=True, execution_id="successor-execution", lineage_successor=None):
    return SimpleNamespace(
        instance_id="owned-instance",
        execution_id=execution_id,
        parent_instance_id=None,
        native_continuation_profile="native-can-v1",
        native_continuation=(
            SimpleNamespace(
                predecessor_execution_id="predecessor-execution",
                successor_execution_id=lineage_successor or execution_id,
                input_sha256=input_sha256,
            )
            if continuation
            else None
        ),
    )


def test_native_checkpoint_restores_only_for_matching_backend_continuation():
    workflow, definition, value, input_wire = _candidate()

    restored = _restore_native_checkpoint(
        value,
        _context(hashlib.sha256(input_wire.encode("utf-8")).hexdigest()),
        _registration(definition),
        workflow,
    )

    assert restored["contract"] == CONTRACT
    assert restored["phase"] == "prepare"
    assert restored["pending"]["start"] == [("first", ["__workflow_start__"])]


@pytest.mark.parametrize(
    ("continuation", "lineage_successor", "definition", "message"),
    [
        (False, None, "a" * 64, "verified native CAN fact"),
        (True, None, "c" * 64, "definition or semantic envelope"),
        (True, "different-successor", "a" * 64, "verified native CAN fact"),
    ],
)
def test_native_checkpoint_rejects_root_restart_or_mismatched_successor(
    continuation, lineage_successor, definition, message
):
    workflow, _, value, input_wire = _candidate()
    context = _context(
        hashlib.sha256(input_wire.encode("utf-8")).hexdigest(),
        continuation=continuation,
        lineage_successor=lineage_successor,
    )

    with pytest.raises(ValueError, match=message):
        _restore_native_checkpoint(value, context, _registration(definition), workflow)


def test_native_checkpoint_emission_is_not_available_to_migration_workers():
    with pytest.raises(ValueError, match="new-workflows-only"):
        register_current_handlers(
            cast(Any, None),
            cast(Any, None),
            backend_id="test",
            hub="test",
            keys={},
            store=cast(Any, None),
            generation="test",
            bundle_sha256="a" * 64,
            bundle_directory="unused",
            emission_profile="native-can-v1",
        )


def test_native_emission_without_proposal_capability_fails_before_registration(monkeypatch):
    effects = []

    class Sink:
        def add_activity(self, function):
            effects.append("activity")

        def add_entity(self, entity):
            effects.append("entity")

        def add_orchestrator(self, function, *, replay_profile=None):
            effects.append("orchestrator")

    monkeypatch.setattr(_release, "durabletask_task", SimpleNamespace(NATIVE_CONTINUATION_PROFILE=None))

    with pytest.raises(RuntimeError, match="matching SDK/provider proposal capability"):
        register_current_handlers(
            cast(Any, Sink()),
            cast(Any, object()),
            backend_id="test",
            hub="test",
            keys={},
            store=cast(Any, object()),
            generation="test",
            bundle_sha256="a" * 64,
            bundle_directory="unused",
            admission_profile="new-workflows-only",
            emission_profile="native-can-v1",
        )

    assert effects == []


def test_native_emission_requires_dispatch_capability_before_registration(monkeypatch):
    effects = []

    class PartialContext:
        native_continuation_profile = None
        native_continuation = None

    class Sink:
        def add_activity(self, function):
            effects.append("activity")

        def add_entity(self, entity):
            effects.append("entity")

        def add_orchestrator(self, function, *, replay_profile=None):
            effects.append("orchestrator")

    monkeypatch.setattr(
        _release,
        "durabletask_task",
        SimpleNamespace(
            NATIVE_CONTINUATION_PROFILE="native-can-v1",
            OrchestrationContext=PartialContext,
        ),
    )

    with pytest.raises(RuntimeError, match="matching SDK/provider proposal capability"):
        register_current_handlers(
            cast(Any, Sink()),
            cast(Any, object()),
            backend_id="test",
            hub="test",
            keys={},
            store=cast(Any, object()),
            generation="test",
            bundle_sha256="a" * 64,
            bundle_directory="unused",
            admission_profile="new-workflows-only",
            emission_profile="native-can-v1",
        )

    assert effects == []


def test_native_emission_profile_reaches_normal_semantic_continuation(monkeypatch):
    definition = "a" * 64
    received = {}

    def run(ctx, workflow, checkpoint, *, automatic_emission=None, **native_handles):
        received["automatic_emission"] = automatic_emission
        received["native_handles"] = native_handles
        yield "continued"

    monkeypatch.setattr(_continuation, "run", run)
    continuation = _continuation.continue_checkpoint(
        cast(Any, None),
        cast(Any, None),
        {"phase": "prepare"},
        automatic_emission={"definition_sha256": definition},
    )

    assert next(continuation) == "continued"
    assert received == {
        "automatic_emission": {"definition_sha256": definition},
        "native_handles": {},
    }


def test_automatic_checkpoint_start_is_recognized_as_copied_checkpoint_input():
    assert _is_checkpoint_document(
        {
            "contract": "checkpoint-only-start/1",
            "state": {
                "contract": _NATIVE_AUTO_CHECKPOINT_CONTRACT,
                "definition_sha256": "a" * 64,
                "workflow_name": "native-can-release",
                "semantic": {},
            },
        }
    )
