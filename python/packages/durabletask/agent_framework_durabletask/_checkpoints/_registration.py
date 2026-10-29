# Copyright (c) Microsoft. All rights reserved.

"""Trusted deployment registration. This module does not import customer types."""

from __future__ import annotations

import hashlib
import importlib
import importlib.abc
import importlib.util
import inspect
import json
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from importlib.machinery import ModuleSpec
from pathlib import Path
from types import ModuleType
from typing import Any

from ._types import (
    JsonObject,
    is_json_object,
    is_list,
    is_list_or_tuple,
    is_string_keyed_dict,
)

CONTRACT = "trusted-workflow-registration/1"
_NAME = re.compile(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*", re.ASCII)
_HEX = re.compile(r"[0-9a-f]{64}")


class RegistrationError(ValueError):
    """Invalid registration, changed deployment, or unsupported continuation."""


def _unique(pairs: list[tuple[str, Any]]) -> JsonObject:
    result: JsonObject = {}
    for key, value in pairs:
        if key in result:
            raise RegistrationError(f"Duplicate registration key: {key}")
        result[key] = value
    return result


@dataclass(frozen=True)
class Source:
    module: str
    path: str
    sha256: str
    package: bool


@dataclass(frozen=True)
class Registration:
    entrypoint: str
    sources: tuple[Source, ...]
    definition_digest: str
    _manifest: Path = field(repr=False)

    def load_workflow(self, *, continuation_profile: str = "activity-only") -> Any:
        """Load trusted deployment code after its bundle and release are verified.

        Customer code is trusted arbitrary Python, not a sandbox. Declared bytes
        are rechecked and compiled directly so stale pyc files cannot substitute
        a different implementation. All declared modules are loaded while the
        verified finder is active so deferred handler imports remain available.
        """
        fresh = validate_registration(self._manifest)
        if fresh != self:
            raise RegistrationError("Registration changed after validation.")
        contents = _read_sources(self._manifest.parent, self.sources)
        finder = _ManifestFinder(self.sources, contents, self._manifest.parent)
        for source in self.sources:
            existing = sys.modules.get(source.module)
            if existing is not None and (
                getattr(existing, "__registration_source_sha256__", None) != source.sha256
                or Path(getattr(existing, "__file__", "")).resolve() != (self._manifest.parent / source.path).resolve()
            ):
                raise RegistrationError(f"Unpinned module already loaded: {source.module}")
        sys.meta_path.insert(0, finder)
        try:
            module_name, factory_name = self.entrypoint.split(":")
            module = importlib.import_module(module_name)
            factory = getattr(module, factory_name, None)
            if not callable(factory) or inspect.iscoroutinefunction(factory):
                raise RegistrationError("Entrypoint must be a synchronous callable factory.")
            try:
                inspect.signature(factory).bind()
            except (TypeError, ValueError) as error:
                raise RegistrationError("Factory must accept no arguments.") from error
            workflow = factory()
            # The verified finder is scoped to construction. Load declared helpers now so
            # handler-local imports resolve from the pinned module cache after it is removed.
            for source in self.sources:
                importlib.import_module(source.module)
            validate_workflow(workflow, continuation_profile=continuation_profile)
            return workflow
        finally:
            sys.meta_path.remove(finder)


class _ManifestFinder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def __init__(self, sources: Sequence[Source], contents: dict[str, bytes], root: Path) -> None:
        self.sources = {source.module: source for source in sources}
        self.contents = contents
        self.root = root

    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None = None,
        target: ModuleType | None = None,
    ) -> ModuleSpec | None:
        source = self.sources.get(fullname)
        if source is None:
            return None
        return importlib.util.spec_from_loader(
            fullname,
            self,
            origin=str(self.root / source.path),
            is_package=source.package,
        )

    def create_module(self, spec: ModuleSpec) -> None:
        return None

    def exec_module(self, module: ModuleType) -> None:
        source = self.sources[module.__name__]
        path = self.root / source.path
        module.__file__ = str(path)
        if source.package:
            module.__path__ = [str(path.parent)]
        # Ruff S102: the bytes are hash-verified against the trusted manifest immediately before
        # this loader compiles them into the registered module namespace.
        exec(compile(self.contents[source.module], str(path), "exec"), module.__dict__)  # noqa: S102
        module.__dict__["__registration_source_sha256__"] = source.sha256


def _read_sources(root: Path, sources: Sequence[Source]) -> dict[str, bytes]:
    contents: dict[str, bytes] = {}
    for source in sources:
        path = root / source.path
        try:
            resolved = path.resolve(strict=True)
            if not resolved.is_relative_to(root.resolve()):
                raise RegistrationError(f"Source escapes registration root: {source.path}")
            content = resolved.read_bytes()
        except OSError as error:
            raise RegistrationError(f"Missing or unreadable source: {source.path}") from error
        if hashlib.sha256(content).hexdigest() != source.sha256:
            raise RegistrationError(f"Source digest mismatch: {source.path}")
        contents[source.module] = content
    return contents


def validate_registration(manifest_path: str | Path) -> Registration:
    """Hash only deployment bytes. No module discovery or customer import occurs.

    Digest = SHA256(UTF8(JSON(manifest, sort_keys=True, separators=(",", ":"),
    ensure_ascii=True))). The entire strict manifest participates, including the
    module:factory entrypoint and every declared source path and content digest.
    """
    path = Path(manifest_path).resolve()
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RegistrationError("Cannot read registration JSON.") from error
    if not is_json_object(manifest) or set(manifest) != {"contract", "entrypoint", "sources"}:
        raise RegistrationError("Registration requires exactly contract, entrypoint, sources.")
    if manifest["contract"] != CONTRACT:
        raise RegistrationError("Unsupported registration contract.")
    entry = manifest["entrypoint"]
    if not isinstance(entry, str) or entry.count(":") != 1:
        raise RegistrationError("Entrypoint must be module:factory.")
    module, factory = entry.split(":")
    if not _NAME.fullmatch(module) or not factory.isidentifier() or "." in factory:
        raise RegistrationError("Entrypoint must be module:factory.")
    records = manifest["sources"]
    if not is_json_object(records) or not records:
        raise RegistrationError("A nonempty declared module source manifest is required.")
    sources: list[Source] = []
    names: set[str] = set()
    paths: set[str] = set()
    for name, record in sorted(records.items()):
        if not _NAME.fullmatch(name) or name.lower() in names:
            raise RegistrationError("Invalid or case-colliding source module.")
        if not is_json_object(record) or set(record) != {"path", "sha256"}:
            raise RegistrationError("Source records require exactly path and sha256.")
        relative, digest = record["path"], record["sha256"]
        if not isinstance(relative, str) or not isinstance(digest, str) or not _HEX.fullmatch(digest):
            raise RegistrationError("Source requires a relative .py path and lowercase SHA256.")
        # POSIX separators keep the manifest portable across Windows and Unix hosts.
        ordinary = "/".join(name.split(".")) + ".py"
        package = "/".join(name.split(".")) + "/__init__.py"
        if relative not in (ordinary, package) or relative.lower() in paths:
            raise RegistrationError("Source path must match its module name.")
        names.add(name.lower())
        paths.add(relative.lower())
        sources.append(Source(name, relative, digest, relative == package))
    by_name = {source.module: source for source in sources}
    if module not in by_name:
        raise RegistrationError("Entrypoint module is absent from source manifest.")
    for source in sources:
        parts = source.module.split(".")
        for index in range(1, len(parts)):
            parent = by_name.get(".".join(parts[:index]))
            if parent is None or not parent.package:
                raise RegistrationError("Every parent package must be declared with __init__.py.")
    _read_sources(path.parent, sources)
    canonical = json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return Registration(entry, tuple(sources), hashlib.sha256(canonical).hexdigest(), path)


def _sync(callback: object) -> bool:
    if callback is None:
        return True
    if not callable(callback):
        return False
    call_method = type(callback).__call__
    return not inspect.iscoroutinefunction(callback) and not inspect.iscoroutinefunction(call_method)


def validate_workflow(workflow: Any, *, continuation_profile: str = "activity-only") -> Any:
    """Validate a deployment-selected profile, never a payload-selected profile."""
    if continuation_profile not in ("activity-only", "published-checkpoint"):
        raise RegistrationError("Unsupported deployment continuation profile.")
    if continuation_profile == "published-checkpoint":
        from agent_framework_durabletask._workflows.registration import collect_hosted_workflows

        _validate_workflow_graph(workflow, allow_hosted=True)
        for graph in collect_hosted_workflows(workflow):
            if graph is not workflow:
                _validate_workflow_graph(graph, allow_hosted=True)
    else:
        _validate_workflow_graph(workflow, allow_hosted=False)
    return workflow


def _validate_workflow_graph(workflow: Any, *, allow_hosted: bool) -> Any:
    from agent_framework import AgentExecutor, Executor, Workflow, WorkflowExecutor
    from agent_framework._workflows._edge import (
        FanInEdgeGroup,
        FanOutEdgeGroup,
        InternalEdgeGroup,
        SingleEdgeGroup,
        SwitchCaseEdgeGroup,
    )

    if type(workflow) is not Workflow:
        raise RegistrationError("Factory must return an original Core Workflow object.")
    if (
        not isinstance(workflow.name, str)
        or not workflow.name
        or type(workflow.max_iterations) is not int
        or workflow.max_iterations < 1
    ):
        raise RegistrationError("Workflow requires a name and positive max_iterations.")
    if workflow.start_executor_id not in workflow.executors:
        raise RegistrationError("Workflow start executor is missing.")
    for name, executor in workflow.executors.items():
        if not isinstance(executor, Executor) or executor.id != name:
            raise RegistrationError("Executor registry does not match executor IDs.")
        if not allow_hosted and isinstance(executor, (AgentExecutor, WorkflowExecutor)):
            raise RegistrationError(
                "AgentExecutor and WorkflowExecutor require an owning-surface continuation "
                "profile. This registration profile supports activity executors only."
            )
    for group in workflow.edge_groups:
        if type(group) not in (
            SingleEdgeGroup,
            FanOutEdgeGroup,
            FanInEdgeGroup,
            SwitchCaseEdgeGroup,
            InternalEdgeGroup,
        ):
            raise RegistrationError(f"Unsupported concrete edge group: {type(group).__name__}")
        if type(group) is InternalEdgeGroup:
            if (
                len(group.edges) != 1
                or group.edges[0].target_id not in workflow.executors
                or group.edges[0].source_id != "internal:" + group.edges[0].target_id
                or group.edges[0].has_condition
            ):
                raise RegistrationError("Unsupported internal edge group.")
            continue
        if any(name not in workflow.executors for name in (*group.source_executor_ids, *group.target_executor_ids)):
            raise RegistrationError("Edge references an unknown executor.")
        if isinstance(group, FanInEdgeGroup) and (
            len(group.target_executor_ids) != 1 or any(edge.has_condition for edge in group.edges)
        ):
            raise RegistrationError("Only unconditional single-target fan-in is supported.")
        if isinstance(group, (FanOutEdgeGroup, SwitchCaseEdgeGroup)):
            if not _sync(group.selection_func):
                raise RegistrationError("Async selection callbacks are unsupported.")
            for case in getattr(group, "cases", []):
                if not _sync(getattr(case, "condition", None)):
                    raise RegistrationError("Async switch conditions are unsupported.")
        # The edge API exposes only has_condition, not the callback needed for sync validation.
        if any(not _sync(edge._condition) for edge in group.edges):  # pyright: ignore[reportPrivateUsage]
            raise RegistrationError("Async edge conditions are unsupported.")
    return workflow


def validate_checkpoint(workflow: Any, checkpoint: JsonObject) -> JsonObject:
    """Preflight trusted, already decoded semantic state before any scheduling.

    This narrower profile accepts drained prepare cuts only. HITL and child
    joins need additional admission/result ownership and are not silently
    reinterpreted here.
    """
    validate_workflow(workflow)
    from ._target import CONTRACT as CHECKPOINT_CONTRACT
    from ._target import fan_bindings

    required = {
        "contract",
        "routingPolicy",
        "phase",
        "eventStreaming",
        "pending",
        "outputs",
        "iteration",
        "child_counter",
        "is_child",
        "address",
        "events",
        "shared_state",
        "requests",
        "waiting",
        "admittedReplyKeys",
        "fanBindings",
        "fan",
        "children",
        "notify_parent",
    }
    if not is_json_object(checkpoint) or set(checkpoint) != required:
        raise RegistrationError("Unsupported concrete checkpoint fields.")
    if checkpoint["contract"] != CHECKPOINT_CONTRACT or checkpoint["routingPolicy"] != "published-batch-join/1":
        raise RegistrationError("Unsupported semantic checkpoint contract or routing policy.")
    if checkpoint["phase"] != "prepare" or checkpoint["is_child"] is not False:
        raise RegistrationError("Only drained root prepare checkpoints are supported.")
    if (
        not isinstance(checkpoint["requests"], dict)
        or any(not isinstance(checkpoint[key], list) for key in ("waiting", "admittedReplyKeys", "children"))
        or checkpoint["notify_parent"] is not None
    ):
        raise RegistrationError("Unsupported request, child or parent-notification shape.")
    if any(checkpoint[key] for key in ("requests", "waiting", "admittedReplyKeys", "children", "notify_parent")):
        raise RegistrationError("HITL, child joins, and parent notification prefixes are unsupported.")
    if type(checkpoint["eventStreaming"]) is not bool:
        raise RegistrationError("Checkpoint requires an explicit event publication policy.")
    if any(type(checkpoint[key]) is not int or checkpoint[key] < 0 for key in ("iteration", "child_counter")):
        raise RegistrationError("Invalid checkpoint counters.")
    if checkpoint["child_counter"]:
        raise RegistrationError("Child invocation prefix is unsupported.")
    address = checkpoint["address"]
    if (
        not is_string_keyed_dict(address)
        or set(address) != {"root_instance_id", "root_workflow_name", "request_path_prefix"}
        or not isinstance(address["root_instance_id"], str)
        or not address["root_instance_id"]
        or address["root_workflow_name"] != workflow.name
        or address["request_path_prefix"] != ""
    ):
        raise RegistrationError("Unsupported root workflow address or request prefix.")
    pending = checkpoint["pending"]
    if not is_string_keyed_dict(pending) or any(name not in workflow.executors for name in pending):
        raise RegistrationError("Checkpoint has an unknown pending executor.")
    allowed_sources = set(workflow.executors) | {"__workflow_start__", "__orchestrator__"}
    for rows in pending.values():
        if not is_list(rows):
            raise RegistrationError("Pending rows must be lists.")
        for row in rows:
            if (
                not is_list_or_tuple(row)
                or len(row) != 2
                or not is_list(row[1])
                or not row[1]
                or any(not isinstance(name, str) or name not in allowed_sources for name in row[1])
            ):
                raise RegistrationError("Unsupported pending message source prefix.")
    bindings, _ = fan_bindings(workflow)
    if (
        checkpoint["fanBindings"] != bindings
        or not is_string_keyed_dict(checkpoint["fan"])
        or set(checkpoint["fan"]) != set(bindings)
    ):
        raise RegistrationError("Checkpoint fan-in bindings differ from the loaded workflow.")
    for key, rows in checkpoint["fan"].items():
        if not is_string_keyed_dict(rows) or any(source not in bindings[key]["sources"] for source in rows):
            raise RegistrationError("Unsupported fan-in source.")
        for source, values in rows.items():
            if not is_list(values) or any(
                not is_list_or_tuple(row) or len(row) != 2 or row[1] != source for row in values
            ):
                raise RegistrationError("Unsupported fan-in row prefix.")
    if not isinstance(checkpoint["outputs"], list) or not isinstance(checkpoint["events"], list):
        raise RegistrationError("Checkpoint outputs and events must be lists.")
    if checkpoint["shared_state"] is not None and not isinstance(checkpoint["shared_state"], dict):
        raise RegistrationError("Checkpoint shared_state must be a dictionary or null.")
    return checkpoint


def run_registered(ctx: Any, workflow: Any, checkpoint: JsonObject) -> Any:
    """Use the actual semantic scheduler, after synchronous preflight succeeds."""
    validate_checkpoint(workflow, checkpoint)
    if ctx.instance_id != checkpoint["address"]["root_instance_id"]:
        raise RegistrationError("Checkpoint root instance differs from the runtime context.")
    from ._target import run

    return run(ctx, workflow, checkpoint)
