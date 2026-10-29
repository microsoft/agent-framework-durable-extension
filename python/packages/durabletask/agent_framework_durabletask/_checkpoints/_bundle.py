# Copyright (c) Microsoft. All rights reserved.

"""Build a physically separate current-only worker from explicit sources."""

from __future__ import annotations

import ast
import hashlib
import importlib
import json
import shutil
from collections.abc import Sequence
from pathlib import Path

import durabletask

_OMITTED_FILES = frozenset({
    "_durable_agent_state.py",
    "_state_migration.py",
    "checkpoint_migration.py",
    "checkpoint_migration_inventory.json",
    "checkpoint_entity_migration.py",
})
_OMITTED_EXPORTS = frozenset({"LegacyDurableAgentState", "migrate_legacy_state", "state_snapshot_digest"})


def _current_exports(source: str) -> str:
    tree = ast.parse(source)
    retained: list[ast.stmt] = []
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module in ("_durable_agent_state", "_state_migration"):
            continue
        if (
            isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets)
            and isinstance(node.value, ast.List)
        ):
            node.value.elts = [
                item
                for item in node.value.elts
                if not isinstance(item, ast.Constant) or item.value not in _OMITTED_EXPORTS
            ]
        retained.append(node)
    tree.body = retained
    return "# Copyright (c) Microsoft. All rights reserved.\n\n" + ast.unparse(tree) + "\n"


def build_checkpoint_bundle(
    destination: str | Path,
    customer_directory: str | Path,
    *,
    customer_files: Sequence[str],
    include_functions: bool = False,
) -> str:
    """Copy installed candidate SDK/framework and explicitly selected customer files.

    Returns the SHA256 of the immutable deployment-pins.json bytes. Dependencies
    outside these packages must be installed separately at pinned versions.
    This is a directory builder, not dependency resolution or remote deployment.
    Existing destinations are never overwritten. A failed build remains for
    diagnosis and must not be activated. Migration tools/private keys are absent.
    """
    destination, customer_directory = Path(destination), Path(customer_directory)
    if destination.exists():
        raise FileExistsError(destination)
    root = customer_directory.resolve(strict=True)
    if not root.is_dir() or not customer_files:
        raise ValueError("An explicit trusted customer source selection is required.")
    selected: list[tuple[Path, Path]] = []
    for name in customer_files:
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts or relative.suffix not in (".py", ".json"):
            raise ValueError("Customer source must be an explicit relative Python/JSON file.")
        source = root / relative
        if source.is_symlink() or not source.resolve().is_relative_to(root) or not source.is_file():
            raise ValueError("Customer source is missing or escapes its trusted root.")
        selected.append((source, relative))
    framework = Path(__file__).resolve().parents[1]
    sdk = Path(durabletask.__file__).resolve().parent
    function_sources: list[tuple[Path, Path]] = []
    function_framework: Path | None = None
    if include_functions:
        for module_name, relative in (
            ("azure.durable_functions", Path("azure") / "durable_functions"),
            ("azure.functions", Path("azure") / "functions"),
        ):
            module = importlib.import_module(module_name)
            if module.__file__ is None:
                raise ValueError("The exact candidate Functions package location is unavailable.")
            function_sources.append((Path(module.__file__).resolve().parent, relative))
        agent_framework_azurefunctions = importlib.import_module("agent_framework_azurefunctions")
        if agent_framework_azurefunctions.__file__ is None:
            raise ValueError("The framework Functions package location is unavailable.")
        function_framework = Path(agent_framework_azurefunctions.__file__).resolve().parent
    destination.mkdir(parents=True)
    for source, relative in selected:
        output = destination / "customer" / relative
        output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, output)
    for source in (framework, sdk):
        shutil.copytree(
            source,
            destination / source.name,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc", *_OMITTED_FILES),
        )
    for source, relative in function_sources:
        shutil.copytree(
            source,
            destination / relative,
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
    if function_framework is not None:
        for name in ("__init__.py", "checkpoint.py", "_checkpoint_routes.py", "_routes.py", "_errors.py", "py.typed"):
            output = destination / "agent_framework_azurefunctions" / name
            output.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(function_framework / name, output)
    initializer = destination / framework.name / "__init__.py"
    initializer.write_text(_current_exports(initializer.read_text(encoding="utf-8")), encoding="utf-8")
    # The cold entity cannot call an importer even when customer code asks it to.
    forbidden = [str(path) for path in destination.rglob("*") if path.name in _OMITTED_FILES]
    if forbidden:
        raise ValueError(f"Migration-only files remain in the cold bundle: {forbidden}")
    pins = {
        path.relative_to(destination).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(destination.rglob("*"))
        if path.is_file()
    }
    encoded = json.dumps(pins, sort_keys=True, separators=(",", ":")).encode()
    (destination / "deployment-pins.json").write_bytes(encoded)
    return hashlib.sha256(encoded).hexdigest()


def verify_checkpoint_bundle(directory: str | Path, *, bundle_sha256: str) -> None:
    """Check the whole cold directory before loading trusted customer callbacks."""
    root = Path(directory).resolve(strict=True)
    encoded = (root / "deployment-pins.json").read_bytes()
    if hashlib.sha256(encoded).hexdigest() != bundle_sha256:
        raise ValueError("Cold bundle manifest differs from the activated deployment.")
    pins = json.loads(encoded)
    actual = {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file() and path != root / "deployment-pins.json"
    }
    if not isinstance(pins, dict) or pins != actual:
        raise ValueError("Cold bundle contains missing, extra or changed files.")
