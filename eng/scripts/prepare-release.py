#!/usr/bin/env python3
# Copyright (c) Microsoft. All rights reserved.

"""Prepare a release by bumping package versions and stamping CHANGELOG.md files."""

from __future__ import annotations

import argparse
import re
from pathlib import Path


def read_current_dotnet_version(props_path: Path) -> str:
    content = props_path.read_text(encoding="utf-8")
    git_tag_match = re.search(r"<GitTag>([^<]+)</GitTag>", content)
    if git_tag_match:
        return git_tag_match.group(1).strip()
    prefix_match = re.search(r"<VersionPrefix>([^<]+)</VersionPrefix>", content)
    date_match = re.search(r"<DateSuffix>([^<]+)</DateSuffix>", content)
    if prefix_match and date_match:
        return f"{prefix_match.group(1).strip()}-preview.{date_match.group(1).strip()}.1"
    raise ValueError(f"Could not determine current .NET version from {props_path}")


def read_current_python_version(pyproject_path: Path) -> str:
    content = pyproject_path.read_text(encoding="utf-8")
    match = re.search(r'^version\s*=\s*"([^"]+)"', content, flags=re.MULTILINE)
    if not match:
        raise ValueError(f"Could not determine current Python version from {pyproject_path}")
    return match.group(1).strip()


def update_dotnet_version(
    props_path: Path, version_prefix: str, date_suffix: str, dotnet_version: str, dry_run: bool = False
) -> None:
    content = props_path.read_text(encoding="utf-8")
    content = re.sub(
        r"<VersionPrefix>[^<]+</VersionPrefix>",
        f"<VersionPrefix>{version_prefix}</VersionPrefix>",
        content,
    )
    if date_suffix:
        content = re.sub(
            r"<DateSuffix>[^<]+</DateSuffix>",
            f"<DateSuffix>{date_suffix}</DateSuffix>",
            content,
        )
    content = re.sub(
        r"<GitTag>[^<]+</GitTag>",
        f"<GitTag>{dotnet_version}</GitTag>",
        content,
    )
    if not dry_run:
        props_path.write_text(content, encoding="utf-8")


def update_python_version(repo_root: Path, python_version: str, dry_run: bool = False) -> None:
    pyproject_files = [
        repo_root / "python" / "pyproject.toml",
        repo_root / "python" / "packages" / "durabletask" / "pyproject.toml",
        repo_root / "python" / "packages" / "azurefunctions" / "pyproject.toml",
    ]
    for path in pyproject_files:
        content = path.read_text(encoding="utf-8")
        content = re.sub(
            r'^version\s*=\s*"[^"]+"',
            f'version = "{python_version}"',
            content,
            count=1,
            flags=re.MULTILINE,
        )
        content = re.sub(
            r'("agent-framework-durabletask(?:\[[^\]]+\])?>=)[^,<"]+(,<2")',
            rf"\g<1>{python_version}\2",
            content,
        )
        if not dry_run:
            path.write_text(content, encoding="utf-8")


def stamp_changelog(changelog_path: Path, version_header: str, dry_run: bool = False) -> bool:
    """Stamp the ## [Unreleased] section with ## v<version_header> if it has entries."""
    if not changelog_path.exists():
        return False

    content = changelog_path.read_text(encoding="utf-8")
    normalized_header = version_header if version_header.startswith("v") else f"v{version_header}"
    target_heading = f"## {normalized_header}"

    if re.search(rf"^##\s+{re.escape(normalized_header)}\s*$", content, flags=re.MULTILINE):
        return False

    pattern = re.compile(
        r"(^## \[Unreleased\]\s*\n)(.*?)(?=^## |\Z)",
        flags=re.MULTILINE | re.DOTALL,
    )
    match = pattern.search(content)
    if not match:
        return False

    unreleased_body = match.group(2).strip()
    if not unreleased_body:
        return False

    replacement = f"## [Unreleased]\n\n{target_heading}\n\n{unreleased_body}\n\n"
    updated = content[: match.start()] + replacement + content[match.end() :].lstrip("\n")
    if not dry_run:
        changelog_path.write_text(updated, encoding="utf-8")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--dotnet-version-prefix", help=".NET VersionPrefix (e.g., 1.16.0)")
    parser.add_argument("--date-suffix", help="YYMMDD date suffix (e.g., 261022)")
    parser.add_argument(
        "--dotnet-version",
        help="Explicit full .NET package version (e.g., 1.16.0-preview.261022.1). Computed if omitted.",
    )
    parser.add_argument(
        "--python-version",
        help="Explicit Python package version (e.g., 1.0.0b261022). Computed from --date-suffix if omitted.",
    )
    parser.add_argument("--skip-dotnet", action="store_true", help="Skip updating .NET versions and changelogs")
    parser.add_argument("--skip-python", action="store_true", help="Skip updating Python versions and changelogs")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview version bumps and changelog stamps without modifying files on disk",
    )
    args = parser.parse_args()

    repo_root: Path = args.repo_root.resolve()
    props_path = repo_root / "dotnet" / "nuget" / "nuget-package.props"
    mode_prefix = "[DRY RUN] " if args.dry_run else ""

    if not args.skip_dotnet:
        props_content = props_path.read_text(encoding="utf-8")
        current_prefix_match = re.search(r"<VersionPrefix>([^<]+)</VersionPrefix>", props_content)
        current_date_match = re.search(r"<DateSuffix>([^<]+)</DateSuffix>", props_content)
        version_prefix = args.dotnet_version_prefix or (
            current_prefix_match.group(1).strip() if current_prefix_match else "1.16.0"
        )
        date_suffix = args.date_suffix or (
            current_date_match.group(1).strip() if current_date_match else ""
        )
        dotnet_version = args.dotnet_version or f"{version_prefix}-preview.{date_suffix}.1"
        update_dotnet_version(props_path, version_prefix, date_suffix, dotnet_version, dry_run=args.dry_run)
        print(f"{mode_prefix}.NET version -> {dotnet_version} ({props_path.relative_to(repo_root)})")

        for changelog in [
            repo_root / "dotnet" / "src" / "Microsoft.Agents.AI.DurableTask" / "CHANGELOG.md",
            repo_root / "dotnet" / "src" / "Microsoft.Agents.AI.Hosting.AzureFunctions" / "CHANGELOG.md",
        ]:
            stamped = stamp_changelog(changelog, dotnet_version, dry_run=args.dry_run)
            status = "stamped" if stamped else "unchanged"
            print(f"{mode_prefix}{changelog.relative_to(repo_root)}: {status}")

    if not args.skip_python:
        python_version = args.python_version
        if not python_version:
            if not args.date_suffix:
                python_version = read_current_python_version(repo_root / "python" / "pyproject.toml")
            else:
                python_version = f"1.0.0b{args.date_suffix}"
        update_python_version(repo_root, python_version, dry_run=args.dry_run)
        print(f"{mode_prefix}Python version -> {python_version}")

        py_changelog = repo_root / "python" / "CHANGELOG.md"
        stamped = stamp_changelog(py_changelog, python_version, dry_run=args.dry_run)
        status = "stamped" if stamped else "unchanged"
        print(f"{mode_prefix}{py_changelog.relative_to(repo_root)}: {status}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
