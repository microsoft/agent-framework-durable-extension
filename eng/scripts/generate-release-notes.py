#!/usr/bin/env python3
# Copyright (c) Microsoft. All rights reserved.

"""Generate unified GitHub Release notes from package CHANGELOG.md files."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


def read_dotnet_version(props_path: Path) -> str:
    content = props_path.read_text(encoding="utf-8")
    git_tag_match = re.search(r"<GitTag>([^<]+)</GitTag>", content)
    if git_tag_match:
        return git_tag_match.group(1).strip()
    prefix_match = re.search(r"<VersionPrefix>([^<]+)</VersionPrefix>", content)
    date_match = re.search(r"<DateSuffix>([^<]+)</DateSuffix>", content)
    if prefix_match and date_match:
        return f"{prefix_match.group(1).strip()}-preview.{date_match.group(1).strip()}.1"
    raise ValueError(f"Could not determine .NET version from {props_path}")


def read_python_version(pyproject_path: Path) -> str:
    content = pyproject_path.read_text(encoding="utf-8")
    match = re.search(r'^version\s*=\s*"([^"]+)"', content, flags=re.MULTILINE)
    if not match:
        raise ValueError(f"Could not determine Python version from {pyproject_path}")
    return match.group(1).strip()


def extract_section_bullets(changelog_path: Path, version: str, fallback_unreleased: bool = True) -> list[str]:
    if not changelog_path.exists():
        return []
    content = changelog_path.read_text(encoding="utf-8")
    normalized = version.lstrip("v")
    candidates = [f"v{normalized}", normalized]
    if fallback_unreleased:
        candidates.append("[Unreleased]")

    for candidate in candidates:
        pattern = re.compile(
            rf"^##\s+{re.escape(candidate)}\s*$\n(.*?)(?=^##\s+|\Z)",
            flags=re.MULTILINE | re.DOTALL,
        )
        match = pattern.search(content)
        if match:
            body = match.group(1).strip()
            if not body:
                continue
            bullets: list[str] = []
            current: list[str] = []
            for raw_line in body.splitlines():
                line = raw_line.rstrip()
                if line.startswith("- "):
                    if current:
                        bullets.append(" ".join(current))
                    current = [line[2:].strip()]
                elif line.startswith("### ") or not line.strip():
                    if current and not line.strip():
                        bullets.append(" ".join(current))
                        current = []
                elif current:
                    current.append(line.strip())
            if current:
                bullets.append(" ".join(current))
            if bullets:
                return bullets
    return []


def split_breaking_and_fixes(bullets: list[str]) -> tuple[list[str], list[str]]:
    breaking: list[str] = []
    fixes: list[str] = []
    for item in bullets:
        if item.startswith("[BREAKING]") or re.search(r":\s*\[BREAKING\]\s*", item):
            cleaned = re.sub(r"\[BREAKING\]\s*", "", item, count=1)
            breaking.append(cleaned)
        else:
            fixes.append(item)
    return breaking, fixes


def deduplicate_preserving_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def build_release_notes(
    repo_root: Path,
    dotnet_version: str,
    python_version: str,
    include_dotnet: bool = True,
    include_python: bool = True,
) -> str:
    dt_changelog = repo_root / "dotnet" / "src" / "Microsoft.Agents.AI.DurableTask" / "CHANGELOG.md"
    af_changelog = repo_root / "dotnet" / "src" / "Microsoft.Agents.AI.Hosting.AzureFunctions" / "CHANGELOG.md"
    py_changelog = repo_root / "python" / "CHANGELOG.md"

    lines: list[str] = ["## Packages", ""]

    if include_dotnet:
        lines.extend(
            [
                f"**.NET — `{dotnet_version}`**",
                "",
                "| Package | |",
                "|---|---|",
                f"| `Microsoft.Agents.AI.DurableTask` | https://www.nuget.org/packages/Microsoft.Agents.AI.DurableTask/{dotnet_version} |",
                f"| `Microsoft.Agents.AI.Hosting.AzureFunctions` | https://www.nuget.org/packages/Microsoft.Agents.AI.Hosting.AzureFunctions/{dotnet_version} |",
                "",
            ]
        )

    if include_python:
        lines.extend(
            [
                f"**Python — `{python_version}`**",
                "",
                "| Package | |",
                "|---|---|",
                f"| `agent-framework-durabletask` | https://pypi.org/project/agent-framework-durabletask/{python_version}/ |",
                f"| `agent-framework-azurefunctions` | https://pypi.org/project/agent-framework-azurefunctions/{python_version}/ |",
                "",
            ]
        )

    if include_dotnet:
        lines.extend(
            [
                "All .NET assemblies and packages in this release are Authenticode signed through ESRP.",
                "",
            ]
        )

    package_sections: list[tuple[str, list[str]]] = []
    if include_dotnet:
        package_sections.append(
            ("Microsoft.Agents.AI.DurableTask", extract_section_bullets(dt_changelog, dotnet_version))
        )
        package_sections.append(
            ("Microsoft.Agents.AI.Hosting.AzureFunctions", extract_section_bullets(af_changelog, dotnet_version))
        )

    if include_python:
        package_sections.append(
            (
                "agent-framework-durabletask / agent-framework-azurefunctions",
                extract_section_bullets(py_changelog, python_version),
            )
        )

    breaking_by_pkg: list[tuple[str, list[str]]] = []
    all_fixes: list[str] = []

    for pkg_name, bullets in package_sections:
        breaking, fixes = split_breaking_and_fixes(bullets)
        if breaking:
            breaking_by_pkg.append((pkg_name, breaking))
        all_fixes.extend(fixes)

    if breaking_by_pkg:
        lines.extend(["## Breaking changes", ""])
        for pkg_name, items in breaking_by_pkg:
            lines.append(f"**`{pkg_name}`**")
            lines.append("")
            for item in items:
                lines.append(f"- {item}")
            lines.append("")

    deduped_fixes = deduplicate_preserving_order(all_fixes)
    if deduped_fixes:
        lines.extend(["## Fixes and improvements", ""])
        for item in deduped_fixes:
            lines.append(f"- {item}")
        lines.append("")

    lines.extend(
        [
            "## Full changelogs",
            "",
            "- [`Microsoft.Agents.AI.DurableTask`](https://github.com/microsoft/agent-framework-durable-extension/blob/main/dotnet/src/Microsoft.Agents.AI.DurableTask/CHANGELOG.md)",
            "- [`Microsoft.Agents.AI.Hosting.AzureFunctions`](https://github.com/microsoft/agent-framework-durable-extension/blob/main/dotnet/src/Microsoft.Agents.AI.Hosting.AzureFunctions/CHANGELOG.md)",
            "- [`Python packages`](https://github.com/microsoft/agent-framework-durable-extension/blob/main/python/CHANGELOG.md)",
        ]
    )

    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--dotnet-version", help=".NET version (defaults to GitTag in nuget-package.props)")
    parser.add_argument("--python-version", help="Python version (defaults to version in python/pyproject.toml)")
    parser.add_argument("--skip-dotnet", action="store_true", help="Exclude .NET section from release notes")
    parser.add_argument("--skip-python", action="store_true", help="Exclude Python section from release notes")
    parser.add_argument("--output", type=Path, help="Write release notes to file instead of stdout")
    args = parser.parse_args()

    repo_root: Path = args.repo_root.resolve()
    dotnet_version = (
        args.dotnet_version or read_dotnet_version(repo_root / "dotnet" / "nuget" / "nuget-package.props")
    ).lstrip("v")
    python_version = (
        args.python_version or read_python_version(repo_root / "python" / "pyproject.toml")
    ).lstrip("v")

    notes = build_release_notes(
        repo_root=repo_root,
        dotnet_version=dotnet_version,
        python_version=python_version,
        include_dotnet=not args.skip_dotnet,
        include_python=not args.skip_python,
    )

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(notes, encoding="utf-8")
    else:
        sys.stdout.buffer.write(notes.encode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
