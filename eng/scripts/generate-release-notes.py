#!/usr/bin/env python3
# Copyright (c) Microsoft. All rights reserved.

"""Generate unified GitHub Release notes from package CHANGELOG.md files."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tomllib
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
    project = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))["project"]
    version = project.get("version")
    if not isinstance(version, str) or not version.strip():
        raise ValueError(f"Could not determine Python version from {pyproject_path}")
    return version.strip()


def package_status(version: str, ecosystem: str) -> str:
    if ecosystem == "NuGet":
        if "-preview." in version:
            return "Preview"
        if "-rc" in version:
            return "Release candidate"
        return "Prerelease" if "-" in version else "Stable"
    if re.search(r"b\d+", version):
        return "Beta"
    if re.search(r"rc\d+", version):
        return "Release candidate"
    if re.search(r"a\d+", version):
        return "Alpha"
    return "Development" if ".dev" in version else "Stable"


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
    dotnet_version: str | None = None,
    python_version: str | None = None,
    include_dotnet: bool = True,
    include_python: bool = True,
    source_ref: str | None = None,
) -> str:
    dt_changelog = repo_root / "dotnet" / "src" / "Microsoft.Agents.AI.DurableTask" / "CHANGELOG.md"
    af_changelog = repo_root / "dotnet" / "src" / "Microsoft.Agents.AI.Hosting.AzureFunctions" / "CHANGELOG.md"
    py_changelog = repo_root / "python" / "CHANGELOG.md"
    source_dotnet_version = (
        read_dotnet_version(repo_root / "dotnet" / "nuget" / "nuget-package.props")
        if include_dotnet
        else ""
    )
    dotnet_version = dotnet_version or source_dotnet_version

    if source_ref is None:
        source_ref = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=repo_root, text=True
        ).strip()
    source_url = f"https://github.com/microsoft/agent-framework-durable-extension/blob/{source_ref}"
    lines: list[str] = [
        "## Packages",
        "",
        f"Source snapshot: [`{source_ref}`](https://github.com/microsoft/agent-framework-durable-extension/tree/{source_ref}).",
        "",
        "**Registry publication not verified by this workflow.** This table records package versions",
        "declared in the source snapshot, not successful registry publication or signing. Follow the recommended",
        "[publication checklist](https://github.com/microsoft/agent-framework-durable-extension/blob/main/eng/README.md#publishing-the-github-release)",
        "and identify any packages omitted from publication. GitHub release visibility",
        "is independent of ADO package publishing.",
        "",
        "| Ecosystem | Package | Source version | Status | Publication |",
        "| --- | --- | --- | --- | --- |",
    ]

    if include_dotnet:
        for package in ("Microsoft.Agents.AI.DurableTask", "Microsoft.Agents.AI.Hosting.AzureFunctions"):
            lines.append(
                f"| NuGet | `{package}` | `{source_dotnet_version}` | {package_status(source_dotnet_version, 'NuGet')} | Not verified |"
            )

    python_versions: list[tuple[str, str]] = []
    overrides: list[tuple[str, str, str, str]] = []
    if include_dotnet and dotnet_version != source_dotnet_version:
        for package in ("Microsoft.Agents.AI.DurableTask", "Microsoft.Agents.AI.Hosting.AzureFunctions"):
            overrides.append(("NuGet", package, source_dotnet_version, dotnet_version))
    if include_python:
        for directory, package in (
            ("durabletask", "agent-framework-durabletask"),
            ("azurefunctions", "agent-framework-azurefunctions"),
        ):
            source_version = read_python_version(
                repo_root / "python" / "packages" / directory / "pyproject.toml"
            )
            version = python_version or source_version
            python_versions.append((package, version))
            lines.append(
                f"| PyPI | `{package}` | `{source_version}` | {package_status(source_version, 'PyPI')} | Not verified |"
            )
            if version != source_version:
                overrides.append(("PyPI", package, source_version, version))
    lines.append("")
    if overrides:
        lines.extend(
            [
                "### Release-note version overrides",
                "",
                "The following effective overrides differ from the source metadata. They select the release-note",
                "versions and changelog sections only; they do not change source files, built packages,",
                "or ADO publishing. A .NET override may be supplied explicitly or derived from the release tag.",
                "",
                "| Ecosystem | Package | Source version | Release-note version | Release-note status |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for ecosystem, package, source_version, version in overrides:
            lines.append(
                f"| {ecosystem} | `{package}` | `{source_version}` | `{version}` | {package_status(version, ecosystem)} |"
            )
        lines.append("")

    package_sections: list[tuple[str, list[str]]] = []
    if include_dotnet:
        package_sections.append(
            ("Microsoft.Agents.AI.DurableTask", extract_section_bullets(dt_changelog, dotnet_version))
        )
        package_sections.append(
            ("Microsoft.Agents.AI.Hosting.AzureFunctions", extract_section_bullets(af_changelog, dotnet_version))
        )

    for version in dict.fromkeys(version for _, version in python_versions):
        packages = " / ".join(
            package
            for package, package_version in python_versions
            if package_version == version
        )
        package_sections.append((packages, extract_section_bullets(py_changelog, version)))

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
            f"- [`Microsoft.Agents.AI.DurableTask`]({source_url}/dotnet/src/Microsoft.Agents.AI.DurableTask/CHANGELOG.md)",
            f"- [`Microsoft.Agents.AI.Hosting.AzureFunctions`]({source_url}/dotnet/src/Microsoft.Agents.AI.Hosting.AzureFunctions/CHANGELOG.md)",
            f"- [`Python packages`]({source_url}/python/CHANGELOG.md)",
        ]
    )

    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--dotnet-version", help=".NET release-note version override (source metadata is also shown)")
    parser.add_argument("--python-version", help="Override both Python release-note versions (source metadata is also shown)")
    parser.add_argument("--source-ref", help="Source commit for links (defaults to the checked-out HEAD)")
    parser.add_argument("--skip-dotnet", action="store_true", help="Exclude .NET section from release notes")
    parser.add_argument("--skip-python", action="store_true", help="Exclude Python section from release notes")
    parser.add_argument("--output", type=Path, help="Write release notes to file instead of stdout")
    args = parser.parse_args()

    repo_root: Path = args.repo_root.resolve()
    dotnet_version = args.dotnet_version.lstrip("v") if args.dotnet_version else None
    python_version = args.python_version.lstrip("v") if args.python_version else None

    notes = build_release_notes(
        repo_root=repo_root,
        dotnet_version=dotnet_version,
        python_version=python_version,
        include_dotnet=not args.skip_dotnet,
        include_python=not args.skip_python,
        source_ref=args.source_ref,
    )

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(notes, encoding="utf-8")
    else:
        sys.stdout.buffer.write(notes.encode("utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
