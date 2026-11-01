# Copyright (c) Microsoft. All rights reserved.

from __future__ import annotations

import runpy
import tempfile
import unittest
from pathlib import Path

GENERATOR = Path(__file__).resolve().parents[1] / "generate-release-notes.py"
generate = runpy.run_path(str(GENERATOR))


class ReleaseNotesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.write(
            "dotnet",
            "nuget",
            "nuget-package.props",
            content="<Project><GitTag>1.16.0-preview.123.1</GitTag></Project>",
        )
        self.write(
            "python",
            "pyproject.toml",
            content='[project]\nversion = "9.9.9"\n',
        )
        for directory, version in (
            ("durabletask", "1.0.0b123"),
            ("azurefunctions", "2.0.0b456"),
        ):
            self.write(
                "python",
                "packages",
                directory,
                "pyproject.toml",
                content=f'[project]\nversion = "{version}"\n',
            )
        self.write(
            "python",
            "CHANGELOG.md",
            content="## 1.0.0b123\n\n- Durable fix.\n\n## 2.0.0b456\n\n- Functions fix.\n",
        )
        for package in (
            "Microsoft.Agents.AI.DurableTask",
            "Microsoft.Agents.AI.Hosting.AzureFunctions",
        ):
            self.write(
                "dotnet",
                "src",
                package,
                "CHANGELOG.md",
                content="## 1.16.0-preview.123.1\n\n- [BREAKING] Changed API.\n- Shared fix.\n",
            )

    def write(self, *parts: str, content: str) -> Path:
        path = self.root.joinpath(*parts)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return path

    def notes(self, **kwargs: object) -> str:
        return generate["build_release_notes"](
            self.root, "1.16.0-preview.123.1", source_ref="a" * 40, **kwargs
        )

    def test_each_package_has_its_own_version_and_status(self) -> None:
        notes = self.notes()
        rows = [
            line
            for line in notes.splitlines()
            if line.startswith(("| NuGet |", "| PyPI |"))
        ]
        self.assertEqual(len(rows), 4)
        self.assertIn(
            "| `agent-framework-durabletask` | `1.0.0b123` | Beta | Not verified |",
            notes,
        )
        self.assertIn(
            "| `agent-framework-azurefunctions` | `2.0.0b456` | Beta | Not verified |",
            notes,
        )
        self.assertEqual(
            notes.count("| `1.16.0-preview.123.1` | Preview | Not verified |"),
            2,
        )
        self.assertNotIn("9.9.9", notes)
        self.assertNotIn("Authenticode signed", notes)
        self.assertIn("**Registry publication not verified by this workflow.**", notes)

    def test_changelog_links_are_pinned_and_fixes_are_preserved(self) -> None:
        notes = self.notes()
        self.assertIn(f"/blob/{'a' * 40}/python/CHANGELOG.md", notes)
        self.assertNotIn("/blob/main/python/CHANGELOG.md", notes)
        self.assertIn("## Breaking changes", notes)
        self.assertEqual(notes.count("- Shared fix."), 1)
        self.assertIn("- Durable fix.", notes)
        self.assertIn("- Functions fix.", notes)

    def test_preview_override_and_skip_options_remain_available(self) -> None:
        notes = self.notes(python_version="3.0.0rc1", include_dotnet=False)
        self.assertNotIn("| NuGet |", notes)
        self.assertEqual(notes.count("| `3.0.0rc1` | Release candidate |"), 2)
        self.assertIn("| `1.0.0b123` | Beta | Not verified |", notes)
        self.assertIn("| `2.0.0b456` | Beta | Not verified |", notes)
        notes = self.notes(include_python=False)
        self.assertNotIn("| PyPI |", notes)

    def test_dotnet_override_keeps_source_version_and_selects_override_changelog(
        self,
    ) -> None:
        self.write(
            "dotnet",
            "src",
            "Microsoft.Agents.AI.DurableTask",
            "CHANGELOG.md",
            content="## 7.0.0-rc1\n\n- Override fix.\n",
        )
        notes = generate["build_release_notes"](
            self.root, "7.0.0-rc1", include_python=False, source_ref="a" * 40
        )
        self.assertEqual(
            notes.count("| `1.16.0-preview.123.1` | Preview | Not verified |"), 2
        )
        self.assertEqual(
            notes.count("| `1.16.0-preview.123.1` | `7.0.0-rc1` | Release candidate |"),
            2,
        )
        self.assertIn("- Override fix.", notes)

    def test_matching_overrides_need_no_mismatch_table(self) -> None:
        self.write(
            "python",
            "packages",
            "azurefunctions",
            "pyproject.toml",
            content='[project]\nversion = "1.0.0b123"\n',
        )
        self.assertNotIn(
            "### Release-note version overrides", self.notes(python_version="1.0.0b123")
        )

    def test_shared_python_changelog_does_not_duplicate_breaking_changes(self) -> None:
        self.write(
            "python",
            "CHANGELOG.md",
            content="## 1.0.0b123\n\n- [BREAKING] Python API changed.\n",
        )
        notes = self.notes(python_version="1.0.0b123", include_dotnet=False)
        self.assertIn(
            "agent-framework-durabletask / agent-framework-azurefunctions", notes
        )
        self.assertEqual(notes.count("- Python API changed."), 1)

    def test_python_reader_uses_project_metadata_not_other_tables(self) -> None:
        path = self.write(
            "pyproject.toml",
            content='[tool.example]\nversion = "9.9.9"\n[project]\nversion = "1.0.0b1"\n',
        )
        self.assertEqual(generate["read_python_version"](path), "1.0.0b1")
        path.write_text("[project]\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            generate["read_python_version"](path)

    def test_missing_package_version_fails_instead_of_using_workspace_version(
        self,
    ) -> None:
        self.write(
            "python",
            "packages",
            "azurefunctions",
            "pyproject.toml",
            content="[project]\n",
        )
        with self.assertRaises(ValueError):
            self.notes()

    def test_dotnet_reader_and_maturity(self) -> None:
        path = self.write(
            "versions.props",
            content="<Project><GitTag>1.2.3-preview.123.1</GitTag></Project>",
        )
        self.assertEqual(generate["read_dotnet_version"](path), "1.2.3-preview.123.1")
        for ecosystem, version, status in (
            ("NuGet", "1.2.3-preview.123.1", "Preview"),
            ("NuGet", "1.2.3-rc1", "Release candidate"),
            ("NuGet", "1.2.3", "Stable"),
            ("PyPI", "1.2.3b1", "Beta"),
            ("PyPI", "1.2.3a1", "Alpha"),
            ("PyPI", "1.2.3rc1", "Release candidate"),
            ("PyPI", "1.2.3.dev1", "Development"),
            ("PyPI", "1.2.3", "Stable"),
        ):
            with self.subTest(version=version):
                self.assertEqual(generate["package_status"](version, ecosystem), status)


if __name__ == "__main__":
    unittest.main()
