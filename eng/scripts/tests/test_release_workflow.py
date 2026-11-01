# Copyright (c) Microsoft. All rights reserved.

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
WORKFLOW = REPO / ".github" / "workflows" / "create-release.yml"
BASH = shutil.which("bash")


def workflow_script(name: str) -> str:
    step = WORKFLOW.read_text(encoding="utf-8").split(f"      - name: {name}\n", 1)[1]
    body = step.split("        run: |\n", 1)[1]
    lines = []
    for line in body.splitlines():
        if line and not line.startswith("          "):
            break
        lines.append(line)
    return textwrap.dedent("\n".join(lines))


@unittest.skipUnless(
    BASH and shutil.which("git"), "Workflow checks require bash and git"
)
class ReleaseWorkflowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "repo"
        self.source.mkdir()
        self.env = os.environ.copy()
        self.env.update(
            EVENT_NAME="workflow_dispatch",
            REF_NAME="main",
            INPUT_TAG="",
            INPUT_DOTNET_VERSION="",
            INPUT_PYTHON_VERSION="",
            GITHUB_WORKSPACE=str(self.source),
            RUNNER_TEMP=str(self.root),
            GITHUB_OUTPUT=str(self.root / "output"),
            GITHUB_STEP_SUMMARY=str(self.root / "summary"),
            GITHUB_REPOSITORY="example/test",
            TAG="v1.0.0-preview.10.1",
            TARGET="a" * 40,
            PRERELEASE="true",
            DRAFT="",
            MOCK_EXISTING="",
            MOCK_API_FAILURE="false",
        )
        self.git("init", "-q")
        self.git("config", "core.autocrlf", "false")
        self.git("config", "user.name", "Release tooling tests")
        self.git("config", "user.email", "test@example.invalid")
        self.generator = self.source / "eng" / "scripts" / "generate-release-notes.py"
        self.generator.parent.mkdir(parents=True)
        self.generator.write_text(
            "raise SystemExit('old generator must not run')\n", encoding="utf-8"
        )
        self.metadata("1.0.0-preview.10.1", "1.0.0b10", "2.0.0b20")
        self.commit()
        self.old_sha = self.git("rev-parse", "HEAD").stdout.strip()
        self.tag = "v9.9.9-misleading"
        self.git("tag", "-a", self.tag, "-m", "Older coordinated snapshot")
        shutil.copyfile(
            REPO / "eng" / "scripts" / "generate-release-notes.py", self.generator
        )
        shutil.copyfile(self.generator, self.root / "generate-release-notes.py")
        self.metadata("3.0.0-preview.30.1", "3.0.0b30", "4.0.0b40")
        self.commit()
        self.new_sha = self.git("rev-parse", "HEAD").stdout.strip()

    def git(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", *args], cwd=self.source, capture_output=True, text=True, check=True
        )

    def commit(self) -> None:
        self.git("add", ".")
        self.git(
            "-c",
            "core.hooksPath=" + str(self.root / "no-hooks"),
            "commit",
            "-qm",
            "Release test fixture\n\nCo-authored-by: Copilot App <223556219+Copilot@users.noreply.github.com>",
        )

    def metadata(self, dotnet: str, durable: str, functions: str) -> None:
        props = self.source / "dotnet" / "nuget" / "nuget-package.props"
        props.parent.mkdir(parents=True, exist_ok=True)
        props.write_text(
            f"<Project><GitTag>{dotnet}</GitTag></Project>", encoding="utf-8"
        )
        for directory, version in (
            ("durabletask", durable),
            ("azurefunctions", functions),
        ):
            path = self.source / "python" / "packages" / directory / "pyproject.toml"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f'[project]\nversion = "{version}"\n', encoding="utf-8")

    def run_step(self, name: str) -> subprocess.CompletedProcess[str]:
        bootstrap = """
            if command -v cygpath >/dev/null; then
              export RUNNER_TEMP=$(cygpath -u "$RUNNER_TEMP")
              export GITHUB_WORKSPACE=$(cygpath -u "$GITHUB_WORKSPACE")
              export GITHUB_OUTPUT=$(cygpath -u "$GITHUB_OUTPUT")
              export GITHUB_STEP_SUMMARY=$(cygpath -u "$GITHUB_STEP_SUMMARY")
            fi
            gh() {
              if [[ "$1" == "api" ]]; then
                if [[ "$MOCK_API_FAILURE" == "true" ]]; then
                  echo "mock API failure" >&2
                  return 1
                fi
                printf '%s' "$MOCK_EXISTING"
              else
                printf '%s\\n' "$@" >> "$RUNNER_TEMP/gh-calls"
              fi
            }
        """
        return subprocess.run(
            [BASH, "-e", "-c", textwrap.dedent(bootstrap) + workflow_script(name)],
            cwd=self.source,
            env=self.env,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_tag_push_and_manual_existing_tag_use_tagged_metadata(self) -> None:
        for event in ("push", "workflow_dispatch"):
            with self.subTest(event=event):
                self.git("checkout", "--detach", self.new_sha)
                self.env.update(EVENT_NAME=event, REF_NAME=self.tag, INPUT_TAG=self.tag)
                result = self.run_step(
                    "Resolve release source and generate release notes"
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                notes = (self.source / "artifacts" / "release-notes.md").read_text(
                    encoding="utf-8"
                )
                self.assertIn(self.old_sha, notes)
                self.assertIn("`1.0.0-preview.10.1` | Preview", notes)
                self.assertIn("`1.0.0b10` | Beta", notes)
                self.assertIn("`2.0.0b20` | Beta", notes)
                self.assertIn(
                    "| `1.0.0-preview.10.1` | `9.9.9-misleading` | Prerelease |", notes
                )
                self.assertNotIn("`3.0.0b30`", notes)

    def test_manual_new_tag_uses_target_snapshot_and_default_git_tag(self) -> None:
        result = self.run_step("Resolve release source and generate release notes")
        self.assertEqual(result.returncode, 0, result.stderr)
        output = (self.root / "output").read_text(encoding="utf-8")
        self.assertIn("tag=v3.0.0-preview.30.1", output)
        self.assertIn(f"source_sha={self.new_sha}", output)
        self.assertIn(
            "`4.0.0b40`",
            (self.source / "artifacts" / "release-notes.md").read_text(
                encoding="utf-8"
            ),
        )

    def test_manual_older_target_uses_preserved_current_generator(self) -> None:
        self.git("checkout", "--detach", self.old_sha)
        result = self.run_step("Resolve release source and generate release notes")
        self.assertEqual(result.returncode, 0, result.stderr)
        output = (self.root / "output").read_text(encoding="utf-8")
        self.assertIn(f"source_sha={self.old_sha}", output)
        self.assertIn("tag=v1.0.0-preview.10.1", output)

    def test_missing_pushed_tag_fails(self) -> None:
        self.env.update(EVENT_NAME="push", REF_NAME="v-missing")
        result = self.run_step("Resolve release source and generate release notes")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("was not found", result.stdout)

    def test_new_release_is_draft_with_exact_target(self) -> None:
        self.env["DRAFT"] = "true"
        result = self.run_step("Create or update GitHub Release")
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = (self.root / "gh-calls").read_text(encoding="utf-8")
        self.assertIn("create\n", calls)
        self.assertIn("--draft\n", calls)
        self.assertIn(f"--target\n{'a' * 40}\n", calls)
        self.assertIn("--prerelease\n", calls)
        self.assertIn(
            "does not verify ADO publishing",
            (self.root / "summary").read_text(encoding="utf-8"),
        )

    def test_default_explicit_false_and_tag_push_create_published_release(self) -> None:
        for draft, event in (
            ("", "workflow_dispatch"),
            ("false", "workflow_dispatch"),
            ("", "push"),
        ):
            with self.subTest(draft=draft, event=event):
                self.env.update(DRAFT=draft, EVENT_NAME=event)
                result = self.run_step("Create or update GitHub Release")
                self.assertEqual(result.returncode, 0, result.stderr)
                calls = (self.root / "gh-calls").read_text(encoding="utf-8")
                self.assertIn("create\n", calls)
                self.assertNotIn("--draft", calls)
                (self.root / "gh-calls").unlink()

    def test_dispatch_inputs_and_defaults_are_functional(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn("INPUT_DOTNET_VERSION: ${{ inputs.dotnet_version }}", workflow)
        self.assertIn("INPUT_PYTHON_VERSION: ${{ inputs.python_version }}", workflow)
        self.assertIn("DRAFT: ${{ inputs.draft }}", workflow)
        self.assertIn(
            '      draft:\n        description: "Create release as a draft"\n'
            "        required: false\n        default: false\n        type: boolean",
            workflow,
        )

    def test_both_overrides_define_default_tag_and_preserve_source_versions(
        self,
    ) -> None:
        self.env.update(
            INPUT_DOTNET_VERSION="v8.0.0-rc2", INPUT_PYTHON_VERSION="5.0.0b50"
        )
        result = self.run_step("Resolve release source and generate release notes")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(
            "tag=v8.0.0-rc2", (self.root / "output").read_text(encoding="utf-8")
        )
        notes = (self.source / "artifacts" / "release-notes.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("| `3.0.0-preview.30.1` | Preview | Not verified |", notes)
        self.assertIn("| `3.0.0b30` | Beta | Not verified |", notes)
        self.assertIn("| `4.0.0b40` | Beta | Not verified |", notes)
        self.assertEqual(notes.count("| `8.0.0-rc2` | Release candidate |"), 2)
        self.assertEqual(notes.count("| `5.0.0b50` | Beta |"), 2)

    def test_explicit_tag_takes_priority_but_overrides_select_changelogs(self) -> None:
        tag = "v7.0.0-explicit"
        self.env.update(
            INPUT_TAG=tag,
            INPUT_DOTNET_VERSION="8.0.0-rc2",
            INPUT_PYTHON_VERSION="5.0.0b50",
        )
        # Keep the changelog on the tagged source, just as a historical override would use it.
        self.git("checkout", "--detach", self.old_sha)
        py_changelog = self.source / "python" / "CHANGELOG.md"
        py_changelog.write_text(
            "## 5.0.0b50\n\n- Python override fix.\n", encoding="utf-8"
        )
        dotnet_changelog = (
            self.source
            / "dotnet"
            / "src"
            / "Microsoft.Agents.AI.DurableTask"
            / "CHANGELOG.md"
        )
        dotnet_changelog.parent.mkdir(parents=True)
        dotnet_changelog.write_text(
            "## 8.0.0-rc2\n\n- .NET override fix.\n", encoding="utf-8"
        )
        self.commit()
        self.git("tag", tag)
        self.git("checkout", "--detach", self.new_sha)
        result = self.run_step("Resolve release source and generate release notes")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"tag={tag}", (self.root / "output").read_text(encoding="utf-8"))
        notes = (self.source / "artifacts" / "release-notes.md").read_text(
            encoding="utf-8"
        )
        self.assertIn("- Python override fix.", notes)
        self.assertIn("- .NET override fix.", notes)
        self.assertIn("| `1.0.0b10` | `5.0.0b50` | Beta |", notes)
        self.assertIn("| `2.0.0b20` | `5.0.0b50` | Beta |", notes)

    def test_existing_draft_is_updated_without_publishing(self) -> None:
        self.env["MOCK_EXISTING"] = "true"
        for draft in ("", "false", "true"):
            with self.subTest(draft=draft):
                self.env["DRAFT"] = draft
                result = self.run_step("Create or update GitHub Release")
                self.assertEqual(result.returncode, 0, result.stderr)
                calls = (self.root / "gh-calls").read_text(encoding="utf-8")
                self.assertIn("edit\n", calls)
                self.assertIn(f"--target\n{'a' * 40}\n", calls)
                self.assertNotIn("--draft", calls)
                (self.root / "gh-calls").unlink()

    def test_published_release_is_untouched(self) -> None:
        self.env["MOCK_EXISTING"] = "false"
        self.env["DRAFT"] = "true"
        result = self.run_step("Create or update GitHub Release")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((self.root / "gh-calls").exists())

    def test_api_failure_and_duplicate_releases_fail_without_mutating(self) -> None:
        for existing, failure in (("", "true"), ("true\ntrue", "false")):
            with self.subTest(existing=existing, failure=failure):
                self.env.update(MOCK_EXISTING=existing, MOCK_API_FAILURE=failure)
                result = self.run_step("Create or update GitHub Release")
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse((self.root / "gh-calls").exists())


if __name__ == "__main__":
    unittest.main()
