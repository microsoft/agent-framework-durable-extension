# Engineering pipelines

This directory contains pipeline definitions and shared templates for work that runs outside the normal GitHub pull request flow.

GitHub remains the source of truth for code review and day-to-day development. Pull requests are opened and reviewed in GitHub, and GitHub Actions provide the public build and unit-test signal. End-to-end tests and package publishing run in Azure DevOps, which offers a more controlled environment for identity-based access and credentials.

Azure DevOps is used for jobs that need a more controlled execution environment, especially jobs that require protected credentials or service connections. The Azure DevOps pipelines are defined in this repo so changes can be reviewed with the code, while the pipeline resources, approvals, and secret stores are managed outside the public repository.

## Pipeline layout

| Path | Purpose |
| --- | --- |
| `eng/ci/code-mirror.yml` | Mirrors the GitHub repository into Azure Repos so Azure DevOps can run trusted pipelines from a controlled copy of the source. |
| `eng/ci/integration-tests.yml` | Runs integration tests on pull requests and on a daily schedule, reaching Azure models through a service connection identity. |
| `eng/ci/package-release.yml` | Builds package artifacts, generates unified release notes from `CHANGELOG.md`, and, when explicitly enabled, publishes .NET and Python packages. |
| `eng/scripts/prepare-release.py` | Bumps `.NET` and `Python` package versions and stamps `## [Unreleased]` in `CHANGELOG.md` files (invoked by `.github/workflows/prepare-release.yml`). |
| `.github/workflows/create-release.yml` | Creates a GitHub release (published by default, optionally a draft) or refreshes an existing draft; notes show resolved source versions and any differing effective overrides. |
| `eng/scripts/generate-release-notes.py` | Lists intended packages with their versions and status, and extracts versioned release notes from the `.NET` and `Python` `CHANGELOG.md` files (invoked by `.github/workflows/create-release.yml` and `eng/templates/official/jobs/build-packages.yml`). |
| `eng/templates/jobs/` | Reusable jobs for shared test infrastructure and language-specific integration test runs. |
| `eng/templates/official/jobs/` | Reusable jobs for package build and release stages. |

## Test approach

Pull requests should continue to rely on GitHub Actions for fast validation:

- .NET restore, build, unit tests, and package creation
- Python linting, type checking, unit tests, and package creation

Integration tests run in Azure DevOps because they can require access to protected resources. The integration test pipeline uses a Governed 1ES Linux agent and starts local dependencies on the build agent:

- Durable Task Scheduler emulator
- Azurite
- Redis
- Azure Functions Core Tools

The pipeline is connected to the GitHub repository and runs on pull requests targeting `main`, as well as on a daily schedule. Pull requests from forks do not run: the Azure DevOps organization/project setting "Limit building pull requests from forked GitHub repositories" is set to disable fork builds, so only team-authored pull requests run and secrets are never exposed to forks.

Access to Azure OpenAI and Foundry models uses the identity of an Azure service connection (no API keys). Model endpoints and deployment or model names are provided as non-secret pipeline variables. As a result, the integration test pipeline does not rely on any secret variables.

## Release approach

### Versions and source snapshots

.NET and Python packages share development on `main`, but use ecosystem-specific versions. The two .NET packages use the central release version in `dotnet/nuget/nuget-package.props`; each Python package declares its version in its own `python/packages/*/pyproject.toml`. NuGet preview versions and Python beta versions are distinct version schemes, not different branches or guarantees of stable support.

Release tags denote a coordinated source snapshot. A tag named after the .NET version does not assign that version to Python packages, nor does it prove that every package in the snapshot has shipped. Release notes list all four packages with their individual source versions and maturity status, read from the resolved source commit. Differing release-note versions are shown separately so overrides are not mistaken for source or published versions. Changelog and source links are pinned to that commit.

Use `main` for ongoing development. Create an older-release maintenance branch only when independently servicing that older release; a separate branch is not needed merely because .NET and Python versions differ.

### Package publication

Package release also runs in Azure DevOps. The release pipeline builds the .NET and Python artifacts first, then requires an explicit publish decision.

Publishing is disabled by default. Maintainers should first run the release pipeline as a dry run, inspect the generated artifacts, confirm the package versions, and only then rerun with the appropriate publish option enabled.

The release pipeline is structured to publish:

- .NET NuGet packages and symbol packages
- Python source distribution and wheel packages

.NET publishing uses an Azure DevOps service connection; Python publishing uses ESRP with a managed identity and signing certificate. Credentials and approvals are managed outside this repository. If the release process changes, update the pipeline templates in this directory rather than putting credentials or internal setup notes in the repository.

### Publishing the GitHub release

Creating a GitHub release is separate from registry publication. `.github/workflows/create-release.yml` preserves the existing manual-dispatch inputs and defaults: **`draft=false` publishes a newly created GitHub release immediately**, while `draft=true` keeps it unpublished. Tag pushes also create published releases by default. `draft` affects GitHub release visibility only; it neither starts nor waits for independent ADO package publishing.

For an existing tag, source versions and changelogs come from the tagged commit even if `main` has advanced. For a new tag requested by manual dispatch, they come from the selected target commit. `dotnet_version` overrides the .NET release-note version and determines the default `v<version>` tag when `tag` is omitted; otherwise the default tag comes from the target's `GitTag`. If no explicit .NET override is supplied, a `v*` tag supplies the .NET release-note version as before. `python_version` overrides the release-note version for both Python packages. These effective versions select changelog sections; they do not change package manifests, artifacts, or publishing. Valid overrides that differ from source versions are accepted and explicitly shown alongside source versions in the notes.

Rerunning the workflow refreshes an existing draft's notes without changing its draft status, even when `draft=false`; it leaves already-published releases untouched, including when `draft=true`. Drafts are visible only to repository writers. All generated notes mark registry publication as unverified regardless of GitHub visibility. Generated notes in a dry-run ADO artifact also do not prove publication or signing. Authenticode and NuGet signing run only when .NET publication is enabled.

There is **no enforced ADO-to-GitHub publication gate**. We recommend using `draft=true` and completing these checks before publishing the draft, or before creating a new release with the default `draft=false`:

1. Resolve the release tag to its full source commit SHA and confirm the matching ADO run built that same commit (not just the same branch name). For a new draft whose tag is not yet created, use its recorded source snapshot/target SHA and confirm the tag resolves to it when publishing.
2. Compare each intended package/version in the notes with the ADO build artifacts and selected publish options. Resolve any source-versus-override discrepancy using actual artifact metadata and registry availability. A successful dry run, manual publish approval, or one ecosystem's successful job is not confirmation that all packages shipped.
3. Confirm the corresponding publish jobs completed successfully and each intended package/version is actually available in its registry. For .NET, also confirm the signing verification steps and symbol-package publication; for Python, confirm both wheel and source distributions. Account for registry propagation before proceeding.
4. Update every package's publication column to `Published` or explicitly `Not published` with a reason. Record the actual published version and any omitted ecosystem/package rather than presenting an override or omission as shipped. Replace the unverified-publication notice with a factual publication summary referencing the confirmed source SHA and ADO run (keep protected run links in internal release records).
5. Publish the reviewed draft in the GitHub release editor, preserving its prerelease status for preview/beta packages. Alternatively, after updating the notes, use `gh release edit <tag> --draft=false`.

If publication fails, is partial, or cannot be confirmed, we recommend keeping the GitHub release as a draft until publication is verified or omissions are explicitly reviewed and documented. The workflow does not enforce this recommendation or automatically publish an existing draft after ADO completion. Do not rerun publishing blindly: already-published package versions can conflict on retry. This check does not change ADO publishing, credentials, release preparation, versions, or the tagging scheme.

## Maintainer notes

- Keep public pipeline documentation in this file high level and safe to share.
- Keep organization-specific setup steps, links, permissions, and secret names in the internal team docs.
- Review pipeline changes the same way as product code changes.
- Prefer dry-run package builds before enabling publish steps.
- Run release tooling checks with `python -m unittest discover -s eng/scripts/tests -v` (Python 3.11+ and bash/git required).
