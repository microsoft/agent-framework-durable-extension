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
| `.github/workflows/create-release.yml` | Creates or refreshes a draft GitHub release from the resolved source snapshot; publication requires the maintainer check below. |
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

Release tags denote a coordinated source snapshot. A tag named after the .NET version does not assign that version to Python packages, nor does it prove that every package in the snapshot has shipped. Release notes list all four intended packages with their individual versions and maturity status, read from the release source rather than inferred from the tag name. Changelog and source links are pinned to that commit.

Use `main` for ongoing development. Create an older-release maintenance branch only when independently servicing that older release; a separate branch is not needed merely because .NET and Python versions differ.

### Package publication

Package release also runs in Azure DevOps. The release pipeline builds the .NET and Python artifacts first, then requires an explicit publish decision.

Publishing is disabled by default. Maintainers should first run the release pipeline as a dry run, inspect the generated artifacts, confirm the package versions, and only then rerun with the appropriate publish option enabled.

The release pipeline is structured to publish:

- .NET NuGet packages and symbol packages
- Python source distribution and wheel packages

.NET publishing uses an Azure DevOps service connection; Python publishing uses ESRP with a managed identity and signing certificate. Credentials and approvals are managed outside this repository. If the release process changes, update the pipeline templates in this directory rather than putting credentials or internal setup notes in the repository.

### Publishing the GitHub release

Creating a GitHub release is separate from registry publication. `.github/workflows/create-release.yml` always creates a **draft**, whether triggered by a tag push or manually dispatched. For an existing tag it generates notes from the tagged commit, even if `main` has advanced. For a new tag requested by manual dispatch it uses the selected target commit. Version overrides are not offered by this workflow: package versions come from that source snapshot.

Rerunning the workflow refreshes an existing draft's notes, but leaves already-published releases untouched. Drafts are visible only to repository writers, and their package table explicitly marks publication as pending verification. Generated notes in a dry-run ADO artifact also do not prove publication or signing. Authenticode and NuGet signing run only when .NET publication is enabled.

There is no automated ADO-to-GitHub publication confirmation in this workflow. A maintainer with release write access must complete this check before publishing the draft:

1. Resolve the release tag to its full source commit SHA and confirm the matching ADO run built that same commit (not just the same branch name). For a new draft whose tag is not yet created, use its recorded source snapshot/target SHA and confirm the tag resolves to it when publishing.
2. Compare each intended package/version in the draft with the ADO build artifacts and selected publish options. A successful dry run, manual publish approval, or one ecosystem's successful job is not confirmation that all packages shipped.
3. Confirm the corresponding publish jobs completed successfully and each intended package/version is actually available in its registry. For .NET, also confirm the signing verification steps and symbol-package publication; for Python, confirm both wheel and source distributions. Account for registry propagation before proceeding.
4. Update every package's publication column to `Published` or explicitly `Not published` with a reason. Record any omitted ecosystem/package rather than presenting it as shipped. Replace the pending-verification notice with a factual publication summary referencing the confirmed source SHA and ADO run (keep protected run links in internal release records).
5. Publish the reviewed draft in the GitHub release editor, preserving its prerelease status for preview/beta packages. Alternatively, after updating the notes, use `gh release edit <tag> --draft=false`.

If publication fails, is partial, or cannot be confirmed, keep the GitHub release as a draft until the intended publication is verified or omissions are explicitly reviewed and documented. Do not rerun publishing blindly: already-published package versions can conflict on retry. This check does not change ADO publishing, credentials, release preparation, versions, or the tagging scheme.

## Maintainer notes

- Keep public pipeline documentation in this file high level and safe to share.
- Keep organization-specific setup steps, links, permissions, and secret names in the internal team docs.
- Review pipeline changes the same way as product code changes.
- Prefer dry-run package builds before enabling publish steps.
- Run release tooling checks with `python -m unittest discover -s eng/scripts/tests -v` (Python 3.11+ and bash/git required).
