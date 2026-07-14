# Engineering pipelines

This directory contains pipeline definitions and shared templates for work that runs outside the normal GitHub pull request flow.

GitHub remains the source of truth for code review and day-to-day development. Pull requests are opened and reviewed in GitHub, and the GitHub Actions workflows provide the public build and unit-test signal for ordinary changes.

Azure DevOps is used for jobs that need a more controlled execution environment, especially jobs that require protected credentials or service connections. The Azure DevOps pipelines are defined in this repo so changes can be reviewed with the code, while the pipeline resources, approvals, and secret stores are managed outside the public repository.

## Pipeline layout

| Path | Purpose |
| --- | --- |
| `eng/ci/code-mirror.yml` | Mirrors the GitHub repository into Azure Repos so Azure DevOps can run trusted pipelines from a controlled copy of the source. |
| `eng/ci/e2e-tests.yml` | Runs end-to-end tests that require protected model credentials or service connections. |
| `eng/ci/package-release.yml` | Builds package artifacts and, when explicitly enabled, publishes .NET and Python packages. |
| `eng/templates/jobs/` | Reusable jobs for shared test infrastructure and language-specific E2E test runs. |
| `eng/templates/official/jobs/` | Reusable jobs for package build and release stages. |

## Test approach

Pull requests should continue to rely on GitHub Actions for fast validation:

- .NET restore, build, unit tests, and package creation
- Python linting, type checking, unit tests, and package creation

End-to-end tests run in Azure DevOps because they can require protected resources. The E2E pipeline starts local dependencies on the build agent:

- Durable Task Scheduler emulator
- Azurite
- Redis
- Azure Functions Core Tools

Model endpoints, deployment names, API keys, and service connections are supplied by protected Azure DevOps variables or service connections. They should not be stored in this repository.

## Release approach

Package release also runs in Azure DevOps. The release pipeline builds the .NET and Python artifacts first, then requires an explicit publish decision.

Publishing is disabled by default. Maintainers should first run the release pipeline as a dry run, inspect the generated artifacts, confirm the package versions, and only then rerun with the appropriate publish option enabled.

The release pipeline is structured to publish:

- .NET NuGet packages and symbol packages
- Python source distribution and wheel packages

The exact publishing credentials or service connections are managed in Azure DevOps. If the release process changes, update the pipeline templates in this directory rather than putting credentials or internal setup notes in the repository.

## Maintainer notes

- Keep public pipeline documentation in this file high level and safe to share.
- Keep organization-specific setup steps, links, permissions, and secret names in the internal team docs.
- Review pipeline changes the same way as product code changes.
- Prefer dry-run package builds before enabling publish steps.
