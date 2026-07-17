# Engineering pipelines

This directory contains pipeline definitions and shared templates for work that runs outside the normal GitHub pull request flow.

GitHub remains the source of truth for code review and day-to-day development. Pull requests are opened and reviewed in GitHub, and GitHub Actions provide the public build and unit-test signal. End-to-end tests and package publishing run in Azure DevOps, which offers a more controlled environment for identity-based access and credentials.

Azure DevOps is used for jobs that need a more controlled execution environment, especially jobs that require protected credentials or service connections. The Azure DevOps pipelines are defined in this repo so changes can be reviewed with the code, while the pipeline resources, approvals, and secret stores are managed outside the public repository.

## Pipeline layout

| Path | Purpose |
| --- | --- |
| `eng/ci/code-mirror.yml` | Mirrors the GitHub repository into Azure Repos so Azure DevOps can run trusted pipelines from a controlled copy of the source. |
| `eng/ci/integration-tests.yml` | Runs integration tests on pull requests and on a daily schedule, reaching Azure models through a service connection identity. |
| `eng/ci/package-release.yml` | Builds package artifacts and, when explicitly enabled, publishes .NET and Python packages. |
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

Package release also runs in Azure DevOps. The release pipeline builds the .NET and Python artifacts first, then requires an explicit publish decision.

Publishing is disabled by default. Maintainers should first run the release pipeline as a dry run, inspect the generated artifacts, confirm the package versions, and only then rerun with the appropriate publish option enabled.

The release pipeline is structured to publish:

- .NET NuGet packages and symbol packages
- Python source distribution and wheel packages

The publishing credentials are held inside Azure DevOps service connections for NuGet.org and PyPI. The pipeline never reads these credentials directly, so there are no publishing secrets in this repository or in pipeline variables. If the release process changes, update the pipeline templates in this directory rather than putting credentials or internal setup notes in the repository.

## Maintainer notes

- Keep public pipeline documentation in this file high level and safe to share.
- Keep organization-specific setup steps, links, permissions, and secret names in the internal team docs.
- Review pipeline changes the same way as product code changes.
- Prefer dry-run package builds before enabling publish steps.
