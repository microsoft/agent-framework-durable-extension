# Microsoft Agent Framework Durable Extension

This repository contains the Durable Task extensions for Microsoft Agent Framework across .NET and Python.

The migrated code covers:

- .NET durable agents and durable workflows for console apps.
- .NET Azure Functions hosting for durable agents and workflows.
- Python Durable Task integration for Microsoft Agent Framework.
- Unit and integration tests, samples, package build metadata, and CI workflows for the Durable extension.

For the full Agent Framework source, see [microsoft/agent-framework](https://github.com/microsoft/agent-framework).

## Repository layout

```text
dotnet/
  src/
    Microsoft.Agents.AI.DurableTask/
    Microsoft.Agents.AI.Hosting.AzureFunctions/
  tests/
    Microsoft.Agents.AI.DurableTask.UnitTests/
    Microsoft.Agents.AI.DurableTask.IntegrationTests/
    Microsoft.Agents.AI.Hosting.AzureFunctions.UnitTests/
    Microsoft.Agents.AI.Hosting.AzureFunctions.IntegrationTests/
  samples/
    DurableAgents/
    DurableWorkflows/
python/
  packages/durabletask/
  samples/
docs/features/durable-agents/
schemas/
```

## Dependencies

Non-Durable Agent Framework components are consumed as published packages instead of local project references.

- .NET uses NuGet packages such as `Microsoft.Agents.AI`, `Microsoft.Agents.AI.Workflows`, and `Microsoft.Agents.AI.OpenAI`.
- Python uses PyPI packages such as `agent-framework-core`, `agent-framework-foundry`, and `agent-framework-openai`.

## Build and test

### .NET

```powershell
Set-Location dotnet
dotnet restore .\agent-framework-durable-extension.slnx
dotnet build .\agent-framework-durable-extension.slnx --configuration Release --no-restore
dotnet test --project .\tests\Microsoft.Agents.AI.DurableTask.UnitTests\Microsoft.Agents.AI.DurableTask.UnitTests.csproj --configuration Release --no-build
dotnet test --project .\tests\Microsoft.Agents.AI.Hosting.AzureFunctions.UnitTests\Microsoft.Agents.AI.Hosting.AzureFunctions.UnitTests.csproj --configuration Release --no-build
```

Package artifacts:

```powershell
dotnet pack .\src\Microsoft.Agents.AI.DurableTask\Microsoft.Agents.AI.DurableTask.csproj --configuration Release --output .\artifacts\packages
dotnet pack .\src\Microsoft.Agents.AI.Hosting.AzureFunctions\Microsoft.Agents.AI.Hosting.AzureFunctions.csproj --configuration Release --output .\artifacts\packages
```

### Python

```powershell
Set-Location python
uv sync --all-packages --group dev --group test
uv run pytest -m "not integration" packages\durabletask\tests
uv build packages\durabletask --out-dir dist
```

## Integration tests

Integration tests require local infrastructure:

- Durable Task Scheduler emulator
- Azurite
- Redis
- Azure Functions Core Tools for Azure Functions samples/tests
- Foundry environment variables for model-backed tests

Common environment variables:

```text
DURABLE_TASK_SCHEDULER_CONNECTION_STRING=Endpoint=http://localhost:8080;TaskHub=default;Authentication=None
AzureWebJobsStorage=UseDevelopmentStorage=true
REDIS_CONNECTION_STRING=redis://localhost:6379
FOUNDRY_PROJECT_ENDPOINT=<foundry-project-endpoint>
FOUNDRY_MODEL=<foundry-model>
```

The `.github/actions/azure-functions-integration-setup` action starts the emulator dependencies in CI.

## Samples

- .NET Durable Agents: `dotnet\samples\DurableAgents`
- .NET Durable Workflows: `dotnet\samples\DurableWorkflows`
- Python Durable Task samples: `python\samples`

Python samples are copied directly under `python\samples`; there is no redundant `durabletask` path segment.
