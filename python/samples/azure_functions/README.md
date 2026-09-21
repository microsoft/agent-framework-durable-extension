# Azure Functions Samples

These are common instructions for setting up your environment for every sample in this directory.
These samples illustrate the Durable extensibility for Agent Framework running in Azure Functions.

> [!WARNING]
> This branch requires `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2` before `func start`.
> Use only a **new, empty, uniquely named task hub** with matching upgraded clients and no old
> or unrelated workers. Start fresh workflow instances after this update, including upgrades from
> earlier v2 builds. Protocol `2` is unchanged, but older v2 in-flight replay is unsupported and may
> fail under the revised HITL checkpoints and mixed parent/child scheduling. The marker checks start
> admission, not feature or replay compatibility. Keep old runs on their original deployment if
> they must finish. This is an operator acknowledgement, not proof of isolation or production readiness.
> There is no automatic compatibility fallback or history migration. See the shared
> [Environment Configuration](../README.md#environment-configuration) for the deployment requirements.

All of these samples are set up to run in Azure Functions. Azure Functions has a local development tool called [CoreTools](https://learn.microsoft.com/azure/azure-functions/functions-run-local?tabs=windows%2Cpython%2Cv2&pivots=programming-language-python#install-the-azure-functions-core-tools) which we will set up to run these samples locally.

## Import convention

These samples import `AgentFunctionApp` (and other hosting types) **directly from the extension
packages** (`agent_framework_azurefunctions`, `agent_framework_durabletask`):

```python
from agent_framework_azurefunctions import AgentFunctionApp
```

The same entry-point types are also re-exported from `agent_framework.azure` in the core
`agent-framework` package for backward compatibility. **New and updated samples should use the
direct package imports above** rather than the `agent_framework.azure` shim.

## History and Retention Sample

- **[14_conversation_compaction](14_conversation_compaction/)** shows durable compaction with independent eager-pruning and explicit local byte-budget settings.

## Retention Defaults

`AgentFunctionApp` defaults to `retention="keep_all"` and `max_state_bytes=None`.
`follow_compaction` enables eligible eager pruning. An independent positive byte budget enables
pressure eviction at the `0.85` high watermark toward the `0.70` low watermark, subject to protected
state. Functions rejects `"backend_limit"`, even with DTS. Whole-entity ASCII-escaped JSON size is
a Python-host estimate, not a backend acceptance guarantee.

Agent and workflow budget overrides distinguish `INHERIT` from explicit `None`, which disables pressure
eviction. Protected delivery records and session state can cause `StateCapacityError` rather than
being deleted to fit. There is no bounded receipt cleanup. Idle response expiry requires
application-owned maintenance, and retention metrics never confirm durable commits. See the
[Functions retention contract](../../packages/azurefunctions/README.md#retention-and-state-budgets)
and [metric semantics](../../packages/azurefunctions/README.md#retention-metrics).

## Quick Prerequisites Checklist

Install and verify these tools before [Environment Setup](#environment-setup):

- **[Python 3.10 or later](https://www.python.org/downloads/)**
- **[Azure Functions Core Tools](https://learn.microsoft.com/azure/azure-functions/functions-run-local?tabs=windows%2Cpython%2Cv2&pivots=programming-language-python#install-the-azure-functions-core-tools)** – run samples locally with `func start`
- **[Azurite](https://learn.microsoft.com/azure/storage/common/storage-install-azurite)** – local storage emulator, required before `func start`
- **[Docker](https://docs.docker.com/get-docker/)** and the **[Durable Task Scheduler emulator](https://learn.microsoft.com/azure/durable-task/scheduler/develop-with-durable-task-scheduler#durable-task-scheduler-emulator)** are optional for the [DTS backend](#optional-durable-task-scheduler-backend), not required by the shipped Azure Storage configuration.
- **[uv](https://docs.astral.sh/uv/)** – create virtual environments (recommended, especially on Windows)
- **[Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli)** – authenticate with `az login` for `AzureCliCredential`

**Windows (PowerShell):**

```powershell
winget install Microsoft.Azure.FunctionsCoreTools
npm install -g azurite
irm https://astral.sh/uv/install.ps1 | iex
winget install Microsoft.AzureCLI
```

**macOS:**

```bash
brew tap azure/functions
brew install azure-functions-core-tools@4
npm install -g azurite
curl -LsSf https://astral.sh/uv/install.sh | sh
# Azure CLI: https://learn.microsoft.com/cli/azure/install-azure-cli
```

**Linux:**

```bash
npm install -g azure-functions-core-tools@4 --unsafe-perm true
npm install -g azurite
curl -LsSf https://astral.sh/uv/install.sh | sh
# Azure CLI: https://learn.microsoft.com/cli/azure/install-azure-cli
```

**Verify:**

```bash
func --version
azurite --version
uv --version
az account show
```

Start Azurite before `func start` when using the supplied `UseDevelopmentStorage=true` setting:

```bash
azurite
```

The samples' host configurations omit `storageProvider`, so Durable Functions uses **Azure Storage
by default**, with `AzureWebJobsStorage` pointing to Azurite locally. The supplied
`DURABLE_TASK_SCHEDULER_CONNECTION_STRING` value does not select a backend and is unused by that
default provider. These deployments do not appear in the DTS dashboard.

## Environment Setup

### 1. Install dependencies and create appropriate services

- Install [Azure Functions Core Tools 4.x](https://learn.microsoft.com/azure/azure-functions/functions-run-local?tabs=windows%2Cpython%2Cv2&pivots=programming-language-python#install-the-azure-functions-core-tools)

- Install [Azurite storage emulator](https://learn.microsoft.com/azure/storage/common/storage-install-azurite?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json&tabs=visual-studio%2Cblob-storage)

- Only for an explicitly configured DTS deployment, install [Docker](https://docs.docker.com/get-docker/) and follow the [optional backend setup](#optional-durable-task-scheduler-backend).

- Create a [Microsoft Foundry project](https://learn.microsoft.com/azure/ai-foundry/) with an OpenAI model deployment. Note the Foundry project endpoint and deployment name, and ensure you can authenticate with `AzureCliCredential`.

- Install a tool to execute HTTP calls, for example the [REST Client extension](https://marketplace.visualstudio.com/items?itemName=humao.rest-client)

- [Optionally] Create an [Azure Function Python app](https://learn.microsoft.com/azure/azure-functions/functions-create-function-app-portal?tabs=core-tools&pivots=flex-consumption-plan) to later deploy your app to Azure if you so desire.

### 2. Create and activate a virtual environment

Using [uv](https://docs.astral.sh/uv/) (recommended):

**Windows (PowerShell):**

```powershell
uv venv .venv
.venv\Scripts\Activate.ps1
```

**Linux/macOS:**

```bash
uv venv .venv
source .venv/bin/activate
```

> [!NOTE]
> The `python -m venv .venv` command also works, but can hang indefinitely on Windows with Microsoft Store Python due to a known `ensurepip` issue. Use `uv venv .venv` to avoid this.

### 3. Running the samples

- Start [Azurite](https://learn.microsoft.com/azure/storage/common/storage-install-azurite?tabs=npm%2Cblob-storage#run-azurite) as shown above. Start DTS only if you explicitly select it as described below.

- Inside each sample:
  - Install Python dependencies – from the sample directory, run `pip install -r requirements.txt` (or the equivalent in your active virtual environment).
  - Copy the supplied `local.settings.json.template` or `local.settings.json.sample` to `local.settings.json`.
  - Configure the Foundry variables in that file (`FOUNDRY_PROJECT_ENDPOINT` and `FOUNDRY_MODEL`). The samples use `AzureCliCredential`, so ensure you're logged in via `az login`.
  - Merge these required settings into the file's `Values` object, preserving the storage,
    model, and other sample settings. Do not keep `TASKHUB_NAME` set to `default`.

    ```json
    {
      "Values": {
        "DURABLE_AGENTS_DEPLOYMENT_MODE": "isolated_v2",
        "TASKHUB_NAME": "durablesamplev2UNIQUE",
        "AzureFunctionsJobHost__extensions__durableTask__hubName": "durablesamplev2UNIQUE"
      }
    }
    ```

    Replace `UNIQUE` with your own unique alphanumeric suffix for a new, empty hub reserved for
    this deployment in its actual storage backend. Keep the two hub values identical. The host
    override also covers samples without a `TASKHUB_NAME` binding. With the default backend,
    isolation is within the storage account or Azurite instance, not a DTS scheduler.

  - Run the command `func start` from the root of the sample
  - Follow each sample's README for scenario-specific steps, and use its `demo.http` file (or provided curl examples) to trigger the hosted HTTP endpoints.

## Optional Durable Task Scheduler backend

This is an explicit choice for a **new deployment**, not the default and not a migration of existing
state. Before starting that deployment, merge this configuration into its host configuration,
preserving other settings. Use a host extension bundle that supports `azureManaged`. The
[DTS quickstart](https://learn.microsoft.com/azure/durable-task/scheduler/quickstart-durable-task-scheduler?pivots=python)
requires bundle version 4.32.0 or later for Python.

```json
{
  "extensionBundle": {
    "id": "Microsoft.Azure.Functions.ExtensionBundle",
    "version": "[4.32.0, 5.0.0)"
  },
  "extensions": {
    "durableTask": {
      "hubName": "%TASKHUB_NAME%",
      "storageProvider": {
        "type": "azureManaged",
        "connectionStringName": "DURABLE_TASK_SCHEDULER_CONNECTION_STRING"
      }
    }
  }
}
```

Set `DURABLE_TASK_SCHEDULER_CONNECTION_STRING` in the local settings `Values` to
`Endpoint=http://localhost:8080;TaskHub=durablesamplev2UNIQUE;Authentication=None` for the emulator.
Replace the hub placeholder with the same fresh hub as `TASKHUB_NAME` and any host hub override.
Compatible clients must target that same hub and scheduler. Provision the hub first for an
Azure-hosted scheduler and use its authenticated connection configuration instead.

For local DTS, start the emulator with dynamic hubs enabled, as CI does:

```bash
docker run -d --name dts-emulator -p 8080:8080 -p 8082:8082 \
  -e DTS_USE_DYNAMIC_TASK_HUBS=true mcr.microsoft.com/dts/dts-emulator:latest
```

The endpoint is `http://localhost:8080` and the dashboard is `http://localhost:8082`. Keep Azurite
running for the supplied `AzureWebJobsStorage` host-storage setting, even when durable state uses
DTS. Dynamic hub creation does not verify deployment isolation. Check the host startup logs for
the selected provider and hub before sending work.
