# Azure Functions Samples

These are common instructions for setting up your environment for every sample in this directory.
These samples illustrate the Durable extensibility for Agent Framework running in Azure Functions.

All of these samples are set up to run in Azure Functions. Azure Functions has a local development tool called [CoreTools](https://learn.microsoft.com/azure/azure-functions/functions-run-local?tabs=windows%2Cpython%2Cv2&pivots=programming-language-python#install-the-azure-functions-core-tools) which we will set up to run these samples locally.

## Import convention

These samples import `AgentFunctionApp` (and other hosting types) **directly from the extension
packages** (`agent_framework_azurefunctions`, `agent_framework_durabletask`):

```python
from agent_framework_azurefunctions import AgentFunctionApp
```

The same entry-point types are also re-exported from `agent_framework.azure` in the core
`agent-framework` package for backward compatibility. **New and updated samples should use the
direct package imports above** — the canonical, self-contained path for this repo — rather than
routing through the `agent_framework.azure` shim.

## Quick Prerequisites Checklist

Install and verify these tools before [Environment Setup](#environment-setup):

- **[Azure Functions Core Tools](https://learn.microsoft.com/azure/azure-functions/functions-run-local?tabs=windows%2Cpython%2Cv2&pivots=programming-language-python#install-the-azure-functions-core-tools)** – run samples locally with `func start`
- **[Azurite](https://learn.microsoft.com/azure/storage/common/storage-install-azurite)** – local storage emulator; must be running before `func start`
- **[Docker](https://docs.docker.com/get-docker/)** – run the local Durable Task Scheduler emulator
- **[Durable Task Scheduler emulator](https://learn.microsoft.com/azure/durable-task/scheduler/develop-with-durable-task-scheduler#durable-task-scheduler-emulator)** – local durable task backend; must be running before `func start`
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
docker --version
uv --version
az account show
```

Start the Durable Task Scheduler emulator and Azurite before `func start`:

```bash
docker run -d --name dts-emulator -p 8080:8080 -p 8082:8082 \
  mcr.microsoft.com/dts/dts-emulator:latest
azurite
```

The scheduler endpoint is `http://localhost:8080`; its dashboard is available at
`http://localhost:8082`.

## Environment Setup

### 1. Install dependencies and create appropriate services

- Install [Azure Functions Core Tools 4.x](https://learn.microsoft.com/azure/azure-functions/functions-run-local?tabs=windows%2Cpython%2Cv2&pivots=programming-language-python#install-the-azure-functions-core-tools)

- Install [Azurite storage emulator](https://learn.microsoft.com/azure/storage/common/storage-install-azurite?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json&tabs=visual-studio%2Cblob-storage)

- Install [Docker](https://docs.docker.com/get-docker/) and run the [Durable Task Scheduler emulator](https://learn.microsoft.com/azure/durable-task/scheduler/develop-with-durable-task-scheduler#durable-task-scheduler-emulator)

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

- Start the Durable Task Scheduler emulator and [Azurite](https://learn.microsoft.com/azure/storage/common/storage-install-azurite?tabs=npm%2Cblob-storage#run-azurite) as shown above.

- Inside each sample:
  - Install Python dependencies – from the sample directory, run `pip install -r requirements.txt` (or the equivalent in your active virtual environment).
  - Copy the supplied `local.settings.json.template` or `local.settings.json.sample` to `local.settings.json`.
  - Configure the Foundry variables in that file (`FOUNDRY_PROJECT_ENDPOINT` and `FOUNDRY_MODEL`). The samples use `AzureCliCredential`, so ensure you're logged in via `az login`.
    - Keep `TASKHUB_NAME` set to `default` unless you plan to change the durable task hub name.
  - Run the command `func start` from the root of the sample
  - Follow each sample's README for scenario-specific steps, and use its `demo.http` file (or provided curl examples) to trigger the hosted HTTP endpoints.
