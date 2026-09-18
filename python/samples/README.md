# Durable Task Samples

This directory contains samples for durable agent hosting using the Durable Task Scheduler. These samples demonstrate the worker-client architecture pattern, enabling distributed agent execution with persistent conversation state.

> [!WARNING]
> **Breaking change on this branch.** The unreleased schema-v2 runtime requires
> `DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2` before a sample host starts. Set it only for a
> **new, empty, uniquely named task hub** with upgraded clients and no old or unrelated workers.
> Do not use `default`, an old shared hub, or upgrade a live hub in place. Existing instances
> and recorded workflow histories must remain on their original hub and old engine.
> This is an operator acknowledgement, not proof of isolation or production readiness. There
> is no automatic isolation check, compatibility fallback, or history migration. Follow
> [Environment Configuration](#environment-configuration) for standalone and Azure Functions setup.

## Import convention

These samples import the durable hosting types **directly from the extension packages** —
`agent_framework_durabletask` and `agent_framework_azurefunctions`:

```python
from agent_framework_durabletask import DurableAIAgentWorker, DurableWorkflowClient
from agent_framework_azurefunctions import AgentFunctionApp
```

For backward compatibility these entry-point types are also re-exported from
`agent_framework.azure` in the core `agent-framework` package, so existing
`from agent_framework.azure import ...` code keeps working. **New and updated samples should use
the direct package imports shown above** — the canonical, self-contained path for this repo —
rather than routing through the `agent_framework.azure` shim.

## Quick Prerequisites Checklist

Install and verify these tools before [Running the Samples](#running-the-samples):

- **[Docker](https://docs.docker.com/get-docker/)** – run the Durable Task Scheduler emulator locally
- **[uv](https://docs.astral.sh/uv/)** – manage Python dependencies (optional but recommended)
- **[Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli)** – authenticate with `az login` for `AzureCliCredential`

**Windows (PowerShell):**

```powershell
winget install Docker.DockerDesktop
irm https://astral.sh/uv/install.ps1 | iex
winget install Microsoft.AzureCLI
```

**macOS / Linux:**

```bash
# Docker: https://docs.docker.com/get-docker/
curl -LsSf https://astral.sh/uv/install.sh | sh
# Azure CLI: https://learn.microsoft.com/cli/azure/install-azure-cli
```

**Verify:**

```bash
docker --version
uv --version
az account show
```

## Sample Catalog

### Basic Patterns

- **[01_single_agent](01_single_agent/)**: Host a single conversational agent and interact with it via a client. Demonstrates basic worker-client architecture and agent state management.
- **[02_multi_agent](02_multi_agent/)**: Host multiple domain-specific agents (physicist and chemist) and route requests to the appropriate agent based on the question topic.
- **[03_single_agent_streaming](03_single_agent_streaming/)**: Enable reliable, resumable streaming using Redis Streams with agent response callbacks. Demonstrates non-blocking agent execution and cursor-based resumption for disconnected clients.

### Orchestration Patterns

- **[04_single_agent_orchestration_chaining](04_single_agent_orchestration_chaining/)**: Chain multiple invocations of the same agent using durable orchestration, preserving conversation context across sequential runs.
- **[05_multi_agent_orchestration_concurrency](05_multi_agent_orchestration_concurrency/)**: Run multiple agents concurrently within an orchestration, aggregating their responses in parallel.
- **[06_multi_agent_orchestration_conditionals](06_multi_agent_orchestration_conditionals/)**: Implement conditional branching in orchestrations with spam detection and email assistant agents. Demonstrates structured outputs with Pydantic models and activity functions for side effects.
- **[07_single_agent_orchestration_hitl](07_single_agent_orchestration_hitl/)**: Human-in-the-loop pattern with external event handling, timeouts, and iterative refinement based on human feedback. Shows long-running workflows with external interactions.

### Workflow Hosting Patterns

- **[08_workflow](08_workflow/)**: Host a MAF `Workflow` as a durable orchestration on a standalone worker via `DurableAIAgentWorker.configure_workflow`. Demonstrates conditional routing and mixing AI agents with non-agent executors.
- **[09_workflow_hitl](09_workflow_hitl/)**: A workflow that pauses for human approval using `ctx.request_info` / `@response_handler`, with the client discovering and answering the pending request.
- **[10_workflow_streaming](10_workflow_streaming/)**: Stream a hosted workflow's events as typed `WorkflowEvent` objects by polling the orchestration's custom status.
- **[11_subworkflow](11_subworkflow/)**: Compose workflows by embedding an inner `Workflow` as a node via `WorkflowExecutor`. On the durable host the inner workflow runs as its own child orchestration, and a single `configure_workflow` call registers both.
- **[12_subworkflow_hitl](12_subworkflow_hitl/)**: A human-in-the-loop pause that lives **inside a sub-workflow**. The nested request surfaces to the client with a qualified request id (`{executor}~{ordinal}~{requestId}`) behind a single top-level addressing surface.

### Azure Functions Hosting

These samples host workflows and agents on Azure Durable Functions (`func start`) instead of the worker-client model above. Each has its own setup steps in its README, and shared environment setup lives in [azure_functions/README.md](azure_functions/README.md).

- **[azure_functions/01_single_agent](azure_functions/01_single_agent/)**: Host a single AI agent on Azure Functions with direct HTTP API access for interactive conversations.
- **[azure_functions/02_multi_agent](azure_functions/02_multi_agent/)**: Host multiple AI agents on Azure Functions, each reachable via its own HTTP endpoint.
- **[azure_functions/03_reliable_streaming](azure_functions/03_reliable_streaming/)**: Reliable, resumable streaming for durable agents using Redis Streams with cursor-based reconnection.
- **[azure_functions/04_single_agent_orchestration_chaining](azure_functions/04_single_agent_orchestration_chaining/)**: Chain two invocations of the same agent inside a Durable Functions orchestration, preserving conversation state between runs.
- **[azure_functions/05_multi_agent_orchestration_concurrency](azure_functions/05_multi_agent_orchestration_concurrency/)**: Run two agents in parallel inside a Durable Functions orchestration and merge their responses.
- **[azure_functions/06_multi_agent_orchestration_conditionals](azure_functions/06_multi_agent_orchestration_conditionals/)**: Conditional orchestration that screens emails with a spam-detector agent and drafts replies with an email assistant agent.
- **[azure_functions/07_single_agent_orchestration_hitl](azure_functions/07_single_agent_orchestration_hitl/)**: Human-in-the-loop orchestration where a writer agent iterates until a reviewer approves or the attempt limit is reached.
- **[azure_functions/08_mcp_server](azure_functions/08_mcp_server/)**: Expose agents as both HTTP endpoints and [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) tools.
- **[azure_functions/09_workflow_shared_state](azure_functions/09_workflow_shared_state/)**: Run a MAF `Workflow` with `SharedState` on Azure Durable Functions.
- **[azure_functions/10_workflow_no_shared_state](azure_functions/10_workflow_no_shared_state/)**: Run a MAF `Workflow` on Azure Durable Functions without SharedState.
- **[azure_functions/11_workflow_parallel](azure_functions/11_workflow_parallel/)**: Parallel execution of executors and agents in an Azure Durable Functions workflow.
- **[azure_functions/12_workflow_hitl](azure_functions/12_workflow_hitl/)**: The workflow human-in-the-loop pattern on Azure Durable Functions, with the reviewer notified from inside the workflow via `WorkflowHitlContext`.
- **[azure_functions/13_subworkflow_hitl](azure_functions/13_subworkflow_hitl/)**: A human-in-the-loop pause inside a sub-workflow on Azure Durable Functions, exposed through a single top-level respond surface.

## Running the Samples

These samples are designed to be run locally in a cloned repository.

### Prerequisites

The following prerequisites are required to run the samples:

- [Python 3.9 or later](https://www.python.org/downloads/)
- [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli) installed and authenticated (`az login`)
- [Microsoft Foundry project](https://learn.microsoft.com/azure/foundry/how-to/create-projects) with a deployed model, configured through `FOUNDRY_PROJECT_ENDPOINT` and `FOUNDRY_MODEL` (gpt-4o-mini or better is recommended)
- [Durable Task Scheduler](https://learn.microsoft.com/azure/azure-functions/durable/durable-task-scheduler/develop-with-durable-task-scheduler) (local emulator or Azure-hosted)
- [Docker](https://docs.docker.com/get-docker/) installed if running the Durable Task Scheduler emulator locally

### Configuring RBAC Permissions for Azure OpenAI

These samples are configured to use the Azure OpenAI service with RBAC permissions to access the model. You'll need to configure the RBAC permissions for the Azure OpenAI service to allow the Python app to access the model.

Below is an example of how to configure the RBAC permissions for the Azure OpenAI service to allow the current user to access the model.

Bash (Linux/macOS/WSL):

```bash
az role assignment create \
  --assignee "yourname@contoso.com" \
  --role "Cognitive Services OpenAI User" \
  --scope /subscriptions/<your-subscription-id>/resourceGroups/<your-resource-group-name>/providers/Microsoft.CognitiveServices/accounts/<your-openai-resource-name>
```

PowerShell:

```powershell
az role assignment create `
  --assignee "yourname@contoso.com" `
  --role "Cognitive Services OpenAI User" `
  --scope /subscriptions/<your-subscription-id>/resourceGroups/<your-resource-group-name>/providers/Microsoft.CognitiveServices/accounts/<your-openai-resource-name>
```

More information on how to configure RBAC permissions for Azure OpenAI can be found in the [Azure OpenAI documentation](https://learn.microsoft.com/azure/ai-services/openai/how-to/create-resource?pivots=cli).

### Start Durable Task Scheduler

Most samples use the Durable Task Scheduler (DTS) to support hosted agents and durable orchestrations. DTS also allows you to view the status of orchestrations and their inputs and outputs from a web UI.

To run the Durable Task Scheduler locally, you can use the following `docker` command:

```bash
docker run -d --name dts-emulator -p 8080:8080 -p 8082:8082 mcr.microsoft.com/dts/dts-emulator:latest
```

The DTS dashboard will be available at `http://localhost:8082`.

### Environment Configuration

#### Required isolated-v2 acknowledgement

Choose a new, unique alphanumeric hub name starting with a letter. The examples use
`durablesamplev2UNIQUE` as a placeholder. Replace `UNIQUE` with your own unique alphanumeric
suffix and use the resulting name consistently. Verify the hub is empty and reserved for this
sample deployment. Provision the hub first when using an Azure-hosted scheduler. Do not reuse
`default` or a hub containing old state, even if another sample guide or template uses it.

Upgrade all clients that will access the new hub to match this branch's runtime. Keep old
clients, workers, instances, and recorded histories on the old deployment. Setting the flag or
rewrapping an old start input does not migrate history. Do not hard-code `deployment_mode` in
sample workers or add an automatic fallback to bypass this deployment decision.

#### Standalone samples

Set these variables in **both the worker and client terminals**, using the same chosen hub
name and endpoint. For combined samples, set them in the terminal running the sample.
The endpoint below is for the local emulator.

POSIX shell (Linux/macOS/WSL):

```bash
export FOUNDRY_PROJECT_ENDPOINT="https://your-project.services.ai.azure.com/api/projects/your-project"
export FOUNDRY_MODEL="your-deployment-name"
export ENDPOINT="http://localhost:8080"
export TASKHUB="durablesamplev2UNIQUE"
export DURABLE_AGENTS_DEPLOYMENT_MODE="isolated_v2"
```

PowerShell:

```powershell
$env:FOUNDRY_PROJECT_ENDPOINT="https://your-project.services.ai.azure.com/api/projects/your-project"
$env:FOUNDRY_MODEL="your-deployment-name"
$env:ENDPOINT="http://localhost:8080"
$env:TASKHUB="durablesamplev2UNIQUE"
$env:DURABLE_AGENTS_DEPLOYMENT_MODE="isolated_v2"
```

For host-generated workflows, use the upgraded `DurableWorkflowClient.start_workflow` with
the application input and workflow name (or a constructor default). It supplies the v2 start
envelope. Azure Functions generated workflow start routes do the same. Application-owned
native orchestrators keep their original raw input contracts. Do not wrap their payloads.

#### Azure Functions samples

Copy the sample's local-settings template as described in
[azure_functions/README.md](azure_functions/README.md), then merge these entries into its
`Values` object before `func start`. Keep the existing storage, model, and other sample settings.
Replace every `durablesamplev2UNIQUE` with the same new, empty hub name chosen for that function app.
These settings supersede older instructions to leave the hub as `default`.

```json
{
  "Values": {
    "DURABLE_AGENTS_DEPLOYMENT_MODE": "isolated_v2",
    "TASKHUB_NAME": "durablesamplev2UNIQUE",
    "AzureFunctionsJobHost__extensions__durableTask__hubName": "durablesamplev2UNIQUE",
    "DURABLE_TASK_SCHEDULER_CONNECTION_STRING": "Endpoint=http://localhost:8080;TaskHub=durablesamplev2UNIQUE;Authentication=None"
  }
}
```

The host setting explicitly selects the hub even for samples without a `TASKHUB_NAME` binding.
Keep it, `TASKHUB_NAME`, and the connection string's `TaskHub` identical. The connection string
above is for the local emulator only, not production configuration. See the Azure Functions
[host configuration override guidance](https://learn.microsoft.com/azure/azure-functions/functions-host-json#override-hostjson-values).

### Installing Dependencies

Navigate to the sample directory and install dependencies. For example:

```bash
cd samples/01_single_agent
pip install -r requirements.txt
```

If you're using `uv` for package management:

```bash
uv pip install -r requirements.txt
```

### Starting Workers and Clients

Each sample follows a worker-client architecture. Most samples provide separate `worker.py` and `client.py` files, though some include a combined `sample.py` for convenience.

**Running with separate worker and client:**

In one terminal, start the worker:

```bash
python worker.py
```

In another terminal, run the client:

```bash
python client.py
```

**Running with combined sample:**

```bash
python sample.py
```

### Viewing the Sample Output

The sample output is displayed directly in the terminal where you ran the Python script. Agent responses are printed to stdout with log formatting for better readability.

You can also see the state of agents and orchestrations in the Durable Task Scheduler dashboard at `http://localhost:8082`.
