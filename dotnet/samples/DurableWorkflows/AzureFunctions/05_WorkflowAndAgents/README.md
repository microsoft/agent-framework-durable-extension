# Workflow and Agents Sample

This sample demonstrates how to register **both** AI agents **and** workflows in a single Azure Functions app, using separate `ConfigureDurableAgents` and `ConfigureDurableWorkflows` calls.

These methods compose: call them in any order, as many times as you like, and the configurations are additive.

```csharp
using IHost app = FunctionsApplication
    .CreateBuilder(args)
    .ConfigureFunctionsWebApplication()
    .ConfigureDurableAgents(agents => agents.AddAIAgent(assistant, enableHttpTrigger: true, enableMcpToolTrigger: true))
    .ConfigureDurableWorkflows(workflows => workflows.AddWorkflow(translateWorkflow, enableMcpToolTrigger: true))
    .Build();
app.Run();
```

If you prefer to configure everything from a single delegate, `ConfigureDurableOptions` is an equivalent alternative and can be freely mixed with the two methods above:

```csharp
    .ConfigureDurableOptions(options =>
    {
        options.Agents.AddAIAgent(assistant, enableHttpTrigger: true, enableMcpToolTrigger: true);
        options.Workflows.AddWorkflow(translateWorkflow, enableMcpToolTrigger: true);
    })
```

## Key Concepts Demonstrated

- **Composable Configuration**: `ConfigureDurableAgents` and `ConfigureDurableWorkflows` combine in the same app
- **Standalone Agent**: An AI agent accessible via HTTP and MCP tool triggers
- **Workflow**: A simple text translation workflow also exposed as an MCP tool
- **Mixed Triggers**: Both agents and workflows coexist in the same Functions host

## Sample Architecture

### Standalone Agent

| Agent | Description |
|-------|-------------|
| **Assistant** | A general-purpose AI assistant accessible via HTTP (`/agents/Assistant/run`) and as an MCP tool |

### Translate Workflow

| Executor | Input | Output | Description |
|----------|-------|--------|-------------|
| **TranslateText** | `string` | `TranslationResult` | Converts input text to uppercase |
| **FormatOutput** | `TranslationResult` | `string` | Formats the result into a readable string |

## Environment Setup

See the [README.md](../../README.md) file in the parent directory for complete setup instructions, including:

- Prerequisites installation
- Durable Task Scheduler setup
- Storage emulator configuration

This sample also requires Foundry project configuration. Copy `local.settings.json.template` to `local.settings.json`, then set the following values:

- `FOUNDRY_PROJECT_ENDPOINT`: Your Foundry project endpoint URL
- `FOUNDRY_MODEL`: Your Foundry model deployment name

## Running the Sample

1. **Start the Function App**:

   ```bash
   cd dotnet/samples/DurableWorkflows/AzureFunctions/05_WorkflowAndAgents
   func start
   ```

2. **Expected Functions**: When the app starts, you should see functions for both the agent and the workflow:

   - `dafx-Assistant` (entity trigger for the agent)
   - `http-Assistant` (HTTP trigger for the agent)
   - `mcptool-Assistant` (MCP tool trigger for the agent)
   - `wf-Translate` (orchestration trigger for the workflow)
   - `mcptool-wf-Translate` (MCP tool trigger for the workflow)

## Invoking the Agent via HTTP

```bash
curl -X POST http://localhost:7071/agents/Assistant/run \
  -H "Content-Type: application/json" \
  -d '{"query": "What is the capital of France?"}'
```

## Invoking via MCP Inspector

1. Install and run the [MCP Inspector](https://modelcontextprotocol.io/docs/tools/inspector):

   ```bash
   npx @modelcontextprotocol/inspector
   ```

2. Connect to `http://localhost:7071/runtime/webhooks/mcp` using **Streamable HTTP** transport.

3. Click **List Tools** to see both the `Assistant` agent tool and the `Translate` workflow tool.
