// Copyright (c) Microsoft. All rights reserved.

// This sample demonstrates registering BOTH agents AND workflows in a single Azure Functions app.
// It uses a workflow to translate text and a standalone AI agent accessible via HTTP and MCP tool
// triggers.
//
// ConfigureDurableAgents and ConfigureDurableWorkflows compose: call them in any order, as many
// times as you like, and the configurations are additive. ConfigureDurableOptions is an equivalent
// alternative that configures both from a single delegate - see the README for that variant.

#pragma warning disable IDE0002 // Simplify Member Access

using Azure.AI.OpenAI;
using Azure.Identity;
using Microsoft.Agents.AI;
using Microsoft.Agents.AI.Hosting.AzureFunctions;
using Microsoft.Agents.AI.Workflows;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Extensions.Hosting;
using OpenAI.Chat;
using WorkflowAndAgents;

// Get the Foundry project endpoint and model deployment name from environment variables.
string projectEndpoint = Environment.GetEnvironmentVariable("FOUNDRY_PROJECT_ENDPOINT")
    ?? throw new InvalidOperationException("FOUNDRY_PROJECT_ENDPOINT is not set.");
string deploymentName = Environment.GetEnvironmentVariable("FOUNDRY_MODEL")
    ?? throw new InvalidOperationException("FOUNDRY_MODEL is not set.");

// The Azure OpenAI endpoint is the authority (scheme + host) of the Foundry project endpoint.
string endpoint = new Uri(projectEndpoint).GetLeftPart(UriPartial.Authority);

// WARNING: DefaultAzureCredential is convenient for development but requires careful consideration in production.
// In production, consider using a specific credential (e.g., ManagedIdentityCredential) to avoid
// latency issues, unintended credential probing, and potential security risks from fallback mechanisms.
AzureOpenAIClient client = new(new Uri(endpoint), new DefaultAzureCredential());

ChatClient chatClient = client.GetChatClient(deploymentName);

// Define a standalone AI agent
AIAgent assistant = chatClient.AsAIAgent(
    "You are a helpful assistant. Answer questions clearly and concisely.",
    "Assistant",
    description: "A general-purpose helpful assistant.");

// Define workflow executors
TranslateText translateText = new();
FormatOutput formatOutput = new();

// Build a workflow: TranslateText -> FormatOutput
Workflow translateWorkflow = new WorkflowBuilder(translateText)
    .WithName("Translate")
    .WithDescription("Translate text to uppercase and format the result")
    .AddEdge(translateText, formatOutput)
    .Build();

// Register agents and workflows through separate, composable calls.
using IHost app = FunctionsApplication
    .CreateBuilder(args)
    .ConfigureFunctionsWebApplication()

    // Register the standalone agent with HTTP and MCP tool triggers
    .ConfigureDurableAgents(agents => agents.AddAIAgent(assistant, enableHttpTrigger: true, enableMcpToolTrigger: true))

    // Register the workflow with an HTTP endpoint and MCP tool trigger
    .ConfigureDurableWorkflows(workflows => workflows.AddWorkflow(translateWorkflow, enableStatusEndpoint: false, enableMcpToolTrigger: true))
    .Build();
app.Run();
