// Copyright (c) Microsoft. All rights reserved.

#pragma warning disable IDE0002 // Simplify Member Access

using Azure.AI.OpenAI;
using Azure.Identity;
using Microsoft.Agents.AI;
using Microsoft.Agents.AI.Hosting.AzureFunctions;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Extensions.Hosting;
using OpenAI.Chat;

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

// Two agents used by the orchestration to demonstrate concurrent execution.
const string PhysicistName = "PhysicistAgent";
const string PhysicistInstructions = "You are an expert in physics. You answer questions from a physics perspective.";

const string ChemistName = "ChemistAgent";
const string ChemistInstructions = "You are an expert in chemistry. You answer questions from a chemistry perspective.";

AIAgent physicistAgent = client.GetChatClient(deploymentName).AsAIAgent(PhysicistInstructions, PhysicistName);
AIAgent chemistAgent = client.GetChatClient(deploymentName).AsAIAgent(ChemistInstructions, ChemistName);

using IHost app = FunctionsApplication
    .CreateBuilder(args)
    .ConfigureFunctionsWebApplication()
    .ConfigureDurableAgents(options =>
    {
        options
            .AddAIAgent(physicistAgent)
            .AddAIAgent(chemistAgent);
    })
    .Build();

app.Run();
