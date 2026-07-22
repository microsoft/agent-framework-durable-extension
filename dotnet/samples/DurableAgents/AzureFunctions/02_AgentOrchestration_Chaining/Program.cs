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

// Single agent used by the orchestration to demonstrate sequential calls on the same session.
const string WriterName = "WriterAgent";
const string WriterInstructions =
    """
    You refine short pieces of text. When given an initial sentence you enhance it;
    when given an improved sentence you polish it further.
    """;

AIAgent writerAgent = client.GetChatClient(deploymentName).AsAIAgent(WriterInstructions, WriterName);

using IHost app = FunctionsApplication
    .CreateBuilder(args)
    .ConfigureFunctionsWebApplication()
    .ConfigureDurableAgents(options => options.AddAIAgent(writerAgent))
    .Build();

app.Run();
