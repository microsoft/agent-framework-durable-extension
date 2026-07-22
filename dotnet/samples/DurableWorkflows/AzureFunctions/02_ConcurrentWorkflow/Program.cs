// Copyright (c) Microsoft. All rights reserved.

using Azure.AI.OpenAI;
using Azure.Identity;
using Microsoft.Agents.AI;
using Microsoft.Agents.AI.DurableTask;
using Microsoft.Agents.AI.Hosting.AzureFunctions;
using Microsoft.Agents.AI.Workflows;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Extensions.Hosting;
using OpenAI.Chat;
using WorkflowConcurrency;

// Get the Foundry project endpoint and model deployment name from environment variables.
string projectEndpoint = Environment.GetEnvironmentVariable("FOUNDRY_PROJECT_ENDPOINT")
    ?? throw new InvalidOperationException("FOUNDRY_PROJECT_ENDPOINT is not set.");
string deploymentName = Environment.GetEnvironmentVariable("FOUNDRY_MODEL")
    ?? throw new InvalidOperationException("FOUNDRY_MODEL is not set.");

// The Azure OpenAI endpoint is the authority (scheme + host) of the Foundry project endpoint.
string endpoint = new Uri(projectEndpoint).GetLeftPart(UriPartial.Authority);

// Create Azure OpenAI client
AzureOpenAIClient openAiClient = new(new Uri(endpoint), new AzureCliCredential());
ChatClient chatClient = openAiClient.GetChatClient(deploymentName);

// Define the 4 executors for the workflow
ParseQuestionExecutor parseQuestion = new();
AIAgent physicist = chatClient.AsAIAgent("You are a physics expert. Be concise (2-3 sentences).", "Physicist");
AIAgent chemist = chatClient.AsAIAgent("You are a chemistry expert. Be concise (2-3 sentences).", "Chemist");
AggregatorExecutor aggregator = new();

// Build workflow: ParseQuestion -> [Physicist, Chemist] (parallel) -> Aggregator
Workflow workflow = new WorkflowBuilder(parseQuestion)
    .WithName("ExpertReview")
    .AddFanOutEdge(parseQuestion, [physicist, chemist])
    .AddFanInBarrierEdge([physicist, chemist], aggregator)
    .Build();

using IHost app = FunctionsApplication
    .CreateBuilder(args)
    .ConfigureFunctionsWebApplication()
    .ConfigureDurableWorkflows(workflows => workflows.AddWorkflows(workflow))
    .Build();
app.Run();
