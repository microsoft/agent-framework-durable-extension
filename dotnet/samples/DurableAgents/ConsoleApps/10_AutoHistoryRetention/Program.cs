// Copyright (c) Microsoft. All rights reserved.

using AutoHistoryRetention;
using Azure.AI.OpenAI;
using Azure.Identity;
using Microsoft.Agents.AI;
using Microsoft.Agents.AI.DurableTask;
using Microsoft.DurableTask.Client.AzureManaged;
using Microsoft.DurableTask.Worker.AzureManaged;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenAI.Chat;

// Use an isolated task hub with schema-v2-compatible workers, clients and readers in real deployments.
string connectionString = Environment.GetEnvironmentVariable("DURABLE_TASK_SCHEDULER_CONNECTION_STRING")
    ?? "Endpoint=http://localhost:8080;TaskHub=default;Authentication=None";
string projectEndpoint = Environment.GetEnvironmentVariable("FOUNDRY_PROJECT_ENDPOINT")
    ?? throw new InvalidOperationException("FOUNDRY_PROJECT_ENDPOINT is not set.");
string deploymentName = Environment.GetEnvironmentVariable("FOUNDRY_MODEL")
    ?? throw new InvalidOperationException("FOUNDRY_MODEL is not set.");
string endpoint = new Uri(projectEndpoint).GetLeftPart(UriPartial.Authority);

Console.WriteLine("Legacy and KeepAll are the defaults. This sample separately acknowledges SchemaV2 and opts into Auto.");
Console.WriteLine("This is durable transcript pressure retention, not model-context compaction or FollowCompaction.");
Console.WriteLine("External history stores are never pruned; results, completion receipts and session state remain protected.");
Console.WriteLine("The 32 KiB demo budget is not universal: oversized protected state fails atomically.");
Console.WriteLine("OpenTelemetry metrics describe attempts, not confirmed durable commits.");
Console.Write("Project topic (up to 80 characters): ");
string? topic = Console.ReadLine();
if (string.IsNullOrWhiteSpace(topic) || topic.Length > HistoryRetentionDemo.MaxTopicCharacters)
{
    Console.Error.WriteLine("A non-empty project topic of at most 80 characters is required.");
    return 1;
}

// DefaultAzureCredential is convenient for development; select a specific credential in production.
AzureOpenAIClient client = new(new Uri(endpoint), new DefaultAzureCredential());
AIAgent agent = client.GetChatClient(deploymentName).AsAIAgent(HistoryRetentionDemo.CreateAgentOptions());
using IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(logging => logging.SetMinimumLevel(LogLevel.Warning))
    .ConfigureServices(services =>
    {
        services.AddRetentionMetrics();
        services.ConfigureDurableAgents(
            options =>
            {
                HistoryRetentionDemo.ConfigureRetention(options);
                options.AddAIAgent(agent);
            },
            workerBuilder: builder => builder.UseDurableTaskScheduler(connectionString),
            clientBuilder: builder => builder.UseDurableTaskScheduler(connectionString));
    })
    .Build();

await host.StartAsync();
try
{
    AIAgent proxy = host.Services.GetDurableAgentProxy(agent.Name!);
    AgentSession session = await proxy.CreateSessionAsync();
    string marker = HistoryRetentionDemo.CreateMarker();
    Console.WriteLine($"Original marker: {marker}");
    HistoryRetentionScenarioResult result = await new HistoryRetentionScenario(proxy).RunAsync(
        topic,
        marker,
        session,
        beforeNote: turn => Console.WriteLine($"Sending note {turn} of {HistoryRetentionDemo.ScenarioTurns}..."));
    Console.WriteLine($"Diagnostic question: {HistoryRetentionDemo.DiagnosticQuestion}");
    Console.WriteLine($"Model response: {result.DiagnosticResponse}");
    Console.WriteLine($"Marker observation: {result.Observation} (illustrative, not proof of eviction).");
}
finally
{
    await host.StopAsync();
}

return 0;
