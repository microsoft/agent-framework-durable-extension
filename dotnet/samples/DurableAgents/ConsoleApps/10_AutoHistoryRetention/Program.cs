// Copyright (c) Microsoft. All rights reserved.

using AutoHistoryRetention;
using Azure.AI.OpenAI;
using Azure.Identity;
using Microsoft.Agents.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenAI.Chat;

const string AgentName = "HistoryKeeper";

string projectEndpoint = Environment.GetEnvironmentVariable("FOUNDRY_PROJECT_ENDPOINT")
    ?? throw new InvalidOperationException("FOUNDRY_PROJECT_ENDPOINT is not set.");
string deploymentName = Environment.GetEnvironmentVariable("FOUNDRY_MODEL")
    ?? throw new InvalidOperationException("FOUNDRY_MODEL is not set.");
string endpoint = new Uri(projectEndpoint).GetLeftPart(UriPartial.Authority);
string dtsConnectionString = Environment.GetEnvironmentVariable("DURABLE_TASK_SCHEDULER_CONNECTION_STRING")
    ?? "Endpoint=http://localhost:8080;TaskHub=default;Authentication=None";

// DefaultAzureCredential is convenient for development. Production applications should select
// credentials deliberately, such as ManagedIdentityCredential when hosted in Azure.
AzureOpenAIClient client = new(new Uri(endpoint), new DefaultAzureCredential());

const string AgentInstructions =
    """
    You help users maintain concise project notes.
    Acknowledge each numbered note in one short sentence and keep every response under 20 words.
    When asked for a marker that is not in the conversation history, answer UNKNOWN instead of guessing.
    """;

AIAgent agent = client.GetChatClient(deploymentName).AsAIAgent(AgentInstructions, AgentName);

using IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(logging => logging.SetMinimumLevel(LogLevel.Warning))
    .ConfigureServices(
        services => HistoryRetentionHost.ConfigureServices(
            services,
            agent,
            dtsConnectionString))
    .Build();

await host.StartAsync();

AIAgent durableAgent = host.Services.GetRequiredKeyedService<AIAgent>(AgentName);
AgentSession session = await durableAgent.CreateSessionAsync();

Console.ForegroundColor = ConsoleColor.Cyan;
Console.WriteLine("=== Automatic Durable History Retention Sample ===");
Console.ResetColor();
Console.WriteLine("Enter a project topic:");
Console.WriteLine();

Console.ForegroundColor = ConsoleColor.Yellow;
Console.Write("Topic: ");
Console.ResetColor();
string? topic = Console.ReadLine();
if (string.IsNullOrWhiteSpace(topic))
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.Error.WriteLine("Error: A topic is required.");
    Console.ResetColor();
    Environment.ExitCode = 1;
    await host.StopAsync();
    return;
}

topic = topic.Trim();
if (topic.Length > HistoryRetentionDemo.MaxTopicCharacters)
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.Error.WriteLine(
        $"Error: Keep the topic to {HistoryRetentionDemo.MaxTopicCharacters} characters or fewer.");
    Console.ResetColor();
    Environment.ExitCode = 1;
    await host.StopAsync();
    return;
}

Console.WriteLine();
Console.WriteLine(
    $"Adding {HistoryRetentionDemo.ScenarioTurns} moderate turns to cross the " +
    $"{HistoryRetentionDemo.HighWatermarkBytes:N0}-byte high watermark of the " +
    $"{HistoryRetentionDemo.MaxStateBytes:N0}-byte durable-state budget.");
Console.WriteLine(
    "The filler creates pressure but is not printed, so the scenario remains readable.");
Console.WriteLine(
    "Watch the OpenTelemetry console exporter for durable.agent.history.* retention metrics.");
Console.WriteLine();

string firstMarker = HistoryRetentionDemo.CreateMarker();
Console.WriteLine($"First note marker: {firstMarker}");
Console.WriteLine();

HistoryRetentionScenario scenario = new(durableAgent);
HistoryRetentionScenarioResult result = await scenario.RunAsync(
    topic,
    firstMarker,
    session,
    beforeEligibilityWait: () =>
    {
        Console.WriteLine("Durable requests use a signal plus polling.");
        Console.WriteLine(
            "Successful responses are normally protected for 60 seconds so a caller can retrieve them.");
        Console.WriteLine(
            "Waiting 61 seconds makes the old exchanges normally eligible for Auto retention.");
        Console.WriteLine(
            "The budget can override protection under hard pressure, but this scenario avoids that forced path.");
        Console.WriteLine();
    },
    beforeNote: turn =>
    {
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine(turn == 1
            ? $"Sending note {turn} with marker {firstMarker}..."
            : $"Sending note {turn}...");
        Console.ResetColor();
    });

Console.WriteLine();
Console.WriteLine(
    "Retention evidence is emitted through the standard OpenTelemetry metrics pipeline.");
Console.WriteLine(
    "These are attempt-level operational measurements recorded before entity commit, not durable committed truth.");
Console.WriteLine(
    "The marker question is a human-readable demonstration; product tests cover internal retention mechanics.");
Console.WriteLine();
Console.WriteLine($"Original marker: {firstMarker}");
Console.WriteLine($"Diagnostic question: {HistoryRetentionDemo.DiagnosticQuestion}");

Console.ForegroundColor = ConsoleColor.Green;
Console.WriteLine($"HistoryKeeper: {result.DiagnosticResponse}");
Console.ResetColor();

switch (result.Observation)
{
    case MarkerObservation.Present:
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine(
            "Observation: the exact marker is present; eviction was not demonstrated by this model response.");
        break;
    case MarkerObservation.Unavailable:
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine(
            "Observation: the agent answered only UNKNOWN; the old marker is unavailable to the model.");
        break;
    default:
        Console.ForegroundColor = ConsoleColor.Yellow;
        Console.WriteLine(
            "Observation: the answer is inconclusive; do not infer retention from this response alone.");
        break;
}

Console.ResetColor();
Console.WriteLine(
    "This is durable entity pressure retention, not MAF stateful compaction or FollowCompaction.");
Console.WriteLine(
    "Stopping and disposing the host gives the OpenTelemetry console exporter a final flush opportunity.");

await host.StopAsync();
