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

if (!IsExperimentalMailboxRuntimeAvailable())
{
    Console.Error.WriteLine(
        "DRAFT SAMPLE: automatic transcript retention requires schema 2 mailbox writes, which are " +
        "protected by an internal, default-disabled rollout gate. This sample cannot run against " +
        "production or mixed-language runtimes yet.");
    Environment.ExitCode = 2;
    return;
}

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

AIAgent agent = client.GetChatClient(deploymentName).AsAIAgent(
    HistoryRetentionDemo.CreateAgentOptions());

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
Console.WriteLine("=== Opt-in Durable Transcript Pressure Retention Sample ===");
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
    "KeepAll is the default. This sample explicitly enables mailbox writes and selects Auto to bound the model transcript.");
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
    "Auto removes old model transcript while mailbox results, completion receipts, binding, session, and bookkeeping stay protected.");
Console.WriteLine(
    "The marker question is illustrative; deterministic tests prove transcript eviction, result retrieval, and idempotent redelivery.");
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
    "Auto cannot make an oversized protected newest inline payload or tool result fit; that operation fails instead.");
Console.WriteLine(
    "Stopping and disposing the host gives the OpenTelemetry console exporter a final flush opportunity.");

await host.StopAsync();

static bool IsExperimentalMailboxRuntimeAvailable() => false;
