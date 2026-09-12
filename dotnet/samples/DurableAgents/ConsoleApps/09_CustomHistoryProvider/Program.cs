// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Azure.AI.OpenAI;
using Azure.Identity;
using CustomHistoryProvider;
using Microsoft.Agents.AI;
using Microsoft.Agents.AI.DurableTask;
using Microsoft.DurableTask.Client.AzureManaged;
using Microsoft.DurableTask.Worker.AzureManaged;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using OpenAI.Chat;

const string AgentName = "HistoryAgent";
const long OneMiB = 1_048_576;
const int SeedMessageBytes = 4 * 1024;
const int ModelHistoryMessageLimit = 12;
const int ModelHistoryByteLimit = 32 * 1024;

if (!IsExperimentalMailboxRuntimeAvailable())
{
    Console.Error.WriteLine(
        "DRAFT SAMPLE: schema 2 mailbox writes are protected by an internal, default-disabled rollout gate. " +
        "This sample cannot run against production or mixed-language runtimes yet.");
    Environment.ExitCode = 2;
    return;
}

// Get the Foundry project endpoint and model deployment name from environment variables.
string projectEndpoint = Environment.GetEnvironmentVariable("FOUNDRY_PROJECT_ENDPOINT")
    ?? throw new InvalidOperationException("FOUNDRY_PROJECT_ENDPOINT is not set.");
string deploymentName = Environment.GetEnvironmentVariable("FOUNDRY_MODEL")
    ?? throw new InvalidOperationException("FOUNDRY_MODEL is not set.");

// The Azure OpenAI endpoint is the authority (scheme + host) of the Foundry project endpoint.
string endpoint = new Uri(projectEndpoint).GetLeftPart(UriPartial.Authority);

// Get DTS connection string from environment variable
string dtsConnectionString = Environment.GetEnvironmentVariable("DURABLE_TASK_SCHEDULER_CONNECTION_STRING")
    ?? "Endpoint=http://localhost:8080;TaskHub=default;Authentication=None";

// WARNING: DefaultAzureCredential is convenient for development but requires careful consideration in production.
// In production, consider using a specific credential (e.g., ManagedIdentityCredential) to avoid
// latency issues, unintended credential probing, and potential security risks from fallback mechanisms.
AzureOpenAIClient client = new(new Uri(endpoint), new DefaultAzureCredential());

Console.ForegroundColor = ConsoleColor.Cyan;
Console.WriteLine("=== Custom External History Provider Sample ===");
Console.ResetColor();
Console.WriteLine("Enter a short marker for the durable agent to remember:");
Console.WriteLine();

Console.ForegroundColor = ConsoleColor.Yellow;
Console.Write("Marker: ");
Console.ResetColor();
string? marker = Console.ReadLine();
if (string.IsNullOrWhiteSpace(marker))
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.Error.WriteLine("Error: A marker is required.");
    Console.ResetColor();
    Environment.ExitCode = 1;
    return;
}

string storeDirectory = Path.Combine(
    AppContext.BaseDirectory,
    ".sample-history",
    Guid.NewGuid().ToString("N"));
Directory.CreateDirectory(storeDirectory);

try
{
    JsonElement serializedSession;
    string historyId;

    using (IHost firstHost = CreateHost(
        client,
        deploymentName,
        dtsConnectionString,
        storeDirectory,
        out JsonFileChatHistoryProvider firstProvider))
    {
        await firstHost.StartAsync();
        AIAgent agent = firstHost.Services.GetRequiredKeyedService<AIAgent>(AgentName);
        AgentSession session = await agent.CreateSessionAsync();

        _ = await agent.RunAsync("Reply with READY only.", session);
        historyId = firstProvider.GetObservedHistoryIds().Single();

        JsonFileChatHistoryProvider.SeedHistoryResult seeded = await firstProvider.SeedHistoryAsync(
            historyId,
            minimumStoreBytes: OneMiB,
            messageTextUtf8Bytes: SeedMessageBytes);

        Console.WriteLine();
        Console.WriteLine(
            $"External history: {seeded.PersistedBytes:N0} bytes in " +
            $"{seeded.PersistedMessageCount:N0} moderate records.");
        Console.WriteLine(
            "The size is cumulative; no individual durable request approaches the 1 MiB DTS boundary.");

        AgentResponse response = await agent.RunAsync(
            $"Remember this exact marker: {marker}. Reply with the marker only.",
            session);
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($"HistoryAgent: {response.Text}");
        Console.ResetColor();

        JsonFileChatHistoryProvider.HistoryStatistics statistics =
            await firstProvider.GetStatisticsAsync(historyId);
        WriteModelWindow(statistics);
        serializedSession = await agent.SerializeSessionAsync(session);
        await firstHost.StopAsync();
    }

    Console.WriteLine();
    Console.WriteLine("Restarting the host and restoring the same durable session...");

    using (IHost secondHost = CreateHost(
        client,
        deploymentName,
        dtsConnectionString,
        storeDirectory,
        out JsonFileChatHistoryProvider secondProvider))
    {
        await secondHost.StartAsync();
        AIAgent agent = secondHost.Services.GetRequiredKeyedService<AIAgent>(AgentName);
        AgentSession session = await agent.DeserializeSessionAsync(serializedSession);
        AgentResponse response = await agent.RunAsync(
            "What exact marker did I ask you to remember?",
            session);

        if (!response.Text.Contains(marker, StringComparison.Ordinal) ||
            secondProvider.GetObservedHistoryIds().Single() != historyId)
        {
            throw new InvalidOperationException("The marker or external history identity was not restored.");
        }

        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine($"HistoryAgent after restart: {response.Text}");
        Console.WriteLine(
            "Marker recall and the logical external history reference were preserved after restart.");
        Console.ResetColor();

        JsonFileChatHistoryProvider.HistoryStatistics statistics =
            await secondProvider.GetStatisticsAsync(historyId);
        WriteModelWindow(statistics);
        await secondHost.StopAsync();
    }
}
finally
{
    Directory.Delete(storeDirectory, recursive: true);
}

static void WriteModelWindow(JsonFileChatHistoryProvider.HistoryStatistics statistics)
{
    JsonFileChatHistoryProvider.ModelWindowStatistics? window = statistics.LastModelWindow;
    Console.WriteLine(
        $"Provider-supplied model history: {window?.MessageCount ?? 0} records, " +
        $"{window?.TextUtf8Bytes ?? 0:N0} UTF-8 text bytes " +
        $"(limits: {ModelHistoryMessageLimit} records / {ModelHistoryByteLimit:N0} bytes).");
}

static IHost CreateHost(
    AzureOpenAIClient client,
    string deploymentName,
    string dtsConnectionString,
    string storeDirectory,
    out JsonFileChatHistoryProvider historyProvider)
{
    historyProvider = new JsonFileChatHistoryProvider(
        storeDirectory,
        maxModelMessages: ModelHistoryMessageLimit,
        maxModelTextUtf8Bytes: ModelHistoryByteLimit);
    JsonFileChatHistoryProvider registeredProvider = historyProvider;
    AIAgent agent = client.GetChatClient(deploymentName).AsAIAgent(
        new ChatClientAgentOptions
        {
            Name = AgentName,
            ChatOptions = new()
            {
                Instructions =
                    "Remember exact markers when asked, reproduce them exactly, and keep responses concise.",
            },
            ChatHistoryProvider = registeredProvider,
        });

    return Host.CreateDefaultBuilder()
        .ConfigureLogging(logging => logging.SetMinimumLevel(LogLevel.Warning))
        .ConfigureServices(services =>
        {
            services.AddSingleton(registeredProvider);
            services.ConfigureDurableAgents(
                options =>
                {
                    options.AddAIAgent(agent, timeToLive: TimeSpan.FromHours(1));
                    options.SetHistoryProviderKey(
                        AgentName,
                        JsonFileChatHistoryProvider.ProviderKey);
                    options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.KeepAll;
                },
                workerBuilder: builder => builder.UseDurableTaskScheduler(dtsConnectionString),
                clientBuilder: builder => builder.UseDurableTaskScheduler(dtsConnectionString));
        })
        .Build();
}

static bool IsExperimentalMailboxRuntimeAvailable() => false;
