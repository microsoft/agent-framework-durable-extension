// Copyright (c) Microsoft. All rights reserved.

using Azure.AI.Projects;
using Azure.AI.Projects.Agents;
using Azure.Identity;
using Microsoft.Agents.AI;
using Microsoft.Agents.AI.DurableTask;
using Microsoft.Agents.AI.Foundry;
using Microsoft.DurableTask.Client.AzureManaged;
using Microsoft.DurableTask.Worker.AzureManaged;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

const string FoundryServiceHistoryProviderKey = "foundry-managed-service.v1";

if (!IsExperimentalMailboxRuntimeAvailable())
{
    Console.Error.WriteLine(
        "DRAFT SAMPLE: schema 2 mailbox writes are protected by an internal, default-disabled rollout gate. " +
        "This sample cannot run against production or mixed-language runtimes yet.");
    return 2;
}

string projectEndpoint = Environment.GetEnvironmentVariable("FOUNDRY_PROJECT_ENDPOINT")
    ?? throw new InvalidOperationException("FOUNDRY_PROJECT_ENDPOINT is not set.");
string deploymentName = Environment.GetEnvironmentVariable("FOUNDRY_MODEL")
    ?? throw new InvalidOperationException("FOUNDRY_MODEL is not set.");
string dtsConnectionString = Environment.GetEnvironmentVariable("DURABLE_TASK_SCHEDULER_CONNECTION_STRING")
    ?? throw new InvalidOperationException("DURABLE_TASK_SCHEDULER_CONNECTION_STRING is not set.");

// WARNING: DefaultAzureCredential is convenient for development but requires careful consideration in production.
// In production, consider using a specific credential (e.g., ManagedIdentityCredential) to avoid
// latency issues, unintended credential probing, and potential security risks from fallback mechanisms.
DefaultAzureCredential credential = new();
AIProjectClient projectClient = new(new Uri(projectEndpoint), credential);

string agentName = $"durable-foundry-{Guid.NewGuid():N}"[..24];
ProjectsAgentVersion? createdVersion = null;
IHost? host = null;
int exitCode = 0;

try
{
    createdVersion = await projectClient.AgentAdministrationClient.CreateAgentVersionAsync(
        agentName,
        new ProjectsAgentVersionCreationOptions(
            new DeclarativeAgentDefinition(deploymentName)
            {
                Instructions =
                    """
                    You are a conversation continuity test agent.
                    When asked to remember a marker, acknowledge it and retain it.
                    When later asked for the marker only, respond with exactly that marker and no other text.
                    """,
            }));

    FoundryAgent foundryAgent = CreateFoundryAgent(projectClient, createdVersion);
    host = CreateHost(foundryAgent, dtsConnectionString);
    await host.StartAsync();

    AIAgent durableAgent = host.Services.GetRequiredKeyedService<AIAgent>(foundryAgent.Name!);
    DurableAgentSession durableSession =
        (DurableAgentSession)await durableAgent.CreateSessionAsync();

    string marker = $"durable-{Guid.NewGuid():N}"[..16];
    Console.WriteLine("=== Durable Foundry Managed Agent Sample ===");
    Console.WriteLine($"Foundry agent version: {createdVersion.Name}/{createdVersion.Version}");
    Console.WriteLine($"Durable session: {durableSession.GetService<AgentSessionId>()}");

    AgentResponse first = await durableAgent.RunAsync(
        $"Remember this marker for our conversation: {marker}. Confirm the marker in your reply.",
        durableSession);
    Console.WriteLine($"First response: {first.Text}");

    if (!first.Text.Contains(marker, StringComparison.Ordinal))
    {
        throw new InvalidOperationException("The first response did not confirm the generated marker.");
    }

    IHost firstHost = host;
    host = null;
    await StopAndDisposeHostAsync(firstHost);
    Console.WriteLine("Host stopped. Starting a new host to force durable state restoration.");

    FoundryAgent restoredFoundryAgent = CreateFoundryAgent(projectClient, createdVersion);
    host = CreateHost(restoredFoundryAgent, dtsConnectionString);
    await host.StartAsync();

    AIAgent restoredDurableAgent =
        host.Services.GetRequiredKeyedService<AIAgent>(restoredFoundryAgent.Name!);
    AgentResponse second = await restoredDurableAgent.RunAsync(
        "Return the marker only.",
        durableSession);
    Console.WriteLine($"Second response: {second.Text}");

    if (!second.Text.Contains(marker, StringComparison.Ordinal))
    {
        throw new InvalidOperationException(
            "Marker recall check failed: the second response did not contain the generated marker.");
    }

    Console.WriteLine("Marker recall after durable restart: PASS");
    Console.WriteLine(
        "The fixed service-owner binding and restored service conversation identity are verified by deterministic tests.");
}
catch (Exception ex)
{
    exitCode = 1;
    Console.Error.WriteLine($"Sample failed: {ex}");
}
finally
{
    if (host is not null)
    {
        try
        {
            await StopAndDisposeHostAsync(host);
        }
        catch (Exception ex)
        {
            exitCode = 1;
            Console.Error.WriteLine($"Failed to stop the durable host cleanly: {ex.Message}");
        }
    }

    if (createdVersion is not null)
    {
        try
        {
            await projectClient.AgentAdministrationClient.DeleteAgentVersionAsync(
                createdVersion.Name,
                createdVersion.Version);
            Console.WriteLine($"Deleted Foundry agent version: {createdVersion.Name}/{createdVersion.Version}");
        }
        catch (Exception ex)
        {
            exitCode = 1;
            Console.Error.WriteLine(
                $"Failed to delete Foundry agent version {createdVersion.Name}/{createdVersion.Version}: {ex.Message}");
        }
    }
}

return exitCode;

static FoundryAgent CreateFoundryAgent(
    AIProjectClient projectClient,
    ProjectsAgentVersion agentVersion)
{
    FoundryAgent agent = projectClient.AsAIAgent(agentVersion);

    _ = agent.GetService<ChatClientAgent>()
        ?? throw new InvalidOperationException(
            "The FoundryAgent did not expose its inner ChatClientAgent pipeline.");

    return agent;
}

static IHost CreateHost(AIAgent foundryAgent, string dtsConnectionString)
{
    return Host.CreateDefaultBuilder()
        .ConfigureLogging(logging => logging.SetMinimumLevel(LogLevel.Warning))
        .ConfigureServices(services =>
        {
            services.ConfigureDurableAgents(
                options =>
                {
                    options.AddAIAgent(foundryAgent, timeToLive: TimeSpan.FromHours(1));
                    options.SetHistoryProviderKey(
                        foundryAgent.Name!,
                        FoundryServiceHistoryProviderKey);
                    options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.KeepAll;

                    // FoundryAgent's versioned-agent path does not enable
                    // RequirePerServiceCallChatHistoryPersistence. Once the first response supplies
                    // a service conversation ID, this C# runtime binds the durable session to the
                    // logical Foundry service owner and restores that opaque continuation on restart.
                },
                workerBuilder: builder => builder.UseDurableTaskScheduler(dtsConnectionString),
                clientBuilder: builder => builder.UseDurableTaskScheduler(dtsConnectionString));
        })
        .Build();
}

static bool IsExperimentalMailboxRuntimeAvailable() => false;

static async Task StopAndDisposeHostAsync(IHost host)
{
    try
    {
        await host.StopAsync();
    }
    finally
    {
        host.Dispose();
    }
}
