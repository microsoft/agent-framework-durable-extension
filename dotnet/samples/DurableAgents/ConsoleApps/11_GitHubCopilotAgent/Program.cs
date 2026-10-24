// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using GitHub.Copilot;
using GitHubCopilotDurableAgent;
using Microsoft.Agents.AI;
using Microsoft.Agents.AI.DurableTask;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Client.AzureManaged;
using Microsoft.DurableTask.Worker;
using Microsoft.DurableTask.Worker.AzureManaged;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

string dtsConnectionString = Environment.GetEnvironmentVariable("DURABLE_TASK_SCHEDULER_CONNECTION_STRING")
    ?? "Endpoint=http://localhost:8080;TaskHub=default;Authentication=None";
string? copilotRuntimeUrl = Environment.GetEnvironmentVariable("COPILOT_RUNTIME_URL");
string? copilotBaseDirectory = Environment.GetEnvironmentVariable("COPILOT_BASE_DIRECTORY");

Console.WriteLine("=== Durable GitHub Copilot Agent Sample ===");
Console.WriteLine($"Agent Name: {GitHubCopilotDurableDemo.AgentName}");
Console.WriteLine($"History ReplayMode: {DurableAgentHistoryReplayMode.CurrentRequestOnly}");
Console.WriteLine($"History ProviderKey: {GitHubCopilotDurableDemo.HistoryProviderKey}");
Console.WriteLine($"Copilot Connection: {(string.IsNullOrWhiteSpace(copilotRuntimeUrl) ? "Local Copilot CLI" : copilotRuntimeUrl)}");
Console.WriteLine();

await using CopilotClient copilotClient = new(
    GitHubCopilotDurableDemo.CreateClientOptions(copilotRuntimeUrl, copilotBaseDirectory));

// ---------------------------------------------------------------------------
// Demo 1: Multi-turn Durable Agent across a host restart
// ---------------------------------------------------------------------------
Console.WriteLine("--- Demo 1: Multi-turn Durable Session Across Host Restart ---");

string marker = $"MARKER-{Guid.NewGuid():N}"[..15].ToUpperInvariant();
JsonElement serializedDurableSession;

IHost firstHost = CreateHost(copilotClient, dtsConnectionString);
await firstHost.StartAsync();
try
{
    AIAgent durableAgent = firstHost.Services.GetDurableAgentProxy(GitHubCopilotDurableDemo.AgentName);
    AgentSession session = await durableAgent.CreateSessionAsync();

    Console.WriteLine($"[Host 1 / Turn 1] Asking CopilotAgent to remember marker: {marker}");
    AgentResponse turn1Response = await durableAgent.RunAsync(
        $"Remember this incident token for our session: {marker}. Confirm the token in one short sentence.",
        session);
    Console.WriteLine($"[Host 1 / Turn 1] Response: {turn1Response.Text}");

    // Serialize the outer DurableAgentSession handle so a second host or process can resume it.
    serializedDurableSession = await durableAgent.SerializeSessionAsync(session);
}
finally
{
    await StopAndDisposeHostAsync(firstHost);
}

Console.WriteLine("Host 1 stopped. Starting Host 2 to verify cold restoration of GitHubCopilotAgentSession.SessionId...");

IHost secondHost = CreateHost(copilotClient, dtsConnectionString);
await secondHost.StartAsync();
try
{
    AIAgent restoredDurableAgent = secondHost.Services.GetDurableAgentProxy(GitHubCopilotDurableDemo.AgentName);
    AgentSession restoredSession = await restoredDurableAgent.DeserializeSessionAsync(serializedDurableSession);

    Console.WriteLine("[Host 2 / Turn 2] Resuming durable session after host restart...");
    AgentResponse turn2Response = await restoredDurableAgent.RunAsync(
        "What incident token did I ask you to remember in Turn 1? Reply with the token.",
        restoredSession);
    Console.WriteLine($"[Host 2 / Turn 2] Response: {turn2Response.Text}");
    Console.WriteLine();

    // ---------------------------------------------------------------------------
    // Demo 2: Durable Orchestration with Human-in-the-Loop (HITL) Approval
    // ---------------------------------------------------------------------------
    Console.WriteLine("--- Demo 2: Durable Orchestration with Human-in-the-Loop Approval ---");
    DurableTaskClient durableClient = secondHost.Services.GetRequiredService<DurableTaskClient>();

    CopilotIncidentInput incidentInput = new(
        IncidentSummary: "Elevated HTTP 503 errors on checkout-service after deployment v2.4.1",
        ApprovalTimeoutMinutes: 30);

    string instanceId = await durableClient.ScheduleNewOrchestrationInstanceAsync(
        nameof(GitHubCopilotDurableDemo.RunIncidentOrchestrationAsync),
        incidentInput);
    Console.WriteLine($"Started orchestration '{instanceId}'. Waiting for Turn 1 remediation plan...");

    while (true)
    {
        OrchestrationMetadata? metadata = await durableClient.GetInstanceAsync(instanceId, getInputsAndOutputs: true);
        string? customStatus = metadata?.ReadCustomStatusAs<string>();
        if (customStatus?.Contains(GitHubCopilotDurableDemo.ApprovalEventName, StringComparison.Ordinal) is true)
        {
            Console.WriteLine($"Orchestration status: {customStatus}");
            break;
        }

        if (metadata?.RuntimeStatus is OrchestrationRuntimeStatus.Completed
            or OrchestrationRuntimeStatus.Failed
            or OrchestrationRuntimeStatus.Terminated)
        {
            break;
        }

        await Task.Delay(TimeSpan.FromMilliseconds(250));
    }

    Console.Write("Approve the proposed remediation plan? [y/n] (default: y): ");
    string? approvalInput = Console.ReadLine();
    bool approved = string.IsNullOrWhiteSpace(approvalInput) ||
        approvalInput.Trim().Equals("y", StringComparison.OrdinalIgnoreCase);

    await durableClient.RaiseEventAsync(
        instanceId,
        GitHubCopilotDurableDemo.ApprovalEventName,
        approved);

    OrchestrationMetadata completed = await durableClient.WaitForInstanceCompletionAsync(
        instanceId,
        getInputsAndOutputs: true);

    CopilotIncidentResult? result = completed.ReadOutputAs<CopilotIncidentResult>();
    Console.WriteLine($"Orchestration completed with status: {completed.RuntimeStatus}");
    Console.WriteLine($"Turn 1 Proposed Plan: {result?.ProposedPlan}");
    Console.WriteLine($"Approved: {result?.Approved}");
    Console.WriteLine($"Turn 2 Final Resolution: {result?.FinalResolution}");
}
finally
{
    await StopAndDisposeHostAsync(secondHost);
}

static IHost CreateHost(CopilotClient copilotClient, string dtsConnectionString)
{
    AIAgent copilotAgent = GitHubCopilotDurableDemo.CreateCopilotAgent(copilotClient);

    return Host.CreateDefaultBuilder()
        .ConfigureLogging(logging => logging.SetMinimumLevel(LogLevel.Warning))
        .ConfigureServices(services =>
        {
            services.ConfigureDurableAgents(
                options => GitHubCopilotDurableDemo.RegisterCopilotAgent(options, copilotAgent),
                workerBuilder: builder =>
                {
                    builder.UseDurableTaskScheduler(dtsConnectionString);
                    builder.AddTasks(registry =>
                    {
                        registry.AddOrchestratorFunc<CopilotIncidentInput, CopilotIncidentResult>(
                            nameof(GitHubCopilotDurableDemo.RunIncidentOrchestrationAsync),
                            GitHubCopilotDurableDemo.RunIncidentOrchestrationAsync);
                    });
                },
                clientBuilder: builder => builder.UseDurableTaskScheduler(dtsConnectionString));
        })
        .Build();
}

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
