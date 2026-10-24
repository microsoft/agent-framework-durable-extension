// Copyright (c) Microsoft. All rights reserved.

using GitHub.Copilot;
using Microsoft.Agents.AI;
using Microsoft.Agents.AI.DurableTask;
using Microsoft.Agents.AI.Workflows;
using Microsoft.DurableTask;

namespace GitHubCopilotDurableAgent;

/// <summary>
/// Input for the human-in-the-loop incident remediation orchestration.
/// </summary>
/// <param name="IncidentSummary">Short description of the incident to diagnose and remediate.</param>
/// <param name="ApprovalTimeoutMinutes">Maximum minutes to wait for human approval.</param>
public sealed record CopilotIncidentInput(
    string IncidentSummary,
    int ApprovalTimeoutMinutes = 30);

/// <summary>
/// Output produced by <see cref="GitHubCopilotDurableDemo.RunIncidentOrchestrationAsync"/>.
/// </summary>
/// <param name="ProposedPlan">Initial remediation plan proposed on Turn 1.</param>
/// <param name="Approved">Whether the human operator approved the remediation plan.</param>
/// <param name="FinalResolution">Final execution summary from Turn 2 (or cancellation message if rejected).</param>
public sealed record CopilotIncidentResult(
    string ProposedPlan,
    bool Approved,
    string FinalResolution);

/// <summary>
/// Configuration and orchestration helpers for hosting <see cref="Microsoft.Agents.AI.GitHub.Copilot.GitHubCopilotAgent"/>
/// as a Durable Agent and Durable Workflow executor.
/// </summary>
public static class GitHubCopilotDurableDemo
{
    /// <summary>
    /// Single-word alphanumeric agent name. Avoid spaces or hyphens so Durable Workflow
    /// executor ID sanitization (<c>AIAgentExtensions.GetDescriptiveId()</c>) and
    /// <c>WorkflowNamingHelper.GetExecutorName(...)</c> resolve the exact registered name.
    /// </summary>
    public const string AgentName = "CopilotAgent";

    /// <summary>
    /// Stable logical history provider key required when <see cref="DurableAgentHistoryReplayMode.CurrentRequestOnly"/>
    /// delegates conversation continuity to <c>GitHubCopilotAgentSession.SessionId</c>.
    /// </summary>
    public const string HistoryProviderKey = "github-copilot-session.v1";

    /// <summary>
    /// External event name used by the human-in-the-loop orchestration.
    /// </summary>
    public const string ApprovalEventName = "Approval";

    /// <summary>
    /// Name of the sample Durable Workflow containing the Copilot agent node.
    /// </summary>
    public const string WorkflowName = "CopilotTriageWorkflow";

    /// <summary>
    /// Default system instructions appended to the Copilot session.
    /// </summary>
    public const string DefaultInstructions =
        """
        You are a concise cloud reliability assistant powered by GitHub Copilot.
        Keep responses under 80 words, preserve context across turns, and clearly state proposed vs. executed actions.
        """;

    /// <summary>
    /// Creates <see cref="CopilotClientOptions"/> configured for durable hosting.
    /// </summary>
    /// <param name="runtimeUrl">
    /// Optional TCP/URI endpoint (<c>host:port</c> or <c>http://host:port</c>) for a shared Copilot runtime server.
    /// When null or whitespace, <see cref="CopilotClient"/> uses the local Copilot CLI process.
    /// </param>
    /// <param name="baseDirectory">
    /// Optional shared base directory for <c>COPILOT_HOME/session-state/{sessionId}</c> when multiple workers
    /// resume sessions against a shared filesystem mount.
    /// </param>
    /// <returns>A configured <see cref="CopilotClientOptions"/> instance.</returns>
    public static CopilotClientOptions CreateClientOptions(
        string? runtimeUrl = null,
        string? baseDirectory = null)
    {
        CopilotClientOptions clientOptions = new()
        {
            Mode = CopilotClientMode.Empty,
        };

        if (!string.IsNullOrWhiteSpace(runtimeUrl))
        {
            clientOptions.Connection = RuntimeConnection.ForUri(runtimeUrl);
        }

        if (!string.IsNullOrWhiteSpace(baseDirectory))
        {
            clientOptions.BaseDirectory = baseDirectory;
        }

        return clientOptions;
    }

    /// <summary>
    /// Wraps a <see cref="CopilotClient"/> as a Microsoft Agent Framework <see cref="AIAgent"/>
    /// with a workflow-safe name (<see cref="AgentName"/>) and default <c>id: null</c>.
    /// </summary>
    /// <param name="copilotClient">The <see cref="CopilotClient"/> instance.</param>
    /// <param name="instructions">Optional instructions appended to the session system message.</param>
    /// <returns>The configured <see cref="AIAgent"/>.</returns>
    public static AIAgent CreateCopilotAgent(
        CopilotClient copilotClient,
        string? instructions = DefaultInstructions)
    {
        ArgumentNullException.ThrowIfNull(copilotClient);

        SessionConfig sessionConfig = new()
        {
            Streaming = true,
            AvailableTools = [],
            SystemMessage = string.IsNullOrWhiteSpace(instructions)
                ? null
                : new SystemMessageConfig
                {
                    Mode = SystemMessageMode.Append,
                    Content = instructions,
                },
        };

        // Leave id: null so AIAgent generates a 32-hex GUID that WorkflowNamingHelper.GetExecutorName
        // can strip cleanly when the agent is used inside a Durable Workflow.
        return copilotClient.AsAIAgent(
            sessionConfig: sessionConfig,
            ownsClient: false,
            id: null,
            name: AgentName,
            description: "Durable GitHub Copilot agent");
    }

    /// <summary>
    /// Registers the Copilot agent with <see cref="DurableAgentsOptions"/> using
    /// <see cref="DurableAgentHistoryReplayMode.CurrentRequestOnly"/> and <see cref="HistoryProviderKey"/>.
    /// </summary>
    /// <param name="options">The durable agents options builder.</param>
    /// <param name="copilotAgent">The Copilot <see cref="AIAgent"/> to register.</param>
    /// <param name="timeToLive">Optional entity time-to-live (defaults to 1 hour).</param>
    public static void RegisterCopilotAgent(
        DurableAgentsOptions options,
        AIAgent copilotAgent,
        TimeSpan? timeToLive = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(copilotAgent);

        // GitHubCopilotAgent persists GitHubCopilotAgentSession.SessionId in durable entity state
        // and calls CopilotClient.ResumeSessionAsync(sessionId) on subsequent turns.
        // Using CurrentRequestOnly prevents AgentEntity from also prepending prior turns into the prompt.
        options.AddAIAgent(
            copilotAgent,
            timeToLive: timeToLive ?? TimeSpan.FromHours(1),
            configureHistory: static history =>
            {
                history.ReplayMode = DurableAgentHistoryReplayMode.CurrentRequestOnly;
                history.ProviderKey = new DurableAgentHistoryProviderKey(HistoryProviderKey);
            });
    }

    /// <summary>
    /// Builds a single-node <see cref="Workflow"/> backed by the Copilot <see cref="AIAgent"/>.
    /// </summary>
    /// <param name="copilotAgent">The Copilot <see cref="AIAgent"/>.</param>
    /// <returns>A configured <see cref="Workflow"/>.</returns>
    public static Workflow BuildTriageWorkflow(AIAgent copilotAgent)
    {
        ArgumentNullException.ThrowIfNull(copilotAgent);

        return new WorkflowBuilder(copilotAgent)
            .WithName(WorkflowName)
            .Build();
    }

    /// <summary>
    /// Durable orchestration that runs a two-turn incident remediation conversation on a single
    /// <see cref="AgentSession"/>, pausing between turns for human approval via <see cref="ApprovalEventName"/>.
    /// </summary>
    /// <param name="context">The task orchestration context.</param>
    /// <param name="input">The incident remediation input.</param>
    /// <returns>The orchestration result containing both turns' outputs.</returns>
    public static async Task<CopilotIncidentResult> RunIncidentOrchestrationAsync(
        TaskOrchestrationContext context,
        CopilotIncidentInput input)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(input);

        DurableAIAgent agent = context.GetAgent(AgentName);
        AgentSession session = await agent.CreateSessionAsync();

        context.SetCustomStatus($"Diagnosing incident: {input.IncidentSummary}");

        // Turn 1: Ask Copilot to diagnose the incident and propose a remediation plan.
        AgentResponse planResponse = await agent.RunAsync(
            $"Diagnose this incident and propose a step-by-step remediation plan: {input.IncidentSummary}",
            session);

        context.SetCustomStatus($"Waiting for human approval ({ApprovalEventName})...");

        bool approved = await context.WaitForExternalEvent<bool>(ApprovalEventName);

        if (!approved)
        {
            context.SetCustomStatus("Remediation plan rejected by operator.");
            return new CopilotIncidentResult(
                ProposedPlan: planResponse.Text,
                Approved: false,
                FinalResolution: "Remediation cancelled by human operator.");
        }

        context.SetCustomStatus("Approved. Executing Turn 2 on the same Copilot session...");

        // Turn 2: Resume the same Copilot session to generate the post-approval execution commands.
        AgentResponse executionResponse = await agent.RunAsync(
            "The operator approved the remediation plan. Provide the exact verification steps for the approved plan.",
            session);

        context.SetCustomStatus("Incident remediation completed.");

        return new CopilotIncidentResult(
            ProposedPlan: planResponse.Text,
            Approved: true,
            FinalResolution: executionResponse.Text);
    }
}
