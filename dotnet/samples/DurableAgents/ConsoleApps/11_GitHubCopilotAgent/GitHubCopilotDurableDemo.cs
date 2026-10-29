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
/// <param name="TimedOut">Whether the human approval step exceeded the configured SLA timer.</param>
/// <param name="FinalResolution">Final execution summary from Turn 2 (or cancellation/timeout message).</param>
public sealed record CopilotIncidentResult(
    string ProposedPlan,
    bool Approved,
    bool TimedOut,
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
        // Use CopilotClientMode.Empty so session tool access is explicitly controlled by SessionConfig.
        CopilotClientOptions clientOptions = new()
        {
            Mode = CopilotClientMode.Empty,
        };

        // Connect to a remote/shared Copilot runtime endpoint when COPILOT_RUNTIME_URL is configured;
        // otherwise CopilotClient launches and communicates with the local `copilot` CLI binary.
        if (!string.IsNullOrWhiteSpace(runtimeUrl))
        {
            clientOptions.Connection = RuntimeConnection.ForUri(runtimeUrl);
        }

        // Optional shared filesystem path for COPILOT_HOME/session-state/{sessionId} in multi-worker hosts.
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

        // CopilotClientMode.Empty requires AvailableTools to be explicitly initialized on SessionConfig.
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

        // Use a single-word alphanumeric name ("CopilotAgent") and leave id: null so AIAgent generates
        // a 32-hex GUID that WorkflowNamingHelper.GetExecutorName can strip cleanly in Durable Workflows.
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
        // Setting ReplayMode = CurrentRequestOnly with an explicit ProviderKey ensures AgentEntity
        // sends only the new turn's prompt instead of prepending prior turns into the prompt.
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
    /// Fires two concurrent alert updates at the same <see cref="AgentSession"/> using <see cref="Task.WhenAll(Task[])"/>.
    /// </summary>
    /// <remarks>
    /// In raw <c>GitHub.Copilot.SDK</c>, calling <c>SendAsync</c> concurrently on the same session is unsupported
    /// and requires an external distributed lock (such as Redis). When hosted as a Durable Agent, the entity
    /// mailbox (<c>@dafx-v2-copilotagent@{key}</c>) automatically serializes concurrent turns one at a time on
    /// the same underlying <c>GitHubCopilotAgentSession.SessionId</c>.
    /// </remarks>
    /// <param name="durableAgent">The durable agent proxy.</param>
    /// <param name="session">The shared durable agent session receiving concurrent requests.</param>
    /// <param name="firstAlert">First concurrent alert payload.</param>
    /// <param name="secondAlert">Second concurrent alert payload.</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    /// <returns>The two completed <see cref="AgentResponse"/> instances.</returns>
    public static async Task<(AgentResponse First, AgentResponse Second)> RunConcurrentAlertBurstAsync(
        AIAgent durableAgent,
        AgentSession session,
        string firstAlert,
        string secondAlert,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(durableAgent);
        ArgumentNullException.ThrowIfNull(session);
        ArgumentException.ThrowIfNullOrWhiteSpace(firstAlert);
        ArgumentException.ThrowIfNullOrWhiteSpace(secondAlert);

        // Issue both requests concurrently against the same DurableAgentSession.
        // The backing Durable Entity queues and executes them sequentially.
        Task<AgentResponse> firstTurnTask = durableAgent.RunAsync(firstAlert, session, cancellationToken: cancellationToken);
        Task<AgentResponse> secondTurnTask = durableAgent.RunAsync(secondAlert, session, cancellationToken: cancellationToken);

        AgentResponse[] responses = await Task.WhenAll(firstTurnTask, secondTurnTask);
        return (responses[0], responses[1]);
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
    /// <see cref="AgentSession"/>, pausing between turns for human approval via <see cref="ApprovalEventName"/>
    /// bounded by a durable SLA timer (<see cref="TaskOrchestrationContext.CreateTimer(DateTime, CancellationToken)"/>).
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

        // Step 1: Obtain the durable CopilotAgent proxy and create a deterministic DurableAgentSession.
        // All turns using this session route to the same @dafx-v2-copilotagent@{key} entity instance.
        DurableAIAgent agent = context.GetAgent(AgentName);
        AgentSession session = await agent.CreateSessionAsync();

        context.SetCustomStatus($"Diagnosing incident: {input.IncidentSummary}");

        // Step 2 (Turn 1): Ask Copilot to diagnose the incident and propose a remediation plan.
        // AgentEntity creates the underlying CopilotSession and persists its SessionId in entity state.
        AgentResponse planResponse = await agent.RunAsync(
            $"Diagnose this incident and propose a step-by-step remediation plan: {input.IncidentSummary}",
            session);

        // Step 3: Publish the proposed plan via custom status and pause for human approval,
        // racing the external "Approval" event against a durable SLA timer.
        // While waiting here, the orchestration is dehydrated to storage and requires zero active workers.
        int slaMinutes = Math.Max(1, input.ApprovalTimeoutMinutes);
        context.SetCustomStatus(
            $"Waiting for human approval ({ApprovalEventName}) within {slaMinutes}m SLA. Proposed plan: {planResponse.Text}");

        using CancellationTokenSource timerCts = new();
        Task<bool> approvalTask = context.WaitForExternalEvent<bool>(ApprovalEventName);
        DateTime slaDeadline = context.CurrentUtcDateTime.AddMinutes(slaMinutes);
        Task timeoutTask = context.CreateTimer(slaDeadline, timerCts.Token);

        Task winner = await Task.WhenAny(approvalTask, timeoutTask);
        if (ReferenceEquals(winner, timeoutTask))
        {
            // The SLA timer fired before an operator responded; exit cleanly without running Turn 2.
            context.SetCustomStatus($"Approval SLA expired after {slaMinutes}m.");
            return new CopilotIncidentResult(
                ProposedPlan: planResponse.Text,
                Approved: false,
                TimedOut: true,
                FinalResolution: $"Remediation timed out after {slaMinutes} minutes waiting for operator approval.");
        }

        // Step 4: Approval event arrived first—cancel the outstanding durable SLA timer.
        await timerCts.CancelAsync();
        bool approved = await approvalTask;

        if (!approved)
        {
            // Operator rejected the proposed remediation plan; skip Turn 2.
            context.SetCustomStatus("Remediation plan rejected by operator.");
            return new CopilotIncidentResult(
                ProposedPlan: planResponse.Text,
                Approved: false,
                TimedOut: false,
                FinalResolution: "Remediation cancelled by human operator.");
        }

        context.SetCustomStatus("Approved. Executing Turn 2 on the same Copilot session...");

        // Step 5 (Turn 2): Resume the exact same Copilot session (even if the worker host was stopped
        // and replaced while waiting for approval) to produce the post-approval verification steps.
        AgentResponse executionResponse = await agent.RunAsync(
            "The operator approved the remediation plan. Provide the exact verification steps for the approved plan.",
            session);

        context.SetCustomStatus("Incident remediation completed.");

        return new CopilotIncidentResult(
            ProposedPlan: planResponse.Text,
            Approved: true,
            TimedOut: false,
            FinalResolution: executionResponse.Text);
    }
}
