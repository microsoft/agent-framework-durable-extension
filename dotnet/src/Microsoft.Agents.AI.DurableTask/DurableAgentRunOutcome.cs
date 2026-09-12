// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;

namespace Microsoft.Agents.AI.DurableTask;

internal enum DurableAgentRunOutcomeKind
{
    Pending,
    Succeeded,
    Failed,
    CompletedResultUnavailable,
}

internal sealed record DurableAgentRunOutcome(
    DurableAgentRunOutcomeKind Kind,
    AgentResponse? Response,
    DurableAgentStateTerminalError? Error,
    DurableAgentStateCompletionReceipt? Receipt)
{
    /// <summary>Gets the caller-visible JSON value; Undefined means absent, not explicit null.</summary>
    public JsonElement Value { get; init; }

    public static DurableAgentRunOutcome Pending { get; } =
        new(DurableAgentRunOutcomeKind.Pending, null, null, null);

    public static DurableAgentRunOutcome Succeeded(
        AgentResponse response,
        DurableAgentStateCompletionReceipt? receipt) =>
        new(DurableAgentRunOutcomeKind.Succeeded, response, null, receipt);

    public static DurableAgentRunOutcome Failed(
        AgentResponse response,
        DurableAgentStateTerminalError error,
        DurableAgentStateCompletionReceipt? receipt) =>
        new(DurableAgentRunOutcomeKind.Failed, response, error, receipt);

    public static DurableAgentRunOutcome CompletedResultUnavailable(
        DurableAgentStateCompletionReceipt receipt) =>
        new(DurableAgentRunOutcomeKind.CompletedResultUnavailable, null, null, receipt);

    internal AgentResponse GetResponse(string correlationId) => this.Kind switch
    {
        DurableAgentRunOutcomeKind.Succeeded => this.Response!,
        DurableAgentRunOutcomeKind.Failed => throw new DurableAgentTerminalException(
            correlationId, this.Error!.Code, this.Error.Message, this.Error.Details, this.Response!),
        DurableAgentRunOutcomeKind.CompletedResultUnavailable => throw new DurableAgentResultUnavailableException(
            correlationId, this.Receipt!.CompletedAt, this.Receipt.ResultExpiresAt, this.Receipt.Outcome),
        _ => throw new InvalidOperationException($"Durable agent outcome '{this.Kind}' is not terminal."),
    };
}
