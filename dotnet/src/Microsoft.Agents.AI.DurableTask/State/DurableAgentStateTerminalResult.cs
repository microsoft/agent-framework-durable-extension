// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Immutable terminal result envelope detached from evictable conversation history.
/// </summary>
internal sealed class DurableAgentStateTerminalResult
{
    [JsonPropertyName("correlationId")]
    public required string CorrelationId { get; init; }

    [JsonPropertyName("outcome")]
    public required string Outcome { get; init; }

    [JsonPropertyName("completedAt")]
    public required DateTimeOffset CompletedAt { get; init; }

    [JsonPropertyName("resultExpiresAt")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTimeOffset? ResultExpiresAt { get; init; }

    [JsonPropertyName("response")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DurableAgentStateTerminalResponse? Response { get; init; }

    [JsonPropertyName("error")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DurableAgentStateTerminalError? Error { get; init; }

    [JsonExtensionData]
    public IDictionary<string, JsonElement>? UnknownProperties { get; set; }

    public static DurableAgentStateTerminalResult FromResponse(
        string correlationId,
        AgentResponse response,
        DateTimeOffset completedAt,
        DateTimeOffset? resultExpiresAt = null,
        ILogger? logger = null)
    {
        DurableAgentStateContract.ValidateIdentifier(correlationId, "terminalResults.correlationId");
        return new()
        {
            CorrelationId = correlationId,
            Outcome = DurableAgentStateCompletionReceipt.SucceededOutcome,
            CompletedAt = completedAt,
            ResultExpiresAt = resultExpiresAt,
            Response = DurableAgentStateTerminalResponse.FromResponse(
                response,
                correlationId,
                completedAt,
                logger),
        };
    }

    public void Validate(string dictionaryKey)
    {
        DurableAgentStateContract.ValidateIdentifier(dictionaryKey, "terminalResults key");
        DurableAgentStateContract.ValidateIdentifier(this.CorrelationId, "terminalResults.correlationId");
        if (!string.Equals(dictionaryKey, this.CorrelationId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"The durable agent state terminal result key '{dictionaryKey}' does not match correlation ID '{this.CorrelationId}'.");
        }

        if (this.Outcome is not DurableAgentStateCompletionReceipt.SucceededOutcome and
            not DurableAgentStateCompletionReceipt.FailedOutcome)
        {
            throw new InvalidOperationException(
                $"The durable agent state terminal outcome '{this.Outcome}' is not supported.");
        }

        if (this.CompletedAt == default)
        {
            throw new InvalidOperationException(
                "A durable agent terminal result requires a completion timestamp.");
        }

        if (this.Response is null)
        {
            throw new InvalidOperationException(
                "A durable agent terminal result must contain a response payload.");
        }

        if (this.Outcome == DurableAgentStateCompletionReceipt.SucceededOutcome && this.Error is not null)
        {
            throw new InvalidOperationException(
                "A successful durable agent terminal result cannot contain error metadata.");
        }

        if (this.Outcome == DurableAgentStateCompletionReceipt.FailedOutcome && this.Error is null)
        {
            throw new InvalidOperationException(
                "A failed durable agent terminal result must contain error metadata.");
        }

        if (this.ResultExpiresAt < this.CompletedAt)
        {
            throw new InvalidOperationException(
                "The durable agent terminal result expiry cannot precede completion.");
        }

        this.Response.Validate();
        this.Error?.Validate();
    }
}
