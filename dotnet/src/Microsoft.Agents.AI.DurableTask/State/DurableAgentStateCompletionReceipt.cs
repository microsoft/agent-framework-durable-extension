// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Immutable evidence that a correlation completed, retained independently from its result payload.
/// </summary>
internal sealed class DurableAgentStateCompletionReceipt
{
    public const string SucceededOutcome = "succeeded";
    public const string FailedOutcome = "failed";
    public const string AvailableResult = "available";
    public const string UnavailableResult = "unavailable";

    [JsonPropertyName("correlationId")]
    public required string CorrelationId { get; init; }

    [JsonPropertyName("outcome")]
    public required string Outcome { get; init; }

    [JsonPropertyName("completedAt")]
    public required DateTimeOffset CompletedAt { get; init; }

    [JsonPropertyName("resultState")]
    public required string ResultState { get; init; }

    [JsonPropertyName("resultExpiresAt")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTimeOffset? ResultExpiresAt { get; init; }

    [JsonPropertyName("resultUnavailableAt")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTimeOffset? ResultUnavailableAt { get; init; }

    [JsonExtensionData]
    public IDictionary<string, JsonElement>? UnknownProperties { get; set; }

    public void Validate(string dictionaryKey)
    {
        DurableAgentStateContract.ValidateIdentifier(dictionaryKey, "completionReceipts key");
        DurableAgentStateContract.ValidateIdentifier(this.CorrelationId, "completionReceipts.correlationId");
        if (!string.Equals(dictionaryKey, this.CorrelationId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"The durable agent state completion receipt key '{dictionaryKey}' does not match correlation ID '{this.CorrelationId}'.");
        }

        if (this.Outcome is not SucceededOutcome and not FailedOutcome)
        {
            throw new InvalidOperationException(
                $"The durable agent state completion outcome '{this.Outcome}' is not supported.");
        }

        if (this.CompletedAt == default)
        {
            throw new InvalidOperationException(
                "A durable agent completion receipt requires a completion timestamp.");
        }

        if (this.ResultState is not AvailableResult and not UnavailableResult)
        {
            throw new InvalidOperationException(
                $"The durable agent state result state '{this.ResultState}' is not supported.");
        }

        if (this.ResultExpiresAt < this.CompletedAt)
        {
            throw new InvalidOperationException(
                "The durable agent state result expiry cannot precede completion.");
        }

        if (this.ResultState == AvailableResult && this.ResultUnavailableAt is not null)
        {
            throw new InvalidOperationException(
                "An available durable agent result cannot have an unavailable timestamp.");
        }

        if (this.ResultState == UnavailableResult &&
            (this.ResultUnavailableAt is null || this.ResultUnavailableAt < this.CompletedAt))
        {
            throw new InvalidOperationException(
                "An unavailable durable agent result requires an unavailable timestamp at or after completion.");
        }
    }
}
