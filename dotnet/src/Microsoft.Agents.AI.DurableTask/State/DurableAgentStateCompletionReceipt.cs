// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Records durable evidence that a correlated request reached a terminal state,
/// independently of the transcript and the retained result payload.
/// The receipt prevents transcript pruning or result expiry from making completed
/// work appear pending or eligible to execute again.
/// </summary>
internal sealed class DurableAgentStateCompletionReceipt
{
    public const string SucceededOutcome = "succeeded";
    public const string FailedOutcome = "failed";
    public const string AvailableResult = "available";
    public const string UnavailableResult = "unavailable";

    /// <summary>
    /// Gets the durable correlation identifier for the completed request.
    /// This value must match the key used in the <c>completionReceipts</c>
    /// dictionary and is used to prevent a completed request from executing again.
    /// </summary>
    [JsonPropertyName("correlationId")]
    public required string CorrelationId { get; init; }

    /// <summary>
    /// Gets the terminal outcome of the request.
    /// The value remains available after the result payload expires so callers can
    /// distinguish a completed success from a completed failure without retaining
    /// the original response or error payload.
    /// </summary>
    [JsonPropertyName("outcome")]
    public required string Outcome { get; init; }

    /// <summary>
    /// Gets the time at which the request reached its committed terminal state.
    /// This records durable completion, not when the result was read or acknowledged
    /// by a caller.
    /// </summary>
    [JsonPropertyName("completedAt")]
    public required DateTimeOffset CompletedAt { get; init; }

    /// <summary>
    /// Gets whether the terminal result payload is still available for retrieval.
    /// A value of <c>available</c> means the corresponding result is expected to be
    /// present in the terminal-results mailbox. A value of <c>unavailable</c> means
    /// the request is still known to have completed, but its payload can no longer
    /// be returned.
    /// </summary>
    [JsonPropertyName("resultState")]
    public required string ResultState { get; init; }

    /// <summary>
    /// Gets the configured time after which the terminal result payload becomes
    /// eligible for removal.
    /// This does not remove the completion receipt or make the correlation reusable.
    /// A null value means that no result-payload expiry was scheduled.
    /// </summary>
    [JsonPropertyName("resultExpiresAt")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTimeOffset? ResultExpiresAt { get; init; }

    /// <summary>
    /// Gets the time at which the terminal result payload actually became
    /// unavailable.
    /// This is recorded when cleanup removes or invalidates the payload and may be
    /// later than <see cref="ResultExpiresAt"/>. It is required when
    /// <see cref="ResultState"/> is <c>unavailable</c>.
    /// </summary>
    [JsonPropertyName("resultUnavailableAt")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTimeOffset? ResultUnavailableAt { get; init; }

    /// <summary>
    /// Gets additional undeclared JSON properties preserved for forward
    /// compatibility.
    /// The durable runtime does not interpret these values, but retains them so a
    /// read-and-write cycle does not discard fields written by a newer runtime.
    /// </summary>
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
