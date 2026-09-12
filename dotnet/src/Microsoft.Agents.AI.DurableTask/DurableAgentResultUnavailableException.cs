// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// The exception thrown when a durable request completed but its result payload is no longer available.
/// </summary>
public sealed class DurableAgentResultUnavailableException : InvalidOperationException
{
    /// <summary>Initializes an empty instance.</summary>
    public DurableAgentResultUnavailableException()
    {
    }

    /// <summary>Initializes an instance with a message.</summary>
    public DurableAgentResultUnavailableException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes an instance with a message and inner exception.</summary>
    public DurableAgentResultUnavailableException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    internal DurableAgentResultUnavailableException(
        string correlationId,
        DateTimeOffset completedAt,
        DateTimeOffset? resultExpiresAt,
        string? outcome = null,
        Exception? innerException = null)
        : base($"Durable agent request '{correlationId}' completed, but its result payload is unavailable.",
            DurableAgentFailure.CreateMetadataException(new DurableAgentFailureData
            {
                Version = 1,
                CorrelationId = correlationId,
                CompletedAt = completedAt,
                ResultExpiresAt = resultExpiresAt,
                Outcome = outcome,
            }, innerException))
    {
        this.CorrelationId = correlationId;
        this.CompletedAt = completedAt;
        this.ResultExpiresAt = resultExpiresAt;
        this.Outcome = outcome;
    }

    /// <summary>Gets the completed request correlation, when available.</summary>
    public string? CorrelationId { get; }

    /// <summary>Gets when the request completed, when available.</summary>
    public DateTimeOffset? CompletedAt { get; }

    /// <summary>Gets when the result payload expired, when configured.</summary>
    public DateTimeOffset? ResultExpiresAt { get; }

    /// <summary>
    /// Gets the original terminal outcome recorded by the completion receipt:
    /// <c>succeeded</c> or <c>failed</c>.
    /// </summary>
    /// <remarks>
    /// Result unavailability describes payload retention, not execution success. A missing or
    /// expired payload must not turn a recorded failure into success, or a recorded success into
    /// an execution failure. Durable entity delivery and polling populate this property from
    /// the authoritative receipt. It is <see langword="null"/> for exceptions constructed without
    /// receipt metadata using the general-purpose public constructors.
    /// </remarks>
    public string? Outcome { get; }
}
