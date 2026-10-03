// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// The exception thrown when durable agent state violates a required storage invariant.
/// </summary>
public sealed class DurableAgentStateCorruptionException : InvalidOperationException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DurableAgentStateCorruptionException"/> class.
    /// </summary>
    public DurableAgentStateCorruptionException()
    {
    }

    /// <summary>
    /// Initializes a new instance with a specified error message.
    /// </summary>
    public DurableAgentStateCorruptionException(string? message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance with a specified error message and inner exception.
    /// </summary>
    public DurableAgentStateCorruptionException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// Initializes a new instance for duplicate terminal responses.
    /// </summary>
    public DurableAgentStateCorruptionException(string correlationId, int terminalResponseCount)
        : base(
            $"Durable agent state contains {terminalResponseCount} terminal responses for correlation " +
            $"'{correlationId}'; at most one terminal response is allowed.")
    {
        this.CorrelationId = correlationId;
        this.TerminalResponseCount = terminalResponseCount;
    }

    /// <summary>
    /// Gets the correlation ID whose invariant was violated, when available.
    /// </summary>
    public string? CorrelationId { get; }

    /// <summary>
    /// Gets the number of terminal responses found, when available.
    /// </summary>
    public int? TerminalResponseCount { get; }
}
