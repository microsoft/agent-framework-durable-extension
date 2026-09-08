// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// The exception thrown when automatic retention cannot reduce durable agent state below its safe write threshold.
/// </summary>
public sealed class DurableAgentStateSizeLimitExceededException : InvalidOperationException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DurableAgentStateSizeLimitExceededException"/> class.
    /// </summary>
    public DurableAgentStateSizeLimitExceededException()
    {
    }

    /// <summary>
    /// Initializes a new instance with a specified error message.
    /// </summary>
    public DurableAgentStateSizeLimitExceededException(string? message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance with a specified error message and inner exception.
    /// </summary>
    public DurableAgentStateSizeLimitExceededException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DurableAgentStateSizeLimitExceededException"/> class.
    /// </summary>
    /// <param name="stateSizeBytes">The final serialized state size.</param>
    /// <param name="maxStateBytes">The configured state budget.</param>
    public DurableAgentStateSizeLimitExceededException(int stateSizeBytes, int maxStateBytes)
        : base(
            $"Durable agent state has a protected floor of {stateSizeBytes} bytes after all eligible transcript " +
            $"eviction and cannot be safely persisted within the configured {maxStateBytes} byte budget.")
    {
        this.StateSizeBytes = stateSizeBytes;
        this.MaxStateBytes = maxStateBytes;
    }

    /// <summary>
    /// Gets the final serialized state size.
    /// </summary>
    public int StateSizeBytes { get; }

    /// <summary>
    /// Gets the configured state budget.
    /// </summary>
    public int MaxStateBytes { get; }
}
