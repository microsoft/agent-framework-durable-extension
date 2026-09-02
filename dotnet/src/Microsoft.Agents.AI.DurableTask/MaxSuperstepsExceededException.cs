// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Exception thrown when a workflow reaches its configured superstep limit with work still queued.
/// </summary>
public sealed class MaxSuperstepsExceededException : InvalidOperationException
{
    // Not used, but required by static analysis.
    private MaxSuperstepsExceededException()
    {
        this.InstanceId = string.Empty;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MaxSuperstepsExceededException"/> class with the specified message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public MaxSuperstepsExceededException(string message)
        : base(message)
    {
        this.InstanceId = string.Empty;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MaxSuperstepsExceededException"/> class with an inner exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public MaxSuperstepsExceededException(string message, Exception? innerException)
        : base(message, innerException)
    {
        this.InstanceId = string.Empty;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MaxSuperstepsExceededException"/> class.
    /// </summary>
    /// <param name="instanceId">The ID of the workflow instance that exceeded the limit.</param>
    /// <param name="maxSupersteps">The configured maximum number of supersteps.</param>
    /// <param name="remainingExecutors">The number of executors that still had queued work.</param>
    public MaxSuperstepsExceededException(string instanceId, int maxSupersteps, int remainingExecutors)
        : base(GetMessage(instanceId, maxSupersteps, remainingExecutors))
    {
        this.InstanceId = instanceId;
        this.MaxSupersteps = maxSupersteps;
        this.RemainingExecutors = remainingExecutors;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MaxSuperstepsExceededException"/> class with an inner exception.
    /// </summary>
    /// <param name="instanceId">The ID of the workflow instance that exceeded the limit.</param>
    /// <param name="maxSupersteps">The configured maximum number of supersteps.</param>
    /// <param name="remainingExecutors">The number of executors that still had queued work.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public MaxSuperstepsExceededException(string instanceId, int maxSupersteps, int remainingExecutors, Exception? innerException)
        : base(GetMessage(instanceId, maxSupersteps, remainingExecutors), innerException)
    {
        this.InstanceId = instanceId;
        this.MaxSupersteps = maxSupersteps;
        this.RemainingExecutors = remainingExecutors;
    }

    /// <summary>
    /// Gets the ID of the workflow instance that exceeded the limit.
    /// </summary>
    public string InstanceId { get; }

    /// <summary>
    /// Gets the configured maximum number of supersteps.
    /// </summary>
    public int MaxSupersteps { get; }

    /// <summary>
    /// Gets the number of executors that still had queued work.
    /// </summary>
    public int RemainingExecutors { get; }

    private static string GetMessage(string instanceId, int maxSupersteps, int remainingExecutors)
    {
        ArgumentException.ThrowIfNullOrEmpty(instanceId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxSupersteps);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(remainingExecutors);

        return $"Workflow instance '{instanceId}' reached the maximum of {maxSupersteps} supersteps with {remainingExecutors} executor(s) still queued.";
    }
}
