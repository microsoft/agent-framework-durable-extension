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
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MaxSuperstepsExceededException"/> class with the specified message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public MaxSuperstepsExceededException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MaxSuperstepsExceededException"/> class with an inner exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public MaxSuperstepsExceededException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
