// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// The exception thrown when a durable session is reopened with a different logical history owner.
/// </summary>
public sealed class DurableAgentHistoryBindingMismatchException : InvalidOperationException
{
    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    public DurableAgentHistoryBindingMismatchException()
    {
    }

    /// <summary>
    /// Initializes a new instance with a specified error message.
    /// </summary>
    public DurableAgentHistoryBindingMismatchException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance with a specified error message and inner exception.
    /// </summary>
    public DurableAgentHistoryBindingMismatchException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
