// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// The exception thrown when a durable agent is combined with unsupported stateful compaction.
/// </summary>
public sealed class DurableAgentCompactionNotSupportedException : NotSupportedException
{
    private const string DefaultMessage =
        "Durable .NET agents do not support directly discoverable stateful chat-history compaction. " +
        "The current Agent Framework compaction state contains full message copies. Persisting it duplicates " +
        "transcript content for entity, external-provider, and service-owned conversations, while discarding it " +
        "loses compaction semantics. Disable stateful compaction for durable execution.";

    /// <summary>
    /// Initializes a new instance of the <see cref="DurableAgentCompactionNotSupportedException"/> class.
    /// </summary>
    public DurableAgentCompactionNotSupportedException()
        : base(DefaultMessage)
    {
    }

    /// <summary>
    /// Initializes a new instance with a specified error message.
    /// </summary>
    public DurableAgentCompactionNotSupportedException(string? message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance with a specified error message and inner exception.
    /// </summary>
    public DurableAgentCompactionNotSupportedException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
