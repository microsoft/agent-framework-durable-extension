// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// The exception thrown when durable history ownership cannot be determined through public Agent Framework APIs.
/// </summary>
public sealed class DurableAgentHistoryOwnershipNotSupportedException : NotSupportedException
{
    private const string DefaultMessage =
        "Agent Framework per-service-call history persistence can represent either framework-local history " +
        "or a service-managed conversation, but it does not publicly expose which kind a conversation ID is. " +
        "Call DurableAgentsOptions.SetServiceManagedPerServiceCallHistory when the model service owns history. " +
        "Framework-local per-service-call persistence is not currently supported by durable agents. The declaration " +
        "is ignored when RequirePerServiceCallChatHistoryPersistence is disabled.";

    /// <summary>
    /// Initializes a new instance of the <see cref="DurableAgentHistoryOwnershipNotSupportedException"/> class.
    /// </summary>
    public DurableAgentHistoryOwnershipNotSupportedException()
        : base(DefaultMessage)
    {
    }

    /// <summary>
    /// Initializes a new instance with a specified error message.
    /// </summary>
    public DurableAgentHistoryOwnershipNotSupportedException(string? message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance with a specified error message and inner exception.
    /// </summary>
    public DurableAgentHistoryOwnershipNotSupportedException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
