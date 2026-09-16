// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Carries framework-serialized metadata as the inner exception of a durable agent failure.
/// </summary>
/// <remarks>
/// Durable Task retains exception messages and inner failures, but not custom exception properties.
/// Applications should handle the enclosing <see cref="DurableAgentTerminalException"/> or
/// <see cref="DurableAgentResultUnavailableException"/>, rather than interpret this transport payload.
/// </remarks>
public sealed class DurableAgentFailureMetadataException : Exception
{
    /// <summary>Initializes an empty instance.</summary>
    public DurableAgentFailureMetadataException()
    {
    }

    /// <summary>Initializes an instance with a message.</summary>
    public DurableAgentFailureMetadataException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes an instance with a message and inner exception.</summary>
    public DurableAgentFailureMetadataException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }
}
