// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// The exception thrown when durable state records a terminal request failure.
/// </summary>
public sealed class DurableAgentTerminalException : InvalidOperationException
{
    /// <summary>Initializes an empty instance.</summary>
    public DurableAgentTerminalException()
    {
    }

    /// <summary>Initializes an instance with a message.</summary>
    public DurableAgentTerminalException(string? message)
        : base(message)
    {
    }

    /// <summary>Initializes an instance with a message and inner exception.</summary>
    public DurableAgentTerminalException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    internal DurableAgentTerminalException(
        string correlationId,
        string code,
        string message,
        JsonElement? details,
        AgentResponse response,
        Exception? innerException = null)
        : base(message, DurableAgentFailure.CreateMetadataException(new DurableAgentFailureData
        {
            Version = 1,
            CorrelationId = correlationId,
            Code = code,
            Details = details ?? default,
            SerializedResponse = new DurableDataConverter().Serialize(response),
        }, innerException))
    {
        this.CorrelationId = correlationId;
        this.Code = code;
        this.Details = details is JsonElement { ValueKind: not JsonValueKind.Undefined } value ? value.Clone() : null;
        this.Response = response;
    }

    /// <summary>Gets the failed request correlation, when available.</summary>
    public string? CorrelationId { get; }

    /// <summary>Gets the durable terminal error code, when available.</summary>
    public string? Code { get; }

    /// <summary>Gets structured durable terminal error details, when available.</summary>
    public JsonElement? Details { get; }

    /// <summary>Gets the recorded response payload, when available.</summary>
    public AgentResponse? Response { get; }
}
