// Copyright (c) Microsoft. All rights reserved.

using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Entities;

namespace Microsoft.Agents.AI.DurableTask;

// Durable Task serializes exception type, message, and inner failure, but not custom CLR
// properties. This framework-only inner exception carries a versioned metadata snapshot.
internal static class DurableAgentFailure
{
    internal static Exception CreateMetadataException(DurableAgentFailureData data, Exception? innerException = null) =>
        new DurableAgentFailureMetadataException(
            JsonSerializer.Serialize(data, DurableAgentJsonUtilities.JsonContext.Default.DurableAgentFailureData), innerException);

    internal static bool TryRestore(Exception exception, [NotNullWhen(true)] out Exception? restored)
    {
        restored = null;
        TaskFailureDetails? failure = exception switch
        {
            EntityOperationFailedException entityFailure => entityFailure.FailureDetails,
            TaskFailedException taskFailure => taskFailure.FailureDetails,
            _ => null,
        };
        bool terminal = failure?.ErrorType == typeof(DurableAgentTerminalException).FullName;
        bool unavailable = failure?.ErrorType == typeof(DurableAgentResultUnavailableException).FullName;
        if (!terminal && !unavailable)
        {
            return false;
        }

        TaskFailureDetails? metadata = failure!.InnerFailure;
        if (metadata?.ErrorType != typeof(DurableAgentFailureMetadataException).FullName)
        {
            // Older or message-only failures still fail with a typed exception. Never try
            // to extract a contract from their human/model-authored error message.
            restored = terminal
                ? new DurableAgentTerminalException(failure.ErrorMessage, exception)
                : new DurableAgentResultUnavailableException(failure.ErrorMessage, exception);
            return true;
        }

        try
        {
            DurableAgentFailureData? data = JsonSerializer.Deserialize(
                metadata!.ErrorMessage, DurableAgentJsonUtilities.JsonContext.Default.DurableAgentFailureData);
            if (data is null || data.Version != 1 || string.IsNullOrWhiteSpace(data.CorrelationId))
            {
                return false;
            }

            if (terminal && !string.IsNullOrEmpty(data.Code) && data.SerializedResponse is not null)
            {
                AgentResponse? response = new DurableDataConverter().Deserialize(data.SerializedResponse, typeof(AgentResponse)) as AgentResponse;
                if (response is not null)
                {
                    restored = new DurableAgentTerminalException(
                        data.CorrelationId, data.Code, failure.ErrorMessage, data.Details, response, exception);
                }
            }
            else if (unavailable && data.CompletedAt is DateTimeOffset completedAt &&
                data.Outcome is DurableAgentStateCompletionReceipt.SucceededOutcome or DurableAgentStateCompletionReceipt.FailedOutcome)
            {
                restored = new DurableAgentResultUnavailableException(
                    data.CorrelationId, completedAt, data.ResultExpiresAt, data.Outcome, exception);
            }
        }
        catch (JsonException)
        {
            // Unsupported/malformed metadata must leave the original SDK failure intact.
        }
        catch (InvalidOperationException)
        {
            // Includes invalid canonical response metadata; never degrade it to success.
        }

        return restored is not null;
    }
}

internal sealed class DurableAgentFailureData
{
    public required int Version { get; init; }

    public required string CorrelationId { get; init; }

    public string? Code { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public JsonElement Details { get; init; }

    public string? SerializedResponse { get; init; }

    public DateTimeOffset? CompletedAt { get; init; }

    public DateTimeOffset? ResultExpiresAt { get; init; }

    public string? Outcome { get; init; }
}
