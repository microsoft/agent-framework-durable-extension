// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Logging;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Represents a user or system request entry in the durable agent state.
/// </summary>
internal sealed class DurableAgentStateRequest : DurableAgentStateEntry
{
    /// <summary>
    /// Gets the ID of the orchestration that initiated this request (if any).
    /// </summary>
    [JsonPropertyName("orchestrationId")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? OrchestrationId { get; init; }

    /// <summary>
    /// Gets the expected response type for this request (e.g. "json" or "text").
    /// </summary>
    /// <remarks>
    /// If omitted, the expectation is that the agent will respond in plain text.
    /// </remarks>
    [JsonPropertyName("responseType")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? ResponseType { get; init; }

    /// <summary>
    /// Gets the expected response JSON schema for this request, if applicable.
    /// </summary>
    /// <remarks>
    /// This is only applicable when <see cref="ResponseType"/> is "json".
    /// If omitted, no specific schema is expected.
    /// </remarks>
    [JsonPropertyName("responseSchema")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public JsonElement? ResponseSchema { get; init; }

    /// <summary>
    /// Creates a <see cref="DurableAgentStateRequest"/> from a <see cref="RunRequest"/>.
    /// </summary>
    /// <param name="request">The <see cref="RunRequest"/> to convert.</param>
    /// <param name="logger">The logger used to report safe unknown-content fallbacks.</param>
    /// <returns>A <see cref="DurableAgentStateRequest"/> representing the original request.</returns>
    public static DurableAgentStateRequest FromRunRequest(
        RunRequest request,
        ILogger? logger = null)
        => FromRunRequest(request, allowLosslessV2: false, logger);

    internal static DurableAgentStateRequest FromRunRequestV2(
        RunRequest request,
        ILogger? logger = null)
        => FromRunRequest(request, allowLosslessV2: true, logger);

    private static DurableAgentStateRequest FromRunRequest(
        RunRequest request,
        bool allowLosslessV2,
        ILogger? logger)
    {
        DateTimeOffset createdAt = request.Messages.Min(m => m.CreatedAt) ?? DateTimeOffset.UtcNow;
        return new DurableAgentStateRequest()
        {
            CorrelationId = request.CorrelationId,
            OrchestrationId = request.OrchestrationId,
            Messages = request.Messages.Select(
                (message, index) =>
                {
                    string messageId = DurableAgentStateMessageIdentity.Create(
                        "request",
                        request.CorrelationId,
                        createdAt,
                        index);
                    return allowLosslessV2
                        ? DurableAgentStateMessage.FromTerminalChatMessage(message, messageId, logger)
                        : DurableAgentStateMessage.FromChatMessage(message, messageId, logger);
                }).ToList(),
            CreatedAt = createdAt,
            ResponseType = request.ResponseFormat is ChatResponseFormatJson ? "json" : "text",
            ResponseSchema = (request.ResponseFormat as ChatResponseFormatJson)?.Schema
        };
    }
}
