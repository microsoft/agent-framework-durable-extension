// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json.Serialization;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Logging;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Represents a durable agent state entry that is a response from the agent.
/// </summary>
internal class DurableAgentStateResponse : DurableAgentStateEntry
{
    /// <summary>
    /// Gets the usage details for this state response.
    /// </summary>
    [JsonPropertyName("usage")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DurableAgentStateUsage? Usage { get; init; }

    /// <summary>
    /// Creates a <see cref="DurableAgentStateResponse"/> from an <see cref="AgentResponse"/>.
    /// </summary>
    /// <param name="correlationId">The correlation ID linking this response to its request.</param>
    /// <param name="response">The <see cref="AgentResponse"/> to convert.</param>
    /// <param name="logger">The logger used to report safe unknown-content fallbacks.</param>
    /// <returns>A <see cref="DurableAgentStateResponse"/> representing the original response.</returns>
    public static DurableAgentStateResponse FromResponse(
        string correlationId,
        AgentResponse response,
        ILogger? logger = null)
        => FromResponse(correlationId, response, allowLosslessV2: false, logger);

    internal static DurableAgentStateResponse FromResponseV2(
        string correlationId,
        AgentResponse response,
        ILogger? logger = null)
        => FromResponse(correlationId, response, allowLosslessV2: true, logger);

    private static DurableAgentStateResponse FromResponse(
        string correlationId,
        AgentResponse response,
        bool allowLosslessV2,
        ILogger? logger)
    {
        List<ChatMessage> messages = response.Messages.ToList();
        DateTimeOffset createdAt = response.CreatedAt ?? GetCreatedAt(messages);
        return new DurableAgentStateResponse()
        {
            CorrelationId = correlationId,
            CreatedAt = createdAt,
            Messages = CreateStoredMessages(messages, correlationId, createdAt, logger, allowLosslessV2),
            Usage = DurableAgentStateUsage.FromUsage(response.Usage)
        };
    }

    /// <summary>
    /// Creates a response entry from response messages before aggregate response metadata is available.
    /// </summary>
    public static DurableAgentStateResponse FromMessages(
        string correlationId,
        IEnumerable<ChatMessage> messages,
        ILogger? logger = null)
        => FromMessages(correlationId, messages, allowLosslessV2: false, logger);

    internal static DurableAgentStateResponse FromMessagesV2(
        string correlationId,
        IEnumerable<ChatMessage> messages,
        ILogger? logger = null)
        => FromMessages(correlationId, messages, allowLosslessV2: true, logger);

    private static DurableAgentStateResponse FromMessages(
        string correlationId,
        IEnumerable<ChatMessage> messages,
        bool allowLosslessV2,
        ILogger? logger)
    {
        List<ChatMessage> messageList = messages.ToList();
        DateTimeOffset createdAt = GetCreatedAt(messageList);
        return new DurableAgentStateResponse()
        {
            CorrelationId = correlationId,
            CreatedAt = createdAt,
            Messages = CreateStoredMessages(
                messageList,
                correlationId,
                createdAt,
                logger,
                allowLosslessV2),
        };
    }

    /// <summary>
    /// Converts this <see cref="DurableAgentStateResponse"/> back to an <see cref="AgentResponse"/>.
    /// </summary>
    /// <returns>A <see cref="AgentResponse"/> representing this response.</returns>
    public AgentResponse ToResponse()
    {
        return new AgentResponse()
        {
            CreatedAt = this.CreatedAt,
            Messages = this.Messages.Select(m => m.ToChatMessage()).ToList(),
            Usage = this.Usage?.ToUsageDetails(),
        };
    }

    private static List<DurableAgentStateMessage> CreateStoredMessages(
        IEnumerable<ChatMessage> messages,
        string correlationId,
        DateTimeOffset createdAt,
        ILogger? logger,
        bool allowLosslessV2 = false)
    {
        return messages
            .Select((message, storedIndex) =>
            {
                string messageId = DurableAgentStateMessageIdentity.Create(
                    "response",
                    correlationId,
                    createdAt,
                    storedIndex);
                return allowLosslessV2
                    ? DurableAgentStateMessage.FromTerminalChatMessage(message, messageId, logger)
                    : DurableAgentStateMessage.FromChatMessage(message, messageId, logger);
            })
            .ToList();
    }

    private static DateTimeOffset GetCreatedAt(IReadOnlyList<ChatMessage> messages)
    {
        return messages
            .Select(message => message.CreatedAt)
            .Where(createdAt => createdAt.HasValue)
            .Select(createdAt => createdAt!.Value)
            .DefaultIfEmpty(DateTimeOffset.UtcNow)
            .Max();
    }
}
