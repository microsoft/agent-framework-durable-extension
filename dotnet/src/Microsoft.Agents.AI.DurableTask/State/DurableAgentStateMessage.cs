// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Logging;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Represents a single message within a durable agent state entry.
/// </summary>
internal sealed class DurableAgentStateMessage
{
    /// <summary>
    /// Gets the name of the author of this message.
    /// </summary>
    [JsonPropertyName("authorName")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? AuthorName { get; init; }

    /// <summary>
    /// Gets the timestamp when this message was created.
    /// </summary>
    [JsonPropertyName("createdAt")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTimeOffset? CreatedAt { get; init; }

    /// <summary>
    /// Gets the stable message identifier.
    /// </summary>
    [JsonPropertyName("messageId")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? MessageId { get; set; }

    /// <summary>
    /// Gets producer-defined message values from the schema's declared <c>extensionData</c> field.
    /// </summary>
    /// <remarks>
    /// The CLR name mirrors <see cref="ChatMessage.AdditionalProperties"/> so conversion does not invent a
    /// second metadata vocabulary. The wire name remains <c>extensionData</c> for cross-language schema
    /// compatibility. This declared field is distinct from <see cref="UnknownProperties"/>, which captures
    /// undeclared future members adjacent to the message's known JSON fields.
    /// </remarks>
    [JsonPropertyName("extensionData")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IDictionary<string, JsonElement>? AdditionalProperties { get; init; }

    /// <summary>
    /// Gets the contents of this message.
    /// </summary>
    [JsonPropertyName("contents")]
    public IReadOnlyList<DurableAgentStateContent> Contents
    {
        get;
        init => field = value ?? [];
    } = [];

    /// <summary>
    /// Gets the role of the message sender (e.g., "user", "assistant", "system").
    /// </summary>
    [JsonPropertyName("role")]
    public required string Role { get; init; }

    /// <summary>
    /// Gets undeclared future message properties that appear beside the schema's known fields.
    /// </summary>
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? UnknownProperties { get; set; }

    /// <summary>
    /// Creates a <see cref="DurableAgentStateMessage"/> from a <see cref="ChatMessage"/>.
    /// </summary>
    /// <param name="message">The <see cref="ChatMessage"/> to convert.</param>
    /// <param name="generatedMessageId">The stable identifier to use when the message does not already have one.</param>
    /// <param name="logger">The logger used to report safe unknown-content fallbacks.</param>
    /// <returns>A <see cref="DurableAgentStateMessage"/> representing the original message.</returns>
    public static DurableAgentStateMessage FromChatMessage(
        ChatMessage message,
        string? generatedMessageId = null,
        ILogger? logger = null)
        => FromChatMessage(message, generatedMessageId, requireJsonSafeMetadata: false, logger);

    internal static DurableAgentStateMessage FromTerminalChatMessage(
        ChatMessage message,
        string? generatedMessageId = null,
        ILogger? logger = null)
        => FromChatMessage(message, generatedMessageId, requireJsonSafeMetadata: true, logger);

    private static DurableAgentStateMessage FromChatMessage(
        ChatMessage message,
        string? generatedMessageId,
        bool requireJsonSafeMetadata,
        ILogger? logger)
    {
        string role = message.Role.ToString();
        if (!requireJsonSafeMetadata &&
            role is not ("user" or "assistant" or "system" or "tool"))
        {
            throw new InvalidOperationException(
                $"The legacy durable agent state cannot persist message role '{role}'.");
        }

        Dictionary<string, JsonElement>? additionalProperties = null;
        if (message.AdditionalProperties is not null)
        {
            foreach ((string key, object? value) in message.AdditionalProperties)
            {
                JsonElement element = requireJsonSafeMetadata
                    ? DurableAgentStateTerminalResponse.ConvertMetadata(value, key)
                    : JsonSerializer.SerializeToElement(
                        value,
                        DurableAgentJsonUtilities.DefaultOptions.GetTypeInfo(typeof(object)));
                additionalProperties ??= [];
                additionalProperties[key] = element;
            }
        }

        return new DurableAgentStateMessage()
        {
            CreatedAt = message.CreatedAt,
            AuthorName = message.AuthorName,
            MessageId = message.MessageId ?? generatedMessageId,
            AdditionalProperties = additionalProperties,
            Role = role,
            Contents = message.Contents.Select(content =>
                requireJsonSafeMetadata
                    ? DurableAgentStateContent.FromAIContentV2(content, logger)
                    : DurableAgentStateContent.FromAIContent(content, logger)).ToList()
        };
    }

    /// <summary>
    /// Converts this <see cref="DurableAgentStateMessage"/> to a <see cref="ChatMessage"/>.
    /// </summary>
    /// <returns>A <see cref="ChatMessage"/> representing this message.</returns>
    public ChatMessage ToChatMessage() => this.ToChatMessage(static content => content.ToAIContent());

    /// <summary>
    /// Projects shared schema-2 content without inventing native representations for opaque shapes.
    /// </summary>
    internal ChatMessage ToChatMessageV2() => this.ToChatMessage(static content =>
        content is DurableAgentStateUriContent { MediaType: null }
            ? new DurableAgentStateUnknownContent
            {
                Content = JsonSerializer.SerializeToElement(
                    content, DurableAgentStateJsonContext.Default.DurableAgentStateContent),
            }.ToAIContent()
            : content.ToAIContent());

    private ChatMessage ToChatMessage(Func<DurableAgentStateContent, AIContent> convertContent)
    {
        AdditionalPropertiesDictionary? additionalProperties = this.AdditionalProperties is null
            ? null
            : new AdditionalPropertiesDictionary(
                this.AdditionalProperties.Select(pair =>
                    new KeyValuePair<string, object?>(pair.Key, pair.Value)));

        return new ChatMessage()
        {
            CreatedAt = this.CreatedAt,
            AuthorName = this.AuthorName,
            MessageId = this.MessageId,
            AdditionalProperties = additionalProperties,
            Contents = this.Contents.Select(convertContent).ToList(),
            Role = new(this.Role)
        };
    }

    public void ValidateV2()
    {
        if (this.Role is not "user" and
            not "assistant" and
            not "system" and
            not "developer" and
            not "tool")
        {
            throw new InvalidOperationException(
                $"The durable agent state message role '{this.Role}' is not supported.");
        }

        if (this.Contents.Any(static content => content is null))
        {
            throw new InvalidOperationException(
                "A durable agent state message cannot contain null content entries.");
        }

        foreach (DurableAgentStateContent content in this.Contents)
        {
            content.ValidateV2();
        }
    }
}
