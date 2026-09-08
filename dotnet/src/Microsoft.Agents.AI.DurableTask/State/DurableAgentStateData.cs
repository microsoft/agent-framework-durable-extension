// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Represents the data of a durable agent, including its conversation history.
/// </summary>
internal sealed class DurableAgentStateData
{
    /// <summary>
    /// Gets the ordered list of state entries representing the complete conversation history.
    /// This includes both user messages and agent responses in chronological order.
    /// </summary>
    [JsonPropertyName("conversationHistory")]
    public IList<DurableAgentStateEntry> ConversationHistory { get; init; } = [];

    /// <summary>
    /// Gets or sets the serialized inner agent session.
    /// </summary>
    [JsonPropertyName("session")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public JsonElement? Session { get; set; }

    /// <summary>
    /// Gets or sets the highest workflow conversation position ingested from each executor.
    /// </summary>
    /// <remarks>
    /// The .NET workflow path does not populate these watermarks yet, but they are preserved for
    /// cross-language schema compatibility.
    /// </remarks>
    [JsonPropertyName("ingestedPositions")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IDictionary<string, int>? IngestedPositions { get; set; }

    /// <summary>
    /// Gets or sets bounded evidence that retention removed conversation messages.
    /// </summary>
    [JsonPropertyName("truncation")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DurableAgentStateTruncation? Truncation { get; set; }

    /// <summary>
    /// Gets or sets the expiration time (UTC) for this agent entity.
    /// If the entity is idle beyond this time, it will be automatically deleted.
    /// </summary>
    [JsonPropertyName("expirationTimeUtc")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTime? ExpirationTimeUtc { get; set; }

    /// <summary>
    /// Gets application-defined data-level metadata from the schema's <c>extensionData</c> property.
    /// </summary>
    [JsonPropertyName("extensionData")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IDictionary<string, JsonElement>? ExtensionData { get; init; }

    /// <summary>
    /// Gets unknown data properties that are outside the declared schema.
    /// </summary>
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? UnknownProperties { get; set; }
}
