// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Represents a single entry in the durable agent state, which can either be a
/// user/system request or agent response.
/// </summary>
[JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")]
[JsonDerivedType(typeof(DurableAgentStateRequest), "request")]
[JsonDerivedType(typeof(DurableAgentStateResponse), "response")]
[JsonDerivedType(typeof(DurableAgentStateErrorResponse), "errorResponse")]
[JsonDerivedType(typeof(DurableAgentStateCompaction), "compaction")]
internal abstract class DurableAgentStateEntry
{
    /// <summary>
    /// Gets the correlation ID for this entry.
    /// </summary>
    /// <remarks>
    /// This ID is used to correlate <see cref="DurableAgentStateResponse"/> back to its
    /// <see cref="DurableAgentStateRequest"/>. Compaction entries do not have a correlation ID.
    /// </remarks>
    [JsonPropertyName("correlationId")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? CorrelationId { get; init; }

    /// <summary>
    /// Gets the timestamp when this entry was created.
    /// </summary>
    [JsonPropertyName("createdAt")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTimeOffset? CreatedAt { get; init; }

    /// <summary>
    /// Gets the list of messages associated with this entry, in chronological order.
    /// </summary>
    [JsonPropertyName("messages")]
    public IReadOnlyList<DurableAgentStateMessage> Messages
    {
        get;
        init => field = value ?? [];
    } = [];

    /// <summary>
    /// Gets application-defined entry metadata from the schema's <c>extensionData</c> property.
    /// </summary>
    [JsonPropertyName("extensionData")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IDictionary<string, JsonElement>? ExtensionData { get; init; }

    /// <summary>
    /// Gets unknown entry properties that are outside the declared schema.
    /// </summary>
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? UnknownProperties { get; set; }

    public void ValidateV2()
    {
        if (this is DurableAgentStateCompaction)
        {
            if (this.CorrelationId is not null)
            {
                throw new InvalidOperationException(
                    "A durable agent compaction entry cannot have a correlation ID.");
            }
        }
        else if (this.CorrelationId is not null)
        {
            DurableAgentStateContract.ValidateIdentifier(
                this.CorrelationId,
                "conversationHistory.correlationId");
        }

        foreach (DurableAgentStateMessage? message in this.Messages)
        {
            if (message is null)
            {
                throw new InvalidOperationException(
                    "A revised durable agent state cannot contain null messages.");
            }

            message.ValidateV2();
        }
    }
}
