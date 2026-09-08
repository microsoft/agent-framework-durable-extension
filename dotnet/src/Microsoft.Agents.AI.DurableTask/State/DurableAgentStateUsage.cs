// Copyright (c) Microsoft. All rights reserved.

using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Represents the token usage details for a durable agent state response.
/// </summary>
internal sealed class DurableAgentStateUsage
{
    /// <summary>
    /// Gets the number of input tokens used.
    /// </summary>
    [JsonPropertyName("inputTokenCount")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public long? InputTokenCount { get; init; }

    /// <summary>
    /// Gets the number of output tokens used.
    /// </summary>
    [JsonPropertyName("outputTokenCount")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public long? OutputTokenCount { get; init; }

    /// <summary>
    /// Gets the total number of tokens used.
    /// </summary>
    [JsonPropertyName("totalTokenCount")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public long? TotalTokenCount { get; init; }

    /// <summary>
    /// Gets provider-specific usage counts from the schema's <c>extensionData</c> property.
    /// </summary>
    [JsonPropertyName("extensionData")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IDictionary<string, JsonElement>? ExtensionData { get; init; }

    /// <summary>
    /// Gets unknown usage properties that are outside the declared schema.
    /// </summary>
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? UnknownProperties { get; set; }

    /// <summary>
    /// Creates a <see cref="DurableAgentStateUsage"/> from a <see cref="UsageDetails"/>.
    /// </summary>
    /// <param name="usage">The <see cref="UsageDetails"/> to convert.</param>
    /// <returns>A <see cref="DurableAgentStateUsage"/> representing the original usage details.</returns>
    [return: NotNullIfNotNull(nameof(usage))]
    public static DurableAgentStateUsage? FromUsage(UsageDetails? usage) =>
        usage is not null
            ? new()
            {
                InputTokenCount = usage.InputTokenCount,
                OutputTokenCount = usage.OutputTokenCount,
                TotalTokenCount = usage.TotalTokenCount,
                ExtensionData = usage.AdditionalCounts?.ToDictionary(
                    pair => pair.Key,
                    pair => JsonSerializer.SerializeToElement(
                        pair.Value,
                        DurableAgentStateJsonContext.Default.Int64)),
            }
            : null;

    /// <summary>
    /// Converts this <see cref="DurableAgentStateUsage"/> back to a <see cref="UsageDetails"/>.
    /// </summary>
    /// <returns>A <see cref="UsageDetails"/> representing this usage.</returns>
    public UsageDetails ToUsageDetails()
    {
        AdditionalPropertiesDictionary<long>? additionalCounts = null;
        foreach (IDictionary<string, JsonElement>? values in new[] { this.ExtensionData, this.UnknownProperties })
        {
            if (values is null)
            {
                continue;
            }

            foreach ((string name, JsonElement value) in values)
            {
                if (value.ValueKind == JsonValueKind.Number &&
                    value.TryGetInt64(out long count))
                {
                    additionalCounts ??= [];
                    additionalCounts[name] = count;
                }
            }
        }

        return new()
        {
            InputTokenCount = this.InputTokenCount,
            OutputTokenCount = this.OutputTokenCount,
            TotalTokenCount = this.TotalTokenCount,
            AdditionalCounts = additionalCounts,
        };
    }
}
