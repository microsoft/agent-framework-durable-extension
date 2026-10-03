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
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public JsonElement InputTokenCount
    {
        get;
        init => field = ValidateCount(value, "inputTokenCount");
    }

    /// <summary>
    /// Gets the number of output tokens used.
    /// </summary>
    [JsonPropertyName("outputTokenCount")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public JsonElement OutputTokenCount
    {
        get;
        init => field = ValidateCount(value, "outputTokenCount");
    }

    /// <summary>
    /// Gets the total number of tokens used.
    /// </summary>
    [JsonPropertyName("totalTokenCount")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public JsonElement TotalTokenCount
    {
        get;
        init => field = ValidateCount(value, "totalTokenCount");
    }

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
                InputTokenCount = ToJsonElement(usage.InputTokenCount),
                OutputTokenCount = ToJsonElement(usage.OutputTokenCount),
                TotalTokenCount = ToJsonElement(usage.TotalTokenCount),
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
                if (DurableAgentStateContract.TryGetInt64(value, out long count))
                {
                    additionalCounts ??= [];
                    additionalCounts[name] = count;
                }
            }
        }

        return new()
        {
            InputTokenCount = ToInt64(this.InputTokenCount),
            OutputTokenCount = ToInt64(this.OutputTokenCount),
            TotalTokenCount = ToInt64(this.TotalTokenCount),
            AdditionalCounts = additionalCounts,
        };
    }

    private static JsonElement ToJsonElement(long? value) => value.HasValue
        ? JsonSerializer.SerializeToElement(value.Value, DurableAgentStateJsonContext.Default.Int64)
        : default;

    private static long? ToInt64(JsonElement value) =>
        DurableAgentStateContract.TryGetInt64(value, out long count) ? count : null;

    private static JsonElement ValidateCount(JsonElement value, string propertyName)
    {
        if (value.ValueKind != JsonValueKind.Undefined &&
            !DurableAgentStateContract.IsNonNegativeInt64(value))
        {
            throw new JsonException(
                $"The durable agent usage '{propertyName}' property must be a non-negative Int64 value.");
        }

        return value.ValueKind == JsonValueKind.Undefined ? default : value.Clone();
    }
}
