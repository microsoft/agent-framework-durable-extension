// Copyright (c) Microsoft. All rights reserved.

using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Numerics;
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
                if (TryGetInt64(value, out long count))
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

    private static long? ToInt64(JsonElement value) => TryGetInt64(value, out long count) ? count : null;

    private static bool TryGetInt64(JsonElement value, out long count)
    {
        count = default;
        return value.ValueKind == JsonValueKind.Number &&
            decimal.TryParse(
                value.GetRawText(),
                NumberStyles.Float,
                CultureInfo.InvariantCulture,
                out decimal decimalCount) &&
            decimalCount == decimal.Truncate(decimalCount) &&
            decimalCount >= long.MinValue &&
            decimalCount <= long.MaxValue &&
            (count = decimal.ToInt64(decimalCount)) == decimalCount;
    }

    private static JsonElement ValidateCount(JsonElement value, string propertyName)
    {
        if (value.ValueKind != JsonValueKind.Undefined &&
            (value.ValueKind != JsonValueKind.Number || !IsJsonInteger(value.GetRawText())))
        {
            throw new JsonException($"The durable agent usage '{propertyName}' property must be an integer.");
        }

        return value;
    }

    internal static bool IsJsonInteger(string value)
    {
        int exponentIndex = value.IndexOfAny('e', 'E');
        ReadOnlySpan<char> significand = exponentIndex >= 0 ? value.AsSpan(0, exponentIndex) : value;
        ReadOnlySpan<char> exponentText = exponentIndex >= 0 ? value.AsSpan(exponentIndex + 1) : default;
        int decimalIndex = significand.IndexOf('.');
        int fractionalDigitCount = decimalIndex >= 0 ? significand.Length - decimalIndex - 1 : 0;
        int trailingZeroCount = 0;
        bool hasNonZeroDigit = false;
        for (int index = significand.Length - 1; index >= 0; index--)
        {
            char character = significand[index];
            if (character is '.' or '-')
            {
                continue;
            }

            if (character == '0' && !hasNonZeroDigit)
            {
                trailingZeroCount++;
            }
            else
            {
                hasNonZeroDigit = true;
            }
        }

        if (!hasNonZeroDigit)
        {
            return true;
        }

        BigInteger exponent = exponentText.IsEmpty
            ? BigInteger.Zero
            : BigInteger.Parse(exponentText, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
        return exponent >= fractionalDigitCount - trailingZeroCount;
    }
}
