// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Logging;

namespace Microsoft.Agents.AI.DurableTask.State;

#pragma warning disable MEAI001 // ResponseContinuationToken is part of the AgentResponse contract captured here.

/// <summary>
/// Immutable, JSON-safe projection of the fields consumed from an <see cref="AgentResponse"/>.
/// </summary>
internal sealed class DurableAgentStateTerminalResponse
{
    [JsonPropertyName("messages")]
    public IReadOnlyList<DurableAgentStateMessage> Messages
    {
        get;
        init => field = value ?? [];
    } = [];

    [JsonPropertyName("usage")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DurableAgentStateUsage? Usage { get; init; }

    [JsonPropertyName("createdAt")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTimeOffset? CreatedAt { get; init; }

    /// <summary>
    /// Gets an optional caller-visible JSON result independent of the response messages.
    /// </summary>
    /// <remarks>
    /// <see cref="JsonValueKind.Undefined"/> means the wire property was absent. All other JSON values,
    /// including explicit null, false, zero, empty strings, arrays, and objects, are present values.
    /// </remarks>
    [JsonPropertyName("value")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public JsonElement Value
    {
        get;
        init => field = value.ValueKind == JsonValueKind.Undefined ? default : value.Clone();
    }

    [JsonPropertyName("responseId")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? ResponseId { get; init; }

    [JsonPropertyName("agentId")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? AgentId { get; init; }

    [JsonPropertyName("finishReason")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? FinishReason { get; init; }

    [JsonPropertyName("continuationToken")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? ContinuationToken { get; init; }

    /// <summary>
    /// Gets JSON-safe producer-defined values from the declared wire-level <c>extensionData</c> field.
    /// </summary>
    /// <remarks>
    /// The CLR name mirrors <see cref="AgentResponse.AdditionalProperties"/>. Keeping that name makes the
    /// projection boundary explicit while <see cref="JsonPropertyNameAttribute"/> preserves the shared
    /// schema name. This field is not the same as <see cref="UnknownProperties"/>, which contains
    /// undeclared future JSON members.
    /// </remarks>
    [JsonPropertyName("extensionData")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IDictionary<string, JsonElement>? AdditionalProperties { get; init; }

    [JsonExtensionData]
    public IDictionary<string, JsonElement>? UnknownProperties { get; set; }

    public static DurableAgentStateTerminalResponse FromResponse(
        AgentResponse response,
        string correlationId,
        DateTimeOffset completedAt,
        JsonElement structuredValue = default,
        ILogger? logger = null)
    {
        Dictionary<string, JsonElement>? additionalProperties = null;
        if (response.AdditionalProperties is not null)
        {
            foreach ((string key, object? value) in response.AdditionalProperties)
            {
                additionalProperties ??= [];
                additionalProperties[key] = ConvertMetadata(value, key);
            }
        }

        return new()
        {
            Messages = response.Messages
                .Select((message, index) => DurableAgentStateMessage.FromTerminalChatMessage(
                    message,
                    DurableAgentStateMessageIdentity.Create("result", correlationId, completedAt, index),
                    logger))
                .ToList(),
            Usage = DurableAgentStateUsage.FromUsage(response.Usage),
            CreatedAt = response.CreatedAt,
            Value = structuredValue,
            ResponseId = response.ResponseId,
            AgentId = response.AgentId,
            FinishReason = response.FinishReason?.Value,
            ContinuationToken = response.ContinuationToken is null
                ? null
                : Convert.ToBase64String(response.ContinuationToken.ToBytes().Span),
            AdditionalProperties = additionalProperties,
        };
    }

    public AgentResponse ToResponse() => this.ToResponse(static message => message.ToChatMessage());

    internal AgentResponse ToResponse(Func<DurableAgentStateMessage, ChatMessage> messageConverter)
    {
        AdditionalPropertiesDictionary? additionalProperties = this.AdditionalProperties is null
            ? null
            : new(this.AdditionalProperties.Select(pair =>
                new KeyValuePair<string, object?>(pair.Key, pair.Value)));

        return new AgentResponse
        {
            Messages = this.Messages.Select(messageConverter).ToList(),
            Usage = this.Usage?.ToUsageDetails(),
            CreatedAt = this.CreatedAt,
            ResponseId = this.ResponseId,
            AgentId = this.AgentId,
            FinishReason = this.FinishReason is null ? null : new ChatFinishReason(this.FinishReason),
            ContinuationToken = this.ContinuationToken is null
                ? null
                : ResponseContinuationToken.FromBytes(Convert.FromBase64String(this.ContinuationToken)),
            AdditionalProperties = additionalProperties,
        };
    }

    public void Validate()
    {
        if (this.Messages is null)
        {
            throw new InvalidOperationException(
                "A durable agent terminal response requires a messages collection.");
        }

        foreach (DurableAgentStateMessage? message in this.Messages)
        {
            if (message is null || message.Contents is null)
            {
                throw new InvalidOperationException(
                    "A durable agent terminal response cannot contain null messages or content collections.");
            }

            message.ValidateV2();
        }

        ValidateOptionalIdentifier(this.ResponseId, "terminalResults.response.responseId");
        ValidateOptionalIdentifier(this.AgentId, "terminalResults.response.agentId");
        ValidateOptionalIdentifier(this.FinishReason, "terminalResults.response.finishReason");

        if (this.ContinuationToken is not null)
        {
            if (this.ContinuationToken.Length > DurableAgentStateContract.MaxMetadataStringLength)
            {
                throw new InvalidOperationException(
                    "The durable agent terminal response continuation token is too large.");
            }

            try
            {
                byte[] decoded = Convert.FromBase64String(this.ContinuationToken);
                if (!string.Equals(
                    this.ContinuationToken,
                    Convert.ToBase64String(decoded),
                    StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        "The durable agent terminal response continuation token must use canonical base64 encoding.");
                }
            }
            catch (FormatException exception)
            {
                throw new InvalidOperationException(
                    "The durable agent terminal response continuation token must be base64 encoded.",
                    exception);
            }
        }

        if (this.AdditionalProperties is not null)
        {
            foreach (string key in this.AdditionalProperties.Keys)
            {
                DurableAgentStateContract.ValidateIdentifier(key, "terminalResults.response.extensionData key");
            }
        }
    }

    private static void ValidateOptionalIdentifier(string? value, string propertyName)
    {
        if (value is not null)
        {
            DurableAgentStateContract.ValidateIdentifier(value, propertyName);
        }
    }

    internal static JsonElement ConvertMetadata(object? value, string propertyName)
    {
        switch (value)
        {
            case null:
                return JsonSerializer.SerializeToElement(
                    value,
                    DurableAgentStateJsonContext.Default.Object);
            case JsonElement jsonElement:
                return jsonElement.Clone();
            case string text when text.Length <= DurableAgentStateContract.MaxMetadataStringLength:
                return JsonSerializer.SerializeToElement(
                    text,
                    DurableAgentStateJsonContext.Default.String);
            case bool boolean:
                return JsonSerializer.SerializeToElement(
                    boolean,
                    DurableAgentStateJsonContext.Default.Boolean);
            case int integer:
                return JsonSerializer.SerializeToElement(
                    integer,
                    DurableAgentStateJsonContext.Default.Int32);
            case long longInteger:
                return JsonSerializer.SerializeToElement(
                    longInteger,
                    DurableAgentStateJsonContext.Default.Int64);
            case double doubleValue when double.IsFinite(doubleValue):
                return JsonSerializer.SerializeToElement(
                    doubleValue,
                    DurableAgentStateJsonContext.Default.Double);
            case decimal decimalValue:
                return JsonSerializer.SerializeToElement(
                    decimalValue,
                    DurableAgentStateJsonContext.Default.Decimal);
            case DateTimeOffset dateTimeOffset:
                return JsonSerializer.SerializeToElement(
                    dateTimeOffset,
                    DurableAgentStateJsonContext.Default.DateTimeOffset);
            default:
                throw new InvalidOperationException(
                    $"The AgentResponse metadata property '{propertyName}' has unsupported runtime type '{value?.GetType()}'.");
        }
    }

#pragma warning restore MEAI001
}
