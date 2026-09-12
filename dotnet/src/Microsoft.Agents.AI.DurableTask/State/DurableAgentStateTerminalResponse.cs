// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Logging;

namespace Microsoft.Agents.AI.DurableTask.State;

#pragma warning disable MEAI001 // ResponseContinuationToken is part of the AgentResponse contract captured here.

/// <summary>
/// Stores a JSON-safe snapshot of the final caller-visible
/// <see cref="AgentResponse"/> for durable result delivery.
/// </summary>
/// <remarks>
/// This response is part of the durable result mailbox, not the model transcript.
/// It allows polling and duplicate requests to return the original completed
/// result even when conversation history has been compacted or removed.
///
/// The snapshot contains only data that can be safely persisted as JSON. It does
/// not serialize the original runtime object graph, exceptions, services, or
/// executable .NET types.
/// </remarks>
internal sealed class DurableAgentStateTerminalResponse
{
    /// <summary>
    /// Gets the messages in the final aggregate agent response.
    /// </summary>
    /// <remarks>
    /// These are the caller-visible response messages produced by the completed
    /// outer invocation. They must not include a second copy of previously loaded
    /// history or intermediate responses produced during a model/tool loop.
    ///
    /// The collection may be empty but is never null.
    /// </remarks>
    [JsonPropertyName("messages")]
    public IReadOnlyList<DurableAgentStateMessage> Messages
    {
        get;
        init => field = value ?? [];
    } = [];

    /// <summary>
    /// Gets the usage information reported for the completed response, when
    /// provided by the model or agent implementation.
    /// </summary>
    /// <remarks>
    /// This preserves caller-visible usage metadata such as token counts. It is not
    /// used as durable completion evidence and may be absent when the underlying
    /// provider does not report usage.
    /// </remarks>
    [JsonPropertyName("usage")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DurableAgentStateUsage? Usage { get; init; }

    /// <summary>
    /// Gets the creation time reported by the original agent response.
    /// </summary>
    /// <remarks>
    /// This is response metadata supplied by the agent or provider. It is distinct
    /// from the durable completion time recorded by the terminal result and
    /// completion receipt.
    /// </remarks>
    [JsonPropertyName("createdAt")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTimeOffset? CreatedAt { get; init; }

    /// <summary>
    /// Gets the optional caller-visible structured result independently of the
    /// response messages.
    /// </summary>
    /// <remarks>
    /// Some agent APIs return a structured value in addition to messages or text.
    /// Keeping it as a separate JSON value preserves that contract without
    /// serializing model classes or arbitrary runtime objects.
    ///
    /// <see cref="JsonValueKind.Undefined"/> means the wire property was absent.
    /// Every other JSON value, including explicit null, false, zero, an empty
    /// string, an empty array, or an empty object, represents a present result.
    /// </remarks>
    [JsonPropertyName("value")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public JsonElement Value
    {
        get;
        init => field = value.ValueKind == JsonValueKind.Undefined ? default : value.Clone();
    }

    /// <summary>
    /// Gets the identifier assigned to the original response by the agent or model
    /// provider, when available.
    /// </summary>
    /// <remarks>
    /// This is provider response metadata. It is not the durable request correlation
    /// identifier and is not used for duplicate suppression.
    /// </remarks>
    [JsonPropertyName("responseId")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? ResponseId { get; init; }

    /// <summary>
    /// Gets the agent identifier reported by the original response, when available.
    /// </summary>
    /// <remarks>
    /// This preserves caller-visible response metadata. It does not identify the
    /// durable entity or determine which history provider owns the session.
    /// </remarks>
    [JsonPropertyName("agentId")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? AgentId { get; init; }

    /// <summary>
    /// Gets the model or agent finish reason reported by the original response,
    /// when available.
    /// </summary>
    /// <remarks>
    /// This value describes why response generation ended. It is not a workflow
    /// halt instruction and must never be interpreted as durable control state.
    /// </remarks>
    [JsonPropertyName("finishReason")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? FinishReason { get; init; }

    /// <summary>
    /// Gets the original response continuation token encoded as canonical base64,
    /// when one was returned.
    /// </summary>
    /// <remarks>
    /// The token is opaque provider data preserved for caller-visible response
    /// fidelity. Persisting it does not guarantee that another provider or runtime
    /// can interpret or resume it.
    ///
    /// This is distinct from the serialized <c>AgentSession</c> continuation state
    /// used to restore the durable agent session.
    /// </remarks>
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

    /// <summary>
    /// Gets undeclared JSON properties preserved for forward compatibility.
    /// </summary>
    /// <remarks>
    /// The current runtime does not interpret these properties. It retains them so
    /// reading and rewriting state does not discard fields written by a newer
    /// compatible runtime.
    ///
    /// These values must not be treated as trusted control data or used to activate
    /// runtime types.
    /// </remarks>
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
        => this.Validate(DurableAgentStateSchemaVersion.ParseSupported(DurableAgentState.RevisedSchemaVersion));

    internal void Validate(DurableAgentStateSchemaVersion version)
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

            message.Validate(version);
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
