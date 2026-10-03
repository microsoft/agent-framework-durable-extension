// Copyright (c) Microsoft. All rights reserved.

using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Custom data converter for durable agents and workflows that ensures proper JSON serialization.
/// </summary>
/// <remarks>
/// <para>
/// This converter handles special cases like <see cref="DurableAgentState"/> using source-generated JSON contexts for
/// AOT compatibility, and falls back to reflection-based serialization for other types.
/// </para>
/// <para>
/// It also carries the lossless terminal result associated with an <see cref="AgentResponse"/> across the Durable Task
/// serialization boundary. The ordinary SDK response projection cannot hold every persisted field, while the
/// process-local sidecar association cannot cross processes by itself. A reserved envelope transports that exact
/// committed JSON without changing how unrelated serializers represent <see cref="AgentResponse"/>.
/// </para>
/// </remarks>
internal sealed class DurableDataConverter : DataConverter
{
    private const string ResponseEnvelopeProperty = "$microsoftAgentFrameworkDurableTask";
    private const string ResponseEnvelopeKind = "agentResponse";

    private static readonly JsonSerializerOptions s_options = new(DurableAgentJsonUtilities.DefaultOptions)
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
    };

    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Fallback uses reflection when metadata unavailable.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Fallback uses reflection when metadata unavailable.")]
    public override object? Deserialize(string? data, Type targetType)
    {
        if (data is null)
        {
            return null;
        }

        if (targetType == typeof(DurableAgentState))
        {
            return JsonSerializer.Deserialize(data, DurableAgentStateJsonContext.Default.DurableAgentState);
        }

        // The entity-side response object no longer exists after serialization. Read its retained result from the
        // framework envelope, create the orchestration-side response, and establish the same sidecar association on
        // that new object. Otherwise GetDurableResult() would be lost at exactly the process boundary it must survive.
        (JsonElement? RetainedResult, DurableAgentFailureData? Failure) durableMetadata =
            typeof(AgentResponse).IsAssignableFrom(targetType)
                ? ReadDurableResponseMetadata(data)
                : default;
        JsonTypeInfo? typeInfo = s_options.GetTypeInfo(targetType);
        object? deserialized = typeInfo is not null
            ? JsonSerializer.Deserialize(data, typeInfo)
            : JsonSerializer.Deserialize(data, targetType, s_options);
        if (durableMetadata.RetainedResult is JsonElement result && deserialized is AgentResponse response)
        {
            DurableAgentJsonUtilities.CaptureRetainedResult(response, result);
            if (durableMetadata.Failure is DurableAgentFailureData failure)
            {
                DurableAgentJsonUtilities.CaptureCommittedFailure(response, failure);
            }
        }

        return deserialized;
    }

    [return: NotNullIfNotNull(nameof(value))]
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Fallback uses reflection when metadata unavailable.")]
    [UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Fallback uses reflection when metadata unavailable.")]
    public override string? Serialize(object? value)
    {
        if (value is null)
        {
            return null;
        }

        if (value is DurableAgentState durableAgentState)
        {
            return JsonSerializer.Serialize(durableAgentState, DurableAgentStateJsonContext.Default.DurableAgentState);
        }

        JsonTypeInfo? typeInfo = s_options.GetTypeInfo(value.GetType());
        if (value is AgentResponse response &&
            DurableAgentJsonUtilities.GetRetainedResult(response) is JsonElement result)
        {
            // The native response is a lossy view of the entity's committed terminal result. Send both: the normal SDK
            // response for compatibility and the canonical result for lossless duplicate delivery. The private envelope
            // is added only by this Durable Task converter, so ordinary AgentResponse serialization remains unchanged.
            JsonElement native = typeInfo is not null
                ? JsonSerializer.SerializeToElement(value, typeInfo)
                : JsonSerializer.SerializeToElement(value, value.GetType(), s_options);
            return WriteResponseEnvelope(
                native,
                result,
                DurableAgentJsonUtilities.GetCommittedFailure(response));
        }

        return typeInfo is not null
            ? JsonSerializer.Serialize(value, typeInfo)
            : JsonSerializer.Serialize(value, s_options);
    }

    /// <summary>
    /// Adds framework-owned delivery metadata to the native response sent over the Durable Task boundary.
    /// </summary>
    /// <remarks>
    /// The streaming writer is intentional. The native response can contain arbitrary provider-defined or future
    /// properties that this package does not understand. Copying each serialized property with
    /// <see cref="JsonElement.WriteTo(Utf8JsonWriter)"/> preserves those JSON values without translating the response
    /// through a framework-owned DTO or materializing the entire response as a mutable
    /// <see cref="System.Text.Json.Nodes.JsonObject"/>. It also lets this method inspect every top-level property for the
    /// reserved key before appending the durable envelope in the same JSON object.
    ///
    /// For example, a native response shaped like:
    /// <code>
    /// {
    ///   "messages": [ ... ]
    /// }
    /// </code>
    /// is written across the Durable Task boundary as:
    /// <code>
    /// {
    ///   "messages": [ ... ],
    ///   "$microsoftAgentFrameworkDurableTask": {
    ///     "kind": "agentResponse",
    ///     "version": 1,
    ///     "result": {
    ///       "messages": [ ... ],
    ///       "value": null
    ///     }
    ///   }
    /// }
    /// </code>
    /// The original response properties remain at the root for normal <see cref="AgentResponse"/> deserialization. The
    /// reserved property carries the separate, lossless result used by durable delivery.
    ///
    /// A collision with the reserved property fails rather than overwriting either value. Accepting a collision could
    /// let provider/application response data masquerade as the framework's canonical committed result.
    /// </remarks>
    private static string WriteResponseEnvelope(
        JsonElement nativeResponse,
        JsonElement result,
        DurableAgentFailureData? failure)
    {
        using MemoryStream stream = new();
        using (Utf8JsonWriter writer = new(stream))
        {
            writer.WriteStartObject();
            foreach (JsonProperty property in nativeResponse.EnumerateObject())
            {
                if (string.Equals(property.Name, ResponseEnvelopeProperty, StringComparison.OrdinalIgnoreCase))
                {
                    throw new JsonException("The native response conflicts with reserved durable response metadata.");
                }

                property.WriteTo(writer);
            }

            writer.WritePropertyName(ResponseEnvelopeProperty);
            writer.WriteStartObject();
            writer.WriteString("kind", ResponseEnvelopeKind);
            writer.WriteNumber("version", failure is null ? 1 : 2);
            writer.WritePropertyName("result");
            result.WriteTo(writer);
            if (failure is not null)
            {
                writer.WritePropertyName("failure");
                JsonSerializer.Serialize(
                    writer,
                    failure,
                    DurableAgentJsonUtilities.JsonContext.Default.DurableAgentFailureData);
            }

            writer.WriteEndObject();
            writer.WriteEndObject();
        }

        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }

    /// <summary>
    /// Reads and validates the framework delivery envelope without interpreting ordinary response JSON as durable data.
    /// </summary>
    /// <remarks>
    /// Responses without an envelope remain compatible and simply have no retained result. If the reserved envelope is
    /// present, however, malformed, duplicate, or unsupported fields fail closed so untrusted or corrupted metadata
    /// cannot be exposed as the entity's committed outcome.
    /// </remarks>
    private static (JsonElement? RetainedResult, DurableAgentFailureData? Failure)
        ReadDurableResponseMetadata(string data)
    {
        using JsonDocument document = JsonDocument.Parse(data);
        JsonElement root = document.RootElement;
        DurableAgentStateJsonConverter.ValidateNoDuplicateRecognizedProperties(
            root, "durable response root", StringComparer.OrdinalIgnoreCase, ResponseEnvelopeProperty);
        if (root.ValueKind != JsonValueKind.Object ||
            !TryGetResponseMetadataProperty(root, ResponseEnvelopeProperty, out JsonElement envelope))
        {
            return default;
        }

        DurableAgentStateJsonConverter.ValidateNoDuplicateRecognizedProperties(
            envelope, "durable response envelope", StringComparer.OrdinalIgnoreCase,
            "kind", "version", "result", "failure");
        if (envelope.ValueKind != JsonValueKind.Object ||
            !TryGetResponseMetadataProperty(envelope, "kind", out JsonElement kind) ||
            kind.ValueKind != JsonValueKind.String || kind.GetString() != ResponseEnvelopeKind ||
            !TryGetResponseMetadataProperty(envelope, "version", out JsonElement version) ||
            version.ValueKind != JsonValueKind.Number || !version.TryGetInt32(out int versionNumber) ||
            versionNumber is not 1 and not 2 ||
            !TryGetResponseMetadataProperty(envelope, "result", out JsonElement result) || result.ValueKind != JsonValueKind.Object ||
            !result.TryGetProperty("messages", out JsonElement messages) || messages.ValueKind != JsonValueKind.Array)
        {
            throw new JsonException("The durable response metadata envelope is malformed or unsupported.");
        }

        DurableAgentFailureData? failure = null;
        bool hasFailure = TryGetResponseMetadataProperty(envelope, "failure", out JsonElement failureElement);
        if (versionNumber == 1 && hasFailure ||
            versionNumber == 2 && !hasFailure)
        {
            throw new JsonException("The durable response metadata envelope is malformed or unsupported.");
        }

        if (hasFailure)
        {
            DurableAgentStateJsonConverter.ValidateNoDuplicateRecognizedProperties(
                failureElement, "durable response failure", StringComparer.OrdinalIgnoreCase,
                DurableAgentJsonUtilities.JsonContext.Default.DurableAgentFailureData.Properties
                    .Select(property => property.Name).ToArray());
        }

        DurableAgentStateTerminalResponse terminalResponse = result.Deserialize(
            DurableAgentStateJsonContext.Default.DurableAgentStateTerminalResponse)
            ?? throw new JsonException("The durable response result is missing.");
        terminalResponse.Validate();

        if (hasFailure)
        {
            failure = failureElement.Deserialize(
                DurableAgentJsonUtilities.JsonContext.Default.DurableAgentFailureData);
            if (failure is null ||
                failure.Version != 1 ||
                string.IsNullOrWhiteSpace(failure.CorrelationId) ||
                string.IsNullOrWhiteSpace(failure.Code) ||
                string.IsNullOrWhiteSpace(failure.Message) ||
                failure.Outcome != DurableAgentStateCompletionReceipt.FailedOutcome ||
                failure.CompletedAt is not DateTimeOffset completedAt ||
                completedAt == default ||
                failure.ResultExpiresAt < completedAt ||
                failure.SerializedResponse is not null)
            {
                throw new JsonException("The durable response failure metadata is malformed or unsupported.");
            }

            try
            {
                DurableAgentStateContract.ValidateIdentifier(
                    failure.CorrelationId,
                    nameof(DurableAgentFailureData.CorrelationId));
                new DurableAgentStateTerminalError
                {
                    Code = failure.Code,
                    Message = failure.Message,
                    Details = failure.Details,
                }.Validate();
            }
            catch (InvalidOperationException exception)
            {
                throw new JsonException(
                    "The durable response failure metadata is malformed or unsupported.",
                    exception);
            }
        }

        return (result.Clone(), failure);
    }

    private static bool TryGetResponseMetadataProperty(JsonElement element, string name, out JsonElement value)
    {
        foreach (JsonProperty property in element.EnumerateObject())
        {
            if (string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                value = property.Value;
                return true;
            }
        }

        value = default;
        return false;
    }
}
