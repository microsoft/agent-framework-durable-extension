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
/// This converter handles special cases like <see cref="DurableAgentState"/> using source-generated
/// JSON contexts for AOT compatibility, and falls back to reflection-based serialization for other types.
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

        JsonElement? retainedResult = typeof(AgentResponse).IsAssignableFrom(targetType)
            ? ReadRetainedResult(data)
            : null;
        JsonTypeInfo? typeInfo = s_options.GetTypeInfo(targetType);
        object? deserialized = typeInfo is not null
            ? JsonSerializer.Deserialize(data, typeInfo)
            : JsonSerializer.Deserialize(data, targetType, s_options);
        if (retainedResult is JsonElement result && deserialized is AgentResponse response)
        {
            DurableAgentJsonUtilities.CaptureRetainedResult(response, result);
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
            JsonElement native = typeInfo is not null
                ? JsonSerializer.SerializeToElement(value, typeInfo)
                : JsonSerializer.SerializeToElement(value, value.GetType(), s_options);
            return WriteResponseEnvelope(native, result);
        }

        return typeInfo is not null
            ? JsonSerializer.Serialize(value, typeInfo)
            : JsonSerializer.Serialize(value, s_options);
    }

    private static string WriteResponseEnvelope(JsonElement nativeResponse, JsonElement result)
    {
        using MemoryStream stream = new();
        using (Utf8JsonWriter writer = new(stream))
        {
            writer.WriteStartObject();
            foreach (JsonProperty property in nativeResponse.EnumerateObject())
            {
                if (property.NameEquals(ResponseEnvelopeProperty))
                {
                    throw new JsonException("The native response conflicts with reserved durable response metadata.");
                }

                property.WriteTo(writer);
            }

            writer.WritePropertyName(ResponseEnvelopeProperty);
            writer.WriteStartObject();
            writer.WriteString("kind", ResponseEnvelopeKind);
            writer.WriteNumber("version", 1);
            writer.WritePropertyName("result");
            result.WriteTo(writer);
            writer.WriteEndObject();
            writer.WriteEndObject();
        }

        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }

    private static JsonElement? ReadRetainedResult(string data)
    {
        using JsonDocument document = JsonDocument.Parse(data);
        JsonElement root = document.RootElement;
        if (root.ValueKind != JsonValueKind.Object ||
            !root.TryGetProperty(ResponseEnvelopeProperty, out JsonElement envelope))
        {
            return null;
        }

        if (root.EnumerateObject().Count(property => property.NameEquals(ResponseEnvelopeProperty)) != 1 ||
            envelope.ValueKind != JsonValueKind.Object ||
            envelope.EnumerateObject().Count(property => property.NameEquals("kind")) != 1 ||
            envelope.EnumerateObject().Count(property => property.NameEquals("version")) != 1 ||
            envelope.EnumerateObject().Count(property => property.NameEquals("result")) != 1 ||
            !envelope.TryGetProperty("kind", out JsonElement kind) ||
            kind.ValueKind != JsonValueKind.String || kind.GetString() != ResponseEnvelopeKind ||
            !envelope.TryGetProperty("version", out JsonElement version) ||
            version.ValueKind != JsonValueKind.Number || !version.TryGetInt32(out int versionNumber) || versionNumber != 1 ||
            !envelope.TryGetProperty("result", out JsonElement result) || result.ValueKind != JsonValueKind.Object ||
            !result.TryGetProperty("messages", out JsonElement messages) || messages.ValueKind != JsonValueKind.Array)
        {
            throw new JsonException("The durable response metadata envelope is malformed or unsupported.");
        }

        DurableAgentStateTerminalResponse terminalResponse = result.Deserialize(
            DurableAgentStateJsonContext.Default.DurableAgentStateTerminalResponse)
            ?? throw new JsonException("The durable response result is missing.");
        terminalResponse.Validate();
        return result.Clone();
    }
}
