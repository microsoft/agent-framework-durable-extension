// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// JSON converter for <see cref="DurableAgentState"/> which performs schema version checks before deserialization.
/// </summary>
internal sealed class DurableAgentStateJsonConverter : JsonConverter<DurableAgentState>
{
    private const string SchemaVersionPropertyName = "schemaVersion";
    private const string DataPropertyName = "data";
    private const string ExtensionDataPropertyName = "extensionData";

    /// <inheritdoc/>
    public override DurableAgentState? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        JsonElement? element = JsonSerializer.Deserialize(
            ref reader,
            DurableAgentStateJsonContext.Default.JsonElement);

        if (element is null)
        {
            throw new JsonException("The durable agent state is not valid JSON.");
        }

        if (!element.Value.TryGetProperty(SchemaVersionPropertyName, out JsonElement versionElement))
        {
            throw new InvalidOperationException("The durable agent state is missing the 'schemaVersion' property.");
        }

        string? schemaVersionText = versionElement.ValueKind == JsonValueKind.String
            ? versionElement.GetString()
            : null;
        _ = DurableAgentStateSchemaVersion.ParseSupported(schemaVersionText);

        if (!element.Value.TryGetProperty(DataPropertyName, out JsonElement dataElement))
        {
            throw new InvalidOperationException("The durable agent state is missing the 'data' property.");
        }

        DurableAgentStateData? data = dataElement.Deserialize(
            DurableAgentStateJsonContext.Default.DurableAgentStateData);
        DurableAgentStateSchemaVersion schemaVersion =
            DurableAgentStateSchemaVersion.ParseSupported(schemaVersionText);
        if (schemaVersion.Major == DurableAgentState.RevisedSchemaMajorVersion)
        {
            ValidateRevisedLayout(dataElement);
        }

        (data ??= new DurableAgentStateData()).Validate(schemaVersionText!);
        Dictionary<string, JsonElement>? extensionData =
            element.Value.TryGetProperty(ExtensionDataPropertyName, out JsonElement extensionDataElement)
                ? ReadExtensionData(extensionDataElement)
                : null;
        Dictionary<string, JsonElement>? unknownProperties = null;
        foreach (JsonProperty property in element.Value.EnumerateObject())
        {
            if (property.NameEquals(SchemaVersionPropertyName) ||
                property.NameEquals(DataPropertyName) ||
                property.NameEquals(ExtensionDataPropertyName))
            {
                continue;
            }

            unknownProperties ??= [];
            unknownProperties[property.Name] = property.Value.Clone();
        }

        return new DurableAgentState
        {
            SchemaVersion = schemaVersionText!,
            Data = data,
            ExtensionData = extensionData,
            UnknownProperties = unknownProperties,
        };
    }

    /// <inheritdoc/>
    public override void Write(Utf8JsonWriter writer, DurableAgentState value, JsonSerializerOptions options)
    {
        _ = DurableAgentStateSchemaVersion.ParseSupported(value.SchemaVersion);
        value.Data.Validate(value.SchemaVersion);

        writer.WriteStartObject();
        writer.WritePropertyName(SchemaVersionPropertyName);
        writer.WriteStringValue(value.SchemaVersion);
        writer.WritePropertyName(DataPropertyName);
        JsonSerializer.Serialize(
            writer,
            value.Data,
            DurableAgentStateJsonContext.Default.DurableAgentStateData);
        if (value.ExtensionData is not null)
        {
            writer.WritePropertyName(ExtensionDataPropertyName);
            WriteExtensionData(writer, value.ExtensionData);
        }

        if (value.UnknownProperties is not null)
        {
            foreach ((string propertyName, JsonElement propertyValue) in value.UnknownProperties)
            {
                if (propertyName is not SchemaVersionPropertyName and
                    not DataPropertyName and
                    not ExtensionDataPropertyName)
                {
                    writer.WritePropertyName(propertyName);
                    propertyValue.WriteTo(writer);
                }
            }
        }

        writer.WriteEndObject();
    }

    private static Dictionary<string, JsonElement>? ReadExtensionData(JsonElement element)
    {
        if (element.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        if (element.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException("The durable agent state 'extensionData' property must be an object.");
        }

        return element.EnumerateObject().ToDictionary(
            property => property.Name,
            property => property.Value.Clone());
    }

    private static void WriteExtensionData(
        Utf8JsonWriter writer,
        IDictionary<string, JsonElement> extensionData)
    {
        writer.WriteStartObject();
        foreach ((string propertyName, JsonElement propertyValue) in extensionData)
        {
            writer.WritePropertyName(propertyName);
            propertyValue.WriteTo(writer);
        }

        writer.WriteEndObject();
    }

    private static void ValidateRevisedLayout(JsonElement dataElement)
    {
        if (dataElement.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException("The revised durable agent state 'data' property must be an object.");
        }

        foreach (string requiredProperty in new[]
        {
            "conversationHistory",
            "terminalResults",
            "completionReceipts",
            "historyBinding",
        })
        {
            if (!dataElement.TryGetProperty(requiredProperty, out _))
            {
                throw new InvalidOperationException(
                    $"The revised durable agent state is missing the 'data.{requiredProperty}' property.");
            }
        }

        ValidateUniqueObjectKeys(dataElement.GetProperty("terminalResults"), "terminalResults");
        ValidateUniqueObjectKeys(dataElement.GetProperty("completionReceipts"), "completionReceipts");
        ValidateTerminalMessages(dataElement.GetProperty("terminalResults"));
    }

    private static void ValidateUniqueObjectKeys(JsonElement element, string propertyName)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException($"The revised durable agent state 'data.{propertyName}' property must be an object.");
        }

        HashSet<string> keys = new(StringComparer.Ordinal);
        foreach (JsonProperty property in element.EnumerateObject())
        {
            if (!keys.Add(property.Name))
            {
                throw new InvalidOperationException(
                    $"The revised durable agent state 'data.{propertyName}' property contains duplicate correlation ID '{property.Name}'.");
            }
        }
    }

    private static void ValidateTerminalMessages(JsonElement terminalResults)
    {
        foreach (JsonProperty result in terminalResults.EnumerateObject())
        {
            if (!result.Value.TryGetProperty("response", out JsonElement response) ||
                !response.TryGetProperty("messages", out JsonElement messages))
            {
                throw new InvalidOperationException(
                    $"Durable agent terminal result '{result.Name}' requires a response messages collection.");
            }

            if (messages.ValueKind != JsonValueKind.Array)
            {
                throw new InvalidOperationException(
                    $"Durable agent terminal result '{result.Name}' contains a non-array response messages property.");
            }

            foreach (JsonElement message in messages.EnumerateArray())
            {
                if (message.ValueKind != JsonValueKind.Object)
                {
                    throw new InvalidOperationException(
                        $"Durable agent terminal result '{result.Name}' contains a non-object message.");
                }

                if (message.TryGetProperty("contents", out JsonElement contents) &&
                    contents.ValueKind != JsonValueKind.Array)
                {
                    throw new InvalidOperationException(
                        $"Durable agent terminal result '{result.Name}' contains a non-array message contents property.");
                }
            }
        }
    }
}
