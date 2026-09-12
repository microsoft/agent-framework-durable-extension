// Copyright (c) Microsoft. All rights reserved.

using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// JSON converter for <see cref="DurableAgentState"/> which performs schema version checks before deserialization.
/// </summary>
internal sealed class DurableAgentStateJsonConverter : JsonConverter<DurableAgentState>
{
    private static readonly Regex s_rfc3339Pattern = new(
        @"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$",
        RegexOptions.CultureInvariant);

    private const string SchemaVersionPropertyName = "schemaVersion";
    private const string DataPropertyName = "data";
    private const string ExtensionDataPropertyName = "extensionData";

    /// <inheritdoc/>
    public override DurableAgentState? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        JsonElement? element = JsonSerializer.Deserialize(
            ref reader,
            DurableAgentStateJsonContext.Default.JsonElement);

        DurableAgentState? state = ReadElement(element, allowRevisedSchema: true);
        if (state?.SchemaVersion == DurableAgentState.RevisedSchemaVersion)
        {
            // This worker understands receipts. Preserve already revised state without a downgrade,
            // including when a read-only duplicate operation republishes the hydrated state.
            state.MailboxWritesAuthorized = true;
        }

        return state;
    }

    internal static DurableAgentState DeserializeRevisedContract(string json)
    {
        using JsonDocument document = JsonDocument.Parse(json);
        return ReadElement(document.RootElement.Clone(), allowRevisedSchema: true)
            ?? throw new JsonException("The durable agent state is not valid JSON.");
    }

    internal static string SerializeRevisedContract(DurableAgentState state)
    {
        using MemoryStream stream = new();
        using (Utf8JsonWriter writer = new(stream))
        {
            WriteValue(writer, state, allowRevisedSchema: true);
        }

        return System.Text.Encoding.UTF8.GetString(stream.ToArray());
    }

    private static DurableAgentState? ReadElement(JsonElement? element, bool allowRevisedSchema)
    {
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

        if (dataElement.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException("The durable agent state 'data' property must be an object.");
        }

        ValidateOpaqueSession(dataElement);
        ValidateDeclaredExtensionData(element.Value, dataElement);
        ValidateKnownFieldShapes(dataElement);
        DurableAgentStateSchemaVersion schemaVersion =
            DurableAgentStateSchemaVersion.ParseSupported(schemaVersionText);
        if (schemaVersion.Major == DurableAgentState.RevisedSchemaMajorVersion)
        {
            if (!allowRevisedSchema)
            {
                throw new InvalidOperationException(
                    "Durable agent state schema 2.0.0 requires mailbox-aware runtime activation.");
            }

            ValidateRevisedLayout(dataElement);
        }
        else
        {
            RejectLegacyRevisedFields(dataElement);
            ValidateLegacyTranscript(dataElement);
        }

        DurableAgentStateData? data = dataElement.Deserialize(
            DurableAgentStateJsonContext.Default.DurableAgentStateData);
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
        WriteValue(writer, value, allowRevisedSchema: value.MailboxWritesAuthorized);
    }

    private static void WriteValue(
        Utf8JsonWriter writer,
        DurableAgentState value,
        bool allowRevisedSchema)
    {
        _ = DurableAgentStateSchemaVersion.ParseSupported(value.SchemaVersion);
        if (value.SchemaVersion == DurableAgentState.RevisedSchemaVersion && !allowRevisedSchema)
        {
            throw new InvalidOperationException(
                "Durable agent state schema 2.0.0 requires mailbox-aware runtime activation.");
        }

        value.Data.Validate(value.SchemaVersion);

        JsonElement data = JsonSerializer.SerializeToElement(
            value.Data, DurableAgentStateJsonContext.Default.DurableAgentStateData);
        if (value.SchemaVersion != DurableAgentState.RevisedSchemaVersion)
        {
            // Apply the historical reader's shape checks before publishing any legacy JSON.
            // DTOs and extension properties must not bypass the legacy message adapters.
            RejectLegacyRevisedFields(data);
            ValidateLegacyTranscript(data);
        }

        writer.WriteStartObject();
        writer.WritePropertyName(SchemaVersionPropertyName);
        writer.WriteStringValue(value.SchemaVersion);
        writer.WritePropertyName(DataPropertyName);
        data.WriteTo(writer);
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
        ValidateIngestionAndTruncation(dataElement);
        ValidateTranscript(dataElement.GetProperty("conversationHistory"));
        ValidateTerminalMessages(dataElement.GetProperty("terminalResults"));
    }

    private static void ValidateOpaqueSession(JsonElement dataElement)
    {
        if (dataElement.ValueKind == JsonValueKind.Object &&
            dataElement.TryGetProperty("session", out JsonElement session) &&
            session.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException(
                "The durable agent state 'data.session' property must be a JSON object.");
        }
    }

    private static void RejectLegacyRevisedFields(JsonElement dataElement)
    {
        if (dataElement.ValueKind != JsonValueKind.Object)
        {
            return;
        }

        foreach (string propertyName in new[]
        {
            "terminalResults",
            "completionReceipts",
            "historyBinding",
        })
        {
            if (dataElement.TryGetProperty(propertyName, out _))
            {
                throw new InvalidOperationException(
                    $"The durable agent state 'data.{propertyName}' property requires schema version 2.0.0.");
            }
        }

        ValidateIngestionAndTruncation(dataElement);
    }

    private static void ValidateLegacyTranscript(JsonElement dataElement)
    {
        if (!dataElement.TryGetProperty("conversationHistory", out JsonElement history))
        {
            return;
        }

        if (history.ValueKind != JsonValueKind.Array)
        {
            throw new JsonException(
                "The legacy durable agent state 'data.conversationHistory' property must be an array.");
        }

        foreach (JsonElement entry in history.EnumerateArray())
        {
            if (entry.ValueKind != JsonValueKind.Object)
            {
                throw new InvalidOperationException(
                    "Legacy durable agent conversation history cannot contain non-object entries.");
            }

            if (!entry.TryGetProperty("messages", out JsonElement messages))
            {
                continue;
            }

            if (messages.ValueKind != JsonValueKind.Array)
            {
                throw new InvalidOperationException(
                    "Legacy durable agent entry messages must be an array when present.");
            }

            foreach (JsonElement message in messages.EnumerateArray())
            {
                if (message.ValueKind != JsonValueKind.Object)
                {
                    throw new InvalidOperationException(
                        "Legacy durable agent entry messages cannot contain non-object values.");
                }

                string? roleText =
                    message.TryGetProperty("role", out JsonElement role) &&
                    role.ValueKind == JsonValueKind.String
                        ? role.GetString()
                        : null;
                if (roleText is not ("user" or "assistant" or "system" or "tool"))
                {
                    throw new InvalidOperationException(
                        $"The legacy durable agent state message role '{roleText}' is not supported.");
                }

                if (!message.TryGetProperty("contents", out JsonElement contents))
                {
                    continue;
                }

                if (contents.ValueKind != JsonValueKind.Array)
                {
                    throw new InvalidOperationException(
                        "Legacy durable agent message contents must be an array when present.");
                }

                foreach (JsonElement content in contents.EnumerateArray())
                {
                    if (content.ValueKind != JsonValueKind.Object ||
                        !content.TryGetProperty("$type", out JsonElement contentType) ||
                        contentType.ValueKind != JsonValueKind.String)
                    {
                        throw new InvalidOperationException(
                            "Legacy durable agent message contents require object values with string discriminators.");
                    }

                    if (contentType.ValueEquals("functionCall") &&
                        content.TryGetProperty("arguments", out JsonElement arguments) &&
                        arguments.ValueKind != JsonValueKind.Object)
                    {
                        throw new InvalidOperationException(
                            "Legacy durable agent function-call arguments must be an object when present.");
                    }

                    if (contentType.ValueEquals("uri") &&
                        (!content.TryGetProperty("mediaType", out JsonElement mediaType) ||
                         mediaType.ValueKind != JsonValueKind.String))
                    {
                        throw new InvalidOperationException(
                            "Legacy durable agent URI content requires a string mediaType.");
                    }

                    if (contentType.ValueEquals("usage") &&
                        content.TryGetProperty("usage", out JsonElement contentUsage))
                    {
                        if (contentUsage.ValueKind != JsonValueKind.Object)
                        {
                            throw new InvalidOperationException(
                                "Legacy durable agent usage content requires an object-valued usage property.");
                        }

                        ValidateUsageObject(contentUsage, "message.contents.usage");
                    }

                    ValidateKnownContentFields(content, contentType.GetString()!);
                }
            }
        }
    }

    private static void ValidateIngestionAndTruncation(JsonElement dataElement)
    {
        if (dataElement.TryGetProperty("ingestedPositions", out JsonElement ingestedPositions))
        {
            if (ingestedPositions.ValueKind != JsonValueKind.Object)
            {
                throw new JsonException(
                    "The durable agent state 'data.ingestedPositions' property must be an object.");
            }

            foreach (JsonProperty position in ingestedPositions.EnumerateObject())
            {
                if (position.Value.ValueKind != JsonValueKind.Number ||
                    !position.Value.TryGetInt32(out int value) ||
                    value < 0)
                {
                    throw new InvalidOperationException(
                        $"The durable agent ingestion position '{position.Name}' must be a non-negative Int32 value.");
                }
            }
        }

        if (dataElement.TryGetProperty("truncation", out JsonElement truncation))
        {
            if (truncation.ValueKind != JsonValueKind.Object ||
                !truncation.TryGetProperty("evictedMessageCount", out _) ||
                !truncation.TryGetProperty("firstEvictedAt", out _) ||
                !truncation.TryGetProperty("lastEvictedAt", out _))
            {
                throw new InvalidOperationException(
                    "Durable agent truncation evidence requires evictedMessageCount, firstEvictedAt, and lastEvictedAt.");
            }
        }
    }

    private static void ValidateDeclaredExtensionData(JsonElement root, JsonElement data)
    {
        RequireObjectWhenPresent(root, ExtensionDataPropertyName, "extensionData");
        RequireObjectWhenPresent(data, ExtensionDataPropertyName, "data.extensionData");

        if (data.TryGetProperty("conversationHistory", out JsonElement history) &&
            history.ValueKind == JsonValueKind.Array)
        {
            foreach (JsonElement entry in history.EnumerateArray())
            {
                if (entry.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                RequireObjectWhenPresent(entry, ExtensionDataPropertyName, "conversationHistory.extensionData");
                string? entryType = entry.TryGetProperty("$type", out JsonElement typeElement) &&
                    typeElement.ValueKind == JsonValueKind.String
                        ? typeElement.GetString()
                        : null;
                if (entryType is "response" or "errorResponse" &&
                    entry.TryGetProperty("usage", out JsonElement usage) &&
                    usage.ValueKind == JsonValueKind.Object)
                {
                    RequireObjectWhenPresent(usage, ExtensionDataPropertyName, "conversationHistory.usage.extensionData");
                }

                if (entry.TryGetProperty("messages", out JsonElement messages) &&
                    messages.ValueKind == JsonValueKind.Array)
                {
                    foreach (JsonElement message in messages.EnumerateArray())
                    {
                        if (message.ValueKind == JsonValueKind.Object)
                        {
                            RequireObjectWhenPresent(
                                message,
                                ExtensionDataPropertyName,
                                "conversationHistory.messages.extensionData");
                        }
                    }
                }
            }
        }

        if (data.TryGetProperty("terminalResults", out JsonElement terminalResults) &&
            terminalResults.ValueKind == JsonValueKind.Object)
        {
            foreach (JsonProperty result in terminalResults.EnumerateObject())
            {
                if (result.Value.TryGetProperty("resultExpiresAt", out JsonElement resultExpiresAt) &&
                    resultExpiresAt.ValueKind != JsonValueKind.String)
                {
                    throw new JsonException(
                        $"Durable agent terminal result '{result.Name}' resultExpiresAt must be a string when present.");
                }

                if (result.Value.TryGetProperty("error", out JsonElement error) &&
                    error.ValueKind != JsonValueKind.Object)
                {
                    throw new JsonException(
                        $"Durable agent terminal result '{result.Name}' error must be an object when present.");
                }

                if (result.Value.TryGetProperty("response", out JsonElement response) &&
                    response.ValueKind == JsonValueKind.Object)
                {
                    RequireObjectWhenPresent(
                        response,
                        ExtensionDataPropertyName,
                        $"terminalResults.{result.Name}.response.extensionData");
                    foreach (string propertyName in new[]
                    {
                        "createdAt",
                        "responseId",
                        "agentId",
                        "finishReason",
                        "continuationToken",
                    })
                    {
                        RequireStringWhenPresent(
                            response,
                            propertyName,
                            $"terminalResults.{result.Name}.response.{propertyName}");
                    }

                    if (response.TryGetProperty("usage", out JsonElement usage) &&
                        usage.ValueKind == JsonValueKind.Object)
                    {
                        RequireObjectWhenPresent(
                            usage,
                            ExtensionDataPropertyName,
                            $"terminalResults.{result.Name}.response.usage.extensionData");
                    }
                }
            }
        }

        if (data.TryGetProperty("completionReceipts", out JsonElement receipts) &&
            receipts.ValueKind == JsonValueKind.Object)
        {
            foreach (JsonProperty receipt in receipts.EnumerateObject())
            {
                foreach (string propertyName in new[] { "resultExpiresAt", "resultUnavailableAt" })
                {
                    RequireStringWhenPresent(
                        receipt.Value,
                        propertyName,
                        $"completionReceipts.{receipt.Name}.{propertyName}");
                }
            }
        }
    }

    private static void ValidateKnownFieldShapes(JsonElement data)
    {
        RequireDateTimeWhenPresent(
            data,
            "expirationTimeUtc",
            "data.expirationTimeUtc",
            allowNull: true);

        if (data.TryGetProperty("conversationHistory", out JsonElement history) &&
            history.ValueKind == JsonValueKind.Array)
        {
            foreach (JsonElement entry in history.EnumerateArray())
            {
                if (entry.ValueKind != JsonValueKind.Object)
                {
                    continue;
                }

                RequireDateTimeWhenPresent(entry, "createdAt", "conversationHistory.createdAt");
                RequireStringWhenPresent(entry, "correlationId", "conversationHistory.correlationId");
                string? entryType = entry.TryGetProperty("$type", out JsonElement typeElement) &&
                    typeElement.ValueKind == JsonValueKind.String
                        ? typeElement.GetString()
                        : null;
                if (entryType == "request")
                {
                    RequireStringWhenPresent(entry, "orchestrationId", "conversationHistory.orchestrationId");
                    RequireStringWhenPresent(entry, "responseType", "conversationHistory.responseType");
                    RequireObjectWhenPresent(entry, "responseSchema", "conversationHistory.responseSchema");
                }
                else if (entryType is "response" or "errorResponse")
                {
                    ValidateUsageWhenPresent(entry, "usage", "conversationHistory.usage");
                }

                if (entry.TryGetProperty("messages", out JsonElement messages) &&
                    messages.ValueKind == JsonValueKind.Array)
                {
                    foreach (JsonElement message in messages.EnumerateArray())
                    {
                        if (message.ValueKind != JsonValueKind.Object)
                        {
                            continue;
                        }

                        RequireStringWhenPresent(message, "authorName", "conversationHistory.messages.authorName");
                        RequireDateTimeWhenPresent(
                            message,
                            "createdAt",
                            "conversationHistory.messages.createdAt");
                        RequireStringWhenPresent(message, "messageId", "conversationHistory.messages.messageId");
                    }
                }
            }
        }

        if (data.TryGetProperty("terminalResults", out JsonElement terminalResults) &&
            terminalResults.ValueKind == JsonValueKind.Object)
        {
            foreach (JsonProperty result in terminalResults.EnumerateObject())
            {
                if (result.Value.TryGetProperty("response", out JsonElement response) &&
                    response.ValueKind == JsonValueKind.Object)
                {
                    RequireDateTimeWhenPresent(
                        result.Value,
                        "completedAt",
                        $"terminalResults.{result.Name}.completedAt");
                    RequireDateTimeWhenPresent(
                        result.Value,
                        "resultExpiresAt",
                        $"terminalResults.{result.Name}.resultExpiresAt");
                    RequireDateTimeWhenPresent(
                        response,
                        "createdAt",
                        $"terminalResults.{result.Name}.response.createdAt");
                    ValidateUsageWhenPresent(
                        response,
                        "usage",
                        $"terminalResults.{result.Name}.response.usage");
                }
            }
        }

        if (data.TryGetProperty("completionReceipts", out JsonElement receipts) &&
            receipts.ValueKind == JsonValueKind.Object)
        {
            foreach (JsonProperty receipt in receipts.EnumerateObject())
            {
                RequireDateTimeWhenPresent(
                    receipt.Value,
                    "completedAt",
                    $"completionReceipts.{receipt.Name}.completedAt");
                RequireDateTimeWhenPresent(
                    receipt.Value,
                    "resultExpiresAt",
                    $"completionReceipts.{receipt.Name}.resultExpiresAt");
                RequireDateTimeWhenPresent(
                    receipt.Value,
                    "resultUnavailableAt",
                    $"completionReceipts.{receipt.Name}.resultUnavailableAt");
            }
        }

        if (data.TryGetProperty("truncation", out JsonElement truncation) &&
            truncation.ValueKind == JsonValueKind.Object)
        {
            RequireDateTimeWhenPresent(truncation, "firstEvictedAt", "data.truncation.firstEvictedAt");
            RequireDateTimeWhenPresent(truncation, "lastEvictedAt", "data.truncation.lastEvictedAt");
        }
    }

    private static void ValidateUsageWhenPresent(JsonElement parent, string propertyName, string path)
    {
        if (!parent.TryGetProperty(propertyName, out JsonElement usage))
        {
            return;
        }

        if (usage.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException($"The durable agent state '{path}' property must be an object.");
        }

        ValidateUsageObject(usage, path);
    }

    private static void ValidateUsageObject(JsonElement usage, string path)
    {
        foreach (string countName in new[] { "inputTokenCount", "outputTokenCount", "totalTokenCount" })
        {
            if (usage.TryGetProperty(countName, out JsonElement count) &&
                (count.ValueKind != JsonValueKind.Number || !count.TryGetInt64(out _)))
            {
                throw new JsonException(
                    $"The durable agent state '{path}.{countName}' property must be an Int64 value.");
            }
        }

        RequireObjectWhenPresent(usage, ExtensionDataPropertyName, $"{path}.extensionData");
    }

    private static void RequireObjectWhenPresent(JsonElement parent, string propertyName, string path)
    {
        if (parent.TryGetProperty(propertyName, out JsonElement value) &&
            value.ValueKind != JsonValueKind.Object)
        {
            throw new JsonException($"The durable agent state '{path}' property must be an object.");
        }
    }

    private static void RequireStringWhenPresent(JsonElement parent, string propertyName, string path)
    {
        if (parent.TryGetProperty(propertyName, out JsonElement value) &&
            value.ValueKind != JsonValueKind.String)
        {
            throw new JsonException($"The durable agent state '{path}' property must be a string.");
        }
    }

    private static void RequireDateTimeWhenPresent(
        JsonElement parent,
        string propertyName,
        string path,
        bool allowNull = false)
    {
        if (!parent.TryGetProperty(propertyName, out JsonElement value))
        {
            return;
        }

        if (allowNull && value.ValueKind == JsonValueKind.Null)
        {
            return;
        }

        if (value.ValueKind != JsonValueKind.String ||
            !IsOffsetRfc3339(value.GetString()))
        {
            throw new JsonException(
                $"The durable agent state '{path}' property must be an RFC 3339 date-time with an explicit offset.");
        }
    }

    private static bool IsOffsetRfc3339(string? value)
    {
        if (string.IsNullOrEmpty(value) ||
            !s_rfc3339Pattern.IsMatch(value) ||
            !DateTimeOffset.TryParse(
                value,
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out _))
        {
            return false;
        }

        return value.EndsWith('Z') ||
            (value.Length >= 6 &&
             value[^6] is '+' or '-' &&
             value[^3] == ':');
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

            ValidateMessageArray(messages, $"terminal result '{result.Name}'");
        }
    }

    private static void ValidateTranscript(JsonElement conversationHistory)
    {
        if (conversationHistory.ValueKind != JsonValueKind.Array)
        {
            throw new JsonException(
                "The revised durable agent state 'data.conversationHistory' property must be an array.");
        }

        foreach (JsonElement entry in conversationHistory.EnumerateArray())
        {
            if (entry.ValueKind != JsonValueKind.Object ||
                !entry.TryGetProperty("$type", out JsonElement typeElement) ||
                typeElement.ValueKind != JsonValueKind.String)
            {
                continue;
            }

            string? entryType = typeElement.GetString();
            bool hasCorrelation = entry.TryGetProperty("correlationId", out JsonElement correlation);
            if (entryType == "compaction" && hasCorrelation)
            {
                throw new InvalidOperationException(
                    "A revised durable agent compaction entry cannot declare correlationId.");
            }

            if (entryType is "request" or "response" or "errorResponse" && hasCorrelation)
            {
                if (correlation.ValueKind != JsonValueKind.String)
                {
                    throw new InvalidOperationException(
                        "A revised durable agent transcript correlationId must be a string when present.");
                }

                DurableAgentStateContract.ValidateIdentifier(
                    correlation.GetString(),
                    "conversationHistory.correlationId");
            }

            if (entry.TryGetProperty("messages", out JsonElement messages))
            {
                ValidateMessageArray(messages, "conversationHistory");
            }
        }
    }

    private static void ValidateMessageArray(JsonElement messages, string location)
    {
        if (messages.ValueKind != JsonValueKind.Array)
        {
            throw new InvalidOperationException(
                $"Durable agent {location} messages must be an array.");
        }

        foreach (JsonElement message in messages.EnumerateArray())
        {
            if (message.ValueKind != JsonValueKind.Object)
            {
                throw new InvalidOperationException(
                    $"Durable agent {location} contains a non-object message.");
            }

            RequireStringWhenPresent(message, "authorName", $"{location}.authorName");
            RequireDateTimeWhenPresent(message, "createdAt", $"{location}.createdAt");
            RequireStringWhenPresent(message, "messageId", $"{location}.messageId");
            RequireObjectWhenPresent(message, ExtensionDataPropertyName, $"{location}.extensionData");

            if (!message.TryGetProperty("contents", out JsonElement contents))
            {
                continue;
            }

            if (contents.ValueKind != JsonValueKind.Array)
            {
                throw new InvalidOperationException(
                    $"Durable agent {location} contains a non-array message contents property.");
            }

            foreach (JsonElement content in contents.EnumerateArray())
            {
                if (content.ValueKind != JsonValueKind.Object ||
                    !content.TryGetProperty("$type", out JsonElement contentType) ||
                    contentType.ValueKind != JsonValueKind.String)
                {
                    continue;
                }

                if (contentType.ValueEquals("functionCall") &&
                    content.TryGetProperty("arguments", out JsonElement arguments) &&
                    arguments.ValueKind is not JsonValueKind.Object and not JsonValueKind.String)
                {
                    throw new InvalidOperationException(
                        "Durable agent function-call arguments must be an object or string when present.");
                }

                if (contentType.ValueEquals("uri") &&
                    content.TryGetProperty("mediaType", out JsonElement mediaType) &&
                    mediaType.ValueKind != JsonValueKind.String)
                {
                    throw new InvalidOperationException(
                        "Durable agent URI mediaType must be a string when present.");
                }

                if (contentType.ValueEquals("usage") &&
                    content.TryGetProperty("usage", out JsonElement contentUsage))
                {
                    if (contentUsage.ValueKind != JsonValueKind.Object)
                    {
                        throw new InvalidOperationException(
                            "Durable agent usage content requires an object-valued usage property.");
                    }

                    ValidateUsageObject(contentUsage, "message.contents.usage");
                }

                ValidateKnownContentFields(content, contentType.GetString()!);
            }
        }
    }

    private static void ValidateKnownContentFields(JsonElement content, string contentType)
    {
        switch (contentType)
        {
            case "data":
                RequireString(content, "uri", contentType);
                OptionalString(content, "mediaType", contentType);
                break;
            case "error":
                OptionalString(content, "message", contentType);
                OptionalString(content, "errorCode", contentType);
                break;
            case "functionCall":
                RequireString(content, "callId", contentType);
                RequireString(content, "name", contentType);
                break;
            case "functionResult":
                RequireString(content, "callId", contentType);
                break;
            case "hostedFile":
                RequireString(content, "fileId", contentType);
                break;
            case "hostedVectorStore":
                RequireString(content, "vectorStoreId", contentType);
                break;
            case "text":
                RequireString(content, "text", contentType);
                break;
            case "reasoning":
                OptionalString(content, "text", contentType);
                break;
            case "uri":
                RequireString(content, "uri", contentType);
                break;
            case "usage":
                if (!content.TryGetProperty("usage", out JsonElement usage) ||
                    usage.ValueKind != JsonValueKind.Object)
                {
                    throw new InvalidOperationException(
                        "Durable agent usage content requires an object-valued usage property.");
                }

                break;
            case "unknown":
                if (!content.TryGetProperty("content", out _))
                {
                    throw new InvalidOperationException(
                        "Durable agent unknown content requires the original content value.");
                }

                break;
        }
    }

    private static void RequireString(JsonElement element, string propertyName, string contentType)
    {
        if (!element.TryGetProperty(propertyName, out JsonElement value) ||
            value.ValueKind != JsonValueKind.String)
        {
            throw new InvalidOperationException(
                $"Durable agent '{contentType}' content requires string property '{propertyName}'.");
        }
    }

    private static void OptionalString(JsonElement element, string propertyName, string contentType)
    {
        if (element.TryGetProperty(propertyName, out JsonElement value) &&
            value.ValueKind != JsonValueKind.String)
        {
            throw new InvalidOperationException(
                $"Durable agent '{contentType}' content property '{propertyName}' must be a string when present.");
        }
    }
}
