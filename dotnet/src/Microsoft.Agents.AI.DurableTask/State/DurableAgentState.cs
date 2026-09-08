// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Represents the state of a durable agent, including its conversation history.
/// </summary>
[JsonConverter(typeof(DurableAgentStateJsonConverter))]
internal sealed class DurableAgentState
{
    internal const string CurrentSchemaVersion = "1.2.0";
    private static readonly DurableAgentStateSchemaVersion s_currentSchemaVersion =
        DurableAgentStateSchemaVersion.ParseSupported(CurrentSchemaVersion);

    /// <summary>
    /// Gets the data of the durable agent.
    /// </summary>
    [JsonPropertyName("data")]
    public DurableAgentStateData Data { get; init; } = new();

    /// <summary>
    /// Gets the schema version of the durable agent state.
    /// </summary>
    /// <remarks>
    /// New states default to <see cref="CurrentSchemaVersion"/>. Deserialization assigns the
    /// persisted value through this init-only property, and <see cref="Clone"/> constructs a new
    /// state when an older compatible version must be promoted for a write. Future compatible
    /// versions are preserved rather than rewritten.
    /// </remarks>
    [JsonPropertyName("schemaVersion")]
    public string SchemaVersion { get; init; } = CurrentSchemaVersion;

    /// <summary>
    /// Gets application-defined root extension metadata from the schema's <c>extensionData</c> property.
    /// </summary>
    [JsonPropertyName("extensionData")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IDictionary<string, JsonElement>? ExtensionData { get; init; }

    /// <summary>
    /// Gets unknown root properties that are outside the declared schema.
    /// </summary>
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? UnknownProperties { get; set; }

    /// <summary>
    /// Creates an independent copy suitable for an atomic entity operation.
    /// </summary>
    public DurableAgentState Clone()
    {
        byte[] serialized = JsonSerializer.SerializeToUtf8Bytes(
            this,
            DurableAgentStateJsonContext.Default.DurableAgentState);
        DurableAgentState clone = JsonSerializer.Deserialize(
            serialized,
            DurableAgentStateJsonContext.Default.DurableAgentState)
            ?? throw new JsonException("The durable agent state could not be cloned.");
        DurableAgentStateMessageIdentity.EnsureMessageIds(clone.Data.ConversationHistory);

        return new DurableAgentState
        {
            SchemaVersion = SelectSchemaVersionForWrite(clone.SchemaVersion),
            Data = clone.Data,
            ExtensionData = clone.ExtensionData,
            UnknownProperties = clone.UnknownProperties,
        };
    }

    private static string SelectSchemaVersionForWrite(string schemaVersion)
    {
        DurableAgentStateSchemaVersion sourceVersion =
            DurableAgentStateSchemaVersion.ParseSupported(schemaVersion);
        return sourceVersion.CompareTo(s_currentSchemaVersion) < 0
            ? CurrentSchemaVersion
            : schemaVersion;
    }
}
