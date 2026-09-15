// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// JSON-safe failure metadata for a terminal result.
/// </summary>
internal sealed class DurableAgentStateTerminalError
{
    [JsonPropertyName("code")]
    public required string Code { get; init; }

    [JsonPropertyName("message")]
    public required string Message { get; init; }

    [JsonPropertyName("details")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public JsonElement Details
    {
        get;
        init => field = value.ValueKind == JsonValueKind.Undefined ? default : value.Clone();
    }

    [JsonExtensionData]
    public IDictionary<string, JsonElement>? UnknownProperties { get; set; }

    public void Validate()
    {
        DurableAgentStateContract.ValidateIdentifier(this.Code, "terminalResults.error.code");
        if (string.IsNullOrWhiteSpace(this.Message) ||
            this.Message.EnumerateRunes()
                .Take(DurableAgentStateContract.MaxMetadataStringLength + 1)
                .Count() > DurableAgentStateContract.MaxMetadataStringLength)
        {
            throw new InvalidOperationException(
                $"The durable agent terminal error message must contain a non-whitespace character and be at most {DurableAgentStateContract.MaxMetadataStringLength} Unicode characters.");
        }
    }
}
