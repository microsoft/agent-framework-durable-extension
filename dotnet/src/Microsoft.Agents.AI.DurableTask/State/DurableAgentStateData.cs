// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Represents the data of a durable agent, including its conversation history.
/// </summary>
internal sealed class DurableAgentStateData
{
    /// <summary>
    /// Gets the ordered list of state entries representing the complete conversation history.
    /// This includes both user messages and agent responses in chronological order.
    /// </summary>
    [JsonPropertyName("conversationHistory")]
    public IList<DurableAgentStateEntry> ConversationHistory { get; init; } = [];

    /// <summary>
    /// Gets immutable terminal result payloads indexed by correlation ID.
    /// </summary>
    [JsonPropertyName("terminalResults")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IDictionary<string, DurableAgentStateTerminalResult>? TerminalResults { get; init; }

    /// <summary>
    /// Gets completion receipts retained independently from result payload expiry.
    /// </summary>
    [JsonPropertyName("completionReceipts")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IDictionary<string, DurableAgentStateCompletionReceipt>? CompletionReceipts { get; init; }

    /// <summary>
    /// Gets the fixed logical history ownership binding for this durable session.
    /// </summary>
    [JsonPropertyName("historyBinding")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DurableAgentStateHistoryBinding? HistoryBinding { get; init; }

    /// <summary>
    /// Gets or sets the serialized inner agent session.
    /// </summary>
    [JsonPropertyName("session")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public JsonElement? Session { get; set; }

    /// <summary>
    /// Gets or sets the highest workflow conversation position ingested from each executor.
    /// </summary>
    /// <remarks>
    /// The .NET workflow path does not populate these watermarks yet, but they are preserved for
    /// cross-language schema compatibility.
    /// </remarks>
    [JsonPropertyName("ingestedPositions")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IDictionary<string, int>? IngestedPositions { get; set; }

    /// <summary>
    /// Gets or sets bounded evidence that retention removed conversation messages.
    /// </summary>
    [JsonPropertyName("truncation")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DurableAgentStateTruncation? Truncation { get; set; }

    /// <summary>
    /// Gets or sets the expiration time (UTC) for this agent entity.
    /// If the entity is idle beyond this time, it will be automatically deleted.
    /// </summary>
    [JsonPropertyName("expirationTimeUtc")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DateTime? ExpirationTimeUtc { get; set; }

    /// <summary>
    /// Gets application-defined data-level metadata from the schema's <c>extensionData</c> property.
    /// </summary>
    [JsonPropertyName("extensionData")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IDictionary<string, JsonElement>? ExtensionData { get; init; }

    /// <summary>
    /// Gets unknown data properties that are outside the declared schema.
    /// </summary>
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? UnknownProperties { get; set; }

    public void Validate(string schemaVersion)
    {
        DurableAgentStateSchemaVersion version =
            DurableAgentStateSchemaVersion.ParseSupported(schemaVersion);
        if (version.Major == DurableAgentState.RevisedSchemaMajorVersion)
        {
            if (this.ConversationHistory is null)
            {
                throw new InvalidOperationException(
                    "A revised durable agent state requires a conversation history collection.");
            }

            if (this.HistoryBinding is null ||
                this.TerminalResults is null ||
                this.CompletionReceipts is null)
            {
                throw new InvalidOperationException(
                    "A revised durable agent state requires history binding, terminal results, and completion receipts.");
            }

            this.HistoryBinding.Validate();
            foreach ((string correlationId, DurableAgentStateTerminalResult result) in this.TerminalResults)
            {
                result.Validate(correlationId);
                if (!this.CompletionReceipts.TryGetValue(correlationId, out DurableAgentStateCompletionReceipt? receipt))
                {
                    throw new InvalidOperationException(
                        $"Durable agent terminal result '{correlationId}' has no completion receipt.");
                }

                if (receipt.ResultState != DurableAgentStateCompletionReceipt.AvailableResult ||
                    receipt.Outcome != result.Outcome ||
                    receipt.CompletedAt != result.CompletedAt ||
                    receipt.ResultExpiresAt != result.ResultExpiresAt)
                {
                    throw new InvalidOperationException(
                        $"Durable agent terminal result '{correlationId}' is inconsistent with its completion receipt.");
                }
            }

            foreach ((string correlationId, DurableAgentStateCompletionReceipt receipt) in this.CompletionReceipts)
            {
                receipt.Validate(correlationId);
                bool hasResult = this.TerminalResults.ContainsKey(correlationId);
                if (receipt.ResultState == DurableAgentStateCompletionReceipt.AvailableResult != hasResult)
                {
                    throw new InvalidOperationException(
                        $"Durable agent completion receipt '{correlationId}' is inconsistent with result availability.");
                }
            }
        }
        else if (this.TerminalResults is not null ||
                 this.CompletionReceipts is not null ||
                 this.HistoryBinding is not null)
        {
            throw new InvalidOperationException(
                "Mailbox and fixed-history binding fields require durable agent state schema version 2.x.");
        }
    }
}
