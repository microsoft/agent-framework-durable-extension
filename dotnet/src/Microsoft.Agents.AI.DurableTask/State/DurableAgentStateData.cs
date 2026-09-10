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
    /// Gets or sets the opaque state produced by the configured agent's session serialization contract.
    /// </summary>
    /// <remarks>
    /// This value can contain service conversation identity, continuation state, and provider-specific
    /// state that cannot be reduced to a conversation ID. The durable state layer owns only the JSON
    /// representation: it requires an object, clones assigned values away from caller-owned
    /// <see cref="JsonDocument"/> instances, and round-trips the object without interpreting property
    /// names such as <c>$type</c> or <c>$runtimeType</c>. It never uses this JSON to select or construct a
    /// CLR type. A later integration layer may return the object only to the configured agent through
    /// that agent's session deserialization contract.
    ///
    /// The normal <c>System.Text.Json</c> nesting limit applies when the enclosing state is parsed.
    /// This schema layer intentionally has no independent byte cap because valid opaque provider state can
    /// vary in size; the durable entity storage budget and retention policy remain the outer trust boundary.
    /// Producers must therefore treat session state as persisted data, not as a trusted instruction or an
    /// object graph.
    /// </remarks>
    [JsonPropertyName("session")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public JsonElement? Session
    {
        get;
        set
        {
            if (value is not JsonElement element)
            {
                field = null;
                return;
            }

            if (element.ValueKind != JsonValueKind.Object)
            {
                throw new JsonException(
                    "The durable agent state 'data.session' property must be a JSON object.");
            }

            field = element.Clone();
        }
    }

    /// <summary>
    /// Gets or sets the highest legacy scalar conversation position ingested from each workflow producer.
    /// </summary>
    /// <remarks>
    /// This field records the legacy scalar-watermark design for workflow producers: a compatible producer
    /// can use the highest known contiguous position to avoid redelivering messages it already incorporated.
    /// It is distinct from the exact completion-receipt design used for terminal delivery. The current .NET
    /// and Python production paths do not produce or consume these values; .NET preserves and round-trips
    /// them so state written by a compatible workflow implementation is not discarded.
    /// </remarks>
    [JsonPropertyName("ingestedPositions")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IDictionary<string, int>? IngestedPositions { get; set; }

    /// <summary>
    /// Gets or sets bounded evidence that transcript messages were removed from durable state.
    /// </summary>
    /// <remarks>
    /// The evidence persists after the corresponding transcript entries are gone and records the cumulative
    /// count plus the first and latest eviction times. This lets readers and operators distinguish an
    /// intentionally truncated transcript from one in which the missing messages were never persisted.
    /// It is diagnostic provenance only: it is not model context, a terminal result, or proof that a
    /// correlation completed. This layer preserves the contract but does not currently produce or consume
    /// truncation evidence.
    /// </remarks>
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
    /// Gets producer-defined values from the schema's declared data-level <c>extensionData</c> field.
    /// </summary>
    /// <remarks>
    /// This is an explicit interoperability field. It is separate from <see cref="UnknownProperties"/>,
    /// which captures undeclared future JSON members through <see cref="JsonExtensionDataAttribute"/>.
    /// </remarks>
    [JsonPropertyName("extensionData")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IDictionary<string, JsonElement>? ExtensionData { get; init; }

    /// <summary>
    /// Gets undeclared future data properties that appear beside the schema's known fields.
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
