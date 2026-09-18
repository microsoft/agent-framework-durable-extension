// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Agents.AI.DurableTask.State;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Tracks the result-expiration self-signal currently scheduled for one agent entity.
/// </summary>
/// <remarks>
/// This runtime-owned schedule metadata is stored under <see cref="DurableAgentState.ExtensionData"/>. It contains the entity
/// identity, scheduled UTC time, and generation token needed to reject delayed signals that belong to an older
/// schedule or deleted entity generation.
///
/// It is separate from the shared durable-state contract: it does not contain request results, completion receipts,
/// or conversation-history evidence. The metadata is optional because a successful run or explicit cleanup operation
/// can reconstruct it from the retained result-expiration timestamps.
/// </remarks>
internal sealed class AgentEntityResultExpirySchedule
{
    // This is a persisted protocol key, not a type name. Keep it stable across code refactors so existing entity
    // state remains readable by future runtimes.
    internal const string ExtensionKey = "Microsoft.Agents.AI.DurableTask.resultExpiry";
    private const int CurrentMetadataVersion = 1;
    private const string VersionPropertyName = "version";
    private const string EntityIdPropertyName = "entityId";
    private const string ScheduledResultExpiryUtcPropertyName = "scheduledResultExpiryUtc";
    private const string TokenPropertyName = "token";
    private readonly Dictionary<string, JsonElement> _metadataProperties;

    private AgentEntityResultExpirySchedule(
        Dictionary<string, JsonElement> metadataProperties,
        AgentEntityResultExpirationCheck? pending)
    {
        this._metadataProperties = metadataProperties;
        this.Pending = pending;
    }

    public AgentEntityResultExpirationCheck? Pending { get; }

    public static AgentEntityResultExpirySchedule? Read(DurableAgentState state, string entityId)
    {
        if (state.ExtensionData?.TryGetValue(ExtensionKey, out JsonElement scheduleMetadata) != true)
        {
            return null;
        }

        if (scheduleMetadata.ValueKind != JsonValueKind.Object)
        {
            throw InvalidScheduleMetadata();
        }

        Dictionary<string, JsonElement> properties = new(StringComparer.Ordinal);
        foreach (JsonProperty property in scheduleMetadata.EnumerateObject())
        {
            if (!properties.TryAdd(property.Name, property.Value.Clone()))
            {
                throw InvalidScheduleMetadata();
            }
        }

        if (!properties.TryGetValue(VersionPropertyName, out JsonElement version) ||
            version.ValueKind != JsonValueKind.Number ||
            !version.TryGetInt32(out int number) ||
            number != CurrentMetadataVersion ||
            !properties.TryGetValue(EntityIdPropertyName, out JsonElement identity) ||
            identity.ValueKind != JsonValueKind.String || identity.GetString() != entityId ||
            !properties.TryGetValue(ScheduledResultExpiryUtcPropertyName, out JsonElement deadline) ||
            !properties.TryGetValue(TokenPropertyName, out JsonElement token))
        {
            throw InvalidScheduleMetadata();
        }

        AgentEntityResultExpirationCheck? pending = null;
        if (deadline.ValueKind != JsonValueKind.Null || token.ValueKind != JsonValueKind.Null)
        {
            if (deadline.ValueKind != JsonValueKind.String || !deadline.TryGetDateTimeOffset(out DateTimeOffset scheduledTime) ||
                scheduledTime.Offset != TimeSpan.Zero ||
                token.ValueKind != JsonValueKind.String || !Guid.TryParseExact(token.GetString(), "N", out Guid generation) ||
                generation == Guid.Empty)
            {
                throw InvalidScheduleMetadata();
            }

            pending = new AgentEntityResultExpirationCheck(scheduledTime, token.GetString(), entityId);
        }

        return new AgentEntityResultExpirySchedule(properties, pending);
    }

    public static DurableAgentState Write(
        DurableAgentState state,
        string entityId,
        AgentEntityResultExpirySchedule? previous,
        AgentEntityResultExpirationCheck? pending)
    {
        if (previous?.Pending == pending)
        {
            return state;
        }

        JsonObject scheduleMetadata = new()
        {
            [VersionPropertyName] = CurrentMetadataVersion,
            [EntityIdPropertyName] = entityId,
            [ScheduledResultExpiryUtcPropertyName] = pending?.ScheduledTime,
            [TokenPropertyName] = pending?.Token,
        };

        if (previous is not null)
        {
            foreach ((string key, JsonElement value) in previous._metadataProperties)
            {
                if (key is not (
                    VersionPropertyName or
                    EntityIdPropertyName or
                    ScheduledResultExpiryUtcPropertyName or
                    TokenPropertyName))
                {
                    scheduleMetadata[key] =
                        value.Deserialize(DurableAgentStateJsonContext.Default.JsonNode);
                }
            }
        }

        Dictionary<string, JsonElement> extensions = state.ExtensionData is null
            ? new(StringComparer.Ordinal)
            : new(state.ExtensionData, StringComparer.Ordinal);
        extensions[ExtensionKey] = JsonSerializer.SerializeToElement(
            scheduleMetadata,
            DurableAgentStateJsonContext.Default.JsonObject);
        return new DurableAgentState
        {
            SchemaVersion = state.SchemaVersion,
            PersistentRequestOutcomesAuthorized = state.PersistentRequestOutcomesAuthorized,
            Data = state.Data,
            ExtensionData = extensions,
            UnknownProperties = state.UnknownProperties,
        };
    }

    private static InvalidOperationException InvalidScheduleMetadata() =>
        new($"The '{ExtensionKey}' schedule metadata is malformed, unsupported, or belongs to another entity. " +
            "Result-expiry scheduling cannot safely continue; preserve the metadata and use a compatible writer.");
}
