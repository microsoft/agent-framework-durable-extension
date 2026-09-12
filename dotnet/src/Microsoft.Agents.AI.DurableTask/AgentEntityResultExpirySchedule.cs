// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>An optional runtime profile, not part of the shared mailbox schema or history binding.</summary>
internal sealed class AgentEntityResultExpirySchedule
{
    internal const string ExtensionName = "Microsoft.Agents.AI.DurableTask.resultExpiry";
    private readonly Dictionary<string, JsonElement> _properties;

    private AgentEntityResultExpirySchedule(
        Dictionary<string, JsonElement> properties,
        AgentEntityResultExpirationCheck? pending)
    {
        this._properties = properties;
        this.Pending = pending;
    }

    public AgentEntityResultExpirationCheck? Pending { get; }

    public static AgentEntityResultExpirySchedule? Read(DurableAgentState state, string entityId)
    {
        if (state.ExtensionData?.TryGetValue(ExtensionName, out JsonElement profile) != true)
        {
            return null;
        }

        if (profile.ValueKind != JsonValueKind.Object)
        {
            throw InvalidProfile();
        }

        Dictionary<string, JsonElement> properties = new(StringComparer.Ordinal);
        foreach (JsonProperty property in profile.EnumerateObject())
        {
            if (!properties.TryAdd(property.Name, property.Value.Clone()))
            {
                throw InvalidProfile();
            }
        }

        if (!properties.TryGetValue("version", out JsonElement version) ||
            version.ValueKind != JsonValueKind.Number || !version.TryGetInt32(out int number) || number != 1 ||
            !properties.TryGetValue("entityId", out JsonElement identity) ||
            identity.ValueKind != JsonValueKind.String || identity.GetString() != entityId ||
            !properties.TryGetValue("scheduledResultExpiryUtc", out JsonElement deadline) ||
            !properties.TryGetValue("token", out JsonElement token))
        {
            throw InvalidProfile();
        }

        AgentEntityResultExpirationCheck? pending = null;
        if (deadline.ValueKind != JsonValueKind.Null || token.ValueKind != JsonValueKind.Null)
        {
            if (deadline.ValueKind != JsonValueKind.String || !deadline.TryGetDateTimeOffset(out DateTimeOffset scheduledTime) ||
                scheduledTime.Offset != TimeSpan.Zero ||
                token.ValueKind != JsonValueKind.String || !Guid.TryParseExact(token.GetString(), "N", out Guid generation) ||
                generation == Guid.Empty)
            {
                throw InvalidProfile();
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

        using MemoryStream buffer = new();
        using (Utf8JsonWriter writer = new(buffer))
        {
            writer.WriteStartObject();
            writer.WriteNumber("version", 1);
            writer.WriteString("entityId", entityId);
            if (pending is null)
            {
                writer.WriteNull("scheduledResultExpiryUtc");
                writer.WriteNull("token");
            }
            else
            {
                writer.WriteString("scheduledResultExpiryUtc", pending.ScheduledTime);
                writer.WriteString("token", pending.Token);
            }

            if (previous is not null)
            {
                foreach ((string key, JsonElement value) in previous._properties)
                {
                    if (key is not ("version" or "entityId" or "scheduledResultExpiryUtc" or "token"))
                    {
                        writer.WritePropertyName(key);
                        value.WriteTo(writer);
                    }
                }
            }

            writer.WriteEndObject();
        }

        using JsonDocument profile = JsonDocument.Parse(buffer.ToArray());
        Dictionary<string, JsonElement> extensions = state.ExtensionData is null
            ? new(StringComparer.Ordinal)
            : new(state.ExtensionData, StringComparer.Ordinal);
        extensions[ExtensionName] = profile.RootElement.Clone();
        return new DurableAgentState
        {
            SchemaVersion = state.SchemaVersion,
            MailboxWritesAuthorized = state.MailboxWritesAuthorized,
            Data = state.Data,
            ExtensionData = extensions,
            UnknownProperties = state.UnknownProperties,
        };
    }

    private static InvalidOperationException InvalidProfile() =>
        new($"The '{ExtensionName}' runtime profile is malformed, unsupported, or belongs to another entity. " +
            "Result-expiry scheduling cannot safely continue; preserve the profile and use a compatible writer.");
}
