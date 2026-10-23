// Copyright (c) Microsoft. All rights reserved.

using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// An <see cref="AgentSession"/> implementation for durable agents.
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class DurableAgentSession : AgentSession
{
    internal DurableAgentSession(AgentSessionId sessionId, bool canAdoptReplayedEntityVersion = false)
    {
        this.SessionId = sessionId;
        this.CanAdoptReplayedEntityVersion = canAdoptReplayedEntityVersion;
    }

    [JsonConstructor]
    internal DurableAgentSession(AgentSessionId sessionId, AgentSessionStateBag stateBag) : base(stateBag)
    {
        this.SessionId = sessionId;
    }

    /// <summary>
    /// Gets the agent session ID.
    /// </summary>
    [JsonInclude]
    [JsonPropertyName("sessionId")]
    internal AgentSessionId SessionId { get; set; }

    /// <summary>
    /// Gets a value indicating whether this session was newly created inside an orchestration and can adopt
    /// the entity protocol version observed on its first replayed turn.
    /// </summary>
    [JsonIgnore]
    internal bool CanAdoptReplayedEntityVersion { get; private set; }

    internal void LockEntityVersion(int? entityVersion = null)
    {
        if (!this.CanAdoptReplayedEntityVersion)
        {
            return;
        }

        if (entityVersion is int version and >= 1)
        {
            this.SessionId = this.SessionId.WithVersion(version);
        }

        this.CanAdoptReplayedEntityVersion = false;
    }

    /// <inheritdoc/>
    internal JsonElement Serialize(JsonSerializerOptions? jsonSerializerOptions = null)
    {
        var jso = jsonSerializerOptions ?? DurableAgentJsonUtilities.DefaultOptions;
        return JsonSerializer.SerializeToElement(this, jso.GetTypeInfo(typeof(DurableAgentSession)));
    }

    /// <summary>
    /// Deserializes a DurableAgentSession from JSON.
    /// </summary>
    /// <param name="serializedSession">The serialized session data.</param>
    /// <param name="jsonSerializerOptions">Optional JSON serializer options.</param>
    /// <returns>The deserialized DurableAgentSession.</returns>
    internal static DurableAgentSession Deserialize(JsonElement serializedSession, JsonSerializerOptions? jsonSerializerOptions = null)
    {
        if (!serializedSession.TryGetProperty("sessionId", out JsonElement sessionIdElement) ||
            sessionIdElement.ValueKind != JsonValueKind.String)
        {
            throw new JsonException("Invalid or missing sessionId property.");
        }

        string sessionIdString = sessionIdElement.GetString() ?? throw new JsonException("sessionId property is null.");
        AgentSessionId sessionId = AgentSessionId.Parse(sessionIdString);
        AgentSessionStateBag stateBag = serializedSession.TryGetProperty("stateBag", out JsonElement stateBagElement)
            ? AgentSessionStateBag.Deserialize(stateBagElement)
            : new AgentSessionStateBag();

        return new DurableAgentSession(sessionId, stateBag);
    }

    /// <inheritdoc/>
    public override object? GetService(Type serviceType, object? serviceKey = null)
    {
        if (serviceType == typeof(AgentSessionId))
        {
            return this.SessionId;
        }

        return base.GetService(serviceType, serviceKey);
    }

    /// <inheritdoc/>
    public override string ToString()
    {
        return this.SessionId.ToString();
    }

    [DebuggerBrowsable(DebuggerBrowsableState.Never)]
    private string DebuggerDisplay =>
        $"SessionId = {this.SessionId}, StateBag Count = {this.StateBag.Count}";
}
