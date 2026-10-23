// Copyright (c) Microsoft. All rights reserved.

using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.DurableTask.Entities;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Represents an agent session ID, which is used to identify a long-running agent session.
/// </summary>
[JsonConverter(typeof(AgentSessionIdJsonConverter))]
public readonly struct AgentSessionId : IEquatable<AgentSessionId>
{
    internal const int CurrentEntityVersion = 2;
    internal const string EntityNamePrefix = "dafx-v2-";
    internal const string LegacyEntityNamePrefix = "dafx-";
    internal const string ReservedVersionPrefix = "v2-";
    private readonly EntityInstanceId _entityId;

    /// <summary>
    /// Initializes a new instance of the <see cref="AgentSessionId"/> struct.
    /// </summary>
    /// <param name="name">The name of the agent that owns the session (case-insensitive).</param>
    /// <param name="key">The unique key of the agent session (case-sensitive).</param>
    public AgentSessionId(string name, string key)
    {
        this.Name = name;
        this.EntityVersion = CurrentEntityVersion;
        this._entityId = new EntityInstanceId(ToEntityName(name, CurrentEntityVersion), key);
    }

    private AgentSessionId(string name, EntityInstanceId entityId, int entityVersion)
    {
        this.Name = name;
        this._entityId = entityId;
        this.EntityVersion = entityVersion;
    }

    /// <summary>
    /// Gets the name of the agent that owns the session. Names are case-insensitive.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the unique key of the agent session. Keys are case-sensitive and are used to identify the session.
    /// </summary>
    public string Key => this._entityId.Key;

    /// <summary>
    /// Gets the entity protocol version encoded in the underlying entity name (<c>1</c> for <c>dafx-{name}</c>,
    /// or <c>N</c> for <c>dafx-v{N}-{name}</c>).
    /// </summary>
    internal int EntityVersion { get; }

    /// <summary>
    /// Gets a value indicating whether this session ID targets a prior-version entity (<c>EntityVersion &lt; CurrentEntityVersion</c>).
    /// </summary>
    internal bool IsLegacyEntity => this.EntityVersion < CurrentEntityVersion;

    /// <summary>
    /// Converts an agent name to its underlying current-version entity name representation (<c>dafx-v2-{name}</c>).
    /// </summary>
    /// <param name="name">The agent name.</param>
    /// <returns>The current-version entity name used by Durable Task for this agent.</returns>
    internal static string ToEntityName(string name) => ToEntityName(name, CurrentEntityVersion);

    /// <summary>
    /// Converts an agent name to its underlying entity name representation for the specified entity version
    /// (<c>dafx-{name}</c> for version 1, or <c>dafx-v{version}-{name}</c> for version 2 and above).
    /// </summary>
    /// <param name="name">The agent name.</param>
    /// <param name="version">The entity protocol version (<c>&gt;= 1</c>).</param>
    /// <returns>The entity name used by Durable Task for this agent and version.</returns>
    internal static string ToEntityName(string name, int version)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(version, 1);
        return version == 1
            ? $"{LegacyEntityNamePrefix}{name}"
            : $"{LegacyEntityNamePrefix}v{version.ToString(CultureInfo.InvariantCulture)}-{name}";
    }

    /// <summary>
    /// Converts an agent name to its legacy v1 entity name representation (<c>dafx-{name}</c>).
    /// </summary>
    /// <param name="name">The agent name.</param>
    /// <returns>The legacy v1 entity name used by Durable Task for this agent.</returns>
    internal static string ToLegacyEntityName(string name) => ToEntityName(name, version: 1);

    /// <summary>
    /// Creates a legacy v1 <see cref="AgentSessionId"/> targeting <c>@dafx-{name}@{key}</c>.
    /// </summary>
    /// <param name="name">The name of the agent that owns the session.</param>
    /// <param name="key">The unique key of the agent session.</param>
    /// <returns>A legacy <see cref="AgentSessionId"/> targeting <c>@dafx-{name}@{key}</c>.</returns>
    internal static AgentSessionId CreateLegacy(string name, string key) =>
        new(name, new EntityInstanceId(ToEntityName(name, version: 1), key), entityVersion: 1);

    /// <summary>
    /// Creates a copy of this <see cref="AgentSessionId"/> targeting the specified entity protocol version.
    /// </summary>
    internal AgentSessionId WithVersion(int version) =>
        version == this.EntityVersion
            ? this
            : new(this.Name, new EntityInstanceId(ToEntityName(this.Name, version), this.Key), version);

    /// <summary>
    /// Determines whether the specified agent name begins with a reserved <c>v{digits}-</c> version prefix.
    /// </summary>
    internal static bool HasReservedVersionPrefix(ReadOnlySpan<char> value, out int dashIndex)
    {
        dashIndex = -1;
        if (value.Length < 3 || value[0] is not ('v' or 'V'))
        {
            return false;
        }

        int separator = value.IndexOf('-');
        if (separator < 2)
        {
            return false;
        }

        for (int index = 1; index < separator; index++)
        {
            if (value[index] is < '0' or > '9')
            {
                return false;
            }
        }

        dashIndex = separator;
        return true;
    }

    /// <summary>
    /// Converts the <see cref="AgentSessionId"/> to an <see cref="EntityInstanceId"/>.
    /// </summary>
    /// <returns>The <see cref="EntityInstanceId"/> representation of the <see cref="AgentSessionId"/>.</returns>
    internal EntityInstanceId ToEntityId() => this._entityId;

    /// <summary>
    /// Creates a new <see cref="AgentSessionId"/> with the specified name and a randomly generated key.
    /// </summary>
    /// <param name="name">The name of the agent that owns the session.</param>
    /// <returns>A new <see cref="AgentSessionId"/> with the specified name and a random key.</returns>
    public static AgentSessionId WithRandomKey(string name) =>
        new(name, Guid.NewGuid().ToString("N"));

    /// <summary>
    /// Determines whether two <see cref="AgentSessionId"/> instances are equal.
    /// </summary>
    /// <param name="left">The first <see cref="AgentSessionId"/> to compare.</param>
    /// <param name="right">The second <see cref="AgentSessionId"/> to compare.</param>
    /// <returns><c>true</c> if the two instances are equal; otherwise, <c>false</c>.</returns>
    public static bool operator ==(AgentSessionId left, AgentSessionId right) =>
        left._entityId == right._entityId;

    /// <summary>
    /// Determines whether two <see cref="AgentSessionId"/> instances are not equal.
    /// </summary>
    /// <param name="left">The first <see cref="AgentSessionId"/> to compare.</param>
    /// <param name="right">The second <see cref="AgentSessionId"/> to compare.</param>
    /// <returns><c>true</c> if the two instances are not equal; otherwise, <c>false</c>.</returns>
    public static bool operator !=(AgentSessionId left, AgentSessionId right) =>
        left._entityId != right._entityId;

    /// <summary>
    /// Determines whether the specified <see cref="AgentSessionId"/> is equal to the current <see cref="AgentSessionId"/>.
    /// </summary>
    /// <param name="other">The <see cref="AgentSessionId"/> to compare with the current <see cref="AgentSessionId"/>.</param>
    /// <returns><c>true</c> if the specified <see cref="AgentSessionId"/> is equal to the current <see cref="AgentSessionId"/>; otherwise, <c>false</c>.</returns>
    public bool Equals(AgentSessionId other) => this == other;

    /// <summary>
    /// Determines whether the specified object is equal to the current <see cref="AgentSessionId"/>.
    /// </summary>
    /// <param name="obj">The object to compare with the current <see cref="AgentSessionId"/>.</param>
    /// <returns><c>true</c> if the specified object is equal to the current <see cref="AgentSessionId"/>; otherwise, <c>false</c>.</returns>
    public override bool Equals(object? obj) => obj is AgentSessionId other && this == other;

    /// <summary>
    /// Returns the hash code for this <see cref="AgentSessionId"/>.
    /// </summary>
    /// <returns>A hash code for the current <see cref="AgentSessionId"/>.</returns>
    public override int GetHashCode() => this._entityId.GetHashCode();

    /// <summary>
    /// Returns a string representation of this <see cref="AgentSessionId"/> in the form of @name@key.
    /// </summary>
    /// <returns>A string representation of the current <see cref="AgentSessionId"/>.</returns>
    public override string ToString() => this._entityId.ToString();

    /// <summary>
    /// Converts the string representation of an agent session ID to its <see cref="AgentSessionId"/> equivalent.
    /// The input string must be in the form of @name@key.
    /// </summary>
    /// <param name="sessionIdString">A string containing an agent session ID to convert.</param>
    /// <returns>A <see cref="AgentSessionId"/> equivalent to the agent session ID contained in <paramref name="sessionIdString"/>.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="sessionIdString"/> is not a valid agent session ID format.</exception>
    public static AgentSessionId Parse(string sessionIdString)
    {
        EntityInstanceId entityId = EntityInstanceId.FromString(sessionIdString);
        return FromEntityId(entityId, nameof(sessionIdString), sessionIdString);
    }

    /// <summary>
    /// Implicitly converts an <see cref="AgentSessionId"/> to an <see cref="EntityInstanceId"/>.
    /// This conversion is useful for entity API interoperability.
    /// </summary>
    /// <param name="agentSessionId">The <see cref="AgentSessionId"/> to convert.</param>
    /// <returns>The equivalent <see cref="EntityInstanceId"/>.</returns>
    public static implicit operator EntityInstanceId(AgentSessionId agentSessionId) => agentSessionId.ToEntityId();

    /// <summary>
    /// Implicitly converts an <see cref="EntityInstanceId"/> to an <see cref="AgentSessionId"/>.
    /// </summary>
    /// <param name="entityId">The <see cref="EntityInstanceId"/> to convert.</param>
    /// <returns>The equivalent <see cref="AgentSessionId"/>.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1065:Do not raise exceptions in unexpected locations", Justification = "Implicit conversion must validate format.")]
    public static implicit operator AgentSessionId(EntityInstanceId entityId)
    {
        return FromEntityId(entityId, nameof(entityId), entityId.ToString());
    }

    private static AgentSessionId FromEntityId(EntityInstanceId entityId, string paramName, string rawValue)
    {
        if (entityId.Name.StartsWith(LegacyEntityNamePrefix, StringComparison.OrdinalIgnoreCase))
        {
            ReadOnlySpan<char> remainder = entityId.Name.AsSpan(LegacyEntityNamePrefix.Length);
            if (HasReservedVersionPrefix(remainder, out int dashIndex))
            {
                ReadOnlySpan<char> versionDigits = remainder[1..dashIndex];
                if (versionDigits[0] != '0' &&
                    int.TryParse(versionDigits, NumberStyles.None, CultureInfo.InvariantCulture, out int version) &&
                    version >= 2)
                {
                    string agentName = remainder[(dashIndex + 1)..].ToString();
                    if (agentName.Length > 0)
                    {
                        return new AgentSessionId(agentName, entityId, version);
                    }
                }
            }
            else if (remainder.Length > 0)
            {
                return new AgentSessionId(remainder.ToString(), entityId, entityVersion: 1);
            }
        }

        throw new ArgumentException($"'{rawValue}' is not a valid agent session ID.", paramName);
    }

    /// <summary>
    /// Custom JSON converter for <see cref="AgentSessionId"/> to ensure proper serialization and deserialization.
    /// </summary>
    public sealed class AgentSessionIdJsonConverter : JsonConverter<AgentSessionId>
    {
        /// <inheritdoc/>
        public override AgentSessionId Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.String)
            {
                throw new JsonException("Expected string value");
            }

            string value = reader.GetString() ?? string.Empty;

            return Parse(value);
        }

        /// <inheritdoc/>
        public override void Write(Utf8JsonWriter writer, AgentSessionId value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value.ToString());
        }
    }
}
