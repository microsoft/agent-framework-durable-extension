// Copyright (c) Microsoft. All rights reserved.

using System.Numerics;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Centralizes strict parsing, supported-major validation, and ordering for durable state versions.
/// </summary>
internal readonly record struct DurableAgentStateSchemaVersion(BigInteger Major, BigInteger Minor, BigInteger Patch)
    : IComparable<DurableAgentStateSchemaVersion>
{
    private static readonly HashSet<string> s_supportedVersions =
    [
        "1.0.0",
        "1.1.0",
        DurableAgentState.CurrentSchemaVersion,
        DurableAgentState.RevisedSchemaVersion,
    ];

    /// <summary>
    /// Parses and validates a supported durable agent state schema version.
    /// </summary>
    public static DurableAgentStateSchemaVersion ParseSupported(string? value)
    {
        if (value is null || !s_supportedVersions.Contains(value))
        {
            throw new InvalidOperationException($"The durable agent state schema version '{value}' is not supported.");
        }

        _ = TryParse(value, out DurableAgentStateSchemaVersion version);
        return version;
    }

    /// <summary>
    /// Parses the schema's strict numeric <c>major.minor.patch</c> grammar.
    /// </summary>
    public static bool TryParse(string? value, out DurableAgentStateSchemaVersion version)
    {
        version = default;
        if (string.IsNullOrEmpty(value))
        {
            return false;
        }

        ReadOnlySpan<char> remaining = value.AsSpan();
        if (!TryReadComponent(ref remaining, out BigInteger major) ||
            !TryReadComponent(ref remaining, out BigInteger minor) ||
            !TryReadFinalComponent(remaining, out BigInteger patch))
        {
            return false;
        }

        version = new(major, minor, patch);
        return true;
    }

    /// <inheritdoc/>
    public int CompareTo(DurableAgentStateSchemaVersion other)
    {
        int majorComparison = this.Major.CompareTo(other.Major);
        if (majorComparison != 0)
        {
            return majorComparison;
        }

        int minorComparison = this.Minor.CompareTo(other.Minor);
        return minorComparison != 0
            ? minorComparison
            : this.Patch.CompareTo(other.Patch);
    }

    private static bool TryReadComponent(ref ReadOnlySpan<char> value, out BigInteger component)
    {
        int separatorIndex = value.IndexOf('.');
        if (separatorIndex <= 0 ||
            !TryParseNumericIdentifier(value[..separatorIndex], out component))
        {
            component = default;
            return false;
        }

        value = value[(separatorIndex + 1)..];
        return true;
    }

    private static bool TryReadFinalComponent(ReadOnlySpan<char> value, out BigInteger component)
    {
        component = default;
        return value.IndexOf('.') < 0 && TryParseNumericIdentifier(value, out component);
    }

    private static bool TryParseNumericIdentifier(ReadOnlySpan<char> value, out BigInteger component)
    {
        component = 0;
        if (value.IsEmpty || (value.Length > 1 && value[0] == '0'))
        {
            return false;
        }

        foreach (char character in value)
        {
            if (character is < '0' or > '9')
            {
                return false;
            }

            component = (component * 10) + (character - '0');
        }

        return true;
    }
}
