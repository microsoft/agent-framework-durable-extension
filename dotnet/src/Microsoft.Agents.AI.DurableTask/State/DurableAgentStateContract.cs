// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask.State;

internal static class DurableAgentStateContract
{
    public const int MaxIdentifierLength = 256;
    public const int MaxMetadataKeyLength = 256;
    public const int MaxMetadataStringLength = 16 * 1024;

    public static void ValidateIdentifier(string? value, string propertyName)
    {
        if (string.IsNullOrWhiteSpace(value) ||
            value.EnumerateRunes().Take(MaxIdentifierLength + 1).Count() > MaxIdentifierLength ||
            value.Any(char.IsControl))
        {
            throw new InvalidOperationException(
                $"The durable agent state '{propertyName}' property must be a non-empty string of at most {MaxIdentifierLength} characters without control characters.");
        }
    }
}
