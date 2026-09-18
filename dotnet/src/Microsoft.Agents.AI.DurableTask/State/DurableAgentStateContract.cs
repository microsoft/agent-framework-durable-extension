// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask.State;

internal static class DurableAgentStateContract
{
    public const int MaxIdentifierLength = 256;
    public const int MaxMetadataKeyLength = 256;
    public const int MaxMetadataStringLength = 16 * 1024;

    /// <summary>
    /// Validates an identifier before it is stored in durable state.
    /// </summary>
    /// <param name="value">The identifier value to validate.</param>
    /// <param name="propertyPath">
    /// The durable-state field path to include in any validation error.
    /// This parameter identifies the source of the value; it is not itself validated.
    /// </param>
    public static void ValidateIdentifier(string? value, string propertyPath)
    {
        if (string.IsNullOrWhiteSpace(value) ||
            value.EnumerateRunes().Take(MaxIdentifierLength + 1).Count() > MaxIdentifierLength ||
            value.Any(char.IsControl))
        {
            throw new InvalidOperationException(
                $"The durable agent state '{propertyPath}' property must be a non-empty string of at most {MaxIdentifierLength} characters without control characters.");
        }
    }
}
