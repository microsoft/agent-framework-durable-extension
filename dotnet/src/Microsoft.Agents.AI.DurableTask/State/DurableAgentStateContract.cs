// Copyright (c) Microsoft. All rights reserved.

using System.Globalization;
using System.Numerics;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Microsoft.Agents.AI.DurableTask.State;

internal static class DurableAgentStateContract
{
    private static readonly Regex s_rfc3339Pattern = new(
        @"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$",
        RegexOptions.CultureInvariant);

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

    public static bool IsJsonInteger(JsonElement value) =>
        value.ValueKind == JsonValueKind.Number && IsJsonInteger(value.GetRawText());

    public static bool IsNonNegativeInt64(JsonElement value) =>
        TryGetInt64(value, out long result) && result >= 0;

    public static bool IsPositiveInt64(JsonElement value) =>
        TryGetInt64(value, out long result) && result > 0;

    public static bool TryGetInt64(JsonElement value, out long result)
    {
        result = default;
        if (!IsJsonInteger(value))
        {
            return false;
        }

        string raw = value.GetRawText();
        int exponentIndex = raw.IndexOfAny('e', 'E');
        ReadOnlySpan<char> significand = exponentIndex >= 0
            ? raw.AsSpan(0, exponentIndex)
            : raw.AsSpan();
        BigInteger exponent = exponentIndex >= 0
            ? BigInteger.Parse(
                raw.AsSpan(exponentIndex + 1),
                NumberStyles.AllowLeadingSign,
                CultureInfo.InvariantCulture)
            : BigInteger.Zero;
        bool negative = significand[0] == '-';
        int decimalIndex = significand.IndexOf('.');
        int fractionalDigitCount = decimalIndex >= 0
            ? significand.Length - decimalIndex - 1
            : 0;
        StringBuilder digitsBuilder = new(significand.Length);
        foreach (char character in significand)
        {
            if (char.IsAsciiDigit(character))
            {
                digitsBuilder.Append(character);
            }
        }

        string digits = digitsBuilder.ToString().TrimStart('0');
        if (digits.Length == 0)
        {
            result = 0;
            return true;
        }

        BigInteger scale = exponent - fractionalDigitCount;
        if (scale < 0)
        {
            BigInteger digitsToRemove = -scale;
            if (digitsToRemove > digits.Length)
            {
                return false;
            }

            digits = digits[..^(int)digitsToRemove];
        }
        else if (scale > 0)
        {
            if (scale > 19)
            {
                return false;
            }

            digits += new string('0', (int)scale);
        }

        digits = digits.TrimStart('0');
        if (digits.Length == 0)
        {
            result = 0;
            return true;
        }

        string integerText = negative ? $"-{digits}" : digits;
        return long.TryParse(
            integerText,
            NumberStyles.AllowLeadingSign,
            CultureInfo.InvariantCulture,
            out result);
    }

    public static bool IsOffsetRfc3339(string? value)
    {
        if (string.IsNullOrEmpty(value) ||
            !s_rfc3339Pattern.IsMatch(value) ||
            !DateTimeOffset.TryParse(
                value,
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out _))
        {
            return false;
        }

        return value.EndsWith('Z') ||
            (value.Length >= 6 &&
             value[^6] is '+' or '-' &&
             value[^3] == ':');
    }

    public static DateTimeOffset ParseOffsetRfc3339(string value, string propertyName)
    {
        if (!IsOffsetRfc3339(value))
        {
            throw new JsonException(
                $"The durable agent state '{propertyName}' property must be an RFC 3339 date-time with an explicit offset.");
        }

        return DateTimeOffset.Parse(
            value,
            CultureInfo.InvariantCulture,
            DateTimeStyles.None);
    }

    private static bool IsJsonInteger(string value)
    {
        int exponentIndex = value.IndexOfAny('e', 'E');
        ReadOnlySpan<char> significand = exponentIndex >= 0 ? value.AsSpan(0, exponentIndex) : value;
        ReadOnlySpan<char> exponentText = exponentIndex >= 0 ? value.AsSpan(exponentIndex + 1) : default;
        int decimalIndex = significand.IndexOf('.');
        int fractionalDigitCount = decimalIndex >= 0 ? significand.Length - decimalIndex - 1 : 0;
        int trailingZeroCount = 0;
        bool hasNonZeroDigit = false;
        for (int index = significand.Length - 1; index >= 0; index--)
        {
            char character = significand[index];
            if (character is '.' or '-')
            {
                continue;
            }

            if (character == '0' && !hasNonZeroDigit)
            {
                trailingZeroCount++;
            }
            else
            {
                hasNonZeroDigit = true;
            }
        }

        if (!hasNonZeroDigit)
        {
            return true;
        }

        BigInteger exponent = exponentText.IsEmpty
            ? BigInteger.Zero
            : BigInteger.Parse(
                exponentText,
                NumberStyles.AllowLeadingSign,
                CultureInfo.InvariantCulture);
        return exponent >= fractionalDigitCount - trailingZeroCount;
    }
}
