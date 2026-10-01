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
    // This bounds materializing compact exponent notation. Durable raw integer tokens remain unbounded.
    internal const int MaxExpandedIntegerDigits = 4_096;

    /// <summary>
    /// Validates an identifier before it is stored in durable state.
    /// </summary>
    /// <param name="value">The identifier value to validate.</param>
    /// <param name="diagnosticPath">
    /// A compiler-checked label identifying the source of the value in validation errors. This label is diagnostic
    /// only: it is not parsed as a JSON path and is not itself validated.
    /// </param>
    public static void ValidateIdentifier(string? value, string diagnosticPath)
    {
        if (string.IsNullOrWhiteSpace(value) ||
            value.EnumerateRunes().Take(MaxIdentifierLength + 1).Count() > MaxIdentifierLength ||
            value.Any(char.IsControl))
        {
            throw new InvalidOperationException(
                $"The durable agent state '{diagnosticPath}' property must be a non-empty string of at most {MaxIdentifierLength} characters without control characters.");
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
        if (!TryGetBigInteger(value, maximumExpandedDigits: 19, out BigInteger integer) ||
            integer < long.MinValue ||
            integer > long.MaxValue)
        {
            return false;
        }

        result = (long)integer;
        return true;
    }

    public static bool TryGetBigInteger(
        JsonElement value,
        out BigInteger result) =>
        TryGetBigInteger(value, MaxExpandedIntegerDigits, out result);

    private static bool TryGetBigInteger(
        JsonElement value,
        int maximumExpandedDigits,
        out BigInteger result)
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
        ReadOnlySpan<char> exponentText = exponentIndex >= 0
            ? raw.AsSpan(exponentIndex + 1)
            : default;
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
            result = BigInteger.Zero;
            return true;
        }

        if (!TryParseJsonExponent(
            exponentText,
            maximumPositiveMagnitude: (long)fractionalDigitCount + maximumExpandedDigits,
            maximumNegativeMagnitude: Math.Max(
                0,
                (long)digits.Length - fractionalDigitCount),
            out long exponent))
        {
            return false;
        }

        long scale = exponent - fractionalDigitCount;
        if (scale < 0)
        {
            long digitsToRemove = -scale;
            if (digitsToRemove > digits.Length)
            {
                return false;
            }

            digits = digits[..^(int)digitsToRemove];
        }
        else if (scale > 0)
        {
            if (scale > maximumExpandedDigits ||
                digits.Length + scale > maximumExpandedDigits)
            {
                return false;
            }

            digits += new string('0', (int)scale);
        }

        digits = digits.TrimStart('0');
        if (digits.Length > maximumExpandedDigits)
        {
            return false;
        }
        if (digits.Length == 0)
        {
            result = BigInteger.Zero;
            return true;
        }

        string integerText = negative ? $"-{digits}" : digits;
        return BigInteger.TryParse(
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

        return IsJsonExponentAtLeast(
            exponentText,
            fractionalDigitCount - trailingZeroCount);
    }

    private static bool IsJsonExponentAtLeast(
        ReadOnlySpan<char> exponentText,
        int minimum)
    {
        if (exponentText.IsEmpty)
        {
            return minimum <= 0;
        }

        bool negative = exponentText[0] == '-';
        int digitIndex = exponentText[0] is '+' or '-' ? 1 : 0;
        while (digitIndex < exponentText.Length &&
            exponentText[digitIndex] == '0')
        {
            digitIndex++;
        }

        if (digitIndex == exponentText.Length)
        {
            return minimum <= 0;
        }

        ReadOnlySpan<char> magnitude = exponentText[digitIndex..];
        if (!negative)
        {
            return minimum <= 0 ||
                CompareDecimalMagnitude(magnitude, minimum) >= 0;
        }

        return minimum < 0 &&
            CompareDecimalMagnitude(magnitude, -(long)minimum) <= 0;
    }

    private static int CompareDecimalMagnitude(
        ReadOnlySpan<char> digits,
        long value)
    {
        string valueText = value.ToString(CultureInfo.InvariantCulture);
        int lengthComparison = digits.Length.CompareTo(valueText.Length);
        return lengthComparison != 0
            ? lengthComparison
            : digits.SequenceCompareTo(valueText.AsSpan());
    }

    private static bool TryParseJsonExponent(
        ReadOnlySpan<char> exponentText,
        long maximumPositiveMagnitude,
        long maximumNegativeMagnitude,
        out long result)
    {
        result = 0;
        if (exponentText.IsEmpty)
        {
            return true;
        }

        bool negative = exponentText[0] == '-';
        long maximumMagnitude = negative
            ? maximumNegativeMagnitude
            : maximumPositiveMagnitude;
        int digitIndex = exponentText[0] is '+' or '-' ? 1 : 0;
        long magnitude = 0;
        for (; digitIndex < exponentText.Length; digitIndex++)
        {
            int digit = exponentText[digitIndex] - '0';
            if ((uint)digit > 9 ||
                magnitude > maximumMagnitude / 10 ||
                magnitude == maximumMagnitude / 10 &&
                digit > maximumMagnitude % 10)
            {
                return false;
            }

            magnitude = (magnitude * 10) + digit;
        }

        result = negative ? -magnitude : magnitude;
        return true;
    }
}
