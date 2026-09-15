// Copyright (c) Microsoft. All rights reserved.

using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace CustomHistoryProvider;

/// <summary>
/// Validates the short marker used by the custom history provider sample.
/// </summary>
public static class MarkerInput
{
    /// <summary>
    /// Maximum marker size in UTF-8 bytes.
    /// </summary>
    public const int MaximumUtf8Bytes = 1024;

    private static readonly Encoding s_strictUtf8 = new UTF8Encoding(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    /// <summary>
    /// Checks whether a marker is present, valid Unicode, and within the sample's byte limit.
    /// </summary>
    public static bool TryValidate(
        [NotNullWhen(true)] string? marker,
        out string errorMessage)
    {
        if (string.IsNullOrWhiteSpace(marker))
        {
            errorMessage = "A marker is required.";
            return false;
        }

        int markerBytes;
        try
        {
            markerBytes = s_strictUtf8.GetByteCount(marker);
        }
        catch (EncoderFallbackException)
        {
            errorMessage = "The marker must contain valid Unicode text.";
            return false;
        }

        if (markerBytes > MaximumUtf8Bytes)
        {
            errorMessage =
                $"The marker must be at most {MaximumUtf8Bytes} UTF-8 bytes; received {markerBytes}.";
            return false;
        }

        errorMessage = string.Empty;
        return true;
    }
}
