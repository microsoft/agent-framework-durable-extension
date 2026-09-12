// Copyright (c) Microsoft. All rights reserved.

using CustomHistoryProvider;

namespace CustomHistoryProviderTests;

public sealed class MarkerInputTests
{
    [Fact]
    public void AcceptsMarkerAtUtf8ByteLimit()
    {
        string marker = new('a', MarkerInput.MaximumUtf8Bytes);

        bool accepted = MarkerInput.TryValidate(marker, out string errorMessage);

        Assert.True(accepted);
        Assert.Empty(errorMessage);
    }

    [Fact]
    public void RejectsMarkerOverUtf8ByteLimit()
    {
        string marker = new('a', MarkerInput.MaximumUtf8Bytes + 1);

        bool accepted = MarkerInput.TryValidate(marker, out string errorMessage);

        Assert.False(accepted);
        Assert.Contains("1024 UTF-8 bytes", errorMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void RejectsMultibyteMarkerOverUtf8ByteLimit()
    {
        string marker = new('\u20ac', (MarkerInput.MaximumUtf8Bytes / 3) + 1);

        bool accepted = MarkerInput.TryValidate(marker, out string errorMessage);

        Assert.False(accepted);
        Assert.Contains("1026", errorMessage, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void RejectsMissingMarker(string? marker)
    {
        bool accepted = MarkerInput.TryValidate(marker, out string errorMessage);

        Assert.False(accepted);
        Assert.Equal("A marker is required.", errorMessage);
    }
}
