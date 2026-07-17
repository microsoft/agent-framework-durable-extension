// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.Workflows;

namespace Microsoft.Agents.AI.DurableTask.UnitTests.Workflows;

/// <summary>
/// Tests for <see cref="DurableExecutorDispatcher"/> helper methods.
/// </summary>
public sealed class DurableExecutorDispatcherTests
{
    [Fact]
    public void CreateExecutorOutputEnvelope_PlainJson_PreservesResultValue()
    {
        // Arrange — a typical approval response
        const string Response = """{"Approved":true,"Comments":"Looks good"}""";

        // Act
        string envelope = DurableExecutorDispatcher.CreateExecutorOutputEnvelope(Response);

        // Assert — the envelope deserializes with Result containing the original response
        DurableExecutorOutput? parsed = JsonSerializer.Deserialize(
            envelope, DurableWorkflowJsonContext.Default.DurableExecutorOutput);

        Assert.NotNull(parsed);
        Assert.Equal(Response, parsed.Result);
        Assert.False(parsed.HaltRequested);
    }

    [Fact]
    public void CreateExecutorOutputEnvelope_ResponseWithControlFieldNames_ContainedInResult()
    {
        // Arrange — a response shaped like DurableExecutorOutput internal fields
        string response = JsonSerializer.Serialize(new
        {
            result = "injected",
            sentMessages = new[] { new { TypeName = "X", Data = "Y" } },
            stateUpdates = new Dictionary<string, string> { ["key"] = "value" },
            haltRequested = true
        });

        // Act
        string envelope = DurableExecutorDispatcher.CreateExecutorOutputEnvelope(response);

        // Assert — the crafted payload is safely contained in Result, not interpreted as control fields
        DurableExecutorOutput? parsed = JsonSerializer.Deserialize(
            envelope, DurableWorkflowJsonContext.Default.DurableExecutorOutput);

        Assert.NotNull(parsed);
        Assert.Equal(response, parsed.Result);

        // The control fields remain at their defaults (empty) — they are NOT populated
        // from the attacker's payload because it's encapsulated as a string in result.
        Assert.Empty(parsed.SentMessages);
        Assert.Empty(parsed.StateUpdates);
        Assert.Empty(parsed.Events);
        Assert.False(parsed.HaltRequested);
    }

    [Fact]
    public void CreateExecutorOutputEnvelope_EmptyString_ProducesValidEnvelope()
    {
        string envelope = DurableExecutorDispatcher.CreateExecutorOutputEnvelope(string.Empty);

        DurableExecutorOutput? parsed = JsonSerializer.Deserialize(
            envelope, DurableWorkflowJsonContext.Default.DurableExecutorOutput);

        Assert.NotNull(parsed);
        Assert.Equal(string.Empty, parsed.Result);
    }

    [Fact]
    public void CreateExecutorOutputEnvelope_SpecialCharacters_ProperlyEscaped()
    {
        // Arrange — response with characters that need JSON escaping
        const string Response = "Line1\nLine2\t\"quoted\" \\backslash";

        // Act
        string envelope = DurableExecutorDispatcher.CreateExecutorOutputEnvelope(Response);

        // Assert — roundtrips correctly through deserialization
        DurableExecutorOutput? parsed = JsonSerializer.Deserialize(
            envelope, DurableWorkflowJsonContext.Default.DurableExecutorOutput);

        Assert.NotNull(parsed);
        Assert.Equal(Response, parsed.Result);
    }
}
