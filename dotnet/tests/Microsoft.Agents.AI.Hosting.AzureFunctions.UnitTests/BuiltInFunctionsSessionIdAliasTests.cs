// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.Hosting.AzureFunctions.UnitTests;

/// <summary>
/// Tests for resolving the canonical <c>session_id</c> value and its deprecated <c>thread_id</c> alias
/// on incoming requests, and for ensuring only <c>session_id</c> is emitted on responses.
/// </summary>
public sealed class BuiltInFunctionsSessionIdAliasTests
{
    [Theory]
    [InlineData(null, null, null)]
    [InlineData("abc", null, "abc")]
    [InlineData(null, "abc", "abc")] // deprecated alias is still honored on its own
    [InlineData("abc", "abc", "abc")]
    [InlineData("", "abc", "abc")] // blank canonical value defers to the alias
    [InlineData("abc", "", "abc")] // blank alias defers to the canonical value
    [InlineData("   ", "abc", "abc")] // whitespace is treated as absent
    [InlineData("abc", "   ", "abc")]
    [InlineData("", "", null)]
    [InlineData("   ", null, null)]
    public void TryCombineSessionIdAliases_ResolvesValue(string? sessionId, string? threadId, string? expected)
    {
        // Act
        bool succeeded = BuiltInFunctions.TryCombineSessionIdAliases(sessionId, threadId, out string? result);

        // Assert
        Assert.True(succeeded);
        Assert.Equal(expected, result);
    }

    [Theory]
    [InlineData("abc", "def")]
    [InlineData("ABC", "abc")] // comparison is ordinal, so casing matters
    public void TryCombineSessionIdAliases_FailsOnConflict(string sessionId, string threadId)
    {
        // Act
        bool succeeded = BuiltInFunctions.TryCombineSessionIdAliases(sessionId, threadId, out string? result);

        // Assert
        Assert.False(succeeded);
        Assert.Null(result);
    }

    [Fact]
    public void AgentRunRequest_DeserializesBothAliases()
    {
        // Arrange
        const string Json = """{"message":"hi","session_id":"s1","thread_id":"t1"}""";

        // Act
        BuiltInFunctions.AgentRunRequest? request = JsonSerializer.Deserialize<BuiltInFunctions.AgentRunRequest>(Json);

        // Assert
        Assert.NotNull(request);
        Assert.Equal("hi", request.Message);
        Assert.Equal("s1", request.SessionId);
        Assert.Equal("t1", request.ThreadId);
    }

    [Fact]
    public void AgentRunRequest_DeserializesLegacyAliasOnly()
    {
        // Arrange
        const string Json = """{"message":"hi","thread_id":"t1"}""";

        // Act
        BuiltInFunctions.AgentRunRequest? request = JsonSerializer.Deserialize<BuiltInFunctions.AgentRunRequest>(Json);

        // Assert
        Assert.NotNull(request);
        Assert.Null(request.SessionId);
        Assert.Equal("t1", request.ThreadId);
    }

    [Fact]
    public void AgentRunSuccessResponse_EmitsOnlySessionId()
    {
        // Arrange
        AgentResponse agentResponse = new(new ChatMessage(ChatRole.Assistant, "hello"));
        BuiltInFunctions.AgentRunSuccessResponse response = new(200, "session-1", agentResponse);

        // Act
        using JsonDocument document = JsonDocument.Parse(JsonSerializer.Serialize(response));

        // Assert
        Assert.Equal("session-1", document.RootElement.GetProperty("session_id").GetString());
        Assert.Equal(200, document.RootElement.GetProperty("status").GetInt32());
        Assert.False(document.RootElement.TryGetProperty("thread_id", out _));
    }

    [Fact]
    public void AgentRunAcceptedResponse_EmitsOnlySessionId()
    {
        // Arrange
        BuiltInFunctions.AgentRunAcceptedResponse response = new(202, "session-2");

        // Act
        using JsonDocument document = JsonDocument.Parse(JsonSerializer.Serialize(response));

        // Assert
        Assert.Equal("session-2", document.RootElement.GetProperty("session_id").GetString());
        Assert.Equal(202, document.RootElement.GetProperty("status").GetInt32());
        Assert.False(document.RootElement.TryGetProperty("thread_id", out _));
    }

    [Theory]
    // bodySessionId, bodyThreadId, querySessionId, queryThreadId, expected
    [InlineData(null, null, null, null, null)]
    [InlineData("s", null, null, null, "s")]
    [InlineData(null, "t", null, null, "t")] // body-only deprecated alias
    [InlineData(null, null, null, "t", "t")] // query-only deprecated alias
    [InlineData(null, null, "s", null, "s")]
    [InlineData(null, "t", "t", null, "t")] // same value under different alias names
    [InlineData("s", null, null, "s", "s")]
    [InlineData("s", "s", "s", "s", "s")]
    [InlineData(null, "   ", null, "t", "t")] // blank body alias falls through to the query
    [InlineData("s", null, "   ", null, "s")] // blank query value does not conflict with the body
    public void TryResolveSessionKey_ResolvesValue(
        string? bodySessionId,
        string? bodyThreadId,
        string? querySessionId,
        string? queryThreadId,
        string? expected)
    {
        // Act
        bool succeeded = BuiltInFunctions.TryResolveSessionKey(
            bodySessionId, bodyThreadId, querySessionId, queryThreadId, out string? sessionKey, out string? error);

        // Assert
        Assert.True(succeeded);
        Assert.Null(error);
        Assert.Equal(expected, sessionKey);
    }

    [Theory]
    [InlineData("a", "b", null, null, "request body")]
    [InlineData(null, null, "a", "b", "query string")]
    [InlineData("a", null, "b", null, "both the query string and request body")]
    [InlineData(null, "a", null, "b", "both the query string and request body")]
    [InlineData(null, "a", "b", null, "both the query string and request body")] // mismatch across alias names
    [InlineData("a", null, null, "b", "both the query string and request body")]
    public void TryResolveSessionKey_FailsOnConflict(
        string? bodySessionId,
        string? bodyThreadId,
        string? querySessionId,
        string? queryThreadId,
        string expectedMessageFragment)
    {
        // Act
        bool succeeded = BuiltInFunctions.TryResolveSessionKey(
            bodySessionId, bodyThreadId, querySessionId, queryThreadId, out string? sessionKey, out string? error);

        // Assert
        Assert.False(succeeded);
        Assert.Null(sessionKey);
        Assert.NotNull(error);
        Assert.Contains(expectedMessageFragment, error, StringComparison.Ordinal);
    }
}
