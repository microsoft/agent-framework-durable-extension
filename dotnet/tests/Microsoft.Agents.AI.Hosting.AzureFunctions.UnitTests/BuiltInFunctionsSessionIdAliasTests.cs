// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.AI;
using Moq;

namespace Microsoft.Agents.AI.Hosting.AzureFunctions.UnitTests;

/// <summary>
/// Tests for resolving the canonical <c>sessionId</c> value and deprecated aliases
/// on incoming requests, and for the transitional response shape.
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
    public void AgentRunRequest_DeserializesCamelCaseAndLegacyAliases()
    {
        // Arrange
        const string Json = """{"message":"hi","sessionId":"s1","session_id":"legacy-s1","thread_id":"t1"}""";

        // Act
        BuiltInFunctions.AgentRunRequest? request = JsonSerializer.Deserialize<BuiltInFunctions.AgentRunRequest>(Json);

        // Assert
        Assert.NotNull(request);
        Assert.Equal("hi", request.Message);
        Assert.Equal("s1", request.SessionId);
        Assert.Equal("legacy-s1", request.LegacySessionId);
        Assert.Equal("t1", request.ThreadId);
    }

    [Fact]
    public void AgentRunRequest_DeserializesLegacySessionIdOnly()
    {
        // Arrange
        const string Json = """{"message":"hi","session_id":"s1"}""";

        // Act
        BuiltInFunctions.AgentRunRequest? request = JsonSerializer.Deserialize<BuiltInFunctions.AgentRunRequest>(Json);

        // Assert
        Assert.NotNull(request);
        Assert.Null(request.SessionId);
        Assert.Equal("s1", request.LegacySessionId);
        Assert.Null(request.ThreadId);
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
        Assert.Null(request.LegacySessionId);
        Assert.Equal("t1", request.ThreadId);
    }

    [Fact]
    public void AgentRunSuccessResponse_EmitsCamelCaseAndLegacySessionIdDuringTransition()
    {
        // Arrange
        AgentResponse agentResponse = new(new ChatMessage(ChatRole.Assistant, "hello"));
        BuiltInFunctions.AgentRunSuccessResponse response = new(200, "session-1", "session-1", agentResponse);

        // Act
        using JsonDocument document = JsonDocument.Parse(JsonSerializer.Serialize(response));

        // Assert
        Assert.Equal("session-1", document.RootElement.GetProperty("sessionId").GetString());
        Assert.Equal("session-1", document.RootElement.GetProperty("session_id").GetString());
        Assert.Equal(200, document.RootElement.GetProperty("status").GetInt32());
        Assert.False(document.RootElement.TryGetProperty("thread_id", out _));
    }

    [Fact]
    public void AgentRunAcceptedResponse_EmitsCamelCaseAndLegacySessionIdDuringTransition()
    {
        // Arrange
        BuiltInFunctions.AgentRunAcceptedResponse response = new(202, "session-2", "session-2");

        // Act
        using JsonDocument document = JsonDocument.Parse(JsonSerializer.Serialize(response));

        // Assert
        Assert.Equal("session-2", document.RootElement.GetProperty("sessionId").GetString());
        Assert.Equal("session-2", document.RootElement.GetProperty("session_id").GetString());
        Assert.Equal(202, document.RootElement.GetProperty("status").GetInt32());
        Assert.False(document.RootElement.TryGetProperty("thread_id", out _));
    }

    [Theory]
    // bodySessionId, bodyLegacySessionId, bodyThreadId, querySessionId, queryLegacySessionId, queryThreadId, expected
    [InlineData(null, null, null, null, null, null, null)]
    [InlineData("s", null, null, null, null, null, "s")]
    [InlineData(null, "legacy", null, null, null, null, "legacy")]
    [InlineData(null, null, "t", null, null, null, "t")] // body-only deprecated alias
    [InlineData(null, null, null, null, null, "t", "t")] // query-only deprecated alias
    [InlineData(null, null, null, "s", null, null, "s")]
    [InlineData(null, null, "t", "t", null, null, "t")] // same value under different alias names
    [InlineData("s", null, null, null, null, "s", "s")]
    [InlineData("s", "s", "s", "s", "s", "s", "s")]
    [InlineData(null, null, "   ", null, null, "t", "t")] // blank body alias falls through to the query
    [InlineData("s", null, null, "   ", null, null, "s")] // blank query value does not conflict with the body
    public void TryResolveSessionKey_ResolvesValue(
        string? bodySessionId,
        string? bodyLegacySessionId,
        string? bodyThreadId,
        string? querySessionId,
        string? queryLegacySessionId,
        string? queryThreadId,
        string? expected)
    {
        // Act
        bool succeeded = BuiltInFunctions.TryResolveSessionKey(
            bodySessionId,
            bodyLegacySessionId,
            bodyThreadId,
            querySessionId,
            queryLegacySessionId,
            queryThreadId,
            out string? sessionKey,
            out string? error);

        // Assert
        Assert.True(succeeded);
        Assert.Null(error);
        Assert.Equal(expected, sessionKey);
    }

    [Theory]
    [InlineData("a", "b", null, null, null, null, "request body")]
    [InlineData("a", null, "b", null, null, null, "request body")]
    [InlineData(null, null, null, "a", "b", null, "query string")]
    [InlineData("a", null, null, "b", null, null, "both the query string and request body")]
    [InlineData(null, null, "a", null, null, "b", "both the query string and request body")]
    [InlineData(null, null, "a", "b", null, null, "both the query string and request body")] // mismatch across alias names
    [InlineData("a", null, null, null, null, "b", "both the query string and request body")]
    public void TryResolveSessionKey_FailsOnConflict(
        string? bodySessionId,
        string? bodyLegacySessionId,
        string? bodyThreadId,
        string? querySessionId,
        string? queryLegacySessionId,
        string? queryThreadId,
        string expectedMessageFragment)
    {
        // Act
        bool succeeded = BuiltInFunctions.TryResolveSessionKey(
            bodySessionId,
            bodyLegacySessionId,
            bodyThreadId,
            querySessionId,
            queryLegacySessionId,
            queryThreadId,
            out string? sessionKey,
            out string? error);

        // Assert
        Assert.False(succeeded);
        Assert.Null(sessionKey);
        Assert.NotNull(error);
        Assert.Contains(expectedMessageFragment, error, StringComparison.Ordinal);
    }

    [Fact]
    public void AddAgentHttpDeprecationHeaders_AddsMigrationHeadersOnlyForLegacyRequests()
    {
        // Arrange
        Mock<HttpResponseData> response = new(Mock.Of<FunctionContext>());
        response.SetupGet(r => r.Headers).Returns(new HttpHeadersCollection());

        // Act
        BuiltInFunctions.AddAgentHttpDeprecationHeaders(response.Object, shouldAdd: false);

        // Assert
        Assert.Empty(response.Object.Headers);

        // Act
        BuiltInFunctions.AddAgentHttpDeprecationHeaders(response.Object, shouldAdd: true);

        // Assert
        Assert.True(response.Object.Headers.TryGetValues("Deprecation", out IEnumerable<string>? deprecation));
        Assert.Equal("true", Assert.Single(deprecation));
        Assert.True(response.Object.Headers.TryGetValues("Link", out IEnumerable<string>? links));
        Assert.Contains("rel=\"deprecation\"", Assert.Single(links), StringComparison.Ordinal);
        Assert.True(response.Object.Headers.TryGetValues("Warning", out IEnumerable<string>? warnings));
        Assert.Contains("Deprecated agent HTTP field names", Assert.Single(warnings), StringComparison.Ordinal);
    }
}
