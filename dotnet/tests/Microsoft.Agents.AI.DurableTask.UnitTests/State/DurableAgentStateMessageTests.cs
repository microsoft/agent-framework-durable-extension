// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit.State;

public sealed class DurableAgentStateMessageTests
{
    [Fact]
    public void MessageSerializationDeserialization()
    {
        // Arrange
        TextContent textContent = new("Hello, world!");
        ChatMessage message = new(ChatRole.User, [textContent])
        {
            AuthorName = "User123",
            CreatedAt = DateTimeOffset.UtcNow
        };

        DurableAgentStateMessage durableMessage = DurableAgentStateMessage.FromChatMessage(message);

        // Act
        string jsonContent = JsonSerializer.Serialize(
            durableMessage,
            DurableAgentStateJsonContext.Default.GetTypeInfo(typeof(DurableAgentStateMessage))!);

        DurableAgentStateMessage? convertedJsonContent = (DurableAgentStateMessage?)JsonSerializer.Deserialize(
            jsonContent,
            DurableAgentStateJsonContext.Default.GetTypeInfo(typeof(DurableAgentStateMessage))!);

        // Assert
        Assert.NotNull(convertedJsonContent);

        ChatMessage convertedMessage = convertedJsonContent.ToChatMessage();

        Assert.Equal(message.AuthorName, convertedMessage.AuthorName);
        Assert.Equal(message.CreatedAt, convertedMessage.CreatedAt);
        Assert.Equal(message.Role, convertedMessage.Role);

        AIContent convertedContent = Assert.Single(convertedMessage.Contents);
        TextContent convertedTextContent = Assert.IsType<TextContent>(convertedContent);

        Assert.Equal(textContent.Text, convertedTextContent.Text);
    }

    [Fact]
    public void MessageIdAndAdditionalPropertiesRoundTrip()
    {
        ChatMessage message = new(ChatRole.User, "hello")
        {
            MessageId = "message-1",
            AdditionalProperties = new AdditionalPropertiesDictionary
            {
                ["excluded"] = true,
                ["summary"] = "summary-1",
            },
        };

        DurableAgentStateMessage stored = DurableAgentStateMessage.FromChatMessage(message);
        ChatMessage restored = stored.ToChatMessage();

        Assert.Equal("message-1", restored.MessageId);
        Assert.NotNull(restored.AdditionalProperties);
        Assert.Equal(JsonValueKind.True, Assert.IsType<JsonElement>(restored.AdditionalProperties["excluded"]).ValueKind);
        Assert.Equal("summary-1", Assert.IsType<JsonElement>(restored.AdditionalProperties["summary"]).GetString());
    }

    [Fact]
    public void StandaloneConversionDoesNotInventRandomIdentity()
    {
        DurableAgentStateMessage stored =
            DurableAgentStateMessage.FromChatMessage(new ChatMessage(ChatRole.User, "hello"));

        Assert.Null(stored.MessageId);
        Assert.Null(stored.ToChatMessage().MessageId);
    }

    [Fact]
    public void EntryFactoriesSynthesizeDeterministicMessageIds()
    {
        RunRequest request = new("hello") { CorrelationId = "correlation" };

        DurableAgentStateRequest first = DurableAgentStateRequest.FromRunRequest(request);
        DurableAgentStateRequest second = DurableAgentStateRequest.FromRunRequest(request);

        Assert.Equal("durable_request_correlation_0", first.Messages[0].MessageId);
        Assert.Equal(first.Messages[0].MessageId, second.Messages[0].MessageId);
    }

    [Fact]
    public void EntryFactoriesPreserveProducerMessageIds()
    {
        RunRequest request = new(
            [new ChatMessage(ChatRole.User, "hello") { MessageId = "producer-id" }])
        {
            CorrelationId = "correlation",
        };

        DurableAgentStateRequest stored = DurableAgentStateRequest.FromRunRequest(request);

        Assert.Equal("producer-id", stored.Messages[0].MessageId);
    }

    [Fact]
    public void RequestFactoryUsesStoredPositionsWithoutFiltering()
    {
        RunRequest request = new(
            [
                new ChatMessage(ChatRole.User, [new AIContent()]),
                new ChatMessage(ChatRole.User, "hello"),
            ])
        {
            CorrelationId = "correlation",
        };

        DurableAgentStateRequest stored = DurableAgentStateRequest.FromRunRequest(request);

        Assert.Equal(
            ["durable_request_correlation_0", "durable_request_correlation_1"],
            stored.Messages.Select(message => message.MessageId));
    }

    [Fact]
    public void LegacyCorrelationlessCompactionUsesStoredPositions()
    {
        DateTimeOffset createdAt =
            DateTimeOffset.Parse("2026-07-27T12:34:56.123456+00:00");
        DurableAgentStateCompaction compaction = new()
        {
            CreatedAt = createdAt,
            Messages =
            [
                new DurableAgentStateMessage
                {
                    Role = ChatRole.Assistant.Value,
                    Contents = [],
                },
                new DurableAgentStateMessage
                {
                    Role = ChatRole.Assistant.Value,
                    Contents = [new DurableAgentStateTextContent { Text = "summary" }],
                },
            ],
        };

        DurableAgentStateMessageIdentity.EnsureMessageIds([compaction]);

        Assert.Equal(
            [
                "durable_compaction_2026-07-27T12:34:56.123456+00:00_0",
                "durable_compaction_2026-07-27T12:34:56.123456+00:00_1",
            ],
            compaction.Messages.Select(message => message.MessageId));
    }

    [Fact]
    public void LegacyEmptyCorrelationIdUsesTimestampScope()
    {
        DateTimeOffset createdAt =
            DateTimeOffset.Parse("2026-07-27T12:34:56.123456+00:00");

        string messageId = DurableAgentStateMessageIdentity.Create(
            "compaction",
            string.Empty,
            createdAt,
            storedIndex: 1);

        Assert.Equal(
            "durable_compaction_2026-07-27T12:34:56.123456+00:00_1",
            messageId);
    }

    [Fact]
    public void AdditionalPropertiesAreCopiedOnBothConversions()
    {
        AdditionalPropertiesDictionary producerProperties = new()
        {
            ["marker"] = "original",
        };
        ChatMessage message = new(ChatRole.User, "hello")
        {
            AdditionalProperties = producerProperties,
        };

        DurableAgentStateMessage stored = DurableAgentStateMessage.FromChatMessage(message);
        producerProperties["marker"] = "producer-mutated";
        ChatMessage restored = stored.ToChatMessage();
        restored.AdditionalProperties!["marker"] = "consumer-mutated";

        Assert.Equal("original", stored.AdditionalProperties?["marker"].GetString());
    }

    [Theory]
    [InlineData("2026-07-27T12:34:56+00:00", "2026-07-27T12:34:56+00:00")]
    [InlineData("2026-07-27T12:34:56.1234567+00:00", "2026-07-27T12:34:56.123456+00:00")]
    [InlineData("2026-07-27T12:34:56.1000000+05:30", "2026-07-27T12:34:56.100000+05:30")]
    public void CorrelationlessTimestampScopeMatchesPythonIsoFormat(string input, string expected)
    {
        DateTimeOffset timestamp = DateTimeOffset.Parse(input);

        string actual = DurableAgentStateMessageIdentity.FormatPythonIsoTimestamp(timestamp);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void VersionOnePointTwoFixtureRoundTrips()
    {
        const string Json = """
            {
              "schemaVersion": "1.2.0",
              "data": {
                "conversationHistory": [{
                  "$type": "request",
                  "correlationId": "request-1",
                  "createdAt": "2026-01-01T00:00:00Z",
                  "messages": [{
                    "role": "user",
                    "messageId": "message-1",
                    "extensionData": { "excluded": true },
                    "contents": [{ "$type": "text", "text": "hello" }]
                  }]
                }],
                "session": {
                  "conversationId": "service-1",
                  "stateBag": {}
                },
                "ingestedPositions": {
                  "input": 0,
                  "writer": 1
                },
                "truncation": {
                  "evictedMessageCount": 2,
                  "firstEvictedAt": "2026-01-01T00:00:00Z",
                  "lastEvictedAt": "2026-01-02T00:00:00Z"
                }
              }
            }
            """;

        DurableAgentState? state = JsonSerializer.Deserialize(
            Json,
            DurableAgentStateJsonContext.Default.DurableAgentState);
        string roundTrip = JsonSerializer.Serialize(
            state,
            DurableAgentStateJsonContext.Default.DurableAgentState);

        Assert.NotNull(state);
        Assert.Equal("1.2.0", state.SchemaVersion);
        Assert.Equal("message-1", state.Data.ConversationHistory[0].Messages[0].MessageId);
        Assert.Equal(0, state.Data.IngestedPositions?["input"]);
        Assert.Equal(1, state.Data.IngestedPositions?["writer"]);
        Assert.Contains("\"conversationId\":\"service-1\"", roundTrip, StringComparison.Ordinal);
        Assert.Contains("\"ingestedPositions\":{\"input\":0,\"writer\":1}", roundTrip, StringComparison.Ordinal);
        Assert.Contains("\"evictedMessageCount\":2", roundTrip, StringComparison.Ordinal);
    }
}
