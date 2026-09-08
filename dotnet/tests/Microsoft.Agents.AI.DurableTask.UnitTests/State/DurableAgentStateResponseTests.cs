// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit.State;

public sealed class DurableAgentStateResponseTests
{
    [Fact]
    public void FromResponsePreservesMessagesContainingOnlyOpaqueContent()
    {
        // Arrange: one message with real text, one with only opaque AIContent
        ChatMessage usefulMessage = new(ChatRole.Assistant, "Hello, world!")
        {
            CreatedAt = DateTimeOffset.UtcNow
        };
        ChatMessage opaqueOnlyMessage = new(ChatRole.Assistant, [
            new AIContent
            {
                RawRepresentation = new { kind = "sessionEvent", sessionId = "s123" }
            }])
        {
            CreatedAt = DateTimeOffset.UtcNow.AddSeconds(1)
        };

        AgentResponse response = new(new List<ChatMessage> { usefulMessage, opaqueOnlyMessage })
        {
            CreatedAt = DateTimeOffset.UtcNow
        };

        // Act
        DurableAgentStateResponse durableResponse = DurableAgentStateResponse.FromResponse("corr-123", response);

        Assert.Equal(2, durableResponse.Messages.Count);
        Assert.Equal(ChatRole.Assistant.Value, durableResponse.Messages[1].Role);

        AgentResponse convertedResponse = durableResponse.ToResponse();
        Assert.Equal(2, convertedResponse.Messages.Count);
        TextContent textContent = Assert.IsType<TextContent>(Assert.Single(convertedResponse.Messages[0].Contents));
        Assert.Equal("Hello, world!", textContent.Text);
        Assert.IsType<AIContent>(Assert.Single(convertedResponse.Messages[1].Contents));
    }

    [Fact]
    public void FromResponseKeepsMessagesWithMixedContent()
    {
        // Arrange: one message with both real text and opaque AIContent
        ChatMessage mixedMessage = new(ChatRole.Assistant, [
            new TextContent("Some useful text"),
            new AIContent { RawRepresentation = new { kind = "metadata" } }])
        {
            CreatedAt = DateTimeOffset.UtcNow
        };

        AgentResponse response = new(new List<ChatMessage> { mixedMessage })
        {
            CreatedAt = DateTimeOffset.UtcNow
        };

        // Act
        DurableAgentStateResponse durableResponse = DurableAgentStateResponse.FromResponse("corr-456", response);

        // Assert: the message is kept because it contains at least one serializable content
        DurableAgentStateMessage durableMessage = Assert.Single(durableResponse.Messages);
        Assert.Equal(ChatRole.Assistant.Value, durableMessage.Role);
    }

    [Fact]
    public void FromResponsePreservesAllMessagesWhenAllAreOpaque()
    {
        // Arrange: all messages contain only opaque AIContent
        ChatMessage opaque1 = new(ChatRole.Assistant, [
            new AIContent { RawRepresentation = new { kind = "event1" } }])
        {
            CreatedAt = DateTimeOffset.UtcNow
        };
        ChatMessage opaque2 = new(ChatRole.Assistant, [
            new AIContent { RawRepresentation = new { kind = "event2" } }])
        {
            CreatedAt = DateTimeOffset.UtcNow.AddSeconds(1)
        };

        AgentResponse response = new(new List<ChatMessage> { opaque1, opaque2 })
        {
            CreatedAt = DateTimeOffset.UtcNow
        };

        // Act
        DurableAgentStateResponse durableResponse = DurableAgentStateResponse.FromResponse("corr-789", response);

        Assert.Equal(2, durableResponse.Messages.Count);
    }

    [Fact]
    public void FromResponseKeepsBaseAIContentWithAnnotations()
    {
        // Arrange: base AIContent with annotations should be kept
        AIContent contentWithAnnotations = new()
        {
            RawRepresentation = new { kind = "event" },
            Annotations = [new AIAnnotation() { AdditionalProperties = new() { ["cite"] = "ref-1" } }]
        };
        ChatMessage message = new(ChatRole.Assistant, [contentWithAnnotations])
        {
            CreatedAt = DateTimeOffset.UtcNow
        };

        AgentResponse response = new([message]) { CreatedAt = DateTimeOffset.UtcNow };

        // Act
        DurableAgentStateResponse durableResponse = DurableAgentStateResponse.FromResponse("corr-ann", response);

        // Assert: message is kept because the AIContent has annotations
        Assert.Single(durableResponse.Messages);
    }

    [Fact]
    public void FromResponseKeepsBaseAIContentWithAdditionalProperties()
    {
        // Arrange: base AIContent with additional properties should be kept
        AIContent contentWithProps = new()
        {
            RawRepresentation = new { kind = "event" },
            AdditionalProperties = new() { ["custom_key"] = "custom_value" }
        };
        ChatMessage message = new(ChatRole.Assistant, [contentWithProps])
        {
            CreatedAt = DateTimeOffset.UtcNow
        };

        AgentResponse response = new([message]) { CreatedAt = DateTimeOffset.UtcNow };

        // Act
        DurableAgentStateResponse durableResponse = DurableAgentStateResponse.FromResponse("corr-props", response);

        // Assert: message is kept because the AIContent has additional properties
        Assert.Single(durableResponse.Messages);
    }

    [Fact]
    public void FromResponseUsesFinalPersistedPositionForGeneratedMessageId()
    {
        ChatMessage metadataOnly = new(ChatRole.Assistant, [])
        {
            AdditionalProperties = new() { ["kind"] = "metadata" },
        };
        ChatMessage text = new(ChatRole.Assistant, "kept");
        AgentResponse response = new([metadataOnly, text]);

        DurableAgentStateResponse stored =
            DurableAgentStateResponse.FromResponse("correlation", response);

        Assert.Equal(2, stored.Messages.Count);
        Assert.Equal("durable_response_correlation_0", stored.Messages[0].MessageId);
        Assert.Equal("durable_response_correlation_1", stored.Messages[1].MessageId);
    }

    [Fact]
    public void FromMessagesUsesFinalPersistedPositionForGeneratedMessageId()
    {
        ChatMessage metadataOnly = new(ChatRole.Assistant, [])
        {
            MessageId = "producer-metadata-id",
        };
        ChatMessage text = new(ChatRole.Assistant, "kept");

        DurableAgentStateResponse stored =
            DurableAgentStateResponse.FromMessages("correlation", [metadataOnly, text]);

        Assert.Equal(2, stored.Messages.Count);
        Assert.Equal("producer-metadata-id", stored.Messages[0].MessageId);
        Assert.Equal("durable_response_correlation_1", stored.Messages[1].MessageId);
    }

    [Fact]
    public void FromResponsePreservesProducerIdAfterFiltering()
    {
        ChatMessage metadataOnly = new(ChatRole.Assistant, []);
        ChatMessage text = new(ChatRole.Assistant, "kept")
        {
            MessageId = "producer-id",
        };

        DurableAgentStateResponse stored =
            DurableAgentStateResponse.FromResponse("correlation", new AgentResponse([metadataOnly, text]));

        Assert.Equal("producer-id", stored.Messages[1].MessageId);
    }

    [Fact]
    public void MetadataOnlyResponsePersistsAndRoundTrips()
    {
        DateTimeOffset createdAt = DateTimeOffset.Parse("2026-09-06T12:34:56+00:00");
        ChatMessage metadataOnly = new(ChatRole.Assistant, [])
        {
            AuthorName = "agent",
            CreatedAt = createdAt,
            MessageId = "producer-message-id",
            AdditionalProperties = new()
            {
                ["trace"] = "value",
            },
        };

        DurableAgentStateResponse stored =
            DurableAgentStateResponse.FromResponse("correlation", new AgentResponse([metadataOnly]));
        string json = System.Text.Json.JsonSerializer.Serialize(
            stored,
            DurableAgentStateJsonContext.Default.DurableAgentStateResponse);
        DurableAgentStateResponse restored = Assert.IsType<DurableAgentStateResponse>(
            System.Text.Json.JsonSerializer.Deserialize(
                json,
                DurableAgentStateJsonContext.Default.DurableAgentStateResponse));
        ChatMessage roundTripped = Assert.Single(restored.ToResponse().Messages);

        Assert.Empty(roundTripped.Contents);
        Assert.Equal(ChatRole.Assistant, roundTripped.Role);
        Assert.Equal("agent", roundTripped.AuthorName);
        Assert.Equal(createdAt, roundTripped.CreatedAt);
        Assert.Equal("producer-message-id", roundTripped.MessageId);
        Assert.Equal(
            "value",
            Assert.IsType<System.Text.Json.JsonElement>(
                roundTripped.AdditionalProperties?["trace"]).GetString());
    }

    [Fact]
    public void ToResponseRetainsMetadataOnlyMessageForPolling()
    {
        DurableAgentStateResponse stored = new()
        {
            CorrelationId = "correlation",
            CreatedAt = DateTimeOffset.Parse("2026-09-06T12:00:00+00:00"),
            Messages =
            [
                new DurableAgentStateMessage
                {
                    Role = ChatRole.Assistant.Value,
                    MessageId = "pollable-metadata",
                    AdditionalProperties = new Dictionary<string, System.Text.Json.JsonElement>
                    {
                        ["status"] = System.Text.Json.JsonSerializer.SerializeToElement("complete"),
                    },
                    Contents = [],
                },
            ],
        };

        ChatMessage message = Assert.Single(stored.ToResponse().Messages);

        Assert.Equal("pollable-metadata", message.MessageId);
        Assert.Empty(message.Contents);
        Assert.Equal(
            "complete",
            Assert.IsType<System.Text.Json.JsonElement>(
                message.AdditionalProperties?["status"]).GetString());
    }

    [Fact]
    public void EmptyResponseGetsCreatedAtWithoutThrowing()
    {
        DurableAgentStateResponse stored =
            DurableAgentStateResponse.FromResponse("correlation", new AgentResponse());

        Assert.Empty(stored.Messages);
        Assert.NotEqual(default, stored.CreatedAt);
    }
}
