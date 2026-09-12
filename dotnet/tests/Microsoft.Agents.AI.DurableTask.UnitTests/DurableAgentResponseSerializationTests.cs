// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.AI;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class DurableAgentResponseSerializationTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("null")]
    [InlineData("false")]
    [InlineData("0")]
    [InlineData("\"\"")]
    [InlineData("{}")]
    [InlineData("""{"nested":[null,0,""]}""")]
    public void DurableResponseRoundTripPreservesCanonicalResultAndNativeShape(string? valueJson)
    {
        AgentResponse response = CreateResponse(valueJson);
        JsonElement expected = Assert.IsType<JsonElement>(response.GetDurableResult());
        JsonElement nativeBefore = SerializeNative(response);
        DurableDataConverter converter = new();

        string wire = converter.Serialize(response);
        AgentResponse restored = Assert.IsType<AgentResponse>(converter.Deserialize(wire, typeof(AgentResponse)));
        JsonElement actual = Assert.IsType<JsonElement>(restored.GetDurableResult());

        Assert.NotSame(response, restored);
        Assert.True(JsonElement.DeepEquals(expected, actual));
        Assert.True(JsonElement.DeepEquals(nativeBefore, SerializeNative(restored)));
        Assert.Equal(valueJson is not null, actual.TryGetProperty("value", out _));
        Assert.True(actual.GetProperty("futureResponseField").GetProperty("preserve").GetBoolean());
        Assert.Null(restored.RawRepresentation);
    }

    [Fact]
    public async Task OrchestrationCallReceivesCanonicalResultAfterDurableWireRoundTripAsync()
    {
        DurableDataConverter converter = new();
        string wire = converter.Serialize(CreateResponse("null"));
        AgentSessionId sessionId = new("agent", "session");
        Mock<TaskOrchestrationEntityFeature> entities = new();
        entities.Setup(value => value.CallEntityAsync<AgentResponse>(
                sessionId, nameof(AgentEntity.Run), It.IsAny<object?>(), It.IsAny<CallEntityOptions?>()))
            .ReturnsAsync(() => Assert.IsType<AgentResponse>(converter.Deserialize(wire, typeof(AgentResponse))));
        Mock<TaskOrchestrationContext> context = new();
        context.SetupGet(value => value.Entities).Returns(entities.Object);
        context.SetupGet(value => value.InstanceId).Returns("orchestration");
        DurableAIAgent agent = new(context.Object, "agent");

        AgentResponse response = await agent.RunAsync(
            new ChatMessage(ChatRole.User, "request"), new DurableAgentSession(sessionId));

        JsonElement result = Assert.IsType<JsonElement>(response.GetDurableResult());
        Assert.Equal(JsonValueKind.Null, result.GetProperty("value").ValueKind);
        Assert.True(result.GetProperty("futureResponseField").GetProperty("preserve").GetBoolean());
    }

    [Fact]
    public void SharedOpaqueUriSurvivesDurableResponseSerializationWithoutInventedMediaType()
    {
        DurableAgentState state = JsonSerializer.Deserialize(
            File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "shared-durable-agent-state-2.0-lossless.json")),
            DurableAgentStateJsonContext.Default.DurableAgentState)!;
        AgentResponse response = DurableAgentStateOutcomeResolver.Resolve(
            state, "corr-lossless", DateTimeOffset.UtcNow).Response!;
        DurableDataConverter converter = new();

        AgentResponse restored = Assert.IsType<AgentResponse>(
            converter.Deserialize(converter.Serialize(response), typeof(AgentResponse)));

        JsonElement result = Assert.IsType<JsonElement>(restored.GetDurableResult());
        JsonElement uri = result.GetProperty("messages")[0].GetProperty("contents")[1];
        Assert.Equal("uri", uri.GetProperty("$type").GetString());
        Assert.False(uri.TryGetProperty("mediaType", out _));
        Assert.Equal(JsonValueKind.False, result.GetProperty("value").ValueKind);
    }

    [Theory]
    [InlineData("""{"kind":"agentResponse","version":2,"result":{"messages":[]}}""")]
    [InlineData("""{"kind":"unknown","version":1,"result":{"messages":[]}}""")]
    [InlineData("""{"kind":"agentResponse","version":1,"result":null}""")]
    [InlineData("""{"kind":"agentResponse","version":1,"result":{"value":null}}""")]
    [InlineData("""{"kind":"agentResponse","version":1,"version":1,"result":{"messages":[]}}""")]
    public void MalformedOrUnsupportedDurableResponseEnvelopeFailsClosed(string envelope)
    {
        string wire = """{"messages":[],"$microsoftAgentFrameworkDurableTask":ENVELOPE}"""
            .Replace("ENVELOPE", envelope, StringComparison.Ordinal);

        Assert.Throws<JsonException>(() => new DurableDataConverter().Deserialize(wire, typeof(AgentResponse)));
    }

    [Fact]
    public void LegacyNativeResponseRemainsReadableWithoutFabricatingCanonicalMetadata()
    {
        DurableDataConverter converter = new();
        AgentResponse source = new(new ChatMessage(ChatRole.Assistant, "null"));

        string wire = converter.Serialize(source);
        AgentResponse restored = Assert.IsType<AgentResponse>(converter.Deserialize(wire, typeof(AgentResponse)));

        Assert.Equal("null", restored.Text);
        Assert.Null(restored.GetDurableResult());
        Assert.DoesNotContain("$microsoftAgentFrameworkDurableTask", wire, StringComparison.Ordinal);
    }

    private static JsonElement SerializeNative(AgentResponse response) =>
        JsonSerializer.SerializeToElement(response, DurableAgentJsonUtilities.DefaultOptions.GetTypeInfo(typeof(AgentResponse)));

    private static AgentResponse CreateResponse(string? valueJson)
    {
        DateTimeOffset completedAt = DateTimeOffset.UtcNow;
        DurableAgentStateTerminalResult result = DurableAgentStateTerminalResult.FromResponse(
            "correlation", new AgentResponse(new ChatMessage(ChatRole.Assistant, "native text")),
            completedAt, structuredValue: valueJson is null
                ? default
                : JsonSerializer.Deserialize(valueJson, DurableAgentStateJsonContext.Default.JsonElement));
        result.Response!.UnknownProperties = new Dictionary<string, JsonElement>
        {
            ["futureResponseField"] = JsonSerializer.SerializeToElement(new { preserve = true }),
        };
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult> { ["correlation"] = result },
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>
                {
                    ["correlation"] = new()
                    {
                        CorrelationId = "correlation",
                        Outcome = DurableAgentStateCompletionReceipt.SucceededOutcome,
                        CompletedAt = completedAt,
                        ResultState = DurableAgentStateCompletionReceipt.AvailableResult,
                    },
                },
            },
        };
        return DurableAgentStateOutcomeResolver.Resolve(state, "correlation", completedAt).Response!;
    }
}
