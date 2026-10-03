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
    public async Task OrchestrationCallThrowsCommittedFailureAfterNormalEntityReturnAsync()
    {
        DateTimeOffset completedAt = DateTimeOffset.Parse("2026-10-01T06:00:00+00:00");
        AgentResponse entityResponse = new() { CreatedAt = completedAt, Messages = [] };
        DurableAgentStateTerminalResult terminalResult = new()
        {
            CorrelationId = "correlation",
            Outcome = DurableAgentStateCompletionReceipt.FailedOutcome,
            CompletedAt = completedAt,
            Response = DurableAgentStateTerminalResponse.FromResponse(
                entityResponse,
                "correlation",
                completedAt),
            Error = new DurableAgentStateTerminalError
            {
                Code = "providerFinalFailure",
                Message = "The provider request failed.",
            },
        };
        DurableAgentJsonUtilities.CaptureRetainedResult(entityResponse, terminalResult.Response);
        DurableAgentJsonUtilities.CaptureCommittedFailure(
            entityResponse,
            new DurableAgentFailureData
            {
                Version = 1,
                CorrelationId = "correlation",
                Code = terminalResult.Error.Code,
                Message = terminalResult.Error.Message,
                CompletedAt = completedAt,
                Outcome = DurableAgentStateCompletionReceipt.FailedOutcome,
            });
        DurableDataConverter converter = new();
        string wire = converter.Serialize(entityResponse);
        Assert.Equal(
            2,
            JsonDocument.Parse(wire).RootElement
                .GetProperty("$microsoftAgentFrameworkDurableTask")
                .GetProperty("version").GetInt32());

        AgentSessionId sessionId = new("agent", "session");
        Mock<TaskOrchestrationEntityFeature> entities = new();
        entities.Setup(value => value.CallEntityAsync<AgentResponse>(
                sessionId, nameof(AgentEntity.Run), It.IsAny<object?>(), It.IsAny<CallEntityOptions?>()))
            .ReturnsAsync(() => Assert.IsType<AgentResponse>(converter.Deserialize(wire, typeof(AgentResponse))));
        Mock<TaskOrchestrationContext> context = new();
        context.SetupGet(value => value.Entities).Returns(entities.Object);
        context.SetupGet(value => value.InstanceId).Returns("orchestration");
        DurableAIAgent agent = new(context.Object, "agent");

        DurableAgentTerminalException exception =
            await Assert.ThrowsAsync<DurableAgentTerminalException>(
                () => agent.RunAsync(
                    new ChatMessage(ChatRole.User, "request"),
                    new DurableAgentSession(sessionId)));

        Assert.Equal("correlation", exception.CorrelationId);
        Assert.Equal("providerFinalFailure", exception.Code);
        Assert.Equal("The provider request failed.", exception.Message);
        Assert.NotNull(exception.Response?.GetDurableResult());
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
    public void MixedCaseEnvelopeDuplicateFailsBeforeNativeResponseMaterialization()
    {
        const string Wire = """
            {"messages":false,"$microsoftAgentFrameworkDurableTask":{
              "kind":"agentResponse","version":1,"VERSION":1,"result":{"messages":[]}}}
            """;

        JsonException exception = Assert.Throws<JsonException>(
            () => new DurableDataConverter().Deserialize(Wire, typeof(AgentResponse)));

        Assert.Contains("duplicate recognized property", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(RecognizedEnvelopeDuplicates))]
    public void RecognizedEnvelopeDuplicatesFailClosed(string wire)
    {
        JsonException exception = Assert.Throws<JsonException>(
            () => new DurableDataConverter().Deserialize(wire, typeof(AgentResponse)));

        Assert.Contains("duplicate recognized property", exception.Message, StringComparison.Ordinal);
    }

    public static IEnumerable<object[]> RecognizedEnvelopeDuplicates()
    {
        const string Failure = """
            {"version":1,"correlationId":"correlation","code":"failed","message":"failure",
             "details":{"flag":false},"serializedResponse":null,"completedAt":"2026-10-01T06:00:00Z",
             "resultExpiresAt":"2026-10-01T06:01:00Z","outcome":"failed"}
            """;
        string envelope = $$"""
            {"kind":"agentResponse","version":2,"result":{"messages":[]},"failure":{{Failure}}}
            """;
        string root = $$"""
            {"messages":[],"$microsoftAgentFrameworkDurableTask":{{envelope}}}
            """;
        foreach ((string level, string json) in new[] { ("root", root), ("envelope", envelope), ("failure", Failure) })
        {
            using JsonDocument document = JsonDocument.Parse(json);
            foreach (JsonProperty property in document.RootElement.EnumerateObject())
            {
                if (level == "root" && property.Name == "messages")
                {
                    continue;
                }

                foreach (string duplicateName in new[] { property.Name, property.Name.ToUpperInvariant() })
                {
                    foreach (bool conflicting in new[] { false, true })
                    {
                        foreach (bool duplicateFirst in new[] { false, true })
                        {
                            string conflictingValue = property.Value.ValueKind == JsonValueKind.Null ? "\"conflict\"" : "null";
                            string duplicate = $"\"{duplicateName}\":{(conflicting ? conflictingValue : property.Value.GetRawText())}";
                            string duplicated = duplicateFirst
                                ? $"{{{duplicate},{json[1..]}"
                                : $"{json[..^1]},{duplicate}}}";
                            yield return
                            [
                                level switch
                                {
                                    "root" => duplicated,
                                    "envelope" => root.Replace(envelope, duplicated, StringComparison.Ordinal),
                                    _ => root.Replace(Failure, duplicated, StringComparison.Ordinal),
                                },
                            ];
                        }
                    }
                }
            }
        }
    }

    [Fact]
    public void UnknownFieldsAndNestedEnvelopeNamesRemainOpaque()
    {
        const string Wire = """
            {"messages":[],"kind":"native","KIND":"future","future":1,"FUTURE":2,
             "$microsoftAgentFrameworkDurableTask":{
               "kind":"agentResponse","version":2,"future":1,"FUTURE":2,
               "result":{"messages":[],"value":{"version":1,"VERSION":2},
                         "kind":"opaque","KIND":"also opaque","MESSAGES":"future"},
               "failure":{"version":1,"correlationId":"correlation","code":"failed","message":"failure",
                          "completedAt":"2026-10-01T06:00:00Z","outcome":"failed","future":1,"FUTURE":2,
                          "details":{"kind":"opaque","KIND":"also opaque","version":1,"VERSION":2}}}}
            """;

        AgentResponse restored = Assert.IsType<AgentResponse>(
            new DurableDataConverter().Deserialize(Wire, typeof(AgentResponse)));

        JsonElement result = Assert.IsType<JsonElement>(restored.GetDurableResult());
        Assert.Equal("future", result.GetProperty("MESSAGES").GetString());
        Assert.Equal("also opaque", result.GetProperty("KIND").GetString());
        Assert.Equal(2, result.GetProperty("value").GetProperty("VERSION").GetInt32());
        Assert.Equal(2, DurableAgentJsonUtilities.GetCommittedFailure(restored)!.Details.GetProperty("VERSION").GetInt32());
    }

    [Fact]
    public void SingleCaseVariantEnvelopeFieldsUseCaseInsensitiveMetadataSemantics()
    {
        const string Wire = """
            {"messages":[],"$MICROSOFTAGENTFRAMEWORKDURABLETASK":{
              "KIND":"agentResponse","VERSION":2,"RESULT":{"messages":[]},
              "FAILURE":{"VERSION":1,"CORRELATIONID":"correlation","CODE":"failed","MESSAGE":"failure",
                         "COMPLETEDAT":"2026-10-01T06:00:00Z","OUTCOME":"failed"}}}
            """;

        AgentResponse restored = Assert.IsType<AgentResponse>(
            new DurableDataConverter().Deserialize(Wire, typeof(AgentResponse)));

        Assert.NotNull(restored.GetDurableResult());
        Assert.Equal("correlation", DurableAgentJsonUtilities.GetCommittedFailure(restored)!.CorrelationId);
    }

    [Fact]
    public void SharedStateDuplicateValidationKeepsOrdinalPropertySemantics()
    {
        const string Wire = """
            {"schemaVersion":"1.0.0","SCHEMAVERSION":"future","data":{"conversationHistory":[]},"DATA":"future"}
            """;

        DurableAgentState state = Assert.IsType<DurableAgentState>(
            new DurableDataConverter().Deserialize(Wire, typeof(DurableAgentState)));

        Assert.Equal("1.0.0", state.SchemaVersion);
        Assert.Equal("future", state.UnknownProperties!["SCHEMAVERSION"].GetString());
        Assert.Equal("future", state.UnknownProperties["DATA"].GetString());
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
