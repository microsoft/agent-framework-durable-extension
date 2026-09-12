// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Extensions.AI;
using static Microsoft.Agents.AI.DurableTask.Tests.Unit.AgentEntityDeliveryTests;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class LegacyPromotionMetadataTests
{
    public static TheoryData<string, string?> LegacyValues
    {
        get
        {
            TheoryData<string, string?> cases = new();
            foreach (string version in new[] { "1.0.0", "1.1.0", "1.2.0" })
            {
                foreach (string? value in new[] { null, "null", "false", "0", "\"\"", "{}", """{"nested":[null,false,0,""]}""" })
                {
                    cases.Add(version, value);
                }
            }

            return cases;
        }
    }

    [Theory]
    [MemberData(nameof(LegacyValues))]
    public async Task AuthorizedPromotionAndColdDuplicatesPreserveCanonicalMetadataAsync(string version, string? value)
    {
        const string Text = """{"value":"not the canonical value","haltRequested":true}""";
        string valueProperty = value is null ? string.Empty : $",\"value\":{value}";
        string json = """
            {"schemaVersion":"VERSION","data":{"conversationHistory":[
              {"$type":"response","correlationId":"correlation","createdAt":"2026-09-10T05:00:00Z",
               "messages":[{"role":"assistant","messageId":"message","contents":[
                 {"$type":"text","text":TEXT,"futureContent":{"zero":0}}]}],
               "usage":{"inputTokenCount":0,"outputTokenCount":2,"totalTokenCount":2},
               "extensionData":{"null":null,"false":false,"zero":0,"empty":"","json":"{\"value\":false}"},
               "futureResponse":{"nested":[null,false,0,""]}VALUE_PROPERTY}]}}
            """
            .Replace("VERSION", version, StringComparison.Ordinal)
            .Replace("TEXT", JsonSerializer.Serialize(Text), StringComparison.Ordinal)
            .Replace("VALUE_PROPERTY", valueProperty, StringComparison.Ordinal);
        DurableAgentState legacy = JsonSerializer.Deserialize(json, DurableAgentStateJsonContext.Default.DurableAgentState)!;
        AgentResponse polledLegacy = await AgentRunHandleTests.CreateHandle(legacy).ReadAgentResponseAsync();
        JsonElement expected = AssertTransport(polledLegacy);
        Assert.Equal(Text, polledLegacy.Text);
        Assert.Equal(value is not null, expected.TryGetProperty("value", out _));
        Assert.False(expected.GetProperty("extensionData").TryGetProperty("absent", out _));
        Assert.Equal(JsonValueKind.Null, expected.GetProperty("extensionData").GetProperty("null").ValueKind);
        Assert.Equal(JsonValueKind.False, expected.GetProperty("extensionData").GetProperty("false").ValueKind);
        Assert.Equal(0, expected.GetProperty("extensionData").GetProperty("zero").GetInt32());
        Assert.Equal(string.Empty, expected.GetProperty("extensionData").GetProperty("empty").GetString());
        Assert.Equal("""{"value":false}""", expected.GetProperty("extensionData").GetProperty("json").GetString());

        RecordingAgent agent = new("agent");
        EntityHarness promotion = CreateHarness(agent, legacy, registerWithFactory: true,
            onFactoryInvoked: () => Assert.Fail("Promotion must bypass the agent factory."),
            onSignal: (_, _) => Assert.Fail("Promotion must not schedule a signal."));
        AgentResponse first = await promotion.RunAsync(new RunRequest([]) { CorrelationId = "correlation" });
        Assert.True(JsonElement.DeepEquals(expected, AssertTransport(first)));

        DurableAgentState committed = Assert.IsType<DurableAgentState>(promotion.PersistedState);
        Assert.Equal(DurableAgentState.RevisedSchemaVersion, committed.SchemaVersion);
        Assert.Single(committed.Data.CompletionReceipts!);
        Assert.Single(committed.Data.TerminalResults!);
        DurableAgentStateResponse transcript = Assert.IsType<DurableAgentStateResponse>(
            Assert.Single(committed.Data.ConversationHistory));
        DurableAgentStateTerminalResponse snapshot = committed.Data.TerminalResults!["correlation"].Response!;
        Assert.NotSame(transcript.ExtensionData, snapshot.AdditionalProperties);
        Assert.NotSame(transcript.UnknownProperties, snapshot.UnknownProperties);
        transcript.ExtensionData!["false"] = JsonSerializer.SerializeToElement(true);
        transcript.UnknownProperties!["futureResponse"] = JsonSerializer.SerializeToElement("changed");
        transcript.Messages[0].MessageId = "changed";
        committed.Data.ConversationHistory.Clear();
        first.Messages.Clear();

        for (int duplicateIndex = 0; duplicateIndex < 2; duplicateIndex++)
        {
            committed = Reload(committed);
            string beforePoll = Serialize(committed);
            AgentResponse polled = await AgentRunHandleTests.CreateHandle(committed).ReadAgentResponseAsync();
            Assert.Equal(beforePoll, Serialize(committed));
            Assert.True(JsonElement.DeepEquals(expected, AssertTransport(polled)));
            Assert.Equal(Text, polled.Text);
            Assert.False(Assert.IsType<JsonElement>(polled.AdditionalProperties!["false"]).GetBoolean());
            polled.AdditionalProperties["false"] = "native mutation";

            EntityHarness duplicate = CreateHarness(agent, committed, enableMailboxWrites: false,
                registerWithFactory: true,
                onFactoryInvoked: () => Assert.Fail("Duplicate must bypass the agent factory."),
                onSignal: (_, _) => Assert.Fail("Duplicate must not schedule a signal."));
            AgentResponse response = await duplicate.RunAsync(new RunRequest([]) { CorrelationId = "correlation" });
            Assert.True(JsonElement.DeepEquals(expected, AssertTransport(response)));
            Assert.Equal(Text, response.Text);
            Assert.Equal(2, response.Usage!.TotalTokenCount);
            Assert.Equal(JsonValueKind.False, Assert.IsType<JsonElement>(response.AdditionalProperties!["false"]).ValueKind);
            committed = Assert.IsType<DurableAgentState>(duplicate.PersistedState);
            Assert.Empty(committed.Data.ConversationHistory);
            Assert.Single(committed.Data.CompletionReceipts!);
            Assert.Single(committed.Data.TerminalResults!);
        }

        Assert.Equal(0, agent.InvocationCount);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PromotionPreservesAbsentAndEmptyMetadataWithoutInventingValueAsync(bool emptyMetadata)
    {
        DurableAgentState legacy = new();
        legacy.Data.ConversationHistory.Add(new DurableAgentStateResponse
        {
            CorrelationId = "correlation",
            CreatedAt = new DateTimeOffset(2026, 9, 10, 5, 0, 0, TimeSpan.Zero),
            Messages = [DurableAgentStateMessage.FromChatMessage(new ChatMessage(ChatRole.Assistant, "null") { MessageId = "message" })],
            ExtensionData = emptyMetadata ? new Dictionary<string, JsonElement>() : null,
        });
        JsonElement expected = AssertTransport(await AgentRunHandleTests.CreateHandle(legacy).ReadAgentResponseAsync());
        EntityHarness promotion = CreateHarness(new RecordingAgent("agent"), legacy,
            registerWithFactory: true, onFactoryInvoked: () => Assert.Fail("No model is needed."));
        AssertTransport(await promotion.RunAsync(new RunRequest([]) { CorrelationId = "correlation" }));
        DurableAgentState state = Reload(Assert.IsType<DurableAgentState>(promotion.PersistedState));
        JsonElement actual = AssertTransport(await AgentRunHandleTests.CreateHandle(state).ReadAgentResponseAsync());

        Assert.True(JsonElement.DeepEquals(expected, actual), $"Expected: {expected}\nActual: {actual}");
        Assert.Equal(emptyMetadata, actual.TryGetProperty("extensionData", out _));
        Assert.False(actual.TryGetProperty("value", out _));
    }

    private static JsonElement AssertTransport(AgentResponse response)
    {
        JsonElement expected = Assert.IsType<JsonElement>(response.GetDurableResult());
        JsonElement native = JsonSerializer.SerializeToElement(
            response, DurableAgentJsonUtilities.DefaultOptions.GetTypeInfo(typeof(AgentResponse)));
        DurableDataConverter converter = new();
        AgentResponse restored = Assert.IsType<AgentResponse>(
            converter.Deserialize(converter.Serialize(response), typeof(AgentResponse)));
        Assert.True(JsonElement.DeepEquals(native, JsonSerializer.SerializeToElement(
            restored, DurableAgentJsonUtilities.DefaultOptions.GetTypeInfo(typeof(AgentResponse)))));
        JsonElement retained = Assert.IsType<JsonElement>(restored.GetDurableResult());
        Assert.True(JsonElement.DeepEquals(expected, retained));
        return retained;
    }

    private static string Serialize(DurableAgentState state) =>
        JsonSerializer.Serialize(state, DurableAgentStateJsonContext.Default.DurableAgentState);

    private static DurableAgentState Reload(DurableAgentState state) =>
        JsonSerializer.Deserialize(Serialize(state), DurableAgentStateJsonContext.Default.DurableAgentState)!;
}
