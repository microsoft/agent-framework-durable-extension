// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class AgentEntityDeliveryTests
{
    [Fact]
    public async Task DefaultRolloutLeavesProductionWritesLegacyAsync()
    {
        DurableAgentsOptions defaults = new();
        Assert.False(defaults.EnableMailboxWrites);
        Assert.Null(defaults.ResultRetentionPeriod);
        EntityHarness harness = CreateHarness(
            new RecordingAgent("agent"), new DurableAgentState(), enableMailboxWrites: false);

        await harness.RunAsync(new RunRequest("request") { CorrelationId = "new" });

        DurableAgentState committed = Assert.IsType<DurableAgentState>(harness.PersistedState);
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, committed.SchemaVersion);
        Assert.Null(committed.Data.TerminalResults);
        Assert.Null(committed.Data.CompletionReceipts);
    }

    [Theory]
    [InlineData("1.0.0", false)]
    [InlineData("1.0.0", true)]
    [InlineData("1.1.0", false)]
    [InlineData("1.1.0", true)]
    [InlineData("1.2.0", false)]
    [InlineData("1.2.0", true)]
    public async Task LegacyDeveloperMessageFailureLeavesStateUnchangedAndRetryableAsync(
        string schemaVersion,
        bool inResponse)
    {
        DurableAgentState state = new() { SchemaVersion = schemaVersion };
        state.Data.ConversationHistory.Add(CreateResponse("old", "retained"));
        string before = JsonSerializer.Serialize(state, DurableAgentStateJsonContext.Default.DurableAgentState);
        int factoryCalls = 0;
        RecordingAgent agent = new("agent")
        {
            ResponseUpdate = new AgentResponseUpdate(
                inResponse ? new ChatRole("developer") : ChatRole.Assistant, "response"),
        };
        EntityHarness harness = CreateHarness(agent, state, enableMailboxWrites: false,
            registerWithFactory: true, onFactoryInvoked: () => factoryCalls++);
        RunRequest request = new([
            new ChatMessage(inResponse ? ChatRole.User : new ChatRole("developer"), "request"),
        ])
        {
            CorrelationId = "new",
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => harness.RunAsync(request));

        Assert.Equal(inResponse ? 1 : 0, factoryCalls);
        Assert.Equal(inResponse ? 1 : 0, agent.InvocationCount);
        Assert.False(harness.StateWasPersisted);
        Assert.Equal(before, JsonSerializer.Serialize(state, DurableAgentStateJsonContext.Default.DurableAgentState));
        Assert.Null(state.Data.CompletionReceipts);
        Assert.Equal(DurableAgentRunOutcomeKind.Pending,
            DurableAgentStateOutcomeResolver.Resolve(state, "new", DateTimeOffset.UtcNow).Kind);

        EntityHarness retry = CreateHarness(new RecordingAgent("agent"), state, enableMailboxWrites: false);
        Assert.Equal("response", (await retry.RunAsync(new RunRequest("corrected") { CorrelationId = "new" })).Text);
        DurableAgentState committed = Reload(Assert.IsType<DurableAgentState>(retry.PersistedState));
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, committed.SchemaVersion);
        Assert.Equal(3, committed.Data.ConversationHistory.Count);
        Assert.Null(committed.Data.TerminalResults);
        Assert.Null(committed.Data.CompletionReceipts);
    }

    [Theory]
    [InlineData("1.0.0", false)]
    [InlineData("1.0.0", true)]
    [InlineData("1.1.0", false)]
    [InlineData("1.1.0", true)]
    [InlineData("1.2.0", false)]
    [InlineData("1.2.0", true)]
    public async Task LegacyRequestAndResponseKeepHistoricalFunctionArgumentMappingAsync(
        string schemaVersion,
        bool rawOnly)
    {
        FunctionCallContent call = new("call", "function",
            rawOnly ? null : new Dictionary<string, object?> { ["value"] = false })
        {
            RawRepresentation = " { \"incomplete\": ",
        };
        RecordingAgent agent = new("agent")
        {
            ResponseUpdate = new AgentResponseUpdate(ChatRole.Assistant, [call]),
        };
        EntityHarness harness = CreateHarness(agent,
            new DurableAgentState { SchemaVersion = schemaVersion }, enableMailboxWrites: false);

        await harness.RunAsync(new RunRequest([new ChatMessage(ChatRole.User, [call])]) { CorrelationId = "new" });

        DurableAgentState committed = Reload(Assert.IsType<DurableAgentState>(harness.PersistedState));
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, committed.SchemaVersion);
        Assert.Null(committed.Data.CompletionReceipts);
        foreach (DurableAgentStateEntry entry in committed.Data.ConversationHistory)
        {
            DurableAgentStateFunctionCallContent stored = Assert.IsType<DurableAgentStateFunctionCallContent>(
                Assert.Single(Assert.Single(entry.Messages).Contents));
            Assert.Equal(rawOnly ? JsonValueKind.Undefined : JsonValueKind.Object, stored.Arguments.ValueKind);
            if (!rawOnly)
            {
                Assert.False(stored.Arguments.GetProperty("value").GetBoolean());
            }
        }

        Assert.Null(Assert.IsType<FunctionCallContent>(Assert.Single(Assert.Single(agent.LastMessages).Contents)).RawRepresentation);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" { \"incomplete\": ")]
    [InlineData("""{"halt":true,"state":{"value":0}}""")]
    public async Task MailboxRequestResponseAndDuplicatePreserveV2OnlyShapesAsync(string arguments)
    {
        FunctionCallContent call = new("call", "function") { RawRepresentation = arguments };
        RecordingAgent agent = new("agent")
        {
            ResponseUpdate = new AgentResponseUpdate(new ChatRole("developer"), [call]),
        };
        EntityHarness harness = CreateHarness(agent, state: null, authorizeLegacyMigration: false);

        AgentResponse response = await harness.RunAsync(new RunRequest([
            new ChatMessage(new ChatRole("developer"), [call]),
        ])
        {
            CorrelationId = "new",
        });

        DurableAgentState committed = Reload(Assert.IsType<DurableAgentState>(harness.PersistedState));
        Assert.Equal(DurableAgentState.RevisedSchemaVersion, committed.SchemaVersion);
        Assert.Single(committed.Data.CompletionReceipts!);
        foreach (DurableAgentStateEntry entry in committed.Data.ConversationHistory)
        {
            DurableAgentStateMessage message = Assert.Single(entry.Messages);
            Assert.Equal("developer", message.Role);
            Assert.Equal(arguments,
                Assert.IsType<DurableAgentStateFunctionCallContent>(Assert.Single(message.Contents)).Arguments.GetString());
        }

        Assert.Equal(arguments,
            Assert.IsType<FunctionCallContent>(Assert.Single(Assert.Single(agent.LastMessages).Contents)).RawRepresentation);
        JsonElement originalResult = Assert.IsType<JsonElement>(response.GetDurableResult());
        Assert.Equal(arguments, originalResult.GetProperty("messages")[0].GetProperty("contents")[0].GetProperty("arguments").GetString());

        committed.Data.ConversationHistory.Clear();
        EntityHarness duplicate = CreateHarness(new RecordingAgent("agent"), Reload(committed),
            enableMailboxWrites: false, registerWithFactory: true,
            onFactoryInvoked: () => throw new InvalidOperationException("duplicate must bypass factory"));
        AgentResponse retained = await duplicate.RunAsync(new RunRequest([]) { CorrelationId = "new" });
        Assert.True(JsonElement.DeepEquals(originalResult, Assert.IsType<JsonElement>(retained.GetDurableResult())));
    }

    [Fact]
    public async Task NewMailboxCommitAndPrunedDuplicatePreserveLosslessSharedResultAsync()
    {
        DurableAgentState state = ReadFixture("shared-durable-agent-state-2.0-lossless.json");
        JsonElement originalResult = Assert.IsType<JsonElement>(DurableAgentStateOutcomeResolver
            .Resolve(state, "corr-lossless", DateTimeOffset.UtcNow).Response!.GetDurableResult());
        EntityHarness harness = CreateHarness(new RecordingAgent("agent"), state);

        await harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" });

        DurableAgentState committed = Reload(Assert.IsType<DurableAgentState>(harness.PersistedState));
        Assert.Single(state.Data.CompletionReceipts!);
        Assert.Equal(2, committed.Data.CompletionReceipts!.Count);
        committed.Data.ConversationHistory.Clear();
        EntityHarness duplicate = CreateHarness(new RecordingAgent("agent"), Reload(committed),
            enableMailboxWrites: false, registerWithFactory: true,
            onFactoryInvoked: () => throw new InvalidOperationException("duplicate must bypass factory"));

        AgentResponse response = await duplicate.RunAsync(new RunRequest([]) { CorrelationId = "corr-lossless" });

        JsonElement retained = Assert.IsType<JsonElement>(response.GetDurableResult());
        Assert.True(JsonElement.DeepEquals(originalResult, retained));
        Assert.False(retained.GetProperty("messages")[0].GetProperty("contents")[1].TryGetProperty("mediaType", out _));
        Assert.Equal(JsonValueKind.False, retained.GetProperty("value").ValueKind);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ColdMailboxHistoryProjectsMediaLessUriWithoutLosingCanonicalStateAsync(bool requestHistory)
    {
        const string MessageJson = """
            {"role":"developer","messageId":"media-message","authorName":"producer","createdAt":"2026-09-12T00:00:00Z",
             "extensionData":{"flag":false},"futureMessage":{"value":null},"contents":[
             {"$type":"uri","uri":"https://example.test/media","futureUri":{"value":0}},
             {"$type":"uri","uri":"https://example.test/image","mediaType":"image/png"},
             {"$type":"unknown","content":{"future":false}}]}
            """;
        DurableAgentStateMessage message = JsonSerializer.Deserialize(
            MessageJson, DurableAgentStateJsonContext.Default.DurableAgentStateMessage)!;
        DurableAgentState state = CreateRevisedState("old", "retained");
        state.MailboxWritesAuthorized = true;
        state.Data.ConversationHistory.Add(requestHistory
            ? new DurableAgentStateRequest { CorrelationId = "old", Messages = [message] }
            : new DurableAgentStateResponse { CorrelationId = "old", Messages = [message] });
        RecordingAgent agent = new("agent");
        EntityHarness harness = CreateHarness(agent, Reload(state));

        await harness.RunAsync(new RunRequest("next") { CorrelationId = "next" });

        ChatMessage modelMessage = agent.LastMessages[0];
        Assert.Equal(new ChatRole("developer"), modelMessage.Role);
        Assert.Equal(message.MessageId, modelMessage.MessageId);
        Assert.Equal(message.AuthorName, modelMessage.AuthorName);
        Assert.Equal(message.CreatedAt, modelMessage.CreatedAt);
        Assert.False(Assert.IsType<JsonElement>(modelMessage.AdditionalProperties!["flag"]).GetBoolean());
        JsonElement opaque = Assert.IsType<JsonElement>(Assert.IsType<AIContent>(modelMessage.Contents[0]).RawRepresentation);
        Assert.Equal("uri", opaque.GetProperty("$type").GetString());
        Assert.False(opaque.TryGetProperty("mediaType", out _));
        Assert.Equal(0, opaque.GetProperty("futureUri").GetProperty("value").GetInt32());
        Assert.Equal("image/png", Assert.IsType<UriContent>(modelMessage.Contents[1]).MediaType);
        Assert.False(Assert.IsType<JsonElement>(modelMessage.Contents[2].RawRepresentation).GetProperty("future").GetBoolean());
        DurableAgentState committed = Reload(Assert.IsType<DurableAgentState>(harness.PersistedState));
        Assert.True(JsonElement.DeepEquals(
            JsonSerializer.SerializeToElement(message, DurableAgentStateJsonContext.Default.DurableAgentStateMessage),
            JsonSerializer.SerializeToElement(committed.Data.ConversationHistory[0].Messages[0],
                DurableAgentStateJsonContext.Default.DurableAgentStateMessage)));
        Assert.Throws<InvalidOperationException>(() => message.ToChatMessage());
    }

    [Fact]
    public async Task ResponseWithDefaultMediaUriSurvivesNextColdInvocationAsync()
    {
        RecordingAgent firstAgent = new("agent")
        {
            ResponseUpdate = new AgentResponseUpdate(ChatRole.Assistant,
                [new UriContent(new Uri("https://example.test/media"), null!)]),
        };
        EntityHarness first = CreateHarness(firstAgent, state: null);
        await first.RunAsync(new RunRequest("first") { CorrelationId = "first" });
        DurableAgentState state = Reload(Assert.IsType<DurableAgentState>(first.PersistedState));
        Assert.Equal("application/octet-stream", Assert.IsType<DurableAgentStateUriContent>(
            state.Data.ConversationHistory[1].Messages[0].Contents[0]).MediaType);
        RecordingAgent nextAgent = new("agent");
        EntityHarness next = CreateHarness(nextAgent, state);

        await next.RunAsync(new RunRequest("next") { CorrelationId = "next" });

        UriContent uri = Assert.IsType<UriContent>(nextAgent.LastMessages[1].Contents[0]);
        Assert.Equal("application/octet-stream", uri.MediaType);
        Assert.Equal("https://example.test/media", uri.Uri.ToString());
    }

    [Fact]
    public async Task NewMissingStateGenerationCanInitializeMailboxOnlyUnderInternalGateAsync()
    {
        EntityHarness harness = CreateHarness(new RecordingAgent("agent"), state: null,
            authorizeLegacyMigration: false);

        await harness.RunAsync(new RunRequest("request") { CorrelationId = "new" });

        DurableAgentState state = Assert.IsType<DurableAgentState>(harness.PersistedState);
        Assert.Equal(DurableAgentState.RevisedSchemaVersion, state.SchemaVersion);
        Assert.Single(state.Data.CompletionReceipts!);
        Assert.Null(state.Data.ExpirationTimeUtc);
    }

    [Fact]
    public async Task RetainedLegacyEvidenceDoesNotAuthorizeMigrationAsync()
    {
        DurableAgentState state = CreateStateWithResponse("old", "retained");
        RecordingAgent agent = new("agent");
        EntityHarness duplicate = CreateHarness(agent, state, authorizeLegacyMigration: false);

        Assert.Equal("retained", (await duplicate.RunAsync(new RunRequest([]) { CorrelationId = "old" })).Text);
        Assert.Equal(0, agent.InvocationCount);
        Assert.Equal(DurableAgentState.CurrentSchemaVersion,
            Assert.IsType<DurableAgentState>(duplicate.PersistedState).SchemaVersion);

        EntityHarness next = CreateHarness(agent, state, authorizeLegacyMigration: false);
        await next.RunAsync(new RunRequest("request") { CorrelationId = "new" });
        DurableAgentState committed = Assert.IsType<DurableAgentState>(next.PersistedState);
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, committed.SchemaVersion);
        Assert.Null(committed.Data.CompletionReceipts);
    }

    [Fact]
    public async Task PreviouslyPersistedEmptyLegacyStateDoesNotBecomeEmptyMailboxAsync()
    {
        EntityHarness harness = CreateHarness(new RecordingAgent("agent"), new DurableAgentState(),
            authorizeLegacyMigration: false);

        await harness.RunAsync(new RunRequest("request") { CorrelationId = "new" });

        DurableAgentState committed = Assert.IsType<DurableAgentState>(harness.PersistedState);
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, committed.SchemaVersion);
        Assert.Null(committed.Data.CompletionReceipts);
    }

    [Fact]
    public void MailboxActivationAndDeletionHaveNoPublicOptInOrImplicitTtl()
    {
        DurableAgentsOptions options = new();
        Assert.Null(typeof(DurableAgentsOptions).GetProperty("EnableMailboxWrites"));
        Assert.Null(typeof(DurableAgentsOptions).GetProperty("EnableMailboxEntityDeletion"));
        Assert.Equal(TimeSpan.FromDays(14), options.GetTimeToLive("agent"));
        Assert.Null(options.GetTimeToLive("agent", revisedState: true));
        options.EnableMailboxEntityDeletion = true;
        Assert.Null(options.GetTimeToLive("agent", revisedState: true));
        options.DefaultTimeToLive = TimeSpan.FromMinutes(10);
        Assert.Equal(TimeSpan.FromMinutes(10), options.GetTimeToLive("agent", revisedState: true));
    }

    [Fact]
    public async Task DisabledRolloutRejectsNewRevisedExecutionButAllowsDuplicateAsync()
    {
        RecordingAgent agent = new("agent");
        DurableAgentState state = CreateRevisedState("old", "retained");
        EntityHarness harness = CreateHarness(agent, state, enableMailboxWrites: false);

        Assert.Equal("retained", (await harness.RunAsync(new RunRequest([]) { CorrelationId = "old" })).Text);
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));
        Assert.Equal(0, agent.InvocationCount);
        Assert.Single(state.Data.CompletionReceipts!);
    }

    [Fact]
    public async Task PreCancelledRequestBypassesFactoryAndLeavesStateRetryableAsync()
    {
        int factoryCalls = 0;
        using CancellationTokenSource cancellation = new();
        cancellation.Cancel();
        DurableAgentState state = new();
        EntityHarness harness = CreateHarness(new RecordingAgent("agent"), state,
            registerWithFactory: true, onFactoryInvoked: () => factoryCalls++,
            cancellationToken: cancellation.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => harness.RunAsync(new RunRequest("request") { CorrelationId = "new" }));

        Assert.Equal(0, factoryCalls);
        Assert.False(harness.StateWasPersisted);
        Assert.Empty(state.Data.ConversationHistory);
    }

    [Fact]
    public async Task CommitCapacityFailureRollsBackAndCorrelationCanRetryAsync()
    {
        RecordingAgent agent = new("agent");
        DurableAgentState state = new();
        RunRequest request = new("request") { CorrelationId = "new" };
        EntityHarness failing = CreateHarness(agent, state,
            onCommit: _ => throw new InvalidOperationException("backend capacity exceeded"));

        await Assert.ThrowsAsync<InvalidOperationException>(() => failing.RunAsync(request));

        Assert.False(failing.StateWasPersisted);
        Assert.Empty(state.Data.ConversationHistory);
        Assert.Null(state.Data.CompletionReceipts);
        EntityHarness retry = CreateHarness(agent, state);
        await retry.RunAsync(request);
        Assert.Equal(2, agent.InvocationCount);
        Assert.Single(Assert.IsType<DurableAgentState>(retry.PersistedState).Data.CompletionReceipts!);
    }

    [Fact]
    public async Task ProviderFailureCanRetryWithoutCreatingFailedReceiptAsync()
    {
        RecordingAgent agent = new("agent") { Exception = new InvalidOperationException("provider unavailable") };
        DurableAgentState state = new();
        RunRequest request = new("request") { CorrelationId = "new" };
        EntityHarness first = CreateHarness(agent, state);

        await Assert.ThrowsAsync<InvalidOperationException>(() => first.RunAsync(request));

        EntityHarness retry = CreateHarness(new RecordingAgent("agent"), state);
        await retry.RunAsync(request);
        DurableAgentState committed = Assert.IsType<DurableAgentState>(retry.PersistedState);
        Assert.Equal(DurableAgentStateCompletionReceipt.SucceededOutcome, committed.Data.CompletionReceipts!["new"].Outcome);
    }

    [Fact]
    public async Task CancellationAfterModelOutputDoesNotPublishCompletionAsync()
    {
        using CancellationTokenSource cancellation = new();
        DurableAgentState state = new();
        RecordingAgent agent = new("agent") { OnStreamCompleted = () => cancellation.Cancel() };
        EntityHarness harness = CreateHarness(agent, state, cancellationToken: cancellation.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => harness.RunAsync(new RunRequest("request") { CorrelationId = "new" }));

        Assert.Equal(1, agent.InvocationCount);
        Assert.False(harness.StateWasPersisted);
        Assert.Null(state.Data.CompletionReceipts);
    }

    [Fact]
    public async Task ResponseHandlerCannotCommitWithoutConsumingCompleteModelStreamAsync()
    {
        DurableAgentState state = new();
        Mock<IAgentResponseHandler> handler = new();
        handler.Setup(value => value.OnStreamingResponseUpdateAsync(
                It.IsAny<IAsyncEnumerable<AgentResponseUpdate>>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);
        EntityHarness harness = CreateHarness(new RecordingAgent("agent"), state, responseHandler: handler.Object);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.RunAsync(new RunRequest("request") { CorrelationId = "new" }));

        Assert.False(harness.StateWasPersisted);
        Assert.Null(state.Data.CompletionReceipts);
    }

    [Fact]
    public async Task ResponseMetadataIsCapturedBeforeCallerCanMutateReturnedResponseAsync()
    {
        DateTimeOffset createdAt = new(2026, 9, 10, 5, 0, 0, TimeSpan.Zero);
        AgentResponseUpdate update = new(ChatRole.Assistant, "response")
        {
            ResponseId = "response-id",
            CreatedAt = createdAt,
            FinishReason = ChatFinishReason.Stop,
            AdditionalProperties = new() { ["region"] = "test", ["explicitNull"] = null },
        };
#pragma warning disable MEAI001
        update.ContinuationToken = ResponseContinuationToken.FromBytes(new byte[] { 1, 2, 3 });
#pragma warning restore MEAI001
        update.Contents.Add(new UsageContent(new UsageDetails { InputTokenCount = 4, OutputTokenCount = 2, TotalTokenCount = 6 }));
        EntityHarness harness = CreateHarness(new RecordingAgent("agent") { ResponseUpdate = update }, new DurableAgentState());

        AgentResponse original = await harness.RunAsync(new RunRequest("request") { CorrelationId = "new" });
        original.Messages.Clear();
        original.AdditionalProperties!["region"] = "mutated";
        DurableAgentState committed = Reload(Assert.IsType<DurableAgentState>(harness.PersistedState));
        DurableAgentRunOutcome outcome = DurableAgentStateOutcomeResolver.Resolve(committed, "new", DateTimeOffset.UtcNow);
        AgentResponse stored = outcome.Response!;

        Assert.Equal("response", stored.Text);
        Assert.Equal("response-id", stored.ResponseId);
        Assert.Equal(original.AgentId, stored.AgentId);
        Assert.Equal(createdAt, stored.CreatedAt);
        Assert.Equal(ChatFinishReason.Stop, stored.FinishReason);
        Assert.Equal(6, stored.Usage!.TotalTokenCount);
#pragma warning disable MEAI001
        Assert.Equal(new byte[] { 1, 2, 3 }, stored.ContinuationToken!.ToBytes().ToArray());
#pragma warning restore MEAI001
        Assert.Equal("test", Assert.IsType<JsonElement>(stored.AdditionalProperties!["region"]).GetString());
        Assert.Equal(JsonValueKind.Null, Assert.IsType<JsonElement>(stored.AdditionalProperties["explicitNull"]).ValueKind);
        Assert.Equal(JsonValueKind.Undefined, outcome.Value.ValueKind);
    }

    [Fact]
    public async Task InvalidTerminalMetadataFailsBeforePublishingStateAsync()
    {
        DurableAgentState state = new();
        RecordingAgent agent = new("agent")
        {
            ResponseUpdate = new AgentResponseUpdate(ChatRole.Assistant, "response")
            {
                ResponseId = new string('x', DurableAgentStateContract.MaxIdentifierLength + 1),
            },
        };
        EntityHarness harness = CreateHarness(agent, state);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.RunAsync(new RunRequest("request") { CorrelationId = "new" }));

        Assert.False(harness.StateWasPersisted);
        Assert.Empty(state.Data.ConversationHistory);
        Assert.Null(state.Data.CompletionReceipts);
    }

    [Theory]
    [InlineData("null")]
    [InlineData("false")]
    [InlineData("0")]
    [InlineData("\"\"")]
    [InlineData("[]")]
    [InlineData("{}")]
    public async Task JsonResponseFormatDoesNotInventValueFromTextAsync(string text)
    {
        EntityHarness harness = CreateHarness(new RecordingAgent("agent") { ResponseText = text }, new DurableAgentState());
        await harness.RunAsync(new RunRequest("request", responseFormat: ChatResponseFormat.Json) { CorrelationId = "new" });
        DurableAgentState committed = Assert.IsType<DurableAgentState>(harness.PersistedState);
        DurableAgentState reloaded = Reload(committed);

        DurableAgentRunOutcome outcome = DurableAgentStateOutcomeResolver.Resolve(reloaded, "new", DateTimeOffset.UtcNow);
        Assert.Equal(JsonValueKind.Undefined, outcome.Value.ValueKind);
        Assert.Equal(text, outcome.Response!.Text);
    }

    [Fact]
    public async Task TextDoesNotBecomeAStructuredValueWithoutIndependentProducerEvidenceAsync()
    {
        DurableAgentState state = new();
        EntityHarness harness = CreateHarness(new RecordingAgent("agent"), state);

        await harness.RunAsync(new RunRequest("request", responseFormat: ChatResponseFormat.Json) { CorrelationId = "new" });

        DurableAgentState committed = Reload(Assert.IsType<DurableAgentState>(harness.PersistedState));
        Assert.Equal(JsonValueKind.Undefined, committed.Data.TerminalResults!["new"].Response!.Value.ValueKind);
    }

    [Fact]
    public async Task RetentionUsesInjectedClockAndPreservesCompletionWhenResultExpiresAsync()
    {
        DateTimeOffset start = new(2026, 9, 10, 5, 0, 0, TimeSpan.Zero);
        EntityHarness harness = CreateHarness(new RecordingAgent("agent"), new DurableAgentState(),
            resultRetentionPeriod: TimeSpan.FromMinutes(2), timeProvider: new FixedTimeProvider(start));
        await harness.RunAsync(new RunRequest("request") { CorrelationId = "new" });
        DurableAgentState committed = Assert.IsType<DurableAgentState>(harness.PersistedState);

        Assert.Equal(start.AddMinutes(2), committed.Data.TerminalResults!["new"].ResultExpiresAt);
        Assert.Null(committed.Data.ExpirationTimeUtc);
        EntityHarness duplicate = CreateHarness(new RecordingAgent("agent"), Reload(committed),
            timeProvider: new FixedTimeProvider(start.AddMinutes(2)));
        DurableAgentResultUnavailableException exception = await Assert.ThrowsAsync<DurableAgentResultUnavailableException>(
            () => duplicate.RunAsync(new RunRequest([]) { CorrelationId = "new" }));
        Assert.Equal(DurableAgentStateCompletionReceipt.SucceededOutcome, exception.Outcome);
        Assert.Single(committed.Data.CompletionReceipts!);
    }

    [Fact]
    public async Task PythonShapedCompactedLegacyStateStaysLegacyAndPreservesOpaqueMetadataAsync()
    {
        DurableAgentState state = ReadFixture("shared-durable-agent-state-1.2-python-shape.json");
        EntityHarness harness = CreateHarness(new RecordingAgent("agent"), state,
            registerWithFactory: true, onFactoryInvoked: () => throw new InvalidOperationException("factory must not run"),
            authorizeLegacyMigration: false);

        await harness.RunAsync(new RunRequest([]) { CorrelationId = "corr-python" });
        DurableAgentState committed = Reload(Assert.IsType<DurableAgentState>(harness.PersistedState));

        Assert.True(JsonElement.DeepEquals(state.Data.Session!.Value, committed.Data.Session!.Value));
        Assert.Equal(state.Data.IngestedPositions, committed.Data.IngestedPositions);
        Assert.Equal("python", committed.Data.ExtensionData!["dataProducer"].GetString());
        Assert.True(committed.Data.UnknownProperties!["futureDataProperty"].GetProperty("preserve").GetBoolean());
        Assert.True(committed.UnknownProperties!["futureRootProperty"].GetProperty("preserve").GetBoolean());
        Assert.Equal("interop-fixture", committed.ExtensionData!["rootProducer"].GetString());
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, committed.SchemaVersion);
        Assert.Null(committed.Data.CompletionReceipts);
        Assert.Equal(JsonValueKind.Object,
            committed.Data.ConversationHistory.OfType<DurableAgentStateResponse>()
                .Single(response => response.CorrelationId == "corr-python").Usage!.ExtensionData!["futureObject"].ValueKind);
        Assert.Equal(JsonValueKind.Undefined, committed.Data.HistoryBinding.ValueKind);
        Assert.Null(state.Data.TerminalResults);
    }

    [Fact]
    public async Task RevisedCommitKeepsSessionIngestionTruncationAndReceiptsIndependentAsync()
    {
        DurableAgentState state = ReadFixture("shared-durable-agent-state-2.0-pruned.json");
        EntityHarness harness = CreateHarness(new RecordingAgent("agent"), state);

        await harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" });

        DurableAgentState committed = Reload(Assert.IsType<DurableAgentState>(harness.PersistedState));
        Assert.True(JsonElement.DeepEquals(state.Data.Session!.Value, committed.Data.Session!.Value));
        Assert.Equal(state.Data.IngestedPositions, committed.Data.IngestedPositions);
        Assert.Equal(state.Data.Truncation!.EvictedMessageCount, committed.Data.Truncation!.EvictedMessageCount);
        Assert.True(JsonElement.DeepEquals(state.Data.HistoryBinding, committed.Data.HistoryBinding));
        Assert.Equal(3, committed.Data.CompletionReceipts!.Count);
        Assert.Equal(2, state.Data.CompletionReceipts!.Count);
        committed.Data.IngestedPositions!["example-producer"] = 99;
        Assert.Equal(3, state.Data.IngestedPositions!["example-producer"]);
    }

    [Fact]
    public async Task PythonShapedFailedUnavailableReceiptBypassesFactoryWithRolloutOffAsync()
    {
        DurableAgentState state = ReadFixture("shared-durable-agent-state-2.0.json");
        EntityHarness harness = CreateHarness(new RecordingAgent("agent"), state, enableMailboxWrites: false,
            registerWithFactory: true, onFactoryInvoked: () => throw new InvalidOperationException("factory must not run"));

        DurableAgentResultUnavailableException exception = await Assert.ThrowsAsync<DurableAgentResultUnavailableException>(
            () => harness.RunAsync(new RunRequest([]) { CorrelationId = "corr-expired" }));

        Assert.Equal(DurableAgentStateCompletionReceipt.FailedOutcome, exception.Outcome);
        Assert.False(harness.StateWasPersisted);
    }

    private static DurableAgentState ReadFixture(string name) =>
        JsonSerializer.Deserialize(
            File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", name)),
            DurableAgentStateJsonContext.Default.DurableAgentState)!;

    private static DurableAgentState Reload(DurableAgentState state) =>
        JsonSerializer.Deserialize(
            JsonSerializer.Serialize(state, DurableAgentStateJsonContext.Default.DurableAgentState),
            DurableAgentStateJsonContext.Default.DurableAgentState)!;

    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }

    [Fact]
    public async Task ReusedSuccessfulCorrelationReturnsWithoutComparingContentOrConstructingAgentAsync()
    {
        DurableAgentState initialState = CreateStateWithResponse(
            "duplicate",
            "response");
        int factoryInvocationCount = 0;
        EntityHarness harness = CreateHarness(
            new RecordingAgent("agent"),
            initialState,
            registerWithFactory: true,
            onFactoryInvoked: () => factoryInvocationCount++);

        AgentResponse response = await harness.RunAsync(
            new RunRequest("different logical request") { CorrelationId = "duplicate" });

        Assert.Equal("response", response.Text);
        Assert.Equal(0, factoryInvocationCount);
        DurableAgentState persisted = Assert.IsType<DurableAgentState>(harness.PersistedState);
        Assert.Equal(DurableAgentState.RevisedSchemaVersion, persisted.SchemaVersion);
        Assert.Equal("response", persisted.Data.TerminalResults?["duplicate"].Response?.ToResponse().Text);
    }

    [Fact]
    public async Task ReusedErrorCorrelationWithEmptyMessagesThrowsBeforeValidationAsync()
    {
        DurableAgentState initialState = CreateStateWithResponse(
            "duplicate",
            "persisted failure",
            isError: true);
        RecordingAgent agent = new("agent");
        int factoryInvocationCount = 0;
        EntityHarness harness = CreateHarness(agent, initialState,
            registerWithFactory: true, onFactoryInvoked: () => factoryInvocationCount++);

        DurableAgentTerminalException exception = await Assert.ThrowsAsync<DurableAgentTerminalException>(
            () => harness.RunAsync(new RunRequest([]) { CorrelationId = "duplicate" }));

        Assert.Equal("persisted failure", exception.Response?.Text);
        Assert.Equal("legacyErrorResponse", exception.Code);
        Assert.Equal("duplicate", exception.CorrelationId);
        Assert.Equal(0, agent.InvocationCount);
        Assert.Equal(0, factoryInvocationCount);
        Assert.False(harness.StateWasPersisted);
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, initialState.SchemaVersion);
        Assert.Null(initialState.Data.CompletionReceipts);
    }

    [Fact]
    public async Task DuplicateTerminalStateThrowsBeforeValidationAndAgentConstructionAsync()
    {
        DurableAgentState initialState = CreateStateWithResponse(
            "duplicate",
            "first");
        initialState.Data.ConversationHistory.Add(
            CreateResponse("duplicate", "second", isError: true));
        int factoryInvocationCount = 0;
        EntityHarness harness = CreateHarness(
            new RecordingAgent("agent"),
            initialState,
            registerWithFactory: true,
            onFactoryInvoked: () => factoryInvocationCount++);

        DurableAgentStateCorruptionException exception =
            await Assert.ThrowsAsync<DurableAgentStateCorruptionException>(
                () => harness.RunAsync(
                    new RunRequest([]) { CorrelationId = "duplicate" }));

        Assert.Equal(2, exception.TerminalResponseCount);
        Assert.Equal(0, factoryInvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task InvalidCorrelationAndNewEmptyRequestFailBeforeAgentSideEffectsAsync()
    {
        int factoryInvocationCount = 0;
        EntityHarness harness = CreateHarness(
            new RecordingAgent("agent"),
            new DurableAgentState(),
            registerWithFactory: true,
            onFactoryInvoked: () => factoryInvocationCount++);

        ArgumentException correlationException = await Assert.ThrowsAsync<ArgumentException>(
            () => harness.RunAsync(new RunRequest([]) { CorrelationId = "" }));
        ArgumentException messageException = await Assert.ThrowsAsync<ArgumentException>(
            () => harness.RunAsync(new RunRequest([]) { CorrelationId = "new" }));

        Assert.Equal("request", correlationException.ParamName);
        Assert.Equal("request", messageException.ParamName);
        Assert.Equal(0, factoryInvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ModelFailureDoesNotMutateOrPersistHydratedStateAsync()
    {
        RecordingAgent agent = new("agent")
        {
            Exception = new InvalidOperationException("model failed"),
        };
        DurableAgentState initialState = CreateStateWithResponse(
            "old",
            "old response");
        EntityHarness harness = CreateHarness(agent, initialState);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.RunAsync(
                new RunRequest("new request") { CorrelationId = "new" }));

        Assert.False(harness.StateWasPersisted);
        Assert.Single(initialState.Data.ConversationHistory);
        Assert.DoesNotContain(
            initialState.Data.ConversationHistory,
            entry => entry.CorrelationId == "new");
    }

    [Fact]
    public async Task CancellationDoesNotCreateCompletionOrMutateHydratedStateAsync()
    {
        RecordingAgent agent = new("agent")
        {
            Exception = new OperationCanceledException("cancelled"),
        };
        DurableAgentState initialState = new();
        EntityHarness harness = CreateHarness(agent, initialState);

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => harness.RunAsync(
                new RunRequest("new request") { CorrelationId = "new" }));

        Assert.False(harness.StateWasPersisted);
        Assert.Empty(initialState.Data.ConversationHistory);
        Assert.Null(initialState.Data.TerminalResults);
        Assert.Null(initialState.Data.CompletionReceipts);
    }

    [Fact]
    public async Task ResultSerializationFailureDoesNotCommitTranscriptOrCompletionAsync()
    {
        RecordingAgent agent = new("agent")
        {
            UnsupportedResponseMetadata = new object(),
        };
        DurableAgentState initialState = new();
        EntityHarness harness = CreateHarness(agent, initialState);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.RunAsync(
                new RunRequest("new request") { CorrelationId = "new" }));

        Assert.Equal(1, agent.InvocationCount);
        Assert.False(harness.StateWasPersisted);
        Assert.Empty(initialState.Data.ConversationHistory);
        Assert.Null(initialState.Data.TerminalResults);
        Assert.Null(initialState.Data.CompletionReceipts);
    }

    [Fact]
    public async Task NewRequestUsesExistingHistoryAndCommitsRequestAndResponseAsync()
    {
        RecordingAgent agent = new("agent");
        DurableAgentState initialState = CreateStateWithResponse(
            "old",
            "old response");
        EntityHarness harness = CreateHarness(agent, initialState);

        AgentResponse response = await harness.RunAsync(
            new RunRequest("new request") { CorrelationId = "new" });

        Assert.Equal("response", response.Text);
        Assert.Equal(["old response", "new request"], agent.LastMessages.Select(message => message.Text));
        DurableAgentState persisted = Assert.IsType<DurableAgentState>(harness.PersistedState);
        Assert.Equal(3, persisted.Data.ConversationHistory.Count);
        Assert.Equal(DurableAgentState.RevisedSchemaVersion, persisted.SchemaVersion);
        Assert.Equal(2, persisted.Data.TerminalResults?.Count);
        Assert.Equal(2, persisted.Data.CompletionReceipts?.Count);
        Assert.Null(persisted.Data.TerminalResults?["new"].ResultExpiresAt);
        Assert.Null(persisted.Data.CompletionReceipts?["new"].ResultExpiresAt);
        Assert.Single(initialState.Data.ConversationHistory);
    }

    [Fact]
    public async Task RevisedDuplicateAfterTranscriptRemovalBypassesAgentAsync()
    {
        DurableAgentState initialState = CreateRevisedState("duplicate", "mailbox");
        int factoryInvocationCount = 0;
        EntityHarness harness = CreateHarness(
            new RecordingAgent("agent"),
            initialState,
            registerWithFactory: true,
            onFactoryInvoked: () => factoryInvocationCount++);

        AgentResponse response = await harness.RunAsync(
            new RunRequest("different request") { CorrelationId = "duplicate" });

        Assert.Equal("mailbox", response.Text);
        Assert.Equal(0, factoryInvocationCount);
        Assert.Empty(initialState.Data.ConversationHistory);
    }

    [Fact]
    public async Task RevisedUnavailableCompletionNeverExecutesAgentOrFallsBackToTranscriptAsync()
    {
        DurableAgentState initialState = CreateRevisedState(
            "duplicate",
            "removed",
            resultAvailable: false);
        initialState.Data.ConversationHistory.Add(
            CreateResponse("duplicate", "stale transcript"));
        int factoryInvocationCount = 0;
        EntityHarness harness = CreateHarness(
            new RecordingAgent("agent"),
            initialState,
            registerWithFactory: true,
            onFactoryInvoked: () => factoryInvocationCount++);

        DurableAgentResultUnavailableException exception =
            await Assert.ThrowsAsync<DurableAgentResultUnavailableException>(
                () => harness.RunAsync(
                    new RunRequest("different request") { CorrelationId = "duplicate" }));

        Assert.Equal("duplicate", exception.CorrelationId);
        Assert.Equal(0, factoryInvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ColdReloadUsesCommittedMailboxWithoutAgentExecutionAsync()
    {
        RecordingAgent firstAgent = new("agent");
        EntityHarness firstHarness = CreateHarness(firstAgent, new DurableAgentState());
        RunRequest request = new("new request") { CorrelationId = "correlation" };

        _ = await firstHarness.RunAsync(request);
        DurableAgentState persisted = Assert.IsType<DurableAgentState>(
            firstHarness.PersistedState);
        persisted.Data.ConversationHistory.Clear();
        string json = JsonSerializer.Serialize(
            persisted,
            DurableAgentStateJsonContext.Default.DurableAgentState);
        DurableAgentState reloaded = Assert.IsType<DurableAgentState>(
            JsonSerializer.Deserialize(
                json,
                DurableAgentStateJsonContext.Default.DurableAgentState));
        int factoryInvocationCount = 0;
        EntityHarness secondHarness = CreateHarness(
            new RecordingAgent("agent"),
            reloaded,
            registerWithFactory: true,
            onFactoryInvoked: () => factoryInvocationCount++);

        AgentResponse duplicate = await secondHarness.RunAsync(request);

        Assert.Equal("response", duplicate.Text);
        Assert.Equal(0, factoryInvocationCount);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("null")]
    [InlineData("false")]
    [InlineData("0")]
    [InlineData("\"\"")]
    [InlineData("\"profile\"")]
    [InlineData("[]")]
    [InlineData("[null,false,0,{}]")]
    [InlineData("{}")]
    [InlineData("""{"version":99,"ownerKind":"future","providerKey":null,"$type":"untrusted","nested":{"preserve":true}}""")]
    public async Task OpaqueHistoryBindingIsPreservedWithoutSelectingProviderAsync(string? bindingJson)
    {
        using JsonDocument? bindingDocument = bindingJson is null ? null : JsonDocument.Parse(bindingJson);
        DurableAgentState state = CreateRevisedState("old", "response");
        state = new DurableAgentState
        {
            SchemaVersion = state.SchemaVersion,
            Data = new DurableAgentStateData
            {
                ConversationHistory = state.Data.ConversationHistory,
                TerminalResults = state.Data.TerminalResults,
                CompletionReceipts = state.Data.CompletionReceipts,
                HistoryBinding = bindingDocument?.RootElement ?? default,
            },
        };
        bindingDocument?.Dispose();
        DurableAgentState clone = state.Clone();
        Assert.Equal(state.Data.HistoryBinding.ValueKind, clone.Data.HistoryBinding.ValueKind);
        if (bindingJson is not null)
        {
            Assert.True(JsonElement.DeepEquals(state.Data.HistoryBinding, clone.Data.HistoryBinding));
        }

        int factoryInvocationCount = 0;
        EntityHarness harness = CreateHarness(
            new RecordingAgent("agent"),
            state,
            registerWithFactory: true,
            onFactoryInvoked: () => factoryInvocationCount++);

        await harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" });

        Assert.Equal(1, factoryInvocationCount);
        DurableAgentState committed = Reload(Assert.IsType<DurableAgentState>(harness.PersistedState));
        Assert.NotSame(state.Data, committed.Data);
        Assert.Equal(state.Data.HistoryBinding.ValueKind, committed.Data.HistoryBinding.ValueKind);
        if (bindingJson is not null)
        {
            Assert.True(JsonElement.DeepEquals(state.Data.HistoryBinding, committed.Data.HistoryBinding));
        }

        Assert.Single(state.Data.CompletionReceipts!);
        Assert.Equal(2, committed.Data.CompletionReceipts!.Count);
    }

    private static DurableAgentState CreateStateWithResponse(
        string correlationId,
        string text,
        bool isError = false)
    {
        DurableAgentState state = new();
        state.Data.ConversationHistory.Add(CreateResponse(correlationId, text, isError));
        return state;
    }

    private static DurableAgentStateResponse CreateResponse(
        string correlationId,
        string text,
        bool isError = false)
    {
        IReadOnlyList<DurableAgentStateMessage> messages =
        [
            DurableAgentStateMessage.FromChatMessage(
                new ChatMessage(ChatRole.Assistant, text)),
        ];
        return isError
            ? new DurableAgentStateErrorResponse
            {
                CorrelationId = correlationId,
                CreatedAt = DateTimeOffset.UtcNow,
                Messages = messages,
            }
            : new DurableAgentStateResponse
            {
                CorrelationId = correlationId,
                CreatedAt = DateTimeOffset.UtcNow,
                Messages = messages,
            };
    }

    private static DurableAgentState CreateRevisedState(
        string correlationId,
        string text,
        bool resultAvailable = true)
    {
        DateTimeOffset completedAt = DateTimeOffset.UtcNow.AddMinutes(-1);
        DurableAgentStateTerminalResult result =
            DurableAgentStateTerminalResult.FromResponse(
                correlationId,
                new AgentResponse(new ChatMessage(ChatRole.Assistant, text)),
                completedAt);
        return new DurableAgentState
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            Data = new DurableAgentStateData
            {
                ConversationHistory = [],
                TerminalResults = resultAvailable
                    ? new Dictionary<string, DurableAgentStateTerminalResult>
                    {
                        [correlationId] = result,
                    }
                    : new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>
                {
                    [correlationId] = new()
                    {
                        CorrelationId = correlationId,
                        Outcome = result.Outcome,
                        CompletedAt = completedAt,
                        ResultState = resultAvailable
                            ? DurableAgentStateCompletionReceipt.AvailableResult
                            : DurableAgentStateCompletionReceipt.UnavailableResult,
                        ResultUnavailableAt = resultAvailable ? null : completedAt.AddSeconds(1),
                    },
                },
            },
        };
    }

    internal static EntityHarness CreateHarness(
        RecordingAgent agent,
        DurableAgentState? state,
        bool registerWithFactory = false,
        Action? onFactoryInvoked = null,
        bool enableMailboxWrites = true,
        TimeSpan? resultRetentionPeriod = null,
        TimeProvider? timeProvider = null,
        Action<object?>? onCommit = null,
        IAgentResponseHandler? responseHandler = null,
        bool authorizeLegacyMigration = true,
        Action<string, SignalEntityOptions?>? onSignal = null,
        Action<object?>? onSignalInput = null,
        CancellationToken cancellationToken = default)
    {
        AgentSessionId sessionId = new(agent.Name!, "session");
        DurableAgentsOptions options = new()
        {
            DefaultTimeToLive = null,
            EnableMailboxWrites = enableMailboxWrites,
            ResultRetentionPeriod = resultRetentionPeriod,
            AuthorizeLegacyMigration = authorizeLegacyMigration
                ? candidate => ReferenceEquals(candidate, state)
                : null,
        };
        if (registerWithFactory)
        {
            options.AddAIAgentFactory(
                agent.Name!,
                _ =>
                {
                    onFactoryInvoked?.Invoke();
                    return agent;
                });
        }
        else
        {
            options.AddAIAgent(agent);
        }

        Dictionary<Type, object> services = new()
        {
            [typeof(DurableTaskClient)] = new Mock<DurableTaskClient>("test").Object,
            [typeof(ILoggerFactory)] = Extensions.Logging.Abstractions.NullLoggerFactory.Instance,
            [typeof(DurableAgentsOptions)] = options,
            [typeof(IReadOnlyDictionary<string, Func<IServiceProvider, AIAgent>>)] =
                options.GetAgentFactories(),
            [typeof(IHostApplicationLifetime)] = Mock.Of<IHostApplicationLifetime>(
                lifetime => lifetime.ApplicationStopping == CancellationToken.None),
            [typeof(TimeProvider)] = timeProvider ?? TimeProvider.System,
        };
        if (responseHandler is not null)
        {
            services[typeof(IAgentResponseHandler)] = responseHandler;
        }

        Mock<TaskEntityContext> context = new();
        context.SetupGet(value => value.Id).Returns(sessionId);
        context.Setup(value => value.SignalEntity(
                sessionId, It.IsAny<string>(), It.IsAny<object?>(), It.IsAny<SignalEntityOptions?>()))
            .Callback<EntityInstanceId, string, object?, SignalEntityOptions?>(
                (_, operationName, input, signalOptions) =>
                {
                    onSignal?.Invoke(operationName, signalOptions);
                    onSignalInput?.Invoke(input);
                });
        Mock<TaskEntityState> entityState = new();
        entityState.Setup(value => value.GetState(typeof(DurableAgentState))).Returns(state);
        object? persistedState = null;
        entityState.Setup(value => value.SetState(It.IsAny<object?>()))
            .Callback<object?>(value =>
            {
                onCommit?.Invoke(value);
                persistedState = value;
            });

        Mock<TaskEntityOperation> operation = new();
        operation.SetupGet(value => value.Name).Returns(nameof(AgentEntity.Run));
        operation.SetupGet(value => value.Context).Returns(context.Object);
        operation.SetupGet(value => value.State).Returns(entityState.Object);
        operation.SetupGet(value => value.HasInput).Returns(true);

        AgentEntity entity = new(new DictionaryServiceProvider(services), cancellationToken);
        return new EntityHarness(entity, operation, () => persistedState);
    }

    internal sealed class EntityHarness(
        AgentEntity entity,
        Mock<TaskEntityOperation> operation,
        Func<object?> persistedState)
    {
        public object? PersistedState => persistedState();

        public bool StateWasPersisted => this.PersistedState is not null;

        public async Task<AgentResponse> RunAsync(RunRequest request)
        {
            operation.SetupGet(value => value.Name).Returns(nameof(AgentEntity.Run));
            operation.Setup(value => value.GetInput(typeof(RunRequest))).Returns(request);
            object? result = await ((ITaskEntity)entity).RunAsync(operation.Object);
            return Assert.IsType<AgentResponse>(result);
        }

        public async Task CheckResultsExpirationAsync(AgentEntityResultExpirationCheck? scheduledCheck = null)
        {
            operation.SetupGet(value => value.Name).Returns("CheckAndExpireResults");
            operation.SetupGet(value => value.HasInput).Returns(scheduledCheck is not null);
            operation.Setup(value => value.GetInput(typeof(AgentEntityResultExpirationCheck))).Returns(scheduledCheck);
            _ = await ((ITaskEntity)entity).RunAsync(operation.Object);
        }
    }

    internal sealed class RecordingAgent(string name) : AIAgent
    {
        public override string? Name => name;

        public Exception? Exception { get; init; }

        public object? UnsupportedResponseMetadata { get; init; }

        public string ResponseText { get; init; } = "response";

        public AgentResponseUpdate? ResponseUpdate { get; init; }

        public Action? OnStreamCompleted { get; init; }

        public int InvocationCount { get; private set; }

        public List<ChatMessage> LastMessages { get; private set; } = [];

        protected override ValueTask<AgentSession> CreateSessionCoreAsync(
            CancellationToken cancellationToken = default) => new(new RecordingSession());

        protected override ValueTask<JsonElement> SerializeSessionCoreAsync(
            AgentSession session,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) =>
            new(JsonSerializer.SerializeToElement(new { }));

        protected override ValueTask<AgentSession> DeserializeSessionCoreAsync(
            JsonElement serializedState,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) =>
            new(new RecordingSession());

        protected override Task<AgentResponse> RunCoreAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        protected override async IAsyncEnumerable<AgentResponseUpdate> RunCoreStreamingAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            this.InvocationCount++;
            this.LastMessages = messages.ToList();
            if (this.Exception is not null)
            {
                throw this.Exception;
            }

            await Task.Yield();
            yield return this.ResponseUpdate ?? new AgentResponseUpdate(ChatRole.Assistant, this.ResponseText)
            {
                AdditionalProperties = this.UnsupportedResponseMetadata is null
                    ? null
                    : new AdditionalPropertiesDictionary
                    {
                        ["unsupported"] = this.UnsupportedResponseMetadata,
                    },
            };
            this.OnStreamCompleted?.Invoke();
        }

        private sealed class RecordingSession : AgentSession;
    }

    private sealed class DictionaryServiceProvider(IReadOnlyDictionary<Type, object> services) : IServiceProvider
    {
        public object? GetService(Type serviceType) =>
            services.TryGetValue(serviceType, out object? service) ? service : null;
    }
}
