// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Client.Entities;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class AgentRunHandleTests
{
    private static readonly AgentSessionId s_sessionId = new("agent", "session");

    [Theory]
    [InlineData("shared-durable-agent-state-2.0-lossless.json", "corr-lossless", nameof(DurableAgentRunOutcomeKind.Succeeded))]
    [InlineData("shared-durable-agent-state-2.0-pruned.json", "corr-failed", nameof(DurableAgentRunOutcomeKind.Failed))]
    [InlineData("shared-durable-agent-state-2.0.json", "corr-expired", nameof(DurableAgentRunOutcomeKind.CompletedResultUnavailable))]
    public async Task PollingReadsSharedEntityFixturesAsync(string fileName, string correlationId, string expected)
    {
        DurableAgentState state = JsonSerializer.Deserialize(
            File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", fileName)),
            DurableAgentStateJsonContext.Default.DurableAgentState)!;

        DurableAgentRunOutcome outcome = await CreateHandle(state, correlationId: correlationId).ReadAgentOutcomeAsync();

        Assert.Equal(expected, outcome.Kind.ToString());
        if (outcome.Kind == DurableAgentRunOutcomeKind.CompletedResultUnavailable)
        {
            Assert.Equal(DurableAgentStateCompletionReceipt.FailedOutcome, outcome.Receipt!.Outcome);
        }

        if (correlationId == "corr-lossless")
        {
            Assert.Equal(JsonValueKind.False, outcome.Value.ValueKind);
            AIContent opaqueUri = outcome.Response!.Messages[0].Contents[1];
            JsonElement raw = Assert.IsType<JsonElement>(opaqueUri.RawRepresentation);
            Assert.Equal("uri", raw.GetProperty("$type").GetString());
            Assert.Equal("https://example.test/media/1", raw.GetProperty("uri").GetString());
            Assert.False(raw.TryGetProperty("mediaType", out _));
            JsonElement retained = Assert.IsType<JsonElement>(
                DurableAgentJsonUtilities.GetRetainedResult(outcome.Response!));
            JsonElement retainedUri = retained.GetProperty("messages")[0].GetProperty("contents")[1];
            Assert.Equal("uri", retainedUri.GetProperty("$type").GetString());
            Assert.False(retainedUri.TryGetProperty("mediaType", out _));
        }
    }

    [Fact]
    public async Task PollingReadsFullMetadataAndResponseIsIndependentOfMailboxAsync()
    {
        DurableAgentState state = JsonSerializer.Deserialize(
            File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "shared-durable-agent-state-2.0.json")),
            DurableAgentStateJsonContext.Default.DurableAgentState)!;
        AgentRunHandle handle = CreateHandle(state, correlationId: "corr-2",
            timeProvider: new FixedTimeProvider(new(2026, 9, 10, 5, 0, 4, TimeSpan.Zero)));

        AgentResponse response = await handle.ReadAgentResponseAsync();
        Assert.Equal("response-id-2", response.ResponseId);
        Assert.Equal("agent-id-2", response.AgentId);
        Assert.Equal(ChatFinishReason.Stop, response.FinishReason);
#pragma warning disable MEAI001
        Assert.Equal(new byte[] { 1, 2, 3 }, response.ContinuationToken!.ToBytes().ToArray());
#pragma warning restore MEAI001
        Assert.Equal(8, response.Usage!.TotalTokenCount);
        Assert.Equal("test", Assert.IsType<JsonElement>(response.AdditionalProperties!["region"]).GetString());
        response.Messages.Clear();
        response.AdditionalProperties["region"] = "mutated";

        AgentResponse repeated = await handle.ReadAgentResponseAsync();
        Assert.Single(repeated.Messages);
        Assert.Equal("test", Assert.IsType<JsonElement>(repeated.AdditionalProperties!["region"]).GetString());
    }

    [Fact]
    public async Task RevisedTranscriptWithoutReceiptRemainsPendingUntilCancelledAsync()
    {
        DurableAgentState state = CreateRevisedState("mailbox", includeTranscript: true);
        state.Data.TerminalResults!.Clear();
        state.Data.CompletionReceipts!.Clear();
        using CancellationTokenSource cancellation = new();
        AgentRunHandle handle = CreateHandle(state, () => cancellation.Cancel());

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => handle.ReadAgentOutcomeAsync(cancellation.Token));
    }

    [Fact]
    public async Task PollingReturnsUniqueSuccessfulResponseAsync()
    {
        DurableAgentState state = new();
        state.Data.ConversationHistory.Add(CreateResponse("correlation", "success"));

        AgentResponse response = await CreateHandle(state).ReadAgentResponseAsync();

        Assert.Equal("success", response.Text);
    }

    [Fact]
    public async Task PollingThrowsRecordedTerminalErrorAsync()
    {
        DurableAgentState state = new();
        state.Data.ConversationHistory.Add(CreateErrorResponse("correlation", "failure"));

        DurableAgentTerminalException exception =
            await Assert.ThrowsAsync<DurableAgentTerminalException>(
                () => CreateHandle(state).ReadAgentResponseAsync());

        Assert.Equal("legacyErrorResponse", exception.Code);
        Assert.Equal("failure", exception.Response?.Text);
    }

    [Fact]
    public async Task PollingUsesRevisedMailboxAfterTranscriptRemovalAsync()
    {
        DurableAgentState state = CreateRevisedState(
            resultText: "mailbox",
            includeTranscript: false);

        AgentResponse response = await CreateHandle(state).ReadAgentResponseAsync();

        Assert.Equal("mailbox", response.Text);
        Assert.Equal("response-id", response.ResponseId);
    }

    [Fact]
    public async Task PollingExpiredRevisedResultThrowsUnavailableWithoutTranscriptFallbackAsync()
    {
        DateTimeOffset completedAt = DateTimeOffset.UtcNow.AddMinutes(-2);
        DateTimeOffset expiresAt = completedAt.AddMinutes(1);
        DurableAgentState state = CreateRevisedState(
            resultText: "mailbox",
            includeTranscript: true,
            completedAt,
            expiresAt);
        state.MailboxWritesAuthorized = true;
        string before = JsonSerializer.Serialize(state, DurableAgentStateJsonContext.Default.DurableAgentState);

        DurableAgentResultUnavailableException exception =
            await Assert.ThrowsAsync<DurableAgentResultUnavailableException>(
                () => CreateHandle(state).ReadAgentResponseAsync());

        Assert.Equal("correlation", exception.CorrelationId);
        Assert.Equal(completedAt, exception.CompletedAt);
        Assert.Equal(before, JsonSerializer.Serialize(state, DurableAgentStateJsonContext.Default.DurableAgentState));
        Assert.Equal("available", state.Data.CompletionReceipts!["correlation"].ResultState);
        Assert.Single(state.Data.TerminalResults!);
    }

    [Fact]
    public async Task PollingDuplicateTerminalsThrowsImmediatelyAsync()
    {
        DurableAgentState state = new();
        state.Data.ConversationHistory.Add(CreateResponse("correlation", "first"));
        state.Data.ConversationHistory.Add(CreateResponse("correlation", "second"));
        int readCount = 0;

        DurableAgentStateCorruptionException exception =
            await Assert.ThrowsAsync<DurableAgentStateCorruptionException>(
                () => CreateHandle(state, () => readCount++).ReadAgentResponseAsync());

        Assert.Equal("correlation", exception.CorrelationId);
        Assert.Equal(2, exception.TerminalResponseCount);
        Assert.Equal(1, readCount);
    }

    [Fact]
    public async Task PollingWithoutTerminalRemainsPendingAsync()
    {
        DurableAgentState state = new();
        state.Data.ConversationHistory.Add(
            new DurableAgentStateRequest
            {
                CorrelationId = "correlation",
                CreatedAt = DateTimeOffset.UtcNow,
            });
        using CancellationTokenSource cancellation = new();
        int readCount = 0;
        AgentRunHandle handle = CreateHandle(
            state,
            () =>
            {
                readCount++;
                cancellation.Cancel();
            });

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => handle.ReadAgentResponseAsync(cancellation.Token));

        Assert.Equal(1, readCount);
    }

    [Fact]
    public async Task ClientRejectsInvalidCorrelationBeforeSignallingAsync()
    {
        Mock<DurableTaskClient> client = new(MockBehavior.Strict, "test");
        DefaultDurableAgentClient durableAgentClient =
            new(client.Object, NullLoggerFactory.Instance);
        RunRequest request = new("request") { CorrelationId = "" };

        ArgumentException exception = await Assert.ThrowsAsync<ArgumentException>(
            () => durableAgentClient.RunAgentAsync(s_sessionId, request));

        Assert.Equal("request", exception.ParamName);
        client.VerifyNoOtherCalls();
    }

    [Fact]
    public void HandleRejectsInvalidCorrelationBeforePolling()
    {
        Mock<DurableTaskClient> client = new("test");

        ArgumentException exception = Assert.Throws<ArgumentException>(
            () => new AgentRunHandle(
                client.Object,
                NullLogger.Instance,
                s_sessionId,
                " "));

        Assert.Equal("correlationId", exception.ParamName);
    }

    internal static AgentRunHandle CreateHandle(
        DurableAgentState state,
        Action? onRead = null,
        string correlationId = "correlation",
        TimeProvider? timeProvider = null)
    {
        Mock<DurableEntityClient> entities = new("test");
        entities
            .Setup(client => client.GetEntityAsync<DurableAgentState>(
                s_sessionId,
                It.IsAny<CancellationToken>()))
            .Callback(onRead ?? (() => { }))
            .ReturnsAsync(new EntityMetadata<DurableAgentState>(s_sessionId, state));

        Mock<DurableTaskClient> client = new("test");
        client.SetupGet(value => value.Entities).Returns(entities.Object);
        return new AgentRunHandle(
            client.Object,
            NullLogger.Instance,
            s_sessionId,
            correlationId,
            timeProvider);
    }

    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }

    private static DurableAgentState CreateRevisedState(
        string resultText,
        bool includeTranscript,
        DateTimeOffset? completedAt = null,
        DateTimeOffset? expiresAt = null)
    {
        DateTimeOffset completed = completedAt ?? DateTimeOffset.UtcNow;
        AgentResponse response = new(new ChatMessage(ChatRole.Assistant, resultText))
        {
            ResponseId = "response-id",
        };
        DurableAgentStateTerminalResult result =
            DurableAgentStateTerminalResult.FromResponse(
                "correlation",
                response,
                completed,
                expiresAt);
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            Data = new DurableAgentStateData
            {
                ConversationHistory = [],
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>
                {
                    ["correlation"] = result,
                },
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>
                {
                    ["correlation"] = new()
                    {
                        CorrelationId = "correlation",
                        Outcome = result.Outcome,
                        CompletedAt = completed,
                        ResultState = DurableAgentStateCompletionReceipt.AvailableResult,
                        ResultExpiresAt = expiresAt,
                    },
                },
            },
        };
        if (includeTranscript)
        {
            state.Data.ConversationHistory.Add(
                CreateResponse("correlation", "transcript"));
        }

        return state;
    }

    private static DurableAgentStateResponse CreateResponse(string correlationId, string text)
    {
        return new DurableAgentStateResponse
        {
            CorrelationId = correlationId,
            CreatedAt = DateTimeOffset.UtcNow,
            Messages =
            [
                DurableAgentStateMessage.FromChatMessage(
                    new ChatMessage(ChatRole.Assistant, text)),
            ],
        };
    }

    private static DurableAgentStateErrorResponse CreateErrorResponse(string correlationId, string text)
    {
        return new DurableAgentStateErrorResponse
        {
            CorrelationId = correlationId,
            CreatedAt = DateTimeOffset.UtcNow,
            Messages =
            [
                DurableAgentStateMessage.FromChatMessage(
                    new ChatMessage(ChatRole.Assistant, text)),
            ],
        };
    }
}
