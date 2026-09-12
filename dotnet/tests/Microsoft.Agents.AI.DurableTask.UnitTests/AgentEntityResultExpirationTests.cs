// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.AI;
using static Microsoft.Agents.AI.DurableTask.Tests.Unit.AgentEntityDeliveryTests;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class AgentEntityResultExpirationTests
{
    private static readonly DateTimeOffset s_now = new(2026, 9, 12, 0, 0, 0, TimeSpan.Zero);

    [Fact]
    public async Task SuccessfulRunSchedulesCleanupAndColdDueTurnPersistsUnavailableAsync()
    {
        List<DateTimeOffset> signals = [];
        AgentEntityResultExpirationCheck? scheduledCheck = null;
        EntityHarness first = CreateHarness(new RecordingAgent("agent"), state: null,
            resultRetentionPeriod: TimeSpan.FromMinutes(2), timeProvider: new Clock(s_now),
            onSignalInput: input => scheduledCheck = JsonSerializer.Deserialize<AgentEntityResultExpirationCheck>(
                JsonSerializer.Serialize(Assert.IsType<AgentEntityResultExpirationCheck>(input))),
            onSignal: (name, options) => CaptureSignal(signals, name, options));
        await first.RunAsync(new RunRequest("request") { CorrelationId = "request" });
        Assert.Equal(s_now.AddMinutes(2), Assert.Single(signals));
        DurableAgentState committed = Reload(Assert.IsType<DurableAgentState>(first.PersistedState));
        DurableAgentState before = Reload(committed);
        EntityHarness cleanup = CleanupHarness(committed, signals, s_now.AddMinutes(2));

        await cleanup.CheckResultsExpirationAsync(Assert.IsType<AgentEntityResultExpirationCheck>(scheduledCheck));

        DurableAgentState cleaned = Reload(Assert.IsType<DurableAgentState>(cleanup.PersistedState));
        Assert.Empty(cleaned.Data.TerminalResults!);
        DurableAgentStateCompletionReceipt receipt = Assert.Single(cleaned.Data.CompletionReceipts!).Value;
        Assert.Equal(DurableAgentStateCompletionReceipt.UnavailableResult, receipt.ResultState);
        Assert.Equal(s_now, receipt.CompletedAt);
        Assert.Equal(s_now.AddMinutes(2), receipt.ResultExpiresAt);
        Assert.Equal(s_now.AddMinutes(2), receipt.ResultUnavailableAt);
        Assert.Equal(Serialize(before), Serialize(committed));
        Assert.Null(cleaned.Data.ExpirationTimeUtc);
        Assert.Single(signals);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ImportedExpiredOutcomesCleanWithoutModelOrDeletionAndStayUnavailableAsync(bool failed)
    {
        DurableAgentState state = CreateState(failed);
        string original = Serialize(state);
        List<DateTimeOffset> signals = [];
        EntityHarness cleanup = CleanupHarness(state, signals, s_now);

        await cleanup.CheckResultsExpirationAsync();

        DurableAgentState cleaned = Reload(Assert.IsType<DurableAgentState>(cleanup.PersistedState));
        Assert.Empty(cleaned.Data.TerminalResults!);
        DurableAgentStateCompletionReceipt receipt = cleaned.Data.CompletionReceipts!["request"];
        Assert.Equal(failed ? "failed" : "succeeded", receipt.Outcome);
        Assert.Equal(s_now.AddMinutes(-2), receipt.CompletedAt);
        Assert.Equal(s_now.AddMinutes(-1), receipt.ResultExpiresAt);
        Assert.Equal(s_now, receipt.ResultUnavailableAt);
        Assert.Equal("unavailable", receipt.ResultState);
        Assert.False(receipt.UnknownProperties!["future"].GetProperty("flag").GetBoolean());
        Assert.Equal(state.Data.ExpirationTimeUtc, cleaned.Data.ExpirationTimeUtc);
        Assert.True(JsonElement.DeepEquals(state.Data.HistoryBinding, cleaned.Data.HistoryBinding));
        Assert.True(JsonElement.DeepEquals(state.Data.Session!.Value, cleaned.Data.Session!.Value));
        Assert.Equal(state.Data.IngestedPositions, cleaned.Data.IngestedPositions);
        Assert.Equal(original, Serialize(state));
        Assert.Empty(signals);

        EntityHarness repeated = CleanupHarness(cleaned, signals, s_now.AddHours(1));
        await repeated.CheckResultsExpirationAsync();
        DurableAgentState twice = Reload(Assert.IsType<DurableAgentState>(repeated.PersistedState));
        Assert.Equal(Serialize(cleaned), Serialize(twice));
        EntityHarness duplicate = CleanupHarness(twice, signals, s_now.AddHours(1));
        DurableAgentResultUnavailableException exception = await Assert.ThrowsAsync<DurableAgentResultUnavailableException>(
            () => duplicate.RunAsync(new RunRequest([]) { CorrelationId = "request" }));
        Assert.Equal(receipt.Outcome, exception.Outcome);
        Assert.False(duplicate.StateWasPersisted);
        string beforePoll = Serialize(twice);
        AgentRunHandle handle = AgentRunHandleTests.CreateHandle(twice, correlationId: "request",
            timeProvider: new Clock(s_now.AddHours(1)));
        Assert.Equal(DurableAgentRunOutcomeKind.CompletedResultUnavailable, (await handle.ReadAgentOutcomeAsync()).Kind);
        await Assert.ThrowsAsync<DurableAgentResultUnavailableException>(() => handle.ReadAgentResponseAsync());
        Assert.Equal(beforePoll, Serialize(twice));
    }

    [Theory]
    [InlineData(-10)]
    [InlineData(0)]
    [InlineData(10)]
    public async Task EarlyStaleAndNewGenerationChecksUseCurrentDeadlineAndClockAsync(int clockMinutes)
    {
        // A stale self-signal carries no expiry authority: it sweeps the authoritative
        // generation. Reusing the same correlation after deletion cannot expire this new result.
        DurableAgentState state = CreateState(expiresAt: s_now.AddMinutes(20));
        string before = Serialize(state);
        List<DateTimeOffset> signals = [];
        EntityHarness cleanup = CleanupHarness(state, signals, s_now.AddMinutes(clockMinutes));

        await cleanup.CheckResultsExpirationAsync();

        Assert.Equal(before, Serialize(Assert.IsType<DurableAgentState>(cleanup.PersistedState)));
        Assert.Equal(s_now.AddMinutes(20), Assert.Single(signals));
        EntityHarness duplicate = CleanupHarness(Reload(state), signals, s_now.AddMinutes(clockMinutes));
        await duplicate.CheckResultsExpirationAsync();
        Assert.Equal(2, signals.Count);
        Assert.All(signals, signal => Assert.True(signal > s_now.AddMinutes(clockMinutes)));
    }

    [Fact]
    public async Task EarlyScheduledChecksWithBackwardClockAdvanceSchedulingWithoutExpiringPayloadAsync()
    {
        DurableAgentState state = CreateState(expiresAt: s_now.AddMinutes(20));
        List<DateTimeOffset> signals = [];
        AgentEntityResultExpirationCheck previous = new(s_now.AddMinutes(20));
        for (int check = 0; check < 3; check++)
        {
            EntityHarness cleanup = CleanupHarness(state, signals, s_now);
            await cleanup.CheckResultsExpirationAsync(previous);
            Assert.True(signals[^1] >= previous.ScheduledTime.AddMinutes(1));
            state = Reload(Assert.IsType<DurableAgentState>(cleanup.PersistedState));
            Assert.Equal("available", state.Data.CompletionReceipts!["request"].ResultState);
            Assert.Single(state.Data.TerminalResults!);
            previous = new(signals[^1]);
        }
    }

    [Fact]
    public async Task CleanupReschedulesNearDeadlineWithPositiveDelayAndExpiresOnlyDueResultsAsync()
    {
        DurableAgentState state = CreateState();
        DurableAgentStateOutcomeResolver.AddSuccessfulResult(state, "future",
            new AgentResponse(new ChatMessage(ChatRole.Assistant, "future")), s_now, s_now.AddSeconds(1));
        DurableAgentStateOutcomeResolver.AddSuccessfulResult(state, "forever",
            new AgentResponse(new ChatMessage(ChatRole.Assistant, "forever")), s_now);
        List<DateTimeOffset> signals = [];
        EntityHarness cleanup = CleanupHarness(state, signals, s_now);
        await cleanup.CheckResultsExpirationAsync();
        DurableAgentState cleaned = Reload(Assert.IsType<DurableAgentState>(cleanup.PersistedState));
        Assert.Equal(2, cleaned.Data.TerminalResults!.Count);
        Assert.False(cleaned.Data.TerminalResults.ContainsKey("request"));
        DateTimeOffset scheduled = Assert.Single(signals);
        Assert.True(scheduled >= s_now.AddSeconds(1));
        Assert.True(scheduled <= s_now.AddMinutes(1));
        EntityHarness next = CleanupHarness(cleaned, signals, scheduled);
        await next.CheckResultsExpirationAsync();
        DurableAgentState finished = Reload(Assert.IsType<DurableAgentState>(next.PersistedState));
        Assert.Equal("forever", Assert.Single(finished.Data.TerminalResults!).Key);
        Assert.Equal(3, finished.Data.CompletionReceipts!.Count);
        Assert.Single(signals);
    }

    [Fact]
    public async Task SuccessfulLaterRunRecoversImportedExpiredResultsWithoutRefreshingTheirTtlAsync()
    {
        DurableAgentState state = CreateState();
        EntityHarness harness = CreateHarness(new RecordingAgent("agent"), state, timeProvider: new Clock(s_now));
        await harness.RunAsync(new RunRequest("next") { CorrelationId = "next" });
        DurableAgentState committed = Reload(Assert.IsType<DurableAgentState>(harness.PersistedState));
        Assert.Equal("next", Assert.Single(committed.Data.TerminalResults!).Key);
        Assert.Equal("unavailable", committed.Data.CompletionReceipts!["request"].ResultState);
        Assert.Null(committed.Data.TerminalResults!["next"].ResultExpiresAt);
    }

    [Fact]
    public async Task MissingEntityCleanupDoesNotRecreateStateOrCallFactoryAsync()
    {
        List<DateTimeOffset> signals = [];
        bool deleted = false;
        EntityHarness cleanup = CreateHarness(new RecordingAgent("agent"), state: null,
            registerWithFactory: true, onFactoryInvoked: () => Assert.Fail("factory must not run"),
            onCommit: state => deleted = state is null,
            onSignal: (name, options) => CaptureSignal(signals, name, options));
        await cleanup.CheckResultsExpirationAsync();
        Assert.True(deleted);
        Assert.Null(cleanup.PersistedState);
        Assert.Empty(signals);
    }

    [Fact]
    public async Task OldScheduledCleanupCannotExpireReusedCorrelationInNewGenerationAsync()
    {
        List<DateTimeOffset> signals = [];
        EntityHarness oldGeneration = CreateHarness(new RecordingAgent("agent"), state: null,
            timeProvider: new Clock(s_now), resultRetentionPeriod: TimeSpan.FromMinutes(2),
            onSignal: (name, options) => CaptureSignal(signals, name, options));
        await oldGeneration.RunAsync(new RunRequest("old") { CorrelationId = "reused" });
        DateTimeOffset oldSignal = Assert.Single(signals);
        EntityHarness newGeneration = CreateHarness(new RecordingAgent("agent"), state: null,
            timeProvider: new Clock(s_now.AddMinutes(1)), resultRetentionPeriod: TimeSpan.FromMinutes(20));
        await newGeneration.RunAsync(new RunRequest("new") { CorrelationId = "reused" });
        DurableAgentState fresh = Reload(Assert.IsType<DurableAgentState>(newGeneration.PersistedState));
        string before = Serialize(fresh);

        EntityHarness stale = CleanupHarness(fresh, signals, oldSignal);
        await stale.CheckResultsExpirationAsync(new AgentEntityResultExpirationCheck(oldSignal));

        Assert.Equal(before, Serialize(Assert.IsType<DurableAgentState>(stale.PersistedState)));
        Assert.Equal(s_now.AddMinutes(21), signals[^1]);
        Assert.Equal("available", fresh.Data.CompletionReceipts!["reused"].ResultState);
    }

    [Fact]
    public async Task NoExpiryCleanupPreservesStateAndSchedulesNothingAsync()
    {
        EntityHarness first = CreateHarness(new RecordingAgent("agent"), state: null, timeProvider: new Clock(s_now),
            onSignal: (_, _) => Assert.Fail("no retention must not schedule"));
        await first.RunAsync(new RunRequest("request") { CorrelationId = "request" });
        DurableAgentState state = Reload(Assert.IsType<DurableAgentState>(first.PersistedState));
        string before = Serialize(state);
        List<DateTimeOffset> signals = [];
        EntityHarness cleanup = CleanupHarness(state, signals, s_now.AddYears(1));

        await cleanup.CheckResultsExpirationAsync();

        Assert.Equal(before, Serialize(Assert.IsType<DurableAgentState>(cleanup.PersistedState)));
        Assert.Empty(signals);
    }

    [Theory]
    [InlineData("1.0.0")]
    [InlineData("1.1.0")]
    [InlineData("1.2.0")]
    [InlineData("3.0.0")]
    public async Task CleanupRejectsInvalidLegacyOrUnknownVersionWithoutPromotionAsync(string schemaVersion)
    {
        DurableAgentState state = new() { SchemaVersion = schemaVersion };
        state.Data.ConversationHistory.Add(new DurableAgentStateRequest
        {
            Messages =
            [
                new DurableAgentStateMessage
                {
                    Role = "user",
                    Contents = [new DurableAgentStateUriContent { Uri = new Uri("https://example.test/media") }],
                },
            ],
        });
        EntityHarness harness = CleanupHarness(state, [], s_now);

        await Assert.ThrowsAnyAsync<InvalidOperationException>(() => harness.CheckResultsExpirationAsync());

        Assert.False(harness.StateWasPersisted);
        Assert.Equal(schemaVersion, state.SchemaVersion);
        Assert.Null(state.Data.CompletionReceipts);
    }

    [Theory]
    [InlineData("1.0.0")]
    [InlineData("1.1.0")]
    [InlineData("1.2.0")]
    public async Task CleanupNeverPromotesLegacyStateAsync(string schemaVersion)
    {
        DurableAgentState state = new() { SchemaVersion = schemaVersion };
        state.Data.ConversationHistory.Add(new DurableAgentStateRequest { CorrelationId = "legacy" });
        List<DateTimeOffset> signals = [];
        EntityHarness cleanup = CleanupHarness(state, signals, s_now);
        await cleanup.CheckResultsExpirationAsync();
        DurableAgentState persisted = Assert.IsType<DurableAgentState>(cleanup.PersistedState);
        Assert.Equal(schemaVersion, persisted.SchemaVersion);
        Assert.Null(persisted.Data.CompletionReceipts);
        Assert.Empty(signals);
    }

    [Fact]
    public async Task WriterDisabledCleanupFailsWithoutMutationOrSchedulingAsync()
    {
        DurableAgentState state = CreateState();
        string before = Serialize(state);
        EntityHarness cleanup = CreateHarness(new RecordingAgent("agent"), state, enableMailboxWrites: false,
            onSignal: (_, _) => Assert.Fail("must not schedule"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => cleanup.CheckResultsExpirationAsync());
        Assert.Equal(before, Serialize(state));
        Assert.False(cleanup.StateWasPersisted);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CleanupSchedulingFailureOrCancellationRollsBackExpiredResultsAsync(bool cancel)
    {
        DurableAgentState state = CreateState();
        DurableAgentStateOutcomeResolver.AddSuccessfulResult(state, "future",
            new AgentResponse(new ChatMessage(ChatRole.Assistant, "future")), s_now, s_now.AddHours(1));
        string before = Serialize(state);
        using CancellationTokenSource cancellation = new();
        EntityHarness cleanup = CreateHarness(new RecordingAgent("agent"), state,
            timeProvider: new Clock(s_now), cancellationToken: cancellation.Token,
            onSignal: (_, _) =>
            {
                if (cancel)
                {
                    cancellation.Cancel();
                }
                else
                {
                    throw new InvalidOperationException("scheduling failed");
                }
            });
        if (cancel)
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => cleanup.CheckResultsExpirationAsync());
        }
        else
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => cleanup.CheckResultsExpirationAsync());
        }

        Assert.False(cleanup.StateWasPersisted);
        Assert.Equal(before, Serialize(state));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RunSchedulingFailureOrCancellationLeavesNoCompletionAsync(bool cancel)
    {
        DurableAgentState state = CreateState(expiresAt: s_now.AddMinutes(20));
        string before = Serialize(state);
        using CancellationTokenSource cancellation = new();
        EntityHarness harness = CreateHarness(new RecordingAgent("agent"), state,
            resultRetentionPeriod: TimeSpan.FromMinutes(2), timeProvider: new Clock(s_now),
            cancellationToken: cancellation.Token, onSignal: (_, _) =>
            {
                if (cancel)
                {
                    cancellation.Cancel();
                }
                else
                {
                    throw new InvalidOperationException("scheduling failed");
                }
            });
        if (cancel)
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => harness.RunAsync(new RunRequest("next") { CorrelationId = "next" }));
        }
        else
        {
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => harness.RunAsync(new RunRequest("next") { CorrelationId = "next" }));
        }

        Assert.False(harness.StateWasPersisted);
        Assert.Equal(before, Serialize(state));
    }

    [Fact]
    public async Task PreCancelledCleanupLeavesHydratedStateUnchangedAsync()
    {
        DurableAgentState state = CreateState();
        string before = Serialize(state);
        EntityHarness harness = CreateHarness(new RecordingAgent("agent"), state,
            cancellationToken: new CancellationToken(true), onSignal: (_, _) => Assert.Fail("must not schedule"));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => harness.CheckResultsExpirationAsync());
        Assert.False(harness.StateWasPersisted);
        Assert.Equal(before, Serialize(state));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task InvalidStateOrSerializationFailureCannotPartiallyCleanAsync(bool serializationFailure)
    {
        DurableAgentState state = CreateState();
        if (serializationFailure)
        {
            state.UnknownProperties = new Dictionary<string, JsonElement> { ["invalid"] = default };
        }
        else
        {
            state.Data.CompletionReceipts!.Remove("request");
        }

        DurableAgentStateTerminalResult result = state.Data.TerminalResults!["request"];
        EntityHarness cleanup = CleanupHarness(state, [], s_now);
        await Assert.ThrowsAnyAsync<InvalidOperationException>(() => cleanup.CheckResultsExpirationAsync());
        Assert.Same(result, state.Data.TerminalResults["request"]);
        Assert.False(cleanup.StateWasPersisted);
    }

    [Fact]
    public async Task CleanupCommitFailureLeavesHydratedStateIntactForRetryAsync()
    {
        DurableAgentState state = CreateState();
        string before = Serialize(state);
        EntityHarness cleanup = CreateHarness(new RecordingAgent("agent"), state,
            timeProvider: new Clock(s_now), onCommit: _ => throw new InvalidOperationException("commit failed"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => cleanup.CheckResultsExpirationAsync());
        Assert.Equal(before, Serialize(state));
        EntityHarness retry = CleanupHarness(state, [], s_now);
        await retry.CheckResultsExpirationAsync();
        Assert.Empty(Assert.IsType<DurableAgentState>(retry.PersistedState).Data.TerminalResults!);
    }

    private static EntityHarness CleanupHarness(DurableAgentState state, List<DateTimeOffset> signals, DateTimeOffset now) =>
        CreateHarness(new RecordingAgent("agent"), state, registerWithFactory: true,
            onFactoryInvoked: () => Assert.Fail("cleanup/duplicate must not invoke factory"),
            timeProvider: new Clock(now), onSignal: (name, options) => CaptureSignal(signals, name, options));

    private static void CaptureSignal(List<DateTimeOffset> signals, string name, SignalEntityOptions? options)
    {
        Assert.Equal("CheckAndExpireResults", name);
        signals.Add(Assert.IsType<DateTimeOffset>(options?.SignalTime));
    }

    private static DurableAgentState CreateState(bool failed = false, DateTimeOffset? expiresAt = null)
    {
        DateTimeOffset completedAt = s_now.AddMinutes(-2);
        DateTimeOffset expiration = expiresAt ?? s_now.AddMinutes(-1);
        string outcome = failed ? "failed" : "succeeded";
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            MailboxWritesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>
                {
                    ["request"] = new()
                    {
                        CorrelationId = "request",
                        Outcome = outcome,
                        CompletedAt = completedAt,
                        ResultExpiresAt = expiration,
                        Response = DurableAgentStateTerminalResponse.FromResponse(
                            new AgentResponse(new ChatMessage(ChatRole.Assistant, "retained payload")), "request", completedAt),
                        Error = failed ? new DurableAgentStateTerminalError { Code = "failed", Message = "failure" } : null,
                    },
                },
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>
                {
                    ["request"] = new()
                    {
                        CorrelationId = "request",
                        Outcome = outcome,
                        CompletedAt = completedAt,
                        ResultExpiresAt = expiration,
                        ResultState = "available",
                        UnknownProperties = new Dictionary<string, JsonElement>
                        {
                            ["future"] = JsonSerializer.SerializeToElement(new { flag = false, value = (string?)null }),
                        },
                    },
                },
                ExpirationTimeUtc = s_now.AddDays(2).UtcDateTime,
                HistoryBinding = JsonSerializer.SerializeToElement<object?>(null),
                Session = JsonSerializer.SerializeToElement(new { continuation = "session" }),
                IngestedPositions = new Dictionary<string, int> { ["producer"] = 2 },
            },
        };
        state.Data.ConversationHistory.Add(new DurableAgentStateResponse
        {
            CorrelationId = "request",
            Messages = [new DurableAgentStateMessage { Role = "assistant", Contents = [new DurableAgentStateTextContent { Text = "not a delivery fallback" }] }],
        });
        return Reload(state);
    }

    private static string Serialize(DurableAgentState state) =>
        JsonSerializer.Serialize(state, DurableAgentStateJsonContext.Default.DurableAgentState);

    private static DurableAgentState Reload(DurableAgentState state) =>
        JsonSerializer.Deserialize(Serialize(state), DurableAgentStateJsonContext.Default.DurableAgentState)!;

    private sealed class Clock(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }
}
