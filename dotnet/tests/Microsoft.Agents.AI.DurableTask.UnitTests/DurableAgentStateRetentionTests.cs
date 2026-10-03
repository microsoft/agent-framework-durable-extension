// Copyright (c) Microsoft. All rights reserved.

using System.Text;
using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Logging.Abstractions;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class DurableAgentStateRetentionTests
{
    [Fact]
    public void KeepAllNeverDeletes()
    {
        DurableAgentState state = CreateLargeState();
        int originalCount = state.Data.ConversationHistory.Count;

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.KeepAll,
            maxStateBytes: null,
            DateTimeOffset.UtcNow,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.Equal(0, removed);
        Assert.Equal(originalCount, state.Data.ConversationHistory.Count);
    }

    [Fact]
    public void DefaultRetentionModeIsKeepAll()
    {
        DurableAgentsOptions options = new();

        Assert.Equal(
            DurableAgentHistoryRetentionMode.KeepAll,
            options.HistoryRetentionMode);
        Assert.Null(options.MaxStateBytes);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void StateBudgetMustBePositive(int value)
    {
        DurableAgentsOptions options = new();

        _ = Assert.Throws<ArgumentOutOfRangeException>(
            () => options.MaxStateBytes = value);
    }

    [Fact]
    public void AutoRequiresExplicitPositiveBudget()
    {
        DurableAgentsOptions options = new()
        {
            HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto,
        };

        InvalidOperationException exception =
            Assert.Throws<InvalidOperationException>(
                () => _ = options.GetRetentionSettings());

        Assert.Contains("explicit positive", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void KeepAllRejectsInapplicableRetentionConfiguration()
    {
        DurableAgentsOptions options = new()
        {
            MaxStateBytes = 1_000,
        };

        _ = Assert.Throws<InvalidOperationException>(
            () => _ = options.GetRetentionSettings());
        _ = Assert.Throws<InvalidOperationException>(
            () => DurableAgentStateRetention.Enforce(
                CreateRevisedState(),
                DurableAgentHistoryRetentionMode.KeepAll,
                1_000,
                DateTimeOffset.UtcNow,
                NullLogger.Instance,
                new AgentSessionId("agent", "session")));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-0.1)]
    [InlineData(1.1)]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    public void WatermarksMustBeFiniteFractions(double value)
    {
        DurableAgentsOptions options = new();

        _ = Assert.Throws<ArgumentOutOfRangeException>(
            () => options.HistoryRetentionHighWatermark = value);
        _ = Assert.Throws<ArgumentOutOfRangeException>(
            () => options.HistoryRetentionLowWatermark = value);
    }

    [Fact]
    public void AutoUsesValidatedConfigurableWatermarks()
    {
        DurableAgentsOptions options = new()
        {
            HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto,
            MaxStateBytes = 10_000,
            HistoryRetentionHighWatermark = 0.9,
            HistoryRetentionLowWatermark = 0.6,
        };

        DurableAgentRetentionSettings settings = options.GetRetentionSettings();

        Assert.Equal(10_000, settings.MaxStateBytes);
        Assert.Equal(0.9, settings.HighWatermark);
        Assert.Equal(0.6, settings.LowWatermark);

        options.HistoryRetentionLowWatermark = 0.95;
        _ = Assert.Throws<InvalidOperationException>(
            () => _ = options.GetRetentionSettings());
    }

    [Fact]
    public void UndefinedRetentionModeIsRejected()
    {
        DurableAgentsOptions options = new();
        const DurableAgentHistoryRetentionMode Invalid =
            (DurableAgentHistoryRetentionMode)42;

        _ = Assert.Throws<ArgumentOutOfRangeException>(
            () => options.HistoryRetentionMode = Invalid);
        _ = Assert.Throws<ArgumentOutOfRangeException>(
            () => DurableAgentStateRetention.Enforce(
                CreateRevisedState(),
                Invalid,
                1_000,
                DateTimeOffset.UtcNow,
                NullLogger.Instance,
                new AgentSessionId("agent", "session")));
    }

    [Fact]
    public void AutoEvictsOldestExchangeAndRecordsBoundedEvidence()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateLargeState(now);

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            4_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.True(removed > 0);
        Assert.DoesNotContain(state.Data.ConversationHistory, entry => entry.CorrelationId == "oldest");
        Assert.Contains(state.Data.ConversationHistory, entry => entry.CorrelationId == "newest");
        Assert.NotNull(state.Data.Truncation);
        Assert.Equal(removed, ReadCount(state.Data.Truncation.EvictedMessageCount));
        Assert.True(
            DurableAgentStateRetention.GetSerializedSize(state) <
            4_000 * DurableAgentStateRetention.HighWatermark);
    }

    [Fact]
    public void ConfiguredWatermarksControlPressureThresholdAndTarget()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState baseline = CreateLargeState(now);
        DurableAgentState belowHigh = baseline.Clone();
        DurableAgentState underPressure = baseline.Clone();
        int initialSize = DurableAgentStateRetention.GetSerializedSize(belowHigh);
        int budget = (int)Math.Ceiling(initialSize / 0.85);

        RetentionResult? noAction = DurableAgentStateRetention.EnforceForCommit(
            belowHigh,
            new(
                DurableAgentHistoryRetentionMode.Auto,
                budget,
                HighWatermark: 0.90,
                LowWatermark: 0.80),
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));
        RetentionResult? evicted = DurableAgentStateRetention.EnforceForCommit(
            underPressure,
            new(
                DurableAgentHistoryRetentionMode.Auto,
                budget,
                HighWatermark: 0.80,
                LowWatermark: 0.60),
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.Equal(RetentionOutcome.NoAction, noAction?.Outcome);
        Assert.Equal(RetentionOutcome.TranscriptEvicted, evicted?.Outcome);
        Assert.True(
            DurableAgentStateRetention.GetSerializedSize(underPressure) <=
            budget * 0.60);
    }

    [Fact]
    public void AutoDoesNotMoveTruncationEvidenceBackwardWhenClockRegresses()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DateTimeOffset firstEviction = now.AddMinutes(10);
        DateTimeOffset lastEviction = now.AddMinutes(20);
        DurableAgentState state = CreateLargeState(now);
        state.Data.Truncation = new DurableAgentStateTruncation
        {
            EvictedMessageCount = Count(4),
            FirstEvictedAt = firstEviction,
            LastEvictedAt = lastEviction,
        };

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            4_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.True(removed > 0);
        Assert.Equal(firstEviction, state.Data.Truncation?.FirstEvictedAt);
        Assert.Equal(lastEviction, state.Data.Truncation?.LastEvictedAt);
        Assert.Equal(4 + removed, ReadCount(state.Data.Truncation!.EvictedMessageCount));
    }

    [Fact]
    public void AutoIncrementsEvictedMessageCountBeyondInt32()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateLargeState(now);
        state.Data.Truncation = new DurableAgentStateTruncation
        {
            EvictedMessageCount = Count(int.MaxValue),
            FirstEvictedAt = now.AddMinutes(-20),
            LastEvictedAt = now.AddMinutes(-10),
        };

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            4_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.True(removed > 0);
        Assert.Equal(
            (long)int.MaxValue + removed,
            ReadCount(state.Data.Truncation!.EvictedMessageCount));
    }

    [Theory]
    [InlineData("1.0", 1L)]
    [InlineData("1e3", 1_000L)]
    [InlineData("9.223372036854775800e18", 9_223_372_036_854_775_800L)]
    public void AutoIncrementsEquivalentJsonIntegersWithinInt64(
        string countJson,
        long initialCount)
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateLargeState(now);
        state.Data.Truncation = new DurableAgentStateTruncation
        {
            EvictedMessageCount = JsonDocument.Parse(countJson).RootElement.Clone(),
            FirstEvictedAt = now.AddMinutes(-20),
            LastEvictedAt = now.AddMinutes(-10),
        };

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            4_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.True(removed > 0);
        Assert.Equal(
            checked(initialCount + removed),
            ReadCount(state.Data.Truncation!.EvictedMessageCount));
    }

    [Fact]
    public void AutoRejectsCounterOverflowWithoutMutatingState()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateLargeState(now);
        state.Data.Truncation = new DurableAgentStateTruncation
        {
            EvictedMessageCount = Count(long.MaxValue),
            FirstEvictedAt = now.AddMinutes(-20),
            LastEvictedAt = now.AddMinutes(-10),
        };
        string originalState = JsonSerializer.Serialize(
            state,
            DurableAgentStateJsonContext.Default.DurableAgentState);

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => DurableAgentStateRetention.Enforce(
                state,
                DurableAgentHistoryRetentionMode.Auto,
                4_000,
                now,
                NullLogger.Instance,
                new AgentSessionId("agent", "session")));

        Assert.Contains("Int64", exception.Message, StringComparison.Ordinal);
        Assert.Equal(
            originalState,
            JsonSerializer.Serialize(
                state,
                DurableAgentStateJsonContext.Default.DurableAgentState));
    }

    [Fact]
    public void AutoUsesFeasiblePrefixWhenHypotheticalFullEvictionWouldOverflow()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState baseline = CreateRevisedState();
        for (int index = 0; index < 20; index++)
        {
            AddExchange(
                baseline,
                $"correlation-{index}",
                new string((char)('a' + (index % 20)), 500),
                now.AddMinutes(index - 20));
        }

        DurableAgentState control = baseline.Clone();
        control.Data.Truncation = new DurableAgentStateTruncation
        {
            EvictedMessageCount = Count(long.MaxValue - 100),
            FirstEvictedAt = now.AddMinutes(-30),
            LastEvictedAt = now.AddMinutes(-20),
        };
        int removed = DurableAgentStateRetention.Enforce(
            control,
            DurableAgentHistoryRetentionMode.Auto,
            10_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));
        Assert.InRange(removed, 1, 37);

        DurableAgentState state = baseline.Clone();
        state.Data.Truncation = new DurableAgentStateTruncation
        {
            EvictedMessageCount = Count(long.MaxValue - removed),
            FirstEvictedAt = now.AddMinutes(-30),
            LastEvictedAt = now.AddMinutes(-20),
        };
        DurableAgentRetentionSettings settings = new(
            DurableAgentHistoryRetentionMode.Auto,
            MaxStateBytes: 10_000,
            DurableAgentStateRetention.DefaultHighWatermark,
            DurableAgentStateRetention.DefaultLowWatermark);

        DurableAgentStateRetention.ValidateProtectedFloor(
            state,
            settings,
            now);
        int actualRemoved = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            10_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.Equal(removed, actualRemoved);
        Assert.Equal(
            long.MaxValue,
            ReadCount(state.Data.Truncation!.EvictedMessageCount));
        Assert.Equal(
            control.Data.ConversationHistory.Select(entry => entry.CorrelationId),
            state.Data.ConversationHistory.Select(entry => entry.CorrelationId));
    }

    [Theory]
    [InlineData("9223372036854775808")]
    [InlineData("9.223372036854775808e18")]
    [InlineData("1e4096")]
    public void AutoRejectsOutOfRangeCounterWithoutMutatingState(string countJson)
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateLargeState(now);
        state.Data.Truncation = new DurableAgentStateTruncation
        {
            EvictedMessageCount = JsonDocument.Parse(countJson).RootElement.Clone(),
            FirstEvictedAt = now.AddMinutes(-20),
            LastEvictedAt = now.AddMinutes(-10),
        };
        DurableAgentStateTruncation originalTruncation = state.Data.Truncation;
        DurableAgentStateEntry[] originalHistory =
            [.. state.Data.ConversationHistory];

        _ = Assert.Throws<InvalidOperationException>(
            () => DurableAgentStateRetention.Enforce(
                state,
                DurableAgentHistoryRetentionMode.Auto,
                4_000,
                now,
                NullLogger.Instance,
                new AgentSessionId("agent", "session")));

        Assert.Same(originalTruncation, state.Data.Truncation);
        Assert.Equal(countJson, state.Data.Truncation.EvictedMessageCount.GetRawText());
        Assert.Equal(originalHistory, state.Data.ConversationHistory);
    }

    [Fact]
    public void AutoPreservesSystemExchange()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateLargeState(now);
        state.Data.ConversationHistory.Insert(
            0,
            CreateRequest("system", ChatRole.System, new string('s', 500), now.AddMinutes(-10)));

        _ = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            5_200,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.Contains(state.Data.ConversationHistory, entry => entry.CorrelationId == "system");
    }

    [Fact]
    public void AutoPreservesWholeSameCorrelationSystemExchange()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        state.Data.ConversationHistory.Add(
            CreateRequest("system", ChatRole.System, new string('s', 500), now.AddMinutes(-20)));
        state.Data.ConversationHistory.Add(
            CreateResponse("system", new string('a', 500), now.AddMinutes(-20)));
        AddExchange(state, "evictable", new string('e', 4_000), now.AddMinutes(-10));
        AddExchange(state, "newest", "newest", now);

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            4_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.True(removed > 0);
        Assert.Equal(
            2,
            state.Data.ConversationHistory.Count(entry => entry.CorrelationId == "system"));
        Assert.DoesNotContain(
            state.Data.ConversationHistory,
            entry => entry.CorrelationId == "evictable");
        Assert.Contains(
            state.Data.ConversationHistory,
            entry => entry.CorrelationId == "newest");
    }

    [Fact]
    public void AutoPreservesSystemCorrelationBridgedThroughNewestToolOccurrence()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        AddExchange(state, "filler", new string('f', 5_000), now.AddMinutes(-30));
        state.Data.ConversationHistory.Add(
            CreateRequest("call", ChatRole.User, "call request", now.AddMinutes(-20)));
        state.Data.ConversationHistory.Add(
            CreateToolCallResponse("call", "shared-call", "invoke", now.AddMinutes(-20)));
        state.Data.ConversationHistory.Add(
            new DurableAgentStateRequest
            {
                CorrelationId = "system",
                CreatedAt = now.AddMinutes(-10),
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(
                            ChatRole.System,
                            [new FunctionResultContent("shared-call", "system result")])
                        {
                            CreatedAt = now.AddMinutes(-10),
                        }),
                ],
            });
        state.Data.ConversationHistory.Add(
            CreateResponse("system", "system response", now.AddMinutes(-10)));
        state.Data.ConversationHistory.Add(
            CreateToolResultRequest("newest", "shared-call", "latest result", now));

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            5_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.True(removed > 0);
        Assert.DoesNotContain(
            state.Data.ConversationHistory,
            entry => entry.CorrelationId == "filler");
        Assert.Equal(
            2,
            state.Data.ConversationHistory.Count(entry => entry.CorrelationId == "system"));
        Assert.Equal(
            2,
            state.Data.ConversationHistory.Count(entry => entry.CorrelationId == "call"));
        Assert.Contains(
            state.Data.ConversationHistory,
            entry => entry.CorrelationId == "newest");
    }

    [Fact]
    public void AutoPreservesNewestExchange()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateLargeState(now);

        _ = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            4_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.Contains(state.Data.ConversationHistory, entry => entry.CorrelationId == "newest");
    }

    [Fact]
    public void AutoPreservesMailboxResultWhileEvictingTranscript()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        AddExchange(state, "old", new string('o', 400), now.AddMinutes(-5));
        AddExchange(state, "completed", new string('a', 400), now.AddSeconds(-30));
        AddExchange(state, "newest", new string('b', 400), now);
        AddMailboxResult(state, "completed", "authoritative result", now.AddSeconds(-30));

        _ = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            4_500,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.DoesNotContain(state.Data.ConversationHistory, entry => entry.CorrelationId == "old");
        Assert.DoesNotContain(state.Data.ConversationHistory, entry => entry.CorrelationId == "completed");
        Assert.Contains(state.Data.ConversationHistory, entry => entry.CorrelationId == "newest");
        DurableAgentRunOutcome outcome =
            DurableAgentStateOutcomeResolver.Resolve(state, "completed", now);
        Assert.Equal(DurableAgentRunOutcomeKind.Succeeded, outcome.Kind);
        Assert.Equal("authoritative result", outcome.Response?.Text);
        Assert.Contains("completed", state.Data.CompletionReceipts!.Keys);
    }

    [Fact]
    public void AutoCanRemoveZeroMessagePrefixWithoutTruncationEvidence()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        for (int index = 0; index < 3; index++)
        {
            state.Data.ConversationHistory.Add(
                new DurableAgentStateCompaction
                {
                    CreatedAt = now.AddMinutes(index - 4),
                });
        }

        state.Data.ConversationHistory.Add(
            new DurableAgentStateCompaction
            {
                CreatedAt = now.AddMinutes(-1),
                Messages =
                [
                    new DurableAgentStateMessage
                    {
                        Role = ChatRole.Assistant.Value,
                    },
                ],
            });
        state.Data.ConversationHistory.Add(
            new DurableAgentStateCompaction
            {
                CreatedAt = now,
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.Assistant, new string('p', 500))),
                ],
            });

        int sizeAfterTwo = MeasureProjectedPrefix(state.Clone(), 2, now);
        int sizeAfterThree = MeasureProjectedPrefix(state.Clone(), 3, now);
        Assert.True(sizeAfterThree < sizeAfterTwo);
        int maxStateBytes = (int)Math.Ceiling(
            sizeAfterThree / DurableAgentStateRetention.LowWatermark);
        int lowWatermark = (int)(
            maxStateBytes * DurableAgentStateRetention.LowWatermark);
        Assert.InRange(lowWatermark, sizeAfterThree, sizeAfterTwo - 1);

        RetentionResult? result = DurableAgentStateRetention.EnforceForCommit(
            state,
            new(
                DurableAgentHistoryRetentionMode.Auto,
                maxStateBytes,
                HighWatermark: 0.71,
                LowWatermark: DurableAgentStateRetention.LowWatermark),
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.Equal(0, result?.RemovedMessageCount);
        Assert.Equal(2, state.Data.ConversationHistory.Count);
        Assert.Null(state.Data.Truncation);
        Assert.Equal(sizeAfterThree, DurableAgentStateRetention.GetSerializedSize(state));
    }

    [Fact]
    public void AutoFailsWhenProtectedMailboxAndNewestTranscriptAloneExceedBudget()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        AddMailboxResult(state, "completed", new string('m', 2_000), now.AddMinutes(-5));
        AddExchange(state, "newest", new string('c', 500), now);
        int protectedFloorSize = DurableAgentStateRetention.GetSerializedSize(state);
        const int HighWatermark =
            (int)(1_500 * DurableAgentStateRetention.HighWatermark);

        Assert.True(protectedFloorSize >= HighWatermark);

        DurableAgentStateSizeLimitExceededException exception =
            Assert.Throws<DurableAgentStateSizeLimitExceededException>(
            () => DurableAgentStateRetention.Enforce(
                state,
                DurableAgentHistoryRetentionMode.Auto,
                1_500,
                now,
                NullLogger.Instance,
                new AgentSessionId("agent", "session")));

        Assert.Equal(protectedFloorSize, exception.StateSizeBytes);
        Assert.Contains("completed", state.Data.TerminalResults!.Keys);
        Assert.Contains("completed", state.Data.CompletionReceipts!.Keys);
        Assert.Contains(state.Data.ConversationHistory, entry => entry.CorrelationId == "newest");
        Assert.Equal(2, state.Data.ConversationHistory.Count);
    }

    [Fact]
    public void AutoFailsRatherThanPersistProtectedStateOverSafeThreshold()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        AddExchange(state, "newest", new string('x', 2_000), now);

        DurableAgentStateSizeLimitExceededException exception =
            Assert.Throws<DurableAgentStateSizeLimitExceededException>(
                () => DurableAgentStateRetention.Enforce(
                    state,
                    DurableAgentHistoryRetentionMode.Auto,
                    500,
                    now,
                    NullLogger.Instance,
                    new AgentSessionId("agent", "session")));

        Assert.True(exception.StateSizeBytes >= 500 * DurableAgentStateRetention.HighWatermark);
        Assert.Equal(500, exception.MaxStateBytes);
        Assert.Equal(2, state.Data.ConversationHistory.Count);
    }

    [Fact]
    public void AutoRemovesToolCallAndResultAtomicallyWithExchange()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        DurableAgentStateRequest request = CreateRequest("tools", ChatRole.User, new string('a', 400), now.AddMinutes(-10));
        DurableAgentStateResponse response = DurableAgentStateResponse.FromResponse(
            "tools",
            new AgentResponse(
                new ChatMessage(
                    ChatRole.Assistant,
                    [
                        new FunctionCallContent("call", "tool"),
                        new FunctionResultContent("call", "result"),
                    ])
                {
                    CreatedAt = now.AddMinutes(-10),
                }));
        state.Data.ConversationHistory.Add(request);
        state.Data.ConversationHistory.Add(response);
        AddExchange(state, "newest", new string('b', 400), now);

        _ = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            3_500,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.DoesNotContain(state.Data.ConversationHistory, entry => entry.CorrelationId == "tools");
        Assert.DoesNotContain(
            state.Data.ConversationHistory.SelectMany(entry => entry.Messages).SelectMany(message => message.Contents),
            content => content is DurableAgentStateFunctionCallContent or DurableAgentStateFunctionResultContent);
    }

    [Fact]
    public void AutoEvictsToolCallAndResultAcrossDifferentCorrelations()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        state.Data.ConversationHistory.Add(
            CreateRequest("call", ChatRole.User, "invoke", now.AddMinutes(-10)));
        state.Data.ConversationHistory.Add(
            CreateToolCallResponse("call", "shared-call", new string('a', 2_000), now.AddMinutes(-10)));
        state.Data.ConversationHistory.Add(
            CreateToolResultRequest("result", "shared-call", new string('b', 2_000), now.AddMinutes(-9)));
        state.Data.ConversationHistory.Add(
            CreateResponse("result", "after tool", now.AddMinutes(-9)));
        AddExchange(state, "newest", new string('c', 400), now);

        _ = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            5_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.DoesNotContain(
            state.Data.ConversationHistory,
            entry => entry.CorrelationId is "call" or "result");
        Assert.False(ContainsToolCallId(state, "shared-call"));
        Assert.Contains(state.Data.ConversationHistory, entry => entry.CorrelationId == "newest");
        Assert.True(
            DurableAgentStateRetention.GetSerializedSize(state) <
            5_000 * DurableAgentStateRetention.HighWatermark);
    }

    [Fact]
    public void AutoTreatsInterleavedToolCallsAsOneConnectedComponent()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        state.Data.ConversationHistory.Add(
            CreateRequest("calls", ChatRole.User, "invoke", now.AddMinutes(-10)));
        state.Data.ConversationHistory.Add(
            CreateToolCallResponse(
                "calls",
                now.AddMinutes(-10),
                ("call-a", new string('a', 1_000)),
                ("call-b", new string('b', 1_000))));
        state.Data.ConversationHistory.Add(
            CreateToolResultRequest("result-a", "call-a", new string('c', 1_000), now.AddMinutes(-9)));
        state.Data.ConversationHistory.Add(
            CreateResponse("result-a", "after a", now.AddMinutes(-9)));
        state.Data.ConversationHistory.Add(
            CreateToolResultRequest("result-b", "call-b", new string('d', 1_000), now.AddMinutes(-8)));
        state.Data.ConversationHistory.Add(
            CreateResponse("result-b", "after b", now.AddMinutes(-8)));
        AddExchange(state, "newest", new string('e', 400), now);

        _ = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            5_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.DoesNotContain(
            state.Data.ConversationHistory,
            entry => entry.CorrelationId is "calls" or "result-a" or "result-b");
        Assert.False(ContainsToolCallId(state, "call-a"));
        Assert.False(ContainsToolCallId(state, "call-b"));
        Assert.Contains(state.Data.ConversationHistory, entry => entry.CorrelationId == "newest");
    }

    [Fact]
    public void AutoProtectsWholeToolComponentWhenResultIsInNewestExchange()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        AddExchange(state, "filler", new string('f', 7_000), now.AddMinutes(-20));
        state.Data.ConversationHistory.Add(
            CreateRequest("call", ChatRole.User, "invoke", now.AddMinutes(-10)));
        state.Data.ConversationHistory.Add(
            CreateToolCallResponse("call", "protected-call", new string('a', 500), now.AddMinutes(-10)));
        state.Data.ConversationHistory.Add(
            CreateToolResultRequest("newest", "protected-call", new string('b', 500), now));
        state.Data.ConversationHistory.Add(
            CreateResponse("newest", "final", now));

        _ = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            5_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.DoesNotContain(state.Data.ConversationHistory, entry => entry.CorrelationId == "filler");
        Assert.Equal(
            2,
            state.Data.ConversationHistory.Count(entry => entry.CorrelationId == "call"));
        Assert.Equal(
            2,
            state.Data.ConversationHistory.Count(entry => entry.CorrelationId == "newest"));
        Assert.True(ContainsToolCallId(state, "protected-call"));
    }

    [Fact]
    public void AutoProtectsMultiHopCorrelationAndToolChain()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        AddExchange(state, "filler", new string('f', 7_000), now.AddMinutes(-30));
        state.Data.ConversationHistory.Add(
            CreateRequest("first", ChatRole.User, "first request", now.AddMinutes(-20)));
        state.Data.ConversationHistory.Add(
            CreateToolCallResponse("first", "call-a", "first call", now.AddMinutes(-20)));
        state.Data.ConversationHistory.Add(
            CreateToolResultRequest("middle", "call-a", "first result", now.AddMinutes(-10)));
        state.Data.ConversationHistory.Add(
            CreateToolCallResponse("middle", "call-b", "second call", now.AddMinutes(-10)));
        state.Data.ConversationHistory.Add(
            CreateToolResultRequest("newest", "call-b", "second result", now));
        state.Data.ConversationHistory.Add(
            CreateResponse("newest", "final", now));

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            5_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.True(removed > 0);
        Assert.DoesNotContain(state.Data.ConversationHistory, entry => entry.CorrelationId == "filler");
        Assert.Equal(
            2,
            state.Data.ConversationHistory.Count(entry => entry.CorrelationId == "first"));
        Assert.Equal(
            2,
            state.Data.ConversationHistory.Count(entry => entry.CorrelationId == "middle"));
        Assert.Equal(
            2,
            state.Data.ConversationHistory.Count(entry => entry.CorrelationId == "newest"));
    }

    [Fact]
    public void AutoProtectsCorrelationReachedThroughReverseOrderedToolLink()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        AddExchange(state, "filler", new string('f', 7_000), now.AddMinutes(-30));
        DurableAgentStateRequest linkedResult =
            CreateToolResultRequest("linked", "reverse-call", "result first", now.AddMinutes(-20));
        DurableAgentStateRequest linkedRequest =
            CreateRequest("linked", ChatRole.User, "same correlation", now.AddMinutes(-20));
        DurableAgentStateResponse linkedCall =
            CreateToolCallResponse("linked", "reverse-call", "call later", now.AddMinutes(-10));
        state.Data.ConversationHistory.Add(linkedResult);
        state.Data.ConversationHistory.Add(linkedRequest);
        state.Data.ConversationHistory.Add(linkedCall);
        state.Data.ConversationHistory.Add(
            CreateToolCallResponse("newest", "reverse-call", "latest occurrence", now));

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            5_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.True(removed > 0);
        Assert.DoesNotContain(state.Data.ConversationHistory, entry => entry.CorrelationId == "filler");
        Assert.Equal(
            3,
            state.Data.ConversationHistory.Count(entry => entry.CorrelationId == "linked"));
        Assert.Contains(
            state.Data.ConversationHistory,
            entry => entry.Messages.Any(message => message.ToChatMessage().Text == "same correlation"));
        Assert.True(ContainsToolCallId(state, "reverse-call"));
    }

    [Fact]
    public void AutoProtectsTwelveCallNewestConnectedComponent()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        const int ToolCount = 12;
        AddExchange(state, "filler", new string('f', 30_000), now.AddMinutes(-30));
        state.Data.ConversationHistory.Add(
            CreateRequest("turn-0", ChatRole.User, "start", now.AddMinutes(-ToolCount)));
        for (int index = 0; index < ToolCount; index++)
        {
            state.Data.ConversationHistory.Add(
                CreateToolCallResponse(
                    $"turn-{index}",
                    $"call-{index}",
                    new string('c', 150),
                    now.AddMinutes(index - ToolCount)));
            state.Data.ConversationHistory.Add(
                CreateToolResultRequest(
                    $"turn-{index + 1}",
                    $"call-{index}",
                    new string('r', 150),
                    now.AddMinutes(index - ToolCount + 1)));
        }

        state.Data.ConversationHistory.Add(
            CreateResponse($"turn-{ToolCount}", "final", now));

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            25_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.True(removed > 0);
        Assert.DoesNotContain(
            state.Data.ConversationHistory,
            entry => entry.CorrelationId == "filler");
        Assert.Equal(
            (ToolCount * 2) + 2,
            state.Data.ConversationHistory.Count);
        Assert.All(
            GetToolCallIdCounts(state),
            pair => Assert.Equal(2, pair.Value));
    }

    [Fact]
    public void AutoEvictsDisconnectedTwelveCallComponent()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        const int ToolCount = 12;
        state.Data.ConversationHistory.Add(
            CreateRequest("turn-0", ChatRole.User, "start", now.AddMinutes(-ToolCount)));
        for (int index = 0; index < ToolCount; index++)
        {
            state.Data.ConversationHistory.Add(
                CreateToolCallResponse(
                    $"turn-{index}",
                    $"call-{index}",
                    new string('c', 900),
                    now.AddMinutes(index - ToolCount)));
            state.Data.ConversationHistory.Add(
                CreateToolResultRequest(
                    $"turn-{index + 1}",
                    $"call-{index}",
                    new string('r', 900),
                    now.AddMinutes(index - ToolCount + 1)));
        }

        state.Data.ConversationHistory.Add(
            CreateResponse($"turn-{ToolCount}", "old final", now.AddMinutes(-1)));
        AddExchange(state, "newest", "newest", now);

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            9_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.True(removed > 0);
        Assert.DoesNotContain(
            state.Data.ConversationHistory,
            entry => entry.CorrelationId?.StartsWith(
                "turn-",
                StringComparison.Ordinal) == true);
        Assert.Empty(GetToolCallIdCounts(state));
        Assert.Equal(
            2,
            state.Data.ConversationHistory.Count(entry => entry.CorrelationId == "newest"));
        Assert.True(
            DurableAgentStateRetention.GetSerializedSize(state) <=
            9_000 * DurableAgentStateRetention.LowWatermark);
    }

    [Fact]
    public void AutoEvictsDisconnectedCyclicToolComponent()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        state.Data.ConversationHistory.Add(
            CreateToolCallResponse("cycle-a", "call-a", new string('a', 2_000), now.AddMinutes(-20)));
        state.Data.ConversationHistory.Add(
            CreateToolResultRequest("cycle-b", "call-a", new string('b', 2_000), now.AddMinutes(-15)));
        state.Data.ConversationHistory.Add(
            CreateToolCallResponse("cycle-b", "call-b", new string('c', 2_000), now.AddMinutes(-10)));
        state.Data.ConversationHistory.Add(
            CreateToolResultRequest("cycle-a", "call-b", new string('d', 2_000), now.AddMinutes(-5)));
        AddExchange(state, "newest", "newest", now);

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            5_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.True(removed > 0);
        Assert.DoesNotContain(
            state.Data.ConversationHistory,
            entry => entry.CorrelationId is "cycle-a" or "cycle-b");
        Assert.Empty(GetToolCallIdCounts(state));
        Assert.Equal(
            2,
            state.Data.ConversationHistory.Count(entry => entry.CorrelationId == "newest"));
    }

    [Fact]
    public void ProtectedFloorPreflightSkipsCloneWhenNoTranscriptCanBeEvicted()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        DurableAgentStateRequest pending =
            CreateRequest("newest", ChatRole.System, new string('s', 2_000), now);
        DurableAgentStateRetention.ExecutionStatistics statistics = new();

        _ = Assert.Throws<DurableAgentStateSizeLimitExceededException>(
            () => DurableAgentStateRetention.ValidateProtectedFloor(
                state,
                new(
                    DurableAgentHistoryRetentionMode.Auto,
                    MaxStateBytes: 500,
                    DurableAgentStateRetention.DefaultHighWatermark,
                    DurableAgentStateRetention.DefaultLowWatermark),
                now,
                pending,
                statistics));

        Assert.Empty(state.Data.ConversationHistory);
        Assert.Equal(0, statistics.ProtectedFloorCloneCount);
        Assert.Equal(1, statistics.SerializedStateMeasurementCount);
    }

    [Fact]
    public void AutoTreatsDuplicateToolIdsAsOneConservativeGroup()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        AddExchange(state, "filler", new string('f', 7_000), now.AddMinutes(-20));
        state.Data.ConversationHistory.Add(
            CreateRequest("first", ChatRole.User, "first request", now.AddMinutes(-10)));
        state.Data.ConversationHistory.Add(
            CreateToolCallResponse("first", "duplicate", new string('a', 500), now.AddMinutes(-10)));
        state.Data.ConversationHistory.Add(
            CreateRequest("second", ChatRole.User, "second request", now.AddMinutes(-5)));
        state.Data.ConversationHistory.Add(
            CreateToolCallResponse("second", "duplicate", new string('b', 500), now.AddMinutes(-5)));
        state.Data.ConversationHistory.Add(
            CreateToolResultRequest("newest", "duplicate", "result", now));
        state.Data.ConversationHistory.Add(
            CreateResponse("newest", "final", now));

        _ = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            6_500,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.DoesNotContain(state.Data.ConversationHistory, entry => entry.CorrelationId == "filler");
        Assert.Equal(
            2,
            state.Data.ConversationHistory.Count(entry => entry.CorrelationId == "first"));
        Assert.Equal(
            2,
            state.Data.ConversationHistory.Count(entry => entry.CorrelationId == "second"));
        Assert.Equal(
            2,
            state.Data.ConversationHistory.Count(entry => entry.CorrelationId == "newest"));
    }

    [Fact]
    public void AutoDoesNotConnectOrphanedToolContentWithDifferentIds()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        state.Data.ConversationHistory.Add(
            CreateToolCallResponse("orphan-call", "call-only", new string('a', 5_000), now.AddMinutes(-10)));
        state.Data.ConversationHistory.Add(
            CreateToolResultRequest("orphan-result", "result-only", new string('b', 500), now.AddMinutes(-5)));
        state.Data.ConversationHistory.Add(
            CreateResponse("orphan-result", "after orphan", now.AddMinutes(-5)));
        AddExchange(state, "newest", new string('c', 400), now);

        _ = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            7_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.DoesNotContain(state.Data.ConversationHistory, entry => entry.CorrelationId == "orphan-call");
        Assert.Contains(state.Data.ConversationHistory, entry => entry.CorrelationId == "orphan-result");
        Assert.True(ContainsToolCallId(state, "result-only"));
    }

    [Fact]
    public void AutoDoesNotConnectToolContentWithMissingIds()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        state.Data.ConversationHistory.Add(
            CreateToolCallResponse("missing-call", string.Empty, new string('a', 5_000), now.AddMinutes(-10)));
        state.Data.ConversationHistory.Add(
            CreateToolResultRequest("missing-result", string.Empty, new string('b', 500), now.AddMinutes(-5)));
        state.Data.ConversationHistory.Add(
            CreateResponse("missing-result", "after orphan", now.AddMinutes(-5)));
        AddExchange(state, "newest", new string('c', 400), now);

        _ = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            7_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.DoesNotContain(state.Data.ConversationHistory, entry => entry.CorrelationId == "missing-call");
        Assert.Contains(state.Data.ConversationHistory, entry => entry.CorrelationId == "missing-result");
    }

    [Fact]
    public void AutoDoesNotConnectToolContentWithWhitespaceIds()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        state.Data.ConversationHistory.Add(
            CreateToolCallResponse("malformed-call", " ", new string('a', 5_000), now.AddMinutes(-10)));
        state.Data.ConversationHistory.Add(
            CreateToolResultRequest("malformed-result", " ", new string('b', 500), now.AddMinutes(-5)));
        state.Data.ConversationHistory.Add(
            CreateResponse("malformed-result", "after malformed", now.AddMinutes(-5)));
        AddExchange(state, "newest", new string('c', 400), now);

        _ = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            7_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.DoesNotContain(
            state.Data.ConversationHistory,
            entry => entry.CorrelationId == "malformed-call");
        Assert.Equal(
            2,
            state.Data.ConversationHistory.Count(entry => entry.CorrelationId == "malformed-result"));
    }

    [Fact]
    public void SerializedSizeIncludesSessionAndTruncation()
    {
        DurableAgentState state = CreateRevisedState();
        int emptySize = DurableAgentStateRetention.GetSerializedSize(state);
        state.Data.Session = JsonSerializer.SerializeToElement(new { conversationId = new string('c', 100) });
        state.Data.Truncation = new DurableAgentStateTruncation
        {
            EvictedMessageCount = Count(2),
            FirstEvictedAt = DateTimeOffset.UtcNow,
            LastEvictedAt = DateTimeOffset.UtcNow,
        };

        int completeSize = DurableAgentStateRetention.GetSerializedSize(state);

        Assert.True(completeSize > emptySize + 100);
    }

    [Fact]
    public void SerializedSizeMeasuresEscapedStorageEnvelope()
    {
        DurableAgentState state = CreateRevisedState();
        AddExchange(
            state,
            "quoted",
            string.Concat(Enumerable.Repeat("\"é漢\\", 300)),
            DateTimeOffset.UtcNow);
        string converterPayload = JsonSerializer.Serialize(
            state,
            DurableAgentStateJsonContext.Default.DurableAgentState);
        int rawPayloadBytes = Encoding.UTF8.GetByteCount(converterPayload);
        int escapedEnvelopeBytes = JsonSerializer.SerializeToUtf8Bytes(
            converterPayload,
            DurableAgentStateJsonContext.Default.String).Length;

        Assert.Equal(
            escapedEnvelopeBytes,
            DurableAgentStateRetention.GetSerializedSize(state));
        Assert.True(escapedEnvelopeBytes > rawPayloadBytes);
    }

    [Fact]
    public void AutoPreservesMailboxContinuationBindingTtlAndBookkeepingFloor()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState(
            DurableAgentStateHistoryBinding.HistoryProviderOwner,
            "external-history.v1");
        AddMailboxResult(state, "completed", new string('r', 2_000), now.AddMinutes(-5));
        state.Data.Session = JsonSerializer.SerializeToElement(
            new { continuation = new string('s', 2_000) });
        state.Data.ExpirationTimeUtc = now.AddDays(1).UtcDateTime;
        state.Data.IngestedPositions = new Dictionary<string, JsonElement>
        {
            ["workflow"] = Count(42),
        };
        state.Data.UnknownProperties = new Dictionary<string, JsonElement>
        {
            ["control"] = JsonSerializer.SerializeToElement(new string('e', 1_000)),
        };

        DurableAgentStateSizeLimitExceededException exception =
            Assert.Throws<DurableAgentStateSizeLimitExceededException>(
                () => DurableAgentStateRetention.Enforce(
                    state,
                    DurableAgentHistoryRetentionMode.Auto,
                    1_000,
                    now,
                    NullLogger.Instance,
                    new AgentSessionId("agent", "session")));

        Assert.Empty(state.Data.ConversationHistory);
        Assert.Contains("completed", state.Data.TerminalResults!.Keys);
        Assert.Contains("completed", state.Data.CompletionReceipts!.Keys);
        Assert.Equal(
            "external-history.v1",
            DurableAgentHistoryBinding.Parse(state.Data.HistoryBinding)?.ProviderKey);
        Assert.NotNull(state.Data.Session);
        Assert.NotNull(state.Data.ExpirationTimeUtc);
        Assert.Equal(42, ReadCount(state.Data.IngestedPositions!["workflow"]));
        Assert.True(exception.StateSizeBytes > exception.MaxStateBytes);
    }

    [Fact]
    public void AutoPreservesLosslessStructuredResultAndUnavailableReceipt()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        DurableAgentStateOutcomeResolver.AddSuccessfulResult(
            state,
            "lossless",
            new AgentResponse(new ChatMessage(ChatRole.Assistant, "mailbox")),
            now.AddMinutes(-5),
            structuredValue: JsonSerializer.SerializeToElement(
                new { count = 3, label = "retained" }));
        DurableAgentStateOutcomeResolver.AddSuccessfulResult(
            state,
            "unavailable",
            new AgentResponse(new ChatMessage(ChatRole.Assistant, "expired")),
            now.AddMinutes(-5),
            resultExpiresAt: now.AddMinutes(-1));
        Assert.True(
            DurableAgentStateOutcomeResolver.MarkExpiredResultUnavailable(
                state,
                "unavailable",
                now));
        AddExchange(state, "old", new string('x', 4_000), now.AddMinutes(-10));
        AddExchange(state, "newest", "newest", now);

        _ = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            4_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        DurableAgentRunOutcome lossless =
            DurableAgentStateOutcomeResolver.Resolve(state, "lossless", now);
        Assert.Equal(DurableAgentRunOutcomeKind.Succeeded, lossless.Kind);
        Assert.Equal(3, lossless.Value.GetProperty("count").GetInt32());
        Assert.Equal("retained", lossless.Value.GetProperty("label").GetString());
        DurableAgentRunOutcome unavailable =
            DurableAgentStateOutcomeResolver.Resolve(state, "unavailable", now);
        Assert.Equal(
            DurableAgentRunOutcomeKind.CompletedResultUnavailable,
            unavailable.Kind);
        Assert.Equal(
            DurableAgentStateCompletionReceipt.UnavailableResult,
            unavailable.Receipt?.ResultState);
        Assert.DoesNotContain("unavailable", state.Data.TerminalResults!.Keys);
    }

    [Fact]
    public void AutoAccountsForMixedTextMediaAndMetadataWhenEvictingTranscript()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        ChatMessage oldMessage = new(
            ChatRole.User,
            [
                new TextContent(new string('t', 2_000)),
                new DataContent(
                    "data:application/octet-stream;base64," +
                    Convert.ToBase64String(new byte[4_000]),
                    mediaType: null),
            ])
        {
            CreatedAt = now.AddMinutes(-5),
            AdditionalProperties = new()
            {
                ["metadata"] = new string('m', 2_000),
            },
        };
        state.Data.ConversationHistory.Add(
            new DurableAgentStateRequest
            {
                CorrelationId = "mixed",
                CreatedAt = now.AddMinutes(-5),
                Messages = [DurableAgentStateMessage.FromChatMessage(oldMessage)],
            });
        state.Data.ConversationHistory.Add(
            CreateResponse("mixed", "old response", now.AddMinutes(-5)));
        AddExchange(state, "newest", "newest", now);
        int initialSize = DurableAgentStateRetention.GetSerializedSize(state);

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            4_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.True(removed > 0);
        Assert.DoesNotContain(state.Data.ConversationHistory, entry => entry.CorrelationId == "mixed");
        Assert.True(DurableAgentStateRetention.GetSerializedSize(state) < initialSize);
    }

    [Fact]
    public void RetentionModesAreOnlyKeepAllAndAuto()
    {
        Assert.Equal(
            [nameof(DurableAgentHistoryRetentionMode.KeepAll), nameof(DurableAgentHistoryRetentionMode.Auto)],
            Enum.GetNames<DurableAgentHistoryRetentionMode>());
    }

    [Fact]
    public void AutoRejectsLegacyLayoutBeforeRemovingTerminalEvidence()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = new();
        AddExchange(state, "legacy", new string('x', 2_000), now.AddMinutes(-5));
        AddExchange(state, "newest", "newest", now);
        string original = JsonSerializer.Serialize(
            state,
            DurableAgentStateJsonContext.Default.DurableAgentState);

        DurableAgentStateCorruptionException exception =
            Assert.Throws<DurableAgentStateCorruptionException>(
                () => DurableAgentStateRetention.Enforce(
                    state,
                    DurableAgentHistoryRetentionMode.Auto,
                    1_000,
                    now,
                    NullLogger.Instance,
                    new AgentSessionId("agent", "session")));

        Assert.Contains("schema 2 mailbox", exception.Message, StringComparison.Ordinal);
        Assert.Equal(
            original,
            JsonSerializer.Serialize(
                state,
                DurableAgentStateJsonContext.Default.DurableAgentState));
    }

    [Fact]
    public void AutoCanEvictOlderCorrelationlessCompaction()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        state.Data.ConversationHistory.Add(
            new DurableAgentStateCompaction
            {
                CreatedAt = now.AddMinutes(-5),
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.Assistant, new string('a', 1_000))),
                ],
            });
        state.Data.ConversationHistory.Add(
            new DurableAgentStateCompaction
            {
                CreatedAt = now,
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.Assistant, "newest")),
                ],
            });

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            1_200,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.Equal(1, removed);
        DurableAgentStateCompaction remaining =
            Assert.IsType<DurableAgentStateCompaction>(Assert.Single(state.Data.ConversationHistory));
        Assert.Equal("newest", remaining.Messages[0].ToChatMessage().Text);
    }

    [Fact]
    public void AutoRemovesOnlyOneOfDuplicateEqualCorrelationlessEntriesByIdentity()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        for (int index = 0; index < 2; index++)
        {
            state.Data.ConversationHistory.Add(
                new DurableAgentStateCompaction
                {
                    CreatedAt = now,
                    Messages =
                    [
                        DurableAgentStateMessage.FromChatMessage(
                            new ChatMessage(
                                ChatRole.Assistant,
                                new string('d', 1_000))),
                    ],
                });
        }

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            3_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.Equal(1, removed);
        Assert.Single(state.Data.ConversationHistory);
        Assert.Equal(
            new string('d', 1_000),
            state.Data.ConversationHistory[0].Messages[0].ToChatMessage().Text);
    }

    [Fact]
    public void AutoProtectsActualNewestCorrelationlessTranscriptComponent()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        AddExchange(state, "older", new string('o', 4_000), now.AddMinutes(-5));
        state.Data.ConversationHistory.Add(
            new DurableAgentStateCompaction
            {
                CreatedAt = now,
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.Assistant, new string('n', 500))),
                ],
            });

        _ = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            2_500,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        DurableAgentStateCompaction newest =
            Assert.Single(state.Data.ConversationHistory.OfType<DurableAgentStateCompaction>());
        Assert.Equal(new string('n', 500), newest.Messages[0].ToChatMessage().Text);
        Assert.DoesNotContain(
            state.Data.ConversationHistory,
            entry => entry.CorrelationId == "older");
    }

    [Fact]
    public void AutoUsesBoundedExactMeasurementsForManyIndependentExchanges()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        const int ExchangeCount = 120;
        for (int index = 0; index < ExchangeCount; index++)
        {
            AddExchange(
                state,
                $"exchange-{index:D3}",
                new string((char)('a' + (index % 26)), 200),
                now.AddMinutes(index - ExchangeCount));
        }

        AddExchange(state, "newest", "protected newest", now);
        DurableAgentStateRetention.ExecutionStatistics statistics = new();

        int removedMessages = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            8_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"),
            statistics);

        Assert.True(removedMessages > 100);
        Assert.Equal(1, statistics.CandidateGroupingPassCount);
        Assert.InRange(statistics.SerializedStateMeasurementCount, 3, 20);
        Assert.True(
            statistics.SerializedStateMeasurementCount <
            (removedMessages / 4));
        Assert.Contains(
            state.Data.ConversationHistory,
            entry => entry.CorrelationId == "newest");
        Assert.True(
            DurableAgentStateRetention.GetSerializedSize(state) <=
            8_000 * DurableAgentStateRetention.LowWatermark);
    }

    private static DurableAgentState CreateLargeState(DateTimeOffset? now = null)
    {
        DateTimeOffset current = now ?? DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        AddExchange(state, "oldest", new string('a', 500), current.AddMinutes(-10));
        AddExchange(state, "middle", new string('b', 500), current.AddMinutes(-5));
        AddExchange(state, "newest", new string('c', 500), current);
        return state;
    }

    private static DurableAgentState CreateRevisedState(
        string ownerKind = DurableAgentStateHistoryBinding.DurableStateOwner,
        string providerKey = DurableAgentHistoryBinding.DurableStateProviderKey)
    {
        DurableAgentHistoryOwnership ownership = ownerKind switch
        {
            DurableAgentStateHistoryBinding.DurableStateOwner =>
                DurableAgentHistoryOwnership.Entity,
            DurableAgentStateHistoryBinding.HistoryProviderOwner =>
                DurableAgentHistoryOwnership.ExternalProvider,
            DurableAgentStateHistoryBinding.ModelServiceOwner =>
                DurableAgentHistoryOwnership.Service,
            _ => throw new ArgumentOutOfRangeException(nameof(ownerKind)),
        };
        return new DurableAgentState
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            PersistentRequestOutcomesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(
                    StringComparer.Ordinal),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(
                    StringComparer.Ordinal),
                HistoryBinding = DurableAgentHistoryBinding.ToJson(
                    DurableAgentHistoryBinding.Create(
                        ownership,
                        ownership == DurableAgentHistoryOwnership.Entity
                            ? null
                            : providerKey)),
            },
        };
    }

    private static void AddMailboxResult(
        DurableAgentState state,
        string correlationId,
        string content,
        DateTimeOffset completedAt)
    {
        DurableAgentStateOutcomeResolver.AddSuccessfulResult(
            state,
            correlationId,
            new AgentResponse(new ChatMessage(ChatRole.Assistant, content)),
            completedAt);
    }

    private static int MeasureProjectedPrefix(
        DurableAgentState state,
        int entryCount,
        DateTimeOffset now)
    {
        List<DurableAgentStateEntry> originalHistory =
            [.. state.Data.ConversationHistory];
        DurableAgentStateTruncation? originalTruncation = state.Data.Truncation;
        int removedMessages = originalHistory
            .Take(entryCount)
            .Sum(entry => entry.Messages.Count);
        try
        {
            state.Data.ConversationHistory.Clear();
            foreach (DurableAgentStateEntry entry in originalHistory.Skip(entryCount))
            {
                state.Data.ConversationHistory.Add(entry);
            }

            state.Data.Truncation = removedMessages == 0
                ? null
                : new DurableAgentStateTruncation
                {
                    EvictedMessageCount = Count(removedMessages),
                    FirstEvictedAt = now,
                    LastEvictedAt = now,
                };
            return DurableAgentStateRetention.GetSerializedSize(state);
        }
        finally
        {
            state.Data.ConversationHistory.Clear();
            foreach (DurableAgentStateEntry entry in originalHistory)
            {
                state.Data.ConversationHistory.Add(entry);
            }

            state.Data.Truncation = originalTruncation;
        }
    }

    private static void AddExchange(
        DurableAgentState state,
        string correlationId,
        string content,
        DateTimeOffset createdAt)
    {
        state.Data.ConversationHistory.Add(
            CreateRequest(correlationId, ChatRole.User, content, createdAt));
        state.Data.ConversationHistory.Add(
            new DurableAgentStateResponse
            {
                CorrelationId = correlationId,
                CreatedAt = createdAt,
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.Assistant, content) { CreatedAt = createdAt }),
                ],
            });
    }

    private static DurableAgentStateRequest CreateRequest(
        string correlationId,
        ChatRole role,
        string content,
        DateTimeOffset createdAt)
    {
        return new DurableAgentStateRequest
        {
            CorrelationId = correlationId,
            CreatedAt = createdAt,
            Messages =
            [
                DurableAgentStateMessage.FromChatMessage(
                    new ChatMessage(role, content) { CreatedAt = createdAt }),
            ],
        };
    }

    private static DurableAgentStateResponse CreateResponse(
        string correlationId,
        string content,
        DateTimeOffset createdAt)
    {
        return new DurableAgentStateResponse
        {
            CorrelationId = correlationId,
            CreatedAt = createdAt,
            Messages =
            [
                DurableAgentStateMessage.FromChatMessage(
                    new ChatMessage(ChatRole.Assistant, content) { CreatedAt = createdAt }),
            ],
        };
    }

    private static DurableAgentStateResponse CreateToolCallResponse(
        string correlationId,
        string callId,
        string payload,
        DateTimeOffset createdAt)
        => CreateToolCallResponse(correlationId, createdAt, (callId, payload));

    private static DurableAgentStateResponse CreateToolCallResponse(
        string correlationId,
        DateTimeOffset createdAt,
        params (string CallId, string Payload)[] calls)
    {
        List<AIContent> contents = calls
            .Select(call => (AIContent)new FunctionCallContent(
                call.CallId,
                "tool",
                new Dictionary<string, object?> { ["payload"] = call.Payload }))
            .ToList();
        return new DurableAgentStateResponse
        {
            CorrelationId = correlationId,
            CreatedAt = createdAt,
            Messages =
            [
                DurableAgentStateMessage.FromChatMessage(
                    new ChatMessage(ChatRole.Assistant, contents) { CreatedAt = createdAt }),
            ],
        };
    }

    private static DurableAgentStateRequest CreateToolResultRequest(
        string correlationId,
        string callId,
        object result,
        DateTimeOffset createdAt)
    {
        return new DurableAgentStateRequest
        {
            CorrelationId = correlationId,
            CreatedAt = createdAt,
            Messages =
            [
                DurableAgentStateMessage.FromChatMessage(
                    new ChatMessage(
                        ChatRole.Tool,
                        [new FunctionResultContent(callId, result)])
                    {
                        CreatedAt = createdAt,
                    }),
            ],
        };
    }

    private static bool ContainsToolCallId(DurableAgentState state, string callId)
    {
        return state.Data.ConversationHistory
            .SelectMany(entry => entry.Messages)
            .SelectMany(message => message.Contents)
            .Any(content => content switch
            {
                DurableAgentStateFunctionCallContent functionCall => functionCall.CallId == callId,
                DurableAgentStateFunctionResultContent functionResult => functionResult.CallId == callId,
                _ => false,
            });
    }

    private static Dictionary<string, int> GetToolCallIdCounts(
        DurableAgentState state) =>
        state.Data.ConversationHistory
            .SelectMany(entry => entry.Messages)
            .SelectMany(message => message.Contents)
            .Select(content => content switch
            {
                DurableAgentStateFunctionCallContent functionCall => functionCall.CallId,
                DurableAgentStateFunctionResultContent functionResult => functionResult.CallId,
                _ => null,
            })
            .Where(static callId => !string.IsNullOrWhiteSpace(callId))
            .GroupBy(static callId => callId!, StringComparer.Ordinal)
            .ToDictionary(
                static group => group.Key,
                static group => group.Count(),
                StringComparer.Ordinal);

    private static JsonElement Count(long value) =>
        JsonSerializer.SerializeToElement(value);

    private static long ReadCount(JsonElement value)
    {
        Assert.True(DurableAgentStateContract.TryGetInt64(value, out long count));
        return count;
    }
}
