// Copyright (c) Microsoft. All rights reserved.

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
            500,
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
        Assert.Equal(1_048_576, options.MaxStateBytes);
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
            2_500,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.True(removed > 0);
        Assert.DoesNotContain(state.Data.ConversationHistory, entry => entry.CorrelationId == "oldest");
        Assert.Contains(state.Data.ConversationHistory, entry => entry.CorrelationId == "newest");
        Assert.NotNull(state.Data.Truncation);
        Assert.Equal(removed, state.Data.Truncation.EvictedMessageCount);
        Assert.True(
            DurableAgentStateRetention.GetSerializedSize(state) <
            2_500 * DurableAgentStateRetention.HighWatermark);
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
            EvictedMessageCount = 4,
            FirstEvictedAt = firstEviction,
            LastEvictedAt = lastEviction,
        };

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            2_500,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.True(removed > 0);
        Assert.Equal(firstEviction, state.Data.Truncation?.FirstEvictedAt);
        Assert.Equal(lastEviction, state.Data.Truncation?.LastEvictedAt);
        Assert.Equal(4 + removed, state.Data.Truncation?.EvictedMessageCount);
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
            3_200,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.Contains(state.Data.ConversationHistory, entry => entry.CorrelationId == "system");
    }

    [Fact]
    public void AutoPreservesNewestExchange()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateLargeState(now);

        _ = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            2_500,
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
            2_600,
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
    public void AutoFindsZeroMessagePrefixBeforeTruncationSizeJump()
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

        int sizeAfterTwo = MeasureProjectedPrefix(state, 2, now);
        int sizeAfterThree = MeasureProjectedPrefix(state, 3, now);
        int sizeAfterFour = MeasureProjectedPrefix(state, 4, now);
        Assert.True(sizeAfterThree < sizeAfterTwo);
        Assert.True(sizeAfterThree < sizeAfterFour);
        int maxStateBytes = (int)Math.Ceiling(
            sizeAfterThree / DurableAgentStateRetention.LowWatermark);
        int lowWatermark = (int)(
            maxStateBytes * DurableAgentStateRetention.LowWatermark);
        Assert.InRange(lowWatermark, sizeAfterThree, Math.Min(sizeAfterTwo, sizeAfterFour) - 1);

        int removedMessages = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            maxStateBytes,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.Equal(0, removedMessages);
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
            2_000,
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
        Assert.Contains(state.Data.ConversationHistory, entry => entry.CorrelationId == "call");
        Assert.Contains(state.Data.ConversationHistory, entry => entry.CorrelationId == "newest");
        Assert.True(ContainsToolCallId(state, "protected-call"));
    }

    [Fact]
    public void AutoTreatsDuplicateToolIdsAsOneConservativeGroup()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        AddExchange(state, "filler", new string('f', 7_000), now.AddMinutes(-20));
        state.Data.ConversationHistory.Add(
            CreateToolCallResponse("first", "duplicate", new string('a', 500), now.AddMinutes(-10)));
        state.Data.ConversationHistory.Add(
            CreateToolCallResponse("second", "duplicate", new string('b', 500), now.AddMinutes(-5)));
        state.Data.ConversationHistory.Add(
            CreateToolResultRequest("newest", "duplicate", "result", now));
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
        Assert.Contains(state.Data.ConversationHistory, entry => entry.CorrelationId == "first");
        Assert.Contains(state.Data.ConversationHistory, entry => entry.CorrelationId == "second");
        Assert.Contains(state.Data.ConversationHistory, entry => entry.CorrelationId == "newest");
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
            4_000,
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
            4_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.DoesNotContain(state.Data.ConversationHistory, entry => entry.CorrelationId == "missing-call");
        Assert.Contains(state.Data.ConversationHistory, entry => entry.CorrelationId == "missing-result");
    }

    [Fact]
    public void SerializedSizeIncludesSessionAndTruncation()
    {
        DurableAgentState state = CreateRevisedState();
        int emptySize = DurableAgentStateRetention.GetSerializedSize(state);
        state.Data.Session = JsonSerializer.SerializeToElement(new { conversationId = new string('c', 100) });
        state.Data.Truncation = new DurableAgentStateTruncation
        {
            EvictedMessageCount = 2,
            FirstEvictedAt = DateTimeOffset.UtcNow,
            LastEvictedAt = DateTimeOffset.UtcNow,
        };

        int completeSize = DurableAgentStateRetention.GetSerializedSize(state);

        Assert.True(completeSize > emptySize + 100);
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
        state.Data.IngestedPositions = new Dictionary<string, int>
        {
            ["workflow"] = 42,
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
        Assert.Equal(42, state.Data.IngestedPositions?["workflow"]);
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
    public void PublicRetentionModesAreOnlyKeepAllAndAuto()
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
            MailboxWritesAuthorized = true,
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
                    EvictedMessageCount = removedMessages,
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
}
