// Copyright (c) Microsoft. All rights reserved.

using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Logging.Abstractions;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class DurableAgentTelemetryTests
{
    private static readonly string[] s_allowedTagNames = ["agent.name", "outcome", "reason"];

    [Fact]
    public void NormalEvictionRecordsCountsBytesSizesAndBoundedTags()
    {
        const string AgentName = "metric-normal";
        const string SessionKey = "do-not-export-session";
        const string Content = "do-not-export-content";
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateLargeState(now, Content);
        int initialSize = DurableAgentStateRetention.GetSerializedSize(state);
        using RetentionMetricListener listener = new(AgentName);

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            2_500,
            now,
            NullLogger.Instance,
            new AgentSessionId(AgentName, SessionKey));

        int finalSize = DurableAgentStateRetention.GetSerializedSize(state);
        MetricMeasurement operation = listener.Single(
            DurableAgentTelemetry.RetentionOperationsInstrumentName);
        MetricMeasurement evictedEntries = listener.Single(
            DurableAgentTelemetry.EvictedEntriesInstrumentName);
        MetricMeasurement evicted = listener.Single(
            DurableAgentTelemetry.EvictedMessagesInstrumentName);
        MetricMeasurement reclaimed = listener.Single(
            DurableAgentTelemetry.ReclaimedBytesInstrumentName);
        MetricMeasurement before = listener.Single(
            DurableAgentTelemetry.StateSizeBeforeInstrumentName);
        MetricMeasurement after = listener.Single(
            DurableAgentTelemetry.StateSizeAfterInstrumentName);

        Assert.Equal(DurableAgentTelemetry.TranscriptEvictedOutcome, operation.Tags["outcome"]);
        Assert.Equal(DurableAgentTelemetry.TranscriptPressureReason, evicted.Tags["reason"]);
        Assert.Equal("{operation}", operation.Unit);
        Assert.Equal("{entry}", evictedEntries.Unit);
        Assert.Equal("{message}", evicted.Unit);
        Assert.Equal("By", reclaimed.Unit);
        Assert.Equal("By", before.Unit);
        Assert.Equal("By", after.Unit);
        Assert.Equal(removed, evicted.Value);
        Assert.Equal(removed, evictedEntries.Value);
        Assert.Equal(initialSize - finalSize, reclaimed.Value);
        Assert.Equal(initialSize, before.Value);
        Assert.Equal(finalSize, after.Value);
        Assert.All(
            listener.Measurements,
            measurement =>
            {
                Assert.Equal(AgentName, measurement.Tags["agent.name"]);
                Assert.DoesNotContain(SessionKey, measurement.Tags.Values);
                Assert.DoesNotContain(Content, measurement.Tags.Values);
                Assert.All(
                    measurement.Tags.Keys,
                    key => Assert.Contains(key, s_allowedTagNames));
            });
    }

    [Fact]
    public void ZeroMessageEntryEvictionRecordsEntryOutcomeAndReclaimedBytes()
    {
        const string AgentName = "metric-empty-entry";
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        state.Data.ConversationHistory.Add(
            new DurableAgentStateCompaction
            {
                CreatedAt = now.AddMinutes(-5),
                ExtensionData = new Dictionary<string, JsonElement>
                {
                    ["padding"] = JsonSerializer.SerializeToElement(new string('x', 2_000)),
                },
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
        int initialSize = DurableAgentStateRetention.GetSerializedSize(state);
        using RetentionMetricListener listener = new(AgentName);

        int removedMessages = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            2_000,
            now,
            NullLogger.Instance,
            new AgentSessionId(AgentName, "session"));

        int finalSize = DurableAgentStateRetention.GetSerializedSize(state);
        MetricMeasurement operation = listener.Single(
            DurableAgentTelemetry.RetentionOperationsInstrumentName);
        MetricMeasurement evictedEntries = listener.Single(
            DurableAgentTelemetry.EvictedEntriesInstrumentName);
        MetricMeasurement reclaimed = listener.Single(
            DurableAgentTelemetry.ReclaimedBytesInstrumentName);

        Assert.Equal(0, removedMessages);
        Assert.Equal(DurableAgentTelemetry.TranscriptEvictedOutcome, operation.Tags["outcome"]);
        Assert.Equal(1, evictedEntries.Value);
        Assert.Equal(DurableAgentTelemetry.TranscriptPressureReason, evictedEntries.Tags["reason"]);
        Assert.Empty(listener.Find(DurableAgentTelemetry.EvictedMessagesInstrumentName));
        Assert.Equal(initialSize - finalSize, reclaimed.Value);
        Assert.True(reclaimed.Value > 0);
    }

    [Fact]
    public void ZeroMessageEvictionDoesNotCreateInvalidTruncationEvidence()
    {
        const string AgentName = "metric-truncation-offset";
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        state.Data.ConversationHistory.Add(
            new DurableAgentStateCompaction
            {
                CreatedAt = now.AddMinutes(-5),
            });
        state.Data.ConversationHistory.Add(
            new DurableAgentStateCompaction
            {
                CreatedAt = now,
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.Assistant, new string('x', 2_000))),
                ],
            });
        int initialSize = DurableAgentStateRetention.GetSerializedSize(state);
        using RetentionMetricListener listener = new(AgentName);

        _ = Assert.Throws<DurableAgentStateSizeLimitExceededException>(
            () => DurableAgentStateRetention.Enforce(
                state,
                DurableAgentHistoryRetentionMode.Auto,
                initialSize,
                now,
                NullLogger.Instance,
                new AgentSessionId(AgentName, "session")));

        int finalSize = DurableAgentStateRetention.GetSerializedSize(state);
        MetricMeasurement operation = listener.Single(
            DurableAgentTelemetry.RetentionOperationsInstrumentName);
        MetricMeasurement evictedEntries = listener.Single(
            DurableAgentTelemetry.EvictedEntriesInstrumentName);
        MetricMeasurement reclaimed = listener.Single(
            DurableAgentTelemetry.ReclaimedBytesInstrumentName);

        Assert.True(finalSize < initialSize);
        Assert.Equal(
            DurableAgentTelemetry.ProtectedStateCapacityFailureOutcome,
            operation.Tags["outcome"]);
        Assert.Equal(1, evictedEntries.Value);
        Assert.Equal(DurableAgentTelemetry.TranscriptPressureReason, evictedEntries.Tags["reason"]);
        Assert.Empty(listener.Find(DurableAgentTelemetry.EvictedMessagesInstrumentName));
        Assert.Equal(initialSize - finalSize, reclaimed.Value);
        Assert.Null(state.Data.Truncation);
    }

    [Fact]
    public void ProtectedStateFailureRecordsFailedOutcomeAndSizes()
    {
        const string AgentName = "metric-failure";
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        AddExchange(state, "newest", new string('x', 2_000), now);
        int initialSize = DurableAgentStateRetention.GetSerializedSize(state);
        using RetentionMetricListener listener = new(AgentName);

        _ = Assert.Throws<DurableAgentStateSizeLimitExceededException>(
            () => DurableAgentStateRetention.Enforce(
                state,
                DurableAgentHistoryRetentionMode.Auto,
                500,
                now,
                NullLogger.Instance,
                new AgentSessionId(AgentName, "session")));

        MetricMeasurement operation = listener.Single(
            DurableAgentTelemetry.RetentionOperationsInstrumentName);
        MetricMeasurement before = listener.Single(
            DurableAgentTelemetry.StateSizeBeforeInstrumentName);
        MetricMeasurement after = listener.Single(
            DurableAgentTelemetry.StateSizeAfterInstrumentName);

        Assert.Equal(
            DurableAgentTelemetry.ProtectedStateCapacityFailureOutcome,
            operation.Tags["outcome"]);
        Assert.Equal(initialSize, before.Value);
        Assert.Equal(DurableAgentStateRetention.GetSerializedSize(state), after.Value);
        Assert.Empty(listener.Find(DurableAgentTelemetry.EvictedMessagesInstrumentName));
        Assert.Empty(listener.Find(DurableAgentTelemetry.EvictedEntriesInstrumentName));
        Assert.Empty(listener.Find(DurableAgentTelemetry.ReclaimedBytesInstrumentName));
    }

    [Fact]
    public void BelowHighWatermarkRecordsOnlyNoActionOperation()
    {
        const string AgentName = "metric-no-action";
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        AddExchange(state, "newest", "small", now);
        using RetentionMetricListener listener = new(AgentName);

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            100_000,
            now,
            NullLogger.Instance,
            new AgentSessionId(AgentName, "session"));

        Assert.Equal(0, removed);
        MetricMeasurement operation = listener.Single(
            DurableAgentTelemetry.RetentionOperationsInstrumentName);
        Assert.Equal(DurableAgentTelemetry.NoActionOutcome, operation.Tags["outcome"]);
        Assert.Single(listener.Measurements);
    }

    [Fact]
    public void KeepAllDoesNotEmitRetentionMetrics()
    {
        const string AgentName = "metric-keep-all";
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateLargeState(now, "large");
        using RetentionMetricListener listener = new(AgentName);

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.KeepAll,
            500,
            now,
            NullLogger.Instance,
            new AgentSessionId(AgentName, "session"));

        Assert.Equal(0, removed);
        Assert.Empty(listener.Measurements);
    }

    [Fact]
    public void ConcurrentRetentionCallsRecordIndependently()
    {
        const string AgentName = "metric-concurrent";
        const int AttemptCount = 32;
        DateTimeOffset now = DateTimeOffset.UtcNow;
        using RetentionMetricListener listener = new(AgentName);

        Parallel.For(
            0,
            AttemptCount,
            _ =>
            {
                DurableAgentState state = CreateLargeState(now, "payload");
                int removed = DurableAgentStateRetention.Enforce(
                    state,
                    DurableAgentHistoryRetentionMode.Auto,
                    2_500,
                    now,
                    NullLogger.Instance,
                    new AgentSessionId(AgentName, "session"));
                Assert.True(removed > 0);
            });

        Assert.Equal(
            AttemptCount,
            listener.Find(DurableAgentTelemetry.RetentionOperationsInstrumentName).Count);
        Assert.Equal(
            AttemptCount,
            listener.Find(DurableAgentTelemetry.StateSizeBeforeInstrumentName).Count);
        Assert.Equal(
            AttemptCount,
            listener.Find(DurableAgentTelemetry.StateSizeAfterInstrumentName).Count);
    }

    [Fact]
    public void ListenerAbsenceDoesNotChangeRetention()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateLargeState(now, "payload");

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            2_500,
            now,
            NullLogger.Instance,
            new AgentSessionId("metric-no-listener", "session"));

        Assert.True(removed > 0);
        Assert.DoesNotContain(
            state.Data.ConversationHistory,
            entry => entry.CorrelationId == "oldest");
        Assert.Contains(
            state.Data.ConversationHistory,
            entry => entry.CorrelationId == "newest");
    }

    [Fact]
    public void ThrowingListenerCannotAffectRetention()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateLargeState(now, "payload");
        using MeterListener listener = new();
        listener.InstrumentPublished = static (instrument, meterListener) =>
        {
            if (instrument.Meter.Name == DurableAgentTelemetry.MeterName)
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>(
            static (_, _, _, _) => throw new InvalidOperationException("listener failure"));
        listener.Start();

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            2_500,
            now,
            NullLogger.Instance,
            new AgentSessionId("metric-throwing-listener", "session"));

        Assert.True(removed > 0);
        Assert.Contains(
            state.Data.ConversationHistory,
            entry => entry.CorrelationId == "newest");
    }

    private static DurableAgentState CreateLargeState(
        DateTimeOffset now,
        string content)
    {
        DurableAgentState state = CreateRevisedState();
        AddExchange(state, "oldest", new string('a', 500) + content, now.AddMinutes(-10));
        AddExchange(state, "middle", new string('b', 500), now.AddMinutes(-5));
        AddExchange(state, "newest", new string('c', 500), now);
        return state;
    }

    private static DurableAgentState CreateRevisedState()
    {
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
                        DurableAgentHistoryOwnership.Entity,
                        configuredProviderKey: null)),
            },
        };
    }

    private static void AddExchange(
        DurableAgentState state,
        string correlationId,
        string content,
        DateTimeOffset createdAt)
    {
        state.Data.ConversationHistory.Add(
            new DurableAgentStateRequest
            {
                CorrelationId = correlationId,
                CreatedAt = createdAt,
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.User, content) { CreatedAt = createdAt }),
                ],
            });
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

    private sealed record MetricMeasurement(
        string InstrumentName,
        string? Unit,
        long Value,
        IReadOnlyDictionary<string, object?> Tags);

    private sealed class RetentionMetricListener : IDisposable
    {
        private readonly string _agentName;
        private readonly ConcurrentQueue<MetricMeasurement> _measurements = new();
        private readonly MeterListener _listener = new();

        public RetentionMetricListener(string agentName)
        {
            this._agentName = agentName;
            this._listener.InstrumentPublished = static (instrument, listener) =>
            {
                if (instrument.Meter.Name == DurableAgentTelemetry.MeterName)
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            this._listener.SetMeasurementEventCallback<long>(this.Record);
            this._listener.Start();
        }

        public IReadOnlyList<MetricMeasurement> Measurements => [.. this._measurements];

        public List<MetricMeasurement> Find(string instrumentName) =>
            this.Measurements
                .Where(measurement => measurement.InstrumentName == instrumentName)
                .ToList();

        public MetricMeasurement Single(string instrumentName) =>
            Assert.Single(this.Find(instrumentName));

        public void Dispose() => this._listener.Dispose();

        private void Record(
            Instrument instrument,
            long measurement,
            ReadOnlySpan<KeyValuePair<string, object?>> tags,
            object? state)
        {
            Dictionary<string, object?> copiedTags = new(StringComparer.Ordinal);
            foreach (KeyValuePair<string, object?> tag in tags)
            {
                copiedTags[tag.Key] = tag.Value;
            }

            if (copiedTags.TryGetValue(
                    DurableAgentTelemetry.AgentNameTagName,
                    out object? agentName) &&
                string.Equals(agentName as string, this._agentName, StringComparison.Ordinal))
            {
                this._measurements.Enqueue(
                    new MetricMeasurement(
                        instrument.Name,
                        instrument.Unit,
                        measurement,
                        copiedTags));
            }
        }
    }
}
