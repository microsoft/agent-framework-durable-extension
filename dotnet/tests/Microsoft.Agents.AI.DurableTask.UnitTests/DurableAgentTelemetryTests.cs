// Copyright (c) Microsoft. All rights reserved.

using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Logging.Abstractions;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

[CollectionDefinition("Durable retention telemetry", DisableParallelization = true)]
public sealed class DurableAgentTelemetrySerialGroup;

[Collection("Durable retention telemetry")]
public sealed class DurableAgentTelemetryTests
{
    [Fact]
    public void SharedMeterAndInstrumentNamesMatchPythonContract()
    {
        Assert.Equal("agent_framework.durabletask", DurableAgentTelemetry.MeterName);
        Assert.Equal(
            [
                "durable.retention.evaluations",
                "durable.retention.budget",
                "durable.retention.state.size",
                "durable.retention.removed_messages",
                "durable.retention.removed_entries",
                "durable.retention.reclaimed_bytes",
                "durable.retention.capacity_failures",
                "durable.retention.write_attempts",
                "durable.retention.operations",
            ],
            new[]
            {
                DurableAgentTelemetry.EvaluationsInstrumentName,
                DurableAgentTelemetry.BudgetInstrumentName,
                DurableAgentTelemetry.StateSizeInstrumentName,
                DurableAgentTelemetry.RemovedMessagesInstrumentName,
                DurableAgentTelemetry.RemovedEntriesInstrumentName,
                DurableAgentTelemetry.ReclaimedBytesInstrumentName,
                DurableAgentTelemetry.CapacityFailuresInstrumentName,
                DurableAgentTelemetry.WriteAttemptsInstrumentName,
                DurableAgentTelemetry.OperationsInstrumentName,
            });
    }

    [Fact]
    public void NormalEvictionRecordsSharedMeasurementsWithoutIdentifiers()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateLargeState(now, "do-not-export-content");
        int initialSize = DurableAgentStateRetention.GetSerializedSize(state);
        using RetentionMetricListener listener = new();

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            4_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("do-not-export-agent", "do-not-export-session"));

        int finalSize = DurableAgentStateRetention.GetSerializedSize(state);
        MetricMeasurement evaluation = listener.Single(
            DurableAgentTelemetry.EvaluationsInstrumentName);
        MetricMeasurement budget = listener.Single(
            DurableAgentTelemetry.BudgetInstrumentName);
        List<MetricMeasurement> sizes = listener.Find(
            DurableAgentTelemetry.StateSizeInstrumentName);
        MetricMeasurement removedMessages = listener.Single(
            DurableAgentTelemetry.RemovedMessagesInstrumentName);
        MetricMeasurement removedEntries = listener.Single(
            DurableAgentTelemetry.RemovedEntriesInstrumentName);
        MetricMeasurement reclaimed = listener.Single(
            DurableAgentTelemetry.ReclaimedBytesInstrumentName);

        Assert.Equal(DurableAgentTelemetry.StagedOutcome, evaluation.Tags["outcome"]);
        Assert.Equal(DurableAgentTelemetry.PressureMechanism, evaluation.Tags["mechanism"]);
        Assert.Equal(
            DurableAgentTelemetry.NotAttemptedCommitStatus,
            evaluation.Tags["commit_status"]);
        Assert.Equal(4_000, budget.Value);
        Assert.Equal(2, sizes.Count);
        Assert.Equal(
            initialSize,
            Assert.Single(sizes, value => Equals(value.Tags["phase"], "before")).Value);
        Assert.Equal(
            finalSize,
            Assert.Single(sizes, value => Equals(value.Tags["phase"], "after")).Value);
        Assert.Equal(removed, removedMessages.Value);
        Assert.True(removedEntries.Value > 0);
        Assert.Equal(initialSize - finalSize, reclaimed.Value);
        Assert.All(
            listener.Measurements,
            measurement =>
            {
                Assert.DoesNotContain(
                    measurement.Tags.Keys,
                    key => key is "agent.name" or "session.id" or "correlation.id");
                Assert.DoesNotContain(
                    measurement.Tags.Values,
                    value => Equals(value, "do-not-export-agent") ||
                        Equals(value, "do-not-export-session") ||
                        Equals(value, "do-not-export-content"));
            });
        Assert.Empty(listener.Find(DurableAgentTelemetry.CapacityFailuresInstrumentName));
        Assert.Empty(listener.Find(DurableAgentTelemetry.WriteAttemptsInstrumentName));
        Assert.Empty(listener.Find(DurableAgentTelemetry.OperationsInstrumentName));
    }

    [Fact]
    public void CapacityFailureIsAtomicAndRecordsUncommittedFailure()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        AddExchange(state, "old", new string('o', 2_000), now.AddMinutes(-5));
        AddExchange(state, "newest", new string('n', 2_000), now);
        string original = JsonSerializer.Serialize(
            state,
            DurableAgentStateJsonContext.Default.DurableAgentState);
        using RetentionMetricListener listener = new();

        _ = Assert.Throws<DurableAgentStateSizeLimitExceededException>(
            () => DurableAgentStateRetention.Enforce(
                state,
                DurableAgentHistoryRetentionMode.Auto,
                500,
                now,
                NullLogger.Instance,
                new AgentSessionId("agent", "session")));

        Assert.Equal(
            original,
            JsonSerializer.Serialize(
                state,
                DurableAgentStateJsonContext.Default.DurableAgentState));
        Assert.Equal(
            DurableAgentTelemetry.ProtectedFloorOutcome,
            listener.Single(DurableAgentTelemetry.EvaluationsInstrumentName).Tags["outcome"]);
        Assert.Equal(
            1,
            listener.Single(DurableAgentTelemetry.CapacityFailuresInstrumentName).Value);
        MetricMeasurement operation = listener.Single(
            DurableAgentTelemetry.OperationsInstrumentName);
        Assert.Equal(DurableAgentTelemetry.FailedOutcome, operation.Tags["outcome"]);
        Assert.Equal(
            DurableAgentTelemetry.NotAttemptedCommitStatus,
            operation.Tags["commit_status"]);
    }

    [Fact]
    public void BelowThresholdRecordsEvaluationBudgetAndBothPhases()
    {
        DurableAgentState state = CreateRevisedState();
        AddExchange(state, "newest", "small", DateTimeOffset.UtcNow);
        using RetentionMetricListener listener = new();

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            100_000,
            DateTimeOffset.UtcNow,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.Equal(0, removed);
        Assert.Equal(
            DurableAgentTelemetry.BelowThresholdOutcome,
            listener.Single(DurableAgentTelemetry.EvaluationsInstrumentName).Tags["outcome"]);
        Assert.Equal(
            2,
            listener.Find(DurableAgentTelemetry.StateSizeInstrumentName).Count);
        Assert.Empty(listener.Find(DurableAgentTelemetry.RemovedMessagesInstrumentName));
        Assert.Empty(listener.Find(DurableAgentTelemetry.RemovedEntriesInstrumentName));
    }

    [Fact]
    public void BoundedCumulativeEvidenceDoesNotNarrowAttemptTelemetry()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState baseline = CreateLargeState(now, "large");
        DurableAgentState control = baseline.Clone();
        int expectedRemoved = DurableAgentStateRetention.Enforce(
            control,
            DurableAgentHistoryRetentionMode.Auto,
            4_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));
        DurableAgentState state = baseline.Clone();
        state.Data.Truncation = new DurableAgentStateTruncation
        {
            EvictedMessageCount = JsonSerializer.SerializeToElement(
                long.MaxValue - expectedRemoved),
            FirstEvictedAt = now.AddMinutes(-20),
            LastEvictedAt = now.AddMinutes(-10),
        };
        using RetentionMetricListener listener = new();

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.Auto,
            4_000,
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.Equal(expectedRemoved, removed);
        Assert.Equal(
            long.MaxValue,
            state.Data.Truncation!.EvictedMessageCount.GetInt64());
        Assert.Equal(
            removed,
            listener.Single(DurableAgentTelemetry.RemovedMessagesInstrumentName).Value);
    }

    [Fact]
    public void CounterOverflowIsAtomicAndDoesNotReportRemovalTelemetry()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateLargeState(now, "large");
        state.Data.Truncation = new DurableAgentStateTruncation
        {
            EvictedMessageCount = JsonSerializer.SerializeToElement(long.MaxValue),
            FirstEvictedAt = now.AddMinutes(-20),
            LastEvictedAt = now.AddMinutes(-10),
        };
        string original = JsonSerializer.Serialize(
            state,
            DurableAgentStateJsonContext.Default.DurableAgentState);
        using RetentionMetricListener listener = new();

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
            original,
            JsonSerializer.Serialize(
                state,
                DurableAgentStateJsonContext.Default.DurableAgentState));
        Assert.Empty(listener.Find(DurableAgentTelemetry.RemovedMessagesInstrumentName));
        Assert.Empty(listener.Find(DurableAgentTelemetry.RemovedEntriesInstrumentName));
        Assert.Empty(listener.Find(DurableAgentTelemetry.ReclaimedBytesInstrumentName));
    }

    [Fact]
    public void ZeroMessageEntryEvictionRecordsReclaimedBytesAndDeletion()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        for (int index = 0; index < 3; index++)
        {
            state.Data.ConversationHistory.Add(
                new DurableAgentStateCompaction
                {
                    CreatedAt = now.AddMinutes(index - 4),
                    UnknownProperties = new Dictionary<string, JsonElement>
                    {
                        ["padding"] = JsonSerializer.SerializeToElement(new string('p', 500)),
                    },
                });
        }
        AddExchange(state, "newest", "newest", now);
        using RetentionMetricListener listener = new();

        RetentionResult? result = DurableAgentStateRetention.EnforceForCommit(
            state,
            new(
                DurableAgentHistoryRetentionMode.Auto,
                MaxStateBytes: 2_000,
                HighWatermark: 0.85,
                LowWatermark: 0.70),
            now,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.NotNull(result);
        Assert.True(result.RemovedEntryCount > 0);
        Assert.Equal(0, result.RemovedMessageCount);
        Assert.Empty(listener.Find(DurableAgentTelemetry.RemovedMessagesInstrumentName));
        Assert.True(
            listener.Single(DurableAgentTelemetry.RemovedEntriesInstrumentName).Value > 0);
        Assert.True(
            listener.Single(DurableAgentTelemetry.ReclaimedBytesInstrumentName).Value > 0);
    }

    [Fact]
    public void ZeroMessageCapacityFailureReportsDeletionStaged()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CreateRevisedState();
        state.Data.ConversationHistory.Add(
            new DurableAgentStateCompaction
            {
                CreatedAt = now.AddMinutes(-2),
                UnknownProperties = new Dictionary<string, JsonElement>
                {
                    ["padding"] = JsonSerializer.SerializeToElement(new string('p', 500)),
                },
            });
        AddExchange(state, "newest", new string('n', 2_000), now);
        int originalEntryCount = state.Data.ConversationHistory.Count;
        using RetentionMetricListener listener = new();

        _ = Assert.Throws<DurableAgentStateSizeLimitExceededException>(
            () => DurableAgentStateRetention.Enforce(
                state,
                DurableAgentHistoryRetentionMode.Auto,
                1_000,
                now,
                NullLogger.Instance,
                new AgentSessionId("agent", "session")));

        Assert.Equal(originalEntryCount, state.Data.ConversationHistory.Count);
        Assert.True(
            listener.Single(DurableAgentTelemetry.RemovedEntriesInstrumentName).Value > 0);
        Assert.True(
            listener.Single(DurableAgentTelemetry.ReclaimedBytesInstrumentName).Value > 0);
        MetricMeasurement operation = listener.Single(
            DurableAgentTelemetry.OperationsInstrumentName);
        Assert.Equal(DurableAgentTelemetry.FailedOutcome, operation.Tags["outcome"]);
        Assert.Equal(true, operation.Tags["deletion_staged"]);
    }

    [Fact]
    public void WriteAndOperationMeasurementsCarryCommitStatus()
    {
        using RetentionMetricListener listener = new();

        DurableAgentTelemetry.RecordWrite(
            DurableAgentTelemetry.SerializationStage,
            DurableAgentTelemetry.ReturnedOutcome,
            DurableAgentTelemetry.NotAttemptedCommitStatus,
            deletionStaged: true);
        DurableAgentTelemetry.RecordWrite(
            DurableAgentTelemetry.SetStateStage,
            DurableAgentTelemetry.ReturnedOutcome,
            DurableAgentTelemetry.UnknownCommitStatus,
            deletionStaged: true);
        DurableAgentTelemetry.RecordOperation(
            DurableAgentTelemetry.ReturnedOutcome,
            DurableAgentTelemetry.UnknownCommitStatus,
            deletionStaged: true);

        List<MetricMeasurement> writes = listener.Find(
            DurableAgentTelemetry.WriteAttemptsInstrumentName);
        Assert.Equal(2, writes.Count);
        Assert.Equal(
            DurableAgentTelemetry.NotAttemptedCommitStatus,
            Assert.Single(
                writes,
                value => Equals(
                    value.Tags["stage"],
                    DurableAgentTelemetry.SerializationStage)).Tags["commit_status"]);
        Assert.Equal(
            DurableAgentTelemetry.UnknownCommitStatus,
            Assert.Single(
                writes,
                value => Equals(
                    value.Tags["stage"],
                    DurableAgentTelemetry.SetStateStage)).Tags["commit_status"]);
        MetricMeasurement operation = listener.Single(
            DurableAgentTelemetry.OperationsInstrumentName);
        Assert.Equal(DurableAgentTelemetry.UnknownCommitStatus, operation.Tags["commit_status"]);
        Assert.Equal(true, operation.Tags["deletion_staged"]);
    }

    [Fact]
    public void KeepAllDoesNotEmitRetentionMetrics()
    {
        DurableAgentState state = CreateLargeState(DateTimeOffset.UtcNow, "large");
        using RetentionMetricListener listener = new();

        int removed = DurableAgentStateRetention.Enforce(
            state,
            DurableAgentHistoryRetentionMode.KeepAll,
            maxStateBytes: null,
            DateTimeOffset.UtcNow,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

        Assert.Equal(0, removed);
        Assert.Empty(listener.Measurements);
    }

    [Fact]
    public void ThrowingListenerCannotAffectRetention()
    {
        DurableAgentState state = CreateLargeState(DateTimeOffset.UtcNow, "payload");
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
            4_000,
            DateTimeOffset.UtcNow,
            NullLogger.Instance,
            new AgentSessionId("agent", "session"));

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
            PersistentRequestOutcomesAuthorized = true,
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
        private readonly ConcurrentQueue<MetricMeasurement> _measurements = new();
        private readonly MeterListener _listener = new();

        public RetentionMetricListener()
        {
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

            this._measurements.Enqueue(
                new MetricMeasurement(
                    instrument.Name,
                    instrument.Unit,
                    measurement,
                    copiedTags));
        }
    }
}
