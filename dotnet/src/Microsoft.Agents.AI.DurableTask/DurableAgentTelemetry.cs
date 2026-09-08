// Copyright (c) Microsoft. All rights reserved.

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Provides telemetry identifiers for durable agents.
/// </summary>
public static class DurableAgentTelemetry
{
    /// <summary>
    /// Gets the name of the meter that emits durable-agent metrics.
    /// </summary>
    public const string MeterName = "Microsoft.Agents.AI.DurableTask";

    internal const string EvictedMessagesInstrumentName =
        "durable.agent.history.evicted.messages";
    internal const string EvictedEntriesInstrumentName =
        "durable.agent.history.evicted.entries";
    internal const string ReclaimedBytesInstrumentName =
        "durable.agent.history.reclaimed.bytes";
    internal const string StateSizeBeforeInstrumentName =
        "durable.agent.history.state.size.before";
    internal const string StateSizeAfterInstrumentName =
        "durable.agent.history.state.size.after";
    internal const string RetentionOperationsInstrumentName =
        "durable.agent.history.retention.operations";

    internal const string AgentNameTagName = "agent.name";
    internal const string OutcomeTagName = "outcome";
    internal const string ReasonTagName = "reason";

    internal const string NoActionOutcome = "no_action";
    internal const string TranscriptEvictedOutcome = "transcript_evicted";
    internal const string ProtectedStateCapacityFailureOutcome =
        "protected_state_capacity_failure";

    internal const string TranscriptPressureReason = "transcript_pressure";

    private static class Instruments
    {
        internal static readonly Meter Meter = new(
            MeterName,
            typeof(DurableAgentTelemetry).Assembly.GetName().Version?.ToString());
        internal static readonly Counter<long> EvictedMessages =
            Meter.CreateCounter<long>(
            EvictedMessagesInstrumentName,
            unit: "{message}",
            description: "Number of messages removed from durable agent history.");
        internal static readonly Counter<long> EvictedEntries =
            Meter.CreateCounter<long>(
            EvictedEntriesInstrumentName,
            unit: "{entry}",
            description: "Number of entries removed from durable agent history.");
        internal static readonly Counter<long> ReclaimedBytes =
            Meter.CreateCounter<long>(
            ReclaimedBytesInstrumentName,
            unit: "By",
            description: "Net serialized durable-state bytes reclaimed by history retention.");
        internal static readonly Histogram<long> StateSizeBefore =
            Meter.CreateHistogram<long>(
            StateSizeBeforeInstrumentName,
            unit: "By",
            description: "Serialized durable-agent state size before a pressure-retention attempt.");
        internal static readonly Histogram<long> StateSizeAfter =
            Meter.CreateHistogram<long>(
            StateSizeAfterInstrumentName,
            unit: "By",
            description: "Serialized durable-agent state size after a pressure-retention attempt.");
        internal static readonly Counter<long> RetentionOperations =
            Meter.CreateCounter<long>(
            RetentionOperationsInstrumentName,
            unit: "{operation}",
            description: "Number of automatic durable-agent history retention checks by outcome.");
    }

    [SuppressMessage(
        "Design",
        "CA1031:Do not catch general exception types",
        Justification = "Telemetry must never affect durable agent execution.")]
    [SuppressMessage(
        "Roslynator",
        "RCS1075:Avoid empty catch clause that catches System.Exception",
        Justification = "Telemetry must never affect durable agent execution.")]
    internal static void RecordNoAction(string agentName)
    {
        try
        {
            Counter<long> retentionOperations = Instruments.RetentionOperations;
            if (!retentionOperations.Enabled)
            {
                return;
            }

            TagList tags = default;
            tags.Add(AgentNameTagName, agentName);
            tags.Add(OutcomeTagName, NoActionOutcome);
            retentionOperations.Add(1, tags);
        }
        catch (Exception)
        {
            // Metrics are best-effort operational telemetry.
        }
    }

    [SuppressMessage(
        "Design",
        "CA1031:Do not catch general exception types",
        Justification = "Telemetry must never affect durable agent execution.")]
    [SuppressMessage(
        "Roslynator",
        "RCS1075:Avoid empty catch clause that catches System.Exception",
        Justification = "Telemetry must never affect durable agent execution.")]
    internal static void RecordRetentionAttempt(
        string agentName,
        RetentionResult result)
    {
        try
        {
            Counter<long> evictedMessages = Instruments.EvictedMessages;
            Counter<long> evictedEntries = Instruments.EvictedEntries;
            Counter<long> reclaimedBytes = Instruments.ReclaimedBytes;
            Histogram<long> stateSizeBefore = Instruments.StateSizeBefore;
            Histogram<long> stateSizeAfter = Instruments.StateSizeAfter;
            Counter<long> retentionOperations = Instruments.RetentionOperations;
            if (!evictedMessages.Enabled &&
                !evictedEntries.Enabled &&
                !reclaimedBytes.Enabled &&
                !stateSizeBefore.Enabled &&
                !stateSizeAfter.Enabled &&
                !retentionOperations.Enabled)
            {
                return;
            }

            string outcome = result.Outcome switch
            {
                RetentionOutcome.TranscriptEvicted => TranscriptEvictedOutcome,
                RetentionOutcome.ProtectedStateCapacityFailure =>
                    ProtectedStateCapacityFailureOutcome,
                _ => NoActionOutcome,
            };

            if (stateSizeBefore.Enabled || stateSizeAfter.Enabled)
            {
                TagList sizeTags = default;
                sizeTags.Add(AgentNameTagName, agentName);
                sizeTags.Add(OutcomeTagName, outcome);
                stateSizeBefore.Record(result.InitialSizeBytes, sizeTags);
                stateSizeAfter.Record(result.FinalSizeBytes, sizeTags);
            }

            RecordEviction(
                agentName,
                TranscriptPressureReason,
                result.RemovedEntryCount,
                result.RemovedMessageCount,
                Math.Max(0, result.InitialSizeBytes - result.FinalSizeBytes));

            if (retentionOperations.Enabled)
            {
                TagList operationTags = default;
                operationTags.Add(AgentNameTagName, agentName);
                operationTags.Add(OutcomeTagName, outcome);
                retentionOperations.Add(1, operationTags);
            }
        }
        catch (Exception)
        {
            // Metrics are best-effort operational telemetry.
        }
    }

    private static void RecordEviction(
        string agentName,
        string reason,
        int evictedEntries,
        int evictedMessages,
        int reclaimedBytes)
    {
        if (evictedEntries <= 0 && evictedMessages <= 0 && reclaimedBytes <= 0)
        {
            return;
        }

        TagList tags = default;
        tags.Add(AgentNameTagName, agentName);
        tags.Add(ReasonTagName, reason);
        if (evictedEntries > 0)
        {
            Instruments.EvictedEntries.Add(evictedEntries, tags);
        }

        if (evictedMessages > 0)
        {
            Instruments.EvictedMessages.Add(evictedMessages, tags);
        }

        if (reclaimedBytes > 0)
        {
            Instruments.ReclaimedBytes.Add(reclaimedBytes, tags);
        }
    }
}
