// Copyright (c) Microsoft. All rights reserved.

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Provides cross-runtime telemetry identifiers for durable-agent retention.
/// </summary>
public static class DurableAgentTelemetry
{
    /// <summary>
    /// Gets the shared meter name used by the Python and .NET durable-agent runtimes.
    /// </summary>
    public const string MeterName = "agent_framework.durabletask";

    internal const string EvaluationsInstrumentName = "durable.retention.evaluations";
    internal const string BudgetInstrumentName = "durable.retention.budget";
    internal const string StateSizeInstrumentName = "durable.retention.state.size";
    internal const string RemovedMessagesInstrumentName = "durable.retention.removed_messages";
    internal const string RemovedEntriesInstrumentName = "durable.retention.removed_entries";
    internal const string ReclaimedBytesInstrumentName = "durable.retention.reclaimed_bytes";
    internal const string CapacityFailuresInstrumentName = "durable.retention.capacity_failures";
    internal const string WriteAttemptsInstrumentName = "durable.retention.write_attempts";
    internal const string OperationsInstrumentName = "durable.retention.operations";

    internal const string MechanismTagName = "mechanism";
    internal const string OutcomeTagName = "outcome";
    internal const string CommitStatusTagName = "commit_status";
    internal const string PhaseTagName = "phase";
    internal const string StageTagName = "stage";
    internal const string DeletionStagedTagName = "deletion_staged";

    internal const string PressureMechanism = "pressure";
    internal const string BelowThresholdOutcome = "below_threshold";
    internal const string StagedOutcome = "staged";
    internal const string ProtectedFloorOutcome = "protected_floor";
    internal const string ReturnedOutcome = "returned";
    internal const string FailedOutcome = "failed";
    internal const string NotAttemptedCommitStatus = "not_attempted";
    internal const string UnknownCommitStatus = "unknown";
    internal const string BeforePhase = "before";
    internal const string AfterPhase = "after";
    internal const string SerializationStage = "serialization";
    internal const string SetStateStage = "set_state";

    private static class Instruments
    {
        internal static readonly Meter Meter = new(
            MeterName,
            typeof(DurableAgentTelemetry).Assembly.GetName().Version?.ToString());
        internal static readonly Counter<long> Evaluations =
            Meter.CreateCounter<long>(
                EvaluationsInstrumentName,
                unit: "{evaluation}",
                description: "Local retention evaluations.");
        internal static readonly Histogram<long> Budget =
            Meter.CreateHistogram<long>(
                BudgetInstrumentName,
                unit: "By",
                description: "Requested resolved whole-entity pressure budget.");
        internal static readonly Histogram<long> StateSize =
            Meter.CreateHistogram<long>(
                StateSizeInstrumentName,
                unit: "By",
                description: "Serialized entity JSON at a retention boundary.");
        internal static readonly Counter<long> RemovedMessages =
            Meter.CreateCounter<long>(
                RemovedMessagesInstrumentName,
                unit: "{message}",
                description: "Messages removed from staged state.");
        internal static readonly Counter<long> RemovedEntries =
            Meter.CreateCounter<long>(
                RemovedEntriesInstrumentName,
                unit: "{entry}",
                description: "Entries removed from staged state.");
        internal static readonly Counter<long> ReclaimedBytes =
            Meter.CreateCounter<long>(
                ReclaimedBytesInstrumentName,
                unit: "By",
                description: "Nonnegative byte reduction in staged state.");
        internal static readonly Counter<long> CapacityFailures =
            Meter.CreateCounter<long>(
                CapacityFailuresInstrumentName,
                unit: "{failure}",
                description: "Unreachable pressure targets.");
        internal static readonly Counter<long> WriteAttempts =
            Meter.CreateCounter<long>(
                WriteAttemptsInstrumentName,
                unit: "{attempt}",
                description: "State serialization or host set-state outcomes, not durable commit confirmation.");
        internal static readonly Counter<long> Operations =
            Meter.CreateCounter<long>(
                OperationsInstrumentName,
                unit: "{operation}",
                description: "Entity operations with retention observations and their host write status.");
    }

    internal static void RecordRetention(
        RetentionResult result,
        int budgetBytes)
    {
        TryRecord(
            () =>
            {
                string outcome = result.Outcome switch
                {
                    RetentionOutcome.TranscriptEvicted => StagedOutcome,
                    RetentionOutcome.ProtectedStateCapacityFailure => ProtectedFloorOutcome,
                    _ => BelowThresholdOutcome,
                };
                TagList tags = CreateRetentionTags(outcome);
                Instruments.Evaluations.Add(1, tags);
                Instruments.Budget.Record(budgetBytes, tags);

                TagList beforeTags = tags;
                beforeTags.Add(PhaseTagName, BeforePhase);
                Instruments.StateSize.Record(result.InitialSizeBytes, beforeTags);
                TagList afterTags = tags;
                afterTags.Add(PhaseTagName, AfterPhase);
                Instruments.StateSize.Record(result.FinalSizeBytes, afterTags);

                if (result.RemovedEntryCount > 0)
                {
                    int reclaimedBytes = Math.Max(
                        0,
                        result.InitialSizeBytes - result.FinalSizeBytes);
                    if (reclaimedBytes > 0)
                    {
                        Instruments.ReclaimedBytes.Add(reclaimedBytes, tags);
                    }
                }

                if (result.RemovedMessageCount > 0)
                {
                    Instruments.RemovedMessages.Add(result.RemovedMessageCount, tags);
                }

                if (result.RemovedEntryCount > 0)
                {
                    Instruments.RemovedEntries.Add(result.RemovedEntryCount, tags);
                }

                if (result.ProtectedStateCapacityFailure)
                {
                    Instruments.CapacityFailures.Add(1, tags);
                }
            });
    }

    internal static void RecordWrite(
        string stage,
        string outcome,
        string commitStatus,
        bool deletionStaged)
    {
        TryRecord(
            () =>
            {
                TagList tags = default;
                tags.Add(StageTagName, stage);
                tags.Add(OutcomeTagName, outcome);
                tags.Add(CommitStatusTagName, commitStatus);
                tags.Add(DeletionStagedTagName, deletionStaged);
                Instruments.WriteAttempts.Add(1, tags);
            });
    }

    internal static void RecordOperation(
        string outcome,
        string commitStatus,
        bool deletionStaged)
    {
        TryRecord(
            () =>
            {
                TagList tags = default;
                tags.Add(OutcomeTagName, outcome);
                tags.Add(CommitStatusTagName, commitStatus);
                tags.Add(DeletionStagedTagName, deletionStaged);
                Instruments.Operations.Add(1, tags);
            });
    }

    private static TagList CreateRetentionTags(string outcome)
    {
        TagList tags = default;
        tags.Add(MechanismTagName, PressureMechanism);
        tags.Add(OutcomeTagName, outcome);
        tags.Add(CommitStatusTagName, NotAttemptedCommitStatus);
        return tags;
    }

    [SuppressMessage(
        "Design",
        "CA1031:Do not catch general exception types",
        Justification = "Optional telemetry must never affect durable agent execution.")]
    [SuppressMessage(
        "Roslynator",
        "RCS1075:Avoid empty catch clause that catches System.Exception",
        Justification = "Optional telemetry must never affect durable agent execution.")]
    private static void TryRecord(Action action)
    {
        try
        {
            action();
        }
        catch (Exception)
        {
            // Metrics are best-effort operational telemetry.
        }
    }
}
