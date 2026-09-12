// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask;

internal enum RetentionOutcome
{
    NoAction,
    TranscriptEvicted,
    ProtectedStateCapacityFailure,
}

internal sealed record RetentionResult(
    int RemovedEntryCount,
    int RemovedMessageCount,
    int InitialSizeBytes,
    int FinalSizeBytes,
    bool ProtectedStateCapacityFailure)
{
    public RetentionOutcome Outcome => this.ProtectedStateCapacityFailure
        ? RetentionOutcome.ProtectedStateCapacityFailure
        : this.RemovedEntryCount > 0 || this.FinalSizeBytes < this.InitialSizeBytes
            ? RetentionOutcome.TranscriptEvicted
            : RetentionOutcome.NoAction;
}
