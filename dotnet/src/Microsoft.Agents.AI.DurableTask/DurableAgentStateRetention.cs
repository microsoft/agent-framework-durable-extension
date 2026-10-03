// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Logging;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Applies deterministic pressure retention to durable agent state.
/// </summary>
internal static class DurableAgentStateRetention
{
    internal const double DefaultHighWatermark = 0.85;
    internal const double DefaultLowWatermark = 0.70;
    internal const double HighWatermark = DefaultHighWatermark;
    internal const double LowWatermark = DefaultLowWatermark;

    internal sealed class ExecutionStatistics
    {
        public int CandidateGroupingPassCount { get; set; }

        public int ProtectedFloorCloneCount { get; set; }

        public int SerializedStateMeasurementCount { get; set; }
    }

    public static int GetSerializedSize(DurableAgentState state)
    {
        string converterPayload = JsonSerializer.Serialize(
            state,
            DurableAgentStateJsonContext.Default.DurableAgentState);
        using SerializedByteCountingStream stream = new();
        JsonSerializer.Serialize(
            stream,
            converterPayload,
            DurableAgentStateJsonContext.Default.String);
        return checked((int)stream.BytesWritten);
    }

    public static int Enforce(
        DurableAgentState state,
        DurableAgentHistoryRetentionMode mode,
        int? maxStateBytes,
        DateTimeOffset now,
        ILogger logger,
        AgentSessionId sessionId)
    {
        if (mode == DurableAgentHistoryRetentionMode.KeepAll)
        {
            if (maxStateBytes.HasValue)
            {
                throw new InvalidOperationException(
                    "A durable state budget is not applicable when history retention is KeepAll.");
            }

            return 0;
        }

        return EnforceAuto(
            state,
            mode,
            maxStateBytes,
            DefaultHighWatermark,
            DefaultLowWatermark,
            now,
            logger,
            sessionId,
            statistics: null).RemovedMessageCount;
    }

    internal static int Enforce(
        DurableAgentState state,
        DurableAgentHistoryRetentionMode mode,
        int? maxStateBytes,
        DateTimeOffset now,
        ILogger logger,
        AgentSessionId sessionId,
        ExecutionStatistics? statistics)
    {
        if (mode == DurableAgentHistoryRetentionMode.KeepAll)
        {
            if (maxStateBytes.HasValue)
            {
                throw new InvalidOperationException(
                    "A durable state budget is not applicable when history retention is KeepAll.");
            }

            return 0;
        }

        return EnforceAuto(
            state,
            mode,
            maxStateBytes,
            DefaultHighWatermark,
            DefaultLowWatermark,
            now,
            logger,
            sessionId,
            statistics).RemovedMessageCount;
    }

    internal static RetentionResult? EnforceForCommit(
        DurableAgentState state,
        DurableAgentRetentionSettings settings,
        DateTimeOffset now,
        ILogger logger,
        AgentSessionId sessionId)
    {
        if (settings.Mode == DurableAgentHistoryRetentionMode.KeepAll)
        {
            return null;
        }

        return EnforceAuto(
            state,
            settings.Mode,
            settings.MaxStateBytes,
            settings.HighWatermark,
            settings.LowWatermark,
            now,
            logger,
            sessionId,
            statistics: null);
    }

    internal static void ValidateProtectedFloor(
        DurableAgentState state,
        DurableAgentRetentionSettings settings,
        DateTimeOffset now,
        DurableAgentStateEntry? pendingEntry = null,
        ExecutionStatistics? statistics = null)
    {
        if (settings.Mode == DurableAgentHistoryRetentionMode.KeepAll)
        {
            return;
        }

        ValidateAutoSettings(
            settings.Mode,
            settings.MaxStateBytes,
            settings.HighWatermark,
            settings.LowWatermark);
        int maxStateBytes = settings.MaxStateBytes!.Value;
        int highWatermark = (int)(maxStateBytes * settings.HighWatermark);
        int initialSize;
        IList<DurableAgentStateEntry> history = state.Data.ConversationHistory;
        if (pendingEntry is not null)
        {
            history.Add(pendingEntry);
        }

        try
        {
            initialSize = GetSerializedSize(state);
            if (statistics is not null)
            {
                statistics.SerializedStateMeasurementCount++;
            }
            if (initialSize < highWatermark)
            {
                return;
            }

            ValidateSchema(state);
            if (FindEligibleExchanges(history).Count == 0)
            {
                RecordProtectedFloorFailure(
                    initialSize,
                    initialSize,
                    maxStateBytes);
            }
        }
        finally
        {
            if (pendingEntry is not null)
            {
                history.RemoveAt(history.Count - 1);
            }
        }

        DurableAgentState floorState = state.Clone();
        if (statistics is not null)
        {
            statistics.ProtectedFloorCloneCount++;
        }
        if (pendingEntry is not null)
        {
            floorState.Data.ConversationHistory.Add(pendingEntry);
        }

        List<List<DurableAgentStateEntry>> eligibleGroups =
            FindEligibleExchanges(floorState.Data.ConversationHistory);
        HashSet<DurableAgentStateEntry> floorEntries =
            CreateEntrySet(eligibleGroups.SelectMany(static group => group));
        int removedMessages = floorEntries.Sum(entry => entry.Messages.Count);
        RemoveEntries(floorState.Data.ConversationHistory, floorEntries);

        floorState.Data.Truncation = ProjectTruncation(
            floorState.Data.Truncation,
            removedMessages,
            now);
        int floorSize = GetSerializedSize(floorState);
        if (statistics is not null)
        {
            statistics.SerializedStateMeasurementCount++;
        }
        if (floorSize >= highWatermark)
        {
            RecordProtectedFloorFailure(
                initialSize,
                floorSize,
                maxStateBytes);
        }
    }

    private static void RecordProtectedFloorFailure(
        int initialSize,
        int floorSize,
        int maxStateBytes)
    {
        DurableAgentTelemetry.RecordRetention(
            new(
                RemovedEntryCount: 0,
                RemovedMessageCount: 0,
                InitialSizeBytes: initialSize,
                FinalSizeBytes: floorSize,
                ProtectedStateCapacityFailure: true),
            maxStateBytes);
        DurableAgentTelemetry.RecordOperation(
            DurableAgentTelemetry.FailedOutcome,
            DurableAgentTelemetry.NotAttemptedCommitStatus,
            deletionStaged: false);
        throw new DurableAgentStateSizeLimitExceededException(floorSize, maxStateBytes);
    }

    private static RetentionResult EnforceAuto(
        DurableAgentState state,
        DurableAgentHistoryRetentionMode mode,
        int? configuredMaxStateBytes,
        double highWatermarkRatio,
        double lowWatermarkRatio,
        DateTimeOffset now,
        ILogger logger,
        AgentSessionId sessionId,
        ExecutionStatistics? statistics)
    {
        ValidateAutoSettings(
            mode,
            configuredMaxStateBytes,
            highWatermarkRatio,
            lowWatermarkRatio);
        int maxStateBytes = configuredMaxStateBytes!.Value;

        int highWatermark = (int)(maxStateBytes * highWatermarkRatio);
        int initialSize = GetSerializedSize(state);
        if (statistics is not null)
        {
            statistics.SerializedStateMeasurementCount++;
        }
        if (initialSize < highWatermark)
        {
            RetentionResult noAction = new(
                RemovedEntryCount: 0,
                RemovedMessageCount: 0,
                InitialSizeBytes: initialSize,
                FinalSizeBytes: initialSize,
                ProtectedStateCapacityFailure: false);
            DurableAgentTelemetry.RecordRetention(noAction, maxStateBytes);
            return noAction;
        }

        ValidateSchema(state);
        DurableAgentState stagedState = state.Clone();

        int lowWatermark = (int)(maxStateBytes * lowWatermarkRatio);
        List<List<DurableAgentStateEntry>> eligibleGroups =
            FindEligibleExchanges(stagedState.Data.ConversationHistory);
        if (statistics is not null)
        {
            statistics.CandidateGroupingPassCount++;
        }

        int selectedGroupCount = FindRemovalPrefix(
            stagedState,
            eligibleGroups,
            lowWatermark,
            now,
            statistics);
        int removedEntries = 0;
        int removedMessages = 0;
        HashSet<DurableAgentStateEntry> selectedEntries =
            new(ReferenceEqualityComparer.Instance);
        for (int index = 0; index < selectedGroupCount; index++)
        {
            List<DurableAgentStateEntry> group = eligibleGroups[index];
            int removedFromGroup = group.Sum(entry => entry.Messages.Count);
            removedEntries += group.Count;
            removedMessages += removedFromGroup;
            foreach (DurableAgentStateEntry entry in group)
            {
                selectedEntries.Add(entry);
            }
        }
        RemoveEntries(stagedState.Data.ConversationHistory, selectedEntries);

        RecordTruncation(stagedState, removedMessages, now);
        int finalSize = selectedGroupCount == 0
            ? initialSize
            : GetSerializedSize(stagedState);
        if (selectedGroupCount > 0 && statistics is not null)
        {
            statistics.SerializedStateMeasurementCount++;
        }

        bool protectedStateCapacityFailure = finalSize >= highWatermark;
        RetentionResult result = new(
            removedEntries,
            removedMessages,
            initialSize,
            finalSize,
            protectedStateCapacityFailure);
        DurableAgentTelemetry.RecordRetention(result, maxStateBytes);

        if (protectedStateCapacityFailure)
        {
            logger.LogDurableHistoryStillOverBudget(
                sessionId,
                finalSize,
                maxStateBytes);
            DurableAgentTelemetry.RecordOperation(
                outcome: DurableAgentTelemetry.FailedOutcome,
                commitStatus: DurableAgentTelemetry.NotAttemptedCommitStatus,
                deletionStaged: removedEntries > 0);
            throw new DurableAgentStateSizeLimitExceededException(finalSize, maxStateBytes);
        }

        ApplyStagedTranscript(state, stagedState);
        if (removedEntries > 0)
        {
            logger.LogDurableHistoryTruncated(
                sessionId,
                initialSize,
                maxStateBytes,
                removedEntries,
                removedMessages,
                finalSize);
        }

        return result;
    }

    private static void ValidateAutoSettings(
        DurableAgentHistoryRetentionMode mode,
        int? maxStateBytes,
        double highWatermark,
        double lowWatermark)
    {
        if (mode != DurableAgentHistoryRetentionMode.Auto)
        {
            throw new ArgumentOutOfRangeException(
                nameof(mode),
                mode,
                "The durable agent history retention mode is not supported.");
        }

        if (maxStateBytes is null or <= 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(maxStateBytes),
                maxStateBytes,
                "Automatic durable history retention requires an explicit positive state byte budget.");
        }

        if (!double.IsFinite(highWatermark) ||
            !double.IsFinite(lowWatermark) ||
            lowWatermark <= 0 ||
            lowWatermark >= highWatermark ||
            highWatermark > 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(highWatermark),
                "Durable history retention watermarks must satisfy 0 < low < high <= 1.");
        }
    }

    private static void ValidateSchema(DurableAgentState state)
    {
        DurableAgentStateSchemaVersion schemaVersion =
            DurableAgentStateSchemaVersion.ParseSupported(state.SchemaVersion);
        if (schemaVersion.Major != DurableAgentState.RevisedSchemaMajorVersion)
        {
            throw new DurableAgentStateCorruptionException(
                "Automatic history retention requires schema 2 mailbox state. Legacy terminal transcript " +
                "entries must be converted to authoritative mailbox results before transcript eviction.");
        }
    }

    private static void ApplyStagedTranscript(
        DurableAgentState state,
        DurableAgentState stagedState)
    {
        state.Data.ConversationHistory.Clear();
        foreach (DurableAgentStateEntry entry in stagedState.Data.ConversationHistory)
        {
            state.Data.ConversationHistory.Add(entry);
        }

        state.Data.Truncation = stagedState.Data.Truncation;
    }

    private static HashSet<DurableAgentStateEntry> CreateEntrySet(
        IEnumerable<DurableAgentStateEntry> entries)
    {
        HashSet<DurableAgentStateEntry> result =
            new(ReferenceEqualityComparer.Instance);
        result.UnionWith(entries);
        return result;
    }

    private static void RemoveEntries(
        IList<DurableAgentStateEntry> history,
        HashSet<DurableAgentStateEntry> removedEntries)
    {
        if (removedEntries.Count == 0)
        {
            return;
        }

        List<DurableAgentStateEntry> retainedEntries = history
            .Where(entry => !removedEntries.Contains(entry))
            .ToList();
        history.Clear();
        foreach (DurableAgentStateEntry entry in retainedEntries)
        {
            history.Add(entry);
        }
    }

    private static int FindRemovalPrefix(
        DurableAgentState state,
        List<List<DurableAgentStateEntry>> eligibleGroups,
        int lowWatermark,
        DateTimeOffset now,
        ExecutionStatistics? statistics)
    {
        if (eligibleGroups.Count == 0)
        {
            return 0;
        }

        Dictionary<int, int> measuredSizes = [];
        int Measure(int groupCount)
        {
            if (measuredSizes.TryGetValue(groupCount, out int measured))
            {
                return measured;
            }

            int size = MeasureRemovalPrefix(state, eligibleGroups, groupCount, now);
            if (statistics is not null)
            {
                statistics.SerializedStateMeasurementCount++;
            }

            measuredSizes[groupCount] = size;
            return size;
        }

        int firstMessageGroupIndex = eligibleGroups.FindIndex(
            static group => group.Any(entry => entry.Messages.Count > 0));
        int zeroMessagePrefixCount = firstMessageGroupIndex < 0
            ? eligibleGroups.Count
            : firstMessageGroupIndex;
        if (zeroMessagePrefixCount > 0 &&
            Measure(zeroMessagePrefixCount) <= lowWatermark)
        {
            return FindFirstPrefixAtOrBelow(
                1,
                zeroMessagePrefixCount,
                lowWatermark,
                Measure);
        }

        if (firstMessageGroupIndex < 0)
        {
            return eligibleGroups.Count;
        }

        // Introducing truncation evidence can make the first message-bearing eviction larger than
        // the preceding zero-message prefix. After that transition, every additional group removes
        // a complete serialized entry while truncation metadata only updates its bounded counters.
        int firstPrefixWithTruncation = firstMessageGroupIndex + 1;
        if (Measure(firstPrefixWithTruncation) <= lowWatermark)
        {
            return firstPrefixWithTruncation;
        }

        if (Measure(eligibleGroups.Count) > lowWatermark)
        {
            return eligibleGroups.Count;
        }

        return FindFirstPrefixAtOrBelow(
            firstPrefixWithTruncation + 1,
            eligibleGroups.Count,
            lowWatermark,
            Measure);
    }

    private static int FindFirstPrefixAtOrBelow(
        int left,
        int right,
        int targetSize,
        Func<int, int> measure)
    {
        while (left < right)
        {
            int middle = left + ((right - left) / 2);
            if (measure(middle) <= targetSize)
            {
                right = middle;
            }
            else
            {
                left = middle + 1;
            }
        }

        return left;
    }

    private static int MeasureRemovalPrefix(
        DurableAgentState state,
        List<List<DurableAgentStateEntry>> eligibleGroups,
        int groupCount,
        DateTimeOffset now)
    {
        List<DurableAgentStateEntry> originalHistory = [.. state.Data.ConversationHistory];
        DurableAgentStateTruncation? originalTruncation = state.Data.Truncation;
        HashSet<DurableAgentStateEntry> removedEntries =
            new(ReferenceEqualityComparer.Instance);
        int removedMessages = 0;
        for (int index = 0; index < groupCount; index++)
        {
            foreach (DurableAgentStateEntry entry in eligibleGroups[index])
            {
                removedEntries.Add(entry);
                removedMessages += entry.Messages.Count;
            }
        }

        try
        {
            state.Data.ConversationHistory.Clear();
            foreach (DurableAgentStateEntry entry in originalHistory)
            {
                if (!removedEntries.Contains(entry))
                {
                    state.Data.ConversationHistory.Add(entry);
                }
            }

            state.Data.Truncation = ProjectTruncation(
                originalTruncation,
                removedMessages,
                now);
            return GetSerializedSize(state);
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

    private static DurableAgentStateTruncation? ProjectTruncation(
        DurableAgentStateTruncation? original,
        int removedMessages,
        DateTimeOffset now)
    {
        if (removedMessages == 0)
        {
            return original;
        }

        DateTimeOffset effectiveTime = GetEffectiveEvictionTime(original, now);
        return new DurableAgentStateTruncation
        {
            EvictedMessageCount = ProjectEvictedMessagesForSizing(
                original?.EvictedMessageCount ?? default,
                removedMessages),
            FirstEvictedAt = original?.FirstEvictedAt ?? effectiveTime,
            LastEvictedAt = effectiveTime,
            UnknownProperties = original?.UnknownProperties,
        };
    }

    private static List<List<DurableAgentStateEntry>> FindEligibleExchanges(
        IList<DurableAgentStateEntry> history)
    {
        HashSet<DurableAgentStateEntry> protectedEntries =
            FindNewestProtectedEntries(history);
        List<DurableAgentStateEntry> eligibleEntries =
            history.Where(entry => !protectedEntries.Contains(entry)).ToList();
        return BuildAtomicGroups(eligibleEntries);
    }

    private static HashSet<DurableAgentStateEntry> FindNewestProtectedEntries(
        IList<DurableAgentStateEntry> history)
    {
        HashSet<DurableAgentStateEntry> protectedEntries =
            new(ReferenceEqualityComparer.Instance);
        if (history.Count == 0)
        {
            return protectedEntries;
        }

        Dictionary<string, List<DurableAgentStateEntry>> correlationOccurrences =
            new(StringComparer.Ordinal);
        Dictionary<string, List<DurableAgentStateEntry>> toolOccurrences =
            new(StringComparer.Ordinal);
        foreach (DurableAgentStateEntry entry in history)
        {
            if (entry.CorrelationId is string correlationId)
            {
                AddOccurrence(correlationOccurrences, correlationId, entry);
            }

            foreach (string callId in GetToolCallIds(entry))
            {
                AddOccurrence(toolOccurrences, callId, entry);
            }
        }

        Queue<DurableAgentStateEntry> pendingEntries = new();
        Protect(history[^1]);
        foreach (DurableAgentStateEntry entry in history)
        {
            if (ContainsSystemMessage(entry))
            {
                Protect(entry);
            }
        }

        HashSet<string> visitedCorrelations = new(StringComparer.Ordinal);
        HashSet<string> visitedCallIds = new(StringComparer.Ordinal);
        while (pendingEntries.TryDequeue(out DurableAgentStateEntry? entry))
        {
            if (entry.CorrelationId is string correlationId &&
                visitedCorrelations.Add(correlationId))
            {
                ProtectOccurrences(correlationOccurrences, correlationId);
            }

            foreach (string callId in GetToolCallIds(entry))
            {
                if (visitedCallIds.Add(callId))
                {
                    ProtectOccurrences(toolOccurrences, callId);
                }
            }
        }

        return protectedEntries;

        static void AddOccurrence(
            Dictionary<string, List<DurableAgentStateEntry>> occurrencesById,
            string id,
            DurableAgentStateEntry entry)
        {
            if (!occurrencesById.TryGetValue(
                id,
                out List<DurableAgentStateEntry>? occurrences))
            {
                occurrences = [];
                occurrencesById.Add(id, occurrences);
            }

            occurrences.Add(entry);
        }

        void Protect(DurableAgentStateEntry entry)
        {
            if (protectedEntries.Add(entry))
            {
                pendingEntries.Enqueue(entry);
            }
        }

        void ProtectOccurrences(
            Dictionary<string, List<DurableAgentStateEntry>> occurrencesById,
            string id)
        {
            if (!occurrencesById.TryGetValue(
                id,
                out List<DurableAgentStateEntry>? occurrences))
            {
                return;
            }

            foreach (DurableAgentStateEntry occurrence in occurrences)
            {
                Protect(occurrence);
            }
        }
    }

    private static bool ContainsSystemMessage(DurableAgentStateEntry entry) =>
        entry.Messages.Any(
            message => string.Equals(
                message.Role,
                ChatRole.System.ToString(),
                StringComparison.Ordinal));

    private static IEnumerable<string> GetToolCallIds(
        DurableAgentStateEntry entry)
    {
        HashSet<string> ids = new(StringComparer.Ordinal);
        foreach (DurableAgentStateContent content in
            entry.Messages.SelectMany(message => message.Contents))
        {
            string? callId = content switch
            {
                DurableAgentStateFunctionCallContent functionCall =>
                    functionCall.CallId,
                DurableAgentStateFunctionResultContent functionResult =>
                    functionResult.CallId,
                _ => null,
            };
            if (!string.IsNullOrWhiteSpace(callId) && ids.Add(callId))
            {
                yield return callId;
            }
        }
    }

    private static List<List<DurableAgentStateEntry>> BuildAtomicGroups(
        List<DurableAgentStateEntry> history)
    {
        int[] parents = Enumerable.Range(0, history.Count).ToArray();
        Dictionary<string, int> correlationOwners = new(StringComparer.Ordinal);
        Dictionary<string, int> toolCallOwners = new(StringComparer.Ordinal);

        for (int index = 0; index < history.Count; index++)
        {
            DurableAgentStateEntry entry = history[index];
            if (entry.CorrelationId is not null)
            {
                UnionWithOwner(correlationOwners, entry.CorrelationId, index);
            }

            HashSet<string> entryToolCallIds = new(StringComparer.Ordinal);
            foreach (DurableAgentStateContent content in entry.Messages.SelectMany(message => message.Contents))
            {
                string? callId = content switch
                {
                    DurableAgentStateFunctionCallContent functionCall => functionCall.CallId,
                    DurableAgentStateFunctionResultContent functionResult => functionResult.CallId,
                    _ => null,
                };

                if (!string.IsNullOrWhiteSpace(callId) && entryToolCallIds.Add(callId))
                {
                    UnionWithOwner(toolCallOwners, callId, index);
                }
            }
        }

        Dictionary<int, List<DurableAgentStateEntry>> components = [];
        List<int> roots = [];
        for (int index = 0; index < history.Count; index++)
        {
            int root = Find(index);
            if (!components.TryGetValue(root, out List<DurableAgentStateEntry>? component))
            {
                component = [];
                components[root] = component;
                roots.Add(root);
            }

            component.Add(history[index]);
        }

        return roots.ConvertAll(root => components[root]);

        void UnionWithOwner(Dictionary<string, int> owners, string key, int index)
        {
            if (owners.TryGetValue(key, out int owner))
            {
                Union(owner, index);
            }
            else
            {
                owners[key] = index;
            }
        }

        int Find(int index)
        {
            while (parents[index] != index)
            {
                parents[index] = parents[parents[index]];
                index = parents[index];
            }

            return index;
        }

        void Union(int first, int second)
        {
            int firstRoot = Find(first);
            int secondRoot = Find(second);
            if (firstRoot == secondRoot)
            {
                return;
            }

            if (firstRoot < secondRoot)
            {
                parents[secondRoot] = firstRoot;
            }
            else
            {
                parents[firstRoot] = secondRoot;
            }
        }
    }

    private static void RecordTruncation(
        DurableAgentState state,
        int removedMessages,
        DateTimeOffset now)
    {
        if (removedMessages == 0)
        {
            return;
        }

        DurableAgentStateTruncation truncation = state.Data.Truncation ??= new()
        {
            FirstEvictedAt = now,
        };

        truncation.EvictedMessageCount = AddEvictedMessages(
            truncation.EvictedMessageCount,
            removedMessages);
        truncation.LastEvictedAt = GetEffectiveEvictionTime(truncation, now);
    }

    private static JsonElement AddEvictedMessages(
        JsonElement current,
        int added)
    {
        long currentValue = ReadEvictedMessageCount(current);
        try
        {
            return JsonSerializer.SerializeToElement(
                checked(currentValue + added),
                DurableAgentStateJsonContext.Default.Int64);
        }
        catch (OverflowException exception)
        {
            throw new InvalidOperationException(
                "The durable evicted message count cannot exceed the nonnegative Int64 range.",
                exception);
        }
    }

    private static JsonElement ProjectEvictedMessagesForSizing(
        JsonElement current,
        int added)
    {
        long currentValue = ReadEvictedMessageCount(current);
        // Prefix probes are hypothetical; cap only their serialized width so a larger infeasible
        // prefix cannot prevent selection of a smaller prefix whose actual checked increment fits.
        long projectedIncrement = Math.Min(
            added,
            long.MaxValue - currentValue);
        return JsonSerializer.SerializeToElement(
            checked(currentValue + projectedIncrement),
            DurableAgentStateJsonContext.Default.Int64);
    }

    private static long ReadEvictedMessageCount(JsonElement current)
    {
        if (current.ValueKind == JsonValueKind.Undefined)
        {
            return 0;
        }

        if (!DurableAgentStateContract.TryGetInt64(current, out long value) ||
            value < 0)
        {
            throw new InvalidOperationException(
                "The durable evicted message count must be a nonnegative Int64 integer.");
        }

        return value;
    }

    private static DateTimeOffset GetEffectiveEvictionTime(
        DurableAgentStateTruncation? truncation,
        DateTimeOffset now)
    {
        if (truncation is null)
        {
            return now;
        }

        DateTimeOffset effectiveTime = now > truncation.LastEvictedAt
            ? now
            : truncation.LastEvictedAt;
        return effectiveTime > truncation.FirstEvictedAt
            ? effectiveTime
            : truncation.FirstEvictedAt;
    }

    private sealed class SerializedByteCountingStream : Stream
    {
        public long BytesWritten { get; private set; }

        public override bool CanRead => false;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public override long Length => this.BytesWritten;

        public override long Position
        {
            get => this.BytesWritten;
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) =>
            throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) =>
            this.BytesWritten = checked(this.BytesWritten + count);

        public override void Write(ReadOnlySpan<byte> buffer) =>
            this.BytesWritten = checked(this.BytesWritten + buffer.Length);
    }
}
