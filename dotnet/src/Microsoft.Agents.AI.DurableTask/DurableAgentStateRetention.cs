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
    internal const double HighWatermark = 0.85;
    internal const double LowWatermark = 0.70;

    internal sealed class ExecutionStatistics
    {
        public int CandidateGroupingPassCount { get; set; }

        public int SerializedStateMeasurementCount { get; set; }
    }

    public static int GetSerializedSize(DurableAgentState state)
    {
        return JsonSerializer.SerializeToUtf8Bytes(
            state,
            DurableAgentStateJsonContext.Default.DurableAgentState).Length;
    }

    public static int Enforce(
        DurableAgentState state,
        DurableAgentHistoryRetentionMode mode,
        int maxStateBytes,
        DateTimeOffset now,
        ILogger logger,
        AgentSessionId sessionId) =>
        Enforce(state, mode, maxStateBytes, now, logger, sessionId, statistics: null);

    internal static int Enforce(
        DurableAgentState state,
        DurableAgentHistoryRetentionMode mode,
        int maxStateBytes,
        DateTimeOffset now,
        ILogger logger,
        AgentSessionId sessionId,
        ExecutionStatistics? statistics)
    {
        if (mode == DurableAgentHistoryRetentionMode.KeepAll)
        {
            return 0;
        }

        if (mode != DurableAgentHistoryRetentionMode.Auto)
        {
            throw new ArgumentOutOfRangeException(
                nameof(mode),
                mode,
                "The durable agent history retention mode is not supported.");
        }

        int highWatermark = (int)(maxStateBytes * HighWatermark);
        int initialSize = GetSerializedSize(state);
        if (statistics is not null)
        {
            statistics.SerializedStateMeasurementCount++;
        }
        if (initialSize < highWatermark)
        {
            DurableAgentTelemetry.RecordNoAction(sessionId.Name);
            return 0;
        }

        DurableAgentStateSchemaVersion schemaVersion =
            DurableAgentStateSchemaVersion.ParseSupported(state.SchemaVersion);
        if (schemaVersion.Major != DurableAgentState.RevisedSchemaMajorVersion)
        {
            throw new DurableAgentStateCorruptionException(
                "Automatic history retention requires schema 2 mailbox state. Legacy terminal transcript " +
                "entries must be converted to authoritative mailbox results before transcript eviction.");
        }

        int lowWatermark = (int)(maxStateBytes * LowWatermark);
        List<List<DurableAgentStateEntry>> eligibleGroups =
            FindEligibleExchanges(state.Data.ConversationHistory);
        if (statistics is not null)
        {
            statistics.CandidateGroupingPassCount++;
        }

        int selectedGroupCount = FindRemovalPrefix(
            state,
            eligibleGroups,
            lowWatermark,
            now,
            statistics);
        int removedEntries = 0;
        int removedMessages = 0;
        for (int index = 0; index < selectedGroupCount; index++)
        {
            List<DurableAgentStateEntry> group = eligibleGroups[index];
            int removedFromGroup = group.Sum(entry => entry.Messages.Count);
            removedEntries += group.Count;
            removedMessages += removedFromGroup;
            foreach (DurableAgentStateEntry entry in group)
            {
                _ = state.Data.ConversationHistory.Remove(entry);
            }
        }

        RecordTruncation(state, removedMessages, now);
        int finalSize = selectedGroupCount == 0
            ? initialSize
            : GetSerializedSize(state);
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
        DurableAgentTelemetry.RecordRetentionAttempt(sessionId.Name, result);

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

        if (protectedStateCapacityFailure)
        {
            logger.LogDurableHistoryStillOverBudget(
                sessionId,
                finalSize,
                maxStateBytes);
            throw new DurableAgentStateSizeLimitExceededException(finalSize, maxStateBytes);
        }

        return result.RemovedMessageCount;
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
        HashSet<DurableAgentStateEntry> removedEntries = [];
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
            EvictedMessageCount =
                (original?.EvictedMessageCount ?? 0) + removedMessages,
            FirstEvictedAt = original?.FirstEvictedAt ?? effectiveTime,
            LastEvictedAt = effectiveTime,
            UnknownProperties = original?.UnknownProperties,
        };
    }

    private static List<List<DurableAgentStateEntry>> FindEligibleExchanges(
        IList<DurableAgentStateEntry> history)
    {
        List<List<DurableAgentStateEntry>> groups = BuildAtomicGroups(history);
        List<DurableAgentStateEntry>? newestGroup = history.Count == 0
            ? null
            : groups.First(group => group.Contains(history[^1]));

        return groups
            .Where(group =>
                !ReferenceEquals(group, newestGroup) &&
                !group.Any(entry =>
                    entry.Messages.Any(message => message.Role == ChatRole.System.ToString())))
            .ToList();
    }

    private static List<List<DurableAgentStateEntry>> BuildAtomicGroups(
        IList<DurableAgentStateEntry> history)
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

        truncation.EvictedMessageCount += removedMessages;
        truncation.LastEvictedAt = GetEffectiveEvictionTime(truncation, now);
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
}
