// Copyright (c) Microsoft. All rights reserved.

using System.Globalization;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Assigns deterministic identities to durable messages that predate schema 1.2.
/// </summary>
internal static class DurableAgentStateMessageIdentity
{
    public static void EnsureMessageIds(IEnumerable<DurableAgentStateEntry> history)
    {
        foreach (DurableAgentStateEntry entry in history)
        {
            string entryType = entry switch
            {
                DurableAgentStateErrorResponse => "errorResponse",
                DurableAgentStateRequest => "request",
                DurableAgentStateResponse => "response",
                DurableAgentStateCompaction => "compaction",
                _ => throw new InvalidOperationException(
                    $"Unsupported durable agent state entry type '{entry.GetType()}'."),
            };

            for (int index = 0; index < entry.Messages.Count; index++)
            {
                DurableAgentStateMessage message = entry.Messages[index];
                if (message.MessageId is null)
                {
                    message.MessageId = Create(entryType, entry.CorrelationId, entry.CreatedAt, index);
                }
            }
        }
    }

    public static string Create(
        string entryType,
        string? correlationId,
        DateTimeOffset createdAt,
        int storedIndex)
    {
        string scope = string.IsNullOrEmpty(correlationId)
            ? FormatPythonIsoTimestamp(createdAt)
            : correlationId;
        return $"durable_{entryType}_{scope}_{storedIndex}";
    }

    internal static string FormatPythonIsoTimestamp(DateTimeOffset timestamp)
    {
        long microseconds = timestamp.Ticks % TimeSpan.TicksPerSecond / 10;
        string fraction = microseconds == 0
            ? string.Empty
            : $".{microseconds.ToString("D6", CultureInfo.InvariantCulture)}";
        return string.Concat(
            timestamp.ToString("yyyy-MM-dd'T'HH:mm:ss", CultureInfo.InvariantCulture),
            fraction,
            timestamp.ToString("zzz", CultureInfo.InvariantCulture));
    }
}
