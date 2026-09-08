// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Projects durable conversation entries into messages that are safe to send back to a model.
/// </summary>
internal static class DurableAgentStateReplay
{
    public static IEnumerable<ChatMessage> GetMessages(
        IEnumerable<DurableAgentStateEntry> history,
        string excludedCorrelationId)
    {
        foreach (DurableAgentStateEntry entry in history)
        {
            // errorResponse is a terminal result for durable callers, not model conversation
            // context. Replaying its failure text would pollute the next prompt.
            if (entry is DurableAgentStateErrorResponse ||
                entry.CorrelationId == excludedCorrelationId)
            {
                continue;
            }

            foreach (DurableAgentStateMessage storedMessage in entry.Messages)
            {
                // Stored message envelopes are non-null. The conversion is nullable because it
                // removes provider-specific reasoning and messages with no remaining replayable content.
                if (storedMessage.ToReplayableChatMessage() is ChatMessage replayableMessage)
                {
                    yield return replayableMessage;
                }
            }
        }
    }
}
