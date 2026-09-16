// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Controls how durable agent conversation state is retained.
/// </summary>
public enum DurableAgentHistoryRetentionMode
{
    /// <summary>
    /// Never proactively removes conversation entries. Persistence can still fail when a backend or provider
    /// state limit is reached.
    /// </summary>
    KeepAll,

    /// <summary>
    /// Removes the oldest eligible exchanges when serialized entity state reaches the configured high watermark.
    /// </summary>
    /// <remarks>
    /// Only conversation transcript entries are eligible. Mailbox results, completion receipts, history
    /// binding, provider continuation, TTL, and other execution-control state are protected. The newest
    /// transcript exchange and system messages are never evicted; if protected state cannot fit below the
    /// safe write threshold, the operation fails instead of persisting oversized state.
    /// </remarks>
    Auto,
}
