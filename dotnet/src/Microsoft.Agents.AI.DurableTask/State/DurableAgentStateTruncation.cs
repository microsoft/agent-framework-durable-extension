// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json.Serialization;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Bounded evidence that durable conversation entries were removed by retention.
/// </summary>
internal sealed class DurableAgentStateTruncation
{
    /// <summary>
    /// Gets or sets the total number of messages removed over the lifetime of the session.
    /// </summary>
    [JsonPropertyName("evictedMessageCount")]
    public int EvictedMessageCount { get; set; }

    /// <summary>
    /// Gets or sets when the first eviction occurred.
    /// </summary>
    [JsonPropertyName("firstEvictedAt")]
    public DateTimeOffset FirstEvictedAt { get; set; }

    /// <summary>
    /// Gets or sets when the latest eviction occurred.
    /// </summary>
    [JsonPropertyName("lastEvictedAt")]
    public DateTimeOffset LastEvictedAt { get; set; }
}
