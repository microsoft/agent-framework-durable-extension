// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json.Serialization;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Bounded diagnostic evidence that durable transcript messages were removed.
/// </summary>
/// <remarks>
/// This object survives the removed entries so a persisted gap is not mistaken for history that never
/// existed. It does not contain model context and does not establish request delivery or completion.
/// </remarks>
internal sealed class DurableAgentStateTruncation
{
    /// <summary>
    /// Gets or sets the cumulative number of transcript messages known to have been removed.
    /// </summary>
    [JsonPropertyName("evictedMessageCount")]
    public int EvictedMessageCount { get; set; }

    /// <summary>
    /// Gets or sets when transcript removal was first recorded.
    /// </summary>
    [JsonPropertyName("firstEvictedAt")]
    public DateTimeOffset FirstEvictedAt { get; set; }

    /// <summary>
    /// Gets or sets when transcript removal was most recently recorded.
    /// </summary>
    [JsonPropertyName("lastEvictedAt")]
    public DateTimeOffset LastEvictedAt { get; set; }
}
