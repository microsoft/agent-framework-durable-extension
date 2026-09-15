// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Controls input history for an agent that does not expose an Agent Framework chat-context pipeline.
/// </summary>
public enum DurableAgentHistoryReplayMode
{
    /// <summary>
    /// Stores full requests in entity history and prepends replayable entity history to each invocation.
    /// </summary>
    /// <remarks>
    /// This is the default for backward compatibility with generic local <see cref="AIAgent"/> implementations.
    /// </remarks>
    PreloadEntityHistory,

    /// <summary>
    /// Passes only the current request and relies on the restored opaque agent session or remote service for context.
    /// </summary>
    /// <remarks>
    /// The entity retains request identity metadata and the final response needed for durable delivery, but does not
    /// retain a second request transcript for replay.
    /// </remarks>
    CurrentRequestOnly,
}
