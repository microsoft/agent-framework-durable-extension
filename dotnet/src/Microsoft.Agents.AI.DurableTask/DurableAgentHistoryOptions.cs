// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Configures history persistence for one durable agent registration.
/// </summary>
public sealed class DurableAgentHistoryOptions
{
    /// <summary>
    /// Gets or sets whether the model service owns history when Agent Framework per-service-call
    /// history persistence is enabled.
    /// </summary>
    public bool ServiceManagedPerServiceCallHistory { get; set; }

    /// <summary>
    /// Gets or sets how an agent without a discoverable chat-context pipeline receives prior history.
    /// </summary>
    public DurableAgentHistoryReplayMode ReplayMode
    {
        get;
        set => field = Enum.IsDefined(value)
            ? value
            : throw new ArgumentOutOfRangeException(nameof(value), value, "The history replay mode is not supported.");
    } = DurableAgentHistoryReplayMode.PreloadEntityHistory;

    /// <summary>
    /// Gets or sets the stable logical identity of a non-entity history owner.
    /// </summary>
    public DurableAgentHistoryProviderKey? ProviderKey { get; set; }
}