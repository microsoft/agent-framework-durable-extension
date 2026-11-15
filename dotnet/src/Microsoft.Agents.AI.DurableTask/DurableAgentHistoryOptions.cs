// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Configures history persistence for one durable agent registration.
/// </summary>
/// <remarks>
/// These settings participate in the fixed-owner schema 2 profile that new durable sessions use by
/// default. A custom history provider must be registered with a stable <see cref="ProviderKey"/> and
/// must write a usable continuation value for every key it declares in <c>StateKeys</c> into the
/// session state bag on each turn. Legacy entities and explicit opt-outs preserve the pre-profile
/// schema 1 behavior.
/// </remarks>
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
    /// <remarks>
    /// Required under the default schema 2 profile when a custom <c>ChatHistoryProvider</c> or a
    /// model service owns history. The key must be stable across deployments, must not contain
    /// secrets, and must not change for an existing session; a different key is rejected as a
    /// different history owner.
    /// </remarks>
    public DurableAgentHistoryProviderKey? ProviderKey { get; set; }
}
