// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Agents.AI.DurableTask.State;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Identifies the logical provider that owns history for a durable agent.
/// </summary>
/// <remarks>
/// The value is persisted with the durable session. It must be stable across provider and agent
/// instances and must not contain credentials or other secrets.
/// </remarks>
public sealed record DurableAgentHistoryProviderKey
{
    /// <summary>
    /// Initializes a new logical provider key.
    /// </summary>
    /// <param name="value">The stable, non-secret provider identifier.</param>
    public DurableAgentHistoryProviderKey(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        DurableAgentStateContract.ValidateIdentifier(value, nameof(value));
        this.Value = value;
    }

    /// <summary>
    /// Gets the value persisted in durable state.
    /// </summary>
    public string Value { get; }

    /// <inheritdoc/>
    public override string ToString() => this.Value;
}
