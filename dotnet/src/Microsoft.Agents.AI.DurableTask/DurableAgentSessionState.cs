// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Restores and serializes the inner Agent Framework session carried as opaque JSON in durable state.
/// </summary>
/// <remarks>
/// This helper intentionally lives beside the agent integration rather than the durable-state DTOs:
/// the concrete <see cref="AIAgent"/> owns the session format through
/// <see cref="AIAgent.SerializeSessionAsync"/> and <see cref="AIAgent.DeserializeSessionAsync"/>.
/// The durable layer neither interprets that JSON nor adds a second serializer customization API.
/// </remarks>
internal static class DurableAgentSessionState
{
    public static ValueTask<AgentSession> RestoreAsync(
        AIAgent agent,
        JsonElement? serializedSession,
        CancellationToken cancellationToken)
    {
        return serializedSession is JsonElement serialized
            ? agent.DeserializeSessionAsync(serialized, cancellationToken: cancellationToken)
            : agent.CreateSessionAsync(cancellationToken);
    }

    public static ValueTask<JsonElement> SerializeAsync(
        AIAgent agent,
        AgentSession session,
        IEnumerable<string> excludedStateKeys,
        CancellationToken cancellationToken)
    {
        foreach (string stateKey in excludedStateKeys)
        {
            _ = session.StateBag.TryRemoveValue(stateKey);
        }

        return agent.SerializeSessionAsync(session, cancellationToken: cancellationToken);
    }
}
