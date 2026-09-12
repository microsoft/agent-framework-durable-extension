// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>Provides access to canonical results retained by durable agent delivery.</summary>
public static class DurableAgentResponseExtensions
{
    /// <summary>
    /// Gets the immutable canonical terminal-response JSON accompanying a durable agent response.
    /// </summary>
    /// <remarks>
    /// This preserves stored response metadata, opaque content, and an independently supplied
    /// <c>value</c>. An absent <c>value</c> property differs from explicit JSON null. No value is
    /// inferred from text. Native <see cref="AgentResponse"/> serialization is unchanged; the
    /// registered durable data converter transports this canonical result across durable calls.
    /// Serializing through an unrelated serializer or reconstructing the response does not retain
    /// that association.
    /// </remarks>
    /// <param name="response">The response returned by a durable agent or proxy.</param>
    /// <returns>Canonical result JSON, or null when no retained result accompanies the response.</returns>
    public static JsonElement? GetDurableResult(this AgentResponse response) =>
        DurableAgentJsonUtilities.GetRetainedResult(response);
}
