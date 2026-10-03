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
    /// The SDK <see cref="AgentResponse"/> is a convenient projection of a persisted terminal response, but it cannot
    /// represent every durable field. This method exposes the exact terminal-response JSON that accompanied the
    /// response, preserving stored metadata, opaque content, unknown future fields, and an independently supplied
    /// <c>value</c>. In particular, an absent <c>value</c> property differs from explicit JSON null; no value is inferred
    /// from response text.
    ///
    /// The result is associated with the response without modifying native <see cref="AgentResponse"/> serialization.
    /// The registered durable data converter transports the association across entity/orchestration calls. Serializing
    /// through an unrelated converter or manually reconstructing the response does not preserve it because there is no
    /// committed durable result associated with that new object.
    /// </remarks>
    /// <param name="response">The response returned by a durable agent or proxy.</param>
    /// <returns>Canonical result JSON, or null when no retained result accompanies the response.</returns>
    public static JsonElement? GetDurableResult(this AgentResponse response) =>
        DurableAgentJsonUtilities.GetRetainedResult(response);
}
