// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Controls which state contract a durable agent deployment may write.
/// </summary>
public enum DurableAgentDeploymentMode
{
    /// <summary>
    /// Preserves the schema 1.2 writer and historical behavior. Schema 2 results remain readable.
    /// </summary>
    Legacy = 0,

    /// <summary>
    /// Acknowledges an isolated deployment with schema-2-compatible workers, clients and readers.
    /// </summary>
    /// <remarks>
    /// Authorizes schema 2 for genuinely new sessions with either retention policy. This is an operator
    /// acknowledgement, not verification of routing or isolation, and never authorizes legacy migration.
    /// </remarks>
    SchemaV2 = 1,
}
