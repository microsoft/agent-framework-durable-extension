// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Represents a compacted transcript message written by another durable agent implementation.
/// </summary>
/// <remarks>
/// This layer serializes, deserializes, and converts the shared compaction contract. Agent entity
/// replay and retention integration are deferred to later layers.
/// </remarks>
internal sealed class DurableAgentStateCompaction : DurableAgentStateEntry;
