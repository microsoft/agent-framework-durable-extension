// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Represents a failed turn recorded by another durable agent implementation.
/// </summary>
/// <remarks>
/// .NET durable agents do not currently create pollable error responses, but preserve this shared-schema
/// entry kind when reading state written by another language implementation.
/// </remarks>
internal sealed class DurableAgentStateErrorResponse : DurableAgentStateResponse;
