// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Identifies the provider lifecycle phase that produced a certified terminal failure.
/// </summary>
internal enum DurableAgentProviderFailurePhase
{
    Load,
    Invoke,
    Store,
}

/// <summary>
/// Identifies why no further provider retry is permitted for a certified terminal failure.
/// </summary>
internal enum DurableAgentProviderFailureFinality
{
    NonRetryable,
    RetriesExhausted,
}

/// <summary>
/// Sanitized, explicit evidence supplied by a certified provider adapter.
/// </summary>
internal sealed record DurableAgentProviderFailureAttestation(
    DurableAgentProviderFailurePhase Phase,
    DurableAgentProviderFailureFinality Finality,
    bool InputAccepted,
    string Code,
    string Message,
    JsonElement Details = default);

/// <summary>
/// Classifies only provider failures whose finality and accepted-input semantics are authoritative.
/// </summary>
/// <remarks>
/// No implementation is registered by default. Generic exception types, messages, and HTTP status codes
/// are not sufficient evidence. A certified adapter must understand its provider's load/invoke/store
/// boundary and retry policy and return sanitized JSON-safe metadata.
/// </remarks>
internal interface IDurableAgentProviderFailureAttestor
{
    bool TryAttest(Exception exception, out DurableAgentProviderFailureAttestation? attestation);
}
