// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Binds a durable session to one logical history owner for its lifetime.
/// </summary>
internal sealed class DurableAgentStateHistoryBinding
{
    public const int CurrentVersion = 1;
    public const string DurableStateOwner = "durableState";
    public const string HistoryProviderOwner = "historyProvider";
    public const string ModelServiceOwner = "modelService";

    [JsonPropertyName("version")]
    [JsonRequired]
    public int Version { get; init; } = CurrentVersion;

    [JsonPropertyName("ownerKind")]
    public required string OwnerKind { get; init; }

    [JsonPropertyName("providerKey")]
    public required string ProviderKey { get; init; }

    [JsonExtensionData]
    public IDictionary<string, JsonElement>? UnknownProperties { get; set; }

    public void Validate()
    {
        if (this.Version != CurrentVersion)
        {
            throw new InvalidOperationException(
                $"The durable agent state history binding version '{this.Version}' is not supported.");
        }

        if (this.OwnerKind is not DurableStateOwner and
            not HistoryProviderOwner and
            not ModelServiceOwner)
        {
            throw new InvalidOperationException(
                $"The durable agent state history owner kind '{this.OwnerKind}' is not supported.");
        }

        DurableAgentStateContract.ValidateIdentifier(this.ProviderKey, "historyBinding.providerKey");
    }
}
