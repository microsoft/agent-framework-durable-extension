// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Agents.AI.DurableTask.State;

namespace Microsoft.Agents.AI.DurableTask;

internal sealed class DurableAgentStateHistoryBinding
{
    public const int CurrentVersion = 1;
    public const string DurableStateOwner = "durableState";
    public const string HistoryProviderOwner = "historyProvider";
    public const string ModelServiceOwner = "modelService";

    public int Version { get; init; } = CurrentVersion;

    public required string OwnerKind { get; init; }

    public required string ProviderKey { get; init; }

    public IDictionary<string, System.Text.Json.JsonElement>? UnknownProperties { get; set; }
}

/// <summary>
/// Creates and validates the fixed logical history binding for a durable session.
/// </summary>
internal static class DurableAgentHistoryBinding
{
    internal const string DurableStateProviderKey = "durable-state.v1";
    internal const string FrameworkLocalHistoryConversationId = "_agent_local_chat_history";
    private const string CSharpFixedOwnerProperty = "csharpFixedOwner";

    public static DurableAgentStateHistoryBinding Create(
        DurableAgentHistoryOwnership ownership,
        string? configuredProviderKey,
        bool remoteTransitionDetectedAfterExecution = false)
    {
        DurableAgentStateHistoryBinding binding = ownership switch
        {
            DurableAgentHistoryOwnership.Entity => new()
            {
                OwnerKind = DurableAgentStateHistoryBinding.DurableStateOwner,
                ProviderKey = DurableStateProviderKey,
            },
            DurableAgentHistoryOwnership.ExternalProvider or
            DurableAgentHistoryOwnership.AgentSession => new()
            {
                OwnerKind = DurableAgentStateHistoryBinding.HistoryProviderOwner,
                ProviderKey = RequireProviderKey(
                    ownership,
                    configuredProviderKey,
                    remoteTransitionDetectedAfterExecution),
            },
            DurableAgentHistoryOwnership.Service => new()
            {
                OwnerKind = DurableAgentStateHistoryBinding.ModelServiceOwner,
                ProviderKey = RequireProviderKey(
                    ownership,
                    configuredProviderKey,
                    remoteTransitionDetectedAfterExecution),
            },
            _ => throw new InvalidOperationException(
                $"History ownership '{ownership}' is not a sealable durable owner."),
        };
        binding.UnknownProperties = new Dictionary<string, System.Text.Json.JsonElement>
        {
            [CSharpFixedOwnerProperty] =
                System.Text.Json.JsonDocument.Parse("true").RootElement.Clone(),
        };
        return binding;
    }

    public static bool IsSealedByCSharp(DurableAgentStateHistoryBinding? binding)
    {
        return binding?.UnknownProperties?.TryGetValue(
                CSharpFixedOwnerProperty,
                out System.Text.Json.JsonElement value) is true &&
            value.ValueKind is System.Text.Json.JsonValueKind.True;
    }

    public static DurableAgentStateHistoryBinding? Parse(
        System.Text.Json.JsonElement binding)
    {
        if (binding.ValueKind == System.Text.Json.JsonValueKind.Undefined)
        {
            return null;
        }

        if (binding.ValueKind != System.Text.Json.JsonValueKind.Object ||
            !binding.TryGetProperty("version", out System.Text.Json.JsonElement version) ||
            !version.TryGetInt32(out int versionValue) ||
            !binding.TryGetProperty("ownerKind", out System.Text.Json.JsonElement ownerKind) ||
            ownerKind.ValueKind != System.Text.Json.JsonValueKind.String ||
            !binding.TryGetProperty("providerKey", out System.Text.Json.JsonElement providerKey) ||
            providerKey.ValueKind != System.Text.Json.JsonValueKind.String)
        {
            return null;
        }

        string ownerKindValue = ownerKind.GetString()!;
        string providerKeyValue = providerKey.GetString()!;
        if (versionValue != DurableAgentStateHistoryBinding.CurrentVersion ||
            ownerKindValue is not (
                DurableAgentStateHistoryBinding.DurableStateOwner or
                DurableAgentStateHistoryBinding.HistoryProviderOwner or
                DurableAgentStateHistoryBinding.ModelServiceOwner) ||
            string.IsNullOrWhiteSpace(providerKeyValue))
        {
            return null;
        }

        Dictionary<string, System.Text.Json.JsonElement> unknown = [];
        foreach (System.Text.Json.JsonProperty property in binding.EnumerateObject())
        {
            if (property.Name is not "version" and not "ownerKind" and not "providerKey")
            {
                unknown[property.Name] = property.Value.Clone();
            }
        }

        return new DurableAgentStateHistoryBinding
        {
            Version = versionValue,
            OwnerKind = ownerKindValue,
            ProviderKey = providerKeyValue,
            UnknownProperties = unknown.Count == 0 ? null : unknown,
        };
    }

    public static void ValidateMarkedProfile(
        System.Text.Json.JsonElement binding,
        DurableAgentStateHistoryBinding? parsed)
    {
        if (binding.ValueKind != System.Text.Json.JsonValueKind.Object ||
            !binding.TryGetProperty(
                CSharpFixedOwnerProperty,
                out System.Text.Json.JsonElement marker) ||
            marker.ValueKind != System.Text.Json.JsonValueKind.True)
        {
            return;
        }

        if (parsed is null)
        {
            throw new DurableAgentHistoryBindingMismatchException(
                "The durable session contains a C# fixed-owner history profile that is malformed or " +
                "uses an unsupported version. Restore a supported profile or start a new durable session.");
        }

        if (parsed.OwnerKind == DurableAgentStateHistoryBinding.DurableStateOwner &&
            !string.Equals(
                parsed.ProviderKey,
                DurableStateProviderKey,
                StringComparison.Ordinal))
        {
            throw new DurableAgentHistoryBindingMismatchException(
                $"The C# entity-owned history profile must use provider key '{DurableStateProviderKey}', " +
                $"not '{parsed.ProviderKey}'. Restore a supported profile or start a new durable session.");
        }
    }

    public static void ValidateExisting(
        DurableAgentStateHistoryBinding? existing,
        DurableAgentStateHistoryBinding expected,
        bool remoteTransitionDetectedAfterExecution = false)
    {
        if (existing is null)
        {
            return;
        }

        if (existing.Version == expected.Version &&
            string.Equals(existing.OwnerKind, expected.OwnerKind, StringComparison.Ordinal) &&
            string.Equals(existing.ProviderKey, expected.ProviderKey, StringComparison.Ordinal))
        {
            return;
        }

        throw new DurableAgentHistoryBindingMismatchException(
            $"Durable session history is fixed to '{existing.OwnerKind}/{existing.ProviderKey}' " +
            $"but the configured runtime resolved '{expected.OwnerKind}/{expected.ProviderKey}'. " +
            "Restore the original logical provider configuration or start a new durable session." +
            GetRemoteTransitionSuffix(remoteTransitionDetectedAfterExecution));
    }

    public static void ValidateLegacyAdoption(
        DurableAgentState legacyState,
        DurableAgentHistoryOwnership ownership,
        AgentSession restoredSession,
        ChatClientAgent? chatClientAgent,
        bool requiresPerServiceCallPersistence)
    {
        if (!HasPriorContinuity(legacyState) &&
            !HasInMemoryProviderMessages(restoredSession, chatClientAgent))
        {
            return;
        }

        if (ownership == DurableAgentHistoryOwnership.Entity)
        {
            if (!HasInMemoryProviderMessages(restoredSession, chatClientAgent))
            {
                return;
            }

            throw new DurableAgentHistoryBindingMismatchException(
                "The unsealed durable session contains in-memory provider history that is not proven " +
                "equivalent to conversationHistory. Automatic transcript merging is not supported; " +
                "restore an entity-owned durable transcript or start a new durable session.");
        }

        bool continuityProven = ownership switch
        {
            DurableAgentHistoryOwnership.Service =>
                !requiresPerServiceCallPersistence &&
                restoredSession is ChatClientAgentSession serviceSession &&
                IsRealServiceConversationId(serviceSession.ConversationId),
            DurableAgentHistoryOwnership.ExternalProvider =>
                HasDeclaredProviderContinuation(restoredSession, chatClientAgent?.ChatHistoryProvider),
            _ => false,
        };
        if (continuityProven)
        {
            return;
        }

        throw new DurableAgentHistoryBindingMismatchException(
            "Legacy durable state has conversation evidence but no owner-specific continuation that proves " +
            $"the configured {ownership} owner is the same logical history store. Restore the prior " +
            "configuration or start a new durable session; automatic history migration is not supported.");
    }

    public static void ValidateLegacyTransition(
        DurableAgentState legacyState,
        DurableAgentHistoryOwnership initialOwnership,
        DurableAgentHistoryOwnership finalOwnership,
        bool remoteTransitionDetectedAfterExecution)
    {
        if (initialOwnership == finalOwnership ||
            !HasPriorContinuity(legacyState))
        {
            return;
        }

        throw new DurableAgentHistoryBindingMismatchException(
            $"Legacy durable state was initially resolved as '{initialOwnership}' but the completed call " +
            $"resolved '{finalOwnership}'. Automatic history-owner switching or migration is not supported." +
            GetRemoteTransitionSuffix(remoteTransitionDetectedAfterExecution));
    }

    public static void ValidateContinuationPresence(
        DurableAgentStateHistoryBinding? existing,
        System.Text.Json.JsonElement? serializedSession)
    {
        if (existing is null ||
            existing.OwnerKind == DurableAgentStateHistoryBinding.DurableStateOwner ||
            serializedSession is not null)
        {
            return;
        }

        throw new DurableAgentHistoryBindingMismatchException(
            $"Durable session history is fixed to '{existing.OwnerKind}/{existing.ProviderKey}', " +
            "but its serialized provider or agent continuation is missing. The runtime will not create " +
            "a replacement logical conversation; restore the continuation or start a new durable session.");
    }

    public static void ValidateConfiguredKey(
        DurableAgentStateHistoryBinding? existing,
        string? configuredProviderKey)
    {
        if (existing is null ||
            existing.OwnerKind == DurableAgentStateHistoryBinding.DurableStateOwner)
        {
            return;
        }

        if (string.Equals(
            existing.ProviderKey,
            configuredProviderKey,
            StringComparison.Ordinal))
        {
            return;
        }

        string configuredDescription = string.IsNullOrWhiteSpace(configuredProviderKey)
            ? "no logical provider key"
            : $"logical provider key '{configuredProviderKey}'";
        throw new DurableAgentHistoryBindingMismatchException(
            $"Durable session history is fixed to '{existing.OwnerKind}/{existing.ProviderKey}' " +
            $"but the current configuration supplies {configuredDescription}. Restore the original " +
            "logical provider configuration or start a new durable session.");
    }

    public static void ValidateBoundContinuation(
        DurableAgentHistoryOwnership ownership,
        AgentSession session,
        ChatClientAgent? chatClientAgent,
        bool remoteTransitionDetectedAfterExecution = false)
    {
        bool valid = ownership switch
        {
            DurableAgentHistoryOwnership.Entity or
            DurableAgentHistoryOwnership.AgentSession => true,
            DurableAgentHistoryOwnership.ExternalProvider =>
                HasDeclaredProviderContinuation(session, chatClientAgent?.ChatHistoryProvider),
            DurableAgentHistoryOwnership.Service =>
                session is ChatClientAgentSession serviceSession &&
                IsRealServiceConversationId(serviceSession.ConversationId),
            _ => false,
        };
        if (valid)
        {
            return;
        }

        throw new DurableAgentHistoryBindingMismatchException(
            $"The restored {ownership} session does not contain the public continuation evidence " +
            "required by its fixed durable history binding. The runtime will not initialize a replacement " +
            "logical conversation; restore the missing continuation or start a new durable session." +
            GetRemoteTransitionSuffix(remoteTransitionDetectedAfterExecution));
    }

    public static void ValidatePreExecutionContinuationContract(
        DurableAgentHistoryOwnership ownership,
        AgentSession session,
        ChatClientAgent? chatClientAgent,
        bool requiresPerServiceCallPersistence)
    {
        if (!requiresPerServiceCallPersistence &&
            session is ChatClientAgentSession chatSession &&
            string.Equals(
                chatSession.ConversationId,
                FrameworkLocalHistoryConversationId,
                StringComparison.Ordinal))
        {
            throw new DurableAgentHistoryBindingMismatchException(
                "The restored session contains Agent Framework's local per-service-call history sentinel, " +
                "but per-service-call persistence is not active. The C# durable runtime cannot safely " +
                "resume or reclassify that conversation; restore the prior configuration or start a new session.");
        }

        if (ownership == DurableAgentHistoryOwnership.ExternalProvider &&
            chatClientAgent?.ChatHistoryProvider?.StateKeys is not { Count: > 0 })
        {
            throw new DurableAgentHistoryBindingMismatchException(
                "An external history provider used by the C# fixed-owner profile must declare at least " +
                "one StateKey so durable continuation can be verified before later model calls.");
        }
    }

    public static DurableAgentState Seal(
        DurableAgentState state,
        DurableAgentStateHistoryBinding binding)
    {
        return new DurableAgentState
        {
            SchemaVersion = state.SchemaVersion,
            Data = new DurableAgentStateData
            {
                ConversationHistory = state.Data.ConversationHistory,
                TerminalResults = state.Data.TerminalResults,
                CompletionReceipts = state.Data.CompletionReceipts,
                HistoryBinding = ToJson(binding),
                Session = state.Data.Session,
                IngestedPositions = state.Data.IngestedPositions,
                Truncation = state.Data.Truncation,
                ExpirationTimeUtc = state.Data.ExpirationTimeUtc,
                ExtensionData = state.Data.ExtensionData,
                UnknownProperties = state.Data.UnknownProperties,
            },
            ExtensionData = state.ExtensionData,
            UnknownProperties = state.UnknownProperties,
        };
    }

    public static DurableAgentStateHistoryBinding MergeProvisionalMetadata(
        DurableAgentStateHistoryBinding binding,
        DurableAgentStateHistoryBinding? provisional)
    {
        if (provisional?.UnknownProperties is null)
        {
            return binding;
        }

        Dictionary<string, System.Text.Json.JsonElement> unknown =
            provisional.UnknownProperties.ToDictionary(
                pair => pair.Key,
                pair => pair.Value.Clone(),
                StringComparer.Ordinal);
        foreach ((string key, System.Text.Json.JsonElement value) in
            binding.UnknownProperties ??
            new Dictionary<string, System.Text.Json.JsonElement>())
        {
            unknown[key] = value.Clone();
        }

        return new DurableAgentStateHistoryBinding
        {
            Version = binding.Version,
            OwnerKind = binding.OwnerKind,
            ProviderKey = binding.ProviderKey,
            UnknownProperties = unknown,
        };
    }

    private static string RequireProviderKey(
        DurableAgentHistoryOwnership ownership,
        string? configuredProviderKey,
        bool remoteTransitionDetectedAfterExecution)
    {
        if (string.IsNullOrWhiteSpace(configuredProviderKey))
        {
            throw new DurableAgentHistoryBindingMismatchException(
                $"History ownership '{ownership}' requires an explicit stable logical provider key. " +
                "Configure DurableAgentsOptions.SetHistoryProviderKey before running this durable session." +
                GetRemoteTransitionSuffix(remoteTransitionDetectedAfterExecution));
        }

        return configuredProviderKey;
    }

    private static string GetRemoteTransitionSuffix(bool remoteTransitionDetectedAfterExecution)
    {
        return remoteTransitionDetectedAfterExecution
            ? " The remote service may already have observed the rejected call, but durable state was not committed."
            : string.Empty;
    }

    private static bool HasPriorContinuity(DurableAgentState state)
    {
        return state.Data.ConversationHistory.Count > 0 ||
            state.Data.Session is not null ||
            state.Data.IngestedPositions is not null ||
            state.Data.Truncation is not null;
    }

    internal static System.Text.Json.JsonElement ToJson(
        DurableAgentStateHistoryBinding binding)
    {
        using MemoryStream stream = new();
        using (System.Text.Json.Utf8JsonWriter writer = new(stream))
        {
            writer.WriteStartObject();
            writer.WriteNumber("version", binding.Version);
            writer.WriteString("ownerKind", binding.OwnerKind);
            writer.WriteString("providerKey", binding.ProviderKey);
            if (binding.UnknownProperties is not null)
            {
                foreach ((string key, System.Text.Json.JsonElement value) in binding.UnknownProperties)
                {
                    writer.WritePropertyName(key);
                    value.WriteTo(writer);
                }
            }

            writer.WriteEndObject();
        }

        using System.Text.Json.JsonDocument document =
            System.Text.Json.JsonDocument.Parse(stream.ToArray());
        return document.RootElement.Clone();
    }

    private static bool HasDeclaredProviderContinuation(
        AgentSession session,
        ChatHistoryProvider? provider)
    {
        if (provider?.StateKeys is not { Count: > 0 })
        {
            return false;
        }

        System.Text.Json.JsonElement stateBag = session.StateBag.Serialize();
        return provider.StateKeys.All(
            key => stateBag.TryGetProperty(key, out _));
    }

    private static bool HasInMemoryProviderMessages(
        AgentSession session,
        ChatClientAgent? chatClientAgent)
    {
        if (chatClientAgent?.ChatHistoryProvider is not InMemoryChatHistoryProvider provider)
        {
            return false;
        }

        foreach (string key in provider.StateKeys)
        {
            if (session.StateBag.TryGetValue(
                    key,
                    out InMemoryChatHistoryProvider.State? state) &&
                state?.Messages.Count > 0)
            {
                return true;
            }
        }

        return false;
    }

    internal static bool IsRealServiceConversationId(string? conversationId)
    {
        return !string.IsNullOrWhiteSpace(conversationId) &&
            !string.Equals(
                conversationId,
                FrameworkLocalHistoryConversationId,
                StringComparison.Ordinal);
    }
}
