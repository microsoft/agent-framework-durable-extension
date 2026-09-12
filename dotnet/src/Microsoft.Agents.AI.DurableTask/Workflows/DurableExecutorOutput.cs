// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;

namespace Microsoft.Agents.AI.DurableTask.Workflows;

/// <summary>
/// Output payload from executor execution, containing the result, state updates, and emitted events.
/// </summary>
internal sealed class DurableExecutorOutput
{
    private static readonly string[] s_outputProperties =
        ["result", "stateUpdates", "clearedScopes", "events", "sentMessages", "haltRequested"];

    private static readonly string[] s_messageProperties = ["typeName", "data"];

    /// <summary>
    /// Gets the executor result.
    /// </summary>
    public string? Result { get; init; }

    /// <summary>
    /// Gets the state updates (scope-prefixed key to value; null indicates deletion).
    /// </summary>
    public Dictionary<string, string?> StateUpdates { get; init; } = [];

    /// <summary>
    /// Gets the scope names that were cleared.
    /// </summary>
    public List<string> ClearedScopes { get; init; } = [];

    /// <summary>
    /// Gets the workflow events emitted during execution.
    /// </summary>
    public List<string> Events { get; init; } = [];

    /// <summary>
    /// Gets the typed messages sent to downstream executors.
    /// </summary>
    public List<TypedPayload> SentMessages { get; init; } = [];

    /// <summary>
    /// Gets a value indicating whether the executor requested a workflow halt.
    /// </summary>
    public bool HaltRequested { get; init; }

    /// <summary>
    /// Reads controls only from a trusted activity response. Legacy text and invalid
    /// envelopes remain opaque results; no controls from a partially valid envelope are applied.
    /// </summary>
    internal static DurableExecutorOutput FromActivityResult(string rawResult)
    {
        if (!string.IsNullOrEmpty(rawResult))
        {
            try
            {
                using JsonDocument document = JsonDocument.Parse(rawResult);
                if (HasUnambiguousProperties(document.RootElement, s_outputProperties, out HashSet<string> presentProperties))
                {
                    DurableExecutorOutput? output = document.RootElement.Deserialize(
                        DurableWorkflowJsonContext.Default.DurableExecutorOutput);

                    if (output is not null && HasValidCollections(output, presentProperties) && HasMeaningfulContent(output) &&
                        (output.SentMessages is null || HasValidTypedMessages(output.SentMessages)))
                    {
                        bool validMessages = true;
                        foreach (JsonProperty property in document.RootElement.EnumerateObject())
                        {
                            if (property.Name.Equals("sentMessages", StringComparison.OrdinalIgnoreCase))
                            {
                                validMessages = property.Value.EnumerateArray().All(HasValidTypedMessage);
                            }
                        }

                        if (validMessages)
                        {
                            // Source-generated deserialization overwrites omitted init-only collections with null.
                            // Explicit null properties were rejected above; only missing collections use defaults.
                            return new DurableExecutorOutput
                            {
                                Result = output.Result,
                                StateUpdates = output.StateUpdates ?? [],
                                ClearedScopes = output.ClearedScopes ?? [],
                                Events = output.Events ?? [],
                                SentMessages = output.SentMessages ?? [],
                                HaltRequested = output.HaltRequested,
                            };
                        }
                    }
                }
            }
            catch (JsonException)
            {
                // An invalid known field rejects the entire envelope, including otherwise valid controls.
            }
        }

        return new DurableExecutorOutput { Result = rawResult };
    }

    private static bool HasUnambiguousProperties(
        JsonElement element,
        string[] knownProperties,
        out HashSet<string> presentProperties)
    {
        presentProperties = new(StringComparer.OrdinalIgnoreCase);
        if (element.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        foreach (JsonProperty property in element.EnumerateObject())
        {
            if (knownProperties.Contains(property.Name, StringComparer.OrdinalIgnoreCase) && !presentProperties.Add(property.Name))
            {
                return false;
            }
        }

        return true;
    }

    private static bool HasValidTypedMessage(JsonElement message)
    {
        return HasUnambiguousProperties(message, s_messageProperties, out HashSet<string> presentProperties) &&
            presentProperties.Contains(nameof(TypedPayload.TypeName)) &&
            presentProperties.Contains(nameof(TypedPayload.Data));
    }

    /// <summary>
    /// Validates the entire typed collection before activity or sub-workflow controls are accepted.
    /// </summary>
    internal static bool HasValidTypedMessages(List<TypedPayload> messages)
    {
        // Both fields are CLR strings. JSON null/false/0/"" payloads are serialized *inside*
        // Data, not supplied as null/scalar envelope fields. Do not parse or reinterpret that text.
        return messages.All(message => message is not null &&
            !string.IsNullOrWhiteSpace(message.TypeName) && !string.IsNullOrWhiteSpace(message.Data));
    }

    private static bool HasValidCollections(DurableExecutorOutput output, HashSet<string> presentProperties)
    {
        return (output.StateUpdates is not null || !presentProperties.Contains(nameof(StateUpdates)))
            && IsValidList(output.ClearedScopes, presentProperties, nameof(ClearedScopes))
            && IsValidList(output.Events, presentProperties, nameof(Events))
            && IsValidList(output.SentMessages, presentProperties, nameof(SentMessages));
    }

    private static bool IsValidList<T>(List<T>? values, HashSet<string> presentProperties, string propertyName)
        where T : class
    {
        return values is null
            ? !presentProperties.Contains(propertyName)
            : values.All(value => value is not null);
    }

    private static bool HasMeaningfulContent(DurableExecutorOutput output)
    {
        return output.Result is not null
            || output.SentMessages?.Count > 0
            || output.Events?.Count > 0
            || output.StateUpdates?.Count > 0
            || output.ClearedScopes?.Count > 0
            || output.HaltRequested;
    }
}
