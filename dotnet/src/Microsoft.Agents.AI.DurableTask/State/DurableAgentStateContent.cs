// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Base class for durable agent state content types.
/// </summary>
[JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")]
[JsonDerivedType(typeof(DurableAgentStateDataContent), "data")]
[JsonDerivedType(typeof(DurableAgentStateErrorContent), "error")]
[JsonDerivedType(typeof(DurableAgentStateFunctionCallContent), "functionCall")]
[JsonDerivedType(typeof(DurableAgentStateFunctionResultContent), "functionResult")]
[JsonDerivedType(typeof(DurableAgentStateHostedFileContent), "hostedFile")]
[JsonDerivedType(typeof(DurableAgentStateHostedVectorStoreContent), "hostedVectorStore")]
[JsonDerivedType(typeof(DurableAgentStateTextContent), "text")]
[JsonDerivedType(typeof(DurableAgentStateTextReasoningContent), "reasoning")]
[JsonDerivedType(typeof(DurableAgentStateUriContent), "uri")]
[JsonDerivedType(typeof(DurableAgentStateUsageContent), "usage")]
[JsonDerivedType(typeof(DurableAgentStateUnknownContent), "unknown")]
internal abstract class DurableAgentStateContent
{
    private static readonly JsonElement s_nullElement = JsonSerializer.SerializeToElement(
        value: null,
        jsonTypeInfo: AIJsonUtilities.DefaultOptions.GetTypeInfo(typeof(object)));

    /// <summary>
    /// Gets any additional data found during deserialization that does not map to known properties.
    /// </summary>
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? ExtensionData { get; set; }

    /// <summary>
    /// Converts this durable agent state content to an <see cref="AIContent"/>.
    /// </summary>
    /// <returns>A converted <see cref="AIContent"/> instance.</returns>
    public abstract AIContent ToAIContent();

    /// <summary>
    /// Creates a <see cref="DurableAgentStateContent"/> from an <see cref="AIContent"/>.
    /// </summary>
    /// <param name="content">The <see cref="AIContent"/> to convert.</param>
    /// <returns>A <see cref="DurableAgentStateContent"/> representing the original <see cref="AIContent"/>.</returns>
    public static DurableAgentStateContent FromAIContent(AIContent content)
    {
        return content switch
        {
            DataContent dataContent => DurableAgentStateDataContent.FromDataContent(dataContent),
            ErrorContent errorContent => DurableAgentStateErrorContent.FromErrorContent(errorContent),
            FunctionCallContent functionCallContent => DurableAgentStateFunctionCallContent.FromFunctionCallContent(functionCallContent),
            FunctionResultContent functionResultContent => DurableAgentStateFunctionResultContent.FromFunctionResultContent(functionResultContent),
            HostedFileContent hostedFileContent => DurableAgentStateHostedFileContent.FromHostedFileContent(hostedFileContent),
            HostedVectorStoreContent hostedVectorStoreContent => DurableAgentStateHostedVectorStoreContent.FromHostedVectorStoreContent(hostedVectorStoreContent),
            TextContent textContent => DurableAgentStateTextContent.FromTextContent(textContent),
            TextReasoningContent textReasoningContent => DurableAgentStateTextReasoningContent.FromTextReasoningContent(textReasoningContent),
            UriContent uriContent => DurableAgentStateUriContent.FromUriContent(uriContent),
            UsageContent usageContent => DurableAgentStateUsageContent.FromUsageContent(usageContent),
            _ => DurableAgentStateUnknownContent.FromUnknownContent(content)
        };
    }

    /// <summary>
    /// Encodes a loosely typed value as a <see cref="JsonElement"/> so that it can be persisted.
    /// </summary>
    /// <param name="value">
    /// The value to encode. Values that are already a <see cref="JsonElement"/> are returned unchanged.
    /// </param>
    /// <returns>The encoded value.</returns>
    /// <remarks>
    /// <see cref="DurableAgentStateJsonContext"/> is source generated and has no reflection fallback, so
    /// <see cref="object"/> typed members must be reduced to JSON before the state is written. Otherwise
    /// serialization throws for any runtime type the context was not generated for, which fails the entity
    /// operation after the model call has already happened.
    /// See https://github.com/microsoft/agent-framework-durable-extension/issues/33.
    /// </remarks>
    protected static JsonElement ToJsonElement(object? value)
    {
        return value switch
        {
            null => s_nullElement,
            JsonElement element => element,
            _ => JsonSerializer.SerializeToElement(
                value: value,
                jsonTypeInfo: AIJsonUtilities.DefaultOptions.GetTypeInfo(value.GetType()))
        };
    }
}
