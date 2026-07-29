// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Represents the function result content for a durable agent state response.
/// </summary>
/// <remarks>
/// At most one of <see cref="Result"/>, <see cref="ResultContent"/>, and <see cref="ResultContents"/> is
/// populated, matching the shape of the value the tool returned. All three are absent when the tool
/// returned nothing.
/// </remarks>
internal sealed class DurableAgentStateFunctionResultContent : DurableAgentStateContent
{
    /// <summary>
    /// Gets the function call identifier.
    /// </summary>
    /// <remarks>
    /// This is used to correlate this function result with its originating
    /// <see cref="DurableAgentStateFunctionCallContent"/>.
    /// </remarks>
    [JsonPropertyName("callId")]
    public required string CallId { get; init; }

    /// <summary>
    /// Gets the function result, encoded as JSON.
    /// </summary>
    /// <remarks>
    /// Tools created via <c>AIFunctionFactory</c> already marshal their return values into a
    /// <see cref="JsonElement"/>. Custom <see cref="AIFunction"/> implementations may return arbitrary
    /// objects, which are serialized here using <see cref="AIJsonUtilities.DefaultOptions"/>.
    /// </remarks>
    [JsonPropertyName("result")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public JsonElement? Result { get; init; }

    /// <summary>
    /// Gets the function result when the tool returned a single <see cref="AIContent"/> value.
    /// </summary>
    /// <remarks>
    /// MCP tools return <see cref="AIContent"/> from their invocation so that downstream chat clients can
    /// specialize handling of the tool output (for example, sending image results back to the model as
    /// multi-modal tool responses). Storing the content in its structured form preserves that behavior
    /// across durable checkpoints.
    /// </remarks>
    [JsonPropertyName("resultContent")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public DurableAgentStateContent? ResultContent { get; init; }

    /// <summary>
    /// Gets the function result when the tool returned a collection of <see cref="AIContent"/> values,
    /// which MCP tools do when a tool result carries more than one content block.
    /// </summary>
    [JsonPropertyName("resultContents")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IReadOnlyList<DurableAgentStateContent>? ResultContents { get; init; }

    /// <summary>
    /// Creates a <see cref="DurableAgentStateFunctionResultContent"/> from a <see cref="FunctionResultContent"/>.
    /// </summary>
    /// <param name="content">The <see cref="FunctionResultContent"/> to convert.</param>
    /// <returns>A <see cref="DurableAgentStateFunctionResultContent"/> representing the original content.</returns>
    public static DurableAgentStateFunctionResultContent FromFunctionResultContent(FunctionResultContent content)
    {
        JsonElement? result = null;
        DurableAgentStateContent? resultContent = null;
        IReadOnlyList<DurableAgentStateContent>? resultContents = null;

        switch (content.Result)
        {
            case null:
                break;

            case JsonElement element:
                result = element;
                break;

            case AIContent aiContent:
                resultContent = FromAIContent(aiContent);
                break;

            case IEnumerable<AIContent> aiContents:
                resultContents = [.. aiContents.Select(FromAIContent)];
                break;

            default:
                result = ToJsonElement(content.Result);
                break;
        }

        return new DurableAgentStateFunctionResultContent()
        {
            CallId = content.CallId,
            Result = result,
            ResultContent = resultContent,
            ResultContents = resultContents
        };
    }

    /// <inheritdoc/>
    public override AIContent ToAIContent()
    {
        object? result;
        if (this.ResultContent is not null)
        {
            result = this.ResultContent.ToAIContent();
        }
        else if (this.ResultContents is not null)
        {
            result = this.ResultContents.Select(content => content.ToAIContent()).ToArray();
        }
        else
        {
            // Boxing a JsonElement? yields either a boxed JsonElement or null, which matches what
            // the tool originally returned.
            result = this.Result;
        }

        return new FunctionResultContent(this.CallId, result);
    }
}
