// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Represents the function result content for a durable agent state response.
/// </summary>
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
    /// Gets the function result, encoded as JSON. Absent when the tool returned nothing.
    /// </summary>
    /// <remarks>
    /// Tools created via <c>AIFunctionFactory</c> already marshal their return values into a
    /// <see cref="JsonElement"/>. Custom <see cref="AIFunction"/> implementations may return arbitrary
    /// objects, and MCP tools return <see cref="AIContent"/> or a collection of it. All of these are
    /// encoded here using <see cref="AIJsonUtilities.DefaultOptions"/> so that every result shape is
    /// persisted under this single property.
    /// </remarks>
    [JsonPropertyName("result")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public JsonElement Result
    {
        get;
        init => field = value.ValueKind == JsonValueKind.Undefined ? default : value.Clone();
    }

    /// <summary>
    /// Creates a <see cref="DurableAgentStateFunctionResultContent"/> from a <see cref="FunctionResultContent"/>.
    /// </summary>
    /// <param name="content">The <see cref="FunctionResultContent"/> to convert.</param>
    /// <returns>A <see cref="DurableAgentStateFunctionResultContent"/> representing the original content.</returns>
    public static DurableAgentStateFunctionResultContent FromFunctionResultContent(FunctionResultContent content)
    {
        return new DurableAgentStateFunctionResultContent()
        {
            CallId = content.CallId,

            // A null result is left absent rather than encoded as a JSON null so that it round trips
            // back to a null FunctionResultContent.Result.
            Result = content.Result is null ? default : ToJsonElement(content.Result)
        };
    }

    /// <inheritdoc/>
    public override AIContent ToAIContent()
    {
        object? result = this.Result.ValueKind == JsonValueKind.Undefined ? null : this.Result;
        return new FunctionResultContent(this.CallId, result);
    }
}
