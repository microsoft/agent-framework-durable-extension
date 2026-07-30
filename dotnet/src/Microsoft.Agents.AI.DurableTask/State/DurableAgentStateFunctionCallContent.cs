// Copyright (c) Microsoft. All rights reserved.

using System.Collections.Immutable;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask.State;

/// <summary>
/// Durable agent state content representing a function call.
/// </summary>
internal sealed class DurableAgentStateFunctionCallContent : DurableAgentStateContent
{
    /// <summary>
    /// The function call arguments, each encoded as JSON.
    /// </summary>
    /// <remarks>
    /// Arguments produced by a chat client from a model response are already <see cref="JsonElement"/>
    /// values, but callers can supply <see cref="FunctionCallContent"/> containing arbitrary objects (for
    /// example when replaying history or resuming an approval). Those are encoded here using
    /// <see cref="AIJsonUtilities.DefaultOptions"/> so that persisting the state cannot fail on a type the
    /// state serializer has no metadata for.
    /// </remarks>
    /// TODO: Consider ensuring that empty dictionaries are omitted from serialization.
    [JsonPropertyName("arguments")]
    public required IReadOnlyDictionary<string, JsonElement> Arguments { get; init; } =
        ImmutableDictionary<string, JsonElement>.Empty;

    /// <summary>
    /// Gets the function call identifier.
    /// </summary>
    /// <remarks>
    /// This is used to correlate this function call with its resulting
    /// <see cref="DurableAgentStateFunctionResultContent"/>.
    /// </remarks>
    [JsonPropertyName("callId")]
    public required string CallId { get; init; }

    /// <summary>
    /// Gets the function name.
    /// </summary>
    [JsonPropertyName("name")]
    public required string Name { get; init; }

    /// <summary>
    /// Creates a <see cref="DurableAgentStateFunctionCallContent"/> from a <see cref="FunctionCallContent"/>.
    /// </summary>
    /// <param name="content">The <see cref="FunctionCallContent"/> to convert.</param>
    /// <returns>
    /// A <see cref="DurableAgentStateFunctionCallContent"/> representing the original content.
    /// </returns>
    public static DurableAgentStateFunctionCallContent FromFunctionCallContent(FunctionCallContent content)
    {
        Dictionary<string, JsonElement> arguments = [];
        if (content.Arguments is not null)
        {
            foreach (KeyValuePair<string, object?> argument in content.Arguments)
            {
                arguments[argument.Key] = ToJsonElement(argument.Value);
            }
        }

        return new DurableAgentStateFunctionCallContent()
        {
            Arguments = arguments,
            CallId = content.CallId,
            Name = content.Name
        };
    }

    /// <inheritdoc/>
    public override AIContent ToAIContent()
    {
        Dictionary<string, object?> arguments = new(this.Arguments.Count);
        foreach (KeyValuePair<string, JsonElement> argument in this.Arguments)
        {
            arguments[argument.Key] = argument.Value;
        }

        return new FunctionCallContent(this.CallId, this.Name, arguments);
    }
}
