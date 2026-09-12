// Copyright (c) Microsoft. All rights reserved.

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
    /// Gets the original function-call arguments as an object or verbatim string.
    /// </summary>
    /// <remarks>
    /// String form is preserved without parsing or normalization, including incomplete or non-JSON text.
    /// </remarks>
    [JsonPropertyName("arguments")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public JsonElement Arguments { get; init; }

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
    /// <param name="allowLosslessV2">Whether v2-only verbatim string arguments may be persisted.</param>
    /// <returns>
    /// A <see cref="DurableAgentStateFunctionCallContent"/> representing the original content.
    /// </returns>
    public static DurableAgentStateFunctionCallContent FromFunctionCallContent(
        FunctionCallContent content,
        bool allowLosslessV2 = false)
    {
        JsonElement arguments = default;
        if (allowLosslessV2 && content.RawRepresentation is string encodedArguments)
        {
            arguments = JsonSerializer.SerializeToElement(
                encodedArguments,
                DurableAgentStateJsonContext.Default.String);
        }
        else if (content.Arguments is not null)
        {
            Dictionary<string, JsonElement> argumentValues = [];
            foreach (KeyValuePair<string, object?> argument in content.Arguments)
            {
                argumentValues[argument.Key] = ToJsonElement(argument.Value);
            }

            arguments = JsonSerializer.SerializeToElement(
                argumentValues,
                DurableAgentStateJsonContext.Default.DictionaryStringJsonElement);
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
        if (this.Arguments.ValueKind == JsonValueKind.String)
        {
            string encodedArguments = this.Arguments.GetString()!;
            return new FunctionCallContent(this.CallId, this.Name)
            {
                RawRepresentation = encodedArguments,
            };
        }

        Dictionary<string, object?>? arguments =
            this.Arguments.ValueKind == JsonValueKind.Undefined ? [] : null;
        if (this.Arguments.ValueKind == JsonValueKind.Object)
        {
            arguments = [];
            foreach (JsonProperty argument in this.Arguments.EnumerateObject())
            {
                arguments[argument.Name] = argument.Value.Clone();
            }
        }

        return new FunctionCallContent(this.CallId, this.Name, arguments);
    }

    /// <inheritdoc/>
    public override void ValidateV2()
    {
        if (this.Arguments.ValueKind is not JsonValueKind.Undefined and
            not JsonValueKind.Object and
            not JsonValueKind.String)
        {
            throw new InvalidOperationException(
                "Durable agent function-call arguments must be an object, a verbatim string, or absent.");
        }
    }
}
