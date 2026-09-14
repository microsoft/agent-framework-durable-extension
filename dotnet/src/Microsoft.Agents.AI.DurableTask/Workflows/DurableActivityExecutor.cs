// Copyright (c) Microsoft. All rights reserved.

using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Json;
using Microsoft.Agents.AI.Workflows;
using Microsoft.Agents.AI.Workflows.Checkpointing;
using Microsoft.Agents.AI.Workflows.Observability;

namespace Microsoft.Agents.AI.DurableTask.Workflows;

/// <summary>
/// Executes workflow activities by invoking executor bindings and handling serialization.
/// </summary>
[UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "Workflow and executor types are registered at startup.")]
[UnconditionalSuppressMessage("Trimming", "IL2057", Justification = "Workflow and executor types are registered at startup.")]
[UnconditionalSuppressMessage("AOT", "IL3050", Justification = "Workflow and executor types are registered at startup.")]
internal static class DurableActivityExecutor
{
    /// <summary>
    /// Executes an activity using the provided executor binding.
    /// </summary>
    /// <param name="binding">The executor binding to invoke.</param>
    /// <param name="input">The serialized input string.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The serialized activity output.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="binding"/> is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the executor factory is not configured.</exception>
    internal static async Task<string> ExecuteAsync(
        ExecutorBinding binding,
        string input,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(binding);

        if (binding.FactoryAsync is null)
        {
            throw new InvalidOperationException($"Executor binding for '{binding.Id}' does not have a factory configured.");
        }

        DurableActivityInput? inputWithState = TryDeserializeActivityInput(input);
        string executorInput = inputWithState?.Input ?? input;
        Dictionary<string, string> sharedState = inputWithState?.State ?? [];

        Executor executor = await binding.FactoryAsync(binding.Id).ConfigureAwait(false);
        Type inputType = ResolveInputType(inputWithState?.InputTypeName, executor.InputTypes);
        object typedInput = DeserializeInput(executorInput, inputType);

        DurableWorkflowContext workflowContext = new(sharedState, executor);
        object? result = await executor.ExecuteCoreAsync(
            typedInput,
            new TypeId(inputType),
            workflowContext,
            WorkflowTelemetryContext.Disabled,
            cancellationToken).ConfigureAwait(false);

        return SerializeActivityOutput(result, workflowContext);
    }

    private static string SerializeActivityOutput(object? result, DurableWorkflowContext context)
    {
        DurableExecutorOutput output = new()
        {
            Result = SerializeResult(result),
            StateUpdates = context.StateUpdates,
            ClearedScopes = [.. context.ClearedScopes],
            Events = context.OutboundEvents.ConvertAll(SerializeEvent),
            SentMessages = context.SentMessages,
            HaltRequested = context.HaltRequested
        };

        return JsonSerializer.Serialize(output, DurableWorkflowJsonContext.Default.DurableExecutorOutput);
    }

    /// <summary>
    /// Serializes a workflow event with type information for proper deserialization.
    /// </summary>
    private static string SerializeEvent(WorkflowEvent evt)
    {
        Type eventType = evt.GetType();
        TypedPayload wrapper = new()
        {
            TypeName = eventType.AssemblyQualifiedName,
            Data = JsonSerializer.Serialize(evt, eventType, DurableSerialization.Options)
        };

        return JsonSerializer.Serialize(wrapper, DurableWorkflowJsonContext.Default.TypedPayload);
    }

    private static string SerializeResult(object? result)
    {
        if (result is null)
        {
            return string.Empty;
        }

        if (result is string str)
        {
            return str;
        }

        return JsonSerializer.Serialize(result, result.GetType(), DurableSerialization.Options);
    }

    private static DurableActivityInput? TryDeserializeActivityInput(string input)
    {
        bool hasTypeHint = false;
        try
        {
            // A recognized top-level hint identifies an envelope even if another field is
            // malformed. Do not lose that provenance through the legacy raw-input fallback.
            Utf8JsonReader reader = new(Encoding.UTF8.GetBytes(input));
            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.PropertyName &&
                    reader.CurrentDepth == 1 &&
                    string.Equals(reader.GetString(), nameof(DurableActivityInput.InputTypeName), StringComparison.OrdinalIgnoreCase))
                {
                    if (hasTypeHint)
                    {
                        throw new JsonException("Activity input contains duplicate input type hints.");
                    }

                    hasTypeHint = true;
                    if (!reader.Read() || reader.TokenType is not (JsonTokenType.String or JsonTokenType.Null))
                    {
                        throw new JsonException("Activity input type hint must be a string or null.");
                    }
                }
            }

            DurableActivityInput? activityInput = JsonSerializer.Deserialize(
                input,
                DurableWorkflowJsonContext.Default.DurableActivityInput);
            if (hasTypeHint && activityInput?.Input is null)
            {
                throw new JsonException("Activity input is required when an input type hint is present.");
            }

            return activityInput;
        }
        catch (JsonException) when (!hasTypeHint)
        {
            return null;
        }
    }

    internal static object DeserializeInput(string input, Type targetType)
    {
        if (targetType == typeof(string))
        {
            return input;
        }

        // Fan-in aggregation serializes results as a JSON array of strings (e.g., ["{...}", "{...}"]).
        // When the target type is a non-string array, deserialize each element individually.
        if (targetType.IsArray && targetType != typeof(string[]))
        {
            Type elementType = targetType.GetElementType()!;
            string[]? stringArray = JsonSerializer.Deserialize<string[]>(input, DurableSerialization.Options);
            if (stringArray is not null)
            {
                Array result = Array.CreateInstance(elementType, stringArray.Length);
                for (int i = 0; i < stringArray.Length; i++)
                {
                    object element = JsonSerializer.Deserialize(stringArray[i], elementType, DurableSerialization.Options)
                        ?? throw new InvalidOperationException($"Failed to deserialize element {i} to type '{elementType.Name}'.");
                    result.SetValue(element, i);
                }

                return result;
            }
        }

        return JsonSerializer.Deserialize(input, targetType, DurableSerialization.Options)
            ?? throw new InvalidOperationException($"Failed to deserialize input to type '{targetType.Name}'.");
    }

    internal static Type ResolveInputType(string? inputTypeName, ISet<Type> supportedTypes)
    {
        if (string.IsNullOrEmpty(inputTypeName))
        {
            return supportedTypes.FirstOrDefault() ?? typeof(string);
        }

        if (inputTypeName.AsSpan().Trim().Length != inputTypeName.Length)
        {
            throw new InvalidOperationException($"Input type '{inputTypeName}' must not have surrounding whitespace.");
        }

        Type? loadedType = DurableTaskTypeResolver.Resolve(inputTypeName);
        if (loadedType is not null && supportedTypes.Contains(loadedType))
        {
            return loadedType;
        }

        Type? matchedType = supportedTypes.FirstOrDefault(t =>
            t.FullName == inputTypeName ||
            t.Name == inputTypeName);

        if (matchedType is not null)
        {
            return matchedType;
        }

        // Fall back if type is string or string[] but executor doesn't support it
        if (loadedType is not null && !supportedTypes.Contains(loadedType))
        {
            if (loadedType == typeof(string) || loadedType == typeof(string[]))
            {
                return supportedTypes.FirstOrDefault() ?? typeof(string);
            }
        }

        // Resolution is not authorization: reject before scalar or fan-in deserialization
        // can invoke payload constructors or converters for an unregistered type.
        throw new InvalidOperationException($"Input type '{inputTypeName}' is not supported by this executor.");
    }
}
