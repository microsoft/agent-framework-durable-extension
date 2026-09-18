// Copyright (c) Microsoft. All rights reserved.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>Provides JSON serialization utilities and source-generated contracts for Durable Agent types.</summary>
/// <remarks>
/// <para>
/// This mirrors the pattern used by other libraries (e.g. <c>WorkflowsJsonUtilities</c>) to enable Native AOT and trimming
/// friendly serialization without relying on runtime reflection. It establishes a singleton <see cref="JsonSerializerOptions"/>
/// instance that is preconfigured with:
/// </para>
/// <list type="number">
/// <item><description><see cref="JsonSerializerDefaults.Web"/> baseline defaults.</description></item>
/// <item><description><see cref="JsonIgnoreCondition.WhenWritingNull"/> for default null-value suppression.</description></item>
/// <item><description><see cref="JsonNumberHandling.AllowReadingFromString"/> to tolerate numbers encoded as strings.</description></item>
/// <item><description>Chained type info resolvers from shared agent abstractions to cover cross-package types (e.g. <see cref="ChatMessage"/>, <see cref="AgentResponse"/>).</description></item>
/// </list>
/// <para>
/// Keep the list of <c>[JsonSerializable]</c> types in sync with the Durable Agent data model anytime new state or request/response
/// containers are introduced that must round-trip via JSON.
/// </para>
/// </remarks>
internal static partial class DurableAgentJsonUtilities
{
    /// <summary>
    /// Associates each materialized <see cref="AgentResponse"/> instance with its canonical durable result JSON.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Durable state stores the exact terminal response that was committed for a correlation ID. The public
    /// <see cref="AgentResponse"/> created from that state is a convenient SDK projection, but it cannot represent every
    /// detail in the persisted contract. For example, projection can discard unknown fields written by a newer version
    /// and cannot preserve the difference between an absent <c>value</c> property and an explicit JSON null.
    /// </para>
    /// <para>
    /// Those details matter when a completed request is delivered back to an orchestration, especially when a duplicate
    /// request returns the previously committed outcome instead of running the model again. Returning JSON reconstructed
    /// from the lossy SDK projection would mean the caller did not receive the same outcome that the entity committed.
    /// </para>
    /// <para>
    /// <see cref="AgentResponse"/> has no framework-owned property for this lossless snapshot. Storing it in response
    /// additional properties would also expose an internal transport detail and change the response's normal serialized
    /// shape for every serializer. This sidecar table solves both problems by associating the canonical JSON with the
    /// exact response object without modifying that object.
    /// </para>
    /// <para>
    /// A <see cref="ConditionalWeakTable{TKey,TValue}"/> is used instead of a dictionary so the sidecar does not become
    /// a memory leak. The table does not keep its response keys alive; when application code releases a response, its
    /// retained snapshot becomes collectible as well.
    /// </para>
    /// <para>
    /// This table is process-local, not durable storage. When Durable Task serializes a response from an entity and later
    /// creates a different response object in an orchestration, <see cref="DurableDataConverter"/> writes the canonical
    /// snapshot into a private transport envelope and restores this association on the new object.
    /// </para>
    /// </remarks>
    private static readonly ConditionalWeakTable<AgentResponse, RetainedResultHolder>
        s_retainedResultsByResponse = new();

    /// <summary>
    /// Gets the singleton <see cref="JsonSerializerOptions"/> used for Durable Agent serialization.
    /// </summary>
    public static JsonSerializerOptions DefaultOptions { get; } = CreateDefaultOptions();

    /// <summary>
    /// Gets the canonical retained terminal-response JSON associated with a durable delivery response.
    /// </summary>
    /// <remarks>
    /// The snapshot is the exact durable contract that accompanied this response, not JSON regenerated from the
    /// response's text or other projected properties. It therefore preserves absent versus explicit-null <c>value</c>,
    /// opaque content, and unknown metadata. A response created outside durable delivery has no such association and
    /// returns null rather than fabricating a result that was never committed.
    /// </remarks>
    /// <param name="response">The response returned by durable polling or its proxy.</param>
    /// <returns>The immutable retained JSON, or null for a response not produced by durable delivery.</returns>
    internal static JsonElement? GetRetainedResult(AgentResponse response)
    {
        ArgumentNullException.ThrowIfNull(response);
        return s_retainedResultsByResponse.TryGetValue(response, out RetainedResultHolder? retained)
            ? retained.Value
            : null;
    }

    /// <summary>
    /// Serializes a persisted terminal-response DTO into the canonical JSON associated with a runtime response.
    /// </summary>
    /// <remarks>
    /// This is used while resolving a committed entity outcome into an SDK response. Serializing the persisted DTO
    /// directly, before it is reduced to the SDK projection, preserves unknown future metadata and the distinction
    /// between an absent value and JSON null.
    /// </remarks>
    internal static void CaptureRetainedResult(
        AgentResponse response,
        DurableAgentStateTerminalResponse terminalResponse)
    {
        JsonElement snapshot = JsonSerializer.SerializeToElement(
            terminalResponse, DurableAgentStateJsonContext.Default.DurableAgentStateTerminalResponse);
        CaptureRetainedResult(response, snapshot);
    }

    /// <summary>
    /// Associates an already-canonical JSON snapshot with one response instance.
    /// </summary>
    /// <remarks>
    /// The element is cloned because callers may supply a value backed by a disposable <see cref="JsonDocument"/>. The
    /// clone gives the response association independent ownership, so reading <c>GetDurableResult()</c> remains valid
    /// after the source document has been disposed.
    ///
    /// <see cref="ConditionalWeakTable{TKey,TValue}.Add"/> intentionally rejects a second snapshot for the same
    /// response instance; silently replacing it could make one response object represent two different committed
    /// outcomes.
    /// </remarks>
    internal static void CaptureRetainedResult(AgentResponse response, JsonElement snapshot) =>
        s_retainedResultsByResponse.Add(response, new RetainedResultHolder(snapshot.Clone()));

    /// <summary>
    /// Converts a legacy transcript response into the canonical terminal-response shape and associates it with the
    /// projected runtime response.
    /// </summary>
    /// <remarks>
    /// Older persisted state predates the dedicated terminal-response contract. This conversion preserves the legacy
    /// fields that are available so old completed requests can participate in the same delivery path without pretending
    /// that information absent from the old schema was present.
    /// </remarks>
    internal static void CaptureRetainedLegacyResult(AgentResponse response, DurableAgentStateResponse source) =>
        CaptureRetainedResult(response, new DurableAgentStateTerminalResponse
        {
            Messages = source.Messages,
            Usage = source.Usage,
            CreatedAt = source.CreatedAt,
            AdditionalProperties = source.ExtensionData,
            UnknownProperties = source.UnknownProperties,
        });

    /// <summary>
    /// Copies the canonical durable association when code creates a different response object for the same outcome.
    /// </summary>
    /// <remarks>
    /// Associations are keyed by object identity, so creating an <see cref="AgentResponse{T}"/> wrapper or another
    /// response instance does not inherit the source association automatically. Without this copy, the typed API would
    /// return the correct visible response but <c>GetDurableResult()</c> would unexpectedly become null.
    /// </remarks>
    internal static void CopyRetainedResult(AgentResponse source, AgentResponse target)
    {
        if (GetRetainedResult(source) is JsonElement result)
        {
            CaptureRetainedResult(target, result);
        }
    }

    /// <summary>
    /// Reference-type holder required because <see cref="ConditionalWeakTable{TKey,TValue}"/> values must be classes.
    /// </summary>
    private sealed class RetainedResultHolder(JsonElement value)
    {
        /// <summary>
        /// Gets the immutable, independently owned canonical result JSON.
        /// </summary>
        public JsonElement Value { get; } = value;
    }

    /// <summary>
    /// Serializes a sequence of chat messages using the durable agent default options.
    /// </summary>
    /// <param name="messages">The messages to serialize.</param>
    /// <returns>A <see cref="JsonElement"/> representing the serialized messages.</returns>
    public static JsonElement Serialize(this IEnumerable<ChatMessage> messages) =>
        JsonSerializer.SerializeToElement(messages, DefaultOptions.GetTypeInfo(typeof(IEnumerable<ChatMessage>)));

    /// <summary>
    /// Deserializes chat messages from a <see cref="JsonElement"/> using durable agent options.
    /// </summary>
    /// <param name="element">The JSON element containing the messages.</param>
    /// <returns>The deserialized list of chat messages.</returns>
    public static List<ChatMessage> DeserializeMessages(this JsonElement element) =>
        (List<ChatMessage>?)element.Deserialize(DefaultOptions.GetTypeInfo(typeof(List<ChatMessage>))) ?? [];

    /// <summary>
    /// Creates the configured <see cref="JsonSerializerOptions"/> instance for durable agents.
    /// </summary>
    /// <returns>The configured options.</returns>
    [UnconditionalSuppressMessage("ReflectionAnalysis", "IL3050:RequiresDynamicCode", Justification = "Converter is guarded by IsReflectionEnabledByDefault check.")]
    [UnconditionalSuppressMessage("Trimming", "IL2026:Members annotated with 'RequiresUnreferencedCodeAttribute' require dynamic access", Justification = "Converter is guarded by IsReflectionEnabledByDefault check.")]
    private static JsonSerializerOptions CreateDefaultOptions()
    {
        // Base configuration from the source-generated context below.
        JsonSerializerOptions options = new(JsonContext.Default.Options)
        {
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping, // same as AgentAbstractionsJsonUtilities and AIJsonUtilities
        };

        // Chain in shared abstractions resolver (Microsoft.Extensions.AI + Agent abstractions) so dependent types are covered.
        options.TypeInfoResolverChain.Clear();
        options.TypeInfoResolverChain.Add(AgentAbstractionsJsonUtilities.DefaultOptions.TypeInfoResolver!);
        options.TypeInfoResolverChain.Add(JsonContext.Default.Options.TypeInfoResolver!);

        if (JsonSerializer.IsReflectionEnabledByDefault)
        {
            options.Converters.Add(new JsonStringEnumConverter());
        }

        options.MakeReadOnly();
        return options;
    }

    // Keep in sync with CreateDefaultOptions above.
    [JsonSourceGenerationOptions(JsonSerializerDefaults.Web,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        NumberHandling = JsonNumberHandling.AllowReadingFromString)]

    // Durable Agent State Types
    [JsonSerializable(typeof(DurableAgentState))]
    [JsonSerializable(typeof(DurableAgentSession))]

    // Request Types
    [JsonSerializable(typeof(RunRequest))]
    [JsonSerializable(typeof(AgentEntityDeletionCheck))]
    [JsonSerializable(typeof(AgentEntityResultExpirationCheck))]
    [JsonSerializable(typeof(DurableAgentFailureData))]

    // Primitive / Supporting Types
    [JsonSerializable(typeof(ChatMessage))]
    [JsonSerializable(typeof(JsonElement))]

    [ExcludeFromCodeCoverage]
    internal sealed partial class JsonContext : JsonSerializerContext;
}
