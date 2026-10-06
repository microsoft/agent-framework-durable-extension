// Copyright (c) Microsoft. All rights reserved.

using System.Runtime.CompilerServices;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Releases an operation-scoped durable history override after the model service supplies conversation ownership.
/// </summary>
internal sealed class DurableChatHistoryProviderChatClient(
    IChatClient innerClient,
    DurableChatHistoryProvider historyProvider) : DelegatingChatClient(innerClient)
{
    public override async Task<ChatResponse> GetResponseAsync(
        IEnumerable<ChatMessage> messages,
        ChatOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ChatResponse response = await base.GetResponseAsync(messages, options, cancellationToken).ConfigureAwait(false);
        this.ReleaseOverrideForServiceResponse(options, response.ConversationId);
        return response;
    }

    public override async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
        IEnumerable<ChatMessage> messages,
        ChatOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        List<ChatResponseUpdate> updates = [];
        await foreach (ChatResponseUpdate update in base.GetStreamingResponseAsync(
            messages, options, cancellationToken).ConfigureAwait(false))
        {
            updates.Add(update);
            yield return update;
        }

        // Use the SDK's own aggregation rules, and release only after the complete stream succeeds.
        this.ReleaseOverrideForServiceResponse(options, updates.ToChatResponse().ConversationId);
    }

    private void ReleaseOverrideForServiceResponse(ChatOptions? options, string? conversationId)
    {
        if (DurableAgentHistoryBinding.IsRealServiceConversationId(conversationId) &&
            options?.AdditionalProperties?.TryGetValue(out ChatHistoryProvider? provider) is true &&
            ReferenceEquals(provider, historyProvider))
        {
            // The SDK resolves history again after setting the session's service conversation ID.
            // The durable override has already supplied model input; retaining it now conflicts with
            // service ownership. AgentEntity still validates the fixed binding before committing.
            options.AdditionalProperties.Remove<ChatHistoryProvider>();
        }
    }
}
