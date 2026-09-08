// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Logging;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Provides chat history from the entity's working state for one durable agent invocation.
/// </summary>
/// <remarks>
/// This provider is a state adapter, not a persistence backend. It stages changes in the entity
/// operation's working <see cref="DurableAgentStateEntry"/> list; <see cref="AgentEntity"/> commits
/// that aggregate state once after response finalization, session serialization, and TTL
/// processing have all succeeded.
/// </remarks>
internal sealed class DurableChatHistoryProvider(
    IList<DurableAgentStateEntry> history,
    RunRequest request,
    bool allowLosslessV2 = false,
    ILogger? logger = null) : ChatHistoryProvider
{
    private readonly IList<DurableAgentStateEntry> _history = history;
    private readonly RunRequest _request = request;
    private readonly ILogger? _logger = logger;
    private int _responseIndex = -1;

    /// <inheritdoc />
    public override IReadOnlyList<string> StateKeys => [];

    /// <summary>
    /// Gets a value indicating whether this provider staged the current turn in the working state.
    /// </summary>
    public bool HasStagedTurn => this._responseIndex >= 0;

    /// <inheritdoc />
    protected override ValueTask<IEnumerable<ChatMessage>> ProvideChatHistoryAsync(
        InvokingContext context,
        CancellationToken cancellationToken = default)
    {
        DurableAgentStateMessageIdentity.EnsureMessageIds(this._history);
        return new(DurableAgentStateReplay.GetMessages(
            this._history,
            this._request.CorrelationId));
    }

    /// <inheritdoc />
    protected override ValueTask StoreChatHistoryAsync(
        InvokedContext context,
        CancellationToken cancellationToken = default)
    {
        if (context.Session is ChatClientAgentSession chatSession &&
            DurableAgentHistoryBinding.IsRealServiceConversationId(chatSession.ConversationId))
        {
            return default;
        }

        // ChatHistoryProvider calls this "store", but the list is the entity operation's isolated
        // working state. Do not write the Durable Task backend here: doing so would persist an
        // intermediate turn before aggregate response metadata, session state, and TTL.
        this._history.Add(DurableAgentStateRequest.FromRunRequest(
            this._request,
            this._request.Messages,
            allowLosslessV2,
            this._logger));
        this._history.Add(
            allowLosslessV2
                ? DurableAgentStateResponse.FromMessagesV2(
                    this._request.CorrelationId,
                    context.ResponseMessages ?? [],
                    this._logger)
                : DurableAgentStateResponse.FromMessages(
                    this._request.CorrelationId,
                    context.ResponseMessages ?? [],
                    this._logger));
        this._responseIndex = this._history.Count - 1;
        return default;
    }

    /// <summary>
    /// Replaces the staged response with the complete response, including usage metadata.
    /// </summary>
    public void CompleteStagedResponse(AgentResponse response)
    {
        if (this._responseIndex >= 0)
        {
            this._history[this._responseIndex] =
                allowLosslessV2
                    ? DurableAgentStateResponse.FromResponseV2(
                        this._request.CorrelationId,
                        response,
                        this._logger)
                    : DurableAgentStateResponse.FromResponse(
                        this._request.CorrelationId,
                        response,
                        this._logger);
        }
    }
}
