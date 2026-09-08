// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.AI;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class DurableChatHistoryProviderTests
{
    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public async Task EmptyOrWhitespaceConversationIdStillStagesEntityHistoryAsync(
        string conversationId)
    {
        RecordingChatClient client = new();
        ChatClientAgent chatAgent = new(client, name: "test-agent");
        AgentSession session = await chatAgent.CreateSessionAsync(conversationId);
        DurableAgentState state = new();
        RunRequest request = new("new request") { CorrelationId = "new" };
        DurableChatHistoryProvider provider = new(
            state.Data.ConversationHistory,
            request,
            allowLosslessV2: true);

        await provider.InvokedAsync(
            new ChatHistoryProvider.InvokedContext(
                chatAgent,
                session,
                request.Messages,
                [new ChatMessage(ChatRole.Assistant, "response")]));

        Assert.True(provider.HasStagedTurn);
        Assert.Equal(2, state.Data.ConversationHistory.Count);
    }

    [Fact]
    public async Task FrameworkLocalConversationSentinelStillStagesEntityHistoryAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent chatAgent = new(client, name: "test-agent");
        AgentSession session = await chatAgent.CreateSessionAsync(
            DurableAgentHistoryBinding.FrameworkLocalHistoryConversationId);
        DurableAgentState state = new();
        RunRequest request = new("new request") { CorrelationId = "new" };
        DurableChatHistoryProvider provider = new(
            state.Data.ConversationHistory,
            request,
            allowLosslessV2: true);

        await provider.InvokedAsync(
            new ChatHistoryProvider.InvokedContext(
                chatAgent,
                session,
                request.Messages,
                [new ChatMessage(ChatRole.Assistant, "response")]));

        Assert.True(provider.HasStagedTurn);
        Assert.Equal(2, state.Data.ConversationHistory.Count);
    }

    [Fact]
    public async Task V2ProviderStagesDeveloperRoleResponseLosslesslyAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent chatAgent = new(client, name: "test-agent");
        AgentSession session = await chatAgent.CreateSessionAsync();
        DurableAgentState state = new();
        RunRequest request = new("new request") { CorrelationId = "new" };
        DurableChatHistoryProvider provider = new(
            state.Data.ConversationHistory,
            request,
            allowLosslessV2: true);

        await provider.InvokedAsync(
            new ChatHistoryProvider.InvokedContext(
                chatAgent,
                session,
                request.Messages,
                [new ChatMessage(new ChatRole("developer"), "developer response")]));

        DurableAgentStateResponse response =
            Assert.IsType<DurableAgentStateResponse>(state.Data.ConversationHistory[1]);
        Assert.Equal("developer", Assert.Single(response.Messages).Role);

        provider.CompleteStagedResponse(
            new AgentResponse(
                new ChatMessage(new ChatRole("developer"), "developer response")));
        Assert.Equal(
            "developer",
            Assert.Single(
                Assert.IsType<DurableAgentStateResponse>(
                    state.Data.ConversationHistory[1]).Messages).Role);
    }

    [Fact]
    public async Task EntityWrapperUsesDurableProviderThroughDelegatingAgentAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent chatAgent = new(client, name: "test-agent");
        AIAgent wrappedAgent = new TestDelegatingAgent(chatAgent);
        AgentSession session = await wrappedAgent.CreateSessionAsync();
        DurableAgentState state = CreateStateWithExchange("old", "old request", "old response");
        RunRequest request = new("new request") { CorrelationId = "new" };
        DurableChatHistoryProvider provider = new(state.Data.ConversationHistory, request);
        Mock<TaskEntityContext> context = new();
        context.SetupGet(value => value.Id).Returns(new EntityInstanceId("dafx-test-agent", "session"));
        EntityAgentWrapper wrapper = new(wrappedAgent, context.Object, request, chatHistoryProvider: provider);

        AgentResponse response = await wrapper.RunAsync(request.Messages, session);
        provider.CompleteStagedResponse(response);

        Assert.Equal(3, client.LastMessages.Count);
        Assert.Equal(["old request", "old response", "new request"], client.LastMessages.Select(message => message.Text));
        Assert.True(provider.HasStagedTurn);
        Assert.Equal(4, state.Data.ConversationHistory.Count);
    }

    [Fact]
    public async Task ServiceManagedSessionDoesNotStoreTranscriptAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent chatAgent = new(client, name: "test-agent");
        AgentSession session = await chatAgent.CreateSessionAsync("service-conversation");
        DurableAgentState state = new();
        RunRequest request = new("new request") { CorrelationId = "new" };
        DurableChatHistoryProvider provider = new(state.Data.ConversationHistory, request);

        await provider.InvokedAsync(
            new ChatHistoryProvider.InvokedContext(
                chatAgent,
                session,
                request.Messages,
                [new ChatMessage(ChatRole.Assistant, "response")]));

        Assert.False(provider.HasStagedTurn);
        Assert.Empty(state.Data.ConversationHistory);
    }

    [Fact]
    public async Task ProviderStoresOnlyNonHistoryRequestMessagesAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent chatAgent = new(client, name: "test-agent");
        AgentSession session = await chatAgent.CreateSessionAsync();
        DurableAgentState state = CreateStateWithExchange("old", "old request", "old response");
        RunRequest request = new("new request") { CorrelationId = "new" };
        DurableChatHistoryProvider provider = new(state.Data.ConversationHistory, request);

        IEnumerable<ChatMessage> merged = await provider.InvokingAsync(
            new ChatHistoryProvider.InvokingContext(chatAgent, session, request.Messages));
        await provider.InvokedAsync(
            new ChatHistoryProvider.InvokedContext(
                chatAgent,
                session,
                merged,
                [new ChatMessage(ChatRole.Assistant, "new response")]));

        DurableAgentStateRequest storedRequest =
            Assert.IsType<DurableAgentStateRequest>(state.Data.ConversationHistory[^2]);
        Assert.Single(storedRequest.Messages);
        Assert.Equal("new request", storedRequest.Messages[0].ToChatMessage().Text);
        Assert.Equal(4, state.Data.ConversationHistory.Count);
        Assert.Single(
            state.Data.ConversationHistory.OfType<DurableAgentStateRequest>(),
            entry => entry.CorrelationId == "new");
    }

    [Fact]
    public async Task ProviderReplaysCompactionButNotErrorResponseAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent chatAgent = new(client, name: "test-agent");
        AgentSession session = await chatAgent.CreateSessionAsync();
        DurableAgentState state = new();
        state.Data.ConversationHistory.Add(
            new DurableAgentStateErrorResponse
            {
                CorrelationId = "failed",
                CreatedAt = DateTimeOffset.UtcNow,
                Messages = [DurableAgentStateMessage.FromChatMessage(new ChatMessage(ChatRole.Assistant, "error"))],
            });
        state.Data.ConversationHistory.Add(
            new DurableAgentStateCompaction
            {
                CreatedAt = DateTimeOffset.UtcNow,
                Messages = [DurableAgentStateMessage.FromChatMessage(new ChatMessage(ChatRole.Assistant, "summary"))],
            });
        RunRequest request = new("new request") { CorrelationId = "new" };
        DurableChatHistoryProvider provider = new(state.Data.ConversationHistory, request);

        IEnumerable<ChatMessage> messages = await provider.InvokingAsync(
            new ChatHistoryProvider.InvokingContext(chatAgent, session, request.Messages));

        Assert.Equal(["summary", "new request"], messages.Select(message => message.Text));
    }

    [Fact]
    public async Task ProviderDropsReasoningOnlyMessagesAndKeepsOtherMixedContentAsync()
    {
        ChatClientAgent chatAgent = new(new RecordingChatClient(), name: "test-agent");
        AgentSession session = await chatAgent.CreateSessionAsync();
        DurableAgentState state = new();
        state.Data.ConversationHistory.Add(
            new DurableAgentStateResponse
            {
                CorrelationId = "old",
                CreatedAt = DateTimeOffset.UtcNow,
                Messages =
                [
                    new DurableAgentStateMessage
                    {
                        Role = ChatRole.Assistant.Value,
                        Contents =
                        [
                            new DurableAgentStateTextReasoningContent { Text = "reasoning only" },
                        ],
                    },
                    new DurableAgentStateMessage
                    {
                        Role = ChatRole.Assistant.Value,
                        Contents =
                        [
                            new DurableAgentStateTextReasoningContent { Text = "private reasoning" },
                            new DurableAgentStateTextContent { Text = "visible answer" },
                        ],
                    },
                ],
            });
        RunRequest request = new("new request") { CorrelationId = "new" };
        DurableChatHistoryProvider provider = new(state.Data.ConversationHistory, request);

        List<ChatMessage> messages = (await provider.InvokingAsync(
            new ChatHistoryProvider.InvokingContext(
                chatAgent,
                session,
                request.Messages))).ToList();

        Assert.Equal(["visible answer", "new request"], messages.Select(message => message.Text));
        Assert.DoesNotContain(
            messages.SelectMany(message => message.Contents),
            content => content is TextReasoningContent);
    }

    [Fact]
    public async Task ProviderDoesNotReplayMetadataOnlyRequestEnvelopesAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent chatAgent = new(client, name: "test-agent");
        AgentSession session = await chatAgent.CreateSessionAsync();
        DurableAgentState state = new();
        state.Data.ConversationHistory.Add(
            new DurableAgentStateRequest
            {
                CorrelationId = "old",
                CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
                Messages =
                [
                    new DurableAgentStateMessage
                    {
                        Role = ChatRole.User.Value,
                        Contents = [],
                    },
                ],
            });
        state.Data.ConversationHistory.Add(
            DurableAgentStateResponse.FromResponse(
                "old",
                new AgentResponse(new ChatMessage(ChatRole.Assistant, "old response"))));
        RunRequest request = new("new request") { CorrelationId = "new" };
        DurableChatHistoryProvider provider = new(state.Data.ConversationHistory, request);

        IEnumerable<ChatMessage> messages = await provider.InvokingAsync(
            new ChatHistoryProvider.InvokingContext(chatAgent, session, request.Messages));

        Assert.Equal(["old response", "new request"], messages.Select(message => message.Text));
    }

    [Fact]
    public async Task ProviderPersistsButDoesNotReplayMetadataOnlyResponsesAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent chatAgent = new(client, name: "test-agent");
        AgentSession session = await chatAgent.CreateSessionAsync();
        DurableAgentState state = new();
        state.Data.ConversationHistory.Add(
            DurableAgentStateResponse.FromResponse(
                "old",
                new AgentResponse(
                [
                    new ChatMessage(ChatRole.Assistant, [])
                    {
                        MessageId = "metadata-only",
                        AdditionalProperties = new() { ["status"] = "complete" },
                    },
                    new ChatMessage(ChatRole.Assistant, "old response"),
                ])));
        RunRequest request = new("new request") { CorrelationId = "new" };
        DurableChatHistoryProvider provider = new(state.Data.ConversationHistory, request);

        IEnumerable<ChatMessage> messages = await provider.InvokingAsync(
            new ChatHistoryProvider.InvokingContext(chatAgent, session, request.Messages));

        DurableAgentStateResponse stored =
            Assert.IsType<DurableAgentStateResponse>(Assert.Single(state.Data.ConversationHistory));
        Assert.Equal(2, stored.Messages.Count);
        Assert.Equal("metadata-only", stored.Messages[0].MessageId);
        Assert.Equal(["old response", "new request"], messages.Select(message => message.Text));
    }

    [Fact]
    public async Task ProviderMigratesIdsBeforeReplayFilteringAsync()
    {
        DateTimeOffset compactionTime =
            DateTimeOffset.Parse("2026-07-27T12:34:56.123456+00:00");
        DurableAgentState state = new();
        DurableAgentStateRequest requestEntry = new()
        {
            CorrelationId = "old",
            CreatedAt = compactionTime.AddSeconds(-2),
            Messages =
            [
                new DurableAgentStateMessage
                {
                    Role = ChatRole.User.Value,
                    Contents = [new DurableAgentStateTextContent { Text = "old request" }],
                },
            ],
        };
        DurableAgentStateResponse responseEntry = new()
        {
            CorrelationId = "old",
            CreatedAt = compactionTime.AddSeconds(-1),
            Messages =
            [
                new DurableAgentStateMessage
                {
                    Role = ChatRole.Assistant.Value,
                    Contents = [],
                },
                new DurableAgentStateMessage
                {
                    Role = ChatRole.Assistant.Value,
                    Contents = [new DurableAgentStateTextContent { Text = "old response" }],
                },
                new DurableAgentStateMessage
                {
                    Role = ChatRole.Assistant.Value,
                    Contents = [new DurableAgentStateTextReasoningContent { Text = "reasoning" }],
                },
            ],
        };
        DurableAgentStateErrorResponse errorEntry = new()
        {
            CorrelationId = "failed",
            CreatedAt = compactionTime,
            Messages =
            [
                new DurableAgentStateMessage
                {
                    Role = ChatRole.Assistant.Value,
                    Contents = [new DurableAgentStateTextContent { Text = "error" }],
                },
            ],
        };
        DurableAgentStateCompaction compactionEntry = new()
        {
            CreatedAt = compactionTime,
            Messages =
            [
                new DurableAgentStateMessage
                {
                    Role = ChatRole.Assistant.Value,
                    Contents = [new DurableAgentStateTextContent { Text = "summary" }],
                },
            ],
        };
        state.Data.ConversationHistory.Add(requestEntry);
        state.Data.ConversationHistory.Add(responseEntry);
        state.Data.ConversationHistory.Add(errorEntry);
        state.Data.ConversationHistory.Add(compactionEntry);
        state.Data.ConversationHistory.Add(
            new DurableAgentStateRequest
            {
                CorrelationId = "current",
                CreatedAt = compactionTime,
                Messages =
                [
                    new DurableAgentStateMessage
                    {
                        Role = ChatRole.User.Value,
                        Contents = [new DurableAgentStateTextContent { Text = "must not duplicate" }],
                    },
                ],
            });
        RunRequest request = new("new request") { CorrelationId = "current" };
        DurableChatHistoryProvider provider = new(state.Data.ConversationHistory, request);
        ChatClientAgent agent = new(new RecordingChatClient(), name: "test-agent");
        AgentSession session = await agent.CreateSessionAsync();

        IEnumerable<ChatMessage> messages = await provider.InvokingAsync(
            new ChatHistoryProvider.InvokingContext(agent, session, request.Messages));

        Assert.Equal(["old request", "old response", "summary", "new request"], messages.Select(message => message.Text));
        Assert.Equal("durable_request_old_0", requestEntry.Messages[0].MessageId);
        Assert.Equal("durable_response_old_0", responseEntry.Messages[0].MessageId);
        Assert.Equal("durable_response_old_1", responseEntry.Messages[1].MessageId);
        Assert.Equal("durable_response_old_2", responseEntry.Messages[2].MessageId);
        Assert.Equal("durable_errorResponse_failed_0", errorEntry.Messages[0].MessageId);
        Assert.Equal(
            "durable_compaction_2026-07-27T12:34:56.123456+00:00_0",
            compactionEntry.Messages[0].MessageId);

        string serialized = System.Text.Json.JsonSerializer.Serialize(
            state,
            DurableAgentStateJsonContext.Default.DurableAgentState);
        DurableAgentState restored = Assert.IsType<DurableAgentState>(
            System.Text.Json.JsonSerializer.Deserialize(
                serialized,
                DurableAgentStateJsonContext.Default.DurableAgentState));

        Assert.Equal(
            state.Data.ConversationHistory.SelectMany(entry => entry.Messages).Select(message => message.MessageId),
            restored.Data.ConversationHistory.SelectMany(entry => entry.Messages).Select(message => message.MessageId));
    }

    private static DurableAgentState CreateStateWithExchange(
        string correlationId,
        string request,
        string response)
    {
        DurableAgentState state = new();
        state.Data.ConversationHistory.Add(
            DurableAgentStateRequest.FromRunRequest(
                new RunRequest(request) { CorrelationId = correlationId }));
        state.Data.ConversationHistory.Add(
            DurableAgentStateResponse.FromResponse(
                correlationId,
                new AgentResponse(new ChatMessage(ChatRole.Assistant, response))));
        return state;
    }

    private sealed class TestDelegatingAgent(AIAgent innerAgent) : DelegatingAIAgent(innerAgent);

    private sealed class RecordingChatClient : IChatClient
    {
        public List<ChatMessage> LastMessages { get; private set; } = [];

        public void Dispose()
        {
        }

        public object? GetService(Type serviceType, object? serviceKey = null) =>
            serviceType.IsInstanceOfType(this) ? this : null;

        public Task<ChatResponse> GetResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            this.LastMessages = messages.ToList();
            return Task.FromResult(
                new ChatResponse(new ChatMessage(ChatRole.Assistant, "response")));
        }

        public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            this.LastMessages = messages.ToList();
            await Task.Yield();
            yield return new ChatResponseUpdate(ChatRole.Assistant, "response");
        }
    }
}
