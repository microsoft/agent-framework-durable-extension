// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class DurableAgentSessionStateTests
{
    [Fact]
    public async Task ColdRestorePreservesNonHistoryProviderStateAsync()
    {
        ChatClientAgent agent = new(new StubChatClient(), name: "agent");
        AgentSession session = await agent.CreateSessionAsync();
        session.StateBag.SetValue("custom", "value");
        session.StateBag.SetValue("custom-compaction-state", "preserved");
        session.StateBag.SetValue(
            nameof(InMemoryChatHistoryProvider),
            new InMemoryChatHistoryProvider.State
            {
                Messages = [new ChatMessage(ChatRole.User, "duplicate")],
            });

        var serialized = await DurableAgentSessionState.SerializeAsync(
            agent,
            session,
            [nameof(InMemoryChatHistoryProvider)],
            CancellationToken.None);
        AgentSession restored = await DurableAgentSessionState.RestoreAsync(
            agent,
            serialized,
            CancellationToken.None);

        Assert.Equal("value", restored.StateBag.GetValue<string>("custom"));
        Assert.Equal(
            "preserved",
            restored.StateBag.GetValue<string>("custom-compaction-state"));
        Assert.False(
            restored.StateBag.TryGetValue<InMemoryChatHistoryProvider.State>(
                nameof(InMemoryChatHistoryProvider),
                out _));
    }

    [Fact]
    public async Task ExternalProviderStateIsPreservedWhenNothingIsExcludedAsync()
    {
        ChatClientAgent agent = new(new StubChatClient(), name: "agent");
        AgentSession session = await agent.CreateSessionAsync();
        session.StateBag.SetValue("external-history", new ExternalState { ConversationKey = "abc" });

        var serialized = await DurableAgentSessionState.SerializeAsync(
            agent,
            session,
            [],
            CancellationToken.None);
        AgentSession restored = await DurableAgentSessionState.RestoreAsync(
            agent,
            serialized,
            CancellationToken.None);

        ExternalState? state = restored.StateBag.GetValue<ExternalState>("external-history");
        Assert.Equal("abc", state?.ConversationKey);
    }

    [Fact]
    public async Task ServiceConversationIdentityRoundTripsAsync()
    {
        ChatClientAgent agent = new(new StubChatClient(), name: "agent");
        AgentSession session = await agent.CreateSessionAsync("service-id");

        var serialized = await DurableAgentSessionState.SerializeAsync(
            agent,
            session,
            [],
            CancellationToken.None);
        AgentSession restored = await DurableAgentSessionState.RestoreAsync(
            agent,
            serialized,
            CancellationToken.None);

        ChatClientAgentSession typed = Assert.IsType<ChatClientAgentSession>(restored);
        Assert.Equal("service-id", typed.ConversationId);
    }

    [Fact]
    public async Task UsesTheConcreteAgentsSessionSerializationContractAsync()
    {
        CustomSessionAgent agent = new();
        AgentSession created = await DurableAgentSessionState.RestoreAsync(
            agent,
            serializedSession: null,
            CancellationToken.None);

        JsonElement serialized = await DurableAgentSessionState.SerializeAsync(
            agent,
            created,
            [],
            CancellationToken.None);
        AgentSession restored = await DurableAgentSessionState.RestoreAsync(
            agent,
            serialized,
            CancellationToken.None);

        Assert.IsType<CustomSessionAgent.CustomSession>(created);
        Assert.IsType<CustomSessionAgent.CustomSession>(restored);
        Assert.Equal("agent-owned-format", serialized.GetProperty("format").GetString());
        Assert.Equal(1, agent.CreateCount);
        Assert.Equal(1, agent.SerializeCount);
        Assert.Equal(1, agent.DeserializeCount);
    }

    [Fact]
    public async Task InvalidSerializedContinuationDoesNotFallBackToCreatingANewSessionAsync()
    {
        CustomSessionAgent agent = new();
        JsonElement invalid = JsonSerializer.SerializeToElement(new { format = "unexpected" });

        await Assert.ThrowsAnyAsync<Exception>(
            async () => await DurableAgentSessionState.RestoreAsync(
                agent,
                invalid,
                CancellationToken.None));

        Assert.Equal(0, agent.CreateCount);
        Assert.Equal(1, agent.DeserializeCount);
    }

    private sealed class ExternalState
    {
        public string? ConversationKey { get; set; }
    }

    private sealed class StubChatClient : IChatClient
    {
        public void Dispose()
        {
        }

        public object? GetService(Type serviceType, object? serviceKey = null) => null;

        public Task<ChatResponse> GetResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(new ChatResponse(new ChatMessage(ChatRole.Assistant, "response")));

        public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            yield return new ChatResponseUpdate(ChatRole.Assistant, "response");
        }
    }

    private sealed class CustomSessionAgent : AIAgent
    {
        public int CreateCount { get; private set; }

        public int SerializeCount { get; private set; }

        public int DeserializeCount { get; private set; }

        protected override ValueTask<AgentSession> CreateSessionCoreAsync(
            CancellationToken cancellationToken = default)
        {
            this.CreateCount++;
            return new(new CustomSession());
        }

        protected override ValueTask<JsonElement> SerializeSessionCoreAsync(
            AgentSession session,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default)
        {
            Assert.IsType<CustomSession>(session);
            this.SerializeCount++;
            return new(JsonSerializer.SerializeToElement(new { format = "agent-owned-format" }));
        }

        protected override ValueTask<AgentSession> DeserializeSessionCoreAsync(
            JsonElement serializedState,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default)
        {
            this.DeserializeCount++;
            Assert.Equal("agent-owned-format", serializedState.GetProperty("format").GetString());
            return new(new CustomSession());
        }

        protected override Task<AgentResponse> RunCoreAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        protected override async IAsyncEnumerable<AgentResponseUpdate> RunCoreStreamingAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            yield break;
        }

        public sealed class CustomSession : AgentSession;
    }
}
