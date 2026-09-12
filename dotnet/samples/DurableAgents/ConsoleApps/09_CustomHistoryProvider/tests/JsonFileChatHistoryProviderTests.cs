// Copyright (c) Microsoft. All rights reserved.

using System.Text;
using CustomHistoryProvider;
using Microsoft.Agents.AI;
using Microsoft.Extensions.AI;

namespace CustomHistoryProviderTests;

public sealed class JsonFileChatHistoryProviderTests
{
    [Fact]
    public void LogicalProviderKeyIsAStableExplicitIdentifier()
    {
        Assert.Equal(
            "sample-json-file-history.v1",
            JsonFileChatHistoryProvider.ProviderKey);
    }

    [Fact]
    public async Task HistoryRoundTripsThroughANewProviderInstanceAsync()
    {
        string directory = CreateStoreDirectory();
        try
        {
            using JsonFileChatHistoryProvider firstProvider = new(
                directory,
                maxModelMessages: 8,
                maxModelTextUtf8Bytes: 24 * 1024);
            ChatClientAgent firstAgent = CreateAgent(firstProvider);
            AgentSession firstSession = await firstAgent.CreateSessionAsync();
            ChatMessage request = new(ChatRole.User, "first request");

            IEnumerable<ChatMessage> firstInput = await firstProvider.InvokingAsync(
                new ChatHistoryProvider.InvokingContext(firstAgent, firstSession, [request]));
            await firstProvider.InvokedAsync(
                new ChatHistoryProvider.InvokedContext(
                    firstAgent,
                    firstSession,
                    firstInput,
                    [new ChatMessage(ChatRole.Assistant, "first response")]));
            var serializedSession = await firstAgent.SerializeSessionAsync(firstSession);
            string firstHistoryId = firstProvider.GetHistoryId(firstSession);

            using JsonFileChatHistoryProvider secondProvider = new(
                directory,
                maxModelMessages: 8,
                maxModelTextUtf8Bytes: 24 * 1024);
            ChatClientAgent secondAgent = CreateAgent(secondProvider);
            AgentSession restoredSession = await secondAgent.DeserializeSessionAsync(serializedSession);
            IEnumerable<ChatMessage> restoredInput = await secondProvider.InvokingAsync(
                new ChatHistoryProvider.InvokingContext(
                    secondAgent,
                    restoredSession,
                    [new ChatMessage(ChatRole.User, "second request")]));

            Assert.Equal(
                ["first request", "first response", "second request"],
                restoredInput.Select(message => message.Text));
            Assert.Equal(firstHistoryId, secondProvider.GetHistoryId(restoredSession));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task StoreDoesNotDuplicateTheProvidedHistoryPrefixAsync()
    {
        string directory = CreateStoreDirectory();
        try
        {
            using JsonFileChatHistoryProvider provider = new(directory);
            ChatClientAgent agent = CreateAgent(provider);
            AgentSession session = await agent.CreateSessionAsync();

            IEnumerable<ChatMessage> firstInput = await provider.InvokingAsync(
                new ChatHistoryProvider.InvokingContext(
                    agent,
                    session,
                    [new ChatMessage(ChatRole.User, "first request")]));
            await provider.InvokedAsync(
                new ChatHistoryProvider.InvokedContext(
                    agent,
                    session,
                    firstInput,
                    [new ChatMessage(ChatRole.Assistant, "first response")]));

            IEnumerable<ChatMessage> secondInput = await provider.InvokingAsync(
                new ChatHistoryProvider.InvokingContext(
                    agent,
                    session,
                    [new ChatMessage(ChatRole.User, "second request")]));
            await provider.InvokedAsync(
                new ChatHistoryProvider.InvokedContext(
                    agent,
                    session,
                    secondInput,
                    [new ChatMessage(ChatRole.Assistant, "second response")]));

            IReadOnlyList<ChatMessage> stored = await provider.ReadMessagesAsync(session);
            Assert.Equal(
                ["first request", "first response", "second request", "second response"],
                stored.Select(message => message.Text));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task StoreCanExceedOneMiBWhileModelHistoryRemainsBoundedAsync()
    {
        const long OneMiB = 1_048_576;
        const int MessageTextBytes = 4 * 1024;
        const int WindowMessages = 8;
        const int WindowTextBytes = 24 * 1024;

        string directory = CreateStoreDirectory();
        try
        {
            using JsonFileChatHistoryProvider provider = new(
                directory,
                maxModelMessages: WindowMessages,
                maxModelTextUtf8Bytes: WindowTextBytes);
            ChatClientAgent agent = CreateAgent(provider);
            AgentSession session = await agent.CreateSessionAsync();

            IEnumerable<ChatMessage> bootstrapInput = await provider.InvokingAsync(
                new ChatHistoryProvider.InvokingContext(
                    agent,
                    session,
                    [new ChatMessage(ChatRole.User, "bootstrap")]));
            await provider.InvokedAsync(
                new ChatHistoryProvider.InvokedContext(
                    agent,
                    session,
                    bootstrapInput,
                    [new ChatMessage(ChatRole.Assistant, "ready")]));

            string historyId = provider.GetHistoryId(session);
            JsonFileChatHistoryProvider.SeedHistoryResult seed = await provider.SeedHistoryAsync(
                historyId,
                OneMiB,
                MessageTextBytes);

            IEnumerable<ChatMessage> projectedInput = await provider.InvokingAsync(
                new ChatHistoryProvider.InvokingContext(
                    agent,
                    session,
                    [new ChatMessage(ChatRole.User, "small current request")]));
            List<ChatMessage> projectedMessages = projectedInput.ToList();
            JsonFileChatHistoryProvider.HistoryStatistics statistics =
                await provider.GetStatisticsAsync(historyId);
            IReadOnlyList<ChatMessage> allMessages = await provider.ReadMessagesAsync(session);
            var serializedSession = await agent.SerializeSessionAsync(session);

            Assert.True(seed.PersistedBytes > OneMiB);
            Assert.True(seed.SeededMessageCount > 100);
            Assert.Equal(MessageTextBytes, seed.MaximumSeedMessageTextUtf8Bytes);
            Assert.All(
                allMessages.Where(message => message.MessageId?.StartsWith(
                    "simulated-old-history-",
                    StringComparison.Ordinal) is true),
                message => Assert.Equal(MessageTextBytes, Encoding.UTF8.GetByteCount(message.Text!)));
            Assert.Equal("small current request", projectedMessages[^1].Text);
            Assert.NotNull(statistics.LastModelWindow);
            Assert.Equal(statistics.LastModelWindow.MessageCount + 1, projectedMessages.Count);
            Assert.True(statistics.LastModelWindow.MessageCount <= WindowMessages);
            Assert.True(statistics.LastModelWindow.TextUtf8Bytes <= WindowTextBytes);
            Assert.True(statistics.PersistedMessageCount > statistics.LastModelWindow.MessageCount);
            Assert.DoesNotContain("SIMULATED OLD HISTORY", serializedSession.GetRawText());
            Assert.True(Encoding.UTF8.GetByteCount(serializedSession.GetRawText()) < 4096);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task FrameworkFilteredHistoryIsNotStoredAgainAfterInvocationAsync()
    {
        string directory = CreateStoreDirectory();
        try
        {
            using JsonFileChatHistoryProvider provider = new(
                directory,
                maxModelMessages: 4,
                maxModelTextUtf8Bytes: 16 * 1024);
            ChatClientAgent agent = CreateAgent(provider);
            AgentSession session = await agent.CreateSessionAsync();
            string historyId = provider.GetHistoryId(session);
            await provider.SeedHistoryAsync(historyId, 32 * 1024, 1024);

            int countBefore = (await provider.ReadMessagesAsync(session)).Count;
            IEnumerable<ChatMessage> input = await provider.InvokingAsync(
                new ChatHistoryProvider.InvokingContext(
                    agent,
                    session,
                    [new ChatMessage(ChatRole.User, "new request")]));
            await provider.InvokedAsync(
                new ChatHistoryProvider.InvokedContext(
                    agent,
                    session,
                    input,
                    [new ChatMessage(ChatRole.Assistant, "new response")]));

            IReadOnlyList<ChatMessage> stored = await provider.ReadMessagesAsync(session);
            Assert.Equal(countBefore + 2, stored.Count);
            Assert.Equal(["new request", "new response"], stored.TakeLast(2).Select(message => message.Text));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task UnsupportedContentFailsWithoutWritingAHistoryFileAsync()
    {
        string directory = CreateStoreDirectory();
        try
        {
            using JsonFileChatHistoryProvider provider = new(directory);
            ChatClientAgent agent = CreateAgent(provider);
            AgentSession session = await agent.CreateSessionAsync();
            ChatMessage request = new(
                ChatRole.User,
                [new FunctionCallContent("call-id", "sample-tool")]);

            IEnumerable<ChatMessage> input = await provider.InvokingAsync(
                new ChatHistoryProvider.InvokingContext(agent, session, [request]));

            await Assert.ThrowsAsync<NotSupportedException>(
                () => provider.InvokedAsync(
                    new ChatHistoryProvider.InvokedContext(
                        agent,
                        session,
                        input,
                        [new ChatMessage(ChatRole.Assistant, "not stored")])).AsTask());

            Assert.Empty(Directory.EnumerateFiles(directory));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Fact]
    public async Task CanceledSeedDoesNotCreateOrMutateHistoryAsync()
    {
        string directory = CreateStoreDirectory();
        try
        {
            using JsonFileChatHistoryProvider provider = new(directory);
            using CancellationTokenSource cancellation = new();
            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => provider.SeedHistoryAsync(
                    "canceled.json",
                    minimumStoreBytes: 1024,
                    messageTextUtf8Bytes: 512,
                    cancellation.Token));

            Assert.Empty(Directory.EnumerateFiles(directory));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static ChatClientAgent CreateAgent(JsonFileChatHistoryProvider provider) =>
        new(
            new TestChatClient(),
            new ChatClientAgentOptions
            {
                Name = "test-agent",
                ChatHistoryProvider = provider,
            });

    private static string CreateStoreDirectory()
    {
        string directory = Path.Combine(
            AppContext.BaseDirectory,
            ".test-history",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        return directory;
    }

    private sealed class TestChatClient : IChatClient
    {
        public void Dispose()
        {
        }

        public object? GetService(Type serviceType, object? serviceKey = null) =>
            serviceType.IsInstanceOfType(this) ? this : null;

        public Task<ChatResponse> GetResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            CancellationToken cancellationToken = default) =>
            Task.FromResult(
                new ChatResponse(new ChatMessage(ChatRole.Assistant, "test response")));

        public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            yield return new ChatResponseUpdate(ChatRole.Assistant, "test response");
        }
    }
}
