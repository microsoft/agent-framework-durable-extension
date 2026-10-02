// Copyright (c) Microsoft. All rights reserved.

using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Text.Json;
using Microsoft.Agents.AI.Compaction;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class AgentEntityHistoryTests
{
    private static readonly TimeSpan s_stageTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan s_testTimeout = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task EntityExecutionUsesDurableProviderAndPersistsSessionAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent agent = new(client, name: "agent");
        DurableAgentState initialState = CreateStateWithExchange("old", "old request", "old response");

        DurableAgentState persisted = await RunEntityAsync(
            agent,
            initialState,
            new RunRequest("new request") { CorrelationId = "new" });

        Assert.Equal(["old request", "old response", "new request"], client.LastMessages.Select(message => message.Text));
        Assert.Equal(4, persisted.Data.ConversationHistory.Count);
        Assert.Equal(DurableAgentStateHistoryBinding.DurableStateOwner, GetBinding(persisted)?.OwnerKind);
        Assert.Equal(DurableAgentHistoryBinding.DurableStateProviderKey, GetBinding(persisted)?.ProviderKey);
        Assert.Equal(2, persisted.Data.TerminalResults?.Count);
        Assert.Equal(2, persisted.Data.CompletionReceipts?.Count);
        Assert.NotNull(persisted.Data.Session);
        Assert.DoesNotContain(
            nameof(InMemoryChatHistoryProvider),
            persisted.Data.Session.Value.GetProperty("stateBag").EnumerateObject().Select(property => property.Name));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task DurableAdapterExcludesStaleInMemoryHistoryOnSuccessAndCertifiedFailureAsync(
        bool certifiedFailure)
    {
        InvalidOperationException failure = new("provider-private-message");
        RecordingChatClient client = new() { Exception = certifiedFailure ? failure : null };
        ChatClientAgent agent = new(client, name: "agent");
        AgentSession session = await agent.CreateSessionAsync();
        InMemoryChatHistoryProvider provider =
            Assert.IsType<InMemoryChatHistoryProvider>(agent.ChatHistoryProvider);
        provider.SetMessages(session, [new ChatMessage(ChatRole.User, "stale provider transcript")]);
        session.StateBag.SetValue("other-state", new ExternalHistoryState { Count = 1 });
        DurableAgentState initialState = await RunEntityAsync(
            new ChatClientAgent(new RecordingChatClient(), name: "agent"),
            new DurableAgentState(),
            new RunRequest("old request") { CorrelationId = "old" });
        initialState.Data.Session = await agent.SerializeSessionAsync(session);
        foreach (string key in provider.StateKeys)
        {
            Assert.True(initialState.Data.Session.Value.GetProperty("stateBag").TryGetProperty(key, out _));
        }

        EntityHarness harness = CreateHarness(
            agent,
            initialState,
            enableProviderFailureFinalization: certifiedFailure,
            providerFailureAttestor: new RecordingProviderFailureAttestor(failure));

        _ = await harness.RunAsync(
            new RunRequest("new request") { CorrelationId = "new" });

        DurableAgentState persisted = DeserializeState(
            SerializeState(Assert.IsType<DurableAgentState>(harness.PersistedState)));
        Assert.Equal(["old request", "response", "new request"], client.LastMessages.Select(message => message.Text));
        Assert.Equal(DurableAgentStateHistoryBinding.DurableStateOwner, GetBinding(persisted)?.OwnerKind);
        Assert.Equal(
            certifiedFailure ? DurableAgentStateCompletionReceipt.FailedOutcome : DurableAgentStateCompletionReceipt.SucceededOutcome,
            persisted.Data.TerminalResults!["new"].Outcome);
        Assert.Equal(certifiedFailure ? 3 : 4, persisted.Data.ConversationHistory.Count);
        JsonElement stateBag = persisted.Data.Session!.Value.GetProperty("stateBag");
        Assert.True(stateBag.TryGetProperty("other-state", out _));
        foreach (string key in provider.StateKeys)
        {
            Assert.False(stateBag.TryGetProperty(key, out _));
        }
    }

    [Fact]
    public async Task WrappedAgentDoesNotReplayTranscriptOutsideProviderAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent chatAgent = new(client, name: "agent");
        AIAgent wrappedAgent = new TestDelegatingAgent(chatAgent);
        DurableAgentState initialState = CreateStateWithExchange("old", "old request", "old response");

        DurableAgentState persisted = await RunEntityAsync(
            wrappedAgent,
            initialState,
            new RunRequest("new request") { CorrelationId = "new" });

        Assert.Equal(3, client.LastMessages.Count);
        Assert.Equal(4, persisted.Data.ConversationHistory.Count);
    }

    [Fact]
    public async Task LegacyMigrationUsesProviderReplayWithoutDuplicatingCurrentRequestAsync()
    {
        RecordingChatClient firstClient = new();
        ChatClientAgent firstAgent = new(firstClient, name: "agent");
        DurableAgentState legacyState = CreateStateWithExchange(
            "old",
            "old request",
            "old response");
        legacyState.Data.ConversationHistory.Add(
            new DurableAgentStateErrorResponse
            {
                CorrelationId = "failed",
                CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-2),
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.Assistant, "must not replay")),
                ],
            });
        legacyState.Data.ConversationHistory.Add(
            new DurableAgentStateResponse
            {
                CorrelationId = "reasoning",
                CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
                Messages =
                [
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

        DurableAgentState firstWrite = await RunEntityAsync(
            firstAgent,
            legacyState,
            new RunRequest("first new request") { CorrelationId = "new-1" });

        Assert.Equal(
            ["old request", "old response", "visible answer", "first new request"],
            firstClient.LastMessages.Select(message => message.Text));
        Assert.Equal(
            1,
            firstClient.LastMessages.Count(message => message.Text == "first new request"));
        Assert.DoesNotContain(
            firstClient.LastMessages.SelectMany(message => message.Contents),
            content => content is TextReasoningContent);
        Assert.Single(
            firstWrite.Data.ConversationHistory.OfType<DurableAgentStateRequest>(),
            entry => entry.CorrelationId == "new-1");
        Assert.Single(
            firstWrite.Data.ConversationHistory.OfType<DurableAgentStateResponse>(),
            entry => entry.CorrelationId == "new-1");

        RecordingChatClient secondClient = new();
        ChatClientAgent secondAgent = new(secondClient, name: "agent");
        DurableAgentState secondWrite = await RunEntityAsync(
            secondAgent,
            DeserializeState(SerializeState(firstWrite)),
            new RunRequest("second new request") { CorrelationId = "new-2" });

        Assert.Equal(
            [
                "old request",
                "old response",
                "visible answer",
                "first new request",
                "response",
                "second new request",
            ],
            secondClient.LastMessages.Select(message => message.Text));
        Assert.Equal(
            1,
            secondClient.LastMessages.Count(message => message.Text == "second new request"));
        Assert.Single(
            secondWrite.Data.ConversationHistory.OfType<DurableAgentStateRequest>(),
            entry => entry.CorrelationId == "new-2");
    }

    [Fact]
    public async Task NativeLegacyChatClientKeepsFullHistoryAcrossColdRestartAsync()
    {
        RecordingChatClient firstClient = new();
        ChatClientAgent firstAgent = new(firstClient, name: "agent");
        DurableAgentState legacyState = CreateStateWithExchange(
            "old",
            "old request",
            "old response");
        EntityHarness firstHarness = CreateHarness(
            firstAgent,
            legacyState,
            enableMailboxWrites: false);

        _ = await firstHarness.RunAsync(
            new RunRequest("first new request") { CorrelationId = "new-1" });
        DurableAgentState firstWrite =
            Assert.IsType<DurableAgentState>(firstHarness.PersistedState);

        Assert.Equal(
            ["old request", "old response", "first new request"],
            firstClient.LastMessages.Select(message => message.Text));
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, firstWrite.SchemaVersion);

        RecordingChatClient secondClient = new();
        ChatClientAgent secondAgent = new(secondClient, name: "agent");
        EntityHarness secondHarness = CreateHarness(
            secondAgent,
            DeserializeState(SerializeState(firstWrite)),
            enableMailboxWrites: false);

        _ = await secondHarness.RunAsync(
            new RunRequest("second new request") { CorrelationId = "new-2" });

        Assert.Equal(
            [
                "old request",
                "old response",
                "first new request",
                "response",
                "second new request",
            ],
            secondClient.LastMessages.Select(message => message.Text));

        DurableAgentState secondWrite = Assert.IsType<DurableAgentState>(secondHarness.PersistedState);
        RecordingChatClient thirdClient = new();
        EntityHarness thirdHarness = CreateHarness(
            new ChatClientAgent(thirdClient, name: "agent"),
            DeserializeState(SerializeState(secondWrite)),
            enableMailboxWrites: false);
        await thirdHarness.RunAsync(new RunRequest("third new request") { CorrelationId = "new-3" });

        Assert.Equal(
            ["old request", "old response", "first new request", "response", "second new request", "response", "third new request"],
            thirdClient.LastMessages.Select(message => message.Text));
        Assert.Equal(1, thirdClient.InvocationCount);
        Assert.DoesNotContain(
            nameof(InMemoryChatHistoryProvider),
            Assert.IsType<DurableAgentState>(thirdHarness.PersistedState)
                .Data.Session!.Value.GetProperty("stateBag").EnumerateObject().Select(property => property.Name));
    }

    [Fact]
    public async Task CustomProviderOwnsTranscriptAndEntityStoresOnlyMailboxAndContinuationAsync()
    {
        RecordingChatClient client = new();
        RecordingHistoryProvider provider = new();
        ChatClientAgent agent = new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = provider,
            });

        DurableAgentState persisted = await RunEntityAsync(
            agent,
            new DurableAgentState(),
            new RunRequest("new request") { CorrelationId = "new" },
            options => options.ProviderKey = new("external-history.v1"));

        Assert.Equal(1, provider.StoreCount);
        Assert.Empty(persisted.Data.ConversationHistory);
        Assert.Equal(DurableAgentStateHistoryBinding.HistoryProviderOwner, GetBinding(persisted)?.OwnerKind);
        Assert.Equal("external-history.v1", GetBinding(persisted)?.ProviderKey);
        Assert.Single(persisted.Data.TerminalResults!);
        Assert.Single(persisted.Data.CompletionReceipts!);
        Assert.True(
            persisted.Data.Session?.GetProperty("stateBag").TryGetProperty("external-history", out _) is true);
    }

    [Fact]
    public async Task DefaultSchemaWritesPreserveCustomProviderCompatibilityAsync()
    {
        RecordingChatClient client = new();
        RecordingHistoryProvider provider = new();
        EntityHarness harness = CreateHarness(
            CreateAgentWithProvider(client, provider),
            new DurableAgentState(),
            enableMailboxWrites: false);

        await harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" });

        DurableAgentState persisted = Assert.IsType<DurableAgentState>(harness.PersistedState);
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, persisted.SchemaVersion);
        Assert.Equal(1, provider.LoadCount);
        Assert.Equal(1, provider.StoreCount);
        Assert.Equal(1, client.InvocationCount);
        Assert.Equal(2, persisted.Data.ConversationHistory.Count);
        Assert.Equal(JsonValueKind.Undefined, persisted.Data.HistoryBinding.ValueKind);
        Assert.Null(persisted.Data.TerminalResults);
        Assert.Null(persisted.Data.CompletionReceipts);
    }

    [Fact]
    public async Task DefaultSchemaCustomProviderIsSoleHistorySourceAcrossColdRestartAsync()
    {
        RecordingHistoryProvider firstProvider = new() { PersistTranscript = true };
        RecordingChatClient firstClient = new();
        EntityHarness firstHarness = CreateHarness(
            CreateAgentWithProvider(firstClient, firstProvider),
            new DurableAgentState(),
            enableMailboxWrites: false);

        await firstHarness.RunAsync(
            new RunRequest("first request") { CorrelationId = "first" });
        DurableAgentState firstWrite =
            Assert.IsType<DurableAgentState>(firstHarness.PersistedState);

        RecordingHistoryProvider secondProvider = new() { PersistTranscript = true };
        RecordingChatClient secondClient = new();
        EntityHarness secondHarness = CreateHarness(
            CreateAgentWithProvider(secondClient, secondProvider),
            DeserializeState(SerializeState(firstWrite)),
            enableMailboxWrites: false);

        await secondHarness.RunAsync(
            new RunRequest("second request") { CorrelationId = "second" });

        Assert.Equal(
            ["first request", "response", "second request"],
            secondClient.LastMessages.Select(message => message.Text));
        Assert.Equal(
            1,
            secondClient.LastMessages.Count(message => message.Text == "first request"));
        Assert.Equal(
            1,
            secondClient.LastMessages.Count(message => message.Text == "response"));
        Assert.Equal(1, secondProvider.LoadCount);
        Assert.Equal(1, secondProvider.StoreCount);
        Assert.Equal(1, secondClient.InvocationCount);
    }

    [Fact]
    public async Task DefaultSchemaCustomProviderToolLoopStoresAndReplaysOneOuterTurnAsync()
    {
        AIFunction tool = AIFunctionFactory.Create(
            (string value) => $"tool result: {value}",
            name: "echo");
        RecordingHistoryProvider firstProvider = new() { PersistTranscript = true };
        ToolLoopChatClient firstClient = new(tool.Name);
        ChatClientAgent firstAgent = new(
            firstClient,
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = firstProvider,
                ChatOptions = new ChatOptions { Tools = [tool] },
            });
        EntityHarness firstHarness = CreateHarness(
            firstAgent,
            new DurableAgentState(),
            enableMailboxWrites: false);

        await firstHarness.RunAsync(
            new RunRequest("first request") { CorrelationId = "first" });
        DurableAgentState firstWrite =
            Assert.IsType<DurableAgentState>(firstHarness.PersistedState);

        Assert.Equal(2, firstClient.InvocationCount);
        Assert.Equal(1, firstProvider.LoadCount);
        Assert.Equal(1, firstProvider.StoreCount);

        RecordingHistoryProvider secondProvider = new() { PersistTranscript = true };
        RecordingChatClient secondClient = new();
        ChatClientAgent secondAgent = new(
            secondClient,
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = secondProvider,
                ChatOptions = new ChatOptions { Tools = [tool] },
            });
        EntityHarness secondHarness = CreateHarness(
            secondAgent,
            DeserializeState(SerializeState(firstWrite)),
            enableMailboxWrites: false);

        await secondHarness.RunAsync(
            new RunRequest("second request") { CorrelationId = "second" });

        Assert.Equal(
            1,
            secondClient.LastMessages.Count(message => message.Text == "first request"));
        Assert.Equal(
            1,
            secondClient.LastMessages.Count(message => message.Text == "second request"));
        Assert.Equal(1, secondProvider.LoadCount);
        Assert.Equal(1, secondProvider.StoreCount);
        Assert.Equal(1, secondClient.InvocationCount);
    }

    [Fact]
    public async Task DefaultSchemaWritesPreserveFirstServiceTransitionCompatibilityAsync()
    {
        RecordingChatClient client = new() { ResponseConversationId = "service-id" };
        EntityHarness harness = CreateHarness(
            new ChatClientAgent(client, name: "agent"),
            new DurableAgentState(),
            enableMailboxWrites: false);

        await harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" });

        DurableAgentState persisted = Assert.IsType<DurableAgentState>(harness.PersistedState);
        Assert.Equal(1, client.InvocationCount);
        Assert.Equal(2, persisted.Data.ConversationHistory.Count);
        Assert.Equal(JsonValueKind.Undefined, persisted.Data.HistoryBinding.ValueKind);
        Assert.Equal(
            "service-id",
            persisted.Data.Session?.GetProperty("conversationId").GetString());
    }

    [Fact]
    public async Task DefaultSchemaServiceSessionIsSoleHistorySourceAcrossColdRestartAsync()
    {
        RecordingChatClient firstClient = new() { ResponseConversationId = "service-id" };
        EntityHarness firstHarness = CreateHarness(
            new ChatClientAgent(firstClient, name: "agent"),
            new DurableAgentState(),
            enableMailboxWrites: false);

        await firstHarness.RunAsync(
            new RunRequest("first request") { CorrelationId = "first" });
        DurableAgentState firstWrite =
            Assert.IsType<DurableAgentState>(firstHarness.PersistedState);

        RecordingChatClient secondClient = new();
        EntityHarness secondHarness = CreateHarness(
            new ChatClientAgent(secondClient, name: "agent"),
            DeserializeState(SerializeState(firstWrite)),
            enableMailboxWrites: false);

        await secondHarness.RunAsync(
            new RunRequest("second request") { CorrelationId = "second" });

        Assert.Equal(["second request"], secondClient.LastMessages.Select(message => message.Text));
        Assert.Equal("service-id", secondClient.LastConversationId);
        Assert.Equal(1, secondClient.InvocationCount);
    }

    [Fact]
    public async Task ServiceManagedConversationStoresOnlyMailboxAndContinuationAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent agent = new(client, name: "agent");
        AgentSession serviceSession = await agent.CreateSessionAsync("service-id");
        DurableAgentState initialState = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            PersistentRequestOutcomesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
            },
        };
        initialState.Data.Session = await agent.SerializeSessionAsync(serviceSession);

        DurableAgentState persisted = await RunEntityAsync(
            agent,
            initialState,
            new RunRequest("new request") { CorrelationId = "new" },
            options => options.ProviderKey = new("model-service.v1"));

        Assert.Equal(["new request"], client.LastMessages.Select(message => message.Text));
        Assert.Empty(persisted.Data.ConversationHistory);
        Assert.Equal(DurableAgentStateHistoryBinding.ModelServiceOwner, GetBinding(persisted)?.OwnerKind);
        Assert.Equal("model-service.v1", GetBinding(persisted)?.ProviderKey);
        Assert.Single(persisted.Data.TerminalResults!);
        Assert.Equal(
            "service-id",
            persisted.Data.Session?.GetProperty("conversationId").GetString());
    }

    [Fact]
    public async Task KnownServiceOwnerWithoutProviderKeyFailsBeforeModelAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent agent = new(client, name: "agent");
        AgentSession serviceSession = await agent.CreateSessionAsync("service-id");
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            PersistentRequestOutcomesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
                Session = await agent.SerializeSessionAsync(serviceSession),
            },
        };
        EntityHarness harness = CreateHarness(agent, state);

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" }));

        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task FirstServiceManagedTurnDoesNotLeaveEntityOwnedTranscriptAsync()
    {
        RecordingChatClient client = new() { ResponseConversationId = "service-id" };
        ChatClientAgent agent = new(client, name: "agent");

        DurableAgentState persisted = await RunEntityAsync(
            agent,
            new DurableAgentState(),
            new RunRequest("new request") { CorrelationId = "new" },
            options => options.ProviderKey = new("model-service.v1"));

        Assert.Empty(persisted.Data.ConversationHistory);
        Assert.Equal(DurableAgentStateHistoryBinding.ModelServiceOwner, GetBinding(persisted)?.OwnerKind);
        Assert.Equal("model-service.v1", GetBinding(persisted)?.ProviderKey);
        Assert.Single(persisted.Data.TerminalResults!);
        Assert.Single(persisted.Data.CompletionReceipts!);
        Assert.Equal(
            "service-id",
            persisted.Data.Session?.GetProperty("conversationId").GetString());
    }

    [Fact]
    public async Task ServiceManagedPerCallDeclarationIsIgnoredWhenPerCallPersistenceIsDisabledAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent agent = new(client, name: "agent");
        DurableAgentState state = CreateStateWithExchange("old", "old request", "old response");

        DurableAgentState persisted = await RunEntityAsync(
            agent,
            state,
            new RunRequest("new request") { CorrelationId = "new" },
            options => options.ServiceManagedPerServiceCallHistory = true);

        Assert.Equal(["old request", "old response", "new request"], client.LastMessages.Select(message => message.Text));
        Assert.Equal(4, persisted.Data.ConversationHistory.Count);
    }

    [Fact]
    public async Task PerServiceCallServiceOwnershipExcludesLocalProviderTranscriptStateAsync()
    {
        RecordingChatClient client = new() { ResponseConversationId = "service-id" };
#pragma warning disable MAAI001
        ChatClientAgent agent = new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                RequirePerServiceCallChatHistoryPersistence = true,
            });
#pragma warning restore MAAI001

        DurableAgentState persisted = await RunEntityAsync(
            agent,
            new DurableAgentState(),
            new RunRequest("new request") { CorrelationId = "new" },
            options =>
            {
                options.ServiceManagedPerServiceCallHistory = true;
                options.ProviderKey = new("model-service.v1");
            });

        Assert.Equal(["new request"], client.LastMessages.Select(message => message.Text));
        Assert.Empty(persisted.Data.ConversationHistory);
        Assert.Equal(DurableAgentStateHistoryBinding.ModelServiceOwner, GetBinding(persisted)?.OwnerKind);
        Assert.Single(persisted.Data.TerminalResults!);
        Assert.Equal("service-id", persisted.Data.Session?.GetProperty("conversationId").GetString());
        JsonElement serializedSession = persisted.Data.Session.GetValueOrDefault();
        Assert.DoesNotContain(
            nameof(InMemoryChatHistoryProvider),
            serializedSession.GetProperty("stateBag").EnumerateObject().Select(property => property.Name));
    }

    [Fact]
    public async Task AmbiguousPerServiceCallOwnershipFailsBeforeModelExecutionAsync()
    {
        RecordingChatClient client = new();
        RecordingHistoryProvider provider = new();
#pragma warning disable MAAI001
        ChatClientAgent agent = new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = provider,
                RequirePerServiceCallChatHistoryPersistence = true,
            });
#pragma warning restore MAAI001
        EntityHarness harness = CreateHarness(agent, new DurableAgentState());

        await Assert.ThrowsAsync<DurableAgentHistoryOwnershipNotSupportedException>(
            () => harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" }));

        Assert.Equal(0, client.InvocationCount);
        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task LegacyPerServiceCallStateCannotBeAdoptedAsServiceHistoryAsync()
    {
        RecordingChatClient client = new();
#pragma warning disable MAAI001
        ChatClientAgent agent = new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                RequirePerServiceCallChatHistoryPersistence = true,
            });
#pragma warning restore MAAI001
        DurableAgentState legacyState = CreateStateWithExchange("old", "old request", "old response");
        legacyState.Data.Session = await agent.SerializeSessionAsync(
            await agent.CreateSessionAsync("legacy-conversation"));
        EntityHarness harness = CreateHarness(
            agent,
            legacyState,
            options =>
            {
                options.ServiceManagedPerServiceCallHistory = true;
                options.ProviderKey = new("model-service.v1");
            });

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task StaleLocalPerCallSentinelWithCustomProviderFailsBeforeCallbacksAsync()
    {
        RecordingHistoryProvider provider = new();
        RecordingChatClient client = new();
        ChatClientAgent agent = CreateAgentWithProvider(client, provider);
        AgentSession session = await agent.CreateSessionAsync(
            DurableAgentHistoryBinding.FrameworkLocalHistoryConversationId);
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            PersistentRequestOutcomesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
                Session = await agent.SerializeSessionAsync(session),
            },
        };
        EntityHarness harness = CreateHarness(
            agent,
            state,
            options => options.ProviderKey = new("external-history.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ModelInputCompactionRunsWithoutPruningDurableHistoryAsync()
    {
        RecordingChatClient client = new();
        CompactionProvider compactionProvider = new(
            new SlidingWindowCompactionStrategy(
                trigger: _ => true,
                minimumPreservedTurns: 1,
                target: index => index.IncludedTurnCount <= 1),
            stateKey: "durable-compaction");
        ChatClientAgent agent = new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                AIContextProviders =
                [
                    compactionProvider,
                ],
            });
        DurableAgentState state = CreateStateWithExchange("old", "old request", "old response");

        DurableAgentState persisted = await RunEntityAsync(
            agent,
            state,
            new RunRequest("new request") { CorrelationId = "new" });

        Assert.Equal(1, client.InvocationCount);
        Assert.Equal(["new request"], client.LastMessages.Select(message => message.Text));
        Assert.Equal(4, persisted.Data.ConversationHistory.Count);
        JsonElement serializedSession = persisted.Data.Session.GetValueOrDefault();
        Assert.True(
            serializedSession.GetProperty("stateBag").TryGetProperty(
                "durable-compaction",
                out _));
    }

    [Fact]
    public async Task FactoryAgentWithCompactionIsConstructedOnceAndExecutesAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent agent = new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                AIContextProviders =
                [
                    new CompactionProvider(
                        new SlidingWindowCompactionStrategy(_ => true)),
                ],
            });
        int factoryInvocationCount = 0;
        EntityHarness harness = CreateHarness(
            agent,
            new DurableAgentState(),
            registerWithFactory: true,
            onFactoryInvoked: () => factoryInvocationCount++);

        await harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" });

        Assert.Equal(1, factoryInvocationCount);
        Assert.Equal(1, client.InvocationCount);
        Assert.True(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ExplicitInMemoryProviderFailsBeforeModelSideEffectsAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent agent = new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = new InMemoryChatHistoryProvider(
                    new InMemoryChatHistoryProviderOptions
                    {
                        StorageInputRequestMessageFilter = messages => messages.TakeLast(1),
                    }),
            });
        EntityHarness harness = CreateHarness(
            agent,
            new DurableAgentState(),
            registerWithFactory: true);

        await Assert.ThrowsAsync<DurableAgentHistoryOwnershipNotSupportedException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Theory]
    [InlineData(null, true)]
    [InlineData(DurableAgentHistoryReplayMode.PreloadEntityHistory, true)]
    public async Task AgentWithoutContextPipelineAppliesReplayModeAndStorageSemanticsAsync(
        DurableAgentHistoryReplayMode? replayMode,
        bool expectsPreloadedHistory)
    {
        RecordingAgent agent = new("agent");
        DurableAgentState initialState = CreateStateWithExchange("old", "old request", "old response");
        initialState.Data.ConversationHistory.Add(
            new DurableAgentStateErrorResponse
            {
                CorrelationId = "failed",
                CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-2),
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.Assistant, "must not replay")),
                ],
            });
        initialState.Data.ConversationHistory.Add(
            new DurableAgentStateResponse
            {
                CorrelationId = "reasoning",
                CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
                Messages =
                [
                    new DurableAgentStateMessage
                    {
                        Role = ChatRole.Assistant.Value,
                        Contents =
                        [
                            new DurableAgentStateTextReasoningContent { Text = "private reasoning" },
                            new DurableAgentStateTextContent { Text = "visible answer" },
                        ],
                    },
                    new DurableAgentStateMessage
                    {
                        Role = ChatRole.Assistant.Value,
                        Contents =
                        [
                            new DurableAgentStateTextReasoningContent { Text = "reasoning only" },
                        ],
                    },
                ],
            });
        DurableAgentState persisted = await RunEntityAsync(
            agent,
            initialState,
            new RunRequest("new request") { CorrelationId = "new" },
            options =>
            {
                if (replayMode.HasValue)
                {
                    options.ReplayMode = replayMode.Value;
                }
            });

        Assert.Equal(
            expectsPreloadedHistory
                ? ["old request", "old response", "visible answer", "new request"]
                : ["new request"],
            agent.LastMessages.Select(message => message.Text));
        Assert.DoesNotContain(
            agent.LastMessages.SelectMany(message => message.Contents),
            content => content is TextReasoningContent);
        Assert.True(expectsPreloadedHistory);
        Assert.Equal(6, persisted.Data.ConversationHistory.Count);
        DurableAgentStateRequest storedRequest =
            Assert.IsType<DurableAgentStateRequest>(persisted.Data.ConversationHistory[^2]);
        DurableAgentStateMessage storedMessage = Assert.Single(storedRequest.Messages);
        Assert.Equal("new request", Assert.IsType<DurableAgentStateTextContent>(
            Assert.Single(storedMessage.Contents)).Text);
        Assert.IsType<DurableAgentStateResponse>(persisted.Data.ConversationHistory[^1]);
        Assert.Equal(DurableAgentStateHistoryBinding.DurableStateOwner, GetBinding(persisted)?.OwnerKind);

        Assert.Equal(4, persisted.Data.TerminalResults?.Count);
        Assert.Contains("new", persisted.Data.TerminalResults!.Keys);
        Assert.NotNull(persisted.Data.Session);
    }

    [Fact]
    public async Task CurrentRequestOnlyDoesNotOverrideDiscoverableEntityHistoryAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent agent = new(client, name: "agent");
        DurableAgentState initialState = CreateStateWithExchange("old", "old request", "old response");

        DurableAgentState persisted = await RunEntityAsync(
            agent,
            initialState,
            new RunRequest("new request") { CorrelationId = "new" },
            options => options.ReplayMode = DurableAgentHistoryReplayMode.CurrentRequestOnly);

        Assert.Equal(
            ["old request", "old response", "new request"],
            client.LastMessages.Select(message => message.Text));
        Assert.Equal(DurableAgentStateHistoryBinding.DurableStateOwner, GetBinding(persisted)?.OwnerKind);
    }

    [Fact]
    public async Task CurrentRequestOnlySealsOpaqueOwnerAndSurvivesColdReloadAsync()
    {
        RecordingAgent firstAgent = new("agent");
        DurableAgentState firstWrite = await RunEntityAsync(
            firstAgent,
            new DurableAgentState(),
            new RunRequest("first") { CorrelationId = "first" },
            options =>
            {
                options.ReplayMode = DurableAgentHistoryReplayMode.CurrentRequestOnly;
                options.ProviderKey = new("opaque-agent-session.v1");
            });

        RecordingAgent secondAgent = new("agent");
        DurableAgentState secondWrite = await RunEntityAsync(
            secondAgent,
            DeserializeState(SerializeState(firstWrite)),
            new RunRequest("second") { CorrelationId = "second" },
            options =>
            {
                options.ReplayMode = DurableAgentHistoryReplayMode.CurrentRequestOnly;
                options.ProviderKey = new("opaque-agent-session.v1");
            });

        Assert.Equal(["first"], firstAgent.LastMessages.Select(message => message.Text));
        Assert.Equal(["second"], secondAgent.LastMessages.Select(message => message.Text));
        Assert.Empty(secondWrite.Data.ConversationHistory);
        Assert.Equal(2, secondWrite.Data.TerminalResults?.Count);
        Assert.Equal(DurableAgentStateHistoryBinding.HistoryProviderOwner, GetBinding(secondWrite)?.OwnerKind);
        Assert.Equal("opaque-agent-session.v1", GetBinding(secondWrite)?.ProviderKey);
    }

    [Fact]
    public async Task LegacyCurrentRequestOnlySessionCannotBeAdoptedWithoutPriorBindingAsync()
    {
        RecordingAgent agent = new("agent");
        DurableAgentState legacyState = CreateStateWithExchange("old", "old request", "old response");
        legacyState.Data.Session =
            await agent.SerializeSessionAsync(await agent.CreateSessionAsync());
        EntityHarness harness = CreateHarness(
            agent,
            legacyState,
            options =>
            {
                options.ReplayMode = DurableAgentHistoryReplayMode.CurrentRequestOnly;
                options.ProviderKey = new("opaque-agent-session.v1");
            });

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Empty(agent.LastMessages);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task AgentWithoutContextPipelineDoesNotReplayErrorResponsesAsync()
    {
        RecordingAgent agent = new("agent");
        DurableAgentState initialState = CreateStateWithExchange("old", "old request", "old response");
        initialState.Data.ConversationHistory.Add(
            new DurableAgentStateErrorResponse
            {
                CorrelationId = "failed",
                CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.Assistant, "must not replay")),
                ],
            });

        _ = await RunEntityAsync(
            agent,
            initialState,
            new RunRequest("new request") { CorrelationId = "new" });

        Assert.Equal(["old request", "old response", "new request"], agent.LastMessages.Select(message => message.Text));
    }

    [Fact]
    public async Task AgentWithoutContextPipelineFiltersReasoningFromReplayAsync()
    {
        RecordingAgent agent = new("agent");
        DurableAgentState initialState = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            PersistentRequestOutcomesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
            },
        };
        initialState.Data.ConversationHistory.Add(
            new DurableAgentStateResponse
            {
                CorrelationId = "old",
                CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
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

        _ = await RunEntityAsync(
            agent,
            initialState,
            new RunRequest("new request") { CorrelationId = "new" });

        Assert.Equal(["visible answer", "new request"], agent.LastMessages.Select(message => message.Text));
        Assert.DoesNotContain(
            agent.LastMessages.SelectMany(message => message.Contents),
            content => content is TextReasoningContent);
    }

    [Fact]
    public async Task EntityPreservesButDoesNotCreateCompactionEntriesAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent agent = new(client, name: "agent");
        DurableAgentState initialState = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            PersistentRequestOutcomesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
            },
        };
        initialState.Data.ConversationHistory.Add(
            new DurableAgentStateCompaction
            {
                CreatedAt = DateTimeOffset.UtcNow.AddMinutes(-1),
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.Assistant, "shared summary")),
                ],
            });

        DurableAgentState persisted = await RunEntityAsync(
            agent,
            initialState,
            new RunRequest("new request") { CorrelationId = "new" });

        Assert.Equal(["shared summary", "new request"], client.LastMessages.Select(message => message.Text));
        DurableAgentStateCompaction compaction =
            Assert.Single(persisted.Data.ConversationHistory.OfType<DurableAgentStateCompaction>());
        Assert.Equal("shared summary", compaction.Messages[0].ToChatMessage().Text);
        Assert.Equal(3, persisted.Data.ConversationHistory.Count);
    }

    [Fact]
    public async Task WrappedServerManagedSessionSurvivesColdEntityInvocationAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent chatAgent = new(client, name: "agent");
        AIAgent wrappedAgent = new TestDelegatingAgent(chatAgent);
        AgentSession serviceSession = await chatAgent.CreateSessionAsync("service-conversation");
        serviceSession.StateBag.SetValue("opaque-server-state", "preserved");
        DurableAgentState initialState = new()
        {
            Data =
            {
                Session = await wrappedAgent.SerializeSessionAsync(serviceSession),
            },
        };

        DurableAgentState persisted = await RunEntityAsync(
            wrappedAgent,
            initialState,
            new RunRequest("new request") { CorrelationId = "new" },
            options => options.ProviderKey = new("model-service.v1"));
        AgentSession restored = await wrappedAgent.DeserializeSessionAsync(
            persisted.Data.Session!.Value);
        ChatClientAgentSession restoredTyped = Assert.IsType<ChatClientAgentSession>(restored);

        Assert.Equal(["new request"], client.LastMessages.Select(message => message.Text));
        Assert.Empty(persisted.Data.ConversationHistory);
        Assert.Equal(DurableAgentStateHistoryBinding.ModelServiceOwner, GetBinding(persisted)?.OwnerKind);
        Assert.Equal("service-conversation", restoredTyped.ConversationId);
        Assert.Equal("preserved", restoredTyped.StateBag.GetValue<string>("opaque-server-state"));
    }

    [Fact]
    public async Task LegacyMessageIdsPersistAcrossProviderLoadAndColdReloadAsync()
    {
        const string Json = """
            {
              "schemaVersion": "1.1.0",
              "data": {
                "conversationHistory": [
                  {
                    "$type": "request",
                    "correlationId": "old",
                    "createdAt": "2026-07-27T12:34:50+00:00",
                    "messages": [
                      {
                        "role": "user",
                        "contents": [{ "$type": "text", "text": "old request" }]
                      },
                      {
                        "role": "user",
                        "messageId": "producer-id",
                        "contents": [{ "$type": "text", "text": "preserved" }]
                      }
                    ]
                  },
                  {
                    "$type": "response",
                    "correlationId": "old",
                    "createdAt": "2026-07-27T12:34:51+00:00",
                    "messages": [
                      {
                        "role": "assistant",
                        "contents": []
                      },
                      {
                        "role": "assistant",
                        "contents": [{ "$type": "text", "text": "old response" }]
                      }
                    ]
                  }
                ]
              }
            }
            """;
        DurableAgentState initialState = Assert.IsType<DurableAgentState>(
            JsonSerializer.Deserialize(Json, DurableAgentStateJsonContext.Default.DurableAgentState));
        RecordingChatClient client = new();
        ChatClientAgent agent = new(client, name: "agent");

        DurableAgentState firstWrite = await RunEntityAsync(
            agent,
            initialState,
            new RunRequest("first new request") { CorrelationId = "new-1" });
        string serialized = JsonSerializer.Serialize(
            firstWrite,
            DurableAgentStateJsonContext.Default.DurableAgentState);
        DurableAgentState coldState = Assert.IsType<DurableAgentState>(
            JsonSerializer.Deserialize(serialized, DurableAgentStateJsonContext.Default.DurableAgentState));
        string?[] firstIds = firstWrite.Data.ConversationHistory
            .Take(2)
            .SelectMany(entry => entry.Messages)
            .Select(message => message.MessageId)
            .ToArray();

        DurableAgentState secondWrite = await RunEntityAsync(
            agent,
            coldState,
            new RunRequest("second new request") { CorrelationId = "new-2" });
        string?[] secondIds = secondWrite.Data.ConversationHistory
            .Take(2)
            .SelectMany(entry => entry.Messages)
            .Select(message => message.MessageId)
            .ToArray();

        string?[] expectedIds =
            ["durable_request_old_0", "producer-id", "durable_response_old_0", "durable_response_old_1"];
        Assert.Equal(expectedIds, firstIds);
        Assert.Equal(firstIds, secondIds);
        Assert.Contains("\"messageId\":\"durable_response_old_1\"", serialized, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FailedFirstTurnDoesNotSealHistoryBindingOrMailboxAsync()
    {
        RecordingChatClient client = new() { Exception = new InvalidOperationException("model failed") };
        ChatClientAgent agent = new(client, name: "agent");
        DurableAgentState initialState = new();
        EntityHarness harness = CreateHarness(agent, initialState);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" }));

        Assert.False(harness.StateWasPersisted);
        Assert.Equal(JsonValueKind.Undefined, initialState.Data.HistoryBinding.ValueKind);
        Assert.Null(initialState.Data.TerminalResults);
        Assert.Null(initialState.Data.CompletionReceipts);
    }

    [Fact]
    public async Task RecreatedExternalProviderWithSameLogicalKeyContinuesWithoutTranscriptMirrorAsync()
    {
        RecordingHistoryProvider firstProvider = new();
        ChatClientAgent firstAgent = CreateAgentWithProvider(new RecordingChatClient(), firstProvider);
        DurableAgentState firstWrite = await RunEntityAsync(
            firstAgent,
            new DurableAgentState(),
            new RunRequest("first") { CorrelationId = "first" },
            options => options.ProviderKey = new("external-history.v1"));
        DurableAgentState coldState = DeserializeState(SerializeState(firstWrite));

        RecordingHistoryProvider secondProvider = new();
        RecordingChatClient secondClient = new();
        ChatClientAgent secondAgent = CreateAgentWithProvider(secondClient, secondProvider);
        DurableAgentState secondWrite = await RunEntityAsync(
            secondAgent,
            coldState,
            new RunRequest("second") { CorrelationId = "second" },
            options => options.ProviderKey = new("external-history.v1"));

        Assert.Equal(1, secondProvider.LoadCount);
        Assert.Equal(1, secondProvider.StoreCount);
        Assert.Equal(["second"], secondClient.LastMessages.Select(message => message.Text));
        Assert.Empty(secondWrite.Data.ConversationHistory);
        Assert.Equal(2, secondWrite.Data.TerminalResults?.Count);
        Assert.Equal(2, secondWrite.Data.CompletionReceipts?.Count);
        Assert.Equal("external-history.v1", GetBinding(secondWrite)?.ProviderKey);
        Assert.True(
            secondWrite.Data.Session?.GetProperty("stateBag").TryGetProperty("external-history", out _) is true);
    }

    [Fact]
    public async Task ChangedExternalProviderKeyRejectsBeforeProviderOrModelCallbacksAsync()
    {
        ChatClientAgent firstAgent = CreateAgentWithProvider(
            new RecordingChatClient(),
            new RecordingHistoryProvider());
        DurableAgentState persisted = await RunEntityAsync(
            firstAgent,
            new DurableAgentState(),
            new RunRequest("first") { CorrelationId = "first" },
            options => options.ProviderKey = new("external-history.v1"));

        RecordingHistoryProvider replacementProvider = new();
        RecordingChatClient replacementClient = new();
        CountingSessionAgent replacementAgent = new(CreateAgentWithProvider(
            replacementClient,
            replacementProvider));
        int factoryInvocationCount = 0;
        EntityHarness harness = CreateHarness(
            replacementAgent,
            DeserializeState(SerializeState(persisted)),
            options => options.ProviderKey = new("external-history.v2"),
            registerWithFactory: true,
            onFactoryInvoked: () => factoryInvocationCount++);

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("second") { CorrelationId = "second" }));

        Assert.Equal(0, factoryInvocationCount);
        Assert.Equal(0, replacementAgent.DeserializeCount);
        Assert.Equal(0, replacementProvider.LoadCount);
        Assert.Equal(0, replacementProvider.StoreCount);
        Assert.Equal(0, replacementClient.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ChangedProvisionalExternalProviderKeyRejectsBeforeAgentConstructionAsync()
    {
        RecordingHistoryProvider provider = new();
        RecordingChatClient client = new();
        ChatClientAgent agent = CreateAgentWithProvider(client, provider);
        DurableAgentState state = await CreateProvisionalExternalStateAsync(
            agent,
            "external-history.v1");
        int factoryInvocationCount = 0;
        EntityHarness harness = CreateHarness(
            agent,
            state,
            options => options.ProviderKey = new("external-history.v2"),
            registerWithFactory: true,
            onFactoryInvoked: () => factoryInvocationCount++);

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Equal(0, factoryInvocationCount);
        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ColdDuplicateBypassesProvisionalProviderKeyRevalidationAsync()
    {
        RecordingHistoryProvider firstProvider = new();
        DurableAgentState persisted = await RunEntityAsync(
            CreateAgentWithProvider(new RecordingChatClient(), firstProvider),
            new DurableAgentState(),
            new RunRequest("first") { CorrelationId = "first" },
            options => options.ProviderKey = new("external-history.v1"));
        DurableAgentStateHistoryBinding binding = GetBinding(persisted)!;
        DurableAgentState provisional = CopyState(
            persisted,
            persisted.Data.Session,
            new DurableAgentStateHistoryBinding
            {
                OwnerKind = binding.OwnerKind,
                ProviderKey = binding.ProviderKey,
            });

        RecordingHistoryProvider duplicateProvider = new();
        RecordingChatClient duplicateClient = new();
        ChatClientAgent duplicateAgent = CreateAgentWithProvider(
            duplicateClient,
            duplicateProvider);
        int factoryInvocationCount = 0;
        EntityHarness duplicateHarness = CreateHarness(
            duplicateAgent,
            DeserializeState(SerializeState(provisional)),
            options => options.ProviderKey = new("external-history.v2"),
            registerWithFactory: true,
            onFactoryInvoked: () => factoryInvocationCount++);

        AgentResponse duplicate = await duplicateHarness.RunAsync(
            new RunRequest([]) { CorrelationId = "first" });

        Assert.Equal("response", duplicate.Text);
        Assert.Equal(0, factoryInvocationCount);
        Assert.Equal(0, duplicateProvider.LoadCount);
        Assert.Equal(0, duplicateProvider.StoreCount);
        Assert.Equal(0, duplicateClient.InvocationCount);
    }

    [Fact]
    public async Task ChangedOwnerRejectsBeforeExternalProviderOrModelCallbacksAsync()
    {
        ChatClientAgent entityAgent = new(new RecordingChatClient(), name: "agent");
        DurableAgentState persisted = await RunEntityAsync(
            entityAgent,
            new DurableAgentState(),
            new RunRequest("first") { CorrelationId = "first" });

        RecordingHistoryProvider replacementProvider = new();
        RecordingChatClient replacementClient = new();
        ChatClientAgent replacementAgent = CreateAgentWithProvider(
            replacementClient,
            replacementProvider);
        EntityHarness harness = CreateHarness(
            replacementAgent,
            DeserializeState(SerializeState(persisted)),
            options => options.ProviderKey = new("external-history.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("second") { CorrelationId = "second" }));

        Assert.Equal(0, replacementProvider.LoadCount);
        Assert.Equal(0, replacementClient.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task MissingExternalContinuationNeverCreatesReplacementConversationAsync()
    {
        RecordingHistoryProvider firstProvider = new();
        ChatClientAgent firstAgent = CreateAgentWithProvider(new RecordingChatClient(), firstProvider);
        DurableAgentState persisted = await RunEntityAsync(
            firstAgent,
            new DurableAgentState(),
            new RunRequest("first") { CorrelationId = "first" },
            options => options.ProviderKey = new("external-history.v1"));
        DurableAgentState missingContinuation = CopyState(persisted, session: null);

        RecordingHistoryProvider replacementProvider = new();
        RecordingChatClient replacementClient = new();
        EntityHarness harness = CreateHarness(
            CreateAgentWithProvider(replacementClient, replacementProvider),
            missingContinuation,
            options => options.ProviderKey = new("external-history.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("second") { CorrelationId = "second" }));

        Assert.Equal(0, replacementProvider.LoadCount);
        Assert.Equal(0, replacementClient.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task BoundExternalProviderRejectsSessionWithMissingDeclaredStateBeforeCallbacksAsync()
    {
        DurableAgentState persisted = await CreateBoundExternalStateAsync();
        RecordingHistoryProvider replacementProvider = new();
        RecordingChatClient replacementClient = new();
        ChatClientAgent replacementAgent = CreateAgentWithProvider(
            replacementClient,
            replacementProvider);
        JsonElement emptySession = await replacementAgent.SerializeSessionAsync(
            await replacementAgent.CreateSessionAsync());
        EntityHarness harness = CreateHarness(
            replacementAgent,
            CopyState(persisted, emptySession),
            options => options.ProviderKey = new("external-history.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("second") { CorrelationId = "second" }));

        Assert.Equal(0, replacementProvider.LoadCount);
        Assert.Equal(0, replacementClient.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Theory]
    [InlineData("null")]
    [InlineData("\"\"")]
    [InlineData("\"   \"")]
    [InlineData("false")]
    [InlineData("{}")]
    [InlineData("[]")]
    public async Task BoundExternalProviderRejectsInvalidDeclaredStateBeforeCallbacksAsync(
        string continuationJson)
    {
        DurableAgentState persisted = await CreateBoundExternalStateAsync();
        RecordingHistoryProvider replacementProvider = new();
        RecordingChatClient replacementClient = new();
        ChatClientAgent replacementAgent = CreateAgentWithProvider(
            replacementClient,
            replacementProvider);
        AgentSession session = await replacementAgent.CreateSessionAsync();
        using JsonDocument continuation = JsonDocument.Parse(continuationJson);
        session.StateBag.SetValue<object>(
            "external-history",
            continuation.RootElement.Clone());
        JsonElement invalidSession =
            await replacementAgent.SerializeSessionAsync(session);
        EntityHarness harness = CreateHarness(
            replacementAgent,
            CopyState(persisted, invalidSession),
            options => options.ProviderKey = new("external-history.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("second") { CorrelationId = "second" }));

        Assert.Equal(0, replacementProvider.LoadCount);
        Assert.Equal(0, replacementProvider.StoreCount);
        Assert.Equal(0, replacementClient.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ExternalProviderCannotSealWithoutItsDeclaredContinuationStateAsync()
    {
        RecordingHistoryProvider provider = new() { SkipContinuationWrite = true };
        RecordingChatClient client = new();
        EntityHarness harness = CreateHarness(
            CreateAgentWithProvider(client, provider),
            new DurableAgentState(),
            options => options.ProviderKey = new("external-history.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("first") { CorrelationId = "first" }));

        Assert.Equal(1, provider.LoadCount);
        Assert.Equal(1, provider.StoreCount);
        Assert.Equal(1, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ExternalProviderCannotSealNullDeclaredContinuationStateAsync()
    {
        RecordingHistoryProvider provider = new()
        {
            ContinuationValue = JsonSerializer.SerializeToElement<object?>(null),
        };
        RecordingChatClient client = new();
        EntityHarness harness = CreateHarness(
            CreateAgentWithProvider(client, provider),
            new DurableAgentState(),
            options => options.ProviderKey = new("external-history.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("first") { CorrelationId = "first" }));

        Assert.Equal(1, provider.LoadCount);
        Assert.Equal(1, provider.StoreCount);
        Assert.Equal(1, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ExternalProviderWithoutStateKeysFailsBeforeCallbacksAsync()
    {
        EmptyStateKeysHistoryProvider provider = new();
        RecordingChatClient client = new();
        EntityHarness harness = CreateHarness(
            new ChatClientAgent(
                client,
                new ChatClientAgentOptions
                {
                    Name = "agent",
                    ChatHistoryProvider = provider,
                }),
            new DurableAgentState(),
            options => options.ProviderKey = new("empty-provider.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("first") { CorrelationId = "first" }));

        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task DefaultSchemaWritesPreserveExplicitInMemoryProviderCompatibilityAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent agent = new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = new InMemoryChatHistoryProvider(
                    new InMemoryChatHistoryProviderOptions
                    {
                        StorageInputRequestMessageFilter = messages => messages.TakeLast(1),
                    }),
            });
        EntityHarness harness = CreateHarness(
            agent,
            new DurableAgentState(),
            enableMailboxWrites: false);

        await harness.RunAsync(new RunRequest("new") { CorrelationId = "new" });

        DurableAgentState persisted = Assert.IsType<DurableAgentState>(harness.PersistedState);
        Assert.Equal(1, client.InvocationCount);
        Assert.Equal(2, persisted.Data.ConversationHistory.Count);
        Assert.Equal(JsonValueKind.Undefined, persisted.Data.HistoryBinding.ValueKind);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("configured-history")]
    public async Task LegacyExplicitInMemoryProviderHasOneHistorySourceAcrossThreeColdTurnsAsync(
        string? stateKey)
    {
        DurableAgentState state = new();
        List<(ChatRole Role, string Text)> expectedMessages = [];
        for (int turn = 1; turn <= 3; turn++)
        {
            RecordingChatClient client = new();
            InMemoryChatHistoryProvider provider = stateKey is null
                ? new()
                : new(new InMemoryChatHistoryProviderOptions { StateKey = stateKey });
            ChatClientAgent agent = new(
                client,
                new ChatClientAgentOptions
                {
                    Name = "agent",
                    ChatHistoryProvider = provider,
                });
            EntityHarness harness = CreateHarness(agent, state, enableMailboxWrites: false);
            string request = $"request {turn}";

            await harness.RunAsync(new RunRequest(request) { CorrelationId = $"c{turn}" });

            state = DeserializeState(
                SerializeState(Assert.IsType<DurableAgentState>(harness.PersistedState)));
            expectedMessages.Add((ChatRole.User, request));
            Assert.Equal(
                expectedMessages,
                client.LastMessages.Select(message => (message.Role, message.Text)));
            Assert.Equal(1, client.InvocationCount);
            Assert.Equal(DurableAgentState.CurrentSchemaVersion, state.SchemaVersion);
            Assert.Equal(turn * 2, state.Data.ConversationHistory.Count);
            AgentSession restored = await agent.DeserializeSessionAsync(state.Data.Session!.Value);
            expectedMessages.Add((ChatRole.Assistant, "response"));
            Assert.Equal(
                expectedMessages,
                provider.GetMessages(restored).Select(message => (message.Role, message.Text)));
        }
    }

    [Theory]
    [InlineData(null)]
    [InlineData("configured-history")]
    public async Task LegacyExplicitInMemoryProviderKeepsEmptyFilteredHistoryAcrossColdTurnsAsync(
        string? stateKey)
    {
        DurableAgentState state = new();
        for (int turn = 1; turn <= 3; turn++)
        {
            RecordingChatClient client = new();
            InMemoryChatHistoryProvider provider = new(
                new InMemoryChatHistoryProviderOptions
                {
                    StateKey = stateKey,
                    StorageInputRequestMessageFilter = _ => [],
                    StorageInputResponseMessageFilter = _ => [],
                });
            ChatClientAgent agent = new(
                client,
                new ChatClientAgentOptions { Name = "agent", ChatHistoryProvider = provider });
            EntityHarness harness = CreateHarness(agent, state, enableMailboxWrites: false);
            string request = $"request {turn}";

            await harness.RunAsync(new RunRequest(request) { CorrelationId = $"c{turn}" });

            Assert.Equal([request], client.LastMessages.Select(message => message.Text));
            Assert.Equal(1, client.InvocationCount);
            state = DeserializeState(
                SerializeState(Assert.IsType<DurableAgentState>(harness.PersistedState)));
            AgentSession restored = await agent.DeserializeSessionAsync(state.Data.Session!.Value);
            Assert.Empty(provider.GetMessages(restored));
            Assert.Equal(turn * 2, state.Data.ConversationHistory.Count);
        }
    }

    [Fact]
    public async Task LegacyExplicitInMemoryProviderToolLoopDoesNotDuplicateHistoryOrCallbacksAsync()
    {
        int toolCalls = 0;
        int loadCallbacks = 0;
        int storeCallbacks = 0;
        AIFunction tool = AIFunctionFactory.Create(
            (string value) =>
            {
                toolCalls++;
                return $"tool result: {value}";
            },
            name: "echo");
        InMemoryChatHistoryProvider CreateProvider() => new(
            new InMemoryChatHistoryProviderOptions
            {
                ProvideOutputMessageFilter = messages =>
                {
                    loadCallbacks++;
                    return messages;
                },
                StorageInputRequestMessageFilter = messages =>
                {
                    storeCallbacks++;
                    return messages;
                },
            });
        ToolLoopChatClient firstClient = new(tool.Name);
        InMemoryChatHistoryProvider firstProvider = CreateProvider();
        ChatClientAgent firstAgent = new(
            firstClient,
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = firstProvider,
                ChatOptions = new ChatOptions { Tools = [tool] },
            });
        EntityHarness firstHarness = CreateHarness(firstAgent, new DurableAgentState(), enableMailboxWrites: false);
        await firstHarness.RunAsync(new RunRequest("first request") { CorrelationId = "first" });
        Assert.Equal(2, firstClient.InvocationCount);
        Assert.Equal(1, toolCalls);
        Assert.Equal(1, loadCallbacks);
        Assert.Equal(1, storeCallbacks);
        DurableAgentState state = DeserializeState(
            SerializeState(Assert.IsType<DurableAgentState>(firstHarness.PersistedState)));
        AgentSession restored = await firstAgent.DeserializeSessionAsync(state.Data.Session!.Value);
        List<ChatMessage> expectedMessages = firstProvider.GetMessages(restored).ToList();
        Assert.Equal([ChatRole.User, ChatRole.Assistant, ChatRole.Tool, ChatRole.Assistant], expectedMessages.Select(message => message.Role));
        Assert.IsType<FunctionCallContent>(Assert.Single(expectedMessages[1].Contents));
        Assert.IsType<FunctionResultContent>(Assert.Single(expectedMessages[2].Contents));

        RecordingChatClient secondClient = new();
        ChatClientAgent secondAgent = new(
            secondClient,
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = CreateProvider(),
                ChatOptions = new ChatOptions { Tools = [tool] },
            });
        EntityHarness secondHarness = CreateHarness(secondAgent, state, enableMailboxWrites: false);
        await secondHarness.RunAsync(new RunRequest("second request") { CorrelationId = "second" });
        expectedMessages.Add(new ChatMessage(ChatRole.User, "second request"));
        Assert.Equal(
            expectedMessages.Select(message => (message.Role, message.Text)),
            secondClient.LastMessages.Select(message => (message.Role, message.Text)));
        Assert.Single(secondClient.LastMessages.SelectMany(message => message.Contents).OfType<FunctionCallContent>());
        Assert.Single(secondClient.LastMessages.SelectMany(message => message.Contents).OfType<FunctionResultContent>());
        Assert.Equal(1, secondClient.InvocationCount);
        Assert.Equal(1, toolCalls);
        Assert.Equal(2, loadCallbacks);
        Assert.Equal(2, storeCallbacks);

        DurableAgentState secondWrite = DeserializeState(
            SerializeState(Assert.IsType<DurableAgentState>(secondHarness.PersistedState)));
        EntityHarness duplicateHarness = CreateHarness(secondAgent, secondWrite, enableMailboxWrites: false);
        await duplicateHarness.RunAsync(new RunRequest("second request") { CorrelationId = "second" });
        Assert.Equal(1, secondClient.InvocationCount);
        Assert.Equal(1, toolCalls);
        Assert.Equal(2, loadCallbacks);
        Assert.Equal(2, storeCallbacks);
        Assert.Equal(
            SerializeState(secondWrite),
            SerializeState(Assert.IsType<DurableAgentState>(duplicateHarness.PersistedState)));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("configured-history")]
    public async Task LegacyInMemoryProviderPreservesReducedFilteredHistoryAcrossColdTurnsAsync(
        string? stateKey)
    {
        DurableAgentState state = new();
        string? previousRequest = null;
        for (int turn = 1; turn <= 3; turn++)
        {
            RecordingChatClient client = new();
            SummarizingChatReducer reducer = new();
            InMemoryChatHistoryProvider provider = new(
                new InMemoryChatHistoryProviderOptions
                {
                    StateKey = stateKey,
                    StateInitializer = _ => new InMemoryChatHistoryProvider.State
                    {
                        Messages = [new ChatMessage(ChatRole.Assistant, "provider initialization")],
                    },
                    ChatReducer = reducer,
                    ReducerTriggerEvent =
                        InMemoryChatHistoryProviderOptions.ChatReducerTriggerEvent.AfterMessageAdded,
                    StorageInputRequestMessageFilter = messages => messages.TakeLast(1),
                    ProvideOutputMessageFilter = messages =>
                        messages.Where(message => message.Role == ChatRole.System),
                });
            ChatClientAgent agent = new(
                client,
                new ChatClientAgentOptions
                {
                    Name = "agent",
                    ChatHistoryProvider = provider,
                });
            EntityHarness harness = CreateHarness(
                agent,
                state,
                enableMailboxWrites: false);
            string request = $"request {turn}";

            await harness.RunAsync(
                new RunRequest(
                    [
                        new ChatMessage(ChatRole.User, $"filtered request {turn}"),
                        new ChatMessage(ChatRole.User, request),
                    ])
                {
                    CorrelationId = $"turn-{turn}",
                });

            state = DeserializeState(
                SerializeState(Assert.IsType<DurableAgentState>(harness.PersistedState)));
            Assert.Equal(DurableAgentState.CurrentSchemaVersion, state.SchemaVersion);
            Assert.Equal(JsonValueKind.Undefined, state.Data.HistoryBinding.ValueKind);
            Assert.Equal(turn * 2, state.Data.ConversationHistory.Count);
            Assert.Null(state.Data.TerminalResults);
            Assert.Null(state.Data.CompletionReceipts);
            Assert.Equal(
                previousRequest is null
                    ? ["provider initialization", request, "response"]
                    : [$"summary: {previousRequest}", "response", request, "response"],
                reducer.LastMessages.Select(message => message.Text));
            Assert.Equal(
                previousRequest is null
                    ? [$"filtered request {turn}", request]
                    : [$"summary: {previousRequest}", $"filtered request {turn}", request],
                client.LastMessages.Select(message => message.Text));
            Assert.Equal(1, client.InvocationCount);
            Assert.Equal(1, reducer.InvocationCount);

            JsonElement sessionState = state.Data.Session!.Value;
            foreach (string key in provider.StateKeys)
            {
                Assert.True(sessionState.GetProperty("stateBag").TryGetProperty(key, out _));
            }

            AgentSession restored = await agent.DeserializeSessionAsync(sessionState);
            Assert.Equal(
                [$"summary: {request}", "response"],
                provider.GetMessages(restored).Select(message => message.Text));
            previousRequest = request;
        }
    }

    [Fact]
    public async Task ExternalProviderRequiresEveryDeclaredContinuationKeyAsync()
    {
        MultiKeyHistoryProvider provider = new(writeSecondKey: false);
        RecordingChatClient client = new();
        EntityHarness harness = CreateHarness(
            new ChatClientAgent(
                client,
                new ChatClientAgentOptions
                {
                    Name = "agent",
                    ChatHistoryProvider = provider,
                }),
            new DurableAgentState(),
            options => options.ProviderKey = new("multi-key-history.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("first") { CorrelationId = "first" }));

        Assert.Equal(1, provider.StoreCount);
        Assert.Equal(1, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ExternalProviderSealsWhenEveryDeclaredContinuationKeyExistsAsync()
    {
        MultiKeyHistoryProvider provider = new(writeSecondKey: true);
        RecordingChatClient client = new();
        DurableAgentState persisted = await RunEntityAsync(
            new ChatClientAgent(
                client,
                new ChatClientAgentOptions
                {
                    Name = "agent",
                    ChatHistoryProvider = provider,
                }),
            new DurableAgentState(),
            new RunRequest("first") { CorrelationId = "first" },
            options => options.ProviderKey = new("multi-key-history.v1"));

        Assert.Equal(1, provider.StoreCount);
        Assert.Equal("multi-key-history.v1", GetBinding(persisted)?.ProviderKey);
    }

    [Fact]
    public async Task AmbiguousLegacyExternalOwnershipFailsBeforeCallbacksAsync()
    {
        RecordingHistoryProvider provider = new();
        RecordingChatClient client = new();
        ChatClientAgent agent = CreateAgentWithProvider(client, provider);
        DurableAgentState legacyState = CreateStateWithExchange("old", "old request", "old response");
        EntityHarness harness = CreateHarness(
            agent,
            legacyState,
            options => options.ProviderKey = new("external-history.v1"));

        DurableAgentHistoryBindingMismatchException exception =
            await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
                () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Contains("Legacy durable state", exception.Message, StringComparison.Ordinal);
        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task UnboundSchema2EntityTranscriptCannotSwitchToExternalProviderAsync()
    {
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            PersistentRequestOutcomesAuthorized = true,
            Data = new DurableAgentStateData
            {
                ConversationHistory =
                [
                    DurableAgentStateRequest.FromRunRequestV2(
                        new RunRequest("old request") { CorrelationId = "old" }),
                    DurableAgentStateResponse.FromResponseV2(
                        "old",
                        new AgentResponse(
                            new ChatMessage(ChatRole.Assistant, "old response"))),
                ],
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
            },
        };
        RecordingHistoryProvider provider = new();
        RecordingChatClient client = new();
        EntityHarness harness = CreateHarness(
            CreateAgentWithProvider(client, provider),
            state,
            options => options.ProviderKey = new("external-history.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task PrunedSchema2MailboxCannotAdoptExternalProviderAsync(
        bool resultAvailable)
    {
        DateTimeOffset completedAt = DateTimeOffset.UtcNow.AddMinutes(-5);
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            PersistentRequestOutcomesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = resultAvailable
                    ? new Dictionary<string, DurableAgentStateTerminalResult>
                    {
                        ["old"] = DurableAgentStateTerminalResult.FromResponse(
                            "old",
                            new AgentResponse(
                                new ChatMessage(ChatRole.Assistant, "old response")),
                            completedAt),
                    }
                    : new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>
                {
                    ["old"] = new()
                    {
                        CorrelationId = "old",
                        Outcome = DurableAgentStateCompletionReceipt.SucceededOutcome,
                        CompletedAt = completedAt,
                        ResultState = resultAvailable
                            ? DurableAgentStateCompletionReceipt.AvailableResult
                            : DurableAgentStateCompletionReceipt.UnavailableResult,
                        ResultUnavailableAt = resultAvailable
                            ? null
                            : completedAt.AddMinutes(1),
                    },
                },
            },
        };
        RecordingHistoryProvider provider = new();
        RecordingChatClient client = new();
        EntityHarness harness = CreateHarness(
            CreateAgentWithProvider(client, provider),
            state,
            options => options.ProviderKey = new("external-history.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ProvisionalExternalBindingWithoutContinuationCannotAdoptProviderAsync()
    {
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            PersistentRequestOutcomesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
                HistoryBinding = DurableAgentHistoryBinding.ToJson(
                    new DurableAgentStateHistoryBinding
                    {
                        OwnerKind = DurableAgentStateHistoryBinding.HistoryProviderOwner,
                        ProviderKey = "external-history.v1",
                    }),
            },
        };
        RecordingHistoryProvider provider = new();
        RecordingChatClient client = new();
        EntityHarness harness = CreateHarness(
            CreateAgentWithProvider(client, provider),
            state,
            options => options.ProviderKey = new("external-history.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task UnsealedEntitySessionWithInMemoryOnlyHistoryFailsBeforeModelAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent agent = new(client, name: "agent");
        AgentSession session = await agent.CreateSessionAsync();
        session.StateBag.SetValue(
            nameof(InMemoryChatHistoryProvider),
            new InMemoryChatHistoryProvider.State
            {
                Messages = [new ChatMessage(ChatRole.User, "session-only history")],
            });
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            PersistentRequestOutcomesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
                Session = await agent.SerializeSessionAsync(session),
            },
        };
        EntityHarness harness = CreateHarness(agent, state);

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ProvisionalExternalKeyRemainsUsableAfterCSharpSealWithoutExplicitOptionAsync()
    {
        RecordingHistoryProvider provider = new();
        RecordingChatClient firstClient = new();
        ChatClientAgent firstAgent = CreateAgentWithProvider(firstClient, provider);
        AgentSession session = await firstAgent.CreateSessionAsync();
        session.StateBag.SetValue(
            "external-history",
            new ExternalHistoryState { Count = 4 });
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            PersistentRequestOutcomesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
                HistoryBinding = DurableAgentHistoryBinding.ToJson(
                    new DurableAgentStateHistoryBinding
                    {
                        OwnerKind = DurableAgentStateHistoryBinding.HistoryProviderOwner,
                        ProviderKey = "external-history.v1",
                    }),
                Session = await firstAgent.SerializeSessionAsync(session),
            },
        };

        DurableAgentState firstWrite = await RunEntityAsync(
            firstAgent,
            state,
            new RunRequest("first") { CorrelationId = "first" },
            options => options.ProviderKey = new("external-history.v1"));
        Assert.True(
            firstWrite.Data.HistoryBinding
                .GetProperty("csharpFixedOwner")
                .GetBoolean());

        RecordingHistoryProvider secondProvider = new();
        RecordingChatClient secondClient = new();
        DurableAgentState secondWrite = await RunEntityAsync(
            CreateAgentWithProvider(secondClient, secondProvider),
            DeserializeState(SerializeState(firstWrite)),
            new RunRequest("second") { CorrelationId = "second" });

        Assert.Equal(1, secondProvider.LoadCount);
        Assert.Equal(1, secondClient.InvocationCount);
        Assert.Equal("external-history.v1", GetBinding(secondWrite)?.ProviderKey);
    }

    [Fact]
    public async Task OpaqueSharedBindingRejectsCurrentRequestOnlyBeforeModelAsync()
    {
        RecordingAgent agent = new("agent");
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            PersistentRequestOutcomesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
                HistoryBinding = JsonSerializer.SerializeToElement(new
                {
                    runtime = "python",
                    perRunOwnership = true,
                }),
            },
        };
        EntityHarness harness = CreateHarness(
            agent,
            state,
            options =>
            {
                options.ReplayMode = DurableAgentHistoryReplayMode.CurrentRequestOnly;
                options.ProviderKey = new("opaque-agent-session.v1");
            });

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Empty(agent.LastMessages);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task ForeignSharedBindingDoesNotAuthorizeExternalProviderAdoptionAsync()
    {
        RecordingHistoryProvider provider = new();
        RecordingChatClient client = new();
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            PersistentRequestOutcomesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
                HistoryBinding = JsonSerializer.SerializeToElement(new
                {
                    version = 99,
                    ownerKind = "historyProvider",
                    providerKey = "foreign-history.v1",
                    runtime = "python",
                }),
            },
        };
        EntityHarness harness = CreateHarness(
            CreateAgentWithProvider(client, provider),
            state,
            options => options.ProviderKey = new("external-history.v1"));

        DurableAgentHistoryBindingMismatchException exception =
            await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
                () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Contains("opaque shared historyBinding", exception.Message, StringComparison.Ordinal);
        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task OpaqueSharedBindingRejectsPerCallServiceBeforeModelAsync()
    {
        RecordingChatClient client = new();
#pragma warning disable MAAI001
        ChatClientAgent agent = new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                RequirePerServiceCallChatHistoryPersistence = true,
            });
#pragma warning restore MAAI001
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            PersistentRequestOutcomesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
                HistoryBinding = JsonSerializer.SerializeToElement(new
                {
                    runtime = "python",
                    perRunOwnership = true,
                }),
            },
        };
        EntityHarness harness = CreateHarness(
            agent,
            state,
            options =>
            {
                options.ServiceManagedPerServiceCallHistory = true;
                options.ProviderKey = new("model-service.v1");
            });

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task OpaqueSharedBindingRejectsPostResponseServiceTransitionBeforeCommitAsync()
    {
        RecordingChatClient client = new() { ResponseConversationId = "remote-conversation" };
        ChatClientAgent agent = new(client, name: "agent");
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            PersistentRequestOutcomesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
                HistoryBinding = JsonSerializer.SerializeToElement(new
                {
                    runtime = "python",
                    perRunOwnership = true,
                }),
            },
        };
        EntityHarness harness = CreateHarness(
            agent,
            state,
            options => options.ProviderKey = new("model-service.v1"));

        DurableAgentHistoryBindingMismatchException exception =
            await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
                () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Contains("remote service may already have observed", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task LegacyExternalProviderAdoptsOnlyDeclaredContinuationEvidenceAsync()
    {
        RecordingHistoryProvider provider = new();
        RecordingChatClient client = new();
        ChatClientAgent agent = CreateAgentWithProvider(client, provider);
        DurableAgentState legacyState = CreateStateWithExchange("old", "old request", "old response");
        AgentSession session = await agent.CreateSessionAsync();
        session.StateBag.SetValue(
            "external-history",
            new ExternalHistoryState { Count = 7 });
        legacyState.Data.Session = await agent.SerializeSessionAsync(session);

        DurableAgentState persisted = await RunEntityAsync(
            agent,
            legacyState,
            new RunRequest("new") { CorrelationId = "new" },
            options => options.ProviderKey = new("external-history.v1"));

        Assert.Equal(1, provider.LoadCount);
        Assert.Equal(1, provider.StoreCount);
        Assert.Equal(2, persisted.Data.ConversationHistory.Count);
        Assert.Equal(DurableAgentStateHistoryBinding.HistoryProviderOwner, GetBinding(persisted)?.OwnerKind);
        Assert.Equal("external-history.v1", GetBinding(persisted)?.ProviderKey);
    }

    [Fact]
    public async Task UnexpectedServiceTransitionRejectsBeforeCommitButCannotUndoRemoteCallAsync()
    {
        ChatClientAgent firstAgent = new(new RecordingChatClient(), name: "agent");
        DurableAgentState persisted = await RunEntityAsync(
            firstAgent,
            new DurableAgentState(),
            new RunRequest("first") { CorrelationId = "first" });

        RecordingChatClient transitioningClient = new() { ResponseConversationId = "remote-conversation" };
        ChatClientAgent transitioningAgent = new(transitioningClient, name: "agent");
        EntityHarness harness = CreateHarness(
            transitioningAgent,
            DeserializeState(SerializeState(persisted)),
            options => options.ProviderKey = new("model-service.v1"));

        DurableAgentHistoryBindingMismatchException exception =
            await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
                () => harness.RunAsync(new RunRequest("second") { CorrelationId = "second" }));

        Assert.Contains("remote service may already have observed", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, transitioningClient.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task PerCallOwnerTransitionAgainstEntityBindingFailsBeforeCallbacksAsync()
    {
        DurableAgentState entityOwnedState = await RunEntityAsync(
            new ChatClientAgent(new RecordingChatClient(), name: "agent"),
            new DurableAgentState(),
            new RunRequest("first") { CorrelationId = "first" });
        RecordingChatClient replacementClient = new();
#pragma warning disable MAAI001
        ChatClientAgent replacementAgent = new(
            replacementClient,
            new ChatClientAgentOptions
            {
                Name = "agent",
                RequirePerServiceCallChatHistoryPersistence = true,
            });
#pragma warning restore MAAI001
        EntityHarness harness = CreateHarness(
            replacementAgent,
            DeserializeState(SerializeState(entityOwnedState)),
            options =>
            {
                options.ServiceManagedPerServiceCallHistory = true;
                options.ProviderKey = new("model-service.v1");
            });

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("second") { CorrelationId = "second" }));

        Assert.Equal(0, replacementClient.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task RestoredServiceOwnerAgainstEntityBindingFailsBeforeModelAsync()
    {
        DurableAgentState entityOwnedState = await RunEntityAsync(
            new ChatClientAgent(new RecordingChatClient(), name: "agent"),
            new DurableAgentState(),
            new RunRequest("first") { CorrelationId = "first" });
        RecordingChatClient replacementClient = new();
        ChatClientAgent replacementAgent = new(replacementClient, name: "agent");
        JsonElement serviceSession = await replacementAgent.SerializeSessionAsync(
            await replacementAgent.CreateSessionAsync("service-id"));
        EntityHarness harness = CreateHarness(
            replacementAgent,
            CopyState(
                DeserializeState(SerializeState(entityOwnedState)),
                serviceSession),
            options => options.ProviderKey = new("model-service.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(new RunRequest("second") { CorrelationId = "second" }));

        Assert.Equal(0, replacementClient.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task RestoredServiceConversationWithCustomProviderFailsBeforeCallbacksAsync()
    {
        RecordingHistoryProvider provider = new();
        RecordingChatClient client = new();
        ChatClientAgent agent = CreateAgentWithProvider(client, provider);
        AgentSession session = await agent.CreateSessionAsync("service-id");
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            PersistentRequestOutcomesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
                Session = await agent.SerializeSessionAsync(session),
            },
        };
        EntityHarness harness = CreateHarness(
            agent,
            state,
            options => options.ProviderKey = new("model-service.v1"));

        await Assert.ThrowsAsync<DurableAgentHistoryOwnershipNotSupportedException>(
            () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Equal(0, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task FirstServiceTransitionWithoutLogicalKeyWarnsAboutRemoteSideEffectsAsync()
    {
        RecordingChatClient client = new() { ResponseConversationId = "remote-conversation" };
        ChatClientAgent agent = new(client, name: "agent");
        EntityHarness harness = CreateHarness(agent, new DurableAgentState());

        DurableAgentHistoryBindingMismatchException exception =
            await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
                () => harness.RunAsync(new RunRequest("first") { CorrelationId = "first" }));

        Assert.Contains("remote service may already have observed", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task CustomProviderServiceConflictWarnsAboutRemoteSideEffectsAsync()
    {
        RecordingHistoryProvider provider = new();
        RecordingChatClient client = new() { ResponseConversationId = "remote-conversation" };
        EntityHarness harness = CreateHarness(
            CreateAgentWithProvider(client, provider),
            new DurableAgentState(),
            options => options.ProviderKey = new("external-history.v1"));

        DurableAgentHistoryBindingMismatchException exception =
            await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
                () => harness.RunAsync(new RunRequest("first") { CorrelationId = "first" }));

        Assert.IsType<InvalidOperationException>(exception.InnerException);
        Assert.Contains("remote service may already have observed", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.Equal(1, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task MissingServiceConversationIdWarnsAboutRemoteSideEffectsAsync()
    {
        RecordingChatClient client = new() { SuppressConversationId = true };
        ChatClientAgent agent = new(client, name: "agent");
        DurableAgentState initialState = new();
        initialState.Data.Session = await agent.SerializeSessionAsync(
            await agent.CreateSessionAsync("service-conversation"));
        EntityHarness harness = CreateHarness(
            agent,
            initialState,
            options => options.ProviderKey = new("model-service.v1"));

        DurableAgentHistoryBindingMismatchException exception =
            await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
                () => harness.RunAsync(new RunRequest("next") { CorrelationId = "next" }));

        Assert.IsType<InvalidOperationException>(exception.InnerException);
        Assert.Contains("remote service may already have observed", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task AmbiguousLegacyServiceTransitionWarnsAboutRemoteSideEffectsAsync()
    {
        DurableAgentState legacyState = CreateStateWithExchange("old", "old request", "old response");
        RecordingChatClient client = new() { ResponseConversationId = "remote-conversation" };
        ChatClientAgent agent = new(client, name: "agent");
        EntityHarness harness = CreateHarness(
            agent,
            legacyState,
            options => options.ProviderKey = new("model-service.v1"));

        DurableAgentHistoryBindingMismatchException exception =
            await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
                () => harness.RunAsync(new RunRequest("new") { CorrelationId = "new" }));

        Assert.Contains("remote service may already have observed", exception.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(1, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task MatchingHistoryBindingPreservesForwardCompatibleFieldsAsync()
    {
        ChatClientAgent agent = new(new RecordingChatClient(), name: "agent");
        DurableAgentState firstWrite = await RunEntityAsync(
            agent,
            new DurableAgentState(),
            new RunRequest("first") { CorrelationId = "first" });
        DurableAgentStateHistoryBinding firstBinding = GetBinding(firstWrite)!;
        Dictionary<string, JsonElement> bindingProperties =
            firstBinding.UnknownProperties?
                .ToDictionary(pair => pair.Key, pair => pair.Value) ?? [];
        bindingProperties["futureBindingField"] =
            JsonSerializer.SerializeToElement(new { preserve = true });
        DurableAgentState withFutureBindingField = CopyState(
            firstWrite,
            session: firstWrite.Data.Session,
            historyBinding: new DurableAgentStateHistoryBinding
            {
                OwnerKind = firstBinding.OwnerKind,
                ProviderKey = firstBinding.ProviderKey,
                UnknownProperties = bindingProperties,
            });

        DurableAgentState secondWrite = await RunEntityAsync(
            agent,
            withFutureBindingField,
            new RunRequest("second") { CorrelationId = "second" });

        Assert.True(
            GetBinding(secondWrite)?.UnknownProperties?
                .ContainsKey("futureBindingField") is true);
    }

    [Fact]
    public async Task FailureAfterConversationFinalizationDoesNotMutateHydratedStateAsync()
    {
        FailingSerializationAgent agent = new("agent");
        DurableAgentState initialState = CreateStateWithExchange("old", "old request", "old response");
        EntityHarness harness = CreateHarness(agent, initialState);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" }));

        Assert.True(agent.ExecutionCompleted);
        Assert.False(harness.StateWasPersisted);
        Assert.Equal(2, initialState.Data.ConversationHistory.Count);
        Assert.DoesNotContain(
            initialState.Data.ConversationHistory,
            entry => entry.CorrelationId == "new");
    }

    [Fact]
    public async Task CertifiedEntityFailureSealsBindingAndPersistsOnlyAcceptedRequestAsync()
    {
        InvalidOperationException failure = new("provider-private-message");
        RecordingProviderFailureAttestor attestor = new(failure);
        RecordingChatClient client = new() { Exception = failure };
        EntityHarness harness = CreateHarness(
            new ChatClientAgent(client, name: "agent"),
            new DurableAgentState(),
            enableProviderFailureFinalization: true,
            providerFailureAttestor: attestor);

        AgentResponse response = await harness.RunAsync(
            new RunRequest(
                [
                    new ChatMessage(ChatRole.User, "accepted prefix"),
                    new ChatMessage(ChatRole.User, "accepted suffix"),
                ])
            {
                CorrelationId = "new",
            });

        DurableAgentState persisted =
            DeserializeState(SerializeState(Assert.IsType<DurableAgentState>(harness.PersistedState)));
        Assert.Equal(DurableAgentStateHistoryBinding.DurableStateOwner, GetBinding(persisted)?.OwnerKind);
        DurableAgentStateRequest accepted =
            Assert.IsType<DurableAgentStateRequest>(Assert.Single(persisted.Data.ConversationHistory));
        Assert.Equal(
            ["accepted prefix", "accepted suffix"],
            accepted.Messages.Select(message => message.ToChatMessage().Text));
        Assert.DoesNotContain(
            persisted.Data.ConversationHistory,
            entry => entry is DurableAgentStateResponse);
        Assert.Equal(
            DurableAgentStateCompletionReceipt.FailedOutcome,
            persisted.Data.TerminalResults!["new"].Outcome);
        Assert.Empty(response.Messages);
        Assert.Equal(1, client.InvocationCount);
        Assert.Equal(1, attestor.InvocationCount);
    }

    [Fact]
    public async Task CertifiedExternalStoreFailureSealsBindingWithoutEntityTranscriptAsync()
    {
        InvalidOperationException failure = new("provider-private-message");
        RecordingProviderFailureAttestor attestor = new(failure);
        RecordingHistoryProvider provider = new() { StoreException = failure };
        RecordingChatClient client = new();
        EntityHarness harness = CreateHarness(
            CreateAgentWithProvider(client, provider),
            new DurableAgentState(),
            options => options.ProviderKey = new("external-history.v1"),
            enableProviderFailureFinalization: true,
            providerFailureAttestor: attestor);

        _ = await harness.RunAsync(
            new RunRequest("accepted request") { CorrelationId = "new" });

        DurableAgentState persisted =
            DeserializeState(SerializeState(Assert.IsType<DurableAgentState>(harness.PersistedState)));
        Assert.Equal(DurableAgentStateHistoryBinding.HistoryProviderOwner, GetBinding(persisted)?.OwnerKind);
        Assert.Equal("external-history.v1", GetBinding(persisted)?.ProviderKey);
        Assert.Empty(persisted.Data.ConversationHistory);
        Assert.Equal(
            DurableAgentStateCompletionReceipt.FailedOutcome,
            persisted.Data.TerminalResults!["new"].Outcome);
        Assert.True(
            persisted.Data.Session?.GetProperty("stateBag").TryGetProperty("external-history", out _) is true);
        Assert.Equal(1, provider.LoadCount);
        Assert.Equal(1, provider.StoreCount);
        Assert.Equal(1, client.InvocationCount);

        RecordingHistoryProvider duplicateProvider = new();
        RecordingChatClient duplicateClient = new();
        EntityHarness duplicateHarness = CreateHarness(
            CreateAgentWithProvider(duplicateClient, duplicateProvider),
            persisted,
            options => options.ProviderKey = new("external-history.v1"),
            enableProviderFailureFinalization: true,
            providerFailureAttestor: attestor);

        _ = await Assert.ThrowsAsync<DurableAgentTerminalException>(
            () => duplicateHarness.RunAsync(
                new RunRequest([]) { CorrelationId = "new" }));

        Assert.Equal(0, duplicateProvider.LoadCount);
        Assert.Equal(0, duplicateProvider.StoreCount);
        Assert.Equal(0, duplicateClient.InvocationCount);
        Assert.Equal(1, attestor.InvocationCount);
    }

    [Fact]
    public async Task CertifiedExternalFailureWithoutContinuationDoesNotCommitAsync()
    {
        InvalidOperationException failure = new("provider-private-message");
        RecordingProviderFailureAttestor attestor = new(failure);
        RecordingHistoryProvider provider = new()
        {
            StoreException = failure,
            SkipContinuationWrite = true,
        };
        RecordingChatClient client = new();
        EntityHarness harness = CreateHarness(
            CreateAgentWithProvider(client, provider),
            new DurableAgentState(),
            options => options.ProviderKey = new("external-history.v1"),
            enableProviderFailureFinalization: true,
            providerFailureAttestor: attestor);

        await Assert.ThrowsAsync<DurableAgentHistoryBindingMismatchException>(
            () => harness.RunAsync(
                new RunRequest("accepted request") { CorrelationId = "new" }));

        Assert.False(harness.StateWasPersisted);
        Assert.Equal(1, provider.LoadCount);
        Assert.Equal(1, provider.StoreCount);
        Assert.Equal(1, client.InvocationCount);
        Assert.Equal(1, attestor.InvocationCount);
    }

    [Fact]
    public async Task CertifiedServiceFailureSealsFinalServiceOwnerWithoutEntityTranscriptAsync()
    {
        InvalidOperationException failure = new("provider-private-message");
        RecordingProviderFailureAttestor attestor = new(failure);
        RecordingChatClient client = new() { Exception = failure };
        ChatClientAgent agent = new(client, name: "agent");
        DurableAgentState initialState = new();
        initialState.Data.Session = await agent.SerializeSessionAsync(
            await agent.CreateSessionAsync("service-id"));
        EntityHarness harness = CreateHarness(
            agent,
            initialState,
            options => options.ProviderKey = new("model-service.v1"),
            enableProviderFailureFinalization: true,
            providerFailureAttestor: attestor);

        _ = await harness.RunAsync(
            new RunRequest("accepted request") { CorrelationId = "new" });

        DurableAgentState persisted =
            DeserializeState(SerializeState(Assert.IsType<DurableAgentState>(harness.PersistedState)));
        Assert.Equal(DurableAgentStateHistoryBinding.ModelServiceOwner, GetBinding(persisted)?.OwnerKind);
        Assert.Equal("model-service.v1", GetBinding(persisted)?.ProviderKey);
        Assert.Empty(persisted.Data.ConversationHistory);
        Assert.Equal(
            "service-id",
            persisted.Data.Session?.GetProperty("conversationId").GetString());
        Assert.Equal(
            DurableAgentStateCompletionReceipt.FailedOutcome,
            persisted.Data.TerminalResults!["new"].Outcome);
        Assert.Equal(1, client.InvocationCount);
        Assert.Equal(1, attestor.InvocationCount);
    }

    [Fact]
    public async Task AutoRetentionRunsOnCompletedEntityExecutionAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent agent = new(client, name: "agent");
        DurableAgentState initialState = CreateLargeState();
        ConcurrentQueue<string> measuredInstruments = new();
        using MeterListener listener = new();
        listener.InstrumentPublished = static (instrument, meterListener) =>
        {
            if (instrument.Meter.Name == DurableAgentTelemetry.MeterName)
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>(
            (instrument, _, _, _) => measuredInstruments.Enqueue(instrument.Name));
        listener.Start();

        DurableAgentState persisted = await RunEntityAsync(
            agent,
            initialState,
            new RunRequest(new string('n', 500)) { CorrelationId = "new" },
            configureOptions: options =>
            {
                options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto;
                options.MaxStateBytes = 9_000;
            });

        Assert.NotNull(persisted.Data.Truncation);
        Assert.DoesNotContain(persisted.Data.ConversationHistory, entry => entry.CorrelationId == "oldest");
        Assert.Contains(persisted.Data.ConversationHistory, entry => entry.CorrelationId == "new");
        Assert.Contains("oldest", persisted.Data.TerminalResults!.Keys);
        Assert.Contains("oldest", persisted.Data.CompletionReceipts!.Keys);
        Assert.Contains(
            DurableAgentTelemetry.OperationsInstrumentName,
            measuredInstruments);
        Assert.Contains(
            DurableAgentTelemetry.WriteAttemptsInstrumentName,
            measuredInstruments);

        DurableAgentState reloaded = DeserializeState(SerializeState(persisted));
        DurableAgentRunOutcome retainedOutcome =
            DurableAgentStateOutcomeResolver.Resolve(
                reloaded,
                "oldest",
                DateTimeOffset.UtcNow);
        Assert.Equal(DurableAgentRunOutcomeKind.Succeeded, retainedOutcome.Kind);
        Assert.Equal(new string('b', 600), retainedOutcome.Response?.Text);

        RecordingChatClient duplicateClient = new();
        AgentResponse duplicate = await CreateHarness(
            new ChatClientAgent(duplicateClient, name: "agent"),
            reloaded).RunAsync(
                new RunRequest("different request") { CorrelationId = "oldest" });
        Assert.Equal(new string('b', 600), duplicate.Text);
        Assert.Equal(0, duplicateClient.InvocationCount);

        RecordingChatClient nextClient = new();
        _ = await RunEntityAsync(
            new ChatClientAgent(nextClient, name: "agent"),
            DeserializeState(SerializeState(persisted)),
            new RunRequest("next request") { CorrelationId = "next" });
        Assert.DoesNotContain(
            nextClient.LastMessages,
            message => message.Text == new string('a', 600) ||
                message.Text == new string('b', 600));
    }

    [Fact]
    public async Task AutoRetentionCounterOverflowDoesNotCommitRunStateAsync()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        RecordingChatClient client = new();
        ChatClientAgent agent = new(client, name: "agent");
        DurableAgentState initialState = CopyState(
            DurableAgentStateOutcomeResolver.PrepareRevisedWorkingState(
                CreateLargeState(),
                hasAuthoritativeLegacyHistory: true),
            session: null,
            historyBinding: DurableAgentHistoryBinding.Create(
                DurableAgentHistoryOwnership.Entity,
                configuredProviderKey: null));
        initialState.Data.Truncation = new DurableAgentStateTruncation
        {
            EvictedMessageCount = JsonSerializer.SerializeToElement(long.MaxValue),
            FirstEvictedAt = now.AddMinutes(-20),
            LastEvictedAt = now.AddMinutes(-10),
        };
        string originalState = SerializeState(initialState);
        EntityHarness harness = CreateHarness(
            agent,
            initialState,
            configureOptions: options =>
            {
                options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto;
                options.MaxStateBytes = 9_000;
            });

        InvalidOperationException exception = await Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.RunAsync(
                new RunRequest(new string('n', 500)) { CorrelationId = "new" }));

        Assert.Contains("Int64", exception.Message, StringComparison.Ordinal);
        Assert.Equal(1, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
        Assert.Equal(originalState, SerializeState(initialState));
    }

    [Fact]
    public async Task AutoRetentionRemovesMixedMediaToolGroupFromReloadedModelInputAsync()
    {
        DateTimeOffset now = DateTimeOffset.UtcNow;
        DurableAgentState state = CopyState(
            DurableAgentStateOutcomeResolver.PrepareRevisedWorkingState(
                new DurableAgentState(),
                hasAuthoritativeLegacyHistory: true),
            session: null,
            historyBinding: DurableAgentHistoryBinding.Create(
                DurableAgentHistoryOwnership.Entity,
                configuredProviderKey: null));
        using JsonDocument opaqueDocument = JsonDocument.Parse(
            """{"$runtimeType":"future-opaque","payload":{"value":42}}""");
        state.Data.ConversationHistory.Add(
            new DurableAgentStateRequest
            {
                CorrelationId = "old-call",
                CreatedAt = now.AddMinutes(-10),
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.User, "invoke old tool")),
                ],
            });
        state.Data.ConversationHistory.Add(
            new DurableAgentStateResponse
            {
                CorrelationId = "old-call",
                CreatedAt = now.AddMinutes(-10),
                Messages =
                [
                    new DurableAgentStateMessage
                    {
                        Role = ChatRole.Assistant.Value,
                        Contents =
                        [
                            new DurableAgentStateFunctionCallContent
                            {
                                CallId = "large-call",
                                Name = "tool",
                                Arguments = JsonSerializer.SerializeToElement(
                                    new { payload = new string('a', 4_000) }),
                            },
                            new DurableAgentStateUriContent
                            {
                                Uri = new Uri("https://example.test/media"),
                                MediaType = null,
                            },
                            new DurableAgentStateUnknownContent
                            {
                                Content = opaqueDocument.RootElement.Clone(),
                            },
                        ],
                    },
                ],
            });
        state.Data.ConversationHistory.Add(
            new DurableAgentStateRequest
            {
                CorrelationId = "old-result",
                CreatedAt = now.AddMinutes(-9),
                Messages =
                [
                    new DurableAgentStateMessage
                    {
                        Role = ChatRole.Tool.Value,
                        Contents =
                        [
                            new DurableAgentStateFunctionResultContent
                            {
                                CallId = "large-call",
                                Result = JsonSerializer.SerializeToElement(new string('r', 8_000)),
                            },
                        ],
                    },
                ],
            });
        state.Data.ConversationHistory.Add(
            new DurableAgentStateResponse
            {
                CorrelationId = "old-result",
                CreatedAt = now.AddMinutes(-9),
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.Assistant, "old tool complete")),
                ],
            });
        AddExchange(state, "recent", "recent request", "recent response", now.AddMinutes(-1));

        DurableAgentState persisted = await RunEntityAsync(
            new ChatClientAgent(new RecordingChatClient(), name: "agent"),
            state,
            new RunRequest("first new request") { CorrelationId = "first-new" },
            configureOptions: options =>
            {
                options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto;
                options.MaxStateBytes = 7_000;
            });

        Assert.DoesNotContain(
            persisted.Data.ConversationHistory,
            entry => entry.CorrelationId is "old-call" or "old-result");
        DurableAgentState reloaded = DeserializeState(SerializeState(persisted));
        RecordingChatClient nextClient = new();
        _ = await RunEntityAsync(
            new ChatClientAgent(nextClient, name: "agent"),
            reloaded,
            new RunRequest("second new request") { CorrelationId = "second-new" });

        Assert.Contains(nextClient.LastMessages, message => message.Text == "recent request");
        Assert.Contains(nextClient.LastMessages, message => message.Text == "first new request");
        Assert.DoesNotContain(
            nextClient.LastMessages.SelectMany(message => message.Contents),
            content =>
                content is FunctionCallContent { CallId: "large-call" } ||
                content is FunctionResultContent { CallId: "large-call" } ||
                content is UriContent uri &&
                    uri.Uri == new Uri("https://example.test/media") ||
                content.RawRepresentation is JsonElement element &&
                    element.ValueKind == JsonValueKind.Object &&
                    element.TryGetProperty("$runtimeType", out _));
    }

    [Fact]
    public async Task KeepAllRejectsInapplicableBudgetBeforeModelExecutionAsync()
    {
        RecordingChatClient client = new();
        DurableAgentState initialState = CreateLargeState();
        EntityHarness harness = CreateHarness(
            new ChatClientAgent(client, name: "agent"),
            initialState,
            configureOptions: options => options.MaxStateBytes = 500);

        InvalidOperationException exception =
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => harness.RunAsync(
                    new RunRequest("new request") { CorrelationId = "new" }));

        Assert.Contains("only valid", exception.Message, StringComparison.Ordinal);
        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public async Task LegacyAutoWithoutAuthorizedMigrationFailsBeforeModelExecutionAsync()
    {
        RecordingChatClient client = new();
        DurableAgentState initialState =
            CreateStateWithExchange("old", "old request", "old response");
        EntityHarness harness = CreateHarness(
            new ChatClientAgent(client, name: "agent"),
            initialState,
            configureOptions: options =>
            {
                options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto;
                options.MaxStateBytes = 500;
                options.AuthorizeLegacyMigration = null;
            });

        DurableAgentStateCorruptionException exception =
            await Assert.ThrowsAsync<DurableAgentStateCorruptionException>(
                () => harness.RunAsync(
                    new RunRequest("new request") { CorrelationId = "new" }));

        Assert.Contains(
            "independently authoritative complete history",
            exception.Message,
            StringComparison.Ordinal);
        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, initialState.SchemaVersion);
    }

    [Fact]
    public async Task DuplicateLegacyAutoMigrationStillEnforcesStateBudgetAsync()
    {
        RecordingChatClient client = new();
        DurableAgentState initialState = CreateStateWithExchange(
            "duplicate",
            new string('q', 1_000),
            new string('a', 2_000));
        EntityHarness harness = CreateHarness(
            new ChatClientAgent(client, name: "agent"),
            initialState,
            configureOptions: options =>
            {
                options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto;
                options.MaxStateBytes = 500;
            });

        _ = await Assert.ThrowsAsync<DurableAgentStateSizeLimitExceededException>(
            () => harness.RunAsync(
                new RunRequest("different request") { CorrelationId = "duplicate" }));

        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
        Assert.Equal(DurableAgentState.CurrentSchemaVersion, initialState.SchemaVersion);
    }

    [Fact]
    public async Task DuplicateLegacyAutoWithoutAuthorizedMigrationFailsClosedAsync()
    {
        RecordingChatClient client = new();
        DurableAgentState initialState =
            CreateStateWithExchange("duplicate", "request", "response");
        EntityHarness harness = CreateHarness(
            new ChatClientAgent(client, name: "agent"),
            initialState,
            configureOptions: options =>
            {
                options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto;
                options.MaxStateBytes = 10_000;
                options.AuthorizeLegacyMigration = null;
            });

        DurableAgentStateCorruptionException exception =
            await Assert.ThrowsAsync<DurableAgentStateCorruptionException>(
                () => harness.RunAsync(
                    new RunRequest("different request") { CorrelationId = "duplicate" }));

        Assert.Contains(
            "independently authoritative complete history",
            exception.Message,
            StringComparison.Ordinal);
        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
    }

    [Fact]
    public void AutoDoesNotInitializeMailboxStateWithoutInternalGate()
    {
        DurableAgentsOptions options = new()
        {
            EnablePersistentRequestOutcomes = false,
            HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto,
        };
        Dictionary<Type, object> services = new()
        {
            [typeof(DurableTaskClient)] = new Mock<DurableTaskClient>("test").Object,
            [typeof(ILoggerFactory)] = new ListLoggerFactory(new ListLoggerProvider()),
            [typeof(DurableAgentsOptions)] = options,
        };
        TestableAgentEntity entity = new(new DictionaryServiceProvider(services));
        Mock<TaskEntityOperation> operation = new();
        operation.SetupGet(value => value.Name).Returns(nameof(AgentEntity.Run));

        DurableAgentState initialized = entity.Initialize(operation.Object);

        Assert.Equal(DurableAgentState.CurrentSchemaVersion, initialized.SchemaVersion);
        Assert.False(initialized.PersistentRequestOutcomesAuthorized);
        Assert.Null(initialized.Data.TerminalResults);
        Assert.Null(initialized.Data.CompletionReceipts);
    }

    [Fact]
    public async Task OversizedProtectedStateFailsWithoutPersistenceAsync()
    {
        RecordingChatClient client = new();
        ChatClientAgent agent = new(client, name: "agent");
        DurableAgentState initialState = new();
        string originalState = SerializeState(initialState);
        EntityHarness harness = CreateHarness(
            agent,
            initialState,
            configureOptions: options =>
            {
                options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.Auto;
                options.MaxStateBytes = 500;
            });

        await Assert.ThrowsAsync<DurableAgentStateSizeLimitExceededException>(
            () => harness.RunAsync(
                new RunRequest(new string('x', 2_000)) { CorrelationId = "new" }));

        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
        Assert.Equal(originalState, SerializeState(initialState));
    }

    [Fact]
    public async Task ProviderLoadFailureDoesNotInvokeModelOrCommitWorkingStateAsync()
    {
        InvalidOperationException expected = new("provider load failed");
        RecordingHistoryProvider provider = new() { LoadException = expected };
        RecordingChatClient client = new();
        ChatClientAgent agent = CreateAgentWithProvider(client, provider);
        DurableAgentState initialState = await CreateBoundExternalStateAsync();
        string originalState = SerializeState(initialState);
        EntityHarness harness = CreateHarness(
            agent,
            initialState,
            options => options.ProviderKey = new("external-history.v1"));

        InvalidOperationException actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" }));

        Assert.Same(expected, actual);
        Assert.Equal(1, provider.LoadCount);
        Assert.Equal(0, provider.StoreCount);
        Assert.Equal(0, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
        Assert.Equal(originalState, SerializeState(initialState));
        Assert.Contains(harness.Logs, entry => ReferenceEquals(expected, entry.Exception));
    }

    [Fact]
    public async Task ProviderStoreFailureDoesNotCommitWorkingStateAsync()
    {
        InvalidOperationException expected = new("provider store failed");
        RecordingHistoryProvider provider = new() { StoreException = expected };
        RecordingChatClient client = new();
        ChatClientAgent agent = CreateAgentWithProvider(client, provider);
        DurableAgentState initialState = await CreateBoundExternalStateAsync();
        string originalState = SerializeState(initialState);
        EntityHarness harness = CreateHarness(
            agent,
            initialState,
            options => options.ProviderKey = new("external-history.v1"));

        InvalidOperationException actual = await Assert.ThrowsAsync<InvalidOperationException>(
            () => harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" }));

        Assert.Same(expected, actual);
        Assert.Equal(1, provider.LoadCount);
        Assert.Equal(1, provider.StoreCount);
        Assert.Equal(1, client.InvocationCount);
        Assert.False(harness.StateWasPersisted);
        Assert.Equal(originalState, SerializeState(initialState));
        Assert.Contains(harness.Logs, entry => ReferenceEquals(expected, entry.Exception));
    }

    [Fact]
    public async Task ProviderLoadCancellationPropagatesWithoutCommitOrWrappingAsync()
    {
        using TestHostApplicationLifetime lifetime = new();
        RecordingHistoryProvider provider = new()
        {
            WaitForLoadCancellation = true,
        };
        RecordingChatClient client = new();
        ChatClientAgent agent = CreateAgentWithProvider(client, provider);
        DurableAgentState initialState = await CreateBoundExternalStateAsync();
        string originalState = SerializeState(initialState);
        EntityHarness harness = CreateHarness(
            agent,
            initialState,
            options => options.ProviderKey = new("external-history.v1"),
            applicationLifetime: lifetime);

        Task runTask = harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" });
        try
        {
            await WaitForProviderStageAsync(
                provider.LoadStarted.Task,
                runTask,
                "history provider load callback");
            lifetime.StopApplication();
            OperationCanceledException actual =
                await Assert.ThrowsAnyAsync<OperationCanceledException>(
                    () => runTask.WaitAsync(s_testTimeout));

            Assert.Same(provider.LoadCancellationException, actual);
            Assert.Equal(lifetime.ApplicationStopping, provider.LoadCancellationToken);
            Assert.Equal(1, provider.LoadCount);
            Assert.Equal(0, provider.StoreCount);
            Assert.Equal(0, client.InvocationCount);
            Assert.False(harness.StateWasPersisted);
            Assert.Equal(originalState, SerializeState(initialState));
        }
        finally
        {
            lifetime.StopApplication();
            await JoinRunTaskAsync(runTask, "history provider load cancellation");
        }
    }

    [Fact]
    public async Task ProviderStoreCancellationPropagatesWithoutPartialCommitOrWrappingAsync()
    {
        using TestHostApplicationLifetime lifetime = new();
        RecordingHistoryProvider provider = new()
        {
            WaitForStoreCancellation = true,
        };
        RecordingChatClient client = new();
        ChatClientAgent agent = CreateAgentWithProvider(client, provider);
        DurableAgentState initialState = await CreateBoundExternalStateAsync();
        string originalState = SerializeState(initialState);
        EntityHarness harness = CreateHarness(
            agent,
            initialState,
            options => options.ProviderKey = new("external-history.v1"),
            applicationLifetime: lifetime);

        Task runTask = harness.RunAsync(new RunRequest("new request") { CorrelationId = "new" });
        try
        {
            await WaitForProviderStageAsync(
                provider.StoreStarted.Task,
                runTask,
                "history provider store callback");
            lifetime.StopApplication();
            OperationCanceledException actual =
                await Assert.ThrowsAnyAsync<OperationCanceledException>(
                    () => runTask.WaitAsync(s_testTimeout));

            Assert.Same(provider.StoreCancellationException, actual);
            Assert.Equal(lifetime.ApplicationStopping, client.LastCancellationToken);
            Assert.Equal(lifetime.ApplicationStopping, provider.LoadCancellationToken);
            Assert.Equal(lifetime.ApplicationStopping, provider.StoreCancellationToken);
            Assert.Equal(1, provider.LoadCount);
            Assert.Equal(1, provider.StoreCount);
            Assert.Equal(1, client.InvocationCount);
            Assert.False(harness.StateWasPersisted);
            Assert.Equal(originalState, SerializeState(initialState));
        }
        finally
        {
            lifetime.StopApplication();
            await JoinRunTaskAsync(runTask, "history provider store cancellation");
        }
    }

    private static async Task<DurableAgentState> RunEntityAsync(
        AIAgent agent,
        DurableAgentState state,
        RunRequest request,
        Action<DurableAgentHistoryOptions>? configureHistory = null,
        Action<DurableAgentsOptions>? configureOptions = null)
    {
        EntityHarness harness = CreateHarness(
            agent,
            state,
            configureHistory,
            configureOptions: configureOptions);
        await harness.RunAsync(request);
        return Assert.IsType<DurableAgentState>(harness.PersistedState);
    }

    private static EntityHarness CreateHarness(
        AIAgent agent,
        DurableAgentState state,
        Action<DurableAgentHistoryOptions>? configureHistory = null,
        bool registerWithFactory = false,
        Action? onFactoryInvoked = null,
        IHostApplicationLifetime? applicationLifetime = null,
        bool enableMailboxWrites = true,
        bool enableProviderFailureFinalization = false,
        IDurableAgentProviderFailureAttestor? providerFailureAttestor = null,
        Action<DurableAgentsOptions>? configureOptions = null)
    {
        AgentSessionId sessionId = new(agent.Name!, "session");
        DurableAgentsOptions options = new()
        {
            DefaultTimeToLive = null,
            EnablePersistentRequestOutcomes = enableMailboxWrites,
            EnableProviderFailureFinalization = enableProviderFailureFinalization,
            AuthorizeLegacyMigration = enableMailboxWrites ? static _ => true : null,
        };
        configureOptions?.Invoke(options);
        if (registerWithFactory)
        {
            options.AddAIAgentFactory(
                agent.Name!,
                _ =>
                {
                    onFactoryInvoked?.Invoke();
                    return agent;
                },
                timeToLive: null,
                configureHistory: configureHistory);
        }
        else
        {
            options.AddAIAgent(agent, timeToLive: null, configureHistory: configureHistory);
        }

        ListLoggerProvider loggerProvider = new();
        Dictionary<Type, object> services = new()
        {
            [typeof(DurableTaskClient)] = new Mock<DurableTaskClient>("test").Object,
            [typeof(ILoggerFactory)] = new ListLoggerFactory(loggerProvider),
            [typeof(DurableAgentsOptions)] = options,
            [typeof(IReadOnlyDictionary<string, Func<IServiceProvider, AIAgent>>)] = options.GetAgentFactories(),
            [typeof(IHostApplicationLifetime)] = applicationLifetime ??
                Mock.Of<IHostApplicationLifetime>(
                    lifetime => lifetime.ApplicationStopping == CancellationToken.None),
        };
        if (providerFailureAttestor is not null)
        {
            services[typeof(IDurableAgentProviderFailureAttestor)] = providerFailureAttestor;
        }
        IServiceProvider serviceProvider = new DictionaryServiceProvider(services);

        Mock<TaskEntityContext> context = new();
        context.SetupGet(value => value.Id).Returns(sessionId);
        Mock<TaskEntityState> entityState = new();
        entityState.Setup(value => value.GetState(typeof(DurableAgentState))).Returns(state);
        object? persistedState = null;
        entityState.Setup(value => value.SetState(It.IsAny<object?>()))
            .Callback<object?>(value => persistedState = value);

        Mock<TaskEntityOperation> operation = new();
        operation.SetupGet(value => value.Name).Returns(nameof(AgentEntity.Run));
        operation.SetupGet(value => value.Context).Returns(context.Object);
        operation.SetupGet(value => value.State).Returns(entityState.Object);
        operation.SetupGet(value => value.HasInput).Returns(true);

        AgentEntity entity = new(serviceProvider);
        return new EntityHarness(
            entity,
            operation,
            loggerProvider,
            () => persistedState);
    }

    private static ChatClientAgent CreateAgentWithProvider(
        RecordingChatClient client,
        RecordingHistoryProvider provider) =>
        new(
            client,
            new ChatClientAgentOptions
            {
                Name = "agent",
                ChatHistoryProvider = provider,
            });

    private static async Task<DurableAgentState> CreateBoundExternalStateAsync()
    {
        ChatClientAgent agent = CreateAgentWithProvider(
            new RecordingChatClient(),
            new RecordingHistoryProvider());
        return await RunEntityAsync(
            agent,
            new DurableAgentState(),
            new RunRequest("seed") { CorrelationId = "seed" },
            options => options.ProviderKey = new("external-history.v1"));
    }

    private static async Task<DurableAgentState> CreateProvisionalExternalStateAsync(
        ChatClientAgent agent,
        string providerKey)
    {
        AgentSession session = await agent.CreateSessionAsync();
        session.StateBag.SetValue(
            "external-history",
            new ExternalHistoryState { Count = 1 });
        return new DurableAgentState
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            PersistentRequestOutcomesAuthorized = true,
            Data = new DurableAgentStateData
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
                HistoryBinding = DurableAgentHistoryBinding.ToJson(
                    new DurableAgentStateHistoryBinding
                    {
                        OwnerKind = DurableAgentStateHistoryBinding.HistoryProviderOwner,
                        ProviderKey = providerKey,
                    }),
                Session = await agent.SerializeSessionAsync(session),
            },
        };
    }

    private static string SerializeState(DurableAgentState state) =>
        JsonSerializer.Serialize(
            state,
            DurableAgentStateJsonContext.Default.DurableAgentState);

    private static DurableAgentState DeserializeState(string json) =>
        Assert.IsType<DurableAgentState>(
            JsonSerializer.Deserialize(
                json,
                DurableAgentStateJsonContext.Default.DurableAgentState));

    private static DurableAgentStateHistoryBinding? GetBinding(
        DurableAgentState state) =>
        DurableAgentHistoryBinding.Parse(state.Data.HistoryBinding);

    private static DurableAgentState CopyState(
        DurableAgentState state,
        JsonElement? session,
        DurableAgentStateHistoryBinding? historyBinding = null)
    {
        return new DurableAgentState
        {
            SchemaVersion = state.SchemaVersion,
            PersistentRequestOutcomesAuthorized = state.PersistentRequestOutcomesAuthorized,
            Data = new DurableAgentStateData
            {
                ConversationHistory = state.Data.ConversationHistory,
                TerminalResults = state.Data.TerminalResults,
                CompletionReceipts = state.Data.CompletionReceipts,
                HistoryBinding = historyBinding is null
                    ? state.Data.HistoryBinding
                    : DurableAgentHistoryBinding.ToJson(historyBinding),
                Session = session,
                IngestedPositions = state.Data.IngestedPositions,
                Truncation = state.Data.Truncation,
                ExpirationTimeUtc = state.Data.ExpirationTimeUtc,
                ExtensionData = state.Data.ExtensionData,
                UnknownProperties = state.Data.UnknownProperties,
            },
            ExtensionData = state.ExtensionData,
            UnknownProperties = state.UnknownProperties,
        };
    }

    private static async Task WaitForProviderStageAsync(
        Task stageTask,
        Task runTask,
        string stageDescription)
    {
        Task completedTask;
        try
        {
            completedTask = await Task.WhenAny(stageTask, runTask).WaitAsync(s_stageTimeout);
        }
        catch (TimeoutException exception)
        {
            throw new Xunit.Sdk.XunitException(
                $"Timed out after {s_stageTimeout} waiting to reach the {stageDescription}.",
                exception);
        }

        if (ReferenceEquals(completedTask, runTask))
        {
            try
            {
                await runTask;
            }
            catch (Exception exception)
            {
                throw new Xunit.Sdk.XunitException(
                    $"The entity run failed before reaching the {stageDescription}.",
                    exception);
            }

            throw new Xunit.Sdk.XunitException(
                $"The entity run completed before reaching the {stageDescription}.");
        }

        await stageTask;
    }

    private static async Task JoinRunTaskAsync(Task runTask, string scenarioDescription)
    {
        try
        {
            await runTask.WaitAsync(s_testTimeout);
        }
        catch (OperationCanceledException) when (runTask.IsCanceled)
        {
            // The test body asserts the propagated cancellation; cleanup only joins the same task.
        }
        catch (TimeoutException exception)
        {
            throw new Xunit.Sdk.XunitException(
                $"Timed out after {s_testTimeout} joining the entity run during cleanup for {scenarioDescription}; the test may have leaked a running task.",
                exception);
        }
        catch (Exception exception)
        {
            throw new Xunit.Sdk.XunitException(
                $"The entity run faulted unexpectedly during cleanup for {scenarioDescription}.",
                exception);
        }
    }

    private static DurableAgentState CreateStateWithExchange(
        string correlationId,
        string request,
        string response)
    {
        DurableAgentState state = new();
        AddExchange(state, correlationId, request, response, DateTimeOffset.UtcNow.AddMinutes(-5));
        return state;
    }

    private static DurableAgentState CreateLargeState()
    {
        DurableAgentState state = new();
        DateTimeOffset now = DateTimeOffset.UtcNow;
        AddExchange(state, "oldest", new string('a', 600), new string('b', 600), now.AddMinutes(-10));
        AddExchange(state, "middle", new string('c', 600), new string('d', 600), now.AddMinutes(-5));
        return state;
    }

    private static void AddExchange(
        DurableAgentState state,
        string correlationId,
        string request,
        string response,
        DateTimeOffset createdAt)
    {
        state.Data.ConversationHistory.Add(
            new DurableAgentStateRequest
            {
                CorrelationId = correlationId,
                CreatedAt = createdAt,
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.User, request) { CreatedAt = createdAt }),
                ],
            });
        state.Data.ConversationHistory.Add(
            new DurableAgentStateResponse
            {
                CorrelationId = correlationId,
                CreatedAt = createdAt,
                Messages =
                [
                    DurableAgentStateMessage.FromChatMessage(
                        new ChatMessage(ChatRole.Assistant, response) { CreatedAt = createdAt }),
                ],
            });
    }

    private sealed class EntityHarness(
        AgentEntity entity,
        Mock<TaskEntityOperation> operation,
        ListLoggerProvider loggerProvider,
        Func<object?> persistedState)
    {
        public object? PersistedState => persistedState();

        public bool StateWasPersisted => this.PersistedState is not null;

        public IReadOnlyList<LogRecord> Logs => loggerProvider.Records;

        public async Task<AgentResponse> RunAsync(RunRequest request)
        {
            operation.Setup(value => value.GetInput(typeof(RunRequest))).Returns(request);
            object? result = await ((ITaskEntity)entity).RunAsync(operation.Object);
            return Assert.IsType<AgentResponse>(result);
        }
    }

    private sealed class TestDelegatingAgent(AIAgent innerAgent) : DelegatingAIAgent(innerAgent);

    private sealed class CountingSessionAgent(AIAgent innerAgent) : DelegatingAIAgent(innerAgent)
    {
        public int DeserializeCount { get; private set; }

        protected override ValueTask<AgentSession> DeserializeSessionCoreAsync(
            JsonElement serializedState,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default)
        {
            this.DeserializeCount++;
            return base.DeserializeSessionCoreAsync(
                serializedState,
                jsonSerializerOptions,
                cancellationToken);
        }
    }

    private sealed class RecordingAgent(string name) : AIAgent
    {
        public override string? Name => name;

        public List<ChatMessage> LastMessages { get; private set; } = [];

        protected override ValueTask<AgentSession> CreateSessionCoreAsync(
            CancellationToken cancellationToken = default) => new(new RecordingSession());

        protected override ValueTask<JsonElement> SerializeSessionCoreAsync(
            AgentSession session,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) =>
            new(JsonSerializer.SerializeToElement(new { stateBag = session.StateBag.Serialize() }));

        protected override ValueTask<AgentSession> DeserializeSessionCoreAsync(
            JsonElement serializedState,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) =>
            new(new RecordingSession());

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
            this.LastMessages = messages.ToList();
            await Task.Yield();
            yield return new AgentResponseUpdate(ChatRole.Assistant, "response");
        }

        private sealed class RecordingSession : AgentSession;
    }

    private sealed class TestableAgentEntity(IServiceProvider services) : AgentEntity(services)
    {
        public DurableAgentState Initialize(TaskEntityOperation operation) =>
            this.InitializeState(operation);
    }

    private sealed class FailingSerializationAgent(string name) : AIAgent
    {
        public override string? Name => name;

        public bool ExecutionCompleted { get; private set; }

        protected override ValueTask<AgentSession> CreateSessionCoreAsync(
            CancellationToken cancellationToken = default) => new(new FailingSerializationSession());

        protected override ValueTask<JsonElement> SerializeSessionCoreAsync(
            AgentSession session,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException("session serialization failed");

        protected override ValueTask<AgentSession> DeserializeSessionCoreAsync(
            JsonElement serializedState,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) =>
            new(new FailingSerializationSession());

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
            this.ExecutionCompleted = true;
            await Task.Yield();
            yield return new AgentResponseUpdate(ChatRole.Assistant, "response");
        }

        private sealed class FailingSerializationSession : AgentSession;
    }

    private sealed class RecordingHistoryProvider : ChatHistoryProvider
    {
        public override IReadOnlyList<string> StateKeys => ["external-history"];

        public Exception? LoadException { get; init; }

        public Exception? StoreException { get; init; }

        public bool WaitForLoadCancellation { get; init; }

        public bool WaitForStoreCancellation { get; init; }

        public bool SkipContinuationWrite { get; init; }

        public JsonElement? ContinuationValue { get; init; }

        public bool PersistTranscript { get; init; }

        public TaskCompletionSource<bool> LoadStarted { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource<bool> StoreStarted { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int LoadCount { get; private set; }

        public int StoreCount { get; private set; }

        public CancellationToken LoadCancellationToken { get; private set; }

        public CancellationToken StoreCancellationToken { get; private set; }

        public OperationCanceledException? LoadCancellationException { get; private set; }

        public OperationCanceledException? StoreCancellationException { get; private set; }

        protected override async ValueTask<IEnumerable<ChatMessage>> ProvideChatHistoryAsync(
            InvokingContext context,
            CancellationToken cancellationToken = default)
        {
            this.LoadCount++;
            this.LoadCancellationToken = cancellationToken;
            if (this.WaitForLoadCancellation)
            {
                this.LoadStarted.TrySetResult(true);
                try
                {
                    await Task.Delay(Timeout.Infinite, cancellationToken)
                        .WaitAsync(s_stageTimeout, CancellationToken.None);
                }
                catch (OperationCanceledException exception)
                {
                    this.LoadCancellationException = exception;
                    throw;
                }
                catch (TimeoutException exception)
                {
                    throw new Xunit.Sdk.XunitException(
                        $"Timed out after {s_stageTimeout} waiting for ApplicationStopping during provider load.",
                        exception);
                }
            }

            if (this.LoadException is not null)
            {
                throw this.LoadException;
            }

            if (this.PersistTranscript &&
                context.Session!.StateBag.TryGetValue(
                    "external-history",
                    out TranscriptHistoryState? transcript))
            {
                return transcript?.Messages.Select(message => message.ToChatMessage()) ?? [];
            }

            return [];
        }

        protected override async ValueTask StoreChatHistoryAsync(
            InvokedContext context,
            CancellationToken cancellationToken = default)
        {
            this.StoreCount++;
            this.StoreCancellationToken = cancellationToken;
            if (this.PersistTranscript)
            {
                _ = context.Session!.StateBag.TryGetValue(
                    "external-history",
                    out TranscriptHistoryState? transcript);
                transcript ??= new();
                transcript.Messages.AddRange(
                    context.RequestMessages.Select(
                        message => DurableAgentStateMessage.FromChatMessage(message)));
                transcript.Messages.AddRange(
                    (context.ResponseMessages ?? [])
                        .Select(message => DurableAgentStateMessage.FromChatMessage(message)));
                context.Session.StateBag.SetValue("external-history", transcript);
            }
            else if (!this.SkipContinuationWrite)
            {
                if (this.ContinuationValue is JsonElement continuationValue)
                {
                    context.Session!.StateBag.SetValue<object>(
                        "external-history",
                        continuationValue);
                }
                else
                {
                    context.Session!.StateBag.SetValue(
                        "external-history",
                        new ExternalHistoryState { Count = this.StoreCount });
                }
            }
            if (this.WaitForStoreCancellation)
            {
                this.StoreStarted.TrySetResult(true);
                try
                {
                    await Task.Delay(Timeout.Infinite, cancellationToken)
                        .WaitAsync(s_stageTimeout, CancellationToken.None);
                }
                catch (OperationCanceledException exception)
                {
                    this.StoreCancellationException = exception;
                    throw;
                }
                catch (TimeoutException exception)
                {
                    throw new Xunit.Sdk.XunitException(
                        $"Timed out after {s_stageTimeout} waiting for ApplicationStopping during provider store.",
                        exception);
                }
            }

            if (this.StoreException is not null)
            {
                throw this.StoreException;
            }
        }
    }

    private sealed class ExternalHistoryState
    {
        public int Count { get; set; }
    }

    private sealed class TranscriptHistoryState
    {
        public List<DurableAgentStateMessage> Messages { get; set; } = [];
    }

    private sealed class MultiKeyHistoryProvider(bool writeSecondKey) : ChatHistoryProvider
    {
        public override IReadOnlyList<string> StateKeys => ["external-primary", "external-index"];

        public int StoreCount { get; private set; }

        protected override ValueTask<IEnumerable<ChatMessage>> ProvideChatHistoryAsync(
            InvokingContext context,
            CancellationToken cancellationToken = default) =>
            new([]);

        protected override ValueTask StoreChatHistoryAsync(
            InvokedContext context,
            CancellationToken cancellationToken = default)
        {
            this.StoreCount++;
            context.Session!.StateBag.SetValue("external-primary", "primary");
            if (writeSecondKey)
            {
                context.Session.StateBag.SetValue("external-index", "index");
            }

            return default;
        }
    }

    private sealed class EmptyStateKeysHistoryProvider : ChatHistoryProvider
    {
        public override IReadOnlyList<string> StateKeys => [];

        public int LoadCount { get; private set; }

        public int StoreCount { get; private set; }

        protected override ValueTask<IEnumerable<ChatMessage>> ProvideChatHistoryAsync(
            InvokingContext context,
            CancellationToken cancellationToken = default)
        {
            this.LoadCount++;
            return new([]);
        }

        protected override ValueTask StoreChatHistoryAsync(
            InvokedContext context,
            CancellationToken cancellationToken = default)
        {
            this.StoreCount++;
            return default;
        }
    }

    private sealed class RecordingChatClient : IChatClient
    {
        public Exception? Exception { get; init; }

        public string? ResponseConversationId { get; init; }

        public bool SuppressConversationId { get; init; }

        public int InvocationCount { get; private set; }

        public CancellationToken LastCancellationToken { get; private set; }

        public string? LastConversationId { get; private set; }

        public List<ChatMessage> LastMessages { get; private set; } = [];

        public void Dispose()
        {
        }

        public object? GetService(Type serviceType, object? serviceKey = null) => null;

        public Task<ChatResponse> GetResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            this.InvocationCount++;
            this.LastCancellationToken = cancellationToken;
            this.LastConversationId = options?.ConversationId;
            this.LastMessages = messages.ToList();
            if (this.Exception is not null)
            {
                throw this.Exception;
            }

            await Task.Yield();
            yield return new ChatResponseUpdate(ChatRole.Assistant, "response")
            {
                ConversationId = this.SuppressConversationId
                    ? null
                    : this.ResponseConversationId ?? options?.ConversationId,
            };
        }
    }

    private sealed class ToolLoopChatClient(string toolName) : IChatClient
    {
        public int InvocationCount { get; private set; }

        public void Dispose()
        {
        }

        public object? GetService(Type serviceType, object? serviceKey = null) => null;

        public Task<ChatResponse> GetResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public async IAsyncEnumerable<ChatResponseUpdate> GetStreamingResponseAsync(
            IEnumerable<ChatMessage> messages,
            ChatOptions? options = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            this.InvocationCount++;
            await Task.Yield();
            if (this.InvocationCount == 1)
            {
                yield return new ChatResponseUpdate(
                    ChatRole.Assistant,
                    [
                        new FunctionCallContent(
                            "call-1",
                            toolName,
                            new Dictionary<string, object?> { ["value"] = "from tool" }),
                    ]);
                yield break;
            }

            yield return new ChatResponseUpdate(ChatRole.Assistant, "final response");
        }
    }

    private sealed class SummarizingChatReducer : IChatReducer
    {
        public int InvocationCount { get; private set; }

        public IReadOnlyList<ChatMessage> LastMessages { get; private set; } = [];

        public Task<IEnumerable<ChatMessage>> ReduceAsync(
            IEnumerable<ChatMessage> messages,
            CancellationToken cancellationToken)
        {
            this.InvocationCount++;
            this.LastMessages = messages.ToList();
            return Task.FromResult<IEnumerable<ChatMessage>>(
                [
                    new ChatMessage(
                        ChatRole.System,
                        $"summary: {this.LastMessages.Last(message => message.Role == ChatRole.User).Text}"),
                    this.LastMessages[^1],
                ]);
        }
    }

    private sealed class RecordingProviderFailureAttestor(
        Exception expected) : IDurableAgentProviderFailureAttestor
    {
        public int InvocationCount { get; private set; }

        public bool TryAttest(
            Exception exception,
            out DurableAgentProviderFailureAttestation? attestation)
        {
            this.InvocationCount++;
            attestation = ReferenceEquals(exception, expected)
                ? new(
                    DurableAgentProviderFailurePhase.Invoke,
                    DurableAgentProviderFailureFinality.NonRetryable,
                    InputAccepted: true,
                    Code: "providerFailure",
                    Message: "The provider request failed.")
                : null;
            return attestation is not null;
        }
    }

    private sealed class TestHostApplicationLifetime : IHostApplicationLifetime, IDisposable
    {
        private readonly CancellationTokenSource _applicationStopping = new();

        public CancellationToken ApplicationStarted => CancellationToken.None;

        public CancellationToken ApplicationStopping => this._applicationStopping.Token;

        public CancellationToken ApplicationStopped => CancellationToken.None;

        public void StopApplication() => this._applicationStopping.Cancel();

        public void Dispose() => this._applicationStopping.Dispose();
    }

    private sealed class ListLoggerProvider : ILoggerProvider
    {
        public List<LogRecord> Records { get; } = [];

        public ILogger CreateLogger(string categoryName) => new ListLogger(this.Records);

        public void Dispose()
        {
        }
    }

    private sealed class ListLoggerFactory : ILoggerFactory
    {
        private readonly ListLoggerProvider _provider;

        public ListLoggerFactory(ListLoggerProvider provider)
        {
            this._provider = provider;
        }

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public ILogger CreateLogger(string categoryName) => this._provider.CreateLogger(categoryName);

        public void Dispose() => this._provider.Dispose();
    }

    private sealed class DictionaryServiceProvider(IReadOnlyDictionary<Type, object> services) : IServiceProvider
    {
        public object? GetService(Type serviceType) =>
            services.TryGetValue(serviceType, out object? service) ? service : null;
    }

    private sealed class ListLogger(List<LogRecord> records) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            records.Add(new LogRecord(logLevel, eventId, exception, formatter(state, exception)));
        }
    }

    private sealed record LogRecord(
        LogLevel Level,
        EventId EventId,
        Exception? Exception,
        string Message);
}
