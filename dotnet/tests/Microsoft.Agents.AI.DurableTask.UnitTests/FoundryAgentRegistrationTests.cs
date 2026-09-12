// Copyright (c) Microsoft. All rights reserved.

using System.ClientModel;
using System.ClientModel.Primitives;
using Azure.AI.Extensions.OpenAI;
using Azure.AI.Projects;
using Microsoft.Agents.AI.Foundry;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit;

public sealed class FoundryAgentRegistrationTests
{
    private const string FoundryServiceHistoryProviderKey = "foundry-managed-service.v1";

    [Fact]
    public async Task ServerManagedFoundryAgentRestoresFixedServiceOwnershipAsync()
    {
        AIProjectClient projectClient = new(
            new Uri("https://example.services.ai.azure.com/api/projects/test"),
            new FakeAuthenticationTokenProvider());
        FoundryAgent foundryAgent =
            projectClient.AsAIAgent(new AgentReference("foundry-managed-agent"));

        ChatClientAgent? innerAgent = foundryAgent.GetService<ChatClientAgent>();
        DurableAgentsOptions options = new();
        AgentSession serviceSession =
            await foundryAgent.CreateSessionAsync("service-conversation-id");
        var serializedSession =
            await foundryAgent.SerializeSessionAsync(serviceSession);
        AgentSession restoredSession =
            await foundryAgent.DeserializeSessionAsync(serializedSession);

        options.AddAIAgent(foundryAgent);
        options.SetHistoryProviderKey(
            foundryAgent.Name!,
            FoundryServiceHistoryProviderKey);
        options.EnableMailboxWrites = true;
        options.HistoryRetentionMode = DurableAgentHistoryRetentionMode.KeepAll;

        Assert.NotNull(innerAgent);
        Assert.Same(innerAgent, DurableAgentHistoryOwnershipResolver.FindChatClientAgent(foundryAgent));
        (DurableAgentHistoryOwnership ownership, ChatClientAgent? restoredInnerAgent) =
            DurableAgentHistoryOwnershipResolver.Resolve(foundryAgent, restoredSession);
        ChatClientAgentSession typedSession =
            Assert.IsType<ChatClientAgentSession>(restoredSession);
        Assert.Equal(DurableAgentHistoryOwnership.Service, ownership);
        Assert.Same(innerAgent, restoredInnerAgent);
        Assert.Equal("service-conversation-id", typedSession.ConversationId);
        Assert.Equal(
            FoundryServiceHistoryProviderKey,
            options.GetHistoryProviderKey(foundryAgent.Name!));
        Assert.True(options.EnableMailboxWrites);
        Assert.Equal(DurableAgentHistoryRetentionMode.KeepAll, options.HistoryRetentionMode);
        Assert.False(options.IsServiceManagedPerServiceCallHistory(foundryAgent.Name!));
    }

    private sealed class FakeAuthenticationTokenProvider : AuthenticationTokenProvider
    {
        public override GetTokenOptions? CreateTokenOptions(
            IReadOnlyDictionary<string, object> properties)
        {
            return new GetTokenOptions(new Dictionary<string, object>());
        }

        public override AuthenticationToken GetToken(
            GetTokenOptions options,
            CancellationToken cancellationToken)
        {
            return new AuthenticationToken(
                "test-token",
                "Bearer",
                DateTimeOffset.UtcNow.AddHours(1));
        }

        public override ValueTask<AuthenticationToken> GetTokenAsync(
            GetTokenOptions options,
            CancellationToken cancellationToken)
        {
            return new(this.GetToken(options, cancellationToken));
        }
    }
}
