// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.Workflows;
using Microsoft.Agents.AI.Workflows;
using Microsoft.DurableTask.Worker;
using Microsoft.Extensions.AI;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Microsoft.Agents.AI.DurableTask.UnitTests;

/// <summary>
/// Tests that <see cref="ServiceCollectionExtensions.ConfigureDurableAgents"/> and
/// <see cref="ServiceCollectionExtensions.ConfigureDurableWorkflows"/> compose when both are called
/// on the same application, instead of forcing users onto
/// <see cref="ServiceCollectionExtensions.ConfigureDurableOptions"/>.
/// </summary>
/// <remarks>
/// Regression coverage for https://github.com/microsoft/agent-framework-durable-extension/issues/27.
/// </remarks>
public sealed class DurableConfigurationCompositionTests
{
    [Fact]
    public void ConfigureDurableAgentsThenWorkflows_RegistersBothAgentAndWorkflow()
    {
        ServiceCollection services = new();

        services.ConfigureDurableAgents(agents => agents.AddAIAgent(new CompositionTestAgent("Assistant")));
        services.ConfigureDurableWorkflows(workflows => workflows.AddWorkflow(BuildWorkflow("Translate")));

        DurableOptions options = GetRegisteredOptions(services);

        Assert.Contains("Assistant", options.Agents.GetAgentFactories().Keys);
        Assert.Contains("Translate", options.Workflows.Workflows.Keys);
    }

    [Fact]
    public void ConfigureDurableWorkflowsThenAgents_RegistersBothAgentAndWorkflow()
    {
        ServiceCollection services = new();

        services.ConfigureDurableWorkflows(workflows => workflows.AddWorkflow(BuildWorkflow("Translate")));
        services.ConfigureDurableAgents(agents => agents.AddAIAgent(new CompositionTestAgent("Assistant")));

        DurableOptions options = GetRegisteredOptions(services);

        Assert.Contains("Assistant", options.Agents.GetAgentFactories().Keys);
        Assert.Contains("Translate", options.Workflows.Workflows.Keys);
    }

    /// <summary>
    /// The first <c>Configure*</c> call latches the core service registrations, so worker and client
    /// builders supplied to a later call must still be honored.
    /// </summary>
    [Fact]
    public void ConfigureDurableAgentsThenWorkflows_HonorsWorkerAndClientBuildersFromSecondCall()
    {
        ServiceCollection services = new();

        services.ConfigureDurableAgents(agents => agents.AddAIAgent(new CompositionTestAgent("Assistant")));
        services.ConfigureDurableWorkflows(
            workflows => workflows.AddWorkflow(BuildWorkflow("Translate")),
            workerBuilder: _ => { },
            clientBuilder: _ => { });

        Assert.Contains(services, d => d.ServiceType == typeof(IWorkflowClient));
    }

    /// <summary>
    /// Builders supplied to the first call must survive additional <c>Configure*</c> calls, and the
    /// core services must not be registered twice.
    /// </summary>
    [Fact]
    public void ConfigureDurableAgentsThenWorkflows_RegistersCoreServicesExactlyOnce()
    {
        ServiceCollection services = new();

        services.ConfigureDurableAgents(
            agents => agents.AddAIAgent(new CompositionTestAgent("Assistant")),
            workerBuilder: _ => { },
            clientBuilder: _ => { });
        services.ConfigureDurableWorkflows(
            workflows => workflows.AddWorkflow(BuildWorkflow("Translate")),
            workerBuilder: _ => { },
            clientBuilder: _ => { });

        Assert.Equal(1, services.Count(d => d.ServiceType == typeof(IWorkflowClient)));
        Assert.Equal(1, services.Count(d => d.ServiceType == typeof(DurableOptions)));
    }

    /// <summary>
    /// When more than one call supplies a builder, the first non-null delegate wins and the rest are ignored.
    /// Passing the same builder to every call is the common case, so applying each one would configure the
    /// worker and client repeatedly.
    /// </summary>
    [Fact]
    public void ConfigureDurableAgentsThenWorkflows_AppliesOnlyTheFirstSuppliedBuilders()
    {
        ServiceCollection services = new();
        List<string> workerBuilderCalls = [];
        List<string> clientBuilderCalls = [];

        services.ConfigureDurableAgents(
            agents => agents.AddAIAgent(new CompositionTestAgent("FirstWinsAssistant")),
            workerBuilder: _ => workerBuilderCalls.Add("agents"),
            clientBuilder: _ => clientBuilderCalls.Add("agents"));
        services.ConfigureDurableWorkflows(
            workflows => workflows.AddWorkflow(BuildWorkflow("FirstWinsTranslate")),
            workerBuilder: _ => workerBuilderCalls.Add("workflows"),
            clientBuilder: _ => clientBuilderCalls.Add("workflows"));

        Assert.Equal(["agents"], workerBuilderCalls);
        Assert.Equal(["agents"], clientBuilderCalls);
    }

    /// <summary>
    /// A workflow auto-registers the agents it references, so configuring workflows first must not stop the
    /// caller from registering the same agent explicitly afterwards. The explicit registration promotes the
    /// agent to a standalone agent instead of throwing "already registered".
    /// </summary>
    [Fact]
    public void ConfigureDurableWorkflowsThenAgents_PromotesWorkflowRegisteredAgentInsteadOfThrowing()
    {
        ServiceCollection services = new();
        CompositionTestAgent agent = new("PromotedAssistant");

        services.ConfigureDurableWorkflows(
            workflows => workflows.AddWorkflow(BuildAgentWorkflow("PromotedWorkflow", agent)));

        DurableOptions options = GetRegisteredOptions(services);
        Assert.True(options.Agents.ContainsAgent("PromotedAssistant"));
        Assert.True(options.Agents.IsWorkflowRegisteredAgent("PromotedAssistant"));

        services.ConfigureDurableAgents(agents => agents.AddAIAgent(agent));

        Assert.True(options.Agents.ContainsAgent("PromotedAssistant"));
        Assert.False(options.Agents.IsWorkflowRegisteredAgent("PromotedAssistant"));
    }

    /// <summary>
    /// Promotion only applies to agents a workflow registered. Registering the same name explicitly twice is
    /// still a caller error.
    /// </summary>
    [Fact]
    public void ConfigureDurableAgents_ThrowsWhenTheSameAgentIsRegisteredExplicitlyTwice()
    {
        ServiceCollection services = new();

        services.ConfigureDurableAgents(agents => agents.AddAIAgent(new CompositionTestAgent("DuplicateAssistant")));

        ArgumentException exception = Assert.Throws<ArgumentException>(
            () => services.ConfigureDurableAgents(agents => agents.AddAIAgent(new CompositionTestAgent("DuplicateAssistant"))));

        Assert.Contains("has already been registered", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("v2-assistant")]
    [InlineData("V2-Assistant")]
    [InlineData("v2-")]
    public void ConfigureDurableAgents_RejectsReservedV2AgentNamePrefix(string reservedName)
    {
        ServiceCollection services = new();

        ArgumentException agentException = Assert.Throws<ArgumentException>(
            () => services.ConfigureDurableAgents(agents => agents.AddAIAgent(new CompositionTestAgent(reservedName))));
        Assert.Contains("v2-", agentException.Message, StringComparison.OrdinalIgnoreCase);

        ArgumentException factoryException = Assert.Throws<ArgumentException>(
            () => services.ConfigureDurableAgents(agents => agents.AddAIAgentFactory(reservedName, _ => new CompositionTestAgent("valid"))));
        Assert.Contains("v2-", factoryException.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ConfigureDurableOptions_PopulatesWorkItemFiltersForPhase1AndPhase2(bool processLegacyEntities)
    {
        ServiceCollection services = new();
        CompositionTestAgent standaloneAgent = new("StandaloneAssistant");
        CompositionTestAgent workflowAgent = new("WorkflowAssistant");

        services.ConfigureDurableOptions(
            options =>
            {
                options.Agents.ProcessLegacyEntities = processLegacyEntities;
                options.Agents.AddAIAgent(standaloneAgent);
                options.Workflows.AddWorkflow(BuildAgentWorkflow("OrderWorkflow", workflowAgent));
            },
            workerBuilder: _ => { });

        using ServiceProvider provider = services.BuildServiceProvider();
        DurableTaskWorkerWorkItemFilters filters =
            provider.GetRequiredService<IOptionsMonitor<DurableTaskWorkerWorkItemFilters>>().Get(Options.DefaultName);

        string[] entityNames = [.. filters.Entities.Select(e => e.Name)];
        Assert.Contains("dafx-v2-StandaloneAssistant", entityNames);
        Assert.Contains("dafx-v2-WorkflowAssistant", entityNames);

        if (processLegacyEntities)
        {
            Assert.Contains("dafx-StandaloneAssistant", entityNames);
            Assert.Contains("dafx-WorkflowAssistant", entityNames);
        }
        else
        {
            Assert.DoesNotContain("dafx-StandaloneAssistant", entityNames);
            Assert.DoesNotContain("dafx-WorkflowAssistant", entityNames);
        }

        Assert.Contains(filters.Orchestrations, o => o.Name == "dafx-OrderWorkflow");
        Assert.Contains(filters.Activities, a => a.Name == "dafx-start");
    }

    [Fact]
    public void ConfigureDurableAgents_AllowsWorkerBuilderToOverrideWorkItemFilters()
    {
        ServiceCollection services = new();
        DurableTaskWorkerWorkItemFilters customFilters = new()
        {
            Entities = [new DurableTaskWorkerWorkItemFilters.EntityFilter { Name = "dafx-v2-PrimaryAssistant" }],
        };

        services.ConfigureDurableAgents(
            agents =>
            {
                agents.AddAIAgent(new CompositionTestAgent("PrimaryAssistant"));
                agents.AddAIAgent(new CompositionTestAgent("SecondaryAssistant"));
            },
            workerBuilder: builder => builder.UseWorkItemFilters(customFilters));

        using ServiceProvider provider = services.BuildServiceProvider();
        DurableTaskWorkerWorkItemFilters resolved =
            provider.GetRequiredService<IOptionsMonitor<DurableTaskWorkerWorkItemFilters>>().Get(Options.DefaultName);

        Assert.Equal(["dafx-v2-PrimaryAssistant"], resolved.Entities.Select(e => e.Name));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ConfigureDurableOptions_RejectsCrossKindCollisionBetweenWorkflowV2PrefixAndAgent(bool workflowFirst)
    {
        ServiceCollection services = new();

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(() =>
        {
            if (workflowFirst)
            {
                services.ConfigureDurableWorkflows(workflows => workflows.AddWorkflow(BuildWorkflow("v2-Writer")));
                services.ConfigureDurableAgents(agents => agents.AddAIAgent(new CompositionTestAgent("Writer")));
            }
            else
            {
                services.ConfigureDurableAgents(agents => agents.AddAIAgent(new CompositionTestAgent("Writer")));
                services.ConfigureDurableWorkflows(workflows => workflows.AddWorkflow(BuildWorkflow("v2-Writer")));
            }
        });

        Assert.Contains("dafx-v2-Writer", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ConfigureDurableOptions_RejectsCrossKindCollisionBetweenActivityV2PrefixAndAgent(bool workflowFirst)
    {
        ServiceCollection services = new();
        Workflow workflowWithCollidingActivity = new WorkflowBuilder(new FunctionExecutor<string>("v2-Writer", (_, _, _) => default))
            .WithName("PublishingWorkflow")
            .Build();

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(() =>
        {
            if (workflowFirst)
            {
                services.ConfigureDurableWorkflows(workflows => workflows.AddWorkflow(workflowWithCollidingActivity));
                services.ConfigureDurableAgents(agents => agents.AddAIAgent(new CompositionTestAgent("Writer")));
            }
            else
            {
                services.ConfigureDurableAgents(agents => agents.AddAIAgent(new CompositionTestAgent("Writer")));
                services.ConfigureDurableWorkflows(workflows => workflows.AddWorkflow(workflowWithCollidingActivity));
            }
        });

        Assert.Contains("dafx-v2-Writer", exception.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ConfigureDurableOptions_AllowsRequestPortAndSubworkflowAliasSharingNameWithAgent(bool workflowFirst)
    {
        ServiceCollection services = new();
        FunctionExecutor<string> childStart = new("child-start", (_, _, _) => default);
        Workflow subWorkflow = new WorkflowBuilder(childStart)
            .WithName("ChildReviewWorkflow")
            .Build();
        FunctionExecutor<string> parentStart = new("parent-start", (_, _, _) => default);
        RequestPort<string, string> requestPort = RequestPort.Create<string, string>("ApprovalAgent");
        ExecutorBinding subWorkflowAlias = subWorkflow.BindAsExecutor("SubworkflowAgent");
        Workflow parentWorkflow = new WorkflowBuilder(parentStart)
            .WithName("ParentApprovalWorkflow")
            .AddEdge(parentStart, requestPort)
            .AddEdge(requestPort, subWorkflowAlias)
            .Build();

        if (workflowFirst)
        {
            services.ConfigureDurableWorkflows(workflows => workflows.AddWorkflow(parentWorkflow));
            services.ConfigureDurableAgents(agents =>
            {
                agents.AddAIAgent(new CompositionTestAgent("ApprovalAgent"));
                agents.AddAIAgent(new CompositionTestAgent("SubworkflowAgent"));
            });
        }
        else
        {
            services.ConfigureDurableAgents(agents =>
            {
                agents.AddAIAgent(new CompositionTestAgent("ApprovalAgent"));
                agents.AddAIAgent(new CompositionTestAgent("SubworkflowAgent"));
            });
            services.ConfigureDurableWorkflows(workflows => workflows.AddWorkflow(parentWorkflow));
        }

        DurableOptions options = GetRegisteredOptions(services);
        Assert.True(options.Agents.ContainsAgent("ApprovalAgent"));
        Assert.True(options.Agents.ContainsAgent("SubworkflowAgent"));
        Assert.Contains("ParentApprovalWorkflow", options.Workflows.Workflows.Keys);
    }

    private static DurableOptions GetRegisteredOptions(IServiceCollection services)
    {
        ServiceDescriptor descriptor = Assert.Single(
            services, d => d.ServiceType == typeof(DurableOptions));

        return Assert.IsType<DurableOptions>(descriptor.ImplementationInstance, exactMatch: false);
    }

    private static Workflow BuildWorkflow(string name) =>
        new WorkflowBuilder(new FunctionExecutor<string>("start", (_, _, _) => default))
            .WithName(name)
            .Build();

    /// <summary>
    /// Builds a workflow that references <paramref name="agent"/>, so the agent is registered as a side effect
    /// of registering the workflow rather than by an explicit agent registration call.
    /// </summary>
    private static Workflow BuildAgentWorkflow(string workflowName, AIAgent agent)
    {
        FunctionExecutor<string> start = new("start", (_, _, _) => default);

        return new WorkflowBuilder(start)
            .WithName(workflowName)
            .AddEdge(start, agent)
            .Build();
    }

    private sealed class CompositionTestAgent(string name) : AIAgent
    {
        public override string? Name => name;

        protected override ValueTask<AgentSession> CreateSessionCoreAsync(CancellationToken cancellationToken = default)
            => new(new EmptySession());

        protected override ValueTask<JsonElement> SerializeSessionCoreAsync(
            AgentSession session,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();

        protected override ValueTask<AgentSession> DeserializeSessionCoreAsync(
            JsonElement serializedState,
            JsonSerializerOptions? jsonSerializerOptions = null,
            CancellationToken cancellationToken = default) => new(new EmptySession());

        protected override Task<AgentResponse> RunCoreAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            CancellationToken cancellationToken = default) => Task.FromResult(new AgentResponse([.. messages]));

        protected override IAsyncEnumerable<AgentResponseUpdate> RunCoreStreamingAsync(
            IEnumerable<ChatMessage> messages,
            AgentSession? session = null,
            AgentRunOptions? options = null,
            CancellationToken cancellationToken = default) => throw new NotSupportedException();

        private sealed class EmptySession : AgentSession;
    }
}
