// Copyright (c) Microsoft. All rights reserved.

using System.Collections;
using System.Reflection;
using Microsoft.Agents.AI.Workflows;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Azure.Functions.Worker.Core.FunctionMetadata;
using Microsoft.Azure.Functions.Worker.Invocation;
using Microsoft.Azure.Functions.Worker.Middleware;
using Microsoft.Extensions.DependencyInjection;
using Moq;

namespace Microsoft.Agents.AI.Hosting.AzureFunctions.UnitTests;

/// <summary>
/// Tests that <see cref="FunctionsApplicationBuilderExtensions.ConfigureDurableAgents"/> and
/// <see cref="FunctionsApplicationBuilderExtensions.ConfigureDurableWorkflows"/> compose when both are
/// called on the same Functions app, instead of forcing users onto
/// <see cref="FunctionsApplicationBuilderExtensions.ConfigureDurableOptions"/>.
/// </summary>
/// <remarks>
/// Regression coverage for https://github.com/microsoft/agent-framework-durable-extension/issues/27.
/// </remarks>
public sealed class FunctionsDurableConfigurationCompositionTests
{
    /// <summary>
    /// Entry points that must be routed to <see cref="BuiltInFunctionExecutor"/> once both agents and
    /// workflows are configured. Without the built-in executor the generated functions have no
    /// implementation to run.
    /// </summary>
    /// <remarks>
    /// Discovered by reflection rather than listed by hand so that an entry point added to
    /// <see cref="BuiltInFunctions"/> is covered automatically. A new entry point that is not added to the
    /// middleware predicate fails these theories instead of silently going unrouted.
    /// </remarks>
    public static TheoryData<string> BuiltInEntryPointNames() =>
    [
        .. typeof(BuiltInFunctions)
            .GetFields(BindingFlags.NonPublic | BindingFlags.Static)
            .Where(field => field.FieldType == typeof(string)
                && field.Name.EndsWith("FunctionEntryPoint", StringComparison.Ordinal))
            .Select(field => (string)field.GetValue(null)!)
            .OrderBy(entryPoint => entryPoint, StringComparer.Ordinal)
    ];

    [Theory]
    [MemberData(nameof(BuiltInEntryPointNames))]
    public async Task ConfigureDurableAgentsThenWorkflows_RoutesAllBuiltInEntryPointsAsync(string entryPoint)
    {
        FunctionsApplicationBuilder builder = FunctionsApplication.CreateBuilder([]);
        builder.ConfigureDurableAgents(agents => agents.AddAIAgent(new TestAgent("AgentsFirstAgent", "desc")));
        builder.ConfigureDurableWorkflows(workflows => workflows.AddWorkflow(BuildWorkflow("AgentsFirstWorkflow")));

        await AssertRoutedToBuiltInExecutorAsync(builder, entryPoint);
    }

    [Theory]
    [MemberData(nameof(BuiltInEntryPointNames))]
    public async Task ConfigureDurableWorkflowsThenAgents_RoutesAllBuiltInEntryPointsAsync(string entryPoint)
    {
        FunctionsApplicationBuilder builder = FunctionsApplication.CreateBuilder([]);
        builder.ConfigureDurableWorkflows(workflows => workflows.AddWorkflow(BuildWorkflow("WorkflowsFirstWorkflow")));
        builder.ConfigureDurableAgents(agents => agents.AddAIAgent(new TestAgent("WorkflowsFirstAgent", "desc")));

        await AssertRoutedToBuiltInExecutorAsync(builder, entryPoint);
    }

    [Fact]
    public void ConfigureDurableAgentsAndWorkflows_RegistersBuiltInMiddlewareOnce()
    {
        FunctionsApplicationBuilder builder = FunctionsApplication.CreateBuilder([]);
        builder.ConfigureDurableWorkflows(workflows => workflows.AddWorkflow(BuildWorkflow("OnceWorkflow")));
        builder.ConfigureDurableAgents(agents => agents.AddAIAgent(new TestAgent("OnceAgent", "desc")));

        Assert.Equal(1, builder.Services.Count(d => d.ServiceType == typeof(BuiltInFunctionExecutionMiddleware)));
        Assert.Equal(1, builder.Services.Count(d => d.ServiceType == typeof(BuiltInFunctionExecutor)));
    }

    [Fact]
    public void ConfigureDurableAgentsAndWorkflows_RegistersBothMetadataTransformers()
    {
        FunctionsApplicationBuilder builder = FunctionsApplication.CreateBuilder([]);
        builder.ConfigureDurableAgents(agents => agents.AddAIAgent(new TestAgent("TransformerAgent", "desc")));
        builder.ConfigureDurableWorkflows(workflows => workflows.AddWorkflow(BuildWorkflow("TransformerWorkflow")));

        List<Type?> transformers = [.. builder.Services
            .Where(d => d.ServiceType == typeof(IFunctionMetadataTransformer))
            .Select(d => d.ImplementationType)];

        Assert.Contains(typeof(DurableAgentFunctionMetadataTransformer), transformers);
        Assert.Contains(typeof(DurableWorkflowsFunctionMetadataTransformer), transformers);
    }

    /// <summary>
    /// An agent registered through <c>ConfigureDurableOptions</c> must get the same entry points it would get
    /// from <c>ConfigureDurableAgents</c>. Previously it got none at all, not even the entity trigger, which
    /// left the agent unreachable with no error reported anywhere.
    /// </summary>
    [Fact]
    public void ConfigureDurableOptions_GeneratesSameAgentFunctionsAsConfigureDurableAgents()
    {
        List<string> viaOptions = GenerateFunctionNames(builder =>
            builder.ConfigureDurableOptions(options => options.Agents.AddAIAgent(new TestAgent("ParityOptionsAgent", "desc"))));

        List<string> viaAgents = GenerateFunctionNames(builder =>
            builder.ConfigureDurableAgents(agents => agents.AddAIAgent(new TestAgent("ParityAgentsAgent", "desc"))));

        Assert.Equal(
            viaAgents.Select(name => name.Replace("ParityAgentsAgent", "<agent>", StringComparison.Ordinal)),
            viaOptions.Select(name => name.Replace("ParityOptionsAgent", "<agent>", StringComparison.Ordinal)));

        Assert.Contains("dafx-ParityOptionsAgent", viaOptions);
        Assert.Contains("http-ParityOptionsAgent", viaOptions);
    }

    /// <summary>
    /// An agent that exists only because a workflow references it is an implementation detail of that
    /// workflow, so it must not get its own HTTP endpoint even when the workflow is registered through
    /// <c>ConfigureDurableOptions</c>, which now applies agent defaults.
    /// </summary>
    [Fact]
    public void ConfigureDurableOptions_DoesNotGiveWorkflowRegisteredAgentsAnHttpEndpoint()
    {
        List<string> functions = GenerateFunctionNames(builder =>
            builder.ConfigureDurableOptions(options =>
                options.Workflows.AddWorkflow(BuildAgentWorkflow("ImplicitWorkflow", "ImplicitAgent"))));

        Assert.Contains("dafx-ImplicitAgent", functions);
        Assert.DoesNotContain("http-ImplicitAgent", functions);
    }

    /// <summary>
    /// Runs the registered metadata transformers the way the Functions host would, and returns the names of
    /// the functions they generate.
    /// </summary>
    private static List<string> GenerateFunctionNames(Action<FunctionsApplicationBuilder> configure)
    {
        FunctionsApplicationBuilder builder = FunctionsApplication.CreateBuilder([]);
        configure(builder);

        using ServiceProvider provider = builder.Services.BuildServiceProvider();
        List<IFunctionMetadata> metadata = [];

        foreach (IFunctionMetadataTransformer transformer in provider.GetServices<IFunctionMetadataTransformer>())
        {
            transformer.Transform(metadata);
        }

        return [.. metadata.Select(m => m.Name!)];
    }

    private static async Task AssertRoutedToBuiltInExecutorAsync(FunctionsApplicationBuilder builder, string entryPoint)
    {
        using ServiceProvider provider = builder.Services.BuildServiceProvider();
        FunctionExecutionDelegate pipeline = BuildInvocationPipeline(builder);

        TestInvocationFeatures features = new();
        FunctionContext context = CreateContext(provider, features, entryPoint);

        await pipeline(context);

        IFunctionExecutor? executor = features.Get<IFunctionExecutor>();
        Assert.NotNull(executor);
        Assert.IsType<BuiltInFunctionExecutor>(executor);
    }

    /// <summary>
    /// Builds the middleware pipeline that the Functions host would build. The pipeline is held by the
    /// worker builder that <see cref="FunctionsApplicationBuilder"/> wraps and is not otherwise reachable
    /// before <c>Build()</c>.
    /// </summary>
    private static FunctionExecutionDelegate BuildInvocationPipeline(FunctionsApplicationBuilder builder)
    {
        object workerBuilder = GetFieldValue(
            builder,
            f => typeof(IFunctionsWorkerApplicationBuilder).IsAssignableFrom(f.FieldType));

        object pipelineBuilder = GetFieldValue(
            workerBuilder,
            f => f.FieldType.Name.StartsWith("IInvocationPipelineBuilder", StringComparison.Ordinal));

        MethodInfo build = pipelineBuilder.GetType().GetMethod("Build", Type.EmptyTypes)
            ?? throw new InvalidOperationException("Could not find the invocation pipeline Build method.");

        return (FunctionExecutionDelegate)build.Invoke(pipelineBuilder, null)!;
    }

    private static object GetFieldValue(object instance, Func<FieldInfo, bool> predicate)
    {
        FieldInfo field = Array.Find(
            instance.GetType().GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public),
            f => predicate(f))
            ?? throw new InvalidOperationException(
                $"Could not locate the expected field on {instance.GetType().FullName}.");

        return field.GetValue(instance)
            ?? throw new InvalidOperationException(
                $"Field '{field.Name}' on {instance.GetType().FullName} was null.");
    }

    private static FunctionContext CreateContext(
        IServiceProvider services,
        IInvocationFeatures features,
        string entryPoint)
    {
        Mock<FunctionDefinition> definition = new();
        definition.SetupGet(d => d.EntryPoint).Returns(entryPoint);
        definition.SetupGet(d => d.Name).Returns("test-function");

        Mock<FunctionContext> context = new();
        context.SetupGet(c => c.InstanceServices).Returns(services);
        context.SetupGet(c => c.FunctionDefinition).Returns(definition.Object);
        context.SetupGet(c => c.Features).Returns(features);

        return context.Object;
    }

    private static Workflow BuildWorkflow(string name) =>
        new WorkflowBuilder(new FunctionExecutor<string>("start", (_, _, _) => default))
            .WithName(name)
            .Build();

    /// <summary>
    /// Builds a workflow that references an agent, so the agent is auto-registered as a side effect of
    /// registering the workflow rather than by an explicit agent registration call.
    /// </summary>
    private static Workflow BuildAgentWorkflow(string workflowName, string agentName)
    {
        FunctionExecutor<string> start = new("start", (_, _, _) => default);

        return new WorkflowBuilder(start)
            .WithName(workflowName)
            .AddEdge(start, new TestAgent(agentName, "desc"))
            .Build();
    }

    private sealed class TestInvocationFeatures : IInvocationFeatures
    {
        private readonly Dictionary<Type, object> _features = [];

        public T? Get<T>() => this._features.TryGetValue(typeof(T), out object? value) ? (T)value : default;

        public void Set<T>(T instance)
        {
            if (instance is null)
            {
                this._features.Remove(typeof(T));
            }
            else
            {
                this._features[typeof(T)] = instance;
            }
        }

        public IEnumerator<KeyValuePair<Type, object>> GetEnumerator() => this._features.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => this.GetEnumerator();
    }
}
