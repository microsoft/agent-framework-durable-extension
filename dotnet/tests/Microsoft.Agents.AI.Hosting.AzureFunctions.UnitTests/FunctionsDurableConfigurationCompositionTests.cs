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
    public static TheoryData<string> BuiltInEntryPointNames() =>
    [
        "AgentHttp",
        "AgentEntity",
        "WorkflowOrchestrationHttp",
        "WorkflowOrchestration",
        "WorkflowActivity",
        "WorkflowStatusHttp",
    ];

    private static string ResolveEntryPoint(string name) => name switch
    {
        "AgentHttp" => BuiltInFunctions.RunAgentHttpFunctionEntryPoint,
        "AgentEntity" => BuiltInFunctions.RunAgentEntityFunctionEntryPoint,
        "WorkflowOrchestrationHttp" => BuiltInFunctions.RunWorkflowOrchestrationHttpFunctionEntryPoint,
        "WorkflowOrchestration" => BuiltInFunctions.RunWorkflowOrchestrationFunctionEntryPoint,
        "WorkflowActivity" => BuiltInFunctions.InvokeWorkflowActivityFunctionEntryPoint,
        "WorkflowStatusHttp" => BuiltInFunctions.GetWorkflowStatusHttpFunctionEntryPoint,
        _ => throw new ArgumentOutOfRangeException(nameof(name), name, "Unknown built-in entry point."),
    };

    [Theory]
    [MemberData(nameof(BuiltInEntryPointNames))]
    public async Task ConfigureDurableAgentsThenWorkflows_RoutesAllBuiltInEntryPointsAsync(string entryPointName)
    {
        FunctionsApplicationBuilder builder = FunctionsApplication.CreateBuilder([]);
        builder.ConfigureDurableAgents(agents => agents.AddAIAgent(new TestAgent("AgentsFirstAgent", "desc")));
        builder.ConfigureDurableWorkflows(workflows => workflows.AddWorkflow(BuildWorkflow("AgentsFirstWorkflow")));

        await AssertRoutedToBuiltInExecutorAsync(builder, ResolveEntryPoint(entryPointName));
    }

    [Theory]
    [MemberData(nameof(BuiltInEntryPointNames))]
    public async Task ConfigureDurableWorkflowsThenAgents_RoutesAllBuiltInEntryPointsAsync(string entryPointName)
    {
        FunctionsApplicationBuilder builder = FunctionsApplication.CreateBuilder([]);
        builder.ConfigureDurableWorkflows(workflows => workflows.AddWorkflow(BuildWorkflow("WorkflowsFirstWorkflow")));
        builder.ConfigureDurableAgents(agents => agents.AddAIAgent(new TestAgent("WorkflowsFirstAgent", "desc")));

        await AssertRoutedToBuiltInExecutorAsync(builder, ResolveEntryPoint(entryPointName));
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
