// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Agents.AI.DurableTask;
using Microsoft.Agents.AI.DurableTask.Workflows;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Azure.Functions.Worker.Core.FunctionMetadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Microsoft.Agents.AI.Hosting.AzureFunctions;

/// <summary>
/// Extension methods for the <see cref="FunctionsApplicationBuilder"/> class.
/// </summary>
public static class FunctionsApplicationBuilderExtensions
{
    /// <summary>
    /// Configures the application to use durable agents with a builder pattern.
    /// </summary>
    /// <remarks>
    /// Multiple calls to this method, and calls combined with <see cref="ConfigureDurableWorkflows"/> or
    /// <see cref="ConfigureDurableOptions"/>, are supported and compose additively.
    /// </remarks>
    /// <param name="builder">The functions application builder.</param>
    /// <param name="configure">A delegate to configure the durable agents.</param>
    /// <returns>The functions application builder.</returns>
    public static FunctionsApplicationBuilder ConfigureDurableAgents(
        this FunctionsApplicationBuilder builder,
        Action<DurableAgentsOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        return ConfigureDurableCore(builder, options => configure(options.Agents));
    }

    /// <summary>
    /// Configures durable options for the functions application, allowing customization of Durable Task framework
    /// settings.
    /// </summary>
    /// <remarks>This method ensures that a single shared <see cref="DurableOptions"/> instance is used across all
    /// configuration calls. If any workflows have been added, it configures the necessary orchestrations and registers
    /// required middleware. Agents added through this method get the same entry points they would get from
    /// <see cref="ConfigureDurableAgents"/>.</remarks>
    /// <param name="builder">The functions application builder to configure. Cannot be null.</param>
    /// <param name="configure">An action that configures the <see cref="DurableOptions"/> instance. Cannot be null.</param>
    /// <returns>The updated <see cref="FunctionsApplicationBuilder"/> instance, enabling method chaining.</returns>
    public static FunctionsApplicationBuilder ConfigureDurableOptions(
        this FunctionsApplicationBuilder builder,
        Action<DurableOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        return ConfigureDurableCore(builder, configure);
    }

    /// <summary>
    /// Configures durable workflow support for the specified Azure Functions application builder.
    /// </summary>
    /// <remarks>
    /// Multiple calls to this method, and calls combined with <see cref="ConfigureDurableAgents"/> or
    /// <see cref="ConfigureDurableOptions"/>, are supported and compose additively.
    /// </remarks>
    /// <param name="builder">The <see cref="FunctionsApplicationBuilder"/> instance to configure for durable workflows.</param>
    /// <param name="configure">An action that configures the <see cref="DurableWorkflowOptions"/>, allowing customization of durable workflow behavior.</param>
    /// <returns>The updated <see cref="FunctionsApplicationBuilder"/> instance, enabling method chaining.</returns>
    public static FunctionsApplicationBuilder ConfigureDurableWorkflows(
        this FunctionsApplicationBuilder builder,
        Action<DurableWorkflowOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        return ConfigureDurableCore(builder, options => configure(options.Workflows));
    }

    /// <summary>
    /// Applies a configuration delegate to the shared <see cref="DurableOptions"/> instance and registers the
    /// Functions-specific services. All public configuration entry points funnel through here so that agents and
    /// workflows are wired identically no matter which combination of methods the application calls, or in which
    /// order.
    /// </summary>
    /// <param name="builder">The functions application builder.</param>
    /// <param name="configure">A delegate to apply to the shared durable options.</param>
    /// <remarks>
    /// Agents added by <paramref name="configure"/> that have no explicit <see cref="FunctionsAgentOptions"/>
    /// receive the defaults for the agent-focused entry point. Agents that exist only because a workflow
    /// references them are skipped: they are an implementation detail of that workflow rather than separately
    /// addressable agents, so they must not get their own HTTP endpoint.
    /// </remarks>
    private static FunctionsApplicationBuilder ConfigureDurableCore(
        FunctionsApplicationBuilder builder,
        Action<DurableOptions> configure)
    {
        // Ensure FunctionsDurableOptions is registered BEFORE the core extension creates a plain DurableOptions
        FunctionsDurableOptions sharedOptions = GetOrCreateSharedOptions(builder.Services);

        // Agent names are case-insensitive everywhere else, so this snapshot must match that comparer.
        HashSet<string> agentsBeforeConfigure = new(
            sharedOptions.Agents.GetAgentFactories().Keys,
            StringComparer.OrdinalIgnoreCase);

        builder.Services.ConfigureDurableOptions(configure);

        DurableAgentsOptionsExtensions.EnsureDefaultOptionsForAll(
            sharedOptions.Agents.GetAgentFactories().Keys
                .Where(name => !agentsBeforeConfigure.Contains(name) && !sharedOptions.Agents.IsWorkflowRegisteredAgent(name)));

        if (DurableAgentsOptionsExtensions.GetAgentOptionsSnapshot().Count > 0)
        {
            builder.Services.TryAddSingleton<IFunctionsAgentOptionsProvider>(_ =>
                new DefaultFunctionsAgentOptionsProvider(DurableAgentsOptionsExtensions.GetAgentOptionsSnapshot()));
            builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IFunctionMetadataTransformer, DurableAgentFunctionMetadataTransformer>());
        }

        if (sharedOptions.Workflows.Workflows.Count > 0)
        {
            builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IFunctionMetadataTransformer, DurableWorkflowsFunctionMetadataTransformer>());
        }

        EnsureMiddlewareRegistered(builder);

        return builder;
    }

    private static void EnsureMiddlewareRegistered(FunctionsApplicationBuilder builder)
    {
        // Guard against registering the middleware filter multiple times in the pipeline.
        if (builder.Services.Any(d => d.ServiceType == typeof(BuiltInFunctionExecutor)))
        {
            return;
        }

        builder.UseWhen<BuiltInFunctionExecutionMiddleware>(static context =>
            string.Equals(context.FunctionDefinition.EntryPoint, BuiltInFunctions.RunAgentHttpFunctionEntryPoint, StringComparison.Ordinal) ||
            string.Equals(context.FunctionDefinition.EntryPoint, BuiltInFunctions.RunAgentMcpToolFunctionEntryPoint, StringComparison.Ordinal) ||
            string.Equals(context.FunctionDefinition.EntryPoint, BuiltInFunctions.RunAgentEntityFunctionEntryPoint, StringComparison.Ordinal) ||
            string.Equals(context.FunctionDefinition.EntryPoint, BuiltInFunctions.RunWorkflowOrchestrationHttpFunctionEntryPoint, StringComparison.Ordinal) ||
            string.Equals(context.FunctionDefinition.EntryPoint, BuiltInFunctions.RunWorkflowOrchestrationFunctionEntryPoint, StringComparison.Ordinal) ||
            string.Equals(context.FunctionDefinition.EntryPoint, BuiltInFunctions.InvokeWorkflowActivityFunctionEntryPoint, StringComparison.Ordinal) ||
            string.Equals(context.FunctionDefinition.EntryPoint, BuiltInFunctions.GetWorkflowStatusHttpFunctionEntryPoint, StringComparison.Ordinal) ||
            string.Equals(context.FunctionDefinition.EntryPoint, BuiltInFunctions.RespondToWorkflowHttpFunctionEntryPoint, StringComparison.Ordinal) ||
            string.Equals(context.FunctionDefinition.EntryPoint, BuiltInFunctions.RunWorkflowMcpToolFunctionEntryPoint, StringComparison.Ordinal)
        );
        builder.Services.TryAddSingleton<BuiltInFunctionExecutor>();
    }

    /// <summary>
    /// Gets or creates a shared <see cref="DurableOptions"/> instance from the service collection.
    /// </summary>
    private static FunctionsDurableOptions GetOrCreateSharedOptions(IServiceCollection services)
    {
        ServiceDescriptor? existingDescriptor = services.FirstOrDefault(
            d => d.ServiceType == typeof(DurableOptions) && d.ImplementationInstance is not null);

        if (existingDescriptor?.ImplementationInstance is FunctionsDurableOptions existing)
        {
            return existing;
        }

        FunctionsDurableOptions options = new();
        services.AddSingleton<DurableOptions>(options);
        services.AddSingleton(options);
        return options;
    }
}
