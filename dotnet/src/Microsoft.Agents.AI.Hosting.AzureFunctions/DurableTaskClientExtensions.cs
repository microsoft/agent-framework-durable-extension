// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Agents.AI.DurableTask;
using Microsoft.Agents.AI.DurableTask.Workflows;
using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.DependencyInjection;

namespace Microsoft.Agents.AI.Hosting.AzureFunctions;

/// <summary>
/// Extension methods for the <see cref="DurableTaskClient"/> class.
/// </summary>
public static class DurableTaskClientExtensions
{
    /// <summary>
    /// Converts a <see cref="DurableTaskClient"/> to a durable agent proxy.
    /// </summary>
    /// <param name="durableClient">The <see cref="DurableTaskClient"/> to convert.</param>
    /// <param name="context">The <see cref="FunctionContext"/> for the current function invocation.</param>
    /// <param name="agentName">The name of the agent.</param>
    /// <returns>A durable agent proxy.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="durableClient"/> or <paramref name="context"/> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="agentName"/> is null or empty.</exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when durable agents have not been configured on the service collection.
    /// </exception>
    /// <exception cref="AgentNotRegisteredException">
    /// Thrown when the agent has not been registered.
    /// </exception>
    public static AIAgent AsDurableAgentProxy(
        this DurableTaskClient durableClient,
        FunctionContext context,
        string agentName)
    {
        ArgumentNullException.ThrowIfNull(durableClient);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentException.ThrowIfNullOrEmpty(agentName);

        // Validate that the agent is registered
        DurableTask.ServiceCollectionExtensions.ValidateAgentIsRegistered(context.InstanceServices, agentName);

        DefaultDurableAgentClient agentClient = ActivatorUtilities.CreateInstance<DefaultDurableAgentClient>(
            context.InstanceServices,
            durableClient);

        return new DurableAIAgentProxy(agentName, agentClient);
    }

    /// <summary>
    /// Gets an <see cref="IWorkflowClient"/> for starting and monitoring durable workflows that were
    /// registered with the function app.
    /// </summary>
    /// <remarks>
    /// This allows any function to invoke a registered workflow directly, without constructing an
    /// endpoint URI or issuing an HTTP request. The workflow is started through the durable backend
    /// rather than through the workflow's generated HTTP route, so the call path does not depend on
    /// that route and works from any trigger type.
    /// </remarks>
    /// <param name="durableClient">The <see cref="DurableTaskClient"/> obtained from a <c>[DurableClient]</c> binding.</param>
    /// <param name="context">The <see cref="FunctionContext"/> for the current function invocation.</param>
    /// <returns>A workflow client scoped to the current function invocation.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="durableClient"/> or <paramref name="context"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// Thrown when durable services have not been configured on the application builder. Note that a
    /// client is returned even when no workflows are registered; starting an unregistered workflow by
    /// name then throws <see cref="WorkflowNotRegisteredException"/>.
    /// </exception>
    /// <example>
    /// <code>
    /// [Function(nameof(CancelOrder))]
    /// public async Task&lt;IActionResult&gt; CancelOrder(
    ///     [QueueTrigger("order-cancellations")] string orderId,
    ///     [DurableClient] DurableTaskClient durableClient,
    ///     FunctionContext context)
    /// {
    ///     IWorkflowClient workflows = durableClient.AsWorkflowClient(context);
    ///     IWorkflowRun run = await workflows.RunAsync("CancelOrder", orderId);
    ///     return new OkObjectResult(run.RunId);
    /// }
    /// </code>
    /// </example>
    public static IWorkflowClient AsWorkflowClient(
        this DurableTaskClient durableClient,
        FunctionContext context)
    {
        ArgumentNullException.ThrowIfNull(durableClient);
        ArgumentNullException.ThrowIfNull(context);

        DurableOptions options = context.InstanceServices.GetService<DurableOptions>()
            ?? throw new InvalidOperationException(
                $"Durable services have not been configured. Ensure {nameof(FunctionsApplicationBuilderExtensions.ConfigureDurableWorkflows)} " +
                $"or {nameof(FunctionsApplicationBuilderExtensions.ConfigureDurableOptions)} has been called on the application builder.");

        return new DurableWorkflowClient(durableClient, options);
    }
}
