// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Agents.AI.DurableTask.Workflows;
using Microsoft.Agents.AI.Hosting.AzureFunctions;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.DurableTask.Client;

namespace SequentialWorkflow;

/// <summary>
/// Demonstrates invoking a registered workflow from your own function code.
/// </summary>
/// <remarks>
/// <para>
/// These functions are hand-written endpoints that sit in front of the <c>CancelOrder</c> workflow,
/// rather than the endpoints the framework generates. Instead of creating an <see cref="HttpClient"/>
/// and POSTing to <c>workflows/CancelOrder/run</c>, they get an <see cref="IWorkflowClient"/> from the
/// <c>[DurableClient]</c> binding and start the workflow by name.
/// </para>
/// <para>
/// The workflow is started through the durable backend rather than through the workflow's generated
/// HTTP route, so the call path does not depend on that route and the same two lines work from any
/// trigger type - queue, timer, Event Grid, Service Bus.
/// </para>
/// </remarks>
public sealed class OrderFunctions
{
    private const string CancelOrderWorkflow = "CancelOrder";

    /// <summary>
    /// Starts the <c>CancelOrder</c> workflow and returns its run ID immediately.
    /// </summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="orderId">The order to cancel, taken from the route.</param>
    /// <param name="durableClient">The Durable Task client provided by the <c>[DurableClient]</c> binding.</param>
    /// <param name="context">The function invocation context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <c>202 Accepted</c> response carrying the workflow run ID.</returns>
    [Function(nameof(CancelOrderAsync))]
    public async Task<IActionResult> CancelOrderAsync(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "orders/{orderId}/cancel")] HttpRequest request,
        string orderId,
        [DurableClient] DurableTaskClient durableClient,
        FunctionContext context,
        CancellationToken cancellationToken)
    {
        // Get a workflow client from the durable client binding. No endpoint URI, no HttpClient.
        IWorkflowClient workflows = durableClient.AsWorkflowClient(context);

        // Start the registered workflow by name. The Workflow object built in Program.cs is not needed here.
        IWorkflowRun run = await workflows.RunAsync(CancelOrderWorkflow, orderId, cancellationToken: cancellationToken);

        return new AcceptedResult(location: null, value: $"Workflow orchestration started for {CancelOrderWorkflow}. Orchestration runId: {run.RunId}");
    }

    /// <summary>
    /// Starts the <c>CancelOrder</c> workflow and waits for it to finish before responding.
    /// </summary>
    /// <param name="request">The HTTP request.</param>
    /// <param name="orderId">The order to cancel, taken from the route.</param>
    /// <param name="durableClient">The Durable Task client provided by the <c>[DurableClient]</c> binding.</param>
    /// <param name="context">The function invocation context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <c>200 OK</c> response carrying the workflow result.</returns>
    [Function(nameof(CancelOrderAndWaitAsync))]
    public async Task<IActionResult> CancelOrderAndWaitAsync(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "orders/{orderId}/cancel-and-wait")] HttpRequest request,
        string orderId,
        [DurableClient] DurableTaskClient durableClient,
        FunctionContext context,
        CancellationToken cancellationToken)
    {
        IWorkflowClient workflows = durableClient.AsWorkflowClient(context);

        IWorkflowRun run = await workflows.RunAsync(CancelOrderWorkflow, orderId, cancellationToken: cancellationToken);

        // Durable workflow runs also implement IAwaitableWorkflowRun, so the same handle can be
        // awaited for the final result.
        if (run is not IAwaitableWorkflowRun awaitableRun)
        {
            return new AcceptedResult(location: null, value: run.RunId);
        }

        string? result = await awaitableRun.WaitForCompletionAsync<string>(cancellationToken);
        return new OkObjectResult(result);
    }
}
