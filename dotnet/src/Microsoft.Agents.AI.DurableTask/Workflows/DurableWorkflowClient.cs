// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Agents.AI.Workflows;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;

namespace Microsoft.Agents.AI.DurableTask.Workflows;

/// <summary>
/// Provides a durable task-based implementation of <see cref="IWorkflowClient"/> for running
/// workflows as durable orchestrations.
/// </summary>
internal sealed class DurableWorkflowClient : IWorkflowClient
{
    private readonly DurableTaskClient _client;
    private readonly DurableOptions _options;

    /// <summary>
    /// Initializes a new instance of the <see cref="DurableWorkflowClient"/> class.
    /// </summary>
    /// <param name="client">The durable task client for orchestration operations.</param>
    /// <param name="options">The durable options containing the registered workflows.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="client"/> or <paramref name="options"/> is null.</exception>
    public DurableWorkflowClient(DurableTaskClient client, DurableOptions options)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(options);
        this._client = client;
        this._options = options;
    }

    /// <inheritdoc/>
    public async ValueTask<IWorkflowRun> RunAsync<TInput>(
        Workflow workflow,
        TInput input,
        string? runId = null,
        CancellationToken cancellationToken = default)
        where TInput : notnull
    {
        ArgumentNullException.ThrowIfNull(workflow);

        if (string.IsNullOrEmpty(workflow.Name))
        {
            throw new ArgumentException("Workflow must have a valid Name property.", nameof(workflow));
        }

        DurableWorkflowInput<TInput> workflowInput = new() { Input = input };

        string instanceId = await this._client.ScheduleNewOrchestrationInstanceAsync(
            orchestratorName: WorkflowNamingHelper.ToOrchestrationFunctionName(workflow.Name),
            input: workflowInput,
            options: runId is not null ? new StartOrchestrationOptions(runId) : null,
            cancellation: cancellationToken).ConfigureAwait(false);

        return new DurableWorkflowRun(this._client, instanceId, workflow.Name);
    }

    /// <inheritdoc/>
    public ValueTask<IWorkflowRun> RunAsync(
        Workflow workflow,
        string input,
        string? runId = null,
        CancellationToken cancellationToken = default)
        => this.RunAsync<string>(workflow, input, runId, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IWorkflowRun> RunAsync<TInput>(
        string workflowName,
        TInput input,
        string? runId = null,
        CancellationToken cancellationToken = default)
        where TInput : notnull
        => this.RunAsync(this.ResolveWorkflow(workflowName), input, runId, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IWorkflowRun> RunAsync(
        string workflowName,
        string input,
        string? runId = null,
        CancellationToken cancellationToken = default)
        => this.RunAsync<string>(workflowName, input, runId, cancellationToken);

    /// <inheritdoc/>
    public async ValueTask<IStreamingWorkflowRun> StreamAsync<TInput>(
        Workflow workflow,
        TInput input,
        string? runId = null,
        CancellationToken cancellationToken = default)
        where TInput : notnull
    {
        ArgumentNullException.ThrowIfNull(workflow);

        if (string.IsNullOrEmpty(workflow.Name))
        {
            throw new ArgumentException("Workflow must have a valid Name property.", nameof(workflow));
        }

        DurableWorkflowInput<TInput> workflowInput = new() { Input = input };

        string instanceId = await this._client.ScheduleNewOrchestrationInstanceAsync(
            orchestratorName: WorkflowNamingHelper.ToOrchestrationFunctionName(workflow.Name),
            input: workflowInput,
            options: runId is not null ? new StartOrchestrationOptions(runId) : null,
            cancellation: cancellationToken).ConfigureAwait(false);

        return new DurableStreamingWorkflowRun(this._client, instanceId, workflow);
    }

    /// <inheritdoc/>
    public ValueTask<IStreamingWorkflowRun> StreamAsync(
        Workflow workflow,
        string input,
        string? runId = null,
        CancellationToken cancellationToken = default)
        => this.StreamAsync<string>(workflow, input, runId, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IStreamingWorkflowRun> StreamAsync<TInput>(
        string workflowName,
        TInput input,
        string? runId = null,
        CancellationToken cancellationToken = default)
        where TInput : notnull
        => this.StreamAsync(this.ResolveWorkflow(workflowName), input, runId, cancellationToken);

    /// <inheritdoc/>
    public ValueTask<IStreamingWorkflowRun> StreamAsync(
        string workflowName,
        string input,
        string? runId = null,
        CancellationToken cancellationToken = default)
        => this.StreamAsync<string>(workflowName, input, runId, cancellationToken);

    /// <summary>
    /// Resolves a registered workflow by name.
    /// </summary>
    /// <param name="workflowName">The name of the workflow to resolve.</param>
    /// <returns>The registered <see cref="Workflow"/>.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="workflowName"/> is null or empty.</exception>
    /// <exception cref="WorkflowNotRegisteredException">Thrown when no workflow with the specified name has been registered.</exception>
    private Workflow ResolveWorkflow(string workflowName)
    {
        ArgumentException.ThrowIfNullOrEmpty(workflowName);

        if (!this._options.Workflows.Workflows.TryGetValue(workflowName, out Workflow? workflow))
        {
            throw new WorkflowNotRegisteredException(workflowName);
        }

        return workflow;
    }
}
