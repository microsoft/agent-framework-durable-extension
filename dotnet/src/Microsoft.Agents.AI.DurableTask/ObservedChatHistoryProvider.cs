// Copyright (c) Microsoft. All rights reserved.

using System.Runtime.InteropServices;
using System.Text.Json;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Observes unrecovered callbacks without changing the configured provider's filtering or state contract.
/// </summary>
internal sealed class ObservedChatHistoryProvider(ChatHistoryProvider inner) : ChatHistoryProvider
{
    public const string FailureCode = "historyProviderFailure";
    public const string FailureMessage =
        "The chat history provider failed. External effects may have occurred; the last committed continuation was preserved.";

    private Exception? _failure;

    public override IReadOnlyList<string> StateKeys => inner.StateKeys;

    public bool FailedWith(Exception exception) => ReferenceEquals(this._failure, exception);

    public override object? GetService(Type serviceType, object? serviceKey = null) =>
        inner.GetService(serviceType, serviceKey);

    protected override async ValueTask<IEnumerable<ChatMessage>> InvokingCoreAsync(
        InvokingContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            // Enumerate inside the provider boundary: retrieval and output filters can be lazy.
            return (await inner.InvokingAsync(context, cancellationToken).ConfigureAwait(false)).ToArray();
        }
        catch (Exception exception) when (CanFinalize(exception, cancellationToken))
        {
            this._failure = exception;
            throw;
        }
    }

    protected override async ValueTask InvokedCoreAsync(
        InvokedContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await inner.InvokedAsync(context, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (
            context.InvokeException is null &&
            CanFinalize(exception, cancellationToken))
        {
            this._failure = exception;
            throw;
        }
    }

    private static bool CanFinalize(Exception exception, CancellationToken cancellationToken)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return false;
        }

        Stack<Exception> pending = new();
        HashSet<Exception> visited = new(ReferenceEqualityComparer.Instance);
        pending.Push(exception);
        while (pending.TryPop(out Exception? current))
        {
            if (!visited.Add(current))
            {
                continue;
            }

            if (current is OperationCanceledException or
                ArgumentException or
                JsonException or
                NotSupportedException or
                OutOfMemoryException or
                StackOverflowException or
                AccessViolationException or
                AppDomainUnloadedException or
                BadImageFormatException or
                CannotUnloadAppDomainException or
                InvalidProgramException or
                SEHException or
                DurableAgentStateCorruptionException or
                DurableAgentHistoryBindingMismatchException or
                DurableAgentHistoryOwnershipNotSupportedException)
            {
                return false;
            }

            // Any excluded cause vetoes finality; AggregateException.InnerException exposes only its first child.
            if (current is AggregateException aggregate)
            {
                foreach (Exception innerException in aggregate.InnerExceptions)
                {
                    pending.Push(innerException);
                }
            }
            else if (current.InnerException is Exception innerException)
            {
                pending.Push(innerException);
            }
        }

        return true;
    }
}
