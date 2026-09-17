// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask.UnitTests;

/// <summary>
/// Unit tests for the <see cref="MaxSuperstepsExceededException"/> class.
/// </summary>
public sealed class MaxSuperstepsExceededExceptionTests
{
    [Fact]
    public void MessageConstructorLeavesStructuredPropertiesUnset()
    {
        MaxSuperstepsExceededException exception = new("message");

        Assert.Null(exception.InstanceId);
        Assert.Null(exception.MaxSupersteps);
        Assert.Null(exception.RemainingExecutors);
    }

    [Fact]
    public void MessageAndInnerExceptionConstructorLeavesStructuredPropertiesUnset()
    {
        InvalidOperationException innerException = new();

        MaxSuperstepsExceededException exception = new("message", innerException);

        Assert.Same(innerException, exception.InnerException);
        Assert.Null(exception.InstanceId);
        Assert.Null(exception.MaxSupersteps);
        Assert.Null(exception.RemainingExecutors);
    }
}
