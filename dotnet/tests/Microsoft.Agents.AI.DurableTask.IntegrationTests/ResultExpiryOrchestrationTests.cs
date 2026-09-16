// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Entities;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.IntegrationTests;

/// <summary>Executes the backend test's actual orchestration handler without connecting to a backend.</summary>
public sealed class ResultExpiryOrchestrationTests
{
    private static readonly EntityInstanceId s_id = new("dafx-ExpiryProbe", "local-proof");
    private static readonly JsonElement s_input = JsonSerializer.SerializeToElement(new AgentEntityResultExpirationCheck(
        new DateTimeOffset(2026, 9, 12, 0, 0, 0, TimeSpan.Zero), "5b9ddf23b2d94e42b1dd4d946134f043", s_id.ToString()));

    [Fact]
    public async Task SuccessfulEntityCallReturnsTrueAsync()
    {
        Mock<TaskOrchestrationEntityFeature> entities = CreateEntities(null);
        Assert.True(await InvokeAsync(entities));
        VerifySingleCall(entities);
    }

    [Fact]
    public async Task InjectedCleanupSdkFailureReturnsFalseAsync()
    {
        EntityOperationFailedException failure = new(s_id, nameof(AgentEntity.CheckAndExpireResults), InjectedDetails());
        Mock<TaskOrchestrationEntityFeature> entities = CreateEntities(failure);
        Assert.False(await InvokeAsync(entities));
        VerifySingleCall(entities);
    }

    [Theory]
    [InlineData("entity")]
    [InlineData("operation")]
    [InlineData("command")]
    [InlineData("type")]
    [InlineData("message")]
    [InlineData("inner")]
    public async Task UnexpectedEntityFailuresPropagateAsync(string mismatch)
    {
        TaskFailureDetails expected = InjectedDetails();
        TaskFailureDetails details = new(
            mismatch == "type" ? typeof(InvalidOperationException).FullName! : expected.ErrorType,
            mismatch == "message" ? "unexpected failure" : expected.ErrorMessage,
            expected.StackTrace,
            mismatch == "inner" ? TaskFailureDetails.FromException(new InvalidOperationException("unexpected cause")) : null,
            expected.Properties);
        string operation = mismatch == "command" ? nameof(AgentEntity.Run) : nameof(AgentEntity.CheckAndExpireResults);
        EntityOperationFailedException failure = new(
            mismatch == "entity" ? new EntityInstanceId(s_id.Name, "other") : s_id,
            mismatch == "operation" ? nameof(AgentEntity.Run) : operation, details);
        Mock<TaskOrchestrationEntityFeature> entities = CreateEntities(failure, operation);
        Assert.Same(failure, await Assert.ThrowsAsync<EntityOperationFailedException>(() => InvokeAsync(entities, operation)));
        VerifySingleCall(entities, operation);
    }

    [Theory]
    [InlineData("task")]
    [InlineData("local")]
    [InlineData("cancellation")]
    public async Task NonEntityFailuresPropagateAsync(string kind)
    {
        Exception failure = kind switch
        {
            "task" => new TaskFailedException("unexpected-task", 0, InjectedDetails()),
            "cancellation" => new OperationCanceledException(),
            _ => new InvalidOperationException(ResultExpiryAtomicityTests.InjectedCleanupFailureException.FailureMessage),
        };
        Mock<TaskOrchestrationEntityFeature> entities = CreateEntities(failure);
        Assert.Same(failure, await Assert.ThrowsAsync(failure.GetType(), () => InvokeAsync(entities)));
        VerifySingleCall(entities);
    }

    private static TaskFailureDetails InjectedDetails() =>
        JsonSerializer.Deserialize<TaskFailureDetails>(JsonSerializer.Serialize(TaskFailureDetails.FromException(
            new ResultExpiryAtomicityTests.InjectedCleanupFailureException())))!;

    private static Mock<TaskOrchestrationEntityFeature> CreateEntities(
        Exception? failure, string operation = nameof(AgentEntity.CheckAndExpireResults))
    {
        Mock<TaskOrchestrationEntityFeature> entities = new(MockBehavior.Strict);
        entities.Setup(value => value.CallEntityAsync(
                s_id, operation, s_input, It.IsAny<CallEntityOptions?>()))
            .Returns(() => failure is null ? Task.CompletedTask : Task.FromException(failure));
        return entities;
    }

    private static Task<bool> InvokeAsync(
        Mock<TaskOrchestrationEntityFeature> entities, string operation = nameof(AgentEntity.CheckAndExpireResults)) =>
        ResultExpiryAtomicityTests.InvokeEntityAsync(entities.Object, s_id, operation, s_input);

    private static void VerifySingleCall(
        Mock<TaskOrchestrationEntityFeature> entities, string operation = nameof(AgentEntity.CheckAndExpireResults)) =>
        entities.Verify(value => value.CallEntityAsync(
            s_id, operation, s_input, It.IsAny<CallEntityOptions?>()), Times.Once);
}
