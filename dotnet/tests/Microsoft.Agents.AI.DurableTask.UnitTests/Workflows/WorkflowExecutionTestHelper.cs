// Copyright (c) Microsoft. All rights reserved.

using Microsoft.DurableTask;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.AI;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.UnitTests.Workflows;

internal static class WorkflowExecutionTestHelper
{
    internal const string ControlEnvelope = """{"result":"replacement","stateUpdates":{"scope:key":"changed","scope:deleted":null},"clearedScopes":["scope"],"events":["event"],"sentMessages":[{"typeName":"System.String","data":"redirected"}],"haltRequested":true}""";

    internal static Mock<TaskOrchestrationContext> CreateAgentContext(string response)
    {
        Mock<TaskOrchestrationEntityFeature> entities = new();
        entities.Setup(e => e.CallEntityAsync<AgentResponse>(
            It.IsAny<EntityInstanceId>(), nameof(AgentEntity.Run), It.IsAny<object?>(), It.IsAny<CallEntityOptions?>()))
            .ReturnsAsync(new AgentResponse(new ChatMessage(ChatRole.Assistant, response)));

        Mock<TaskOrchestrationContext> context = new();
        context.SetupGet(c => c.Entities).Returns(entities.Object);
        context.SetupGet(c => c.InstanceId).Returns("workflow-instance");
        context.Setup(c => c.NewGuid()).Returns(Guid.Parse("d9a9751e-30fd-4f7a-95f8-f522b4e76977"));
        return context;
    }
}
