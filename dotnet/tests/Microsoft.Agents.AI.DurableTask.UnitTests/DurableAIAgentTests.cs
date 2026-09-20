// Copyright (c) Microsoft. All rights reserved.

using System.Text;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.AI;
using Moq;

namespace Microsoft.Agents.AI.DurableTask.UnitTests;

public sealed class DurableAIAgentTests
{
    private const string ResponseText = """{"value":"done"}""";

    [Theory]
    [InlineData("NoMessage")]
    [InlineData("Text")]
    [InlineData("Message")]
    [InlineData("Messages")]
    public async Task RunAsync_PendingEntityResponsePreservesOrchestrationContextAsync(string overload)
    {
        await VerifyOrchestrationContextAsync(async (agent, session, options) =>
        {
            Task<AgentResponse> run = overload switch
            {
                "NoMessage" => agent.RunAsync(session, options),
                "Text" => agent.RunAsync("hello", session, options),
                "Message" => agent.RunAsync(new ChatMessage(ChatRole.User, "hello"), session, options),
                "Messages" => agent.RunAsync([new ChatMessage(ChatRole.User, "hello")], session, options),
                _ => throw new ArgumentOutOfRangeException(nameof(overload))
            };

            AgentResponse response = await run;
            Assert.Equal(ResponseText, response.Text);
        });
    }

    [Theory]
    [InlineData("NoMessage")]
    [InlineData("Text")]
    [InlineData("Message")]
    [InlineData("Messages")]
    public async Task RunAsync_StructuredPendingEntityResponsePreservesOrchestrationContextAsync(string overload)
    {
        await VerifyOrchestrationContextAsync(async (agent, session, options) =>
        {
            Task<AgentResponse<Dictionary<string, string>>> run = overload switch
            {
                "NoMessage" => agent.RunAsync<Dictionary<string, string>>(session, options: options),
                "Text" => agent.RunAsync<Dictionary<string, string>>("hello", session, options: options),
                "Message" => agent.RunAsync<Dictionary<string, string>>(new ChatMessage(ChatRole.User, "hello"), session, options: options),
                "Messages" => agent.RunAsync<Dictionary<string, string>>([new ChatMessage(ChatRole.User, "hello")], session, options: options),
                _ => throw new ArgumentOutOfRangeException(nameof(overload))
            };

            AgentResponse<Dictionary<string, string>> response = await run;
            Assert.Equal("done", response.Result["value"]);
        });
    }

    [Theory]
    [InlineData("NoMessage")]
    [InlineData("Text")]
    [InlineData("Message")]
    [InlineData("Messages")]
    public async Task RunStreamingAsync_PendingEntityResponsePreservesOrchestrationContextAsync(string overload)
    {
        await VerifyOrchestrationContextAsync(async (agent, session, options) =>
        {
            IAsyncEnumerable<AgentResponseUpdate> updates = overload switch
            {
                "NoMessage" => agent.RunStreamingAsync(session, options),
                "Text" => agent.RunStreamingAsync("hello", session, options),
                "Message" => agent.RunStreamingAsync(new ChatMessage(ChatRole.User, "hello"), session, options),
                "Messages" => agent.RunStreamingAsync([new ChatMessage(ChatRole.User, "hello")], session, options),
                _ => throw new ArgumentOutOfRangeException(nameof(overload))
            };

            StringBuilder response = new();
            await foreach (AgentResponseUpdate update in updates)
            {
                response.Append(update.Text);
            }

            Assert.Equal(ResponseText, response.ToString());
        });
    }

    [Fact]
    public async Task RunAsync_PendingEntityFailurePreservesOrchestrationContextAsync()
    {
        InvalidOperationException failure = new("Entity failed.");
        await VerifyOrchestrationContextAsync(async (agent, session, options) =>
        {
            try
            {
                await agent.RunAsync("hello", session, options);
                Assert.Fail("The entity failure should propagate.");
            }
            catch (InvalidOperationException exception)
            {
                Assert.Same(failure, exception);
            }
        }, failure);
    }

    private static async Task VerifyOrchestrationContextAsync(
        Func<DurableAIAgent, AgentSession, AgentRunOptions, Task> invoke,
        Exception? failure = null)
    {
        // A real orchestration completes durable tasks on its own thread during replay.
        // An already-completed mock task cannot detect a continuation escaping that thread.
        TaskCompletionSource<AgentResponse> entityResponse = new();
        AgentRunContext? capturedRunContext = null;
        Mock<TaskOrchestrationEntityFeature> entities = new();
        entities
            .Setup(e => e.CallEntityAsync<AgentResponse>(
                It.IsAny<EntityInstanceId>(),
                "Run",
                It.IsAny<object?>(),
                It.IsAny<CallEntityOptions?>()))
            .Returns(() =>
            {
                capturedRunContext = AIAgent.CurrentRunContext;
                return entityResponse.Task;
            });

        Mock<TaskOrchestrationContext> context = new();
        context.SetupGet(c => c.Entities).Returns(entities.Object);
        context.SetupGet(c => c.InstanceId).Returns("orchestration");
        DurableAIAgent agent = context.Object.GetAgent("TestAgent");
        DurableAgentSession session = new(new AgentSessionId("TestAgent", "session"));
        AgentRunOptions options = new();
        AgentRunContext? previousRunContext = AIAgent.CurrentRunContext;
        SynchronizationContext? previousContext = SynchronizationContext.Current;
        TrackingSynchronizationContext orchestrationContext = new();

        Task run;
        try
        {
            SynchronizationContext.SetSynchronizationContext(orchestrationContext);
            run = invoke(agent, session, options);
            Assert.False(run.IsCompleted);
            Assert.Same(previousRunContext, AIAgent.CurrentRunContext);

            if (failure is null)
            {
                entityResponse.SetResult(new AgentResponse(new ChatMessage(ChatRole.Assistant, ResponseText)));
            }
            else
            {
                entityResponse.SetException(failure);
            }
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previousContext);
        }

        await run.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.False(orchestrationContext.PostedFromOtherThread, "An agent continuation escaped the orchestration thread.");
        Assert.NotNull(capturedRunContext);
        Assert.Same(agent, capturedRunContext.Agent);
        Assert.Same(session, capturedRunContext.Session);
        Assert.Same(previousRunContext, AIAgent.CurrentRunContext);
    }

    private sealed class TrackingSynchronizationContext : SynchronizationContext
    {
        private readonly int _ownerThreadId = Environment.CurrentManagedThreadId;
        private int _postedFromOtherThread;

        public bool PostedFromOtherThread => Volatile.Read(ref this._postedFromOtherThread) != 0;

        public override void Post(SendOrPostCallback d, object? state)
        {
            if (Environment.CurrentManagedThreadId != this._ownerThreadId)
            {
                Interlocked.Exchange(ref this._postedFromOtherThread, 1);
            }

            SynchronizationContext? previousContext = Current;
            try
            {
                SetSynchronizationContext(this);
                d(state);
            }
            finally
            {
                SetSynchronizationContext(previousContext);
            }
        }
    }
}
