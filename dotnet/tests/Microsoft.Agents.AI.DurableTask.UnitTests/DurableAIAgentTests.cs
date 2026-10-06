// Copyright (c) Microsoft. All rights reserved.

using System.Collections.Concurrent;
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
    [InlineData(false)]
    [InlineData(true)]
    public async Task RunAsync_OneShotMessagesAreEnumeratedOnceAsync(bool streaming)
    {
        ChatMessage[] expected = [new(ChatRole.User, "first"), new(ChatRole.User, "second")];
        Queue<ChatMessage> pending = new(expected);
        int enumerations = 0;
        IEnumerable<ChatMessage> Messages()
        {
            enumerations++;
            while (pending.TryDequeue(out ChatMessage? message))
            {
                yield return message;
            }
        }

        Mock<TaskOrchestrationEntityFeature> entities = new();
        entities
            .Setup(e => e.CallEntityAsync<AgentResponse>(
                It.IsAny<EntityInstanceId>(),
                "Run",
                It.IsAny<object?>(),
                It.IsAny<CallEntityOptions?>()))
            .Returns((EntityInstanceId _, string _, object? input, CallEntityOptions? _) =>
            {
                RunRequest request = Assert.IsType<RunRequest>(input);
                Assert.Equal(expected, request.Messages);
                Assert.Equal(expected, AIAgent.CurrentRunContext!.RequestMessages);
                return Task.FromResult(new AgentResponse(new ChatMessage(ChatRole.Assistant, ResponseText)));
            });
        Mock<TaskOrchestrationContext> context = new();
        context.SetupGet(c => c.Entities).Returns(entities.Object);
        context.SetupGet(c => c.InstanceId).Returns("orchestration");
        DurableAIAgent agent = context.Object.GetAgent("TestAgent");
        DurableAgentSession session = new(new AgentSessionId("TestAgent", "session"));

        if (streaming)
        {
            StringBuilder text = new();
            await foreach (AgentResponseUpdate update in agent.RunStreamingAsync(Messages(), session))
            {
                text.Append(update.Text);
            }

            Assert.Equal(ResponseText, text.ToString());
        }
        else
        {
            Assert.Equal(ResponseText, (await agent.RunAsync(Messages(), session)).Text);
        }

        Assert.Equal(1, enumerations);
        entities.VerifyAll();
    }

    [Theory]
    [InlineData("NoMessage")]
    [InlineData("Text")]
    [InlineData("Message")]
    [InlineData("Messages")]
    public async Task RunAsync_PendingEntityResponsePreservesOrchestrationContextAsync(string overload)
    {
        await VerifyOrchestrationContextAsync(async (agent, session, options, observe) =>
        {
            Task<AgentResponse> run = overload switch
            {
                "NoMessage" => agent.RunAsync(session, options),
                "Text" => agent.RunAsync("hello", session, options),
                "Message" => agent.RunAsync(new ChatMessage(ChatRole.User, "hello"), session, options),
                "Messages" => agent.RunAsync([new ChatMessage(ChatRole.User, "hello")], session, options),
                _ => throw new ArgumentOutOfRangeException(nameof(overload))
            };

            observe(run);
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
        await VerifyOrchestrationContextAsync(async (agent, session, options, observe) =>
        {
            Task<AgentResponse<Dictionary<string, string>>> run = overload switch
            {
                "NoMessage" => agent.RunAsync<Dictionary<string, string>>(session, options: options),
                "Text" => agent.RunAsync<Dictionary<string, string>>("hello", session, options: options),
                "Message" => agent.RunAsync<Dictionary<string, string>>(new ChatMessage(ChatRole.User, "hello"), session, options: options),
                "Messages" => agent.RunAsync<Dictionary<string, string>>([new ChatMessage(ChatRole.User, "hello")], session, options: options),
                _ => throw new ArgumentOutOfRangeException(nameof(overload))
            };

            observe(run);
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
        await VerifyOrchestrationContextAsync(async (agent, session, options, observe) =>
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
            await using IAsyncEnumerator<AgentResponseUpdate> enumerator = updates.GetAsyncEnumerator();
            Task<bool> moveNext = enumerator.MoveNextAsync().AsTask();
            observe(moveNext);
            while (await moveNext)
            {
                response.Append(enumerator.Current.Text);
                moveNext = enumerator.MoveNextAsync().AsTask();
            }

            Assert.Equal(ResponseText, response.ToString());
        });
    }

    [Fact]
    public async Task RunAsync_PendingEntityFailurePreservesOrchestrationContextAsync()
    {
        InvalidOperationException failure = new("Entity failed.");
        await VerifyOrchestrationContextAsync(async (agent, session, options, observe) =>
        {
            try
            {
                Task<AgentResponse> run = agent.RunAsync("hello", session, options);
                observe(run);
                await run;
                Assert.Fail("The entity failure should propagate.");
            }
            catch (InvalidOperationException exception)
            {
                Assert.Same(failure, exception);
            }
        }, failure);
    }

    private static Task VerifyOrchestrationContextAsync(
        Func<DurableAIAgent, AgentSession, AgentRunOptions, Action<Task>, Task> invoke,
        Exception? failure = null)
    {
        return Task.Run(() => VerifyOrchestrationContextCoreAsync(invoke, failure));
    }

    private static async Task VerifyOrchestrationContextCoreAsync(
        Func<DurableAIAgent, AgentSession, AgentRunOptions, Action<Task>, Task> invoke,
        Exception? failure)
    {
        TaskCompletionSource<AgentResponse> entityResponse = new(TaskCreationOptions.RunContinuationsAsynchronously);
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
        using TrackingSynchronizationContext orchestrationContext = new();
        int ownerThreadId = Environment.CurrentManagedThreadId;
        int completionThreadId = 0;
        SynchronizationContext? completionContext = null;
        Task? observation = null;
        void Observe(Task task)
        {
            // Observe completion before the caller's await can marshal back and hide an escaping continuation.
            observation = task.ContinueWith(
                _ =>
                {
                    completionThreadId = Environment.CurrentManagedThreadId;
                    completionContext = SynchronizationContext.Current;
                },
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        Task run;
        try
        {
            SynchronizationContext.SetSynchronizationContext(orchestrationContext);
            run = invoke(agent, session, options, Observe);
            Assert.False(run.IsCompleted);
            Assert.Same(previousRunContext, AIAgent.CurrentRunContext);

            Thread entityCompletion = new(() =>
            {
                if (failure is null)
                {
                    entityResponse.SetResult(new AgentResponse(new ChatMessage(ChatRole.Assistant, ResponseText)));
                }
                else
                {
                    entityResponse.SetException(failure);
                }
            });
            entityCompletion.Start();
            Assert.True(entityCompletion.Join(TimeSpan.FromSeconds(10)));
            Assert.NotEqual(ownerThreadId, entityCompletion.ManagedThreadId);
            orchestrationContext.PumpUntil(run);
            await run;
            Assert.NotNull(observation);
            orchestrationContext.PumpUntil(observation);
            await observation;
            Assert.Equal(ownerThreadId, completionThreadId);
            Assert.Same(orchestrationContext, completionContext);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previousContext);
        }

        Assert.NotNull(capturedRunContext);
        Assert.Same(agent, capturedRunContext.Agent);
        Assert.Same(session, capturedRunContext.Session);
        Assert.Same(previousRunContext, AIAgent.CurrentRunContext);
    }

    private sealed class TrackingSynchronizationContext : SynchronizationContext, IDisposable
    {
        private readonly BlockingCollection<(SendOrPostCallback Callback, object? State)> _work = new();

        public override void Post(SendOrPostCallback d, object? state) => this._work.Add((d, state));

        public void PumpUntil(Task task)
        {
            DateTime deadline = DateTime.UtcNow.AddSeconds(10);
            while (!task.IsCompleted)
            {
                Assert.True(DateTime.UtcNow < deadline, "The orchestration continuation did not complete.");
                if (this._work.TryTake(out var work, TimeSpan.FromMilliseconds(10)))
                {
                    work.Callback(work.State);
                }
            }
        }

        public void Dispose() => this._work.Dispose();
    }
}
