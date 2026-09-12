// Copyright (c) Microsoft. All rights reserved.

using System.Collections.Specialized;
using System.Net;
using System.Text;
using System.Text.Json;
using Azure.Core.Serialization;
using Microsoft.Agents.AI.DurableTask;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Extensions.Mcp;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask.Client;
using Microsoft.DurableTask.Client.Entities;
using Microsoft.DurableTask.Entities;
using Microsoft.Extensions.DependencyInjection;
using Moq;

namespace Microsoft.Agents.AI.Hosting.AzureFunctions.UnitTests;

public sealed class BuiltInFunctionsAgentOutcomeTests
{
    private const string AgentName = "TestAgent";
    private const string SessionKey = "session-1";

    [Fact]
    public async Task Http_FireAndForget_ReturnsAcceptedWithoutPollingAsync()
    {
        using EndpointFixture fixture = new(waitForResponse: false);

        HttpResponseData response = await BuiltInFunctions.RunAgentHttpAsync(
            fixture.Request, fixture.Client.Object, fixture.Context);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        using JsonDocument body = ReadBody(response);
        Assert.Equal(202, body.RootElement.GetProperty("status").GetInt32());
        Assert.False(body.RootElement.TryGetProperty("response", out _));
        fixture.Entities.Verify(
            c => c.GetEntityAsync<DurableAgentState>(
                It.IsAny<EntityInstanceId>(), It.IsAny<bool>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Theory]
    [InlineData("application/json")]
    [InlineData("text/plain")]
    public async Task Http_LegacySuccess_ReturnsNegotiatedResponseAsync(string accept)
    {
        using EndpointFixture fixture = new(accept: accept);

        HttpResponseData response = await BuiltInFunctions.RunAgentHttpAsync(
            fixture.Request, fixture.Client.Object, fixture.Context);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(SessionKey, Assert.Single(response.Headers.GetValues("x-ms-session-id")));
        if (accept == "application/json")
        {
            using JsonDocument body = ReadBody(response);
            Assert.Equal(200, body.RootElement.GetProperty("status").GetInt32());
            Assert.Equal("original result", body.RootElement.GetProperty("response")
                .GetProperty("messages")[0].GetProperty("contents")[0].GetProperty("text").GetString());
        }
        else
        {
            response.Body.Position = 0;
            using StreamReader reader = new(response.Body, leaveOpen: true);
            Assert.Equal("original result", await reader.ReadToEndAsync());
        }
    }

    [Theory]
    [InlineData(null)]
    [InlineData("text")]
    [InlineData("json")]
    public async Task Mcp_Success_PreservesLegacyTextOrExplicitJsonAsync(string? format)
    {
        using EndpointFixture fixture = new();

        string? response = await BuiltInFunctions.RunMcpToolAsync(
            CreateToolContext(format), fixture.Client.Object, fixture.Context);

        if (format == "json")
        {
            using JsonDocument body = JsonDocument.Parse(Assert.IsType<string>(response));
            Assert.Equal(SessionKey, body.RootElement.GetProperty("session_id").GetString());
            Assert.Equal("original result", body.RootElement.GetProperty("response")
                .GetProperty("messages")[0].GetProperty("contents")[0].GetProperty("text").GetString());
        }
        else
        {
            Assert.Equal("original result", response);
        }
    }

    [Theory]
    [InlineData("yaml")]
    [InlineData("")]
    [InlineData(3)]
    public async Task Mcp_InvalidFormat_DoesNotDispatchAsync(object format)
    {
        using EndpointFixture fixture = new();
        ToolInvocationContext invocation = CreateToolContext();
        invocation.Arguments![BuiltInFunctionsTestResponseFormat] = format;

        await Assert.ThrowsAsync<ArgumentException>(() => BuiltInFunctions.RunMcpToolAsync(
            invocation, fixture.Client.Object, fixture.Context));

        fixture.Entities.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task Mcp_Cancellation_ReachesDurableClientAsync()
    {
        using CancellationTokenSource cancellation = new();
        using EndpointFixture fixture = new(cancellationToken: cancellation.Token);
        fixture.Entities
            .Setup(c => c.SignalEntityAsync(
                It.IsAny<EntityInstanceId>(), It.IsAny<string>(), It.IsAny<object>(),
                It.IsAny<SignalEntityOptions>(), cancellation.Token))
            .ThrowsAsync(new OperationCanceledException(cancellation.Token));

        await Assert.ThrowsAsync<OperationCanceledException>(() => BuiltInFunctions.RunMcpToolAsync(
            CreateToolContext(), fixture.Client.Object, fixture.Context));
    }

    [Theory]
    [InlineData("succeeded", "application/json")]
    [InlineData("failed", "application/json")]
    [InlineData("succeeded", "text/plain")]
    [InlineData("failed", "text/plain")]
    public async Task Http_Unavailable_RetainsCompletionOutcomeAsync(string outcome, string accept)
    {
        using EndpointFixture fixture = new(
            accept: accept, stateFactory: correlation => CreateMailboxState(correlation, outcome, available: false));

        HttpResponseData response = await BuiltInFunctions.RunAgentHttpAsync(
            fixture.Request, fixture.Client.Object, fixture.Context);

        Assert.Equal(HttpStatusCode.Gone, response.StatusCode);
        Assert.Equal(outcome, Assert.Single(response.Headers.GetValues("x-ms-agent-completion-outcome")));
        if (accept == "application/json")
        {
            using JsonDocument body = ReadBody(response);
            Assert.Equal("completedResultUnavailable", body.RootElement.GetProperty("outcome").GetString());
            Assert.Equal(outcome, body.RootElement.GetProperty("completion_outcome").GetString());
            Assert.False(body.RootElement.TryGetProperty("response", out _));
        }
    }

    [Fact]
    public async Task Http_CommittedFailure_PreservesErrorDetailsAsync()
    {
        using EndpointFixture fixture = new(
            stateFactory: correlation => CreateMailboxState(correlation, "failed", available: true));

        HttpResponseData response = await BuiltInFunctions.RunAgentHttpAsync(
            fixture.Request, fixture.Client.Object, fixture.Context);

        Assert.Equal(HttpStatusCode.InternalServerError, response.StatusCode);
        using JsonDocument body = ReadBody(response);
        Assert.Equal("failed", body.RootElement.GetProperty("outcome").GetString());
        JsonElement error = body.RootElement.GetProperty("error");
        Assert.Equal("committedFailure", error.GetProperty("code").GetString());
        Assert.True(error.GetProperty("details").GetProperty("retained").GetBoolean());
        Assert.False(body.RootElement.TryGetProperty("response", out _));
    }

    [Theory]
    [InlineData("text", "succeeded")]
    [InlineData("json", "succeeded")]
    [InlineData("text", "failed")]
    [InlineData("json", "failed")]
    public async Task Mcp_Unavailable_ThrowsInsteadOfReturningSuccessfulTextAsync(string format, string outcome)
    {
        using EndpointFixture fixture = new(
            stateFactory: correlation => CreateMailboxState(correlation, outcome, available: false));

        DurableAgentResultUnavailableException exception =
            await Assert.ThrowsAsync<DurableAgentResultUnavailableException>(() => BuiltInFunctions.RunMcpToolAsync(
                CreateToolContext(format), fixture.Client.Object, fixture.Context));

        Assert.Equal(outcome, exception.Outcome);
    }

    [Theory]
    [InlineData("text")]
    [InlineData("json")]
    public async Task Mcp_CommittedFailure_ThrowsWithOriginalDetailsAsync(string format)
    {
        using EndpointFixture fixture = new(
            stateFactory: correlation => CreateMailboxState(correlation, "failed", available: true));

        DurableAgentTerminalException exception =
            await Assert.ThrowsAsync<DurableAgentTerminalException>(() => BuiltInFunctions.RunMcpToolAsync(
                CreateToolContext(format), fixture.Client.Object, fixture.Context));

        Assert.Equal("committedFailure", exception.Code);
        Assert.True(exception.Details!.Value.GetProperty("retained").GetBoolean());
    }

    [Fact]
    public async Task Http_TransientReadFailure_IsNotConvertedToTerminalSuccessAsync()
    {
        using EndpointFixture fixture = new();
        fixture.Entities
            .Setup(c => c.GetEntityAsync<DurableAgentState>(
                It.IsAny<EntityInstanceId>(), true, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Storage temporarily unavailable."));

        await Assert.ThrowsAsync<InvalidOperationException>(() => BuiltInFunctions.RunAgentHttpAsync(
            fixture.Request, fixture.Client.Object, fixture.Context));
    }

    [Fact]
    public async Task Mcp_PendingUntilCancellation_DoesNotReturnEmptySuccessAsync()
    {
        using CancellationTokenSource cancellation = new();
        using EndpointFixture fixture = new(cancellationToken: cancellation.Token);
        fixture.Entities
            .Setup(c => c.GetEntityAsync<DurableAgentState>(
                It.IsAny<EntityInstanceId>(), true, It.IsAny<CancellationToken>()))
            .Callback(cancellation.Cancel)
            .ReturnsAsync((EntityMetadata<DurableAgentState>?)null);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => BuiltInFunctions.RunMcpToolAsync(
            CreateToolContext("json"), fixture.Client.Object, fixture.Context));
    }

    [Theory]
    [InlineData(false, null)]
    [InlineData(false, "null")]
    [InlineData(false, "false")]
    [InlineData(false, "0")]
    [InlineData(false, "\"\"")]
    [InlineData(false, "{\"answer\":42}")]
    [InlineData(true, null)]
    [InlineData(true, "null")]
    [InlineData(true, "false")]
    [InlineData(true, "0")]
    [InlineData(true, "\"\"")]
    [InlineData(true, "{\"answer\":42}")]
    public async Task JsonSuccess_PreservesCanonicalMetadataAndValueAsync(bool mcp, string? value)
    {
        using EndpointFixture fixture = new(
            stateFactory: correlation => CreateSuccessfulMailboxState(correlation, value));
        using JsonDocument body = mcp
            ? JsonDocument.Parse(Assert.IsType<string>(await BuiltInFunctions.RunMcpToolAsync(
                CreateToolContext("json"), fixture.Client.Object, fixture.Context)))
            : ReadBody(await BuiltInFunctions.RunAgentHttpAsync(
                fixture.Request, fixture.Client.Object, fixture.Context));

        Assert.True(body.RootElement.TryGetProperty("result", out JsonElement result),
            "The JSON response must include the lossless terminal result, not only its AgentResponse projection.");
        Assert.Equal("response-1", result.GetProperty("responseId").GetString());
        Assert.Equal("agent-1", result.GetProperty("agentId").GetString());
        Assert.Equal("stop", result.GetProperty("finishReason").GetString());
        Assert.Equal("AQID", result.GetProperty("continuationToken").GetString());
        Assert.Equal(8, result.GetProperty("usage").GetProperty("totalTokenCount").GetInt64());
        Assert.True(result.GetProperty("futureResponseField").GetProperty("retained").GetBoolean());
        Assert.Equal("west", result.GetProperty("extensionData").GetProperty("region").GetString());
        Assert.Equal(value is not null, result.TryGetProperty("value", out JsonElement actualValue));
        if (value is not null)
        {
            Assert.Equal(value, actualValue.GetRawText());
        }

        Assert.True(body.RootElement.TryGetProperty("response", out _));
    }

    private const string BuiltInFunctionsTestResponseFormat = "responseFormat";

    private static DurableAgentState CreateSuccessfulMailboxState(string correlation, string? value)
    {
        string valueField = value is null ? string.Empty : $",\"value\":{value}";
        return JsonSerializer.Deserialize<DurableAgentState>(
            $$"""
            {
              "schemaVersion":"2.0.0",
              "data":{
                "conversationHistory":[],
                "terminalResults":{
                  "{{correlation}}":{
                    "correlationId":"{{correlation}}","outcome":"succeeded","completedAt":"2026-09-11T12:00:00Z",
                    "response":{
                      "messages":[{"role":"assistant","contents":[{"$type":"text","text":"full result"}]}],
                      "responseId":"response-1","agentId":"agent-1","finishReason":"stop","continuationToken":"AQID",
                      "createdAt":"2026-09-11T12:00:00Z",
                      "usage":{"inputTokenCount":5,"outputTokenCount":3,"totalTokenCount":8},
                      "extensionData":{"region":"west"},"futureResponseField":{"retained":true}
                      {{valueField}}
                    }
                  }
                },
                "completionReceipts":{
                  "{{correlation}}":{
                    "correlationId":"{{correlation}}","outcome":"succeeded",
                    "completedAt":"2026-09-11T12:00:00Z","resultState":"available"
                  }
                }
              }
            }
            """)!;
    }

    private static DurableAgentState CreateMailboxState(string correlation, string outcome, bool available)
    {
        string result = available
            ? $$"""
                "{{correlation}}":{
                  "correlationId":"{{correlation}}","outcome":"failed","completedAt":"2026-09-11T12:00:00Z",
                  "response":{"messages":[]},
                  "error":{"code":"committedFailure","message":"A durable failure.","details":{"retained":true} }
                }
                """
            : string.Empty;
        string unavailableTimestamp = available
            ? string.Empty
            : ""","resultUnavailableAt":"2026-09-11T12:00:01Z" """;
        return JsonSerializer.Deserialize<DurableAgentState>(
            $$"""
            {
              "schemaVersion":"2.0.0",
              "data":{
                "conversationHistory":[],
                "terminalResults":{ {{result}} },
                "completionReceipts":{
                  "{{correlation}}":{
                    "correlationId":"{{correlation}}","outcome":"{{outcome}}",
                    "completedAt":"2026-09-11T12:00:00Z","resultState":"{{(available ? "available" : "unavailable")}}"{{unavailableTimestamp}}
                  }
                }
              }
            }
            """)!;
    }

    private static ToolInvocationContext CreateToolContext(string? format = null)
    {
        Dictionary<string, object> arguments = new()
        {
            ["query"] = "hello",
            ["sessionId"] = SessionKey,
        };
        if (format is not null)
        {
            arguments[BuiltInFunctionsTestResponseFormat] = format;
        }

        return new ToolInvocationContext { Name = AgentName, Arguments = arguments };
    }

    private static JsonDocument ReadBody(HttpResponseData response)
    {
        response.Body.Position = 0;
        return JsonDocument.Parse(response.Body);
    }

    private sealed class EndpointFixture : IDisposable
    {
        private readonly ServiceProvider _services;
        private readonly MemoryStream _requestBody = new(Encoding.UTF8.GetBytes("hello"));
        private readonly MemoryStream _responseBody = new();
        private string? _correlationId;

        public EndpointFixture(
            bool waitForResponse = true,
            string accept = "application/json",
            Func<string, DurableAgentState>? stateFactory = null,
            CancellationToken cancellationToken = default)
        {
            ServiceCollection services = new();
            services.AddLogging();
            services.ConfigureDurableAgents(agents =>
                agents.AddAIAgent(new TestAgent(AgentName, "An agent used for endpoint tests.")));
            services.Configure<WorkerOptions>(options =>
                options.Serializer = new JsonObjectSerializer(new JsonSerializerOptions(JsonSerializerDefaults.Web)));
            this._services = services.BuildServiceProvider();

            Mock<FunctionDefinition> definition = new();
            definition.SetupGet(d => d.Name).Returns("http-" + AgentName);
            Mock<FunctionContext> context = new();
            context.SetupGet(c => c.InstanceServices).Returns(this._services);
            context.SetupGet(c => c.FunctionDefinition).Returns(definition.Object);
            context.SetupGet(c => c.InvocationId).Returns("invocation-1");
            context.SetupGet(c => c.CancellationToken).Returns(cancellationToken);
            this.Context = context.Object;

            HttpHeadersCollection headers = new();
            headers.Add("Accept", accept);
            Mock<HttpResponseData> response = new(this.Context);
            response.SetupProperty(r => r.StatusCode, HttpStatusCode.OK);
            response.SetupProperty(r => r.Body, this._responseBody);
            response.SetupGet(r => r.Headers).Returns(new HttpHeadersCollection());
            Mock<HttpRequestData> request = new(this.Context);
            request.SetupGet(r => r.Headers).Returns(headers);
            request.SetupGet(r => r.Body).Returns(this._requestBody);
            request.SetupGet(r => r.Url).Returns(new Uri(
                $"https://localhost/api/agents/{AgentName}/run?session_id={SessionKey}&wait_for_response={waitForResponse}"));
            request.SetupGet(r => r.Query).Returns(new NameValueCollection
            {
                ["session_id"] = SessionKey,
                ["wait_for_response"] = waitForResponse.ToString(),
            });
            request.Setup(r => r.CreateResponse()).Returns(response.Object);
            this.Request = request.Object;

            this.Entities = new Mock<DurableEntityClient>("test") { CallBase = true };
            this.Entities
                .Setup(c => c.SignalEntityAsync(
                    It.IsAny<EntityInstanceId>(), It.IsAny<string>(), It.IsAny<object>(),
                    It.IsAny<SignalEntityOptions>(), It.IsAny<CancellationToken>()))
                .Callback<EntityInstanceId, string, object, SignalEntityOptions?, CancellationToken>(
                    (_, _, input, _, _) => this._correlationId = Assert.IsType<RunRequest>(input).CorrelationId)
                .Returns(Task.CompletedTask);
            this.Entities
                .Setup(c => c.GetEntityAsync<DurableAgentState>(
                    It.IsAny<EntityInstanceId>(), true, It.IsAny<CancellationToken>()))
                .Returns<EntityInstanceId, bool, CancellationToken>((id, _, _) =>
                    Task.FromResult<EntityMetadata<DurableAgentState>?>(
                        new(id, (stateFactory ?? CreateLegacyState)(this._correlationId!))));
            this.Client = new Mock<DurableTaskClient>("test");
            this.Client.SetupGet(c => c.Entities).Returns(this.Entities.Object);
        }

        public HttpRequestData Request { get; }

        public FunctionContext Context { get; }

        public Mock<DurableTaskClient> Client { get; }

        public Mock<DurableEntityClient> Entities { get; }

        public void Dispose()
        {
            this._services.Dispose();
            this._requestBody.Dispose();
            this._responseBody.Dispose();
        }

        private static DurableAgentState CreateLegacyState(string correlationId) =>
            JsonSerializer.Deserialize<DurableAgentState>(
                $$"""
                {
                  "schemaVersion":"1.2.0",
                  "data":{"conversationHistory":[{
                    "$type":"response","correlationId":"{{correlationId}}",
                    "createdAt":"2026-09-11T12:00:00Z",
                    "messages":[{"role":"assistant","contents":[{"$type":"text","text":"original result"}]}]
                  }] }
                }
                """)!;
    }
}
