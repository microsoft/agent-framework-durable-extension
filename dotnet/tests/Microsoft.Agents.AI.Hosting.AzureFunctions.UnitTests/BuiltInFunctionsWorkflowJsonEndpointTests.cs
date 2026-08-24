// Copyright (c) Microsoft. All rights reserved.

using System.Net;
using System.Text;
using System.Text.Json;
using Azure.Core.Serialization;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Options;
using Moq;

namespace Microsoft.Agents.AI.Hosting.AzureFunctions.UnitTests;

/// <summary>
/// Verifies that the workflow status and respond endpoints, which have no plain-text
/// representation on success, also return JSON on failure regardless of the request's
/// <c>Accept</c> header.
/// </summary>
public sealed class BuiltInFunctionsWorkflowJsonEndpointTests
{
    private const string OrchestrationName = "dafx-TestWorkflow";
    private const string StatusFunctionName = "http-TestWorkflow-status";
    private const string RespondFunctionName = "http-TestWorkflow-respond";
    private const string RunId = "workflow-123";
    private const string EventName = "ApprovalPort";
    private const string ValidRespondBody = @"{""eventName"":""" + EventName + @""",""response"":{""approved"":true}}";

    public static TheoryData<string?> AcceptHeaders => new(null, "text/plain", "application/json", "*/*");

    [Theory]
    [MemberData(nameof(AcceptHeaders))]
    public async Task GetWorkflowStatusAsync_MissingRunId_ReturnsJsonErrorAsync(string? accept)
    {
        (HttpRequestData request, FunctionContext context) = CreateRequest(StatusFunctionName, runId: null, accept);

        HttpResponseData response = await BuiltInFunctions.GetWorkflowStatusAsync(
            request, new Mock<DurableTaskClient>("test").Object, context);

        AssertJsonError(response, HttpStatusCode.BadRequest, "Run ID is required.");
    }

    [Theory]
    [MemberData(nameof(AcceptHeaders))]
    public async Task GetWorkflowStatusAsync_UnknownRun_ReturnsJsonErrorAsync(string? accept)
    {
        (HttpRequestData request, FunctionContext context) = CreateRequest(StatusFunctionName, RunId, accept);

        HttpResponseData response = await BuiltInFunctions.GetWorkflowStatusAsync(
            request, CreateClient(metadata: null).Object, context);

        AssertJsonError(response, HttpStatusCode.NotFound, $"Workflow run '{RunId}' not found.");
    }

    [Theory]
    [MemberData(nameof(AcceptHeaders))]
    public async Task GetWorkflowStatusAsync_ExistingRun_ReturnsJsonAsync(string? accept)
    {
        (HttpRequestData request, FunctionContext context) = CreateRequest(StatusFunctionName, RunId, accept);
        OrchestrationMetadata metadata = CreateMetadata(OrchestrationRuntimeStatus.Running, CreatePendingEventStatus());

        HttpResponseData response = await BuiltInFunctions.GetWorkflowStatusAsync(
            request, CreateClient(metadata).Object, context);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        AssertJsonContentType(response);
        using JsonDocument body = JsonDocument.Parse(GetResponseBody(response));
        Assert.Equal(RunId, body.RootElement.GetProperty("runId").GetString());
        Assert.Equal("Running", body.RootElement.GetProperty("status").GetString());
        JsonElement pending = Assert.Single(body.RootElement.GetProperty("waitingForInput").EnumerateArray());
        Assert.Equal(EventName, pending.GetProperty("eventName").GetString());
    }

    [Theory]
    [MemberData(nameof(AcceptHeaders))]
    public async Task RespondToWorkflowAsync_MissingRunId_ReturnsJsonErrorAsync(string? accept)
    {
        (HttpRequestData request, FunctionContext context) = CreateRequest(RespondFunctionName, runId: null, accept);

        HttpResponseData response = await BuiltInFunctions.RespondToWorkflowAsync(
            request, new Mock<DurableTaskClient>("test").Object, context);

        AssertJsonError(response, HttpStatusCode.BadRequest, "Run ID is required.");
    }

    [Theory]
    [MemberData(nameof(AcceptHeaders))]
    public async Task RespondToWorkflowAsync_MalformedBody_ReturnsJsonErrorAsync(string? accept)
    {
        (HttpRequestData request, FunctionContext context) =
            CreateRequest(RespondFunctionName, RunId, accept, body: "{ not json");

        HttpResponseData response = await BuiltInFunctions.RespondToWorkflowAsync(
            request, new Mock<DurableTaskClient>("test").Object, context);

        AssertJsonError(response, HttpStatusCode.BadRequest, "Request body is not valid JSON.");
    }

    [Theory]
    [MemberData(nameof(AcceptHeaders))]
    public async Task RespondToWorkflowAsync_MissingEventName_ReturnsJsonErrorAsync(string? accept)
    {
        (HttpRequestData request, FunctionContext context) =
            CreateRequest(RespondFunctionName, RunId, accept, body: """{"response":{"approved":true}}""");

        HttpResponseData response = await BuiltInFunctions.RespondToWorkflowAsync(
            request, new Mock<DurableTaskClient>("test").Object, context);

        AssertJsonError(
            response,
            HttpStatusCode.BadRequest,
            "Body must contain a non-empty 'eventName' and a 'response' property.");
    }

    [Theory]
    [MemberData(nameof(AcceptHeaders))]
    public async Task RespondToWorkflowAsync_UnknownRun_ReturnsJsonErrorAsync(string? accept)
    {
        (HttpRequestData request, FunctionContext context) =
            CreateRequest(RespondFunctionName, RunId, accept, body: ValidRespondBody);

        HttpResponseData response = await BuiltInFunctions.RespondToWorkflowAsync(
            request, CreateClient(metadata: null).Object, context);

        AssertJsonError(response, HttpStatusCode.NotFound, $"Workflow run '{RunId}' not found.");
    }

    [Theory]
    [MemberData(nameof(AcceptHeaders))]
    public async Task RespondToWorkflowAsync_TerminalRun_ReturnsJsonErrorAsync(string? accept)
    {
        (HttpRequestData request, FunctionContext context) =
            CreateRequest(RespondFunctionName, RunId, accept, body: ValidRespondBody);
        OrchestrationMetadata metadata = CreateMetadata(OrchestrationRuntimeStatus.Completed);

        HttpResponseData response = await BuiltInFunctions.RespondToWorkflowAsync(
            request, CreateClient(metadata).Object, context);

        AssertJsonError(
            response,
            HttpStatusCode.BadRequest,
            $"Workflow run '{RunId}' is in terminal state 'Completed'.");
    }

    [Theory]
    [MemberData(nameof(AcceptHeaders))]
    public async Task RespondToWorkflowAsync_UnexpectedEvent_ReturnsJsonErrorAsync(string? accept)
    {
        (HttpRequestData request, FunctionContext context) =
            CreateRequest(RespondFunctionName, RunId, accept, body: ValidRespondBody);
        OrchestrationMetadata metadata = CreateMetadata(
            OrchestrationRuntimeStatus.Running,
            CreatePendingEventStatus("SomeOtherPort"));

        HttpResponseData response = await BuiltInFunctions.RespondToWorkflowAsync(
            request, CreateClient(metadata).Object, context);

        AssertJsonError(
            response,
            HttpStatusCode.BadRequest,
            $"Workflow is not waiting for event '{EventName}'.");
    }

    [Theory]
    [MemberData(nameof(AcceptHeaders))]
    public async Task RespondToWorkflowAsync_PendingEvent_ReturnsJsonAsync(string? accept)
    {
        (HttpRequestData request, FunctionContext context) =
            CreateRequest(RespondFunctionName, RunId, accept, body: ValidRespondBody);
        OrchestrationMetadata metadata = CreateMetadata(
            OrchestrationRuntimeStatus.Running,
            CreatePendingEventStatus());
        Mock<DurableTaskClient> client = CreateClient(metadata);
        client
            .Setup(c => c.RaiseEventAsync(RunId, EventName, It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        HttpResponseData response = await BuiltInFunctions.RespondToWorkflowAsync(
            request, client.Object, context);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        AssertJsonContentType(response);
        using JsonDocument body = JsonDocument.Parse(GetResponseBody(response));
        Assert.Equal(RunId, body.RootElement.GetProperty("runId").GetString());
        Assert.Equal(EventName, body.RootElement.GetProperty("eventName").GetString());
        Assert.True(body.RootElement.GetProperty("validated").GetBoolean());
    }

    private static void AssertJsonError(HttpResponseData response, HttpStatusCode expectedStatus, string expectedError)
    {
        Assert.Equal(expectedStatus, response.StatusCode);
        AssertJsonContentType(response);
        using JsonDocument body = JsonDocument.Parse(GetResponseBody(response));
        Assert.Equal((int)expectedStatus, body.RootElement.GetProperty("status").GetInt32());
        Assert.Equal(expectedError, body.RootElement.GetProperty("error").GetString());
    }

    private static void AssertJsonContentType(HttpResponseData response)
    {
        Assert.True(response.Headers.TryGetValues("Content-Type", out IEnumerable<string>? contentTypes));
        Assert.Contains("application/json", Assert.Single(contentTypes), StringComparison.OrdinalIgnoreCase);
    }

    private static string GetResponseBody(HttpResponseData response) =>
        Encoding.UTF8.GetString(((MemoryStream)response.Body).ToArray());

    private static string CreatePendingEventStatus(string eventName = EventName) =>
        JsonSerializer.Serialize(new
        {
            pendingEvents = new[] { new { eventName, input = """{"amount":100}""" } },
        });

    private static OrchestrationMetadata CreateMetadata(
        OrchestrationRuntimeStatus runtimeStatus,
        string? serializedCustomStatus = null) =>
        new(OrchestrationName, RunId)
        {
            RuntimeStatus = runtimeStatus,
            DataConverter = Microsoft.DurableTask.Converters.JsonDataConverter.Default,
            SerializedCustomStatus = serializedCustomStatus,
        };

    private static Mock<DurableTaskClient> CreateClient(OrchestrationMetadata? metadata)
    {
        Mock<DurableTaskClient> client = new("test");
        client
            .Setup(c => c.GetInstanceAsync(RunId, true, It.IsAny<CancellationToken>()))
            .ReturnsAsync(metadata);
        return client;
    }

    private static (HttpRequestData Request, FunctionContext Context) CreateRequest(
        string functionName,
        string? runId,
        string? accept,
        string? body = null)
    {
        Mock<ObjectSerializer> serializer = new();
        serializer
            .Setup(s => s.SerializeAsync(
                It.IsAny<Stream>(), It.IsAny<object?>(), It.IsAny<Type>(), It.IsAny<CancellationToken>()))
            .Returns<Stream, object?, Type, CancellationToken>(async (stream, value, type, token) =>
                await JsonSerializer.SerializeAsync(stream, value, type, cancellationToken: token));
        serializer
            .Setup(s => s.DeserializeAsync(It.IsAny<Stream>(), It.IsAny<Type>(), It.IsAny<CancellationToken>()))
            .Returns<Stream, Type, CancellationToken>((stream, type, token) =>
                JsonSerializer.DeserializeAsync(stream, type, cancellationToken: token));

        WorkerOptions workerOptions = new() { Serializer = serializer.Object };
        Mock<IOptions<WorkerOptions>> options = new();
        options.SetupGet(o => o.Value).Returns(workerOptions);
        Mock<IServiceProvider> services = new();
        services.Setup(s => s.GetService(typeof(IOptions<WorkerOptions>))).Returns(options.Object);

        Dictionary<string, object?> bindingData = new(StringComparer.OrdinalIgnoreCase);
        if (runId is not null)
        {
            bindingData["runId"] = runId;
        }

        Mock<BindingContext> bindingContext = new();
        bindingContext.SetupGet(b => b.BindingData).Returns(bindingData);

        Mock<FunctionDefinition> functionDefinition = new();
        functionDefinition.SetupGet(d => d.Name).Returns(functionName);

        Mock<FunctionContext> context = new();
        context.SetupGet(c => c.CancellationToken).Returns(CancellationToken.None);
        context.SetupGet(c => c.InstanceServices).Returns(services.Object);
        context.SetupGet(c => c.BindingContext).Returns(bindingContext.Object);
        context.SetupGet(c => c.FunctionDefinition).Returns(functionDefinition.Object);

        Mock<HttpResponseData> response = new(context.Object);
        response.SetupProperty(r => r.StatusCode, HttpStatusCode.OK);
        response.SetupProperty(r => r.Body, new MemoryStream());
        response.SetupGet(r => r.Headers).Returns(new HttpHeadersCollection());

        HttpHeadersCollection requestHeaders = new();
        if (accept is not null)
        {
            Assert.True(requestHeaders.TryAddWithoutValidation("Accept", accept));
        }

        Mock<HttpRequestData> request = new(context.Object);
        request.SetupGet(r => r.Headers).Returns(requestHeaders);
        request.SetupGet(r => r.Body).Returns(new MemoryStream(Encoding.UTF8.GetBytes(body ?? string.Empty)));
        request.Setup(r => r.CreateResponse()).Returns(response.Object);

        return (request.Object, context.Object);
    }
}
