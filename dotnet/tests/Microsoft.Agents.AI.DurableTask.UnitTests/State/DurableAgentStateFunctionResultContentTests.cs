// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit.State;

/// <summary>
/// Regression tests for function results that are not plain JSON values, such as the
/// <see cref="AIContent"/> results produced by MCP tools.
/// See https://github.com/microsoft/agent-framework-durable-extension/issues/33.
/// </summary>
public sealed class DurableAgentStateFunctionResultContentTests
{
    private static readonly JsonTypeInfo s_stateContentTypeInfo =
        DurableAgentStateJsonContext.Default.GetTypeInfo(typeof(DurableAgentStateContent))!;

    private static FunctionResultContent RoundTrip(FunctionResultContent content)
    {
        DurableAgentStateContent durableContent = DurableAgentStateContent.FromAIContent(content);
        string json = JsonSerializer.Serialize(durableContent, s_stateContentTypeInfo);

        DurableAgentStateContent? deserialized =
            (DurableAgentStateContent?)JsonSerializer.Deserialize(json, s_stateContentTypeInfo);

        Assert.NotNull(deserialized);
        return Assert.IsType<FunctionResultContent>(deserialized.ToAIContent());
    }

    [Fact]
    public void SingleAIContentResultRoundTrips()
    {
        // McpClientTool returns a single AIContent when the tool result has exactly one content block.
        FunctionResultContent result = RoundTrip(new("call-1", new TextContent("hello from mcp")));

        Assert.Equal("call-1", result.CallId);
        TextContent text = Assert.IsType<TextContent>(result.Result);
        Assert.Equal("hello from mcp", text.Text);
    }

    [Fact]
    public void MultipleAIContentResultsRoundTrip()
    {
        // McpClientTool returns AIContent[] when the tool result has multiple content blocks.
        AIContent[] contents = [new TextContent("first"), new UriContent("https://example.com/img.png", "image/png")];
        FunctionResultContent result = RoundTrip(new("call-2", contents));

        Assert.Equal("call-2", result.CallId);
        IEnumerable<AIContent> roundTripped = Assert.IsAssignableFrom<IEnumerable<AIContent>>(result.Result);

        AIContent[] items = [.. roundTripped];
        Assert.Equal(2, items.Length);
        Assert.Equal("first", Assert.IsType<TextContent>(items[0]).Text);
        Assert.Equal(new Uri("https://example.com/img.png"), Assert.IsType<UriContent>(items[1]).Uri);
    }

    [Fact]
    public void JsonElementResultRoundTrips()
    {
        // Tools created through AIFunctionFactory marshal their return value into a JsonElement.
        JsonElement element = JsonSerializer.SerializeToElement(new { city = "Seattle", tempF = 72 });
        FunctionResultContent result = RoundTrip(new("call-3", element));

        JsonElement roundTripped = Assert.IsType<JsonElement>(result.Result);
        Assert.Equal("Seattle", roundTripped.GetProperty("city").GetString());
        Assert.Equal(72, roundTripped.GetProperty("tempF").GetInt32());
    }

    [Fact]
    public void ObjectResultRoundTrips()
    {
        // Custom AIFunction implementations may return arbitrary objects.
        FunctionResultContent result = RoundTrip(new("call-4", new Forecast("Seattle", 72)));

        JsonElement roundTripped = Assert.IsType<JsonElement>(result.Result);
        Assert.Equal("Seattle", roundTripped.GetProperty("city").GetString());
        Assert.Equal(72, roundTripped.GetProperty("tempF").GetInt32());
    }

    [Fact]
    public void StringResultRoundTrips()
    {
        FunctionResultContent result = RoundTrip(new("call-5", "return value"));

        JsonElement roundTripped = Assert.IsType<JsonElement>(result.Result);
        Assert.Equal("return value", roundTripped.GetString());
    }

    [Fact]
    public void NullResultRoundTrips()
    {
        FunctionResultContent result = RoundTrip(new("call-6", result: null));

        Assert.Equal("call-6", result.CallId);
        Assert.Null(result.Result);
    }

    [Fact]
    public void ErrorContentResultRoundTrips()
    {
        // MCP tools surface tool failures as an ErrorContent result rather than a JSON payload.
        FunctionResultContent result = RoundTrip(new("call-8", new ErrorContent("tool exploded") { ErrorCode = "E42" }));

        ErrorContent error = Assert.IsType<ErrorContent>(result.Result);
        Assert.Equal("tool exploded", error.Message);
        Assert.Equal("E42", error.ErrorCode);
    }

    [Fact]
    public void PreviouslyPersistedResultIsStillReadable()
    {
        // State written before AIContent results were supported stores the raw JSON under "result".
        const string LegacyJson = """{"$type":"functionResult","callId":"call-7","result":{"city":"Seattle"}}""";

        DurableAgentStateContent? deserialized =
            (DurableAgentStateContent?)JsonSerializer.Deserialize(LegacyJson, s_stateContentTypeInfo);

        Assert.NotNull(deserialized);
        FunctionResultContent result = Assert.IsType<FunctionResultContent>(deserialized.ToAIContent());

        Assert.Equal("call-7", result.CallId);
        JsonElement element = Assert.IsType<JsonElement>(result.Result);
        Assert.Equal("Seattle", element.GetProperty("city").GetString());
    }

    private sealed record Forecast(string City, int TempF);
}
