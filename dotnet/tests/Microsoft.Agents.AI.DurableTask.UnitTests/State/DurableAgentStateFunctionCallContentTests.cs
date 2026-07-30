// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit.State;

/// <summary>
/// Regression tests for function call arguments whose values are not plain JSON.
/// See https://github.com/microsoft/agent-framework-durable-extension/issues/33.
/// </summary>
public sealed class DurableAgentStateFunctionCallContentTests
{
    private static readonly JsonTypeInfo s_stateContentTypeInfo =
        DurableAgentStateJsonContext.Default.GetTypeInfo(typeof(DurableAgentStateContent))!;

    private static FunctionCallContent RoundTrip(FunctionCallContent content)
    {
        DurableAgentStateContent durableContent = DurableAgentStateContent.FromAIContent(content);
        string json = JsonSerializer.Serialize(durableContent, s_stateContentTypeInfo);

        DurableAgentStateContent? deserialized =
            (DurableAgentStateContent?)JsonSerializer.Deserialize(json, s_stateContentTypeInfo);

        Assert.NotNull(deserialized);
        return Assert.IsType<FunctionCallContent>(deserialized.ToAIContent());
    }

    [Fact]
    public void JsonElementArgumentsRoundTrip()
    {
        // Chat clients parse model supplied arguments into JsonElement values.
        JsonElement city = JsonSerializer.SerializeToElement("Seattle");
        FunctionCallContent result = RoundTrip(new("call-1", "get_weather", new Dictionary<string, object?>
        {
            ["city"] = city
        }));

        Assert.Equal("call-1", result.CallId);
        Assert.Equal("get_weather", result.Name);
        Assert.NotNull(result.Arguments);
        Assert.Equal("Seattle", Assert.IsType<JsonElement>(result.Arguments["city"]).GetString());
    }

    [Fact]
    public void ObjectArgumentRoundTrips()
    {
        // Callers can supply function calls containing arbitrary objects, for example when replaying
        // history or resuming a function approval.
        FunctionCallContent result = RoundTrip(new("call-2", "get_weather", new Dictionary<string, object?>
        {
            ["location"] = new Location("Seattle", "WA")
        }));

        JsonElement location = Assert.IsType<JsonElement>(result.Arguments!["location"]);
        Assert.Equal("Seattle", location.GetProperty("city").GetString());
        Assert.Equal("WA", location.GetProperty("state").GetString());
    }

    [Fact]
    public void BclValueArgumentRoundTrips()
    {
        // DateOnly is not registered on DurableAgentStateJsonContext, so an object typed argument
        // holding one used to fail serialization outright.
        FunctionCallContent result = RoundTrip(new("call-3", "get_forecast", new Dictionary<string, object?>
        {
            ["date"] = new DateOnly(2026, 1, 31)
        }));

        Assert.Equal("2026-01-31", Assert.IsType<JsonElement>(result.Arguments!["date"]).GetString());
    }

    [Fact]
    public void NullArgumentRoundTrips()
    {
        FunctionCallContent result = RoundTrip(new("call-4", "get_weather", new Dictionary<string, object?>
        {
            ["city"] = null
        }));

        Assert.Equal(JsonValueKind.Null, Assert.IsType<JsonElement>(result.Arguments!["city"]).ValueKind);
    }

    [Fact]
    public void NoArgumentsRoundTrip()
    {
        FunctionCallContent result = RoundTrip(new("call-5", "get_time", arguments: null));

        Assert.Equal("get_time", result.Name);
        Assert.Empty(result.Arguments!);
    }

    [Fact]
    public void PreviouslyPersistedArgumentsAreStillReadable()
    {
        // State written before arguments were normalized stores the raw JSON under "arguments".
        const string LegacyJson =
            """{"$type":"functionCall","arguments":{"city":"Seattle","days":3},"callId":"call-6","name":"get_forecast"}""";

        DurableAgentStateContent? deserialized =
            (DurableAgentStateContent?)JsonSerializer.Deserialize(LegacyJson, s_stateContentTypeInfo);

        Assert.NotNull(deserialized);
        FunctionCallContent result = Assert.IsType<FunctionCallContent>(deserialized.ToAIContent());

        Assert.Equal("call-6", result.CallId);
        Assert.Equal("Seattle", Assert.IsType<JsonElement>(result.Arguments!["city"]).GetString());
        Assert.Equal(3, Assert.IsType<JsonElement>(result.Arguments["days"]).GetInt32());
    }

    private sealed record Location(string City, string State);
}
