// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit.State;

public sealed class DurableAgentStateTests
{
    [Fact]
    public void NewStateDefaultsToCurrentSchemaVersion()
    {
        DurableAgentState state = new();

        Assert.Equal(DurableAgentState.CurrentSchemaVersion, state.SchemaVersion);
        Assert.Equal("1.2.0", state.SchemaVersion);
        Assert.Equal("2.0.0", DurableAgentState.RevisedSchemaVersion);
    }

    [Fact]
    public void InvalidVersion()
    {
        // Arrange
        const string JsonText = """
            {
                "schemaVersion": "hello"
            }
            """;

        // Act & Assert
        Assert.Throws<InvalidOperationException>(
            () => JsonSerializer.Deserialize(JsonText, DurableAgentStateJsonContext.Default.DurableAgentState));
    }

    [Theory]
    [InlineData("1.0.0")]
    [InlineData("1.1.9")]
    [InlineData("1.2.0")]
    [InlineData("1.2.7")]
    [InlineData("1.3.0")]
    [InlineData("1.9.2")]
    [InlineData("1.2147483648.0")]
    [InlineData("1.2.2147483648")]
    public void StrictNumericSemVerIsAccepted(string version)
    {
        string json = $$"""
            {
              "schemaVersion": "{{version}}",
              "data": {
                "conversationHistory": []
              }
            }
            """;

        DurableAgentState state = Assert.IsType<DurableAgentState>(
            JsonSerializer.Deserialize(json, DurableAgentStateJsonContext.Default.DurableAgentState));

        Assert.Equal(version, state.SchemaVersion);
    }

    [Theory]
    [InlineData("1.2")]
    [InlineData("1.2.0.0")]
    [InlineData("v1.2.0")]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("-1.2.0")]
    [InlineData("1.-2.0")]
    [InlineData("1.2.-3")]
    [InlineData("01.2.0")]
    [InlineData("1.02.0")]
    [InlineData("1.2.00")]
    [InlineData("1.2.0-alpha")]
    [InlineData("1.2.0+build")]
    [InlineData("1.2.0-alpha+build")]
    public void InvalidSchemaVersionGrammarIsRejected(string version)
    {
        string json = $$"""
            {
              "schemaVersion": {{JsonSerializer.Serialize(version)}},
              "data": {
                "conversationHistory": []
              }
            }
            """;

        Assert.Throws<InvalidOperationException>(
            () => JsonSerializer.Deserialize(
                json,
                DurableAgentStateJsonContext.Default.DurableAgentState));
    }

    [Fact]
    public void NonStringSchemaVersionIsRejected()
    {
        const string JsonText = """
            {
              "schemaVersion": 10200,
              "data": {
                "conversationHistory": []
              }
            }
            """;

        Assert.Throws<InvalidOperationException>(
            () => JsonSerializer.Deserialize(
                JsonText,
                DurableAgentStateJsonContext.Default.DurableAgentState));
    }

    [Fact]
    public void InvalidSchemaVersionCannotBeSerialized()
    {
        DurableAgentState state = new()
        {
            SchemaVersion = "1.2",
        };

        Assert.Throws<InvalidOperationException>(
            () => JsonSerializer.Serialize(
                state,
                DurableAgentStateJsonContext.Default.DurableAgentState));
    }

    [Fact]
    public void UnsupportedMajorVersion()
    {
        // Arrange
        const string JsonText = """
            {
                "schemaVersion": "3.0.0"
            }
            """;

        // Act & Assert
        Assert.Throws<InvalidOperationException>(
            () => JsonSerializer.Deserialize(JsonText, DurableAgentStateJsonContext.Default.DurableAgentState));
    }

    [Fact]
    public void MissingData()
    {
        // Arrange
        const string JsonText = """
            {
                "schemaVersion": "1.0.0"
            }
            """;

        // Act & Assert
        Assert.Throws<InvalidOperationException>(
            () => JsonSerializer.Deserialize(JsonText, DurableAgentStateJsonContext.Default.DurableAgentState));
    }

    [Fact]
    public void UnknownDataPropertiesRoundTrip()
    {
        // Arrange
        const string JsonText = """
            {
                "schemaVersion": "1.0.0",
                "data": {
                    "conversationHistory": [],
                    "extraField": "someValue"
                }
            }
            """;

        // Act
        DurableAgentState? state = JsonSerializer.Deserialize(JsonText, DurableAgentStateJsonContext.Default.DurableAgentState);

        // Assert
        Assert.NotNull(state?.Data?.UnknownProperties);

        Assert.True(state.Data.UnknownProperties!.ContainsKey("extraField"));
        Assert.Equal("someValue", state.Data.UnknownProperties["extraField"].ToString());

        // Act
        string jsonState = JsonSerializer.Serialize(state, DurableAgentStateJsonContext.Default.DurableAgentState);
        JsonDocument? jsonDocument = JsonSerializer.Deserialize<JsonDocument>(jsonState);

        // Assert
        Assert.NotNull(jsonDocument);
        Assert.True(jsonDocument.RootElement.TryGetProperty("data", out JsonElement dataElement));
        Assert.True(dataElement.TryGetProperty("extraField", out JsonElement extraFieldElement));
        Assert.Equal("someValue", extraFieldElement.ToString());
    }

    [Fact]
    public void DeclaredExtensionDataAndUnknownPropertiesRoundTripIndependently()
    {
        const string JsonText = """
            {
              "schemaVersion": "1.2.0",
              "extensionData": { "rootMetadata": "root" },
              "futureRoot": 1,
              "data": {
                "extensionData": { "dataMetadata": "data" },
                "futureData": 2,
                "conversationHistory": [{
                  "$type": "response",
                  "correlationId": "correlation",
                  "createdAt": "2026-09-07T12:00:00+00:00",
                  "extensionData": { "entryMetadata": "entry" },
                  "futureEntry": 3,
                  "usage": {
                    "extensionData": { "providerCount": 4 },
                    "futureUsage": 5
                  },
                  "messages": [{
                    "role": "assistant",
                    "extensionData": { "messageMetadata": "message" },
                    "futureMessage": 6,
                    "contents": [{
                      "$type": "text",
                      "text": "answer",
                      "futureContent": 7
                    }]
                  }]
                }]
              }
            }
            """;

        DurableAgentState state = Assert.IsType<DurableAgentState>(
            JsonSerializer.Deserialize(JsonText, DurableAgentStateJsonContext.Default.DurableAgentState));
        DurableAgentStateResponse response =
            Assert.IsType<DurableAgentStateResponse>(Assert.Single(state.Data.ConversationHistory));
        DurableAgentStateMessage message = Assert.Single(response.Messages);
        DurableAgentStateTextContent content =
            Assert.IsType<DurableAgentStateTextContent>(Assert.Single(message.Contents));
        DurableAgentStateUsage usage = Assert.IsType<DurableAgentStateUsage>(response.Usage);

        Assert.Equal("root", state.ExtensionData?["rootMetadata"].GetString());
        Assert.Equal(1, state.UnknownProperties?["futureRoot"].GetInt32());
        Assert.Equal("data", state.Data.ExtensionData?["dataMetadata"].GetString());
        Assert.Equal(2, state.Data.UnknownProperties?["futureData"].GetInt32());
        Assert.Equal("entry", response.ExtensionData?["entryMetadata"].GetString());
        Assert.Equal(3, response.UnknownProperties?["futureEntry"].GetInt32());
        Assert.Equal("message", message.AdditionalProperties?["messageMetadata"].GetString());
        Assert.Equal(6, message.UnknownProperties?["futureMessage"].GetInt32());
        Assert.Equal(7, content.UnknownProperties?["futureContent"].GetInt32());
        Assert.Equal(4, usage.ExtensionData?["providerCount"].GetInt32());
        Assert.Equal(5, usage.UnknownProperties?["futureUsage"].GetInt32());

        string roundTrip = JsonSerializer.Serialize(
            state,
            DurableAgentStateJsonContext.Default.DurableAgentState);
        using JsonDocument document = JsonDocument.Parse(roundTrip);
        JsonElement root = document.RootElement;
        JsonElement data = root.GetProperty("data");
        JsonElement entry = data.GetProperty("conversationHistory")[0];
        JsonElement roundTrippedMessage = entry.GetProperty("messages")[0];
        JsonElement roundTrippedContent = roundTrippedMessage.GetProperty("contents")[0];
        JsonElement roundTrippedUsage = entry.GetProperty("usage");

        Assert.Equal("root", root.GetProperty("extensionData").GetProperty("rootMetadata").GetString());
        Assert.Equal(1, root.GetProperty("futureRoot").GetInt32());
        Assert.Equal("data", data.GetProperty("extensionData").GetProperty("dataMetadata").GetString());
        Assert.Equal(2, data.GetProperty("futureData").GetInt32());
        Assert.Equal("entry", entry.GetProperty("extensionData").GetProperty("entryMetadata").GetString());
        Assert.Equal(3, entry.GetProperty("futureEntry").GetInt32());
        Assert.Equal(
            "message",
            roundTrippedMessage.GetProperty("extensionData").GetProperty("messageMetadata").GetString());
        Assert.Equal(6, roundTrippedMessage.GetProperty("futureMessage").GetInt32());
        Assert.Equal(7, roundTrippedContent.GetProperty("futureContent").GetInt32());
        Assert.Equal(4, roundTrippedUsage.GetProperty("extensionData").GetProperty("providerCount").GetInt32());
        Assert.Equal(5, roundTrippedUsage.GetProperty("futureUsage").GetInt32());
    }

    [Fact]
    public void BasicState()
    {
        // Arrange
        const string JsonText = """
          {
              "schemaVersion": "1.0.0",
              "data": {
                  "conversationHistory": [
                      {
                          "$type": "request",
                          "correlationId": "12345",
                          "createdAt": "2024-01-01T12:00:00Z",
                          "messages": [
                              {
                                  "role": "user",
                                  "contents": [
                                      {
                                          "$type": "text",
                                          "text": "Hello, agent!"
                                      }
                                  ]
                              }
                          ]
                      },
                      {
                          "$type": "response",
                          "correlationId": "12345",
                          "createdAt": "2024-01-01T12:01:00Z",
                          "messages": [
                              {
                                  "role": "agent",
                                  "contents": [
                                      {
                                          "$type": "text",
                                          "text": "Hi user!"
                                      }
                                  ]
                              }
                          ]
                      }
                  ]
              }
          }
          """;

        // Act
        DurableAgentState? state = JsonSerializer.Deserialize(
            JsonText,
            DurableAgentStateJsonContext.Default.DurableAgentState);

        // Assert
        Assert.NotNull(state);
        Assert.Equal("1.0.0", state.SchemaVersion);
        Assert.NotNull(state.Data);

        Assert.Collection(state.Data.ConversationHistory,
            entry =>
            {
                Assert.IsType<DurableAgentStateRequest>(entry);
                Assert.Equal("12345", entry.CorrelationId);
                Assert.Equal(DateTimeOffset.Parse("2024-01-01T12:00:00Z"), entry.CreatedAt);
                Assert.Single(entry.Messages);
                Assert.Equal("user", entry.Messages[0].Role);
                DurableAgentStateContent content = Assert.Single(entry.Messages[0].Contents);
                DurableAgentStateTextContent textContent = Assert.IsType<DurableAgentStateTextContent>(content);
                Assert.Equal("Hello, agent!", textContent.Text);
            },
            entry =>
            {
                Assert.IsType<DurableAgentStateResponse>(entry);
                Assert.Equal("12345", entry.CorrelationId);
                Assert.Equal(DateTimeOffset.Parse("2024-01-01T12:01:00Z"), entry.CreatedAt);
                Assert.Single(entry.Messages);
                Assert.Equal("agent", entry.Messages[0].Role);
                Assert.Single(entry.Messages[0].Contents);
                DurableAgentStateContent content = Assert.Single(entry.Messages[0].Contents);
                DurableAgentStateTextContent textContent = Assert.IsType<DurableAgentStateTextContent>(content);
                Assert.Equal("Hi user!", textContent.Text);
            });
    }

    [Theory]
    [InlineData("1.0.0")]
    [InlineData("1.0.7")]
    [InlineData("1.1.0")]
    [InlineData("1.1.9")]
    public void CloneForWritePromotesOlderCompatibleStateToCurrentVersion(string version)
    {
        string json = $$"""
            {
              "schemaVersion": "{{version}}",
              "data": {
                "conversationHistory": [],
                "ingestedPositions": { "writer": 2 }
              }
            }
            """;
        DurableAgentState state = Assert.IsType<DurableAgentState>(
            JsonSerializer.Deserialize(json, DurableAgentStateJsonContext.Default.DurableAgentState));

        DurableAgentState promoted = state.Clone();
        string roundTrip = JsonSerializer.Serialize(
            promoted,
            DurableAgentStateJsonContext.Default.DurableAgentState);

        Assert.Equal(DurableAgentState.CurrentSchemaVersion, promoted.SchemaVersion);
        Assert.Equal(2, promoted.Data.IngestedPositions?["writer"]);
        Assert.Contains("\"schemaVersion\":\"1.2.0\"", roundTrip, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("1.2.0")]
    [InlineData("1.2.7")]
    [InlineData("1.3.0")]
    [InlineData("1.9.2")]
    public void CloneForWritePreservesCurrentAndFutureCompatibleVersions(string version)
    {
        string json = $$"""
            {
              "schemaVersion": "{{version}}",
              "data": {
                "conversationHistory": []
              }
            }
            """;
        DurableAgentState state = Assert.IsType<DurableAgentState>(
            JsonSerializer.Deserialize(json, DurableAgentStateJsonContext.Default.DurableAgentState));

        DurableAgentState clone = state.Clone();
        string roundTrip = JsonSerializer.Serialize(
            clone,
            DurableAgentStateJsonContext.Default.DurableAgentState);

        Assert.Equal(version, clone.SchemaVersion);
        Assert.Contains($"\"schemaVersion\":\"{version}\"", roundTrip, StringComparison.Ordinal);
    }

    [Fact]
    public void FutureCompatibleVersionAndUnknownFieldsSurviveMutationAndRoundTrip()
    {
        const string JsonText = """
            {
              "schemaVersion": "1.3.0",
              "data": {
                "conversationHistory": [
                  {
                    "$type": "response",
                    "correlationId": "future",
                    "createdAt": "2026-09-06T12:00:00+00:00",
                    "futureEntry": { "keep": true },
                    "messages": [
                      {
                        "role": "assistant",
                        "contents": [],
                        "futureMessage": [1, 2, 3]
                      }
                    ]
                  }
                ],
                "futureData": "preserve"
              },
              "futureRoot": 42
            }
            """;
        DurableAgentState state = Assert.IsType<DurableAgentState>(
            JsonSerializer.Deserialize(JsonText, DurableAgentStateJsonContext.Default.DurableAgentState));

        DurableAgentState mutated = state.Clone();
        mutated.Data.IngestedPositions = new Dictionary<string, int> { ["writer"] = 7 };
        string roundTrip = JsonSerializer.Serialize(
            mutated,
            DurableAgentStateJsonContext.Default.DurableAgentState);
        using JsonDocument document = JsonDocument.Parse(roundTrip);

        Assert.Equal("1.3.0", document.RootElement.GetProperty("schemaVersion").GetString());
        Assert.Equal(42, document.RootElement.GetProperty("futureRoot").GetInt32());
        JsonElement data = document.RootElement.GetProperty("data");
        Assert.Equal("preserve", data.GetProperty("futureData").GetString());
        Assert.Equal(7, data.GetProperty("ingestedPositions").GetProperty("writer").GetInt32());
        JsonElement response = data.GetProperty("conversationHistory")[0];
        Assert.True(response.GetProperty("futureEntry").GetProperty("keep").GetBoolean());
        Assert.Equal(3, response.GetProperty("messages")[0].GetProperty("futureMessage").GetArrayLength());
    }

    [Fact]
    public void PythonPr59ShapeFixtureMigratesIdsAndPreservesExtensions()
    {
        string json = File.ReadAllText(
            Path.Combine(AppContext.BaseDirectory, "Fixtures", "python-durable-agent-state-1.2.json"));
        using JsonDocument sourceDocument = JsonDocument.Parse(json);
        JsonElement sourceUnknownContent = sourceDocument.RootElement.GetProperty("data")
            .GetProperty("conversationHistory")[1]
            .GetProperty("messages")[3]
            .GetProperty("contents")[0]
            .GetProperty("content");
        DurableAgentState state = Assert.IsType<DurableAgentState>(
            JsonSerializer.Deserialize(json, DurableAgentStateJsonContext.Default.DurableAgentState));

        DurableAgentState migrated = state.Clone();
        string roundTrip = JsonSerializer.Serialize(
            migrated,
            DurableAgentStateJsonContext.Default.DurableAgentState);
        using JsonDocument document = JsonDocument.Parse(roundTrip);

        Assert.Equal("producer-request-id", migrated.Data.ConversationHistory[0].Messages[0].MessageId);
        Assert.Equal("python-metadata-only", migrated.Data.ConversationHistory[1].Messages[0].MessageId);
        Assert.Equal("durable_response_corr-python_1", migrated.Data.ConversationHistory[1].Messages[1].MessageId);
        Assert.Equal("durable_response_corr-python_2", migrated.Data.ConversationHistory[1].Messages[2].MessageId);
        Assert.Equal("python-unknown-content", migrated.Data.ConversationHistory[1].Messages[3].MessageId);
        Assert.Equal("durable_errorResponse_corr-error_0", migrated.Data.ConversationHistory[2].Messages[0].MessageId);
        Assert.Equal(
            "durable_compaction_2026-07-27T12:34:56.123456+00:00_0",
            migrated.Data.ConversationHistory[3].Messages[0].MessageId);
        DurableAgentStateResponse response =
            Assert.IsType<DurableAgentStateResponse>(migrated.Data.ConversationHistory[1]);
        ChatMessage metadataOnly = response.ToResponse().Messages[0];
        Assert.Empty(metadataOnly.Contents);
        Assert.Equal("python-metadata-only", metadataOnly.MessageId);
        Assert.Equal("python-agent", metadataOnly.AuthorName);
        Assert.Equal(
            DateTimeOffset.Parse("2026-07-27T12:34:51+00:00"),
            metadataOnly.CreatedAt);
        Assert.Equal(
            "python",
            Assert.IsType<JsonElement>(metadataOnly.AdditionalProperties?["metadataOrigin"]).GetString());
        DurableAgentStateUnknownContent unknown = Assert.IsType<DurableAgentStateUnknownContent>(
            migrated.Data.ConversationHistory[1].Messages[3].Contents[0]);
        JsonElement pythonContent = unknown.Content;
        Assert.Equal(
            "python-owned-user-field",
            pythonContent.GetProperty("$runtimeType").GetString());
        Assert.Equal("future_python_content", pythonContent.GetProperty("type").GetString());
        Assert.Equal("python-value", pythonContent.GetProperty("payload").GetString());
        Assert.Equal(
            3,
            pythonContent.GetProperty("future_payload").GetProperty("nested").GetArrayLength());
        JsonElement persistedUnknownContent = document.RootElement.GetProperty("data")
            .GetProperty("conversationHistory")[1]
            .GetProperty("messages")[3]
            .GetProperty("contents")[0]
            .GetProperty("content");
        Assert.True(JsonElement.DeepEquals(sourceUnknownContent, persistedUnknownContent));
        UsageDetails usage = Assert.IsType<UsageDetails>(response.ToResponse().Usage);
        Assert.Equal(7, usage.AdditionalCounts?["providerCount"]);
        Assert.Equal(11, usage.AdditionalCounts?["futureNumeric"]);
        Assert.DoesNotContain("futureString", usage.AdditionalCounts?.Keys ?? []);
        Assert.DoesNotContain("futureObject", usage.AdditionalCounts?.Keys ?? []);
        Assert.DoesNotContain("futureArray", usage.AdditionalCounts?.Keys ?? []);
        Assert.Contains("\"futureString\":\"seven\"", roundTrip, StringComparison.Ordinal);
        Assert.Contains("\"futureObject\":{\"count\":8}", roundTrip, StringComparison.Ordinal);
        Assert.Contains("\"futureArray\":[9]", roundTrip, StringComparison.Ordinal);
        Assert.Contains("\"type\":\"future_python_content\"", roundTrip, StringComparison.Ordinal);
        Assert.Contains("\"payload\":\"python-value\"", roundTrip, StringComparison.Ordinal);
        Assert.Equal(3, migrated.Data.IngestedPositions?["writer"]);
        Assert.Equal("interop-fixture", migrated.ExtensionData?["rootProducer"].GetString());
        Assert.True(migrated.UnknownProperties?["futureRootProperty"].GetProperty("preserve").GetBoolean());
        Assert.Equal("python", migrated.Data.ExtensionData?["dataProducer"].GetString());
        Assert.True(migrated.Data.UnknownProperties?["futureDataProperty"].GetProperty("preserve").GetBoolean());
        Assert.True(document.RootElement.TryGetProperty("extensionData", out _));
        Assert.True(document.RootElement.TryGetProperty("futureRootProperty", out _));
        Assert.True(document.RootElement.GetProperty("data").TryGetProperty("extensionData", out _));
        Assert.True(document.RootElement.GetProperty("data").TryGetProperty("futureDataProperty", out _));
    }

    [Fact]
    public void OptionalRequestPropertiesAreOmittedWhenAbsent()
    {
        DurableAgentState state = new();
        state.Data.ConversationHistory.Add(
            new DurableAgentStateRequest
            {
                CreatedAt = DateTimeOffset.UtcNow,
            });

        string json = JsonSerializer.Serialize(
            state,
            DurableAgentStateJsonContext.Default.DurableAgentState);

        Assert.DoesNotContain("\"orchestrationId\"", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"responseType\"", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"expirationTimeUtc\"", json, StringComparison.Ordinal);
    }
}
