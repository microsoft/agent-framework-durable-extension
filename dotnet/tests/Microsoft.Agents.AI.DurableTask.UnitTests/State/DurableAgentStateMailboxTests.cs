// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Agents.AI;
using Microsoft.Agents.AI.DurableTask.State;
using Microsoft.Extensions.AI;

namespace Microsoft.Agents.AI.DurableTask.Tests.Unit.State;

public sealed class DurableAgentStateMailboxTests
{
    [Fact]
    public void LegacyStateRoundTripsWithoutRevisedFields()
    {
        const string Json = """
            {
              "schemaVersion": "1.2.0",
              "data": {
                "conversationHistory": []
              }
            }
            """;

        DurableAgentState state = Deserialize(Json);
        string roundTrip = Serialize(state);

        Assert.DoesNotContain("\"terminalResults\"", roundTrip, StringComparison.Ordinal);
        Assert.DoesNotContain("\"completionReceipts\"", roundTrip, StringComparison.Ordinal);
        Assert.DoesNotContain("\"historyBinding\"", roundTrip, StringComparison.Ordinal);
    }

    [Fact]
    public void RevisedFixtureRoundTripsTypedMailboxAndFutureFields()
    {
        string json = File.ReadAllText(
            Path.Combine(AppContext.BaseDirectory, "Fixtures", "shared-durable-agent-state-2.0.json"));

        DurableAgentState state = Deserialize(json);
        string roundTrip = Serialize(state);
        DurableAgentStateTerminalResult result = Assert.IsType<DurableAgentStateTerminalResult>(
            state.Data.TerminalResults?["corr-2"]);
        DurableAgentStateCompletionReceipt unavailable = Assert.IsType<DurableAgentStateCompletionReceipt>(
            state.Data.CompletionReceipts?["corr-expired"]);

        Assert.Equal(DurableAgentState.RevisedSchemaVersion, state.SchemaVersion);
        Assert.Equal("contoso.support-history.v1", state.Data.HistoryBinding?.ProviderKey);
        Assert.Equal("response-id-2", result.Response?.ResponseId);
        Assert.Equal(DurableAgentStateCompletionReceipt.UnavailableResult, unavailable.ResultState);
        Assert.Contains("\"futureResponseField\":{\"preserve\":true}", roundTrip, StringComparison.Ordinal);
        Assert.Contains("\"futureReceiptField\":7", roundTrip, StringComparison.Ordinal);
        Assert.Contains("\"futureBindingField\":\"preserve\"", roundTrip, StringComparison.Ordinal);
        Assert.Contains("\"futureRootField\":{\"preserve\":true}", roundTrip, StringComparison.Ordinal);

        DurableAgentState clone = state.Clone();
        Assert.Equal(DurableAgentState.RevisedSchemaVersion, clone.SchemaVersion);
        Assert.Equal("response-id-2", clone.Data.TerminalResults?["corr-2"].Response?.ResponseId);
    }

    [Theory]
    [InlineData("terminalResults")]
    [InlineData("completionReceipts")]
    [InlineData("historyBinding")]
    public void RevisedStateRequiresCompleteLayout(string missingProperty)
    {
        Dictionary<string, object?> data = new()
        {
            ["conversationHistory"] = Array.Empty<object>(),
            ["terminalResults"] = new Dictionary<string, object>(),
            ["completionReceipts"] = new Dictionary<string, object>(),
            ["historyBinding"] = new
            {
                version = 1,
                ownerKind = DurableAgentStateHistoryBinding.DurableStateOwner,
                providerKey = "durable-state.v1",
            },
        };
        _ = data.Remove(missingProperty);
        string json = JsonSerializer.Serialize(new
        {
            schemaVersion = DurableAgentState.RevisedSchemaVersion,
            data,
        });

        Assert.Throws<InvalidOperationException>(() => Deserialize(json));
    }

    [Theory]
    [InlineData("conversationHistory")]
    [InlineData("terminalResults.messages")]
    public void RevisedStateRejectsNullRequiredCollections(string collection)
    {
        string json = collection == "conversationHistory"
            ? """
                {
                  "schemaVersion": "2.0.0",
                  "data": {
                    "conversationHistory": null,
                    "terminalResults": {},
                    "completionReceipts": {},
                    "historyBinding": {
                      "version": 1,
                      "ownerKind": "durableState",
                      "providerKey": "durable-state.v1"
                    }
                  }
                }
                """
            : """
                {
                  "schemaVersion": "2.0.0",
                  "data": {
                    "conversationHistory": [],
                    "terminalResults": {
                      "correlation": {
                        "correlationId": "correlation",
                        "outcome": "succeeded",
                        "completedAt": "2026-09-10T05:00:00+00:00",
                        "response": { "messages": null }
                      }
                    },
                    "completionReceipts": {
                      "correlation": {
                        "correlationId": "correlation",
                        "outcome": "succeeded",
                        "completedAt": "2026-09-10T05:00:00+00:00",
                        "resultState": "available"
                      }
                    },
                    "historyBinding": {
                      "version": 1,
                      "ownerKind": "durableState",
                      "providerKey": "durable-state.v1"
                    }
                  }
                }
                """;

        Assert.Throws<InvalidOperationException>(() => Deserialize(json));
    }

    [Fact]
    public void TerminalMessageMayOmitContentsButCannotUseNullEntries()
    {
        const string MetadataOnlyJson = """
            {
              "schemaVersion": "2.0.0",
              "data": {
                "conversationHistory": [],
                "terminalResults": {
                  "metadata": {
                    "correlationId": "metadata",
                    "outcome": "succeeded",
                    "completedAt": "2026-09-10T05:00:00+00:00",
                    "response": { "messages": [{ "role": "assistant" }] }
                  }
                },
                "completionReceipts": {
                  "metadata": {
                    "correlationId": "metadata",
                    "outcome": "succeeded",
                    "completedAt": "2026-09-10T05:00:00+00:00",
                    "resultState": "available"
                  }
                },
                "historyBinding": {
                  "version": 1,
                  "ownerKind": "durableState",
                  "providerKey": "durable-state.v1"
                }
              }
            }
            """;
        const string NullMessageJson = """
            {
              "schemaVersion": "2.0.0",
              "data": {
                "conversationHistory": [],
                "terminalResults": {
                  "metadata": {
                    "correlationId": "metadata",
                    "outcome": "succeeded",
                    "completedAt": "2026-09-10T05:00:00+00:00",
                    "response": { "messages": [null] }
                  }
                },
                "completionReceipts": {
                  "metadata": {
                    "correlationId": "metadata",
                    "outcome": "succeeded",
                    "completedAt": "2026-09-10T05:00:00+00:00",
                    "resultState": "available"
                  }
                },
                "historyBinding": {
                  "version": 1,
                  "ownerKind": "durableState",
                  "providerKey": "durable-state.v1"
                }
              }
            }
            """;

        AgentResponse response = Assert.IsType<DurableAgentStateTerminalResponse>(
            Deserialize(MetadataOnlyJson).Data.TerminalResults?["metadata"].Response).ToResponse();
        Assert.Empty(Assert.Single(response.Messages).Contents);
        Assert.Throws<InvalidOperationException>(() => Deserialize(NullMessageJson));
    }

    [Fact]
    public void UnknownMailboxDiscriminatorIsRejected()
    {
        string json = CreateRevisedJson(
            resultOutcome: "futureOutcome",
            receiptOutcome: "futureOutcome",
            resultState: DurableAgentStateCompletionReceipt.AvailableResult);

        Assert.Throws<InvalidOperationException>(() => Deserialize(json));
    }

    [Fact]
    public void DuplicateCompletionCorrelationIsRejected()
    {
        const string Json = """
            {
              "schemaVersion": "2.0.0",
              "data": {
                "conversationHistory": [],
                "terminalResults": {},
                "completionReceipts": {
                  "duplicate": {
                    "correlationId": "duplicate",
                    "outcome": "succeeded",
                    "completedAt": "2026-09-10T05:00:00+00:00",
                    "resultState": "unavailable",
                    "resultUnavailableAt": "2026-09-10T05:00:01+00:00"
                  },
                  "duplicate": {
                    "correlationId": "duplicate",
                    "outcome": "succeeded",
                    "completedAt": "2026-09-10T05:00:00+00:00",
                    "resultState": "unavailable",
                    "resultUnavailableAt": "2026-09-10T05:00:01+00:00"
                  }
                },
                "historyBinding": {
                  "version": 1,
                  "ownerKind": "durableState",
                  "providerKey": "durable-state.v1"
                }
              }
            }
            """;

        Assert.Throws<InvalidOperationException>(() => Deserialize(Json));
    }

    [Theory]
    [InlineData("available", false)]
    [InlineData("unavailable", true)]
    public void ResultAndReceiptAvailabilityMustBeConsistent(string resultState, bool includeResult)
    {
        string json = CreateRevisedJson(
            resultOutcome: DurableAgentStateCompletionReceipt.SucceededOutcome,
            receiptOutcome: DurableAgentStateCompletionReceipt.SucceededOutcome,
            resultState,
            includeResult);

        Assert.Throws<InvalidOperationException>(() => Deserialize(json));
    }

    [Fact]
    public void ResultAndReceiptMetadataMustMatch()
    {
        string json = CreateRevisedJson(
            resultOutcome: DurableAgentStateCompletionReceipt.SucceededOutcome,
            receiptOutcome: DurableAgentStateCompletionReceipt.FailedOutcome,
            resultState: DurableAgentStateCompletionReceipt.AvailableResult);

        Assert.Throws<InvalidOperationException>(() => Deserialize(json));
    }

    [Fact]
    public void FailedTerminalResultWithMatchingReceiptIsValid()
    {
        const string Json = """
            {
              "schemaVersion": "2.0.0",
              "data": {
                "conversationHistory": [],
                "terminalResults": {
                  "failed": {
                    "correlationId": "failed",
                    "outcome": "failed",
                    "completedAt": "2026-09-10T05:00:00+00:00",
                    "response": {
                      "messages": [{
                        "role": "assistant",
                        "contents": [{
                          "$type": "error",
                          "message": "failed",
                          "errorCode": "Example"
                        }]
                      }]
                    },
                    "error": {
                      "code": "Example",
                      "message": "The operation failed."
                    }
                  }
                },
                "completionReceipts": {
                  "failed": {
                    "correlationId": "failed",
                    "outcome": "failed",
                    "completedAt": "2026-09-10T05:00:00+00:00",
                    "resultState": "available"
                  }
                },
                "historyBinding": {
                  "version": 1,
                  "ownerKind": "durableState",
                  "providerKey": "durable-state.v1"
                }
              }
            }
            """;

        DurableAgentState state = Deserialize(Json);

        Assert.Equal(
            "Example",
            state.Data.TerminalResults?["failed"].Error?.Code);
    }

    [Fact]
    public void TerminalErrorLengthCountsUnicodeScalars()
    {
        DurableAgentState state = CreateEmptyRevisedState(new()
        {
            OwnerKind = DurableAgentStateHistoryBinding.DurableStateOwner,
            ProviderKey = "durable-state.v1",
        });
        const string CorrelationId = "failed";
        DateTimeOffset completedAt = DateTimeOffset.Parse("2026-09-10T05:00:00+00:00");
        state.Data.TerminalResults![CorrelationId] = new()
        {
            CorrelationId = CorrelationId,
            Outcome = DurableAgentStateCompletionReceipt.FailedOutcome,
            CompletedAt = completedAt,
            Response = new(),
            Error = new()
            {
                Code = "Example",
                Message = string.Concat(Enumerable.Repeat("\U0001F600", 10_000)),
            },
        };
        state.Data.CompletionReceipts![CorrelationId] = new()
        {
            CorrelationId = CorrelationId,
            Outcome = DurableAgentStateCompletionReceipt.FailedOutcome,
            CompletedAt = completedAt,
            ResultState = DurableAgentStateCompletionReceipt.AvailableResult,
        };

        string json = Serialize(state);

        Assert.Equal(
            10_000,
            Deserialize(json).Data.TerminalResults![CorrelationId].Error!.Message.EnumerateRunes().Count());
    }

    [Fact]
    public void TerminalResponseMetadataRequiresValidKeys()
    {
        string json = File.ReadAllText(
            Path.Combine(AppContext.BaseDirectory, "Fixtures", "shared-durable-agent-state-2.0.json"))
            .Replace("\"region\": \"test\"", "\"\": \"test\"", StringComparison.Ordinal);

        Assert.Throws<InvalidOperationException>(() => Deserialize(json));
    }

    [Theory]
    [InlineData(2, DurableAgentStateHistoryBinding.DurableStateOwner, "durable-state.v1")]
    [InlineData(1, "futureOwner", "provider.v1")]
    [InlineData(1, DurableAgentStateHistoryBinding.HistoryProviderOwner, " ")]
    public void InvalidHistoryBindingIsRejected(int version, string ownerKind, string providerKey)
    {
        DurableAgentState state = CreateEmptyRevisedState(new()
        {
            Version = version,
            OwnerKind = ownerKind,
            ProviderKey = providerKey,
        });

        Assert.Throws<InvalidOperationException>(() => Serialize(state));
    }

    [Fact]
    public void HistoryBindingRequiresExplicitWireVersion()
    {
        string json = File.ReadAllText(
            Path.Combine(AppContext.BaseDirectory, "Fixtures", "shared-durable-agent-state-2.0.json"))
            .Replace("\"version\": 1,", string.Empty, StringComparison.Ordinal);

        Assert.Throws<JsonException>(() => Deserialize(json));
    }

    [Fact]
    public void TerminalResponsePreservesConsumerFieldsWithoutRuntimeObjects()
    {
        DateTimeOffset completedAt = DateTimeOffset.Parse("2026-09-10T05:00:03+00:00");
        ChatMessage message = new(
            ChatRole.Assistant,
            [
                new TextContent("done"),
                new UriContent("https://example.test/result.json", "application/json"),
            ])
        {
            MessageId = "message-id",
            AuthorName = "agent",
        };
        AgentResponse response = new([message])
        {
            CreatedAt = completedAt,
            ResponseId = "response-id",
            AgentId = "agent-id",
            FinishReason = new ChatFinishReason("stop"),
            ContinuationToken = ResponseContinuationToken.FromBytes(new byte[] { 1, 2, 3 }),
            Usage = new UsageDetails
            {
                InputTokenCount = 4,
                OutputTokenCount = 2,
                TotalTokenCount = 6,
            },
            AdditionalProperties = new()
            {
                ["region"] = "test",
                ["attempt"] = 2,
            },
            RawRepresentation = new object(),
        };

        DurableAgentStateTerminalResult stored = DurableAgentStateTerminalResult.FromResponse(
            "correlation",
            response,
            completedAt);
        AgentResponse restored = Assert.IsType<DurableAgentStateTerminalResponse>(stored.Response).ToResponse();

        Assert.Equal("response-id", restored.ResponseId);
        Assert.Equal("agent-id", restored.AgentId);
        Assert.Equal("stop", restored.FinishReason?.Value);
        Assert.Equal(completedAt, restored.CreatedAt);
        Assert.Equal([1, 2, 3], restored.ContinuationToken?.ToBytes().ToArray());
        Assert.Equal(6, restored.Usage?.TotalTokenCount);
        Assert.Equal("test", Assert.IsType<JsonElement>(restored.AdditionalProperties?["region"]).GetString());
        Assert.Equal(2, Assert.IsType<JsonElement>(restored.AdditionalProperties?["attempt"]).GetInt32());
        Assert.Null(restored.RawRepresentation);
        ChatMessage restoredMessage = Assert.Single(restored.Messages);
        Assert.Equal("message-id", restoredMessage.MessageId);
        Assert.Collection(
            restoredMessage.Contents,
            content => Assert.Equal("done", Assert.IsType<TextContent>(content).Text),
            content =>
            {
                UriContent uri = Assert.IsType<UriContent>(content);
                Assert.Equal("https://example.test/result.json", uri.Uri.ToString());
                Assert.Equal("application/json", uri.MediaType);
            });
    }

    [Fact]
    public void TerminalResponseRejectsArbitraryRuntimeMetadata()
    {
        AgentResponse response = new()
        {
            AdditionalProperties = new()
            {
                ["unsupported"] = new object(),
            },
        };

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => DurableAgentStateTerminalResult.FromResponse(
                "correlation",
                response,
                DateTimeOffset.Parse("2026-09-10T05:00:03+00:00")));

        Assert.Contains("unsupported runtime type", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TerminalResponseRejectsArbitraryMessageMetadata()
    {
        ChatMessage message = new(ChatRole.Assistant, "done")
        {
            AdditionalProperties = new()
            {
                ["unsupported"] = new object(),
            },
        };

        InvalidOperationException exception = Assert.Throws<InvalidOperationException>(
            () => DurableAgentStateTerminalResult.FromResponse(
                "correlation",
                new AgentResponse([message]),
                DateTimeOffset.Parse("2026-09-10T05:00:03+00:00")));

        Assert.Contains("unsupported runtime type", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TerminalResponseRejectsRuntimeBackedJsonNodeMetadata()
    {
        AgentResponse response = new()
        {
            AdditionalProperties = new()
            {
                ["unsupported"] = JsonValue.Create<object>(new Dictionary<string, int>
                {
                    ["runtimeValue"] = 42,
                }),
            },
        };

        Assert.Throws<InvalidOperationException>(
            () => DurableAgentStateTerminalResult.FromResponse(
                "correlation",
                response,
                DateTimeOffset.Parse("2026-09-10T05:00:03+00:00")));
    }

    [Fact]
    public void NonCanonicalContinuationTokenIsRejected()
    {
        string json = File.ReadAllText(
            Path.Combine(AppContext.BaseDirectory, "Fixtures", "shared-durable-agent-state-2.0.json"))
            .Replace("\"AQID\"", "\"AQ ID\"", StringComparison.Ordinal);

        Assert.Throws<InvalidOperationException>(() => Deserialize(json));
    }

    [Fact]
    public void TerminalResultClonesJsonElementContent()
    {
        DurableAgentStateTerminalResult result;
        using (JsonDocument document = JsonDocument.Parse("""{"value":1}"""))
        {
            ChatMessage message = new(
                ChatRole.Assistant,
                [new FunctionResultContent("call-1", document.RootElement)]);
            result = DurableAgentStateTerminalResult.FromResponse(
                "correlation",
                new AgentResponse([message]),
                DateTimeOffset.Parse("2026-09-10T05:00:03+00:00"));
        }

        AgentResponse restored = Assert.IsType<DurableAgentStateTerminalResponse>(result.Response).ToResponse();
        FunctionResultContent content =
            Assert.IsType<FunctionResultContent>(Assert.Single(Assert.Single(restored.Messages).Contents));
        Assert.Equal(1, Assert.IsType<JsonElement>(content.Result).GetProperty("value").GetInt32());
    }

    [Fact]
    public void TerminalResultIsDetachedFromTranscriptAndSourceResponse()
    {
        DateTimeOffset completedAt = DateTimeOffset.Parse("2026-09-10T05:00:03+00:00");
        ChatMessage sourceMessage = new(ChatRole.Assistant, "original");
        AgentResponse response = new([sourceMessage]);
        DurableAgentStateTerminalResult result = DurableAgentStateTerminalResult.FromResponse(
            "correlation",
            response,
            completedAt);
        DurableAgentStateResponse transcript = DurableAgentStateResponse.FromResponse("correlation", response);

        sourceMessage.Contents.Clear();
        transcript.Messages[0].MessageId = "transcript-mutated";

        DurableAgentStateMessage resultMessage =
            Assert.Single(Assert.IsType<DurableAgentStateTerminalResponse>(result.Response).Messages);
        Assert.Single(resultMessage.Contents);
        Assert.Equal("durable_result_correlation_0", resultMessage.MessageId);
    }

    [Fact]
    public void VersionOneStateCannotWriteRevisedFields()
    {
        DurableAgentState state = new()
        {
            Data = new()
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
            },
        };

        Assert.Throws<InvalidOperationException>(() => Serialize(state));
    }

    [Fact]
    public void IdentifierLengthCountsUnicodeScalars()
    {
        string providerKey = string.Concat(Enumerable.Repeat("\U0001F600", 200));
        DurableAgentState state = CreateEmptyRevisedState(new()
        {
            OwnerKind = DurableAgentStateHistoryBinding.HistoryProviderOwner,
            ProviderKey = providerKey,
        });

        string json = Serialize(state);
        DurableAgentState restored = Deserialize(json);

        Assert.Equal(providerKey, restored.Data.HistoryBinding?.ProviderKey);
    }

    private static DurableAgentState CreateEmptyRevisedState(DurableAgentStateHistoryBinding binding)
    {
        return new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            Data = new()
            {
                ConversationHistory = [],
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(),
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(),
                HistoryBinding = binding,
            },
        };
    }

    private static string CreateRevisedJson(
        string resultOutcome,
        string receiptOutcome,
        string resultState,
        bool includeResult = true)
    {
        string result = includeResult
            ? $$"""
                "correlation": {
                  "correlationId": "correlation",
                  "outcome": "{{resultOutcome}}",
                  "completedAt": "2026-09-10T05:00:00+00:00",
                  "response": { "messages": [] }
                }
                """
            : string.Empty;
        string unavailableAt = resultState == DurableAgentStateCompletionReceipt.UnavailableResult
            ? """, "resultUnavailableAt": "2026-09-10T05:00:01+00:00" """
            : string.Empty;

        return $$"""
            {
              "schemaVersion": "2.0.0",
              "data": {
                "conversationHistory": [],
                "terminalResults": { {{result}} },
                "completionReceipts": {
                  "correlation": {
                    "correlationId": "correlation",
                    "outcome": "{{receiptOutcome}}",
                    "completedAt": "2026-09-10T05:00:00+00:00",
                    "resultState": "{{resultState}}"{{unavailableAt}}
                  }
                },
                "historyBinding": {
                  "version": 1,
                  "ownerKind": "durableState",
                  "providerKey": "durable-state.v1"
                }
              }
            }
            """;
    }

    private static DurableAgentState Deserialize(string json) =>
        Assert.IsType<DurableAgentState>(
            JsonSerializer.Deserialize(json, DurableAgentStateJsonContext.Default.DurableAgentState));

    private static string Serialize(DurableAgentState state) =>
        JsonSerializer.Serialize(state, DurableAgentStateJsonContext.Default.DurableAgentState);
}
