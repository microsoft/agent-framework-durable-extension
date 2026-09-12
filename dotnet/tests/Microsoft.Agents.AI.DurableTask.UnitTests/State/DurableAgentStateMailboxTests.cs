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
    public void VersionedEnvelopeCasesMatchDotNetReaders()
    {
        string json = File.ReadAllText(
            Path.Combine(AppContext.BaseDirectory, "Fixtures", "versioned-envelope-cases.json"));
        using JsonDocument document = JsonDocument.Parse(json);
        int caseCount = 0;
        int schemaOnlyLegacyCases = 0;

        foreach (JsonElement group in document.RootElement.EnumerateArray())
        {
            foreach (JsonElement test in group.GetProperty("tests").EnumerateArray())
            {
                string stateJson = test.GetProperty("data").GetRawText();
                bool valid = test.GetProperty("valid").GetBoolean();
                if (valid)
                {
                    JsonElement data = test.GetProperty("data");
                    if (IsSchemaOnlyLegacyEntryCase(data))
                    {
                        // Historical schema snapshots allowed an undiscriminated generic entry.
                        // The existing .NET model has always required a typed entry discriminator;
                        // this implementation must not invent one while round-tripping old data.
                        schemaOnlyLegacyCases++;
                    }
                    else
                    {
                        DurableAgentState state = Deserialize(stateJson);
                        _ = Serialize(state);
                    }
                }
                else
                {
                    Assert.ThrowsAny<Exception>(() => Deserialize(stateJson));
                }

                caseCount++;
            }
        }

        Assert.Equal(44, caseCount);
        Assert.Equal(3, schemaOnlyLegacyCases);
    }

    [Theory]
    [InlineData("""{"role":"developer","contents":[]}""")]
    [InlineData("""{"role":"assistant","contents":[{"$type":"functionCall","callId":"c","name":"f","arguments":"verbatim"}]}""")]
    [InlineData("""{"role":"assistant","contents":[{"$type":"uri","uri":"https://example.test/media"}]}""")]
    public void LegacySnapshotsRejectV2OnlyMessageShapes(string messageJson)
    {
        string json = $$"""
            {
              "schemaVersion": "1.2.0",
              "data": {
                "conversationHistory": [{
                  "$type": "request",
                  "messages": [{{messageJson}}]
                }]
              }
            }
            """;

        Assert.Throws<InvalidOperationException>(() => Deserialize(json));
    }

    [Theory]
    [InlineData("1.0.0")]
    [InlineData("1.1.0")]
    [InlineData("1.2.0")]
    [InlineData("2.0.0")]
    public void ProductionWriterEnforcesVersionedRequestAndResponseShapes(string schemaVersion)
    {
        string[] messageShapes =
        [
            """{"role":"developer","contents":[]}""",
            """{"role":"assistant","contents":[{"$type":"functionCall","callId":"c","name":"f","arguments":"verbatim"}]}""",
            """{"role":"assistant","contents":[{"$type":"uri","uri":"https://example.test/media"}]}""",
        ];
        foreach (string messageJson in messageShapes)
        {
            DurableAgentStateMessage message = JsonSerializer.Deserialize(
                messageJson, DurableAgentStateJsonContext.Default.DurableAgentStateMessage)!;
            foreach (bool response in new[] { false, true })
            {
                bool revised = schemaVersion == DurableAgentState.RevisedSchemaVersion;
                DurableAgentState state = new()
                {
                    SchemaVersion = schemaVersion,
                    MailboxWritesAuthorized = revised,
                    Data = new()
                    {
                        ConversationHistory =
                        [
                            response
                                ? new DurableAgentStateResponse { Messages = [message] }
                                : new DurableAgentStateRequest { Messages = [message] },
                        ],
                        TerminalResults = revised ? new Dictionary<string, DurableAgentStateTerminalResult>() : null,
                        CompletionReceipts = revised ? new Dictionary<string, DurableAgentStateCompletionReceipt>() : null,
                    },
                };

                if (revised)
                {
                    string json = JsonSerializer.Serialize(state, DurableAgentStateJsonContext.Default.DurableAgentState);
                    DurableAgentState restored = JsonSerializer.Deserialize(json, DurableAgentStateJsonContext.Default.DurableAgentState)!;
                    JsonElement roundTrip = JsonSerializer.SerializeToElement(
                        Assert.Single(Assert.Single(restored.Data.ConversationHistory).Messages),
                        DurableAgentStateJsonContext.Default.DurableAgentStateMessage);
                    using JsonDocument expected = JsonDocument.Parse(messageJson);
                    Assert.True(JsonElement.DeepEquals(expected.RootElement, roundTrip));
                }
                else
                {
                    Assert.Throws<InvalidOperationException>(() =>
                        JsonSerializer.Serialize(state, DurableAgentStateJsonContext.Default.DurableAgentState));
                    Assert.Null(state.Data.CompletionReceipts);
                }
            }
        }
    }

    [Theory]
    [InlineData("null")]
    [InlineData("[null]")]
    public void LegacySnapshotsRejectMalformedConversationHistory(string historyJson)
    {
        string json = $$"""
            {
              "schemaVersion": "1.2.0",
              "data": {
                "conversationHistory": {{historyJson}}
              }
            }
            """;

        Assert.ThrowsAny<Exception>(() => Deserialize(json));
    }

    [Theory]
    [InlineData("""{"$type":"request","messages":null}""")]
    [InlineData("""{"$type":"request","messages":[null]}""")]
    [InlineData("""{"$type":"request","messages":[{"role":null}]}""")]
    [InlineData("""{"$type":"request","messages":[{"role":"user","contents":null}]}""")]
    [InlineData("""{"$type":"request","messages":[{"role":"user","contents":[null]}]}""")]
    [InlineData("""{"$type":"request","messages":[{"role":"user","contents":[{"$type":"text","text":null}]}]}""")]
    [InlineData("""{"$type":"request","messages":[{"role":"user","contents":[{"$type":"functionCall","callId":null,"name":"f"}]}]}""")]
    [InlineData("""{"$type":"request","messages":[{"role":"user","contents":[{"$type":"uri","uri":null,"mediaType":"text/plain"}]}]}""")]
    [InlineData("""{"$type":"request","messages":[{"role":"user","contents":[{"$type":"usage","usage":null}]}]}""")]
    public void LegacySnapshotsRejectMalformedEntryShapes(string entryJson)
    {
        string json = $$"""
            {
              "schemaVersion": "1.2.0",
              "data": {
                "conversationHistory": [{{entryJson}}]
              }
            }
            """;

        Assert.ThrowsAny<Exception>(() => Deserialize(json));
    }

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
    public void ProductionReaderSupportsRevisedStateButPassiveDtosDoNotActivateNewWrites()
    {
        const string Json = """
            {
              "schemaVersion": "2.0.0",
              "data": {
                "conversationHistory": [],
                "terminalResults": {},
                "completionReceipts": {}
              }
            }
            """;
        DurableAgentState state = Deserialize(Json);

        DurableAgentState hydrated = Assert.IsType<DurableAgentState>(
            JsonSerializer.Deserialize(Json, DurableAgentStateJsonContext.Default.DurableAgentState));
        Assert.Equal(DurableAgentState.RevisedSchemaVersion, hydrated.SchemaVersion);
        Assert.Contains("\"schemaVersion\":\"2.0.0\"",
            JsonSerializer.Serialize(hydrated, DurableAgentStateJsonContext.Default.DurableAgentState),
            StringComparison.Ordinal);
        Assert.Throws<InvalidOperationException>(
            () => JsonSerializer.Serialize(
                state,
                DurableAgentStateJsonContext.Default.DurableAgentState));
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
        Assert.Equal(
            "contoso.support-history.v1",
            state.Data.HistoryBinding.GetProperty("providerKey").GetString());
        Assert.Equal("response-id-2", result.Response?.ResponseId);
        Assert.Equal(DurableAgentStateCompletionReceipt.UnavailableResult, unavailable.ResultState);
        Assert.Contains("\"futureResponseField\":{\"preserve\":true}", roundTrip, StringComparison.Ordinal);
        Assert.Contains("\"futureReceiptField\":7", roundTrip, StringComparison.Ordinal);
        Assert.Contains("\"futureBindingField\":\"preserve\"", roundTrip, StringComparison.Ordinal);
        Assert.Contains("\"futureRootField\":{\"preserve\":true}", roundTrip, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("terminalResults")]
    [InlineData("completionReceipts")]
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
                ownerKind = "durableState",
                providerKey = "durable-state.v1",
            },
        };
        _ = data.Remove(missingProperty);
        string json = JsonSerializer.Serialize(new
        {
            schemaVersion = DurableAgentState.RevisedSchemaVersion,
            data,
        });

        Assert.ThrowsAny<Exception>(() => Deserialize(json));
    }

    [Fact]
    public void RevisedStateAllowsOmittedHistoryBinding()
    {
        const string Json = """
            {
              "schemaVersion": "2.0.0",
              "data": {
                "conversationHistory": [],
                "terminalResults": {},
                "completionReceipts": {}
              }
            }
            """;

        DurableAgentState state = Deserialize(Json);

        Assert.Equal(JsonValueKind.Undefined, state.Data.HistoryBinding.ValueKind);
    }

    [Theory]
    [InlineData("request", "")]
    [InlineData("response", "   ")]
    [InlineData("errorResponse", "id\u0001")]
    public void RevisedTranscriptRejectsInvalidPresentCorrelation(string entryType, string correlationId)
    {
        string json = $$"""
            {
              "schemaVersion": "2.0.0",
              "data": {
                "conversationHistory": [{
                  "$type": "{{entryType}}",
                  "correlationId": {{JsonSerializer.Serialize(correlationId)}}
                }],
                "terminalResults": {},
                "completionReceipts": {}
              }
            }
            """;

        Assert.ThrowsAny<Exception>(() => Deserialize(json));
    }

    [Fact]
    public void RevisedTranscriptAllowsMissingCorrelationButCompactionForbidsIt()
    {
        const string MissingCorrelation = """
            {
              "schemaVersion": "2.0.0",
              "data": {
                "conversationHistory": [{ "$type": "request" }],
                "terminalResults": {},
                "completionReceipts": {}
              }
            }
            """;
        const string CompactionCorrelation = """
            {
              "schemaVersion": "2.0.0",
              "data": {
                "conversationHistory": [{
                  "$type": "compaction",
                  "correlationId": "not-allowed"
                }],
                "terminalResults": {},
                "completionReceipts": {}
              }
            }
            """;

        Assert.IsType<DurableAgentStateRequest>(
            Assert.Single(Deserialize(MissingCorrelation).Data.ConversationHistory));
        Assert.Throws<InvalidOperationException>(() => Deserialize(CompactionCorrelation));
    }

    [Fact]
    public void TranscriptEntryPreservesAbsentCreatedAt()
    {
        const string Json = """
            {
              "schemaVersion": "2.0.0",
              "data": {
                "conversationHistory": [{ "$type": "request" }],
                "terminalResults": {},
                "completionReceipts": {}
              }
            }
            """;

        string roundTrip = Serialize(Deserialize(Json));
        using JsonDocument document = JsonDocument.Parse(roundTrip);
        JsonElement entry = document.RootElement.GetProperty("data").GetProperty("conversationHistory")[0];

        Assert.False(entry.TryGetProperty("createdAt", out _));
    }

    [Fact]
    public void EntryPreservesFieldsOwnedByAnotherVariantAsUnknown()
    {
        const string Json = """
            {
              "schemaVersion": "2.0.0",
              "data": {
                "conversationHistory": [{
                  "$type": "request",
                  "usage": {
                    "extensionData": null
                  }
                }, {
                  "$type": "response",
                  "responseSchema": "opaque"
                }],
                "terminalResults": {},
                "completionReceipts": {}
              }
            }
            """;

        string roundTrip = Serialize(Deserialize(Json));

        Assert.Contains("\"usage\":{\"extensionData\":null}", roundTrip, StringComparison.Ordinal);
        Assert.Contains("\"responseSchema\":\"opaque\"", roundTrip, StringComparison.Ordinal);
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

        Assert.ThrowsAny<Exception>(() => Deserialize(json));
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
    public void ExplicitResultRemovalMayPrecedeScheduledExpiry()
    {
        const string Json = """
            {
              "schemaVersion": "2.0.0",
              "data": {
                "conversationHistory": [],
                "terminalResults": {},
                "completionReceipts": {
                  "removed": {
                    "correlationId": "removed",
                    "outcome": "succeeded",
                    "completedAt": "2026-09-11T10:00:00Z",
                    "resultState": "unavailable",
                    "resultExpiresAt": "2026-09-12T10:00:00Z",
                    "resultUnavailableAt": "2026-09-11T11:00:00Z"
                  }
                }
              }
            }
            """;

        DurableAgentState state = Deserialize(Json);

        Assert.Equal(
            DateTimeOffset.Parse("2026-09-11T11:00:00Z"),
            state.Data.CompletionReceipts?["removed"].ResultUnavailableAt);
    }

    [Fact]
    public void TerminalErrorLengthCountsUnicodeScalars()
    {
        DurableAgentState state = CreateEmptyRevisedState();
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
    public void TerminalErrorMessageAllowsContractValidControlCharacters()
    {
        DurableAgentStateTerminalError error = new()
        {
            Code = "Example",
            Message = "line\u0001break",
        };

        error.Validate();

        Assert.Contains('\u0001', error.Message);
    }

    [Fact]
    public void TerminalResponseMetadataRequiresValidKeys()
    {
        string json = File.ReadAllText(
            Path.Combine(AppContext.BaseDirectory, "Fixtures", "shared-durable-agent-state-2.0.json"))
            .Replace("\"region\": \"test\"", "\"\": \"test\"", StringComparison.Ordinal);

        Assert.Throws<InvalidOperationException>(() => Deserialize(json));
    }

    [Fact]
    public void HistoryBindingIsPreservedAsOpaqueRuntimeProfile()
    {
        const string Json = """
            {
              "schemaVersion": "2.0.0",
              "data": {
                "conversationHistory": [],
                "terminalResults": {},
                "completionReceipts": {},
                "historyBinding": {
                  "runtime": "csharp",
                  "version": -1,
                  "ownerKind": null,
                  "nested": {
                    "$runtimeType": "inert"
                  }
                }
              }
            }
            """;

        string roundTrip = Serialize(Deserialize(Json));

        Assert.Contains("\"version\":-1", roundTrip, StringComparison.Ordinal);
        Assert.Contains("\"ownerKind\":null", roundTrip, StringComparison.Ordinal);
        Assert.Contains("\"$runtimeType\":\"inert\"", roundTrip, StringComparison.Ordinal);
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

    [Theory]
    [InlineData("null", JsonValueKind.Null)]
    [InlineData("false", JsonValueKind.False)]
    [InlineData("0", JsonValueKind.Number)]
    [InlineData("\"\"", JsonValueKind.String)]
    [InlineData("[]", JsonValueKind.Array)]
    [InlineData("{}", JsonValueKind.Object)]
    public void TerminalResponsePreservesPresentStructuredValue(string valueJson, JsonValueKind expectedKind)
    {
        using JsonDocument valueDocument = JsonDocument.Parse(valueJson);
        DurableAgentStateTerminalResult stored = DurableAgentStateTerminalResult.FromResponse(
            "correlation",
            new AgentResponse(),
            DateTimeOffset.Parse("2026-09-11T10:00:00+00:00"),
            structuredValue: valueDocument.RootElement);

        string json = JsonSerializer.Serialize(
            stored,
            DurableAgentStateJsonContext.Default.DurableAgentStateTerminalResult);
        DurableAgentStateTerminalResult restored = Assert.IsType<DurableAgentStateTerminalResult>(
            JsonSerializer.Deserialize(
                json,
                DurableAgentStateJsonContext.Default.DurableAgentStateTerminalResult));

        Assert.Contains("\"value\":", json, StringComparison.Ordinal);
        Assert.Equal(expectedKind, Assert.IsType<DurableAgentStateTerminalResponse>(restored.Response).Value.ValueKind);
    }

    [Fact]
    public void TerminalResponsePreservesAbsentStructuredValue()
    {
        DurableAgentStateTerminalResult stored = DurableAgentStateTerminalResult.FromResponse(
            "correlation",
            new AgentResponse(),
            DateTimeOffset.Parse("2026-09-11T10:00:00+00:00"));

        string json = JsonSerializer.Serialize(
            stored,
            DurableAgentStateJsonContext.Default.DurableAgentStateTerminalResult);

        Assert.DoesNotContain("\"value\"", json, StringComparison.Ordinal);
        Assert.Equal(
            JsonValueKind.Undefined,
            Assert.IsType<DurableAgentStateTerminalResponse>(stored.Response).Value.ValueKind);
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

    [Theory]
    [InlineData("terminalResults")]
    [InlineData("completionReceipts")]
    [InlineData("historyBinding")]
    public void LegacyStateRejectsPresentNullRevisedFields(string propertyName)
    {
        string json = $$"""
            {
              "schemaVersion": "1.2.0",
              "data": {
                "conversationHistory": [],
                "{{propertyName}}": null
              }
            }
            """;

        Assert.Throws<InvalidOperationException>(() => Deserialize(json));
    }

    [Fact]
    public void RevisedStatePreservesNullHistoryProfileWhenPresent()
    {
        const string Json = """
            {
              "schemaVersion": "2.0.0",
              "data": {
                "conversationHistory": [],
                "terminalResults": {},
                "completionReceipts": {},
                "historyBinding": null
              }
            }
            """;

        string roundTrip = Serialize(Deserialize(Json));

        Assert.Contains("\"historyBinding\":null", roundTrip, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("""{"schemaVersion":"1.2.0","extensionData":null,"data":{"conversationHistory":[]}}""")]
    [InlineData("""{"schemaVersion":"1.2.0","data":{"conversationHistory":[],"extensionData":null}}""")]
    [InlineData("""{"schemaVersion":"2.0.0","data":{"conversationHistory":[],"terminalResults":{"c":{"correlationId":"c","outcome":"succeeded","completedAt":"2026-09-11T10:00:00Z","resultExpiresAt":null,"response":{"messages":[]}}},"completionReceipts":{"c":{"correlationId":"c","outcome":"succeeded","completedAt":"2026-09-11T10:00:00Z","resultState":"available"}}}}""")]
    [InlineData("""{"schemaVersion":"2.0.0","data":{"conversationHistory":[],"terminalResults":{"c":{"correlationId":"c","outcome":"succeeded","completedAt":"2026-09-11T10:00:00Z","response":{"messages":[],"extensionData":null}}},"completionReceipts":{"c":{"correlationId":"c","outcome":"succeeded","completedAt":"2026-09-11T10:00:00Z","resultState":"available"}}}}""")]
    [InlineData("""{"schemaVersion":"2.0.0","data":{"conversationHistory":[{"$type":"request","responseSchema":null}],"terminalResults":{},"completionReceipts":{}}}""")]
    [InlineData("""{"schemaVersion":"2.0.0","data":{"conversationHistory":[{"$type":"request","messages":[{"role":"user","createdAt":null}]}],"terminalResults":{},"completionReceipts":{}}}""")]
    [InlineData("""{"schemaVersion":"2.0.0","data":{"conversationHistory":[],"terminalResults":{"c":{"correlationId":"c","outcome":"succeeded","completedAt":"2026-09-11T10:00:00Z","response":{"messages":[],"usage":null}}},"completionReceipts":{"c":{"correlationId":"c","outcome":"succeeded","completedAt":"2026-09-11T10:00:00Z","resultState":"available"}}}}""")]
    [InlineData("""{"schemaVersion":"2.0.0","data":{"conversationHistory":[],"terminalResults":{"c":{"correlationId":"c","outcome":"succeeded","completedAt":"2026-09-11T10:00:00Z","response":{"messages":[],"usage":{"inputTokenCount":null}}}},"completionReceipts":{"c":{"correlationId":"c","outcome":"succeeded","completedAt":"2026-09-11T10:00:00Z","resultState":"available"}}}}""")]
    public void ExplicitNullKnownFieldsAreRejected(string json)
    {
        Assert.Throws<JsonException>(() => Deserialize(json));
    }

    [Fact]
    public void TerminalErrorDetailsPreservesAbsentAndExplicitNull()
    {
        const string ExplicitNull = """
            {
              "code": "Example",
              "message": "failed",
              "details": null
            }
            """;

        DurableAgentStateTerminalError present = Assert.IsType<DurableAgentStateTerminalError>(
            JsonSerializer.Deserialize(
                ExplicitNull,
                DurableAgentStateJsonContext.Default.DurableAgentStateTerminalError));
        DurableAgentStateTerminalError absent = new()
        {
            Code = "Example",
            Message = "failed",
        };
        string presentJson = JsonSerializer.Serialize(
            present,
            DurableAgentStateJsonContext.Default.DurableAgentStateTerminalError);
        string absentJson = JsonSerializer.Serialize(
            absent,
            DurableAgentStateJsonContext.Default.DurableAgentStateTerminalError);

        Assert.Equal(JsonValueKind.Null, present.Details.ValueKind);
        Assert.Contains("\"details\":null", presentJson, StringComparison.Ordinal);
        Assert.DoesNotContain("\"details\"", absentJson, StringComparison.Ordinal);
    }

    [Fact]
    public void IngestionPositionsMustBeNonNegative()
    {
        const string Json = """
            {
              "schemaVersion": "1.2.0",
              "data": {
                "conversationHistory": [],
                "ingestedPositions": {
                  "producer": -1
                }
              }
            }
            """;

        Assert.Throws<InvalidOperationException>(() => Deserialize(Json));
    }

    [Fact]
    public void TruncationRequiresCompleteValidEvidence()
    {
        const string MissingFields = """
            {
              "schemaVersion": "1.2.0",
              "data": {
                "conversationHistory": [],
                "truncation": {}
              }
            }
            """;
        DurableAgentState invalidState = new()
        {
            Data = new()
            {
                Truncation = new()
                {
                    EvictedMessageCount = 1,
                    FirstEvictedAt = DateTimeOffset.Parse("2026-09-11T11:00:00+00:00"),
                    LastEvictedAt = DateTimeOffset.Parse("2026-09-11T10:00:00+00:00"),
                },
            },
        };

        Assert.Throws<InvalidOperationException>(() => Deserialize(MissingFields));
        Assert.Throws<InvalidOperationException>(() => Serialize(invalidState));
    }

    [Fact]
    public void TruncationUnknownEvidenceRoundTrips()
    {
        const string Json = """
            {
              "schemaVersion": "1.2.0",
              "data": {
                "conversationHistory": [],
                "truncation": {
                  "evictedMessageCount": 2,
                  "firstEvictedAt": "2026-09-11T10:00:00Z",
                  "lastEvictedAt": "2026-09-11T11:00:00Z",
                  "futureEvidence": 42
                }
              }
            }
            """;

        string roundTrip = Serialize(Deserialize(Json));

        Assert.Contains("\"futureEvidence\":42", roundTrip, StringComparison.Ordinal);
    }

    [Fact]
    public void MailboxCrossMapComparisonIsAlwaysOrdinal()
    {
        DateTimeOffset completedAt = DateTimeOffset.Parse("2026-09-11T10:00:00Z");
        DurableAgentState state = new()
        {
            SchemaVersion = DurableAgentState.RevisedSchemaVersion,
            Data = new()
            {
                TerminalResults = new Dictionary<string, DurableAgentStateTerminalResult>(
                    StringComparer.OrdinalIgnoreCase)
                {
                    ["Case-ID"] = new()
                    {
                        CorrelationId = "Case-ID",
                        Outcome = DurableAgentStateCompletionReceipt.SucceededOutcome,
                        CompletedAt = completedAt,
                        Response = new(),
                    },
                },
                CompletionReceipts = new Dictionary<string, DurableAgentStateCompletionReceipt>(
                    StringComparer.OrdinalIgnoreCase)
                {
                    ["case-id"] = new()
                    {
                        CorrelationId = "case-id",
                        Outcome = DurableAgentStateCompletionReceipt.SucceededOutcome,
                        CompletedAt = completedAt,
                        ResultState = DurableAgentStateCompletionReceipt.AvailableResult,
                    },
                },
            },
        };

        Assert.Throws<InvalidOperationException>(() => Serialize(state));
    }

    [Theory]
    [InlineData(
        "\"completedAt\": \"2026-09-10T05:00:03+00:00\"",
        "\"completedAt\": \"2026-09-10T05:00:03\"")]
    [InlineData(
        "\"resultExpiresAt\": \"2026-09-11T05:00:03+00:00\"",
        "\"resultExpiresAt\": \"2026-09-11T05:00:03\"")]
    [InlineData(
        "\"resultUnavailableAt\": \"2026-09-10T05:00:04+00:00\"",
        "\"resultUnavailableAt\": \"2026-09-10T05:00:04\"")]
    public void RevisedMailboxRequiresOffsetBearingRfc3339Timestamps(
        string original,
        string invalid)
    {
        string json = File.ReadAllText(
            Path.Combine(AppContext.BaseDirectory, "Fixtures", "shared-durable-agent-state-2.0.json"))
            .Replace(original, invalid, StringComparison.Ordinal);

        Assert.Throws<JsonException>(() => Deserialize(json));
    }

    [Fact]
    public void UnavailableReceiptIsTimestampValidatedWithoutTerminalResults()
    {
        const string Json = """
            {
              "schemaVersion": "2.0.0",
              "data": {
                "conversationHistory": [],
                "terminalResults": {},
                "completionReceipts": {
                  "c": {
                    "correlationId": "c",
                    "outcome": "succeeded",
                    "completedAt": "2026-09-11T10:00:00Z",
                    "resultState": "unavailable",
                    "resultUnavailableAt": "2026-09-11T11:00:00"
                  }
                }
              }
            }
            """;

        Assert.Throws<JsonException>(() => Deserialize(Json));
    }

    [Fact]
    public void LegacyTruncationRequiresOffsetAndSeconds()
    {
        const string Json = """
            {
              "schemaVersion": "1.2.0",
              "data": {
                "conversationHistory": [],
                "truncation": {
                  "evictedMessageCount": 1,
                  "firstEvictedAt": "2026-09-11T10:00Z",
                  "lastEvictedAt": "2026-09-11T11:00:00Z"
                }
              }
            }
            """;

        Assert.Throws<JsonException>(() => Deserialize(Json));
    }

    [Theory]
    [InlineData("""{"$type":"text","text":null}""")]
    [InlineData("""{"$type":"functionCall","callId":"c","name":null}""")]
    [InlineData("""{"$type":"uri","uri":"https://example.test","mediaType":null}""")]
    [InlineData("""{"$type":"usage","usage":{"inputTokenCount":null}}""")]
    [InlineData("""{"$type":"usage","usage":{"extensionData":null}}""")]
    public void RevisedStateRejectsMalformedKnownContent(string contentJson)
    {
        string json = $$"""
            {
              "schemaVersion": "2.0.0",
              "data": {
                "conversationHistory": [{
                  "$type": "request",
                  "messages": [{
                    "role": "user",
                    "contents": [{{contentJson}}]
                  }]
                }],
                "terminalResults": {},
                "completionReceipts": {}
              }
            }
            """;

        Assert.ThrowsAny<Exception>(() => Deserialize(json));
    }

    [Theory]
    [InlineData("inputTokenCount")]
    [InlineData("outputTokenCount")]
    [InlineData("totalTokenCount")]
    [InlineData("extensionData")]
    public void LegacyUsageContentRejectsExplicitNullKnownFields(string propertyName)
    {
        string json = $$"""
            {
              "schemaVersion": "1.2.0",
              "data": {
                "conversationHistory": [{
                  "$type": "request",
                  "messages": [{
                    "role": "user",
                    "contents": [{
                      "$type": "usage",
                      "usage": {
                        "{{propertyName}}": null
                      }
                    }]
                  }]
                }]
              }
            }
            """;

        Assert.ThrowsAny<Exception>(() => Deserialize(json));
    }

    [Theory]
    [InlineData("\"authorName\":null")]
    [InlineData("\"messageId\":null")]
    [InlineData("\"createdAt\":null")]
    [InlineData("\"extensionData\":null")]
    public void TerminalMessagesRejectExplicitNullKnownFields(string property)
    {
        string json = $$"""
            {
              "schemaVersion": "2.0.0",
              "data": {
                "conversationHistory": [],
                "terminalResults": {
                  "c": {
                    "correlationId": "c",
                    "outcome": "succeeded",
                    "completedAt": "2026-09-12T00:00:00Z",
                    "response": {
                      "messages": [{
                        "role": "assistant",
                        {{property}}
                      }]
                    }
                  }
                },
                "completionReceipts": {
                  "c": {
                    "correlationId": "c",
                    "outcome": "succeeded",
                    "completedAt": "2026-09-12T00:00:00Z",
                    "resultState": "available"
                  }
                }
              }
            }
            """;

        Assert.ThrowsAny<Exception>(() => Deserialize(json));
    }

    [Fact]
    public void IdentifierLengthCountsUnicodeScalars()
    {
        string providerKey = string.Concat(Enumerable.Repeat("\U0001F600", 200));
        DurableAgentState state = CreateEmptyRevisedState(
            JsonSerializer.SerializeToElement(new
            {
                ownerKind = "historyProvider",
                providerKey,
            }));

        string json = Serialize(state);
        DurableAgentState restored = Deserialize(json);

        Assert.Equal(providerKey, restored.Data.HistoryBinding.GetProperty("providerKey").GetString());
    }

    [Fact]
    public void LosslessFixturePreservesDeveloperRoleArgumentsUriOpaqueContentAndValue()
    {
        string json = File.ReadAllText(
            Path.Combine(AppContext.BaseDirectory, "Fixtures", "shared-durable-agent-state-2.0-lossless.json"));

        DurableAgentState state = Deserialize(json);
        string roundTrip = Serialize(state);
        DurableAgentStateRequest request =
            Assert.IsType<DurableAgentStateRequest>(Assert.Single(state.Data.ConversationHistory));
        Assert.Equal("developer", Assert.Single(request.Messages).Role);

        DurableAgentStateTerminalResponse response = Assert.IsType<DurableAgentStateTerminalResponse>(
            state.Data.TerminalResults?["corr-lossless"].Response);
        DurableAgentStateMessage message = Assert.Single(response.Messages);
        DurableAgentStateFunctionCallContent functionCall =
            Assert.IsType<DurableAgentStateFunctionCallContent>(message.Contents[0]);
        DurableAgentStateUriContent uri = Assert.IsType<DurableAgentStateUriContent>(message.Contents[1]);
        DurableAgentStateUnknownContent unknown =
            Assert.IsType<DurableAgentStateUnknownContent>(message.Contents[2]);

        Assert.Equal(" { \"partial\": ", functionCall.Arguments.GetString());
        Assert.Null(uri.MediaType);
        Assert.Equal("opaque-data-only", unknown.Content.GetProperty("$runtimeType").GetString());
        Assert.Equal(JsonValueKind.False, response.Value.ValueKind);
        FunctionCallContent runtimeFunctionCall =
            Assert.IsType<FunctionCallContent>(functionCall.ToAIContent());
        Assert.Equal(" { \"partial\": ", runtimeFunctionCall.RawRepresentation);
        Assert.Throws<InvalidOperationException>(() => uri.ToAIContent());
        using JsonDocument roundTripDocument = JsonDocument.Parse(roundTrip);
        Assert.Equal(
            " { \"partial\": ",
            roundTripDocument.RootElement.GetProperty("data")
                .GetProperty("terminalResults")
                .GetProperty("corr-lossless")
                .GetProperty("response")
                .GetProperty("messages")[0]
                .GetProperty("contents")[0]
                .GetProperty("arguments")
                .GetString());
        Assert.DoesNotContain("\"mediaType\"", JsonSerializer.Serialize(
            uri,
            DurableAgentStateJsonContext.Default.DurableAgentStateUriContent), StringComparison.Ordinal);
    }

    [Fact]
    public void PrunedFixturePreservesExpiredOutcomeOpaqueSessionAndHighestSeenPosition()
    {
        string json = File.ReadAllText(
            Path.Combine(AppContext.BaseDirectory, "Fixtures", "shared-durable-agent-state-2.0-pruned.json"));

        DurableAgentState state = Deserialize(json);
        DurableAgentStateCompletionReceipt receipt =
            Assert.IsType<DurableAgentStateCompletionReceipt>(state.Data.CompletionReceipts?["corr-pruned"]);

        Assert.Equal(DurableAgentStateCompletionReceipt.SucceededOutcome, receipt.Outcome);
        Assert.Equal(DurableAgentStateCompletionReceipt.UnavailableResult, receipt.ResultState);
        Assert.False(state.Data.TerminalResults?.ContainsKey("corr-pruned"));
        Assert.Equal(3, state.Data.IngestedPositions?["example-producer"]);
        Assert.Equal(
            "opaque-user-data",
            state.Data.Session?.GetProperty("exampleContinuation").GetProperty("$runtimeType").GetString());
        Assert.Equal(4, state.Data.Truncation?.EvictedMessageCount);
    }

    [Theory]
    [InlineData("null", JsonValueKind.Null)]
    [InlineData("\"verbatim\"", JsonValueKind.String)]
    [InlineData("[0,false,null]", JsonValueKind.Array)]
    public void ExplicitOpaqueJsonContentRoundTripsLosslessly(string contentJson, JsonValueKind expectedKind)
    {
        string json = $$"""
            {
              "$type": "unknown",
              "content": {{contentJson}}
            }
            """;

        DurableAgentStateUnknownContent content = Assert.IsType<DurableAgentStateUnknownContent>(
            JsonSerializer.Deserialize(
                json,
                DurableAgentStateJsonContext.Default.DurableAgentStateContent));
        string roundTrip = JsonSerializer.Serialize(
            content,
            DurableAgentStateJsonContext.Default.DurableAgentStateUnknownContent);

        using JsonDocument document = JsonDocument.Parse(roundTrip);
        Assert.Equal(expectedKind, content.Content.ValueKind);
        Assert.True(JsonElement.DeepEquals(
            JsonDocument.Parse(contentJson).RootElement,
            document.RootElement.GetProperty("content")));
    }

    [Fact]
    public void KnownContentPreservesExplicitNullVersusAbsent()
    {
        const string Json = """
            {
              "schemaVersion": "2.0.0",
              "data": {
                "conversationHistory": [{
                  "$type": "request",
                  "messages": [{
                    "role": "user",
                    "contents": [
                      { "$type": "error", "details": null },
                      { "$type": "functionResult", "callId": "null", "result": null },
                      { "$type": "functionResult", "callId": "absent" }
                    ]
                  }]
                }],
                "terminalResults": {},
                "completionReceipts": {}
              }
            }
            """;

        string roundTrip = Serialize(Deserialize(Json));
        using JsonDocument document = JsonDocument.Parse(roundTrip);
        JsonElement contents = document.RootElement.GetProperty("data")
            .GetProperty("conversationHistory")[0]
            .GetProperty("messages")[0]
            .GetProperty("contents");

        Assert.Equal(JsonValueKind.Null, contents[0].GetProperty("details").ValueKind);
        Assert.Equal(JsonValueKind.Null, contents[1].GetProperty("result").ValueKind);
        Assert.False(contents[2].TryGetProperty("result", out _));
    }

    private static DurableAgentState CreateEmptyRevisedState(JsonElement binding = default)
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

    private static DurableAgentState Deserialize(string json)
    {
        using JsonDocument document = JsonDocument.Parse(json);
        return document.RootElement.GetProperty("schemaVersion").GetString() == DurableAgentState.RevisedSchemaVersion
            ? DurableAgentStateJsonConverter.DeserializeRevisedContract(json)
            : Assert.IsType<DurableAgentState>(
                JsonSerializer.Deserialize(json, DurableAgentStateJsonContext.Default.DurableAgentState));
    }

    private static string Serialize(DurableAgentState state) =>
        state.SchemaVersion == DurableAgentState.RevisedSchemaVersion
            ? DurableAgentStateJsonConverter.SerializeRevisedContract(state)
            : JsonSerializer.Serialize(state, DurableAgentStateJsonContext.Default.DurableAgentState);

    private static bool IsSchemaOnlyLegacyEntryCase(JsonElement state)
    {
        if (state.GetProperty("schemaVersion").GetString() == DurableAgentState.RevisedSchemaVersion ||
            !state.GetProperty("data").TryGetProperty("conversationHistory", out JsonElement history))
        {
            return false;
        }

        return history.EnumerateArray().Any(entry =>
            entry.ValueKind == JsonValueKind.Object &&
            !entry.TryGetProperty("$type", out _));
    }
}
