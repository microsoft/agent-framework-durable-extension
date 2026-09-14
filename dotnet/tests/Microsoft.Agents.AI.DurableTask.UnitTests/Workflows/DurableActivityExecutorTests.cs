// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Agents.AI.DurableTask.Workflows;
using Microsoft.Agents.AI.Workflows;

namespace Microsoft.Agents.AI.DurableTask.UnitTests.Workflows;

public sealed class DurableActivityExecutorTests
{
    private static readonly AsyncLocal<ActivationCounts?> s_activations = new();

    private static readonly JsonSerializerOptions s_camelCaseOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true
    };

    #region DeserializeInput

    [Fact]
    public void DeserializeInput_StringType_ReturnsInputAsIs()
    {
        // Arrange
        const string Input = "hello world";

        // Act
        object result = DurableActivityExecutor.DeserializeInput(Input, typeof(string));

        // Assert
        Assert.Equal("hello world", result);
    }

    [Fact]
    public void DeserializeInput_SimpleObject_DeserializesCorrectly()
    {
        // Arrange
        string input = JsonSerializer.Serialize(new TestRecord("EXP-001", 100.50m), s_camelCaseOptions);

        // Act
        object result = DurableActivityExecutor.DeserializeInput(input, typeof(TestRecord));

        // Assert
        TestRecord record = Assert.IsType<TestRecord>(result);
        Assert.Equal("EXP-001", record.Id);
        Assert.Equal(100.50m, record.Amount);
    }

    [Fact]
    public void DeserializeInput_StringArray_DeserializesDirectly()
    {
        // Arrange
        string input = JsonSerializer.Serialize((string[])["a", "b", "c"]);

        // Act
        object result = DurableActivityExecutor.DeserializeInput(input, typeof(string[]));

        // Assert
        string[] array = Assert.IsType<string[]>(result);
        Assert.Equal(["a", "b", "c"], array);
    }

    [Fact]
    public void DeserializeInput_TypedArrayFromFanIn_DeserializesEachElement()
    {
        // Arrange — fan-in produces a JSON array of serialized strings
        TestRecord r1 = new("EXP-001", 100m);
        TestRecord r2 = new("EXP-002", 200m);
        string[] serializedElements =
        [
            JsonSerializer.Serialize(r1, s_camelCaseOptions),
            JsonSerializer.Serialize(r2, s_camelCaseOptions)
        ];
        string input = JsonSerializer.Serialize(serializedElements);

        // Act
        object result = DurableActivityExecutor.DeserializeInput(input, typeof(TestRecord[]));

        // Assert
        TestRecord[] records = Assert.IsType<TestRecord[]>(result);
        Assert.Equal(2, records.Length);
        Assert.Equal("EXP-001", records[0].Id);
        Assert.Equal(100m, records[0].Amount);
        Assert.Equal("EXP-002", records[1].Id);
        Assert.Equal(200m, records[1].Amount);
    }

    [Fact]
    public void DeserializeInput_TypedArrayWithSingleElement_DeserializesCorrectly()
    {
        // Arrange
        TestRecord r1 = new("EXP-001", 50m);
        string[] serializedElements = [JsonSerializer.Serialize(r1, s_camelCaseOptions)];
        string input = JsonSerializer.Serialize(serializedElements);

        // Act
        object result = DurableActivityExecutor.DeserializeInput(input, typeof(TestRecord[]));

        // Assert
        TestRecord[] records = Assert.IsType<TestRecord[]>(result);
        Assert.Single(records);
        Assert.Equal("EXP-001", records[0].Id);
    }

    [Fact]
    public void DeserializeInput_TypedArrayWithNullElement_ThrowsInvalidOperationException()
    {
        // Arrange — one element is "null"
        string input = JsonSerializer.Serialize((string[])["null"]);

        // Act & Assert
        Assert.Throws<InvalidOperationException>(
            () => DurableActivityExecutor.DeserializeInput(input, typeof(TestRecord[])));
    }

    [Fact]
    public void DeserializeInput_InvalidJson_ThrowsJsonException()
    {
        // Arrange
        const string Input = "not valid json";

        // Act & Assert
        Assert.ThrowsAny<JsonException>(
            () => DurableActivityExecutor.DeserializeInput(Input, typeof(TestRecord)));
    }

    #endregion

    #region ResolveInputType

    [Fact]
    public void ResolveInputType_NullTypeName_ReturnsFirstSupportedType()
    {
        // Arrange
        HashSet<Type> supportedTypes = [typeof(TestRecord), typeof(string)];

        // Act
        Type result = DurableActivityExecutor.ResolveInputType(null, supportedTypes);

        // Assert
        Assert.Equal(typeof(TestRecord), result);
    }

    [Fact]
    public void ResolveInputType_EmptyTypeName_ReturnsFirstSupportedType()
    {
        // Arrange
        HashSet<Type> supportedTypes = [typeof(TestRecord)];

        // Act
        Type result = DurableActivityExecutor.ResolveInputType(string.Empty, supportedTypes);

        // Assert
        Assert.Equal(typeof(TestRecord), result);
    }

    [Fact]
    public void ResolveInputType_EmptySupportedTypes_DefaultsToString()
    {
        // Arrange
        HashSet<Type> supportedTypes = [];

        // Act
        Type result = DurableActivityExecutor.ResolveInputType(null, supportedTypes);

        // Assert
        Assert.Equal(typeof(string), result);
    }

    [Fact]
    public void ResolveInputType_MatchesByFullName()
    {
        // Arrange
        HashSet<Type> supportedTypes = [typeof(TestRecord)];

        // Act
        Type result = DurableActivityExecutor.ResolveInputType(typeof(TestRecord).FullName, supportedTypes);

        // Assert
        Assert.Equal(typeof(TestRecord), result);
    }

    [Fact]
    public void ResolveInputType_MatchesByName()
    {
        // Arrange
        HashSet<Type> supportedTypes = [typeof(TestRecord)];

        // Act
        Type result = DurableActivityExecutor.ResolveInputType("TestRecord", supportedTypes);

        // Assert
        Assert.Equal(typeof(TestRecord), result);
    }

    [Fact]
    public void ResolveInputType_StringArrayFallsBackToSupportedType()
    {
        // Arrange — fan-in sends string[] but executor expects TestRecord[]
        HashSet<Type> supportedTypes = [typeof(TestRecord[])];

        // Act
        Type result = DurableActivityExecutor.ResolveInputType(typeof(string[]).FullName, supportedTypes);

        // Assert
        Assert.Equal(typeof(TestRecord[]), result);
    }

    [Fact]
    public void ResolveInputType_StringFallsBackToSupportedType()
    {
        // Arrange — executor doesn't support string
        HashSet<Type> supportedTypes = [typeof(TestRecord)];

        // Act
        Type result = DurableActivityExecutor.ResolveInputType(typeof(string).FullName, supportedTypes);

        // Assert
        Assert.Equal(typeof(TestRecord), result);
    }

    [Fact]
    public void ResolveInputType_StringArrayRetainedWhenSupported()
    {
        // Arrange — executor explicitly supports string[]
        HashSet<Type> supportedTypes = [typeof(string[])];

        // Act
        Type result = DurableActivityExecutor.ResolveInputType(typeof(string[]).FullName, supportedTypes);

        // Assert
        Assert.Equal(typeof(string[]), result);
    }

    #endregion

    #region ExecuteAsync

    [Theory]
    [InlineData("object", false)]
    [InlineData("converter", false)]
    [InlineData("array", false)]
    [InlineData("generic", false)]
    [InlineData("converter-array", false)]
    [InlineData("converter-generic", false)]
    [InlineData("object", true)]
    [InlineData("converter", true)]
    [InlineData("array", true)]
    [InlineData("generic", true)]
    [InlineData("converter-array", true)]
    [InlineData("converter-generic", true)]
    public async Task ExecuteAsync_UnsupportedResolvedType_RejectsBeforeActivationAsync(string shape, bool malformedPayload)
    {
        (Type type, string payload) = shape switch
        {
            "object" => (typeof(ActivatedInput), "{}"),
            "converter" => (typeof(ConvertedInput), "{}"),
            "array" => (typeof(ActivatedInput[]), """["{}","{}"]"""),
            "generic" => (typeof(List<ActivatedInput>), "[{},{}]"),
            "converter-array" => (typeof(ConvertedInput[]), """["{}","{}"]"""),
            "converter-generic" => (typeof(List<ConvertedInput>), "[{},{}]"),
            _ => throw new ArgumentOutOfRangeException(nameof(shape))
        };
        ActivationCounts counts = StartTracking();
        string wire = Envelope(type.AssemblyQualifiedName, malformedPayload ? "not JSON" : payload);

        Exception? exception = await Record.ExceptionAsync(() => ExecuteWithContractAsync<AllowedInput>(wire, counts));

        AssertNoActivation(counts);
        Assert.IsType<InvalidOperationException>(exception);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExecuteAsync_SupportedElementDoesNotAuthorizeContainerAsync(bool array)
    {
        ActivationCounts counts = StartTracking();
        Type type = array ? typeof(ActivatedInput[]) : typeof(List<ActivatedInput>);
        string wire = Envelope(type.AssemblyQualifiedName, array ? """["{}","{}"]""" : "[{},{}]");

        Exception? exception = await Record.ExceptionAsync(() => ExecuteWithContractAsync<ActivatedInput>(wire, counts));

        AssertNoActivation(counts);
        Assert.IsType<InvalidOperationException>(exception);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExecuteAsync_VersionNormalizationDoesNotAuthorizeUnsupportedTypeAsync(bool generic)
    {
        ActivationCounts counts = StartTracking();
        Type type = generic ? typeof(List<ActivatedInput>) : typeof(ActivatedInput);
        string wire = Envelope(MutatedVersionName(type), generic ? "[{},{}]" : "{}");

        Exception? exception = await Record.ExceptionAsync(() => ExecuteWithContractAsync<AllowedInput>(wire, counts));

        AssertNoActivation(counts);
        Assert.IsType<InvalidOperationException>(exception);
    }

    [Theory]
    [InlineData("invalid-version")]
    [InlineData("invalid-generic")]
    [InlineData("invalid-array")]
    [InlineData("missing-assembly")]
    public async Task ExecuteAsync_MalformedSupportedName_DoesNotUseRegisteredDefaultAsync(string malformed)
    {
        ActivationCounts counts = StartTracking();
        string typeName = malformed switch
        {
            "invalid-version" => $"{typeof(AllowedInput).FullName}, {typeof(AllowedInput).Assembly.GetName().Name}, Version=invalid",
            "invalid-generic" => typeof(AllowedInput).FullName + "[[",
            "invalid-array" => typeof(AllowedInput).FullName + "[",
            _ => typeof(AllowedInput).FullName + ", Missing.Assembly"
        };

        Exception? exception = await Record.ExceptionAsync(
            () => ExecuteWithContractAsync<AllowedInput>(Envelope(typeName, "not JSON"), counts));

        AssertNoActivation(counts);
        Assert.NotNull(exception);
        Assert.False(exception is JsonException);
    }

    [Theory]
    [InlineData(" ")]
    [InlineData("\t\r\n")]
    [InlineData(" System.String")]
    [InlineData("System.String ")]
    [InlineData("Missing.Input")]
    [InlineData("Missing.Input, Missing.Assembly")]
    [InlineData("System.Collections.Generic.List`1[[")]
    [InlineData("System.String[")]
    [InlineData("System.Int32,")]
    public async Task ExecuteAsync_InvalidExplicitName_RejectsBeforePayloadParsingAsync(string typeName)
    {
        ActivationCounts counts = StartTracking();

        Exception? exception = await Record.ExceptionAsync(
            () => ExecuteWithContractAsync<AllowedInput>(Envelope(typeName, "not JSON"), counts));

        AssertNoActivation(counts);
        Assert.NotNull(exception);
        Assert.False(exception is JsonException);
        Assert.IsNotType<NotSupportedException>(exception);
    }

    [Theory]
    [InlineData("""{"inputTypeName":"Missing.Input","input":{},"state":{}}""")]
    [InlineData("""{"input":{},"state":{},"InputTypeName":"Missing.Input"}""")]
    [InlineData("""{"inputTypeName":"Missing.Input","input":"{}","state":42}""")]
    [InlineData("""{"state":42,"input":"{}","INPUTTYPENAME":"Missing.Input"}""")]
    [InlineData("""{"inputTypeName":42,"input":"{}"}""")]
    [InlineData("""{"inputTypeName":{},"input":"{}"}""")]
    [InlineData("""{"inputTypeName":[],"input":"{}"}""")]
    [InlineData("""{"inputTypeName":true,"input":"{}"}""")]
    [InlineData("""{"inputTypeName":"Missing.Input","InputTypeName":null,"input":"{}"}""")]
    [InlineData("""{"inputTypeName":"Missing.Input","InputTypeName":"","input":"{}"}""")]
    [InlineData("""{"inputTypeName":null,"InputTypeName":"Missing.Input","input":"{}"}""")]
    [InlineData("""{"inputTypeName":"Missing.Input","InputTypeName":"System.String","input":"{}"}""")]
    [InlineData("""{"inputTypeName":"Missing.Input","input":""")]
    [InlineData("""{"inputTypeName":""")]
    [InlineData("""{"inputTypeName":"Missing.Input"} trailing""")]
    [InlineData("""{"inputTypeName":null,"input":"{}","state":42}""")]
    [InlineData("""{"inputTypeName":"","input":"{}","state":42}""")]
    [InlineData("""{"input\u0054ypeName":"Missing.Input","input":"{}","state":42}""")]
    public async Task ExecuteAsync_MalformedNamedEnvelope_DoesNotBecomeLegacyInputAsync(string wire)
    {
        ActivationCounts counts = StartTracking();

        Exception? exception = await Record.ExceptionAsync(() => ExecuteWithContractAsync<AllowedInput>(wire, counts));

        AssertNoActivation(counts);
        Assert.IsAssignableFrom<JsonException>(exception);
    }

    [Theory]
    [InlineData("assembly")]
    [InlineData("full")]
    [InlineData("short")]
    [InlineData("version")]
    [InlineData("string")]
    [InlineData("absent")]
    [InlineData("null")]
    [InlineData("empty")]
    [InlineData("raw")]
    public async Task ExecuteAsync_SupportedScalarAndLegacyHints_InvokeRegisteredHandlerAsync(string hint)
    {
        ActivationCounts counts = StartTracking();
        string? typeName = hint switch
        {
            "assembly" => typeof(AllowedInput).AssemblyQualifiedName,
            "full" => typeof(AllowedInput).FullName,
            "short" => nameof(AllowedInput),
            "version" => MutatedVersionName(typeof(AllowedInput)),
            "string" => typeof(string).AssemblyQualifiedName,
            "empty" => string.Empty,
            _ => null
        };
        string wire = hint switch
        {
            "raw" => """{"value":"accepted"}""",
            "null" => """{"input":"{\"value\":\"accepted\"}","inputTypeName":null}""",
            _ => Envelope(typeName, """{"value":"accepted"}""")
        };

        string result = await ExecuteWithContractAsync<AllowedInput>(wire, counts);

        AssertHandled(result, counts, 1);
        Assert.Equal("accepted", Assert.IsType<AllowedInput>(counts.Input).Value);
    }

    [Theory]
    [InlineData("assembly")]
    [InlineData("full")]
    [InlineData("version")]
    [InlineData("string-array")]
    [InlineData("string")]
    [InlineData("absent")]
    public async Task ExecuteAsync_SupportedFanInArray_MaterializesRegisteredElementsAsync(string hint)
    {
        ActivationCounts counts = StartTracking();
        string? typeName = hint switch
        {
            "assembly" => typeof(AllowedInput[]).AssemblyQualifiedName,
            "full" => typeof(AllowedInput[]).FullName,
            "version" => MutatedVersionName(typeof(AllowedInput[])),
            "string-array" => typeof(string[]).FullName,
            "string" => typeof(string).FullName,
            _ => null
        };
        const string Payload = """["{\"value\":\"one\"}","{\"value\":\"two\"}"]""";

        string result = await ExecuteWithContractAsync<AllowedInput[]>(Envelope(typeName, Payload), counts);

        AssertHandled(result, counts, 2);
        Assert.Equal(["one", "two"], Assert.IsType<AllowedInput[]>(counts.Input).Select(item => item.Value));
    }

    [Theory]
    [InlineData("assembly")]
    [InlineData("full")]
    [InlineData("version")]
    [InlineData("string")]
    [InlineData("absent")]
    public async Task ExecuteAsync_SupportedClosedGeneric_MaterializesRegisteredElementsAsync(string hint)
    {
        ActivationCounts counts = StartTracking();
        string? typeName = hint switch
        {
            "assembly" => typeof(List<AllowedInput>).AssemblyQualifiedName,
            "full" => typeof(List<AllowedInput>).FullName,
            "version" => MutatedVersionName(typeof(List<AllowedInput>)),
            "string" => typeof(string).FullName,
            _ => null
        };

        string result = await ExecuteWithContractAsync<List<AllowedInput>>(
            Envelope(typeName, """[{"value":"one"},{"value":"two"}]"""), counts);

        AssertHandled(result, counts, 2);
        Assert.Equal(["one", "two"], Assert.IsType<List<AllowedInput>>(counts.Input).Select(item => item.Value));
    }

    [Fact]
    public async Task ExecuteAsync_SupportedConverter_InvokesConverterAndHandlerAsync()
    {
        ActivationCounts counts = StartTracking();

        string result = await ExecuteWithContractAsync<ConvertedInput>(Envelope(typeof(ConvertedInput).AssemblyQualifiedName, "{}"), counts);

        Assert.Equal("handled", JsonSerializer.Deserialize(result, DurableWorkflowJsonContext.Default.DurableExecutorOutput)!.Result);
        Assert.Equal(1, counts.PayloadConstructors);
        Assert.Equal(1, counts.ConverterReads);
        Assert.InRange(counts.ConverterConstructors, 0, 1);
        Assert.Equal(1, counts.Handlers);
        Assert.IsType<ConvertedInput>(counts.Input);
    }

    [Theory]
    [InlineData("hello")]
    [InlineData("{not JSON")]
    [InlineData("""{"value":"raw object"}""")]
    [InlineData("""{"input":{},"state":42}""")]
    [InlineData("""{"nested":{"inputTypeName":"Missing.Input"}}""")]
    [InlineData("""[{"inputTypeName":"Missing.Input"}]""")]
    public async Task ExecuteAsync_RawLegacyStringWithoutTopLevelHint_RemainsOpaqueAsync(string wire)
    {
        ActivationCounts counts = StartTracking();

        string result = await ExecuteWithContractAsync<string>(wire, counts);

        AssertHandled(result, counts, 0);
        Assert.Equal(wire, Assert.IsType<string>(counts.Input));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExecuteAsync_SupportedString_KeepsExactPayloadAsync(bool stringArrayHint)
    {
        ActivationCounts counts = StartTracking();
        const string Payload = """["{\"value\":42}","not JSON"]""";
        Type hint = stringArrayHint ? typeof(string[]) : typeof(string);

        string result = await ExecuteWithContractAsync<string>(Envelope(hint.AssemblyQualifiedName, Payload), counts);

        AssertHandled(result, counts, 0);
        Assert.Equal(Payload, Assert.IsType<string>(counts.Input));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExecuteAsync_SupportedStringArray_KeepsExactElementsAsync(bool stringHint)
    {
        ActivationCounts counts = StartTracking();
        Type hint = stringHint ? typeof(string) : typeof(string[]);

        string result = await ExecuteWithContractAsync<string[]>(Envelope(hint.AssemblyQualifiedName, """["{}","not JSON"]"""), counts);

        AssertHandled(result, counts, 0);
        Assert.Equal(["{}", "not JSON"], Assert.IsType<string[]>(counts.Input));
    }

    private static ActivationCounts StartTracking()
    {
        ActivationCounts counts = new();
        s_activations.Value = counts;
        return counts;
    }

    private static string Envelope(string? typeName, string payload) =>
        JsonSerializer.Serialize(new DurableActivityInput { Input = payload, InputTypeName = typeName }, DurableWorkflowJsonContext.Default.DurableActivityInput);

    private static string MutatedVersionName(Type type) =>
        type.AssemblyQualifiedName!
            .Replace($"Version={type.Assembly.GetName().Version}", "Version=99.0.0.0", StringComparison.Ordinal)
            .Replace($"Version={typeof(AllowedInput).Assembly.GetName().Version}", "Version=99.0.0.0", StringComparison.Ordinal);

    private static Task<string> ExecuteWithContractAsync<T>(string wire, ActivationCounts counts)
    {
        FunctionExecutor<T, string> executor = new("input-probe", (input, _, _) =>
        {
            counts.Handlers++;
            counts.Input = input;
            return "handled";
        });
        Workflow workflow = new WorkflowBuilder(executor).Build();
        Assert.Equal([typeof(T)], executor.InputTypes);
        return DurableActivityExecutor.ExecuteAsync(workflow.ReflectExecutors()[executor.Id], wire);
    }

    private static void AssertNoActivation(ActivationCounts counts)
    {
        Assert.True(counts.PayloadConstructors == 0 && counts.ConverterConstructors == 0 &&
            counts.ConverterReads == 0 && counts.ConverterWrites == 0 && counts.Handlers == 0,
            $"Payload constructors: {counts.PayloadConstructors}; converter constructors: {counts.ConverterConstructors}; " +
            $"converter reads: {counts.ConverterReads}; converter writes: {counts.ConverterWrites}; handlers: {counts.Handlers}.");
        Assert.Null(counts.Input);
    }

    private static void AssertHandled(string result, ActivationCounts counts, int constructors)
    {
        Assert.Equal("handled", JsonSerializer.Deserialize(result, DurableWorkflowJsonContext.Default.DurableExecutorOutput)!.Result);
        Assert.Equal(constructors, counts.PayloadConstructors);
        Assert.Equal(0, counts.ConverterConstructors);
        Assert.Equal(0, counts.ConverterReads);
        Assert.Equal(1, counts.Handlers);
    }

    private sealed class ActivationCounts
    {
        public int PayloadConstructors { get; set; }

        public int ConverterConstructors { get; set; }

        public int ConverterReads { get; set; }

        public int ConverterWrites { get; set; }

        public int Handlers { get; set; }

        public object? Input { get; set; }
    }

    public sealed class AllowedInput
    {
        public AllowedInput() => s_activations.Value!.PayloadConstructors++;

        public string? Value { get; set; }
    }

    public sealed class ActivatedInput
    {
        public ActivatedInput() => s_activations.Value!.PayloadConstructors++;
    }

    [JsonConverter(typeof(InputSentinelConverter))]
    public sealed class ConvertedInput
    {
        public ConvertedInput() => s_activations.Value!.PayloadConstructors++;
    }

    public sealed class InputSentinelConverter : JsonConverter<ConvertedInput>
    {
        public InputSentinelConverter() => s_activations.Value!.ConverterConstructors++;

        public override ConvertedInput Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            s_activations.Value!.ConverterReads++;
            using JsonDocument document = JsonDocument.ParseValue(ref reader);
            return new ConvertedInput();
        }

        public override void Write(Utf8JsonWriter writer, ConvertedInput value, JsonSerializerOptions options)
        {
            s_activations.Value!.ConverterWrites++;
            writer.WriteStartObject();
            writer.WriteEndObject();
        }
    }

    #endregion

    private sealed record TestRecord(string Id, decimal Amount);
}
