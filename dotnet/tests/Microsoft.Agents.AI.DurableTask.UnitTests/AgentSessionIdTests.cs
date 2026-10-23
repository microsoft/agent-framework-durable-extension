// Copyright (c) Microsoft. All rights reserved.

using Microsoft.DurableTask.Entities;

namespace Microsoft.Agents.AI.DurableTask.UnitTests;

public sealed class AgentSessionIdTests
{
    [Theory]
    [InlineData("@dafx-v2-test-agent@12345", "test-agent", "12345", 2, false)]
    [InlineData("@dafx-v3-test-agent@12345", "test-agent", "12345", 3, false)]
    [InlineData("@dafx-test-agent@12345", "test-agent", "12345", 1, true)]
    public void ParseValidSessionId(string sessionIdString, string expectedName, string expectedKey, int expectedVersion, bool expectedIsLegacy)
    {
        AgentSessionId sessionId = AgentSessionId.Parse(sessionIdString);

        Assert.Equal(expectedName, sessionId.Name);
        Assert.Equal(expectedKey, sessionId.Key);
        Assert.Equal(expectedVersion, sessionId.EntityVersion);
        Assert.Equal(expectedIsLegacy, sessionId.IsLegacyEntity);
        Assert.Equal(sessionIdString, sessionId.ToString());
    }

    [Theory]
    [InlineData("@test-agent@12345")] // Missing "dafx-" or "dafx-v2-" prefix
    [InlineData("@dafx-@12345")] // Empty agent name after legacy prefix
    [InlineData("@dafx-v2-@12345")] // Empty agent name after v2 prefix
    [InlineData("@dafx-v3-@12345")] // Empty agent name after v3 prefix
    [InlineData("@dafx-v1-test-agent@12345")] // v1 uses dafx- without v1- prefix
    [InlineData("@dafx-v0-test-agent@12345")] // Invalid version 0
    public void ParseInvalidSessionId(string invalidSessionIdString)
    {
        Assert.Throws<ArgumentException>(() => AgentSessionId.Parse(invalidSessionIdString));
    }

    [Theory]
    [InlineData("dafx-v2-test-agent", "12345", "test-agent", 2, false)]
    [InlineData("dafx-v3-test-agent", "12345", "test-agent", 3, false)]
    [InlineData("dafx-test-agent", "12345", "test-agent", 1, true)]
    public void FromEntityId(string entityName, string key, string expectedName, int expectedVersion, bool expectedIsLegacy)
    {
        EntityInstanceId entityId = new(entityName, key);
        AgentSessionId sessionId = (AgentSessionId)entityId;

        Assert.Equal(expectedName, sessionId.Name);
        Assert.Equal(key, sessionId.Key);
        Assert.Equal(expectedVersion, sessionId.EntityVersion);
        Assert.Equal(expectedIsLegacy, sessionId.IsLegacyEntity);
        Assert.Equal(entityId, sessionId.ToEntityId());
    }

    [Theory]
    [InlineData("test-agent")]
    [InlineData("dafx-")]
    [InlineData("dafx-v2-")]
    [InlineData("dafx-v3-")]
    [InlineData("dafx-v1-test-agent")]
    public void FromInvalidEntityId(string invalidEntityName)
    {
        const string Key = "12345";

        EntityInstanceId entityId = new(invalidEntityName, Key);

        Assert.Throws<ArgumentException>(() =>
        {
            // This assignment should throw an exception because
            // the entity ID is not a valid agent session ID.
            AgentSessionId sessionId = entityId;
        });
    }

    [Fact]
    public void ConstructorAndWithRandomKeyUseV2EntityPrefix()
    {
        AgentSessionId explicitId = new("test-agent", "12345");
        Assert.False(explicitId.IsLegacyEntity);
        Assert.Equal(2, explicitId.EntityVersion);
        Assert.Equal("dafx-v2-test-agent", explicitId.ToEntityId().Name);
        Assert.Equal("@dafx-v2-test-agent@12345", explicitId.ToString());

        AgentSessionId randomId = AgentSessionId.WithRandomKey("test-agent");
        Assert.False(randomId.IsLegacyEntity);
        Assert.Equal(2, randomId.EntityVersion);
        Assert.Equal("dafx-v2-test-agent", randomId.ToEntityId().Name);
        Assert.StartsWith("@dafx-v2-test-agent@", randomId.ToString(), StringComparison.Ordinal);

        AgentSessionId legacyId = AgentSessionId.CreateLegacy("test-agent", "12345");
        Assert.True(legacyId.IsLegacyEntity);
        Assert.Equal(1, legacyId.EntityVersion);
        Assert.Equal("dafx-test-agent", legacyId.ToEntityId().Name);
        Assert.Equal("@dafx-test-agent@12345", legacyId.ToString());
        Assert.NotEqual(explicitId, legacyId);
    }

    [Fact]
    public void ToEntityNameAndParseSupportMultiVersionEntityPrefixes()
    {
        Assert.Equal("dafx-agent", AgentSessionId.ToEntityName("agent", 1));
        Assert.Equal("dafx-v2-agent", AgentSessionId.ToEntityName("agent", 2));
        Assert.Equal("dafx-v3-agent", AgentSessionId.ToEntityName("agent", 3));
        Assert.Throws<ArgumentOutOfRangeException>(() => AgentSessionId.ToEntityName("agent", 0));

        AgentSessionId v1 = AgentSessionId.Parse("@dafx-agent@k");
        Assert.Equal("agent", v1.Name);
        Assert.Equal("k", v1.Key);
        Assert.Equal(1, v1.EntityVersion);
        Assert.True(v1.IsLegacyEntity);

        AgentSessionId v2 = AgentSessionId.Parse("@dafx-v2-agent@k");
        Assert.Equal("agent", v2.Name);
        Assert.Equal("k", v2.Key);
        Assert.Equal(2, v2.EntityVersion);
        Assert.False(v2.IsLegacyEntity);

        AgentSessionId v3 = AgentSessionId.Parse("@dafx-v3-agent@k");
        Assert.Equal("agent", v3.Name);
        Assert.Equal("k", v3.Key);
        Assert.Equal(3, v3.EntityVersion);
        Assert.False(v3.IsLegacyEntity);
    }

    [Theory]
    [InlineData("v2-foo")]
    [InlineData("v3-foo")]
    [InlineData("V10-bar")]
    [InlineData("v1-baz")]
    public void AddAIAgentFactoryRejectsAnyVersionedAgentNamePrefix(string invalidAgentName)
    {
        DurableAgentsOptions options = new();

        Assert.Throws<ArgumentException>(
            () => options.AddAIAgentFactory(invalidAgentName, _ => throw new InvalidOperationException()));
    }

    // Ensures the 2-arg constructor treats the key as opaque and never re-interprets
    // it as a serialized session id, so the resulting Name always comes from the first
    // argument regardless of the key's shape.
    [Fact]
    public void ConstructorTreatsKeyAsOpaqueValue()
    {
        AgentSessionId sessionId = new("agentA", "@dafx-agentB@some-key");

        Assert.Equal("agentA", sessionId.Name);
        Assert.Equal("@dafx-agentB@some-key", sessionId.Key);
        Assert.False(sessionId.IsLegacyEntity);
    }
}
