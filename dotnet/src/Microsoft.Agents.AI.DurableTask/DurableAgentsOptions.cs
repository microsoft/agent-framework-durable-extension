// Copyright (c) Microsoft. All rights reserved.

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Builder for configuring durable agents.
/// </summary>
public sealed class DurableAgentsOptions
{
    // Agent names are case-insensitive
    private readonly Dictionary<string, Func<IServiceProvider, AIAgent>> _agentFactories = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, TimeSpan?> _agentTimeToLive = new(StringComparer.OrdinalIgnoreCase);

    // Agents that were discovered on a workflow rather than registered explicitly by the caller. Hosts use
    // this to decide whether an agent should get its own entry points: an agent that only exists because a
    // workflow references it is an implementation detail of that workflow, not a separately addressable agent.
    private readonly HashSet<string> _workflowRegisteredAgents = new(StringComparer.OrdinalIgnoreCase);

    internal DurableAgentsOptions()
    {
    }

    /// <summary>
    /// Gets or sets the default time-to-live (TTL) for agent entities.
    /// </summary>
    /// <remarks>
    /// If an agent entity is idle for this duration, it will be automatically deleted.
    /// Defaults to 14 days. Set to <see langword="null"/> to disable TTL for agents without explicit TTL configuration.
    /// </remarks>
    public TimeSpan? DefaultTimeToLive { get; set; } = TimeSpan.FromDays(14);

    /// <summary>
    /// Gets or sets the minimum delay for scheduling TTL deletion signals. Defaults to 5 minutes.
    /// </summary>
    /// <remarks>
    /// This property is primarily useful for testing (where shorter delays are needed) or for
    /// shorter-lived agents in workflows that need more rapid cleanup. The maximum allowed value is 5 minutes.
    /// Reducing the minimum deletion delay below 5 minutes can be useful for testing or for ensuring rapid cleanup of short-lived agent sessions.
    /// However, this can also increase the load on the system and should be used with caution.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the value exceeds 5 minutes.</exception>
    public TimeSpan MinimumTimeToLiveSignalDelay
    {
        get;
        set
        {
            const int MaximumDelayMinutes = 5;
            if (value > TimeSpan.FromMinutes(MaximumDelayMinutes))
            {
                throw new ArgumentOutOfRangeException(
                    nameof(value),
                    value,
                    $"The minimum time-to-live signal delay cannot exceed {MaximumDelayMinutes} minutes.");
            }

            field = value;
        }
    } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Adds an AI agent factory to the options.
    /// </summary>
    /// <param name="name">The name of the agent.</param>
    /// <param name="factory">The factory function to create the agent.</param>
    /// <param name="timeToLive">Optional time-to-live for this agent's entities. If not specified, uses <see cref="DefaultTimeToLive"/>.</param>
    /// <returns>The options instance.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="name"/> or <paramref name="factory"/> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when an agent with the same name has already been registered explicitly.</exception>
    public DurableAgentsOptions AddAIAgentFactory(string name, Func<IServiceProvider, AIAgent> factory, TimeSpan? timeToLive = null)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(factory);
        this.AddExplicitAgentFactory(name, factory, nameof(name));
        if (timeToLive.HasValue)
        {
            this._agentTimeToLive[name] = timeToLive;
        }

        return this;
    }

    /// <summary>
    /// Adds an AI agent to the options.
    /// </summary>
    /// <param name="agent">The agent to add.</param>
    /// <param name="timeToLive">Optional time-to-live for this agent's entities. If not specified, uses <see cref="DefaultTimeToLive"/>.</param>
    /// <returns>The options instance.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="agent"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="agent.Name"/> is null or whitespace or when an agent with the same name has already been registered explicitly.
    /// </exception>
    /// <remarks>
    /// Registering an agent that a workflow already discovered is allowed: the explicit registration takes over,
    /// so an agent can be promoted to a standalone agent regardless of whether the workflow was configured first.
    /// </remarks>
    public DurableAgentsOptions AddAIAgent(AIAgent agent, TimeSpan? timeToLive = null)
    {
        ArgumentNullException.ThrowIfNull(agent);

        if (string.IsNullOrWhiteSpace(agent.Name))
        {
            throw new ArgumentException($"{nameof(agent.Name)} must not be null or whitespace.", nameof(agent));
        }

        this.AddExplicitAgentFactory(agent.Name, sp => agent, nameof(agent));
        if (timeToLive.HasValue)
        {
            this._agentTimeToLive[agent.Name] = timeToLive;
        }

        return this;
    }

    /// <summary>
    /// Records an agent the caller registered directly.
    /// </summary>
    /// <remarks>
    /// An agent that is only present because a workflow references it is an implicit registration made on that
    /// workflow's behalf, so an explicit registration for the same name replaces it and clears the marker. Two
    /// explicit registrations for one name remain an error.
    /// </remarks>
    private void AddExplicitAgentFactory(string name, Func<IServiceProvider, AIAgent> factory, string paramName)
    {
        if (this._agentFactories.ContainsKey(name) && !this._workflowRegisteredAgents.Contains(name))
        {
            throw new ArgumentException($"An agent with name '{name}' has already been registered.", paramName);
        }

        this._agentFactories[name] = factory;
        this._workflowRegisteredAgents.Remove(name);
    }

    /// <summary>
    /// Adds an agent that was discovered on a workflow, and records that it was registered on the workflow's
    /// behalf rather than explicitly by the caller.
    /// </summary>
    /// <remarks>
    /// Only agents that are not already registered reach this method, so an agent the caller added explicitly is
    /// never flagged, regardless of whether a workflow also references it.
    /// </remarks>
    internal DurableAgentsOptions AddWorkflowRegisteredAIAgent(AIAgent agent)
    {
        this.AddAIAgent(agent);
        this._workflowRegisteredAgents.Add(agent.Name!);

        return this;
    }

    /// <summary>
    /// Determines whether the named agent is only present because a workflow references it.
    /// </summary>
    internal bool IsWorkflowRegisteredAgent(string agentName)
    {
        ArgumentNullException.ThrowIfNull(agentName);
        return this._workflowRegisteredAgents.Contains(agentName);
    }

    /// <summary>
    /// Gets the agents that have been added to this builder.
    /// </summary>
    /// <returns>A read-only collection of agents.</returns>
    internal IReadOnlyDictionary<string, Func<IServiceProvider, AIAgent>> GetAgentFactories()
    {
        return this._agentFactories.AsReadOnly();
    }

    /// <summary>
    /// Gets the time-to-live for a specific agent, or the default TTL if not specified.
    /// </summary>
    /// <param name="agentName">The name of the agent.</param>
    /// <returns>The time-to-live for the agent, or the default TTL if not specified.</returns>
    internal TimeSpan? GetTimeToLive(string agentName)
    {
        return this._agentTimeToLive.TryGetValue(agentName, out TimeSpan? ttl) ? ttl : this.DefaultTimeToLive;
    }

    /// <summary>
    /// Determines whether an agent with the specified name is registered.
    /// </summary>
    /// <param name="agentName">The name of the agent to locate. Cannot be null.</param>
    /// <returns>true if an agent with the specified name is registered; otherwise, false.</returns>
    internal bool ContainsAgent(string agentName)
    {
        ArgumentNullException.ThrowIfNull(agentName);
        return this._agentFactories.ContainsKey(agentName);
    }
}
