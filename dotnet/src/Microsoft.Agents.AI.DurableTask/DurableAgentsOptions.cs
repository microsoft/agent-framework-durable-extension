// Copyright (c) Microsoft. All rights reserved.

using Microsoft.Agents.AI.DurableTask.State;

namespace Microsoft.Agents.AI.DurableTask;

/// <summary>
/// Builder for configuring durable agents.
/// </summary>
public sealed class DurableAgentsOptions
{
    private const int DefaultMaxStateBytes = 1_048_576;
    private const string DeploymentModeEnvironmentVariableName = "DURABLE_AGENTS_DEPLOYMENT_MODE";
    private const string SchemaV2EnvironmentValue = "isolated_v2";
    // Agent names are case-insensitive
    private readonly Dictionary<string, Func<IServiceProvider, AIAgent>> _agentFactories = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, TimeSpan?> _agentTimeToLive = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, DurableAgentHistoryConfiguration> _historyConfigurations = new(StringComparer.OrdinalIgnoreCase);
    private bool _defaultTimeToLiveConfigured;
    private DurableAgentDeploymentMode? _configuredDeploymentMode;
    private DurableAgentDeploymentMode? _resolvedDeploymentMode;
    private bool _deploymentEnvironmentCaptured;
    private string? _deploymentEnvironmentValue;

    // Agents that were discovered on a workflow rather than registered explicitly by the caller. Hosts use
    // this to decide whether an agent should get its own entry points: an agent that only exists because a
    // workflow references it is an implementation detail of that workflow, not a separately addressable agent.
    private readonly HashSet<string> _workflowRegisteredAgents = new(StringComparer.OrdinalIgnoreCase);

    internal DurableAgentsOptions()
    {
    }

    /// <summary>
    /// Gets or sets the deployment's state writer mode. Defaults to <see cref="DurableAgentDeploymentMode.Legacy"/>.
    /// </summary>
    /// <remarks>
    /// When unspecified, the exact environment value DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2 acknowledges
    /// an isolated schema 2 deployment. Explicit Legacy conflicts with that environment value; all other
    /// present values are rejected, including empty, case-varied and whitespace-bearing values.
    /// The environment is captured once at completed configuration validation, not by this getter or setter.
    /// After validation the getter reports the resolved mode. SchemaV2 is an acknowledgement, not proof
    /// of isolation, and supports both KeepAll and Auto without migrating existing legacy sessions.
    /// </remarks>
    public DurableAgentDeploymentMode DeploymentMode
    {
        get => this._resolvedDeploymentMode ?? this._configuredDeploymentMode ?? DurableAgentDeploymentMode.Legacy;
        set
        {
            if (!Enum.IsDefined(value))
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "The durable agent deployment mode is not supported.");
            }

            this._configuredDeploymentMode = value;
            this._resolvedDeploymentMode = null;
        }
    }

    /// <summary>
    /// Gets or sets the default time-to-live (TTL) for agent entities.
    /// </summary>
    /// <remarks>
    /// If an agent entity is idle for this duration, it will be automatically deleted.
    /// Defaults to 14 days. Set to <see langword="null"/> to disable TTL for agents without explicit TTL configuration.
    /// </remarks>
    public TimeSpan? DefaultTimeToLive
    {
        get;
        set
        {
            this._defaultTimeToLiveConfigured = true;
            field = value;
        }
    } = TimeSpan.FromDays(14);

    /// <summary>
    /// Gets or sets whether successful entity operations may persist schema 2.0 request outcomes.
    /// Defaults to <see langword="false"/>.
    /// </summary>
    /// <remarks>
    /// The resolved isolated deployment enables this writer independently of history retention.
    /// The setter remains internal for execution tests; readers support both layouts regardless of this setting.
    /// </remarks>
    internal bool EnablePersistentRequestOutcomes
    {
        get => field || this.DeploymentMode == DurableAgentDeploymentMode.SchemaV2;
        set;
    }

    /// <summary>
    /// Gets or sets whether a certified provider adapter may commit explicitly attested terminal failures.
    /// </summary>
    /// <remarks>
    /// This internal capability is disabled by default. It is not enabled merely by mailbox activation.
    /// </remarks>
    internal bool EnableProviderFailureFinalization { get; set; }

    /// <summary>
    /// Gets or sets the test-only agreement to delete receipt-bearing entities after an explicit TTL.
    /// Production deletion remains disabled until a late-duplicate policy is agreed.
    /// </summary>
    internal bool EnableMailboxEntityDeletion { get; set; }

    /// <summary>
    /// Gets or sets a trusted, per-state authorization for complete synthetic legacy migration fixtures.
    /// </summary>
    /// <remarks>
    /// No production authorization is installed. Retained transcript entries, an empty transcript,
    /// or an absence of truncation metadata cannot prove that earlier completions were not evicted.
    /// Known truncation/compaction prevents migration even when this callback authorizes the fixture.
    /// </remarks>
    internal Func<DurableAgentState, bool>? AuthorizeLegacyMigration { get; set; }

    /// <summary>
    /// Gets or sets optional retention for persisted request outcome payloads. Defaults to 60 seconds.
    /// Completion receipts remain until the whole entity is deleted.
    /// </summary>
    /// <remarks>
    /// When persistent request outcomes are enabled, committed runs schedule entity-local payload cleanup.
    /// Physical removal may lag expiry; polling reports unavailable without modifying state.
    /// Imported states without a scheduled check need an explicit cleanup operation or a successful new run.
    /// This setting does not activate mailbox persistence by itself. SchemaV2 deployment acknowledgement
    /// activates schema 2 for new entity generations; existing legacy sessions are not migrated.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">The retention period is not positive.</exception>
    public TimeSpan? ResultRetentionPeriod
    {
        get;
        set
        {
            if (value <= TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "Result retention must be positive.");
            }

            field = value;
        }
    } = TimeSpan.FromSeconds(60);

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
    /// Gets or sets retention for durable entity conversation history. Defaults to
    /// <see cref="DurableAgentHistoryRetentionMode.KeepAll"/>.
    /// </summary>
    /// <remarks>
    /// Auto requires <see cref="DurableAgentDeploymentMode.SchemaV2"/> and uses a 1 MiB budget by default.
    /// Deployment acknowledgement and retention are independent: isolated KeepAll also writes schema 2.
    /// Changing retention does not change the state schema or downgrade existing state.
    /// Receipt-bearing entities are not automatically deleted by the legacy idle TTL.
    /// </remarks>
    public DurableAgentHistoryRetentionMode HistoryRetentionMode
    {
        get;
        set => field = Enum.IsDefined(value)
            ? value
            : throw new ArgumentOutOfRangeException(
                nameof(value),
                value,
                "The durable agent history retention mode is not supported.");
    } = DurableAgentHistoryRetentionMode.KeepAll;

    /// <summary>
    /// Gets or sets the positive storage-envelope byte budget for automatic retention. Defaults to 1 MiB.
    /// </summary>
    /// <remarks>
    /// The budget measures the complete UTF-8 escaped JSON-string entity state, including protected
    /// results, completion receipts, session continuation and metadata. It is not a token or transcript limit.
    /// A null value also resolves to 1 MiB under Auto; it does not disable Auto. Budget settings are inert
    /// under KeepAll. Backend limits still apply regardless of this setting.
    /// </remarks>
    public int? MaxStateBytes
    {
        get;
        set => field = value is null or > 0
            ? value
            : throw new ArgumentOutOfRangeException(
                nameof(value),
                value,
                "The durable agent state budget must be null or a positive byte count.");
    } = DefaultMaxStateBytes;

    /// <summary>
    /// Gets or sets the fraction of <see cref="MaxStateBytes"/> at which automatic retention starts.
    /// Defaults to 0.85 when not specified.
    /// </summary>
    /// <remarks>Under Auto, watermarks satisfy 0 &lt; low &lt; high &lt;= 1. They are inert under KeepAll.</remarks>
    public double? HistoryRetentionHighWatermark
    {
        get;
        set
        {
            ValidateWatermark(value, nameof(value));
            field = value;
        }
    }

    /// <summary>
    /// Gets or sets the fraction of <see cref="MaxStateBytes"/> targeted by automatic retention.
    /// Defaults to 0.70 when not specified.
    /// </summary>
    /// <remarks>
    /// Under Auto, watermarks satisfy 0 &lt; low &lt; high &lt;= 1. They are inert under KeepAll. If protected state
    /// prevents reaching this target, a write can still succeed below the high watermark.
    /// </remarks>
    public double? HistoryRetentionLowWatermark
    {
        get;
        set
        {
            ValidateWatermark(value, nameof(value));
            field = value;
        }
    }

    internal DurableAgentRetentionSettings GetRetentionSettings()
    {
        if (!this._deploymentEnvironmentCaptured)
        {
            this._deploymentEnvironmentValue =
                Environment.GetEnvironmentVariable(DeploymentModeEnvironmentVariableName);
            this._deploymentEnvironmentCaptured = true;
        }

        this._resolvedDeploymentMode = ResolveDeploymentMode(
            this._configuredDeploymentMode, this._deploymentEnvironmentValue);
        if (this.HistoryRetentionMode == DurableAgentHistoryRetentionMode.Auto &&
            this._resolvedDeploymentMode != DurableAgentDeploymentMode.SchemaV2)
        {
            throw new InvalidOperationException(
                "Automatic durable history retention requires schema-v2 deployment acknowledgement. " +
                "Set DurableAgentsOptions.DeploymentMode to DurableAgentDeploymentMode.SchemaV2, " +
                "or set DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2 before startup.");
        }

        if (this.HistoryRetentionMode == DurableAgentHistoryRetentionMode.KeepAll)
        {
            return new(
                DurableAgentHistoryRetentionMode.KeepAll,
                MaxStateBytes: null,
                DurableAgentStateRetention.DefaultHighWatermark,
                DurableAgentStateRetention.DefaultLowWatermark);
        }

        int maxStateBytes = this.MaxStateBytes ?? DefaultMaxStateBytes;

        double highWatermark =
            this.HistoryRetentionHighWatermark ?? DurableAgentStateRetention.DefaultHighWatermark;
        double lowWatermark =
            this.HistoryRetentionLowWatermark ?? DurableAgentStateRetention.DefaultLowWatermark;
        if (lowWatermark >= highWatermark)
        {
            throw new InvalidOperationException(
                "Durable history retention watermarks must satisfy 0 < low < high <= 1.");
        }

        return new(
            DurableAgentHistoryRetentionMode.Auto,
            maxStateBytes,
            highWatermark,
            lowWatermark);
    }

    internal void ValidateConfiguration()
    {
        _ = this.GetRetentionSettings();
    }

    private static DurableAgentDeploymentMode ResolveDeploymentMode(
        DurableAgentDeploymentMode? configuredMode,
        string? environmentValue)
    {
        if (environmentValue is not null &&
            !string.Equals(environmentValue, SchemaV2EnvironmentValue, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "DURABLE_AGENTS_DEPLOYMENT_MODE must be unset or exactly 'isolated_v2'; " +
                "empty values, aliases, case changes and surrounding whitespace are not supported.");
        }

        if (environmentValue is not null && configuredMode == DurableAgentDeploymentMode.Legacy)
        {
            throw new InvalidOperationException(
                "Explicit Legacy deployment mode conflicts with DURABLE_AGENTS_DEPLOYMENT_MODE=isolated_v2. " +
                "Remove the environment setting to keep Legacy, or set DurableAgentsOptions.DeploymentMode " +
                "to DurableAgentDeploymentMode.SchemaV2.");
        }

        return configuredMode ?? (environmentValue is null
            ? DurableAgentDeploymentMode.Legacy
            : DurableAgentDeploymentMode.SchemaV2);
    }

    private static void ValidateWatermark(double? value, string parameterName)
    {
        if (value.HasValue &&
            (!double.IsFinite(value.Value) || value.Value <= 0 || value.Value > 1))
        {
            throw new ArgumentOutOfRangeException(
                parameterName,
                value,
                "A durable history retention watermark must be a finite number in (0, 1].");
        }
    }

    /// <summary>
    /// Adds an AI agent factory to the options.
    /// </summary>
    /// <param name="name">The name of the agent.</param>
    /// <param name="factory">The factory function to create the agent.</param>
    /// <param name="timeToLive">Optional time-to-live for this agent's entities.</param>
    /// <returns>The options instance.</returns>
    public DurableAgentsOptions AddAIAgentFactory(
        string name,
        Func<IServiceProvider, AIAgent> factory,
        TimeSpan? timeToLive = null) =>
        this.AddAIAgentFactory(name, factory, timeToLive, configureHistory: null);

    /// <summary>
    /// Adds an AI agent factory to the options and configures its history policy.
    /// </summary>
    /// <param name="name">The name of the agent.</param>
    /// <param name="factory">The factory function to create the agent.</param>
    /// <param name="timeToLive">Optional time-to-live for this agent's entities. If not specified, uses <see cref="DefaultTimeToLive"/>.</param>
    /// <param name="configureHistory">Configures history ownership and replay for this registration.</param>
    /// <returns>The options instance.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="name"/> or <paramref name="factory"/> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when an agent with the same name has already been registered explicitly.</exception>
    /// <remarks>
    /// The factory is not invoked for validation during registration. Its returned agent is validated once, before
    /// session restoration or model/provider execution, on each entity operation that needs to execute the agent.
    /// </remarks>
    public DurableAgentsOptions AddAIAgentFactory(
        string name,
        Func<IServiceProvider, AIAgent> factory,
        TimeSpan? timeToLive,
        Action<DurableAgentHistoryOptions>? configureHistory)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(factory);
        DurableAgentHistoryConfiguration historyConfiguration = CreateHistoryConfiguration(configureHistory);
        this.AddExplicitAgentFactory(name, factory, nameof(name));
        this._historyConfigurations[name] = historyConfiguration;
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
    /// <param name="timeToLive">Optional time-to-live for this agent's entities.</param>
    /// <returns>The options instance.</returns>
    public DurableAgentsOptions AddAIAgent(AIAgent agent, TimeSpan? timeToLive = null) =>
        this.AddAIAgent(agent, timeToLive, configureHistory: null);

    /// <summary>
    /// Adds an AI agent to the options and configures its history policy.
    /// </summary>
    /// <param name="agent">The agent to add.</param>
    /// <param name="timeToLive">Optional time-to-live for this agent's entities. If not specified, uses <see cref="DefaultTimeToLive"/>.</param>
    /// <param name="configureHistory">Configures history ownership and replay for this registration.</param>
    /// <returns>The options instance.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="agent"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="agent.Name"/> is null or whitespace or when an agent with the same name has already been registered explicitly.
    /// </exception>
    /// <remarks>
    /// Registering an agent that a workflow already discovered is allowed: the explicit registration takes over,
    /// so an agent can be promoted to a standalone agent regardless of whether the workflow was configured first.
    /// </remarks>
    public DurableAgentsOptions AddAIAgent(
        AIAgent agent,
        TimeSpan? timeToLive,
        Action<DurableAgentHistoryOptions>? configureHistory)
    {
        ArgumentNullException.ThrowIfNull(agent);

        if (string.IsNullOrWhiteSpace(agent.Name))
        {
            throw new ArgumentException($"{nameof(agent.Name)} must not be null or whitespace.", nameof(agent));
        }

        DurableAgentHistoryConfiguration historyConfiguration = CreateHistoryConfiguration(configureHistory);
        this.AddExplicitAgentFactory(agent.Name, sp => agent, nameof(agent));
        this._historyConfigurations[agent.Name] = historyConfiguration;
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
    /// <param name="revisedState">Whether receipt-aware state requires an explicit deletion policy and TTL.</param>
    /// <returns>The time-to-live for the agent, or the default TTL if not specified.</returns>
    internal TimeSpan? GetTimeToLive(string agentName, bool revisedState = false)
    {
        if (revisedState && !this.EnableMailboxEntityDeletion)
        {
            return null;
        }

        if (this._agentTimeToLive.TryGetValue(agentName, out TimeSpan? ttl))
        {
            return ttl;
        }

        // The legacy idle default must not silently delete completion evidence in revised state.
        return revisedState && !this._defaultTimeToLiveConfigured ? null : this.DefaultTimeToLive;
    }

    private static DurableAgentHistoryConfiguration CreateHistoryConfiguration(
        Action<DurableAgentHistoryOptions>? configure)
    {
        DurableAgentHistoryOptions options = new();
        configure?.Invoke(options);
        return new(
            options.ServiceManagedPerServiceCallHistory,
            options.ReplayMode,
            options.ProviderKey);
    }

    internal DurableAgentHistoryConfiguration GetHistoryConfiguration(string agentName)
    {
        return this._historyConfigurations.TryGetValue(agentName, out DurableAgentHistoryConfiguration configuration)
            ? configuration
            : DurableAgentHistoryConfiguration.Default;
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

internal readonly record struct DurableAgentRetentionSettings(
    DurableAgentHistoryRetentionMode Mode,
    int? MaxStateBytes,
    double HighWatermark,
    double LowWatermark);

internal readonly record struct DurableAgentHistoryConfiguration(
    bool ServiceManagedPerServiceCallHistory,
    DurableAgentHistoryReplayMode ReplayMode,
    DurableAgentHistoryProviderKey? ProviderKey)
{
    public static DurableAgentHistoryConfiguration Default { get; } = new(
        ServiceManagedPerServiceCallHistory: false,
        DurableAgentHistoryReplayMode.PreloadEntityHistory,
        ProviderKey: null);
}
