namespace LawnDart.Snapshots;

/// <summary>
/// Default implementation of <see cref="ISnapshotStrategyResolver"/> backed by two
/// type-keyed dictionaries — one for aggregate types, one for DCB state types.
/// Unregistered types fall back to <see cref="NeverSnapshotStrategy"/>.
/// </summary>
/// <remarks>
/// Intended to be constructed and configured during <c>Startup</c> / <c>Program.cs</c> via
/// the <c>.WithSnapshots(config => { ... })</c> builder extension on the event store
/// registration, then registered as a singleton in the DI container.
/// <example>
/// <code>
/// services.AddBoundedContext("default").UseInMemory()
///         .WithSnapshots(config =>
///         {
///             config.RegisterForAggregate&lt;OrderAggregate&gt;(new EventCountSnapshotStrategy(500));
///             config.RegisterForDcb&lt;InventoryEntity&gt;(new EventCountSnapshotStrategy(500));
///         });
/// </code>
/// </example>
/// </remarks>
public sealed class SnapshotStrategyResolver : ISnapshotStrategyResolver
{
    private readonly Dictionary<Type, ISnapshotStrategy> _aggregate = new();
    private readonly Dictionary<Type, ISnapshotStrategy> _dcb = new();
    private ISnapshotStrategy _globalDefault = NeverSnapshotStrategy.Instance;

    /// <summary>
    /// Sets a global fallback strategy applied to any aggregate or DCB type that does not have
    /// a type-specific registration. Useful for enabling a baseline strategy (e.g.
    /// <see cref="EventCountSnapshotStrategy"/>) for all types via configuration without
    /// enumerating each type individually.
    /// </summary>
    /// <remarks>
    /// Type-specific registrations (via <see cref="RegisterForAggregate{T}"/> /
    /// <see cref="RegisterForDcb{TState}"/>) always take precedence over the global default.
    /// If no global default is set, <see cref="NeverSnapshotStrategy"/> is used.
    /// </remarks>
    public void SetGlobalDefault(ISnapshotStrategy strategy)
        => _globalDefault = strategy ?? throw new ArgumentNullException(nameof(strategy));

    /// <summary>
    /// Registers a strategy for the specified aggregate type, overriding any prior registration.
    /// </summary>
    public void RegisterForAggregate<T>(ISnapshotStrategy strategy)
        => _aggregate[typeof(T)] = strategy ?? throw new ArgumentNullException(nameof(strategy));

    /// <summary>
    /// Registers a strategy for the specified DCB entity type, overriding any prior registration.
    /// </summary>
    /// <remarks>
    /// <c>DcbRepository.HandleCommandAsync</c> resolves the strategy by
    /// <c>typeof(TEntity)</c>. Pass the entity class (for example
    /// <c>InventoryEntity</c>), not the <c>IState</c> class.
    /// </remarks>
    public void RegisterForDcb<TState>(ISnapshotStrategy strategy)
        => _dcb[typeof(TState)] = strategy ?? throw new ArgumentNullException(nameof(strategy));

    /// <inheritdoc/>
    public ISnapshotStrategy ResolveForAggregate(Type aggregateType)
        => _aggregate.TryGetValue(aggregateType, out var strategy) ? strategy : _globalDefault;

    /// <inheritdoc/>
    public ISnapshotStrategy ResolveForDcb(Type dcbStateType)
        => _dcb.TryGetValue(dcbStateType, out var strategy) ? strategy : _globalDefault;
}
