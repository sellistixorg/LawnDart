namespace LawnDart.Snapshots;

/// <summary>
/// Resolves the <see cref="ISnapshotStrategy"/> to use for a given aggregate or DCB entity type.
/// </summary>
/// <remarks>
/// Strategies are registered per concrete type at startup via
/// <see cref="SnapshotStrategyResolver.RegisterForAggregate{T}"/> or
/// <see cref="SnapshotStrategyResolver.RegisterForDcb{TState}"/>. Types without an explicit
/// registration fall back to <see cref="NeverSnapshotStrategy"/>, ensuring that snapshotting
/// is always opt-in.
/// </remarks>
public interface ISnapshotStrategyResolver
{
    /// <summary>
    /// Returns the strategy registered for <paramref name="aggregateType"/>, or
    /// <see cref="NeverSnapshotStrategy"/> if no registration exists.
    /// </summary>
    ISnapshotStrategy ResolveForAggregate(Type aggregateType);

    /// <summary>
    /// Returns the strategy registered for <paramref name="dcbStateType"/> (the DCB
    /// entity type passed to <c>HandleCommandAsync</c>), or
    /// <see cref="NeverSnapshotStrategy"/> if no registration exists.
    /// </summary>
    ISnapshotStrategy ResolveForDcb(Type dcbStateType);
}
