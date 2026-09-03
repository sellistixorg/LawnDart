namespace LawnDart.Snapshots;

/// <summary>
/// Provides durable snapshot storage for stream-based aggregates.
/// </summary>
/// <remarks>
/// Snapshots are correctness-optional performance hints. A missing or corrupt snapshot
/// causes the repository to fall back to full event replay — correctness is never at risk.
/// <para>
/// Snapshot writes occur fire-and-forget after a successful append: they are never on the
/// command hot path. Only read (load) performance is affected by whether a snapshot exists.
/// </para>
/// <para>
/// The default system-wide strategy is <see cref="NeverSnapshotStrategy"/>, which means
/// <see cref="SaveSnapshotAsync{TState}"/> is never called unless an explicit
/// <see cref="ISnapshotStrategy"/> is registered via <see cref="ISnapshotStrategyResolver"/>
/// for the aggregate type.
/// </para>
/// </remarks>
public interface ISnapshotStore
{
    /// <summary>
    /// Loads the most recent valid snapshot for <paramref name="streamId"/>.
    /// Returns <c>(default, null)</c> when no snapshot exists or the snapshot fails
    /// checksum validation (triggering a full event replay in the repository).
    /// </summary>
    /// <typeparam name="TState">The aggregate state type to deserialize into.</typeparam>
    /// <param name="streamId">The stream identifier for the aggregate.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>
    /// A tuple of the deserialized state and its associated <see cref="SnapshotInfo"/>,
    /// or <c>(default, null)</c> if no valid snapshot is available.
    /// </returns>
    Task<(TState? State, SnapshotInfo? Info)> LoadSnapshotAsync<TState>(
        string streamId,
        CancellationToken ct = default);

    /// <summary>
    /// Persists a snapshot of <paramref name="state"/> for <paramref name="streamId"/>
    /// at the given stream <paramref name="version"/> and global <paramref name="globalSequence"/>.
    /// </summary>
    /// <typeparam name="TState">The aggregate state type to serialize.</typeparam>
    /// <param name="streamId">The stream identifier for the aggregate.</param>
    /// <param name="version">Current stream version (number of events applied).</param>
    /// <param name="globalSequence">Global event store sequence at the time of this snapshot.</param>
    /// <param name="state">The aggregate state to serialize and store.</param>
    /// <param name="ct">Cancellation token.</param>
    Task SaveSnapshotAsync<TState>(
        string streamId,
        long version,
        long globalSequence,
        TState state,
        CancellationToken ct = default);
}
