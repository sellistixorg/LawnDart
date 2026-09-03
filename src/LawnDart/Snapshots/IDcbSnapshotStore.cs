namespace LawnDart.Snapshots;

/// <summary>
/// Provides durable snapshot storage for DCB (Dynamic Consistency Boundary) entities.
/// </summary>
/// <remarks>
/// DCB snapshots differ from stream snapshots in one critical respect: the
/// <c>ConsistencyMarker</c> stored alongside the snapshot <b>must never be reused directly</b>
/// after restore. The consistency boundary may have shifted since the snapshot was taken.
/// The repository must replay delta events (those committed after <see cref="SnapshotInfo.GlobalSequence"/>)
/// and recompute a fresh marker from those deltas before the next conditional append.
/// <para>
/// The default system-wide strategy is <see cref="NeverSnapshotStrategy"/>, which means
/// <see cref="SaveDcbSnapshotAsync{TState}"/> is never called unless an explicit
/// <see cref="ISnapshotStrategy"/> is registered via <see cref="ISnapshotStrategyResolver"/>
/// for the DCB entity type used as <c>TEntity</c> in <c>HandleCommandAsync</c>.
/// </para>
/// </remarks>
public interface IDcbSnapshotStore
{
    /// <summary>
    /// Loads the most recent valid DCB snapshot for <paramref name="dcbId"/>.
    /// Returns <c>(default, null)</c> when no snapshot exists or checksum validation fails.
    /// </summary>
    /// <typeparam name="TState">The DCB state type to deserialize into.</typeparam>
    /// <param name="dcbId">Hashed identity from <c>DcbSnapshotId.FromLoadTags</c> (64-char SHA-256 hex). Do not join tags with <c>|</c>.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>
    /// A tuple of the deserialized state and its associated <see cref="SnapshotInfo"/>,
    /// or <c>(default, null)</c> if no valid snapshot is available.
    /// </returns>
    Task<(TState? State, SnapshotInfo? Info)> LoadDcbSnapshotAsync<TState>(
        string dcbId,
        CancellationToken ct = default);

    /// <summary>
    /// Persists a DCB snapshot of <paramref name="state"/> at the given global <paramref name="globalSequence"/>.
    /// </summary>
    /// <typeparam name="TState">The DCB state type to serialize.</typeparam>
    /// <param name="dcbId">Hashed identity from <c>DcbSnapshotId.FromLoadTags</c> (64-char SHA-256 hex).</param>
    /// <param name="globalSequence">Global event store sequence at the time of this snapshot.</param>
    /// <param name="state">The DCB state to serialize and store.</param>
    /// <param name="consistencyMarker">
    /// Optional consistency marker at snapshot time. Stored for informational purposes only;
    /// the repository must recompute a fresh marker after delta replay on restore.
    /// </param>
    /// <param name="loadTags">
    /// Optional plaintext load-query tags persisted beside the hashed <paramref name="dcbId"/>
    /// for operations. Not used as the snapshot key.
    /// </param>
    /// <param name="ct">Cancellation token.</param>
    Task SaveDcbSnapshotAsync<TState>(
        string dcbId,
        long globalSequence,
        TState state,
        byte[]? consistencyMarker = null,
        IReadOnlyList<string>? loadTags = null,
        CancellationToken ct = default);
}
