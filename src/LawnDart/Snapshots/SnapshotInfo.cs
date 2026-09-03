namespace LawnDart.Snapshots;

/// <summary>
/// Metadata about an existing snapshot returned alongside the deserialized state.
/// Provides the information needed to compute <see cref="SnapshotContext.EventsSinceLastSnapshot"/>
/// and <see cref="SnapshotContext.LastSnapshotUtc"/> without additional store queries.
/// </summary>
/// <param name="Version">The stream version (event count) at the time the snapshot was taken.</param>
/// <param name="GlobalSequence">The global event store sequence position at snapshot time.</param>
/// <param name="TakenAtUtc">UTC timestamp when the snapshot was written.</param>
public record SnapshotInfo(long Version, long GlobalSequence, DateTime TakenAtUtc);
