namespace LawnDart.Snapshots;

/// <summary>
/// Administrative operations for the snapshot subsystem.
/// Provides stream-level and store-level snapshot management for operational tooling,
/// migration scripts, and test infrastructure.
/// </summary>
/// <remarks>
/// Deleting snapshots never affects event data — the event log is immutable and always
/// the source of truth. A deleted snapshot causes the repository to fall back to full
/// event replay on the next load, which is always correct.
/// <para>
/// The primary operational use cases are:
/// <list type="bullet">
/// <item>Clearing snapshots after a bug in state serialization that produced corrupt snapshots.</item>
/// <item>Clearing all snapshots after an aggregate or DCB state schema migration.</item>
/// <item>Auditing which streams have snapshots before deciding to clear them.</item>
/// </list>
/// </para>
/// </remarks>
public interface ISnapshotAdmin
{
    /// <summary>
    /// Deletes all snapshots for a single stream. The next load of this aggregate will
    /// perform a full event replay from the beginning of the stream.
    /// </summary>
    /// <param name="streamId">The stream identifier whose snapshots should be cleared.</param>
    /// <param name="ct">Cancellation token.</param>
    Task DeleteSnapshotAsync(string streamId, CancellationToken ct = default);

    /// <summary>
    /// Deletes all snapshots for all streams in this store.
    /// Use after an aggregate or DCB state schema migration where existing snapshots
    /// are incompatible with the new state shape.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    Task DeleteAllSnapshotsAsync(CancellationToken ct = default);

    /// <summary>
    /// Returns snapshot metadata for a single stream without deserializing the payload.
    /// Returns <c>null</c> if no snapshot exists for the stream.
    /// </summary>
    /// <param name="streamId">The stream identifier to inspect.</param>
    /// <param name="ct">Cancellation token.</param>
    Task<SnapshotInfo?> GetSnapshotInfoAsync(string streamId, CancellationToken ct = default);

    /// <summary>
    /// Enumerates the stream identifiers of all streams that currently have at least one
    /// non-tombstoned snapshot. Useful for auditing snapshot coverage before a bulk delete.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    IAsyncEnumerable<string> EnumerateSnapshotStreamsAsync(CancellationToken ct = default);
}
