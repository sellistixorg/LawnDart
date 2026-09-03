namespace LawnDart.Snapshots;

/// <summary>
/// Contextual information passed to <see cref="ISnapshotStrategy.ShouldSnapshot"/>
/// so the strategy can make a data-driven decision.
/// </summary>
/// <remarks>
/// All values are derived from durable sources (the stream, the snapshot index) so the
/// context can be constructed correctly even after a process restart. Strategies should be
/// pure functions of this record and may be safely registered as DI singletons.
/// </remarks>
/// <param name="StreamId">
/// The stream or DCB identifier being evaluated.
/// </param>
/// <param name="CurrentVersion">
/// The stream version (event count) after the most recent append.
/// For DCB, this is the total number of events matched by the consistency boundary.
/// </param>
/// <param name="EventsSinceLastSnapshot">
/// Number of events applied since the last snapshot was written.
/// Computed as <c>CurrentVersion - snapshotVersion</c> when a snapshot exists,
/// or <c>CurrentVersion</c> when no snapshot has ever been taken.
/// </param>
/// <param name="LastSnapshotUtc">
/// UTC timestamp when the most recent snapshot was written.
/// <c>null</c> when no snapshot has ever been taken for this stream.
/// </param>
/// <param name="GlobalSequence">
/// Global event store sequence position of the last appended event.
/// </param>
public record SnapshotContext(
    string StreamId,
    long CurrentVersion,
    long EventsSinceLastSnapshot,
    DateTime? LastSnapshotUtc,
    long GlobalSequence);
