namespace LawnDart.Snapshots;

/// <summary>
/// Determines whether a snapshot should be written after a successful event append.
/// </summary>
/// <remarks>
/// Strategies are pure functions of <see cref="SnapshotContext"/> and carry no per-stream
/// state. They are safe to register as DI singletons and reuse across many aggregate instances.
/// <para>
/// The system-wide default is <see cref="NeverSnapshotStrategy"/>, which means no snapshots
/// are written unless a strategy is explicitly registered for an aggregate or DCB state type
/// via <see cref="ISnapshotStrategyResolver"/>.
/// </para>
/// <para>
/// Snapshot writes are always fire-and-forget — they never add latency to the command hot path.
/// A strategy returning <c>true</c> triggers a background write; no strategy result ever blocks
/// <c>AppendAsync</c> from returning.
/// </para>
/// </remarks>
public interface ISnapshotStrategy
{
    /// <summary>
    /// Returns <c>true</c> if a snapshot should be written based on the provided context.
    /// Called after each successful event append.
    /// </summary>
    /// <param name="context">Contextual information about the current stream state.</param>
    bool ShouldSnapshot(SnapshotContext context);
}
