namespace LawnDart.Snapshots;

/// <summary>
/// A snapshot strategy that never triggers a snapshot write. This is the system-wide default.
/// </summary>
/// <remarks>
/// Using <see cref="NeverSnapshotStrategy"/> as the default ensures that no snapshot I/O
/// occurs unless a developer explicitly opts in for a specific aggregate or DCB state type.
/// This prevents hidden performance regressions on short-lived streams where snapshot write
/// overhead exceeds the load-time benefit.
/// <para>
/// Upstream snapshot benchmarks (not in this repository) show that snapshot load outperforms
/// full replay only for streams with approximately 400+ events, depending on state object
/// complexity. Below that threshold, the snapshot file write cost exceeds the replay savings.
/// </para>
/// </remarks>
public sealed class NeverSnapshotStrategy : ISnapshotStrategy
{
    /// <summary>The singleton instance. Use this instead of constructing a new one.</summary>
    public static readonly NeverSnapshotStrategy Instance = new();

    /// <inheritdoc/>
    public bool ShouldSnapshot(SnapshotContext context) => false;
}
