namespace LawnDart.Snapshots;

/// <summary>
/// A snapshot strategy that triggers a write when either the event count since the last
/// snapshot reaches <paramref name="eventThreshold"/>, or the elapsed time since the last
/// snapshot exceeds <paramref name="timeThreshold"/> — whichever fires first.
/// </summary>
/// <remarks>
/// This strategy is recommended over <see cref="EventCountSnapshotStrategy"/> for aggregates
/// with irregular event rates. A stream receiving a trickle of events may never reach the
/// event count threshold; the time leg ensures that even slow-moving aggregates eventually
/// get snapshotted and avoid full replay on every cold load.
/// <para>
/// When no snapshot has ever been taken (<see cref="SnapshotContext.LastSnapshotUtc"/> is
/// <c>null</c>), only the event count leg is evaluated. This prevents a single initial
/// snapshot from firing immediately on the first append regardless of the time threshold.
/// </para>
/// <para>
/// Upstream snapshot benchmarks (not in this repository) show that for most state object
/// sizes, the crossover where snapshot load outperforms full replay is approximately
/// 400–1,000 events. Setting <paramref name="eventThreshold"/> below this range risks
/// degrading load performance. Run the benchmark against your specific state type first.
/// </para>
/// </remarks>
/// <param name="eventThreshold">
/// Number of events since the last snapshot before a new one is triggered.
/// Must be greater than zero.
/// </param>
/// <param name="timeThreshold">
/// Maximum time that may elapse since the last snapshot before a new one is triggered.
/// Only evaluated when a prior snapshot exists. Must be a positive duration.
/// </param>
public sealed class DynamicSnapshotStrategy(int eventThreshold, TimeSpan timeThreshold) : ISnapshotStrategy
{
    private readonly int _eventThreshold = eventThreshold > 0
        ? eventThreshold
        : throw new ArgumentOutOfRangeException(nameof(eventThreshold), "Event threshold must be greater than zero.");

    private readonly TimeSpan _timeThreshold = timeThreshold > TimeSpan.Zero
        ? timeThreshold
        : throw new ArgumentOutOfRangeException(nameof(timeThreshold), "Time threshold must be a positive duration.");

    /// <inheritdoc/>
    public bool ShouldSnapshot(SnapshotContext context)
    {
        if (context.EventsSinceLastSnapshot >= _eventThreshold)
            return true;

        // Only evaluate the time leg when a prior snapshot exists.
        // This avoids triggering immediately on the first append for streams with a
        // low event rate and a short time threshold.
        if (context.LastSnapshotUtc.HasValue &&
            DateTime.UtcNow - context.LastSnapshotUtc.Value >= _timeThreshold)
            return true;

        return false;
    }
}
