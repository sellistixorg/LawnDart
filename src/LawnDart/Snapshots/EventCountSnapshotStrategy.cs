namespace LawnDart.Snapshots;

/// <summary>
/// A snapshot strategy that triggers a write every time the number of events applied
/// since the last snapshot reaches the configured <paramref name="threshold"/>.
/// </summary>
/// <remarks>
/// This is the simplest strategy and works well for aggregates with a predictable,
/// high event rate. For streams with infrequent events spread over long periods,
/// consider <see cref="DynamicSnapshotStrategy"/> to add a time-based trigger.
/// <para>
/// Upstream snapshot benchmarks (not in this repository) show that for most state object
/// sizes, the crossover where snapshot load outperforms full replay is approximately
/// 400–1,000 events. Setting <paramref name="threshold"/> below this range will likely
/// <b>degrade</b> load performance. Run the benchmark against your specific state type
/// to find the correct threshold before enabling this strategy.
/// </para>
/// </remarks>
/// <param name="threshold">
/// The number of events that must be applied since the last snapshot before a new one
/// is triggered. Must be greater than zero.
/// </param>
public sealed class EventCountSnapshotStrategy(int threshold) : ISnapshotStrategy
{
    private readonly int _threshold = threshold > 0
        ? threshold
        : throw new ArgumentOutOfRangeException(nameof(threshold), "Threshold must be greater than zero.");

    /// <inheritdoc/>
    public bool ShouldSnapshot(SnapshotContext context) =>
        context.EventsSinceLastSnapshot >= _threshold;
}
