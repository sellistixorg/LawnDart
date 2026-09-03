using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace LawnDart.Snapshots.Telemetry;

/// <summary>
/// OpenTelemetry instrumentation for snapshot operations.
/// </summary>
/// <remarks>
/// <para>
/// <b>Meter name:</b> <c>LawnDart.Snapshots</c>
/// </para>
/// <para>
/// Instruments:
/// <list type="bullet">
///   <item><c>snapshot.hits_total</c> — Counter(long): successful restores from a snapshot; tags: entity_type, kind</item>
///   <item><c>snapshot.misses_total</c> — Counter(long): fallbacks to full replay after a failed restore; tags: entity_type, kind</item>
///   <item><c>snapshot.delta_events</c> — Histogram(long): events replayed on top of a restored snapshot; tags: entity_type, kind</item>
///   <item><c>snapshot.writes_total</c> — Counter(long): fire-and-forget snapshot writes completed; tags: entity_type, kind</item>
///   <item><c>snapshot.restore_duration_ms</c> — Histogram(double): elapsed time of the restore attempt in milliseconds</item>
///   <item><c>snapshot.write_duration_ms</c> — Histogram(double): elapsed time of the snapshot write in milliseconds</item>
/// </list>
/// </para>
/// <para>
/// Tags used across instruments:
/// <list type="bullet">
///   <item><b>entity_type</b> — the short type name of the aggregate or DCB state being snapshotted</item>
///   <item><b>kind</b> — either <c>aggregate</c> or <c>dcb</c></item>
/// </list>
/// </para>
/// </remarks>
public static class SnapshotTelemetry
{
    internal const string SourceName = "LawnDart.Snapshots";

    private static readonly Meter _meter = new(SourceName, "1.0.0");

    private static readonly Counter<long> _hits =
        _meter.CreateCounter<long>(
            "snapshot.hits_total",
            description: "Number of successful aggregate or DCB state restores from a snapshot.");

    private static readonly Counter<long> _misses =
        _meter.CreateCounter<long>(
            "snapshot.misses_total",
            description: "Number of snapshot restore failures that fell back to full event replay.");

    private static readonly Counter<long> _writes =
        _meter.CreateCounter<long>(
            "snapshot.writes_total",
            description: "Number of fire-and-forget snapshot writes that completed successfully.");

    private static readonly Histogram<long> _deltaEvents =
        _meter.CreateHistogram<long>(
            "snapshot.delta_events",
            unit: "events",
            description: "Number of events replayed on top of a restored snapshot to reach current state.");

    private static readonly Histogram<double> _restoreDuration =
        _meter.CreateHistogram<double>(
            "snapshot.restore_duration_ms",
            unit: "ms",
            description: "Elapsed time of the snapshot restore attempt, including deserialization, in milliseconds.");

    private static readonly Histogram<double> _writeDuration =
        _meter.CreateHistogram<double>(
            "snapshot.write_duration_ms",
            unit: "ms",
            description: "Elapsed time of the fire-and-forget snapshot write, including serialization and I/O, in milliseconds.");

    // ── Recording helpers ─────────────────────────────────────────────────────

    /// <summary>
    /// Records a successful snapshot restore hit plus the number of delta events
    /// replayed on top of the restored state.
    /// </summary>
    /// <param name="entityType">Short type name of the aggregate or DCB state.</param>
    /// <param name="kind"><c>aggregate</c> or <c>dcb</c>.</param>
    /// <param name="deltaEventCount">Events replayed after the snapshot was applied.</param>
    /// <param name="restoreDuration">Time taken for the restore operation.</param>
    public static void RecordHit(string entityType, string kind, long deltaEventCount, TimeSpan restoreDuration)
    {
        var tags = BuildTags(entityType, kind);
        _hits.Add(1, tags);
        _deltaEvents.Record(deltaEventCount, tags);
        _restoreDuration.Record(restoreDuration.TotalMilliseconds, tags);
    }

    /// <summary>
    /// Records a snapshot restore miss — restore was attempted but failed (corrupt or absent),
    /// so the caller fell back to full event replay.
    /// </summary>
    /// <param name="entityType">Short type name of the aggregate or DCB state.</param>
    /// <param name="kind"><c>aggregate</c> or <c>dcb</c>.</param>
    /// <param name="restoreDuration">Time taken before the restore was abandoned.</param>
    public static void RecordMiss(string entityType, string kind, TimeSpan restoreDuration)
    {
        var tags = BuildTags(entityType, kind);
        _misses.Add(1, tags);
        _restoreDuration.Record(restoreDuration.TotalMilliseconds, tags);
    }

    /// <summary>
    /// Records a successful fire-and-forget snapshot write.
    /// </summary>
    /// <param name="entityType">Short type name of the aggregate or DCB state.</param>
    /// <param name="kind"><c>aggregate</c> or <c>dcb</c>.</param>
    /// <param name="writeDuration">Time taken to serialize and persist the snapshot.</param>
    public static void RecordWrite(string entityType, string kind, TimeSpan writeDuration)
    {
        var tags = BuildTags(entityType, kind);
        _writes.Add(1, tags);
        _writeDuration.Record(writeDuration.TotalMilliseconds, tags);
    }

    private static KeyValuePair<string, object?>[] BuildTags(string entityType, string kind) =>
    [
        new("entity_type", entityType),
        new("kind",        kind)
    ];
}
