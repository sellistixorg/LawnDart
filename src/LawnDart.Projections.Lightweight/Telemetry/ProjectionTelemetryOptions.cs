namespace LawnDart.Projections.Telemetry;

/// <summary>
/// Toggleable OpenTelemetry metrics for Lightweight / projection pipelines.
/// Flags are process-wide once applied via <see cref="ProjectionTelemetry.Configure"/>.
/// </summary>
/// <remarks>
/// Azure Monitor tip: if metric cardinality or ingestion cost spikes, disable
/// <see cref="EnableCacheMetrics"/> and <see cref="EnableHighDetailTags"/> first;
/// keep <see cref="EnableFlushMetrics"/> on for durable-write health.
/// </remarks>
public sealed class ProjectionTelemetryOptions
{
    /// <summary>
    /// Enables working-set size/eviction gauges and <c>projections.read.stale_rejected</c>.
    /// <para>Default: <see langword="true"/>.</para>
    /// </summary>
    public bool EnableExtendedMetrics { get; set; } = true;

    /// <summary>
    /// Enables read/cache counters (<c>projections.read.*</c> hit/miss/total).
    /// <para>Default: <see langword="true"/>.</para>
    /// </summary>
    public bool EnableCacheMetrics { get; set; } = true;

    /// <summary>
    /// Enables flush histograms/counters and checkpoint lag.
    /// <para>Default: <see langword="true"/>.</para>
    /// </summary>
    public bool EnableFlushMetrics { get; set; } = true;

    /// <summary>
    /// When <see langword="true"/>, <c>projections.read.total</c> includes a low-cardinality
    /// <c>source</c> tag (<c>memory|sql|redis</c>). Never tags instance/stream/tenant ids.
    /// <para>Default: <see langword="false"/>.</para>
    /// </summary>
    public bool EnableHighDetailTags { get; set; }
}
