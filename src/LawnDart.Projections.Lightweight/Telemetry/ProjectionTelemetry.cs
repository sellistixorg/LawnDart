using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace LawnDart.Projections.Telemetry;

/// <summary>
/// OpenTelemetry instrumentation for projection system.
/// Provides distributed tracing and metrics.
/// </summary>
public static class ProjectionTelemetry
{
    public const string ActivitySourceName = "LawnDart.Projections";
    public const string MeterName = "LawnDart.Projections";

    private static readonly ActivitySource ActivitySource = new(ActivitySourceName, "1.0.0");
    private static readonly Meter Meter = new(MeterName, "1.0.0");

    private static ProjectionTelemetryOptions _options = new();

    // Counters
    private static readonly Counter<long> EventsProcessedCounter = Meter.CreateCounter<long>(
        "projections.events.processed",
        description: "Total number of events processed by projections");

    private static readonly Counter<long> EventsFailedCounter = Meter.CreateCounter<long>(
        "projections.events.failed",
        description: "Total number of events that failed processing");

    private static readonly Counter<long> CheckpointsSavedCounter = Meter.CreateCounter<long>(
        "projections.checkpoints.saved",
        description: "Total number of checkpoints saved");

    private static readonly Counter<long> FlushErrorsCounter = Meter.CreateCounter<long>(
        "projections.flush.errors",
        description: "Failed barriered view/checkpoint flushes");

    private static readonly Counter<long> ReadTotalCounter = Meter.CreateCounter<long>(
        "projections.read.total",
        description: "Projection GET requests that resolved a view");

    private static readonly Counter<long> ReadCacheHitCounter = Meter.CreateCounter<long>(
        "projections.read.cache.hit",
        description: "Projection GETs served from runner memory or hot cache");

    private static readonly Counter<long> ReadCacheMissCounter = Meter.CreateCounter<long>(
        "projections.read.cache.miss",
        description: "Projection GETs that fell through to the durable view store");

    private static readonly Counter<long> ReadStaleRejectedCounter = Meter.CreateCounter<long>(
        "projections.read.stale_rejected",
        description: "Projection GETs rejected by minSequence freshness check");

    private static readonly Counter<long> WorkingSetEvictionsCounter = Meter.CreateCounter<long>(
        "projections.workingset.evictions",
        description: "Clean projection instances evicted from the runner working set");

    // Histograms
    private static readonly Histogram<double> EventProcessingDuration = Meter.CreateHistogram<double>(
        "projections.event.processing.duration",
        unit: "ms",
        description: "Time taken to process an event");

    private static readonly Histogram<double> PollDuration = Meter.CreateHistogram<double>(
        "projections.poll.duration",
        unit: "ms",
        description: "Time taken to poll for new events");

    private static readonly Histogram<double> FlushDuration = Meter.CreateHistogram<double>(
        "projections.flush.duration",
        unit: "ms",
        description: "Duration of a barriered view flush + checkpoint");

    private static readonly Histogram<int> FlushViews = Meter.CreateHistogram<int>(
        "projections.flush.views",
        description: "Dirty view count persisted in a barriered flush");

    private static readonly Histogram<long> CheckpointLag = Meter.CreateHistogram<long>(
        "projections.checkpoint.lag",
        description: "Event-store head minus last processed checkpoint position");

    private static readonly Histogram<double> PipelineLagHistogram = Meter.CreateHistogram<double>(
        "projections.lag.pipeline",
        unit: "ms",
        description: "Time from event commit to projection processing (actual pipeline latency)");

    private static readonly Histogram<double> PipelineProjectionLagHistogram = Meter.CreateHistogram<double>(
        "projections.lag.pipeline.projection",
        unit: "ms",
        description: "Time from event poll to projection processing (actual projection latency)");

    private static readonly Histogram<int> ProjectionInstanceMailboxSize = Meter.CreateHistogram<int>(
        "projections.instance.mailboxsize",
        description: "Size of mailbox");

    private static readonly Histogram<int> ProjectionCoordinatorMailboxSize = Meter.CreateHistogram<int>(
        "projections.coordinator.mailboxsize",
        description: "Size of mailbox");

    private static readonly Histogram<int> ProjectionSupervisorMailboxSize = Meter.CreateHistogram<int>(
        "projections.supervisor.mailboxsize",
        description: "Size of mailbox");

    private static readonly Histogram<int> ProjectionStatsAggregatorMailboxSize = Meter.CreateHistogram<int>(
        "projections.statsaggregator.mailboxsize",
        description: "Size of mailbox");

    // Gauges (Observable)
    private static long _activeProjectionInstances = 0;
    private static long _totalLagMs = 0;
    private static long _maxPipelineLagMs = 0;
    private static long _maxPipelineProjectionLagMs = 0;
    private static long _averagePipelineLagMs = 0;
    private static long _averagePipelineProjectionLagMs = 0;
    private static readonly ConcurrentDictionary<string, long> WorkingSetSizes = new(StringComparer.Ordinal);

    static ProjectionTelemetry()
    {
        Meter.CreateObservableGauge(
            "projections.instances.active",
            () => _activeProjectionInstances,
            description: "Number of active projection instances");

        Meter.CreateObservableGauge(
            "projections.lag.total",
            () => _totalLagMs,
            unit: "ms",
            description: "Total lag across all projections");

        Meter.CreateObservableGauge(
            "projections.lag.pipeline.max",
            () => _maxPipelineLagMs,
            unit: "ms",
            description: "Maximum pipeline lag across all projection instances");

        Meter.CreateObservableGauge(
            "projections.lag.pipeline.projection.max",
            () => _maxPipelineProjectionLagMs,
            unit: "ms",
            description: "Maximum pipeline projection lag across all projection instances");

        Meter.CreateObservableGauge(
            "projections.lag.pipeline.average",
            () => _averagePipelineLagMs,
            unit: "ms",
            description: "Average pipeline lag across all projection instances");

        Meter.CreateObservableGauge(
            "projections.lag.pipeline.projection.average",
            () => _averagePipelineProjectionLagMs,
            unit: "ms",
            description: "Average pipeline projection lag across all projection instances");

        Meter.CreateObservableGauge(
            "projections.workingset.size",
            ObserveWorkingSetSizes,
            description: "In-memory projection instance count per projection type");
    }

    /// <summary>
    /// Applies process-wide metric toggles. Called from Lightweight DI registration.
    /// </summary>
    public static void Configure(ProjectionTelemetryOptions? options) =>
        _options = options ?? new ProjectionTelemetryOptions();

    /// <summary>Current process-wide telemetry options (tests / diagnostics).</summary>
    public static ProjectionTelemetryOptions Options => _options;

    private static IEnumerable<Measurement<long>> ObserveWorkingSetSizes()
    {
        if (!_options.EnableExtendedMetrics)
            yield break;

        foreach (var (projectionType, size) in WorkingSetSizes)
        {
            yield return new Measurement<long>(
                size,
                new KeyValuePair<string, object?>("projection.type", projectionType));
        }
    }

    /// <summary>
    /// Updates the active instance count metric.
    /// </summary>
    public static void SetActiveInstances(long count)
    {
        _activeProjectionInstances = count;
    }

    /// <summary>
    /// Updates the total lag metric.
    /// </summary>
    public static void SetTotalLag(long lagMs)
    {
        _totalLagMs = lagMs;
    }

    /// <summary>
    /// Starts a new activity for event processing.
    /// </summary>
    public static Activity? StartEventProcessingActivity(string projectionType, string instanceId, string eventType)
    {
        var activity = ActivitySource.StartActivity("ProcessEvent", ActivityKind.Internal);
        activity?.SetTag("projection.type", projectionType);
        activity?.SetTag("projection.instance", instanceId);
        activity?.SetTag("event.type", eventType);
        return activity;
    }

    /// <summary>
    /// Starts a new activity for polling.
    /// </summary>
    public static Activity? StartPollingActivity(string projectionType, int nodeInstance)
    {
        var activity = ActivitySource.StartActivity("PollEvents", ActivityKind.Internal);
        activity?.SetTag("projection.type", projectionType);
        activity?.SetTag("node.instance", nodeInstance);
        return activity;
    }

    /// <summary>
    /// Records an event processing success.
    /// </summary>
    public static void RecordEventProcessed(string projectionType, string eventType, double durationMs)
    {
        EventsProcessedCounter.Add(1,
            new KeyValuePair<string, object?>("projection.type", projectionType),
            new KeyValuePair<string, object?>("event.type", eventType));

        // Duration tags: projection.type only (avoid event.type cardinality on the histogram).
        EventProcessingDuration.Record(durationMs,
            new KeyValuePair<string, object?>("projection.type", projectionType));
    }

    /// <summary>
    /// Records an event processing failure.
    /// </summary>
    public static void RecordEventFailed(string projectionType, string eventType, string errorType)
    {
        EventsFailedCounter.Add(1,
            new KeyValuePair<string, object?>("projection.type", projectionType),
            new KeyValuePair<string, object?>("event.type", eventType),
            new KeyValuePair<string, object?>("error.type", errorType));
    }

    /// <summary>
    /// Records a checkpoint save.
    /// </summary>
    public static void RecordCheckpointSaved(string projectionType, long sequencePosition)
    {
        CheckpointsSavedCounter.Add(1,
            new KeyValuePair<string, object?>("projection.type", projectionType));
    }

    /// <summary>
    /// Records poll duration.
    /// </summary>
    public static void RecordPollDuration(string projectionType, double durationMs, int streamsProcessed)
    {
        PollDuration.Record(durationMs,
            new KeyValuePair<string, object?>("projection.type", projectionType));
    }

    /// <summary>
    /// Records mailbox size.
    /// </summary>
    public static void RecordProjectionInstanceMailboxSize(string projectionId, int size)
    {
        ProjectionInstanceMailboxSize.Record(size,
            new KeyValuePair<string, object?>("instance.id", projectionId));
    }

    /// <summary>
    /// Records mailbox size.
    /// </summary>
    public static void RecordProjectionCoordinatorMailboxSize(string projectionId, int size)
    {
        ProjectionCoordinatorMailboxSize.Record(size,
            new KeyValuePair<string, object?>("id", projectionId));
    }

    /// <summary>
    /// Records mailbox size.
    /// </summary>
    public static void RecordProjectionSupervisorMailboxSize(string projectionId, int size)
    {
        ProjectionSupervisorMailboxSize.Record(size,
            new KeyValuePair<string, object?>("id", projectionId));
    }

    /// <summary>
    /// Records mailbox size.
    /// </summary>
    public static void RecordProjectionStatsAggregatorMailboxSize(string projectionId, int size)
    {
        ProjectionStatsAggregatorMailboxSize.Record(size,
            new KeyValuePair<string, object?>("id", projectionId));
    }

    /// <summary>
    /// Records pipeline lag (time from event commit to projection processing).
    /// This is the key metric for verifying performance objectives.
    /// </summary>
    public static void RecordPipelineLag(string projectionType, double lagMs, string instanceId)
    {
        // Record to histogram for percentile calculations in Aspire (p50, p95, p99)
        // NOTE: Only use low-cardinality dimensions to avoid hitting OpenTelemetry cardinality limits
        // With 10,000+ instances, instance.id would exceed limits and cause metric drops
        PipelineLagHistogram.Record(lagMs,
            new KeyValuePair<string, object?>("projection.type", projectionType));
        
        // Update max gauge (will be aggregated properly by StatsAggregator)
        // Individual instances update this, StatsAggregator will set the true max
        if (lagMs > _maxPipelineLagMs)
        {
            Interlocked.Exchange(ref _maxPipelineLagMs, (long)lagMs);
        }
    }

    /// <summary>
    /// Records pipeline lag (time from event commit to projection processing).
    /// This is the key metric for verifying performance objectives.
    /// </summary>
    public static void RecordPipelineProjectionLag(string projectionType, double lagMs, string instanceId)
    {
        // Record to histogram for percentile calculations in Aspire (p50, p95, p99)
        // NOTE: Only use low-cardinality dimensions to avoid hitting OpenTelemetry cardinality limits
        // With 10,000+ instances, instance.id would exceed limits and cause metric drops
        PipelineProjectionLagHistogram.Record(lagMs,
            new KeyValuePair<string, object?>("projection.type", projectionType));

        // Update max gauge (will be aggregated properly by StatsAggregator)
        // Individual instances update this, StatsAggregator will set the true max
        if (lagMs > _maxPipelineProjectionLagMs)
        {
            Interlocked.Exchange(ref _maxPipelineProjectionLagMs, (long)lagMs);
        }
    }

    /// <summary>
    /// Updates the maximum pipeline lag gauge (called by StatsAggregator).
    /// </summary>
    public static void SetMaxPipelineLag(long lagMs)
    {
        _maxPipelineLagMs = lagMs;
    }

    /// <summary>
    /// Updates the average pipeline lag gauge (called by StatsAggregator).
    /// </summary>
    public static void SetAveragePipelineLag(long lagMs)
    {
        _averagePipelineLagMs = lagMs;
    }

    /// <summary>
    /// Updates the average pipeline projection lag gauge (called by StatsAggregator).
    /// </summary>
    public static void SetAveragePipelineProjectionLag(long lagMs)
    {
        _averagePipelineProjectionLagMs = lagMs;
    }

    /// <summary>
    /// Updates the maximum pipeline proejction lag gauge (called by StatsAggregator).
    /// </summary>
    public static void SetMaxPipelineProjectionLag(long lagMs)
    {
        _maxPipelineProjectionLagMs = lagMs;
    }

    /// <summary>Records a successful barriered flush (gated by <see cref="ProjectionTelemetryOptions.EnableFlushMetrics"/>).</summary>
    public static void RecordFlush(string projectionType, double durationMs, int viewCount)
    {
        if (!_options.EnableFlushMetrics)
            return;

        var tag = new KeyValuePair<string, object?>("projection.type", projectionType);
        FlushDuration.Record(durationMs, tag);
        FlushViews.Record(viewCount, tag);
    }

    /// <summary>Records a failed barriered flush.</summary>
    public static void RecordFlushError(string projectionType)
    {
        if (!_options.EnableFlushMetrics)
            return;

        FlushErrorsCounter.Add(1,
            new KeyValuePair<string, object?>("projection.type", projectionType));
    }

    /// <summary>Records <c>headSeq - checkpoint</c> lag when known.</summary>
    public static void RecordCheckpointLag(string projectionType, long lagSequences)
    {
        if (!_options.EnableFlushMetrics)
            return;

        CheckpointLag.Record(Math.Max(0, lagSequences),
            new KeyValuePair<string, object?>("projection.type", projectionType));
    }

    /// <summary>
    /// Records a successful projection GET. <paramref name="cacheHit"/> distinguishes
    /// memory/hot-cache vs durable fallback. Optional <paramref name="readSource"/> is tagged
    /// only when <see cref="ProjectionTelemetryOptions.EnableHighDetailTags"/> is on.
    /// </summary>
    public static void RecordRead(string projectionType, bool cacheHit, string? readSource = null)
    {
        if (!_options.EnableCacheMetrics)
            return;

        var typeTag = new KeyValuePair<string, object?>("projection.type", projectionType);

        if (_options.EnableHighDetailTags && !string.IsNullOrWhiteSpace(readSource))
        {
            ReadTotalCounter.Add(1, typeTag,
                new KeyValuePair<string, object?>("source", readSource));
        }
        else
        {
            ReadTotalCounter.Add(1, typeTag);
        }

        if (cacheHit)
            ReadCacheHitCounter.Add(1, typeTag);
        else
            ReadCacheMissCounter.Add(1, typeTag);
    }

    /// <summary>Records a <c>minSequence</c> freshness rejection (409).</summary>
    public static void RecordStaleRejected(string projectionType)
    {
        if (!_options.EnableExtendedMetrics)
            return;

        ReadStaleRejectedCounter.Add(1,
            new KeyValuePair<string, object?>("projection.type", projectionType));
    }

    /// <summary>Updates the working-set size gauge for <paramref name="projectionType"/>.</summary>
    public static void SetWorkingSetSize(string projectionType, int count)
    {
        if (!_options.EnableExtendedMetrics)
            return;

        WorkingSetSizes[projectionType] = count;
    }

    /// <summary>Records clean working-set evictions.</summary>
    public static void RecordWorkingSetEviction(string projectionType, int count, string reason = "max_instances")
    {
        if (!_options.EnableExtendedMetrics || count <= 0)
            return;

        if (_options.EnableHighDetailTags)
        {
            WorkingSetEvictionsCounter.Add(count,
                new KeyValuePair<string, object?>("projection.type", projectionType),
                new KeyValuePair<string, object?>("reason", reason));
        }
        else
        {
            WorkingSetEvictionsCounter.Add(count,
                new KeyValuePair<string, object?>("projection.type", projectionType));
        }
    }
}
