using LawnDart.Projections.Telemetry;

namespace LawnDart.Projections.Lightweight.Hosting;

/// <summary>
/// Configuration options for the lightweight projection runner.
/// Configure via the callback passed to <c>WithProjections()</c> or
/// <c>WithProjections()</c> on a <c>BoundedContextBuilder</c>.
/// </summary>
public sealed class LightweightProjectionOptions
{
    /// <summary>
    /// The bounded context name these projections belong to.
    /// When set, the runner resolves <c>IEventStore</c> and <c>ICheckpointStore</c> as
    /// keyed services using this name; otherwise the unkeyed singletons are resolved.
    /// <para>Defaults to <see langword="null"/> (unkeyed / legacy behaviour).</para>
    /// </summary>
    public string? ContextName { get; set; }
    /// <summary>
    /// Gets or sets how frequently the runner polls the event store for new events
    /// when there are no pending events to process.
    /// <para>Default: 200 ms.</para>
    /// </summary>
    public TimeSpan PollInterval { get; set; } = TimeSpan.FromMilliseconds(200);

    /// <summary>
    /// Gets or sets the number of events processed between checkpoint saves.
    /// A lower value improves crash recovery at the cost of additional I/O.
    /// <para>Default: 500.</para>
    /// </summary>
    public int CheckpointInterval { get; set; } = 500;

    /// <summary>
    /// Gets or sets the maximum number of events read per poll batch.
    /// Larger values reduce I/O round-trips during catch-up; smaller values
    /// reduce latency in steady state.
    /// <para>Default: 1000.</para>
    /// </summary>
    public int BatchSize { get; set; } = 1000;

    /// <summary>
    /// Gets or sets the current node instance index (0-based).
    /// Used together with <see cref="TotalInstances"/> to partition stream ownership
    /// via consistent hashing across multiple running copies of the application.
    /// <para>Default: 0 (single-node — no partitioning).</para>
    /// </summary>
    public int NodeInstance { get; set; } = 0;

    /// <summary>
    /// Gets or sets the total number of application instances sharing projection load.
    /// Set to 1 to disable partitioning (all streams owned by this node).
    /// <para>Default: 1.</para>
    /// </summary>
    public int TotalInstances { get; set; } = 1;

    /// <summary>
    /// Maximum concurrent <c>IViewStore.SaveViewAsync</c> calls during a barriered flush.
    /// Values less than 1 are treated as 1. All view writes still complete before the checkpoint advances.
    /// <para>Default: 8.</para>
    /// </summary>
    public int FlushMaxDegreeOfParallelism { get; set; } = 8;

    /// <summary>
    /// When <see cref="SkipTailFlushWhileCatchingUp"/> is enabled, opportunistic (tail / gap-walk)
    /// flushes are skipped while
    /// <c>eventStoreHead - lastProcessedPosition &gt; CatchUpFlushThreshold</c>.
    /// Interval flushes and graceful-stop flushes are unaffected.
    /// <para>Default: 1000.</para>
    /// </summary>
    public long CatchUpFlushThreshold { get; set; } = 1000;

    /// <summary>
    /// When <see langword="true"/>, skip dirty-driven tail/gap flushes while the runner is still
    /// catching up past <see cref="CatchUpFlushThreshold"/> sequences behind the store head.
    /// Reduces SQL write amplification during rebuild/catch-up. Checkpoint-interval and stop flushes
    /// still run.
    /// <para>Default: <see langword="true"/>.</para>
    /// </summary>
    public bool SkipTailFlushWhileCatchingUp { get; set; } = true;

    /// <summary>
    /// In-process hot read cache mode for <c>MapProjectionQueries</c>.
    /// <para>Default: <see cref="ProjectionReadCacheMode.Hot"/>.</para>
    /// </summary>
    public ProjectionReadCacheMode ReadCacheMode { get; set; } = ProjectionReadCacheMode.Hot;

    /// <summary>
    /// Hard cap on cached view entries per cache instance (per bounded context).
    /// <para>Default: 10_000.</para>
    /// </summary>
    public int ReadCacheMaxEntries { get; set; } = 10_000;

    /// <summary>
    /// In <see cref="ProjectionReadCacheMode.Hot"/>, number of durable GET hits required before
    /// a view is promoted into the hot cache. <see cref="ProjectionReadCacheMode.Resident"/>
    /// always promotes on the first durable hit.
    /// <para>Default: 1.</para>
    /// </summary>
    public int ReadCachePromoteAfterHits { get; set; } = 1;

    /// <summary>
    /// How projection instances are loaded into the runner working set at startup.
    /// <para>Default: <see cref="ProjectionWorkingSetMode.EagerRestore"/>.</para>
    /// </summary>
    public ProjectionWorkingSetMode WorkingSetMode { get; set; } = ProjectionWorkingSetMode.EagerRestore;

    /// <summary>
    /// Maximum in-memory projection instances retained by the runner after a successful flush.
    /// Clean (non-dirty) instances are evicted LRU-first when over this budget.
    /// Dirty instances are never evicted. <c>0</c> means unlimited.
    /// <para>Default: 0 (unlimited).</para>
    /// </summary>
    public int WorkingSetMaxInstances { get; set; }

    /// <summary>
    /// Toggleable OpenTelemetry metrics for flush / cache / working-set instruments.
    /// Applied process-wide via <see cref="ProjectionTelemetry.Configure"/> when projections are registered.
    /// </summary>
    public ProjectionTelemetryOptions Telemetry { get; set; } = new();

    /// <summary>
    /// When <see langword="true"/>, barriered flushes run on a serialized background drain so the
    /// poll loop can continue applying events while SQL writes complete. Checkpoint still advances
    /// <strong>only after</strong> durable view ack for that flush wave (never enqueue-then-checkpoint).
    /// When the number of in-flight waves reaches <see cref="WriteBehindMaxInFlight"/>, the poll
    /// loop awaits backpressure. Graceful stop always drains pending waves.
    /// <para>Default: <see langword="false"/> (inline flush — production-safe without write-behind).</para>
    /// </summary>
    public bool EnableWriteBehind { get; set; }

    /// <summary>
    /// Maximum concurrent write-behind flush waves before the poll loop awaits drain backpressure.
    /// Values less than 1 are treated as 1. Only applies when <see cref="EnableWriteBehind"/> is true.
    /// <para>Default: 1.</para>
    /// </summary>
    public int WriteBehindMaxInFlight { get; set; } = 1;

    /// <summary>
    /// When <see langword="true"/> (default) and the event store implements
    /// <see cref="LawnDart.EventStore.IEventStoreSubscriptions"/>, the runner
    /// subscribes from the checkpoint and uses the poll loop only for recovery / fallback.
    /// Set <see langword="false"/> to force poll (kill-switch).
    /// </summary>
    public bool UseSubscribe { get; set; } = true;

    /// <summary>
    /// How long a healthy subscription may sit idle before one recovery poll window runs.
    /// Unused when <see cref="UseSubscribe"/> is false or the store does not implement subscriptions.
    /// <para>Default: 3 seconds.</para>
    /// </summary>
    public TimeSpan SubscribeRecoveryPollInterval { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>
    /// When <see langword="true"/> (default) and runners are started through
    /// <see cref="BoundedContextProjectionRunnerManager"/>, those runners share one
    /// process-level Subscribe (union filter) instead of one Subscribe each.
    /// Direct <c>new LightweightProjectionRunnerService</c> (no manager) always uses
    /// a per-runner Subscribe. Set <see langword="false"/> to keep per-runner Subscribe
    /// even under the manager (kill-switch).
    /// </summary>
    public bool UseSharedSubscribe { get; set; } = true;

    /// <summary>
    /// A handler whose <c>lastApplied</c> is more than this many sequences behind
    /// <c>min(live lastApplied) + 1</c> does private filtered catch-up until it reaches
    /// the join point, then attaches to the shared pipe. Live handlers are not rewound.
    /// <para>Default: 10_000.</para>
    /// </summary>
    public long CatchUpJoinThreshold { get; set; } = 10_000;
}
