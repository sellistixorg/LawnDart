using System.Diagnostics;
using System.Reflection;
using System.Text.Json;
using Microsoft.Extensions.Hosting;
using LawnDart;
using LawnDart.Projections.Lightweight;
using Microsoft.Extensions.Logging;
using LawnDart.EventStore;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Sdk;
using LawnDart.Projections.Storage;
using LawnDart.Projections.Telemetry;

namespace LawnDart.Projections.Lightweight.Hosting;

/// <summary>
/// A poll-based <see cref="BackgroundService"/> that drives a single registered lightweight
/// projection. One instance of this service is created per
/// <see cref="ProjectionRegistration"/> by <c>WithProjections()</c>.
/// </summary>
/// <remarks>
/// <para>
/// <strong>Startup sequence</strong><br/>
/// <list type="number">
///   <item>Load the global checkpoint from <see cref="ICheckpointStore"/>. A missing checkpoint
///         is treated as a cold start: polling begins at global sequence position 0.</item>
///   <item>For <see cref="ProjectionKind.SingleStream"/> projections, existing view snapshots
///         are loaded from <see cref="IViewStore"/> and their state is restored into in-memory
///         projection instances, allowing warm restarts to avoid replaying events that are
///         already represented in the saved view.</item>
///   <item>The poll loop reads events in capped batches of
///         <see cref="LightweightProjectionOptions.BatchSize"/> events per cycle. A full
///         batch triggers an immediate re-poll (more events almost certainly exist). A
///         partial or empty batch transitions to the tail-detection path: the store's
///         current sequence is checked via <see cref="IEventStore.GetCurrentSequenceAsync"/>
///         (probed once at startup; gracefully degraded for stores that do not support it).
///         If the sequence has advanced since the last read the loop continues immediately;
///         otherwise the runner sleeps for <see cref="LightweightProjectionOptions.PollInterval"/>
///         before trying again. No segment I/O is performed on a genuinely idle cycle.
///         Live reads (Subscribe and poll) use a type filter from DCB/MultiStream
///         <c>QueryTypes</c> or compiled <c>Handle</c> methods. Empty typed windows
///         advance the cursor by one <see cref="LightweightProjectionOptions.BatchSize"/> —
///         they do not fall back to <c>Query.All()</c>. <c>Query.All()</c> is rebuild-only
///         when the projection has no live type list.</item>
/// </list>
/// </para>
/// <para>
/// <strong>Poison events</strong><br/>
/// When a compiled <c>Handle</c> method throws, the runner restores the instance to its
/// pre-apply view snapshot and retries up to <see cref="PoisonRetryAttempts"/> times.
/// If every attempt fails, the global checkpoint does not advance past the failed sequence,
/// later events are not applied, and the runner parks with
/// <see cref="ProjectionRunnerObservabilitySnapshot.IsFaulted"/> until stop or rebuild.
/// Unmatched types, unowned partitions, and a null multi-stream entity id still skip and
/// advance the cursor.
/// </para>
/// <para>
/// <strong>Checkpointing</strong><br/>
/// After processing every <see cref="LightweightProjectionOptions.CheckpointInterval"/> events
/// all dirty views are written to <see cref="IViewStore"/> first, then the global checkpoint
/// is advanced. This ordering guarantees that on restart, view state is always consistent with
/// or ahead of the checkpoint position. A final checkpoint is saved on graceful stop.
/// When <see cref="LightweightProjectionOptions.EnableWriteBehind"/> is enabled, view writes run
/// on a serialized background drain so the poll loop can overlap SQL I/O; the checkpoint still
/// advances only after durable ack for that flush wave (never enqueue-then-checkpoint).
/// </para>
/// <para>
/// <strong>Multi-node partitioning</strong><br/>
/// For <see cref="ProjectionKind.SingleStream"/> projections the
/// <see cref="IPartitioningService.OwnsStream"/> guard prevents duplicate processing when
/// multiple application instances share projection load.
/// </para>
/// </remarks>
public sealed class LightweightProjectionRunnerService : BackgroundService
{
    private readonly ProjectionRegistration _registration;
    private readonly IEventStore _eventStore;
    private readonly IViewStore _viewStore;
    private readonly ICheckpointStore _checkpointStore;
    private readonly IPartitioningService _partitioningService;
    private readonly LightweightProjectionOptions _options;
    private readonly IProjectionReadCache? _readCache;
    private readonly ILogger<LightweightProjectionRunnerService> _logger;
    private readonly IEventStoreSubscriptions? _subscriptions;
    private LightweightProjectionSharedPipe? _sharedPipe;
    private bool _sharedPipePrivateCatchUp;

    private ISubscriptionHandle? _subscriptionHandle;
    private CancellationTokenSource? _subscriptionCts;
    private bool _subscribeFaulted;
    private bool _suppressResubscribe;
    private bool _poisoned;
    private long _poisonSequence = -1;
    private string? _poisonInstanceId;
    private string? _poisonEventType;

    /// <summary>Total <c>Handle</c> attempts (initial + retries) before a poison event halts the runner.</summary>
    internal const int PoisonRetryAttempts = 3;

    // Per-stream or single instance dictionary.
    // Key: instanceId (view store key), Value: projection handler instance.
    private readonly Dictionary<string, object> _instances = new();

    // Compiled delegate cache (one per handler type, shared across instances).
    private Dictionary<Type, Action<object, IEvent>>? _compiledHandlers;
    private Func<object, object>? _compiledGetView;
    private Query? _liveQuery;
    private bool _liveQueryResolved;

    // For MultiStream projections: a single prototype instance used only to resolve entity IDs.
    // The prototype itself is never used for state — per-entity instances live in _instances.
    private IMultiStreamEntityResolver? _entityResolver;

    // Tracks which instance IDs have unsaved changes since the last flush.
    private readonly HashSet<string> _dirtyInstances = new();

    // Per-instance last successfully applied global sequence.
    // Persisted into the view store checkpoint column on flush (not the flush-wave position).
    private readonly Dictionary<string, long> _lastAppliedByInstance = new();

    // LRU touch clock for clean-instance eviction.
    private readonly Dictionary<string, long> _lastAccessTicks = new();

    // Set to true when the event store supports GetCurrentSequenceAsync() without throwing.
    // Probed once on startup; false for stores that do not implement the method, which is
    // typically the case for remote ones. When true the poll loop skips the segment scan on
    // idle cycles where the store sequence has not advanced beyond the last processed position.
    private bool _supportsSequenceCheck;

    // Observability: last checkpoint + max observed pipeline lag on processed events.
    private long   _lastCheckpointSequence;
    private long   _lastAppliedSequence;
    private double? _maxPipelineLagMs;

    // Serialized write-behind drain (checkpoint only after durable view ack).
    private readonly object _writeBehindGate = new();
    private Task _writeBehindTail = Task.CompletedTask;
    private int _writeBehindInFlight;

    // Protects working-set / dirty / last-applied mutations when write-behind drain overlaps the poll loop.
    private readonly object _instanceStateGate = new();

    private string DisplayName => _registration.DisplayName;
    internal string StorageKey => _registration.StorageKey;

    /// <summary>Live type names for the process-level union filter.</summary>
    internal IReadOnlyList<string> GetLiveEventTypeNames() => ResolveLiveEventTypeNames();

    /// <summary>Attaches this runner to a manager-owned shared Subscribe pipe.</summary>
    internal void AttachSharedPipe(LightweightProjectionSharedPipe pipe) =>
        _sharedPipe = pipe ?? throw new ArgumentNullException(nameof(pipe));

    private bool UsesSharedPipe =>
        _sharedPipe is { IsEnabled: true }
        && _options.UseSharedSubscribe
        && _options.UseSubscribe;

    /// <summary>
    /// Initializes a new <see cref="LightweightProjectionRunnerService"/>.
    /// </summary>
    /// <param name="registration">The projection registration to drive.</param>
    /// <param name="eventStore">Event store used for polling.</param>
    /// <param name="viewStore">View store for persisting and loading projection snapshots.</param>
    /// <param name="checkpointStore">Checkpoint store for tracking progress.</param>
    /// <param name="partitioningService">
    /// Service that determines which streams this node owns.
    /// </param>
    /// <param name="options">Runner configuration.</param>
    /// <param name="logger">Optional logger.</param>
    /// <param name="readCache">Optional hot read cache; updated on apply.</param>
    public LightweightProjectionRunnerService(
        ProjectionRegistration registration,
        IEventStore eventStore,
        IViewStore viewStore,
        ICheckpointStore checkpointStore,
        IPartitioningService partitioningService,
        LightweightProjectionOptions options,
        ILogger<LightweightProjectionRunnerService>? logger = null,
        IProjectionReadCache? readCache = null)
    {
        _registration       = registration        ?? throw new ArgumentNullException(nameof(registration));
        _eventStore         = eventStore          ?? throw new ArgumentNullException(nameof(eventStore));
        _viewStore          = viewStore           ?? throw new ArgumentNullException(nameof(viewStore));
        _checkpointStore    = checkpointStore     ?? throw new ArgumentNullException(nameof(checkpointStore));
        _partitioningService = partitioningService ?? throw new ArgumentNullException(nameof(partitioningService));
        _options            = options             ?? throw new ArgumentNullException(nameof(options));
        _logger             = logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<LightweightProjectionRunnerService>.Instance;
        _readCache          = readCache;
        _subscriptions      = eventStore as IEventStoreSubscriptions;
    }

    /// <inheritdoc/>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "[{Context}/{Projection}] Runner starting (kind={Kind}, node={Node}/{Total})",
            _options.ContextName ?? "default",
            DisplayName, _registration.Kind,
            _options.NodeInstance, _options.TotalInstances);

        // 1. Load global checkpoint
        var checkpoint = await _checkpointStore.GetCheckpointAsync(
            StorageKey,
            _options.NodeInstance,
            stoppingToken);

        long lastPosition = checkpoint?.LastSequencePosition ?? -1;
        _lastAppliedSequence = lastPosition;
        long totalEventsProcessed = checkpoint?.TotalEventsProcessed ?? 0;
        int eventsSinceCheckpoint = 0;
        bool isColdStart = checkpoint is null;

        _logger.LogInformation(
            "[{Projection}] {Mode}: resuming from global position {Position}",
            DisplayName,
            isColdStart ? "Cold start" : "Warm start",
            lastPosition);

        // 2. Restore working set (Eager) or skip bulk load (Lazy — hydrate on first event).
        if (_registration.Kind == ProjectionKind.MultiStream)
        {
            // Initialize the shared resolver prototype before the poll loop
            var prototype = Activator.CreateInstance(_registration.HandlerType);
            _entityResolver = prototype as IMultiStreamEntityResolver
                ?? throw new InvalidOperationException(
                    $"Projection '{DisplayName}' (kind=MultiStream) handler type " +
                    $"'{_registration.HandlerType.FullName}' does not implement IMultiStreamEntityResolver.");
        }

        if (_options.WorkingSetMode == ProjectionWorkingSetMode.EagerRestore)
        {
            if (_registration.Kind == ProjectionKind.SingleStream
                || _registration.Kind == ProjectionKind.MultiStream)
            {
                await RestoreSingleStreamInstancesAsync(stoppingToken);
            }
            else
            {
                await RestoreSingleInstanceAsync(stoppingToken);
            }

            // Cap RAM immediately after eager warm-restore (all instances are clean).
            lock (_instanceStateGate)
            {
                EvictCleanInstancesIfNeeded_NoLock();
                ProjectionTelemetry.SetWorkingSetSize(StorageKey, _instances.Count);
            }
        }
        else
        {
            _logger.LogInformation(
                "[{Projection}] Lazy working-set mode: skipping bulk restore ({Count} will hydrate on demand)",
                DisplayName, 0);
            ProjectionTelemetry.SetWorkingSetSize(StorageKey, 0);
        }

        // 3. Probe whether the store supports GetCurrentSequenceAsync.
        //    Stores without support throw NotSupportedException; the in-process stores
        //    return immediately from a cached long field. The result is cached for the
        //    lifetime of this runner.
        try
        {
            await _eventStore.GetCurrentSequenceAsync(stoppingToken);
            _supportsSequenceCheck = true;
            _logger.LogDebug(
                "[{Projection}] Store supports sequence check — idle polls will skip I/O when caught up",
                DisplayName);
        }
        catch (NotSupportedException)
        {
            _supportsSequenceCheck = false;
            _logger.LogDebug(
                "[{Projection}] Store does not support sequence check — falling back to timed poll interval",
                DisplayName);
        }

        // 4. Subscribe (when available) + poll fallback / recovery
        var cursor = new DeliveryCursor
        {
            LastPosition = lastPosition,
            TotalEventsProcessed = totalEventsProcessed,
            EventsSinceCheckpoint = eventsSinceCheckpoint
        };
        if (UsesSharedPipe)
        {
            _sharedPipe!.Register(this, cursor.LastPosition);
            await _sharedPipe.WaitUntilActivatedAsync(stoppingToken);
            _sharedPipePrivateCatchUp = !_sharedPipe.IsAttached(StorageKey);
        }
        else
        {
            TryStartSubscription(cursor.LastPosition, stoppingToken);
        }

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (_poisoned)
                {
                    await HaltOnPoisonAsync(lastPosition, totalEventsProcessed, stoppingToken);
                    break;
                }

                if (UsesSharedPipe && !_sharedPipePrivateCatchUp && _sharedPipe!.IsLive)
                {
                    cursor.LastPosition = lastPosition;
                    cursor.TotalEventsProcessed = totalEventsProcessed;
                    cursor.EventsSinceCheckpoint = eventsSinceCheckpoint;
                    var ingested = await DrainSharedPipeAsync(cursor, stoppingToken);
                    lastPosition = cursor.LastPosition;
                    totalEventsProcessed = cursor.TotalEventsProcessed;
                    eventsSinceCheckpoint = cursor.EventsSinceCheckpoint;
                    _lastAppliedSequence = lastPosition;
                    _sharedPipe.ReportLastApplied(StorageKey, lastPosition);

                    if (_poisoned)
                    {
                        await HaltOnPoisonAsync(lastPosition, totalEventsProcessed, stoppingToken);
                        break;
                    }

                    if (ingested == 0 && _sharedPipe.CaughtUpSequence > lastPosition)
                    {
                        lastPosition = _sharedPipe.CaughtUpSequence;
                        cursor.LastPosition = lastPosition;
                        _lastAppliedSequence = lastPosition;
                        _sharedPipe.ReportLastApplied(StorageKey, lastPosition);
                    }

                    if (await FlushLeftoverDirtiesAsync(
                            lastPosition, totalEventsProcessed, stoppingToken).ConfigureAwait(false))
                    {
                        eventsSinceCheckpoint = 0;
                        cursor.EventsSinceCheckpoint = 0;
                    }

                    continue;
                }

                if (UsesSharedPipe && _sharedPipePrivateCatchUp
                    && lastPosition >= 0
                    && lastPosition >= Math.Max(
                        _sharedPipe!.JoinSequence - 1,
                        Math.Max(_sharedPipe.CaughtUpSequence, _sharedPipe.LastFanoutSequence)))
                {
                    _sharedPipe.ReportLastApplied(StorageKey, lastPosition);
                    _sharedPipe.Attach(StorageKey);
                    _sharedPipePrivateCatchUp = false;
                    continue;
                }

                if (IsSubscriptionLive)
                {
                    if (await TryFaultSubscribeIfFarBehindAsync(lastPosition, stoppingToken)
                            .ConfigureAwait(false))
                    {
                        // Poll loop below — no 3s Subscribe idle.
                    }
                    else
                    {
                        cursor.LastPosition = lastPosition;
                        cursor.TotalEventsProcessed = totalEventsProcessed;
                        cursor.EventsSinceCheckpoint = eventsSinceCheckpoint;
                        var ingested = await DrainSubscriptionAsync(cursor, stoppingToken);
                        lastPosition = cursor.LastPosition;
                        totalEventsProcessed = cursor.TotalEventsProcessed;
                        eventsSinceCheckpoint = cursor.EventsSinceCheckpoint;
                        _lastAppliedSequence = lastPosition;

                        if (_poisoned)
                        {
                            await HaltOnPoisonAsync(lastPosition, totalEventsProcessed, stoppingToken);
                            break;
                        }

                        if (await FlushLeftoverDirtiesAsync(
                                lastPosition, totalEventsProcessed, stoppingToken).ConfigureAwait(false))
                        {
                            eventsSinceCheckpoint = 0;
                            cursor.EventsSinceCheckpoint = 0;
                        }

                        if (ingested > 0)
                            continue;

                        if (IsSubscriptionLive)
                        {
                            await RecoveryPollWindowAsync(cursor, stoppingToken);
                            lastPosition = cursor.LastPosition;
                            totalEventsProcessed = cursor.TotalEventsProcessed;
                            eventsSinceCheckpoint = cursor.EventsSinceCheckpoint;
                            _lastAppliedSequence = lastPosition;

                            if (_poisoned)
                            {
                                await HaltOnPoisonAsync(lastPosition, totalEventsProcessed, stoppingToken);
                                break;
                            }

                            if (await FlushLeftoverDirtiesAsync(
                                    lastPosition, totalEventsProcessed, stoppingToken).ConfigureAwait(false))
                            {
                                eventsSinceCheckpoint = 0;
                                cursor.EventsSinceCheckpoint = 0;
                            }

                            continue;
                        }
                    }
                }
                else if (_subscribeFaulted)
                {
                    TryStartSubscription(lastPosition, stoppingToken);
                    if (IsSubscriptionLive)
                        continue;
                }

                int batchCount = 0;
                long fromPosition = lastPosition + 1;
                // Cap each read to BatchSize events so that:
                //   • memory pressure is bounded during large catch-ups
                //   • a full batch acts as a signal that more events exist (immediate re-poll)
                //   • a partial batch transitions to the sequence-check / idle-wait path
                long toPosition = lastPosition + _options.BatchSize;
                var pollSw = Stopwatch.StartNew();

                await foreach (var se in ReadEventsAsync(fromPosition, toPosition, stoppingToken))
                {
                    if (!await DispatchEventAsync(se, stoppingToken))
                        break;
                    lastPosition = se.SequencePosition;
                    totalEventsProcessed++;
                    eventsSinceCheckpoint++;
                    batchCount++;

                    if (eventsSinceCheckpoint >= _options.CheckpointInterval)
                    {
                        await FlushViewsAndCheckpointAsync(
                            lastPosition, totalEventsProcessed, stoppingToken);
                        eventsSinceCheckpoint = 0;
                    }
                }

                if (_poisoned)
                {
                    await HaltOnPoisonAsync(lastPosition, totalEventsProcessed, stoppingToken);
                    break;
                }

                // Filtered queries can return empty while the store head has already advanced
                // (append/query visibility race). Re-read the same window once before treating
                // the batch as empty and walking the cursor forward without dispatching.
                if (batchCount == 0 && UsesFilteredEventQuery())
                {
                    await foreach (var se in ReadEventsAsync(fromPosition, toPosition, stoppingToken))
                    {
                        if (!await DispatchEventAsync(se, stoppingToken))
                            break;
                        lastPosition = se.SequencePosition;
                        totalEventsProcessed++;
                        eventsSinceCheckpoint++;
                        batchCount++;

                        if (eventsSinceCheckpoint >= _options.CheckpointInterval)
                        {
                            await FlushViewsAndCheckpointAsync(
                                lastPosition, totalEventsProcessed, stoppingToken);
                            eventsSinceCheckpoint = 0;
                        }
                    }
                }

                if (_poisoned)
                {
                    await HaltOnPoisonAsync(lastPosition, totalEventsProcessed, stoppingToken);
                    break;
                }

                pollSw.Stop();
                _lastAppliedSequence = lastPosition;
                ProjectionTelemetry.RecordPollDuration(
                    StorageKey, pollSw.Elapsed.TotalMilliseconds, batchCount);

                // ── Decide what to do next ────────────────────────────────────────────
                //
                // Full batch received → almost certainly more events exist.
                // Skip all checks and loop immediately to keep catching up at full speed.
                if (batchCount == _options.BatchSize)
                    continue;

                // Partial or empty batch → at or near the tail. Flush dirties unless we are still
                // catching up (SkipTailFlushWhileCatchingUp) — interval/stop flushes still apply.
                if (_dirtyInstances.Count > 0
                    && !await ShouldSkipOpportunisticFlushAsync(lastPosition, stoppingToken))
                {
                    await FlushViewsAndCheckpointAsync(
                        lastPosition, totalEventsProcessed, stoppingToken);
                    eventsSinceCheckpoint = 0;
                }

                if (_supportsSequenceCheck)
                {
                    // Ask the store whether any new events exist beyond our last position.
                    // This is a cheap in-memory read for in-process stores and a lightweight
                    // round trip for remote ones.
                    var headSeq = await _eventStore.GetCurrentSequenceAsync(stoppingToken);
                    ProjectionTelemetry.RecordCheckpointLag(StorageKey, headSeq - lastPosition);
                    if (!IsFarBehind(lastPosition, headSeq))
                    {
                        _suppressResubscribe = false;
                        if (_subscribeFaulted || _subscriptionHandle is null)
                            TryStartSubscription(lastPosition, stoppingToken);
                    }

                    if (headSeq > lastPosition)
                    {
                        // Filtered reads (MultiStream / DCB with an event-type query) can yield
                        // zero events for a global sequence window even when many unrelated
                        // events exist in that range.  Advancing lastPosition by the read window
                        // (capped at head) walks the global log without dispatching — unlike the
                        // unfiltered HiLo gap bridge below, we must NOT jump to head-1 in one
                        // step or we would skip matching events in the middle of the log.
                        if (batchCount == 0 && UsesFilteredEventQuery())
                        {
                            // Filtered query already covered [from, to]. Advance one window so
                            // unrelated traffic (e.g. PLAID ticks) is not re-downloaded via Query.All().
                            // Do not jump to head — matching events later in the log must still be read.
                            long windowEnd = lastPosition + _options.BatchSize;
                            long newLast = Math.Min(windowEnd, headSeq);
                            if (newLast > lastPosition)
                            {
                                _logger.LogDebug(
                                    "[{Projection}] Filtered catch-up: advancing cursor from {Prev} to {New} " +
                                    "(head={Head}, windowEnd={WindowEnd}) — empty typed window",
                                    DisplayName, lastPosition, newLast, headSeq, windowEnd);
                                lastPosition = newLast;
                                if (!await ShouldSkipOpportunisticFlushAsync(lastPosition, stoppingToken))
                                {
                                    await FlushViewsAndCheckpointAsync(
                                        lastPosition, totalEventsProcessed, stoppingToken);
                                    eventsSinceCheckpoint = 0;
                                }
                            }
                            else
                            {
                                // Tail band: head is close but nothing visible yet — poll again.
                                await Task.Delay(_options.PollInterval, stoppingToken);
                                continue;
                            }
                        }
                        // Unfiltered gap walk: when the store head is beyond this batch window and
                        // the window is empty, advance the cursor by one batch (capped at head).
                        // Do not jump to head-1 — SQL Server CACHE gaps and similar patterns can
                        // have committed events after an empty range (e.g. 1-198, gap, 1002+).
                        else if (batchCount == 0 && headSeq > lastPosition + _options.BatchSize)
                        {
                            long windowEnd = lastPosition + _options.BatchSize;
                            long newLast    = Math.Min(windowEnd, headSeq);
                            if (newLast > lastPosition)
                            {
                                _logger.LogDebug(
                                    "[{Projection}] Sequence gap walk: advancing cursor from {Prev} to {New} " +
                                    "(head={Head}, windowEnd={WindowEnd}) — empty global window",
                                    DisplayName, lastPosition, newLast, headSeq, windowEnd);
                                lastPosition = newLast;
                                if (!await ShouldSkipOpportunisticFlushAsync(lastPosition, stoppingToken))
                                {
                                    await FlushViewsAndCheckpointAsync(
                                        lastPosition, totalEventsProcessed, stoppingToken);
                                    eventsSinceCheckpoint = 0;
                                }
                            }
                        }

                        // New events exist beyond lastPosition (or we advanced the cursor).
                        // Loop immediately to pick them up — no delay.
                        continue;
                    }

                    // Genuinely caught up: nothing new in the store.
                    await Task.Delay(_options.PollInterval, stoppingToken);
                }
                else
                {
                    // No sequence check available (the store does not implement
                    // GetCurrentSequenceAsync). Fall back to: if we got at least one event in the
                    // partial batch there may be more just below the batch ceiling, so loop
                    // immediately; otherwise wait the full poll interval.
                    if (batchCount == 0)
                        await Task.Delay(_options.PollInterval, stoppingToken);
                    // else: partial batch > 0 → loop immediately
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                // Subscribe drain updates the cursor before a flush can throw. Keep that
                // progress so the next loop does not re-apply and inflate the view.
                lastPosition = Math.Max(lastPosition, cursor.LastPosition);
                totalEventsProcessed = Math.Max(totalEventsProcessed, cursor.TotalEventsProcessed);
                eventsSinceCheckpoint = Math.Max(eventsSinceCheckpoint, cursor.EventsSinceCheckpoint);
                _lastAppliedSequence = lastPosition;
                _logger.LogError(ex,
                    "[{Projection}] Error in poll loop; backing off 5 s",
                    DisplayName);
                await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
            }
        }

        // 4. Graceful stop — drop the subscription, drain write-behind, then persist dirty state.
        _sharedPipe?.Unregister(StorageKey);
        StopSubscription();
        try
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            await DrainWriteBehindAsync(cts.Token);
            if (_dirtyInstances.Count > 0 || eventsSinceCheckpoint > 0 || lastPosition > _lastCheckpointSequence)
            {
                // Stop path always uses the inline barrier (await durable ack).
                await FlushViewsAndCheckpointCoreAsync(lastPosition, totalEventsProcessed, cts.Token);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "[{Projection}] Final checkpoint flush did not complete cleanly",
                DisplayName);
        }

        _logger.LogInformation(
            "[{Projection}] Runner stopped at position {Position} ({Total} events total)",
            DisplayName, lastPosition, totalEventsProcessed);
    }

    // ── Event reading ──────────────────────────────────────────────────────────

    /// <summary>
    /// Returns an async sequence of at most <see cref="LightweightProjectionOptions.BatchSize"/>
    /// events from the event store starting at <paramref name="fromPosition"/>, applying the
    /// appropriate query for the projection kind.
    /// </summary>
    /// <remarks>
    /// Capping reads to <c>BatchSize</c> events per cycle keeps memory pressure bounded during
    /// large backlog catch-ups and gives the poll loop a natural decision point: when a full
    /// batch is returned the loop continues immediately (more events almost certainly exist);
    /// when fewer than <c>BatchSize</c> events are returned the loop transitions to the
    /// sequence-check / idle-wait path.
    /// </remarks>
    private IAsyncEnumerable<SequencedEvent> ReadEventsAsync(
        long fromPosition, long toPosition, CancellationToken ct)
    {
        return _eventStore.ReadByQueryStreamAsync(
            GetLiveQuery() ?? Query.All(),
            fromSequencePosition: fromPosition,
            toSequencePosition: toPosition,
            cancellationToken: ct);
    }

    /// <summary>
    /// Live type filter for Subscribe and poll. <see langword="null"/> means
    /// <see cref="Query.All"/> (rebuild / no Handle or DCB types).
    /// </summary>
    private Query? GetLiveQuery()
    {
        if (_liveQueryResolved)
            return _liveQuery;

        var typeNames = ResolveLiveEventTypeNames();
        _liveQuery = typeNames.Count > 0
            ? Query.FromItems(QueryItem.ByType(typeNames.ToArray()))
            : null;
        _liveQueryResolved = true;
        return _liveQuery;
    }

    private IReadOnlyList<string> ResolveLiveEventTypeNames()
    {
        if (_registration.DcbQueryTypes.Count > 0)
            return _registration.DcbQueryTypes;

        var names = GetCompiledHandlers().Keys
            .Select(EventTypeNameResolver.GetName)
            .Where(n => !string.IsNullOrWhiteSpace(n))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        return names;
    }

    private EventSubscriptionFilter BuildLiveSubscriptionFilter()
    {
        var query = GetLiveQuery();
        return query is null
            ? EventSubscriptionFilter.All()
            : EventSubscriptionFilter.ForQuery(query);
    }

    /// <summary>
    /// True when live reads use an event-type filter over the global sequence (not <see cref="Query.All"/>).
    /// </summary>
    private bool UsesFilteredEventQuery() => GetLiveQuery() is not null;

    // ── Event dispatching ──────────────────────────────────────────────────────

    private Task<bool> DispatchEventAsync(SequencedEvent se, CancellationToken ct) =>
        _registration.Kind switch
        {
            ProjectionKind.SingleStream => DispatchToSingleStreamInstanceAsync(se, ct),
            ProjectionKind.MultiStream => DispatchToMultiStreamInstanceAsync(se, ct),
            _ => DispatchToSingleInstanceAsync(se, ct)
        };

    private async Task<bool> DispatchToSingleStreamInstanceAsync(SequencedEvent se, CancellationToken ct)
    {
        // Filter to streams matching the configured stream type
        if (!IsMatchingStreamType(se.StreamId))
            return true;

        // Partition check — skip streams owned by other nodes
        if (!_partitioningService.OwnsStream(se.StreamId))
            return true;

        var instanceId = BuildInstanceId(se.StreamId);
        var instance = await GetOrHydrateInstanceAsync(instanceId, ct);
        return ApplyEvent(instance, instanceId, se);
    }

    private void RecordMaxLag(double lagMs) =>
        _maxPipelineLagMs = _maxPipelineLagMs is null ? lagMs : Math.Max(_maxPipelineLagMs.Value, lagMs);

    private void NoteApplied(string instanceId, SequencedEvent se)
    {
        lock (_instanceStateGate)
        {
            _dirtyInstances.Add(instanceId);
            _lastAppliedByInstance[instanceId] = se.SequencePosition;
            Touch(instanceId);
        }

        PublishToReadCache(instanceId);

        if (se.Metadata.CommitTimestamp.HasValue)
        {
            var lagMs = (DateTime.UtcNow - se.Metadata.CommitTimestamp.Value).TotalMilliseconds;
            RecordMaxLag(lagMs);
            ProjectionTelemetry.RecordPipelineLag(StorageKey, lagMs, instanceId);
        }
    }

    private void Touch(string instanceId) =>
        _lastAccessTicks[instanceId] = Environment.TickCount64;

    private void PublishToReadCache(string instanceId)
    {
        if (_readCache is null || _options.ReadCacheMode == ProjectionReadCacheMode.Off)
            return;

        object instance;
        long sequence;
        lock (_instanceStateGate)
        {
            if (!_instances.TryGetValue(instanceId, out instance!))
                return;

            if (!_lastAppliedByInstance.TryGetValue(instanceId, out sequence))
                return;
        }

        var viewObj = GetCompiledGetView()(instance);
        var viewJson = JsonSerializer.Serialize(viewObj, viewObj.GetType(), ProjectionViewJson.Write);
        _readCache.Set(StorageKey, instanceId, viewJson, sequence);
    }

    /// <summary>
    /// Tries to read a live in-memory view for <paramref name="instanceId"/> (working set).
    /// Used by hot-path GET before falling back to durable stores.
    /// </summary>
    public bool TryGetView(string instanceId, out string viewJson, out long sequence)
    {
        viewJson = string.Empty;
        sequence = 0;

        if (string.IsNullOrWhiteSpace(instanceId))
            return false;

        object instance;
        lock (_instanceStateGate)
        {
            if (!_instances.TryGetValue(instanceId, out instance!))
                return false;

            if (!_lastAppliedByInstance.TryGetValue(instanceId, out sequence))
                return false;

            Touch(instanceId);
        }

        var viewObj = GetCompiledGetView()(instance);
        viewJson = JsonSerializer.Serialize(viewObj, viewObj.GetType(), ProjectionViewJson.Write);
        return true;
    }

    /// <summary>Current in-memory instance count (working set size).</summary>
    public int WorkingSetCount
    {
        get { lock (_instanceStateGate) return _instances.Count; }
    }

    /// <summary>
    /// Loads an instance from the working set, or creates one hydrated from durable storage
    /// when missing (Lazy cold start or post-eviction).
    /// </summary>
    private async Task<object> GetOrHydrateInstanceAsync(string instanceId, CancellationToken ct)
    {
        lock (_instanceStateGate)
        {
            if (_instances.TryGetValue(instanceId, out var existing))
            {
                Touch(instanceId);
                return existing;
            }
        }

        var instance = Activator.CreateInstance(_registration.HandlerType)
            ?? throw new InvalidOperationException(
                $"Cannot create instance of '{_registration.HandlerType.FullName}'.");

        var saved = await _viewStore.GetViewWithCheckpointAsync(StorageKey, instanceId, ct);
        if (saved is not null)
        {
            SetProjectionState(instance, saved.Value.ViewData);
            _readCache?.Set(StorageKey, instanceId, saved.Value.ViewData, saved.Value.Checkpoint);
            _logger.LogDebug(
                "[{Projection}] Hydrated instance '{Instance}' from durable store at seq {Seq}",
                DisplayName, instanceId, saved.Value.Checkpoint);
        }
        else
        {
            _logger.LogDebug(
                "[{Projection}] New empty instance created for '{Instance}'",
                DisplayName, instanceId);
        }

        lock (_instanceStateGate)
        {
            if (_instances.TryGetValue(instanceId, out var raced))
            {
                Touch(instanceId);
                return raced;
            }

            if (saved is not null)
                _lastAppliedByInstance[instanceId] = saved.Value.Checkpoint;

            _instances[instanceId] = instance;
            Touch(instanceId);
            ProjectionTelemetry.SetWorkingSetSize(StorageKey, _instances.Count);
            return instance;
        }
    }

    private async Task<bool> DispatchToSingleInstanceAsync(SequencedEvent se, CancellationToken ct)
    {
        var singleInstanceId = ProjectionInstanceIds.ForUnpartitioned(_options);
        var instance = await GetOrHydrateInstanceAsync(singleInstanceId, ct);
        return ApplyEvent(instance, singleInstanceId, se);
    }

    private async Task<bool> DispatchToMultiStreamInstanceAsync(SequencedEvent se, CancellationToken ct)
    {
        // Ask the shared resolver prototype which entity this event belongs to
        var entityId = _entityResolver?.GetEntityId(se.Event);
        if (string.IsNullOrEmpty(entityId))
            return true;

        var instanceId = BuildMultiStreamInstanceId(se.StreamId, entityId);

        // Partition by instance id so each entity has a single writer node (aligned with restore).
        if (!_partitioningService.OwnsStream(instanceId))
            return true;

        var instance = await GetOrHydrateInstanceAsync(instanceId, ct);
        return ApplyEvent(instance, instanceId, se);
    }

    /// <summary>
    /// Applies a matching event. Returns <see langword="true"/> when the cursor may advance
    /// (applied or no handler). Returns <see langword="false"/> after
    /// <see cref="PoisonRetryAttempts"/> failed <c>Handle</c> calls — caller must halt.
    /// </summary>
    private bool ApplyEvent(object instance, string instanceId, SequencedEvent se)
    {
        var eventClrType = se.Event.GetType();
        var eventType = eventClrType.Name;
        if (!GetCompiledHandlers().TryGetValue(eventClrType, out var handle))
            return true;

        var preApplyJson = SnapshotViewJson(instance);
        Exception? lastEx = null;
        for (var attempt = 1; attempt <= PoisonRetryAttempts; attempt++)
        {
            if (attempt > 1)
                SetProjectionState(instance, preApplyJson);

            var sw = Stopwatch.StartNew();
            try
            {
                handle(instance, se.Event);
                sw.Stop();
                NoteApplied(instanceId, se);
                ProjectionTelemetry.RecordEventProcessed(StorageKey, eventType, sw.Elapsed.TotalMilliseconds);
                return true;
            }
            catch (Exception ex)
            {
                lastEx = ex;
                SetProjectionState(instance, preApplyJson);
                ProjectionTelemetry.RecordEventFailed(StorageKey, eventType, ex.GetType().Name);
                _logger.LogError(ex,
                    "[{Projection}] Error processing event {EventType} for instance '{Instance}' " +
                    "(attempt {Attempt}/{Max}) at sequence {Sequence}",
                    DisplayName, eventType, instanceId, attempt, PoisonRetryAttempts, se.SequencePosition);
            }
        }

        _poisoned = true;
        _poisonSequence = se.SequencePosition;
        _poisonInstanceId = instanceId;
        _poisonEventType = eventType;
        _logger.LogError(lastEx,
            "[{Projection}] Poison event halted runner after {Attempts} attempts. " +
            "Event {EventType} at sequence {Sequence} instance '{Instance}'. " +
            "Checkpoint will remain at the last successful sequence. Rebuild after a handler fix to recover.",
            DisplayName, PoisonRetryAttempts, eventType, se.SequencePosition, instanceId);
        return false;
    }

    private string SnapshotViewJson(object instance)
    {
        var viewObj = GetCompiledGetView()(instance);
        return JsonSerializer.Serialize(viewObj, viewObj.GetType(), ProjectionViewJson.Write);
    }

    /// <summary>
    /// Flush last successful position, then park until <paramref name="stoppingToken"/> is cancelled.
    /// Does not advance past the poison sequence. Do not throw — the poll loop's 5s backoff is not halt.
    /// </summary>
    private async Task HaltOnPoisonAsync(
        long lastSuccessfulPosition,
        long totalEventsProcessed,
        CancellationToken stoppingToken)
    {
        _poisoned = true;
        _lastAppliedSequence = lastSuccessfulPosition;
        StopSubscription();
        _sharedPipe?.Unregister(StorageKey);

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            cts.CancelAfter(TimeSpan.FromSeconds(30));
            await DrainWriteBehindAsync(cts.Token);
            await FlushViewsAndCheckpointCoreAsync(lastSuccessfulPosition, totalEventsProcessed, cts.Token);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(ex,
                "[{Projection}] Flush on poison halt did not complete cleanly; last success {Position}",
                DisplayName, lastSuccessfulPosition);
        }

        _logger.LogError(
            "[{Projection}] Parked after poison event {EventType} at sequence {PoisonSequence} " +
            "(instance '{Instance}'). Last successful position {Position}.",
            DisplayName, _poisonEventType, _poisonSequence, _poisonInstanceId, lastSuccessfulPosition);

        try
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    // ── Startup restoration ────────────────────────────────────────────────────

    private async Task RestoreSingleStreamInstancesAsync(CancellationToken ct)
    {
        var savedViews = await _viewStore.GetViewsByTypeAsync(StorageKey, ct);

        foreach (var (instanceId, viewJson) in savedViews)
        {
            // Only restore instances owned by this node
            var streamId = InstanceIdToStreamId(instanceId);
            if (!_partitioningService.OwnsStream(streamId ?? instanceId))
                continue;

            var instance = Activator.CreateInstance(_registration.HandlerType);
            if (instance is null)
                continue;

            SetProjectionState(instance, viewJson);
            _instances[instanceId] = instance;
            Touch(instanceId);

            // Prefer checkpoint column (last-applied) when available for freshness tracking.
            var withCheckpoint = await _viewStore.GetViewWithCheckpointAsync(
                StorageKey, instanceId, ct);
            if (withCheckpoint is not null)
            {
                _lastAppliedByInstance[instanceId] = withCheckpoint.Value.Checkpoint;
                _readCache?.Set(StorageKey, instanceId, viewJson, withCheckpoint.Value.Checkpoint);
            }
        }

        _logger.LogInformation(
            "[{Projection}] Restored {Count} instance(s) from view store",
            DisplayName, _instances.Count);
    }

    private async Task RestoreSingleInstanceAsync(CancellationToken ct)
    {
        var singleInstanceId = ProjectionInstanceIds.ForUnpartitioned(_options);

        var saved = await _viewStore.GetViewWithCheckpointAsync(
            StorageKey, singleInstanceId, ct);

        if (saved is null)
            return;

        var instance = Activator.CreateInstance(_registration.HandlerType);
        if (instance is null)
            return;

        SetProjectionState(instance, saved.Value.ViewData);
        _instances[singleInstanceId] = instance;
        _lastAppliedByInstance[singleInstanceId] = saved.Value.Checkpoint;
        Touch(singleInstanceId);
        _readCache?.Set(StorageKey, singleInstanceId, saved.Value.ViewData, saved.Value.Checkpoint);

        _logger.LogInformation(
            "[{Projection}] Restored unpartitioned instance '{InstanceId}' from view store (checkpoint {Pos})",
            DisplayName, singleInstanceId, saved.Value.Checkpoint);
    }

    // ── View flushing ──────────────────────────────────────────────────────────

    /// <summary>
    /// True when opportunistic (non-interval) flushes should be deferred because the runner is
    /// still far behind the event-store head.
    /// </summary>
    private async Task<bool> ShouldSkipOpportunisticFlushAsync(long lastPosition, CancellationToken ct)
    {
        if (!_options.SkipTailFlushWhileCatchingUp || !_supportsSequenceCheck)
            return false;

        try
        {
            var headSeq = await _eventStore.GetCurrentSequenceAsync(ct);
            return ProjectionFlushPolicy.ShouldSkipOpportunisticFlush(
                _options.SkipTailFlushWhileCatchingUp,
                supportsSequenceCheck: true,
                headSeq,
                lastPosition,
                _options.CatchUpFlushThreshold);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogDebug(ex,
                "[{Projection}] Could not evaluate catch-up flush skip; allowing opportunistic flush",
                DisplayName);
            return false;
        }
    }

    /// <summary>
    /// Retries a barriered flush when dirty views remain after a failed persist or an idle
    /// Subscribe drain. Without this, a live subscription that already consumed the events
    /// never calls flush again and the checkpoint stays unset.
    /// </summary>
    private async Task<bool> FlushLeftoverDirtiesAsync(
        long lastPosition, long totalEventsProcessed, CancellationToken ct)
    {
        if (_dirtyInstances.Count == 0)
            return false;
        if (await ShouldSkipOpportunisticFlushAsync(lastPosition, ct).ConfigureAwait(false))
            return false;

        await FlushViewsAndCheckpointAsync(lastPosition, totalEventsProcessed, ct).ConfigureAwait(false);
        return true;
    }

    private async Task FlushViewsAndCheckpointAsync(
        long position, long totalEventsProcessed, CancellationToken ct)
    {
        if (!_options.EnableWriteBehind)
        {
            await FlushViewsAndCheckpointCoreAsync(position, totalEventsProcessed, ct);
            return;
        }

        await ScheduleWriteBehindFlushAsync(position, totalEventsProcessed, ct);
    }

    /// <summary>
    /// Captures a flush wave on the runner thread, then drains it on a serialized background chain.
    /// Checkpoint advances only inside the drain after durable view ack. When in-flight waves reach
    /// <see cref="LightweightProjectionOptions.WriteBehindMaxInFlight"/>, awaits backpressure.
    /// </summary>
    private async Task ScheduleWriteBehindFlushAsync(
        long position, long totalEventsProcessed, CancellationToken ct)
    {
        var wave = CaptureFlushWave(position, totalEventsProcessed);
        if (wave.Writes.Count == 0 && position <= _lastCheckpointSequence)
            return;

        Task flushTask;
        int inFlight;
        lock (_writeBehindGate)
        {
            _writeBehindInFlight++;
            inFlight = _writeBehindInFlight;
            var prev = _writeBehindTail;
            _writeBehindTail = DrainWaveAfterAsync(prev, wave);
            flushTask = _writeBehindTail;
        }

        _logger.LogDebug(
            "[{Projection}] Write-behind scheduled at position {Position} ({DirtyCount} view(s), inFlight={InFlight})",
            DisplayName, position, wave.Writes.Count, inFlight);

        // Allow up to MaxInFlight waves without awaiting so the poll loop overlaps SQL I/O.
        // Await only when we would exceed the budget (backpressure).
        var maxInFlight = Math.Max(1, _options.WriteBehindMaxInFlight);
        if (inFlight > maxInFlight)
            await flushTask.WaitAsync(ct);
    }

    private async Task DrainWaveAfterAsync(Task previous, FlushWave wave)
    {
        try
        {
            await previous.ConfigureAwait(false);
        }
        catch
        {
            // Prior wave failure must not block later drains; dirties remain for retry.
        }

        try
        {
            await PersistFlushWaveAsync(wave, CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex,
                "[{Projection}] Write-behind flush failed at position {Position}; dirties retained for retry",
                DisplayName, wave.Position);
        }
        finally
        {
            lock (_writeBehindGate)
                _writeBehindInFlight = Math.Max(0, _writeBehindInFlight - 1);
        }
    }

    private async Task DrainWriteBehindAsync(CancellationToken ct)
    {
        Task pending;
        lock (_writeBehindGate)
            pending = _writeBehindTail;

        if (!pending.IsCompleted)
            await pending.WaitAsync(ct);
    }

    private FlushWave CaptureFlushWave(long position, long totalEventsProcessed)
    {
        // Snapshot dirty ids + JSON. Do not clear dirties until durable ack
        // (and only when the instance was not re-applied after this snapshot).
        List<(string InstanceId, string ViewData, long Checkpoint)> writes;
        lock (_instanceStateGate)
        {
            var dirtyIds = _dirtyInstances.ToArray();
            writes = new List<(string InstanceId, string ViewData, long Checkpoint)>(dirtyIds.Length);

            foreach (var instanceId in dirtyIds)
            {
                if (!_instances.TryGetValue(instanceId, out var instance))
                    continue;

                var viewObj = GetCompiledGetView()(instance);
                var viewJson = JsonSerializer.Serialize(viewObj, viewObj.GetType(), ProjectionViewJson.Write);
                // Persist per-instance last-applied sequence (Q3), not the flush-wave position.
                var applied = _lastAppliedByInstance.TryGetValue(instanceId, out var seq)
                    ? seq
                    : position;
                writes.Add((instanceId, viewJson, applied));
            }
        }

        return new FlushWave(position, totalEventsProcessed, writes);
    }

    private Task FlushViewsAndCheckpointCoreAsync(
        long position, long totalEventsProcessed, CancellationToken ct)
    {
        var wave = CaptureFlushWave(position, totalEventsProcessed);
        return PersistFlushWaveAsync(wave, ct);
    }

    /// <summary>
    /// Durability barrier: SaveViewsAsync (if any) → SaveCheckpointAsync → clear dirties that
    /// were not re-applied after the wave snapshot → optional working-set eviction.
    /// </summary>
    private async Task PersistFlushWaveAsync(FlushWave wave, CancellationToken ct)
    {
        var flushSw = Stopwatch.StartNew();
        try
        {
            if (wave.Writes.Count > 0)
            {
                // Bulk path: SqlViewStore uses one OPENJSON MERGE round-trip; other stores
                // override or fall back to looping SaveViewAsync via the interface default.
                await _viewStore.SaveViewsAsync(StorageKey, wave.Writes, ct);
            }

            // Advance checkpoint only after all view writes for this flush have succeeded.
            await _checkpointStore.SaveCheckpointAsync(new LawnDart.Projections.ProjectionCheckpoint
            {
                ProjectionType = StorageKey,
                NodeId = _options.NodeInstance,
                LastSequencePosition = wave.Position,
                LastUpdated = DateTime.UtcNow,
                TotalEventsProcessed = wave.TotalEventsProcessed
            }, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ProjectionTelemetry.RecordFlushError(StorageKey);
            throw;
        }

        flushSw.Stop();
        ProjectionTelemetry.RecordFlush(StorageKey, flushSw.Elapsed.TotalMilliseconds, wave.Writes.Count);

        lock (_instanceStateGate)
        {
            // Clear dirty only when the instance was not re-applied after this snapshot (write-behind).
            foreach (var (id, _, snapApplied) in wave.Writes)
            {
                if (_lastAppliedByInstance.TryGetValue(id, out var current) && current > snapApplied)
                    continue;
                _dirtyInstances.Remove(id);
            }

            // After dirties are clean, trim the working set if over budget (never evict dirty).
            EvictCleanInstancesIfNeeded_NoLock();
            ProjectionTelemetry.SetWorkingSetSize(StorageKey, _instances.Count);

            if (wave.Position > _lastCheckpointSequence)
                _lastCheckpointSequence = wave.Position;
        }

        _logger.LogDebug(
            "[{Projection}] Checkpoint saved at position {Position} ({DirtyCount} view(s))",
            DisplayName, wave.Position, wave.Writes.Count);

        ProjectionTelemetry.RecordCheckpointSaved(StorageKey, wave.Position);
    }

    private readonly record struct FlushWave(
        long Position,
        long TotalEventsProcessed,
        IReadOnlyList<(string InstanceId, string ViewData, long Checkpoint)> Writes);

    /// <summary>
    /// Evicts clean (non-dirty) instances when <see cref="LightweightProjectionOptions.WorkingSetMaxInstances"/>
    /// is exceeded. Dirty instances are never removed. Caller must hold <see cref="_instanceStateGate"/>.
    /// </summary>
    private void EvictCleanInstancesIfNeeded_NoLock()
    {
        var max = _options.WorkingSetMaxInstances;
        if (max <= 0 || _instances.Count <= max)
            return;

        var overflow = _instances.Count - max;
        var victims = _instances.Keys
            .Where(id => !_dirtyInstances.Contains(id))
            .OrderBy(id => _lastAccessTicks.GetValueOrDefault(id))
            .Take(overflow)
            .ToList();

        foreach (var id in victims)
        {
            _instances.Remove(id);
            _lastAppliedByInstance.Remove(id);
            _lastAccessTicks.Remove(id);
            // Hot read cache is a separate accelerator with its own budget; leave it alone.
            _logger.LogDebug(
                "[{Projection}] Evicted clean instance '{Instance}' (working set {Count}/{Max})",
                DisplayName, id, _instances.Count, max);
        }

        if (victims.Count > 0)
        {
            ProjectionTelemetry.RecordWorkingSetEviction(StorageKey, victims.Count);
            ProjectionTelemetry.SetWorkingSetSize(StorageKey, _instances.Count);
            _logger.LogInformation(
                "[{Projection}] Evicted {Evicted} clean instance(s); working set now {Count} (max {Max})",
                DisplayName, victims.Count, _instances.Count, max);
        }
    }

    /// <summary>Snapshot for tenant projection observability (health / admin endpoints).</summary>
    public ProjectionRunnerObservabilitySnapshot GetObservabilitySnapshot() =>
        new(
            _registration.LogicalName,
            _registration.Version,
            StorageKey,
            _lastCheckpointSequence,
            _maxPipelineLagMs,
            IsFaulted: _poisoned,
            WorkingSetCount: _instances.Count,
            LastAppliedSequence: _lastAppliedSequence);

    // ── Helpers ────────────────────────────────────────────────────────────────

    private bool IsMatchingStreamType(string streamId)
    {
        if (string.IsNullOrEmpty(_registration.StreamType))
            return true;

        // Handles both "Type:id" (legacy) and "tenant:Type:id" (multi-tenant) formats
        return StreamIdParser.ExtractAggregateType(streamId)
            .Equals(_registration.StreamType, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Builds the view store instance ID from a stream ID, applying tenant scope rules.
    /// </summary>
    private string BuildInstanceId(string streamId)
    {
        return _registration.TenantScope switch
        {
            TenantScope.TenantScoped => streamId, // full stream ID is the instance key
            TenantScope.TenantGlobal => StreamIdParser.ExtractAggregateIdString(streamId) ?? streamId,
            TenantScope.SystemGlobal => StreamIdParser.ExtractAggregateIdString(streamId) ?? streamId,
            _ => streamId
        };
    }

    /// <summary>
    /// Builds the view store instance ID for a <see cref="ProjectionKind.MultiStream"/> projection,
    /// combining tenant (when <see cref="TenantScope.TenantScoped"/>) and entity ID.
    /// </summary>
    private string BuildMultiStreamInstanceId(string streamId, string entityId)
    {
        return _registration.TenantScope switch
        {
            TenantScope.TenantScoped =>
                StreamIdParser.ExtractTenantId(streamId) is { } tenantId
                    ? $"{tenantId}:{entityId}"
                    : entityId,
            _ => entityId
        };
    }

    /// <summary>
    /// Attempts to reconstruct a stream ID from an instance ID stored in the view store.
    /// Used only for partition ownership checks during instance restoration.
    /// </summary>
    private static string? InstanceIdToStreamId(string instanceId) => instanceId;

    /// <summary>
    /// Restores a <c>ProjectionBase&lt;TView&gt;.State</c> property from serialized JSON
    /// using reflection, bypassing the <c>protected</c> access modifier.
    /// </summary>
    private void SetProjectionState(object instance, string viewJson)
    {
        try
        {
            var state = JsonSerializer.Deserialize(viewJson, _registration.ViewType, ProjectionViewJson.Read);
            if (state is null)
                return;

            // Walk the inheritance chain to find ProjectionBase<TView>
            var baseType = instance.GetType().BaseType;
            while (baseType != null && baseType != typeof(object))
            {
                if (baseType.IsGenericType &&
                    baseType.GetGenericTypeDefinition() == typeof(ProjectionBase<>))
                {
                    var prop = baseType.GetProperty("State",
                        BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
                    prop?.SetValue(instance, state);
                    return;
                }
                baseType = baseType.BaseType;
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "[{Projection}] Could not restore state from saved view; instance will rebuild",
                DisplayName);
        }
    }

    // ── Portable Subscribe ─────────────────────────────────────────────────────

    private sealed class DeliveryCursor
    {
        public long LastPosition;
        public long TotalEventsProcessed;
        public int EventsSinceCheckpoint;
    }

    /// <summary>True when a live <see cref="IEventStoreSubscriptions"/> handle is draining.</summary>
    internal bool HasLiveSubscription =>
        IsSubscriptionLive
        || (UsesSharedPipe && !_sharedPipePrivateCatchUp && _sharedPipe!.IsLive);

    internal bool IsSharedPipePrivateCatchUp => _sharedPipePrivateCatchUp;

    private bool IsSubscriptionLive =>
        _subscriptionHandle is not null && !_subscribeFaulted;

    /// <summary>Test hook: cancel the push subscription so the runner poll-recovers.</summary>
    internal void CancelSubscriptionForTests(bool allowResubscribe = false)
    {
        _suppressResubscribe = !allowResubscribe;
        _logger.LogWarning("[{Projection}] Test cancelled subscription; poll recover", DisplayName);
        FaultSubscription();
    }

    private void TryStartSubscription(long lastPosition, CancellationToken stoppingToken)
    {
        if (_suppressResubscribe
            || !_options.UseSubscribe
            || _subscriptions is null
            || stoppingToken.IsCancellationRequested)
            return;

        try
        {
            StopSubscription();
            _subscribeFaulted = false;
            _subscriptionCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            var fromSequence = Math.Max(0, lastPosition + 1);
            var subscriberId = $"lightweight:{StorageKey}:n{_options.NodeInstance}";
            var filter = BuildLiveSubscriptionFilter();
            _subscriptionHandle = _subscriptions.Subscribe(
                subscriberId,
                fromSequence,
                filter,
                _subscriptionCts.Token);
            _logger.LogInformation(
                "[{Projection}] Subscribe from {From} (subscriber={Subscriber}, filter={Filter})",
                DisplayName, fromSequence, subscriberId, filter.Kind);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "[{Projection}] Subscribe failed; using poll fallback",
                DisplayName);
            FaultSubscription();
        }
    }

    private void FaultSubscription()
    {
        _subscribeFaulted = true;
        StopSubscription();
    }

    private bool IsFarBehind(long lastApplied, long head) =>
        head - lastApplied > Math.Max(0L, _options.CatchUpFlushThreshold);

    private async Task<bool> TryFaultSubscribeIfFarBehindAsync(long lastApplied, CancellationToken ct)
    {
        if (!_supportsSequenceCheck)
            return false;

        long head;
        try
        {
            head = await _eventStore.GetCurrentSequenceAsync(ct).ConfigureAwait(false);
        }
        catch (NotSupportedException)
        {
            return false;
        }

        if (!IsFarBehind(lastApplied, head))
            return false;

        _logger.LogWarning(
            "[{Projection}] Far behind store head ({Lag}); poll recover from {Position}",
            DisplayName, head - lastApplied, lastApplied);
        _suppressResubscribe = true;
        FaultSubscription();
        return true;
    }

    private void StopSubscription()
    {
        try { _subscriptionCts?.Cancel(); }
        catch (ObjectDisposedException) { /* already torn down */ }

        try { _subscriptionHandle?.Dispose(); }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "[{Projection}] Subscription dispose failed", DisplayName);
        }

        _subscriptionHandle = null;
        _subscriptionCts?.Dispose();
        _subscriptionCts = null;
    }

    private async Task<int> DrainSubscriptionAsync(DeliveryCursor cursor, CancellationToken stoppingToken)
    {
        var handle = _subscriptionHandle;
        if (handle is null)
            return 0;

        var reader = handle.Events;
        CancellationTokenSource? timeoutCts = null;
        SequencedEvent first;
        try
        {
            if (!reader.TryRead(out first!))
            {
                // After Subscribe has delivered at least one match, lastPosition is the last
                // match seq — not H. Walk one BatchSize (HiLo-safe) immediately. Do not do
                // this before the first delivery: InMemory Subscribe scans historically on a
                // background task, and an immediate empty-window walk races that scan and
                // poll-walks the whole log.
                if (UsesFilteredEventQuery()
                    && handle.LastDeliveredSequence > 0
                    && _supportsSequenceCheck
                    && await _eventStore.GetCurrentSequenceAsync(stoppingToken).ConfigureAwait(false)
                        > cursor.LastPosition)
                {
                    return 0;
                }

                timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                var recovery = _options.SubscribeRecoveryPollInterval;
                if (recovery > TimeSpan.Zero)
                    timeoutCts.CancelAfter(recovery);

                try
                {
                    if (!await reader.WaitToReadAsync(timeoutCts.Token).ConfigureAwait(false))
                    {
                        _logger.LogWarning(
                            "[{Projection}] Subscription completed; poll recover",
                            DisplayName);
                        FaultSubscription();
                        return 0;
                    }
                }
                catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
                {
                    return 0;
                }

                if (!reader.TryRead(out first))
                    return 0;
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex,
                "[{Projection}] Subscription fault; poll recover from {Position}",
                DisplayName, cursor.LastPosition);
            FaultSubscription();
            return 0;
        }
        finally
        {
            timeoutCts?.Dispose();
        }

        // Persist/dispatch errors must reach the poll-loop backoff. Treating them as a
        // dead subscription disposes the handle and skips leftover dirty flushes.
        var count = 0;
        var current = first;
        while (true)
        {
            if (!await ApplySubscribedEventAsync(current, cursor, stoppingToken).ConfigureAwait(false))
                return count;
            count++;
            if (count >= Math.Max(1, _options.BatchSize))
                break;
            if (!reader.TryRead(out current!))
                break;
        }

        return count;
    }

    private async Task<int> DrainSharedPipeAsync(DeliveryCursor cursor, CancellationToken stoppingToken)
    {
        var reader = _sharedPipe?.GetReader(StorageKey);
        if (reader is null)
            return 0;

        if (!reader.TryRead(out var first))
        {
            if (_sharedPipe!.CaughtUpSequence > cursor.LastPosition)
                return 0;

            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            timeoutCts.CancelAfter(TimeSpan.FromMilliseconds(15));

            try
            {
                if (!await reader.WaitToReadAsync(timeoutCts.Token).ConfigureAwait(false))
                    return 0;
            }
            catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
            {
                return 0;
            }

            if (!reader.TryRead(out first))
                return 0;
        }

        var count = 0;
        var current = first;
        while (true)
        {
            if (!await ApplySubscribedEventAsync(current, cursor, stoppingToken).ConfigureAwait(false))
                return count;
            count++;
            if (count >= Math.Max(1, _options.BatchSize))
                break;
            if (!reader.TryRead(out current!))
                break;
        }

        return count;
    }

    private async Task<bool> ApplySubscribedEventAsync(
        SequencedEvent se,
        DeliveryCursor cursor,
        CancellationToken stoppingToken)
    {
        if (se.SequencePosition <= cursor.LastPosition)
            return true;

        if (!await DispatchEventAsync(se, stoppingToken).ConfigureAwait(false))
            return false;

        cursor.LastPosition = se.SequencePosition;
        cursor.TotalEventsProcessed++;
        cursor.EventsSinceCheckpoint++;

        if (cursor.EventsSinceCheckpoint >= _options.CheckpointInterval)
        {
            await FlushViewsAndCheckpointAsync(
                cursor.LastPosition, cursor.TotalEventsProcessed, stoppingToken).ConfigureAwait(false);
            cursor.EventsSinceCheckpoint = 0;
        }

        return true;
    }

    /// <summary>
    /// One typed/unfiltered poll window to fill Subscribe holes. Does not idle-sleep.
    /// After Subscribe has delivered matches, lastPosition is the last match — not H.
    /// Query through the current head (the allowed “queried <c>to</c>”) so an empty
    /// remaining range advances to that <c>to</c> without a HiLo jump and without
    /// walking one <see cref="LightweightProjectionOptions.BatchSize"/> at a time.
    /// </summary>
    private async Task RecoveryPollWindowAsync(DeliveryCursor cursor, CancellationToken stoppingToken)
    {
        var fromPosition = cursor.LastPosition + 1;
        var toPosition = cursor.LastPosition + _options.BatchSize;
        var queriedThroughHead = false;

        if (UsesFilteredEventQuery()
            && IsSubscriptionLive
            && _subscriptionHandle is { LastDeliveredSequence: > 0 }
            && _supportsSequenceCheck)
        {
            var head = await _eventStore.GetCurrentSequenceAsync(stoppingToken).ConfigureAwait(false);
            if (head > cursor.LastPosition)
            {
                toPosition = head;
                queriedThroughHead = true;
            }
        }

        var batchCount = 0;

        async Task ReadWindowAsync()
        {
            await foreach (var se in ReadEventsAsync(fromPosition, toPosition, stoppingToken).ConfigureAwait(false))
            {
                if (se.SequencePosition <= cursor.LastPosition)
                    continue;
                if (!await DispatchEventAsync(se, stoppingToken).ConfigureAwait(false))
                    return;
                cursor.LastPosition = se.SequencePosition;
                cursor.TotalEventsProcessed++;
                cursor.EventsSinceCheckpoint++;
                batchCount++;

                if (cursor.EventsSinceCheckpoint >= _options.CheckpointInterval)
                {
                    await FlushViewsAndCheckpointAsync(
                        cursor.LastPosition, cursor.TotalEventsProcessed, stoppingToken).ConfigureAwait(false);
                    cursor.EventsSinceCheckpoint = 0;
                }

                if (!queriedThroughHead && batchCount >= _options.BatchSize)
                    break;
            }
        }

        await ReadWindowAsync().ConfigureAwait(false);
        if (_poisoned)
            return;

        // Same visibility retry as the poll loop: empty typed window can be a store race.
        if (batchCount == 0 && UsesFilteredEventQuery())
            await ReadWindowAsync().ConfigureAwait(false);

        if (_poisoned)
            return;

        if (batchCount == 0 && UsesFilteredEventQuery() && _supportsSequenceCheck)
        {
            var headSeq = queriedThroughHead
                ? toPosition
                : await _eventStore.GetCurrentSequenceAsync(stoppingToken).ConfigureAwait(false);
            if (headSeq > cursor.LastPosition)
            {
                long windowEnd = queriedThroughHead
                    ? toPosition
                    : cursor.LastPosition + _options.BatchSize;
                long newLast = Math.Min(windowEnd, headSeq);
                if (newLast > cursor.LastPosition)
                    cursor.LastPosition = newLast;
            }
        }
    }

    // ── Compiled delegate accessors (lazy, cached per registration) ────────────

    private Dictionary<Type, Action<object, IEvent>> GetCompiledHandlers()
    {
        if (_compiledHandlers is not null)
            return _compiledHandlers;

        var descriptor = new ProjectionDescriptor
        {
            ProjectionType = StorageKey,
            HandlerType = _registration.HandlerType,
            ViewType = _registration.ViewType,
            Version = _registration.Version,
            StreamTypes = _registration.StreamType is not null
                ? [_registration.StreamType]
                : []
        };

        // Dispatch is keyed by Handle parameter CLR type. EventTypeName aliases are
        // store-query tokens only — both attributed and default-named events hit this map
        // via se.Event.GetType().
        _compiledHandlers = descriptor.GetCompiledHandlers();
        return _compiledHandlers;
    }

    private Func<object, object> GetCompiledGetView()
    {
        if (_compiledGetView is not null)
            return _compiledGetView;

        var descriptor = new ProjectionDescriptor
        {
            ProjectionType = StorageKey,
            HandlerType = _registration.HandlerType,
            ViewType = _registration.ViewType,
            Version = _registration.Version,
        };

        _compiledGetView = descriptor.GetCompiledGetView();
        return _compiledGetView;
    }
}
