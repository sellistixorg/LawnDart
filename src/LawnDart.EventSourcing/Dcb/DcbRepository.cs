using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using LawnDart;
using LawnDart.Authorization;
using LawnDart.Dcb;
using LawnDart.Dcb.Telemetry;
using LawnDart.EventStore;
using LawnDart.EventSourcing.Performance;
using LawnDart.Metadata;
using LawnDart.Snapshots;
using LawnDart.Snapshots.Telemetry;
using LawnDart.Tagging;

namespace LawnDart.EventSourcing.Dcb;

/// <summary>
/// Repository implementation for DCB-style entities.
/// Provides metadata enrichment, authorization, and tag management.
/// </summary>
public class DcbRepository : IDcbRepository
{
    private readonly IEventStore _eventStore;
    private readonly IMetadataProvider _metadataProvider;
    private readonly ITenantContextProvider _tenantContextProvider;
    private readonly ITagProvider? _tagProvider;
    private readonly AuthorizationService? _authorizationService;
    private readonly LawnDartOptions _options;
    private readonly EventSourcingOptions _eventSourcingOptions;
    private readonly ILogger<DcbRepository>? _logger;
    private readonly IDcbSnapshotStore? _dcbSnapshotStore;
    private readonly ISnapshotStrategyResolver? _strategyResolver;

    /// <summary>
    /// Initializes a new instance of the DcbRepository.
    /// </summary>
    public DcbRepository(
        IEventStore eventStore,
        IMetadataProvider metadataProvider,
        ITenantContextProvider tenantContextProvider,
        IOptions<LawnDartOptions> options,
        ITagProvider? tagProvider = null,
        AuthorizationService? authorizationService = null,
        ILogger<DcbRepository>? logger = null,
        IOptions<EventSourcingOptions>? eventSourcingOptions = null,
        IDcbSnapshotStore? dcbSnapshotStore = null,
        ISnapshotStrategyResolver? strategyResolver = null)
    {
        _eventStore = eventStore ?? throw new ArgumentNullException(nameof(eventStore));
        _metadataProvider = metadataProvider ?? throw new ArgumentNullException(nameof(metadataProvider));
        _tenantContextProvider = tenantContextProvider ?? throw new ArgumentNullException(nameof(tenantContextProvider));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _eventSourcingOptions = eventSourcingOptions?.Value ?? new EventSourcingOptions();
        _tagProvider = tagProvider;
        _authorizationService = authorizationService;
        _logger = logger;
        _dcbSnapshotStore = dcbSnapshotStore;
        _strategyResolver = strategyResolver;
    }
    
    /// <inheritdoc/>
    public async Task<TState> GetStateAsync<TState>(
        string[] tags,
        CancellationToken cancellationToken = default) where TState : IState, new()
    {
        using var activity = DcbTelemetry.StartGetStateActivity(typeof(TState).Name, tags);
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            // Enforce tenant isolation if enabled
            var tagsList = tags.ToList();
            if (_eventSourcingOptions.EnforceDcbTenantIsolation)
            {
                EnsureTenantTag(tagsList);
            }

            // ── DCB snapshot restore (optional) ────────────────────────────────
            // Derive a stable DCB id from the sorted tag set so snapshots survive
            // tag ordering changes across calls.
            var dcbId = DcbSnapshotId.FromLoadTags(tagsList);

            TState state;
            SnapshotInfo? snapshotInfo = null;
            TimeSpan snapshotRestoreElapsed = TimeSpan.Zero;

            if (_dcbSnapshotStore != null)
            {
                var restoreSw = Stopwatch.StartNew();
                try
                {
                    var (snappedState, info) = await _dcbSnapshotStore.LoadDcbSnapshotAsync<TState>(dcbId, cancellationToken);
                    restoreSw.Stop();
                    snapshotRestoreElapsed = restoreSw.Elapsed;
                    if (info != null && snappedState != null)
                    {
                        state        = snappedState;
                        snapshotInfo = info;
                        _logger?.LogDebug(
                            "Restored DCB state {StateType} from snapshot at global seq {Seq}",
                            typeof(TState).Name, info.GlobalSequence);
                        // Hit telemetry recorded after delta scan so delta count is known.
                    }
                    else
                    {
                        state = new TState();
                    }
                }
                catch (Exception ex)
                {
                    restoreSw.Stop();
                    _logger?.LogWarning(ex,
                        "DCB snapshot restore failed for {DcbId}; falling back to full scan",
                        dcbId);
                    SnapshotTelemetry.RecordMiss(typeof(TState).Name, "dcb", restoreSw.Elapsed);
                    state = new TState();
                }
            }
            else
            {
                state = new TState();
            }

            // Delta scan: from snapshot GlobalSequence+1 when a snapshot was restored,
            // or from the beginning when no snapshot exists.
            var fromSequencePosition = snapshotInfo != null ? snapshotInfo.GlobalSequence + 1 : (long?)null;
            var query = Query.FromItems(QueryItem.ByTags(tagsList.ToArray()));
            var events = new List<SequencedEvent>();
            await foreach (var evt in _eventStore.ReadByQueryStreamAsync(
                query,
                fromSequencePosition,
                cancellationToken: cancellationToken))
            {
                events.Add(evt);
            }

            // Record snapshot hit now that delta count is known.
            if (snapshotInfo != null)
                SnapshotTelemetry.RecordHit(typeof(TState).Name, "dcb", events.Count, snapshotRestoreElapsed);

            // Apply delta events using compiled expressions for performance
            foreach (var sequencedEvent in events.OrderBy(e => e.SequencePosition))
            {
                CompiledEventApplicator.ApplyEvent(state, sequencedEvent.Event);
            }
            
            DcbTelemetry.RecordGetState(events.Count, stopwatch.Elapsed);
            _logger?.LogDebug(
                "Rebuilt state {StateType} from {EventCount} delta events (snapshot: {HasSnapshot})",
                typeof(TState).Name, events.Count, snapshotInfo != null);
            
            return state;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    /// <inheritdoc/>
    public async Task<TState> GetStateAtAsync<TState>(
        string[] tags,
        DateTime? asOfTimestamp = null,
        long? asOfSequencePosition = null,
        CancellationToken cancellationToken = default) where TState : IState, new()
    {
        if (!asOfTimestamp.HasValue && !asOfSequencePosition.HasValue)
        {
            return await GetStateAsync<TState>(tags, cancellationToken).ConfigureAwait(false);
        }

        using var activity = DcbTelemetry.StartGetStateActivity(typeof(TState).Name, tags);

        var tagsList = tags.ToList();
        if (_eventSourcingOptions.EnforceDcbTenantIsolation)
        {
            EnsureTenantTag(tagsList);
        }

        var query = Query.FromItems(QueryItem.ByTags(tagsList.ToArray()));
        var events = new List<SequencedEvent>();
        await foreach (var evt in _eventStore.ReadByQueryStreamAsync(
            query,
            fromSequencePosition: null,
            toSequencePosition: asOfSequencePosition,
            toTimestamp: asOfTimestamp,
            cancellationToken: cancellationToken))
        {
            events.Add(evt);
        }

        var state = new TState();
        foreach (var sequencedEvent in events.OrderBy(e => e.SequencePosition))
        {
            CompiledEventApplicator.ApplyEvent(state, sequencedEvent.Event);
        }

        return state;
    }
    
    /// <inheritdoc/>
    public async Task<long> GetLastSequencePositionAsync(
        string[] tags,
        CancellationToken cancellationToken = default)
    {
        var tagsList = tags.ToList();
        if (_eventSourcingOptions.EnforceDcbTenantIsolation)
        {
            EnsureTenantTag(tagsList);
        }

        var query = Query.FromItems(QueryItem.ByTags(tagsList.ToArray()));
        return await _eventStore.GetMaxSequencePositionAsync(query, cancellationToken: cancellationToken);
    }
    
    /// <inheritdoc/>
    public async Task<IReadOnlyList<long>> AppendWithContextAsync(
        IEnumerable<IEvent> events,
        IEnumerable<string> tags,
        AppendCondition? condition = null,
        CommandMetadata? commandMetadata = null,
        CancellationToken cancellationToken = default)
    {
        using var activity = DcbTelemetry.StartAppendActivity(events.Count());
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            var eventsList = events.ToList();
            if (eventsList.Count == 0)
                throw new ArgumentException("At least one event is required", nameof(events));

            var tagsList = tags.ToList();
            
            // Enforce tenant isolation if enabled
            if (_eventSourcingOptions.EnforceDcbTenantIsolation)
            {
                EnsureTenantTag(tagsList);
            }

            // Get or create command metadata
            var metadata = commandMetadata ?? _metadataProvider.CaptureCommandMetadata();
            
            // Apply additional tags from tag provider
            if (_tagProvider != null)
            {
                foreach (var @event in eventsList)
                {
                    var additionalTags = _tagProvider.GetTags(@event, null);
                    tagsList.AddRange(additionalTags);
                }
            }
            
            // Remove duplicates
            tagsList = tagsList.Distinct().ToList();
            
            // Enrich event metadata
            var eventMetadata = _metadataProvider.EnrichEventMetadata(
                new EventMetadata(),
                metadata,
                eventsList.First());
            
            // Append to event store
            IReadOnlyList<long> sequencePositions;
            if (condition != null)
            {
                var appendResult = await _eventStore.AppendAsync(
                    eventsList,
                    condition,
                    eventMetadata,
                    tagsList,
                    cancellationToken);
                sequencePositions = appendResult.SequencePositions;
            }
            else
            {
                // Use tenant-aware stream ID for DCB
                var tenantId = metadata.TenantId ?? _tenantContextProvider.GetTenantId();
                var streamId = tenantId != null 
                    ? $"{tenantId}:dcb:{Guid.NewGuid()}"
                    : $"dcb:{Guid.NewGuid()}";
                    
                var appendResult = await _eventStore.AppendAsync(
                    streamId,
                    eventsList,
                    null,
                    eventMetadata,
                    tagsList,
                    cancellationToken);
                sequencePositions = appendResult.SequencePositions;
            }
            
            DcbTelemetry.RecordAppend(eventsList.Count, stopwatch.Elapsed);
            _logger?.LogDebug("Appended {Count} DCB events with {TagCount} tags", eventsList.Count, tagsList.Count);
            
            return sequencePositions;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }
    
    /// <inheritdoc/>
    public async Task<TEntity> CreateEntityAsync<TEntity>(
        string[] tags,
        CancellationToken cancellationToken = default) where TEntity : DcbEntity, new()
    {
        var tagsList = tags.ToList();
        if (_eventSourcingOptions.EnforceDcbTenantIsolation)
        {
            EnsureTenantTag(tagsList);
        }

        var entity = new TEntity();
        var loadTags = tagsList.ToArray();
        entity.SetTags(loadTags);
        entity.SetConsistencyTags(loadTags);
        
        _logger?.LogDebug("Created new DCB entity {EntityType} with {TagCount} tags", typeof(TEntity).Name, tagsList.Count);
        
        return await Task.FromResult(entity);
    }
    
    /// <inheritdoc/>
    public async Task<TEntity> GetOrCreateEntityAsync<TEntity>(
        string[] tags,
        CancellationToken cancellationToken = default) where TEntity : DcbEntity, new()
    {
        var tagsList = tags.ToList();
        if (_eventSourcingOptions.EnforceDcbTenantIsolation)
        {
            EnsureTenantTag(tagsList);
        }

        // ── DCB entity snapshot restore (optional) ──────────────────────────────
        var dcbId = DcbSnapshotId.FromLoadTags(tagsList);

        var entity = new TEntity();
        var loadTags = tagsList.ToArray();
        ApplyLoadTags(entity, loadTags);

        SnapshotInfo? snapshotInfo = null;
        TimeSpan entitySnapshotRestoreElapsed = TimeSpan.Zero;
        if (_dcbSnapshotStore != null)
        {
            var restoreSw = Stopwatch.StartNew();
            try
            {
                // Snapshot for DcbEntity: we store the entity itself (as TEntity) and restore it directly.
                var (snappedEntity, info) = await _dcbSnapshotStore.LoadDcbSnapshotAsync<TEntity>(dcbId, cancellationToken);
                restoreSw.Stop();
                entitySnapshotRestoreElapsed = restoreSw.Elapsed;
                if (info != null && snappedEntity != null)
                {
                    entity        = snappedEntity;
                    ApplyLoadTags(entity, loadTags);
                    entity.SetSnapshotCursor(info.GlobalSequence, info.TakenAtUtc);
                    snapshotInfo  = info;
                    _logger?.LogDebug(
                        "Restored DCB entity {EntityType} from snapshot at global seq {Seq}",
                        typeof(TEntity).Name, info.GlobalSequence);
                    // Hit telemetry recorded after delta replay so delta count is known.
                }
            }
            catch (Exception ex)
            {
                restoreSw.Stop();
                _logger?.LogWarning(ex,
                    "DCB entity snapshot restore failed for {DcbId}; falling back to full scan",
                    dcbId);
                SnapshotTelemetry.RecordMiss(typeof(TEntity).Name, "dcb", restoreSw.Elapsed);
                entity = new TEntity();
                ApplyLoadTags(entity, loadTags);
            }
        }

        // Delta replay from snapshot position or beginning.
        // The consistency marker is always recomputed from delta events — never reuse the stored marker.
        var fromSequencePosition = snapshotInfo != null ? snapshotInfo.GlobalSequence + 1 : (long?)null;
        var query = Query.FromItems(QueryItem.ByTags(tagsList.ToArray()));
        var queryResult = await _eventStore.ReadByQueryAsync(query, fromSequencePosition, limit: null, cancellationToken: cancellationToken);

        // Record snapshot hit now that delta count is known.
        if (snapshotInfo != null)
            SnapshotTelemetry.RecordHit(typeof(TEntity).Name, "dcb", queryResult.Events.Count, entitySnapshotRestoreElapsed);

        if (queryResult.Events.Count > 0)
        {
            entity.ReplayEvents(queryResult.Events);
            _logger?.LogDebug(
                "Loaded DCB entity {EntityType} with {EventCount} delta events (snapshot: {HasSnapshot})",
                typeof(TEntity).Name, queryResult.Events.Count, snapshotInfo != null);
        }
        else if (snapshotInfo == null)
        {
            _logger?.LogDebug("Created new DCB entity {EntityType} (no existing events)", typeof(TEntity).Name);
        }

        // Carry the fresh consistency marker forward so HandleCommandAsync can forward it
        // into the AppendCondition, letting hash-based backends skip the redundant re-read.
        entity.SetConsistencyMarker(queryResult.ConsistencyMarker);
        
        return entity;
    }
    
    /// <inheritdoc/>
    public async Task<TEntity> HandleCommandAsync<TEntity, TCommand>(
        TEntity entity,
        TCommand command,
        CommandMetadata? commandMetadata = null,
        CancellationToken cancellationToken = default)
        where TEntity : DcbEntity
        where TCommand : ICommand
    {
        using var activity = DcbTelemetry.StartHandleCommandActivity(typeof(TCommand).Name);
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Capture metadata if not provided
        commandMetadata ??= _metadataProvider.CaptureCommandMetadata();
        commandMetadata.CausationId ??= command.Id.ToString();
        
        try
        {
            // Authorization check (if enabled and service is registered)
            if (_options.EnableAuthorization && _authorizationService != null)
            {
                var authResult = await _authorizationService.AuthorizeCommandAsync(command, cancellationToken);
                if (!authResult.IsAuthorized)
                {
                    throw new UnauthorizedAccessException(
                        $"Authorization failed: {authResult.FailureReason}. " +
                        $"Failed checks: {string.Join(", ", authResult.FailedChecks)}");
                }
                
                // Capture authorization info in metadata
                commandMetadata.AuthorizedAt = DateTime.UtcNow;
                commandMetadata.AuthorizationPolicies = authResult.FailedChecks.ToArray();
            }

            if (CompiledCommandApplicator.OverridesHandleAsync(entity.GetType()))
            {
#pragma warning disable CS0618
                await entity.HandleAsync(command, cancellationToken);
#pragma warning restore CS0618
            }
            else
                await CompiledCommandApplicator.DispatchAsync(entity, command, cancellationToken);

            var pendingCount = entity.PendingEvents.Count();
            
            // Persist events — fence on the load-query tags (ConsistencyTags), not the
            // accumulated payload-tag union. After is the fence. ConsistencyMarker is
            // forwarded for compatibility; the shipped backends ignore it and evaluate
            // FailIfMatches as "no matches with seq > After".
            var fenceTags = entity.ConsistencyTags.Count > 0
                ? entity.ConsistencyTags
                : entity.Tags;
            var condition = AppendCondition.FailIfMatches(
                Query.FromItems(QueryItem.ByTags(fenceTags.ToArray())),
                entity.LastSequencePosition > 0 ? entity.LastSequencePosition : null)
                with { ConsistencyMarker = entity.ConsistencyMarker };
            
            var sequencePositions = await AppendWithContextAsync(
                entity.PendingEvents,
                entity.Tags,
                condition,
                commandMetadata,
                cancellationToken);
            
            // Update entity's last sequence position
            if (sequencePositions.Count > 0)
            {
                entity.SetLastSequencePosition(sequencePositions[^1]);
            }

            entity.RecordFlushedEvents(pendingCount);
            entity.ClearPendingEvents();

            DcbTelemetry.RecordHandleCommand(typeof(TCommand).Name, stopwatch.Elapsed);
            _logger?.LogDebug("Handled command {CommandType} on DCB entity {EntityType}", 
                typeof(TCommand).Name, typeof(TEntity).Name);

            // ── Fire-and-forget DCB snapshot (optional) ─────────────────────────
            if (_dcbSnapshotStore != null && _strategyResolver != null && sequencePositions.Count > 0)
            {
                var strategy = _strategyResolver.ResolveForDcb(typeof(TEntity));
                var globalSeq = sequencePositions[^1];
                var dcbId     = DcbIdFromFence(entity);

                var ctx = new SnapshotContext(
                    StreamId:                dcbId,
                    CurrentVersion:          entity.LastSequencePosition,
                    EventsSinceLastSnapshot: entity.EventsSinceLastSnapshot,
                    LastSnapshotUtc:         entity.LastSnapshotUtc,
                    GlobalSequence:          globalSeq);

                if (strategy.ShouldSnapshot(ctx))
                {
                    var capturedStore    = _dcbSnapshotStore;
                    var capturedEntity   = entity;
                    var capturedId       = dcbId;
                    var capturedSeq      = globalSeq;
                    var capturedTypeName = typeof(TEntity).Name;
                    var capturedTags     = fenceTags.ToArray();
                    entity.NoteSnapshotWritten(capturedSeq, DateTime.UtcNow);
                    _ = Task.Run(async () =>
                    {
                        var writeSw = Stopwatch.StartNew();
                        try
                        {
                            await capturedStore.SaveDcbSnapshotAsync(
                                capturedId, capturedSeq, capturedEntity,
                                consistencyMarker: null,
                                loadTags: capturedTags,
                                ct: CancellationToken.None);
                            writeSw.Stop();
                            _logger?.LogDebug(
                                "DCB snapshot written for {DcbId} at global seq {Seq}",
                                capturedId, capturedSeq);
                            SnapshotTelemetry.RecordWrite(capturedTypeName, "dcb", writeSw.Elapsed);
                        }
                        catch (Exception ex)
                        {
                            _logger?.LogWarning(ex,
                                "Fire-and-forget DCB snapshot write failed for {DcbId}",
                                capturedId);
                        }
                    });
                }
            }
            
            return entity;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    /// <summary>
    /// Applies the caller's load-query tags as both payload tags (initial) and the
    /// consistency fence. Replay/Emit may add payload tags; they must not change the fence.
    /// </summary>
    private static void ApplyLoadTags(DcbEntity entity, string[] loadTags)
    {
        entity.SetTags(loadTags);
        entity.SetConsistencyTags(loadTags);
    }

    private static string DcbIdFromFence(DcbEntity entity)
    {
        var tags = entity.ConsistencyTags.Count > 0 ? entity.ConsistencyTags : entity.Tags;
        return DcbSnapshotId.FromLoadTags(tags);
    }

    /// <summary>
    /// Ensures tags include a tenant tag when DCB tenant isolation is enforced.
    /// </summary>
    private void EnsureTenantTag(List<string> tags)
    {
        // Check if tags already contain a tenant tag
        if (tags.Any(t => t.StartsWith("tenant:", StringComparison.OrdinalIgnoreCase)))
        {
            return; // Tenant tag already present
        }

        // Try to get tenant from context
        var tenantId = _tenantContextProvider.GetTenantId();
        if (tenantId != null)
        {
            // Auto-inject tenant tag
            tags.Add($"tenant:{tenantId}");
            _logger?.LogDebug("Auto-injected tenant tag: tenant:{TenantId}", tenantId);
        }
        else
        {
            // Tenant is required but not available
            throw new InvalidOperationException(
                "DCB tenant isolation is enforced, but no tenant tag was provided and ITenantContextProvider did not return a tenant ID. " +
                "Either provide a tenant tag in the format 'tenant:{id}' or ensure ITenantContextProvider is properly configured.");
        }
    }
}
