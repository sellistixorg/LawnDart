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
using LawnDart.EventSourcing.Snapshots;
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
    private readonly ITagFormat? _tagFormat;
    private readonly UnknownTagWarningLog _unknownTagWarnings;
    private readonly AuthorizationService? _authorizationService;
    private readonly LawnDartOptions _options;
    private readonly EventSourcingOptions _eventSourcingOptions;
    private readonly ILogger<DcbRepository>? _logger;
    private readonly IDcbSnapshotStore? _dcbSnapshotStore;
    private readonly ISnapshotStrategyResolver? _strategyResolver;
    private readonly ISnapshotWriteQueue? _snapshotWriteQueue;

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
        : this(
            eventStore,
            metadataProvider,
            tenantContextProvider,
            options,
            tagProvider,
            authorizationService,
            logger,
            eventSourcingOptions,
            dcbSnapshotStore,
            strategyResolver,
            snapshotWriteQueue: null)
    {
    }

    internal DcbRepository(
        IEventStore eventStore,
        IMetadataProvider metadataProvider,
        ITenantContextProvider tenantContextProvider,
        IOptions<LawnDartOptions> options,
        ITagProvider? tagProvider,
        AuthorizationService? authorizationService,
        ILogger<DcbRepository>? logger,
        IOptions<EventSourcingOptions>? eventSourcingOptions,
        IDcbSnapshotStore? dcbSnapshotStore,
        ISnapshotStrategyResolver? strategyResolver,
        ISnapshotWriteQueue? snapshotWriteQueue,
        ITagFormat? tagFormat = null,
        UnknownTagWarningLog? unknownTagWarnings = null)
    {
        _eventStore = eventStore ?? throw new ArgumentNullException(nameof(eventStore));
        _metadataProvider = metadataProvider ?? throw new ArgumentNullException(nameof(metadataProvider));
        _tenantContextProvider = tenantContextProvider ?? throw new ArgumentNullException(nameof(tenantContextProvider));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _eventSourcingOptions = eventSourcingOptions?.Value ?? new EventSourcingOptions();
        _tagProvider = tagProvider;
        _tagFormat = tagFormat;
        _unknownTagWarnings = unknownTagWarnings ?? new UnknownTagWarningLog();
        _authorizationService = authorizationService;
        _logger = logger;
        _dcbSnapshotStore = dcbSnapshotStore;
        _strategyResolver = strategyResolver;
        _snapshotWriteQueue = snapshotWriteQueue;
    }
    
    /// <inheritdoc/>
    public async Task<TState> GetStateAsync<TState>(
        Query query,
        CancellationToken cancellationToken = default) where TState : IState, new()
    {
        var decision = PrepareDecisionQuery(query);
        var tags = CollectTags(decision);
        using var activity = DcbTelemetry.StartGetStateActivity(typeof(TState).Name, tags);
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            WarnOnQueriedTags(tags);

            // Snapshot identity is the hash of the query after tenant injection.
            var dcbId = DcbSnapshotId.FromQuery(decision);

            TState state;
            SnapshotInfo? snapshotInfo = null;
            TimeSpan snapshotRestoreElapsed = TimeSpan.Zero;

            if (_dcbSnapshotStore != null)
            {
                var restoreSw = Stopwatch.StartNew();
                try
                {
                    var (snappedState, info) = await _dcbSnapshotStore.LoadDcbSnapshotAsync<TState>(dcbId, cancellationToken).ConfigureAwait(false);
                    restoreSw.Stop();
                    snapshotRestoreElapsed = restoreSw.Elapsed;
                    if (info != null && snappedState != null)
                    {
                        state        = snappedState;
                        snapshotInfo = info;
                        if (_logger is not null)
                            DcbRepositoryLog.StateRestored(_logger, typeof(TState).Name, info.GlobalSequence);
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
                    if (_logger is not null)
                        DcbRepositoryLog.StateRestoreFailed(_logger, ex, dcbId);
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
            var events = new List<SequencedEvent>();
            await foreach (var evt in _eventStore.ReadByQueryStreamAsync(
                decision,
                fromSequencePosition,
                cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                events.Add(evt);
            }

            // Record snapshot hit now that delta count is known.
            if (snapshotInfo != null)
                SnapshotTelemetry.RecordHit(typeof(TState).Name, "dcb", events.Count, snapshotRestoreElapsed);

            foreach (var sequencedEvent in events.OrderBy(e => e.SequencePosition))
            {
                CompiledEventApplicator.ApplyEvent(state, sequencedEvent.Event);
            }
            
            DcbTelemetry.RecordGetState(events.Count, stopwatch.Elapsed);
            if (_logger is not null)
                DcbRepositoryLog.StateRebuilt(_logger, typeof(TState).Name, events.Count, snapshotInfo != null);
            
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
        Query query,
        DateTime? asOfTimestamp = null,
        long? asOfSequencePosition = null,
        CancellationToken cancellationToken = default) where TState : IState, new()
    {
        if (!asOfTimestamp.HasValue && !asOfSequencePosition.HasValue)
        {
            return await GetStateAsync<TState>(query, cancellationToken).ConfigureAwait(false);
        }

        var decision = PrepareDecisionQuery(query);
        var tags = CollectTags(decision);
        using var activity = DcbTelemetry.StartGetStateActivity(typeof(TState).Name, tags);

        WarnOnQueriedTags(tags);

        var events = new List<SequencedEvent>();
        await foreach (var evt in _eventStore.ReadByQueryStreamAsync(
            decision,
            fromSequencePosition: null,
            toSequencePosition: asOfSequencePosition,
            toTimestamp: asOfTimestamp,
            cancellationToken: cancellationToken).ConfigureAwait(false))
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
        Query query,
        CancellationToken cancellationToken = default)
    {
        var decision = PrepareDecisionQuery(query);
        WarnOnQueriedTags(CollectTags(decision));
        return await _eventStore.GetMaxSequencePositionAsync(decision, cancellationToken: cancellationToken).ConfigureAwait(false);
    }
    
    /// <inheritdoc/>
    public Task<IReadOnlyList<long>> AppendWithContextAsync(
        IEnumerable<TaggedEvent> events,
        AppendCondition? condition = null,
        CommandMetadata? commandMetadata = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(events);
        var eventsList = events.ToList();
        if (eventsList.Any(e => e is null))
            throw new ArgumentException("Events cannot contain null items", nameof(events));
        return AppendEventsAsync(eventsList, condition, commandMetadata, cancellationToken);
    }

    /// <summary>
    /// Appends events so each event is written with only its own tags, plus the tag
    /// provider's tags for that event and the tenant tag when tenant isolation is on.
    /// </summary>
    /// <param name="callerEvents">Events to append, each with the tags the caller gave for it.</param>
    /// <param name="condition">Append condition, or <c>null</c> for an unconditional append.</param>
    /// <param name="commandMetadata">Command metadata, captured when <c>null</c>.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    private async Task<IReadOnlyList<long>> AppendEventsAsync(
        IReadOnlyList<TaggedEvent> callerEvents,
        AppendCondition? condition,
        CommandMetadata? commandMetadata,
        CancellationToken cancellationToken)
    {
        using var activity = DcbTelemetry.StartAppendActivity(callerEvents.Count);
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        
        try
        {
            if (callerEvents.Count == 0)
                throw new ArgumentException("At least one event is required", "events");

            WarnOnCondition(condition);

            // Get or create command metadata
            var metadata = commandMetadata ?? _metadataProvider.CaptureCommandMetadata();

            // Build each event's tags: that event's own tags, the tag provider's tags for
            // that event, and the tenant tag when tenant isolation is on. Tags from other
            // events in the batch are not added.
            string? tenantTag = null;
            var tagged = new List<TaggedEvent>(callerEvents.Count);
            foreach (var item in callerEvents)
            {
                var eventTags = new List<string>(item.Tags);
                if (_eventSourcingOptions.EnforceDcbTenantIsolation
                    && !eventTags.Any(t => t.StartsWith("tenant:", StringComparison.OrdinalIgnoreCase)))
                {
                    if (tenantTag is null)
                    {
                        var injected = new List<string>();
                        EnsureTenantTag(injected);
                        tenantTag = injected[0];
                    }

                    eventTags.Add(tenantTag);
                }

                if (_tagProvider != null)
                    eventTags.AddRange(_tagProvider.GetTags(item.Event, null));
                tagged.Add(new TaggedEvent(item.Event, eventTags));
            }
            
            // Enrich event metadata
            var eventMetadata = _metadataProvider.EnrichEventMetadata(
                new EventMetadata(),
                metadata,
                tagged[0].Event);

            if (_options.RequireTenantId)
            {
                var tenantId = metadata.TenantId ?? _tenantContextProvider.GetTenantId();
                if (!string.IsNullOrWhiteSpace(tenantId))
                    LawnDartNames.ValidateTenantId(tenantId, nameof(commandMetadata), _options.ValidateNames);
            }

            string? streamId = null;
            if (condition == null)
            {
                // Use tenant-aware stream ID for DCB
                var tenantId = metadata.TenantId ?? _tenantContextProvider.GetTenantId();
                streamId = tenantId != null 
                    ? $"{tenantId}:dcb:{Guid.NewGuid()}"
                    : $"dcb:{Guid.NewGuid()}";
            }

            // Append to event store
            // One atomic append; each event carries only its own tags.
            var appendResult = condition != null
                ? await _eventStore.AppendAsync(tagged, condition, eventMetadata, cancellationToken).ConfigureAwait(false)
                : await _eventStore.AppendAsync(streamId!, tagged, null, eventMetadata, cancellationToken).ConfigureAwait(false);

            var sequencePositions = appendResult.SequencePositions;
            
            DcbTelemetry.RecordAppend(tagged.Count, stopwatch.Elapsed);
            if (_logger is not null)
            {
                var distinctTagCount = tagged.SelectMany(t => t.Tags).Distinct(StringComparer.Ordinal).Count();
                DcbRepositoryLog.EventsAppended(_logger, tagged.Count, distinctTagCount);
            }
            
            return sequencePositions;
        }
        catch (Exception ex)
        {
            activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    /// <inheritdoc/>
    public Task<TEntity> CreateEntityAsync<TEntity>(
        Query query,
        CancellationToken cancellationToken = default) where TEntity : DcbEntity, new()
    {
        var decision = PrepareDecisionQuery(query);
        var entity = new TEntity();
        ApplyDecision(entity, decision);

        if (_logger is not null)
            DcbRepositoryLog.EntityCreated(_logger, typeof(TEntity).Name, CollectTags(decision).Length);

        return Task.FromResult(entity);
    }
    
    /// <inheritdoc/>
    public async Task<TEntity> GetOrCreateEntityAsync<TEntity>(
        Query query,
        CancellationToken cancellationToken = default) where TEntity : DcbEntity, new()
    {
        var decision = PrepareDecisionQuery(query);
        WarnOnQueriedTags(CollectTags(decision));

        var dcbId = DcbSnapshotId.FromQuery(decision);

        var entity = new TEntity();
        ApplyDecision(entity, decision);

        SnapshotInfo? snapshotInfo = null;
        TimeSpan entitySnapshotRestoreElapsed = TimeSpan.Zero;
        if (_dcbSnapshotStore != null)
        {
            var restoreSw = Stopwatch.StartNew();
            try
            {
                // Snapshot for DcbEntity: we store the entity itself (as TEntity) and restore it directly.
                var (snappedEntity, info) = await _dcbSnapshotStore.LoadDcbSnapshotAsync<TEntity>(dcbId, cancellationToken).ConfigureAwait(false);
                restoreSw.Stop();
                entitySnapshotRestoreElapsed = restoreSw.Elapsed;
                if (info != null && snappedEntity != null)
                {
                    entity        = snappedEntity;
                    ApplyDecision(entity, decision);
                    entity.SetSnapshotCursor(info.GlobalSequence, info.TakenAtUtc);
                    snapshotInfo  = info;
                    if (_logger is not null)
                        DcbRepositoryLog.EntityRestored(_logger, typeof(TEntity).Name, info.GlobalSequence);
                    // Hit telemetry recorded after delta replay so delta count is known.
                }
            }
            catch (Exception ex)
            {
                restoreSw.Stop();
                if (_logger is not null)
                    DcbRepositoryLog.EntityRestoreFailed(_logger, ex, dcbId);
                SnapshotTelemetry.RecordMiss(typeof(TEntity).Name, "dcb", restoreSw.Elapsed);
                entity = new TEntity();
                ApplyDecision(entity, decision);
            }
        }

        // Delta replay from snapshot position or beginning.
        // The consistency marker is always recomputed from delta events. The stored marker is not reused.
        var fromSequencePosition = snapshotInfo != null ? snapshotInfo.GlobalSequence + 1 : (long?)null;
        var queryResult = await _eventStore.ReadByQueryAsync(decision, fromSequencePosition, limit: null, cancellationToken: cancellationToken).ConfigureAwait(false);

        // Record snapshot hit now that delta count is known.
        if (snapshotInfo != null)
            SnapshotTelemetry.RecordHit(typeof(TEntity).Name, "dcb", queryResult.Events.Count, entitySnapshotRestoreElapsed);

        if (queryResult.Events.Count > 0)
        {
            entity.ReplayEvents(queryResult.Events);
            if (_logger is not null)
                DcbRepositoryLog.EntityLoaded(
                    _logger, typeof(TEntity).Name, queryResult.Events.Count, snapshotInfo != null);
        }
        else if (snapshotInfo == null)
        {
            if (_logger is not null)
                DcbRepositoryLog.EntityCreatedEmpty(_logger, typeof(TEntity).Name);
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
                var authResult = await _authorizationService.AuthorizeCommandAsync(command, cancellationToken).ConfigureAwait(false);
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
                await entity.HandleAsync(command, cancellationToken).ConfigureAwait(false);
#pragma warning restore CS0618
            }
            else
                await CompiledCommandApplicator.DispatchAsync(entity, command, cancellationToken).ConfigureAwait(false);

            var pendingCount = entity.PendingEvents.Count();

            // Fence on the query the repository loaded. There is no fallback to Tags.
            // Each emitted event should match at least one item. After is the last sequence
            // this instance has seen, including an event that matches no item.
            var decision = entity.DecisionQuery
                ?? throw new InvalidOperationException(
                    "This entity has no decision query. Load it with GetOrCreateEntityAsync or CreateEntityAsync before HandleCommandAsync.");
            var condition = AppendCondition.FailIfMatches(
                decision,
                entity.LastSequencePosition > 0 ? entity.LastSequencePosition : null)
                with { ConsistencyMarker = entity.ConsistencyMarker };

            // Each event is written with only the tags passed to Emit for that event (plus
            // the tag provider's tags for that event, and the tenant tag when tenant isolation
            // is on). The decision query, tags read during replay, and tags of other events in
            // this command are not written.
            var pendingEvents = entity.PendingEvents.ToList();
            var pendingTags = entity.PendingEventTags;
            var pendingTagged = new List<TaggedEvent>(pendingEvents.Count);
            for (var i = 0; i < pendingEvents.Count; i++)
                pendingTagged.Add(new TaggedEvent(pendingEvents[i], i < pendingTags.Count ? pendingTags[i] : []));
            var sequencePositions = await AppendEventsAsync(
                pendingTagged,
                condition,
                commandMetadata,
                cancellationToken).ConfigureAwait(false);
            
            // Update entity's last sequence position
            if (sequencePositions.Count > 0)
            {
                entity.SetLastSequencePosition(sequencePositions[^1]);
            }

            entity.RecordFlushedEvents(pendingCount);
            entity.ClearPendingEvents();

            DcbTelemetry.RecordHandleCommand(typeof(TCommand).Name, stopwatch.Elapsed);
            if (_logger is not null)
                DcbRepositoryLog.CommandHandled(_logger, typeof(TCommand).Name, typeof(TEntity).Name);

            // Capture on this thread, enqueue wait-free. The hosted consumer writes.
            if (_dcbSnapshotStore != null && _strategyResolver != null && _snapshotWriteQueue != null
                && sequencePositions.Count > 0)
            {
                var strategy = _strategyResolver.ResolveForDcb(typeof(TEntity));
                var globalSeq = sequencePositions[^1];
                var dcbId     = DcbSnapshotId.FromQuery(decision);

                var ctx = new SnapshotContext(
                    StreamId:                dcbId,
                    CurrentVersion:          entity.LastSequencePosition,
                    EventsSinceLastSnapshot: entity.EventsSinceLastSnapshot,
                    LastSnapshotUtc:         entity.LastSnapshotUtc,
                    GlobalSequence:          globalSeq);

                if (strategy.ShouldSnapshot(ctx))
                {
                    var captured = entity.CaptureCommittedSnapshot();
                    var capturedTags = new[] { DecisionQueryText.Format(decision) };
                    entity.NoteSnapshotWritten(globalSeq, DateTime.UtcNow);
                    _snapshotWriteQueue.TryEnqueue(new SnapshotWriteItem
                    {
                        Kind = "dcb",
                        EntityType = typeof(TEntity).Name,
                        Id = dcbId,
                        Version = captured.Version,
                        GlobalSequence = globalSeq,
                        Payload = captured.Payload,
                        StateType = captured.StateType,
                        DcbStore = _dcbSnapshotStore,
                        ConsistencyMarker = entity.ConsistencyMarker,
                        LoadTags = capturedTags
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
    /// Logs once per tag type when a load tag does not round-trip through the context format.
    /// Never throws and never changes the tag list.
    /// </summary>
    private void WarnOnQueriedTags(IEnumerable<string>? tags)
    {
        if (!_eventSourcingOptions.WarnOnUnknownTag || _logger is null || _tagFormat is null)
            return;

        _unknownTagWarnings.Warn(_logger, _tagFormat, tags);
    }

    /// <summary>
    /// Logs once for each tag inside an <see cref="AppendCondition.FailIfMatches"/> query.
    /// </summary>
    private void WarnOnCondition(AppendCondition? condition)
    {
        if (condition is null)
            return;

        foreach (var item in condition.FailIfEventsMatch.Items)
        {
            if (item.Tags is { Count: > 0 })
                WarnOnQueriedTags(item.Tags);
        }
    }

    /// <summary>
    /// Stores the decision query and the tags it names. Replay and Emit may add
    /// diagnostic tags. They do not change the query.
    /// </summary>
    private static void ApplyDecision(DcbEntity entity, Query decision)
    {
        entity.SetTags(CollectTags(decision));
        entity.SetDecisionQuery(decision);
    }

    /// <summary>
    /// Checks the query, then copies it when tenant isolation must add a tenant tag.
    /// The caller's query is not mutated.
    /// </summary>
    private Query PrepareDecisionQuery(Query query)
    {
        ArgumentNullException.ThrowIfNull(query);
        if (query.Items.Count == 0)
            throw new ArgumentException(
                "A decision query needs at least one item. Query.All() matches every event and is not a decision boundary.",
                nameof(query));

        foreach (var item in query.Items)
        {
            if (item is null)
                throw new ArgumentException("A decision query item cannot be null.", nameof(query));
            if (item.PartitionFilter is not null)
                throw new ArgumentException(
                    "A decision query item cannot set PartitionFilter. Partition filters stay on event store queries.",
                    nameof(query));

            var hasTypes = item.Types is { Count: > 0 };
            var hasTags = item.Tags is { Count: > 0 };
            if (!hasTypes && !hasTags)
                throw new ArgumentException(
                    "A decision query item needs an event type or a tag.",
                    nameof(query));

            if (item.Types is not null && item.Types.Any(t => t is null))
                throw new ArgumentException("A decision query event type cannot be null.", nameof(query));
            if (item.Tags is not null && item.Tags.Any(t => t is null))
                throw new ArgumentException("A decision query tag cannot be null.", nameof(query));
        }

        if (!_eventSourcingOptions.EnforceDcbTenantIsolation)
            return query;

        if (query.Items.Any(item =>
                item.Tags?.Any(t => t.StartsWith("tenant:", StringComparison.OrdinalIgnoreCase)) == true))
            return query;

        var tenantId = _tenantContextProvider.GetTenantId();
        if (tenantId is null)
        {
            throw new InvalidOperationException(
                "DCB tenant isolation is enforced, but no tenant tag was provided and ITenantContextProvider did not return a tenant ID. " +
                "Either provide a tenant tag in the format 'tenant:{id}' or ensure ITenantContextProvider is properly configured.");
        }

        var tenantTag = $"tenant:{tenantId}";
        var cloned = new QueryItem[query.Items.Count];
        for (var i = 0; i < query.Items.Count; i++)
        {
            var item = query.Items[i];
            var tags = item.Tags is { Count: > 0 }
                ? item.Tags.Append(tenantTag).ToArray()
                : new[] { tenantTag };
            cloned[i] = new QueryItem
            {
                Types = item.Types is { Count: > 0 } ? item.Types.ToArray() : item.Types,
                Tags = tags
            };
        }

        if (_logger is not null)
            DcbRepositoryLog.TenantTagInjected(_logger, tenantId);

        return Query.FromItems(cloned);
    }

    /// <summary>
    /// Tags named by the query, in item order, first copy kept.
    /// </summary>
    private static string[] CollectTags(Query query)
    {
        var tags = new List<string>();
        foreach (var item in query.Items)
        {
            if (item.Tags is null)
                continue;
            foreach (var tag in item.Tags)
            {
                if (!tags.Contains(tag))
                    tags.Add(tag);
            }
        }

        return tags.ToArray();
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
            if (_logger is not null)
                DcbRepositoryLog.TenantTagInjected(_logger, tenantId);
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
