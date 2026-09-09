using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using LawnDart.Aggregates;
using LawnDart.Authorization;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Snapshots;
using LawnDart.Snapshots.Telemetry;
using LawnDart.Tagging;
using LawnDart.EventSourcing.Performance;

namespace LawnDart.EventSourcing.Aggregates;

/// <summary>
/// Repository implementation for loading and persisting aggregates with metadata propagation.
/// Supports optional authorization checking via AuthorizationService.
/// </summary>
public class AggregateRepository : IAggregateRepository
{
    private readonly IEventStore _eventStore;
    private readonly IMetadataProvider _metadataProvider;
    private readonly ITenantContextProvider _tenantContextProvider;
    private readonly ITagProvider? _tagProvider;
    private readonly AuthorizationService? _authorizationService;
    private readonly LawnDartOptions _options;
    private readonly ILogger<AggregateRepository>? _logger;
    private readonly ISnapshotStore? _snapshotStore;
    private readonly ISnapshotStrategyResolver? _strategyResolver;

    public AggregateRepository(
        IEventStore eventStore,
        IMetadataProvider metadataProvider,
        ITenantContextProvider tenantContextProvider,
        IOptions<LawnDartOptions> options,
        ITagProvider? tagProvider = null,
        AuthorizationService? authorizationService = null,
        ILogger<AggregateRepository>? logger = null,
        ISnapshotStore? snapshotStore = null,
        ISnapshotStrategyResolver? strategyResolver = null)
    {
        _eventStore = eventStore ?? throw new ArgumentNullException(nameof(eventStore));
        _metadataProvider = metadataProvider ?? throw new ArgumentNullException(nameof(metadataProvider));
        _tenantContextProvider = tenantContextProvider ?? throw new ArgumentNullException(nameof(tenantContextProvider));
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _tagProvider = tagProvider;
        _authorizationService = authorizationService;
        _logger = logger;
        _snapshotStore = snapshotStore;
        _strategyResolver = strategyResolver;
    }

    public Task<T?> GetAsync<T>(Guid id, CancellationToken cancellationToken = default) where T : AggregateRoot, new()
        => GetAsync<T>(ResolveStreamId<T>(id), cancellationToken);

    public async Task<T?> GetAsync<T>(string streamId, CancellationToken cancellationToken = default) where T : AggregateRoot, new()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);

        // ── Snapshot restore (optional) ────────────────────────────────────────
        // Create aggregate first so we can call the virtual snapshot hook.
        var aggregate = new T();
        aggregate.SetStreamId(streamId);

        SnapshotInfo? snapshotInfo = null;
        TimeSpan snapshotRestoreElapsed = TimeSpan.Zero;
        if (_snapshotStore != null)
        {
            var restoreSw = Stopwatch.StartNew();
            try
            {
                snapshotInfo = await aggregate.TryRestoreFromSnapshotAsync(_snapshotStore, cancellationToken);
                restoreSw.Stop();
                snapshotRestoreElapsed = restoreSw.Elapsed;
                if (snapshotInfo != null)
                {
                    _logger?.LogDebug(
                        "Restored {Type} {StreamId} from snapshot at version {Version} (global seq {Seq})",
                        typeof(T).Name, streamId, snapshotInfo.Version, snapshotInfo.GlobalSequence);
                    // Hit telemetry is recorded after delta replay so the delta count is known.
                }
            }
            catch (Exception ex)
            {
                restoreSw.Stop();
                // Corrupt or incompatible snapshot — fall back to full replay.
                _logger?.LogWarning(ex,
                    "Snapshot restore failed for {Type} {StreamId}; falling back to full replay",
                    typeof(T).Name, streamId);
                SnapshotTelemetry.RecordMiss(typeof(T).Name, "aggregate", restoreSw.Elapsed);
                snapshotInfo = null;
                aggregate = new T();
                aggregate.SetStreamId(streamId);
            }
        }

        // Delta replay: from version 0 for full replay, or snapshotVersion+1 for delta.
        var fromVersion = snapshotInfo != null ? snapshotInfo.Version + 1 : 0;
        var events = await _eventStore.ReadStreamAsync(streamId, fromVersion, cancellationToken: cancellationToken);

        if (events.Count == 0 && snapshotInfo == null)
        {
            _logger?.LogDebug("Aggregate {Type} with stream {StreamId} not found", typeof(T).Name, streamId);
            return null;
        }

        // Record snapshot hit now that we know the delta event count.
        if (snapshotInfo != null)
            SnapshotTelemetry.RecordHit(typeof(T).Name, "aggregate", events.Count, snapshotRestoreElapsed);

        // Replay events to rebuild state
        if (events.Count > 0)
        {
            var eventList = events.Select(e => e.Event).ToList();
            
            // Try to call ReplayEvents method if available (preferred path)
            var replayMethod = aggregate.GetType().GetMethod("ReplayEvents", new[] { typeof(IEnumerable<IEvent>) });
            if (replayMethod != null)
            {
                replayMethod.Invoke(aggregate, new object[] { eventList });
            }
            else
            {
                var stateProperty = aggregate.GetType().GetProperty("State");
                if (stateProperty != null)
                {
                    var state = stateProperty.GetValue(aggregate) as IState;
                    if (state != null)
                    {
                        foreach (var @event in eventList)
                        {
                            CompiledEventApplicator.ApplyEvent(state, @event);
                        }
                    }
                }
            }

            // Set version from last delta event
            aggregate.SetVersion(events[^1].Version);
            aggregate.SetCommittedVersion(events[^1].Version);
        }
        else if (snapshotInfo != null)
        {
            // Snapshot-only: delta = 0. CommittedVersion = snapshot version.
            aggregate.SetCommittedVersion(snapshotInfo.Version);
        }

        _logger?.LogDebug(
            "Loaded aggregate {Type} with stream {StreamId}, version {Version}, {DeltaEventCount} delta events (snapshot: {HasSnapshot})",
            typeof(T).Name,
            streamId,
            aggregate.Version,
            events.Count,
            snapshotInfo != null);

        return aggregate as T;
    }

    public Task<T> CreateAsync<T>(Guid id, CancellationToken cancellationToken = default) where T : AggregateRoot, new()
        => CreateAsync<T>(ResolveStreamId<T>(id), cancellationToken);

    public Task<T> CreateAsync<T>(string streamId, CancellationToken cancellationToken = default) where T : AggregateRoot, new()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(streamId);

        var aggregate = new T();
        aggregate.SetStreamId(streamId);
        aggregate.SetCommittedVersion(-1);   // -1 = brand new, never flushed: "stream must not exist yet"

        _logger?.LogDebug("Created new aggregate {Type} with StreamId: {StreamId}", typeof(T).Name, streamId);

        return Task.FromResult(aggregate);
    }

    public Task<T> GetOrCreateAsync<T>(Guid id, CancellationToken cancellationToken = default) where T : AggregateRoot, new()
        => GetOrCreateAsync<T>(ResolveStreamId<T>(id), cancellationToken);

    public async Task<T> GetOrCreateAsync<T>(string streamId, CancellationToken cancellationToken = default) where T : AggregateRoot, new()
    {
        var existing = await GetAsync<T>(streamId, cancellationToken);
        if (existing != null)
        {
            _logger?.LogDebug("Found existing aggregate {Type} with stream {StreamId}, version {Version}", typeof(T).Name, streamId, existing.Version);
            return existing;
        }

        var aggregate = await CreateAsync<T>(streamId, cancellationToken);
        _logger?.LogDebug("Created new aggregate {Type} with stream {StreamId} (did not exist)", typeof(T).Name, streamId);
        return aggregate;
    }

    public async Task SaveAsync(
        AggregateRoot aggregate,
        CommandMetadata commandMetadata,
        CancellationToken cancellationToken = default)
    {
        if (aggregate == null)
            throw new ArgumentNullException(nameof(aggregate));

        if (commandMetadata == null)
            throw new ArgumentNullException(nameof(commandMetadata));

        var events = aggregate.PendingEvents.ToList();
        if (events.Count == 0)
        {
            _logger?.LogDebug("No pending events to flush for aggregate {StreamId}", aggregate.StreamId);
            return;
        }

        // Validate tenant ID - conditional based on RequireTenantId option
        string? tenantId = null;
        if (_options.RequireTenantId)
        {
            // Tenant is required
            var tenantIdFromMetadata = commandMetadata.TenantId 
                ?? throw new ArgumentException("TenantId is required in command metadata when RequireTenantId is true", nameof(commandMetadata));
            
            var tenantIdFromContext = _tenantContextProvider.GetTenantId();
            if (tenantIdFromContext != null && tenantIdFromContext != tenantIdFromMetadata)
            {
                throw new InvalidOperationException(
                    $"Tenant ID mismatch: metadata has '{tenantIdFromMetadata}' but context has '{tenantIdFromContext}'");
            }
            
            tenantId = tenantIdFromMetadata;
        }
        else
        {
            // Tenant is optional
            tenantId = commandMetadata.TenantId ?? _tenantContextProvider.GetTenantId();
        }

        // Auto-set StreamId if not already set (safety net)
        if (string.IsNullOrEmpty(aggregate.StreamId))
        {
            var aggregateId = ExtractAggregateIdFromState(aggregate) 
                            ?? ExtractAggregateIdFromFirstEvent(events, aggregate.GetType());
            
            if (aggregateId.HasValue)
            {
                var streamId = tenantId != null 
                    ? GetStreamId(aggregate.GetType(), aggregateId.Value, tenantId)
                    : $"{aggregate.GetType().Name}:{aggregateId.Value}";
                aggregate.SetStreamId(streamId);
                _logger?.LogDebug("Auto-set StreamId for aggregate {Type} with ID {Id}, StreamId: {StreamId}", 
                    aggregate.GetType().Name, aggregateId.Value, streamId);
            }
            else
            {
                throw new InvalidOperationException(
                    $"Cannot determine StreamId for aggregate {aggregate.GetType().Name}. " +
                    $"Either use CreateAsync<T>(id) to create the aggregate, manually set StreamId, " +
                    $"or ensure the aggregate state contains a property named '{aggregate.GetType().Name}Id' " +
                    $"or the first event contains the aggregate ID.");
            }
        }
        
        // Validate stream ID includes tenant prefix (only if tenant is required)
        if (_options.RequireTenantId && tenantId != null)
        {
            if (!aggregate.StreamId.StartsWith($"{tenantId}:"))
            {
                throw new InvalidOperationException(
                    $"Stream ID '{aggregate.StreamId}' does not match tenant ID '{tenantId}'. " +
                    $"Stream IDs must be in format '{{tenantId}}:{{aggregateType}}:{{aggregateId}}'");
            }
        }

        // Enrich events with metadata
        var enrichedEvents = new List<(IEvent Event, EventMetadata Metadata, IEnumerable<string> Tags)>();

        foreach (var @event in events)
        {
            // Enrich metadata
            var eventMetadata = _metadataProvider.EnrichEventMetadata(
                new EventMetadata(),
                commandMetadata,
                @event);

            // Get tags
            var tags = GetTagsForEvent(@event, aggregate);

            enrichedEvents.Add((@event, eventMetadata, tags));
        }

        // Determine expected version for optimistic concurrency without a store round-trip.
        // CommittedVersion is set by CreateAsync (-1 = "stream must not exist yet", matching the
        // existing store convention for AppendAsync(expectedVersion: -1)) and by GetAsync / a prior
        // SaveAsync (N ≥ 0 = "stream must be at version N").
        long? expectedVersion = aggregate.CommittedVersion;
        var allTags = enrichedEvents.SelectMany(e => e.Tags).Distinct().ToList();

        // Use first event's metadata (they should all be similar)
        var metadata = enrichedEvents.First().Metadata;

        var appendResult = await _eventStore.AppendAsync(
            aggregate.StreamId,
            enrichedEvents.Select(e => e.Event),
            expectedVersion,
            metadata,
            allTags,
            cancellationToken);

        // Clear pending events
        aggregate.ClearPendingEvents();

        // Sync the aggregate's in-memory version to the version the store actually assigned
        // to the last appended event. This is essential when backends differ in their
        // version numbering (SQL Server / in-memory are 1-based; others may be 0-based).
        // Subsequent commands on the same in-memory instance will then compute expectedVersion
        // correctly via: aggregate.Version - pendingEvents.Count.
        if (appendResult.LastStreamVersion >= 0)
        {
            aggregate.SetVersion(appendResult.LastStreamVersion);
            aggregate.SetCommittedVersion(appendResult.LastStreamVersion);
        }

        _logger?.LogDebug(
            "Saved {Count} events for aggregate {StreamId}, new version {Version}",
            events.Count,
            aggregate.StreamId,
            aggregate.Version);

        // ── Fire-and-forget snapshot (optional) ────────────────────────────────
        // Only attempt when a store and strategy resolver are registered, and the
        // strategy fires for this aggregate type. The write is entirely off the hot path.
        if (_snapshotStore != null && _strategyResolver != null)
        {
            var strategy = _strategyResolver.ResolveForAggregate(aggregate.GetType());
            var globalSequence = appendResult.SequencePositions.Count > 0
                ? appendResult.SequencePositions[^1]
                : 0L;

            // Build snapshot context — EventsSinceLastSnapshot is approximated as the
            // number of events just flushed (conservative: we don't load prior snapshot info here).
            var ctx = new SnapshotContext(
                StreamId:                 aggregate.StreamId,
                CurrentVersion:           aggregate.Version,
                EventsSinceLastSnapshot:  events.Count,
                LastSnapshotUtc:          null,  // conservative — no prior snapshot info at flush time
                GlobalSequence:           globalSequence);

            if (strategy.ShouldSnapshot(ctx))
            {
                var capturedStore     = _snapshotStore;
                var capturedAggregate = aggregate;
                var capturedSeq       = globalSequence;
                var capturedTypeName  = aggregate.GetType().Name;
                _ = Task.Run(async () =>
                {
                    var writeSw = Stopwatch.StartNew();
                    try
                    {
                        await capturedAggregate.TrySaveSnapshotAsync(capturedStore, capturedSeq, CancellationToken.None);
                        writeSw.Stop();
                        _logger?.LogDebug(
                            "Snapshot written for {StreamId} at version {Version}",
                            capturedAggregate.StreamId, capturedAggregate.Version);
                        SnapshotTelemetry.RecordWrite(capturedTypeName, "aggregate", writeSw.Elapsed);
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogWarning(ex,
                            "Fire-and-forget snapshot write failed for {StreamId}",
                            capturedAggregate.StreamId);
                    }
                });
            }
        }
    }

    /// <summary>
    /// Handles a command on an aggregate with optional authorization checking.
    /// This method performs authorization (if enabled), executes the command, and persists events.
    /// </summary>
    /// <typeparam name="T">Aggregate type.</typeparam>
    /// <typeparam name="TCommand">Command type.</typeparam>
    /// <param name="aggregate">Aggregate instance.</param>
    /// <param name="command">Command to execute.</param>
    /// <param name="commandMetadata">Command metadata.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The aggregate after command handling.</returns>
    /// <exception cref="UnauthorizedAccessException">Thrown if authorization fails.</exception>
    public async Task<T> HandleCommandAsync<T, TCommand>(
        T aggregate,
        TCommand command,
        CommandMetadata? commandMetadata = null,
        CancellationToken cancellationToken = default) 
        where T : AggregateRoot
        where TCommand : ICommand
    {
        if (aggregate == null)
            throw new ArgumentNullException(nameof(aggregate));
        if (command == null)
            throw new ArgumentNullException(nameof(command));
        
        // Capture metadata if not provided
        commandMetadata ??= _metadataProvider.CaptureCommandMetadata();
        
        // Authorization check (if enabled and service is registered)
        if (_options.EnableAuthorization && _authorizationService != null)
        {
            var authResult = await _authorizationService.AuthorizeCommandAsync(command, cancellationToken);
            if (!authResult.IsAuthorized)
            {
                _logger?.LogWarning(
                    "Authorization failed for command {CommandType}: {Reason}",
                    typeof(TCommand).Name,
                    authResult.FailureReason);
                    
                throw new UnauthorizedAccessException(
                    $"Authorization failed: {authResult.FailureReason}. " +
                    $"Failed checks: {string.Join(", ", authResult.FailedChecks)}");
            }
            
            // Capture authorization info in metadata
            commandMetadata.AuthorizedAt = DateTime.UtcNow;
            commandMetadata.AuthorizedBy = commandMetadata.UserId;
            
            _logger?.LogDebug(
                "Command {CommandType} authorized for user {UserId}",
                typeof(TCommand).Name,
                commandMetadata.UserId);
        }

        if (CompiledCommandApplicator.OverridesHandleAsync(aggregate.GetType()))
        {
#pragma warning disable CS0618
            await aggregate.HandleAsync(command, cancellationToken);
#pragma warning restore CS0618
        }
        else
            await CompiledCommandApplicator.DispatchAsync(aggregate, command, cancellationToken);
        
        // Persist events
        await SaveAsync(aggregate, commandMetadata, cancellationToken);
        
        return aggregate;
    }

    private string ResolveStreamId<T>(Guid id) where T : AggregateRoot
    {
        if (_options.RequireTenantId)
        {
            var tenantId = _tenantContextProvider.GetTenantIdRequired();
            return GetStreamId<T>(id, tenantId);
        }

        var optionalTenant = _tenantContextProvider.GetTenantId();
        return optionalTenant != null
            ? GetStreamId<T>(id, optionalTenant)
            : $"{typeof(T).Name}:{id}";
    }

    private static string GetStreamId<T>(Guid id, string tenantId) where T : AggregateRoot
    {
        var aggregateTypeName = typeof(T).Name;
        return $"{tenantId}:{aggregateTypeName}:{id}";
    }

    private static string GetStreamId(Type aggregateType, Guid id, string tenantId)
    {
        var aggregateTypeName = aggregateType.Name;
        return $"{tenantId}:{aggregateTypeName}:{id}";
    }

    private static Guid? ExtractAggregateIdFromState(AggregateRoot aggregate)
    {
        // Try to extract aggregate ID from state using reflection
        // Look for a property named "{AggregateType}Id" (e.g., "OrderId", "CartId", "ProductId")
        var aggregateTypeName = aggregate.GetType().Name;
        var expectedPropertyName = $"{aggregateTypeName}Id";
        
        // Get the State property from AggregateRoot<TState>
        var stateProperty = aggregate.GetType().GetProperty("State", 
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
        
        if (stateProperty != null)
        {
            var state = stateProperty.GetValue(aggregate);
            if (state != null)
            {
                var idProperty = state.GetType().GetProperty(expectedPropertyName,
                    System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
                
                if (idProperty != null)
                {
                    var idValue = idProperty.GetValue(state);
                    if (idValue is Guid guid)
                    {
                        return guid;
                    }
                }
            }
        }
        
        return null;
    }

    private static Guid? ExtractAggregateIdFromFirstEvent(IList<IEvent> events, Type aggregateType)
    {
        if (events.Count == 0)
            return null;

        var firstEvent = events[0];
        var eventType = firstEvent.GetType();
        
        // Look for common "Created" event patterns that contain the aggregate ID
        // Check for properties like "OrderId", "CartId", "ProductId", etc.
        var aggregateTypeName = aggregateType.Name;
        var expectedPropertyName = $"{aggregateTypeName}Id";
        
        var idProperty = eventType.GetProperty(expectedPropertyName,
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
        
        if (idProperty != null)
        {
            var idValue = idProperty.GetValue(firstEvent);
            if (idValue is Guid guid)
            {
                return guid;
            }
        }
        
        return null;
    }

    private IEnumerable<string> GetTagsForEvent(IEvent @event, AggregateRoot aggregate)
    {
        var tags = new List<string>();

        // Get tags from attribute
        var eventType = @event.GetType();
        var tagAttributes = eventType.GetCustomAttributes(typeof(TagAttribute), inherit: true)
            .Cast<TagAttribute>()
            .Select(a => a.Value);

        tags.AddRange(tagAttributes);

        // Get tags from tag provider
        if (_tagProvider != null)
        {
            var providerTags = _tagProvider.GetTags(@event, aggregate);
            tags.AddRange(providerTags);
        }

        // Default: add aggregate tag
        if (!tags.Any(t => t.StartsWith($"{aggregate.GetType().Name}:")))
        {
            var aggregateId = ExtractAggregateId(aggregate.StreamId);
            if (aggregateId != null)
            {
                tags.Add($"{aggregate.GetType().Name}:{aggregateId}");
            }
        }

        return tags.Distinct();
    }

    private static string? ExtractAggregateId(string streamId)
        => StreamIdParser.ExtractAggregateIdString(streamId);

    private static string? ExtractTenantId(string streamId)
        => StreamIdParser.ExtractTenantId(streamId);
}

