using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using LawnDart.EventSourcing.Telemetry;
using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.EventSourcing.EventStore;

/// <summary>
/// In-memory event store implementation for testing and development.
/// Implements portable <see cref="IEventStoreSubscriptions"/> (catch-up → seamless live).
/// </summary>
public class InMemoryEventStore : IEventStore, IEventStoreSubscriptions
{
    private readonly ILogger<InMemoryEventStore>? _logger;
    private readonly Dictionary<string, List<SequencedEvent>> _streams = new();
    private readonly List<SequencedEvent> _allEvents = new();
    private readonly Dictionary<string, StreamMetadata> _streamRegistry = new();
    private readonly ConcurrentDictionary<InMemorySubscriptionHandle, byte> _subscriptions = new();
    private readonly int _subscriptionChannelCapacity;
    private long _nextSequencePosition = 1;
    private readonly object _lock = new();
    private readonly bool _enableRegistry;

    /// <summary>
    /// The bounded context name this store belongs to.
    /// Defaults to <c>"default"</c> when not part of a named bounded context.
    /// </summary>
    public string ContextName { get; }

    /// <param name="enableRegistry">Whether stream metadata registry tracking is enabled.</param>
    /// <param name="contextName">
    /// The bounded context name.  Defaults to <c>"default"</c>.
    /// When created via <c>AddBoundedContext().UseInMemory()</c> this is set automatically.
    /// </param>
    /// <param name="logger">Optional logger.</param>
    /// <param name="options">Optional in-memory store options (subscription channel capacity).</param>
    public InMemoryEventStore(
        bool enableRegistry = true,
        string contextName  = "default",
        ILogger<InMemoryEventStore>? logger = null,
        InMemoryEventStoreOptions? options = null)
    {
        _enableRegistry = enableRegistry;
        ContextName     = string.IsNullOrWhiteSpace(contextName) ? "default" : contextName;
        _logger         = logger;
        var capacity = options?.SubscriptionChannelCapacity
            ?? InMemoryEventStoreOptions.DefaultSubscriptionChannelCapacity;
        if (capacity < 1)
            throw new ArgumentOutOfRangeException(nameof(options), "SubscriptionChannelCapacity must be at least 1.");
        _subscriptionChannelCapacity = capacity;
    }

    public Task<IReadOnlyList<SequencedEvent>> ReadStreamAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(streamId))
            throw new ArgumentException("Stream ID cannot be null or empty", nameof(streamId));

        using var activity = EventStoreTelemetry.StartReadActivity("Stream", streamId);
        var stopwatch = Stopwatch.StartNew();

        try
        {
            lock (_lock)
            {
                if (!_streams.TryGetValue(streamId, out var events))
                {
                    var emptyResult = Array.Empty<SequencedEvent>();
                    EventStoreTelemetry.RecordRead("Stream", 0, stopwatch.Elapsed, streamId);
                    return Task.FromResult<IReadOnlyList<SequencedEvent>>(emptyResult);
                }

                var result = events
                    .Where(e => e.Version >= fromVersion
                        && (!toVersion.HasValue || e.Version <= toVersion.Value)
                        && (!toTimestamp.HasValue || e.Metadata.Timestamp <= toTimestamp.Value))
                    .OrderBy(e => e.Version)
                    .ToList()
                    .AsReadOnly();

                EventStoreTelemetry.RecordRead("Stream", result.Count, stopwatch.Elapsed, streamId);
                return Task.FromResult<IReadOnlyList<SequencedEvent>>(result);
            }
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            throw;
        }
    }

    public async IAsyncEnumerable<SequencedEvent> ReadStreamEnumerableAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(streamId))
            throw new ArgumentException("Stream ID cannot be null or empty", nameof(streamId));

        using var activity = EventStoreTelemetry.StartReadActivity("Stream", streamId);

        List<SequencedEvent> events;
        lock (_lock)
        {
            if (!_streams.TryGetValue(streamId, out var streamEvents))
            {
                yield break;
            }
            events = streamEvents
                .Where(e => e.Version >= fromVersion
                    && (!toVersion.HasValue || e.Version <= toVersion.Value)
                    && (!toTimestamp.HasValue || e.Metadata.Timestamp <= toTimestamp.Value))
                .OrderBy(e => e.Version)
                .ToList();
        }

        foreach (var evt in events)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return evt;
        }
    }

    public async IAsyncEnumerable<SequencedEvent> ReadByQueryStreamAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (query == null)
            throw new ArgumentNullException(nameof(query));

        List<SequencedEvent> snapshot;
        lock (_lock)
        {
            var events = _allEvents.AsEnumerable();

            if (fromSequencePosition.HasValue)
                events = events.Where(e => e.SequencePosition >= fromSequencePosition.Value);
            if (toSequencePosition.HasValue)
                events = events.Where(e => e.SequencePosition <= toSequencePosition.Value);
            if (toTimestamp.HasValue)
                events = events.Where(e => e.Metadata.Timestamp <= toTimestamp.Value);

            if (query.Items.Count > 0)
                events = events.Where(e => MatchesQuery(e, query));

            snapshot = events.OrderBy(e => e.SequencePosition).ToList();
        }

        foreach (var evt in snapshot)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return evt;
        }
    }

    public Task<QueryResult> ReadByQueryAsync(
        Query query,
        long? fromSequencePosition = null,
        int? limit = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
    {
        if (query == null)
            throw new ArgumentNullException(nameof(query));

        lock (_lock)
        {
            var events = _allEvents.AsEnumerable();

            if (fromSequencePosition.HasValue)
                events = events.Where(e => e.SequencePosition >= fromSequencePosition.Value);
            if (toSequencePosition.HasValue)
                events = events.Where(e => e.SequencePosition <= toSequencePosition.Value);
            if (toTimestamp.HasValue)
                events = events.Where(e => e.Metadata.Timestamp <= toTimestamp.Value);

            if (query.Items.Count > 0)
                events = events.Where(e => MatchesQuery(e, query));

            events = events.OrderBy(e => e.SequencePosition);

            if (limit.HasValue)
                events = events.Take(limit.Value);

            var result = events.ToList().AsReadOnly();
            return Task.FromResult(new QueryResult(result));
        }
    }

    public Task<AppendResult> AppendAsync(
        string streamId,
        IEnumerable<IEvent> events,
        long? expectedVersion = null,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(streamId))
            throw new ArgumentException("Stream ID cannot be null or empty", nameof(streamId));

        var eventsList = events?.ToList() ?? throw new ArgumentNullException(nameof(events));
        if (eventsList.Count == 0)
            throw new ArgumentException("At least one event is required", nameof(events));

        var tagsList = tags?.ToList() ?? new List<string>();

        using var activity = EventStoreTelemetry.StartAppendActivity(streamId, eventsList.Count);
        var stopwatch = Stopwatch.StartNew();
        var success = false;

        try
        {
            lock (_lock)
        {
            // Check expected version for optimistic concurrency
            if (expectedVersion.HasValue)
            {
                var currentVersion = _streams.TryGetValue(streamId, out var existingEvents)
                    ? existingEvents.Count > 0 ? existingEvents.Max(e => e.Version) : -1
                    : -1;

                if (currentVersion != expectedVersion.Value)
                {
                    throw new ConcurrencyException(
                        $"Expected version {expectedVersion.Value} but current version is {currentVersion}",
                        expectedVersion.Value,
                        currentVersion);
                }
            }

            var sequencePositions = new List<long>();
            var streamVersion = _streams.TryGetValue(streamId, out var streamEvents)
                ? streamEvents.Count > 0 ? streamEvents.Max(e => e.Version) : 0
                : 0;

            if (!_streams.ContainsKey(streamId))
            {
                _streams[streamId] = new List<SequencedEvent>();
            }

            foreach (var @event in eventsList)
            {
                var version = ++streamVersion;
                var sequencePosition = _nextSequencePosition++;

                // Create or clone metadata to set CommitTimestamp
                var eventMetadata = metadata ?? new EventMetadata { EventId = @event.Id.ToString(), Timestamp = @event.Timestamp };
                // Set CommitTimestamp to actual commit time (now)
                eventMetadata.CommitTimestamp = DateTime.UtcNow;

                var sequencedEvent = new SequencedEvent(
                    @event,
                    sequencePosition,
                    streamId,
                    version,
                    eventMetadata,
                    tagsList);

                _streams[streamId].Add(sequencedEvent);
                _allEvents.Add(sequencedEvent);
                sequencePositions.Add(sequencePosition);
            }

            // Update stream registry
            if (_enableRegistry)
            {
                UpdateStreamRegistry(streamId, streamVersion, sequencePositions.Last(), eventsList.Count, tagsList);
            }

            _logger?.LogDebug(
                "Appended {Count} events to stream {StreamId}, versions {FromVersion}-{ToVersion}",
                eventsList.Count,
                streamId,
                streamVersion - eventsList.Count + 1,
                streamVersion);

            success = true;
            EventStoreTelemetry.RecordAppend(streamId, eventsList.Count, stopwatch.Elapsed, success);
            // ConsistencyMarker is null for the in-memory backend; only hash-based backends populate it.
            var result = new AppendResult(sequencePositions.AsReadOnly(), null, streamVersion);
            SignalSubscriptions();
            return Task.FromResult(result);
            }
        }
        catch (Exception ex)
        {
            activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            EventStoreTelemetry.RecordAppend(streamId, eventsList.Count, stopwatch.Elapsed, false);
            throw;
        }
    }

    public Task<AppendResult> AppendAsync(
        IEnumerable<IEvent> events,
        AppendCondition condition,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default)
    {
        var eventsList = events?.ToList() ?? throw new ArgumentNullException(nameof(events));
        if (eventsList.Count == 0)
            throw new ArgumentException("At least one event is required", nameof(events));

        if (condition == null)
            throw new ArgumentNullException(nameof(condition));

        var tagsList = tags?.ToList() ?? new List<string>();

        lock (_lock)
        {
            // Check append condition
            var matchingEvents = _allEvents.AsEnumerable();

            if (condition.After.HasValue)
            {
                matchingEvents = matchingEvents.Where(e => e.SequencePosition > condition.After.Value);
            }

            matchingEvents = matchingEvents.Where(e => MatchesQuery(e, condition.FailIfEventsMatch));

            if (matchingEvents.Any())
            {
                throw new ConcurrencyException(
                    $"Append condition failed: found {matchingEvents.Count()} matching events",
                    condition.After);
            }

            // DCB appends use opaque internal stream IDs for parity with the SQL backend.
            var streamId = BuildDcbStreamId(metadata?.TenantId);

            var sequencePositions = new List<long>();

            foreach (var @event in eventsList)
            {
                var sequencePosition = _nextSequencePosition++;

                // Create or clone metadata to set CommitTimestamp
                var eventMetadata = metadata ?? new EventMetadata { EventId = @event.Id.ToString(), Timestamp = @event.Timestamp };
                // Set CommitTimestamp to actual commit time (now)
                eventMetadata.CommitTimestamp = DateTime.UtcNow;

                var sequencedEvent = new SequencedEvent(
                    @event,
                    sequencePosition,
                    streamId,
                    0, // Version not meaningful for DCB approach
                    eventMetadata,
                    tagsList);

                _allEvents.Add(sequencedEvent);
                sequencePositions.Add(sequencePosition);
            }

            // Update stream registry for DCB streams
            if (_enableRegistry)
            {
                UpdateStreamRegistry(streamId, 0, sequencePositions.Last(), eventsList.Count, tagsList);
            }

            _logger?.LogDebug(
                "Appended {Count} events with DCB condition, sequence positions {FromPosition}-{ToPosition}",
                eventsList.Count,
                sequencePositions.FirstOrDefault(),
                sequencePositions.LastOrDefault());

            // ConsistencyMarker is null for the in-memory backend; only hash-based backends populate it.
            var result = new AppendResult(sequencePositions.AsReadOnly());
            SignalSubscriptions();
            return Task.FromResult(result);
        }
    }

    private static bool MatchesQuery(SequencedEvent sequencedEvent, Query query)
        => EventQueryMatcher.Matches(sequencedEvent, query);

    /// <inheritdoc />
    public ISubscriptionHandle Subscribe(
        string subscriberId,
        long fromSequence,
        EventSubscriptionFilter? filter = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(subscriberId))
            throw new ArgumentException("Subscriber id is required.", nameof(subscriberId));
        if (fromSequence < 0)
            throw new ArgumentOutOfRangeException(nameof(fromSequence), "fromSequence cannot be negative.");

        filter ??= EventSubscriptionFilter.All();

        var handle = new InMemorySubscriptionHandle(
            subscriberId,
            filter,
            _subscriptionChannelCapacity,
            fromSequence,
            cancellationToken,
            h => _subscriptions.TryRemove(h, out _));

        if (!_subscriptions.TryAdd(handle, 0))
            throw new InvalidOperationException("Failed to register subscription handle.");

        _ = Task.Run(
            () => RunSubscriptionAsync(handle),
            CancellationToken.None);

        return handle;
    }

    private async Task RunSubscriptionAsync(InMemorySubscriptionHandle handle)
    {
        var ct = handle.CancellationToken;
        long lastDelivered = handle.FromSequence - 1;

        try
        {
            while (!ct.IsCancellationRequested && !handle.IsDisposed)
            {
                List<SequencedEvent> batch;
                lock (_lock)
                {
                    batch = _allEvents
                        .Where(e => e.SequencePosition > lastDelivered
                            && e.SequencePosition >= handle.FromSequence
                            && handle.Filter.Matches(e))
                        .OrderBy(e => e.SequencePosition)
                        .ToList();
                }

                if (batch.Count == 0)
                {
                    try
                    {
                        await handle.Wake.Reader.WaitToReadAsync(ct).ConfigureAwait(false);
                        while (handle.Wake.Reader.TryRead(out _)) { }
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                        break;
                    }

                    continue;
                }

                foreach (var evt in batch)
                {
                    if (ct.IsCancellationRequested || handle.IsDisposed)
                        break;

                    await handle.WriteAsync(evt, ct).ConfigureAwait(false);
                    lastDelivered = evt.SequencePosition;
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Normal cancel / dispose path.
        }
        catch (ChannelClosedException)
        {
            // Handle disposed while writing.
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "InMemory subscription {SubscriberId} failed.", handle.SubscriberId);
        }
        finally
        {
            await handle.DisposeAsync().ConfigureAwait(false);
        }
    }

    private void SignalSubscriptions()
    {
        foreach (var handle in _subscriptions.Keys)
            handle.Signal();
    }

    private static string BuildDcbStreamId(string? tenantId)
    {
        if (!string.IsNullOrWhiteSpace(tenantId))
        {
            return $"{tenantId}:dcb:{Guid.NewGuid()}";
        }

        return $"dcb:{Guid.NewGuid()}";
    }

    /// <summary>
    /// Clears all events (for testing). Active subscriptions are disposed.
    /// </summary>
    public void Clear()
    {
        foreach (var handle in _subscriptions.Keys.ToArray())
            handle.Dispose();

        lock (_lock)
        {
            _streams.Clear();
            _allEvents.Clear();
            _streamRegistry.Clear();
            _nextSequencePosition = 1;
        }
    }

    // Stream Registry Implementation

    public Task<StreamMetadata?> GetStreamAsync(
        string streamId,
        CancellationToken cancellationToken = default)
    {
        if (!_enableRegistry)
            throw new InvalidOperationException("Stream registry is not enabled");

        if (string.IsNullOrWhiteSpace(streamId))
            throw new ArgumentException("Stream ID cannot be null or empty", nameof(streamId));

        lock (_lock)
        {
            return Task.FromResult(_streamRegistry.TryGetValue(streamId, out var metadata) ? metadata : null);
        }
    }

    public Task<IReadOnlyList<StreamMetadata>> GetStreamsByAggregateTypeAsync(
        string aggregateType,
        CancellationToken cancellationToken = default)
    {
        if (!_enableRegistry)
            throw new InvalidOperationException("Stream registry is not enabled");

        if (string.IsNullOrWhiteSpace(aggregateType))
            throw new ArgumentException("Aggregate type cannot be null or empty", nameof(aggregateType));

        lock (_lock)
        {
            var streams = _streamRegistry.Values
                .Where(s => s.AggregateType == aggregateType && s.Status == StreamStatus.Active)
                .OrderByDescending(s => s.LastEventAt)
                .ToList();
            return Task.FromResult<IReadOnlyList<StreamMetadata>>(streams.AsReadOnly());
        }
    }

    public Task<IReadOnlyList<StreamMetadata>> GetStreamsByTagAsync(
        string tag,
        CancellationToken cancellationToken = default)
    {
        if (!_enableRegistry)
            throw new InvalidOperationException("Stream registry is not enabled");

        if (string.IsNullOrWhiteSpace(tag))
            throw new ArgumentException("Tag cannot be null or empty", nameof(tag));

        lock (_lock)
        {
            var streams = _streamRegistry.Values
                .Where(s => s.Tags.Contains(tag) && s.Status == StreamStatus.Active)
                .OrderByDescending(s => s.LastEventAt)
                .ToList();
            return Task.FromResult<IReadOnlyList<StreamMetadata>>(streams.AsReadOnly());
        }
    }

    public Task<IReadOnlyList<string>> EnumerateStreamIdsAsync(
        string? prefix = null,
        CancellationToken cancellationToken = default)
    {
        if (!_enableRegistry)
            throw new InvalidOperationException("Stream registry is not enabled");

        lock (_lock)
        {
            var streamIds = _streamRegistry.Values
                .Where(s => s.Status == StreamStatus.Active)
                .Select(s => s.StreamId)
                .Where(id => prefix == null || id.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                .OrderBy(id => id)
                .ToList();
            return Task.FromResult<IReadOnlyList<string>>(streamIds.AsReadOnly());
        }
    }

    public Task<IReadOnlyList<StreamMetadata>> GetStreamsUpdatedAfterAsync(
        long afterSequencePosition,
        int? limit = null,
        CancellationToken cancellationToken = default)
    {
        if (!_enableRegistry)
            throw new InvalidOperationException("Stream registry is not enabled");

        lock (_lock)
        {
            var streams = _streamRegistry.Values
                .Where(s => s.LastSequencePosition > afterSequencePosition && s.Status == StreamStatus.Active)
                .OrderBy(s => s.LastSequencePosition)
                .ToList();

            if (limit.HasValue)
            {
                streams = streams.Take(limit.Value).ToList();
            }

            return Task.FromResult<IReadOnlyList<StreamMetadata>>(streams.AsReadOnly());
        }
    }

    /// <inheritdoc />
    public Task<long> GetStreamCountAsync(string? prefix = null, CancellationToken cancellationToken = default)
    {
        if (!_enableRegistry)
            throw new InvalidOperationException("Stream registry is not enabled");

        lock (_lock)
        {
            var count = _streamRegistry.Values
                .Where(s => s.Status == StreamStatus.Active &&
                            (prefix == null || s.StreamId.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)))
                .LongCount();
            return Task.FromResult(count);
        }
    }

    /// <inheritdoc />
    public Task<long> GetCurrentSequenceAsync(CancellationToken cancellationToken = default)
    {
        lock (_lock)
        {
            return Task.FromResult(_nextSequencePosition - 1);
        }
    }

    /// <inheritdoc />
    public Task<long> GetMaxSequencePositionAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
    {
        if (query == null)
            throw new ArgumentNullException(nameof(query));

        lock (_lock)
        {
            var events = _allEvents.AsEnumerable();

            if (fromSequencePosition.HasValue)
                events = events.Where(e => e.SequencePosition >= fromSequencePosition.Value);
            if (toSequencePosition.HasValue)
                events = events.Where(e => e.SequencePosition <= toSequencePosition.Value);
            if (toTimestamp.HasValue)
                events = events.Where(e => e.Metadata.Timestamp <= toTimestamp.Value);
            if (query.Items.Count > 0)
                events = events.Where(e => MatchesQuery(e, query));

            var max = 0L;
            foreach (var evt in events)
            {
                if (evt.SequencePosition > max)
                    max = evt.SequencePosition;
            }

            return Task.FromResult(max);
        }
    }

    private void UpdateStreamRegistry(
        string streamId,
        long currentVersion,
        long lastSequencePosition,
        int eventCountDelta,
        IReadOnlyList<string> tags)
    {
        if (!_enableRegistry)
            return;

        lock (_lock)
        {
            var now = DateTime.UtcNow;
            var aggregateType = ExtractAggregateType(streamId);
            var aggregateId = ExtractAggregateId(streamId);

            if (_streamRegistry.TryGetValue(streamId, out var existing))
            {
                // Update existing
                existing.CurrentVersion = currentVersion;
                existing.LastSequencePosition = lastSequencePosition;
                existing.LastEventAt = now;
                existing.EventCount += eventCountDelta;
                existing.Tags = existing.Tags.Union(tags).Distinct().ToList();
            }
            else
            {
                // Create new
                var tenantId = ExtractTenantId(streamId);
                _streamRegistry[streamId] = new StreamMetadata
                {
                    StreamId = streamId,
                    TenantId = tenantId,
                    AggregateType = aggregateType,
                    AggregateId = aggregateId,
                    CurrentVersion = currentVersion,
                    LastSequencePosition = lastSequencePosition,
                    CreatedAt = now,
                    LastEventAt = now,
                    EventCount = eventCountDelta,
                    Tags = tags.ToList(),
                    Status = StreamStatus.Active
                };
            }
        }
    }

    private static string ExtractAggregateType(string streamId)
        => StreamIdParser.ExtractAggregateType(streamId);

    private static Guid? ExtractAggregateId(string streamId)
        => StreamIdParser.ExtractAggregateId(streamId);

    private static string? ExtractTenantId(string streamId)
        => StreamIdParser.ExtractTenantId(streamId);
}

