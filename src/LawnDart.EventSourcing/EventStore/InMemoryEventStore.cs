using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using LawnDart.EventSourcing.Serialization;
using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.EventSourcing.EventStore;

/// <summary>
/// In-memory <see cref="IEventLog"/>. Typed <see cref="IEventStore"/> methods
/// forward to <see cref="EventStoreAdapter"/>.
/// </summary>
public class InMemoryEventStore : IEventStore, IEventStoreSubscriptions, IEventLog, IEventLogSubscriptions
{
    private readonly ILogger<InMemoryEventStore>? _logger;
    private readonly EventSession _session;
    private readonly EventStoreAdapter _adapter;
    private readonly Dictionary<string, List<RecordedEvent>> _streams = new();
    private readonly List<RecordedEvent> _allEvents = new();
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
    /// <param name="session">
    /// Typed session used to serialize on append and hydrate on read.
    /// Defaults to STJ UTF-8 and a per-store write-through catalog (not a
    /// process-wide resolver). Hosts must pass a materialized catalog via
    /// <c>WithEventTypes</c>.
    /// </param>
    public InMemoryEventStore(
        bool enableRegistry = true,
        string contextName  = "default",
        ILogger<InMemoryEventStore>? logger = null,
        InMemoryEventStoreOptions? options = null,
        EventSession? session = null)
    {
        _enableRegistry = enableRegistry;
        ContextName     = string.IsNullOrWhiteSpace(contextName) ? "default" : contextName;
        _logger         = logger;
        _session        = session ?? new EventSession(
            new JsonEventSerializer(),
            new WriteThroughEventTypeCatalog());
        var capacity = options?.SubscriptionChannelCapacity
            ?? InMemoryEventStoreOptions.DefaultSubscriptionChannelCapacity;
        if (capacity < 1)
            throw new ArgumentOutOfRangeException(nameof(options), "SubscriptionChannelCapacity must be at least 1.");
        _subscriptionChannelCapacity = capacity;
        _adapter = new EventStoreAdapter(this, _session, this, _subscriptionChannelCapacity);
    }

    public Task<IReadOnlyList<SequencedEvent>> ReadStreamAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
        => _adapter.ReadStreamAsync(streamId, fromVersion, toVersion, toTimestamp, cancellationToken);

    public IAsyncEnumerable<SequencedEvent> ReadStreamEnumerableAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
        => _adapter.ReadStreamEnumerableAsync(streamId, fromVersion, toVersion, toTimestamp, cancellationToken);

    public IAsyncEnumerable<SequencedEvent> ReadByQueryStreamAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
        => _adapter.ReadByQueryStreamAsync(query, fromSequencePosition, toSequencePosition, toTimestamp, cancellationToken);

    public Task<QueryResult> ReadByQueryAsync(
        Query query,
        long? fromSequencePosition = null,
        int? limit = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
        => _adapter.ReadByQueryAsync(query, fromSequencePosition, limit, toSequencePosition, toTimestamp, cancellationToken);

    public Task<AppendResult> AppendAsync(
        string streamId,
        IEnumerable<IEvent> events,
        long? expectedVersion = null,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default)
        => _adapter.AppendAsync(streamId, events, expectedVersion, metadata, tags, cancellationToken);

    public Task<AppendResult> AppendAsync(
        IEnumerable<IEvent> events,
        AppendCondition condition,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default)
        => _adapter.AppendAsync(events, condition, metadata, tags, cancellationToken);

    public Task<AppendResult> AppendAsync(
        string streamId,
        IEnumerable<AppendEvent> events,
        long? expectedVersion = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(streamId))
            throw new ArgumentException("Stream ID cannot be null or empty", nameof(streamId));

        var envelopes = events?.ToList() ?? throw new ArgumentNullException(nameof(events));
        if (envelopes.Count == 0)
            throw new ArgumentException("At least one event is required", nameof(events));

        lock (_lock)
        {
            var result = AppendFramesToStream(streamId, envelopes, expectedVersion);
            SignalSubscriptions();
            return Task.FromResult(result);
        }
    }

    public Task<AppendResult> AppendAsync(
        IEnumerable<AppendEvent> events,
        AppendCondition condition,
        CancellationToken cancellationToken = default)
    {
        var envelopes = events?.ToList() ?? throw new ArgumentNullException(nameof(events));
        if (envelopes.Count == 0)
            throw new ArgumentException("At least one event is required", nameof(events));

        ArgumentNullException.ThrowIfNull(condition);

        lock (_lock)
        {
            var result = AppendFramesDcb(envelopes, condition, TryReadTenantId(envelopes));
            SignalSubscriptions();
            return Task.FromResult(result);
        }
    }

    async IAsyncEnumerable<RecordedEvent> IEventLog.ReadStreamEnumerableAsync(
        string streamId,
        long fromVersion,
        long? toVersion,
        DateTime? toCommitTimestamp,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(streamId))
            throw new ArgumentException("Stream ID cannot be null or empty", nameof(streamId));

        foreach (var frame in SnapshotStream(streamId, fromVersion, toVersion, toCommitTimestamp))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return frame;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    Task<IReadOnlyList<RecordedEvent>> IEventLog.ReadStreamAsync(
        string streamId,
        long fromVersion,
        long? toVersion,
        DateTime? toCommitTimestamp,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(streamId))
            throw new ArgumentException("Stream ID cannot be null or empty", nameof(streamId));

        return Task.FromResult<IReadOnlyList<RecordedEvent>>(
            SnapshotStream(streamId, fromVersion, toVersion, toCommitTimestamp));
    }

    Task<EventLogQueryResult> IEventLog.ReadByQueryAsync(
        Query query,
        long? fromSequencePosition,
        int? limit,
        long? toSequencePosition,
        DateTime? toCommitTimestamp,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        var frames = SnapshotQuery(query, fromSequencePosition, toSequencePosition, toCommitTimestamp, limit);
        return Task.FromResult(new EventLogQueryResult(frames));
    }

    async IAsyncEnumerable<RecordedEvent> IEventLog.ReadByQueryStreamAsync(
        Query query,
        long? fromSequencePosition,
        long? toSequencePosition,
        DateTime? toCommitTimestamp,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        foreach (var frame in SnapshotQuery(query, fromSequencePosition, toSequencePosition, toCommitTimestamp, limit: null))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return frame;
        }

        await Task.CompletedTask.ConfigureAwait(false);
    }

    Task<long> IEventLog.GetMaxSequencePositionAsync(
        Query query,
        long? fromSequencePosition,
        long? toSequencePosition,
        DateTime? toCommitTimestamp,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(query);
        var frames = SnapshotQuery(query, fromSequencePosition, toSequencePosition, toCommitTimestamp, limit: null);
        return Task.FromResult(MaxSequence(frames));
    }

    /// <inheritdoc />
    public ISubscriptionHandle Subscribe(
        string subscriberId,
        long fromSequence,
        EventSubscriptionFilter? filter = null,
        CancellationToken cancellationToken = default)
        => _adapter.Subscribe(subscriberId, fromSequence, filter, cancellationToken);

    /// <inheritdoc />
    IEventLogSubscriptionHandle IEventLogSubscriptions.Subscribe(
        string subscriberId,
        long fromSequence,
        EventSubscriptionFilter? filter,
        CancellationToken cancellationToken)
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
                List<RecordedEvent> batch;
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

                foreach (var frame in batch)
                {
                    if (ct.IsCancellationRequested || handle.IsDisposed)
                        break;

                    await handle.WriteAsync(frame, ct).ConfigureAwait(false);
                    lastDelivered = frame.SequencePosition;
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
        => _adapter.GetMaxSequencePositionAsync(
            query, fromSequencePosition, toSequencePosition, toTimestamp, cancellationToken);

    private List<RecordedEvent> SnapshotStream(
        string streamId,
        long fromVersion,
        long? toVersion,
        DateTime? toCommitTimestamp)
    {
        lock (_lock)
        {
            if (!_streams.TryGetValue(streamId, out var events))
                return [];

            return events
                .Where(e => e.StreamVersion >= fromVersion
                    && (!toVersion.HasValue || e.StreamVersion <= toVersion.Value)
                    && MatchesCommitTime(e, toCommitTimestamp))
                .OrderBy(e => e.StreamVersion)
                .ToList();
        }
    }

    private List<RecordedEvent> SnapshotQuery(
        Query query,
        long? fromSequencePosition,
        long? toSequencePosition,
        DateTime? toCommitTimestamp,
        int? limit)
    {
        lock (_lock)
        {
            IEnumerable<RecordedEvent> events = _allEvents;

            if (fromSequencePosition.HasValue)
                events = events.Where(e => e.SequencePosition >= fromSequencePosition.Value);
            if (toSequencePosition.HasValue)
                events = events.Where(e => e.SequencePosition <= toSequencePosition.Value);
            events = events.Where(e => MatchesCommitTime(e, toCommitTimestamp));
            if (query.Items.Count > 0)
                events = events.Where(e => EventQueryMatcher.Matches(e, query));

            events = events.OrderBy(e => e.SequencePosition);
            if (limit.HasValue)
                events = events.Take(limit.Value);

            return events.ToList();
        }
    }

    private AppendResult AppendFramesToStream(
        string streamId,
        IReadOnlyList<AppendEvent> envelopes,
        long? expectedVersion)
    {
        if (expectedVersion.HasValue)
        {
            var currentVersion = _streams.TryGetValue(streamId, out var existingEvents)
                ? existingEvents.Count > 0 ? existingEvents.Max(e => e.StreamVersion) : -1
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
            ? streamEvents.Count > 0 ? streamEvents.Max(e => e.StreamVersion) : 0
            : 0;

        if (!_streams.ContainsKey(streamId))
            _streams[streamId] = [];

        foreach (var envelope in envelopes)
        {
            var recorded = Record(envelope, streamId, ++streamVersion);
            _streams[streamId].Add(recorded);
            _allEvents.Add(recorded);
            sequencePositions.Add(recorded.SequencePosition);
        }

        if (_enableRegistry)
        {
            UpdateStreamRegistry(streamId, streamVersion, sequencePositions.Last(), envelopes.Count, UnionTags(envelopes));
        }

        _logger?.LogDebug(
            "Appended {Count} events to stream {StreamId}, versions {FromVersion}-{ToVersion}",
            envelopes.Count,
            streamId,
            streamVersion - envelopes.Count + 1,
            streamVersion);

        return new AppendResult(sequencePositions.AsReadOnly(), null, streamVersion);
    }

    private AppendResult AppendFramesDcb(
        IReadOnlyList<AppendEvent> envelopes,
        AppendCondition condition,
        string? tenantId)
    {
        var matchingEvents = _allEvents.AsEnumerable();

        if (condition.After.HasValue)
            matchingEvents = matchingEvents.Where(e => e.SequencePosition > condition.After.Value);

        matchingEvents = matchingEvents.Where(e => EventQueryMatcher.Matches(e, condition.FailIfEventsMatch));

        if (matchingEvents.Any())
        {
            throw new ConcurrencyException(
                $"Append condition failed: found {matchingEvents.Count()} matching events",
                condition.After);
        }

        var streamId = BuildDcbStreamId(tenantId);
        var sequencePositions = new List<long>();

        foreach (var envelope in envelopes)
        {
            var recorded = Record(envelope, streamId, streamVersion: 0);
            _allEvents.Add(recorded);
            sequencePositions.Add(recorded.SequencePosition);
        }

        if (_enableRegistry)
        {
            UpdateStreamRegistry(streamId, 0, sequencePositions.Last(), envelopes.Count, UnionTags(envelopes));
        }

        _logger?.LogDebug(
            "Appended {Count} events with DCB condition, sequence positions {FromPosition}-{ToPosition}",
            envelopes.Count,
            sequencePositions.FirstOrDefault(),
            sequencePositions.LastOrDefault());

        return new AppendResult(sequencePositions.AsReadOnly());
    }

    private RecordedEvent Record(AppendEvent envelope, string streamId, long streamVersion)
    {
        var sequencePosition = _nextSequencePosition++;
        return new RecordedEvent(
            envelope.EventType,
            envelope.Payload,
            streamId,
            streamVersion,
            sequencePosition,
            DateTime.UtcNow,
            envelope.Metadata,
            envelope.SchemaVersion,
            envelope.CodecId,
            envelope.Tags);
    }

    private static IReadOnlyList<string> UnionTags(IReadOnlyList<AppendEvent> envelopes)
        => envelopes.SelectMany(e => e.Tags).Distinct(StringComparer.Ordinal).ToList();

    private static bool MatchesCommitTime(RecordedEvent recorded, DateTime? toCommitTimestamp)
        => !toCommitTimestamp.HasValue || recorded.CommitTimestamp <= toCommitTimestamp.Value;

    private static long MaxSequence(IEnumerable<RecordedEvent> frames)
    {
        var max = 0L;
        foreach (var frame in frames)
        {
            if (frame.SequencePosition > max)
                max = frame.SequencePosition;
        }

        return max;
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
                existing.CurrentVersion = currentVersion;
                existing.LastSequencePosition = lastSequencePosition;
                existing.LastEventAt = now;
                existing.EventCount += eventCountDelta;
                existing.Tags = existing.Tags.Union(tags).Distinct().ToList();
            }
            else
            {
                var extractedTenant = ExtractTenantId(streamId);
                _streamRegistry[streamId] = new StreamMetadata
                {
                    StreamId = streamId,
                    TenantId = extractedTenant,
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

    private static string? TryReadTenantId(IReadOnlyList<AppendEvent> envelopes)
    {
        foreach (var envelope in envelopes)
        {
            if (envelope.Metadata.IsEmpty)
                continue;

            try
            {
                using var doc = JsonDocument.Parse(envelope.Metadata);
                if (doc.RootElement.TryGetProperty("TenantId", out var property)
                    && property.ValueKind == JsonValueKind.String)
                {
                    var value = property.GetString();
                    if (!string.IsNullOrWhiteSpace(value))
                        return value;
                }
            }
            catch (JsonException)
            {
                // Metadata is opaque JSON to the log; skip unreadable blobs.
            }
        }

        return null;
    }
}
