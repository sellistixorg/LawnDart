using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
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
public class InMemoryEventStore : IEventStore, IEventStoreSubscriptions, IEventLog, IEventLogSubscriptions, ISignalsNewEvents
{
    private readonly ILogger<InMemoryEventStore>? _logger;
    private readonly EventSession _session;
    private readonly EventStoreAdapter _adapter;
    private readonly Dictionary<string, List<RecordedEvent>> _streams = new();
    private readonly List<RecordedEvent> _allEvents = new();
    private readonly HashSet<Guid> _eventIds = [];
    private readonly Dictionary<string, StreamMetadata> _streamRegistry = new();
    private readonly ConcurrentDictionary<InMemorySubscriptionHandle, byte> _subscriptions = new();
    private readonly int _subscriptionChannelCapacity;
    private readonly NewEventsSignal _newEvents = new();
    private readonly TimeSpan _subscriptionPollInterval;
    private long _nextSequencePosition = 1;
    private readonly object _lock = new();
    private readonly bool _enableRegistry;
    private readonly bool _requireTenantId;
    private readonly bool _validateNames;

    /// <summary>
    /// The bounded context name this store belongs to.
    /// Defaults to <c>"default"</c> when not part of a named bounded context.
    /// </summary>
    public string ContextName { get; }

    /// <summary>
    /// In-process wake-up for this store. Raised after a save commits.
    /// Call <see cref="NewEventsSignal.Notify"/> on this same object when events
    /// were committed outside this store.
    /// </summary>
    public NewEventsSignal NewEvents => _newEvents;

    /// <summary>
    /// Test seam. A committed save does not raise <see cref="NewEvents"/> when this is false.
    /// </summary>
    internal bool PublishNewEventsSignal { get; set; } = true;

    /// <summary>
    /// Test seam. Runs after an idle subscription read and before the wait.
    /// </summary>
    internal Action? BeforeIdleWait { get; set; }

    /// <param name="enableRegistry">Whether stream metadata registry tracking is enabled.</param>
    /// <param name="contextName">
    /// The bounded context name.  Defaults to <c>"default"</c>.
    /// When created via <c>AddBoundedContext().UseInMemory()</c> this is set automatically.
    /// </param>
    /// <param name="logger">Optional logger.</param>
    /// <param name="options">
    /// Optional store options: subscription capacity, <see cref="InMemoryEventStoreOptions.RequireTenantId"/>,
    /// and <see cref="InMemoryEventStoreOptions.ValidateNames"/>.
    /// When omitted, <see cref="InMemoryEventStoreOptions.RequireTenantId"/> is true.
    /// A stream id must include a tenant prefix. Set that option to false to append without one.
    /// </param>
    /// <param name="session">
    /// Typed session used to serialize on append and read records back into objects.
    /// Defaults to System.Text.Json UTF-8 and a per-store write-through catalog (not a
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
        options ??= new InMemoryEventStoreOptions();
        _requireTenantId = options.RequireTenantId;
        _validateNames = options.ValidateNames;
        var capacity = options.SubscriptionChannelCapacity;
        if (capacity < 1)
            throw new ArgumentOutOfRangeException(nameof(options), "SubscriptionChannelCapacity must be at least 1.");
        _subscriptionChannelCapacity = capacity;
        var poll = options.SubscriptionPollInterval;
        if (poll < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(options), "SubscriptionPollInterval cannot be negative.");
        _subscriptionPollInterval = poll;
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

        AppendTenantRules.ValidateStreamAppend(streamId, envelopes, _requireTenantId, _validateNames);

        lock (_lock)
        {
            var result = AppendFramesToStream(streamId, envelopes, expectedVersion);
            SignalCommitted(result);
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

        var tenantId = AppendTenantRules.TryReadTenantId(envelopes);
        AppendTenantRules.ValidateDcbAppend(tenantId, _requireTenantId, _validateNames);

        lock (_lock)
        {
            var result = AppendFramesDcb(envelopes, condition, tenantId);
            SignalCommitted(result);
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
                var ticket = _newEvents.TakeTicket();
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
                    BeforeIdleWait?.Invoke();
                    try
                    {
                        await _newEvents.WaitAsync(ticket, _subscriptionPollInterval, ct).ConfigureAwait(false);
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
            if (_logger is not null)
                InMemoryEventStoreLog.SubscriptionFailed(_logger, ex, handle.SubscriberId);
            handle.Fault(new EventSubscriptionFailedException(handle.SubscriberId, ex));
        }
        finally
        {
            await handle.DisposeAsync().ConfigureAwait(false);
        }
    }

    private void SignalCommitted(AppendResult result)
    {
        if (PublishNewEventsSignal)
            _newEvents.NotifyCommittedPositions(result.SequencePositions);
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
            _eventIds.Clear();
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
                // Prefix match is ordinal ignore-case. SQL Server prefix queries follow the StreamId column collation.
                .OrderBy(id => id, StringComparer.Ordinal)
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
    public Task<StreamPage> ListStreamsAsync(
        StreamListFilter? filter = null,
        string? pageToken = null,
        int pageSize = StreamPage.DefaultPageSize,
        CancellationToken cancellationToken = default)
    {
        if (!_enableRegistry)
            throw new InvalidOperationException("Stream registry is not enabled");

        StreamPageToken.CheckPageSize(pageSize);
        var after = StreamPageToken.ReadAfter(pageToken, filter);
        var prefix = StreamPageToken.Normalize(filter?.Prefix);
        var tag = StreamPageToken.Normalize(filter?.Tag);
        var aggregateType = StreamPageToken.Normalize(filter?.AggregateType);

        lock (_lock)
        {
            var rows = new List<StreamMetadata>();
            foreach (var metadata in _streamRegistry.Values)
            {
                if (metadata.Status != StreamStatus.Active)
                    continue;
                if (prefix is not null && !metadata.StreamId.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                    continue;
                if (aggregateType is not null && !string.Equals(metadata.AggregateType, aggregateType, StringComparison.Ordinal))
                    continue;
                if (tag is not null && !HasTag(metadata, tag))
                    continue;
                if (after is not null && string.CompareOrdinal(metadata.StreamId, after) <= 0)
                    continue;
                rows.Add(metadata);
            }

            rows.Sort(static (left, right) => string.CompareOrdinal(left.StreamId, right.StreamId));
            if (rows.Count > pageSize + 1)
                rows.RemoveRange(pageSize + 1, rows.Count - (pageSize + 1));
            return Task.FromResult(StreamPageToken.ToPage(rows, pageSize, filter));
        }
    }

    private static bool HasTag(StreamMetadata metadata, string tag)
    {
        foreach (var candidate in metadata.Tags)
        {
            if (string.Equals(candidate, tag, StringComparison.Ordinal))
                return true;
        }

        return false;
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
                // Ordinal ignore-case. SQL Server prefix queries follow the StreamId column collation.
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

        ReserveEventIds(envelopes);
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

        if (_logger is not null)
            InMemoryEventStoreLog.StreamEventsAppended(
                _logger,
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

        ReserveEventIds(envelopes);
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

        if (_logger is not null)
            InMemoryEventStoreLog.DcbEventsAppended(
                _logger,
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

    private void ReserveEventIds(IReadOnlyList<AppendEvent> envelopes)
    {
        var ids = EventIdResolver.ResolveBatch(envelopes.Select(e => e.Metadata).ToList());
        foreach (var id in ids)
        {
            if (_eventIds.Contains(id))
                throw new EventAlreadyAppendedException(id);
        }

        foreach (var id in ids)
            _eventIds.Add(id);
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
}
