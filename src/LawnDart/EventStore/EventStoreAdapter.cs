using System.Runtime.CompilerServices;
using LawnDart.Metadata;

namespace LawnDart.EventStore;

/// <summary>
/// Typed <see cref="IEventStore"/> session over a schema-dumb <see cref="IEventLog"/>.
/// </summary>
/// <remarks>
/// Application code keeps using this adapter. Third-party stores implement
/// <see cref="IEventLog"/>. Serialize on append; hydrate on read. Stream registry
/// and <see cref="AppendResult.ConsistencyMarker"/> pass through from the log unchanged.
/// <para>
/// Subscriptions: the log pushes <see cref="RecordedEvent"/> frames;
/// this adapter hydrates. A fail-closed hydrate does not advance the typed cursor.
/// </para>
/// </remarks>
public sealed class EventStoreAdapter : IEventStore, IEventStoreSubscriptions
{
    /// <summary>
    /// Default per-handle bounded channel capacity when the caller does not
    /// specify one. Matches the shipped InMemory / SQL defaults.
    /// </summary>
    public const int DefaultSubscriptionChannelCapacity = 10_000;

    private readonly IEventLog _log;
    private readonly EventSession _session;
    private readonly IEventLogSubscriptions? _logSubscriptions;
    private readonly int _subscriptionChannelCapacity;

    /// <param name="log">Schema-dumb log. Also used as <see cref="IStreamRegistry"/>.</param>
    /// <param name="session">Typed serialize / hydrate session.</param>
    /// <param name="logSubscriptions">
    /// Log-level push surface. Defaults to <paramref name="log"/> when it implements
    /// <see cref="IEventLogSubscriptions"/>.
    /// </param>
    /// <param name="subscriptionChannelCapacity">Typed subscription channel capacity.</param>
    public EventStoreAdapter(
        IEventLog log,
        EventSession session,
        IEventLogSubscriptions? logSubscriptions = null,
        int subscriptionChannelCapacity = DefaultSubscriptionChannelCapacity)
    {
        _log = log ?? throw new ArgumentNullException(nameof(log));
        _session = session ?? throw new ArgumentNullException(nameof(session));
        _logSubscriptions = logSubscriptions ?? log as IEventLogSubscriptions;
        if (subscriptionChannelCapacity < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(subscriptionChannelCapacity),
                "SubscriptionChannelCapacity must be at least 1.");
        }

        _subscriptionChannelCapacity = subscriptionChannelCapacity;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<SequencedEvent>> ReadStreamAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
    {
        var frames = await _log.ReadStreamAsync(
            streamId, fromVersion, toVersion, toCommitTimestamp: null, cancellationToken)
            .ConfigureAwait(false);
        return Hydrate(frames, toTimestamp);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<SequencedEvent> ReadStreamEnumerableAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var frame in _log.ReadStreamEnumerableAsync(
            streamId, fromVersion, toVersion, toCommitTimestamp: null, cancellationToken)
            .ConfigureAwait(false))
        {
            var sequenced = _session.Hydrate(frame);
            if (toTimestamp.HasValue && sequenced.Metadata.Timestamp > toTimestamp.Value)
                continue;
            yield return sequenced;
        }
    }

    /// <inheritdoc />
    public async Task<QueryResult> ReadByQueryAsync(
        Query query,
        long? fromSequencePosition = null,
        int? limit = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
    {
        var logResult = await _log.ReadByQueryAsync(
            query, fromSequencePosition, limit, toSequencePosition, toCommitTimestamp: null, cancellationToken)
            .ConfigureAwait(false);
        return new QueryResult(Hydrate(logResult.Events, toTimestamp), logResult.ConsistencyMarker);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<SequencedEvent> ReadByQueryStreamAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var frame in _log.ReadByQueryStreamAsync(
            query, fromSequencePosition, toSequencePosition, toCommitTimestamp: null, cancellationToken)
            .ConfigureAwait(false))
        {
            var sequenced = _session.Hydrate(frame);
            if (toTimestamp.HasValue && sequenced.Metadata.Timestamp > toTimestamp.Value)
                continue;
            yield return sequenced;
        }
    }

    /// <inheritdoc />
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

        var tagsList = tags?.ToList() ?? [];
        var envelopes = eventsList.ConvertAll(e => _session.ToAppendEvent(e, metadata, tagsList));
        return _log.AppendAsync(streamId, envelopes, expectedVersion, cancellationToken);
    }

    /// <inheritdoc />
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
        ArgumentNullException.ThrowIfNull(condition);

        var tagsList = tags?.ToList() ?? [];
        var envelopes = eventsList.ConvertAll(e => _session.ToAppendEvent(e, metadata, tagsList));
        return _log.AppendAsync(envelopes, condition, cancellationToken);
    }

    /// <inheritdoc />
    public Task<long> GetCurrentSequenceAsync(CancellationToken cancellationToken = default)
        => _log.GetCurrentSequenceAsync(cancellationToken);

    /// <inheritdoc />
    public async Task<long> GetMaxSequencePositionAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(query);

        if (!toTimestamp.HasValue)
        {
            return await _log.GetMaxSequencePositionAsync(
                query, fromSequencePosition, toSequencePosition, toCommitTimestamp: null, cancellationToken)
                .ConfigureAwait(false);
        }

        var logResult = await _log.ReadByQueryAsync(
            query, fromSequencePosition, limit: null, toSequencePosition, toCommitTimestamp: null, cancellationToken)
            .ConfigureAwait(false);

        var max = 0L;
        foreach (var frame in logResult.Events)
        {
            var sequenced = _session.Hydrate(frame);
            if (sequenced.Metadata.Timestamp <= toTimestamp.Value && frame.SequencePosition > max)
                max = frame.SequencePosition;
        }

        return max;
    }

    /// <inheritdoc />
    public Task<StreamMetadata?> GetStreamAsync(string streamId, CancellationToken cancellationToken = default)
        => _log.GetStreamAsync(streamId, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<StreamMetadata>> GetStreamsByAggregateTypeAsync(
        string aggregateType,
        CancellationToken cancellationToken = default)
        => _log.GetStreamsByAggregateTypeAsync(aggregateType, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<StreamMetadata>> GetStreamsByTagAsync(
        string tag,
        CancellationToken cancellationToken = default)
        => _log.GetStreamsByTagAsync(tag, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<string>> EnumerateStreamIdsAsync(
        string? prefix = null,
        CancellationToken cancellationToken = default)
        => _log.EnumerateStreamIdsAsync(prefix, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<StreamMetadata>> GetStreamsUpdatedAfterAsync(
        long afterSequencePosition,
        int? limit = null,
        CancellationToken cancellationToken = default)
        => _log.GetStreamsUpdatedAfterAsync(afterSequencePosition, limit, cancellationToken);

    /// <inheritdoc />
    public Task<long> GetStreamCountAsync(string? prefix = null, CancellationToken cancellationToken = default)
        => _log.GetStreamCountAsync(prefix, cancellationToken);

    /// <inheritdoc />
    public ISubscriptionHandle Subscribe(
        string subscriberId,
        long fromSequence,
        EventSubscriptionFilter? filter = null,
        CancellationToken cancellationToken = default)
    {
        if (_logSubscriptions is null)
        {
            throw new InvalidOperationException(
                "This event log does not implement IEventLogSubscriptions. The typed session cannot subscribe.");
        }

        var logHandle = _logSubscriptions.Subscribe(subscriberId, fromSequence, filter, cancellationToken);
        return new HydratingSubscriptionHandle(
            _session,
            logHandle,
            _subscriptionChannelCapacity,
            cancellationToken);
    }

    private IReadOnlyList<SequencedEvent> Hydrate(
        IEnumerable<RecordedEvent> frames,
        DateTime? toBusinessTimestamp)
    {
        IEnumerable<SequencedEvent> typed = frames.Select(_session.Hydrate);
        if (toBusinessTimestamp.HasValue)
            typed = typed.Where(e => e.Metadata.Timestamp <= toBusinessTimestamp.Value);
        return typed.ToList().AsReadOnly();
    }
}
