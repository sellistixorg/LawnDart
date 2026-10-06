using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.Demo.Shop.Infrastructure;

/// <summary>
/// Decorates the default <see cref="IEventStore"/> and records appended events on the
/// active <see cref="CorrelationScope"/> so the Event Stream page can show them.
/// </summary>
public sealed class LoggingEventStore : IEventStore, IEventLog, IEventStoreSubscriptions, IEventLogSubscriptions
{
    private readonly IEventStore _store;
    private readonly IEventLog _log;
    private readonly IEventStoreSubscriptions _subscriptions;
    private readonly IEventLogSubscriptions _logSubscriptions;
    private readonly CommandEventLog _commandLog;

    /// <summary>
    /// Wraps <paramref name="inner"/>. The inner store must also implement the log and subscription surfaces.
    /// </summary>
    public LoggingEventStore(IEventStore inner, CommandEventLog commandLog)
    {
        _store = inner;
        _log = (IEventLog)inner;
        _subscriptions = (IEventStoreSubscriptions)inner;
        _logSubscriptions = (IEventLogSubscriptions)inner;
        _commandLog = commandLog;
    }

    /// <summary>The store this decorator forwards to.</summary>
    public IEventStore Inner => _store;

    /// <inheritdoc />
    public async Task<AppendResult> AppendAsync(
        string streamId,
        IEnumerable<IEvent> events,
        long? expectedVersion = null,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default)
    {
        var list = events.ToList();
        var result = await _store.AppendAsync(streamId, list, expectedVersion, metadata, tags, cancellationToken)
            .ConfigureAwait(false);
        Record(streamId, list, result.SequencePositions, metadata);
        return result;
    }

    /// <inheritdoc />
    public async Task<AppendResult> AppendAsync(
        IEnumerable<IEvent> events,
        AppendCondition condition,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default)
    {
        var list = events.ToList();
        var result = await _store.AppendAsync(list, condition, metadata, tags, cancellationToken)
            .ConfigureAwait(false);
        Record("(dcb)", list, result.SequencePositions, metadata);
        return result;
    }

    private void Record(string streamId, IReadOnlyList<IEvent> events, IReadOnlyList<long> positions, EventMetadata? metadata)
    {
        var correlationId = CorrelationScope.Current?.CorrelationId;
        if (correlationId is null)
            return;

        for (var i = 0; i < events.Count; i++)
        {
            var position = i < positions.Count ? positions[i] : 0;
            _commandLog.RecordEvent(correlationId, events[i].GetType().Name, streamId, position, events[i], metadata);
        }
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<SequencedEvent>> ReadStreamAsync(
        string streamId, long fromVersion = 0, long? toVersion = null,
        DateTime? toTimestamp = null, CancellationToken cancellationToken = default)
        => _store.ReadStreamAsync(streamId, fromVersion, toVersion, toTimestamp, cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<SequencedEvent> ReadStreamEnumerableAsync(
        string streamId, long fromVersion = 0, long? toVersion = null,
        DateTime? toTimestamp = null, CancellationToken cancellationToken = default)
        => _store.ReadStreamEnumerableAsync(streamId, fromVersion, toVersion, toTimestamp, cancellationToken);

    /// <inheritdoc />
    public Task<QueryResult> ReadByQueryAsync(
        Query query, long? fromSequencePosition = null, int? limit = null,
        long? toSequencePosition = null, DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
        => _store.ReadByQueryAsync(query, fromSequencePosition, limit, toSequencePosition, toTimestamp, cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<SequencedEvent> ReadByQueryStreamAsync(
        Query query, long? fromSequencePosition = null, long? toSequencePosition = null,
        DateTime? toTimestamp = null, CancellationToken cancellationToken = default)
        => _store.ReadByQueryStreamAsync(query, fromSequencePosition, toSequencePosition, toTimestamp, cancellationToken);

    /// <inheritdoc />
    public Task<long> GetCurrentSequenceAsync(CancellationToken cancellationToken = default)
        => _store.GetCurrentSequenceAsync(cancellationToken);

    /// <inheritdoc />
    public Task<long> GetMaxSequencePositionAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
        => _store.GetMaxSequencePositionAsync(query, fromSequencePosition, toSequencePosition, toTimestamp, cancellationToken);

    /// <inheritdoc />
    public Task<StreamMetadata?> GetStreamAsync(string streamId, CancellationToken cancellationToken = default)
        => _store.GetStreamAsync(streamId, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<StreamMetadata>> GetStreamsByAggregateTypeAsync(
        string aggregateType, CancellationToken cancellationToken = default)
        => _store.GetStreamsByAggregateTypeAsync(aggregateType, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<StreamMetadata>> GetStreamsByTagAsync(
        string tag, CancellationToken cancellationToken = default)
        => _store.GetStreamsByTagAsync(tag, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<string>> EnumerateStreamIdsAsync(
        string? prefix = null, CancellationToken cancellationToken = default)
        => _store.EnumerateStreamIdsAsync(prefix, cancellationToken);

    /// <inheritdoc />
    public Task<IReadOnlyList<StreamMetadata>> GetStreamsUpdatedAfterAsync(
        long afterSequencePosition, int? limit = null, CancellationToken cancellationToken = default)
        => _store.GetStreamsUpdatedAfterAsync(afterSequencePosition, limit, cancellationToken);

    /// <inheritdoc />
    public Task<long> GetStreamCountAsync(string? prefix = null, CancellationToken cancellationToken = default)
        => _store.GetStreamCountAsync(prefix, cancellationToken);

    /// <inheritdoc />
    public ISubscriptionHandle Subscribe(
        string subscriberId,
        long fromSequence,
        EventSubscriptionFilter? filter = null,
        CancellationToken cancellationToken = default)
        => _subscriptions.Subscribe(subscriberId, fromSequence, filter, cancellationToken);

    Task<IReadOnlyList<RecordedEvent>> IEventLog.ReadStreamAsync(
        string streamId, long fromVersion, long? toVersion, DateTime? toCommitTimestamp, CancellationToken cancellationToken)
        => _log.ReadStreamAsync(streamId, fromVersion, toVersion, toCommitTimestamp, cancellationToken);

    IAsyncEnumerable<RecordedEvent> IEventLog.ReadStreamEnumerableAsync(
        string streamId, long fromVersion, long? toVersion, DateTime? toCommitTimestamp, CancellationToken cancellationToken)
        => _log.ReadStreamEnumerableAsync(streamId, fromVersion, toVersion, toCommitTimestamp, cancellationToken);

    Task<EventLogQueryResult> IEventLog.ReadByQueryAsync(
        Query query, long? fromSequencePosition, int? limit, long? toSequencePosition, DateTime? toCommitTimestamp, CancellationToken cancellationToken)
        => _log.ReadByQueryAsync(query, fromSequencePosition, limit, toSequencePosition, toCommitTimestamp, cancellationToken);

    IAsyncEnumerable<RecordedEvent> IEventLog.ReadByQueryStreamAsync(
        Query query, long? fromSequencePosition, long? toSequencePosition, DateTime? toCommitTimestamp, CancellationToken cancellationToken)
        => _log.ReadByQueryStreamAsync(query, fromSequencePosition, toSequencePosition, toCommitTimestamp, cancellationToken);

    Task<AppendResult> IEventLog.AppendAsync(
        string streamId, IEnumerable<AppendEvent> events, long? expectedVersion, CancellationToken cancellationToken)
        => _log.AppendAsync(streamId, events, expectedVersion, cancellationToken);

    Task<AppendResult> IEventLog.AppendAsync(
        IEnumerable<AppendEvent> events, AppendCondition condition, CancellationToken cancellationToken)
        => _log.AppendAsync(events, condition, cancellationToken);

    Task<long> IEventLog.GetCurrentSequenceAsync(CancellationToken cancellationToken)
        => _log.GetCurrentSequenceAsync(cancellationToken);

    Task<long> IEventLog.GetMaxSequencePositionAsync(
        Query query, long? fromSequencePosition, long? toSequencePosition, DateTime? toCommitTimestamp, CancellationToken cancellationToken)
        => _log.GetMaxSequencePositionAsync(query, fromSequencePosition, toSequencePosition, toCommitTimestamp, cancellationToken);

    IEventLogSubscriptionHandle IEventLogSubscriptions.Subscribe(
        string subscriberId, long fromSequence, EventSubscriptionFilter? filter, CancellationToken cancellationToken)
        => _logSubscriptions.Subscribe(subscriberId, fromSequence, filter, cancellationToken);
}
