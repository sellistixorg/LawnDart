using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using LawnDart;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Integration.Delivery;

/// <summary>
/// Forwards to an inner <see cref="IEventStore"/> and counts events <em>yielded</em> by
/// <see cref="ReadByQueryStreamAsync"/> and portable <see cref="IEventStoreSubscriptions"/>.
/// </summary>
internal sealed class CountingEventStore : IEventStore, IEventStoreSubscriptions
{
    private readonly IEventStore _inner;
    private long _enumerated;

    public CountingEventStore(IEventStore inner) =>
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));

    public long EventsEnumerated => Interlocked.Read(ref _enumerated);

    public void ResetEnumerated() => Interlocked.Exchange(ref _enumerated, 0);

    public ISubscriptionHandle Subscribe(
        string subscriberId,
        long fromSequence,
        EventSubscriptionFilter? filter = null,
        CancellationToken cancellationToken = default)
    {
        if (_inner is not IEventStoreSubscriptions subs)
            throw new NotSupportedException("Inner store does not implement IEventStoreSubscriptions.");

        var handle = subs.Subscribe(subscriberId, fromSequence, filter, cancellationToken);
        return new CountingSubscriptionHandle(handle, () => Interlocked.Increment(ref _enumerated));
    }

    public Task<IReadOnlyList<SequencedEvent>> ReadStreamAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default) =>
        _inner.ReadStreamAsync(streamId, fromVersion, toVersion, toTimestamp, cancellationToken);

    public IAsyncEnumerable<SequencedEvent> ReadStreamEnumerableAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default) =>
        _inner.ReadStreamEnumerableAsync(streamId, fromVersion, toVersion, toTimestamp, cancellationToken);

    public Task<QueryResult> ReadByQueryAsync(
        Query query,
        long? fromSequencePosition = null,
        int? limit = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default) =>
        _inner.ReadByQueryAsync(query, fromSequencePosition, limit, toSequencePosition, toTimestamp, cancellationToken);

    public async IAsyncEnumerable<SequencedEvent> ReadByQueryStreamAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var se in _inner.ReadByQueryStreamAsync(
                           query, fromSequencePosition, toSequencePosition, toTimestamp, cancellationToken)
                       .ConfigureAwait(false))
        {
            Interlocked.Increment(ref _enumerated);
            yield return se;
        }
    }

    public Task<AppendResult> AppendAsync(
        string streamId,
        IEnumerable<IEvent> events,
        long? expectedVersion = null,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default) =>
        _inner.AppendAsync(streamId, events, expectedVersion, metadata, tags, cancellationToken);

    public Task<AppendResult> AppendAsync(
        IEnumerable<IEvent> events,
        AppendCondition condition,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default) =>
        _inner.AppendAsync(events, condition, metadata, tags, cancellationToken);

    public Task<long> GetCurrentSequenceAsync(CancellationToken cancellationToken = default) =>
        _inner.GetCurrentSequenceAsync(cancellationToken);

    public Task<long> GetMaxSequencePositionAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default) =>
        _inner.GetMaxSequencePositionAsync(query, fromSequencePosition, toSequencePosition, toTimestamp, cancellationToken);

    public Task<StreamMetadata?> GetStreamAsync(string streamId, CancellationToken cancellationToken = default) =>
        _inner.GetStreamAsync(streamId, cancellationToken);

    public Task<IReadOnlyList<StreamMetadata>> GetStreamsByAggregateTypeAsync(
        string aggregateType,
        CancellationToken cancellationToken = default) =>
        _inner.GetStreamsByAggregateTypeAsync(aggregateType, cancellationToken);

    public Task<IReadOnlyList<StreamMetadata>> GetStreamsByTagAsync(
        string tag,
        CancellationToken cancellationToken = default) =>
        _inner.GetStreamsByTagAsync(tag, cancellationToken);

    public Task<IReadOnlyList<string>> EnumerateStreamIdsAsync(
        string? prefix = null,
        CancellationToken cancellationToken = default) =>
        _inner.EnumerateStreamIdsAsync(prefix, cancellationToken);

    public Task<IReadOnlyList<StreamMetadata>> GetStreamsUpdatedAfterAsync(
        long afterSequencePosition,
        int? limit = null,
        CancellationToken cancellationToken = default) =>
        _inner.GetStreamsUpdatedAfterAsync(afterSequencePosition, limit, cancellationToken);

    public Task<long> GetStreamCountAsync(string? prefix = null, CancellationToken cancellationToken = default) =>
        _inner.GetStreamCountAsync(prefix, cancellationToken);
}

/// <summary>Counts durable view flush waves (<see cref="IViewStore.SaveViewsAsync"/>).</summary>
internal sealed class CountingViewStore : IViewStore
{
    private readonly IViewStore _inner;
    private long _flushWaves;
    private long _viewWrites;
    private readonly ConcurrentDictionary<string, long> _flushWavesByKey = new(StringComparer.Ordinal);

    public CountingViewStore(IViewStore inner) =>
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));

    public long FlushWaves => Interlocked.Read(ref _flushWaves);
    public long ViewWrites => Interlocked.Read(ref _viewWrites);

    public void ResetCounts()
    {
        Interlocked.Exchange(ref _flushWaves, 0);
        Interlocked.Exchange(ref _viewWrites, 0);
        _flushWavesByKey.Clear();
    }

    public long FlushWavesFor(string storageKey) =>
        _flushWavesByKey.TryGetValue(storageKey, out var n) ? n : 0;

    public Task SaveViewAsync(
        string projectionType,
        string instanceId,
        string viewData,
        long checkpoint,
        CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref _flushWaves);
        Interlocked.Increment(ref _viewWrites);
        _flushWavesByKey.AddOrUpdate(projectionType, 1, (_, n) => n + 1);
        return _inner.SaveViewAsync(projectionType, instanceId, viewData, checkpoint, cancellationToken);
    }

    public Task SaveViewsAsync(
        string projectionType,
        IReadOnlyList<(string InstanceId, string ViewData, long Checkpoint)> views,
        CancellationToken cancellationToken = default)
    {
        if (views.Count > 0)
        {
            Interlocked.Increment(ref _flushWaves);
            Interlocked.Add(ref _viewWrites, views.Count);
            _flushWavesByKey.AddOrUpdate(projectionType, 1, (_, n) => n + 1);
        }

        return _inner.SaveViewsAsync(projectionType, views, cancellationToken);
    }

    public Task<string?> GetViewAsync(
        string projectionType,
        string instanceId,
        CancellationToken cancellationToken = default) =>
        _inner.GetViewAsync(projectionType, instanceId, cancellationToken);

    public Task<IEnumerable<(string InstanceId, string ViewData)>> GetViewsByTypeAsync(
        string projectionType,
        CancellationToken cancellationToken = default) =>
        _inner.GetViewsByTypeAsync(projectionType, cancellationToken);

    public Task DeleteViewAsync(
        string projectionType,
        string instanceId,
        CancellationToken cancellationToken = default) =>
        _inner.DeleteViewAsync(projectionType, instanceId, cancellationToken);

    public Task<(string ViewData, long Checkpoint)?> GetViewWithCheckpointAsync(
        string projectionType,
        string instanceId,
        CancellationToken cancellationToken = default) =>
        _inner.GetViewWithCheckpointAsync(projectionType, instanceId, cancellationToken);

    public Task DeleteAllViewsAsync(
        string projectionType,
        CancellationToken cancellationToken = default) =>
        _inner.DeleteAllViewsAsync(projectionType, cancellationToken);
}

/// <summary>
/// Implements <see cref="IEventStoreSubscriptions"/> with a channel that never delivers,
/// so the runner must poll-recover. Reads and appends go to the inner store.
/// </summary>
internal sealed class IdleSubscribeEventStore : IEventStore, IEventStoreSubscriptions
{
    private readonly IEventStore _inner;

    public IdleSubscribeEventStore(IEventStore inner) =>
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));

    public ISubscriptionHandle Subscribe(
        string subscriberId,
        long fromSequence,
        EventSubscriptionFilter? filter = null,
        CancellationToken cancellationToken = default) =>
        new IdleSubscriptionHandle(subscriberId);

    public Task<IReadOnlyList<SequencedEvent>> ReadStreamAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default) =>
        _inner.ReadStreamAsync(streamId, fromVersion, toVersion, toTimestamp, cancellationToken);

    public IAsyncEnumerable<SequencedEvent> ReadStreamEnumerableAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default) =>
        _inner.ReadStreamEnumerableAsync(streamId, fromVersion, toVersion, toTimestamp, cancellationToken);

    public Task<QueryResult> ReadByQueryAsync(
        Query query,
        long? fromSequencePosition = null,
        int? limit = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default) =>
        _inner.ReadByQueryAsync(query, fromSequencePosition, limit, toSequencePosition, toTimestamp, cancellationToken);

    public IAsyncEnumerable<SequencedEvent> ReadByQueryStreamAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default) =>
        _inner.ReadByQueryStreamAsync(query, fromSequencePosition, toSequencePosition, toTimestamp, cancellationToken);

    public Task<AppendResult> AppendAsync(
        string streamId,
        IEnumerable<IEvent> events,
        long? expectedVersion = null,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default) =>
        _inner.AppendAsync(streamId, events, expectedVersion, metadata, tags, cancellationToken);

    public Task<AppendResult> AppendAsync(
        IEnumerable<IEvent> events,
        AppendCondition condition,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default) =>
        _inner.AppendAsync(events, condition, metadata, tags, cancellationToken);

    public Task<long> GetCurrentSequenceAsync(CancellationToken cancellationToken = default) =>
        _inner.GetCurrentSequenceAsync(cancellationToken);

    public Task<long> GetMaxSequencePositionAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default) =>
        _inner.GetMaxSequencePositionAsync(query, fromSequencePosition, toSequencePosition, toTimestamp, cancellationToken);

    public Task<StreamMetadata?> GetStreamAsync(string streamId, CancellationToken cancellationToken = default) =>
        _inner.GetStreamAsync(streamId, cancellationToken);

    public Task<IReadOnlyList<StreamMetadata>> GetStreamsByAggregateTypeAsync(
        string aggregateType,
        CancellationToken cancellationToken = default) =>
        _inner.GetStreamsByAggregateTypeAsync(aggregateType, cancellationToken);

    public Task<IReadOnlyList<StreamMetadata>> GetStreamsByTagAsync(
        string tag,
        CancellationToken cancellationToken = default) =>
        _inner.GetStreamsByTagAsync(tag, cancellationToken);

    public Task<IReadOnlyList<string>> EnumerateStreamIdsAsync(
        string? prefix = null,
        CancellationToken cancellationToken = default) =>
        _inner.EnumerateStreamIdsAsync(prefix, cancellationToken);

    public Task<IReadOnlyList<StreamMetadata>> GetStreamsUpdatedAfterAsync(
        long afterSequencePosition,
        int? limit = null,
        CancellationToken cancellationToken = default) =>
        _inner.GetStreamsUpdatedAfterAsync(afterSequencePosition, limit, cancellationToken);

    public Task<long> GetStreamCountAsync(string? prefix = null, CancellationToken cancellationToken = default) =>
        _inner.GetStreamCountAsync(prefix, cancellationToken);
}

internal sealed class IdleSubscriptionHandle : ISubscriptionHandle
{
    private readonly Channel<SequencedEvent> _channel = Channel.CreateBounded<SequencedEvent>(
        new BoundedChannelOptions(1) { FullMode = BoundedChannelFullMode.Wait });

    public IdleSubscriptionHandle(string subscriberId) => SubscriberId = subscriberId;

    public string SubscriberId { get; }
    public ChannelReader<SequencedEvent> Events => _channel.Reader;
    public long LastDeliveredSequence => 0;

    public void Dispose() => _channel.Writer.TryComplete();
    public ValueTask DisposeAsync()
    {
        _channel.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }
}

internal sealed class CountingSubscriptionHandle : ISubscriptionHandle
{
    private readonly ISubscriptionHandle _inner;
    private readonly CountingChannelReader _reader;

    public CountingSubscriptionHandle(ISubscriptionHandle inner, Action onEvent)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _reader = new CountingChannelReader(inner.Events, onEvent);
    }

    public string SubscriberId => _inner.SubscriberId;
    public ChannelReader<SequencedEvent> Events => _reader;
    public long LastDeliveredSequence => _inner.LastDeliveredSequence;

    public void Dispose() => _inner.Dispose();
    public ValueTask DisposeAsync() => _inner.DisposeAsync();
}

internal sealed class CountingChannelReader : ChannelReader<SequencedEvent>
{
    private readonly ChannelReader<SequencedEvent> _inner;
    private readonly Action _onEvent;

    public CountingChannelReader(ChannelReader<SequencedEvent> inner, Action onEvent)
    {
        _inner = inner;
        _onEvent = onEvent;
    }

    public override Task Completion => _inner.Completion;

    public override bool TryRead(out SequencedEvent item)
    {
        if (_inner.TryRead(out item!))
        {
            _onEvent();
            return true;
        }

        item = default!;
        return false;
    }

    public override ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default) =>
        _inner.WaitToReadAsync(cancellationToken);
}
