using System.Runtime.CompilerServices;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.Projections.Lightweight.Tests.Fakes;

/// <summary>
/// Wraps <see cref="InMemoryEventStore"/> to simulate filtered-query visibility races:
/// empty <see cref="IEventStore.ReadByQueryStreamAsync"/> results while
/// <see cref="IEventStore.GetCurrentSequenceAsync"/> already reflects appended events.
/// </summary>
public sealed class ControllableQueryEventStore : IEventStore
{
    private readonly InMemoryEventStore _inner = new();
    private int _emptyQueryReadsRemaining;

    /// <summary>
    /// When &gt; 0, the next N <see cref="ReadByQueryStreamAsync"/> calls yield no events
    /// (while <see cref="GetCurrentSequenceAsync"/> still delegates to the inner store).
    /// </summary>
    public int EmptyQueryReadsRemaining
    {
        get => _emptyQueryReadsRemaining;
        set => _emptyQueryReadsRemaining = value;
    }

    public InMemoryEventStore Inner => _inner;

    public Task<AppendResult> AppendAsync(
        string streamId,
        IEnumerable<IEvent> events,
        long? expectedVersion = null,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default)
        => _inner.AppendAsync(streamId, events, expectedVersion, metadata, tags, cancellationToken);

    public Task<AppendResult> AppendAsync(
        IEnumerable<IEvent> events,
        AppendCondition condition,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default)
        => _inner.AppendAsync(events, condition, metadata, tags, cancellationToken);

    public Task<long> GetCurrentSequenceAsync(CancellationToken cancellationToken = default)
        => _inner.GetCurrentSequenceAsync(cancellationToken);

    public async IAsyncEnumerable<SequencedEvent> ReadByQueryStreamAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (_emptyQueryReadsRemaining > 0)
        {
            _emptyQueryReadsRemaining--;
            yield break;
        }

        await foreach (var se in _inner.ReadByQueryStreamAsync(
                           query,
                           fromSequencePosition,
                           toSequencePosition,
                           toTimestamp,
                           cancellationToken)
                       .ConfigureAwait(false))
        {
            yield return se;
        }
    }

    public async Task<QueryResult> ReadByQueryAsync(
        Query query,
        long? fromSequencePosition = null,
        int? limit = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
    {
        if (_emptyQueryReadsRemaining > 0)
        {
            _emptyQueryReadsRemaining--;
            return new QueryResult(Array.Empty<SequencedEvent>());
        }

        return await _inner.ReadByQueryAsync(
            query,
            fromSequencePosition,
            limit,
            toSequencePosition,
            toTimestamp,
            cancellationToken).ConfigureAwait(false);
    }

    public Task<IReadOnlyList<SequencedEvent>> ReadStreamAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
        => _inner.ReadStreamAsync(streamId, fromVersion, toVersion, toTimestamp, cancellationToken);

    public IAsyncEnumerable<SequencedEvent> ReadStreamEnumerableAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
        => _inner.ReadStreamEnumerableAsync(streamId, fromVersion, toVersion, toTimestamp, cancellationToken);

    public Task<StreamMetadata?> GetStreamAsync(string streamId, CancellationToken cancellationToken = default)
        => _inner.GetStreamAsync(streamId, cancellationToken);

    public Task<IReadOnlyList<StreamMetadata>> GetStreamsByAggregateTypeAsync(
        string aggregateType,
        CancellationToken cancellationToken = default)
        => _inner.GetStreamsByAggregateTypeAsync(aggregateType, cancellationToken);

    public Task<IReadOnlyList<StreamMetadata>> GetStreamsByTagAsync(
        string tag,
        CancellationToken cancellationToken = default)
        => _inner.GetStreamsByTagAsync(tag, cancellationToken);

    public Task<IReadOnlyList<string>> EnumerateStreamIdsAsync(
        string? prefix = null,
        CancellationToken cancellationToken = default)
        => _inner.EnumerateStreamIdsAsync(prefix, cancellationToken);

    public Task<IReadOnlyList<StreamMetadata>> GetStreamsUpdatedAfterAsync(
        long afterSequencePosition,
        int? limit = null,
        CancellationToken cancellationToken = default)
        => _inner.GetStreamsUpdatedAfterAsync(afterSequencePosition, limit, cancellationToken);

    public Task<long> GetStreamCountAsync(string? prefix = null, CancellationToken cancellationToken = default)
        => _inner.GetStreamCountAsync(prefix, cancellationToken);

    public Task<long> GetMaxSequencePositionAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
        => _inner.GetMaxSequencePositionAsync(
            query,
            fromSequencePosition,
            toSequencePosition,
            toTimestamp,
            cancellationToken);
}
