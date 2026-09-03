using System.Runtime.CompilerServices;
using LawnDart.EventStore;
using LawnDart.Metadata;

namespace LawnDart.Projections.Lightweight.Tests.Fakes;

/// <summary>
/// In-memory event store with explicit sequence positions and gaps — simulates SQL Server
/// <c>CACHE 1000</c> sequence jumps where committed events exist after an empty range.
/// </summary>
public sealed class SequenceGapEventStore : IEventStore
{
    private readonly IReadOnlyList<SequencedEvent> _events;

    public SequenceGapEventStore(IEnumerable<SequencedEvent> events)
    {
        _events = events
            .OrderBy(e => e.SequencePosition)
            .ToList();
    }

    public Task<long> GetCurrentSequenceAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(_events.Count == 0 ? 0L : _events[^1].SequencePosition);
    }

    public async IAsyncEnumerable<SequencedEvent> ReadByQueryStreamAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var se in _events)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (fromSequencePosition.HasValue && se.SequencePosition < fromSequencePosition.Value)
                continue;
            if (toSequencePosition.HasValue && se.SequencePosition > toSequencePosition.Value)
                continue;
            if (toTimestamp.HasValue && se.Metadata.Timestamp > toTimestamp.Value)
                continue;
            if (!MatchesQuery(query, se))
                continue;

            yield return se;
        }

        await Task.CompletedTask;
    }

    public Task<QueryResult> ReadByQueryAsync(
        Query query,
        long? fromSequencePosition = null,
        int? limit = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
    {
        var list = new List<SequencedEvent>();
        foreach (var se in _events)
        {
            if (fromSequencePosition.HasValue && se.SequencePosition < fromSequencePosition.Value)
                continue;
            if (toSequencePosition.HasValue && se.SequencePosition > toSequencePosition.Value)
                continue;
            if (toTimestamp.HasValue && se.Metadata.Timestamp > toTimestamp.Value)
                continue;
            if (!MatchesQuery(query, se))
                continue;

            list.Add(se);
            if (limit.HasValue && list.Count >= limit.Value)
                break;
        }

        return Task.FromResult(new QueryResult(list.AsReadOnly()));
    }

    private static bool MatchesQuery(Query query, SequencedEvent se)
    {
        if (query.Items.Count == 0)
            return true;

        var eventTypeName = se.Event.GetType().FullName ?? se.Event.GetType().Name;
        var shortName = se.Event.GetType().Name;

        foreach (var item in query.Items)
        {
            var typeMatch = item.Types is null or { Count: 0 }
                || item.Types.Any(t =>
                    string.Equals(t, eventTypeName, StringComparison.Ordinal)
                    || string.Equals(t, shortName, StringComparison.Ordinal)
                    || eventTypeName.StartsWith(t + ",", StringComparison.Ordinal));

            var tagMatch = item.Tags is null or { Count: 0 }
                || item.Tags.Any(tag => se.Tags.Contains(tag));

            if (typeMatch && tagMatch)
                return true;
        }

        return false;
    }

    public Task<AppendResult> AppendAsync(
        string streamId,
        IEnumerable<IEvent> events,
        long? expectedVersion = null,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<AppendResult> AppendAsync(
        IEnumerable<IEvent> events,
        AppendCondition condition,
        EventMetadata? metadata = null,
        IEnumerable<string>? tags = null,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<IReadOnlyList<SequencedEvent>> ReadStreamAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public IAsyncEnumerable<SequencedEvent> ReadStreamEnumerableAsync(
        string streamId,
        long fromVersion = 0,
        long? toVersion = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<StreamMetadata?> GetStreamAsync(string streamId, CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<IReadOnlyList<StreamMetadata>> GetStreamsByAggregateTypeAsync(
        string aggregateType,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<IReadOnlyList<StreamMetadata>> GetStreamsByTagAsync(
        string tag,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<IReadOnlyList<string>> EnumerateStreamIdsAsync(
        string? prefix = null,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<IReadOnlyList<StreamMetadata>> GetStreamsUpdatedAfterAsync(
        long afterSequencePosition,
        int? limit = null,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<long> GetStreamCountAsync(string? prefix = null, CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<long> GetMaxSequencePositionAsync(
        Query query,
        long? fromSequencePosition = null,
        long? toSequencePosition = null,
        DateTime? toTimestamp = null,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var max = 0L;
        foreach (var se in _events)
        {
            if (fromSequencePosition.HasValue && se.SequencePosition < fromSequencePosition.Value)
                continue;
            if (toSequencePosition.HasValue && se.SequencePosition > toSequencePosition.Value)
                continue;
            if (toTimestamp.HasValue && se.Metadata.Timestamp > toTimestamp.Value)
                continue;
            if (!MatchesQuery(query, se))
                continue;
            if (se.SequencePosition > max)
                max = se.SequencePosition;
        }

        return Task.FromResult(max);
    }
}
