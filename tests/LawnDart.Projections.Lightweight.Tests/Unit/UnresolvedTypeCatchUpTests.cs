using LawnDart;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

/// <summary>
/// A filtered window that cannot resolve its type must not checkpoint to the head.
/// </summary>
public class UnresolvedTypeCatchUpTests
{
    [Fact]
    public async Task FilteredCatchUp_DoesNotCheckpoint_WhenTypesAreUnresolved()
    {
        var store = new UnresolvedTypeEventStore();
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        await store.AppendAsync(
            "acme:ListingAggregate:listing-a",
            [new CatalogItemAdded(Guid.NewGuid(), DateTime.UtcNow, Guid.NewGuid(), Guid.NewGuid())],
            metadata: ProjectionRunnerTestHarness.MakeMeta(),
            cancellationToken: cts.Token);
        var head = await store.GetCurrentSequenceAsync(cts.Token);
        Assert.True(head >= 1);

        var runner = await ProjectionRunnerTestHarness.StartRunnerAsync(
            ProjectionScanner.TryBuildRegistration(typeof(CatalogListingProjection))!,
            store,
            views,
            checkpoints,
            ProjectionRunnerTestHarness.AggressivePollOptions,
            cts.Token);

        await Task.Delay(TimeSpan.FromMilliseconds(400), cts.Token);
        await runner.StopAsync(CancellationToken.None);

        var checkpoint = await checkpoints.GetCheckpointAsync("CatalogListing:v1", 0, CancellationToken.None);
        if (checkpoint is not null)
            Assert.True(checkpoint.LastSequencePosition < head);
    }

    private sealed class UnresolvedTypeEventStore : IEventStore
    {
        private readonly InMemoryEventStore _inner = new();

        public Task<QueryResult> ReadByQueryAsync(
            Query query,
            long? fromSequencePosition = null,
            int? limit = null,
            long? toSequencePosition = null,
            DateTime? toTimestamp = null,
            CancellationToken cancellationToken = default)
            => Task.FromResult(new QueryResult([]) { TypesResolved = false });

        public async IAsyncEnumerable<SequencedEvent> ReadByQueryStreamAsync(
            Query query,
            long? fromSequencePosition = null,
            long? toSequencePosition = null,
            DateTime? toTimestamp = null,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;
            yield break;
        }

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

        public Task<long> GetMaxSequencePositionAsync(
            Query query,
            long? fromSequencePosition = null,
            long? toSequencePosition = null,
            DateTime? toTimestamp = null,
            CancellationToken cancellationToken = default)
            => Task.FromResult(0L);

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
    }
}
