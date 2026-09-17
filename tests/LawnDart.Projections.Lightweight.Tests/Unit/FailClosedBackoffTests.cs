using LawnDart;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Lightweight.Tests.Helpers;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

/// <summary>
/// A typed fail-closed read must back off instead of hot-looping the stuck sequence.
/// </summary>
public sealed class FailClosedBackoffTests
{
    [Fact]
    public async Task PollLoop_EventHydrationException_DoesNotHotLoopStuckSequence()
    {
        var store = new FailClosedStore();
        var views = new InMemoryViewStore();
        var checkpoints = new InMemoryCheckpointStore(views);
        var options = new LightweightProjectionOptions
        {
            UseSubscribe = false,
            PollInterval = TimeSpan.FromMilliseconds(10),
            CheckpointInterval = 1,
            BatchSize = 10,
            SkipTailFlushWhileCatchingUp = false
        };

        var reg = ProjectionScanner.TryBuildRegistration(typeof(GlobalTagIndexProjection))!;
        var runner = new LightweightProjectionRunnerService(
            reg, store, views, checkpoints, new SingleNodePartitioningService(), options);
        await runner.StartAsync(CancellationToken.None);

        await Task.Delay(TimeSpan.FromSeconds(3));

        await runner.StopAsync(CancellationToken.None);

        Assert.True(
            store.Reads <= 2,
            $"Stuck sequence hot-looped: {store.Reads} reads in 3 s (backoff is {LightweightProjectionRunnerService.FailClosedBackoff}).");
        Assert.True(store.Reads >= 1, "Runner never attempted a typed read.");
    }

    private sealed class FailClosedStore : IEventStore
    {
        public int Reads;

        public Task<long> GetCurrentSequenceAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(1L);

        public IAsyncEnumerable<SequencedEvent> ReadByQueryStreamAsync(
            Query query,
            long? fromSequencePosition = null,
            long? toSequencePosition = null,
            DateTime? toTimestamp = null,
            CancellationToken cancellationToken = default)
            => ReadAndThrow();

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

        public Task<QueryResult> ReadByQueryAsync(
            Query query,
            long? fromSequencePosition = null,
            int? limit = null,
            long? toSequencePosition = null,
            DateTime? toTimestamp = null,
            CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

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

        public Task<long> GetMaxSequencePositionAsync(
            Query query,
            long? fromSequencePosition = null,
            long? toSequencePosition = null,
            DateTime? toTimestamp = null,
            CancellationToken cancellationToken = default)
            => Task.FromResult(0L);

        public Task<StreamMetadata?> GetStreamAsync(
            string streamId,
            CancellationToken cancellationToken = default)
            => Task.FromResult<StreamMetadata?>(null);

        public Task<IReadOnlyList<StreamMetadata>> GetStreamsByAggregateTypeAsync(
            string aggregateType,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<StreamMetadata>>(Array.Empty<StreamMetadata>());

        public Task<IReadOnlyList<StreamMetadata>> GetStreamsByTagAsync(
            string tag,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<StreamMetadata>>(Array.Empty<StreamMetadata>());

        public Task<IReadOnlyList<string>> EnumerateStreamIdsAsync(
            string? prefix = null,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());

        public Task<IReadOnlyList<StreamMetadata>> GetStreamsUpdatedAfterAsync(
            long afterSequencePosition,
            int? limit = null,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IReadOnlyList<StreamMetadata>>(Array.Empty<StreamMetadata>());

        public Task<long> GetStreamCountAsync(
            string? prefix = null,
            CancellationToken cancellationToken = default)
            => Task.FromResult(0L);

        private async IAsyncEnumerable<SequencedEvent> ReadAndThrow()
        {
            Interlocked.Increment(ref Reads);
            await Task.Yield();
            if (Reads > 0)
                throw new EventSchemaTooNewException("author-registered", 99, 1);
            yield break;
        }
    }
}
