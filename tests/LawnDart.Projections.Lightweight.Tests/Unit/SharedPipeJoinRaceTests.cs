using LawnDart;
using LawnDart.EventSourcing.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using LawnDart.Projections.Checkpoints;
using LawnDart.Projections.Lightweight;
using LawnDart.Projections.Lightweight.Hosting;
using LawnDart.Projections.Lightweight.Registration;
using LawnDart.Projections.Partitioning;
using LawnDart.Projections.Sdk;
using LawnDart.Projections.Storage;

namespace LawnDart.Projections.Lightweight.Tests.Unit;

/// <summary>
/// Appends while a runner is still in private catch-up, after the live pipe has
/// already fanned those events out. Each event must be applied once. A join that
/// attaches from a horizon taken before that fan-out skips them.
/// </summary>
public class SharedPipeJoinRaceTests
{
    private const int Repeats = 25;
    private const int SeedCount = 10;
    private const int LiveCount = 10;

    [Fact]
    public async Task CatchUpJoin_AppendsDuringAttach_DeliversEachEventOnce()
    {
        for (var iteration = 1; iteration <= Repeats; iteration++)
        {
            JoinRaceLeaderProjection.Seen.Clear();
            JoinRaceLaggerProjection.Seen.Clear();

            var store = new GatedEventStore();
            var views = new InMemoryViewStore();
            var checkpoints = new InMemoryCheckpointStore(views);
            var options = new LightweightProjectionOptions
            {
                UseSubscribe = true,
                UseSharedSubscribe = true,
                CatchUpJoinThreshold = 0,
                BatchSize = 100,
                PollInterval = TimeSpan.FromMilliseconds(10),
                CheckpointInterval = 100,
                SkipTailFlushWhileCatchingUp = true
            };
            var regs = new[]
            {
                ProjectionScanner.TryBuildRegistration(typeof(JoinRaceLeaderProjection))!,
                ProjectionScanner.TryBuildRegistration(typeof(JoinRaceLaggerProjection))!
            };
            var manager = new BoundedContextProjectionRunnerManager(
                regs,
                reg => new LightweightProjectionRunnerService(
                    reg, store, views, checkpoints, new SingleNodePartitioningService(), options),
                logger: null,
                store,
                options);
            var leaderKey = regs[0].StorageKey;
            var laggerKey = regs[1].StorageKey;
            var ids = new List<Guid>();

            try
            {
                await manager.StartAsync(leaderKey);
                ids.AddRange(await AppendAsync(store, SeedCount));
                Assert.True(
                    await WaitUntilAsync(() => JoinRaceLeaderProjection.Count >= SeedCount, TimeSpan.FromSeconds(5)),
                    $"Iteration {iteration}: leader did not catch the seed.");

                store.Arm(SeedCount);
                await manager.StartAsync(laggerKey);
                Assert.True(
                    await WaitUntilAsync(() => store.IsBlocked, TimeSpan.FromSeconds(5)),
                    $"Iteration {iteration}: lagger never read sequence {SeedCount}.");

                ids.AddRange(await AppendAsync(store, LiveCount));
                Assert.True(
                    await WaitUntilAsync(() => JoinRaceLeaderProjection.Count >= ids.Count, TimeSpan.FromSeconds(5)),
                    $"Iteration {iteration}: live pipe did not fan out the events appended during catch-up.");

                store.Release();

                Assert.True(
                    await WaitUntilAsync(
                        () => (manager.GetSnapshot(laggerKey)?.LastAppliedSequence ?? -1) >= ids.Count
                              && JoinRaceLaggerProjection.Count >= ids.Count,
                        TimeSpan.FromSeconds(5)),
                    $"Iteration {iteration}: lagger stopped at {manager.GetSnapshot(laggerKey)?.LastAppliedSequence}, saw {JoinRaceLaggerProjection.Count}.");

                Assert.Equal(ids, JoinRaceLaggerProjection.Snapshot());
            }
            finally
            {
                store.Release();
                await manager.StopAllAsync();
            }
        }
    }

    private static async Task<List<Guid>> AppendAsync(GatedEventStore store, int count)
    {
        var ids = new List<Guid>(count);
        var meta = new EventMetadata { Timestamp = DateTime.UtcNow, UserId = "join-race" };
        for (var i = 0; i < count; i++)
        {
            var id = Guid.NewGuid();
            ids.Add(id);
            await store.AppendAsync("join", [new JoinRaceEvent(id, meta.Timestamp)], metadata: meta);
        }

        return ids;
    }

    private static async Task<bool> WaitUntilAsync(Func<bool> ready, TimeSpan timeout)
    {
        var started = DateTime.UtcNow;
        while (DateTime.UtcNow - started < timeout)
        {
            if (ready())
                return true;
            await Task.Delay(10);
        }

        return ready();
    }

    [EventTypeName("projection-tests.join-race")]
    private sealed record JoinRaceEvent(Guid Id, DateTime Timestamp) : IEvent;

    [GlobalProjection("JoinRaceLeader")]
    private sealed class JoinRaceLeaderProjection : ProjectionBase<JoinRaceView>
    {
        public static readonly List<Guid> Seen = [];

        public static int Count
        {
            get { lock (Seen) return Seen.Count; }
        }

        public void Handle(JoinRaceEvent e)
        {
            lock (Seen)
                Seen.Add(e.Id);
            State.Count++;
        }
    }

    [GlobalProjection("JoinRaceLagger")]
    private sealed class JoinRaceLaggerProjection : ProjectionBase<JoinRaceView>
    {
        public static readonly List<Guid> Seen = [];

        public static int Count
        {
            get { lock (Seen) return Seen.Count; }
        }

        public static Guid[] Snapshot()
        {
            lock (Seen)
                return Seen.ToArray();
        }

        public void Handle(JoinRaceEvent e)
        {
            lock (Seen)
                Seen.Add(e.Id);
            State.Count++;
        }
    }

    private sealed class JoinRaceView
    {
        public int Count { get; set; }
    }

    /// <summary>
    /// Forwards to <see cref="InMemoryEventStore"/> and holds the catch-up read that
    /// contains <see cref="Arm"/>'s sequence until <see cref="Release"/>.
    /// </summary>
    private sealed class GatedEventStore : IEventStore, IEventStoreSubscriptions
    {
        private readonly InMemoryEventStore _inner = new(options: new InMemoryEventStoreOptions { RequireTenantId = false });
        private readonly object _gate = new();
        private long _sequence;
        private TaskCompletionSource? _entered;
        private TaskCompletionSource? _release;
        private int _armed;

        public bool IsBlocked
        {
            get
            {
                lock (_gate)
                    return _entered?.Task.IsCompleted == true;
            }
        }

        public void Arm(long sequence)
        {
            lock (_gate)
            {
                _sequence = sequence;
                _entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _armed = 1;
            }
        }

        public void Release()
        {
            TaskCompletionSource? release;
            lock (_gate)
                release = _release;
            release?.TrySetResult();
        }

        public async Task<QueryResult> ReadByQueryAsync(
            Query query,
            long? fromSequencePosition = null,
            int? limit = null,
            long? toSequencePosition = null,
            DateTime? toTimestamp = null,
            CancellationToken cancellationToken = default)
        {
            var result = await _inner.ReadByQueryAsync(
                query, fromSequencePosition, limit, toSequencePosition, toTimestamp, cancellationToken)
                .ConfigureAwait(false);

            TaskCompletionSource? entered;
            TaskCompletionSource? release;
            lock (_gate)
            {
                var startsAfterSeed = fromSequencePosition is long from && from > _sequence;
                if (_armed != 1
                    || startsAfterSeed
                    || !result.Events.Any(e => e.SequencePosition == _sequence))
                    return result;

                _armed = 2;
                entered = _entered;
                release = _release;
            }

            entered?.TrySetResult();
            if (release is not null)
                await release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return result;
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
            => _inner.GetMaxSequencePositionAsync(query, fromSequencePosition, toSequencePosition, toTimestamp, cancellationToken);

        public Task<IReadOnlyList<SequencedEvent>> ReadStreamAsync(
            string streamId, long fromVersion = 0, long? toVersion = null, DateTime? toTimestamp = null, CancellationToken cancellationToken = default)
            => _inner.ReadStreamAsync(streamId, fromVersion, toVersion, toTimestamp, cancellationToken);

        public IAsyncEnumerable<SequencedEvent> ReadStreamEnumerableAsync(
            string streamId, long fromVersion = 0, long? toVersion = null, DateTime? toTimestamp = null, CancellationToken cancellationToken = default)
            => _inner.ReadStreamEnumerableAsync(streamId, fromVersion, toVersion, toTimestamp, cancellationToken);

        public IAsyncEnumerable<SequencedEvent> ReadByQueryStreamAsync(
            Query query, long? fromSequencePosition = null, long? toSequencePosition = null, DateTime? toTimestamp = null, CancellationToken cancellationToken = default)
            => _inner.ReadByQueryStreamAsync(query, fromSequencePosition, toSequencePosition, toTimestamp, cancellationToken);

        public ISubscriptionHandle Subscribe(
            string subscriberId, long fromSequence, EventSubscriptionFilter? filter = null, CancellationToken cancellationToken = default)
            => _inner.Subscribe(subscriberId, fromSequence, filter, cancellationToken);

        public Task<StreamMetadata?> GetStreamAsync(string streamId, CancellationToken cancellationToken = default)
            => _inner.GetStreamAsync(streamId, cancellationToken);

        public Task<IReadOnlyList<StreamMetadata>> GetStreamsByAggregateTypeAsync(string aggregateType, CancellationToken cancellationToken = default)
            => _inner.GetStreamsByAggregateTypeAsync(aggregateType, cancellationToken);

        public Task<IReadOnlyList<StreamMetadata>> GetStreamsByTagAsync(string tag, CancellationToken cancellationToken = default)
            => _inner.GetStreamsByTagAsync(tag, cancellationToken);

        public Task<IReadOnlyList<string>> EnumerateStreamIdsAsync(string? prefix = null, CancellationToken cancellationToken = default)
            => _inner.EnumerateStreamIdsAsync(prefix, cancellationToken);

        public Task<IReadOnlyList<StreamMetadata>> GetStreamsUpdatedAfterAsync(long afterSequencePosition, int? limit = null, CancellationToken cancellationToken = default)
            => _inner.GetStreamsUpdatedAfterAsync(afterSequencePosition, limit, cancellationToken);

        public Task<long> GetStreamCountAsync(string? prefix = null, CancellationToken cancellationToken = default)
            => _inner.GetStreamCountAsync(prefix, cancellationToken);
    }
}
