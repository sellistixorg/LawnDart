using LawnDart;
using LawnDart.EventSourcing.SqlServer;
using LawnDart.EventSourcing.SqlServer.EventStore;
using LawnDart.EventStore;
using LawnDart.Metadata;
using Testcontainers.MsSql;
using Xunit;

namespace LawnDart.EventSourcing.SqlServer.Tests.Dcb;

/// <summary>
/// SQL Server DCB fence concurrency tests: when writers race on the same append
/// condition, exactly one must win and the rest must fail with a concurrency error.
/// </summary>
[Trait("Category", "Integration")]
public sealed class DcbSqlServerConcurrencyTests : IAsyncLifetime
{
    private MsSqlContainer? _container;
    private SqlServerEventStore? _store;

    public async Task InitializeAsync()
    {
        _container = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2025-latest")
            .WithPassword("Test123!")
            .Build();
        await _container.StartAsync();

        _store = new SqlServerEventStore(
            _container.GetConnectionString(),
            serializer: null,
            tableName: "Events",
            registryTableName: "Streams",
            enableRegistry: true,
            new SqlServerEventStoreOptions { RequireTenantId = false },
            outboxWriter: null,
            logger: null);

        await _store.InitializeSchemaAsync();
    }

    public async Task DisposeAsync()
    {
        if (_container != null)
            await _container.DisposeAsync();
    }

    /// <summary>
    /// Ten racers <c>FailIfMatches</c> the same tag with <c>After</c> null and emit that tag.
    /// Exactly one append must succeed.
    /// </summary>
    [Fact]
    public async Task DcbAppends_ConcurrentSameTag_ExactlyOneWins()
    {
        const string tag = "dcb:concurrent:domain-a";
        const int racers = 10;
        var condition = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag)));

        var exceptions = await RunBarrierAsync(racers, i =>
            _store!.AppendAsync(
                [new DcbConcurrencyEvent(Guid.NewGuid(), DateTime.UtcNow, i)],
                condition,
                new EventMetadata { Timestamp = DateTime.UtcNow },
                tags: [tag]));

        var successes = exceptions.Count(e => e is null);
        var conflicts = exceptions.Count(e => e is ConcurrencyException);

        Assert.Equal(1, successes);
        Assert.Equal(racers - 1, conflicts);

        var query = await _store!.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tag)));
        Assert.Single(query.Events);
    }

    /// <summary>
    /// Independent domains must not share events, and After-null uniqueness on different
    /// tags must both succeed (they must not needlessly serialize as one lock).
    /// </summary>
    [Fact]
    public async Task DcbAppends_IndependentDomains_NoContamination()
    {
        const string tagX = "dcb:concurrent:domain-x";
        const string tagY = "dcb:concurrent:domain-y";

        var uniqueness = await RunBarrierAsync(2, i =>
        {
            var tag = i == 0 ? tagX : tagY;
            return _store!.AppendAsync(
                [new DcbConcurrencyEvent(Guid.NewGuid(), DateTime.UtcNow, i)],
                AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))),
                new EventMetadata { Timestamp = DateTime.UtcNow },
                tags: [tag]);
        });

        Assert.All(uniqueness, e => Assert.Null(e));

        const int writesPerDomain = 5;

        var domainXTasks = Enumerable.Range(0, writesPerDomain).Select(i => Task.Run(async () =>
        {
            var current = await _store!.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tagX)));
            var condition = AppendCondition.FailIfMatches(
                Query.FromItems(QueryItem.ByTags(tagX)),
                after: current.Events.Count == 0 ? null : current.Events[^1].SequencePosition);
            try
            {
                await _store.AppendAsync(
                    [new DcbConcurrencyEvent(Guid.NewGuid(), DateTime.UtcNow, i)],
                    condition,
                    new EventMetadata { Timestamp = DateTime.UtcNow },
                    tags: [tagX]);
            }
            catch (ConcurrencyException)
            {
                /* Expected — race inside domain X. */
            }
        }));

        var domainYTasks = Enumerable.Range(0, writesPerDomain).Select(i => Task.Run(async () =>
        {
            var current = await _store!.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tagY)));
            var condition = AppendCondition.FailIfMatches(
                Query.FromItems(QueryItem.ByTags(tagY)),
                after: current.Events.Count == 0 ? null : current.Events[^1].SequencePosition);
            try
            {
                await _store.AppendAsync(
                    [new DcbConcurrencyEvent(Guid.NewGuid(), DateTime.UtcNow, i + 100)],
                    condition,
                    new EventMetadata { Timestamp = DateTime.UtcNow },
                    tags: [tagY]);
            }
            catch (ConcurrencyException)
            {
                /* Expected — race inside domain Y. */
            }
        }));

        await Task.WhenAll(domainXTasks.Concat(domainYTasks));

        var xEvents = await _store!.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tagX)));
        var yEvents = await _store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tagY)));

        Assert.NotEmpty(xEvents.Events);
        Assert.NotEmpty(yEvents.Events);
        Assert.All(xEvents.Events, e => Assert.DoesNotContain(tagY, e.Tags ?? []));
        Assert.All(yEvents.Events, e => Assert.DoesNotContain(tagX, e.Tags ?? []));
    }

    /// <summary>
    /// AND-tag writers pass tags in opposite order. Sorted lock acquisition must complete
    /// without hanging (deadlock 1205 is retried; this test must not time out).
    /// </summary>
    [Fact(Timeout = 60_000)]
    public async Task DcbAppends_AndTagsOppositeOrder_DoesNotDeadlock()
    {
        const string tagA = "dcb:deadlock:zeta";
        const string tagB = "dcb:deadlock:alpha";

        var exceptions = await RunBarrierAsync(2, i =>
        {
            var tags = i == 0 ? new[] { tagA, tagB } : new[] { tagB, tagA };
            return _store!.AppendAsync(
                [new DcbConcurrencyEvent(Guid.NewGuid(), DateTime.UtcNow, i)],
                AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tags))),
                new EventMetadata { Timestamp = DateTime.UtcNow },
                tags: tags);
        });

        var successes = exceptions.Count(e => e is null);
        var conflicts = exceptions.Count(e => e is ConcurrencyException);
        Assert.Equal(1, successes);
        Assert.Equal(1, conflicts);
        Assert.All(exceptions, e => Assert.True(e is null || e is ConcurrencyException));
    }

    /// <summary>
    /// Existence-only condition: a hot tag with many events still 409s, without needing
    /// the caller to drain the match set.
    /// </summary>
    [Fact]
    public async Task DcbAppend_FailIfMatches_HotTag_ThrowsConcurrencyException()
    {
        const string tag = "dcb:hot:existence";
        var seed = Enumerable.Range(0, 50)
            .Select(i => (IEvent)new DcbConcurrencyEvent(Guid.NewGuid(), DateTime.UtcNow, i))
            .ToArray();
        await _store!.AppendAsync("bench:hot", seed, tags: [tag]);

        var ex = await Assert.ThrowsAsync<ConcurrencyException>(() =>
            _store.AppendAsync(
                [new DcbConcurrencyEvent(Guid.NewGuid(), DateTime.UtcNow, 99)],
                AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))),
                new EventMetadata { Timestamp = DateTime.UtcNow },
                tags: [tag]));

        Assert.NotNull(ex);
    }

    private static async Task<Exception?[]> RunBarrierAsync(int racers, Func<int, Task> work)
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ready = 0;

        var tasks = Enumerable.Range(0, racers).Select(async i =>
        {
            if (Interlocked.Increment(ref ready) == racers)
                gate.SetResult();
            await gate.Task.ConfigureAwait(false);
            try
            {
                await work(i).ConfigureAwait(false);
                return (Exception?)null;
            }
            catch (Exception ex)
            {
                return ex;
            }
        });

        return await Task.WhenAll(tasks);
    }

    private sealed record DcbConcurrencyEvent(Guid Id, DateTime Timestamp, int Value) : IEvent;
}
