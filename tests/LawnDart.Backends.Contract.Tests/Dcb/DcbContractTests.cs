using LawnDart;
using LawnDart.Dcb;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventStore;
using LawnDart.Metadata;
using Microsoft.Extensions.Options;

namespace LawnDart.Backends.Contract.Tests;

/// <summary>
/// Dynamic consistency boundary appends, queries, and repository reads.
/// </summary>
public abstract class DcbContractTests : ContractSuite
{
    protected DcbContractTests(IContractBackend backend)
        : base(backend)
    {
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_ValidatesCondition()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var result = await store.AppendAsync(
            [ContractEvent.Tick()],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByType("SomeOtherEvent"))));
        Assert.Single(result.SequencePositions);
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithAfterSequencePosition_IgnoresEarlierEvents()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var first = await store.AppendAsync(Stream(), [ContractEvent.Tick()]);
        var name = EventTypeCatalog.TryGetDeclaredName(typeof(ContractTestEvent))!;
        var condition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByType(name)),
            after: first.SequencePositions[0]);
        var result = await store.AppendAsync([ContractEvent.Tick()], condition);
        Assert.Single(result.SequencePositions);
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithAfterSequencePosition_FailsWhenMatchAfterPosition()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var first = await store.AppendAsync(Stream(), [ContractEvent.Tick()]);
        var name = EventTypeCatalog.TryGetDeclaredName(typeof(ContractTestEvent))!;
        var condition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByType(name)),
            after: first.SequencePositions[0]);
        await store.AppendAsync(Stream(), [ContractEvent.Tick()]);
        await Assert.ThrowsAsync<ConcurrencyException>(() => store.AppendAsync([ContractEvent.Tick()], condition));
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithTagQuery_ValidatesTagBasedCondition()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var present = Tag("order");
        var absent = Tag("order");
        await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [present]);
        var result = await store.AppendAsync(
            [new ContractAnotherEvent(Guid.NewGuid(), DateTime.UtcNow)],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(absent))),
            tags: [absent]);
        Assert.Single(result.SequencePositions);
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithTagQuery_FailsWhenTagMatches()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var tag = Tag("order");
        await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [tag]);
        await Assert.ThrowsAsync<ConcurrencyException>(() => store.AppendAsync(
            [new ContractAnotherEvent(Guid.NewGuid(), DateTime.UtcNow)],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))),
            tags: [tag]));
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithMultiTagQuery_RequiresAllTags()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var order = Tag("order");
        var payment = Tag("payment");
        await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [order]);
        var result = await store.AppendAsync(
            [new ContractAnotherEvent(Guid.NewGuid(), DateTime.UtcNow)],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(order, payment))),
            tags: [order, payment]);
        Assert.Single(result.SequencePositions);
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithMultiTagQuery_FailsWhenAllTagsMatch()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var order = Tag("order");
        var payment = Tag("payment");
        await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [order, payment]);
        await Assert.ThrowsAsync<ConcurrencyException>(() => store.AppendAsync(
            [new ContractAnotherEvent(Guid.NewGuid(), DateTime.UtcNow)],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(order, payment))),
            tags: [order, payment]));
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithMultiQueryItem_UsesOrLogic()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var left = Tag("order");
        var right = Tag("order");
        await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [left]);
        await Assert.ThrowsAsync<ConcurrencyException>(() => store.AppendAsync(
            [new ContractAnotherEvent(Guid.NewGuid(), DateTime.UtcNow)],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(left), QueryItem.ByTags(right))),
            tags: [Tag("order")]));
    }

    [Fact]
    public async Task ReadByQueryAsync_WithDCBTags_ReturnsCrossEntityEvents()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var tag = Tag("order");
        await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [tag]);
        await store.AppendAsync(Stream(), [new ContractAnotherEvent(Guid.NewGuid(), DateTime.UtcNow)], tags: [tag, Tag("payment")]);
        await store.AppendAsync(Stream(), [ContractEvent.Tick()], tags: [tag, Tag("product")]);
        var result = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tag)));
        Assert.Equal(3, result.Events.Count);
        Assert.All(result.Events, e => Assert.Contains(tag, e.Tags));
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_MultiEntityConsistency_EnforcesAtomicity()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var tags = new[] { Tag("order"), Tag("payment"), Tag("product") };
        var query = Query.FromItems(tags.Select(tag => QueryItem.ByTags(tag)).ToArray());
        var result = await store.AppendAsync(
            [ContractEvent.Tick(), new ContractAnotherEvent(Guid.NewGuid(), DateTime.UtcNow)],
            AppendCondition.FailIfMatches(query, after: 0),
            tags: tags);
        Assert.Equal(2, result.SequencePositions.Count);
        Assert.Equal(2, (await store.ReadByQueryAsync(query)).Events.Count);
    }

    [Fact]
    public async Task AppendAsync_WithDcbCondition_UsesOpaqueDcbStreamId()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var append = await store.AppendAsync(
            [ContractEvent.Tick()],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags("order:opaque"))),
            metadata: new EventMetadata { TenantId = "tenant-opaque" },
            tags: ["order:opaque"]);
        var read = await store.ReadByQueryAsync(Query.All(), fromSequencePosition: append.SequencePositions[0], limit: 1);
        var evt = Assert.Single(read.Events);
        Assert.StartsWith("tenant-opaque:dcb:", evt.StreamId);
    }

    [Fact]
    public async Task DcbAppends_ConcurrentSameTag_ExactlyOneWins()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var tag = Tag("race");
        var exceptions = await RunBarrierAsync(10, _ => store.AppendAsync(
            [new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 1)],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))),
            tags: [tag]));
        Assert.Equal(1, exceptions.Count(e => e is null));
        Assert.Equal(9, exceptions.Count(e => e is ConcurrencyException));
        Assert.Single((await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tag)))).Events);
    }

    [Fact]
    public async Task DcbAppends_IndependentDomains_NoContamination()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var tagX = Tag("x");
        var tagY = Tag("y");
        var uniqueness = await RunBarrierAsync(2, i =>
        {
            var tag = i == 0 ? tagX : tagY;
            return store.AppendAsync(
                [new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, i)],
                AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))),
                new EventMetadata { Timestamp = DateTime.UtcNow },
                tags: [tag]);
        });
        Assert.All(uniqueness, e => Assert.Null(e));

        const int writesPerDomain = 5;
        var domainX = Enumerable.Range(0, writesPerDomain).Select(i => Task.Run(async () =>
        {
            var current = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tagX)));
            var condition = AppendCondition.FailIfMatches(
                Query.FromItems(QueryItem.ByTags(tagX)),
                after: current.Events.Count == 0 ? null : current.Events[^1].SequencePosition);
            try
            {
                await store.AppendAsync(
                    [new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, i)],
                    condition,
                    new EventMetadata { Timestamp = DateTime.UtcNow },
                    tags: [tagX]);
            }
            catch (ConcurrencyException)
            {
                // A race inside this tag is expected.
            }
        }));
        var domainY = Enumerable.Range(0, writesPerDomain).Select(i => Task.Run(async () =>
        {
            var current = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tagY)));
            var condition = AppendCondition.FailIfMatches(
                Query.FromItems(QueryItem.ByTags(tagY)),
                after: current.Events.Count == 0 ? null : current.Events[^1].SequencePosition);
            try
            {
                await store.AppendAsync(
                    [new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, i + 100)],
                    condition,
                    new EventMetadata { Timestamp = DateTime.UtcNow },
                    tags: [tagY]);
            }
            catch (ConcurrencyException)
            {
                // A race inside this tag is expected.
            }
        }));
        await Task.WhenAll(domainX.Concat(domainY));

        var xEvents = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tagX)));
        var yEvents = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tagY)));
        Assert.NotEmpty(xEvents.Events);
        Assert.NotEmpty(yEvents.Events);
        Assert.All(xEvents.Events, e => Assert.DoesNotContain(tagY, e.Tags));
        Assert.All(yEvents.Events, e => Assert.DoesNotContain(tagX, e.Tags));
    }

    [Fact(Timeout = 60_000)]
    public async Task DcbAppends_AndTagsOppositeOrder_DoesNotDeadlock()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var tagA = Tag("zeta");
        var tagB = Tag("alpha");
        var exceptions = await RunBarrierAsync(2, i =>
        {
            var tags = i == 0 ? new[] { tagA, tagB } : new[] { tagB, tagA };
            return store.AppendAsync(
                [new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, i)],
                AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tags))),
                new EventMetadata { Timestamp = DateTime.UtcNow },
                tags);
        });
        Assert.Equal(1, exceptions.Count(e => e is null));
        Assert.Equal(1, exceptions.Count(e => e is ConcurrencyException));
    }

    [Fact]
    public async Task DcbAppend_FailIfMatches_HotTag_ThrowsConcurrencyException()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var tag = Tag("hot");
        var seed = Enumerable.Range(0, 50)
            .Select(i => (IEvent)new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, i))
            .ToArray();
        await store.AppendAsync(Stream(), seed, tags: [tag]);
        await Assert.ThrowsAsync<ConcurrencyException>(() => store.AppendAsync(
            [new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 99)],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))),
            new EventMetadata { Timestamp = DateTime.UtcNow },
            tags: [tag]));
    }

    [Fact]
    public async Task GetStateAsync_RebuildsStateFromEvents()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var repository = Repo(store, "test-tenant");
        var tags = new[] { Tag("order"), Tag("customer") };
        await repository.AppendWithContextAsync(
            [
                new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 100),
                new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 50)
            ],
            tags);
        var state = await repository.GetStateAsync<ContractDcbState>(tags);
        Assert.Equal(150, state.TotalValue);
        Assert.Equal(2, state.EventCount);
    }

    [Fact]
    public async Task AppendWithContextAsync_WithAppendCondition_EnforcesConcurrency()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var repository = Repo(store, "test-tenant");
        var tags = new[] { Tag("order"), Tag("customer") };
        var positions = await repository.AppendWithContextAsync(
            [new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 100)],
            tags);
        var condition = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tags)), positions[0]);
        Assert.NotEmpty(await repository.AppendWithContextAsync(
            [new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 50)],
            tags,
            condition));
        await Assert.ThrowsAsync<ConcurrencyException>(() => repository.AppendWithContextAsync(
            [new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 25)],
            tags,
            condition));
    }

    [Fact]
    public async Task DcbOperations_WithMultipleTags_QueriesCorrectly()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var repository = Repo(store, "test-tenant");
        var entityA = Tag("entity");
        var entityB = Tag("entity");
        var order = Tag("type");
        var payment = Tag("type");
        await repository.AppendWithContextAsync([new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 10)], [entityA, order]);
        await repository.AppendWithContextAsync([new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 20)], [entityB, order]);
        await repository.AppendWithContextAsync([new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 30)], [entityA, payment]);
        Assert.Equal(40, (await repository.GetStateAsync<ContractDcbState>([entityA])).TotalValue);
        Assert.Equal(20, (await repository.GetStateAsync<ContractDcbState>([entityB])).TotalValue);
    }

    [Fact]
    public async Task DcbOperations_WithTenantTags_IsolatesData()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated())).Store;
        var repository = Repo(store, "tenant-1");
        var order = Tag("order");
        var tenant1 = "tenant:tenant-1-" + Guid.NewGuid().ToString("N");
        var tenant2 = "tenant:tenant-2-" + Guid.NewGuid().ToString("N");
        await repository.AppendWithContextAsync([new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 100)], [tenant1, order]);
        await repository.AppendWithContextAsync([new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 200)], [tenant2, order]);
        Assert.Equal(100, (await repository.GetStateAsync<ContractDcbState>([tenant1, order])).TotalValue);
        Assert.Equal(200, (await repository.GetStateAsync<ContractDcbState>([tenant2, order])).TotalValue);
    }

    [Fact]
    public async Task GetLastSequencePositionAsync_UsesStoreMax()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated());
        var repository = Repo(opened.Store, "test-tenant");
        var tags = new[] { Tag("order") };
        await repository.AppendWithContextAsync([new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 1)], tags);
        await repository.AppendWithContextAsync([new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 2)], tags);
        var fromRepo = await repository.GetLastSequencePositionAsync(tags);
        var fromStore = await opened.Store.GetMaxSequencePositionAsync(Query.FromItems(QueryItem.ByTags(tags)));
        Assert.Equal(fromStore, fromRepo);
        Assert.True(fromRepo > 0);
    }

    private static DcbRepository Repo(IEventStore store, string tenant)
        => new(
            store,
            new TestMetadataProvider(),
            new TestTenantContextProvider(tenant),
            Options.Create(new LawnDartOptions()),
            null,
            null,
            null);

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
        return await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    private static string Stream() => "test-tenant:Order:" + Guid.NewGuid().ToString("N");

    private static string Tag(string kind) => kind + ":" + Guid.NewGuid().ToString("N");
}

public sealed class ContractDcbState : IState
{
    public int TotalValue { get; private set; }

    public int EventCount { get; private set; }

    public void Apply(ContractDcbEvent @event)
    {
        TotalValue += @event.Value;
        EventCount++;
    }
}
