using LawnDart;
using LawnDart.Dcb;
using LawnDart.EventSourcing.Dcb;
using LawnDart.EventStore;
using LawnDart.Metadata;
using Microsoft.Extensions.Options;

namespace LawnDart.EventSourcing.Testing;

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
    public async Task AppendAsync_WithDCBCondition_BadMetadataTenantWhenValidationOff_ThrowsArgumentException()
    {
        var store = (await Backend.OpenEventStoreAsync(
            ContractStores.Isolated(tenancy: Tenancy.Shared, validateNames: false))).Store;
        var condition = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByType("SomeOtherEvent")));

        await Assert.ThrowsAsync<ArgumentException>(() => store.AppendAsync(
            [ContractEvent.Tick()],
            condition,
            metadata: new EventMetadata { TenantId = "a:b" }));

        await Assert.ThrowsAsync<ArgumentException>(() => store.AppendAsync(
            [ContractEvent.Tick()],
            condition,
            metadata: new EventMetadata { TenantId = " acme" }));

        Assert.Equal(0, await store.GetCurrentSequenceAsync());
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_ValidatesCondition()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var result = await store.AppendAsync(
            [ContractEvent.Tick()],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByType("SomeOtherEvent"))),
            metadata: new EventMetadata { TenantId = "test-tenant" });
        Assert.Single(result.SequencePositions);
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithAfterSequencePosition_IgnoresEarlierEvents()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var first = await store.AppendAsync(Stream(), [ContractEvent.Tick()]);
        var name = EventTypeCatalog.TryGetDeclaredName(typeof(ContractTestEvent))!;
        var condition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByType(name)),
            after: first.SequencePositions[0]);
        var result = await store.AppendAsync([ContractEvent.Tick()], condition, metadata: new EventMetadata { TenantId = "test-tenant" });
        Assert.Single(result.SequencePositions);
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithAfterSequencePosition_FailsWhenMatchAfterPosition()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var first = await store.AppendAsync(Stream(), [ContractEvent.Tick()]);
        var name = EventTypeCatalog.TryGetDeclaredName(typeof(ContractTestEvent))!;
        var condition = AppendCondition.FailIfMatches(
            Query.FromItems(QueryItem.ByType(name)),
            after: first.SequencePositions[0]);
        await store.AppendAsync(Stream(), [ContractEvent.Tick()]);
        await Assert.ThrowsAsync<ConcurrencyException>(() => store.AppendAsync([ContractEvent.Tick()], condition, metadata: new EventMetadata { TenantId = "test-tenant" }));
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithTagQuery_ValidatesTagBasedCondition()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var present = Tag("order");
        var absent = Tag("order");
        await store.AppendAsync(Stream(), [new TaggedEvent(ContractEvent.Tick(), [present])]);
        var result = await store.AppendAsync(
            [new TaggedEvent(new ContractAnotherEvent(Guid.NewGuid(), DateTime.UtcNow), [absent])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(absent))),
            metadata: new EventMetadata { TenantId = "test-tenant" });
        Assert.Single(result.SequencePositions);
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithTagQuery_FailsWhenTagMatches()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var tag = Tag("order");
        await store.AppendAsync(Stream(), [new TaggedEvent(ContractEvent.Tick(), [tag])]);
        await Assert.ThrowsAsync<ConcurrencyException>(() => store.AppendAsync(
            [new TaggedEvent(new ContractAnotherEvent(Guid.NewGuid(), DateTime.UtcNow), [tag])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))),
            metadata: new EventMetadata { TenantId = "test-tenant" }));
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithMultiTagQuery_RequiresAllTags()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var order = Tag("order");
        var payment = Tag("payment");
        await store.AppendAsync(Stream(), [new TaggedEvent(ContractEvent.Tick(), [order])]);
        var result = await store.AppendAsync(
            [new TaggedEvent(new ContractAnotherEvent(Guid.NewGuid(), DateTime.UtcNow), [order, payment])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(order, payment))),
            metadata: new EventMetadata { TenantId = "test-tenant" });
        Assert.Single(result.SequencePositions);
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithMultiTagQuery_FailsWhenAllTagsMatch()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var order = Tag("order");
        var payment = Tag("payment");
        await store.AppendAsync(Stream(), [new TaggedEvent(ContractEvent.Tick(), [order, payment])]);
        await Assert.ThrowsAsync<ConcurrencyException>(() => store.AppendAsync(
            [new TaggedEvent(new ContractAnotherEvent(Guid.NewGuid(), DateTime.UtcNow), [order, payment])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(order, payment))),
            metadata: new EventMetadata { TenantId = "test-tenant" }));
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_WithMultiQueryItem_UsesOrLogic()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var left = Tag("order");
        var right = Tag("order");
        await store.AppendAsync(Stream(), [new TaggedEvent(ContractEvent.Tick(), [left])]);
        await Assert.ThrowsAsync<ConcurrencyException>(() => store.AppendAsync(
            [new TaggedEvent(new ContractAnotherEvent(Guid.NewGuid(), DateTime.UtcNow), [Tag("order")])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(left), QueryItem.ByTags(right))),
            metadata: new EventMetadata { TenantId = "test-tenant" }));
    }

    [Fact]
    public async Task ReadByQueryAsync_WithDCBTags_ReturnsCrossEntityEvents()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var tag = Tag("order");
        await store.AppendAsync(Stream(), [new TaggedEvent(ContractEvent.Tick(), [tag])]);
        await store.AppendAsync(Stream(), [new TaggedEvent(new ContractAnotherEvent(Guid.NewGuid(), DateTime.UtcNow), [tag, Tag("payment")])]);
        await store.AppendAsync(Stream(), [new TaggedEvent(ContractEvent.Tick(), [tag, Tag("product")])]);
        var result = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tag)).ForTenant("test-tenant"));
        Assert.Equal(3, result.Events.Count);
        Assert.All(result.Events, e => Assert.Contains(tag, e.Tags));
    }

    [Fact]
    public async Task AppendAsync_WithDCBCondition_MultiEntityConsistency_EnforcesAtomicity()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var tags = new[] { Tag("order"), Tag("payment"), Tag("product") };
        var query = Query.FromItems(tags.Select(tag => QueryItem.ByTags(tag)).ToArray()).ForTenant("test-tenant");
        var result = await store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), tags), new TaggedEvent(new ContractAnotherEvent(Guid.NewGuid(), DateTime.UtcNow), tags)],
            AppendCondition.FailIfMatches(query, after: 0),
            metadata: new EventMetadata { TenantId = "test-tenant" });
        Assert.Equal(2, result.SequencePositions.Count);
        Assert.Equal(2, (await store.ReadByQueryAsync(query)).Events.Count);
    }

    [Fact]
    public async Task AppendAsync_WithDcbCondition_UsesTenantDcbStream()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var append = await store.AppendAsync(
            [new TaggedEvent(ContractEvent.Tick(), [LawnDart.EventStore.Tag.Parse("order:opaque")])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(LawnDart.EventStore.Tag.Parse("order:opaque")))),
            metadata: new EventMetadata { TenantId = "tenant-opaque" });
        var read = await store.ReadByQueryAsync(Query.All().ForTenant("tenant-opaque"), fromSequencePosition: append.SequencePositions[0], limit: 1);
        var evt = Assert.Single(read.Events);
        Assert.Equal(StreamId.Dcb("tenant-opaque"), evt.StreamId);
    }

    [Fact]
    public async Task AppendAsync_Dcb_ReservedTenantDefault_Throws()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var ex = await Assert.ThrowsAsync<ArgumentException>(() => store.AppendAsync(
            [ContractEvent.Tick()],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(LawnDart.EventStore.Tag.Parse("order:absent")))),
            metadata: new EventMetadata { TenantId = "default" }));
        Assert.Contains("reserved", ex.Message);
    }

    [Fact]
    public async Task AppendAsync_Dcb_ValidateNamesFalse_AllowsReservedTenantDefault()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared, validateNames: false))).Store;
        var append = await store.AppendAsync(
            [ContractEvent.Tick()],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(LawnDart.EventStore.Tag.Parse("order:absent")))),
            metadata: new EventMetadata { TenantId = "default" });
        var read = await store.ReadByQueryAsync(Query.All().ForTenant("default"), fromSequencePosition: append.SequencePositions[0], limit: 1);
        var evt = Assert.Single(read.Events);
        Assert.Equal(StreamId.Dcb("default"), evt.StreamId);
    }

    [Fact]
    public async Task AppendAsync_Dcb_MissingTenant_WhenRequired_Throws()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var ex = await Assert.ThrowsAsync<ArgumentException>(() => store.AppendAsync(
            [ContractEvent.Tick()],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(LawnDart.EventStore.Tag.Parse("order:absent"))))));
        Assert.Contains("TenantId is required in event metadata", ex.Message);
    }

    [Fact]
    public async Task DcbAppends_ConcurrentSameTag_ExactlyOneWins()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var tag = Tag("race");
        var exceptions = await RunBarrierAsync(10, _ => store.AppendAsync(
            [new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 1), [tag])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))),
            metadata: new EventMetadata { TenantId = "test-tenant" }));
        Assert.Equal(1, exceptions.Count(e => e is null));
        Assert.Equal(9, exceptions.Count(e => e is ConcurrencyException));
        Assert.Single((await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tag)).ForTenant("test-tenant"))).Events);
    }

    [Fact]
    public async Task DcbAppends_IndependentDomains_NoContamination()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var tagX = Tag("x");
        var tagY = Tag("y");
        var uniqueness = await RunBarrierAsync(2, i =>
        {
            var tag = i == 0 ? tagX : tagY;
            return store.AppendAsync(
                [new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, i), [tag])],
                AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))),
                new EventMetadata { Timestamp = DateTime.UtcNow, TenantId = "test-tenant" });
        });
        Assert.All(uniqueness, e => Assert.Null(e));

        const int writesPerDomain = 5;
        var domainX = Enumerable.Range(0, writesPerDomain).Select(i => Task.Run(async () =>
        {
            var current = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tagX)).ForTenant("test-tenant"));
            var condition = AppendCondition.FailIfMatches(
                Query.FromItems(QueryItem.ByTags(tagX)),
                after: current.Events.Count == 0 ? null : current.Events[^1].SequencePosition);
            try
            {
                await store.AppendAsync(
                    [new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, i), [tagX])],
                    condition,
                    new EventMetadata { Timestamp = DateTime.UtcNow, TenantId = "test-tenant" });
            }
            catch (ConcurrencyException)
            {
                // A race inside this tag is expected.
            }
        }));
        var domainY = Enumerable.Range(0, writesPerDomain).Select(i => Task.Run(async () =>
        {
            var current = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tagY)).ForTenant("test-tenant"));
            var condition = AppendCondition.FailIfMatches(
                Query.FromItems(QueryItem.ByTags(tagY)),
                after: current.Events.Count == 0 ? null : current.Events[^1].SequencePosition);
            try
            {
                await store.AppendAsync(
                    [new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, i + 100), [tagY])],
                    condition,
                    new EventMetadata { Timestamp = DateTime.UtcNow, TenantId = "test-tenant" });
            }
            catch (ConcurrencyException)
            {
                // A race inside this tag is expected.
            }
        }));
        await Task.WhenAll(domainX.Concat(domainY));

        var xEvents = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tagX)).ForTenant("test-tenant"));
        var yEvents = await store.ReadByQueryAsync(Query.FromItems(QueryItem.ByTags(tagY)).ForTenant("test-tenant"));
        Assert.NotEmpty(xEvents.Events);
        Assert.NotEmpty(yEvents.Events);
        Assert.All(xEvents.Events, e => Assert.DoesNotContain(tagY, e.Tags));
        Assert.All(yEvents.Events, e => Assert.DoesNotContain(tagX, e.Tags));
    }

    [Fact(Timeout = 60_000)]
    public async Task DcbAppends_AndTagsOppositeOrder_DoesNotDeadlock()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var tagA = Tag("zeta");
        var tagB = Tag("alpha");
        var exceptions = await RunBarrierAsync(2, i =>
        {
            var tags = i == 0 ? new[] { tagA, tagB } : new[] { tagB, tagA };
            return store.AppendAsync(
                [new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, i), tags)],
                AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tags))),
                new EventMetadata { Timestamp = DateTime.UtcNow, TenantId = "test-tenant" });
        });
        Assert.Equal(1, exceptions.Count(e => e is null));
        Assert.Equal(1, exceptions.Count(e => e is ConcurrencyException));
        Assert.All(exceptions, e => Assert.True(e is null || e is ConcurrencyException));
    }

    [Fact]
    public async Task DcbAppend_FailIfMatches_HotTag_ThrowsConcurrencyException()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var tag = Tag("hot");
        var seed = Enumerable.Range(0, 50)
            .Select(i => (IEvent)new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, i))
            .ToArray();
        await store.AppendAsync(Stream(), seed.Select(e => new TaggedEvent(e, [tag])).ToList());
        var ex = await Assert.ThrowsAsync<ConcurrencyException>(() => store.AppendAsync(
            [new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 99), [tag])],
            AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tag))),
            new EventMetadata { Timestamp = DateTime.UtcNow, TenantId = "test-tenant" }));
        Assert.NotNull(ex);
    }

    [Fact]
    public async Task GetStateAsync_RebuildsStateFromEvents()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var repository = Repo(store, "test-tenant");
        var tags = new[] { Tag("order"), Tag("customer") };
        await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 100), tags), new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 50), tags)]);
        var state = await repository.GetStateAsync<ContractDcbState>(Query.FromItems(QueryItem.ByTags(tags)));
        Assert.Equal(150, state.TotalValue);
        Assert.Equal(2, state.EventCount);
    }

    [Fact]
    public async Task AppendWithContextAsync_WithAppendCondition_EnforcesConcurrency()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var repository = Repo(store, "test-tenant");
        var tags = new[] { Tag("order"), Tag("customer") };
        var positions = await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 100), tags)]);
        var condition = AppendCondition.FailIfMatches(Query.FromItems(QueryItem.ByTags(tags)), positions[0]);
        Assert.NotEmpty(await repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 50), tags)], condition));
        await Assert.ThrowsAsync<ConcurrencyException>(() => repository.AppendWithContextAsync(
            [new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 25), tags)], condition));
    }

    [Fact]
    public async Task DcbOperations_WithMultipleTags_QueriesCorrectly()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var repository = Repo(store, "test-tenant");
        var entityA = Tag("entity");
        var entityB = Tag("entity");
        var order = Tag("type");
        var payment = Tag("type");
        await repository.AppendWithContextAsync([new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 10), [entityA, order])]);
        await repository.AppendWithContextAsync([new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 20), [entityB, order])]);
        await repository.AppendWithContextAsync([new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 30), [entityA, payment])]);
        Assert.Equal(40, (await repository.GetStateAsync<ContractDcbState>(Query.FromItems(QueryItem.ByTags([entityA])))).TotalValue);
        Assert.Equal(20, (await repository.GetStateAsync<ContractDcbState>(Query.FromItems(QueryItem.ByTags([entityB])))).TotalValue);
    }

    [Fact]
    public async Task DcbOperations_WithTenantTags_IsolatesData()
    {
        var store = (await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared))).Store;
        var repository = Repo(store, "tenant-1");
        var order = Tag("order");
        var tenant1 = LawnDart.EventStore.Tag.Parse("tenant:tenant-1-" + Guid.NewGuid().ToString("N"));
        var tenant2 = LawnDart.EventStore.Tag.Parse("tenant:tenant-2-" + Guid.NewGuid().ToString("N"));
        await repository.AppendWithContextAsync([new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 100), [tenant1, order])]);
        await repository.AppendWithContextAsync([new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 200), [tenant2, order])]);
        Assert.Equal(100, (await repository.GetStateAsync<ContractDcbState>(Query.FromItems(QueryItem.ByTags([tenant1, order])))).TotalValue);
        Assert.Equal(200, (await repository.GetStateAsync<ContractDcbState>(Query.FromItems(QueryItem.ByTags([tenant2, order])))).TotalValue);
    }

    [Fact]
    public async Task GetLastSequencePositionAsync_UsesStoreMax()
    {
        var opened = await Backend.OpenEventStoreAsync(ContractStores.Isolated(tenancy: Tenancy.Shared));
        var repository = Repo(opened.Store, "test-tenant");
        var tags = new[] { Tag("order") };
        await repository.AppendWithContextAsync([new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 1), tags)]);
        await repository.AppendWithContextAsync([new TaggedEvent(new ContractDcbEvent(Guid.NewGuid(), DateTime.UtcNow, 2), tags)]);
        var fromRepo = await repository.GetLastSequencePositionAsync(Query.FromItems(QueryItem.ByTags(tags)));
        var fromStore = await opened.Store.GetMaxSequencePositionAsync(Query.FromItems(QueryItem.ByTags(tags)).ForTenant("test-tenant"));
        Assert.Equal(fromStore, fromRepo);
        Assert.True(fromRepo > 0);
    }

    private static DcbRepository Repo(IEventStore store, string tenant)
        => new(
            store,
            new ContractMetadataProvider(),
            new ContractTenantContextProvider(tenant),
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

    private static StreamId Stream() => StreamId.For("test-tenant", "Order", Guid.NewGuid().ToString("N"));

    private static LawnDart.EventStore.Tag Tag(string kind) => LawnDart.EventStore.Tag.Create(kind, Guid.NewGuid().ToString("N"));
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
